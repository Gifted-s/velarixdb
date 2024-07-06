use crate::cfg::Config;
use crate::compactors::{CompactionReason, Compactor};
use crate::consts::{
    BUCKETS_DIRECTORY_NAME, HEAD_ENTRY_KEY, KB, MAX_KEY_SIZE, MAX_VALUE_SIZE, META_DIRECTORY_NAME, TOMB_STONE_MARKER,
    VALUE_LOG_DIRECTORY_NAME, VLOG_START_OFFSET,
};
use crate::db::keyspace::is_valid_keyspace_name;
use crate::flush::Flusher;
use crate::gc::gc::GC;
use crate::index::Index;
use crate::key_range::KeyRange;
use crate::memtable::{Entry, MemTable, K};
use crate::meta::Meta;
use crate::range::RangeIterator;
use crate::sst::Table;
use crate::types::{
    BloomFilterHandle, Bool, BucketMapHandle, CreatedAt, FlushSignal, GCUpdatedEntries, ImmutableMemTables, Key,
    KeyRangeHandle, MemtableFlushStream, Value,
};
use crate::util;
use crate::vlog::ValueLog;
use chrono::Utc;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self};
use tokio::sync::{Mutex, RwLock};

/// DataStore struct is the main struct for the library crate
/// i.e user-facing struct
pub struct DataStore<'a, Key>
where
    Key: K,
{
    /// Keyspace name
    pub(crate) keyspace: &'a str,

    /// Directory to be used by store
    pub(crate) dir: DirPath,

    /// Active memtable that accepts read and writes using a lock free skipmap
    pub(crate) active_memtable: MemTable<Key>,

    /// In memory bloom filters for fast retreival
    pub(crate) filters: BloomFilterHandle,

    /// Value log to perisist entries and for crash recovery
    pub(crate) val_log: ValueLog,

    /// Bucket Map that groups sstables by size
    pub(crate) buckets: BucketMapHandle,

    /// Stores largest and smallest key of each sstable for fast retrieval
    pub(crate) key_range: KeyRangeHandle,

    /// handles compaction of sstables
    pub(crate) compactor: Compactor,

    /// Keeps track of store metadata
    pub(crate) meta: Meta,

    /// Handles flushing of memtables to disk
    pub(crate) flusher: Flusher,

    /// Store configuration
    pub(crate) config: Config,

    /// Garbage Collector to remove osbolete entries from disk
    pub(crate) gc: GC,

    /// Handles range queries
    pub(crate) range_iterator: Option<RangeIterator<'a>>,

    /// Stores read only memtables yet to be flushed
    pub(crate) read_only_memtables: ImmutableMemTables<Key>,

    /// Sends singnal to subscribers whenever a flush happens
    pub(crate) flush_signal_tx: async_broadcast::Sender<FlushSignal>,

    /// Flush listeners receives signals from this channel
    pub(crate) flush_signal_rx: async_broadcast::Receiver<FlushSignal>,

    /// Stores valid entries gotten from garbage collection but yet to be synced with 
    /// memtable
    pub(crate) gc_updated_entries: GCUpdatedEntries<Key>,

    /// GC Table is synced with active memtable in case GC is triggered, we don't need to  use main
    /// active memtable as this can impact performance
    pub(crate) gc_table: Arc<RwLock<MemTable<Key>>>,

    /// GC Log is similar to value log but opeations are tailored to GC
    pub(crate) gc_log: Arc<RwLock<ValueLog>>,

    /// keeps track of memtable going through flush
    pub(crate) flush_stream: MemtableFlushStream,
    // TODO: pub block_cache: BlockCache
}

#[derive(Clone, Debug)]
pub struct DirPath {
    pub root: PathBuf,
    pub val_log: PathBuf,
    pub buckets: PathBuf,
    pub meta: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SizeUnit {
    Bytes,
    Kilobytes,
    Megabytes,
    Gigabytes,
}

impl SizeUnit {
    pub(crate) const fn to_bytes(&self, value: usize) -> usize {
        match self {
            SizeUnit::Bytes => value,
            SizeUnit::Kilobytes => value * KB,
            SizeUnit::Megabytes => value * KB * KB,
            SizeUnit::Gigabytes => value * KB * KB * KB,
        }
    }
}

impl DataStore<'static, Key> {
    /// Opens a keyspace in the given directory.
    ///
    /// Keyspace names can be up to 255 characters long, can not be empty and
    /// can only contain alphanumerics, underscore (`_`) and dash (`-`).
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    ///
    ///   
    /// # Panics
    ///
    /// Panics if the keyspace name is invalid.
    pub async fn open<P: AsRef<Path> + Send + Sync>(
        keyspace: &'static str,
        dir: P,
    ) -> Result<DataStore<'static, Key>, crate::err::Error> {
        assert!(is_valid_keyspace_name(keyspace));
        let dir = DirPath::build(dir);
        let default_config = Config::default();
        let mut store = Self::create_or_recover(dir.to_owned(), SizeUnit::Bytes, default_config).await?;
        store.keyspace = keyspace;
        store.start_background_tasks();
        Ok(store)
    }

    /// Same as [`Datastor::open`], but does not start background tasks.
    ///
    /// Needed to open a keyspace without background tasks for testing.
    ///
    /// Should not be user-facing.
    #[doc(hidden)]
    pub async fn open_without_background<P: AsRef<Path> + Send + Sync>(
        keyspace: &'static str,
        dir: P,
    ) -> Result<DataStore<'static, Key>, crate::err::Error> {
        assert!(is_valid_keyspace_name(keyspace));
        log::info!("Opening keyspace at {:?}", dir.as_ref());

        let dir = DirPath::build(dir);
        let default_config = Config::default();
        let mut store = Self::create_or_recover(dir.to_owned(), SizeUnit::Bytes, default_config).await?;
        store.keyspace = keyspace;
        Ok(store)
    }

    /// Starts background tasks that maintain the keyspace.
    ///
    /// Should not be called, unless [`DataStore::open`]
    /// and should not be user-facing.
    pub(crate) fn start_background_tasks(&self) {
        self.compactor.start_periodic_background_compaction(
            Arc::clone(&self.buckets),
            Arc::clone(&self.filters),
            Arc::clone(&self.key_range),
        );
        self.compactor.start_flush_listener(
            self.flush_signal_rx.clone(),
            Arc::clone(&self.buckets),
            Arc::clone(&self.filters),
            Arc::clone(&self.key_range),
        );
        self.gc
            .start_background_gc_task(Arc::clone(&self.key_range), Arc::clone(&self.read_only_memtables));
    }

    pub async fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, val: V) -> Result<Bool, crate::err::Error> {
        self.validate_size(key.as_ref(), Some(val.as_ref()))?;

        if !self.gc_updated_entries.read().await.is_empty() {
            self.sync_gc_update_with_store().await?
        }

        // This ensures sstables in key range whose filter is newly loaded(after crash) are mapped to the sstables
        self.key_range.write().await.update_key_range().await;
        let is_tombstone = std::str::from_utf8(val.as_ref()).unwrap() == TOMB_STONE_MARKER;
        let created_at = Utc::now();
        let v_offset = self
            .val_log
            .append(key.as_ref(), val.as_ref(), created_at, is_tombstone)
            .await;
        let entry = Entry::new(key.as_ref().to_vec(), v_offset, created_at, is_tombstone);

        if self.active_memtable.is_full(HEAD_ENTRY_KEY.len()) {
            self.migrate_memtable_to_read_only().await;
        }
        self.active_memtable.insert(&entry);
        let gc_table = Arc::clone(&self.gc_table);
        tokio::spawn(async move { gc_table.write().await.insert(&entry) });
        // TODO: Figure out the best way to empty flush stream
        Ok(true)
    }

    /// Moves active memtable to read-only memtables 
    ///
    /// Marks the active memtable as read only,
    /// updates store metadata and moves the memtable
    /// to read-only memtables 
    pub(crate) async fn migrate_memtable_to_read_only(&mut self) {
        let head_offset = self.active_memtable.get_most_recent_offset();

        self.val_log.set_head(head_offset);
        self.meta.set_head(head_offset);
        self.meta.update_last_modified();

        let gc_log = Arc::clone(&self.gc_log);
        tokio::spawn(async move {
            (gc_log.write().await).head_offset = head_offset;
        });

        let head_entry = Entry::new(HEAD_ENTRY_KEY.to_vec(), head_offset, Utc::now(), false);
        self.active_memtable.insert(&head_entry);
        self.active_memtable.mark_readonly();
        self.update_meta_background();
        self.read_only_memtables.write().await.insert(
            MemTable::generate_table_id(),
            Arc::new(RwLock::new(self.active_memtable.to_owned())),
        );

        if self.read_only_memtables.read().await.len() >= self.config.max_buffer_write_number {
            // Background
            let _ = self.flush_read_only_memtables().await;
        }
        self.reset_memtables();
    }

    /// Synchronize GC table with active memtable
    ///
    /// Valid entries collected during garbage collection are
    /// inserted to active memtable, unused space is reclaimed,
    /// and metadata is updated
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    #[doc(hidden)]
    pub(crate) async fn sync_gc_update_with_store(&mut self) -> Result<(), crate::err::Error> {
        let gc_entries_reader = self.gc_updated_entries.read().await;
        for e in gc_entries_reader.iter() {
            self.active_memtable.insert(&Entry::new(
                e.key().to_vec(),
                e.value().val_offset,
                e.value().created_at,
                e.value().is_tombstone,
            ));
        }
        gc_entries_reader.clear();
        let (updated_head, updated_tail) = self.gc.free_unused_space().await?;
        self.meta.v_log_head = updated_head;
        self.meta.v_log_tail = updated_tail;
        self.meta.last_modified = Utc::now();
        self.val_log.head_offset = updated_head;
        self.val_log.tail_offset = updated_tail;
        Ok(())
    }

    /// Updates metadata in background
    /// #[doc(hidden)]
    pub(crate) fn update_meta_background(&self) {
        let meta = Arc::new(Mutex::new(self.meta.to_owned()));
        tokio::spawn(async move {
            if let Err(err) = meta.lock().await.write().await {
                log::error!("{}", err)
            }
        });
    }

    pub async fn delete<T: AsRef<[u8]>>(&mut self, key: T) -> Result<bool, crate::err::Error> {
        self.validate_size(key.as_ref(), None::<T>)?;
        self.get(key.as_ref()).await?;
        let value = TOMB_STONE_MARKER;
        self.put(key.as_ref(), value).await
    }

    /// Flushes read-only memtable to disk using a background tokio task
    pub(crate) async fn flush_read_only_memtables(&mut self) {
        let tables = self.read_only_memtables.read().await;
        for (table_id, table) in tables.iter() {
            if self.flush_stream.contains(table_id) {
                continue;
            }
            let table_inner = Arc::clone(table);
            let id = table_id.clone();
            let mut flusher = self.flusher.clone();
            let tx = self.flush_signal_tx.clone();
            // NOTE: If the put method returns before the code inside tokio::spawn finishes executing,
            // the tokio::spawn task will continue to run independently of the original function call.
            // This is because tokio::spawn creates a new asynchronous task that is managed by the Tokio runtime.
            // The spawned task is executed concurrently and its lifecycle is not tied to the function that spawned it.
            // TODO: See if we can introduce semaphors to prevent overloading the system
            self.flush_stream.insert(id.to_owned());
            tokio::spawn(async move {
                flusher.flush_handler(id, table_inner, tx);
            });
        }
    }

    /// Resets both active memtable and GC table to new
    pub(crate) fn reset_memtables(&mut self) {
        let capacity = self.active_memtable.capacity();
        let size_unit = self.active_memtable.size_unit();
        let false_positive_rate = self.active_memtable.false_positive_rate();
        self.active_memtable = MemTable::with_specified_capacity_and_rate(size_unit, capacity, false_positive_rate);
        self.gc_table = Arc::new(RwLock::new(MemTable::with_specified_capacity_and_rate(
            size_unit,
            capacity,
            false_positive_rate,
        )));
    }

    pub async fn get<T: AsRef<[u8]>>(&self, key: T) -> Result<(Value, CreatedAt), crate::err::Error> {
        self.validate_size(key.as_ref(), None::<T>)?;
        let gc_entries_reader = self.gc_updated_entries.read().await;
        if !gc_entries_reader.is_empty() {
            let res = gc_entries_reader.get(key.as_ref());
            if res.is_some() {
                let value = res.to_owned().unwrap().value().to_owned();
                if value.is_tombstone {
                    return Err(crate::err::Error::NotFoundInDB);
                }
                return self.get_value_from_vlog(value.val_offset, value.created_at).await;
            }
        }
        drop(gc_entries_reader);
        let mut offset = VLOG_START_OFFSET;
        let mut insert_time = util::default_datetime();
        let lowest_insert_time = util::default_datetime();
        if let Some(value) = self.active_memtable.get(key.as_ref()) {
            if value.is_tombstone {
                return Err(crate::err::Error::NotFoundInDB);
            }
            self.get_value_from_vlog(value.val_offset, value.created_at).await
        } else {
            let mut is_deleted = false;
            for (_, table) in self.read_only_memtables.read().await.iter() {
                if let Some(value) = table.read().await.get(key.as_ref()) {
                    if value.created_at > insert_time {
                        offset = value.val_offset;
                        insert_time = value.created_at;
                        is_deleted = value.is_tombstone
                    }
                }
            }

            if self.found_in_table(insert_time, lowest_insert_time) {
                if is_deleted {
                    return Err(crate::err::Error::NotFoundInDB);
                }
                self.get_value_from_vlog(offset, insert_time).await
            } else {
                let key_range = &self.key_range.read().await;
                let ssts = key_range.filter_sstables_by_biggest_key(key.as_ref()).await?;
                if ssts.is_empty() {
                    return Err(crate::err::Error::NotFoundInDB);
                }
                self.search_key_in_sstables(key, ssts).await
            }
        }
    }

    pub async fn update<T: AsRef<[u8]>>(&mut self, key: T, value: T) -> Result<bool, crate::err::Error> {
        self.validate_size(key.as_ref(), Some(value.as_ref()))?;
        self.get(key.as_ref()).await?;
        self.put(key, value).await
    }

    /// Validate key and value sizes.
    ///
    /// Key size can be up to 65536 bytes in size, and value size can be
    /// up to 2^32 bytes, key cannot be zero length and value(if provded)
    /// cannot be zero length
    ///
    /// # Errors
    ///
    /// Returns error, if validation failed.
    pub(crate) fn validate_size<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        val: Option<V>,
    ) -> Result<(), crate::err::Error> {
        if key.as_ref().is_empty() {
            return Err(crate::err::Error::KeySizeNone);
        }

        if key.as_ref().len() > MAX_KEY_SIZE {
            return Err(crate::err::Error::KeyMaxSizeExceeded);
        }

        if val.is_some() && val.as_ref().unwrap().as_ref().is_empty() {
            return Err(crate::err::Error::ValueSizeNone);
        }

        if val.is_some() && val.as_ref().unwrap().as_ref().len() > MAX_VALUE_SIZE {
            return Err(crate::err::Error::ValMaxSizeExceeded);
        }
        Ok(())
    }

    /// Search for a key across SSTables
    ///
    /// [`Index`] is used to locate block is sstables that
    /// can possibly contain the key
    ///
    ///
    /// # Errors
    ///
    /// Returns error, if any error occurs.
    pub(crate) async fn search_key_in_sstables<K: AsRef<[u8]>>(
        &self,
        key: K,
        ssts: Vec<Table>,
    ) -> Result<(Value, CreatedAt), crate::err::Error> {
        let mut insert_time = util::default_datetime();
        let lowest_insert_date = util::default_datetime();
        let mut offset = 0;
        let mut is_deleted = false;
        for sst in ssts.iter() {
            let index = Index::new(sst.index_file.path.to_owned(), sst.index_file.file.to_owned());
            let block_handle = index.get(&key).await?;
            if block_handle.is_some() {
                let sst_res = sst.get(block_handle.unwrap(), &key).await?;
                if sst_res.as_ref().is_some() {
                    let (val_offset, created_at, is_tombstone) = sst_res.unwrap();
                    if created_at > insert_time {
                        offset = val_offset;
                        insert_time = created_at;
                        is_deleted = is_tombstone;
                    }
                }
            }
        }
        if self.found_in_table(insert_time, lowest_insert_date) {
            if is_deleted {
                return Err(crate::err::Error::NotFoundInDB);
            }
            return self.get_value_from_vlog(offset, insert_time).await;
        }
        Err(crate::err::Error::NotFoundInDB)
    }

    /// Checks if insert time is greater than the least
    /// possible insert time meaning the key was found
    pub fn found_in_table(&self, insert_time: CreatedAt, lowest_insert_date: CreatedAt) -> bool {
        insert_time > lowest_insert_date
    }

    /// Retrieves value from Value Log
    ///
    /// Returns value from value log using the provided offset
    ///
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurs or key was not found
    pub(crate) async fn get_value_from_vlog(
        &self,
        offset: usize,
        creation_time: CreatedAt,
    ) -> Result<(Value, CreatedAt), crate::err::Error> {
        let res = self.val_log.get(offset).await?;
        if res.is_some() {
            let (value, is_tombstone) = res.unwrap();
            if is_tombstone {
                return Err(crate::err::Error::KeyFoundAsTombstoneInValueLogError);
            }
            return Ok((value, creation_time));
        }
        Err(crate::err::Error::KeyNotFoundInValueLogError)
    }



    /// Flushes all memtable (active and read-only) to disk
    ///
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurs or key was not found
    #[cfg(test)]
    pub(crate) async fn flush_all_memtables(&mut self) -> Result<(), crate::err::Error> {
        use indexmap::IndexMap;

        self.active_memtable.read_only = true;

        self.read_only_memtables.write().await.insert(
            MemTable::generate_table_id(),
            Arc::new(RwLock::new(self.active_memtable.to_owned())),
        );
        let immutable_tables = self.read_only_memtables.read().await.to_owned();
        let mut flusher = Flusher::new(
            Arc::clone(&self.read_only_memtables),
            Arc::clone(&self.buckets),
            Arc::clone(&self.filters),
            Arc::clone(&self.key_range),
        );
        for (id, table) in immutable_tables.iter() {
            if self.flush_stream.contains(id) {
                continue;
            }
            self.flush_stream.insert(id.to_vec());
            flusher.flush(table.to_owned()).await?;
        }
        self.active_memtable.clear();
        self.read_only_memtables = Arc::new(RwLock::new(IndexMap::new()));
        Ok(())
    }

    /// Creates or opens a keyspace in the specified directory.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    async fn create_or_recover(
        dir: DirPath,
        size_unit: SizeUnit,
        config: Config,
    ) -> Result<DataStore<'static, Key>, crate::err::Error> {
        let vlog_path = &dir.clone().val_log;
        let buckets_path = dir.buckets.clone();
        let vlog_exit = vlog_path.exists();
        let vlog_empty = !vlog_exit
            || fs::metadata(vlog_path)
                .await
                .map_err(crate::err::Error::GetFileMetaDataError)?
                .len()
                == 0;
        let key_range = KeyRange::new();
        let vlog = ValueLog::new(vlog_path).await?;
        let meta = Meta::new(&dir.meta).await?;
        if vlog_empty {
            return DataStore::handle_empty_vlog(dir, buckets_path, vlog, key_range, &config, size_unit, meta).await;
        }
        DataStore::recover(dir, buckets_path, vlog, key_range, &config, size_unit, meta).await
    }

    /// Strigger compaction mannually
    ///
    /// # Errors
    ///
    /// Returns error, if trigger failed
    pub async fn run_compaction(&mut self) -> Result<(), crate::err::Error> {
        self.compactor.reason = CompactionReason::Manual;
        Compactor::handle_compaction(
            Arc::clone(&self.buckets),
            Arc::clone(&self.filters.clone()),
            Arc::clone(&self.key_range),
            &self.compactor.config,
        )
        .await
    }

    /// Returns length of entries in active memtable
    pub async fn len_of_entries_in_memtable(&mut self) -> usize {
        self.active_memtable.entries.len()
    }

    /// Get [`DataStore`] directories
    pub async fn get_dir(&mut self) -> DirPath {
        self.dir.to_owned()
    }

    /// Checks if `range_iterator` is set for [`DataStore`]
    pub async fn is_range_iterator_set(&mut self) -> Bool {
        self.range_iterator.is_some()
    }
}
impl DirPath {
    pub(crate) fn build<P: AsRef<Path> + Send + Sync>(root_path: P) -> Self {
        let root = root_path;
        let val_log = root.as_ref().join(VALUE_LOG_DIRECTORY_NAME);
        let buckets = root.as_ref().join(BUCKETS_DIRECTORY_NAME);
        let meta = root.as_ref().join(META_DIRECTORY_NAME);
        Self {
            root: root.as_ref().to_path_buf(),
            val_log,
            buckets,
            meta,
        }
    }
}
