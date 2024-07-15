use crate::cfg::Config;
use crate::compactors::{CompactionReason, Compactor};
use crate::consts::{
    BUCKETS_DIRECTORY_NAME, HEAD_ENTRY_KEY, KB, MAX_KEY_SIZE, MAX_VALUE_SIZE, META_DIRECTORY_NAME, TOMB_STONE_MARKER,
    VALUE_LOG_DIRECTORY_NAME, VLOG_START_OFFSET,
};
use crate::db::keyspace::is_valid_keyspace_name;
use crate::flush::Flusher;
use crate::gc::garbage_collector::GC;
use crate::index::Index;
use crate::key_range::KeyRange;
use crate::memtable::{Entry, MemTable, UserEntry, K};
use crate::meta::Meta;
use crate::range::RangeIterator;
use crate::sst::Table;
use crate::types::{
    Bool, BucketMapHandle, CreatedAt, FlushSignal, GCUpdatedEntries, ImmutableMemTables, Key, KeyRangeHandle,
    MemtableFlushStream,
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

    /// Active memtable that accepts reads and writes using a lock free skipmap
    pub(crate) active_memtable: MemTable<Key>,

    /// Value log to persist entries and for crash recovery
    pub(crate) val_log: ValueLog,

    /// Bucket Map that groups sstables by size
    pub(crate) buckets: BucketMapHandle,

    /// Stores largest and smallest key of each sstable for fast retrieval
    pub(crate) key_range: KeyRangeHandle,

    /// Handles compaction of sstables
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

    /// Flush listeners receiver
    pub(crate) flush_signal_rx: async_broadcast::Receiver<FlushSignal>,

    /// Stores valid entries gotten from garbage collection but yet to be synced with
    /// memtable
    pub(crate) gc_updated_entries: GCUpdatedEntries<Key>,

    /// GC Table is synced with active memtable in case GC is triggered, we don't need to  use main
    /// active memtable as this can impact performance
    pub(crate) gc_table: Arc<RwLock<MemTable<Key>>>,

    /// GC Log is similar to value log but with lock
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
    pub(crate) const fn as_bytes(&self, value: usize) -> usize {
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
        // NOTE: we only incrememnt the ref counter not a deep clone
        self.compactor
            .spawn_compaction_worker(self.buckets.clone(), self.key_range.clone());

        self.compactor.start_flush_listener(
            self.flush_signal_rx.clone(),
            self.buckets.clone(),
            self.key_range.clone(),
        );

        self.gc
            .start_gc_worker(self.key_range.clone(), self.read_only_memtables.clone());
    }

    /// Inserts a new entry into the store
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// use velarixdb::db::DataStore;

    /// #[tokio::main]
    /// async fn main() {
    ///     let root = tempdir().unwrap();
    ///     let path = root.path().join("velarixdb");
    ///     let mut store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error
    ///
    ///     let res1 = store.put("apple", "tim cook").await;
    ///     let res2 = store.put("google", "sundar pichai").await;
    ///     let res3 = store.put("nvidia", "jensen huang").await;
    ///     let res4 = store.put("microsoft", "satya nadella").await;
    ///     let res5 = store.put("meta", "mark zuckerberg").await;
    ///     let res6 = store.put("openai", "sam altman").await;
    ///
    ///     assert!(res1.is_ok());
    ///     assert!(res2.is_ok());
    ///     assert!(res3.is_ok());
    ///     assert!(res4.is_ok());
    ///     assert!(res5.is_ok());
    ///     assert!(res6.is_ok());
    /// }
    ///
    /// ```
    pub async fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, val: V) -> Result<Bool, crate::err::Error> {
        self.validate_size(key.as_ref(), Some(val.as_ref()))?;

        if !self.gc_updated_entries.read().await.is_empty() {
            self.sync_gc_update_with_store().await?
        }

        // This ensures sstables in key range whose filter is newly loaded(after crash) are mapped to the sstables
        self.key_range.update_key_range().await;
        let is_tombstone = std::str::from_utf8(val.as_ref()).unwrap() == TOMB_STONE_MARKER;
        let created_at = Utc::now();
        let v_offset = self
            .val_log
            .append(key.as_ref(), val.as_ref(), created_at, is_tombstone)
            .await?;
        let entry = Entry::new(key.as_ref().to_vec(), v_offset, created_at, is_tombstone);

        if self.active_memtable.is_full(HEAD_ENTRY_KEY.len()) {
            self.migrate_memtable_to_read_only();
        }
        self.active_memtable.insert(&entry);
        let gc_table = Arc::clone(&self.gc_table);
        tokio::spawn(async move { gc_table.write().await.insert(&entry) });
        Ok(true)
    }

    /// Moves active memtable to read-only memtables
    ///
    /// Marks the active memtable as read only,
    /// updates store metadata and moves the memtable
    /// to read-only memtables
    pub(crate) fn migrate_memtable_to_read_only(&mut self) {
        let head_offset = self.active_memtable.get_most_recent_offset();

        self.val_log.set_head(head_offset);
        self.meta.set_head(head_offset);
        self.meta.update_last_modified();

        let gc_log = Arc::clone(&self.gc_log);
        tokio::spawn(async move {
            (gc_log.write().await).head_offset = head_offset;
        });
        let is_tombstone = false;
        let head_entry = Entry::new(HEAD_ENTRY_KEY.to_vec(), head_offset, Utc::now(), is_tombstone);
        self.active_memtable.insert(&head_entry);
        self.active_memtable.mark_readonly();
        self.update_meta_background();

        if self.read_only_memtables.is_empty() {
            self.flush_stream.clear();
        }
        self.read_only_memtables
            .insert(MemTable::generate_table_id(), Arc::new(self.active_memtable.to_owned()));

        if self.read_only_memtables.len() >= self.config.max_buffer_write_number {
            self.flush_read_only_memtables();
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
        self.meta.set_head(updated_head);
        self.meta.set_tail(updated_tail);
        self.meta.update_last_modified();
        self.val_log.set_head(updated_head);
        self.val_log.set_tail(updated_tail);
        Ok(())
    }

    /// Updates metadata in background
    #[doc(hidden)]
    pub(crate) fn update_meta_background(&self) {
        let meta = Arc::new(Mutex::new(self.meta.to_owned()));
        tokio::spawn(async move {
            if let Err(err) = meta.lock().await.write().await {
                log::error!("{}", err)
            }
        });
    }

    /// Removes an entry from the store
    ///
    ///
    /// # Examples
    ///
    /// ```
    /// # use tempfile::tempdir;
    /// use velarixdb::db::DataStore;
    ///
    /// #[tokio::main]
    ///  async fn main() {
    ///  let root = tempdir().unwrap();
    ///  let path = root.path().join("velarixdb");
    ///  let mut store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error
    ///
    ///   store.put("apple", "tim cook").await.unwrap(); // handle error
    ///   // Retrieve entry
    ///   let entry = store.get("apple").await.unwrap();
    ///   assert!(entry.is_some());
    ///
    ///   // Delete entry
    ///   store.delete("apple").await.unwrap();
    ///
    ///   // Entry should now be None
    ///   let entry = store.get("apple").await.unwrap();
    ///   assert!(entry.is_none());
    /// }
    ///
    /// ```

    pub async fn delete<T: AsRef<[u8]>>(&mut self, key: T) -> Result<bool, crate::err::Error> {
        self.validate_size(key.as_ref(), None::<T>)?;
        self.get(key.as_ref()).await?;
        let value = TOMB_STONE_MARKER;
        self.put(key.as_ref(), value).await
    }

    /// Flushes read-only memtable to disk using a background tokio task
    pub(crate) fn flush_read_only_memtables(&mut self) {
        for table in self.read_only_memtables.iter() {
            let key = table.key().to_owned();
            let value = table.value().to_owned();
            if self.flush_stream.contains(&key) {
                continue;
            }
            let mut flusher = self.flusher.clone();
            let tx = self.flush_signal_tx.clone();
            // NOTE: If the put method returns before the code inside tokio::spawn finishes executing,
            // the tokio::spawn task will continue to run independently of the original function call.
            // This is because tokio::spawn creates a new asynchronous task that is managed by the Tokio runtime.
            // The spawned task is executed concurrently and its lifecycle is not tied to the function that spawned it.
            // TODO: See if we can introduce semaphors to prevent overloading the system
            self.flush_stream.insert(key.to_vec());
            tokio::spawn(async move {
                flusher.flush_handler(key, value, tx);
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

    /// Reteives an entry from the [`DataStore`]
    ///
    ///
    /// This is user facing and its asyncronous
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use tempfile::tempdir;
    /// use velarixdb::db::DataStore;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///  let root = tempdir().unwrap();
    ///  let path = root.path().join("velarixdb");
    ///  let mut store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error
    ///
    ///  let res1 = store.put("apple", "tim cook").await;
    ///  let res2 = store.put("google", "sundar pichai").await;
    ///  let res3 = store.put("nvidia", "jensen huang").await;
    ///  let res4 = store.put("microsoft", "satya nadella").await;
    ///  let res5 = store.put("meta", "mark zuckerberg").await;
    ///  let res6 = store.put("openai", "sam altman").await;
    ///
    ///  assert!(res1.is_ok());
    ///  assert!(res2.is_ok());
    ///  assert!(res3.is_ok());
    ///  assert!(res4.is_ok());
    ///  assert!(res5.is_ok());
    ///  assert!(res6.is_ok());
    ///
    ///  let entry1 = store.get("apple").await.unwrap(); // Handle error
    ///  let entry2 = store.get("google").await.unwrap();
    ///  let entry3 = store.get("nvidia").await.unwrap();
    ///  let entry4 = store.get("microsoft").await.unwrap();
    ///  let entry5 = store.get("meta").await.unwrap();
    ///  let entry6 = store.get("openai").await.unwrap();
    ///  let entry7 = store.get("***not_found_key**").await.unwrap();
    ///
    ///  assert_eq!(std::str::from_utf8(&entry1.unwrap().val).unwrap(), "tim cook");
    ///  assert_eq!(std::str::from_utf8(&entry2.unwrap().val).unwrap(), "sundar pichai");
    ///  assert_eq!(std::str::from_utf8(&entry3.unwrap().val).unwrap(), "jensen huang");
    ///  assert_eq!(std::str::from_utf8(&entry4.unwrap().val).unwrap(), "satya nadella");
    ///  assert_eq!(std::str::from_utf8(&entry5.unwrap().val).unwrap(), "mark zuckerberg");
    ///  assert_eq!(std::str::from_utf8(&entry6.unwrap().val).unwrap(), "sam altman");
    ///  assert!(entry7.is_none())
    /// }
    /// ```

    pub async fn get<T: AsRef<[u8]>>(&self, key: T) -> Result<Option<UserEntry>, crate::err::Error> {
        self.validate_size(key.as_ref(), None::<T>)?;

        if let Some(val) = self.search_gc_entries(key.as_ref()).await? {
            return Ok(Some(val));
        }

        let mut offset = VLOG_START_OFFSET;
        let mut insert_time = util::default_datetime();
        let lowest_insert_time = util::default_datetime();
        if let Some(val) = self.active_memtable.get(key.as_ref()) {
            if val.is_tombstone {
                return Ok(None);
            }
            self.get_value_from_vlog(val.val_offset, val.created_at).await
        } else {
            let mut is_deleted = false;
            for table in self.read_only_memtables.iter() {
                if let Some(val) = table.value().get(key.as_ref()) {
                    if val.created_at > insert_time {
                        offset = val.val_offset;
                        insert_time = val.created_at;
                        is_deleted = val.is_tombstone
                    }
                }
            }
            if self.found_in_table(insert_time, lowest_insert_time) {
                if is_deleted {
                    return Ok(None);
                }
                self.get_value_from_vlog(offset, insert_time).await
            } else {
                let ssts = &self.key_range.filter_sstables_by_key_range(key.as_ref()).await?;
                if ssts.is_empty() {
                    return Ok(None);
                }
                self.search_key_in_sstables(key, ssts.to_vec()).await
            }
        }
    }

    /// Searches for entries from gc yet be synced to active memtable
    ///
    ///
    /// # Errors
    ///
    /// Returns error, if IO error occurs
    async fn search_gc_entries(&self, key: &[u8]) -> Result<Option<UserEntry>, crate::err::Error> {
        let gc_entries = self.gc_updated_entries.read().await;
        if !gc_entries.is_empty() {
            if let Some(e) = gc_entries.get(key) {
                let val = e.value();
                if val.is_tombstone {
                    return Ok(None);
                }
                return self.get_value_from_vlog(val.val_offset, val.created_at).await;
            }
        }
        Ok(None)
    }

    /// Removes an entry from the store
    ///
    /// # Examples
    ///
    /// ```
    /// # use tempfile::tempdir;
    /// use velarixdb::db::DataStore;
    /// #[tokio::main]
    /// async fn main() {
    ///     let root = tempdir().unwrap();
    ///     let path = root.path().join("velarixdb");
    ///     let mut store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error
    ///
    ///     store.put("apple", "tim cook").await.unwrap(); // handle error
    ///
    ///     // Update entry
    ///     let success = store.update("apple", "elon musk").await;
    ///     assert!(success.is_ok());
    ///
    ///     // Entry should now be updated
    ///     let entry = store.get("apple").await.unwrap(); // handle error
    ///     assert!(entry.is_some());
    ///     assert_eq!(std::str::from_utf8(&entry.unwrap().val).unwrap(), "elon musk")
    /// }
    /// ```

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
    ) -> Result<Option<UserEntry>, crate::err::Error> {
        let mut insert_time = util::default_datetime();
        let lowest_insert_date = util::default_datetime();
        let mut offset = VLOG_START_OFFSET;
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
                return Ok(None);
            }
            return self.get_value_from_vlog(offset, insert_time).await;
        }
        Ok(None)
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
        created_at: CreatedAt,
    ) -> Result<Option<UserEntry>, crate::err::Error> {
        let res = self.val_log.get(offset).await?;
        if let Some((value, is_tombstone)) = res {
            if is_tombstone {
                return Ok(None);
            }
            return Ok(Some(UserEntry::new(value, created_at)));
        }
        Ok(None)
    }

    /// Flushes all memtable (active and read-only) to disk
    ///
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurs or key was not found
    #[doc(hidden)]
    #[cfg(test)]
    pub(crate) async fn force_flush(&mut self) -> Result<(), crate::err::Error> {
        use crossbeam_skiplist::SkipMap;

        self.active_memtable.mark_readonly();

        self.read_only_memtables
            .insert(MemTable::generate_table_id(), Arc::new(self.active_memtable.to_owned()));
        let immutable_tables = self.read_only_memtables.to_owned();
        let mut flusher = Flusher::new(
            Arc::clone(&self.read_only_memtables),
            Arc::clone(&self.buckets),
            Arc::clone(&self.key_range),
        );
        for table in immutable_tables.iter() {
            if self.flush_stream.contains(table.key()) {
                continue;
            }
            self.flush_stream.insert(table.key().to_vec());
            flusher.flush(table.value().to_owned()).await?;
        }
        self.active_memtable.clear();
        self.read_only_memtables = Arc::new(SkipMap::new());
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
                .map_err(crate::err::Error::GetFileMetaData)?
                .len()
                == 0;
        let key_range = KeyRange::default();
        let vlog = ValueLog::new(vlog_path).await?;
        let meta = Meta::new(&dir.meta).await?;
        if vlog_empty {
            return DataStore::handle_empty_vlog(dir, buckets_path, vlog, key_range, &config, size_unit, meta).await;
        }
        DataStore::recover(dir, buckets_path, vlog, key_range, &config, size_unit, meta).await
    }

    /// Trigger compaction mannually
    ///
    /// # Errors
    ///
    /// Returns error, if trigger failed
    pub async fn run_compaction(&mut self) -> Result<(), crate::err::Error> {
        self.compactor.reason = CompactionReason::Manual;
        Compactor::handle_compaction(
            Arc::clone(&self.buckets),
            Arc::clone(&self.key_range),
            &self.compactor.config,
        )
        .await
    }

    /// Returns length of entries in active memtable
    pub fn len_of_entries_in_memtable(&self) -> usize {
        self.active_memtable.entries.len()
    }

    /// Get [`DataStore`] directories
    pub async fn get_dir(&self) -> DirPath {
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
