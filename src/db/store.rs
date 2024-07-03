use crate::cfg::Config;
use crate::compactors::Compactor;
use crate::consts::{
    BUCKETS_DIRECTORY_NAME, HEAD_ENTRY_KEY, KB, META_DIRECTORY_NAME, TOMB_STONE_MARKER, VALUE_LOG_DIRECTORY_NAME,
    VLOG_START_OFFSET,
};
use crate::err::Error;
use crate::err::Error::*;

use crate::flush::Flusher;
use crate::gc::gc::GC;
use crate::index::Index;
use crate::key_range::KeyRange;
use crate::memtable::{Entry, MemTable, K};
use crate::meta::Meta;
use crate::range::RangeIterator;
use crate::sst::Table;
use crate::types::{
    BloomFilterHandle, Bool, BucketMapHandle, CreatedAt, FlushSignal, GCUpdatedEntries, ImmutableMemTable, Key,
    KeyRangeHandle, MemtableFlushStream, Value,
};
use crate::util;
use crate::vlog::ValueLog;
use async_trait::async_trait;
use chrono::Utc;
use futures::lock::Mutex;
use indexmap::IndexMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self};
use tokio::sync::RwLock;

pub struct DataStore<'a, Key>
where
    Key: K,
{
    /// Keyspace name
    pub(crate) keyspace: &'a str,

    /// Directory to be used by store
    pub(crate) dir: DirPath,

    /// Active memtable that accepts read and writes
    /// it uses a lock free skipmap to store the entries
    pub(crate) active_memtable: MemTable<Key>,

    /// In memory bloomfilters for fast retreival
    pub(crate) filters: BloomFilterHandle,

    /// Value log to perisist entries and
    /// for crash recovery
    pub(crate) val_log: ValueLog,

    /// Bucket Map that groups sstables by size
    pub(crate) buckets: BucketMapHandle,

    /// Stores largest and smallest key of
    /// each sstable for fast retrieval
    pub(crate) key_range: KeyRangeHandle,

    /// handles compaction of sstables
    pub(crate) compactor: Compactor,

    /// Keys track of store information
    pub(crate) meta: Meta,

    /// Handles flushing of memtables to disk
    pub(crate) flusher: Flusher,

    /// Store configuration
    pub(crate) config: Config,

    /// Garbage Colector to remove osbolete
    /// entries from disk
    pub(crate) gc: GC,

    /// Handles range queries
    pub(crate) range_iterator: Option<RangeIterator<'a>>,

    /// Stores read only memtables yet to be flushed
    pub(crate) read_only_memtables: ImmutableMemTable<Key>,

    /// Sends singnal to subscribers whenever a flush happens
    pub(crate) flush_signal_tx: async_broadcast::Sender<FlushSignal>,

    /// Flush listeners receives signals from this channel
    pub(crate) flush_signal_rx: async_broadcast::Receiver<FlushSignal>,

    /// Stores valid entries gotten from garbage
    /// collection but yet to be synced with store
    pub(crate) gc_updated_entries: GCUpdatedEntries<Key>,

    /// GC Table is synced with active memtable
    /// in case GC is triggered, we don't need to  use main
    /// active memtable as this can impact performance
    pub(crate) gc_table: Arc<RwLock<MemTable<Key>>>,

    /// GC Log is similar to value log
    /// but opeations are tailored to GC
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
    pub async fn new<P: AsRef<Path> + Send + Sync>(
        keyspace: &'static str,
        dir: P,
    ) -> Result<DataStore<'static, Key>, Error> {
        let dir = DirPath::build(dir);
        let default_config = Config::default();
        let mut store = DataStore::with_default_config(dir.to_owned(), SizeUnit::Bytes, default_config).await?;
        store.keyspace = keyspace;
        store.start_background_tasks();
        return Ok(store);
    }

    pub fn start_background_tasks(&self) {
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

    pub async fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, val: V) -> Result<Bool, Error> {
        if !self.gc_updated_entries.read().await.is_empty() {
            if let Err(err) = self.sync_gc_update_with_store().await {
                log::error!("{}", err)
            }
        }
        // This ensures sstables in key range whose filter is newly loaded(after crash) are mapped to the sstables
        self.key_range.write().await.update_key_range().await;
        let is_tombstone = std::str::from_utf8(val.as_ref()).unwrap() == TOMB_STONE_MARKER;
        let created_at = Utc::now();
        let v_offset = self
            .val_log
            .append(key.as_ref(), val.as_ref(), created_at, is_tombstone)
            .await?;
        let entry = Entry::new(key.as_ref().to_vec(), v_offset, created_at, is_tombstone);

        if self.active_memtable.is_full(HEAD_ENTRY_KEY.len()) {
            let head_offset = self.active_memtable.get_most_recent_offset();
            // reset head in vLog
            self.val_log.set_head(head_offset as usize);
            self.meta.v_log_head = head_offset;
            self.meta.last_modified = Utc::now();
            let gc_log = Arc::clone(&self.gc_log);
            tokio::spawn(async move {
                (gc_log.write().await).head_offset = head_offset;
            });

            let head_entry = Entry::new(HEAD_ENTRY_KEY.to_vec(), head_offset, Utc::now(), false);
            self.active_memtable.insert(&head_entry)?;
            self.active_memtable.read_only = true;
            self.update_meta_background();
            self.read_only_memtables.write().await.insert(
                MemTable::generate_table_id(),
                Arc::new(RwLock::new(self.active_memtable.to_owned())),
            );

            if self.read_only_memtables.read().await.len() >= self.config.max_buffer_write_number {
                let _ = self.flush_read_only_memtables().await;
            }
            self.reset_memtables();
        }
        self.active_memtable.insert(&entry)?;
        let gc_table = Arc::clone(&self.gc_table);
        tokio::spawn(async move { gc_table.write().await.insert(&entry) });
        // TODO: Figure out the best way to empty flush stream
        Ok(true)
    }

    pub async fn sync_gc_update_with_store(&mut self) -> Result<(), Error> {
        let gc_entries_reader = self.gc_updated_entries.read().await;
        for e in gc_entries_reader.iter() {
            self.active_memtable.insert(&Entry::new(
                e.key().to_vec(),
                e.value().val_offset,
                e.value().created_at,
                e.value().is_tombstone,
            ))?;
        }
        gc_entries_reader.clear();
        let (updated_head, updated_tail) = self.gc.free_unused_space().await?;
        self.meta.v_log_head = updated_head;
        self.meta.v_log_tail = updated_tail;
        self.meta.last_modified = Utc::now();
        self.val_log.head_offset = updated_head;
        self.val_log.tail_offset = updated_tail;
        return Ok(());
    }

    pub fn update_meta_background(&self) {
        let meta = Arc::new(Mutex::new(self.meta.to_owned()));
        tokio::spawn(async move {
            if let Err(err) = meta.lock().await.write().await {
                log::error!("{}", err)
            }
        });
    }

    pub async fn delete<T: AsRef<[u8]>>(&mut self, key: T) -> Result<bool, Error> {
        self.get(key.as_ref()).await?;
        let value = TOMB_STONE_MARKER;
        self.put(key.as_ref(), value).await
    }

    pub async fn flush_read_only_memtables(&mut self) {
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

    pub fn reset_memtables(&mut self) {
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

    pub async fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<(Value, CreatedAt), Error> {
        let gc_entries_reader = self.gc_updated_entries.read().await;
        if !gc_entries_reader.is_empty() {
            let res = gc_entries_reader.get(key.as_ref());
            if res.is_some() {
                let value = res.to_owned().unwrap().value().to_owned();
                if value.is_tombstone {
                    return Err(NotFoundInDB);
                }
                return self.get_value_from_vlog(value.val_offset, value.created_at).await;
            }
        }
        drop(gc_entries_reader);
        let mut offset = VLOG_START_OFFSET;
        let mut insert_time = util::default_datetime();
        let lowest_insert_time = util::default_datetime();

        // Step 1: Check the active memtable
        if let Some(value) = self.active_memtable.get(key.as_ref()) {
            if value.is_tombstone {
                return Err(NotFoundInDB);
            }
            return self.get_value_from_vlog(value.val_offset, value.created_at).await;
        } else {
            // Step 2: Check the read-only memtables
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
                    return Err(NotFoundInDB);
                }
                return self.get_value_from_vlog(offset, insert_time).await;
            } else {
                // Step 3: Check sstables
                let key_range = &self.key_range.read().await;
                let ssts = key_range.filter_sstables_by_biggest_key(key.as_ref()).await?;
                if ssts.is_empty() {
                    return Err(NotFoundInDB);
                }
                return self.search_key_in_sstables(key, ssts).await;
            }
        }
    }


    pub async fn update<T: AsRef<[u8]>>(&mut self, key: T, value: T) -> Result<bool, Error> {
        self.get(key.as_ref()).await?;
        self.put(key, value).await
    }

    pub async fn search_key_in_sstables<K: AsRef<[u8]>>(
        &self,
        key: K,
        ssts: Vec<Table>,
    ) -> Result<(Value, CreatedAt), Error> {
        let mut insert_time = util::default_datetime();
        let lowest_insert_date = util::default_datetime();
        let mut offset = 0;
        let mut is_deleted = false;
        for sst in ssts.iter() {
            let index = Index::new(sst.index_file.path.to_owned(), sst.index_file.file.to_owned());
            let block_handle = index.get(&key).await;
            match block_handle {
                Ok(None) => continue,
                Ok(result) => {
                    if let Some(block_offset) = result {
                        let sst_res = sst.get(block_offset, &key).await;
                        match sst_res {
                            Ok(None) => continue,
                            Ok(result) => {
                                if let Some((val_offset, created_at, is_tombstone)) = result {
                                    if created_at > insert_time {
                                        offset = val_offset;
                                        insert_time = created_at;
                                        is_deleted = is_tombstone;
                                    }
                                }
                            }
                            Err(err) => log::error!("{}", err),
                        }
                    }
                }
                Err(err) => log::error!("{}", err),
            }
        }
        if self.found_in_table(insert_time, lowest_insert_date) {
            if is_deleted {
                return Err(NotFoundInDB);
            }
            return self.get_value_from_vlog(offset, insert_time).await;
        }
        return Err(NotFoundInDB);
    }

    pub fn found_in_table(&self, insert_time: CreatedAt, lowest_insert_date: CreatedAt) -> bool {
        insert_time > lowest_insert_date
    }

    pub async fn get_value_from_vlog(
        &self,
        offset: usize,
        creation_time: CreatedAt,
    ) -> Result<(Value, CreatedAt), Error> {
        let res = self.val_log.get(offset).await?;
        if res.is_some(){
            let (value, is_tombstone) = res.unwrap();
            if is_tombstone {
                return Err(KeyFoundAsTombstoneInValueLogError);
            }
            return Ok((value, creation_time));
        }
        return Err(KeyNotFoundInValueLogError)
    }


    #[cfg(test)]
    pub async fn flush_all_memtables(&mut self) -> Result<(), Error> {
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

    async fn with_default_config(
        dir: DirPath,
        size_unit: SizeUnit,
        config: Config,
    ) -> Result<DataStore<'static, Key>, Error> {
        Self::with_capacity_and_rate(dir, size_unit, config).await
    }

    async fn with_capacity_and_rate(
        dir: DirPath,
        size_unit: SizeUnit,
        config: Config,
    ) -> Result<DataStore<'static, Key>, Error> {
        let vlog_path = &dir.clone().val_log;
        let buckets_path = dir.buckets.clone();
        let vlog_exit = vlog_path.exists();
        let vlog_empty = !vlog_exit || fs::metadata(vlog_path).await.map_err(GetFileMetaDataError)?.len() == 0;
        let key_range = KeyRange::new();
        let vlog = ValueLog::new(vlog_path).await?;
        let meta = Meta::new(&dir.meta).await?;
        if vlog_empty {
            return DataStore::handle_empty_vlog(dir, buckets_path, vlog, key_range, &config, size_unit, meta).await;
        }
        return DataStore::recover(dir, buckets_path, vlog, key_range, &config, size_unit, meta).await;
    }

    pub async fn run_compaction(&mut self) -> Result<(), Error> {
        Compactor::handle_compaction(
            Arc::clone(&self.buckets),
            Arc::clone(&self.filters.clone()),
            Arc::clone(&self.key_range),
            &self.compactor.config,
        )
        .await
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
