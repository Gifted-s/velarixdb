use crate::bucket::bucket::InsertableToBucket;
use crate::cfg::Config;
use crate::compactors::Compactor;
use crate::consts::{
    BUCKETS_DIRECTORY_NAME, HEAD_ENTRY_KEY, KB, META_DIRECTORY_NAME, TOMB_STONE_MARKER, VALUE_LOG_DIRECTORY_NAME,
};
use crate::err::Error;
use crate::err::Error::*;
use crate::filter::BloomFilter;
use crate::flusher::Flusher;
use crate::gc::gc::GC;
use crate::index::Index;
use crate::key_range::KeyRange;
use crate::memtable::{Entry, MemTable};
use crate::meta::Meta;
use crate::range::RangeIterator;
use crate::types::{
    self, BloomFilterHandle, Bool, BucketMapHandle, CreationTime, FlushSignal, GCUpdatedEntries, ImmutableMemTable,
    Key, KeyRangeHandle, Value,
};
use crate::value_log::ValueLog;
use chrono::Utc;
use indexmap::IndexMap;
use std::path::PathBuf;
use std::{hash::Hash, sync::Arc};
use tokio::fs::{self};
use tokio::sync::RwLock;
pub struct DataStore<'a, K>
where
    K: Hash + Ord + Send + Sync + Clone,
{
    pub dir: DirPath,
    pub active_memtable: MemTable<K>,
    pub filters: BloomFilterHandle,
    pub val_log: ValueLog,
    pub buckets: BucketMapHandle,
    pub key_range: KeyRangeHandle,
    pub compactor: Compactor,
    pub meta: Meta,
    pub flusher: Flusher,
    pub config: Config,
    pub gc: GC,
    pub range_iterator: Option<RangeIterator<'a>>,
    pub read_only_memtables: ImmutableMemTable<K>,
    pub flush_signal_tx: async_broadcast::Sender<FlushSignal>,
    pub flush_signal_rx: async_broadcast::Receiver<FlushSignal>,
    pub gc_updated_entries: GCUpdatedEntries<K>,
    pub gc_table: Arc<RwLock<MemTable<Key>>>,
    pub gc_log: Arc<RwLock<ValueLog>>,
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

impl<'a> DataStore<'a, Key> {
    pub async fn new(dir: PathBuf) -> Result<DataStore<'a, Vec<u8>>, Error> {
        let dir = DirPath::build(dir);
        let default_config = Config::default();
        let store = DataStore::with_default_config(dir.to_owned(), SizeUnit::Bytes, default_config).await?;
        store.start_background_jobs();
        return Ok(store);
    }

    pub async fn new_with_custom_config(dir: PathBuf, config: Config) -> Result<DataStore<'a, Key>, Error> {
        let dir = DirPath::build(dir);
        let store = DataStore::with_default_config(dir.clone(), SizeUnit::Bytes, config).await?;
        store.start_background_jobs();
        return Ok(store);
    }

    pub fn start_background_jobs(&self) {
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

        self.gc.start_background_gc_task(
            Arc::clone(&self.filters),
            Arc::clone(&self.key_range),
            Arc::clone(&self.read_only_memtables),
            Arc::clone(&self.gc_updated_entries),
        );
    }

    pub async fn put(&mut self, key: &str, val: &str) -> Result<Bool, Error> {
        let gc_entries_reader = self.gc_updated_entries.read().await;
        if !gc_entries_reader.is_empty() {
            for e in gc_entries_reader.iter() {
                self.active_memtable.insert(&Entry::new(
                    e.key().to_vec(),
                    e.value().val_offset,
                    e.value().created_at,
                    e.value().is_tombstone,
                ))?;
            }
            gc_entries_reader.clear();
        }
        drop(gc_entries_reader);
        let is_tombstone = val == TOMB_STONE_MARKER;
        let key = &key.as_bytes().to_vec();
        let val = &val.as_bytes().to_vec();
        let created_at = Utc::now().timestamp_millis() as u64;
        let v_offset = self.val_log.append(key, val, created_at, is_tombstone).await?;

        let entry = Entry::new(key.to_vec(), v_offset, created_at, is_tombstone);
        if self.active_memtable.is_full(HEAD_ENTRY_KEY.len()) {
            let capacity = self.active_memtable.capacity();
            let size_unit = self.active_memtable.size_unit();
            let false_pos = self.active_memtable.false_positive_rate();
            let head_offset = self.active_memtable.most_recent_entry.val_offset;

            // reset head in vLog
            self.val_log.set_head(head_offset as usize);
            let head_entry = Entry::new(
                HEAD_ENTRY_KEY.to_vec(),
                head_offset,
                Utc::now().timestamp_millis() as u64,
                false,
            );
            self.active_memtable.insert(&head_entry)?;
            self.active_memtable.read_only = true;
            self.read_only_memtables.write().await.insert(
                MemTable::generate_table_id(),
                Arc::new(RwLock::new(self.active_memtable.to_owned())),
            );

            if self.read_only_memtables.read().await.len() >= self.config.max_buffer_write_number {
                let immutable_tables = self.read_only_memtables.read().await;
                for (table_id, table) in immutable_tables.iter() {
                    let table_inner = Arc::clone(table);
                    let id = table_id.clone();
                    let mut flusher = self.flusher.clone();
                    let tx = self.flush_signal_tx.clone();
                    // NOTE: If the put method returns before the code inside tokio::spawn finishes executing,
                    // the tokio::spawn task will continue to run independently of the original function call.
                    // This is because tokio::spawn creates a new asynchronous task that is managed by the Tokio runtime.
                    // The spawned task is executed concurrently and its lifecycle is not tied to the function that spawned it.
                    tokio::spawn(async move {
                        flusher.flush_handler(id, table_inner, tx);
                    });
                }
            }
            self.active_memtable = MemTable::with_specified_capacity_and_rate(size_unit, capacity, false_pos);
            self.gc_table = Arc::new(RwLock::new(MemTable::with_specified_capacity_and_rate(
                size_unit, capacity, false_pos,
            )));
        }
        self.active_memtable.insert(&entry)?;
        let gc_table = Arc::clone(&self.gc_table);
        tokio::spawn(async move { gc_table.write().await.insert(&entry) });
        Ok(true)
    }

    pub async fn delete(&mut self, key: &str) -> Result<bool, Error> {
        self.get(key).await?;
        let value = TOMB_STONE_MARKER;
        self.put(key, value).await
    }

    pub async fn get(&self, key: &str) -> Result<(Value, CreationTime), Error> {
        let key = key.as_bytes().to_vec();
        let gc_entries_reader = self.gc_updated_entries.read().await;
        if !gc_entries_reader.is_empty() {
            let res = gc_entries_reader.get(&key);
            if res.is_some() {
                let value = res.to_owned().unwrap().value().to_owned();
                if value.is_tombstone {
                    return Err(NotFoundInDB);
                }
                return self.get_value_from_vlog(value.val_offset, value.created_at).await;
            }
        }
        drop(gc_entries_reader);
        let mut offset = 0;
        let mut most_recent_insert_time = 0;
        // Step 1: Check the active memtable
        if let Some(value) = self.active_memtable.get(&key) {
            if value.is_tombstone {
                return Err(NotFoundInDB);
            }
            return self.get_value_from_vlog(value.val_offset, value.created_at).await;
        } else {
            // Step 2: Check the read-only memtables
            let mut is_deleted = false;
            for (_, table) in self.read_only_memtables.read().await.iter() {
                if let Some(value) = table.read().await.get(&key) {
                    if value.created_at > most_recent_insert_time {
                        offset = value.val_offset;
                        most_recent_insert_time = value.created_at;
                        is_deleted = value.is_tombstone
                    }
                }
            }
            if self.found_in_table(most_recent_insert_time)  {
                if is_deleted {
                    return Err(NotFoundInDB);
                }
                return self.get_value_from_vlog(offset, most_recent_insert_time).await;
            } else {
                // Step 3: Check sstables
                let key_range = &self.key_range.read().await;
                let mut ssts = key_range.filter_sstables_by_biggest_key(&key);
                if ssts.is_empty() {
                    return Err(NotFoundInDB);
                }
                let filters = &self.filters.read().await;
                ssts = BloomFilter::ssts_within_key_range(&key, filters, &ssts);
                if ssts.is_empty() {
                    return Err(NotFoundInDB);
                }
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
                                            if created_at > most_recent_insert_time {
                                                offset = val_offset;
                                                most_recent_insert_time = created_at;
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
                if self.found_in_table(most_recent_insert_time) {
                    if is_deleted {
                        return Err(NotFoundInDB);
                    }
                    // Step 5: Read value from value log based on offset
                    return self.get_value_from_vlog(offset, most_recent_insert_time).await;
                }
            }
        }
        Err(NotFoundInDB)
    }

    pub fn found_in_table(&self, most_recent_insert_time: u64)-> bool {
        most_recent_insert_time > 0
    }

    pub async fn get_value_from_vlog(
        &self,
        offset: usize,
        creation_time: CreationTime,
    ) -> Result<(Value, CreationTime), Error> {
        let res = self.val_log.get(offset).await?;
        match res {
            Some((value, is_tombstone)) => {
                if is_tombstone {
                    return Err(KeyFoundAsTombstoneInValueLogError);
                }
                return Ok((value, creation_time));
            }
            None => return Err(KeyNotFoundInValueLogError),
        };
    }

    pub async fn update(&mut self, key: &str, value: &str) -> Result<bool, Error> {
        self.get(key).await?;
        self.put(key, value).await
    }
    // Flush all memtables
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
        for (_, table) in immutable_tables.iter() {
            let table_inner = Arc::clone(table);
            flusher.flush(table_inner).await?;
        }
        self.active_memtable.clear();
        self.read_only_memtables = Arc::new(RwLock::new(IndexMap::new()));
        Ok(())
    }

    async fn with_default_config(
        dir: DirPath,
        size_unit: SizeUnit,
        config: Config,
    ) -> Result<DataStore<'a, types::Key>, Error> {
        Self::with_capacity_and_rate(dir, size_unit, config).await
    }

    async fn with_capacity_and_rate(
        dir: DirPath,
        size_unit: SizeUnit,
        config: Config,
    ) -> Result<DataStore<'a, types::Key>, Error> {
        let vlog_path = &dir.clone().val_log;
        let buckets_path = dir.buckets.clone();
        let vlog_exit = vlog_path.exists();
        let vlog_empty = !vlog_exit || fs::metadata(vlog_path).await.map_err(GetFileMetaDataError)?.len() == 0;
        let key_range = KeyRange::new();
        let vlog = ValueLog::new(vlog_path).await?;
        let meta = Meta::new(&dir.meta);
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
    pub(crate) fn build(root_path: PathBuf) -> Self {
        let root = root_path;
        let val_log = root.join(VALUE_LOG_DIRECTORY_NAME);
        let buckets = root.join(BUCKETS_DIRECTORY_NAME);
        let meta = root.join(META_DIRECTORY_NAME);
        Self {
            root,
            val_log,
            buckets,
            meta,
        }
    }
}
