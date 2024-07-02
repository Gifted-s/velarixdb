use crate::cfg::Config;
use crate::compactors::Compactor;
use crate::consts::{
    BUCKETS_DIRECTORY_NAME, HEAD_ENTRY_KEY, KB, META_DIRECTORY_NAME, TOMB_STONE_MARKER, VALUE_LOG_DIRECTORY_NAME,
};
use crate::err::Error;
use crate::err::Error::*;
use crate::filter::BloomFilter;

use crate::flush::Flusher;
use crate::gc::gc::GC;
use crate::helpers;
use crate::index::Index;
use crate::key_range::KeyRange;
use crate::memtable::{Entry, MemTable, K};
use crate::meta::Meta;
use crate::range::RangeIterator;
use crate::types::{
    BloomFilterHandle, Bool, BucketMapHandle, CreatedAt, FlushSignal, GCUpdatedEntries, ImmutableMemTable, Key,
    KeyRangeHandle, Value,
};
use crate::vlog::ValueLog;
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
    pub dir: DirPath,
    pub active_memtable: MemTable<Key>,
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
    pub read_only_memtables: ImmutableMemTable<Key>,
    pub flush_signal_tx: async_broadcast::Sender<FlushSignal>,
    pub flush_signal_rx: async_broadcast::Receiver<FlushSignal>,
    pub gc_updated_entries: GCUpdatedEntries<Key>,
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

impl DataStore<'static, Key> {
    pub async fn new<P: AsRef<Path> + Send + Sync>(dir: P) -> Result<DataStore<'static, Key>, Error> {
        let dir = DirPath::build(dir);
        let default_config = Config::default();
        let store = DataStore::with_default_config(dir.to_owned(), SizeUnit::Bytes, default_config).await?;
        store.start_background_jobs();
        return Ok(store);
    }

    pub async fn new_with_custom_config<P: AsRef<Path> + Send + Sync>(
        dir: P,
        config: Config,
    ) -> Result<DataStore<'static, Key>, Error> {
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

        self.gc
            .start_background_gc_task(Arc::clone(&self.key_range), Arc::clone(&self.read_only_memtables));
    }

    pub async fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, val: V) -> Result<Bool, Error> {
        if !self.gc_updated_entries.read().await.is_empty() {
            if let Err(err) = self.sync_gc_update_with_store().await {
                log::error!("{}", err)
            }
        }
        // This ensures that sstables in key range map whose filter is newly loaded(after crash) are mapped to the sstable
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
            let gc_log = Arc::clone(&self.gc_log);
            tokio::spawn(async move {
                (gc_log.write().await).head_offset = head_offset;
            });

            let head_entry = Entry::new(HEAD_ENTRY_KEY.to_vec(), head_offset, Utc::now(), false);
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
                self.update_meta_background();
            }
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
        self.active_memtable.insert(&entry)?;
        let gc_table = Arc::clone(&self.gc_table);
        tokio::spawn(async move { gc_table.write().await.insert(&entry) });
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
        let mut offset = 0;
        let mut most_recent_insert_time = helpers::default_datetime();
        let lowest_insert_date = helpers::default_datetime();
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
                    if value.created_at > most_recent_insert_time {
                        offset = value.val_offset;
                        most_recent_insert_time = value.created_at;
                        is_deleted = value.is_tombstone
                    }
                }
            }
            if self.found_in_table(most_recent_insert_time, lowest_insert_date) {
                if is_deleted {
                    return Err(NotFoundInDB);
                }
                return self.get_value_from_vlog(offset, most_recent_insert_time).await;
            } else {
                // // Step 3: Check sstables
                let key_range = &self.key_range.read().await;
                let ssts = key_range.filter_sstables_by_biggest_key(key.as_ref()).await?;
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
                if self.found_in_table(most_recent_insert_time, lowest_insert_date) {
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

    pub fn found_in_table(&self, most_recent_insert_time: CreatedAt, lowest_insert_date: CreatedAt) -> bool {
        most_recent_insert_time > lowest_insert_date
    }

    pub async fn get_value_from_vlog(
        &self,
        offset: usize,
        creation_time: CreatedAt,
    ) -> Result<(Value, CreatedAt), Error> {
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

    pub async fn update<T: AsRef<[u8]>>(&mut self, key: T, value: T) -> Result<bool, Error> {
        self.get(key.as_ref()).await?;
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
