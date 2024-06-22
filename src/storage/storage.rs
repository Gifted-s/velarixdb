use crate::bucket::bucket::InsertableToBucket;
use crate::bucket::{Bucket, BucketID, BucketMap};
use crate::cfg::Config;
use crate::compactors::{self, Compactor};
use crate::consts::{
    BUCKETS_DIRECTORY_NAME, DEFAULT_FLUSH_SIGNAL_CHANNEL_SIZE, HEAD_ENTRY_KEY, KB, META_DIRECTORY_NAME, SIZE_OF_U32,
    SIZE_OF_U64, SIZE_OF_U8, TAIL_ENTRY_KEY, TOMB_STONE_MARKER, VALUE_LOG_DIRECTORY_NAME,
};
use crate::err::Error;
use crate::err::Error::*;
use crate::filter::BloomFilter;
use crate::flusher::Flusher;
use crate::fs::{DataFileNode, DataFs, FileAsync, FileNode, IndexFileNode, IndexFs};
use crate::index::{Index, IndexFile};
use crate::key_range::KeyRange;
use crate::memtable::{Entry, MemTable};
use crate::meta::Meta;
use crate::range::RangeIterator;
use crate::sst::{self, DataFile, Table};
use crate::types::{
    self, BloomFilterHandle, BucketMapHandle, CreationTime, FlushSignal, ImmutableMemTable, Key, KeyRangeHandle,
    MemtableId, ValOffset, Value,
};
use crate::value_log::ValueLog;
use async_broadcast::broadcast;
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use indexmap::IndexMap;
use log::error;

use std::path::PathBuf;
use std::{hash::Hash, sync::Arc};
use tokio::fs::{self, read_dir};
use tokio::{spawn, sync::RwLock};

#[derive(Debug)]
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
    pub range_iterator: Option<RangeIterator<'a>>,
    pub read_only_memtables: ImmutableMemTable<K>,
    pub flush_signal_tx: async_broadcast::Sender<FlushSignal>,
    pub flush_signal_rx: async_broadcast::Receiver<FlushSignal>,
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
    }

    /// A Result indicating success or an `Error` if an error occurred.
    pub async fn put(&mut self, key: &str, value: &str, existing_val_offset: Option<ValOffset>) -> Result<bool, Error> {
        let is_tombstone = value.len() == 0;
        let key = &key.as_bytes().to_vec();
        let val = &value.as_bytes().to_vec();
        let created_at = Utc::now().timestamp_millis() as u64;

        let v_offset;
        if let Some(offset) = existing_val_offset {
            v_offset = offset;
        } else {
            v_offset = self.val_log.append(key, val, created_at, is_tombstone).await?;
        }
        let entry = Entry::new(key.to_vec(), v_offset, created_at, is_tombstone);
        if self.active_memtable.is_full(HEAD_ENTRY_KEY.len()) {
            let capacity = self.active_memtable.capacity();
            let size_unit = self.active_memtable.size_unit();
            let false_pos = self.active_memtable.false_positive_rate();
            let head_offset = self.active_memtable.entries.iter().max_by_key(|e| e.value().0);

            // reset head in vLog
            self.val_log.set_head(head_offset.to_owned().unwrap().value().0);
            let head_entry = Entry::new(
                HEAD_ENTRY_KEY.to_vec(),
                head_offset.unwrap().value().0,
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
                    spawn(async move {
                        flusher.flush_handler(id, table_inner, tx);
                    });
                }
            }
            self.active_memtable = MemTable::with_specified_capacity_and_rate(size_unit, capacity, false_pos);
        }

        self.active_memtable.insert(&entry)?;
        Ok(true)
    }

    pub async fn delete(&mut self, key: &str) -> Result<bool, Error> {
        self.get(key).await?;
        let value = TOMB_STONE_MARKER;
        self.put(key, value, None).await
    }

    pub async fn get(&self, key: &str) -> Result<(Value, CreationTime), Error> {
        let key = key.as_bytes().to_vec();
        let mut offset = 0;
        let mut most_recent_insert_time = 0;
        // Step 1: Check the active memtable
        if let Some((val_offset, inserted_at, is_tombstone)) = self.active_memtable.get(&key) {
            if is_tombstone {
                return Err(NotFoundInDB);
            }
            return self.get_value_from_vlog(val_offset, inserted_at).await;
        } else {
            // Step 2: Check the read-only memtables
            let mut is_deleted = false;
            for (_, table) in self.read_only_memtables.read().await.iter() {
                if let Some((val_offset, insert_at, is_tombstone)) = table.read().await.get(&key) {
                    if insert_at > most_recent_insert_time {
                        offset = val_offset;
                        most_recent_insert_time = insert_at;
                        is_deleted = is_tombstone
                    }
                }
            }
            if most_recent_insert_time > 0 {
                if is_deleted {
                    return Err(NotFoundInDB);
                }
                return self.get_value_from_vlog(offset, most_recent_insert_time).await;
            } else {
                // Step 3: Check sstables
                let key_range = &self.key_range.read().await;
                let mut ssts = key_range.filter_sstables_by_biggest_key(&key);
                if ssts.is_empty() {
                    return Err(KeyNotFoundInAnySSTableError);
                }
                let filters = &self.filters.read().await;
                ssts = BloomFilter::ssts_within_key_range(&key, filters, &ssts);
                if ssts.is_empty() {
                    return Err(KeyNotFoundByAnyBloomFilterError);
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
                if most_recent_insert_time > 0 {
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
        self.put(key, value, None).await
    }

    pub async fn clear(&'a mut self) -> Result<DataStore<'a, types::Key>, Error> {
        let size_unit = self.active_memtable.size_unit();
        self.active_memtable.clear();
        self.buckets.write().await.clear_all().await;
        self.val_log.clear_all().await;
        DataStore::with_capacity_and_rate(self.dir.clone(), size_unit, self.config.to_owned()).await
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
        let mut key_range = KeyRange::new();
        let mut vlog = ValueLog::new(vlog_path).await?;
        let meta = Meta::new(&dir.meta);
        if vlog_empty {
            return DataStore::handle_empty_vlog(dir, buckets_path, vlog, key_range, &config, size_unit, meta).await;
        }
        let mut recovered_buckets: IndexMap<BucketID, Bucket> = IndexMap::new();
        let mut filters: Vec<BloomFilter> = Vec::new();
        let mut most_recent_head_timestamp = 0;
        let mut most_recent_head_offset = 0;
        let mut most_recent_tail_timestamp = 0;
        let mut most_recent_tail_offset = 0;

        // Get bucket diretories streams
        let mut buckets_stream = read_dir(buckets_path.to_owned())
            .await
            .map_err(|err| DirectoryOpenError {
                path: buckets_path.to_owned(),
                error: err,
            })?;
        // for each bucket directory
        while let Some(bucket_dir) = buckets_stream.next_entry().await.map_err(|err| DirectoryOpenError {
            path: buckets_path.to_owned(),
            error: err,
        })? {
            // get read stream for sstable directories stream in the bucket
            let mut sst_directories_stream =
                read_dir(bucket_dir.path().to_owned())
                    .await
                    .map_err(|err| DirectoryOpenError {
                        path: buckets_path.to_owned(),
                        error: err,
                    })?;
            // iterate over each sstable directory
            while let Some(sst_dir) = sst_directories_stream
                .next_entry()
                .await
                .map_err(|err| DirectoryOpenError {
                    path: buckets_path.to_owned(),
                    error: err,
                })?
            {
                // get read stream for files in the sstable directory
                let files_read_stream = read_dir(sst_dir.path()).await.map_err(|err| FileOpenError {
                    path: sst_dir.path(),
                    error: err,
                });
                let mut sst_files = Vec::new();
                let mut reader = files_read_stream.unwrap();
                // iterate over each file
                while let Some(file) = reader.next_entry().await.map_err(|err| DirectoryOpenError {
                    path: buckets_path.to_owned(),
                    error: err,
                })? {
                    let file_path = file.path();
                    if file_path.is_file() {
                        sst_files.push(file_path);
                    }
                }
                // Sort to make order deterministic
                sst_files.sort();
                let bucket_id = Self::get_bucket_id_from_full_bucket_path(sst_dir.path());
                if sst_files.len() < 2 {
                    return Err(InvalidSSTableDirectoryError {
                        input_string: sst_dir.path().to_owned().to_string_lossy().to_string(),
                    });
                }
                let data_file_path = sst_files[0].to_owned();
                let index_file_path = sst_files[1].to_owned();
                let table = Table::build_from(
                    sst_dir.path().to_owned(),
                    data_file_path.to_owned(),
                    index_file_path.to_owned(),
                )
                .await;
                let bucket_uuid = uuid::Uuid::parse_str(&bucket_id).map_err(|err| InvaidUUIDParseString {
                    input_string: bucket_id,
                    error: err,
                })?;

                if let Some(b) = recovered_buckets.get(&bucket_uuid) {
                    let temp_sstables = b.sstables.clone();
                    temp_sstables.write().await.push(table.clone());
                    let updated_bucket =
                        Bucket::from(bucket_dir.path(), bucket_uuid, temp_sstables.read().await.clone(), 0).await?;
                    recovered_buckets.insert(bucket_uuid, updated_bucket);
                } else {
                    // Create new bucket
                    let updated_bucket = Bucket::from(bucket_dir.path(), bucket_uuid, vec![table.clone()], 0).await?;
                    recovered_buckets.insert(bucket_uuid, updated_bucket);
                }

                let sstable = table.load_entries_from_file().await?;
                let head_entry = sstable.get_value_from_entries(HEAD_ENTRY_KEY);
                let tail_entry = sstable.get_value_from_entries(TAIL_ENTRY_KEY);
                let biggest_key = sstable.find_biggest_key().unwrap();
                let smallest_key = sstable.find_smallest_key().unwrap();
                // update head
                if let Some((head_offset, date_created, _)) = head_entry {
                    if date_created > most_recent_head_timestamp {
                        most_recent_head_offset = head_offset;
                        most_recent_head_timestamp = date_created;
                    }
                }
                // update tail
                if let Some((tail_offset, date_created, _)) = tail_entry {
                    if date_created > most_recent_tail_timestamp {
                        most_recent_tail_offset = tail_offset;
                        most_recent_tail_timestamp = date_created;
                    }
                }
                let mut filter = Table::build_filter_from_sstable(&sstable.entries);
                table.entries.clear();
                filter.set_sstable(table.clone());
                filters.push(filter);
                key_range.set(data_file_path.to_owned(), smallest_key, biggest_key, table);
            }
        }
        let mut buckets_map = BucketMap::new(buckets_path.clone());
        for (bucket_id, bucket) in recovered_buckets.iter() {
            buckets_map.buckets.insert(*bucket_id, bucket.clone());
        }
        vlog.set_head(most_recent_head_offset);
        vlog.set_tail(most_recent_tail_offset);

        let recover_res = DataStore::recover_memtable(
            size_unit,
            config.write_buffer_size,
            config.false_positive_rate,
            &dir.val_log,
            most_recent_head_offset,
        )
        .await;
        let (flush_signal_tx, flush_signal_rx) = broadcast(DEFAULT_FLUSH_SIGNAL_CHANNEL_SIZE);
        match recover_res {
            Ok((active_memtable, read_only_memtables)) => {
                let buckets = Arc::new(RwLock::new(buckets_map.to_owned()));
                let filters = Arc::new(RwLock::new(filters));
                //TODO:  we also need to recover this from sstable
                let key_range = Arc::new(RwLock::new(key_range.to_owned()));
                let read_only_memtables = Arc::new(RwLock::new(read_only_memtables));

                let flusher = Flusher::new(
                    read_only_memtables.clone(),
                    buckets.clone(),
                    filters.clone(),
                    key_range.clone(),
                );

                Ok(DataStore {
                    active_memtable,
                    val_log: vlog,
                    dir,
                    buckets,
                    filters,
                    key_range,
                    meta,
                    flusher,
                    compactor: Compactor::new(
                        config.enable_ttl,
                        config.entry_ttl_millis,
                        config.tombstone_ttl,
                        config.background_compaction_interval,
                        config.compactor_flush_listener_interval,
                        config.tombstone_compaction_interval,
                        config.compaction_strategy,
                        compactors::CompactionReason::MaxSize,
                        config.false_positive_rate,
                    ),
                    config: config.clone(),
                    read_only_memtables,
                    range_iterator: None,
                    flush_signal_tx,
                    flush_signal_rx,
                })
            }
            Err(err) => Err(MemTableRecoveryError(Box::new(err))),
        }
    }

    async fn recover_memtable(
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
        vlog_path: &PathBuf,
        head_offset: usize,
    ) -> Result<(MemTable<Key>, IndexMap<MemtableId, Arc<RwLock<MemTable<Key>>>>), Error> {
        let mut read_only_memtables: IndexMap<MemtableId, Arc<RwLock<MemTable<Key>>>> = IndexMap::new();
        let mut active_memtable = MemTable::with_specified_capacity_and_rate(size_unit, capacity, false_positive_rate);
        let mut vlog = ValueLog::new(&vlog_path.clone()).await?;
        let mut most_recent_offset = head_offset;
        let entries = vlog.recover(head_offset).await?;

        for e in entries {
            let entry = Entry::new(e.key.to_owned(), most_recent_offset, e.created_at, e.is_tombstone);
            // Since the most recent offset is the offset we start reading entries from in value log
            // and we retrieved this from the sstable, therefore should not re-write the initial entry in
            // memtable since it's already in the sstable
            if most_recent_offset != head_offset {
                if active_memtable.is_full(e.key.len()) {
                    // Make memtable read only
                    active_memtable.read_only = true;
                    read_only_memtables.insert(
                        MemTable::generate_table_id(),
                        Arc::new(RwLock::new(active_memtable.to_owned())),
                    );
                    active_memtable =
                        MemTable::with_specified_capacity_and_rate(size_unit, capacity, false_positive_rate);
                }
                active_memtable.insert(&entry)?;
            }
            most_recent_offset += SIZE_OF_U32   // Key Size(for fetching key length)
                        +SIZE_OF_U32            // Value Length(for fetching value length)
                        + SIZE_OF_U64           // Date Length
                        + SIZE_OF_U8            // tombstone marker
                        + e.key.len()           // Key Length
                        + e.value.len(); // Value Length
        }
        Ok((active_memtable, read_only_memtables))
    }

    pub async fn handle_empty_vlog(
        dir: DirPath,
        buckets_path: PathBuf,
        mut vlog: ValueLog,
        key_range: KeyRange,
        config: &Config,
        size_unit: SizeUnit,
        meta: Meta,
    ) -> Result<DataStore<'a, types::Key>, Error> {
        let mut active_memtable =
            MemTable::with_specified_capacity_and_rate(size_unit, config.write_buffer_size, config.false_positive_rate);
        // if ValueLog is empty then we want to insert both tail and head
        let created_at = Utc::now().timestamp_millis() as u64;
        let tail_offset = vlog
            .append(&TAIL_ENTRY_KEY.to_vec(), &vec![], created_at, false)
            .await?;
        let tail_entry = Entry::new(TAIL_ENTRY_KEY.to_vec(), tail_offset, created_at, false);
        let head_offset = vlog
            .append(&HEAD_ENTRY_KEY.to_vec(), &vec![], created_at, false)
            .await?;
        let head_entry = Entry::new(HEAD_ENTRY_KEY.to_vec(), head_offset, created_at, false);
        vlog.set_head(head_offset);
        vlog.set_tail(tail_offset);

        // insert tail and head to memtable
        active_memtable.insert(&tail_entry.to_owned())?;
        active_memtable.insert(&head_entry.to_owned())?;
        let buckets = BucketMap::new(buckets_path);
        let (flush_signal_tx, flush_signal_rx) = broadcast(DEFAULT_FLUSH_SIGNAL_CHANNEL_SIZE);
        let read_only_memtables = IndexMap::new();
        let filters = Arc::new(RwLock::new(Vec::new()));
        let buckets = Arc::new(RwLock::new(buckets.to_owned()));
        let key_range = Arc::new(RwLock::new(key_range));
        let read_only_memtables = Arc::new(RwLock::new(read_only_memtables));

        let flusher = Flusher::new(
            read_only_memtables.clone(),
            buckets.clone(),
            filters.clone(),
            key_range.clone(),
        );

        return Ok(DataStore {
            active_memtable,
            val_log: vlog,
            filters,
            buckets,
            dir,
            key_range,
            compactor: Compactor::new(
                config.enable_ttl,
                config.entry_ttl_millis,
                config.tombstone_ttl,
                config.background_compaction_interval,
                config.compactor_flush_listener_interval,
                config.tombstone_compaction_interval,
                config.compaction_strategy,
                compactors::CompactionReason::MaxSize,
                config.false_positive_rate,
            ),
            config: config.clone(),
            meta,
            flusher,
            read_only_memtables,
            range_iterator: None,
            flush_signal_tx,
            flush_signal_rx,
        });
    }
    // Flush all memtables
    pub async fn flush_all_memtables(&mut self) -> Result<(), Error> {
        // Flush active memtable
        let hotness = 1;
        self.flush_memtable(Arc::new(RwLock::new(self.active_memtable.to_owned())), hotness)
            .await?;
        let readonly_memtables = self.read_only_memtables.read().await.to_owned();
        // TODO: handle with multiple threads
        for (_, memtable) in readonly_memtables.iter() {
            self.flush_memtable(memtable.clone(), hotness).await?;
        }
        drop(readonly_memtables);
        // Sort filters by hotness after flushing read-only memtables
        self.filters
            .write()
            .await
            .sort_by(|a, b| b.get_sst().get_hotness().cmp(&a.get_sst().get_hotness()));
        self.active_memtable.clear();
        self.read_only_memtables = Arc::new(RwLock::new(IndexMap::new()));
        Ok(())
    }

    async fn flush_memtable(&mut self, memtable: Arc<RwLock<MemTable<Key>>>, _: u64) -> Result<(), Error> {
        let sstable_path = self
            .buckets
            .write()
            .await
            .insert_to_appropriate_bucket(Arc::new(Box::new(memtable.read().await.to_owned())))
            .await?;

        let mut filter = memtable.read().await.get_bloom_filter();
        filter.set_sstable(sstable_path.clone());
        self.filters.write().await.push(filter);
        let biggest_key = memtable.read().await.find_biggest_key()?;
        let smallest_key = memtable.read().await.find_smallest_key()?;
        self.key_range.write().await.set(
            sstable_path.get_data_file_path(),
            smallest_key,
            biggest_key,
            sstable_path,
        );
        Ok(())
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

    fn get_bucket_id_from_full_bucket_path(full_path: PathBuf) -> String {
        let full_path_as_str = full_path.to_string_lossy().to_string();
        let mut bucket_id = String::new();
        // Find the last occurrence of "bucket" in the file path
        if let Some(idx) = full_path_as_str.rfind("bucket") {
            // Extract the substring starting from the index after the last occurrence of "bucket"
            let uuid_part = &full_path_as_str[idx + "bucket".len()..];
            if let Some(end_idx) = uuid_part.find('/') {
                // Extract the UUID
                let uuid = &uuid_part[..end_idx];
                bucket_id = uuid.to_string();
            }
        }
        bucket_id
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

#[cfg(test)]
mod tests {

    use crate::consts::DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL_MILLI;
    use crate::utils;

    use super::*;
    use futures::future::join_all;
    use log::info;
    use tokio::time::{sleep, Duration};

    // fn init() {
    //     let res = env_logger::builder().is_test(true).try_init();
    //     match res {
    //         Ok(_) => {}
    //         Err(err) => println!("err {}", err),
    //     }
    // }

    // Generate test to find keys after compaction
    #[tokio::test]
    async fn datastore_create_asynchronous() {
        let path = PathBuf::new().join("bump1");
        let s_engine = DataStore::new(path.clone()).await.unwrap();

        // // Specify the number of random strings to generate
        let num_strings = 20000; // 100k

        // Specify the length of each random string
        let string_length = 5;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::with_capacity(num_strings);
        for _ in 0..num_strings {
            let random_string = utils::generate_random_id(string_length);
            random_strings.push(random_string);
        }
        // for k in random_strings.clone() {
        //    s_engine.put(&k, "boyode", None).await.unwrap();
        // }

        let sg = Arc::new(RwLock::new(s_engine));

        let tasks = random_strings.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                value.put(&k, "boy", None).await
            })
        });

        let all_results = join_all(tasks).await;
        for tokio_response in all_results {
            match tokio_response {
                Ok(entry) => match entry {
                    Ok(is_inserted) => {
                        assert_eq!(is_inserted, true)
                    }
                    Err(err) => assert!(false, "{}", err.to_string()),
                },
                Err(err) => {
                    assert!(false, "{}", err.to_string())
                }
            }
        }
        println!("Write completed ");
        sleep(Duration::from_millis(
            DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL_MILLI * 3,
        ))
        .await;
        println!("About to start reading");
        // println!("Compaction completed !");
        random_strings.sort();
        let tasks = random_strings
            .get(0..(num_strings / 2))
            .unwrap_or_default()
            .iter()
            .map(|k| {
                let s_engine = Arc::clone(&sg);
                let key = k.clone();
                tokio::spawn(async move {
                    let value = s_engine.read().await;
                    let nn = value.get(&key).await;
                    return nn;
                })
            });
        let all_results = join_all(tasks).await;
        for tokio_response in all_results {
            match tokio_response {
                Ok(entry) => match entry {
                    Ok(v) => {
                        assert_eq!(v.0, b"boy");
                    }
                    Err(err) => assert!(false, "Error: {}", err.to_string()),
                },
                Err(err) => {
                    assert!(false, "{}", err.to_string())
                }
            }
        }

        //  let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn datastore_create_synchronous() {
        let path = PathBuf::new().join("bump2");
        let mut s_engine = DataStore::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 1000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = utils::generate_random_id(string_length);
            random_strings.push(random_string);
        }

        // Insert the generated random strings
        for (_, s) in random_strings.iter().enumerate() {
            s_engine.put(s, "boyode", None).await.unwrap();
        }
        // let compactor = Compactor::new();

        let compaction_opt = s_engine.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    s_engine.buckets.read().await.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    s_engine.filters.read().await.len()
                );
            }
            Err(err) => {
                println!("Error during compaction {}", err)
            }
        }

        // random_strings.sort();
        for k in random_strings {
            let result = s_engine.get(&k).await;
            match result {
                Ok((value, _)) => {
                    assert_eq!(value, b"boyode");
                }
                Err(_) => {
                    assert!(false, "No err should be found");
                }
            }
        }

        let _ = fs::remove_dir_all(path.clone()).await;
        // sort to make fetch random
    }

    #[tokio::test]
    async fn datastore_compaction_asynchronous() {
        let path = PathBuf::new().join("bump3");
        let s_engine = DataStore::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 50000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = utils::generate_random_id(string_length);
            random_strings.push(random_string);
        }
        // for k in random_strings.clone() {
        //     s_engine.put(&k, "boyode").await.unwrap();
        // }
        let sg = Arc::new(RwLock::new(s_engine));
        let binding = random_strings.clone();
        let tasks = binding.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                value.put(&k, "boyode", None).await
            })
        });

        //Collect the results from the spawned tasks
        for task in tasks {
            tokio::select! {
                result = task => {
                    match result{
                        Ok(v_opt)=>{
                            match v_opt{
                                Ok(v) => {
                                    assert_eq!(v, true)
                                },
                                Err(_) => { assert!(false, "No err should be found")},
                            }
                             }
                        Err(_) =>  assert!(false, "No err should be found") }
                    //println!("{:?}",result);
                }
            }
        }

        // sort to make fetch random
        random_strings.sort();
        let key = &random_strings[0];

        let get_res1 = sg.read().await.get(key).await;
        let get_res2 = sg.read().await.get(key).await;
        let get_res3 = sg.read().await.get(key).await;
        let get_res4 = sg.read().await.get(key).await;
        match get_res1 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        match get_res2 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        match get_res3 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }
        match get_res4 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        let del_res = sg.write().await.delete(key).await;
        match del_res {
            Ok(v) => {
                assert_eq!(v, true)
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        let get_res2 = sg.read().await.get(key).await;
        match get_res2 {
            Ok(_) => {
                assert!(false, "Should not be found after compaction")
            }
            Err(err) => {
                assert_eq!(Error::NotFoundInDB.to_string(), err.to_string())
            }
        }

        let _ = sg.write().await.flush_all_memtables().await;
        sg.write().await.active_memtable.clear();

        // We expect tombstone to be flushed to an sstable at this point
        let get_res2 = sg.read().await.get(key).await;
        match get_res2 {
            Ok(_) => {
                assert!(false, "Should not be found after compaction")
            }
            Err(err) => {
                assert_eq!(Error::NotFoundInDB.to_string(), err.to_string())
            }
        }

        let compaction_opt = sg.write().await.run_compaction().await;
        // Insert the generated random strings
        // let compactor = Compactor::new();
        // let compaction_opt = sg.write().await.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    sg.read().await.buckets.read().await.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    sg.read().await.filters.read().await.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }

        // Insert the generated random strings
        let get_res3 = sg.read().await.get(key).await;
        match get_res3 {
            Ok(_) => {
                assert!(false, "Deleted key should be found as tumbstone");
            }

            Err(err) => {
                println!("{}", err);
                if err.to_string() != NotFoundInDB.to_string() && err.to_string() != NotFoundInDB.to_string() {
                    println!("{}", err);
                    assert!(false, "Key should be mapped to tombstone or deleted from all sstables")
                }
            }
        }
        let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn datastore_update_asynchronous() {
        let path = PathBuf::new().join("bump4");
        let mut s_engine = DataStore::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 6000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = utils::generate_random_id(string_length);
            random_strings.push(random_string);
        }
        for k in random_strings.clone() {
            s_engine.put(&k, "boyode", None).await.unwrap();
        }
        let sg = Arc::new(RwLock::new(s_engine));
        let binding = random_strings.clone();
        let tasks = binding.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                value.put(&k, "boyode", None).await
            })
        });

        // Collect the results from the spawned tasks
        for task in tasks {
            tokio::select! {
                result = task => {
                    match result{
                        Ok(v_opt)=>{
                            match v_opt{
                                Ok(v) => {
                                    assert_eq!(v, true)
                                },
                                Err(_) => { assert!(false, "No err should be found")},
                            }
                             }
                        Err(_) =>  assert!(false, "No err should be found") }
                }
            }
        }
        // // sort to make fetch random
        random_strings.sort();
        let key = &random_strings[0];
        let updated_value = "updated_key";

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        let update_res = sg.write().await.update(key, updated_value).await;
        match update_res {
            Ok(v) => {
                assert_eq!(v, true)
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }
        let _ = sg.write().await.flush_all_memtables().await;
        sg.write().await.active_memtable.clear();

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((value, _)) => {
                assert_eq!(value, updated_value.as_bytes().to_vec())
            }
            Err(_) => {
                assert!(false, "Should not run")
            }
        }

        // // Run compaction
        let compaction_opt = sg.write().await.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    sg.read().await.buckets.read().await.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    sg.read().await.filters.read().await.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((value, _)) => {
                assert_eq!(value, updated_value.as_bytes().to_vec())
            }
            Err(_) => {
                assert!(false, "Should not run")
            }
        }
        let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn datastore_deletion_asynchronous() {
        let path = PathBuf::new().join("bump5");
        let s_engine = DataStore::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 50000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = utils::generate_random_id(string_length);
            random_strings.push(random_string);
        }
        // for k in random_strings.clone() {
        //     s_engine.put(&k, "boyode").await.unwrap();
        // }
        let sg = Arc::new(RwLock::new(s_engine));
        let binding = random_strings.clone();
        let tasks = binding.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                value.put(&k, "boyode", None).await
            })
        });
        let key = "aunkanmi";
        let _ = sg.write().await.put(key, "boyode", None).await;
        // // Collect the results from the spawned tasks
        for task in tasks {
            tokio::select! {
                result = task => {
                    match result{
                        Ok(v_opt)=>{
                            match v_opt{
                                Ok(v) => {
                                    assert_eq!(v, true)
                                },
                                Err(_) => { assert!(false, "No err should be found")},
                            }
                             }
                        Err(_) =>  assert!(false, "No err should be found") }
                }
            }
        }
        // sort to make fetch random
        random_strings.sort();
        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((value, _)) => {
                assert_eq!(value, "boyode".as_bytes().to_vec());
            }
            Err(err) => {
                assert_ne!(key.as_bytes().to_vec(), err.to_string().as_bytes().to_vec());
            }
        }

        let del_res = sg.write().await.delete(key).await;
        match del_res {
            Ok(v) => {
                assert_eq!(v, true);
            }
            Err(err) => {
                assert!(err.to_string().is_empty())
            }
        }

        let _ = sg.write().await.flush_all_memtables().await;

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((v, d)) => {
                println!("{:?} {}", v, d);
                assert!(false, "Should not be executed")
            }
            Err(err) => {
                assert_eq!(NotFoundInDB.to_string(), err.to_string())
            }
        }

        let compaction_opt = sg.write().await.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    sg.read().await.buckets.read().await.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    sg.read().await.filters.read().await.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }

        // Insert the generated random strings
        println!("trying to get this after compaction {}", key);
        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((G, R)) => {
                println!("{:?}. {:?}", G, R);
                assert!(false, "Should not ne executed")
            }
            Err(err) => {
                if err.to_string() != NotFoundInDB.to_string()
                    && err.to_string() != KeyNotFoundByAnyBloomFilterError.to_string()
                {
                    assert!(false, "Key should be mapped to tombstone or deleted from all sstables")
                }
            }
        }
        let _ = fs::remove_dir_all(path.clone()).await;
    }
}
