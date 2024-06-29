use std::path::Path;

use super::{storage::DirPath, DataStore, SizeUnit};

use crate::bucket::bucket::InsertableToBucket;
use crate::bucket::{Bucket, BucketID, BucketMap};
use crate::cfg::Config;
use crate::compact::{self, Compactor};
use crate::consts::{
    DEFAULT_FLUSH_SIGNAL_CHANNEL_SIZE, HEAD_ENTRY_KEY, HEAD_ENTRY_VALUE, SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8,
    TAIL_ENTRY_KEY, TAIL_ENTRY_VALUE,
};
use crate::err::Error;
use crate::err::Error::*;
use crate::filter::BloomFilter;
use crate::flush::Flusher;
use crate::gc::gc::GC;
use crate::helpers;
use crate::key_range::KeyRange;
use crate::mem::{Entry, MemTable, K};
use crate::meta::Meta;
use crate::sst::Table;
use crate::types::{Key, MemtableId};
use crate::vlog::ValueLog;
use async_broadcast::broadcast;
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use indexmap::IndexMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::fs::read_dir;
use tokio::sync::RwLock;

impl DataStore<'static, Key> {
    pub async fn recover<P: AsRef<Path> + Send + Sync + Clone>(
        dir: DirPath,
        buckets_path: P,
        mut vlog: ValueLog,
        mut key_range: KeyRange,
        config: &Config,
        size_unit: SizeUnit,
        meta: Meta,
    ) -> Result<DataStore<'static, Key>, Error> {
        let mut recovered_buckets: IndexMap<BucketID, Bucket> = IndexMap::new();
        let mut filters: Vec<BloomFilter> = Vec::new();
        let mut most_recent_head_timestamp = helpers::default_datetime();
        let mut most_recent_head_offset = 0;
        let mut most_recent_tail_timestamp = helpers::default_datetime();
        let mut most_recent_tail_offset = 0;

        // Get bucket diretories streams
        let mut buckets_stream = read_dir(buckets_path.to_owned())
            .await
            .map_err(|err| DirectoryOpenError {
                path: buckets_path.as_ref().to_path_buf(),
                error: err,
            })?;
        // for each bucket directory
        while let Some(bucket_dir) = buckets_stream.next_entry().await.map_err(|err| DirectoryOpenError {
            path: buckets_path.as_ref().to_path_buf(),
            error: err,
        })? {
            // get read stream for sstable directories stream in the bucket
            let mut sst_directories_stream =
                read_dir(bucket_dir.path().to_owned())
                    .await
                    .map_err(|err| DirectoryOpenError {
                        path: buckets_path.as_ref().to_path_buf(),
                        error: err,
                    })?;
            // iterate over each sstable directory
            while let Some(sst_dir) = sst_directories_stream
                .next_entry()
                .await
                .map_err(|err| DirectoryOpenError {
                    path: buckets_path.as_ref().to_path_buf(),
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
                    path: buckets_path.as_ref().to_path_buf(),
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
                if let Some(value) = head_entry {
                    if value.created_at > most_recent_head_timestamp {
                        most_recent_head_offset = value.val_offset;
                        most_recent_head_timestamp = value.created_at;
                    }
                }
                // update tail
                if let Some(value) = tail_entry {
                    if value.created_at > most_recent_tail_timestamp {
                        most_recent_tail_offset = value.val_offset;
                        most_recent_tail_timestamp = value.created_at;
                    }
                }
                let mut filter = Table::build_filter_from_sstable(&sstable.entries, config.false_positive_rate);
                table.entries.clear();
                filter.set_sstable(table.clone());
                filters.push(filter);
                key_range.set(data_file_path.to_owned(), smallest_key, biggest_key, table);
            }
        }
        let mut buckets_map = BucketMap::new(buckets_path.clone()).await?;
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
                let key_range = Arc::new(RwLock::new(key_range.to_owned()));
                let read_only_memtables = Arc::new(RwLock::new(read_only_memtables));
                let gc_table = Arc::new(RwLock::new(active_memtable.to_owned()));
                let gc_log = Arc::new(RwLock::new(vlog.to_owned()));
                let flusher = Flusher::new(
                    read_only_memtables.clone(),
                    buckets.clone(),
                    filters.clone(),
                    key_range.clone(),
                );
                Ok(DataStore {
                    active_memtable: active_memtable.to_owned(),
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
                        compact::CompactionReason::MaxSize,
                        config.false_positive_rate,
                    ),
                    config: config.clone(),
                    gc: GC::new(
                        config.online_gc_interval,
                        config.gc_chunk_size,
                        gc_table.clone(),
                        gc_log.clone(),
                    ),
                    read_only_memtables,
                    range_iterator: None,
                    flush_signal_tx,
                    flush_signal_rx,
                    gc_log,
                    gc_table,
                    gc_updated_entries: Arc::new(RwLock::new(SkipMap::new())),
                })
            }
            Err(err) => Err(MemTableRecoveryError(Box::new(err))),
        }
    }

    pub async fn recover_memtable<P: AsRef<Path> + Send + Sync>(
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
        vlog_path: P,
        head_offset: usize,
    ) -> Result<(MemTable<Key>, IndexMap<MemtableId, Arc<RwLock<MemTable<Key>>>>), Error> {
        let mut read_only_memtables: IndexMap<MemtableId, Arc<RwLock<MemTable<Key>>>> = IndexMap::new();
        let mut active_memtable = MemTable::with_specified_capacity_and_rate(size_unit, capacity, false_positive_rate);
        let mut vlog = ValueLog::new(vlog_path.as_ref()).await?;
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

    pub async fn handle_empty_vlog<P: AsRef<Path> + Send + Sync>(
        dir: DirPath,
        buckets_path: P,
        mut vlog: ValueLog,
        key_range: KeyRange,
        config: &Config,
        size_unit: SizeUnit,
        meta: Meta,
    ) -> Result<DataStore<'static, Key>, Error> {
        let mut active_memtable =
            MemTable::with_specified_capacity_and_rate(size_unit, config.write_buffer_size, config.false_positive_rate);
        // if ValueLog is empty then we want to insert both tail and head
        let created_at = Utc::now();
        let tail_offset = vlog
            .append(&TAIL_ENTRY_KEY.to_vec(), &TAIL_ENTRY_VALUE.to_vec(), created_at, false)
            .await?;
        let tail_entry = Entry::new(TAIL_ENTRY_KEY.to_vec(), tail_offset, created_at, false);
        let head_offset = vlog
            .append(&HEAD_ENTRY_KEY.to_vec(), &HEAD_ENTRY_VALUE.to_vec(), created_at, false)
            .await?;
        let head_entry = Entry::new(HEAD_ENTRY_KEY.to_vec(), head_offset, created_at, false);
        vlog.set_head(head_offset);
        vlog.set_tail(tail_offset);

        // insert tail and head to memtable
        active_memtable.insert(&tail_entry.to_owned())?;
        active_memtable.insert(&head_entry.to_owned())?;
        let buckets = BucketMap::new(buckets_path).await?;
        let (flush_signal_tx, flush_signal_rx) = broadcast(DEFAULT_FLUSH_SIGNAL_CHANNEL_SIZE);
        let read_only_memtables = IndexMap::new();
        let filters = Arc::new(RwLock::new(Vec::new()));
        let buckets = Arc::new(RwLock::new(buckets.to_owned()));
        let key_range = Arc::new(RwLock::new(key_range));
        let read_only_memtables = Arc::new(RwLock::new(read_only_memtables));
        let gc_table = Arc::new(RwLock::new(active_memtable.to_owned()));
        let gc_log = Arc::new(RwLock::new(vlog.to_owned()));
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
                compact::CompactionReason::MaxSize,
                config.false_positive_rate,
            ),
            config: config.clone(),
            meta,
            flusher,
            read_only_memtables,
            range_iterator: None,
            flush_signal_tx,
            flush_signal_rx,
            gc: GC::new(
                config.online_gc_interval,
                config.gc_chunk_size,
                gc_table.clone(),
                gc_log.clone(),
            ),
            gc_log,
            gc_table,
            gc_updated_entries: Arc::new(RwLock::new(SkipMap::new())),
        });
    }

    fn get_bucket_id_from_full_bucket_path<P: AsRef<Path> + Send + Sync>(full_path: P) -> String {
        let full_path_as_str = full_path.as_ref().to_string_lossy().to_string();
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
