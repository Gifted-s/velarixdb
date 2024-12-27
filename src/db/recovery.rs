use std::collections::HashSet;

use super::{store::DirPath, DataStore, SizeUnit};

use crate::bucket::{Bucket, BucketID, BucketMap};
use crate::cfg::Config;
use crate::compactors::{self, Compactor, IntervalParams, TtlParams};
use crate::consts::{
    DEFAULT_DB_NAME, DEFAULT_FLUSH_SIGNAL_CHANNEL_SIZE, HEAD_ENTRY_KEY, HEAD_ENTRY_VALUE, SIZE_OF_U32,
    SIZE_OF_U64, SIZE_OF_U8, TAIL_ENTRY_KEY, TAIL_ENTRY_VALUE,
};
use crate::err::Error;
use crate::err::Error::*;
use crate::filter::BloomFilter;
use crate::flush::Flusher;
use crate::fs::{FileAsync, P};
use crate::gc::garbage_collector::GC;
use crate::key_range::KeyRange;
use crate::memtable::{Entry, MemTable};
use crate::meta::Meta;
use crate::open_dir_stream;
use crate::sst::{Summary, Table};
use crate::types::{ImmutableMemTablesLockFree, Key};
use crate::vlog::ValueLog;
use async_broadcast::broadcast;
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use indexmap::IndexMap;
use std::sync::Arc;
use tokio::fs::read_dir;
use tokio::sync::RwLock;

/// Parameters to create an empty ['DataStore'] or recover to previous state before crash
pub struct CreateOrRecoverStoreParams<'a, P> {
    pub dir: &'a DirPath,
    pub buckets_path: P,
    pub vlog: ValueLog,
    pub key_range: KeyRange,
    pub config: Config,
    pub size_unit: SizeUnit,
    pub meta: Meta,
}

impl DataStore<'static, Key> {
    /// Recovers [`DataStore`] state after crash
    ///
    /// Errors
    ///
    /// Returns error incase there is an IO error
    pub async fn recover(
        params: CreateOrRecoverStoreParams<'_, impl P>,
    ) -> Result<DataStore<'static, Key>, Error> {
        let (buckets_path, dir, mut vlog, key_range, config, size_unit, mut meta) = (
            params.buckets_path,
            params.dir,
            params.vlog,
            params.key_range,
            params.config,
            params.size_unit,
            params.meta,
        );

        let mut recovered_buckets: IndexMap<BucketID, Bucket> = IndexMap::new();
        // Get bucket diretories streams
        let mut buckets_stream = open_dir_stream!(buckets_path.as_ref().to_path_buf());
        // for each bucket directory
        while let Some(bucket_dir) = buckets_stream.next_entry().await.map_err(|err| DirOpen {
            path: buckets_path.as_ref().to_path_buf(),
            error: err,
        })? {
            // get read stream for sstable directories stream in the bucket
            let mut sst_dir_stream = open_dir_stream!(bucket_dir.path());

            // iterate over each sstable directory
            while let Some(sst_dir) = sst_dir_stream.next_entry().await.map_err(|err| DirOpen {
                path: buckets_path.as_ref().to_path_buf(),
                error: err,
            })? {
                // get read stream for files in the sstable directory
                let mut files_stream = open_dir_stream!(sst_dir.path());
                let mut files = Vec::new();

                // iterate over each file
                while let Some(file) = files_stream.next_entry().await.map_err(|err| DirOpen {
                    path: buckets_path.as_ref().to_path_buf(),
                    error: err,
                })? {
                    let file_path = file.path();
                    if file_path.is_file() {
                        files.push(file_path);
                    }
                }
                // Sort to make order deterministic
                files.sort();
                let bucket_id = Self::get_bucket_id_from_full_bucket_path(sst_dir.path());

                if files.len() < 4 {
                    return Err(InvalidSSTableDirectory {
                        input_string: sst_dir.path().to_owned().to_string_lossy().to_string(),
                    });
                }

                let data_file_path = files[0].to_owned();
                let filter_file_path = files[1].to_owned();
                let index_file_path = files[2].to_owned();
                let _summary_file_path = files[3].to_owned();

                let mut table = Table::build_from(
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
                    let updated_bucket = Bucket::from(
                        bucket_dir.path(),
                        bucket_uuid,
                        temp_sstables.read().await.clone(),
                        0,
                    )
                    .await?;
                    recovered_buckets.insert(bucket_uuid, updated_bucket);
                } else {
                    // Create new bucket
                    let updated_bucket =
                        Bucket::from(bucket_dir.path(), bucket_uuid, vec![table.clone()], 0).await?;
                    recovered_buckets.insert(bucket_uuid, updated_bucket);
                }

                // recover summary
                let mut summary = Summary::new(sst_dir.path());
                summary.recover().await?;
                table.summary = Some(summary.to_owned());

                // store bloomfilter metadata in table
                let new_filter = BloomFilter {
                    file_path: Some(filter_file_path),
                    ..Default::default()
                };
                table.filter = Some(new_filter);

                key_range
                    .set(sst_dir.path(), summary.smallest_key, summary.biggest_key, table)
                    .await;
            }
        }
        let mut buckets_map = BucketMap::new(buckets_path.as_ref()).await?;
        for (bucket_id, bucket) in recovered_buckets.iter() {
            buckets_map.buckets.insert(*bucket_id, bucket.clone());
        }
        if meta.file_handle.file.node.size().await > 0 {
            meta.recover().await?;
            vlog.set_head(meta.v_log_head);
            vlog.set_tail(meta.v_log_tail);
        } else {
            // if meta is empty then no flush has happened before crash
            // therefore read from the beginning of vlog
            vlog.set_head(
                SIZE_OF_U32               // tail key length 
                +SIZE_OF_U32              // tail value length
                + SIZE_OF_U64             // date Length
                + SIZE_OF_U8              // tombstone marker
                + TAIL_ENTRY_KEY.len()    // tail key
                + TAIL_ENTRY_VALUE.len(), // tail value
            );
            vlog.set_tail(0);
        }

        let recover_res = DataStore::recover_memtable(
            size_unit,
            config.write_buffer_size,
            config.false_positive_rate,
            &dir.val_log,
            vlog.head_offset,
        )
        .await;
        let (flush_signal_tx, flush_signal_rx) = broadcast(DEFAULT_FLUSH_SIGNAL_CHANNEL_SIZE);
        match recover_res {
            Ok((active_memtable, read_only_memtables)) => {
                let buckets = Arc::new(RwLock::new(buckets_map.to_owned()));
                let key_range = Arc::new(key_range.to_owned());
                let read_only_memtables = Arc::new(read_only_memtables);
                let gc_table = Arc::new(RwLock::new(active_memtable.to_owned()));
                let gc_log = Arc::new(RwLock::new(vlog.to_owned()));
                let flusher = Flusher::new(read_only_memtables.clone(), buckets.clone(), key_range.clone());
                let gc_updated_entries = Arc::new(RwLock::new(SkipMap::new()));
                Ok(DataStore {
                    keyspace: DEFAULT_DB_NAME,
                    active_memtable: active_memtable.to_owned(),
                    val_log: vlog,
                    dir: dir.to_owned(),
                    buckets,
                    key_range,
                    meta: meta.to_owned(),
                    flusher,
                    compactor: Compactor::new(
                        config.enable_ttl,
                        TtlParams {
                            entry_ttl: config.entry_ttl,
                            tombstone_ttl: config.tombstone_ttl,
                        },
                        IntervalParams {
                            background_interval: config.background_compaction_interval,
                            flush_listener_interval: config.compactor_flush_listener_interval,
                            tombstone_compaction_interval: config.tombstone_compaction_interval,
                        },
                        config.compaction_strategy,
                        compactors::CompactionReason::MaxSize,
                        config.false_positive_rate,
                    ),
                    config: config.clone(),
                    gc: GC::new(
                        config.online_gc_interval,
                        config.gc_chunk_size,
                        gc_table.clone(),
                        gc_log.clone(),
                        gc_updated_entries.clone(),
                    ),
                    read_only_memtables,
                    range_iterator: None,
                    flush_signal_tx,
                    flush_signal_rx,
                    gc_log,
                    gc_table,
                    gc_updated_entries,
                    flush_stream: HashSet::new(),
                })
            }
            Err(err) => Err(MemTableRecovery(Box::new(err))),
        }
    }

    /// Recovers memtable state
    ///
    /// Recovers both active and readonly memtable states using value log
    ///
    /// Returns a tuple of active memtable and read only memtables
    pub async fn recover_memtable(
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
        vlog_path: impl P,
        head_offset: usize,
    ) -> Result<(MemTable<Key>, ImmutableMemTablesLockFree<Key>), Error> {
        let read_only_memtables: ImmutableMemTablesLockFree<Key> = SkipMap::new();
        let mut active_memtable =
            MemTable::with_specified_capacity_and_rate(size_unit, capacity, false_positive_rate);
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
                        Arc::new(active_memtable.to_owned()),
                    );
                    active_memtable =
                        MemTable::with_specified_capacity_and_rate(size_unit, capacity, false_positive_rate);
                }
                active_memtable.insert(&entry);
            }
            most_recent_offset += SIZE_OF_U32   // Key Size(for fetching key length)
                        +SIZE_OF_U32            // Value Length(for fetching value length)
                        + SIZE_OF_U64           // Date Length
                        + SIZE_OF_U8            // Tombstone marker
                        + e.key.len()           // Key Length
                        + e.value.len(); // Value Length
        }

        Ok((active_memtable, read_only_memtables))
    }

    /// Creates new [`DataStore`]
    /// Used in case there is no recovery needed
    pub async fn handle_empty_vlog(
        params: CreateOrRecoverStoreParams<'_, impl P>,
    ) -> Result<DataStore<'static, Key>, Error> {
        let (buckets_path, dir, mut vlog, key_range, config, size_unit, meta) = (
            params.buckets_path,
            params.dir,
            params.vlog,
            params.key_range,
            params.config,
            params.size_unit,
            params.meta,
        );

        let mut active_memtable = MemTable::with_specified_capacity_and_rate(
            size_unit,
            config.write_buffer_size,
            config.false_positive_rate,
        );
        // if ValueLog is empty then we want to insert both tail and head
        let created_at = Utc::now();
        let tail_offset = vlog
            .append(
                &TAIL_ENTRY_KEY.to_vec(),
                &TAIL_ENTRY_VALUE.to_vec(),
                created_at,
                false,
            )
            .await?;
        let tail_entry = Entry::new(TAIL_ENTRY_KEY.to_vec(), tail_offset, created_at, false);
        let head_offset = vlog
            .append(
                &HEAD_ENTRY_KEY.to_vec(),
                &HEAD_ENTRY_VALUE.to_vec(),
                created_at,
                false,
            )
            .await?;
        let head_entry = Entry::new(HEAD_ENTRY_KEY.to_vec(), head_offset, created_at, false);
        vlog.set_head(head_offset);
        vlog.set_tail(tail_offset);

        // insert tail and head to memtable
        active_memtable.insert(&tail_entry.to_owned());
        active_memtable.insert(&head_entry.to_owned());
        let buckets = BucketMap::new(buckets_path).await?;
        let (flush_signal_tx, flush_signal_rx) = broadcast(DEFAULT_FLUSH_SIGNAL_CHANNEL_SIZE);
        let read_only_memtables = SkipMap::new();
        let buckets = Arc::new(RwLock::new(buckets.to_owned()));
        let key_range = Arc::new(key_range);
        let read_only_memtables = Arc::new(read_only_memtables);
        let gc_table = Arc::new(RwLock::new(active_memtable.to_owned()));
        let gc_log = Arc::new(RwLock::new(vlog.to_owned()));
        let flusher = Flusher::new(read_only_memtables.clone(), buckets.clone(), key_range.clone());
        let gc_updated_entries = Arc::new(RwLock::new(SkipMap::new()));
        Ok(DataStore {
            keyspace: DEFAULT_DB_NAME,
            active_memtable,
            val_log: vlog,
            buckets,
            dir: dir.clone(),
            key_range,
            compactor: Compactor::new(
                config.enable_ttl,
                TtlParams {
                    entry_ttl: config.entry_ttl,
                    tombstone_ttl: config.tombstone_ttl,
                },
                IntervalParams {
                    background_interval: config.background_compaction_interval,
                    flush_listener_interval: config.compactor_flush_listener_interval,
                    tombstone_compaction_interval: config.tombstone_compaction_interval,
                },
                config.compaction_strategy,
                compactors::CompactionReason::MaxSize,
                config.false_positive_rate,
            ),
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
                gc_updated_entries.clone(),
            ),
            gc_log,
            gc_table,
            gc_updated_entries,
            flush_stream: HashSet::new(),
            config,
        })
    }

    fn get_bucket_id_from_full_bucket_path(full_path: impl P) -> String {
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
