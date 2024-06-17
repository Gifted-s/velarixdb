// Compaction Process:

// Compaction involves merging multiple SSTables into a new, optimized one. During this process, VikingsDB considers both data and tombstones.

// Expired Tombstones: If a tombstone's timestamp is older than a specific threshold (defined by gc_grace_seconds), it's considered expired.
// These expired tombstones are removed entirely during compaction, freeing up disk space.

// Unexpired Tombstones: If a tombstone is not expired, it means the data it shadows might still be relevant on other nodes in the cluster.  In
// this case, VikingsDB keeps both the tombstone and the data in the new SSTable. This ensures consistency across the buckets and allows for repairs if needed.

// Read about sized tier compaction here (https://shrikantbang.wordpress.com/2014/04/22/size-tiered-compaction-strategy-in-apache-cassandra/)
use core::sync;
use crossbeam_skiplist::SkipMap;
use futures::lock::Mutex;
use std::env::current_exe;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;

use log::{error, info, warn};
use std::cmp::Ordering;

use crate::bucket::{self, Bucket, BucketID, BucketMap, BucketsToCompact, InsertableToBucket};
use crate::consts::{DEFAULT_COMPACTION_INTERVAL_MILLI, DEFAULT_TOMBSTONE_COMPACTION_INTERVAL_MILLI};
use crate::fs::{FileAsync, FileNode};
use crate::types::{
    BloomFilterHandle, Bool, BucketMapHandle, Duration, FlushReceiver, FlushSignal, Key, KeyRangeHandle,
};
use crate::{
    consts::TOMB_STONE_TTL, err::Error, filter::BloomFilter, key_range::KeyRange, memtable::Entry, sst::Table,
};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::fs;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use tokio::time::sleep;
use uuid::Uuid;
use Error::*;

use super::TableInsertor;
#[derive(Debug, Clone)]
pub struct Compactor {
    pub is_active: Arc<Mutex<CompState>>,
    // tombstones are considered and used to identify
    // and remove deleted data during compaction
    pub tombstones: HashMap<Key, u64>,
    // config
    pub config: Config,
}

#[derive(Debug, Clone)]
pub struct Config {
    // should compactor remove entry that has exceeded time to live?
    pub use_ttl: Bool,
    // entry expected time to live
    pub entry_ttl: Duration,

    pub flush_listener_interval: Duration,

    pub background_interval: Duration,
}
impl Config {
    pub fn new(
        use_ttl: Bool,
        entry_ttl: Duration,
        flush_listener_interval: Duration,
        background_interval: Duration,
    ) -> Self {
        Config {
            use_ttl,
            entry_ttl,
            flush_listener_interval,
            background_interval,
        }
    }
}

pub struct WriteTracker {
    pub actual: usize,
    pub expected: usize,
}
impl WriteTracker {
    fn new(expected: usize) -> Self {
        Self { actual: 0, expected }
    }
}

#[derive(Debug, Clone)]
pub enum CompState {
    Sleep,
    Active,
}

#[derive(Debug)]
pub enum CompReason {
    MaxSize,
    Manual,
}

#[derive(Debug)]
pub struct MergedSSTable {
    pub sstable: Box<dyn InsertableToBucket>,
    pub hotness: u64,
    pub filter: BloomFilter,
}

impl Clone for MergedSSTable {
    fn clone(&self) -> Self {
        Self {
            sstable: Box::new(TableInsertor::from(self.sstable.get_entries(), self.sstable.size())),
            hotness: self.hotness,
            filter: self.filter.clone(),
        }
    }
}

impl MergedSSTable {
    pub fn new(sstable: Box<dyn InsertableToBucket>, filter: BloomFilter, hotness: u64) -> Self {
        Self {
            sstable,
            hotness,
            filter,
        }
    }
}

impl Compactor {
    pub fn new(use_ttl: Bool, entry_ttl: Duration, interval: Duration, flush_listener_interval: Duration) -> Self {
        Self {
            tombstones: HashMap::new(),
            is_active: Arc::new(Mutex::new(CompState::Sleep)),
            config: Config::new(use_ttl, entry_ttl, flush_listener_interval, interval),
        }
    }
    /// TODO: Trigger tombstone compaction on interval
    pub fn tombstone_compaction_condition_background_checker(&self, rcx: Arc<RwLock<Receiver<BucketMap>>>) {
        let receiver = Arc::clone(&rcx);
        tokio::spawn(async move {
            loop {
                let _ = receiver.write().await.try_recv();
                sleep(core::time::Duration::from_millis(
                    DEFAULT_TOMBSTONE_COMPACTION_INTERVAL_MILLI,
                ))
                .await;
            }
        });
    }

    pub fn start_flush_listner(
        &self,
        signal_receiver: FlushReceiver,
        bucket_map: BucketMapHandle,
        filter: BloomFilterHandle,
        key_range: KeyRangeHandle,
    ) {
        let mut rcx = signal_receiver.clone();
        let curr_state = Arc::clone(&self.is_active);
        let cfg = self.config.to_owned();
        tokio::spawn(async move {
            loop {
                Compactor::sleep_compaction(cfg.flush_listener_interval).await;
                let signal = rcx.try_recv();
                let mut comp_state = curr_state.lock().await;
                if let CompState::Sleep = *comp_state {
                    if let Err(err) = signal {
                        drop(comp_state);
                        match err {
                            async_broadcast::TryRecvError::Overflowed(_) => {
                                log::error!("{}", FlushSignalOverflowError)
                            }
                            async_broadcast::TryRecvError::Closed => {
                                log::error!("{}", FlushSignalClosedError)
                            }
                            async_broadcast::TryRecvError::Empty => {}
                        }
                        continue;
                    }
                    *comp_state = CompState::Active;
                    drop(comp_state);
                    if let Err(err) =
                        Compactor::handle_compaction(bucket_map.clone(), filter.clone(), key_range.clone(), &cfg).await
                    {
                        log::info!("{}", Error::CompactionFailed(err.to_string()));
                        continue;
                    }
                    let mut comp_state = curr_state.lock().await;
                    *comp_state = CompState::Sleep;
                }
            }
        });
    }

    pub async fn handle_compaction(
        buckets: BucketMapHandle,
        filter: BloomFilterHandle,
        key_range: KeyRangeHandle,
        cfg: &Config,
    ) -> Result<(), Error> {
        if !buckets.read().await.is_balanced().await {
            // compaction will continue until all buckets are balanced
            let mut comp = Compactor::new(
                cfg.use_ttl,
                cfg.entry_ttl,
                cfg.background_interval,
                cfg.flush_listener_interval,
            );
            if let Err(err) = comp
                .run_compaction(Arc::clone(&buckets), Arc::clone(&filter), Arc::clone(&key_range))
                .await
            {
                return Err(err);
            }
            return Ok(());
        }
        Ok(())
    }

    pub async fn fetch_imbalanced_buckets(bucket_map: BucketMapHandle) -> BucketsToCompact {
        bucket_map.read().await.extract_buckets_to_compact().await
    }

    pub async fn run_compaction(
        &mut self,
        bucket_map: BucketMapHandle,
        filters: BloomFilterHandle,
        key_range: KeyRangeHandle,
    ) -> Result<(), Error> {
        let mut completed = 0;
        // The compaction loop will keep running until there
        // are no more buckets with more than minimum treshold size
        // TODO: Handle this with multiple threads
        loop {
            let buckets: BucketMapHandle = Arc::clone(&bucket_map);
            let filters = Arc::clone(&filters);
            let key_range = Arc::clone(&key_range);
            // Step 1: Extract imbalanced buckets
            let (imbalanced_buckets, ssts_to_remove) = Compactor::fetch_imbalanced_buckets(buckets.clone()).await?;
            if imbalanced_buckets.is_empty() {
                self.tombstones.clear();
                return Ok(());
            }
            completed += 1;

            // Step 2: Merge SSTs in each imbalanced buckct
            match self.merge_ssts_in_buckets(&imbalanced_buckets.to_owned()).await {
                Ok(merged_sstables) => {
                    let mut tracker = WriteTracker::new(merged_sstables.len());

                    // Step 3: Insert Merged SSTs to appropriate buckets
                    for mut merged_sst in merged_sstables.into_iter() {
                        let mut bucket = buckets.write().await;
                        let table = merged_sst.clone().sstable;
                        let insert_res = bucket
                            .insert_to_appropriate_bucket(Arc::new(table), merged_sst.hotness)
                            .await;
                        drop(bucket);
                        match insert_res {
                            Ok(sst) => {
                                log::info!(
                                    "SST written, data: {:?}, index {:?}",
                                    sst.data_file.path,
                                    sst.index_file.path
                                );
                                // Step 4: Store SST in Filter
                                let data_file_path = sst.get_data_file_path();
                                merged_sst.filter.set_sstable(sst.clone());

                                // Step 5: Store Filter in Filters Vec
                                filters.write().await.push(merged_sst.filter);
                                let biggest_key = merged_sst.sstable.find_biggest_key()?;
                                let smallest_key = merged_sst.sstable.find_smallest_key()?;
                                if biggest_key.is_empty() {
                                    return Err(BiggestKeyIndexError);
                                }
                                if smallest_key.is_empty() {
                                    return Err(LowestKeyIndexError);
                                }
                                key_range
                                    .write()
                                    .await
                                    .set(data_file_path, smallest_key, biggest_key, sst);
                                tracker.actual += 1;
                            }
                            Err(err) => {
                                // Step 6 (Optional): Trigger recovery in case compaction failed at any point
                                // Ensure filters are restored to the previous state
                                // Remove merged sstables written to disk so far to prevent stale data
                                while tracker.actual > 0 {
                                    if let Some(filter) = filters.write().await.pop() {
                                        let table = filter.get_sst().to_owned();
                                        if let Err(err) = fs::remove_dir_all(table.dir).await {
                                            log::error!("{}", CompactionFailed(err.to_string()));
                                            tracker.actual -= 1;
                                            continue;
                                        }
                                        key_range.write().await.remove(table.data_file.path);
                                    }
                                    tracker.actual -= 1;
                                }
                                return Err(CompactionFailed(err.to_string()));
                            }
                        }
                    }

                    if tracker.expected == tracker.actual {
                        // Step 7:  Delete the sstables that we already merged from their previous buckets and update bloom filters
                        let filters_updated = self
                            .clean_up_after_compaction(buckets, &ssts_to_remove.clone(), filters, key_range)
                            .await;
                        match filters_updated {
                            Ok(None) => {
                                return Err(Error::CompactionPartiallyFailed(Box::new(
                                    CompactionCleanupPartialError,
                                )));
                            }
                            Err(err) => {
                                return Err(Error::CompactionCleanupError(Box::new(err)));
                            }
                            _ => {}
                        }
                    } else {
                        warn!("Cannot remove obsolete sstables from disk because not every merged sstable was written to disk")
                    }
                }
                Err(err) => return Err(CompactionFailed(err.to_string())),
            }
        }
    }

    pub async fn clean_up_after_compaction(
        &self,
        buckets: BucketMapHandle,
        sstables_to_delete: &Vec<(BucketID, Vec<Table>)>,
        filter_with_both_old_and_new_sstables: BloomFilterHandle,
        key_range: KeyRangeHandle,
    ) -> Result<Option<()>, Error> {
        // Remove obsolete keys from biggest keys index
        sstables_to_delete.iter().for_each(|(_, sstables)| {
            sstables.iter().for_each(|s| {
                let index = Arc::clone(&key_range);
                let path = s.get_data_file_path();
                tokio::spawn(async move {
                    index.write().await.remove(path);
                });
            })
        });
        let all_sstables_deleted = buckets.write().await.delete_sstables(sstables_to_delete).await?;
        // if all sstables were not deleted then don't remove the associated bloom filters
        // although this can lead to redundancy bloom filters are in-memory and its also less costly
        // since keys are represented in bits
        if all_sstables_deleted {
            // Step 8: Delete the bloom filters associated with the sstables that we already merged
            self.filter_out_old_filter(filter_with_both_old_and_new_sstables, sstables_to_delete)
                .await;
            return Ok(Some(()));
        }
        Ok(None)
    }

    pub async fn filter_out_old_filter(
        &self,
        filter_with_both_old_and_new_sstables: BloomFilterHandle,
        sstables_to_delete: &Vec<(Uuid, Vec<Table>)>,
    ) {
        let mut filter_map: HashMap<PathBuf, BloomFilter> = filter_with_both_old_and_new_sstables
            .read()
            .await
            .iter()
            .map(|b| (b.get_sst().dir.to_owned(), b.to_owned()))
            .collect();

        sstables_to_delete.iter().for_each(|(_, sstable_files_paths)| {
            sstable_files_paths.iter().for_each(|file_path_to_delete| {
                filter_map.remove(&file_path_to_delete.dir);
            })
        });
        filter_with_both_old_and_new_sstables.write().await.clear();
        filter_with_both_old_and_new_sstables
            .write()
            .await
            .extend(filter_map.into_values());
    }

    async fn merge_ssts_in_buckets(&mut self, buckets: &Vec<Bucket>) -> Result<Vec<MergedSSTable>, Error> {
        let mut merged_sstables: Vec<MergedSSTable> = Vec::new();
        for b in buckets.iter() {
            let mut hotness = 0;
            let sstable_paths = &b.sstables.read().await;
            let mut merged_sst: Box<dyn InsertableToBucket> = Box::new(sstable_paths.get(0).unwrap().to_owned());

            for path in sstable_paths[1..].iter() {
                hotness += path.hotness;
                let sst_opt = path
                    .from_file()
                    .await
                    .map_err(|err| CompactionFailed(err.to_string()))?;

                match sst_opt {
                    Some(sst) => {
                        merged_sst = self
                            .merge_sstables(merged_sst, Box::new(sst))
                            .await
                            .map_err(|err| CompactionFailed(err.to_string()))?;
                    }
                    None => {}
                }
            }
            // Rebuild the bloom filter since a new sstable has been created
            let new_bloom_filter = Table::build_bloomfilter_from_sstable(&merged_sst.get_entries());
            merged_sstables.push(MergedSSTable::new(merged_sst, new_bloom_filter, hotness));
        }

        if merged_sstables.is_empty() {
            return Err(CompactionFailed("Merged SSTables cannot be empty".to_owned()));
        }
        Ok(merged_sstables)
    }

    async fn merge_sstables(
        &mut self,
        sst1: Box<dyn InsertableToBucket>,
        sst2: Box<dyn InsertableToBucket>,
    ) -> Result<Box<dyn InsertableToBucket>, Error> {
        let mut new_sstable = TableInsertor::new();
        let new_sstable_map = Arc::new(SkipMap::new());
        let mut merged_entries = Vec::new();
        let entries1 = sst1
            .get_entries()
            .iter()
            .map(|e| Entry::new(e.key().to_vec(), e.value().0, e.value().1, e.value().2))
            .collect::<Vec<Entry<Vec<u8>, usize>>>();

        let entries2 = sst2
            .get_entries()
            .iter()
            .map(|e| Entry::new(e.key().to_vec(), e.value().0, e.value().1, e.value().2))
            .collect::<Vec<Entry<Vec<u8>, usize>>>();

        let (mut i, mut j) = (0, 0);
        // Compare elements from both arrays and merge them
        while i < entries1.len() && j < entries2.len() {
            match entries1[i].key.cmp(&entries2[j].key) {
                Ordering::Less => {
                    self.tombstone_check(&entries1[i], &mut merged_entries)
                        .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    i += 1;
                }
                Ordering::Equal => {
                    if entries1[i].created_at > entries2[j].created_at {
                        self.tombstone_check(&entries1[i], &mut merged_entries)
                            .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    } else {
                        self.tombstone_check(&entries2[j], &mut merged_entries)
                            .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    }
                    i += 1;
                    j += 1;
                }
                Ordering::Greater => {
                    self.tombstone_check(&entries2[j], &mut merged_entries)
                        .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    j += 1;
                }
            }
        }

        // If there are any remaining elements in arr1, append them
        while i < entries1.len() {
            self.tombstone_check(&entries1[i], &mut merged_entries)
                .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
            i += 1;
        }

        // If there are any remaining elements in arr2, append them
        while j < entries2.len() {
            self.tombstone_check(&entries2[j], &mut merged_entries)
                .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
            j += 1;
        }

        merged_entries.iter().for_each(|e| {
            new_sstable_map.insert(e.key.to_owned(), (e.val_offset, e.created_at, e.is_tombstone));
        });
        new_sstable.set_entries(new_sstable_map);
        Ok(Box::new(new_sstable))
    }

    fn tombstone_check(
        &mut self,
        entry: &Entry<Vec<u8>, usize>,
        merged_entries: &mut Vec<Entry<Vec<u8>, usize>>,
    ) -> Result<Bool, Error> {
        let mut insert_entry = false;
        // If key has been mapped to any tombstone
        if self.tombstones.contains_key(&entry.key) {
            let tombstone_insertion_time = *self.tombstones.get(&entry.key).unwrap();

            // Then check if entry is more recent than tombstone
            if entry.created_at > tombstone_insertion_time {
                // If entry key maps to a tombstone then update the tombstone hashmap
                if entry.is_tombstone {
                    self.tombstones.insert(entry.key.clone(), entry.created_at);
                    // if tombstone has not expired then re-insert it.
                    if !entry.has_expired(TOMB_STONE_TTL) {
                        insert_entry = true;
                    }
                }
                // otherwise attempt to insert entry because obviously
                // this key was re-inserted after it was deleted
                else {
                    // if ttl was enabled then ensure entry has not expired before insertion
                    if self.config.use_ttl {
                        insert_entry = !entry.has_expired(self.config.entry_ttl);
                    } else {
                        insert_entry = true
                    }
                }
            }
        }
        // If key was mapped to a tombstone and it does not exist in the
        // tombstone hashmap then insert it
        else {
            if entry.is_tombstone {
                self.tombstones.insert(entry.key.clone(), entry.created_at);
                // if tombstone has not expired then re-insert it.
                if !entry.has_expired(TOMB_STONE_TTL) {
                    insert_entry = true;
                }
            } else {
                // if ttl was enabled then ensure entry has not expired before insertion
                if self.config.use_ttl {
                    insert_entry = !entry.has_expired(self.config.entry_ttl);
                } else {
                    insert_entry = true
                }
            }
        }

        if insert_entry {
            merged_entries.push(entry.clone())
        }
        Ok(true)
    }

    async fn sleep_compaction(duration: Duration) {
        sleep(core::time::Duration::from_millis(duration)).await;
    }

    // async fn check_tombsone_for_merged_sstables(
    //     &mut self,
    //     merged_sstables: Vec<MergedSSTable>,
    // ) -> Result<Vec<MergedSSTable>, Error> {
    //     println!(" DIVIDER ");
    //     let mut filterd_merged_sstables: Vec<MergedSSTable> = Vec::new();
    //     for m in merged_sstables.iter() {
    //         let new_index = Arc::new(SkipMap::new());
    //         let mut new_sstable = TableInsertor::new();
    //         for entry in m.sstable.get_entries().iter() {
    //             if entry.key().cmp(&"aunkanmi".as_bytes().to_vec())== Ordering::Equal{
    //                println!("{:?} tb len {}", entry, self.tombstones.len());
    //             }
    //             if self.tombstones.contains_key(entry.key()) {
    //                 if entry.key().cmp(&"aunkanmi".as_bytes().to_vec())== Ordering::Equal{
    //                     println!("as tumbstone {:?}", entry);
    //                  }
    //                 let tombstone_timestamp = *self.tombstones.get(entry.key()).unwrap();
    //                 if tombstone_timestamp < entry.value().1 {
    //                     new_index.insert(
    //                         entry.key().to_vec(),
    //                         (entry.value().0, entry.value().1, entry.value().2),
    //                     );
    //                 }
    //             } else {
    //                 if entry.key().cmp(&"aunkanmi".as_bytes().to_vec())== Ordering::Equal{
    //                     println!("inserted {:?}", entry);
    //                  }
    //                 new_index.insert(
    //                     entry.key().to_vec(),
    //                     (entry.value().0, entry.value().1, entry.value().2),
    //                 );
    //             }
    //         }
    //         new_sstable.set_entries(new_index);
    //         filterd_merged_sstables.push(MergedSSTable {
    //             sstable: Box::new(new_sstable),
    //             hotness: m.hotness,
    //             filter: m.filter.clone(),
    //         })
    //     }

    //     Ok(filterd_merged_sstables)
    // }

    pub fn start_periodic_background_compaction(
        &self,
        bucket_map: BucketMapHandle,
        filter: BloomFilterHandle,
        key_range: KeyRangeHandle,
    ) {
        let use_ttl = self.config.use_ttl;
        let entry_ttl = self.config.entry_ttl;
        let curr_state = Arc::clone(&self.is_active);
        tokio::spawn(async move {
            let current_buckets = bucket_map;
            let current_filter = filter;
            let current_key_range = key_range;
            loop {
                // let mut is_active = curr_state.lock().unwrap();
                // if !is_active {
                //     curr_state.store(true, std::sync::atomic::Ordering::Relaxed);
                //     let compaction_result = Compactor::handle_compaction(
                //         use_ttl,
                //         entry_ttl,
                //         current_buckets.clone(),
                //         current_filter.clone(),
                //         current_key_range.clone(),
                //     )
                //     .await;
                //     match compaction_result {
                //         Ok(done) => {
                //             log::info!("Compaction complete : {}", done);
                //         }
                //         Err(err) => {
                //             log::info!("{}", Error::CompactionFailed(err.to_string()))
                //         }
                //     }
                //     curr_state.store(false, std::sync::atomic::Ordering::Relaxed);
                // }

                sleep(core::time::Duration::from_millis(DEFAULT_COMPACTION_INTERVAL_MILLI)).await;
            }
        });
    }
}
