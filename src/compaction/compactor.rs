use crossbeam_skiplist::SkipMap;
use log::{error, info, warn};
use std::{cmp::Ordering, collections::HashMap, path::PathBuf, sync::Arc};
use tokio::fs;
use tokio::sync::{mpsc::Receiver, RwLock};
use tokio::time::{sleep, Duration};
use uuid::Uuid;

use super::{
    bucket_coordinator::{Bucket, BucketID},
    BucketMap,
};
use crate::consts::{
    DEFAULT_COMPACTION_INTERVAL_MILLI, DEFAULT_TOMBSTONE_COMPACTION_INTERVAL_MILLI,
};
use crate::storage_engine::ExRw;
use crate::{
    bloom_filter::BloomFilter,
    consts::TOMB_STONE_TTL,
    err::StorageEngineError,
    key_offseter::KeyRange,
    memtable::Entry,
    sstable::{SSTable, SSTablePath},
};
use StorageEngineError::*;
#[derive(Clone, Debug)]
pub struct Compactor {
    // tombstones are considered and used to identify
    // and remove deleted data during compaction
    pub tombstones: HashMap<Vec<u8>, u64>,
    // should compactor remove entry that has exceeded time to live?
    pub use_ttl: bool,
    // entry expected time to live
    pub entry_ttl: u64,
}
pub struct MergedSSTable {
    pub sstable: SSTable,
    pub hotness: u64,
    pub bloom_filter: BloomFilter,
}

impl MergedSSTable {
    pub fn new(sstable: SSTable, bloom_filter: BloomFilter, hotness: u64) -> Self {
        Self {
            sstable,
            hotness,
            bloom_filter,
        }
    }
}

impl Compactor {
    pub fn new(use_ttl: bool, entry_ttl: u64) -> Self {
        Self {
            tombstones: HashMap::new(),
            use_ttl,
            entry_ttl,
        }
    }
    /// TODO: This method will be used to check for the condition to trigger tombstone compaction
    /// for now this feature has not been implememnted
    pub fn tombstone_compaction_condition_background_checker(
        &self,
        rcx: ExRw<Receiver<BucketMap>>,
    ) {
        let receiver = Arc::clone(&rcx);
        tokio::spawn(async move {
            loop {
                let _ = receiver.write().await.try_recv();
                sleep(Duration::from_millis(
                    DEFAULT_TOMBSTONE_COMPACTION_INTERVAL_MILLI,
                ))
                .await;
            }
        });
    }

    pub fn start_periodic_background_compaction(
        &self,
        bucket_map: ExRw<BucketMap>,
        bloom_filters: ExRw<Vec<BloomFilter>>,
        key_range: ExRw<KeyRange>,
    ) {
        let use_ttl = self.use_ttl;
        let entry_ttl = self.entry_ttl;
        tokio::spawn(async move {
            let current_buckets = &bucket_map;
            let current_bloom_filters = &bloom_filters;
            let current_key_range = &key_range;
            loop {
                let mut should_compact = false;
                // check for compaction conditions before returning
                for (level, (_, bucket)) in current_buckets.read().await.buckets.iter().enumerate()
                {
                    if bucket.should_trigger_compaction(level) {
                        should_compact = true;
                        break;
                    }
                }

                if should_compact {
                    // compaction will continue to until all the table is balanced
                    let mut compactor = Compactor::new(use_ttl, entry_ttl);
                    let comp_res = compactor
                        .run_compaction(
                            Arc::clone(&current_buckets),
                            Arc::clone(&current_bloom_filters),
                            Arc::clone(&current_key_range),
                        )
                        .await;
                    match comp_res {
                        Ok(done) => {
                            println!("Compactoin complete : {}", done);
                        }
                        Err(err) => {
                            println!("Error during compaction : {}", err)
                        }
                    }
                }
                sleep(Duration::from_millis(DEFAULT_COMPACTION_INTERVAL_MILLI)).await;
            }
        });
    }

    pub async fn run_compaction(
        &mut self,
        bucket_map: ExRw<BucketMap>,
        bf: ExRw<Vec<BloomFilter>>,
        key_range: ExRw<KeyRange>,
    ) -> Result<bool, StorageEngineError> {
        println!("Compaction started");
        let mut number_of_compactions = 0;
        // The compaction loop will keep running until there
        // are no more buckets with more than minimum treshold size

        // TODO: Handle this with multiple threads while keeping track of number of Disk IO used
        // so we don't run out of Disk IO during large compactions
        loop {
            let buckets = Arc::clone(&bucket_map);
            let bloom_filters = Arc::clone(&bf);
            let key_range = Arc::clone(&key_range);
            // Step 1: Extract buckets to compact
            let buckets_to_compact_and_sstables_to_remove =
                buckets.read().await.extract_buckets_to_compact().await?;
            let buckets_to_compact = buckets_to_compact_and_sstables_to_remove.0;
            let sstables_files_to_remove = buckets_to_compact_and_sstables_to_remove.1;

            // Exit the compaction loop if there are no more buckets to compact
            if buckets_to_compact.is_empty() {
                self.tombstones.clear();
                return Ok(true);
            }
            number_of_compactions += 1;
            // Step 2: Merge SSTables in each buckct
            match self.merge_sstables_in_buckets(&buckets_to_compact).await {
                Ok(merged_sstables) => {
                    // Number of sstables actually written to disk
                    let mut actual_number_of_sstables_written_to_disk = 0;
                    // Number of sstables expected to be inserted to disk
                    let expected_sstables_to_be_writtten_to_disk = merged_sstables.len();

                    // Step 3: Write merged sstables to bucket map
                    for (_, mut m) in merged_sstables.into_iter().enumerate() {
                        match buckets
                            .write()
                            .await
                            .insert_to_appropriate_bucket(&m.sstable, m.hotness)
                            .await
                        {
                            Ok(sst_file_path) => {
                                println!(
                                    "SSTable written to Disk data path: {:?}, index path {:?}",
                                    sst_file_path.data_file_path, sst_file_path.index_file_path
                                );
                                // Step 4: Map this bloom filter to its sstable file path
                                let sstable_data_file_path = sst_file_path.get_data_file_path();
                                m.bloom_filter.set_sstable_path(sst_file_path);

                                // Step 5: Store the bloom filter in the bloom filters vector
                                bloom_filters.write().await.push(m.bloom_filter);
                                let biggest_key = m.sstable.find_biggest_key()?;
                                let smallest_key = m.sstable.find_smallest_key()?;
                                if biggest_key.is_empty() {
                                    return Err(BiggestKeyIndexError);
                                }
                                if smallest_key.is_empty() {
                                    return Err(LowestKeyIndexError);
                                }
                                key_range.write().await.set(
                                    sstable_data_file_path,
                                    smallest_key,
                                    biggest_key,
                                );
                                actual_number_of_sstables_written_to_disk += 1;
                            }
                            Err(err) => {
                                // Step 6: Trigger recovery in case compaction failed at any point

                                // Ensure that bloom filter is restored to the previous state by removing entries added so far in
                                // the compaction process and also remove merged sstables written to disk so far to prevent unstable state
                                while actual_number_of_sstables_written_to_disk > 0 {
                                    if let Some(bf) = bloom_filters.write().await.pop() {
                                        match fs::remove_dir_all(bf.get_sstable_path().dir.clone())
                                            .await
                                        {
                                            Ok(()) => {
                                                key_range.write().await.remove(
                                                    bf.get_sstable_path().data_file_path.clone(),
                                                );
                                                info!("Stale SSTable File successfully deleted.")
                                            }
                                            Err(e) => {
                                                error!("Stale SSTable File not deleted. {}", e)
                                            }
                                        }
                                    }

                                    actual_number_of_sstables_written_to_disk -= 1;
                                }

                                error!("merged SSTable was not written to disk  {}", err);
                                return Err(CompactionFailed(err.to_string()));
                            }
                        }
                    }

                    info!(
                        "Expected number of new SSTables written to disk : {}, Actual number of SSTables written {}",
                         expected_sstables_to_be_writtten_to_disk, actual_number_of_sstables_written_to_disk);

                    if expected_sstables_to_be_writtten_to_disk
                        == actual_number_of_sstables_written_to_disk
                    {
                        // Step 7:  Delete the sstables that we already merged from their previous buckets and update bloom filters
                        let bloom_filter_updated_opt = self
                            .clean_up_after_compaction(
                                buckets,
                                &sstables_files_to_remove,
                                bloom_filters,
                                key_range,
                            )
                            .await;
                        match bloom_filter_updated_opt {
                            Ok(Some(is_bloom_filter_updated)) => {
                                // clear Tumbstone Map so as to not interfere with the next compaction process
                                info!(
                                    "{} COMPACTION COMPLETED SUCCESSFULLY : {}",
                                    number_of_compactions, is_bloom_filter_updated
                                );
                            }
                            Ok(None) => {
                                return Err(StorageEngineError::CompactionPartiallyFailed(String::from(
                                      "Partial failure, obsolete sstables not deleted but sstable merge was successful",
                                  )));
                            }
                            Err(err) => {
                                let mut err_des = String::from(
                                    "Compaction cleanup failed but sstable merge was successful ",
                                );
                                err_des.push_str(&err.to_string());
                                return Err(StorageEngineError::CompactionPartiallyFailed(err_des));
                            }
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
        buckets: ExRw<BucketMap>,
        sstables_to_delete: &Vec<(BucketID, Vec<SSTablePath>)>,
        bloom_filters_with_both_old_and_new_sstables: ExRw<Vec<BloomFilter>>,
        key_range: ExRw<KeyRange>,
    ) -> Result<Option<bool>, StorageEngineError> {
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
        let all_sstables_deleted = buckets
            .write()
            .await
            .delete_sstables(sstables_to_delete)
            .await?;
        // if all sstables were not deleted then don't remove the associated bloom filters
        // although this can lead to redundancy bloom filters are in-memory and its also less costly
        // since keys are represented in bits
        if all_sstables_deleted {
            // Step 8: Delete the bloom filters associated with the sstables that we already merged
            let bloom_filter_updated = self
                .filter_out_old_bloom_filters(
                    bloom_filters_with_both_old_and_new_sstables,
                    sstables_to_delete,
                )
                .await;
            return Ok(bloom_filter_updated);
        }
        Ok(None)
    }

    pub async fn filter_out_old_bloom_filters(
        &self,
        bloom_filters_with_both_old_and_new_sstables: ExRw<Vec<BloomFilter>>,
        sstables_to_delete: &Vec<(Uuid, Vec<SSTablePath>)>,
    ) -> Option<bool> {
        let mut bloom_filters_map: HashMap<PathBuf, BloomFilter> =
            bloom_filters_with_both_old_and_new_sstables
                .read()
                .await
                .iter()
                .map(|b| (b.get_sstable_path().dir.to_owned(), b.to_owned()))
                .collect();

        sstables_to_delete
            .iter()
            .for_each(|(_, sstable_files_paths)| {
                sstable_files_paths.iter().for_each(|file_path_to_delete| {
                    bloom_filters_map.remove(&file_path_to_delete.dir);
                })
            });
        bloom_filters_with_both_old_and_new_sstables
            .write()
            .await
            .clear();
        bloom_filters_with_both_old_and_new_sstables
            .write()
            .await
            .extend(bloom_filters_map.into_values());
        Some(true)
    }

    async fn merge_sstables_in_buckets(
        &mut self,
        buckets: &Vec<Bucket>,
    ) -> Result<Vec<MergedSSTable>, StorageEngineError> {
        let mut merged_sstables: Vec<MergedSSTable> = Vec::new();
        for b in buckets.iter() {
            let mut hotness = 0;
            let sstable_paths = &b.sstables;
            let mut merged_sstable = SSTable::new(b.dir.clone(), false).await;
            for path in sstable_paths.iter() {
                hotness += path.hotness;
                let sst_opt = SSTable::from_file(
                    path.dir.clone(),
                    path.data_file_path.clone(),
                    path.index_file_path.clone(),
                )
                .await
                .map_err(|err| CompactionFailed(err.to_string()))?;

                match sst_opt {
                    Some(sst) => {
                        merged_sstable = self
                            .merge_sstables(&merged_sstable, &sst)
                            .await
                            .map_err(|err| CompactionFailed(err.to_string()))?;
                    }
                    None => {}
                }
            }

            // Rebuild the bloom filter since a new sstable has been created
            let new_bloom_filter = SSTable::build_bloomfilter_from_sstable(&merged_sstable.index);
            merged_sstables.push(MergedSSTable {
                sstable: merged_sstable,
                hotness,
                bloom_filter: new_bloom_filter,
            })
        }
        if merged_sstables.is_empty() {
            return Err(CompactionFailed(
                "Merged SSTables cannot be empty".to_owned(),
            ));
        }
        let filtered_sstables = self
            .check_tombsone_for_merged_sstables(merged_sstables)
            .await?;
        Ok(filtered_sstables)
    }

    async fn merge_sstables(
        &mut self,
        sst1: &SSTable,
        sst2: &SSTable,
    ) -> Result<SSTable, StorageEngineError> {
        let mut new_sstable = SSTable::new(PathBuf::new(), false).await;
        let new_sstable_index = Arc::new(SkipMap::new());
        let mut merged_indexes = Vec::new();
        let index1 = sst1
            .get_index()
            .iter()
            .map(|e| Entry::new(e.key().to_vec(), e.value().0, e.value().1, e.value().2))
            .collect::<Vec<Entry<Vec<u8>, usize>>>();

        let index2 = sst2
            .get_index()
            .iter()
            .map(|e| Entry::new(e.key().to_vec(), e.value().0, e.value().1, e.value().2))
            .collect::<Vec<Entry<Vec<u8>, usize>>>();

        let (mut i, mut j) = (0, 0);
        // Compare elements from both arrays and merge them
        while i < index1.len() && j < index2.len() {
            match index1[i].key.cmp(&index2[j].key) {
                Ordering::Less => {
                    self.tombstone_check(&index1[i], &mut merged_indexes)
                        .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    i += 1;
                }
                Ordering::Equal => {
                    if index1[i].created_at > index2[j].created_at {
                        self.tombstone_check(&index1[i], &mut merged_indexes)
                            .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    } else {
                        self.tombstone_check(&index2[j], &mut merged_indexes)
                            .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    }
                    i += 1;
                    j += 1;
                }
                Ordering::Greater => {
                    self.tombstone_check(&index2[j], &mut merged_indexes)
                        .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    j += 1;
                }
            }
        }

        // If there are any remaining elements in arr1, append them
        while i < index1.len() {
            self.tombstone_check(&index1[i], &mut merged_indexes)
                .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
            i += 1;
        }

        // If there are any remaining elements in arr2, append them
        while j < index2.len() {
            self.tombstone_check(&index2[j], &mut merged_indexes)
                .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
            j += 1;
        }

        merged_indexes.iter().for_each(|e| {
            new_sstable_index.insert(
                e.key.to_owned(),
                (e.val_offset, e.created_at, e.is_tombstone),
            );
        });
        new_sstable.set_index(new_sstable_index);
        Ok(new_sstable)
    }

    fn tombstone_check(
        &mut self,
        entry: &Entry<Vec<u8>, usize>,
        merged_indexes: &mut Vec<Entry<Vec<u8>, usize>>,
    ) -> Result<bool, StorageEngineError> {
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
                    if self.use_ttl {
                        insert_entry = !entry.has_expired(self.entry_ttl);
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
                if self.use_ttl {
                    insert_entry = !entry.has_expired(self.entry_ttl);
                } else {
                    insert_entry = true
                }
            }
        }

        if insert_entry {
            merged_indexes.push(entry.clone())
        }
        Ok(true)
    }

    async fn check_tombsone_for_merged_sstables(
        &mut self,
        merged_sstables: Vec<MergedSSTable>,
    ) -> Result<Vec<MergedSSTable>, StorageEngineError> {
        let mut filterd_merged_sstables: Vec<MergedSSTable> = Vec::new();
        for m in merged_sstables.iter() {
            let new_index: Arc<SkipMap<Vec<u8>, (usize, u64, bool)>> = Arc::new(SkipMap::new());
            let mut new_sstable = SSTable::new(PathBuf::new(), false).await;
            for entry in m.sstable.index.iter() {
                if self.tombstones.contains_key(entry.key()) {
                    let tombstone_timestamp = *self.tombstones.get(entry.key()).unwrap();
                    if tombstone_timestamp < entry.value().1 {
                        new_index.insert(
                            entry.key().to_vec(),
                            (entry.value().0, entry.value().1, entry.value().2),
                        );
                    }
                } else {
                    new_index.insert(
                        entry.key().to_vec(),
                        (entry.value().0, entry.value().1, entry.value().2),
                    );
                }
            }
            new_sstable.set_index(new_index);
            filterd_merged_sstables.push(MergedSSTable {
                sstable: new_sstable,
                hotness: m.hotness,
                bloom_filter: m.bloom_filter.clone(),
            })
        }

        Ok(filterd_merged_sstables)
    }
}
