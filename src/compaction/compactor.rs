use crossbeam_skiplist::SkipMap;
use log::{error, info, warn};
use std::{cmp::Ordering, collections::HashMap, path::PathBuf, sync::Arc};
use uuid::Uuid;

use crate::{
    bloom_filter::BloomFilter,
    err::StorageEngineError,
    memtable::Entry,
    sstable::{SSTable, SSTablePath},
};

use super::{bucket_coordinator::Bucket, BucketMap};

pub struct Compactor;
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
    pub fn new() -> Self {
        Self
    }

    pub fn run_compaction(
        &self,
        buckets: &mut BucketMap,
        bloom_filters: &mut Vec<BloomFilter>,
    ) -> Result<bool, StorageEngineError> {
        let mut number_of_compactions = 0;
        // The compaction loop will keep running until there
        // are no more buckets with more than minimum treshold size

        // TODO: Handle this with multiple threads while keeping track of number of Disk IO used
        // so we don't run out of Disk IO during large compactions
        loop {
            // Step 1: Extract buckets to compact
            let buckets_to_compact_and_sstables_to_remove = buckets.extract_buckets_to_compact();
            let buckets_to_compact = buckets_to_compact_and_sstables_to_remove.0;
            let sstables_files_to_remove = buckets_to_compact_and_sstables_to_remove.1;

            // Exit the compaction loop if there are no more buckets to compact
            if buckets_to_compact.is_empty() {
                return Ok(true);
            }
            number_of_compactions += 1;
            // Step 2: Merge SSTables in each buckct
            let merged_sstable_opt = self.merge_sstables_in_buckets(&buckets_to_compact);
            let mut actual_number_of_sstables_written_to_disk = 0;
            let mut expected_sstables_to_be_writtten_to_disk = 0;
            match merged_sstable_opt {
                Some(merged_sstables) => {
                    // Number of sstables expected to be inserted to disk
                    expected_sstables_to_be_writtten_to_disk = merged_sstables.len();

                    //Step 3: Write merged sstables to bucket map
                    merged_sstables
                        .into_iter()
                        .enumerate()
                        .for_each(|(_, mut m)| {
                            let insert_result =
                                buckets.insert_to_appropriate_bucket(&m.sstable, m.hotness);
                            match insert_result {
                                Ok(sst_file_path) => {
                                    // Step 4: Map this bloom filter to its sstable file path
                                    m.bloom_filter.set_sstable_path(sst_file_path);
                                    // Step 5: Store the bloom filter in the bloom filters vector
                                    bloom_filters.push(m.bloom_filter);
                                    actual_number_of_sstables_written_to_disk += 1;
                                }
                                Err(_) => {
                                    error!("merged SSTable was not written to disk ")
                                }
                            }
                        })
                }
                None => {}
            }

            info!(
              "Expected number of new SSTables written to disk : {}, Actual number of SSTables written {}",
               expected_sstables_to_be_writtten_to_disk, actual_number_of_sstables_written_to_disk);

            if expected_sstables_to_be_writtten_to_disk == actual_number_of_sstables_written_to_disk
            {
                // Step 6:  Delete the sstables that we already merged from their previous buckets and update bloom filters
                let bloom_filter_updated_opt = self.clean_up_after_compaction(
                    buckets,
                    &sstables_files_to_remove,
                    bloom_filters,
                );
                match bloom_filter_updated_opt {
                    Some(is_bloom_filter_updated) => {
                        info!(
                            "{} COMPACTION COMPLETED SUCCESSFULLY : {}",
                            number_of_compactions, is_bloom_filter_updated
                        );
                    }
                    None => {
                        return Err(StorageEngineError::CompactionPartiallyFailed(String::from(
                            "Compaction cleanup failed but sstable merge was successful",
                        )));
                    }
                }
            } else {
                warn!("Cannot remove obsolete sstables from disk because not every merged sstable was written to disk")
            }
        }
    }

    pub fn clean_up_after_compaction(
        &self,
        buckets: &mut BucketMap,
        sstables_to_delete: &Vec<(Uuid, Vec<SSTablePath>)>,
        bloom_filters_with_both_old_and_new_sstables: &mut Vec<BloomFilter>,
    ) -> Option<bool> {
        let all_sstables_deleted = buckets.delete_sstables(sstables_to_delete);

        // if all sstables were not deleted then don't remove the associated bloom filters
        // although this can lead to redundancy bloom filters are in-memory and its also less costly
        // since keys are represented in bits
        if all_sstables_deleted {
            // Step 7: Delete the bloom filters associated with the sstables that we already merged
            let bloom_filter_updated = self.filter_out_old_bloom_filters(
                bloom_filters_with_both_old_and_new_sstables,
                sstables_to_delete,
            );
            return bloom_filter_updated;
        }
        None
    }

    pub fn filter_out_old_bloom_filters(
        &self,
        bloom_filters_with_both_old_and_new_sstables: &mut Vec<BloomFilter>,
        sstables_to_delete: &Vec<(Uuid, Vec<SSTablePath>)>,
    ) -> Option<bool> {
        let mut bloom_filters_map: HashMap<PathBuf, BloomFilter> =
            bloom_filters_with_both_old_and_new_sstables
                .iter()
                .map(|b| (b.get_sstable_path().get_path().to_owned(), b.to_owned()))
                .collect();

        sstables_to_delete
            .iter()
            .for_each(|(_, sstable_files_paths)| {
                sstable_files_paths.iter().for_each(|file_path_to_delete| {
                    bloom_filters_map.remove(&file_path_to_delete.get_path());
                })
            });
        bloom_filters_with_both_old_and_new_sstables.clear();
        bloom_filters_with_both_old_and_new_sstables.extend(bloom_filters_map.into_values());
        Some(true)
    }

    fn merge_sstables_in_buckets(&self, buckets: &Vec<Bucket>) -> Option<Vec<MergedSSTable>> {
        let mut merged_sstbales: Vec<MergedSSTable> = Vec::new();

        buckets.iter().for_each(|b| {
            let mut hotness = 0;
            let sstable_paths = &b.sstables;

            let mut merged_sstable = SSTable::new(b.dir.clone(), false);
            sstable_paths.iter().for_each(|path| {
                hotness += path.hotness;
                let sst_opt = SSTable::from_file(PathBuf::new().join(path.get_path())).unwrap();
                match sst_opt {
                    Some(sst) => {
                        merged_sstable = self.merge_sstables(&merged_sstable, &sst);
                    }
                    None => {}
                }
            });

            // Rebuild the bloom filter since a new sstable has been created
            let new_bloom_filter = SSTable::build_bloomfilter_from_sstable(&merged_sstable.index);
            merged_sstbales.push(MergedSSTable {
                sstable: merged_sstable,
                hotness,
                bloom_filter: new_bloom_filter,
            })
        });
        if merged_sstbales.is_empty() {
            return None;
        }
        Some(merged_sstbales)
    }

    fn merge_sstables(&self, sst1: &SSTable, sst2: &SSTable) -> SSTable {
        let mut new_sstable = SSTable::new(PathBuf::new(), false);
        let new_sstable_index = Arc::new(SkipMap::new());
        let mut merged_indexes = Vec::new();
        let index1 = sst1
            .get_index()
            .iter()
            .map(|e| Entry::new(e.key().to_vec(), e.value().0, e.value().1))
            .collect::<Vec<Entry<Vec<u8>, usize>>>();

        let index2 = sst2
            .get_index()
            .iter()
            .map(|e| Entry::new(e.key().to_vec(), e.value().0, e.value().1))
            .collect::<Vec<Entry<Vec<u8>, usize>>>();

        let (mut i, mut j) = (0, 0);
        // Compare elements from both arrays and merge them
        while i < index1.len() && j < index2.len() {
            match index1[i].key.cmp(&index2[j].key) {
                Ordering::Less => {
                    // increase new_sstable size
                    merged_indexes.push(index1[i].clone());
                    i += 1;
                }
                Ordering::Equal => {
                    if index1[i].created_at > index2[j].created_at {
                        merged_indexes.push(index1[i].clone());
                    } else {
                        merged_indexes.push(index2[j].clone());
                    }
                    i += 1;
                    j += 1;
                }
                Ordering::Greater => {
                    merged_indexes.push(index2[j].clone());
                    j += 1;
                }
            }
        }

        // If there are any remaining elements in arr1, append them
        while i < index1.len() {
            merged_indexes.push(index1[i].clone());
            i += 1;
        }

        // If there are any remaining elements in arr2, append them
        while j < index2.len() {
            merged_indexes.push(index2[j].clone());
            j += 1;
        }
        merged_indexes.iter().for_each(|e| {
            new_sstable_index.insert(e.key.to_owned(), (e.val_offset, e.created_at));
        });
        new_sstable.set_index(new_sstable_index);
        new_sstable
    }
}
