use std::{mem, path::PathBuf, sync::Arc};

use crossbeam_skiplist::SkipMap;

use crate::{
    bloom_filter::{self, BloomFilter},
    memtable::Entry,
    sstable::SSTable,
};

use super::{bucket_coordinator::Bucket, BucketMap};

struct Compactor;

impl Compactor {
    pub fn new() -> Self {
        return Self;
    }

    fn run_compaction(&self, buckets: &mut BucketMap, bloom_filters: &mut Vec<BloomFilter>) {
        // Step 1: Extract buckets to compact
        let buckets_to_compact = buckets.extract_buckets_to_compact();

        // Step 2: Merge SSTables in each buckct
    }

    fn merge_sstables_in_buckets(&self, buckets: &Vec<Bucket>) {
        let mut sstables_and_their_associated_bloom_filter_and_hotness: Vec<(
            SSTable,
            BloomFilter,
        )> = Vec::new();

        buckets.iter().for_each(|b| {
            let mut hotness = 0;
            let sstable_paths = &b.sstables;
            let mut sstables = Vec::new();
            sstable_paths.iter().for_each(|path| {
                hotness += path.hotness;
                let sst_opt = SSTable::from_file(PathBuf::new().join(path.get_path())).unwrap();
                match sst_opt {
                    Some(sst) => sstables.push(sst),
                    None => {}
                }
            })
        })
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
            if index1[i].key[0] < index2[j].key[0] {
                // increase new_sstable size
                merged_indexes.push(index1[i].clone());
                i += 1;
            } else if index1[i].key[0] == index2[i].key[0] {
                // If the keys are thesame pick the updated one based on creation time
                // TODO: Thumbstone compaction(with TTL) seperately
                if index1[i].created_at > index2[i].created_at {
                    merged_indexes.push(index1[i].clone());
                } else {
                    merged_indexes.push(index2[i].clone());
                }
                i += 1;
                j += 1;
            } else {
                merged_indexes.push(index2[j].clone());
                j += 1;
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
