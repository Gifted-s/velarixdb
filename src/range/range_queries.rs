use std::{cmp::Ordering, collections::HashMap, path::PathBuf};

// NOTE: STCS can handle range queries but scans within identified SSTables might be neccessary.
// Data for your range might be spread across multiple SSTables. Even with a successful bloom filter check,
// each identified SSTable might still contain data outside your desired range. For heavily range query-focused workloads, LCS or TWSC should be considered
// Although this stratedy is not available for now, It will be implmented in the future

use crate::{
    memtable::{self, Entry, InsertionTime, IsDeleted, SkipMapKey, ValueOffset},
    storage_engine::StorageEngine,
};

pub(crate) type Key = Vec<u8>;
pub(crate) type ValOffset = usize;

pub struct FetchedEntry {
    pub Key: Vec<u8>,
    pub Val: Vec<u8>,
}
pub struct RangeIterator<'a> {
    start: &'a [u8],
    current: u32,
    end: &'a [u8],
    allow_prefetch: bool,
    prefetch_entries_size: usize,
    prefetch_entries: Vec<FetchedEntry>,
    keys: Vec<Vec<(Key, (ValueOffset, InsertionTime, IsDeleted))>>,
}

impl<'a> RangeIterator<'a> {
    fn new(
        start: &'a [u8],
        end: &'a [u8],
        allow_prefetch: bool,
        prefetch_entries_size: usize,
    ) -> Self {
        Self {
            start,
            current: 0,
            end,
            allow_prefetch,
            prefetch_entries_size,
            prefetch_entries: Vec::new(),
            keys: Vec::new(),
        }
    }

    fn next(&mut self) -> Option<FetchedEntry> {
        None
    }
    fn prev(&mut self) -> Option<FetchedEntry> {
        None
    }
    fn key<K>(&mut self) -> Option<K> {
        None
    }

    fn value<V>(&mut self) -> Option<V> {
        None
    }

    // Move the iterator to the end of the collection.
    fn end(&mut self) -> Option<FetchedEntry> {
        None
    }
}

impl<'a> Iterator for RangeIterator<'a> {
    type Item = FetchedEntry;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl StorageEngine<Vec<u8>> {
    // Start if the range query
    pub async fn seek<'a>(&self, start: &'a [u8], end: &'a [u8]) -> impl Iterator + 'a {
        let mut entries = Vec::new();
        // check entries within active memtable
        if !self.active_memtable.index.is_empty() {
            if self
                .active_memtable
                .index
                .lower_bound(std::ops::Bound::Included(start))
                .is_some()
                || self
                    .active_memtable
                    .index
                    .upper_bound(std::ops::Bound::Included(end))
                    .is_some()
            {
                entries.push(
                    self.active_memtable
                        .clone()
                        .index
                        .iter()
                        .map(|e| {
                            Entry::new(e.key().to_vec(), e.value().0, e.value().1, e.value().2)
                        })
                        .collect::<Vec<Entry<Vec<u8>, usize>>>(),
                );
            }
        }
        // check inactive memtable
        if !self.read_only_memtables.read().await.is_empty() {
            let read_only_memtables = self.read_only_memtables.read().await.clone();

            for (_, memtable) in read_only_memtables {
                let memtable_ref = memtable.read().await;
                if memtable_ref
                    .index
                    .lower_bound(std::ops::Bound::Included(start))
                    .is_some()
                    || memtable_ref
                        .index
                        .upper_bound(std::ops::Bound::Included(end))
                        .is_some()
                {
                    entries.push(
                        memtable_ref
                            .clone()
                            .index
                            .iter()
                            .map(|e| {
                                Entry::new(e.key().to_vec(), e.value().0, e.value().1, e.value().2)
                            })
                            .collect::<Vec<Entry<Key, ValOffset>>>(),
                    );
                }
            }
        }

        let sstables_within_range = {
            let mut sstable_path = HashMap::new();
            let bloom_filters = self.bloom_filters.read().await;
            for b in bloom_filters.iter(){
              if b.contains(&start.to_vec()) || b.contains(&end.to_vec()){
                sstable_path.insert(b.sstable_path.as_ref().unwrap().data_file_path.to_str().unwrap(), b.sstable_path.as_ref());
              }
            }
            let key_range = self.key_range.read().await;
            let paths_from_key_range = key_range.range_scan(&start.to_vec(), &end.to_vec());
            if !paths_from_key_range.is_empty(){
                for path in paths_from_key_range.iter() {
                    let sparse_index =
                        SparseIndex::new(path.index_file_path.clone()).await;

                    match sparse_index.get(&key).await {
                        Ok(None) => continue,
                        Ok(result) => {
                            if let Some(block_offset) = result {
                                let sst = SSTable::new_with_exisiting_file_path(
                                    sstable.dir.to_owned(),
                                    sstable.data_file_path.to_owned(),
                                    sstable.index_file_path.to_owned(),
                                );
                                match sst.get(block_offset, &key).await {
                                    Ok(None) => continue,
                                    Ok(result) => {
                                        if let Some((
                                            value_offset,
                                            created_at,
                                            is_tombstone,
                                        )) = result
                                        {
                                            if created_at > most_recent_insert_time {
                                                offset = value_offset;
                                                most_recent_insert_time = created_at;
                                                is_deleted = is_tombstone;
                                            }
                                        }
                                    }
                                    Err(err) => error!("{}", err),
                                }
                            }
                        }
                        Err(err) => error!("{}", err),
                    }
                }
            }
            // let bucket_map = self.buckets.read().await;
            // for (_, bucket) in &bucket_map.buckets{
              
            // }
             None
        };
        println!("{:?}", entries);
        RangeIterator::new(
            start,
            end,
            self.config.allow_prefetch,
            self.config.prefetch_size,
        )
    }
}
