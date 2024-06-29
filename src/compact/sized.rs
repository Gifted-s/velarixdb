use std::{cmp, collections::HashMap, hash::Hash, ops::Deref, path::PathBuf, sync::Arc};

use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;
use uuid::Uuid;

use super::{
    compact::{Config, MergePointer, WriteTracker},
    MergedSSTable, TableInsertor,
};
use crate::{
    bucket::{Bucket, ImbalancedBuckets, InsertableToBucket, SSTablesToRemove},
    err::Error,
    filter::BloomFilter,
    mem::Entry,
    sst::Table,
    types::{BloomFilterHandle, Bool, BucketMapHandle, CreatedAt, Key, KeyRangeHandle, ValOffset},
};
use crate::{err::Error::*, mem::SkipMapValue};

#[derive(Debug, Clone)]
pub struct SizedTierRunner<'a> {
    bucket_map: BucketMapHandle,
    filters: BloomFilterHandle,
    key_range: KeyRangeHandle,
    config: &'a Config,
    tombstones: HashMap<Key, CreatedAt>,
}

impl<'a> SizedTierRunner<'a> {
    pub fn new(
        bucket_map: BucketMapHandle,
        filters: BloomFilterHandle,
        key_range: KeyRangeHandle,
        config: &'a Config,
    ) -> SizedTierRunner<'a> {
        Self {
            tombstones: HashMap::new(),
            bucket_map,
            filters,
            key_range,
            config,
        }
    }
    pub async fn fetch_imbalanced_buckets(bucket_map: BucketMapHandle) -> ImbalancedBuckets {
        bucket_map.read().await.extract_imbalanced_buckets().await
    }
    pub async fn run_compaction(&mut self) -> Result<(), Error> {
        if self.bucket_map.read().await.is_balanced().await {
            return Ok(());
        }
        // The compaction loop will keep running until there
        // are no more buckets with more than minimum treshold size
        // TODO: Handle this with multiple threads
        loop {
            let buckets: BucketMapHandle = Arc::clone(&self.bucket_map);
            let filters = Arc::clone(&self.filters);
            let key_range = Arc::clone(&self.key_range);
            // Step 1: Extract imbalanced buckets
            let (imbalanced_buckets, ssts_to_remove) =
                SizedTierRunner::fetch_imbalanced_buckets(buckets.clone()).await?;
            if imbalanced_buckets.is_empty() {
                self.tombstones.clear();
                return Ok(());
            }

            // Step 2: Merge SSTs in each imbalanced buckct
            match self.merge_ssts_in_buckets(&imbalanced_buckets.to_owned()).await {
                Ok(merged_sstables) => {
                    let mut tracker = WriteTracker::new(merged_sstables.len());
                    // Step 3: Insert Merged SSTs to appropriate buckets
                    for mut merged_sst in merged_sstables.into_iter() {
                        let mut bucket = buckets.write().await;
                        let table = merged_sst.clone().sstable;
                        let insert_res = bucket.insert_to_appropriate_bucket(Arc::new(table)).await;
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
                                        if let Err(err) = tokio::fs::remove_dir_all(table.dir).await {
                                            log::error!("{}", CompactionFailed(Box::new(DirDeleteError(err))));
                                            tracker.actual -= 1;
                                            continue;
                                        }
                                        key_range.write().await.remove(table.data_file.path);
                                    }
                                    tracker.actual -= 1;
                                }
                                return Err(CompactionFailed(Box::new(err)));
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
                        log::error!("{}", Error::CannotRemoveObsoleteSSTError)
                    }
                }
                Err(err) => return Err(CompactionFailed(Box::new(err))),
            }
        }
    }

    pub async fn clean_up_after_compaction(
        &self,
        buckets: BucketMapHandle,
        ssts_to_delete: &SSTablesToRemove,
        filters: BloomFilterHandle,
        key_range: KeyRangeHandle,
    ) -> Result<Option<()>, Error> {
        // Remove obsolete keys from keys range
        ssts_to_delete.iter().for_each(|(_, sstables)| {
            sstables.iter().for_each(|s| {
                let range = Arc::clone(&key_range);
                let path = s.get_data_file_path();
                tokio::spawn(async move {
                    range.write().await.remove(path);
                });
            })
        });

        // if all obsolete sstables were not deleted then don't remove the associated filters
        // although this can lead to redundancy but bloom filters are in-memory and its also less costly
        // in terms of memory
        if buckets.write().await.delete_ssts(ssts_to_delete).await? {
            // Step 8: Delete the filters associated with the sstables that we already merged
            self.filter_out_old_filter(filters, ssts_to_delete).await;
            return Ok(Some(()));
        }
        Ok(None)
    }

    pub async fn filter_out_old_filter(&self, filters: BloomFilterHandle, ssts_to_delete: &Vec<(Uuid, Vec<Table>)>) {
        let mut filter_map: HashMap<PathBuf, BloomFilter> = filters
            .read()
            .await
            .iter()
            .map(|b| (b.get_sst().dir.to_owned(), b.to_owned()))
            .collect();
        ssts_to_delete.iter().for_each(|(_, tables)| {
            tables.iter().for_each(|t| {
                filter_map.remove(&t.dir);
            })
        });
        filters.write().await.clear();
        filters.write().await.extend(filter_map.into_values());
    }

    async fn merge_ssts_in_buckets(&mut self, buckets: &Vec<Bucket>) -> Result<Vec<MergedSSTable>, Error> {
        let mut merged_ssts = Vec::new();
        for bucket in buckets.iter() {
            let mut hotness = 0;
            let tables = &bucket.sstables.read().await;
            let mut merged_sst: Box<dyn InsertableToBucket> = Box::new(tables.get(0).unwrap().to_owned());
            for sst in tables[1..].iter() {
                hotness += sst.hotness;
                let table = sst
                    .load_entries_from_file()
                    .await
                    .map_err(|err| CompactionFailed(Box::new(err)))?;
                merged_sst = self
                    .merge_sstables(merged_sst, Box::new(table))
                    .await
                    .map_err(|err| CompactionFailed(Box::new(err)))?;
            }
            let filter = Table::build_filter_from_sstable(&merged_sst.get_entries(), self.config.filter_false_positive);
            merged_ssts.push(MergedSSTable::new(merged_sst, filter, hotness));
        }
        if merged_ssts.is_empty() {
            return Err(CompactionFailed(Box::new(MergeSSTContainsZeroEntries)));
        }
        Ok(merged_ssts)
    }

    async fn merge_sstables(
        &mut self,
        sst1: Box<dyn InsertableToBucket>,
        sst2: Box<dyn InsertableToBucket>,
    ) -> Result<Box<dyn InsertableToBucket>, Error> {
        let mut new_sst = TableInsertor::new();
        let new_sst_map = Arc::new(SkipMap::new());
        let mut merged_entries = Vec::new();
        let entries1 = sst1
            .get_entries()
            .iter()
            .map(|e| {
                Entry::new(
                    e.key().to_vec(),
                    e.value().val_offset,
                    e.value().created_at,
                    e.value().is_tombstone,
                )
            })
            .collect::<Vec<Entry<Key, ValOffset>>>();
        let entries2 = sst2
            .get_entries()
            .iter()
            .map(|e| {
                Entry::new(
                    e.key().to_vec(),
                    e.value().val_offset,
                    e.value().created_at,
                    e.value().is_tombstone,
                )
            })
            .collect::<Vec<Entry<Key, ValOffset>>>();
        let mut ptr = MergePointer::new();

        while ptr.ptr1 < entries1.len() && ptr.ptr2 < entries2.len() {
            match entries1[ptr.ptr1].key.cmp(&entries2[ptr.ptr2].key) {
                cmp::Ordering::Less => {
                    self.tombstone_check(&entries1[ptr.ptr1], &mut merged_entries)
                        .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    ptr.increment_ptr1();
                }
                cmp::Ordering::Equal => {
                    if entries1[ptr.ptr1].created_at > entries2[ptr.ptr2].created_at {
                        self.tombstone_check(&entries1[ptr.ptr1], &mut merged_entries)
                            .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    } else {
                        self.tombstone_check(&entries2[ptr.ptr2], &mut merged_entries)
                            .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    }
                    ptr.increment_ptr1();
                    ptr.increment_ptr2();
                }
                cmp::Ordering::Greater => {
                    self.tombstone_check(&entries2[ptr.ptr2], &mut merged_entries)
                        .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
                    ptr.increment_ptr2();
                }
            }
        }
        while ptr.ptr1 < entries1.len() {
            self.tombstone_check(&entries1[ptr.ptr1], &mut merged_entries)
                .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
            ptr.increment_ptr1();
        }

        while ptr.ptr2 < entries2.len() {
            self.tombstone_check(&entries2[ptr.ptr2], &mut merged_entries)
                .map_err(|err| TombStoneCheckFailed(err.to_string()))?;
            ptr.increment_ptr2();
        }

        merged_entries.iter().for_each(|e| {
            new_sst_map.insert(
                e.key.to_owned(),
                SkipMapValue::new(e.val_offset, e.created_at, e.is_tombstone),
            );
        });
        new_sst.set_entries(new_sst_map);
        Ok(Box::new(new_sst))
    }

    fn tombstone_check(
        &mut self,
        entry: &Entry<Key, usize>,
        merged_entries: &mut Vec<Entry<Key, usize>>,
    ) -> Result<Bool, Error> {
        let mut should_insert = false;
        if self.tombstones.contains_key(&entry.key) {
            let tomb_insert_time = *self.tombstones.get(&entry.key).unwrap();
            if entry.created_at > tomb_insert_time {
                if entry.is_tombstone {
                    self.tombstones.insert(entry.key.to_owned(), entry.created_at);
                    should_insert = !entry.to_owned().has_expired(self.config.tombstone_ttl);
                } else {
                    if self.config.use_ttl {
                        should_insert = !entry.has_expired(self.config.entry_ttl);
                    } else {
                        should_insert = true
                    }
                }
            }
        } else {
            if entry.is_tombstone {
                self.tombstones.insert(entry.key.to_owned(), entry.created_at);
                should_insert = !entry.has_expired(self.config.tombstone_ttl);
            } else {
                if self.config.use_ttl {
                    should_insert = !entry.has_expired(self.config.entry_ttl);
                } else {
                    should_insert = true
                }
            }
        }
        if should_insert {
            merged_entries.push(entry.clone())
        }
        Ok(true)
    }
}
