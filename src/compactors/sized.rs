use std::{cmp, collections::HashMap, path::PathBuf, sync::Arc};

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
    memtable::Entry,
    sst::Table,
    types::{BloomFilterHandle, Bool, BucketMapHandle, CreatedAt, Key, KeyRangeHandle, ValOffset},
};
use crate::{err::Error::*, memtable::SkipMapValue};

/// Sized Tier Compaction Runner (STCS)
///
/// Responsible for merging sstables of almost similar sizes to form a bigger one,
/// obsolete entries are removed from the sstables and expired tombstones are removed
///
#[derive(Debug, Clone)]
pub struct SizedTierRunner<'a> {
    /// A thread-safe BucketMap with each bucket mapped to its id
    bucket_map: BucketMapHandle,

    /// A thread-safe vector of sstable bloom filters
    filters: BloomFilterHandle,

    /// A thread-safe hashmap of sstables each mapped to its key range
    key_range: KeyRangeHandle,

    /// Compaction configuration
    config: &'a Config,

    /// Keeps track of tombstones encountered during compaction
    /// to predict validity of subseqeunt entries
    tombstones: HashMap<Key, CreatedAt>,
}

impl<'a> SizedTierRunner<'a> {
    /// creates new instance of `SizedTierRunner`
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

    /// Returns buckets whose size exceeds max threshold
    pub async fn fetch_imbalanced_buckets(bucket_map: BucketMapHandle) -> ImbalancedBuckets {
        bucket_map.read().await.extract_imbalanced_buckets().await
    }

    /// Main compaction runner
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
                    for merged_sst in merged_sstables.into_iter() {
                        let mut bucket = buckets.write().await;
                        let table = merged_sst.clone().sstable;
                        let insert_res = bucket.insert_to_appropriate_bucket(Arc::new(table)).await;
                        drop(bucket);
                        match insert_res {
                            Ok(sst) => {
                                if sst.summary.is_none() {
                                    return Err(TableSummaryIsNoneError);
                                }
                                if sst.filter.is_none() {
                                    return Err(FilterNotProvidedForFlush);
                                }
                                // IMPORTANT: Don't keep sst entries in memory
                                sst.entries.clear();
                                // Step 4 Store Filter in Filters Vec
                                filters.write().await.push(sst.filter.to_owned().unwrap());
                                let summary = sst.summary.clone().unwrap();
                                // Step 5 Store sst key range
                                key_range.write().await.set(
                                    sst.dir.to_owned(),
                                    summary.smallest_key,
                                    summary.biggest_key,
                                    sst,
                                );
                                tracker.actual += 1;
                            }
                            Err(err) => {
                                // Step 6 (Optional): Trigger recovery in case compaction failed at any point
                                // Ensure filters are restored to the previous state
                                // Remove merged sstables written to disk so far to prevent stale data
                                while tracker.actual > 0 {
                                    if let Some(filter) = filters.write().await.pop() {
                                        let sst_dir = filter.get_sst_dir().to_owned();
                                        if let Err(err) = tokio::fs::remove_dir_all(sst_dir.to_owned()).await {
                                            log::error!("{}", CompactionFailed(Box::new(DirDeleteError(err))));
                                            tracker.actual -= 1;
                                            continue;
                                        }
                                        key_range.write().await.remove(sst_dir.to_owned());
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

    /// Removes sstables that are already merged to form larger table(s)
    ///
    /// NOTE: This should only be called if merged sstables have been written to disk
    /// otherwise data loss can happen
    ///
    /// # Errors
    ///
    /// Returns error if deletion fails
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

    /// Remove filters of obsolete sstables from filters vector
    pub async fn filter_out_old_filter(&self, filters: BloomFilterHandle, ssts_to_delete: &Vec<(Uuid, Vec<Table>)>) {
        let mut filter_map: HashMap<PathBuf, BloomFilter> = filters
            .read()
            .await
            .iter()
            .map(|b| (b.sst_dir.to_owned().unwrap(), b.to_owned()))
            .collect();
        ssts_to_delete.iter().for_each(|(_, tables)| {
            tables.iter().for_each(|t| {
                filter_map.remove(&t.dir);
            })
        });
        filters.write().await.clear();
        filters.write().await.extend(filter_map.into_values());
    }

    /// Merges the sstables in each `Bucket` to form a larger one
    ///
    /// Returns `Result` with merged sstable or error
    ///
    /// # Errors
    ///
    /// Returns error incase an error occured during merge
    async fn merge_ssts_in_buckets(&mut self, buckets: &Vec<Bucket>) -> Result<Vec<MergedSSTable>, Error> {
        let mut merged_ssts = Vec::new();
        for bucket in buckets.iter() {
            let mut hotness = 0;
            let tables = &bucket.sstables.read().await;

            let mut merged_sst: Box<dyn InsertableToBucket> = Box::new(tables.get(0).unwrap().to_owned());
            for sst in tables[1..].iter() {
                let mut insertable_sst = sst.to_owned();
                hotness += insertable_sst.hotness;
                insertable_sst
                    .load_entries_from_file()
                    .await
                    .map_err(|err| CompactionFailed(Box::new(err)))?;
                merged_sst = self
                    .merge_sstables(merged_sst, Box::new(insertable_sst))
                    .await
                    .map_err(|err| CompactionFailed(Box::new(err)))?;
            }

            let entries = &merged_sst.get_entries();
            let mut filter = BloomFilter::new(self.config.filter_false_positive, entries.len());
            filter.build_filter_from_entries(entries);
            merged_ssts.push(MergedSSTable::new(merged_sst, filter, hotness));
        }
        if merged_ssts.is_empty() {
            return Err(CompactionFailed(Box::new(MergeSSTContainsZeroEntries)));
        }
        Ok(merged_ssts)
    }

    /// Merge two `Table` together one returns a larger one
    ///
    /// Errors
    ///
    /// Returns error if an error occured during merge
    async fn merge_sstables(
        &mut self,
        sst1: Box<dyn InsertableToBucket>,
        sst2: Box<dyn InsertableToBucket>,
    ) -> Result<Box<dyn InsertableToBucket>, Error> {
        let mut new_sst = TableInsertor::default();
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
                    self.tombstone_check(&entries1[ptr.ptr1], &mut merged_entries);

                    ptr.increment_ptr1();
                }
                cmp::Ordering::Equal => {
                    if entries1[ptr.ptr1].created_at > entries2[ptr.ptr2].created_at {
                        self.tombstone_check(&entries1[ptr.ptr1], &mut merged_entries);
                    } else {
                        self.tombstone_check(&entries2[ptr.ptr2], &mut merged_entries);
                    }
                    ptr.increment_ptr1();
                    ptr.increment_ptr2();
                }
                cmp::Ordering::Greater => {
                    self.tombstone_check(&entries2[ptr.ptr2], &mut merged_entries);
                    ptr.increment_ptr2();
                }
            }
        }

        while ptr.ptr1 < entries1.len() {
            self.tombstone_check(&entries1[ptr.ptr1], &mut merged_entries);
            ptr.increment_ptr1();
        }

        while ptr.ptr2 < entries2.len() {
            self.tombstone_check(&entries2[ptr.ptr2], &mut merged_entries);
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

    /// Checks if an entry has been deleted or not
    /// 
    /// Deleted entries are discoverd using the tombstones hashmap
    /// and prevented from being inserted
    ///
    /// Returns true if entry should be inserted or false otherwise
    fn tombstone_check(&mut self, entry: &Entry<Key, usize>, merged_entries: &mut Vec<Entry<Key, usize>>) -> Bool {
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
        true
    }
}
