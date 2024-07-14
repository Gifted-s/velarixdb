use std::{cmp, collections::HashMap, sync::Arc};

use crossbeam_skiplist::SkipMap;

use super::{
    compact::{Config, MergePointer, WriteTracker},
    MergedSSTable, TableInsertor,
};
use crate::{
    bucket::{Bucket, ImbalancedBuckets, InsertableToBucket, SSTablesToRemove},
    err::Error,
    filter::BloomFilter,
    memtable::Entry,
    types::{Bool, BucketMapHandle, CreatedAt, Key, KeyRangeHandle, ValOffset},
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
    pub fn new(bucket_map: BucketMapHandle, key_range: KeyRangeHandle, config: &'a Config) -> SizedTierRunner<'a> {
        Self {
            tombstones: HashMap::new(),
            bucket_map,
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
                                    return Err(TableSummaryIsNone);
                                }
                                if sst.filter.is_none() {
                                    return Err(FilterNotProvidedForFlush);
                                }
                                // IMPORTANT: Don't keep sst entries in memory
                                sst.entries.clear();
                                let summary = sst.summary.clone().unwrap();
                                // Step 5 Store sst key range
                                key_range
                                    .set(sst.dir.to_owned(), summary.smallest_key, summary.biggest_key, sst)
                                    .await;
                                tracker.actual += 1;
                            }
                            Err(err) => {
                                return Err(CompactionFailed(Box::new(err)));
                            }
                        }
                    }

                    if tracker.expected == tracker.actual {
                        // Step 6:  Delete the sstables that we already merged from their previous buckets and update bloom filters
                        let filters_updated = self
                            .clean_up_after_compaction(buckets, &ssts_to_remove.clone(), key_range)
                            .await;
                        match filters_updated {
                            Ok(None) => {
                                return Err(Error::CompactionPartiallyFailed(Box::new(CompactionCleanupPartial)));
                            }
                            Err(err) => {
                                return Err(Error::CompactionCleanup(Box::new(err)));
                            }
                            _ => {}
                        }
                    } else {
                        log::error!("{}", Error::CannotRemoveObsoleteSST)
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
        key_range: KeyRangeHandle,
    ) -> Result<Option<()>, Error> {
        // if all obsolete sstables were not deleted then don't remove the associated filters
        // although this can lead to redundancy but bloom filters are in-memory and its also less costly
        // in terms of memory
        if buckets.write().await.delete_ssts(ssts_to_delete).await? {
            // Step 7: Remove obsolete keys from keys range
            ssts_to_delete.iter().for_each(|(_, sstables)| {
                sstables.iter().for_each(|s| {
                    let range = Arc::clone(&key_range);
                    let path = s.get_data_file_path();
                    tokio::spawn(async move {
                        range.remove(path).await;
                    });
                })
            });
            return Ok(Some(()));
        }
        Ok(None)
    }

    /// Merges the sstables in each `Bucket` to form a larger one
    ///
    /// Returns `Result` with merged sstable or error
    ///
    /// # Errors
    ///
    /// Returns error incase an error occured during merge
    async fn merge_ssts_in_buckets(&mut self, buckets: &[Bucket]) -> Result<Vec<MergedSSTable>, Error> {
        let mut merged_ssts = Vec::new();
        for bucket in buckets.iter() {
            let mut hotness: u64 = Default::default();
            let tables = &bucket.sstables.read().await;

            let mut merged_sst: Box<dyn InsertableToBucket> = Box::new(tables.first().unwrap().to_owned());
            for sst in tables[1..].iter() {
                let mut insertable_sst = sst.to_owned();
                hotness += insertable_sst.hotness;
                insertable_sst
                    .load_entries_from_file()
                    .await
                    .map_err(|err| CompactionFailed(Box::new(err)))?;

                // TODO: merge_sstables() can be CPU intensive so we should use spawn blocking here
                // tokio::task::spawn_blocking(||{
                       // merge sstable here
                // });
                merged_sst = self
                    .merge_sstables(merged_sst, Box::new(insertable_sst))
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
    fn merge_sstables(
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
                } else if self.config.use_ttl {
                    should_insert = !entry.has_expired(self.config.entry_ttl);
                } else {
                    should_insert = true
                }
            }
        } else if entry.is_tombstone {
            self.tombstones.insert(entry.key.to_owned(), entry.created_at);
            should_insert = !entry.has_expired(self.config.tombstone_ttl);
        } else if self.config.use_ttl {
            should_insert = !entry.has_expired(self.config.entry_ttl);
        } else {
            should_insert = true
        }
        if should_insert {
            merged_entries.push(entry.clone())
        }
        true
    }
}
