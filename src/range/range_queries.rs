// NOTE: STCS can handle range queries but scans within identified SSTables might be neccessary.
// Data for your range might be spread across multiple SSTables. Even with a successful bloom filter check,
// each identified SSTable might still contain data outside your desired range. For heavily range query-focused workloads, LCS or TWSC should be considered
// Although this stratedy is not available for now, It will be implmented in the future
use serde_json::map::Entry;

use crate::storage_engine::StorageEngine;

pub struct RangeIterator<'a> {
    start: u64,
    current: u64,
    end: u64,
    allow_prefetch: bool,
    prefetch_entries_size: usize,
    prefetch_entries: Vec<Entry<'a>>,
}

impl<'a> RangeIterator<'a> {
    fn new(start: u64, allow_prefetch: bool, prefetch_entries_size: usize) -> Self {
        Self {
            start,
            current: 0,
            end: 0,
            allow_prefetch,
            prefetch_entries_size,
            prefetch_entries: Vec::new(),
        }
    }

    fn next(&mut self) -> Option<Entry> {
        None
    }
    fn prev(&mut self) -> Option<Entry> {
        None
    }
    fn key<K>(&mut self) -> Option<K> {
        None
    }

    fn value<V>(&mut self) -> Option<V> {
        None
    }

    // Move the iterator to the end of the collection.
    fn end(&mut self) -> Option<Entry> {
        None
    }
}

impl<'a> Iterator for RangeIterator<'a> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl StorageEngine<Vec<u8>> {
    // Start if the range query
    pub async fn seek(&self, start: u64) -> impl Iterator {
        RangeIterator::new(start, self.config.allow_prefetch, self.config.prefetch_size)
    }
}
