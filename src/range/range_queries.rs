use serde_json::map::Entry;

use crate::storage_engine::StorageEngine;

struct RangeIterator<'a> {
    start: i64,
    current: i64,
    end: i64,
    allow_prefetch: bool,
    prefetch_entries_size: usize,
    prefetch_entries: Vec<Entry<'a>>,
}

impl<'a> RangeIterator<'a> {
    fn new(&self, start: i64, allow_prefetch: bool, prefetch_entries_size: usize) -> Self {
        Self {
            start,
            current: 0,
            end: 0,
            allow_prefetch,
            prefetch_entries_size,
            prefetch_entries: Vec::new(),
        }
    }

    fn seek(&mut self) -> Option<Entry> {
        None
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
        todo!()
    }
}


impl StorageEngine<Vec<u8>> {}
