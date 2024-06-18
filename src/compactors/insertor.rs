use std::sync::Arc;

use crate::{
    consts::{SIZE_OF_U64, SIZE_OF_U8, SIZE_OF_USIZE},
    err::Error::{BiggestKeyIndexError, LowestKeyIndexError},
};
use crossbeam_skiplist::SkipMap;
//TODO this should be from the types module not memtable
use crate::{bucket::InsertableToBucket, err::Error, types::*};

#[derive(Debug, Clone)]
pub struct TableInsertor {
    pub(crate) entries: SkipMapEntries<Key>,
    pub(crate) size: usize,
}

// TODO: This is redundant
impl InsertableToBucket for TableInsertor {
    fn get_entries(&self) -> SkipMapEntries<Key> {
        Arc::clone(&self.entries)
    }
    fn size(&self) -> usize {
        self.size
    }
    fn find_biggest_key(&self) -> Result<Vec<u8>, Error> {
        let largest_entry = self.entries.iter().next_back();
        match largest_entry {
            Some(e) => return Ok(e.key().to_vec()),
            None => Err(BiggestKeyIndexError),
        }
    }

    // Find the biggest element in the skip list
    fn find_smallest_key(&self) -> Result<Vec<u8>, Error> {
        let largest_entry = self.entries.iter().next();
        match largest_entry {
            Some(e) => return Ok(e.key().to_vec()),
            None => Err(LowestKeyIndexError),
        }
    }
}

impl TableInsertor {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(SkipMap::new()),
            size: 0,
        }
    }
    pub fn from(entries: SkipMapEntries<Key>) -> Self {
        let size = entries
            .iter()
            .map(|e| e.key().len() + SIZE_OF_USIZE + SIZE_OF_U64 + SIZE_OF_U8)
            .sum::<usize>();
        Self { entries, size }
    }

    pub(crate) fn set_entries(&mut self, entries: SkipMapEntries<Key>) {
        self.entries = entries;
        self.set_sst_size_from_entries();
    }
    pub(crate) fn set_sst_size_from_entries(&mut self) {
        self.size = self
            .entries
            .iter()
            .map(|e| e.key().len() + SIZE_OF_USIZE + SIZE_OF_U64 + SIZE_OF_U8)
            .sum::<usize>();
    }
}
