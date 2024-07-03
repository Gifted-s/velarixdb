use crate::err::Error::FilterNotFoundError;
use crate::filter::BloomFilter;
use crate::{bucket::InsertableToBucket, err::Error, types::*};
use crate::{
    consts::{SIZE_OF_U64, SIZE_OF_U8, SIZE_OF_USIZE},
    err::Error::{BiggestKeyIndexError, LowestKeyIndexError},
};
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TableInsertor {
    pub(crate) entries: SkipMapEntries<Key>,
    pub(crate) size: usize,
    pub(crate) filter: BloomFilter,
}

impl InsertableToBucket for TableInsertor {
    fn get_entries(&self) -> SkipMapEntries<Key> {
        Arc::clone(&self.entries)
    }
    fn get_filter(&self) -> BloomFilter {
        return self.filter.to_owned();
    }
    fn size(&self) -> usize {
        self.size
    }
}

impl TableInsertor {
    pub fn from(entries: SkipMapEntries<Key>, filter: &BloomFilter) -> Self {
        let size = entries
            .iter()
            .map(|e| e.key().len() + SIZE_OF_USIZE + SIZE_OF_U64 + SIZE_OF_U8)
            .sum::<usize>();
        Self {
            entries,
            size,
            filter: filter.to_owned(),
        }
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
impl Default for TableInsertor {
    fn default() -> Self {
        Self {
            entries: Arc::new(SkipMap::new()),
            size: 0,
            filter: BloomFilter::default(),
        }
    }
}
