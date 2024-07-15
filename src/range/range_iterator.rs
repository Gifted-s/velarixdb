
use crate::err::Error;
use crate::db::DataStore;
use crate::memtable::Entry;
use crate::types::{Key, ValOffset, Value};
use crate::vlog::ValueLog;

#[derive(Debug, Clone)]
pub struct FetchedEntry {
    pub key: Key,
    pub val: Value,
}

#[derive(Debug, Clone)]
pub struct RangeIterator<'a> {
    pub start: &'a [u8],
    pub current: usize,
    pub end: &'a [u8],
    pub allow_prefetch: bool,
    pub prefetch_entries_size: usize,
    pub prefetch_entries: Vec<FetchedEntry>,
    pub keys: Vec<Entry<Key, ValOffset>>,
    pub v_log: ValueLog,
}

impl<'a> RangeIterator<'a> {
    fn new(
        start: &'a [u8],
        end: &'a [u8],
        allow_prefetch: bool,
        prefetch_entries_size: usize,
        keys: Vec<Entry<Key, ValOffset>>,
        v_log: ValueLog,
    ) -> Self {
        Self {
            start,
            current: 0,
            end,
            allow_prefetch,
            prefetch_entries_size,
            prefetch_entries: Vec::new(),
            keys,
            v_log,
        }
    }
}

impl<'a> DataStore<'a, Key> {
    // TODO: range query
    pub async fn seek(&self, _: &'a [u8], _: &'a [u8]) -> Result<RangeIterator, Error> {
      
        let range_iterator = RangeIterator::<'a>::new(
            &[1],
            &[2],
            self.config.allow_prefetch,
            self.config.prefetch_size,
            Merger::new().entries,
            self.val_log.clone(),
        );
        Ok(range_iterator)
    }
}
pub struct Merger {
    entries: Vec<Entry<Key, ValOffset>>,
}
impl Merger {
    fn new() -> Self {
        Self { entries: Vec::new() }
    }
}
