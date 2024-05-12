use crate::{
    sstable::SSTablePath,
    types::{self, Key},
};
use std::{cmp::Ordering, collections::HashMap, path::PathBuf};

type LargestKey = types::Key;
type SmallestKey = types::Key;
#[derive(Clone, Debug)]
pub struct KeyRange {
    pub key_ranges: HashMap<PathBuf, Range>,
}

#[derive(Clone, Debug)]
pub struct Range {
    pub smallest_key: SmallestKey,
    pub biggest_key: LargestKey,
    pub full_sst_path: SSTablePath,
}
impl Range {
    pub fn new(
        smallest_key: SmallestKey,
        biggest_key: LargestKey,
        full_sst_path: SSTablePath,
    ) -> Self {
        Self {
            smallest_key,
            biggest_key,
            full_sst_path,
        }
    }
}
impl KeyRange {
    pub fn new() -> Self {
        Self {
            key_ranges: HashMap::new(),
        }
    }

    pub fn set(
        &mut self,
        sst_path: PathBuf,
        smallest_key: SmallestKey,
        biggest_key: LargestKey,
        full_sst_path: SSTablePath,
    ) -> bool {
        self.key_ranges
            .insert(
                sst_path,
                Range::new(smallest_key, biggest_key, full_sst_path),
            )
            .is_some()
    }

    pub fn remove(&mut self, sst_path: PathBuf) -> bool {
        self.key_ranges.remove(&sst_path).is_some()
    }

    // Returns SSTables whose last key is greater than the supplied key parameter
    pub fn filter_sstables_by_biggest_key(&self, key: &Key) -> Vec<&PathBuf> {
        self.key_ranges
            .iter()
            .filter(|(_, range)| {
                range.biggest_key.as_slice().cmp(key) == Ordering::Greater
                    || range.biggest_key.as_slice().cmp(key) == Ordering::Equal
            })
            .map(|(path, _)| return path)
            .collect()
    }

    // Returns SSTables whose keys overlap with the key range supplied
    pub fn range_scan(&self, start_key: &SmallestKey, end_key: &LargestKey) -> Vec<&Range> {
        self.key_ranges
            .iter()
            .filter(|(_, range)| {
                // Check minimum range
                (range.smallest_key.as_slice().cmp(start_key) == Ordering::Less
                    || range.smallest_key.as_slice().cmp(start_key) == Ordering::Equal)

                    // Check maximum range
                    || (range.biggest_key.as_slice().cmp(end_key) == Ordering::Greater
                        || range.biggest_key.as_slice().cmp(end_key) == Ordering::Equal)
            })
            .map(|(_, path)| path)
            .collect()
    }
}
