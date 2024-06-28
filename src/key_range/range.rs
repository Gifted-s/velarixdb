use crate::{
    sst::Table,
    types::{self, Key},
};
use std::{
    cmp::Ordering,
    collections::HashMap,
    path::{Path, PathBuf},
};

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
    pub sst: Table,
}
impl Range {
    pub fn new<T: AsRef<[u8]>>(smallest_key: T, biggest_key: T, sst: Table) -> Self {
        Self {
            smallest_key: smallest_key.as_ref().to_vec(),
            biggest_key: biggest_key.as_ref().to_vec(),
            sst,
        }
    }
}
impl KeyRange {
    pub fn new() -> Self {
        Self {
            key_ranges: HashMap::new(),
        }
    }

    pub fn set<P: AsRef<Path> + Send + Sync, T: AsRef<[u8]>>(
        &mut self,
        sst_path: P,
        smallest_key: T,
        biggest_key: T,
        table: Table,
    ) -> bool {
        self.key_ranges
            .insert(
                sst_path.as_ref().to_path_buf(),
                Range::new(smallest_key.as_ref(), biggest_key.as_ref(), table),
            )
            .is_some()
    }

    pub fn remove<P: AsRef<Path> + Send + Sync>(&mut self, sst_path: P) -> bool {
        self.key_ranges.remove(sst_path.as_ref()).is_some()
    }

    // Returns SSTables whose last key is greater than the supplied key parameter
    pub fn filter_sstables_by_biggest_key<K: AsRef<[u8]>>(&self, key: K) -> Vec<Table> {
        self.key_ranges
            .iter()
            .filter(|(_, range)| {
                range.biggest_key.as_slice().cmp(key.as_ref()) == Ordering::Greater
                    || range.biggest_key.as_slice().cmp(key.as_ref()) == Ordering::Equal
            })
            .map(|(_, range)| return range.sst.to_owned())
            .collect()
    }

    // Returns SSTables whose keys overlap with the key range supplied
    pub fn range_scan<T: AsRef<[u8]>>(&self, start_key: T, end_key: T) -> Vec<&Range> {
        self.key_ranges
            .iter()
            .filter(|(_, range)| {
                // Check minimum range
                (range.smallest_key.as_slice().cmp(start_key.as_ref()) == Ordering::Less
                    || range.smallest_key.as_slice().cmp(start_key.as_ref()) == Ordering::Equal)

                    // Check maximum range
                    || (range.biggest_key.as_slice().cmp(end_key.as_ref()) == Ordering::Greater
                        || range.biggest_key.as_slice().cmp(end_key.as_ref()) == Ordering::Equal)
            })
            .map(|(_, path)| path)
            .collect()
    }
}
