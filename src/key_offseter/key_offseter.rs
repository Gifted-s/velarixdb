use std::{cmp::Ordering, collections::HashMap, path::PathBuf};

type LargestKeyType = Vec<u8>;
type SmallestKeyType = Vec<u8>;
#[derive(Clone, Debug)]
pub struct KeyRange {
    pub key_ranges: HashMap<PathBuf, (SmallestKeyType, LargestKeyType)>,
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
        smallest_key: SmallestKeyType,
        biggest_key: LargestKeyType,
    ) -> bool {
        self.key_ranges
            .insert(sst_path, (smallest_key, biggest_key))
            .is_some()
    }

    pub fn remove(&mut self, sst_path: PathBuf) -> bool {
        self.key_ranges.remove(&sst_path).is_some()
    }

    // Returns SSTables whose last key is greater than the supplied key parameter
    pub fn filter_sstables_by_biggest_key(&self, key: &Vec<u8>) -> Vec<&PathBuf> {
        self.key_ranges
            .iter()
            .filter(|(_, (_, biggest_key))| {
                biggest_key.as_slice().cmp(key) == Ordering::Greater
                    || biggest_key.as_slice().cmp(key) == Ordering::Equal
            })
            .map(|(path, _)| return path)
            .collect()
    }

    // Returns SSTables whose keys overlap with the key range supplied
    pub fn range_scan(
        &self,
        start_key: &SmallestKeyType,
        end_key: &LargestKeyType,
    ) -> Vec<&PathBuf> {
        self.key_ranges
            .iter()
            .filter(|(_, (smallest_key, biggest_key))| {
                // Check minimum range
                (smallest_key.as_slice().cmp(start_key) == Ordering::Less
                    || smallest_key.as_slice().cmp(start_key) == Ordering::Equal)

                    // Check maximum range
                    || (biggest_key.as_slice().cmp(end_key) == Ordering::Greater
                        || biggest_key.as_slice().cmp(end_key) == Ordering::Equal)
            })
            .map(|(path, _)| path)
            .collect()
    }
}
