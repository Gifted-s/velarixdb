use std::{cmp::Ordering, collections::HashMap, path::PathBuf};

#[derive(Clone, Debug)]
pub struct TableBiggestKeys {
    pub sstables: HashMap<PathBuf, Vec<u8>>,
}
impl TableBiggestKeys {
    pub fn new() -> Self {
        Self {
            sstables: HashMap::new(),
        }
    }

    pub fn set(&mut self, sst_path: PathBuf, biggest_key: Vec<u8>) -> bool {
        self.sstables.insert(sst_path, biggest_key).is_some()
    }

    pub fn remove(&mut self, sst_path: PathBuf) -> bool {
        self.sstables.remove(&sst_path).is_some()
    }

    // Returns SSTables whose last key is greater than the supplied key parameter
    pub fn filter_sstables_by_biggest_key(&self, key: &Vec<u8>) -> Vec<&PathBuf> {
        self.sstables
            .iter()
            .filter(|(_, key_prefix)| {
                key_prefix.as_slice().cmp(key) == Ordering::Greater
                    || key_prefix.as_slice().cmp(key) == Ordering::Equal
            })
            .map(|(path, _)| return path)
            .collect()
    }
}
