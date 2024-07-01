use tokio::sync::RwLock;

use crate::{
    err::Error,
    filter::BloomFilter,
    sst::Table,
    types::{self, Key},
};
use std::{
    cmp::Ordering,
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

type LargestKey = types::Key;
type SmallestKey = types::Key;
#[derive(Clone, Debug)]
pub struct KeyRange {
    pub key_ranges: HashMap<PathBuf, Range>,
    pub restored_ranges: Arc<RwLock<HashMap<PathBuf, Range>>>,
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
            restored_ranges: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set<P: AsRef<Path> + Send + Sync, T: AsRef<[u8]>>(
        &mut self,
        sst_dir: P,
        smallest_key: T,
        biggest_key: T,
        table: Table,
    ) -> bool {
        self.key_ranges
            .insert(
                sst_dir.as_ref().to_path_buf(),
                Range::new(smallest_key.as_ref(), biggest_key.as_ref(), table),
            )
            .is_some()
    }

    pub fn remove<P: AsRef<Path> + Send + Sync>(&mut self, sst_path: P) -> bool {
        self.key_ranges.remove(sst_path.as_ref()).is_some()
    }

    // Returns SSTables whose last key is greater than the supplied key parameter
    pub async fn filter_sstables_by_biggest_key<K: AsRef<[u8]>>(&self, key: K) -> Result<Vec<Table>, Error> {
        let mut filtered_ssts: Vec<Table> = Vec::new();
        let has_restored_ranges = !self.restored_ranges.read().await.is_empty();
        if has_restored_ranges {
            filtered_ssts = self.check_restored_key_ranges(key.as_ref()).await?;
        }

        let mut restored_range_map: HashMap<PathBuf, Range> = HashMap::new();
        for (_, range) in self.key_ranges.iter() {
            if has_restored_ranges {
                if self.restored_ranges.read().await.contains_key(range.sst.dir.as_path()) {
                    continue;
                }
            }
            if range.biggest_key.as_slice().cmp(key.as_ref()) == Ordering::Greater
                || range.biggest_key.as_slice().cmp(key.as_ref()) == Ordering::Equal
            {
                //  If an sstable does not have a bloom filter then
                //  it means there has been a crash and we need to restore
                //  filter from disk using filter metadata stored on sstable
                if let None = range.sst.filter.as_ref().unwrap().sst_dir {
                    let mut mut_range = range.to_owned();
                    let mut filter = mut_range.sst.filter.as_ref().unwrap().to_owned();

                    filter.recover_meta().await?;
                    filter.sst_dir = Some(mut_range.sst.dir.to_owned());

                    let t = mut_range.sst.load_entries_from_file().await?;
                    filter.build_filter_from_entries(&t.entries);
                    // Don't keep sst entries in memory
                    mut_range.sst.entries.clear();
                    mut_range.sst.filter = Some(filter.to_owned());
                    restored_range_map.insert(mut_range.sst.dir.to_owned(), mut_range.to_owned());

                    if filter.contains(key.as_ref()) {
                        filtered_ssts.push(mut_range.sst)
                    }
                }
                if range.sst.filter.as_ref().unwrap().contains(key.as_ref()) {
                    filtered_ssts.push(range.sst.to_owned())
                }
            }
        }
        if !restored_range_map.is_empty() {
            // store the key ranges with sstables that contains
            // bloom filters just restored to disk in the restored_ranges map we are not
            // updating key_ranges immediatlely to prevent a mutable reference on get operations
            let restored_ranges = self.restored_ranges.clone();
            tokio::spawn(async move {
                *(restored_ranges.write().await) = restored_range_map;
            });
        }
        return Ok(filtered_ssts);
    }

    // Returns SSTables whose last key is greater than the supplied key parameter
    pub async fn check_restored_key_ranges<K: AsRef<[u8]>>(&self, key: K) -> Result<Vec<Table>, Error> {
        let mut filtered_ssts: Vec<Table> = Vec::new();
        let key_ranges = self.restored_ranges.read().await;
        for (_, range) in key_ranges.iter() {
            if range.biggest_key.as_slice().cmp(key.as_ref()) == Ordering::Greater
                || range.biggest_key.as_slice().cmp(key.as_ref()) == Ordering::Equal
            {
                if range.sst.filter.as_ref().unwrap().contains(key.as_ref()) {
                    filtered_ssts.push(range.sst.to_owned())
                }
            }
        }
        return Ok(filtered_ssts);
    }

    pub async fn update_key_range(&mut self) {
        let restored_ranges = self.restored_ranges.read().await;
        if !restored_ranges.is_empty() {
            for (path, range) in restored_ranges.iter() {
                self.key_ranges.insert(path.to_owned(), range.to_owned());
            }
            drop(restored_ranges);
            self.restored_ranges.write().await.clear();
        }
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
