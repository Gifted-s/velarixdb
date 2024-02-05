use serde::de::value;

use crate::{
    memtable::{InMemoryTable, DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MEMTABLE_CAPACITY},
    sstable::SSTable,
    value_log::{ValueLog, VLOG_FILE_NAME},
};
use std::hash::Hash;
use std::{clone, fs, io, path::PathBuf};

pub(crate) static DEFAULT_ALLOW_PREFETCH: bool = true;
pub(crate) static DEFAULT_PREFETCH_SIZE: usize = 32;

pub struct StorageEngine<K: Hash + PartialOrd> {
    pub dir: DirPath,
    pub memtable: InMemoryTable<K>,
    pub val_log: ValueLog,
    pub sstables: Vec<SSTable>,
    pub key_index: LevelsBiggestKeys,
    pub allow_prefetch: bool,
    pub prefetch_size: usize,
}

pub struct LevelsBiggestKeys {
    levels: Vec<Vec<u32>>,
}
impl LevelsBiggestKeys {
    pub fn new(levels: Vec<Vec<u32>>) -> Self {
        Self { levels }
    }
}

pub struct DirPath {
    root: PathBuf,
    val_log: PathBuf,
    sst: PathBuf,
    meta: PathBuf,
}

#[derive(Clone, Copy)]
pub enum SizeUnit {
    Bytes,
    Kilobytes,
    Megabytes,
    Gigabytes,
}

impl StorageEngine<Vec<u8>> {
    pub fn new(dir: PathBuf) -> io::Result<Self> {
        let dir = DirPath::build(dir);

        StorageEngine::with_capacity(dir, SizeUnit::Bytes, DEFAULT_MEMTABLE_CAPACITY)
    }

    /// A Result indicating success or an `io::Error` if an error occurred.
    pub fn put(&mut self, key: &str, value: &str) -> io::Result<bool> {
        // Convert the key and value into Vec<u8> from given &str.
        let key = &key.as_bytes().to_vec();
        let value = &value.as_bytes().to_vec();

        // Write to value log first which returns the offset
        let v_offset_opt = self.val_log.append(key, value);
        match v_offset_opt {
            Ok(v_offset) => {
                // then check if the length of the memtable + head offset > than memtable length
                // we will later store the head offset in the sstable
                // 4 bytes to store length of key "head"
                // 4 bytes to store the actual key "head"
                // 4 bytes to store the head offset
                if self.memtable.size() + 12 >= self.memtable.capacity() {
                    let capacity = self.memtable.capacity();
                    let size_unit = self.memtable.size_unit();
                    let false_positive_rate = self.memtable.false_positive_rate();

                    self.flush_memtable()?;

                    self.memtable = InMemoryTable::with_specified_capacity_and_rate(
                        size_unit,
                        capacity,
                        false_positive_rate,
                    );
                }
                self.memtable.insert(key, v_offset.try_into().unwrap())?;
                return Ok(true);
            }
            Err(err) => io::Error::new(err.kind(), "Could not write entry to value log"),
        };
        Ok(true)
    }

    /// A Result indicating success or an `io::Error` if an error occurred.
    pub fn get(&mut self, key: &str) -> Option<String> {
        let key = key.as_bytes().to_vec();
        let offset;
        if let Ok(Some(value_offset)) = self.memtable.get(&key) {
            offset = value_offset;
        } else {
            // search sstable
            // perform some magic here later
            match self.sstables[0].get(&key) {
                Ok(value_offset) => offset = value_offset,
                Err(err) => {
                    println!("Error fetching key {}", err);
                    return None;
                }
            }
        }

        let value = self.val_log.get(offset).unwrap();
        value
    }

    pub fn update(&mut self, key: &str, value: &str) -> io::Result<bool> {
        // Call set method defined in StorageEngine.
        self.put(key, value)
    }

    pub fn clear(mut self) -> io::Result<Self> {
        // Get the current capacity.
        let capacity = self.memtable.capacity();

        // Get the current size_unit.
        let size_unit = self.memtable.size_unit();

        // Get the current false_positive_rate.
        let false_positive_rate = self.memtable.false_positive_rate();

        // Delete the memtable by calling the `clear` method defined in MemTable.
        self.memtable.clear();

        // // Delete the wal by calling the `clear` method defined in ValueLog.
        // self.val_log.clear()?;

        // Call the build method of StorageEngine and return a new instance.
        StorageEngine::with_capacity_and_rate(
            self.dir,
            size_unit,
            capacity,
            false_positive_rate,
            DEFAULT_ALLOW_PREFETCH,
            DEFAULT_PREFETCH_SIZE,
        )
    }

    // if write + head offset is greater than size then flush to disk
    fn flush_memtable(&mut self) -> io::Result<()> {
        let index = self.memtable.index.clone();
        let bloom_filter = &self.memtable.bloom_filter;
        let mut sstable = SSTable::new(self.dir.sst.clone(), true);
        sstable.set_bloom_filter(bloom_filter);
        sstable.set_index(index);
        println!("Writing to sstable =================================================");
        //write the memtable to the disk as SS Tables
        match sstable.write_to_file() {
            Ok(_) => {
                self.memtable = self.memtable.clear();
                Ok(())
            }
            Err(err) => Err(io::Error::new(err.kind(), err.to_string())),
        }
    }

    pub(crate) fn with_capacity(
        dir: DirPath,
        size_unit: SizeUnit,
        capacity: usize,
    ) -> io::Result<Self> {
        Self::with_capacity_and_rate(
            dir,
            size_unit,
            capacity,
            DEFAULT_FALSE_POSITIVE_RATE,
            DEFAULT_ALLOW_PREFETCH,
            DEFAULT_PREFETCH_SIZE,
        )
    }

    pub fn with_capacity_and_rate(
        dir: DirPath,
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
        allow_prefetch: bool,
        prefetch_size: usize,
    ) -> io::Result<Self> {
        let vlog_path = dir.val_log.join(VLOG_FILE_NAME);
        let sst_path = dir.sst.clone();
        let vlog_exit = vlog_path.exists();
        let vlog_empty = !vlog_exit || fs::metadata(&vlog_path)?.len() == 0;

        // no ss table exists
        let sst_exit = sst_path.exists();
        let sst_empty = !sst_exit || fs::metadata(&sst_path)?.len() == 0;

        let key_index = LevelsBiggestKeys::new(Vec::new());
        let vlog = ValueLog::new(&dir.val_log)?;
        if vlog_empty || sst_empty {
            let memtable = InMemoryTable::with_specified_capacity_and_rate(
                size_unit,
                capacity,
                false_positive_rate,
            );

            let sstables = vec![SSTable::new(sst_path, false)];
            Ok(Self {
                memtable,
                val_log: vlog,
                sstables,
                dir,
                key_index,
                allow_prefetch,
                prefetch_size,
            })
        } else {
            // recover memtable
            let recover_result = StorageEngine::recover_memtable(
                size_unit,
                capacity,
                false_positive_rate,
                &sst_path,
                &dir.val_log,
            );

            match recover_result {
                Ok(memtable) => {
                    let sstables = vec![SSTable::new(sst_path, true)];
                    Ok(Self {
                        memtable,
                        val_log: vlog,
                        sstables,
                        dir,
                        key_index,
                        allow_prefetch,
                        prefetch_size,
                    })
                }
                Err(err) => Err(io::Error::new(
                    err.kind(),
                    "Error retriving entries from value logs",
                )),
            }
        }
    }

    fn recover_memtable(
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
        sst_path: &PathBuf,
        vlog_path: &PathBuf,
    ) -> io::Result<InMemoryTable<Vec<u8>>> {
        let mut memtable = InMemoryTable::with_specified_capacity_and_rate(
            size_unit,
            capacity,
            false_positive_rate,
        );
        // Normally we need to scan the in memory levels biggest key to know this sstable will posibly contain the "head" key
        let sstable = SSTable::new(sst_path.clone(), true);
        let head_opt = sstable.get(b"head");
        match head_opt {
            Ok(mut offset) => {
                let mut vlog = ValueLog::new(&vlog_path.clone())?;
                match vlog.recover(offset) {
                    Ok(entries) => {
                        for e in entries {
                            // we will be rewriting head to memtable but safe since we also set the offset of the head
                            memtable.insert(&e.key, offset.try_into().unwrap())?;
                            offset += e.ksize + e.vsize + e.key.len() + e.value.len();
                        }
                    }
                    Err(err) => {
                        println!("Error retrieving entries from value logs")
                    }
                }
                return Ok(memtable);
            }
            Err(err) => Err(io::Error::new(err.kind(), "read failure")),
        }
    }
}

impl DirPath {
    fn build(root_path: PathBuf) -> Self {
        let root = root_path;
        let val_log = root.join("v_log");
        let sst = root.join("sst");
        let meta = root.join("meta");
        Self {
            root,
            val_log,
            sst,
            meta,
        }
    }

    fn get_dir(&self) -> &str {
        self.root
            .to_str()
            .expect("Failed to convert pathj to string")
    }
}
impl SizeUnit {
    pub(crate) const fn to_bytes(&self, value: usize) -> usize {
        match self {
            SizeUnit::Bytes => value,
            SizeUnit::Kilobytes => value * 1024,
            SizeUnit::Megabytes => value * 1024 * 1024,
            SizeUnit::Gigabytes => value * 1024 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_dir;

    use super::*;

    #[test]
    fn storage_engine_create() {
        let k1 = "sunkanmi";
        let k2 = "ayomide";
        let k3 = "kolade";
        let k4 = "bodunde";

        let path = PathBuf::new().join("wired_tiger");
        let mut wt = StorageEngine::new(path.clone()).unwrap();

        wt.put(k1, "boyode").unwrap();
        wt.put(k2, "boyode").unwrap();
        wt.put(k3, "boyode").unwrap();

        let value1 = wt.get(k1);
        let value2 = wt.get(k2);
        let value3 = wt.get(k3);
        let value4 = wt.get(k4);
        assert_eq!(value1.unwrap().as_str(), "boyode");
        assert_eq!(value2.unwrap().as_str(), "boyode");
        assert_eq!(value3.unwrap().as_str(), "boyode");

        assert_eq!(value4, None);
        //fs::remove_dir_all(path.clone()).unwrap();
    }
}
