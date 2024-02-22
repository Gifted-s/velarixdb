use crate::{
    bloom_filter::{self, BloomFilter},
    compaction::{Bucket, BucketMap, Compactor, SSTablePath},
    memtable::{Entry, InMemoryTable, DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MEMTABLE_CAPACITY},
    sstable::SSTable,
    value_log::{ValueLog, VLOG_FILE_NAME},
};
use chrono::{DateTime, Utc};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::de::value;
use std::{
    clone,
    collections::HashMap,
    fs,
    io::{self, Error},
    mem,
    path::{Component, Path, PathBuf},
};
use std::{cmp, hash::Hash};

pub(crate) static DEFAULT_ALLOW_PREFETCH: bool = true;
pub(crate) static DEFAULT_PREFETCH_SIZE: usize = 32;

pub struct StorageEngine<K: Hash + PartialOrd + std::cmp::Ord> {
    pub(crate) dir: DirPath,
    pub(crate) memtable: InMemoryTable<K>,
    pub(crate) bloom_filters: Vec<BloomFilter>,
    pub(crate) val_log: ValueLog,
    pub(crate) buckets: BucketMap,
    pub(crate) key_index: LevelsBiggestKeys,
    pub(crate) allow_prefetch: bool,
    pub(crate) prefetch_size: usize,
    pub(crate) compactor: Compactor,
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
    buckets: PathBuf,
    meta: PathBuf,
}

#[derive(Clone, Copy, Debug)]
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
        let created_at = Utc::now().timestamp_millis() as u64;

        // Write to value log first which returns the offset
        let v_offset_opt = self.val_log.append(key, value, created_at);
        match v_offset_opt {
            Ok(v_offset) => {
                // then check if the length of the memtable + head offset > than memtable length
                // we will later store the head offset in the sstable
                // 4 bytes to store length of key "head"
                // 4 bytes to store the actual key "head"
                // 4 bytes to store the head offset
                // 8 bytes to store the head entry creation date
                if self.memtable.size() + 20 >= self.memtable.capacity() {
                    let capacity = self.memtable.capacity();
                    let size_unit = self.memtable.size_unit();
                    let false_positive_rate = self.memtable.false_positive_rate();
                    let head_offset = self.memtable.index.iter().max_by_key(|e| e.value().0);
                    let head_entry = Entry::new(
                        b"head".to_vec(),
                        head_offset.unwrap().value().0,
                        Utc::now().timestamp_millis() as u64,
                    );
                    let _ = self.memtable.insert(&head_entry);
                    let flush_result = self.flush_memtable();
                    match flush_result {
                        Ok(_) => {
                            self.memtable = InMemoryTable::with_specified_capacity_and_rate(
                                size_unit,
                                capacity,
                                false_positive_rate,
                            );
                        }
                        Err(err) => {
                            return Err(io::Error::new(err.kind(), err.to_string()));
                        }
                    }
                }
                let entry = Entry::new(key.to_vec(), v_offset.try_into().unwrap(), created_at);
                self.memtable.insert(&entry)?;
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
            // match self.sstables[0].get(&key) {
            //     Ok(value_offset) => offset = value_offset,
            //     Err(err) => {
            //         println!("Error fetching key {}", err);
            //         return None;
            //     }
            // }
        }

        let value = self.val_log.get(0).unwrap();
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
        let hotness = 1;
        let insert_result = self
            .buckets
            .insert_to_appropriate_bucket(&self.memtable, hotness);
        // let mut sstable = SSTable::new(self.dir.sst.clone(), true);
        println!(" ====================== FLUSHING TO SS TABLE ===========================");
        //write the memtable to the disk as SS Tables
        match insert_result {
            Ok(sstable_path) => {
                // insert to bloom filter
                let mut bf = self.memtable.get_bloom_filter();
                bf.set_sstable_path(sstable_path);
                self.bloom_filters.push(bf);

                // sort bloom filter by hotness
                self.bloom_filters.sort_by(|a, b| {
                    b.get_sstable_path()
                        .get_hotness()
                        .cmp(&a.get_sstable_path().get_hotness())
                });
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
        let buckets_path = dir.buckets.clone();
        let vlog_exit = vlog_path.exists();
        let vlog_empty = !vlog_exit || fs::metadata(&vlog_path)?.len() == 0;

        // no ss table exists
        let buckets_dir_exit = buckets_path.exists();
        let buckets_empty = !buckets_dir_exit || fs::metadata(&buckets_path)?.len() == 0;

        let key_index = LevelsBiggestKeys::new(Vec::new());
        let vlog = ValueLog::new(&dir.val_log)?;
        if vlog_empty {
            let memtable = InMemoryTable::with_specified_capacity_and_rate(
                size_unit,
                capacity,
                false_positive_rate,
            );

            return Ok(Self {
                memtable,
                val_log: vlog,
                bloom_filters: Vec::new(),
                buckets: BucketMap::new(buckets_path.clone()),
                dir,
                key_index,
                allow_prefetch,
                prefetch_size,
                compactor: Compactor::new(),
            });
        }

        let mut recovered_buckets: HashMap<uuid::Uuid, Bucket> = HashMap::new();
        let mut bloom_filters: Vec<BloomFilter> = Vec::new();
        let mut most_recent_head_timestamp = 0;
        let mut most_recent_head_offset = 0;
        for buckets_directories in fs::read_dir(buckets_path.clone())? {
            for bucket_dir in fs::read_dir(buckets_directories.unwrap().path())? {
                if let Ok(entry) = bucket_dir {
                    let file_path = entry.path();
                    // Check if the entry is a file
                    if file_path.is_file() {
                        let sstable_path_str = file_path.to_string_lossy().to_string();
                        let bucket_id =
                            Self::get_bucket_id_from_full_bucket_path(sstable_path_str.clone());
                        let sst_path = SSTablePath::new(sstable_path_str.clone());

                        match uuid::Uuid::parse_str(&bucket_id) {
                            Ok(bucket_uuid) => {
                                if let Some(b) = recovered_buckets.get(&bucket_uuid) {
                                    let mut temp_sstables = b.sstables.clone();
                                    temp_sstables.push(sst_path.clone());
                                    let updated_bucket =
                                        Bucket::new_with_id_dir_average_and_sstables(
                                            PathBuf::new().join(sstable_path_str.clone()),
                                            bucket_uuid,
                                            temp_sstables.to_owned(),
                                            0,
                                        );
                                    recovered_buckets.insert(bucket_uuid, updated_bucket);
                                } else {
                                    let updated_bucket =
                                        Bucket::new_with_id_dir_average_and_sstables(
                                            PathBuf::new().join(sstable_path_str.clone()),
                                            bucket_uuid,
                                            vec![sst_path.clone()],
                                            0,
                                        );
                                    recovered_buckets.insert(bucket_uuid, updated_bucket);
                                }

                                let sstable_from_file = SSTable::from_file(
                                    PathBuf::new().join(sstable_path_str.clone()),
                                );
                                let sstable = sstable_from_file.as_ref().unwrap().as_ref().unwrap();
                                let (head_offset, date_created) =
                                    sstable.get_value_from_index(b"head");

                                if date_created > most_recent_head_timestamp {
                                    most_recent_head_offset = head_offset;
                                    most_recent_head_timestamp = date_created;
                                }
                                let mut bf = SSTable::build_bloomfilter_from_sstable(
                                    &sstable.index, // Handle error later
                                );
                                bf.set_sstable_path(sst_path.clone());
                                bloom_filters.push(bf)
                            }
                            Err(err) => {
                                return Err(Error::new(
                                    io::ErrorKind::InvalidInput,
                                    err.to_string(),
                                ))
                            }
                        }
                    }
                }
            }
        }

        let mut buckets_map = BucketMap::new(buckets_path.clone());
        buckets_map.set_buckets(recovered_buckets);
        // buckets_map.buckets.iter().for_each(|b| {
        //     println!("==========RECOVERED BUCKETDS===={:?}============", b.1);
        // });

        // bloom_filters.iter().for_each(|b| {
        //     println!("==========RECOVERED Bloom filters ===={:?}============", b);
        // });

        //println!("MOST recent head offset {}", most_recent_head_offset);
        // recover memtable
        let recover_result = StorageEngine::recover_memtable(
            size_unit,
            capacity,
            false_positive_rate,
            &buckets_path,
            &dir.val_log,
            most_recent_head_offset,
        );

        match recover_result {
            Ok(memtable) => {
                println!("Heren is the memtable {:?}", memtable.index.len());
                return Ok(Self {
                    memtable,
                    val_log: vlog,
                    dir,
                    buckets: buckets_map,
                    bloom_filters,
                    key_index,
                    allow_prefetch,
                    prefetch_size,
                    compactor: Compactor::new(),
                });
            }
            Err(err) => {
                return Err(io::Error::new(
                    err.kind(),
                    "Error retriving entries from value logs",
                ));
            }
        }
        Err(io::Error::new(io::ErrorKind::Other, "Unknown Error"))
    }
    fn recover_memtable(
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
        sst_path: &PathBuf,
        vlog_path: &PathBuf,
        mut head_offset: usize,
    ) -> io::Result<InMemoryTable<Vec<u8>>> {
        let mut memtable = InMemoryTable::with_specified_capacity_and_rate(
            size_unit,
            capacity,
            false_positive_rate,
        );

        let mut vlog = ValueLog::new(&vlog_path.clone())?;
        match vlog.recover(head_offset) {
            Ok(entries) => {
                for e in entries {
                    println!("en {:?}", e);
                    let entry = Entry::new(e.key.to_owned(), head_offset, e.created_at);
                    // we will be rewriting head to memtable but safe since we also set the offset of the head
                    memtable.insert(&entry)?;
                    // Entry Length
                    // Key Size -> for fetching key length
                    // Value Length -> for fetching value length
                    // Date Length
                    // Key Length
                    // Value Length
                    head_offset += mem::size_of::<u32>()
                        + mem::size_of::<u32>()
                        + mem::size_of::<u32>()
                        + mem::size_of::<u64>()
                        + e.key.len()
                        + e.value.len();
                }
            }
            Err(err) => {
                println!("Error retrieving entries from value logs inner {}", err.to_string())
            }
        }
        return Ok(memtable);
    }

    pub fn run_compaction(&mut self) {
        let _ = self
            .compactor
            .run_compaction(&mut self.buckets, &mut self.bloom_filters);
    }

    pub fn get_bucket_id_from_full_bucket_path(full_path: String) -> String {
        // Find the last occurrence of "bucket" in the file path
        if let Some(idx) = full_path.rfind("bucket") {
            // Extract the substring starting from the index after the last occurrence of "bucket"
            let uuid_part = &full_path[idx + "bucket".len()..];
            if let Some(end_idx) = uuid_part.find('/') {
                // Extract the UUID
                let uuid = &uuid_part[..end_idx];
                uuid.to_string()
            } else {
                "".to_owned()
            }
        } else {
            "".to_owned()
        }
    }
}

impl DirPath {
    fn build(root_path: PathBuf) -> Self {
        let root = root_path;
        let val_log = root.join("v_log");
        let buckets = root.join("buckets");
        let meta = root.join("meta");
        Self {
            root,
            val_log,
            buckets,
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

    use crate::bloom_filter;

    use super::*;

    #[test]
    fn storage_engine_create() {
        let path = PathBuf::new().join("bump");
        let mut s_engine = StorageEngine::new(path.clone()).unwrap();

        // Specify the number of random strings to generate
        let num_strings = 50000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = generate_random_string(string_length);
            random_strings.push(random_string);
        }

        // Print the generated random strings
        // for (_, s) in random_strings.iter().enumerate() {
        //     s_engine.put(s, "boyode").unwrap();
        // }
        // let compactor = Compactor::new();

        // let compaction_opt = s_engine.compactor.run_compaction(&mut s_engine.buckets, &mut s_engine.bloom_filters);
        // match compaction_opt {
        //     Ok(_)=>{
        //         println!("Compaction is now successful");
        //         println!("Length of bucket after compaction {:?}", s_engine.buckets.buckets.len());
        //         println!("Length of bloom filters after compaction {:?}", s_engine.bloom_filters.len());
        //     }
        //     Err(err)=>{
        //         println!("Error during compaction {}", err)
        //     }
        // }
        // let bloom_filters = s_engine.compactor.run_compaction(&mut s_engine.buckets, &mut s_engine.bloom_filters);
        // s_engine.bloom_filters = bloom_filters.unwrap();

        // wt.put(k2, "boyode").unwrap();
        // wt.put(k3, "boyode").unwrap();

        // let value1 = wt.get(k1);
        // let value2 = wt.get(k2);
        // let value3 = wt.get(k3);
        // let value4 = wt.get(k4);
        // assert_eq!(value1.unwrap().as_str(), "boyode");
        // assert_eq!(value2.unwrap().as_str(), "boyode");
        // assert_eq!(value3.unwrap().as_str(), "boyode");

        // assert_eq!(value4, None);
    }
}

fn generate_random_string(length: usize) -> String {
    let rng = thread_rng();
    rng.sample_iter(&Alphanumeric)
        .take(length)
        .map(|c| c as char)
        .collect()
}
