use crate::{
    bloom_filter::BloomFilter,
    cfg::Config,
    compaction::{Bucket, BucketMap, Compactor},
    consts::{
        BUCKETS_DIRECTORY_NAME, DEFAULT_ALLOW_PREFETCH, DEFAULT_FALSE_POSITIVE_RATE,
        DEFAULT_MEMTABLE_CAPACITY, DEFAULT_PREFETCH_SIZE, ENABLE_TTL, ENTRY_TTL, GC_THREAD_COUNT,
        HEAD_ENTRY_LENGTH, META_DIRECTORY_NAME, TOMB_STONE, VALUE_LOG_DIRECTORY_NAME,
    },
    err::StorageEngineError,
    memtable::{Entry, InMemoryTable},
    meta::Meta,
    sstable::{SSTable, SSTablePath},
    value_log::ValueLog,
};
use chrono::Utc;
use log::info;
use num_traits::ToBytes;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::hash::Hash;
use std::{collections::HashMap, fs, mem, path::PathBuf};

use crate::err::StorageEngineError::*;

pub struct StorageEngine<K: Hash + PartialOrd + std::cmp::Ord> {
    pub dir: DirPath,
    pub memtable: InMemoryTable<K>,
    pub bloom_filters: Vec<BloomFilter>,
    pub val_log: ValueLog,
    pub buckets: BucketMap,
    pub key_index: LevelsBiggestKeys,
    pub compactor: Compactor,
    pub meta: Meta,
    pub config: Config,
}

pub struct LevelsBiggestKeys {
    levels: Vec<Vec<u32>>,
}
impl LevelsBiggestKeys {
    pub fn new(levels: Vec<Vec<u32>>) -> Self {
        Self { levels }
    }
}

#[derive(Clone, Debug)]
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
    pub fn new(dir: PathBuf) -> Result<Self, StorageEngineError> {
        let dir = DirPath::build(dir);
        let default_config = Config::default();

        StorageEngine::with_default_capacity_and_config(
            dir.clone(),
            SizeUnit::Bytes,
            DEFAULT_MEMTABLE_CAPACITY,
            &default_config,
        )
    }

    pub fn new_with_custom_config(
        dir: PathBuf,
        config: &Config,
    ) -> Result<Self, StorageEngineError> {
        let dir = DirPath::build(dir);
        StorageEngine::with_default_capacity_and_config(
            dir.clone(),
            SizeUnit::Bytes,
            DEFAULT_MEMTABLE_CAPACITY,
            config,
        )
    }

    /// A Result indicating success or an `StorageEngineError` if an error occurred.
    pub fn put(&mut self, key: &str, value: &str) -> Result<bool, StorageEngineError> {
        // Convert the key and value into Vec<u8> from given &str.
        let key = &key.as_bytes().to_vec();
        let value = &value.as_bytes().to_vec();
        let created_at = Utc::now().timestamp_millis() as u64;
        let is_tombstone = false;
        // Write to value log first which returns the offset
        let v_offset = self.val_log.append(key, value, created_at, is_tombstone)?;

        // then check if the length of the memtable + head offset > than memtable length
        // store the head offset in the sstable for recovery in case of crash
        if self.memtable.size() + HEAD_ENTRY_LENGTH >= self.memtable.capacity() {
            let capacity = self.memtable.capacity();
            let size_unit = self.memtable.size_unit();
            let false_positive_rate = self.memtable.false_positive_rate();
            let head_offset = self.memtable.index.iter().max_by_key(|e| e.value().0);
            let head_entry = Entry::new(
                b"head".to_vec(),
                head_offset.unwrap().value().0,
                Utc::now().timestamp_millis() as u64,
                false,
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
                    return Err(FlushToDiskError {
                        error: Box::new(err),
                    });
                }
            }
        }
        let entry = Entry::new(
            key.to_vec(),
            v_offset.try_into().unwrap(),
            created_at,
            is_tombstone,
        );
        self.memtable.insert(&entry)?;
        Ok(true)
    }

    // A Result indicating success or an `io::Error` if an error occurred.
    pub fn get(&mut self, key: &str) -> Result<(Vec<u8>, u64), StorageEngineError> {
        let key = key.as_bytes().to_vec();
        let mut offset = 0;
        let mut most_recent_insert_time = 0;

        // Step 1: Check if key exist in MemTable
        if let Ok(Some((value_offset, creation_date, is_tombstone))) = self.memtable.get(&key) {
            offset = value_offset;
            most_recent_insert_time = creation_date;
            if is_tombstone {
                return Err(KeyFoundAsTombstoneInMemtableError);
            }
        } else {
            // Step 2: If key does not exist in MemTable then we can load sstables that probaby contains this key fr8om bloom filter
            let sstable_paths =
                BloomFilter::get_sstable_paths_that_contains_key(&self.bloom_filters, &key);
            match sstable_paths {
                Some(paths) => {
                    // Step 3: Get the most recent value offset from sstables
                    let mut is_deleted = false;
                    for sst_path in paths.iter() {
                        let sstable = SSTable::new_with_exisiting_file_path(sst_path.get_path());
                        match sstable.get(&key) {
                            Ok(result) => {
                                if let Some((value_offset, created_at, is_tombstone)) = result {
                                    if created_at > most_recent_insert_time {
                                        offset = value_offset;
                                        most_recent_insert_time = created_at;
                                        is_deleted = is_tombstone;
                                    }
                                }
                            }
                            Err(_) => {}
                        }
                    }

                    if most_recent_insert_time > 0 && is_deleted {
                        return Err(KeyFoundAsTombstoneInSSTableError);
                    }
                }
                None => {
                    return Err(KeyNotFoundInAnySSTableError);
                }
            }
        }

        // most_recent_insert_time cannot be zero unless did not find this key in any sstable
        if most_recent_insert_time > 0 {
            // Step 5: Read value from value log based on offset
            let value: Option<(Vec<u8>, bool)> = self.val_log.get(offset)?;
            match value {
                Some((v, is_tombstone)) => {
                    if is_tombstone {
                        return Err(KeyFoundAsTombstoneInValueLogError);
                    }
                    return Ok((v, most_recent_insert_time));
                }
                None => return Err(KeyNotFoundInValueLogError),
            };
        }
        Err(NotFoundInDB)
    }

    /// A Result indicating success or an `io::Error` if an error occurred.
    pub fn delete(&mut self, key: &str) -> Result<bool, StorageEngineError> {
        // First check if the key exist before triggering a deletion
        // Return error if not
        self.get(key)?;

        // Convert the key and value into Vec<u8> from given &str.
        let key = &key.as_bytes().to_vec();
        let value = &TOMB_STONE.to_le_bytes().to_vec(); // Value will be a thumbstone
        let created_at = Utc::now().timestamp_millis() as u64;
        let is_tombstone = true;

        // Write to value log first which returns the offset
        let v_offset = self.val_log.append(key, value, created_at, is_tombstone)?;

        // then check if the length of the memtable + head offset > than memtable length
        // head offset is stored in sstable for recovery incase of crash
        if self.memtable.size() + HEAD_ENTRY_LENGTH >= self.memtable.capacity() {
            let capacity = self.memtable.capacity();
            let size_unit = self.memtable.size_unit();
            let false_positive_rate = self.memtable.false_positive_rate();
            let head_offset = self.memtable.index.iter().max_by_key(|e| e.value().0);
            let head_entry = Entry::new(
                b"head".to_vec(),
                head_offset.unwrap().value().0,
                Utc::now().timestamp_millis() as u64,
                is_tombstone,
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
                    return Err(FlushToDiskError {
                        error: Box::new(err),
                    });
                }
            }
        }
        let entry = Entry::new(
            key.to_vec(),
            v_offset.try_into().unwrap(),
            created_at,
            is_tombstone,
        );
        self.memtable.insert(&entry)?;
        Ok(true)
    }

    pub fn update(&mut self, key: &str, value: &str) -> Result<bool, StorageEngineError> {
        // Call set method defined in StorageEngine.
        self.put(key, value)
    }

    pub fn clear(&mut self) -> Result<Self, StorageEngineError> {
        // Get the current capacity.
        let capacity = self.memtable.capacity();

        // Get the current size_unit.
        let size_unit = self.memtable.size_unit();

        // Delete the memtable by calling the `clear` method defined in MemTable.
        self.memtable.clear();

        let config = self.config.clone();
        // Delete the velue_log by calling the `clear` method defined in ValueLog.
        // self.val_log.clear()?;

        // Call the build method of StorageEngine and return a new instance.
        StorageEngine::with_capacity_and_rate(self.dir.clone(), size_unit, capacity, &config)
    }

    // if write + head offset is greater than size then flush to disk
    fn flush_memtable(&mut self) -> Result<(), StorageEngineError> {
        let hotness = 1;
        let insert_result = self
            .buckets
            .insert_to_appropriate_bucket(&self.memtable, hotness);
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
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn with_default_capacity_and_config(
        dir: DirPath,
        size_unit: SizeUnit,
        capacity: usize,
        config: &Config,
    ) -> Result<Self, StorageEngineError> {
        Self::with_capacity_and_rate(dir, size_unit, capacity, &config)
    }

    fn with_capacity_and_rate(
        dir: DirPath,
        size_unit: SizeUnit,
        capacity: usize,
        config: &Config,
    ) -> Result<Self, StorageEngineError> {
        let vlog_path = &dir.val_log;
        let buckets_path = dir.buckets.clone();
        let vlog_exit = vlog_path.exists();
        let vlog = ValueLog::new(vlog_path);
        let vlog_empty =
            !vlog_exit || fs::metadata(vlog_path).map_err(GetFileMetaDataError)?.len() == 0;

        let key_index = LevelsBiggestKeys::new(Vec::new());
        let vlog = ValueLog::new(vlog_path)?;
        let meta = Meta::new(&dir.meta);
        if vlog_empty {
            let memtable = InMemoryTable::with_specified_capacity_and_rate(
                size_unit,
                capacity,
                config.false_positive_rate,
            );

            return Ok(Self {
                memtable,
                val_log: vlog,
                bloom_filters: Vec::new(),
                buckets: BucketMap::new(buckets_path.clone()),
                dir,
                key_index,
                compactor: Compactor::new(config.enable_ttl, config.entry_ttl_millis),
                config: config.clone(),
                meta,
            });
        }

        let mut recovered_buckets: HashMap<uuid::Uuid, Bucket> = HashMap::new();
        let mut bloom_filters: Vec<BloomFilter> = Vec::new();
        let mut most_recent_head_timestamp = 0;
        let mut most_recent_head_offset = 0;

        // engine_root/buckets/bucket{id}
        for buckets_directories in
            fs::read_dir(buckets_path.clone()).map_err(|err| BucketDirectoryOpenError {
                path: buckets_path.clone(),
                error: err,
            })?
        {
            //  engine_root/buckets/bucket{id}/sstable_{timestamp}_.db
            for bucket_dir in
                fs::read_dir(buckets_directories.as_ref().unwrap().path()).map_err(|err| {
                    BucketDirectoryOpenError {
                        path: buckets_directories.as_ref().unwrap().path(),
                        error: err,
                    }
                })?
            {
                if let Ok(entry) = bucket_dir {
                    let sstable_path_str = entry.path();
                    // Check if the entry is a file
                    if sstable_path_str.is_file() {
                        // Extract bucket id
                        let bucket_id =
                            Self::get_bucket_id_from_full_bucket_path(sstable_path_str.clone());
                        let sst_path = SSTablePath::new(sstable_path_str.clone());

                        let bucket_uuid = uuid::Uuid::parse_str(&bucket_id).map_err(|err| {
                            InvaidUUIDParseString {
                                input_string: bucket_id,
                                error: err,
                            }
                        })?;
                        // If bucket already exisit in recovered bucket then just append sstable to its sstables vector
                        if let Some(b) = recovered_buckets.get(&bucket_uuid) {
                            let mut temp_sstables = b.sstables.clone();
                            temp_sstables.push(sst_path.clone());
                            let updated_bucket = Bucket::new_with_id_dir_average_and_sstables(
                                buckets_directories.as_ref().unwrap().path(),
                                bucket_uuid,
                                temp_sstables.to_owned(),
                                0,
                            );
                            recovered_buckets.insert(bucket_uuid, updated_bucket);
                        } else {
                            // Create new bucket
                            let updated_bucket = Bucket::new_with_id_dir_average_and_sstables(
                                buckets_directories.as_ref().unwrap().path(),
                                bucket_uuid,
                                vec![sst_path.clone()],
                                0,
                            );
                            recovered_buckets.insert(bucket_uuid, updated_bucket);
                        }

                        let sstable_from_file =
                            SSTable::from_file(PathBuf::new().join(sstable_path_str.clone()));
                        let sstable = sstable_from_file.as_ref().unwrap().as_ref().unwrap();

                        // We need to fetch the most recent write offset so it can
                        // use it to recover entries not written into sstables from value log
                        let fetch_result = sstable.get_value_from_index(b"head");

                        if let Some((head_offset, date_created, _)) = fetch_result {
                            if date_created > most_recent_head_timestamp {
                                most_recent_head_offset = head_offset;
                                most_recent_head_timestamp = date_created;
                            }
                        }

                        let mut bf = SSTable::build_bloomfilter_from_sstable(&sstable.index);
                        bf.set_sstable_path(sst_path.clone());
                        // update bloom filters
                        bloom_filters.push(bf)
                    }
                }
            }
        }

        let mut buckets_map = BucketMap::new(buckets_path.clone());
        buckets_map.set_buckets(recovered_buckets);

        // recover memtable
        let recover_result = StorageEngine::recover_memtable(
            size_unit,
            capacity,
            config.false_positive_rate,
            &dir.val_log,
            most_recent_head_offset,
        );

        match recover_result {
            Ok(memtable) => Ok(Self {
                memtable,
                val_log: vlog,
                dir,
                buckets: buckets_map,
                bloom_filters,
                key_index,
                meta,
                compactor: Compactor::new(config.enable_ttl, config.entry_ttl_millis),
                config: config.clone(),
            }),
            Err(err) => Err(MemTableRecoveryError(Box::new(err))),
        }
    }
    fn recover_memtable(
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
        vlog_path: &PathBuf,
        head_offset: usize,
    ) -> Result<InMemoryTable<Vec<u8>>, StorageEngineError> {
        let mut memtable = InMemoryTable::with_specified_capacity_and_rate(
            size_unit,
            capacity,
            false_positive_rate,
        );

        let mut vlog = ValueLog::new(&vlog_path.clone())?;
        let mut most_recent_offset = head_offset;
        let entries = vlog.recover(head_offset)?;

        for e in entries {
            let entry = Entry::new(
                e.key.to_owned(),
                most_recent_offset,
                e.created_at,
                e.is_tombstone,
            );
            // Since the most recent offset is the offset we start reading entries from in value log
            // and we retrieved this from the sstable, therefore should not re-write the initial entry in
            // memtable since it's already in the sstable
            if most_recent_offset != head_offset {
                memtable.insert(&entry)?;
            }
            most_recent_offset += mem::size_of::<u32>() // Key Size -> for fetching key length
                        + mem::size_of::<u32>() // Value Length -> for fetching value length
                        + mem::size_of::<u64>() // Date Length
                        + mem::size_of::<u8>() // tombstone marker
                        + e.key.len() // Key Length
                        + e.value.len(); // Value Length
        }

        Ok(memtable)
    }

    fn run_compaction(&mut self) -> Result<bool, StorageEngineError> {
        self.compactor
            .run_compaction(&mut self.buckets, &mut self.bloom_filters)
    }

    fn get_bucket_id_from_full_bucket_path(full_path: PathBuf) -> String {
        let full_path_as_str = full_path.to_string_lossy().to_string();
        let mut bucket_id = String::new();
        // Find the last occurrence of "bucket" in the file path
        if let Some(idx) = full_path_as_str.rfind("bucket") {
            // Extract the substring starting from the index after the last occurrence of "bucket"
            let uuid_part = &full_path_as_str[idx + "bucket".len()..];
            if let Some(end_idx) = uuid_part.find('/') {
                // Extract the UUID
                let uuid = &uuid_part[..end_idx];
                bucket_id = uuid.to_string();
            }
        }
        bucket_id
    }
}

impl DirPath {
    pub(crate) fn build(root_path: PathBuf) -> Self {
        let root = root_path;
        let val_log = root.join(VALUE_LOG_DIRECTORY_NAME);
        let buckets = root.join(BUCKETS_DIRECTORY_NAME);
        let meta = root.join(META_DIRECTORY_NAME);
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
            .expect("Failed to convert path to string")
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
    use super::*;
    use crate::bloom_filter;
    use log::info;
    use std::fs::remove_dir;
    use StorageEngineError::*;

    // Generate test to find keys after compaction
    #[test]
    fn storage_engine_create() {
        let path = PathBuf::new().join("bump");
        let mut s_engine = StorageEngine::new(path.clone()).unwrap();

        // Specify the number of random strings to generate
        let num_strings = 10000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = generate_random_string(string_length);
            random_strings.push(random_string);
        }

        // Insert the generated random strings
        for (_, s) in random_strings.iter().enumerate() {
            s_engine.put(s, "boyode").unwrap();
        }
        // let compactor = Compactor::new();

        let compaction_opt = s_engine.run_compaction();
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    s_engine.buckets.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    s_engine.bloom_filters.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }
        // sort to make fetch random
        random_strings.sort();

        for key in random_strings.clone() {
            assert_eq!(s_engine.get(&key).unwrap().0, b"boyode");
        }

        let _ = fs::remove_dir_all(path.clone());
    }

    #[test]
    fn storage_engine_compaction() {
        let path = PathBuf::new().join("bump2");
        let mut s_engine = StorageEngine::new(path.clone()).unwrap();

        // Specify the number of random strings to generate
        let num_strings = 10000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = generate_random_string(string_length);
            random_strings.push(random_string);
        }

        // Insert the generated random strings
        for (_, s) in random_strings.iter().enumerate() {
            s_engine.put(s, "boyode").unwrap();
        }

        // sort to make fetch random
        random_strings.sort();
        let key = &random_strings[0];

        let get_res = s_engine.get(key);
        match get_res {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        let del_res = s_engine.delete(key);
        match del_res {
            Ok(v) => {
                assert_eq!(v, true)
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }
        let _ = s_engine.flush_memtable();
        s_engine.memtable.clear();

        let get_res = s_engine.get(key);
        match get_res {
            Ok(_) => {
                assert!(false, "Should not be found after compaction")
            }
            Err(err) => {
                assert_eq!(
                    StorageEngineError::KeyFoundAsTombstoneInSSTableError.to_string(),
                    err.to_string()
                )
            }
        }

        let compaction_opt = s_engine.run_compaction();
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    s_engine.buckets.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    s_engine.bloom_filters.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }

        // Insert the generated random strings

        let get_res = s_engine.get(key);
        match get_res {
            Ok(_) => {
                assert!(false, "Deleted key should not be found after compaction");
            }

            Err(err) => {
                if err.to_string() != KeyFoundAsTombstoneInSSTableError.to_string()
                    && err.to_string() != KeyFoundAsTombstoneInSSTableError.to_string()
                {
                    assert!(
                        false,
                        "Key should be mapped to tombstone or deleted from all sstables"
                    )
                }
            }
        }
        let _ = fs::remove_dir_all(path.clone());
    }

    #[test]
    fn storage_engine_deletion() {
        let path = PathBuf::new().join("bump3");
        let mut s_engine = StorageEngine::new(path.clone()).unwrap();

        // Specify the number of random strings to generate
        let num_strings = 10000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        random_strings.push("aunkanmi".to_owned());

        for _ in 0..num_strings {
            let random_string = generate_random_string(string_length);
            random_strings.push(random_string);
        }

        // Insert the generated random strings
        for (_, s) in random_strings.iter().enumerate() {
            s_engine.put(s, "boyode").unwrap();
        }

        // sort to make fetch random
        random_strings.sort();
        let key = "aunkanmi";
        let get_res = s_engine.get(key);
        match get_res {
            Ok((value, _)) => {
                assert_eq!(value, "boyode".as_bytes().to_vec());
            }
            Err(err) => {
                assert_ne!(key.as_bytes().to_vec(), err.to_string().as_bytes().to_vec());
            }
        }

        let del_res = s_engine.delete(key);
        match del_res {
            Ok(v) => {
                assert_eq!(v, true);
            }
            Err(err) => {
                assert!(err.to_string().is_empty())
            }
        }
        let _ = s_engine.flush_memtable();
        s_engine.memtable.clear();

        let get_res = s_engine.get(key);
        match get_res {
            Ok((_, _)) => {
                assert!(false, "Should not be executed")
            }
            Err(err) => {
                assert_eq!(
                    KeyFoundAsTombstoneInSSTableError.to_string(),
                    err.to_string()
                )
            }
        }

        let compaction_opt = s_engine.run_compaction();
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    s_engine.buckets.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    s_engine.bloom_filters.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }

        // Insert the generated random strings
        println!("trying to get this after compaction {}", key);
        let get_res = s_engine.get(key);
        match get_res {
            Ok((_, _)) => {
                assert!(false, "Should not ne executed")
            }
            Err(err) => {
                if err.to_string() != KeyFoundAsTombstoneInSSTableError.to_string()
                    && err.to_string() != KeyFoundAsTombstoneInSSTableError.to_string()
                {
                    assert!(
                        false,
                        "Key should be mapped to tombstone or deleted from all sstables"
                    )
                }
            }
        }
        let _ = fs::remove_dir_all(path.clone());
    }
}

fn generate_random_string(length: usize) -> String {
    let rng = thread_rng();
    rng.sample_iter(&Alphanumeric)
        .take(length)
        .map(|c| c as char)
        .collect()
}
