use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;
use num_traits::{ops::bytes, ToBytes};
use serde::de::value::Error;
use std::{
    cmp::Ordering,
    fs::{self, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{
    bloom_filter::{self, BloomFilter},
    memtable::{DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MEMTABLE_CAPACITY},
};

pub struct SSTable {
    file_path: PathBuf,
    index: Arc<SkipMap<Vec<u8>, usize>>,
    created_at: DateTime<Utc>,
}

impl SSTable {
    pub fn new(dir: PathBuf, create_file: bool) -> Self {
        let created_at = Utc::now();
        let file_name = format!("sstable_{}_.db", created_at.timestamp_millis());
        if !dir.exists() {
            fs::create_dir_all(&dir).expect("ss table directory was not created successfullt")
        }

        let file_path = dir.join(file_name);
        if create_file {
            _ = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(file_path.clone())
                .expect("error creating file");
        }
        let index = Arc::new(SkipMap::new());
        let bloom_filter = BloomFilter::new(DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MEMTABLE_CAPACITY);
        Self {
            file_path,
            index,
            created_at,
        }
    }

    pub(crate) fn write_to_file(&self) -> io::Result<()> {
        // Open the file in write mode with the append flag.
        let file_path = PathBuf::from(&self.file_path);
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(file_path)?;

        let file_mutex = Mutex::new(file);

        // This will store the head offset(this stores the most recent offset)
        let mut head_offset = 0;

        let mut locked_file = file_mutex.lock().unwrap();
        let _ = self
            .index
            .iter()
            .map(|e| {
                let entry = (e.key().clone(), *e.value());
                // key length(used during fetch) + key len(actual key length) + value length(4 bytes)
                let entry_len = 4 + entry.0.len() + 4;
                let mut entry_vec = Vec::with_capacity(entry_len as usize);

                entry_vec.extend_from_slice(&(entry.0.len() as u32).to_le_bytes());
                entry_vec.extend_from_slice(&entry.0);
                entry_vec.extend_from_slice(&(entry.1 as u32).to_le_bytes());
                locked_file.write_all(&entry_vec).unwrap();

                assert!(entry_len == entry_vec.len(), "Incorrect entry size");

                locked_file.flush().unwrap();

                match Self::compare_offsets(head_offset, entry.1) {
                    Ordering::Less => head_offset = entry.1,
                    _ => {
                        // We only update header if the value offset is greater than existing header offset
                    }
                }
            })
            .collect::<Vec<_>>();
        let mut entry_vec = Vec::with_capacity(12);

        // will be stored as <head, head_offset>
        let key = "head";
        // write new header offset
        entry_vec.extend_from_slice(&(key.len() as u32).to_le_bytes());
        entry_vec.extend_from_slice(key.as_bytes());
        entry_vec.extend_from_slice(&(head_offset as u32).to_le_bytes());
        locked_file.write_all(&entry_vec).unwrap();
        locked_file.flush().unwrap();

        return Ok(());
    }

    // for now we assume that we have only have one sstable but in the future we will have levels table for biggest keys
    pub(crate) fn get(&self, searched_key: &[u8]) -> io::Result<usize> {
        // Open the file in read mode
        let file_path = PathBuf::from(&self.file_path);
        let file = OpenOptions::new().read(true).open(file_path)?;

        let file_mutex = Mutex::new(file);
       
        // read bloom filter to check if the key possbly exists in the sstable
        let mut locked_file = file_mutex.lock().unwrap();

        // search sstable for key
        loop {
            let mut key_len_bytes = [0; 4];
            let mut bytes_read = locked_file.read(&mut key_len_bytes)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Key {:?} not found", searched_key),
                ));
            }
            let key_len = u32::from_le_bytes(key_len_bytes);

            let mut key = vec![0; key_len as usize];
            bytes_read = locked_file.read(&mut key)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Key {:?} not found", searched_key),
                ));
            }
            let mut val_offset_bytes = [0; 4];
            bytes_read = locked_file.read(&mut val_offset_bytes)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Key {:?} not found", searched_key),
                ));
            }
            let value_offset = u32::from_le_bytes(val_offset_bytes);

            if key == searched_key {
                return Ok(value_offset as usize);
            }
        }
    }

    fn compare_offsets(offset_a: usize, offset_b: usize) -> Ordering {
        if offset_a < offset_b {
            Ordering::Less
        } else if offset_a == offset_b {
            Ordering::Equal
        } else {
            Ordering::Greater
        }
    }

    pub fn set_index(&mut self, index: Arc<SkipMap<Vec<u8>, usize>>) {
        self.index = index
    }
    pub fn get_path(&self) -> String {
        self.file_path.to_string_lossy().into_owned()
    }

    fn from_file(file_path: PathBuf) {}
}
