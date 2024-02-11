use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;
use num_traits::{ops::bytes, ToBytes};
use serde::de::value::Error;
use std::{
    cmp::Ordering,
    fs::{self, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{
    compaction::IndexWithSizeInBytes,
    memtable::{Entry, DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MEMTABLE_CAPACITY},
};

pub struct SSTable {
    pub file_path: PathBuf,
    pub index: Arc<SkipMap<Vec<u8>, (usize, u64)>>,
    pub created_at: u64,
    pub size: usize,
}

impl IndexWithSizeInBytes for SSTable {
    fn get_index(&self) -> Arc<SkipMap<Vec<u8>, (usize, u64)>> {
        Arc::clone(&self.index)
    }
    fn size(&self) -> usize {
        self.size
    }
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
        Self {
            file_path,
            index,
            size: 0,
            created_at: created_at.timestamp_millis() as u64,
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

        // This will store the head offset(this stores the most recent value offset)
        let mut head_offset = 0;

        let mut locked_file = file_mutex.lock().unwrap();
        self.index.iter().for_each(|e| {
            let entry = Entry::new(e.key().clone(), e.value().0, e.value().1);

    
            // mem::size_of function is efficient because the size is known at compile time so 
            // during compilation this will be replaced  with the actual size during compilation i.e number of bytes to store the type
            // key length(used during fetch) + key len(actual key length) + value length(4 bytes) + date in milliseconds(8 bytes)
            let entry_len = mem::size_of::<u32>()
                + entry.key.len()
                + mem::size_of::<u32>()
                + mem::size_of::<u64>();
            let mut entry_vec = Vec::with_capacity(entry_len as usize);

            //add key len
            entry_vec.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());

            //add key
            entry_vec.extend_from_slice(&entry.key);

            //add value offset
            entry_vec.extend_from_slice(&(entry.val_offset as u32).to_le_bytes());

            //write date created in milliseconds
            entry_vec.extend_from_slice(&(entry.created_at as u64).to_le_bytes());

            //write to file
            locked_file.write_all(&entry_vec).unwrap();

            assert!(entry_len == entry_vec.len(), "Incorrect entry size");

            locked_file.flush().unwrap();

            // We check that the head offset is less than the value offset because
            // there is no gurantee that the value offset of the next key will be greater than
            // the previous key since index is sorted based on key and not value offset
            match Self::compare_offsets(head_offset, entry.val_offset) {
                Ordering::Less => head_offset = entry.val_offset,
                _ => {
                    // We only update header if the value offset is greater than existing header offset
                }
            }
        });

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
            let mut key_len_bytes = [0;  mem::size_of::<u32>()];
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
            let mut val_offset_bytes = [0;  mem::size_of::<u32>()];
            bytes_read = locked_file.read(&mut val_offset_bytes)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Key {:?} not found", searched_key),
                ));
            }
            let mut created_at_bytes = [0;  mem::size_of::<u64>()];
            bytes_read = locked_file.read(&mut created_at_bytes)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Key {:?} not found", searched_key),
                ));
            }
            let _ = u64::from_le_bytes(created_at_bytes);
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

    fn size(&self) -> usize {
        self.size
    }

    pub fn set_index(&mut self, index: Arc<SkipMap<Vec<u8>, (usize, u64)>>) {
        self.index = index;
        self.set_sst_size_from_index();
    }

    pub fn get_index(&self) -> Arc<SkipMap<Vec<u8>, (usize, u64)>> {
        self.index.clone()
    }
    pub fn set_sst_size_from_index(&mut self) {
        self.size = self
            .index
            .iter()
            .map(|e| e.key().len() + mem::size_of::<usize>() + mem::size_of::<u64>())
            .sum::<usize>();
    }

    pub fn get_path(&self) -> String {
        self.file_path.to_string_lossy().into_owned()
    }

    fn file_exists(path_buf: &PathBuf) -> bool {
        // Convert the PathBuf to a Path
        let path: &Path = path_buf.as_path();

        // Check if the file exists
        path.exists() && path.is_file()
    }

    pub fn from_file(sstable_file_path: PathBuf) -> io::Result<Option<SSTable>> {
        let index = Arc::new(SkipMap::new());
        // Open the file in read mode
        let file_path = PathBuf::from(&sstable_file_path);
        if Self::file_exists(&file_path) {
            return Ok(None);
        }

        let file = OpenOptions::new().read(true).open(file_path)?;

        let file_mutex = Mutex::new(file);

        // read bloom filter to check if the key possbly exists in the sstable
        let mut locked_file = file_mutex.lock().unwrap();

        // search sstable for key
        loop {
            let mut key_len_bytes = [0;  mem::size_of::<u32>()];
            let mut bytes_read = locked_file.read(&mut key_len_bytes)?;
            if bytes_read == 0 {
                break;
            }
            let key_len = u32::from_le_bytes(key_len_bytes);

            let mut key = vec![0; key_len as usize];
            bytes_read = locked_file.read(&mut key)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("File read ended expectedly"),
                ));
            }
            let mut val_offset_bytes = [0;  mem::size_of::<u32>()];
            bytes_read = locked_file.read(&mut val_offset_bytes)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("File read ended expectedly"),
                ));
            }
            let mut created_at_bytes = [0;  mem::size_of::<u64>()];
            bytes_read = locked_file.read(&mut created_at_bytes)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("File read ended expectedly"),
                ));
            }
            let created_at = u64::from_le_bytes(created_at_bytes);
            let value_offset = u32::from_le_bytes(val_offset_bytes);
            index.insert(key, (value_offset as usize, created_at as u64));
        }
        let created_at = Utc::now().timestamp_millis() as u64;
        return Ok(Some(SSTable {
            file_path: sstable_file_path.clone(),
            index: index.to_owned(),
            created_at,
            size: fs::metadata(sstable_file_path).unwrap().len() as usize,
        }));
    }
}
