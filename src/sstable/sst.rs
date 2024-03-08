use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use std::{
    cmp::Ordering,
    mem,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs::OpenOptions;
use tokio::io::{self};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::{
    bloom_filter::BloomFilter,
    compaction::IndexWithSizeInBytes,
    consts::{DEFAULT_FALSE_POSITIVE_RATE, EOF},
    err::StorageEngineError,
    memtable::Entry,
};

use StorageEngineError::*;

pub struct SSTable {
    pub file_path: PathBuf,
    pub index: Arc<SkipMap<Vec<u8>, (usize, u64, bool)>>,
    pub created_at: u64,
    pub size: usize,
}

impl IndexWithSizeInBytes for SSTable {
    fn get_index(&self) -> Arc<SkipMap<Vec<u8>, (usize, u64, bool)>> {
        Arc::clone(&self.index)
    }
    fn size(&self) -> usize {
        self.size
    }
}

#[derive(Debug, Clone)]
pub struct SSTablePath {
    pub(crate) file_path: PathBuf,
    pub(crate) hotness: u64,
}
impl SSTablePath {
    pub fn new(file_path: PathBuf) -> Self {
        Self {
            file_path,
            hotness: 0,
        }
    }
    pub fn increase_hotness(&mut self) {
        self.hotness += 1;
    }
    pub fn get_path(&self) -> PathBuf {
        self.file_path.clone()
    }

    pub fn get_hotness(&self) -> u64 {
        self.hotness
    }
}

#[allow(dead_code)]
impl SSTable {
    pub(crate) async fn new(dir: PathBuf, create_file: bool) -> Self {
        let created_at = Utc::now();
        let file_name = format!("sstable_{}_.db", created_at.timestamp_millis());
        if !dir.exists() {
            fs::create_dir_all(&dir)
                .await
                .expect("ss table directory was not created successfullt")
        }

        let file_path = dir.join(file_name);
        if create_file {
            _ = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(file_path.clone())
                .await
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

    pub(crate) fn new_with_exisiting_file_path(file_path: PathBuf) -> Self {
        let created_at = Utc::now();
        let index = Arc::new(SkipMap::new());
        Self {
            file_path,
            index,
            size: 0,
            created_at: created_at.timestamp_millis() as u64,
        }
    }

    pub(crate) async fn write_to_file(&self) -> Result<(), StorageEngineError> {
        // Open the file in write mode with the append flag.
        let file_path = PathBuf::from(&self.file_path);
        let mut file = OpenOptions::new()
            .append(true)
            .open(file_path.clone())
            .await
            .map_err(|err| SSTableFileOpenError {
                path: file_path.clone(),
                error: err,
            })?;

        // This will store the head offset(this stores the most recent value offset)
        let mut head_offset = 0;
        for e in self.index.iter(){
            let entry = Entry::new(e.key().clone(), e.value().0, e.value().1, e.value().2);

            // mem::size_of function is efficient because the size is known at compile time so
            // during compilation this will be replaced  with the actual size during compilation i.e number of bytes to store the type
            // key length(used during fetch) + key len(actual key length) + value length(4 bytes) + date in milliseconds(8 bytes)
            let entry_len = mem::size_of::<u32>()
                + entry.key.len()
                + mem::size_of::<u32>()
                + mem::size_of::<u64>()
                + mem::size_of::<u8>();
            let mut entry_vec = Vec::with_capacity(entry_len);

            //add key len
            entry_vec.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());

            //add key
            entry_vec.extend_from_slice(&entry.key);

            //add value offset
            entry_vec.extend_from_slice(&(entry.val_offset as u32).to_le_bytes());

            //write date created in milliseconds
            entry_vec.extend_from_slice(&entry.created_at.to_le_bytes());

            //write is tombstone to file
            entry_vec.push(entry.is_tombstone as u8);

            //write to file
            file.write_all(&entry_vec).await.map_err(|err| SSTableWriteError{path:file_path.clone(), error: err})?;

            assert!(entry_len == entry_vec.len(), "Incorrect entry size");

            file.flush().await.map_err(|err| SSTableFlushError{path:file_path.clone(), error: err})?;

            // We check that the head offset is less than the value offset because
            // there is no gurantee that the value offset of the next key will be greater than
            // the previous key since index is sorted based on key and not value offset
            match Self::compare_offsets(head_offset, entry.val_offset) {
                Ordering::Less => head_offset = entry.val_offset,
                _ => {}  // We only update header if the value offset is greater than existing header offset
            }
        }
     

        Ok(())
    }

    pub(crate) async fn get(
        &self,
        searched_key: &[u8],
    ) -> Result<Option<(usize, u64, bool)>, StorageEngineError> {
        // Open the file in read mode
        let file_path = PathBuf::from(&self.file_path);
        let mut file = OpenOptions::new()
            .read(true)
            .open(file_path.clone())
            .await
            .map_err(|err| SSTableFileOpenError {
                path: file_path.clone(),
                error: err,
            })?;

        // read bloom filter to check if the key possbly exists in the sstable

        // search sstable for key
        loop {
            let mut key_len_bytes = [0; mem::size_of::<u32>()];
            let mut bytes_read =
                file.read(&mut key_len_bytes)
                    .await
                    .map_err(|err| SSTableFileReadError {
                        path: file_path.clone(),
                        error: err,
                    })?;
            if bytes_read == 0 {
                return Ok(None);
            }
            let key_len = u32::from_le_bytes(key_len_bytes);

            let mut key = vec![0; key_len as usize];
            bytes_read = file
                .read(&mut key)
                .await
                .map_err(|err| SSTableFileReadError {
                    path: file_path.clone(),
                    error: err,
                })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            let mut val_offset_bytes = [0; mem::size_of::<u32>()];
            bytes_read =
                file.read(&mut val_offset_bytes)
                    .await
                    .map_err(|err| SSTableFileReadError {
                        path: file_path.clone(),
                        error: err,
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            let mut created_at_bytes = [0; mem::size_of::<u64>()];
            bytes_read =
                file.read(&mut created_at_bytes)
                    .await
                    .map_err(|err| SSTableFileReadError {
                        path: file_path.clone(),
                        error: err,
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }

            let mut is_tombstone_byte = [0; 1];
            bytes_read =
                file.read(&mut is_tombstone_byte)
                    .await
                    .map_err(|err| SSTableFileReadError {
                        path: file_path.clone(),
                        error: err,
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }

            let created_at = u64::from_le_bytes(created_at_bytes);
            let value_offset = u32::from_le_bytes(val_offset_bytes);
            let is_tombstone = is_tombstone_byte[0] == 1;

            if key == searched_key {
                return Ok(Some((value_offset as usize, created_at, is_tombstone)));
            }
        }
    }

    pub(crate) fn build_bloomfilter_from_sstable(
        index: &Arc<SkipMap<Vec<u8>, (usize, u64, bool)>>,
    ) -> BloomFilter {
        //TODO: FALSE POS should be from config
        // Rebuild the bloom filter since a new sstable has been created
        let mut new_bloom_filter = BloomFilter::new(DEFAULT_FALSE_POSITIVE_RATE, index.len());
        index.iter().for_each(|e| new_bloom_filter.set(e.key()));
        new_bloom_filter
    }

    pub(crate) fn get_value_from_index(&self, key: &[u8]) -> Option<(usize, u64, bool)> {
        self.index.get(key).map(|entry| entry.value().to_owned())
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

    pub(crate) fn set_index(&mut self, index: Arc<SkipMap<Vec<u8>, (usize, u64, bool)>>) {
        self.index = index;
        self.set_sst_size_from_index();
    }

    pub(crate) fn get_index(&self) -> Arc<SkipMap<Vec<u8>, (usize, u64, bool)>> {
        self.index.clone()
    }
    pub(crate) fn set_sst_size_from_index(&mut self) {
        self.size = self
            .index
            .iter()
            .map(|e| {
                e.key().len()
                    + mem::size_of::<usize>()
                    + mem::size_of::<u64>()
                    + mem::size_of::<u8>()
            })
            .sum::<usize>();
    }

    pub(crate) fn get_path(&self) -> PathBuf {
        self.file_path.clone()
    }

    pub(crate) fn file_exists(path_buf: &PathBuf) -> bool {
        // Convert the PathBuf to a Path
        let path: &Path = path_buf.as_path();
        // Check if the file exists
        path.exists() && path.is_file()
    }

    pub(crate) async fn from_file(
        sstable_file_path: PathBuf,
    ) -> Result<Option<SSTable>, StorageEngineError> {
        let index = Arc::new(SkipMap::new());
        // Open the file in read mode
        if !Self::file_exists(&sstable_file_path) {
            return Ok(None);
        }

        let mut file = OpenOptions::new()
            .read(true)
            .open(sstable_file_path.clone())
            .await
            .map_err(|err| SSTableFileOpenError {
                path: sstable_file_path.clone(),
                error: err,
            })?;

        // read bloom filter to check if the key possbly exists in the sstable
        // search sstable for key
        loop {
            let mut key_len_bytes = [0; mem::size_of::<u32>()];
            let mut bytes_read =
                file.read(&mut key_len_bytes)
                    .await
                    .map_err(|err| SSTableFileReadError {
                        path: sstable_file_path.clone(),
                        error: err,
                    })?;
            if bytes_read == 0 {
                break;
            }
            let key_len = u32::from_le_bytes(key_len_bytes);

            let mut key = vec![0; key_len as usize];
            bytes_read = file
                .read(&mut key)
                .await
                .map_err(|err| SSTableFileReadError {
                    path: sstable_file_path.clone(),
                    error: err,
                })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            let mut val_offset_bytes = [0; mem::size_of::<u32>()];
            bytes_read =
                file.read(&mut val_offset_bytes)
                    .await
                    .map_err(|err| SSTableFileReadError {
                        path: sstable_file_path.clone(),
                        error: err,
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            let mut created_at_bytes = [0; mem::size_of::<u64>()];
            bytes_read =
                file.read(&mut created_at_bytes)
                    .await
                    .map_err(|err| SSTableFileReadError {
                        path: sstable_file_path.clone(),
                        error: err,
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }

            let mut is_tombstone_byte = [0; 1];
            bytes_read =
                file.read(&mut is_tombstone_byte)
                    .await
                    .map_err(|err| SSTableFileReadError {
                        path: sstable_file_path.clone(),
                        error: err,
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }

            let created_at = u64::from_le_bytes(created_at_bytes);
            let value_offset = u32::from_le_bytes(val_offset_bytes);
            let is_tombstone = is_tombstone_byte[0] == 1;
            index.insert(key, (value_offset as usize, created_at, is_tombstone));
        }
        let created_at = Utc::now().timestamp_millis() as u64;
        Ok(Some(SSTable {
            file_path: sstable_file_path.clone(),
            index: index.to_owned(),
            created_at,
            size: fs::metadata(sstable_file_path).await.unwrap().len() as usize,
        }))
    }
}
