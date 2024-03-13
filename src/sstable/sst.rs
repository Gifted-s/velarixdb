use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use futures::TryFutureExt;
use std::{
    cmp::Ordering,
    mem,
    path::{Path, PathBuf},
    sync::Arc,
    thread::current,
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
};
use tokio::{fs::File, io};
use tokio::{fs::OpenOptions, io::AsyncSeekExt};

use crate::{
    block::{self, Block},
    bloom_filter::BloomFilter,
    compaction::IndexWithSizeInBytes,
    consts::{DEFAULT_FALSE_POSITIVE_RATE, EOF, SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8},
    err::StorageEngineError,
    memtable::Entry,
    sparse_index::{self, SparseIndex},
};

use StorageEngineError::*;

#[derive(Debug, Clone)]
pub struct SSTable {
    pub data_file_path: PathBuf,
    pub index_file_path: PathBuf,
    pub sstable_dir: PathBuf,
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
    pub(crate) dir: PathBuf,
    pub(crate) data_file_path: PathBuf,
    pub(crate) index_file_path: PathBuf,
    pub(crate) hotness: u64,
}
impl SSTablePath {
    pub fn new(dir: PathBuf, data_file_path: PathBuf, index_file_path: PathBuf) -> Self {
        Self {
            data_file_path,
            index_file_path,
            dir,
            hotness: 0,
        }
    }
    pub fn increase_hotness(&mut self) {
        self.hotness += 1;
    }
    pub fn get_data_file_path(&self) -> PathBuf {
        self.data_file_path.clone()
    }

    pub fn get_hotness(&self) -> u64 {
        self.hotness
    }
}

#[allow(dead_code)]
impl SSTable {
    pub(crate) async fn new(dir: PathBuf, create_file: bool) -> Self {
        let created_at = Utc::now();
        let data_file_name = format!("sstable_{}_.db", created_at.timestamp_millis());
        let index_file_name = format!("index_{}_.db", created_at.timestamp_millis());

        if !dir.exists() {
            fs::create_dir_all(&dir)
                .await
                .expect("ss table directory was not created successfullt")
        }

        let data_file_path = dir.join(data_file_name.clone());
        let index_file_path = dir.join(index_file_name.clone());
        if create_file {
            _ = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(data_file_path.clone())
                .await
                .expect("error creating file");

            _ = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(index_file_path.clone())
                .await
                .expect("error creating file");
        }

        let index = Arc::new(SkipMap::new());
        Self {
            data_file_path,
            index_file_path,
            index,
            size: 0,
            created_at: created_at.timestamp_millis() as u64,
            sstable_dir: dir,
        }
    }

    pub(crate) fn new_with_exisiting_file_path(
        dir: PathBuf,
        data_file_path: PathBuf,
        index_file_path: PathBuf,
    ) -> Self {
        let created_at = Utc::now();
        let index = Arc::new(SkipMap::new());
        Self {
            data_file_path,
            index_file_path,
            sstable_dir: dir,
            index,
            size: 0,
            created_at: created_at.timestamp_millis() as u64,
        }
    }

    pub(crate) async fn write_to_file(&self) -> Result<(), StorageEngineError> {
        // Open the file in write mode with the append flag.
        let data_file_path = &self.data_file_path;
        let index_file_path = &self.index_file_path;

        let mut file = OpenOptions::new()
            .append(true)
            .open(data_file_path.clone())
            .await
            .map_err(|err| SSTableFileOpenError {
                path: data_file_path.clone(),
                error: err,
            })?;

        let mut blocks: Vec<Block> = Vec::new();
        let mut sparse_index = sparse_index::SparseIndex::new(index_file_path.clone()).await;
        let mut current_block = Block::new();
        for e in self.index.iter() {
            let entry = Entry::new(e.key().clone(), e.value().0, e.value().1, e.value().2);

            // mem::size_of function is efficient because the size is known at compile time so
            // during compilation this will be replaced  with the actual size i.e number of bytes to store the type
            // key length(used during fetch) + key len(actual key length) + value length(4 bytes) + date in milliseconds(8 bytes)
            let entry_size = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;

            if current_block.is_full(entry_size) {
                blocks.push(current_block);
                current_block = Block::new();
            }

            current_block.set_entry(
                entry.key.len() as u32,
                entry.key,
                entry.val_offset as u32,
                entry.created_at,
                entry.is_tombstone,
            )?;
        }

        for block in blocks.iter() {
            self.write_block(&mut file, block, &mut sparse_index)
                .await?;
        }

        // Incase we have some entries in current block, write them to disk
        if current_block.data.len() > 0 {
            self.write_block(&mut file, &current_block, &mut sparse_index)
                .await?;
        }

        sparse_index.write_to_file().await?;
        Ok(())
    }

    async fn write_block(
        &self,
        file: &mut File,
        block: &Block,
        sparse_index: &mut SparseIndex,
    ) -> Result<(), StorageEngineError> {
        // Get the current offset before writing (this will be the offset of the value stored in the sparse index)
        let offset = file
            .metadata()
            .await
            .map_err(|err| GetFileMetaDataError(err))?
            .len();
        let first_entry = block.get_first_entry();
        // Store initial entry key and its sstable file offset in sparse index
        sparse_index.insert(first_entry.key_prefix, first_entry.key, offset as u32);
        block.write_to_file(file).await?;
        Ok(())
    }

    pub(crate) async fn get(
        &self,
        start_offset: u32,
        searched_key: &[u8],
    ) -> Result<Option<(usize, u64, bool)>, StorageEngineError> {
        // Open the file in read mode
        let file_path = PathBuf::from(&self.data_file_path);
        let mut file = OpenOptions::new()
            .read(true)
            .open(file_path.clone())
            .await
            .map_err(|err| SSTableFileOpenError {
                path: file_path.clone(),
                error: err,
            })?;

        file.seek(tokio::io::SeekFrom::Start(start_offset.into()))
            .await
            .map_err(|err| FileSeekError(err))?;
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
        self.data_file_path.clone()
    }

    pub(crate) fn data_file_exists(path_buf: &PathBuf) -> bool {
        // Convert the PathBuf to a Path
        let path: &Path = path_buf.as_path();
        // Check if the file exists
        path.exists() && path.is_file()
    }

    pub(crate) async fn from_file(
        dir: PathBuf,
        data_file_path: PathBuf,
        index_file_path: PathBuf,
    ) -> Result<Option<SSTable>, StorageEngineError> {
        let index = Arc::new(SkipMap::new());
        // Open the file in read mode
        if !Self::data_file_exists(&data_file_path) {
            return Ok(None);
        }

        let mut file = OpenOptions::new()
            .read(true)
            .open(data_file_path.clone())
            .await
            .map_err(|err| SSTableFileOpenError {
                path: data_file_path.clone(),
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
                        path: data_file_path.clone(),
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
                    path: data_file_path.clone(),
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
                        path: data_file_path.clone(),
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
                        path: data_file_path.clone(),
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
                        path: data_file_path.clone(),
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
            data_file_path: data_file_path.clone(),
            index_file_path,
            sstable_dir: dir,
            index: index.to_owned(),
            created_at,
            size: fs::metadata(data_file_path).await.unwrap().len() as usize,
        }))
    }
}
