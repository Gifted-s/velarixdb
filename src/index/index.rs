use crate::consts::{EOF, SIZE_OF_U32};
use crate::err::Error;
use crate::types::Key;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::{
    fs::OpenOptions,
    io::{self, AsyncReadExt, AsyncWriteExt},
};
use Error::*;
type Offset = u32;
struct IndexEntry {
    key_prefix: u32,
    key: Vec<u8>,
    block_handle: u32,
}

pub struct Index {
    entries: Vec<IndexEntry>,
    file_path: PathBuf,
    file: Arc<tokio::sync::RwLock<tokio::fs::File>>,
}

pub struct RangeOffset {
    pub start_offset: Offset,
    pub end_offset: Offset,
}

impl RangeOffset {
    pub fn new(start: Offset, end: Offset) -> Self {
        Self {
            start_offset: start,
            end_offset: end,
        }
    }
}

impl Index {
    pub fn new(file_path: PathBuf, file: Arc<RwLock<tokio::fs::File>>) -> Self {
        Self {
            file_path,
            entries: Vec::new(),
            file,
        }
    }

    pub fn insert(&mut self, key_prefix: u32, key: Key, offset: Offset) {
        self.entries.push(IndexEntry {
            key_prefix,
            key,
            block_handle: offset,
        })
    }

    pub async fn write_to_file(&self) -> Result<(), Error> {
        let mut file = self.file.write().await;
        for entry in &self.entries {
            let entry_len = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U32;

            let mut entry_vec = Vec::with_capacity(entry_len);

            //add key len
            entry_vec.extend_from_slice(&(entry.key_prefix).to_le_bytes());

            //add key
            entry_vec.extend_from_slice(&entry.key);

            //add value offset
            entry_vec.extend_from_slice(&(entry.block_handle as u32).to_le_bytes());
            assert!(entry_len == entry_vec.len(), "Incorrect entry size");

            file.write_all(&entry_vec)
                .await
                .map_err(|err| IndexFileWriteError(err))?;

            file.flush().await.map_err(|err| IndexFileFlushError(err))?;
        }

        Ok(())
    }

    pub(crate) async fn get(&self, searched_key: &[u8]) -> Result<Option<u32>, Error> {
        let mut block_offset = -1;
        // Open the file in read mode
        let mut file = self.file.write().await;
        // read bloom filter to check if the key possbly exists in the sstable
        // search sstable for key
        loop {
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read =
                file.read(&mut key_len_bytes)
                    .await
                    .map_err(|err| SSTableFileReadError {
                        path: self.file_path.clone(),
                        error: err,
                    })?;
            // If the end of the file is reached and no match is found, return non
            if bytes_read == 0 {
                println!("0 bytes read");
                if block_offset == -1 {
                    return Ok(None);
                }
                return Ok(Some(block_offset as u32));
            }
            let key_len = u32::from_le_bytes(key_len_bytes);
            let mut key = vec![0; key_len as usize];
            bytes_read = file
                .read(&mut key)
                .await
                .map_err(|err| IndexFileReadError(err))?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            let mut key_offset_bytes = [0; SIZE_OF_U32];
            bytes_read =
                file.read(&mut key_offset_bytes)
                    .await
                    .map_err(|err| SSTableFileReadError {
                        path: self.file_path.clone(),
                        error: err,
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }

            let offset = u32::from_le_bytes(key_offset_bytes);
            match key.cmp(&searched_key.to_vec()) {
                std::cmp::Ordering::Less => block_offset = offset as i32,
                std::cmp::Ordering::Equal => {
                    return Ok(Some(offset));
                }
                std::cmp::Ordering::Greater => {
                    // if all index keys are greater than the searched key then return none
                    if block_offset == -1 {
                        return Ok(None);
                    }
                    return Ok(Some(block_offset as u32));
                }
            }
        }
    }

    pub(crate) async fn get_block_offset_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<RangeOffset, Error> {
        let mut range_offset = RangeOffset::new(0, 0);
        // Open the file in read mode
        let file_path = PathBuf::from(&self.file_path);
        let mut file = self.file.write().await;
        // read bloom filter to check if the key possbly exists in the sstable
        // search sstable for key
        loop {
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read =
                file.read(&mut key_len_bytes)
                    .await
                    .map_err(|err| SSTableFileReadError {
                        path: file_path.clone(),
                        error: err,
                    })?;
            // If the end of the file is reached and no match is found, return non
            if bytes_read == 0 {
                return Ok(range_offset);
            }
            let key_len = u32::from_le_bytes(key_len_bytes);
            let mut key = vec![0; key_len as usize];
            bytes_read = file
                .read(&mut key)
                .await
                .map_err(|err| IndexFileReadError(err))?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            let mut key_offset_bytes = [0; SIZE_OF_U32];
            bytes_read =
                file.read(&mut key_offset_bytes)
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

            let offset = u32::from_le_bytes(key_offset_bytes);
            match key.cmp(&start_key.to_vec()) {
                std::cmp::Ordering::Greater => match key.cmp(&end_key.to_vec()) {
                    std::cmp::Ordering::Greater => {
                        range_offset.end_offset = offset;
                        return Ok(range_offset);
                    }
                    _ => range_offset.end_offset = offset,
                },
                _ => range_offset.start_offset = offset,
            }
        }
    }
}
