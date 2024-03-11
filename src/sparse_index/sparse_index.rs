use std::path::PathBuf;

use chrono::Utc;
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
};

use crate::{
    consts::{SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8},
    err::StorageEngineError,
};
use StorageEngineError::*;
pub struct SparseIndex {
    entries: Vec<SparseIndexEntry>,
    file_path: PathBuf,
}

impl SparseIndex {
    pub async fn new(file_path: PathBuf) -> Self {
      
        Self {
            file_path,
            entries: Vec::new(),
        }
    }

    pub fn insert(&mut self, key_prefix: u32, key: Vec<u8>, offset: u32) {
        self.entries.push(SparseIndexEntry {
            key,
            key_prefix,
            offset,
        })
    }

    pub async fn write_to_file(&self) -> Result<(), StorageEngineError> {
        let file_path = PathBuf::from(&self.file_path);
        let mut file = OpenOptions::new()
            .append(true)
            .open(file_path.clone())
            .await
            .map_err(|err| SSTableFileOpenError {
                path: file_path.clone(),
                error: err,
            })?;
        for entry in &self.entries {
            let entry_len = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U32;

            let mut entry_vec = Vec::with_capacity(entry_len);

            //add key len
            entry_vec.extend_from_slice(&(entry.key_prefix).to_le_bytes());

            //add key
            entry_vec.extend_from_slice(&entry.key);

            //add value offset
            entry_vec.extend_from_slice(&(entry.offset as u32).to_le_bytes());

            assert!(entry_len == entry_vec.len(), "Incorrect entry size");

            file.write_all(&entry_vec)
                .await
                .map_err(|err| IndexFileWriteError(err))?;

            file.flush().await.map_err(|err| IndexFileFlushError(err))?;
        }
        Ok(())
    }
}

struct SparseIndexEntry {
    key_prefix: u32,
    key: Vec<u8>,
    offset: u32,
}
