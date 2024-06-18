use crate::consts::{EOF, SIZE_OF_U32};
use crate::err::Error;
use crate::fs::{FileAsync, FileNode, IndexFileNode, IndexFs};
use crate::types::Key;
use std::path::PathBuf;

use tokio::io::{self};
use Error::*;
type Offset = u32;

#[derive(Debug, Clone)]
pub struct IndexFile<F>
where
    F: IndexFs,
{
    pub(crate) file: F,
    pub(crate) path: PathBuf,
}

impl<F> IndexFile<F>
where
    F: IndexFs,
{
    pub fn new(path: PathBuf, file: F) -> Self {
        Self { path, file }
    }
}
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub key_prefix: u32,
    pub key: Vec<u8>,
    pub block_handle: u32,
}
#[derive(Debug, Clone)]
pub struct Index {
    entries: Vec<IndexEntry>,
    file: IndexFile<IndexFileNode>,
}
#[derive(Debug, Clone)]
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
    pub fn new(path: PathBuf, file: IndexFileNode) -> Self {
        Self {
            entries: Vec::new(),
            file: IndexFile::new(path, file),
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
        for e in &self.entries {
            let serialized_input = self.serialize_entry(e)?;
            self.file.file.node.write_all(&serialized_input).await?;
        }
        Ok(())
    }

    fn serialize_entry(&self, e: &IndexEntry) -> Result<Vec<u8>, Error> {
        let entry_len = e.key.len() + SIZE_OF_U32 + SIZE_OF_U32;

        let mut entry_vec = Vec::with_capacity(entry_len);

        //add key len
        entry_vec.extend_from_slice(&(e.key_prefix).to_le_bytes());

        //add key
        entry_vec.extend_from_slice(&e.key);

        //add value offset
        entry_vec.extend_from_slice(&(e.block_handle as u32).to_le_bytes());
        if entry_len != entry_vec.len() {
            return Err(SerializationError("Invalid entry size"));
        }
        Ok(entry_vec)
    }

    pub(crate) async fn get(
        &self,
        searched_key: &[u8],
    ) -> Result<Option<u32>, Error> {
        self.file.file.get_from_index(searched_key).await
    }

    pub(crate) async fn get_block_offset_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<RangeOffset, Error> {
        self.file.file.get_block_range(start_key, end_key).await
    }
}
