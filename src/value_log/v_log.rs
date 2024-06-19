//! # Value Log
//!
//! The Sequential Value Log is a crucial component of the LSM Tree storage engine.
//! It provides durability and atomicity guarantees by logging write operations before they are applied to the main data structure.
//! The sstable only stores the value offsets from this file
//!
//! When a write operation is received, the key-value pair is first appended to the Value Log.
//! In the event of a crash or system failure, the Value Log can be replayed to recover the data modifications and bring the MemTable back to a consistent state.
//!
//! ## Value Log Structure
//!
//! The `ValueLog` structure contains the following field:
//!
//! ```rs
//! struct ValueLog {
//!     content: Arc<RwLock<VLogFileNode>>,
//!     head_offset: usize
//!     tail_offset: usize
//! }
//! ```
//!
//! ### content
//!
//! The `content` field is of type `Arc<Rwlock<VLogFileNode>>`. It represents the VLog file and provides concurrent access and modification through the use of an `Arc` (Atomic Reference Counting) and `RwLock`.
//! We use RwLock to ensure multiple threads can read from the log file while permmitting only one thread to write
//!
//! ## Log File Structure Diagram
//!
//! The `log_file` is structured as follows:
//!
//! ```text
//! +-------------------+
//! |    Key Size       |   (4 bytes)
//! +-------------------+
//! |   Value Size      |   (4 byte)
//! +-------------------+
//! |      Key          |   (variable)
//! +-------------------+
//! |     Value         |   (variable)
//! +-------------------+
//! |   Created At      |   (8 bytes)
//! |                   |
//! +-------------------+
//! |  Is Tombstone     |   (1 byte)
//! |                   |
//! |                   |
//! +-------------------+
//! |    Key Size       |   (4 bytes)
//! +-------------------+
//! |   Value Size      |   (4 byte)
//! +-------------------+
//! |      Key          |   (variable)
//! +-------------------+
//! |     Value         |   (variable)
//! +-------------------+
//! |   Created At      |   (8 bytes)
//! |                   |
//! +-------------------+
//! |  Is Tombstone     |   (1 byte)
//! |                   |
//! |                   |
//! +-------------------+
//! ```
//!
//! - **Key Size**: A 4-byte field representing the length of the key in bytes.
//! - **Value Size**: A 4-byte field representing the length of the value in bytes.
//! - **Key**: The actual key data, which can vary in size.
//! - **Value**: The actual value data, which can vary in size.
//! - **Created At**: A 8-byte field representing the time of insertion in bytes.
//! - **Is Tombstone**: A 1 byte field representing a boolean of deleted or not deleted entry

use crate::{
    consts::{EOF, SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8, VLOG_FILE_NAME},
    err::Error,
    fs::{FileAsync, FileNode, VLogFileNode, VLogFs},
};
use log::error;
use std::{mem, path::PathBuf};
use tokio::io::{self};

type TotalBytesRead = usize;

#[derive(Debug, Clone)]
pub struct VFile<F>
where
    F: VLogFs,
{
    pub(crate) file: F,
    pub(crate) path: PathBuf,
}

impl<F> VFile<F>
where
    F: VLogFs,
{
    pub fn new(path: PathBuf, file: F) -> Self {
        Self { path, file }
    }
}

#[derive(Debug, Clone)]
pub struct ValueLog {
    pub content: VFile<VLogFileNode>,
    pub head_offset: usize,
    pub tail_offset: usize,
}

#[derive(PartialEq, Debug, Clone)]
pub struct ValueLogEntry {
    pub ksize: usize,
    pub vsize: usize,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub created_at: u64,
    pub is_tombstone: bool,
}

impl ValueLog {
    pub async fn new(dir: &PathBuf) -> Result<Self, Error> {
        let dir_path = PathBuf::from(dir);
        FileNode::create_dir_all(dir_path.to_owned()).await?;
        let file_path = dir_path.join(VLOG_FILE_NAME);
        let file = VLogFileNode::new(file_path.to_owned(), crate::fs::FileType::ValueLog)
            .await
            .unwrap();
        Ok(Self {
            head_offset: 0,
            tail_offset: 0,
            content: VFile::new(file_path, file),
        })
    }

    pub async fn append(
        &mut self,
        key: &Vec<u8>,
        value: &Vec<u8>,
        created_at: u64,
        is_tombstone: bool,
    ) -> Result<usize, Error> {
        let v_log_entry = ValueLogEntry::new(
            key.len(),
            value.len(),
            key.to_vec(),
            value.to_vec(),
            created_at,
            is_tombstone,
        );
        let serialized_data = v_log_entry.serialize();
        // Get the current offset before writing(this will be the offset of the value stored in the memtable)
        let last_offset = self.content.file.node.size().await;
        let data_file = &self.content;
        let _ = data_file.file.node.write_all(&serialized_data).await;
        Ok(last_offset as usize)
    }

    pub async fn get(&self, start_offset: usize) -> Result<Option<(Vec<u8>, bool)>, Error> {
        self.content.file.get(start_offset).await
    }

    pub async fn sync_to_disk(&self) -> Result<(), Error> {
        self.content.file.node.sync_all().await
    }

    pub async fn recover(&mut self, start_offset: usize) -> Result<Vec<ValueLogEntry>, Error> {
        self.content.file.recover(start_offset).await
    }

    pub async fn read_chunk_to_garbage_collect(
        &self,
        bytes_to_collect: usize,
    ) -> Result<(Vec<ValueLogEntry>, TotalBytesRead), Error> {
        self.content
            .file
            .read_chunk_to_garbage_collect(bytes_to_collect, self.tail_offset as u64)
            .await
    }

    // CAUTION: This deletes the value log file
    pub async fn clear_all(&mut self) {
        if self.content.file.node.metadata().await.is_ok() {
            if let Err(err) = self.content.file.node.remove_dir_all().await {
                log::error!("{}", err);
            }
        }
        self.tail_offset = 0;
        self.head_offset = 0;
    }

    pub fn set_head(&mut self, head: usize) {
        self.head_offset = head;
    }

    pub fn set_tail(&mut self, tail: usize) {
        self.tail_offset = tail;
    }
}

impl ValueLogEntry {
    pub fn new(ksize: usize, vsize: usize, key: Vec<u8>, value: Vec<u8>, created_at: u64, is_tombstone: bool) -> Self {
        Self {
            ksize,
            vsize,
            key,
            value,
            created_at,
            is_tombstone,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let entry_len = SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + self.key.len() + self.value.len() + SIZE_OF_U8;

        let mut serialized_data = Vec::with_capacity(entry_len);

        serialized_data.extend_from_slice(&(self.key.len() as u32).to_le_bytes());

        serialized_data.extend_from_slice(&(self.value.len() as u32).to_le_bytes());

        serialized_data.extend_from_slice(&self.created_at.to_le_bytes());

        serialized_data.push(self.is_tombstone as u8);

        serialized_data.extend_from_slice(&self.key);

        serialized_data.extend_from_slice(&self.value);

        serialized_data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialized_deserialized() {}
}
