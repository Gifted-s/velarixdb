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
//! ### head_offset
//!
//! The `head_offset` field stores the start postion of the last entry inserted into the value log
//!
//! ### tail_offset
//!
//! The `tail_offset` field stores the position  we start reading from either normal reads or during garbage collection
//!
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

use chrono::{DateTime, Utc};

use crate::{
    consts::{SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8, VLOG_FILE_NAME},
    err::Error,
    fs::{FileAsync, FileNode, VLogFileNode, VLogFs},
    types::{ByteSerializedEntry, CreatedAt, IsTombStone, ValOffset, Value},
};
use std::path::{Path, PathBuf};
type TotalBytesRead = usize;

/// Value log file
#[derive(Debug, Clone)]
pub struct VFile<F: VLogFs> {
    pub file: F,
    pub path: PathBuf,
}

impl<F: VLogFs> VFile<F> {
    pub fn new<P: AsRef<Path> + Send + Sync>(path: P, file: F) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            file,
        }
    }
}

/// Append only log that keeps entries
/// persisted on the disk
#[derive(Debug, Clone)]
pub struct ValueLog {
    /// Value log file contents
    pub content: VFile<VLogFileNode>,

    /// Head of value log (represents the offset reads will
    /// start from in case of crash recovery, the field is updated to
    /// offset of the most recent entry in a `MemTable` during flush)
    pub head_offset: usize,

    /// Tail of the value log (represents the start offset of value log,
    /// reads starts here and it is updated during Garbage collection)
    pub tail_offset: usize,

    /// Size of the Value log
    pub size: usize,
}

/// Value log entry
#[derive(PartialEq, Debug, Clone)]
pub struct ValueLogEntry {
    /// Represents size of key
    pub ksize: usize,

    /// Represents size of value
    pub vsize: usize,

    /// Represents key
    pub key: Vec<u8>,

    /// Represents value
    pub value: Vec<u8>,

    /// Represents when entry was created
    pub created_at: DateTime<Utc>,

    /// True means entry has been deleted
    pub is_tombstone: bool,
}

impl ValueLog {
    /// Creates new `ValueLog`
    pub async fn new<P: AsRef<Path> + Send + Sync>(dir: P) -> Result<Self, Error> {
        // will only create if directory does not exist
        FileNode::create_dir_all(dir.as_ref()).await?;
        let file_path = dir.as_ref().join(VLOG_FILE_NAME);
        let file = VLogFileNode::new(file_path.to_owned(), crate::fs::FileType::ValueLog)
            .await
            .unwrap();
        // Get size from file in case of crash recovery
        let size = file.node.size().await;
        Ok(Self {
            head_offset: 0,
            tail_offset: 0,
            content: VFile::new(file_path, file),
            // IMPORTANT: cache vlog size in memory
            size,
        })
    }

    /// Appends new entry to value log
    ///
    /// Returns start offset of the newly inserted entry
    pub async fn append<T: AsRef<[u8]>>(
        &mut self,
        key: T,
        value: T,
        created_at: CreatedAt,
        is_tombstone: bool,
    ) -> Result<ValOffset, Error> {
        let v_log_entry = ValueLogEntry::new(
            key.as_ref().len(),
            value.as_ref().len(),
            key.as_ref().to_vec(),
            value.as_ref().to_vec(),
            created_at,
            is_tombstone,
        );

        let serialized_data = v_log_entry.serialize();
        // Get the current offset before writing(this will be the offset of the value stored in the memtable)
        let last_offset = self.size;
        let data_file = &self.content;
        data_file.file.node.write_all(&serialized_data).await?;
        self.size += serialized_data.len();
        Ok(last_offset)
    }

    /// Fetches value from value log
    ///
    /// returns tuple of Value and Tombstone
    ///
    /// # Error
    ///
    /// Returns error in case there is an IO error
    pub async fn get(&self, start_offset: usize) -> Result<Option<(Value, IsTombStone)>, Error> {
        self.content.file.get(start_offset).await
    }

    /// Ensures value log entries are persisted on the disk
    ///
    ///
    /// # Error
    ///
    /// Returns error in case there is an IO error
    pub async fn sync_to_disk(&self) -> Result<(), Error> {
        self.content.file.node.sync_all().await
    }

    /// Fetches an entry from value log using the `start_offset`
    /// 
    /// 
    /// This is used to fetch all entries that is yet to be flushed
    /// before crash happened
    /// 
    /// # Error
    ///
    /// Returns error in case there is an IO error
    pub async fn recover(&mut self, start_offset: usize) -> Result<Vec<ValueLogEntry>, Error> {
        self.content.file.recover(start_offset).await
    }

    /// Returns entries within `gc_chunk_size` to garbage collection
    ///
    /// # Errors
    ///
    /// Returns error in case there is an IO error
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
                log::info!("{}", err);
            }
        }
        self.size=0;
        self.tail_offset = 0;
        self.head_offset = 0;
    }

    /// Sets `head_offset` of `ValueLog`
    pub fn set_head(&mut self, head: usize) {
        self.head_offset = head;
    }

    /// Sets `tail_offset` of `ValueLog`
    pub fn set_tail(&mut self, tail: usize) {
        self.tail_offset = tail;
    }
}

impl ValueLogEntry {
    /// Creates new `ValueLogEntry`
    pub fn new<T: AsRef<[u8]>>(
        ksize: usize,
        vsize: usize,
        key: T,
        value: T,
        created_at: CreatedAt,
        is_tombstone: bool,
    ) -> Self {
        Self {
            ksize,
            vsize,
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
            created_at,
            is_tombstone,
        }
    }

    /// Converts value log entry to a byte vector
    pub(crate) fn serialize(&self) -> ByteSerializedEntry {
        let entry_len = SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + self.key.len() + self.value.len() + SIZE_OF_U8;
        let mut serialized_data = Vec::with_capacity(entry_len);

        serialized_data.extend_from_slice(&(self.key.len() as u32).to_le_bytes());

        serialized_data.extend_from_slice(&(self.value.len() as u32).to_le_bytes());

        serialized_data.extend_from_slice(&self.created_at.timestamp_millis().to_le_bytes());

        serialized_data.push(self.is_tombstone as u8);

        serialized_data.extend_from_slice(&self.key);

        serialized_data.extend_from_slice(&self.value);

        serialized_data
    }
}

