//! # SSTable Index
//!
//! The `Index` is used for fast scan on the sstable.
//!
//! The Index structure
//!
//! ```text
//! +----------------------------------+
//! |             Index                |
//! +----------------------------------+
//! |  - entries: Vec<IndexEntry>      |   // entries within the index
//! |                                  |
//! |  - file: IndexFile               |   // Index File
//! +----------------------------------+
//! |         Index Entries            |
//! |   +------------------------+     |
//! |   |   Entry 1              |     |
//! |   | +-------------------+  |     |
//! |   | |   Key Prefix      |  |     |
//! |   | | (4 bytes, little- |  |     |
//! |   | |   endian format)  |  |     |
//! |   | +-------------------+  |     |
//! |   | |       Key         |  |     |
//! |   | | (variable length) |  |     |
//! |   | +-------------------+  |     |
//! |   | |   Block Handle    |  |     |     
//! |   | | (4 bytes,little-  |  |     |
//! |   | |   endian format)  |  |     |
//! |   | +-------------------+  |     |
//! |   | |  TODO[Compressed  |  |     |
//! |   | |    Block Size]    |  |     |
//! |   | | (4 bytes, little- |  |     |
//! |   | |  endian format)   |  |     |
//! |   | +-------------------+  |     |
//! |   +------------------------+     |
//! |   |   Entry 2              |     |
//! |   |       ...              |     |
//! |   +------------------------+     |
//! +----------------------------------+
//! ```
//!
//! In the diagram:
//! - The `Index` struct represents the index for the sstable.
//! - The `entries` field of the `Index` is a vector (`Vec<IndexEntry>`) that stores the index entries
//!
//! Each entry within the block consists of four parts:
//! 1. Length Prefix: A 4-byte length prefix in little-endian format, indicating the length of the last key in the block.
//! 2. Key: Variable-length key bytes, representing the last key in the block.
//! 3. Block Handle: A 4-byte length prefix in little-endian format, indicating the start of the block in the data file
//! - TODO: Block compresion size:  A 4-byte length prefix in little-endian format, indicating the compressed size of the block
use crate::consts::SIZE_OF_U32;
use crate::err::Error;
use crate::fs::{FileAsync, IndexFileNode, IndexFs};
use crate::types::{ByteSerializedEntry, Key};
use std::path::{Path, PathBuf};

use Error::*;
type Offset = u32;
type KeyLength = u32;
type BlockOffset = u32;

/// Represents index file  
#[derive(Debug, Clone)]
pub struct IndexFile<F: IndexFs> {
    pub(crate) file: F,
    pub(crate) path: PathBuf,
}

impl<F: IndexFs> IndexFile<F> {
    pub fn new<P: AsRef<Path> + Send + Sync>(path: P, file: F) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            file,
        }
    }
}

/// Represents each entry in the index
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// Key length used to determine the length of key during
    /// retrieval from file
    pub key_len: KeyLength,

    /// Key
    pub key: Key,

    /// Start offset of the [`Block`] key is located at
    pub block_handle: Offset,
    // TODO: pub: compressed_size
}

/// Represents index
#[derive(Debug, Clone)]
pub struct Index {
    entries: Vec<IndexEntry>,
    file: IndexFile<IndexFileNode>,
}

/// Represents range offset (used for range queries)
#[derive(Debug, Clone)]
pub struct RangeOffset {
    /// Block to start reading from in case of range queries
    pub start_offset: Offset,

    /// Block to stop reading from in case of range queries
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
    // Creates new `Index`
    pub fn new<P: AsRef<Path> + Send + Sync>(path: P, file: IndexFileNode) -> Self {
        Self {
            entries: Vec::new(),
            file: IndexFile::new(path, file),
        }
    }

    /// Inserts new entry
    pub fn insert(&mut self, key_len: u32, key: Key, offset: Offset) {
        self.entries.push(IndexEntry {
            key_len,
            key,
            block_handle: offset,
        })
    }

    /// Writes index to file,
    /// Return IO error in case it happens
    pub async fn write_to_file(&self) -> Result<(), Error> {
        for e in &self.entries {
            let serialized_input = self.serialize_entry(e)?;
            self.file.file.node.write_all(&serialized_input).await?;
        }
        Ok(())
    }

    /// Serializes the entry in the index as a byte vector
    ///
    /// Returns `ByteSerializedEntry`or Error if not
    fn serialize_entry(&self, e: &IndexEntry) -> Result<ByteSerializedEntry, Error> {
        let entry_len = e.key.len() + SIZE_OF_U32 + SIZE_OF_U32;

        let mut entry_vec = Vec::with_capacity(entry_len);

        // key len
        entry_vec.extend_from_slice(&(e.key_len).to_le_bytes());

        // key
        entry_vec.extend_from_slice(&e.key);

        // block offset
        entry_vec.extend_from_slice(&e.block_handle.to_le_bytes());
        if entry_len != entry_vec.len() {
            return Err(Serialization("Invalid entry size"));
        }
        Ok(entry_vec)
    }
    /// Retrieves a Block Offset from index file
    pub(crate) async fn get<K: AsRef<[u8]>>(&self, searched_key: K) -> Result<Option<BlockOffset>, Error> {
        self.file.file.get_from_index(searched_key.as_ref()).await
    }

    // pub(crate) async fn get_block_offset_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<RangeOffset, Error> {
    //     self.file.file.get_block_range(start_key, end_key).await
    // }
}
