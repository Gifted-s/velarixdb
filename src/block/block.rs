//! # SSTable Block
//!
//! The `SSTable` manages multiple `Block` instances to store entries, and the `Block` handles individual block-level operations.
//!
//! The block structure
//!
//! ```text
//! +----------------------------------+
//! |             Block                |
//! +----------------------------------+
//! |  - entries: Vec<Entry>           |   // entries entries within the block
//! |                                  |
//! |  - entry_count: usize            |   // Number of entries in the block
//! |                                  |
//! |  - size: usize                   |   // Size of the block in bytes
//! +----------------------------------+
//! |         Block Entries            |
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
//! |   | |   Value Offset    |  |     |     
//! |   | | (4 bytes,little-  |  |     |
//! |   | |   endian format)  |  |     |
//! |   | +-------------------+  |     |
//! |   | |   Creation Date   |  |     |
//! |   | | (8 bytes, little- |  |     |
//! |   | |  endian format)   |  |     |
//! |   | +-------------------+  |     |
//! |   | |   Is_tombstone    |  |     |
//! |   | | (1 bytes, little- |  |     |   
//! |   | |   endian format)  |  |     |    
//! |   | +-------------------+  |     |
//! |   +------------------------+     |
//! |   |   Entry 2              |     |
//! |   |       ...              |     |
//! |   +------------------------+     |
//! +----------------------------------+
//! ```
//!
//! In the diagram:
//! - The `Block` struct represents an individual block within the SSTable.
//! - The `entries` field of the `Block` is a vector (`Vec<BlockEntry>`) that stores the block entries
//! - The `entry_count` field keeps track of the number of entries in the block.
//!
//! Each entry within the block consists of four parts:
//! 1. Length Prefix: A 4-byte length prefix in little-endian format, indicating the length of the key.
//! 2. Key: Variable-length key bytes.
//! 3. Value Offset: A 4-byte length prefix in little-endian format, indicating the position of the value in the value log
//! 4. Creation Date: A 8-byte length prefix in little-endian format, indicating the time the insertion was made
//! 5. Is Tombstone: A 1-byte length prefix in little-endian format, indicating if the key has been deleted or not
//!
//! The block's entries vector (`entries`) stores these entries sequentially. Each entry follows the format mentioned above, and they are concatenated one after another within the entries vector.
//!
// NOTE: For creation time while a 32-bit integer can technically hold milliseconds, the usable range is limited,
// making it unsuitable for long-term timekeeping applications. For those scenarios, 64-bit(8 byte) integers are typically used.

use err::Error::*;

use crate::{
    consts::{SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8},
    err::{self, Error},
    fs::{FileAsync, FileNode}, types::Key,
};
type BytesWritten = usize;
const BLOCK_SIZE: usize = 4 * 1024; // 4KB

#[derive(Debug, Clone)]
pub struct Block {
    pub entries: Vec<BlockEntry>,
    pub size: usize,
    pub entry_count: usize,
}

#[derive(Debug, Clone)]
pub struct BlockEntry {
    pub key_prefix: u32,
    pub key: Vec<u8>,
    pub value_offset: u32,
    pub creation_date: u64,
    pub is_tombstone: bool,
}
impl Block {
    /// Creates a new empty Block.
    pub fn new() -> Self {
        Block {
            size: 0,
            entries: Vec::with_capacity(BLOCK_SIZE),
            entry_count: 0,
        }
    }

    pub fn get_last_entry(&self) -> BlockEntry {
        self.entries[self.entries.len() - 1].to_owned()
    }

    /// Sets an entry with the provided key and value offset in the Block.
    ///
    /// Returns an `Result` indicating success or failure. An error is returned if the Block
    /// is already full and cannot accommodate the new entry.
    pub fn set_entry<K: AsRef<[u8]>>(
        &mut self,
        key_prefix: u32,
        key: K,
        value_offset: u32,
        creation_date: u64,
        is_tombstone: bool,
    ) -> Result<(), Error> {
        // Key + Key Prefix + Value Offset +  Creation Date + Tombstone Marker
        let entry_size = key.as_ref().len() + SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;

        if self.is_full(entry_size) {
            return Err(Error::BlockIsFullError);
        }

        let entry = BlockEntry {
            key: key.as_ref().to_vec(),
            key_prefix,
            creation_date,
            is_tombstone,
            value_offset,
        };
        self.entries.push(entry);
        self.size += entry_size;
        self.entry_count += 1;

        Ok(())
    }

    /// Writes entries in the block to the sstable file
    ///
    /// Returns an `Result` indicating success or failure. An error is returned if write fails
    pub async fn write_to_file(&self, file: FileNode) -> Result<BytesWritten, Error> {
        let mut bytes_written = 0;
        for entry in &self.entries {
            let serialized_entry = self.serialize(entry)?;
            file.write_all(&serialized_entry).await?;
            bytes_written += serialized_entry.len();
        }
        Ok(bytes_written)
    }

    /// Checks if the Block is full given the size of an entry.
    pub fn is_full(&self, entry_size: usize) -> bool {
        self.size + entry_size > BLOCK_SIZE
    }

    #[cfg(test)]
    /// Get entry count
    pub fn get_entry_count(&self) -> usize {
        self.entry_count
    }

    /// Serializes the entries in the block as a byte vector
    ///
    /// Returns `Ok(entry_vec)`or Error if not
    pub(crate) fn serialize(&self, entry: &BlockEntry) -> Result<Key, Error> {
        let entry_len = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;
        let mut entry_vec = Vec::with_capacity(entry_len);
        entry_vec.extend_from_slice(&(entry.key_prefix).to_le_bytes());
        entry_vec.extend_from_slice(&entry.key);
        entry_vec.extend_from_slice(&(entry.value_offset as u32).to_le_bytes());
        entry_vec.extend_from_slice(&entry.creation_date.to_le_bytes());
        entry_vec.push(entry.is_tombstone as u8);
        if entry_len != entry_vec.len() {
            return Err(SerializationError("Invalid input"));
        }

        Ok(entry_vec)
    }

    /// Retrieves the value offset associated with the provided key from the Block.
    ///
    /// Returns `Some(value)` if the key is found in the Block, `None` otherwise.
    /// Method will be used when we implement the block cache
    #[allow(dead_code)]
    pub(crate) fn get_entry<K: AsRef<[u8]>>(&self, key: &K) -> Option<&BlockEntry> {
        self.entries.iter().find(|entry| *entry.key == *key.as_ref())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use tokio::{fs::File, sync::RwLock};

    #[test]
    fn test_new_empty_block_creation() {
        let block = Block::new();
        assert_eq!(block.entries.len(), 0);
        assert_eq!(block.entries.capacity(), BLOCK_SIZE);
        assert_eq!(block.entry_count, 0);
    }

    #[test]
    fn test_is_full() {
        let block = Block::new();
        assert!(!block.is_full(10));
        assert!(block.is_full(BLOCK_SIZE + 1));
    }

    #[test]
    fn test_set_entry() {
        let mut block = Block::new();
        let key: Key = vec![1, 2, 3];
        let value_offset: u32 = 1000;
        let creation_date: u64 = 16345454545;
        let is_tombstone: bool = false;

        let res = block.set_entry(
            key.len() as u32,
            key.to_owned(),
            value_offset,
            creation_date,
            is_tombstone,
        );
        // check if we have Error.
        assert!(res.is_ok());

        assert_eq!(block.entries.len(), 1);
        assert_eq!(block.entry_count, 1);

        assert_eq!(
            block.size,
            key.len() + SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8
        );
    }

    #[test]
    fn test_serialize() {
        let block = Block::new();
        let key: Key= vec![1, 2, 3];
        let value_offset: u32 = 1000;
        let creation_date: u64 = 16345454545;
        let is_tombstone: bool = false;

        let entry = BlockEntry {
            key_prefix: key.len() as u32,
            key: key.clone(),
            value_offset,
            creation_date,
            is_tombstone,
        };
        let res = block.serialize(&entry);
        // check if we have Error.
        assert!(res.is_ok());
        assert_eq!(
            res.unwrap().len(),
            key.len() + SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8
        );
    }

    #[tokio::test]
    async fn test_write_to_file() {
        let mut block = Block::new();
        let key: Key = vec![1, 2, 3];
        let value_offset: u32 = 1000;
        let creation_date: u64 = 16345454545;
        let is_tombstone: bool = false;

        let res = block.set_entry(
            key.len() as u32,
            key.to_owned(),
            value_offset,
            creation_date,
            is_tombstone,
        );
        // check if we have Error.
        assert!(res.is_ok());
        assert_eq!(block.entries.len(), 1);
        assert_eq!(block.entry_count, 1);
        assert_eq!(
            block.size,
            key.len() + SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8
        );
        let temp_file = NamedTempFile::new().unwrap();
        let temp_file_path = temp_file.path().to_path_buf();

        let std_file = temp_file.into_file();
        let tokio_file = File::from_std(std_file);

        let file = FileNode {
            file_path: temp_file_path.to_owned(),
            file: Arc::new(RwLock::new(tokio_file)),
            file_type: crate::fs::FileType::Data,
        };
        let write_res = block.write_to_file(file.clone()).await;
        assert!(write_res.is_ok());
        assert_eq!(write_res.unwrap(), block.size)
    }

    #[test]
    fn test_get_entry() {
        let mut block = Block::new();
        let key: Key = vec![1, 2, 3];
        let value_offset: u32 = 1000;
        let creation_date: u64 = 16345454545;
        let is_tombstone: bool = false;

        let res = block.set_entry(
            key.len() as u32,
            key.to_owned(),
            value_offset,
            creation_date,
            is_tombstone,
        );
        assert!(res.is_ok());
        let entry = block.get_entry(&key);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().key, key);
    }

    #[test]
    fn test_get_value_nonexistent_key() {
        let block = Block::new();
        // Test case to check getting a value for a non-existent key
        let key: Key = vec![1, 2, 3];
        let value = block.get_entry(&key);
        assert!(value.is_none());
    }

    #[test]
    fn test_set_entry_full_block() {
        // Test case to check setting an entry when the block is already full
        let mut block = Block::new();
        let key: Key = vec![1, 2, 3];
        let value_offset: u32 = 1000;
        let creation_date: u64 = 16345454545;
        let is_tombstone: bool = false;

        // Fill the block to its maximum capacity
        while !block.is_full(key.len() + SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8) {
            block
                .set_entry(
                    key.len() as u32,
                    key.to_owned(),
                    value_offset,
                    creation_date,
                    is_tombstone,
                )
                .unwrap();
        }

        // Attempt to set a new entry, which should result in an error
        let res = block.set_entry(
            key.len() as u32,
            key.to_owned(),
            value_offset,
            creation_date,
            is_tombstone,
        );
        assert!(res.is_err());
        assert_eq!(
            block.get_entry_count(),
            BLOCK_SIZE / (key.len() + SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8)
        );
    }
}
