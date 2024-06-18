//! # SSTable Block
//!
//! The `SSTable` manages multiple `Block` instances to store the data, and the `Block` handles individual block-level operations and indexing.
//!
//! Alignments of key-value pairs inside a Block:
//!
//! ```text
//! +----------------------------------+
//! |             Block                |
//! +----------------------------------+
//! |  - data: Vec<u8>                 |   // Data entries within the block
//! |                                  |
//! |  - entry_count: usize            |   // Number of entries in the block
//! +----------------------------------+
//! |           Block Data             |
//! |   +------------------------+     |
//! |   |   Entry 1              |     |
//! |   | +-------------------+  |     |
//! |   | |   Length Prefix   |  |     |
//! |   | | (4 bytes, little- |  |     |
//! |   | |   endian format)  |  |     |
//! |   | +-------------------+  |     |
//! |   | |       Key         |  |     |
//! |   | | (variable length) |  |     |
//! |   | +-------------------+  |     |
//! |   | |      Value        |  |     |
//! |   | | (variable length) |  |     |
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
//! - The `data` field of the `Block` is a vector (`Vec<u8>`) that stores the data entries.
//! - The `index` field is a `HashMap` that maintains the index for efficient key-based lookups.
//! - The `entry_count` field keeps track of the number of entries in the block.
//!
//! Each entry within the block consists of three parts:
//! 1. Length Prefix: A 4-byte length prefix in little-endian format, indicating the length of the value.
//! 2. Key: Variable-length key bytes.
//! 3. Value: Variable-length value bytes.
//!
//! The block's data vector (`data`) stores these entries sequentially. Each entry follows the format mentioned above, and they are concatenated one after another within the data vector.
//! The index hashmap (`index`) maintains references to the keys and their corresponding offsets within the data vector.
//!

use tokio::io::{self};

use err::Error::*;

use crate::{
    consts::{SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8},
    err::{self, Error},
    fs::{FileAsync, FileNode},
};
const BLOCK_SIZE: usize = 4 * 1024; // 4KB

#[derive(Debug, Clone)]
pub struct Block {
    pub data: Vec<BlockEntry>,
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
            data: Vec::with_capacity(BLOCK_SIZE),
            entry_count: 0,
        }
    }

    pub fn get_first_entry(&self) -> BlockEntry {
        self.data[0].clone()
    }

    /// Sets an entry with the provided key and value in the Block.
    ///
    /// Returns an `io::Result` indicating success or failure. An error is returned if the Block
    /// is already full and cannot accommodate the new entry.
    pub fn set_entry(
        &mut self,
        key_prefix: u32,
        key: Vec<u8>,
        value_offset: u32,
        creation_date: u64,
        is_tombstone: bool,
    ) -> Result<(), Error> {
        // Calculate the total size of the entry, including the key, value, and the size of the length prefix.
        // Key + Key Prefix + Value Offset +  Creation Date + Tombstone Marker
        let entry_size =
            key.len() + SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;

        // Check if the Block is already full and cannot accommodate the new entry.
        if self.is_full(entry_size) {
            return Err(Error::BlockIsFullError);
        }

        // Get the current offset in the data vector and extend it with the new entry.
        let entry = BlockEntry {
            key,
            key_prefix,
            creation_date,
            is_tombstone,
            value_offset,
        };
        self.data.push(entry);
        self.size += entry_size;
        // Increment the entry count.
        self.entry_count += 1;

        Ok(())
    }

    pub async fn write_to_file(&self, file: FileNode) -> Result<(), Error> {
        for entry in &self.data {
            let serialized_entry = self.serialize(entry)?;
            file.write_all(&serialized_entry).await?;
        }
        Ok(())
    }

    /// Checks if the Block is full given the size of an entry.
    pub fn is_full(&self, entry_size: usize) -> bool {
        self.size + entry_size > BLOCK_SIZE
    }

    /// Retrieves the value associated with the provided key from the Block.
    ///
    /// Returns `Some(value)` if the key is found in the Block, `None` otherwise.
    pub(crate) fn serialize(
        &self,
        entry: &BlockEntry,
    ) -> Result<Vec<u8>, Error> {
        let entry_len = entry.key.len()
            + SIZE_OF_U32
            + SIZE_OF_U32
            + SIZE_OF_U64
            + SIZE_OF_U8;
        let mut entry_vec = Vec::with_capacity(entry_len);
        //TODO: serialize
        //add key len
        entry_vec.extend_from_slice(&(entry.key_prefix).to_le_bytes());

        //add key
        entry_vec.extend_from_slice(&entry.key);

        //add value offset
        entry_vec.extend_from_slice(&(entry.value_offset as u32).to_le_bytes());

        //write date created in milliseconds
        entry_vec.extend_from_slice(&entry.creation_date.to_le_bytes());

        //write is tombstone to file
        entry_vec.push(entry.is_tombstone as u8);
        if entry_len != entry_vec.len() {
            return Err(SSTableWriteError {
                error: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid Input",
                ),
            });
        }

        Ok(entry_vec)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_new_empty_block_creation() {
//         let block = Block::new();
//         assert_eq!(block.data.len(), 0);
//         assert_eq!(block.data.capacity(), BLOCK_SIZE);
//         assert_eq!(block.entry_count, 0);
//     }

//     #[test]
//     fn test_is_full() {
//         let block = Block::new();
//         assert!(!block.is_full(10));
//         assert!(block.is_full(BLOCK_SIZE + 1));
//     }

//     #[test]
//     fn test_set_entry() {
//         let mut block = Block::new();
//         let mut key: Vec<u8> = vec![1, 2, 3];
//         let mut value: Vec<u8> = vec![4, 5, 6];
//         let entry_vec= key.append(&mut value);
//         let res = block.set_entry(key);
//         // check if we have IO error.
//         assert!(res.is_ok());

//         assert_eq!(block.data.len(), key.len() + value.len() + SIZE_OF_U32);
//         assert_eq!(block.entry_count, 1);
//     }

//     #[test]
//     fn test_set_and_get_value() {
//         let mut block = Block::new();
//         let key: &[u8] = &[1, 2, 3];
//         let value: &[u8] = &[4, 5, 6];
//         let entry_vec= key.append(&mut value);
//         let res = block.set_entry(key.to_vec());
//         assert!(res.is_ok());

//         // Retrieve the value using the key
//         // let retrieved_value = block.get_value(key);
//         // assert_eq!(retrieved_value, Some(value.to_vec()));
//     }

//     #[test]
//     fn test_set_and_remove_entry() {
//         let mut block = Block::new();
//         let key1: Vec<u8> = vec![1, 2, 3];
//         let value1: Vec<u8> = vec![4, 5, 6];
//         let key2: Vec<u8> = vec![7, 8, 9];
//         let value2: Vec<u8> = vec![10, 11, 12];
//         let key3: Vec<u8> = vec![13, 14, 15];
//         let value3: Vec<u8> = vec![16];

//         let _ = block.set_entry(&key1, &value1);
//         assert_eq!(block.entry_count, 1);
//         // assert_eq!(block.get_value(&key1), Some(value1));

//         let _ = block.set_entry(&key2, &value2);
//         assert_eq!(block.entry_count, 2);
//         // assert_eq!(block.get_value(&key2), Some(value2));

//         let _ = block.set_entry(&key3, &value3);
//         assert_eq!(block.entry_count, 3);
//         // assert_eq!(block.get_value(&key3), Some(value3.clone()));

//         let entry_count_before_removal = block.entry_count();
//         // let result = block.remove_entry(&key1);
//         // assert!(result);
//         assert_eq!(block.entry_count(), entry_count_before_removal - 1);
//         // assert_eq!(block.get_value(&key1), None);
//     }

//     #[test]
//     fn test_set_entry_full_block() {
//         // Test case to check setting an entry when the block is already full
//         let mut block = Block::new();
//         let key: Vec<u8> = vec![1, 2, 3];
//         let value: Vec<u8> = vec![4, 5, 6];

//         // Fill the block to its maximum capacity
//         while !block.is_full(key.len() + value.len() + SIZE_OF_U32) {
//             block.set_entry(&key, &value).unwrap();
//         }

//         // Attempt to set a new entry, which should result in an error
//         let res = block.set_entry(&key, &value);
//         assert!(res.is_err());
//         assert_eq!(
//             block.entry_count(),
//             BLOCK_SIZE / (key.len() + value.len() + SIZE_OF_U32)
//         );
//     }

//     #[test]
//     fn test_remove_entry_nonexistent_key() {
//         // Test case to check removing a non-existent key
//         let mut block = Block::new();
//         let key1: Vec<u8> = vec![1, 2, 3];
//         let key2: Vec<u8> = vec![4, 5, 6];

//         block.set_entry(&key1, &[7, 8, 9]).unwrap();

//         // Attempt to remove a non-existent key, which should return false
//         // let result = block.remove_entry(&key2);
//         // assert!(!result);
//         // assert_eq!(block.entry_count(), 1);
//     }

//     #[test]
//     fn test_get_value_nonexistent_key() {
//         // Test case to check getting a value for a non-existent key
//         let block = Block::new();
//         let key: Vec<u8> = vec![1, 2, 3];

//         // Attempt to get the value for a non-existent key, which should return None
//         //let value = block.get_value(&key);
//         // assert_eq!(value, None);
//     }
