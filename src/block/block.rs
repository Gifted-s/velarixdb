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
//! |  - index: HashMap<Arc<Vec<u8>>, usize> |   // Index for key-based lookups
//! |  - entry_count: usize             |   // Number of entries in the block
//! +----------------------------------+
//! |           Block Data              |
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

use std::{collections::HashMap, sync::Arc};

use crate::err::StorageEngineError;

const BLOCK_SIZE: usize = 4 * 1024; // 4KB

const SIZE_OF_U32: usize = std::mem::size_of::<u32>();

#[derive(Debug, Clone)]
pub struct Block {
    pub data: Vec<u8>,
    pub entry_count: usize,
}

impl Block {
    /// Creates a new empty Block.
    pub fn new() -> Self {
        Block {
            data: Vec::with_capacity(BLOCK_SIZE),
            entry_count: 0,
        }
    }

    /// Checks if the Block is full given the size of an entry.
    pub fn is_full(&self, entry_size: usize) -> bool {
        self.data.len() + entry_size > BLOCK_SIZE
    }

    /// Sets an entry with the provided key and value in the Block.
    ///
    /// Returns an `io::Result` indicating success or failure. An error is returned if the Block
    /// is already full and cannot accommodate the new entry.
    pub fn set_entry(&mut self, entry_vec: Vec<u8>) -> Result<(), StorageEngineError> {
        // Calculate the total size of the entry, including the key, value, and the size of the length prefix.
        let entry_size = entry_vec.len();

        // Check if the Block is already full and cannot accommodate the new entry.
        if self.is_full(entry_size) {
            return Err(StorageEngineError::BlockIsFullError);
        }

        // Get the current offset in the data vector and extend it with the new entry.
        let offset = self.data.len();
        self.data.extend_from_slice(&entry_vec);

        // Increment the entry count.
        self.entry_count += 1;

        Ok(())
    }


    /// Retrieves the value associated with the provided key from the Block.
    ///
    /// Returns `Some(value)` if the key is found in the Block, `None` otherwise.
    // pub(crate) fn get_value(&self, key: &[u8]) -> Option<Vec<u8>> {
    //     // Check if the key exists in the index.
    //     if let Some(&offset) = self.index.get(&Arc::new(key.to_owned())) {
    //         // Calculate the starting position of the value in the data vector.
    //         let start = offset + SIZE_OF_U32 + key.len();

    //         // Extract the bytes representing the length of the value from the data vector.
    //         let value_len_bytes = &self.data[offset..offset + SIZE_OF_U32];

    //         // Convert the value length bytes into a u32 value using little-endian byte order.
    //         let value_len = u32::from_le_bytes(value_len_bytes.try_into().unwrap()) as usize;

    //         // Calculate the ending position of the value in the data vector.
    //         let end = start + value_len;

    //         // Extract the value bytes from the data vector and return them as a new Vec<u8>.
    //         Some(self.data[start..end].to_vec())
    //     } else {
    //         // The key was not found in the index, return None.
    //         None
    //     }
    // }

    /// Returns the number of entries in the Block.
    pub(crate) fn entry_count(&self) -> usize {
        self.entry_count
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
