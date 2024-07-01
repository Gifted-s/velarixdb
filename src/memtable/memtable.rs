//! # Memtable
//!
//! Memtable buffers write in the RAM before it's flushed to the disk once the size exceeds `write_buffer_size`.
//! Entries are stored in a SkipMap so they can be retrieved effectively.
//! Before a memtable is finally flushed to the disk, it is made read-only and added to the read-only memtable vector.
//! Once the read-only memtable vector exceeds the `max_buffer_write_number` all memtable in the vector is flushed to to the disk concurrently

use crate::bucket::InsertableToBucket;
use crate::consts::{DEFAULT_FALSE_POSITIVE_RATE, SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8, WRITE_BUFFER_SIZE};
use crate::err::Error;
use crate::filter::BloomFilter;
use crate::storage::SizeUnit;
use crate::types::{CreatedAt, IsTombStone, Key, SkipMapEntries, ValOffset, Value};
use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;
use futures::lock::Mutex;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::cmp::Ordering;
use std::fmt::Debug;
use Error::*;

use std::{hash::Hash, sync::Arc};

pub trait K: AsRef<[u8]> + Hash + Ord + Send + Sync + Clone + Debug {}

impl<T> K for T where T: AsRef<[u8]> + Hash + Ord + Send + Sync + Clone + Debug {}

#[derive(PartialOrd, PartialEq, Copy, Clone, Debug)]
pub struct Entry<Key: K, V: Ord> {
    pub key: Key,
    pub val_offset: V,
    pub created_at: CreatedAt,
    pub is_tombstone: bool,
}
#[derive(Clone, Debug, PartialEq)]
pub struct SkipMapValue<V: Ord> {
    pub val_offset: V,
    pub created_at: CreatedAt,
    pub is_tombstone: IsTombStone,
}

impl<V: Ord> SkipMapValue<V> {
    pub(crate) fn new(val_offset: V, created_at: CreatedAt, is_tombstone: IsTombStone) -> Self {
        SkipMapValue {
            val_offset,
            created_at,
            is_tombstone,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemTable<Key: K> {
    pub entries: SkipMapEntries<Key>,
    pub bloom_filter: BloomFilter,
    pub false_positive_rate: f64,
    pub size: usize,
    pub size_unit: SizeUnit,
    pub capacity: usize,
    pub created_at: CreatedAt,
    pub read_only: bool,
    pub most_recent_entry: Entry<Key, ValOffset>,
}

#[allow(dead_code)]
pub enum ValueOption {
    /// We might need to cache the raw value in memory if the size is small, this will reduce
    /// number of Disk IO because value of smaller size can reside and fetched in memory before it
    /// is been flushed to disk
    Raw(Value),

    /// Value offset gotten from value position in value log
    Offset(ValOffset),

    /// Represents deleted entry
    TombStone(IsTombStone),
}

impl InsertableToBucket for MemTable<Key> {
    fn get_entries(&self) -> SkipMapEntries<Key> {
        self.entries.clone()
    }

    fn size(&self) -> usize {
        self.size
    }

    fn get_filter(&self) -> BloomFilter {
        return self.bloom_filter.to_owned();
    }

    fn find_biggest_key(&self) -> Result<Key, Error> {
        let largest_entry = self.entries.iter().next_back();
        match largest_entry {
            Some(e) => Ok(e.key().to_vec()),
            None => Err(BiggestKeyIndexError),
        }
    }

    fn find_smallest_key(&self) -> Result<Key, Error> {
        let smallest_entry = self.entries.iter().next();
        match smallest_entry {
            Some(e) => Ok(e.key().to_vec()),
            None => Err(LowestKeyIndexError),
        }
    }
}

impl Entry<Key, ValOffset> {
    pub(crate) fn new<EntryKey: K>(
        key: EntryKey,
        val_offset: ValOffset,
        created_at: CreatedAt,
        is_tombstone: IsTombStone,
    ) -> Self {
        Entry {
            key: key.as_ref().to_vec(),
            val_offset,
            created_at,
            is_tombstone,
        }
    }

    pub(crate) fn has_expired(&self, ttl: std::time::Duration) -> bool {
        let current_time = Utc::now();
        let current_timestamp = current_time.timestamp_millis() as u64;
        current_timestamp > (self.created_at.timestamp_millis() as u64 + ttl.as_millis() as u64)
    }
}

impl MemTable<Key> {
    pub fn new() -> Self {
        Self::with_specified_capacity_and_rate(SizeUnit::Bytes, WRITE_BUFFER_SIZE, DEFAULT_FALSE_POSITIVE_RATE)
    }

    pub fn with_specified_capacity_and_rate(size_unit: SizeUnit, capacity: usize, false_positive_rate: f64) -> Self {
        assert!(
            false_positive_rate >= 0.0,
            "False positive rate can not be les than or equal to zero"
        );
        assert!(capacity > 0, "Capacity should be greater than 0");

        let capacity_to_bytes = size_unit.to_bytes(capacity);
        let avg_entry_size = 100;
        let max_no_of_entries = capacity_to_bytes / avg_entry_size as usize;
        let bf = BloomFilter::new(false_positive_rate, max_no_of_entries);
        let entries = SkipMap::new();
        let now = Utc::now();
        Self {
            entries: Arc::new(entries),
            bloom_filter: bf,
            size: 0,
            size_unit: SizeUnit::Bytes,
            capacity: capacity_to_bytes,
            created_at: now,
            false_positive_rate,
            read_only: false,
            most_recent_entry: Entry::new(vec![], 0, Utc::now(), false),
        }
    }

    pub fn insert(&mut self, entry: &Entry<Key, ValOffset>) -> Result<(), Error> {
        let entry_length_byte = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;
        if !self.bloom_filter.contains(&entry.key) {
            self.bloom_filter.set(&entry.key);
            self.entries.insert(
                entry.key.to_owned(),
                SkipMapValue::new(entry.val_offset, entry.created_at, entry.is_tombstone),
            );
            if entry.val_offset > self.most_recent_entry.val_offset {
                self.most_recent_entry = entry.to_owned();
            }
            self.size += entry_length_byte;
            return Ok(());
        }

        self.entries.insert(
            entry.key.to_owned(),
            SkipMapValue::new(entry.val_offset, entry.created_at, entry.is_tombstone),
        );
        if entry.val_offset > self.most_recent_entry.val_offset {
            self.most_recent_entry = entry.to_owned();
        }
        self.size += entry_length_byte;
        Ok(())
    }

    pub fn get<EntryKey: K>(&self, key: EntryKey) -> Option<SkipMapValue<ValOffset>> {
        if self.bloom_filter.contains(&key.as_ref().to_vec()) {
            if let Some(entry) = self.entries.get(key.as_ref()) {
                return Some(entry.value().to_owned()); // returns value offset
            }
        }
        None
    }

    pub fn update(&mut self, entry: &Entry<Key, ValOffset>) -> Result<(), Error> {
        if !self.bloom_filter.contains(&entry.key) {
            return Err(KeyNotFoundInMemTable);
        }
        self.entries.insert(
            entry.key.to_vec(),
            SkipMapValue::new(entry.val_offset, entry.created_at, entry.is_tombstone),
        );
        Ok(())
    }

    pub fn upsert(&mut self, entry: &Entry<Key, usize>) -> Result<(), Error> {
        self.insert(&entry)
    }

    pub fn generate_table_id() -> Vec<u8> {
        let rng = rand::thread_rng();
        let id: String = rng.sample_iter(&Alphanumeric).take(10).map(char::from).collect();
        id.as_bytes().to_vec()
    }

    pub fn delete(&mut self, entry: &Entry<Key, ValOffset>) -> Result<(), Error> {
        if !self.bloom_filter.contains(&entry.key) {
            return Err(KeyNotFoundInMemTable);
        }
        // Insert thumb stone to indicate deletion
        self.entries.insert(
            entry.key.to_vec(),
            SkipMapValue::new(entry.val_offset, Utc::now(), entry.is_tombstone),
        );
        Ok(())
    }

    pub fn is_full(&mut self, key_len: usize) -> bool {
        self.size + key_len + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8 >= self.capacity()
    }

    pub fn is_entry_within_range<CustomKey: AsRef<[u8]>>(
        e: &crossbeam_skiplist::map::Entry<Key, (ValOffset, CreatedAt, IsTombStone)>,
        start: CustomKey,
        end: CustomKey,
    ) -> bool {
        e.key().cmp(&start.as_ref().to_vec()) == Ordering::Greater
            || e.key().cmp(&start.as_ref().to_vec()) == Ordering::Equal
            || e.key().cmp(&end.as_ref().to_vec()) == Ordering::Less
            || e.key().cmp(&end.as_ref().to_vec()) == Ordering::Equal
    }

    pub fn false_positive_rate(&mut self) -> f64 {
        self.false_positive_rate
    }
    pub fn size(&mut self) -> usize {
        self.size
    }

    pub fn get_bloom_filter(&self) -> BloomFilter {
        self.bloom_filter.clone()
    }

    pub fn capacity(&mut self) -> usize {
        self.capacity
    }

    pub fn size_unit(&mut self) -> SizeUnit {
        self.size_unit
    }

    pub fn range() {}

    /// Clears all key-value entries in the MemTable.
    pub fn clear(&mut self) {
        let capacity_to_bytes = self.size_unit.to_bytes(self.capacity);
        let avg_entry_size = 100;
        let max_no_of_entries = capacity_to_bytes / avg_entry_size as usize;

        self.entries.clear();
        self.size = 0;
        self.bloom_filter = BloomFilter::new(self.false_positive_rate, max_no_of_entries);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Mutex, thread};

    #[test]
    fn test_with_specified_capacity_and_rate() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;

        let mem_table = MemTable::with_specified_capacity_and_rate(SizeUnit::Bytes, buffer_size, false_pos_rate);
        assert_eq!(mem_table.entries.len(), 0);
        assert_eq!(mem_table.bloom_filter.num_elements(), 0);
        assert_eq!(mem_table.size, 0);
        assert_eq!(mem_table.size_unit, SizeUnit::Bytes);
        assert_eq!(mem_table.capacity, mem_table.size_unit.to_bytes(buffer_size));
        assert_eq!(mem_table.false_positive_rate, false_pos_rate);
        assert_eq!(mem_table.read_only, false);
    }

    #[test]
    fn test_new() {
        let mem_table = MemTable::new();
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;
        assert_eq!(mem_table.entries.len(), 0);
        assert_eq!(mem_table.bloom_filter.num_elements(), 0);
        assert_eq!(mem_table.size, 0);
        assert_eq!(mem_table.size_unit, SizeUnit::Bytes);
        assert_eq!(mem_table.capacity, mem_table.size_unit.to_bytes(buffer_size));
        assert_eq!(mem_table.false_positive_rate, false_pos_rate);
        assert_eq!(mem_table.read_only, false);
    }

    #[test]
    fn test_insert() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;

        let mut mem_table = MemTable::with_specified_capacity_and_rate(SizeUnit::Bytes, buffer_size, false_pos_rate);
        assert_eq!(mem_table.entries.len(), 0);
        assert_eq!(mem_table.bloom_filter.num_elements(), 0);
        assert_eq!(mem_table.size, 0);
        let key = vec![1, 2, 3, 4];
        let val_offset = 400;
        let is_tombstone = false;
        let created_at = Utc::now();
        let entry = Entry::new(key, val_offset, created_at, is_tombstone);
        let expected_len = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;

        let _ = mem_table.insert(&entry);
        assert_eq!(mem_table.size, expected_len);

        let _ = mem_table.insert(&entry);
        assert_eq!(mem_table.size, expected_len + expected_len);

        let _ = mem_table.insert(&entry);
        assert_eq!(mem_table.size, expected_len + expected_len + expected_len);
    }

    #[test]
    fn test_get() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;
        let mut mem_table = MemTable::with_specified_capacity_and_rate(SizeUnit::Bytes, buffer_size, false_pos_rate);
        assert_eq!(mem_table.size, 0);
        let key = vec![1, 2, 3, 4];
        let val_offset = 400;
        let is_tombstone = false;
        let created_at = Utc::now();
        let entry = Entry::new(key.to_owned(), val_offset, created_at, is_tombstone);
        let expected_len = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;

        let _ = mem_table.insert(&entry);
        assert_eq!(mem_table.size, expected_len);
        // get key
        let res = mem_table.get(&key);
        assert!(res.is_some());
        // get key the was not inserted
        let invalid_key = vec![8, 2, 3, 4];
        let res = mem_table.get(&invalid_key);
        assert!(res.is_none());
    }

    // this tests what happens when multiple keys are written consurrently
    // NOTE: handling thesame keys written at thesame exact time will be handled at the concurrency level(isolation level)
    #[test]
    fn test_concurrent_write() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;
        let mem_table = MemTable::with_specified_capacity_and_rate(SizeUnit::Bytes, buffer_size, false_pos_rate);
        let mem_table = Arc::new(Mutex::new(mem_table));
        let mut handlers = Vec::with_capacity(5 as usize);
        let keys = vec![
            vec![1, 2, 3, 4],
            vec![2, 2, 3, 4],
            vec![3, 2, 3, 4],
            vec![4, 2, 3, 4],
            vec![5, 2, 3, 4],
        ];
        let is_tombstone = false;
        let created_at = Utc::now();
        for i in 0..5 {
            let keys_clone = keys.clone();
            let m = mem_table.clone();
            let handler = thread::spawn(move || {
                let entry = Entry::new(keys_clone[i].to_owned(), i, created_at, is_tombstone);
                m.lock().unwrap().insert(&entry).unwrap();
            });
            handlers.push(handler)
        }

        for handler in handlers {
            handler.join().unwrap();
        }
        assert_eq!(
            mem_table.lock().unwrap().get(&keys[0]).unwrap(),
            SkipMapValue {
                val_offset: 0,
                created_at,
                is_tombstone
            }
        );
        assert_eq!(
            mem_table.lock().unwrap().get(&keys[1]).unwrap(),
            SkipMapValue {
                val_offset: 1,
                created_at,
                is_tombstone
            }
        );
        assert_eq!(
            mem_table.lock().unwrap().get(&keys[2]).unwrap(),
            SkipMapValue {
                val_offset: 2,
                created_at,
                is_tombstone
            }
        );
        assert_eq!(
            mem_table.lock().unwrap().get(&keys[3]).unwrap(),
            SkipMapValue {
                val_offset: 3,
                created_at,
                is_tombstone
            }
        );
        assert_eq!(
            mem_table.lock().unwrap().get(&keys[4]).unwrap(),
            SkipMapValue {
                val_offset: 4,
                created_at,
                is_tombstone
            }
        );
    }

    #[test]
    fn test_update() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;

        let mut mem_table = MemTable::with_specified_capacity_and_rate(SizeUnit::Bytes, buffer_size, false_pos_rate);
        assert_eq!(mem_table.entries.len(), 0);
        assert_eq!(mem_table.bloom_filter.num_elements(), 0);
        assert_eq!(mem_table.size, 0);
        let key = vec![1, 2, 3, 4];
        let val_offset = 400;
        let is_tombstone = false;
        let created_at = Utc::now();
        let mut entry = Entry::new(key, val_offset, created_at, is_tombstone);

        let _ = mem_table.insert(&entry);

        let e = mem_table.get(&entry.key);
        assert!(e.is_some());
        assert_eq!(e.unwrap().val_offset, val_offset);

        entry.val_offset = 300;
        let _ = mem_table.update(&entry);

        let e = mem_table.get(&entry.key);
        assert_eq!(e.unwrap().val_offset, 300);

        entry.is_tombstone = true;
        let _ = mem_table.update(&entry);

        let e = mem_table.get(&entry.key);
        assert_eq!(e.unwrap().is_tombstone, true);

        entry.key = vec![2, 2, 3, 4];
        let e = mem_table.update(&entry);
        assert!(e.is_err());
        // assert_eq!(e.try_into(), Err(KeyNotFoundInMemTable))
    }

    #[test]
    fn test_delete() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;

        let mut mem_table = MemTable::with_specified_capacity_and_rate(SizeUnit::Bytes, buffer_size, false_pos_rate);
        assert_eq!(mem_table.entries.len(), 0);
        assert_eq!(mem_table.bloom_filter.num_elements(), 0);
        assert_eq!(mem_table.size, 0);
        let key = vec![1, 2, 3, 4];
        let val_offset = 400;
        let is_tombstone = false;
        let created_at = Utc::now();
        let mut entry = Entry::new(key, val_offset, created_at, is_tombstone);

        let _ = mem_table.insert(&entry);

        let e = mem_table.get(&entry.key);
        assert!(e.is_some());
        assert_eq!(e.unwrap().val_offset, val_offset);
        entry.is_tombstone = true;
        let _ = mem_table.delete(&entry);

        let e = mem_table.get(&entry.key);
        assert_eq!(e.unwrap().is_tombstone, true);

        entry.key = vec![2, 2, 3, 4];
        let e = mem_table.delete(&entry);
        assert!(e.is_err());
    }

    #[test]
    fn test_generate_table_id() {
        let id1 = MemTable::generate_table_id();
        let id2 = MemTable::generate_table_id();
        let id3 = MemTable::generate_table_id();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_is_entry_within_range() {
        let keys = vec![
            vec![1, 2, 3, 4],
            vec![2, 2, 3, 4],
            vec![3, 2, 3, 4],
            vec![4, 2, 3, 4],
            vec![5, 2, 3, 4],
        ];
        let map = SkipMap::new();
        let is_tombstone = false;
        let created_at = Utc::now();
        let val_offset = 500;
        map.insert(keys[0].to_owned(), (val_offset, created_at, is_tombstone));
        map.insert(keys[1].to_owned(), (val_offset, created_at, is_tombstone));
        map.insert(keys[2].to_owned(), (val_offset, created_at, is_tombstone));
        map.insert(keys[3].to_owned(), (val_offset, created_at, is_tombstone));
        map.insert(keys[4].to_owned(), (val_offset, created_at, is_tombstone));

        let within_range =
            MemTable::is_entry_within_range(&map.get(&keys[0]).unwrap(), keys[0].to_owned(), keys[3].to_owned());
        assert_eq!(within_range, true);

        let start_invalid = vec![10, 20, 30, 40];
        let end_invalid = vec![0, 0, 0, 0];
        let within_range = MemTable::is_entry_within_range(&map.get(&keys[0]).unwrap(), start_invalid, end_invalid);
        assert_eq!(within_range, false);

        let start_valid = &keys[0];
        let end_invalid = vec![0, 0, 0, 0];
        let within_range = MemTable::is_entry_within_range(&map.get(&keys[0]).unwrap(), start_valid, &end_invalid);
        assert_eq!(within_range, true);
    }

    #[test]
    fn test_find_smallest_key() {
        let keys = vec![
            vec![1, 2, 3, 4],
            vec![2, 2, 3, 4],
            vec![3, 2, 3, 4],
            vec![4, 2, 3, 4],
            vec![5, 2, 3, 4],
        ];
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;
        let is_tombstone = false;
        let created_at = Utc::now();
        let mut mem_table = MemTable::with_specified_capacity_and_rate(SizeUnit::Bytes, buffer_size, false_pos_rate);
        for i in 0..5 {
            let entry = Entry::new(keys[i].to_owned(), i, created_at, is_tombstone);
            let _ = mem_table.insert(&entry);
        }

        let smallest = mem_table.find_smallest_key();
        assert!(smallest.is_ok());
        assert_eq!(smallest.unwrap(), keys[0]);
    }

    #[test]
    fn test_find_biggest_key() {
        let keys = vec![
            vec![1, 2, 3, 4],
            vec![2, 2, 3, 4],
            vec![3, 2, 3, 4],
            vec![4, 2, 3, 4],
            vec![5, 2, 3, 4],
        ];
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;
        let is_tombstone = false;
        let created_at = Utc::now();
        let mut mem_table = MemTable::with_specified_capacity_and_rate(SizeUnit::Bytes, buffer_size, false_pos_rate);
        for i in 0..5 {
            let entry = Entry::new(keys[i].to_owned(), i, created_at, is_tombstone);
            let _ = mem_table.insert(&entry);
        }

        let biggest = mem_table.find_biggest_key();
        assert!(biggest.is_ok());
        assert_eq!(biggest.unwrap(), keys[4]);
    }

    #[test]
    fn test_is_full() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;
        let mut mem_table = MemTable::with_specified_capacity_and_rate(SizeUnit::Bytes, buffer_size, false_pos_rate);
        let key = vec![1, 2, 3, 4];
        let is_full = mem_table
            .to_owned()
            .is_full(key.len() + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8 + mem_table.capacity());
        assert_eq!(is_full, true);
    }
}
