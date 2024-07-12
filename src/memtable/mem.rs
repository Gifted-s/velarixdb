//! # Memtable
//!
//! Memtable buffers write in the RAM before it's flushed to the disk once the size exceeds `write_buffer_size`.
//! Entries are stored in a SkipMap so they can be retrieved effectively.
//! Before a memtable is finally flushed to the disk, it is made read-only and added to the read-only memtable vector.
//! Once the read-only memtable vector exceeds the `max_buffer_write_number` all memtable in the vector is flushed to to the disk concurrently

use crate::bucket::InsertableToBucket;
use crate::consts::{SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8};
use crate::db::SizeUnit;
use crate::err::Error;
use crate::filter::BloomFilter;
use crate::types::{CreatedAt, IsTombStone, Key, SkipMapEntries, ValOffset, Value};
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::cmp::Ordering;
use std::fmt::Debug;
use Error::*;

use std::{hash::Hash, sync::Arc};

pub trait K: AsRef<[u8]> + Hash + Ord + Send + Sync + Clone + Debug {}

impl<T> K for T where T: AsRef<[u8]> + Hash + Ord + Send + Sync + Clone + Debug {}

/// Each entry in `Memtable`
#[derive(PartialOrd, PartialEq, Copy, Clone, Debug)]
pub struct Entry<Key: K, V: Ord> {
    pub key: Key,
    pub val_offset: V,
    pub created_at: CreatedAt,
    pub is_tombstone: bool,
}

/// Entry returned to user upon  retreival
#[derive(Debug)]
pub struct UserEntry {
    pub val: Value,
    pub created_at: CreatedAt,
}

impl UserEntry {
    /// Creates new `UserEntry`
    pub fn new(val: Value, created_at: CreatedAt) -> Self {
        Self { val, created_at }
    }
}

/// Value in SkipMap
#[derive(Clone, Debug, PartialEq)]
pub struct SkipMapValue<V: Ord> {
    pub val_offset: V,
    pub created_at: CreatedAt,
    pub is_tombstone: IsTombStone,
}

impl<V: Ord> SkipMapValue<V> {
    /// Creates new `SkipMapValue`
    pub(crate) fn new(val_offset: V, created_at: CreatedAt, is_tombstone: IsTombStone) -> Self {
        SkipMapValue {
            val_offset,
            created_at,
            is_tombstone,
        }
    }
}

/// Stores entries in RAM before it's
/// flushed to disk
#[derive(Clone, Debug)]
pub struct MemTable<Key: K> {
    /// Lock-free skipmap from crossbeam
    pub entries: SkipMapEntries<Key>,

    /// Filter to quickly search for key
    pub bloom_filter: BloomFilter,

    /// Size of memtable in `size_unit`
    pub size: usize,

    /// Date created
    pub created_at: CreatedAt,

    /// Signifies if we can continue to write
    /// to memtable
    pub read_only: bool,

    /// Most recent entry inserted to memtable
    pub most_recent_entry: Entry<Key, ValOffset>,

    /// Memtable configuration
    pub config: Config,
}

#[derive(Clone, Debug)]
/// Configuration for Memtable
pub struct Config {
    /// Acceptable false positive rate
    pub false_pos_rate: f64,

    /// Capacity to be reached before flush
    pub capacity: usize,

    /// Unit to represent size
    pub size_unit: SizeUnit,
}
impl Config {
    /// Creates new `Config`
    fn new(size_unit: SizeUnit, capacity: usize, false_pos_rate: f64) -> Self {
        Self {
            size_unit,
            capacity,
            false_pos_rate,
        }
    }
}

#[allow(dead_code)]
pub enum ValueOption {
    /// TODO: Value will be cached in memory if the size is small, this will reduce
    /// number of Disk IO
    Raw(Value),

    /// Value offset gotten from value position in value log
    Offset(ValOffset),

    /// Represents deleted entry
    TombStone(IsTombStone),
}

impl Entry<Key, ValOffset> {
    /// Creates new `Entry`
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

/// Allows `MemTable` to be insertable
impl InsertableToBucket for MemTable<Key> {
    fn get_entries(&self) -> SkipMapEntries<Key> {
        self.entries.clone()
    }

    fn size(&self) -> usize {
        self.size
    }

    fn get_filter(&self) -> BloomFilter {
        self.bloom_filter.to_owned()
    }
}

impl MemTable<Key> {
    /// Created new `MemTable`
    pub fn new(capacity: usize, false_positive_rate: f64) -> Self {
        Self::with_specified_capacity_and_rate(SizeUnit::Bytes, capacity, false_positive_rate)
    }

    pub fn with_specified_capacity_and_rate(size_unit: SizeUnit, capacity: usize, false_positive_rate: f64) -> Self {
        assert!(
            false_positive_rate >= 0.0,
            "False positive rate can not be les than or equal to zero"
        );
        assert!(capacity > 0, "Capacity should be greater than 0");

        let capacity_to_bytes = size_unit.as_bytes(capacity);
        let avg_entry_size = 100;
        let max_no_of_entries = capacity_to_bytes / avg_entry_size as usize;
        let bf = BloomFilter::new(false_positive_rate, max_no_of_entries);
        let entries = SkipMap::new();
        let now = Utc::now();
        let config = Config::new(size_unit, capacity, false_positive_rate);
        Self {
            entries: Arc::new(entries),
            bloom_filter: bf,
            size: 0,
            config,
            created_at: now,
            read_only: false,
            most_recent_entry: Entry::new(vec![], 0, Utc::now(), false),
        }
    }

    /// Inserts an entry to the `MemTable`
    pub fn insert(&mut self, entry: &Entry<Key, ValOffset>) {
        let entry_length_byte = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;
        if !self.bloom_filter.contains(&entry.key) {
            self.bloom_filter.set(&entry.key);
            self.entries.insert(
                entry.key.to_owned(),
                SkipMapValue::new(entry.val_offset, entry.created_at, entry.is_tombstone),
            );
            if entry.val_offset > self.most_recent_entry.val_offset {
                entry.clone_into(&mut self.most_recent_entry)
            }
            self.size += entry_length_byte;
            return;
        }

        self.entries.insert(
            entry.key.to_owned(),
            SkipMapValue::new(entry.val_offset, entry.created_at, entry.is_tombstone),
        );
        if entry.val_offset > self.most_recent_entry.val_offset {
            entry.clone_into(&mut self.most_recent_entry);
        }
        self.size += entry_length_byte;
    }
    /// Returns value for an entry or `None`
    pub fn get<EntryKey: K>(&self, key: EntryKey) -> Option<SkipMapValue<ValOffset>> {
        if self.bloom_filter.contains(&key.as_ref().to_vec()) {
            if let Some(entry) = self.entries.get(key.as_ref()) {
                return Some(entry.value().to_owned()); // returns value offset
            }
        }
        None
    }

    /// Updates an entry in `entries` map
    ///
    /// # Error
    ///
    /// Returns error if key was not found
    ///
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

    /// Returns most recent entry value offset
    pub fn get_most_recent_offset(&self) -> usize {
        self.most_recent_entry.val_offset
    }

    /// Used to generate id for read-only `MemTable`
    pub fn generate_table_id() -> Vec<u8> {
        let rng = rand::thread_rng();
        let id: String = rng.sample_iter(&Alphanumeric).take(5).map(char::from).collect();
        id.as_bytes().to_vec()
    }

    /// Inserts an entry with tombstone to `entries` map
    ///
    /// # Errors
    ///
    /// Returns error if key was not found
    pub fn delete(&mut self, entry: &Entry<Key, ValOffset>) -> Result<(), Error> {
        if !self.bloom_filter.contains(&entry.key) {
            return Err(KeyNotFoundInMemTable);
        }
        self.entries.insert(
            entry.key.to_vec(),
            SkipMapValue::new(entry.val_offset, Utc::now(), entry.is_tombstone),
        );
        Ok(())
    }
    /// Returns `true` if `Memtable` is full
    pub fn is_full(&mut self, key_len: usize) -> bool {
        self.size + key_len + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8 >= self.capacity()
    }

    /// Seals an Memtable
    pub fn mark_readonly(&mut self) {
        self.read_only = true;
    }

    /// Checks if a key range exists in the memtable
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

    /// Sets false positive rate for `MemTable`
    pub fn false_positive_rate(&self) -> f64 {
        self.config.false_pos_rate
    }
    /// Returns `MemTable` size
    pub fn size(&mut self) -> usize {
        self.size
    }

    /// Returns `Memtable` bloom filter
    pub fn get_bloom_filter(&self) -> BloomFilter {
        self.bloom_filter.clone()
    }

    /// Returns the capacity of the `MemTable`
    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    /// Returns `size` of `MemTable`
    pub fn size_unit(&self) -> SizeUnit {
        self.config.size_unit
    }

    pub fn range() {}

    /// Clears all key-value entries in the MemTable.
    pub fn clear(&mut self) {
        let capacity_to_bytes = self.config.size_unit.as_bytes(self.config.capacity);
        let avg_entry_size = 100;
        let max_no_of_entries = capacity_to_bytes / avg_entry_size as usize;

        self.entries.clear();
        self.size = 0;
        self.bloom_filter = BloomFilter::new(self.config.false_pos_rate, max_no_of_entries);
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

        let memtable = MemTable::with_specified_capacity_and_rate(SizeUnit::Bytes, buffer_size, false_pos_rate);
        assert_eq!(memtable.entries.len(), 0);
        assert_eq!(memtable.bloom_filter.num_elements(), 0);
        assert_eq!(memtable.size, 0);
        assert_eq!(memtable.config.size_unit, SizeUnit::Bytes);
        assert_eq!(
            memtable.config.capacity,
            memtable.config.size_unit.as_bytes(buffer_size)
        );
        assert_eq!(memtable.config.false_pos_rate, false_pos_rate);
        assert!(!memtable.read_only);
    }

    #[test]
    fn test_new() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-10;
        let memtable = MemTable::new(buffer_size, false_pos_rate);
        assert_eq!(memtable.entries.len(), 0);
        assert_eq!(memtable.bloom_filter.num_elements(), 0);
        assert_eq!(memtable.size, 0);
        assert_eq!(memtable.config.size_unit, SizeUnit::Bytes);
        assert_eq!(
            memtable.config.capacity,
            memtable.config.size_unit.as_bytes(buffer_size)
        );
        assert_eq!(memtable.config.false_pos_rate, false_pos_rate);
        assert!(!memtable.read_only);
    }

    #[test]
    fn test_insert() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;

        let mut memtable = MemTable::new(buffer_size, false_pos_rate);
        assert_eq!(memtable.entries.len(), 0);
        assert_eq!(memtable.bloom_filter.num_elements(), 0);
        assert_eq!(memtable.size, 0);
        let key = vec![1, 2, 3, 4];
        let val_offset = 400;
        let is_tombstone = false;
        let created_at = Utc::now();
        let entry = Entry::new(key, val_offset, created_at, is_tombstone);
        let expected_len = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;

        memtable.insert(&entry);
       
        assert_eq!(memtable.size, expected_len);

        memtable.insert(&entry);
        assert_eq!(memtable.size, expected_len + expected_len);

        memtable.insert(&entry);
        assert_eq!(memtable.size, expected_len + expected_len + expected_len);
    }

    #[test]
    fn test_get() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;
        let mut memtable = MemTable::new(buffer_size, false_pos_rate);
        assert_eq!(memtable.size, 0);
        let key = vec![1, 2, 3, 4];
        let val_offset = 400;
        let is_tombstone = false;
        let created_at = Utc::now();
        let entry = Entry::new(key.to_owned(), val_offset, created_at, is_tombstone);
        let expected_len = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;

        memtable.insert(&entry);
        assert_eq!(memtable.size, expected_len);
        // get key
        let res = memtable.get(&key);
        assert!(res.is_some());
        // get key the was not inserted
        let invalid_key = vec![8, 2, 3, 4];
        let res = memtable.get(invalid_key);
        assert!(res.is_none());
    }

    // this tests what happens when multiple keys are written consurrently
    // NOTE: handling thesame keys written at thesame exact time will be handled at the concurrency level(isolation level)
    #[test]
    fn test_concurrent_write() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;
        let memtable = MemTable::new(buffer_size, false_pos_rate);
        let memtable = Arc::new(Mutex::new(memtable));
        let mut handlers = Vec::with_capacity(5_usize);
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
            let m = memtable.clone();
            let handler = thread::spawn(move || {
                let entry = Entry::new(keys_clone[i].to_owned(), i, created_at, is_tombstone);
                m.lock().unwrap().insert(&entry);
            });
            handlers.push(handler)
        }

        for handler in handlers {
            handler.join().unwrap();
        }
        assert_eq!(
            memtable.lock().unwrap().get(&keys[0]).unwrap(),
            SkipMapValue {
                val_offset: 0,
                created_at,
                is_tombstone
            }
        );
        assert_eq!(
            memtable.lock().unwrap().get(&keys[1]).unwrap(),
            SkipMapValue {
                val_offset: 1,
                created_at,
                is_tombstone
            }
        );
        assert_eq!(
            memtable.lock().unwrap().get(&keys[2]).unwrap(),
            SkipMapValue {
                val_offset: 2,
                created_at,
                is_tombstone
            }
        );
        assert_eq!(
            memtable.lock().unwrap().get(&keys[3]).unwrap(),
            SkipMapValue {
                val_offset: 3,
                created_at,
                is_tombstone
            }
        );
        assert_eq!(
            memtable.lock().unwrap().get(&keys[4]).unwrap(),
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

        let mut memtable = MemTable::new(buffer_size, false_pos_rate);
        assert_eq!(memtable.entries.len(), 0);
        assert_eq!(memtable.bloom_filter.num_elements(), 0);
        assert_eq!(memtable.size, 0);
        let key = vec![1, 2, 3, 4];
        let val_offset = 400;
        let is_tombstone = false;
        let created_at = Utc::now();
        let mut entry = Entry::new(key, val_offset, created_at, is_tombstone);

        memtable.insert(&entry);

        let e = memtable.get(&entry.key);
        assert!(e.is_some());
        assert_eq!(e.unwrap().val_offset, val_offset);

        entry.val_offset = 300;
        let _ = memtable.update(&entry);

        let e = memtable.get(&entry.key);
        assert_eq!(e.unwrap().val_offset, 300);

        entry.is_tombstone = true;
        let _ = memtable.update(&entry);

        let e = memtable.get(&entry.key);
        assert!(e.unwrap().is_tombstone);

        entry.key = vec![2, 2, 3, 4];
        let e = memtable.update(&entry);
        assert!(e.is_err());
        // assert_eq!(e.try_into(), Err(KeyNotFoundInMemTable))
    }

    #[test]
    fn test_delete() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;

        let mut memtable = MemTable::new(buffer_size, false_pos_rate);
        assert_eq!(memtable.entries.len(), 0);
        assert_eq!(memtable.bloom_filter.num_elements(), 0);
        assert_eq!(memtable.size, 0);
        let key = vec![1, 2, 3, 4];
        let val_offset = 400;
        let is_tombstone = false;
        let created_at = Utc::now();
        let mut entry = Entry::new(key, val_offset, created_at, is_tombstone);

        memtable.insert(&entry);

        let e = memtable.get(&entry.key);
        assert!(e.is_some());
        assert_eq!(e.unwrap().val_offset, val_offset);
        entry.is_tombstone = true;
        let _ = memtable.delete(&entry);

        let e = memtable.get(&entry.key);
        assert!(e.unwrap().is_tombstone);

        entry.key = vec![2, 2, 3, 4];
        let e = memtable.delete(&entry);
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
        let keys = [
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
        assert!(within_range);

        let start_invalid = vec![10, 20, 30, 40];
        let end_invalid = vec![0, 0, 0, 0];
        let within_range = MemTable::is_entry_within_range(&map.get(&keys[0]).unwrap(), start_invalid, end_invalid);
        assert!(!within_range);

        let start_valid = &keys[0];
        let end_invalid = vec![0, 0, 0, 0];
        let within_range = MemTable::is_entry_within_range(&map.get(&keys[0]).unwrap(), start_valid, &end_invalid);
        assert!(within_range);
    }

    #[test]
    fn test_is_full() {
        let buffer_size = 51200;
        let false_pos_rate = 1e-300;
        let memtable = MemTable::new(buffer_size, false_pos_rate);
        let key = [1, 2, 3, 4];
        let is_full = memtable
            .to_owned()
            .is_full(key.len() + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8 + memtable.capacity());
        assert!(is_full);
    }
}
