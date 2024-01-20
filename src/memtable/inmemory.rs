use crate::bloom_filter::BloomFilter;
//use crate::memtable::val_option::ValueOption;
use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;

use std::cmp::Ordering;
use std::io;
use std::{
    hash::Hash,
    sync::{Arc, Mutex},
    thread,
};
//pub(crate) static DEFAULT_MEMTABLE_CAPACITY: usize = SizeUnit::Gigabytes.to_bytes(1);

pub(crate) static THUMB_STONE: usize = 0;

pub(crate) static DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.0001;

#[derive(Clone)]
enum SizeUnit {
    Bytes,
    Kilobytes,
    Megabytes,
    Gigabytes,
}

#[derive(PartialOrd, PartialEq)]
struct Entry<K: Hash + PartialOrd, V> {
    key: K,
    val_offset: V,
}

struct InMemoryTable<K: Hash + PartialOrd> {
    index: Arc<SkipMap<K, usize>>,
    bloom_filter: BloomFilter,
    size: usize,
    size_unit: SizeUnit,
    capacity: usize,
    created_at: DateTime<Utc>,
}

impl InMemoryTable<Vec<u8>> {
    pub fn new(size_unit: SizeUnit, capacity: usize, false_positive_rate: f64) -> Self {
        assert!(
            false_positive_rate >= 0.0,
            "False positive rate can not be les than or equal to zero"
        );
        assert!(capacity > 0, "Capacity should be greater than 0");

        let capacity_to_bytes = size_unit.to_bytes(capacity);
        let avg_entry_size = 100;
        let max_no_of_entries = capacity_to_bytes / avg_entry_size as usize;
        let bf = BloomFilter::new(false_positive_rate, max_no_of_entries);
        let index = SkipMap::new();
        let now: DateTime<Utc> = Utc::now();
        Self {
            index: Arc::new(index),
            bloom_filter: bf,
            size: 0,
            size_unit: SizeUnit::Bytes,
            capacity: capacity_to_bytes,
            created_at: now,
        }
    }

    pub fn insert(&mut self, key: &Vec<u8>, val_offset: u32) -> io::Result<()> {
        if !self.bloom_filter.contains(key) {
            self.bloom_filter.set(key);
            self.index
                .insert(key.to_vec(), val_offset.try_into().unwrap());
            // it takes 4 bytes to store a 32 bit integer since 8 bits makes 1 byte
            let entry_length_byte = key.len() + 4;
            self.size += entry_length_byte;
            return Ok(());
        }
        // If the key already exist in the bloom filter then just insert into the entry alone
        self.index
            .insert(key.to_vec(), val_offset.try_into().unwrap());
        let entry_length_byte = key.len() + 4;
        self.size += entry_length_byte;
        return Ok(());
    }

    pub fn get(&mut self, key: &Vec<u8>) -> io::Result<Option<usize>> {
        if self.bloom_filter.contains(key) {
            let value = *self.index.get(key).unwrap().value();
            return Ok(Some(value));
        }

        Err(io::Error::new(io::ErrorKind::NotFound, "Key not found"))
    }

    pub fn update(&mut self, key: &Vec<u8>, val_offset: u32) -> io::Result<()> {
        if !self.bloom_filter.contains(key) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Key does not exist",
            ));
        }
        // If the key already exist in the bloom filter then just insert into the entry alone
        self.index
            .insert(key.to_vec(), val_offset.try_into().unwrap());
        return Ok(());
    }

    pub fn upsert(&mut self, key: &Vec<u8>, val_offset: u32) -> io::Result<()> {
        self.insert(key, val_offset)
    }

    pub fn delete(&mut self, key: &Vec<u8>) -> io::Result<()> {
        if !self.bloom_filter.contains(key) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Key does not exist",
            ));
        }
        // Insert thumb stone to indicate deletion
        self.index.insert(key.to_vec(), THUMB_STONE);
        return Ok(());
    }

    pub fn range() {}
}

impl SizeUnit {
    pub fn to_bytes(&self, value: usize) -> usize {
        match self {
            SizeUnit::Bytes => value,
            SizeUnit::Kilobytes => value * 1024,
            SizeUnit::Megabytes => value * 1024 * 1024,
            SizeUnit::Gigabytes => value * 1024 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Error;

    use super::*;

    #[test]
    fn test_new() {
        let mem_table = InMemoryTable::new(SizeUnit::Kilobytes, 4, 0.01);
        assert_eq!(mem_table.capacity, 4 * 1024);
        assert_eq!(mem_table.size, 0);
    }

    #[test]
    fn test_insert() {
        let mut mem_table = InMemoryTable::new(SizeUnit::Kilobytes, 4, 0.01);
        assert_eq!(mem_table.capacity, 4 * 1024);
        assert_eq!(mem_table.size, 0);
        let k1 = &vec![1, 2, 3, 4];
        let k2 = &vec![5, 6, 7, 8];
        let k3 = &vec![10, 11, 12, 13];

        let _ = mem_table.insert(k1, 10);
        assert_eq!(mem_table.size, k1.len() + 4);

        let prev_size = mem_table.size;
        let _ = mem_table.insert(k2, 10);
        assert_eq!(mem_table.size, prev_size + k2.len() + 4);

        let prev_size = mem_table.size;
        let _ = mem_table.insert(k3, 10);
        assert_eq!(mem_table.size, prev_size + k3.len() + 4);
    }

    // this tests what happens when multiple keys are written consurrently
    // NOTE: handling thesame keys written at thesame exact time will be handled at the concurrency level(isolation level)
    #[test]
    fn test_concurrent_write() {
        let mem_table = Arc::new(Mutex::new(InMemoryTable::new(SizeUnit::Kilobytes, 4, 0.01)));
        let mut handlers = Vec::with_capacity(5 as usize);

        for i in 0..5 {
            let m = mem_table.clone();
            let handler = thread::spawn(move || {
                m.lock().unwrap().insert(&vec![i], i as u32).unwrap();
            });
            handlers.push(handler)
        }

        for handler in handlers {
            handler.join().unwrap();
        }
        assert_eq!(mem_table.lock().unwrap().get(&vec![0]).unwrap().unwrap(), 0);
        assert_eq!(mem_table.lock().unwrap().get(&vec![1]).unwrap().unwrap(), 1);
        assert_eq!(mem_table.lock().unwrap().get(&vec![2]).unwrap().unwrap(), 2);
        assert_eq!(mem_table.lock().unwrap().get(&vec![3]).unwrap().unwrap(), 3);
        assert_eq!(mem_table.lock().unwrap().get(&vec![4]).unwrap().unwrap(), 4);
    }

    //test get
    #[test]
    fn test_get() {
        let mut mem_table = InMemoryTable::new(SizeUnit::Kilobytes, 4, 0.01);
        let k1 = &vec![1, 2, 3, 4];
        let k2 = &vec![5, 6, 7, 8];
        let k3 = &vec![10, 11, 12, 13];

        let _ = mem_table.insert(k1, 10);
        let _ = mem_table.insert(k2, 11);
        let _ = mem_table.insert(k3, 12);

        assert_eq!(*mem_table.index.get(k1).unwrap().value(), 10);
        assert_eq!(*mem_table.index.get(k2).unwrap().value(), 11);
        assert_eq!(*mem_table.index.get(k3).unwrap().value(), 12);
    }
    // test latest will be returned
    #[test]
    fn test_return_latest_value() {
        let mut mem_table = InMemoryTable::new(SizeUnit::Kilobytes, 4, 0.01);
        let k = &vec![1, 2, 3, 4];

        let _ = mem_table.insert(k, 10);
        let _ = mem_table.insert(k, 11);
        let _ = mem_table.insert(k, 12);

        //expect latest value to be returned
        assert_eq!(mem_table.get(k).unwrap().unwrap(), 12);
    }

    //test update
    #[test]
    fn test_update() {
        let mut mem_table = InMemoryTable::new(SizeUnit::Kilobytes, 4, 0.01);
        let k = &vec![1, 2, 3, 4];

        let _ = mem_table.insert(k, 10);
        let _ = mem_table.update(k, 11);
        //expect latest value to be returned
        assert_eq!(mem_table.get(k).unwrap().unwrap(), 11);

        let unknown_key = &vec![0, 0, 0, 0];
        assert!(mem_table.update(unknown_key, 10).is_err());
    }

    #[test]
    fn test_upsert() {
        let mut mem_table = InMemoryTable::new(SizeUnit::Kilobytes, 4, 0.01);
        let k = &vec![1, 2, 3, 4];

        let _ = mem_table.insert(k, 10);
        let _ = mem_table.upsert(k, 11);
        //expect latest value to be returned
        assert_eq!(mem_table.get(k).unwrap().unwrap(), 11);

        let new_key = &vec![5, 6, 7, 8];
        mem_table.upsert(new_key, 14).unwrap();
        //expect new key to be inserted if key does not already exist
        assert_eq!(mem_table.get(new_key).unwrap().unwrap(), 14);
    }

    #[test]
    fn test_delete() {
        let mut mem_table = InMemoryTable::new(SizeUnit::Kilobytes, 4, 0.01);
        let k = &vec![1, 2, 3, 4];

        let _ = mem_table.insert(k, 10);
        //expect latest value to be returned
        assert_eq!(mem_table.get(k).unwrap().unwrap(), 10);
        let _ = mem_table.delete(k);
        assert_eq!(mem_table.get(k).unwrap().unwrap(), THUMB_STONE);
    }
}
