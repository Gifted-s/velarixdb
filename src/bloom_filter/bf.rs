use crate::sstable::SSTFile;
use bit_vec::BitVec;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
};

#[derive(Debug)]
pub struct BloomFilter {
    pub sstable_path: Option<SSTFile>,
    pub no_of_hash_func: usize,
    pub no_of_elements: AtomicU32,
    pub bit_vec: Arc<Mutex<BitVec>>,
}

impl BloomFilter {
    pub fn new(false_positive_rate: f64, no_of_elements: usize) -> Self {
        assert!(
            false_positive_rate >= 0.0,
            "False positive rate can not be les than or equal to zero"
        );
        assert!(
            no_of_elements > 0,
            "No of elements should be greater than 0"
        );

        let no_of_bits = Self::calculate_no_of_bits(no_of_elements, false_positive_rate);
        let no_of_hash_func =
            Self::calculate_no_of_hash_function(no_of_bits, no_of_elements as u32) as usize;
        let bv = BitVec::from_elem(no_of_bits as usize, false);

        Self {
            no_of_elements: AtomicU32::new(0),
            no_of_hash_func,
            sstable_path: None,
            bit_vec: Arc::new(Mutex::new(bv)),
        }
    }
    pub(crate) fn set<T: Hash>(&mut self, key: &T) {
        let mut bits = self.bit_vec.lock().expect("Failed to lock file");
        for i in 0..self.no_of_hash_func {
            let hash = self.calculate_hash(key, i);
            let index = (hash % bits.len() as u64) as usize;
            bits.set(index, true)
        }
        self.no_of_elements.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn contains<T: Hash>(&self, key: &T) -> bool {
        let bits = self.bit_vec.lock().expect("Failed to lock file");
        for i in 0..self.no_of_hash_func {
            let hash = self.calculate_hash(key, i);
            let index = (hash % bits.len() as u64) as usize;
            if !bits[index] {
                return false;
            }
        }
        true
    }

    pub fn set_sstable_path(&mut self, sstable_path: SSTFile) {
        self.sstable_path = Some(sstable_path);
    }

    pub fn bloom_filters_within_key_range<'a>(
        bloom_filters: &'a Vec<BloomFilter>,
        paths: Vec<&'a PathBuf>,
    ) -> Vec<&'a BloomFilter> {
        let mut filtered_bfs = Vec::new();
        paths.into_iter().for_each(|p| {
            bloom_filters.iter().for_each(|b| {
                if b.get_sstable_path().data_file_path.as_path() == p.as_path() {
                    filtered_bfs.push(b)
                }
            })
        });
        filtered_bfs
    }

    pub fn sstables_within_key_range<T: Hash>(
        bloom_filters: Vec<&BloomFilter>,
        key: &T,
    ) -> Option<Vec<SSTFile>> {
        let mut sstables: Vec<SSTFile> = Vec::new();
        for bloom_filter in bloom_filters {
            if bloom_filter.contains(key) {
                if let Some(path) = &bloom_filter.sstable_path {
                    sstables.push(path.to_owned());
                }
            }
        }
        if sstables.is_empty() {
            return None;
        }
        Some(sstables)
    }

    pub fn clear(&mut self) -> Self {
        let mut bits = self.bit_vec.lock().expect("Failed to lock file");
        for i in 0..bits.len() {
            bits.set(i, false);
        }
        let no_of_hash_func = self.no_of_hash_func;
        let bit_vec = BitVec::from_elem(bits.len(), false);
        Self {
            sstable_path: None,
            no_of_hash_func,
            no_of_elements: AtomicU32::new(0),
            bit_vec: Arc::new(Mutex::new(bit_vec)),
        }
    }

    /// Returns the current number of elements inserted into the Bloom filter.
    pub fn num_elements(&self) -> usize {
        // Retrieve the element count atomically.
        self.no_of_elements.load(Ordering::Relaxed) as usize
    }

    /// Returns the current number of elements inserted into the Bloom filter.
    pub fn num_bits(&self) -> usize {
        // Retrieve the element count atomically.
        self.bit_vec.lock().unwrap().len()
    }

    /// Returns the current number of hash functions.
    pub fn num_of_hash_functions(&self) -> usize {
        // Retrieve the element count atomically.
        self.no_of_hash_func
    }

    /// Get SSTable path
    pub fn get_sstable_path(&self) -> &SSTFile {
        // Retrieve the element count atomically.
        self.sstable_path.as_ref().unwrap()
    }

    fn calculate_hash<T: Hash>(&self, key: &T, seed: usize) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.write_u64(seed as u64);
        hasher.finish()
    }

    fn calculate_no_of_bits(no_of_elements: usize, false_positive_rate: f64) -> u32 {
        let no_bits =
            -((no_of_elements as f64 * false_positive_rate.ln()) / ((2_f64.ln()).powi(2))).ceil();
        no_bits as u32
    }

    fn calculate_no_of_hash_function(no_of_bits: u32, no_of_elements: u32) -> u32 {
        let no_hash_func = (no_of_bits as f64 / no_of_elements as f64) * (2_f64.ln()).ceil();
        no_hash_func as u32
    }
}

impl Clone for BloomFilter {
    fn clone(&self) -> Self {
        // Implement custom logic here if needed
        BloomFilter {
            sstable_path: self.sstable_path.clone(),
            no_of_hash_func: self.no_of_hash_func,
            no_of_elements: AtomicU32::load(&self.no_of_elements, Ordering::Relaxed).into(),
            bit_vec: self.bit_vec.clone(),
        }
    }
}

#[cfg(test)]

mod tests {

    use super::*;

    #[test]
    fn test_set_and_contain() {
        let false_positive_rate = 0.01;
        let no_of_elements: usize = 10;
        let mut bloom_filter = BloomFilter::new(false_positive_rate, no_of_elements);

        let no_bits =
            -((no_of_elements as f64 * false_positive_rate.ln()) / ((2_f64.ln()).powi(2))).ceil();

        let expected_no_hash_func =
            ((no_bits / no_of_elements as f64) * (2_f64.ln()).ceil()) as usize;

        assert_eq!(bloom_filter.num_elements(), 0);
        assert_eq!(bloom_filter.no_of_hash_func, expected_no_hash_func);
        assert_eq!(bloom_filter.bit_vec.lock().unwrap().len(), no_bits as usize);
        let k = &vec![1, 2, 3, 4];
        bloom_filter.set(k);
        assert_eq!(bloom_filter.num_elements(), 1);
        assert!(bloom_filter.contains(k));
    }

    #[test]
    fn test_number_of_elements() {
        let false_positive_rate = 0.01;
        let no_of_elements: usize = 10;
        let mut bloom_filter = BloomFilter::new(false_positive_rate, no_of_elements);

        for i in 0..10 {
            bloom_filter.set(&i)
        }

        assert_eq!(bloom_filter.num_elements(), 10)
    }

    #[test]
    fn test_false_positives_high_rate() {
        // Number of elements.
        let num_elements = 10000;

        // False Positive Rate.
        let false_positive_rate = 0.1;

        // Create a Bloom Filter.
        let mut bloom = BloomFilter::new(false_positive_rate, num_elements);

        // Insert elements into the Bloom Filter.
        for i in 0..num_elements {
            bloom.set(&i);
        }

        let mut false_positives = 0;
        let num_tested_elements = 2000;

        // Test all non-inserted elements for containment.
        // Count the number of false positives.
        for i in num_elements..num_elements + num_tested_elements {
            if bloom.contains(&i) {
                false_positives += 1;
            }
        }

        // Calculate the observed false positive rate.
        let observed_false_positive_rate = false_positives as f64 / num_tested_elements as f64;

        // Allow for a small margin (10%) of error due to the probabilistic nature of Bloom filters.
        // Maximum Allowed False Positive Rate = False Positive Rate + (False Positive Rate * Tolerance)
        let max_allowed_false_positive_rate = false_positive_rate + (false_positive_rate * 0.1);

        assert!(
            observed_false_positive_rate <= max_allowed_false_positive_rate,
            "Observed false positive rate ({}) is greater than the maximum allowed ({})",
            observed_false_positive_rate,
            max_allowed_false_positive_rate
        );
    }

    #[test]
    fn test_false_positives_medium_rate() {
        // Number of elements.
        let num_elements = 10000;

        // False Positive Rate.
        let false_positive_rate = 0.0001;

        // Create a Bloom Filter.
        let mut bloom = BloomFilter::new(false_positive_rate, num_elements);

        // Insert elements into the Bloom Filter.
        for i in 0..num_elements {
            bloom.set(&i);
        }

        let mut false_positives = 0;
        let num_tested_elements = 2000;

        // Test all non-inserted elements for containment.
        // Count the number of false positives.
        for i in num_elements..num_elements + num_tested_elements {
            if bloom.contains(&i) {
                false_positives += 1;
            }
        }

        // Calculate the observed false positive rate.
        let observed_false_positive_rate = false_positives as f64 / num_tested_elements as f64;

        // Allow for a small margin (10%) of error due to the probabilistic nature of Bloom filters.
        // Maximum Allowed False Positive Rate = False Positive Rate + (False Positive Rate * Tolerance)
        let max_allowed_false_positive_rate = false_positive_rate + (false_positive_rate * 0.1);

        assert!(
            observed_false_positive_rate <= max_allowed_false_positive_rate,
            "Observed false positive rate ({}) is greater than the maximum allowed ({})",
            observed_false_positive_rate,
            max_allowed_false_positive_rate
        );
    }

    #[test]
    fn test_false_positives_low_rate() {
        // Number of elements.
        let num_elements = 10000;

        // False Positive Rate.
        let false_positive_rate = 0.0000001;

        // Create a Bloom Filter.
        let mut bloom = BloomFilter::new(false_positive_rate, num_elements);

        // Insert elements into the Bloom Filter.
        for i in 0..num_elements {
            bloom.set(&i);
        }

        let mut false_positives = 0;
        let num_tested_elements = 2000;

        // Test all non-inserted elements for containment.
        // Count the number of false positives.
        for i in num_elements..num_elements + num_tested_elements {
            if bloom.contains(&i) {
                false_positives += 1;
            }
        }

        // Calculate the observed false positive rate.
        let observed_false_positive_rate = false_positives as f64 / num_tested_elements as f64;

        // Allow for a small margin (10%) of error due to the probabilistic nature of Bloom filters.
        // Maximum Allowed False Positive Rate = False Positive Rate + (False Positive Rate * Tolerance)
        let max_allowed_false_positive_rate = false_positive_rate + (false_positive_rate * 0.1);

        assert!(
            observed_false_positive_rate <= max_allowed_false_positive_rate,
            "Observed false positive rate ({}) is greater than the maximum allowed ({})",
            observed_false_positive_rate,
            max_allowed_false_positive_rate
        );
    }
}
