use crate::filter::bf::Error::FilterFilePathNotProvided;
use crate::types::ByteSerializedEntry;
use crate::types::Key;
use crate::types::SkipMapEntries;
use crate::{
    consts::{FILTER_FILE_NAME, SIZE_OF_U32, SIZE_OF_U64},
    err::Error,
    fs::{FileAsync, FilterFileNode, FilterFs},
    util,
};
use bit_vec::BitVec;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
};
/// Alias for false positive rate
pub type FalsePositive = f64;

/// Alias for hash functions used by filter
pub type NoHashFunc = u32;

/// Alias for number of elements inserted to filter
pub type NoOfElements = u32;

/// Bloom filter struct responsile for all operation
/// specific to bloom filters
///
/// A Bloom filter is a space-efficient probabilistic data structure that is used to test
/// whether an element is a member of a set in this case if a key exists in sstable
/// For more understanding <https://brilliant.org/wiki/bloom-filter/>
#[derive(Debug)]
pub struct BloomFilter {
    /// SSTable directory for bloom filter. Not set until a flush happens
    pub sst_dir: Option<PathBuf>,

    /// Number of hash function used by filter
    pub no_of_hash_func: usize,

    /// Number of elements inserted to filter
    pub no_of_elements: AtomicU32,

    /// Thread-safe bit vector which is used to predict if a key
    /// exists in the filter or not
    pub bit_vec: Arc<Mutex<BitVec>>,

    /// To what extent should we permit false positives the lower
    /// the more acurate but more costly in terms of computation
    pub false_positive_rate: f64,

    /// File path for file that stores filter metadata
    pub file_path: Option<PathBuf>,
}

impl BloomFilter {
    /// creates new [`BloomFilter`] instance
    pub fn new(false_positive_rate: f64, no_of_elements: usize) -> Self {
        assert!(
            false_positive_rate >= 0.0,
            "False positive rate can not be less than or equal to zero"
        );
        assert!(no_of_elements > 0, "No of elements should be greater than 0");

        let no_of_bits = Self::calculate_no_of_bits(no_of_elements, false_positive_rate);
        let no_of_hash_func = Self::calculate_no_of_hash_function(no_of_bits, no_of_elements as u32) as usize;
        let bv = BitVec::from_elem(no_of_bits as usize, false);

        Self {
            no_of_elements: AtomicU32::new(0),
            no_of_hash_func,
            sst_dir: None,
            bit_vec: Arc::new(Mutex::new(bv)),
            false_positive_rate,
            file_path: None,
        }
    }

    /// Adds key to filter
    pub(crate) fn set<K: Hash + Copy>(&mut self, key: K) {
        let mut bits = self.bit_vec.lock().expect("Failed to lock file");
        for i in 0..self.no_of_hash_func {
            let hash = self.calculate_hash(key, i);
            let index = (hash % bits.len() as u64) as usize;
            bits.set(index, true)
        }
        self.no_of_elements.fetch_add(1, Ordering::Relaxed);
    }

    /// Checks if a key exists or not
    pub(crate) fn contains<K: Hash + Copy>(&self, key: K) -> bool {
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
    /// Writes filter metadata to disk
    ///
    /// Writes filter to disk, note, this does not include the
    /// `bit_vec``, bit vec re-computed during crash recovery
    ///
    /// # Errors
    ///
    /// Returns IO error in case write fails
    pub async fn write<P: AsRef<Path> + Send + Sync>(&mut self, dir: P) -> Result<(), Error> {
        let file_path = dir.as_ref().join(format!("{}.db", FILTER_FILE_NAME));
        let file = FilterFileNode::new(file_path.to_owned(), crate::fs::FileType::Filter)
            .await
            .unwrap();
        let serialized_data = self.serialize();
        file.node.write_all(&serialized_data).await?;
        self.file_path = Some(file_path.to_owned());
        Ok(())
    }

    /// Reconstructs `bit_vec`` from entries
    pub(crate) fn build_filter_from_entries(&mut self, entries: &SkipMapEntries<Key>) {
        entries.iter().for_each(|e| self.set(e.key()));
    }

    /// Retrieves filter meta data from disk
    ///
    /// # Errors
    ///
    /// Returns IO error in case recovery fails
    pub async fn recover_meta(&mut self) -> Result<(), Error> {
        if self.file_path.is_none() {
            return Err(FilterFilePathNotProvided);
        };
        let (false_pos, no_hash_func, no_elements) = FilterFileNode::recover(self.file_path.as_ref().unwrap()).await?;
        self.false_positive_rate = false_pos;
        self.no_of_hash_func = no_hash_func as usize;
        self.no_of_elements = AtomicU32::new(no_elements);
        let no_of_bits = Self::calculate_no_of_bits(
            self.no_of_elements.load(Ordering::Relaxed) as usize,
            self.false_positive_rate,
        );
        self.bit_vec = Arc::new(Mutex::new(BitVec::from_elem(no_of_bits as usize, false)));
        Ok(())
    }

    /// Serializes `BloomFilter` attributes
    ///
    /// Converts `BloomFilter` atttributes such as no_of_hash_func, no_of_elements and
    /// false positive floating point into byte vector
    ///
    /// Returns the byte vector
    fn serialize(&self) -> ByteSerializedEntry {
        // No of Hash Function + No of Elements  + False Positive
        let entry_len = SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64;

        let mut serialized_data = Vec::with_capacity(entry_len);

        serialized_data.extend_from_slice(&(self.no_of_hash_func as u32).to_le_bytes());

        serialized_data.extend_from_slice(&AtomicU32::load(&self.no_of_elements, Ordering::Relaxed).to_le_bytes());

        serialized_data.extend_from_slice(&util::float_to_le_bytes(self.false_positive_rate));

        serialized_data
    }

    /// Sets the sst_dir field for [`BloomFilter`]
    pub fn set_sstable_path<P: AsRef<Path>>(&mut self, path: P) {
        self.sst_dir = Some(path.as_ref().to_path_buf());
    }

    /// Resets the [`BloomFilter`] instance
    pub fn clear(&mut self) -> Self {
        let mut bits = self.bit_vec.lock().expect("Failed to lock file");
        for i in 0..bits.len() {
            bits.set(i, false);
        }
        let no_of_hash_func = self.no_of_hash_func;
        let bit_vec = BitVec::from_elem(bits.len(), false);
        Self {
            sst_dir: None,
            no_of_hash_func,
            no_of_elements: AtomicU32::new(0),
            bit_vec: Arc::new(Mutex::new(bit_vec)),
            false_positive_rate: self.false_positive_rate,
            file_path: None,
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
    pub fn get_sst_dir(&self) -> &PathBuf {
        // Retrieve the element count atomically.
        self.sst_dir.as_ref().unwrap()
    }

    /// Generates the hashed version of a provided key
    fn calculate_hash<K: Hash>(&self, key: K, seed: usize) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.write_u64(seed as u64);
        hasher.finish()
    }

    /// Calculates number of bits to be inserted to `bit_vec`
    fn calculate_no_of_bits(no_of_elements: usize, false_positive_rate: f64) -> u32 {
        let no_bits = -((no_of_elements as f64 * false_positive_rate.ln()) / ((2_f64.ln()).powi(2))).ceil();
        no_bits as u32
    }

    /// Calculates number of hash fuctions to be used by [`BloomFilter`]
    fn calculate_no_of_hash_function(no_of_bits: u32, no_of_elements: u32) -> u32 {
        let no_hash_func = (no_of_bits as f64 / no_of_elements as f64) * (2_f64.ln()).ceil();
        no_hash_func as u32
    }
}

impl Clone for BloomFilter {
    fn clone(&self) -> Self {
        // Implement custom logic here if needed
        BloomFilter {
            sst_dir: self.sst_dir.clone(),
            no_of_hash_func: self.no_of_hash_func,
            no_of_elements: AtomicU32::load(&self.no_of_elements, Ordering::Relaxed).into(),
            bit_vec: self.bit_vec.clone(),
            false_positive_rate: self.false_positive_rate,
            file_path: self.file_path.to_owned(),
        }
    }
}

impl Default for BloomFilter {
    fn default() -> Self {
        Self {
            sst_dir: None,
            no_of_hash_func: 0,
            no_of_elements: AtomicU32::new(0),
            bit_vec: Arc::new(Mutex::new(BitVec::new())),
            false_positive_rate: 0.0,
            file_path: None,
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

        let no_bits = -((no_of_elements as f64 * false_positive_rate.ln()) / ((2_f64.ln()).powi(2))).ceil();

        let expected_no_hash_func = ((no_bits / no_of_elements as f64) * (2_f64.ln()).ceil()) as usize;

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
            bloom_filter.set(i)
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
            bloom.set(i);
        }

        let mut false_positives = 0;
        let num_tested_elements = 2000;

        // Test all non-inserted elements for containment.
        // Count the number of false positives.
        for i in num_elements..num_elements + num_tested_elements {
            if bloom.contains(i) {
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
            bloom.set(i);
        }

        let mut false_positives = 0;
        let num_tested_elements = 2000;

        // Test all non-inserted elements for containment.
        // Count the number of false positives.
        for i in num_elements..num_elements + num_tested_elements {
            if bloom.contains(i) {
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
            bloom.set(i);
        }

        let mut false_positives = 0;
        let num_tested_elements = 2000;

        // Test all non-inserted elements for containment.
        // Count the number of false positives.
        for i in num_elements..num_elements + num_tested_elements {
            if bloom.contains(i) {
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
