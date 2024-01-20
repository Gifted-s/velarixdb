use std::{
    collections::hash_map::DefaultHasher,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{self, Read, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
};

use bit_vec::BitVec;
pub struct BloomFilter {
    bits_len: u32,  //  this is the length of the bit stored
    bytes_len: u32, // this is the number of bytes it takes to store the bit vector, this is useful when we want to retrive the bit vector from disk
    no_of_hash_func: usize, // number of hash functions to used by the bloom filter
    no_of_elements: AtomicU32, // number of elements in the bloom filter
    bit_vec: Arc<Mutex<BitVec>>, // store the bit array each representing 0 or 1 meaning true or false
}

impl BloomFilter {
    pub(crate) fn new(false_positive_rate: f64, no_of_elements: usize) -> Self {
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
            bits_len: no_of_bits,
            bytes_len: bv.len() as u32,
            no_of_elements: AtomicU32::new(0),
            no_of_hash_func,
            bit_vec: Arc::new(Mutex::new(bv)),
        }
    }
    pub(crate) fn set<T: Hash>(&mut self, key: &T) {
        let mut bits = self.bit_vec.lock().expect("Failed to lock file");
        for i in 0..self.no_of_hash_func {
            let hash = self.calculate_hash(key, i as usize);
            let index = (hash % bits.len() as u64) as usize;
            bits.set(index, true)
        }
        self.no_of_elements.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn contains<T: Hash>(&self, key: &T) -> bool {
        let bits = self.bit_vec.lock().expect("Failed to lock file");
        for i in 0..self.no_of_hash_func {
            let hash = self.calculate_hash(key, i as usize);
            let index = (hash % bits.len() as u64) as usize;
            if !bits[index] {
                return false;
            }
        }
        true
    }

    pub(crate) fn write_to_file(&self, file_path: PathBuf) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)?;

        // write number of element
        let capacity_bytes = self.bit_vec.lock().unwrap().to_bytes().len() as u32;
        let capacity_bits = self.bit_vec.lock().unwrap().len() as u32;
        let num_hash_function = self.num_of_hash_functions() as u32;
        let num_of_elem = self.num_elements() as u32;

        // write capacity function as little endian byte array
        file.write_all(&capacity_bytes.to_le_bytes())
            .expect("Failure to write bloom filter capacity to file");

        // write capacity in bits function as little endian byte array
        file.write_all(&capacity_bits.to_le_bytes())
            .expect("Failure to write bloom filter capacity to file");

        // write number of elements as little endian byte array
        file.write_all(&num_of_elem.to_le_bytes())
            .expect("Failure to write bloom filter size to file");

        // write num of hash function as little endian byte array
        file.write_all(&num_hash_function.to_le_bytes())
            .expect("Fail to write number of hash function file");

        // convert bit vector to bytes then store in file
        file.write_all(&self.bit_vec.lock().unwrap().to_bytes())
            .expect("unable to store bit vec in file");

        Ok(())
    }

    pub(crate) fn from_file(&self, file_path: PathBuf) -> Self {
        let mut file = File::open(file_path).expect("Error opening file");

        // Read capacity from file
        let mut bitvec_byte_len_as_byte_array = [0; 4];
        file.read_exact(&mut bitvec_byte_len_as_byte_array)
            .expect("Error reading file");
        let bytes_len = self.bytes_to_usize(bitvec_byte_len_as_byte_array);

        // Read capacity from file
        let mut bitvec_bit_len_as_byte_array = [0; 4];
        file.read_exact(&mut bitvec_bit_len_as_byte_array)
            .expect("Error reading file");
        let bits_len = self.bytes_to_usize(bitvec_bit_len_as_byte_array);

        // Read no of elements from file
        let mut no_of_elem_bytes = [0; 4];
        file.read_exact(&mut no_of_elem_bytes)
            .expect("Error reading file");
        let no_of_elements = self.bytes_to_usize(no_of_elem_bytes);

        // Read number of hash function from file
        let mut no_of_hash_func_bytes = [0; 4];
        file.read_exact(&mut no_of_hash_func_bytes)
            .expect("Error reading file");
        let no_of_hash_func = self.bytes_to_usize(no_of_hash_func_bytes);

        // Read bit vector as bytes
        let mut bit_vec_bytes: Vec<u8> = vec![0; bytes_len];
        file.read_exact(&mut bit_vec_bytes)
            .expect("Unable to read file");
        let mut retrieved_bit_vector = BitVec::from_bytes(&bit_vec_bytes);

        // remove trailing bits since we retrived the bit vector as a series of bytes represented as array of 8bits
        if retrieved_bit_vector.len() > bits_len {
            for _ in 0..(retrieved_bit_vector.len() - bits_len) {
                retrieved_bit_vector.pop();
            }
        }
        Self {
            bits_len: bits_len as u32,
            bytes_len: bytes_len as u32,
            no_of_hash_func,
            no_of_elements: AtomicU32::new(no_of_elements as u32),
            bit_vec: Arc::new(Mutex::new(retrieved_bit_vector)),
        }
    }

    fn bytes_to_usize(&self, bytes: [u8; 4]) -> usize {
        return u32::from_le_bytes(bytes) as usize;
    }

    pub fn clear(&self) -> Self {
        let mut bits = self.bit_vec.lock().expect("Failed to lock file");
        for i in 0..bits.len() {
            bits.set(i, false);
        }
        let no_of_hash_func = self.no_of_hash_func;
        let bit_vec = BitVec::from_elem(bits.len(), false);
        Self {
            bits_len: 0,
            bytes_len: 0,
            no_of_hash_func,
            no_of_elements: AtomicU32::new(0),
            bit_vec: Arc::new(Mutex::new(bit_vec)),
        }
    }

    /// Returns the current number of elements inserted into the Bloom filter.
    pub(crate) fn num_elements(&self) -> usize {
        // Retrieve the element count atomically.
        self.no_of_elements.load(Ordering::Relaxed) as usize
    }

    /// Returns the current number of elements inserted into the Bloom filter.
    pub(crate) fn num_bits(&self) -> usize {
        // Retrieve the element count atomically.
        self.bit_vec.lock().unwrap().len() as usize
    }

    /// Returns the current number of hash functions.
    pub(crate) fn num_of_hash_functions(&self) -> usize {
        // Retrieve the element count atomically.
        self.no_of_hash_func as usize
    }

    fn calculate_hash<T: Hash>(&self, key: &T, seed: usize) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.write_usize(seed);
        hasher.finish()
    }

    fn calculate_no_of_bits(no_of_elements: usize, false_positive_rate: f64) -> u32 {
        let no_bits =
            -((no_of_elements as f64 * false_positive_rate.ln()) / ((2_f64.ln()).powi(2))).ceil();
        return no_bits as u32;
    }

    fn calculate_no_of_hash_function(no_of_bits: u32, no_of_elements: u32) -> u32 {
        let no_hash_func = (no_of_bits as f64 / no_of_elements as f64) * (2_f64.ln()).ceil();
        no_hash_func as u32
    }
}

#[cfg(test)]

mod tests {
    use std::{
        fs::{self, remove_dir},
        path,
    };

    use super::*;

    #[test]
    fn test_set_and_contain() {
        let false_positive_rate = 0.01;
        let no_of_elements: usize = 10;
        let mut bloom_filter = BloomFilter::new(false_positive_rate, no_of_elements);

        let no_bits =
            -((no_of_elements as f64 * false_positive_rate.ln()) / ((2_f64.ln()).powi(2))).ceil();

        let expected_no_hash_func =
            ((no_bits as f64 / no_of_elements as f64) * (2_f64.ln()).ceil()) as usize;

        assert_eq!(bloom_filter.num_elements(), 0);
        assert_eq!(bloom_filter.no_of_hash_func, expected_no_hash_func);
        assert_eq!(bloom_filter.bit_vec.lock().unwrap().len(), no_bits as usize);
        let k = &vec![1, 2, 3, 4];
        bloom_filter.set(k);
        assert_eq!(bloom_filter.num_elements(), 1);
        assert!(bloom_filter.contains(k));
    }

    #[test]
    fn test_write_to_file() {
        let false_positive_rate = 0.01;
        let no_of_elements: usize = 10;
        let mut bloom_filter = BloomFilter::new(false_positive_rate, no_of_elements);

        let no_bits =
            -((no_of_elements as f64 * false_positive_rate.ln()) / ((2_f64.ln()).powi(2))).ceil();

        let expected_no_hash_func =
            ((no_bits as f64 / no_of_elements as f64) * (2_f64.ln()).ceil()) as usize;

        assert_eq!(bloom_filter.num_elements(), 0);
        assert_eq!(bloom_filter.no_of_hash_func, expected_no_hash_func);
        assert_eq!(bloom_filter.bit_vec.lock().unwrap().len(), no_bits as usize);

        let k1 = 2;
        let k2 = 3;
        let k3 = 4;
        let k4 = 5;
        bloom_filter.set(&k1);
        bloom_filter.set(&k2);
        bloom_filter.set(&k3);
        bloom_filter.set(&k4);

        let path = PathBuf::new().join("sunkanmi.sst");
        bloom_filter.write_to_file(path.clone()).unwrap();
        let new_bloom_filter =
            BloomFilter::new(false_positive_rate, no_of_elements).from_file(path.clone());

        assert_eq!(new_bloom_filter.num_elements(), 4);
        assert_eq!(new_bloom_filter.no_of_hash_func, expected_no_hash_func);

        assert!(new_bloom_filter.contains(&k1));
        assert!(new_bloom_filter.contains(&k2));
        assert!(new_bloom_filter.contains(&k3));
        assert!(new_bloom_filter.contains(&k4));
        std::fs::remove_file(path.clone()).unwrap();
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
