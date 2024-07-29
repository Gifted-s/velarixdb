mod bit_array;

use crate::serde::{Deserializable, Serializable};
use crate::{DeserializeError, SerializeError};
use bit_array::BitArray;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use seahash::SeaHasher;
use std::hash::Hasher;
use std::io::{Read, Write};

pub const BLOOM_HEADER_MAGIC: &[u8] = &[b'B', b'L', b'0', b'0', b'1'];

pub type CompositeHash = (u64, u64);

#[derive(Debug, Eq, PartialEq)]
pub struct BloomFilter {
    inner: BitArray,

    no_of_bits: usize,

    no_of_hash_func: usize,
}

impl Serializable for BloomFilter {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_all(BLOOM_HEADER_MAGIC)?;

        // Filter type (unsed for now)
        writer.write_u8(0)?;

        writer.write_u64::<BigEndian>(self.no_of_bits as u64)?;
        writer.write_u64::<BigEndian>(self.no_of_hash_func as u64)?;
        writer.write_all(self.inner.bytes())?;

        Ok(())
    }
}

impl Deserializable for BloomFilter {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let mut magic = [0u8; BLOOM_HEADER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != BLOOM_HEADER_MAGIC {
            return Err(DeserializeError::InvalidHeader("BloomFilter".to_string()));
        }

        // Filter type not used for now
        let _ = reader.read_u8()?;

        let no_of_bits = reader.read_u64::<BigEndian>()? as usize;
        let no_of_hash_func = reader.read_u64::<BigEndian>()? as usize;

        let mut bytes = vec![0; no_of_bits / 8];
        reader.read_exact(&mut bytes)?;

        Ok(Self::from(
            no_of_bits,
            no_of_hash_func,
            bytes.into_boxed_slice(),
        ))
    }
}

impl BloomFilter {
    fn from(no_of_bits: usize, no_of_hash_func: usize, bytes: Box<[u8]>) -> Self {
        Self {
            inner: BitArray::from_bytes(bytes),
            no_of_bits,
            no_of_hash_func,
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn calculate_no_of_bits(n: usize, fp_rate: f32) -> usize {
        use std::f32::consts::LN_2;

        let n = n as f32;
        let ln2_squared = LN_2.powi(2);

        let m = -(n * fp_rate.ln() / ln2_squared);
        ((m / 8.0).ceil() * 8.0) as usize
    }

    // TODO: Compare this with get_no_hash_func_heuristic
    // /// Calculates number of hash fuctions to be used by [`BloomFilter`]
    // fn calculate_no_of_hash_func(no_of_bits: u32, no_of_elements: u32) -> u32 {
    //     let no_hash_func = (no_of_bits as f64 / no_of_elements as f64) * (2_f64.ln()).ceil();
    //     no_hash_func as u32
    // }

    /// Heuristically get the somewhat-optimal k value for a given desired FPR
    fn get_no_hash_func_heuristic(fp_rate: f32) -> usize {
        match fp_rate {
            _ if fp_rate > 0.4 => 1,
            _ if fp_rate > 0.2 => 2,
            _ if fp_rate > 0.1 => 3,
            _ if fp_rate > 0.05 => 4,
            _ if fp_rate > 0.03 => 5,
            _ if fp_rate > 0.02 => 5,
            _ if fp_rate > 0.01 => 7,
            _ if fp_rate > 0.001 => 10,
            _ if fp_rate > 0.000_1 => 13,
            _ if fp_rate > 0.000_01 => 17,
            _ => 20,
        }
    }

    #[must_use]
    pub fn with_fp_rate(item_count: usize, fp_rate: f32) -> Self {
        // NOTE: Some sensible minimum
        let fp_rate = fp_rate.max(0.000_001);

        let no_of_hash_func = Self::get_no_hash_func_heuristic(fp_rate);
        let no_of_bits = Self::calculate_no_of_bits(item_count, fp_rate);

        Self {
            inner: BitArray::with_capacity(no_of_bits / 8),
            no_of_bits,
            no_of_hash_func,
        }
    }

    /// Adds the key to the filter
    pub fn set_with_hash(&mut self, (mut h1, mut h2): CompositeHash) {
        for i in 0..(self.no_of_hash_func as u64) {
            let idx = h1 % (self.no_of_bits as u64);

            self.enable_bit(idx as usize);

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(i);
        }
    }

    pub fn contains_hash(&self, hash: CompositeHash) -> bool {
        let (mut h1, mut h2) = hash;

        for i in 0..(self.no_of_hash_func as u64) {
            let idx = h1 % (self.no_of_bits as u64);

            if !self.inner.get(idx as usize) {
                return false;
            }

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(i);
        }
        true
    }

    /// Sets the bit at the given index to `true`
    fn enable_bit(&mut self, idx: usize) {
        self.inner.set(idx, true);
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.contains_hash(Self::get_hash(key))
    }

    #[must_use]
    pub fn get_hash(key: &[u8]) -> CompositeHash {
        let mut hasher = SeaHasher::default();
        hasher.write(key);
        let h1 = hasher.finish();

        hasher.write(key);
        let h2 = hasher.finish();

        (h1, h2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use test_log::test;

    #[test]
    fn bloom_serde_round_trip() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("bf");
        let mut file = File::create(&path)?;

        let mut filter = BloomFilter::with_fp_rate(10, 0.0001);

        for key in [
            b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
            b"item8", b"item9",
        ] {
            filter.set_with_hash(BloomFilter::get_hash(key));
        }

        filter.serialize(&mut file)?;
        file.sync_all()?;
        drop(file);

        let mut file = File::open(&path)?;
        let filter_copy = BloomFilter::deserialize(&mut file)?;

        assert_eq!(filter, filter_copy);

        Ok(())
    }

    #[test]
    fn bloom_calculate_no_of_bits() {
        assert_eq!(9_592, BloomFilter::calculate_no_of_bits(1_000, 0.01));
        assert_eq!(4_800, BloomFilter::calculate_no_of_bits(1_000, 0.1));
        assert_eq!(4_792_536, BloomFilter::calculate_no_of_bits(1_000_000, 0.1));
    }

    #[test]
    fn bloom_basic() {
        let mut filter = BloomFilter::with_fp_rate(10, 0.0001);

        for key in [
            b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
            b"item8", b"item9",
        ] {
            assert!(!filter.contains(key));
            filter.set_with_hash(BloomFilter::get_hash(key));
            assert!(filter.contains(key));

            assert!(!filter.contains(b"asdasdasdasdasdasdasd"));
        }
    }

    #[test]
    fn bloom_fpr() {
        let item_count = 1_000_000;
        let fpr = 0.01;

        let mut filter = BloomFilter::with_fp_rate(item_count, fpr);

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            filter.set_with_hash(BloomFilter::get_hash(key));
            assert!(filter.contains(key));
        }

        let mut false_positives = 0;

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            if filter.contains(key) {
                false_positives += 1;
            }
        }

        assert!((10_000 - false_positives) < 200);
    }
}
