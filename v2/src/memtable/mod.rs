use crate::range::prefix_to_range;
use crate::value::{ParsedInternalKey, SeqNo, UserKey, ValueOffset, ValueType};
use crate::Value;
use crossbeam_skiplist::SkipMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

/// The memtable stores entries in memory before flushing to disk
#[derive(Default)]
pub struct MemTable {
    #[doc(hidden)]
    items: SkipMap<ParsedInternalKey, ValueOffset>,

    /// Approximate active memtable size
    ///
    /// If this grows too large, a flush is triggered
    pub(crate) approximate_size: AtomicU32,
}

impl MemTable {
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = Value> + '_ {
        self.items
            .iter()
            .map(|entry| Value::from((entry.key().clone(), entry.value().clone())))
    }

    pub fn range<'a, R: RangeBounds<ParsedInternalKey> + 'a>(
        &'a self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = Value> + 'a {
        self.items
            .range(range)
            .map(|entry| Value::from((entry.key().clone(), entry.value().clone())))
    }

    /// Returns the by key if it exists
    ///
    /// The item with the highest seqno will be returned, if `seqno` is None
    pub fn get<K: AsRef<[u8]>>(&self, key: K, seqno: Option<SeqNo>) -> Option<Value> {
        let prefix = key.as_ref();

        // NOTE: This range start deserves some explanation...
        // InternalKeys are multi-sorted by 2 categories: user_key and Reverse(seqno). (tombstone doesn't really matter)
        // We search for the lowest entry that is greater or equal the user's prefix key
        // and has the highest seqno (because the seqno is stored in reverse order)
        //
        // Example: We search for "abc"
        //
        // key -> seqno
        //
        // a   -> 7
        // abc -> 5 <<< This is the lowest key (highest seqno) that matches the range
        // abc -> 4
        // abc -> 3
        // abcdef -> 6
        // abcdef -> 5

        let (lower_bound, upper_bound) = prefix_to_range(prefix);

        let lower_bound = match lower_bound {
            Included(key) => Included(ParsedInternalKey::new(key, SeqNo::MAX, ValueType::Value)),
            Unbounded => Unbounded,
            _ => panic!("lower bound cannot be excluded"),
        };

        let upper_bound = match upper_bound {
            Excluded(key) => Excluded(ParsedInternalKey::new(key, SeqNo::MAX, ValueType::Value)),
            Unbounded => Unbounded,
            _ => panic!("upper bound cannot be included"),
        };

        let range = (lower_bound, upper_bound);

        for entry in self.items.range(range) {
            let key = entry.key();

            // NOTE: We are past the searched key, so we can immediately return None
            if &*key.user_key > prefix {
                return None;
            }

            if let Some(seqno) = seqno {
                if key.seqno < seqno {
                    return Some(Value::from((entry.key().clone(), entry.value().clone())));
                }
            } else {
                return Some(Value::from((entry.key().clone(), entry.value().clone())));
            }
        }
        None
    }

    /// Get approximate size of memtable in bytes.
    pub fn size(&self) -> u32 {
        self.approximate_size.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Count the amount of items in the memtable.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` if the memtable is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Inserts an item into the memtable
    pub fn insert(&self, item: Value) -> (u32, u32) {
        #[allow(clippy::cast_possible_truncation)]
        let item_size = item.size() as u32;

        let size_before = self
            .approximate_size
            .fetch_add(item_size, std::sync::atomic::Ordering::AcqRel);

        let key = ParsedInternalKey::new(item.key, item.seqno, item.value_type);
        self.items.insert(key, item.value_offset);

        (item_size, size_before + item_size)
    }

    /// Returns the highest sequence number in the memtable.
    pub fn get_lsn(&self) -> Option<SeqNo> {
        self.items
            .iter()
            .map(|x| {
                let key = x.key();
                key.seqno
            })
            .max()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{value::ValueType, UserKey};
    use test_log::test;

    fn memtable_mvcc_point_read() {
        let memtable = MemTable::default();

        memtable.insert(Value::new(*b"hello-key-999991", 100, 0, ValueType::Value));

        let item = memtable.get("hello-key-99999", None);
        assert_eq!(None, item);

        let item = memtable.get("hello-key-999991", None);
        assert_eq!(100, item.unwrap().value_offset);

        memtable.insert(Value::new(*b"hello-key-999991", 200, 1, ValueType::Value));
        let item = memtable.get("hello-key-99999", None);
        assert_eq!(None, item);

        let item = memtable.get("hello-key-999991", None);
        assert_eq!(200, item.unwrap().value_offset);

        let item = memtable.get("hello-key-99999", Some(1));
        assert_eq!(None, item);

        let item = memtable.get("hello-key-999991", Some(1));
        assert_eq!(100, item.unwrap().value_offset);

        let item = memtable.get("hello-key-99999", Some(2));
        assert_eq!(None, item);

        let item = memtable.get("hello-key-999991", Some(2));
        assert_eq!(200, item.unwrap().value_offset);
    }

    #[test]
    fn memtable_get() {
        let memtable = MemTable::default();

        let value = Value::new(b"abc".to_vec(), 100, 0, ValueType::Value);

        memtable.insert(value.clone());

        assert_eq!(Some(value), memtable.get("abc", None));
    }

    #[test]
    fn memtable_get_highest_seqno() {
        let memtable = MemTable::default();

        memtable.insert(Value::new(b"abc".to_vec(), 100, 0, ValueType::Value));
        memtable.insert(Value::new(b"abc".to_vec(), 100, 1, ValueType::Value));
        memtable.insert(Value::new(b"abc".to_vec(), 100, 2, ValueType::Value));
        memtable.insert(Value::new(b"abc".to_vec(), 100, 3, ValueType::Value));
        memtable.insert(Value::new(b"abc".to_vec(), 100, 4, ValueType::Value));

        assert_eq!(
            Some(Value::new(b"abc".to_vec(), 100, 4, ValueType::Value,)),
            memtable.get("abc", None)
        );
    }

    #[test]
    fn memtable_get_prefix() {
        let memtable = MemTable::default();

        memtable.insert(Value::new(b"abc0".to_vec(), 200, 0, ValueType::Value));
        memtable.insert(Value::new(b"abc".to_vec(), 200, 255, ValueType::Value));

        assert_eq!(
            Some(Value::new(b"abc".to_vec(), 200, 255, ValueType::Value,)),
            memtable.get("abc", None)
        );

        assert_eq!(
            Some(Value::new(b"abc0".to_vec(), 200, 0, ValueType::Value,)),
            memtable.get("abc0", None)
        );
    }
}

#[test]
fn memtable_range() {
    let memtable = MemTable::default();
    memtable.insert(Value::new(b"a".to_vec(), 100, 0, ValueType::Value));
    memtable.insert(Value::new(b"abc".to_vec(), 100, 0, ValueType::Value));
    memtable.insert(Value::new(b"b".to_vec(), 100, 0, ValueType::Value));
    memtable.insert(Value::new(b"abcdef".to_vec(), 100, 0, ValueType::Value));

    let key: UserKey = Arc::new(*b"abcdef");

    assert_eq!(
        2,
        memtable
            .range(ParsedInternalKey::new(key, SeqNo::MAX, ValueType::Value)..)
            .count()
    );
}
