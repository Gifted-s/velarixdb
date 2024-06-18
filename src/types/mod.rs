/// Contains type aliases shared across modules to prevent redeclaration and help with readability
use crossbeam_skiplist::SkipMap;
use indexmap::IndexMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{bucket::BucketMap, filter::BloomFilter, key_range::KeyRange, memtable::InMemoryTable};
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type ValOffset = usize;
pub type CreationTime = u64;
pub type IsTombStone = bool;
pub type FlushSignal = u8;
pub type NoBytesRead = usize;
pub type SkipMapEntries<K> = Arc<SkipMap<K, (ValOffset, CreationTime, IsTombStone)>>;
pub type FlushReceiver = async_broadcast::Receiver<FlushSignal>;
pub type BucketMapHandle = Arc<RwLock<BucketMap>>;
pub type BloomFilterHandle = Arc<RwLock<Vec<BloomFilter>>>;
pub type KeyRangeHandle = Arc<RwLock<KeyRange>>;
pub type ImmutableMemTable<K> = Arc<RwLock<IndexMap<K, Arc<RwLock<InMemoryTable<K>>>>>>;
pub type Duration = u64;
pub type Bool = bool;
