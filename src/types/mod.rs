use crate::{
    bucket::BucketMap,
    filter::BloomFilter,
    key_range::KeyRange,
    mem::{MemTable, SkipMapValue},
};
use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;
use indexmap::IndexMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Contains type aliases to help with readability
/// Represents a key in the database
pub type Key = Vec<u8>;

/// Represents a value in the database
pub type Value = Vec<u8>;

/// Represents the offset of a value
pub type ValOffset = usize;

/// Represents the creation time of an entry
pub type CreatedAt = DateTime<Utc>;

/// Represents a tombstone marker (true if entry is deleted)
pub type IsTombStone = bool;

/// Represents a signal for flushing data
pub type FlushSignal = u8;

/// Represents the number of bytes read
pub type NoBytesRead = usize;

/// Represents entries in a SkipMap with generic key type
pub type SkipMapEntries<K> = Arc<SkipMap<K, SkipMapValue<ValOffset>>>;

/// Represents a receiver for flush signals
pub type FlushReceiver = async_broadcast::Receiver<FlushSignal>;

/// Thread-safe BucketMap
pub type BucketMapHandle = Arc<RwLock<BucketMap>>;

/// Thread-safe vector of BloomFilters
pub type BloomFilterHandle = Arc<RwLock<Vec<BloomFilter>>>;

/// Thread-safe KeyRange type
pub type KeyRangeHandle = Arc<RwLock<KeyRange>>;

/// Represents an immutable MemTable
pub type ImmutableMemTable<K> = Arc<RwLock<IndexMap<K, Arc<RwLock<MemTable<K>>>>>>;

/// Alias for a boolean value
pub type Bool = bool;

/// Represents an ID for a MemTable
pub type MemtableId = Vec<u8>;

/// Represents a database name as a string slice
pub type DBName<'a> = &'a str;

/// Represents updated entries in a SkipMap after garbage collection, with a generic key type
pub type GCUpdatedEntries<K> = Arc<RwLock<SkipMap<K, SkipMapValue<ValOffset>>>>;
