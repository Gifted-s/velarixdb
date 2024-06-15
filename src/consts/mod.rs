use crate::storage::SizeUnit;

pub const GC_THREAD_COUNT: u32 = 5;

pub const GC_CHUNK_SIZE: usize = SizeUnit::Kilobytes.to_bytes(1);

pub const WRITE_BUFFER_SIZE: usize = SizeUnit::Kilobytes.to_bytes(50);

pub const DEFAULT_MAX_WRITE_BUFFER_NUMBER: usize = 2;

pub const DEFAULT_FALSE_POSITIVE_RATE: f64 = 1e-300;

pub const VALUE_LOG_DIRECTORY_NAME: &str = "v_log";

pub const BUCKETS_DIRECTORY_NAME: &str = "buckets";

pub const BUCKET_DIRECTORY_PREFIX: &str = "bucket";

pub const VLOG_FILE_NAME: &str = "val_log.bin";

pub const META_DIRECTORY_NAME: &str = "meta";

pub const TOMB_STONE_MARKER: usize = 0;

// This is a minimum time that must pass since the last compaction attempt for a specific data file (SSTable).
// This prevents continuous re-compactions of the same file.
pub const DEFAULT_TOMBSTONE_COMPACTION_INTERVAL_MILLI: u64 = 10 * 86400000;

// 1 Hour
pub const DEFAULT_COMPACTION_INTERVAL_MILLI: u64 = 3600000;

// 1 Min
//pub const DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL_MILLI: u64 = 60000;
pub const DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL_MILLI: u64 = 1000 * 20;

//pub const DEFAULT_MAJOR_GARBAGE_COLLECTION_INTERVAL_MILLI: 10 hours
pub const DEFAULT_MAJOR_GARBAGE_COLLECTION_INTERVAL_MILLI: u64 = 36000000;

pub const DEFAULT_FLUSH_DATA_CHANNEL_SIZE: usize = 10;

/// Only the compactor listens to flush events for now
/// This should be greater than or equal to the writter buffer size since
/// all be buffer can be flushed in parallel leading to multiple signals sent at once
pub const DEFAULT_FLUSH_SIGNAL_CHANNEL_SIZE: usize = 2;
// tombstone should only be removed after 120 days to guarantee that obsolete data don't
// resurrect by prematurelly deleting tombstone
pub const TOMB_STONE_TTL: u64 = 120 * 86400000;

pub const DEFUALT_ENABLE_TTL: bool = false;

// When data is written with a TTL, it is automatically deleted after the specified time.
// For now this is set to a year in milliseconds
//pub const ENTRY_TTL: u64 = 365 * 86400000;

pub const ENTRY_TTL: u64 = 1 * 86400000;

pub const BUCKET_LOW: f64 = 0.5;

pub const BUCKET_HIGH: f64 = 1.5;

pub const MIN_SSTABLE_SIZE: usize = 32;

pub const MIN_TRESHOLD: usize = 4;

pub const MAX_TRESHOLD: usize = 32;

pub const DEFAULT_ALLOW_PREFETCH: bool = true;

pub const DEFAULT_PREFETCH_SIZE: usize = 10;

pub const EOF: &str = "EOF";

pub const HEAD_ENTRY_KEY: &[u8; 4] = b"head";

pub const TAIL_ENTRY_KEY: &[u8; 4] = b"tail";
// 4 bytes to store length of key "head"
// 4 bytes to store the actual key "head"
// 4 bytes to store the head offset
// 8 bytes to store the head entry creation date
// 1 byte for tombstone marker
pub const HEAD_ENTRY_LENGTH: usize = 21;

pub const VLOG_TAIL_ENTRY_LENGTH: usize = 21;

pub const SIZE_OF_USIZE: usize = std::mem::size_of::<usize>();

pub const SIZE_OF_U32: usize = std::mem::size_of::<u32>();

pub const SIZE_OF_U64: usize = std::mem::size_of::<u64>();

pub const SIZE_OF_U8: usize = std::mem::size_of::<u8>();

pub const FLUSH_SIGNAL: u8 = 1;
