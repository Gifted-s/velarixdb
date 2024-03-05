use crate::storage_engine::SizeUnit;

pub const GC_THREAD_COUNT: u32 = 5;

pub const DEFAULT_MEMTABLE_CAPACITY: usize = SizeUnit::Kilobytes.to_bytes(1);

pub const DEFAULT_FALSE_POSITIVE_RATE: f64 = 1e-100;

pub const VALUE_LOG_DIRECTORY_NAME: &str = "v_log";

pub const BUCKETS_DIRECTORY_NAME: &str = "buckets";

pub const BUCKET_DIRECTORY_PREFIX: &str = "bucket";

pub const VLOG_FILE_NAME: &str = "val_log.bin";

pub const META_DIRECTORY_NAME: &str = "meta";

pub const TOMB_STONE: usize = 0;

pub const ENABLE_TTL: bool = false;

// When data is written with a TTL, it is automatically deleted after the specified time.
// For now this is set to a year in milliseconds
pub const ENTRY_TTL: u64 = 365 * 86400000;

pub const BUCKET_LOW: f64 = 0.5;

pub const BUCKET_HIGH: f64 = 1.5;

pub const MIN_SSTABLE_SIZE: usize = 32;

pub const MIN_TRESHOLD: usize = 4;

pub const MAX_TRESHOLD: usize = 32;

pub const DEFAULT_ALLOW_PREFETCH: bool = true;

pub const DEFAULT_PREFETCH_SIZE: usize = 32;

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


