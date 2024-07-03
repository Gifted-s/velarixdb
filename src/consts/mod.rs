use crate::db::SizeUnit;

pub const KB: usize = 1024;

pub const DEFAULT_FLUSH_SIGNAL_CHANNEL_SIZE: usize = 1;

pub const DEFAULT_MAX_WRITE_BUFFER_NUMBER: usize = 2;

pub const DEFAULT_FALSE_POSITIVE_RATE: f64 = 1e-300;

pub const VALUE_LOG_DIRECTORY_NAME: &str = "v_log";

pub const BUCKETS_DIRECTORY_NAME: &str = "buckets";

pub const BUCKET_DIRECTORY_PREFIX: &str = "bucket";

pub const VLOG_FILE_NAME: &str = "val_log.bin";

pub const FILTER_FILE_NAME: &str = "filter";

pub const DATA_FILE_NAME: &str = "data";

pub const META_FILE_NAME: &str = "meta";

pub const SUMMARY_FILE_NAME: &str = "summary";

pub const INDEX_FILE_NAME: &str = "index";

pub const DEFAULT_DB_NAME: &str = "vikings";

pub const META_DIRECTORY_NAME: &str = "meta";

pub const TOMB_STONE_MARKER: &str = "*";

/// TODO: Many lightweight computations here, benchmark against Lazy initialization

/// 1KB
pub static GC_CHUNK_SIZE: usize = SizeUnit::Kilobytes.to_bytes(1);

/// 50KB
pub const WRITE_BUFFER_SIZE: usize = SizeUnit::Kilobytes.to_bytes(50);

/// 5 days
pub const DEFAULT_TOMBSTONE_COMPACTION_INTERVAL: std::time::Duration = std::time::Duration::from_millis(5 * 86400000);

// 1 Hour
pub const DEFAULT_COMPACTION_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1 * 1000 * 60 * 60);

/// 1 Min
pub const DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL: std::time::Duration = std::time::Duration::from_millis(1000 * 60);

/// 10 hours
pub const DEFAULT_ONLINE_GC_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10 * 1000 * 60 * 60);

/// If entry TTL enabled, it is automatically deleted after 1 year
pub const ENTRY_TTL: std::time::Duration = std::time::Duration::from_millis(365 * 86400000);

/// Tombstone should only be removed after 120 days to guarantee that obsolete data don't
/// resurrect by prematurelly deleting tombstone
pub const DEFAULT_TOMBSTONE_TTL: std::time::Duration = std::time::Duration::from_millis(120 * 86400000);

pub const DEFAULT_ENABLE_TTL: bool = false;

pub const BUCKET_LOW: f64 = 0.5;

pub const BUCKET_HIGH: f64 = 1.5;

pub const MIN_SSTABLE_SIZE: usize = SizeUnit::Kilobytes.to_bytes(4);

pub const MIN_TRESHOLD: usize = 4;

pub const MAX_TRESHOLD: usize = 32;

pub const DEFAULT_ALLOW_PREFETCH: bool = true;

pub const DEFAULT_PREFETCH_SIZE: usize = 10;

pub const EOF: &str = "EOF";

pub const HEAD_ENTRY_KEY: &[u8; 4] = b"head";

pub const TAIL_ENTRY_KEY: &[u8; 4] = b"tail";

pub const HEAD_ENTRY_VALUE: &[u8; 4] = b"head";

pub const TAIL_ENTRY_VALUE: &[u8; 4] = b"tail";

pub const SIZE_OF_USIZE: usize = std::mem::size_of::<usize>();

pub const SIZE_OF_U32: usize = std::mem::size_of::<u32>();

pub const SIZE_OF_U64: usize = std::mem::size_of::<u64>();

pub const SIZE_OF_U8: usize = std::mem::size_of::<u8>();

pub const FLUSH_SIGNAL: u8 = 1;

pub const BLOCK_SIZE: usize = 4 * 1024; // 4KB

pub const VLOG_START_OFFSET: usize = 0;
