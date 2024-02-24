use crate::storage_engine::SizeUnit;

pub const DEFAULT_MEMTABLE_CAPACITY: usize = SizeUnit::Kilobytes.to_bytes(1);

pub const THUMB_STONE: usize = 0;

pub const DEFAULT_FALSE_POSITIVE_RATE: f64 = 1e-100;

pub const VALUE_LOG_DIRECTORY_NAME: &str = "v_log";

pub const BUCKETS_DIRECTORY_NAME: &str = "buckets";

pub const BUCKET_DIRECTORY_PREFIX: &str = "bucket";

pub const VLOG_FILE_NAME: &str = "val_log.bin";

pub const META_DIRECTORY_NAME: &str = "meta";

pub const BUCKET_LOW: f64 = 0.5;

pub const BUCKET_HIGH: f64 = 1.5;

pub const MIN_SSTABLE_SIZE: usize = 32;

pub const MIN_TRESHOLD: usize = 4;

pub const MAX_TRESHOLD: usize = 32;

pub static DEFAULT_ALLOW_PREFETCH: bool = true;

pub static DEFAULT_PREFETCH_SIZE: usize = 32;


