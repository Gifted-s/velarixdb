use crate::consts::{
    DEFAULT_ALLOW_PREFETCH, DEFAULT_FALSE_POSITIVE_RATE,
    DEFAULT_MAJOR_GARBAGE_COLLECTION_INTERVAL_MILLI, DEFAULT_MAX_WRITE_BUFFER_NUMBER,
    DEFAULT_PREFETCH_SIZE, DEFUALT_ENABLE_TTL, ENTRY_TTL, GC_THREAD_COUNT, WRITE_BUFFER_SIZE,
};

#[derive(Clone, Debug)]
/// Configuration options for the storage engine.
pub struct Config {
    /// False positive rate for the Bloom filter. The lower the value, the more accurate,
    /// but it incurs extra cost on the CPU.
    pub false_positive_rate: f64,

    /// Should we delete entries that have exceeded their time to live (TTL)?
    pub enable_ttl: bool,

    /// Time for an entry to exist before it is removed automatically (in milliseconds).
    pub entry_ttl_millis: u64,

    /// Should we prefetch upcoming values in case of range queries?
    pub allow_prefetch: bool,

    /// How many keys should we prefetch in case of range queries?
    pub prefetch_size: usize,

    /// The size of each memtable
    pub write_buffer_size: usize,

    /// How many memtables should we have
    pub max_buffer_write_number: usize,

    pub major_garbage_collection_interval: u64,
}
impl Config {
    pub fn new(
        false_positive_rate: f64,
        enable_ttl: bool,
        entry_ttl_millis: u64,
        allow_prefetch: bool,
        prefetch_size: usize,
        write_buffer_size: usize,
        max_buffer_write_number: usize,
        major_garbage_collection_interval: u64,
    ) -> Self {
        Self {
            false_positive_rate,
            enable_ttl,
            entry_ttl_millis,
            allow_prefetch,
            prefetch_size,
            max_buffer_write_number,
            write_buffer_size,
            major_garbage_collection_interval,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            false_positive_rate: DEFAULT_FALSE_POSITIVE_RATE,
            enable_ttl: DEFUALT_ENABLE_TTL,
            entry_ttl_millis: ENTRY_TTL, // 1 year
            allow_prefetch: DEFAULT_ALLOW_PREFETCH,
            prefetch_size: DEFAULT_PREFETCH_SIZE,
            max_buffer_write_number: DEFAULT_MAX_WRITE_BUFFER_NUMBER,
            write_buffer_size: WRITE_BUFFER_SIZE,
            major_garbage_collection_interval: DEFAULT_MAJOR_GARBAGE_COLLECTION_INTERVAL_MILLI,
        }
    }
}
