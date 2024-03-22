use crate::consts::{
    DEFAULT_ALLOW_PREFETCH, DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MAX_WRITE_BUFFER_NUMBER,
    DEFAULT_PREFETCH_SIZE, DEFUALT_ENABLE_TTL, ENTRY_TTL, GC_THREAD_COUNT, WRITE_BUFFER_SIZE,
};

#[derive(Clone, Debug)]
/// Configuration options for the storage engine.
pub struct Config {
    /// Number of threads to be used to handle garbage collection.
    /// Tokio::task is used, which functions like a lightweight thread and
    /// multiplexed to the OS threads at runtime.
    pub gc_thread_count: u32,

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
}
impl Config {
    pub fn new(
        gc_thread_count: u32,
        false_positive_rate: f64,
        enable_ttl: bool,
        entry_ttl_millis: u64,
        allow_prefetch: bool,
        prefetch_size: usize,
        write_buffer_size: usize,
        max_buffer_write_number: usize,
    ) -> Self {
        Self {
            gc_thread_count,
            false_positive_rate,
            enable_ttl,
            entry_ttl_millis,
            allow_prefetch,
            prefetch_size,
            max_buffer_write_number,
            write_buffer_size,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            gc_thread_count: GC_THREAD_COUNT,
            false_positive_rate: DEFAULT_FALSE_POSITIVE_RATE,
            enable_ttl: DEFUALT_ENABLE_TTL,
            entry_ttl_millis: ENTRY_TTL, // 1 year
            allow_prefetch: DEFAULT_ALLOW_PREFETCH,
            prefetch_size: DEFAULT_PREFETCH_SIZE,
            max_buffer_write_number: DEFAULT_MAX_WRITE_BUFFER_NUMBER,
            write_buffer_size: WRITE_BUFFER_SIZE,
        }
    }
}
