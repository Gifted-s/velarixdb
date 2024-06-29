use crate::{
    compact,
    consts::{
        DEFAULT_ALLOW_PREFETCH, DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL, DEFAULT_COMPACTION_INTERVAL,
        DEFAULT_ENABLE_TTL, DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MAX_WRITE_BUFFER_NUMBER,
        DEFAULT_ONLINE_GC_INTERVAL, DEFAULT_PREFETCH_SIZE, DEFAULT_TOMBSTONE_COMPACTION_INTERVAL,
        DEFAULT_TOMBSTONE_TTL, ENTRY_TTL, GC_CHUNK_SIZE, WRITE_BUFFER_SIZE,
    },
};

#[derive(Clone, Debug)]
/// Configuration options for the storage engine.
pub struct Config {
    /// False positive rate for the Bloom filter. The lower the value, the more accurate,
    /// but it incurs extra cost on the CPU for more accuracy.
    pub false_positive_rate: f64,

    /// Should we delete entries that have exceeded their time to live (TTL)?
    pub enable_ttl: bool,

    /// Time for an entry to exist before it is removed automatically (in days).
    pub entry_ttl_millis: std::time::Duration,

    /// Time for a tombstone to exist before it is removed automatically (in days).
    pub tombstone_ttl: std::time::Duration,

    /// Should we prefetch upcoming values in case of range queries?
    pub allow_prefetch: bool,

    /// How many keys should we prefetch in case of range queries?
    pub prefetch_size: usize,

    /// The size of each memtable in bytes
    pub write_buffer_size: usize,

    /// How many memtables should we have
    pub max_buffer_write_number: usize,

    /// Interval at which compaction checks if a flush has been made (in seconds)
    pub compactor_flush_listener_interval: std::time::Duration,

    /// Interval at which background compaction is triggered (in seconds)
    pub background_compaction_interval: std::time::Duration,

    /// Interval at which tombstone compaction is triggered (in days)
    pub tombstone_compaction_interval: std::time::Duration,

    /// Which compaction strategy is used STCS, LCS, ICS, TCS or UCS
    pub compaction_strategy: compact::Strategy,

    /// Interval at which tombstone compaction is triggered (in hours)
    pub online_gc_interval: std::time::Duration,

    /// How many bytes should be checked in value log for garbage collection
    pub gc_chunk_size: usize,
}
impl Config {
    pub fn new(
        false_positive_rate: f64,
        enable_ttl: bool,
        entry_ttl_millis: std::time::Duration,
        allow_prefetch: bool,
        prefetch_size: usize,
        write_buffer_size: usize,
        max_buffer_write_number: usize,
        compactor_flush_listener_interval: std::time::Duration,
        background_compaction_interval: std::time::Duration,
        tombstone_ttl: std::time::Duration,
        tombstone_compaction_interval: std::time::Duration,
        compaction_strategy: compact::Strategy,
        online_gc_interval: std::time::Duration,
        gc_chunk_size: usize,
    ) -> Self {
        Self {
            false_positive_rate,
            enable_ttl,
            entry_ttl_millis,
            allow_prefetch,
            prefetch_size,
            max_buffer_write_number,
            write_buffer_size,
            compactor_flush_listener_interval,
            background_compaction_interval,
            tombstone_ttl,
            tombstone_compaction_interval,
            compaction_strategy,
            online_gc_interval,
            gc_chunk_size,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            false_positive_rate: DEFAULT_FALSE_POSITIVE_RATE,
            enable_ttl: DEFAULT_ENABLE_TTL,
            entry_ttl_millis: ENTRY_TTL,
            allow_prefetch: DEFAULT_ALLOW_PREFETCH,
            prefetch_size: DEFAULT_PREFETCH_SIZE,
            max_buffer_write_number: DEFAULT_MAX_WRITE_BUFFER_NUMBER,
            write_buffer_size: WRITE_BUFFER_SIZE,
            compactor_flush_listener_interval: DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL,
            background_compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            tombstone_ttl: DEFAULT_TOMBSTONE_TTL,
            tombstone_compaction_interval: DEFAULT_TOMBSTONE_COMPACTION_INTERVAL,
            compaction_strategy: compact::Strategy::STCS,
            online_gc_interval: DEFAULT_ONLINE_GC_INTERVAL,
            gc_chunk_size: GC_CHUNK_SIZE,
        }
    }
}
