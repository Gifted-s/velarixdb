use crate::{
    compactors,
    consts::{
        DEFAULT_ALLOW_PREFETCH,
        DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL_MILLI,
        DEFAULT_COMPACTION_INTERVAL_MILLI, DEFAULT_FALSE_POSITIVE_RATE,
        DEFAULT_MAJOR_GARBAGE_COLLECTION_INTERVAL_MILLI,
        DEFAULT_MAX_WRITE_BUFFER_NUMBER, DEFAULT_PREFETCH_SIZE,
        DEFAULT_TOMBSTONE_COMPACTION_INTERVAL_MILLI, DEFAULT_TOMBSTONE_TTL,
        DEFUALT_ENABLE_TTL, ENTRY_TTL, WRITE_BUFFER_SIZE,
    },
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

    /// Time for a tombstone to exist before it is removed automatically (in milliseconds).
    pub tombstone_ttl: u64,

    /// Should we prefetch upcoming values in case of range queries?
    pub allow_prefetch: bool,

    /// How many keys should we prefetch in case of range queries?
    pub prefetch_size: usize,

    /// The size of each memtable
    pub write_buffer_size: usize,

    /// How many memtables should we have
    pub max_buffer_write_number: usize,

    pub major_garbage_collection_interval: u64,

    pub compactor_flush_listener_interval: u64,

    pub background_compaction_interval: u64,

    pub tombstone_compaction_interval: u64,

    pub compaction_strategy: compactors::Strategy,
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
        compactor_flush_listener_interval: u64,
        background_compaction_interval: u64,
        tombstone_ttl: u64,
        tombstone_compaction_interval: u64,
        compaction_strategy: compactors::Strategy,
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
            compactor_flush_listener_interval,
            background_compaction_interval,
            tombstone_ttl,
            tombstone_compaction_interval,
            compaction_strategy,
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
            major_garbage_collection_interval:
                DEFAULT_MAJOR_GARBAGE_COLLECTION_INTERVAL_MILLI,
            compactor_flush_listener_interval:
                DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL_MILLI,
            background_compaction_interval: DEFAULT_COMPACTION_INTERVAL_MILLI,
            tombstone_ttl: DEFAULT_TOMBSTONE_TTL,
            tombstone_compaction_interval:
                DEFAULT_TOMBSTONE_COMPACTION_INTERVAL_MILLI,
            compaction_strategy: compactors::Strategy::STCS,
        }
    }
}
