use crate::{
    compactors,
    consts::{
        DEFAULT_ALLOW_PREFETCH, DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL, DEFAULT_COMPACTION_INTERVAL,
        DEFAULT_ENABLE_TTL, DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MAX_WRITE_BUFFER_NUMBER, DEFAULT_ONLINE_GC_INTERVAL,
        DEFAULT_PREFETCH_SIZE, DEFAULT_TOMBSTONE_COMPACTION_INTERVAL, DEFAULT_TOMBSTONE_TTL, ENTRY_TTL, GC_CHUNK_SIZE,
        WRITE_BUFFER_SIZE,
    },
};
use crate::{
    db::{DataStore, SizeUnit},
    types::Key,
};
use std::time::Duration;

#[derive(Clone, Debug)]
/// Configuration for  data store.
pub struct Config {
    /// False positive rate for the Bloom filter. The lower the value, the more accurate,
    /// but it incurs extra cost on the CPU for more accuracy.
    pub false_positive_rate: f64,

    /// Should we prefetch upcoming values in case of range queries?
    pub allow_prefetch: bool,

    /// How many keys should we prefetch in case of range queries?
    pub prefetch_size: usize,

    /// The size of each memtable in bytes
    pub write_buffer_size: usize,

    /// How many memtables should we have
    pub max_buffer_write_number: usize,

    /// Should we delete entries that have exceeded their time to live (TTL)?
    pub enable_ttl: bool,

    /// Time for an entry to exist before it is removed automatically.
    pub entry_ttl: std::time::Duration,

    /// Time for a tombstone to exist before it is removed automatically
    pub tombstone_ttl: std::time::Duration,

    /// Interval at which compaction checks if a flush has been made
    pub compactor_flush_listener_interval: std::time::Duration,

    /// Interval at which background compaction is triggered
    pub background_compaction_interval: std::time::Duration,

    /// Interval at which tombstone compaction is triggered
    pub tombstone_compaction_interval: std::time::Duration,

    /// Which compaction strategy is used STCS, LCS, ICS, TCS or UCS
    pub compaction_strategy: compactors::Strategy,

    /// Interval at which tombstone compaction is triggered
    pub online_gc_interval: std::time::Duration,

    /// How many bytes should be checked in value log for garbage collection in kilobytes
    pub gc_chunk_size: usize,

    /// Maximum number of files that can be opened at once
    pub open_files_limit: usize,
}

fn get_open_file_limit() -> usize {
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    return 900;

    #[cfg(target_os = "windows")]
    return 400;

    #[cfg(target_os = "macos")]
    return 150;
}


impl Default for Config {
    fn default() -> Self {
        Config {
            false_positive_rate: DEFAULT_FALSE_POSITIVE_RATE,
            enable_ttl: DEFAULT_ENABLE_TTL,
            entry_ttl: ENTRY_TTL,
            allow_prefetch: DEFAULT_ALLOW_PREFETCH,
            prefetch_size: DEFAULT_PREFETCH_SIZE,
            max_buffer_write_number: DEFAULT_MAX_WRITE_BUFFER_NUMBER,
            write_buffer_size: WRITE_BUFFER_SIZE,
            compactor_flush_listener_interval: DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL,
            background_compaction_interval: DEFAULT_COMPACTION_INTERVAL,
            tombstone_ttl: DEFAULT_TOMBSTONE_TTL,
            tombstone_compaction_interval: DEFAULT_TOMBSTONE_COMPACTION_INTERVAL,
            compaction_strategy: compactors::Strategy::STCS,
            online_gc_interval: DEFAULT_ONLINE_GC_INTERVAL,
            gc_chunk_size: GC_CHUNK_SIZE,
            open_files_limit: get_open_file_limit(),
        }
    }
}

impl DataStore<'static, Key> {
    /// Sets the false positive rate for the DataStore.
    /// The rate must be greater than 0.0.
    pub fn with_false_positive_rate(mut self, rate: f64) -> Self {
        assert!(rate > 0.0, "false_positive_rate must be greater than 0.0");
        self.config.false_positive_rate = rate;
        self
    }

    /// Enables or disables prefetching.
    pub fn with_allow_prefetch(mut self, allow: bool) -> Self {
        self.config.allow_prefetch = allow;
        self
    }

    /// Sets the prefetch size if prefetching is allowed.
    /// The size must be greater than 0 if allow_prefetch is set to true.
    pub fn with_prefetch_size(mut self, size: usize) -> Self {
        assert!(
            (self.config.allow_prefetch && size > 0),
            "prefetch_size should be greater than 0 if allow_prefetch is set to true"
        );
        self.config.prefetch_size = size;
        self
    }

    /// Sets the write buffer size in kilobytes.
    /// The size must be at least 50 kilobytes.
    pub fn with_write_buffer_size(mut self, size: usize) -> Self {
        assert!(size >= 50, "write_buffer_size should not be less than 50 Kilobytes");
        self.config.write_buffer_size = SizeUnit::Kilobytes.as_bytes(size);
        self
    }

    /// Sets the maximum number of buffer writes.
    /// The number must be greater than 0.
    pub fn with_max_buffer_write_number(mut self, number: usize) -> Self {
        assert!(number > 0, "max_buffer_write_number should be greater zero");
        self.config.max_buffer_write_number = number;
        self
    }

    /// Enables or disables TTL (Time-To-Live) for entries.
    pub fn with_enable_ttl(mut self, enable: bool) -> Self {
        self.config.enable_ttl = enable;
        self
    }

    /// Sets the TTL for entries if TTL is enabled.
    /// The TTL must be at least 3 days if enable_ttl is set to true.
    pub fn with_entry_ttl(mut self, ttl: std::time::Duration) -> Self {
        assert!(
            (self.config.enable_ttl && ttl.as_millis() >= Duration::from_secs(3 * 24 * 60 * 60).as_millis()),
            "entry_ttl_millis cannot be less than 3 days if enable_ttl is set to true"
        );
        self.config.entry_ttl = ttl;
        self
    }

    /// Sets the TTL for tombstones.
    /// The TTL must be at least 10 days to prevent resurrecting entries marked deleted.
    pub fn with_tombstone_ttl(mut self, ttl: std::time::Duration) -> Self {
        assert!(
            ttl.as_millis() >= Duration::from_secs(10 * 24 * 60 * 60).as_millis(),
            "tombstone_ttl should not be less than 10 days to prevent resurrecting entries marked deleted"
        );
        self.config.tombstone_ttl = ttl;
        self
    }

    /// Sets the interval for compactor flush listener.
    /// The interval must be at least 2 minutes to prevent overloading the system.
    pub fn with_compactor_flush_listener_interval(mut self, interval: std::time::Duration) -> Self {
        assert!(
            interval.as_millis() >= Duration::from_secs(2 * 60).as_millis(),
            "compactor_flush_listener_interval should not be less than 2 minutes, to prevent overloading the system"
        );
        self.config.compactor_flush_listener_interval = interval;
        self
    }

    /// Sets the interval for background compaction.
    /// The interval must be at least 5 minutes to prevent overloads.
    pub fn with_background_compaction_interval(mut self, interval: std::time::Duration) -> Self {
        assert!(
            interval.as_millis() >= Duration::from_secs(5 * 60).as_millis(),
            "background_compaction_interval should not be less than 5 minutes to prevent overloads"
        );
        self.config.background_compaction_interval = interval;
        self
    }

    /// Sets the interval for tombstone compaction.
    /// The interval must be at least 10 days.
    pub fn with_tombstone_compaction_interval(mut self, interval: std::time::Duration) -> Self {
        assert!(
            interval.as_millis() >= Duration::from_secs(10 * 24 * 60 * 60).as_millis(),
            "tombstone_compaction_interval should not be less than 10 days"
        );
        self.config.tombstone_compaction_interval = interval;
        self
    }

    /// Sets the compaction strategy.
    pub fn with_compaction_strategy(mut self, strategy: compactors::Strategy) -> Self {
        self.config.compaction_strategy = strategy;
        self
    }

    /// Sets the interval for online garbage collection.
    /// The interval must be at least 1 hour.
    pub fn with_online_gc_interval(mut self, interval: std::time::Duration) -> Self {
        assert!(
            interval.as_secs() >= Duration::from_secs(60 * 60).as_secs(),
            "online_gc_interval should not be less than 1 hour"
        );
        self.config.online_gc_interval = interval;
        self
    }

    /// Sets the with_gc_chunk_size in kilobytes i.e offsets in
    /// vlog file to scan for garbage collection.
    /// The size must be at least 50 kilobytes.
    pub fn with_gc_chunk_size(mut self, size: usize) -> Self {
        assert!(size >= 50, "gc_chunk_size should not be less than 50 Kilobyte");
        self.config.gc_chunk_size = SizeUnit::Kilobytes.as_bytes(size);
        self
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::cfg::Config;

    use super::*;
    use std::time::Duration;

    async fn create_datastore() -> DataStore<'static, Key> {
        let root = tempdir().unwrap();
        let path = root.path().join("store_test_3");
        let mut store = DataStore::open("test", path).await.unwrap();
        // Initialize with default or dummy values
        let config = Config {
            false_positive_rate: 0.01,
            allow_prefetch: false,
            prefetch_size: 0,
            write_buffer_size: 51200,
            max_buffer_write_number: 1,
            enable_ttl: false,
            entry_ttl: Duration::from_secs(0),
            tombstone_ttl: Duration::from_secs(0),
            compactor_flush_listener_interval: Duration::from_secs(0),
            background_compaction_interval: Duration::from_secs(0),
            tombstone_compaction_interval: Duration::from_secs(0),
            compaction_strategy: compactors::Strategy::STCS,
            online_gc_interval: Duration::from_secs(0),
            gc_chunk_size: 51200,
            open_files_limit: 150,
        };
        store.config = config;
        store
    }

    #[tokio::test]
    #[should_panic(expected = "false_positive_rate must be greater than 0.0")]
    async fn test_with_false_positive_rate_invalid() {
        let ds = create_datastore().await;
        ds.with_false_positive_rate(0.0);
    }

    #[tokio::test]
    async fn test_with_false_positive_rate() {
        let ds = create_datastore().await;
        let ds = ds.with_false_positive_rate(0.05);
        assert_eq!(ds.config.false_positive_rate, 0.05);
    }

    #[tokio::test]
    async fn test_with_allow_prefetch() {
        let ds = create_datastore().await;
        let ds = ds.with_allow_prefetch(true);
        assert!(ds.config.allow_prefetch);
    }

    #[tokio::test]
    #[should_panic(expected = "prefetch_size should be greater than 0 if allow_prefetch is set to true")]
    async fn test_with_prefetch_size_invalid() {
        let ds = create_datastore().await.with_allow_prefetch(true);
        ds.with_prefetch_size(0);
    }

    #[tokio::test]
    async fn test_with_prefetch_size() {
        let ds = create_datastore().await.with_allow_prefetch(true);
        let ds = ds.with_prefetch_size(10);
        assert_eq!(ds.config.prefetch_size, 10);
    }

    #[tokio::test]
    #[should_panic(expected = "write_buffer_size should not be less than 50 Kilobytes")]
    async fn test_with_write_buffer_size_invalid() {
        let ds = create_datastore().await;
        ds.with_write_buffer_size(49);
    }

    #[tokio::test]
    async fn test_with_write_buffer_size() {
        let ds = create_datastore().await;
        let ds = ds.with_write_buffer_size(100);
        assert_eq!(ds.config.write_buffer_size, SizeUnit::Kilobytes.as_bytes(100));
    }

    #[tokio::test]
    #[should_panic(expected = "max_buffer_write_number should be greater zero")]
    async fn test_with_max_buffer_write_number_invalid() {
        let ds = create_datastore().await;
        ds.with_max_buffer_write_number(0);
    }

    #[tokio::test]
    async fn test_with_max_buffer_write_number() {
        let ds = create_datastore().await;
        let ds = ds.with_max_buffer_write_number(5);
        assert_eq!(ds.config.max_buffer_write_number, 5);
    }

    #[tokio::test]
    async fn test_with_enable_ttl() {
        let ds = create_datastore().await;
        let ds = ds.with_enable_ttl(true);
        assert!(ds.config.enable_ttl);
    }

    #[tokio::test]
    #[should_panic(expected = "entry_ttl_millis cannot be less than 3 days if enable_ttl is set to true")]
    async fn test_with_entry_ttl_invalid() {
        let ds = create_datastore().await.with_enable_ttl(true);
        ds.with_entry_ttl(Duration::from_secs(24 * 60 * 60)); // 1 day
    }

    #[tokio::test]
    async fn test_with_entry_ttl() {
        let ds = create_datastore().await.with_enable_ttl(true);
        let ds = ds.with_entry_ttl(Duration::from_secs(4 * 24 * 60 * 60)); // 4 days
        assert_eq!(ds.config.entry_ttl, Duration::from_secs(4 * 24 * 60 * 60));
    }

    #[tokio::test]
    #[should_panic(
        expected = "tombstone_ttl should not be less than 10 days to prevent resurrecting entries marked deleted"
    )]
    async fn test_with_tombstone_ttl_invalid() {
        let ds = create_datastore();
        ds.await.with_tombstone_ttl(Duration::from_secs(9 * 24 * 60 * 60)); // 9 days
    }

    #[tokio::test]
    async fn test_with_tombstone_ttl() {
        let ds = create_datastore();
        let ds = ds.await.with_tombstone_ttl(Duration::from_secs(15 * 24 * 60 * 60)); // 15 days
        assert_eq!(ds.config.tombstone_ttl, Duration::from_secs(15 * 24 * 60 * 60));
    }

    #[tokio::test]
    #[should_panic(
        expected = "compactor_flush_listener_interval should not be less than 2 minutes, to prevent overloading the system"
    )]
    async fn test_with_compactor_flush_listener_interval_invalid() {
        let ds = create_datastore().await;
        ds.with_compactor_flush_listener_interval(Duration::from_secs(60)); // 1 minute
    }

    #[tokio::test]
    async fn test_with_compactor_flush_listener_interval() {
        let ds = create_datastore().await;
        let ds = ds.with_compactor_flush_listener_interval(Duration::from_secs(3 * 60)); // 3 minutes
        assert_eq!(ds.config.compactor_flush_listener_interval, Duration::from_secs(3 * 60));
    }

    #[tokio::test]
    #[should_panic(expected = "background_compaction_interval should not be less than 5 minutes to prevent overloads")]
    async fn test_with_background_compaction_interval_invalid() {
        let ds = create_datastore().await;
        ds.with_background_compaction_interval(Duration::from_secs(4 * 60)); // 4 minutes
    }

    #[tokio::test]
    async fn test_with_background_compaction_interval() {
        let ds = create_datastore().await;
        let ds = ds.with_background_compaction_interval(Duration::from_secs(6 * 60)); // 6 minutes
        assert_eq!(ds.config.background_compaction_interval, Duration::from_secs(6 * 60));
    }

    #[tokio::test]
    #[should_panic(expected = "tombstone_compaction_interval should not be less than 10 days")]
    async fn test_with_tombstone_compaction_interval_invalid() {
        let ds = create_datastore().await;
        ds.with_tombstone_compaction_interval(Duration::from_secs(9 * 24 * 60 * 60));
        // 9 days
    }

    #[tokio::test]
    async fn test_with_tombstone_compaction_interval() {
        let ds = create_datastore().await;
        let ds = ds.with_tombstone_compaction_interval(Duration::from_secs(11 * 24 * 60 * 60)); // 11 days
        assert_eq!(
            ds.config.tombstone_compaction_interval,
            Duration::from_secs(11 * 24 * 60 * 60)
        );
    }

    #[tokio::test]
    async fn test_with_compaction_strategy() {
        let ds = create_datastore().await;
        let strategy = compactors::Strategy::STCS; // Replace with actual strategy
        let ds = ds.with_compaction_strategy(strategy);
        assert_eq!(ds.config.compaction_strategy, compactors::Strategy::STCS);
    }

    #[tokio::test]
    #[should_panic(expected = "online_gc_interval should not be less than 1 hour")]
    async fn test_with_online_gc_interval_invalid() {
        let ds = create_datastore().await;
        ds.with_online_gc_interval(Duration::from_secs(30 * 60)); // 30 minutes
    }

    #[tokio::test]
    async fn test_with_online_gc_interval() {
        let ds = create_datastore().await;
        let ds = ds.with_online_gc_interval(Duration::from_secs(2 * 60 * 60)); // 2 hours
        assert_eq!(ds.config.online_gc_interval, Duration::from_secs(2 * 60 * 60));
    }

    #[tokio::test]
    #[should_panic(expected = "gc_chunk_size should not be less than 50 Kilobyte")]
    async fn test_with_gc_chunk_size_invalid() {
        let ds = create_datastore().await;
        ds.with_gc_chunk_size(49);
    }

    #[tokio::test]
    async fn test_with_gc_chunk_size() {
        let ds = create_datastore().await;
        let ds = ds.with_gc_chunk_size(100);
        assert_eq!(ds.config.gc_chunk_size, SizeUnit::Kilobytes.as_bytes(100));
    }
}
