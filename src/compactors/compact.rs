use crate::bucket::InsertableToBucket;
use crate::types::{Bool, BucketMapHandle, FlushReceiver, KeyRangeHandle};
use crate::{err::Error, filter::BloomFilter};
use std::sync::Arc;
use std::time;
use tokio::sync::Mutex;
use tokio::time::sleep;
use Error::*;

/// `Compactor` is responsible for merging SSTables together.
///
/// During this process, it handles obsolete entries and tombstones (markers for deleted entries) as follows:
///
/// - **Expired Tombstones**: If a tombstone's timestamp is older than a specified threshold (defined by `tombstone_ttl`), it's considered expired.
///   These expired tombstones are removed entirely during compaction, freeing up disk space.
///
/// - **Unexpired Tombstones**: If a tombstone is not expired, it means the data it shadows might still be relevant in other tiers.
///   In this case, velarixDB keeps both the tombstone and the data in the new SSTable. This ensures consistency across tiers and allows for repairs if needed.
///
/// Currently, only the Sized-Tier Compaction Strategy (STCS) is supported. However, support for Leveled Compaction (LCS), Time-Window Compaction (TWCS), and Unified Compaction (UCS) strategies is planned.
#[derive(Debug, Clone)]
pub struct Compactor {
    pub config: Config,

    /// Compaction reason (manual or automated)
    pub reason: CompactionReason,

    /// Is compaction active or sleeping
    pub is_active: Arc<Mutex<CompState>>,
}

/// Compactor configuration
#[derive(Debug, Clone)]
pub struct Config {
    /// should compactor remove entry that has exceeded time to live?
    pub(crate) use_ttl: Bool,

    /// entry expected time to live
    pub(crate) entry_ttl: std::time::Duration,

    /// tombstone expected time to live
    pub(crate) tombstone_ttl: std::time::Duration,

    /// interval to listen for flush event
    pub(crate) flush_listener_interval: std::time::Duration,

    /// interval to trigger background compaction
    pub(crate) background_interval: std::time::Duration,

    /// interval to trigger background tombstone compaction
    pub(crate) tombstone_compaction_interval: std::time::Duration,

    /// compaction strategy
    pub(crate) strategy: Strategy,

    pub(crate) filter_false_positive: f64,
}

/// Groups TTL params
#[derive(Debug, Clone)]
pub struct TtlParams {
    pub entry_ttl: time::Duration,
    pub tombstone_ttl: time::Duration,
}

/// Groups Interval params
#[derive(Debug, Clone)]
pub struct IntervalParams {
    pub background_interval: time::Duration,
    pub flush_listener_interval: time::Duration,
    pub tombstone_compaction_interval: time::Duration,
}

/// Supported Compaction strategies
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Strategy {
    STCS,
    // LCS,  TODO
    // TCS,  TODO
    // UCS,  TODO
}

/// Compaction states
/// `Sleep`` means nothing is happening
/// `Active`` means the compaction is running
#[derive(Debug, Clone, PartialEq)]
pub enum CompState {
    Sleep,
    Active,
}

/// Reasons for compaction 
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionReason {
    MaxSize,
    Manual,
}

/// Tracks how many sstables has been written
/// to disk during compaction
pub(crate) struct WriteTracker {
    pub actual: usize,
    pub expected: usize,
}
impl WriteTracker {
    pub fn new(expected: usize) -> Self {
        Self {
            actual: Default::default(),
            expected,
        }
    }
}

/// Pointers used during merge
#[derive(Debug, Clone)]
pub(crate) struct MergePointer {
    pub ptr1: usize,
    pub ptr2: usize,
}
impl MergePointer {
    pub fn new() -> Self {
        Self {
            ptr1: Default::default(),
            ptr2: Default::default(),
        }
    }
    pub fn increment_ptr1(&mut self) {
        self.ptr1 += 1;
    }
    pub fn increment_ptr2(&mut self) {
        self.ptr2 += 1;
    }
}

/// Merged SSTable stored here
/// before being flushed to disk
#[derive(Debug)]
pub struct MergedSSTable {
    pub sstable: Box<dyn InsertableToBucket>,
    pub hotness: u64,
    pub filter: BloomFilter,
}

impl Clone for MergedSSTable {
    fn clone(&self) -> Self {
        Self {
            sstable: Box::new(super::TableInsertor::from(self.sstable.get_entries(), &self.filter)),
            hotness: self.hotness,
            filter: self.filter.clone(),
        }
    }
}

impl MergedSSTable {
    /// Creates new `MergedSSTable`
    pub fn new(sstable: Box<dyn InsertableToBucket>, filter: BloomFilter, hotness: u64) -> Self {
        Self {
            sstable,
            hotness,
            filter,
        }
    }
}

impl Config {
    pub fn new(
        use_ttl: bool,
        ttl: TtlParams,
        intervals: IntervalParams,
        strategy: Strategy,
        filter_false_positive: f64,
    ) -> Self {
        Config {
            use_ttl,
            entry_ttl: ttl.entry_ttl,
            tombstone_ttl: ttl.tombstone_ttl,
            flush_listener_interval: intervals.flush_listener_interval,
            background_interval: intervals.background_interval,
            tombstone_compaction_interval: intervals.tombstone_compaction_interval,
            strategy,
            filter_false_positive,
        }
    }
}

impl Compactor {
    // Creates new `Compactor`
    pub fn new(
        use_ttl: bool,
        ttl: TtlParams,
        intervals: IntervalParams,
        strategy: Strategy,
        reason: CompactionReason,
        filter_false_positive: f64,
    ) -> Self {
        Self {
            is_active: Arc::new(Mutex::new(CompState::Sleep)),
            reason,
            config: Config::new(use_ttl, ttl, intervals, strategy, filter_false_positive),
        }
    }
    /// FUTURE: Explicitly trigger tombstone compaction to remove expired tombstones, although this is handled during
    /// normal compaction
    #[allow(unused_variables, dead_code)]
    pub fn tombstone_compaction_condition_background_checker(
        &self,
        bucket_map: BucketMapHandle,
        key_range: KeyRangeHandle,
    ) {
        let cfg = self.config.to_owned();
        tokio::spawn(async move {
            loop {
                Compactor::sleep_compaction(cfg.tombstone_compaction_interval).await;
            }
        });
    }

    /// Background flush listener
    ///
    /// If a flush signal has been sent then compaction handler is called
    pub fn start_flush_listener(
        &self,
        flush_rx: FlushReceiver,
        bucket_map: BucketMapHandle,
        key_range: KeyRangeHandle,
    ) {
        let mut rx = flush_rx.clone();
        let comp_state = Arc::clone(&self.is_active);
        let cfg = self.config.to_owned();
        tokio::spawn(async move {
            loop {
                Compactor::sleep_compaction(cfg.flush_listener_interval).await;
                let signal = rx.try_recv();
                let mut state = comp_state.lock().await;
                if let CompState::Sleep = *state {
                    if let Err(err) = signal {
                        drop(state);
                        match err {
                            async_broadcast::TryRecvError::Overflowed(_) => {
                                log::error!("{}", FlushSignalChannelOverflow)
                            }
                            async_broadcast::TryRecvError::Closed => {
                                log::error!("{}", FlushSignalChannelClosed)
                            }
                            async_broadcast::TryRecvError::Empty => {}
                        }
                        continue;
                    }
                    *state = CompState::Active;
                    drop(state);
                    if let Err(err) =
                        Compactor::handle_compaction(Arc::clone(&bucket_map), Arc::clone(&key_range), &cfg).await
                    {
                        log::info!("{}", Error::CompactionFailed(Box::new(err)));
                        continue;
                    }
                    let mut state = comp_state.lock().await;
                    *state = CompState::Sleep;
                }
            }
        });
        log::info!("Compactor flush listener active");
    }

    /// Background compaction runner for maintenance
    pub fn spawn_compaction_worker(&self, buckets: BucketMapHandle, key_range: KeyRangeHandle) {
        let cfg = self.config.to_owned();
        let comp_state = Arc::clone(&self.is_active);
        tokio::spawn(async move {
            loop {
                Compactor::sleep_compaction(cfg.background_interval).await;
                let mut state = comp_state.lock().await;
                if let CompState::Sleep = *state {
                    *state = CompState::Active;
                    drop(state);
                    if let Err(err) =
                        Compactor::handle_compaction(Arc::clone(&buckets), Arc::clone(&key_range), &cfg).await
                    {
                        log::info!("{}", Error::CompactionFailed(Box::new(err)))
                    }
                    let mut state = comp_state.lock().await;
                    *state = CompState::Sleep;
                }
            }
        });
    }

    pub async fn handle_compaction(
        buckets: BucketMapHandle,
        key_range: KeyRangeHandle,
        cfg: &Config,
    ) -> Result<(), Error> {
        match cfg.strategy {
            Strategy::STCS => {
                let mut runner = super::sized::SizedTierRunner::new(Arc::clone(&buckets), Arc::clone(&key_range), cfg);
                runner.run_compaction().await
            } // LCS, UCS and TWS will be added later
        }
    }

    async fn sleep_compaction(duration: std::time::Duration) {
        sleep(duration).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_comp_params_new() {
        let use_ttl = true;
        let ttl = TtlParams {
            entry_ttl: Duration::new(60, 0),
            tombstone_ttl: Duration::new(120, 0),
        };
        let intervals = IntervalParams {
            background_interval: Duration::new(30, 0),
            flush_listener_interval: Duration::new(10, 0),
            tombstone_compaction_interval: Duration::new(45, 0),
        };
        let strategy = Strategy::STCS;
        let reason = CompactionReason::MaxSize;
        let filter_false_positive = 0.01;

        let compactor = Compactor::new(
            use_ttl,
            ttl.to_owned(),
            intervals.to_owned(),
            strategy,
            reason.to_owned(),
            filter_false_positive,
        );

        assert_eq!(compactor.config.use_ttl, use_ttl);
        assert_eq!(compactor.config.entry_ttl, ttl.entry_ttl);
        assert_eq!(compactor.config.tombstone_ttl, ttl.tombstone_ttl);
        assert_eq!(compactor.config.background_interval, intervals.background_interval);
        assert_eq!(
            compactor.config.flush_listener_interval,
            intervals.flush_listener_interval
        );
        assert_eq!(
            compactor.config.tombstone_compaction_interval,
            intervals.tombstone_compaction_interval
        );
        assert_eq!(compactor.config.strategy, strategy);
        assert_eq!(compactor.reason, reason);
        assert_eq!(compactor.config.filter_false_positive, filter_false_positive);
    }
}
