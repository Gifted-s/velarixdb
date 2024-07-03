/// Compaction involves merging multiple SSTables into a new, optimized one. During this process, VikingsDB considers both data and tombstones.
///
/// Expired Tombstones: If a tombstone's timestamp is older than a specific threshold (defined by `tombstone_ttl``), it's considered expired.
/// These expired tombstones are removed entirely during compaction, freeing up disk space.
///
/// Unexpired Tombstones: If a tombstone is not expired, it means the data it shadows might still be relevant on other tiers.  In
/// this case, VikingsDB keeps both the tombstone and the data in the new SSTable. This ensures consistency across the tiers and allows for repairs if needed.
use crate::bucket::InsertableToBucket;
use crate::types::{BloomFilterHandle, Bool, BucketMapHandle, FlushReceiver, KeyRangeHandle};
use crate::{err::Error, filter::BloomFilter};
use futures::lock::Mutex;
use std::sync::Arc;
use tokio::time::sleep;
use Error::*;

#[derive(Debug, Clone)]
pub struct Compactor {
    pub config: Config,

    /// compaction reason (manual or automated)
    pub reason: CompactionReason,

    ///  is compaction active or sleeping
    pub is_active: Arc<Mutex<CompState>>,
}

#[derive(Debug, Clone)]
pub struct Config {
    /// should compactor remove entry that has exceeded time to live?
    pub use_ttl: Bool,

    /// entry expected time to live
    pub entry_ttl: std::time::Duration,

    /// tombstone expected time to live
    pub tombstone_ttl: std::time::Duration,

    /// interval to listen for flush event
    pub flush_listener_interval: std::time::Duration,

    /// interval to trigger background compaction
    pub background_interval: std::time::Duration,

    /// interval to trigger background tombstone compaction
    pub tombstone_compaction_interval: std::time::Duration,

    /// compaction strategy
    pub strategy: Strategy,

    pub filter_false_positive: f64,
}
impl Config {
    pub fn new(
        use_ttl: Bool,
        entry_ttl: std::time::Duration,
        tombstone_ttl: std::time::Duration,
        flush_listener_interval: std::time::Duration,
        background_interval: std::time::Duration,
        tombstone_compaction_interval: std::time::Duration,
        strategy: Strategy,
        filter_false_positive: f64,
    ) -> Self {
        Config {
            use_ttl,
            entry_ttl,
            tombstone_ttl,
            flush_listener_interval,
            background_interval,
            tombstone_compaction_interval,
            strategy,
            filter_false_positive,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Strategy {
    STCS,
    LCS, // TODO
    ICS, // TODO
    TCS, // TODO
    UCS, // TODO
}

#[derive(Debug, Clone)]
pub enum CompState {
    Sleep,
    Active,
}

#[derive(Debug, Clone)]
pub enum CompactionReason {
    MaxSize,
    Manual,
}

pub struct WriteTracker {
    pub actual: usize,
    pub expected: usize,
}
impl WriteTracker {
    pub fn new(expected: usize) -> Self {
        Self { actual: 0, expected }
    }
}

#[derive(Debug, Clone)]
pub struct MergePointer {
    pub ptr1: usize,
    pub ptr2: usize,
}
impl MergePointer {
    pub fn new() -> Self {
        Self { ptr1: 0, ptr2: 0 }
    }
    pub fn increment_ptr1(&mut self) {
        self.ptr1 += 1;
    }
    pub fn increment_ptr2(&mut self) {
        self.ptr2 += 1;
    }
}

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
    pub fn new(sstable: Box<dyn InsertableToBucket>, filter: BloomFilter, hotness: u64) -> Self {
        Self {
            sstable,
            hotness,
            filter,
        }
    }
}

impl Compactor {
    pub fn new(
        use_ttl: Bool,
        entry_ttl: std::time::Duration,
        tombstone_ttl: std::time::Duration,
        background_interval: std::time::Duration,
        flush_listener_interval: std::time::Duration,
        tombstone_compaction_interval: std::time::Duration,
        strategy: Strategy,
        reason: CompactionReason,
        filter_false_positive: f64,
    ) -> Self {
        Self {
            is_active: Arc::new(Mutex::new(CompState::Sleep)),
            reason,
            config: Config::new(
                use_ttl,
                entry_ttl,
                tombstone_ttl,
                flush_listener_interval,
                background_interval,
                tombstone_compaction_interval,
                strategy,
                filter_false_positive,
            ),
        }
    }
    /// FUTURE: Maybe trigger tombstone compaction on interval in addtion to normal periodic sstable compaction
    #[allow(unused_variables, dead_code)]
    pub fn tombstone_compaction_condition_background_checker(
        &self,
        bucket_map: BucketMapHandle,
        filter: BloomFilterHandle,
        key_range: KeyRangeHandle,
    ) {
        let cfg = self.config.to_owned();
        tokio::spawn(async move {
            loop {
                Compactor::sleep_compaction(cfg.tombstone_compaction_interval).await;
            }
        });
    }

    pub fn start_flush_listener(
        &self,
        flush_rx: FlushReceiver,
        bucket_map: BucketMapHandle,
        filter: BloomFilterHandle,
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
                                log::error!("{}", FlushSignalChannelOverflowError)
                            }
                            async_broadcast::TryRecvError::Closed => {
                                log::error!("{}", FlushSignalChannelClosedError)
                            }
                            async_broadcast::TryRecvError::Empty => {}
                        }
                        continue;
                    }
                    *state = CompState::Active;
                    drop(state);
                    if let Err(err) =
                        Compactor::handle_compaction(bucket_map.clone(), filter.clone(), key_range.clone(), &cfg).await
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

    pub fn start_periodic_background_compaction(
        &self,
        buckets: BucketMapHandle,
        filter: BloomFilterHandle,
        key_range: KeyRangeHandle,
    ) {
        let cfg = self.config.to_owned();
        let comp_state = Arc::clone(&self.is_active);
        tokio::spawn(async move {
            loop {
                Compactor::sleep_compaction(cfg.background_interval).await;
                let mut state = comp_state.lock().await;
                if let CompState::Sleep = *state {
                    *state = CompState::Active;
                    drop(state);
                    if let Err(err) = Compactor::handle_compaction(
                        Arc::clone(&buckets),
                        Arc::clone(&filter),
                        Arc::clone(&key_range),
                        &cfg,
                    )
                    .await
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
        filter: BloomFilterHandle,
        key_range: KeyRangeHandle,
        cfg: &Config,
    ) -> Result<(), Error> {
        match cfg.strategy {
            Strategy::STCS => {
                let mut runner = super::sized::SizedTierRunner::new(
                    Arc::clone(&buckets),
                    Arc::clone(&filter),
                    Arc::clone(&key_range),
                    cfg,
                );
                return runner.run_compaction().await;
            }
            Strategy::LCS => {
                log::info!("LCS not curently supported, try SCS instead");
                return Ok(());
            }
            Strategy::ICS => {
                log::info!("ICS not curently supported, try SCS instead");
                return Ok(());
            }
            Strategy::TCS => {
                log::info!("TCS not curently supported, try SCS instead");
                return Ok(());
            }
            Strategy::UCS => {
                log::info!("UCS not curently supported, try SCS instead");
                return Ok(());
            }
        }
    }

    async fn sleep_compaction(duration: std::time::Duration) {
        sleep(duration).await;
    }
}
