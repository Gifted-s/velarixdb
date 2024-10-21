use crate::consts::FLUSH_SIGNAL;
use crate::flush::flusher::Error::FilterNotProvidedForFlush;
use crate::flush::flusher::Error::TableSummaryIsNone;
use crate::types::{self, BucketMapHandle, FlushSignal, ImmutableMemTables, KeyRangeHandle};
use crate::{err::Error, memtable::MemTable};
use std::fmt::Debug;
use std::sync::Arc;

type K = types::Key;
pub type InActiveMemtable = Arc<MemTable<K>>;

/// Responsible for flushing memtables to disk
#[derive(Debug, Clone)]
pub struct Flusher {
    pub(crate) read_only_memtable: ImmutableMemTables<K>,
    pub(crate) bucket_map: BucketMapHandle,
    pub(crate) key_range: KeyRangeHandle,
}

impl Flusher {
    pub fn new(
        read_only_memtable: ImmutableMemTables<K>,
        bucket_map: BucketMapHandle,
        key_range: KeyRangeHandle,
    ) -> Self {
        Self {
            read_only_memtable,
            bucket_map,
            key_range,
        }
    }

    /// Handles a single flush operation
    ///
    /// This method writes memtable to the right bucket and update the
    /// `KeyRange` with the new sstable
    pub async fn flush(&mut self, table: InActiveMemtable) -> Result<(), Error> {
        let flush_data = self;
        let table_reader = table;
        if table_reader.entries.is_empty() {
            return Err(Error::FailedToInsertToBucket(
                "Cannot flush an empty table".to_string(),
            ));
        }
        let mut bucket_lock = flush_data.bucket_map.write().await;
        let sst = bucket_lock
            .insert_to_appropriate_bucket(Arc::new(Box::new(table_reader.as_ref().to_owned())))
            .await?;
        drop(table_reader);
        if sst.summary.is_none() {
            return Err(TableSummaryIsNone);
        }
        if sst.filter.is_none() {
            return Err(FilterNotProvidedForFlush);
        }
        //IMPORTANT: Don't keep sst entries in memory
        sst.entries.clear();
        let summary = sst.summary.clone().unwrap();
        flush_data
            .key_range
            .set(sst.dir.to_owned(), summary.smallest_key, summary.biggest_key, sst)
            .await;
        Ok(())
    }

    /// Flushes memtable to disk in background
    ///
    /// Handles flushing memtable to disk in background and
    /// removes it from the read only memtables
    ///
    /// It also notifies flush listener
    pub fn flush_handler<Id: 'static + AsRef<[u8]> + Send + Sync + Debug>(
        &mut self,
        table_id: Id,
        table_to_flush: InActiveMemtable,
        flush_tx: async_broadcast::Sender<FlushSignal>,
    ) {
        let tx = flush_tx.clone();
        let buckets = self.bucket_map.clone();
        let key_range = self.key_range.clone();
        let read_only_memtable = self.read_only_memtable.clone();
        tokio::spawn(async move {
            let mut flusher = Flusher::new(read_only_memtable.clone(), buckets, key_range);
            match flusher.flush(table_to_flush).await {
                Ok(_) => {
                    read_only_memtable.remove(&table_id.as_ref().to_vec());
                    if let Err(err) = tx.try_broadcast(FLUSH_SIGNAL) {
                        match err {
                            async_broadcast::TrySendError::Full(_) => {
                                log::info!("{}", Error::FlushSignalChannelOverflow.to_string())
                            }
                            _ => log::error!("{}", err),
                        }
                    }
                }
                Err(err) => {
                    log::error!("{}", err.to_string())
                }
            }
        });
    }
}
