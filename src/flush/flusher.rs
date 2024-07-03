use crate::consts::FLUSH_SIGNAL;
use crate::flush::flusher::Error::FilterNotProvidedForFlush;
use crate::flush::flusher::Error::FlushError;
use crate::flush::flusher::Error::TableSummaryIsNoneError;
use crate::types::{self, BloomFilterHandle, BucketMapHandle, FlushSignal, ImmutableMemTable, KeyRangeHandle};
use crate::{err::Error, memtable::MemTable};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLock;

type K = types::Key;
pub type InActiveMemtable = Arc<RwLock<MemTable<K>>>;

#[derive(Debug, Clone)]
pub struct Flusher {
    pub(crate) read_only_memtable: ImmutableMemTable<K>,
    pub(crate) bucket_map: BucketMapHandle,
    pub(crate) filters: BloomFilterHandle,
    pub(crate) key_range: KeyRangeHandle,
}

impl Flusher {
    pub fn new(
        read_only_memtable: ImmutableMemTable<K>,
        bucket_map: BucketMapHandle,
        filters: BloomFilterHandle,
        key_range: KeyRangeHandle,
    ) -> Self {
        Self {
            read_only_memtable,
            bucket_map,
            filters,
            key_range,
        }
    }

    pub async fn flush(&mut self, table: InActiveMemtable) -> Result<(), Error> {
        let flush_data = self;
        let table_reader = table.read().await;
        if table_reader.entries.is_empty() {
            return Err(Error::FailedToInsertToBucket("Cannot flush an empty table".to_string()));
        }
        let mut bucket_lock = flush_data.bucket_map.write().await;
        let sst = bucket_lock
            .insert_to_appropriate_bucket(Arc::new(Box::new(table_reader.to_owned())))
            .await?;
        drop(table_reader);
        if sst.summary.is_none() {
            return Err(TableSummaryIsNoneError);
        }
        if sst.filter.is_none() {
            return Err(FilterNotProvidedForFlush);
        }
        //IMPORTANT: Don't keep sst entries in memory
        sst.entries.clear();
        flush_data.filters.write().await.push(sst.filter.to_owned().unwrap());
        let summary = sst.summary.clone().unwrap();
        flush_data
            .key_range
            .write()
            .await
            .set(sst.dir.to_owned(), summary.smallest_key, summary.biggest_key, sst);
        Ok(())
    }

    pub fn flush_handler<Id: 'static + AsRef<[u8]> + Send + Sync + Debug>(
        &mut self,
        table_id: Id,
        table_to_flush: InActiveMemtable,
        flush_tx: async_broadcast::Sender<FlushSignal>,
    ) {
        let tx = flush_tx.clone();
        let buckets = self.bucket_map.clone();
        let filters = self.filters.clone();
        let key_range = self.key_range.clone();
        let read_only_memtable = self.read_only_memtable.clone();
        tokio::spawn(async move {
            let mut flusher = Flusher::new(read_only_memtable.clone(), buckets, filters, key_range);
            match flusher.flush(table_to_flush).await {
                Ok(_) => {
                    read_only_memtable
                        .write()
                        .await
                        .shift_remove(&table_id.as_ref().to_vec());
                    if let Err(err) = tx.try_broadcast(FLUSH_SIGNAL) {
                        match err {
                            async_broadcast::TrySendError::Full(_) => {
                                log::info!("{}", Error::FlushSignalChannelOverflowError.to_string())
                            }
                            _ => log::error!("{}", err),
                        }
                    }
                }
                Err(err) => {
                    log::error!("{}", FlushError(Box::new(err)))
                }
            }
        });
    }
}
