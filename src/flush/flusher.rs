use crate::bucket::bucket::InsertableToBucket;
use crate::consts::FLUSH_SIGNAL;
use crate::flush::flusher::Error::FlushError;
use crate::flush::flusher::Error::TableSummaryIsNoneError;
use crate::types::{self, BloomFilterHandle, BucketMapHandle, FlushSignal, ImmutableMemTable, KeyRangeHandle};
use crate::{err::Error, memtable::MemTable};
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
        let table_lock = table.read().await;
        if table_lock.entries.is_empty() {
            return Err(Error::FailedToInsertToBucket("Cannot flush an empty table".to_string()));
        }

        let filter = &mut table_lock.bloom_filter.to_owned();
        let mut bucket_lock = flush_data.bucket_map.write().await;
        let mut sst = bucket_lock
            .insert_to_appropriate_bucket(Arc::new(Box::new(table_lock.to_owned())))
            .await?;
        drop(table_lock);
        let data_file_path = sst.get_data_file_path().clone();
        filter.set_sstable_path(data_file_path.to_owned());
        flush_data.filters.write().await.push(filter.to_owned());
        sst.filter = Some(filter.to_owned());
        // TODO: drop entries
        if sst.summary.is_none() {
            return Err(TableSummaryIsNoneError);
        }
        let summary = sst.summary.clone().unwrap();
        flush_data
            .key_range
            .write()
            .await
            .set(data_file_path, summary.smallest_key, summary.biggest_key, sst);
        Ok(())
    }

    pub fn flush_handler<Id: 'static + AsRef<[u8]> + Send + Sync>(
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
                    let mut tables = read_only_memtable.write().await;
                    tables.shift_remove(&table_id.as_ref().to_vec());
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
