use crate::bucket::bucket::InsertableToBucket;
use crate::consts::FLUSH_SIGNAL;
use crate::flusher::flusher::Error::FlushError;
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
        let biggest_key = table_lock.find_biggest_key()?;
        let smallest_key = table_lock.find_smallest_key()?;
        let mut bucket_lock = flush_data.bucket_map.write().await;
        let sst = bucket_lock
            .insert_to_appropriate_bucket(Arc::new(Box::new(table_lock.to_owned())))
            .await?;
        drop(table_lock);
        let data_file_path = sst.get_data_file_path().clone();
        flush_data
            .key_range
            .write()
            .await
            .set(data_file_path, smallest_key, biggest_key, sst.clone());
        filter.set_sstable(sst);
        flush_data.filters.write().await.push(filter.to_owned());

        // sort bloom filter by hotness
        flush_data
            .filters
            .write()
            .await
            .sort_by(|a, b| b.get_sst().get_hotness().cmp(&a.get_sst().get_hotness()));

        Ok(())
    }

    pub fn flush_handler(
        &mut self,
        table_id: Vec<u8>,
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
                    tables.shift_remove(&table_id);
                    if let Err(err) = tx.try_broadcast(FLUSH_SIGNAL) {
                        match err {
                            async_broadcast::TrySendError::Full(_) => {
                                log::error!("{}", Error::FlushSignalChannelOverflowError)
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
