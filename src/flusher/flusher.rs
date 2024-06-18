use crate::bucket::BucketMap;
use crate::consts::FLUSH_SIGNAL;
use crate::fs::FileAsync;
use crate::types::{self, FlushSignal};
use crate::{cfg::Config, err::Error, filter::BloomFilter, key_range::KeyRange, memtable::InMemoryTable};
use futures::lock::Mutex;
use indexmap::IndexMap;
use std::sync::Arc;
use tokio::sync::RwLock;

type K = types::Key;

pub type InActiveMemtableID = Vec<u8>;
pub type InActiveMemtable = Arc<RwLock<InMemoryTable<K>>>;
pub type FlushDataMemTable = (InActiveMemtableID, InActiveMemtable);

#[derive(Debug, Clone)]
pub struct Flusher {
    pub(crate) read_only_memtable: Arc<RwLock<IndexMap<K, Arc<RwLock<InMemoryTable<K>>>>>>,
    pub(crate) bucket_map: Arc<RwLock<BucketMap>>,
    pub(crate) bloom_filters: Arc<RwLock<Vec<BloomFilter>>>,
    pub(crate) key_range: Arc<RwLock<KeyRange>>,
    pub(crate) use_ttl: bool,
    pub(crate) entry_ttl: u64,
}

impl Flusher {
    pub fn new(
        read_only_memtable: Arc<RwLock<IndexMap<K, Arc<RwLock<InMemoryTable<K>>>>>>,
        bucket_map: Arc<RwLock<BucketMap>>,
        bloom_filters: Arc<RwLock<Vec<BloomFilter>>>,
        key_range: Arc<RwLock<KeyRange>>,
        use_ttl: bool,
        entry_ttl: u64,
    ) -> Self {
        Self {
            read_only_memtable,
            bucket_map,
            bloom_filters,
            key_range,
            use_ttl,
            entry_ttl,
        }
    }

    pub async fn flush(&mut self, table: Arc<RwLock<InMemoryTable<K>>>) -> Result<(), Error> {
        let flush_data = self;
        let table_lock = table.read().await;
        if table_lock.entries.is_empty() {
            return Err(Error::FailedToInsertToBucket("Cannot flush an empty table".to_string()));
        }

        let table_bloom_filter = &mut table_lock.bloom_filter.to_owned();
        let table_biggest_key = table_lock.find_biggest_key()?;
        let table_smallest_key = table_lock.find_smallest_key()?;
        let hotness = 1;
        let mut bucket_lock = flush_data.bucket_map.write().await;
        let sst = bucket_lock
            .insert_to_appropriate_bucket(Arc::new(Box::new(table_lock.to_owned())), hotness)
            .await?;
        let data_file_path = sst.get_data_file_path().clone();
        flush_data
            .key_range
            .write()
            .await
            .set(data_file_path, table_smallest_key, table_biggest_key, sst.clone());
        table_bloom_filter.set_sstable(sst);
        flush_data
            .bloom_filters
            .write()
            .await
            .push(table_bloom_filter.to_owned());

        // sort bloom filter by hotness
        flush_data
            .bloom_filters
            .write()
            .await
            .sort_by(|a, b| b.get_sst().get_hotness().cmp(&a.get_sst().get_hotness()));

        Ok(())
    }

    pub fn flush_handler(
        &mut self,
        table_id: Vec<u8>,
        table_to_flush: Arc<RwLock<InMemoryTable<K>>>,
        flush_signal_sender: async_broadcast::Sender<FlushSignal>,
    ) {
        let flush_signal_sender_clone = flush_signal_sender.clone();
        let buckets_ref = self.bucket_map.clone();
        let bloomfilter_ref = self.bloom_filters.clone();
        let key_range_ref = self.key_range.clone();
        let read_only_memtable_ref = self.read_only_memtable.clone();
        let use_ttl = self.use_ttl;
        let entry_ttl = self.entry_ttl;
        tokio::spawn(async move {
            let mut flusher = Flusher::new(
                read_only_memtable_ref.clone(),
                buckets_ref,
                bloomfilter_ref,
                key_range_ref,
                use_ttl,
                entry_ttl,
            );

            match flusher.flush(table_to_flush).await {
                Ok(_) => {
                    let mut memtable_ref_lock = read_only_memtable_ref.write().await;
                    memtable_ref_lock.shift_remove(&table_id);
                    let broadcase_res = flush_signal_sender_clone.try_broadcast(FLUSH_SIGNAL);
                    match broadcase_res {
                        Err(err) => match err {
                            async_broadcast::TrySendError::Full(_) => {
                                log::error!("{}", Error::FlushSignalOverflowError)
                            }
                            _ => log::error!("{}", err),
                        },
                        _ => {}
                    }
                }
                // Handle failure case here
                Err(err) => {
                    println!("Flush error: {}", err);
                }
            }
        });
    }
}
