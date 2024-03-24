use crate::{
    bloom_filter::BloomFilter,
    cfg::Config,
    compaction::{BucketMap, Compactor},
    err::StorageEngineError,
    key_offseter::TableBiggestKeys,
    memtable::InMemoryTable,
    storage_engine::ExcRwAcc,
};
use indexmap::IndexMap;
use std::sync::Arc;
use std::{borrow::Borrow, hash::Hash};
use tokio::time::sleep;
use tokio::time::Duration;
pub type InActiveMemtableID = Vec<u8>;
pub type InActiveMemtable = Arc<RwLock<InMemoryTable<Vec<u8>>>>;
pub type FlushDataMemTable = (InActiveMemtableID, InActiveMemtable);

use tokio::spawn;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct FlushUpdateMsg {
    pub flushed_memtable_id: InActiveMemtableID,
    pub buckets: BucketMap,
    pub bloom_filters: Vec<BloomFilter>,
    pub biggest_key_index: TableBiggestKeys,
}

#[derive(Debug)]
pub enum FlushResponse {
    Success {
        table_id: Vec<u8>,
        updated_bucket_map: BucketMap,
        updated_bloom_filters: Vec<BloomFilter>,
        updated_biggest_key_index: TableBiggestKeys,
    },
    Failed {
        reason: StorageEngineError,
    },
}

#[derive(Debug, Clone)]
pub struct Flusher<K>
where
    K: Hash + PartialOrd + Ord + Send + Sync,
{
    pub(crate) read_only_memtable: ExcRwAcc<IndexMap<K, Arc<RwLock<InMemoryTable<K>>>>>,
    pub(crate) table_to_flush: Arc<RwLock<InMemoryTable<Vec<u8>>>>,
    pub(crate) table_id: Vec<u8>,
    pub(crate) bucket_map: ExcRwAcc<BucketMap>,
    pub(crate) bloom_filters: ExcRwAcc<Vec<BloomFilter>>,
    pub(crate) biggest_key_index: ExcRwAcc<TableBiggestKeys>,
    pub(crate) use_ttl: bool,
    pub(crate) entry_ttl: u64,
}

impl<K> Flusher<K>
where
    K: Hash + PartialOrd + Ord + Send + Sync + 'static + Borrow<std::vec::Vec<u8>>,
{
    pub fn new(
        read_only_memtable: ExcRwAcc<IndexMap<K, Arc<RwLock<InMemoryTable<K>>>>>,
        table_to_flush: Arc<RwLock<InMemoryTable<Vec<u8>>>>,
        table_id: Vec<u8>,
        bucket_map: ExcRwAcc<BucketMap>,
        bloom_filters: ExcRwAcc<Vec<BloomFilter>>,
        biggest_key_index: ExcRwAcc<TableBiggestKeys>,
        use_ttl: bool,
        entry_ttl: u64,
    ) -> Self {
        Self {
            read_only_memtable,
            table_to_flush,
            table_id,
            bucket_map,
            bloom_filters,
            biggest_key_index,
            use_ttl,
            entry_ttl,
        }
    }

    pub async fn flush(&mut self) -> Result<(), StorageEngineError> {
        let flush_data = self;
        let table = Arc::clone(&flush_data.table_to_flush);
        let table_id = &flush_data.table_id;
        if table.read().await.index.is_empty() {
            println!("Cannot flush an empty table");
            return Err(StorageEngineError::FailedToInsertToBucket(
                "Cannot flush an empty table".to_string(),
            ));
        }

        let table_bloom_filter = &mut table.read().await.bloom_filter.to_owned();
        let table_biggest_key = table.read().await.find_biggest_key()?;

        let hotness = 1;
        let sstable_path = flush_data
            .bucket_map
            .write()
            .await
            .insert_to_appropriate_bucket(&table.read().await.to_owned(), hotness)
            .await?;

        let data_file_path = sstable_path.get_data_file_path().clone();
        table_bloom_filter.set_sstable_path(sstable_path);
        flush_data
            .bloom_filters
            .write()
            .await
            .push(table_bloom_filter.to_owned());

        // sort bloom filter by hotness
        flush_data.bloom_filters.write().await.sort_by(|a, b| {
            b.get_sstable_path()
                .get_hotness()
                .cmp(&a.get_sstable_path().get_hotness())
        });
        flush_data
            .biggest_key_index
            .write()
            .await
            .set(data_file_path, table_biggest_key);
        let mut should_compact = false;
        //check for compaction conditions before returning
        // for (level, (_, bucket)) in flush_data.bucket_map.clone().buckets.iter().enumerate() {
        //     if bucket.should_trigger_compaction(level) {
        //         should_compact = true;
        //         break;
        //     }
        // }

        // if should_compact {
        //     let mut compactor = Compactor::new(flush_data.use_ttl, flush_data.entry_ttl);
        //     // compaction will continue to until all the table is balanced
        //     compactor
        //         .run_compaction(
        //             &mut flush_data.bucket_map,
        //             &mut flush_data.bloom_filters,
        //             &mut flush_data.biggest_key_index,
        //         )
        //         .await?;
        // }
        Ok(())
    }

    pub fn flush_data_collector(
        &self,
        rcx: Arc<RwLock<Receiver<FlushDataMemTable>>>,
        sdx: Arc<RwLock<Sender<Result<FlushUpdateMsg, StorageEngineError>>>>,
        buckets: ExcRwAcc<BucketMap>,
        bloom_filters: ExcRwAcc<Vec<BloomFilter>>,
        biggest_key_index: ExcRwAcc<TableBiggestKeys>,
        read_only_memtable: ExcRwAcc<IndexMap<K, Arc<RwLock<InMemoryTable<K>>>>>,
        config: Config,
    ) {
        let rcx_clone = Arc::clone(&rcx);
        let sdx_clone = Arc::clone(&sdx);

        spawn(async move {
            let current_buckets = Arc::clone(&buckets);
            let current_bloom_filters = Arc::clone(&bloom_filters);
            let current_biggest_key_index = Arc::clone(&biggest_key_index);
            let current_read_only_memtables = Arc::clone(&read_only_memtable);
            while let Some((table_id, table_to_flush)) = rcx_clone.write().await.recv().await {
                let mut flusher = Flusher::<K>::new(
                    Arc::clone(&read_only_memtable),
                    table_to_flush,
                    table_id.to_owned(),
                    Arc::clone(&current_buckets),
                    Arc::clone(&current_bloom_filters),
                    Arc::clone(&current_biggest_key_index),
                    config.enable_ttl,
                    config.entry_ttl_millis,
                );

                match flusher.flush().await {
                    Ok(_) => {
                        current_read_only_memtables
                            .write()
                            .await
                            .shift_remove(&table_id);
                        println!("Fix is successful")
                        // Sleep for 200 seconds
                        //sleep(Duration::from_secs(1)).await;
                    }
                    // Handle failure case here
                    Err(err) => {
                        println!("Flush error: {}", err);
                        // Handle error here
                    }
                }
            }
        });
    }
}
