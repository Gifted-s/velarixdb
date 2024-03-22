use crate::{
    bloom_filter::BloomFilter,
    cfg::Config,
    compaction::{BucketMap, Compactor},
    err::StorageEngineError,
    key_offseter::TableBiggestKeys,
    memtable::InMemoryTable,
};

use std::sync::Arc;

pub type InActiveMemtableID = Vec<u8>;
pub type InActiveMemtable = Arc<RwLock<InMemoryTable<Vec<u8>>>>;
pub type FlushDataMemTable = (InActiveMemtableID, InActiveMemtable);

use tokio::spawn;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct FlushUpdateMsg {
    pub flushed_memtable_ids: Vec<InActiveMemtableID>,
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
pub struct Flusher {
    pub(crate) table_to_flush: Arc<RwLock<InMemoryTable<Vec<u8>>>>,
    pub(crate) table_id: Vec<u8>,
    pub(crate) bucket_map: BucketMap,
    pub(crate) bloom_filters: Vec<BloomFilter>,
    pub(crate) biggest_key_index: TableBiggestKeys,
    pub(crate) use_ttl: bool,
    pub(crate) entry_ttl: u64,
}

impl Flusher {
    pub fn new(
        table_to_flush: Arc<RwLock<InMemoryTable<Vec<u8>>>>,
        table_id: Vec<u8>,
        bucket_map: BucketMap,
        bloom_filters: Vec<BloomFilter>,
        biggest_key_index: TableBiggestKeys,
        use_ttl: bool,
        entry_ttl: u64,
    ) -> Self {
        Self {
            table_to_flush,
            table_id,
            bucket_map,
            bloom_filters,
            biggest_key_index,
            use_ttl,
            entry_ttl,
        }
    }

    pub async fn flush(&mut self) -> Result<FlushResponse, StorageEngineError> {
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
            .insert_to_appropriate_bucket(&table.read().await.to_owned(), hotness)
            .await?;

        let data_file_path = sstable_path.get_data_file_path().clone();
        table_bloom_filter.set_sstable_path(sstable_path);
        flush_data.bloom_filters.push(table_bloom_filter.to_owned());

        // sort bloom filter by hotness
        flush_data.bloom_filters.sort_by(|a, b| {
            b.get_sstable_path()
                .get_hotness()
                .cmp(&a.get_sstable_path().get_hotness())
        });
        flush_data
            .biggest_key_index
            .set(data_file_path, table_biggest_key);
        let mut should_compact = false;
        // check for compaction conditions before returning
        // for (level, (_, bucket)) in flush_data.bucket_map.clone().buckets.iter().enumerate() {
        //     if bucket.should_trigger_compaction(level) {
        //         should_compact = true;
        //         break;
        //     }
        // }

        if should_compact {
            let mut compactor = Compactor::new(flush_data.use_ttl, flush_data.entry_ttl);
            // compaction will continue to until all the table is balanced
            compactor
                .run_compaction(
                    &mut flush_data.bucket_map,
                    &mut flush_data.bloom_filters,
                    &mut flush_data.biggest_key_index,
                )
                .await?;
            return Ok(FlushResponse::Success {
                table_id: table_id.to_vec(),
                updated_bucket_map: flush_data.bucket_map.to_owned(),
                updated_bloom_filters: flush_data.bloom_filters.to_owned(),
                updated_biggest_key_index: flush_data.biggest_key_index.to_owned(),
            });
        }

        return Ok(FlushResponse::Success {
            table_id: table_id.to_vec(),
            updated_bucket_map: flush_data.bucket_map.to_owned(),
            updated_bloom_filters: flush_data.bloom_filters.to_owned(),
            updated_biggest_key_index: flush_data.biggest_key_index.to_owned(),
        });
    }

    pub fn flush_data_collector(
        &self,
        rcx: Arc<RwLock<Receiver<FlushDataMemTable>>>,
        sdx: Arc<RwLock<Sender<Result<FlushUpdateMsg, StorageEngineError>>>>,
        buckets: BucketMap,
        bloom_filters: Vec<BloomFilter>,
        biggest_key_index: TableBiggestKeys,
        config: Config,
    ) {
        let rcx_clone = Arc::clone(&rcx);
        let sdx_clone = Arc::clone(&sdx);

        spawn(async move {
            let mut current_buckets = buckets;
            let mut current_bloom_filters = bloom_filters;
            let mut current_biggest_key_index = biggest_key_index;
            let mut flushed_memtable_ids: Vec<InActiveMemtableID> = Vec::new();
            loop {
                match rcx_clone.write().await.try_recv() {
                    Ok((table_id, table_to_flush)) => {
                        let mut flusher = Flusher::new(
                            table_to_flush,
                            table_id,
                            current_buckets.to_owned(),
                            current_bloom_filters.to_owned(),
                            current_biggest_key_index.to_owned(),
                            config.enable_ttl,
                            config.entry_ttl_millis,
                        );
                        let response = flusher.flush().await;

                        match response {
                            Ok(flush_res) => match flush_res {
                                FlushResponse::Success {
                                    table_id,
                                    updated_bucket_map,
                                    updated_bloom_filters,
                                    updated_biggest_key_index,
                                } => {
                                    flushed_memtable_ids.push(table_id);
                                    current_bloom_filters = updated_bloom_filters.to_owned();
                                    current_buckets = updated_bucket_map.to_owned();
                                    current_biggest_key_index =
                                        updated_biggest_key_index.to_owned();
                                }

                                //TODO: Handle failure case
                                FlushResponse::Failed { reason } => todo!(),
                            },
                            Err(err) => {
                                println!("Flush Err : {}", err)
                            }
                        }
                    }
                    Err(err) => match err {
                        tokio::sync::mpsc::error::TryRecvError::Empty => {
                            let _ = sdx_clone.write().await.send(Ok(FlushUpdateMsg {
                                flushed_memtable_ids: flushed_memtable_ids.to_owned(),
                                buckets: current_buckets.to_owned(),
                                bloom_filters: current_bloom_filters.to_owned(),
                                biggest_key_index: current_biggest_key_index.to_owned(),
                            }));
                        }
                        tokio::sync::mpsc::error::TryRecvError::Disconnected => todo!(),
                    },
                }
            }
        });
    }
}
