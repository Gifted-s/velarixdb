use crate::compaction::Bucket;
use crate::{
    bloom_filter::BloomFilter, compaction::BucketMap, err::StorageEngineError,
    key_offseter::TableBiggestKeys, memtable::InMemoryTable, sstable::SSTablePath,
};

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum BackgroundResponse {
    FlushSuccessResponse {
        table_id: Vec<u8>,
        updated_bucket_map: BucketMap,
        updated_bloom_filters: Vec<BloomFilter>,
        updated_biggest_key_index: TableBiggestKeys,
    },
}

pub enum BackgroundJob {
    // Flush oldest memtable to disk
    Flush(FlushData), // Remove obsolete keys from the value log
                      // GarbageCollection,
                      // // Merge sstables to form bigger ones and also remove deleted and obsolete keys
                      // Compaction,
}

pub struct FlushData {
    pub(crate) table_to_flush: Arc<RwLock<InMemoryTable<Vec<u8>>>>,
    pub(crate) table_id: Vec<u8>,
    pub(crate) bucket_map: BucketMap,
    pub(crate) bloom_filters: Vec<BloomFilter>,
    pub(crate) biggest_key_index: TableBiggestKeys,
}

impl FlushData {
    pub(crate) fn new(
        table_to_flush: Arc<RwLock<InMemoryTable<Vec<u8>>>>,
        table_id: Vec<u8>,
        bucket_map: BucketMap,
        bloom_filters: Vec<BloomFilter>,
        biggest_key_index: TableBiggestKeys,
    ) -> Self {
        Self {
            table_to_flush,
            table_id,
            bucket_map,
            bloom_filters,
            biggest_key_index,
        }
    }
}

impl BackgroundJob {
    pub async fn run(&mut self) -> Result<BackgroundResponse, StorageEngineError> {
        match self {
            BackgroundJob::Flush(flush_data) => {
                let table = Arc::clone(&flush_data.table_to_flush);
                let table_id = &flush_data.table_id;
                if table.read().await.index.is_empty() {
                    println!("Cannot flush an empty table");
                    return Err(StorageEngineError::FailedToInsertToBucket(
                        "Cannot flush an empty table".to_string(),
                    ));
                }

                let table_bloom_filter = &mut table.read().await.bloom_filter.to_owned();
                let table_biggest_key = table.read().await.find_biggest_key();

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

                match table_biggest_key {
                    Ok(biggest_key) => {
                        flush_data
                            .biggest_key_index
                            .set(data_file_path, biggest_key);
                        // Remove flushed memtable
                        return Ok(BackgroundResponse::FlushSuccessResponse {
                            table_id: table_id.to_vec(),
                            updated_bucket_map: flush_data.bucket_map.to_owned(),
                            updated_bloom_filters: flush_data.bloom_filters.to_owned(),
                            updated_biggest_key_index: flush_data.biggest_key_index.to_owned(),
                        });
                    }
                    Err(err) => {
                        println!(
                            "Insert succeeded but biggest key index was not set successfully {}",
                            err.to_string()
                        );
                        return Ok(BackgroundResponse::FlushSuccessResponse {
                            table_id: table_id.to_vec(),
                            updated_bucket_map: flush_data.bucket_map.to_owned(),
                            updated_bloom_filters: flush_data.bloom_filters.to_owned(),
                            updated_biggest_key_index: flush_data.biggest_key_index.to_owned(),
                        });
                    }
                }

                // Self::GarbageCollection => todo!(),
                // Self::Compaction => todo!(),
            }
        }
    }
}
