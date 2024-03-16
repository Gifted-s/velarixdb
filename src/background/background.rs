use crate::compaction::Bucket;
use crate::{
    bloom_filter::BloomFilter, compaction::BucketMap, err::StorageEngineError,
    key_offseter::TableBiggestKeys, memtable::InMemoryTable, sstable::SSTablePath,
};

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use uuid::Uuid;

pub enum BackgroundJobType {
    // Flush oldest memtable to disk
    Flush(
        Arc<RwLock<Vec<InMemoryTable<Vec<u8>>>>>,
        Arc<RwLock<BucketMap>>,
        Arc<RwLock<Vec<BloomFilter>>>,
        Arc<RwLock<TableBiggestKeys>>
    ),
    // Remove obsolete keys from the value log
    // GarbageCollection,
    // // Merge sstables to form bigger ones and also remove deleted and obsolete keys
    // Compaction,
}

impl BackgroundJobType {
    pub async fn run(&mut self) -> Result<(Vec<InMemoryTable<Vec<u8>>>, BucketMap, Vec<BloomFilter>,TableBiggestKeys), StorageEngineError> {
        match self {
            BackgroundJobType::Flush(tables, bucket_map, bloom_filters, biggest_key_index) => {
                
                if tables.read().await.is_empty() {
                    println!("Cannot flush an empty table");
                    return Err(StorageEngineError::FailedToInsertToBucket("Cannot flush an empty table".to_string()));
                }
                let (tx, mut rx): (
                    mpsc::Sender<
                        Result<
                            (SSTablePath, HashMap<Uuid, Bucket, RandomState>),
                            StorageEngineError,
                        >,
                    >,
                    mpsc::Receiver<
                        Result<
                            (SSTablePath, HashMap<Uuid, Bucket, RandomState>),
                            StorageEngineError,
                        >,
                    >,
                ) = mpsc::channel(1);
                let  t = tables.read().await;
                let t_f = t.first().unwrap();
                let table_bloom_filter = &mut t_f.bloom_filter.to_owned();
                let table_biggest_key = t_f.find_biggest_key();
                let table_lock = Arc::new(RwLock::new(t_f.clone()));
                let bmap_clone = Arc::new(RwLock::new(bucket_map.read().await.to_owned()));
                tokio::spawn(async move {
                    let hotness = 1;
                    let bmap_clone = Arc::clone(&bmap_clone);
                    let table_to_flush_clone = Arc::clone(&table_lock);
                    let last_table = table_to_flush_clone.read().await;
                    let mut bucket_map_wl = bmap_clone.write().await;
                    let result = bucket_map_wl
                        .insert_to_appropriate_bucket(&last_table.to_owned(), hotness)
                        .await;
                    if let Ok(new_sstable_path) = result {
                        tx
                            .send(Ok((new_sstable_path, bucket_map_wl.buckets.to_owned())))
                            .await
                            .unwrap();
                    }
                });

                let insert_result = rx.recv().await;
                match insert_result {
                    Some(result) => {
                        match result {
                            Ok((sstable_path, updated_buckets)) => { 
                                // insert to bloom filter
                                bucket_map.write().await.buckets = updated_buckets;
                                let data_file_path = sstable_path.get_data_file_path().clone();
                                table_bloom_filter.set_sstable_path(sstable_path);
                                bloom_filters.write().await.push(table_bloom_filter.to_owned());

                                // sort bloom filter by hotness
                                bloom_filters.write().await.sort_by(|a, b| {
                                    b.get_sstable_path()
                                        .get_hotness()
                                        .cmp(&a.get_sstable_path().get_hotness())
                                });
                                if let Ok(biggest_key) = table_biggest_key {
                                    biggest_key_index.write().await.set(data_file_path, biggest_key);
                                    // Remove flushed memtable
                                    let t_rlock = tables.read().await.to_owned();
                                    let  splited = &t_rlock[1..];
                                    return Ok((splited.to_owned(), bucket_map.read().await.to_owned(), bloom_filters.read().await.to_owned(), biggest_key_index.read().await.to_owned() ));
                                } else {
                                
                                    println!("Insert succeeded but biggest key index was not set successfully");
                                    return Ok((tables.read().await.to_owned(), bucket_map.read().await.to_owned(), bloom_filters.read().await.to_owned(), biggest_key_index.read().await.to_owned() ));
                                }
                            }
                            Err(err) => {
                                return Err(StorageEngineError::FailedToInsertToBucket(err.to_string()));
                            }
                        }
                    }
                    None => {
                        return Err(StorageEngineError::FailedToInsertToBucket("Rreceiver error".to_string()));
                    }
                }

                // TODO: It makes more sense to clear the memtable here
            }

            // Self::GarbageCollection => todo!(),
            // Self::Compaction => todo!(),
        }
    }
}
