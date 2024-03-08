use crate::consts::{
    BUCKET_DIRECTORY_PREFIX, BUCKET_HIGH, BUCKET_LOW, MAX_TRESHOLD, MIN_SSTABLE_SIZE, MIN_TRESHOLD,
};
use crate::err::StorageEngineError;
use crate::sstable::{SSTable, SSTablePath};
use crossbeam_skiplist::SkipMap;
use log::{error, info};
use std::collections::HashMap;
use std::{path::PathBuf, sync::Arc};

use tokio::fs;
use uuid::Uuid;

#[derive(Debug)]
pub struct BucketMap {
    pub dir: PathBuf,
    pub buckets: HashMap<Uuid, Bucket>,
}
#[derive(Debug)]
pub struct Bucket {
    pub(crate) id: Uuid,
    pub(crate) dir: PathBuf,
    pub(crate) avarage_size: usize,
    pub(crate) sstables: Vec<SSTablePath>,
}
use StorageEngineError::*;
pub trait IndexWithSizeInBytes {
    fn get_index(&self) -> Arc<SkipMap<Vec<u8>, (usize, u64, bool)>>; // usize for value offset, u64 to store entry creation date in milliseconds
    fn size(&self) -> usize;
}

impl Bucket {
    pub async fn new(dir: PathBuf) -> Self {
        let bucket_id = Uuid::new_v4();
        let bucket_dir =
            dir.join(BUCKET_DIRECTORY_PREFIX.to_string() + bucket_id.to_string().as_str());

        if !bucket_dir.exists() {
            let _ = fs::create_dir_all(&bucket_dir).await;
        }
        Self {
            id: bucket_id,
            dir: bucket_dir,
            avarage_size: 0,
            sstables: Vec::new(),
        }
    }

    pub async fn new_with_id_dir_average_and_sstables(
        dir: PathBuf,
        id: Uuid,
        sstables: Vec<SSTablePath>,
        mut avarage_size: usize,
    ) -> Result<Bucket, StorageEngineError> {
        if avarage_size == 0 {
            avarage_size = Bucket::calculate_buckets_avg_size(&sstables).await?;
        }
        Ok(Self {
            id,
            dir,
            avarage_size,
            sstables,
        })
    }

    async fn calculate_buckets_avg_size(
        sstables: &Vec<SSTablePath>,
    ) -> Result<usize, StorageEngineError> {
        let mut all_sstable_size = 0;
        let fetch_files_meta = sstables
            .iter()
            .map(|s| tokio::spawn(fs::metadata(s.get_path())));
        for meta_task in fetch_files_meta {
            let meta_data = meta_task
                .await
                .map_err(|err| GetFileMetaDataError(err.into()))?
                .unwrap();
            all_sstable_size += meta_data.len() as usize;
        }
        Ok(all_sstable_size / sstables.len() as u64 as usize)
    }
}

impl BucketMap {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            buckets: HashMap::new(),
        }
    }
    pub fn set_buckets(&mut self, buckets: HashMap<Uuid, Bucket>) {
        self.buckets = buckets
    }

    pub async fn insert_to_appropriate_bucket<T: IndexWithSizeInBytes>(
        &mut self,
        table: &T,
        hotness: u64,
    ) -> Result<SSTablePath, StorageEngineError> {
        let added_to_bucket = false;
        for (_, bucket) in &mut self.buckets {
            // if (bucket low * bucket avg) is less than sstable size
            if (bucket.avarage_size as f64 * BUCKET_LOW  < table.size() as f64)
                    // and sstable size is less than (bucket avg * bucket high)
                    && (table.size() < (bucket.avarage_size as f64 * BUCKET_HIGH) as usize)
                    // or the (sstable size is less than min sstabke size) and (bucket avg is less than the min sstable size )
                    || (table.size() < MIN_SSTABLE_SIZE && bucket.avarage_size  < MIN_SSTABLE_SIZE)
            {
                let mut sstable = SSTable::new(bucket.dir.clone(), true).await;
                sstable.set_index(table.get_index());
                sstable.write_to_file().await?;
                // add sstable to bucket
                let sstable_path = SSTablePath {
                    file_path: sstable.get_path(),
                    hotness,
                };
                bucket.sstables.push(sstable_path.clone());
                bucket
                    .sstables
                    .iter_mut()
                    .for_each(|s| s.increase_hotness());
                bucket.avarage_size = Bucket::calculate_buckets_avg_size(&bucket.sstables).await?;
                return Ok(sstable_path);
            }
        }

        // create a new bucket if none of the condition above was satisfied
        if !added_to_bucket {
            let mut bucket = Bucket::new(self.dir.clone()).await;
            let mut sstable = SSTable::new(bucket.dir.clone(), true).await;
            sstable.set_index(table.get_index());
            sstable.write_to_file().await?;

            // add sstable to bucket
            let sstable_path = SSTablePath {
                file_path: sstable.get_path(),
                hotness: 1,
            };
            bucket.sstables.push(sstable_path.clone());
            bucket.avarage_size = fs::metadata(sstable.get_path())
                .await
                .map_err(|err| GetFileMetaDataError(err))?
                .len() as usize;

            self.buckets.insert(bucket.id, bucket);
            return Ok(sstable_path);
        }

        Err(FailedToInsertSSTableToBucketError)
    }

    pub fn extract_buckets_to_compact(&self) -> (Vec<Bucket>, Vec<(Uuid, Vec<SSTablePath>)>) {
        let mut sstables_to_delete: Vec<(Uuid, Vec<SSTablePath>)> = Vec::new();
        let mut buckets_to_compact: Vec<Bucket> = Vec::new();
        // Extract buckets
        self.buckets
            .iter()
            .enumerate()
            .filter(|elem| {
                // b for bucket :)
                let b = elem.1 .1;
                b.sstables.len() >= MIN_TRESHOLD
            })
            .for_each(|(_, elem)| {
                let b = elem.1;
                // get the sstables we have to delete after compaction is complete
                // if the number of sstables is more than the max treshhold then just extract the first 32 sstables
                // otherwise extract all the sstables in the bucket
                sstables_to_delete.push((
                    b.id,
                    b.sstables
                        .get(0..MAX_TRESHOLD)
                        .unwrap_or(&b.sstables)
                        .to_vec(),
                ));

                buckets_to_compact.push(Bucket {
                    sstables: b
                        .sstables
                        .get(0..MAX_TRESHOLD)
                        .unwrap_or(&b.sstables)
                        .to_vec(),
                    id: b.id,
                    dir: b.dir.to_owned(),
                    avarage_size: b.avarage_size, // passing the average size is redundant here becuase
                                                  // we don't need it for the actual compaction but we leave it to keep things readable
                })
            });
        (buckets_to_compact, sstables_to_delete)
    }

    // NOTE:  This should be called only after compaction is complete
    pub async fn delete_sstables(
        &mut self,
        sstables_to_delete: &Vec<(Uuid, Vec<SSTablePath>)>,
    ) -> bool {
        let mut all_sstables_deleted = true;
        let mut buckets_to_delete: Vec<&Uuid> = Vec::new();

        for (bucket_id, sst_paths) in sstables_to_delete {
            if let Some(bucket) = self.buckets.get_mut(bucket_id) {
                let sstables_remaining = bucket.sstables.get(sst_paths.len()..).unwrap_or_default();

                if !sstables_remaining.is_empty() {
                    *bucket = Bucket {
                        id: bucket.id,
                        dir: bucket.dir.clone(),
                        avarage_size: bucket.avarage_size,
                        sstables: sstables_remaining.to_vec(),
                    };
                } else {
                    buckets_to_delete.push(bucket_id);

                    if let Err(err) = fs::remove_dir_all(&bucket.dir).await {
                        error!(
                            "Bucket directory deletion error: bucket id={}, path={:?}, err={:?} ",
                            bucket_id, bucket.dir, err
                        );
                    } else {
                        info!("Bucket successfully removed with bucket id {}", bucket_id);
                    }
                }
            }

            for sst in sst_paths {
                if SSTable::file_exists(&PathBuf::new().join(sst.get_path())) {
                    if let Err(err) = fs::remove_file(&sst.file_path).await {
                        all_sstables_deleted = false;
                        error!(
                            "SStable file table delete path={:?}, err={:?} ",
                            sst.file_path, err
                        );
                    } else {
                        info!("SS Table deleted successfully.");
                    }
                }
            }
        }

        if !buckets_to_delete.is_empty() {
            buckets_to_delete.iter().for_each(|&bucket_id| {
                self.buckets.remove(bucket_id);
            });
        }
        all_sstables_deleted
    }
}
