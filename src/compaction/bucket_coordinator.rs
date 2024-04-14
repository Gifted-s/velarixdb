use crate::consts::{
    BUCKET_DIRECTORY_PREFIX, BUCKET_HIGH, BUCKET_LOW, DEFAULT_TARGET_FILE_SIZE_BASE,
    DEFAULT_TARGET_FILE_SIZE_MULTIPLIER, MAX_TRESHOLD, MIN_SSTABLE_SIZE, MIN_TRESHOLD,
};
use crate::err::StorageEngineError;

use crate::memtable::{InsertionTime, IsDeleted, SkipMapKey, ValueOffset};
use crate::sstable::{SSTable, SSTablePath};
use chrono::Utc;
use crossbeam_skiplist::SkipMap;

use indexmap::IndexMap;
use log::{error, info};
use std::{path::PathBuf, sync::Arc};
use tokio::fs;
use uuid::Uuid;

type SSTablesToRemove = Vec<(BucketID, Vec<SSTablePath>)>;
type BucketsToCompact = Result<(Vec<Bucket>, SSTablesToRemove), StorageEngineError>;
pub type BucketID = Uuid;

#[derive(Debug, Clone)]
pub struct BucketMap {
    pub dir: PathBuf,
    pub buckets: IndexMap<BucketID, Bucket>,
}
#[derive(Debug, Clone)]
pub struct Bucket {
    pub(crate) id: BucketID,
    pub(crate) dir: PathBuf,
    pub(crate) size: usize,
    pub(crate) avarage_size: usize,
    pub(crate) sstables: Vec<SSTablePath>,
}

use StorageEngineError::*;

pub trait IndexWithSizeInBytes {
    fn get_index(&self) -> Arc<SkipMap<SkipMapKey, (ValueOffset, InsertionTime, IsDeleted)>>; // usize for value offset, u64 to store entry creation date in milliseconds
    fn size(&self) -> usize;
    fn find_biggest_key_from_table(&self) -> Result<Vec<u8>, StorageEngineError>;
    fn find_smallest_key_from_table(&self) -> Result<Vec<u8>, StorageEngineError>;
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
            size: 0,
            avarage_size: 0,
            sstables: Vec::new(),
        }
    }

    pub async fn new_with_id_dir_average_and_sstables(
        dir: PathBuf,
        id: BucketID,
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
            size: sstables.len() * avarage_size,
            sstables,
        })
    }

    pub fn should_trigger_compaction(&self, level: usize) -> bool {
        self.needs_compaction_by_count()
            || self.needs_compaction_by_size(self.calculate_target_size(level))
    }

    async fn calculate_buckets_avg_size(
        sstables: &Vec<SSTablePath>,
    ) -> Result<usize, StorageEngineError> {
        let mut all_sstable_size = 0;
        let fetch_files_meta = sstables
            .iter()
            .map(|s| tokio::spawn(fs::metadata(s.data_file_path.clone())));
        for meta_task in fetch_files_meta {
            let meta_data = meta_task
                .await
                .map_err(|err| GetFileMetaDataError(err.into()))?
                .unwrap();
            all_sstable_size += meta_data.len() as usize;
        }
        Ok(all_sstable_size / sstables.len() as u64 as usize)
    }

    async fn extract_and_calculate_sstables(
        &self,
    ) -> Result<(Vec<SSTablePath>, usize), StorageEngineError> {
        let extracted_sstables = self
            .sstables
            .get(0..MAX_TRESHOLD)
            .unwrap_or(&self.sstables)
            .to_vec();
        let average = Bucket::calculate_buckets_avg_size(&extracted_sstables).await?;
        Ok((extracted_sstables, average))
    }

    fn calculate_target_size(&self, level: usize) -> usize {
        (DEFAULT_TARGET_FILE_SIZE_BASE as i32
            * DEFAULT_TARGET_FILE_SIZE_MULTIPLIER.pow(level as u32)) as usize
    }

    fn needs_compaction_by_size(&self, target_size: usize) -> bool {
        self.size > target_size
    }

    fn needs_compaction_by_count(&self) -> bool {
        self.sstables.len() >= MIN_TRESHOLD
    }
}

impl BucketMap {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            buckets: IndexMap::new(),
        }
    }
    pub fn set_buckets(&mut self, buckets: IndexMap<BucketID, Bucket>) {
        self.buckets = buckets
    }

    pub async fn insert_to_appropriate_bucket<T: IndexWithSizeInBytes>(
        &mut self,
        table: &T,
        hotness: u64,
    ) -> Result<SSTablePath, StorageEngineError> {
        let added_to_bucket = false;
        let created_at = Utc::now();
        for (_, bucket) in &mut self.buckets {
            // if (bucket low * bucket avg) is less than sstable size
            if (bucket.avarage_size as f64 * BUCKET_LOW  < table.size() as f64)
                    // and sstable size is less than (bucket avg * bucket high)
                    && (table.size() < (bucket.avarage_size as f64 * BUCKET_HIGH) as usize)
                    // or the (sstable size is less than min sstabke size) and (bucket avg is less than the min sstable size )
                    || (table.size() < MIN_SSTABLE_SIZE && bucket.avarage_size  < MIN_SSTABLE_SIZE)
            {
                let sstable_directory = bucket
                    .dir
                    .join(format!("sstable_{}", created_at.timestamp_millis()));
                let mut sstable = SSTable::new(sstable_directory, true).await;
                sstable.set_index(table.get_index());
                sstable.write_to_file().await?;
                // add sstable to bucket
                let sstable_path = SSTablePath {
                    data_file_path: sstable.data_file_path,
                    index_file_path: sstable.index_file_path,
                    dir: sstable.sstable_dir,
                    hotness,
                };
                bucket.sstables.push(sstable_path.clone());
                bucket
                    .sstables
                    .iter_mut()
                    .for_each(|s| s.increase_hotness());
                bucket.avarage_size = Bucket::calculate_buckets_avg_size(&bucket.sstables).await?;
                bucket.size = bucket.avarage_size * bucket.sstables.len();
                return Ok(sstable_path);
            }
        }

        // create a new bucket if none of the condition above was satisfied
        if !added_to_bucket {
            let mut bucket = Bucket::new(self.dir.clone()).await;
            let sstable_directory = bucket
                .dir
                .join(format!("sstable_{}", created_at.timestamp_millis()));
            let mut sstable = SSTable::new(sstable_directory, true).await;
            sstable.set_index(table.get_index());
            sstable.write_to_file().await?;

            // add sstable to bucket
            let sstable_path = SSTablePath {
                data_file_path: sstable.data_file_path.clone(),
                index_file_path: sstable.index_file_path,
                dir: sstable.sstable_dir,
                hotness: 1,
            };
            bucket.sstables.push(sstable_path.clone());
            bucket.avarage_size = fs::metadata(sstable.data_file_path)
                .await
                .map_err(|err| GetFileMetaDataError(err))?
                .len() as usize;
            self.buckets.insert(bucket.id, bucket);
            return Ok(sstable_path);
        }

        Err(FailedToInsertSSTableToBucketError)
    }

    pub async fn extract_buckets_to_compact(&self) -> BucketsToCompact {
        let mut sstables_to_delete: Vec<(BucketID, Vec<SSTablePath>)> = Vec::new();
        let mut buckets_to_compact: Vec<Bucket> = Vec::new();

        for (level, (bucket_id, bucket)) in self.buckets.iter().enumerate() {
            let target_size = bucket.calculate_target_size(level);
            if Bucket::needs_compaction_by_size(bucket, target_size) {
                let (extracted_sstables, average) =
                    Bucket::extract_and_calculate_sstables(bucket).await?;
                sstables_to_delete.push((*bucket_id, extracted_sstables.clone()));
                buckets_to_compact.push(Bucket {
                    size: average * extracted_sstables.len(),
                    sstables: extracted_sstables,
                    id: *bucket_id,
                    dir: bucket.dir.to_owned(),
                    avarage_size: average,
                });
            } else if Bucket::needs_compaction_by_count(bucket) {
                let (extracted_sstables, average) =
                    Bucket::extract_and_calculate_sstables(bucket).await?;
                sstables_to_delete.push((*bucket_id, extracted_sstables.clone()));
                buckets_to_compact.push(Bucket {
                    size: average * extracted_sstables.len(),
                    sstables: extracted_sstables,
                    id: *bucket_id,
                    dir: bucket.dir.to_owned(),
                    avarage_size: average,
                });
            }
        }
        Ok((buckets_to_compact, sstables_to_delete))
    }

    // NOTE: This should be called only after compaction is complete
    pub async fn delete_sstables(
        &mut self,
        sstables_to_delete: &Vec<(BucketID, Vec<SSTablePath>)>,
    ) -> Result<bool, StorageEngineError> {
        let mut all_sstables_deleted = true;
        let mut buckets_to_delete: Vec<&BucketID> = Vec::new();

        for (bucket_id, sst_paths) in sstables_to_delete {
            if let Some(bucket) = self.buckets.get_mut(bucket_id) {
                let sstables_remaining = bucket.sstables.get(sst_paths.len()..).unwrap_or_default();

                if !sstables_remaining.is_empty() {
                    let new_average =
                        Bucket::calculate_buckets_avg_size(&sstables_remaining.to_vec()).await?;
                    *bucket = Bucket {
                        id: bucket.id,
                        size: new_average * sstables_remaining.len(),
                        dir: bucket.dir.clone(),
                        avarage_size: new_average,
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
                if fs::metadata(&sst.dir).await.is_ok() {
                    if let Err(err) = fs::remove_dir_all(&sst.dir).await {
                        all_sstables_deleted = false;
                        error!(
                            "SStable directory not successfully deleted path={:?}, err={:?} ",
                            sst.data_file_path, err
                        );
                    } else {
                        info!("SS Table directory deleted successfully.");
                    }
                }
            }
        }

        if !buckets_to_delete.is_empty() {
            buckets_to_delete.iter().for_each(|&bucket_id| {
                self.buckets.shift_remove(bucket_id);
            });
        }
        Ok(all_sstables_deleted)
    }

    // CAUTION: This removes all sstables and buckets and should only be used for total cleanup
    pub async fn clear_all(&mut self) {
        for (_, bucket) in &self.buckets {
            if fs::metadata(&bucket.dir).await.is_ok() {
                if let Err(err) = fs::remove_dir_all(&bucket.dir).await {
                    error!(
                        "Err sstable not deleted path={:?}, err={:?} ",
                        bucket.dir, err
                    );
                }
            }
        }
        self.buckets = IndexMap::new();
    }
}
