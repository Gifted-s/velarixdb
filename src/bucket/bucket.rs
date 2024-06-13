use crate::consts::{
    BUCKET_DIRECTORY_PREFIX, BUCKET_HIGH, BUCKET_LOW, MAX_TRESHOLD, MIN_SSTABLE_SIZE, MIN_TRESHOLD,
};
use crate::err::Error;
use crate::fs::{FileAsync, FileNode};
use crate::memtable::{InsertionTime, IsDeleted};
use crate::sst::Table;
use crate::types::{Key, ValOffset};
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use indexmap::IndexMap;
use log::{error, info};
use std::fmt::Debug;
use std::{path::PathBuf, sync::Arc};
use tokio::fs;
use tokio::sync::RwLock;
use uuid::Uuid;

type SSTablesToRemove = Vec<(BucketID, Vec<Table<FileNode>>)>;
type BucketsToCompact = Result<(Vec<Bucket<FileNode>>, SSTablesToRemove), Error>;
pub type BucketID = Uuid;

#[derive(Debug, Clone)]
pub struct BucketMap<F: FileAsync> {
    pub dir: PathBuf,
    pub buckets: IndexMap<BucketID, Bucket<F>>,
}
#[derive(Debug, Clone)]
pub struct Bucket<F: FileAsync> {
    pub(crate) id: BucketID,
    pub(crate) dir: PathBuf,
    pub(crate) size: usize,
    pub(crate) avarage_size: usize,
    pub(crate) sstables: Arc<RwLock<Vec<Table<F>>>>,
}

use Error::*;

pub trait InsertableToBucket: Debug + Send + Sync {
    fn get_entries(&self) -> Arc<SkipMap<Key, (ValOffset, InsertionTime, IsDeleted)>>;
    fn size(&self) -> usize;
    fn find_biggest_key_from_table(&self) -> Result<Vec<u8>, Error>;
    fn find_smallest_key_from_table(&self) -> Result<Vec<u8>, Error>;
}

impl<F:FileAsync> Bucket<F> {
    pub async fn new(dir: PathBuf) -> Self {
        let bucket_id = Uuid::new_v4();
        let bucket_dir =
            dir.join(BUCKET_DIRECTORY_PREFIX.to_string() + bucket_id.to_string().as_str());
        FileNode::create_dir_all(bucket_dir).await;
        Self {
            id: bucket_id,
            dir: bucket_dir,
            size: 0,
            avarage_size: 0,
            sstables: Arc::new(RwLock::new(Vec::new())),
        }
    }
    pub async fn new_with_id_dir_average_and_sstables(
        dir: PathBuf,
        id: BucketID,
        sstables: Vec<Table<F>>,
        mut avarage_size: usize,
    ) -> Result<Bucket<F>, Error> {
        if avarage_size == 0 {
            avarage_size = Bucket::calculate_buckets_avg_size(sstables.clone()).await?;
        }
        Ok(Self {
            id,
            dir,
            avarage_size,
            size: sstables.len() * avarage_size,
            sstables: Arc::new(RwLock::new(sstables)),
        })
    }

    async fn calculate_buckets_avg_size(
        sstables: Vec<Table<F>>,
    ) -> Result<usize, Error> {
        if sstables.is_empty() {
            return Ok(0);
        }
        let mut all_sstable_size = 0;
        let sst = sstables;
        let fetch_files_meta = sst
            .iter()
            .map(|s| tokio::spawn(fs::metadata(s.data_file_path.clone())));
        for meta_task in fetch_files_meta {
            let meta_data = meta_task
                .await
                .map_err(|err| GetFileMetaDataError(err.into()))?
                .unwrap();
            all_sstable_size += meta_data.len() as usize;
        }
        Ok(all_sstable_size / sst.len() as u64 as usize)
    }

    async fn extract_sstables(&self) -> Result<(Vec<Table<F>>, usize), Error> {
        if self.sstables.read().await.len() < MIN_TRESHOLD {
            return Ok((vec![], 0));
        }
        let extracted_sstables = self
            .sstables
            .read()
            .await
            .get(0..MAX_TRESHOLD)
            .unwrap_or(&self.sstables.read().await.clone())
            .to_vec();
        let average = Bucket::calculate_buckets_avg_size(extracted_sstables.clone()).await?;
        Ok((extracted_sstables, average))
    }

    async fn sstable_count_exceeds_threshhold(&self) -> bool {
        self.sstables.read().await.len() >= MIN_TRESHOLD
    }
}

impl<F: FileAsync> BucketMap<F> {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            buckets: IndexMap::new(),
        }
    }
    pub fn set_buckets(&mut self, buckets: IndexMap<BucketID, Bucket<F>>) {
        self.buckets = buckets
    }

    pub async fn insert_to_appropriate_bucket<T: InsertableToBucket + ?Sized>(
        &mut self,
        table: Arc<Box<T>>,
        hotness: u64,
    ) -> Result<Table<impl FileAsync>, Error> {
        let added_to_bucket = false;
        let created_at = Utc::now();
        for (_, bucket) in &mut self.buckets {
            if (bucket.avarage_size as f64 * BUCKET_LOW < table.size() as f64)
                && (table.size() < (bucket.avarage_size as f64 * BUCKET_HIGH) as usize)
                || (table.size() < MIN_SSTABLE_SIZE && bucket.avarage_size < MIN_SSTABLE_SIZE)
            {
                let sstable_directory = bucket
                    .dir
                    .join(format!("sstable_{}", created_at.timestamp_millis()));
                let mut sstable = Table::new(sstable_directory).await;
                sstable.set_entries(table.get_entries());
                sstable.write_to_file().await?;
                bucket.sstables.write().await.push(sstable.clone());
                // bucket
                //     .sstables
                //     .iter_mut()
                //     .for_each(|s| s.increase_hotness());
                bucket.avarage_size =
                    Bucket::calculate_buckets_avg_size((&bucket.sstables.read().await).to_vec())
                        .await?;
                bucket.size = bucket.avarage_size * bucket.sstables.read().await.len();
                return Ok(sstable);
            }
        }

        // create a new bucket if none of the condition above was satisfied
        if !added_to_bucket {
            let mut bucket = Bucket::new(self.dir.clone()).await;
            let sstable_directory = bucket
                .dir
                .join(format!("sstable_{}", created_at.timestamp_millis()));
            let mut sstable = Table::new(sstable_directory).await;
            sstable.set_entries(table.get_entries());
            sstable.write_to_file().await?;

            bucket.sstables.write().await.push(sstable.clone());
            bucket.avarage_size = fs::metadata(sstable.clone().data_file_path)
                .await
                .map_err(|err| GetFileMetaDataError(err))?
                .len() as usize;
            self.buckets.insert(bucket.id, bucket.clone());
            println!("insert was successful 1234");
            return Ok(sstable);
        }

        Err(FailedToInsertSSTableToBucketError)
    }

    pub async fn extract_buckets_to_compact(&self) -> BucketsToCompact {
        let mut sstables_to_delete: Vec<(BucketID, Vec<Table>)> = Vec::new();
        let mut buckets_to_compact: Vec<Bucket> = Vec::new();

        for (_, (bucket_id, bucket)) in self.buckets.iter().enumerate() {
            let (extracted_sstables, average) = Bucket::extract_sstables(&bucket).await?;
            if !extracted_sstables.is_empty() {
                sstables_to_delete.push((*bucket_id, extracted_sstables.clone()));
                buckets_to_compact.push(Bucket {
                    size: average * extracted_sstables.len(),
                    sstables: Arc::new(RwLock::new(extracted_sstables)),
                    id: *bucket_id,
                    dir: bucket.dir.to_owned(),
                    avarage_size: average,
                });
            }
        }
        Ok((buckets_to_compact, sstables_to_delete))
    }
    pub async fn is_balanced(&self) -> bool {
        for (_, (_, bucket)) in self.buckets.iter().enumerate() {
            if bucket.sstable_count_exceeds_threshhold().await {
                return false;
            }
        }
        return true;
    }
    // NOTE: This should be called only after compaction is complete
    pub async fn delete_sstables(
        &mut self,
        sstables_to_delete: &Vec<(BucketID, Vec<Table<FileNode>>)>,
    ) -> Result<bool, Error> {
        let mut all_sstables_deleted = true;
        let mut buckets_to_delete: Vec<&BucketID> = Vec::new();
        for (bucket_id, sst_paths) in sstables_to_delete {
            if let Some(bucket) = self.buckets.get_mut(bucket_id) {
                let bucket_clone = bucket.clone();
                let b = bucket_clone.sstables.read().await;
                let sstables_remaining = b.get(sst_paths.len()..).unwrap_or_default();
                if !sstables_remaining.is_empty() {
                    let new_average =
                        Bucket::calculate_buckets_avg_size(sstables_remaining.to_vec()).await?;
                    *bucket = Bucket {
                        id: bucket.id,
                        size: new_average * sstables_remaining.len(),
                        dir: bucket.dir.clone(),
                        avarage_size: new_average,
                        sstables: Arc::new(RwLock::new(sstables_remaining.to_vec())),
                    };
                } else {
                    *bucket = Bucket {
                        id: bucket.id,
                        size: 0,
                        dir: bucket.dir.clone(),
                        avarage_size: 0,
                        sstables: Arc::new(RwLock::new(vec![])),
                    };
                    //REMOVE
                    buckets_to_delete.push(bucket_id);
                    //REMOVE
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
        //REMOVE
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
