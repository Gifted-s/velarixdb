use crate::consts::{BUCKET_DIRECTORY_PREFIX, BUCKET_HIGH, BUCKET_LOW, MAX_TRESHOLD, MIN_SSTABLE_SIZE, MIN_TRESHOLD};
use crate::err::Error;
use crate::filter::BloomFilter;
use crate::fs::{FileAsync, FileNode};
use crate::sst::Table;
use crate::types::{Bool, Key, SkipMapEntries};
use chrono::Utc;
use indexmap::IndexMap;
use std::fmt::Debug;
use std::path::Path;
use std::{path::PathBuf, sync::Arc};
use tokio::fs;
use tokio::sync::RwLock;
use uuid::Uuid;
use Error::*;

static SST_PREFIX: &str = "sstable";

/// sstables to remove from each bucket
pub type SSTablesToRemove = Vec<(BucketID, Vec<Table>)>;

/// imbalanced buckets
pub type ImbalancedBuckets = Result<(Vec<Bucket>, SSTablesToRemove), Error>;

pub type BucketID = Uuid;

#[derive(Debug, Clone)]
pub struct BucketMap {
    pub dir: PathBuf,
    pub buckets: IndexMap<BucketID, Bucket>,
}

pub(crate) enum InsertionType {
    New,
    Exisiting,
}
#[derive(Debug, Clone)]
pub struct Bucket {
    pub(crate) id: BucketID,
    pub(crate) dir: PathBuf,
    pub(crate) size: usize,
    pub(crate) avarage_size: usize,
    pub(crate) sstables: Arc<RwLock<Vec<Table>>>,
}

pub trait InsertableToBucket: Debug + Send + Sync {
    fn get_entries(&self) -> SkipMapEntries<Key>;
    fn size(&self) -> usize;
    fn get_filter(&self) -> BloomFilter;
}

impl Bucket {
    pub async fn new<P: AsRef<Path>>(dir: P) -> Result<Bucket, Error> {
        let dir = dir.as_ref();
        let id = Uuid::new_v4();
        let dir = dir.join(BUCKET_DIRECTORY_PREFIX.to_string() + id.to_string().as_str());
        FileNode::create_dir_all(dir.to_owned()).await?;
        Ok(Self {
            id,
            dir,
            size: Default::default(),
            avarage_size: Default::default(),
            sstables: Arc::new(RwLock::new(Vec::new())),
        })
    }
    pub async fn from(
        dir: PathBuf,
        id: BucketID,
        sstables: Vec<Table>,
        mut avarage_size: usize,
    ) -> Result<Bucket, Error> {
        if avarage_size == 0 {
            avarage_size = Bucket::cal_average_size(sstables.clone()).await?;
        }
        Ok(Self {
            id,
            dir,
            avarage_size,
            size: sstables.len() * avarage_size,
            sstables: Arc::new(RwLock::new(sstables)),
        })
    }

    pub async fn cal_average_size(sstables: Vec<Table>) -> Result<usize, Error> {
        if sstables.is_empty() {
            return Ok(0);
        }
        let mut all_sstable_size = 0;
        let sst = sstables;
        let fetch_files_meta = sst.iter().map(|s| tokio::spawn(fs::metadata(s.data_file.path.clone())));
        for meta_task in fetch_files_meta {
            let meta_data = meta_task
                .await
                .map_err(|err| GetFileMetaDataError(err.into()))?
                .unwrap();
            all_sstable_size += meta_data.len() as usize;
        }
        Ok(all_sstable_size / sst.len() as u64 as usize)
    }

    pub fn fits_into_bucket<T: InsertableToBucket + ?Sized>(&self, table: Arc<Box<T>>) -> Bool {
        (self.avarage_size as f64 * BUCKET_LOW < table.size() as f64)
            && (table.size() < (self.avarage_size as f64 * BUCKET_HIGH) as usize)
            || (table.size() < MIN_SSTABLE_SIZE && self.avarage_size < MIN_SSTABLE_SIZE)
    }

    pub(crate) async fn extract_sstables(&self) -> Result<(Vec<Table>, usize), Error> {
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
        let average = Bucket::cal_average_size(extracted_sstables.clone()).await?;
        Ok((extracted_sstables, average))
    }

    pub async fn sstable_count_exceeds_threshhold(&self) -> bool {
        self.sstables.read().await.len() >= MIN_TRESHOLD
    }
}

impl BucketMap {
    pub async fn new<P: AsRef<Path>>(dir: P) -> Result<BucketMap, Error> {
        let dir = dir.as_ref();
        FileNode::create_dir_all(dir.to_path_buf()).await?;
        Ok(Self {
            dir: dir.to_path_buf(),
            buckets: IndexMap::new(),
        })
    }

    pub async fn insert_to_appropriate_bucket<T: InsertableToBucket + ?Sized>(
        &mut self,
        table: Arc<Box<T>>,
    ) -> Result<Table, Error> {
        for (_, bucket) in self.buckets.iter() {
            if bucket.fits_into_bucket(table.clone()) {
                return self
                    .insert_to_bucket(bucket.to_owned(), table, InsertionType::Exisiting)
                    .await;
            }
        }

        let bucket = Bucket::new(self.dir.clone()).await?;
        return self.insert_to_bucket(bucket, table, InsertionType::New).await;
    }

    async fn insert_to_bucket<T: InsertableToBucket + ?Sized>(
        &mut self,
        mut bucket: Bucket,
        table: Arc<Box<T>>,
        insert_type: InsertionType,
    ) -> Result<Table, Error> {
        let created_at = Utc::now();
        let sst_dir = bucket
            .dir
            .join(format!("{}_{}", SST_PREFIX, created_at.timestamp_millis()));
        let mut sst = Table::new(sst_dir).await?;
        sst.set_entries(table.get_entries());
        sst.filter = Some(table.get_filter());
        sst.write_to_file().await?;
        bucket.sstables.write().await.push(sst.to_owned());
        match insert_type {
            InsertionType::New => {
                bucket.avarage_size = fs::metadata(sst.clone().data_file.path)
                    .await
                    .map_err(|err| GetFileMetaDataError(err))?
                    .len() as usize;
                self.buckets.insert(bucket.id, bucket);
            }
            InsertionType::Exisiting => {
                bucket
                    .sstables
                    .write()
                    .await
                    .iter_mut()
                    .for_each(|s| s.increase_hotness());
                bucket.avarage_size = Bucket::cal_average_size((&bucket.sstables.read().await).to_vec()).await?;
                bucket.size = bucket.avarage_size * bucket.sstables.read().await.len();
                self.buckets.insert(bucket.id, bucket);
            }
        }

        return Ok(sst);
    }

    pub async fn extract_imbalanced_buckets(&self) -> ImbalancedBuckets {
        let mut ssts_to_delete: SSTablesToRemove = Vec::new();
        let mut imbalanced_buckets: Vec<Bucket> = Vec::new();
        for (_, (bucket_id, bucket)) in self.buckets.iter().enumerate() {
            let (ssts, avg) = Bucket::extract_sstables(&bucket).await?;
            if !ssts.is_empty() {
                ssts_to_delete.push((*bucket_id, ssts.clone()));
                imbalanced_buckets.push(Bucket {
                    size: avg * ssts.len(),
                    sstables: Arc::new(RwLock::new(ssts)),
                    id: *bucket_id,
                    dir: bucket.dir.to_owned(),
                    avarage_size: avg,
                });
            }
        }
        Ok((imbalanced_buckets, ssts_to_delete))
    }

    pub async fn is_balanced(&self) -> bool {
        for (_, bucket) in self.buckets.iter() {
            if bucket.sstable_count_exceeds_threshhold().await {
                return false;
            }
        }
        return true;
    }
    // NOTE: This should be called only after compaction is complete
    pub async fn delete_ssts(&mut self, ssts_to_delete: &SSTablesToRemove) -> Result<bool, Error> {
        let mut all_ssts_deleted = true;
        let mut buckets_to_delete: Vec<&BucketID> = Vec::new();

        for (bucket_id, ssts) in ssts_to_delete {
            if let Some(bucket) = self.buckets.get_mut(bucket_id) {
                let bucket_clone = bucket.clone();
                let b = bucket_clone.sstables.read().await;
                let ssts_remaining = b.get(ssts.len()..).unwrap_or_default();
                if !ssts_remaining.is_empty() {
                    let new_average = Bucket::cal_average_size(ssts_remaining.to_vec()).await?;
                    *bucket = Bucket {
                        id: bucket.id,
                        size: new_average * ssts_remaining.len(),
                        dir: bucket.dir.clone(),
                        avarage_size: new_average,
                        sstables: Arc::new(RwLock::new(ssts_remaining.to_vec())),
                    };
                } else {
                    buckets_to_delete.push(bucket_id);
                    if let Err(err) = fs::remove_dir_all(&bucket.dir).await {
                        log::error!("{}", DirDeleteError(err));
                    }
                }
            }

            for sst in ssts {
                if fs::metadata(&sst.dir).await.is_ok() {
                    if let Err(err) = fs::remove_dir_all(&sst.dir).await {
                        all_ssts_deleted = false;
                        log::error!("{}", DirDeleteError(err));
                    }
                }
            }
        }

        if !buckets_to_delete.is_empty() {
            buckets_to_delete.iter().for_each(|&bucket_id| {
                self.buckets.shift_remove(bucket_id);
            });
        }
        Ok(all_ssts_deleted)
    }

    // CAUTION: This removes all sstables and buckets and should only be used for total cleanup
    #[allow(dead_code)]
    pub async fn clear_all(&mut self) {
        for (_, bucket) in &self.buckets {
            if fs::metadata(&bucket.dir).await.is_ok() {
                if let Err(err) = fs::remove_dir_all(&bucket.dir).await {
                    log::error!("{}", FileDeleteError(err));
                }
            }
        }
        self.buckets = IndexMap::new();
    }
}
