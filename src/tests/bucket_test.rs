#[cfg(test)]
mod tests {
    use crate::sst::DataFile;
    use crate::tests::workload::FilterWorkload;
    use crate::{
        bucket::{Bucket, BucketMap},
        consts::{BUCKET_HIGH, MIN_TRESHOLD},
        err::Error,
    };
    use std::path::Path;
    use std::{path::PathBuf, sync::Arc};
    use tempfile::tempdir;
    use tokio::fs;
    use uuid::Uuid;

    use chrono::Utc;
    use crossbeam_skiplist::SkipMap;
    use tokio::{fs::File, sync::RwLock};

    use crate::{
        fs::{DataFileNode, FileNode, FileType, IndexFileNode},
        index::IndexFile,
        sst::Table,
    };

    pub struct SSTContructor {
        pub dir: PathBuf,
        pub data_path: PathBuf,
        pub index_path: PathBuf,
    }

    impl SSTContructor {
        fn new<P: AsRef<Path> + Send + Sync>(dir: P, data_path: P, index_path: P) -> Self {
            return Self {
                dir: dir.as_ref().to_path_buf(),
                data_path: data_path.as_ref().to_path_buf(),
                index_path: index_path.as_ref().to_path_buf(),
            };
        }

        pub async fn generate_ssts(number: u32) -> Vec<Table> {
            let sst_contructor: Vec<SSTContructor> = vec![
        SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830953696"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958016/data_1718830958016_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958016/index_1718830958016_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830954572"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958858/data_1718830958858_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958858/index_1718830958858_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830955463"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958906/data_1718830958906_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958906/index_1718830958906_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830956313"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958953/data_1718830958953_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958953/index_1718830958953_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830957169"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959000/data_1718830959000_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959000/index_1718830959000_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958810"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959049/data_1718830959049_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959049/index_1718830959049_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958858"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959097/data_1718830959097_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959097/index_1718830959097_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958906"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959145/data_1718830959145_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959145/index_1718830959145_.db",
        ),
    ),
    ];
            let mut ssts = Vec::new();
            for i in 0..number {
                let idx = i as usize;
                ssts.push(Table {
                    dir: sst_contructor[idx].dir.to_owned(),
                    hotness: 100,
                    size: 4096,
                    created_at: Utc::now(),
                    data_file: DataFile {
                        file: DataFileNode {
                            node: FileNode {
                                file_path: sst_contructor[idx].data_path.to_owned(),
                                file: Arc::new(RwLock::new(
                                    File::open(sst_contructor[idx].data_path.to_owned())
                                        .await
                                        .unwrap(),
                                )),
                                file_type: FileType::Data,
                            },
                        },
                        path: sst_contructor[idx].data_path.to_owned(),
                    },
                    index_file: IndexFile {
                        file: IndexFileNode {
                            node: FileNode {
                                file_path: sst_contructor[idx].index_path.to_owned(),
                                file: Arc::new(RwLock::new(
                                    File::open(sst_contructor[idx].index_path.to_owned())
                                        .await
                                        .unwrap(),
                                )),
                                file_type: FileType::Index,
                            },
                        },
                        path: sst_contructor[idx].index_path.to_owned(),
                    },
                    entries: Arc::new(SkipMap::default()),
                    filter: None,
                    summary: None,
                })
            }
            ssts
        }
    }

    #[tokio::test]
    async fn test_bucket_new() {
        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let new_bucket = Bucket::new(path.to_owned()).await;
        assert!(new_bucket.is_ok());
        let new_bucket = new_bucket.unwrap();
        let new_dir = new_bucket.dir.to_str().unwrap();
        let prefix = new_dir.rfind("bucket").unwrap();
        assert_eq!(&new_dir[..prefix - 1], path.to_str().unwrap());
        assert_eq!(new_bucket.size, 0);
        assert_eq!(new_bucket.avarage_size, 0);
        assert!(new_bucket.sstables.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_bucket_from_with_empty() {
        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let id = Uuid::new_v4();
        let average_size = 0;
        let sstables = Vec::new();
        let res = Bucket::from(path.to_owned(), id, sstables, average_size).await;
        assert!(res.is_ok());
        let new_bucket = res.unwrap();
        assert_eq!(new_bucket.dir, path);
        assert_eq!(new_bucket.avarage_size, average_size);
        assert_eq!(new_bucket.id, id);
        assert!(new_bucket.sstables.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_bucket_from_with_sstables() {
        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let id = Uuid::new_v4();
        let sst_count = 3;
        let sst_samples = SSTContructor::generate_ssts(sst_count).await;
        let sst_meta = sst_samples
            .iter()
            .map(|s| tokio::spawn(fs::metadata(s.data_file.path.clone())));
        let mut all_sstable_size = 0;
        for meta_task in sst_meta {
            let meta_data = meta_task
                .await
                .map_err(|err| Error::GetFileMetaData(err.into()))
                .unwrap();
            all_sstable_size += meta_data.unwrap().len() as usize;
        }
        let expected_avg = all_sstable_size / sst_count as usize;
        let res = Bucket::from(path.to_owned(), id, sst_samples, expected_avg).await;
        assert!(res.is_ok());
        let new_bucket = res.unwrap();
        assert_eq!(new_bucket.dir, path);
        assert_eq!(new_bucket.avarage_size, expected_avg);
        assert_eq!(new_bucket.id, id);
        assert_eq!(new_bucket.sstables.read().await.len(), sst_count as usize);
    }

    #[tokio::test]
    async fn test_cal_average_size() {
        let sst_count = 3;
        let sst_samples = SSTContructor::generate_ssts(sst_count).await;
        let sst_meta = sst_samples
            .iter()
            .map(|s| tokio::spawn(fs::metadata(s.data_file.path.clone())));
        let mut all_sstable_size = 0;
        for meta_task in sst_meta {
            let meta_data = meta_task
                .await
                .map_err(|err| Error::GetFileMetaData(err.into()))
                .unwrap();
            all_sstable_size += meta_data.unwrap().len() as usize;
        }
        let expected_avg = all_sstable_size / sst_count as usize;
        let actual_avg = Bucket::cal_average_size(sst_samples).await;
        assert!(actual_avg.is_ok());
        assert_eq!(actual_avg.unwrap(), expected_avg);
    }

    #[tokio::test]
    async fn test_sstcount_exceed_threshold() {
        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let new_bucket = Bucket::new(path.to_owned()).await.unwrap();
        let sst_count = 5;
        let sst_samples = SSTContructor::generate_ssts(sst_count).await;
        for s in sst_samples {
            new_bucket.sstables.write().await.push(s)
        }
        assert!(new_bucket.sstable_count_exceeds_threshhold().await);

        new_bucket.sstables.write().await.clear();

        assert!(!(new_bucket.sstable_count_exceeds_threshhold().await));
    }

    #[tokio::test]
    async fn test_extract_sstable_to_compact() {
        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let new_bucket = Bucket::new(path.to_owned()).await.unwrap();
        let sst_count = 5;
        let sst_samples = SSTContructor::generate_ssts(sst_count).await;
        let sst_meta = sst_samples
            .iter()
            .map(|s| tokio::spawn(fs::metadata(s.data_file.path.clone())));
        let mut all_sstable_size = 0;
        for meta_task in sst_meta {
            let meta_data = meta_task
                .await
                .map_err(|err| Error::GetFileMetaData(err.into()))
                .unwrap();
            all_sstable_size += meta_data.unwrap().len() as usize;
        }
        for s in sst_samples {
            new_bucket.sstables.write().await.push(s)
        }
        let expected_avg = all_sstable_size / sst_count as usize;
        let extracted_ssts = new_bucket.extract_sstables().await;
        assert!(extracted_ssts.is_ok());
        let (ssts, avg) = extracted_ssts.unwrap();
        assert_eq!(avg, expected_avg);
        assert_eq!(ssts.len(), sst_count as usize);
    }

    #[tokio::test]
    async fn table_fits_into_bucket() {
        let root = tempdir().unwrap();
        let path =root.path().join(".");
        let mut new_bucket = Bucket::new(path.to_owned()).await.unwrap();
        let sst_sample = SSTContructor::generate_ssts(2).await;
        for s in sst_sample {
            new_bucket.sstables.write().await.push(s)
        }
        let mut sst_within_size_range = SSTContructor::generate_ssts(1).await[0].to_owned();
        new_bucket.avarage_size = sst_within_size_range.size();
        let fits_into_bucket = new_bucket.fits_into_bucket(Arc::new(Box::new(sst_within_size_range.to_owned())));
        // size of sstable is not less than bucket low
        assert!(fits_into_bucket);
        // increase sstable size to be greater than bucket high range
        sst_within_size_range.size = ((new_bucket.avarage_size as f64 * BUCKET_HIGH) * 2.0) as usize;
        let fits_into_bucket = new_bucket.fits_into_bucket(Arc::new(Box::new(sst_within_size_range.to_owned())));
        // sstable size is greater than bucket high range
        assert!(!fits_into_bucket);
        // increase bucket average
        new_bucket.avarage_size = ((new_bucket.avarage_size as f64 * BUCKET_HIGH) * 2.0) as usize;
        let fits_into_bucket = new_bucket.fits_into_bucket(Arc::new(Box::new(sst_within_size_range.to_owned())));
        // sstable size is within bucket range
        assert!(fits_into_bucket);
    }

    #[tokio::test]
    async fn test_bucket_map_new() {
        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let bucket_map = BucketMap::new(path.to_owned()).await;
        assert!(bucket_map.is_ok());
        let bucket_map = bucket_map.unwrap();
        assert_eq!(bucket_map.dir, path);
        assert_eq!(bucket_map.buckets.len(), 0);
    }

    #[tokio::test]
    async fn test_bucket_map_extract_imbalanced_buckets() {
        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let new_bucket1 = Bucket::new(path.to_owned()).await.unwrap();
        let sst_count = 6;
        let sst_samples = SSTContructor::generate_ssts(sst_count).await;
        for s in sst_samples.iter().cloned() {
            new_bucket1.sstables.write().await.push(s)
        }

        let new_bucket2 = Bucket::new(path.to_owned()).await.unwrap();
        for s in sst_samples.iter().cloned() {
            new_bucket2.sstables.write().await.push(s)
        }

        let new_bucket3 = Bucket::new(path.to_owned()).await.unwrap();
        for s in sst_samples.iter().cloned() {
            new_bucket3.sstables.write().await.push(s)
        }

        let new_bucket4 = Bucket::new(path.to_owned()).await.unwrap();
        for s in sst_samples.iter().cloned() {
            new_bucket4.sstables.write().await.push(s)
        }

        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let mut bucket_map = BucketMap::new(path.to_owned()).await.unwrap();
        bucket_map.buckets.insert(new_bucket1.id, new_bucket1.to_owned());
        bucket_map.buckets.insert(new_bucket2.id, new_bucket2);
        bucket_map.buckets.insert(new_bucket3.id, new_bucket3);
        bucket_map.buckets.insert(new_bucket4.id, new_bucket4);

        let imbalanced_buckets = bucket_map.extract_imbalanced_buckets().await;
        assert!(imbalanced_buckets.is_ok());
        let (buckets, ssts_to_remove) = imbalanced_buckets.unwrap();
        let mut expected_ssts_to_remove_in_buckets = 0;
        assert_eq!(buckets.len(), 4);
        for bucket in buckets.iter().cloned() {
            let sst_len = bucket.sstables.read().await.len();
            assert!(sst_len == sst_count as usize);
            assert!(sst_len > MIN_TRESHOLD);
            expected_ssts_to_remove_in_buckets += sst_len;
        }
        let mut expected_ssts_to_remove_from_file = 0;
        for (_id, ssts) in ssts_to_remove {
            expected_ssts_to_remove_from_file += ssts.len();
        }
        assert_eq!(expected_ssts_to_remove_from_file, expected_ssts_to_remove_in_buckets);

        // test empty map
        bucket_map.buckets.clear();
        let imbalanced_buckets = bucket_map.extract_imbalanced_buckets().await;
        assert!(imbalanced_buckets.is_ok());
        let (buckets, sst_to_remove) = imbalanced_buckets.unwrap();
        assert_eq!(buckets.len(), 0);
        assert_eq!(sst_to_remove.len(), 0);

        // Should not return balanced buckets i.e bucket with sstables less than min treshold
        new_bucket1.sstables.write().await.clear();
        new_bucket1.sstables.write().await.push(sst_samples[0].to_owned());
        bucket_map.buckets.insert(new_bucket1.id, new_bucket1);
        let imbalanced_buckets = bucket_map.extract_imbalanced_buckets().await;
        assert!(imbalanced_buckets.is_ok());
        let (buckets, sst_to_remove) = imbalanced_buckets.unwrap();
        assert_eq!(buckets.len(), 0);
        assert_eq!(sst_to_remove.len(), 0);
    }

    #[tokio::test]
    async fn test_bucket_map_is_balanced() {
        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let new_bucket1 = Bucket::new(path.to_owned()).await.unwrap();
        let sst_count = 6;
        let sst_samples = SSTContructor::generate_ssts(sst_count).await;
        for s in sst_samples.iter().cloned() {
            new_bucket1.sstables.write().await.push(s)
        }

        let new_bucket2 = Bucket::new(path.to_owned()).await.unwrap();
        for s in sst_samples.iter().cloned() {
            new_bucket2.sstables.write().await.push(s)
        }

        let new_bucket3 = Bucket::new(path.to_owned()).await.unwrap();
        for s in sst_samples.iter().cloned() {
            new_bucket3.sstables.write().await.push(s)
        }

        let new_bucket4 = Bucket::new(path.to_owned()).await.unwrap();
        for s in sst_samples.iter().cloned() {
            new_bucket4.sstables.write().await.push(s)
        }

        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let mut bucket_map = BucketMap::new(path.to_owned()).await.unwrap();
        bucket_map.buckets.insert(new_bucket1.id, new_bucket1.to_owned());
        bucket_map.buckets.insert(new_bucket2.id, new_bucket2);
        bucket_map.buckets.insert(new_bucket3.id, new_bucket3);
        bucket_map.buckets.insert(new_bucket4.id, new_bucket4);

        let is_balanced = bucket_map.is_balanced().await;
        assert!(!is_balanced);

        // test empty map
        bucket_map.buckets.clear();
        let is_balanced = bucket_map.is_balanced().await;
        assert!(is_balanced);

        // Should not return false if all buckets are balanced
        new_bucket1.sstables.write().await.clear();
        new_bucket1.sstables.write().await.push(sst_samples[0].to_owned());
        bucket_map.buckets.insert(new_bucket1.id, new_bucket1);
        let is_balanced = bucket_map.is_balanced().await;
        assert!(is_balanced); 
    }

    #[tokio::test]
    async fn table_insert_to_appropriate_bucket() {
        let root = tempdir().unwrap();
        let path =root.path().join(".");
        let mut bucket_map = BucketMap::new(path.to_owned()).await.unwrap();
        let false_pos = 0.1;
        let mut sst_within_size_range = SSTContructor::generate_ssts(1).await[0].to_owned();
        sst_within_size_range.load_entries_from_file().await.unwrap();
        sst_within_size_range.filter = Some(FilterWorkload::from(
            false_pos,
            sst_within_size_range.entries.to_owned(),
        ));
        // bucket insertion is succeeds
        let insert_res = bucket_map
            .insert_to_appropriate_bucket(Arc::new(Box::new(sst_within_size_range.to_owned())))
            .await;
        assert!(insert_res.is_ok());
        assert_eq!(bucket_map.buckets.len(), 1);
        let insert_res = bucket_map
            .insert_to_appropriate_bucket(Arc::new(Box::new(sst_within_size_range.to_owned())))
            .await;
        assert!(insert_res.is_ok());
        // SST size is within first bucket size range so buckets should still be 1
        assert_eq!(bucket_map.buckets.len(), 1);
        sst_within_size_range.size = ((sst_within_size_range.size as f64 * BUCKET_HIGH) * 2.0) as usize;
        let insert_res = bucket_map
            .insert_to_appropriate_bucket(Arc::new(Box::new(sst_within_size_range.to_owned())))
            .await;
        assert!(insert_res.is_ok());
        // SST size is not within first bucket size range so a new bucket should have be created
        assert_eq!(bucket_map.buckets.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_sstables() {
        let root = tempdir().unwrap();
        let path =root.path().join(".");
        let new_bucket1 = Bucket::new(path.to_owned()).await.unwrap();
        let sst_count = 6;
        let sst_samples = SSTContructor::generate_ssts(sst_count).await;
        for s in sst_samples.iter().cloned() {
            new_bucket1.sstables.write().await.push(s)
        }

        let new_bucket2 = Bucket::new(path.to_owned()).await.unwrap();
        for s in sst_samples.iter().cloned() {
            new_bucket2.sstables.write().await.push(s)
        }

        let new_bucket3 = Bucket::new(path.to_owned()).await.unwrap();
        for s in sst_samples.iter().cloned() {
            new_bucket3.sstables.write().await.push(s)
        }

        let new_bucket4 = Bucket::new(path.to_owned()).await.unwrap();
        for s in sst_samples.iter().cloned() {
            new_bucket4.sstables.write().await.push(s)
        }

        let new_bucket5 = Bucket::new(path.to_owned()).await.unwrap();
        for s in sst_samples.iter().cloned() {
            new_bucket5.sstables.write().await.push(s)
        }

        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let mut bucket_map = BucketMap::new(path.to_owned()).await.unwrap();
        bucket_map.buckets.insert(new_bucket1.id, new_bucket1.to_owned());
        bucket_map.buckets.insert(new_bucket2.id, new_bucket2);
        bucket_map.buckets.insert(new_bucket3.id, new_bucket3);
        bucket_map.buckets.insert(new_bucket4.id, new_bucket4);
        bucket_map.buckets.insert(new_bucket5.id, new_bucket5);

        let imbalanced_buckets = bucket_map.extract_imbalanced_buckets().await;
        assert!(imbalanced_buckets.is_ok());
        let (buckets, ssts_to_remove) = imbalanced_buckets.unwrap();
        assert_eq!(buckets.len(), 5);

        let delete_res = bucket_map.delete_ssts(&ssts_to_remove).await;
        assert!(delete_res.is_ok());
        assert_eq!(bucket_map.buckets.len(), 0);
    }
}
