mod tests {
    use super::*;
    use crate::{bucket::Bucket, err::Error, tests::fixtures};
    use std::{
        path::{PathBuf, Prefix},
        time::Duration,
    };
    use tempfile::{tempdir, tempfile};
    use tokio::{fs, time::sleep};
    use uuid::Uuid;
    #[tokio::test]
    async fn test_bucket_new() -> Result<(), Error> {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("."));
        let new_bucket = Bucket::new(path.to_owned()).await;
        let new_dir = new_bucket.dir.to_str().unwrap();
        let prefix = new_dir.rfind("bucket").unwrap();

        assert_eq!(&new_dir[..prefix - 1], path.to_str().unwrap());
        assert_eq!(new_bucket.size, 0);
        assert_eq!(new_bucket.avarage_size, 0);
        assert!(new_bucket.sstables.read().await.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_bucket_from_with_empty() -> Result<(), Error> {
        let path = PathBuf::from(".");
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
        Ok(())
    }

    #[tokio::test]
    async fn test_bucket_from_with_sstables() -> Result<(), Error> {
        let path = PathBuf::from(".");
        let id = Uuid::new_v4();
        let sst_count = 3;
        let sst_samples = fixtures::sst::generate_ssts(sst_count).await;
        let sst_meta = sst_samples
            .iter()
            .map(|s| tokio::spawn(fs::metadata(s.data_file.path.clone())));
        let mut all_sstable_size = 0;
        for meta_task in sst_meta {
            let meta_data = meta_task
                .await
                .map_err(|err| Error::GetFileMetaDataError(err.into()))?
                .unwrap();
            all_sstable_size += meta_data.len() as usize;
        }
        let expected_avg = all_sstable_size / sst_count as usize;
        let res = Bucket::from(path.to_owned(), id, sst_samples, expected_avg).await;
        assert!(res.is_ok());
        let new_bucket = res.unwrap();
        assert_eq!(new_bucket.dir, path);
        assert_eq!(new_bucket.avarage_size, expected_avg);
        assert_eq!(new_bucket.id, id);
        assert_eq!(new_bucket.sstables.read().await.len(), sst_count as usize);
        Ok(())
    }

    #[tokio::test]
    async fn test_cal_average_size() -> Result<(), Error> {
        let sst_count = 3;
        let sst_samples = fixtures::sst::generate_ssts(sst_count).await;
        let sst_meta = sst_samples
            .iter()
            .map(|s| tokio::spawn(fs::metadata(s.data_file.path.clone())));
        let mut all_sstable_size = 0;
        for meta_task in sst_meta {
            let meta_data = meta_task
                .await
                .map_err(|err| Error::GetFileMetaDataError(err.into()))?
                .unwrap();
            all_sstable_size += meta_data.len() as usize;
        }
        let expected_avg = all_sstable_size / sst_count as usize;
        let actual_avg = Bucket::cal_average_size(sst_samples).await;
        assert!(actual_avg.is_ok());
        assert_eq!(actual_avg.unwrap(), expected_avg);
        Ok(())
    }

    #[tokio::test]
    async fn test_sstcount_exceed_threshold() -> Result<(), Error> {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("."));
        let new_bucket = Bucket::new(path.to_owned()).await;
        let sst_count = 5;
        let sst_samples = fixtures::sst::generate_ssts(sst_count).await;
        for s in sst_samples {
            new_bucket.sstables.write().await.push(s)
        }
        assert_eq!(new_bucket.sstable_count_exceeds_threshhold().await, true);

        new_bucket.sstables.write().await.clear();

        assert_eq!(new_bucket.sstable_count_exceeds_threshhold().await, false);
        Ok(())
    }

    #[tokio::test]
    async fn test_extract_sstable_to_compact() -> Result<(), Error> {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("."));
        let new_bucket = Bucket::new(path.to_owned()).await;
        let sst_count = 5;
        let sst_samples = fixtures::sst::generate_ssts(sst_count).await;
        let sst_meta = sst_samples
            .iter()
            .map(|s| tokio::spawn(fs::metadata(s.data_file.path.clone())));
        let mut all_sstable_size = 0;
        for meta_task in sst_meta {
            let meta_data = meta_task
                .await
                .map_err(|err| Error::GetFileMetaDataError(err.into()))?
                .unwrap();
            all_sstable_size += meta_data.len() as usize;
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
        Ok(())
    }
}
