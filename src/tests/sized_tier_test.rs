#[cfg(test)]
mod tests {
    use crate::bucket::{Bucket, BucketMap};
    use crate::compactors::{Config, IntervalParams, SizedTierRunner, Strategy, TtlParams};
    use crate::consts::MIN_TRESHOLD;
    use crate::key_range::KeyRange;
    use crate::memtable::Entry;
    use crate::tests::workload::SSTContructor;
    use chrono::Utc;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::RwLock;
    use tokio::time::sleep;

    fn generate_config() -> Config {
        let use_ttl = false;
        let ttl = TtlParams {
            entry_ttl: Duration::new(60, 0),
            tombstone_ttl: Duration::new(120, 0),
        };
        let filter_false_positive = 0.01;
        let strategy = Strategy::STCS;
        let intervals = IntervalParams {
            background_interval: Duration::new(30, 0),
            flush_listener_interval: Duration::new(10, 0),
            tombstone_compaction_interval: Duration::new(45, 0),
        };

        Config::new(
            use_ttl.to_owned(),
            ttl.to_owned(),
            intervals.to_owned(),
            strategy,
            filter_false_positive.to_owned(),
        )
    }
    #[tokio::test]
    async fn test_sized_tier_runner_new() {
        let root = tempdir().unwrap();
        let path = root.path().join("bucket_map_new");
        let bucket_map = BucketMap::new(path.to_owned()).await.unwrap();

        let default_key_range = KeyRange::default();
        let use_ttl = false;
        let ttl = TtlParams {
            entry_ttl: Duration::new(60, 0),
            tombstone_ttl: Duration::new(120, 0),
        };
        let intervals = IntervalParams {
            background_interval: Duration::new(30, 0),
            flush_listener_interval: Duration::new(10, 0),
            tombstone_compaction_interval: Duration::new(45, 0),
        };
        let strategy = Strategy::STCS;
        let filter_false_positive = 0.01;
        let config = &Config::new(
            use_ttl.to_owned(),
            ttl.to_owned(),
            intervals.to_owned(),
            strategy,
            filter_false_positive.to_owned(),
        );

        let new_sized_tier_compaction_runner = SizedTierRunner::new(
            Arc::new(RwLock::new(bucket_map)),
            Arc::new(default_key_range),
            config,
        );
        assert!(new_sized_tier_compaction_runner
            .bucket_map
            .read()
            .await
            .buckets
            .is_empty());
        assert!(new_sized_tier_compaction_runner
            .key_range
            .key_ranges
            .read()
            .await
            .is_empty());
        assert!(new_sized_tier_compaction_runner.tombstones.is_empty());
        assert_eq!(new_sized_tier_compaction_runner.config.use_ttl, use_ttl);
        assert_eq!(
            new_sized_tier_compaction_runner.config.tombstone_ttl,
            ttl.tombstone_ttl
        );
        assert_eq!(new_sized_tier_compaction_runner.config.entry_ttl, ttl.entry_ttl);
        assert_eq!(
            new_sized_tier_compaction_runner.config.flush_listener_interval,
            intervals.flush_listener_interval
        );
        assert_eq!(
            new_sized_tier_compaction_runner.config.background_interval,
            intervals.background_interval
        );
        assert_eq!(
            new_sized_tier_compaction_runner
                .config
                .tombstone_compaction_interval,
            intervals.tombstone_compaction_interval
        );
        assert_eq!(
            new_sized_tier_compaction_runner.config.filter_false_positive,
            filter_false_positive
        );
    }

    #[tokio::test]
    async fn test_fetch_imbalanced_buckets() {
        let root = tempdir().unwrap();
        let path = root.path().join("imbalanced_bucket");
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
        let path = root.path().join("buket_map_extract");
        let mut bucket_map = BucketMap::new(path.to_owned()).await.unwrap();
        bucket_map.buckets.insert(new_bucket1.id, new_bucket1.to_owned());
        bucket_map.buckets.insert(new_bucket2.id, new_bucket2);
        bucket_map.buckets.insert(new_bucket3.id, new_bucket3);
        bucket_map.buckets.insert(new_bucket4.id, new_bucket4);

        let imbalanced_buckets =
            SizedTierRunner::fetch_imbalanced_buckets(Arc::new(RwLock::new(bucket_map))).await;
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
        assert_eq!(
            expected_ssts_to_remove_from_file,
            expected_ssts_to_remove_in_buckets
        );
    }

    #[tokio::test]
    async fn test_merge_ssts_in_buckets() {
        let root = tempdir().unwrap();
        let path = root.path().join("imbalanced_bucket");
        let bucket = Bucket::new(path.to_owned()).await.unwrap();
        let sst_count = 6;
        let sst_samples = SSTContructor::generate_ssts(sst_count).await;
        let mut number_of_entries_per_sstable = 0;
        let sstables_compacted = sst_count;
        for (idx, s) in sst_samples.iter().enumerate() {
            let mut sst = s.to_owned();
            sst.load_entries_from_file().await.unwrap();
            let mut filter = sst.filter.to_owned().unwrap();
            filter.recover_meta().await.unwrap();
            filter.build_filter_from_entries(&sst.entries);
            sst.filter = Some(filter);
            if idx == 0 {
                number_of_entries_per_sstable = sst.entries.len();
            }
            bucket.sstables.write().await.push(sst)
        }

        let root = tempdir().unwrap();
        let path = root.path().join("bucket_map_new");
        let mut bucket_map = BucketMap::new(path.to_owned()).await.unwrap();
        bucket_map.buckets.insert(uuid::Uuid::new_v4(), bucket.to_owned());

        let default_key_range = KeyRange::default();
        let config = &generate_config();
        let mut sized_tier_compaction_runner = SizedTierRunner::new(
            Arc::new(RwLock::new(bucket_map)),
            Arc::new(default_key_range),
            config,
        );
        let merge_ssts = sized_tier_compaction_runner
            .merge_ssts_in_buckets(&[bucket])
            .await;
        assert!(merge_ssts.is_ok());
        assert!(!merge_ssts.as_ref().unwrap().is_empty());
        for sst in merge_ssts.unwrap() {
            assert_eq!(
                sst.sstable.get_entries().len(),
                number_of_entries_per_sstable * sstables_compacted as usize
            )
        }
    }

    #[tokio::test]
    async fn test_run_compaction() {
        let root = tempdir().unwrap();
        let path = root.path().join("bucket_map_to_compact");

        let key_range = KeyRange::new();
        let sst_count = 6;

        let sst_samples = SSTContructor::generate_ssts(sst_count).await;
        let sst1 = tempdir().unwrap().path().to_owned();
        let sst2 = tempdir().unwrap().path().to_owned();
        let sst3 = tempdir().unwrap().path().to_owned();
        let sst4 = tempdir().unwrap().path().to_owned();
        let sst5 = tempdir().unwrap().path().to_owned();
        let sst6 = tempdir().unwrap().path().to_owned();
        let ssts = [sst1, sst2, sst3, sst4, sst5, sst6];

        let new_bucket1 = Bucket::new(path.to_owned()).await.unwrap();
        for (idx, mut s) in sst_samples.iter().cloned().enumerate() {
            s.dir = ssts[idx].to_owned().to_path_buf();
            let mut sst = s.to_owned();
            sst.load_entries_from_file().await.unwrap();

            let mut filter = sst.filter.to_owned().unwrap();
            filter.recover_meta().await.unwrap();
            filter.build_filter_from_entries(&sst.entries);
            sst.filter = Some(filter);
            key_range
                .set(
                    s.dir.to_owned(),
                    sst.entries.front().unwrap().key(),
                    sst.entries.back().unwrap().key(),
                    sst.to_owned(),
                )
                .await;
            new_bucket1.sstables.write().await.push(sst);
        }

        let new_bucket2 = Bucket::new(path.to_owned()).await.unwrap();
        for (idx, mut s) in sst_samples.iter().cloned().enumerate() {
            s.dir = ssts[idx].to_owned().to_path_buf();
            let mut sst = s.to_owned();
            sst.load_entries_from_file().await.unwrap();

            let mut filter = sst.filter.to_owned().unwrap();
            filter.recover_meta().await.unwrap();
            filter.build_filter_from_entries(&sst.entries);
            sst.filter = Some(filter);
            key_range
                .set(
                    s.dir.to_owned(),
                    sst.entries.front().unwrap().key(),
                    sst.entries.back().unwrap().key(),
                    sst.to_owned(),
                )
                .await;
            new_bucket2.sstables.write().await.push(sst);
        }

        let new_bucket3 = Bucket::new(path.to_owned()).await.unwrap();
        for (idx, mut s) in sst_samples.iter().cloned().enumerate() {
            s.dir = ssts[idx].to_owned().to_path_buf();
            let mut sst = s.to_owned();
            sst.load_entries_from_file().await.unwrap();

            let mut filter = sst.filter.to_owned().unwrap();
            filter.recover_meta().await.unwrap();
            filter.build_filter_from_entries(&sst.entries);
            sst.filter = Some(filter);
            key_range
                .set(
                    s.dir.to_owned(),
                    sst.entries.front().unwrap().key(),
                    sst.entries.back().unwrap().key(),
                    sst.to_owned(),
                )
                .await;
            new_bucket3.sstables.write().await.push(sst);
        }

        let new_bucket4 = Bucket::new(path.to_owned()).await.unwrap();
        for (idx, mut s) in sst_samples.iter().cloned().enumerate() {
            s.dir = ssts[idx].to_owned().to_path_buf();
            let mut sst = s.to_owned();
            sst.load_entries_from_file().await.unwrap();

            let mut filter = sst.filter.to_owned().unwrap();
            filter.recover_meta().await.unwrap();
            filter.build_filter_from_entries(&sst.entries);
            sst.filter = Some(filter);
            key_range
                .set(
                    s.dir.to_owned(),
                    sst.entries.front().unwrap().key(),
                    sst.entries.back().unwrap().key(),
                    sst.to_owned(),
                )
                .await;
            new_bucket4.sstables.write().await.push(sst);
        }

        let new_bucket5 = Bucket::new(path.to_owned()).await.unwrap();
        for (idx, mut s) in sst_samples.iter().cloned().enumerate() {
            s.dir = ssts[idx].to_owned().to_path_buf();
            let mut sst = s.to_owned();
            sst.load_entries_from_file().await.unwrap();

            let mut filter = sst.filter.to_owned().unwrap();
            filter.recover_meta().await.unwrap();
            filter.build_filter_from_entries(&sst.entries);
            sst.filter = Some(filter);
            key_range
                .set(
                    s.dir.to_owned(),
                    sst.entries.front().unwrap().key(),
                    sst.entries.back().unwrap().key(),
                    sst.to_owned(),
                )
                .await;
            new_bucket5.sstables.write().await.push(sst);
        }

        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let mut bucket_map = BucketMap::new(path.to_owned()).await.unwrap();
        bucket_map.buckets.insert(new_bucket1.id, new_bucket1.to_owned());
        bucket_map.buckets.insert(new_bucket2.id, new_bucket2);
        bucket_map.buckets.insert(new_bucket3.id, new_bucket3);
        bucket_map.buckets.insert(new_bucket4.id, new_bucket4);
        bucket_map.buckets.insert(new_bucket5.id, new_bucket5);

        let use_ttl = false;
        let ttl = TtlParams {
            entry_ttl: Duration::new(60, 0),
            tombstone_ttl: Duration::new(120, 0),
        };
        let intervals = IntervalParams {
            background_interval: Duration::new(30, 0),
            flush_listener_interval: Duration::new(10, 0),
            tombstone_compaction_interval: Duration::new(45, 0),
        };
        let strategy = Strategy::STCS;
        let filter_false_positive = 0.01;
        let config = &Config::new(
            use_ttl.to_owned(),
            ttl.to_owned(),
            intervals.to_owned(),
            strategy,
            filter_false_positive.to_owned(),
        );

        let mut sized_tier_compaction_runner =
            SizedTierRunner::new(Arc::new(RwLock::new(bucket_map)), Arc::new(key_range), config);
        let compaction_res = sized_tier_compaction_runner.run_compaction().await;
        assert!(compaction_res.is_ok());
        assert!(sized_tier_compaction_runner.tombstones.is_empty());
        assert!(!sized_tier_compaction_runner
            .bucket_map
            .read()
            .await
            .buckets
            .is_empty());
        // all sstables should have been compacted to 1
        assert_eq!(
            sized_tier_compaction_runner.bucket_map.read().await.buckets.len(),
            1
        );
        assert_eq!(
            sized_tier_compaction_runner.bucket_map.read().await.buckets[0]
                .sstables
                .read()
                .await
                .len(),
            1
        );

        assert!(!sized_tier_compaction_runner
            .key_range
            .key_ranges
            .read()
            .await
            .is_empty());
        // all sstables should have been compacted to 1 so we should have one range
        assert_eq!(
            sized_tier_compaction_runner
                .key_range
                .key_ranges
                .read()
                .await
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_cleanup_after_compaction() {
        let root = tempdir().unwrap();
        let path = root.path().join("bucket_map");

        let key_range = KeyRange::new();
        let sst_count = 6;

        let sst_samples = SSTContructor::generate_ssts(sst_count).await;
        let sst1 = tempdir().unwrap().path().to_owned();
        let sst2 = tempdir().unwrap().path().to_owned();
        let sst3 = tempdir().unwrap().path().to_owned();
        let sst4 = tempdir().unwrap().path().to_owned();
        let sst5 = tempdir().unwrap().path().to_owned();
        let sst6 = tempdir().unwrap().path().to_owned();
        let ssts = [sst1, sst2, sst3, sst4, sst5, sst6];

        let new_bucket1 = Bucket::new(path.to_owned()).await.unwrap();
        for (idx, mut s) in sst_samples.iter().cloned().enumerate() {
            s.dir = ssts[idx].to_owned().to_path_buf();
            let mut sst = s.to_owned();
            sst.load_entries_from_file().await.unwrap();

            let mut filter = sst.filter.to_owned().unwrap();
            filter.recover_meta().await.unwrap();
            filter.build_filter_from_entries(&sst.entries);
            sst.filter = Some(filter);
            key_range
                .set(
                    s.dir.to_owned(),
                    sst.entries.front().unwrap().key(),
                    sst.entries.back().unwrap().key(),
                    sst.to_owned(),
                )
                .await;
            new_bucket1.sstables.write().await.push(sst);
        }

        let new_bucket2 = Bucket::new(path.to_owned()).await.unwrap();
        for (idx, mut s) in sst_samples.iter().cloned().enumerate() {
            s.dir = ssts[idx].to_owned().to_path_buf();
            let mut sst = s.to_owned();
            sst.load_entries_from_file().await.unwrap();

            let mut filter = sst.filter.to_owned().unwrap();
            filter.recover_meta().await.unwrap();
            filter.build_filter_from_entries(&sst.entries);
            sst.filter = Some(filter);
            key_range
                .set(
                    s.dir.to_owned(),
                    sst.entries.front().unwrap().key(),
                    sst.entries.back().unwrap().key(),
                    sst.to_owned(),
                )
                .await;
            new_bucket2.sstables.write().await.push(sst);
        }

        let new_bucket3 = Bucket::new(path.to_owned()).await.unwrap();
        for (idx, mut s) in sst_samples.iter().cloned().enumerate() {
            s.dir = ssts[idx].to_owned().to_path_buf();
            let mut sst = s.to_owned();
            sst.load_entries_from_file().await.unwrap();

            let mut filter = sst.filter.to_owned().unwrap();
            filter.recover_meta().await.unwrap();
            filter.build_filter_from_entries(&sst.entries);
            sst.filter = Some(filter);
            key_range
                .set(
                    s.dir.to_owned(),
                    sst.entries.front().unwrap().key(),
                    sst.entries.back().unwrap().key(),
                    sst.to_owned(),
                )
                .await;
            new_bucket3.sstables.write().await.push(sst);
        }

        let new_bucket4 = Bucket::new(path.to_owned()).await.unwrap();
        for (idx, mut s) in sst_samples.iter().cloned().enumerate() {
            s.dir = ssts[idx].to_owned().to_path_buf();
            let mut sst = s.to_owned();
            sst.load_entries_from_file().await.unwrap();

            let mut filter = sst.filter.to_owned().unwrap();
            filter.recover_meta().await.unwrap();
            filter.build_filter_from_entries(&sst.entries);
            sst.filter = Some(filter);
            key_range
                .set(
                    s.dir.to_owned(),
                    sst.entries.front().unwrap().key(),
                    sst.entries.back().unwrap().key(),
                    sst.to_owned(),
                )
                .await;
            new_bucket4.sstables.write().await.push(sst);
        }

        let new_bucket5 = Bucket::new(path.to_owned()).await.unwrap();
        for (idx, mut s) in sst_samples.iter().cloned().enumerate() {
            s.dir = ssts[idx].to_owned().to_path_buf();
            let mut sst = s.to_owned();
            sst.load_entries_from_file().await.unwrap();
            let mut filter = sst.filter.to_owned().unwrap();

            filter.recover_meta().await.unwrap();
            filter.build_filter_from_entries(&sst.entries);
            sst.filter = Some(filter);
            key_range
                .set(
                    s.dir.to_owned(),
                    sst.entries.front().unwrap().key(),
                    sst.entries.back().unwrap().key(),
                    sst.to_owned(),
                )
                .await;
            new_bucket5.sstables.write().await.push(sst);
        }

        let root = tempdir().unwrap();
        let path = root.path().join(".");
        let mut bucket_map = BucketMap::new(path.to_owned()).await.unwrap();
        bucket_map.buckets.insert(new_bucket1.id, new_bucket1.to_owned());
        bucket_map.buckets.insert(new_bucket2.id, new_bucket2);
        bucket_map.buckets.insert(new_bucket3.id, new_bucket3);
        bucket_map.buckets.insert(new_bucket4.id, new_bucket4);
        bucket_map.buckets.insert(new_bucket5.id, new_bucket5);

        let config = &generate_config();
        let ssts_to_delete = &bucket_map.extract_imbalanced_buckets().await.unwrap().1;
        let bucket_map_ref = Arc::new(RwLock::new(bucket_map));
        let key_range_ref = Arc::new(key_range);
        let sized_tier_compaction_runner =
            SizedTierRunner::new(bucket_map_ref.clone(), key_range_ref.clone(), config);

        let cleanup_res = sized_tier_compaction_runner
            .clean_up_after_compaction(bucket_map_ref.clone(), ssts_to_delete, key_range_ref.clone())
            .await;
        assert!(cleanup_res.is_ok());
        assert!(cleanup_res.unwrap().is_some());
        assert!(sized_tier_compaction_runner
            .bucket_map
            .read()
            .await
            .buckets
            .is_empty());
        assert!(sized_tier_compaction_runner
            .key_range
            .key_ranges
            .read()
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn test_not_insert_tombstone_elements() {
        let root = tempdir().unwrap();
        let path = root.path().join("bucket_map_new");
        let bucket_map = BucketMap::new(path.to_owned()).await.unwrap();
        let default_key_range = KeyRange::default();
        let config = &generate_config();
        let mut sized_tier_compaction_runner = SizedTierRunner::new(
            Arc::new(RwLock::new(bucket_map)),
            Arc::new(default_key_range),
            config,
        );

        let not_tombstone = false;
        let merged_entries = [
            Entry::new("key1", 100, Utc::now(), not_tombstone),
            Entry::new("key2", 200, Utc::now(), not_tombstone),
            Entry::new("key3", 300, Utc::now(), not_tombstone),
        ];

        let is_tombstone = true;
        let to_insert = Entry::new("key4", 400, Utc::now(), is_tombstone);

        sized_tier_compaction_runner.tombstone_check(&to_insert, &mut merged_entries.to_vec());
        // length should not change since insertion is not be allowed
        assert_eq!(merged_entries.len(), 3);
    }

    #[tokio::test]
    async fn test_not_insert_tombstone_elements_found_in_tombstone_hashmap() {
        let root = tempdir().unwrap();
        let path = root.path().join("bucket_map_new");
        let bucket_map = BucketMap::new(path.to_owned()).await.unwrap();
        let default_key_range = KeyRange::default();
        let config = &generate_config();
        let mut sized_tier_compaction_runner = SizedTierRunner::new(
            Arc::new(RwLock::new(bucket_map)),
            Arc::new(default_key_range),
            config,
        );

        let not_tombstone = false;
        let merged_entries = [
            Entry::new("key1", 100, Utc::now(), not_tombstone),
            Entry::new("key2", 200, Utc::now(), not_tombstone),
            Entry::new("key3", 300, Utc::now(), not_tombstone),
        ];
        sleep(Duration::from_secs(1)).await;
        let is_tombstone = false;
        let deletion_time = Utc::now();
        let to_insert = Entry::new("key3", 300, deletion_time, is_tombstone);
        sized_tier_compaction_runner
            .tombstones
            .insert(to_insert.key.to_owned(), deletion_time);

        sized_tier_compaction_runner.tombstone_check(&to_insert, &mut merged_entries.to_vec());
        // length should not change since insertion is not be allowed
        assert_eq!(merged_entries.len(), 3);
    }

    #[tokio::test]
    async fn test_insert_valid_elements() {
        let root = tempdir().unwrap();
        let path = root.path().join("bucket_map_new");
        let bucket_map = BucketMap::new(path.to_owned()).await.unwrap();
        let default_key_range = KeyRange::default();
        let config = &generate_config();
        let mut sized_tier_compaction_runner = SizedTierRunner::new(
            Arc::new(RwLock::new(bucket_map)),
            Arc::new(default_key_range),
            config,
        );

        let not_tombstone = false;
        let mut merged_entries = vec![
            Entry::new("key1", 100, Utc::now(), not_tombstone),
            Entry::new("key2", 200, Utc::now(), not_tombstone),
            Entry::new("key3", 300, Utc::now(), not_tombstone),
        ];

        let not_tombstone = false;
        let to_insert = Entry::new("key4", 400, Utc::now(), not_tombstone);

        sized_tier_compaction_runner.tombstone_check(&to_insert, &mut merged_entries);
        // length should increase since insertion is allowed
        assert_eq!(merged_entries.len(), 4);
    }
}
