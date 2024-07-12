#[cfg(test)]
mod tests {
    use crate::tests::*;
    use crate::key_range::KeyRange;
    use std::time::Duration;
    use workload::SSTContructor;

    #[tokio::test]
    async fn test_range_new() {
        let smallest_key = "smallest_key";
        let biggest_key = "biggest_key";

        let fake_sstable = SSTContructor::generate_ssts(1).await[0].to_owned();
        let fake_sst_dir = fake_sstable.dir.to_owned();
        let new_range = crate::key_range::Range::new(smallest_key, biggest_key, fake_sstable);

        assert_eq!(new_range.biggest_key, biggest_key.as_bytes().to_vec());
        assert_eq!(new_range.smallest_key, smallest_key.as_bytes().to_vec());
        assert_eq!(new_range.sst.dir, fake_sst_dir);
    }

    #[tokio::test]
    async fn test_default_keyrange() {
        let default_key_range = KeyRange::default();
        assert!(default_key_range.key_ranges.read().await.is_empty());
        assert!(default_key_range.restored_ranges.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_new_keyrange() {
        let new_key_range = KeyRange::new();
        assert!(new_key_range.key_ranges.read().await.is_empty());
        assert!(new_key_range.restored_ranges.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_key_range_set() {
        let key_range = KeyRange::new();

        let smallest_key = "smallest_key";
        let biggest_key = "biggest_key";
        let fake_sstable = SSTContructor::generate_ssts(1).await[0].to_owned();
        let fake_sst_dir = fake_sstable.dir.to_owned();
        key_range
            .set(fake_sst_dir, smallest_key, biggest_key, fake_sstable)
            .await;
        assert_eq!(key_range.key_ranges.read().await.len(), 1);
    }

    #[tokio::test]
    async fn test_key_range_remove() {
        let key_range = KeyRange::new();

        let smallest_key = "smallest_key";
        let biggest_key = "biggest_key";
        let fake_sstable = SSTContructor::generate_ssts(1).await[0].to_owned();
        let fake_sst_dir = fake_sstable.dir.to_owned();
        key_range
            .set(fake_sst_dir.to_owned(), smallest_key, biggest_key, fake_sstable)
            .await;
        assert_eq!(key_range.key_ranges.read().await.len(), 1);

        key_range.remove(fake_sst_dir).await;
        assert!(key_range.key_ranges.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_key_range_filters_by_biggest_key() {
        let key_range = KeyRange::new();
        let ssts = SSTContructor::generate_ssts(2).await;

        // Insert sstable
        let mut fake_sstable = ssts[0].to_owned();
        fake_sstable.load_entries_from_file().await.unwrap();
        let entries = fake_sstable.entries.to_owned();
        let binding = entries.front().unwrap();
        let smallest_key = binding.key();
        let binding = entries.back().unwrap();
        let biggest_key = binding.key();
        let fake_sst_dir = fake_sstable.dir.to_owned();
        key_range
            .set(fake_sst_dir.to_owned(), smallest_key, biggest_key, fake_sstable)
            .await;
        assert_eq!(key_range.key_ranges.read().await.len(), 1);

        // Insert second sstable
        let mut fake_sstable2 = ssts[1].to_owned();
        fake_sstable2.load_entries_from_file().await.unwrap();
        let entries2 = fake_sstable2.entries.to_owned();
        let binding = entries2.front().unwrap();
        let smallest_key2 = binding.key();
        let binding = entries2.back().unwrap();
        let biggest_key2 = binding.key();
        let fake_sst_dir2 = fake_sstable2.dir.to_owned();
        key_range
            .set(fake_sst_dir2.to_owned(), smallest_key2, biggest_key2, fake_sstable2)
            .await;
        assert_eq!(key_range.key_ranges.read().await.len(), 2);

        // Searched for the first smallest key
        let retrieved_sstables = key_range.filter_sstables_by_key_range(smallest_key).await;
        assert!(retrieved_sstables.is_ok());
        assert!(!retrieved_sstables.as_ref().unwrap().is_empty());
        let mut sst_is_within_range = false;
        for sst in retrieved_sstables.unwrap() {
            if sst.dir == fake_sst_dir {
                sst_is_within_range = true
            }
        }
        assert!(sst_is_within_range);

        // Search fo the second biggest key
        let retrieved_sstables = key_range.filter_sstables_by_key_range(biggest_key2).await;
        assert!(retrieved_sstables.is_ok());
        assert!(!retrieved_sstables.as_ref().unwrap().is_empty());
        let mut sst_is_within_range = false;
        for sst in retrieved_sstables.unwrap() {
            if sst.dir == fake_sst_dir2 {
                sst_is_within_range = true
            }
        }
        assert!(sst_is_within_range)
    }

    #[tokio::test]
    async fn test_key_range_recover_bloomfilter() {
        let key_range = KeyRange::new();
        let ssts = SSTContructor::generate_ssts(2).await;

        // Insert sstable
        let mut fake_sstable = ssts[0].to_owned();
        fake_sstable.load_entries_from_file().await.unwrap();
        let entries = fake_sstable.entries.to_owned();
        let binding = entries.front().unwrap();
        let smallest_key = binding.key();
        let binding = entries.back().unwrap();
        let biggest_key = binding.key();
        let fake_sst_dir = fake_sstable.dir.to_owned();
        key_range
            .set(fake_sst_dir.to_owned(), smallest_key, biggest_key, fake_sstable)
            .await;
        assert_eq!(key_range.key_ranges.read().await.len(), 1);

        // Bloom Filter should be empty
        let range = key_range.key_ranges.read().await;
        let sst_with_empty_filter = range.get(&fake_sst_dir).unwrap();

        // Ensure Bloom Filter does not exist for this sstable
        assert!(sst_with_empty_filter.sst.filter.clone().unwrap().sst_dir.is_none());

        // Searched for the first smallest key
        let retrieved_sstables = key_range.filter_sstables_by_key_range(smallest_key).await;
        assert!(retrieved_sstables.is_ok());
        assert!(!retrieved_sstables.as_ref().unwrap().is_empty());
        let mut sst_found = false;
        for sst in retrieved_sstables.unwrap() {
            if sst.dir == fake_sst_dir {
                sst_found = true;
                // Bloom Filter should have be restored
                assert!(sst.filter.clone().unwrap().sst_dir.is_some());
            }
        }
        // SSTable should have been found
        assert!(sst_found)
    }

    #[tokio::test]
    async fn test_key_range_not_found() {
        let key_range = KeyRange::new();
        let ssts = SSTContructor::generate_ssts(2).await;

        // Insert sstable
        let mut fake_sstable = ssts[0].to_owned();
        fake_sstable.load_entries_from_file().await.unwrap();
        let entries = fake_sstable.entries.to_owned();
        let binding = entries.front().unwrap();
        let smallest_key = binding.key();
        let binding = entries.back().unwrap();
        let biggest_key = binding.key();
        let fake_sst_dir = fake_sstable.dir.to_owned();
        key_range
            .set(fake_sst_dir.to_owned(), smallest_key, biggest_key, fake_sstable)
            .await;
        assert_eq!(key_range.key_ranges.read().await.len(), 1);
        let fake_key = "***Not Found***";

        let range = key_range.filter_sstables_by_key_range(fake_key).await;
        assert!(range.is_ok());
        assert!(range.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_restored_key_range() {
        let key_range = KeyRange::new();
        let ssts = SSTContructor::generate_ssts(2).await;

        // Insert sstable
        let mut fake_sstable = ssts[0].to_owned();
        fake_sstable.load_entries_from_file().await.unwrap();
        let entries = fake_sstable.entries.to_owned();
        let binding = entries.front().unwrap();
        let smallest_key = binding.key();
        let binding = entries.back().unwrap();
        let biggest_key = binding.key();
        let fake_sst_dir = fake_sstable.dir.to_owned();
        key_range
            .set(fake_sst_dir.to_owned(), smallest_key, biggest_key, fake_sstable)
            .await;
        assert_eq!(key_range.key_ranges.read().await.len(), 1);

        // Bloom Filter should be empty
        let range = key_range.key_ranges.read().await;
        let sst_with_empty_filter = range.get(&fake_sst_dir).unwrap();

        // Ensure Bloom Filter does not exist for this sstable
        assert!(sst_with_empty_filter.sst.filter.clone().unwrap().sst_dir.is_none());

        // Ensure restored ranges is loaded
        let retrieved_sstables = key_range.filter_sstables_by_key_range(smallest_key).await;
        assert!(retrieved_sstables.is_ok());
        assert!(!retrieved_sstables.as_ref().unwrap().is_empty());

        // sleep for 3 seconds to ensure all background tasks completes
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Since we have not synced restored ssts with major key range
        // we should find the sstable in the retored key range
        let retrieved_sstables = key_range.check_restored_key_ranges(smallest_key).await;
        assert!(retrieved_sstables.is_ok());
        assert!(!retrieved_sstables.as_ref().unwrap().is_empty());
        let mut sst_found = false;
        for sst in retrieved_sstables.unwrap() {
            if sst.dir == fake_sst_dir {
                sst_found = true;
                // Bloom Filter should have be restored
                assert!(sst.filter.clone().unwrap().sst_dir.is_some());
            }
        }
        // SSTable should have been found
        assert!(sst_found)
    }

    #[tokio::test]
    async fn test_update_key_range() {
        let key_range = KeyRange::new();
        let ssts = SSTContructor::generate_ssts(2).await;

        // Insert sstable
        let mut fake_sstable = ssts[0].to_owned();
        fake_sstable.load_entries_from_file().await.unwrap();
        let entries = fake_sstable.entries.to_owned();
        let binding = entries.front().unwrap();
        let smallest_key = binding.key();
        let binding = entries.back().unwrap();
        let biggest_key = binding.key();
        let fake_sst_dir = fake_sstable.dir.to_owned();
        key_range
            .set(fake_sst_dir.to_owned(), smallest_key, biggest_key, fake_sstable)
            .await;
        assert_eq!(key_range.key_ranges.read().await.len(), 1);

        // Bloom Filter should be empty
        let range = key_range.key_ranges.read().await;
        let sst_with_empty_filter = range.get(&fake_sst_dir).unwrap();

        // Ensure Bloom Filter does not exist for this sstable
        assert!(sst_with_empty_filter.sst.filter.clone().unwrap().sst_dir.is_none());

        // Ensure restored ranges is loaded
        let retrieved_sstables = key_range.filter_sstables_by_key_range(smallest_key).await;
        assert!(retrieved_sstables.is_ok());
        assert!(!retrieved_sstables.as_ref().unwrap().is_empty());

        // Sleep for 3 seconds to ensure all background tasks completes
        tokio::time::sleep(Duration::from_secs(3)).await;
        // Drop read lock
        drop(range);
        // Call update on key ranges to sync restored key range with main key ranges hash map
        key_range.update_key_range().await;

        let retrieved_sstables = key_range.check_restored_key_ranges(smallest_key).await;
        assert!(retrieved_sstables.is_ok());
        // retrieved sstables should now be empty since we synced it
        // with main key ranges hash map
        assert!(retrieved_sstables.as_ref().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_key_range_range_query_scan() {
        let key_range = KeyRange::new();
        let ssts = SSTContructor::generate_ssts(2).await;

        // Insert sstable
        let mut fake_sstable = ssts[0].to_owned();
        fake_sstable.load_entries_from_file().await.unwrap();
        let entries = fake_sstable.entries.to_owned();
        let binding = entries.front().unwrap();
        let smallest_key = binding.key();
        let binding = entries.back().unwrap();
        let biggest_key = binding.key();
        let fake_sst_dir = fake_sstable.dir.to_owned();
        key_range
            .set(fake_sst_dir.to_owned(), smallest_key, biggest_key, fake_sstable)
            .await;

        let range = key_range.range_query_scan(smallest_key, biggest_key).await;
        assert_eq!(range.len(), 1);
        assert_eq!(range.first().unwrap().sst.dir, fake_sst_dir);
    }
}
