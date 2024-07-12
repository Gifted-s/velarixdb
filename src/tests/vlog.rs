#[cfg(test)]
mod tests {
    use crate::consts::{SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8};
    use crate::vlog::{ValueLog, ValueLogEntry};
    use chrono::Utc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_vlog_new() {
        let root = tempdir().unwrap();
        let path = root.path().join("vlog_new");

        let vlog = ValueLog::new(path).await;

        assert!(vlog.is_ok());
        assert_eq!(vlog.as_ref().unwrap().head_offset, 0);
        assert_eq!(vlog.as_ref().unwrap().tail_offset, 0);
        assert_eq!(vlog.unwrap().size, 0);
    }

    #[tokio::test]
    async fn test_append() {
        let root = tempdir().unwrap();
        let path = root.path().join("vlog_append");

        let mut vlog = ValueLog::new(path).await.unwrap();

        let key1 = "key1";
        let val1 = "val1";
        let key2 = "key2";
        let val2 = "val2";
        let is_tombstone = false;
        let offset = vlog.append(key1, val1, Utc::now(), is_tombstone).await;
        assert!(offset.is_ok());

        let offset = vlog.append(key2, val2, Utc::now(), is_tombstone).await;
        assert!(offset.is_ok());
    }

    #[tokio::test]
    async fn test_get() {
        let root = tempdir().unwrap();
        let path = root.path().join("vlog_append");

        let mut vlog = ValueLog::new(path).await.unwrap();

        let key1 = "key1";
        let val1 = "val1";
        let key2 = "key2";
        let val2 = "val2";
        let time = Utc::now();
        let is_tombstone = false;
        let offset = vlog.append(key1, val1, time, is_tombstone).await;
        assert!(offset.is_ok());
        let start_offset1 = offset.unwrap();

        let is_tombstone_true = true;
        let offset = vlog.append(key2, val2, time, is_tombstone_true).await;
        assert!(offset.is_ok());
        let start_offset2 = offset.unwrap();

        // get first key
        let recvoered = vlog.get(start_offset1).await;
        assert!(recvoered.is_ok());
        assert!(recvoered.as_ref().unwrap().is_some());
        let (value, is_tomb) = recvoered.unwrap().unwrap();
        assert_eq!(value, val1.as_bytes().to_vec());
        assert_eq!(is_tomb, is_tombstone);

        // get second key
        let recvoered = vlog.get(start_offset2).await;
        assert!(recvoered.is_ok());
        assert!(recvoered.as_ref().unwrap().is_some());
        let (value, is_tomb) = recvoered.unwrap().unwrap();
        assert_eq!(value, val2.as_bytes().to_vec());
        assert_eq!(is_tomb, is_tombstone_true);
    }

    #[tokio::test]
    async fn test_sync_all() {
        let root = tempdir().unwrap();
        let path = root.path().join("vlog_sync_all");

        let mut vlog = ValueLog::new(path).await.unwrap();

        let key1 = "key1";
        let val1 = "val1";
        let time = Utc::now();
        let is_tombstone = false;
        let offset = vlog.append(key1, val1, time, is_tombstone).await;
        assert!(offset.is_ok());

        assert!(vlog.sync_to_disk().await.is_ok());
    }

    #[tokio::test]
    async fn test_recover() {
        let root = tempdir().unwrap();
        let path = root.path().join("vlog_recover");

        let mut vlog = ValueLog::new(path).await.unwrap();

        let key1 = "key1";
        let val1 = "val1";
        let key2 = "key2";
        let val2 = "val2";
        let time = Utc::now();
        let is_tombstone = false;
        let offset = vlog.append(key1, val1, time, is_tombstone).await;
        assert!(offset.is_ok());
        let start_offset = offset.unwrap();

        let is_tombstone_true = true;
        let offset = vlog.append(key2, val2, time, is_tombstone_true).await;
        assert!(offset.is_ok());

        let entries = vlog.recover(start_offset).await;
        assert!(entries.is_ok());
        assert_eq!(entries.unwrap().len(), 2)
    }

    #[tokio::test]
    async fn test_read_chunk_to_garbage_collect() {
        let root = tempdir().unwrap();
        let path = root.path().join("vlog_gc");

        let mut vlog = ValueLog::new(path).await.unwrap();

        let key1 = "key1";
        let val1 = "val1";
        let key2 = "key2";
        let val2 = "val2";
        let time = Utc::now();
        let entry_len1 = SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + key1.len() + val1.len() + SIZE_OF_U8;
        let entry_len2 = SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + key2.len() + val2.len() + SIZE_OF_U8;

        let bytes_to_collect = entry_len1 + entry_len2;

        let is_tombstone = false;
        let offset = vlog.append(key1, val1, time, is_tombstone).await;
        assert!(offset.is_ok());

        let is_tombstone_true = true;
        let offset = vlog.append(key2, val2, time, is_tombstone_true).await;
        assert!(offset.is_ok());

        let entries = vlog.read_chunk_to_garbage_collect(bytes_to_collect).await;
        assert!(entries.is_ok());
        let (val_entries, bytes) = entries.unwrap();
        assert_eq!(val_entries.len(), 2);
        assert_eq!(bytes, bytes_to_collect);
    }

    #[tokio::test]
    async fn test_clear_all() {
        let root = tempdir().unwrap();
        let path = root.path().join("vlog_clear_all");

        let mut vlog = ValueLog::new(path).await.unwrap();

        let key1 = "key1";
        let val1 = "val1";
        let key2 = "key2";
        let val2 = "val2";
        let time = Utc::now();
        let is_tombstone = false;
        let mut offset = vlog.append(key1, val1, time, is_tombstone).await;
        assert!(offset.is_ok());
        let is_tombstone_true = true;
        offset = vlog.append(key2, val2, time, is_tombstone_true).await;
        assert!(offset.is_ok());

        vlog.clear_all().await;

        assert_eq!(vlog.head_offset, 0);
        assert_eq!(vlog.tail_offset, 0);
        assert_eq!(vlog.size, 0);
    }

    #[tokio::test]
    async fn test_set_head_tail() {
        let root = tempdir().unwrap();
        let path = root.path().join("vlog_set_head_tail");

        let mut vlog = ValueLog::new(path).await.unwrap();

        assert_eq!(vlog.head_offset, 0);
        assert_eq!(vlog.tail_offset, 0);
        assert_eq!(vlog.size, 0);

        let new_head = 100;
        let new_tail = 50;

        vlog.set_head(new_head);
        vlog.set_tail(new_tail);
        assert_eq!(vlog.head_offset, new_head);
        assert_eq!(vlog.tail_offset, new_tail);
    }

    #[tokio::test]
    async fn test_vlog_entry_new() {
        let key = "test_key";
        let val = "test_val";
        let time = Utc::now();
        let is_tombstone = false;
        let entry = ValueLogEntry::new(key.len(), val.len(), key, val, time, is_tombstone);

        assert_eq!(entry.ksize, key.len());
        assert_eq!(entry.vsize, val.len());
        assert_eq!(entry.key, key.as_bytes().to_vec());
        assert_eq!(entry.value, val.as_bytes().to_vec());
        assert_eq!(entry.is_tombstone, is_tombstone);
        assert_eq!(entry.created_at, time);
    }

    #[tokio::test]
    async fn test_vlog_entry_serialize() {
        let key = "test_key";
        let val = "test_val";
        let time = Utc::now();
        let is_tombstone = false;
        let entry = ValueLogEntry::new(key.len(), val.len(), key, val, time, is_tombstone);

        let expected_entry_len = SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + key.len() + val.len() + SIZE_OF_U8;

        let serialized_entry = entry.serialize();

        assert_eq!(serialized_entry.len(), expected_entry_len);
    }
}
