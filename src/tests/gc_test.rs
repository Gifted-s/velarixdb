#[cfg(test)]
mod tests {
    use crate::consts::{SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8};
    use crate::err::Error;
    use crate::gc::gc::GC;
    use crate::storage::{DataStore, SizeUnit};
    use crate::tests::workload::Workload;
    use crate::types::Key;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::RwLock;

    async fn setup(
        store: Arc<RwLock<DataStore<'static, Key>>>,
        workload: &Workload,
        prepare_delete: bool,
    ) -> Result<(), Error> {
        let _ = env_logger::builder().is_test(true).try_init();
        let (_, data) = workload.generate_workload_data_as_vec();
        workload.insert_parallel(&data, store.clone()).await?;
        if prepare_delete {
            let _ = store.write().await.put("test_key", "test").await?;
            let _ = store.write().await.delete("test_key").await?;
        }
        return Ok(());
    }
    // Generate test to find keys after compaction
    #[tokio::test]
    async fn datastore_gc_test_success() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("gc_test_1"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        if let Err(err) = setup(store.clone(), &workload, true).await {
            log::error!("Setup failed {}", err);
            return;
        }
        let storage_reader = store.read().await;
        let config = storage_reader.gc.config.clone();
        let res = GC::gc_handler(
            &config,
            Arc::clone(&storage_reader.gc_table),
            Arc::clone(&storage_reader.gc_log),
            Arc::clone(&storage_reader.key_range),
            Arc::clone(&storage_reader.read_only_memtables),
            Arc::clone(&storage_reader.gc_updated_entries),
            Arc::clone(&storage_reader.gc.punch_marker),
        )
        .await;

        #[cfg(target_os = "linux")]
        {
            assert!(res.is_ok())
        }
    }

    #[tokio::test]
    async fn datastore_gc_test_unsupported_platform() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("gc_test_2"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        if let Err(err) = setup(store.clone(), &workload, true).await {
            log::error!("Setup failed {}", err);
            return;
        }
        let storage_reader = store.read().await;
        let config = storage_reader.gc.config.clone();
        let res = GC::gc_handler(
            &config,
            Arc::clone(&storage_reader.gc_table),
            Arc::clone(&storage_reader.gc_log),
            Arc::clone(&storage_reader.key_range),
            Arc::clone(&storage_reader.read_only_memtables),
            Arc::clone(&storage_reader.gc_updated_entries),
            Arc::clone(&storage_reader.gc.punch_marker),
        )
        .await;

        #[cfg(not(target_os = "linux"))]
        {
            assert!(res.is_ok());
        }
    }

    #[tokio::test]
    async fn datastore_gc_test_tail_shifted() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("gc_test_3"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        if let Err(err) = setup(store.clone(), &workload, true).await {
            log::error!("Setup failed {}", err);
            return;
        }

        let storage_reader = store.read().await;
        let config = storage_reader.gc.config.clone();
        let initial_tail_offset = storage_reader.gc_log.read().await.tail_offset;

        let _ = GC::gc_handler(
            &config,
            Arc::clone(&storage_reader.gc_table),
            Arc::clone(&storage_reader.gc_log),
            Arc::clone(&storage_reader.key_range),
            Arc::clone(&storage_reader.read_only_memtables),
            Arc::clone(&storage_reader.gc_updated_entries),
            Arc::clone(&storage_reader.gc.punch_marker),
        )
        .await;
        drop(storage_reader);
        // call a put operation to sync gc with memtable
        let _ = store.write().await.put("test_key", "test_val").await;
        assert!(store.read().await.gc.vlog.read().await.tail_offset >= initial_tail_offset);
    }

    #[tokio::test]
    async fn datastore_gc_test_free_before_synchronization() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("gc_test_free"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        if let Err(err) = setup(store.clone(), &workload, true).await {
            log::error!("Setup failed {}", err);
            return;
        }
        let storage_reader = store.read().await;
        let config = storage_reader.gc.config.clone();
        let initial_tail_offset = storage_reader.gc_log.read().await.tail_offset;

        let _ = GC::gc_handler(
            &config,
            Arc::clone(&storage_reader.gc_table),
            Arc::clone(&storage_reader.gc_log),
            Arc::clone(&storage_reader.key_range),
            Arc::clone(&storage_reader.read_only_memtables),
            Arc::clone(&storage_reader.gc_updated_entries),
            Arc::clone(&storage_reader.gc.punch_marker),
        )
        .await;
        drop(storage_reader);
        // no tail should happen because we have not synchronize gc entries with store memtable±±
        assert!(store.read().await.gc.vlog.read().await.tail_offset == initial_tail_offset);
    }

    #[tokio::test]
    async fn datastore_gc_test_tail_shifted_to_correct_position() {
        let bytes_to_scan_for_garbage_colection = SizeUnit::Bytes.to_bytes(100);
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("gc_test_4"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let workload_size = 5;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        if let Err(err) = setup(store.clone(), &workload, true).await {
            log::error!("Setup failed {}", err);
            return;
        }
        let string_length = 5;
        let vaue_len = 3;
        let storage_reader = store.read().await;
        let mut config = storage_reader.gc.config.clone();

        let initial_tail_offset = storage_reader.gc_log.read().await.tail_offset;
        config.gc_chunk_size = bytes_to_scan_for_garbage_colection;
        let _ = GC::gc_handler(
            &config,
            Arc::clone(&storage_reader.gc_table),
            Arc::clone(&storage_reader.gc_log),
            Arc::clone(&storage_reader.key_range),
            Arc::clone(&storage_reader.read_only_memtables),
            Arc::clone(&storage_reader.gc_updated_entries),
            Arc::clone(&storage_reader.gc.punch_marker),
        )
        .await;
        drop(storage_reader);
        // call a put operation to sync gc with memtable
        let _ = store.write().await.put("test_key", "test_val").await;
        let max_extention_length = SIZE_OF_U32   // Key Size(for fetching key length)
        +SIZE_OF_U32            // Value Length(for fetching value length)
        + SIZE_OF_U64           // Date Length
        + SIZE_OF_U8            // Tombstone marker len
        + string_length         // Key Len
        + vaue_len; // Value Len
        assert!(
            store.read().await.gc.vlog.read().await.tail_offset
                <= initial_tail_offset + bytes_to_scan_for_garbage_colection + max_extention_length
        );
    }

    #[tokio::test]
    async fn datastore_gc_test_head_shifted() {
        let bytes_to_scan_for_garbage_colection = SizeUnit::Bytes.to_bytes(100);
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("gc_test_5"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let _ = store.write().await.put("test_key", "test_val").await;
        let _ = store.write().await.delete("test_key").await;
        let workload_size = 2;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        if let Err(err) = setup(store.clone(), &workload, true).await {
            log::error!("Setup failed {}", err);
            return;
        }
        (store.write().await).gc.config.gc_chunk_size = bytes_to_scan_for_garbage_colection;
        let storage_reader = store.read().await;
        let initial_head_offset = storage_reader.gc_log.read().await.head_offset;
        let _ = GC::gc_handler(
            &storage_reader.gc.config.clone(),
            Arc::clone(&storage_reader.gc_table),
            Arc::clone(&storage_reader.gc_log),
            Arc::clone(&storage_reader.key_range),
            Arc::clone(&storage_reader.read_only_memtables),
            Arc::clone(&storage_reader.gc_updated_entries),
            Arc::clone(&storage_reader.gc.punch_marker),
        )
        .await;
        drop(storage_reader);
        // call a put operation to sync gc with memtable
        let _ = store.write().await.put("test_key", "test_val").await;
        assert!(store.read().await.gc.vlog.read().await.head_offset != initial_head_offset);
    }

    #[tokio::test]
    async fn datastore_gc_test_no_entry_to_collect() {
        let prepare_delete = false;
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("gc_test_no_delete"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        if let Err(err) = setup(store.clone(), &workload, prepare_delete).await {
            log::error!("Setup failed {}", err);
            return;
        }
        let storage_reader = store.read().await;
        let config = storage_reader.gc.config.clone();
        let initial_tail_offset = storage_reader.gc_log.read().await.tail_offset;

        let _ = GC::gc_handler(
            &config,
            Arc::clone(&storage_reader.gc_table),
            Arc::clone(&storage_reader.gc_log),
            Arc::clone(&storage_reader.key_range),
            Arc::clone(&storage_reader.read_only_memtables),
            Arc::clone(&storage_reader.gc_updated_entries),
            Arc::clone(&storage_reader.gc.punch_marker),
        )
        .await;
        drop(storage_reader);
        // no tail should happen because no entries to collect
        assert!(store.read().await.gc.vlog.read().await.tail_offset == initial_tail_offset);
    }
}
