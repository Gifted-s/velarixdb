#[cfg(test)]
mod tests {
    use crate::consts::{SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8};
    use crate::err::Error;
    use crate::gc::gc::GC;
    use crate::storage::{DataStore, SizeUnit};
    use crate::tests::workload::{generate_workload_data_as_vec, insert_parallel};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::fs::{self};
    use tokio::sync::RwLock;

    async fn setup(
        store: Arc<RwLock<DataStore<'static, Vec<u8>>>>,
        workload_size: usize,
        key_len: usize,
        val_len: usize,
        write_read_ratio: f64,
    ) -> Result<(), Error> {
        let _ = env_logger::builder().is_test(true).try_init();
        let (_, write_workload) = generate_workload_data_as_vec(workload_size, key_len, val_len, write_read_ratio);
        insert_parallel(&write_workload, store).await
    }
    // Generate test to find keys after compaction
    #[tokio::test]
    async fn datastore_gc_test_success() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("bump1"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        if let Err(err) = setup(store.clone(), workload_size, key_len, val_len, write_read_ratio).await {
            log::error!("Setup failed {}", err);
            return;
        }
        let storage_eng = store.read().await;
        let config = storage_eng.gc.config.clone();
        let memtable = storage_eng.gc_table.clone();
        let vlog = storage_eng.gc_log.clone();
        let filters = storage_eng.filters.clone();
        let key_range = storage_eng.key_range.clone();
        let read_only_memtables = storage_eng.read_only_memtables.clone();
        let gc_updated_entries = storage_eng.gc_updated_entries.clone();
        drop(storage_eng);
        let res = GC::gc_handler(
            &config,
            memtable,
            vlog,
            filters,
            key_range,
            read_only_memtables,
            gc_updated_entries,
        )
        .await;

        #[cfg(target_os = "linux")]
        {
            assert!(res.is_ok())
        }
        let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn datastore_gc_test_unsupported_platform() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("bump2"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        if let Err(err) = setup(store.clone(), workload_size, key_len, val_len, write_read_ratio).await {
            log::error!("Setup failed {}", err);
            return;
        }
        let storage_eng = store.read().await;
        let config = storage_eng.gc.config.clone();
        let memtable = storage_eng.gc_table.clone();
        let vlog = storage_eng.gc_log.clone();
        let filters = storage_eng.filters.clone();
        let key_range = storage_eng.key_range.clone();
        let read_only_memtables = storage_eng.read_only_memtables.clone();
        let gc_updated_entries = storage_eng.gc_updated_entries.clone();
        drop(storage_eng);
        let res = GC::gc_handler(
            &config,
            memtable,
            vlog,
            filters,
            key_range,
            read_only_memtables,
            gc_updated_entries,
        )
        .await;

        #[cfg(not(target_os = "linux"))]
        {
            assert!(res.is_err());
            match res.as_ref().err().unwrap() {
                Error::GCError(err) => {
                    assert_eq!(err.to_string(), "Unsuported OS for garbage collection, err message `File system does not support file punch hole`")
                }
                _ => {
                    assert!(false, "Invalid error kind")
                }
            }
        }

        let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn datastore_gc_test_tail_shifted() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("bump3"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        if let Err(err) = setup(store.clone(), workload_size, key_len, val_len, write_read_ratio).await {
            log::error!("Setup failed {}", err);
            return;
        }
        let storage_eng = store.read().await;
        let config = storage_eng.gc.config.clone();
        let memtable = storage_eng.gc_table.clone();
        let vlog = storage_eng.gc_log.clone();
        let initial_tail_offset = vlog.read().await.tail_offset;
        let filters = storage_eng.filters.clone();
        let key_range = storage_eng.key_range.clone();
        let read_only_memtables = storage_eng.read_only_memtables.clone();
        let gc_updated_entries = storage_eng.gc_updated_entries.clone();
        let _ = GC::gc_handler(
            &config,
            memtable,
            vlog,
            filters,
            key_range,
            read_only_memtables,
            gc_updated_entries,
        )
        .await;

        assert!(storage_eng.gc.vlog.read().await.tail_offset != initial_tail_offset);
        let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn datastore_gc_test_tail_shifted_to_correct_position() {
        let bytes_to_scan_for_garbage_colection = SizeUnit::Kilobytes.to_bytes(2);
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("bump4"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        if let Err(err) = setup(store.clone(), workload_size, key_len, val_len, write_read_ratio).await {
            log::error!("Setup failed {}", err);
            return;
        }
        let string_length = 5;
        let vaue_len = 3;
        let storage_eng = store.read().await;
        let mut config = storage_eng.gc.config.clone();
        let memtable = storage_eng.gc_table.clone();
        let vlog = storage_eng.gc_log.clone();
        let initial_tail_offset = vlog.read().await.tail_offset;
        let filters = storage_eng.filters.clone();
        let key_range = storage_eng.key_range.clone();
        let read_only_memtables = storage_eng.read_only_memtables.clone();
        let gc_updated_entries = storage_eng.gc_updated_entries.clone();
        config.gc_chunk_size = bytes_to_scan_for_garbage_colection;
        let _ = GC::gc_handler(
            &config,
            memtable,
            vlog,
            filters,
            key_range,
            read_only_memtables,
            gc_updated_entries,
        )
        .await;
        let max_extention_length = SIZE_OF_U32   // Key Size(for fetching key length)
        +SIZE_OF_U32            // Value Length(for fetching value length)
        + SIZE_OF_U64           // Date Length
        + SIZE_OF_U8            // Tombstone marker len
        + string_length         // Key Len
        + vaue_len; // Value Len
        assert!(
            storage_eng.gc.vlog.read().await.tail_offset
                <= initial_tail_offset + bytes_to_scan_for_garbage_colection + max_extention_length
        );
        let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn datastore_gc_test_head_shifted() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("bump5"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        let store = Arc::new(RwLock::new(s_engine));
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        if let Err(err) = setup(store.clone(), workload_size, key_len, val_len, write_read_ratio).await {
            log::error!("Setup failed {}", err);
            return;
        }
        let storage_eng = store.read().await;
        let config = storage_eng.gc.config.clone();
        let memtable = storage_eng.gc_table.clone();
        let vlog = storage_eng.gc_log.clone();
        let initial_head_offset = vlog.read().await.head_offset;
        let filters = storage_eng.filters.clone();
        let key_range = storage_eng.key_range.clone();
        let read_only_memtables = storage_eng.read_only_memtables.clone();
        let gc_updated_entries = storage_eng.gc_updated_entries.clone();
        let _ = GC::gc_handler(
            &config,
            memtable,
            vlog,
            filters,
            key_range,
            read_only_memtables,
            gc_updated_entries,
        )
        .await;

        assert!(storage_eng.gc.vlog.read().await.head_offset != initial_head_offset);
        let _ = fs::remove_dir_all(path.clone()).await;
    }
}
