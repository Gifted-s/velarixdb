#[cfg(test)]
mod tests {
    use crate::err::Error;
    use crate::storage::DataStore;
    use crate::tests::workload::Workload;
    use futures::future::join_all;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::fs::{self};
    use tokio::sync::RwLock;

    fn setup() {
        let _ = env_logger::builder().is_test(true).try_init();
    }
    #[tokio::test]
    async fn datastore_create_new() {
        setup();
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("store_test_1"));
        let store = DataStore::new(path.clone()).await;
        assert!(store.is_ok())
    }

    #[tokio::test]
    async fn datastore_put_test() {
        setup();
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("store_test_2"));
        let store = DataStore::new(path.clone()).await.unwrap();
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        let (_, write_workload) = workload.generate_workload_data_as_map();

        let store_ref = Arc::new(RwLock::new(store));
        let write_tasks = write_workload.iter().map(|e| {
            let store_inner = Arc::clone(&store_ref);
            let key = e.0.to_owned();
            let val = e.1.to_owned();
            tokio::spawn(async move {
                let mut writer = store_inner.write().await;
                writer.put(key, val).await
            })
        });

        let all_results = join_all(write_tasks).await;
        for tokio_res in all_results {
            assert!(tokio_res.is_ok());
            assert!(tokio_res.as_ref().unwrap().is_ok());
            assert_eq!(tokio_res.unwrap().unwrap(), true);
        }
    }

    #[tokio::test]
    async fn datastore_test_put_and_get() {
        setup();
        let root = PathBuf::new();
        let path = PathBuf::from(root.join("store_test_3"));
        let store = DataStore::new(path.clone()).await.unwrap();
        let workload_size = 20000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 1.0;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        let (read_workload, write_workload) = workload.generate_workload_data_as_map();
        let store_ref = Arc::new(RwLock::new(store));
        let write_tasks = write_workload.iter().map(|e| {
            let store_inner = Arc::clone(&store_ref);
            let key = e.0.to_owned();
            let val = e.1.to_owned();
            tokio::spawn(async move {
                let mut value = store_inner.write().await;
                value.put(key, val).await
            })
        });

        let all_results = join_all(write_tasks).await;
        for tokio_res in all_results {
            assert!(tokio_res.is_ok());
            assert!(tokio_res.as_ref().unwrap().is_ok());
            assert_eq!(tokio_res.unwrap().unwrap(), true);
        }

        let read_tasks = read_workload.iter().map(|e| {
            let store_inner = Arc::clone(&store_ref);
            let key = e.0.to_owned();
            tokio::spawn(async move {
                match store_inner.read().await.get(key.to_owned()).await {
                    Ok((value, _)) => Ok((key, value)),
                    Err(err) => return Err(err),
                }
            })
        });

        let all_results = join_all(read_tasks).await;

        for tokio_res in all_results {
            assert!(tokio_res.is_ok());
            assert!(tokio_res.as_ref().unwrap().is_ok());
            let (key, value) = tokio_res.unwrap().unwrap();
            assert_eq!(value, *write_workload.get(&key).unwrap());
        }
    }

    #[tokio::test]
    async fn datastore_test_put_and_get_concurrent() {
        setup();
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("store_test_4"));
        let store = DataStore::new(path.clone()).await.unwrap();
        let workload_size = 1;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 0.5;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        let (_, write_workload) = workload.generate_workload_data_as_vec();
        let key = &write_workload[0].key;
        let entry1 = write_workload[0].to_owned();
        let mut entry2 = entry1.clone();
        let mut entry3 = entry1.clone();
        let mut entry4 = entry1.clone();
        let mut entry5 = entry1.clone();
        entry2.val = b"val2".to_vec();
        entry3.val = b"val3".to_vec();
        entry4.val = b"val4".to_vec();
        entry5.val = b"val5".to_vec();

        let concurrent_write_workload = vec![entry1, entry2, entry3, entry4, entry5.to_owned()];
        let store_ref = Arc::new(RwLock::new(store));

        let concurrent_write_tasks = concurrent_write_workload.iter().map(|e| {
            let store_inner = Arc::clone(&store_ref);
            let key = e.key.to_owned();
            let val = e.val.to_owned();
            tokio::spawn(async move {
                let mut value = store_inner.write().await;
                value.put(key, val).await
            })
        });

        let all_results = join_all(concurrent_write_tasks).await;
        for tokio_res in all_results {
            assert!(tokio_res.is_ok());
            assert!(tokio_res.as_ref().unwrap().is_ok());
            assert_eq!(tokio_res.unwrap().unwrap(), true);
        }

        let res = store_ref.read().await.get(std::str::from_utf8(&key).unwrap()).await;
        assert!(res.is_ok());
        // Even though the write of thesame key happened concurrently, we expect the last entry to reflect
        assert_eq!(res.unwrap().0, entry5.val);
    }

    #[tokio::test]
    async fn datastore_test_seqential_put() {
        setup();
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("store_test_5"));
        let mut store = DataStore::new(path.clone()).await.unwrap();
        let workload_size = 5000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 1.0;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        let (_, write_workload) = workload.generate_workload_data_as_map();
        for e in write_workload.iter() {
            let key = e.0.to_owned();
            let val = e.1.to_owned();
            let res = store.put(key, val).await;
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), true);
        }
    }

    #[tokio::test]
    async fn datastore_test_sequential_put_and_get() {
        setup();
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("store_test_6"));
        let mut store = DataStore::new(path.clone()).await.unwrap();
        let workload_size = 5000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 1.0;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        let (read_workload, write_workload) = workload.generate_workload_data_as_vec();

        for e in write_workload.iter() {
            let res = store.put(e.key.to_owned(), e.val.to_owned()).await;
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), true);
        }
        for e in read_workload.iter() {
            let res = store.get(&e.key).await;
            assert!(res.is_ok());
            let w = write_workload.iter().find(|e1| e1.key == e.key).unwrap();
            assert_eq!(res.unwrap().0, w.val);
        }
    }

    #[tokio::test]
    async fn datastore_not_found() {
        setup();
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("store_test_7"));
        let store = DataStore::new(path.clone()).await.unwrap();
        let workload_size = 10000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 1.0;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        let (_, write_workload) = workload.generate_workload_data_as_vec();
        let store_ref = Arc::new(RwLock::new(store));
        let res = workload.insert_parallel(&write_workload, store_ref.clone()).await;
        if !res.is_ok() {
            log::error!("Insert failed {:?}", res.err());
            return;
        }

        let not_found_key = "**_not_found_**";
        let res = store_ref.read().await.get(not_found_key).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), Error::NotFoundInDB.to_string());
    }

    #[tokio::test]
    async fn datastore_compaction_asynchronous() {
        setup();
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("store_test_8"));
        let store = DataStore::new(path.clone()).await.unwrap();
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 1.0;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        let (_, write_workload) = workload.generate_workload_data_as_vec();
        let store_ref = Arc::new(RwLock::new(store));
        let res = workload.insert_parallel(&write_workload, store_ref.clone()).await;
        if !res.is_ok() {
            log::error!("Insert failed {:?}", res.err());
            return;
        }
        let key1 = &write_workload[0].key;
        let key2 = &write_workload[1].key;
        let key3 = &write_workload[2].key;
        let key4 = &write_workload[3].key;
        let res1 = store_ref.read().await.get(key1).await;
        let res2 = store_ref.read().await.get(key2).await;
        let res3 = store_ref.read().await.get(key3).await;
        let res4 = store_ref.read().await.get(key4).await;

        assert!(res1.is_ok());
        assert!(res2.is_ok());
        assert!(res3.is_ok());
        assert!(res4.is_ok());
        assert_eq!(res1.unwrap().0, write_workload[0].val);
        assert_eq!(res2.unwrap().0, write_workload[1].val);
        assert_eq!(res3.unwrap().0, write_workload[2].val);
        assert_eq!(res4.unwrap().0, write_workload[3].val);

        let res = store_ref.write().await.delete(key1).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);

        let res = store_ref.read().await.get(key1).await;
        assert!(res.is_err());
        assert_eq!(Error::NotFoundInDB.to_string(), res.err().unwrap().to_string());

        let _ = store_ref.write().await.flush_all_memtables().await;

        // We expect tombstone to be flushed to an sstable at this point
        let res = store_ref.read().await.get(key1).await;
        assert!(res.is_err());
        assert_eq!(Error::NotFoundInDB.to_string(), res.err().unwrap().to_string());

        let comp_res = store_ref.write().await.run_compaction().await;
        assert!(comp_res.is_ok());

        // Fetch after compaction
        let res = store_ref.read().await.get(key1).await;
        assert!(res.is_err());
        assert_eq!(Error::NotFoundInDB.to_string(), res.err().unwrap().to_string());
    }

    #[tokio::test]
    async fn datastore_update_asynchronous() {
        setup();
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("store_test_9"));
        let store = DataStore::new(path.clone()).await.unwrap();
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 1.0;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        let (_, write_workload) = workload.generate_workload_data_as_vec();
        let store_ref = Arc::new(RwLock::new(store));
        let res = workload.insert_parallel(&write_workload, store_ref.clone()).await;
        if !res.is_ok() {
            log::error!("Insert failed {:?}", res.err());
            return;
        }

        let key1 = &write_workload[0].key;
        let updated_value = "updated_key".as_bytes().to_vec();

        let res = store_ref.read().await.get(key1).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap().0, write_workload[0].val);

        let res = store_ref.write().await.update(key1, &updated_value).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);

        let res = store_ref.read().await.get(key1).await;
        assert!(res.is_ok());
        assert_ne!(res.as_ref().unwrap().0, write_workload[0].val);
        assert_eq!(res.as_ref().unwrap().0, updated_value);

        let res = store_ref.write().await.flush_all_memtables().await;
        assert!(res.is_ok());

        let res = store_ref.read().await.get(key1).await;
        println!("RESPONSE {:?}", res);
        assert!(res.is_ok());
        assert_eq!(res.unwrap().0, updated_value);

        // // Run compaction
        let comp_res = store_ref.write().await.run_compaction().await;
        assert!(comp_res.is_ok());

        let res = store_ref.read().await.get(key1).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap().0, updated_value);
    }

    #[tokio::test]
    async fn datastore_deletion_asynchronous() {
        setup();
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("store_test_10"));
        let store = DataStore::new(path.clone()).await.unwrap();
        let workload_size = 15000;
        let key_len = 5;
        let val_len = 5;
        let write_read_ratio = 1.0;
        let workload = Workload::new(workload_size, key_len, val_len, write_read_ratio);
        let (_, write_workload) = workload.generate_workload_data_as_vec();
        let store_ref = Arc::new(RwLock::new(store));
        let res = workload.insert_parallel(&write_workload, store_ref.clone()).await;
        if !res.is_ok() {
            log::error!("Insert failed {:?}", res.err());
            return;
        }
        let key1 = &write_workload[0].key;

        let res = store_ref.read().await.get(key1).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap().0, write_workload[0].val);

        let res = store_ref.write().await.delete(key1).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);

        let res = store_ref.write().await.flush_all_memtables().await;
        assert!(res.is_ok());

        let res = store_ref.read().await.get(key1).await;
        assert!(res.is_err());
        assert_eq!(Error::NotFoundInDB.to_string(), res.err().unwrap().to_string());

        let comp_opt = store_ref.write().await.run_compaction().await;
        assert!(comp_opt.is_ok());

        let res = store_ref.read().await.get(key1).await;
        assert!(res.is_err());
        assert_eq!(Error::NotFoundInDB.to_string(), res.err().unwrap().to_string());
        let _ = fs::remove_dir_all(path.clone()).await;
    }
}
