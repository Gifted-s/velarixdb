#[cfg(test)]
mod tests {

    use crate::consts::DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL_MILLI;
    use crate::storage::DataStore;
    use crate::helpers;
    use futures::future::join_all;
    use log::info;
    use tempfile::tempdir;
    use tokio::time::{sleep, Duration};
    use crate::err::Error;
    use crate::err::Error::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::fs::{self};
    use tokio::sync::RwLock;

    fn init() {
         let _ = env_logger::builder().is_test(true).try_init();
    }
    // Generate test to find keys after compaction
    #[tokio::test]
    async fn datastore_create_asynchronous() {
        init();
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("bump1"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();
        // Specify the number of random strings to generate
        let num_strings = 20000; // 100k

        // Specify the length of each random string
        let string_length = 5;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::with_capacity(num_strings);
        for _ in 0..num_strings {
            let random_string = helpers::generate_random_id(string_length);
            random_strings.push(random_string);
        }
        let sg = Arc::new(RwLock::new(s_engine));

        let tasks = random_strings.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                value.put(&k, "boy").await
            })
        });

        let all_results = join_all(tasks).await;
        for tokio_response in all_results {
            match tokio_response {
                Ok(entry) => match entry {
                    Ok(is_inserted) => {
                        assert_eq!(is_inserted, true)
                    }
                    Err(err) => assert!(false, "{}", err.to_string()),
                },
                Err(err) => {
                    assert!(false, "{}", err.to_string())
                }
            }
        }
        // println!("Write completed ");
        // sleep(Duration::from_millis(
        //     DEFAULT_COMPACTION_FLUSH_LISTNER_INTERVAL_MILLI * 3,
        // ))
        // .await;
        // println!("About to start reading");
        // println!("Compaction completed !");
        random_strings.sort();
        let tasks = random_strings
            .get(0..(num_strings / 2))
            .unwrap_or_default()
            .iter()
            .map(|k| {
                let s_engine = Arc::clone(&sg);
                let key = k.clone();
                tokio::spawn(async move {
                    let value = s_engine.read().await;
                    let nn = value.get(&key).await;
                    return nn;
                })
            });
        let all_results = join_all(tasks).await;
        for tokio_response in all_results {
            match tokio_response {
                Ok(entry) => match entry {
                    Ok(v) => {
                        assert_eq!(v.0, b"boy");
                    }
                    Err(err) => assert!(false, "Error: {}", err.to_string()),
                },
                Err(err) => {
                    assert!(false, "{}", err.to_string())
                }
            }
        }

        //  let _ = fs::remove_dir_all(path.clone()).await;
    }



    

    #[tokio::test]
    async fn datastore_create_synchronous() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("bump2"));
        let mut s_engine = DataStore::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 1000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = helpers::generate_random_id(string_length);
            random_strings.push(random_string);
        }

        // Insert the generated random strings
        for (_, s) in random_strings.iter().enumerate() {
            s_engine.put(s, "boyode").await.unwrap();
        }
        // let compactor = Compactor::new();

        let compaction_opt = s_engine.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    s_engine.buckets.read().await.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    s_engine.filters.read().await.len()
                );
            }
            Err(err) => {
                println!("Error during compaction {}", err)
            }
        }

        // random_strings.sort();
        for k in random_strings {
            let result = s_engine.get(&k).await;
            match result {
                Ok((value, _)) => {
                    assert_eq!(value, b"boyode");
                }
                Err(_) => {
                    assert!(false, "No err should be found");
                }
            }
        }

        let _ = fs::remove_dir_all(path.clone()).await;
        // sort to make fetch random
    }

    #[tokio::test]
    async fn datastore_compaction_asynchronous() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("bump3"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 50000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = helpers::generate_random_id(string_length);
            random_strings.push(random_string);
        }
        // for k in random_strings.clone() {
        //     s_engine.put(&k, "boyode").await.unwrap();
        // }
        let sg = Arc::new(RwLock::new(s_engine));
        let binding = random_strings.clone();
        let tasks = binding.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                value.put(&k, "boyode").await
            })
        });

        //Collect the results from the spawned tasks
        for task in tasks {
            tokio::select! {
                result = task => {
                    match result{
                        Ok(v_opt)=>{
                            match v_opt{
                                Ok(v) => {
                                    assert_eq!(v, true)
                                },
                                Err(_) => { assert!(false, "No err should be found")},
                            }
                             }
                        Err(_) =>  assert!(false, "No err should be found") }
                    //println!("{:?}",result);
                }
            }
        }

        // sort to make fetch random
        random_strings.sort();
        let key = &random_strings[0];

        let get_res1 = sg.read().await.get(key).await;
        let get_res2 = sg.read().await.get(key).await;
        let get_res3 = sg.read().await.get(key).await;
        let get_res4 = sg.read().await.get(key).await;
        match get_res1 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        match get_res2 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        match get_res3 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }
        match get_res4 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        let del_res = sg.write().await.delete(key).await;
        match del_res {
            Ok(v) => {
                assert_eq!(v, true)
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        let get_res2 = sg.read().await.get(key).await;
        match get_res2 {
            Ok(_) => {
                assert!(false, "Should not be found after compaction")
            }
            Err(err) => {
                assert_eq!(Error::NotFoundInDB.to_string(), err.to_string())
            }
        }

        let _ = sg.write().await.flush_all_memtables().await;
        sg.write().await.active_memtable.clear();

        // We expect tombstone to be flushed to an sstable at this point
        let get_res2 = sg.read().await.get(key).await;
        match get_res2 {
            Ok(_) => {
                assert!(false, "Should not be found after compaction")
            }
            Err(err) => {
                assert_eq!(Error::NotFoundInDB.to_string(), err.to_string())
            }
        }

        let compaction_opt = sg.write().await.run_compaction().await;
        // Insert the generated random strings
        // let compactor = Compactor::new();
        // let compaction_opt = sg.write().await.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    sg.read().await.buckets.read().await.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    sg.read().await.filters.read().await.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }

        // Insert the generated random strings
        let get_res3 = sg.read().await.get(key).await;
        match get_res3 {
            Ok(_) => {
                assert!(false, "Deleted key should be found as tumbstone");
            }

            Err(err) => {
                println!("{}", err);
                if err.to_string() != NotFoundInDB.to_string() && err.to_string() != NotFoundInDB.to_string() {
                    println!("{}", err);
                    assert!(false, "Key should be mapped to tombstone or deleted from all sstables")
                }
            }
        }
        let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn datastore_update_asynchronous() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("bump4"));
        let mut s_engine = DataStore::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 6000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = helpers::generate_random_id(string_length);
            random_strings.push(random_string);
        }
        for k in random_strings.clone() {
            s_engine.put(&k, "boyode").await.unwrap();
        }
        let sg = Arc::new(RwLock::new(s_engine));
        let binding = random_strings.clone();
        let tasks = binding.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                value.put(&k, "boyode").await
            })
        });

        // Collect the results from the spawned tasks
        for task in tasks {
            tokio::select! {
                result = task => {
                    match result{
                        Ok(v_opt)=>{
                            match v_opt{
                                Ok(v) => {
                                    assert_eq!(v, true)
                                },
                                Err(_) => { assert!(false, "No err should be found")},
                            }
                             }
                        Err(_) =>  assert!(false, "No err should be found") }
                }
            }
        }
        // // sort to make fetch random
        random_strings.sort();
        let key = &random_strings[0];
        let updated_value = "updated_key";

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        let update_res = sg.write().await.update(key, updated_value).await;
        match update_res {
            Ok(v) => {
                assert_eq!(v, true)
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }
        let _ = sg.write().await.flush_all_memtables().await;
        sg.write().await.active_memtable.clear();

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((value, _)) => {
                assert_eq!(value, updated_value.as_bytes().to_vec())
            }
            Err(_) => {
                assert!(false, "Should not run")
            }
        }

        // // Run compaction
        let compaction_opt = sg.write().await.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    sg.read().await.buckets.read().await.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    sg.read().await.filters.read().await.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((value, _)) => {
                assert_eq!(value, updated_value.as_bytes().to_vec())
            }
            Err(_) => {
                assert!(false, "Should not run")
            }
        }
        let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn datastore_deletion_asynchronous() {
        let root = tempdir().unwrap();
        let path = PathBuf::from(root.path().join("bump5"));
        let s_engine = DataStore::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 50000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = helpers::generate_random_id(string_length);
            random_strings.push(random_string);
        }
        // for k in random_strings.clone() {
        //     s_engine.put(&k, "boyode").await.unwrap();
        // }
        let sg = Arc::new(RwLock::new(s_engine));
        let binding = random_strings.clone();
        let tasks = binding.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                value.put(&k, "boyode").await
            })
        });
        let key = "aunkanmi";
        let _ = sg.write().await.put(key, "boyode").await;
        // // Collect the results from the spawned tasks
        for task in tasks {
            tokio::select! {
                result = task => {
                    match result{
                        Ok(v_opt)=>{
                            match v_opt{
                                Ok(v) => {
                                    assert_eq!(v, true)
                                },
                                Err(_) => { assert!(false, "No err should be found")},
                            }
                             }
                        Err(_) =>  assert!(false, "No err should be found") }
                }
            }
        }
        // sort to make fetch random
        random_strings.sort();
        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((value, _)) => {
                assert_eq!(value, "boyode".as_bytes().to_vec());
            }
            Err(err) => {
                assert_ne!(key.as_bytes().to_vec(), err.to_string().as_bytes().to_vec());
            }
        }

        let del_res = sg.write().await.delete(key).await;
        match del_res {
            Ok(v) => {
                assert_eq!(v, true);
            }
            Err(err) => {
                assert!(err.to_string().is_empty())
            }
        }

        let _ = sg.write().await.flush_all_memtables().await;

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((v, d)) => {
                println!("{:?} {}", v, d);
                assert!(false, "Should not be executed")
            }
            Err(err) => {
                assert_eq!(NotFoundInDB.to_string(), err.to_string())
            }
        }

        let compaction_opt = sg.write().await.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    sg.read().await.buckets.read().await.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    sg.read().await.filters.read().await.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }

        // Insert the generated random strings
        println!("trying to get this after compaction {}", key);
        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((g, r)) => {
                println!("{:?}. {:?}", g, r);
                assert!(false, "Should not ne executed")
            }
            Err(err) => {
                if err.to_string() != NotFoundInDB.to_string()
                    && err.to_string() != KeyNotFoundByAnyBloomFilterError.to_string()
                {
                    assert!(false, "Key should be mapped to tombstone or deleted from all sstables")
                }
            }
        }
        let _ = fs::remove_dir_all(path.clone()).await;
    }
}
