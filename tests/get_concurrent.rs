use std::{collections::HashMap, sync::Arc};

use futures::future::join_all;
use tempfile::tempdir;
use tokio::sync::RwLock;
use velarixdb::db::DataStore;


#[tokio::test]
async fn test_get_concurrent() {
    let root = tempdir().unwrap();
    let path = root.path().join("velarix");
    let store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error
    let mut entries = HashMap::new();
    entries.insert("apple", "tim cook");
    entries.insert("google", "sundar pichai");
    entries.insert("nvidia", "jensen huang");
    entries.insert("microsoft", "satya nadella");
    entries.insert("meta", "mark zuckerberg");
    entries.insert("openai", "sam altman");

    let store_ref = Arc::new(RwLock::new(store));
    let writes = entries.iter().map(|(k, v)| {
        let store_inner = Arc::clone(&store_ref);
        let key = k.to_owned();
        let val = v.to_owned();
        tokio::spawn(async move {
            let mut writer = store_inner.write().await;
            writer.put(key, val).await
        })
    });
    let all_results = join_all(writes).await;
    for tokio_res in all_results {
        assert!(tokio_res.is_ok());
        assert!(tokio_res.as_ref().unwrap().is_ok());
    }



    // Read entries concurently
    let reads = entries.keys().map(|k| {
        let store_inner = Arc::clone(&store_ref);
        let key = k.to_owned();
        tokio::spawn(async move {
            match store_inner.read().await.get(key.to_owned()).await {
                Ok(entry) => Ok((key, entry)),
                Err(err) => Err(err),
            }
        })
    });

    let all_results = join_all(reads).await;
    for tokio_res in all_results {
        assert!(tokio_res.is_ok());
        assert!(tokio_res.as_ref().unwrap().is_ok());
        let (key, value) = tokio_res.unwrap().unwrap();
        assert!(value.is_some());
        assert_eq!(
            std::str::from_utf8(&value.unwrap().val).unwrap(),
            *entries.get(&key).unwrap()
        );
    }
}
