use std::sync::Arc;

use futures::future::join_all;
use tempfile::tempdir;
use tokio::sync::RwLock;
use velarixdb::db::DataStore;


#[tokio::test]
async fn test_put_concurrent() {
    let root = tempdir().unwrap();
    let path = root.path().join("velarixdb");
    let store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error

    let entries = [
        ["apple", "tim cook"],
        ["google", "sundar pichai"],
        ["nvidia", "jensen huang"],
        ["microsoft", "satya nadella"],
        ["meta", "mark zuckerberg"],
        ["openai", "sam altman"],
    ];

    let store_ref = Arc::new(RwLock::new(store));
    let write_tasks = entries.iter().map(|e| {
        let store_inner = Arc::clone(&store_ref);
        let key = e[0];
        let val = e[1];
        tokio::spawn(async move {
            let mut writer = store_inner.write().await;
            writer.put(key, val).await
        })
    });
    let all_results = join_all(write_tasks).await;
    for tokio_res in all_results {
        assert!(tokio_res.is_ok());
        assert!(tokio_res.as_ref().unwrap().is_ok());
    }
}
