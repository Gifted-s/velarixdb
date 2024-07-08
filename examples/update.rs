use tempfile::tempdir;
use velarixdb::db::DataStore;

#[tokio::main]
async fn main() {
    let root = tempdir().unwrap();
    let path = root.path().join("velarix");
    let mut store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error

    store.put("apple", "tim cook").await.unwrap(); // handle error

    // Update entry
    let success = store.update("apple", "elon musk").await;
    assert!(success.is_ok());

    // Entry should now be updated
    let entry = store.get("apple").await.unwrap(); // handle error
    assert!(entry.is_some());
    assert_eq!(std::str::from_utf8(&entry.unwrap().val).unwrap(), "elon musk")
}
