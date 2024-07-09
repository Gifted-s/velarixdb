use tempfile::tempdir;
use velarixdb::db::DataStore;

#[tokio::test]
async fn test_delete() {
    let root = tempdir().unwrap();
    let path = root.path().join("velarix");
    let mut store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error

    store.put("apple", "tim cook").await.unwrap(); // handle error

    // Retrieve entry
    let entry = store.get("apple").await.unwrap();
    assert!(entry.is_some());

    // Delete entry
    store.delete("apple").await.unwrap();

    // Entry should now be None
    let entry = store.get("apple").await.unwrap();
    assert!(entry.is_none());
}
