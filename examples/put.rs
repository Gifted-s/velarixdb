use tempfile::tempdir;
use velarixdb::db::DataStore;

#[tokio::main]
async fn main() {
    let root = tempdir().unwrap();
    let path = root.path().join("velarix");
    let mut store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error

    let res1 = store.put("apple", "tim cook").await;
    let res2 = store.put("google", "sundar pichai").await;
    let res3 = store.put("nvidia", "jensen huang").await;
    let res4 = store.put("microsoft", "satya nadella").await;
    let res5 = store.put("meta", "mark zuckerberg").await;
    let res6 = store.put("openai", "sam altman").await;

    assert!(res1.is_ok());
    assert!(res2.is_ok());
    assert!(res3.is_ok());
    assert!(res4.is_ok());
    assert!(res5.is_ok());
    assert!(res6.is_ok());
}
