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

    let entry1 = store.get("apple").await.unwrap(); // Handle error
    let entry2 = store.get("google").await.unwrap();
    let entry3 = store.get("nvidia").await.unwrap();
    let entry4 = store.get("microsoft").await.unwrap();
    let entry5 = store.get("meta").await.unwrap();
    let entry6 = store.get("openai").await.unwrap();
    let entry7 = store.get("***not_found_key**").await.unwrap();

    assert_eq!(std::str::from_utf8(&entry1.unwrap().val).unwrap(), "tim cook");
    assert_eq!(std::str::from_utf8(&entry2.unwrap().val).unwrap(), "sundar pichai");
    assert_eq!(std::str::from_utf8(&entry3.unwrap().val).unwrap(), "jensen huang");
    assert_eq!(std::str::from_utf8(&entry4.unwrap().val).unwrap(), "satya nadella");
    assert_eq!(std::str::from_utf8(&entry5.unwrap().val).unwrap(), "mark zuckerberg");
    assert_eq!(std::str::from_utf8(&entry6.unwrap().val).unwrap(), "sam altman");
    assert!(entry7.is_none())
}
