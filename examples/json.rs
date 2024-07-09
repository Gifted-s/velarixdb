use serde::{Deserialize, Serialize};
use serde_json;
use tempfile::tempdir;
use velarixdb::db::DataStore;

#[tokio::main]
async fn main() {
    let root = tempdir().unwrap();
    let path = root.path().join("velarix");
    let mut store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error

    #[derive(Serialize, Deserialize)]
    struct BigTech {
        name: String,
        rank: i32,
    }

    let new_entry = BigTech {
        name: String::from("Google"),
        rank: 50,
    };

    let json_string = serde_json::to_string(&new_entry).unwrap();
    let res = store.put("google", json_string).await;

    assert!(res.is_ok());

    let entry = store.get("google").await.unwrap().unwrap();
    let entry_string = std::str::from_utf8(&entry.val).unwrap();
    let big_tech: BigTech = serde_json::from_str(&entry_string).unwrap();

    assert_eq!(big_tech.name, new_entry.name);
    assert_eq!(big_tech.rank, new_entry.rank);
}
