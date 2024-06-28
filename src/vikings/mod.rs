use std::path::PathBuf;
use std::sync::Arc;

use chrono::Duration;
use libc::sleep;
use tokio::sync::RwLock;

use crate::consts::{DEFAULT_ONLINE_GARBAGE_COLLECTION_INTERVAL_MILLI, DEFAULT_PREFETCH_SIZE, GC_CHUNK_SIZE};
use crate::gc::gc::Config;
use crate::types::DBName;
use crate::{storage::DataStore, types::Key};

// struct VikingsDB<'a> {
//     name: DBName<'a>,
//     store: DataStore<'a, Key>,
// }

// impl VikingsDB<'static> {
//     pub async fn new(name: &'static str) -> Self {
//         let path = PathBuf::new().join(name);
//         let store = DataStore::new(path.clone()).await.unwrap();

//       let db =  Self { name, store };
//       db.start_background_tasks();
//       db
//     }

//     pub fn start_background_tasks(&self) {
//         let store = Arc::new(RwLock::new(self.store));
//         let gc = GC::new(DEFAULT_ONLINE_GARBAGE_COLLECTION_INTERVAL_MILLI, GC_CHUNK_SIZE as u32);
//         gc.start_background_gc_task(store);
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     // Generate test to find keys after compaction
//     #[tokio::test]
//     async fn datastore_gc_new() {
//         let _ = VikingsDB::new("vikings_db").await;
//         tokio::time::sleep(tokio::time::Duration::from_millis(5000 * 10)).await;
//     }
// }
