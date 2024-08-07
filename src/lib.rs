//! Velarixdb is an LSM-based storage engine designed to significantly reduce IO amplification, resulting in better performance and durability for storage devices.
//!
//![![codecov](https://codecov.io/gh/Gifted-s/velarixdb/branch/ft%2Ffinal/graph/badge.svg?token=01K79PJWQA)](https://codecov.io/gh/Gifted-s/velarixdb)
//![![Tests](https://github.com/Gifted-s/velarixdb/actions/workflows/rust.yml/badge.svg)](https://github.com/Gifted-s/velarixdb/actions/workflows/rust.yml)
//![![Crates.io](https://img.shields.io/crates/v/velarixdb.svg)](https://crates.io/crates/velarixdb)
//![![Documentation](https://docs.rs/velarixdb/badge.svg)](https://docs.rs/velarixdb)
//![![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md)
//![![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

//! ## Introduction
//!
//! # Velarixdb: Designed to reduce IO amplification
//!
//! This is an ongoing project (**not production ready**) designed to optimize data movement during load times, random access, and compaction. Inspired by the WiscKey paper, [WiscKey: Separating Keys from Values in SSD-conscious Storage](https://usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), velarixdb aims to significantly enhance performance over traditional key-value stores.
//!
//! ## Problem
//! 
//! During compaction in LevelDB or RocksDB, in the worst case, up to 10 SSTable files needs to be read, sorted and re-written since keys are not allowed to overlapp across all the sstables from Level 1 downwards. Suppose after merging SSTables in one level, the next level exceeds its threshold, compaction can cascade from Level 0 all the way to Level 6 meaning the overall write amplification can be up to 50 (ignoring the first compaction level).[ Reference -> [Official LevelDB Compaction Process Docs](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction) ]. 
//! This repetitive data movement can cause significant wear on SSDs, reducing their lifespan due to the high number of write cycles. The goal is to minimize the amount of data moved during compaction, thereby reducing the amount of data re-written and extending the device's lifetime.
//!
//! ## Solution
//! 
//! To address this, we focus on whether a key has been deleted or updated. Including values in the compaction process (which are often larger than keys) unnecessarily amplifies the amount of data read and written. Therefore, we store keys and values separately. Specifically, we map value offsets to the keys, represented as 32-bit integers.
//! This approach reduces the amount of data read, written, and moved during compaction, leading to improved performance and less wear on storage devices, particularly SSDs. By minimizing the data movement, we not only enhance the efficiency of the database but also significantly extend the lifespan of the underlying storage hardware.
//! 
//! ## Performance Benefits
//!
//! According to the benchmarks presented in the WiscKey paper, implementations can outperform LevelDB and RocksDB by:
//! - **2.5x to 111x** for database loading
//! - **1.6x to 14x** for random lookups
//! 
//! ## Addressing major concerns
//! - **Range Query**: Since keys are separate from values, won't that affect range queries performance. Well, we now how have internal parallelism in SSDs, as we fetch the keys from  the LSM tree we can fetch the values in parallel from the vlog file. This [benchmark](https://github.com/Gifted-s/velarixdb/blob/main/bench.png) from the Wisckey Paper shows how for request size ≥ 64KB, the aggregate throughput of random reads with 32 threads matches the sequential read throughput.
//! - **More Disk IO for Reads**: Since keys are now seperate from values, we have to make extra disk IO to fetch values? Yes, but since the key density now increases for each level (since we are only storing keys and value offsets in the sstable), we will most likely search fewer levels compared to LevelDB or RocksDB for thesame query. A significant portion of the LSM tree can also be cached in memory.
//! 
//! ## Designed for asynchronous runtime (unstable)
//!
//! Based on the introduction and efficiency of async IO at the OS kernel level e.g **io_uring** for the Linux kernel, the experimental version of Velarixdb is designed for asynchronous runtime. In this case Tokio runtime.
//! Tokio allows for efficient and scalable asynchronous operations, making the most of modern multi-core processors. Frankly, most OS File System does not provide async API currently but Tokio uses a thread pool to offload blocking file system operations.
//! This means that even though the file system operations themselves are blocking at the OS level, Tokio can handle them without blocking the main async task executor.  
//! Tokio might adopt [io_uring](https://docs.rs/tokio/latest/tokio/fs/index.html#:~:text=Currently%2C%20Tokio%20will%20always%20use%20spawn_blocking%20on%20all%20platforms%2C%20but%20it%20may%20be%20changed%20to%20use%20asynchronous%20file%20system%20APIs%20such%20as%20io_uring%20in%20the%20future.) in the future,
//! (We haven't benchmarked the async version therefore this is unstable and might be removed in future versions)
//! 
//! ## Disclaimer
//!
//! Please note that velarixdb is still under development and is not yet production-ready.
//! 
//! ### Features
//! - [x] Atomic `Put()`, `Get()`, `Delete()`, and `Update()` operations
//! - [x] 100% safe & stable Rust
//! - [x] Separation of keys from values, reducing the amount of data  moved during compaction (i.e., reduced IO amplification)
//! - [x] Garbage Collector
//! - [x] Lock-free memtable (no `Mutex`)
//! - [x] Tokio Runtime for efficient thread management
//! - [x] Bloom Filters for fast in-memory key searches
//! - [x] Crash recovery using the Value Log
//! - [x] Index to improve searches on Sorted String Tables (SSTs)
//! - [x] Key Range to store the largest and smallest keys in an SST
//! - [x] Sized Tier Compaction Strategy (STCS)
//! - [ ] Rigorous benchmark
//! - [ ] Block Cache
//! - [ ] Batched Writes
//! - [ ] Range Query
//! - [ ] Snappy Compression 
//! - [ ] Value Buffer to keep values in memory and only flush in batches to reduce IO (under investigation)
//! - [ ] Checksum to detect data corruption
//! - [ ] Leveled Compaction (LCS), Time-Window Compaction (TCS), and Unified Compaction (UCS)
//! - [ ] Monitoring module to continuously monitor and generate reports
//!
//!
//! ### It is not:
//! - A standalone server
//! - A relational database
//!
//! ### Constraint
//! - Keys are limited to 65,536 bytes, and values are limited to 2^32 bytes. Larger keys and values have a bigger performance impact.
//!
//! - Like any typical key-value store, keys are stored in lexicographic order.
//!   If you are storing integer keys (e.g., timeseries data), use the big-endian form to adhere to locality.
//!
//!
//! # Basic usage
//!
//!
//! ```rust
//!   use velarixdb::db::DataStore;
//! # use tempfile::tempdir;
//!
//! #[tokio::main]
//! async fn main() {
//!     let root = tempdir().unwrap();
//!     let path = root.path().join("velarix");
//!     let mut store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error
//!
//!     store.put("apple", "tim cook").await;
//!     store.put("google", "sundar pichai").await;
//!     store.put("nvidia", "jensen huang").await;
//!     store.put("microsoft", "satya nadella").await;
//!     store.put("meta", "mark zuckerberg").await;
//!     store.put("openai", "sam altman").await;
//!
//!
//!     let entry1 = store.get("apple").await.unwrap(); // handle error
//!     let entry2 = store.get("google").await.unwrap();
//!     let entry3 = store.get("nvidia").await.unwrap();
//!     let entry4 = store.get("microsoft").await.unwrap();
//!     let entry5 = store.get("meta").await.unwrap();
//!     let entry6 = store.get("openai").await.unwrap();
//!     let entry7 = store.get("***not_found_key**").await.unwrap();
//!
//!     assert_eq!(std::str::from_utf8(&entry1.unwrap().val).unwrap(), "tim cook");
//!     assert_eq!(std::str::from_utf8(&entry2.unwrap().val).unwrap(), "sundar pichai");
//!     assert_eq!(std::str::from_utf8(&entry3.unwrap().val).unwrap(), "jensen huang");
//!     assert_eq!(std::str::from_utf8(&entry4.unwrap().val).unwrap(), "satya nadella");
//!     assert_eq!(std::str::from_utf8(&entry5.unwrap().val).unwrap(), "mark zuckerberg");
//!     assert_eq!(std::str::from_utf8(&entry6.unwrap().val).unwrap(), "sam altman");
//!     assert!(entry7.is_none());
//!
//!     // Remove an entry
//!     store.delete("apple").await.unwrap();
//!
//!     // Update an entry
//!     let success = store.update("microsoft", "elon musk").await;
//!     assert!(success.is_ok());
//! }
//! ```
//!
//!
//! ### Store JSON
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use serde_json;
//! use velarixdb::db::DataStore;
//! # use tempfile::tempdir;
//!
//! #[tokio::main]
//! async fn main() {
//!     let root = tempdir().unwrap();
//!     let path = root.path().join("velarix");
//!     let mut store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error
//!
//!     #[derive(Serialize, Deserialize)]
//!     struct BigTech {
//!         name: String,
//!         rank: i32,
//!     }
//!     let new_entry = BigTech {
//!         name: String::from("Google"),
//!         rank: 50,
//!     };
//!     let json_string = serde_json::to_string(&new_entry).unwrap();
//!
//!     let res = store.put("google", json_string).await;
//!     assert!(res.is_ok());
//!
//!     let entry = store.get("google").await.unwrap().unwrap();
//!     let entry_string = std::str::from_utf8(&entry.val).unwrap();
//!     let big_tech: BigTech = serde_json::from_str(&entry_string).unwrap();
//!
//!     assert_eq!(big_tech.name, new_entry.name);
//!     assert_eq!(big_tech.rank, new_entry.rank);
//! }
//! ```

#![doc(html_logo_url = "https://firebasestorage.googleapis.com/v0/b/generalsapi.appspot.com/o/Screenshot%202024-07-23%20at%2023.41.43.png?alt=media&token=109ab2a9-25d1-4a36-9d7e-7f8cfeb6ce6b")]
#![doc(html_favicon_url = "https://firebasestorage.googleapis.com/v0/b/generalsapi.appspot.com/o/Screenshot%202024-07-23%20at%2023.41.43.png?alt=media&token=109ab2a9-25d1-4a36-9d7e-7f8cfeb6ce6b")]

mod block;
mod bucket;
mod cfg;
// contains compaction strategies
pub mod compactors;
mod consts;
pub mod db;
mod err;
mod filter;
mod flush;
mod fs;
mod gc;
mod index;
mod key_range;
mod r#macro;
mod memtable;
mod meta;
mod range;
mod sst;
mod tests;
mod types;
mod util;
mod vlog;
