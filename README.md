
<p align="center">
<img src="/logo.png" height="145">
</p>

[![codecov](https://codecov.io/gh/Gifted-s/velarixdb/branch/ft%2Ffinal/graph/badge.svg?token=01K79PJWQA)](https://codecov.io/gh/Gifted-s/velarixdb)
[![Tests](https://github.com/Gifted-s/velarixdb/actions/workflows/rust.yml/badge.svg)](https://github.com/Gifted-s/velarixdb/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/velarixdb.svg)](https://crates.io/crates/velarixdb)
[![Documentation](https://docs.rs/velarixdb/badge.svg)](https://docs.rs/velarixdb)
[![Clippy Tests](https://github.com/Gifted-s/bd/actions/workflows/clippy.yml/badge.svg)](https://github.com/Gifted-s/bd/actions/workflows/clippy.yml)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


VelarixDB is an LSM-based storage engine designed to significantly reduce IO amplification, resulting in better performance and durability for storage devices.


## Introduction

**VelarixDB: Designed to reduce IO amplification**

VelarixDB is an ongoing project (*not production ready*) designed to optimize data movement during load times and compaction. Inspired by the WiscKey paper, [WiscKey: Separating Keys from Values in SSD-conscious Storage](https://usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), velarixdb aims to significantly enhance performance over traditional key-value stores.

## Problem

During compaction in LevelDB or RocksDB, [thesame key-value pair can be re-read and re-written from Level 1 to Level 6 in the worst case](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction), the goal is to reduce size of data moved around during this process.

## Solution

We mostly care if the key has been deleted or updated therefore including values in the compaction process (which is often larger than the key) can unneccssarily amplify the data read and written during compaction. We therefore store keys and values separately (we map value offset to the key represented as 32 bit integer),
This reduces the amount of data read, written, and moved during compaction, leading to improved performance and reduced wear on storage devices, particularly SSDs.

## Performance Benefits

According to the benchmarks presented in the WiscKey paper, implementations can outperform LevelDB and RocksDB by:
- **2.5x to 111x** for database loading
- **1.6x to 14x** for random lookups

## Concurrency with Tokio

To achieve high concurrency without the overhead of spawning new threads every time, velarixdb utilizes the Tokio runtime. This allows for efficient and scalable asynchronous operations, making the most of modern multi-core processors. Frankly,
most Operating System File System does not provide async API but Tokio uses a thread pool to offload blocking file system operations. When you perform an asynchronous file operation in Tokio, the operation is executed on a dedicated thread from this pool.
This means that even though the file system operations themselves are blocking at the OS level, Tokio can handle them without blocking the main async task executor.  
Tokio might also use [io_uring](https://docs.rs/tokio/latest/tokio/fs/index.html#:~:text=Currently%2C%20Tokio%20will%20always%20use%20spawn_blocking%20on%20all%20platforms%2C%20but%20it%20may%20be%20changed%20to%20use%20asynchronous%20file%20system%20APIs%20such%20as%20io_uring%20in%20the%20future.) in the future

## Disclaimer

Please note that velarixdb is still under development and is not yet production-ready.

### Basic Features
- [x] Atomic `Put()`, `Get()`, `Delete()`, and `Update()` operations
- [x] 100% safe & stable Rust
- [x] Separation of keys from values, reducing the amount of data moved during compaction (i.e., reduced IO amplification)
- [x] Garbage Collector
- [x] Lock-free memtable with Crossbeam SkipMap (no `Mutex`)
- [x] Tokio Runtime for efficient thread management
- [x] Bloom Filters for fast in-memory key searches
- [x] Crash recovery using the Value Log
- [x] Index to improve searches on Sorted String Tables (SSTs)
- [x] Key Range to store the largest and smallest keys in an SST
- [x] Sized Tier Compaction Strategy (STCS)

### TODO
- [ ] Snapshot Isolation
- [ ] Block Cache
- [ ] Batched Writes
- [ ] Range Query
- [ ] Snappy Compression
- [ ] Value Buffer to keep values in memory and only flush in batches to reduce IO (under investigation)
- [ ] Checksum to detect data corruption
- [ ] Leveled Compaction (LCS), Time-Window Compaction (TCS), and Unified Compaction (UCS)
- [ ] Monitoring module to continuously monitor and generate reports

### It is not:
- A standalone server
- A relational database
- A wide-column database: it has no notion of columns

### Constraint
- Keys are limited to 65,536 bytes, and values are limited to 2^32 bytes. Larger keys and values have a bigger performance impact.
- Like any typical key-value store, keys are stored in lexicographic order. If you are storing integer keys (e.g., timeseries data), use the big-endian form to adhere to locality.

# Basic usage

```sh
cargo add velarixdb
```

```rust
use velarixdb::db::DataStore;
# use tempfile::tempdir;

#[tokio::main]
async fn main() {
    let root = tempdir().unwrap();
    let path = root.path().join("velarix");
    let mut store = DataStore::open("big_tech", path).await.unwrap(); // handle IO error

    store.put("apple", "tim cook").await;
    store.put("google", "sundar pichai").await;
    store.put("nvidia", "jensen huang").await;
    store.put("microsoft", "satya nadella").await;
    store.put("meta", "mark zuckerberg").await;
    store.put("openai", "sam altman").await;


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
    assert!(entry7.is_none());

    // Remove an entry
    store.delete("apple").await.unwrap();

    // Update an entry
    let success = store.update("microsoft", "elon musk").await;
    assert!(success.is_ok());
}
```

### Store JSON

```rust
use serde::{Deserialize, Serialize};
use serde_json;
use velarixdb::db::DataStore;
# use tempfile::tempdir;

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
```

## Examples
[See here](https://github.com/Gifted-s/velarixdb/tree/main/examples) for practical examples
