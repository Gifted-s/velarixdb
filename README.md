
<p align="center">
<img src="/logo.png" height="170">
</p>


[![Tests](https://github.com/Gifted-s/velarixdb/actions/workflows/rust.yml/badge.svg)](https://github.com/Gifted-s/velarixdb/actions/workflows/rust.yml)
[![Clippy Tests](https://github.com/Gifted-s/bd/actions/workflows/clippy.yml/badge.svg)](https://github.com/Gifted-s/bd/actions/workflows/clippy.yml)
[![Codecov](https://github.com/Gifted-s/velarixdb/actions/workflows/codecov.yml/badge.svg)](https://github.com/Gifted-s/velarixdb/actions/workflows/codecov.yml)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md)


VelarixDB is an LSM-based storage engine designed to significantly reduce IO amplification, resulting in better performance and durability for storage devices.


## Introduction

**VelarixDB: Efficient Data Management for High Performance**

VelarixDB is an ongoing project (*not production ready*) designed to optimize data movement during load times, random access, and compaction. Inspired by the WiscKey paper, [WiscKey: Separating Keys from Values in SSD-conscious Storage](https://usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), velarixdb aims to significantly enhance performance over traditional key-value stores.

## Performance Benefits

According to the benchmarks presented in the WiscKey paper, implementations can outperform LevelDB and RocksDB by:
- **2.5x to 111x** for database loading
- **1.6x to 14x** for random lookups

## Concurrency with Tokio

To achieve high concurrency without the overhead of spawning new threads every time, velarixdb utilizes the Tokio runtime. This allows for efficient and scalable asynchronous operations, making the most of modern multi-core processors.

## Disclaimer

Please note that velarixdb is still under development and is not yet production-ready.

### Features
- [x] Atomic `Put()`, `Get()`, `Delete()`, and `Update()` operations
- [x] 100% safe & stable Rust
- [x] Separation of keys from values, reducing the amount of data moved during compaction (i.e., reduced IO amplification)
- [x] Garbage Collector
- [x] Lock-free memtable (no `Mutex`)
- [x] Tokio Runtime for efficient thread management
- [x] Bloom Filters for fast in-memory key searches
- [x] Crash recovery using the Value Log
- [x] Index to improve searches on Sorted String Tables (SSTs)
- [x] Key Range to store the largest and smallest keys in an SST
- [x] Sized Tier Compaction Strategy (STCS)
- [ ] Batched Writes
- [ ] Snappy Compression Algorithm
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

# Store JSON

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
[See here](https://usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) for practical examples
