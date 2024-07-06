//! VikingsDB is an LSM-based storage engine designed to significantly reduce IO amplification, resulting in better performance and durability for storage devices.
//!
//! ## Introduction
//!
//! VikingsDB is an ongoing project (**not production ready**) that aims to reduce the amount of data moved during load times, random access, and compaction. It is based on the WiscKey paper (<https://usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf>).
//! According to the paper, benchmarks shows that WiscKey implementations can outperform LevelDB by a factor of **2.5x to 111x** for loading a database and **1.6x to 14x** for random lookups. VikingsDB uses the Tokio Runtime so we can achieve concurrency without necessarily spawning new threads.
//!
//! ### Features
//! - [x] Atomic `Put()`, `Get()`, `Delete()`, `Update()` operations
//! - [x] 100% safe & stable Rust
//! - [x] Seperates keys from values, reducing amount of data physically moved during compaction (i.e Reduced IO amplification)
//! - [x] Garbage Collector to physically free up space occupied by obsolete entries on the storage device
//! - [x] Lock-Free Skipmap from crossbeam as memtable data structure (no Mutex on memtable)
//! - [x] Tokio Asynchronous Runtime to achieve concurrency
//! - [x] Bloom Filters for fast in-memory key searches
//! - [x] Crash recovery using the Value Log
//! - [x] Index to improve searches on Sorted String Tables (SSTs)
//! - [x] Key Range to store the largest and smallest keys in a String Table (SST)
//! - [x] Sized Tier Compaction Strategy (STCS)
//! - [ ] Snappy Compression Algorithm
//! - [ ] Value Buffer to keep values in memory and only flush in batches to reduce IO (under investigation)
//! - [ ] Checksum to detect data corruption
//! - [ ] Batched Writes
//! - [ ] Leveled Compaction (LCS), Time-Window Compaction (TCS), and Unified Compaction (UCS)
//! - [ ] Monitoring module to constantly monitor and generate reports
//!
//! ### It is not:
//! - A standalone server
//! - A relational database
//! - A wide-column database: it has no notion of columns
//!
//! ### Constraint
//! - Keys are limited to 65,536 bytes, and values are limited to 2^32 bytes. Larger keys and values have a bigger performance impact.
//! 
//! - Like any typical key-value store, keys are stored in lexicographic order.
//!   If you are storing integer keys (e.g., timeseries data), use the big-endian form to adhere to locality.

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
