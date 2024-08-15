# August 2024

### Experimental implementation of basic features:

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
