use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
/// Contains type aliases shared across modules to prevent redeclaration and help with readability
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type ValOffset = usize;
pub type CreationTime = u64;
pub type IsTombStone = bool;
pub type FlushSignal = u8;
pub type NoBytesRead = usize;
pub type SkipMapEntries = Arc<SkipMap<Key, (ValOffset, CreationTime, IsTombStone)>>;
