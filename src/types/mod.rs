pub type Key = Vec<u8>;
pub type Value = Vec<u8>; // NOTE: This is actual value not value offset
pub type ValOffset = usize;
pub type CreationTime = u64;
pub type IsTombStone = bool;
pub type FlushSignal = u8;
