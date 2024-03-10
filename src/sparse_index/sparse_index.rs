pub struct SparseIndex {
    entries: Vec<SparseIndexEntry>,
}

impl SparseIndex {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn insert(&mut self, key_prefix: u32, key: Vec<u8>, offset: u32) {
        self.entries.push(SparseIndexEntry {
            key,
            key_prefix,
            offset,
        })
    }
}

struct SparseIndexEntry {
    key_prefix: u32,
    key: Vec<u8>,
    offset: u32,
}
