use crate::SeqNo;

use std::sync::{
    atomic::{
        AtomicU64,
        Ordering::Release,
    },
    Arc,
};


#[derive(Clone, Default, Debug)]
pub struct SequenceNumberCounter(Arc<AtomicU64>);

impl std::ops::Deref for SequenceNumberCounter {
    type Target = Arc<AtomicU64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SequenceNumberCounter {
    /// Creates new counter, setting it to some previous value
    #[must_use]
    pub fn new(prev: SeqNo) -> Self {
        Self(Arc::new(AtomicU64::new(prev)))
    }

    /// Gets the next sequence number, without incrementing the counter
    ///
    /// This should only be used when creating a snapshot
    #[must_use]
    pub fn next(&self) -> SeqNo {
        self.fetch_add(1, Release)
    }
}
