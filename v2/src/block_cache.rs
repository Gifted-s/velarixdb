use std::sync::Arc;

use crate::block_cache::Either::Left;
use crate::block_cache::Either::Right;
use crate::{
    either::Either,
    sst::{
        block_index::{block_handle::KeyedBlockHandle, IndexBlock},
        id::GlobalSegmentId,
        value_offset_block::ValueOffsetBlock,
    },
};
use quick_cache::Weighter;
use quick_cache::{sync::Cache, Equivalent};

type Item = Either<Arc<ValueOffsetBlock>, Arc<IndexBlock>>;

// (Type (TreeId, Segment ID),  Block offset)
#[derive(Eq, std::hash::Hash, PartialEq)]
struct CacheKey(GlobalSegmentId, u64);

impl Equivalent<CacheKey> for (GlobalSegmentId, u64) {
    fn equivalent(&self, key: &CacheKey) -> bool {
        self.0 == key.0 && self.1 == key.1
    }
}

impl From<(GlobalSegmentId, u64)> for CacheKey {
    fn from((gid, bid): (GlobalSegmentId, u64)) -> Self {
        Self(gid, bid)
    }
}

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<CacheKey, Item> for BlockWeighter {
    fn weight(&self, _: &CacheKey, block: &Item) -> u64 {
        // NOTE: Truncation is fine: blocks are definitely below 4 GiB
        #[allow(clippy::cast_possible_truncation)]
        match block {
            Either::Left(block) => block.size() as u64,
            Either::Right(block) => block
                .items
                .iter()
                .map(|x| x.end_key.len() + std::mem::size_of::<KeyedBlockHandle>())
                .sum::<usize>() as u64,
        }
    }
}

pub struct BlockCache {
    data: Cache<CacheKey, Item, BlockWeighter>,
    capacity: u64,
}

impl BlockCache {
    #[must_use]
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        Self {
            data: Cache::with_weighter(1_000_000, bytes, BlockWeighter),
            capacity: bytes,
        }
    }

    /// Returns the amount of cached bytes
    #[must_use]
    pub fn size(&self) -> u64 {
        self.data.weight()
    }

    /// Returns the cache capacity in bytes.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns the number of cached blocks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if there are no cached blocks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[doc(hidden)]
    pub fn insert_disk_block(&self, sstable_id: GlobalSegmentId, offset: u64, value: Arc<ValueOffsetBlock>) {
        if self.capacity > 0 {
            self.data.insert((sstable_id, offset).into(), Left(value));
        }
    }

    #[doc(hidden)]
    pub fn insert_index_block(&self, sstable_id: GlobalSegmentId, offset: u64, value: Arc<IndexBlock>) {
        if self.capacity > 0 {
            self.data.insert((sstable_id, offset).into(), Right(value));
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_disk_block(&self, sstable_id: GlobalSegmentId, offset: u64) -> Option<Arc<ValueOffsetBlock>> {
        let key = (sstable_id, offset);
        let item = self.data.get(&key)?;
        Some(item.left())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_index_block(&self, sstable_id: GlobalSegmentId, offset: u64) -> Option<Arc<IndexBlock>> {
        let key = (sstable_id, offset);
        let item = self.data.get(&key)?;
        Some(item.right())
    }
}
