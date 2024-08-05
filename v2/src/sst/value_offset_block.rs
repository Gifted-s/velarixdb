use crate::LSMEntry;

use super::block::Block;


#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CachePolicy{
    /// Read cache blocks, but do not change cache
    Read,

    /// Read cached blocks, and update cache
    Write
}

/// Value offset blocks are the building blocks of a [`crate::sst::SSTable`]. Each block is 
/// a sorted list of [`LSMEntry`]s, 
/// NOTE: This list is not compressed since all we have in it is a list of - key, sequence number
/// value offset and value type (tombstone or not), this are pretty light weight fixed-size
/// data therefore we don't compress the list, key and value are already compressed in the 
/// value log
/// 
/// The integrity of a block can be checked using the CRC value that is saved in it
#[allow(clippy::module_name_repetitions)]
pub type ValueOffsetBlock = Block<LSMEntry>;

impl ValueOffsetBlock {
    #[must_use]
    pub fn size(&self) -> usize{
        std::mem::size_of::<Self>() + self.items.iter().map(LSMEntry::size).sum::<usize>()
    }

    
}  

