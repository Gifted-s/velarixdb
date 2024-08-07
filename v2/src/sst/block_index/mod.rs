pub mod block_handle;
mod top_level;
pub mod writer;

use self::block_handle::KeyedBlockHandle;
use super::block::Block;
pub type IndexBlock = Block<KeyedBlockHandle>;

impl IndexBlock {
    // TODO: same as TLI::get_lowest_block_containing_key
    #[must_use]
    pub fn get_lowest_data_block_handle_containing_item(&self, key: &[u8]) -> Option<&KeyedBlockHandle> {
        let idx = self.items.partition_point(|x| &*x.end_key < key);

        let handle = self.items.get(idx)?;

        if key > &*handle.end_key {
            None
        } else {
            Some(handle)
        }
    }
}
