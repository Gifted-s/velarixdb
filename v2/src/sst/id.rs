use crate::tree::inner::TreeId;

use super::meta::SegmentId;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(clippy::module_name_repetitions)]
pub struct GlobalSegmentId((TreeId, SegmentId));

impl GlobalSegmentId {
    #[must_use]
    pub fn tree_id(&self) -> TreeId {
        self.0 .0
    }

    #[must_use]
    pub fn segment_id(&self) -> SegmentId {
        self.0 .1
    }
}

impl From<(TreeId, SegmentId)> for GlobalSegmentId {
    fn from(value: (TreeId, SegmentId)) -> Self {
        Self(value)
    }
}
