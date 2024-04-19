// TODO: 

#[allow(dead_code)]
pub enum ValueOption {
    /// We might need to cache the raw value in memory if the size is very small, this will reduce
    /// number of Disk IO made because value of smaller size can reside and fetched in memory before it is been flushed to disk
    Raw(Vec<u8>),

    /// Value offset gotten from value position in value log
    Offset(usize),

    // Represents deleted entry
    ThumbStone(u8),
}
