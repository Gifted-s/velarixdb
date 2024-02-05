
pub enum ValueOption{
    Raw(Vec<u8>) ,
    Offset(usize),
    ThumbStone(u8)
} 