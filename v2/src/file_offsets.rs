use std::io::{Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::{serde::{Deserializable, Serializable}, DeserializeError, SerializeError};

#[derive(Debug, Default)]
pub struct FileOffsets {
    pub index_block_ptr: u64,
    pub tli_ptr: u64,
    pub bloom_ptr: u64,
    pub range_tombstone_ptr: u64,
    pub metadata_ptr: u64,
}

impl FileOffsets {
    #[must_use]
    pub const fn serialize_len() -> usize {
        5 * std::mem::size_of::<u64>()
    }
}

impl Serializable for FileOffsets {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_u64::<BigEndian>(self.index_block_ptr)?;
        writer.write_u64::<BigEndian>(self.tli_ptr)?;
        writer.write_u64::<BigEndian>(self.bloom_ptr)?;
        writer.write_u64::<BigEndian>(self.range_tombstone_ptr)?;
        writer.write_u64::<BigEndian>(self.metadata_ptr)?;
        Ok(())
    }
}

impl Deserializable for FileOffsets {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let index_block_ptr = reader.read_u64::<BigEndian>()?;
        let tli_ptr = reader.read_u64::<BigEndian>()?;
        let bloom_ptr = reader.read_u64::<BigEndian>()?;
        let range_tombstone_ptr = reader.read_u64::<BigEndian>()?;
        let metadata_ptr = reader.read_u64::<BigEndian>()?;

        Ok(Self {
            index_block_ptr,
            tli_ptr,
            bloom_ptr,
            range_tombstone_ptr,
            metadata_ptr,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn file_offsets_serialized_len() -> crate::Result<()> {
        let mut buf = vec![];
        FileOffsets::default().serialize(&mut buf)?;

        assert_eq!(FileOffsets::serialize_len(), buf.len());

        Ok(())
    }
}
