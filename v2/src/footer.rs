use std::{
    fs::File,
    io::{BufReader, Read, Seek, Write},
    path::Path,
};

use crate::{
    file_offsets::FileOffsets,
    serde::{Deserializable, Serializable},
    sst::meta::Metadata,
};

pub const FOOTER_MAGIC: &[u8] = &[b'V', b'E', b'L', b'A', b'T', b'R', b'L', b'1'];
pub const FOOTER_SIZE: usize = 256;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct SegmentFileFooter {
    #[doc(hidden)]
    pub metadata: Metadata,

    #[doc(hidden)]
    pub offsets: FileOffsets,
}

impl SegmentFileFooter {
    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        reader.seek(std::io::SeekFrom::End(-(FOOTER_SIZE as i64)))?;

        // Parse Pointers
        let offsets = FileOffsets::deserialize(&mut reader)?;

        let remaining_padding = FOOTER_SIZE - FileOffsets::serialize_len() - FOOTER_MAGIC.len();
        reader.seek_relative(remaining_padding as i64)?;

        // Check footer magic
        let mut magic = [0u8; FOOTER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != FOOTER_MAGIC {
            return Err(crate::Error::Deserialize(crate::DeserializeError::InvalidHeader(
                "SegmentTrailer".to_owned(),
            )));
        }

        log::trace!("Trailer offsets: {offsets:#?}");

        // Jump to metadata and parse
        reader.seek(std::io::SeekFrom::Start(offsets.metadata_ptr))?;
        let metadata = Metadata::deserialize(&mut reader)?;

        Ok(Self { metadata, offsets })
    }
}

impl Serializable for SegmentFileFooter {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), crate::SerializeError> {
        let mut v = Vec::with_capacity(FOOTER_SIZE);

        self.offsets.serialize(&mut v)?;

        // Pad with remaining bytes,
        // padding ensures we have a fixed size for footer
        v.resize(FOOTER_SIZE - FOOTER_MAGIC.len(), 0);

        v.write_all(FOOTER_MAGIC)?;

        assert_eq!(v.len(), FOOTER_SIZE, "segment file trailer has invalid size");

        writer.write_all(&v)?;

        Ok(())
    }
}
