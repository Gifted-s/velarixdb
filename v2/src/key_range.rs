use std::{
    io::{Read, Write},
    ops::Deref,
    sync::Arc,
};

use crate::{
    serde::{Deserializable, Serializable},
    DeserializeError, UserKey,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// A key range in the format of [min, max] (inclusive on both sides)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange((UserKey, UserKey));

impl std::ops::Deref for KeyRange {
    type Target = (UserKey, UserKey);

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl KeyRange {
    pub fn new(range: (UserKey, UserKey)) -> Self {
        Self(range)
    }
}

impl Serializable for KeyRange {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), crate::SerializeError> {
        // NOTE: Max key size = u16
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16::<BigEndian>(self.deref().0.len() as u16)?;
        writer.write_all(&self.deref().0)?;

        // NOTE: Max key size = u16
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16::<BigEndian>(self.deref().1.len() as u16)?;
        writer.write_all(&self.deref().1)?;
        Ok(())
    }
}

impl Deserializable for KeyRange {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let key_min_len = reader.read_u16::<BigEndian>()?;
        let mut key_min = vec![0; key_min_len.into()];
        reader.read_exact(&mut key_min)?;
        let key_min: UserKey = Arc::from(key_min);

        let key_max_len = reader.read_u16::<BigEndian>()?;
        let mut key_max = vec![0; key_max_len.into()];
        reader.read_exact(&mut key_max)?;
        let key_max: UserKey = Arc::from(key_max);

        Ok(Self::new((key_min, key_max)))
    }
}
