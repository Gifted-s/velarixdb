use crate::serde::{Deserializable, DeserializeError, Serializable, SerializeError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    cmp::Reverse,
    io::{Read, Write},
    sync::Arc,
};


/// Key provided user
pub type UserKey = Arc<[u8]>;

/// Value provided by user
pub type UserValue = Arc<[u8]>;

/// Position of value in value log
pub type ValueOffset = u64;

/// Sequence number - a monotonically increasing counter
///
/// Value with thesame seqno are part of the same batch
///
/// A value with a higher sequence number shadows an item with the
/// same key but with lower sequence number. For MVCC
///
///
/// Stale entries are garbage collected during compaction
pub type SeqNo = u64;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ValueType {
    /// Exisitng value
    Value,

    /// Deleted value
    Tombstone,
}

impl From<u8> for ValueType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Value,
            _ => Self::Tombstone,
        }
    }
}

impl From<ValueType> for u8 {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Value => 0,
            ValueType::Tombstone => 1,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ParsedInternalKey {
    pub user_key: UserKey,
    pub seqno: SeqNo,
    pub value_type: ValueType,
}

impl std::fmt::Debug for ParsedInternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{}:{}",
            self.user_key,
            self.seqno,
            u8::from(self.value_type)
        )
    }
}

impl ParsedInternalKey {
    pub fn new<K: Into<UserKey>>(user_key: K, seqno: SeqNo, value_type: ValueType) -> Self {
        Self {
            user_key: user_key.into(),
            seqno,
            value_type,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value_type == ValueType::Tombstone
    }
}

impl PartialOrd for ParsedInternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Order by user key, THEN by sequencce number
// This is important to ensure most recent keys are
// use, otherwise queries will bnehave unexpectedly
impl Ord for ParsedInternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.user_key, Reverse(self.seqno)).cmp(&(&other.user_key, Reverse(other.seqno)))
    }
}

/// Represents a value in the LSM-tree
///
/// NOTE:  This represents values stored in
/// LSM-tree not in Value log
#[derive(Clone, PartialEq, Eq)]
pub struct Value {
    /// User-define3d key - an arbitrary byte array
    ///
    /// Supports up to 2^16 bytes for perfmance considerations
    pub key: UserKey,

    /// Offset of value in value log
    pub value_offset: ValueOffset,

    /// Sequence number
    pub seqno: SeqNo,

    /// Tombstone marker, true if deleted otherwise false
    pub value_type: ValueType,
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{}:{} => {:?}",
            self.key,
            self.seqno,
            match self.value_type {
                ValueType::Value => "V",
                ValueType::Tombstone => "T",
            },
            format!("{:?}", self.value_offset)
        )
    }
}

impl From<(ParsedInternalKey, ValueOffset)> for Value {
    fn from(val: (ParsedInternalKey, ValueOffset)) -> Value {
        let key = val.0;

        Self {
            key: key.user_key,
            value_offset: val.1,
            seqno: key.seqno,
            value_type: key.value_type,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Order by user key, THEN by sequencce number
// This is important to ensure most resent keys are
// use, otherwise queries will bnehave unexpectedly
impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.key, Reverse(self.seqno)).cmp(&(&other.key, Reverse(other.seqno)))
    }
}

impl Value {
    /// Creates a new [`Value`].
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16, or the value is greater than 2^64.
    pub fn new<K: Into<UserKey>>(
        key: K,
        value_offset: ValueOffset,
        seqno: u64,
        value_type: ValueType,
    ) -> Self {
        let k = key.into();

        assert!(!k.is_empty());
        assert!(k.len() <= u16::MAX.into());
        assert!(u64::try_from(value_offset).is_ok());

        Self {
            key: k,
            value_offset,
            seqno,
            value_type,
        }
    }

    pub fn new_tombstone<K: Into<UserKey>>(key: K, seqno: u64) -> Self {
        let k = key.into();

        assert!(!k.is_empty());
        assert!(k.len() <= u16::MAX.into());

        Self {
            key: k,
            value_offset: 0,
            seqno,
            value_type: ValueType::Tombstone,
        }
    }

    pub fn size(&self) -> usize {
        let key_size = self.key.len();
        std::mem::size_of::<Self>() + key_size
    }

    pub fn is_tombstone(&self) -> bool {
        self.value_type == ValueType::Tombstone
    }
}

impl From<Value> for ParsedInternalKey {
    fn from(val: Value) -> Self {
        Self {
            user_key: val.key,
            seqno: val.seqno,
            value_type: val.value_type,
        }
    }
}


impl Serializable for Value {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_u64::<BigEndian>(self.seqno)?;
        writer.write_u8(u8::from(self.value_type))?;

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16::<BigEndian>(self.key.len() as u16)?;
        writer.write_all(&self.key)?;

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u64::<BigEndian>(self.value_offset)?;

        Ok(())
    }
}

impl Deserializable for Value {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let seqno = reader.read_u64::<BigEndian>()?;
        let value_type = reader.read_u8()?.into();

        let key_len = reader.read_u16::<BigEndian>()?;
        let mut key = vec![0; key_len.into()];
        reader.read_exact(&mut key)?;

        let value_offset = reader.read_u64::<BigEndian>()?;

        Ok(Self::new(key, value_offset, seqno, value_type))
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use test_log::test;

    #[test]
    fn value_raw() -> crate::Result<()> {
        // Create an empty Value instance
        let value = Value::new(vec![1, 2, 3], 10, 1, ValueType::Value);

        #[rustfmt::skip]
        let  bytes = &[
            // Seqno
            0, 0, 0, 0, 0, 0, 0, 1,
            
            // Type
            0,
            
            // Key
            0, 3, 1, 2, 3,
            
             // Value Offset
            0, 0, 0, 0, 0, 0, 0 , 10,
        ];

        // Deserialize the empty Value
        let deserialized = Value::deserialize(&mut Cursor::new(bytes))?;

        // Check if deserialized Value is equivalent to the original empty Value
        assert_eq!(value, deserialized);

        Ok(())
    }

    #[test]
    fn value_zero_value_offset() -> crate::Result<()> {
        // Create an empty Value instance
        let value = Value::new(vec![1, 2, 3], 0, 42, ValueType::Value);

        // Serialize the empty Value
        let mut serialized = Vec::new();
        value.serialize(&mut serialized)?;

        // Deserialize the empty Value
        let deserialized = Value::deserialize(&mut &serialized[..])?;

        // Check if deserialized Value is equivalent to the original empty Value
        assert_eq!(value, deserialized);

        Ok(())
    }

    #[test]
    fn value_with_value() -> crate::Result<()> {
        // Create an empty Value instance
        let value = Value::new(
            vec![1, 2, 3],
            10,
            42,
            ValueType::Value,
        );

        // Serialize the empty Value
        let mut serialized = Vec::new();
        value.serialize(&mut serialized)?;

        // Deserialize the empty Value
        let deserialized = Value::deserialize(&mut &serialized[..])?;

        // Check if deserialized Value is equivalent to the original empty Value
        assert_eq!(value, deserialized);

        Ok(())
    }
}