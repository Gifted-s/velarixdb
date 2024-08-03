use std::io::{Read, Write, Error as IoError};
use thiserror::Error;
/// Error during serialization
#[derive(Error, Debug)]
pub enum SerializeError {
    #[error("IO Error: {0}")]
    Io(IoError),
}

impl From<std::io::Error> for SerializeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}


#[derive(Debug)]
pub struct Tag(pub String, pub u8);

impl std::fmt::Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.0, self.1)
    }
}

/// Error during deserialization
#[derive(Error, Debug)]
pub enum DeserializeError {
    #[error("IO Error: {0}")]
    Io(IoError),

    #[error("UTF8 Error: {0}")]
    Utf8(std::str::Utf8Error),

    #[error("Invalid Tag: {0}")]
    InvalidTag(Tag),

    #[error("InvalidTrailer")]
    InvalidTrailer,

    #[error("InvalidHeader: {0}")]
    InvalidHeader(String),
}

impl From<std::io::Error> for DeserializeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<std::str::Utf8Error> for DeserializeError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::Utf8(value)
    }
}

/// Trait to serialize structs
pub trait Serializable {
    /// Serialize to bytes
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError>;
}

/// Trait to deserialize structs
pub trait Deserializable {
    /// Deserialize from bytes
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError>
    where
        Self: Sized;
}
