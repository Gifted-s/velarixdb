use crate::serde::{DeserializeError, SerializeError};
use lz4_flex::block::DecompressError;
use std::io::Error as IoError;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("IO error")]
    Io(#[from] IoError),

    #[error("SerializeError({0})")]
    Serialize(#[from] SerializeError),

    #[error("DeserializeError({0})")]
    Deserialize(#[from] DeserializeError),

    #[error("DecompressError({0})")]
    Decompress(DecompressError),
}


/// Tree result
pub type Result<T> = std::result::Result<T, Error>;
