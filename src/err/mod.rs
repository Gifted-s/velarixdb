use std::{io, path::PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StorageEngineError {
    /// There was an error while writing to sstbale file
    #[error("Failed to open file")]
    SSTableFileOpenError {
        path: PathBuf,
        #[source]
        error: io::Error,
    },

    /// There was an error while atttempting to read from sstbale file
    #[error("Failed to read sstsable file `{path}`: {error}")]
    SSTableFileReadError { path: PathBuf, error: io::Error },

    /// There was an error while atttempting to read from sstbale file
    #[error("Failed to open bucket directory `{path}`: {error}")]
    BucketDirectoryOpenError { path: PathBuf, error: io::Error },

    /// There was an error while atttempting to read from sstbale file
    #[error("Failed to insert sstable to bucket because no insertion condition was met")]
    FailedToInsertSSTableToBucketError,

    /// There was an error while flushing memtable to sstable
    #[error("Error occured while flushing to disk")]
    FlushToDiskError {
        #[source]
        error: Box<Self>, // Flush to disk can be caused by any of the error above
    },

    /// There was an error while atttempting to create value log directory
    #[error("Failed to create v_log directory `{path}`: {error}")]
    VLogDirectoryCreationError { path: PathBuf, error: io::Error },

    /// There was an error while atttempting to create value log file
    #[error("Failed to create v_log.bin file `{path}`: {error}")]
    VLogFileCreationError { path: PathBuf, error: io::Error },

    /// There was an error inserting entry to memtable
    #[error("Error occured while inserting entry to memtable value  Key: `{key}` Value: `{value_offset}`")]
    InsertToMemTableFailedError { key: String, value_offset: usize },

    /// There was an error while floushing memtable to sstable
    #[error("Error while recovering memtable from disk")]
    MemTableRecoveryError(#[source] Box<Self>), //  Memtable recovery failure can be caused by any of the error above

    #[error("Failed to get file metadata")]
    GetFileMetaDataError(#[source] std::io::Error),

    /// There was an error while atttempting to parse string to UUID
    #[error("Invalid string provided to be parsed to UUID input `{input_string}`: {error}")]
    InvaidUUIDParseString {
        input_string: String,
        error: uuid::Error,
    },
    #[error("File read ended unexpectedly")]
    UnexpectedEOF(#[source] io::Error),

    #[error("Compaction partially failed reason : {0}")]
    CompactionPartiallyFailed(String),
}
