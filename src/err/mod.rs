use std::{io, path::PathBuf};
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// There was an error while writing to sstbale file
    #[error("Failed to open sstable file")]
    SSTableFileOpenError {
        path: PathBuf,
        #[source]
        error: io::Error,
    },

    /// There while opening v_log file
    #[error("Failed to open v_lof file")]
    VlogFileOpenError {
        path: PathBuf,
        #[source]
        error: io::Error,
    },

    /// There was an error while opening file
    #[error("Failed to open  file")]
    FileOpenError {
        path: PathBuf,
        #[source]
        error: io::Error,
    },

    #[error("Failed to create directory")]
    DirCreationError {
        path: PathBuf,
        #[source]
        error: io::Error,
    },
    /// There was an error while atttempting to read a file
    #[error("Failed to read file `{path}`: {error}")]
    FileReadError { path: PathBuf, error: io::Error },

    /// There was an error while atttempting to write to file
    #[error("Failed to write to file `{path}`: {error}")]
    FileWriteError { path: PathBuf, error: io::Error },

    /// There was an error while atttempting to read from sstbale file
    #[error("Failed to read sstsable file `{path}`: {error}")]
    SSTableFileReadError { path: PathBuf, error: io::Error },

    /// There was an error while atttempting to wrtie to a sstbale file
    #[error("Failed to write to sstsable file  {error}")]
    SSTableWriteError { error: io::Error },

    /// There was an error while atttempting to flush sstable write to disk
    #[error("Failed to flush write to disk for sstable : {error}")]
    SSTableFlushError { error: io::Error },

    /// There was an error while atttempting to read from sstbale file
    #[error("Failed to open bucket directory `{path}`: {error}")]
    BucketDirectoryOpenError { path: PathBuf, error: io::Error },

    /// There was an error while atttempting to insert sstable to appropriate bucket
    #[error("Failed to insert sstable to bucket because no insertion condition was met")]
    FailedToInsertSSTableToBucketError,

    /// There was an error while flushing memtable to sstable
    #[error("Error occured while flushing to disk")]
    FlushToDiskError {
        #[source]
        error: Box<Self>, // Flush to disk can be caused by any of the errors above
    },

    /// RE There was an error while atttempting to create value log directory
    #[error("Failed to create v_log directory `{path}`: {error}")]
    VLogDirectoryCreationError { path: PathBuf, error: io::Error },

    /// RE There was an error while atttempting to create value log file
    #[error("Failed to create v_log.bin file `{path}`: {error}")]
    VLogFileCreationError { path: PathBuf, error: io::Error },

    /// There was an error while atttempting to create a file
    #[error("Failed to create file: `{path}`: {error}")]
    FileCreationError { path: PathBuf, error: io::Error },

    /// There was an error while atttempting to write to value log file
    #[error("Failed to write to value log file")]
    VLogFileWriteError(String),

    /// There was an error while atttempting to seek in file
    #[error("File seek error")]
    FileSeekError(#[source] io::Error),

    /// There was an error inserting entry to memtable
    #[error("Error occured while inserting entry to memtable value  Key: `{key}` Value: `{value_offset}`")]
    InsertToMemTableFailedError { key: String, value_offset: usize },

    /// There was an error while trying to recover memtable from sstables
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

    /// There was an error while atttempting to parse string to UUID
    #[error("Invalid sstable directory error `{input_string}`")]
    InvalidSSTableDirectoryError { input_string: String },

    #[error("Invalid string provided to be parsed to UUID input `{input_string}`: {error}")]
    InvaidSSTable {
        input_string: String,
        error: uuid::Error,
    },

    /// Unexpected end of file while reading
    #[error("File read ended unexpectedly")]
    UnexpectedEOF(#[source] io::Error),

    /// Failure while trying to lock file
    #[error("File lock unsuccessful")]
    FileLockError(String),

    /// Error occured during compaction
    #[error("Compaction failed reason : {0}")]
    CompactionFailed(String),

    /// Partial error occured during compaction
    #[error("Compaction partially failed failed reason : {0}")]
    CompactionPartiallyFailed(String),

    /// No SSTable contains the key searched
    #[error("No SS Tables contains the searched key")]
    KeyNotFoundInAnySSTableError,

    #[error("Key found as tombstone in sstable")]
    KeyFoundAsTombstoneInSSTableError,

    #[error("Key found as tombstone in memtable")]
    KeyFoundAsTombstoneInMemtableError,

    #[error("Key found as tombstone in value log")]
    KeyFoundAsTombstoneInValueLogError,

    /// Memtable does not contain key
    #[error("Memtable does not contains the searched key")]
    KeyNotFoundInMemTable,

    /// Key was found in SSTable but was not found in value log
    #[error("Key does not exist in value log")]
    KeyNotFoundInValueLogError,

    /// Key not found due to any of the reasons stated above
    #[error("Key not found, reason: ")]
    KeyNotFound(#[source] Box<Self>),

    /// Key not found in db
    #[error("Key not found")]
    NotFoundInDB,

    /// There was an error while atttempting to read from value log file
    #[error("Failed to read value log file : {error}")]
    ValueLogFileReadError { error: io::Error },

    /// There was an error while atttempting to sync writes to the value log file
    #[error("Failed to sync writes to value log file : {error}")]
    ValueLogFileSyncError { error: io::Error },

    /// There was an error while atttempting to sync writes to a file
    #[error("Failed to sync writes to file : {error}")]
    FileSyncError { error: io::Error },

    /// There was an error while atttempting to open v_log directory
    #[error("Failed to open value log directory `{error}`")]
    ValueLogDirectoryOpenError { error: io::Error },

    /// Tombstone check failed error can happens during compaction
    #[error("Tombstone check failed {0}")]
    TombStoneCheckFailed(String),

    /// Block is full
    #[error("Block is full")]
    BlockIsFullError,

    /// Error while writing to index file
    #[error("Index file write error")]
    IndexFileWriteError(#[source] io::Error),

    /// Error while reading from index file
    #[error("Index file read error")]
    IndexFileReadError(#[source] io::Error),

    /// Error while flushing write to disk for index file
    #[error("Index file flush error")]
    IndexFileFlushError(#[source] io::Error),

    #[error("Error finding biggest key in memtable (None was returned)")]
    BiggestKeyIndexError,

    #[error("Error finding lowest key in memtable (None was returned)")]
    LowestKeyIndexError,

    #[error("All bloom filters return false for all sstables")]
    KeyNotFoundByAnyBloomFilterError,

    /// There was an error while atttempting to insert sstable to appropriate bucket
    #[error("Failed to insert to a bucket, reason `{0}`")]
    FailedToInsertToBucket(String),

    /// Error punching hole in file
    #[error("Error punching hole in file, reason `{0}`")]
    GCErrorFailedToPunchHoleInVlogFile(io::Error),

    /// Error running GC in an unsurpported operating system
    #[error("Unsuported OS, err message `{0}`")]
    GCErrorUnsupportedPlatform(String),

    /// Error joining multiple tokio tasks to run concurrently
    #[error("Error joining tokio tasks. error: `{error}`, context: `{context}`")]
    TokioTaskJoinError { error: JoinError, context: String },

    /// Error invalid lock type
    #[error("Invalid lock type: expected `{expected}`, Found: `{found}`")]
    InvalidLockType {
        expected: &'static str,
        found: &'static str,
    },

    /// Error while trying to perform a range scan
    #[error("Range scan error `{0}`")]
    RangeScanError(Box<Self>),

    #[error("Flush signal channel was overloaded with signals, please check all signal consumers")]
    FlushSignalOverflowError,

    #[error("Flush signal channel has been closed")]
    FlushSignalClosedError,
}
