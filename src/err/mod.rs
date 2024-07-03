use std::{io, path::PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to sync writes to file : {error}")]
    FileSyncError { error: io::Error },

    #[error("Failed to create file: `{path}`: {error}")]
    FileCreationError { path: PathBuf, error: io::Error },

    #[error("File seek error")]
    FileSeekError(#[source] io::Error),

    #[error("Directory deletion error")]
    DirDeleteError(#[source] io::Error),

    #[error("Filter file path not provided")]
    FilterFilePathNotProvided,

    #[error("Filter file open error: path `{0}`")]
    FilterFileOpenError(PathBuf),

    #[error("File deletion error")]
    FileDeleteError(#[source] io::Error),

    #[error("Failed to open file")]
    FileOpenError { path: PathBuf, error: io::Error },

    #[error("Failed to get file metadata")]
    GetFileMetaDataError(#[source] std::io::Error),

    #[error("Failed to create directory")]
    DirCreationError { path: PathBuf, error: io::Error },

    #[error("Failed to clear file: `{path}`: {error}")]
    FileClearError { path: PathBuf, error: io::Error },

    #[error("Failed to read file `{path}`: {error}")]
    FileReadError { path: PathBuf, error: io::Error },

    #[error("Failed to write to file `{path}`: {error}")]
    FileWriteError { path: PathBuf, error: io::Error },

    #[error("Failed to open directory `{path}`: {error}")]
    DirOpenError { path: PathBuf, error: io::Error },

    #[error("File read ended unexpectedly")]
    UnexpectedEOF(#[source] io::Error),

    #[error("GC error attempting to remove unsynced entries from disk")]
    GCErrorAttemptToRemoveUnsyncedEntries,

    #[error("Failed to insert sstable to bucket because no insertion condition was met")]
    ConditionsToInsertToBucketNotMetError,

    #[error("Error occured while flushing to disk")]
    FlushToDiskError {
        #[source]
        error: Box<Self>, // Flush to disk can be caused by any of the errors above
    },

    #[error("Error occured while inserting entry to memtable value  Key: `{key}` Value: `{value_offset}`")]
    InsertToMemTableFailedError { key: String, value_offset: usize },

    #[error("Error while recovering memtable from value log")]
    MemTableRecoveryError(#[source] Box<Self>),

    #[error("Invalid string provided to be parsed to UUID input `{input_string}`: {error}")]
    InvaidUUIDParseString { input_string: String, error: uuid::Error },

    #[error("Invalid sstable directory error: `{input_string}`")]
    InvalidSSTableDirectoryError { input_string: String },

    #[error("Compaction failed reason : {0}")]
    CompactionFailed(Box<Self>),

    #[error("Compaction partially failed failed reason: {0}")]
    CompactionPartiallyFailed(Box<Self>),

    #[error("No SSTable contains the searched key")]
    KeyNotFoundInAnySSTableError,

    #[error("Key found as tombstone in sstable")]
    KeyFoundAsTombstoneInSSTableError,

    #[error("Key found as tombstone in memtable")]
    KeyFoundAsTombstoneInMemtableError,

    #[error("Key found as tombstone in value log")]
    KeyFoundAsTombstoneInValueLogError,

    #[error("Memtable does not contains the searched key")]
    KeyNotFoundInMemTable,

    #[error("Key does not exist in value log")]
    KeyNotFoundInValueLogError,

    #[error("Key not found, reason: ")]
    KeyNotFound(#[source] Box<Self>),

    #[error("Key not found")]
    NotFoundInDB,

    #[error("Tombstone check failed {0}")]
    TombStoneCheckFailed(String),

    #[error("Block is full")]
    BlockIsFullError,

    #[error("Filter not provided, needed to flush table to disk")]
    FilterNotProvidedForFlush,

    #[error("Filter not found")]
    FilterNotFoundError,

    #[error("Error finding biggest key in memtable (None was returned)")]
    BiggestKeyIndexError,

    #[error("Error finding lowest key in memtable (None was returned)")]
    LowestKeyIndexError,

    #[error("SSTable summary field is None")]
    TableSummaryIsNoneError,

    #[error("All bloom filters return false for all sstables")]
    KeyNotFoundByAnyBloomFilterError,

    #[error("Failed to insert to a bucket, reason `{0}`")]
    FailedToInsertToBucket(String),

    #[error("Error punching hole in file, reason `{0}`")]
    GCErrorFailedToPunchHoleInVlogFile(io::Error),

    #[error("Unsuported OS for garbage collection, err message `{0}`")]
    GCErrorUnsupportedPlatform(String),

    #[error("GC Error `{0}`")]
    GCError(String),

    #[error("Range scan error `{0}`")]
    RangeScanError(Box<Self>),

    #[error("Flush signal channel was overloaded with signals, please check all signal consumers or try again later")]
    FlushSignalChannelOverflowError,

    #[error("GC update channel was overloaded with data, please check all  consumers")]
    GCUpdateChannelOverflowError,

    #[error("Flush signal channel has been closed")]
    FlushSignalChannelClosedError,

    #[error("Serializartion error: {0} ")]
    SerializationError(&'static str),

    #[error("Flush error: {0} ")]
    FlushError(Box<Self>),

    #[error("Partial failure, sstable merge was successful but obsolete sstables not deleted  ")]
    CompactionCleanupPartialError,

    #[error("Compaction cleanup failed but sstable merge was successful : {0} ")]
    CompactionCleanupError(Box<Self>),

    #[error("Cannot remove obsolete sstables from disk because not every merged sstable was written to disk")]
    CannotRemoveObsoleteSSTError,

    #[error("Error, merged sstables has empty entries")]
    MergeSSTContainsZeroEntries,

    #[error("Tokio join tasks error")]
    TokioJoinError,

    #[error("Entries cannot be empty during flush")]
    EntriesCannotBeEmptyDuringFlush,
}
