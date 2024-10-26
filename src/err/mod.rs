use std::{io, path::PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to sync writes to file")]
    FileSync(#[source] io::Error),

    #[error("Failed to create file: `{path}`: {error}")]
    FileCreation { path: PathBuf, error: io::Error },

    #[error("File seek error")]
    FileSeek(#[source] io::Error),

    #[error("Directory deletion error")]
    DirDelete(#[source] io::Error),

    #[error("Filter file path not provided")]
    FilterFilePathNotProvided,

    #[error("Filter file open error: path `{0}`")]
    FilterFileOpen(PathBuf),

    #[error("File deletion error")]
    FileDelete(#[source] io::Error),

    #[error("Failed to open file")]
    FileOpen { path: PathBuf, error: io::Error },

    #[error("Failed to get file metadata")]
    GetFileMetaData(#[source] std::io::Error),

    #[error("Failed to check if file path exist")]
    TryFilePathExist(#[source] std::io::Error),

    #[error("Failed to create directory")]
    DirCreation { path: PathBuf, error: io::Error },

    #[error("Failed to clear file: `{path}`: {error}")]
    FileClear { path: PathBuf, error: io::Error },

    #[error("Failed to read file `{path}`: {error}")]
    FileRead { path: PathBuf, error: io::Error },

    #[error("Failed to write to file `{path}`: {error}")]
    FileWrite { path: PathBuf, error: io::Error },

    #[error("Failed to open directory `{path}`: {error}")]
    DirOpen { path: PathBuf, error: io::Error },

    #[error("File read ended unexpectedly")]
    UnexpectedEOF(#[source] io::Error),

    #[error("GC error: attempting to remove unsynced entries from disk")]
    GCErrorAttemptToRemoveUnsyncedEntries,

    #[error("Failed to insert sstable to bucket because no insertion condition was met")]
    ConditionsToInsertToBucketNotMet,

    #[error("Error occured while flushing to disk")]
    FlushToDisk {
        #[source]
        error: Box<Self>, // Flush to disk can be caused by any of the errors above
    },

    #[error("Error occured while inserting entry to memtable value  Key: `{key}` Value: `{value_offset}`")]
    InsertToMemTableFailed { key: String, value_offset: usize },

    #[error("Error while recovering memtable from value log")]
    MemTableRecovery(#[source] Box<Self>),

    #[error("Invalid string provided to be parsed to UUID `{input_string}`: {error}")]
    InvaidUUIDParseString {
        input_string: String,
        error: uuid::Error,
    },

    #[error("Invalid sstable directory error: `{input_string}`")]
    InvalidSSTableDirectory { input_string: String },

    #[error("Compaction failed reason : {0}")]
    CompactionFailed(Box<Self>),

    #[error("Compaction partially failed failed reason: {0}")]
    CompactionPartiallyFailed(Box<Self>),

    #[error("No SSTable contains the searched key")]
    KeyNotFoundInAnySSTable,

    #[error("Key found as tombstone in sstable")]
    KeyFoundAsTombstoneInSSTable,

    #[error("Key found as tombstone in memtable")]
    KeyFoundAsTombstoneInMemtable,

    #[error("Key found as tombstone in value log")]
    KeyFoundAsTombstoneInValueLog,

    #[error("Memtable does not contains the searched key")]
    KeyNotFoundInMemTable,

    #[error("Key does not exist in value log")]
    KeyNotFoundInValueLog,

    #[error("Key not found, reason: ")]
    KeyNotFound(#[source] Box<Self>),

    #[error("Key not found")]
    NotFoundInDB,

    #[error("Tombstone check failed {0}")]
    TombStoneCheckFailed(String),

    #[error("Block is full")]
    BlockIsFull,

    #[error("Filter not provided, needed to flush table to disk")]
    FilterNotProvidedForFlush,

    #[error("Key size too large, key must not exceed 65536 bytes")]
    KeyMaxSizeExceeded,

    #[error("Key cannot be empty")]
    KeySizeNone,

    #[error("Value cannot be empty")]
    ValueSizeNone,

    #[error("Value too large, value must not exceed 2^32 bytes")]
    ValMaxSizeExceeded,

    #[error("Filter not found")]
    FilterNotFound,

    #[error("Error finding biggest key in memtable (None was returned)")]
    BiggestKeyIndex,

    #[error("Error finding lowest key in memtable (None was returned)")]
    LowestKeyIndex,

    #[error("SSTable summary field is None")]
    TableSummaryIsNone,

    #[error("All bloom filters return false for all sstables")]
    KeyNotFoundByAnyBloomFilter,

    #[error("Failed to insert to a bucket, reason `{0}`")]
    FailedToInsertToBucket(String),

    #[error("Error punching hole in file, reason `{0}`")]
    GCErrorFailedToPunchHoleInVlogFile(io::Error),

    #[error("Unsuported OS for garbage collection, err message `{0}`")]
    GCErrorUnsupportedPlatform(String),

    #[error("Range scan error `{0}`")]
    RangeScan(Box<Self>),

    #[error("Flush signal channel was overloaded with signals, please check all signal consumers or try again later")]
    FlushSignalChannelOverflow,

    #[error("GC update channel was overloaded with data, please check all  consumers")]
    GCUpdateChannelOverflow,

    #[error("Flush signal channel has been closed")]
    FlushSignalChannelClosed,

    #[error("Serializartion error: {0} ")]
    Serialization(&'static str),

    #[error("Partial failure, sstable merge was successful but obsolete sstables not deleted  ")]
    CompactionCleanupPartial,

    #[error("Compaction cleanup failed but sstable merge was successful : {0} ")]
    CompactionCleanup(Box<Self>),

    #[error(
        "Cannot remove obsolete sstables from disk because not every merged sstable was written to disk"
    )]
    CannotRemoveObsoleteSST,

    #[error("Error, merged sstables has empty entries")]
    MergeSSTContainsZeroEntries,

    #[error("Tokio join tasks error")]
    TokioJoin,

    #[error("Entries cannot be empty during flush")]
    EntriesCannotBeEmptyDuringFlush,
}
