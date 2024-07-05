use crate::{
    consts::{META_FILE_NAME, SIZE_OF_U32, SIZE_OF_U64},
    err::Error,
    fs::{FileAsync, FileNode, MetaFileNode, MetaFs},
    types::{ByteSerializedEntry, CreatedAt, LastModified, VLogHead, VLogTail},
};
use chrono::Utc;
use std::path::{Path, PathBuf};

/// Meta file
#[derive(Debug, Clone)]
pub struct MetaFile<F: MetaFs> {
    pub file: F,
    pub path: PathBuf,
}

impl<F: MetaFs> MetaFile<F> {
    /// Creates a new `MetaFile`
    pub fn new<P: AsRef<Path> + Send + Sync>(path: P, file: F) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            file,
        }
    }
}

/// metadata for `DataStore`
#[derive(Debug, Clone)]
pub struct Meta {
    /// Handles file operations
    pub file_handle: MetaFile<MetaFileNode>,
    pub v_log_tail: VLogHead,
    pub v_log_head: VLogTail,
    pub created_at: CreatedAt,
    pub last_modified: LastModified,
}

impl Meta {
    /// Creates new `Meta`
    pub async fn new<P: AsRef<Path> + Send + Sync>(dir: P) -> Result<Self, Error> {
        let created_at = Utc::now();
        let last_modified = Utc::now();
        FileNode::create_dir_all(dir.as_ref()).await?;
        let file_path = dir.as_ref().join(format!("{}.bin", META_FILE_NAME));
        let file = MetaFileNode::new(file_path.to_owned(), crate::fs::FileType::Meta)
            .await
            .unwrap();

        Ok(Self {
            file_handle: MetaFile::new(file_path, file),
            v_log_tail: 0,
            v_log_head: 0,
            created_at,
            last_modified,
        })
    }
    /// Writes `Meta` to disk
    pub async fn write(&mut self) -> Result<(), Error> {
        let serialized_data = self.serialize();
        self.file_handle.file.node.clear().await?;
        self.file_handle.file.node.write_all(&serialized_data).await?;
        return Ok(());
    }
    /// Sets `Meta` `v_log_head` field
    pub fn set_head(&mut self, head: usize) {
        self.v_log_head = head;
    }
    /// Updates `last_modified` field
    pub fn update_last_modified(&mut self) {
        self.last_modified = Utc::now();
    }

    /// Recovers `Meta` from disk
    /// 
    /// # Errors
    ///
    /// Returns IO error in case it occurs
    pub async fn recover(&mut self) -> Result<(), Error> {
        let (head, tail, created_at, last_modified) = MetaFileNode::recover(self.file_handle.path.to_owned()).await?;
        self.v_log_head = head;
        self.v_log_tail = tail;
        self.created_at = created_at;
        self.last_modified = last_modified;
        return Ok(());
    }

    /// Serializes `Meta` into byte vector
    fn serialize(&self) -> ByteSerializedEntry {
        // head offset + tail offset + created_at + last_modified
        let entry_len = SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U64;

        let mut serialized_data = Vec::with_capacity(entry_len);

        serialized_data.extend_from_slice(&(self.v_log_head as u32).to_le_bytes());

        serialized_data.extend_from_slice(&(self.v_log_tail as u32).to_le_bytes());

        serialized_data.extend_from_slice(&(self.created_at.timestamp_millis() as u64).to_le_bytes());

        serialized_data.extend_from_slice(&(self.last_modified.timestamp_millis() as u64).to_le_bytes());

        serialized_data
    }
}
