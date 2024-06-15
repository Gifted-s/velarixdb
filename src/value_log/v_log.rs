use crate::{
    consts::{EOF, VLOG_FILE_NAME},
    err::Error,
    fs::{FileAsync, FileNode, VLogFileNode, VLogFs},
};
use log::error;
use std::{mem, path::PathBuf};
use tokio::io::{self};

type TotalBytesRead = usize;

#[derive(Debug, Clone)]
pub struct VFile<F>
where
    F: VLogFs,
{
    pub(crate) file: F,
    pub(crate) path: PathBuf,
}

impl<F> VFile<F>
where
    F: VLogFs,
{
    pub fn new(path: PathBuf, file: F) -> Self {
        Self { path, file }
    }
}

#[derive(Debug, Clone)]
pub struct ValueLog {
    pub content: VFile<VLogFileNode>,
    pub head_offset: usize,
    pub tail_offset: usize,
}

#[derive(PartialEq, Debug, Clone)]
pub struct ValueLogEntry {
    pub ksize: usize,
    pub vsize: usize,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub created_at: u64,
    pub is_tombstone: bool,
}

impl ValueLog {
    pub async fn new(dir: &PathBuf) -> Result<Self, Error> {
        let dir_path = PathBuf::from(dir);
        FileNode::create_dir_all(dir_path.to_owned()).await?;
        let file_path = dir_path.join(VLOG_FILE_NAME);
        let file = VLogFileNode::new(file_path.to_owned(), crate::fs::FileType::ValueLog)
            .await
            .unwrap();
        Ok(Self {
            head_offset: 0,
            tail_offset: 0,
            content: VFile::new(file_path, file),
        })
    }

    pub async fn append(
        &mut self,
        key: &Vec<u8>,
        value: &Vec<u8>,
        created_at: u64,
        is_tombstone: bool,
    ) -> Result<usize, Error> {
        let v_log_entry = ValueLogEntry::new(
            key.len(),
            value.len(),
            key.to_vec(),
            value.to_vec(),
            created_at,
            is_tombstone,
        );
        let serialized_data = v_log_entry.serialize();
        // Get the current offset before writing(this will be the offset of the value stored in the memtable)
        let last_offset = self.content.file.node.size().await;
        let data_file = &self.content;
        let _ = data_file.file.node.write_all(&serialized_data).await;
        Ok(last_offset as usize)
    }

    pub async fn get(&self, start_offset: usize) -> Result<Option<(Vec<u8>, bool)>, Error> {
        self.content.file.get(start_offset).await
    }

    pub async fn recover(&mut self, start_offset: usize) -> Result<Vec<ValueLogEntry>, Error> {
        self.content.file.recover(start_offset).await
    }

    pub async fn read_chunk_to_garbage_collect(
        &self,
        bytes_to_collect: usize,
    ) -> Result<(Vec<ValueLogEntry>, TotalBytesRead), Error> {
        self.content
            .file
            .read_chunk_to_garbage_collect(bytes_to_collect, self.tail_offset as u64)
            .await
    }

    // CAUTION: This deletes the value log file
    pub async fn clear_all(&mut self) {
        if self.content.file.node.metadata().await.is_ok() {
            if let Err(err) = self.content.file.node.remove_dir_all().await {
                error!(
                    "path: {:?}, err={:?} ",
                    self.content.file.node.file_path, err
                );
            }
        }
        self.tail_offset = 0;
        self.head_offset = 0;
    }

    pub fn set_head(&mut self, head: usize) {
        self.head_offset = head;
    }

    pub fn set_tail(&mut self, tail: usize) {
        self.tail_offset = tail;
    }
}

impl ValueLogEntry {
    pub fn new(
        ksize: usize,
        vsize: usize,
        key: Vec<u8>,
        value: Vec<u8>,
        created_at: u64,
        is_tombstone: bool,
    ) -> Self {
        Self {
            ksize,
            vsize,
            key,
            value,
            created_at,
            is_tombstone,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let entry_len = mem::size_of::<u32>()
            + mem::size_of::<u32>()
            + mem::size_of::<u64>()
            + self.key.len()
            + self.value.len()
            + mem::size_of::<u8>();

        let mut serialized_data = Vec::with_capacity(entry_len);

        serialized_data.extend_from_slice(&(self.key.len() as u32).to_le_bytes());

        serialized_data.extend_from_slice(&(self.value.len() as u32).to_le_bytes());

        serialized_data.extend_from_slice(&self.created_at.to_le_bytes());

        serialized_data.push(self.is_tombstone as u8);

        serialized_data.extend_from_slice(&self.key);

        serialized_data.extend_from_slice(&self.value);

        serialized_data
    }

    #[allow(dead_code)]
    fn deserialize(serialized_data: &[u8]) -> io::Result<Self> {
        let key_len = u32::from_le_bytes([
            serialized_data[4],
            serialized_data[5],
            serialized_data[6],
            serialized_data[7],
        ]) as usize;

        let value_len = u32::from_le_bytes([
            serialized_data[8],
            serialized_data[9],
            serialized_data[10],
            serialized_data[11],
        ]) as usize;

        let created_at = u64::from_le_bytes([
            serialized_data[12],
            serialized_data[13],
            serialized_data[14],
            serialized_data[15],
            serialized_data[16],
            serialized_data[17],
            serialized_data[18],
            serialized_data[19],
        ]);

        let is_tombstone = serialized_data[20] == 1;

        if serialized_data.len() != 20 + key_len + value_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid length of serialized data",
            ));
        }

        let key = serialized_data[20..(20 + key_len)].to_vec();
        let value = serialized_data[(20 + key_len)..].to_vec();

        Ok(ValueLogEntry::new(
            key_len,
            value_len,
            key,
            value,
            created_at,
            is_tombstone,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialized_deserialized() {
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let created_at = 164343434343434;
        let is_tombstone = false;
        let original_entry = ValueLogEntry::new(
            key.len(),
            value.len(),
            key.clone(),
            value.clone(),
            created_at,
            is_tombstone,
        );
        let serialized_data = original_entry.serialize();

        let expected_entry_len = 4 + 4 + 8 + 1 + key.len() + value.len();

        assert_eq!(serialized_data.len(), expected_entry_len);

        let deserialized_entry =
            ValueLogEntry::deserialize(&serialized_data).expect("failed to deserialize data");

        assert_eq!(deserialized_entry, original_entry);
    }
}
