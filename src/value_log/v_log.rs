use std::{mem, path::PathBuf};
use tokio::fs::{OpenOptions};
use tokio::io::{self, AsyncSeekExt, SeekFrom};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::{
    consts::{EOF, VLOG_FILE_NAME},
    err::StorageEngineError,
};
use StorageEngineError::*;
#[derive(Debug, Clone)]
pub struct ValueLog {
    pub file_path: PathBuf,
    pub head_offset: usize,
    pub tail_offset: usize,
}

#[derive(PartialEq, Debug, Clone)]
pub struct ValueLogEntry {
    pub ksize: usize,
    pub vsize: usize,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub created_at: u64, // date to milliseconds
    pub is_tombstone: bool,
}

impl ValueLog {
    pub async fn new(dir: &PathBuf) -> Result<Self, StorageEngineError> {
        let dir_path = PathBuf::from(dir);

        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)
                .await
                .map_err(|err| VLogDirectoryCreationError {
                    path: dir_path.clone(),
                    error: err,
                })?;
        }

        let file_path = dir_path.join(VLOG_FILE_NAME);
        OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(file_path.clone())
            .await
            .map_err(|err| VLogFileCreationError {
                path: dir_path,
                error: err,
            })?;

        Ok(Self {
            file_path,
            head_offset: 0,
            tail_offset: 0,
        })
    }

    pub fn set_head(&mut self, head: usize) {
        self.head_offset = head;
    }

    pub fn set_tail(&mut self, tail: usize) {
        self.tail_offset = tail;
    }

    pub async fn append(
        &mut self,
        key: &Vec<u8>,
        value: &Vec<u8>,
        created_at: u64,
        is_tombstone: bool,
    ) -> Result<usize, StorageEngineError> {
        let v_log_entry = ValueLogEntry::new(
            key.len(),
            value.len(),
            key.to_vec(),
            value.to_vec(),
            created_at,
            is_tombstone,
        );
        let serialized_data = v_log_entry.serialize();
        // Open the file in write mode with the append flag.
        let file_path = PathBuf::from(&self.file_path);
        let mut file = OpenOptions::new()
            .append(true)
            .open(file_path.clone())
            .await
            .map_err(|err| SSTableFileOpenError {
                path: file_path.clone(),
                error: err,
            })?;

        // Get the current offset before writing(this will be the offset of the value stored in the memtable)
        let value_offset = file
            .seek(io::SeekFrom::End(0))
            .await
            .map_err(|err| FileSeekError(err))?;

        file.write_all(&serialized_data)
            .await
            .expect("Failed to write to value log entry");
        file.flush()
            .await
            .map_err(|error| VLogFileWriteError(error.to_string()))?;

        Ok(value_offset.try_into().unwrap())
    }

    pub async fn get(
        &self,
        start_offset: usize,
    ) -> Result<Option<(Vec<u8>, bool)>, StorageEngineError> {
        let file_path = PathBuf::from(&self.file_path);
        let mut file = OpenOptions::new()
            .read(true)
            .open(file_path.clone())
            .await
            .map_err(|err| SSTableFileOpenError {
                path: file_path.clone(),
                error: err,
            })?;

        file.seek(SeekFrom::Start(start_offset as u64))
            .await
            .map_err(|err| ValueLogFileReadError {
                error: io::Error::new(err.kind(), EOF),
            })?;

        // get key length
        let mut key_len_bytes = [0; mem::size_of::<u32>()];
        let bytes_read =
            file.read(&mut key_len_bytes)
                .await
                .map_err(|err| ValueLogFileReadError {
                    error: io::Error::new(err.kind(), EOF),
                })?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let key_len = u32::from_le_bytes(key_len_bytes);

        // get value length
        let mut val_len_bytes = [0; mem::size_of::<u32>()];
        let bytes_read =
            file.read(&mut val_len_bytes)
                .await
                .map_err(|err| ValueLogFileReadError {
                    error: io::Error::new(err.kind(), EOF),
                })?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let val_len = u32::from_le_bytes(val_len_bytes);

        // get date length
        let mut creation_date_bytes = [0; mem::size_of::<u64>()];
        let bytes_read =
            file.read(&mut creation_date_bytes)
                .await
                .map_err(|err| ValueLogFileReadError {
                    error: io::Error::new(err.kind(), EOF),
                })?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let _ = u64::from_le_bytes(creation_date_bytes);

        // get tombstone
        let mut istombstone_bytes = [0; mem::size_of::<u8>()];
        let mut bytes_read =
            file.read(&mut istombstone_bytes)
                .await
                .map_err(|err| ValueLogFileReadError {
                    error: io::Error::new(err.kind(), EOF),
                })?;
        if bytes_read == 0 {
            return Ok(None);
        }

        let is_tombstone = istombstone_bytes[0] == 1;

        let mut key = vec![0; key_len as usize];
        bytes_read = file
            .read(&mut key)
            .await
            .map_err(|err| ValueLogFileReadError {
                error: io::Error::new(err.kind(), EOF),
            })?;
        if bytes_read == 0 {
            return Ok(None);
        }

        let mut value = vec![0; val_len as usize];
        bytes_read = file
            .read(&mut value)
            .await
            .map_err(|err| ValueLogFileReadError {
                error: io::Error::new(err.kind(), EOF),
            })?;

        if bytes_read == 0 {
            return Ok(None);
        }
        Ok(Some((value, is_tombstone)))
    }

    pub async fn recover(
        &mut self,
        start_offset: usize,
    ) -> Result<Vec<ValueLogEntry>, StorageEngineError> {
        let file_path = PathBuf::from(&self.file_path);
        let mut file = OpenOptions::new()
            .read(true)
            .open(file_path.clone())
            .await
            .map_err(|err| SSTableFileOpenError {
                path: file_path.clone(),
                error: err,
            })?;
        file.seek(SeekFrom::Start(start_offset as u64))
            .await
            .map_err(|err| FileSeekError(err))?;
        let mut entries = Vec::new();
        loop {
            // get key length
            let mut key_len_bytes = [0; mem::size_of::<u32>()];
            let bytes_read =
                file.read(&mut key_len_bytes)
                    .await
                    .map_err(|err| ValueLogFileReadError {
                        error: io::Error::new(err.kind(), EOF),
                    })?;
            if bytes_read == 0 {
                return Ok(entries);
            }
            let key_len = u32::from_le_bytes(key_len_bytes);

            // get value length
            let mut val_len_bytes = [0; mem::size_of::<u32>()];
            let bytes_read =
                file.read(&mut val_len_bytes)
                    .await
                    .map_err(|err| ValueLogFileReadError {
                        error: io::Error::new(err.kind(), EOF),
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            let val_len = u32::from_le_bytes(val_len_bytes);

            // get date length
            let mut creation_date_bytes = [0; mem::size_of::<u64>()];
            let bytes_read =
                file.read(&mut creation_date_bytes)
                    .await
                    .map_err(|err| ValueLogFileReadError {
                        error: io::Error::new(err.kind(), EOF),
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }

            // is tombstone
            let mut istombstone_bytes = [0; mem::size_of::<u8>()];
            let mut bytes_read =
                file.read(&mut istombstone_bytes)
                    .await
                    .map_err(|err| ValueLogFileReadError {
                        error: io::Error::new(err.kind(), EOF),
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }

            let created_at = u64::from_le_bytes(creation_date_bytes);

            let mut key = vec![0; key_len as usize];
            bytes_read = file
                .read(&mut key)
                .await
                .map_err(|err| ValueLogFileReadError {
                    error: io::Error::new(err.kind(), EOF),
                })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }

            let mut value = vec![0; val_len as usize];
            bytes_read = file
                .read(&mut value)
                .await
                .map_err(|err| ValueLogFileReadError {
                    error: io::Error::new(err.kind(), EOF),
                })?;

            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }

            let is_tombstone = istombstone_bytes[0] == 1;
            entries.push(ValueLogEntry {
                ksize: key_len as usize,
                vsize: val_len as usize,
                key,
                value,
                created_at,
                is_tombstone,
            })
        }
    }

    pub async fn read_chunk_to_garbage_collect(
        &mut self,
        bytes_to_collect: usize,
    ) -> Result<Vec<ValueLogEntry>, StorageEngineError> {
        let file_path = PathBuf::from(&self.file_path);
        let mut file = OpenOptions::new()
            .read(true)
            .open(file_path.clone())
            .await
            .map_err(|err| SSTableFileOpenError {
                path: file_path.clone(),
                error: err,
            })?;
        file.seek(SeekFrom::Start(self.tail_offset as u64))
            .await
            .map_err(|err| FileSeekError(err))?;
        let mut entries = Vec::new();

        let mut total_bytes_read = 0;

        loop {
            // get key length
            let mut key_len_bytes = [0; mem::size_of::<u32>()];
            let bytes_read =
                file.read(&mut key_len_bytes)
                    .await
                    .map_err(|err| ValueLogFileReadError {
                        error: io::Error::new(err.kind(), EOF),
                    })?;
            if bytes_read == 0 {
                return Ok(entries);
            }
            total_bytes_read += bytes_read;
            let key_len = u32::from_le_bytes(key_len_bytes);

            // get value length
            let mut val_len_bytes = [0; mem::size_of::<u32>()];
            let bytes_read =
                file.read(&mut val_len_bytes)
                    .await
                    .map_err(|err| ValueLogFileReadError {
                        error: io::Error::new(err.kind(), EOF),
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            total_bytes_read += bytes_read;
            let val_len = u32::from_le_bytes(val_len_bytes);

            // get date length
            let mut creation_date_bytes = [0; mem::size_of::<u64>()];
            let bytes_read =
                file.read(&mut creation_date_bytes)
                    .await
                    .map_err(|err| ValueLogFileReadError {
                        error: io::Error::new(err.kind(), EOF),
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            total_bytes_read += bytes_read;

            // is tombstone
            let mut istombstone_bytes = [0; mem::size_of::<u8>()];
            let mut bytes_read =
                file.read(&mut istombstone_bytes)
                    .await
                    .map_err(|err| ValueLogFileReadError {
                        error: io::Error::new(err.kind(), EOF),
                    })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            total_bytes_read += bytes_read;

            let created_at = u64::from_le_bytes(creation_date_bytes);

            let mut key = vec![0; key_len as usize];
            bytes_read = file
                .read(&mut key)
                .await
                .map_err(|err| ValueLogFileReadError {
                    error: io::Error::new(err.kind(), EOF),
                })?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            total_bytes_read += bytes_read;

            let mut value = vec![0; val_len as usize];
            bytes_read = file
                .read(&mut value)
                .await
                .map_err(|err| ValueLogFileReadError {
                    error: io::Error::new(err.kind(), EOF),
                })?;

            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            total_bytes_read += bytes_read;

            let is_tombstone = istombstone_bytes[0] == 1;
            entries.push(ValueLogEntry {
                ksize: key_len as usize,
                vsize: val_len as usize,
                key,
                value,
                created_at,
                is_tombstone,
            });

            // Ensure the size read from value log is approximately bytes expected to be garbage collected
            if total_bytes_read >= bytes_to_collect {
                return Ok(entries);
            }
        }
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
