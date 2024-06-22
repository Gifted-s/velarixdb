use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use std::{fmt::Debug, fs::Metadata, io::SeekFrom, path::PathBuf, sync::Arc};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{
    consts::{EOF, SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8},
    err::Error::{self, *},
    index::RangeOffset,
    load_buffer,
    memtable::Entry,
    types::{CreationTime, IsTombStone, Key, NoBytesRead, SkipMapEntries, ValOffset},
    value_log::ValueLogEntry,
};

#[derive(Debug, Clone)]
pub enum FileType {
    Index,
    SSTable,
    ValueLog,
}
pub type Buf = [u8];
pub type RGuard<'a, T> = RwLockReadGuard<'a, T>;
pub type WGuard<'a, T> = RwLockWriteGuard<'a, T>;

#[async_trait]
pub trait FileAsync: Send + Sync + Debug + Clone {
    async fn create(path: PathBuf) -> Result<File, Error>;

    async fn create_dir_all(path: PathBuf) -> Result<(), Error>;

    async fn metadata(&self) -> Result<Metadata, Error>;

    async fn open(path: PathBuf) -> Result<File, Error>;

    async fn read_buf(&self, buf: &mut Buf) -> Result<usize, Error>;

    async fn write_all(&self, src: &Buf) -> Result<(), Error>;

    async fn sync_all(&self) -> Result<(), Error>;

    async fn flush(&self) -> Result<(), Error>;

    async fn seek(&self, start_offset: u64) -> Result<u64, Error>;

    async fn remove_dir_all(&self) -> Result<(), Error>;

    async fn w_lock(&self) -> WGuard<File>;

    async fn r_lock(&self) -> RGuard<File>;

    async fn size(&self) -> usize {
        return self.metadata().await.unwrap().len() as usize;
    }

    async fn is_empty(&self) -> bool {
        self.size().await == 0
    }
}

#[async_trait]
pub trait DataFs: Send + Sync + Debug + Clone {
    async fn new(path: PathBuf, file_type: FileType) -> Result<Self, Error>;
    async fn load_entries(&self) -> Result<(SkipMapEntries<Key>, usize), Error>;

    async fn find_entry(
        &self,
        offset: u32,
        searched_key: &[u8],
    ) -> Result<Option<(ValOffset, CreationTime, IsTombStone)>, Error>;

    async fn load_entries_within_range(&self, range_offset: RangeOffset) -> Result<Vec<Entry<Vec<u8>, usize>>, Error>;
}

#[async_trait]
pub trait VLogFs: Send + Sync + Debug + Clone {
    async fn new(path: PathBuf, file_type: FileType) -> Result<Self, Error>;

    async fn get(&self, start_offset: usize) -> Result<Option<(Vec<u8>, bool)>, Error>;

    async fn recover(&self, start_offset: usize) -> Result<Vec<ValueLogEntry>, Error>;

    async fn read_chunk_to_garbage_collect(
        &self,
        bytes_to_collect: usize,
        offset: u64,
    ) -> Result<(Vec<ValueLogEntry>, NoBytesRead), Error>;
}

#[async_trait]
pub trait IndexFs: Send + Sync + Debug + Clone {
    async fn new(path: PathBuf, file_type: FileType) -> Result<Self, Error>;

    async fn get_from_index(&self, searched_key: &[u8]) -> Result<Option<u32>, Error>;

    async fn get_block_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<RangeOffset, Error>;
}

#[derive(Debug, Clone)]
pub struct FileNode {
    pub file_path: PathBuf,
    pub file: Arc<RwLock<File>>,
    pub file_type: FileType,
}

impl FileNode {
    pub async fn new(path: PathBuf, file_type: FileType) -> Result<Self, Error> {
        let file = FileNode::create(path.to_owned()).await?;
        return Ok(Self {
            file_type,
            file: Arc::new(RwLock::new(file)),
            file_path: path,
        });
    }
}

#[async_trait]
impl FileAsync for FileNode {
    async fn create(path: PathBuf) -> Result<File, Error> {
        Ok(OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.clone())
            .await
            .map_err(|err| FileCreationError { path, error: err })?)
    }

    async fn create_dir_all(dir: PathBuf) -> Result<(), Error> {
        if !dir.exists() {
            return Ok(fs::create_dir_all(&dir)
                .await
                .map_err(|err| DirCreationError { path: dir, error: err })?);
        }
        Ok(())
    }

    async fn metadata(&self) -> Result<Metadata, Error> {
        let file = self.r_lock().await;
        Ok(file.metadata().await.map_err(|err| GetFileMetaDataError(err))?)
    }

    async fn open(path: PathBuf) -> Result<File, Error> {
        Ok(File::open(path.to_owned())
            .await
            .map_err(|err| FileOpenError { path, error: err })?)
    }

    async fn read_buf(&self, mut buf: &mut Buf) -> Result<usize, Error> {
        let mut file = self.w_lock().await;
        Ok(file.read(&mut buf).await.map_err(|err| FileReadError {
            path: self.file_path.clone(),
            error: err,
        })?)
    }

    async fn write_all(&self, mut buf: &Buf) -> Result<(), Error> {
        let mut file = self.w_lock().await;
        Ok(file.write_all(&mut buf).await.map_err(|err| FileWriteError {
            path: self.file_path.clone(),
            error: err,
        })?)
    }

    async fn sync_all(&self) -> Result<(), Error> {
        let file = self.w_lock().await;
        Ok(file
            .sync_all()
            .await
            .map_err(|err| Error::FileSyncError { error: err })?)
    }

    async fn flush(&self) -> Result<(), Error> {
        let mut file = self.w_lock().await;
        Ok(file.flush().await.map_err(|err| Error::FileSyncError { error: err })?)
    }

    async fn seek(&self, start_offset: u64) -> Result<u64, Error> {
        let mut file = self.w_lock().await;
        Ok(file
            .seek(SeekFrom::Start(start_offset))
            .await
            .map_err(|err| FileSeekError(err))?)
    }

    async fn remove_dir_all(&self) -> Result<(), Error> {
        Ok(fs::remove_dir_all(&self.file_path)
            .await
            .map_err(|err| DirDeleteError(err))?)
    }

    async fn w_lock(&self) -> WGuard<File> {
        self.file.write().await
    }

    async fn r_lock(&self) -> RGuard<File> {
        self.file.read().await
    }
}

#[derive(Debug, Clone)]
pub struct DataFileNode {
    pub node: FileNode,
}

#[async_trait]
impl DataFs for DataFileNode {
    async fn new(path: PathBuf, file_type: FileType) -> Result<DataFileNode, Error> {
        let node = FileNode::new(path, file_type).await?;
        Ok(DataFileNode { node })
    }
    async fn load_entries(&self) -> Result<(SkipMapEntries<Key>, NoBytesRead), Error> {
        let entries = Arc::new(SkipMap::new());
        let mut total_bytes_read = 0;
        let path = &self.node.file_path;
        let mut file = self.node.file.write().await;
        file.seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|err| FileSeekError(err))?;

        loop {
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read = load_buffer!(file, &mut key_len_bytes, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                break;
            }

            let key_len = u32::from_le_bytes(key_len_bytes);
            let mut key = vec![0; key_len as usize];
            bytes_read = load_buffer!(file, &mut key, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut val_offset_bytes = [0; SIZE_OF_U32];
            bytes_read = load_buffer!(file, &mut val_offset_bytes, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut created_at_bytes = [0; SIZE_OF_U64];
            bytes_read = load_buffer!(file, &mut created_at_bytes, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut is_tombstone_byte = [0; SIZE_OF_U8];
            bytes_read = load_buffer!(file, &mut is_tombstone_byte, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let created_at = u64::from_le_bytes(created_at_bytes);
            let value_offset = u32::from_le_bytes(val_offset_bytes);
            let is_tombstone = is_tombstone_byte[0] == 1;
            entries.insert(key, (value_offset as usize, created_at, is_tombstone));
        }
        return Ok((entries, total_bytes_read));
    }

    async fn find_entry(
        &self,
        offset: u32,
        searched_key: &[u8],
    ) -> Result<Option<(ValOffset, CreationTime, IsTombStone)>, Error> {
        let path = &self.node.file_path;
        let mut file = self.node.file.write().await;
        file.seek(std::io::SeekFrom::Start(offset.into()))
            .await
            .map_err(|err| FileSeekError(err))?;

        loop {
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read = load_buffer!(file, &mut key_len_bytes, path.to_owned())?;
            if bytes_read == 0 {
                return Ok(None);
            }

            let key_len = u32::from_le_bytes(key_len_bytes);
            let mut key = vec![0; key_len as usize];
            bytes_read = load_buffer!(file, &mut key, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut val_offset_bytes = [0; SIZE_OF_U32];
            bytes_read = load_buffer!(file, &mut val_offset_bytes, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut created_at_bytes = [0; SIZE_OF_U64];
            bytes_read = load_buffer!(file, &mut created_at_bytes, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut is_tombstone_byte = [0; SIZE_OF_U8];
            bytes_read = load_buffer!(file, &mut is_tombstone_byte, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let created_at = u64::from_le_bytes(created_at_bytes);
            let value_offset = u32::from_le_bytes(val_offset_bytes);
            let is_tombstone = is_tombstone_byte[0] == 1;
            if key == searched_key {
                return Ok(Some((value_offset as usize, created_at, is_tombstone)));
            }
        }
    }

    async fn load_entries_within_range(&self, range_offset: RangeOffset) -> Result<Vec<Entry<Vec<u8>, usize>>, Error> {
        let mut entries = Vec::new();
        let mut total_bytes_read = 0;
        let path = &self.node.file_path;
        let mut file = self.node.file.write().await;
        file.seek(std::io::SeekFrom::Start((range_offset.start_offset) as u64))
            .await
            .map_err(|err| FileSeekError(err))?;

        loop {
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read = load_buffer!(file, &mut key_len_bytes, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Ok(entries);
            }

            let key_len = u32::from_le_bytes(key_len_bytes);
            let mut key = vec![0; key_len as usize];
            bytes_read = load_buffer!(file, &mut key, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut val_offset_bytes = [0; SIZE_OF_U32];
            bytes_read = load_buffer!(file, &mut val_offset_bytes, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut created_at_bytes = [0; SIZE_OF_U64];
            bytes_read = load_buffer!(file, &mut created_at_bytes, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut is_tombstone_byte = [0; SIZE_OF_U8];
            bytes_read = load_buffer!(file, &mut is_tombstone_byte, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let created_at = u64::from_le_bytes(created_at_bytes);
            let value_offset = u32::from_le_bytes(val_offset_bytes) as usize;
            let is_tombstone = is_tombstone_byte[0] == 1;
            entries.push(Entry::new(key, value_offset, created_at, is_tombstone));

            if total_bytes_read as u32 >= range_offset.end_offset {
                return Ok(entries);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct VLogFileNode {
    pub node: FileNode,
}

#[async_trait]
impl VLogFs for VLogFileNode {
    async fn new(path: PathBuf, file_type: FileType) -> Result<VLogFileNode, Error> {
        let node = FileNode::new(path, file_type).await?;
        Ok(VLogFileNode { node })
    }
    async fn get(&self, start_offset: usize) -> Result<Option<(Vec<u8>, bool)>, Error> {
        let path = &self.node.file_path;
        let mut file = self.node.file.write().await;
        file.seek(std::io::SeekFrom::Start((start_offset) as u64))
            .await
            .map_err(|err| FileSeekError(err))?;

        let mut key_len_bytes = [0; SIZE_OF_U32];
        let mut bytes_read = load_buffer!(file, &mut key_len_bytes, path.to_owned())?;
        if bytes_read == 0 {
            return Ok(None);
        }

        let key_len = u32::from_le_bytes(key_len_bytes);
        let mut val_len_bytes = [0; SIZE_OF_U32];
        bytes_read = load_buffer!(file, &mut val_len_bytes, path.to_owned())?;
        if bytes_read == 0 {
            return Err(FileNode::unexpected_eof());
        }

        let val_len = u32::from_le_bytes(val_len_bytes);
        let mut creation_date_bytes = [0; SIZE_OF_U64];
        bytes_read = load_buffer!(file, &mut creation_date_bytes, path.to_owned())?;
        if bytes_read == 0 {
            return Err(FileNode::unexpected_eof());
        }

        let _ = u64::from_le_bytes(creation_date_bytes);
        let mut istombstone_bytes = [0; SIZE_OF_U8];
        let mut bytes_read = load_buffer!(file, &mut istombstone_bytes, path.to_owned())?;
        if bytes_read == 0 {
            return Err(FileNode::unexpected_eof());
        }

        let is_tombstone = istombstone_bytes[0] == 1;
        let mut key = vec![0; key_len as usize];
        bytes_read = load_buffer!(file, &mut key, path.to_owned())?;
        if bytes_read == 0 {
            return Err(FileNode::unexpected_eof());
        }

        let mut value = vec![0; val_len as usize];
        bytes_read = load_buffer!(file, &mut value, path.to_owned())?;
        if bytes_read == 0 {
            return Err(FileNode::unexpected_eof());
        }
        Ok(Some((value, is_tombstone)))
    }

    async fn recover(&self, start_offset: usize) -> Result<Vec<ValueLogEntry>, Error> {
        let path = &self.node.file_path;
        let mut entries = Vec::new();
        let mut file = self.node.file.write().await;
        file.seek(std::io::SeekFrom::Start((start_offset) as u64))
            .await
            .map_err(|err| FileSeekError(err))?;

        loop {
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read = load_buffer!(file, &mut key_len_bytes, path.to_owned())?;
            if bytes_read == 0 {
                return Ok(entries);
            }

            let key_len = u32::from_le_bytes(key_len_bytes);
            let mut val_len_bytes = [0; SIZE_OF_U32];
            bytes_read = load_buffer!(file, &mut val_len_bytes, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let val_len = u32::from_le_bytes(val_len_bytes);
            let mut creation_date_bytes = [0; SIZE_OF_U64];
            bytes_read = load_buffer!(file, &mut creation_date_bytes, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let created_at = u64::from_le_bytes(creation_date_bytes);
            let mut istombstone_bytes = [0; SIZE_OF_U8];
            let mut bytes_read = load_buffer!(file, &mut istombstone_bytes, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let is_tombstone = istombstone_bytes[0] == 1;
            let mut key = vec![0; key_len as usize];
            bytes_read = load_buffer!(file, &mut key, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut value = vec![0; val_len as usize];
            bytes_read = load_buffer!(file, &mut value, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }
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

    async fn read_chunk_to_garbage_collect(
        &self,
        bytes_to_collect: usize,
        offset: u64,
    ) -> Result<(Vec<ValueLogEntry>, NoBytesRead), Error> {
        let path = &self.node.file_path;
        let mut entries = Vec::new();
        let mut file = self.node.file.write().await;
        file.seek(std::io::SeekFrom::Start((offset) as u64))
            .await
            .map_err(|err| FileSeekError(err))?;
        let mut total_bytes_read: usize = 0;
        loop {
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read = load_buffer!(file, &mut key_len_bytes, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Ok((entries, total_bytes_read));
            }

            let key_len = u32::from_le_bytes(key_len_bytes);
            let mut val_len_bytes = [0; SIZE_OF_U32];
            bytes_read = load_buffer!(file, &mut val_len_bytes, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let val_len = u32::from_le_bytes(val_len_bytes);
            let mut creation_date_bytes = [0; SIZE_OF_U64];
            bytes_read = load_buffer!(file, &mut creation_date_bytes, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let created_at = u64::from_le_bytes(creation_date_bytes);
            let mut istombstone_bytes = [0; SIZE_OF_U8];
            let mut bytes_read = load_buffer!(file, &mut istombstone_bytes, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let is_tombstone = istombstone_bytes[0] == 1;
            let mut key = vec![0; key_len as usize];
            bytes_read = load_buffer!(file, &mut key, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut value = vec![0; val_len as usize];
            bytes_read = load_buffer!(file, &mut value, path.to_owned())?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }
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
                return Ok((entries, total_bytes_read));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexFileNode {
    pub node: FileNode,
}
#[async_trait]
impl IndexFs for IndexFileNode {
    async fn new(path: PathBuf, file_type: FileType) -> Result<IndexFileNode, Error> {
        let node = FileNode::new(path, file_type).await?;
        Ok(IndexFileNode { node })
    }
    async fn get_from_index(&self, searched_key: &[u8]) -> Result<Option<u32>, Error> {
        let path = &self.node.file_path;
        let mut block_offset: i32 = -1;
        let mut file = self.node.file.write().await;
        file.seek(std::io::SeekFrom::Start((0) as u64))
            .await
            .map_err(|err| FileSeekError(err))?;

        loop {
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read = load_buffer!(file, &mut key_len_bytes, path.to_owned())?;
            if bytes_read == 0 {
                if block_offset == -1 {
                    return Ok(None);
                }
                return Ok(Some(block_offset as u32));
            }

            let key_len = u32::from_le_bytes(key_len_bytes);
            let mut key = vec![0; key_len as usize];
            bytes_read = load_buffer!(file, &mut key, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut key_offset_bytes = [0; SIZE_OF_U32];
            bytes_read = load_buffer!(file, &mut key_offset_bytes, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }
            let offset = u32::from_le_bytes(key_offset_bytes);
            match key.cmp(&searched_key.to_vec()) {
                std::cmp::Ordering::Less => {
                    continue;
                }
                std::cmp::Ordering::Equal => {
                    return Ok(Some(offset));
                }
                std::cmp::Ordering::Greater => {
                    return Ok(Some(offset));
                }
            }
        }
    }

    async fn get_block_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<RangeOffset, Error> {
        let path = &self.node.file_path;
        let mut range_offset = RangeOffset::new(0, 0);
        let mut file = self.node.file.write().await;
        file.seek(std::io::SeekFrom::Start((0) as u64))
            .await
            .map_err(|err| FileSeekError(err))?;

        loop {
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read = load_buffer!(file, &mut key_len_bytes, path.to_owned())?;
            if bytes_read == 0 {
                return Ok(range_offset);
            }

            let key_len = u32::from_le_bytes(key_len_bytes);
            let mut key = vec![0; key_len as usize];
            bytes_read = load_buffer!(file, &mut key, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }

            let mut key_offset_bytes = [0; SIZE_OF_U32];
            bytes_read = load_buffer!(file, &mut key_offset_bytes, path.to_owned())?;
            if bytes_read == 0 {
                return Err(FileNode::unexpected_eof());
            }
            let offset = u32::from_le_bytes(key_offset_bytes);
            match key.cmp(&start_key.to_vec()) {
                std::cmp::Ordering::Greater => match key.cmp(&end_key.to_vec()) {
                    std::cmp::Ordering::Greater => {
                        range_offset.end_offset = offset;
                        return Ok(range_offset);
                    }
                    _ => range_offset.end_offset = offset,
                },
                _ => range_offset.start_offset = offset,
            }
        }
    }
}

impl FileNode {
    fn unexpected_eof() -> Error {
        return UnexpectedEOF(io::Error::new(io::ErrorKind::UnexpectedEof, EOF));
    }
}
