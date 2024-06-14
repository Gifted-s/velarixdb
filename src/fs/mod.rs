use async_trait::async_trait;
use std::{
    fmt::Debug,
    fs::Metadata,
    io::SeekFrom,
    path::{Path, PathBuf},
    result,
    sync::Arc,
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::err::Error::{self, *};

pub type Buf = [u8];
pub type RGuard<'a, T> = RwLockReadGuard<'a, T>;
pub type WGuard<'a, T> = RwLockWriteGuard<'a, T>;

const RLOCK: &str = "RwLockReadGuard";
const WLOCK: &str = "RwLockWriteGuard";

pub enum LockType<'a> {
    ReadOuterLock(&'a RGuard<'a, File>),
    WriteOuterLock(&'a WGuard<'a, File>),
    ReadInnerLock,
    WriteInnerLock,
}

impl From<LockType<'_>> for &str {
    fn from(value: LockType) -> Self {
        match value {
            LockType::ReadOuterLock(_) => RLOCK,
            LockType::WriteOuterLock(_) => WLOCK,
            LockType::ReadInnerLock => RLOCK,
            LockType::WriteInnerLock => WLOCK,
        }
    }
}
#[derive(Debug, Clone)]
pub enum FileType {
    Index,
    SSTable,
    ValueLog,
}

#[async_trait]
pub trait FileAsync: Send + Sync + Debug + Clone {
    async fn create(path: PathBuf) -> Result<File, Error>;

    async fn create_dir_all(path: PathBuf) -> Result<(), Error>;

    async fn metadata(&self) -> Result<Metadata, Error>;

    async fn open(path: PathBuf) -> Result<File, Error>;

    async fn read_buf(&self, file: LockType<'_>, buf: &mut Buf) -> Result<usize, Error>;

    async fn write_all(&self, file: LockType<'_>, src: &Buf) -> Result<(), Error>;

    async fn sync_all(&self, file: LockType<'_>) -> Result<(), Error>;

    async fn flush(&self, file: LockType<'_>) -> Result<(), Error>;

    async fn seek(&self, mut file: LockType<'_>, start_offset: u64) -> Result<u64, Error>;

    async fn size(&self) -> usize {
        return self.metadata().await.unwrap().len() as usize;
    }

    async fn is_empty(&self) -> bool {
        self.size().await == 0
    }

    async fn w_lock(&self) -> WGuard<File>;

    async fn r_lock(&self) -> RGuard<File>;

    fn unlock(&self, lock_opt: LockType);
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
                .map_err(|err| DirCreationError {
                    path: dir,
                    error: err,
                })?);
        }
        Ok(())
    }

    async fn metadata(&self) -> Result<Metadata, Error> {
        let file = self.r_lock().await;
        Ok(file
            .metadata()
            .await
            .map_err(|err| GetFileMetaDataError(err))?)
    }

    async fn open(path: PathBuf) -> Result<File, Error> {
        Ok(File::open(path.to_owned())
            .await
            .map_err(|err| FileOpenError { path, error: err })?)
    }

    async fn read_buf(&self, file_lock: LockType<'_>, mut buf: &mut Buf) -> Result<usize, Error> {
        match file_lock {
            LockType::WriteOuterLock(file) => {
                Ok(file.read(&mut buf).await.map_err(|err| FileReadError {
                    path: self.file_path.clone(),
                    error: err,
                })?)
            }
            LockType::WriteInnerLock => {
                let file = self.w_lock().await;
                Ok(file.read(&mut buf).await.map_err(|err| FileReadError {
                    path: self.file_path.clone(),
                    error: err,
                })?)
            }
            _ => Err(Error::InvalidLockType {
                expected: WLOCK,
                found: file_lock.into(),
            }),
        }
    }

    async fn write_all(&self, file_lock: LockType<'_>, mut buf: &Buf) -> Result<(), Error> {
        match file_lock {
            LockType::WriteOuterLock(mut file) => {
                Ok(file
                    .write_all(&mut buf)
                    .await
                    .map_err(|err| FileWriteError {
                        path: self.file_path.clone(),
                        error: err,
                    })?)
            }
            LockType::WriteInnerLock => {
                let mut file = self.w_lock().await;
                Ok(file
                    .write_all(&mut buf)
                    .await
                    .map_err(|err| FileWriteError {
                        path: self.file_path.clone(),
                        error: err,
                    })?)
            }
            _ => Err(Error::InvalidLockType {
                expected: WLOCK,
                found: file_lock.into(),
            }),
        }
    }

    async fn sync_all(&self, file_lock: LockType<'_>) -> Result<(), Error> {
        match file_lock {
            LockType::WriteOuterLock(file) => Ok(file
                .sync_all()
                .await
                .map_err(|err| Error::FileSyncError { error: err })?),
            LockType::WriteInnerLock => {
                let file = self.w_lock().await;
                Ok(file
                    .sync_all()
                    .await
                    .map_err(|err| Error::FileSyncError { error: err })?)
            }
            _ => Err(Error::InvalidLockType {
                expected: WLOCK,
                found: file_lock.into(),
            }),
        }
    }

    async fn flush(&self, file_lock: LockType<'_>) -> Result<(), Error> {
        match file_lock {
            LockType::WriteOuterLock(mut file) => Ok(file
                .flush()
                .await
                .map_err(|err| Error::FileSyncError { error: err })?),
            LockType::WriteInnerLock => {
                let mut file = self.w_lock().await;
                Ok(file
                    .flush()
                    .await
                    .map_err(|err| Error::FileSyncError { error: err })?)
            }

            _ => Err(Error::InvalidLockType {
                expected: WLOCK,
                found: file_lock.into(),
            }),
        }
    }

    async fn seek(&self, file_lock: LockType<'_>, start_offset: u64) -> Result<u64, Error> {
        match file_lock {
            LockType::WriteOuterLock(mut file) => Ok(file
                .seek(SeekFrom::Start(start_offset))
                .await
                .map_err(|err| FileSeekError(err))?),
            LockType::WriteInnerLock => {
                let mut file = self.w_lock().await;
                Ok(file
                    .seek(SeekFrom::Start(start_offset))
                    .await
                    .map_err(|err| FileSeekError(err))?)
            }

            _ => Err(Error::InvalidLockType {
                expected: WLOCK,
                found: file_lock.into(),
            }),
        }
    }

    async fn w_lock(&self) -> WGuard<File> {
        self.file.write().await
    }

    async fn r_lock(&self) -> RGuard<File> {
        self.file.read().await
    }

    fn unlock(&self, lock_opt: LockType<'_>) {
        match lock_opt {
            LockType::ReadOuterLock(lock) => drop(lock),
            LockType::WriteOuterLock(lock) => drop(lock),
            _ => {}
        }
    }
}
