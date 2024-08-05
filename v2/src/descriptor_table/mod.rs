use crate::sst::id::GlobalSegmentId;
use lru::LruList;
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
};

mod lru;

pub struct FileGuard(Arc<FileDescriptorWrapper>);

impl std::ops::Deref for FileGuard {
    type Target = Arc<FileDescriptorWrapper>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        self.0.is_used.store(false, std::sync::atomic::Ordering::Release);
    }
}

pub struct FileDescriptorWrapper {
    pub file: Mutex<BufReader<File>>,
    is_used: AtomicBool,
}

pub struct FileHandle {
    descriptors: RwLock<Vec<Arc<FileDescriptorWrapper>>>,
    path: PathBuf,
}

pub struct FileDescriptorInner {
    table: HashMap<GlobalSegmentId, FileHandle>,
    lru: Mutex<LruList<GlobalSegmentId>>,
    size: AtomicUsize,
}

pub struct FileDescriptorTable {
    inner: RwLock<FileDescriptorInner>,
    concurrency: usize,
    limit: usize,
}

impl FileDescriptorTable {
    /// Closes all file descriptor
    pub fn clear(&self) {
        let mut lock = self.inner.write().expect("lock is poisoned");
        lock.table.clear()
    }

    #[must_use]
    pub fn new(limit: usize, concurrency: usize) -> Self {
        Self {
            inner: RwLock::new(FileDescriptorInner {
                table: HashMap::with_capacity(100),
                lru: Mutex::new(LruList::with_capacity(100)),
                size: AtomicUsize::default(),
            }),
            concurrency,
            limit,
        }
    }

    /// Number of segments
    pub fn len(&self) -> usize {
        self.inner.read().expect("lock is poisoned").table.len()
    }

    pub fn size(&self) -> usize {
        self.inner
            .read()
            .expect("lock is poisoned")
            .size
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn access(&self, id: &GlobalSegmentId) -> crate::Result<Option<FileGuard>> {
        let lock = self.inner.read().expect("lock is poisoned");

        let Some(item) = lock.table.get(id) else {
            return Ok(None);
        };

        let fd_vec = item.descriptors.read().expect("lock is poisoned");

        if fd_vec.is_empty() {
            drop(fd_vec);
            drop(lock);

            let lock = self.inner.write().expect("lock is poisoned");
            let mut lru = lock.lru.lock().expect("lock is poisoned");
            lru.refresh(*id);

            let fd = {
                let item = lock.table.get(id).expect("should exist");
                let mut fd_lock = item.descriptors.write().expect("lock is poisoned");

                for _ in 0..(self.concurrency - 1) {
                    let fd = Arc::new(FileDescriptorWrapper {
                        file: Mutex::new(BufReader::new(File::open(&item.path)?)),
                        is_used: AtomicBool::default(),
                    });
                    fd_lock.push(fd.clone());
                }
                let fd = Arc::new(FileDescriptorWrapper {
                    file: Mutex::new(BufReader::new(File::open(&item.path)?)),
                    is_used: AtomicBool::default(),
                });
                fd_lock.push(fd.clone());

                fd
            };

            let mut size_now = lock
                .size
                .fetch_add(self.concurrency, std::sync::atomic::Ordering::AcqRel)
                + self.concurrency;

            while size_now > self.limit {
                if let Some(oldest) = lru.get_least_recently_used() {
                    if &oldest != id {
                        if let Some(item) = lock.table.get(&oldest) {
                            let mut oldest_lock = item.descriptors.write().expect("lock is poisoned");

                            lock.size
                                .fetch_sub(oldest_lock.len(), std::sync::atomic::Ordering::Release);
                            size_now -= oldest_lock.len();

                            oldest_lock.clear();
                        }
                    }
                } else {
                    break;
                }
            }
            Ok(Some(FileGuard(fd)))
        } else {
            loop {
                for shard in &*fd_vec {
                    if shard.is_used.compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    ) == Ok(false)
                    {
                        return Ok(Some(FileGuard(shard.clone())));
                    }
                }
            }
        }
    }

    fn inner_insert(mut lock: RwLockWriteGuard<'_, FileDescriptorInner>, path: PathBuf, id: GlobalSegmentId) {
        lock.table.insert(
            id,
            FileHandle {
                descriptors: RwLock::new(vec![]),
                path,
            },
        );
        lock.lru.lock().expect("lock poisoned").refresh(id);
    }

    pub fn insert<P: Into<PathBuf>>(&self, path: P, id: GlobalSegmentId) {
        let lock = self.inner.write().expect("lock poisoned");
        Self::inner_insert(lock, path.into(), id);
    }

    pub fn remove(&self, id: GlobalSegmentId) {
        let mut lock = self.inner.write().expect("lock is poisoned");

        if let Some(item) = lock.table.remove(&id) {
            lock.size.fetch_sub(
                item.descriptors.read().expect("lock is poisoned").len(),
                std::sync::atomic::Ordering::Release,
            );
        }

        lock.lru.lock().expect("lock is poisoned").remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn descriptor_table_limit() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path();

        File::create(path.join("1"))?;
        File::create(path.join("2"))?;
        File::create(path.join("3"))?;

        let table = FileDescriptorTable::new(2, 1);

        assert_eq!(0, table.size());

        table.insert(path.join("1"), (0, 1).into());
        assert_eq!(0, table.size());

        {
            let _ = table.access(&(0, 1).into());
            assert_eq!(1, table.size());
        }

        table.insert(path.join("2"), (0, 2).into());

        {
            assert_eq!(1, table.size());
            let _ = table.access(&(0, 1).into());
        }

        {
            let _ = table.access(&(0, 2).into());
            assert_eq!(2, table.size());
        }

        table.insert(path.join("3"), (0, 3).into());
        assert_eq!(2, table.size());

        {
            let _ = table.access(&(0, 3).into());
            assert_eq!(2, table.size());
        }

        table.remove((0, 3).into());
        assert_eq!(1, table.size());

        table.remove((0, 2).into());
        assert_eq!(0, table.size());

        let _ = table.access(&(0, 1).into());
        assert_eq!(1, table.size());

        Ok(())
    }
}
