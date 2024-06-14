use chrono::{DateTime, Utc};
use crossbeam_skiplist::{SkipList, SkipMap};
use futures::TryFutureExt;
use num_traits::ops::bytes;
use std::{
    cmp::Ordering,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{fs, io::AsyncReadExt, sync::RwLock};
use tokio::{fs::File, io};
use tokio::{fs::OpenOptions, io::AsyncSeekExt};

use crate::{
    block::Block,
    bucket::InsertableToBucket,
    consts::{
        DEFAULT_FALSE_POSITIVE_RATE, EOF, SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8, SIZE_OF_USIZE,
    },
    err::Error,
    filter::BloomFilter,
    fs::{FileAsync, FileNode},
    index::{self, Index, RangeOffset},
    memtable::{Entry, InsertionTime, IsDeleted},
    types::{CreationTime, IsTombStone, Key, ValOffset},
    utils::data_file_exists,
};

use Error::*;

#[derive(Debug, Clone)]
pub struct TableFile<F: FileAsync> {
    pub(crate) file: F,
    pub(crate) path: PathBuf,
}

impl<F: FileAsync> TableFile<F> {
    pub fn new(path: PathBuf, file: F) -> Self {
        Self { path, file }
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub(crate) dir: PathBuf,
    pub(crate) hotness: u64,
    pub(crate) size: usize,
    pub(crate) created_at: CreationTime,
    pub(crate) data_file: TableFile<FileNode>,
    pub(crate) index_file: TableFile<FileNode>,
    pub(crate) entries: Arc<SkipMap<Key, (ValOffset, InsertionTime, IsDeleted)>>,
}

impl InsertableToBucket for Table {
    fn get_entries(&self) -> Arc<SkipMap<Key, (ValOffset, InsertionTime, IsDeleted)>> {
        Arc::clone(&self.entries)
    }
    fn size(&self) -> usize {
        self.size
    }
    fn find_biggest_key_from_table(&self) -> Result<Vec<u8>, Error> {
        self.find_biggest_key()
    }

    fn find_smallest_key_from_table(&self) -> Result<Vec<u8>, Error> {
        self.find_smallest_key()
    }
}

impl Table {
    pub async fn new(dir: PathBuf) -> Self {
        //TODO: handle error during file creation
        let (data_file_path, index_file_path, creation_time) =
            Table::generate_file_path(dir.to_owned()).await.unwrap();
        let data_file = FileNode::new(data_file_path.to_owned(), crate::fs::FileType::SSTable)
            .await
            .unwrap();
        let index_file = FileNode::new(index_file_path.to_owned(), crate::fs::FileType::Index)
            .await
            .unwrap();

        Self {
            dir,
            hotness: 0,
            index_file: TableFile::new(index_file_path, index_file),
            data_file: TableFile::new(data_file_path, data_file),
            created_at: creation_time.timestamp_millis() as u64,
            entries: Arc::new(SkipMap::new()),
            size: 0,
        }
    }
    pub fn increase_hotness(&mut self) {
        self.hotness += 1;
    }
    pub fn get_data_file_path(&self) -> PathBuf {
        self.data_file.path.clone()
    }

    pub fn get_hotness(&self) -> u64 {
        self.hotness
    }

    pub async fn generate_file_path(
        dir: PathBuf,
    ) -> Result<(PathBuf, PathBuf, DateTime<Utc>), Error> {
        let created_at = Utc::now();
        let _ = FileNode::create_dir_all(dir.to_owned()).await?;
        let data_file_name = format!("sstable_{}_.db", created_at.timestamp_millis());
        let index_file_name = format!("index_{}_.db", created_at.timestamp_millis());

        let data_file_path = dir.join(data_file_name.to_owned());
        let index_file_path = dir.join(index_file_name.to_owned());
        Ok((data_file_path, index_file_path, created_at))
    }

    
    pub(crate) async fn get(
        &self,
        start_offset: u32,
        searched_key: &[u8],
    ) -> Result<Option<(ValOffset, CreationTime, IsTombStone)>, Error> {
        let data_file = &self.data_file.file;
        // data_file
        //     .seek(
        //         crate::fs::LockType::ReadInnerLock,
        //         start_offset.into(),
        //     )
        //     .await?;
            let mut lock = self.data_file.file.w_lock().await;
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read = data_file
                .read_buf(
                    crate::fs::LockType::WriteOuterLock(&mut lock),
                    &mut key_len_bytes,
                )
                .await?;
          
            return Ok(None);
        // loop {
        //     let mut key_len_bytes = [0; SIZE_OF_U32];
        //     let mut bytes_read = data_file
        //         .read_buf(
        //             crate::fs::LockType::WriteInnerLock,
        //             &mut key_len_bytes,
        //         )
        //         .await?;
        //     if bytes_read == 0 {
        //         return Ok(None);
        //     }
        //     let key_len = u32::from_le_bytes(key_len_bytes);
        //     let mut key = vec![0; key_len as usize];
        //     bytes_read = data_file
        //         .read_buf(
        //             crate::fs::LockType::WriteInnerLock,
        //             &mut key,
        //         )
        //         .await?;
        //     if bytes_read == 0 {
        //         return Err(UnexpectedEOF(io::Error::new(
        //             io::ErrorKind::UnexpectedEof,
        //             EOF,
        //         )));
        //     }
        //     let mut val_offset_bytes = [0; SIZE_OF_U32];
        //     bytes_read = data_file
        //         .read_buf(
        //             crate::fs::LockType::WriteInnerLock,
        //             &mut val_offset_bytes,
        //         )
        //         .await?;
        //     if bytes_read == 0 {
        //         return Err(UnexpectedEOF(io::Error::new(
        //             io::ErrorKind::UnexpectedEof,
        //             EOF,
        //         )));
        //     }
        //     let mut created_at_bytes = [0; SIZE_OF_U64];
        //     bytes_read = data_file
        //         .read_buf(
        //             crate::fs::LockType::WriteInnerLock,
        //             &mut created_at_bytes,
        //         )
        //         .await?;
        //     if bytes_read == 0 {
        //         return Err(UnexpectedEOF(io::Error::new(
        //             io::ErrorKind::UnexpectedEof,
        //             EOF,
        //         )));
        //     }

        //     let mut is_tombstone_byte = [0; SIZE_OF_U8];
        //     bytes_read = data_file
        //         .read_buf(
        //             crate::fs::LockType::WriteInnerLock,
        //             &mut is_tombstone_byte,
        //         )
        //         .await?;
        //     if bytes_read == 0 {
        //         return Err(UnexpectedEOF(io::Error::new(
        //             io::ErrorKind::UnexpectedEof,
        //             EOF,
        //         )));
        //     }

        //     let created_at = u64::from_le_bytes(created_at_bytes);
        //     let value_offset = u32::from_le_bytes(val_offset_bytes);
        //     let is_tombstone = is_tombstone_byte[0] == 1;
        //     if key == searched_key {
        //         return Ok(Some((value_offset as usize, created_at, is_tombstone)));
        //     }
        // }
    }

    pub(crate) async fn from_file(&self) -> Result<Option<Table>, Error> {
        let entries = Arc::new(SkipMap::new());
        let mut total_bytes_read = 0;
        let data_file = &self.data_file.file;
        data_file
            .seek(crate::fs::LockType::WriteInnerLock, 0)
            .await?;
        loop {
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read = data_file
                .read_buf(
                    crate::fs::LockType::WriteInnerLock,
                    &mut key_len_bytes,
                )
                .await?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                break;
            }
            let key_len = u32::from_le_bytes(key_len_bytes);

            let mut key = vec![0; key_len as usize];
            bytes_read = data_file
                .read_buf(
                    crate::fs::LockType::WriteInnerLock,
                    &mut key,
                )
                .await?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            let mut val_offset_bytes = [0; SIZE_OF_U32];
            bytes_read = data_file
                .read_buf(
                    crate::fs::LockType::WriteInnerLock,
                    &mut val_offset_bytes,
                )
                .await?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            let mut created_at_bytes = [0; SIZE_OF_U64];
            bytes_read = data_file
                .read_buf(
                    crate::fs::LockType::WriteInnerLock,
                    &mut created_at_bytes,
                )
                .await?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            let mut is_tombstone_byte = [0; SIZE_OF_U8];
            bytes_read = data_file
                .read_buf(
                    crate::fs::LockType::WriteInnerLock,
                    &mut is_tombstone_byte,
                )
                .await?;
            total_bytes_read += bytes_read;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }

            let created_at = u64::from_le_bytes(created_at_bytes);
            let value_offset = u32::from_le_bytes(val_offset_bytes);
            let is_tombstone = is_tombstone_byte[0] == 1;
            entries.insert(key, (value_offset as usize, created_at, is_tombstone));
        }

        for entry in entries.iter() {
            println!(
                "ENTRIE LENGTH K {:?} V {:?}",
                String::from_utf8_lossy(entry.key()),
                entry.value()
            )
        }

        println!("here is how we return {}", entries.len());

        Ok(Some(Table {
            entries,
            size: total_bytes_read,
            dir: self.dir.clone(),
            hotness: self.hotness,
            created_at: self.created_at,
            data_file: self.data_file.to_owned(),
            index_file: self.index_file.to_owned(),
        }))
    }

    // Find the biggest element in the skip list
    pub fn find_biggest_key(&self) -> Result<Vec<u8>, Error> {
        let largest_entry = self.entries.iter().next_back();
        match largest_entry {
            Some(e) => return Ok(e.key().to_vec()),
            None => Err(BiggestKeyIndexError),
        }
    }

    // Find the biggest element in the skip list
    pub fn find_smallest_key(&self) -> Result<Vec<u8>, Error> {
        let largest_entry = self.entries.iter().next();
        match largest_entry {
            Some(e) => return Ok(e.key().to_vec()),
            None => Err(LowestKeyIndexError),
        }
    }

    pub(crate) async fn write_to_file(&self) -> Result<(), Error> {
        //TODO handle this errors

        let index_file = &self.index_file;

        let mut blocks: Vec<Block> = Vec::new();
        let mut table_index = Index::new(self.index_file.path.clone(), index_file.file.clone());
        let mut current_block = Block::new();
        for e in self.entries.iter() {
            let entry = Entry::new(e.key().clone(), e.value().0, e.value().1, e.value().2);

            // mem::size_of function is efficient because the size is known at compile time so
            // during compilation this will be replaced  with the actual size i.e number of bytes to store the type
            // key length(used during fetch) + key len(actual key length) + value length(4 bytes) + date in milliseconds(8 bytes)
            let entry_size = entry.key.len() + SIZE_OF_U32 + SIZE_OF_U32 + SIZE_OF_U64 + SIZE_OF_U8;

            if current_block.is_full(entry_size) {
                blocks.push(current_block);
                current_block = Block::new();
            }

            current_block.set_entry(
                entry.key.len() as u32,
                entry.key,
                entry.val_offset as u32,
                entry.created_at,
                entry.is_tombstone,
            )?;
        }

        for block in blocks.iter() {
            self.write_block(block, &mut table_index).await?;
        }

        // Incase we have some entries in current block, write them to disk
        if current_block.data.len() > 0 {
            self.write_block(&current_block, &mut table_index).await?;
        }

        table_index.write_to_file().await?;
        Ok(())
    }

    async fn write_block(&self, block: &Block, table_index: &mut Index) -> Result<(), Error> {
        let data_file = &self.data_file;
        let data_file_lock = data_file.file.r_lock().await;
        // Get the current offset before writing (this will be the offset of the value stored in the sparse index)
        let offset = data_file_lock
            .metadata()
            .await
            .map_err(|err| GetFileMetaDataError(err))?
            .len();
        drop(data_file_lock);
        let first_entry = block.get_first_entry();
        // Store initial entry key and its sstable file offset in sparse index
        table_index.insert(first_entry.key_prefix, first_entry.key, offset as u32);

        block.write_to_file(data_file.file.clone()).await?;
        Ok(())
    }

    pub(crate) async fn range(
        &self,
        range_offset: RangeOffset,
    ) -> Result<Vec<Entry<Vec<u8>, usize>>, Error> {
        let mut entries = Vec::new();
        // Open the file in read mode
        let data_file = &self.data_file.file;
        let data_file_lock = self.data_file.file.w_lock().await;
        data_file
            .seek(crate::fs::LockType::WriteInnerLock, 0)
            .await?;
        let mut total_bytes_read = range_offset.start_offset as usize;
        // search sstable for key
        loop {
            let mut key_len_bytes = [0; SIZE_OF_U32];
            let mut bytes_read = data_file
                .read_buf(
                    crate::fs::LockType::WriteInnerLock,
                    &mut key_len_bytes,
                )
                .await?;
            if bytes_read == 0 {
                return Ok(entries);
            }
            total_bytes_read += bytes_read;
            let key_len = u32::from_le_bytes(key_len_bytes);
            let mut key = vec![0; key_len as usize];
            bytes_read = data_file
                .read_buf(
                    crate::fs::LockType::WriteInnerLock,
                    &mut key,
                )
                .await?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            total_bytes_read += bytes_read;
            let mut val_offset_bytes = [0; SIZE_OF_U32];
            bytes_read = data_file
                .read_buf(
                    crate::fs::LockType::WriteInnerLock,
                    &mut val_offset_bytes,
                )
                .await?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            total_bytes_read += bytes_read;
            let mut created_at_bytes = [0; SIZE_OF_U64];
            bytes_read = data_file
                .read_buf(
                    crate::fs::LockType::WriteInnerLock,
                    &mut created_at_bytes,
                )
                .await?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            total_bytes_read += bytes_read;

            let mut is_tombstone_byte = [0; SIZE_OF_U8];
            bytes_read = data_file
                .read_buf(
                    crate::fs::LockType::WriteInnerLock,
                    &mut is_tombstone_byte,
                )
                .await?;
            if bytes_read == 0 {
                return Err(UnexpectedEOF(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    EOF,
                )));
            }
            total_bytes_read += bytes_read;

            let created_at = u64::from_le_bytes(created_at_bytes);
            let value_offset = u32::from_le_bytes(val_offset_bytes) as usize;
            let is_tombstone = is_tombstone_byte[0] == 1;
            entries.push(Entry::new(key, value_offset, created_at, is_tombstone));

            if total_bytes_read as u32 >= range_offset.end_offset {
                return Ok(entries);
            }
        }
    }

    pub(crate) fn build_bloomfilter_from_sstable(
        entries: &Arc<SkipMap<Vec<u8>, (ValOffset, CreationTime, IsTombStone)>>,
    ) -> BloomFilter {
        //TODO: FALSE POSITIVE should be from config
        // Rebuild the bloom filter since a new sstable has been created
        let mut new_bloom_filter = BloomFilter::new(DEFAULT_FALSE_POSITIVE_RATE, entries.len());
        entries.iter().for_each(|e| new_bloom_filter.set(e.key()));
        new_bloom_filter
    }

    pub(crate) fn get_value_from_entries(
        &self,
        key: &[u8],
    ) -> Option<(ValOffset, CreationTime, IsTombStone)> {
        self.entries.get(key).map(|entry| entry.value().to_owned())
    }

    fn compare_offsets(offset_a: usize, offset_b: usize) -> Ordering {
        if offset_a < offset_b {
            Ordering::Less
        } else if offset_a == offset_b {
            Ordering::Equal
        } else {
            Ordering::Greater
        }
    }

    fn size(&self) -> usize {
        self.size
    }

    pub(crate) fn set_entries(
        &mut self,
        entries: Arc<SkipMap<Key, (ValOffset, CreationTime, IsTombStone)>>,
    ) {
        self.entries = entries;
        self.set_sst_size_from_entries();
    }

    pub(crate) fn get_entries(&self) -> Arc<SkipMap<Key, (ValOffset, CreationTime, IsTombStone)>> {
        self.entries.clone()
    }
    pub(crate) fn set_sst_size_from_entries(&mut self) {
        self.size = self
            .entries
            .iter()
            .map(|e| e.key().len() + SIZE_OF_USIZE + SIZE_OF_U64 + SIZE_OF_U8)
            .sum::<usize>();
    }
}
