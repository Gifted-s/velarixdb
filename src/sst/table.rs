use chrono::{DateTime, Utc};
use crossbeam_skiplist::SkipMap;
use std::{
    cmp::Ordering,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use crate::{
    block::Block,
    bucket::InsertableToBucket,
    consts::{DEFAULT_FALSE_POSITIVE_RATE, SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8, SIZE_OF_USIZE},
    err::Error,
    filter::BloomFilter,
    fs::{DataFileNode, DataFs, FileAsync, FileNode, IndexFileNode, IndexFs},
    index::{Index, IndexFile, RangeOffset},
    memtable::Entry,
    types::{CreationTime, IsTombStone, Key, SkipMapEntries, ValOffset},
};

use Error::*;

#[derive(Debug, Clone)]
pub struct DataFile<F>
where
    F: DataFs,
{
    pub(crate) file: F,
    pub(crate) path: PathBuf,
}

impl<F> DataFile<F>
where
    F: DataFs,
{
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
    pub(crate) data_file: DataFile<DataFileNode>,
    pub(crate) index_file: IndexFile<IndexFileNode>,
    pub(crate) entries: SkipMapEntries<Key>,
}

impl InsertableToBucket for Table {
    fn get_entries(&self) -> SkipMapEntries<Key> {
        self.entries.clone()
    }
    fn size(&self) -> usize {
        self.size
    }
    fn find_biggest_key(&self) -> Result<Vec<u8>, Error> {
        let largest_entry = self.entries.iter().next_back();
        match largest_entry {
            Some(e) => return Ok(e.key().to_vec()),
            None => Err(BiggestKeyIndexError),
        }
    }

    fn find_smallest_key(&self) -> Result<Vec<u8>, Error> {
        let largest_entry = self.entries.iter().next();
        match largest_entry {
            Some(e) => return Ok(e.key().to_vec()),
            None => Err(LowestKeyIndexError),
        }
    }
}

impl Table {
    pub async fn new(dir: PathBuf) -> Self {
        //TODO: handle error during file creation
        let (data_file_path, index_file_path, creation_time) = Table::generate_file_path(dir.to_owned()).await.unwrap();
        let data_file = DataFileNode::new(data_file_path.to_owned(), crate::fs::FileType::SSTable)
            .await
            .unwrap();
        let index_file = IndexFileNode::new(index_file_path.to_owned(), crate::fs::FileType::Index)
            .await
            .unwrap();

        Self {
            dir,
            hotness: 0,
            index_file: IndexFile::new(index_file_path, index_file),
            data_file: DataFile::new(data_file_path, data_file),
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

    pub async fn generate_file_path(dir: PathBuf) -> Result<(PathBuf, PathBuf, DateTime<Utc>), Error> {
        let created_at = Utc::now();
        let _ = FileNode::create_dir_all(dir.to_owned()).await?;
        let data_file_name = format!("data_{}_.db", created_at.timestamp_millis());
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
        self.data_file.file.find_entry(start_offset, searched_key).await
    }

    pub(crate) async fn load_entries_from_file(&self) -> Result<Table, Error> {
        let (entries, bytes_read) = self.data_file.file.load_entries().await?;
        Ok(Table {
            entries,
            size: bytes_read,
            dir: self.dir.clone(),
            hotness: self.hotness,
            created_at: self.created_at,
            data_file: self.data_file.to_owned(),
            index_file: self.index_file.to_owned(),
        })
    }

    pub(crate) async fn build_from(dir: PathBuf, data_file_path: PathBuf, index_file_path: PathBuf) -> Table {
        let mut table = Table {
            dir,
            hotness: 1,
            created_at: Utc::now().timestamp_millis() as u64,
            data_file: DataFile {
                file: DataFileNode::new(data_file_path.to_owned(), crate::fs::FileType::SSTable)
                    .await
                    .unwrap(),
                path: data_file_path,
            },
            index_file: IndexFile {
                file: IndexFileNode::new(index_file_path.to_owned(), crate::fs::FileType::Index)
                    .await
                    .unwrap(),
                path: index_file_path,
            },
            size: 0,
            entries: Arc::new(SkipMap::new()),
        };
        table.size = table.data_file.file.node.size().await;
        let modified_time = table.data_file.file.node.metadata().await.unwrap().modified().unwrap();
        let epoch = SystemTime::UNIX_EPOCH;
        let elapsed_nanos = modified_time.duration_since(epoch).unwrap().as_nanos();
        table.created_at = (elapsed_nanos / 1_000_000) as u64;
        return table;
    }

    pub(crate) async fn write_to_file(&self) -> Result<(), Error> {
        let index_file = &self.index_file;
        let mut blocks: Vec<Block> = Vec::new();
        let mut table_index = Index::new(self.index_file.path.clone(), index_file.file.clone());
        let mut current_block = Block::new();

        for e in self.entries.iter() {
            let entry = Entry::new(e.key().clone(), e.value().0, e.value().1, e.value().2);
            //TODO: reorder  key length(used during fetch) + key len(actual key length) + value length(4 bytes) + date in milliseconds(8 bytes)
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
        if current_block.entries.len() > 0 {
            self.write_block(&current_block, &mut table_index).await?;
        }

        table_index.write_to_file().await?;
        Ok(())
    }

    async fn write_block(&self, block: &Block, table_index: &mut Index) -> Result<(), Error> {
        // Get the current offset before writing (this will be the offset of the value stored in the sparse index)
        let offset = self.data_file.file.node.size().await;
        let last_entry = block.get_last_entry();

        // Store last entry key and its sstable file offset in sparse index
        table_index.insert(last_entry.key_prefix, last_entry.key, offset as u32);

        block.write_to_file(self.data_file.file.node.clone()).await?;
        Ok(())
    }

    pub(crate) async fn range(&self, range_offset: RangeOffset) -> Result<Vec<Entry<Vec<u8>, usize>>, Error> {
        self.data_file.file.load_entries_within_range(range_offset).await
    }

    pub(crate) fn build_filter_from_sstable(entries: &SkipMapEntries<Key>) -> BloomFilter {
        // TODO: FALSE POSITIVE should be from config
        let mut filter = BloomFilter::new(DEFAULT_FALSE_POSITIVE_RATE, entries.len());
        entries.iter().for_each(|e| filter.set(e.key()));
        filter
    }

    pub(crate) fn get_value_from_entries(&self, key: &[u8]) -> Option<(ValOffset, CreationTime, IsTombStone)> {
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

    pub(crate) fn set_entries(&mut self, entries: Arc<SkipMap<Key, (ValOffset, CreationTime, IsTombStone)>>) {
        self.entries = entries;
        self.set_sst_size_from_entries();
    }

    pub(crate) fn set_sst_size_from_entries(&mut self) {
        self.size = self
            .entries
            .iter()
            .map(|e| e.key().len() + SIZE_OF_USIZE + SIZE_OF_U64 + SIZE_OF_U8)
            .sum::<usize>();
    }
}
