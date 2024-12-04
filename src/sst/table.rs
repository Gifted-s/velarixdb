//! # SSTable Data Block
//!
//! The `Data Block` manages multiple `Block` instances and each block stores entries
//! A block size is 4KB on disk but as the project evolve, we will introduce
//! Snappy Compression, Checksum and also have block cache for fast recovery
//!
//! The data block structure
//!
//! ```text
//! +----------------------------------+
//! |            Entry 1               |
//! |            Entry 2               |  // Block 1 (4KB- Uncompressed)
//! |            Entry 3               |
//! +----------------------------------+
//! |            Entry 4               |   
//! |            Entry 5               |  // Block 2 (4KB- Uncompressed)
//! |            Entry 6               |
//! +----------------------------------+
//! |            Entry 7               |
//! |            Enrty 8               |  // Block 3 (4KB- Uncompressed)
//! |            Entry 9               |
//! +----------------------------------+
//! |            Entry 10              |
//! |            Entry 11              |  // Block 4 (4KB- Uncompressed)
//! |            Entry 12              |
//! +----------------------------------+
//! |                                  |
//! |             ...                  |  // ...
//! |                                  |
//! +----------------------------------+
//! ```
//!
//! In the diagram:
//! - The `Block` stores entries until it is 4KB in size and then writes to data file
//! - TODO: In the future we will introduce Snappy Compression to reduce the size on the disk and also introduce checksum to ensure the data has not been corrupted

use crate::{
    block::Block,
    bucket::InsertableToBucket,
    consts::{
        DATA_FILE_NAME, INDEX_FILE_NAME, SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8, SIZE_OF_USIZE,
        SUMMARY_FILE_NAME,
    },
    err::Error,
    filter::BloomFilter,
    fs::{DataFileNode, DataFs, FileAsync, FileNode, IndexFileNode, IndexFs, SummaryFileNode, SummaryFs},
    index::{Index, IndexFile, RangeOffset},
    key_range::{BiggestKey, SmallestKey},
    memtable::{Entry, SkipMapValue},
    types::{ByteSerializedEntry, CreatedAt, IsTombStone, Key, SkipMapEntries, ValOffset},
    util,
};
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};
use Error::*;

/// DataFile
#[derive(Debug, Clone)]
pub struct DataFile<F: DataFs> {
    pub(crate) file: F,
    pub(crate) path: PathBuf,
}

impl<F: DataFs> DataFile<F> {
    /// Creates new `DataFile`
    pub fn new<P: AsRef<Path> + Send + Sync>(path: P, file: F) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            file,
        }
    }
}

/// An SSTable
#[derive(Debug, Clone)]
pub struct Table {
    /// Directory sstable files are stored at
    pub(crate) dir: PathBuf,

    /// How often is this sstable used?
    pub(crate) hotness: u64,

    /// Size of the sstable
    pub(crate) size: usize,

    /// Date created
    pub(crate) created_at: CreatedAt,

    /// SSTable data file
    pub(crate) data_file: DataFile<DataFileNode>,

    /// SSTable index data file
    pub(crate) index_file: IndexFile<IndexFileNode>,

    /// Lock-Free skipmap to store sstable entries
    /// used during retrieval or before flush
    pub(crate) entries: SkipMapEntries<Key>,

    /// Bloom filter for sstable in case of crash recovery. This is not
    /// set until flush
    pub(crate) filter: Option<BloomFilter>,

    /// Stores the summary including biggest and smallest key
    pub(crate) summary: Option<Summary>,
}

/// Defines trait to make `Table` insertable to bucket
impl InsertableToBucket for Table {
    fn get_entries(&self) -> SkipMapEntries<Key> {
        self.entries.clone()
    }

    fn size(&self) -> usize {
        self.size
    }

    fn get_filter(&self) -> BloomFilter {
        self.filter.as_ref().unwrap().to_owned()
    }
}

impl Table {
    /// Creates a new `Table`
    pub async fn new<P: AsRef<Path> + Send + Sync>(dir: P) -> Result<Table, Error> {
        let (data_file_path, index_file_path, created_at) = Table::generate_file_path(dir.as_ref()).await?;
        let data_file = DataFileNode::new(data_file_path.to_owned(), crate::fs::FileType::Data)
            .await
            .unwrap();
        let index_file = IndexFileNode::new(index_file_path.to_owned(), crate::fs::FileType::Index)
            .await
            .unwrap();

        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            hotness: Default::default(),
            index_file: IndexFile::new(index_file_path, index_file),
            data_file: DataFile::new(data_file_path, data_file),
            created_at,
            entries: Arc::new(SkipMap::new()),
            size: Default::default(),
            filter: None,
            summary: None,
        })
    }
    pub fn increase_hotness(&mut self) {
        self.hotness += 1;
    }
    /// Returns `Table` `data_file` path
    pub fn get_data_file_path(&self) -> PathBuf {
        self.data_file.path.clone()
    }

    /// Returns `Table` `hotness`
    pub fn get_hotness(&self) -> u64 {
        self.hotness
    }

    /// Creates table directory
    ///
    /// Returns data and index file name
    pub async fn generate_file_path<P: AsRef<Path> + Send + Sync>(
        dir: P,
    ) -> Result<(PathBuf, PathBuf, CreatedAt), Error> {
        let created_at = Utc::now();
        FileNode::create_dir_all(dir.as_ref()).await?;
        let data_file_name = format!("{}.db", DATA_FILE_NAME);
        let index_file_name = format!("{}.db", INDEX_FILE_NAME);

        let data_file_path = dir.as_ref().join(data_file_name);
        let index_file_path = dir.as_ref().join(index_file_name);
        Ok((data_file_path, index_file_path, created_at))
    }

    /// Returns a key from a block in sstable data file
    ///
    /// # Errors
    ///
    /// Returns IO error in case it occurs
    pub(crate) async fn get<K: AsRef<[u8]>>(
        &self,
        start_offset: u32,
        searched_key: K,
    ) -> Result<Option<(ValOffset, CreatedAt, IsTombStone)>, Error> {
        self.data_file
            .file
            .find_entry(start_offset, searched_key.as_ref())
            .await
    }

    /// Build  `entries` from sstable data file
    ///
    /// # Errors
    ///
    /// Returns IO error incase it occurs
    pub(crate) async fn load_entries_from_file(&mut self) -> Result<(), Error> {
        let (entries, bytes_read) = self.data_file.file.load_entries().await?;
        self.entries = entries;
        self.size = bytes_read;
        Ok(())
    }

    /// Returns new `Table` using the supplied parameters
    pub(crate) async fn build_from<P: AsRef<Path> + Send + Sync + Clone>(
        dir: P,
        data_file_path: P,
        index_file_path: P,
    ) -> Table {
        let mut table = Table {
            dir: dir.as_ref().to_path_buf(),
            hotness: 1,
            created_at: Utc::now(),
            data_file: DataFile {
                file: DataFileNode::new(data_file_path.to_owned(), crate::fs::FileType::Data)
                    .await
                    .unwrap(),
                path: data_file_path.as_ref().to_path_buf(),
            },
            index_file: IndexFile {
                file: IndexFileNode::new(index_file_path.to_owned(), crate::fs::FileType::Index)
                    .await
                    .unwrap(),
                path: index_file_path.as_ref().to_path_buf(),
            },
            size: Default::default(),
            entries: Arc::new(SkipMap::new()),
            filter: None,
            summary: None,
        };
        table.size = table.data_file.file.node.size().await;
        let modified_time = table
            .data_file
            .file
            .node
            .metadata()
            .await
            .unwrap()
            .modified()
            .unwrap();
        let epoch = SystemTime::UNIX_EPOCH;
        let elapsed_nanos = modified_time.duration_since(epoch).unwrap().as_nanos() as u64;
        table.created_at = util::milliseconds_to_datetime(elapsed_nanos / 1_000_000);
        table
    }

    /// Writes SSTable files to disk
    ///
    /// After successful write, the summary and bloom filter
    /// for the table is set and stored in memory
    ///
    /// Errors
    ///
    /// Returns error in case of IO error
    pub(crate) async fn write_to_file(&mut self) -> Result<(), Error> {
        if self.filter.is_none() {
            return Err(FilterNotProvidedForFlush);
        }
        if self.entries.is_empty() {
            return Err(EntriesCannotBeEmptyDuringFlush);
        }
        let index_file = &self.index_file;
        let mut blocks: Vec<Block> = Vec::new();
        let mut index = Index::new(self.index_file.path.clone(), index_file.file.clone());
        let mut summary = Summary::new(self.dir.to_owned());

        let smallest_entry = self.entries.front();
        let biggest_entry = self.entries.back();

        summary.smallest_key = smallest_entry.unwrap().key().to_vec();
        summary.biggest_key = biggest_entry.unwrap().key().to_vec();

        // write summary to disk
        summary.write_to_file().await?;
        self.summary = Some(summary);

        // write filter to disk
        self.filter.as_mut().unwrap().write(self.dir.to_owned()).await?;
        self.filter
            .as_mut()
            .unwrap()
            .set_sstable_path(&self.data_file.path);

        // write data blocks
        let mut current_block = Block::new();
        if self.size > 0 {
            self.reset_size();
        }

        for e in self.entries.iter() {
            let entry = Entry::new(
                e.key(),
                e.value().val_offset,
                e.value().created_at,
                e.value().is_tombstone,
            );

            // key len(variable) +  key prefix + value offset length(4 bytes) + insertion time (8 bytes) + tombstone (1 byte)
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
            self.write_block(block, &mut index).await?;
        }

        // Incase we have some entries left in current block, write them to disk
        if !current_block.entries.is_empty() {
            self.write_block(&current_block, &mut index).await?;
        }
        index.write_to_file().await?;
        Ok(())
    }

    /// Write block to disk
    ///
    /// Errors
    ///
    /// Returns error in case of IO error
    async fn write_block(&mut self, block: &Block, table_index: &mut Index) -> Result<(), Error> {
        let offset = self.size;
        let last_entry = block.get_last_entry();
        table_index.insert(last_entry.key_prefix, last_entry.key, offset as u32);
        let bytes_written = block.write_to_file(self.data_file.file.node.clone()).await?;
        self.size += bytes_written;
        Ok(())
    }

    /// Retreives entries within a specific block range
    ///
    /// Errors
    ///
    /// Returns error in case of IO error
    #[allow(dead_code)]
    pub(crate) async fn range(&self, range_offset: RangeOffset) -> Result<Vec<Entry<Key, usize>>, Error> {
        self.data_file.file.load_entries_within_range(range_offset).await
    }

    pub(crate) fn reset_size(&mut self) {
        self.size = 0;
    }

    /// Returns `size` of `Table`
    pub fn size(&self) -> usize {
        self.size
    }

    /// Set `entries` field in `Table`
    pub(crate) fn set_entries(&mut self, entries: Arc<SkipMap<Key, SkipMapValue<ValOffset>>>) {
        self.entries = entries;
        self.set_sst_size_from_entries();
    }

    /// Set byte `size` from entries size
    pub(crate) fn set_sst_size_from_entries(&mut self) {
        self.size = self
            .entries
            .iter()
            .map(|e| e.key().len() + SIZE_OF_USIZE + SIZE_OF_U64 + SIZE_OF_U8)
            .sum::<usize>();
    }
}

/// Summary of SSTable
#[derive(Debug, Clone)]
pub struct Summary {
    /// File path for summary file
    pub path: PathBuf,

    /// Smallest key in `Table`
    pub smallest_key: SmallestKey,

    /// Biggest key in `Table`
    pub biggest_key: BiggestKey,
}

impl Summary {
    /// Create new `Summary`
    pub fn new<P: AsRef<Path> + Send + Sync>(path: P) -> Self {
        let file_path = path.as_ref().join(format!("{}.db", SUMMARY_FILE_NAME));
        Self {
            path: file_path,
            biggest_key: vec![],
            smallest_key: vec![],
        }
    }

    /// Writes `Summary` to file
    ///
    /// # Errors
    ///
    /// Returns IO error in case it occurs
    pub async fn write_to_file(&mut self) -> Result<(), Error> {
        let file = SummaryFileNode::new(self.path.to_owned(), crate::fs::FileType::Summary)
            .await
            .unwrap();
        let serialized_data = self.serialize();
        file.node.write_all(&serialized_data).await?;
        Ok(())
    }

    /// Recovers `Summary` fields from summary file
    ///
    /// # Errors
    ///
    /// Returns IO error in case it occurs
    pub async fn recover(&mut self) -> Result<(), Error> {
        let (smallest_key, biggest_key) = SummaryFileNode::recover(self.path.to_owned()).await?;
        self.smallest_key = smallest_key;
        self.biggest_key = biggest_key;
        Ok(())
    }

    /// Serializes `Summary` to byte vector
    pub(crate) fn serialize(&self) -> ByteSerializedEntry {
        let entry_len = SIZE_OF_U32 + SIZE_OF_U32 + self.biggest_key.len() + self.smallest_key.len();
        let mut serialized_data = Vec::with_capacity(entry_len);

        serialized_data.extend_from_slice(&(self.smallest_key.len() as u32).to_le_bytes());

        serialized_data.extend_from_slice(&(self.biggest_key.len() as u32).to_le_bytes());

        serialized_data.extend_from_slice(&self.smallest_key);

        serialized_data.extend_from_slice(&self.biggest_key);

        serialized_data
    }
}
