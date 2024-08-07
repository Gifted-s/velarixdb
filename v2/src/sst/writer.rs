use super::block_index::writer::Writer as IndexWriter;
#[cfg(feature = "bloom")]
use crate::bloom::BloomFilter;
use crate::{
    bloom::BloomFilter,
    lsm_entry::ValueOffset,
    serde::Serializable,
    sst::{block::header::Header as BlockHeader, value_offset_block::ValueOffsetBlock},
    LSMEntry, SeqNo, UserKey,
};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
};

use super::meta::SegmentId;

pub struct Writer {
    pub opts: Options,

    segment_file_path: PathBuf,
    block_writer: BufWriter<File>,
    index_writer: IndexWriter,
    chunk: Vec<LSMEntry>,

    pub block_count: usize,
    pub item_count: usize,
    pub file_pos: u64,

    prev_pos: (u64, u64),

    /// Only takes user data into account
    pub file_size: u64,

    pub first_key: Option<UserKey>,
    pub last_key: Option<UserKey>,
    pub tombstone_count: usize,
    pub chunk_size: usize,

    pub lowest_seqno: SeqNo,
    pub highest_seqno: SeqNo,

    pub key_count: usize,
    current_key: Option<UserKey>,

    /// Hashes for bloom filter
    ///
    /// using enhanced double hashing, so we got two u64s
    #[cfg(feature = "bloom")]
    bloom_hash_buffer: Vec<(u64, u64)>,
}

pub struct Options {
    pub folder: PathBuf,
    pub evict_tombstones: bool,
    pub block_size: u32,

    pub segment_id: SegmentId,

    #[cfg(feature = "bloom")]
    pub bloom_fp_rate: f32,
}

impl Writer {
    pub fn new(opts: Options) -> crate::Result<Self> {
        let segment_file_path = opts.folder.join(opts.segment_id.to_string());

        let file = File::create(&segment_file_path)?;
        let block_writer = BufWriter::with_capacity(u16::MAX.into(), file);

        let index_writer = IndexWriter::new(opts.segment_id, &opts.folder, opts.block_size)?;

        let chunk = Vec::with_capacity(10_000);

        Ok(Self {
            opts,
            segment_file_path,
            block_writer,
            index_writer,
            chunk,

            block_count: 0,
            item_count: 0,
            file_pos: 0,

            prev_pos: (0, 0),

            file_size: 0,

            first_key: None,
            last_key: None,
            tombstone_count: 0,
            chunk_size: 0,

            lowest_seqno: SeqNo::MAX,
            highest_seqno: 0,

            key_count: 0,
            current_key: None,

            #[cfg(feature = "bloom")]
            bloom_hash_buffer: Vec::with_capacity(10_000),
        })
    }

    /// Writes a block to disk
    ///
    /// This is triggered when a `Writer::write` causes the buffer to grow to the configured `block_size`
    pub(crate) fn write_block(&mut self) -> crate::Result<()> {
        debug_assert!(self.chunk.is_empty());

        let size = self.chunk.iter().map(|item| item.size() as u64).sum::<u64>();

        self.file_size += size;

        let (header, data) = ValueOffsetBlock::to_bytes(&self.chunk, self.prev_pos.0)?;

        header.serialize(&mut self.block_writer)?;
        self.block_writer.write_all(&data)?;

        let bytes_written = (BlockHeader::serialized_len() + data.len()) as u64;

        //NOTE: Expect is fine because the chunk is not empty
        let last = self.chunk.last().expect("chunk should not be empty");

        self.index_writer
            .register_block(last.key.to_owned(), self.file_pos)?;

        // Adjust metadata
        self.file_pos += bytes_written;
        self.item_count += self.chunk.len();
        self.block_count += 1;

        self.prev_pos.0 = self.prev_pos.1;
        self.prev_pos.1 += bytes_written;

        self.chunk.clear();

        Ok(())
    }

    /// Writes an item
    ///
    /// #Note
    ///
    /// item must be sorted otherwise this will result in unexpected behaviour
    pub fn write(&mut self, item: LSMEntry) -> crate::Result<()> {
        if item.is_tombstone() {
            if self.opts.evict_tombstones {
                return Ok(());
            }
            self.tombstone_count += 1;
        }

        if Some(&item.key) != self.current_key.as_ref() {
            self.key_count += 1;
            self.current_key = Some(item.key.clone());

            // NOTE: Do not buffer every item's key since
            // there may be multiple versions of thesame key
            #[cfg(feature = "bloom")]
            self.bloom_hash_buffer.push(BloomFilter::get_hash(item.key));
        }

        let item_key = item.key.clone();
        let seqno = item.seqno;

        self.chunk_size += item.size();
        self.chunk.push(item);

        if self.chunk_size >= self.opts.block_size as usize {
            self.write_block()?;
            self.chunk_size = 0;
        }

        if self.first_key.is_none() {
            self.first_key = Some(item_key.clone());
        }

        self.last_key = Some(item_key);

        if self.lowest_seqno > seqno {
            self.lowest_seqno = seqno;
        }

        if self.highest_seqno < seqno {
            self.highest_seqno = seqno;
        }

        Ok(())
    }

    // Finishes the segment, making sure all data is written durably
    // pub fn finish(&mut self) -> crate::Result<Optio>>
}
