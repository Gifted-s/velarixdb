use std::{io, path::PathBuf};
use chrono::{DateTime, Utc};

use crate::bloom_filter::BloomFilter;

pub struct SSTable {
  fine_path: PathBuf,
  index: Vec<(Vec<u8>, usize)>,
  bloom_filter: BloomFilter,
  created_at: DateTime<Utc>
}


