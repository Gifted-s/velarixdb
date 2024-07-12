use crate::filter::BloomFilter;
use crate::memtable::SkipMapValue;
use crate::sst::{DataFile, Summary};
use crate::{
    db::DataStore,
    err::Error,
    types::{Key, Value},
    util,
};
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use futures::future::join_all;
use std::path::{Path, PathBuf};
use std::{collections::HashMap, sync::Arc};
use tokio::fs::File;
use tokio::sync::RwLock;

type WriteWorkloadMap = HashMap<Key, Value>;

type ReadWorkloadMap = HashMap<Key, Value>;

type ReadWorkloadVec = Vec<Entry>;

type WriteWorkloadVec = Vec<Entry>;

#[derive(Clone, Debug)]
pub struct Entry {
    pub key: Key,
    pub val: Value,
}

#[derive(Clone, Debug)]
pub struct Workload {
    pub size: usize,
    pub key_len: usize,
    pub val_len: usize,
    pub write_read_ratio: f64,
}

impl Workload {
    pub fn new(size: usize, key_len: usize, val_len: usize, write_read_ratio: f64) -> Self {
        Self {
            size,
            key_len,
            val_len,
            write_read_ratio,
        }
    }

    pub fn generate_workload_data_as_map(&self) -> (ReadWorkloadMap, WriteWorkloadMap) {
        let mut write_workload = HashMap::with_capacity(self.size);
        let mut read_workload = HashMap::with_capacity((self.size as f64 * self.write_read_ratio) as usize);
        for _ in 0..self.size {
            let key = util::generate_random_id(self.key_len);
            let val = util::generate_random_id(self.val_len);
            write_workload.insert(key.as_bytes().to_vec(), val.as_bytes().to_vec());
        }

        let read_workload_size = (self.size as f64 * self.write_read_ratio) as usize;
        read_workload.extend(
            write_workload
                .iter()
                .take(read_workload_size)
                .map(|(key, value)| (key.to_vec(), value.to_vec())),
        );
        (read_workload, write_workload)
    }

    pub fn generate_workload_data_as_vec(&self) -> (ReadWorkloadVec, WriteWorkloadVec) {
        let mut write_workload = Vec::with_capacity(self.size);
        let mut read_workload = Vec::with_capacity((self.size as f64 * self.write_read_ratio) as usize);
        for _ in 0..self.size {
            let key = util::generate_random_id(self.key_len);
            let val = util::generate_random_id(self.val_len);
            let entry = Entry {
                key: key.as_bytes().to_vec(),
                val: val.as_bytes().to_vec(),
            };
            write_workload.push(entry);
        }
        let read_workload_size = (self.size as f64 * self.write_read_ratio) as usize;
        read_workload.extend(write_workload.iter().take(read_workload_size).map(|e| Entry {
            key: e.key.to_owned(),
            val: e.val.to_owned(),
        }));

        (read_workload, write_workload)
    }

    pub async fn insert_parallel(
        &self,
        entries: &[Entry],
        store: Arc<RwLock<DataStore<'static, Key>>>,
    ) -> Result<(), Error> {
        let tasks = entries.iter().map(|e| {
            let s_engine = Arc::clone(&store);
            let key = e.key.clone();
            let val = e.val.clone();
            tokio::spawn(async move {
                let key_str = std::str::from_utf8(&key).unwrap();
                let val_str = std::str::from_utf8(&val).unwrap();
                let mut value = s_engine.write().await;
                value.put(key_str, val_str).await
            })
        });

        let all_results = join_all(tasks).await;
        for tokio_response in all_results {
            match tokio_response {
                Ok(entry) => {
                    entry?;
                }
                Err(_) => return Err(Error::TokioJoin),
            }
        }
        Ok(())
    }
}

pub struct FilterWorkload {}

impl FilterWorkload {
    pub fn from(false_pos: f64, entries: Arc<SkipMap<Vec<u8>, SkipMapValue<usize>>>) -> crate::filter::BloomFilter {
        let mut filter = crate::filter::BloomFilter::new(false_pos, entries.len());
        filter.build_filter_from_entries(&entries);
        filter
    }
}

use crate::{
    fs::{DataFileNode, FileNode, FileType, IndexFileNode},
    index::IndexFile,
    sst::Table,
};

pub struct SSTContructor {
    pub dir: PathBuf,
    pub data_path: PathBuf,
    pub index_path: PathBuf,
    pub filter_path: PathBuf,
    pub summary_path: PathBuf,
}

impl SSTContructor {
    fn new<P: AsRef<Path> + Send + Sync>(dir: P, data_path: P, index_path: P, filter_path: P, summary_path: P) -> Self {
        return Self {
            dir: dir.as_ref().to_path_buf(),
            data_path: data_path.as_ref().to_path_buf(),
            index_path: index_path.as_ref().to_path_buf(),
            filter_path: filter_path.as_ref().to_path_buf(),
            summary_path: summary_path.as_ref().to_path_buf(),
        };
    }

    pub async fn generate_ssts(number: u32) -> Vec<Table> {
        let sst_contructor: Vec<SSTContructor> = vec![
    SSTContructor::new(
    PathBuf::from("src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785462309"),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785462309/data.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785462309/index.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785462309/filter.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785462309/summary.db",
    ),
),
SSTContructor::new(
    PathBuf::from("src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463686"),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463686/data.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463686/index.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463686/filter.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463686/summary.db",
    ),
),
SSTContructor::new(
    PathBuf::from("src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463735"),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463735/data.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463735/index.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463735/filter.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463735/summary.db",
    ),
),
SSTContructor::new(
    PathBuf::from("src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463779"),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463779/data.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463779/index.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463779/filter.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463779/summary.db",
    ),
),
SSTContructor::new(
    PathBuf::from("src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463825"),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463825/data.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463825/index.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463825/filter.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463825/summary.db",
    ),
),
SSTContructor::new(
    PathBuf::from("src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463872"),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463872/data.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463872/index.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463872/filter.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463872/summary.db",
    ),
),
SSTContructor::new(
    PathBuf::from("src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463919"),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463919/data.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463919/index.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463919/filter.db",
    ),
    PathBuf::from(
        "src/tests/fixtures/data/buckets/bucket1201eb6b-8903-4557-a5bd-d87cb725f1d8/sstable_1720785463919/summary.db",
    ),
)
];
        let mut ssts = Vec::new();
        for i in 0..number {
            let idx = i as usize;
            ssts.push(Table {
                dir: sst_contructor[idx].dir.to_owned(),
                hotness: 100,
                size: 4096,
                created_at: Utc::now(),
                data_file: DataFile {
                    file: DataFileNode {
                        node: FileNode {
                            file_path: sst_contructor[idx].data_path.to_owned(),
                            file: Arc::new(RwLock::new(
                                File::open(sst_contructor[idx].data_path.to_owned()).await.unwrap(),
                            )),
                            file_type: FileType::Data,
                        },
                    },
                    path: sst_contructor[idx].data_path.to_owned(),
                },
                index_file: IndexFile {
                    file: IndexFileNode {
                        node: FileNode {
                            file_path: sst_contructor[idx].index_path.to_owned(),
                            file: Arc::new(RwLock::new(
                                File::open(sst_contructor[idx].index_path.to_owned()).await.unwrap(),
                            )),
                            file_type: FileType::Index,
                        },
                    },
                    path: sst_contructor[idx].index_path.to_owned(),
                },
                entries: Arc::new(SkipMap::default()),
                filter: Some(BloomFilter {
                    file_path: Some(sst_contructor[idx].filter_path.to_owned()),
                    ..Default::default()
                }),
                summary: Some(Summary::new(sst_contructor[idx].summary_path.to_owned())),
            })
        }
        ssts
    }
}
