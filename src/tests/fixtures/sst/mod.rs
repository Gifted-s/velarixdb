use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crossbeam_skiplist::SkipMap;
use tokio::{fs::File, sync::RwLock};

use crate::{
    fs::{DataFileNode, FileNode, FileType, IndexFileNode},
    index::IndexFile,
    sst::{DataFile, Table},
};

pub struct SSTContructor {
    dir: PathBuf,
    data_path: PathBuf,
    index_path: PathBuf,
}
impl SSTContructor {
    fn new<P: AsRef<Path> + Send + Sync>(dir: P, data_path: P, index_path: P) -> Self {
        return Self {
            dir: dir.as_ref().to_path_buf(),
            data_path: data_path.as_ref().to_path_buf(),
            index_path: index_path.as_ref().to_path_buf(),
        };
    }
}

pub async fn generate_ssts(number: u32) -> Vec<Table> {
    let sst_contructor: Vec<SSTContructor> = vec![
        SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830953696"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958016/data_1718830958016_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958016/index_1718830958016_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830954572"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958858/data_1718830958858_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958858/index_1718830958858_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830955463"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958906/data_1718830958906_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958906/index_1718830958906_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830956313"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958953/data_1718830958953_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958953/index_1718830958953_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830957169"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959000/data_1718830959000_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959000/index_1718830959000_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958810"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959049/data_1718830959049_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959049/index_1718830959049_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958858"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959097/data_1718830959097_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959097/index_1718830959097_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958906"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959145/data_1718830959145_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830959145/index_1718830959145_.db",
        ),
    ),
    ];
    let mut ssts = Vec::new();
    for i in 0..number {
        ssts.push(Table {
            dir: sst_contructor[i as usize].dir.to_owned(),
            hotness: 100,
            size: 4096,
            created_at: 1655580700,
            data_file: DataFile {
                file: DataFileNode {
                    node: FileNode {
                        file_path: sst_contructor[i as usize].data_path.to_owned(),
                        file: Arc::new(RwLock::new(
                            File::open(sst_contructor[i as usize].data_path.to_owned())
                                .await
                                .unwrap(),
                        )),
                        file_type: FileType::Data,
                    },
                },
                path: sst_contructor[i as usize].data_path.to_owned(),
            },
            index_file: IndexFile {
                file: IndexFileNode {
                    node: FileNode {
                        file_path: sst_contructor[i as usize].index_path.to_owned(),
                        file: Arc::new(RwLock::new(
                            File::open(sst_contructor[i as usize].index_path.to_owned())
                                .await
                                .unwrap(),
                        )),
                        file_type: FileType::Index,
                    },
                },
                path: sst_contructor[i as usize].index_path.to_owned(),
            },
            entries: Arc::new(SkipMap::default()),
        })
    }
    return ssts;
}
