use std::{path::PathBuf, sync::Arc};

use crossbeam_skiplist::SkipMap;
use tokio::{fs::File, sync::RwLock};

use crate::{
    fs::{DataFileNode, FileNode, FileType, IndexFileNode},
    index::IndexFile,
    sst::{DataFile, Table},
};

struct SSTContructor {
    dir: PathBuf,
    data_path: PathBuf,
    index_path: PathBuf,
}
impl SSTContructor {
    fn new(dir: PathBuf, data_path: PathBuf, index_path: PathBuf) -> Self {
        return Self {
            dir,
            data_path,
            index_path,
        };
    }
}

pub async fn generate_ssts(number: u32) -> Vec<Table> {
    let sst_contructor: Vec<SSTContructor> = vec![
        SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830953696"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830953696/data_1718830953697_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830953696/index_1718830953697_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830954572"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830954572/data_1718830954572_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830954572/index_1718830954572_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830955463"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830955463/data_1718830955463_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830955463/index_1718830955463_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830956313"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830956313/data_1718830956313_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830956313/index_1718830956313_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830957169"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830957169/data_1718830957169_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830957169/index_1718830957169_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958810"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958810/data_1718830958810_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958810/index_1718830958810_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958858"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958858/data_1718830958858_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958858/index_1718830958858_.db",
        ),
    ),
    SSTContructor::new(
        PathBuf::from("src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958906"),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958906/data_1718830958906_.db",
        ),
        PathBuf::from(
            "src/tests/fixtures/data/buckets/bucket5af1d6ce-fca8-4b31-8380-c9b1612a9879/sstable_1718830958906/index_1718830958906_.db",
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
                        file_type: FileType::SSTable,
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
