use crate::memtable::SkipMapValue;
use crate::{
    db::DataStore,
    err::Error,
    types::{Key, Value},
    util,
};
use crossbeam_skiplist::SkipMap;
use futures::future::join_all;
use std::{collections::HashMap, sync::Arc};
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
