use std::{collections::HashMap, sync::Arc};

use crate::tests::workload::Error::TokioJoinError;
use crate::{
    err::Error,
    helpers,
    storage::DataStore,
    types::{Key, Value},
};
use futures::future::join_all;
use rand::{distributions::Alphanumeric, Rng};
use tokio::sync::RwLock;
#[derive(Clone, Debug)]
pub struct Entry {
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

type WriteWorkloadMap = HashMap<Key, Value>;

type ReadWorkloadMap = HashMap<Key, Value>;

type ReadWorkloadVec = Vec<Entry>;

type WriteWorkloadVec = Vec<Entry>;

pub fn generate_workload_data_as_map(
    size: usize,
    key_len: usize,
    val_len: usize,
    write_read_ratio: f64,
) -> (ReadWorkloadMap, WriteWorkloadMap) {
    let mut write_workload = HashMap::with_capacity(size);
    let mut read_workload = HashMap::with_capacity((size as f64 * write_read_ratio) as usize);
    for _ in 0..size {
        let key = helpers::generate_random_id(key_len);
        let val = helpers::generate_random_id(val_len);
        write_workload.insert(key.as_bytes().to_vec(), val.as_bytes().to_vec());
    }

    let read_workload_size = (size as f64 * write_read_ratio) as usize;
    read_workload.extend(
        write_workload
            .iter()
            .take(read_workload_size)
            .map(|(key, value)| (key.to_vec(), value.to_vec())),
    );
    (read_workload, write_workload)
}

pub fn generate_workload_data_as_vec(
    size: usize,
    key_len: usize,
    val_len: usize,
    write_read_ratio: f64,
) -> (ReadWorkloadVec, WriteWorkloadVec) {
    let mut write_workload = Vec::with_capacity(size);
    let mut read_workload = Vec::with_capacity((size as f64 * write_read_ratio) as usize);
    for _ in 0..size {
        let key = helpers::generate_random_id(key_len);
        let val = helpers::generate_random_id(val_len);
        let entry = Entry {
            key: key.as_bytes().to_vec(),
            val: val.as_bytes().to_vec(),
        };
        write_workload.push(entry);
    }
    let read_workload_size = (size as f64 * write_read_ratio) as usize;
    read_workload.extend(write_workload.iter().take(read_workload_size).map(|e| Entry {
        key: e.key.to_owned(),
        val: e.val.to_owned(),
    }));

    (read_workload, write_workload)
}

pub async fn insert_parallel(
    entries: &Vec<Entry>,
    store: Arc<RwLock<DataStore<'static, Vec<u8>>>>,
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
                if let Err(err) = entry {
                    return Err(err);
                }
            }
            Err(_) => return Err(TokioJoinError),
        }
    }
    return Ok(());
}

pub async fn delete_parallel(
    entries: &'static Vec<Entry>,
    store: Arc<RwLock<DataStore<'static, Vec<u8>>>>,
) -> Result<(), Error> {
    let tasks = entries.iter().map(|e| {
        let s_engine = Arc::clone(&store);
        tokio::spawn(async move {
            let mut value = s_engine.write().await;
            value.delete(std::str::from_utf8(&e.key).unwrap()).await
        })
    });

    let all_results = join_all(tasks).await;
    for tokio_response in all_results {
        match tokio_response {
            Ok(entry) => match entry {
                Ok(_) => {}
                Err(err) => return Err(err),
            },
            Err(_) => return Err(TokioJoinError),
        }
    }
    return Ok(());
}
