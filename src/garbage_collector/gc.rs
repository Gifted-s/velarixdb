// NOTE: GC is only supported on Linux based OS for now because File Systems for other OS does not
// support the FILE_PUNCH_HOLE command which is crucial for reclaiming unused spaces on the disk

extern crate libc;
extern crate nix;

use std::io::Error;
use std::os::unix::io::AsRawFd;

use nix::libc::{c_int, off_t};

use chrono::Utc;
use log::{error, info};
use tokio::sync::RwLock;

use crate::consts::{GC_CHUNK_SIZE, TAIL_ENTRY_KEY, TOMB_STONE_MARKER};

use crate::err;
use crate::value_log::ValueLogEntry;
use crate::{err::StorageEngineError, storage_engine::*};
use std::str;

use std::sync::Arc;

pub(crate) type K = Vec<u8>;
pub(crate) type V = Vec<u8>;
pub(crate) type VOffset = usize;

extern "C" {
    fn fallocate(fd: libc::c_int, mode: c_int, offset: off_t, len: off_t) -> c_int;
}

const FALLOC_FL_PUNCH_HOLE: c_int = 0x2;
const FALLOC_FL_KEEP_SIZE: c_int = 0x1;

pub struct GarbageCollector {}
// will be bringing in tokio library
impl GarbageCollector {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn run(
        &self,
        engine: ExRw<StorageEngine<K>>,
    ) -> std::result::Result<(), StorageEngineError> {
        let invalid_entries: ExRw<Vec<ValueLogEntry>> = Arc::new(RwLock::new(Vec::new()));
        let valid_entries: ExRw<Vec<(K, V)>> = Arc::new(RwLock::new(Vec::new()));
        let synced_entries: ExRw<Vec<(K, V, VOffset)>> = Arc::new(RwLock::new(Vec::new()));
        // let valid_entries = Vec::new();
        // Step 1: Read chunks to garbage collect
        // TODO handle errors
        let store = engine.read().await;
        let punch_hole_start_offset = store.val_log.tail_offset;
        let (entries, total_bytes_read) = store
            .val_log
            .read_chunk_to_garbage_collect(GC_CHUNK_SIZE)
            .await?;

        let tasks = entries.into_iter().map(|entry| {
            let s_engine = Arc::clone(&engine);
            let invalid_entries_clone = Arc::clone(&invalid_entries);
            let valid_entries_clone = Arc::clone(&valid_entries);
            tokio::spawn(async move {
                let value = s_engine.read().await;
                let most_recent_value = value.get(str::from_utf8(&entry.key).unwrap()).await;
                match most_recent_value {
                    Ok((value, creation_time)) => {
                        if entry.created_at != creation_time
                            || value == TOMB_STONE_MARKER.to_le_bytes().to_vec()
                        {
                            invalid_entries_clone.write().await.push(entry);
                        } else {
                            valid_entries_clone.write().await.push((entry.key, value));
                        }
                    }
                    Err(err) => match err {
                        err::StorageEngineError::KeyFoundAsTombstoneInMemtableError => {
                            invalid_entries_clone.write().await.push(entry);
                        }
                        err::StorageEngineError::KeyNotFoundInAnySSTableError => {
                            invalid_entries_clone.write().await.push(entry);
                        }
                        err::StorageEngineError::KeyNotFoundByAnyBloomFilterError => {
                            invalid_entries_clone.write().await.push(entry);
                        }
                        err::StorageEngineError::KeyFoundAsTombstoneInSSTableError => {
                            invalid_entries_clone.write().await.push(entry);
                        }
                        err::StorageEngineError::KeyFoundAsTombstoneInValueLogError => {
                            invalid_entries_clone.write().await.push(entry);
                        }
                        err::StorageEngineError::KeyNotFoundInValueLogError => {
                            invalid_entries_clone.write().await.push(entry);
                        }
                        err::StorageEngineError::NotFoundInDB => {
                            invalid_entries_clone.write().await.push(entry);
                        }
                        _ => {
                            error!(
                                "Error fetching key {:?} {:?}",
                                str::from_utf8(&entry.key).unwrap(),
                                err
                            )
                        }
                    },
                }
            })
        });

        for task in tasks {
            tokio::select! {
                result = task => {
                    match result {
                        Ok(_) => {
                            info!("Valid entries fetch complete")
                        }
                        Err(err) => {
                            error!("Error running task {}", err)

                        }
                    }
                }
            }
        }

        let new_tail_offset = engine.read().await.val_log.tail_offset + total_bytes_read;

        let v_offset = engine
            .write()
            .await
            .val_log
            .append(
                &TAIL_ENTRY_KEY.to_vec(),
                &new_tail_offset.to_le_bytes().to_vec(),
                Utc::now().timestamp_millis() as u64,
                false,
            )
            .await?;
        synced_entries.write().await.push((
            TAIL_ENTRY_KEY.to_vec(),
            new_tail_offset.to_le_bytes().to_vec(),
            v_offset,
        ));

        for (key, value) in valid_entries.to_owned().read().await.iter() {
            let mut store = engine.write().await;
            let v_offset = store
                .val_log
                .append(&key, &value, Utc::now().timestamp_millis() as u64, false)
                .await?;
            synced_entries
                .write()
                .await
                .push((key.to_owned(), value.to_owned(), v_offset));
        }

        // call fsync on vLog
        let engine_r_lock = engine.read().await;
        let v_log = tokio::fs::File::open(engine_r_lock.val_log.file_path.to_owned())
            .await
            .map_err(|err| StorageEngineError::ValueLogFileReadError { error: err })?;
        v_log
            .sync_all()
            .await
            .map_err(|err| StorageEngineError::ValueLogFileSyncError { error: err })?;
        engine.write().await.val_log.set_tail(new_tail_offset);
        for (key, value, existing_v_offset) in synced_entries.to_owned().read().await.iter() {
            let _ = engine
                .write()
                .await
                .put(
                    str::from_utf8(&key).unwrap(),
                    str::from_utf8(&value).unwrap(),
                    Some(*existing_v_offset),
                )
                .await;
        }

        self.garbage_collect(
            Arc::clone(&engine),
            invalid_entries,
            punch_hole_start_offset,
            total_bytes_read,
        )
        .await?;

        Ok(())
    }

    pub async fn garbage_collect(
        &self,
        engine: ExRw<StorageEngine<K>>,
        invalid_entries: ExRw<Vec<ValueLogEntry>>,
        punch_hole_start_offset: usize,
        punch_hole_length: usize,
    ) -> std::result::Result<(), StorageEngineError> {
        #[cfg(target_os = "linux")]
        {
            let eng_read_lock = engine.read().await;
            let garbage_collected = self
                .punch_holes(
                    eng_read_lock.val_log.file_path.to_str().unwrap(),
                    punch_hole_start_offset as i64,
                    punch_hole_length as i64,
                )
                .await?;
            Ok(())
        }

        #[cfg(not(target_os = "linux"))]
        {
            return Err(StorageEngineError::GCErrorUnsupportedPlatform(
                String::from("File system does not support file punch hole"),
            ));
        }
    }

    pub async fn punch_holes(
        &self,
        file_path: &str,
        offset: off_t,
        length: off_t,
    ) -> std::result::Result<(), StorageEngineError> {
        let file = tokio::fs::File::open(file_path)
            .await
            .map_err(|err| StorageEngineError::ValueLogFileReadError { error: err })?;

        let fd = file.as_raw_fd();

        unsafe {
            let result = fallocate(
                fd,
                FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                offset,
                length,
            );

            if result == 0 {
                Ok(())
            } else {
                Err(StorageEngineError::GCErrorFailedToPunchHoleInVlogFile(
                    Error::last_os_error(),
                ))
            }
        }
    }
}
