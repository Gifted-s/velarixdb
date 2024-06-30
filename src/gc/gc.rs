// NOTE: GarbageCollector is only supported on Linux based OS for now because File Systems for other OS does not
// support the FILE_PUNCH_HOLE command which is crucial for reclaiming unused spaces on the disk

extern crate libc;
extern crate nix;
use crate::consts::{TAIL_ENTRY_KEY, TOMB_STONE_MARKER};
use crate::filter::BloomFilter;
use crate::fs::{FileAsync, FileNode};
use crate::index::Index;
use crate::memtable::{Entry, MemTable, SkipMapValue, K};
use crate::types::{BloomFilterHandle, CreatedAt, ImmutableMemTable, Key, KeyRangeHandle, ValOffset, Value};
use crate::vlog::{ValueLog, ValueLogEntry};
use crate::{err, helpers};
use crate::err::Error;
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use err::Error::*;
use futures::future::join_all;
use nix::libc::{c_int, off_t};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::sleep;

extern "C" {
    fn fallocate(fd: libc::c_int, mode: c_int, offset: off_t, len: off_t) -> c_int;
}

const FALLOC_FL_PUNCH_HOLE: c_int = 0x2;
const FALLOC_FL_KEEP_SIZE: c_int = 0x1;

/// thread-safe memtable type for garbage collector
type GCTable = Arc<RwLock<MemTable<Key>>>;

/// thread-safe log for garbage collector
type GCLog = Arc<RwLock<ValueLog>>;

/// thread-safe valid entries to re-insert
type ValidEntries = Arc<RwLock<Vec<(Key, Value, ValOffset)>>>;

/// thread-safe invalid entries to remove
type InvalidEntries = Arc<RwLock<Vec<ValueLogEntry>>>;

/// thread-safe valid etries synced to disk
type SyncedEntries = Arc<RwLock<Vec<(Key, Value, ValOffset)>>>;

/// thread-safe entries map keeping track of valid entries not
/// yet inserted to main store active memtable
type GCUpdatedEntries<K> = Arc<RwLock<SkipMap<K, SkipMapValue<ValOffset>>>>;

#[derive(Debug)]
pub struct GC {
    pub table: GCTable,
    pub vlog: GCLog,
    pub config: Config,
}
#[derive(Clone, Debug)]
pub struct Config {
    pub online_gc_interval: std::time::Duration,
    pub gc_chunk_size: usize,
}

impl GC {
    pub fn new(online_gc_interval: std::time::Duration, gc_chunk_size: usize, table: GCTable, vlog: GCLog) -> Self {
        Self {
            table,
            vlog,
            config: Config {
                online_gc_interval,
                gc_chunk_size,
            },
        }
    }
    pub fn start_background_gc_task(
        &self,
        filters: BloomFilterHandle,
        key_range: KeyRangeHandle,
        read_only_memtables: ImmutableMemTable<Key>,
        gc_updated_entries: GCUpdatedEntries<Key>,
    ) {
        let cfg = self.config.to_owned();
        let memtable = self.table.clone();
        let vlog = self.vlog.clone();
        let table_ref = Arc::clone(&memtable);
        let vlog_ref = Arc::clone(&vlog);
        let filters_ref = Arc::clone(&filters);
        let key_range_ref = Arc::clone(&key_range);
        let read_only_memtables_ref = Arc::clone(&read_only_memtables);
        let gc_updated_entries_ref = Arc::clone(&gc_updated_entries);
        tokio::spawn(async move {
            loop {
                sleep_gc_task(cfg.online_gc_interval).await;
                let res = GC::gc_handler(
                    &cfg,
                    Arc::clone(&table_ref),
                    Arc::clone(&vlog_ref),
                    Arc::clone(&filters_ref),
                    Arc::clone(&key_range_ref),
                    Arc::clone(&read_only_memtables_ref),
                    Arc::clone(&gc_updated_entries_ref),
                )
                .await;
                match res {
                    Ok(_) => {
                        log::info!("GC successful, tail shifted {}", vlog.read().await.tail_offset)
                    }
                    Err(err) => {
                        log::error!("{}", GCError(err.to_string()))
                    }
                }
            }
        });
    }

    pub async fn gc_handler(
        cfg: &Config,
        memtable: GCTable,
        vlog: GCLog,
        filters: BloomFilterHandle,
        key_range: KeyRangeHandle,
        read_only_memtables: ImmutableMemTable<Key>,
        gc_updated_entries: GCUpdatedEntries<Key>,
    ) -> Result<(), Error> {
        let invalid_entries = Arc::new(RwLock::new(Vec::new()));
        let valid_entries = Arc::new(RwLock::new(Vec::new()));
        let synced_entries = Arc::new(RwLock::new(Vec::new()));
        let vlog_reader = vlog.read().await;
        let punch_hole_start_offset = vlog_reader.tail_offset.to_owned();
        let chunk_res = vlog_reader.read_chunk_to_garbage_collect(cfg.gc_chunk_size).await;
        drop(vlog_reader);
        Ok(match chunk_res {
            Ok((entries, total_bytes_read)) => {
                let tasks = entries.into_iter().map(|entry| {
                    let invalid_entries_ref = Arc::clone(&invalid_entries);
                    let valid_entries_ref = Arc::clone(&valid_entries);
                    let table_ref = Arc::clone(&memtable);
                    let vlog_ref = Arc::clone(&vlog);
                    let filters_ref = Arc::clone(&filters);
                    let key_range_ref = Arc::clone(&key_range);
                    let read_only_memtables_ref = Arc::clone(&read_only_memtables);

                    tokio::spawn(async move {
                        let most_recent_value = GC::get(
                            std::str::from_utf8(&entry.key).unwrap(),
                            Arc::clone(&table_ref),
                            Arc::clone(&filters_ref),
                            Arc::clone(&key_range_ref),
                            Arc::clone(&vlog_ref),
                            Arc::clone(&read_only_memtables_ref),
                        )
                        .await;
                        match most_recent_value {
                            Ok((value, creation_time)) => {
                                if entry.created_at != creation_time || value == TOMB_STONE_MARKER.as_bytes().to_vec() {
                                    invalid_entries_ref.write().await.push(entry);
                                } else {
                                    valid_entries_ref.write().await.push((entry.key, value));
                                }
                                Ok(())
                            }
                            Err(err) => GC::handle_deleted_entries(invalid_entries_ref, entry, err).await,
                        }
                    })
                });

                let all_results = join_all(tasks).await;
                for res in all_results {
                    match res {
                        Ok(entry) => {
                            if let Err(err) = entry {
                                return Err(GCError(err.to_string()));
                            }
                        }
                        Err(err) => {
                            return Err(GCError(err.to_string()));
                        }
                    }
                }
                let new_tail_offset = vlog.read().await.tail_offset + total_bytes_read;
                let append_res = GC::update_tail(Arc::clone(&vlog), new_tail_offset).await;
                match append_res {
                    Ok(v_offset) => {
                        synced_entries.write().await.push((
                            TAIL_ENTRY_KEY.to_vec(),
                            new_tail_offset.to_le_bytes().to_vec(),
                            v_offset,
                        ));
                        if let Err(err) =
                            GC::write_valid_entries_to_vlog(valid_entries, synced_entries.to_owned(), Arc::clone(&vlog))
                                .await
                        {
                            return Err(GCError(err.to_string()));
                        }
                        // call fsync on vlog to guarantee persistence to disk
                        let sync_res = vlog.write().await.sync_to_disk().await;
                        match sync_res {
                            Ok(_) => {
                                vlog.write().await.set_tail(new_tail_offset);
                                if let Err(err) = GC::write_valid_entries_to_store(
                                    synced_entries.to_owned(),
                                    Arc::clone(&memtable),
                                    gc_updated_entries,
                                    Arc::clone(&vlog),
                                )
                                .await
                                {
                                    return Err(GCError(err.to_string()));
                                }

                                if let Err(err) = GC::remove_unsed(
                                    Arc::clone(&vlog),
                                    invalid_entries,
                                    punch_hole_start_offset,
                                    total_bytes_read,
                                )
                                .await
                                {
                                    return Err(GCError(err.to_string()));
                                };
                            }
                            Err(err) => return Err(GCError(err.to_string())),
                        }
                    }
                    Err(err) => return Err(GCError(err.to_string())),
                }
            }
            Err(err) => return Err(GCError(err.to_string())),
        })
    }

    pub async fn update_tail(vlog: GCLog, new_tail_offset: usize) -> Result<usize, Error> {
        vlog.write()
            .await
            .append(
                &TAIL_ENTRY_KEY.to_vec(),
                &new_tail_offset.to_le_bytes().to_vec(),
                Utc::now(),
                false,
            )
            .await
    }

    pub async fn write_valid_entries_to_store(
        valid_entries: ValidEntries,
        table: GCTable,
        gc_updated_entries: GCUpdatedEntries<Key>,
        vlog: GCLog,
    ) -> Result<(), Error> {
        gc_updated_entries.write().await.clear();
        for (key, value, existing_v_offset) in valid_entries.to_owned().read().await.iter() {
            if let Err(err) = GC::put(
                key,
                value,
                *existing_v_offset,
                Arc::clone(&table),
                gc_updated_entries.clone(),
            )
            .await
            {
                return Err(err);
            };
            if existing_v_offset > &vlog.read().await.head_offset {
                vlog.write().await.set_head(*existing_v_offset)
            }
        }
        Ok(())
    }

    pub async fn write_valid_entries_to_vlog(
        valid_entries: Arc<RwLock<Vec<(Key, Value)>>>,
        synced_entries: SyncedEntries,
        vlog: GCLog,
    ) -> Result<(), Error> {
        for (key, value) in valid_entries.to_owned().read().await.iter() {
            let append_res = vlog.write().await.append(&key, &value, Utc::now(), false).await;

            match append_res {
                Ok(v_offset) => {
                    synced_entries
                        .write()
                        .await
                        .push((key.to_owned(), value.to_owned(), v_offset));
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    #[allow(unused_variables)]
    pub async fn remove_unsed(
        vlog: GCLog,
        invalid_entries: InvalidEntries,
        punch_hole_start_offset: usize,
        punch_hole_length: usize,
    ) -> std::result::Result<(), Error> {
        let vlog_path = vlog.read().await.content.file.node.file_path.to_owned();
        #[cfg(target_os = "linux")]
        {
            GC::punch_holes(vlog_path, punch_hole_start_offset as i64, punch_hole_length as i64).await
        }
        #[cfg(not(target_os = "linux"))]
        {
            return Err(Error::GCErrorUnsupportedPlatform(String::from(
                "File system does not support file punch hole",
            )));
        }
    }

    pub async fn punch_holes<P: AsRef<Path>>(
        file_path: P,
        offset: off_t,
        length: off_t,
    ) -> std::result::Result<(), Error> {
        let file = FileNode::open(file_path.as_ref()).await?;
        let fd = file.as_raw_fd();
        unsafe {
            let result = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, length);
            // 0 return means the punch was successful
            if result == 0 {
                Ok(())
            } else {
                Err(Error::GCErrorFailedToPunchHoleInVlogFile(
                    std::io::Error::last_os_error(),
                ))
            }
        }
    }

    pub async fn put<T: AsRef<[u8]>>(
        key: T,
        value: T,
        val_offset: ValOffset,
        memtable: GCTable,
        gc_updated_entries: GCUpdatedEntries<Key>,
    ) -> Result<bool, Error> {
        let is_tombstone = value.as_ref().len() == 0;
        let created_at = Utc::now();
        let v_offset = val_offset;
        let entry = Entry::new(key.as_ref(), v_offset, created_at, is_tombstone);
        memtable.write().await.insert(&entry)?;
        gc_updated_entries.write().await.insert(
            key.as_ref().to_vec(),
            SkipMapValue::new(v_offset, created_at, is_tombstone),
        );
        Ok(true)
    }

    pub async fn get<CustomKey: K>(
        key: CustomKey,
        memtable: GCTable,
        filters: BloomFilterHandle,
        key_range: KeyRangeHandle,
        vlog: Arc<RwLock<ValueLog>>,
        read_only_memtables: ImmutableMemTable<Key>,
    ) -> Result<(Value, CreatedAt), Error> {
        let key = key.as_ref().to_vec();
        let mut offset = 0;
        let lowest_possible_date = helpers::default_datetime();
        let mut most_recent_insert_time = helpers::default_datetime();
        // Step 1: Check the active memtable
        if let Some(value) = memtable.read().await.get(&key) {
            if value.is_tombstone {
                return Err(NotFoundInDB);
            }
            return GC::get_value_from_vlog(vlog, value.val_offset, value.created_at).await;
        } else {
            // Step 2: Check the read-only memtables
            let mut is_deleted = false;
            for (_, table) in read_only_memtables.read().await.iter() {
                if let Some(value) = table.read().await.get(&key) {
                    if value.created_at > most_recent_insert_time {
                        offset = value.val_offset;
                        most_recent_insert_time = value.created_at;
                        is_deleted = value.is_tombstone
                    }
                }
            }
            if most_recent_insert_time > lowest_possible_date {
                if is_deleted {
                    return Err(NotFoundInDB);
                }
                return GC::get_value_from_vlog(vlog, offset, most_recent_insert_time).await;
            } else {
                // Step 3: Check sstables
                let key_range = &key_range.read().await;
                let ssts = key_range.filter_sstables_by_biggest_key(&key).await?;
                for sst in ssts.iter() {
                    let index = Index::new(sst.index_file.path.to_owned(), sst.index_file.file.to_owned());
                    let block_handle = index.get(&key).await;
                    match block_handle {
                        Ok(None) => continue,
                        Ok(result) => {
                            if let Some(block_offset) = result {
                                let sst_res = sst.get(block_offset, &key).await;
                                match sst_res {
                                    Ok(None) => continue,
                                    Ok(result) => {
                                        if let Some((val_offset, created_at, is_tombstone)) = result {
                                            if created_at > most_recent_insert_time {
                                                offset = val_offset;
                                                most_recent_insert_time = created_at;
                                                is_deleted = is_tombstone;
                                            }
                                        }
                                    }
                                    Err(err) => log::error!("{}", err),
                                }
                            }
                        }
                        Err(err) => log::error!("{}", err),
                    }
                }
                if most_recent_insert_time > lowest_possible_date {
                    if is_deleted {
                        return Err(NotFoundInDB);
                    }
                    // Step 5: Read value from value log based on offset
                    return GC::get_value_from_vlog(vlog, offset, most_recent_insert_time).await;
                }
            }
        }
        Err(NotFoundInDB)
    }

    pub async fn get_value_from_vlog(
        val_log: Arc<RwLock<ValueLog>>,
        offset: usize,
        creation_at: CreatedAt,
    ) -> Result<(Value, CreatedAt), Error> {
        let res = val_log.read().await.get(offset).await?;
        match res {
            Some((value, is_tombstone)) => {
                if is_tombstone {
                    return Err(KeyFoundAsTombstoneInValueLogError);
                }
                return Ok((value, creation_at));
            }
            None => return Err(KeyNotFoundInValueLogError),
        };
    }

    pub async fn handle_deleted_entries(
        invalid_entries_ref: Arc<RwLock<Vec<ValueLogEntry>>>,
        entry: ValueLogEntry,
        err: Error,
    ) -> std::result::Result<(), Error> {
        match err {
            KeyFoundAsTombstoneInMemtableError
            | KeyNotFoundInAnySSTableError
            | KeyNotFoundByAnyBloomFilterError
            | KeyFoundAsTombstoneInSSTableError
            | KeyFoundAsTombstoneInValueLogError
            | err::Error::KeyNotFoundInValueLogError
            | NotFoundInDB => {
                invalid_entries_ref.write().await.push(entry);
                Ok(())
            }
            _ => return Err(err),
        }
    }
}

async fn sleep_gc_task(duration: std::time::Duration) {
    sleep(duration).await;
}
