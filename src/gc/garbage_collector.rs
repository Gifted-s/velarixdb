// NOTE: GarbageCollector is only supported on Linux based OS for now because File Systems for other OS does not
// support the FILE_PUNCH_HOLE command which is crucial for reclaiming unused spaces on the disk

extern crate libc;
extern crate nix;
use crate::consts::{TAIL_ENTRY_KEY, TOMB_STONE_MARKER};
use crate::err::Error;
use crate::index::Index;
use crate::memtable::{Entry, MemTable, SkipMapValue, K};
use crate::sst::Table;
use crate::types::{CreatedAt, ImmutableMemTables, Key, KeyRangeHandle, ValOffset, Value};
use crate::vlog::{ValueLog, ValueLogEntry};
use crate::{err, util};
use chrono::Utc;
use crossbeam_skiplist::SkipMap;
use err::Error::*;
use futures::future::join_all;
use nix::libc::{c_int, off_t};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;

extern "C" {
    fn fallocate(fd: libc::c_int, mode: c_int, offset: off_t, len: off_t) -> c_int;
}

const FALLOC_FL_PUNCH_HOLE: c_int = 0x2;
const FALLOC_FL_KEEP_SIZE: c_int = 0x1;

/// Alias for thread-safe memtable type for garbage collector
type GCTable = Arc<RwLock<MemTable<Key>>>;

/// Alias for thread-safe log for garbage collector
type GCLog = Arc<RwLock<ValueLog>>;

/// Alias for thread-safe valid entries to re-insert
type ValidEntries = Arc<RwLock<Vec<(Key, Value, ValOffset)>>>;

/// Alias thread-safe valid etries synced to disk
type SyncedEntries = Arc<RwLock<Vec<(Key, Value, ValOffset)>>>;

/// Alias thread-safe entries map keeping track of valid entries not
/// yet inserted to main store active memtable
type GCUpdatedEntries<K> = Arc<RwLock<SkipMap<K, SkipMapValue<ValOffset>>>>;

// Alias for vlog head
type Tail = usize;

// Alias for log tail
type Head = usize;

/// Handles Garbage Collections
///
/// Responsible for fetching invalid entries and removing them from disk
#[derive(Debug)]
pub struct GC {
    /// A memtable specifically for GC, this table is kept
    /// in sync with major memtable
    pub(crate) table: GCTable,

    /// A value log specifically for GC
    pub(crate) vlog: GCLog,

    /// Configuration of GC
    pub(crate) config: Config,

    /// Valid entries are kept here before they are synced to
    /// main store memtable
    pub(crate) gc_updated_entries: GCUpdatedEntries<Key>,

    /// Keeps track of offsets to punch i.e remove
    pub(crate) punch_marker: Arc<Mutex<PunchMarker>>,
}

/// GC Configuration
#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub online_gc_interval: std::time::Duration,
    pub gc_chunk_size: usize,
}

/// Marks area of value log file
/// to be punched
#[derive(Clone, Debug, Default)]
pub struct PunchMarker {
    /// Offset in value log to start punching hole
    pub(crate) punch_hole_start_offset: usize,

    /// Length of holes to punch
    pub(crate) punch_hole_length: usize,
}

impl GC {
    /// Creates `GC` instance
    pub fn new(
        online_gc_interval: std::time::Duration,
        gc_chunk_size: usize,
        table: GCTable,
        vlog: GCLog,
        gc_updated_entries: GCUpdatedEntries<Key>,
    ) -> Self {
        Self {
            table,
            vlog,
            punch_marker: Arc::new(Mutex::new(PunchMarker::default())),
            gc_updated_entries,
            config: Config {
                online_gc_interval,
                gc_chunk_size,
            },
        }
    }

    /// Continues to check if it's time to run GC (works in background)
    pub fn start_gc_worker(&self, key_range: KeyRangeHandle, read_only_memtables: ImmutableMemTables<Key>) {
        let cfg = self.config.to_owned();
        let memtable = self.table.clone();
        let vlog = self.vlog.clone();
        let table_ref = Arc::clone(&memtable);
        let vlog_ref = Arc::clone(&vlog);
        let key_range_ref = Arc::clone(&key_range);
        let read_only_memtables_ref = Arc::clone(&read_only_memtables);
        let gc_updated_entries_ref = Arc::clone(&self.gc_updated_entries);
        let punch_marker_ref = Arc::clone(&self.punch_marker);
        tokio::spawn(async move {
            loop {
                sleep_gc_task(cfg.online_gc_interval).await;
                // if last valid entries is not synced with store memtable yet don't
                // run another garbage collection
                if !gc_updated_entries_ref.read().await.is_empty() {
                    continue;
                }
                let res = GC::gc_handler(
                    &cfg,
                    Arc::clone(&table_ref),
                    Arc::clone(&vlog_ref),
                    Arc::clone(&key_range_ref),
                    Arc::clone(&read_only_memtables_ref),
                    Arc::clone(&gc_updated_entries_ref),
                    Arc::clone(&punch_marker_ref),
                )
                .await;
                match res {
                    Ok(_) => {
                        log::info!("GC successful, awaiting sync")
                    }
                    Err(err) => {
                        log::error!("GC Error {}", err.to_string());
                    }
                }
            }
        });
    }

    /// Handles online garbage collection
    ///
    /// Fetche `gc_chunk_size` from value log, checks valid
    /// and invalid entries. Re-inserts valid entries while
    /// it filters out invalid entries
    ///
    /// # Error
    ///
    /// Returns error in case there was a failure at any point
    pub(crate) async fn gc_handler(
        cfg: &Config,
        memtable: GCTable,
        vlog: GCLog,
        key_range: KeyRangeHandle,
        read_only_memtables: ImmutableMemTables<Key>,
        gc_updated_entries: GCUpdatedEntries<Key>,
        punch_marker: Arc<Mutex<PunchMarker>>,
    ) -> Result<(), Error> {
        let invalid_entries = Arc::new(RwLock::new(Vec::new()));
        let valid_entries = Arc::new(RwLock::new(Vec::new()));
        let synced_entries = Arc::new(RwLock::new(Vec::new()));
        let vlog_reader = vlog.read().await;
        let chunk_res = vlog_reader.read_chunk_to_garbage_collect(cfg.gc_chunk_size).await;
        drop(vlog_reader);
        match chunk_res {
            Ok((entries, total_bytes_read)) => {
                let tasks = entries.into_iter().map(|entry| {
                    let invalid_entries_ref = Arc::clone(&invalid_entries);
                    let valid_entries_ref = Arc::clone(&valid_entries);
                    let table_ref = Arc::clone(&memtable);
                    let vlog_ref = Arc::clone(&vlog);
                    let key_range_ref = Arc::clone(&key_range);
                    let read_only_memtables_ref = Arc::clone(&read_only_memtables);

                    tokio::spawn(async move {
                        let most_recent_value = GC::get(
                            std::str::from_utf8(&entry.key).unwrap(),
                            Arc::clone(&table_ref),
                            Arc::clone(&key_range_ref),
                            Arc::clone(&vlog_ref),
                            Arc::clone(&read_only_memtables_ref),
                        )
                        .await;
                        match most_recent_value {
                            Ok((value, creation_time)) => {
                                if entry.created_at < creation_time || value == TOMB_STONE_MARKER.as_bytes().to_vec() {
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
                for tokio_res in all_results {
                    match tokio_res {
                        Ok(res) => res?,
                        Err(_) => {
                            return Err(TokioJoin);
                        }
                    }
                }
                // no entries to garbage collect, return early
                if invalid_entries.read().await.is_empty() {
                    return Ok(());
                }
                let new_tail_offset = vlog.read().await.tail_offset + total_bytes_read;
                let v_offset = GC::write_tail_to_disk(Arc::clone(&vlog), new_tail_offset).await;

                synced_entries.write().await.push((
                    TAIL_ENTRY_KEY.to_vec(),
                    new_tail_offset.to_le_bytes().to_vec(),
                    v_offset,
                ));

                GC::write_valid_entries_to_vlog(valid_entries, synced_entries.to_owned(), Arc::clone(&vlog)).await?;
                // call fsync on vlog to guarantee persistence to disk
                vlog.write().await.sync_to_disk().await?;

                GC::write_valid_entries_to_store(
                    synced_entries.to_owned(),
                    Arc::clone(&memtable),
                    gc_updated_entries,
                    Arc::clone(&vlog),
                )
                .await?;

                // Don't free space or update tail immediatley until store active memtable is
                // synced with gc table (handled seperately) but update punch hole marker
                let mut marker_lock = punch_marker.lock().await;
                marker_lock.punch_hole_start_offset = vlog.read().await.tail_offset;
                marker_lock.punch_hole_length = total_bytes_read;
            }
            Err(err) => return Err(err),
        };
        Ok(())
    }

    /// Inserts tail entry to value log
    pub(crate) async fn write_tail_to_disk(vlog: GCLog, new_tail_offset: usize) -> ValOffset {
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

    /// Adds valid entries to GC Table (will be
    /// synced to active memtable later)
    pub(crate) async fn write_valid_entries_to_store(
        valid_entries: ValidEntries,
        table: GCTable,
        gc_updated_entries: GCUpdatedEntries<Key>,
        vlog: GCLog,
    ) -> Result<(), Error> {
        gc_updated_entries.write().await.clear();
        for (key, value, existing_v_offset) in valid_entries.to_owned().read().await.iter() {
            GC::put(
                key,
                value,
                *existing_v_offset,
                Arc::clone(&table),
                gc_updated_entries.clone(),
            )
            .await;
            // update  vlog head to the most recent entry offset
            if existing_v_offset > &vlog.read().await.head_offset {
                vlog.write().await.set_head(*existing_v_offset)
            }
        }
        Ok(())
    }

    /// Adds valid entries to value log
    pub(crate) async fn write_valid_entries_to_vlog(
        valid_entries: Arc<RwLock<Vec<(Key, Value)>>>,
        synced_entries: SyncedEntries,
        vlog: GCLog,
    ) -> Result<(), Error> {
        for (key, value) in valid_entries.to_owned().read().await.iter() {
            let v_offset = vlog.write().await.append(&key, &value, Utc::now(), false).await;
            synced_entries
                .write()
                .await
                .push((key.to_owned(), value.to_owned(), v_offset));
        }
        Ok(())
    }

    #[allow(unused_variables)] // for non-linux environment
    /// Frees unused space on the disk
    ///
    /// Returns new head and new tail in case of success
    ///
    /// # Errors
    ///
    /// Returns error in case of IO error
    pub(crate) async fn free_unused_space(&mut self) -> std::result::Result<(Head, Tail), Error> {
        if !self.gc_updated_entries.read().await.is_empty() {
            return Err(GCErrorAttemptToRemoveUnsyncedEntries);
        }
        let vlog_path = self.vlog.read().await.content.file.node.file_path.to_owned();
        let marker_lock = self.punch_marker.lock().await;
        #[cfg(target_os = "linux")]
        {
            GC::punch_holes(
                vlog_path,
                marker_lock.punch_hole_start_offset as i64,
                marker_lock.punch_hole_length as i64,
            )
            .await?;
            (self.vlog.write().await).tail_offset += marker_lock.punch_hole_length;
            let vlog_reader = self.vlog.read().await;
            Ok((vlog_reader.head_offset, vlog_reader.tail_offset))
        }
        #[cfg(not(target_os = "linux"))]
        {
            log::info!(
                "{}",
                GCErrorUnsupportedPlatform(String::from("File system does not support file punch hole",))
            );
            // Even though punch wasn't successful due to OS incompatability, valid entires has been
            // synced to disk so we can update tail offset
            (self.vlog.write().await).tail_offset += marker_lock.punch_hole_length;
            let vlog_reader = self.vlog.read().await;
            Ok((vlog_reader.head_offset, vlog_reader.tail_offset))
        }
    }

    /// Punch holes in value log file
    ///
    /// Deallocates space (i.e., creates a hole) in the byte range
    /// starting at offset and continuing for len bytes
    /// <https://linux.die.net/man/2/fallocate>
    /// # Errors
    ///
    /// Returns error in case punch failed
    #[allow(dead_code)] // will show unused on non-linux environment
    pub(crate) async fn punch_holes<P: AsRef<Path> + std::marker::Send + 'static>(
        file_path: P,
        offset: off_t,
        length: off_t,
    ) -> std::result::Result<(), Error> {
        let punch_handle = tokio::task::spawn_blocking(move || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&file_path)
                .map_err(|err| Error::FileOpen {
                    path: file_path.as_ref().to_path_buf(),
                    error: err,
                })?;

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
        });
        punch_handle.await.unwrap()
    }

    /// Inserts valid entries to GC table
    ///
    /// # Errors
    ///
    /// Returns error in case put fails
    pub(crate) async fn put<T: AsRef<[u8]>>(
        key: T,
        value: T,
        val_offset: ValOffset,
        memtable: GCTable,
        gc_updated_entries: GCUpdatedEntries<Key>,
    ) {
        let is_tombstone = value.as_ref().is_empty();
        let created_at = Utc::now();
        let v_offset = val_offset;
        let entry = Entry::new(key.as_ref(), v_offset, created_at, is_tombstone);
        memtable.write().await.insert(&entry);
        gc_updated_entries.write().await.insert(
            key.as_ref().to_vec(),
            SkipMapValue::new(v_offset, created_at, is_tombstone),
        );
    }

    /// Retrieves key (searches GC Table first, then SSTables next)
    ///
    /// Returns tuple of value and date created
    ///
    /// # Errors
    ///
    /// Returns error in case search was not successful
    pub(crate) async fn get<CustomKey: K>(
        key: CustomKey,
        memtable: GCTable,
        key_range: KeyRangeHandle,
        vlog: Arc<RwLock<ValueLog>>,
        read_only_memtables: ImmutableMemTables<Key>,
    ) -> Result<(Value, CreatedAt), Error> {
        let key = key.as_ref().to_vec();
        let mut offset = 0;
        let lowest_insert_date = util::default_datetime();
        let mut insert_time = util::default_datetime();
        // Step 1: Check the active memtable
        if let Some(value) = memtable.read().await.get(&key) {
            if value.is_tombstone {
                return Err(NotFoundInDB);
            }
            GC::get_value_from_vlog(&vlog, value.val_offset, value.created_at).await
        } else {
            // Step 2: Check the read-only memtables
            let mut is_deleted = false;
            for table in read_only_memtables.iter() {
                if let Some(value) = table.value().get(&key) {
                    if value.created_at > insert_time {
                        offset = value.val_offset;
                        insert_time = value.created_at;
                        is_deleted = value.is_tombstone
                    }
                }
            }
            if GC::found_in_table(insert_time, lowest_insert_date) {
                if is_deleted {
                    return Err(NotFoundInDB);
                }
                GC::get_value_from_vlog(&vlog, offset, insert_time).await
            } else {
                // Step 3: Check sstables
                let ssts = &key_range.filter_sstables_by_biggest_key(&key).await?;
                GC::search_key_in_sstables(key, ssts.to_vec(), &vlog).await
            }
        }
    }

    /// Retrieves key from SSTable
    ///
    /// Returns tuple of value and date created
    ///
    /// # Errors
    ///
    /// Returns error in case search was not successful
    pub(crate) async fn search_key_in_sstables<K: AsRef<[u8]>>(
        key: K,
        ssts: Vec<Table>,
        val_log: &GCLog,
    ) -> Result<(Value, CreatedAt), Error> {
        let mut insert_time = util::default_datetime();
        let lowest_insert_date = util::default_datetime();
        let mut offset = 0;
        let mut is_deleted = false;
        for sst in ssts.iter() {
            let index = Index::new(sst.index_file.path.to_owned(), sst.index_file.file.to_owned());
            let block_handle = index.get(&key).await?;
            if block_handle.is_some() {
                let sst_res = sst.get(block_handle.unwrap(), &key).await?;
                if sst_res.as_ref().is_some() {
                    let (val_offset, created_at, is_tombstone) = sst_res.unwrap();
                    if created_at > insert_time {
                        offset = val_offset;
                        insert_time = created_at;
                        is_deleted = is_tombstone;
                    }
                }
            }
        }
        if GC::found_in_table(insert_time, lowest_insert_date) {
            if is_deleted {
                return Err(NotFoundInDB);
            }
            return GC::get_value_from_vlog(val_log, offset, insert_time).await;
        }
        Err(NotFoundInDB)
    }

    pub(crate) fn found_in_table(insert_time: CreatedAt, lowest_insert_date: CreatedAt) -> bool {
        insert_time > lowest_insert_date
    }

    /// Retrieves entry from value log
    ///
    /// Returns tuple of value and date created
    ///
    /// # Errors
    ///
    /// Returns error in case search was not successful
    pub(crate) async fn get_value_from_vlog(
        val_log: &GCLog,
        offset: ValOffset,
        creation_at: CreatedAt,
    ) -> Result<(Value, CreatedAt), Error> {
        println!("Here we are");
        let res = val_log.read().await.get(offset).await?;
        if let Some((value, is_tombstone)) = res {
            if is_tombstone {
                return Err(NotFoundInDB);
            }
            return Ok((value, creation_at));
        }
        Err(NotFoundInDB)
    }

    /// Handles entries marked as tombstone
    pub(crate) async fn handle_deleted_entries(
        invalid_entries: Arc<RwLock<Vec<ValueLogEntry>>>,
        entry: ValueLogEntry,
        err: Error,
    ) -> std::result::Result<(), Error> {
        match err {
            NotFoundInDB => {
                invalid_entries.write().await.push(entry);
                Ok(())
            }
            _ => Err(err),
        }
    }
}

async fn sleep_gc_task(duration: std::time::Duration) {
    sleep(duration).await;
}
