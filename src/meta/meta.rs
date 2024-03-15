use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::path::PathBuf;

use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Meta {
    pub path: PathBuf,
    pub v_log_tail: u32,
    pub v_log_head: u32,
    pub created_at: DateTime<Utc>,
    pub last_modified: DateTime<Utc>,
}

impl Meta {
    pub fn new(path: &PathBuf) -> Self {
        let created_at = Utc::now();
        let last_modified = Utc::now();
        Self {
            path: PathBuf::from(path),
            v_log_tail: 0,
            v_log_head: 0,
            created_at,
            last_modified,
        }
    }

    // tried useing serde for serialization but DateTime type doesn't implement Serialize trait(will do that later but for now we write in bytes)
    pub fn write_to_file(&self) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(self.path.clone())?;
        let entry_size = 4 + 4 + 4 + 4; // tail is 4 bytes and head is 4 bytes and 8 bytes to store the dates
        let mut meta_entry: Vec<u8> = Vec::with_capacity(entry_size);
        //store tail in file
        meta_entry.extend_from_slice(&(self.v_log_tail.to_le_bytes()));

        // store head in file
        meta_entry.extend_from_slice(&(self.v_log_head.to_le_bytes()));

        meta_entry.extend_from_slice(&(self.created_at.timestamp_micros().to_le_bytes()));

        meta_entry.extend_from_slice(&(self.last_modified.timestamp_micros().to_le_bytes()));
        // Write the bytes to the file
        file.write_all(&meta_entry)?;

        file.flush()?;
        Ok(())
    }

    pub fn read_from_file(&self) -> io::Result<Meta> {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(self.path.clone())?;
        let mut head_bytes: [u8; 4] = [0; 4];

        file.read_exact(&mut head_bytes)?;
        let head = u32::from_le_bytes(head_bytes);

        let mut tail_bytes: [u8; 4] = [0; 4];

        file.read_exact(&mut tail_bytes)?;
        let tail = u32::from_le_bytes(tail_bytes);

        //TODO:  Abstract date conversion to a seperate function
        let mut date_created_bytes: [u8; 4] = [0; 4];
        file.read_exact(&mut date_created_bytes)?;
        let date_created_in_microseconds = u32::from_le_bytes(date_created_bytes);
        // Convert microseconds to seconds
        let seconds = date_created_in_microseconds / 1_000_000;

        // Extract the remaining microseconds
        let micros_remainder = date_created_in_microseconds % 1_000_000;

        // Create a NaiveDateTime from seconds and microseconds
        let created_utc =
        DateTime::from_timestamp(seconds.into(), micros_remainder * 1000).unwrap();


        let mut date_modified_bytes: [u8; 4] = [0; 4];
        file.read_exact(&mut date_modified_bytes)?;
        let date_modified_in_microseconds = u32::from_le_bytes(date_modified_bytes);
        // Convert microseconds to seconds
        let date_modified_seconds = date_modified_in_microseconds / 1_000_000;

        // Extract the remaining microseconds
        let micros_remainder = date_modified_in_microseconds % 1_000_000;

        // Create a NaiveDateTime from seconds and microseconds
        let modified_date_naive_datetime =  DateTime::from_timestamp(
            date_modified_seconds.into(),
            micros_remainder * 1000,
        ).unwrap();

        Ok(Self {
            created_at: created_utc,
            last_modified: modified_date_naive_datetime,
            path: self.path.clone(),
            v_log_tail: tail,
            v_log_head: head,
        })
    }
}
