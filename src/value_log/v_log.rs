use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub struct ValueLog {
    file: Arc<Mutex<File>>,
}

pub(crate) static VLOG_FILE_NAME: &str = "val_log.bin";
#[derive(PartialEq, Debug)]
pub(crate) struct ValueLogEntry {
    pub(crate) ksize: usize,
    pub(crate) vsize: usize,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) created_at: u64, // date to milliseconds
}

impl ValueLog {
    pub(crate) fn new(dir: &PathBuf) -> io::Result<Self> {
        let dir_path = PathBuf::from(dir);

        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)?;
        }

        let file_path = dir_path.join(VLOG_FILE_NAME);

        let log_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(file_path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(log_file)),
        })
    }

    pub(crate) fn append(
        &self,
        key: &Vec<u8>,
        value: &Vec<u8>,
        created_at: u64,
    ) -> io::Result<usize> {
        let mut log_file = self.file.lock().map_err(|poison_err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to lock file {:?}", poison_err),
            )
        })?;
        let v_log_entry = ValueLogEntry::new(
            key.len(),
            value.len(),
            key.to_vec(),
            value.to_vec(),
            created_at,
        );
        let serialized_data = v_log_entry.serialize();

        // Get the current offset before writing(this will be the offset of the value stored in the memtable)
        let value_offset = log_file.seek(io::SeekFrom::End(0))?;

        log_file
            .write_all(&serialized_data)
            .expect("Failed to write to value log entry");
        log_file.flush()?;

        return Ok(value_offset.try_into().unwrap());
    }

    pub(crate) fn get(&mut self, start_offset: usize) -> io::Result<Option<Vec<u8>>> {
        let mut log_file = self.file.lock().map_err(|poison_err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to lock file {:?}", poison_err),
            )
        })?;
        log_file.seek(SeekFrom::Start(start_offset as u64))?;

        // get entry length
        let mut entry_len_bytes = [0; mem::size_of::<u32>()];
        let bytes_read = log_file.read(&mut entry_len_bytes)?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let _entry_len = u32::from_le_bytes(entry_len_bytes);

        // get key length
        let mut key_len_bytes = [0; mem::size_of::<u32>()];
        let bytes_read = log_file.read(&mut key_len_bytes)?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let key_len = u32::from_le_bytes(key_len_bytes);

        // get value length
        let mut val_len_bytes = [0; mem::size_of::<u32>()];
        let bytes_read = log_file.read(&mut val_len_bytes)?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let val_len = u32::from_le_bytes(val_len_bytes);

        // get date length
        let mut creation_date_bytes = [0; mem::size_of::<u64>()];
        let mut bytes_read = log_file.read(&mut creation_date_bytes)?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let _ = u64::from_le_bytes(creation_date_bytes);

        let mut key = vec![0; key_len as usize];
        bytes_read = log_file.read(&mut key)?;
        if bytes_read == 0 {
            return Ok(None);
        }

        let mut value = vec![0; val_len as usize];
        bytes_read = log_file.read(&mut value)?;

        if bytes_read == 0 {
            return Ok(None);
        }
        Ok(Some(value))
    }

    pub(crate) fn recover(&mut self, start_offset: usize) -> io::Result<Vec<ValueLogEntry>> {
        let mut log_file = self.file.lock().map_err(|poison_err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to lock file {:?}", poison_err),
            )
        })?;
        log_file.seek(SeekFrom::Start(start_offset as u64))?;
        let mut entries = Vec::new();
        loop {
            // get entry length
            let mut entry_len_bytes = [0; mem::size_of::<u32>()];
            let bytes_read = log_file.read(&mut entry_len_bytes)?;
            if bytes_read == 0 {
                return Ok(entries);
            }
            let _entry_len = u32::from_le_bytes(entry_len_bytes);

            // get key length
            let mut key_len_bytes = [0; mem::size_of::<u32>()];
            let bytes_read = log_file.read(&mut key_len_bytes)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("Error while reading entries from value log file"),
                ));
            }
            let key_len = u32::from_le_bytes(key_len_bytes);

            // get value length
            let mut val_len_bytes = [0; mem::size_of::<u32>()];
            let bytes_read = log_file.read(&mut val_len_bytes)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error while reading entries from value log file"),
                ));
            }
            let val_len = u32::from_le_bytes(val_len_bytes);

            // get date length
            let mut creation_date_bytes = [0; mem::size_of::<u64>()];
            let mut bytes_read = log_file.read(&mut creation_date_bytes)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error while reading entries from value log file"),
                ));
            }
            let created_at = u64::from_le_bytes(creation_date_bytes);

            let mut key = vec![0; key_len as usize];
            bytes_read = log_file.read(&mut key)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error while reading entries from value log file"),
                ));
            }

            let mut value = vec![0; val_len as usize];
            bytes_read = log_file.read(&mut value)?;

            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error while reading entries from value log file"),
                ));
            }
            entries.push(ValueLogEntry {
                ksize: key_len as usize,
                vsize: val_len as usize,
                key,
                value,
                created_at,
            })
        }
    }
}

impl ValueLogEntry {
    pub(crate) fn new(
        ksize: usize,
        vsize: usize,
        key: Vec<u8>,
        value: Vec<u8>,
        created_at: u64,
    ) -> Self {
        Self {
            ksize,
            vsize,
            key,
            value,
            created_at,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let entry_len = mem::size_of::<u32>()
            + mem::size_of::<u32>()
            + mem::size_of::<u32>()
            + mem::size_of::<u64>()
            + self.key.len()
            + self.value.len();

        let mut serialized_data = Vec::with_capacity(entry_len);

        serialized_data.extend_from_slice(&(entry_len as u32).to_le_bytes());

        serialized_data.extend_from_slice(&(self.key.len() as u32).to_le_bytes());

        serialized_data.extend_from_slice(&(self.value.len() as u32).to_le_bytes());

        serialized_data.extend_from_slice(&(self.created_at as u64).to_le_bytes());

        serialized_data.extend_from_slice(&self.key);

        serialized_data.extend_from_slice(&self.value);

        serialized_data
    }

    fn deserialize(serialized_data: &[u8]) -> io::Result<Self> {
        if serialized_data.len() < 20 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid length of serialized data",
            ));
        }

        let entry_len = u32::from_le_bytes([
            serialized_data[0],
            serialized_data[1],
            serialized_data[2],
            serialized_data[3],
        ]) as usize;

        if serialized_data.len() != entry_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid length of serialized data",
            ));
        }

        let key_len = u32::from_le_bytes([
            serialized_data[4],
            serialized_data[5],
            serialized_data[6],
            serialized_data[7],
        ]) as usize;

        let value_len = u32::from_le_bytes([
            serialized_data[8],
            serialized_data[9],
            serialized_data[10],
            serialized_data[11],
        ]) as usize;

        let created_at = u64::from_le_bytes([
            serialized_data[12],
            serialized_data[13],
            serialized_data[14],
            serialized_data[15],
            serialized_data[16],
            serialized_data[17],
            serialized_data[18],
            serialized_data[19],
        ]);

        if serialized_data.len() != 20 + key_len + value_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid length of serialized data",
            ));
        }

        let key = serialized_data[20..(20 + key_len)].to_vec();
        let value = serialized_data[(20 + key_len)..].to_vec();

        Ok(ValueLogEntry::new(
            key_len, value_len, key, value, created_at,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialized_deserialized() {
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        let created_at = 164343434343434;
        let original_entry = ValueLogEntry::new(
            key.len(),
            value.len(),
            key.clone(),
            value.clone(),
            created_at,
        );
        let serialized_data = original_entry.serialize();

        let expected_entry_len = 4 + 4 + 4 + 8 + key.len() + value.len();

        assert_eq!(serialized_data.len(), expected_entry_len);

        let deserialized_entry =
            ValueLogEntry::deserialize(&serialized_data).expect("failed to deserialize data");

        assert_eq!(deserialized_entry, original_entry);
    }
}
