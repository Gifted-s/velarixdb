use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

// For now this struct doesn't do much but as the project evolve it will store details about the storage engine
#[derive(Debug, Clone)]
pub struct Meta {
    pub path: PathBuf,
    pub v_log_tail: u32,
    pub v_log_head: u32,
    pub created_at: DateTime<Utc>,
    pub last_modified: DateTime<Utc>,
}

impl Meta {
    pub fn new<P: AsRef<Path> + Send + Sync>(path: P) -> Self {
        let created_at = Utc::now();
        let last_modified = Utc::now();
        Self {
            path: path.as_ref().to_path_buf(),
            v_log_tail: 0,
            v_log_head: 0,
            created_at,
            last_modified,
        }
    }
}
