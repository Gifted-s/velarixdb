use std::path::{Path, PathBuf};

use rand::{distributions::Alphanumeric, Rng};

pub fn generate_random_id(length: usize) -> String {
    let rng = rand::thread_rng();
    let id: String = rng.sample_iter(&Alphanumeric).take(length).map(char::from).collect();
    id
}

pub(crate) fn data_file_exists(path_buf: &PathBuf) -> bool {
    // Convert the PathBuf to a Path
    let path: &Path = path_buf.as_path();
    // Check if the file exists
    path.exists() && path.is_file()
}
