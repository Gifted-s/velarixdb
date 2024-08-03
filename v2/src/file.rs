use std::{fs::File, io::Write, path::Path};

pub const LSM_MARKER: &str = "version";
pub const CONFIG_FILE: &str = "config";
pub const SEGMENTS_FOLDER: &str = "segments";
pub const LEVELS_MANIFEST_FILE: &str = "levels";

pub fn rewrite_atomic<P: AsRef<Path>>(path: P, content: &[u8]) -> std::io::Result<()> {
    let path = path.as_ref();
    let folder = path.parent().expect("should have a parent");
    let mut temp_file = tempfile::NamedTempFile::new_in(folder)?;
    temp_file.write_all(content)?;
    temp_file.persist(path)?;

    #[cfg(not(target_os = "windows"))]
    {
        let file = File::open(path)?;
        file.sync_all()?;
    }

    Ok(())
}

#[cfg(not(target_os = "windows"))]
pub fn fsync_directory<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    let file = File::open(path)?;
    debug_assert!(file.metadata()?.is_dir());
    file.sync_all()
}

#[cfg(target_os = "windows")]
pub fn fsync_directory<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    // Cannot fsync directory on Windows
    Ok()
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use test_log::test;

    use super::rewrite_atomic;
    #[test]
    fn atomic_write() -> std::io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.txt");

        let mut file = File::create(&path)?;
        write!(file, "abcdefghijklmnop")?;

        rewrite_atomic(&path, b"newcontent")?;

        let content = std::fs::read_to_string(&path)?;
        assert_eq!("newcontent", content);
        Ok(())
    }
}
