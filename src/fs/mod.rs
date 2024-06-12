pub trait FileAsync: Send + Sync {
    fn flush(&mut self) -> Result<()>;
    async fn create(path: impl AsRef<Path>) -> Result<File>;


    async fn metadata(&mut self) -> Result<Metadata>;


    async fn open(path: impl AsRef<Path>) -> Result<File>;


    pub async fn sync_all(&self) -> Result<()>;


    fn is_empty(&self) -> bool {
        if let Ok(length) = self.len() {
            return length == 0;
        }
        // Err is considered as empty
        false
    }
    fn read_lock(&self) -> Result<()>;

    
    fn write_lock(&self) -> Result<()>;
}
