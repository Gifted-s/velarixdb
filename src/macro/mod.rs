pub mod fs_macros {
    #[macro_export]
    macro_rules! load_buffer {
        ($file:expr, $buffer:expr, $file_path:expr) => {
            match $file.read($buffer).await {
                Ok(bytes_read) => Ok(bytes_read),
                Err(err) => Err(FileReadError {
                    path: $file_path,
                    error: err,
                }),
            }
        };
    }
    #[macro_export]
    macro_rules! open_dir_stream {
        ($path:expr) => {{
            let stream = read_dir($path.to_owned()).await.map_err(|err| DirOpenError {
                path: $path,
                error: err,
            })?;
            stream
        }};
    }
}
