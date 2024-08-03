pub fn unix_timestamp() -> std::time::Duration {
    let now = std::time::SystemTime::now();

    now.duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("time went backwards")
}
