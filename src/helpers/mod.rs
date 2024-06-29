use rand::{distributions::Alphanumeric, Rng};

use chrono::{DateTime, TimeZone, Utc};

pub fn generate_random_id(length: usize) -> String {
    let rng = rand::thread_rng();
    let id: String = rng.sample_iter(&Alphanumeric).take(length).map(char::from).collect();
    id
}

pub fn milliseconds_to_datetime(milliseconds: u64) -> DateTime<Utc> {
    let seconds = (milliseconds / 1000) as i64;
    let nanoseconds = ((milliseconds % 1000) * 1_000_000) as u32;
    Utc.timestamp_opt(seconds, nanoseconds).unwrap()
}
pub fn default_datetime() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap()
}
