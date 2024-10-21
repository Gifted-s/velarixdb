use chrono::{DateTime, TimeZone, Utc};

#[cfg(test)]
use rand::{distributions::Alphanumeric, Rng};

/// Gnerate random string id of `length`
/// used during test
#[cfg(test)]
pub fn generate_random_id(length: usize) -> String {
    let rng = rand::thread_rng();
    let id: String = rng
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect();
    id
}

/// Converts milliseconds to `DateTime<Utc>`
pub fn milliseconds_to_datetime(milliseconds: u64) -> DateTime<Utc> {
    let seconds = (milliseconds / 1000) as i64;
    let nanoseconds = ((milliseconds % 1000) * 1_000_000) as u32;
    Utc.timestamp_opt(seconds, nanoseconds).unwrap()
}

/// Returns lowest possible `DateTime<Utc>`
pub fn default_datetime() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap()
}

/// Converts float to bytes slice
pub fn float_to_le_bytes(f: f64) -> [u8; 8] {
    // Convert f64 to its bit representation (u64)
    let bits: u64 = f.to_bits();

    // Convert the u64 to an array of 8 bytes in little-endian order
    let bytes: [u8; 8] = bits.to_le_bytes();

    bytes
}
/// Converts bytes slice to float
pub fn float_from_le_bytes(bytes: &[u8]) -> Option<f64> {
    // Ensure the byte array has the correct size for f32
    if bytes.len() != 8 {
        return None;
    }

    // Convert the byte array to a u64 in little-endian order
    let bits: u64 = u64::from_le_bytes(bytes.try_into().unwrap());

    // Convert the u64 bit representation back to f32
    let float: f64 = f64::from_bits(bits);
    Some(float)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_generate_random_id() {
        let id1 = generate_random_id(10);
        let id2 = generate_random_id(10);
        assert_eq!(id1.len(), 10);
        assert_eq!(id2.len(), 10);
        assert_ne!(id1, id2); // Ensure IDs are random and not equal
    }

    #[test]
    fn test_milliseconds_to_datetime() {
        let datetime = milliseconds_to_datetime(1_000);
        assert_eq!(datetime, Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 1).unwrap());

        let datetime = milliseconds_to_datetime(1_000_000);
        assert_eq!(datetime, Utc.with_ymd_and_hms(1970, 1, 1, 0, 16, 40).unwrap());
    }

    #[test]
    fn test_default_datetime() {
        let datetime = default_datetime();
        assert_eq!(datetime, Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_float_to_le_bytes() {
        let float = 1.23_f64;
        let bytes = float_to_le_bytes(float);
        let expected = float.to_le_bytes();
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_float_from_le_bytes() {
        let float = 1.23_f64;
        let bytes = float.to_le_bytes();
        let result = float_from_le_bytes(&bytes);
        assert_eq!(result, Some(float));

        let invalid_bytes = [0_u8; 4]; // Invalid size for f64
        let result = float_from_le_bytes(&invalid_bytes);
        assert_eq!(result, None);
    }
}
