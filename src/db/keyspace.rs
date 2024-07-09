use crate::consts::MAX_KEY_SPACE_SIZE;

const VALID_CHARACTERS: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";

/// Keyspace names can be up to 255 characters long, can not be empty and
/// can only contain alphanumerics, underscore (`_`) and dash (`-`).
pub fn is_valid_keyspace_name(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    if s.len() > MAX_KEY_SPACE_SIZE {
        return false;
    }

    if u8::try_from(s.len()).is_err() {
        return false;
    }

    s.chars().all(|c| VALID_CHARACTERS.contains(c))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_keyspace_name() {
        // Valid keyspace names
        assert!(is_valid_keyspace_name("valid_keyspace_name"));
        assert!(is_valid_keyspace_name("ValidKeyspaceName"));
        assert!(is_valid_keyspace_name("valid-keyspace-name"));
        assert!(is_valid_keyspace_name("ValidKeyspaceName123"));
        assert!(is_valid_keyspace_name("valid_keyspace-123"));

        // Valid but maximum length keyspace name
        let max_length_name: String = "a".repeat(MAX_KEY_SPACE_SIZE);
        assert!(is_valid_keyspace_name(&max_length_name));
    }

    #[test]
    fn test_invalid_keyspace_name() {
        // Empty keyspace name
        assert!(!is_valid_keyspace_name(""));

        // Keyspace name too long
        let too_long_name: String = "a".repeat(MAX_KEY_SPACE_SIZE + 1);
        assert!(!is_valid_keyspace_name(&too_long_name));

        // Keyspace name with invalid characters
        assert!(!is_valid_keyspace_name("invalid keyspace name!"));
        assert!(!is_valid_keyspace_name("invalid$keyspace"));
        assert!(!is_valid_keyspace_name("invalid#keyspace"));

        // Valid characters but length > 255
        let invalid_length_name: String = "a".repeat(256);
        assert!(!is_valid_keyspace_name(&invalid_length_name));
    }

    #[test]
    fn test_keyspace_name_edge_cases() {
        // Keyspace name with one valid character
        assert!(is_valid_keyspace_name("a"));

        // Keyspace name with one invalid character
        assert!(!is_valid_keyspace_name("@"));
    }
}
