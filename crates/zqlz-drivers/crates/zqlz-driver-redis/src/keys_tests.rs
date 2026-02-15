//! Tests for Redis key browser module

use super::keys::*;

#[cfg(test)]
mod key_type_tests {
    use super::*;

    #[test]
    fn test_key_type_from_redis_string() {
        assert_eq!(KeyType::from_redis_type("string"), KeyType::String);
        assert_eq!(KeyType::from_redis_type("STRING"), KeyType::String);
        assert_eq!(KeyType::from_redis_type("list"), KeyType::List);
        assert_eq!(KeyType::from_redis_type("set"), KeyType::Set);
        assert_eq!(KeyType::from_redis_type("zset"), KeyType::Zset);
        assert_eq!(KeyType::from_redis_type("hash"), KeyType::Hash);
        assert_eq!(KeyType::from_redis_type("stream"), KeyType::Stream);
        assert_eq!(KeyType::from_redis_type("none"), KeyType::None);
        assert_eq!(KeyType::from_redis_type("unknown"), KeyType::None);
    }

    #[test]
    fn test_key_type_as_str() {
        assert_eq!(KeyType::String.as_str(), "string");
        assert_eq!(KeyType::List.as_str(), "list");
        assert_eq!(KeyType::Set.as_str(), "set");
        assert_eq!(KeyType::Zset.as_str(), "zset");
        assert_eq!(KeyType::Hash.as_str(), "hash");
        assert_eq!(KeyType::Stream.as_str(), "stream");
        assert_eq!(KeyType::None.as_str(), "none");
    }

    #[test]
    fn test_key_type_display() {
        assert_eq!(format!("{}", KeyType::String), "string");
        assert_eq!(format!("{}", KeyType::Hash), "hash");
        assert_eq!(format!("{}", KeyType::None), "none");
    }

    #[test]
    fn test_key_type_serialization() {
        let key_type = KeyType::String;
        let json = serde_json::to_string(&key_type).unwrap();
        assert_eq!(json, "\"string\"");

        let deserialized: KeyType = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, KeyType::String);
    }

    #[test]
    fn test_key_type_equality() {
        assert_eq!(KeyType::String, KeyType::String);
        assert_ne!(KeyType::String, KeyType::List);
    }
}

#[cfg(test)]
mod key_info_tests {
    use super::*;

    #[test]
    fn test_key_info_creation() {
        let info = KeyInfo::new("mykey", KeyType::String, 3600);
        assert_eq!(info.name, "mykey");
        assert_eq!(info.key_type, KeyType::String);
        assert_eq!(info.ttl, 3600);
        assert!(info.memory_bytes.is_none());
    }

    #[test]
    fn test_key_info_with_memory() {
        let info = KeyInfo::new("mykey", KeyType::Hash, -1).with_memory(1024);
        assert_eq!(info.name, "mykey");
        assert_eq!(info.key_type, KeyType::Hash);
        assert_eq!(info.ttl, -1);
        assert_eq!(info.memory_bytes, Some(1024));
    }

    #[test]
    fn test_key_info_has_expiry() {
        let with_expiry = KeyInfo::new("key1", KeyType::String, 3600);
        assert!(with_expiry.has_expiry());

        let no_expiry = KeyInfo::new("key2", KeyType::String, -1);
        assert!(!no_expiry.has_expiry());

        let not_found = KeyInfo::new("key3", KeyType::None, -2);
        assert!(!not_found.has_expiry());
    }

    #[test]
    fn test_key_info_exists() {
        let exists = KeyInfo::new("key1", KeyType::String, -1);
        assert!(exists.exists());

        let not_found_ttl = KeyInfo::new("key2", KeyType::String, -2);
        assert!(!not_found_ttl.exists());

        let not_found_type = KeyInfo::new("key3", KeyType::None, -1);
        assert!(!not_found_type.exists());
    }

    #[test]
    fn test_key_info_serialization() {
        let info = KeyInfo::new("test:key", KeyType::List, 300);
        let json = serde_json::to_string(&info).unwrap();

        assert!(json.contains("\"name\":\"test:key\""));
        assert!(json.contains("\"key_type\":\"list\""));
        assert!(json.contains("\"ttl\":300"));

        let deserialized: KeyInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "test:key");
        assert_eq!(deserialized.key_type, KeyType::List);
        assert_eq!(deserialized.ttl, 300);
    }

    #[test]
    fn test_key_info_with_memory_serialization() {
        let info = KeyInfo::new("cached:item", KeyType::String, -1).with_memory(2048);
        let json = serde_json::to_string(&info).unwrap();

        assert!(json.contains("\"memory_bytes\":2048"));

        let deserialized: KeyInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.memory_bytes, Some(2048));
    }
}

#[cfg(test)]
mod list_keys_options_tests {
    use super::*;

    #[test]
    fn test_list_keys_options_default() {
        let options = ListKeysOptions::new();
        assert_eq!(options.pattern, "*");
        assert_eq!(options.limit, 0);
        assert_eq!(options.scan_count, 100);
        assert!(!options.include_types);
        assert!(!options.include_ttl);
    }

    #[test]
    fn test_list_keys_options_with_pattern() {
        let options = ListKeysOptions::new().with_pattern("user:*");
        assert_eq!(options.pattern, "user:*");
    }

    #[test]
    fn test_list_keys_options_with_limit() {
        let options = ListKeysOptions::new().with_limit(100);
        assert_eq!(options.limit, 100);
    }

    #[test]
    fn test_list_keys_options_with_scan_count() {
        let options = ListKeysOptions::new().with_scan_count(500);
        assert_eq!(options.scan_count, 500);
    }

    #[test]
    fn test_list_keys_options_include_types() {
        let options = ListKeysOptions::new().include_types();
        assert!(options.include_types);
    }

    #[test]
    fn test_list_keys_options_include_ttl() {
        let options = ListKeysOptions::new().include_ttl();
        assert!(options.include_ttl);
    }

    #[test]
    fn test_list_keys_options_builder_chain() {
        let options = ListKeysOptions::new()
            .with_pattern("cache:*")
            .with_limit(50)
            .with_scan_count(200)
            .include_types()
            .include_ttl();

        assert_eq!(options.pattern, "cache:*");
        assert_eq!(options.limit, 50);
        assert_eq!(options.scan_count, 200);
        assert!(options.include_types);
        assert!(options.include_ttl);
    }
}
