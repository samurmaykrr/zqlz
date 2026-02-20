//! Unit tests for Redis driver

use crate::driver::RedisDriver;
use zqlz_core::{ConnectionConfig, DatabaseDriver};

/// Helper function to get the Redis dialect info via the driver
fn redis_dialect() -> zqlz_core::DialectInfo {
    RedisDriver::new().dialect_info()
}

mod driver_metadata_tests {
    use super::*;

    #[test]
    fn test_redis_driver_id() {
        let driver = RedisDriver::new();
        assert_eq!(driver.id(), "redis");
    }

    #[test]
    fn test_redis_driver_name() {
        let driver = RedisDriver::new();
        assert_eq!(driver.name(), "redis");
        assert_eq!(driver.display_name(), "Redis");
    }

    #[test]
    fn test_redis_default_port() {
        let driver = RedisDriver::new();
        assert_eq!(driver.default_port(), Some(6379));
    }

    #[test]
    fn test_redis_version() {
        let driver = RedisDriver::new();
        assert_eq!(driver.version(), "0.1.0");
    }

    #[test]
    fn test_redis_icon_name() {
        let driver = RedisDriver::new();
        assert_eq!(driver.icon_name(), "redis");
    }

    #[test]
    fn test_redis_default_driver() {
        let driver = RedisDriver;
        assert_eq!(driver.id(), "redis");
    }
}

mod driver_capabilities_tests {
    use super::*;

    #[test]
    fn test_redis_capabilities_transactions() {
        let driver = RedisDriver::new();
        let caps = driver.capabilities();
        assert!(caps.supports_transactions);
        assert!(!caps.supports_savepoints);
    }

    #[test]
    fn test_redis_capabilities_no_sql_features() {
        let driver = RedisDriver::new();
        let caps = driver.capabilities();
        assert!(!caps.supports_prepared_statements);
        assert!(!caps.supports_cte);
        assert!(!caps.supports_window_functions);
        assert!(!caps.supports_foreign_keys);
        assert!(!caps.supports_views);
        assert!(!caps.supports_triggers);
    }

    #[test]
    fn test_redis_capabilities_special_features() {
        let driver = RedisDriver::new();
        let caps = driver.capabilities();
        assert!(caps.supports_json); // RedisJSON
        assert!(caps.supports_full_text_search); // RediSearch
        assert!(caps.supports_multiple_databases); // SELECT 0-15
        assert!(caps.supports_streaming); // Pub/Sub
        assert!(caps.supports_ssl);
    }

    #[test]
    fn test_redis_capabilities_upsert() {
        let driver = RedisDriver::new();
        let caps = driver.capabilities();
        assert!(caps.supports_upsert); // SET with NX/XX
    }
}

mod connection_string_tests {
    use super::*;

    #[test]
    fn test_redis_connection_string_defaults() {
        let driver = RedisDriver::new();
        let config = ConnectionConfig::new("redis", "Test Redis");
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "redis://127.0.0.1:6379/0");
    }

    #[test]
    fn test_redis_connection_string_with_host_port() {
        let driver = RedisDriver::new();
        let mut config = ConnectionConfig::new("redis", "Test Redis");
        config.host = "redis.example.com".to_string();
        config.port = 6380;
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "redis://redis.example.com:6380/0");
    }

    #[test]
    fn test_redis_connection_string_with_database() {
        let driver = RedisDriver::new();
        let config = ConnectionConfig::new("redis", "Test Redis").with_param("database", "5");
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "redis://127.0.0.1:6379/5");
    }

    #[test]
    fn test_redis_connection_string_with_password() {
        let driver = RedisDriver::new();
        let mut config = ConnectionConfig::new("redis", "Test Redis");
        config.password = Some("secret123".to_string());
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "redis://:secret123@127.0.0.1:6379/0");
    }

    #[test]
    fn test_redis_connection_string_with_username_password() {
        let driver = RedisDriver::new();
        let mut config = ConnectionConfig::new("redis", "Test Redis");
        config.username = Some("admin".to_string());
        config.password = Some("secret123".to_string());
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "redis://admin:secret123@127.0.0.1:6379/0");
    }

    #[test]
    fn test_redis_connection_string_with_tls() {
        let driver = RedisDriver::new();
        let config = ConnectionConfig::new("redis", "Test Redis").with_param("tls", "true");
        let conn_str = driver.build_connection_string(&config);
        assert!(conn_str.starts_with("rediss://"));
    }

    #[test]
    fn test_redis_connection_string_with_ssl() {
        let driver = RedisDriver::new();
        let config = ConnectionConfig::new("redis", "Test Redis").with_param("ssl", "true");
        let conn_str = driver.build_connection_string(&config);
        assert!(conn_str.starts_with("rediss://"));
    }

    #[test]
    fn test_redis_connection_string_full() {
        let driver = RedisDriver::new();
        let mut config = ConnectionConfig::new("redis", "Test Redis");
        config.host = "redis.example.com".to_string();
        config.port = 6380;
        config.username = Some("user".to_string());
        config.password = Some("pass".to_string());
        let config = config.with_param("database", "3").with_param("tls", "true");
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "rediss://user:pass@redis.example.com:6380/3");
    }

    #[test]
    fn test_redis_connection_string_help() {
        let driver = RedisDriver::new();
        let help = driver.connection_string_help();
        assert!(help.contains("redis://"));
        assert!(help.contains("rediss://"));
        assert!(help.contains("TLS"));
    }
}

mod dialect_tests {
    use super::*;

    #[test]
    fn test_redis_dialect_id() {
        let dialect = redis_dialect();
        assert_eq!(dialect.id, "redis");
        assert_eq!(dialect.display_name, "Redis Commands");
    }

    #[test]
    fn test_redis_dialect_case_sensitive() {
        let dialect = redis_dialect();
        assert!(dialect.case_sensitive_identifiers);
    }

    #[test]
    fn test_redis_dialect_no_comments() {
        let dialect = redis_dialect();
        assert!(dialect.comment_styles.line_comment.is_none());
        assert!(dialect.comment_styles.block_comment_start.is_none());
        assert!(dialect.comment_styles.block_comment_end.is_none());
    }

    #[test]
    fn test_redis_dialect_no_auto_increment() {
        let dialect = redis_dialect();
        assert!(dialect.auto_increment.is_none());
    }

    #[test]
    fn test_redis_dialect_has_keywords() {
        let dialect = redis_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();
        // Verify key Redis commands are present
        assert!(keyword_names.contains(&"GET"));
        assert!(keyword_names.contains(&"SET"));
        assert!(keyword_names.contains(&"DEL"));
        assert!(keyword_names.contains(&"PING"));
        assert!(keyword_names.contains(&"MULTI"));
        assert!(keyword_names.contains(&"EXEC"));
    }

    #[test]
    fn test_redis_dialect_has_data_types() {
        let dialect = redis_dialect();
        let type_names: Vec<&str> = dialect.data_types.iter().map(|t| t.name.as_ref()).collect();
        assert!(type_names.contains(&"string"));
        assert!(type_names.contains(&"list"));
        assert!(type_names.contains(&"set"));
        assert!(type_names.contains(&"zset"));
        assert!(type_names.contains(&"hash"));
    }

    #[test]
    fn test_redis_dialect_string_commands() {
        let dialect = redis_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();
        assert!(keyword_names.contains(&"MGET"));
        assert!(keyword_names.contains(&"MSET"));
        assert!(keyword_names.contains(&"INCR"));
        assert!(keyword_names.contains(&"DECR"));
    }

    #[test]
    fn test_redis_dialect_key_commands() {
        let dialect = redis_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();
        assert!(keyword_names.contains(&"EXISTS"));
        assert!(keyword_names.contains(&"EXPIRE"));
        assert!(keyword_names.contains(&"TTL"));
        assert!(keyword_names.contains(&"KEYS"));
        assert!(keyword_names.contains(&"SCAN"));
        assert!(keyword_names.contains(&"TYPE"));
    }

    #[test]
    fn test_redis_dialect_hash_commands() {
        let dialect = redis_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();
        assert!(keyword_names.contains(&"HGET"));
        assert!(keyword_names.contains(&"HSET"));
        assert!(keyword_names.contains(&"HGETALL"));
        assert!(keyword_names.contains(&"HDEL"));
    }

    #[test]
    fn test_redis_dialect_list_commands() {
        let dialect = redis_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();
        assert!(keyword_names.contains(&"LPUSH"));
        assert!(keyword_names.contains(&"RPUSH"));
        assert!(keyword_names.contains(&"LPOP"));
        assert!(keyword_names.contains(&"RPOP"));
        assert!(keyword_names.contains(&"LRANGE"));
    }

    #[test]
    fn test_redis_dialect_set_commands() {
        let dialect = redis_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();
        assert!(keyword_names.contains(&"SADD"));
        assert!(keyword_names.contains(&"SREM"));
        assert!(keyword_names.contains(&"SMEMBERS"));
        assert!(keyword_names.contains(&"SISMEMBER"));
    }

    #[test]
    fn test_redis_dialect_sorted_set_commands() {
        let dialect = redis_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();
        assert!(keyword_names.contains(&"ZADD"));
        assert!(keyword_names.contains(&"ZRANGE"));
        assert!(keyword_names.contains(&"ZSCORE"));
        assert!(keyword_names.contains(&"ZRANK"));
    }

    #[test]
    fn test_redis_dialect_transaction_commands() {
        let dialect = redis_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();
        assert!(keyword_names.contains(&"MULTI"));
        assert!(keyword_names.contains(&"EXEC"));
        assert!(keyword_names.contains(&"DISCARD"));
        assert!(keyword_names.contains(&"WATCH"));
    }

    #[test]
    fn test_redis_dialect_server_commands() {
        let dialect = redis_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();
        assert!(keyword_names.contains(&"PING"));
        assert!(keyword_names.contains(&"INFO"));
        assert!(keyword_names.contains(&"DBSIZE"));
        assert!(keyword_names.contains(&"SELECT"));
    }

    #[test]
    fn test_redis_dialect_pubsub() {
        let dialect = redis_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();
        assert!(keyword_names.contains(&"PUBLISH"));
        assert!(keyword_names.contains(&"SUBSCRIBE"));
        assert!(keyword_names.contains(&"PSUBSCRIBE"));
    }

    #[test]
    fn test_redis_dialect_scripting() {
        let dialect = redis_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();
        assert!(keyword_names.contains(&"EVAL"));
        assert!(keyword_names.contains(&"EVALSHA"));
        assert!(keyword_names.contains(&"SCRIPT"));
    }
}

mod connection_tests {
    use super::*;

    #[test]
    fn test_redis_dialect_from_driver() {
        let driver = RedisDriver::new();
        let dialect = driver.dialect_info();
        assert_eq!(dialect.id.as_ref(), "redis");
    }
}
