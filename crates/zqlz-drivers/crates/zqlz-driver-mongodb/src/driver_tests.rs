//! Unit tests for MongoDB driver

use super::*;
use zqlz_core::{ConnectionConfig, DataTypeCategory, DatabaseDriver, FunctionCategory};

// ============================================================================
// Driver Metadata Tests
// ============================================================================

mod driver_metadata_tests {
    use super::*;

    #[test]
    fn test_mongodb_driver_id() {
        let driver = MongoDbDriver::new();
        assert_eq!(driver.id(), "mongodb");
    }

    #[test]
    fn test_mongodb_driver_name() {
        let driver = MongoDbDriver::new();
        assert_eq!(driver.name(), "mongodb");
        assert_eq!(driver.display_name(), "MongoDB");
    }

    #[test]
    fn test_mongodb_driver_version() {
        let driver = MongoDbDriver::new();
        assert_eq!(driver.version(), "0.1.0");
    }

    #[test]
    fn test_mongodb_default_port() {
        let driver = MongoDbDriver::new();
        assert_eq!(driver.default_port(), Some(27017));
    }

    #[test]
    fn test_mongodb_icon_name() {
        let driver = MongoDbDriver::new();
        assert_eq!(driver.icon_name(), "mongodb");
    }

    #[test]
    fn test_mongodb_default_driver() {
        let driver = MongoDbDriver;
        assert_eq!(driver.id(), "mongodb");
    }
}

// ============================================================================
// Driver Capabilities Tests
// ============================================================================

mod capabilities_tests {
    use super::*;

    #[test]
    fn test_mongodb_capabilities_transactions() {
        let driver = MongoDbDriver::new();
        let caps = driver.capabilities();
        assert!(
            caps.supports_transactions,
            "MongoDB 4.0+ supports transactions"
        );
    }

    #[test]
    fn test_mongodb_capabilities_nosql_features() {
        let driver = MongoDbDriver::new();
        let caps = driver.capabilities();
        assert!(!caps.supports_savepoints, "MongoDB doesn't have savepoints");
        assert!(
            !caps.supports_prepared_statements,
            "MongoDB doesn't use prepared statements"
        );
        assert!(
            !caps.supports_multiple_statements,
            "MongoDB commands are individual"
        );
        assert!(
            !caps.supports_foreign_keys,
            "MongoDB doesn't have foreign keys"
        );
    }

    #[test]
    fn test_mongodb_capabilities_json() {
        let driver = MongoDbDriver::new();
        let caps = driver.capabilities();
        assert!(caps.supports_json, "MongoDB natively supports JSON/BSON");
    }

    #[test]
    fn test_mongodb_capabilities_window_functions() {
        let driver = MongoDbDriver::new();
        let caps = driver.capabilities();
        assert!(
            caps.supports_window_functions,
            "MongoDB supports $setWindowFields"
        );
    }

    #[test]
    fn test_mongodb_capabilities_text_search() {
        let driver = MongoDbDriver::new();
        let caps = driver.capabilities();
        assert!(
            caps.supports_full_text_search,
            "MongoDB supports $text search"
        );
    }

    #[test]
    fn test_mongodb_capabilities_multiple_databases() {
        let driver = MongoDbDriver::new();
        let caps = driver.capabilities();
        assert!(
            caps.supports_multiple_databases,
            "MongoDB supports multiple databases"
        );
    }

    #[test]
    fn test_mongodb_capabilities_ssl() {
        let driver = MongoDbDriver::new();
        let caps = driver.capabilities();
        assert!(caps.supports_ssl, "MongoDB supports TLS/SSL");
    }

    #[test]
    fn test_mongodb_capabilities_max_identifier_length() {
        let driver = MongoDbDriver::new();
        let caps = driver.capabilities();
        assert_eq!(caps.max_identifier_length, Some(120));
    }
}

// ============================================================================
// Connection String Tests
// ============================================================================

mod connection_string_tests {
    use super::*;

    #[test]
    fn test_basic_connection_string() {
        let driver = MongoDbDriver::new();
        let mut config = ConnectionConfig::new("mongodb", "Test");
        config.host = "localhost".to_string();
        config.port = 27017;
        config.database = Some("mydb".to_string());

        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "mongodb://localhost:27017/mydb");
    }

    #[test]
    fn test_connection_string_with_credentials() {
        let driver = MongoDbDriver::new();
        let mut config = ConnectionConfig::new("mongodb", "Test");
        config.host = "localhost".to_string();
        config.port = 27017;
        config.database = Some("mydb".to_string());
        config.username = Some("admin".to_string());
        config.password = Some("secret123".to_string());

        let conn_str = driver.build_connection_string(&config);
        assert!(conn_str.starts_with("mongodb://admin:secret123@localhost:27017/mydb"));
        assert!(conn_str.contains("authSource=admin"));
    }

    #[test]
    fn test_connection_string_with_special_chars_in_password() {
        let driver = MongoDbDriver::new();
        let mut config = ConnectionConfig::new("mongodb", "Test");
        config.host = "localhost".to_string();
        config.port = 27017;
        config.database = Some("mydb".to_string());
        config.username = Some("admin".to_string());
        config.password = Some("p@ss:word".to_string());

        let conn_str = driver.build_connection_string(&config);
        // Password should be URL encoded
        assert!(conn_str.contains("p%40ss%3Aword"));
    }

    #[test]
    fn test_connection_string_default_port() {
        let driver = MongoDbDriver::new();
        let mut config = ConnectionConfig::new("mongodb", "Test");
        config.host = "localhost".to_string();
        // port = 0 should default to 27017

        let conn_str = driver.build_connection_string(&config);
        assert!(conn_str.contains(":27017/"));
    }

    #[test]
    fn test_connection_string_default_database() {
        let driver = MongoDbDriver::new();
        let mut config = ConnectionConfig::new("mongodb", "Test");
        config.host = "localhost".to_string();
        config.port = 27017;
        // No database specified should default to admin

        let conn_str = driver.build_connection_string(&config);
        assert!(conn_str.contains("/admin"));
    }

    #[test]
    fn test_connection_string_with_replica_set() {
        let driver = MongoDbDriver::new();
        let mut config = ConnectionConfig::new("mongodb", "Test");
        config.host = "localhost".to_string();
        config.port = 27017;
        config.database = Some("mydb".to_string());
        config
            .params
            .insert("replicaSet".to_string(), "rs0".to_string());

        let conn_str = driver.build_connection_string(&config);
        assert!(conn_str.contains("replicaSet=rs0"));
    }

    #[test]
    fn test_connection_string_with_tls() {
        let driver = MongoDbDriver::new();
        let mut config = ConnectionConfig::new("mongodb", "Test");
        config.host = "localhost".to_string();
        config.port = 27017;
        config.database = Some("mydb".to_string());
        config.params.insert("tls".to_string(), "true".to_string());

        let conn_str = driver.build_connection_string(&config);
        assert!(conn_str.contains("tls=true"));
    }

    #[test]
    fn test_connection_string_with_auth_source() {
        let driver = MongoDbDriver::new();
        let mut config = ConnectionConfig::new("mongodb", "Test");
        config.host = "localhost".to_string();
        config.port = 27017;
        config.database = Some("mydb".to_string());
        config.username = Some("admin".to_string());
        config.password = Some("pass".to_string());
        config
            .params
            .insert("authSource".to_string(), "users".to_string());

        let conn_str = driver.build_connection_string(&config);
        assert!(conn_str.contains("authSource=users"));
    }

    #[test]
    fn test_connection_string_help() {
        let driver = MongoDbDriver::new();
        let help = driver.connection_string_help();
        assert!(help.contains("mongodb://"));
        assert!(help.contains("host"));
        assert!(help.contains("Examples"));
    }
}

// ============================================================================
// Dialect Tests
// ============================================================================

mod dialect_tests {
    use super::*;

    #[test]
    fn test_mongodb_dialect_id() {
        let dialect = mongodb_dialect();
        assert_eq!(dialect.id.as_ref(), "mongodb");
        assert_eq!(dialect.display_name.as_ref(), "MongoDB Query Language");
    }

    #[test]
    fn test_mongodb_dialect_has_query_operators() {
        let dialect = mongodb_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();

        assert!(keyword_names.contains(&"$eq"));
        assert!(keyword_names.contains(&"$ne"));
        assert!(keyword_names.contains(&"$gt"));
        assert!(keyword_names.contains(&"$lt"));
        assert!(keyword_names.contains(&"$in"));
        assert!(keyword_names.contains(&"$and"));
        assert!(keyword_names.contains(&"$or"));
        assert!(keyword_names.contains(&"$regex"));
    }

    #[test]
    fn test_mongodb_dialect_has_update_operators() {
        let dialect = mongodb_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();

        assert!(keyword_names.contains(&"$set"));
        assert!(keyword_names.contains(&"$unset"));
        assert!(keyword_names.contains(&"$inc"));
        assert!(keyword_names.contains(&"$push"));
        assert!(keyword_names.contains(&"$pull"));
        assert!(keyword_names.contains(&"$addToSet"));
    }

    #[test]
    fn test_mongodb_dialect_has_aggregation_stages() {
        let dialect = mongodb_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();

        assert!(keyword_names.contains(&"$match"));
        assert!(keyword_names.contains(&"$project"));
        assert!(keyword_names.contains(&"$group"));
        assert!(keyword_names.contains(&"$lookup"));
        assert!(keyword_names.contains(&"$unwind"));
    }

    #[test]
    fn test_mongodb_dialect_has_commands() {
        let dialect = mongodb_dialect();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();

        assert!(keyword_names.contains(&"find"));
        assert!(keyword_names.contains(&"insert"));
        assert!(keyword_names.contains(&"update"));
        assert!(keyword_names.contains(&"delete"));
        assert!(keyword_names.contains(&"aggregate"));
        assert!(keyword_names.contains(&"createIndex"));
    }

    #[test]
    fn test_mongodb_dialect_keyword_count() {
        let dialect = mongodb_dialect();
        // Should have a good number of keywords
        assert!(
            dialect.keywords.len() >= 70,
            "Expected at least 70 keywords, got {}",
            dialect.keywords.len()
        );
    }
}

// ============================================================================
// Function Tests
// ============================================================================

mod function_tests {
    use super::*;

    #[test]
    fn test_mongodb_dialect_has_aggregate_functions() {
        let dialect = mongodb_dialect();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"$sum"));
        assert!(func_names.contains(&"$avg"));
        assert!(func_names.contains(&"$min"));
        assert!(func_names.contains(&"$max"));
        assert!(func_names.contains(&"$first"));
        assert!(func_names.contains(&"$last"));
    }

    #[test]
    fn test_mongodb_dialect_has_string_functions() {
        let dialect = mongodb_dialect();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"$concat"));
        assert!(func_names.contains(&"$substr"));
        assert!(func_names.contains(&"$toLower"));
        assert!(func_names.contains(&"$toUpper"));
    }

    #[test]
    fn test_mongodb_dialect_has_date_functions() {
        let dialect = mongodb_dialect();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"$dateToString"));
        assert!(func_names.contains(&"$dateFromString"));
        assert!(func_names.contains(&"$year"));
        assert!(func_names.contains(&"$month"));
        assert!(func_names.contains(&"$dayOfMonth"));
    }

    #[test]
    fn test_mongodb_dialect_has_math_functions() {
        let dialect = mongodb_dialect();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"$abs"));
        assert!(func_names.contains(&"$ceil"));
        assert!(func_names.contains(&"$floor"));
        assert!(func_names.contains(&"$round"));
        assert!(func_names.contains(&"$sqrt"));
    }

    #[test]
    fn test_mongodb_dialect_has_array_functions() {
        let dialect = mongodb_dialect();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"$arrayElemAt"));
        assert!(func_names.contains(&"$filter"));
        assert!(func_names.contains(&"$map"));
        assert!(func_names.contains(&"$reduce"));
    }

    #[test]
    fn test_mongodb_dialect_has_conversion_functions() {
        let dialect = mongodb_dialect();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"$toInt"));
        assert!(func_names.contains(&"$toString"));
        assert!(func_names.contains(&"$toDate"));
        assert!(func_names.contains(&"$type"));
    }

    #[test]
    fn test_mongodb_dialect_function_count() {
        let dialect = mongodb_dialect();
        // Should have a good number of functions
        assert!(
            dialect.functions.len() >= 60,
            "Expected at least 60 functions, got {}",
            dialect.functions.len()
        );
    }

    #[test]
    fn test_mongodb_dialect_function_categories() {
        let dialect = mongodb_dialect();

        let aggregate_count = dialect
            .functions
            .iter()
            .filter(|f| matches!(f.category, FunctionCategory::Aggregate))
            .count();
        let string_count = dialect
            .functions
            .iter()
            .filter(|f| matches!(f.category, FunctionCategory::String))
            .count();
        let datetime_count = dialect
            .functions
            .iter()
            .filter(|f| matches!(f.category, FunctionCategory::DateTime))
            .count();

        assert!(
            aggregate_count >= 8,
            "Expected at least 8 aggregate functions"
        );
        assert!(string_count >= 8, "Expected at least 8 string functions");
        assert!(
            datetime_count >= 10,
            "Expected at least 10 datetime functions"
        );
    }
}

// ============================================================================
// Data Type Tests
// ============================================================================

mod data_type_tests {
    use super::*;

    #[test]
    fn test_mongodb_dialect_has_bson_types() {
        let dialect = mongodb_dialect();
        let type_names: Vec<&str> = dialect.data_types.iter().map(|t| t.name.as_ref()).collect();

        assert!(type_names.contains(&"Double"));
        assert!(type_names.contains(&"String"));
        assert!(type_names.contains(&"Object"));
        assert!(type_names.contains(&"Array"));
        assert!(type_names.contains(&"ObjectId"));
        assert!(type_names.contains(&"Boolean"));
        assert!(type_names.contains(&"Date"));
        assert!(type_names.contains(&"Int32"));
        assert!(type_names.contains(&"Int64"));
        assert!(type_names.contains(&"Decimal128"));
    }

    #[test]
    fn test_mongodb_dialect_type_count() {
        let dialect = mongodb_dialect();
        // Should have all BSON types
        assert!(
            dialect.data_types.len() >= 15,
            "Expected at least 15 types, got {}",
            dialect.data_types.len()
        );
    }

    #[test]
    fn test_mongodb_dialect_type_categories() {
        let dialect = mongodb_dialect();

        // Count integer types (Int32, Int64)
        let integer_count = dialect
            .data_types
            .iter()
            .filter(|t| matches!(t.category, DataTypeCategory::Integer))
            .count();

        // Count float types (Double)
        let float_count = dialect
            .data_types
            .iter()
            .filter(|t| matches!(t.category, DataTypeCategory::Float))
            .count();

        // Count decimal types (Decimal128)
        let decimal_count = dialect
            .data_types
            .iter()
            .filter(|t| matches!(t.category, DataTypeCategory::Decimal))
            .count();

        assert!(
            integer_count >= 2,
            "Expected at least 2 integer types (Int32, Int64)"
        );
        assert!(float_count >= 1, "Expected at least 1 float type (Double)");
        assert!(
            decimal_count >= 1,
            "Expected at least 1 decimal type (Decimal128)"
        );
    }
}

// ============================================================================
// Comment Style Tests
// ============================================================================

mod comment_style_tests {
    use super::*;

    #[test]
    fn test_mongodb_dialect_comment_styles() {
        let dialect = mongodb_dialect();

        assert_eq!(dialect.comment_styles.line_comment.as_deref(), Some("//"));
        assert_eq!(
            dialect.comment_styles.block_comment_start.as_deref(),
            Some("/*")
        );
        assert_eq!(
            dialect.comment_styles.block_comment_end.as_deref(),
            Some("*/")
        );
    }
}

// ============================================================================
// Quote Style Tests
// ============================================================================

mod quote_style_tests {
    use super::*;

    #[test]
    fn test_mongodb_dialect_quote_chars() {
        let dialect = mongodb_dialect();

        assert_eq!(dialect.identifier_quote, '"');
        assert_eq!(dialect.string_quote, '"');
    }

    #[test]
    fn test_mongodb_dialect_case_sensitivity() {
        let dialect = mongodb_dialect();
        assert!(
            dialect.case_sensitive_identifiers,
            "MongoDB field names are case-sensitive"
        );
    }
}

// ============================================================================
// URL Encoding Tests
// ============================================================================

mod urlencoding_tests {
    use crate::driver::urlencoding;

    #[test]
    fn test_urlencoding_simple() {
        assert_eq!(urlencoding::encode("hello"), "hello");
    }

    #[test]
    fn test_urlencoding_special_chars() {
        assert_eq!(urlencoding::encode("hello world"), "hello%20world");
        assert_eq!(urlencoding::encode("user@host"), "user%40host");
        assert_eq!(urlencoding::encode("pass:word"), "pass%3Aword");
        assert_eq!(urlencoding::encode("a/b"), "a%2Fb");
    }

    #[test]
    fn test_urlencoding_safe_chars() {
        assert_eq!(urlencoding::encode("hello-world"), "hello-world");
        assert_eq!(urlencoding::encode("hello_world"), "hello_world");
        assert_eq!(urlencoding::encode("hello.world"), "hello.world");
        assert_eq!(urlencoding::encode("hello~world"), "hello~world");
    }
}

// ============================================================================
// Connection Tests (async)
// ============================================================================

mod connection_tests {
    use super::*;

    #[test]
    fn test_mongodb_connection_database() {
        // This is a unit test that doesn't require actual MongoDB
        // We can't easily construct a MongoDbConnection without a real client
        // So we just verify the driver interface
        let driver = MongoDbDriver::new();
        assert_eq!(driver.id(), "mongodb");
    }

    // Integration tests would require a running MongoDB instance
    // They should be in tests/connection_integration.rs
}

// ============================================================================
// Driver Info from Dialect Tests
// ============================================================================

mod driver_dialect_integration_tests {
    use super::*;

    #[test]
    fn test_driver_returns_correct_dialect() {
        let driver = MongoDbDriver::new();
        let dialect = driver.dialect_info();

        assert_eq!(dialect.id.as_ref(), "mongodb");
    }

    #[test]
    fn test_dialect_completeness() {
        let dialect = mongodb_dialect();

        // Verify all essential components are present
        assert!(!dialect.keywords.is_empty(), "Should have keywords");
        assert!(!dialect.functions.is_empty(), "Should have functions");
        assert!(!dialect.data_types.is_empty(), "Should have data types");
        assert!(
            dialect.comment_styles.line_comment.is_some(),
            "Should have line comment style"
        );
    }
}
