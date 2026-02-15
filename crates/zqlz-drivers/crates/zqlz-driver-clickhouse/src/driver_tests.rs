//! Unit tests for ClickHouse driver

use super::*;
use zqlz_core::{ConnectionConfig, DatabaseDriver, Value};

mod driver_metadata_tests {
    use super::*;

    #[test]
    fn test_clickhouse_driver_id() {
        let driver = ClickHouseDriver::new();
        assert_eq!(driver.id(), "clickhouse");
    }

    #[test]
    fn test_clickhouse_driver_name() {
        let driver = ClickHouseDriver::new();
        assert_eq!(driver.name(), "clickhouse");
        assert_eq!(driver.display_name(), "ClickHouse");
    }

    #[test]
    fn test_clickhouse_default_port() {
        let driver = ClickHouseDriver::new();
        assert_eq!(driver.default_port(), Some(8123));
    }

    #[test]
    fn test_clickhouse_version() {
        let driver = ClickHouseDriver::new();
        assert_eq!(driver.version(), "0.1.0");
    }

    #[test]
    fn test_clickhouse_icon_name() {
        let driver = ClickHouseDriver::new();
        assert_eq!(driver.icon_name(), "clickhouse");
    }

    #[test]
    fn test_clickhouse_default() {
        let driver = ClickHouseDriver::default();
        assert_eq!(driver.id(), "clickhouse");
    }
}

mod driver_capabilities_tests {
    use super::*;

    #[test]
    fn test_clickhouse_capabilities_transactions() {
        let driver = ClickHouseDriver::new();
        let caps = driver.capabilities();
        assert!(!caps.supports_transactions); // ClickHouse has limited transaction support
        assert!(!caps.supports_savepoints);
    }

    #[test]
    fn test_clickhouse_capabilities_olap_features() {
        let driver = ClickHouseDriver::new();
        let caps = driver.capabilities();
        assert!(caps.supports_window_functions);
        assert!(caps.supports_cte);
        assert!(caps.supports_json);
        assert!(caps.supports_streaming);
    }

    #[test]
    fn test_clickhouse_capabilities_ddl() {
        let driver = ClickHouseDriver::new();
        let caps = driver.capabilities();
        assert!(caps.supports_views);
        assert!(caps.supports_schemas);
        assert!(caps.supports_multiple_databases);
        assert!(!caps.supports_foreign_keys); // ClickHouse doesn't have foreign keys
        assert!(!caps.supports_triggers); // ClickHouse doesn't have triggers
    }

    #[test]
    fn test_clickhouse_capabilities_other() {
        let driver = ClickHouseDriver::new();
        let caps = driver.capabilities();
        assert!(caps.supports_prepared_statements);
        assert!(caps.supports_multiple_statements);
        assert!(caps.supports_upsert);
        assert!(caps.supports_ssl);
        assert!(caps.supports_explain);
        assert!(caps.supports_cancellation);
        assert!(!caps.supports_stored_procedures);
    }
}

mod connection_string_tests {
    use super::*;

    #[test]
    fn test_connection_string_defaults() {
        let driver = ClickHouseDriver::new();
        let config = ConnectionConfig::new("clickhouse", "ClickHouse");
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "http://localhost:8123/default");
    }

    #[test]
    fn test_connection_string_with_host() {
        let driver = ClickHouseDriver::new();
        let mut config = ConnectionConfig::new("clickhouse", "ClickHouse");
        config.host = "clickhouse.example.com".to_string();
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "http://clickhouse.example.com:8123/default");
    }

    #[test]
    fn test_connection_string_with_port() {
        let driver = ClickHouseDriver::new();
        let mut config = ConnectionConfig::new("clickhouse", "ClickHouse");
        config.port = 9000;
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "http://localhost:9000/default");
    }

    #[test]
    fn test_connection_string_with_database() {
        let driver = ClickHouseDriver::new();
        let mut config = ConnectionConfig::new("clickhouse", "ClickHouse");
        config.database = Some("analytics".to_string());
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "http://localhost:8123/analytics");
    }

    #[test]
    fn test_connection_string_with_username() {
        let driver = ClickHouseDriver::new();
        let mut config = ConnectionConfig::new("clickhouse", "ClickHouse");
        config.username = Some("admin".to_string());
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "http://admin@localhost:8123/default");
    }

    #[test]
    fn test_connection_string_with_password() {
        let driver = ClickHouseDriver::new();
        let mut config = ConnectionConfig::new("clickhouse", "ClickHouse");
        config.username = Some("admin".to_string());
        config.password = Some("secret".to_string());
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(conn_str, "http://admin:secret@localhost:8123/default");
    }

    #[test]
    fn test_connection_string_with_ssl() {
        let driver = ClickHouseDriver::new();
        let mut config = ConnectionConfig::new("clickhouse", "ClickHouse");
        config.params.insert("ssl".to_string(), "true".to_string());
        let conn_str = driver.build_connection_string(&config);
        assert!(conn_str.starts_with("https://"));
    }

    #[test]
    fn test_connection_string_full() {
        let driver = ClickHouseDriver::new();
        let mut config = ConnectionConfig::new("clickhouse", "ClickHouse");
        config.host = "ch.example.com".to_string();
        config.port = 8443;
        config.database = Some("events".to_string());
        config.username = Some("readonly".to_string());
        config.password = Some("password123".to_string());
        config.params.insert("ssl".to_string(), "true".to_string());
        let conn_str = driver.build_connection_string(&config);
        assert_eq!(
            conn_str,
            "https://readonly:password123@ch.example.com:8443/events"
        );
    }

    #[test]
    fn test_connection_string_help() {
        let driver = ClickHouseDriver::new();
        let help = driver.connection_string_help();
        assert!(help.contains("host"));
        assert!(help.contains("port"));
        assert!(help.contains("8123"));
    }
}

mod dialect_tests {
    use super::*;

    #[test]
    fn test_dialect_info_basic() {
        let dialect = clickhouse_dialect();
        assert_eq!(dialect.id, "clickhouse");
        assert_eq!(dialect.display_name, "ClickHouse SQL");
        assert_eq!(dialect.identifier_quote, '`');
        assert_eq!(dialect.string_quote, '\'');
        assert!(dialect.case_sensitive_identifiers);
    }

    #[test]
    fn test_dialect_auto_increment() {
        let dialect = clickhouse_dialect();
        // ClickHouse doesn't have traditional auto-increment
        assert!(dialect.auto_increment.is_none());
    }

    #[test]
    fn test_dialect_explain_config() {
        let dialect = clickhouse_dialect();
        assert!(dialect.explain_config.explain_format.contains("EXPLAIN"));
        assert!(dialect.explain_config.query_plan_format.is_some());
        assert!(dialect.explain_config.analyze_format.is_some());
        assert!(!dialect.explain_config.analyze_is_safe);
    }

    #[test]
    fn test_dialect_table_options() {
        let dialect = clickhouse_dialect();
        let option_keys: Vec<&str> = dialect
            .table_options
            .iter()
            .map(|o| o.key.as_ref())
            .collect();
        assert!(option_keys.contains(&"engine"));
        assert!(option_keys.contains(&"order_by"));
        assert!(option_keys.contains(&"partition_by"));
        assert!(option_keys.contains(&"ttl"));
        assert!(option_keys.contains(&"settings"));
    }

    #[test]
    fn test_dialect_table_option_engines() {
        let dialect = clickhouse_dialect();
        let engine_option = dialect
            .table_options
            .iter()
            .find(|o| o.key == "engine")
            .unwrap();
        assert!(
            engine_option
                .choices
                .iter()
                .any(|c| c.as_ref() == "MergeTree")
        );
        assert!(
            engine_option
                .choices
                .iter()
                .any(|c| c.as_ref() == "ReplacingMergeTree")
        );
        assert!(
            engine_option
                .choices
                .iter()
                .any(|c| c.as_ref() == "Distributed")
        );
    }
}

mod keyword_tests {
    use super::*;

    #[test]
    fn test_keywords_contains_standard_sql() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();

        assert!(keyword_names.contains(&"SELECT"));
        assert!(keyword_names.contains(&"FROM"));
        assert!(keyword_names.contains(&"WHERE"));
        assert!(keyword_names.contains(&"INSERT"));
        assert!(keyword_names.contains(&"UPDATE"));
        assert!(keyword_names.contains(&"DELETE"));
        assert!(keyword_names.contains(&"CREATE"));
        assert!(keyword_names.contains(&"DROP"));
    }

    #[test]
    fn test_keywords_contains_clickhouse_specific() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();

        assert!(keyword_names.contains(&"FINAL"));
        assert!(keyword_names.contains(&"SAMPLE"));
        assert!(keyword_names.contains(&"PREWHERE"));
        assert!(keyword_names.contains(&"ENGINE"));
        assert!(keyword_names.contains(&"OPTIMIZE"));
        assert!(keyword_names.contains(&"FORMAT"));
        assert!(keyword_names.contains(&"SETTINGS"));
        assert!(keyword_names.contains(&"TTL"));
    }

    #[test]
    fn test_keywords_contains_table_engines() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();

        assert!(keyword_names.contains(&"MergeTree"));
        assert!(keyword_names.contains(&"ReplacingMergeTree"));
        assert!(keyword_names.contains(&"SummingMergeTree"));
        assert!(keyword_names.contains(&"AggregatingMergeTree"));
        assert!(keyword_names.contains(&"CollapsingMergeTree"));
        assert!(keyword_names.contains(&"Distributed"));
        assert!(keyword_names.contains(&"Kafka"));
        assert!(keyword_names.contains(&"Memory"));
    }

    #[test]
    fn test_keywords_contains_join_modifiers() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let keyword_names: Vec<&str> = dialect
            .keywords
            .iter()
            .map(|k| k.keyword.as_ref())
            .collect();

        assert!(keyword_names.contains(&"GLOBAL"));
        assert!(keyword_names.contains(&"ANY"));
        assert!(keyword_names.contains(&"ASOF"));
    }
}

mod function_tests {
    use super::*;

    #[test]
    fn test_functions_contains_standard_aggregates() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"count"));
        assert!(func_names.contains(&"sum"));
        assert!(func_names.contains(&"avg"));
        assert!(func_names.contains(&"min"));
        assert!(func_names.contains(&"max"));
    }

    #[test]
    fn test_functions_contains_clickhouse_aggregates() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"any"));
        assert!(func_names.contains(&"anyLast"));
        assert!(func_names.contains(&"argMin"));
        assert!(func_names.contains(&"argMax"));
        assert!(func_names.contains(&"groupArray"));
        assert!(func_names.contains(&"uniq"));
        assert!(func_names.contains(&"uniqExact"));
        assert!(func_names.contains(&"quantile"));
        assert!(func_names.contains(&"topK"));
    }

    #[test]
    fn test_functions_contains_window_functions() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"row_number"));
        assert!(func_names.contains(&"rank"));
        assert!(func_names.contains(&"dense_rank"));
        assert!(func_names.contains(&"lag"));
        assert!(func_names.contains(&"lead"));
    }

    #[test]
    fn test_functions_contains_date_functions() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"now"));
        assert!(func_names.contains(&"today"));
        assert!(func_names.contains(&"toYear"));
        assert!(func_names.contains(&"toMonth"));
        assert!(func_names.contains(&"toStartOfDay"));
        assert!(func_names.contains(&"dateDiff"));
        assert!(func_names.contains(&"formatDateTime"));
    }

    #[test]
    fn test_functions_contains_json_functions() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"JSONExtract"));
        assert!(func_names.contains(&"JSONExtractString"));
        assert!(func_names.contains(&"JSONExtractInt"));
        assert!(func_names.contains(&"JSONHas"));
        assert!(func_names.contains(&"JSONLength"));
    }

    #[test]
    fn test_functions_contains_array_functions() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"array"));
        assert!(func_names.contains(&"arrayConcat"));
        assert!(func_names.contains(&"arrayJoin"));
        assert!(func_names.contains(&"arrayMap"));
        assert!(func_names.contains(&"arrayFilter"));
        assert!(func_names.contains(&"arraySort"));
    }

    #[test]
    fn test_functions_contains_conversion_functions() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"toInt32"));
        assert!(func_names.contains(&"toInt64"));
        assert!(func_names.contains(&"toFloat64"));
        assert!(func_names.contains(&"toString"));
        assert!(func_names.contains(&"toDate"));
        assert!(func_names.contains(&"toDateTime"));
        assert!(func_names.contains(&"cast"));
    }

    #[test]
    fn test_functions_contains_hash_functions() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"cityHash64"));
        assert!(func_names.contains(&"sipHash64"));
        assert!(func_names.contains(&"MD5"));
        assert!(func_names.contains(&"SHA256"));
        assert!(func_names.contains(&"xxHash64"));
    }

    #[test]
    fn test_functions_contains_url_functions() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let func_names: Vec<&str> = dialect.functions.iter().map(|f| f.name.as_ref()).collect();

        assert!(func_names.contains(&"domain"));
        assert!(func_names.contains(&"protocol"));
        assert!(func_names.contains(&"path"));
        assert!(func_names.contains(&"queryString"));
        assert!(func_names.contains(&"extractURLParameter"));
    }
}

mod data_type_tests {
    use super::*;

    #[test]
    fn test_data_types_contains_integers() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let type_names: Vec<&str> = dialect.data_types.iter().map(|t| t.name.as_ref()).collect();

        assert!(type_names.contains(&"Int8"));
        assert!(type_names.contains(&"Int16"));
        assert!(type_names.contains(&"Int32"));
        assert!(type_names.contains(&"Int64"));
        assert!(type_names.contains(&"Int128"));
        assert!(type_names.contains(&"Int256"));
        assert!(type_names.contains(&"UInt8"));
        assert!(type_names.contains(&"UInt16"));
        assert!(type_names.contains(&"UInt32"));
        assert!(type_names.contains(&"UInt64"));
    }

    #[test]
    fn test_data_types_contains_floats() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let type_names: Vec<&str> = dialect.data_types.iter().map(|t| t.name.as_ref()).collect();

        assert!(type_names.contains(&"Float32"));
        assert!(type_names.contains(&"Float64"));
    }

    #[test]
    fn test_data_types_contains_decimals() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let type_names: Vec<&str> = dialect.data_types.iter().map(|t| t.name.as_ref()).collect();

        assert!(type_names.contains(&"Decimal"));
        assert!(type_names.contains(&"Decimal32"));
        assert!(type_names.contains(&"Decimal64"));
        assert!(type_names.contains(&"Decimal128"));
        assert!(type_names.contains(&"Decimal256"));
    }

    #[test]
    fn test_data_types_contains_datetime() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let type_names: Vec<&str> = dialect.data_types.iter().map(|t| t.name.as_ref()).collect();

        assert!(type_names.contains(&"Date"));
        assert!(type_names.contains(&"Date32"));
        assert!(type_names.contains(&"DateTime"));
        assert!(type_names.contains(&"DateTime64"));
    }

    #[test]
    fn test_data_types_contains_string() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let type_names: Vec<&str> = dialect.data_types.iter().map(|t| t.name.as_ref()).collect();

        assert!(type_names.contains(&"String"));
        assert!(type_names.contains(&"FixedString"));
    }

    #[test]
    fn test_data_types_contains_complex() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let type_names: Vec<&str> = dialect.data_types.iter().map(|t| t.name.as_ref()).collect();

        assert!(type_names.contains(&"Array"));
        assert!(type_names.contains(&"Tuple"));
        assert!(type_names.contains(&"Map"));
        assert!(type_names.contains(&"Nested"));
        assert!(type_names.contains(&"Nullable"));
        assert!(type_names.contains(&"LowCardinality"));
    }

    #[test]
    fn test_data_types_contains_special() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let type_names: Vec<&str> = dialect.data_types.iter().map(|t| t.name.as_ref()).collect();

        assert!(type_names.contains(&"UUID"));
        assert!(type_names.contains(&"IPv4"));
        assert!(type_names.contains(&"IPv6"));
        assert!(type_names.contains(&"Enum8"));
        assert!(type_names.contains(&"Enum16"));
        assert!(type_names.contains(&"JSON"));
    }

    #[test]
    fn test_data_types_contains_geo() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let type_names: Vec<&str> = dialect.data_types.iter().map(|t| t.name.as_ref()).collect();

        assert!(type_names.contains(&"Point"));
        assert!(type_names.contains(&"Ring"));
        assert!(type_names.contains(&"Polygon"));
        assert!(type_names.contains(&"MultiPolygon"));
    }

    #[test]
    fn test_data_types_contains_aggregate() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        let type_names: Vec<&str> = dialect.data_types.iter().map(|t| t.name.as_ref()).collect();

        assert!(type_names.contains(&"SimpleAggregateFunction"));
        assert!(type_names.contains(&"AggregateFunction"));
    }
}

mod json_to_value_tests {
    use super::*;

    #[test]
    fn test_json_null() {
        let json = serde_json::Value::Null;
        let value = json_to_value(&json);
        assert!(matches!(value, Value::Null));
    }

    #[test]
    fn test_json_bool() {
        let json = serde_json::Value::Bool(true);
        let value = json_to_value(&json);
        assert!(matches!(value, Value::Bool(true)));

        let json = serde_json::Value::Bool(false);
        let value = json_to_value(&json);
        assert!(matches!(value, Value::Bool(false)));
    }

    #[test]
    fn test_json_integer() {
        let json = serde_json::json!(42);
        let value = json_to_value(&json);
        assert!(matches!(value, Value::Int64(42)));
    }

    #[test]
    fn test_json_float() {
        let json = serde_json::json!(3.14);
        let value = json_to_value(&json);
        if let Value::Float64(f) = value {
            assert!((f - 3.14).abs() < 0.001);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_json_string() {
        let json = serde_json::json!("hello");
        let value = json_to_value(&json);
        assert!(matches!(value, Value::String(s) if s == "hello"));
    }

    #[test]
    fn test_json_array() {
        let json = serde_json::json!([1, 2, 3]);
        let value = json_to_value(&json);
        if let Value::String(s) = value {
            assert_eq!(s, "[1,2,3]");
        } else {
            panic!("Expected String");
        }
    }

    #[test]
    fn test_json_object() {
        let json = serde_json::json!({"key": "value"});
        let value = json_to_value(&json);
        if let Value::String(s) = value {
            assert!(s.contains("key"));
            assert!(s.contains("value"));
        } else {
            panic!("Expected String");
        }
    }
}

mod connection_tests {
    use super::*;

    #[test]
    fn test_connection_debug() {
        let conn = ClickHouseConnection::new(clickhouse::Client::default(), "test_db".to_string());
        let debug_str = format!("{:?}", conn);
        assert!(debug_str.contains("ClickHouseConnection"));
        assert!(debug_str.contains("test_db"));
    }

    #[test]
    fn test_dialect_from_driver() {
        let driver = ClickHouseDriver::new();
        let dialect = driver.dialect_info();
        assert_eq!(dialect.id, "clickhouse");
    }

    #[test]
    fn test_connection_database() {
        let conn =
            ClickHouseConnection::new(clickhouse::Client::default(), "my_database".to_string());
        assert_eq!(conn.database(), "my_database");
    }
}
