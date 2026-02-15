//! Unit tests for DuckDB driver

use super::*;
use zqlz_core::{ConnectionConfig, DatabaseDriver};

#[test]
fn test_duckdb_driver_id() {
    let driver = DuckDbDriver::new();
    assert_eq!(driver.id(), "duckdb");
}

#[test]
fn test_duckdb_driver_name() {
    let driver = DuckDbDriver::new();
    assert_eq!(driver.name(), "duckdb");
    assert_eq!(driver.display_name(), "DuckDB");
}

#[test]
fn test_duckdb_default_port() {
    let driver = DuckDbDriver::new();
    // DuckDB is file-based, no network port
    assert_eq!(driver.default_port(), None);
}

#[test]
fn test_duckdb_capabilities() {
    let driver = DuckDbDriver::new();
    let caps = driver.capabilities();

    assert!(caps.supports_transactions);
    assert!(caps.supports_savepoints);
    assert!(caps.supports_prepared_statements);
    assert!(caps.supports_multiple_statements);
    assert!(caps.supports_returning);
    assert!(caps.supports_upsert);
    assert!(caps.supports_window_functions);
    assert!(caps.supports_cte);
    assert!(caps.supports_json);
    assert!(caps.supports_schemas);
    assert!(caps.supports_multiple_databases); // ATTACH
    assert!(caps.supports_explain);
    assert!(caps.supports_foreign_keys);
    assert!(caps.supports_views);
    assert!(!caps.supports_stored_procedures); // DuckDB doesn't support stored procedures
    assert!(!caps.supports_triggers); // DuckDB doesn't support triggers
    assert!(!caps.supports_ssl); // File-based
    assert_eq!(caps.max_identifier_length, Some(255));
}

#[test]
fn test_duckdb_dialect_info() {
    let driver = DuckDbDriver::new();
    let dialect = driver.dialect_info();

    assert_eq!(dialect.id.as_ref(), "duckdb");
    assert_eq!(dialect.display_name.as_ref(), "DuckDB SQL");
    assert_eq!(dialect.identifier_quote, '"');
    assert_eq!(dialect.string_quote, '\'');
    assert!(!dialect.case_sensitive_identifiers);
    assert_eq!(dialect.statement_terminator, ';');
}

#[test]
fn test_duckdb_keywords() {
    let driver = DuckDbDriver::new();
    let dialect = driver.dialect_info();

    let keyword_names: Vec<&str> = dialect.keyword_names().collect();

    // Standard SQL keywords
    assert!(keyword_names.contains(&"SELECT"));
    assert!(keyword_names.contains(&"FROM"));
    assert!(keyword_names.contains(&"WHERE"));
    assert!(keyword_names.contains(&"JOIN"));

    // DuckDB specific keywords
    assert!(keyword_names.contains(&"PIVOT")); // DuckDB pivot
    assert!(keyword_names.contains(&"UNPIVOT")); // DuckDB unpivot
    assert!(keyword_names.contains(&"SUMMARIZE")); // DuckDB statistics
    assert!(keyword_names.contains(&"ATTACH")); // DuckDB attach database
    assert!(keyword_names.contains(&"PRAGMA")); // DuckDB configuration
    assert!(keyword_names.contains(&"INSTALL")); // DuckDB extension install
    assert!(keyword_names.contains(&"SAMPLE")); // DuckDB sampling
    assert!(keyword_names.contains(&"QUALIFY")); // Window function filtering
}

#[test]
fn test_duckdb_functions() {
    let driver = DuckDbDriver::new();
    let dialect = driver.dialect_info();

    let function_names: Vec<&str> = dialect.function_names().collect();

    // Standard SQL functions
    assert!(function_names.contains(&"COUNT"));
    assert!(function_names.contains(&"SUM"));
    assert!(function_names.contains(&"AVG"));
    assert!(function_names.contains(&"MIN"));
    assert!(function_names.contains(&"MAX"));

    // Window functions
    assert!(function_names.contains(&"ROW_NUMBER"));
    assert!(function_names.contains(&"RANK"));
    assert!(function_names.contains(&"LAG"));
    assert!(function_names.contains(&"LEAD"));

    // DuckDB specific aggregate functions
    assert!(function_names.contains(&"FIRST"));
    assert!(function_names.contains(&"LAST"));
    assert!(function_names.contains(&"MEDIAN"));
    assert!(function_names.contains(&"MODE"));
    assert!(function_names.contains(&"APPROX_COUNT_DISTINCT"));
    assert!(function_names.contains(&"LIST"));
    assert!(function_names.contains(&"HISTOGRAM"));

    // DuckDB table functions
    assert!(function_names.contains(&"READ_CSV"));
    assert!(function_names.contains(&"READ_PARQUET"));
    assert!(function_names.contains(&"READ_JSON"));

    // JSON functions
    assert!(function_names.contains(&"JSON_EXTRACT"));
    assert!(function_names.contains(&"JSON_KEYS"));

    // List functions
    assert!(function_names.contains(&"UNNEST"));
    assert!(function_names.contains(&"LIST_VALUE"));
}

#[test]
fn test_duckdb_data_types() {
    let driver = DuckDbDriver::new();
    let dialect = driver.dialect_info();

    let type_names: Vec<&str> = dialect.data_type_names().collect();

    // Integer types
    assert!(type_names.contains(&"INTEGER"));
    assert!(type_names.contains(&"BIGINT"));
    assert!(type_names.contains(&"HUGEINT")); // DuckDB 128-bit integer
    assert!(type_names.contains(&"UTINYINT")); // DuckDB unsigned types
    assert!(type_names.contains(&"UBIGINT"));

    // String types
    assert!(type_names.contains(&"VARCHAR"));
    assert!(type_names.contains(&"TEXT"));
    assert!(type_names.contains(&"STRING")); // DuckDB alias

    // Date/time types
    assert!(type_names.contains(&"TIMESTAMP"));
    assert!(type_names.contains(&"TIMESTAMPTZ"));
    assert!(type_names.contains(&"INTERVAL"));

    // Complex types (DuckDB specific)
    assert!(type_names.contains(&"LIST"));
    assert!(type_names.contains(&"STRUCT"));
    assert!(type_names.contains(&"MAP"));
    assert!(type_names.contains(&"UNION"));

    // JSON
    assert!(type_names.contains(&"JSON"));

    // UUID
    assert!(type_names.contains(&"UUID"));
}

#[test]
fn test_duckdb_explain_config() {
    let driver = DuckDbDriver::new();
    let dialect = driver.dialect_info();
    let explain = &dialect.explain_config;

    assert!(explain.explain_format.contains("EXPLAIN"));
    assert!(explain.query_plan_format.is_some());
    assert!(explain.analyze_format.is_some());

    let formatted = explain.format_explain("SELECT 1");
    assert!(formatted.contains("SELECT 1"));
    assert!(formatted.contains("EXPLAIN"));
}

#[test]
fn test_duckdb_connection_string() {
    let driver = DuckDbDriver::new();

    let mut config = ConnectionConfig::new("duckdb", "Test Connection");
    config.database = Some("/path/to/test.duckdb".to_string());

    let conn_str = driver.build_connection_string(&config);
    assert_eq!(conn_str, "/path/to/test.duckdb");
}

#[test]
fn test_duckdb_memory_connection_string() {
    let driver = DuckDbDriver::new();

    let config = ConnectionConfig::new("duckdb", "Memory Database");
    // No database path means in-memory

    let conn_str = driver.build_connection_string(&config);
    assert_eq!(conn_str, ":memory:");
}

#[test]
fn test_duckdb_connection_string_from_path_param() {
    let driver = DuckDbDriver::new();

    let config = ConnectionConfig::new("duckdb", "Test Connection")
        .with_param("path", "/data/analytics.duckdb");

    let conn_str = driver.build_connection_string(&config);
    assert_eq!(conn_str, "/data/analytics.duckdb");
}

#[test]
fn test_duckdb_aggregate_functions() {
    let driver = DuckDbDriver::new();
    let dialect = driver.dialect_info();

    assert!(dialect.is_aggregate_function("COUNT"));
    assert!(dialect.is_aggregate_function("SUM"));
    assert!(dialect.is_aggregate_function("AVG"));
    assert!(dialect.is_aggregate_function("MIN"));
    assert!(dialect.is_aggregate_function("MAX"));
    assert!(dialect.is_aggregate_function("STRING_AGG"));
    assert!(dialect.is_aggregate_function("FIRST")); // DuckDB specific
    assert!(dialect.is_aggregate_function("LAST")); // DuckDB specific
    assert!(dialect.is_aggregate_function("MEDIAN")); // DuckDB specific
    assert!(dialect.is_aggregate_function("LIST")); // DuckDB specific
    assert!(!dialect.is_aggregate_function("LENGTH"));
    assert!(!dialect.is_aggregate_function("NOW"));
}

#[test]
fn test_duckdb_version() {
    let driver = DuckDbDriver::new();
    assert_eq!(driver.version(), "0.1.0");
}

#[test]
fn test_duckdb_icon_name() {
    let driver = DuckDbDriver::new();
    assert_eq!(driver.icon_name(), "duckdb");
}

#[test]
fn test_duckdb_connection_string_help() {
    let driver = DuckDbDriver::new();
    let help = driver.connection_string_help();
    assert!(help.contains("memory"));
    assert!(help.contains(".duckdb"));
}

#[test]
fn test_duckdb_connection_is_memory() {
    let conn = DuckDbConnection::new(
        duckdb::Connection::open_in_memory().unwrap(),
        ":memory:".to_string(),
    );
    assert!(conn.is_memory());
    assert_eq!(conn.path(), ":memory:");
}

#[test]
fn test_duckdb_connection_not_memory() {
    // Create a temp file path (don't actually open it)
    let conn = DuckDbConnection::new(
        duckdb::Connection::open_in_memory().unwrap(),
        "/tmp/test.duckdb".to_string(),
    );
    assert!(!conn.is_memory());
    assert_eq!(conn.path(), "/tmp/test.duckdb");
}

#[test]
fn test_duckdb_default_driver() {
    let driver = DuckDbDriver::default();
    assert_eq!(driver.id(), "duckdb");
}

#[tokio::test]
async fn test_duckdb_connect_memory() {
    let driver = DuckDbDriver::new();
    let config = ConnectionConfig::new("duckdb", "Memory Test");

    let result = driver.connect(&config).await;
    assert!(result.is_ok(), "Should connect to in-memory database");
}

#[tokio::test]
async fn test_duckdb_test_connection() {
    let driver = DuckDbDriver::new();
    let config = ConnectionConfig::new("duckdb", "Test Connection");

    let result = driver.test_connection(&config).await;
    assert!(result.is_ok(), "Should test connection successfully");
}

#[tokio::test]
async fn test_duckdb_execute_query() {
    let driver = DuckDbDriver::new();
    let config = ConnectionConfig::new("duckdb", "Test");

    let conn = driver.connect(&config).await.unwrap();

    // Create a table
    let result = conn
        .execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)", &[])
        .await;
    assert!(result.is_ok());

    // Insert a row
    let result = conn
        .execute("INSERT INTO test_table VALUES (1, 'test')", &[])
        .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap().affected_rows, 1);

    // Query the table
    let result = conn.query("SELECT * FROM test_table", &[]).await;
    assert!(result.is_ok());

    let query_result = result.unwrap();
    assert_eq!(query_result.columns.len(), 2);
    assert_eq!(query_result.columns[0].name, "id");
    assert_eq!(query_result.columns[1].name, "name");
}
