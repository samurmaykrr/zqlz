//! Unit tests for MS SQL Server driver

use super::*;
use zqlz_core::{AutoIncrementStyle, ConnectionConfig, DatabaseDriver};

#[test]
fn test_mssql_driver_id() {
    let driver = MssqlDriver::new();
    assert_eq!(driver.id(), "mssql");
}

#[test]
fn test_mssql_driver_name() {
    let driver = MssqlDriver::new();
    assert_eq!(driver.name(), "mssql");
    assert_eq!(driver.display_name(), "MS SQL Server");
}

#[test]
fn test_mssql_default_port() {
    let driver = MssqlDriver::new();
    assert_eq!(driver.default_port(), Some(1433));
}

#[test]
fn test_mssql_capabilities() {
    let driver = MssqlDriver::new();
    let caps = driver.capabilities();

    assert!(caps.supports_transactions);
    assert!(caps.supports_savepoints);
    assert!(caps.supports_prepared_statements);
    assert!(caps.supports_multiple_statements);
    assert!(caps.supports_returning); // OUTPUT clause
    assert!(caps.supports_upsert); // MERGE
    assert!(caps.supports_window_functions);
    assert!(caps.supports_cte);
    assert!(caps.supports_json);
    assert!(caps.supports_stored_procedures);
    assert!(caps.supports_schemas);
    assert!(caps.supports_multiple_databases);
    assert!(caps.supports_explain);
    assert!(caps.supports_foreign_keys);
    assert!(caps.supports_views);
    assert!(caps.supports_triggers);
    assert!(caps.supports_ssl);
    assert_eq!(caps.max_identifier_length, Some(128));
    assert_eq!(caps.max_parameters, Some(2100));
}

#[test]
fn test_mssql_dialect_info() {
    let driver = MssqlDriver::new();
    let dialect = driver.dialect_info();

    assert_eq!(dialect.id.as_ref(), "mssql");
    assert_eq!(dialect.display_name.as_ref(), "T-SQL");
    assert_eq!(dialect.identifier_quote, '[');
    assert_eq!(dialect.string_quote, '\'');
    assert!(!dialect.case_sensitive_identifiers);
    assert_eq!(dialect.statement_terminator, ';');
}

#[test]
fn test_mssql_auto_increment() {
    let driver = MssqlDriver::new();
    let dialect = driver.dialect_info();

    let auto_inc = dialect
        .auto_increment
        .as_ref()
        .expect("should have auto_increment");
    assert_eq!(auto_inc.keyword.as_ref(), "IDENTITY");
    assert!(matches!(auto_inc.style, AutoIncrementStyle::Suffix));
}

#[test]
fn test_mssql_keywords() {
    let driver = MssqlDriver::new();
    let dialect = driver.dialect_info();

    let keyword_names: Vec<&str> = dialect.keyword_names().collect();

    assert!(keyword_names.contains(&"SELECT"));
    assert!(keyword_names.contains(&"FROM"));
    assert!(keyword_names.contains(&"WHERE"));
    assert!(keyword_names.contains(&"GO")); // T-SQL specific
    assert!(keyword_names.contains(&"MERGE")); // T-SQL upsert
    assert!(keyword_names.contains(&"OUTPUT")); // T-SQL returning
    assert!(keyword_names.contains(&"TOP")); // T-SQL limit
}

#[test]
fn test_mssql_functions() {
    let driver = MssqlDriver::new();
    let dialect = driver.dialect_info();

    let function_names: Vec<&str> = dialect.function_names().collect();

    assert!(function_names.contains(&"COUNT"));
    assert!(function_names.contains(&"SUM"));
    assert!(function_names.contains(&"GETDATE")); // T-SQL specific
    assert!(function_names.contains(&"ISNULL")); // T-SQL specific
    assert!(function_names.contains(&"LEN")); // T-SQL string length
    assert!(function_names.contains(&"CHARINDEX")); // T-SQL find substring
    assert!(function_names.contains(&"JSON_VALUE")); // JSON support
    assert!(function_names.contains(&"SCOPE_IDENTITY")); // Identity function
}

#[test]
fn test_mssql_data_types() {
    let driver = MssqlDriver::new();
    let dialect = driver.dialect_info();

    let type_names: Vec<&str> = dialect.data_type_names().collect();

    assert!(type_names.contains(&"INT"));
    assert!(type_names.contains(&"BIGINT"));
    assert!(type_names.contains(&"VARCHAR"));
    assert!(type_names.contains(&"NVARCHAR")); // Unicode string
    assert!(type_names.contains(&"DATETIME2")); // Modern datetime
    assert!(type_names.contains(&"UNIQUEIDENTIFIER")); // UUID
    assert!(type_names.contains(&"MONEY")); // SQL Server specific
    assert!(type_names.contains(&"BIT")); // Boolean
}

#[test]
fn test_mssql_explain_config() {
    let driver = MssqlDriver::new();
    let dialect = driver.dialect_info();
    let explain = &dialect.explain_config;

    assert!(explain.explain_format.contains("SHOWPLAN_TEXT"));
    assert!(explain.query_plan_format.is_some());
    assert!(explain.analyze_format.is_some());

    let formatted = explain.format_explain("SELECT 1");
    assert!(formatted.contains("SELECT 1"));
    assert!(formatted.contains("SHOWPLAN_TEXT ON"));
}

#[test]
fn test_mssql_connection_string() {
    let driver = MssqlDriver::new();

    let mut config = ConnectionConfig::new("mssql", "Test Connection");
    config.host = "localhost".to_string();
    config.port = 1433;
    config.database = Some("testdb".to_string());
    config.username = Some("sa".to_string());
    config.password = Some("secret".to_string());

    let conn_str = driver.build_connection_string(&config);

    assert!(conn_str.contains("Server=localhost,1433"));
    assert!(conn_str.contains("Database=testdb"));
    assert!(conn_str.contains("User Id=sa"));
    assert!(conn_str.contains("Password=secret"));
}

#[test]
fn test_mssql_connection_string_trusted() {
    let driver = MssqlDriver::new();

    let mut config = ConnectionConfig::new("mssql", "Test Connection");
    config.host = "localhost".to_string();
    config.port = 1433;
    config.database = Some("testdb".to_string());

    let conn_str = driver.build_connection_string(&config);

    assert!(conn_str.contains("Server=localhost,1433"));
    assert!(conn_str.contains("Database=testdb"));
    assert!(conn_str.contains("Trusted_Connection=True"));
}

#[test]
fn test_mssql_aggregate_functions() {
    let driver = MssqlDriver::new();
    let dialect = driver.dialect_info();

    assert!(dialect.is_aggregate_function("COUNT"));
    assert!(dialect.is_aggregate_function("SUM"));
    assert!(dialect.is_aggregate_function("AVG"));
    assert!(dialect.is_aggregate_function("MIN"));
    assert!(dialect.is_aggregate_function("MAX"));
    assert!(dialect.is_aggregate_function("STRING_AGG"));
    assert!(!dialect.is_aggregate_function("LEN"));
    assert!(!dialect.is_aggregate_function("GETDATE"));
}

#[test]
fn test_mssql_version() {
    let driver = MssqlDriver::new();
    assert_eq!(driver.version(), "0.1.0");
}

#[test]
fn test_mssql_icon_name() {
    let driver = MssqlDriver::new();
    assert_eq!(driver.icon_name(), "mssql");
}

#[test]
fn test_mssql_connection_string_help() {
    let driver = MssqlDriver::new();
    let help = driver.connection_string_help();
    assert!(help.contains("Server="));
    assert!(help.contains("Database="));
}
