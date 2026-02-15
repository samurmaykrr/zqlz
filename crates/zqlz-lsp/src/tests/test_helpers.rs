//! Test helpers and common fixtures for SQL LSP tests

use crate::{ColumnInfo, SchemaCache, SqlDialect, SqlLsp, TableInfo};
use std::sync::Arc;
use zqlz_services::SchemaService;
use zqlz_ui::widgets::Rope;

/// Helper to create an SqlLsp instance with test data
pub fn create_test_lsp() -> SqlLsp {
    let schema_service = Arc::new(SchemaService::new());
    let mut lsp = SqlLsp::new(schema_service);

    // Manually populate schema cache with test data
    let mut cache = SchemaCache::default();

    // Add test tables
    cache.tables.insert(
        "users".to_string(),
        TableInfo {
            name: "users".to_string(),
            schema: None,
            comment: Some("User accounts".to_string()),
            row_count: Some(100),
        },
    );

    cache.tables.insert(
        "audit_log".to_string(),
        TableInfo {
            name: "audit_log".to_string(),
            schema: None,
            comment: Some("Audit trail".to_string()),
            row_count: Some(1000),
        },
    );

    cache.tables.insert(
        "locations".to_string(),
        TableInfo {
            name: "locations".to_string(),
            schema: None,
            comment: None,
            row_count: Some(50),
        },
    );

    // Add columns for users table
    cache.columns_by_table.insert(
        "users".to_string(),
        vec![
            ColumnInfo {
                table_name: "users".to_string(),
                name: "user_id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: true,
                is_foreign_key: false,
                comment: Some("Primary key".to_string()),
            },
            ColumnInfo {
                table_name: "users".to_string(),
                name: "username".to_string(),
                data_type: "TEXT".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: false,
                is_foreign_key: false,
                comment: None,
            },
            ColumnInfo {
                table_name: "users".to_string(),
                name: "email".to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
                default_value: None,
                is_primary_key: false,
                is_foreign_key: false,
                comment: None,
            },
        ],
    );

    // Add columns for audit_log table
    cache.columns_by_table.insert(
        "audit_log".to_string(),
        vec![
            ColumnInfo {
                table_name: "audit_log".to_string(),
                name: "log_id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: true,
                is_foreign_key: false,
                comment: None,
            },
            ColumnInfo {
                table_name: "audit_log".to_string(),
                name: "log_timestamp".to_string(),
                data_type: "TIMESTAMP".to_string(),
                nullable: false,
                default_value: Some("CURRENT_TIMESTAMP".to_string()),
                is_primary_key: false,
                is_foreign_key: false,
                comment: None,
            },
            ColumnInfo {
                table_name: "audit_log".to_string(),
                name: "action".to_string(),
                data_type: "TEXT".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: false,
                is_foreign_key: false,
                comment: None,
            },
        ],
    );

    // Add columns for locations table
    cache.columns_by_table.insert(
        "locations".to_string(),
        vec![
            ColumnInfo {
                table_name: "locations".to_string(),
                name: "location_id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: true,
                is_foreign_key: false,
                comment: None,
            },
            ColumnInfo {
                table_name: "locations".to_string(),
                name: "location_name".to_string(),
                data_type: "TEXT".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: false,
                is_foreign_key: false,
                comment: None,
            },
        ],
    );

    lsp.set_schema_cache(cache);
    lsp
}

/// Helper to create an SqlLsp instance with a specific dialect
pub fn create_test_lsp_with_dialect(dialect: SqlDialect) -> SqlLsp {
    let schema_service = Arc::new(SchemaService::new());
    let driver_type = match dialect {
        SqlDialect::SQLite => "sqlite",
        SqlDialect::MySQL => "mysql",
        SqlDialect::PostgreSQL => "postgres",
        SqlDialect::SQLServer => "sqlserver",
        SqlDialect::Redis => "redis",
        SqlDialect::Generic => "generic",
    };

    // Create a mock connection UUID
    let connection_id = uuid::Uuid::new_v4();

    // Create LSP with connection (which sets the dialect from driver_type)
    // Note: We pass None for connection since we don't have a real connection in tests
    let mut lsp = SqlLsp::new(schema_service);
    lsp.driver_type = driver_type.to_string();
    lsp.dialect = dialect;

    // Manually populate schema cache with test data (same as create_test_lsp)
    let mut cache = SchemaCache::default();

    // Add test tables
    cache.tables.insert(
        "users".to_string(),
        TableInfo {
            name: "users".to_string(),
            schema: None,
            comment: Some("User accounts".to_string()),
            row_count: Some(100),
        },
    );

    cache.tables.insert(
        "audit_log".to_string(),
        TableInfo {
            name: "audit_log".to_string(),
            schema: None,
            comment: Some("Audit trail".to_string()),
            row_count: Some(1000),
        },
    );

    cache.tables.insert(
        "locations".to_string(),
        TableInfo {
            name: "locations".to_string(),
            schema: None,
            comment: None,
            row_count: Some(50),
        },
    );

    // Add columns for users table
    cache.columns_by_table.insert(
        "users".to_string(),
        vec![
            ColumnInfo {
                table_name: "users".to_string(),
                name: "user_id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: true,
                is_foreign_key: false,
                comment: Some("Primary key".to_string()),
            },
            ColumnInfo {
                table_name: "users".to_string(),
                name: "username".to_string(),
                data_type: "TEXT".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: false,
                is_foreign_key: false,
                comment: None,
            },
            ColumnInfo {
                table_name: "users".to_string(),
                name: "email".to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
                default_value: None,
                is_primary_key: false,
                is_foreign_key: false,
                comment: None,
            },
        ],
    );

    // Add columns for audit_log table
    cache.columns_by_table.insert(
        "audit_log".to_string(),
        vec![
            ColumnInfo {
                table_name: "audit_log".to_string(),
                name: "log_id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: true,
                is_foreign_key: false,
                comment: None,
            },
            ColumnInfo {
                table_name: "audit_log".to_string(),
                name: "log_message".to_string(),
                data_type: "TEXT".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: false,
                is_foreign_key: false,
                comment: None,
            },
        ],
    );

    // Add columns for locations table
    cache.columns_by_table.insert(
        "locations".to_string(),
        vec![
            ColumnInfo {
                table_name: "locations".to_string(),
                name: "location_id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: true,
                is_foreign_key: false,
                comment: None,
            },
            ColumnInfo {
                table_name: "locations".to_string(),
                name: "location_name".to_string(),
                data_type: "TEXT".to_string(),
                nullable: false,
                default_value: None,
                is_primary_key: false,
                is_foreign_key: false,
                comment: None,
            },
        ],
    );

    lsp.set_schema_cache(cache);
    lsp
}

/// Helper assertion: check if completions contain any item with given prefix
pub fn has_completion_with_prefix(completions: &[lsp_types::CompletionItem], prefix: &str) -> bool {
    completions
        .iter()
        .any(|c| c.label.to_lowercase().starts_with(prefix))
}

/// Helper assertion: check if completions contain exact label
pub fn has_completion(completions: &[lsp_types::CompletionItem], label: &str) -> bool {
    completions.iter().any(|c| c.label == label)
}
