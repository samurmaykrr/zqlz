//! Conversion utilities for table handlers.
//!
//! This module provides functions to convert between different data types
//! and formats used throughout the application.

use crate::components::{ColumnInfo, ForeignKeyInfo, IndexInfo, SchemaDetails};
use uuid::Uuid;
use zqlz_core::DriverCategory;

/// Converts a driver name to its category for UI display.
pub(in crate::main_view) fn driver_name_to_category(driver_name: &str) -> DriverCategory {
    match driver_name {
        "mysql" | "postgres" | "mariadb" | "sqlite" | "sqlserver" | "mssql" | "cockroachdb"
        | "clickhouse" => DriverCategory::Relational,
        "mongodb" | "couchdb" | "dynamodb" | "cassandra" | "scylladb" => DriverCategory::Document,
        "redis" | "memcached" | "etcd" => DriverCategory::KeyValue,
        "neo4j" | "arangodb" | "janusgraph" => DriverCategory::Graph,
        "elasticsearch" | "opensearch" | "meilisearch" => DriverCategory::Search,
        _ => DriverCategory::Relational,
    }
}

/// Converts service-level TableDetails to component-level SchemaDetails.
pub(in crate::main_view) fn convert_to_schema_details(
    connection_id: Uuid,
    table_name: &str,
    table_details: zqlz_services::TableDetails,
    create_statement: Option<String>,
) -> SchemaDetails {
    let columns: Vec<ColumnInfo> = table_details
        .columns
        .into_iter()
        .map(|col| ColumnInfo {
            name: col.name,
            data_type: col.data_type,
            nullable: col.nullable,
            primary_key: col.is_primary_key,
            default_value: col.default_value,
        })
        .collect();

    let indexes: Vec<IndexInfo> = table_details
        .indexes
        .into_iter()
        .map(|idx| IndexInfo {
            name: idx.name,
            columns: idx.columns,
            unique: idx.is_unique,
        })
        .collect();

    let foreign_keys: Vec<ForeignKeyInfo> = table_details
        .foreign_keys
        .into_iter()
        .map(|fk| ForeignKeyInfo {
            name: fk.name,
            columns: fk.columns,
            referenced_table: fk.referenced_table,
            referenced_columns: fk.referenced_columns,
        })
        .collect();

    SchemaDetails {
        connection_id,
        object_type: "Table".to_string(),
        object_name: table_name.to_string(),
        columns,
        indexes,
        foreign_keys,
        create_statement,
    }
}

/// Resolves the correct schema qualifier for SQL queries based on driver type.
///
/// For MySQL/MariaDB, database_name and schema are the same concept, so
/// database_name is used directly. For PostgreSQL, the connection is already
/// scoped to the target database, so no schema qualifier is needed (tables
/// default to "public"). For SQLite, schemas don't apply.
pub(in crate::main_view) fn resolve_schema_qualifier(
    driver_name: &str,
    database_name: &Option<String>,
) -> Option<String> {
    match driver_name {
        "mysql" | "mariadb" | "clickhouse" | "mssql" => database_name.clone(),
        _ => None,
    }
}
