//! Conversion utilities for table handlers.
//!
//! This module provides functions to convert between different data types
//! and formats used throughout the application.

use crate::components::{ColumnInfo, ForeignKeyInfo, IndexInfo, SchemaDetails};
use std::sync::Arc;
use uuid::Uuid;

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
            index_type: idx.index_type,
        })
        .collect();

    let foreign_keys: Vec<ForeignKeyInfo> = table_details
        .foreign_keys
        .into_iter()
        .map(|fk| ForeignKeyInfo {
            name: fk.name,
            columns: fk.columns,
            referenced_table: fk.referenced_table,
            referenced_schema: fk.referenced_schema,
            referenced_columns: fk.referenced_columns,
            on_update: fk.on_update,
            on_delete: fk.on_delete,
            is_deferrable: fk.is_deferrable,
            initially_deferred: fk.initially_deferred,
        })
        .collect();

    SchemaDetails {
        connection_id,
        object_type: table_details.table_type.display_name().to_string(),
        object_name: table_name.to_string(),
        columns,
        indexes,
        foreign_keys,
        create_statement,
    }
}

/// App-level adapter that preserves existing table-handler call signatures
/// while delegating qualifier policy to `zqlz-core`.
pub(in crate::main_view) fn resolve_schema_qualifier(
    connection: &Arc<dyn zqlz_core::Connection>,
    database_name: &Option<String>,
) -> Option<String> {
    zqlz_core::resolve_schema_qualifier_for_connection(
        connection.as_ref(),
        database_name.as_deref(),
    )
}
