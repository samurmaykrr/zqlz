//! Conversion utilities for table handlers.
//!
//! This module provides functions to convert between different data types
//! and formats used throughout the application.

use crate::app::AppState;
use crate::components::{ColumnInfo, ForeignKeyInfo, IndexInfo, SchemaDetails, TableViewerPanel};
use anyhow::Context as _;
use gpui::App;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{Connection, TableRef};

/// Converts service-level TableDetails to component-level SchemaDetails.
pub(in crate::main_view) fn convert_to_schema_details(
    connection_id: Uuid,
    table: &TableRef,
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
        table: table.clone(),
        object_name: table.display_name(),
        columns,
        indexes,
        foreign_keys,
        create_statement,
    }
}

/// The table `viewer` shows, as its open resolved it, and the connection
/// statements against it run on.
pub(in crate::main_view) fn viewer_table(
    viewer: &TableViewerPanel,
    cx: &App,
) -> anyhow::Result<(TableRef, Arc<dyn Connection>)> {
    let connection_id = viewer
        .connection_id()
        .context("The viewer has no connection")?;
    let table = viewer.table().context("The table is still opening")?;
    let connection = cx
        .try_global::<AppState>()
        .context("Application state is unavailable")?
        .connection_service
        .get_connection_for_database_cached(connection_id, table.database.as_deref())
        .context("The connection is no longer open")?;
    Ok((table, connection))
}
