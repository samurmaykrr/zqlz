//! Export functionality for UDIF
//!
//! This module provides traits and utilities for exporting data from
//! databases to UDIF documents.

use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;

use crate::document::{
    ColumnDefinition, EncodedRow, ForeignKeyAction, ForeignKeyConstraint, IndexColumn,
    IndexDefinition, PrimaryKeyConstraint, SourceInfo, TableData, TableDefinition, UdifDocument,
};
use crate::type_mapping::{TypeMapper, get_type_mapper};
use crate::value_encoding::encode_value;
use zqlz_core::{
    ColumnInfo, Connection, ForeignKeyAction as CoreForeignKeyAction, ForeignKeyInfo, IndexInfo,
    SchemaIntrospection, TableInfo, ZqlzError,
};

/// Errors during export
#[derive(Debug, Error)]
pub enum ExportError {
    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Schema error: {0}")]
    SchemaError(String),

    #[error("Encoding error: {0}")]
    EncodingError(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Schema introspection not supported")]
    SchemaIntrospectionNotSupported,

    #[error("Export cancelled")]
    Cancelled,
}

impl From<ZqlzError> for ExportError {
    fn from(e: ZqlzError) -> Self {
        ExportError::QueryError(e.to_string())
    }
}

/// Options for export operations
#[derive(Debug, Clone)]
pub struct ExportOptions {
    /// Maximum number of rows to export per table (None = unlimited)
    pub row_limit: Option<u64>,
    /// WHERE clause filter for each table
    pub filters: std::collections::HashMap<String, String>,
    /// Whether to include schema/DDL information
    pub include_schema: bool,
    /// Whether to include data
    pub include_data: bool,
    /// Whether to include indexes
    pub include_indexes: bool,
    /// Whether to include foreign keys
    pub include_foreign_keys: bool,
    /// Batch size for fetching rows
    pub batch_size: u32,
    /// Tables to include (empty = all tables)
    pub include_tables: Vec<String>,
    /// Tables to exclude
    pub exclude_tables: Vec<String>,
    /// Columns to include per table (table_name -> column_names)
    pub include_columns: std::collections::HashMap<String, Vec<String>>,
    /// Schema name (for databases that support schemas)
    pub schema: Option<String>,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            row_limit: None,
            filters: std::collections::HashMap::new(),
            include_schema: true,
            include_data: true,
            include_indexes: true,
            include_foreign_keys: true,
            batch_size: 1000,
            include_tables: Vec::new(),
            exclude_tables: Vec::new(),
            include_columns: std::collections::HashMap::new(),
            schema: None,
        }
    }
}

impl ExportOptions {
    pub fn schema_only() -> Self {
        Self {
            include_data: false,
            ..Default::default()
        }
    }

    pub fn data_only() -> Self {
        Self {
            include_schema: false,
            include_indexes: false,
            include_foreign_keys: false,
            ..Default::default()
        }
    }

    pub fn with_limit(mut self, limit: u64) -> Self {
        self.row_limit = Some(limit);
        self
    }

    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.include_tables = tables;
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }
}

/// Progress callback for export operations
pub type ExportProgressCallback = Box<dyn Fn(ExportProgress) + Send + Sync>;

/// Export progress information
#[derive(Debug, Clone)]
pub struct ExportProgress {
    /// Current phase of export
    pub phase: ExportPhase,
    /// Current table being exported
    pub current_table: Option<String>,
    /// Total number of tables
    pub total_tables: usize,
    /// Number of tables completed
    pub tables_completed: usize,
    /// Rows exported for current table
    pub rows_exported: u64,
    /// Total rows in current table (if known)
    pub total_rows: Option<u64>,
    /// Log message
    pub message: Option<String>,
}

/// Phases of the export process
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportPhase {
    /// Starting export
    Starting,
    /// Fetching schema information
    FetchingSchema,
    /// Exporting table data
    ExportingData,
    /// Finalizing export
    Finalizing,
    /// Export complete
    Complete,
}

/// Trait for exporting data from a database connection
#[async_trait]
pub trait Exporter: Send + Sync {
    /// Export a single table to a UDIF document
    async fn export_table(
        &self,
        table_name: &str,
        options: &ExportOptions,
    ) -> Result<UdifDocument, ExportError>;

    /// Export results of a query to a UDIF document
    async fn export_query(&self, sql: &str, result_name: &str)
    -> Result<UdifDocument, ExportError>;

    /// Export entire database to a UDIF document
    async fn export_database(&self, options: &ExportOptions) -> Result<UdifDocument, ExportError>;

    /// Export with progress callback
    async fn export_database_with_progress(
        &self,
        options: &ExportOptions,
        progress: ExportProgressCallback,
    ) -> Result<UdifDocument, ExportError>;
}

/// Generic exporter implementation that works with any Connection + SchemaIntrospection
pub struct GenericExporter {
    connection: Arc<dyn Connection>,
    type_mapper: Box<dyn TypeMapper>,
    driver_name: String,
}

impl GenericExporter {
    pub fn new(connection: Arc<dyn Connection>, driver_name: &str) -> Self {
        let type_mapper = get_type_mapper(driver_name);
        Self {
            connection,
            type_mapper,
            driver_name: driver_name.to_string(),
        }
    }

    pub fn with_type_mapper(
        connection: Arc<dyn Connection>,
        driver_name: &str,
        type_mapper: Box<dyn TypeMapper>,
    ) -> Self {
        Self {
            connection,
            type_mapper,
            driver_name: driver_name.to_string(),
        }
    }

    fn create_source_info(&self) -> SourceInfo {
        SourceInfo::new(&self.driver_name)
    }

    fn should_include_table(&self, table_name: &str, options: &ExportOptions) -> bool {
        if !options.include_tables.is_empty()
            && !options.include_tables.iter().any(|t| t == table_name)
        {
            return false;
        }
        !options.exclude_tables.iter().any(|t| t == table_name)
    }

    fn get_schema_introspection(&self) -> Result<&dyn SchemaIntrospection, ExportError> {
        self.connection
            .as_schema_introspection()
            .ok_or(ExportError::SchemaIntrospectionNotSupported)
    }

    async fn get_table_list(&self, options: &ExportOptions) -> Result<Vec<TableInfo>, ExportError> {
        let introspection = self.get_schema_introspection()?;
        introspection
            .list_tables(options.schema.as_deref())
            .await
            .map_err(|e| ExportError::SchemaError(e.to_string()))
    }

    async fn build_table_definition(
        &self,
        table_name: &str,
        options: &ExportOptions,
    ) -> Result<TableDefinition, ExportError> {
        let introspection = self.get_schema_introspection()?;
        let columns = introspection
            .get_columns(options.schema.as_deref(), table_name)
            .await
            .map_err(|e| ExportError::SchemaError(e.to_string()))?;

        let mut table_def = TableDefinition::new(table_name);
        table_def.schema = options.schema.clone();

        let include_cols = options.include_columns.get(table_name);
        for col in columns {
            if let Some(cols) = include_cols {
                if !cols.iter().any(|c| c == &col.name) {
                    continue;
                }
            }
            let col_def = self.column_info_to_definition(&col);
            table_def.add_column(col_def);
        }

        if let Ok(Some(pk)) = introspection
            .get_primary_key(options.schema.as_deref(), table_name)
            .await
        {
            table_def.primary_key = Some(PrimaryKeyConstraint {
                name: pk.name,
                columns: pk.columns,
            });
        }

        if options.include_indexes {
            if let Ok(indexes) = introspection
                .get_indexes(options.schema.as_deref(), table_name)
                .await
            {
                for idx in indexes {
                    if !idx.is_primary {
                        table_def.indexes.push(self.index_info_to_definition(&idx));
                    }
                }
            }
        }

        if options.include_foreign_keys {
            if let Ok(fks) = introspection
                .get_foreign_keys(options.schema.as_deref(), table_name)
                .await
            {
                for fk in fks {
                    table_def.foreign_keys.push(self.fk_info_to_definition(&fk));
                }
            }
        }

        Ok(table_def)
    }

    fn column_info_to_definition(&self, col: &ColumnInfo) -> ColumnDefinition {
        let canonical_type = self.type_mapper.to_canonical(&col.data_type);
        let mut col_def = ColumnDefinition::new(&col.name, canonical_type, &col.data_type);
        col_def.nullable = col.nullable;
        if let Some(ref default) = col.default_value {
            col_def.default_value =
                Some(crate::document::DefaultValue::Expression(default.clone()));
        }
        col_def.auto_increment = col.is_auto_increment;
        col_def.comment = col.comment.clone();
        col_def
    }

    fn index_info_to_definition(&self, idx: &IndexInfo) -> IndexDefinition {
        IndexDefinition {
            name: idx.name.clone(),
            columns: idx
                .columns
                .iter()
                .map(|c| IndexColumn {
                    column: c.clone(),
                    order: crate::document::SortOrder::Asc,
                    nulls: crate::document::NullsOrder::Default,
                })
                .collect(),
            unique: idx.is_unique,
            index_type: Some(idx.index_type.clone()),
            where_clause: None,
        }
    }

    fn fk_info_to_definition(&self, fk: &ForeignKeyInfo) -> ForeignKeyConstraint {
        ForeignKeyConstraint {
            name: Some(fk.name.clone()),
            columns: fk.columns.clone(),
            referenced_table: fk.referenced_table.clone(),
            referenced_schema: fk.referenced_schema.clone(),
            referenced_columns: fk.referenced_columns.clone(),
            on_delete: self.convert_fk_action(&fk.on_delete),
            on_update: self.convert_fk_action(&fk.on_update),
        }
    }

    fn convert_fk_action(&self, action: &CoreForeignKeyAction) -> ForeignKeyAction {
        match action {
            CoreForeignKeyAction::NoAction => ForeignKeyAction::NoAction,
            CoreForeignKeyAction::Restrict => ForeignKeyAction::Restrict,
            CoreForeignKeyAction::Cascade => ForeignKeyAction::Cascade,
            CoreForeignKeyAction::SetNull => ForeignKeyAction::SetNull,
            CoreForeignKeyAction::SetDefault => ForeignKeyAction::SetDefault,
        }
    }

    async fn export_table_data(
        &self,
        table_name: &str,
        table_def: &TableDefinition,
        options: &ExportOptions,
    ) -> Result<TableData, ExportError> {
        let mut table_data = TableData::default();

        let column_names: Vec<&str> = table_def.columns.iter().map(|c| c.name.as_str()).collect();
        let columns_sql = if column_names.is_empty() {
            "*".to_string()
        } else {
            column_names
                .iter()
                .map(|c| self.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let filter = options.filters.get(table_name);
        let mut sql = if let Some(where_clause) = filter {
            table_data.filter = Some(where_clause.clone());
            format!(
                "SELECT {} FROM {} WHERE {}",
                columns_sql,
                self.quote_identifier(table_name),
                where_clause
            )
        } else {
            format!(
                "SELECT {} FROM {}",
                columns_sql,
                self.quote_identifier(table_name)
            )
        };

        if let Some(limit) = options.row_limit {
            table_data.partial = true;
            sql = format!("{} LIMIT {}", sql, limit);
        }

        let result = self
            .connection
            .query(&sql, &[])
            .await
            .map_err(|e| ExportError::QueryError(e.to_string()))?;

        for row in result.rows {
            let encoded_values: Vec<_> = row.values.iter().map(encode_value).collect();
            table_data.rows.push(EncodedRow::new(encoded_values));
        }

        Ok(table_data)
    }

    fn quote_identifier(&self, name: &str) -> String {
        match self.driver_name.as_str() {
            "mysql" => format!("`{}`", name),
            "mssql" => format!("[{}]", name),
            _ => format!("\"{}\"", name),
        }
    }
}

#[async_trait]
impl Exporter for GenericExporter {
    async fn export_table(
        &self,
        table_name: &str,
        options: &ExportOptions,
    ) -> Result<UdifDocument, ExportError> {
        let mut doc = UdifDocument::new(self.create_source_info());

        let table_def = if options.include_schema {
            let def = self.build_table_definition(table_name, options).await?;
            doc.add_table(def.clone());
            def
        } else {
            TableDefinition::new(table_name)
        };

        if options.include_data {
            let table_data = self
                .export_table_data(table_name, &table_def, options)
                .await?;
            doc.data.insert(table_name.to_string(), table_data);
        }

        Ok(doc)
    }

    async fn export_query(
        &self,
        sql: &str,
        result_name: &str,
    ) -> Result<UdifDocument, ExportError> {
        let mut doc = UdifDocument::new(self.create_source_info());

        let result = self
            .connection
            .query(sql, &[])
            .await
            .map_err(|e| ExportError::QueryError(e.to_string()))?;

        let mut table_def = TableDefinition::new(result_name);
        for col in &result.columns {
            let canonical_type = self.type_mapper.to_canonical(&col.data_type);
            let col_def = ColumnDefinition::new(&col.name, canonical_type, &col.data_type);
            table_def.add_column(col_def);
        }
        doc.add_table(table_def);

        let mut table_data = TableData::default();
        for row in result.rows {
            let encoded_values: Vec<_> = row.values.iter().map(encode_value).collect();
            table_data.rows.push(EncodedRow::new(encoded_values));
        }
        doc.data.insert(result_name.to_string(), table_data);

        Ok(doc)
    }

    async fn export_database(&self, options: &ExportOptions) -> Result<UdifDocument, ExportError> {
        self.export_database_with_progress(options, Box::new(|_| {}))
            .await
    }

    async fn export_database_with_progress(
        &self,
        options: &ExportOptions,
        progress: ExportProgressCallback,
    ) -> Result<UdifDocument, ExportError> {
        progress(ExportProgress {
            phase: ExportPhase::Starting,
            current_table: None,
            total_tables: 0,
            tables_completed: 0,
            rows_exported: 0,
            total_rows: None,
            message: Some("Export started".into()),
        });

        progress(ExportProgress {
            phase: ExportPhase::FetchingSchema,
            current_table: None,
            total_tables: 0,
            tables_completed: 0,
            rows_exported: 0,
            total_rows: None,
            message: Some("Fetching schema information...".into()),
        });

        let tables = self.get_table_list(options).await?;
        let tables: Vec<_> = tables
            .into_iter()
            .filter(|t| self.should_include_table(&t.name, options))
            .collect();

        let total_tables = tables.len();
        let mut doc = UdifDocument::new(self.create_source_info());

        for (idx, table) in tables.iter().enumerate() {
            progress(ExportProgress {
                phase: ExportPhase::ExportingData,
                current_table: Some(table.name.clone()),
                total_tables,
                tables_completed: idx,
                rows_exported: 0,
                total_rows: table.row_count.map(|r| r as u64),
                message: Some(format!("Exporting table [{}]", table.name)),
            });

            let table_def = if options.include_schema {
                let def = self.build_table_definition(&table.name, options).await?;
                doc.add_table(def.clone());
                def
            } else {
                TableDefinition::new(&table.name)
            };

            if options.include_data {
                let table_data = self
                    .export_table_data(&table.name, &table_def, options)
                    .await?;
                let row_count = table_data.rows.len() as u64;
                doc.data.insert(table.name.clone(), table_data);

                progress(ExportProgress {
                    phase: ExportPhase::ExportingData,
                    current_table: Some(table.name.clone()),
                    total_tables,
                    tables_completed: idx,
                    rows_exported: row_count,
                    total_rows: Some(row_count),
                    message: Some(format!("Exported {} rows from [{}]", row_count, table.name)),
                });
            }
        }

        progress(ExportProgress {
            phase: ExportPhase::Finalizing,
            current_table: None,
            total_tables,
            tables_completed: total_tables,
            rows_exported: 0,
            total_rows: None,
            message: Some("Finalizing export...".into()),
        });

        progress(ExportProgress {
            phase: ExportPhase::Complete,
            current_table: None,
            total_tables,
            tables_completed: total_tables,
            rows_exported: doc.total_rows() as u64,
            total_rows: Some(doc.total_rows() as u64),
            message: Some("Export completed successfully".into()),
        });

        Ok(doc)
    }
}

/// Helper functions for working with UDIF documents
pub mod helpers {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    /// Serialize a UDIF document to JSON
    pub fn to_json(doc: &UdifDocument) -> Result<String, ExportError> {
        serde_json::to_string_pretty(doc).map_err(|e| ExportError::EncodingError(e.to_string()))
    }

    /// Serialize a UDIF document to compact JSON
    pub fn to_json_compact(doc: &UdifDocument) -> Result<String, ExportError> {
        serde_json::to_string(doc).map_err(|e| ExportError::EncodingError(e.to_string()))
    }

    /// Serialize a UDIF document to compressed JSON (gzip)
    pub fn to_json_compressed(doc: &UdifDocument) -> Result<Vec<u8>, ExportError> {
        let json = to_json_compact(doc)?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(json.as_bytes())
            .map_err(|e| ExportError::EncodingError(e.to_string()))?;
        encoder
            .finish()
            .map_err(|e| ExportError::EncodingError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_options_default() {
        let options = ExportOptions::default();
        assert!(options.include_schema);
        assert!(options.include_data);
        assert!(options.include_indexes);
        assert!(options.include_foreign_keys);
        assert_eq!(options.row_limit, None);
    }

    #[test]
    fn test_export_options_schema_only() {
        let options = ExportOptions::schema_only();
        assert!(options.include_schema);
        assert!(!options.include_data);
    }

    #[test]
    fn test_export_options_data_only() {
        let options = ExportOptions::data_only();
        assert!(!options.include_schema);
        assert!(options.include_data);
    }
}
