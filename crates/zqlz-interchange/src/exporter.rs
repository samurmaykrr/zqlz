//! Export functionality for UDIF
//!
//! This module provides traits and utilities for exporting data from
//! databases to UDIF documents.

use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;

use crate::document::{
    CheckConstraint, ColumnDefinition, EncodedRow, EnumDefinition, ForeignKeyAction,
    ForeignKeyConstraint, IndexColumn, IndexDefinition, IndexMethod, PrimaryKeyConstraint,
    SequenceDefinition, SourceInfo, TableData, TableDefinition, UdifDocument,
};
use crate::type_mapping::{
    TypeMapper, canonical_mapper_id, closest_safe_mapper_id, get_type_mapper,
};
use crate::value_encoding::encode_value;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintType, ForeignKeyAction as CoreForeignKeyAction,
    ForeignKeyInfo, IndexInfo, SchemaIntrospection, SqlObjectName, TableInfo, ZqlzError,
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
        let resolved_driver_id = connection
            .dialect_id()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| connection.driver_name().to_string());
        let mapper_driver_id = if canonical_mapper_id(&resolved_driver_id).is_some() {
            resolved_driver_id.clone()
        } else {
            let fallback_mapper = closest_safe_mapper_id(&resolved_driver_id);
            tracing::warn!(
                dialect_id = ?connection.dialect_id(),
                driver_name = connection.driver_name(),
                provided_driver_name = driver_name,
                resolved_driver_id = resolved_driver_id.as_str(),
                fallback_mapper,
                "unknown driver for type mapping; using closest safe mapper"
            );
            fallback_mapper.to_string()
        };
        let type_mapper = get_type_mapper(&mapper_driver_id);
        Self {
            connection,
            type_mapper,
            driver_name: resolved_driver_id,
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
            if let Some(cols) = include_cols
                && !cols.iter().any(|c| c == &col.name)
            {
                continue;
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

        if options.include_indexes
            && let Ok(indexes) = introspection
                .get_indexes(options.schema.as_deref(), table_name)
                .await
        {
            for idx in indexes {
                if !idx.is_primary {
                    table_def.indexes.push(self.index_info_to_definition(&idx));
                }
            }
        }

        if options.include_foreign_keys
            && let Ok(fks) = introspection
                .get_foreign_keys(options.schema.as_deref(), table_name)
                .await
        {
            for fk in fks {
                table_def.foreign_keys.push(self.fk_info_to_definition(&fk));
            }
        }

        // Fetch CHECK constraints so they are preserved in the UDIF document.
        // Other constraint types (PK, FK, Unique) are already captured above via
        // their dedicated fields, so only CHECK is handled here.
        if let Ok(constraints) = introspection
            .get_constraints(options.schema.as_deref(), table_name)
            .await
        {
            for constraint in constraints {
                if constraint.constraint_type == ConstraintType::Check
                    && let Some(expression) = constraint.definition
                {
                    table_def.check_constraints.push(CheckConstraint {
                        name: Some(constraint.name),
                        expression,
                    });
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
            col_def.default_value = Some(canonicalize_default_expression(default));
        }
        col_def.auto_increment = col.is_auto_increment;
        col_def.comment = col.comment.clone();

        // Preserve generated column metadata so importers can recreate the expression
        // rather than treating generated columns as plain columns with static defaults.
        if let Some(ref expression) = col.generation_expression {
            col_def = col_def.generated(expression.clone(), col.is_generated_stored);
        }

        col_def
    }
}

/// Converts a raw default-value string from the database driver into a semantic
/// `DefaultValue` variant when the expression is a well-known cross-driver function.
///
/// Unrecognized expressions are preserved as `Expression(raw)` so the importer
/// can still round-trip them on same-driver imports while emitting a degradation
/// warning on cross-driver ones.
pub(crate) fn canonicalize_default_expression(raw: &str) -> crate::document::DefaultValue {
    // Normalize: lowercase, trim outer whitespace, and strip trailing `()` for
    // zero-argument functions so that `NOW()`, `now()`, and `now` all match.
    let normalized = raw.trim().to_lowercase();
    let normalized = normalized.trim_end_matches("()");

    match normalized {
        // Current-timestamp synonyms across PostgreSQL, MySQL, MariaDB, SQLite
        "now" | "current_timestamp" | "localtimestamp" | "sysdate" | "getdate" | "sysdatetime"
        | "getutcdate" => crate::document::DefaultValue::CurrentTimestamp,

        // Current-date synonyms
        "current_date" | "curdate" | "today" => crate::document::DefaultValue::CurrentDate,

        // Current-time synonyms
        "current_time" | "curtime" | "localtime" => crate::document::DefaultValue::CurrentTime,

        // Current-user synonyms
        "current_user" | "user" | "session_user" | "system_user" => {
            crate::document::DefaultValue::CurrentUser
        }

        // UUID-generation synonyms
        "gen_random_uuid" | "uuid" | "newid" | "sys_guid" | "random_uuid" => {
            crate::document::DefaultValue::GeneratedUuid
        }

        _ => crate::document::DefaultValue::Expression(raw.to_owned()),
    }
}

// Re-open the impl block for the remaining methods
impl GenericExporter {
    fn index_info_to_definition(&self, idx: &IndexInfo) -> IndexDefinition {
        IndexDefinition {
            name: idx.name.clone(),
            columns: idx
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| IndexColumn {
                    column: c.clone(),
                    order: if idx.column_descending.get(i).copied().unwrap_or(false) {
                        crate::document::SortOrder::Desc
                    } else {
                        crate::document::SortOrder::Asc
                    },
                    nulls: crate::document::NullsOrder::Default,
                })
                .collect(),
            unique: idx.is_unique,
            index_method: parse_index_method(&idx.index_type),
            index_type_raw: Some(idx.index_type.clone()),
            where_clause: idx.where_clause.clone(),
            include_columns: idx.include_columns.clone(),
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
            is_deferrable: fk.is_deferrable,
            initially_deferred: fk.initially_deferred,
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
        let filter = options.filters.get(table_name);
        let table_object_name = parse_sql_object_name(table_name);
        let base_sql = if let Some(where_clause) = filter {
            table_data.filter = Some(where_clause.clone());
            self.connection.select_rows_sql(
                &table_object_name,
                &column_names
                    .iter()
                    .map(|column| (*column).to_string())
                    .collect::<Vec<_>>(),
                Some(where_clause),
            )?
        } else {
            self.connection.select_rows_sql(
                &table_object_name,
                &column_names
                    .iter()
                    .map(|column| (*column).to_string())
                    .collect::<Vec<_>>(),
                None,
            )?
        };

        let page_size = u64::from(options.batch_size);
        let mut offset: u64 = 0;
        let mut total_rows_exported: u64 = 0;

        loop {
            // Respect row_limit: fetch no more than what remains under the cap.
            let fetch_count = match options.row_limit {
                Some(limit) => {
                    let remaining = limit.saturating_sub(total_rows_exported);
                    if remaining == 0 {
                        table_data.partial = true;
                        break;
                    }
                    remaining.min(page_size)
                }
                None => page_size,
            };

            let page_sql = self
                .connection
                .paginated_select_sql(&base_sql, fetch_count, offset);
            let result = self
                .connection
                .query(&page_sql, &[])
                .await
                .map_err(|e| ExportError::QueryError(e.to_string()))?;

            let page_row_count = result.rows.len() as u64;
            for row in result.rows {
                let encoded_values: Vec<_> = row.values.iter().map(encode_value).collect();
                table_data.rows.push(EncodedRow::new(encoded_values));
            }

            total_rows_exported += page_row_count;
            offset += page_row_count;

            // A page smaller than the requested fetch count means we have reached
            // the end of the table.
            if page_row_count < fetch_count {
                break;
            }
        }

        // Mark the export as partial when a row_limit truncated the result.
        if let Some(limit) = options.row_limit
            && total_rows_exported >= limit
        {
            table_data.partial = true;
        }

        Ok(table_data)
    }

    /// Populate `doc.schema.sequences` with the high-water mark for every
    /// auto-increment column across all tables that were exported.
    ///
    /// Each driver exposes its counter through a different mechanism:
    /// - PostgreSQL: a first-class SEQUENCE object, queried via `last_value`.
    /// - MySQL: `information_schema.TABLES.AUTO_INCREMENT` (next value, so we
    ///   subtract 1 to get the last-used value).
    /// - SQLite: the `sqlite_sequence` table maintained by the ROWID engine.
    ///
    /// A missing or zero counter is silently skipped — it just means the table
    /// has never had a row inserted and `current_value` stays `None`.
    async fn export_sequences(&self, doc: &mut UdifDocument) -> Result<(), ExportError> {
        // Collect the list of tables and their auto-increment columns from what
        // was already exported so we only query sequences we actually care about.
        let tables: Vec<(String, Vec<String>)> = doc
            .schema
            .tables
            .values()
            .map(|table| {
                let auto_cols: Vec<String> = table
                    .columns
                    .iter()
                    .filter(|col| col.auto_increment)
                    .map(|col| col.name.clone())
                    .collect();
                (table.name.clone(), auto_cols)
            })
            .filter(|(_, cols)| !cols.is_empty())
            .collect();

        for (table_name, columns) in &tables {
            for column_name in columns {
                let Some(current_value) = self
                    .connection
                    .export_sequence_current_value(table_name, column_name)
                    .await
                    .map_err(|error| ExportError::QueryError(error.to_string()))?
                else {
                    continue;
                };

                let key = format!("{}.{}", table_name, column_name);
                doc.schema.sequences.insert(
                    key.clone(),
                    SequenceDefinition {
                        name: key,
                        start_value: 1,
                        increment: 1,
                        min_value: None,
                        max_value: None,
                        current_value: Some(current_value),
                        cycle: false,
                    },
                );
            }
        }

        Ok(())
    }

    /// Populate `doc.schema.enums` with all user-defined named enum types in the source database.
    ///
    /// Only PostgreSQL has first-class schema-level enum types — MySQL embeds enum values inline
    /// in the column DDL and SQLite has no enum type support at all.  For those drivers this
    /// method is a no-op so the importer can synthesize appropriate representations on the
    /// target side.
    async fn export_enums(&self, doc: &mut UdifDocument) -> Result<(), ExportError> {
        let named_enums = self
            .connection
            .export_named_enum_definitions()
            .await
            .map_err(|error| ExportError::QueryError(error.to_string()))?;

        for (type_name, values) in named_enums {
            doc.schema.enums.insert(
                type_name.clone(),
                EnumDefinition {
                    name: type_name,
                    schema: None,
                    values,
                },
            );
        }

        Ok(())
    }

    /// Enriches each column whose native type matches a known enum type with a
    /// `CanonicalType::Enum` that carries the type name and its allowed values.
    ///
    /// PostgreSQL's `to_canonical` maps user-defined types to `Custom { source_type }` because
    /// the generic type mapper has no access to `pg_enum`.  After `export_enums` has populated
    /// `doc.schema.enums` we can fix up those columns so cross-driver imports receive the full
    /// enum definition rather than an opaque custom type name.
    fn enrich_enum_columns(&self, doc: &mut UdifDocument) {
        if doc.schema.enums.is_empty() {
            return;
        }

        for table in doc.schema.tables.values_mut() {
            for col in &mut table.columns {
                // Custom types whose source_type matches a known enum name need upgrading.
                if let crate::CanonicalType::Custom {
                    ref source_type, ..
                } = col.canonical_type
                    && let Some(enum_def) = doc.schema.enums.get(source_type)
                {
                    col.canonical_type = crate::CanonicalType::Enum {
                        name: Some(enum_def.name.clone()),
                        values: enum_def.values.clone(),
                    };
                }
            }
        }
    }
}

fn parse_sql_object_name(object_name: &str) -> SqlObjectName {
    if object_name.contains('.') {
        let mut parts = object_name.splitn(2, '.');
        match (parts.next(), parts.next()) {
            (Some(namespace), Some(name)) if !namespace.is_empty() && !name.is_empty() => {
                SqlObjectName::with_namespace(namespace, name)
            }
            _ => SqlObjectName::new(object_name),
        }
    } else {
        SqlObjectName::new(object_name)
    }
}

/// Map a raw driver index-type string to the canonical `IndexMethod` enum variant.
///
/// Comparisons are case-insensitive so "BTREE", "btree", and "BTree" all map to
/// `IndexMethod::Btree`. Returns `None` for unrecognised strings — the raw string
/// is still preserved in `IndexDefinition::index_type_raw`.
fn parse_index_method(raw: &str) -> Option<IndexMethod> {
    match raw.to_ascii_lowercase().as_str() {
        "btree" | "nonclustered" | "clustered" => Some(IndexMethod::Btree),
        "hash" => Some(IndexMethod::Hash),
        "gin" => Some(IndexMethod::Gin),
        "gist" => Some(IndexMethod::Gist),
        "spgist" | "sp_gist" => Some(IndexMethod::SpGist),
        "brin" => Some(IndexMethod::Brin),
        "fulltext" => Some(IndexMethod::Fulltext),
        "spatial" => Some(IndexMethod::Spatial),
        _ => None,
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

        if options.include_schema && options.include_data {
            self.export_sequences(&mut doc).await?;
        }

        if options.include_schema {
            self.export_enums(&mut doc).await?;
            // Upgrade columns whose native type is a known PostgreSQL enum from
            // Custom → Enum so importers receive the full value list.
            self.enrich_enum_columns(&mut doc);
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
    use std::sync::Arc;
    use zqlz_core::{
        ColumnMeta, QueryResult, Result, Row, StatementResult, Transaction, ZqlzError,
    };

    // Minimal connection stub that satisfies the Connection trait.
    // No actual database access is performed — these tests exercise pure conversion logic.
    struct StubConnection;

    #[async_trait::async_trait]
    impl zqlz_core::Connection for StubConnection {
        fn driver_name(&self) -> &str {
            "sqlite"
        }

        fn rename_table_sql(
            &self,
            table_name: &zqlz_core::SqlObjectName,
            new_table_name: &str,
        ) -> Result<String> {
            Ok(format!(
                "ALTER TABLE {} RENAME TO {}",
                self.render_qualified_name(table_name),
                self.quote_identifier(new_table_name)
            ))
        }

        fn drop_table_sql(
            &self,
            table_name: &zqlz_core::SqlObjectName,
            _options: zqlz_core::DropTableOptions,
        ) -> Result<String> {
            Ok(format!(
                "DROP TABLE {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn drop_view_sql(
            &self,
            view_name: &zqlz_core::SqlObjectName,
            _options: zqlz_core::DropViewOptions,
        ) -> Result<String> {
            Ok(format!(
                "DROP VIEW {}",
                self.render_qualified_name(view_name)
            ))
        }

        fn drop_trigger_sql(
            &self,
            trigger_name: &zqlz_core::SqlObjectName,
            _table_name: Option<&zqlz_core::SqlObjectName>,
            _options: zqlz_core::DropTriggerOptions,
        ) -> Result<String> {
            Ok(format!(
                "DROP TRIGGER {}",
                self.render_qualified_name(trigger_name)
            ))
        }

        fn truncate_table_sql(&self, table_name: &zqlz_core::SqlObjectName) -> Result<String> {
            Ok(format!(
                "TRUNCATE TABLE {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn duplicate_table_sql(
            &self,
            source_table_name: &zqlz_core::SqlObjectName,
            new_table_name: &zqlz_core::SqlObjectName,
        ) -> Result<String> {
            Ok(format!(
                "CREATE TABLE {} AS SELECT * FROM {}",
                self.render_qualified_name(new_table_name),
                self.render_qualified_name(source_table_name)
            ))
        }

        fn clear_table_sql(&self, table_name: &zqlz_core::SqlObjectName) -> Result<String> {
            Ok(format!(
                "DELETE FROM {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn table_has_rows_sql(&self, table_name: &zqlz_core::SqlObjectName) -> Result<String> {
            Ok(format!(
                "SELECT 1 FROM {} LIMIT 1",
                self.render_qualified_name(table_name)
            ))
        }

        fn select_rows_sql(
            &self,
            table_name: &zqlz_core::SqlObjectName,
            projected_columns: &[String],
            where_clause_sql: Option<&str>,
        ) -> Result<String> {
            let projection = if projected_columns.is_empty() {
                "*".to_string()
            } else {
                projected_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let mut sql = format!(
                "SELECT {} FROM {}",
                projection,
                self.render_qualified_name(table_name)
            );
            if let Some(where_clause_sql) = where_clause_sql {
                sql.push_str(" WHERE ");
                sql.push_str(where_clause_sql);
            }
            Ok(sql)
        }

        fn select_distinct_rows_sql(
            &self,
            table_name: &zqlz_core::SqlObjectName,
            projected_columns: &[String],
            where_clause_sql: Option<&str>,
            order_by_columns: &[String],
            limit: u64,
        ) -> Result<String> {
            let mut sql = format!(
                "SELECT DISTINCT {} FROM {}",
                projected_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", "),
                self.render_qualified_name(table_name)
            );
            if let Some(where_clause_sql) = where_clause_sql {
                sql.push_str(" WHERE ");
                sql.push_str(where_clause_sql);
            }
            if !order_by_columns.is_empty() {
                sql.push_str(" ORDER BY ");
                sql.push_str(
                    &order_by_columns
                        .iter()
                        .map(|column| self.quote_identifier(column))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
            sql.push_str(&format!(" LIMIT {}", limit));
            Ok(sql)
        }

        fn insert_row_sql(
            &self,
            table_name: &zqlz_core::SqlObjectName,
            column_names: &[String],
            value_count: usize,
        ) -> Result<String> {
            let placeholders = (0..value_count)
                .map(|index| self.format_bind_placeholder(index))
                .collect::<Vec<_>>()
                .join(", ");
            let columns = column_names
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!(
                "INSERT INTO {} ({}) VALUES ({})",
                self.render_qualified_name(table_name),
                columns,
                placeholders
            ))
        }

        fn performance_metrics_query_sql(&self) -> Result<String> {
            Ok("SELECT 0 as total_queries".to_string())
        }

        async fn execute(
            &self,
            _sql: &str,
            _params: &[zqlz_core::Value],
        ) -> Result<StatementResult> {
            Err(ZqlzError::NotSupported("stub".into()))
        }

        async fn query(&self, _sql: &str, _params: &[zqlz_core::Value]) -> Result<QueryResult> {
            Err(ZqlzError::NotSupported("stub".into()))
        }

        async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
            Err(ZqlzError::NotSupported("stub".into()))
        }

        async fn close(&self) -> Result<()> {
            Ok(())
        }

        fn is_closed(&self) -> bool {
            false
        }
    }

    fn make_exporter() -> GenericExporter {
        GenericExporter::new(Arc::new(StubConnection), "sqlite")
    }

    // ===== ExportOptions tests =====

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

    // ===== parse_index_method tests =====

    #[test]
    fn test_parse_index_method_known_variants() {
        assert_eq!(parse_index_method("btree"), Some(IndexMethod::Btree));
        assert_eq!(parse_index_method("BTREE"), Some(IndexMethod::Btree));
        // MSSQL uses NONCLUSTERED/CLUSTERED — both map to Btree since B-tree is the
        // underlying structure for clustered and non-clustered MSSQL indexes.
        assert_eq!(parse_index_method("nonclustered"), Some(IndexMethod::Btree));
        assert_eq!(parse_index_method("clustered"), Some(IndexMethod::Btree));
        assert_eq!(parse_index_method("hash"), Some(IndexMethod::Hash));
        assert_eq!(parse_index_method("gin"), Some(IndexMethod::Gin));
        assert_eq!(parse_index_method("gist"), Some(IndexMethod::Gist));
        assert_eq!(parse_index_method("spgist"), Some(IndexMethod::SpGist));
        assert_eq!(parse_index_method("sp_gist"), Some(IndexMethod::SpGist));
        assert_eq!(parse_index_method("brin"), Some(IndexMethod::Brin));
        assert_eq!(parse_index_method("fulltext"), Some(IndexMethod::Fulltext));
        assert_eq!(parse_index_method("spatial"), Some(IndexMethod::Spatial));
    }

    #[test]
    fn test_parse_index_method_unknown_returns_none() {
        assert_eq!(parse_index_method(""), None);
        assert_eq!(parse_index_method("custom_extension"), None);
        assert_eq!(parse_index_method("ivfflat"), None);
    }

    // ===== column_info_to_definition tests =====

    #[test]
    fn test_column_info_to_definition_plain_column() {
        let exporter = make_exporter();
        let col = zqlz_core::ColumnInfo {
            name: "email".to_string(),
            data_type: "TEXT".to_string(),
            nullable: true,
            is_auto_increment: false,
            ..Default::default()
        };
        let def = exporter.column_info_to_definition(&col);
        assert_eq!(def.name, "email");
        assert!(def.nullable);
        assert!(!def.auto_increment);
        assert!(def.generation_expression.is_none());
        assert!(!def.is_generated_stored);
    }

    #[test]
    fn test_column_info_to_definition_generated_virtual() {
        let exporter = make_exporter();
        let col = zqlz_core::ColumnInfo {
            name: "full_name".to_string(),
            data_type: "TEXT".to_string(),
            nullable: true,
            generation_expression: Some("first_name || ' ' || last_name".to_string()),
            is_generated_stored: false,
            ..Default::default()
        };
        let def = exporter.column_info_to_definition(&col);
        assert_eq!(
            def.generation_expression.as_deref(),
            Some("first_name || ' ' || last_name")
        );
        assert!(!def.is_generated_stored);
    }

    #[test]
    fn test_column_info_to_definition_generated_stored() {
        let exporter = make_exporter();
        let col = zqlz_core::ColumnInfo {
            name: "total_price".to_string(),
            data_type: "NUMERIC".to_string(),
            nullable: false,
            generation_expression: Some("quantity * unit_price".to_string()),
            is_generated_stored: true,
            ..Default::default()
        };
        let def = exporter.column_info_to_definition(&col);
        assert_eq!(
            def.generation_expression.as_deref(),
            Some("quantity * unit_price")
        );
        assert!(def.is_generated_stored);
    }

    // ===== index_info_to_definition tests =====

    #[test]
    fn test_index_info_to_definition_btree() {
        let exporter = make_exporter();
        let idx = zqlz_core::IndexInfo {
            name: "idx_users_email".to_string(),
            columns: vec!["email".to_string()],
            is_unique: true,
            is_primary: false,
            index_type: "btree".to_string(),
            comment: None,
            where_clause: None,
            include_columns: vec![],
            column_descending: vec![],
        };
        let def = exporter.index_info_to_definition(&idx);
        assert_eq!(def.name, "idx_users_email");
        assert_eq!(def.index_method, Some(IndexMethod::Btree));
        assert_eq!(def.index_type_raw.as_deref(), Some("btree"));
        assert!(def.where_clause.is_none());
        assert!(def.include_columns.is_empty());
        assert!(def.unique);
    }

    #[test]
    fn test_index_info_to_definition_gin_with_method() {
        let exporter = make_exporter();
        let idx = zqlz_core::IndexInfo {
            name: "idx_search".to_string(),
            columns: vec!["document".to_string()],
            is_unique: false,
            is_primary: false,
            index_type: "gin".to_string(),
            comment: None,
            where_clause: None,
            include_columns: vec![],
            column_descending: vec![],
        };
        let def = exporter.index_info_to_definition(&idx);
        assert_eq!(def.index_method, Some(IndexMethod::Gin));
    }

    #[test]
    fn test_index_info_to_definition_partial_index() {
        let exporter = make_exporter();
        let idx = zqlz_core::IndexInfo {
            name: "idx_active_users".to_string(),
            columns: vec!["email".to_string()],
            is_unique: true,
            is_primary: false,
            index_type: "btree".to_string(),
            comment: None,
            where_clause: Some("is_active = true".to_string()),
            include_columns: vec![],
            column_descending: vec![],
        };
        let def = exporter.index_info_to_definition(&idx);
        assert_eq!(def.where_clause.as_deref(), Some("is_active = true"));
    }

    #[test]
    fn test_index_info_to_definition_covering_index() {
        let exporter = make_exporter();
        let idx = zqlz_core::IndexInfo {
            name: "idx_covering".to_string(),
            columns: vec!["user_id".to_string()],
            is_unique: false,
            is_primary: false,
            index_type: "btree".to_string(),
            comment: None,
            where_clause: None,
            include_columns: vec!["email".to_string(), "created_at".to_string()],
            column_descending: vec![],
        };
        let def = exporter.index_info_to_definition(&idx);
        assert_eq!(def.include_columns, vec!["email", "created_at"]);
    }

    #[test]
    fn test_index_info_to_definition_unknown_method_preserved_in_raw() {
        let exporter = make_exporter();
        let idx = zqlz_core::IndexInfo {
            name: "idx_custom".to_string(),
            columns: vec!["embedding".to_string()],
            is_unique: false,
            is_primary: false,
            index_type: "hnsw".to_string(), // pgvector extension type
            comment: None,
            where_clause: None,
            include_columns: vec![],
            column_descending: vec![],
        };
        let def = exporter.index_info_to_definition(&idx);
        // Unknown type has no canonical variant but the raw string is preserved
        assert_eq!(def.index_method, None);
        assert_eq!(def.index_type_raw.as_deref(), Some("hnsw"));
    }

    // ===== canonicalize_default_expression tests =====

    #[test]
    fn test_canonicalize_now_variants_become_current_timestamp() {
        use crate::document::DefaultValue;
        // PostgreSQL, MySQL, and MariaDB all have these forms
        for raw in &[
            "now()",
            "NOW()",
            "CURRENT_TIMESTAMP",
            "current_timestamp",
            "localtimestamp",
        ] {
            let result = canonicalize_default_expression(raw);
            assert!(
                matches!(result, DefaultValue::CurrentTimestamp),
                "Expected CurrentTimestamp for '{raw}', got {result:?}"
            );
        }
    }

    #[test]
    fn test_canonicalize_current_date_variants() {
        use crate::document::DefaultValue;
        for raw in &["CURRENT_DATE", "curdate()", "today"] {
            let result = canonicalize_default_expression(raw);
            assert!(
                matches!(result, DefaultValue::CurrentDate),
                "Expected CurrentDate for '{raw}', got {result:?}"
            );
        }
    }

    #[test]
    fn test_canonicalize_current_time_variants() {
        use crate::document::DefaultValue;
        for raw in &["CURRENT_TIME", "curtime()", "localtime"] {
            let result = canonicalize_default_expression(raw);
            assert!(
                matches!(result, DefaultValue::CurrentTime),
                "Expected CurrentTime for '{raw}', got {result:?}"
            );
        }
    }

    #[test]
    fn test_canonicalize_uuid_variants() {
        use crate::document::DefaultValue;
        for raw in &["gen_random_uuid()", "UUID()", "NEWID()", "random_uuid()"] {
            let result = canonicalize_default_expression(raw);
            assert!(
                matches!(result, DefaultValue::GeneratedUuid),
                "Expected GeneratedUuid for '{raw}', got {result:?}"
            );
        }
    }

    #[test]
    fn test_canonicalize_unknown_expression_preserved() {
        use crate::document::DefaultValue;
        let raw = "my_custom_func()";
        let result = canonicalize_default_expression(raw);
        assert!(
            matches!(result, DefaultValue::Expression(ref s) if s == raw),
            "Expected Expression('{raw}'), got {result:?}"
        );
    }

    // ===== export_table_data pagination tests =====

    /// A connection that simulates a table with a fixed set of rows by parsing LIMIT/OFFSET
    /// from the SQL string and returning the appropriate slice of a pre-built row set.
    struct PaginatingConnection {
        total_rows: u32,
        queries_received: std::sync::Mutex<Vec<String>>,
    }

    impl PaginatingConnection {
        fn new(total_rows: u32) -> Self {
            Self {
                total_rows,
                queries_received: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn query_count(&self) -> usize {
            self.queries_received.lock().unwrap().len()
        }

        fn all_queries(&self) -> Vec<String> {
            self.queries_received.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl zqlz_core::Connection for PaginatingConnection {
        fn driver_name(&self) -> &str {
            "sqlite"
        }

        fn rename_table_sql(
            &self,
            table_name: &zqlz_core::SqlObjectName,
            new_table_name: &str,
        ) -> Result<String> {
            Ok(format!(
                "ALTER TABLE {} RENAME TO {}",
                self.render_qualified_name(table_name),
                self.quote_identifier(new_table_name)
            ))
        }

        fn drop_table_sql(
            &self,
            table_name: &zqlz_core::SqlObjectName,
            _options: zqlz_core::DropTableOptions,
        ) -> Result<String> {
            Ok(format!(
                "DROP TABLE {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn drop_view_sql(
            &self,
            view_name: &zqlz_core::SqlObjectName,
            _options: zqlz_core::DropViewOptions,
        ) -> Result<String> {
            Ok(format!(
                "DROP VIEW {}",
                self.render_qualified_name(view_name)
            ))
        }

        fn drop_trigger_sql(
            &self,
            trigger_name: &zqlz_core::SqlObjectName,
            _table_name: Option<&zqlz_core::SqlObjectName>,
            _options: zqlz_core::DropTriggerOptions,
        ) -> Result<String> {
            Ok(format!(
                "DROP TRIGGER {}",
                self.render_qualified_name(trigger_name)
            ))
        }

        fn truncate_table_sql(&self, table_name: &zqlz_core::SqlObjectName) -> Result<String> {
            Ok(format!(
                "TRUNCATE TABLE {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn duplicate_table_sql(
            &self,
            source_table_name: &zqlz_core::SqlObjectName,
            new_table_name: &zqlz_core::SqlObjectName,
        ) -> Result<String> {
            Ok(format!(
                "CREATE TABLE {} AS SELECT * FROM {}",
                self.render_qualified_name(new_table_name),
                self.render_qualified_name(source_table_name)
            ))
        }

        fn clear_table_sql(&self, table_name: &zqlz_core::SqlObjectName) -> Result<String> {
            Ok(format!(
                "DELETE FROM {}",
                self.render_qualified_name(table_name)
            ))
        }

        fn table_has_rows_sql(&self, table_name: &zqlz_core::SqlObjectName) -> Result<String> {
            Ok(format!(
                "SELECT 1 FROM {} LIMIT 1",
                self.render_qualified_name(table_name)
            ))
        }

        fn select_rows_sql(
            &self,
            table_name: &zqlz_core::SqlObjectName,
            projected_columns: &[String],
            where_clause_sql: Option<&str>,
        ) -> Result<String> {
            let projection = if projected_columns.is_empty() {
                "*".to_string()
            } else {
                projected_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let mut sql = format!(
                "SELECT {} FROM {}",
                projection,
                self.render_qualified_name(table_name)
            );
            if let Some(where_clause_sql) = where_clause_sql {
                sql.push_str(" WHERE ");
                sql.push_str(where_clause_sql);
            }
            Ok(sql)
        }

        fn select_distinct_rows_sql(
            &self,
            table_name: &zqlz_core::SqlObjectName,
            projected_columns: &[String],
            where_clause_sql: Option<&str>,
            order_by_columns: &[String],
            limit: u64,
        ) -> Result<String> {
            let mut sql = format!(
                "SELECT DISTINCT {} FROM {}",
                projected_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", "),
                self.render_qualified_name(table_name)
            );
            if let Some(where_clause_sql) = where_clause_sql {
                sql.push_str(" WHERE ");
                sql.push_str(where_clause_sql);
            }
            if !order_by_columns.is_empty() {
                sql.push_str(" ORDER BY ");
                sql.push_str(
                    &order_by_columns
                        .iter()
                        .map(|column| self.quote_identifier(column))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
            sql.push_str(&format!(" LIMIT {}", limit));
            Ok(sql)
        }

        fn insert_row_sql(
            &self,
            table_name: &zqlz_core::SqlObjectName,
            column_names: &[String],
            value_count: usize,
        ) -> Result<String> {
            let placeholders = (0..value_count)
                .map(|index| self.format_bind_placeholder(index))
                .collect::<Vec<_>>()
                .join(", ");
            let columns = column_names
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!(
                "INSERT INTO {} ({}) VALUES ({})",
                self.render_qualified_name(table_name),
                columns,
                placeholders
            ))
        }

        fn performance_metrics_query_sql(&self) -> Result<String> {
            Ok("SELECT 0 as total_queries".to_string())
        }

        async fn execute(
            &self,
            _sql: &str,
            _params: &[zqlz_core::Value],
        ) -> Result<StatementResult> {
            Err(ZqlzError::NotSupported("stub".into()))
        }

        async fn query(&self, sql: &str, _params: &[zqlz_core::Value]) -> Result<QueryResult> {
            self.queries_received.lock().unwrap().push(sql.to_string());

            // Parse LIMIT and OFFSET from the SQL to return the correct page slice.
            // The exporter always emits `... LIMIT <n> OFFSET <m>` so this simple
            // parse is sufficient for test purposes.
            let limit = parse_sql_clause(sql, "LIMIT");
            let offset = parse_sql_clause(sql, "OFFSET").unwrap_or(0);

            let limit = match limit {
                Some(n) => n,
                None => return Err(ZqlzError::Query("test: missing LIMIT".into())),
            };

            let start = offset.min(self.total_rows as u64) as u32;
            let end = (offset + limit).min(self.total_rows as u64) as u32;

            let col_names = vec!["id".to_string()];
            let rows: Vec<Row> = (start..end)
                .map(|i| Row::new(col_names.clone(), vec![zqlz_core::Value::Int32(i as i32)]))
                .collect();

            Ok(QueryResult {
                id: uuid::Uuid::new_v4(),
                columns: vec![ColumnMeta {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    ..Default::default()
                }],
                rows,
                total_rows: Some(self.total_rows as u64),
                is_estimated_total: false,
                affected_rows: 0,
                execution_time_ms: 0,
                warnings: vec![],
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
            Err(ZqlzError::NotSupported("stub".into()))
        }

        async fn close(&self) -> Result<()> {
            Ok(())
        }

        fn is_closed(&self) -> bool {
            false
        }
    }

    /// Extracts the integer value following a keyword like `LIMIT` or `OFFSET` from
    /// a SQL string. Returns `None` if the keyword is absent.
    fn parse_sql_clause(sql: &str, keyword: &str) -> Option<u64> {
        let upper = sql.to_uppercase();
        let pos = upper.find(keyword)?;
        let after = sql[pos + keyword.len()..].trim_start();
        after
            .split_ascii_whitespace()
            .next()
            .and_then(|token| token.parse::<u64>().ok())
    }

    fn make_paginating_exporter(total_rows: u32) -> (GenericExporter, Arc<PaginatingConnection>) {
        let conn = Arc::new(PaginatingConnection::new(total_rows));
        let exporter =
            GenericExporter::new(conn.clone() as Arc<dyn zqlz_core::Connection>, "sqlite");
        (exporter, conn)
    }

    fn make_table_def_with_id_column() -> TableDefinition {
        let mut table_def = TableDefinition::new("items");
        table_def.add_column(ColumnDefinition::new(
            "id",
            crate::CanonicalType::Integer,
            "INTEGER",
        ));
        table_def
    }

    #[tokio::test]
    async fn test_pagination_fetches_all_rows_across_multiple_pages() {
        let (exporter, conn) = make_paginating_exporter(2500);
        let table_def = make_table_def_with_id_column();
        let options = ExportOptions {
            batch_size: 1000,
            ..Default::default()
        };

        let table_data = exporter
            .export_table_data("items", &table_def, &options)
            .await
            .unwrap();

        assert_eq!(table_data.rows.len(), 2500);
        // 3 pages: [0,1000), [1000,2000), [2000,2500) — last page is partial, stops loop.
        assert_eq!(conn.query_count(), 3);
        assert!(!table_data.partial);
    }

    #[tokio::test]
    async fn test_pagination_exactly_on_page_boundary_issues_extra_empty_page() {
        let (exporter, conn) = make_paginating_exporter(2000);
        let table_def = make_table_def_with_id_column();
        let options = ExportOptions {
            batch_size: 1000,
            ..Default::default()
        };

        let table_data = exporter
            .export_table_data("items", &table_def, &options)
            .await
            .unwrap();

        assert_eq!(table_data.rows.len(), 2000);
        // 3 pages: [0,1000) full, [1000,2000) full, [2000,2000) empty → stops.
        assert_eq!(conn.query_count(), 3);
        assert!(!table_data.partial);
    }

    #[tokio::test]
    async fn test_pagination_row_limit_caps_export_and_marks_partial() {
        let (exporter, conn) = make_paginating_exporter(5000);
        let table_def = make_table_def_with_id_column();
        let options = ExportOptions {
            batch_size: 1000,
            row_limit: Some(2500),
            ..Default::default()
        };

        let table_data = exporter
            .export_table_data("items", &table_def, &options)
            .await
            .unwrap();

        assert_eq!(table_data.rows.len(), 2500);
        // Pages: [0,1000), [1000,2000), [2000,2500) — capped by row_limit.
        assert_eq!(conn.query_count(), 3);
        assert!(
            table_data.partial,
            "partial should be true when row_limit is hit"
        );
    }

    #[tokio::test]
    async fn test_pagination_row_limit_smaller_than_batch_size() {
        let (exporter, conn) = make_paginating_exporter(5000);
        let table_def = make_table_def_with_id_column();
        let options = ExportOptions {
            batch_size: 1000,
            row_limit: Some(300),
            ..Default::default()
        };

        let table_data = exporter
            .export_table_data("items", &table_def, &options)
            .await
            .unwrap();

        assert_eq!(table_data.rows.len(), 300);
        // Only one page needed — fetch_count = min(300, 1000) = 300.
        assert_eq!(conn.query_count(), 1);
        assert!(table_data.partial);
    }

    #[tokio::test]
    async fn test_pagination_single_page_no_partial_flag() {
        let (exporter, conn) = make_paginating_exporter(50);
        let table_def = make_table_def_with_id_column();
        let options = ExportOptions {
            batch_size: 1000,
            ..Default::default()
        };

        let table_data = exporter
            .export_table_data("items", &table_def, &options)
            .await
            .unwrap();

        assert_eq!(table_data.rows.len(), 50);
        assert_eq!(conn.query_count(), 1);
        assert!(!table_data.partial);
    }

    #[tokio::test]
    async fn test_pagination_empty_table_issues_one_query() {
        let (exporter, conn) = make_paginating_exporter(0);
        let table_def = make_table_def_with_id_column();
        let options = ExportOptions::default();

        let table_data = exporter
            .export_table_data("items", &table_def, &options)
            .await
            .unwrap();

        assert_eq!(table_data.rows.len(), 0);
        assert_eq!(conn.query_count(), 1);
        assert!(!table_data.partial);
    }

    #[tokio::test]
    async fn test_pagination_sql_uses_limit_offset_syntax() {
        let (exporter, conn) = make_paginating_exporter(1500);
        let table_def = make_table_def_with_id_column();
        let options = ExportOptions {
            batch_size: 1000,
            ..Default::default()
        };

        exporter
            .export_table_data("items", &table_def, &options)
            .await
            .unwrap();

        let queries = conn.all_queries();
        assert_eq!(queries.len(), 2);
        assert!(
            queries[0].contains("LIMIT 1000") && queries[0].contains("OFFSET 0"),
            "first page SQL: {}",
            queries[0]
        );
        assert!(
            queries[1].contains("LIMIT 1000") && queries[1].contains("OFFSET 1000"),
            "second page SQL: {}",
            queries[1]
        );
    }
}
