//! Import functionality for UDIF
//!
//! This module provides traits and utilities for importing data from
//! UDIF documents into databases.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

use crate::CanonicalType;
use crate::document::{
    ColumnDefinition, ForeignKeyAction, ForeignKeyConstraint, IndexDefinition,
    PrimaryKeyConstraint, TableDefinition, UdifDocument,
};
use crate::type_mapping::{TypeMapper, get_type_mapper};
use crate::value_encoding::{EncodedValue, decode_value};
use zqlz_core::{Connection, Value, ZqlzError};

/// Errors during import
#[derive(Debug, Error)]
pub enum ImportError {
    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Schema error: {0}")]
    SchemaError(String),

    #[error("Decoding error: {0}")]
    DecodingError(String),

    #[error("Type incompatibility: {message}")]
    TypeIncompatibility { message: String },

    #[error("Table already exists: {0}")]
    TableExists(String),

    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("Import cancelled")]
    Cancelled,

    #[error("Validation failed: {0}")]
    ValidationFailed(String),
}

impl From<ZqlzError> for ImportError {
    fn from(e: ZqlzError) -> Self {
        ImportError::QueryError(e.to_string())
    }
}

impl From<crate::value_encoding::EncodingError> for ImportError {
    fn from(e: crate::value_encoding::EncodingError) -> Self {
        ImportError::DecodingError(e.to_string())
    }
}

/// Options for import operations
#[derive(Debug, Clone)]
pub struct ImportOptions {
    /// What to do if a table already exists
    pub if_exists: IfTableExists,
    /// Whether to create tables
    pub create_tables: bool,
    /// Whether to import data
    pub import_data: bool,
    /// Whether to create indexes
    pub create_indexes: bool,
    /// Whether to create foreign keys
    pub create_foreign_keys: bool,
    /// Batch size for inserting rows
    pub batch_size: u32,
    /// Whether to use transactions
    pub use_transaction: bool,
    /// Tables to import (empty = all tables)
    pub include_tables: Vec<String>,
    /// Tables to skip
    pub exclude_tables: Vec<String>,
    /// Column mappings (table.column -> new_column_name)
    pub column_mappings: HashMap<String, String>,
    /// Table name mappings (old_name -> new_name)
    pub table_mappings: HashMap<String, String>,
    /// Whether to validate types before import
    pub validate_types: bool,
    /// Whether to continue on error
    pub continue_on_error: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            if_exists: IfTableExists::Error,
            create_tables: true,
            import_data: true,
            create_indexes: true,
            create_foreign_keys: true,
            batch_size: 1000,
            use_transaction: true,
            include_tables: Vec::new(),
            exclude_tables: Vec::new(),
            column_mappings: HashMap::new(),
            table_mappings: HashMap::new(),
            validate_types: true,
            continue_on_error: false,
        }
    }
}

/// What to do if a table already exists
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IfTableExists {
    /// Return an error
    Error,
    /// Skip the table
    Skip,
    /// Drop and recreate the table
    Replace,
    /// Truncate existing data and insert new
    Truncate,
    /// Append to existing data
    Append,
}

impl ImportOptions {
    pub fn append() -> Self {
        Self {
            if_exists: IfTableExists::Append,
            create_tables: false,
            ..Default::default()
        }
    }

    pub fn replace() -> Self {
        Self {
            if_exists: IfTableExists::Replace,
            ..Default::default()
        }
    }

    pub fn schema_only() -> Self {
        Self {
            import_data: false,
            ..Default::default()
        }
    }

    pub fn with_batch_size(mut self, size: u32) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.include_tables = tables;
        self
    }
}

/// Progress callback for import operations
pub type ImportProgressCallback = Box<dyn Fn(ImportProgress) + Send + Sync>;

/// Import progress information
#[derive(Debug, Clone)]
pub struct ImportProgress {
    /// Current phase of import
    pub phase: ImportPhase,
    /// Current table being imported
    pub current_table: Option<String>,
    /// Total number of tables
    pub total_tables: usize,
    /// Number of tables completed
    pub tables_completed: usize,
    /// Rows imported for current table
    pub rows_imported: u64,
    /// Total rows to import for current table
    pub total_rows: Option<u64>,
    /// Current error (if any)
    pub current_error: Option<String>,
}

/// Phases of the import process
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportPhase {
    /// Validating document
    Validating,
    /// Creating tables
    CreatingTables,
    /// Importing data
    ImportingData,
    /// Creating indexes
    CreatingIndexes,
    /// Creating foreign keys
    CreatingForeignKeys,
    /// Finalizing
    Finalizing,
    /// Complete
    Complete,
}

/// Result of an import operation
#[derive(Debug, Clone)]
pub struct ImportResult {
    /// Number of tables created
    pub tables_created: usize,
    /// Number of tables skipped
    pub tables_skipped: usize,
    /// Number of rows imported per table
    pub rows_imported: HashMap<String, u64>,
    /// Number of indexes created
    pub indexes_created: usize,
    /// Number of foreign keys created
    pub foreign_keys_created: usize,
    /// Warnings encountered during import
    pub warnings: Vec<ImportWarning>,
    /// Errors encountered (if continue_on_error was true)
    pub errors: Vec<String>,
}

impl ImportResult {
    pub fn new() -> Self {
        Self {
            tables_created: 0,
            tables_skipped: 0,
            rows_imported: HashMap::new(),
            indexes_created: 0,
            foreign_keys_created: 0,
            warnings: Vec::new(),
            errors: Vec::new(),
        }
    }

    pub fn total_rows(&self) -> u64 {
        self.rows_imported.values().sum()
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

impl Default for ImportResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Warning during import (non-fatal issues)
#[derive(Debug, Clone)]
pub struct ImportWarning {
    /// Table name (if applicable)
    pub table: Option<String>,
    /// Column name (if applicable)
    pub column: Option<String>,
    /// Warning message
    pub message: String,
    /// Type of warning
    pub kind: ImportWarningKind,
}

/// Types of import warnings
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportWarningKind {
    /// Type was converted/degraded
    TypeConversion,
    /// Precision was lost
    PrecisionLoss,
    /// Constraint was skipped
    ConstraintSkipped,
    /// Index was skipped
    IndexSkipped,
    /// Default value was modified
    DefaultModified,
}

/// Preview of what an import would do
#[derive(Debug, Clone)]
pub struct ImportPreview {
    /// Tables that would be created
    pub tables_to_create: Vec<String>,
    /// Tables that would be skipped
    pub tables_to_skip: Vec<String>,
    /// Tables that would be replaced
    pub tables_to_replace: Vec<String>,
    /// Total rows to import
    pub total_rows: u64,
    /// Type compatibility warnings
    pub type_warnings: Vec<TypeWarning>,
    /// Whether the import can proceed
    pub can_proceed: bool,
    /// Blocking issues (if any)
    pub blocking_issues: Vec<String>,
}

/// Type compatibility warning
#[derive(Debug, Clone)]
pub struct TypeWarning {
    /// Table name
    pub table: String,
    /// Column name
    pub column: String,
    /// Source canonical type
    pub source_type: CanonicalType,
    /// Target native type
    pub target_type: String,
    /// Whether data loss is possible
    pub possible_data_loss: bool,
    /// Description of the issue
    pub message: String,
}

/// Trait for importing data into a database connection
#[async_trait]
pub trait Importer: Send + Sync {
    /// Preview what an import would do without actually importing
    async fn preview(
        &self,
        doc: &UdifDocument,
        options: &ImportOptions,
    ) -> Result<ImportPreview, ImportError>;

    /// Validate type compatibility between document and target database
    fn validate_compatibility(&self, doc: &UdifDocument) -> Vec<TypeWarning>;

    /// Import a UDIF document into the database
    async fn import(
        &self,
        doc: &UdifDocument,
        options: &ImportOptions,
    ) -> Result<ImportResult, ImportError>;

    /// Import with progress callback
    async fn import_with_progress(
        &self,
        doc: &UdifDocument,
        options: &ImportOptions,
        progress: ImportProgressCallback,
    ) -> Result<ImportResult, ImportError>;
}

/// Generic importer implementation that works with any Connection
pub struct GenericImporter {
    connection: Arc<dyn Connection>,
    type_mapper: Box<dyn TypeMapper>,
}

impl GenericImporter {
    pub fn new(connection: Arc<dyn Connection>) -> Self {
        let driver = connection.driver_name();
        let type_mapper = get_type_mapper(driver);
        Self {
            connection,
            type_mapper,
        }
    }

    pub fn with_type_mapper(
        connection: Arc<dyn Connection>,
        type_mapper: Box<dyn TypeMapper>,
    ) -> Self {
        Self {
            connection,
            type_mapper,
        }
    }

    fn should_include_table(&self, table_name: &str, options: &ImportOptions) -> bool {
        if !options.include_tables.is_empty() {
            if !options.include_tables.iter().any(|t| t == table_name) {
                return false;
            }
        }
        if options.exclude_tables.iter().any(|t| t == table_name) {
            return false;
        }
        true
    }

    fn get_target_table_name(&self, source_name: &str, options: &ImportOptions) -> String {
        options
            .table_mappings
            .get(source_name)
            .cloned()
            .unwrap_or_else(|| source_name.to_string())
    }

    fn generate_create_table_sql(&self, table: &TableDefinition) -> String {
        let mut sql = format!("CREATE TABLE {} (\n", self.quote_identifier(&table.name));

        let column_defs: Vec<String> = table
            .columns
            .iter()
            .map(|col| self.generate_column_sql(col))
            .collect();

        sql.push_str(&column_defs.join(",\n"));

        if let Some(ref pk) = table.primary_key {
            sql.push_str(",\n");
            sql.push_str(&self.generate_primary_key_sql(pk));
        }

        for unique in &table.unique_constraints {
            sql.push_str(",\n");
            sql.push_str(&format!(
                "  UNIQUE ({})",
                unique
                    .columns
                    .iter()
                    .map(|c| self.quote_identifier(c))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        sql.push_str("\n)");
        sql
    }

    fn generate_column_sql(&self, col: &ColumnDefinition) -> String {
        let native_type = self.type_mapper.from_canonical(&col.canonical_type);
        let mut sql = format!("  {} {}", self.quote_identifier(&col.name), native_type);

        if !col.nullable {
            sql.push_str(" NOT NULL");
        }

        if let Some(ref default) = col.default_value {
            match default {
                crate::document::DefaultValue::Literal(val) => {
                    if let Ok(value) = decode_value(val) {
                        sql.push_str(&format!(" DEFAULT {}", self.value_to_sql(&value)));
                    }
                }
                crate::document::DefaultValue::Expression(expr) => {
                    sql.push_str(&format!(" DEFAULT {}", expr));
                }
                crate::document::DefaultValue::Null => {
                    sql.push_str(" DEFAULT NULL");
                }
                crate::document::DefaultValue::AutoGenerated => {}
            }
        }

        sql
    }

    fn generate_primary_key_sql(&self, pk: &PrimaryKeyConstraint) -> String {
        let cols: Vec<String> = pk
            .columns
            .iter()
            .map(|c| self.quote_identifier(c))
            .collect();
        format!("  PRIMARY KEY ({})", cols.join(", "))
    }

    fn generate_index_sql(&self, table_name: &str, index: &IndexDefinition) -> String {
        let unique = if index.unique { "UNIQUE " } else { "" };
        let cols: Vec<String> = index
            .columns
            .iter()
            .map(|c| self.quote_identifier(&c.column))
            .collect();
        format!(
            "CREATE {}INDEX {} ON {} ({})",
            unique,
            self.quote_identifier(&index.name),
            self.quote_identifier(table_name),
            cols.join(", ")
        )
    }

    fn generate_foreign_key_sql(&self, table_name: &str, fk: &ForeignKeyConstraint) -> String {
        let cols: Vec<String> = fk
            .columns
            .iter()
            .map(|c| self.quote_identifier(c))
            .collect();
        let ref_cols: Vec<String> = fk
            .referenced_columns
            .iter()
            .map(|c| self.quote_identifier(c))
            .collect();

        let on_delete = match fk.on_delete {
            ForeignKeyAction::Cascade => " ON DELETE CASCADE",
            ForeignKeyAction::SetNull => " ON DELETE SET NULL",
            ForeignKeyAction::SetDefault => " ON DELETE SET DEFAULT",
            ForeignKeyAction::Restrict => " ON DELETE RESTRICT",
            ForeignKeyAction::NoAction => "",
        };

        let on_update = match fk.on_update {
            ForeignKeyAction::Cascade => " ON UPDATE CASCADE",
            ForeignKeyAction::SetNull => " ON UPDATE SET NULL",
            ForeignKeyAction::SetDefault => " ON UPDATE SET DEFAULT",
            ForeignKeyAction::Restrict => " ON UPDATE RESTRICT",
            ForeignKeyAction::NoAction => "",
        };

        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}){}{}",
            self.quote_identifier(table_name),
            self.quote_identifier(
                fk.name
                    .as_ref()
                    .unwrap_or(&format!("fk_{}_{}", table_name, fk.referenced_table))
            ),
            cols.join(", "),
            self.quote_identifier(&fk.referenced_table),
            ref_cols.join(", "),
            on_delete,
            on_update
        )
    }

    #[allow(dead_code)]
    fn generate_insert_sql(&self, table: &TableDefinition, row_count: usize) -> String {
        let cols: Vec<String> = table
            .columns
            .iter()
            .map(|c| self.quote_identifier(&c.name))
            .collect();

        let placeholders = self.generate_placeholders(table.columns.len());
        let value_rows: Vec<String> = (0..row_count).map(|_| placeholders.clone()).collect();

        format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.quote_identifier(&table.name),
            cols.join(", "),
            value_rows.join(", ")
        )
    }

    #[allow(dead_code)]
    fn generate_placeholders(&self, count: usize) -> String {
        let driver = self.connection.driver_name();
        let placeholders: Vec<String> = (1..=count)
            .map(|i| {
                if driver == "postgresql" {
                    format!("${}", i)
                } else {
                    "?".to_string()
                }
            })
            .collect();
        format!("({})", placeholders.join(", "))
    }

    fn quote_identifier(&self, name: &str) -> String {
        match self.connection.driver_name() {
            "mysql" => format!("`{}`", name),
            "mssql" => format!("[{}]", name),
            _ => format!("\"{}\"", name),
        }
    }

    fn value_to_sql(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::Int8(i) => i.to_string(),
            Value::Int16(i) => i.to_string(),
            Value::Int32(i) => i.to_string(),
            Value::Int64(i) => i.to_string(),
            Value::Float32(f) => f.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::Decimal(d) => d.clone(),
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Bytes(_) => "NULL".to_string(),
            Value::Uuid(u) => format!("'{}'", u),
            Value::Date(d) => format!("'{}'", d),
            Value::Time(t) => format!("'{}'", t),
            Value::DateTime(dt) => format!("'{}'", dt),
            Value::DateTimeUtc(dt) => format!("'{}'", dt),
            Value::Json(j) => format!("'{}'", j.to_string().replace('\'', "''")),
            Value::Array(_) => "NULL".to_string(),
        }
    }

    fn decode_row(&self, encoded: &[EncodedValue]) -> Result<Vec<Value>, ImportError> {
        encoded
            .iter()
            .map(|v| decode_value(v).map_err(ImportError::from))
            .collect()
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool, ImportError> {
        let schema = self
            .connection
            .as_schema_introspection()
            .ok_or_else(|| ImportError::SchemaError("Schema introspection not supported".into()))?;

        let tables = schema
            .list_tables(None)
            .await
            .map_err(|e| ImportError::SchemaError(e.to_string()))?;
        Ok(tables.iter().any(|t| t.name == table_name))
    }
}

#[async_trait]
impl Importer for GenericImporter {
    async fn preview(
        &self,
        doc: &UdifDocument,
        options: &ImportOptions,
    ) -> Result<ImportPreview, ImportError> {
        let type_warnings = self.validate_compatibility(doc);
        let mut preview = ImportPreview {
            tables_to_create: Vec::new(),
            tables_to_skip: Vec::new(),
            tables_to_replace: Vec::new(),
            total_rows: 0,
            type_warnings,
            can_proceed: true,
            blocking_issues: Vec::new(),
        };

        for (table_name, _) in &doc.schema.tables {
            if !self.should_include_table(table_name, options) {
                preview.tables_to_skip.push(table_name.clone());
                continue;
            }

            let target_name = self.get_target_table_name(table_name, options);
            let exists = self.table_exists(&target_name).await?;

            if exists {
                match options.if_exists {
                    IfTableExists::Error => {
                        preview
                            .blocking_issues
                            .push(format!("Table '{}' already exists", target_name));
                        preview.can_proceed = false;
                    }
                    IfTableExists::Skip => {
                        preview.tables_to_skip.push(table_name.clone());
                    }
                    IfTableExists::Replace | IfTableExists::Truncate => {
                        preview.tables_to_replace.push(table_name.clone());
                    }
                    IfTableExists::Append => {
                        preview.tables_to_replace.push(table_name.clone());
                    }
                }
            } else {
                preview.tables_to_create.push(table_name.clone());
            }

            if let Some(data) = doc.data.get(table_name) {
                preview.total_rows += data.rows.len() as u64;
            }
        }

        Ok(preview)
    }

    fn validate_compatibility(&self, doc: &UdifDocument) -> Vec<TypeWarning> {
        let mut warnings = Vec::new();

        for (table_name, table_def) in &doc.schema.tables {
            for col in &table_def.columns {
                if !self.type_mapper.supports_type(&col.canonical_type) {
                    let target_type = self.type_mapper.from_canonical(&col.canonical_type);
                    let fallback = col.canonical_type.fallback_type();

                    warnings.push(TypeWarning {
                        table: table_name.clone(),
                        column: col.name.clone(),
                        source_type: col.canonical_type.clone(),
                        target_type: target_type.clone(),
                        possible_data_loss: !matches!(
                            fallback,
                            CanonicalType::Text | CanonicalType::Json { .. }
                        ),
                        message: format!(
                            "Type '{}' will be converted to '{}'",
                            col.canonical_type.display_name(),
                            target_type
                        ),
                    });
                }
            }
        }

        warnings
    }

    async fn import(
        &self,
        doc: &UdifDocument,
        options: &ImportOptions,
    ) -> Result<ImportResult, ImportError> {
        self.import_with_progress(doc, options, Box::new(|_| {}))
            .await
    }

    async fn import_with_progress(
        &self,
        doc: &UdifDocument,
        options: &ImportOptions,
        progress: ImportProgressCallback,
    ) -> Result<ImportResult, ImportError> {
        let mut result = ImportResult::new();

        if options.validate_types {
            progress(ImportProgress {
                phase: ImportPhase::Validating,
                current_table: None,
                total_tables: doc.schema.tables.len(),
                tables_completed: 0,
                rows_imported: 0,
                total_rows: None,
                current_error: None,
            });

            let warnings = self.validate_compatibility(doc);
            for warning in warnings {
                result.warnings.push(ImportWarning {
                    table: Some(warning.table),
                    column: Some(warning.column),
                    message: warning.message,
                    kind: ImportWarningKind::TypeConversion,
                });
            }
        }

        let tables: Vec<_> = doc
            .schema
            .tables
            .iter()
            .filter(|(name, _)| self.should_include_table(name, options))
            .collect();

        let total_tables = tables.len();

        if options.create_tables {
            progress(ImportProgress {
                phase: ImportPhase::CreatingTables,
                current_table: None,
                total_tables,
                tables_completed: 0,
                rows_imported: 0,
                total_rows: None,
                current_error: None,
            });

            for (idx, (table_name, table_def)) in tables.iter().enumerate() {
                let target_name = self.get_target_table_name(table_name, options);
                let exists = self.table_exists(&target_name).await?;

                if exists {
                    match options.if_exists {
                        IfTableExists::Error => {
                            return Err(ImportError::TableExists(target_name));
                        }
                        IfTableExists::Skip => {
                            result.tables_skipped += 1;
                            continue;
                        }
                        IfTableExists::Replace => {
                            let drop_sql =
                                format!("DROP TABLE {}", self.quote_identifier(&target_name));
                            self.connection
                                .execute(&drop_sql, &[])
                                .await
                                .map_err(|e| ImportError::QueryError(e.to_string()))?;
                        }
                        IfTableExists::Truncate => {
                            let truncate_sql =
                                format!("DELETE FROM {}", self.quote_identifier(&target_name));
                            self.connection
                                .execute(&truncate_sql, &[])
                                .await
                                .map_err(|e| ImportError::QueryError(e.to_string()))?;
                        }
                        IfTableExists::Append => {}
                    }
                }

                if !exists || options.if_exists == IfTableExists::Replace {
                    let create_sql = self.generate_create_table_sql(table_def);
                    self.connection
                        .execute(&create_sql, &[])
                        .await
                        .map_err(|e| ImportError::QueryError(e.to_string()))?;
                    result.tables_created += 1;
                }

                progress(ImportProgress {
                    phase: ImportPhase::CreatingTables,
                    current_table: Some(table_name.to_string()),
                    total_tables,
                    tables_completed: idx + 1,
                    rows_imported: 0,
                    total_rows: None,
                    current_error: None,
                });
            }
        }

        if options.import_data {
            progress(ImportProgress {
                phase: ImportPhase::ImportingData,
                current_table: None,
                total_tables,
                tables_completed: 0,
                rows_imported: 0,
                total_rows: None,
                current_error: None,
            });

            for (idx, (table_name, table_def)) in tables.iter().enumerate() {
                if let Some(table_data) = doc.data.get(*table_name) {
                    let target_name = self.get_target_table_name(table_name, options);
                    let total_rows = table_data.rows.len();
                    let mut rows_imported = 0u64;

                    for chunk in table_data.rows.chunks(options.batch_size as usize) {
                        for row in chunk {
                            let values = self.decode_row(&row.values)?;
                            let placeholders: Vec<String> = (1..=values.len())
                                .map(|i| {
                                    if self.connection.driver_name() == "postgresql" {
                                        format!("${}", i)
                                    } else {
                                        "?".to_string()
                                    }
                                })
                                .collect();

                            let cols: Vec<String> = table_def
                                .columns
                                .iter()
                                .map(|c| self.quote_identifier(&c.name))
                                .collect();

                            let sql = format!(
                                "INSERT INTO {} ({}) VALUES ({})",
                                self.quote_identifier(&target_name),
                                cols.join(", "),
                                placeholders.join(", ")
                            );

                            match self.connection.execute(&sql, &values).await {
                                Ok(_) => {
                                    rows_imported += 1;
                                }
                                Err(e) => {
                                    if options.continue_on_error {
                                        result.errors.push(e.to_string());
                                    } else {
                                        return Err(ImportError::QueryError(e.to_string()));
                                    }
                                }
                            }
                        }

                        progress(ImportProgress {
                            phase: ImportPhase::ImportingData,
                            current_table: Some(table_name.to_string()),
                            total_tables,
                            tables_completed: idx,
                            rows_imported,
                            total_rows: Some(total_rows as u64),
                            current_error: None,
                        });
                    }

                    result
                        .rows_imported
                        .insert(table_name.to_string(), rows_imported);
                }
            }
        }

        if options.create_indexes {
            progress(ImportProgress {
                phase: ImportPhase::CreatingIndexes,
                current_table: None,
                total_tables,
                tables_completed: total_tables,
                rows_imported: 0,
                total_rows: None,
                current_error: None,
            });

            for (table_name, table_def) in &tables {
                let target_name = self.get_target_table_name(table_name, options);
                for index in &table_def.indexes {
                    if index.unique {
                        continue;
                    }
                    let sql = self.generate_index_sql(&target_name, index);
                    match self.connection.execute(&sql, &[]).await {
                        Ok(_) => {
                            result.indexes_created += 1;
                        }
                        Err(e) => {
                            if options.continue_on_error {
                                result.warnings.push(ImportWarning {
                                    table: Some(table_name.to_string()),
                                    column: None,
                                    message: format!("Failed to create index: {}", e),
                                    kind: ImportWarningKind::IndexSkipped,
                                });
                            } else {
                                return Err(ImportError::QueryError(e.to_string()));
                            }
                        }
                    }
                }
            }
        }

        if options.create_foreign_keys {
            progress(ImportProgress {
                phase: ImportPhase::CreatingForeignKeys,
                current_table: None,
                total_tables,
                tables_completed: total_tables,
                rows_imported: 0,
                total_rows: None,
                current_error: None,
            });

            for (table_name, table_def) in &tables {
                let target_name = self.get_target_table_name(table_name, options);
                for fk in &table_def.foreign_keys {
                    let sql = self.generate_foreign_key_sql(&target_name, fk);
                    match self.connection.execute(&sql, &[]).await {
                        Ok(_) => {
                            result.foreign_keys_created += 1;
                        }
                        Err(e) => {
                            if options.continue_on_error {
                                result.warnings.push(ImportWarning {
                                    table: Some(table_name.to_string()),
                                    column: None,
                                    message: format!("Failed to create foreign key: {}", e),
                                    kind: ImportWarningKind::ConstraintSkipped,
                                });
                            } else {
                                return Err(ImportError::QueryError(e.to_string()));
                            }
                        }
                    }
                }
            }
        }

        progress(ImportProgress {
            phase: ImportPhase::Complete,
            current_table: None,
            total_tables,
            tables_completed: total_tables,
            rows_imported: result.total_rows(),
            total_rows: Some(result.total_rows()),
            current_error: None,
        });

        Ok(result)
    }
}

/// Helper functions for working with UDIF documents during import
pub mod helpers {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;

    /// Parse a UDIF document from JSON
    pub fn from_json(json: &str) -> Result<UdifDocument, ImportError> {
        serde_json::from_str(json).map_err(|e| ImportError::DecodingError(e.to_string()))
    }

    /// Parse a UDIF document from compressed JSON (gzip)
    pub fn from_json_compressed(data: &[u8]) -> Result<UdifDocument, ImportError> {
        let mut decoder = GzDecoder::new(data);
        let mut json = String::new();
        decoder
            .read_to_string(&mut json)
            .map_err(|e| ImportError::DecodingError(e.to_string()))?;
        from_json(&json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_options_default() {
        let options = ImportOptions::default();
        assert!(options.create_tables);
        assert!(options.import_data);
        assert!(options.create_indexes);
        assert!(options.create_foreign_keys);
        assert_eq!(options.if_exists, IfTableExists::Error);
    }

    #[test]
    fn test_import_options_append() {
        let options = ImportOptions::append();
        assert!(!options.create_tables);
        assert_eq!(options.if_exists, IfTableExists::Append);
    }

    #[test]
    fn test_import_result() {
        let mut result = ImportResult::new();
        result.rows_imported.insert("users".into(), 100);
        result.rows_imported.insert("orders".into(), 500);
        assert_eq!(result.total_rows(), 600);
        assert!(!result.has_errors());
    }
}
