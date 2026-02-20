//! Import functionality for UDIF
//!
//! This module provides traits and utilities for importing data from
//! UDIF documents into databases.

use async_trait::async_trait;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use thiserror::Error;

use crate::CanonicalType;
use crate::document::{
    CheckConstraint, ColumnDefinition, EnumDefinition, ForeignKeyAction, ForeignKeyConstraint,
    IndexDefinition, IndexMethod, PrimaryKeyConstraint, TableDefinition, UdifDocument,
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
    /// Creating enum types (PostgreSQL only)
    CreatingEnumTypes,
    /// Creating tables
    CreatingTables,
    /// Importing data
    ImportingData,
    /// Creating indexes
    CreatingIndexes,
    /// Creating foreign keys
    CreatingForeignKeys,
    /// Restoring sequence / auto-increment counters
    RestoringSequences,
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
    /// Consolidated degradation report: all schema features that could not be
    /// preserved on the target driver, grouped here for the Summary step in the UI.
    pub degradation_warnings: Vec<DegradationWarning>,
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
            degradation_warnings: Vec::new(),
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

impl ImportResult {
    /// Push an `ImportWarning` and, simultaneously, its `DegradationWarning` equivalent.
    ///
    /// Keeping both collections in sync here rather than at each call site avoids the
    /// risk of one being updated without the other.
    fn push_warning(
        &mut self,
        warning: ImportWarning,
        category: DegradationCategory,
        object_name: Option<String>,
        source_feature: impl Into<String>,
        target_action: impl Into<String>,
        severity: DegradationSeverity,
    ) {
        let table_name = warning
            .table
            .clone()
            .unwrap_or_else(|| "<unknown>".to_string());
        self.degradation_warnings.push(DegradationWarning {
            category,
            table_name,
            object_name,
            source_feature: source_feature.into(),
            target_action: target_action.into(),
            severity,
        });
        self.warnings.push(warning);
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
    /// Index was skipped entirely (no equivalent on the target driver)
    IndexSkipped,
    /// Index was created but with a degraded method or missing clause
    /// (e.g. GIN→BTREE on MySQL, partial index without WHERE on MySQL)
    IndexDegraded,
    /// Default value was modified
    DefaultModified,
    /// CHECK constraint is present but not enforced on this target driver/version
    CheckConstraintNonEnforced,
    /// Generated column expression may not be valid on the target driver, or the
    /// storage mode (VIRTUAL vs STORED) was coerced because the target driver does
    /// not support the source storage mode
    GeneratedColumnDegraded,
}

/// Broad schema-feature category for a degradation warning, used to group
/// warnings in the summary report by concern area.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradationCategory {
    Index,
    ForeignKey,
    CheckConstraint,
    GeneratedColumn,
    DefaultValue,
    TypeConversion,
    Enum,
    Other,
}

impl DegradationCategory {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Index => "Index",
            Self::ForeignKey => "Foreign Key",
            Self::CheckConstraint => "Check Constraint",
            Self::GeneratedColumn => "Generated Column",
            Self::DefaultValue => "Default Value",
            Self::TypeConversion => "Type Conversion",
            Self::Enum => "Enum Type",
            Self::Other => "Other",
        }
    }
}

/// How severe the degradation is: the feature was reduced in fidelity (Warning)
/// or removed entirely (Dropped).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegradationSeverity {
    /// Feature is present on the target but with reduced fidelity
    Warning,
    /// Feature was removed entirely because the target driver has no equivalent
    Dropped,
}

impl DegradationSeverity {
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Warning => "Warning",
            Self::Dropped => "Dropped",
        }
    }
}

/// A single schema-feature loss surfaced in the consolidated degradation report.
///
/// These are collected alongside `ImportWarning`s during the import run and
/// returned in `ImportResult::degradation_warnings`. The wizard's Summary step
/// groups them by category so the user can see the full picture of what changed.
#[derive(Debug, Clone)]
pub struct DegradationWarning {
    pub category: DegradationCategory,
    /// Table that owns the affected object (index, column, constraint, …)
    pub table_name: String,
    /// Name of the specific object (index name, column name, constraint name).
    /// `None` for table-level features like storage options.
    pub object_name: Option<String>,
    /// Human-readable description of the source feature that could not be preserved
    pub source_feature: String,
    /// What was done instead (e.g. "created as BTREE", "dropped", "NULL default used")
    pub target_action: String,
    pub severity: DegradationSeverity,
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

/// Outcome of mapping an index from the source document to the target driver.
#[derive(Debug)]
enum IndexMappingOutcome {
    /// The index can be created but only with a degraded method or missing clause.
    /// The SQL is still valid; each message should become an `IndexDegraded` warning.
    Degraded { sql: String, messages: Vec<String> },
    /// The index has no equivalent on the target driver and must be dropped.
    Skipped(String),
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

    #[allow(dead_code)]
    fn generate_create_table_sql(&self, table: &TableDefinition) -> String {
        self.generate_create_table_sql_with_enums(table, &HashMap::new())
    }

    /// Core CREATE TABLE generator that accepts an optional map of
    /// `(table_name, column_name) → synthesized_enum_type_name`.
    ///
    /// The synthesized map is populated by `resolve_enum_types` for MySQL→PostgreSQL imports
    /// where anonymous enum columns need to reference a named type instead of falling back
    /// to an inline representation.
    fn generate_create_table_sql_with_enums(
        &self,
        table: &TableDefinition,
        synthesized_enums: &HashMap<(String, String), String>,
    ) -> String {
        let mut sql = format!("CREATE TABLE {} (\n", self.quote_identifier(&table.name));

        let column_defs: Vec<String> = table
            .columns
            .iter()
            .map(|col| {
                let override_name = synthesized_enums
                    .get(&(table.name.clone(), col.name.clone()))
                    .map(|s| s.as_str());
                self.generate_column_sql_with_enum_override(col, override_name)
            })
            .collect();

        sql.push_str(&column_defs.join(",\n"));

        if let Some(ref pk) = table.primary_key {
            if !pk.columns.is_empty() {
                sql.push_str(",\n");
                sql.push_str(&self.generate_primary_key_sql(pk));
            }
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

        for check in &table.check_constraints {
            sql.push_str(",\n");
            sql.push_str(&self.generate_check_constraint_sql(check));
        }

        sql.push_str("\n)");
        sql
    }

    /// Generates a `CONSTRAINT <name> CHECK (<expression>)` or `CHECK (<expression>)` clause.
    ///
    /// Named constraints use the CONSTRAINT keyword; unnamed ones omit it to produce
    /// valid SQL on all three target drivers.  PostgreSQL-specific cast syntax
    /// (`::typename`) is stripped when targeting SQLite or MySQL.
    fn generate_check_constraint_sql(&self, check: &CheckConstraint) -> String {
        let driver = self.connection.driver_name();
        let expression = if driver == "postgresql" {
            check.expression.clone()
        } else {
            strip_pg_casts(&check.expression)
        };
        match &check.name {
            Some(name) => format!(
                "  CONSTRAINT {} CHECK ({})",
                self.quote_identifier(name),
                expression
            ),
            None => format!("  CHECK ({})", expression),
        }
    }

    fn generate_column_sql(&self, col: &ColumnDefinition) -> String {
        let native_type = self.type_mapper.from_canonical(&col.canonical_type);
        let mut sql = format!("  {} {}", self.quote_identifier(&col.name), native_type);

        // SQLite has no native ENUM type, so we represent it as TEXT and add an
        // inline CHECK constraint to restrict values to the declared set.
        // This must come before the NOT NULL / DEFAULT clauses.
        if let CanonicalType::Enum { ref values, .. } = col.canonical_type {
            let driver = self.connection.driver_name();
            if driver == "sqlite" && !values.is_empty() {
                let quoted_values: Vec<String> = values
                    .iter()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .collect();
                sql.push_str(&format!(
                    " CHECK ({} IN ({}))",
                    self.quote_identifier(&col.name),
                    quoted_values.join(", ")
                ));
            }
        }

        // Generated columns use GENERATED ALWAYS AS syntax instead of DEFAULT.
        // All three drivers (PostgreSQL 12+, MySQL 5.7+, SQLite 3.31+) support
        // this clause, though PostgreSQL only supports STORED (not VIRTUAL).
        // Cross-driver expression warnings are emitted separately via
        // generate_generated_column_warnings.
        if let Some(ref expr) = col.generation_expression {
            let driver = self.connection.driver_name();
            // PostgreSQL does not support VIRTUAL generated columns; STORED is
            // the only option.  We always emit STORED for PostgreSQL regardless
            // of the source flag.  The warning is emitted separately.
            let storage = if col.is_generated_stored || driver == "postgresql" {
                "STORED"
            } else {
                "VIRTUAL"
            };
            sql.push_str(&format!(" GENERATED ALWAYS AS ({}) {}", expr, storage));
            return sql;
        }

        if !col.nullable {
            sql.push_str(" NOT NULL");
        }

        if let Some(ref default) = col.default_value {
            let driver = self.connection.driver_name();
            match default {
                crate::document::DefaultValue::Literal(val) => {
                    if let Ok(value) = decode_value(val) {
                        sql.push_str(&format!(" DEFAULT {}", self.value_to_sql(&value)));
                    }
                }
                crate::document::DefaultValue::Expression(expr) => {
                    // Auto-increment columns carry a driver-specific sequence expression
                    // (e.g. nextval('...') on PostgreSQL) as their default.  That expression
                    // is meaningless — and often invalid — on the target driver, so skip it;
                    // the auto-increment behaviour is expressed through the column flag and
                    // handled by the type mapper and sequence-restore logic.
                    if !col.auto_increment {
                        let effective = if driver == "postgresql" {
                            expr.clone()
                        } else {
                            // Strip PG cast syntax (`::typename`) that is illegal on
                            // SQLite and MySQL.  If the result still contains a function
                            // call we drop the entire DEFAULT — a driver-specific function
                            // (e.g. `to_timestamp`, `date_trunc`) cannot be evaluated by
                            // the target engine.  A warning is emitted separately by
                            // `generate_column_default_warnings`.
                            let stripped = strip_pg_casts(expr);
                            if stripped.contains('(') {
                                String::new()
                            } else {
                                stripped
                            }
                        };
                        if !effective.is_empty() {
                            sql.push_str(&format!(" DEFAULT {}", effective));
                        }
                    }
                }
                crate::document::DefaultValue::Null => {
                    sql.push_str(" DEFAULT NULL");
                }
                crate::document::DefaultValue::AutoGenerated => {}
                // SQL standard CURRENT_TIMESTAMP is supported on all three target drivers.
                crate::document::DefaultValue::CurrentTimestamp => {
                    sql.push_str(" DEFAULT CURRENT_TIMESTAMP");
                }
                // CURRENT_DATE is standard SQL; all three drivers support it.
                crate::document::DefaultValue::CurrentDate => {
                    sql.push_str(" DEFAULT CURRENT_DATE");
                }
                // CURRENT_TIME is standard SQL; all three drivers support it.
                crate::document::DefaultValue::CurrentTime => {
                    sql.push_str(" DEFAULT CURRENT_TIME");
                }
                // CURRENT_USER is supported on PostgreSQL and MySQL but not SQLite.
                // On SQLite we fall back to a literal NULL to keep the DDL parseable;
                // a warning is emitted separately in generate_column_default_warnings.
                crate::document::DefaultValue::CurrentUser => match driver {
                    "sqlite" => sql.push_str(" DEFAULT NULL"),
                    _ => sql.push_str(" DEFAULT CURRENT_USER"),
                },
                // UUID generation syntax differs across drivers; SQLite has no built-in.
                // Warnings for degraded cases are emitted in generate_column_default_warnings.
                crate::document::DefaultValue::GeneratedUuid => match driver {
                    "postgresql" => sql.push_str(" DEFAULT gen_random_uuid()"),
                    "mysql" => sql.push_str(" DEFAULT (UUID())"),
                    // SQLite has no built-in UUID function; emit no default so the
                    // application layer must supply values.
                    _ => {}
                },
            }
        }

        sql
    }

    /// Returns any degradation warnings for semantic default-value variants that cannot
    /// be faithfully expressed on the target driver.
    fn generate_column_default_warnings(
        &self,
        table_name: &str,
        col: &ColumnDefinition,
    ) -> Vec<ImportWarning> {
        let driver = self.connection.driver_name();
        let Some(ref default) = col.default_value else {
            return vec![];
        };
        match default {
            crate::document::DefaultValue::CurrentUser if driver == "sqlite" => vec![ImportWarning {
                table: Some(table_name.to_owned()),
                column: Some(col.name.clone()),
                message: format!(
                    "Column '{}' in table '{}' had DEFAULT CURRENT_USER which is not \
                     supported on SQLite; defaulting to NULL",
                    col.name, table_name
                ),
                kind: ImportWarningKind::DefaultModified,
            }],
            crate::document::DefaultValue::GeneratedUuid if driver == "sqlite" => {
                vec![ImportWarning {
                    table: Some(table_name.to_owned()),
                    column: Some(col.name.clone()),
                    message: format!(
                        "Column '{}' in table '{}' had a UUID-generation default which is not \
                         natively supported on SQLite; no default was emitted — the application \
                         must supply UUID values explicitly",
                        col.name, table_name
                    ),
                    kind: ImportWarningKind::DefaultModified,
                }]
            }
            crate::document::DefaultValue::Expression(expr) => {
                // On non-PG targets we strip PG cast syntax and drop the DEFAULT entirely
                // if the expression still contains a function call (driver-specific).
                // Warn accordingly so users are aware of what was dropped or left as-is.
                if driver == "postgresql" {
                    return vec![];
                }
                let stripped = strip_pg_casts(expr);
                if stripped.contains('(') {
                    vec![ImportWarning {
                        table: Some(table_name.to_owned()),
                        column: Some(col.name.clone()),
                        message: format!(
                            "Column '{}' in table '{}' had DEFAULT expression '{}' containing \
                             a function call that cannot be evaluated on {}; the DEFAULT was \
                             dropped — you may need to add it manually",
                            col.name, table_name, expr, driver
                        ),
                        kind: ImportWarningKind::DefaultModified,
                    }]
                } else if stripped != *expr {
                    // Cast-stripping changed the expression — warn that it was adapted.
                    vec![ImportWarning {
                        table: Some(table_name.to_owned()),
                        column: Some(col.name.clone()),
                        message: format!(
                            "Column '{}' in table '{}' had DEFAULT expression '{}' with \
                             PostgreSQL cast syntax; adapted to '{}' for {}",
                            col.name, table_name, expr, stripped, driver
                        ),
                        kind: ImportWarningKind::DefaultModified,
                    }]
                } else {
                    vec![]
                }
            }
            _ => vec![],
        }
    }

    /// Returns degradation warnings for generated (computed) columns.
    ///
    /// Two sources of degradation exist:
    /// 1. The expression itself may use driver-specific functions (e.g. `SUBSTRING_INDEX`
    ///    on MySQL, `split_part` on PostgreSQL) — a cross-driver import requires manual
    ///    review even though we always preserve the expression verbatim.
    /// 2. PostgreSQL does not support VIRTUAL generated columns; a VIRTUAL column from
    ///    MySQL or SQLite is silently coerced to STORED and a warning is emitted.
    fn generate_generated_column_warnings(
        &self,
        table_name: &str,
        col: &ColumnDefinition,
        source_driver: Option<&str>,
    ) -> Vec<ImportWarning> {
        let Some(ref _expr) = col.generation_expression else {
            return vec![];
        };

        let target_driver = self.connection.driver_name();
        let mut warnings = Vec::new();

        // When the source and target drivers differ the expression may use
        // driver-specific functions that do not translate.  Always warn so the
        // user can audit the expression rather than discovering a silent failure.
        if let Some(src) = source_driver {
            if src != target_driver {
                warnings.push(ImportWarning {
                    table: Some(table_name.to_owned()),
                    column: Some(col.name.clone()),
                    message: format!(
                        "Generated column '{}' in table '{}': expression was written for {} and \
                         may use driver-specific functions — verify the expression is valid on {}",
                        col.name, table_name, src, target_driver
                    ),
                    kind: ImportWarningKind::GeneratedColumnDegraded,
                });
            }
        }

        // PostgreSQL only supports STORED generated columns.  A VIRTUAL column
        // from another driver is coerced to STORED in the emitted DDL.
        if target_driver == "postgresql" && !col.is_generated_stored {
            warnings.push(ImportWarning {
                table: Some(table_name.to_owned()),
                column: Some(col.name.clone()),
                message: format!(
                    "Generated column '{}' in table '{}' was VIRTUAL in the source but PostgreSQL \
                     only supports STORED generated columns; created as STORED instead",
                    col.name, table_name
                ),
                kind: ImportWarningKind::GeneratedColumnDegraded,
            });
        }

        warnings
    }

    /// Builds the complete set of enum type definitions that should be created on the target.
    ///
    /// For PostgreSQL-sourced documents this is just `doc.schema.enums`.  For MySQL-sourced
    /// documents (where enum values are anonymous — `name: None`) we synthesize a named type
    /// for each enum column using the pattern `<table>_<column>_enum` so that PostgreSQL
    /// targets receive a valid named type and the column DDL can reference it by name.
    ///
    /// Returns a map of `type_name → EnumDefinition` and a secondary map of
    /// `(table_name, column_name) → synthesized_type_name` for use when rendering column DDL.
    fn resolve_enum_types(
        &self,
        doc: &UdifDocument,
    ) -> (
        HashMap<String, EnumDefinition>,
        HashMap<(String, String), String>,
    ) {
        let mut enum_map: HashMap<String, EnumDefinition> = doc.schema.enums.clone();
        let mut synthesized: HashMap<(String, String), String> = HashMap::new();

        let target_driver = self.connection.driver_name();

        // Anonymous enums (MySQL source) need a synthetic name on PostgreSQL targets.
        // On other targets they are handled inline by the type mapper so no synthesis needed.
        if target_driver != "postgresql" && target_driver != "postgres" {
            return (enum_map, synthesized);
        }

        for (table_name, table_def) in &doc.schema.tables {
            for col in &table_def.columns {
                if let CanonicalType::Enum {
                    name: None,
                    ref values,
                } = col.canonical_type
                {
                    let type_name = format!(
                        "{}_{}",
                        table_name.to_ascii_lowercase().replace(' ', "_"),
                        col.name.to_ascii_lowercase().replace(' ', "_")
                    );
                    synthesized
                        .insert((table_name.clone(), col.name.clone()), type_name.clone());
                    // Prefer the first definition if the same name somehow appears twice
                    // (e.g. two tables with identical column names).
                    enum_map.entry(type_name.clone()).or_insert(EnumDefinition {
                        name: type_name,
                        schema: None,
                        values: values.clone(),
                    });
                }
            }
        }

        (enum_map, synthesized)
    }

    /// Returns degradation warnings for enum columns that cannot be faithfully
    /// represented on the target driver.
    fn generate_enum_column_warnings(
        &self,
        table_name: &str,
        col: &ColumnDefinition,
    ) -> Vec<ImportWarning> {
        let CanonicalType::Enum { ref name, .. } = col.canonical_type else {
            return vec![];
        };

        let driver = self.connection.driver_name();
        let source_has_name = name.is_some();

        match driver {
            "sqlite" => vec![ImportWarning {
                table: Some(table_name.to_owned()),
                column: Some(col.name.clone()),
                message: format!(
                    "Column '{}' in table '{}': named enum type converted to TEXT with CHECK \
                     constraint on SQLite (no native enum type support)",
                    col.name, table_name
                ),
                kind: ImportWarningKind::TypeConversion,
            }],
            "mysql" if source_has_name => vec![ImportWarning {
                table: Some(table_name.to_owned()),
                column: Some(col.name.clone()),
                message: format!(
                    "Column '{}' in table '{}': named PostgreSQL enum type converted to inline \
                     MySQL ENUM column (named type dropped)",
                    col.name, table_name
                ),
                kind: ImportWarningKind::TypeConversion,
            }],
            _ => vec![],
        }
    }

    /// Generates a `CREATE TYPE <name> AS ENUM (...)` statement for a PostgreSQL target.
    fn generate_create_enum_type_sql(&self, enum_def: &EnumDefinition) -> String {
        let values_sql: Vec<String> = enum_def
            .values
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect();
        format!(
            "CREATE TYPE {} AS ENUM ({})",
            self.quote_identifier(&enum_def.name),
            values_sql.join(", ")
        )
    }

    /// Generates column SQL with an optional override of the canonical enum type name.
    ///
    /// When the target is PostgreSQL and an anonymous enum (MySQL source) was assigned a
    /// synthetic type name via `resolve_enum_types`, the override is passed here so the
    /// column DDL references the correct named type rather than falling back to an
    /// inline CHECK constraint.
    fn generate_column_sql_with_enum_override(
        &self,
        col: &ColumnDefinition,
        synthesized_type_name: Option<&str>,
    ) -> String {
        // Build a temporary column definition with the overridden enum name so that
        // the standard `generate_column_sql` path emits the right type string.
        if let Some(type_name) = synthesized_type_name {
            if let CanonicalType::Enum { ref values, .. } = col.canonical_type {
                let mut patched = col.clone();
                patched.canonical_type = CanonicalType::Enum {
                    name: Some(type_name.to_owned()),
                    values: values.clone(),
                };
                return self.generate_column_sql(&patched);
            }
        }
        self.generate_column_sql(col)
    }

    fn generate_primary_key_sql(&self, pk: &PrimaryKeyConstraint) -> String {        let cols: Vec<String> = pk
            .columns
            .iter()
            .map(|c| self.quote_identifier(c))
            .collect();
        format!("  PRIMARY KEY ({})", cols.join(", "))
    }

    /// Returns the outcome of attempting to map an index to the target driver.
    ///
    /// Three outcomes are possible:
    /// - `Ok(sql)`: the index can be created faithfully
    /// - `Err(IndexMappingOutcome::Degraded { sql, messages })`: the index is created
    ///   but with a degraded method or missing clause; the caller must emit warnings
    /// - `Err(IndexMappingOutcome::Skipped(message))`: the index has no equivalent on
    ///   the target driver and must be dropped entirely
    fn map_index_to_sql(
        &self,
        table_name: &str,
        index: &IndexDefinition,
    ) -> Result<String, IndexMappingOutcome> {
        let driver = self.connection.driver_name();
        let unique = if index.unique { "UNIQUE " } else { "" };

        // Determine the effective access method and whether a degradation warning is needed.
        let (using_clause, method_warning) = match &index.index_method {
            // BTREE is the universal default; omit USING clause for maximum portability.
            Some(IndexMethod::Btree) | None => (String::new(), None),

            Some(IndexMethod::Hash) => match driver {
                "postgresql" => (" USING HASH".to_string(), None),
                // MySQL only supports HASH for MEMORY-engine tables; fall back to BTREE.
                "mysql" => (
                    String::new(),
                    Some(format!(
                        "Hash index '{}' on '{}' is only valid for MEMORY-engine tables on \
                         MySQL; created as a BTREE index instead",
                        index.name, table_name
                    )),
                ),
                // SQLite has no HASH index support.
                _ => (
                    String::new(),
                    Some(format!(
                        "Hash index '{}' on '{}' has no equivalent on SQLite; \
                         created as a BTREE index instead",
                        index.name, table_name
                    )),
                ),
            },

            Some(IndexMethod::Gin) => match driver {
                "postgresql" => (" USING GIN".to_string(), None),
                "mysql" => {
                    return Err(IndexMappingOutcome::Skipped(format!(
                        "GIN index '{}' on '{}' has no equivalent in MySQL — index dropped",
                        index.name, table_name
                    )));
                }
                _ => {
                    return Err(IndexMappingOutcome::Skipped(format!(
                        "GIN index '{}' on '{}' has no equivalent on this driver — index dropped",
                        index.name, table_name
                    )));
                }
            },

            Some(IndexMethod::Gist) => match driver {
                "postgresql" => (" USING GIST".to_string(), None),
                "mysql" => {
                    return Err(IndexMappingOutcome::Skipped(format!(
                        "GIST index '{}' on '{}' has no equivalent in MySQL — index dropped",
                        index.name, table_name
                    )));
                }
                _ => {
                    return Err(IndexMappingOutcome::Skipped(format!(
                        "GIST index '{}' on '{}' has no equivalent on this driver — index dropped",
                        index.name, table_name
                    )));
                }
            },

            Some(IndexMethod::SpGist) => match driver {
                "postgresql" => (" USING SPGIST".to_string(), None),
                "mysql" => {
                    return Err(IndexMappingOutcome::Skipped(format!(
                        "SP-GIST index '{}' on '{}' has no equivalent in MySQL — index dropped",
                        index.name, table_name
                    )));
                }
                _ => {
                    return Err(IndexMappingOutcome::Skipped(format!(
                        "SP-GIST index '{}' on '{}' has no equivalent on this driver — index dropped",
                        index.name, table_name
                    )));
                }
            },

            Some(IndexMethod::Brin) => match driver {
                "postgresql" => (" USING BRIN".to_string(), None),
                "mysql" => {
                    return Err(IndexMappingOutcome::Skipped(format!(
                        "BRIN index '{}' on '{}' has no equivalent in MySQL — index dropped",
                        index.name, table_name
                    )));
                }
                _ => {
                    return Err(IndexMappingOutcome::Skipped(format!(
                        "BRIN index '{}' on '{}' has no equivalent on this driver — index dropped",
                        index.name, table_name
                    )));
                }
            },

            Some(IndexMethod::Fulltext) => match driver {
                // MySQL FULLTEXT indexes use a keyword in place of USING.
                "mysql" => (" FULLTEXT".to_string(), None),
                "postgresql" => (
                    String::new(),
                    Some(format!(
                        "FULLTEXT index '{}' on '{}': PostgreSQL uses tsvector/GIN for \
                         full-text search — a regular BTREE index has been created as a \
                         partial substitute; manually add a GIN index on a tsvector column",
                        index.name, table_name
                    )),
                ),
                _ => {
                    return Err(IndexMappingOutcome::Skipped(format!(
                        "FULLTEXT index '{}' on '{}' has no equivalent on this driver — index dropped",
                        index.name, table_name
                    )));
                }
            },

            Some(IndexMethod::Spatial) => match driver {
                "mysql" => (" SPATIAL".to_string(), None),
                // PostgreSQL uses GIST for spatial data.
                "postgresql" => (
                    " USING GIST".to_string(),
                    Some(format!(
                        "SPATIAL index '{}' on '{}' has been created as a GIST index on \
                         PostgreSQL; verify that the column uses a geometry type (PostGIS)",
                        index.name, table_name
                    )),
                ),
                _ => {
                    return Err(IndexMappingOutcome::Skipped(format!(
                        "SPATIAL index '{}' on '{}' has no equivalent on this driver — index dropped",
                        index.name, table_name
                    )));
                }
            },
        };

        // Build the per-column list with optional ASC/DESC.
        // NULLS FIRST/LAST is only supported by PostgreSQL and SQLite; MySQL has no
        // such syntax so the ordering hint is silently omitted there.
        let cols: Vec<String> = index
            .columns
            .iter()
            .map(|c| {
                use crate::document::{NullsOrder, SortOrder};
                let mut col = self.quote_identifier(&c.column);
                match c.order {
                    SortOrder::Asc => {} // default; omit for brevity
                    SortOrder::Desc => col.push_str(" DESC"),
                }
                match c.nulls {
                    NullsOrder::Default => {}
                    NullsOrder::First if matches!(driver, "postgresql" | "sqlite") => {
                        col.push_str(" NULLS FIRST")
                    }
                    NullsOrder::Last if matches!(driver, "postgresql" | "sqlite") => {
                        col.push_str(" NULLS LAST")
                    }
                    // MySQL: no NULLS ordering syntax — silently omit.
                    NullsOrder::First | NullsOrder::Last => {}
                }
                col
            })
            .collect();

        // Partial index WHERE clause: PostgreSQL and SQLite support it.
        // On MySQL we omit it with a degradation warning so the index covers all rows.
        let (where_fragment, where_warning) = match &index.where_clause {
            Some(clause) => match driver {
                "postgresql" | "sqlite" => (format!(" WHERE {}", clause), None),
                _ => (
                    String::new(),
                    Some(format!(
                        "Partial index '{}' on '{}' (WHERE {}) is not supported on {}; \
                         created as a full-table index covering all rows instead",
                        index.name, table_name, clause, driver
                    )),
                ),
            },
            None => (String::new(), None),
        };

        // INCLUDE (covering) columns are a PostgreSQL 11+ feature.
        // On other drivers we omit them (they are a performance hint, not a correctness
        // requirement) and emit a degradation warning.
        let (include_fragment, include_warning) = if index.include_columns.is_empty() {
            (String::new(), None)
        } else {
            match driver {
                "postgresql" => {
                    let quoted: Vec<String> = index
                        .include_columns
                        .iter()
                        .map(|c| self.quote_identifier(c))
                        .collect();
                    (format!(" INCLUDE ({})", quoted.join(", ")), None)
                }
                _ => (
                    String::new(),
                    Some(format!(
                        "Covering index '{}' on '{}' has INCLUDE columns {:?} which are not \
                         supported on {}; index created without INCLUDE columns",
                        index.name, table_name, index.include_columns, driver
                    )),
                ),
            }
        };

        // FULLTEXT on MySQL does not accept the UNIQUE keyword — guard defensively.
        let unique_keyword =
            if index.index_method == Some(IndexMethod::Fulltext) && driver == "mysql" {
                ""
            } else {
                unique
            };

        let sql = format!(
            "CREATE {}INDEX{} {} ON {} ({}){}{}",
            unique_keyword,
            using_clause,
            self.quote_identifier(&index.name),
            self.quote_identifier(table_name),
            cols.join(", "),
            include_fragment,
            where_fragment,
        );

        // Collect all degradation messages.  If any exist, wrap the SQL in a
        // DegradedIndex outcome so the caller can emit per-warning ImportWarnings.
        let messages: Vec<String> = [method_warning, where_warning, include_warning]
            .into_iter()
            .flatten()
            .collect();

        if messages.is_empty() {
            Ok(sql)
        } else {
            Err(IndexMappingOutcome::Degraded { sql, messages })
        }
    }

    /// Convenience wrapper used by unit tests: returns the SQL string regardless
    /// of whether the mapping is faithful or degraded, and returns an empty string
    /// for skipped indexes.
    #[cfg(test)]
    fn generate_index_sql(&self, table_name: &str, index: &IndexDefinition) -> String {
        match self.map_index_to_sql(table_name, index) {
            Ok(sql) | Err(IndexMappingOutcome::Degraded { sql, .. }) => sql,
            Err(IndexMappingOutcome::Skipped(_)) => String::new(),
        }
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

    /// Returns the maximum number of bound parameters a single statement may
    /// carry for the current driver.  Staying within this limit prevents
    /// "too many SQL variables" errors from the underlying driver.
    fn max_params_per_statement(&self) -> usize {
        match self.connection.driver_name() {
            // SQLite's compile-time default is 32766 (SQLITE_MAX_VARIABLE_NUMBER)
            "sqlite" => 32_766,
            // PostgreSQL and MySQL both support up to 65535 bind parameters
            _ => 65_535,
        }
    }

    /// Calculates how many rows can fit in a single batch INSERT without
    /// exceeding the driver's bound-parameter limit.  When `cols_per_row` is
    /// 0 the requested `batch_size` is returned as-is to avoid division by
    /// zero.
    fn effective_batch_size(&self, batch_size: usize, cols_per_row: usize) -> usize {
        if cols_per_row == 0 {
            return batch_size;
        }
        let max_rows = self.max_params_per_statement() / cols_per_row;
        batch_size.min(max_rows).max(1)
    }

    /// Builds a multi-row `INSERT INTO table (cols) VALUES (…), (…)` statement
    /// for the given slice of pre-decoded row values.  PostgreSQL uses numbered
    /// placeholders (`$1`, `$2`, …) counted globally across all rows; other
    /// drivers use positional `?` placeholders.
    fn build_batch_insert_sql(
        &self,
        table_name: &str,
        col_names: &[String],
        row_count: usize,
    ) -> String {
        let driver = self.connection.driver_name();
        let cols_per_row = col_names.len();

        let value_rows: Vec<String> = (0..row_count)
            .map(|row_idx| {
                let placeholders: Vec<String> = (0..cols_per_row)
                    .map(|col_idx| {
                        if driver == "postgresql" {
                            // Globally-numbered to match the flat params slice
                            format!("${}", row_idx * cols_per_row + col_idx + 1)
                        } else {
                            "?".to_string()
                        }
                    })
                    .collect();
                format!("({})", placeholders.join(", "))
            })
            .collect();

        format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.quote_identifier(table_name),
            col_names.join(", "),
            value_rows.join(", ")
        )
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
            Value::Bytes(_) => {
                tracing::warn!(
                    "value_to_sql: binary (Bytes) value cannot be represented as a SQL literal \
                     and will be inserted as NULL"
                );
                "NULL".to_string()
            }
            Value::Uuid(u) => format!("'{}'", u),
            Value::Date(d) => format!("'{}'", d),
            Value::Time(t) => format!("'{}'", t),
            Value::DateTime(dt) => format!("'{}'", dt),
            Value::DateTimeUtc(dt) => format!("'{}'", dt),
            Value::Json(j) => format!("'{}'", j.to_string().replace('\'', "''")),
            Value::Array(_) => {
                tracing::warn!(
                    "value_to_sql: Array value cannot be represented as a scalar SQL literal \
                     and will be inserted as NULL"
                );
                "NULL".to_string()
            }
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

    /// Empties a table using the most efficient DDL for the target driver.
    ///
    /// PostgreSQL and MySQL support `TRUNCATE TABLE`, which is a metadata-only operation
    /// that is orders of magnitude faster than `DELETE FROM` on large tables and resets
    /// auto-increment sequences on MySQL.  SQLite has no `TRUNCATE` statement, so we fall
    /// back to `DELETE FROM` followed by a `sqlite_sequence` row deletion to reset the
    /// AUTOINCREMENT counter — otherwise the counter keeps climbing after re-import.
    async fn truncate_table(
        &self,
        table_name: &str,
        table_def: &TableDefinition,
    ) -> Result<(), ImportError> {
        let driver = self.connection.driver_name();
        let quoted = self.quote_identifier(table_name);

        match driver {
            "sqlite" => {
                // SQLite does not support TRUNCATE; DELETE FROM achieves the same
                // logical effect but leaves the sqlite_sequence counter intact.
                self.connection
                    .execute(&format!("DELETE FROM {}", quoted), &[])
                    .await
                    .map_err(|e| ImportError::QueryError(e.to_string()))?;

                // When the table has any AUTOINCREMENT column, resetting
                // sqlite_sequence ensures the next INSERT starts from 1 rather
                // than continuing from the previous high-water mark.
                let has_autoincrement = table_def
                    .columns
                    .iter()
                    .any(|c| c.auto_increment || matches!(
                        c.canonical_type,
                        crate::CanonicalType::Serial
                            | crate::CanonicalType::SmallSerial
                            | crate::CanonicalType::BigSerial
                    ));

                if has_autoincrement {
                    // sqlite_sequence may not exist if no AUTOINCREMENT table has
                    // ever had a row inserted; ignore the error in that case.
                    let reset_sql = format!(
                        "DELETE FROM sqlite_sequence WHERE name = '{}'",
                        table_name.replace('\'', "''")
                    );
                    if let Err(e) = self.connection.execute(&reset_sql, &[]).await {
                        tracing::warn!(
                            table = table_name,
                            error = %e,
                            "could not reset sqlite_sequence for table — sequence counter \
                             may not start from 1 after import"
                        );
                    }
                }
            }
            // PostgreSQL and MySQL both support TRUNCATE TABLE which is faster
            // than DELETE FROM and resets MySQL AUTO_INCREMENT counters.
            _ => {
                self.connection
                    .execute(&format!("TRUNCATE TABLE {}", quoted), &[])
                    .await
                    .map_err(|e| ImportError::QueryError(e.to_string()))?;
            }
        }

        Ok(())
    }

    /// Returns the table names from the document in a safe creation order using
    /// Kahn's BFS topological sort on the FK dependency graph.
    ///
    /// Tables with no FK dependencies come first.  When a cycle is detected
    /// (mutual or self-referencing FKs) the cyclic tables are appended at the
    /// end in their original document order so CREATE TABLE can still proceed —
    /// their FKs are all deferred to the ALTER TABLE ADD CONSTRAINT phase anyway.
    ///
    /// Tables that reference a table *not present in the document* (external
    /// references) are treated as having no dependency on that table.
    fn topological_sort_tables<'a>(
        &self,
        tables: &[(&'a String, &'a TableDefinition)],
        options: &ImportOptions,
    ) -> Vec<(&'a String, &'a TableDefinition)> {
        let table_names: HashSet<&str> =
            tables.iter().map(|(name, _)| name.as_str()).collect();

        // Build adjacency list: node A depends on node B (B must come first).
        // Only include edges where the referenced table is in our import set.
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new(); // B → [A…]
        let mut in_degree: HashMap<&str, usize> = HashMap::new();

        for (name, _) in tables {
            in_degree.entry(name.as_str()).or_insert(0);
            dependents.entry(name.as_str()).or_default();
        }

        for (name, table_def) in tables {
            for fk in &table_def.foreign_keys {
                let referenced = fk.referenced_table.as_str();
                // Map the referenced table through table_mappings so the graph
                // uses the same names that will actually appear in the target DB.
                let mapped_referenced = options
                    .table_mappings
                    .get(referenced)
                    .map(|s| s.as_str())
                    .unwrap_or(referenced);

                // Self-references don't create a useful ordering dependency and
                // would trivially form a cycle; skip them in the graph.
                if mapped_referenced == name.as_str() {
                    continue;
                }

                if table_names.contains(mapped_referenced) {
                    dependents
                        .entry(mapped_referenced)
                        .or_default()
                        .push(name.as_str());
                    *in_degree.entry(name.as_str()).or_insert(0) += 1;
                }
            }
        }

        // Kahn's algorithm: start with every node whose in-degree is zero.
        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|&(_, &deg)| deg == 0)
            .map(|(&name, _)| name)
            .collect();
        // Stable ordering within each level: sort by name so the output is
        // deterministic regardless of HashMap iteration order.
        let mut sorted_names: Vec<&str> = Vec::with_capacity(tables.len());

        while let Some(current) = queue.pop_front() {
            sorted_names.push(current);
            if let Some(downstream) = dependents.get(current) {
                let mut next_batch: Vec<&str> = downstream
                    .iter()
                    .filter_map(|&dep| {
                        let degree = in_degree.get_mut(dep)?;
                        *degree -= 1;
                        if *degree == 0 { Some(dep) } else { None }
                    })
                    .collect();
                next_batch.sort_unstable();
                queue.extend(next_batch);
            }
        }

        // Any table not yet in sorted_names is part of a cycle.  Append them
        // in original document order so the output length always equals the
        // input length.
        let sorted_set: HashSet<&str> = sorted_names.iter().copied().collect();
        for (name, _) in tables {
            if !sorted_set.contains(name.as_str()) {
                sorted_names.push(name.as_str());
            }
        }

        // Rebuild the output vec in the computed order.
        let lookup: HashMap<&str, (&String, &TableDefinition)> =
            tables.iter().map(|(name, def)| (name.as_str(), (*name, *def))).collect();

        sorted_names
            .iter()
            .filter_map(|name| lookup.get(name).copied())
            .collect()
    }

    /// Advance the target database's auto-increment counters to match the
    /// values captured at export time, so that the first new insert after
    /// the import does not collide with any of the imported row IDs.
    ///
    /// Each driver exposes its counter through a different mechanism:
    /// - PostgreSQL: `setval(<seq_name>, <value>, true)` — the third argument
    ///   `is_called=true` tells PostgreSQL that `<value>` has already been used.
    /// - MySQL: `ALTER TABLE <t> AUTO_INCREMENT = <next>` takes the *next* value,
    ///   so we pass `current_value + 1`.
    /// - SQLite: upsert into `sqlite_sequence` (the table is created lazily by
    ///   the engine so we INSERT OR REPLACE unconditionally).
    async fn restore_sequences(
        &self,
        doc: &UdifDocument,
        options: &ImportOptions,
        result: &mut ImportResult,
    ) -> Result<(), ImportError> {
        let driver = self.connection.driver_name();

        for seq in doc.schema.sequences.values() {
            let current_value = match seq.current_value {
                Some(v) => v,
                // No rows were ever inserted in the source; skip so we do not
                // reset a counter that may already be ahead on the target.
                None => continue,
            };

            let sql_opt: Option<String> = match driver {
                "postgresql" | "postgres" => {
                    // seq.name for PostgreSQL is the actual sequence object name.
                    Some(format!(
                        "SELECT setval('{}', {}, true)",
                        seq.name.replace('\'', "''"),
                        current_value
                    ))
                }

                "mysql" => {
                    // For MySQL the key is "<table>.<column>"; extract the table
                    // so we can issue ALTER TABLE.
                    let table_name = match seq.name.split_once('.') {
                        Some((table, _col)) => table,
                        // Unexpected format — skip rather than corrupt the schema.
                        None => continue,
                    };
                    let target_table = self.get_target_table_name(table_name, options);
                    if !self.should_include_table(table_name, options) {
                        continue;
                    }
                    Some(format!(
                        "ALTER TABLE {} AUTO_INCREMENT = {}",
                        self.quote_identifier(&target_table),
                        // MySQL's AUTO_INCREMENT is the *next* value to assign.
                        current_value + 1
                    ))
                }

                "sqlite" => {
                    let table_name = match seq.name.split_once('.') {
                        Some((table, _col)) => table,
                        None => continue,
                    };
                    let target_table = self.get_target_table_name(table_name, options);
                    if !self.should_include_table(table_name, options) {
                        continue;
                    }
                    // sqlite_sequence may not exist if no AUTOINCREMENT table has
                    // had a row inserted yet in this session, but the row for our
                    // table definitely does not exist — INSERT OR REPLACE is safe.
                    Some(format!(
                        "INSERT OR REPLACE INTO sqlite_sequence (name, seq) VALUES ('{}', {})",
                        target_table.replace('\'', "''"),
                        current_value
                    ))
                }

                // Unknown driver: nothing to do.
                _ => None,
            };

            if let Some(sql) = sql_opt {
                match self.connection.execute(&sql, &[]).await {
                    Ok(_) => {}
                    Err(e) => {
                        if options.continue_on_error {
                            result.errors.push(format!(
                                "Failed to restore sequence '{}': {}",
                                seq.name, e
                            ));
                        } else {
                            return Err(ImportError::QueryError(format!(
                                "Failed to restore sequence '{}': {}",
                                seq.name, e
                            )));
                        }
                    }
                }
            }
        }

        Ok(())
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
                let source_feature = format!(
                    "Column '{}': source type {:?}",
                    warning.column, warning.source_type
                );
                let target_action = format!("mapped to {}", warning.target_type);
                result.push_warning(
                    ImportWarning {
                        table: Some(warning.table),
                        column: Some(warning.column),
                        message: warning.message,
                        kind: ImportWarningKind::TypeConversion,
                    },
                    DegradationCategory::TypeConversion,
                    None,
                    source_feature,
                    target_action,
                    DegradationSeverity::Warning,
                );
            }
        }

        let tables: Vec<_> = doc
            .schema
            .tables
            .iter()
            .filter(|(name, _)| self.should_include_table(name, options))
            .collect();

        // Sort tables so referenced tables are created before their dependants.
        // Cycles are detected and those tables are appended at the end — their
        // FKs are always applied via ALTER TABLE after data load, so ordering
        // only matters for non-FK constraints within CREATE TABLE.
        let tables = self.topological_sort_tables(&tables, options);

        let total_tables = tables.len();

        // Resolve the full set of enum types — combines named types from the document
        // (PostgreSQL source) with synthesized names for anonymous enums (MySQL source)
        // when the target is PostgreSQL.
        let (resolved_enums, synthesized_enum_columns) = self.resolve_enum_types(doc);

        if options.create_tables && !resolved_enums.is_empty() {
            let driver = self.connection.driver_name();
            // Only PostgreSQL uses schema-level CREATE TYPE.  MySQL handles enums inline
            // in column DDL; SQLite maps them to TEXT + CHECK constraint.
            if matches!(driver, "postgresql" | "postgres") {
                progress(ImportProgress {
                    phase: ImportPhase::CreatingEnumTypes,
                    current_table: None,
                    total_tables,
                    tables_completed: 0,
                    rows_imported: 0,
                    total_rows: None,
                    current_error: None,
                });

                for enum_def in resolved_enums.values() {
                    let sql = self.generate_create_enum_type_sql(enum_def);
                    match self.connection.execute(&sql, &[]).await {
                        Ok(_) => {}
                        Err(e) => {
                            if options.continue_on_error {
                                result.errors.push(format!(
                                    "Failed to create enum type '{}': {}",
                                    enum_def.name, e
                                ));
                            } else {
                                return Err(ImportError::QueryError(format!(
                                    "Failed to create enum type '{}': {}",
                                    enum_def.name, e
                                )));
                            }
                        }
                    }
                }
            }
        }

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
                            self.truncate_table(&target_name, table_def).await?;
                        }
                        IfTableExists::Append => {}
                    }
                }

                if !exists || options.if_exists == IfTableExists::Replace {
                    let create_sql = self.generate_create_table_sql_with_enums(
                        table_def,
                        &synthesized_enum_columns,
                    );
                    self.connection
                        .execute(&create_sql, &[])
                        .await
                        .map_err(|e| ImportError::QueryError(e.to_string()))?;
                    result.tables_created += 1;

                    // Emit enforcement warnings for CHECK constraints on drivers that
                    // may not enforce them.  MySQL < 8.0.16 parses CHECK syntax but
                    // silently ignores it; we always warn for MySQL since we cannot
                    // query the server version through the Connection trait.  SQLite
                    // enforces CHECK from 3.25.2+ but we cannot verify the version
                    // either, so we also warn for SQLite to prompt the user to verify.
                    let driver = self.connection.driver_name();
                    for check in &table_def.check_constraints {
                        let message = match driver {
                            "mysql" => format!(
                                "CHECK constraint '{}' on table '{}' is parsed but NOT enforced \
                                 on MySQL < 8.0.16; verify your MySQL version supports enforcement",
                                check
                                    .name
                                    .as_deref()
                                    .unwrap_or("<unnamed>"),
                                table_name
                            ),
                            "sqlite" => format!(
                                "CHECK constraint '{}' on table '{}' requires SQLite >= 3.25.2 \
                                 for enforcement; verify your SQLite version",
                                check
                                    .name
                                    .as_deref()
                                    .unwrap_or("<unnamed>"),
                                table_name
                            ),
                            _ => continue,
                        };
                        result.push_warning(
                            ImportWarning {
                                table: Some(table_name.to_string()),
                                column: None,
                                message,
                                kind: ImportWarningKind::CheckConstraintNonEnforced,
                            },
                            DegradationCategory::CheckConstraint,
                            check.name.clone(),
                            "CHECK constraint",
                            format!("not enforced on {}", driver),
                            DegradationSeverity::Warning,
                        );
                    }

                    // Emit per-column default-value degradation warnings for semantic
                    // variants that cannot be fully expressed on the target driver.
                    for col in &table_def.columns {
                        let column_warnings =
                            self.generate_column_default_warnings(table_name, col);
                        for w in column_warnings {
                            let object = w.column.clone();
                            let source = w.message.clone();
                            result.push_warning(
                                w,
                                DegradationCategory::DefaultValue,
                                object,
                                source,
                                "default value modified or removed",
                                DegradationSeverity::Warning,
                            );
                        }
                    }

                    // Emit per-column warnings for generated (computed) columns whose
                    // expression may not be portable across drivers, or whose storage
                    // mode (VIRTUAL vs STORED) was coerced.
                    let source_driver = doc.source.driver.as_str();
                    for col in &table_def.columns {
                        let generated_warnings = self.generate_generated_column_warnings(
                            table_name,
                            col,
                            Some(source_driver),
                        );
                        for w in generated_warnings {
                            let object = w.column.clone();
                            let source = w.message.clone();
                            result.push_warning(
                                w,
                                DegradationCategory::GeneratedColumn,
                                object,
                                source,
                                "expression may need manual review",
                                DegradationSeverity::Warning,
                            );
                        }
                    }

                    // Emit per-column enum degradation warnings when the target driver
                    // cannot natively preserve a named or inline enum type.
                    for col in &table_def.columns {
                        let enum_warnings =
                            self.generate_enum_column_warnings(table_name, col);
                        for w in enum_warnings {
                            let object = w.column.clone();
                            let source = w.message.clone();
                            result.push_warning(
                                w,
                                DegradationCategory::Enum,
                                object,
                                source,
                                "enum type converted or synthesized",
                                DegradationSeverity::Warning,
                            );
                        }
                    }
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

                    // Pre-compute which column indices are insertable once per table
                    // rather than once per row.  Columns whose value is automatically
                    // assigned by the database (serial sequences, AUTO_INCREMENT) must
                    // be excluded so the engine can assign them; explicitly supplying a
                    // value causes duplicate-key errors once the sequence catches up.
                    let insertable_indices: Vec<usize> = table_def
                        .columns
                        .iter()
                        .enumerate()
                        .filter(|(_, col)| !col.is_db_generated())
                        .map(|(idx, _)| idx)
                        .collect();

                    let insertable_cols: Vec<String> = insertable_indices
                        .iter()
                        .map(|&idx| self.quote_identifier(&table_def.columns[idx].name))
                        .collect();

                    // Reduce batch size when necessary to stay within the
                    // driver's bound-parameter limit.  A table with many
                    // columns would otherwise produce statements that exceed
                    // the limit at the requested batch_size.
                    let safe_batch = self.effective_batch_size(
                        options.batch_size as usize,
                        insertable_cols.len(),
                    );

                    for chunk in table_data.rows.chunks(safe_batch) {
                        // Decode all rows in the chunk and collect the flat
                        // parameter slice used by the multi-row INSERT.
                        let mut batch_values: Vec<Value> =
                            Vec::with_capacity(chunk.len() * insertable_cols.len());
                        let mut chunk_rows_decoded = 0usize;

                        for row in chunk {
                            let all_values = self.decode_row(&row.values)?;
                            for &col_idx in &insertable_indices {
                                batch_values.push(
                                    all_values
                                        .get(col_idx)
                                        .cloned()
                                        .unwrap_or(Value::Null),
                                );
                            }
                            chunk_rows_decoded += 1;
                        }

                        let sql = self.build_batch_insert_sql(
                            &target_name,
                            &insertable_cols,
                            chunk_rows_decoded,
                        );

                        let insert_result = if options.use_transaction {
                            let tx = self
                                .connection
                                .begin_transaction()
                                .await
                                .map_err(|e| ImportError::QueryError(e.to_string()))?;

                            match tx.execute(&sql, &batch_values).await {
                                Ok(_) => {
                                    tx.commit()
                                        .await
                                        .map_err(|e| ImportError::QueryError(e.to_string()))?;
                                    Ok(chunk_rows_decoded as u64)
                                }
                                Err(e) => {
                                    // Best-effort rollback.  If the rollback itself
                                    // fails there is nothing more we can do; the
                                    // connection error will be surfaced via the
                                    // original INSERT error below, which is the one
                                    // callers care about.
                                    if let Err(rollback_err) = tx.rollback().await {
                                        tracing::warn!(
                                            "rollback failed after batch INSERT error: {}",
                                            rollback_err
                                        );
                                    }
                                    Err(ImportError::QueryError(e.to_string()))
                                }
                            }
                        } else {
                            self.connection
                                .execute(&sql, &batch_values)
                                .await
                                .map(|r| r.affected_rows)
                                .map_err(|e| ImportError::QueryError(e.to_string()))
                        };

                        match insert_result {
                            Ok(affected) => {
                                rows_imported += affected;
                            }
                            Err(e) => {
                                if options.continue_on_error {
                                    result.errors.push(e.to_string());
                                } else {
                                    return Err(e);
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
                    match self.map_index_to_sql(&target_name, index) {
                        Ok(sql) => {
                            match self.connection.execute(&sql, &[]).await {
                                Ok(_) => {
                                    result.indexes_created += 1;
                                }
                                Err(e) => {
                                    if options.continue_on_error {
                                        result.push_warning(
                                            ImportWarning {
                                                table: Some(table_name.to_string()),
                                                column: None,
                                                message: format!("Failed to create index: {}", e),
                                                kind: ImportWarningKind::IndexSkipped,
                                            },
                                            DegradationCategory::Index,
                                            Some(index.name.clone()),
                                            "index",
                                            "dropped due to creation failure",
                                            DegradationSeverity::Dropped,
                                        );
                                    } else {
                                        return Err(ImportError::QueryError(e.to_string()));
                                    }
                                }
                            }
                        }
                        Err(IndexMappingOutcome::Degraded { sql, messages }) => {
                            // The index can still be created, but with reduced fidelity.
                            // Emit all degradation warnings before attempting the CREATE.
                            for message in messages {
                                result.push_warning(
                                    ImportWarning {
                                        table: Some(table_name.to_string()),
                                        column: None,
                                        message: message.clone(),
                                        kind: ImportWarningKind::IndexDegraded,
                                    },
                                    DegradationCategory::Index,
                                    Some(index.name.clone()),
                                    "index method or clause",
                                    message,
                                    DegradationSeverity::Warning,
                                );
                            }
                            match self.connection.execute(&sql, &[]).await {
                                Ok(_) => {
                                    result.indexes_created += 1;
                                }
                                Err(e) => {
                                    if options.continue_on_error {
                                        result.push_warning(
                                            ImportWarning {
                                                table: Some(table_name.to_string()),
                                                column: None,
                                                message: format!(
                                                    "Failed to create degraded index: {}",
                                                    e
                                                ),
                                                kind: ImportWarningKind::IndexSkipped,
                                            },
                                            DegradationCategory::Index,
                                            Some(index.name.clone()),
                                            "degraded index",
                                            "dropped due to creation failure",
                                            DegradationSeverity::Dropped,
                                        );
                                    } else {
                                        return Err(ImportError::QueryError(e.to_string()));
                                    }
                                }
                            }
                        }
                        Err(IndexMappingOutcome::Skipped(message)) => {
                            result.push_warning(
                                ImportWarning {
                                    table: Some(table_name.to_string()),
                                    column: None,
                                    message: message.clone(),
                                    kind: ImportWarningKind::IndexSkipped,
                                },
                                DegradationCategory::Index,
                                Some(index.name.clone()),
                                "index",
                                message,
                                DegradationSeverity::Dropped,
                            );
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

            // Build the set of table names that will actually exist in the
            // target so we can detect FKs that reference external tables.
            let imported_table_names: HashSet<String> = tables
                .iter()
                .map(|(name, _)| self.get_target_table_name(name, options))
                .collect();

            let driver = self.connection.driver_name();

            // On MySQL, FK checks during bulk load can cause ordering failures
            // when tables reference each other.  Disable them for the duration
            // of FK creation and re-enable afterwards.
            if driver == "mysql" {
                self.connection
                    .execute("SET FOREIGN_KEY_CHECKS=0", &[])
                    .await
                    .map_err(|e| ImportError::QueryError(e.to_string()))?;
            }

            // On SQLite, FK enforcement must be off while we ALTER TABLE because
            // the referenced tables may not all exist yet at the time each
            // constraint is applied.
            if driver == "sqlite" {
                self.connection
                    .execute("PRAGMA foreign_keys = OFF", &[])
                    .await
                    .map_err(|e| ImportError::QueryError(e.to_string()))?;
            }

            for (table_name, table_def) in &tables {
                let target_name = self.get_target_table_name(table_name, options);
                for fk in &table_def.foreign_keys {
                    // Map the referenced table through any table renaming so
                    // the ALTER TABLE statement uses the actual target name.
                    let referenced_target = self.get_target_table_name(&fk.referenced_table, options);

                    // If the referenced table is not part of this import (an
                    // external dependency), skip the FK and leave a warning
                    // rather than failing the whole import.
                    if !imported_table_names.contains(&referenced_target) {
                        result.push_warning(
                            ImportWarning {
                                table: Some(table_name.to_string()),
                                column: None,
                                message: format!(
                                    "Skipped FK on '{}' referencing '{}': referenced table is not \
                                     included in this import",
                                    table_name, fk.referenced_table
                                ),
                                kind: ImportWarningKind::ConstraintSkipped,
                            },
                            DegradationCategory::ForeignKey,
                            fk.name.clone(),
                            format!("FK → {}", fk.referenced_table),
                            "dropped: referenced table not in this import",
                            DegradationSeverity::Dropped,
                        );
                        continue;
                    }

                    let sql = self.generate_foreign_key_sql(&target_name, fk);
                    match self.connection.execute(&sql, &[]).await {
                        Ok(_) => {
                            result.foreign_keys_created += 1;
                        }
                        Err(e) => {
                            if options.continue_on_error {
                                result.push_warning(
                                    ImportWarning {
                                        table: Some(table_name.to_string()),
                                        column: None,
                                        message: format!("Failed to create foreign key: {}", e),
                                        kind: ImportWarningKind::ConstraintSkipped,
                                    },
                                    DegradationCategory::ForeignKey,
                                    fk.name.clone(),
                                    format!("FK → {}", fk.referenced_table),
                                    "dropped due to creation failure",
                                    DegradationSeverity::Dropped,
                                );
                            } else {
                                // Best-effort re-enable before returning so the
                                // connection is left in a clean state.
                                if driver == "mysql" {
                                    let _ = self
                                        .connection
                                        .execute("SET FOREIGN_KEY_CHECKS=1", &[])
                                        .await;
                                }
                                if driver == "sqlite" {
                                    let _ = self
                                        .connection
                                        .execute("PRAGMA foreign_keys = ON", &[])
                                        .await;
                                }
                                return Err(ImportError::QueryError(e.to_string()));
                            }
                        }
                    }
                }
            }

            // Re-enable FK checks now that all constraints have been applied.
            if driver == "mysql" {
                self.connection
                    .execute("SET FOREIGN_KEY_CHECKS=1", &[])
                    .await
                    .map_err(|e| ImportError::QueryError(e.to_string()))?;
            }
            if driver == "sqlite" {
                self.connection
                    .execute("PRAGMA foreign_keys = ON", &[])
                    .await
                    .map_err(|e| ImportError::QueryError(e.to_string()))?;
            }
        }

        if !doc.schema.sequences.is_empty() {
            progress(ImportProgress {
                phase: ImportPhase::RestoringSequences,
                current_table: None,
                total_tables,
                tables_completed: total_tables,
                rows_imported: result.total_rows(),
                total_rows: None,
                current_error: None,
            });

            self.restore_sequences(doc, options, &mut result).await?;
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

/// Removes PostgreSQL-style cast suffixes (`::typename`) from a SQL expression string.
///
/// PG defaults and CHECK expressions often contain casts like `'foo'::text` or
/// `(0)::numeric` that are syntactically invalid on SQLite and MySQL.  This helper
/// strips the trailing `::identifier` (or `::identifier[]`) tokens so the resulting
/// expression is at least parseable on all three drivers.  The result may still need
/// further inspection (e.g. if function calls remain) but the cast noise is gone.
fn strip_pg_casts(expr: &str) -> String {
    // Repeatedly remove trailing `::word` / `::word[]` segments.
    // We use a simple scan rather than a regex to keep the dependency footprint small.
    let mut result = expr.to_owned();
    loop {
        // Find the last occurrence of `::` that is followed by an identifier.
        let Some(cast_start) = result.rfind("::") else {
            break;
        };
        let after = &result[cast_start + 2..];
        // An identifier is [A-Za-z_][A-Za-z0-9_ ]* optionally followed by `[]`.
        let ident_len = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ' ')
            .map(char::len_utf8)
            .sum::<usize>();
        if ident_len == 0 {
            break;
        }
        let mut end = cast_start + 2 + ident_len;
        // Also consume a trailing `[]` if present.
        if result[end..].starts_with("[]") {
            end += 2;
        }
        result.replace_range(cast_start..end, "");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{ColumnDefinition, IndexColumn, NullsOrder, SortOrder};
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use zqlz_core::{QueryResult, Result, StatementResult, Transaction, ZqlzError};

    /// Minimal mock connection whose driver name is configurable.
    struct MockConnection {
        driver: &'static str,
    }

    #[async_trait]
    impl zqlz_core::Connection for MockConnection {
        fn driver_name(&self) -> &str {
            self.driver
        }

        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<StatementResult> {
            Err(ZqlzError::NotSupported("mock".into()))
        }

        async fn query(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult> {
            Err(ZqlzError::NotSupported("mock".into()))
        }

        async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
            Err(ZqlzError::NotSupported("mock".into()))
        }

        async fn close(&self) -> Result<()> {
            Ok(())
        }

        fn is_closed(&self) -> bool {
            false
        }
    }

    fn make_importer(driver: &'static str) -> GenericImporter {
        GenericImporter::new(Arc::new(MockConnection { driver }))
    }

    fn make_index(name: &str, columns: &[&str], unique: bool) -> IndexDefinition {
        IndexDefinition {
            name: name.to_string(),
            columns: columns
                .iter()
                .map(|c| IndexColumn {
                    column: c.to_string(),
                    order: SortOrder::Asc,
                    nulls: NullsOrder::Default,
                })
                .collect(),
            unique,
            index_method: None,
            index_type_raw: None,
            where_clause: None,
            include_columns: Vec::new(),
        }
    }

    /// A connection mock that records every SQL string passed to `execute`.
    struct TrackingConnection {
        driver: &'static str,
        executed: Mutex<Vec<String>>,
    }

    impl TrackingConnection {
        fn new(driver: &'static str) -> Self {
            Self {
                driver,
                executed: Mutex::new(Vec::new()),
            }
        }

        fn executed_sql(&self) -> Vec<String> {
            self.executed.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl zqlz_core::Connection for TrackingConnection {
        fn driver_name(&self) -> &str {
            self.driver
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<StatementResult> {
            self.executed.lock().unwrap().push(sql.to_string());
            Ok(StatementResult {
                is_query: false,
                result: None,
                affected_rows: 0,
                error: None,
            })
        }

        async fn query(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult> {
            Err(ZqlzError::NotSupported("mock".into()))
        }

        async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
            Err(ZqlzError::NotSupported("mock".into()))
        }

        async fn close(&self) -> Result<()> {
            Ok(())
        }

        fn is_closed(&self) -> bool {
            false
        }
    }

    fn make_tracking_importer(driver: &'static str) -> (Arc<TrackingConnection>, GenericImporter) {
        let conn = Arc::new(TrackingConnection::new(driver));
        let importer = GenericImporter::new(Arc::clone(&conn) as Arc<dyn zqlz_core::Connection>);
        (conn, importer)
    }

    fn make_table_def_with_autoincrement() -> TableDefinition {
        let col = ColumnDefinition::new("id", crate::CanonicalType::BigInt, "bigint")
            .auto_increment();
        let mut table = TableDefinition::new("users");
        table.add_column(col);
        table
    }

    fn make_table_def_no_autoincrement() -> TableDefinition {
        let col = ColumnDefinition::new("name", crate::CanonicalType::Text, "text");
        let mut table = TableDefinition::new("users");
        table.add_column(col);
        table
    }

    /// PostgreSQL uses TRUNCATE TABLE, which is faster than DELETE FROM and
    /// resets no sequence itself (sequence restoration is handled by ic-033).
    #[tokio::test]
    async fn test_truncate_table_postgresql_emits_truncate() {
        let (conn, importer) = make_tracking_importer("postgresql");
        let table_def = make_table_def_with_autoincrement();
        importer.truncate_table("orders", &table_def).await.unwrap();
        let sqls = conn.executed_sql();
        assert_eq!(sqls.len(), 1);
        assert!(
            sqls[0].contains("TRUNCATE TABLE"),
            "PostgreSQL truncate must use TRUNCATE TABLE, got: {}",
            sqls[0]
        );
        assert!(sqls[0].contains("orders"));
    }

    /// MySQL uses TRUNCATE TABLE, which also resets AUTO_INCREMENT counters.
    #[tokio::test]
    async fn test_truncate_table_mysql_emits_truncate() {
        let (conn, importer) = make_tracking_importer("mysql");
        let table_def = make_table_def_with_autoincrement();
        importer.truncate_table("orders", &table_def).await.unwrap();
        let sqls = conn.executed_sql();
        assert_eq!(sqls.len(), 1);
        assert!(
            sqls[0].contains("TRUNCATE TABLE"),
            "MySQL truncate must use TRUNCATE TABLE, got: {}",
            sqls[0]
        );
    }

    /// SQLite falls back to DELETE FROM and also resets sqlite_sequence for
    /// tables that have an AUTOINCREMENT column, so the counter starts from 1
    /// after re-import rather than continuing from the old high-water mark.
    #[tokio::test]
    async fn test_truncate_table_sqlite_with_autoincrement_resets_sequence() {
        let (conn, importer) = make_tracking_importer("sqlite");
        let table_def = make_table_def_with_autoincrement();
        importer.truncate_table("users", &table_def).await.unwrap();
        let sqls = conn.executed_sql();
        assert_eq!(sqls.len(), 2, "SQLite must issue DELETE FROM and a sequence reset, got: {:?}", sqls);
        assert!(
            sqls[0].starts_with("DELETE FROM"),
            "first SQL must be DELETE FROM, got: {}",
            sqls[0]
        );
        assert!(
            sqls[1].contains("sqlite_sequence") && sqls[1].contains("users"),
            "second SQL must reset sqlite_sequence for the table, got: {}",
            sqls[1]
        );
    }

    /// Without an AUTOINCREMENT column SQLite only needs DELETE FROM — no
    /// sqlite_sequence entry exists for such a table.
    #[tokio::test]
    async fn test_truncate_table_sqlite_without_autoincrement_skips_sequence_reset() {
        let (conn, importer) = make_tracking_importer("sqlite");
        let table_def = make_table_def_no_autoincrement();
        importer.truncate_table("logs", &table_def).await.unwrap();
        let sqls = conn.executed_sql();
        assert_eq!(sqls.len(), 1, "no sequence reset needed for plain tables, got: {:?}", sqls);
        assert!(
            sqls[0].starts_with("DELETE FROM"),
            "SQLite without autoincrement must use DELETE FROM, got: {}",
            sqls[0]
        );
        assert!(!sqls[0].contains("sqlite_sequence"));
    }

    /// Serial canonical types count as auto-increment and must also trigger the
    /// sqlite_sequence reset path.
    #[tokio::test]
    async fn test_truncate_table_sqlite_serial_type_resets_sequence() {
        let (conn, importer) = make_tracking_importer("sqlite");
        let col = ColumnDefinition::new("id", crate::CanonicalType::Serial, "integer");
        let mut table_def = TableDefinition::new("events");
        table_def.add_column(col);
        importer.truncate_table("events", &table_def).await.unwrap();
        let sqls = conn.executed_sql();
        assert_eq!(sqls.len(), 2, "Serial type must trigger sequence reset, got: {:?}", sqls);
        assert!(sqls[1].contains("sqlite_sequence"));
    }

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

    #[test]
    fn test_generate_index_sql_non_unique() {
        let importer = make_importer("sqlite");
        let index = make_index("idx_users_email", &["email"], false);
        let sql = importer.generate_index_sql("users", &index);
        assert!(
            sql.contains("CREATE INDEX"),
            "non-unique index must not have UNIQUE keyword: {sql}"
        );
        assert!(
            !sql.contains("UNIQUE"),
            "non-unique index must not have UNIQUE keyword: {sql}"
        );
    }

    #[test]
    fn test_generate_index_sql_unique_contains_unique_keyword() {
        let importer = make_importer("sqlite");
        let index = make_index("idx_users_email_unique", &["email"], true);
        let sql = importer.generate_index_sql("users", &index);
        assert!(
            sql.contains("CREATE UNIQUE INDEX"),
            "unique index must contain 'CREATE UNIQUE INDEX': {sql}"
        );
    }

    #[test]
    fn test_generate_index_sql_unique_multi_column() {
        let importer = make_importer("postgresql");
        let index = make_index("idx_orders_user_product", &["user_id", "product_id"], true);
        let sql = importer.generate_index_sql("orders", &index);
        assert!(
            sql.contains("CREATE UNIQUE INDEX"),
            "multi-column unique index must contain 'CREATE UNIQUE INDEX': {sql}"
        );
        assert!(
            sql.contains("\"user_id\"") && sql.contains("\"product_id\""),
            "all columns must appear in the SQL: {sql}"
        );
    }

    #[test]
    fn test_generate_index_sql_mysql_uses_backticks() {
        let importer = make_importer("mysql");
        let index = make_index("idx_email", &["email"], true);
        let sql = importer.generate_index_sql("users", &index);
        assert!(
            sql.contains("`email`"),
            "MySQL identifiers must be backtick-quoted: {sql}"
        );
        assert!(
            sql.contains("CREATE UNIQUE INDEX"),
            "unique index must be UNIQUE: {sql}"
        );
    }

    // ── is_db_generated tests ───────────────────────────────────────────────

    #[test]
    fn test_is_db_generated_explicit_auto_increment_flag() {
        let col = ColumnDefinition::new("id", crate::CanonicalType::Integer, "INT")
            .auto_increment();
        assert!(
            col.is_db_generated(),
            "auto_increment flag must mark column as db-generated"
        );
    }

    #[test]
    fn test_is_db_generated_serial_canonical_type() {
        let col = ColumnDefinition::new("id", crate::CanonicalType::Serial, "serial");
        assert!(
            col.is_db_generated(),
            "Serial canonical type must mark column as db-generated"
        );
    }

    #[test]
    fn test_is_db_generated_small_serial() {
        let col = ColumnDefinition::new("id", crate::CanonicalType::SmallSerial, "smallserial");
        assert!(
            col.is_db_generated(),
            "SmallSerial canonical type must mark column as db-generated"
        );
    }

    #[test]
    fn test_is_db_generated_big_serial() {
        let col = ColumnDefinition::new("id", crate::CanonicalType::BigSerial, "bigserial");
        assert!(
            col.is_db_generated(),
            "BigSerial canonical type must mark column as db-generated"
        );
    }

    #[test]
    fn test_is_db_generated_false_for_plain_integer() {
        let col = ColumnDefinition::new("age", crate::CanonicalType::Integer, "INTEGER");
        assert!(
            !col.is_db_generated(),
            "plain Integer column must not be db-generated"
        );
    }

    #[test]
    fn test_is_db_generated_false_for_text() {
        let col = ColumnDefinition::new(
            "name",
            crate::CanonicalType::String {
                max_length: Some(255),
                fixed_length: false,
            },
            "VARCHAR(255)",
        );
        assert!(
            !col.is_db_generated(),
            "text column must not be db-generated"
        );
    }

    // ── batch INSERT helper tests ────────────────────────────────────────────

    /// SQLite must limit each statement to ≤ 32766 bound parameters.  With
    /// 100-column rows and a requested batch of 1000 the effective batch size
    /// must be capped at floor(32766 / 100) = 327.
    #[test]
    fn test_effective_batch_size_sqlite_caps_for_wide_table() {
        let importer = make_importer("sqlite");
        let effective = importer.effective_batch_size(1000, 100);
        assert_eq!(
            effective, 327,
            "SQLite: 100-col rows should cap batch to 327 (32766/100)"
        );
    }

    /// PostgreSQL allows up to 65535 parameters.  With 2 columns and batch 500
    /// the requested size fits (500 * 2 = 1000 < 65535) so it must be returned
    /// unchanged.
    #[test]
    fn test_effective_batch_size_postgres_small_table_unchanged() {
        let importer = make_importer("postgresql");
        let effective = importer.effective_batch_size(500, 2);
        assert_eq!(
            effective, 500,
            "PostgreSQL: 2-col rows with batch 500 fit within param limit"
        );
    }

    /// Zero columns (e.g. before insertable indices are computed) must not
    /// panic or divide by zero — the requested batch_size is returned as-is.
    #[test]
    fn test_effective_batch_size_zero_columns_returns_batch_size() {
        let importer = make_importer("sqlite");
        let effective = importer.effective_batch_size(1000, 0);
        assert_eq!(effective, 1000);
    }

    /// build_batch_insert_sql for PostgreSQL must use globally-numbered
    /// $1…$N placeholders across all rows so the flat params slice aligns.
    #[test]
    fn test_build_batch_insert_sql_postgres_numbered_placeholders() {
        let importer = make_importer("postgresql");
        let cols = vec![
            "\"name\"".to_string(),
            "\"age\"".to_string(),
        ];
        let sql = importer.build_batch_insert_sql("users", &cols, 3);
        // Row 0: $1, $2  |  Row 1: $3, $4  |  Row 2: $5, $6
        assert!(
            sql.contains("($1, $2)"),
            "first row placeholder must start at $1: {sql}"
        );
        assert!(
            sql.contains("($3, $4)"),
            "second row must continue numbering at $3: {sql}"
        );
        assert!(
            sql.contains("($5, $6)"),
            "third row must continue numbering at $5: {sql}"
        );
    }

    /// build_batch_insert_sql for MySQL/SQLite must use positional `?`
    /// placeholders (not numbered).
    #[test]
    fn test_build_batch_insert_sql_mysql_positional_placeholders() {
        let importer = make_importer("mysql");
        let cols = vec!["`name`".to_string(), "`age`".to_string()];
        let sql = importer.build_batch_insert_sql("users", &cols, 2);
        assert!(
            sql.contains("(?, ?)"),
            "MySQL must use positional ? placeholders: {sql}"
        );
        assert!(
            !sql.contains('$'),
            "MySQL must not use numbered $N placeholders: {sql}"
        );
    }

    /// A single-row batch must produce exactly one VALUES tuple, matching the
    /// behaviour of the old per-row INSERT path.
    #[test]
    fn test_build_batch_insert_sql_single_row() {
        let importer = make_importer("sqlite");
        let cols = vec!["\"email\"".to_string()];
        let sql = importer.build_batch_insert_sql("users", &cols, 1);
        assert!(
            sql.starts_with("INSERT INTO"),
            "must start with INSERT INTO: {sql}"
        );
        assert!(
            sql.contains("VALUES (?)"),
            "single-row batch must have exactly one placeholder tuple: {sql}"
        );
    }

    // ── topological sort tests ───────────────────────────────────────────────

    fn make_table_def(name: &str, references: &[&str]) -> TableDefinition {
        let mut table = TableDefinition::new(name);
        for &ref_table in references {
            table.foreign_keys.push(ForeignKeyConstraint {
                name: None,
                columns: vec!["ref_id".into()],
                referenced_table: ref_table.to_string(),
                referenced_schema: None,
                referenced_columns: vec!["id".into()],
                on_delete: ForeignKeyAction::NoAction,
                on_update: ForeignKeyAction::NoAction,
                is_deferrable: false,
                initially_deferred: false,
            });
        }
        table
    }

    /// A simple chain A→B→C should be emitted in order C, B, A so that each
    /// referenced table exists before its dependants are created.
    #[test]
    fn test_topological_sort_linear_chain() {
        let importer = make_importer("postgresql");
        let options = ImportOptions::default();

        let name_a = "a".to_string();
        let name_b = "b".to_string();
        let name_c = "c".to_string();
        let table_a = make_table_def("a", &["b"]); // a depends on b
        let table_b = make_table_def("b", &["c"]); // b depends on c
        let table_c = make_table_def("c", &[]);    // c has no deps

        // Pass in reverse order to show sorting is not just order-preserving.
        let tables: Vec<(&String, &TableDefinition)> =
            vec![(&name_a, &table_a), (&name_b, &table_b), (&name_c, &table_c)];

        let sorted = importer.topological_sort_tables(&tables, &options);
        let names: Vec<&str> = sorted.iter().map(|(n, _)| n.as_str()).collect();

        // c must appear before b, b before a
        let pos_a = names.iter().position(|&n| n == "a").unwrap();
        let pos_b = names.iter().position(|&n| n == "b").unwrap();
        let pos_c = names.iter().position(|&n| n == "c").unwrap();
        assert!(pos_c < pos_b, "c must precede b: order = {:?}", names);
        assert!(pos_b < pos_a, "b must precede a: order = {:?}", names);
    }

    /// Tables with no FK relationships should all be present in the output
    /// (in any order since there are no constraints).
    #[test]
    fn test_topological_sort_independent_tables() {
        let importer = make_importer("sqlite");
        let options = ImportOptions::default();

        let names: Vec<String> = ["users", "products", "categories"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let defs: Vec<TableDefinition> = names
            .iter()
            .map(|n| make_table_def(n, &[]))
            .collect();
        let tables: Vec<(&String, &TableDefinition)> =
            names.iter().zip(defs.iter()).collect();

        let sorted = importer.topological_sort_tables(&tables, &options);
        assert_eq!(sorted.len(), 3, "all tables must be present in output");
    }

    /// A mutual circular reference (A→B and B→A) must not cause a hang or
    /// panic — both tables must appear in the output, one appended after the
    /// other without crashing.
    #[test]
    fn test_topological_sort_circular_reference_does_not_panic() {
        let importer = make_importer("postgresql");
        let options = ImportOptions::default();

        let name_a = "a".to_string();
        let name_b = "b".to_string();
        let table_a = make_table_def("a", &["b"]);
        let table_b = make_table_def("b", &["a"]);

        let tables: Vec<(&String, &TableDefinition)> =
            vec![(&name_a, &table_a), (&name_b, &table_b)];

        let sorted = importer.topological_sort_tables(&tables, &options);
        assert_eq!(sorted.len(), 2, "both cyclic tables must appear in output");
    }

    /// A self-referencing table (parent_id → same table) must not create a
    /// trivial cycle — the table must appear in the output exactly once.
    #[test]
    fn test_topological_sort_self_reference() {
        let importer = make_importer("sqlite");
        let options = ImportOptions::default();

        let name = "nodes".to_string();
        let table = make_table_def("nodes", &["nodes"]);
        let tables: Vec<(&String, &TableDefinition)> = vec![(&name, &table)];

        let sorted = importer.topological_sort_tables(&tables, &options);
        assert_eq!(sorted.len(), 1, "self-referencing table must appear once");
    }

    /// A FK referencing a table that is not included in the import set must
    /// not affect the sort — the external reference is ignored and all
    /// included tables are still returned.
    #[test]
    fn test_topological_sort_external_reference_ignored() {
        let importer = make_importer("postgresql");
        let options = ImportOptions::default();

        let name_orders = "orders".to_string();
        // orders references customers, but customers is not in the import set
        let table_orders = make_table_def("orders", &["customers"]);

        let tables: Vec<(&String, &TableDefinition)> = vec![(&name_orders, &table_orders)];

        let sorted = importer.topological_sort_tables(&tables, &options);
        assert_eq!(sorted.len(), 1, "orders must still appear despite external FK");
    }

    /// generate_foreign_key_sql must produce an ALTER TABLE ADD CONSTRAINT
    /// statement with the correct ON DELETE and ON UPDATE clauses.
    #[test]
    fn test_generate_foreign_key_sql_cascade() {
        let importer = make_importer("postgresql");
        let fk = ForeignKeyConstraint {
            name: Some("fk_orders_customer".into()),
            columns: vec!["customer_id".into()],
            referenced_table: "customers".into(),
            referenced_schema: None,
            referenced_columns: vec!["id".into()],
            on_delete: ForeignKeyAction::Cascade,
            on_update: ForeignKeyAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };

        let sql = importer.generate_foreign_key_sql("orders", &fk);
        assert!(
            sql.contains("ALTER TABLE"),
            "must be an ALTER TABLE statement: {sql}"
        );
        assert!(
            sql.contains("ADD CONSTRAINT"),
            "must use ADD CONSTRAINT: {sql}"
        );
        assert!(
            sql.contains("ON DELETE CASCADE"),
            "must contain ON DELETE CASCADE: {sql}"
        );
        assert!(
            !sql.contains("ON UPDATE"),
            "NoAction must not emit ON UPDATE clause: {sql}"
        );
    }

    // ── check constraint DDL tests ──────────────────────────────────────────

    /// A named CHECK constraint must emit `CONSTRAINT <name> CHECK (<expr>)`.
    #[test]
    fn test_generate_check_constraint_named() {
        let importer = make_importer("postgresql");
        let check = CheckConstraint {
            name: Some("chk_price_positive".into()),
            expression: "price > 0".into(),
        };
        let sql = importer.generate_check_constraint_sql(&check);
        assert!(
            sql.contains("CONSTRAINT"),
            "named constraint must use CONSTRAINT keyword: {sql}"
        );
        assert!(
            sql.contains("chk_price_positive"),
            "constraint name must appear in DDL: {sql}"
        );
        assert!(
            sql.contains("CHECK (price > 0)"),
            "CHECK expression must appear verbatim: {sql}"
        );
    }

    /// An unnamed CHECK constraint must emit `CHECK (<expr>)` without the
    /// CONSTRAINT keyword so the DDL is valid on all three drivers.
    #[test]
    fn test_generate_check_constraint_unnamed() {
        let importer = make_importer("sqlite");
        let check = CheckConstraint {
            name: None,
            expression: "age >= 0".into(),
        };
        let sql = importer.generate_check_constraint_sql(&check);
        assert!(
            !sql.contains("CONSTRAINT"),
            "unnamed constraint must not use CONSTRAINT keyword: {sql}"
        );
        assert!(
            sql.contains("CHECK (age >= 0)"),
            "CHECK expression must appear verbatim: {sql}"
        );
    }

    /// generate_create_table_sql must embed CHECK constraints inline for a
    /// table that has them.
    #[test]
    fn test_generate_create_table_sql_includes_check_constraints() {
        let importer = make_importer("postgresql");
        let mut table = TableDefinition::new("products");
        table.add_column(ColumnDefinition::new(
            "price",
            crate::CanonicalType::Double,
            "DOUBLE PRECISION",
        ));
        table.check_constraints.push(CheckConstraint {
            name: Some("chk_price_positive".into()),
            expression: "price > 0".into(),
        });

        let sql = importer.generate_create_table_sql(&table);
        assert!(
            sql.contains("CHECK (price > 0)"),
            "CREATE TABLE must contain the CHECK expression: {sql}"
        );
        assert!(
            sql.contains("CONSTRAINT"),
            "named CHECK must use CONSTRAINT keyword: {sql}"
        );
    }

    /// A table with no check constraints must not contain any CHECK keyword.
    #[test]
    fn test_generate_create_table_sql_no_check_constraints() {
        let importer = make_importer("sqlite");
        let mut table = TableDefinition::new("users");
        table.add_column(ColumnDefinition::new(
            "name",
            crate::CanonicalType::Text,
            "TEXT",
        ));

        let sql = importer.generate_create_table_sql(&table);
        assert!(
            !sql.contains("CHECK"),
            "table without check constraints must not emit CHECK keyword: {sql}"
        );
    }

    /// Multiple CHECK constraints on the same table must all appear in the DDL.
    #[test]
    fn test_generate_create_table_sql_multiple_check_constraints() {
        let importer = make_importer("mysql");
        let mut table = TableDefinition::new("orders");
        table.add_column(ColumnDefinition::new(
            "quantity",
            crate::CanonicalType::Integer,
            "INT",
        ));
        table.add_column(ColumnDefinition::new(
            "discount",
            crate::CanonicalType::Double,
            "DOUBLE",
        ));
        table.check_constraints.push(CheckConstraint {
            name: Some("chk_quantity_pos".into()),
            expression: "quantity > 0".into(),
        });
        table.check_constraints.push(CheckConstraint {
            name: None,
            expression: "discount <= 1.0".into(),
        });

        let sql = importer.generate_create_table_sql(&table);
        assert!(
            sql.contains("CHECK (quantity > 0)"),
            "first CHECK must appear: {sql}"
        );
        assert!(
            sql.contains("CHECK (discount <= 1.0)"),
            "second CHECK must appear: {sql}"
        );
    }

    // ===== generate_column_sql semantic DefaultValue tests =====

    fn make_col_with_default(name: &str, default: crate::document::DefaultValue) -> ColumnDefinition {
        let mut col = ColumnDefinition::new(name, crate::CanonicalType::Text, "TEXT");
        col.default_value = Some(default);
        col
    }

    #[test]
    fn test_current_timestamp_emits_standard_sql_on_all_drivers() {
        use crate::document::DefaultValue;
        for driver in &["postgresql", "mysql", "sqlite"] {
            let importer = make_importer(driver);
            let col = make_col_with_default("created_at", DefaultValue::CurrentTimestamp);
            let sql = importer.generate_column_sql(&col);
            assert!(
                sql.contains("DEFAULT CURRENT_TIMESTAMP"),
                "driver={driver}: {sql}"
            );
        }
    }

    #[test]
    fn test_current_date_emits_standard_sql_on_all_drivers() {
        use crate::document::DefaultValue;
        for driver in &["postgresql", "mysql", "sqlite"] {
            let importer = make_importer(driver);
            let col = make_col_with_default("day", DefaultValue::CurrentDate);
            let sql = importer.generate_column_sql(&col);
            assert!(sql.contains("DEFAULT CURRENT_DATE"), "driver={driver}: {sql}");
        }
    }

    #[test]
    fn test_current_time_emits_standard_sql_on_all_drivers() {
        use crate::document::DefaultValue;
        for driver in &["postgresql", "mysql", "sqlite"] {
            let importer = make_importer(driver);
            let col = make_col_with_default("ts", DefaultValue::CurrentTime);
            let sql = importer.generate_column_sql(&col);
            assert!(sql.contains("DEFAULT CURRENT_TIME"), "driver={driver}: {sql}");
        }
    }

    #[test]
    fn test_current_user_falls_back_to_null_on_sqlite() {
        use crate::document::DefaultValue;
        let importer = make_importer("sqlite");
        let col = make_col_with_default("owner", DefaultValue::CurrentUser);
        let sql = importer.generate_column_sql(&col);
        assert!(sql.contains("DEFAULT NULL"), "sqlite fallback: {sql}");

        // A warning must be issued for the SQLite downgrade.
        let warnings = importer.generate_column_default_warnings("my_table", &col);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].kind, ImportWarningKind::DefaultModified);
    }

    #[test]
    fn test_current_user_emitted_correctly_on_postgres_and_mysql() {
        use crate::document::DefaultValue;
        for driver in &["postgresql", "mysql"] {
            let importer = make_importer(driver);
            let col = make_col_with_default("owner", DefaultValue::CurrentUser);
            let sql = importer.generate_column_sql(&col);
            assert!(sql.contains("DEFAULT CURRENT_USER"), "driver={driver}: {sql}");
            let warnings = importer.generate_column_default_warnings("t", &col);
            assert!(warnings.is_empty(), "no warning expected for {driver}");
        }
    }

    #[test]
    fn test_generated_uuid_emits_driver_specific_sql() {
        use crate::document::DefaultValue;
        let pg = make_importer("postgresql");
        let col = make_col_with_default("id", DefaultValue::GeneratedUuid);
        assert!(
            pg.generate_column_sql(&col).contains("gen_random_uuid()"),
            "postgresql uuid"
        );

        let my = make_importer("mysql");
        let col = make_col_with_default("id", DefaultValue::GeneratedUuid);
        assert!(
            my.generate_column_sql(&col).contains("UUID()"),
            "mysql uuid"
        );

        // SQLite emits nothing (no built-in) but must warn.
        let sq = make_importer("sqlite");
        let col = make_col_with_default("id", DefaultValue::GeneratedUuid);
        let sql = sq.generate_column_sql(&col);
        assert!(!sql.contains("DEFAULT"), "sqlite: no default expected: {sql}");
        let warnings = sq.generate_column_default_warnings("t", &col);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].kind, ImportWarningKind::DefaultModified);
    }

    #[test]
    fn test_raw_expression_with_function_call_warns_on_mysql_and_sqlite() {
        use crate::document::DefaultValue;
        for driver in &["mysql", "sqlite"] {
            let importer = make_importer(driver);
            let col = make_col_with_default("x", DefaultValue::Expression("my_func()".into()));
            let warnings = importer.generate_column_default_warnings("t", &col);
            assert_eq!(warnings.len(), 1, "driver={driver}");
            assert_eq!(warnings[0].kind, ImportWarningKind::DefaultModified);
        }
    }

    #[test]
    fn test_raw_expression_without_parens_does_not_warn() {
        use crate::document::DefaultValue;
        // A plain literal-like string has no parentheses — no warning should fire.
        for driver in &["mysql", "sqlite", "postgresql"] {
            let importer = make_importer(driver);
            let col = make_col_with_default("x", DefaultValue::Expression("42".into()));
            let warnings = importer.generate_column_default_warnings("t", &col);
            assert!(warnings.is_empty(), "driver={driver}: unexpected warning");
        }
    }

    // ── map_index_to_sql / cross-DB index mapping tests ─────────────────────

    fn make_index_with_method(
        name: &str,
        columns: &[&str],
        unique: bool,
        method: Option<IndexMethod>,
    ) -> IndexDefinition {
        IndexDefinition {
            name: name.to_string(),
            columns: columns
                .iter()
                .map(|c| crate::document::IndexColumn {
                    column: c.to_string(),
                    order: crate::document::SortOrder::Asc,
                    nulls: crate::document::NullsOrder::Default,
                })
                .collect(),
            unique,
            index_method: method,
            index_type_raw: None,
            where_clause: None,
            include_columns: Vec::new(),
        }
    }

    /// BTREE on PostgreSQL must produce `CREATE INDEX … ON … (…)` with no USING clause
    /// (BTREE is the default; omitting it keeps the DDL clean).
    #[test]
    fn test_index_btree_postgres_no_using_clause() {
        let importer = make_importer("postgresql");
        let index = make_index_with_method("idx_name", &["name"], false, Some(IndexMethod::Btree));
        let outcome = importer.map_index_to_sql("users", &index);
        let sql = outcome.expect("BTREE must succeed");
        assert!(!sql.contains("USING"), "BTREE must omit USING clause: {sql}");
        assert!(sql.starts_with("CREATE INDEX"), "must be CREATE INDEX: {sql}");
    }

    /// UNIQUE BTREE on MySQL must use backtick quoting and no USING clause.
    #[test]
    fn test_index_unique_btree_mysql_backticks() {
        let importer = make_importer("mysql");
        let index = make_index_with_method("idx_email", &["email"], true, Some(IndexMethod::Btree));
        let sql = importer.generate_index_sql("users", &index);
        assert!(sql.contains("CREATE UNIQUE INDEX"), "must be UNIQUE: {sql}");
        assert!(sql.contains("`email`"), "MySQL must use backtick quoting: {sql}");
        assert!(!sql.contains("USING"), "BTREE must omit USING clause: {sql}");
    }

    /// GIN on PostgreSQL must emit `USING GIN`.
    #[test]
    fn test_index_gin_postgres_emits_using_gin() {
        let importer = make_importer("postgresql");
        let index = make_index_with_method("idx_tags", &["tags"], false, Some(IndexMethod::Gin));
        let sql = importer
            .map_index_to_sql("articles", &index)
            .expect("GIN must succeed on PostgreSQL");
        assert!(sql.contains("USING GIN"), "must contain USING GIN: {sql}");
    }

    /// GIN on MySQL must be skipped (no equivalent) with a descriptive warning.
    #[test]
    fn test_index_gin_mysql_is_skipped() {
        let importer = make_importer("mysql");
        let index = make_index_with_method("idx_tags", &["tags"], false, Some(IndexMethod::Gin));
        match importer.map_index_to_sql("articles", &index) {
            Err(IndexMappingOutcome::Skipped(msg)) => {
                assert!(
                    msg.contains("GIN"),
                    "skip message must mention GIN: {msg}"
                );
                assert!(
                    msg.contains("MySQL") || msg.contains("mysql"),
                    "skip message must mention MySQL: {msg}"
                );
            }
            _other => panic!("GIN on MySQL must be Skipped, got non-skip outcome"),
        }
    }

    /// GIN on SQLite must also be skipped.
    #[test]
    fn test_index_gin_sqlite_is_skipped() {
        let importer = make_importer("sqlite");
        let index = make_index_with_method("idx_tags", &["tags"], false, Some(IndexMethod::Gin));
        assert!(
            matches!(
                importer.map_index_to_sql("articles", &index),
                Err(IndexMappingOutcome::Skipped(_))
            ),
            "GIN must be skipped on SQLite"
        );
    }

    /// HASH on MySQL must degrade to BTREE with a warning (not be skipped).
    #[test]
    fn test_index_hash_mysql_degrades_to_btree() {
        let importer = make_importer("mysql");
        let index = make_index_with_method("idx_hash", &["id"], false, Some(IndexMethod::Hash));
        match importer.map_index_to_sql("events", &index) {
            Err(IndexMappingOutcome::Degraded { sql, messages }) => {
                assert!(
                    !sql.contains("USING"),
                    "degraded hash must fall back to default (no USING): {sql}"
                );
                assert!(!messages.is_empty(), "must have at least one degradation message");
                assert!(
                    messages[0].contains("MEMORY") || messages[0].contains("BTREE"),
                    "message must explain the HASH limitation: {}",
                    messages[0]
                );
            }
            _other => panic!("HASH on MySQL must be Degraded"),
        }
    }

    /// FULLTEXT on MySQL must emit ` FULLTEXT` in the CREATE statement.
    #[test]
    fn test_index_fulltext_mysql_emits_fulltext_keyword() {
        let importer = make_importer("mysql");
        let index =
            make_index_with_method("idx_body", &["body"], false, Some(IndexMethod::Fulltext));
        let sql = importer
            .map_index_to_sql("posts", &index)
            .expect("FULLTEXT must succeed on MySQL");
        assert!(
            sql.contains("FULLTEXT"),
            "MySQL FULLTEXT index must contain FULLTEXT keyword: {sql}"
        );
    }

    /// FULLTEXT on PostgreSQL must degrade (not skip) with a message about GIN/tsvector.
    #[test]
    fn test_index_fulltext_postgres_degrades_with_gin_hint() {
        let importer = make_importer("postgresql");
        let index =
            make_index_with_method("idx_body", &["body"], false, Some(IndexMethod::Fulltext));
        match importer.map_index_to_sql("posts", &index) {
            Err(IndexMappingOutcome::Degraded { messages, .. }) => {
                assert!(
                    messages.iter().any(|m| m.contains("GIN") || m.contains("tsvector")),
                    "degradation message must hint at GIN/tsvector: {:?}",
                    messages
                );
            }
            _other => panic!("FULLTEXT on PostgreSQL must be Degraded"),
        }
    }

    /// SPATIAL on PostgreSQL must translate to GIST with a degradation warning.
    #[test]
    fn test_index_spatial_postgres_translates_to_gist() {
        let importer = make_importer("postgresql");
        let index =
            make_index_with_method("idx_geom", &["geom"], false, Some(IndexMethod::Spatial));
        match importer.map_index_to_sql("locations", &index) {
            Err(IndexMappingOutcome::Degraded { sql, .. }) => {
                assert!(
                    sql.contains("USING GIST"),
                    "SPATIAL→PG must translate to GIST: {sql}"
                );
            }
            _other => panic!("SPATIAL on PostgreSQL must be Degraded (GIST translation)"),
        }
    }

    /// Partial index WHERE clause must be emitted on PostgreSQL.
    #[test]
    fn test_index_partial_where_clause_on_postgres() {
        let importer = make_importer("postgresql");
        let mut index = make_index_with_method("idx_active", &["id"], false, None);
        index.where_clause = Some("active = true".to_string());
        let sql = importer
            .map_index_to_sql("users", &index)
            .expect("partial index must succeed on PostgreSQL");
        assert!(
            sql.contains("WHERE active = true"),
            "WHERE clause must be present: {sql}"
        );
    }

    /// Partial index WHERE clause must be emitted on SQLite.
    #[test]
    fn test_index_partial_where_clause_on_sqlite() {
        let importer = make_importer("sqlite");
        let mut index = make_index_with_method("idx_active", &["id"], false, None);
        index.where_clause = Some("status = 'pending'".to_string());
        let sql = importer
            .map_index_to_sql("tasks", &index)
            .expect("partial index must succeed on SQLite");
        assert!(
            sql.contains("WHERE status = 'pending'"),
            "WHERE clause must be present on SQLite: {sql}"
        );
    }

    /// Partial index on MySQL must degrade (index created without WHERE) with a warning.
    #[test]
    fn test_index_partial_where_clause_mysql_degrades() {
        let importer = make_importer("mysql");
        let mut index = make_index_with_method("idx_active", &["id"], false, None);
        index.where_clause = Some("active = 1".to_string());
        match importer.map_index_to_sql("users", &index) {
            Err(IndexMappingOutcome::Degraded { sql, messages }) => {
                assert!(
                    !sql.contains("WHERE"),
                    "MySQL degraded index must not contain WHERE: {sql}"
                );
                assert!(
                    messages.iter().any(|m| m.contains("partial") || m.contains("WHERE")),
                    "degradation message must mention partial index: {:?}",
                    messages
                );
            }
            _other => panic!("Partial index on MySQL must be Degraded"),
        }
    }

    /// INCLUDE columns must appear in the CREATE INDEX on PostgreSQL.
    #[test]
    fn test_index_include_columns_on_postgres() {
        let importer = make_importer("postgresql");
        let mut index = make_index_with_method("idx_cust", &["customer_id"], false, None);
        index.include_columns = vec!["total".to_string(), "status".to_string()];
        let sql = importer
            .map_index_to_sql("orders", &index)
            .expect("covering index must succeed on PostgreSQL");
        assert!(
            sql.contains("INCLUDE"),
            "INCLUDE clause must be present: {sql}"
        );
        assert!(
            sql.contains("\"total\"") && sql.contains("\"status\""),
            "INCLUDE columns must be quoted: {sql}"
        );
    }

    /// INCLUDE columns on MySQL must be omitted with a degradation warning.
    #[test]
    fn test_index_include_columns_mysql_degrades() {
        let importer = make_importer("mysql");
        let mut index = make_index_with_method("idx_cust", &["customer_id"], false, None);
        index.include_columns = vec!["total".to_string()];
        match importer.map_index_to_sql("orders", &index) {
            Err(IndexMappingOutcome::Degraded { sql, messages }) => {
                assert!(
                    !sql.contains("INCLUDE"),
                    "MySQL must not emit INCLUDE: {sql}"
                );
                assert!(
                    messages.iter().any(|m| m.contains("INCLUDE") || m.contains("covering")),
                    "degradation message must mention INCLUDE/covering: {:?}",
                    messages
                );
            }
            _other => panic!("Covering index on MySQL must be Degraded"),
        }
    }

    /// DESC column order must appear in the generated SQL.
    #[test]
    fn test_index_desc_column_order_in_sql() {
        let importer = make_importer("postgresql");
        let index = IndexDefinition {
            name: "idx_created_desc".to_string(),
            columns: vec![crate::document::IndexColumn {
                column: "created_at".to_string(),
                order: crate::document::SortOrder::Desc,
                nulls: crate::document::NullsOrder::Default,
            }],
            unique: false,
            index_method: None,
            index_type_raw: None,
            where_clause: None,
            include_columns: Vec::new(),
        };
        let sql = importer
            .map_index_to_sql("events", &index)
            .expect("DESC index must succeed");
        assert!(
            sql.contains("DESC"),
            "DESC sort order must appear in SQL: {sql}"
        );
    }

    /// NULLS FIRST must be emitted on PostgreSQL but silently omitted on MySQL.
    #[test]
    fn test_index_nulls_first_postgres_present_mysql_absent() {
        let index = IndexDefinition {
            name: "idx_nullable".to_string(),
            columns: vec![crate::document::IndexColumn {
                column: "nullable_col".to_string(),
                order: crate::document::SortOrder::Asc,
                nulls: crate::document::NullsOrder::First,
            }],
            unique: false,
            index_method: None,
            index_type_raw: None,
            where_clause: None,
            include_columns: Vec::new(),
        };

        let pg = make_importer("postgresql");
        let pg_sql = pg
            .map_index_to_sql("t", &index)
            .expect("must succeed on PostgreSQL");
        assert!(
            pg_sql.contains("NULLS FIRST"),
            "NULLS FIRST must appear on PostgreSQL: {pg_sql}"
        );

        let my = make_importer("mysql");
        let my_sql = my.generate_index_sql("t", &index);
        assert!(
            !my_sql.contains("NULLS"),
            "NULLS ordering must not appear on MySQL: {my_sql}"
        );
    }

    // ── ic-032: generated column DDL ────────────────────────────────────────

    /// Helper that builds a minimal ColumnDefinition with a generation expression.
    fn make_generated_col(name: &str, expr: &str, stored: bool) -> ColumnDefinition {
        ColumnDefinition::new(name, crate::CanonicalType::Integer, "INTEGER").generated(expr, stored)
    }

    #[test]
    fn test_generated_column_stored_emits_generated_always_as_stored() {
        let importer = make_importer("postgresql");
        let col = make_generated_col("full_name", "first_name || ' ' || last_name", true);
        let sql = importer.generate_column_sql(&col);
        assert!(
            sql.contains("GENERATED ALWAYS AS"),
            "must contain GENERATED ALWAYS AS: {sql}"
        );
        assert!(sql.contains("STORED"), "must contain STORED: {sql}");
        assert!(
            !sql.contains("VIRTUAL"),
            "must not contain VIRTUAL for stored col: {sql}"
        );
    }

    #[test]
    fn test_generated_column_virtual_on_mysql_emits_virtual() {
        let importer = make_importer("mysql");
        let col = make_generated_col("initials", "UPPER(LEFT(name, 1))", false);
        let sql = importer.generate_column_sql(&col);
        assert!(
            sql.contains("GENERATED ALWAYS AS"),
            "must contain GENERATED ALWAYS AS: {sql}"
        );
        assert!(sql.contains("VIRTUAL"), "must contain VIRTUAL on MySQL: {sql}");
    }

    #[test]
    fn test_generated_column_virtual_on_sqlite_emits_virtual() {
        let importer = make_importer("sqlite");
        let col = make_generated_col("upper_name", "UPPER(name)", false);
        let sql = importer.generate_column_sql(&col);
        assert!(
            sql.contains("VIRTUAL"),
            "must contain VIRTUAL on SQLite: {sql}"
        );
    }

    /// PostgreSQL does not support VIRTUAL; a VIRTUAL source column is coerced to STORED.
    #[test]
    fn test_generated_column_virtual_coerced_to_stored_on_postgresql() {
        let importer = make_importer("postgresql");
        // is_generated_stored = false (VIRTUAL in the source)
        let col = make_generated_col("lower_email", "LOWER(email)", false);
        let sql = importer.generate_column_sql(&col);
        assert!(
            sql.contains("STORED"),
            "VIRTUAL column must be coerced to STORED on PostgreSQL: {sql}"
        );
        assert!(
            !sql.contains("VIRTUAL"),
            "VIRTUAL keyword must not appear on PostgreSQL: {sql}"
        );
    }

    /// Importing from a different driver must emit a GeneratedColumnDegraded warning
    /// because the expression may use driver-specific functions.
    #[test]
    fn test_generated_column_cross_driver_emits_warning() {
        let importer = make_importer("postgresql");
        let col = make_generated_col("slug", "LOWER(title)", true);
        let warnings = importer.generate_generated_column_warnings("posts", &col, Some("mysql"));
        assert!(
            !warnings.is_empty(),
            "cross-driver generated column must produce a warning"
        );
        assert!(
            warnings
                .iter()
                .any(|w| w.kind == ImportWarningKind::GeneratedColumnDegraded),
            "warning must be GeneratedColumnDegraded"
        );
    }

    /// Same-driver import must not emit a cross-driver portability warning.
    #[test]
    fn test_generated_column_same_driver_no_portability_warning() {
        let importer = make_importer("postgresql");
        let col = make_generated_col("full_name", "first_name || ' ' || last_name", true);
        let warnings =
            importer.generate_generated_column_warnings("users", &col, Some("postgresql"));
        // There may be no warning at all, or only VIRTUAL→STORED coercion warnings,
        // but there must be no cross-driver portability warning.
        for w in &warnings {
            let msg = &w.message;
            assert!(
                !msg.contains("may use driver-specific functions"),
                "same-driver import must not warn about driver-specific functions: {msg}"
            );
        }
    }

    /// VIRTUAL→STORED coercion warning must be emitted when the source is VIRTUAL
    /// and the target driver is PostgreSQL.
    #[test]
    fn test_generated_column_virtual_to_stored_coercion_warning_on_postgresql() {
        let importer = make_importer("postgresql");
        let col = make_generated_col("computed", "a + b", false); // VIRTUAL
        let warnings =
            importer.generate_generated_column_warnings("tbl", &col, Some("postgresql"));
        assert!(
            warnings.iter().any(|w| {
                w.kind == ImportWarningKind::GeneratedColumnDegraded
                    && w.message.contains("VIRTUAL")
                    && w.message.contains("STORED")
            }),
            "must warn about VIRTUAL→STORED coercion on PostgreSQL: {warnings:?}"
        );
    }

    /// A non-generated column must not produce any generated-column warnings.
    #[test]
    fn test_non_generated_column_produces_no_generated_warnings() {
        let importer = make_importer("postgresql");
        let col = ColumnDefinition::new("email", crate::CanonicalType::Text, "TEXT");
        let warnings =
            importer.generate_generated_column_warnings("users", &col, Some("mysql"));
        assert!(
            warnings.is_empty(),
            "plain column must not produce generated-column warnings"
        );
    }

    /// Generated columns must be excluded from the INSERT column list because the
    /// database engine computes their values.  `is_db_generated()` already covers
    /// this; verify the behaviour is preserved for generated-expression columns.
    #[test]
    fn test_generated_column_is_db_generated_true() {
        let col = make_generated_col("computed_col", "a * 2", true);
        assert!(
            col.is_db_generated(),
            "column with generation_expression must report is_db_generated() == true"
        );
    }

    /// `push_warning` must keep `warnings` and `degradation_warnings` in sync: one
    /// call must append exactly one entry to each collection, with matching fields.
    #[test]
    fn test_push_warning_keeps_collections_in_sync() {
        let mut result = ImportResult::new();
        result.push_warning(
            ImportWarning {
                table: Some("orders".to_string()),
                column: Some("status".to_string()),
                message: "type degraded".to_string(),
                kind: ImportWarningKind::TypeConversion,
            },
            DegradationCategory::TypeConversion,
            Some("status".to_string()),
            "ENUM",
            "TEXT",
            DegradationSeverity::Warning,
        );

        assert_eq!(result.warnings.len(), 1, "must have exactly one ImportWarning");
        assert_eq!(
            result.degradation_warnings.len(),
            1,
            "must have exactly one DegradationWarning"
        );

        let dw = &result.degradation_warnings[0];
        assert_eq!(dw.table_name, "orders");
        assert_eq!(dw.object_name.as_deref(), Some("status"));
        assert_eq!(dw.source_feature, "ENUM");
        assert_eq!(dw.target_action, "TEXT");
        assert!(matches!(dw.severity, DegradationSeverity::Warning));
        assert!(matches!(dw.category, DegradationCategory::TypeConversion));
    }

    /// When `push_warning` is called with a warning whose `table` is `None`,
    /// the degradation entry must use the sentinel value `<unknown>` rather than
    /// panicking or producing an empty string.
    #[test]
    fn test_push_warning_table_none_uses_sentinel() {
        let mut result = ImportResult::new();
        result.push_warning(
            ImportWarning {
                table: None,
                column: None,
                message: "tableless warning".to_string(),
                kind: ImportWarningKind::ConstraintSkipped,
            },
            DegradationCategory::ForeignKey,
            None,
            "FK to external schema",
            "dropped",
            DegradationSeverity::Dropped,
        );

        let dw = &result.degradation_warnings[0];
        assert_eq!(dw.table_name, "<unknown>", "missing table must use sentinel");
    }

    /// `ImportWizardStep::Summary` must not allow forward or backward navigation
    /// for either import format, since it is a terminal step.
    #[test]
    fn test_summary_step_has_no_navigation() {
        use crate::widgets::ImportWizardStep;

        let step = ImportWizardStep::Summary;
        assert!(
            !step.can_go_next(),
            "Summary must not allow forward navigation"
        );
        assert!(
            step.next_for_format(true).is_none(),
            "Summary.next_for_format(UDIF) must be None"
        );
        assert!(
            step.next_for_format(false).is_none(),
            "Summary.next_for_format(CSV) must be None"
        );
        assert!(
            step.previous_for_format(true).is_none(),
            "Summary.previous_for_format(UDIF) must be None"
        );
        assert!(
            step.previous_for_format(false).is_none(),
            "Summary.previous_for_format(CSV) must be None"
        );
    }

    /// `all_for_format` must include `Summary` as the last step for both UDIF and CSV.
    #[test]
    fn test_all_for_format_includes_summary_as_last_step() {
        use crate::widgets::ImportWizardStep;

        let udif_steps = ImportWizardStep::all_for_format(true);
        assert_eq!(
            udif_steps.last(),
            Some(&ImportWizardStep::Summary),
            "UDIF steps must end with Summary"
        );

        let csv_steps = ImportWizardStep::all_for_format(false);
        assert_eq!(
            csv_steps.last(),
            Some(&ImportWizardStep::Summary),
            "CSV steps must end with Summary"
        );
    }

    // ── enum type DDL tests ──────────────────────────────────────────────────

    fn make_enum_col(_table_name: &str, col_name: &str, values: &[&str], named: Option<&str>) -> ColumnDefinition {
        ColumnDefinition::new(
            col_name,
            crate::CanonicalType::Enum {
                name: named.map(|s| s.to_owned()),
                values: values.iter().map(|s| s.to_string()).collect(),
            },
            "mood",
        )
    }

    /// On a PostgreSQL target a named enum should be emitted as just the type name
    /// in the column DDL (no inline values), because `CREATE TYPE` is issued separately.
    #[test]
    fn test_enum_pg_column_sql_uses_type_name() {
        let importer = make_importer("postgresql");
        let col = make_enum_col("people", "mood", &["happy", "sad"], Some("mood"));
        let sql = importer.generate_column_sql(&col);
        assert!(
            sql.contains("mood"),
            "PG column DDL must reference the enum type name: {sql}"
        );
        assert!(
            !sql.contains("CHECK"),
            "PG named enum column must not emit inline CHECK: {sql}"
        );
    }

    /// On a MySQL target an enum column must emit inline `ENUM('val1','val2')`.
    #[test]
    fn test_enum_mysql_inline_enum_in_column() {
        let importer = make_importer("mysql");
        let col = make_enum_col("people", "mood", &["happy", "sad"], None);
        let sql = importer.generate_column_sql(&col);
        assert!(
            sql.contains("ENUM") || sql.contains("enum"),
            "MySQL column DDL must contain ENUM keyword: {sql}"
        );
        assert!(
            sql.contains("'happy'") && sql.contains("'sad'"),
            "MySQL enum column must embed values: {sql}"
        );
    }

    /// On a SQLite target an enum column must be TEXT with a CHECK constraint.
    #[test]
    fn test_enum_sqlite_text_with_check_constraint() {
        let importer = make_importer("sqlite");
        let col = make_enum_col("people", "mood", &["happy", "sad"], Some("mood"));
        let sql = importer.generate_column_sql(&col);
        assert!(
            sql.to_uppercase().contains("TEXT"),
            "SQLite enum column must use TEXT: {sql}"
        );
        assert!(
            sql.contains("CHECK"),
            "SQLite enum column must emit inline CHECK constraint: {sql}"
        );
        assert!(
            sql.contains("'happy'") && sql.contains("'sad'"),
            "SQLite CHECK must include all enum values: {sql}"
        );
    }

    /// `generate_create_table_sql` for SQLite must include the CHECK constraint
    /// when a column uses an enum type.
    #[test]
    fn test_generate_create_table_sql_enum_sqlite_includes_check() {
        let importer = make_importer("sqlite");
        let mut table = TableDefinition::new("people");
        table.add_column(make_enum_col("people", "mood", &["happy", "sad", "angry"], Some("mood")));
        let sql = importer.generate_create_table_sql(&table);
        assert!(
            sql.contains("CHECK"),
            "CREATE TABLE must include CHECK for enum on SQLite: {sql}"
        );
        assert!(
            sql.contains("'happy'") && sql.contains("'sad'") && sql.contains("'angry'"),
            "CHECK must enumerate all allowed values: {sql}"
        );
    }

    /// `generate_create_enum_type_sql` must emit valid `CREATE TYPE … AS ENUM (…)`.
    #[test]
    fn test_generate_create_enum_type_sql() {
        let importer = make_importer("postgresql");
        let enum_def = EnumDefinition {
            name: "mood".to_string(),
            schema: None,
            values: vec!["happy".to_string(), "sad".to_string(), "angry".to_string()],
        };
        let sql = importer.generate_create_enum_type_sql(&enum_def);
        assert!(
            sql.starts_with("CREATE TYPE"),
            "must start with CREATE TYPE: {sql}"
        );
        assert!(
            sql.contains("AS ENUM"),
            "must contain AS ENUM keyword: {sql}"
        );
        assert!(
            sql.contains("'happy'") && sql.contains("'sad'") && sql.contains("'angry'"),
            "must list all values: {sql}"
        );
    }

    /// `generate_create_enum_type_sql` must escape single quotes in values.
    #[test]
    fn test_generate_create_enum_type_sql_escapes_single_quotes() {
        let importer = make_importer("postgresql");
        let enum_def = EnumDefinition {
            name: "tricky".to_string(),
            schema: None,
            values: vec!["it's fine".to_string()],
        };
        let sql = importer.generate_create_enum_type_sql(&enum_def);
        assert!(
            sql.contains("it''s fine"),
            "single quotes in enum values must be doubled for SQL safety: {sql}"
        );
    }

    /// MySQL→PG: anonymous enum columns must receive a synthesized type name.
    #[test]
    fn test_resolve_enum_types_synthesizes_name_for_anonymous_mysql_enum() {
        let importer = make_importer("postgresql");
        let mut doc = UdifDocument::new(crate::document::SourceInfo::new("mysql"));

        let mut table = TableDefinition::new("orders");
        table.add_column(make_enum_col("orders", "status", &["pending", "shipped"], None));
        doc.add_table(table);

        let (enum_map, synthesized) = importer.resolve_enum_types(&doc);

        let synthesized_name = synthesized.get(&("orders".to_string(), "status".to_string()));
        assert!(
            synthesized_name.is_some(),
            "anonymous enum column must get a synthesized type name"
        );
        let type_name = synthesized_name.unwrap();
        assert!(
            enum_map.contains_key(type_name),
            "synthesized type '{}' must appear in the enum map",
            type_name
        );
        assert!(
            enum_map[type_name].values.contains(&"pending".to_string()),
            "synthesized enum must carry original values"
        );
    }

    /// On SQLite/MySQL targets `resolve_enum_types` must not synthesize names
    /// since those drivers handle anonymous enums natively or inline.
    #[test]
    fn test_resolve_enum_types_no_synthesis_on_non_pg_targets() {
        for driver in &["sqlite", "mysql"] {
            let importer = make_importer(driver);
            let mut doc = UdifDocument::new(crate::document::SourceInfo::new("mysql"));

            let mut table = TableDefinition::new("orders");
            table.add_column(make_enum_col("orders", "status", &["a", "b"], None));
            doc.add_table(table);

            let (_enum_map, synthesized) = importer.resolve_enum_types(&doc);
            assert!(
                synthesized.is_empty(),
                "driver '{}' must not synthesize enum names (only PG does): {:?}",
                driver,
                synthesized
            );
        }
    }

    /// Named enum columns on PostgreSQL targets must have no enum degradation warning.
    #[test]
    fn test_enum_column_warnings_pg_named_no_warning() {
        let importer = make_importer("postgresql");
        let col = make_enum_col("people", "mood", &["happy", "sad"], Some("mood"));
        let warnings = importer.generate_enum_column_warnings("people", &col);
        assert!(
            warnings.is_empty(),
            "named enum on PG target should produce no degradation warning"
        );
    }

    /// Named enum columns targeting SQLite must produce a degradation warning.
    #[test]
    fn test_enum_column_warnings_sqlite_emits_warning() {
        let importer = make_importer("sqlite");
        let col = make_enum_col("people", "mood", &["happy", "sad"], Some("mood"));
        let warnings = importer.generate_enum_column_warnings("people", &col);
        assert!(
            !warnings.is_empty(),
            "named enum imported to SQLite must produce a degradation warning"
        );
        assert!(
            warnings[0].message.contains("TEXT"),
            "warning must mention TEXT conversion: {}",
            warnings[0].message
        );
    }

    /// Named PostgreSQL enum targeting MySQL must produce a degradation warning.
    #[test]
    fn test_enum_column_warnings_mysql_from_named_pg_enum() {
        let importer = make_importer("mysql");
        let col = make_enum_col("people", "mood", &["happy", "sad"], Some("mood"));
        let warnings = importer.generate_enum_column_warnings("people", &col);
        assert!(
            !warnings.is_empty(),
            "named PG enum targeting MySQL must produce a degradation warning"
        );
        assert!(
            warnings[0].message.to_lowercase().contains("inline") || warnings[0].message.to_lowercase().contains("named"),
            "warning must mention named-type loss: {}",
            warnings[0].message
        );
    }

    /// `DegradationCategory::Enum` must return "Enum Type" from `display_name`.
    #[test]
    fn test_degradation_category_enum_display_name() {
        assert_eq!(
            DegradationCategory::Enum.display_name(),
            "Enum Type",
            "Enum category display name must be 'Enum Type'"
        );
    }
}
