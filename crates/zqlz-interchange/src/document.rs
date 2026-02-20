//! UDIF Document Structure
//!
//! This module defines the JSON-serializable document format for data interchange.
//! A UDIF document contains schema metadata and data that can be transferred between
//! any database systems.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;

use crate::canonical_types::CanonicalType;
use crate::value_encoding::EncodedValue;

/// Error returned when loading a UDIF document with an incompatible version.
#[derive(Debug)]
pub enum UdifVersionError {
    /// The document was created by a newer version of ZQLZ that this reader does not
    /// understand. The caller should tell the user to upgrade.
    TooNew {
        document_version: String,
        max_supported: String,
    },
    /// The version field is missing from the document. All UDIF documents must include
    /// a version field so the reader can check forward compatibility.
    MissingVersion,
    /// The JSON is not valid or the document is structurally invalid.
    ParseError(serde_json::Error),
}

impl std::fmt::Display for UdifVersionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooNew { document_version, max_supported } => write!(
                f,
                "UDIF document version '{document_version}' was created by a newer version of \
                 ZQLZ (maximum supported: '{max_supported}'). Please upgrade ZQLZ to open this file."
            ),
            Self::MissingVersion => write!(
                f,
                "UDIF document is missing the required 'version' field. \
                 The file may be corrupt or not a UDIF document."
            ),
            Self::ParseError(e) => write!(f, "Failed to parse UDIF document: {e}"),
        }
    }
}

impl std::error::Error for UdifVersionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ParseError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for UdifVersionError {
    fn from(e: serde_json::Error) -> Self {
        Self::ParseError(e)
    }
}

/// Thin wrapper used solely to extract the version field before full deserialization.
/// Keeping this private prevents callers from accidentally relying on it for anything else.
#[derive(Deserialize)]
struct VersionProbe {
    version: Option<String>,
}

///
/// This is the top-level structure for import/export. It contains:
/// - Metadata about the export (version, timestamp, source)
/// - Schema definitions (tables, columns, constraints)
/// - Encoded data rows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdifDocument {
    /// Document format version (for forward compatibility)
    pub version: String,
    /// When this export was created
    pub exported_at: DateTime<Utc>,
    /// Information about the source database
    pub source: SourceInfo,
    /// Schema definitions
    pub schema: SchemaDefinition,
    /// Data for each table (table_name -> rows)
    pub data: HashMap<String, TableData>,
    /// Optional document metadata
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<DocumentMetadata>,
}

impl UdifDocument {
    /// Current UDIF format version
    pub const CURRENT_VERSION: &'static str = "1.0";

    /// Create a new empty document
    pub fn new(source: SourceInfo) -> Self {
        Self {
            version: Self::CURRENT_VERSION.to_string(),
            exported_at: Utc::now(),
            source,
            schema: SchemaDefinition::default(),
            data: HashMap::new(),
            metadata: None,
        }
    }

    /// Parse a UDIF document from a JSON reader with version validation.
    ///
    /// Preferred entry point over raw `serde_json::from_reader`. Rejects documents
    /// whose version is newer than `CURRENT_VERSION` so callers never silently accept
    /// fields from a format revision they do not understand.
    ///
    /// Migration functions for older-than-current versions can be added inside
    /// `apply_migration` as the format evolves — today the only known version is
    /// "1.0" so no migration is needed.
    pub fn from_reader_with_migration<R: Read>(reader: R) -> Result<Self, UdifVersionError> {
        // Buffer the full input so we can parse the version probe and then the full
        // document from the same bytes without re-reading from the reader.
        let mut raw = Vec::new();
        let mut reader = reader;
        reader
            .read_to_end(&mut raw)
            .map_err(|e| UdifVersionError::ParseError(serde_json::Error::io(e)))?;
        Self::from_slice_with_migration(&raw)
    }

    /// Parse a UDIF document from a JSON byte slice with version validation.
    pub fn from_slice_with_migration(data: &[u8]) -> Result<Self, UdifVersionError> {
        let probe: VersionProbe = serde_json::from_slice(data)?;
        let version = probe.version.ok_or(UdifVersionError::MissingVersion)?;
        Self::check_version(&version)?;

        let mut doc: Self = serde_json::from_slice(data)?;
        Self::apply_migration(&mut doc, &version);
        Ok(doc)
    }

    /// Parse a UDIF document from a JSON string with version validation.
    pub fn from_str_with_migration(json: &str) -> Result<Self, UdifVersionError> {
        Self::from_slice_with_migration(json.as_bytes())
    }

    /// Reject any version that is numerically newer than `CURRENT_VERSION`.
    ///
    /// Version strings follow "MAJOR.MINOR" semver-lite. A document is "too new"
    /// when its major version exceeds ours, or its major equals ours and minor
    /// exceeds ours. Older-than-current documents pass through to `apply_migration`.
    fn check_version(version: &str) -> Result<(), UdifVersionError> {
        fn parse_version(v: &str) -> Option<(u64, u64)> {
            let mut parts = v.splitn(2, '.');
            let major: u64 = parts.next()?.parse().ok()?;
            let minor: u64 = parts.next().unwrap_or("0").parse().ok()?;
            Some((major, minor))
        }

        let (doc_major, doc_minor) = parse_version(version).unwrap_or((u64::MAX, u64::MAX));
        let (our_major, our_minor) = parse_version(Self::CURRENT_VERSION).unwrap_or((1, 0));

        if doc_major > our_major || (doc_major == our_major && doc_minor > our_minor) {
            return Err(UdifVersionError::TooNew {
                document_version: version.to_owned(),
                max_supported: Self::CURRENT_VERSION.to_owned(),
            });
        }

        Ok(())
    }

    /// Apply any forward-migration transformations needed to bring an older document up
    /// to the current schema.
    ///
    /// Today there is only one known version ("1.0") and no migration is necessary.
    /// This function exists as an extension point so future format changes can be
    /// handled here without touching the call sites.
    fn apply_migration(_doc: &mut Self, _from_version: &str) {
        // No migrations needed for version 1.0 → 1.0.
    }

    pub fn add_table(&mut self, table: TableDefinition) {
        let name = table.name.clone();
        self.schema.tables.insert(name.clone(), table);
        self.data.entry(name).or_insert_with(TableData::default);
    }

    /// Add rows to a table
    pub fn add_rows(&mut self, table_name: &str, rows: Vec<EncodedRow>) {
        if let Some(table_data) = self.data.get_mut(table_name) {
            table_data.rows.extend(rows);
        }
    }

    /// Get total row count across all tables
    pub fn total_rows(&self) -> usize {
        self.data.values().map(|t| t.rows.len()).sum()
    }

    /// Get table names
    pub fn table_names(&self) -> Vec<&str> {
        self.schema.tables.keys().map(|s| s.as_str()).collect()
    }
}

/// Information about the source database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    /// Database driver/type (e.g., "postgresql", "mysql", "sqlite", "mongodb")
    pub driver: String,
    /// Database server version (if known)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Database name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// Schema name (for databases that support schemas)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Character set / encoding
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
    /// Collation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
}

impl SourceInfo {
    pub fn new(driver: impl Into<String>) -> Self {
        Self {
            driver: driver.into(),
            version: None,
            database: None,
            schema: None,
            charset: None,
            collation: None,
        }
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }
}

/// Schema definitions for the exported data
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaDefinition {
    /// Table definitions (table_name -> definition)
    pub tables: HashMap<String, TableDefinition>,
    /// Enum type definitions (for databases that support named enums)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub enums: HashMap<String, EnumDefinition>,
    /// Custom type definitions
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub custom_types: HashMap<String, CustomTypeDefinition>,
    /// PostgreSQL sequences and equivalent auto-increment trackers for other drivers.
    /// Keyed by sequence name (PostgreSQL) or "<table>.<column>" for MySQL/SQLite.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub sequences: HashMap<String, SequenceDefinition>,
}

/// Definition of a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    /// Table name
    pub name: String,
    /// Schema name (if applicable)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Column definitions (ordered)
    pub columns: Vec<ColumnDefinition>,
    /// Primary key constraint
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<PrimaryKeyConstraint>,
    /// Foreign key constraints
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_keys: Vec<ForeignKeyConstraint>,
    /// Unique constraints
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unique_constraints: Vec<UniqueConstraint>,
    /// Check constraints
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub check_constraints: Vec<CheckConstraint>,
    /// Indexes
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<IndexDefinition>,
    /// Table comment
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Driver-specific table storage options (e.g. MySQL ENGINE, CHARSET, COLLATE).
    /// These are preserved for round-trip fidelity reporting but are not applied
    /// when importing into a different driver — the degradation report covers them.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub storage_options: HashMap<String, String>,
}

impl TableDefinition {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            schema: None,
            columns: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            indexes: Vec::new(),
            comment: None,
            storage_options: HashMap::new(),
        }
    }

    pub fn add_column(&mut self, column: ColumnDefinition) {
        self.columns.push(column);
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

/// Definition of a column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,
    /// Canonical type
    pub canonical_type: CanonicalType,
    /// Original native type from source database
    pub native_type: String,
    /// Whether NULL is allowed
    pub nullable: bool,
    /// Default value expression
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<DefaultValue>,
    /// Whether this is an auto-increment/serial column
    #[serde(default)]
    pub auto_increment: bool,
    /// Column comment
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Collation (for string columns)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    /// Character set (for string columns)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
    /// Expression for GENERATED ALWAYS AS columns.
    /// When set, the column value is computed by the database engine and must be
    /// excluded from INSERT statements. The expression is stored verbatim from the
    /// source database — cross-driver imports require expression review.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation_expression: Option<String>,
    /// Whether a generated column is STORED (materialised on disk) vs VIRTUAL (recomputed on read).
    /// Only meaningful when `generation_expression` is `Some`.
    /// PostgreSQL only supports STORED; MySQL and SQLite support both.
    #[serde(default)]
    pub is_generated_stored: bool,
}

impl ColumnDefinition {
    pub fn new(
        name: impl Into<String>,
        canonical_type: CanonicalType,
        native_type: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            canonical_type,
            native_type: native_type.into(),
            nullable: true,
            default_value: None,
            auto_increment: false,
            comment: None,
            collation: None,
            charset: None,
            generation_expression: None,
            is_generated_stored: false,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn with_default(mut self, default: DefaultValue) -> Self {
        self.default_value = Some(default);
        self
    }

    pub fn auto_increment(mut self) -> Self {
        self.auto_increment = true;
        self
    }

    /// Mark this column as a GENERATED ALWAYS AS column.
    pub fn generated(mut self, expression: impl Into<String>, stored: bool) -> Self {
        self.generation_expression = Some(expression.into());
        self.is_generated_stored = stored;
        self
    }

    /// Returns true when the database engine assigns this column's value automatically.
    ///
    /// Columns that are auto-increment (`serial`, `AUTO_INCREMENT`), whose canonical
    /// type is one of the Serial family, or that have a GENERATED ALWAYS AS expression
    /// must be excluded from INSERT column lists.
    /// Including them causes duplicate-key errors once the DB sequence catches up to
    /// the explicitly supplied values, or an outright rejection by the driver.
    pub fn is_db_generated(&self) -> bool {
        if self.auto_increment || self.generation_expression.is_some() {
            return true;
        }
        matches!(
            self.canonical_type,
            crate::CanonicalType::Serial
                | crate::CanonicalType::SmallSerial
                | crate::CanonicalType::BigSerial
        )
    }
}

/// Default value for a column
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum DefaultValue {
    /// Literal value
    Literal(EncodedValue),
    /// Raw expression that could not be canonicalized (driver-specific; emitted as-is)
    Expression(String),
    /// NULL default
    Null,
    /// Auto-generated (serial, UUID, etc.)
    AutoGenerated,
    /// `CURRENT_TIMESTAMP` / `now()` / `datetime('now')` — semantically "current datetime"
    CurrentTimestamp,
    /// `CURRENT_DATE` / `curdate()` — semantically "today's date"
    CurrentDate,
    /// `CURRENT_TIME` / `curtime()` — semantically "current time of day"
    CurrentTime,
    /// `CURRENT_USER` / `user()` — semantically "current database user"
    CurrentUser,
    /// UUID generation — `gen_random_uuid()`, `uuid()`, `newid()`, etc.
    GeneratedUuid,
}

/// Primary key constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryKeyConstraint {
    /// Constraint name (if named)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Column names in the primary key
    pub columns: Vec<String>,
}

/// Foreign key constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    /// Constraint name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Columns in this table
    pub columns: Vec<String>,
    /// Referenced table
    pub referenced_table: String,
    /// Referenced schema (if different)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub referenced_schema: Option<String>,
    /// Referenced columns
    pub referenced_columns: Vec<String>,
    /// ON DELETE action
    #[serde(default)]
    pub on_delete: ForeignKeyAction,
    /// ON UPDATE action
    #[serde(default)]
    pub on_update: ForeignKeyAction,
    /// Whether the constraint is deferrable (PostgreSQL DEFERRABLE clause).
    /// When false the constraint is always IMMEDIATE. MySQL and SQLite do not support
    /// deferrable FKs — the degradation report (ic-034) will note the loss.
    #[serde(default)]
    pub is_deferrable: bool,
    /// Whether a deferrable constraint starts as DEFERRED rather than IMMEDIATE.
    /// Only meaningful when `is_deferrable` is true.
    #[serde(default)]
    pub initially_deferred: bool,
}

/// Foreign key referential action
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ForeignKeyAction {
    #[default]
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

/// Unique constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueConstraint {
    /// Constraint name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Column names
    pub columns: Vec<String>,
}

/// Check constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckConstraint {
    /// Constraint name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Check expression
    pub expression: String,
}

/// Index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// Index name
    pub name: String,
    /// Column names (with optional sort direction)
    pub columns: Vec<IndexColumn>,
    /// Whether this is a unique index
    #[serde(default)]
    pub unique: bool,
    /// Canonical index access method.
    /// Prefer this over `index_type_raw` when the method is a known variant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_method: Option<IndexMethod>,
    /// Raw index type string for methods not covered by `IndexMethod` (e.g. driver extensions).
    /// Kept alongside `index_method` so truly unknown methods are preserved without data loss.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_type_raw: Option<String>,
    /// Partial index condition (PostgreSQL / SQLite WHERE clause)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<String>,
    /// Non-key columns included in a covering index (PostgreSQL INCLUDE clause).
    /// Empty on drivers that do not support covering indexes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include_columns: Vec<String>,
}

/// Column in an index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexColumn {
    /// Column name or expression
    pub column: String,
    /// Sort order
    #[serde(default)]
    pub order: SortOrder,
    /// Nulls position
    #[serde(default)]
    pub nulls: NullsOrder,
}

/// Sort order for index columns
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// Nulls ordering for index columns
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NullsOrder {
    #[default]
    Default,
    First,
    Last,
}

/// Canonical index access method.
///
/// Using an enum instead of a raw string allows cross-database translation logic to
/// match on a known variant rather than string-compare "gin" vs "GIN" vs "GIN_INDEX".
/// Unknown or driver-extension methods are stored in `IndexDefinition::index_type_raw`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexMethod {
    Btree,
    Hash,
    Gin,
    Gist,
    SpGist,
    Brin,
    Fulltext,
    Spatial,
}

/// Sequence / auto-increment counter exported from the source database.
///
/// PostgreSQL sequences are first-class objects; MySQL and SQLite expose an
/// equivalent high-water mark through their own mechanisms. Capturing the
/// current value enables the importer to advance the target sequence after
/// bulk-loading data so new inserts do not collide with imported row IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceDefinition {
    /// Sequence name (PostgreSQL) or "<table>.<column>" for MySQL/SQLite
    pub name: String,
    /// First value the sequence can produce
    #[serde(default = "default_sequence_start")]
    pub start_value: i64,
    /// Step between successive values
    #[serde(default = "default_sequence_increment")]
    pub increment: i64,
    /// Minimum allowed value
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_value: Option<i64>,
    /// Maximum allowed value
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_value: Option<i64>,
    /// Last value produced at export time (None if no row has been inserted yet)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_value: Option<i64>,
    /// Whether the sequence wraps around when it hits the boundary
    #[serde(default)]
    pub cycle: bool,
}

fn default_sequence_start() -> i64 {
    1
}

fn default_sequence_increment() -> i64 {
    1
}

/// Named enum type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnumDefinition {
    /// Enum name
    pub name: String,
    /// Schema (if applicable)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Allowed values
    pub values: Vec<String>,
}

/// Custom type definition (for database-specific types)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomTypeDefinition {
    /// Type name
    pub name: String,
    /// Base type it extends
    pub base_type: Option<String>,
    /// Type definition (CREATE TYPE statement or similar)
    pub definition: String,
}

/// Data for a single table
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableData {
    /// Rows of data
    pub rows: Vec<EncodedRow>,
    /// Whether this is a partial export (filtered or limited)
    #[serde(default)]
    pub partial: bool,
    /// Total row count in source (if known and partial)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_count: Option<u64>,
    /// Filter condition used (if partial)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

/// A single row of encoded data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncodedRow {
    /// Values in column order
    pub values: Vec<EncodedValue>,
}

impl EncodedRow {
    pub fn new(values: Vec<EncodedValue>) -> Self {
        Self { values }
    }
}

/// Optional document metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMetadata {
    /// Export tool name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    /// Export tool version
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_version: Option<String>,
    /// User-provided description
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Custom tags
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub tags: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A document with the current version must parse without error.
    #[test]
    fn test_from_str_with_migration_current_version_succeeds() {
        let json = r#"{
            "version": "1.0",
            "exported_at": "2026-01-01T00:00:00Z",
            "source": { "driver": "postgresql" },
            "schema": { "tables": {} },
            "data": {}
        }"#;

        let result = UdifDocument::from_str_with_migration(json);
        assert!(result.is_ok(), "version 1.0 must succeed: {:?}", result);
    }

    /// A document whose version is newer than CURRENT_VERSION must return TooNew.
    #[test]
    fn test_from_str_with_migration_future_version_returns_error() {
        let json = r#"{
            "version": "99.0",
            "exported_at": "2026-01-01T00:00:00Z",
            "source": { "driver": "postgresql" },
            "schema": { "tables": {} },
            "data": {}
        }"#;

        match UdifDocument::from_str_with_migration(json) {
            Err(UdifVersionError::TooNew {
                document_version, ..
            }) => {
                assert_eq!(document_version, "99.0");
            }
            other => panic!("expected TooNew, got {:?}", other),
        }
    }

    /// A document whose minor version is newer (same major) must also return TooNew.
    #[test]
    fn test_from_str_with_migration_future_minor_version_returns_error() {
        let json = r#"{
            "version": "1.999",
            "exported_at": "2026-01-01T00:00:00Z",
            "source": { "driver": "postgresql" },
            "schema": { "tables": {} },
            "data": {}
        }"#;

        assert!(matches!(
            UdifDocument::from_str_with_migration(json),
            Err(UdifVersionError::TooNew { .. })
        ));
    }

    /// A document missing the version field entirely must return MissingVersion.
    #[test]
    fn test_from_str_with_migration_missing_version_returns_error() {
        let json = r#"{
            "exported_at": "2026-01-01T00:00:00Z",
            "source": { "driver": "postgresql" },
            "schema": { "tables": {} },
            "data": {}
        }"#;

        assert!(matches!(
            UdifDocument::from_str_with_migration(json),
            Err(UdifVersionError::MissingVersion)
        ));
    }

    /// Completely invalid JSON must return ParseError, not panic.
    #[test]
    fn test_from_str_with_migration_invalid_json_returns_parse_error() {
        assert!(matches!(
            UdifDocument::from_str_with_migration("not valid json {{{"),
            Err(UdifVersionError::ParseError(_))
        ));
    }

    #[test]
    fn test_document_creation() {
        let source = SourceInfo::new("postgresql")
            .with_database("mydb")
            .with_schema("public");

        let mut doc = UdifDocument::new(source);

        let mut table = TableDefinition::new("users");
        table.add_column(
            ColumnDefinition::new("id", CanonicalType::Serial, "serial")
                .not_null()
                .auto_increment(),
        );
        table.add_column(
            ColumnDefinition::new(
                "name",
                CanonicalType::String {
                    max_length: Some(255),
                    fixed_length: false,
                },
                "varchar(255)",
            )
            .not_null(),
        );
        table.add_column(ColumnDefinition::new(
            "email",
            CanonicalType::String {
                max_length: Some(255),
                fixed_length: false,
            },
            "varchar(255)",
        ));

        table.primary_key = Some(PrimaryKeyConstraint {
            name: Some("users_pkey".into()),
            columns: vec!["id".into()],
        });

        doc.add_table(table);

        assert_eq!(doc.table_names(), vec!["users"]);
        assert_eq!(doc.version, UdifDocument::CURRENT_VERSION);
    }

    #[test]
    fn test_serialization() {
        let source = SourceInfo::new("sqlite").with_database("test.db");
        let doc = UdifDocument::new(source);

        let json = serde_json::to_string_pretty(&doc).expect("serialize");
        let parsed: UdifDocument = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(parsed.version, doc.version);
        assert_eq!(parsed.source.driver, "sqlite");
    }

    /// A UDIF 1.0 document without the new optional fields must deserialise
    /// without error and produce the correct defaults for every new field.
    #[test]
    fn test_legacy_document_deserialises_with_defaults() {
        // Minimal UDIF 1.0 document — none of the ic-026 fields are present.
        let json = r#"{
            "version": "1.0",
            "exported_at": "2026-01-01T00:00:00Z",
            "source": { "driver": "postgresql" },
            "schema": {
                "tables": {
                    "orders": {
                        "name": "orders",
                        "columns": [
                            {
                                "name": "id",
                                "canonical_type": { "type": "Serial" },
                                "native_type": "serial",
                                "nullable": false
                            }
                        ]
                    }
                }
            },
            "data": {}
        }"#;

        let doc: UdifDocument = serde_json::from_str(json).expect("must deserialise");
        let table = &doc.schema.tables["orders"];

        // New table-level field defaults
        assert!(
            table.storage_options.is_empty(),
            "storage_options must default to empty"
        );

        // New column-level field defaults
        let col = &table.columns[0];
        assert!(
            col.generation_expression.is_none(),
            "generation_expression must default to None"
        );
        assert!(
            !col.is_generated_stored,
            "is_generated_stored must default to false"
        );

        // New schema-level field defaults
        assert!(
            doc.schema.sequences.is_empty(),
            "sequences must default to empty"
        );
    }

    /// A generated column must round-trip through serialise→deserialise
    /// preserving both the expression and the stored flag.
    #[test]
    fn test_generated_column_round_trips() {
        let mut col = ColumnDefinition::new(
            "full_name",
            CanonicalType::String {
                max_length: None,
                fixed_length: false,
            },
            "text",
        );
        col.generation_expression = Some("first_name || ' ' || last_name".into());
        col.is_generated_stored = true;

        let json = serde_json::to_string(&col).expect("serialize");
        let parsed: ColumnDefinition = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(
            parsed.generation_expression.as_deref(),
            Some("first_name || ' ' || last_name")
        );
        assert!(parsed.is_generated_stored);
    }

    /// IndexDefinition with index_method and include_columns must round-trip.
    #[test]
    fn test_index_with_method_and_include_columns_round_trips() {
        let index = IndexDefinition {
            name: "idx_orders_customer_covering".into(),
            columns: vec![IndexColumn {
                column: "customer_id".into(),
                order: SortOrder::Asc,
                nulls: NullsOrder::Default,
            }],
            unique: false,
            index_method: Some(IndexMethod::Btree),
            index_type_raw: None,
            where_clause: None,
            include_columns: vec!["total".into(), "status".into()],
        };

        let json = serde_json::to_string(&index).expect("serialize");
        let parsed: IndexDefinition = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(parsed.index_method, Some(IndexMethod::Btree));
        assert_eq!(parsed.include_columns, vec!["total", "status"]);
    }

    /// GIN index method serialises as the snake_case string "gin".
    #[test]
    fn test_index_method_gin_serialises_correctly() {
        let index = IndexDefinition {
            name: "idx_body_search".into(),
            columns: vec![IndexColumn {
                column: "tsv".into(),
                order: SortOrder::Asc,
                nulls: NullsOrder::Default,
            }],
            unique: false,
            index_method: Some(IndexMethod::Gin),
            index_type_raw: None,
            where_clause: None,
            include_columns: vec!["tsv".into()],
        };

        let json = serde_json::to_string(&index).expect("serialize");
        assert!(
            json.contains("\"gin\""),
            "IndexMethod::Gin must serialise as \"gin\", got: {json}"
        );

        let parsed: IndexDefinition = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.index_method, Some(IndexMethod::Gin));
        assert_eq!(parsed.include_columns, vec!["tsv"]);
    }

    /// SequenceDefinition must round-trip, including the optional current_value.
    #[test]
    fn test_sequence_definition_round_trips() {
        let seq = SequenceDefinition {
            name: "users_id_seq".into(),
            start_value: 1,
            increment: 1,
            min_value: Some(1),
            max_value: Some(9_223_372_036_854_775_807),
            current_value: Some(500),
            cycle: false,
        };

        let json = serde_json::to_string(&seq).expect("serialize");
        let parsed: SequenceDefinition = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(parsed.name, "users_id_seq");
        assert_eq!(parsed.current_value, Some(500));
        assert!(!parsed.cycle);
    }

    /// A ForeignKeyConstraint without is_deferrable / initially_deferred (legacy
    /// document) must deserialise with both fields defaulting to false.
    #[test]
    fn test_fk_deferrable_fields_default_to_false_for_legacy_documents() {
        let json = r#"{
            "columns": ["customer_id"],
            "referenced_table": "customers",
            "referenced_columns": ["id"]
        }"#;

        let fk: ForeignKeyConstraint = serde_json::from_str(json).expect("deserialize");
        assert!(!fk.is_deferrable);
        assert!(!fk.initially_deferred);
    }

    /// is_db_generated must return true for a column with a generation expression.
    #[test]
    fn test_is_db_generated_true_for_generated_column() {
        let col = ColumnDefinition::new("full_name", CanonicalType::Text, "text")
            .generated("first || last", true);
        assert!(
            col.is_db_generated(),
            "generated column must be excluded from INSERT"
        );
    }

    /// is_db_generated must return false for a plain column with no generation expression.
    #[test]
    fn test_is_db_generated_false_for_plain_column() {
        let col = ColumnDefinition::new("email", CanonicalType::Text, "text");
        assert!(!col.is_db_generated());
    }
}
