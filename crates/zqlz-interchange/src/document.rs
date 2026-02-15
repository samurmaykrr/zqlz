//! UDIF Document Structure
//!
//! This module defines the JSON-serializable document format for data interchange.
//! A UDIF document contains schema metadata and data that can be transferred between
//! any database systems.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::canonical_types::CanonicalType;
use crate::value_encoding::EncodedValue;

/// Universal Data Interchange Format document
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

    /// Add a table schema to the document
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
}

/// Default value for a column
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum DefaultValue {
    /// Literal value
    Literal(EncodedValue),
    /// Expression (e.g., "CURRENT_TIMESTAMP")
    Expression(String),
    /// NULL default
    Null,
    /// Auto-generated (serial, UUID, etc.)
    AutoGenerated,
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
    /// Index type (btree, hash, gin, gist, etc.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_type: Option<String>,
    /// Partial index condition
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<String>,
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
}
