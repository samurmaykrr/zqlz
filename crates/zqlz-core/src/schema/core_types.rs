use serde::{Deserialize, Serialize};

use super::KeyValueInfo;

/// Database information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub owner: Option<String>,
    pub encoding: Option<String>,
    pub size_bytes: Option<i64>,
    pub comment: Option<String>,
}

/// Schema information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub name: String,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

/// Table information (basic)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub schema: Option<String>,
    pub name: String,
    pub table_type: TableType,
    pub owner: Option<String>,
    pub row_count: Option<i64>,
    pub size_bytes: Option<i64>,
    pub comment: Option<String>,
    pub index_count: Option<i64>,
    pub trigger_count: Option<i64>,
    /// Key-value specific metadata (for Redis, Memcached, Valkey, etc.)
    /// This is None for traditional SQL databases.
    #[serde(default)]
    pub key_value_info: Option<KeyValueInfo>,
}

/// Table type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableType {
    Table,
    VirtualTable,
    View,
    MaterializedView,
    ForeignTable,
    PartitionedTable,
    Temporary,
    System,
}

impl TableType {
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Table => "Table",
            Self::VirtualTable => "Virtual Table",
            Self::View => "View",
            Self::MaterializedView => "Materialized View",
            Self::ForeignTable => "Foreign Table",
            Self::PartitionedTable => "Partitioned Table",
            Self::Temporary => "Temporary Table",
            Self::System => "System Table",
        }
    }
}

/// Detailed table information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDetails {
    pub info: TableInfo,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Option<PrimaryKeyInfo>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub indexes: Vec<IndexInfo>,
    pub constraints: Vec<ConstraintInfo>,
    pub triggers: Vec<TriggerInfo>,
}

/// View information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewInfo {
    pub schema: Option<String>,
    pub name: String,
    pub is_materialized: bool,
    pub definition: Option<String>,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

/// Column information
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnInfo {
    pub name: String,
    pub ordinal: usize,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub max_length: Option<i64>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
    pub is_primary_key: bool,
    pub is_auto_increment: bool,
    pub is_unique: bool,
    pub foreign_key: Option<ForeignKeyRef>,
    pub comment: Option<String>,
    /// Character set for string-like columns when the driver exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,
    /// Collation for string-like columns when the driver exposes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
    /// GENERATED ALWAYS AS expression for computed columns.
    /// Populated by drivers that support generated columns (PostgreSQL 12+, MySQL 5.7+, SQLite 3.31+).
    /// None for regular columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation_expression: Option<String>,
    /// Whether a generated column is STORED (materialised on disk) rather than VIRTUAL (recomputed on read).
    /// Only meaningful when `generation_expression` is `Some`.
    #[serde(default)]
    pub is_generated_stored: bool,
    /// Enumerated labels for enum/set-like columns when the driver can resolve them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<String>>,
}

/// Foreign key reference (for column info)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyRef {
    pub table: String,
    pub column: String,
    pub constraint_name: String,
}

/// Index information
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    pub index_type: String,
    pub comment: Option<String>,
    /// Partial index predicate (PostgreSQL / SQLite WHERE clause).
    /// None for indexes that apply to all rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<String>,
    /// Non-key columns pulled into a covering index (PostgreSQL INCLUDE).
    /// Empty for drivers that do not support this feature.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include_columns: Vec<String>,
    /// Per-column sort direction — `true` means DESC, `false` (or absent) means ASC.
    /// Parallel to `columns`; drivers that do not track direction leave this empty
    /// (all columns default to ASC during export).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_descending: Vec<bool>,
}

/// Foreign key information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_schema: Option<String>,
    pub referenced_columns: Vec<String>,
    pub on_update: ForeignKeyAction,
    pub on_delete: ForeignKeyAction,
    /// Whether the constraint can be deferred within a transaction.
    /// Only PostgreSQL supports this; defaults to `false` for all other drivers.
    #[serde(default)]
    pub is_deferrable: bool,
    /// Whether the constraint starts in DEFERRED mode (only relevant when `is_deferrable`
    /// is true).
    #[serde(default)]
    pub initially_deferred: bool,
}

/// Foreign key action
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

/// Primary key information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrimaryKeyInfo {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

/// Constraint information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintInfo {
    pub name: String,
    pub constraint_type: ConstraintType,
    pub columns: Vec<String>,
    pub definition: Option<String>,
}

/// Constraint type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintType {
    PrimaryKey,
    ForeignKey,
    Unique,
    Check,
    Exclusion,
}

/// Function information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInfo {
    pub schema: Option<String>,
    pub name: String,
    pub language: String,
    pub return_type: String,
    pub parameters: Vec<ParameterInfo>,
    pub definition: Option<String>,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

/// Procedure information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcedureInfo {
    pub schema: Option<String>,
    pub name: String,
    pub language: String,
    pub parameters: Vec<ParameterInfo>,
    pub definition: Option<String>,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

/// Parameter information (for functions/procedures)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterInfo {
    pub name: Option<String>,
    pub data_type: String,
    pub mode: ParameterMode,
    pub default_value: Option<String>,
    pub ordinal: usize,
}

/// Parameter mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParameterMode {
    In,
    Out,
    InOut,
    Variadic,
}

/// Trigger information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerInfo {
    pub schema: Option<String>,
    pub name: String,
    pub table_name: String,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub for_each: TriggerForEach,
    pub definition: Option<String>,
    pub enabled: bool,
    pub comment: Option<String>,
}

/// Trigger timing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// Trigger event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Trigger for each
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerForEach {
    Row,
    Statement,
}

/// Sequence information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceInfo {
    pub schema: Option<String>,
    pub name: String,
    pub data_type: String,
    pub start_value: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub increment_by: i64,
    pub current_value: Option<i64>,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

/// Custom type information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeInfo {
    pub schema: Option<String>,
    pub name: String,
    pub type_kind: TypeKind,
    pub values: Option<Vec<String>>, // For enums
    pub definition: Option<String>,
    pub owner: Option<String>,
    pub comment: Option<String>,
}

/// Type kind
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeKind {
    Enum,
    Composite,
    Domain,
    Range,
    Base,
}

/// Database object reference (for DDL generation, dependencies)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseObject {
    pub object_type: ObjectType,
    pub schema: Option<String>,
    pub name: String,
    pub signature: Option<String>,
}

/// Object type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectType {
    Database,
    Schema,
    Table,
    View,
    MaterializedView,
    Index,
    Constraint,
    Function,
    Procedure,
    Trigger,
    Event,
    Sequence,
    Type,
}

/// Object dependency
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dependency {
    pub dependent: DatabaseObject,
    pub referenced: DatabaseObject,
    pub dependency_type: DependencyType,
}

/// Dependency type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DependencyType {
    Normal,
    Automatic,
    Internal,
}
