//! Schema introspection traits and types

use crate::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Schema introspection interface
#[async_trait]
pub trait SchemaIntrospection: Send + Sync {
    /// List all databases
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>>;

    /// List all schemas in the current database
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>>;

    /// List all tables in a schema
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>>;

    /// List all views in a schema
    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>>;

    /// List materialized views in a schema.
    /// Default returns an empty list since not all databases support them.
    async fn list_materialized_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        Ok(Vec::new())
    }

    /// Get detailed table information
    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails>;

    /// Get columns for a table
    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>>;

    /// Get indexes for a table
    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>>;

    /// Get foreign keys for a table
    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>>;

    /// Get primary key for a table
    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>>;

    /// Get constraints for a table
    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>>;

    /// List all functions in a schema
    async fn list_functions(&self, schema: Option<&str>) -> Result<Vec<FunctionInfo>>;

    /// List all procedures in a schema
    async fn list_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>>;

    /// List all triggers in a schema (optionally filtered by table)
    async fn list_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>>;

    /// List all sequences in a schema
    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>>;

    /// List all custom types/enums in a schema
    async fn list_types(&self, schema: Option<&str>) -> Result<Vec<TypeInfo>>;

    /// Generate DDL for a database object
    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String>;

    /// Get object dependencies
    async fn get_dependencies(&self, object: &DatabaseObject) -> Result<Vec<Dependency>>;

    /// Extended table listing for the objects panel.
    ///
    /// Returns driver-specific column definitions and row data so each database
    /// engine can surface its own metadata (e.g. PostgreSQL shows OID, Owner, ACL
    /// while SQLite shows simple counts). Drivers that don't override this get a
    /// reasonable default built from `list_tables()`.
    async fn list_tables_extended(&self, schema: Option<&str>) -> Result<ObjectsPanelData> {
        let tables = self.list_tables(schema).await?;
        Ok(ObjectsPanelData::from_table_infos(tables))
    }
}

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
    View,
    MaterializedView,
    ForeignTable,
    Temporary,
    System,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

/// Foreign key reference (for column info)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyRef {
    pub table: String,
    pub column: String,
    pub constraint_name: String,
}

/// Index information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    pub index_type: String,
    pub comment: Option<String>,
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

/// Key-Value store specific metadata (for Redis, Memcached, Valkey, etc.)
///
/// This extends `TableInfo` for key-value databases where each "table" is actually
/// a key with its value, type, size, and TTL information.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KeyValueInfo {
    /// The type of the key (string, hash, list, set, zset, stream, etc.)
    pub key_type: String,
    /// Preview of the value (truncated for display)
    pub value_preview: Option<String>,
    /// Size in bytes (if available)
    pub size_bytes: Option<i64>,
    /// Time-to-live in seconds (-1 for no expiry, -2 for key not found)
    pub ttl_seconds: Option<i64>,
}

impl KeyValueInfo {
    /// Create new key-value info
    pub fn new(key_type: impl Into<String>) -> Self {
        Self {
            key_type: key_type.into(),
            value_preview: None,
            size_bytes: None,
            ttl_seconds: None,
        }
    }

    /// Set value preview
    pub fn with_value_preview(mut self, preview: impl Into<String>) -> Self {
        self.value_preview = Some(preview.into());
        self
    }

    /// Set size in bytes
    pub fn with_size(mut self, size: i64) -> Self {
        self.size_bytes = Some(size);
        self
    }

    /// Set TTL in seconds
    pub fn with_ttl(mut self, ttl: i64) -> Self {
        self.ttl_seconds = Some(ttl);
        self
    }

    /// Format TTL for display
    pub fn format_ttl(&self) -> String {
        match self.ttl_seconds {
            None => "Unknown".to_string(),
            Some(-1) => "No TTL".to_string(),
            Some(-2) => "Key not found".to_string(),
            Some(ttl) if ttl < 60 => format!("{}s", ttl),
            Some(ttl) if ttl < 3600 => format!("{}m {}s", ttl / 60, ttl % 60),
            Some(ttl) if ttl < 86400 => format!("{}h {}m", ttl / 3600, (ttl % 3600) / 60),
            Some(ttl) => format!("{}d {}h", ttl / 86400, (ttl % 86400) / 3600),
        }
    }

    /// Format size for display
    pub fn format_size(&self) -> String {
        match self.size_bytes {
            None => "-".to_string(),
            Some(size) if size < 1024 => format!("{} B", size),
            Some(size) if size < 1024 * 1024 => format!("{:.1} KB", size as f64 / 1024.0),
            Some(size) => format!("{:.1} MB", size as f64 / (1024.0 * 1024.0)),
        }
    }
}

/// Column alignment for objects panel display
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ObjectsPanelColumnAlignment {
    #[default]
    Left,
    Right,
}

/// Column definition for the objects panel, provided by each driver
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPanelColumn {
    /// Unique column identifier (used for sorting, lookup)
    pub id: String,
    /// Display title shown in the column header
    pub title: String,
    /// Default width in pixels
    pub width: f32,
    /// Minimum width in pixels
    pub min_width: f32,
    /// Whether the column can be resized by the user
    pub resizable: bool,
    /// Whether the column is sortable
    pub sortable: bool,
    /// Text alignment
    pub alignment: ObjectsPanelColumnAlignment,
}

impl ObjectsPanelColumn {
    pub fn new(id: impl Into<String>, title: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            width: 100.0,
            min_width: 50.0,
            resizable: true,
            sortable: false,
            alignment: ObjectsPanelColumnAlignment::Left,
        }
    }

    pub fn width(mut self, width: f32) -> Self {
        self.width = width;
        self
    }

    pub fn min_width(mut self, min_width: f32) -> Self {
        self.min_width = min_width;
        self
    }

    pub fn resizable(mut self, resizable: bool) -> Self {
        self.resizable = resizable;
        self
    }

    pub fn sortable(mut self) -> Self {
        self.sortable = true;
        self
    }

    pub fn text_right(mut self) -> Self {
        self.alignment = ObjectsPanelColumnAlignment::Right;
        self
    }
}

/// A single row in the objects panel, holding both identity info and display values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPanelRow {
    /// Object name (used for context menus, double-click actions)
    pub name: String,
    /// Object type: "table", "view", "key", "redis_database", etc.
    pub object_type: String,
    /// Cell values keyed by column id, in display-ready string form
    pub values: std::collections::BTreeMap<String, String>,
    /// Redis database index (only for "redis_database" objects)
    pub redis_database_index: Option<u16>,
    /// Key-value specific metadata (only for key-value stores)
    pub key_value_info: Option<KeyValueInfo>,
}

/// Complete dataset for the objects panel, fully driver-defined
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPanelData {
    /// Column definitions (order determines display order)
    pub columns: Vec<ObjectsPanelColumn>,
    /// Row data
    pub rows: Vec<ObjectsPanelRow>,
}

impl ObjectsPanelData {
    pub fn new(columns: Vec<ObjectsPanelColumn>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    /// Build from basic `TableInfo` list with a standard relational column set.
    /// Used as the default fallback for drivers that don't override `list_tables_extended`.
    pub fn from_table_infos(table_infos: Vec<TableInfo>) -> Self {
        let columns = vec![
            ObjectsPanelColumn::new("name", "Name")
                .width(400.0)
                .min_width(150.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("row_count", "Rows")
                .width(80.0)
                .min_width(50.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("index_count", "Indexes")
                .width(80.0)
                .min_width(60.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("trigger_count", "Triggers")
                .width(80.0)
                .min_width(60.0)
                .resizable(true)
                .sortable()
                .text_right(),
        ];

        let rows = table_infos
            .into_iter()
            .map(|info| {
                let mut values = std::collections::BTreeMap::new();
                values.insert("name".to_string(), info.name.clone());
                values.insert(
                    "row_count".to_string(),
                    info.row_count
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );
                values.insert(
                    "index_count".to_string(),
                    info.index_count
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );
                values.insert(
                    "trigger_count".to_string(),
                    info.trigger_count
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );

                let object_type = match info.table_type {
                    TableType::View | TableType::MaterializedView => "view",
                    _ => "table",
                };

                ObjectsPanelRow {
                    name: info.name,
                    object_type: object_type.to_string(),
                    values,
                    redis_database_index: None,
                    key_value_info: info.key_value_info,
                }
            })
            .collect();

        Self { columns, rows }
    }
}

/// Database driver category for determining UI behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DriverCategory {
    /// Traditional SQL databases (PostgreSQL, MySQL, SQLite, etc.)
    #[default]
    Relational,
    /// Key-Value stores (Redis, Memcached, Valkey, etc.)
    KeyValue,
    /// Document databases (MongoDB, CouchDB, etc.)
    Document,
    /// Time-series databases (InfluxDB, TimescaleDB, etc.)
    TimeSeries,
    /// Graph databases (Neo4j, etc.)
    Graph,
    /// Search engines (Elasticsearch, etc.)
    Search,
}
