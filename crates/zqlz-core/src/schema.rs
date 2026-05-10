//! Schema introspection traits and types

use crate::{DocumentCollectionInfo, Result, ZqlzError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

    /// Kind-scoped objects panel data.
    ///
    /// Drivers can override this to avoid loading every object kind when the UI
    /// only needs the active Objects Panel kind.
    async fn list_objects_panel_data_for_kind(
        &self,
        schema: Option<&str>,
        kind_id: &str,
    ) -> Result<ObjectsPanelData> {
        Ok(self
            .list_tables_extended(schema)
            .await?
            .for_kind_and_scope(kind_id, None))
    }

    /// Declarative manifest for Objects Panel behavior.
    ///
    /// Drivers can override this to describe which object kinds exist, their
    /// available actions, and UI metadata. The default derives a conservative
    /// manifest from `list_tables_extended` so existing drivers remain compatible.
    async fn list_objects_panel_manifest(
        &self,
        schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        let data = self.list_tables_extended(schema).await?;
        Ok(ObjectsPanelManifest::from_data(&data))
    }

    /// Driver-defined form for creating, editing, or dropping an object kind.
    async fn object_form_spec(
        &self,
        _request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        Ok(None)
    }

    /// Generate executable DDL from a driver-defined object form payload.
    async fn generate_object_form_ddl(
        &self,
        _request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        Err(ZqlzError::NotSupported(
            "Object form DDL generation is not supported by this driver".to_string(),
        ))
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

/// Declarative action definition for Objects Panel menus and toolbars.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPanelAction {
    /// Stable action identifier for app-layer routing (e.g. "open", "delete").
    pub id: String,
    /// Human-readable label shown in UI.
    pub label: String,
    /// Optional icon key resolved by UI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon_key: Option<String>,
    /// Optional grouping hint for menu organization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// Optional object kind created by this action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_object_kind_id: Option<String>,
    /// Whether this action refreshes the objects panel.
    #[serde(default)]
    pub refreshes_objects_panel: bool,
    /// Whether this action can only run on a single selected object.
    pub requires_single_selection: bool,
    /// Whether this action is destructive.
    pub destructive: bool,
    /// Optional object form opened by this action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_form: Option<ObjectFormAction>,
}

impl ObjectsPanelAction {
    pub fn new(id: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            icon_key: None,
            group: None,
            create_object_kind_id: None,
            refreshes_objects_panel: false,
            requires_single_selection: false,
            destructive: false,
            object_form: None,
        }
    }

    pub fn icon_key(mut self, icon_key: impl Into<String>) -> Self {
        self.icon_key = Some(icon_key.into());
        self
    }

    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    pub fn create_object_kind(mut self, kind_id: impl Into<String>) -> Self {
        self.create_object_kind_id = Some(kind_id.into());
        self
    }

    pub fn refreshes_objects_panel(mut self) -> Self {
        self.refreshes_objects_panel = true;
        self
    }

    pub fn single_selection(mut self) -> Self {
        self.requires_single_selection = true;
        self
    }

    pub fn destructive(mut self) -> Self {
        self.destructive = true;
        self
    }

    pub fn object_form(mut self, kind_id: impl Into<String>, mode: ObjectFormMode) -> Self {
        self.object_form = Some(ObjectFormAction {
            kind_id: kind_id.into(),
            mode,
        });
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormAction {
    pub kind_id: String,
    pub mode: ObjectFormMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectFormMode {
    Create,
    Edit,
    Drop,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormSpecRequest {
    pub kind_id: String,
    pub mode: ObjectFormMode,
    pub object_ref: Option<ObjectsPanelObjectRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormDdlRequest {
    pub kind_id: String,
    pub mode: ObjectFormMode,
    pub object_ref: Option<ObjectsPanelObjectRef>,
    pub values: BTreeMap<String, ObjectFormValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormSpec {
    pub kind_id: String,
    pub mode: ObjectFormMode,
    pub title: String,
    pub sections: Vec<ObjectFormSection>,
}

impl ObjectFormSpec {
    pub fn new(kind_id: impl Into<String>, mode: ObjectFormMode, title: impl Into<String>) -> Self {
        Self {
            kind_id: kind_id.into(),
            mode,
            title: title.into(),
            sections: Vec::new(),
        }
    }

    pub fn sections(mut self, sections: Vec<ObjectFormSection>) -> Self {
        self.sections = sections;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormSection {
    pub title: Option<String>,
    pub fields: Vec<ObjectFormField>,
}

impl ObjectFormSection {
    pub fn new(fields: Vec<ObjectFormField>) -> Self {
        Self {
            title: None,
            fields,
        }
    }

    pub fn titled(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormField {
    pub id: String,
    pub label: String,
    pub kind: ObjectFormFieldKind,
    pub required: bool,
    pub placeholder: Option<String>,
    pub help_text: Option<String>,
    pub default_value: ObjectFormValue,
    pub options: Vec<ObjectFormOption>,
    pub read_only: bool,
}

impl ObjectFormField {
    pub fn new(id: impl Into<String>, label: impl Into<String>, kind: ObjectFormFieldKind) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            kind,
            required: false,
            placeholder: None,
            help_text: None,
            default_value: ObjectFormValue::String(String::new()),
            options: Vec::new(),
            read_only: false,
        }
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.placeholder = Some(placeholder.into());
        self
    }

    pub fn help_text(mut self, help_text: impl Into<String>) -> Self {
        self.help_text = Some(help_text.into());
        self
    }

    pub fn default_value(mut self, value: ObjectFormValue) -> Self {
        self.default_value = value;
        self
    }

    pub fn options(mut self, options: Vec<ObjectFormOption>) -> Self {
        self.options = options;
        self
    }

    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectFormFieldKind {
    Text,
    TextArea,
    Select,
    Checkbox,
    StringList,
    KeyValueList,
    SqlExpression,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormOption {
    pub value: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectFormValue {
    String(String),
    Bool(bool),
    StringList(Vec<String>),
    KeyValueList(Vec<(String, String)>),
}

impl ObjectFormValue {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_str()),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_string_list(&self) -> Option<&[String]> {
        match self {
            Self::StringList(value) => Some(value.as_slice()),
            _ => None,
        }
    }
}

/// Declarative object-kind definition for driver-provided Objects Panel behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPanelObjectKind {
    /// Stable object kind id (e.g. "table", "view", "function").
    pub id: String,
    /// Label for one object.
    pub label_singular: String,
    /// Label for multiple objects.
    pub label_plural: String,
    /// Optional icon key resolved by UI.
    pub icon_key: Option<String>,
    /// Column layout for rows of this kind.
    pub columns: Vec<ObjectsPanelColumn>,
    /// Actions available when rows of this kind are selected.
    pub row_actions: Vec<ObjectsPanelAction>,
    /// Optional default action id used for double-click / Enter.
    pub default_row_action_id: Option<String>,
}

impl ObjectsPanelObjectKind {
    pub fn new(
        id: impl Into<String>,
        label_singular: impl Into<String>,
        label_plural: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            label_singular: label_singular.into(),
            label_plural: label_plural.into(),
            icon_key: None,
            columns: Vec::new(),
            row_actions: Vec::new(),
            default_row_action_id: None,
        }
    }

    pub fn icon_key(mut self, icon_key: impl Into<String>) -> Self {
        self.icon_key = Some(icon_key.into());
        self
    }

    pub fn columns(mut self, columns: Vec<ObjectsPanelColumn>) -> Self {
        self.columns = columns;
        self
    }

    pub fn row_actions(mut self, actions: Vec<ObjectsPanelAction>) -> Self {
        self.row_actions = actions;
        self
    }

    pub fn default_row_action(mut self, action_id: impl Into<String>) -> Self {
        self.default_row_action_id = Some(action_id.into());
        self
    }
}

/// Driver-provided manifest describing object kinds and actions for Objects Panel.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObjectsPanelManifest {
    /// Supported object kinds and their row behavior.
    pub object_kinds: Vec<ObjectsPanelObjectKind>,
    /// Optional global toolbar actions.
    pub toolbar_actions: Vec<ObjectsPanelAction>,
}

impl ObjectsPanelManifest {
    /// Builds a conservative manifest from row data for backward-compatible
    /// drivers that only provide `ObjectsPanelData`.
    pub fn from_data(data: &ObjectsPanelData) -> Self {
        let columns = if data.columns.is_empty() {
            ObjectsPanelData::from_table_infos(Vec::<TableInfo>::new()).columns
        } else {
            data.columns.clone()
        };

        let mut seen_kind_ids = std::collections::BTreeSet::new();
        let mut kinds = Vec::new();

        for row in &data.rows {
            let kind_id = row.object_kind_id().to_string();
            if !seen_kind_ids.insert(kind_id.clone()) {
                continue;
            }

            let label = kind_label_from_id(&kind_id);
            let row_actions = default_actions_for_kind(&kind_id);
            let mut kind =
                ObjectsPanelObjectKind::new(kind_id.clone(), label.clone(), format!("{}s", label))
                    .icon_key(kind_id.clone())
                    .columns(columns.clone())
                    .row_actions(row_actions.clone());

            if row_actions.iter().any(|action| action.id == "open") {
                kind = kind.default_row_action("open");
            }

            kinds.push(kind);
        }

        if kinds.is_empty() {
            kinds.push(
                ObjectsPanelObjectKind::new("table", "Table", "Tables")
                    .icon_key("table")
                    .columns(columns)
                    .row_actions(default_actions_for_kind("table"))
                    .default_row_action("open"),
            );
        }

        let has_relational_create_kinds = kinds
            .iter()
            .any(|kind| matches!(kind.id.as_str(), "table" | "view" | "materialized_view"));

        let mut toolbar_actions = vec![
            ObjectsPanelAction::new("refresh", "Refresh")
                .icon_key("refresh")
                .refreshes_objects_panel(),
        ];
        if has_relational_create_kinds {
            toolbar_actions.extend([
                ObjectsPanelAction::new("new_table", "New Table")
                    .icon_key("create")
                    .create_object_kind("table"),
                ObjectsPanelAction::new("new_view", "New View")
                    .icon_key("create")
                    .create_object_kind("view"),
                ObjectsPanelAction::new("import", "Import Wizard...").icon_key("import"),
                ObjectsPanelAction::new("export", "Export Wizard...").icon_key("export"),
            ]);
        }

        Self {
            object_kinds: kinds,
            toolbar_actions,
        }
    }

    pub fn validate(&self) -> Result<()> {
        let mut seen_kind_ids = std::collections::BTreeSet::new();

        for kind in &self.object_kinds {
            if !seen_kind_ids.insert(kind.id.as_str()) {
                return Err(ZqlzError::Schema(format!(
                    "Objects panel manifest has duplicate kind_id '{}'",
                    kind.id
                )));
            }

            let mut seen_action_ids = std::collections::BTreeSet::new();
            for action in &kind.row_actions {
                if !seen_action_ids.insert(action.id.as_str()) {
                    return Err(ZqlzError::Schema(format!(
                        "Objects panel manifest kind '{}' has duplicate action_id '{}'",
                        kind.id, action.id
                    )));
                }
            }

            if let Some(default_action_id) = kind.default_row_action_id.as_deref()
                && !kind
                    .row_actions
                    .iter()
                    .any(|action| action.id == default_action_id)
            {
                return Err(ZqlzError::Schema(format!(
                    "Objects panel manifest kind '{}' has default_row_action_id '{}' with no matching row action",
                    kind.id, default_action_id
                )));
            }
        }

        Ok(())
    }
}

/// Canonical object identity for generic Objects Panel actions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectsPanelObjectRef {
    /// Object kind id (e.g. "table", "view", "function").
    pub kind_id: String,
    /// Database/catalog scope when relevant.
    pub database: Option<String>,
    /// Schema/namespace scope when relevant.
    pub schema: Option<String>,
    /// Object name in its native catalog.
    pub name: String,
    /// Optional function/procedure signature for overloaded routines.
    pub signature: Option<String>,
    /// Stable key suitable for routing, diffing, and version history lookups.
    pub identity_key: String,
}

impl ObjectsPanelObjectRef {
    pub fn new(kind_id: impl Into<String>, name: impl Into<String>) -> Self {
        let kind_id = kind_id.into();
        let name = name.into();
        let identity_key = Self::build_identity_key(&kind_id, None, None, &name, None);

        Self {
            kind_id,
            database: None,
            schema: None,
            name,
            signature: None,
            identity_key,
        }
    }

    pub fn with_database_option(mut self, database: Option<String>) -> Self {
        self.database = database;
        self.rebuild_identity_key();
        self
    }

    pub fn with_schema_option(mut self, schema: Option<String>) -> Self {
        self.schema = schema;
        self.rebuild_identity_key();
        self
    }

    pub fn with_signature_option(mut self, signature: Option<String>) -> Self {
        self.signature = signature;
        self.rebuild_identity_key();
        self
    }

    pub fn build_identity_key(
        kind_id: &str,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        signature: Option<&str>,
    ) -> String {
        let database_part = database.unwrap_or_default();
        let schema_part = schema.unwrap_or_default();
        let signature_part = signature.unwrap_or_default();
        format!(
            "{}::{}::{}::{}::{}",
            kind_id, database_part, schema_part, name, signature_part
        )
    }

    fn rebuild_identity_key(&mut self) {
        self.identity_key = Self::build_identity_key(
            &self.kind_id,
            self.database.as_deref(),
            self.schema.as_deref(),
            &self.name,
            self.signature.as_deref(),
        );
    }
}

/// A single row in the objects panel, holding both identity info and display values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPanelRow {
    /// Object name (used for context menus, double-click actions)
    pub name: String,
    /// Optional schema/namespace for the object when the driver exposes it.
    pub schema: Option<String>,
    /// Object type: "table", "view", "key", "redis_database", etc.
    pub object_type: String,
    /// Canonical object identity used by generic action routing.
    #[serde(default)]
    pub object_ref: Option<ObjectsPanelObjectRef>,
    /// Cell values keyed by column id, in display-ready string form
    pub values: std::collections::BTreeMap<String, String>,
    /// Redis database index (only for "redis_database" objects)
    pub redis_database_index: Option<u16>,
    /// Key-value specific metadata (only for key-value stores)
    pub key_value_info: Option<KeyValueInfo>,
}

impl ObjectsPanelRow {
    pub fn object_kind_id(&self) -> &str {
        self.object_ref
            .as_ref()
            .map(|object_ref| object_ref.kind_id.as_str())
            .unwrap_or(self.object_type.as_str())
    }

    pub fn object_name(&self) -> &str {
        self.object_ref
            .as_ref()
            .map(|object_ref| object_ref.name.as_str())
            .unwrap_or(self.name.as_str())
    }

    pub fn object_schema(&self) -> Option<&str> {
        self.object_ref
            .as_ref()
            .and_then(|object_ref| object_ref.schema.as_deref())
            .or(self.schema.as_deref())
    }
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
                let schema = info.schema.clone();
                let object_name = info.name.clone();
                let mut values = std::collections::BTreeMap::new();
                values.insert("name".to_string(), object_name.clone());
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
                    schema: schema.clone(),
                    object_type: object_type.to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new(object_type, object_name)
                            .with_schema_option(schema),
                    ),
                    values,
                    redis_database_index: None,
                    key_value_info: info.key_value_info,
                }
            })
            .collect();

        Self { columns, rows }
    }

    /// Build a Redis-database-focused dataset with canonical `redis_database`
    /// object identity and key-count presentation columns.
    pub fn from_redis_databases(databases: Vec<(u16, Option<i64>)>) -> Self {
        let columns = vec![
            ObjectsPanelColumn::new("name", "Database")
                .width(300.0)
                .min_width(150.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("key_count", "Keys")
                .width(100.0)
                .min_width(60.0)
                .resizable(false)
                .sortable()
                .text_right(),
        ];

        let rows = databases
            .into_iter()
            .map(|(index, key_count)| {
                let database_name = format!("db{}", index);
                let mut values = std::collections::BTreeMap::new();
                values.insert("name".to_string(), database_name.clone());
                values.insert(
                    "key_count".to_string(),
                    key_count
                        .map(|count| count.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );

                ObjectsPanelRow {
                    name: database_name.clone(),
                    schema: None,
                    object_type: "redis_database".to_string(),
                    object_ref: Some(ObjectsPanelObjectRef::new("redis_database", database_name)),
                    values,
                    redis_database_index: Some(index),
                    key_value_info: None,
                }
            })
            .collect();

        Self { columns, rows }
    }

    /// Build the Redis objects panel data and manifest together.
    ///
    /// Redis hydration is used by both connect-time bootstrap and refresh-time
    /// panel updates, so callers should consume the same assembled pair instead
    /// of reconstructing it in separate layers.
    pub fn from_redis_databases_with_manifest(
        databases: Vec<(u16, Option<i64>)>,
    ) -> (Self, ObjectsPanelManifest) {
        let data = Self::from_redis_databases(databases);
        let manifest = ObjectsPanelManifest::from_data(&data);
        (data, manifest)
    }

    /// Build a document-store-focused dataset with database and collection rows.
    pub fn from_document_databases_and_collections(
        databases: Vec<(String, Option<i64>)>,
        collections: Vec<DocumentCollectionInfo>,
    ) -> Self {
        let columns = vec![
            ObjectsPanelColumn::new("name", "Name")
                .width(300.0)
                .min_width(150.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("database", "Database")
                .width(180.0)
                .min_width(120.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("document_count", "Documents")
                .width(100.0)
                .min_width(70.0)
                .resizable(false)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("index_count", "Indexes")
                .width(80.0)
                .min_width(60.0)
                .resizable(false)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("size", "Size")
                .width(90.0)
                .min_width(60.0)
                .resizable(false)
                .sortable()
                .text_right(),
        ];

        let mut rows = Vec::new();
        for (database_name, size_bytes) in databases {
            let mut values = std::collections::BTreeMap::new();
            values.insert("name".to_string(), database_name.clone());
            values.insert("database".to_string(), database_name.clone());
            values.insert("document_count".to_string(), "-".to_string());
            values.insert("index_count".to_string(), "-".to_string());
            values.insert(
                "size".to_string(),
                size_bytes
                    .map(|size| size.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );

            rows.push(ObjectsPanelRow {
                name: database_name.clone(),
                schema: None,
                object_type: "document_database".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("document_database", database_name.clone())
                        .with_database_option(Some(database_name)),
                ),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        for collection in collections {
            let mut values = std::collections::BTreeMap::new();
            values.insert("name".to_string(), collection.name.clone());
            values.insert("database".to_string(), collection.database.clone());
            values.insert(
                "document_count".to_string(),
                collection
                    .document_count
                    .map(|count| count.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );
            values.insert(
                "index_count".to_string(),
                collection
                    .index_count
                    .map(|count| count.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );
            values.insert(
                "size".to_string(),
                collection
                    .size_bytes
                    .map(|size| size.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );

            rows.push(ObjectsPanelRow {
                name: collection.name.clone(),
                schema: Some(collection.database.clone()),
                object_type: "document_collection".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("document_collection", collection.name)
                        .with_database_option(Some(collection.database)),
                ),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        Self { columns, rows }
    }

    /// Build the document objects panel data and manifest together.
    pub fn from_document_databases_and_collections_with_manifest(
        databases: Vec<(String, Option<i64>)>,
        collections: Vec<DocumentCollectionInfo>,
    ) -> (Self, ObjectsPanelManifest) {
        let data = Self::from_document_databases_and_collections(databases, collections);
        let manifest = ObjectsPanelManifest::from_data(&data);
        (data, manifest)
    }

    /// Build a kind-scoped view of the data while preserving the driver's columns.
    ///
    /// Callers can reuse the same manifest and column metadata while narrowing the
    /// row set to the currently active kind and optional scope.
    pub fn for_kind_and_scope(&self, kind_id: &str, scope: Option<&str>) -> Self {
        let rows = self
            .rows
            .iter()
            .filter(|row| row.object_kind_id() == kind_id)
            .filter(|row| {
                scope
                    .map(|scope_id| row.object_schema() == Some(scope_id))
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        Self {
            columns: self.columns.clone(),
            rows,
        }
    }
}

fn kind_label_from_id(kind_id: &str) -> String {
    let mut words = Vec::new();
    for part in kind_id.split('_').filter(|part| !part.is_empty()) {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            words.push(format!("{}{}", first.to_uppercase(), chars.as_str()));
        }
    }

    if words.is_empty() {
        "Object".to_string()
    } else {
        words.join(" ")
    }
}

fn default_actions_for_kind(kind_id: &str) -> Vec<ObjectsPanelAction> {
    let mut actions = Vec::new();

    if kind_id != "document_database" {
        actions.push(ObjectsPanelAction::new("open", "Open"));
    }

    if !matches!(
        kind_id,
        "redis_database" | "document_database" | "document_collection"
    ) {
        actions.push(ObjectsPanelAction::new("design", "Design").single_selection());
    }

    actions.push(ObjectsPanelAction::new("copy_name", "Copy Name"));
    actions.push(ObjectsPanelAction::new(
        "copy_qualified_name",
        "Copy Qualified Name",
    ));

    if matches!(
        kind_id,
        "table" | "view" | "function" | "procedure" | "trigger"
    ) {
        actions.push(ObjectsPanelAction::new("view_history", "View History").single_selection());
    }

    // Navigation-only document/Redis rows must not advertise destructive actions
    // that the dispatcher cannot satisfy for those surfaces.
    if !matches!(
        kind_id,
        "redis_database" | "document_database" | "document_collection"
    ) {
        actions.push(ObjectsPanelAction::new("delete", "Delete").destructive());
    }
    actions.push(ObjectsPanelAction::new("refresh", "Refresh"));

    actions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn objects_panel_object_ref_identity_key_tracks_optional_parts() {
        let object_ref = ObjectsPanelObjectRef::new("function", "sum_total")
            .with_database_option(Some("app".to_string()))
            .with_schema_option(Some("public".to_string()))
            .with_signature_option(Some("integer,integer".to_string()));

        assert_eq!(
            object_ref.identity_key,
            "function::app::public::sum_total::integer,integer"
        );

        assert_eq!(
            ObjectsPanelObjectRef::build_identity_key("table", None, None, "users", None),
            "table::::::users::"
        );
    }

    #[test]
    fn objects_panel_row_prefers_canonical_object_ref_identity() {
        let row = ObjectsPanelRow {
            name: "legacy_name".to_string(),
            schema: Some("legacy_schema".to_string()),
            object_type: "table".to_string(),
            object_ref: Some(
                ObjectsPanelObjectRef::new("view", "canonical_name")
                    .with_schema_option(Some("canonical_schema".to_string())),
            ),
            values: std::collections::BTreeMap::new(),
            redis_database_index: None,
            key_value_info: None,
        };

        assert_eq!(row.object_kind_id(), "view");
        assert_eq!(row.object_name(), "canonical_name");
        assert_eq!(row.object_schema(), Some("canonical_schema"));
    }

    #[test]
    fn objects_panel_data_for_kind_and_scope_filters_rows_without_changing_columns() {
        let data = ObjectsPanelData {
            columns: vec![ObjectsPanelColumn::new("name", "Name")],
            rows: vec![
                ObjectsPanelRow {
                    name: "users".to_string(),
                    schema: Some("public".to_string()),
                    object_type: "table".to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new("table", "users")
                            .with_schema_option(Some("public".to_string())),
                    ),
                    values: std::collections::BTreeMap::new(),
                    redis_database_index: None,
                    key_value_info: None,
                },
                ObjectsPanelRow {
                    name: "audit_log".to_string(),
                    schema: Some("archive".to_string()),
                    object_type: "table".to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new("table", "audit_log")
                            .with_schema_option(Some("archive".to_string())),
                    ),
                    values: std::collections::BTreeMap::new(),
                    redis_database_index: None,
                    key_value_info: None,
                },
                ObjectsPanelRow {
                    name: "active_users".to_string(),
                    schema: Some("public".to_string()),
                    object_type: "view".to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new("view", "active_users")
                            .with_schema_option(Some("public".to_string())),
                    ),
                    values: std::collections::BTreeMap::new(),
                    redis_database_index: None,
                    key_value_info: None,
                },
            ],
        };

        let scoped = data.for_kind_and_scope("table", Some("public"));

        let original_column_ids: Vec<&str> = data
            .columns
            .iter()
            .map(|column| column.id.as_str())
            .collect();
        let scoped_column_ids: Vec<&str> = scoped
            .columns
            .iter()
            .map(|column| column.id.as_str())
            .collect();
        assert_eq!(scoped_column_ids, original_column_ids);
        assert_eq!(scoped.rows.len(), 1);
        assert_eq!(scoped.rows[0].object_kind_id(), "table");
        assert_eq!(scoped.rows[0].object_name(), "users");
        assert_eq!(scoped.rows[0].object_schema(), Some("public"));
    }

    #[test]
    fn objects_panel_data_from_table_infos_maps_types_and_counts() {
        let data = ObjectsPanelData::from_table_infos(vec![
            TableInfo {
                schema: Some("public".to_string()),
                name: "users".to_string(),
                table_type: TableType::Table,
                owner: None,
                row_count: Some(10),
                size_bytes: None,
                comment: None,
                index_count: Some(2),
                trigger_count: Some(1),
                key_value_info: None,
            },
            TableInfo {
                schema: Some("public".to_string()),
                name: "active_users".to_string(),
                table_type: TableType::View,
                owner: None,
                row_count: None,
                size_bytes: None,
                comment: None,
                index_count: None,
                trigger_count: None,
                key_value_info: None,
            },
        ]);

        let column_ids: Vec<&str> = data
            .columns
            .iter()
            .map(|column| column.id.as_str())
            .collect();
        assert_eq!(
            column_ids,
            vec!["name", "row_count", "index_count", "trigger_count"]
        );

        assert_eq!(data.rows.len(), 2);
        assert_eq!(data.rows[0].object_kind_id(), "table");
        assert_eq!(data.rows[1].object_kind_id(), "view");
        assert_eq!(
            data.rows[1]
                .values
                .get("row_count")
                .map(|value| value.as_str()),
            Some("-")
        );
        assert!(data.rows[0].object_ref.is_some());
        assert!(data.rows[1].object_ref.is_some());
    }

    #[test]
    fn objects_panel_manifest_from_data_deduplicates_kind_and_sets_defaults() {
        let mut values = std::collections::BTreeMap::new();
        values.insert("name".to_string(), "users".to_string());

        let data = ObjectsPanelData {
            columns: vec![ObjectsPanelColumn::new("name", "Name")],
            rows: vec![
                ObjectsPanelRow {
                    name: "users".to_string(),
                    schema: Some("public".to_string()),
                    object_type: "table".to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new("table", "users")
                            .with_schema_option(Some("public".to_string())),
                    ),
                    values: values.clone(),
                    redis_database_index: None,
                    key_value_info: None,
                },
                ObjectsPanelRow {
                    name: "archive_users".to_string(),
                    schema: Some("public".to_string()),
                    object_type: "table".to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new("table", "archive_users")
                            .with_schema_option(Some("public".to_string())),
                    ),
                    values: values.clone(),
                    redis_database_index: None,
                    key_value_info: None,
                },
                ObjectsPanelRow {
                    name: "get_user(int)".to_string(),
                    schema: Some("public".to_string()),
                    object_type: "function".to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new("function", "get_user")
                            .with_schema_option(Some("public".to_string()))
                            .with_signature_option(Some("integer".to_string())),
                    ),
                    values,
                    redis_database_index: None,
                    key_value_info: None,
                },
            ],
        };

        let manifest = ObjectsPanelManifest::from_data(&data);
        assert_eq!(manifest.object_kinds.len(), 2);

        let table_kind = manifest
            .object_kinds
            .iter()
            .find(|kind| kind.id == "table")
            .expect("table kind should exist");
        assert_eq!(table_kind.default_row_action_id.as_deref(), Some("open"));
        assert!(
            table_kind
                .row_actions
                .iter()
                .any(|action| action.id == "view_history")
        );

        let toolbar_action_ids: Vec<&str> = manifest
            .toolbar_actions
            .iter()
            .map(|action| action.id.as_str())
            .collect();
        assert!(toolbar_action_ids.contains(&"refresh"));
        assert!(toolbar_action_ids.contains(&"new_table"));
        assert!(toolbar_action_ids.contains(&"new_view"));
        assert!(toolbar_action_ids.contains(&"import"));
        assert!(toolbar_action_ids.contains(&"export"));
    }

    #[test]
    fn objects_panel_manifest_from_empty_data_provides_usable_defaults() {
        let manifest = ObjectsPanelManifest::from_data(&ObjectsPanelData {
            columns: Vec::new(),
            rows: Vec::new(),
        });

        assert_eq!(manifest.object_kinds.len(), 1);
        let default_kind = &manifest.object_kinds[0];
        assert_eq!(default_kind.id, "table");
        assert!(!default_kind.columns.is_empty());
        assert!(
            default_kind
                .row_actions
                .iter()
                .any(|action| action.id == "open")
        );
        assert!(
            default_kind
                .row_actions
                .iter()
                .any(|action| action.id == "delete")
        );
        assert!(
            default_kind
                .row_actions
                .iter()
                .any(|action| action.id == "refresh")
        );
    }

    #[test]
    fn objects_panel_manifest_validate_rejects_duplicate_kind_ids() {
        let manifest = ObjectsPanelManifest {
            object_kinds: vec![
                ObjectsPanelObjectKind::new("table", "Table", "Tables")
                    .row_actions(vec![ObjectsPanelAction::new("open", "Open")]),
                ObjectsPanelObjectKind::new("table", "Table", "Tables")
                    .row_actions(vec![ObjectsPanelAction::new("open", "Open")]),
            ],
            toolbar_actions: Vec::new(),
        };

        let error = manifest
            .validate()
            .expect_err("duplicate kind ids should fail validation");
        assert!(
            matches!(error, ZqlzError::Schema(message) if message.contains("duplicate kind_id"))
        );
    }

    #[test]
    fn objects_panel_manifest_validate_rejects_duplicate_action_ids_per_kind() {
        let manifest = ObjectsPanelManifest {
            object_kinds: vec![
                ObjectsPanelObjectKind::new("table", "Table", "Tables").row_actions(vec![
                    ObjectsPanelAction::new("open", "Open"),
                    ObjectsPanelAction::new("open", "Open Again"),
                ]),
            ],
            toolbar_actions: Vec::new(),
        };

        let error = manifest
            .validate()
            .expect_err("duplicate action ids should fail validation");
        assert!(
            matches!(error, ZqlzError::Schema(message) if message.contains("duplicate action_id"))
        );
    }

    #[test]
    fn objects_panel_manifest_validate_rejects_unknown_default_row_action_id() {
        let manifest = ObjectsPanelManifest {
            object_kinds: vec![
                ObjectsPanelObjectKind::new("table", "Table", "Tables")
                    .row_actions(vec![ObjectsPanelAction::new("open", "Open")])
                    .default_row_action("design"),
            ],
            toolbar_actions: Vec::new(),
        };

        let error = manifest
            .validate()
            .expect_err("unknown default row action id should fail validation");
        assert!(
            matches!(error, ZqlzError::Schema(message) if message.contains("default_row_action_id"))
        );
    }

    #[test]
    fn objects_panel_manifest_validate_accepts_driver_fallback_manifest() {
        let manifest =
            ObjectsPanelManifest::from_data(&ObjectsPanelData::from_table_infos(vec![TableInfo {
                schema: Some("public".to_string()),
                name: "users".to_string(),
                table_type: TableType::Table,
                owner: None,
                row_count: Some(5),
                size_bytes: None,
                comment: None,
                index_count: Some(1),
                trigger_count: Some(0),
                key_value_info: None,
            }]));

        manifest
            .validate()
            .expect("fallback manifest should pass validation");
    }

    #[test]
    fn objects_panel_data_from_redis_databases_builds_redis_rows_and_columns() {
        let data = ObjectsPanelData::from_redis_databases(vec![(0, Some(12)), (1, None)]);

        let column_ids: Vec<&str> = data
            .columns
            .iter()
            .map(|column| column.id.as_str())
            .collect();
        assert_eq!(column_ids, vec!["name", "key_count"]);

        assert_eq!(data.rows.len(), 2);
        assert_eq!(data.rows[0].object_kind_id(), "redis_database");
        assert_eq!(data.rows[0].object_name(), "db0");
        assert_eq!(data.rows[0].redis_database_index, Some(0));
        assert_eq!(data.rows[1].redis_database_index, Some(1));
        assert_eq!(
            data.rows[0].values.get("key_count").map(String::as_str),
            Some("12")
        );
        assert_eq!(
            data.rows[1].values.get("key_count").map(String::as_str),
            Some("-")
        );
        assert!(data.rows[0].object_ref.is_some());
    }

    #[test]
    fn objects_panel_data_from_redis_databases_with_manifest_matches_redis_fallback_shape() {
        let (data, manifest) =
            ObjectsPanelData::from_redis_databases_with_manifest(vec![(0, Some(5))]);

        assert_eq!(data.rows.len(), 1);
        assert_eq!(data.rows[0].object_kind_id(), "redis_database");

        let toolbar_action_ids: Vec<&str> = manifest
            .toolbar_actions
            .iter()
            .map(|action| action.id.as_str())
            .collect();

        assert_eq!(toolbar_action_ids, vec!["refresh"]);
    }

    #[test]
    fn objects_panel_data_from_document_databases_and_collections_builds_document_rows() {
        let (data, manifest) =
            ObjectsPanelData::from_document_databases_and_collections_with_manifest(
                vec![("app".to_string(), Some(2048))],
                vec![DocumentCollectionInfo {
                    database: "app".to_string(),
                    name: "users".to_string(),
                    collection_type: "collection".to_string(),
                    document_count: Some(42),
                    size_bytes: Some(1024),
                    index_count: Some(3),
                }],
            );

        let column_ids: Vec<&str> = data
            .columns
            .iter()
            .map(|column| column.id.as_str())
            .collect();
        assert_eq!(
            column_ids,
            vec!["name", "database", "document_count", "index_count", "size"]
        );

        assert_eq!(data.rows.len(), 2);
        assert_eq!(data.rows[0].object_kind_id(), "document_database");
        assert_eq!(data.rows[1].object_kind_id(), "document_collection");
        assert_eq!(data.rows[1].object_name(), "users");
        assert_eq!(
            data.rows[1]
                .object_ref
                .as_ref()
                .and_then(|object_ref| object_ref.database.as_deref()),
            Some("app")
        );
        assert_eq!(
            data.rows[1]
                .values
                .get("document_count")
                .map(String::as_str),
            Some("42")
        );

        let collection_kind = manifest
            .object_kinds
            .iter()
            .find(|kind| kind.id == "document_collection")
            .expect("document collection kind should exist");
        let database_kind = manifest
            .object_kinds
            .iter()
            .find(|kind| kind.id == "document_database")
            .expect("document database kind should exist");
        assert_eq!(database_kind.default_row_action_id, None);
        manifest.validate().expect("manifest should validate");

        let row_action_ids: Vec<&str> = collection_kind
            .row_actions
            .iter()
            .map(|action| action.id.as_str())
            .collect();
        assert_eq!(
            row_action_ids,
            vec!["open", "copy_name", "copy_qualified_name", "refresh"]
        );
    }

    #[test]
    fn objects_panel_manifest_from_redis_data_uses_refresh_only_toolbar() {
        let data = ObjectsPanelData::from_redis_databases(vec![(0, Some(5))]);
        let manifest = ObjectsPanelManifest::from_data(&data);

        let toolbar_action_ids: Vec<&str> = manifest
            .toolbar_actions
            .iter()
            .map(|action| action.id.as_str())
            .collect();
        assert_eq!(toolbar_action_ids, vec!["refresh"]);

        let redis_kind = manifest
            .object_kinds
            .iter()
            .find(|kind| kind.id == "redis_database")
            .expect("redis_database kind should exist");
        assert_eq!(redis_kind.default_row_action_id.as_deref(), Some("open"));
    }

    #[test]
    fn objects_panel_manifest_from_redis_data_exposes_only_supported_row_actions() {
        let data = ObjectsPanelData::from_redis_databases(vec![(0, Some(5))]);
        let manifest = ObjectsPanelManifest::from_data(&data);

        let redis_kind = manifest
            .object_kinds
            .iter()
            .find(|kind| kind.id == "redis_database")
            .expect("redis_database kind should exist");

        let row_action_ids: Vec<&str> = redis_kind
            .row_actions
            .iter()
            .map(|action| action.id.as_str())
            .collect();

        assert_eq!(
            row_action_ids,
            vec!["open", "copy_name", "copy_qualified_name", "refresh"]
        );
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
