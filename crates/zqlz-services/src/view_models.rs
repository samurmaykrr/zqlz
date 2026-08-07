use serde::{Deserialize, Serialize};
use zqlz_core::{
    ColumnMeta, ConstraintInfo, ForeignKeyInfo, IndexInfo, ObjectsPanelData, ObjectsPanelManifest,
    TableInfo, TableType, TriggerInfo,
};

/// Database schema overview for UI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSchema {
    /// Full table information including row counts
    pub table_infos: Vec<TableInfo>,
    /// Extended objects panel data (driver-specific columns and values)
    pub objects_panel_data: Option<ObjectsPanelData>,
    /// Declarative behavior metadata for objects panel rendering and actions.
    pub objects_panel_manifest: Option<ObjectsPanelManifest>,
    /// Table names (convenience accessor, derived from table_infos)
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub materialized_views: Vec<String>,
    pub triggers: Vec<String>,
    pub functions: Vec<String>,
    pub procedures: Vec<String>,
    pub events: Vec<String>,
    pub sequences: Vec<String>,
    pub domains: Vec<String>,
    pub types: Vec<String>,
    pub extensions: Vec<String>,
    pub table_indexes: std::collections::HashMap<String, Vec<IndexInfo>>,
    /// The database name this schema belongs to (e.g. "pagila")
    pub database_name: Option<String>,
    /// The schema name these objects belong to (e.g. "public")
    pub schema_name: Option<String>,
    /// All schemas visible to the current connection context.
    pub schema_names: Vec<String>,
}

/// Table details for UI (enriched with additional metadata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDetails {
    pub name: String,
    pub table_type: TableType,
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<IndexInfo>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub constraints: Vec<ConstraintInfo>,
    pub triggers: Vec<TriggerInfo>,
    pub primary_key_columns: Vec<String>,
    pub row_count: Option<usize>,
}

/// The subset of a table's metadata that SQL completions actually need.
///
/// Warming a whole schema for the editor fetches only this, because it is the only
/// part [`TableDetails`] contributes to completions — indexes, constraints and
/// triggers are read solely by user-initiated, per-table views.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableColumnSummary {
    pub columns: Vec<ColumnInfo>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
}

impl From<&TableDetails> for TableColumnSummary {
    fn from(details: &TableDetails) -> Self {
        Self {
            columns: details.columns.clone(),
            foreign_keys: details.foreign_keys.clone(),
        }
    }
}

impl From<&zqlz_core::ColumnInfo> for ColumnInfo {
    fn from(column: &zqlz_core::ColumnInfo) -> Self {
        Self {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            is_primary_key: column.is_primary_key,
            default_value: column.default_value.clone(),
            max_length: column.max_length,
            precision: column.precision,
            scale: column.scale,
            is_auto_increment: column.is_auto_increment,
            comment: column.comment.clone(),
            enum_values: column.enum_values.clone(),
        }
    }
}

/// Column information for UI (simplified from ColumnMeta)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<String>,
    pub max_length: Option<i64>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
    pub is_auto_increment: bool,
    pub comment: Option<String>,
    pub enum_values: Option<Vec<String>>,
}

impl From<ColumnMeta> for ColumnInfo {
    fn from(meta: ColumnMeta) -> Self {
        Self {
            name: meta.name,
            data_type: meta.data_type,
            nullable: meta.nullable,
            is_primary_key: false, // Will be set by TableDetails builder
            default_value: meta.default_value,
            max_length: meta.max_length,
            precision: meta.precision,
            scale: meta.scale,
            is_auto_increment: meta.auto_increment,
            comment: meta.comment,
            enum_values: meta.enum_values,
        }
    }
}
