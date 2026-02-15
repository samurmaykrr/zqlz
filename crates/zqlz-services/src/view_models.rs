use serde::{Deserialize, Serialize};
use zqlz_core::{ColumnMeta, ForeignKeyInfo, IndexInfo, ObjectsPanelData, TableInfo};

/// Database schema overview for UI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSchema {
    /// Full table information including row counts
    pub table_infos: Vec<TableInfo>,
    /// Extended objects panel data (driver-specific columns and values)
    pub objects_panel_data: Option<ObjectsPanelData>,
    /// Table names (convenience accessor, derived from table_infos)
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub materialized_views: Vec<String>,
    pub triggers: Vec<String>,
    pub functions: Vec<String>,
    pub procedures: Vec<String>,
    pub table_indexes: std::collections::HashMap<String, Vec<IndexInfo>>,
    /// The database name this schema belongs to (e.g. "pagila")
    pub database_name: Option<String>,
    /// The schema name these objects belong to (e.g. "public")
    pub schema_name: Option<String>,
}

/// Table details for UI (enriched with additional metadata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDetails {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<IndexInfo>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub primary_key_columns: Vec<String>,
    pub row_count: Option<usize>,
}

/// Column information for UI (simplified from ColumnMeta)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<String>,
}

impl From<ColumnMeta> for ColumnInfo {
    fn from(meta: ColumnMeta) -> Self {
        Self {
            name: meta.name,
            data_type: meta.data_type,
            nullable: meta.nullable,
            is_primary_key: false, // Will be set by TableDetails builder
            default_value: meta.default_value,
        }
    }
}
