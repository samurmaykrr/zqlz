use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Database object types that can be stored and shown in completions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DatabaseObject {
    Table(TableInfo),
    View(ViewInfo),
    Column(ColumnInfo),
    StoredProcedure(ProcedureInfo),
    Function(FunctionInfo),
    Trigger(TriggerInfo),
    Index(IndexInfo),
    Sequence(zqlz_core::SequenceInfo),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub schema: Option<String>,
    pub comment: Option<String>,
    pub row_count: Option<i64>,
    pub table_type: zqlz_core::TableType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewInfo {
    pub name: String,
    pub schema: Option<String>,
    pub definition: Option<String>,
    pub is_materialized: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub table_name: String,
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
    pub is_foreign_key: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcedureInfo {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<ParameterInfo>,
    pub return_type: Option<String>,
    pub definition: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInfo {
    pub name: String,
    pub schema: Option<String>,
    pub parameters: Vec<ParameterInfo>,
    pub return_type: String,
    pub definition: Option<String>,
    pub is_aggregate: bool,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterInfo {
    pub name: String,
    pub data_type: String,
    pub direction: ParameterDirection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterDirection {
    In,
    Out,
    InOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerInfo {
    pub name: String,
    pub table_name: String,
    pub event: String,
    pub timing: String,
    pub definition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
}

#[derive(Default, Serialize, Deserialize)]
pub struct SchemaCache {
    /// All database objects.
    pub objects: Vec<DatabaseObject>,

    /// Quick lookup maps.
    pub tables: HashMap<String, TableInfo>,
    pub views: HashMap<String, ViewInfo>,
    pub columns_by_table: HashMap<String, Vec<ColumnInfo>>,
    pub procedures: HashMap<String, ProcedureInfo>,
    pub functions: HashMap<String, FunctionInfo>,
    pub triggers: HashMap<String, TriggerInfo>,
    pub indexes: HashMap<String, IndexInfo>,
    #[serde(default)]
    pub sequences: HashMap<String, zqlz_core::SequenceInfo>,

    /// Resolved database/schema metadata from last fetch.
    #[serde(default)]
    pub database_name: Option<String>,
    #[serde(default)]
    pub schema_name: Option<String>,
    #[serde(default)]
    pub schema_names: Vec<String>,

    /// Foreign key relationships: table_name -> Vec<ForeignKeyInfo>.
    pub foreign_keys_by_table: HashMap<String, Vec<zqlz_core::ForeignKeyInfo>>,

    /// Reverse foreign key lookup: referenced_table -> Vec<(source_table, fk_info)>.
    pub reverse_foreign_keys: HashMap<String, Vec<(String, zqlz_core::ForeignKeyInfo)>>,

    /// Last time the cache was refreshed.
    #[serde(skip)]
    pub last_refresh: Option<std::time::SystemTime>,
}
