//! Data type information

use super::DatabaseDialect;
use gpui::SharedString;
use zqlz_drivers::get_dialect_info;
use zqlz_ui::widgets::select::SelectItem;

/// Information about a data type
#[derive(Debug, Clone)]
pub struct DataTypeInfo {
    /// Type name as used in SQL
    pub name: String,
    /// Display name for UI
    pub display_name: String,
    /// Category for grouping
    pub category: DataTypeCategory,
    /// Whether this type supports length parameter
    pub supports_length: bool,
    /// Whether this type supports precision/scale
    pub supports_precision: bool,
    /// Whether this is a common/frequently used type
    pub is_common: bool,
}

/// Category of data type for grouping in UI
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataTypeCategory {
    Integer,
    Float,
    Decimal,
    String,
    Binary,
    DateTime,
    Boolean,
    Json,
    Other,
}

impl DataTypeCategory {
    /// Get display name for the category
    pub fn display_name(&self) -> &'static str {
        match self {
            DataTypeCategory::Integer => "Integer",
            DataTypeCategory::Float => "Floating Point",
            DataTypeCategory::Decimal => "Decimal",
            DataTypeCategory::String => "Text",
            DataTypeCategory::Binary => "Binary",
            DataTypeCategory::DateTime => "Date/Time",
            DataTypeCategory::Boolean => "Boolean",
            DataTypeCategory::Json => "JSON",
            DataTypeCategory::Other => "Other",
        }
    }
}

/// Get available data types for a dialect
pub fn get_data_types(dialect: &DatabaseDialect) -> Vec<DataTypeInfo> {
    let driver_name = match dialect {
        DatabaseDialect::Sqlite => "sqlite",
        DatabaseDialect::Postgres => "postgres",
        DatabaseDialect::Mysql => "mysql",
    };

    let dialect_info = get_dialect_info(driver_name);

    dialect_info
        .data_types
        .iter()
        .map(|dt| DataTypeInfo {
            name: dt.name.to_string(),
            display_name: dt.name.to_string(),
            category: convert_data_type_category(&dt.category),
            supports_length: dt.accepts_length,
            supports_precision: dt.accepts_scale,
            is_common: is_common_type(&dt.name, &dt.category),
        })
        .collect()
}

/// Convert from zqlz_core::DataTypeCategory to local DataTypeCategory
fn convert_data_type_category(category: &zqlz_core::DataTypeCategory) -> DataTypeCategory {
    match category {
        zqlz_core::DataTypeCategory::Integer => DataTypeCategory::Integer,
        zqlz_core::DataTypeCategory::Float => DataTypeCategory::Float,
        zqlz_core::DataTypeCategory::Decimal => DataTypeCategory::Decimal,
        zqlz_core::DataTypeCategory::String => DataTypeCategory::String,
        zqlz_core::DataTypeCategory::Binary => DataTypeCategory::Binary,
        zqlz_core::DataTypeCategory::Boolean => DataTypeCategory::Boolean,
        zqlz_core::DataTypeCategory::Date
        | zqlz_core::DataTypeCategory::Time
        | zqlz_core::DataTypeCategory::DateTime
        | zqlz_core::DataTypeCategory::Interval => DataTypeCategory::DateTime,
        zqlz_core::DataTypeCategory::Json => DataTypeCategory::Json,
        _ => DataTypeCategory::Other,
    }
}

/// Determine if a type is commonly used (for UI prioritization)
fn is_common_type(name: &str, category: &zqlz_core::DataTypeCategory) -> bool {
    let upper = name.to_uppercase();
    matches!(
        upper.as_str(),
        "INTEGER"
            | "INT"
            | "BIGINT"
            | "SERIAL"
            | "BIGSERIAL"
            | "TEXT"
            | "VARCHAR"
            | "CHAR"
            | "REAL"
            | "FLOAT"
            | "DOUBLE PRECISION"
            | "BOOLEAN"
            | "BOOL"
            | "TIMESTAMP"
            | "TIMESTAMPTZ"
            | "DATE"
            | "BLOB"
            | "BYTEA"
            | "JSON"
            | "JSONB"
            | "UUID"
    ) || matches!(
        category,
        zqlz_core::DataTypeCategory::Integer | zqlz_core::DataTypeCategory::String
    )
}

impl SelectItem for DataTypeInfo {
    type Value = String;

    fn title(&self) -> SharedString {
        SharedString::from(self.display_name.clone())
    }

    fn value(&self) -> &Self::Value {
        &self.name
    }

    fn matches(&self, query: &str) -> bool {
        self.name.to_lowercase().contains(&query.to_lowercase())
            || self
                .display_name
                .to_lowercase()
                .contains(&query.to_lowercase())
    }
}
