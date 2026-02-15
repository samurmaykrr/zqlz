//! Models for table design
//!
//! Core data structures for representing table structure, columns, indexes,
//! foreign keys, and dialect-specific options.

mod column_design;
mod data_types;
mod foreign_key_design;
mod index_design;
mod table_design;
mod table_options;
mod validation;

pub use column_design::ColumnDesign;
pub use data_types::{get_data_types, DataTypeCategory, DataTypeInfo};
pub use foreign_key_design::ForeignKeyDesign;
pub use index_design::IndexDesign;
pub use table_design::TableDesign;
pub use table_options::TableOptions;
pub use validation::ValidationError;

/// Database dialect for DDL generation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DatabaseDialect {
    #[default]
    Sqlite,
    Postgres,
    Mysql,
}

impl DatabaseDialect {
    /// Create from driver name string
    pub fn from_driver_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "sqlite" => DatabaseDialect::Sqlite,
            "postgres" | "postgresql" => DatabaseDialect::Postgres,
            "mysql" | "mariadb" => DatabaseDialect::Mysql,
            _ => DatabaseDialect::Sqlite,
        }
    }

    /// Get the display name
    pub fn name(&self) -> &'static str {
        match self {
            DatabaseDialect::Sqlite => "SQLite",
            DatabaseDialect::Postgres => "PostgreSQL",
            DatabaseDialect::Mysql => "MySQL",
        }
    }

    /// Get the driver name used to look up `DialectInfo`
    pub fn driver_name(&self) -> &'static str {
        match self {
            DatabaseDialect::Sqlite => "sqlite",
            DatabaseDialect::Postgres => "postgres",
            DatabaseDialect::Mysql => "mysql",
        }
    }
}
