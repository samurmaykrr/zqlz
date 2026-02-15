//! Table loader for loading existing table structures
//!
//! Loads table structure from a database connection and converts it
//! to a TableDesign for editing.

use std::sync::Arc;

use crate::models::{DatabaseDialect, TableDesign};
use zqlz_core::Connection;

/// Table loader for converting database tables to TableDesign
///
/// This is a stateless utility for loading existing tables.
pub struct TableLoader;

impl TableLoader {
    /// Load an existing table's structure for editing
    ///
    /// Returns a TableDesign that can be modified and saved back.
    ///
    /// # Arguments
    /// * `connection` - Database connection with schema introspection support
    /// * `schema` - Schema name (optional, not used by SQLite)
    /// * `table_name` - Name of the table to load
    /// * `dialect` - Database dialect for the loaded design
    ///
    /// # Errors
    /// Returns an error if the table cannot be loaded or introspection is not supported.
    pub async fn load_table(
        connection: Arc<dyn Connection>,
        schema: Option<&str>,
        table_name: &str,
        dialect: DatabaseDialect,
    ) -> anyhow::Result<TableDesign> {
        let schema_introspection = connection.as_schema_introspection().ok_or_else(|| {
            anyhow::anyhow!("Schema introspection not supported for this connection")
        })?;

        let table_details = schema_introspection
            .get_table(schema, table_name)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load table '{}': {}", table_name, e))?;

        Ok(TableDesign::from_table_details(table_details, dialect))
    }

    /// Detect the database dialect from a driver name string
    ///
    /// The caller should obtain the driver name from their connection manager
    /// or connection configuration.
    pub fn detect_dialect_from_driver(driver_name: &str) -> DatabaseDialect {
        DatabaseDialect::from_driver_name(driver_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_dialect() {
        assert_eq!(
            TableLoader::detect_dialect_from_driver("sqlite"),
            DatabaseDialect::Sqlite
        );
        assert_eq!(
            TableLoader::detect_dialect_from_driver("postgres"),
            DatabaseDialect::Postgres
        );
        assert_eq!(
            TableLoader::detect_dialect_from_driver("postgresql"),
            DatabaseDialect::Postgres
        );
        assert_eq!(
            TableLoader::detect_dialect_from_driver("mysql"),
            DatabaseDialect::Mysql
        );
        assert_eq!(
            TableLoader::detect_dialect_from_driver("mariadb"),
            DatabaseDialect::Mysql
        );
        // Unknown defaults to SQLite
        assert_eq!(
            TableLoader::detect_dialect_from_driver("unknown"),
            DatabaseDialect::Sqlite
        );
    }
}
