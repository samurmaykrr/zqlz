//! DDL generation utilities

use zqlz_core::{ColumnInfo, IndexInfo, TableInfo};

/// DDL generator for creating SQL statements
pub struct DdlGenerator {
    /// Database dialect
    #[allow(dead_code)]
    dialect: String,
}

impl DdlGenerator {
    /// Create a new DDL generator
    pub fn new(dialect: &str) -> Self {
        Self {
            dialect: dialect.to_string(),
        }
    }

    /// Generate CREATE TABLE statement
    pub fn create_table(&self, table: &TableInfo, columns: &[ColumnInfo]) -> String {
        tracing::debug!(table = %table.name, column_count = columns.len(), "generating CREATE TABLE DDL");
        let mut sql = format!("CREATE TABLE \"{}\" (\n", table.name);

        let column_defs: Vec<String> = columns
            .iter()
            .map(|col| self.column_definition(col))
            .collect();

        sql.push_str(&column_defs.join(",\n"));
        sql.push_str("\n);");

        sql
    }

    /// Generate column definition
    fn column_definition(&self, column: &ColumnInfo) -> String {
        let mut def = format!("  \"{}\" {}", column.name, column.data_type);

        if !column.nullable {
            def.push_str(" NOT NULL");
        }

        if column.is_primary_key {
            def.push_str(" PRIMARY KEY");
        }

        if let Some(default) = &column.default_value {
            def.push_str(&format!(" DEFAULT {}", default));
        }

        def
    }

    /// Generate CREATE INDEX statement
    pub fn create_index(&self, table: &str, index: &IndexInfo) -> String {
        tracing::debug!(table = %table, index = %index.name, "generating CREATE INDEX DDL");
        let unique = if index.is_unique { "UNIQUE " } else { "" };
        let columns = index
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "CREATE {}INDEX \"{}\" ON \"{}\" ({});",
            unique, index.name, table, columns
        )
    }

    /// Generate DROP TABLE statement
    pub fn drop_table(&self, table: &str) -> String {
        tracing::debug!(table = %table, "generating DROP TABLE DDL");
        format!("DROP TABLE IF EXISTS \"{}\";", table)
    }
}

impl Default for DdlGenerator {
    fn default() -> Self {
        Self::new("sqlite")
    }
}
