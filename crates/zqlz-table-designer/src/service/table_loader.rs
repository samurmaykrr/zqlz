//! Table loader for loading existing table structures
//!
//! Loads table structure from a database connection and converts it
//! to a TableDesign for editing.

use std::sync::Arc;

use crate::models::{DatabaseDialect, TableDesign};
use zqlz_core::{Connection, Value};

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

        let mut design = TableDesign::from_table_details(table_details, dialect);
        if design.dialect == DatabaseDialect::Postgres {
            // Postgres lists a UNIQUE constraint's backing index like any other
            // index; only pg_constraint says which of them must be dropped as
            // a constraint.
            match connection
                .query(
                    "SELECT con.conname
                     FROM pg_constraint con
                     JOIN pg_class rel ON rel.oid = con.conrelid
                     JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                     WHERE con.contype = 'u' AND nsp.nspname = $1 AND rel.relname = $2",
                    &[
                        Value::String(schema.unwrap_or("public").to_string()),
                        Value::String(table_name.to_string()),
                    ],
                )
                .await
            {
                Ok(result) => {
                    design.constraint_indexes = result
                        .rows
                        .iter()
                        .filter_map(|row| row.get(0).and_then(|v| v.as_str()).map(str::to_string))
                        .collect();
                }
                Err(error) => {
                    tracing::warn!(%error, table = table_name, "Failed to load unique constraint names");
                }
            }
        }
        if design.dialect == DatabaseDialect::Mysql {
            super::mysql_loader::apply(connection.as_ref(), schema, table_name, &mut design).await;
        }
        Ok(design)
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
    use crate::service::DdlGenerator;
    use zqlz_drivers::sqlite::SqliteConnection;

    #[tokio::test]
    async fn sqlite_declared_types_round_trip_without_ddl_changes() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("sakila.db");
        let connection = SqliteConnection::open(&path.to_string_lossy())?;
        connection
            .execute_batch(
                "CREATE TABLE rental (
                    rental_id INTEGER NOT NULL,
                    rental_date TIMESTAMP NOT NULL,
                    inventory_id INT NOT NULL,
                    return_date TIMESTAMP DEFAULT NULL,
                    title VARCHAR(255),
                    rental_rate DECIMAL(4,2) NOT NULL DEFAULT 4.99,
                    notes BLOB SUB_TYPE TEXT,
                    PRIMARY KEY (rental_id)
                );",
            )
            .await?;

        let design = TableLoader::load_table(
            Arc::new(connection),
            None,
            "rental",
            DatabaseDialect::Sqlite,
        )
        .await?;

        let types: Vec<(&str, Option<u32>, Option<u32>)> = design
            .columns
            .iter()
            .map(|column| (column.data_type.as_str(), column.length, column.scale))
            .collect();
        assert_eq!(
            types,
            [
                ("INTEGER", None, None),
                ("TIMESTAMP", None, None),
                ("INT", None, None),
                ("TIMESTAMP", None, None),
                ("VARCHAR", Some(255), None),
                ("DECIMAL", Some(4), Some(2)),
                ("BLOB SUB_TYPE TEXT", None, None),
            ]
        );

        assert!(DdlGenerator::generate_alter_table(&design, &design.clone())?.is_empty());

        let create = DdlGenerator::generate_create_table(&design)?;
        for expected in [
            "\"inventory_id\" INT NOT NULL",
            "\"return_date\" TIMESTAMP",
            "\"title\" VARCHAR(255)",
            "\"rental_rate\" DECIMAL(4, 2) NOT NULL",
            "\"notes\" BLOB SUB_TYPE TEXT",
        ] {
            assert!(
                create.contains(expected),
                "missing {expected} in:\n{create}"
            );
        }
        Ok(())
    }

    /// Live check against the docker Postgres used for manual testing.
    /// Run with `-- --ignored`.
    #[tokio::test]
    #[ignore = "needs the zqlz-shots-pg container on 127.0.0.1:55432"]
    async fn live_postgres_key_flags() -> anyhow::Result<()> {
        use zqlz_core::{ConnectionConfig, DatabaseDriver};
        let mut config = ConnectionConfig::new_postgres("127.0.0.1", 55432, "postgres", "nimbus");
        config.password = Some("nimbus-demo".into());
        let connection: Arc<dyn Connection> =
            zqlz_drivers::postgres::PostgresDriver::new().connect(&config).await?;

        // (table, column) -> (pk, uq, ai)
        let cases = [
            ("zqlz_lab", "customers", "customer_id", (true, false, true)),
            ("zqlz_lab", "customers", "tenant_id", (false, false, false)),
            ("zqlz_lab", "customers", "email", (false, false, false)),
            ("zqlz_lab", "customers", "customer_number", (false, true, true)),
            ("zqlz_lab", "orders", "order_id", (true, false, true)),
            ("zqlz_lab", "orders", "placed_at", (true, false, false)),
            ("zqlz_weird_names", "select", "from", (true, false, true)),
        ];
        for (schema, table, column, expected) in cases {
            let design = TableLoader::load_table(
                connection.clone(),
                Some(schema),
                table,
                DatabaseDialect::Postgres,
            )
            .await?;
            let c = design.columns.iter().find(|c| c.name == column).unwrap();
            assert_eq!(
                (c.is_primary_key, c.is_unique, c.is_auto_increment),
                expected,
                "{schema}.{table}.{column}"
            );
            assert!(DdlGenerator::generate_alter_table(&design, &design.clone())?.is_empty());
            if table == "customers" {
                let cols = |name: &str| {
                    design.indexes.iter().find(|i| i.name == name).map(|i| i.columns.clone())
                };
                assert_eq!(
                    cols("customers_email_per_tenant_unique"),
                    Some(vec!["tenant_id".to_string(), "email".to_string()])
                );
                assert_eq!(
                    cols("customers_lower_display_name_idx"),
                    Some(vec!["lower(display_name)".to_string()])
                );
            }
        }
        // Custom-named constraint, Postgres' default `_key` name and a plain
        // unique index must each be dropped under their real name.
        let run = |sql: &'static str| {
            let connection = connection.clone();
            async move { connection.execute(sql, &[]).await }
        };
        run("CREATE SCHEMA IF NOT EXISTS zqlz_test").await?;
        run("DROP TABLE IF EXISTS zqlz_test.designer_uq").await?;
        run("CREATE TABLE zqlz_test.designer_uq (
                id int PRIMARY KEY,
                a text CONSTRAINT custom_a_uq UNIQUE,
                b text,
                d text UNIQUE,
                e text
            )")
        .await?;
        run("CREATE UNIQUE INDEX designer_uq_b_idx ON zqlz_test.designer_uq (b)").await?;

        let load = || {
            TableLoader::load_table(
                connection.clone(),
                Some("zqlz_test"),
                "designer_uq",
                DatabaseDialect::Postgres,
            )
        };
        let design = load().await?;
        let names = |design: &TableDesign| -> Vec<(String, Option<String>)> {
            design
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.unique_constraint_name.clone()))
                .collect()
        };
        assert_eq!(
            names(&design),
            [
                ("id".to_string(), None),
                ("a".to_string(), Some("custom_a_uq".to_string())),
                ("b".to_string(), Some("designer_uq_b_idx".to_string())),
                ("d".to_string(), Some("designer_uq_d_key".to_string())),
                ("e".to_string(), None),
            ]
        );
        assert!(design.constraint_indexes.contains("custom_a_uq"));
        assert!(design.constraint_indexes.contains("designer_uq_d_key"));
        assert!(!design.constraint_indexes.contains("designer_uq_b_idx"));

        let mut edited = design.clone();
        for column in edited.columns.iter_mut().filter(|c| c.is_unique) {
            column.is_unique = false;
        }
        edited.columns[4].is_unique = true;
        let mut statements = DdlGenerator::generate_alter_table(&design, &edited)?;
        // The unique indexes themselves are still listed unchanged, so only the
        // column-level toggles produce statements.
        assert_eq!(statements.len(), 4, "{statements:?}");
        for statement in statements.drain(..) {
            connection.execute(&statement, &[]).await?;
        }
        let reloaded = load().await?;
        assert_eq!(
            names(&reloaded),
            [
                ("id".to_string(), None),
                ("a".to_string(), None),
                ("b".to_string(), None),
                ("d".to_string(), None),
                ("e".to_string(), Some("designer_uq_e_key".to_string())),
            ]
        );
        run("DROP TABLE zqlz_test.designer_uq").await?;
        Ok(())
    }

    #[test]
    fn test_detect_dialect() {
        assert_eq!(
            TableLoader::detect_dialect_from_driver("sqlite"),
            DatabaseDialect::Sqlite
        );
        assert_eq!(
            TableLoader::detect_dialect_from_driver("turso"),
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
