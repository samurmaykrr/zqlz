//! ZQLZ Drivers - Database driver implementations
//!
//! This crate provides concrete implementations of the database driver traits
//! defined in `zqlz-core`.

// SQL Databases
#[cfg(feature = "duckdb")]
pub use zqlz_driver_duckdb as duckdb;
#[cfg(feature = "mssql")]
pub use zqlz_driver_mssql as mssql;
#[cfg(feature = "mysql")]
pub use zqlz_driver_mysql as mysql;
#[cfg(feature = "postgres")]
pub use zqlz_driver_postgres as postgres;
#[cfg(feature = "sqlite")]
pub use zqlz_driver_sqlite as sqlite;

// NoSQL Databases
#[cfg(feature = "clickhouse")]
pub use zqlz_driver_clickhouse as clickhouse;
#[cfg(feature = "mongodb")]
pub use zqlz_driver_mongodb as mongodb;
#[cfg(feature = "redis")]
pub use zqlz_driver_redis as redis;

mod registry;
mod runtime;

pub use registry::{get_dialect_bundle, get_dialect_info, DriverRegistry};
pub use runtime::{block_on_tokio, get_tokio_runtime};

/// Re-export commonly used types from zqlz-core
pub use zqlz_core::{
    ColumnMeta, Connection, ConnectionConfig, DatabaseDriver, DriverCapabilities,
    PreparedStatement, QueryResult, Result, Row, SchemaIntrospection, StatementResult, Transaction,
    Value, ZqlzError,
};

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use sqlite::SqliteConnection;

    #[tokio::test]
    async fn test_sqlite_connection() {
        let conn = SqliteConnection::open(":memory:").expect("Failed to open in-memory db");

        // Create a test table
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)",
            &[],
        )
        .await
        .expect("Failed to create table");

        // Insert some data
        conn.execute(
            "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
            &[],
        )
        .await
        .expect("Failed to insert");

        // Query the data
        let result = conn
            .query("SELECT * FROM users", &[])
            .await
            .expect("Failed to query");

        assert_eq!(result.rows.len(), 1);
        println!("Query returned {} rows", result.rows.len());
    }

    #[tokio::test]
    async fn test_sqlite_schema_introspection() {
        let conn = SqliteConnection::open(":memory:").expect("Failed to open in-memory db");

        // Create test tables
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("Failed to create users table");

        conn.execute(
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)",
            &[],
        )
        .await
        .expect("Failed to create posts table");

        // Create a view
        conn.execute(
            "CREATE VIEW user_posts AS SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
            &[],
        )
        .await
        .expect("Failed to create view");

        // Test schema introspection
        let schema = conn
            .as_schema_introspection()
            .expect("Should have schema introspection");

        let tables = schema
            .list_tables(None)
            .await
            .expect("Failed to list tables");
        assert_eq!(tables.len(), 2);

        let table_names: Vec<_> = tables.iter().map(|t| t.name.as_str()).collect();
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"posts"));

        let views = schema.list_views(None).await.expect("Failed to list views");
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].name, "user_posts");

        println!("Schema introspection test passed!");
    }
}
