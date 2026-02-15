//! Schema introspection integration tests for ClickHouse
//!
//! These tests require a running ClickHouse server.
//! Run with: cargo test -p zqlz-driver-clickhouse --test schema_integration -- --ignored

use zqlz_core::{Connection, ConnectionConfig, DatabaseDriver, SchemaIntrospection};
use zqlz_driver_clickhouse::ClickHouseDriver;

fn test_config() -> ConnectionConfig {
    let mut config = ConnectionConfig::new("clickhouse", "ClickHouse Test");
    config.port = 8123;
    config.database = Some("default".to_string());
    config.username = Some("default".to_string());
    config
        .params
        .insert("host".to_string(), "localhost".to_string());
    config
}

#[tokio::test]
#[ignore = "requires running ClickHouse server"]
async fn test_introspect_system_tables() {
    let driver = ClickHouseDriver::new();
    let config = test_config();

    let conn = driver.connect(&config).await.expect("Failed to connect");

    // Get schema introspection interface
    let schema = conn
        .as_schema_introspection()
        .expect("Should support schema introspection");

    // Test list_databases - should include 'default' and 'system'
    let databases = schema
        .list_databases()
        .await
        .expect("list_databases failed");
    assert!(!databases.is_empty(), "Should have at least one database");
    let db_names: Vec<&str> = databases.iter().map(|d| d.name.as_str()).collect();
    assert!(
        db_names.contains(&"default"),
        "Should have 'default' database"
    );
    assert!(
        db_names.contains(&"system"),
        "Should have 'system' database"
    );

    // Test list_schemas - same as databases in ClickHouse
    let schemas = schema.list_schemas().await.expect("list_schemas failed");
    assert!(!schemas.is_empty(), "Should have at least one schema");

    // Test list_tables for system database - should have many system tables
    let tables = schema
        .list_tables(Some("system"))
        .await
        .expect("list_tables failed");
    assert!(!tables.is_empty(), "System database should have tables");

    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(
        table_names.contains(&"tables"),
        "System should have 'tables' table"
    );
    assert!(
        table_names.contains(&"columns"),
        "System should have 'columns' table"
    );
    assert!(
        table_names.contains(&"databases"),
        "System should have 'databases' table"
    );

    // Test get_columns for system.tables
    let columns = schema
        .get_columns(Some("system"), "tables")
        .await
        .expect("get_columns failed");
    assert!(!columns.is_empty(), "system.tables should have columns");

    let col_names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    assert!(col_names.contains(&"name"), "Should have 'name' column");
    assert!(
        col_names.contains(&"database"),
        "Should have 'database' column"
    );
    assert!(col_names.contains(&"engine"), "Should have 'engine' column");
}

#[tokio::test]
#[ignore = "requires running ClickHouse server"]
async fn test_introspect_user_table() {
    let driver = ClickHouseDriver::new();
    let config = test_config();

    let conn = driver.connect(&config).await.expect("Failed to connect");
    let schema = conn
        .as_schema_introspection()
        .expect("Should support schema introspection");

    // Create a test table
    let table_name = format!("test_introspect_{}", uuid::Uuid::new_v4().simple());
    conn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id UInt64,
                name String,
                value Float64,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree() ORDER BY id",
            table_name
        ),
        &[],
    )
    .await
    .expect("Failed to create test table");

    // Test get_table
    let table_details = schema
        .get_table(Some("default"), &table_name)
        .await
        .expect("get_table failed");

    assert_eq!(table_details.info.name, table_name);
    assert_eq!(table_details.columns.len(), 4);

    // Verify primary key
    let pk = table_details.primary_key.expect("Should have primary key");
    assert!(pk.columns.contains(&"id".to_string()));

    // Cleanup
    conn.execute(&format!("DROP TABLE IF EXISTS {}", table_name), &[])
        .await
        .expect("Failed to drop test table");
}

#[tokio::test]
#[ignore = "requires running ClickHouse server"]
async fn test_generate_ddl() {
    let driver = ClickHouseDriver::new();
    let config = test_config();

    let conn = driver.connect(&config).await.expect("Failed to connect");
    let schema = conn
        .as_schema_introspection()
        .expect("Should support schema introspection");

    // Create a test table
    let table_name = format!("test_ddl_{}", uuid::Uuid::new_v4().simple());
    conn.execute(
        &format!(
            "CREATE TABLE {} (
                id UInt64,
                data String
            ) ENGINE = MergeTree() ORDER BY id",
            table_name
        ),
        &[],
    )
    .await
    .expect("Failed to create test table");

    // Generate DDL
    let object = zqlz_core::DatabaseObject {
        object_type: zqlz_core::ObjectType::Table,
        schema: Some("default".to_string()),
        name: table_name.clone(),
    };

    let ddl = schema
        .generate_ddl(&object)
        .await
        .expect("generate_ddl failed");
    assert!(
        ddl.contains("CREATE TABLE"),
        "DDL should contain CREATE TABLE"
    );
    assert!(ddl.contains(&table_name), "DDL should contain table name");
    assert!(ddl.contains("MergeTree"), "DDL should contain engine");

    // Cleanup
    conn.execute(&format!("DROP TABLE IF EXISTS {}", table_name), &[])
        .await
        .expect("Failed to drop test table");
}
