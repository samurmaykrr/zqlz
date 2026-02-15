//! Integration tests for ClickHouse connection
//!
//! These tests require a running ClickHouse server.
//! They are ignored by default and can be run with:
//! ```
//! cargo test --package zqlz-driver-clickhouse --test connection_integration -- --ignored
//! ```
//!
//! To set up a local ClickHouse server for testing:
//! ```
//! docker run -d --name clickhouse-test -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
//! ```

use zqlz_core::{Connection, ConnectionConfig, DatabaseDriver, Value};
use zqlz_driver_clickhouse::ClickHouseDriver;

/// Helper to create a test connection config
fn test_config() -> ConnectionConfig {
    let mut config = ConnectionConfig::new("clickhouse", "ClickHouse Test");
    config.host = std::env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".to_string());
    config.port = std::env::var("CLICKHOUSE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8123);
    config.database =
        Some(std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".to_string()));
    config.username =
        Some(std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string()));
    config.password = std::env::var("CLICKHOUSE_PASSWORD").ok();
    config
}

/// Test executing a simple analytical query on ClickHouse
#[tokio::test]
#[ignore = "requires running ClickHouse server"]
async fn test_clickhouse_analytical_query() {
    let driver = ClickHouseDriver::new();
    let config = test_config();

    // Connect
    let conn = driver
        .connect(&config)
        .await
        .expect("Failed to connect to ClickHouse");

    // Execute a simple analytical query using ClickHouse's built-in system tables
    // This tests the connection and query functionality without needing to create tables
    let result = conn
        .query(
            "SELECT database, name, engine FROM system.tables LIMIT 10",
            &[],
        )
        .await
        .expect("Query failed");

    // Verify we got results
    assert!(!result.columns.is_empty(), "Expected columns in result");
    assert!(
        result.columns.iter().any(|c| c.name == "database"),
        "Expected 'database' column"
    );
    assert!(
        result.columns.iter().any(|c| c.name == "name"),
        "Expected 'name' column"
    );
    assert!(
        result.columns.iter().any(|c| c.name == "engine"),
        "Expected 'engine' column"
    );

    // Close connection
    conn.close().await.expect("Failed to close connection");
    assert!(conn.is_closed());
}

/// Test creating a MergeTree table and inserting data
#[tokio::test]
#[ignore = "requires running ClickHouse server"]
async fn test_clickhouse_create_table_and_insert() {
    let driver = ClickHouseDriver::new();
    let config = test_config();

    let conn = driver
        .connect(&config)
        .await
        .expect("Failed to connect to ClickHouse");

    // Drop table if exists
    let _ = conn
        .execute("DROP TABLE IF EXISTS test_zqlz_events", &[])
        .await;

    // Create a MergeTree table (ClickHouse's main table engine)
    conn.execute(
        r#"
        CREATE TABLE test_zqlz_events (
            event_date Date,
            event_time DateTime,
            user_id UInt32,
            event_type String,
            value Float64
        ) ENGINE = MergeTree()
        ORDER BY (event_date, user_id)
        "#,
        &[],
    )
    .await
    .expect("Failed to create table");

    // Insert data
    conn.execute(
        r#"
        INSERT INTO test_zqlz_events (event_date, event_time, user_id, event_type, value)
        VALUES
            ('2024-01-01', '2024-01-01 10:00:00', 1, 'click', 1.5),
            ('2024-01-01', '2024-01-01 10:05:00', 2, 'view', 2.0),
            ('2024-01-01', '2024-01-01 10:10:00', 1, 'purchase', 99.99)
        "#,
        &[],
    )
    .await
    .expect("Failed to insert data");

    // Query the data with aggregation (ClickHouse excels at this)
    let result = conn
        .query(
            r#"
            SELECT 
                user_id,
                count() as event_count,
                sum(value) as total_value
            FROM test_zqlz_events
            GROUP BY user_id
            ORDER BY user_id
            "#,
            &[],
        )
        .await
        .expect("Failed to query data");

    // Verify results
    assert_eq!(result.rows.len(), 2, "Expected 2 users");
    assert!(result.columns.iter().any(|c| c.name == "user_id"));
    assert!(result.columns.iter().any(|c| c.name == "event_count"));
    assert!(result.columns.iter().any(|c| c.name == "total_value"));

    // Clean up
    conn.execute("DROP TABLE IF EXISTS test_zqlz_events", &[])
        .await
        .expect("Failed to drop table");

    conn.close().await.expect("Failed to close connection");
}

/// Test ClickHouse-specific features: window functions
#[tokio::test]
#[ignore = "requires running ClickHouse server"]
async fn test_clickhouse_window_functions() {
    let driver = ClickHouseDriver::new();
    let config = test_config();

    let conn = driver
        .connect(&config)
        .await
        .expect("Failed to connect to ClickHouse");

    // Use a window function query on system tables
    let result = conn
        .query(
            r#"
            SELECT 
                database,
                name,
                row_number() OVER (PARTITION BY database ORDER BY name) as rn
            FROM system.tables
            WHERE database = 'system'
            LIMIT 5
            "#,
            &[],
        )
        .await
        .expect("Query failed");

    assert!(!result.rows.is_empty(), "Expected rows in result");
    assert!(
        result.columns.iter().any(|c| c.name == "rn"),
        "Expected 'rn' column from window function"
    );

    conn.close().await.expect("Failed to close connection");
}

/// Test connection state management
#[tokio::test]
#[ignore = "requires running ClickHouse server"]
async fn test_clickhouse_connection_state() {
    let driver = ClickHouseDriver::new();
    let config = test_config();

    let conn = driver
        .connect(&config)
        .await
        .expect("Failed to connect to ClickHouse");

    // Connection should not be closed initially
    assert!(!conn.is_closed());

    // Execute a query
    let _ = conn.query("SELECT 1", &[]).await.expect("Query failed");

    // Still not closed
    assert!(!conn.is_closed());

    // Close the connection
    conn.close().await.expect("Failed to close connection");

    // Now it should be closed
    assert!(conn.is_closed());

    // Attempting to execute should fail
    let result = conn.execute("SELECT 1", &[]).await;
    assert!(result.is_err(), "Expected error on closed connection");
}

/// Test driver metadata and connection string building
#[tokio::test]
async fn test_driver_metadata() {
    let driver = ClickHouseDriver::new();

    assert_eq!(driver.id(), "clickhouse");
    assert_eq!(driver.name(), "clickhouse");
    assert_eq!(driver.display_name(), "ClickHouse");
    assert_eq!(driver.default_port(), Some(8123));

    let config = test_config();
    let conn_str = driver.build_connection_string(&config);
    assert!(conn_str.starts_with("http://") || conn_str.starts_with("https://"));
}

/// Test test_connection method
#[tokio::test]
#[ignore = "requires running ClickHouse server"]
async fn test_clickhouse_test_connection() {
    let driver = ClickHouseDriver::new();
    let config = test_config();

    let result = driver.test_connection(&config).await;
    assert!(
        result.is_ok(),
        "test_connection should succeed: {:?}",
        result
    );
}

/// Test transaction not supported behavior
#[tokio::test]
#[ignore = "requires running ClickHouse server"]
async fn test_clickhouse_transaction_not_supported() {
    let driver = ClickHouseDriver::new();
    let config = test_config();

    let conn = driver
        .connect(&config)
        .await
        .expect("Failed to connect to ClickHouse");

    // ClickHouse has limited transaction support
    let result = conn.begin_transaction().await;
    assert!(result.is_err(), "Expected transaction to not be supported");

    conn.close().await.expect("Failed to close connection");
}
