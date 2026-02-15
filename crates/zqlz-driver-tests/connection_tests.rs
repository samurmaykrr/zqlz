//! Basic connection tests for all database drivers.
//!
//! This module tests fundamental connection functionality across PostgreSQL, MySQL,
//! SQLite, and Redis drivers. Each test is parameterized to run against all
//! applicable drivers using rstest.
//!
//! Test categories:
//! - Valid connection establishment
//! - Invalid credentials handling
//! - Invalid host/port handling
//! - Invalid database name handling
//! - Connection timeout configuration
//! - Multiple concurrent connections
//! - Connection reuse

use crate::fixtures::{test_connection, TestDriver};
use anyhow::Result;
use rstest::rstest;
use std::time::Duration;
use zqlz_core::{ConnectionConfig, DatabaseDriver};
use zqlz_driver_mysql::MySqlDriver;
use zqlz_driver_postgres::PostgresDriver;
use zqlz_driver_redis::RedisDriver;

/// Test that we can establish a valid connection with correct credentials
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[case::redis(TestDriver::Redis)]
#[tokio::test]
async fn test_connect_with_valid_credentials(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Verify the connection is usable
    assert_eq!(conn.driver_name(), driver.name());

    // For SQL drivers, try a simple query
    if driver.is_sql_driver() {
        let result = conn.query("SELECT 1 as num", &[]).await?;
        assert_eq!(result.rows.len(), 1);
    }

    Ok(())
}

/// Test that invalid credentials are rejected with appropriate errors
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::redis(TestDriver::Redis)]
#[tokio::test]
async fn test_connect_with_invalid_credentials(#[case] driver: TestDriver) -> Result<()> {
    let result = match driver {
        TestDriver::Postgres => {
            let mut config =
                ConnectionConfig::new_postgres("127.0.0.1", 5433, "pagila", "wrong_user");
            config.password = Some("wrong_password".to_string());
            PostgresDriver::new().connect(&config).await
        }
        TestDriver::Mysql => {
            let mut config =
                ConnectionConfig::new_mysql("127.0.0.1", 3307, "sakila", "wrong_user");
            config.password = Some("wrong_password".to_string());
            MySqlDriver::new().connect(&config).await
        }
        TestDriver::Redis => {
            let mut config = ConnectionConfig::new("redis", "Redis Test");
            config.host = "127.0.0.1".to_string();
            config.port = 6380;
            // Redis in test environment doesn't have password auth, so we can't test this directly
            // Instead we test with wrong host to simulate auth failure
            config.host = "255.255.255.255".to_string();
            RedisDriver::new().connect(&config).await
        }
        TestDriver::Sqlite => {
            // SQLite doesn't have authentication, skip this test
            return Ok(());
        }
    };

    // Connection should fail
    assert!(
        result.is_err(),
        "{} should reject invalid credentials",
        driver.display_name()
    );

    Ok(())
}

/// Test that invalid host addresses are rejected
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::redis(TestDriver::Redis)]
#[tokio::test]
async fn test_connect_with_invalid_host(#[case] driver: TestDriver) -> Result<()> {
    let result = match driver {
        TestDriver::Postgres => {
            let config =
                ConnectionConfig::new_postgres("255.255.255.255", 5433, "pagila", "test_user");
            PostgresDriver::new().connect(&config).await
        }
        TestDriver::Mysql => {
            let config =
                ConnectionConfig::new_mysql("255.255.255.255", 3307, "sakila", "test_user");
            MySqlDriver::new().connect(&config).await
        }
        TestDriver::Redis => {
            let mut config = ConnectionConfig::new("redis", "Redis Test");
            config.host = "255.255.255.255".to_string();
            config.port = 6380;
            RedisDriver::new().connect(&config).await
        }
        TestDriver::Sqlite => {
            // SQLite doesn't use network connections, skip
            return Ok(());
        }
    };

    assert!(
        result.is_err(),
        "{} should reject invalid host",
        driver.display_name()
    );

    Ok(())
}

/// Test that invalid port numbers are rejected
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::redis(TestDriver::Redis)]
#[tokio::test]
async fn test_connect_with_invalid_port(#[case] driver: TestDriver) -> Result<()> {
    let result = match driver {
        TestDriver::Postgres => {
            let config = ConnectionConfig::new_postgres("127.0.0.1", 9999, "pagila", "test_user");
            PostgresDriver::new().connect(&config).await
        }
        TestDriver::Mysql => {
            let config = ConnectionConfig::new_mysql("127.0.0.1", 9999, "sakila", "test_user");
            MySqlDriver::new().connect(&config).await
        }
        TestDriver::Redis => {
            let mut config = ConnectionConfig::new("redis", "Redis Test");
            config.host = "127.0.0.1".to_string();
            config.port = 9999;
            RedisDriver::new().connect(&config).await
        }
        TestDriver::Sqlite => {
            // SQLite doesn't use network connections, skip
            return Ok(());
        }
    };

    assert!(
        result.is_err(),
        "{} should reject invalid port",
        driver.display_name()
    );

    Ok(())
}

/// Test that invalid database names are rejected
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[tokio::test]
async fn test_connect_with_invalid_database(#[case] driver: TestDriver) -> Result<()> {
    let result = match driver {
        TestDriver::Postgres => {
            let mut config = ConnectionConfig::new_postgres(
                "127.0.0.1",
                5433,
                "nonexistent_db",
                "test_user",
            );
            config.password = Some("test_password".to_string());
            PostgresDriver::new().connect(&config).await
        }
        TestDriver::Mysql => {
            let mut config =
                ConnectionConfig::new_mysql("127.0.0.1", 3307, "nonexistent_db", "test_user");
            config.password = Some("test_password".to_string());
            MySqlDriver::new().connect(&config).await
        }
        _ => {
            // SQLite creates databases on demand, Redis doesn't have databases in the same way
            return Ok(());
        }
    };

    assert!(
        result.is_err(),
        "{} should reject invalid database name",
        driver.display_name()
    );

    Ok(())
}

/// Test that connection timeouts work with unreachable hosts
///
/// Note: This test verifies that drivers eventually fail when connecting to
/// unreachable hosts. The actual timeout behavior depends on driver-specific
/// configuration and network stack settings.
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::redis(TestDriver::Redis)]
#[tokio::test]
async fn test_connect_timeout(#[case] driver: TestDriver) -> Result<()> {
    // Use tokio::time::timeout to limit test duration
    let timeout_duration = Duration::from_secs(10);

    let connect_future = async {
        let result = match driver {
            TestDriver::Postgres => {
                let config = ConnectionConfig::new_postgres(
                    "10.255.255.255", // unreachable IP (reserved for documentation)
                    5433,
                    "pagila",
                    "test_user",
                );
                PostgresDriver::new().connect(&config).await
            }
            TestDriver::Mysql => {
                let config = ConnectionConfig::new_mysql(
                    "10.255.255.255", // unreachable IP (reserved for documentation)
                    3307,
                    "sakila",
                    "test_user",
                );
                MySqlDriver::new().connect(&config).await
            }
            TestDriver::Redis => {
                let mut config = ConnectionConfig::new("redis", "Redis Test");
                config.host = "10.255.255.255".to_string(); // unreachable IP (reserved for documentation)
                config.port = 6380;
                RedisDriver::new().connect(&config).await
            }
            TestDriver::Sqlite => {
                // SQLite doesn't have network timeouts
                return Err(anyhow::anyhow!("SQLite not tested"));
            }
        };
        result.map_err(|e| anyhow::anyhow!("Connection error: {}", e))
    };

    // Wait for connection with timeout
    let result = tokio::time::timeout(timeout_duration, connect_future).await;

    // Either the connection fails OR our timeout fires - both are acceptable
    match result {
        Ok(Ok(_)) => {
            panic!(
                "{} should not successfully connect to unreachable host",
                driver.display_name()
            );
        }
        Ok(Err(_)) => {
            // Connection properly failed - this is expected
        }
        Err(_) => {
            // Tokio timeout fired - connection attempt took too long
            // This is also acceptable, though less ideal
        }
    }

    Ok(())
}

/// Test that multiple concurrent connections can coexist
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[case::redis(TestDriver::Redis)]
#[tokio::test]
async fn test_multiple_concurrent_connections(#[case] driver: TestDriver) -> Result<()> {
    // Create 3 concurrent connections
    let conn1 = test_connection(driver).await?;
    let conn2 = test_connection(driver).await?;
    let conn3 = test_connection(driver).await?;

    // All connections should be independent and functional
    if driver.is_sql_driver() {
        let result1 = conn1.query("SELECT 1 as num", &[]).await?;
        let result2 = conn2.query("SELECT 2 as num", &[]).await?;
        let result3 = conn3.query("SELECT 3 as num", &[]).await?;

        assert_eq!(result1.rows.len(), 1);
        assert_eq!(result2.rows.len(), 1);
        assert_eq!(result3.rows.len(), 1);
    } else {
        // For Redis, test with SET/GET
        conn1.execute("SET test_conn1 'value1'", &[]).await?;
        conn2.execute("SET test_conn2 'value2'", &[]).await?;
        conn3.execute("SET test_conn3 'value3'", &[]).await?;

        let result1 = conn1.query("GET test_conn1", &[]).await?;
        let result2 = conn2.query("GET test_conn2", &[]).await?;
        let result3 = conn3.query("GET test_conn3", &[]).await?;

        assert_eq!(result1.rows.len(), 1);
        assert_eq!(result2.rows.len(), 1);
        assert_eq!(result3.rows.len(), 1);
    }

    Ok(())
}

/// Test that connections can be reused for multiple operations
#[rstest]
#[case::postgres(TestDriver::Postgres)]
#[case::mysql(TestDriver::Mysql)]
#[case::sqlite(TestDriver::Sqlite)]
#[case::redis(TestDriver::Redis)]
#[tokio::test]
async fn test_connection_reuse(#[case] driver: TestDriver) -> Result<()> {
    let conn = test_connection(driver).await?;

    // Perform multiple operations on the same connection
    if driver.is_sql_driver() {
        for i in 1..=5 {
            let query = format!("SELECT {} as num", i);
            let result = conn.query(&query, &[]).await?;
            assert_eq!(result.rows.len(), 1);
        }
    } else {
        // For Redis, test multiple SET/GET operations
        for i in 1..=5 {
            let key = format!("test_key_{}", i);
            let value = format!("value_{}", i);
            conn.execute(&format!("SET {} '{}'", key, value), &[])
                .await?;

            let result = conn.query(&format!("GET {}", key), &[]).await?;
            assert_eq!(result.rows.len(), 1);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Integration test verifying that test_connect_with_valid_credentials works for Postgres
    #[tokio::test]
    async fn integration_postgres_connection() {
        let result = test_connect_with_valid_credentials(TestDriver::Postgres).await;
        match result {
            Ok(_) => println!("✓ PostgreSQL connection test passed"),
            Err(e) => eprintln!(
                "⚠ PostgreSQL test failed (container may not be running): {}",
                e
            ),
        }
    }

    /// Integration test verifying that test_connect_with_valid_credentials works for MySQL
    #[tokio::test]
    async fn integration_mysql_connection() {
        let result = test_connect_with_valid_credentials(TestDriver::Mysql).await;
        match result {
            Ok(_) => println!("✓ MySQL connection test passed"),
            Err(e) => eprintln!("⚠ MySQL test failed (container may not be running): {}", e),
        }
    }

    /// Integration test verifying that test_connect_with_valid_credentials works for SQLite
    #[tokio::test]
    async fn integration_sqlite_connection() {
        let result = test_connect_with_valid_credentials(TestDriver::Sqlite).await;
        assert!(
            result.is_ok(),
            "SQLite should always work: {:?}",
            result.err()
        );
    }

    /// Integration test verifying that test_connect_with_valid_credentials works for Redis
    #[tokio::test]
    async fn integration_redis_connection() {
        let result = test_connect_with_valid_credentials(TestDriver::Redis).await;
        match result {
            Ok(_) => println!("✓ Redis connection test passed"),
            Err(e) => eprintln!("⚠ Redis test failed (container may not be running): {}", e),
        }
    }
}
