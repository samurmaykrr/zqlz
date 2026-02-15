//! Core test fixtures and utilities for parameterized database driver testing.
//!
//! This module provides the foundational infrastructure for running tests across
//! multiple database drivers using a unified approach. It handles connection
//! management, test data setup/cleanup, and provides convenient fixtures for
//! parameterized testing with rstest.
//!
//! # Architecture
//!
//! Tests use the TestDriver enum to identify which database to test against.
//! The fixtures automatically manage Docker containers using testcontainers-rs,
//! so no manual setup is needed. Containers are started automatically on first
//! test execution and cleaned up when the test process exits.
//!
//! For manual container management via docker-compose, you can still use:
//! ```bash
//! ./manage-test-env.sh up
//! ```
//! Set the ZQLZ_TEST_MANUAL_CONTAINERS environment variable to use manually managed
//! containers instead of automatic testcontainers.
//!
//! Each test receives a fresh connection and can optionally request cleanup
//! of test data.
//!
//! # Usage
//!
//! ```rust,ignore
//! use zqlz_driver_tests::fixtures::{TestDriver, test_connection};
//! use rstest::rstest;
//!
//! #[rstest]
//! #[case::postgres(TestDriver::Postgres)]
//! #[case::mysql(TestDriver::Mysql)]
//! #[case::sqlite(TestDriver::Sqlite)]
//! async fn test_basic_connection(#[case] driver: TestDriver) {
//!     let conn = test_connection(driver).await.unwrap();
//!     let result = conn.query("SELECT 1", &[]).await.unwrap();
//!     assert_eq!(result.rows.len(), 1);
//! }
//!
//! // Or use the values attribute:
//! #[rstest]
//! async fn test_insert_actor(
//!     #[values(TestDriver::Postgres, TestDriver::Mysql, TestDriver::Sqlite)]
//!     driver: TestDriver
//! ) {
//!     let conn = test_connection(driver).await.unwrap();
//!     // test code...
//! }
//! ```

use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use std::env;
use std::sync::{Arc, Mutex};
use zqlz_core::{Connection, ConnectionConfig, DatabaseDriver};
use zqlz_driver_mysql::MySqlDriver;
use zqlz_driver_postgres::PostgresDriver;
use zqlz_driver_redis::RedisDriver;
use zqlz_driver_sqlite::SqliteDriver;

use crate::test_containers::{mysql_container, postgres_container, redis_container};

/// Test driver identifier for parameterized testing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TestDriver {
    /// PostgreSQL with Pagila sample database
    Postgres,
    /// MySQL with Sakila sample database
    Mysql,
    /// SQLite with Sakila sample database (file-based)
    Sqlite,
    /// Redis for connection and pooling tests
    Redis,
}

impl TestDriver {
    /// Get the driver name as a string
    pub fn name(&self) -> &'static str {
        match self {
            TestDriver::Postgres => "postgresql",
            TestDriver::Mysql => "mysql",
            TestDriver::Sqlite => "sqlite",
            TestDriver::Redis => "redis",
        }
    }

    /// Get the display name
    pub fn display_name(&self) -> &'static str {
        match self {
            TestDriver::Postgres => "PostgreSQL",
            TestDriver::Mysql => "MySQL",
            TestDriver::Sqlite => "SQLite",
            TestDriver::Redis => "Redis",
        }
    }

    /// Check if this driver supports SQL operations
    pub fn is_sql_driver(&self) -> bool {
        matches!(
            self,
            TestDriver::Postgres | TestDriver::Mysql | TestDriver::Sqlite
        )
    }

    /// Check if this driver supports relational database features
    pub fn is_relational(&self) -> bool {
        self.is_sql_driver()
    }
}

/// Shared SQLite database path for tests
static SQLITE_PATH: Lazy<Mutex<Option<String>>> = Lazy::new(|| Mutex::new(None));

/// Get or create the SQLite test database
fn get_sqlite_path() -> Result<String> {
    let mut path_guard = SQLITE_PATH
        .lock()
        .map_err(|e| anyhow::anyhow!("failed to lock sqlite path: {}", e))?;

    if let Some(path) = &*path_guard {
        return Ok(path.clone());
    }

    tracing::info!("creating SQLite test database from template");
    
    let current_dir = std::env::current_dir().unwrap();
    let template_path = if current_dir.ends_with("zqlz-driver-tests") {
        // Running from crate directory
        current_dir.join("docker/sqlite/sakila-template.db")
    } else {
        // Running from workspace root
        current_dir.join("crates/zqlz-driver-tests/docker/sqlite/sakila-template.db")
    };

    tracing::info!(
        current_dir = %current_dir.display(),
        template_path = %template_path.display(),
        exists = template_path.exists(),
        "checking for SQLite template"
    );

    if !template_path.exists() {
        tracing::warn!(
            "SQLite template not found at {:?}, creating empty database. \
             Run: cd crates/zqlz-driver-tests && docker/sqlite/init-sakila.sh",
            template_path
        );
        
        let temp_dir = tempfile::tempdir().context("failed to create temp directory")?;
        let db_path = temp_dir.path().join("sakila.db");
        let db_path_str = db_path
            .to_str()
            .context("invalid SQLite path")?
            .to_string();

        *path_guard = Some(db_path_str.clone());
        std::mem::forget(temp_dir);

        tracing::info!(path = %db_path_str, "Empty SQLite database created (no test data)");
        return Ok(db_path_str);
    }

    let temp_dir = tempfile::tempdir().context("failed to create temp directory")?;
    let db_path = temp_dir.path().join("sakila.db");
    
    std::fs::copy(&template_path, &db_path)
        .context("failed to copy SQLite template database")?;

    let db_path_str = db_path
        .to_str()
        .context("invalid SQLite path")?
        .to_string();

    *path_guard = Some(db_path_str.clone());
    std::mem::forget(temp_dir);

    tracing::info!(path = %db_path_str, "SQLite database created from template with Sakila data");
    Ok(db_path_str)
}

/// Check if tests should use manually managed containers instead of testcontainers
///
/// Set ZQLZ_TEST_MANUAL_CONTAINERS=1 to use docker-compose managed containers.
fn use_manual_containers() -> bool {
    env::var("ZQLZ_TEST_MANUAL_CONTAINERS")
        .ok()
        .and_then(|v| v.parse::<u8>().ok())
        .map(|v| v != 0)
        .unwrap_or(false)
}

/// Verify that test database has required sample data loaded
///
/// This function checks that the Sakila/Pagila tables exist and contain data.
/// It's called after connecting to ensure the database is properly initialized.
async fn verify_test_data(conn: &Arc<dyn Connection>, driver: TestDriver) -> Result<()> {
    if !driver.is_sql_driver() {
        return Ok(());
    }

    tracing::debug!(driver = %driver.name(), "verifying test database has sample data");

    let result = conn
        .query("SELECT COUNT(*) FROM actor", &[])
        .await
        .context("failed to query actor table - database may not be initialized")?;

    if result.rows.is_empty() {
        return Err(anyhow::anyhow!(
            "actor table exists but returned no rows - database not initialized"
        ));
    }

    let count = result.rows[0]
        .get(0)
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    if count == 0 {
        return Err(anyhow::anyhow!(
            "actor table is empty - Sakila/Pagila data not loaded. \
             For automatic containers, this should not happen. \
             For manual containers, run: ./manage-test-env.sh up"
        ));
    }

    tracing::debug!(
        driver = %driver.name(),
        actor_count = count,
        "test database verification successful"
    );

    Ok(())
}

/// Wait for database to be ready with exponential backoff
///
/// This function attempts to connect and verify data multiple times with
/// increasing delays between attempts. Useful when databases are still
/// initializing after container startup.
async fn wait_for_database_ready(
    driver: TestDriver,
    max_attempts: u32,
    base_delay_secs: u64,
) -> Result<Arc<dyn Connection>> {
    let mut last_error = None;

    for attempt in 1..=max_attempts {
        match try_connect_and_verify(driver).await {
            Ok(conn) => {
                if attempt > 1 {
                    tracing::info!(
                        driver = %driver.name(),
                        attempts = attempt,
                        "database ready after retry"
                    );
                }
                return Ok(conn);
            }
            Err(e) => {
                last_error = Some(e);

                if attempt < max_attempts {
                    let delay = std::time::Duration::from_secs(base_delay_secs * attempt as u64);
                    tracing::warn!(
                        driver = %driver.name(),
                        attempt = attempt,
                        max_attempts = max_attempts,
                        delay_secs = delay.as_secs(),
                        "database not ready, retrying..."
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    Err(last_error.unwrap().context(format!(
        "database not ready after {} attempts",
        max_attempts
    )))
}

/// Try to connect and verify database in one attempt
async fn try_connect_and_verify(driver: TestDriver) -> Result<Arc<dyn Connection>> {
    let conn = connect_without_verification(driver).await?;
    verify_test_data(&conn, driver).await?;
    Ok(conn)
}

/// Connect to database without verification (internal helper)
async fn connect_without_verification(driver: TestDriver) -> Result<Arc<dyn Connection>> {
    match driver {
        TestDriver::Postgres => {
            let (host, port, database, username, password) = if use_manual_containers() {
                (
                    "127.0.0.1".to_string(),
                    5433,
                    "pagila".to_string(),
                    "test_user".to_string(),
                    "test_password".to_string(),
                )
            } else {
                let container_info = postgres_container().await.context(
                    "failed to start PostgreSQL container - is Docker running?",
                )?;
                (
                    container_info.host,
                    container_info.port,
                    container_info
                        .database
                        .unwrap_or_else(|| "postgres".to_string()),
                    container_info
                        .username
                        .unwrap_or_else(|| "postgres".to_string()),
                    container_info
                        .password
                        .unwrap_or_else(|| "postgres".to_string()),
                )
            };

            let mut config = ConnectionConfig::new_postgres(&host, port, &database, &username);
            config.password = Some(password);

            let driver = PostgresDriver::new();
            driver
                .connect(&config)
                .await
                .context("failed to connect to PostgreSQL")
        }
        TestDriver::Mysql => {
            let (host, port, database, username, password) = if use_manual_containers() {
                (
                    "127.0.0.1".to_string(),
                    3307,
                    "sakila".to_string(),
                    "test_user".to_string(),
                    "test_password".to_string(),
                )
            } else {
                let container_info =
                    mysql_container()
                        .await
                        .context("failed to start MySQL container - is Docker running?")?;
                (
                    container_info.host,
                    container_info.port,
                    container_info
                        .database
                        .unwrap_or_else(|| "test".to_string()),
                    container_info
                        .username
                        .unwrap_or_else(|| "test".to_string()),
                    container_info
                        .password
                        .unwrap_or_else(|| "test".to_string()),
                )
            };

            let mut config = ConnectionConfig::new_mysql(&host, port, &database, &username);
            config.password = Some(password);

            let driver = MySqlDriver::new();
            driver.connect(&config).await.context("failed to connect to MySQL")
        }
        TestDriver::Sqlite => {
            let db_path = get_sqlite_path()?;
            let config = ConnectionConfig::new_sqlite(&db_path);

            let driver = SqliteDriver::new();
            driver
                .connect(&config)
                .await
                .context("failed to connect to SQLite")
        }
        TestDriver::Redis => {
            let (host, port) = if use_manual_containers() {
                ("127.0.0.1".to_string(), 6380)
            } else {
                let container_info =
                    redis_container()
                        .await
                        .context("failed to start Redis container - is Docker running?")?;
                (container_info.host, container_info.port)
            };

            let mut config = ConnectionConfig::new("redis", "Redis Test");
            config.host = host;
            config.port = port;

            let driver = RedisDriver::new();
            driver.connect(&config).await.context("failed to connect to Redis")
        }
    }
}

/// Create a test connection for the specified driver
///
/// This function automatically manages Docker containers using testcontainers-rs.
/// Containers are started on first access, initialized with Sakila/Pagila sample data,
/// and cleaned up when the test process exits.
///
/// The function includes automatic retry logic with exponential backoff to handle
/// cases where databases are still initializing. It also verifies that sample data
/// is properly loaded before returning the connection.
///
/// To use manually managed containers (via docker-compose), set the environment variable:
/// ```bash
/// export ZQLZ_TEST_MANUAL_CONTAINERS=1
/// ./manage-test-env.sh up
/// cargo test
/// ```
///
/// Automatic container details (initialized with full sample data):
/// - PostgreSQL: localhost:<random>, database=test, ~200 actors, ~1000 films
/// - MySQL: localhost:<random>, database=test, ~200 actors, ~1000 films  
/// - SQLite: temporary file (currently empty, needs Phase 4 fix)
/// - Redis: localhost:<random>
///
/// Manual container details (from docker-compose.test.yml):
/// - PostgreSQL: localhost:5433, database=pagila, user=test_user, password=test_password
/// - MySQL: localhost:3307, database=sakila, user=test_user, password=test_password
/// - Redis: localhost:6380
/// - SQLite: temporary file
///
/// # Errors
///
/// Returns an error if:
/// - Docker daemon is not running
/// - Container fails to start or initialize
/// - Database connection fails after retries
/// - Sample data verification fails
/// - Configuration is invalid
pub async fn test_connection(driver: TestDriver) -> Result<Arc<dyn Connection>> {
    initialize_logging();

    let (max_attempts, base_delay_secs) = if use_manual_containers() {
        // Manual docker-compose MySQL uses a temporary init server that restarts,
        // so first successful pings can still be followed by brief disconnect windows.
        (12, 2)
    } else {
        (5, 2)
    };

    wait_for_database_ready(driver, max_attempts, base_delay_secs).await
}

/// Clean up test data for a specific driver
///
/// This function removes any test data created during tests. For SQL drivers,
/// it can truncate or delete test-specific data. For Redis, it flushes the
/// test database.
///
/// Note: This does NOT stop Docker containers - they are managed separately
/// via docker-compose.
pub async fn cleanup(driver: TestDriver, conn: &Arc<dyn Connection>) -> Result<()> {
    match driver {
        TestDriver::Postgres | TestDriver::Mysql | TestDriver::Sqlite => {
            tracing::debug!(driver = %driver.name(), "cleaning up SQL test data");
            // For now, we don't cleanup between tests as we use the sample databases
            // which are reset when containers restart. Individual tests can implement
            // their own cleanup using transactions or DELETE statements.
            Ok(())
        }
        TestDriver::Redis => {
            tracing::debug!("flushing Redis test database");
            conn.execute("FLUSHDB", &[])
                .await
                .context("failed to flush Redis database")?;
            Ok(())
        }
    }
}

/// Returns all test drivers (Postgres, MySQL, SQLite, Redis)
///
/// Use this when you need to test across all supported drivers including
/// non-SQL databases like Redis.
pub fn all_drivers() -> Vec<TestDriver> {
    vec![
        TestDriver::Postgres,
        TestDriver::Mysql,
        TestDriver::Sqlite,
        TestDriver::Redis,
    ]
}

/// Returns only SQL drivers (Postgres, MySQL, SQLite)
///
/// Use this for tests that require SQL functionality. This is the most
/// common case for CRUD and query tests.
pub fn sql_drivers() -> Vec<TestDriver> {
    vec![
        TestDriver::Postgres,
        TestDriver::Mysql,
        TestDriver::Sqlite,
    ]
}

/// Returns only relational database drivers
///
/// Alias for sql_drivers() - use whichever name makes more sense in context.
pub fn relational_drivers() -> Vec<TestDriver> {
    sql_drivers()
}

/// Initialize logging for tests if not already initialized
///
/// This sets up tracing with appropriate filters for test output.
fn initialize_logging() {
    use std::sync::Once;
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("zqlz=debug".parse().unwrap())
                    .add_directive("zqlz_driver_tests=debug".parse().unwrap()),
            )
            .with_test_writer()
            .finish();

        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_driver_properties() {
        assert_eq!(TestDriver::Postgres.name(), "postgresql");
        assert_eq!(TestDriver::Mysql.name(), "mysql");
        assert_eq!(TestDriver::Sqlite.name(), "sqlite");
        assert_eq!(TestDriver::Redis.name(), "redis");

        assert!(TestDriver::Postgres.is_sql_driver());
        assert!(TestDriver::Mysql.is_sql_driver());
        assert!(TestDriver::Sqlite.is_sql_driver());
        assert!(!TestDriver::Redis.is_sql_driver());
    }

    #[test]
    fn test_driver_collections() {
        assert_eq!(all_drivers().len(), 4);
        assert_eq!(sql_drivers().len(), 3);
        assert_eq!(relational_drivers().len(), 3);
    }

    #[tokio::test]
    async fn test_postgres_connection() {
        let result = test_connection(TestDriver::Postgres).await;
        match result {
            Ok(conn) => {
                assert_eq!(conn.driver_name(), "postgresql");
                let query_result = conn.query("SELECT 1 as num", &[]).await;
                assert!(
                    query_result.is_ok(),
                    "Query should succeed: {:?}",
                    query_result.err()
                );
            }
            Err(e) => {
                eprintln!(
                    "PostgreSQL container may not be running. Start with: ./manage-test-env.sh up\nError: {}",
                    e
                );
            }
        }
    }

    #[tokio::test]
    async fn test_mysql_connection() {
        let result = test_connection(TestDriver::Mysql).await;
        match result {
            Ok(conn) => {
                assert_eq!(conn.driver_name(), "mysql");
                let query_result = conn.query("SELECT 1 as num", &[]).await;
                assert!(
                    query_result.is_ok(),
                    "Query should succeed: {:?}",
                    query_result.err()
                );
            }
            Err(e) => {
                eprintln!(
                    "MySQL container may not be running. Start with: ./manage-test-env.sh up\nError: {}",
                    e
                );
            }
        }
    }

    #[tokio::test]
    async fn test_sqlite_connection() {
        let result = test_connection(TestDriver::Sqlite).await;
        assert!(
            result.is_ok(),
            "SQLite should always work: {}",
            result.as_ref().err().map(|e| e.to_string()).unwrap_or_default()
        );
        if let Ok(conn) = result {
            assert_eq!(conn.driver_name(), "sqlite");
            let query_result = conn.query("SELECT 1 as num", &[]).await;
            assert!(
                query_result.is_ok(),
                "Query should succeed: {:?}",
                query_result.err()
            );
        }
    }

    #[tokio::test]
    async fn test_redis_connection() {
        let result = test_connection(TestDriver::Redis).await;
        match result {
            Ok(conn) => {
                assert_eq!(conn.driver_name(), "redis");
            }
            Err(e) => {
                eprintln!(
                    "Redis container may not be running. Start with: ./manage-test-env.sh up\nError: {}",
                    e
                );
            }
        }
    }
}
