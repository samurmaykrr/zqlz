//! Docker container management for integration tests.
//!
//! This module provides automatic Docker container lifecycle management using testcontainers-rs.
//! Containers are started automatically when tests begin and cleaned up when tests finish.
//!
//! # Architecture
//!
//! The test containers are managed using a lazy static initialization pattern. When the first
//! test requests a container, it is started, initialized with Sakila/Pagila sample data, and
//! cached for reuse across all tests. This provides:
//!
//! - Automatic container startup (no manual `./manage-test-env.sh up` needed)
//! - Full Sakila/Pagila sample data loaded automatically
//! - Container reuse across tests (faster test execution)
//! - Automatic cleanup when test process exits
//! - Thread-safe access to container information
//!
//! # Database Initialization
//!
//! Containers are initialized with sample data using embedded SQL scripts:
//! - PostgreSQL: Pagila schema + data (~200 actors, ~1000 films)
//! - MySQL: Sakila schema + data (~200 actors, ~1000 films)
//! - Redis: No initialization needed
//!
//! Initialization happens once per test run and includes health checks to verify
//! data is loaded correctly before tests begin.
//!
//! # Usage
//!
//! Tests should use `fixtures::test_connection()` which internally calls the container
//! management functions when needed. Direct usage is also possible:
//!
//! ```rust,ignore
//! use crate::test_containers::postgres_container;
//!
//! let container = postgres_container();
//! let port = container.port();
//! // Connect using the port...
//! ```

use once_cell::sync::Lazy;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::{mysql::Mysql, postgres::Postgres, redis::Redis};

/// Information about a running test container
#[derive(Clone)]
pub struct ContainerInfo {
    /// Host address (typically 127.0.0.1)
    pub host: String,
    /// Port number (randomly assigned by testcontainers)
    pub port: u16,
    /// Database name
    pub database: Option<String>,
    /// Username for authentication
    pub username: Option<String>,
    /// Password for authentication
    pub password: Option<String>,
    /// Whether the container has been initialized with sample data
    pub initialized: bool,
}

/// Postgres container with Pagila database
struct PostgresContainer {
    #[allow(dead_code)]
    inner: ContainerAsync<Postgres>,
    info: ContainerInfo,
}

/// MySQL container with Sakila database
struct MysqlContainer {
    #[allow(dead_code)]
    inner: ContainerAsync<Mysql>,
    info: ContainerInfo,
}

/// Redis container for connection/pooling tests
struct RedisContainer {
    #[allow(dead_code)]
    inner: ContainerAsync<Redis>,
    info: ContainerInfo,
}

/// Global Postgres container instance
static POSTGRES_CONTAINER: Lazy<Arc<Mutex<Option<PostgresContainer>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// Global MySQL container instance
static MYSQL_CONTAINER: Lazy<Arc<Mutex<Option<MysqlContainer>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// Global Redis container instance
static REDIS_CONTAINER: Lazy<Arc<Mutex<Option<RedisContainer>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// Initialize a PostgreSQL container with Pagila sample data using ZQLZ driver
async fn init_postgres_data(info: &ContainerInfo) -> anyhow::Result<()> {
    use zqlz_core::{ConnectionConfig, DatabaseDriver};
    use zqlz_driver_postgres::PostgresDriver;

    tracing::info!("initializing PostgreSQL container with Pagila data");

    let current_dir = std::env::current_dir().unwrap();
    let base_path = if current_dir.ends_with("zqlz-driver-tests") {
        current_dir.join("docker/postgres")
    } else {
        current_dir.join("crates/zqlz-driver-tests/docker/postgres")
    };
    
    let schema_path = base_path.join("pagila-schema.sql");
    let data_path = base_path.join("pagila-data.sql");

    if !schema_path.exists() || !data_path.exists() {
        return Err(anyhow::anyhow!(
            "Pagila SQL files not found at {:?}. Make sure you're running tests from workspace root.",
            base_path
        ));
    }

    let schema_sql = std::fs::read_to_string(&schema_path)
        .map_err(|e| anyhow::anyhow!("failed to read pagila-schema.sql: {}", e))?;
    let data_sql = std::fs::read_to_string(&data_path)
        .map_err(|e| anyhow::anyhow!("failed to read pagila-data.sql: {}", e))?;

    // Remove PostgreSQL version-specific commands that may not be supported
    let schema_sql = schema_sql
        .lines()
        .filter(|line| !line.contains("default_table_access_method"))
        .collect::<Vec<_>>()
        .join("\n");

    // Use ZQLZ PostgreSQL driver for initialization
    let mut config = ConnectionConfig::new_postgres(
        &info.host,
        info.port,
        info.database.as_ref().unwrap(),
        info.username.as_ref().unwrap()
    );
    config.password = info.password.clone();

    let driver = PostgresDriver::new();
    
    // Retry connection with exponential backoff
    let max_retries = 10;
    let mut conn = None;
    
    for attempt in 1..=max_retries {
        match driver.connect(&config).await {
            Ok(c) => {
                conn = Some(c);
                break;
            }
            Err(e) if attempt < max_retries => {
                let delay = Duration::from_secs(2u64.pow(attempt.min(5)));
                tracing::warn!(
                    attempt = attempt,
                    delay_secs = delay.as_secs(),
                    "PostgreSQL connection failed, retrying: {}",
                    e
                );
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(anyhow::anyhow!("failed to connect to PostgreSQL after {} attempts: {}", max_retries, e)),
        }
    }

    let conn = conn.unwrap();

    tracing::info!("loading Pagila schema (this may take 5-10 seconds)...");
    
    // Use ZQLZ transaction API for proper transaction management
    let txn = conn.begin_transaction().await
        .map_err(|e| anyhow::anyhow!("failed to begin transaction: {}", e))?;

     // Parse and execute schema statements
    let schema_statements = parse_sql_statements(&schema_sql);
    tracing::info!(total_schema_statements = schema_statements.len(), "Parsed schema statements");
    
    for (idx, stmt) in schema_statements.iter().enumerate() {
        if idx % 10 == 0 {
            tracing::info!(progress = idx, total = schema_statements.len(), "loading schema...");
        }
        
        // Skip DELIMITER commands (MySQL client-only command, not SQL)
        if stmt.trim().to_uppercase().starts_with("DELIMITER") {
            continue;
        }
        
        txn.execute(stmt, &[]).await
            .map_err(|e| anyhow::anyhow!("failed to execute schema statement {}: {} - Statement: {}", idx, e, &stmt[..stmt.len().min(100)]))?;
    }

    tracing::info!(statements = schema_statements.len(), "Schema statements executed");

    tracing::info!("loading Pagila data (this may take 30-60 seconds)...");
    
    // Parse and execute data statements
    let data_statements = parse_sql_statements(&data_sql);
    for (idx, stmt) in data_statements.iter().enumerate() {
        if idx % 100 == 0 && idx > 0 {
            tracing::debug!(progress = idx, total = data_statements.len(), "loading data...");
        }
        txn.execute(stmt, &[]).await
            .map_err(|e| anyhow::anyhow!("failed to execute data statement {}: {} - Statement: {}", idx, e, &stmt[..stmt.len().min(100)]))?;
    }
    
    tracing::info!(statements = data_statements.len(), "Data statements executed");

    // Verify data inside the transaction before committing
    // Use fully qualified table name since search_path may not be set in transaction context
    tracing::info!("Verifying actor table before commit (within transaction)...");
    
    let result_before_commit = txn.query("SELECT COUNT(*) FROM public.actor", &[]).await
        .map_err(|e| anyhow::anyhow!("failed to verify actor table before commit: {}", e))?;
    
    let actor_count_before = result_before_commit.rows.first()
        .and_then(|row| row.get(0))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    
    tracing::info!(actor_count = actor_count_before, "Actor count BEFORE commit");
    
    if actor_count_before == 0 {
        txn.rollback().await.ok();
        return Err(anyhow::anyhow!("Pagila data not loaded properly - actor table is empty before commit"));
    }

    // Commit the transaction explicitly
    tracing::info!("Committing transaction...");
    txn.commit().await
        .map_err(|e| anyhow::anyhow!("failed to commit transaction: {}", e))?;

    // Verify data loaded successfully using a NEW query (not in transaction)
    // Use fully qualified table name since search_path may not persist
    tracing::info!("Verifying actor table AFTER commit...");
    
    let result = conn.query("SELECT COUNT(*) FROM public.actor", &[]).await
        .map_err(|e| anyhow::anyhow!("failed to verify actor table: {}", e))?;
    
    let actor_count = result.rows.first()
        .and_then(|row| row.get(0))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    tracing::info!(
        actor_count = actor_count,
        "Pagila database initialized successfully"
    );

    if actor_count == 0 {
        return Err(anyhow::anyhow!("Pagila data not loaded properly - actor table is empty"));
    }

    Ok(())
}

/// Parse SQL file into individual statements
///
/// This function splits SQL content by semicolons, handling:
/// - Single-line comments (--) 
/// - Multi-line comments (/* */)
/// - Empty statements
/// - String literals that may contain semicolons (single quotes)
/// - Dollar-quoted strings used in PostgreSQL functions/procedures
fn parse_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current_stmt = String::new();
    let mut in_string = false;
    let mut in_single_line_comment = false;
    let mut in_multi_line_comment = false;
    let mut in_dollar_quote = false;
    let mut dollar_quote_tag = String::new();
    let bytes = sql.as_bytes();
    let mut i = 0;
    
    while i < bytes.len() {
        let ch = bytes[i] as char;
        
        // Handle single-line comments
        if !in_string && !in_multi_line_comment && !in_dollar_quote && ch == '-' && i + 1 < bytes.len() && bytes[i + 1] as char == '-' {
            in_single_line_comment = true;
            i += 2;
            continue;
        }
        
        if in_single_line_comment {
            if ch == '\n' {
                in_single_line_comment = false;
                current_stmt.push(ch);
            }
            i += 1;
            continue;
        }
        
        // Handle multi-line comments
        if !in_string && !in_dollar_quote && ch == '/' && i + 1 < bytes.len() && bytes[i + 1] as char == '*' {
            in_multi_line_comment = true;
            i += 2;
            continue;
        }
        
        if in_multi_line_comment {
            if ch == '*' && i + 1 < bytes.len() && bytes[i + 1] as char == '/' {
                in_multi_line_comment = false;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        
        // Handle dollar-quoted strings (PostgreSQL feature like $_$, $$, $body$, etc.)
        if !in_string && !in_multi_line_comment && ch == '$' {
            let mut tag_end = i + 1;
            
            // Collect the tag characters (alphanumeric or underscore)
            while tag_end < bytes.len() {
                let tag_ch = bytes[tag_end] as char;
                if tag_ch == '$' {
                    // Found closing dollar sign
                    let tag = &sql[i..=tag_end];
                    
                    if in_dollar_quote && tag == dollar_quote_tag {
                        // This is the closing tag
                        current_stmt.push_str(tag);
                        in_dollar_quote = false;
                        dollar_quote_tag.clear();
                        i = tag_end + 1;
                        break;
                    } else if !in_dollar_quote {
                        // This is an opening tag
                        current_stmt.push_str(tag);
                        in_dollar_quote = true;
                        dollar_quote_tag = tag.to_string();
                        i = tag_end + 1;
                        break;
                    } else {
                        // We're inside a different dollar quote, treat as regular text
                        current_stmt.push(ch);
                        i += 1;
                        break;
                    }
                } else if tag_ch.is_alphanumeric() || tag_ch == '_' {
                    tag_end += 1;
                } else {
                    // Not a valid dollar quote tag, treat $ as regular character
                    current_stmt.push(ch);
                    i += 1;
                    break;
                }
            }
            
            if tag_end >= bytes.len() {
                // Reached end of string without completing the tag
                current_stmt.push(ch);
                i += 1;
            }
            continue;
        }
        
        // Handle string literals (single quotes)
        if ch == '\'' && !in_multi_line_comment && !in_dollar_quote {
            in_string = !in_string;
            current_stmt.push(ch);
            i += 1;
            continue;
        }
        
        // Handle statement terminators
        if ch == ';' && !in_string && !in_multi_line_comment && !in_single_line_comment && !in_dollar_quote {
            let trimmed = current_stmt.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_string());
            }
            current_stmt.clear();
            i += 1;
            continue;
        }
        
        current_stmt.push(ch);
        i += 1;
    }
    
    // Add final statement if any
    let trimmed = current_stmt.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }
    
    statements
}

/// Initialize a MySQL container with Sakila sample data using ZQLZ driver
async fn init_mysql_data(info: &ContainerInfo) -> anyhow::Result<()> {
    use zqlz_core::{ConnectionConfig, DatabaseDriver};
    use zqlz_driver_mysql::MySqlDriver;

    tracing::info!("initializing MySQL container with Sakila data");

    let current_dir = std::env::current_dir().unwrap();
    let base_path = if current_dir.ends_with("zqlz-driver-tests") {
        current_dir.join("docker/mysql")
    } else {
        current_dir.join("crates/zqlz-driver-tests/docker/mysql")
    };
    
    let schema_path = base_path.join("sakila-schema.sql");
    let data_path = base_path.join("sakila-data.sql");

    if !schema_path.exists() || !data_path.exists() {
        return Err(anyhow::anyhow!(
            "Sakila SQL files not found at {:?}. Make sure you're running tests from workspace root.",
            base_path
        ));
    }

    let schema_sql = std::fs::read_to_string(&schema_path)
        .map_err(|e| anyhow::anyhow!("failed to read sakila-schema.sql: {}", e))?;
    let data_sql = std::fs::read_to_string(&data_path)
        .map_err(|e| anyhow::anyhow!("failed to read sakila-data.sql: {}", e))?;

    // Use ZQLZ MySQL driver for initialization
    let mut config = ConnectionConfig::new_mysql(
        &info.host,
        info.port,
        info.database.as_ref().unwrap(),
        info.username.as_ref().unwrap()
    );
    config.password = info.password.clone();

    let driver = MySqlDriver::new();
    
    // Retry connection with exponential backoff
    let max_retries = 10;
    let mut conn = None;
    
    for attempt in 1..=max_retries {
        match driver.connect(&config).await {
            Ok(c) => {
                conn = Some(c);
                break;
            }
            Err(e) if attempt < max_retries => {
                let delay = Duration::from_secs(2u64.pow(attempt.min(5)));
                tracing::warn!(
                    attempt = attempt,
                    delay_secs = delay.as_secs(),
                    "MySQL connection failed, retrying: {}",
                    e
                );
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(anyhow::anyhow!("failed to connect to MySQL after {} attempts: {}", max_retries, e)),
        }
    }

    let conn = conn.unwrap();

    tracing::info!("loading Sakila schema (this may take 5-10 seconds)...");
    
    // Disable foreign key checks during schema loading to avoid constraint order issues
    // This must be done before the transaction starts
    conn.execute("SET FOREIGN_KEY_CHECKS=0", &[]).await
        .map_err(|e| anyhow::anyhow!("failed to disable foreign key checks: {}", e))?;
    
    // Use ZQLZ transaction API for proper transaction management
    let txn = conn.begin_transaction().await
        .map_err(|e| anyhow::anyhow!("failed to begin transaction: {}", e))?;

    // Parse and execute schema statements
    let schema_statements = parse_sql_statements(&schema_sql);
    for (idx, stmt) in schema_statements.iter().enumerate() {
        if idx % 10 == 0 && idx > 0 {
            tracing::debug!(progress = idx, total = schema_statements.len(), "loading schema...");
        }
        
        // Skip DELIMITER commands (MySQL client-only command, not SQL)
        if stmt.trim().to_uppercase().starts_with("DELIMITER") {
            continue;
        }
        
        txn.execute(stmt, &[]).await
            .map_err(|e| anyhow::anyhow!("failed to execute schema statement {}: {} - Statement: {}", idx, e, &stmt[..stmt.len().min(100)]))?;
    }

    tracing::info!(statements = schema_statements.len(), "Schema statements executed");

    tracing::info!("loading Sakila data (this may take 30-60 seconds)...");
    
    // Parse and execute data statements
    let data_statements = parse_sql_statements(&data_sql);
    for (idx, stmt) in data_statements.iter().enumerate() {
        if idx % 100 == 0 && idx > 0 {
            tracing::debug!(progress = idx, total = data_statements.len(), "loading data...");
        }
        txn.execute(stmt, &[]).await
            .map_err(|e| anyhow::anyhow!("failed to execute data statement {}: {} - Statement: {}", idx, e, &stmt[..stmt.len().min(100)]))?;
    }
    
    tracing::info!(statements = data_statements.len(), "Data statements executed");

    // Commit the transaction explicitly
    txn.commit().await
        .map_err(|e| anyhow::anyhow!("failed to commit transaction: {}", e))?;
    
    // Re-enable foreign key checks after commit
    conn.execute("SET FOREIGN_KEY_CHECKS=1", &[]).await
        .map_err(|e| anyhow::anyhow!("failed to re-enable foreign key checks: {}", e))?;

    // Verify data loaded successfully
    let result = conn.query("SELECT COUNT(*) FROM actor", &[]).await
        .map_err(|e| anyhow::anyhow!("failed to verify actor table: {}", e))?;
    
    let actor_count = result.rows.first()
        .and_then(|row| row.get(0))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    tracing::info!(
        actor_count = actor_count,
        "Sakila database initialized successfully"
    );

    if actor_count == 0 {
        return Err(anyhow::anyhow!("Sakila data not loaded properly - actor table is empty"));
    }

    Ok(())
}

/// Get or create the Postgres test container with Pagila database
///
/// The container is started lazily on first access and reused for subsequent calls.
/// It runs Postgres 16 on a random host port mapped to the standard Postgres port 5432.
/// The Pagila sample database (schema + data) is loaded automatically on first startup.
///
/// This initialization includes:
/// - Creating all Pagila tables (actor, film, customer, etc.)
/// - Loading ~200 actors, ~1000 films, and related data
/// - Setting up foreign keys and constraints
/// - Verifying data loaded successfully
///
/// The initialization takes 30-90 seconds on first call but subsequent calls are instant.
pub async fn postgres_container() -> anyhow::Result<ContainerInfo> {
    {
        let guard = POSTGRES_CONTAINER
            .lock()
            .map_err(|e| anyhow::anyhow!("failed to lock postgres container: {}", e))?;

        if let Some(ref container) = *guard {
            return Ok(container.info.clone());
        }
    }

    tracing::info!("starting PostgreSQL test container");

    let image = Postgres::default();
    let container = image
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("failed to start postgres container: {}", e))?;

    let host_port = container
        .get_host_port_ipv4(5432)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get postgres port: {}", e))?;

    // testcontainers-modules Postgres defaults: postgres user/password with "postgres" database
    let mut info = ContainerInfo {
        host: "127.0.0.1".to_string(),
        port: host_port,
        database: Some("postgres".to_string()),
        username: Some("postgres".to_string()),
        password: Some("postgres".to_string()),
        initialized: false,
    };

    tracing::info!(
        port = host_port,
        "PostgreSQL test container started, initializing Pagila database..."
    );

    init_postgres_data(&info).await?;
    info.initialized = true;

    {
        let mut guard = POSTGRES_CONTAINER
            .lock()
            .map_err(|e| anyhow::anyhow!("failed to lock postgres container: {}", e))?;

        *guard = Some(PostgresContainer {
            inner: container,
            info: info.clone(),
        });
    }

    Ok(info)
}

/// Get or create the MySQL test container with Sakila database
///
/// The container is started lazily on first access and reused for subsequent calls.
/// It runs MySQL 8.0 on a random host port mapped to the standard MySQL port 3306.
/// The Sakila sample database (schema + data) is loaded automatically on first startup.
///
/// This initialization includes:
/// - Creating all Sakila tables (actor, film, customer, etc.)
/// - Loading ~200 actors, ~1000 films, and related data
/// - Setting up foreign keys and constraints
/// - Verifying data loaded successfully
///
/// The initialization takes 30-90 seconds on first call but subsequent calls are instant.
pub async fn mysql_container() -> anyhow::Result<ContainerInfo> {
    {
        let guard = MYSQL_CONTAINER
            .lock()
            .map_err(|e| anyhow::anyhow!("failed to lock mysql container: {}", e))?;

        if let Some(ref container) = *guard {
            return Ok(container.info.clone());
        }
    }

    tracing::info!("starting MySQL test container");

    let image = Mysql::default();
    let container = image
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("failed to start mysql container: {}", e))?;

    let host_port = container
        .get_host_port_ipv4(3306)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get mysql port: {}", e))?;

    // testcontainers-modules MySQL defaults: root user with empty password and "test" database
    let mut info = ContainerInfo {
        host: "127.0.0.1".to_string(),
        port: host_port,
        database: Some("test".to_string()),
        username: Some("root".to_string()),
        password: None,
        initialized: false,
    };

    tracing::info!(
        port = host_port,
        "MySQL test container started, initializing Sakila database..."
    );

    init_mysql_data(&info).await?;
    info.initialized = true;

    {
        let mut guard = MYSQL_CONTAINER
            .lock()
            .map_err(|e| anyhow::anyhow!("failed to lock mysql container: {}", e))?;

        *guard = Some(MysqlContainer {
            inner: container,
            info: info.clone(),
        });
    }

    Ok(info)
}

/// Get or create the Redis test container
///
/// The container is started lazily on first access and reused for subsequent calls.
/// It runs Redis 7 on a random host port mapped to the standard Redis port 6379.
/// No initialization is needed for Redis as it's used for connection/pooling tests only.
pub async fn redis_container() -> anyhow::Result<ContainerInfo> {
    {
        let guard = REDIS_CONTAINER
            .lock()
            .map_err(|e| anyhow::anyhow!("failed to lock redis container: {}", e))?;

        if let Some(ref container) = *guard {
            return Ok(container.info.clone());
        }
    }

    tracing::info!("starting Redis test container");

    let image = Redis::default();
    let container = image
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("failed to start redis container: {}", e))?;

    let host_port = container
        .get_host_port_ipv4(6379)
        .await
        .map_err(|e| anyhow::anyhow!("failed to get redis port: {}", e))?;

    let info = ContainerInfo {
        host: "127.0.0.1".to_string(),
        port: host_port,
        database: None,
        username: None,
        password: None,
        initialized: true,
    };

    tracing::info!(port = host_port, "Redis test container started successfully");

    {
        let mut guard = REDIS_CONTAINER
            .lock()
            .map_err(|e| anyhow::anyhow!("failed to lock redis container: {}", e))?;

        *guard = Some(RedisContainer {
            inner: container,
            info: info.clone(),
        });
    }

    Ok(info)
}

/// Initialize all test containers
///
/// This function starts all containers in parallel for faster test startup.
/// It's called automatically by the test harness but can also be called manually
/// if you want to pre-warm containers before running tests.
pub async fn init_all_containers() -> anyhow::Result<()> {
    tracing::info!("initializing all test containers");

    let (postgres_result, mysql_result, redis_result) = tokio::join!(
        postgres_container(),
        mysql_container(),
        redis_container()
    );

    postgres_result?;
    mysql_result?;
    redis_result?;

    tracing::info!("all test containers initialized successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_postgres_container_starts() {
        let result = postgres_container().await;
        assert!(
            result.is_ok(),
            "Postgres container should start: {:?}",
            result.err()
        );

        if let Ok(info) = result {
            assert_eq!(info.host, "127.0.0.1");
            assert!(info.port > 0);
            assert!(info.database.is_some());
            assert!(info.username.is_some());
            assert!(info.password.is_some());
        }
    }

    #[tokio::test]
    async fn test_mysql_container_starts() {
        let result = mysql_container().await;
        assert!(
            result.is_ok(),
            "MySQL container should start: {:?}",
            result.err()
        );

        if let Ok(info) = result {
            assert_eq!(info.host, "127.0.0.1");
            assert!(info.port > 0);
            assert!(info.database.is_some());
            assert!(info.username.is_some());
            assert!(info.password.is_some());
        }
    }

    #[tokio::test]
    async fn test_redis_container_starts() {
        let result = redis_container().await;
        assert!(
            result.is_ok(),
            "Redis container should start: {:?}",
            result.err()
        );

        if let Ok(info) = result {
            assert_eq!(info.host, "127.0.0.1");
            assert!(info.port > 0);
            assert_eq!(info.database, None);
        }
    }

    #[tokio::test]
    async fn test_container_reuse() {
        let info1 = postgres_container().await.unwrap();
        let info2 = postgres_container().await.unwrap();

        assert_eq!(info1.port, info2.port, "Container should be reused");
    }

    #[tokio::test]
    async fn test_init_all_containers() {
        let result = init_all_containers().await;
        assert!(
            result.is_ok(),
            "All containers should start: {:?}",
            result.err()
        );
    }
}
