//! Tests for connection pool functionality

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use zqlz_core::{
    Connection, QueryResult, Result, SqlObjectName, StatementResult, Transaction, Value, ZqlzError,
};

use super::config::PoolConfig;
use super::connection_pool::{ConnectionFactory, ConnectionPool};
use super::stats::PoolStats;

/// Mock connection for testing
struct MockConnection {
    #[allow(dead_code)]
    id: usize,
    closed: AtomicBool,
}

impl MockConnection {
    fn new(id: usize) -> Self {
        Self {
            id,
            closed: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl Connection for MockConnection {
    fn driver_name(&self) -> &str {
        "mock"
    }

    async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<StatementResult> {
        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows: 0,
            error: None,
        })
    }

    async fn query(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult> {
        Ok(QueryResult::empty())
    }

    fn rename_table_sql(&self, table_name: &SqlObjectName, new_table_name: &str) -> Result<String> {
        Ok(format!(
            "ALTER TABLE {} RENAME TO {}",
            self.render_qualified_name(table_name),
            self.quote_identifier(new_table_name)
        ))
    }

    fn drop_table_sql(
        &self,
        table_name: &SqlObjectName,
        options: zqlz_core::DropTableOptions,
    ) -> Result<String> {
        let mut sql = String::from("DROP TABLE");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(table_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn drop_view_sql(
        &self,
        view_name: &SqlObjectName,
        options: zqlz_core::DropViewOptions,
    ) -> Result<String> {
        let mut sql = String::from("DROP VIEW");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(view_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn drop_trigger_sql(
        &self,
        trigger_name: &SqlObjectName,
        _table_name: Option<&SqlObjectName>,
        options: zqlz_core::DropTriggerOptions,
    ) -> Result<String> {
        let mut sql = String::from("DROP TRIGGER");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(trigger_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn truncate_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "TRUNCATE TABLE {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn duplicate_table_sql(
        &self,
        source_table_name: &SqlObjectName,
        new_table_name: &SqlObjectName,
    ) -> Result<String> {
        Ok(format!(
            "CREATE TABLE {} AS SELECT * FROM {}",
            self.render_qualified_name(new_table_name),
            self.render_qualified_name(source_table_name)
        ))
    }

    fn clear_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "DELETE FROM {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn table_has_rows_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "SELECT 1 FROM {} LIMIT 1",
            self.render_qualified_name(table_name)
        ))
    }

    fn select_rows_sql(
        &self,
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
    ) -> Result<String> {
        let projection = if projected_columns.is_empty() {
            "*".to_string()
        } else {
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut sql = format!(
            "SELECT {} FROM {}",
            projection,
            self.render_qualified_name(table_name)
        );
        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }
        Ok(sql)
    }

    fn select_distinct_rows_sql(
        &self,
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
        order_by_columns: &[String],
        limit: u64,
    ) -> Result<String> {
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {}",
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", "),
            self.render_qualified_name(table_name)
        );
        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }
        if !order_by_columns.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(
                &order_by_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        sql.push_str(&format!(" LIMIT {}", limit));
        Ok(sql)
    }

    fn insert_row_sql(
        &self,
        table_name: &SqlObjectName,
        column_names: &[String],
        value_count: usize,
    ) -> Result<String> {
        let placeholders = (0..value_count)
            .map(|index| self.format_bind_placeholder(index))
            .collect::<Vec<_>>()
            .join(", ");
        let columns = column_names
            .iter()
            .map(|column| self.quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.render_qualified_name(table_name),
            columns,
            placeholders
        ))
    }

    fn performance_metrics_query_sql(&self) -> Result<String> {
        Ok("SELECT 0 as total_queries".to_string())
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        Err(ZqlzError::NotSupported(
            "Transactions not supported in mock".into(),
        ))
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

/// Mock factory that counts connections created
struct MockConnectionFactory {
    counter: AtomicUsize,
}

impl MockConnectionFactory {
    fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }

    fn count(&self) -> usize {
        self.counter.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ConnectionFactory for MockConnectionFactory {
    async fn create(&self) -> Result<Arc<dyn Connection>> {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(MockConnection::new(id)))
    }
}

// =============================================================================
// PoolConfig tests
// =============================================================================

#[test]
fn test_pool_config_creation() {
    let config = PoolConfig::new(2, 10);
    assert_eq!(config.min_size(), 2);
    assert_eq!(config.max_size(), 10);
    assert_eq!(config.acquire_timeout(), Duration::from_millis(30_000));
    assert_eq!(config.idle_timeout(), Duration::from_millis(600_000));
    assert!(config.max_lifetime().is_none());
}

#[test]
fn test_pool_config_with_timeouts() {
    let config = PoolConfig::new(1, 5)
        .with_acquire_timeout_ms(5000)
        .with_idle_timeout_ms(60000)
        .with_max_lifetime_ms(3600000);

    assert_eq!(config.acquire_timeout(), Duration::from_millis(5000));
    assert_eq!(config.idle_timeout(), Duration::from_millis(60000));
    assert_eq!(config.max_lifetime(), Some(Duration::from_millis(3600000)));
}

#[test]
fn test_pool_config_default() {
    let config = PoolConfig::default();
    assert_eq!(config.min_size(), 1);
    assert_eq!(config.max_size(), 10);
}

#[test]
#[should_panic(expected = "max_size must be greater than 0")]
fn test_pool_config_invalid_max_size() {
    PoolConfig::new(0, 0);
}

#[test]
#[should_panic(expected = "min_size (10) cannot exceed max_size (5)")]
fn test_pool_config_min_exceeds_max() {
    PoolConfig::new(10, 5);
}

#[test]
fn test_pool_config_serialization() {
    let config = PoolConfig::new(2, 10)
        .with_acquire_timeout_ms(5000)
        .with_max_lifetime_ms(3600000);

    let json = serde_json::to_string(&config).expect("serialize");
    let deserialized: PoolConfig = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(deserialized.min_size(), 2);
    assert_eq!(deserialized.max_size(), 10);
    assert_eq!(deserialized.acquire_timeout(), Duration::from_millis(5000));
}

// =============================================================================
// PoolStats tests
// =============================================================================

#[test]
fn test_pool_stats_creation() {
    let stats = PoolStats::new(10, 6, 4, 2);
    assert_eq!(stats.total(), 10);
    assert_eq!(stats.idle(), 6);
    assert_eq!(stats.active(), 4);
    assert_eq!(stats.waiting(), 2);
}

#[test]
fn test_pool_stats_utilization() {
    let stats = PoolStats::new(10, 5, 5, 0);
    assert!((stats.utilization() - 0.5).abs() < 0.001);

    let full_stats = PoolStats::new(10, 0, 10, 0);
    assert!((full_stats.utilization() - 1.0).abs() < 0.001);

    let empty_stats = PoolStats::new(0, 0, 0, 0);
    assert!((empty_stats.utilization() - 0.0).abs() < 0.001);
}

#[test]
fn test_pool_stats_is_full() {
    let stats = PoolStats::new(10, 0, 10, 5);
    assert!(stats.is_full());

    let stats = PoolStats::new(10, 5, 5, 0);
    assert!(!stats.is_full());

    let empty = PoolStats::new(0, 0, 0, 0);
    assert!(!empty.is_full());
}

#[test]
fn test_pool_stats_serialization() {
    let stats = PoolStats::new(10, 6, 4, 2);
    let json = serde_json::to_string(&stats).expect("serialize");
    let deserialized: PoolStats = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(stats, deserialized);
}

// =============================================================================
// ConnectionPool tests
// =============================================================================

#[tokio::test]
async fn test_pool_get_connection() {
    let config = PoolConfig::new(1, 5);
    let factory = MockConnectionFactory::new();
    let pool = ConnectionPool::new(config, factory);

    let conn = pool.get().await.expect("get connection");
    assert_eq!(conn.driver_name(), "mock");

    let stats = pool.stats();
    assert_eq!(stats.active(), 1);
    assert_eq!(stats.idle(), 0);
}

#[tokio::test]
async fn test_pool_connection_return() {
    let factory = Arc::new(MockConnectionFactory::new());
    let config = PoolConfig::new(1, 5);
    let pool = ConnectionPool::new(config, factory.clone());

    {
        let _conn = pool.get().await.expect("get connection");
        assert_eq!(pool.stats().active(), 1);
    }

    // After drop, connection should be returned to pool
    // Give some time for the sync drop to complete
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(pool.stats().active(), 0);
    assert_eq!(pool.stats().idle(), 1);

    // Getting another connection should reuse the idle one
    let _conn2 = pool.get().await.expect("get connection");
    assert_eq!(factory.count(), 1); // Only one connection created total
}

#[tokio::test]
async fn test_pool_max_size_limit() {
    let config = PoolConfig::new(1, 2).with_acquire_timeout_ms(100);
    let factory = MockConnectionFactory::new();
    let pool = ConnectionPool::new(config, factory);

    // Acquire 2 connections (max)
    let conn1 = pool.get().await.expect("get connection 1");
    let conn2 = pool.get().await.expect("get connection 2");

    assert_eq!(pool.stats().active(), 2);

    // Third acquire should timeout
    let result = pool.get().await;
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("Timed out"));

    drop(conn1);
    drop(conn2);
}

#[tokio::test]
async fn test_pool_stats() {
    let config = PoolConfig::new(1, 5);
    let factory = MockConnectionFactory::new();
    let pool = ConnectionPool::new(config, factory);

    // Initial stats
    let stats = pool.stats();
    assert_eq!(stats.total(), 0);
    assert_eq!(stats.idle(), 0);
    assert_eq!(stats.active(), 0);

    // After getting a connection
    let _conn = pool.get().await.expect("get");
    let stats = pool.stats();
    assert_eq!(stats.total(), 1);
    assert_eq!(stats.idle(), 0);
    assert_eq!(stats.active(), 1);
}

#[tokio::test]
async fn test_pool_close_idle() {
    let factory = Arc::new(MockConnectionFactory::new());
    let config = PoolConfig::new(1, 5);
    let pool = ConnectionPool::new(config, factory.clone());

    // Create and return some connections
    {
        let _conn1 = pool.get().await.expect("get");
        let _conn2 = pool.get().await.expect("get");
    }

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(pool.stats().idle(), 2);

    // Close all idle
    pool.close_idle().await;
    assert_eq!(pool.stats().idle(), 0);
}
