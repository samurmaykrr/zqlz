//! Performance metrics collector
//!
//! Collects and aggregates database performance metrics from various sources.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use zqlz_core::{Connection, Result, ZqlzError};

/// Query execution statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryStats {
    /// Total number of queries executed
    pub total_queries: u64,
    /// Number of SELECT queries
    pub select_queries: u64,
    /// Number of INSERT queries
    pub insert_queries: u64,
    /// Number of UPDATE queries
    pub update_queries: u64,
    /// Number of DELETE queries
    pub delete_queries: u64,
    /// Average query execution time in milliseconds
    pub avg_query_time_ms: f64,
    /// Maximum query execution time in milliseconds
    pub max_query_time_ms: f64,
    /// Number of slow queries (based on threshold)
    pub slow_queries: u64,
    /// Queries per second (QPS)
    pub queries_per_second: f64,
}

impl QueryStats {
    /// Create empty QueryStats
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method: set total queries
    pub fn with_total_queries(mut self, count: u64) -> Self {
        self.total_queries = count;
        self
    }

    /// Builder method: set SELECT query count
    pub fn with_select_queries(mut self, count: u64) -> Self {
        self.select_queries = count;
        self
    }

    /// Builder method: set INSERT query count
    pub fn with_insert_queries(mut self, count: u64) -> Self {
        self.insert_queries = count;
        self
    }

    /// Builder method: set UPDATE query count
    pub fn with_update_queries(mut self, count: u64) -> Self {
        self.update_queries = count;
        self
    }

    /// Builder method: set DELETE query count
    pub fn with_delete_queries(mut self, count: u64) -> Self {
        self.delete_queries = count;
        self
    }

    /// Builder method: set average query time
    pub fn with_avg_query_time_ms(mut self, time_ms: f64) -> Self {
        self.avg_query_time_ms = time_ms;
        self
    }

    /// Builder method: set max query time
    pub fn with_max_query_time_ms(mut self, time_ms: f64) -> Self {
        self.max_query_time_ms = time_ms;
        self
    }

    /// Builder method: set slow query count
    pub fn with_slow_queries(mut self, count: u64) -> Self {
        self.slow_queries = count;
        self
    }

    /// Builder method: set queries per second
    pub fn with_qps(mut self, qps: f64) -> Self {
        self.queries_per_second = qps;
        self
    }

    /// Calculate percentage of DML (write) queries
    pub fn write_percentage(&self) -> f64 {
        if self.total_queries == 0 {
            return 0.0;
        }
        let writes = self.insert_queries + self.update_queries + self.delete_queries;
        (writes as f64 / self.total_queries as f64) * 100.0
    }

    /// Calculate percentage of read queries
    pub fn read_percentage(&self) -> f64 {
        if self.total_queries == 0 {
            return 0.0;
        }
        (self.select_queries as f64 / self.total_queries as f64) * 100.0
    }
}

/// Cache/buffer pool statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStats {
    /// Total cache/buffer pool size in bytes
    pub buffer_pool_size: u64,
    /// Used buffer pool size in bytes
    pub buffer_pool_used: u64,
    /// Cache hit ratio (0.0 - 1.0)
    pub cache_hit_ratio: f64,
    /// Number of cache reads
    pub cache_reads: u64,
    /// Number of disk reads
    pub disk_reads: u64,
    /// Number of dirty pages (pages needing flush)
    pub dirty_pages: u64,
}

impl CacheStats {
    /// Create empty CacheStats
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method: set buffer pool size
    pub fn with_buffer_pool_size(mut self, size: u64) -> Self {
        self.buffer_pool_size = size;
        self
    }

    /// Builder method: set buffer pool used
    pub fn with_buffer_pool_used(mut self, used: u64) -> Self {
        self.buffer_pool_used = used;
        self
    }

    /// Builder method: set cache hit ratio
    pub fn with_cache_hit_ratio(mut self, ratio: f64) -> Self {
        self.cache_hit_ratio = ratio.clamp(0.0, 1.0);
        self
    }

    /// Builder method: set cache reads
    pub fn with_cache_reads(mut self, reads: u64) -> Self {
        self.cache_reads = reads;
        self
    }

    /// Builder method: set disk reads
    pub fn with_disk_reads(mut self, reads: u64) -> Self {
        self.disk_reads = reads;
        self
    }

    /// Builder method: set dirty pages
    pub fn with_dirty_pages(mut self, pages: u64) -> Self {
        self.dirty_pages = pages;
        self
    }

    /// Calculate buffer pool usage percentage
    pub fn buffer_pool_usage_percent(&self) -> f64 {
        if self.buffer_pool_size == 0 {
            return 0.0;
        }
        (self.buffer_pool_used as f64 / self.buffer_pool_size as f64) * 100.0
    }

    /// Calculate cache hit ratio from reads (if not directly available)
    pub fn calculate_hit_ratio(&self) -> f64 {
        let total_reads = self.cache_reads + self.disk_reads;
        if total_reads == 0 {
            return 0.0;
        }
        self.cache_reads as f64 / total_reads as f64
    }

    /// Check if cache performance is good (hit ratio > 90%)
    pub fn is_cache_healthy(&self) -> bool {
        self.cache_hit_ratio >= 0.90
    }
}

/// Aggregated performance metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// When these metrics were collected
    pub timestamp: DateTime<Utc>,
    /// Query execution statistics
    pub query_stats: QueryStats,
    /// Cache/buffer pool statistics
    pub cache_stats: CacheStats,
    /// Time taken to collect these metrics
    pub collection_time: Duration,
    /// Database name (if available)
    pub database: Option<String>,
    /// Driver/database type
    pub driver: Option<String>,
}

impl PerformanceMetrics {
    /// Create new metrics with timestamp
    pub fn new(query_stats: QueryStats, cache_stats: CacheStats) -> Self {
        Self {
            timestamp: Utc::now(),
            query_stats,
            cache_stats,
            collection_time: Duration::ZERO,
            database: None,
            driver: None,
        }
    }

    /// Builder method: set collection time
    pub fn with_collection_time(mut self, time: Duration) -> Self {
        self.collection_time = time;
        self
    }

    /// Builder method: set database name
    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }

    /// Builder method: set driver name
    pub fn with_driver(mut self, driver: String) -> Self {
        self.driver = Some(driver);
        self
    }

    /// Check overall database health based on metrics
    pub fn is_healthy(&self) -> bool {
        // Consider healthy if:
        // - Cache hit ratio > 80%
        // - Average query time < 100ms
        self.cache_stats.cache_hit_ratio >= 0.80 && self.query_stats.avg_query_time_ms < 100.0
    }

    /// Get a summary string of the metrics
    pub fn summary(&self) -> String {
        format!(
            "QPS: {:.1}, Avg Query: {:.1}ms, Cache Hit: {:.1}%",
            self.query_stats.queries_per_second,
            self.query_stats.avg_query_time_ms,
            self.cache_stats.cache_hit_ratio * 100.0
        )
    }
}

/// Configuration for performance collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorConfig {
    /// Threshold for slow query detection (ms)
    pub slow_query_threshold_ms: u64,
    /// Include detailed query breakdown by type
    pub include_query_breakdown: bool,
    /// Include cache statistics
    pub include_cache_stats: bool,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            slow_query_threshold_ms: 1000,
            include_query_breakdown: true,
            include_cache_stats: true,
        }
    }
}

impl CollectorConfig {
    /// Create config with custom slow query threshold
    pub fn new(slow_query_threshold_ms: u64) -> Self {
        Self {
            slow_query_threshold_ms,
            ..Default::default()
        }
    }

    /// Builder method: set query breakdown inclusion
    pub fn with_query_breakdown(mut self, include: bool) -> Self {
        self.include_query_breakdown = include;
        self
    }

    /// Builder method: set cache stats inclusion
    pub fn with_cache_stats(mut self, include: bool) -> Self {
        self.include_cache_stats = include;
        self
    }
}

/// Performance metrics collector
pub struct PerformanceCollector {
    config: CollectorConfig,
}

impl PerformanceCollector {
    /// Create a new collector with default configuration
    pub fn new() -> Self {
        Self {
            config: CollectorConfig::default(),
        }
    }

    /// Create a collector with custom configuration
    pub fn with_config(config: CollectorConfig) -> Self {
        Self { config }
    }

    /// Get the current configuration
    pub fn config(&self) -> &CollectorConfig {
        &self.config
    }

    /// Collect metrics from a database connection
    pub async fn collect<C: Connection>(&self, conn: &C) -> Result<PerformanceMetrics> {
        let start = std::time::Instant::now();
        let driver_name = conn.driver_name();

        let query = PerformanceQuery::for_driver(driver_name)?;
        let result = conn.query(query, &[]).await?;

        let (query_stats, cache_stats) = self.parse_metrics_result(&result, driver_name)?;

        let metrics = PerformanceMetrics::new(query_stats, cache_stats)
            .with_collection_time(start.elapsed())
            .with_driver(driver_name.to_string());

        Ok(metrics)
    }

    /// Parse metrics from query result based on database type
    fn parse_metrics_result(
        &self,
        result: &zqlz_core::QueryResult,
        driver_name: &str,
    ) -> Result<(QueryStats, CacheStats)> {
        // Default stats if parsing fails or no data
        if result.rows.is_empty() {
            return Ok((QueryStats::new(), CacheStats::new()));
        }

        let row = &result.rows[0];
        let get_f64 = |name: &str| -> f64 {
            row.get_by_name(name)
                .and_then(|v| match v {
                    zqlz_core::Value::Int8(i) => Some(*i as f64),
                    zqlz_core::Value::Int16(i) => Some(*i as f64),
                    zqlz_core::Value::Int32(i) => Some(*i as f64),
                    zqlz_core::Value::Int64(i) => Some(*i as f64),
                    zqlz_core::Value::Float32(f) => Some(*f as f64),
                    zqlz_core::Value::Float64(f) => Some(*f),
                    zqlz_core::Value::String(s) => s.parse().ok(),
                    zqlz_core::Value::Decimal(s) => s.parse().ok(),
                    _ => None,
                })
                .unwrap_or(0.0)
        };
        let get_u64 = |name: &str| -> u64 { get_f64(name) as u64 };

        let query_stats = match driver_name {
            "postgresql" | "postgres" => QueryStats::new()
                .with_total_queries(get_u64("total_queries"))
                .with_select_queries(get_u64("select_queries"))
                .with_insert_queries(get_u64("insert_queries"))
                .with_update_queries(get_u64("update_queries"))
                .with_delete_queries(get_u64("delete_queries"))
                .with_avg_query_time_ms(get_f64("avg_time_ms"))
                .with_max_query_time_ms(get_f64("max_time_ms")),

            "mysql" => QueryStats::new()
                .with_total_queries(get_u64("total_queries"))
                .with_select_queries(get_u64("select_queries"))
                .with_insert_queries(get_u64("insert_queries"))
                .with_update_queries(get_u64("update_queries"))
                .with_delete_queries(get_u64("delete_queries")),

            _ => QueryStats::new().with_total_queries(get_u64("total_queries")),
        };

        let cache_stats = match driver_name {
            "postgresql" | "postgres" => CacheStats::new()
                .with_cache_hit_ratio(get_f64("cache_hit_ratio"))
                .with_buffer_pool_size(get_u64("shared_buffers"))
                .with_cache_reads(get_u64("blks_hit"))
                .with_disk_reads(get_u64("blks_read")),

            "mysql" => CacheStats::new()
                .with_buffer_pool_size(get_u64("buffer_pool_size"))
                .with_buffer_pool_used(get_u64("buffer_pool_pages_data"))
                .with_cache_reads(get_u64("buffer_pool_read_requests"))
                .with_disk_reads(get_u64("buffer_pool_reads")),

            _ => CacheStats::new(),
        };

        Ok((query_stats, cache_stats))
    }
}

impl Default for PerformanceCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for connections that support performance metrics collection
#[async_trait]
pub trait MetricsConnection: Connection {
    /// Collect performance metrics from the database
    async fn collect_metrics(&self) -> Result<PerformanceMetrics>;
}

/// Query builder for database-specific performance metrics queries
pub struct PerformanceQuery;

impl PerformanceQuery {
    /// Get the SQL query for PostgreSQL performance metrics
    pub fn postgres() -> &'static str {
        r#"
        SELECT
            (SELECT sum(calls) FROM pg_stat_statements) as total_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'SELECT%') as select_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'INSERT%') as insert_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'UPDATE%') as update_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'DELETE%') as delete_queries,
            (SELECT avg(mean_exec_time) FROM pg_stat_statements WHERE calls > 0) as avg_time_ms,
            (SELECT max(max_exec_time) FROM pg_stat_statements) as max_time_ms,
            (SELECT 
                CASE WHEN (blks_hit + blks_read) = 0 THEN 0
                ELSE blks_hit::float / (blks_hit + blks_read)::float END
             FROM pg_stat_database WHERE datname = current_database()) as cache_hit_ratio,
            (SELECT setting::bigint * 8192 FROM pg_settings WHERE name = 'shared_buffers') as shared_buffers,
            (SELECT blks_hit FROM pg_stat_database WHERE datname = current_database()) as blks_hit,
            (SELECT blks_read FROM pg_stat_database WHERE datname = current_database()) as blks_read
        "#
    }

    /// Get the SQL query for MySQL performance metrics
    pub fn mysql() -> &'static str {
        r#"
        SELECT
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Questions') as total_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_select') as select_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_insert') as insert_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_update') as update_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_delete') as delete_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_bytes_data') as buffer_pool_pages_data,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_read_requests') as buffer_pool_read_requests,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_reads') as buffer_pool_reads,
            (SELECT @@innodb_buffer_pool_size) as buffer_pool_size
        "#
    }

    /// Get the SQL query for SQLite (limited metrics)
    pub fn sqlite() -> &'static str {
        "SELECT 0 as total_queries"
    }

    /// Get the SQL query for SQL Server performance metrics
    pub fn mssql() -> &'static str {
        r#"
        SELECT
            (SELECT cntr_value FROM sys.dm_os_performance_counters 
             WHERE counter_name = 'Batch Requests/sec' AND instance_name = '') as total_queries,
            (SELECT cntr_value FROM sys.dm_os_performance_counters 
             WHERE counter_name = 'Buffer cache hit ratio' AND object_name LIKE '%Buffer Manager%') as cache_hit_ratio,
            (SELECT cntr_value FROM sys.dm_os_performance_counters 
             WHERE counter_name = 'Database pages' AND object_name LIKE '%Buffer Manager%') as buffer_pages
        "#
    }

    /// Get the SQL query for ClickHouse performance metrics
    pub fn clickhouse() -> &'static str {
        r#"
        SELECT
            (SELECT sum(value) FROM system.events WHERE event = 'Query') as total_queries,
            (SELECT sum(value) FROM system.events WHERE event = 'SelectQuery') as select_queries,
            (SELECT sum(value) FROM system.events WHERE event = 'InsertQuery') as insert_queries
        "#
    }

    /// Get the SQL query for DuckDB (limited metrics)
    pub fn duckdb() -> &'static str {
        "SELECT 0 as total_queries"
    }

    /// Get the appropriate query for a driver
    pub fn for_driver(driver_name: &str) -> Result<&'static str> {
        match driver_name {
            "postgresql" | "postgres" => Ok(Self::postgres()),
            "mysql" => Ok(Self::mysql()),
            "sqlite" => Ok(Self::sqlite()),
            "mssql" | "sqlserver" => Ok(Self::mssql()),
            "clickhouse" => Ok(Self::clickhouse()),
            "duckdb" => Ok(Self::duckdb()),
            _ => Err(ZqlzError::NotSupported(format!(
                "Performance metrics query not available for driver: {}",
                driver_name
            ))),
        }
    }
}
