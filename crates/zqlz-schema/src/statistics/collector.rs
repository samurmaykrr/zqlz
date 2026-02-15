//! Statistics collector implementation.
//!
//! Provides functionality for collecting table and index statistics from databases.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Statistics for a single table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableStatistics {
    /// Schema name (optional for databases without schemas).
    pub schema: Option<String>,
    /// Table name.
    pub name: String,
    /// Estimated row count.
    pub row_count: u64,
    /// Table data size in bytes.
    pub size_bytes: u64,
    /// Total index size in bytes.
    pub index_size_bytes: u64,
    /// Toast/LOB data size in bytes (PostgreSQL-specific, 0 for others).
    pub toast_size_bytes: u64,
    /// When statistics were last collected.
    pub last_updated: DateTime<Utc>,
    /// When table was last auto-analyzed (if available).
    pub last_analyze: Option<DateTime<Utc>>,
    /// When table was last vacuumed (PostgreSQL-specific).
    pub last_vacuum: Option<DateTime<Utc>>,
    /// Number of dead tuples (PostgreSQL-specific).
    pub dead_tuples: Option<u64>,
    /// Percentage of table that has been modified since last analyze.
    pub modification_percentage: Option<f64>,
}

impl TableStatistics {
    /// Creates a new TableStatistics with required fields.
    pub fn new(name: impl Into<String>, row_count: u64, size_bytes: u64) -> Self {
        Self {
            schema: None,
            name: name.into(),
            row_count,
            size_bytes,
            index_size_bytes: 0,
            toast_size_bytes: 0,
            last_updated: Utc::now(),
            last_analyze: None,
            last_vacuum: None,
            dead_tuples: None,
            modification_percentage: None,
        }
    }

    /// Sets the schema name.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Sets the index size.
    pub fn with_index_size(mut self, size_bytes: u64) -> Self {
        self.index_size_bytes = size_bytes;
        self
    }

    /// Sets the toast/LOB size.
    pub fn with_toast_size(mut self, size_bytes: u64) -> Self {
        self.toast_size_bytes = size_bytes;
        self
    }

    /// Sets the last analyze timestamp.
    pub fn with_last_analyze(mut self, timestamp: DateTime<Utc>) -> Self {
        self.last_analyze = Some(timestamp);
        self
    }

    /// Sets the last vacuum timestamp.
    pub fn with_last_vacuum(mut self, timestamp: DateTime<Utc>) -> Self {
        self.last_vacuum = Some(timestamp);
        self
    }

    /// Sets the dead tuples count.
    pub fn with_dead_tuples(mut self, count: u64) -> Self {
        self.dead_tuples = Some(count);
        self
    }

    /// Sets the modification percentage.
    pub fn with_modification_percentage(mut self, percentage: f64) -> Self {
        self.modification_percentage = Some(percentage);
        self
    }

    /// Returns the total size (data + index + toast).
    pub fn total_size(&self) -> u64 {
        self.size_bytes + self.index_size_bytes + self.toast_size_bytes
    }

    /// Returns a qualified table name (schema.name or just name).
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.name),
            None => self.name.clone(),
        }
    }

    /// Returns true if table needs vacuuming (PostgreSQL).
    /// Uses a heuristic: dead tuples > 10% of row count.
    pub fn needs_vacuum(&self) -> bool {
        if let Some(dead) = self.dead_tuples {
            if self.row_count > 0 {
                let dead_ratio = dead as f64 / self.row_count as f64;
                return dead_ratio > 0.1;
            }
        }
        false
    }

    /// Returns true if table needs analysis.
    /// Uses modification_percentage > 10% as threshold.
    pub fn needs_analyze(&self) -> bool {
        self.modification_percentage.map_or(false, |p| p > 10.0)
    }
}

/// Statistics for a single index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexStatistics {
    /// Schema name.
    pub schema: Option<String>,
    /// Table name.
    pub table_name: String,
    /// Index name.
    pub index_name: String,
    /// Index size in bytes.
    pub size_bytes: u64,
    /// Number of index scans.
    pub scans: Option<u64>,
    /// Number of tuples returned by index scans.
    pub tuples_returned: Option<u64>,
    /// Number of tuples fetched by index scans.
    pub tuples_fetched: Option<u64>,
    /// When statistics were last collected.
    pub last_updated: DateTime<Utc>,
}

impl IndexStatistics {
    /// Creates a new IndexStatistics.
    pub fn new(
        table_name: impl Into<String>,
        index_name: impl Into<String>,
        size_bytes: u64,
    ) -> Self {
        Self {
            schema: None,
            table_name: table_name.into(),
            index_name: index_name.into(),
            size_bytes,
            scans: None,
            tuples_returned: None,
            tuples_fetched: None,
            last_updated: Utc::now(),
        }
    }

    /// Sets the schema name.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Sets scan statistics.
    pub fn with_scans(mut self, scans: u64, tuples_returned: u64, tuples_fetched: u64) -> Self {
        self.scans = Some(scans);
        self.tuples_returned = Some(tuples_returned);
        self.tuples_fetched = Some(tuples_fetched);
        self
    }

    /// Returns true if index appears unused (0 scans).
    pub fn is_unused(&self) -> bool {
        self.scans.map_or(false, |s| s == 0)
    }
}

/// Statistics for an entire schema.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SchemaStatistics {
    /// Schema name.
    pub schema: Option<String>,
    /// Total row count across all tables.
    pub total_rows: u64,
    /// Total data size in bytes.
    pub total_size_bytes: u64,
    /// Total index size in bytes.
    pub total_index_bytes: u64,
    /// Number of tables.
    pub table_count: usize,
    /// Number of indexes.
    pub index_count: usize,
    /// Individual table statistics.
    pub tables: HashMap<String, TableStatistics>,
    /// Individual index statistics.
    pub indexes: HashMap<String, IndexStatistics>,
    /// When statistics were collected.
    pub collected_at: DateTime<Utc>,
}

impl SchemaStatistics {
    /// Creates a new empty SchemaStatistics.
    pub fn new() -> Self {
        Self {
            schema: None,
            total_rows: 0,
            total_size_bytes: 0,
            total_index_bytes: 0,
            table_count: 0,
            index_count: 0,
            tables: HashMap::new(),
            indexes: HashMap::new(),
            collected_at: Utc::now(),
        }
    }

    /// Sets the schema name.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Adds table statistics and updates totals.
    pub fn add_table(&mut self, stats: TableStatistics) {
        self.total_rows += stats.row_count;
        self.total_size_bytes += stats.size_bytes;
        self.total_index_bytes += stats.index_size_bytes;
        self.table_count += 1;
        self.tables.insert(stats.qualified_name(), stats);
    }

    /// Adds index statistics and updates totals.
    pub fn add_index(&mut self, stats: IndexStatistics) {
        self.index_count += 1;
        let key = format!(
            "{}.{}",
            stats.schema.as_deref().unwrap_or(""),
            stats.index_name
        );
        self.indexes.insert(key, stats);
    }

    /// Returns tables that need vacuuming.
    pub fn tables_needing_vacuum(&self) -> Vec<&TableStatistics> {
        self.tables.values().filter(|t| t.needs_vacuum()).collect()
    }

    /// Returns tables that need analysis.
    pub fn tables_needing_analyze(&self) -> Vec<&TableStatistics> {
        self.tables.values().filter(|t| t.needs_analyze()).collect()
    }

    /// Returns unused indexes.
    pub fn unused_indexes(&self) -> Vec<&IndexStatistics> {
        self.indexes.values().filter(|i| i.is_unused()).collect()
    }
}

/// Configuration for the statistics collector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorConfig {
    /// Whether to collect index statistics (may be slower).
    pub collect_index_stats: bool,
    /// Whether to collect maintenance stats (vacuum, analyze).
    pub collect_maintenance_stats: bool,
    /// Default schema to use if none specified.
    pub default_schema: Option<String>,
    /// Maximum tables to collect in parallel.
    pub parallel_limit: usize,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            collect_index_stats: true,
            collect_maintenance_stats: true,
            default_schema: None,
            parallel_limit: 10,
        }
    }
}

impl CollectorConfig {
    /// Creates a new config with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether to collect index statistics.
    pub fn with_index_stats(mut self, collect: bool) -> Self {
        self.collect_index_stats = collect;
        self
    }

    /// Sets whether to collect maintenance statistics.
    pub fn with_maintenance_stats(mut self, collect: bool) -> Self {
        self.collect_maintenance_stats = collect;
        self
    }

    /// Sets the default schema.
    pub fn with_default_schema(mut self, schema: impl Into<String>) -> Self {
        self.default_schema = Some(schema.into());
        self
    }

    /// Sets the parallel collection limit.
    pub fn with_parallel_limit(mut self, limit: usize) -> Self {
        self.parallel_limit = limit;
        self
    }
}

/// Trait for connections that support statistics collection.
#[async_trait]
pub trait StatisticsConnection: Send + Sync {
    /// Gets statistics for a single table.
    async fn get_table_statistics(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> anyhow::Result<TableStatistics>;

    /// Gets statistics for all tables in a schema.
    async fn get_all_table_statistics(
        &self,
        schema: Option<&str>,
    ) -> anyhow::Result<Vec<TableStatistics>>;

    /// Gets statistics for a single index.
    async fn get_index_statistics(
        &self,
        schema: Option<&str>,
        table: &str,
        index: &str,
    ) -> anyhow::Result<IndexStatistics>;

    /// Gets statistics for all indexes on a table.
    async fn get_table_index_statistics(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> anyhow::Result<Vec<IndexStatistics>>;
}

/// Statistics collector that aggregates statistics from database connections.
#[derive(Debug, Clone)]
pub struct StatisticsCollector {
    config: CollectorConfig,
}

impl StatisticsCollector {
    /// Creates a new collector with default configuration.
    pub fn new() -> Self {
        Self {
            config: CollectorConfig::default(),
        }
    }

    /// Creates a new collector with custom configuration.
    pub fn with_config(config: CollectorConfig) -> Self {
        Self { config }
    }

    /// Returns the collector configuration.
    pub fn config(&self) -> &CollectorConfig {
        &self.config
    }

    /// Collects statistics for a single table.
    pub async fn collect_table<C: StatisticsConnection>(
        &self,
        conn: &C,
        schema: Option<&str>,
        table: &str,
    ) -> anyhow::Result<TableStatistics> {
        conn.get_table_statistics(schema, table).await
    }

    /// Collects statistics for all tables in a schema.
    pub async fn collect_schema<C: StatisticsConnection>(
        &self,
        conn: &C,
        schema: Option<&str>,
    ) -> anyhow::Result<SchemaStatistics> {
        let tables = conn.get_all_table_statistics(schema).await?;

        let mut stats = SchemaStatistics::new();
        if let Some(s) = schema {
            stats = stats.with_schema(s);
        }

        for table in tables {
            if self.config.collect_index_stats {
                let indexes = conn
                    .get_table_index_statistics(schema, &table.name)
                    .await
                    .unwrap_or_default();
                for idx in indexes {
                    stats.add_index(idx);
                }
            }
            stats.add_table(table);
        }

        Ok(stats)
    }
}

impl Default for StatisticsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper for generating database-specific statistics queries.
pub struct StatisticsQuery;

impl StatisticsQuery {
    /// Returns SQL for collecting table statistics.
    pub fn table_stats_sql(driver: &str) -> &'static str {
        match driver {
            "postgres" | "postgresql" => {
                r#"SELECT
                    schemaname AS schema_name,
                    relname AS table_name,
                    n_live_tup AS row_count,
                    pg_total_relation_size(schemaname || '.' || relname) AS total_size,
                    pg_relation_size(schemaname || '.' || relname) AS data_size,
                    pg_indexes_size(schemaname || '.' || relname) AS index_size,
                    COALESCE(pg_total_relation_size(schemaname || '.' || relname) - 
                             pg_relation_size(schemaname || '.' || relname) - 
                             pg_indexes_size(schemaname || '.' || relname), 0) AS toast_size,
                    n_dead_tup AS dead_tuples,
                    last_vacuum,
                    last_analyze,
                    CASE WHEN n_live_tup > 0 
                         THEN ROUND(100.0 * n_mod_since_analyze / n_live_tup, 2)
                         ELSE 0 END AS modification_pct
                FROM pg_stat_user_tables
                WHERE schemaname = $1
                ORDER BY n_live_tup DESC"#
            }
            "mysql" => {
                r#"SELECT
                    TABLE_SCHEMA AS schema_name,
                    TABLE_NAME AS table_name,
                    TABLE_ROWS AS row_count,
                    DATA_LENGTH + INDEX_LENGTH AS total_size,
                    DATA_LENGTH AS data_size,
                    INDEX_LENGTH AS index_size,
                    0 AS toast_size,
                    0 AS dead_tuples,
                    NULL AS last_vacuum,
                    UPDATE_TIME AS last_analyze,
                    0 AS modification_pct
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = ?
                  AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_ROWS DESC"#
            }
            "sqlite" => {
                r#"SELECT
                    '' AS schema_name,
                    name AS table_name,
                    0 AS row_count,
                    0 AS total_size,
                    0 AS data_size,
                    0 AS index_size,
                    0 AS toast_size,
                    0 AS dead_tuples,
                    NULL AS last_vacuum,
                    NULL AS last_analyze,
                    0 AS modification_pct
                FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"#
            }
            "mssql" | "sqlserver" => {
                r#"SELECT
                    s.name AS schema_name,
                    t.name AS table_name,
                    SUM(p.rows) AS row_count,
                    SUM(a.total_pages) * 8 * 1024 AS total_size,
                    SUM(a.used_pages) * 8 * 1024 AS data_size,
                    (SUM(a.total_pages) - SUM(a.used_pages)) * 8 * 1024 AS index_size,
                    0 AS toast_size,
                    0 AS dead_tuples,
                    NULL AS last_vacuum,
                    STATS_DATE(t.object_id, 1) AS last_analyze,
                    0 AS modification_pct
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
                INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                WHERE s.name = @schema
                GROUP BY s.name, t.name, t.object_id
                ORDER BY SUM(p.rows) DESC"#
            }
            "clickhouse" => {
                r#"SELECT
                    database AS schema_name,
                    table AS table_name,
                    sum(rows) AS row_count,
                    sum(bytes_on_disk) AS total_size,
                    sum(data_uncompressed_bytes) AS data_size,
                    0 AS index_size,
                    0 AS toast_size,
                    0 AS dead_tuples,
                    NULL AS last_vacuum,
                    max(modification_time) AS last_analyze,
                    0 AS modification_pct
                FROM system.parts
                WHERE database = {db:String}
                  AND active
                GROUP BY database, table
                ORDER BY row_count DESC"#
            }
            _ => {
                r#"SELECT '' AS schema_name, '' AS table_name, 0 AS row_count,
                   0 AS total_size, 0 AS data_size, 0 AS index_size,
                   0 AS toast_size, 0 AS dead_tuples, NULL AS last_vacuum,
                   NULL AS last_analyze, 0 AS modification_pct"#
            }
        }
    }

    /// Returns SQL for collecting index statistics.
    pub fn index_stats_sql(driver: &str) -> &'static str {
        match driver {
            "postgres" | "postgresql" => {
                r#"SELECT
                    schemaname AS schema_name,
                    relname AS table_name,
                    indexrelname AS index_name,
                    pg_relation_size(indexrelid) AS size_bytes,
                    idx_scan AS scans,
                    idx_tup_read AS tuples_returned,
                    idx_tup_fetch AS tuples_fetched
                FROM pg_stat_user_indexes
                WHERE schemaname = $1 AND relname = $2
                ORDER BY pg_relation_size(indexrelid) DESC"#
            }
            "mysql" => {
                r#"SELECT
                    TABLE_SCHEMA AS schema_name,
                    TABLE_NAME AS table_name,
                    INDEX_NAME AS index_name,
                    STAT_VALUE * @@innodb_page_size AS size_bytes,
                    NULL AS scans,
                    NULL AS tuples_returned,
                    NULL AS tuples_fetched
                FROM mysql.innodb_index_stats
                WHERE database_name = ? AND table_name = ? AND stat_name = 'size'
                ORDER BY STAT_VALUE DESC"#
            }
            "sqlite" => {
                r#"SELECT
                    '' AS schema_name,
                    tbl_name AS table_name,
                    name AS index_name,
                    0 AS size_bytes,
                    NULL AS scans,
                    NULL AS tuples_returned,
                    NULL AS tuples_fetched
                FROM sqlite_master
                WHERE type = 'index' AND tbl_name = ?"#
            }
            "mssql" | "sqlserver" => {
                r#"SELECT
                    s.name AS schema_name,
                    t.name AS table_name,
                    i.name AS index_name,
                    SUM(a.total_pages) * 8 * 1024 AS size_bytes,
                    SUM(us.user_seeks + us.user_scans) AS scans,
                    SUM(us.user_lookups) AS tuples_returned,
                    NULL AS tuples_fetched
                FROM sys.indexes i
                INNER JOIN sys.tables t ON i.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                LEFT JOIN sys.allocation_units a ON p.partition_id = a.container_id
                LEFT JOIN sys.dm_db_index_usage_stats us 
                    ON i.object_id = us.object_id AND i.index_id = us.index_id
                WHERE s.name = @schema AND t.name = @table AND i.name IS NOT NULL
                GROUP BY s.name, t.name, i.name
                ORDER BY SUM(a.total_pages) DESC"#
            }
            _ => {
                r#"SELECT '' AS schema_name, '' AS table_name, '' AS index_name,
                   0 AS size_bytes, NULL AS scans, NULL AS tuples_returned,
                   NULL AS tuples_fetched"#
            }
        }
    }
}
