//! Unit tests for performance metrics collection

use super::*;
use std::time::Duration;

mod query_stats_tests {
    use super::*;

    #[test]
    fn test_query_stats_new() {
        let stats = QueryStats::new();
        assert_eq!(stats.total_queries, 0);
        assert_eq!(stats.select_queries, 0);
        assert_eq!(stats.insert_queries, 0);
        assert_eq!(stats.avg_query_time_ms, 0.0);
    }

    #[test]
    fn test_query_stats_builder() {
        let stats = QueryStats::new()
            .with_total_queries(1000)
            .with_select_queries(700)
            .with_insert_queries(150)
            .with_update_queries(100)
            .with_delete_queries(50)
            .with_avg_query_time_ms(25.5)
            .with_max_query_time_ms(500.0)
            .with_slow_queries(10)
            .with_qps(100.0);

        assert_eq!(stats.total_queries, 1000);
        assert_eq!(stats.select_queries, 700);
        assert_eq!(stats.insert_queries, 150);
        assert_eq!(stats.update_queries, 100);
        assert_eq!(stats.delete_queries, 50);
        assert_eq!(stats.avg_query_time_ms, 25.5);
        assert_eq!(stats.max_query_time_ms, 500.0);
        assert_eq!(stats.slow_queries, 10);
        assert_eq!(stats.queries_per_second, 100.0);
    }

    #[test]
    fn test_write_percentage() {
        let stats = QueryStats::new()
            .with_total_queries(100)
            .with_select_queries(70)
            .with_insert_queries(15)
            .with_update_queries(10)
            .with_delete_queries(5);

        assert_eq!(stats.write_percentage(), 30.0);
    }

    #[test]
    fn test_read_percentage() {
        let stats = QueryStats::new()
            .with_total_queries(100)
            .with_select_queries(70);

        assert_eq!(stats.read_percentage(), 70.0);
    }

    #[test]
    fn test_percentages_with_zero_queries() {
        let stats = QueryStats::new();
        assert_eq!(stats.write_percentage(), 0.0);
        assert_eq!(stats.read_percentage(), 0.0);
    }

    #[test]
    fn test_query_stats_serialization() {
        let stats = QueryStats::new()
            .with_total_queries(500)
            .with_avg_query_time_ms(15.5);

        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("\"total_queries\":500"));
        assert!(json.contains("\"avg_query_time_ms\":15.5"));

        let deserialized: QueryStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_queries, 500);
        assert_eq!(deserialized.avg_query_time_ms, 15.5);
    }
}

mod cache_stats_tests {
    use super::*;

    #[test]
    fn test_cache_stats_new() {
        let stats = CacheStats::new();
        assert_eq!(stats.buffer_pool_size, 0);
        assert_eq!(stats.cache_hit_ratio, 0.0);
    }

    #[test]
    fn test_cache_stats_builder() {
        let stats = CacheStats::new()
            .with_buffer_pool_size(1_073_741_824) // 1GB
            .with_buffer_pool_used(805_306_368) // 768MB
            .with_cache_hit_ratio(0.95)
            .with_cache_reads(100_000)
            .with_disk_reads(5_000)
            .with_dirty_pages(100);

        assert_eq!(stats.buffer_pool_size, 1_073_741_824);
        assert_eq!(stats.buffer_pool_used, 805_306_368);
        assert_eq!(stats.cache_hit_ratio, 0.95);
        assert_eq!(stats.cache_reads, 100_000);
        assert_eq!(stats.disk_reads, 5_000);
        assert_eq!(stats.dirty_pages, 100);
    }

    #[test]
    fn test_cache_hit_ratio_clamping() {
        let stats = CacheStats::new().with_cache_hit_ratio(1.5);
        assert_eq!(stats.cache_hit_ratio, 1.0);

        let stats = CacheStats::new().with_cache_hit_ratio(-0.5);
        assert_eq!(stats.cache_hit_ratio, 0.0);
    }

    #[test]
    fn test_buffer_pool_usage_percent() {
        let stats = CacheStats::new()
            .with_buffer_pool_size(1000)
            .with_buffer_pool_used(750);

        assert_eq!(stats.buffer_pool_usage_percent(), 75.0);

        let empty_stats = CacheStats::new();
        assert_eq!(empty_stats.buffer_pool_usage_percent(), 0.0);
    }

    #[test]
    fn test_calculate_hit_ratio() {
        let stats = CacheStats::new().with_cache_reads(950).with_disk_reads(50);

        assert_eq!(stats.calculate_hit_ratio(), 0.95);

        let empty_stats = CacheStats::new();
        assert_eq!(empty_stats.calculate_hit_ratio(), 0.0);
    }

    #[test]
    fn test_is_cache_healthy() {
        let healthy = CacheStats::new().with_cache_hit_ratio(0.95);
        assert!(healthy.is_cache_healthy());

        let boundary = CacheStats::new().with_cache_hit_ratio(0.90);
        assert!(boundary.is_cache_healthy());

        let unhealthy = CacheStats::new().with_cache_hit_ratio(0.85);
        assert!(!unhealthy.is_cache_healthy());
    }

    #[test]
    fn test_cache_stats_serialization() {
        let stats = CacheStats::new()
            .with_buffer_pool_size(1_000_000)
            .with_cache_hit_ratio(0.92);

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: CacheStats = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.buffer_pool_size, 1_000_000);
        assert_eq!(deserialized.cache_hit_ratio, 0.92);
    }
}

mod performance_metrics_tests {
    use super::*;

    #[test]
    fn test_performance_metrics_creation() {
        let query_stats = QueryStats::new().with_total_queries(500);
        let cache_stats = CacheStats::new().with_cache_hit_ratio(0.95);

        let metrics = PerformanceMetrics::new(query_stats, cache_stats);

        assert_eq!(metrics.query_stats.total_queries, 500);
        assert_eq!(metrics.cache_stats.cache_hit_ratio, 0.95);
        assert!(metrics.database.is_none());
        assert!(metrics.driver.is_none());
    }

    #[test]
    fn test_performance_metrics_builder() {
        let metrics = PerformanceMetrics::new(QueryStats::new(), CacheStats::new())
            .with_collection_time(Duration::from_millis(50))
            .with_database("mydb".to_string())
            .with_driver("postgresql".to_string());

        assert_eq!(metrics.collection_time, Duration::from_millis(50));
        assert_eq!(metrics.database, Some("mydb".to_string()));
        assert_eq!(metrics.driver, Some("postgresql".to_string()));
    }

    #[test]
    fn test_is_healthy() {
        // Healthy: good cache ratio and low query time
        let healthy_metrics = PerformanceMetrics::new(
            QueryStats::new().with_avg_query_time_ms(50.0),
            CacheStats::new().with_cache_hit_ratio(0.95),
        );
        assert!(healthy_metrics.is_healthy());

        // Unhealthy: slow queries
        let slow_metrics = PerformanceMetrics::new(
            QueryStats::new().with_avg_query_time_ms(150.0),
            CacheStats::new().with_cache_hit_ratio(0.95),
        );
        assert!(!slow_metrics.is_healthy());

        // Unhealthy: low cache hit ratio
        let low_cache_metrics = PerformanceMetrics::new(
            QueryStats::new().with_avg_query_time_ms(50.0),
            CacheStats::new().with_cache_hit_ratio(0.70),
        );
        assert!(!low_cache_metrics.is_healthy());
    }

    #[test]
    fn test_summary() {
        let metrics = PerformanceMetrics::new(
            QueryStats::new()
                .with_qps(150.5)
                .with_avg_query_time_ms(25.3),
            CacheStats::new().with_cache_hit_ratio(0.945),
        );

        let summary = metrics.summary();
        assert!(summary.contains("QPS: 150.5"));
        assert!(summary.contains("Avg Query: 25.3ms"));
        assert!(summary.contains("Cache Hit: 94.5%"));
    }

    #[test]
    fn test_performance_metrics_serialization() {
        let metrics = PerformanceMetrics::new(
            QueryStats::new().with_total_queries(1000),
            CacheStats::new().with_cache_hit_ratio(0.90),
        )
        .with_driver("postgresql".to_string());

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("\"total_queries\":1000"));
        assert!(json.contains("\"cache_hit_ratio\":0.9"));
        assert!(json.contains("\"driver\":\"postgresql\""));

        let deserialized: PerformanceMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.query_stats.total_queries, 1000);
        assert_eq!(deserialized.driver, Some("postgresql".to_string()));
    }
}

mod collector_config_tests {
    use super::*;

    #[test]
    fn test_collector_config_default() {
        let config = CollectorConfig::default();
        assert_eq!(config.slow_query_threshold_ms, 1000);
        assert!(config.include_query_breakdown);
        assert!(config.include_cache_stats);
    }

    #[test]
    fn test_collector_config_builder() {
        let config = CollectorConfig::new(500)
            .with_query_breakdown(false)
            .with_cache_stats(false);

        assert_eq!(config.slow_query_threshold_ms, 500);
        assert!(!config.include_query_breakdown);
        assert!(!config.include_cache_stats);
    }

    #[test]
    fn test_collector_config_serialization() {
        let config = CollectorConfig::new(2000);
        let json = serde_json::to_string(&config).unwrap();

        let deserialized: CollectorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.slow_query_threshold_ms, 2000);
    }
}

mod performance_collector_tests {
    use super::*;

    #[test]
    fn test_collector_creation() {
        let collector = PerformanceCollector::new();
        assert_eq!(collector.config().slow_query_threshold_ms, 1000);
    }

    #[test]
    fn test_collector_with_config() {
        let config = CollectorConfig::new(500);
        let collector = PerformanceCollector::with_config(config);
        assert_eq!(collector.config().slow_query_threshold_ms, 500);
    }

    #[test]
    fn test_collector_default() {
        let collector = PerformanceCollector::default();
        assert_eq!(collector.config().slow_query_threshold_ms, 1000);
    }
}

mod performance_query_tests {
    use super::*;

    #[test]
    fn test_postgres_query() {
        let query = PerformanceQuery::postgres();
        assert!(query.contains("pg_stat_statements"));
        assert!(query.contains("pg_stat_database"));
        assert!(query.contains("cache_hit_ratio"));
        assert!(query.contains("shared_buffers"));
    }

    #[test]
    fn test_mysql_query() {
        let query = PerformanceQuery::mysql();
        assert!(query.contains("performance_schema"));
        assert!(query.contains("Questions"));
        assert!(query.contains("innodb_buffer_pool"));
    }

    #[test]
    fn test_sqlite_query() {
        let query = PerformanceQuery::sqlite();
        // SQLite has limited metrics
        assert!(query.contains("total_queries"));
    }

    #[test]
    fn test_mssql_query() {
        let query = PerformanceQuery::mssql();
        assert!(query.contains("dm_os_performance_counters"));
        assert!(query.contains("Buffer Manager"));
    }

    #[test]
    fn test_clickhouse_query() {
        let query = PerformanceQuery::clickhouse();
        assert!(query.contains("system.events"));
        assert!(query.contains("SelectQuery"));
    }

    #[test]
    fn test_duckdb_query() {
        let query = PerformanceQuery::duckdb();
        // DuckDB has limited metrics
        assert!(query.contains("total_queries"));
    }

    #[test]
    fn test_for_driver_supported() {
        assert!(PerformanceQuery::for_driver("postgresql").is_ok());
        assert!(PerformanceQuery::for_driver("postgres").is_ok());
        assert!(PerformanceQuery::for_driver("mysql").is_ok());
        assert!(PerformanceQuery::for_driver("sqlite").is_ok());
        assert!(PerformanceQuery::for_driver("mssql").is_ok());
        assert!(PerformanceQuery::for_driver("sqlserver").is_ok());
        assert!(PerformanceQuery::for_driver("clickhouse").is_ok());
        assert!(PerformanceQuery::for_driver("duckdb").is_ok());
    }

    #[test]
    fn test_for_driver_unsupported() {
        let result = PerformanceQuery::for_driver("unknown_db");
        assert!(result.is_err());
    }
}
