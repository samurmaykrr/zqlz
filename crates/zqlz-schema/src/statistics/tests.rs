//! Tests for statistics collector module.

use super::collector::*;
use chrono::Utc;

mod table_statistics_tests {
    use super::*;

    #[test]
    fn test_new_creates_with_basic_fields() {
        let stats = TableStatistics::new("users", 1000, 65536);

        assert_eq!(stats.name, "users");
        assert_eq!(stats.row_count, 1000);
        assert_eq!(stats.size_bytes, 65536);
        assert!(stats.schema.is_none());
        assert_eq!(stats.index_size_bytes, 0);
    }

    #[test]
    fn test_with_schema_sets_schema() {
        let stats = TableStatistics::new("users", 100, 1024).with_schema("public");

        assert_eq!(stats.schema, Some("public".to_string()));
    }

    #[test]
    fn test_with_index_size_sets_index_size() {
        let stats = TableStatistics::new("users", 100, 1024).with_index_size(2048);

        assert_eq!(stats.index_size_bytes, 2048);
    }

    #[test]
    fn test_with_toast_size_sets_toast_size() {
        let stats = TableStatistics::new("users", 100, 1024).with_toast_size(512);

        assert_eq!(stats.toast_size_bytes, 512);
    }

    #[test]
    fn test_total_size_sums_all_sizes() {
        let stats = TableStatistics::new("users", 100, 1000)
            .with_index_size(500)
            .with_toast_size(200);

        assert_eq!(stats.total_size(), 1700);
    }

    #[test]
    fn test_qualified_name_without_schema() {
        let stats = TableStatistics::new("users", 100, 1024);

        assert_eq!(stats.qualified_name(), "users");
    }

    #[test]
    fn test_qualified_name_with_schema() {
        let stats = TableStatistics::new("users", 100, 1024).with_schema("public");

        assert_eq!(stats.qualified_name(), "public.users");
    }

    #[test]
    fn test_needs_vacuum_false_without_dead_tuples() {
        let stats = TableStatistics::new("users", 1000, 1024);

        assert!(!stats.needs_vacuum());
    }

    #[test]
    fn test_needs_vacuum_false_when_dead_tuples_low() {
        let stats = TableStatistics::new("users", 1000, 1024).with_dead_tuples(50); // 5%

        assert!(!stats.needs_vacuum());
    }

    #[test]
    fn test_needs_vacuum_true_when_dead_tuples_high() {
        let stats = TableStatistics::new("users", 1000, 1024).with_dead_tuples(150); // 15%

        assert!(stats.needs_vacuum());
    }

    #[test]
    fn test_needs_analyze_false_without_modification() {
        let stats = TableStatistics::new("users", 1000, 1024);

        assert!(!stats.needs_analyze());
    }

    #[test]
    fn test_needs_analyze_false_when_modification_low() {
        let stats = TableStatistics::new("users", 1000, 1024).with_modification_percentage(5.0);

        assert!(!stats.needs_analyze());
    }

    #[test]
    fn test_needs_analyze_true_when_modification_high() {
        let stats = TableStatistics::new("users", 1000, 1024).with_modification_percentage(15.0);

        assert!(stats.needs_analyze());
    }

    #[test]
    fn test_builder_chain() {
        let timestamp = Utc::now();
        let stats = TableStatistics::new("orders", 5000, 102400)
            .with_schema("sales")
            .with_index_size(20480)
            .with_toast_size(1024)
            .with_last_analyze(timestamp)
            .with_last_vacuum(timestamp)
            .with_dead_tuples(100)
            .with_modification_percentage(2.5);

        assert_eq!(stats.schema, Some("sales".to_string()));
        assert_eq!(stats.name, "orders");
        assert_eq!(stats.row_count, 5000);
        assert_eq!(stats.size_bytes, 102400);
        assert_eq!(stats.index_size_bytes, 20480);
        assert_eq!(stats.toast_size_bytes, 1024);
        assert_eq!(stats.dead_tuples, Some(100));
        assert_eq!(stats.modification_percentage, Some(2.5));
    }

    #[test]
    fn test_serialization() {
        let stats = TableStatistics::new("users", 100, 1024).with_schema("public");
        let json = serde_json::to_string(&stats).expect("should serialize");

        assert!(json.contains("\"name\":\"users\""));
        assert!(json.contains("\"schema\":\"public\""));
        assert!(json.contains("\"row_count\":100"));
    }
}

mod index_statistics_tests {
    use super::*;

    #[test]
    fn test_new_creates_with_basic_fields() {
        let stats = IndexStatistics::new("users", "users_pkey", 4096);

        assert_eq!(stats.table_name, "users");
        assert_eq!(stats.index_name, "users_pkey");
        assert_eq!(stats.size_bytes, 4096);
        assert!(stats.schema.is_none());
    }

    #[test]
    fn test_with_schema_sets_schema() {
        let stats = IndexStatistics::new("users", "users_pkey", 4096).with_schema("public");

        assert_eq!(stats.schema, Some("public".to_string()));
    }

    #[test]
    fn test_with_scans_sets_scan_stats() {
        let stats = IndexStatistics::new("users", "users_pkey", 4096).with_scans(100, 500, 450);

        assert_eq!(stats.scans, Some(100));
        assert_eq!(stats.tuples_returned, Some(500));
        assert_eq!(stats.tuples_fetched, Some(450));
    }

    #[test]
    fn test_is_unused_false_without_scans() {
        let stats = IndexStatistics::new("users", "users_pkey", 4096);

        assert!(!stats.is_unused()); // No data, so not definitively unused
    }

    #[test]
    fn test_is_unused_false_when_scans_nonzero() {
        let stats = IndexStatistics::new("users", "users_pkey", 4096).with_scans(10, 50, 45);

        assert!(!stats.is_unused());
    }

    #[test]
    fn test_is_unused_true_when_scans_zero() {
        let stats = IndexStatistics::new("users", "users_idx", 4096).with_scans(0, 0, 0);

        assert!(stats.is_unused());
    }

    #[test]
    fn test_serialization() {
        let stats = IndexStatistics::new("users", "users_pkey", 4096)
            .with_schema("public")
            .with_scans(100, 500, 450);
        let json = serde_json::to_string(&stats).expect("should serialize");

        assert!(json.contains("\"index_name\":\"users_pkey\""));
        assert!(json.contains("\"scans\":100"));
    }
}

mod schema_statistics_tests {
    use super::*;

    #[test]
    fn test_new_creates_empty() {
        let stats = SchemaStatistics::new();

        assert_eq!(stats.total_rows, 0);
        assert_eq!(stats.total_size_bytes, 0);
        assert_eq!(stats.table_count, 0);
        assert!(stats.tables.is_empty());
    }

    #[test]
    fn test_with_schema_sets_schema() {
        let stats = SchemaStatistics::new().with_schema("public");

        assert_eq!(stats.schema, Some("public".to_string()));
    }

    #[test]
    fn test_add_table_updates_totals() {
        let mut stats = SchemaStatistics::new();
        let table = TableStatistics::new("users", 1000, 10000).with_index_size(2000);

        stats.add_table(table);

        assert_eq!(stats.total_rows, 1000);
        assert_eq!(stats.total_size_bytes, 10000);
        assert_eq!(stats.total_index_bytes, 2000);
        assert_eq!(stats.table_count, 1);
    }

    #[test]
    fn test_add_multiple_tables_accumulates() {
        let mut stats = SchemaStatistics::new();

        stats.add_table(TableStatistics::new("users", 1000, 10000).with_index_size(2000));
        stats.add_table(TableStatistics::new("orders", 5000, 50000).with_index_size(10000));

        assert_eq!(stats.total_rows, 6000);
        assert_eq!(stats.total_size_bytes, 60000);
        assert_eq!(stats.total_index_bytes, 12000);
        assert_eq!(stats.table_count, 2);
    }

    #[test]
    fn test_add_index_updates_count() {
        let mut stats = SchemaStatistics::new();

        stats.add_index(IndexStatistics::new("users", "users_pkey", 4096));
        stats.add_index(IndexStatistics::new("users", "users_email_idx", 2048));

        assert_eq!(stats.index_count, 2);
    }

    #[test]
    fn test_tables_needing_vacuum() {
        let mut stats = SchemaStatistics::new();

        stats.add_table(TableStatistics::new("users", 1000, 10000).with_dead_tuples(50)); // 5%
        stats.add_table(TableStatistics::new("logs", 1000, 10000).with_dead_tuples(200)); // 20%
        stats.add_table(TableStatistics::new("configs", 100, 1000)); // no dead tuples

        let needing_vacuum = stats.tables_needing_vacuum();
        assert_eq!(needing_vacuum.len(), 1);
        assert_eq!(needing_vacuum[0].name, "logs");
    }

    #[test]
    fn test_tables_needing_analyze() {
        let mut stats = SchemaStatistics::new();

        stats.add_table(
            TableStatistics::new("users", 1000, 10000).with_modification_percentage(5.0),
        );
        stats.add_table(
            TableStatistics::new("sessions", 1000, 10000).with_modification_percentage(25.0),
        );

        let needing_analyze = stats.tables_needing_analyze();
        assert_eq!(needing_analyze.len(), 1);
        assert_eq!(needing_analyze[0].name, "sessions");
    }

    #[test]
    fn test_unused_indexes() {
        let mut stats = SchemaStatistics::new();

        stats
            .add_index(IndexStatistics::new("users", "users_pkey", 4096).with_scans(100, 500, 450));
        stats.add_index(IndexStatistics::new("users", "users_unused", 4096).with_scans(0, 0, 0));

        let unused = stats.unused_indexes();
        assert_eq!(unused.len(), 1);
        assert_eq!(unused[0].index_name, "users_unused");
    }
}

mod collector_config_tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = CollectorConfig::default();

        assert!(config.collect_index_stats);
        assert!(config.collect_maintenance_stats);
        assert!(config.default_schema.is_none());
        assert_eq!(config.parallel_limit, 10);
    }

    #[test]
    fn test_builder_pattern() {
        let config = CollectorConfig::new()
            .with_index_stats(false)
            .with_maintenance_stats(false)
            .with_default_schema("myschema")
            .with_parallel_limit(5);

        assert!(!config.collect_index_stats);
        assert!(!config.collect_maintenance_stats);
        assert_eq!(config.default_schema, Some("myschema".to_string()));
        assert_eq!(config.parallel_limit, 5);
    }
}

mod statistics_collector_tests {
    use super::*;

    #[test]
    fn test_new_creates_with_defaults() {
        let collector = StatisticsCollector::new();
        let config = collector.config();

        assert!(config.collect_index_stats);
        assert!(config.collect_maintenance_stats);
    }

    #[test]
    fn test_with_config_uses_custom_config() {
        let config = CollectorConfig::new()
            .with_index_stats(false)
            .with_default_schema("test");

        let collector = StatisticsCollector::with_config(config);

        assert!(!collector.config().collect_index_stats);
        assert_eq!(collector.config().default_schema, Some("test".to_string()));
    }

    #[test]
    fn test_default_creates_new() {
        let collector = StatisticsCollector::default();

        assert!(collector.config().collect_index_stats);
    }
}

mod statistics_query_tests {
    use super::*;

    #[test]
    fn test_postgres_table_stats_sql() {
        let sql = StatisticsQuery::table_stats_sql("postgres");

        assert!(sql.contains("pg_stat_user_tables"));
        assert!(sql.contains("n_live_tup"));
        assert!(sql.contains("pg_total_relation_size"));
    }

    #[test]
    fn test_mysql_table_stats_sql() {
        let sql = StatisticsQuery::table_stats_sql("mysql");

        assert!(sql.contains("information_schema.TABLES"));
        assert!(sql.contains("TABLE_ROWS"));
        assert!(sql.contains("DATA_LENGTH"));
    }

    #[test]
    fn test_sqlite_table_stats_sql() {
        let sql = StatisticsQuery::table_stats_sql("sqlite");

        assert!(sql.contains("sqlite_master"));
        assert!(sql.contains("type = 'table'"));
    }

    #[test]
    fn test_mssql_table_stats_sql() {
        let sql = StatisticsQuery::table_stats_sql("mssql");

        assert!(sql.contains("sys.tables"));
        assert!(sql.contains("sys.partitions"));
    }

    #[test]
    fn test_clickhouse_table_stats_sql() {
        let sql = StatisticsQuery::table_stats_sql("clickhouse");

        assert!(sql.contains("system.parts"));
        assert!(sql.contains("bytes_on_disk"));
    }

    #[test]
    fn test_unknown_driver_returns_fallback() {
        let sql = StatisticsQuery::table_stats_sql("unknown");

        assert!(sql.contains("0 AS row_count"));
    }

    #[test]
    fn test_postgres_index_stats_sql() {
        let sql = StatisticsQuery::index_stats_sql("postgres");

        assert!(sql.contains("pg_stat_user_indexes"));
        assert!(sql.contains("idx_scan"));
    }

    #[test]
    fn test_mysql_index_stats_sql() {
        let sql = StatisticsQuery::index_stats_sql("mysql");

        assert!(sql.contains("innodb_index_stats"));
    }

    #[test]
    fn test_sqlite_index_stats_sql() {
        let sql = StatisticsQuery::index_stats_sql("sqlite");

        assert!(sql.contains("sqlite_master"));
        assert!(sql.contains("type = 'index'"));
    }

    #[test]
    fn test_mssql_index_stats_sql() {
        let sql = StatisticsQuery::index_stats_sql("mssql");

        assert!(sql.contains("sys.indexes"));
        assert!(sql.contains("dm_db_index_usage_stats"));
    }
}
