//! Unit tests for server monitoring

use super::*;
use std::time::Duration;

mod server_health_tests {
    use super::*;

    #[test]
    fn test_server_health_is_healthy() {
        assert!(ServerHealth::Healthy.is_healthy());
        assert!(!ServerHealth::Degraded.is_healthy());
        assert!(!ServerHealth::Unhealthy.is_healthy());
        assert!(!ServerHealth::Unknown.is_healthy());
    }

    #[test]
    fn test_server_health_is_operational() {
        assert!(ServerHealth::Healthy.is_operational());
        assert!(ServerHealth::Degraded.is_operational());
        assert!(!ServerHealth::Unhealthy.is_operational());
        assert!(!ServerHealth::Unknown.is_operational());
    }

    #[test]
    fn test_server_health_default() {
        let health = ServerHealth::default();
        assert_eq!(health, ServerHealth::Unknown);
    }

    #[test]
    fn test_server_health_display() {
        assert_eq!(format!("{}", ServerHealth::Healthy), "Healthy");
        assert_eq!(format!("{}", ServerHealth::Degraded), "Degraded");
        assert_eq!(format!("{}", ServerHealth::Unhealthy), "Unhealthy");
        assert_eq!(format!("{}", ServerHealth::Unknown), "Unknown");
    }

    #[test]
    fn test_server_health_serialization() {
        let health = ServerHealth::Healthy;
        let json = serde_json::to_string(&health).unwrap();
        assert_eq!(json, "\"healthy\"");

        let deserialized: ServerHealth = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, ServerHealth::Healthy);
    }
}

mod server_status_tests {
    use super::*;

    #[test]
    fn test_server_status_creation() {
        let status =
            ServerStatus::new("PostgreSQL 15.4".to_string(), Duration::from_secs(3600), 10);

        assert_eq!(status.version, "PostgreSQL 15.4");
        assert_eq!(status.uptime, Duration::from_secs(3600));
        assert_eq!(status.active_connections, 10);
        assert!(status.max_connections.is_none());
        assert_eq!(status.health, ServerHealth::Healthy);
    }

    #[test]
    fn test_server_status_builder_pattern() {
        let status = ServerStatus::new("MySQL 8.0".to_string(), Duration::from_secs(7200), 5)
            .with_max_connections(100)
            .with_health(ServerHealth::Degraded)
            .with_hostname("db.example.com".to_string())
            .with_process_id(1234)
            .with_database("mydb".to_string())
            .with_response_time(Duration::from_millis(50));

        assert_eq!(status.max_connections, Some(100));
        assert_eq!(status.health, ServerHealth::Degraded);
        assert_eq!(status.hostname, Some("db.example.com".to_string()));
        assert_eq!(status.process_id, Some(1234));
        assert_eq!(status.database, Some("mydb".to_string()));
        assert_eq!(status.response_time, Duration::from_millis(50));
    }

    #[test]
    fn test_connection_usage_percent() {
        let status =
            ServerStatus::new("PostgreSQL 15.4".to_string(), Duration::from_secs(3600), 50)
                .with_max_connections(100);

        assert_eq!(status.connection_usage_percent(), Some(50.0));

        let status_no_max =
            ServerStatus::new("PostgreSQL 15.4".to_string(), Duration::from_secs(3600), 50);
        assert!(status_no_max.connection_usage_percent().is_none());

        let status_zero_max =
            ServerStatus::new("PostgreSQL 15.4".to_string(), Duration::from_secs(3600), 50)
                .with_max_connections(0);
        assert_eq!(status_zero_max.connection_usage_percent(), Some(0.0));
    }

    #[test]
    fn test_connection_limit_approaching() {
        let status_high =
            ServerStatus::new("PostgreSQL 15.4".to_string(), Duration::from_secs(3600), 85)
                .with_max_connections(100);
        assert!(status_high.is_connection_limit_approaching());

        let status_low =
            ServerStatus::new("PostgreSQL 15.4".to_string(), Duration::from_secs(3600), 50)
                .with_max_connections(100);
        assert!(!status_low.is_connection_limit_approaching());

        let status_boundary =
            ServerStatus::new("PostgreSQL 15.4".to_string(), Duration::from_secs(3600), 80)
                .with_max_connections(100);
        assert!(status_boundary.is_connection_limit_approaching());

        let status_no_max =
            ServerStatus::new("PostgreSQL 15.4".to_string(), Duration::from_secs(3600), 85);
        assert!(!status_no_max.is_connection_limit_approaching());
    }

    #[test]
    fn test_uptime_display() {
        let status_seconds = ServerStatus::new("v1".to_string(), Duration::from_secs(45), 0);
        assert_eq!(status_seconds.uptime_display(), "45s");

        let status_minutes = ServerStatus::new("v1".to_string(), Duration::from_secs(125), 0);
        assert_eq!(status_minutes.uptime_display(), "2m 5s");

        let status_hours = ServerStatus::new("v1".to_string(), Duration::from_secs(3665), 0);
        assert_eq!(status_hours.uptime_display(), "1h 1m 5s");

        let status_days = ServerStatus::new("v1".to_string(), Duration::from_secs(90061), 0);
        assert_eq!(status_days.uptime_display(), "1d 1h 1m 1s");
    }

    #[test]
    fn test_server_status_serialization() {
        let status =
            ServerStatus::new("PostgreSQL 15.4".to_string(), Duration::from_secs(3600), 10)
                .with_max_connections(100);

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"version\":\"PostgreSQL 15.4\""));
        assert!(json.contains("\"active_connections\":10"));
        assert!(json.contains("\"max_connections\":100"));

        let deserialized: ServerStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.version, status.version);
        assert_eq!(deserialized.active_connections, status.active_connections);
    }
}

mod monitor_config_tests {
    use super::*;

    #[test]
    fn test_monitor_config_default() {
        let config = MonitorConfig::default();
        assert_eq!(config.healthy_response_ms, 100);
        assert_eq!(config.degraded_response_ms, 500);
        assert_eq!(config.connection_warning_percent, 80.0);
    }

    #[test]
    fn test_monitor_config_custom() {
        let config = MonitorConfig::new(50, 200).with_connection_warning(90.0);

        assert_eq!(config.healthy_response_ms, 50);
        assert_eq!(config.degraded_response_ms, 200);
        assert_eq!(config.connection_warning_percent, 90.0);
    }

    #[test]
    fn test_monitor_config_serialization() {
        let config = MonitorConfig::new(50, 200);
        let json = serde_json::to_string(&config).unwrap();

        let deserialized: MonitorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.healthy_response_ms, 50);
        assert_eq!(deserialized.degraded_response_ms, 200);
    }
}

mod server_monitor_tests {
    use super::*;

    #[test]
    fn test_server_monitor_creation() {
        let monitor = ServerMonitor::new();
        assert_eq!(monitor.config().healthy_response_ms, 100);
        assert_eq!(monitor.config().degraded_response_ms, 500);
    }

    #[test]
    fn test_server_monitor_with_config() {
        let config = MonitorConfig::new(25, 100);
        let monitor = ServerMonitor::with_config(config);

        assert_eq!(monitor.config().healthy_response_ms, 25);
        assert_eq!(monitor.config().degraded_response_ms, 100);
    }

    #[test]
    fn test_classify_health_by_response_time() {
        let monitor = ServerMonitor::new();

        // Healthy: <= 100ms
        assert_eq!(
            monitor.classify_health_by_response_time(Duration::from_millis(50)),
            ServerHealth::Healthy
        );
        assert_eq!(
            monitor.classify_health_by_response_time(Duration::from_millis(100)),
            ServerHealth::Healthy
        );

        // Degraded: > 100ms and <= 500ms
        assert_eq!(
            monitor.classify_health_by_response_time(Duration::from_millis(101)),
            ServerHealth::Degraded
        );
        assert_eq!(
            monitor.classify_health_by_response_time(Duration::from_millis(500)),
            ServerHealth::Degraded
        );

        // Unhealthy: > 500ms
        assert_eq!(
            monitor.classify_health_by_response_time(Duration::from_millis(501)),
            ServerHealth::Unhealthy
        );
        assert_eq!(
            monitor.classify_health_by_response_time(Duration::from_secs(2)),
            ServerHealth::Unhealthy
        );
    }

    #[test]
    fn test_server_monitor_default() {
        let monitor = ServerMonitor::default();
        assert_eq!(monitor.config().healthy_response_ms, 100);
    }
}

mod server_status_query_tests {
    use super::*;

    #[test]
    fn test_postgres_query() {
        let query = ServerStatusQuery::postgres();
        assert!(query.contains("version()"));
        assert!(query.contains("pg_postmaster_start_time"));
        assert!(query.contains("pg_stat_activity"));
        assert!(query.contains("max_connections"));
    }

    #[test]
    fn test_mysql_query() {
        let query = ServerStatusQuery::mysql();
        assert!(query.contains("@@version"));
        assert!(query.contains("uptime"));
        assert!(query.contains("max_connections"));
        assert!(query.contains("processlist"));
    }

    #[test]
    fn test_sqlite_query() {
        let query = ServerStatusQuery::sqlite();
        assert!(query.contains("sqlite_version()"));
    }

    #[test]
    fn test_mssql_query() {
        let query = ServerStatusQuery::mssql();
        assert!(query.contains("@@VERSION"));
        assert!(query.contains("sys.dm_exec_sessions"));
    }

    #[test]
    fn test_clickhouse_query() {
        let query = ServerStatusQuery::clickhouse();
        assert!(query.contains("version()"));
        assert!(query.contains("uptime()"));
    }

    #[test]
    fn test_duckdb_query() {
        let query = ServerStatusQuery::duckdb();
        assert!(query.contains("duckdb_version()"));
    }

    #[test]
    fn test_for_driver_supported() {
        assert!(ServerStatusQuery::for_driver("postgresql").is_ok());
        assert!(ServerStatusQuery::for_driver("postgres").is_ok());
        assert!(ServerStatusQuery::for_driver("mysql").is_ok());
        assert!(ServerStatusQuery::for_driver("sqlite").is_ok());
        assert!(ServerStatusQuery::for_driver("mssql").is_ok());
        assert!(ServerStatusQuery::for_driver("sqlserver").is_ok());
        assert!(ServerStatusQuery::for_driver("clickhouse").is_ok());
        assert!(ServerStatusQuery::for_driver("duckdb").is_ok());
    }

    #[test]
    fn test_for_driver_unsupported() {
        let result = ServerStatusQuery::for_driver("unknown_db");
        assert!(result.is_err());
    }
}
