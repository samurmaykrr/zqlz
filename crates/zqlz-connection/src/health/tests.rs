//! Tests for the health module

use super::*;
use std::time::Duration;

mod status_tests {
    use super::*;

    #[test]
    fn test_health_status_healthy() {
        let status = HealthStatus::from_latency(Duration::from_millis(50));
        assert_eq!(status, HealthStatus::Healthy);
        assert!(status.is_healthy());
        assert!(status.is_usable());
    }

    #[test]
    fn test_health_status_degraded() {
        let status = HealthStatus::from_latency(Duration::from_millis(200));
        assert_eq!(status, HealthStatus::Degraded);
        assert!(!status.is_healthy());
        assert!(status.is_usable());
    }

    #[test]
    fn test_health_status_unhealthy() {
        let status = HealthStatus::from_latency(Duration::from_millis(1000));
        assert_eq!(status, HealthStatus::Unhealthy);
        assert!(!status.is_healthy());
        assert!(!status.is_usable());
    }

    #[test]
    fn test_health_status_at_threshold_boundary() {
        // Exactly at healthy threshold - should be healthy
        let status = HealthStatus::from_latency(Duration::from_millis(100));
        assert_eq!(status, HealthStatus::Healthy);

        // Just over healthy threshold - should be degraded
        let status = HealthStatus::from_latency(Duration::from_millis(101));
        assert_eq!(status, HealthStatus::Degraded);

        // Exactly at degraded threshold - should be degraded
        let status = HealthStatus::from_latency(Duration::from_millis(500));
        assert_eq!(status, HealthStatus::Degraded);

        // Just over degraded threshold - should be unhealthy
        let status = HealthStatus::from_latency(Duration::from_millis(501));
        assert_eq!(status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_status_custom_thresholds() {
        let thresholds = HealthThresholds::new(50, 200);

        let status =
            HealthStatus::from_latency_with_thresholds(Duration::from_millis(30), &thresholds);
        assert_eq!(status, HealthStatus::Healthy);

        let status =
            HealthStatus::from_latency_with_thresholds(Duration::from_millis(100), &thresholds);
        assert_eq!(status, HealthStatus::Degraded);

        let status =
            HealthStatus::from_latency_with_thresholds(Duration::from_millis(300), &thresholds);
        assert_eq!(status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_status_serialization() {
        let status = HealthStatus::Healthy;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"healthy\"");

        let status = HealthStatus::Degraded;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"degraded\"");

        let status = HealthStatus::Unhealthy;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"unhealthy\"");
    }

    #[test]
    fn test_health_status_deserialization() {
        let status: HealthStatus = serde_json::from_str("\"healthy\"").unwrap();
        assert_eq!(status, HealthStatus::Healthy);

        let status: HealthStatus = serde_json::from_str("\"degraded\"").unwrap();
        assert_eq!(status, HealthStatus::Degraded);

        let status: HealthStatus = serde_json::from_str("\"unhealthy\"").unwrap();
        assert_eq!(status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_status_default() {
        let status = HealthStatus::default();
        assert_eq!(status, HealthStatus::Healthy);
    }

    #[test]
    fn test_health_thresholds_default() {
        let thresholds = HealthThresholds::default();
        assert_eq!(thresholds.healthy_threshold, Duration::from_millis(100));
        assert_eq!(thresholds.degraded_threshold, Duration::from_millis(500));
    }

    #[test]
    fn test_health_thresholds_degraded_at_least_healthy() {
        // Degraded threshold should be at least healthy threshold
        let thresholds = HealthThresholds::new(200, 100);
        assert_eq!(thresholds.healthy_threshold, Duration::from_millis(200));
        assert_eq!(thresholds.degraded_threshold, Duration::from_millis(200));
    }
}

mod ping_tests {
    use super::*;

    #[test]
    fn test_ping_error_display() {
        let err = PingError::ConnectionClosed;
        assert_eq!(err.to_string(), "Connection is closed");

        let err = PingError::QueryFailed("test error".to_string());
        assert_eq!(err.to_string(), "Ping query failed: test error");

        let err = PingError::Timeout;
        assert_eq!(err.to_string(), "Ping timed out");
    }

    #[test]
    fn test_get_ping_query() {
        use super::ping::get_ping_query;

        assert_eq!(get_ping_query("mysql"), "SELECT 1");
        assert_eq!(get_ping_query("postgresql"), "SELECT 1");
        assert_eq!(get_ping_query("postgres"), "SELECT 1");
        assert_eq!(get_ping_query("sqlite"), "SELECT 1");
        assert_eq!(get_ping_query("mssql"), "SELECT 1");
        assert_eq!(get_ping_query("unknown"), "SELECT 1");
    }
}

mod checker_tests {
    use super::*;
    use checker::{HealthCheckConfig, HealthCheckResult, HealthChecker, create_shared_checker};

    #[test]
    fn test_health_check_config_default() {
        let config = HealthCheckConfig::default();
        assert_eq!(config.check_interval, Duration::from_secs(30));
        assert_eq!(config.ping_timeout, Duration::from_secs(5));
        assert_eq!(config.failure_threshold, 3);
    }

    #[test]
    fn test_health_check_config_new() {
        let config = HealthCheckConfig::new(Duration::from_secs(60));
        assert_eq!(config.check_interval, Duration::from_secs(60));
    }

    #[test]
    fn test_health_check_config_builder() {
        let config = HealthCheckConfig::new(Duration::from_secs(10))
            .with_ping_timeout(Duration::from_secs(2))
            .with_failure_threshold(5)
            .with_thresholds(HealthThresholds::new(50, 200));

        assert_eq!(config.check_interval, Duration::from_secs(10));
        assert_eq!(config.ping_timeout, Duration::from_secs(2));
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(
            config.thresholds.healthy_threshold,
            Duration::from_millis(50)
        );
        assert_eq!(
            config.thresholds.degraded_threshold,
            Duration::from_millis(200)
        );
    }

    #[test]
    fn test_health_checker_default() {
        let checker = HealthChecker::with_defaults();
        assert_eq!(checker.consecutive_failures(), 0);
        assert_eq!(checker.last_status(), HealthStatus::Healthy);
        assert!(!checker.is_running());
    }

    #[test]
    fn test_health_checker_start_stop() {
        let checker = HealthChecker::with_defaults();
        assert!(!checker.is_running());

        checker.start();
        assert!(checker.is_running());

        checker.stop();
        assert!(!checker.is_running());
    }

    #[test]
    fn test_health_checker_check_interval() {
        let config = HealthCheckConfig::new(Duration::from_secs(45));
        let checker = HealthChecker::new(config);
        assert_eq!(checker.check_interval(), Duration::from_secs(45));
    }

    #[test]
    fn test_health_checker_reset_failures() {
        let checker = HealthChecker::with_defaults();

        // Manually simulate some failures by using internal methods
        // We can't directly set consecutive_failures, but reset should work
        checker.reset_failures();
        assert_eq!(checker.consecutive_failures(), 0);
        assert_eq!(checker.last_status(), HealthStatus::Healthy);
    }

    #[test]
    fn test_health_checker_should_mark_unhealthy() {
        let config = HealthCheckConfig::new(Duration::from_secs(30)).with_failure_threshold(3);
        let checker = HealthChecker::new(config);

        // Initially should not mark unhealthy
        assert!(!checker.should_mark_unhealthy());
    }

    #[test]
    fn test_health_check_result_success() {
        let thresholds = HealthThresholds::default();
        let result = HealthCheckResult::success(Duration::from_millis(50), &thresholds);

        assert_eq!(result.status, HealthStatus::Healthy);
        assert_eq!(result.latency, Some(Duration::from_millis(50)));
        assert!(result.error.is_none());
        assert_eq!(result.consecutive_failures, 0);
    }

    #[test]
    fn test_health_check_result_success_degraded() {
        let thresholds = HealthThresholds::default();
        let result = HealthCheckResult::success(Duration::from_millis(200), &thresholds);

        assert_eq!(result.status, HealthStatus::Degraded);
        assert_eq!(result.latency, Some(Duration::from_millis(200)));
    }

    #[test]
    fn test_health_check_result_success_unhealthy_latency() {
        let thresholds = HealthThresholds::default();
        let result = HealthCheckResult::success(Duration::from_millis(1000), &thresholds);

        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert_eq!(result.latency, Some(Duration::from_millis(1000)));
    }

    #[test]
    fn test_health_check_result_failure() {
        let result = HealthCheckResult::failure("Connection refused".to_string(), 3);

        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.latency.is_none());
        assert_eq!(result.error, Some("Connection refused".to_string()));
        assert_eq!(result.consecutive_failures, 3);
    }

    #[test]
    fn test_create_shared_checker() {
        let config = HealthCheckConfig::new(Duration::from_secs(15));
        let checker = create_shared_checker(config);

        assert_eq!(checker.check_interval(), Duration::from_secs(15));
        assert_eq!(checker.consecutive_failures(), 0);
    }

    #[test]
    fn test_health_checker_config_accessor() {
        let config = HealthCheckConfig::new(Duration::from_secs(20)).with_failure_threshold(5);
        let checker = HealthChecker::new(config);

        assert_eq!(checker.config().check_interval, Duration::from_secs(20));
        assert_eq!(checker.config().failure_threshold, 5);
    }
}
