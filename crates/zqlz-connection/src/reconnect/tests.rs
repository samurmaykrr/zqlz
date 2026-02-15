//! Tests for the reconnect module

use super::*;
use std::time::Duration;

mod backoff_tests {
    use super::*;

    #[test]
    fn test_backoff_first_attempt() {
        let backoff = BackoffStrategy::new(100, 30_000);
        let delay = backoff.calculate_delay(0);
        assert_eq!(delay, Duration::from_millis(100));
    }

    #[test]
    fn test_backoff_second_attempt() {
        let backoff = BackoffStrategy::new(100, 30_000);
        let delay = backoff.calculate_delay(1);
        assert_eq!(delay, Duration::from_millis(200));
    }

    #[test]
    fn test_backoff_exponential_growth() {
        let backoff = BackoffStrategy::new(100, 30_000);

        // Attempt 0: 100ms
        assert_eq!(backoff.calculate_delay(0), Duration::from_millis(100));

        // Attempt 1: 200ms
        assert_eq!(backoff.calculate_delay(1), Duration::from_millis(200));

        // Attempt 2: 400ms
        assert_eq!(backoff.calculate_delay(2), Duration::from_millis(400));

        // Attempt 3: 800ms
        assert_eq!(backoff.calculate_delay(3), Duration::from_millis(800));

        // Attempt 4: 1600ms
        assert_eq!(backoff.calculate_delay(4), Duration::from_millis(1600));
    }

    #[test]
    fn test_backoff_max_limit() {
        let backoff = BackoffStrategy::new(100, 1000);

        // Should be capped at 1000ms
        let delay = backoff.calculate_delay(10);
        assert_eq!(delay, Duration::from_millis(1000));

        // Even higher attempts stay capped
        let delay = backoff.calculate_delay(20);
        assert_eq!(delay, Duration::from_millis(1000));
    }

    #[test]
    fn test_backoff_custom_multiplier() {
        let backoff = BackoffStrategy::new(100, 30_000).with_multiplier(3.0);

        // Attempt 0: 100ms
        assert_eq!(backoff.calculate_delay(0), Duration::from_millis(100));

        // Attempt 1: 300ms (100 * 3)
        assert_eq!(backoff.calculate_delay(1), Duration::from_millis(300));

        // Attempt 2: 900ms (100 * 3^2)
        assert_eq!(backoff.calculate_delay(2), Duration::from_millis(900));
    }

    #[test]
    fn test_backoff_with_jitter() {
        let backoff = BackoffStrategy::new(1000, 30_000).with_jitter(true);

        // With jitter, delays should vary but stay within bounds
        // Jitter is ±25%, so for 1000ms base, range is 750-1250ms
        let delay = backoff.calculate_delay(0);
        assert!(
            delay >= Duration::from_millis(750) && delay <= Duration::from_millis(1250),
            "Delay {:?} should be between 750ms and 1250ms",
            delay
        );
    }

    #[test]
    fn test_backoff_minimum_initial() {
        // Initial delay should be at least 1ms
        let backoff = BackoffStrategy::new(0, 1000);
        assert_eq!(backoff.initial_delay(), Duration::from_millis(1));
    }

    #[test]
    fn test_backoff_max_at_least_initial() {
        // Max should be at least initial
        let backoff = BackoffStrategy::new(1000, 100);
        assert_eq!(backoff.max_delay(), Duration::from_millis(1000));
    }

    #[test]
    fn test_backoff_multiplier_minimum() {
        // Multiplier should be at least 1.0
        let backoff = BackoffStrategy::new(100, 1000).with_multiplier(0.5);
        assert_eq!(backoff.multiplier(), 1.0);
    }

    #[test]
    fn test_backoff_accessors() {
        let backoff = BackoffStrategy::new(100, 30_000)
            .with_multiplier(2.5)
            .with_jitter(true);

        assert_eq!(backoff.initial_delay(), Duration::from_millis(100));
        assert_eq!(backoff.max_delay(), Duration::from_millis(30_000));
        assert_eq!(backoff.multiplier(), 2.5);
        assert!(backoff.has_jitter());
    }

    #[test]
    fn test_backoff_default() {
        let backoff = BackoffStrategy::default();

        assert_eq!(backoff.initial_delay(), Duration::from_millis(100));
        assert_eq!(backoff.max_delay(), Duration::from_millis(30_000));
        assert_eq!(backoff.multiplier(), 2.0);
        assert!(!backoff.has_jitter());
    }

    #[test]
    fn test_backoff_reset_is_noop() {
        let backoff = BackoffStrategy::new(100, 1000);
        backoff.reset(); // Should not panic
        assert_eq!(backoff.calculate_delay(0), Duration::from_millis(100));
    }

    #[test]
    fn test_backoff_clone() {
        let backoff = BackoffStrategy::new(100, 1000).with_multiplier(3.0);
        let cloned = backoff.clone();

        assert_eq!(cloned.initial_delay(), backoff.initial_delay());
        assert_eq!(cloned.max_delay(), backoff.max_delay());
        assert_eq!(cloned.multiplier(), backoff.multiplier());
    }

    #[test]
    fn test_backoff_debug() {
        let backoff = BackoffStrategy::new(100, 1000);
        let debug = format!("{:?}", backoff);
        assert!(debug.contains("BackoffStrategy"));
        assert!(debug.contains("100"));
        assert!(debug.contains("1000"));
    }
}

mod reconnect_config_tests {
    use super::*;

    #[test]
    fn test_reconnect_config_new() {
        let backoff = BackoffStrategy::new(100, 5000);
        let config = ReconnectConfig::new(5, backoff.clone());

        assert_eq!(config.max_attempts(), 5);
        assert_eq!(config.backoff().initial_delay(), backoff.initial_delay());
        assert!(!config.retry_on_query_error());
    }

    #[test]
    fn test_reconnect_config_default() {
        let config = ReconnectConfig::default();

        assert_eq!(config.max_attempts(), 3);
        assert_eq!(config.backoff().initial_delay(), Duration::from_millis(100));
        assert!(!config.retry_on_query_error());
    }

    #[test]
    fn test_reconnect_config_with_retry_on_query_error() {
        let config = ReconnectConfig::default().with_retry_on_query_error(true);
        assert!(config.retry_on_query_error());

        let config = config.with_retry_on_query_error(false);
        assert!(!config.retry_on_query_error());
    }

    #[test]
    fn test_reconnect_config_clone() {
        let config = ReconnectConfig::default().with_retry_on_query_error(true);
        let cloned = config.clone();

        assert_eq!(cloned.max_attempts(), config.max_attempts());
        assert_eq!(
            cloned.backoff().initial_delay(),
            config.backoff().initial_delay()
        );
        assert_eq!(cloned.retry_on_query_error(), config.retry_on_query_error());
    }

    #[test]
    fn test_reconnect_config_debug() {
        let config = ReconnectConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("ReconnectConfig"));
        assert!(debug.contains("max_attempts"));
    }

    #[test]
    fn test_reconnect_config_zero_attempts() {
        let config = ReconnectConfig::new(0, BackoffStrategy::default());
        assert_eq!(config.max_attempts(), 0);
    }

    #[test]
    fn test_reconnect_config_high_attempts() {
        let config = ReconnectConfig::new(100, BackoffStrategy::default());
        assert_eq!(config.max_attempts(), 100);
    }
}

mod reconnect_event_tests {
    use super::*;

    #[test]
    fn test_reconnect_event_attempting() {
        let event = ReconnectEvent::Attempting {
            attempt: 1,
            max_attempts: 3,
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Attempting"));
        assert!(debug.contains("1"));
        assert!(debug.contains("3"));
    }

    #[test]
    fn test_reconnect_event_succeeded() {
        let event = ReconnectEvent::Succeeded { attempts_taken: 2 };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Succeeded"));
        assert!(debug.contains("2"));
    }

    #[test]
    fn test_reconnect_event_failed() {
        let event = ReconnectEvent::Failed {
            attempt: 1,
            error: "Connection refused".into(),
        };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Failed"));
        assert!(debug.contains("Connection refused"));
    }

    #[test]
    fn test_reconnect_event_exhausted() {
        let event = ReconnectEvent::Exhausted { total_attempts: 5 };
        let debug = format!("{:?}", event);
        assert!(debug.contains("Exhausted"));
        assert!(debug.contains("5"));
    }

    #[test]
    fn test_reconnect_event_clone() {
        let event = ReconnectEvent::Succeeded { attempts_taken: 3 };
        let cloned = event.clone();

        if let ReconnectEvent::Succeeded { attempts_taken } = cloned {
            assert_eq!(attempts_taken, 3);
        } else {
            panic!("Clone changed event type");
        }
    }

    mod wrapper_tests {
        use super::*;
        use async_trait::async_trait;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};
        use zqlz_core::{
            Connection, QueryResult, Result, StatementResult, Transaction, Value, ZqlzError,
        };

        /// Shared failure counter that can be shared across connections
        struct SharedFailureCounter {
            fail_count: AtomicU32,
        }

        impl SharedFailureCounter {
            fn new(failures: u32) -> Arc<Self> {
                Arc::new(Self {
                    fail_count: AtomicU32::new(failures),
                })
            }

            fn should_fail(&self) -> bool {
                let remaining = self.fail_count.load(Ordering::SeqCst);
                if remaining > 0 {
                    self.fail_count.fetch_sub(1, Ordering::SeqCst);
                    true
                } else {
                    false
                }
            }
        }

        /// Mock connection that can be configured to fail
        struct MockConnection {
            driver_name: String,
            failure_counter: Option<Arc<SharedFailureCounter>>,
            query_count: AtomicU32,
            closed: std::sync::atomic::AtomicBool,
        }

        impl MockConnection {
            fn new(driver_name: &str) -> Self {
                Self {
                    driver_name: driver_name.to_string(),
                    failure_counter: None,
                    query_count: AtomicU32::new(0),
                    closed: std::sync::atomic::AtomicBool::new(false),
                }
            }

            fn with_shared_failures(driver_name: &str, counter: Arc<SharedFailureCounter>) -> Self {
                Self {
                    driver_name: driver_name.to_string(),
                    failure_counter: Some(counter),
                    query_count: AtomicU32::new(0),
                    closed: std::sync::atomic::AtomicBool::new(false),
                }
            }
        }

        #[async_trait]
        impl Connection for MockConnection {
            fn driver_name(&self) -> &str {
                &self.driver_name
            }

            async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<StatementResult> {
                self.query_count.fetch_add(1, Ordering::SeqCst);
                if let Some(counter) = &self.failure_counter {
                    if counter.should_fail() {
                        return Err(ZqlzError::Connection("Mock connection error".into()));
                    }
                }
                Ok(StatementResult {
                    is_query: false,
                    result: None,
                    affected_rows: 1,
                    error: None,
                })
            }

            async fn query(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult> {
                self.query_count.fetch_add(1, Ordering::SeqCst);
                if let Some(counter) = &self.failure_counter {
                    if counter.should_fail() {
                        return Err(ZqlzError::Connection("Mock connection error".into()));
                    }
                }
                Ok(QueryResult::empty())
            }

            async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
                Err(ZqlzError::NotImplemented(
                    "Transactions not implemented in mock".into(),
                ))
            }

            async fn close(&self) -> Result<()> {
                self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }

            fn is_closed(&self) -> bool {
                self.closed.load(std::sync::atomic::Ordering::SeqCst)
            }
        }

        /// Mock factory that creates MockConnections
        struct MockFactory {
            driver_name: String,
            create_count: AtomicU32,
            fail_creates: AtomicU32,
            connection_failure_counter: Option<Arc<SharedFailureCounter>>,
        }

        impl MockFactory {
            fn new(driver_name: &str) -> Self {
                Self {
                    driver_name: driver_name.to_string(),
                    create_count: AtomicU32::new(0),
                    fail_creates: AtomicU32::new(0),
                    connection_failure_counter: None,
                }
            }

            fn with_connection_failures(driver_name: &str, failures: u32) -> Self {
                Self {
                    driver_name: driver_name.to_string(),
                    create_count: AtomicU32::new(0),
                    fail_creates: AtomicU32::new(0),
                    connection_failure_counter: Some(SharedFailureCounter::new(failures)),
                }
            }

            fn with_create_failures(driver_name: &str, failures: u32) -> Self {
                Self {
                    driver_name: driver_name.to_string(),
                    create_count: AtomicU32::new(0),
                    fail_creates: AtomicU32::new(failures),
                    connection_failure_counter: None,
                }
            }
        }

        #[async_trait]
        impl ConnectionFactory for MockFactory {
            async fn create(&self) -> Result<Arc<dyn Connection>> {
                self.create_count.fetch_add(1, Ordering::SeqCst);
                let remaining = self.fail_creates.load(Ordering::SeqCst);
                if remaining > 0 {
                    self.fail_creates.fetch_sub(1, Ordering::SeqCst);
                    Err(ZqlzError::Connection("Mock factory error".into()))
                } else if let Some(counter) = &self.connection_failure_counter {
                    Ok(Arc::new(MockConnection::with_shared_failures(
                        &self.driver_name,
                        counter.clone(),
                    )))
                } else {
                    Ok(Arc::new(MockConnection::new(&self.driver_name)))
                }
            }
        }

        #[tokio::test]
        async fn test_reconnecting_connection_new() {
            let factory = MockFactory::new("mock");
            let config = ReconnectConfig::default();

            let conn = ReconnectingConnection::new(factory, config).await;
            assert!(conn.is_ok());

            let conn = conn.unwrap();
            assert_eq!(conn.driver_name(), "mock");
            assert!(!conn.is_closed());
        }

        #[tokio::test]
        async fn test_reconnecting_connection_query_success() {
            let factory = MockFactory::new("mock");
            let config = ReconnectConfig::default();
            let conn = ReconnectingConnection::new(factory, config).await.unwrap();

            let result = conn.query("SELECT 1", &[]).await;
            assert!(result.is_ok());
            assert_eq!(conn.consecutive_failures(), 0);
        }

        #[tokio::test]
        async fn test_reconnecting_connection_execute_success() {
            let factory = MockFactory::new("mock");
            let config = ReconnectConfig::default();
            let conn = ReconnectingConnection::new(factory, config).await.unwrap();

            let result = conn.execute("INSERT INTO test VALUES (1)", &[]).await;
            assert!(result.is_ok());
            assert_eq!(result.unwrap().affected_rows, 1);
        }

        #[tokio::test]
        async fn test_reconnecting_connection_retry_on_failure() {
            // Create a connection that fails twice then succeeds
            let factory = MockFactory::with_connection_failures("mock", 2);
            let backoff = BackoffStrategy::new(1, 10); // Very short delays for testing
            let config = ReconnectConfig::new(5, backoff);

            let conn = ReconnectingConnection::new(factory, config).await.unwrap();

            // First query should succeed after retries
            let result = conn.query("SELECT 1", &[]).await;
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn test_reconnecting_connection_gives_up_after_max_attempts() {
            // Create a factory that always fails (100 failures, but only 2 retries allowed)
            let factory = MockFactory::with_connection_failures("mock", 100);
            let backoff = BackoffStrategy::new(1, 10); // Very short delays for testing
            let config = ReconnectConfig::new(2, backoff); // Only retry twice

            let conn = ReconnectingConnection::new(factory, config).await.unwrap();

            // Query should fail after exhausting retries
            let result = conn.query("SELECT 1", &[]).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_reconnecting_connection_close() {
            let factory = MockFactory::new("mock");
            let config = ReconnectConfig::default();
            let conn = ReconnectingConnection::new(factory, config).await.unwrap();

            let result = conn.close().await;
            assert!(result.is_ok());
            assert!(conn.is_closed());

            // Operations should fail after close
            let result = conn.query("SELECT 1", &[]).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_reconnecting_connection_wrap() {
            let mock_conn = Arc::new(MockConnection::new("wrapped"));
            let factory = MockFactory::new("wrapped");
            let config = ReconnectConfig::default();

            let conn = ReconnectingConnection::wrap(mock_conn, factory, config);
            assert_eq!(conn.driver_name(), "wrapped");
            assert!(!conn.is_closed());
        }

        #[tokio::test]
        async fn test_reconnecting_connection_factory_failure() {
            // Factory that fails to create connections initially
            let factory = MockFactory::with_create_failures("mock", 5);
            let config = ReconnectConfig::default();

            // Should fail because factory can't create initial connection
            let result = ReconnectingConnection::new(factory, config).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_reconnecting_connection_reconnect_on_factory_failure() {
            // Connection works, but fails initially (shared counter)
            let factory = MockFactory::with_connection_failures("mock", 1);
            let backoff = BackoffStrategy::new(1, 10);
            let config = ReconnectConfig::new(5, backoff);

            let conn = ReconnectingConnection::new(factory, config).await.unwrap();

            // First query should eventually succeed after reconnection
            let result = conn.query("SELECT 1", &[]).await;
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn test_reconnecting_connection_consecutive_failures() {
            let factory = MockFactory::with_connection_failures("mock", 2);
            let backoff = BackoffStrategy::new(1, 10);
            let config = ReconnectConfig::new(5, backoff);

            let conn = ReconnectingConnection::new(factory, config).await.unwrap();
            assert_eq!(conn.consecutive_failures(), 0);

            // Query that fails but eventually succeeds
            let _ = conn.query("SELECT 1", &[]).await;
            // After success, consecutive failures should be reset
            assert_eq!(conn.consecutive_failures(), 0);
        }
    }
}
