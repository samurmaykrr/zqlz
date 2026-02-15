//! Reconnecting connection wrapper with automatic retry
//!
//! This module provides a wrapper that automatically reconnects and retries
//! failed database operations, with configurable retry limits and backoff.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex;
use zqlz_core::{
    Connection, QueryCancelHandle, QueryResult, Result, SchemaIntrospection, StatementResult,
    Transaction, Value, ZqlzError,
};

use super::BackoffStrategy;

/// Configuration for automatic reconnection behavior
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Maximum number of reconnection attempts (0 = no retry, just reconnect once)
    max_attempts: u32,
    /// Backoff strategy for delays between retries
    backoff: BackoffStrategy,
    /// Whether to retry on query errors (not just connection errors)
    retry_on_query_error: bool,
}

impl ReconnectConfig {
    /// Create a new reconnect configuration
    ///
    /// # Arguments
    ///
    /// * `max_attempts` - Maximum retry attempts (0 = fail immediately, 1 = try once then retry once, etc.)
    /// * `backoff` - Backoff strategy for calculating delays
    pub fn new(max_attempts: u32, backoff: BackoffStrategy) -> Self {
        Self {
            max_attempts,
            backoff,
            retry_on_query_error: false,
        }
    }

    /// Set whether to retry on query errors (default: false)
    ///
    /// When true, non-connection errors will also trigger retries.
    /// Use with caution as this may cause duplicate operations.
    pub fn with_retry_on_query_error(mut self, retry: bool) -> Self {
        self.retry_on_query_error = retry;
        self
    }

    /// Get the maximum number of retry attempts
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Get the backoff strategy
    pub fn backoff(&self) -> &BackoffStrategy {
        &self.backoff
    }

    /// Check if query errors should trigger retries
    pub fn retry_on_query_error(&self) -> bool {
        self.retry_on_query_error
    }
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self::new(3, BackoffStrategy::default())
    }
}

/// Factory trait for creating connections
///
/// Used by ReconnectingConnection to recreate connections after failures.
#[async_trait]
pub trait ConnectionFactory: Send + Sync + 'static {
    /// Create a new connection
    async fn create(&self) -> Result<Arc<dyn Connection>>;
}

#[async_trait]
impl<T: ConnectionFactory> ConnectionFactory for Arc<T> {
    async fn create(&self) -> Result<Arc<dyn Connection>> {
        (**self).create().await
    }
}

/// A connection wrapper that automatically reconnects on failure
///
/// This wrapper intercepts connection errors and automatically attempts
/// to reconnect using the provided factory, with configurable retry
/// limits and exponential backoff.
///
/// # Example
///
/// ```ignore
/// use zqlz_connection::reconnect::{ReconnectingConnection, ReconnectConfig, BackoffStrategy};
///
/// let factory = MyConnectionFactory::new(config);
/// let config = ReconnectConfig::new(3, BackoffStrategy::default());
/// let conn = ReconnectingConnection::new(factory, config).await?;
///
/// // Operations will automatically retry on connection failures
/// let result = conn.query("SELECT 1", &[]).await?;
/// ```
pub struct ReconnectingConnection {
    /// The underlying connection (behind mutex for replacement)
    connection: Mutex<Option<Arc<dyn Connection>>>,
    /// Factory for creating new connections
    factory: Arc<dyn ConnectionFactory>,
    /// Reconnection configuration
    config: ReconnectConfig,
    /// Number of consecutive failures
    consecutive_failures: AtomicU32,
    /// Whether the connection is permanently closed
    permanently_closed: AtomicBool,
    /// Driver name (cached for when connection is unavailable)
    driver_name: String,
}

impl ReconnectingConnection {
    /// Create a new reconnecting connection
    ///
    /// This will immediately create a connection using the factory.
    pub async fn new<F: ConnectionFactory>(
        factory: F,
        config: ReconnectConfig,
    ) -> Result<Arc<Self>> {
        let factory = Arc::new(factory);
        let connection = factory.create().await?;
        let driver_name = connection.driver_name().to_string();

        Ok(Arc::new(Self {
            connection: Mutex::new(Some(connection)),
            factory,
            config,
            consecutive_failures: AtomicU32::new(0),
            permanently_closed: AtomicBool::new(false),
            driver_name,
        }))
    }

    /// Create a reconnecting connection with an existing connection
    ///
    /// Useful for wrapping an already-established connection.
    pub fn wrap<F: ConnectionFactory>(
        connection: Arc<dyn Connection>,
        factory: F,
        config: ReconnectConfig,
    ) -> Arc<Self> {
        let driver_name = connection.driver_name().to_string();
        Arc::new(Self {
            connection: Mutex::new(Some(connection)),
            factory: Arc::new(factory),
            config,
            consecutive_failures: AtomicU32::new(0),
            permanently_closed: AtomicBool::new(false),
            driver_name,
        })
    }

    /// Get the number of consecutive failures
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::SeqCst)
    }

    /// Check if the error should trigger a reconnection attempt
    fn should_reconnect(&self, error: &ZqlzError) -> bool {
        match error {
            ZqlzError::Connection(_) => true,
            ZqlzError::Io(_) => true,
            ZqlzError::Query(_) => self.config.retry_on_query_error,
            ZqlzError::Timeout(_) => true,
            _ => false,
        }
    }

    /// Attempt to reconnect
    async fn reconnect(&self) -> Result<()> {
        let mut guard = self.connection.lock().await;

        // Close existing connection if any
        if let Some(conn) = guard.take() {
            let _ = conn.close().await;
        }

        // Try to create a new connection
        let new_conn = self.factory.create().await?;
        *guard = Some(new_conn);

        self.consecutive_failures.store(0, Ordering::SeqCst);
        Ok(())
    }

    /// Execute an operation with automatic retry
    async fn execute_with_retry<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: Fn(Arc<dyn Connection>) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T>> + Send,
    {
        if self.permanently_closed.load(Ordering::SeqCst) {
            return Err(ZqlzError::Connection(
                "Connection permanently closed".into(),
            ));
        }

        let mut attempt = 0u32;

        loop {
            // Get current connection
            let conn = {
                let guard = self.connection.lock().await;
                match guard.as_ref() {
                    Some(c) => c.clone(),
                    None => {
                        drop(guard);
                        // No connection available, try to reconnect
                        if attempt > self.config.max_attempts {
                            return Err(ZqlzError::Connection(
                                "Max reconnection attempts exceeded".into(),
                            ));
                        }
                        self.reconnect().await?;
                        // Get the newly created connection
                        let guard = self.connection.lock().await;
                        guard
                            .as_ref()
                            .ok_or_else(|| {
                                ZqlzError::Connection("Failed to establish connection".into())
                            })?
                            .clone()
                    }
                }
            };

            // Try the operation
            let result = operation(conn).await;

            match result {
                Ok(value) => {
                    self.consecutive_failures.store(0, Ordering::SeqCst);
                    return Ok(value);
                }
                Err(err) => {
                    self.consecutive_failures.fetch_add(1, Ordering::SeqCst);

                    if !self.should_reconnect(&err) {
                        return Err(err);
                    }

                    if attempt >= self.config.max_attempts {
                        return Err(err);
                    }

                    // Calculate delay and wait
                    let delay = self.config.backoff.calculate_delay(attempt);
                    tokio::time::sleep(delay).await;

                    // Try to reconnect
                    if let Err(reconnect_err) = self.reconnect().await {
                        // If reconnection fails on last attempt, return the error
                        if attempt + 1 >= self.config.max_attempts {
                            return Err(reconnect_err);
                        }
                    }

                    attempt += 1;
                }
            }
        }
    }
}

#[async_trait]
impl Connection for ReconnectingConnection {
    fn driver_name(&self) -> &str {
        &self.driver_name
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        let sql = sql.to_string();
        let params = params.to_vec();
        self.execute_with_retry(move |conn| {
            let sql = sql.clone();
            let params = params.clone();
            async move { conn.execute(&sql, &params).await }
        })
        .await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let sql = sql.to_string();
        let params = params.to_vec();
        self.execute_with_retry(move |conn| {
            let sql = sql.clone();
            let params = params.clone();
            async move { conn.query(&sql, &params).await }
        })
        .await
    }

    fn dialect_id(&self) -> Option<&'static str> {
        // We can't reliably get this without a connection, so return None
        None
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        // Transactions don't support automatic reconnection mid-transaction
        // We'll try once, but not retry
        let guard = self.connection.lock().await;
        match guard.as_ref() {
            Some(conn) => conn.begin_transaction().await,
            None => Err(ZqlzError::Connection("No connection available".into())),
        }
    }

    async fn close(&self) -> Result<()> {
        self.permanently_closed.store(true, Ordering::SeqCst);
        let mut guard = self.connection.lock().await;
        if let Some(conn) = guard.take() {
            conn.close().await
        } else {
            Ok(())
        }
    }

    fn is_closed(&self) -> bool {
        if self.permanently_closed.load(Ordering::SeqCst) {
            return true;
        }
        // We can't safely check without blocking, so assume open if not permanently closed
        false
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        // Schema introspection requires a stable reference, which we can't provide
        // through the reconnecting wrapper
        None
    }

    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        // Cancel handles are connection-specific and may become invalid after reconnect
        None
    }
}

/// Reconnection event for monitoring
#[derive(Debug, Clone)]
pub enum ReconnectEvent {
    /// A reconnection attempt is starting
    Attempting { attempt: u32, max_attempts: u32 },
    /// A reconnection attempt succeeded
    Succeeded { attempts_taken: u32 },
    /// A reconnection attempt failed
    Failed { attempt: u32, error: String },
    /// All reconnection attempts exhausted
    Exhausted { total_attempts: u32 },
}
