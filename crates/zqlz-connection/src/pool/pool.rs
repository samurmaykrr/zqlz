//! Connection pool implementation

use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zqlz_core::{Connection, Result, ZqlzError};

use super::config::PoolConfig;
use super::stats::PoolStats;

/// Factory trait for creating new connections
#[async_trait]
pub trait ConnectionFactory: Send + Sync + 'static {
    /// Create a new connection
    async fn create(&self) -> Result<Arc<dyn Connection>>;

    /// Validate that a connection is still usable
    ///
    /// Default implementation always returns true.
    async fn validate(&self, conn: &dyn Connection) -> bool {
        !conn.is_closed()
    }
}

#[async_trait]
impl<T: ConnectionFactory> ConnectionFactory for Arc<T> {
    async fn create(&self) -> Result<Arc<dyn Connection>> {
        (**self).create().await
    }

    async fn validate(&self, conn: &dyn Connection) -> bool {
        (**self).validate(conn).await
    }
}

/// Internal wrapper for pooled connections with metadata
struct PooledConnectionInner {
    connection: Arc<dyn Connection>,
    created_at: Instant,
    last_used_at: Instant,
}

impl PooledConnectionInner {
    fn new(connection: Arc<dyn Connection>) -> Self {
        let now = Instant::now();
        Self {
            connection,
            created_at: now,
            last_used_at: now,
        }
    }

    fn touch(&mut self) {
        self.last_used_at = Instant::now();
    }
}

/// A connection pool that manages a set of database connections
///
/// The pool maintains a configurable number of connections and provides
/// them to callers on demand. Connections are automatically returned
/// to the pool when the `PooledConnection` wrapper is dropped.
pub struct ConnectionPool {
    /// Pool configuration
    config: PoolConfig,
    /// Connection factory
    factory: Arc<dyn ConnectionFactory>,
    /// Available idle connections
    idle: Mutex<VecDeque<PooledConnectionInner>>,
    /// Semaphore to limit total connections
    semaphore: Arc<Semaphore>,
    /// Number of active connections (borrowed from pool)
    active_count: AtomicUsize,
    /// Number of requests waiting for a connection
    waiting_count: AtomicUsize,
}

impl ConnectionPool {
    /// Create a new connection pool with the given configuration and factory
    pub fn new<F: ConnectionFactory>(config: PoolConfig, factory: F) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_size()));
        Self {
            config,
            factory: Arc::new(factory),
            idle: Mutex::new(VecDeque::new()),
            semaphore,
            active_count: AtomicUsize::new(0),
            waiting_count: AtomicUsize::new(0),
        }
    }

    /// Get a connection from the pool
    ///
    /// This will:
    /// 1. Try to get an idle connection from the pool
    /// 2. If none available and under max_size, create a new connection
    /// 3. If at max_size, wait for a connection to be returned (with timeout)
    ///
    /// Returns an error if the acquire timeout is exceeded.
    pub async fn get(&self) -> Result<PooledConnection<'_>> {
        self.waiting_count.fetch_add(1, Ordering::SeqCst);

        let result = tokio::time::timeout(self.config.acquire_timeout(), async {
            // Acquire a permit from the semaphore (limits total connections)
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| ZqlzError::Connection("Pool semaphore closed".into()))?;

            // Try to get an idle connection
            let connection = self.try_get_idle().await;

            let connection = match connection {
                Some(conn) => conn,
                None => {
                    // Create a new connection
                    self.factory.create().await?
                }
            };

            self.active_count.fetch_add(1, Ordering::SeqCst);
            self.waiting_count.fetch_sub(1, Ordering::SeqCst);

            Ok(PooledConnection {
                connection: Some(connection),
                pool: self,
                _permit: permit,
            })
        })
        .await;

        match result {
            Ok(conn) => conn,
            Err(_) => {
                self.waiting_count.fetch_sub(1, Ordering::SeqCst);
                Err(ZqlzError::Timeout(format!(
                    "Timed out waiting for connection (timeout: {:?})",
                    self.config.acquire_timeout()
                )))
            }
        }
    }

    /// Try to get an idle connection, validating and checking lifetime
    async fn try_get_idle(&self) -> Option<Arc<dyn Connection>> {
        loop {
            let pooled = { self.idle.lock().pop_front() };

            match pooled {
                Some(mut inner) => {
                    // Check if connection has exceeded max lifetime
                    if let Some(max_lifetime) = self.config.max_lifetime() {
                        if inner.created_at.elapsed() > max_lifetime {
                            // Connection too old, close it and try again
                            let _ = inner.connection.close().await;
                            continue;
                        }
                    }

                    // Check idle timeout
                    if inner.last_used_at.elapsed() > self.config.idle_timeout() {
                        // Connection idle too long, close it and try again
                        let _ = inner.connection.close().await;
                        continue;
                    }

                    // Validate connection
                    if !self.factory.validate(&*inner.connection).await {
                        // Connection invalid, close it and try again
                        let _ = inner.connection.close().await;
                        continue;
                    }

                    inner.touch();
                    return Some(inner.connection);
                }
                None => return None,
            }
        }
    }

    /// Return a connection to the pool
    fn return_connection(&self, connection: Arc<dyn Connection>) {
        self.active_count.fetch_sub(1, Ordering::SeqCst);

        // Don't return closed connections
        if connection.is_closed() {
            return;
        }

        // Add to idle pool
        let mut idle = self.idle.lock();
        idle.push_back(PooledConnectionInner::new(connection));
    }

    /// Get current pool statistics
    pub fn stats(&self) -> PoolStats {
        let idle = self.idle.lock().len();
        let active = self.active_count.load(Ordering::SeqCst);
        let waiting = self.waiting_count.load(Ordering::SeqCst);
        PoolStats::new(idle + active, idle, active, waiting)
    }

    /// Get the pool configuration
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Close all idle connections in the pool
    pub async fn close_idle(&self) {
        let connections: Vec<_> = {
            let mut idle = self.idle.lock();
            idle.drain(..).collect()
        };

        for inner in connections {
            let _ = inner.connection.close().await;
        }
    }
}

/// A connection borrowed from the pool
///
/// When dropped, the connection is automatically returned to the pool.
pub struct PooledConnection<'a> {
    connection: Option<Arc<dyn Connection>>,
    pool: &'a ConnectionPool,
    _permit: OwnedSemaphorePermit,
}

impl<'a> Deref for PooledConnection<'a> {
    type Target = dyn Connection;

    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().expect("connection taken").as_ref()
    }
}

impl<'a> Drop for PooledConnection<'a> {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            self.pool.return_connection(conn);
        }
    }
}

impl<'a> PooledConnection<'a> {
    /// Get the underlying connection as an Arc
    pub fn inner(&self) -> &Arc<dyn Connection> {
        self.connection.as_ref().expect("connection taken")
    }
}
