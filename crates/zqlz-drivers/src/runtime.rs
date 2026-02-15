//! Tokio runtime for database drivers that require it
//!
//! Some database drivers (like Redis) require a Tokio runtime to be available
//! for their async operations. GPUI uses its own async runtime, so we provide
//! a shared Tokio runtime that drivers can use.

use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// Global Tokio runtime for database drivers
static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Get or create the shared Tokio runtime for database drivers.
///
/// This runtime is used by drivers like Redis that internally require
/// Tokio's reactor for DNS resolution, networking, etc.
///
/// # Panics
///
/// Panics if the runtime cannot be created.
pub fn get_tokio_runtime() -> &'static Runtime {
    TOKIO_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("zqlz-driver-runtime")
            .build()
            .expect("Failed to create Tokio runtime for database drivers")
    })
}

/// Run a blocking operation on the shared Tokio runtime.
///
/// This blocks the current thread until the future completes.
/// Use this sparingly, only when you need to run Tokio-dependent code
/// from a synchronous context.
///
/// # Example
///
/// ```ignore
/// let result = block_on_tokio(async {
///     redis_client.get_multiplexed_async_connection().await
/// });
/// ```
pub fn block_on_tokio<F, T>(future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    get_tokio_runtime().block_on(future)
}
