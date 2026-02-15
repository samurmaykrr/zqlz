//! Security-related configuration types for database connections
//!
//! This module provides configuration types for secure database connections,
//! including SSH tunnels and TLS/SSL settings.

mod ssh_config;
mod tls_config;

pub use ssh_config::*;
pub use tls_config::*;
