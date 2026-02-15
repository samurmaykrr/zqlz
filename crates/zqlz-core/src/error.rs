//! Error types for ZQLZ

use thiserror::Error;

/// Core error type for ZQLZ operations
#[derive(Error, Debug)]
pub enum ZqlzError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Driver error: {0}")]
    Driver(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Security error: {0}")]
    Security(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Not supported: {0}")]
    NotSupported(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Cancelled")]
    Cancelled,

    #[error("{0}")]
    Other(String),
}

/// Result type alias for ZQLZ operations
pub type Result<T> = std::result::Result<T, ZqlzError>;
