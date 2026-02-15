use thiserror::Error;

pub type ServiceResult<T> = Result<T, ServiceError>;

/// Service-level errors with user-friendly messages
#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Disconnection failed: {0}")]
    DisconnectionFailed(String),

    #[error("Connection not found")]
    ConnectionNotFound,

    #[error("Schema introspection not supported for this database")]
    SchemaNotSupported,

    #[error("DDL generation failed: {0}")]
    DdlGenerationFailed(String),

    #[error("Cell update failed: {0}")]
    UpdateFailed(String),

    #[error("Invalid value: {0}")]
    InvalidValue(String),

    #[error("Schema loading failed: {0}")]
    SchemaLoadFailed(String),

    #[error("Table operation failed: {0}")]
    TableOperationFailed(String),
}
