//! Query service errors

use thiserror::Error;

pub type QueryServiceResult<T> = Result<T, QueryServiceError>;

/// Query service errors with user-friendly messages
#[derive(Debug, Error)]
pub enum QueryServiceError {
    #[error("Query execution failed: {0}")]
    QueryFailed(String),

    #[error("Statement execution failed: {0}")]
    StatementFailed(String),
}
