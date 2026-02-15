//! Savepoint support for database transactions
//!
//! This module provides the `SavepointSupport` trait for transactions that
//! support savepoints - named points within a transaction that can be rolled
//! back to without aborting the entire transaction.

use crate::Result;
use async_trait::async_trait;

/// A savepoint within a transaction.
///
/// Savepoints allow you to create nested rollback points within a transaction.
/// You can rollback to a savepoint without affecting changes made before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Savepoint {
    /// The name of the savepoint
    name: String,
}

impl Savepoint {
    /// Create a new savepoint with the given name.
    ///
    /// Savepoint names should follow database-specific naming rules.
    /// Most databases allow alphanumeric names with underscores.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Get the name of the savepoint.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Trait for transactions that support savepoints.
///
/// Savepoints are named markers within a transaction that allow partial rollback.
/// This is useful for:
/// - Error recovery: Rollback to a known good state without aborting everything
/// - Nested operations: Create logical sub-transactions
/// - Batch processing: Checkpoint progress within large operations
///
/// # Example
/// ```ignore
/// // Start a transaction
/// let tx = conn.begin_transaction().await?;
///
/// // Do some work
/// tx.execute("INSERT INTO users VALUES (1, 'Alice')", &[]).await?;
///
/// // Create a savepoint
/// tx.savepoint("before_bob").await?;
///
/// // Try more work
/// tx.execute("INSERT INTO users VALUES (2, 'Bob')", &[]).await?;
///
/// // Oops, rollback just Bob's insert
/// tx.rollback_to_savepoint("before_bob").await?;
///
/// // Commit Alice's insert
/// tx.commit().await?;
/// ```
#[async_trait]
pub trait SavepointSupport: Send + Sync {
    /// Create a savepoint with the given name.
    ///
    /// # Arguments
    /// * `name` - The name for the savepoint. Should be unique within the transaction.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The savepoint name is invalid
    /// - The database doesn't support savepoints
    /// - A database error occurs
    async fn savepoint(&self, name: &str) -> Result<Savepoint>;

    /// Rollback to a previously created savepoint.
    ///
    /// All changes made after the savepoint was created will be undone.
    /// The savepoint itself remains valid and can be rolled back to again.
    ///
    /// # Arguments
    /// * `name` - The name of the savepoint to rollback to.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The savepoint doesn't exist
    /// - The savepoint has been released
    /// - A database error occurs
    async fn rollback_to_savepoint(&self, name: &str) -> Result<()>;

    /// Release (delete) a savepoint.
    ///
    /// After releasing, the savepoint can no longer be rolled back to.
    /// This is optional - savepoints are automatically released when the
    /// transaction commits or rolls back.
    ///
    /// # Arguments
    /// * `name` - The name of the savepoint to release.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The savepoint doesn't exist
    /// - A database error occurs
    async fn release_savepoint(&self, name: &str) -> Result<()>;

    /// Check if the transaction supports savepoints.
    ///
    /// Some database connections or transaction modes may not support savepoints.
    fn supports_savepoints(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_savepoint_new() {
        let sp = Savepoint::new("test_savepoint");
        assert_eq!(sp.name(), "test_savepoint");
    }

    #[test]
    fn test_savepoint_from_string() {
        let sp = Savepoint::new(String::from("dynamic_name"));
        assert_eq!(sp.name(), "dynamic_name");
    }

    #[test]
    fn test_savepoint_equality() {
        let sp1 = Savepoint::new("same");
        let sp2 = Savepoint::new("same");
        let sp3 = Savepoint::new("different");

        assert_eq!(sp1, sp2);
        assert_ne!(sp1, sp3);
    }

    #[test]
    fn test_savepoint_clone() {
        let sp1 = Savepoint::new("original");
        let sp2 = sp1.clone();

        assert_eq!(sp1.name(), sp2.name());
    }

    #[test]
    fn test_savepoint_debug() {
        let sp = Savepoint::new("debug_test");
        let debug = format!("{:?}", sp);

        assert!(debug.contains("Savepoint"));
        assert!(debug.contains("debug_test"));
    }
}
