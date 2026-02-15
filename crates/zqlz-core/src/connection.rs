//! Connection trait and transaction handling

use crate::{QueryResult, Result, SchemaIntrospection, StatementResult, Value};
use async_trait::async_trait;
use std::sync::Arc;

/// Handle for cancelling a running query from any thread.
///
/// This trait allows database drivers to provide a way to interrupt
/// long-running queries. The handle is safe to call from any thread
/// and can be called multiple times (subsequent calls are no-ops).
pub trait QueryCancelHandle: Send + Sync {
    /// Cancel the currently running query on the associated connection.
    ///
    /// This method is safe to call from any thread and is idempotent.
    /// If no query is running, this is a no-op.
    fn cancel(&self);
}

/// Request to update a single cell value
#[derive(Debug, Clone)]
pub struct CellUpdateRequest {
    /// Table name
    pub table_name: String,
    /// Column name to update
    pub column_name: String,
    /// New value (None for NULL)
    pub new_value: Option<Value>,
    /// Row identifier - can be row index, primary key value, or full row data
    pub row_identifier: RowIdentifier,
}

/// Different ways to identify a row for updating
#[derive(Debug, Clone)]
pub enum RowIdentifier {
    /// Use row index/offset (0-based)
    RowIndex(usize),
    /// Use primary key value(s)
    PrimaryKey(Vec<(String, Value)>),
    /// Use all column values to identify the row uniquely
    FullRow(Vec<(String, Value)>),
}

/// A database connection
#[async_trait]
pub trait Connection: Send + Sync {
    /// Get the driver name (e.g., "sqlite", "postgresql", "mysql")
    fn driver_name(&self) -> &str;

    /// Execute a statement that modifies data (INSERT/UPDATE/DELETE)
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult>;

    /// Execute a query that returns rows (SELECT)
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult>;

    /// Get the dialect identifier for this connection (e.g., "sqlite", "postgresql")
    ///
    /// This is used by the query service to look up dialect-specific behavior
    /// like EXPLAIN syntax. Returns None if the dialect is unknown.
    fn dialect_id(&self) -> Option<&'static str> {
        None
    }

    /// Update a single cell value
    ///
    /// This is a high-level method that each database driver implements according to
    /// its specific requirements. For example:
    /// - SQLite might use ROWID
    /// - PostgreSQL might use ctid or primary keys
    /// - MySQL might use primary keys
    ///
    /// Returns the number of rows affected.
    async fn update_cell(&self, request: CellUpdateRequest) -> Result<u64> {
        tracing::debug!(
            table = %request.table_name,
            column = %request.column_name,
            "updating cell value"
        );
        // Default implementation uses the row identifier to build a WHERE clause
        // Drivers can override this for database-specific optimizations
        let (where_clause, mut params) = match &request.row_identifier {
            RowIdentifier::RowIndex(_) => {
                // This is database-specific and may not work for all databases
                return Err(crate::ZqlzError::NotSupported(
                    "Row index-based updates not supported by this driver. Use primary key or full row identifier.".to_string()
                ));
            }
            RowIdentifier::PrimaryKey(pk_values) => {
                let conditions: Vec<String> = pk_values
                    .iter()
                    .map(|(col, _)| format!("{} = ?", col))
                    .collect();
                let params: Vec<Value> = pk_values.iter().map(|(_, v)| v.clone()).collect();
                (conditions.join(" AND "), params)
            }
            RowIdentifier::FullRow(row_values) => {
                let conditions: Vec<String> = row_values
                    .iter()
                    .map(|(col, val)| {
                        if val == &Value::Null {
                            format!("{} IS NULL", col)
                        } else {
                            format!("{} = ?", col)
                        }
                    })
                    .collect();
                let params: Vec<Value> = row_values
                    .iter()
                    .filter(|(_, val)| val != &Value::Null)
                    .map(|(_, v)| v.clone())
                    .collect();
                (conditions.join(" AND "), params)
            }
        };

        // Build UPDATE statement
        let sql = if let Some(new_val) = &request.new_value {
            params.insert(0, new_val.clone());
            format!(
                "UPDATE {} SET {} = ? WHERE {}",
                request.table_name, request.column_name, where_clause
            )
        } else {
            format!(
                "UPDATE {} SET {} = NULL WHERE {}",
                request.table_name, request.column_name, where_clause
            )
        };

        let result = self.execute(&sql, &params).await?;
        tracing::debug!(
            affected_rows = result.affected_rows,
            "cell update completed"
        );
        Ok(result.affected_rows)
    }

    /// Begin a transaction
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>>;

    /// Close the connection
    async fn close(&self) -> Result<()>;

    /// Check if the connection is closed
    fn is_closed(&self) -> bool;

    /// Get schema introspection interface if supported
    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        None
    }

    /// Get a handle that can be used to cancel running queries.
    ///
    /// Returns `None` if the driver does not support query cancellation.
    /// The returned handle is safe to use from any thread.
    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        None
    }
}

/// A database transaction
#[async_trait]
pub trait Transaction: Send + Sync {
    /// Commit the transaction
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Rollback the transaction
    async fn rollback(self: Box<Self>) -> Result<()>;

    /// Execute a query within the transaction
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult>;

    /// Execute a statement within the transaction
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult>;
}

/// A prepared statement
#[async_trait]
pub trait PreparedStatement: Send + Sync {
    /// Execute the prepared statement with parameters
    async fn execute(&self, params: &[Value]) -> Result<StatementResult>;

    /// Query the prepared statement with parameters
    async fn query(&self, params: &[Value]) -> Result<QueryResult>;

    /// Close/deallocate the prepared statement
    async fn close(self: Box<Self>) -> Result<()>;
}
