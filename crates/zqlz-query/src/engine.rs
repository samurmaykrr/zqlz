//! Query execution engine

use std::sync::Arc;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use zqlz_core::{Connection, QueryResult, Result, StatementResult};

/// Information about a destructive operation that requires confirmation
#[derive(Clone, Debug)]
pub struct DestructiveOperationWarning {
    /// The type of operation (DELETE, UPDATE, DROP, TRUNCATE)
    pub operation_type: DestructiveOperationType,
    /// The table or object being affected
    pub affected_object: String,
    /// Why this operation is considered destructive
    pub reason: String,
    /// The full SQL statement
    pub sql: String,
}

/// Types of destructive operations
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DestructiveOperationType {
    /// DELETE without WHERE clause
    DeleteWithoutWhere,
    /// UPDATE without WHERE clause
    UpdateWithoutWhere,
    /// DROP TABLE/DATABASE statement
    Drop,
    /// TRUNCATE TABLE statement
    Truncate,
}

impl DestructiveOperationType {
    /// Get a human-readable name for the operation type
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::DeleteWithoutWhere => "DELETE without WHERE",
            Self::UpdateWithoutWhere => "UPDATE without WHERE",
            Self::Drop => "DROP",
            Self::Truncate => "TRUNCATE",
        }
    }
}

/// Query execution engine
pub struct QueryEngine;

impl QueryEngine {
    /// Create a new query engine
    pub fn new() -> Self {
        Self
    }

    /// Execute a query and return results
    #[tracing::instrument(skip(self, conn, sql), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    pub async fn execute_query(
        &self,
        conn: &Arc<dyn Connection>,
        sql: &str,
    ) -> Result<QueryResult> {
        tracing::info!("executing query");
        let result = conn.query(sql, &[]).await.map_err(|e| {
            tracing::error!(error = %e, "query execution failed");
            e
        })?;
        tracing::info!(
            rows = result.rows.len(),
            execution_time_ms = result.execution_time_ms,
            "query executed successfully"
        );
        Ok(result)
    }

    /// Execute a statement (INSERT, UPDATE, DELETE, etc.)
    #[tracing::instrument(skip(self, conn, sql), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    pub async fn execute_statement(
        &self,
        conn: &Arc<dyn Connection>,
        sql: &str,
    ) -> Result<StatementResult> {
        tracing::info!("executing statement");
        let result = conn.execute(sql, &[]).await.map_err(|e| {
            tracing::error!(error = %e, "statement execution failed");
            e
        })?;
        tracing::info!(
            affected_rows = result.affected_rows,
            "statement executed successfully"
        );
        Ok(result)
    }

    /// Parse SQL and determine if it's a query or statement
    pub fn is_query(&self, sql: &str) -> bool {
        tracing::trace!(sql_preview = %sql.chars().take(50).collect::<String>(), "checking if SQL is query");
        let trimmed = sql.trim().to_uppercase();
        trimmed.starts_with("SELECT")
            || trimmed.starts_with("WITH")
            || trimmed.starts_with("SHOW")
            || trimmed.starts_with("DESCRIBE")
            || trimmed.starts_with("EXPLAIN")
    }

    /// Analyze SQL statement for destructive operations that require confirmation
    pub fn analyze_for_destructive_operations(&self, sql: &str) -> Option<DestructiveOperationWarning> {
        tracing::trace!(sql_preview = %sql.chars().take(100).collect::<String>(), "analyzing SQL for destructive operations");
        
        let dialect = GenericDialect {};
        let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
            tracing::debug!("failed to parse SQL, skipping destructive operation check");
            return None;
        };

        for statement in statements {
            match statement {
                Statement::Delete(delete) => {
                    if delete.selection.is_none() {
                        let table_name = delete.tables.first()
                            .map(|t| t.to_string())
                            .unwrap_or_else(|| "unknown".to_string());
                        
                        return Some(DestructiveOperationWarning {
                            operation_type: DestructiveOperationType::DeleteWithoutWhere,
                            affected_object: table_name.clone(),
                            reason: format!("This DELETE statement will remove ALL rows from table '{}'", table_name),
                            sql: sql.to_string(),
                        });
                    }
                }
                Statement::Update { table, selection, .. } => {
                    if selection.is_none() {
                        let table_name = table.relation.to_string();
                        
                        return Some(DestructiveOperationWarning {
                            operation_type: DestructiveOperationType::UpdateWithoutWhere,
                            affected_object: table_name.clone(),
                            reason: format!("This UPDATE statement will modify ALL rows in table '{}'", table_name),
                            sql: sql.to_string(),
                        });
                    }
                }
                Statement::Drop { object_type, names, .. } => {
                    let object_names = names.iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    
                    return Some(DestructiveOperationWarning {
                        operation_type: DestructiveOperationType::Drop,
                        affected_object: object_names.clone(),
                        reason: format!("This DROP statement will permanently delete {} '{}'", 
                            object_type, object_names),
                        sql: sql.to_string(),
                    });
                }
                Statement::Truncate { table_names, .. } => {
                    let table_names_str = table_names.iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    
                    return Some(DestructiveOperationWarning {
                        operation_type: DestructiveOperationType::Truncate,
                        affected_object: table_names_str.clone(),
                        reason: format!("This TRUNCATE statement will remove ALL rows from table(s) '{}'", 
                            table_names_str),
                        sql: sql.to_string(),
                    });
                }
                _ => {
                    // Not a destructive operation, continue checking other statements
                }
            }
        }

        tracing::trace!("no destructive operations detected");
        None
    }

    /// Generate a DELETE statement with primary key WHERE clause
    /// 
    /// Builds a parameterized DELETE statement using primary key columns for the WHERE clause.
    /// This is safer and more efficient than using full row matching.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to delete from
    /// * `pk_columns` - Names of the primary key columns
    /// * `pk_values` - Values for the primary key columns (must match order of pk_columns)
    ///
    /// # Returns
    /// A tuple of (SQL string, parameter values) ready for execution
    ///
    /// # Example
    /// ```ignore
    /// let (sql, params) = engine.generate_delete_by_pk(
    ///     "users",
    ///     &["id"],
    ///     &[Value::Integer(123)]
    /// );
    /// // sql: "DELETE FROM users WHERE id = ?"
    /// // params: [Value::Integer(123)]
    /// ```
    pub fn generate_delete_by_pk(
        &self,
        table_name: &str,
        pk_columns: &[String],
        pk_values: &[zqlz_core::Value],
    ) -> (String, Vec<zqlz_core::Value>) {
        if pk_columns.is_empty() || pk_values.is_empty() {
            tracing::warn!("Cannot generate DELETE with empty primary key columns or values");
            return (String::new(), Vec::new());
        }

        if pk_columns.len() != pk_values.len() {
            tracing::warn!(
                "Primary key column count ({}) does not match value count ({})",
                pk_columns.len(),
                pk_values.len()
            );
            return (String::new(), Vec::new());
        }

        let where_conditions: Vec<String> = pk_columns
            .iter()
            .map(|col| format!("\"{}\" = ?", col.replace('"', "\"\"")))
            .collect();

        let sql = format!(
            "DELETE FROM \"{}\" WHERE {}",
            table_name.replace('"', "\"\""),
            where_conditions.join(" AND ")
        );

        let params = pk_values.to_vec();

        tracing::debug!(
            table = %table_name,
            pk_columns = ?pk_columns,
            "generated DELETE statement with primary key"
        );

        (sql, params)
    }

    /// Generate a DELETE statement using all column values for matching
    ///
    /// Builds a DELETE statement that matches on all column values. This is used when
    /// no primary key is available. NULL values are matched with IS NULL.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to delete from
    /// * `column_names` - Names of all columns in the row
    /// * `column_values` - Values for all columns (must match order of column_names)
    ///
    /// # Returns
    /// A tuple of (SQL string, parameter values) ready for execution
    pub fn generate_delete_by_full_row(
        &self,
        table_name: &str,
        column_names: &[String],
        column_values: &[zqlz_core::Value],
    ) -> (String, Vec<zqlz_core::Value>) {
        if column_names.is_empty() || column_values.is_empty() {
            tracing::warn!("Cannot generate DELETE with empty columns or values");
            return (String::new(), Vec::new());
        }

        if column_names.len() != column_values.len() {
            tracing::warn!(
                "Column count ({}) does not match value count ({})",
                column_names.len(),
                column_values.len()
            );
            return (String::new(), Vec::new());
        }

        let mut where_conditions = Vec::new();
        let mut params = Vec::new();

        for (col_name, col_value) in column_names.iter().zip(column_values.iter()) {
            if matches!(col_value, zqlz_core::Value::Null) {
                where_conditions.push(format!("\"{}\" IS NULL", col_name.replace('"', "\"\"")));
            } else {
                where_conditions.push(format!("\"{}\" = ?", col_name.replace('"', "\"\"")));
                params.push(col_value.clone());
            }
        }

        let sql = format!(
            "DELETE FROM \"{}\" WHERE {}",
            table_name.replace('"', "\"\""),
            where_conditions.join(" AND ")
        );

        tracing::debug!(
            table = %table_name,
            column_count = column_names.len(),
            "generated DELETE statement with full row match"
        );

        (sql, params)
    }

    /// Generate a DROP TABLE statement
    ///
    /// Returns a DROP TABLE statement. The caller is responsible for executing this
    /// through proper confirmation flows (Feature 113 guardrails).
    pub fn generate_drop_table(&self, table_name: &str, if_exists: bool) -> String {
        let if_exists_clause = if if_exists { " IF EXISTS" } else { "" };
        let sql = format!(
            "DROP TABLE{} \"{}\"",
            if_exists_clause,
            table_name.replace('"', "\"\"")
        );
        
        tracing::debug!(table = %table_name, "generated DROP TABLE statement");
        sql
    }

    /// Generate a TRUNCATE TABLE statement
    ///
    /// Returns a TRUNCATE TABLE statement. The caller is responsible for executing this
    /// through proper confirmation flows (Feature 113 guardrails).
    pub fn generate_truncate_table(&self, table_name: &str) -> String {
        let sql = format!(
            "TRUNCATE TABLE \"{}\"",
            table_name.replace('"', "\"\"")
        );
        
        tracing::debug!(table = %table_name, "generated TRUNCATE TABLE statement");
        sql
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_delete_by_pk_single_column() {
        let engine = QueryEngine::new();
        let pk_columns = vec!["id".to_string()];
        let pk_values = vec![zqlz_core::Value::Int64(123)];

        let (sql, params) = engine.generate_delete_by_pk("users", &pk_columns, &pk_values);

        assert_eq!(sql, "DELETE FROM \"users\" WHERE \"id\" = ?");
        assert_eq!(params.len(), 1);
        assert!(matches!(params[0], zqlz_core::Value::Int64(123)));
    }

    #[test]
    fn test_generate_delete_by_pk_composite_key() {
        let engine = QueryEngine::new();
        let pk_columns = vec!["user_id".to_string(), "role_id".to_string()];
        let pk_values = vec![
            zqlz_core::Value::Int64(10),
            zqlz_core::Value::Int64(20),
        ];

        let (sql, params) = engine.generate_delete_by_pk("user_roles", &pk_columns, &pk_values);

        assert_eq!(
            sql,
            "DELETE FROM \"user_roles\" WHERE \"user_id\" = ? AND \"role_id\" = ?"
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_generate_delete_by_pk_with_quotes_in_table_name() {
        let engine = QueryEngine::new();
        let pk_columns = vec!["id".to_string()];
        let pk_values = vec![zqlz_core::Value::Int64(1)];

        let (sql, _) = engine.generate_delete_by_pk("test\"table", &pk_columns, &pk_values);

        assert_eq!(sql, "DELETE FROM \"test\"\"table\" WHERE \"id\" = ?");
    }

    #[test]
    fn test_generate_delete_by_pk_empty_columns() {
        let engine = QueryEngine::new();
        let pk_columns: Vec<String> = vec![];
        let pk_values: Vec<zqlz_core::Value> = vec![];

        let (sql, params) = engine.generate_delete_by_pk("users", &pk_columns, &pk_values);

        assert_eq!(sql, "");
        assert_eq!(params.len(), 0);
    }

    #[test]
    fn test_generate_delete_by_pk_mismatched_counts() {
        let engine = QueryEngine::new();
        let pk_columns = vec!["id".to_string()];
        let pk_values = vec![zqlz_core::Value::Int64(1), zqlz_core::Value::Int64(2)];

        let (sql, params) = engine.generate_delete_by_pk("users", &pk_columns, &pk_values);

        assert_eq!(sql, "");
        assert_eq!(params.len(), 0);
    }

    #[test]
    fn test_generate_delete_by_full_row() {
        let engine = QueryEngine::new();
        let column_names = vec!["name".to_string(), "age".to_string()];
        let column_values = vec![
            zqlz_core::Value::String("Alice".to_string()),
            zqlz_core::Value::Int64(30),
        ];

        let (sql, params) =
            engine.generate_delete_by_full_row("users", &column_names, &column_values);

        assert_eq!(sql, "DELETE FROM \"users\" WHERE \"name\" = ? AND \"age\" = ?");
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_generate_delete_by_full_row_with_nulls() {
        let engine = QueryEngine::new();
        let column_names = vec!["name".to_string(), "middle_name".to_string(), "age".to_string()];
        let column_values = vec![
            zqlz_core::Value::String("Alice".to_string()),
            zqlz_core::Value::Null,
            zqlz_core::Value::Int64(30),
        ];

        let (sql, params) =
            engine.generate_delete_by_full_row("users", &column_names, &column_values);

        assert_eq!(
            sql,
            "DELETE FROM \"users\" WHERE \"name\" = ? AND \"middle_name\" IS NULL AND \"age\" = ?"
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_generate_drop_table() {
        let engine = QueryEngine::new();
        let sql = engine.generate_drop_table("users", false);
        assert_eq!(sql, "DROP TABLE \"users\"");
    }

    #[test]
    fn test_generate_drop_table_if_exists() {
        let engine = QueryEngine::new();
        let sql = engine.generate_drop_table("users", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"users\"");
    }

    #[test]
    fn test_generate_drop_table_with_quotes() {
        let engine = QueryEngine::new();
        let sql = engine.generate_drop_table("test\"table", false);
        assert_eq!(sql, "DROP TABLE \"test\"\"table\"");
    }

    #[test]
    fn test_generate_truncate_table() {
        let engine = QueryEngine::new();
        let sql = engine.generate_truncate_table("users");
        assert_eq!(sql, "TRUNCATE TABLE \"users\"");
    }

    #[test]
    fn test_generate_truncate_table_with_quotes() {
        let engine = QueryEngine::new();
        let sql = engine.generate_truncate_table("test\"table");
        assert_eq!(sql, "TRUNCATE TABLE \"test\"\"table\"");
    }
}
