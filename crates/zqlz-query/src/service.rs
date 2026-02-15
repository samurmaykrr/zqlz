//! Query execution service
//!
//! Provides centralized query execution with automatic history tracking,
//! timing, and error handling.

use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use uuid::Uuid;
use zqlz_analyzer::{QueryAnalyzer, parse_postgres_explain, parse_mysql_explain, parse_sqlite_explain};
use zqlz_core::{Connection, ExplainConfig};

use crate::engine::QueryEngine;
use crate::error::{QueryServiceError, QueryServiceResult};
use crate::history::{QueryHistory, QueryHistoryEntry};
use crate::view_models::{QueryExecution, StatementExecution, StatementResult};

/// Service for executing queries and statements
///
/// This service wraps the `QueryEngine` and automatically:
/// - Measures execution time
/// - Tracks query history
/// - Formats results for UI consumption
/// - Provides user-friendly error messages
pub struct QueryService {
    engine: QueryEngine,
    history: Arc<RwLock<QueryHistory>>,
}

impl QueryService {
    /// Create a new query service with a default internal history
    pub fn new() -> Self {
        Self {
            engine: QueryEngine::new(),
            history: Arc::new(RwLock::new(QueryHistory::new(1000))),
        }
    }

    /// Create a query service with a shared history instance
    ///
    /// This allows multiple components to share the same query history.
    pub fn with_shared_history(history: Arc<RwLock<QueryHistory>>) -> Self {
        Self {
            engine: QueryEngine::new(),
            history,
        }
    }

    /// Execute a query with full metadata tracking
    ///
    /// # Arguments
    ///
    /// * `connection` - Database connection to execute against
    /// * `connection_id` - UUID of the connection for history tracking
    /// * `sql` - SQL query to execute
    ///
    /// # Returns
    ///
    /// A `QueryExecution` containing results, timing, and any errors
    #[tracing::instrument(skip(self, connection, sql), fields(connection_id = %connection_id, sql_preview = %sql.chars().take(50).collect::<String>()))]
    pub async fn execute_query(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        sql: &str,
    ) -> QueryServiceResult<QueryExecution> {
        tracing::debug!("Executing query via QueryService");

        let start = std::time::Instant::now();

        // Split SQL into individual statements
        let statements = self.split_statements(sql);

        let mut statement_results = Vec::new();

        for statement_sql in statements {
            let statement_sql = statement_sql.trim();
            if statement_sql.is_empty() {
                continue;
            }

            let statement_start = std::time::Instant::now();

            // Determine if it's a query or statement
            let is_query = self.engine.is_query(statement_sql);

            let statement_result = if is_query {
                match self.engine.execute_query(&connection, statement_sql).await {
                    Ok(query_result) => {
                        let duration = statement_start.elapsed().as_millis() as u64;
                        StatementResult {
                            sql: statement_sql.to_string(),
                            duration_ms: duration,
                            result: Some(query_result),
                            error: None,
                            affected_rows: 0,
                        }
                    }
                    Err(e) => {
                        let duration = statement_start.elapsed().as_millis() as u64;
                        StatementResult {
                            sql: statement_sql.to_string(),
                            duration_ms: duration,
                            result: None,
                            error: Some(e.to_string()),
                            affected_rows: 0,
                        }
                    }
                }
            } else {
                match self
                    .engine
                    .execute_statement(&connection, statement_sql)
                    .await
                {
                    Ok(stmt_result) => {
                        let duration = statement_start.elapsed().as_millis() as u64;
                        StatementResult {
                            sql: statement_sql.to_string(),
                            duration_ms: duration,
                            result: None,
                            error: None,
                            affected_rows: stmt_result.affected_rows,
                        }
                    }
                    Err(e) => {
                        let duration = statement_start.elapsed().as_millis() as u64;
                        StatementResult {
                            sql: statement_sql.to_string(),
                            duration_ms: duration,
                            result: None,
                            error: Some(e.to_string()),
                            affected_rows: 0,
                        }
                    }
                }
            };

            statement_results.push(statement_result);
        }

        let duration = start.elapsed();
        let duration_ms = duration.as_millis() as u64;

        // Track in history
        let success_count = statement_results
            .iter()
            .filter(|s| s.error.is_none())
            .count();
        let total_rows: u64 = statement_results
            .iter()
            .filter_map(|s| s.result.as_ref())
            .map(|r| r.rows.len() as u64)
            .sum();

        let entry = if success_count == statement_results.len() {
            QueryHistoryEntry::success(
                sql.to_string(),
                Some(connection_id),
                duration_ms,
                total_rows,
            )
        } else {
            let errors: Vec<String> = statement_results
                .iter()
                .filter_map(|s| s.error.as_ref())
                .cloned()
                .collect();
            QueryHistoryEntry::failure(
                sql.to_string(),
                Some(connection_id),
                duration_ms,
                errors.join("; "),
            )
        };
        self.history.write().add(entry);

        tracing::info!(
            statements = statement_results.len(),
            success = success_count,
            duration_ms = duration_ms,
            "Query execution completed"
        );

        Ok(QueryExecution {
            sql: sql.to_string(),
            duration_ms,
            statements: statement_results,
        })
    }

    /// Split SQL into individual statements.
    ///
    /// This implementation correctly handles:
    /// - Semicolons inside single-quoted strings ('...')
    /// - Semicolons inside double-quoted identifiers ("...")
    /// - Semicolons inside single-line comments (--)
    /// - Semicolons inside multi-line comments (/* ... */)
    fn split_statements(&self, sql: &str) -> Vec<String> {
        let mut statements = Vec::new();
        let mut current_statement = String::new();
        let mut chars = sql.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                // Single-quoted string
                '\'' => {
                    current_statement.push(c);
                    // Consume until closing quote, handling escaped quotes ('')
                    while let Some(sc) = chars.next() {
                        current_statement.push(sc);
                        if sc == '\'' {
                            // Check for escaped quote ('')
                            if chars.peek() == Some(&'\'') {
                                current_statement.push(chars.next().unwrap());
                            } else {
                                break;
                            }
                        }
                    }
                }
                // Double-quoted identifier
                '"' => {
                    current_statement.push(c);
                    // Consume until closing quote, handling escaped quotes ("")
                    while let Some(sc) = chars.next() {
                        current_statement.push(sc);
                        if sc == '"' {
                            // Check for escaped quote ("")
                            if chars.peek() == Some(&'"') {
                                current_statement.push(chars.next().unwrap());
                            } else {
                                break;
                            }
                        }
                    }
                }
                // Possible comment start
                '-' => {
                    if chars.peek() == Some(&'-') {
                        // Single-line comment
                        current_statement.push(c);
                        current_statement.push(chars.next().unwrap());
                        // Consume until newline
                        while let Some(sc) = chars.next() {
                            current_statement.push(sc);
                            if sc == '\n' {
                                break;
                            }
                        }
                    } else {
                        current_statement.push(c);
                    }
                }
                '/' => {
                    if chars.peek() == Some(&'*') {
                        // Multi-line comment
                        current_statement.push(c);
                        current_statement.push(chars.next().unwrap());
                        // Consume until */
                        let mut prev = '\0';
                        while let Some(sc) = chars.next() {
                            current_statement.push(sc);
                            if prev == '*' && sc == '/' {
                                break;
                            }
                            prev = sc;
                        }
                    } else {
                        current_statement.push(c);
                    }
                }
                // Statement terminator
                ';' => {
                    let trimmed = current_statement.trim();
                    if !trimmed.is_empty() {
                        statements.push(trimmed.to_string());
                    }
                    current_statement.clear();
                }
                _ => {
                    current_statement.push(c);
                }
            }
        }

        // Don't forget the last statement (may not have a trailing semicolon)
        let trimmed = current_statement.trim();
        if !trimmed.is_empty() {
            statements.push(trimmed.to_string());
        }

        statements
    }

    /// Execute a statement (INSERT, UPDATE, DELETE, CREATE, etc.)
    ///
    /// # Arguments
    ///
    /// * `connection` - Database connection to execute against
    /// * `connection_id` - UUID of the connection for history tracking
    /// * `sql` - SQL statement to execute
    ///
    /// # Returns
    ///
    /// A `StatementExecution` containing affected rows, timing, and any errors
    #[tracing::instrument(skip(self, connection, sql), fields(connection_id = %connection_id, sql_preview = %sql.chars().take(50).collect::<String>()))]
    pub async fn execute_statement(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        sql: &str,
    ) -> QueryServiceResult<StatementExecution> {
        tracing::debug!("Executing statement via QueryService");

        let start = std::time::Instant::now();

        let result = self.engine.execute_statement(&connection, sql).await;

        let duration = start.elapsed();
        let duration_ms = duration.as_millis() as u64;

        match result {
            Ok(statement_result) => {
                let affected_rows = statement_result.affected_rows as usize;

                // Track success in history
                let entry = QueryHistoryEntry::success(
                    sql.to_string(),
                    Some(connection_id),
                    duration_ms,
                    affected_rows as u64,
                );
                self.history.write().add(entry);

                tracing::info!(
                    affected_rows = affected_rows,
                    duration_ms = duration_ms,
                    "Statement executed successfully"
                );

                Ok(StatementExecution {
                    sql: sql.to_string(),
                    duration_ms,
                    affected_rows,
                    success: true,
                    error: None,
                })
            }
            Err(e) => {
                let error_msg = e.to_string();

                // Track failure in history
                let entry = QueryHistoryEntry::failure(
                    sql.to_string(),
                    Some(connection_id),
                    duration_ms,
                    error_msg.clone(),
                );
                self.history.write().add(entry);

                tracing::error!(
                    error = %e,
                    duration_ms = duration_ms,
                    "Statement execution failed"
                );

                Err(QueryServiceError::StatementFailed(error_msg))
            }
        }
    }

    /// Get recent query history
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of entries to return (default: 100)
    ///
    /// # Returns
    ///
    /// Vector of query history entries, most recent first
    pub fn get_history(&self, limit: usize) -> Vec<QueryHistoryEntry> {
        self.history.read().entries().take(limit).cloned().collect()
    }

    /// Search query history
    ///
    /// # Arguments
    ///
    /// * `query` - Search term to filter by
    ///
    /// # Returns
    ///
    /// Vector of matching query history entries
    pub fn search_history(&self, query: &str) -> Vec<QueryHistoryEntry> {
        self.history.read().search(query).cloned().collect()
    }

    /// Execute EXPLAIN on a SQL query
    ///
    /// Uses the connection's dialect to determine the correct EXPLAIN syntax.
    /// For SQLite, this runs both EXPLAIN (opcodes) and EXPLAIN QUERY PLAN.
    /// For PostgreSQL, this runs EXPLAIN with different output formats.
    ///
    /// # Arguments
    ///
    /// * `connection` - Database connection to execute against
    /// * `connection_id` - UUID of the connection for tracking
    /// * `sql` - SQL query to explain
    ///
    /// # Returns
    ///
    /// An `ExplainResult` containing the EXPLAIN output
    #[tracing::instrument(skip(self, connection, sql), fields(connection_id = %connection_id, sql_preview = %sql.chars().take(50).collect::<String>()))]
    pub async fn explain_query(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        sql: &str,
    ) -> QueryServiceResult<crate::view_models::ExplainResult> {
        tracing::debug!("Executing EXPLAIN via QueryService");

        let start = std::time::Instant::now();

        // Get just the first statement for EXPLAIN
        let statements = self.split_statements(sql);
        let first_statement = statements.first().map(|s| s.trim()).unwrap_or(sql.trim());

        if first_statement.is_empty() {
            return Ok(crate::view_models::ExplainResult {
                sql: sql.to_string(),
                duration_ms: 0,
                raw_output: None,
                query_plan: None,
                analyzed_plan: None,
                error: Some("No SQL statement to explain".to_string()),
                connection_name: None,
                database_name: None,
            });
        }

        // Get ExplainConfig based on the connection's dialect
        let explain_config = self.get_explain_config_for_connection(&connection);

        // Run the primary EXPLAIN query
        let explain_sql = explain_config.format_explain(first_statement);
        tracing::debug!(explain_sql = %explain_sql, "Running primary EXPLAIN");
        let raw_result = self.engine.execute_query(&connection, &explain_sql).await;

        // Run the query plan EXPLAIN if available for this dialect
        let plan_result = if let Some(plan_sql) = explain_config.format_query_plan(first_statement)
        {
            tracing::debug!(plan_sql = %plan_sql, "Running query plan EXPLAIN");
            Some(self.engine.execute_query(&connection, &plan_sql).await)
        } else {
            None
        };

        let duration = start.elapsed();
        let duration_ms = duration.as_millis() as u64;

        // Determine overall success/error
        let (raw_output, query_plan, error) = match (&raw_result, &plan_result) {
            (Ok(raw), Some(Ok(plan))) => (Some(raw.clone()), Some(plan.clone()), None),
            (Ok(raw), Some(Err(_))) => (Some(raw.clone()), None, None),
            (Ok(raw), None) => (Some(raw.clone()), None, None),
            (Err(e), Some(Ok(plan))) => (None, Some(plan.clone()), Some(e.to_string())),
            (Err(e), Some(Err(_))) => (None, None, Some(e.to_string())),
            (Err(e), None) => (None, None, Some(e.to_string())),
        };

        // Parse and analyze the explain output if available
        let analyzed_plan = self.parse_and_analyze_explain(
            &connection,
            raw_output.as_ref(),
            query_plan.as_ref(),
            duration_ms
        );

        tracing::info!(
            duration_ms = duration_ms,
            has_raw = raw_output.is_some(),
            has_plan = query_plan.is_some(),
            has_analysis = analyzed_plan.is_some(),
            dialect = ?connection.dialect_id(),
            "EXPLAIN completed"
        );

        Ok(crate::view_models::ExplainResult {
            sql: first_statement.to_string(),
            duration_ms,
            raw_output,
            query_plan,
            analyzed_plan,
            error,
            connection_name: None,
            database_name: None,
        })
    }

    /// Parse and analyze EXPLAIN output
    fn parse_and_analyze_explain(
        &self,
        connection: &Arc<dyn Connection>,
        raw_output: Option<&zqlz_core::QueryResult>,
        query_plan: Option<&zqlz_core::QueryResult>,
        duration_ms: u64,
    ) -> Option<zqlz_analyzer::QueryAnalysis> {
        let dialect = connection.dialect_id()?;
        
        // Try to parse based on dialect
        let parsed_plan = match dialect {
            "postgresql" | "postgres" => {
                // For PostgreSQL, try to parse the raw output as JSON
                if let Some(raw) = raw_output {
                    // PostgreSQL EXPLAIN output is typically a single row with JSON
                    if let Some(first_row) = raw.rows.first() {
                        if let Some(first_value) = first_row.values.first() {
                            let json_str = first_value.to_string();
                            parse_postgres_explain(&json_str).ok()
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            "mysql" | "mariadb" => {
                // For MySQL, try to parse the raw output as JSON
                if let Some(raw) = raw_output {
                    if let Some(first_row) = raw.rows.first() {
                        if let Some(first_value) = first_row.values.first() {
                            let json_str = first_value.to_string();
                            parse_mysql_explain(&json_str).ok()
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            "sqlite" => {
                // For SQLite, parse the query plan output
                if let Some(plan) = query_plan {
                    // Convert QueryResult to text format for parsing
                    let plan_text = Self::query_result_to_text(plan);
                    parse_sqlite_explain(&plan_text).ok()
                } else {
                    None
                }
            }
            _ => None,
        };

        // If we successfully parsed the plan, analyze it
        parsed_plan.map(|mut plan| {
            // Add execution time from EXPLAIN itself
            plan.execution_time_ms = Some(duration_ms as f64);
            
            // Run the analyzer to get suggestions
            let analyzer = QueryAnalyzer::new();
            analyzer.analyze(plan)
        })
    }

    /// Convert QueryResult to text format for parsing
    fn query_result_to_text(result: &zqlz_core::QueryResult) -> String {
        let mut lines = Vec::new();
        
        // Add header
        if !result.columns.is_empty() {
            let header: Vec<_> = result.columns.iter().map(|c| c.name.as_str()).collect();
            lines.push(header.join("\t"));
        }
        
        // Add rows
        for row in &result.rows {
            let row_text: Vec<_> = row.values.iter().map(|v| v.to_string()).collect();
            lines.push(row_text.join("\t"));
        }
        
        lines.join("\n")
    }

    /// Get the ExplainConfig for a connection based on its dialect
    fn get_explain_config_for_connection(&self, connection: &Arc<dyn Connection>) -> ExplainConfig {
        match connection.dialect_id() {
            Some("sqlite") => ExplainConfig::sqlite(),
            Some("postgresql") | Some("postgres") => ExplainConfig::postgresql(),
            Some("mysql") | Some("mariadb") => ExplainConfig::mysql(),
            _ => ExplainConfig::default(),
        }
    }

    /// Check if SQL is a query (SELECT, WITH, SHOW, etc.) or a statement
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL text to analyze
    ///
    /// # Returns
    ///
    /// `true` if the SQL is a query that returns results, `false` if it's a statement
    pub fn is_query(&self, sql: &str) -> bool {
        self.engine.is_query(sql)
    }

    /// Get the underlying query engine
    ///
    /// This is provided for advanced use cases but should rarely be needed.
    /// Prefer using the service methods instead.
    pub fn engine(&self) -> &QueryEngine {
        &self.engine
    }

    /// Get a reference to the query history
    pub fn history(&self) -> Arc<RwLock<QueryHistory>> {
        self.history.clone()
    }
}

impl Default for QueryService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_query() {
        let service = QueryService::new();

        assert!(service.is_query("SELECT * FROM users"));
        assert!(service.is_query("WITH cte AS (SELECT 1) SELECT * FROM cte"));
        assert!(service.is_query("SHOW TABLES"));
        assert!(service.is_query("DESCRIBE users"));
        assert!(service.is_query("EXPLAIN SELECT * FROM users"));

        assert!(!service.is_query("INSERT INTO users VALUES (1)"));
        assert!(!service.is_query("UPDATE users SET name = 'foo'"));
        assert!(!service.is_query("DELETE FROM users"));
        assert!(!service.is_query("CREATE TABLE users (id INT)"));
        assert!(!service.is_query("DROP TABLE users"));
    }

    #[test]
    fn test_split_statements_basic() {
        let service = QueryService::new();

        // Basic split
        let stmts = service.split_statements("SELECT 1; SELECT 2");
        assert_eq!(stmts, vec!["SELECT 1", "SELECT 2"]);

        // Trailing semicolon
        let stmts = service.split_statements("SELECT 1; SELECT 2;");
        assert_eq!(stmts, vec!["SELECT 1", "SELECT 2"]);

        // Single statement without semicolon
        let stmts = service.split_statements("SELECT 1");
        assert_eq!(stmts, vec!["SELECT 1"]);

        // Empty input
        let stmts = service.split_statements("");
        assert!(stmts.is_empty());

        // Whitespace only
        let stmts = service.split_statements("   \n\t  ");
        assert!(stmts.is_empty());
    }

    #[test]
    fn test_split_statements_with_strings() {
        let service = QueryService::new();

        // Semicolon inside single-quoted string
        let stmts = service.split_statements("SELECT 'hello; world'; SELECT 2");
        assert_eq!(stmts, vec!["SELECT 'hello; world'", "SELECT 2"]);

        // Escaped single quote
        let stmts = service.split_statements("SELECT 'it''s a test'; SELECT 2");
        assert_eq!(stmts, vec!["SELECT 'it''s a test'", "SELECT 2"]);

        // Semicolon inside double-quoted identifier
        let stmts = service.split_statements("SELECT \"col;name\" FROM t; SELECT 2");
        assert_eq!(stmts, vec!["SELECT \"col;name\" FROM t", "SELECT 2"]);
    }

    #[test]
    fn test_split_statements_with_comments() {
        let service = QueryService::new();

        // Single-line comment with semicolon
        let stmts = service
            .split_statements("SELECT 1 -- this is a comment; not a new statement\n; SELECT 2");
        assert_eq!(
            stmts,
            vec![
                "SELECT 1 -- this is a comment; not a new statement",
                "SELECT 2"
            ]
        );

        // Multi-line comment with semicolon
        let stmts = service.split_statements("SELECT 1 /* comment; with semicolon */; SELECT 2");
        assert_eq!(
            stmts,
            vec!["SELECT 1 /* comment; with semicolon */", "SELECT 2"]
        );

        // Multi-line comment spanning lines
        let stmts = service.split_statements("SELECT 1 /* multi\nline\ncomment; */; SELECT 2");
        assert_eq!(
            stmts,
            vec!["SELECT 1 /* multi\nline\ncomment; */", "SELECT 2"]
        );
    }

    #[test]
    fn test_split_statements_complex() {
        let service = QueryService::new();

        // Combination of strings and comments
        // Note: The comment line between INSERT and SELECT becomes part of the next statement
        // since there's no semicolon after the comment
        let sql = r#"
            INSERT INTO logs (msg) VALUES ('Error; see details');
            -- Log entry with semicolon; in comment
            SELECT * FROM logs WHERE msg LIKE '%;%';
            /* Multi-line
               comment with ; semicolon */
            UPDATE logs SET msg = 'fixed; issue'
        "#;
        let stmts = service.split_statements(sql);
        // Results:
        // 1. INSERT statement
        // 2. (comment line) + SELECT statement
        // 3. (multi-line comment) + UPDATE statement
        assert_eq!(stmts.len(), 3);
        assert!(stmts[0].contains("INSERT"));
        assert!(stmts[1].contains("SELECT"));
        assert!(stmts[1].contains("-- Log entry")); // Comment is attached to SELECT
        assert!(stmts[2].contains("UPDATE"));
        assert!(stmts[2].contains("/* Multi-line")); // Comment is attached to UPDATE
    }
}
