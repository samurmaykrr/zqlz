//! Query execution service
//!
//! Provides centralized query execution with automatic history tracking,
//! timing, and error handling.

use parking_lot::RwLock;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{Connection, DriverCategory, ExplainConfig, ExplainParserKind, Value};

#[cfg(test)]
use crate::batch::split_statements;
use crate::batch::split_statements_for_dialect;
use crate::engine::QueryEngine;
use crate::error::{QueryServiceError, QueryServiceResult};
use crate::history::{QueryHistory, QueryHistoryEntry};
use crate::parameters::{BindError, bind_named_with_policy, bind_positional_with_policy};
use crate::view_models::{QueryExecution, StatementExecution, StatementResult};

/// Which flavour of EXPLAIN to run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplainMode {
    /// Planner estimates only. Never executes the statement.
    Plan,
    /// `EXPLAIN ANALYZE`: the server really runs the statement and reports
    /// actual row counts and timings.
    Analyze,
}

/// Whether an EXPLAIN result cell carries a JSON document rather than plain text.
fn explain_result_looks_like_json(result: &zqlz_core::QueryResult) -> bool {
    let Some(value) = result.rows.first().and_then(|row| row.values.first()) else {
        return false;
    };

    match value {
        Value::Json(_) => true,
        Value::String(text) => {
            let trimmed = text.trim_start();
            trimmed.starts_with('[') || trimmed.starts_with('{')
        }
        Value::Null => false,
        other => {
            let rendered = other.to_string();
            let trimmed = rendered.trim_start();
            trimmed.starts_with('[') || trimmed.starts_with('{')
        }
    }
}

/// `text` without its leading `keyword` (case-insensitive, whole word).
fn strip_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let head = text.get(..keyword.len())?;
    let tail = &text[keyword.len()..];
    if !head.eq_ignore_ascii_case(keyword)
        || tail
            .chars()
            .next()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
    {
        return None;
    }
    Some(tail)
}

/// Drops an `EXPLAIN` the user already typed (`EXPLAIN ANALYZE`, `EXPLAIN
/// FORMAT=TREE`, `EXPLAIN (ANALYZE, BUFFERS)`, `EXPLAIN QUERY PLAN`, ...) so the
/// Explain button wraps the statement itself instead of producing
/// `EXPLAIN EXPLAIN ...`. The format and mode come from the button, not the text.
/// Anything that is not `EXPLAIN <statement>` (e.g. `EXPLAIN users`) is returned as is.
fn strip_explain_prefix(sql: &str) -> &str {
    let mut rest = match strip_keyword(sql.trim_start(), "EXPLAIN") {
        Some(rest) => rest,
        None => return sql,
    };

    loop {
        let trimmed = rest.trim_start();
        if let Some(options) = trimmed.strip_prefix('(') {
            match options.find(')') {
                Some(close) => rest = &options[close + 1..],
                None => return sql,
            }
        } else if let Some(after) = ["ANALYZE", "ANALYSE", "EXTENDED", "PARTITIONS", "VERBOSE"]
            .iter()
            .find_map(|keyword| strip_keyword(trimmed, keyword))
        {
            rest = after;
        } else if let Some(after) = strip_keyword(trimmed, "QUERY")
            .and_then(|after| strip_keyword(after.trim_start(), "PLAN"))
        {
            rest = after;
        } else if let Some(after) = strip_keyword(trimmed, "FORMAT")
            .and_then(|after| after.trim_start().strip_prefix('='))
        {
            let after = after.trim_start();
            let word_end = after
                .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                .unwrap_or(after.len());
            rest = &after[word_end..];
        } else {
            break;
        }
    }

    let statement = rest.trim_start();
    let starts_a_statement = statement.starts_with('(')
        || [
            "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "REPLACE", "TABLE", "VALUES", "MERGE",
        ]
        .iter()
        .any(|keyword| strip_keyword(statement, keyword).is_some());
    if starts_a_statement { statement } else { sql }
}

/// Outcome of one statement before timing is attached.
struct StatementOutcome {
    result: Option<zqlz_core::QueryResult>,
    affected_rows: u64,
    warnings: Vec<String>,
}

/// A call that went through `Connection::query`. A command that returned no
/// result set (`SELECT ... INTO`, a data-modifying CTE without RETURNING)
/// reports only its row count, so it is shown as a statement, not an empty grid.
fn statement_from_query_result(query_result: zqlz_core::QueryResult) -> StatementOutcome {
    let warnings = query_result.warnings.clone();
    if query_result.columns.is_empty()
        && query_result.rows.is_empty()
        && query_result.affected_rows > 0
    {
        return StatementOutcome {
            affected_rows: query_result.affected_rows,
            result: None,
            warnings,
        };
    }

    StatementOutcome {
        result: Some(query_result),
        affected_rows: 0,
        warnings,
    }
}

/// A call that went through `Connection::execute`. Statements the server
/// answers with a result set (`INSERT ... RETURNING`, `CALL`, `EXECUTE`, ...)
/// carry their rows; any other `result` only exists to carry notices.
fn statement_from_statement_result(statement_result: zqlz_core::StatementResult) -> StatementOutcome {
    let (result, warnings) = match statement_result.result {
        Some(query_result) => {
            let warnings = query_result.warnings.clone();
            let has_result_set = !query_result.columns.is_empty();
            (has_result_set.then_some(query_result), warnings)
        }
        None => (None, Vec::new()),
    };

    StatementOutcome {
        result,
        affected_rows: statement_result.affected_rows,
        warnings,
    }
}

fn finish_statement(
    sql: &str,
    elapsed: std::time::Duration,
    outcome: zqlz_core::Result<StatementOutcome>,
) -> StatementResult {
    let (result, error, affected_rows, warnings) = match outcome {
        Ok(outcome) => (outcome.result, None, outcome.affected_rows, outcome.warnings),
        Err(e) => (None, Some(with_aborted_transaction_hint(e.to_string())), 0, Vec::new()),
    };

    StatementResult {
        sql: sql.to_string(),
        duration_ms: elapsed.as_millis() as u64,
        duration_micros: elapsed.as_micros() as u64,
        result,
        error,
        affected_rows,
        warnings,
        skipped: false,
    }
}

/// Postgres refuses everything after a failed statement inside an explicit
/// transaction (SQLSTATE 25P02); say how to recover instead of just repeating it.
fn with_aborted_transaction_hint(message: String) -> String {
    if message.contains("SQLSTATE 25P02") {
        format!("{message}. The current transaction is aborted; run ROLLBACK to continue.")
    } else {
        message
    }
}

/// Placeholder for a statement that was never sent because an earlier one failed.
fn skipped_statement(sql: &str) -> StatementResult {
    StatementResult {
        sql: sql.to_string(),
        duration_ms: 0,
        duration_micros: 0,
        result: None,
        error: None,
        affected_rows: 0,
        warnings: Vec::new(),
        skipped: true,
    }
}

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
    /// * `continue_on_error` - keep running the remaining statements after one fails.
    ///   When `false`, the first failure stops the run and every later statement is
    ///   reported as `skipped` without being sent to the server.
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
        continue_on_error: bool,
    ) -> QueryServiceResult<QueryExecution> {
        tracing::debug!("Executing query via QueryService");

        let start = std::time::Instant::now();

        // Split SQL into individual statements
        let statements = self.split_statements_for(&connection, sql);

        let mut statement_results = Vec::new();
        let mut stopped = false;

        for statement_sql in statements {
            let statement_sql = statement_sql.trim();
            if statement_sql.is_empty() {
                continue;
            }
            if stopped {
                statement_results.push(skipped_statement(statement_sql));
                continue;
            }

            let statement_start = std::time::Instant::now();

            // Determine if it's a query or statement
            let is_query = self.is_connection_query(&connection, statement_sql);

            let statement_result = if is_query {
                self.engine
                    .execute_query(&connection, statement_sql)
                    .await
                    .map(|query_result| statement_from_query_result(query_result))
            } else {
                self.engine
                    .execute_statement(&connection, statement_sql)
                    .await
                    .map(|stmt_result| statement_from_statement_result(stmt_result))
            };
            let statement_result =
                finish_statement(statement_sql, statement_start.elapsed(), statement_result);

            stopped = !continue_on_error && statement_result.error.is_some();
            statement_results.push(statement_result);
        }

        let duration = start.elapsed();
        let duration_ms = duration.as_millis() as u64;
        let duration_micros = duration.as_micros() as u64;

        // Track in history
        let success_count = statement_results
            .iter()
            .filter(|s| s.error.is_none() && !s.skipped)
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
            duration_micros,
            statements: statement_results,
        })
    }

    /// Execute a query with named parameters.
    #[tracing::instrument(skip(self, connection, sql, named_params), fields(connection_id = %connection_id, sql_preview = %sql.chars().take(50).collect::<String>(), named_param_count = named_params.len()))]
    pub async fn execute_query_with_named_params(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        sql: &str,
        named_params: &std::collections::HashMap<String, Value>,
        continue_on_error: bool,
    ) -> QueryServiceResult<QueryExecution> {
        let placeholder_policy = connection.bind_placeholder_policy();
        let bound = bind_named_with_policy(sql, named_params, placeholder_policy)
            .map_err(Self::map_bind_error)?;
        self.execute_query_with_bound_params(
            connection,
            connection_id,
            &bound.sql,
            &bound.values,
            continue_on_error,
        )
        .await
    }

    /// Execute a query with positional parameters.
    #[tracing::instrument(skip(self, connection, sql, positional_params), fields(connection_id = %connection_id, sql_preview = %sql.chars().take(50).collect::<String>(), positional_param_count = positional_params.len()))]
    pub async fn execute_query_with_positional_params(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        sql: &str,
        positional_params: &[Value],
        continue_on_error: bool,
    ) -> QueryServiceResult<QueryExecution> {
        let placeholder_policy = connection.bind_placeholder_policy();
        let bound = bind_positional_with_policy(sql, positional_params, placeholder_policy)
            .map_err(Self::map_bind_error)?;
        self.execute_query_with_bound_params(
            connection,
            connection_id,
            &bound.sql,
            &bound.values,
            continue_on_error,
        )
        .await
    }

    async fn execute_query_with_bound_params(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        sql: &str,
        params: &[Value],
        continue_on_error: bool,
    ) -> QueryServiceResult<QueryExecution> {
        tracing::debug!(
            param_count = params.len(),
            "Executing query via QueryService with bound parameters"
        );

        let start = std::time::Instant::now();

        // Split SQL into individual statements
        let statements = self.split_statements_for(&connection, sql);

        let mut statement_results = Vec::new();
        let mut stopped = false;

        for statement_sql in statements {
            let statement_sql = statement_sql.trim();
            if statement_sql.is_empty() {
                continue;
            }
            if stopped {
                statement_results.push(skipped_statement(statement_sql));
                continue;
            }

            let statement_start = std::time::Instant::now();

            // Determine if it's a query or statement
            let is_query = self.is_connection_query(&connection, statement_sql);

            let statement_result = if is_query {
                self.engine
                    .execute_query_with_params(&connection, statement_sql, params)
                    .await
                    .map(|query_result| statement_from_query_result(query_result))
            } else {
                self.engine
                    .execute_statement_with_params(&connection, statement_sql, params)
                    .await
                    .map(|stmt_result| statement_from_statement_result(stmt_result))
            };
            let statement_result =
                finish_statement(statement_sql, statement_start.elapsed(), statement_result);

            stopped = !continue_on_error && statement_result.error.is_some();
            statement_results.push(statement_result);
        }

        let duration = start.elapsed();
        let duration_ms = duration.as_millis() as u64;
        let duration_micros = duration.as_micros() as u64;

        // Track in history
        let success_count = statement_results
            .iter()
            .filter(|s| s.error.is_none() && !s.skipped)
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
            param_count = params.len(),
            "Parameterized query execution completed"
        );

        Ok(QueryExecution {
            sql: sql.to_string(),
            duration_ms,
            duration_micros,
            statements: statement_results,
        })
    }

    /// Split SQL into individual statements.
    ///
    #[cfg(test)]
    fn split_statements(&self, sql: &str) -> Vec<String> {
        split_statements(sql)
    }

    /// Split SQL using the connection's dialect (MySQL `DELIMITER`, `#` comments, ...).
    fn split_statements_for(&self, connection: &Arc<dyn Connection>, sql: &str) -> Vec<String> {
        split_statements_for_dialect(sql, connection.dialect_id())
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

    fn map_bind_error(error: BindError) -> QueryServiceError {
        QueryServiceError::QueryFailed(format!("Parameter binding failed: {}", error))
    }

    fn is_connection_query(&self, connection: &Arc<dyn Connection>, sql: &str) -> bool {
        if connection.driver_category() == DriverCategory::Document {
            let trimmed = sql.trim();
            return trimmed.starts_with('{')
                || trimmed.starts_with("db.")
                || self.engine.is_query(sql);
        }

        self.engine.is_query(sql)
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
    pub async fn explain_query(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        sql: &str,
    ) -> QueryServiceResult<crate::view_models::ExplainResult> {
        self.explain_query_with_mode(connection, connection_id, sql, ExplainMode::Plan)
            .await
    }

    /// Execute `EXPLAIN ANALYZE` on a SQL query, producing real row counts and timings.
    ///
    /// This genuinely executes the statement on the server. Callers are responsible
    /// for confirming intent before invoking this on a non-read-only statement.
    pub async fn explain_analyze_query(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        sql: &str,
    ) -> QueryServiceResult<crate::view_models::ExplainResult> {
        self.explain_query_with_mode(connection, connection_id, sql, ExplainMode::Analyze)
            .await
    }

    #[tracing::instrument(skip(self, connection, sql), fields(connection_id = %connection_id, mode = ?mode, sql_preview = %sql.chars().take(50).collect::<String>()))]
    pub async fn explain_query_with_mode(
        &self,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        sql: &str,
        mode: ExplainMode,
    ) -> QueryServiceResult<crate::view_models::ExplainResult> {
        tracing::debug!(?mode, "Executing EXPLAIN via QueryService");

        let start = std::time::Instant::now();

        // Get just the first statement for EXPLAIN
        let statements = self.split_statements_for(&connection, sql);
        let first_statement = statements.first().map(|s| s.trim()).unwrap_or(sql.trim());
        let first_statement = strip_explain_prefix(first_statement).trim();

        if first_statement.is_empty() {
            return Ok(crate::view_models::ExplainResult {
                sql: sql.to_string(),
                duration_ms: 0,
                duration_micros: 0,
                analyzed: false,
                raw_output: None,
                query_plan: None,
                analyzed_plan: None,
                provider_id: connection.dialect_id().map(ToString::to_string),
                error: Some("No SQL statement to explain".to_string()),
                connection_name: None,
                database_name: None,
            });
        }

        // Get ExplainConfig based on the connection's dialect
        let explain_config = self.get_explain_config_for_connection(&connection);

        // The secondary statement carries the detail: planner costs in plan mode,
        // real row counts and timings in analyze mode.
        let plan_sql = match mode {
            ExplainMode::Plan => explain_config.format_query_plan(first_statement),
            ExplainMode::Analyze => explain_config.format_analyze(first_statement),
        };

        if mode == ExplainMode::Analyze && plan_sql.is_none() {
            let dialect = connection
                .dialect_id()
                .map(ToString::to_string)
                .unwrap_or_else(|| "this database".to_string());
            return Ok(crate::view_models::ExplainResult {
                sql: first_statement.to_string(),
                duration_ms: 0,
                duration_micros: 0,
                analyzed: true,
                raw_output: None,
                query_plan: None,
                analyzed_plan: None,
                provider_id: connection.dialect_id().map(ToString::to_string),
                error: Some(format!("EXPLAIN ANALYZE is not supported by {dialect}")),
                connection_name: None,
                database_name: None,
            });
        }

        // Run the primary EXPLAIN query
        let explain_sql = explain_config.format_explain(first_statement);
        tracing::debug!(explain_sql = %explain_sql, "Running primary EXPLAIN");
        let raw_result = self.engine.execute_query(&connection, &explain_sql).await;

        let plan_result = if let Some(plan_sql) = plan_sql {
            tracing::debug!(plan_sql = %plan_sql, ?mode, "Running secondary EXPLAIN");
            Some(self.engine.execute_query(&connection, &plan_sql).await)
        } else {
            None
        };

        let duration = start.elapsed();
        let duration_ms = duration.as_millis() as u64;
        let duration_micros = duration.as_micros() as u64;

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
            duration_ms,
        );

        tracing::info!(
            duration_ms = duration_ms,
            duration_micros = duration_micros,
            mode = ?mode,
            has_raw = raw_output.is_some(),
            has_plan = query_plan.is_some(),
            has_analysis = analyzed_plan.is_some(),
            dialect = ?connection.dialect_id(),
            "EXPLAIN completed"
        );

        Ok(crate::view_models::ExplainResult {
            sql: first_statement.to_string(),
            duration_ms,
            duration_micros,
            analyzed: mode == ExplainMode::Analyze,
            raw_output,
            query_plan,
            analyzed_plan,
            provider_id: connection.dialect_id().map(ToString::to_string),
            error,
            connection_name: None,
            database_name: None,
        })
    }

    fn parse_and_analyze_explain(
        &self,
        connection: &Arc<dyn Connection>,
        raw_output: Option<&zqlz_core::QueryResult>,
        query_plan: Option<&zqlz_core::QueryResult>,
        duration_ms: u64,
    ) -> Option<zqlz_analyzer::QueryAnalysis> {
        let parser_kind = connection.explain_parser_kind();

        // The PostgreSQL parser tries `raw_output` first, and the plain-text EXPLAIN
        // in `raw_output` always parses, so a JSON payload in `query_plan` would
        // otherwise never be reached - losing actual rows, actual times and
        // "Rows Removed by Filter". Present the JSON result first when there is one.
        // MySQL is the same story: `raw_output` is the classic table, which always
        // parses, while `query_plan` holds the richer FORMAT=JSON document (or the
        // EXPLAIN ANALYZE tree with real timings). The table stays as the fallback
        // for servers that reject or garble the richer format.
        let (primary, secondary) = if (parser_kind == ExplainParserKind::PostgreSql
            && query_plan.is_some_and(explain_result_looks_like_json))
            || (parser_kind == ExplainParserKind::MySql && query_plan.is_some())
        {
            (query_plan, raw_output)
        } else {
            (raw_output, query_plan)
        };

        let mut plan = zqlz_drivers::explain::parse_explain_plan(
            parser_kind,
            connection.dialect_id(),
            primary,
            secondary,
        )?;

        // EXPLAIN ANALYZE reports the server-side execution time, which is far more
        // meaningful than our round-trip wall clock. Only fall back when absent.
        if plan.execution_time_ms.is_none() {
            plan.execution_time_ms = Some(duration_ms as f64);
        }

        Some(zqlz_analyzer::QueryAnalyzer::new().analyze(plan))
    }

    /// Get the ExplainConfig for a connection based on its dialect
    fn get_explain_config_for_connection(&self, connection: &Arc<dyn Connection>) -> ExplainConfig {
        connection.explain_config()
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
    fn strip_explain_prefix_removes_a_typed_explain() {
        for (typed, expected) in [
            ("EXPLAIN SELECT 1", "SELECT 1"),
            ("explain select 1", "select 1"),
            ("  EXPLAIN   ANALYZE SELECT 1", "SELECT 1"),
            ("EXPLAIN FORMAT=TREE SELECT 1", "SELECT 1"),
            ("EXPLAIN FORMAT = JSON SELECT 1", "SELECT 1"),
            ("EXPLAIN ANALYZE FORMAT=TREE WITH c AS (SELECT 1) SELECT * FROM c", "WITH c AS (SELECT 1) SELECT * FROM c"),
            ("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT 1", "SELECT 1"),
            ("EXPLAIN QUERY PLAN SELECT 1", "SELECT 1"),
            ("EXPLAIN EXTENDED SELECT 1", "SELECT 1"),
            ("EXPLAIN UPDATE t SET a = 1", "UPDATE t SET a = 1"),
            ("EXPLAIN DELETE FROM t", "DELETE FROM t"),
        ] {
            assert_eq!(strip_explain_prefix(typed), expected, "{typed}");
        }

        // not an EXPLAIN of a statement: left alone
        for untouched in [
            "SELECT 1",
            "SELECT * FROM explain_log",
            "EXPLAIN users",
            "EXPLAINED SELECT 1",
            "EXPLAIN",
            "EXPLAIN ANALYZE",
            "EXPLAIN (ANALYZE SELECT 1",
        ] {
            assert_eq!(strip_explain_prefix(untouched), untouched, "{untouched}");
        }
    }

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
    fn query_result(columns: &[&str], rows: usize, affected_rows: u64) -> zqlz_core::QueryResult {
        zqlz_core::QueryResult {
            id: Uuid::new_v4(),
            columns: columns
                .iter()
                .enumerate()
                .map(|(ordinal, name)| zqlz_core::ColumnMeta {
                    name: name.to_string(),
                    data_type: "int4".to_string(),
                    nullable: true,
                    ordinal,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: None,
                    enum_values: None,
                })
                .collect(),
            rows: (0..rows)
                .map(|_| zqlz_core::Row::new(vec!["a".to_string()], vec![Value::Int32(1)]))
                .collect(),
            total_rows: Some(rows as u64),
            is_estimated_total: false,
            affected_rows,
            execution_time_ms: 0,
            warnings: vec!["NOTICE: hi".to_string()],
        }
    }

    #[test]
    fn execute_path_keeps_rows_when_the_driver_returns_a_result_set() {
        // INSERT ... RETURNING / CALL / EXECUTE: classified as a statement by
        // keyword, but the driver answered with rows.
        let outcome = statement_from_statement_result(zqlz_core::StatementResult {
            is_query: true,
            result: Some(query_result(&["id"], 2, 0)),
            affected_rows: 2,
            error: None,
        });
        assert_eq!(outcome.result.expect("rows").rows.len(), 2);
        assert_eq!(outcome.affected_rows, 2);
        assert_eq!(outcome.warnings, vec!["NOTICE: hi"]);

        // A plain command only reports counts; a column-less result just
        // carries notices.
        let outcome = statement_from_statement_result(zqlz_core::StatementResult {
            is_query: false,
            result: Some(query_result(&[], 0, 0)),
            affected_rows: 3,
            error: None,
        });
        assert!(outcome.result.is_none());
        assert_eq!(outcome.affected_rows, 3);
        assert_eq!(outcome.warnings, vec!["NOTICE: hi"]);
    }

    #[test]
    fn query_path_without_a_result_set_is_reported_as_a_statement() {
        let outcome = statement_from_query_result(query_result(&[], 0, 4));
        assert!(outcome.result.is_none());
        assert_eq!(outcome.affected_rows, 4);

        let outcome = statement_from_query_result(query_result(&["id"], 0, 0));
        assert!(outcome.result.is_some(), "an empty SELECT still shows its columns");
        assert_eq!(outcome.affected_rows, 0);
    }

    mod stop_on_error {
        use super::*;
        use async_trait::async_trait;
        use std::sync::Mutex;
        use zqlz_core::{
            DropTableOptions, DropTriggerOptions, DropViewOptions, SqlObjectName, Transaction,
            ZqlzError,
        };

        /// Records every statement it is sent; any SQL containing `fail` errors.
        struct ScriptedConnection {
            seen: Mutex<Vec<String>>,
        }

        impl ScriptedConnection {
            fn new() -> Arc<Self> {
                Arc::new(Self {
                    seen: Mutex::new(Vec::new()),
                })
            }

            fn seen(&self) -> Vec<String> {
                self.seen.lock().unwrap().clone()
            }

            fn run(&self, sql: &str) -> zqlz_core::Result<()> {
                self.seen.lock().unwrap().push(sql.to_string());
                if sql.contains("fail") {
                    Err(ZqlzError::Query(format!("boom: {sql}")))
                } else {
                    Ok(())
                }
            }
        }

        fn unsupported<T>() -> zqlz_core::Result<T> {
            Err(ZqlzError::NotSupported("scripted connection".into()))
        }

        #[async_trait]
        impl Connection for ScriptedConnection {
            fn driver_name(&self) -> &str {
                "scripted"
            }

            async fn execute(
                &self,
                sql: &str,
                _params: &[Value],
            ) -> zqlz_core::Result<zqlz_core::StatementResult> {
                self.run(sql)?;
                Ok(zqlz_core::StatementResult {
                    is_query: false,
                    result: None,
                    affected_rows: 1,
                    error: None,
                })
            }

            async fn query(
                &self,
                sql: &str,
                _params: &[Value],
            ) -> zqlz_core::Result<zqlz_core::QueryResult> {
                self.run(sql)?;
                Ok(query_result(&["id"], 1, 0))
            }

            fn rename_table_sql(&self, _: &SqlObjectName, _: &str) -> zqlz_core::Result<String> {
                unsupported()
            }

            fn drop_table_sql(
                &self,
                _: &SqlObjectName,
                _: DropTableOptions,
            ) -> zqlz_core::Result<String> {
                unsupported()
            }

            fn drop_view_sql(
                &self,
                _: &SqlObjectName,
                _: DropViewOptions,
            ) -> zqlz_core::Result<String> {
                unsupported()
            }

            fn drop_trigger_sql(
                &self,
                _: &SqlObjectName,
                _: Option<&SqlObjectName>,
                _: DropTriggerOptions,
            ) -> zqlz_core::Result<String> {
                unsupported()
            }

            fn truncate_table_sql(&self, _: &SqlObjectName) -> zqlz_core::Result<String> {
                unsupported()
            }

            fn duplicate_table_sql(
                &self,
                _: &SqlObjectName,
                _: &SqlObjectName,
            ) -> zqlz_core::Result<String> {
                unsupported()
            }

            fn clear_table_sql(&self, _: &SqlObjectName) -> zqlz_core::Result<String> {
                unsupported()
            }

            fn table_has_rows_sql(&self, _: &SqlObjectName) -> zqlz_core::Result<String> {
                unsupported()
            }

            fn select_rows_sql(
                &self,
                _: &SqlObjectName,
                _: &[String],
                _: Option<&str>,
            ) -> zqlz_core::Result<String> {
                unsupported()
            }

            fn select_distinct_rows_sql(
                &self,
                _: &SqlObjectName,
                _: &[String],
                _: Option<&str>,
                _: &[String],
                _: u64,
            ) -> zqlz_core::Result<String> {
                unsupported()
            }

            fn insert_row_sql(
                &self,
                _: &SqlObjectName,
                _: &[String],
                _: usize,
            ) -> zqlz_core::Result<String> {
                unsupported()
            }

            fn performance_metrics_query_sql(&self) -> zqlz_core::Result<String> {
                unsupported()
            }

            async fn begin_transaction(&self) -> zqlz_core::Result<Box<dyn Transaction>> {
                unsupported()
            }

            async fn close(&self) -> zqlz_core::Result<()> {
                Ok(())
            }

            fn is_closed(&self) -> bool {
                false
            }
        }

        const SCRIPT: &str = "SELECT 1; INSERT INTO t VALUES (fail); SELECT 3; DELETE FROM t";

        async fn run(continue_on_error: bool) -> (Vec<StatementResult>, Vec<String>) {
            let connection = ScriptedConnection::new();
            let execution = QueryService::new()
                .execute_query(connection.clone(), Uuid::new_v4(), SCRIPT, continue_on_error)
                .await
                .unwrap();
            (execution.statements, connection.seen())
        }

        #[tokio::test]
        async fn stops_at_the_first_error_and_reports_the_rest_as_skipped() {
            let (statements, seen) = run(false).await;

            assert_eq!(seen, ["SELECT 1", "INSERT INTO t VALUES (fail)"]);
            assert_eq!(statements.len(), 4);
            assert!(statements[0].result.is_some() && statements[0].error.is_none());
            assert!(statements[1].error.as_deref().unwrap().contains("boom"));
            assert!(!statements[1].skipped);
            for skipped in &statements[2..] {
                assert!(skipped.skipped && skipped.error.is_none() && skipped.result.is_none());
            }
            assert_eq!(statements[2].sql, "SELECT 3");
            assert_eq!(statements[3].sql, "DELETE FROM t");
        }

        #[tokio::test]
        async fn continue_on_error_runs_everything() {
            let (statements, seen) = run(true).await;

            assert_eq!(seen.len(), 4);
            assert!(statements.iter().all(|s| !s.skipped));
            assert_eq!(statements.iter().filter(|s| s.error.is_some()).count(), 1);
            assert!(statements[2].result.is_some());
            assert_eq!(statements[3].affected_rows, 1);
        }

        #[tokio::test]
        async fn a_failing_first_statement_skips_everything_after_it() {
            let connection = ScriptedConnection::new();
            let execution = QueryService::new()
                .execute_query(
                    connection.clone(),
                    Uuid::new_v4(),
                    "SELECT fail; SELECT 2; SELECT 3",
                    false,
                )
                .await
                .unwrap();

            assert_eq!(connection.seen(), ["SELECT fail"]);
            let flags: Vec<_> = execution.statements.iter().map(|s| s.skipped).collect();
            assert_eq!(flags, [false, true, true]);
            assert!(execution.statements[0].error.is_some());
        }

        #[tokio::test]
        async fn success_path_is_unchanged_in_both_modes() {
            for continue_on_error in [false, true] {
                let connection = ScriptedConnection::new();
                let service = QueryService::new();
                let execution = service
                    .execute_query(
                        connection.clone(),
                        Uuid::new_v4(),
                        "SELECT 1; SELECT 2; DELETE FROM t",
                        continue_on_error,
                    )
                    .await
                    .unwrap();

                assert_eq!(connection.seen().len(), 3);
                assert!(
                    execution
                        .statements
                        .iter()
                        .all(|s| s.error.is_none() && !s.skipped)
                );
            }
        }

        #[tokio::test]
        async fn bound_params_path_honours_the_policy() {
            for (continue_on_error, expected_runs) in [(false, 2), (true, 3)] {
                let connection = ScriptedConnection::new();
                let execution = QueryService::new()
                    .execute_query_with_positional_params(
                        connection.clone(),
                        Uuid::new_v4(),
                        "SELECT 1; SELECT fail; SELECT 3",
                        &[],
                        continue_on_error,
                    )
                    .await
                    .unwrap();

                assert_eq!(connection.seen().len(), expected_runs);
                assert_eq!(
                    execution.statements.iter().filter(|s| s.skipped).count(),
                    3 - expected_runs
                );
            }
        }

        #[tokio::test]
        async fn a_stopped_run_is_recorded_as_a_failure_in_history() {
            let history = Arc::new(RwLock::new(QueryHistory::new(10)));
            let service = QueryService::with_shared_history(history.clone());
            service
                .execute_query(ScriptedConnection::new(), Uuid::new_v4(), SCRIPT, false)
                .await
                .unwrap();

            let history = history.read();
            let entry = history.entries().next().expect("history entry");
            assert!(!entry.success);
        }

        #[test]
        fn aborted_transaction_errors_say_how_to_recover() {
            let hint = with_aborted_transaction_hint(
                "current transaction is aborted, commands ignored until end of transaction block (SQLSTATE 25P02)"
                    .to_string(),
            );
            assert!(hint.contains("run ROLLBACK"));

            let other = "relation \"x\" does not exist (SQLSTATE 42P01)".to_string();
            assert_eq!(with_aborted_transaction_hint(other.clone()), other);
        }
    }
}
