//! Batch query executor implementation
//!
//! Provides optimized execution of multiple SQL statements with configurable
//! execution modes, error handling strategies, and transaction support.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use zqlz_core::{Connection, QueryResult, Result, StatementResult as CoreStatementResult, Value};

/// Configuration options for batch execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOptions {
    /// Execution mode for the batch
    pub mode: ExecutionMode,
    /// Whether to stop on first error or continue with remaining statements
    pub stop_on_error: bool,
    /// Whether to execute all statements within a single transaction
    pub transaction: bool,
    /// Maximum number of parallel executions (only applies to Parallel mode)
    pub max_parallelism: usize,
    /// Timeout per statement in milliseconds (0 = no timeout)
    pub statement_timeout_ms: u64,
}

impl BatchOptions {
    /// Create new batch options with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Create options for sequential execution
    pub fn sequential() -> Self {
        Self {
            mode: ExecutionMode::Sequential,
            ..Self::default()
        }
    }

    /// Create options for parallel execution
    pub fn parallel() -> Self {
        Self {
            mode: ExecutionMode::Parallel,
            ..Self::default()
        }
    }

    /// Set execution mode
    pub fn with_mode(mut self, mode: ExecutionMode) -> Self {
        self.mode = mode;
        self
    }

    /// Enable stop on error behavior
    pub fn with_stop_on_error(mut self, stop: bool) -> Self {
        self.stop_on_error = stop;
        self
    }

    /// Enable transaction wrapping
    pub fn with_transaction(mut self, transaction: bool) -> Self {
        self.transaction = transaction;
        self
    }

    /// Set maximum parallelism for parallel execution
    pub fn with_max_parallelism(mut self, max: usize) -> Self {
        self.max_parallelism = max.max(1);
        self
    }

    /// Set statement timeout in milliseconds
    pub fn with_statement_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.statement_timeout_ms = timeout_ms;
        self
    }
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            mode: ExecutionMode::Sequential,
            stop_on_error: true,
            transaction: false,
            max_parallelism: 4,
            statement_timeout_ms: 0,
        }
    }
}

/// Execution mode for batch operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    /// Execute statements one at a time in order
    Sequential,
    /// Execute statements in parallel (order not guaranteed)
    Parallel,
}

impl Default for ExecutionMode {
    fn default() -> Self {
        Self::Sequential
    }
}

/// Status of a single statement in the batch
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatementStatus {
    /// Statement executed successfully
    Success,
    /// Statement failed with an error
    Failed,
    /// Statement was skipped (due to stop_on_error)
    Skipped,
    /// Statement is pending execution
    Pending,
}

/// Error information for a failed statement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementError {
    /// Error message
    pub message: String,
    /// Error code (if available from database)
    pub code: Option<String>,
}

impl StatementError {
    /// Create a new statement error
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: None,
        }
    }

    /// Create a statement error with a code
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }
}

impl From<zqlz_core::ZqlzError> for StatementError {
    fn from(err: zqlz_core::ZqlzError) -> Self {
        Self::new(err.to_string())
    }
}

impl std::fmt::Display for StatementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(code) = &self.code {
            write!(f, "[{}] {}", code, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

/// Result of executing a single statement in the batch
#[derive(Debug, Clone)]
pub struct BatchResult {
    /// Index of this statement in the batch (0-based)
    pub index: usize,
    /// The SQL that was executed
    pub sql: String,
    /// Status of execution
    pub status: StatementStatus,
    /// Query result (for SELECT statements)
    pub query_result: Option<QueryResult>,
    /// Rows affected (for DML statements)
    pub affected_rows: u64,
    /// Error details (if status is Failed)
    pub error: Option<StatementError>,
    /// Execution time for this statement
    pub execution_time: Duration,
}

impl BatchResult {
    /// Create a successful query result
    pub fn success_query(
        index: usize,
        sql: String,
        result: QueryResult,
        duration: Duration,
    ) -> Self {
        Self {
            index,
            sql,
            status: StatementStatus::Success,
            query_result: Some(result),
            affected_rows: 0,
            error: None,
            execution_time: duration,
        }
    }

    /// Create a successful statement result
    pub fn success_statement(
        index: usize,
        sql: String,
        affected_rows: u64,
        duration: Duration,
    ) -> Self {
        Self {
            index,
            sql,
            status: StatementStatus::Success,
            query_result: None,
            affected_rows,
            error: None,
            execution_time: duration,
        }
    }

    /// Create a failed result
    pub fn failed(index: usize, sql: String, error: StatementError, duration: Duration) -> Self {
        Self {
            index,
            sql,
            status: StatementStatus::Failed,
            query_result: None,
            affected_rows: 0,
            error: Some(error),
            execution_time: duration,
        }
    }

    /// Create a skipped result
    pub fn skipped(index: usize, sql: String) -> Self {
        Self {
            index,
            sql,
            status: StatementStatus::Skipped,
            query_result: None,
            affected_rows: 0,
            error: None,
            execution_time: Duration::ZERO,
        }
    }

    /// Check if this result represents a successful execution
    pub fn is_success(&self) -> bool {
        self.status == StatementStatus::Success
    }

    /// Check if this result represents a failed execution
    pub fn is_failed(&self) -> bool {
        self.status == StatementStatus::Failed
    }

    /// Check if this statement was skipped
    pub fn is_skipped(&self) -> bool {
        self.status == StatementStatus::Skipped
    }
}

/// Result of batch execution containing all statement results
#[derive(Debug, Clone)]
pub struct BatchExecutionResult {
    /// Results for each statement in order
    pub results: Vec<BatchResult>,
    /// Total execution time for the entire batch
    pub total_execution_time: Duration,
    /// Number of successful statements
    pub success_count: usize,
    /// Number of failed statements
    pub failure_count: usize,
    /// Number of skipped statements
    pub skipped_count: usize,
    /// Whether the batch was executed in a transaction
    pub was_transactional: bool,
    /// Whether the transaction was rolled back (if transactional)
    pub transaction_rolled_back: bool,
}

impl BatchExecutionResult {
    /// Create a new batch execution result
    pub fn new(
        results: Vec<BatchResult>,
        total_time: Duration,
        was_transactional: bool,
        rolled_back: bool,
    ) -> Self {
        let success_count = results.iter().filter(|r| r.is_success()).count();
        let failure_count = results.iter().filter(|r| r.is_failed()).count();
        let skipped_count = results.iter().filter(|r| r.is_skipped()).count();

        Self {
            results,
            total_execution_time: total_time,
            success_count,
            failure_count,
            skipped_count,
            was_transactional,
            transaction_rolled_back: rolled_back,
        }
    }

    /// Check if all statements executed successfully
    pub fn all_succeeded(&self) -> bool {
        self.failure_count == 0 && self.skipped_count == 0
    }

    /// Check if any statement failed
    pub fn has_failures(&self) -> bool {
        self.failure_count > 0
    }

    /// Get the total number of rows affected across all statements
    pub fn total_affected_rows(&self) -> u64 {
        self.results.iter().map(|r| r.affected_rows).sum()
    }

    /// Get all failed results
    pub fn failed_results(&self) -> Vec<&BatchResult> {
        self.results.iter().filter(|r| r.is_failed()).collect()
    }

    /// Get all successful results
    pub fn successful_results(&self) -> Vec<&BatchResult> {
        self.results.iter().filter(|r| r.is_success()).collect()
    }

    /// Get the number of statements
    pub fn statement_count(&self) -> usize {
        self.results.len()
    }
}

/// Batch query executor for running multiple SQL statements
#[derive(Debug, Clone)]
pub struct BatchExecutor {
    options: BatchOptions,
}

impl BatchExecutor {
    /// Create a new batch executor with the given options
    pub fn new(options: BatchOptions) -> Self {
        Self { options }
    }

    /// Create a batch executor with default options
    pub fn with_defaults() -> Self {
        Self::new(BatchOptions::default())
    }

    /// Get the current options
    pub fn options(&self) -> &BatchOptions {
        &self.options
    }

    /// Execute a batch of SQL statements
    ///
    /// # Arguments
    /// * `conn` - Database connection to use
    /// * `statements` - Vector of SQL statements to execute
    ///
    /// # Returns
    /// A `BatchExecutionResult` containing results for each statement
    pub async fn execute(
        &self,
        conn: &Arc<dyn Connection>,
        statements: Vec<String>,
    ) -> Result<BatchExecutionResult> {
        let batch_start = Instant::now();

        if statements.is_empty() {
            return Ok(BatchExecutionResult::new(
                vec![],
                Duration::ZERO,
                false,
                false,
            ));
        }

        match self.options.mode {
            ExecutionMode::Sequential => {
                self.execute_sequential(conn, statements, batch_start).await
            }
            ExecutionMode::Parallel => self.execute_parallel(conn, statements, batch_start).await,
        }
    }

    /// Execute statements sequentially
    async fn execute_sequential(
        &self,
        conn: &Arc<dyn Connection>,
        statements: Vec<String>,
        batch_start: Instant,
    ) -> Result<BatchExecutionResult> {
        let mut results = Vec::with_capacity(statements.len());
        let mut transaction = None;
        let mut rolled_back = false;
        let mut should_stop = false;

        // Start transaction if requested
        if self.options.transaction {
            transaction = Some(conn.begin_transaction().await?);
        }

        for (index, sql) in statements.into_iter().enumerate() {
            if should_stop {
                results.push(BatchResult::skipped(index, sql));
                continue;
            }

            let result = self
                .execute_single(conn, transaction.as_ref(), index, sql)
                .await;

            if result.is_failed() && self.options.stop_on_error {
                should_stop = true;

                // Rollback transaction on error
                if let Some(tx) = transaction.take() {
                    let _ = tx.rollback().await;
                    rolled_back = true;
                }
            }

            results.push(result);
        }

        // Commit transaction if no errors
        if let Some(tx) = transaction {
            tx.commit().await?;
        }

        let total_time = batch_start.elapsed();
        Ok(BatchExecutionResult::new(
            results,
            total_time,
            self.options.transaction,
            rolled_back,
        ))
    }

    /// Execute statements in parallel
    async fn execute_parallel(
        &self,
        conn: &Arc<dyn Connection>,
        statements: Vec<String>,
        batch_start: Instant,
    ) -> Result<BatchExecutionResult> {
        // Parallel execution doesn't support transactions
        if self.options.transaction {
            tracing::warn!(
                "Transaction mode is not supported with parallel execution, falling back to sequential"
            );
            return self.execute_sequential(conn, statements, batch_start).await;
        }

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.options.max_parallelism));
        let mut handles = Vec::with_capacity(statements.len());
        let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

        for (index, sql) in statements.into_iter().enumerate() {
            let conn = conn.clone();
            let semaphore = semaphore.clone();
            let stop_flag = stop_flag.clone();
            let stop_on_error = self.options.stop_on_error;

            let handle = tokio::spawn(async move {
                // Check if we should skip due to a previous error
                if stop_on_error && stop_flag.load(std::sync::atomic::Ordering::Acquire) {
                    return BatchResult::skipped(index, sql);
                }

                // Acquire semaphore permit
                let _permit = semaphore.acquire().await;

                // Double-check stop flag after acquiring permit
                if stop_on_error && stop_flag.load(std::sync::atomic::Ordering::Acquire) {
                    return BatchResult::skipped(index, sql);
                }

                let start = Instant::now();
                let result = execute_single_statement(&conn, &sql).await;
                let duration = start.elapsed();

                match result {
                    Ok((query_result, affected_rows)) => {
                        if let Some(qr) = query_result {
                            BatchResult::success_query(index, sql, qr, duration)
                        } else {
                            BatchResult::success_statement(index, sql, affected_rows, duration)
                        }
                    }
                    Err(e) => {
                        if stop_on_error {
                            stop_flag.store(true, std::sync::atomic::Ordering::Release);
                        }
                        BatchResult::failed(index, sql, e.into(), duration)
                    }
                }
            });

            handles.push(handle);
        }

        // Collect results
        let mut results: Vec<BatchResult> = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => {
                    // Task panicked or was cancelled
                    results.push(BatchResult::failed(
                        results.len(),
                        String::new(),
                        StatementError::new(format!("Task error: {}", e)),
                        Duration::ZERO,
                    ));
                }
            }
        }

        // Sort results by index to maintain original order
        results.sort_by_key(|r| r.index);

        let total_time = batch_start.elapsed();
        Ok(BatchExecutionResult::new(results, total_time, false, false))
    }

    /// Execute a single statement
    async fn execute_single(
        &self,
        conn: &Arc<dyn Connection>,
        tx: Option<&Box<dyn zqlz_core::Transaction>>,
        index: usize,
        sql: String,
    ) -> BatchResult {
        let start = Instant::now();

        let result = if let Some(tx) = tx {
            execute_single_in_transaction(tx, &sql).await
        } else {
            execute_single_statement(conn, &sql).await
        };

        let duration = start.elapsed();

        match result {
            Ok((query_result, affected_rows)) => {
                if let Some(qr) = query_result {
                    BatchResult::success_query(index, sql, qr, duration)
                } else {
                    BatchResult::success_statement(index, sql, affected_rows, duration)
                }
            }
            Err(e) => BatchResult::failed(index, sql, e.into(), duration),
        }
    }
}

impl Default for BatchExecutor {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Execute a single statement on a connection
async fn execute_single_statement(
    conn: &Arc<dyn Connection>,
    sql: &str,
) -> Result<(Option<QueryResult>, u64)> {
    let trimmed = sql.trim().to_uppercase();
    let is_query = trimmed.starts_with("SELECT")
        || trimmed.starts_with("WITH")
        || trimmed.starts_with("SHOW")
        || trimmed.starts_with("DESCRIBE")
        || trimmed.starts_with("EXPLAIN");

    if is_query {
        let result = conn.query(sql, &[]).await?;
        Ok((Some(result), 0))
    } else {
        let result = conn.execute(sql, &[]).await?;
        Ok((None, result.affected_rows))
    }
}

/// Execute a single statement within a transaction
async fn execute_single_in_transaction(
    tx: &Box<dyn zqlz_core::Transaction>,
    sql: &str,
) -> Result<(Option<QueryResult>, u64)> {
    let trimmed = sql.trim().to_uppercase();
    let is_query = trimmed.starts_with("SELECT")
        || trimmed.starts_with("WITH")
        || trimmed.starts_with("SHOW")
        || trimmed.starts_with("DESCRIBE")
        || trimmed.starts_with("EXPLAIN");

    if is_query {
        let result = tx.query(sql, &[]).await?;
        Ok((Some(result), 0))
    } else {
        let result = tx.execute(sql, &[]).await?;
        Ok((None, result.affected_rows))
    }
}

/// Split a multi-statement SQL string into individual statements
///
/// This is a simple implementation that splits on semicolons while respecting
/// string literals and comments. For complex SQL dialects, consider using
/// a proper SQL parser.
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut string_char = '"';
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let c = chars[i];
        let next = if i + 1 < len {
            Some(chars[i + 1])
        } else {
            None
        };

        // Handle line comments
        if !in_string && !in_block_comment && c == '-' && next == Some('-') {
            in_line_comment = true;
            current.push(c);
            i += 1;
            continue;
        }

        if in_line_comment {
            current.push(c);
            if c == '\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        // Handle block comments
        if !in_string && !in_line_comment && c == '/' && next == Some('*') {
            in_block_comment = true;
            current.push(c);
            i += 1;
            continue;
        }

        if in_block_comment {
            current.push(c);
            if c == '*' && next == Some('/') {
                current.push(chars[i + 1]);
                in_block_comment = false;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }

        // Handle string literals
        if !in_string && (c == '\'' || c == '"') {
            in_string = true;
            string_char = c;
            current.push(c);
            i += 1;
            continue;
        }

        if in_string {
            current.push(c);
            if c == string_char {
                // Check for escaped quote (doubled)
                if next == Some(string_char) {
                    current.push(chars[i + 1]);
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            i += 1;
            continue;
        }

        // Handle statement separator
        if c == ';' {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_string());
            }
            current.clear();
            i += 1;
            continue;
        }

        current.push(c);
        i += 1;
    }

    // Don't forget the last statement (without trailing semicolon)
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}
