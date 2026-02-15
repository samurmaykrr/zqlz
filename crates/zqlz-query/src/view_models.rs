//! View models for query execution results
//!
//! These are DTOs designed for UI consumption.

use serde::{Deserialize, Serialize};
use zqlz_analyzer::QueryAnalysis;
use zqlz_core::QueryResult;

/// Single statement execution result
#[derive(Debug, Clone)]
pub struct StatementResult {
    pub sql: String,
    pub duration_ms: u64,
    pub result: Option<QueryResult>,
    pub error: Option<String>,
    pub affected_rows: u64,
}

/// Query execution result for UI consumption (supports multiple statements)
#[derive(Debug, Clone)]
pub struct QueryExecution {
    pub sql: String,
    pub duration_ms: u64,
    pub statements: Vec<StatementResult>,
}

/// Statement execution result for UI consumption (legacy single statement)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementExecution {
    pub sql: String,
    pub duration_ms: u64,
    pub affected_rows: usize,
    pub success: bool,
    pub error: Option<String>,
}

/// EXPLAIN query result
#[derive(Debug, Clone)]
pub struct ExplainResult {
    /// The original SQL that was explained
    pub sql: String,
    /// Execution time of the EXPLAIN itself
    pub duration_ms: u64,
    /// The raw EXPLAIN output as a table (for Op tab)
    pub raw_output: Option<QueryResult>,
    /// The EXPLAIN QUERY PLAN output (for Plan tab - SQLite)
    pub query_plan: Option<QueryResult>,
    /// Parsed and analyzed query plan with optimization suggestions
    pub analyzed_plan: Option<QueryAnalysis>,
    /// Error message if EXPLAIN failed
    pub error: Option<String>,
    /// Connection name for display
    pub connection_name: Option<String>,
    /// Database name for display
    pub database_name: Option<String>,
}
