//! Batch query execution module
//!
//! This module provides optimized batch execution of multiple SQL statements
//! with configurable options for parallel execution, error handling, and
//! transaction management.

mod executor;
#[cfg(test)]
mod tests;

pub use executor::{
    BatchExecutionResult, BatchExecutor, BatchOptions, BatchResult, ExecutionMode, StatementError,
    StatementStatus, split_statements,
};
