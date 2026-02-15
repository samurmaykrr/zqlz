//! Stored procedure execution and management
//!
//! This module provides functionality for executing stored procedures across
//! different database systems with support for IN, OUT, and INOUT parameters.

mod executor;

#[cfg(test)]
mod tests;

pub use executor::*;
