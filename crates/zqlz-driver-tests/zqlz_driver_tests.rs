//! ZQLZ Database Driver Testing Suite
//!
//! Comprehensive testing framework for database drivers using Pagila/Sakila benchmarks.
//! Tests run parametrically across PostgreSQL, MySQL, SQLite, and Redis to ensure
//! driver consistency and correctness.
//!
//! # Architecture
//!
//! - **Unified parameterized tests**: One test definition runs against multiple drivers using rstest
//! - **Automatic Docker management**: Containers start automatically with testcontainers-rs
//! - **Real-world data**: Pagila (PostgreSQL) and Sakila (MySQL/SQLite) sample databases
//! - **Cross-database compatibility**: Focus on common RDBMS features
//!
//! # Test Categories
//!
//! - Connection tests (basic, SSL/TLS, pooling)
//! - CRUD operations (INSERT, SELECT, UPDATE, DELETE, UPSERT)
//! - Transactions (basic, savepoints, isolation levels)
//! - Queries (JOINs, subqueries, CTEs, window functions, set operations)
//! - Parameters (prepared statements, parameter binding)
//! - Data types (numeric, string, date/time, boolean, NULL, JSON, binary)
//! - Schema introspection (databases, tables, columns, keys, indexes, views)
//! - Error handling (syntax, constraints, types, connections)
//! - Edge cases (empty results, large datasets, special characters, boundaries)
//! - Performance (query execution, concurrent operations)
//! - Redis operations (key-value, data structures)
//!
//! # Usage
//!
//! ```bash
//! # Run all tests (Docker containers start automatically)
//! cargo test -p zqlz-driver-tests --all-features
//!
//! # Run specific test module
//! cargo test -p zqlz-driver-tests connection_tests
//!
//! # Use manually managed containers (optional)
//! export ZQLZ_TEST_MANUAL_CONTAINERS=1
//! ./manage-test-env.sh up
//! cargo test -p zqlz-driver-tests --all-features
//! ./manage-test-env.sh down
//! ```

#![warn(missing_docs)]
#![warn(clippy::all)]
#![allow(clippy::needless_doctest_main)]

// Core infrastructure
pub mod fixtures;
pub mod test_containers;

#[cfg(test)]
pub mod connection_tests;

#[cfg(test)]
pub mod select_tests;

#[cfg(test)]
pub mod transaction_tests;

#[cfg(test)]
pub mod insert_tests;

#[cfg(test)]
pub mod update_tests;

#[cfg(test)]
pub mod delete_tests;

#[cfg(test)]
pub mod upsert_tests;

#[cfg(test)]
pub mod parameter_tests;

#[cfg(test)]
pub mod pool_tests;

#[cfg(test)]
pub mod schema_tests;

#[cfg(test)]
pub mod query_tests;

#[cfg(test)]
pub mod datatype_tests;

#[cfg(test)]
pub mod error_tests;

#[cfg(test)]
pub mod set_operations_tests;

#[cfg(test)]
pub mod edge_case_empty_results_tests;

#[cfg(test)]
pub mod datatype_json_tests;

#[cfg(test)]
pub mod ssl_tls_tests;

#[cfg(test)]
pub mod redis_tests;

#[cfg(test)]
pub mod edge_case_large_data_tests;

#[cfg(test)]
pub mod edge_case_special_chars_tests;

#[cfg(test)]
pub mod edge_case_boundary_tests;

#[cfg(test)]
pub mod explain_tests;

#[cfg(test)]
pub mod performance_tests;

#[cfg(test)]
mod tests {
    #[test]
    fn test_crate_compiles() {
        // Placeholder test to ensure the crate builds
        assert!(true);
    }
}
