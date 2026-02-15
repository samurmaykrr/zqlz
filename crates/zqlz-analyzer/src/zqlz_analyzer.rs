//! ZQLZ Analyzer - Query analysis and EXPLAIN plan parsing
//!
//! This crate provides functionality for:
//! - Parsing EXPLAIN output from PostgreSQL, MySQL, and SQLite
//! - Query plan analysis and optimization suggestions
//! - Performance metrics extraction from query plans

pub mod explain;
pub mod suggestions;

pub use explain::*;
pub use suggestions::*;
