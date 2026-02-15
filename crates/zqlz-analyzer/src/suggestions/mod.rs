//! Query Analysis Suggestions Module
//!
//! This module provides optimization suggestions by analyzing query execution plans.
//! It identifies common performance issues like missing indexes, full table scans,
//! inefficient joins, and provides actionable recommendations.

mod analyzer;

pub use analyzer::*;
