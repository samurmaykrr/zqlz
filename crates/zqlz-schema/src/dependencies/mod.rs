//! Schema Dependencies Analyzer
//!
//! This module provides tools for analyzing dependencies between database objects
//! such as views, functions, triggers, and their underlying tables.

mod analyzer;

#[cfg(test)]
mod tests;

pub use analyzer::{
    AnalyzerConfig, Dependencies, DependencyAnalyzer, DependencyGraph, ObjectRef,
    extract_table_references,
};
