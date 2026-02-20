//! Code folding detection for SQL text.
//!
//! This module provides detection of foldable regions in SQL:
//! - BEGIN...END blocks
//! - Multi-line comments
//! - Function/procedure definitions
//! - CASE...END expressions
//! - Parenthesized blocks

pub mod detector;

pub use detector::{detect_folds, FoldKind, FoldRegion, FoldingDetector};
