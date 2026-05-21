//! Code folding detection for query text.
//!
//! This module provides detection of foldable regions in query text:
//! - BEGIN...END blocks
//! - Driver-configured multi-line comments
//! - Function/procedure definitions
//! - CASE...END expressions
//! - Parenthesized blocks

pub mod detector;

pub use detector::{
    FoldKind, FoldRegion, FoldingDetector, detect_folds, detect_folds_in_range,
    detect_folds_in_range_with_block_comments, detect_folds_in_range_with_rules,
    detect_folds_with_block_comments, detect_folds_with_rules,
};
