//! ZQLZ Editor - SQL editor features
//!
//! This crate provides functionality for the SQL editor:
//! - SQL formatting with configurable options
//! - Query bookmarks
//! - Find and replace
//! - Code folding

pub mod bookmarks;
pub mod find_replace;
pub mod folding;
pub mod formatter;

pub use bookmarks::{Bookmark, BookmarkFilter, BookmarkManager, BookmarkStorage};
pub use find_replace::{
    FindError, FindOptions, Match, ReplaceResult, count_matches, find_all, find_first, find_next,
    replace_all, replace_first, replace_next,
};
pub use folding::{FoldKind, FoldRegion, FoldingDetector, detect_folds};
pub use formatter::*;
