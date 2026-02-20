//! Find and replace functionality for SQL text.
//!
//! This module provides:
//! - [`find`] - Text search with regex support
//! - [`replace`] - Text replacement with capture group support

pub mod find;
pub mod replace;

pub use find::{count_matches, find_all, find_first, find_next, FindError, FindOptions, Match};
pub use replace::{replace_all, replace_first, replace_next, ReplaceResult};
