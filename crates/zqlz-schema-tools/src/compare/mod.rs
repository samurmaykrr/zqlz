//! Schema comparison module
//!
//! Provides tools for comparing database schemas and generating diffs.

mod comparator;
mod diff;

#[cfg(test)]
mod tests;

pub use comparator::*;
pub use diff::*;
