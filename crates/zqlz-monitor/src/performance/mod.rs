//! Performance metrics module
//!
//! Provides functionality for collecting and analyzing database performance metrics
//! including query statistics, cache statistics, and connection usage.

mod collector;

#[cfg(test)]
mod tests;

pub use collector::*;
