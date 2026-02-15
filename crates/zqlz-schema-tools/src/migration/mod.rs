//! Migration generation module
//!
//! Provides tools for generating SQL migration scripts from schema diffs.

mod generator;

#[cfg(test)]
mod tests;

pub use generator::*;
