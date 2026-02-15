//! Role management module
//!
//! Provides functionality for creating, altering, and dropping database roles
//! and managing permissions across different database dialects.

mod service;

#[cfg(test)]
mod tests;

pub use service::*;
