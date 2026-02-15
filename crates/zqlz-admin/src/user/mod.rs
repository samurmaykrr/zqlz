//! User management module
//!
//! Provides functionality for creating, altering, and dropping database users
//! across different database dialects.

mod service;

#[cfg(test)]
mod tests;

pub use service::*;
