//! View creation and management
//!
//! This module provides functionality for creating, modifying, and dropping
//! database views and materialized views across different database systems.

mod manager;

#[cfg(test)]
mod tests;

pub use manager::*;
