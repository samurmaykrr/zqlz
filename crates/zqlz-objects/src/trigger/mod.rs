//! Trigger creation and management
//!
//! This module provides functionality for creating, modifying, and dropping
//! database triggers across different database systems.

mod manager;

#[cfg(test)]
mod tests;

pub use manager::*;
