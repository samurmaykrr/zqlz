//! User-defined function management
//!
//! This module provides functionality for creating, modifying, and dropping
//! user-defined functions across different database systems.

mod manager;

#[cfg(test)]
mod tests;

pub use manager::*;
