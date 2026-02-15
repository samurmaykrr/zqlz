//! Server monitoring module
//!
//! Provides functionality for retrieving database server status information
//! across different database dialects.

mod status;

#[cfg(test)]
mod tests;

pub use status::*;
