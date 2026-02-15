//! Row Level Security (RLS) management module
//!
//! Provides functionality for managing PostgreSQL Row Level Security policies.
//! RLS allows fine-grained control over which rows are visible or modifiable
//! by specific users or roles.

mod service;

#[cfg(test)]
mod tests;

pub use service::*;
