//! Transaction-related types and traits
//!
//! This module provides additional transaction functionality beyond the
//! base `Transaction` trait defined in the connection module.

mod savepoint;

pub use savepoint::*;
