//! ZQLZ Schema Tools - Schema comparison, diff, and migration generation
//!
//! This crate provides functionality for:
//! - Comparing database schemas
//! - Generating schema diffs
//! - Creating migration scripts
//! - Cross-database schema synchronization

pub mod compare;
pub mod cross_sync;
pub mod migration;

pub use compare::*;
pub use cross_sync::*;
pub use migration::*;
