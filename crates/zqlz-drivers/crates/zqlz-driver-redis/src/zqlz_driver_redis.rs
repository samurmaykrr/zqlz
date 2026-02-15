//! Redis driver for ZQLZ Database IDE
//!
//! Redis is an in-memory key-value data store, used as a database, cache,
//! and message broker. This driver provides basic connection management
//! and command execution for Redis instances.

mod driver;
#[cfg(test)]
mod driver_tests;
pub mod keys;
#[cfg(test)]
mod keys_tests;
mod schema;

pub use driver::*;
pub use keys::*;
