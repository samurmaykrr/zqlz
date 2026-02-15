//! ClickHouse driver for ZQLZ Database IDE
//!
//! ClickHouse is a column-oriented database management system for online
//! analytical processing (OLAP). It is designed for high-performance
//! analytics on large datasets.

mod driver;
#[cfg(test)]
mod driver_tests;
mod schema;

pub use driver::*;
