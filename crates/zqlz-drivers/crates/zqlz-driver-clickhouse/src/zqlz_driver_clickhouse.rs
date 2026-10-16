//! ClickHouse driver for ZQLZ Database IDE
//!
//! ClickHouse is a column-oriented database management system for online
//! analytical processing (OLAP). It is designed for high-performance
//! analytics on large datasets.

mod driver;
mod explain;
#[cfg(test)]
mod driver_tests;
mod schema;
#[cfg(test)]
mod schema_tests;

pub use driver::*;
pub use explain::parse_clickhouse_explain_result;
