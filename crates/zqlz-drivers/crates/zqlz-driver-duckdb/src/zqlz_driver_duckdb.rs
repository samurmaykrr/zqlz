//! DuckDB driver for ZQLZ Database IDE
//!
//! DuckDB is an in-process analytical database management system.
//! It's optimized for OLAP workloads and supports running queries
//! directly on Parquet files and other data sources.

mod driver;
#[cfg(test)]
mod driver_tests;
mod schema;

pub use driver::*;
