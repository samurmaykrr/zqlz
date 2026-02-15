//! MS SQL Server driver for ZQLZ Database IDE
//!
//! This crate provides the core driver implementation for Microsoft SQL Server,
//! supporting T-SQL dialect and SQL Server-specific features.

mod connection;
mod dialect;
mod driver;
mod schema;

#[cfg(test)]
mod connection_tests;
#[cfg(test)]
mod dialect_tests;
#[cfg(test)]
mod driver_tests;
#[cfg(test)]
mod schema_tests;

pub use connection::{MssqlConnection, MssqlConnectionError};
pub use dialect::MssqlDialect;
pub use driver::{MssqlDriver, mssql_dialect};
