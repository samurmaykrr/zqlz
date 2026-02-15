//! MongoDB driver for ZQLZ Database IDE
//!
//! This crate provides MongoDB connectivity for the ZQLZ database IDE.
//! MongoDB is a document-oriented NoSQL database that stores data in
//! flexible, JSON-like documents.
//!
//! # Features
//!
//! - MongoDB connection management
//! - BSON document query support
//! - Collection browsing and introspection
//! - Aggregation pipeline support
//! - Schema inference from document samples
//!
//! # Example
//!
//! ```ignore
//! use zqlz_driver_mongodb::{MongoDbDriver, collections};
//! use zqlz_core::{DatabaseDriver, ConnectionConfig};
//!
//! let driver = MongoDbDriver::new();
//! let config = ConnectionConfig::new("mongodb", "My MongoDB");
//! // config.host = "localhost".into();
//! // config.port = 27017;
//! // let conn = driver.connect(&config).await?;
//! //
//! // // List all databases
//! // let databases = collections::list_databases(&conn).await?;
//! //
//! // // List collections in current database
//! // let collections = collections::list_collections(&conn).await?;
//! ```

pub mod collections;
#[cfg(test)]
mod collections_tests;
mod driver;
#[cfg(test)]
mod driver_tests;

pub use collections::*;
pub use driver::*;
