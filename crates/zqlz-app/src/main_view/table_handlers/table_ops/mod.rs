//! Table operation modules.
//!
//! This module contains operations that can be performed on database tables:
//! - Creating new tables
//! - Opening tables
//! - Designing table structure
//! - Deleting tables
//! - Emptying tables (truncate)
//! - Duplicating tables
//! - Renaming tables
//! - Importing and exporting data
//! - Copying table names to clipboard

mod copy_names;
mod create;
mod delete;
mod design;
mod duplicate;
mod empty;
mod import_export;
mod open;
mod rename;

