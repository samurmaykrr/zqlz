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

mod create;
mod delete;
pub(in crate::main_view) mod design;
mod duplicate;
mod empty;
mod import_export;
mod open;
mod rename;
