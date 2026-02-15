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

pub(super) use copy_names::*;
pub(super) use create::*;
pub(super) use delete::*;
pub(super) use design::*;
pub(super) use duplicate::*;
pub(super) use empty::*;
pub(super) use import_export::*;
pub(super) use open::*;
pub(super) use rename::*;
