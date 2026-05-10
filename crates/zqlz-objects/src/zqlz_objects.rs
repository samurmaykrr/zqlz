//! ZQLZ Objects - Database object management
//!
//! This crate provides functionality for managing database objects like:
//! - Stored procedures and functions
//! - Views
//! - Triggers
//! - User-defined types

mod ddl_planner;
pub mod function;
mod object_definition;
pub mod procedure;
pub mod trigger;
pub mod view;

pub use ddl_planner::*;
pub use function::*;
pub use object_definition::*;
pub use procedure::*;
pub use trigger::*;
pub use view::*;
