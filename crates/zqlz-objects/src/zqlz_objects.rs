//! ZQLZ Objects - Database object management
//!
//! This crate provides functionality for managing database objects like:
//! - Stored procedures and functions
//! - Views
//! - Triggers
//! - User-defined types

pub mod function;
pub mod procedure;
pub mod trigger;
pub mod view;

pub use function::*;
pub use procedure::*;
pub use trigger::*;
pub use view::*;
