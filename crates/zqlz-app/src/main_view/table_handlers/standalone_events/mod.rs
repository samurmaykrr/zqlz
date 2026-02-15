//! Standalone event handler functions for table viewer events.
//!
//! These functions are called from event subscription closures in the table viewer.
//! They receive the window and context from the closure and handle various table operations
//! including query operations, cell editing, row operations, lifecycle events,
//! refresh operations, pagination, SQL generation, and foreign key loading.

mod cell;
mod lifecycle;
mod misc;
mod pagination_basic;
mod pagination_helpers;
mod pagination_load_more;
mod query;
mod refresh;
mod row;

pub(in crate::main_view) use cell::*;
pub(in crate::main_view) use lifecycle::*;
pub(in crate::main_view) use misc::*;
pub(in crate::main_view) use pagination_basic::*;
pub(in crate::main_view) use pagination_load_more::*;
pub(in crate::main_view) use query::*;
pub(in crate::main_view) use refresh::*;
pub(in crate::main_view) use row::*;

// pagination_helpers is not re-exported as it contains internal helper functions
