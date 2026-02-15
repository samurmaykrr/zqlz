//! Row operations module
//!
//! Provides row manipulation operations for the table viewer.
//!
//! ## Modules
//!
//! - `duplicate` - Row duplication operations
//! - `multi_row` - Bulk multi-row operations (set value, delete, duplicate)

mod duplicate;
mod multi_row;

pub use duplicate::{DuplicateOptions, DuplicatedRow, duplicate_row, duplicate_rows};
pub use multi_row::{
    MultiRowOperation, Operation, OperationResult, generate_bulk_delete_sql,
    generate_bulk_update_sql,
};
