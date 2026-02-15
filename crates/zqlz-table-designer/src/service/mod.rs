//! Service layer for table designer
//!
//! Provides DDL generation and table loading functionality.

mod ddl_generator;
mod table_loader;

pub use ddl_generator::{DdlGenerator, fk_action_to_sql};
pub use table_loader::TableLoader;
