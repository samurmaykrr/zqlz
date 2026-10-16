//! Service layer for table designer
//!
//! Provides DDL generation and table loading functionality.

mod ddl_generator;
mod mysql_ddl;
mod mysql_loader;
mod table_loader;

pub use ddl_generator::{DdlGenerator, FK_ACTION_LABELS, fk_action_from_sql, fk_action_to_sql};
pub(crate) use mysql_ddl::mysql_index_part_column;
pub use table_loader::TableLoader;
