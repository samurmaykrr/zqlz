//! SQLite database driver implementation

mod connection;
mod dialect;
mod driver;
mod explain;
mod schema;

pub use connection::{
    DatabaseFileInfo, ExecuteMultiResult, SqliteConnection, SqliteOpenMode, SqliteOpenOptions,
};
pub use dialect::sqlite_dialect;
pub use driver::SqliteDriver;
pub use explain::{parse_sqlite_explain_result, sqlite_query_result_to_plan_text};
