//! SQLite database driver implementation

mod connection;
mod dialect;
mod driver;
mod schema;

pub use connection::{
    DatabaseFileInfo, ExecuteMultiResult, SqliteConnection, SqliteOpenMode, SqliteOpenOptions,
};
pub use dialect::sqlite_dialect;
pub use driver::SqliteDriver;
