//! MySQL/MariaDB driver implementation

mod connection;
mod dialect;
mod driver;
mod schema;
mod ssh;
mod tls;

pub use connection::MySqlConnection;
pub use dialect::mysql_dialect;
pub use driver::MySqlDriver;
pub use ssh::{MysqlSshTunnel, MysqlSshTunnelError};
pub use tls::{MysqlTlsConnector, MysqlTlsError, build_ssl_params, tls_mode_to_ssl_mode};
