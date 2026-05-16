//! MySQL/MariaDB driver implementation

mod connection;
mod dialect;
mod driver;
mod explain;
mod schema;
mod ssh;
mod tls;

pub use connection::{MySqlConnectOptions, MySqlConnection};
pub use dialect::mysql_dialect;
pub use driver::MySqlDriver;
pub use explain::{mysql_query_result_to_plan_text, parse_mysql_explain_result};
pub use ssh::{MysqlSshTunnel, MysqlSshTunnelError};
pub use tls::{MysqlTlsConnector, MysqlTlsError, build_ssl_params, tls_mode_to_ssl_mode};
