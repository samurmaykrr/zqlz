//! PostgreSQL driver implementation

mod connection;
mod dialect;
mod driver;
mod explain;
mod schema;
mod ssh;
mod tls;

pub use connection::{PostgresConnectOptions, PostgresConnection};
pub use dialect::postgres_dialect;
pub use driver::PostgresDriver;
pub use explain::parse_postgres_explain_result;
pub use ssh::{PostgresSshTunnel, SshTunnelError};
pub use tls::{PostgresTlsConnector, TlsError, build_tls_params, tls_mode_to_sslmode};
