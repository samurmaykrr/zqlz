//! MySQL driver implementation

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use zqlz_core::{
    Connection, ConnectionConfig, ConnectionField, ConnectionFieldSchema, DatabaseDriver,
    DialectInfo, DriverCapabilities, Result, ZqlzError,
    security::{SshAuthMethod, SshTunnelConfig, TlsConfig, TlsMode},
};

use crate::{MySqlConnectOptions, MySqlConnection, MysqlSshTunnel, MysqlTlsConnector};

/// MySQL database driver
pub struct MySqlDriver;

impl MySqlDriver {
    /// Create a new MySQL driver instance
    pub fn new() -> Self {
        tracing::debug!("MySQL driver initialized");
        Self
    }
}

impl Default for MySqlDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for MySqlDriver {
    fn name(&self) -> &'static str {
        "mysql"
    }

    fn display_name(&self) -> &'static str {
        "MySQL"
    }

    fn default_port(&self) -> Option<u16> {
        Some(3306)
    }

    fn dialect_info(&self) -> DialectInfo {
        crate::mysql_dialect()
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            supports_transactions: true,
            supports_savepoints: true,
            supports_prepared_statements: true,
            supports_multiple_statements: true,
            supports_returning: false, // MySQL 8.0.21+ has some support, but limited
            supports_upsert: true,     // ON DUPLICATE KEY UPDATE
            supports_window_functions: true, // MySQL 8.0+
            supports_cte: true,        // MySQL 8.0+
            supports_json: true,       // MySQL 5.7+
            supports_full_text_search: true,
            supports_stored_procedures: true,
            supports_schemas: false, // MySQL uses databases instead of schemas
            supports_multiple_databases: true,
            supports_streaming: true,
            supports_cancellation: true,
            supports_explain: true,
            supports_foreign_keys: true,
            supports_views: true,
            supports_triggers: true,
            supports_ssl: true,
            max_identifier_length: Some(64),
            max_parameters: Some(65535),
        }
    }

    #[tracing::instrument(skip(self, config), fields(host = config.get_string("host").as_deref(), database = config.get_string("database").as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        let host = config
            .get_string("host")
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 3306 };
        let database = config.get_string("database");
        let user = config
            .get_string("user")
            .or_else(|| config.get_string("username"));
        let password = config.get_string("password");

        let tls_config = build_tls_config(config)?;
        let ssl_opts = MysqlTlsConnector::build(&tls_config).map_err(|error| {
            ZqlzError::Connection(format!("Failed to configure MySQL TLS: {}", error))
        })?;
        let ssh_tunnel = build_ssh_tunnel(config, &host, port)?;
        let (connect_host, connect_port) = if let Some(tunnel) = ssh_tunnel.as_ref() {
            ("127.0.0.1".to_string(), tunnel.local_port())
        } else {
            (host.clone(), port)
        };

        let conn = MySqlConnection::connect(MySqlConnectOptions {
            host: connect_host,
            port: connect_port,
            database: database.clone(),
            user,
            password,
            ssl_opts,
            ssh_tunnel,
        })
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to connect to MySQL database");
            ZqlzError::Connection(format!("Failed to connect to MySQL database: {}", e))
        })?;

        tracing::info!(host = %host, port = %port, database = ?database, "MySQL connection created");
        Ok(Arc::new(conn))
    }

    #[tracing::instrument(skip(self, config))]
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()> {
        tracing::debug!("testing MySQL connection");
        let conn = self.connect(config).await?;
        conn.query("SELECT 1", &[]).await?;
        Ok(())
    }

    fn build_connection_string(&self, config: &ConnectionConfig) -> String {
        let host = config
            .get_string("host")
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 3306 };
        let database = config.get_string("database");
        let user = config
            .get_string("user")
            .or_else(|| config.get_string("username"));

        let mut conn_str = String::from("mysql://");

        if let Some(u) = user {
            conn_str.push_str(&u);
            if let Some(p) = config.get_string("password") {
                conn_str.push(':');
                conn_str.push_str(&p);
            }
            conn_str.push('@');
        }

        conn_str.push_str(&format!("{}:{}", host, port));

        if let Some(db) = database {
            conn_str.push('/');
            conn_str.push_str(&db);
        }

        conn_str
    }

    fn connection_string_help(&self) -> &'static str {
        "mysql://[user[:password]@]host[:port][/database]"
    }

    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        use zqlz_core::ConnectionFieldOption;

        ConnectionFieldSchema {
            title: Cow::Borrowed("MySQL Connection"),
            fields: vec![
                ConnectionField::text("host", "Host")
                    .placeholder("localhost")
                    .default_value("localhost")
                    .required()
                    .width(0.7)
                    .row_group(1),
                ConnectionField::number("port", "Port")
                    .placeholder("3306")
                    .default_value("3306")
                    .width(0.3)
                    .row_group(1),
                ConnectionField::text("database", "Database").placeholder("mydb"),
                ConnectionField::text("user", "Username")
                    .placeholder("root")
                    .default_value("root")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::password("password", "Password")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::select(
                    "ssl_mode",
                    "SSL Mode",
                    vec![
                        ConnectionFieldOption::new("DISABLED", "Disabled"),
                        ConnectionFieldOption::new("PREFERRED", "Preferred"),
                        ConnectionFieldOption::new("REQUIRED", "Required"),
                        ConnectionFieldOption::new("VERIFY_CA", "Verify CA"),
                        ConnectionFieldOption::new("VERIFY_IDENTITY", "Verify Identity"),
                    ],
                )
                .default_value("DISABLED")
                .tab("ssl"),
                ConnectionField::file_path("ssl_ca_cert", "CA Certificate")
                    .placeholder("/path/to/ca-cert.pem")
                    .with_extensions(vec!["pem", "crt", "cer"])
                    .tab("ssl"),
                ConnectionField::boolean("ssh_enabled", "Use SSH Tunnel")
                    .default_value("false")
                    .tab("ssh"),
                ConnectionField::text("ssh_host", "SSH Host")
                    .placeholder("bastion.example.com")
                    .width(0.7)
                    .row_group(20)
                    .tab("ssh"),
                ConnectionField::number("ssh_port", "SSH Port")
                    .placeholder("22")
                    .default_value("22")
                    .width(0.3)
                    .row_group(20)
                    .tab("ssh"),
                ConnectionField::text("ssh_username", "SSH Username")
                    .width(0.5)
                    .row_group(21)
                    .tab("ssh"),
                ConnectionField::select(
                    "ssh_auth_method",
                    "SSH Auth Method",
                    vec![
                        ConnectionFieldOption::new("password", "Password"),
                        ConnectionFieldOption::new("private_key", "Private Key"),
                        ConnectionFieldOption::new("agent", "Agent"),
                    ],
                )
                .default_value("password")
                .width(0.5)
                .row_group(21)
                .tab("ssh"),
                ConnectionField::password("ssh_password", "SSH Password").tab("ssh"),
                ConnectionField::file_path("ssh_private_key", "SSH Private Key")
                    .placeholder("~/.ssh/id_rsa")
                    .with_extensions(vec!["pem", "key"])
                    .width(0.5)
                    .row_group(22)
                    .tab("ssh"),
                ConnectionField::password("ssh_private_key_passphrase", "SSH Key Passphrase")
                    .width(0.5)
                    .row_group(22)
                    .tab("ssh"),
                ConnectionField::number("ssh_timeout_seconds", "SSH Timeout (seconds)")
                    .default_value("30")
                    .width(0.5)
                    .row_group(23)
                    .tab("ssh"),
                ConnectionField::number("ssh_keepalive_seconds", "SSH Keepalive (seconds)")
                    .default_value("0")
                    .width(0.5)
                    .row_group(23)
                    .tab("ssh"),
            ],
        }
    }
}

fn build_tls_config(config: &ConnectionConfig) -> Result<TlsConfig> {
    let mode = match config
        .get_string("ssl_mode")
        .unwrap_or_else(|| "DISABLED".to_string())
        .to_ascii_uppercase()
        .as_str()
    {
        "DISABLED" => TlsMode::Disable,
        "PREFERRED" => TlsMode::Prefer,
        "REQUIRED" => TlsMode::Require,
        "VERIFY_CA" => TlsMode::VerifyCa,
        "VERIFY_IDENTITY" => TlsMode::VerifyFull,
        value => {
            return Err(ZqlzError::Configuration(format!(
                "Invalid MySQL SSL mode: {}",
                value
            )));
        }
    };

    let mut tls_config = TlsConfig::new(mode);
    if matches!(mode, TlsMode::Require | TlsMode::Prefer) {
        tls_config = tls_config.verify_server(false);
    }
    if let Some(ca_cert) = config
        .get_string("ssl_ca_cert")
        .filter(|value| !value.is_empty())
    {
        tls_config = tls_config.ca_cert(ca_cert);
    }
    tls_config.validate()?;
    Ok(tls_config)
}

fn build_ssh_tunnel(
    config: &ConnectionConfig,
    remote_host: &str,
    remote_port: u16,
) -> Result<Option<MysqlSshTunnel>> {
    if !parse_bool(config, "ssh_enabled", false) {
        return Ok(None);
    }

    let ssh_config = build_ssh_config(config)?;
    MysqlSshTunnel::new(&ssh_config, remote_host, remote_port)
        .map(Some)
        .map_err(|error| {
            ZqlzError::Connection(format!("Failed to establish SSH tunnel: {}", error))
        })
}

fn build_ssh_config(config: &ConnectionConfig) -> Result<SshTunnelConfig> {
    let host = required_param(config, "ssh_host")?;
    let username = required_param(config, "ssh_username")?;
    let port = parse_optional_u16(config, "ssh_port")?.unwrap_or(22);
    let timeout_seconds = parse_optional_u64(config, "ssh_timeout_seconds")?.unwrap_or(30) as u32;
    let keepalive_seconds =
        parse_optional_u64(config, "ssh_keepalive_seconds")?.unwrap_or(0) as u32;

    let auth = match config
        .get_string("ssh_auth_method")
        .unwrap_or_else(|| "password".to_string())
        .as_str()
    {
        "password" => SshAuthMethod::password(required_param(config, "ssh_password")?),
        "private_key" => {
            let key = required_param(config, "ssh_private_key")?;
            let passphrase = config
                .get_string("ssh_private_key_passphrase")
                .filter(|value| !value.is_empty());
            SshAuthMethod::PrivateKey {
                path: key.into(),
                passphrase,
            }
        }
        "agent" => SshAuthMethod::agent(),
        value => {
            return Err(ZqlzError::Configuration(format!(
                "Invalid SSH auth method: {}",
                value
            )));
        }
    };

    let ssh_config = SshTunnelConfig {
        host,
        port,
        username,
        auth,
        timeout_seconds,
        keepalive_seconds,
    };
    ssh_config.validate()?;
    Ok(ssh_config)
}

fn required_param(config: &ConnectionConfig, key: &str) -> Result<String> {
    config
        .get_string(key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ZqlzError::Configuration(format!("{} is required", key)))
}

fn parse_bool(config: &ConnectionConfig, key: &str, default: bool) -> bool {
    config
        .get_string(key)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn parse_optional_u16(config: &ConnectionConfig, key: &str) -> Result<Option<u16>> {
    config
        .get_string(key)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<u16>()
                .map_err(|_| ZqlzError::Configuration(format!("{} must be a whole number", key)))
        })
        .transpose()
}

fn parse_optional_u64(config: &ConnectionConfig, key: &str) -> Result<Option<u64>> {
    config
        .get_string(key)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| ZqlzError::Configuration(format!("{} must be a whole number", key)))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_core::DatabaseDriver;

    #[test]
    fn mysql_schema_has_ssl_and_ssh_fields() {
        let schema = MySqlDriver::new().connection_field_schema();
        let field = |id: &str| schema.fields.iter().find(|field| field.id == id).unwrap();

        assert_eq!(field("ssl_mode").tab.as_deref(), Some("ssl"));
        assert_eq!(field("ssl_mode").default_value.as_deref(), Some("DISABLED"));
        assert_eq!(field("ssl_ca_cert").tab.as_deref(), Some("ssl"));
        assert_eq!(field("ssh_enabled").tab.as_deref(), Some("ssh"));
        assert_eq!(
            field("ssh_auth_method").default_value.as_deref(),
            Some("password")
        );
    }

    #[test]
    fn mysql_verify_ca_requires_ca() {
        let config = ConnectionConfig::new("mysql", "test").with_param("ssl_mode", "VERIFY_CA");

        let error = build_tls_config(&config).unwrap_err();

        assert!(error.to_string().contains("CA certificate"));
    }

    #[test]
    fn mysql_ssh_enabled_requires_host_and_user() {
        let config = ConnectionConfig::new("mysql", "test").with_param("ssh_enabled", "true");

        let error = build_ssh_config(&config).unwrap_err();

        assert!(error.to_string().contains("ssh_host is required"));
    }
}
