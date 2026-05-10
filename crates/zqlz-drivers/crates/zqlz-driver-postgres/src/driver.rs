//! PostgreSQL driver implementation

use async_trait::async_trait;
use std::borrow::Cow;
use std::sync::Arc;
use zqlz_core::{
    Connection, ConnectionConfig, ConnectionField, ConnectionFieldSchema, DatabaseDriver,
    DialectInfo, DriverCapabilities, Result, ZqlzError,
    security::{SshAuthMethod, SshTunnelConfig},
};

use crate::{PostgresConnectOptions, PostgresConnection, PostgresSshTunnel};

/// PostgreSQL database driver
pub struct PostgresDriver;

impl PostgresDriver {
    /// Create a new PostgreSQL driver instance
    pub fn new() -> Self {
        tracing::debug!("PostgreSQL driver initialized");
        Self
    }
}

impl Default for PostgresDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for PostgresDriver {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn display_name(&self) -> &'static str {
        "PostgreSQL"
    }

    fn dialect_info(&self) -> DialectInfo {
        crate::postgres_dialect()
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            supports_transactions: true,
            supports_savepoints: true,
            supports_prepared_statements: true,
            supports_multiple_statements: true,
            supports_returning: true,
            supports_upsert: true,
            supports_window_functions: true,
            supports_cte: true,
            supports_json: true,
            supports_full_text_search: true,
            supports_stored_procedures: true,
            supports_schemas: true,
            supports_multiple_databases: true,
            supports_streaming: true,
            supports_cancellation: true,
            supports_explain: true,
            supports_foreign_keys: true,
            supports_views: true,
            supports_triggers: true,
            supports_ssl: true,
            max_identifier_length: Some(63),
            max_parameters: Some(65535),
        }
    }

    #[tracing::instrument(skip(self, config), fields(host = config.get_string("host").as_deref(), database = config.get_string("database").as_deref()))]
    async fn connect(&self, config: &ConnectionConfig) -> Result<Arc<dyn Connection>> {
        let host = config
            .get_string("host")
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 5432 };
        let database = config
            .get_string("database")
            .unwrap_or_else(|| "postgres".to_string());
        let user = config
            .get_string("user")
            .or_else(|| config.get_string("username"));
        let password = config.get_string("password");

        // Extract SSL configuration
        let ssl_mode = config
            .get_string("ssl_mode")
            .unwrap_or_else(|| "prefer".to_string());
        let ssl_ca_cert = config.get_string("ssl_ca_cert");
        let ssl_client_cert = config.get_string("ssl_client_cert");
        let ssl_client_key = config.get_string("ssl_client_key");
        validate_ssl_config(&ssl_mode, ssl_ca_cert.as_deref())?;

        let connect_timeout_seconds = parse_optional_u64(config, "connect_timeout")?;
        let application_name = config
            .get_string("application_name")
            .filter(|value| !value.is_empty());
        let search_path = config
            .get_string("search_path")
            .filter(|value| !value.is_empty());
        let keepalive = parse_bool(config, "keepalive", true);
        let ssh_tunnel = build_ssh_tunnel(config, &host, port)?;
        let (connect_host, connect_port) = if let Some(tunnel) = ssh_tunnel.as_ref() {
            ("127.0.0.1".to_string(), tunnel.local_port())
        } else {
            (host.clone(), port)
        };

        let conn = PostgresConnection::connect(PostgresConnectOptions {
            host: connect_host,
            port: connect_port,
            database: database.clone(),
            user,
            password,
            ssl_mode: ssl_mode.clone(),
            ssl_ca_cert,
            ssl_client_cert,
            ssl_client_key,
            connect_timeout_seconds,
            application_name,
            search_path,
            keepalive,
            ssh_tunnel,
        })
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "failed to connect to PostgreSQL database");
            ZqlzError::Connection(format!("Failed to connect to PostgreSQL database: {}", e))
        })?;

        tracing::info!(host = %host, port = %port, database = %database, ssl = %ssl_mode, "PostgreSQL connection created");
        Ok(Arc::new(conn))
    }

    #[tracing::instrument(skip(self, config))]
    async fn test_connection(&self, config: &ConnectionConfig) -> Result<()> {
        tracing::debug!("testing PostgreSQL connection");
        let conn = self.connect(config).await?;
        conn.query("SELECT 1", &[]).await?;
        Ok(())
    }

    fn build_connection_string(&self, config: &ConnectionConfig) -> String {
        let host = config
            .get_string("host")
            .unwrap_or_else(|| "localhost".to_string());
        let port = if config.port > 0 { config.port } else { 5432 };
        let database = config
            .get_string("database")
            .unwrap_or_else(|| "postgres".to_string());
        let user = config
            .get_string("user")
            .or_else(|| config.get_string("username"));

        let mut conn_str = "postgresql://".to_string();

        if let Some(u) = user {
            conn_str.push_str(&u);
            if let Some(p) = config.get_string("password") {
                conn_str.push(':');
                conn_str.push_str(&p);
            }
            conn_str.push('@');
        }

        conn_str.push_str(&format!("{}:{}/{}", host, port, database));

        conn_str
    }

    fn connection_field_schema(&self) -> ConnectionFieldSchema {
        use zqlz_core::ConnectionFieldOption;

        ConnectionFieldSchema {
            title: Cow::Borrowed("PostgreSQL Connection"),
            fields: vec![
                // General tab fields
                ConnectionField::text("host", "Host")
                    .placeholder("localhost")
                    .default_value("localhost")
                    .required()
                    .width(0.7)
                    .row_group(1),
                ConnectionField::number("port", "Port")
                    .placeholder("5432")
                    .default_value("5432")
                    .width(0.3)
                    .row_group(1),
                ConnectionField::text("database", "Database")
                    .placeholder("postgres")
                    .default_value("postgres"),
                ConnectionField::text("user", "Username")
                    .placeholder("postgres")
                    .default_value("postgres")
                    .width(0.5)
                    .row_group(2),
                ConnectionField::password("password", "Password")
                    .width(0.5)
                    .row_group(2),
                // SSL tab fields
                ConnectionField::select(
                    "ssl_mode",
                    "SSL Mode",
                    vec![
                        ConnectionFieldOption::new("disable", "Disable"),
                        ConnectionFieldOption::new("allow", "Allow"),
                        ConnectionFieldOption::new("prefer", "Prefer (Recommended)"),
                        ConnectionFieldOption::new("require", "Require"),
                        ConnectionFieldOption::new("verify-ca", "Verify CA"),
                        ConnectionFieldOption::new("verify-full", "Verify Full"),
                    ],
                )
                .default_value("prefer")
                .help_text(
                    "SSL connection mode - Prefer tries SSL first, falls back to unencrypted",
                )
                .tab("ssl"),
                ConnectionField::file_path("ssl_ca_cert", "CA Certificate")
                    .placeholder("/path/to/ca-cert.pem")
                    .with_extensions(vec!["pem", "crt", "cer"])
                    .help_text("Root certificate for verifying server certificate")
                    .tab("ssl"),
                ConnectionField::file_path("ssl_client_cert", "Client Certificate")
                    .placeholder("/path/to/client-cert.pem")
                    .with_extensions(vec!["pem", "crt", "cer"])
                    .help_text("Client certificate for mutual TLS authentication")
                    .width(0.5)
                    .row_group(10)
                    .tab("ssl"),
                ConnectionField::file_path("ssl_client_key", "Client Key")
                    .placeholder("/path/to/client-key.pem")
                    .with_extensions(vec!["pem", "key"])
                    .help_text("Private key for client certificate")
                    .width(0.5)
                    .row_group(10)
                    .tab("ssl"),
                // Advanced tab fields
                ConnectionField::number("connect_timeout", "Connect Timeout (seconds)")
                    .placeholder("10")
                    .default_value("10")
                    .help_text("Maximum time to wait for connection")
                    .tab("advanced"),
                ConnectionField::text("application_name", "Application Name")
                    .placeholder("ZQLZ")
                    .default_value("ZQLZ")
                    .help_text("Application name sent to PostgreSQL server")
                    .tab("advanced"),
                ConnectionField::text("search_path", "Search Path")
                    .placeholder("public")
                    .help_text("Default schema search path (comma-separated)")
                    .tab("advanced"),
                ConnectionField::boolean("keepalive", "Keep Alive")
                    .default_value("true")
                    .help_text("Send TCP keepalive packets to maintain connection")
                    .tab("advanced"),
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

fn build_ssh_tunnel(
    config: &ConnectionConfig,
    remote_host: &str,
    remote_port: u16,
) -> Result<Option<PostgresSshTunnel>> {
    if !parse_bool(config, "ssh_enabled", false) {
        return Ok(None);
    }

    let ssh_config = build_ssh_config(config)?;
    PostgresSshTunnel::new(&ssh_config, remote_host, remote_port)
        .map(Some)
        .map_err(|error| {
            ZqlzError::Connection(format!("Failed to establish SSH tunnel: {}", error))
        })
}

fn validate_ssl_config(ssl_mode: &str, ssl_ca_cert: Option<&str>) -> Result<()> {
    if matches!(
        ssl_mode.to_ascii_lowercase().as_str(),
        "verify-ca" | "verify_ca" | "verify-full" | "verify_full"
    ) && ssl_ca_cert.filter(|value| !value.is_empty()).is_none()
    {
        return Err(ZqlzError::Configuration(
            "PostgreSQL SSL verify modes require a CA certificate".to_string(),
        ));
    }

    Ok(())
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
    fn postgres_schema_has_ssl_ssh_and_advanced_fields() {
        let schema = PostgresDriver::new().connection_field_schema();
        let field = |id: &str| schema.fields.iter().find(|field| field.id == id).unwrap();

        assert_eq!(field("ssl_mode").tab.as_deref(), Some("ssl"));
        assert_eq!(field("connect_timeout").tab.as_deref(), Some("advanced"));
        assert_eq!(
            field("application_name").default_value.as_deref(),
            Some("ZQLZ")
        );
        assert_eq!(field("keepalive").default_value.as_deref(), Some("true"));
        assert_eq!(field("ssh_enabled").tab.as_deref(), Some("ssh"));
        assert_eq!(
            field("ssh_auth_method").default_value.as_deref(),
            Some("password")
        );
    }

    #[test]
    fn postgres_ssh_enabled_requires_host_and_user() {
        let config = ConnectionConfig::new("postgres", "test").with_param("ssh_enabled", "true");

        let error = build_ssh_config(&config).unwrap_err();

        assert!(error.to_string().contains("ssh_host is required"));
    }

    #[test]
    fn postgres_advanced_params_parse() {
        let config = ConnectionConfig::new("postgres", "test")
            .with_param("connect_timeout", "15")
            .with_param("keepalive", "false");

        assert_eq!(
            parse_optional_u64(&config, "connect_timeout").unwrap(),
            Some(15)
        );
        assert!(!parse_bool(&config, "keepalive", true));
    }

    #[test]
    fn postgres_verify_ssl_requires_ca() {
        let error = validate_ssl_config("verify-full", None).unwrap_err();

        assert!(error.to_string().contains("CA certificate"));
    }
}
