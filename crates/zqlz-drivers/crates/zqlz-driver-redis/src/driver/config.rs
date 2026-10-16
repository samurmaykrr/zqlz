//! Connection configuration parsing: TLS, SSH tunnel, and URL building

use zqlz_core::{
    ConnectionConfig, Result, ZqlzError,
    security::{SshAuthMethod, SshTunnelConfig, TlsConfig, TlsMode},
};

use crate::RedisSshTunnel;

pub(super) fn validate_connection_config(config: &ConnectionConfig) -> Result<()> {
    let port = config
        .get_string("port")
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<u16>()
                .ok()
                .filter(|port| *port > 0)
                .ok_or_else(|| {
                    ZqlzError::Configuration("Redis port must be between 1 and 65535".to_string())
                })
        })
        .transpose()?
        .unwrap_or(if config.port > 0 { config.port } else { 6379 });
    if port == 0 {
        return Err(ZqlzError::Configuration(
            "Redis port must be between 1 and 65535".to_string(),
        ));
    }

    if let Some(database) = config
        .get_string("database")
        .filter(|value| !value.is_empty())
    {
        database.parse::<u16>().map_err(|_| {
            ZqlzError::Configuration("Redis database must be a whole number".to_string())
        })?;
    }

    Ok(())
}

pub(super) fn redis_port(config: &ConnectionConfig) -> u16 {
    config
        .get_string("port")
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(if config.port > 0 { config.port } else { 6379 })
}

pub(super) fn redis_database(config: &ConnectionConfig) -> u16 {
    config
        .get_string("database")
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0)
}

pub(super) fn build_redis_connection_string(
    config: &ConnectionConfig,
    tls_config: &TlsConfig,
    host: &str,
    port: u16,
) -> String {
    let database = redis_database(config);
    let username = config.get_string("username").filter(|s| !s.is_empty());
    let password = config.get_string("password").filter(|s| !s.is_empty());
    let use_tls = tls_config.mode != TlsMode::Disable;
    let scheme = if use_tls { "rediss" } else { "redis" };
    let mut url = String::new();
    url.push_str(scheme);
    url.push_str("://");

    match (username, password) {
        (Some(user), Some(pass)) => {
            url.push_str(&encode_url_component(&user));
            url.push(':');
            url.push_str(&encode_url_component(&pass));
            url.push('@');
        }
        (None, Some(pass)) => {
            url.push(':');
            url.push_str(&encode_url_component(&pass));
            url.push('@');
        }
        _ => {}
    }

    url.push_str(host);
    url.push(':');
    url.push_str(&port.to_string());
    url.push('/');
    url.push_str(&database.to_string());

    if use_tls && !tls_config.verify_server {
        url.push_str("#insecure");
    }

    url
}

fn encode_url_component(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

pub(super) fn build_tls_config(config: &ConnectionConfig) -> Result<TlsConfig> {
    let mode = match config
        .get_string("ssl_mode")
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_uppercase())
    {
        Some(value) => match value.as_str() {
            "DISABLED" | "DISABLE" => TlsMode::Disable,
            "PREFERRED" | "PREFER" | "ALLOW" => TlsMode::Prefer,
            "REQUIRED" | "REQUIRE" => TlsMode::Require,
            "VERIFY_CA" | "VERIFY-CA" => TlsMode::VerifyCa,
            "VERIFY_FULL" | "VERIFY-FULL" | "VERIFY_IDENTITY" => TlsMode::VerifyFull,
            _ => {
                return Err(ZqlzError::Configuration(format!(
                    "Invalid Redis SSL mode: {}",
                    value
                )));
            }
        },
        None if parse_bool(config, "ssl", false) || parse_bool(config, "tls", false) => {
            TlsMode::Require
        }
        None => TlsMode::Disable,
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

pub(super) fn build_ssh_tunnel(
    config: &ConnectionConfig,
    remote_host: &str,
    remote_port: u16,
) -> Result<Option<RedisSshTunnel>> {
    if !parse_bool(config, "ssh_enabled", false) {
        return Ok(None);
    }

    let ssh_config = build_ssh_config(config)?;
    RedisSshTunnel::new(&ssh_config, remote_host, remote_port)
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
mod config_tests {
    use super::*;

    #[test]
    fn redis_validation_rejects_invalid_port() {
        let config = ConnectionConfig::new("redis", "test").with_param("port", "0");

        let error = validate_connection_config(&config).unwrap_err();

        assert!(error.to_string().contains("between 1 and 65535"));
    }

    #[test]
    fn redis_validation_accepts_large_database_number() {
        let config = ConnectionConfig::new("redis", "test")
            .with_param("port", "6379")
            .with_param("database", "32");

        validate_connection_config(&config).unwrap();
    }

    #[test]
    fn redis_verify_ca_requires_ca() {
        let config = ConnectionConfig::new("redis", "test").with_param("ssl_mode", "VERIFY_CA");

        let error = build_tls_config(&config).unwrap_err();

        assert!(error.to_string().contains("CA certificate"));
    }

    #[test]
    fn redis_ssh_enabled_requires_host_and_user() {
        let config = ConnectionConfig::new("redis", "test")
            .with_param("ssh_enabled", "true")
            .with_param("ssh_auth_method", "agent");

        let error = build_ssh_config(&config).unwrap_err();

        assert!(error.to_string().contains("ssh_host"));
    }
}
