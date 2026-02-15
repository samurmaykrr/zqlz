//! PostgreSQL TLS Support
//!
//! Provides TLS/SSL functionality for secure PostgreSQL connections.
//! This module handles certificate loading, TLS configuration building,
//! and integration with tokio-postgres.

use anyhow::{Context, Result};
use native_tls::{Certificate, Identity, TlsConnector as NativeTlsConnector, TlsConnectorBuilder};
use postgres_native_tls::MakeTlsConnector;
use std::fs;
use std::path::Path;
use tracing::{debug, info, warn};
use zqlz_core::security::{TlsConfig, TlsMode};

/// Error types for TLS operations
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// Failed to load CA certificate
    #[error("Failed to load CA certificate from {path}: {source}")]
    CaCertLoadFailed {
        path: String,
        source: std::io::Error,
    },

    /// Invalid CA certificate format
    #[error("Invalid CA certificate format: {0}")]
    InvalidCaCert(String),

    /// Failed to load client certificate
    #[error("Failed to load client certificate from {path}: {source}")]
    ClientCertLoadFailed {
        path: String,
        source: std::io::Error,
    },

    /// Failed to load client key
    #[error("Failed to load client key from {path}: {source}")]
    ClientKeyLoadFailed {
        path: String,
        source: std::io::Error,
    },

    /// Invalid client identity format
    #[error("Invalid client identity (cert + key): {0}")]
    InvalidClientIdentity(String),

    /// TLS configuration error
    #[error("TLS configuration error: {0}")]
    ConfigurationError(String),

    /// TLS mode not supported
    #[error("TLS mode {mode:?} is not supported for this operation")]
    UnsupportedMode { mode: TlsMode },
}

/// A PostgreSQL TLS connector builder
///
/// Builds TLS connectors suitable for use with tokio-postgres based on
/// the provided TLS configuration.
///
/// # Example
///
/// ```ignore
/// use zqlz_drivers::postgres::PostgresTlsConnector;
/// use zqlz_core::security::TlsConfig;
///
/// let config = TlsConfig::verify_full()
///     .ca_cert("/path/to/ca.crt");
///
/// let tls_connector = PostgresTlsConnector::build(&config)?;
///
/// let (client, connection) = tokio_postgres::connect(
///     "host=db.example.com port=5432 dbname=mydb",
///     tls_connector,
/// ).await?;
/// ```
#[derive(Debug, Clone)]
pub struct PostgresTlsConnector;

impl PostgresTlsConnector {
    /// Build a TLS connector from configuration
    ///
    /// Creates a `MakeTlsConnector` suitable for use with tokio-postgres.
    /// The connector is configured according to the TLS mode and certificate settings.
    ///
    /// # Arguments
    ///
    /// * `config` - The TLS configuration specifying mode and certificates
    ///
    /// # Returns
    ///
    /// A `MakeTlsConnector` that can be passed to `tokio_postgres::connect`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The TLS mode is Disable (use `NoTls` instead)
    /// - Certificate files cannot be read
    /// - Certificate formats are invalid
    pub fn build(config: &TlsConfig) -> Result<MakeTlsConnector> {
        config.validate().context("Invalid TLS configuration")?;

        if config.mode == TlsMode::Disable {
            return Err(TlsError::UnsupportedMode { mode: config.mode }.into());
        }

        info!(mode = ?config.mode, "Building PostgreSQL TLS connector");

        let mut builder = NativeTlsConnector::builder();

        configure_verification(&mut builder, config)?;

        if let Some(ca_cert_path) = &config.ca_cert {
            apply_ca_cert(&mut builder, ca_cert_path)?;
        }

        if let (Some(cert_path), Some(key_path)) = (&config.client_cert, &config.client_key) {
            apply_client_cert(&mut builder, cert_path, key_path)?;
        }

        let connector = builder
            .build()
            .map_err(|e| TlsError::ConfigurationError(e.to_string()))?;

        debug!("TLS connector built successfully");

        Ok(MakeTlsConnector::new(connector))
    }

    /// Build a TLS connector that accepts any certificate
    ///
    /// Creates a connector with verification disabled. This should only be
    /// used for development or testing - not in production.
    ///
    /// # Warning
    ///
    /// This is insecure and should not be used in production environments.
    pub fn build_insecure() -> Result<MakeTlsConnector> {
        warn!("Building insecure TLS connector - certificate verification disabled");

        let connector = NativeTlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .build()
            .map_err(|e| TlsError::ConfigurationError(e.to_string()))?;

        Ok(MakeTlsConnector::new(connector))
    }

    /// Check if TLS is required for the given configuration
    pub fn is_required(config: &TlsConfig) -> bool {
        config.mode.requires_encryption()
    }

    /// Check if TLS is disabled for the given configuration
    pub fn is_disabled(config: &TlsConfig) -> bool {
        config.mode == TlsMode::Disable
    }
}

/// Configure certificate verification based on TLS mode
fn configure_verification(builder: &mut TlsConnectorBuilder, config: &TlsConfig) -> Result<()> {
    match config.mode {
        TlsMode::Disable => {
            return Err(TlsError::UnsupportedMode { mode: config.mode }.into());
        }
        TlsMode::Allow | TlsMode::Prefer | TlsMode::Require => {
            if !config.verify_server {
                debug!("Disabling certificate verification (verify_server = false)");
                builder.danger_accept_invalid_certs(true);
                builder.danger_accept_invalid_hostnames(true);
            }
        }
        TlsMode::VerifyCa => {
            debug!("Enabling CA verification only (hostname verification disabled)");
            builder.danger_accept_invalid_hostnames(true);
        }
        TlsMode::VerifyFull => {
            debug!("Enabling full certificate verification");
        }
    }

    Ok(())
}

/// Load and apply a CA certificate to the TLS builder
///
/// # Arguments
///
/// * `builder` - The TLS connector builder to configure
/// * `path` - Path to the PEM-encoded CA certificate file
fn apply_ca_cert(builder: &mut TlsConnectorBuilder, path: &Path) -> Result<()> {
    debug!(path = %path.display(), "Loading CA certificate");

    let pem_data = fs::read(path).map_err(|e| TlsError::CaCertLoadFailed {
        path: path.display().to_string(),
        source: e,
    })?;

    let cert =
        Certificate::from_pem(&pem_data).map_err(|e| TlsError::InvalidCaCert(e.to_string()))?;

    builder.add_root_certificate(cert);

    debug!("CA certificate loaded successfully");

    Ok(())
}

/// Load and apply client certificate and key for mutual TLS
///
/// # Arguments
///
/// * `builder` - The TLS connector builder to configure
/// * `cert_path` - Path to the PEM-encoded client certificate file
/// * `key_path` - Path to the PEM-encoded client private key file
fn apply_client_cert(
    builder: &mut TlsConnectorBuilder,
    cert_path: &Path,
    key_path: &Path,
) -> Result<()> {
    debug!(
        cert_path = %cert_path.display(),
        key_path = %key_path.display(),
        "Loading client certificate and key"
    );

    let cert_pem = fs::read(cert_path).map_err(|e| TlsError::ClientCertLoadFailed {
        path: cert_path.display().to_string(),
        source: e,
    })?;

    let key_pem = fs::read(key_path).map_err(|e| TlsError::ClientKeyLoadFailed {
        path: key_path.display().to_string(),
        source: e,
    })?;

    // native-tls Identity::from_pkcs8 expects PEM cert and PEM key
    let identity = Identity::from_pkcs8(&cert_pem, &key_pem)
        .map_err(|e| TlsError::InvalidClientIdentity(e.to_string()))?;

    builder.identity(identity);

    debug!("Client certificate and key loaded successfully");

    Ok(())
}

/// Determine the appropriate SSL mode string for PostgreSQL connection strings
///
/// Converts a TlsMode to the corresponding PostgreSQL sslmode parameter value.
pub fn tls_mode_to_sslmode(mode: TlsMode) -> &'static str {
    match mode {
        TlsMode::Disable => "disable",
        TlsMode::Allow => "allow",
        TlsMode::Prefer => "prefer",
        TlsMode::Require => "require",
        TlsMode::VerifyCa => "verify-ca",
        TlsMode::VerifyFull => "verify-full",
    }
}

/// Build connection string parameters for TLS
///
/// Returns additional connection string parameters for TLS configuration.
pub fn build_tls_params(config: &TlsConfig) -> String {
    let mut params = vec![format!("sslmode={}", tls_mode_to_sslmode(config.mode))];

    if let Some(ca_cert) = &config.ca_cert {
        params.push(format!("sslrootcert={}", ca_cert.display()));
    }

    if let Some(client_cert) = &config.client_cert {
        params.push(format!("sslcert={}", client_cert.display()));
    }

    if let Some(client_key) = &config.client_key {
        params.push(format!("sslkey={}", client_key.display()));
    }

    params.join(" ")
}

#[cfg(test)]
mod tests;
