//! MySQL TLS Support
//!
//! Provides TLS/SSL functionality for secure MySQL connections.
//! This module handles SSL options configuration and integration with mysql_async.

use anyhow::{Context, Result};
use mysql_async::SslOpts;
use std::path::Path;
use tracing::{debug, info, warn};
use zqlz_core::security::{TlsConfig, TlsMode};

/// Error types for MySQL TLS operations
#[derive(Debug, thiserror::Error)]
pub enum MysqlTlsError {
    /// Failed to load CA certificate
    #[error("Failed to load CA certificate from {path}: {source}")]
    CaCertLoadFailed {
        path: String,
        source: std::io::Error,
    },

    /// TLS configuration error
    #[error("TLS configuration error: {0}")]
    ConfigurationError(String),

    /// TLS mode not supported
    #[error("TLS mode {mode:?} is not supported for this operation")]
    UnsupportedMode { mode: TlsMode },
}

/// A MySQL TLS/SSL options builder
///
/// Builds SSL options suitable for use with mysql_async based on
/// the provided TLS configuration.
///
/// # Example
///
/// ```ignore
/// use zqlz_drivers::mysql::MysqlTlsConnector;
/// use zqlz_core::security::TlsConfig;
///
/// let config = TlsConfig::require();
/// let ssl_opts = MysqlTlsConnector::build(&config)?;
///
/// let opts = OptsBuilder::from_opts(Opts::default())
///     .ssl_opts(ssl_opts)
///     // ... other options
/// ```
#[derive(Debug, Clone)]
pub struct MysqlTlsConnector;

impl MysqlTlsConnector {
    /// Build SSL options from configuration
    ///
    /// Creates `SslOpts` suitable for use with mysql_async's OptsBuilder.
    /// The options are configured according to the TLS mode and certificate settings.
    ///
    /// # Arguments
    ///
    /// * `config` - The TLS configuration specifying mode and certificates
    ///
    /// # Returns
    ///
    /// An `Option<SslOpts>` - None if TLS is disabled, Some otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Certificate files cannot be read
    /// - Configuration is invalid
    pub fn build(config: &TlsConfig) -> Result<Option<SslOpts>> {
        config.validate().context("Invalid TLS configuration")?;

        if config.mode == TlsMode::Disable {
            debug!("TLS disabled, returning None for SSL options");
            return Ok(None);
        }

        info!(mode = ?config.mode, "Building MySQL SSL options");

        let mut ssl_opts = SslOpts::default();

        // Configure certificate verification based on TLS mode
        ssl_opts = configure_verification(ssl_opts, config)?;

        // Add CA certificate if provided
        if let Some(ca_cert_path) = &config.ca_cert {
            ssl_opts = apply_ca_cert(ssl_opts, ca_cert_path)?;
        }

        debug!("MySQL SSL options built successfully");

        Ok(Some(ssl_opts))
    }

    /// Build SSL options that accept any certificate
    ///
    /// Creates options with verification disabled. This should only be
    /// used for development or testing - not in production.
    ///
    /// # Warning
    ///
    /// This is insecure and should not be used in production environments.
    pub fn build_insecure() -> SslOpts {
        warn!("Building insecure MySQL SSL options - certificate verification disabled");

        SslOpts::default()
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true)
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
fn configure_verification(ssl_opts: SslOpts, config: &TlsConfig) -> Result<SslOpts> {
    let ssl_opts = match config.mode {
        TlsMode::Disable => {
            return Err(MysqlTlsError::UnsupportedMode { mode: config.mode }.into());
        }
        TlsMode::Allow | TlsMode::Prefer | TlsMode::Require => {
            if !config.verify_server {
                debug!("Disabling certificate verification (verify_server = false)");
                ssl_opts
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true)
            } else {
                // Default behavior: accept invalid certs but verify domain
                // This matches MySQL's REQUIRED mode behavior
                ssl_opts.with_danger_accept_invalid_certs(true)
            }
        }
        TlsMode::VerifyCa => {
            debug!("Enabling CA verification only (hostname verification disabled)");
            ssl_opts.with_danger_skip_domain_validation(true)
        }
        TlsMode::VerifyFull => {
            debug!("Enabling full certificate verification");
            // No danger options - full verification enabled
            ssl_opts
        }
    };

    Ok(ssl_opts)
}

/// Load and apply a CA certificate to the SSL options
///
/// # Arguments
///
/// * `ssl_opts` - The SSL options to configure
/// * `path` - Path to the PEM-encoded CA certificate file
fn apply_ca_cert(ssl_opts: SslOpts, path: &Path) -> Result<SslOpts> {
    debug!(path = %path.display(), "Adding CA certificate to SSL options");

    // Verify the file exists before adding it
    if !path.exists() {
        return Err(MysqlTlsError::CaCertLoadFailed {
            path: path.display().to_string(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"),
        }
        .into());
    }

    // mysql_async SslOpts accepts paths directly - it handles loading internally
    let ssl_opts = ssl_opts
        .with_root_certs(vec![path.to_path_buf().into()])
        .with_disable_built_in_roots(true);

    debug!("CA certificate path added to SSL options");

    Ok(ssl_opts)
}

/// Determine the appropriate SSL mode string for MySQL connection URLs
///
/// Converts a TlsMode to the corresponding MySQL ssl-mode parameter value.
pub fn tls_mode_to_ssl_mode(mode: TlsMode) -> &'static str {
    match mode {
        TlsMode::Disable => "DISABLED",
        TlsMode::Allow => "PREFERRED",
        TlsMode::Prefer => "PREFERRED",
        TlsMode::Require => "REQUIRED",
        TlsMode::VerifyCa => "VERIFY_CA",
        TlsMode::VerifyFull => "VERIFY_IDENTITY",
    }
}

/// Build connection URL parameters for TLS
///
/// Returns additional connection URL parameters for TLS configuration.
pub fn build_ssl_params(config: &TlsConfig) -> String {
    let mut params = vec![format!("ssl-mode={}", tls_mode_to_ssl_mode(config.mode))];

    if let Some(ca_cert) = &config.ca_cert {
        params.push(format!("ssl-ca={}", ca_cert.display()));
    }

    if let Some(client_cert) = &config.client_cert {
        params.push(format!("ssl-cert={}", client_cert.display()));
    }

    if let Some(client_key) = &config.client_key {
        params.push(format!("ssl-key={}", client_key.display()));
    }

    params.join("&")
}

#[cfg(test)]
mod tests;
