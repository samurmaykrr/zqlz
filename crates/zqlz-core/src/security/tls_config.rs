//! TLS/SSL Configuration Types
//!
//! Defines configuration types for TLS/SSL connections to databases.
//! TLS provides encryption and authentication for secure database connections.

use crate::{Result, ZqlzError};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// TLS/SSL mode for database connections
///
/// These modes follow PostgreSQL's SSL mode conventions and are
/// applicable to most database systems with appropriate mapping.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    /// Disable TLS entirely (not recommended for production)
    Disable,
    /// Try TLS first, fall back to unencrypted if unavailable
    #[default]
    Allow,
    /// Prefer TLS, but allow unencrypted connections
    Prefer,
    /// Require TLS, but don't verify the server certificate
    Require,
    /// Require TLS and verify the server certificate against the CA
    VerifyCa,
    /// Require TLS, verify CA, and verify the server hostname matches
    VerifyFull,
}

impl TlsMode {
    /// Returns true if this mode requires encryption
    pub fn requires_encryption(&self) -> bool {
        matches!(
            self,
            TlsMode::Require | TlsMode::VerifyCa | TlsMode::VerifyFull
        )
    }

    /// Returns true if this mode requires certificate verification
    pub fn requires_ca_verification(&self) -> bool {
        matches!(self, TlsMode::VerifyCa | TlsMode::VerifyFull)
    }

    /// Returns true if this mode requires hostname verification
    pub fn requires_hostname_verification(&self) -> bool {
        matches!(self, TlsMode::VerifyFull)
    }
}

/// Configuration for TLS/SSL database connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// TLS mode determining the level of security
    pub mode: TlsMode,
    /// Path to the CA certificate file for server verification
    pub ca_cert: Option<PathBuf>,
    /// Path to the client certificate file for mutual TLS
    pub client_cert: Option<PathBuf>,
    /// Path to the client private key file for mutual TLS
    pub client_key: Option<PathBuf>,
    /// Whether to verify the server's certificate (overrides mode for custom behavior)
    #[serde(default = "default_verify_server")]
    pub verify_server: bool,
}

fn default_verify_server() -> bool {
    true
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            mode: TlsMode::default(),
            ca_cert: None,
            client_cert: None,
            client_key: None,
            verify_server: default_verify_server(),
        }
    }
}

impl TlsConfig {
    /// Create a new TLS configuration with the specified mode
    pub fn new(mode: TlsMode) -> Self {
        Self {
            mode,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            verify_server: mode.requires_ca_verification(),
        }
    }

    /// Create a disabled TLS configuration
    pub fn disabled() -> Self {
        Self::new(TlsMode::Disable)
    }

    /// Create a TLS configuration that requires encryption
    pub fn require() -> Self {
        Self::new(TlsMode::Require)
    }

    /// Create a TLS configuration with full verification
    pub fn verify_full() -> Self {
        Self::new(TlsMode::VerifyFull)
    }

    /// Set the CA certificate path for server verification
    pub fn ca_cert(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_cert = Some(path.into());
        self
    }

    /// Set the client certificate for mutual TLS authentication
    pub fn client_cert(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.client_cert = Some(cert_path.into());
        self.client_key = Some(key_path.into());
        self
    }

    /// Override the server verification setting
    pub fn verify_server(mut self, verify: bool) -> Self {
        self.verify_server = verify;
        self
    }

    /// Validate the TLS configuration
    pub fn validate(&self) -> Result<()> {
        // If mode requires CA verification, CA cert should be provided
        if self.mode.requires_ca_verification() && self.ca_cert.is_none() {
            return Err(ZqlzError::Configuration(
                "TLS mode requires CA certificate but none provided".to_string(),
            ));
        }

        // Check CA cert path is not empty if provided
        if let Some(ca_cert) = &self.ca_cert
            && ca_cert.as_os_str().is_empty()
        {
            return Err(ZqlzError::Configuration(
                "CA certificate path cannot be empty".to_string(),
            ));
        }

        // Check client cert path is not empty if provided
        if let Some(client_cert) = &self.client_cert
            && client_cert.as_os_str().is_empty()
        {
            return Err(ZqlzError::Configuration(
                "Client certificate path cannot be empty".to_string(),
            ));
        }

        // Check client key path is not empty if provided
        if let Some(client_key) = &self.client_key
            && client_key.as_os_str().is_empty()
        {
            return Err(ZqlzError::Configuration(
                "Client key path cannot be empty".to_string(),
            ));
        }

        // If client cert is provided, client key must also be provided
        if self.client_cert.is_some() && self.client_key.is_none() {
            return Err(ZqlzError::Configuration(
                "Client certificate provided but client key is missing".to_string(),
            ));
        }

        // If client key is provided, client cert must also be provided
        if self.client_key.is_some() && self.client_cert.is_none() {
            return Err(ZqlzError::Configuration(
                "Client key provided but client certificate is missing".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
