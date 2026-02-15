//! SSH Tunnel Configuration Types
//!
//! Defines configuration types for establishing SSH tunnels to databases.
//! SSH tunnels allow secure connections to database servers that are only
//! accessible via a bastion/jump host.

use crate::{Result, ZqlzError};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Authentication method for SSH connections
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SshAuthMethod {
    /// Authenticate using a password
    Password {
        /// The password for authentication
        password: String,
    },
    /// Authenticate using a private key file
    PrivateKey {
        /// Path to the private key file
        path: PathBuf,
        /// Optional passphrase for encrypted private keys
        passphrase: Option<String>,
    },
    /// Use the system SSH agent for authentication
    Agent,
}

impl SshAuthMethod {
    /// Create a password authentication method
    pub fn password(password: impl Into<String>) -> Self {
        Self::Password {
            password: password.into(),
        }
    }

    /// Create a private key authentication method
    pub fn private_key(path: impl Into<PathBuf>) -> Self {
        Self::PrivateKey {
            path: path.into(),
            passphrase: None,
        }
    }

    /// Create a private key authentication method with a passphrase
    pub fn private_key_with_passphrase(
        path: impl Into<PathBuf>,
        passphrase: impl Into<String>,
    ) -> Self {
        Self::PrivateKey {
            path: path.into(),
            passphrase: Some(passphrase.into()),
        }
    }

    /// Create an SSH agent authentication method
    pub fn agent() -> Self {
        Self::Agent
    }
}

/// Configuration for establishing an SSH tunnel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshTunnelConfig {
    /// SSH server hostname or IP address
    pub host: String,
    /// SSH server port (default: 22)
    pub port: u16,
    /// Username for SSH authentication
    pub username: String,
    /// Authentication method
    pub auth: SshAuthMethod,
    /// Connection timeout in seconds (default: 30)
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u32,
    /// Keep-alive interval in seconds (0 to disable)
    #[serde(default)]
    pub keepalive_seconds: u32,
}

fn default_timeout() -> u32 {
    30
}

impl SshTunnelConfig {
    /// Create a new SSH tunnel configuration with password authentication
    pub fn with_password(
        host: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: 22,
            username: username.into(),
            auth: SshAuthMethod::password(password),
            timeout_seconds: default_timeout(),
            keepalive_seconds: 0,
        }
    }

    /// Create a new SSH tunnel configuration with private key authentication
    pub fn with_private_key(
        host: impl Into<String>,
        username: impl Into<String>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            host: host.into(),
            port: 22,
            username: username.into(),
            auth: SshAuthMethod::private_key(key_path),
            timeout_seconds: default_timeout(),
            keepalive_seconds: 0,
        }
    }

    /// Create a new SSH tunnel configuration with SSH agent authentication
    pub fn with_agent(host: impl Into<String>, username: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: 22,
            username: username.into(),
            auth: SshAuthMethod::agent(),
            timeout_seconds: default_timeout(),
            keepalive_seconds: 0,
        }
    }

    /// Set the SSH server port
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the connection timeout in seconds
    pub fn timeout(mut self, seconds: u32) -> Self {
        self.timeout_seconds = seconds;
        self
    }

    /// Set the keep-alive interval in seconds
    pub fn keepalive(mut self, seconds: u32) -> Self {
        self.keepalive_seconds = seconds;
        self
    }

    /// Validate the SSH tunnel configuration
    pub fn validate(&self) -> Result<()> {
        if self.host.is_empty() {
            return Err(ZqlzError::Configuration(
                "SSH host cannot be empty".to_string(),
            ));
        }

        if self.port == 0 {
            return Err(ZqlzError::Configuration("SSH port cannot be 0".to_string()));
        }

        if self.username.is_empty() {
            return Err(ZqlzError::Configuration(
                "SSH username cannot be empty".to_string(),
            ));
        }

        if let SshAuthMethod::PrivateKey { path, .. } = &self.auth {
            if path.as_os_str().is_empty() {
                return Err(ZqlzError::Configuration(
                    "SSH private key path cannot be empty".to_string(),
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
