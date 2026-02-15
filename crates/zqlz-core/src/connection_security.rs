//! Connection Security Trait Extensions
//!
//! This module extends the Connection trait with security-related capabilities,
//! allowing drivers to advertise and provide SSH tunnel and TLS support.

use crate::security::{SshTunnelConfig, TlsConfig};

/// Extension trait for connections that support security features.
///
/// This trait allows database drivers to advertise their security capabilities
/// and provide access to their security configurations. Drivers implement this
/// trait to indicate support for SSH tunneling and/or TLS encryption.
///
/// # Example
///
/// ```ignore
/// use zqlz_core::{Connection, ConnectionSecurity, TlsConfig};
///
/// fn check_security<C: Connection + ConnectionSecurity>(conn: &C) {
///     if conn.supports_tls() {
///         if let Some(tls) = conn.tls_config() {
///             println!("TLS mode: {:?}", tls.mode);
///         }
///     }
/// }
/// ```
pub trait ConnectionSecurity {
    /// Returns `true` if this connection supports SSH tunneling.
    ///
    /// Drivers that can establish connections through SSH tunnels should
    /// return `true` from this method when configured with SSH settings.
    fn supports_ssh(&self) -> bool {
        false
    }

    /// Returns `true` if this connection supports TLS/SSL encryption.
    ///
    /// Drivers that can establish encrypted connections should return
    /// `true` from this method when configured with TLS settings.
    fn supports_tls(&self) -> bool {
        false
    }

    /// Returns the SSH tunnel configuration if SSH is being used.
    ///
    /// Returns `Some(&SshTunnelConfig)` if the connection was established
    /// through an SSH tunnel, `None` otherwise.
    fn ssh_config(&self) -> Option<&SshTunnelConfig> {
        None
    }

    /// Returns the TLS configuration if TLS is being used.
    ///
    /// Returns `Some(&TlsConfig)` if the connection uses TLS encryption,
    /// `None` otherwise.
    fn tls_config(&self) -> Option<&TlsConfig> {
        None
    }

    /// Returns `true` if the connection is currently encrypted.
    ///
    /// This indicates whether the actual connection is using encryption,
    /// which may differ from whether TLS is configured (e.g., when TLS
    /// mode is `Allow` or `Prefer` and the server doesn't support TLS).
    fn is_encrypted(&self) -> bool {
        false
    }

    /// Returns `true` if the connection is tunneled through SSH.
    ///
    /// This indicates whether the actual connection is going through
    /// an SSH tunnel, not just whether SSH is configured.
    fn is_tunneled(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests;
