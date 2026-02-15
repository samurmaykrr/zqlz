//! Tests for ConnectionSecurity trait

use super::*;
use crate::security::{SshAuthMethod, SshTunnelConfig, TlsConfig, TlsMode};

/// A mock connection type that uses default trait implementations
struct DefaultSecurityConnection;

impl ConnectionSecurity for DefaultSecurityConnection {}

/// A mock connection type that supports SSH tunneling
struct SshEnabledConnection {
    ssh_config: SshTunnelConfig,
}

impl ConnectionSecurity for SshEnabledConnection {
    fn supports_ssh(&self) -> bool {
        true
    }

    fn ssh_config(&self) -> Option<&SshTunnelConfig> {
        Some(&self.ssh_config)
    }

    fn is_tunneled(&self) -> bool {
        true
    }
}

/// A mock connection type that supports TLS
struct TlsEnabledConnection {
    tls_config: TlsConfig,
}

impl ConnectionSecurity for TlsEnabledConnection {
    fn supports_tls(&self) -> bool {
        true
    }

    fn tls_config(&self) -> Option<&TlsConfig> {
        Some(&self.tls_config)
    }

    fn is_encrypted(&self) -> bool {
        self.tls_config.mode.requires_encryption()
    }
}

/// A mock connection that supports both SSH and TLS
struct FullSecurityConnection {
    ssh_config: SshTunnelConfig,
    tls_config: TlsConfig,
}

impl ConnectionSecurity for FullSecurityConnection {
    fn supports_ssh(&self) -> bool {
        true
    }

    fn supports_tls(&self) -> bool {
        true
    }

    fn ssh_config(&self) -> Option<&SshTunnelConfig> {
        Some(&self.ssh_config)
    }

    fn tls_config(&self) -> Option<&TlsConfig> {
        Some(&self.tls_config)
    }

    fn is_encrypted(&self) -> bool {
        self.tls_config.mode.requires_encryption()
    }

    fn is_tunneled(&self) -> bool {
        true
    }
}

#[test]
fn test_default_ssh_support_false() {
    let conn = DefaultSecurityConnection;
    assert!(!conn.supports_ssh());
    assert!(conn.ssh_config().is_none());
    assert!(!conn.is_tunneled());
}

#[test]
fn test_default_tls_support_false() {
    let conn = DefaultSecurityConnection;
    assert!(!conn.supports_tls());
    assert!(conn.tls_config().is_none());
    assert!(!conn.is_encrypted());
}

#[test]
fn test_ssh_enabled_connection() {
    let conn = SshEnabledConnection {
        ssh_config: SshTunnelConfig::with_password("bastion.example.com", "admin", "secret"),
    };

    assert!(conn.supports_ssh());
    assert!(!conn.supports_tls());
    assert!(conn.is_tunneled());
    assert!(!conn.is_encrypted());

    let config = conn.ssh_config().expect("should have SSH config");
    assert_eq!(config.host, "bastion.example.com");
    assert_eq!(config.username, "admin");
    assert_eq!(config.port, 22);
}

#[test]
fn test_tls_enabled_connection() {
    let conn = TlsEnabledConnection {
        tls_config: TlsConfig::require(),
    };

    assert!(!conn.supports_ssh());
    assert!(conn.supports_tls());
    assert!(!conn.is_tunneled());
    assert!(conn.is_encrypted());

    let config = conn.tls_config().expect("should have TLS config");
    assert!(config.mode.requires_encryption());
}

#[test]
fn test_tls_allow_mode_not_encrypted() {
    let conn = TlsEnabledConnection {
        tls_config: TlsConfig::new(TlsMode::Allow),
    };

    assert!(conn.supports_tls());
    // Allow mode doesn't require encryption, so is_encrypted returns false
    assert!(!conn.is_encrypted());
}

#[test]
fn test_full_security_connection() {
    let conn = FullSecurityConnection {
        ssh_config: SshTunnelConfig::with_agent("bastion.example.com", "deploy"),
        tls_config: TlsConfig::verify_full().ca_cert("/etc/ssl/ca.pem"),
    };

    assert!(conn.supports_ssh());
    assert!(conn.supports_tls());
    assert!(conn.is_tunneled());
    assert!(conn.is_encrypted());

    let ssh = conn.ssh_config().expect("should have SSH config");
    assert_eq!(ssh.username, "deploy");
    assert!(matches!(ssh.auth, SshAuthMethod::Agent));

    let tls = conn.tls_config().expect("should have TLS config");
    assert!(tls.mode.requires_hostname_verification());
    assert!(tls.ca_cert.is_some());
}

#[test]
fn test_ssh_with_private_key() {
    let conn = SshEnabledConnection {
        ssh_config: SshTunnelConfig::with_private_key(
            "jump.example.com",
            "developer",
            "/home/user/.ssh/id_rsa",
        )
        .port(2222)
        .timeout(60),
    };

    let config = conn.ssh_config().expect("should have SSH config");
    assert_eq!(config.port, 2222);
    assert_eq!(config.timeout_seconds, 60);

    match &config.auth {
        SshAuthMethod::PrivateKey { path, passphrase } => {
            assert_eq!(path.to_str().unwrap(), "/home/user/.ssh/id_rsa");
            assert!(passphrase.is_none());
        }
        _ => panic!("expected PrivateKey auth method"),
    }
}
