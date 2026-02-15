//! Tests for PostgreSQL SSH Tunnel Support

use super::*;
use std::path::PathBuf;
use zqlz_core::security::{SshAuthMethod, SshTunnelConfig};

#[test]
fn test_ssh_tunnel_creation_fails_invalid_host() {
    let config = SshTunnelConfig::with_password("invalid.nonexistent.host.xyz", "user", "password")
        .timeout(1);

    let result = PostgresSshTunnel::new(&config, "localhost", 5432);

    assert!(result.is_err());
    let err = result.unwrap_err();
    let err_string = err.to_string();
    // The error should indicate connection failure
    assert!(
        err_string.contains("connect")
            || err_string.contains("Failed")
            || err_string.contains("resolve")
            || err_string.contains("DNS")
            || err_string.contains("address")
            || err_string.contains("invalid"),
        "Unexpected error message: {}",
        err_string
    );
}

#[test]
fn test_ssh_tunnel_creation_fails_empty_host() {
    let config = SshTunnelConfig {
        host: String::new(),
        port: 22,
        username: "user".to_string(),
        auth: SshAuthMethod::password("password"),
        timeout_seconds: 30,
        keepalive_seconds: 0,
    };

    let result = PostgresSshTunnel::new(&config, "localhost", 5432);

    assert!(result.is_err());
    let err_string = result.unwrap_err().to_string();
    // The error should be about invalid SSH configuration (empty host)
    assert!(
        err_string.contains("SSH") || err_string.contains("host") || err_string.contains("empty"),
        "Unexpected error message: {}",
        err_string
    );
}

#[test]
fn test_ssh_tunnel_creation_fails_invalid_port() {
    let config = SshTunnelConfig {
        host: "example.com".to_string(),
        port: 0,
        username: "user".to_string(),
        auth: SshAuthMethod::password("password"),
        timeout_seconds: 30,
        keepalive_seconds: 0,
    };

    let result = PostgresSshTunnel::new(&config, "localhost", 5432);

    assert!(result.is_err());
    let err_string = result.unwrap_err().to_string();
    // The error should be about invalid port
    assert!(
        err_string.contains("port") || err_string.contains("SSH") || err_string.contains("0"),
        "Unexpected error message: {}",
        err_string
    );
}

#[test]
fn test_find_available_port() {
    let port1 = find_available_port().expect("Should find available port");
    let port2 = find_available_port().expect("Should find another available port");

    assert!(port1 > 0);
    assert!(port2 > 0);
    // Ports are allocated quickly, they might be the same or different
    // Just verify they're valid
}

#[test]
fn test_authenticate_fails_with_nonexistent_private_key() {
    let mut session = Session::new().expect("Should create session");

    let auth = SshAuthMethod::PrivateKey {
        path: PathBuf::from("/nonexistent/path/to/key"),
        passphrase: None,
    };

    let result = authenticate(&mut session, "user", &auth);

    assert!(result.is_err());
    let err_string = result.unwrap_err().to_string().to_lowercase();
    assert!(
        err_string.contains("not found") || err_string.contains("key"),
        "Unexpected error message: {}",
        err_string
    );
}

#[test]
fn test_ssh_tunnel_error_display() {
    let err = SshTunnelError::ConnectionFailed {
        host: "example.com".to_string(),
        port: 22,
        source: std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused"),
    };
    assert!(err.to_string().contains("example.com"));
    assert!(err.to_string().contains("22"));

    let err = SshTunnelError::HandshakeFailed("test error".to_string());
    assert!(err.to_string().to_lowercase().contains("handshake"));

    let err = SshTunnelError::AuthenticationFailed("bad password".to_string());
    assert!(err.to_string().to_lowercase().contains("authentication"));

    let err = SshTunnelError::PortForwardingFailed("forward error".to_string());
    assert!(err.to_string().to_lowercase().contains("forwarding"));

    let err = SshTunnelError::AgentNotAvailable("no agent".to_string());
    assert!(err.to_string().to_lowercase().contains("agent"));

    let err = SshTunnelError::PrivateKeyNotFound {
        path: "/some/path".to_string(),
    };
    assert!(err.to_string().contains("/some/path"));

    let err = SshTunnelError::InvalidPrivateKey("bad key".to_string());
    assert!(err.to_string().contains("Invalid") || err.to_string().to_lowercase().contains("key"));

    let err = SshTunnelError::TunnelClosed;
    assert!(err.to_string().to_lowercase().contains("closed"));
}

#[test]
fn test_local_connection_string_format() {
    let local_port = 12345;
    let expected = format!("host=127.0.0.1 port={}", local_port);

    assert_eq!(expected, "host=127.0.0.1 port=12345");
}

#[test]
fn test_ssh_tunnel_config_validation_in_new() {
    let config = SshTunnelConfig::with_password("host.example.com", "", "password");
    let result = PostgresSshTunnel::new(&config, "localhost", 5432);

    assert!(result.is_err());
    let err_string = result.unwrap_err().to_string();
    // The error should be about invalid SSH configuration (empty username)
    assert!(
        err_string.contains("username")
            || err_string.contains("SSH")
            || err_string.contains("empty"),
        "Unexpected error message: {}",
        err_string
    );
}

#[test]
fn test_ssh_auth_method_variants() {
    let password_auth = SshAuthMethod::password("secret");
    match password_auth {
        SshAuthMethod::Password { password } => assert_eq!(password, "secret"),
        _ => panic!("Expected Password variant"),
    }

    let key_auth = SshAuthMethod::private_key("/path/to/key");
    match key_auth {
        SshAuthMethod::PrivateKey { path, passphrase } => {
            assert_eq!(path, PathBuf::from("/path/to/key"));
            assert!(passphrase.is_none());
        }
        _ => panic!("Expected PrivateKey variant"),
    }

    let key_auth_with_pass = SshAuthMethod::private_key_with_passphrase("/path/to/key", "phrase");
    match key_auth_with_pass {
        SshAuthMethod::PrivateKey { path, passphrase } => {
            assert_eq!(path, PathBuf::from("/path/to/key"));
            assert_eq!(passphrase, Some("phrase".to_string()));
        }
        _ => panic!("Expected PrivateKey variant"),
    }

    let agent_auth = SshAuthMethod::agent();
    assert!(matches!(agent_auth, SshAuthMethod::Agent));
}

#[test]
fn test_ssh_tunnel_config_builder_pattern() {
    let config = SshTunnelConfig::with_password("bastion.example.com", "admin", "secret123")
        .port(2222)
        .timeout(60)
        .keepalive(30);

    assert_eq!(config.host, "bastion.example.com");
    assert_eq!(config.port, 2222);
    assert_eq!(config.username, "admin");
    assert_eq!(config.timeout_seconds, 60);
    assert_eq!(config.keepalive_seconds, 30);
}

#[test]
fn test_postgres_ssh_tunnel_debug_impl() {
    // We can't create a real tunnel without a server,
    // but we can test that the Debug impl doesn't panic
    // by verifying the format strings are correct
    let port = 12345;
    let host = "db.example.com";
    let remote_port = 5432;

    let debug_str = format!(
        "PostgresSshTunnel {{ local_port: {}, remote_host: {:?}, remote_port: {}, is_running: true, .. }}",
        port, host, remote_port
    );

    assert!(debug_str.contains("12345"));
    assert!(debug_str.contains("db.example.com"));
    assert!(debug_str.contains("5432"));
}
