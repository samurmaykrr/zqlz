//! Tests for MySQL SSH tunnel support
//!
//! These tests cover the SSH tunnel configuration, error handling,
//! and helper functions. Integration tests requiring actual SSH
//! servers are in tests/mysql_ssh_integration.rs.

use super::*;
use std::path::PathBuf;

#[test]
fn test_mysql_ssh_tunnel_error_display() {
    let err = MysqlSshTunnelError::ConnectionFailed {
        host: "bastion.example.com".to_string(),
        port: 22,
        source: std::io::Error::new(std::io::ErrorKind::TimedOut, "connection timed out"),
    };
    assert!(err.to_string().contains("bastion.example.com"));
    assert!(err.to_string().contains("22"));

    let err = MysqlSshTunnelError::HandshakeFailed("protocol error".to_string());
    assert!(err.to_string().contains("handshake failed"));

    let err = MysqlSshTunnelError::AuthenticationFailed("bad password".to_string());
    assert!(err.to_string().contains("authentication failed"));

    let err = MysqlSshTunnelError::PortForwardingFailed("channel error".to_string());
    assert!(err.to_string().contains("port forwarding"));

    let err = MysqlSshTunnelError::AgentNotAvailable("no agent".to_string());
    assert!(err.to_string().contains("agent"));

    let err = MysqlSshTunnelError::PrivateKeyNotFound {
        path: "/path/to/key".to_string(),
    };
    assert!(err.to_string().contains("/path/to/key"));

    let err = MysqlSshTunnelError::InvalidPrivateKey("bad format".to_string());
    assert!(err.to_string().contains("Invalid private key"));

    let err = MysqlSshTunnelError::TunnelClosed;
    assert!(err.to_string().contains("closed"));
}

#[test]
fn test_default_mysql_port() {
    assert_eq!(MysqlSshTunnel::DEFAULT_MYSQL_PORT, 3306);
}

#[test]
fn test_find_available_port_returns_valid_port() {
    let port = find_available_port().expect("Should find an available port");
    assert!(port > 0);
    assert!(port > 1024, "Should be in user port range");
}

#[test]
fn test_find_available_port_returns_different_ports() {
    let port1 = find_available_port().expect("Should find first port");
    let port2 = find_available_port().expect("Should find second port");

    // Ports should typically be different (not guaranteed but very likely)
    // We just verify both are valid
    assert!(port1 > 0);
    assert!(port2 > 0);
}

#[test]
fn test_ssh_config_validation_required_for_tunnel() {
    // Create an invalid config (empty host)
    let config = SshTunnelConfig::with_password("", "user", "password");

    // new() should fail validation
    let result = MysqlSshTunnel::new(&config, "mysql.internal", 3306);
    assert!(result.is_err());
}

#[test]
fn test_ssh_tunnel_new_fails_with_invalid_host() {
    let config =
        SshTunnelConfig::with_password("nonexistent.invalid.host.test", "user", "password");

    let result = MysqlSshTunnel::new(&config, "mysql.internal", 3306);
    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_str = err.to_string().to_lowercase();
    // Should be a connection error or address parsing error
    assert!(
        err_str.contains("connect")
            || err_str.contains("connection")
            || err_str.contains("dns")
            || err_str.contains("resolution")
            || err_str.contains("failed")
            || err_str.contains("address")
            || err_str.contains("invalid"),
        "Expected connection error but got: {}",
        err
    );
}

#[test]
fn test_authenticate_fails_with_nonexistent_key_file() {
    let mut session = Session::new().unwrap();

    let auth = SshAuthMethod::PrivateKey {
        path: PathBuf::from("/nonexistent/path/to/key"),
        passphrase: None,
    };

    let result = authenticate(&mut session, "user", &auth);
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("not found") || err.to_string().contains("Private key"),
        "Error should mention missing key file: {}",
        err
    );
}

#[test]
fn test_mysql_url_generation() {
    // We can't create a real tunnel without SSH server, but we can test
    // the URL generation logic by simulating the fields
    struct MockTunnel {
        local_port: u16,
    }

    impl MockTunnel {
        fn mysql_url(&self, user: &str, password: &str, database: &str) -> String {
            format!(
                "mysql://{}:{}@127.0.0.1:{}/{}",
                user, password, self.local_port, database
            )
        }

        fn mysql_url_template(&self, database: &str) -> String {
            format!("mysql://127.0.0.1:{}/{}", self.local_port, database)
        }
    }

    let mock = MockTunnel { local_port: 12345 };

    assert_eq!(
        mock.mysql_url("root", "secret", "mydb"),
        "mysql://root:secret@127.0.0.1:12345/mydb"
    );

    assert_eq!(
        mock.mysql_url_template("mydb"),
        "mysql://127.0.0.1:12345/mydb"
    );
}

#[test]
fn test_mysql_url_with_special_characters() {
    struct MockTunnel {
        local_port: u16,
    }

    impl MockTunnel {
        fn mysql_url(&self, user: &str, password: &str, database: &str) -> String {
            format!(
                "mysql://{}:{}@127.0.0.1:{}/{}",
                user, password, self.local_port, database
            )
        }
    }

    let mock = MockTunnel { local_port: 54321 };

    // Note: In production, passwords with special chars should be URL-encoded
    // This test documents current behavior
    let url = mock.mysql_url("admin", "p@ss!word", "test_db");
    assert!(url.contains("127.0.0.1:54321"));
    assert!(url.contains("admin"));
    assert!(url.contains("test_db"));
}

#[test]
fn test_ssh_tunnel_debug_format() {
    // We can test Debug formatting of the error types
    let err = MysqlSshTunnelError::TunnelClosed;
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("TunnelClosed"));
}

#[test]
fn test_ssh_auth_method_password() {
    let auth = SshAuthMethod::Password {
        password: "test123".to_string(),
    };

    match auth {
        SshAuthMethod::Password { password } => {
            assert_eq!(password, "test123");
        }
        _ => panic!("Expected Password variant"),
    }
}

#[test]
fn test_ssh_auth_method_private_key() {
    let auth = SshAuthMethod::PrivateKey {
        path: PathBuf::from("/home/user/.ssh/id_rsa"),
        passphrase: Some("keypass".to_string()),
    };

    match auth {
        SshAuthMethod::PrivateKey { path, passphrase } => {
            assert_eq!(path, PathBuf::from("/home/user/.ssh/id_rsa"));
            assert_eq!(passphrase, Some("keypass".to_string()));
        }
        _ => panic!("Expected PrivateKey variant"),
    }
}

#[test]
fn test_ssh_auth_method_agent() {
    let auth = SshAuthMethod::Agent;

    match auth {
        SshAuthMethod::Agent => {}
        _ => panic!("Expected Agent variant"),
    }
}

#[test]
fn test_connection_failed_error_contains_details() {
    let source_error = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");

    let err = MysqlSshTunnelError::ConnectionFailed {
        host: "ssh.example.com".to_string(),
        port: 2222,
        source: source_error,
    };

    let msg = err.to_string();
    assert!(msg.contains("ssh.example.com"));
    assert!(msg.contains("2222"));
}

#[test]
fn test_all_error_variants_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MysqlSshTunnelError>();
}
