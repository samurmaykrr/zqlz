//! Tests for SSH Tunnel Configuration Types

use super::*;
use std::path::PathBuf;

#[test]
fn test_ssh_config_creation_with_password() {
    let config = SshTunnelConfig::with_password("bastion.example.com", "admin", "secret123");

    assert_eq!(config.host, "bastion.example.com");
    assert_eq!(config.port, 22);
    assert_eq!(config.username, "admin");
    assert_eq!(config.timeout_seconds, 30);
    assert!(matches!(
        config.auth,
        SshAuthMethod::Password { password } if password == "secret123"
    ));
}

#[test]
fn test_ssh_config_creation_with_private_key() {
    let config =
        SshTunnelConfig::with_private_key("bastion.example.com", "admin", "/home/user/.ssh/id_rsa");

    assert_eq!(config.host, "bastion.example.com");
    assert_eq!(config.port, 22);
    assert_eq!(config.username, "admin");
    assert!(matches!(
        config.auth,
        SshAuthMethod::PrivateKey { path, passphrase } if path == PathBuf::from("/home/user/.ssh/id_rsa") && passphrase.is_none()
    ));
}

#[test]
fn test_ssh_config_creation_with_agent() {
    let config = SshTunnelConfig::with_agent("bastion.example.com", "admin");

    assert_eq!(config.host, "bastion.example.com");
    assert!(matches!(config.auth, SshAuthMethod::Agent));
}

#[test]
fn test_ssh_config_validation_empty_host() {
    let config = SshTunnelConfig::with_password("", "admin", "secret");
    let result = config.validate();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("host cannot be empty"));
}

#[test]
fn test_ssh_config_validation_invalid_port() {
    let mut config = SshTunnelConfig::with_password("bastion.example.com", "admin", "secret");
    config.port = 0;
    let result = config.validate();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("port cannot be 0"));
}

#[test]
fn test_ssh_config_validation_empty_username() {
    let config = SshTunnelConfig::with_password("bastion.example.com", "", "secret");
    let result = config.validate();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("username cannot be empty"));
}

#[test]
fn test_ssh_config_validation_empty_private_key_path() {
    let config = SshTunnelConfig::with_private_key("bastion.example.com", "admin", "");
    let result = config.validate();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("private key path cannot be empty"));
}

#[test]
fn test_ssh_config_builder_methods() {
    let config = SshTunnelConfig::with_agent("bastion.example.com", "admin")
        .port(2222)
        .timeout(60)
        .keepalive(30);

    assert_eq!(config.port, 2222);
    assert_eq!(config.timeout_seconds, 60);
    assert_eq!(config.keepalive_seconds, 30);
}

#[test]
fn test_ssh_auth_password_serialization() {
    let auth = SshAuthMethod::password("mysecret");
    let json = serde_json::to_string(&auth).unwrap();
    let deserialized: SshAuthMethod = serde_json::from_str(&json).unwrap();

    assert_eq!(auth, deserialized);
    assert!(json.contains("\"type\":\"password\""));
}

#[test]
fn test_ssh_auth_private_key_serialization() {
    let auth = SshAuthMethod::private_key_with_passphrase("/path/to/key", "keypass");
    let json = serde_json::to_string(&auth).unwrap();
    let deserialized: SshAuthMethod = serde_json::from_str(&json).unwrap();

    assert_eq!(auth, deserialized);
    assert!(json.contains("\"type\":\"private_key\""));
}

#[test]
fn test_ssh_auth_agent_serialization() {
    let auth = SshAuthMethod::agent();
    let json = serde_json::to_string(&auth).unwrap();
    let deserialized: SshAuthMethod = serde_json::from_str(&json).unwrap();

    assert_eq!(auth, deserialized);
    assert!(json.contains("\"type\":\"agent\""));
}

#[test]
fn test_ssh_config_full_serialization() {
    let config = SshTunnelConfig::with_password("bastion.example.com", "admin", "secret")
        .port(2222)
        .timeout(45)
        .keepalive(15);

    let json = serde_json::to_string(&config).unwrap();
    let deserialized: SshTunnelConfig = serde_json::from_str(&json).unwrap();

    assert_eq!(config.host, deserialized.host);
    assert_eq!(config.port, deserialized.port);
    assert_eq!(config.username, deserialized.username);
    assert_eq!(config.timeout_seconds, deserialized.timeout_seconds);
    assert_eq!(config.keepalive_seconds, deserialized.keepalive_seconds);
}

#[test]
fn test_ssh_config_valid_configuration() {
    let config = SshTunnelConfig::with_password("bastion.example.com", "admin", "secret");
    assert!(config.validate().is_ok());

    let config = SshTunnelConfig::with_private_key("bastion.example.com", "admin", "/path/to/key");
    assert!(config.validate().is_ok());

    let config = SshTunnelConfig::with_agent("bastion.example.com", "admin");
    assert!(config.validate().is_ok());
}
