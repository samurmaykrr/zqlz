//! Tests for TLS Configuration Types

use super::*;
use std::path::PathBuf;

#[test]
fn test_tls_config_creation_disabled() {
    let config = TlsConfig::disabled();

    assert_eq!(config.mode, TlsMode::Disable);
    assert!(config.ca_cert.is_none());
    assert!(config.client_cert.is_none());
    assert!(config.client_key.is_none());
}

#[test]
fn test_tls_config_creation_require() {
    let config = TlsConfig::require();

    assert_eq!(config.mode, TlsMode::Require);
    assert!(!config.mode.requires_ca_verification());
}

#[test]
fn test_tls_config_creation_verify_full() {
    let config = TlsConfig::verify_full();

    assert_eq!(config.mode, TlsMode::VerifyFull);
    assert!(config.mode.requires_ca_verification());
    assert!(config.mode.requires_hostname_verification());
}

#[test]
fn test_tls_config_with_ca_cert() {
    let config = TlsConfig::verify_full().ca_cert("/etc/ssl/certs/ca.pem");

    assert_eq!(config.mode, TlsMode::VerifyFull);
    assert_eq!(config.ca_cert, Some(PathBuf::from("/etc/ssl/certs/ca.pem")));
}

#[test]
fn test_tls_config_with_client_cert() {
    let config = TlsConfig::require().client_cert("/path/to/client.crt", "/path/to/client.key");

    assert_eq!(
        config.client_cert,
        Some(PathBuf::from("/path/to/client.crt"))
    );
    assert_eq!(
        config.client_key,
        Some(PathBuf::from("/path/to/client.key"))
    );
}

#[test]
fn test_tls_config_with_all_certs() {
    let config = TlsConfig::verify_full()
        .ca_cert("/etc/ssl/ca.pem")
        .client_cert("/etc/ssl/client.crt", "/etc/ssl/client.key");

    assert_eq!(config.ca_cert, Some(PathBuf::from("/etc/ssl/ca.pem")));
    assert_eq!(
        config.client_cert,
        Some(PathBuf::from("/etc/ssl/client.crt"))
    );
    assert_eq!(
        config.client_key,
        Some(PathBuf::from("/etc/ssl/client.key"))
    );
}

#[test]
fn test_tls_config_validation_verify_ca_without_cert() {
    let config = TlsConfig::new(TlsMode::VerifyCa);
    let result = config.validate();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("requires CA certificate"));
}

#[test]
fn test_tls_config_validation_verify_full_without_cert() {
    let config = TlsConfig::new(TlsMode::VerifyFull);
    let result = config.validate();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("requires CA certificate"));
}

#[test]
fn test_tls_config_validation_empty_ca_cert_path() {
    let config = TlsConfig::require().ca_cert("");
    let result = config.validate();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("CA certificate path cannot be empty")
    );
}

#[test]
fn test_tls_config_validation_empty_client_cert_path() {
    let mut config = TlsConfig::require();
    config.client_cert = Some(PathBuf::from(""));
    config.client_key = Some(PathBuf::from("/path/to/key"));
    let result = config.validate();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string()
            .contains("Client certificate path cannot be empty")
    );
}

#[test]
fn test_tls_config_validation_empty_client_key_path() {
    let mut config = TlsConfig::require();
    config.client_cert = Some(PathBuf::from("/path/to/cert"));
    config.client_key = Some(PathBuf::from(""));
    let result = config.validate();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Client key path cannot be empty"));
}

#[test]
fn test_tls_config_validation_cert_without_key() {
    let mut config = TlsConfig::require();
    config.client_cert = Some(PathBuf::from("/path/to/cert"));
    let result = config.validate();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("client key is missing"));
}

#[test]
fn test_tls_config_validation_key_without_cert() {
    let mut config = TlsConfig::require();
    config.client_key = Some(PathBuf::from("/path/to/key"));
    let result = config.validate();

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("client certificate is missing"));
}

#[test]
fn test_tls_mode_serialization() {
    let modes = vec![
        (TlsMode::Disable, "\"disable\""),
        (TlsMode::Allow, "\"allow\""),
        (TlsMode::Prefer, "\"prefer\""),
        (TlsMode::Require, "\"require\""),
        (TlsMode::VerifyCa, "\"verify_ca\""),
        (TlsMode::VerifyFull, "\"verify_full\""),
    ];

    for (mode, expected_json) in modes {
        let json = serde_json::to_string(&mode).unwrap();
        assert_eq!(json, expected_json);

        let deserialized: TlsMode = serde_json::from_str(&json).unwrap();
        assert_eq!(mode, deserialized);
    }
}

#[test]
fn test_tls_config_full_serialization() {
    let config = TlsConfig::verify_full()
        .ca_cert("/etc/ssl/ca.pem")
        .client_cert("/etc/ssl/client.crt", "/etc/ssl/client.key")
        .verify_server(true);

    let json = serde_json::to_string(&config).unwrap();
    let deserialized: TlsConfig = serde_json::from_str(&json).unwrap();

    assert_eq!(config.mode, deserialized.mode);
    assert_eq!(config.ca_cert, deserialized.ca_cert);
    assert_eq!(config.client_cert, deserialized.client_cert);
    assert_eq!(config.client_key, deserialized.client_key);
    assert_eq!(config.verify_server, deserialized.verify_server);
}

#[test]
fn test_tls_config_disable_mode_validation() {
    let config = TlsConfig::disabled();
    assert!(config.validate().is_ok());
}

#[test]
fn test_tls_config_allow_mode_validation() {
    let config = TlsConfig::new(TlsMode::Allow);
    assert!(config.validate().is_ok());
}

#[test]
fn test_tls_config_prefer_mode_validation() {
    let config = TlsConfig::new(TlsMode::Prefer);
    assert!(config.validate().is_ok());
}

#[test]
fn test_tls_config_require_mode_validation() {
    let config = TlsConfig::require();
    assert!(config.validate().is_ok());
}

#[test]
fn test_tls_config_verify_ca_with_cert_validation() {
    let config = TlsConfig::new(TlsMode::VerifyCa).ca_cert("/etc/ssl/ca.pem");
    assert!(config.validate().is_ok());
}

#[test]
fn test_tls_config_verify_full_with_cert_validation() {
    let config = TlsConfig::verify_full().ca_cert("/etc/ssl/ca.pem");
    assert!(config.validate().is_ok());
}

#[test]
fn test_tls_mode_requires_encryption() {
    assert!(!TlsMode::Disable.requires_encryption());
    assert!(!TlsMode::Allow.requires_encryption());
    assert!(!TlsMode::Prefer.requires_encryption());
    assert!(TlsMode::Require.requires_encryption());
    assert!(TlsMode::VerifyCa.requires_encryption());
    assert!(TlsMode::VerifyFull.requires_encryption());
}

#[test]
fn test_tls_mode_requires_ca_verification() {
    assert!(!TlsMode::Disable.requires_ca_verification());
    assert!(!TlsMode::Allow.requires_ca_verification());
    assert!(!TlsMode::Prefer.requires_ca_verification());
    assert!(!TlsMode::Require.requires_ca_verification());
    assert!(TlsMode::VerifyCa.requires_ca_verification());
    assert!(TlsMode::VerifyFull.requires_ca_verification());
}

#[test]
fn test_tls_mode_requires_hostname_verification() {
    assert!(!TlsMode::Disable.requires_hostname_verification());
    assert!(!TlsMode::Allow.requires_hostname_verification());
    assert!(!TlsMode::Prefer.requires_hostname_verification());
    assert!(!TlsMode::Require.requires_hostname_verification());
    assert!(!TlsMode::VerifyCa.requires_hostname_verification());
    assert!(TlsMode::VerifyFull.requires_hostname_verification());
}

#[test]
fn test_tls_mode_default() {
    let mode: TlsMode = Default::default();
    assert_eq!(mode, TlsMode::Allow);
}

#[test]
fn test_tls_config_default() {
    let config: TlsConfig = Default::default();
    assert_eq!(config.mode, TlsMode::Allow);
    assert!(config.ca_cert.is_none());
    assert!(config.client_cert.is_none());
    assert!(config.client_key.is_none());
    assert!(config.verify_server);
}
