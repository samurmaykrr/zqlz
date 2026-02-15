//! Tests for PostgreSQL TLS support

use super::*;
use std::io::Write;
use tempfile::NamedTempFile;
use zqlz_core::security::{TlsConfig, TlsMode};

// A minimal self-signed CA certificate for testing (PEM format)
// This is NOT a real certificate - just a valid PEM structure for parsing tests
const TEST_CA_CERT_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAKHBfpGqoAAAMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl
c3RjYTAeFw0yNDAxMDEwMDAwMDBaFw0yNTAxMDEwMDAwMDBaMBExDzANBgNVBAMM
BnRlc3RjYTBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQC6XiH1j5L8X7r9C6H9b0x5
Yd5Q5B5Q5B5Q5B5Q5B5Q5B5Q5B5Q5B5Q5B5Q5B5Q5B5Q5B5Q5B5Q5B5Q5B5Q5B5Q
AgMBAAGjUDBOMB0GA1UdDgQWBBQExample0HAdExample1MA8GA1UdEwEB/wQFMAMB
Af8wCwYDVR0PBAQDAgGGMA0GCSqGSIb3DQEBCwUAA0EAExample==
-----END CERTIFICATE-----
";

fn create_temp_file_with_content(content: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(content).unwrap();
    file.flush().unwrap();
    file
}

// =============================================================================
// TLS Mode Conversion Tests
// =============================================================================

#[test]
fn test_tls_mode_to_sslmode_disable() {
    assert_eq!(tls_mode_to_sslmode(TlsMode::Disable), "disable");
}

#[test]
fn test_tls_mode_to_sslmode_allow() {
    assert_eq!(tls_mode_to_sslmode(TlsMode::Allow), "allow");
}

#[test]
fn test_tls_mode_to_sslmode_prefer() {
    assert_eq!(tls_mode_to_sslmode(TlsMode::Prefer), "prefer");
}

#[test]
fn test_tls_mode_to_sslmode_require() {
    assert_eq!(tls_mode_to_sslmode(TlsMode::Require), "require");
}

#[test]
fn test_tls_mode_to_sslmode_verify_ca() {
    assert_eq!(tls_mode_to_sslmode(TlsMode::VerifyCa), "verify-ca");
}

#[test]
fn test_tls_mode_to_sslmode_verify_full() {
    assert_eq!(tls_mode_to_sslmode(TlsMode::VerifyFull), "verify-full");
}

// =============================================================================
// Build TLS Params Tests
// =============================================================================

#[test]
fn test_build_tls_params_disable_mode() {
    let config = TlsConfig::disabled();
    let params = build_tls_params(&config);
    assert!(params.contains("sslmode=disable"));
}

#[test]
fn test_build_tls_params_require_mode() {
    let config = TlsConfig::require();
    let params = build_tls_params(&config);
    assert!(params.contains("sslmode=require"));
}

#[test]
fn test_build_tls_params_verify_full_mode() {
    let config = TlsConfig::verify_full().ca_cert("/path/to/ca.crt");
    let params = build_tls_params(&config);
    assert!(params.contains("sslmode=verify-full"));
    assert!(params.contains("sslrootcert=/path/to/ca.crt"));
}

#[test]
fn test_build_tls_params_with_client_cert() {
    let config = TlsConfig::require()
        .ca_cert("/path/to/ca.crt")
        .client_cert("/path/to/client.crt", "/path/to/client.key");

    let params = build_tls_params(&config);
    assert!(params.contains("sslrootcert=/path/to/ca.crt"));
    assert!(params.contains("sslcert=/path/to/client.crt"));
    assert!(params.contains("sslkey=/path/to/client.key"));
}

#[test]
fn test_build_tls_params_no_certs() {
    let config = TlsConfig::new(TlsMode::Prefer);
    let params = build_tls_params(&config);
    assert_eq!(params, "sslmode=prefer");
}

// =============================================================================
// PostgresTlsConnector Tests
// =============================================================================

#[test]
fn test_is_required_for_require_mode() {
    let config = TlsConfig::require();
    assert!(PostgresTlsConnector::is_required(&config));
}

#[test]
fn test_is_required_for_verify_ca_mode() {
    let config = TlsConfig::new(TlsMode::VerifyCa).ca_cert("/path/to/ca.crt");
    assert!(PostgresTlsConnector::is_required(&config));
}

#[test]
fn test_is_required_for_verify_full_mode() {
    let config = TlsConfig::verify_full().ca_cert("/path/to/ca.crt");
    assert!(PostgresTlsConnector::is_required(&config));
}

#[test]
fn test_is_not_required_for_allow_mode() {
    let config = TlsConfig::new(TlsMode::Allow);
    assert!(!PostgresTlsConnector::is_required(&config));
}

#[test]
fn test_is_not_required_for_prefer_mode() {
    let config = TlsConfig::new(TlsMode::Prefer);
    assert!(!PostgresTlsConnector::is_required(&config));
}

#[test]
fn test_is_not_required_for_disable_mode() {
    let config = TlsConfig::disabled();
    assert!(!PostgresTlsConnector::is_required(&config));
}

#[test]
fn test_is_disabled_for_disable_mode() {
    let config = TlsConfig::disabled();
    assert!(PostgresTlsConnector::is_disabled(&config));
}

#[test]
fn test_is_not_disabled_for_require_mode() {
    let config = TlsConfig::require();
    assert!(!PostgresTlsConnector::is_disabled(&config));
}

// =============================================================================
// Build Connector Tests
// =============================================================================

#[test]
fn test_build_connector_fails_for_disable_mode() {
    let config = TlsConfig::disabled();
    let result = PostgresTlsConnector::build(&config);
    assert!(result.is_err());

    let err_msg = format!("{:?}", result.err().unwrap());
    assert!(err_msg.contains("not supported"));
}

#[test]
fn test_build_connector_require_mode_no_certs() {
    let config = TlsConfig::require().verify_server(false);
    let result = PostgresTlsConnector::build(&config);
    assert!(result.is_ok());
}

#[test]
fn test_build_connector_prefer_mode_no_certs() {
    let config = TlsConfig::new(TlsMode::Prefer);
    let result = PostgresTlsConnector::build(&config);
    assert!(result.is_ok());
}

#[test]
fn test_build_connector_allow_mode_no_certs() {
    let config = TlsConfig::new(TlsMode::Allow);
    let result = PostgresTlsConnector::build(&config);
    assert!(result.is_ok());
}

#[test]
fn test_build_connector_verify_ca_requires_ca_cert() {
    let config = TlsConfig::new(TlsMode::VerifyCa);
    let result = PostgresTlsConnector::build(&config);
    assert!(result.is_err());
}

#[test]
fn test_build_connector_verify_full_requires_ca_cert() {
    let config = TlsConfig::verify_full();
    let result = PostgresTlsConnector::build(&config);
    assert!(result.is_err());
}

#[test]
fn test_build_insecure_connector() {
    let result = PostgresTlsConnector::build_insecure();
    assert!(result.is_ok());
}

// =============================================================================
// CA Certificate Loading Tests
// =============================================================================

#[test]
fn test_apply_ca_cert_file_not_found() {
    let mut builder = NativeTlsConnector::builder();
    let path = Path::new("/nonexistent/path/to/ca.crt");
    let result = apply_ca_cert(&mut builder, path);
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Failed to load CA certificate"));
}

#[test]
fn test_apply_ca_cert_invalid_format() {
    let temp_file = create_temp_file_with_content(b"not a valid certificate");
    let mut builder = NativeTlsConnector::builder();
    let result = apply_ca_cert(&mut builder, temp_file.path());
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Invalid CA certificate"));
}

// =============================================================================
// Client Certificate Loading Tests
// =============================================================================

#[test]
fn test_apply_client_cert_file_not_found() {
    let mut builder = NativeTlsConnector::builder();
    let cert_path = Path::new("/nonexistent/client.crt");
    let key_path = Path::new("/nonexistent/client.key");
    let result = apply_client_cert(&mut builder, cert_path, key_path);
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Failed to load client certificate"));
}

#[test]
fn test_apply_client_cert_invalid_key() {
    let cert_file = create_temp_file_with_content(TEST_CA_CERT_PEM);
    let key_file = create_temp_file_with_content(b"not a valid key");

    let mut builder = NativeTlsConnector::builder();
    let result = apply_client_cert(&mut builder, cert_file.path(), key_file.path());
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Invalid client identity"));
}

// =============================================================================
// Configuration Verification Tests
// =============================================================================

#[test]
fn test_configure_verification_require_with_verify_false() {
    let mut builder = NativeTlsConnector::builder();
    let config = TlsConfig::require().verify_server(false);
    let result = configure_verification(&mut builder, &config);
    assert!(result.is_ok());
}

#[test]
fn test_configure_verification_verify_ca() {
    let mut builder = NativeTlsConnector::builder();
    let config = TlsConfig::new(TlsMode::VerifyCa).ca_cert("/path/to/ca.crt");
    let result = configure_verification(&mut builder, &config);
    assert!(result.is_ok());
}

#[test]
fn test_configure_verification_verify_full() {
    let mut builder = NativeTlsConnector::builder();
    let config = TlsConfig::verify_full().ca_cert("/path/to/ca.crt");
    let result = configure_verification(&mut builder, &config);
    assert!(result.is_ok());
}

#[test]
fn test_configure_verification_disable_fails() {
    let mut builder = NativeTlsConnector::builder();
    let config = TlsConfig::disabled();
    let result = configure_verification(&mut builder, &config);
    assert!(result.is_err());
}

// =============================================================================
// Error Type Tests
// =============================================================================

#[test]
fn test_tls_error_ca_cert_load_failed_display() {
    let err = TlsError::CaCertLoadFailed {
        path: "/path/to/ca.crt".to_string(),
        source: std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"),
    };
    let msg = err.to_string();
    assert!(msg.contains("Failed to load CA certificate"));
    assert!(msg.contains("/path/to/ca.crt"));
}

#[test]
fn test_tls_error_invalid_ca_cert_display() {
    let err = TlsError::InvalidCaCert("bad format".to_string());
    let msg = err.to_string();
    assert!(msg.contains("Invalid CA certificate format"));
}

#[test]
fn test_tls_error_unsupported_mode_display() {
    let err = TlsError::UnsupportedMode {
        mode: TlsMode::Disable,
    };
    let msg = err.to_string();
    assert!(msg.contains("not supported"));
}

#[test]
fn test_tls_error_client_cert_load_failed_display() {
    let err = TlsError::ClientCertLoadFailed {
        path: "/path/to/client.crt".to_string(),
        source: std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"),
    };
    let msg = err.to_string();
    assert!(msg.contains("Failed to load client certificate"));
}

#[test]
fn test_tls_error_client_key_load_failed_display() {
    let err = TlsError::ClientKeyLoadFailed {
        path: "/path/to/client.key".to_string(),
        source: std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"),
    };
    let msg = err.to_string();
    assert!(msg.contains("Failed to load client key"));
}

#[test]
fn test_tls_error_invalid_client_identity_display() {
    let err = TlsError::InvalidClientIdentity("invalid pkcs8".to_string());
    let msg = err.to_string();
    assert!(msg.contains("Invalid client identity"));
}

#[test]
fn test_tls_error_configuration_error_display() {
    let err = TlsError::ConfigurationError("something went wrong".to_string());
    let msg = err.to_string();
    assert!(msg.contains("TLS configuration error"));
}
