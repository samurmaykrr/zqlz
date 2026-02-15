//! Tests for MySQL TLS support

use super::*;
use std::io::Write;
use tempfile::NamedTempFile;
use zqlz_core::security::{TlsConfig, TlsMode};

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
fn test_tls_mode_to_ssl_mode_disabled() {
    assert_eq!(tls_mode_to_ssl_mode(TlsMode::Disable), "DISABLED");
}

#[test]
fn test_tls_mode_to_ssl_mode_allow() {
    assert_eq!(tls_mode_to_ssl_mode(TlsMode::Allow), "PREFERRED");
}

#[test]
fn test_tls_mode_to_ssl_mode_prefer() {
    assert_eq!(tls_mode_to_ssl_mode(TlsMode::Prefer), "PREFERRED");
}

#[test]
fn test_tls_mode_to_ssl_mode_require() {
    assert_eq!(tls_mode_to_ssl_mode(TlsMode::Require), "REQUIRED");
}

#[test]
fn test_tls_mode_to_ssl_mode_verify_ca() {
    assert_eq!(tls_mode_to_ssl_mode(TlsMode::VerifyCa), "VERIFY_CA");
}

#[test]
fn test_tls_mode_to_ssl_mode_verify_full() {
    assert_eq!(tls_mode_to_ssl_mode(TlsMode::VerifyFull), "VERIFY_IDENTITY");
}

// =============================================================================
// Build SSL Params Tests
// =============================================================================

#[test]
fn test_build_ssl_params_disable_mode() {
    let config = TlsConfig::disabled();
    let params = build_ssl_params(&config);
    assert!(params.contains("ssl-mode=DISABLED"));
}

#[test]
fn test_build_ssl_params_require_mode() {
    let config = TlsConfig::require();
    let params = build_ssl_params(&config);
    assert!(params.contains("ssl-mode=REQUIRED"));
}

#[test]
fn test_build_ssl_params_verify_full_mode() {
    let config = TlsConfig::verify_full().ca_cert("/path/to/ca.crt");
    let params = build_ssl_params(&config);
    assert!(params.contains("ssl-mode=VERIFY_IDENTITY"));
    assert!(params.contains("ssl-ca=/path/to/ca.crt"));
}

#[test]
fn test_build_ssl_params_with_client_cert() {
    let config = TlsConfig::require()
        .ca_cert("/path/to/ca.crt")
        .client_cert("/path/to/client.crt", "/path/to/client.key");

    let params = build_ssl_params(&config);
    assert!(params.contains("ssl-ca=/path/to/ca.crt"));
    assert!(params.contains("ssl-cert=/path/to/client.crt"));
    assert!(params.contains("ssl-key=/path/to/client.key"));
}

#[test]
fn test_build_ssl_params_no_certs() {
    let config = TlsConfig::new(TlsMode::Prefer);
    let params = build_ssl_params(&config);
    assert_eq!(params, "ssl-mode=PREFERRED");
}

// =============================================================================
// MysqlTlsConnector Tests
// =============================================================================

#[test]
fn test_is_required_for_require_mode() {
    let config = TlsConfig::require();
    assert!(MysqlTlsConnector::is_required(&config));
}

#[test]
fn test_is_required_for_verify_ca_mode() {
    let config = TlsConfig::new(TlsMode::VerifyCa).ca_cert("/path/to/ca.crt");
    assert!(MysqlTlsConnector::is_required(&config));
}

#[test]
fn test_is_required_for_verify_full_mode() {
    let config = TlsConfig::verify_full().ca_cert("/path/to/ca.crt");
    assert!(MysqlTlsConnector::is_required(&config));
}

#[test]
fn test_is_not_required_for_allow_mode() {
    let config = TlsConfig::new(TlsMode::Allow);
    assert!(!MysqlTlsConnector::is_required(&config));
}

#[test]
fn test_is_not_required_for_prefer_mode() {
    let config = TlsConfig::new(TlsMode::Prefer);
    assert!(!MysqlTlsConnector::is_required(&config));
}

#[test]
fn test_is_not_required_for_disable_mode() {
    let config = TlsConfig::disabled();
    assert!(!MysqlTlsConnector::is_required(&config));
}

#[test]
fn test_is_disabled_for_disable_mode() {
    let config = TlsConfig::disabled();
    assert!(MysqlTlsConnector::is_disabled(&config));
}

#[test]
fn test_is_not_disabled_for_require_mode() {
    let config = TlsConfig::require();
    assert!(!MysqlTlsConnector::is_disabled(&config));
}

// =============================================================================
// Build SSL Options Tests
// =============================================================================

#[test]
fn test_build_returns_none_for_disable_mode() {
    let config = TlsConfig::disabled();
    let result = MysqlTlsConnector::build(&config);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_build_returns_some_for_require_mode() {
    let config = TlsConfig::require();
    let result = MysqlTlsConnector::build(&config);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_build_returns_some_for_prefer_mode() {
    let config = TlsConfig::new(TlsMode::Prefer);
    let result = MysqlTlsConnector::build(&config);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_build_returns_some_for_allow_mode() {
    let config = TlsConfig::new(TlsMode::Allow);
    let result = MysqlTlsConnector::build(&config);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_build_verify_ca_requires_ca_cert() {
    let config = TlsConfig::new(TlsMode::VerifyCa);
    let result = MysqlTlsConnector::build(&config);
    assert!(result.is_err());
}

#[test]
fn test_build_verify_full_requires_ca_cert() {
    let config = TlsConfig::verify_full();
    let result = MysqlTlsConnector::build(&config);
    assert!(result.is_err());
}

#[test]
fn test_build_with_valid_ca_cert() {
    // Create a temp file to represent the CA cert (mysql_async checks existence)
    let ca_file = create_temp_file_with_content(b"dummy cert content");
    let config = TlsConfig::require().ca_cert(ca_file.path());
    let result = MysqlTlsConnector::build(&config);
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[test]
fn test_build_with_nonexistent_ca_cert() {
    let config = TlsConfig::require().ca_cert("/nonexistent/path/to/ca.crt");
    let result = MysqlTlsConnector::build(&config);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Failed to load CA certificate"));
}

#[test]
fn test_build_insecure() {
    let ssl_opts = MysqlTlsConnector::build_insecure();
    assert!(ssl_opts.accept_invalid_certs());
    assert!(ssl_opts.skip_domain_validation());
}

// =============================================================================
// Configure Verification Tests
// =============================================================================

#[test]
fn test_configure_verification_require_with_verify_false() {
    let ssl_opts = SslOpts::default();
    let config = TlsConfig::require().verify_server(false);
    let result = configure_verification(ssl_opts, &config);
    assert!(result.is_ok());
    let opts = result.unwrap();
    assert!(opts.accept_invalid_certs());
    assert!(opts.skip_domain_validation());
}

#[test]
fn test_configure_verification_require_with_verify_true() {
    let ssl_opts = SslOpts::default();
    let config = TlsConfig::require().verify_server(true);
    let result = configure_verification(ssl_opts, &config);
    assert!(result.is_ok());
    let opts = result.unwrap();
    // With verify_server=true but no CA cert, we still accept invalid certs
    // but may verify domain
    assert!(opts.accept_invalid_certs());
}

#[test]
fn test_configure_verification_verify_ca() {
    let ssl_opts = SslOpts::default();
    let config = TlsConfig::new(TlsMode::VerifyCa).ca_cert("/path/to/ca.crt");
    let result = configure_verification(ssl_opts, &config);
    assert!(result.is_ok());
    let opts = result.unwrap();
    // VerifyCa skips domain validation but verifies CA
    assert!(opts.skip_domain_validation());
    assert!(!opts.accept_invalid_certs());
}

#[test]
fn test_configure_verification_verify_full() {
    let ssl_opts = SslOpts::default();
    let config = TlsConfig::verify_full().ca_cert("/path/to/ca.crt");
    let result = configure_verification(ssl_opts, &config);
    assert!(result.is_ok());
    let opts = result.unwrap();
    // VerifyFull enables full verification
    assert!(!opts.skip_domain_validation());
    assert!(!opts.accept_invalid_certs());
}

// =============================================================================
// Apply CA Certificate Tests
// =============================================================================

#[test]
fn test_apply_ca_cert_file_not_found() {
    let ssl_opts = SslOpts::default();
    let path = Path::new("/nonexistent/path/to/ca.crt");
    let result = apply_ca_cert(ssl_opts, path);
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Failed to load CA certificate"));
}

#[test]
fn test_apply_ca_cert_with_existing_file() {
    let ca_file = create_temp_file_with_content(b"dummy cert content");
    let ssl_opts = SslOpts::default();
    let result = apply_ca_cert(ssl_opts, ca_file.path());
    assert!(result.is_ok());
    let opts = result.unwrap();
    // Built-in roots should be disabled when custom CA is provided
    assert!(opts.disable_built_in_roots());
}

// =============================================================================
// Error Type Tests
// =============================================================================

#[test]
fn test_mysql_tls_error_ca_cert_load_failed_display() {
    let err = MysqlTlsError::CaCertLoadFailed {
        path: "/path/to/ca.crt".to_string(),
        source: std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"),
    };
    let msg = err.to_string();
    assert!(msg.contains("Failed to load CA certificate"));
    assert!(msg.contains("/path/to/ca.crt"));
}

#[test]
fn test_mysql_tls_error_configuration_error_display() {
    let err = MysqlTlsError::ConfigurationError("something went wrong".to_string());
    let msg = err.to_string();
    assert!(msg.contains("TLS configuration error"));
}

#[test]
fn test_mysql_tls_error_unsupported_mode_display() {
    let err = MysqlTlsError::UnsupportedMode {
        mode: TlsMode::Disable,
    };
    let msg = err.to_string();
    assert!(msg.contains("not supported"));
}
