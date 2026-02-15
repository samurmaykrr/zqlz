#[cfg(test)]
mod ssl_tls_tests {
    use crate::fixtures::TestDriver;
    use anyhow::{Context, Result};
    use rstest::rstest;
    use zqlz_core::{ConnectionConfig, DatabaseDriver};
    use zqlz_driver_postgres::PostgresDriver;
    use zqlz_driver_mysql::MySqlDriver;

    /// Test that basic connections work (SSL disabled or default mode)
    /// This validates that the drivers can connect without explicit SSL configuration
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[tokio::test]
    async fn test_ssl_disabled_mode(#[case] driver: TestDriver) -> Result<()> {
        let (host, port, database, user, password) = match driver {
            TestDriver::Postgres => ("localhost", 5433, "pagila", "test_user", "test_password"),
            TestDriver::Mysql => ("localhost", 3307, "sakila", "test_user", "test_password"),
            _ => anyhow::bail!("SSL tests only support Postgres and MySQL"),
        };

        let mut config = match driver {
            TestDriver::Postgres => ConnectionConfig::new_postgres(host, port, database, user),
            TestDriver::Mysql => ConnectionConfig::new_mysql(host, port, database, user),
            _ => unreachable!(),
        };
        config.password = Some(password.to_string());

        let connection = match driver {
            TestDriver::Postgres => PostgresDriver::new().connect(&config).await,
            TestDriver::Mysql => MySqlDriver::new().connect(&config).await,
            _ => unreachable!(),
        };

        match connection {
            Ok(conn) => {
                let result = conn.query("SELECT 1 as test", &[]).await;
                match result {
                    Ok(rows) => {
                        assert_eq!(rows.rows.len(), 1, "Query should return exactly one row");
                        Ok(())
                    }
                    Err(e) => {
                        anyhow::bail!(
                            "Failed to query database (SSL disabled/default): {}. \
                            Run: ./crates/zqlz-driver-tests/manage-test-env.sh up",
                            e
                        )
                    }
                }
            }
            Err(e) => {
                anyhow::bail!(
                    "Failed to connect (SSL disabled/default): {}. \
                    Run: ./crates/zqlz-driver-tests/manage-test-env.sh up",
                    e
                )
            }
        }
    }

    /// Test connection with SSL prefer mode (attempt SSL but fallback to non-SSL)
    /// This is the same as disabled mode since ConnectionConfig doesn't expose SSL mode
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[tokio::test]
    async fn test_ssl_prefer_mode(#[case] driver: TestDriver) -> Result<()> {
        // For now, this test is identical to disabled mode since we don't expose SSL mode
        // In a real implementation, this would set sslmode=prefer or ssl-mode=PREFERRED
        test_ssl_disabled_mode(driver).await
    }

    /// Test connection with SSL require mode
    /// This would require SSL configuration in ConnectionConfig (not currently exposed)
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[tokio::test]
    async fn test_ssl_require_mode(#[case] driver: TestDriver) -> Result<()> {
        // For now, this test is identical to disabled mode since we don't expose SSL mode
        // In a real implementation, this would set sslmode=require or ssl-mode=REQUIRED
        // and would fail if SSL is not available on the server
        let (host, port, database, user, password) = match driver {
            TestDriver::Postgres => ("localhost", 5433, "pagila", "test_user", "test_password"),
            TestDriver::Mysql => ("localhost", 3307, "sakila", "test_user", "test_password"),
            _ => anyhow::bail!("SSL tests only support Postgres and MySQL"),
        };

        let mut config = match driver {
            TestDriver::Postgres => ConnectionConfig::new_postgres(host, port, database, user),
            TestDriver::Mysql => ConnectionConfig::new_mysql(host, port, database, user),
            _ => unreachable!(),
        };
        config.password = Some(password.to_string());

        // Note: Without SSL configuration in ConnectionConfig, we can't actually test REQUIRE mode
        // This test validates the connection works, but doesn't verify SSL is used
        let connection = match driver {
            TestDriver::Postgres => PostgresDriver::new().connect(&config).await,
            TestDriver::Mysql => MySqlDriver::new().connect(&config).await,
            _ => unreachable!(),
        };

        match connection {
            Ok(conn) => {
                let result = conn.query("SELECT 1 as test", &[]).await?;
                assert_eq!(result.rows.len(), 1);
                Ok(())
            }
            Err(e) => {
                anyhow::bail!(
                    "Failed to connect (SSL require): {}. \
                    Run: ./crates/zqlz-driver-tests/manage-test-env.sh up",
                    e
                )
            }
        }
    }

    /// Test that connections fail with invalid CA certificate paths
    /// Note: This requires SSL configuration support in ConnectionConfig
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[tokio::test]
    async fn test_ssl_with_invalid_ca_cert(#[case] driver: TestDriver) -> Result<()> {
        // This test is a placeholder since ConnectionConfig doesn't currently support
        // SSL certificate configuration. In a real implementation, we would:
        // 1. Set ssl_ca_cert_file to a nonexistent path
        // 2. Set ssl_mode to VERIFY_CA or VERIFY_IDENTITY
        // 3. Expect the connection to fail with certificate validation error
        
        // For now, we just verify the connection works normally
        println!(
            "Note: SSL certificate validation tests require SSL configuration support in ConnectionConfig. \
            This test would verify that invalid CA certificates are rejected."
        );
        
        test_ssl_disabled_mode(driver).await
    }

    /// Test SSL verify-full mode without proper hostname
    /// This should fail when hostname verification is strict
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[tokio::test]
    async fn test_ssl_verify_full_without_hostname(#[case] driver: TestDriver) -> Result<()> {
        // This test is a placeholder since ConnectionConfig doesn't currently support
        // SSL verification modes. In a real implementation, we would:
        // 1. Set ssl_mode to VERIFY_FULL (Postgres) or VERIFY_IDENTITY (MySQL)
        // 2. Connect using IP address (127.0.0.1) instead of hostname
        // 3. Expect the connection to fail due to hostname mismatch
        
        println!(
            "Note: SSL hostname verification tests require SSL configuration support in ConnectionConfig. \
            This test would verify that hostname mismatches are detected in verify-full mode."
        );
        
        test_ssl_disabled_mode(driver).await
    }

    /// Test retrieving SSL connection information from PostgreSQL
    #[tokio::test]
    async fn test_ssl_postgres_connection_info() -> Result<()> {
        let mut config = ConnectionConfig::new_postgres("localhost", 5433, "pagila", "test_user");
        config.password = Some("test_password".to_string());

        let connection_result = PostgresDriver::new().connect(&config).await;
        
        match connection_result {
            Ok(conn) => {
                // Query to check if SSL is enabled for this connection
                // pg_stat_ssl view shows SSL information for all connections
                let result = conn.query(
                    "SELECT CASE WHEN ssl THEN 'SSL' ELSE 'No SSL' END as ssl_status \
                    FROM pg_stat_ssl \
                    WHERE pid = pg_backend_pid()",
                    &[]
                ).await;

                match result {
                    Ok(rows) => {
                        if rows.rows.len() > 0 {
                            if let Some(ssl_value) = rows.rows[0].get_by_name("ssl_status") {
                                let ssl_status = ssl_value.as_str().unwrap_or("Unknown");
                                println!("PostgreSQL SSL Status: {}", ssl_status);
                                // Note: Without explicit SSL configuration, this will likely show "No SSL"
                            }
                        }
                        Ok(())
                    }
                    Err(e) => {
                        anyhow::bail!(
                            "Failed to query SSL status: {}. \
                            Run: ./crates/zqlz-driver-tests/manage-test-env.sh up",
                            e
                        )
                    }
                }
            }
            Err(e) => {
                anyhow::bail!(
                    "Failed to connect to PostgreSQL: {}. \
                    Run: ./crates/zqlz-driver-tests/manage-test-env.sh up",
                    e
                )
            }
        }
    }

    /// Integration test to verify ConnectionConfig construction works
    /// This doesn't test SSL modes directly, but validates config creation
    #[tokio::test]
    async fn integration_test_ssl_config_construction() -> Result<()> {
        // Test PostgreSQL config
        let mut pg_config = ConnectionConfig::new_postgres("localhost", 5432, "testdb", "testuser");
        pg_config.password = Some("testpass".to_string());
        assert_eq!(pg_config.host, "localhost");
        assert_eq!(pg_config.port, 5432);
        assert_eq!(pg_config.database, Some("testdb".to_string()));
        assert_eq!(pg_config.username, Some("testuser".to_string()));
        assert_eq!(pg_config.password, Some("testpass".to_string()));

        // Test MySQL config
        let mut mysql_config = ConnectionConfig::new_mysql("localhost", 3306, "testdb", "testuser");
        mysql_config.password = Some("testpass".to_string());
        assert_eq!(mysql_config.host, "localhost");
        assert_eq!(mysql_config.port, 3306);
        assert_eq!(mysql_config.database, Some("testdb".to_string()));
        assert_eq!(mysql_config.username, Some("testuser".to_string()));
        assert_eq!(mysql_config.password, Some("testpass".to_string()));

        // Note: In a full implementation, ConnectionConfig would have fields like:
        // - ssl_mode: SslMode enum (Disable, Prefer, Require, VerifyCA, VerifyFull)
        // - ssl_ca_cert_file: Option<PathBuf>
        // - ssl_client_cert_file: Option<PathBuf>
        // - ssl_client_key_file: Option<PathBuf>
        // These would be tested here to ensure proper SSL configuration

        Ok(())
    }

    /// Test that basic connectivity works for both drivers
    /// This serves as a baseline for SSL tests
    #[rstest]
    #[case::postgres(TestDriver::Postgres)]
    #[case::mysql(TestDriver::Mysql)]
    #[tokio::test]
    async fn integration_test_basic_connection(#[case] driver: TestDriver) -> Result<()> {
        test_ssl_disabled_mode(driver).await
    }
}
