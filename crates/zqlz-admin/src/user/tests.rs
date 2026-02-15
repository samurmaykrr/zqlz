//! Tests for user management service

use super::*;

// ============================================================================
// UserSpec Tests
// ============================================================================

mod user_spec_tests {
    use super::*;

    #[test]
    fn test_new_user_spec() {
        let spec = UserSpec::new("test_user");
        assert_eq!(spec.name(), "test_user");
        assert!(spec.can_login());
        assert!(!spec.is_superuser());
        assert!(!spec.can_create_db());
        assert!(!spec.can_create_role());
        assert!(spec.inherits());
        assert!(!spec.can_replicate());
        assert!(spec.password().is_none());
        assert!(spec.connection_limit().is_none());
        assert!(spec.valid_until().is_none());
        assert!(spec.roles().is_empty());
        assert!(spec.comment().is_none());
    }

    #[test]
    fn test_user_spec_with_password() {
        let spec = UserSpec::new("user1").with_password("secret123");
        assert_eq!(spec.password(), Some("secret123"));
        assert!(!spec.is_password_hashed());
    }

    #[test]
    fn test_user_spec_with_hashed_password() {
        let spec = UserSpec::new("user1").with_hashed_password("SCRAM-SHA-256$4096:abc");
        assert_eq!(spec.password(), Some("SCRAM-SHA-256$4096:abc"));
        assert!(spec.is_password_hashed());
    }

    #[test]
    fn test_user_spec_with_all_options() {
        let spec = UserSpec::new("admin")
            .with_password("admin_pass")
            .with_login(true)
            .with_superuser(true)
            .with_create_db(true)
            .with_create_role(true)
            .with_inherit(false)
            .with_replication(true)
            .with_connection_limit(10)
            .with_valid_until("2025-12-31")
            .with_roles(vec!["role1".to_string(), "role2".to_string()])
            .with_comment("Admin user");

        assert!(spec.is_superuser());
        assert!(spec.can_create_db());
        assert!(spec.can_create_role());
        assert!(!spec.inherits());
        assert!(spec.can_replicate());
        assert_eq!(spec.connection_limit(), Some(10));
        assert_eq!(spec.valid_until(), Some("2025-12-31"));
        assert_eq!(spec.roles().len(), 2);
        assert_eq!(spec.comment(), Some("Admin user"));
    }

    #[test]
    fn test_user_spec_with_role() {
        let spec = UserSpec::new("user1")
            .with_role("admin")
            .with_role("developer");
        assert_eq!(spec.roles(), &["admin", "developer"]);
    }
}

// ============================================================================
// UserDialect Tests
// ============================================================================

mod user_dialect_tests {
    use super::*;

    #[test]
    fn test_postgresql_dialect_capabilities() {
        let dialect = UserDialect::PostgreSQL;
        assert!(dialect.supports_superuser());
        assert!(dialect.supports_create_role());
        assert!(dialect.supports_connection_limit());
        assert!(dialect.supports_password_expiration());
        assert!(dialect.supports_inheritance());
        assert!(dialect.supports_replication());
        assert!(!dialect.separates_login_and_user());
    }

    #[test]
    fn test_mysql_dialect_capabilities() {
        let dialect = UserDialect::MySQL;
        assert!(!dialect.supports_superuser());
        assert!(!dialect.supports_create_role());
        assert!(dialect.supports_connection_limit());
        assert!(dialect.supports_password_expiration());
        assert!(!dialect.supports_inheritance());
        assert!(!dialect.supports_replication());
        assert!(!dialect.separates_login_and_user());
    }

    #[test]
    fn test_mssql_dialect_capabilities() {
        let dialect = UserDialect::MsSql;
        assert!(!dialect.supports_superuser());
        assert!(!dialect.supports_create_role());
        assert!(!dialect.supports_connection_limit());
        assert!(!dialect.supports_password_expiration());
        assert!(!dialect.supports_inheritance());
        assert!(!dialect.supports_replication());
        assert!(dialect.separates_login_and_user());
    }
}

// ============================================================================
// Validation Tests
// ============================================================================

mod validation_tests {
    use super::*;

    #[test]
    fn test_validate_empty_name() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("");
        let result = service.validate(&spec);
        assert_eq!(result, Err(UserError::EmptyName));
    }

    #[test]
    fn test_validate_whitespace_name() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("   ");
        let result = service.validate(&spec);
        assert_eq!(result, Err(UserError::EmptyName));
    }

    #[test]
    fn test_validate_superuser_not_supported() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let spec = UserSpec::new("user1").with_superuser(true);
        let result = service.validate(&spec);
        assert_eq!(
            result,
            Err(UserError::NotSupported("SUPERUSER".to_string()))
        );
    }

    #[test]
    fn test_validate_replication_not_supported() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let spec = UserSpec::new("user1").with_replication(true);
        let result = service.validate(&spec);
        assert_eq!(
            result,
            Err(UserError::NotSupported("REPLICATION".to_string()))
        );
    }

    #[test]
    fn test_validate_invalid_connection_limit() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("user1").with_connection_limit(-5);
        let result = service.validate(&spec);
        assert_eq!(result, Err(UserError::InvalidConnectionLimit));
    }

    #[test]
    fn test_validate_valid_unlimited_connections() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("user1").with_connection_limit(-1);
        let result = service.validate(&spec);
        assert!(result.is_ok());
    }
}

// ============================================================================
// PostgreSQL CREATE USER Tests
// ============================================================================

mod postgres_create_user_tests {
    use super::*;

    #[test]
    fn test_simple_user() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("app_user");
        let sql = service.build_create_user(&spec).unwrap();
        assert_eq!(sql, "CREATE ROLE app_user WITH LOGIN");
    }

    #[test]
    fn test_user_with_password() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("app_user").with_password("secret");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("CREATE ROLE app_user"));
        assert!(sql.contains("LOGIN"));
        assert!(sql.contains("PASSWORD 'secret'"));
    }

    #[test]
    fn test_user_with_special_password() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("app_user").with_password("pass'word");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("PASSWORD 'pass''word'"));
    }

    #[test]
    fn test_superuser() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("admin")
            .with_superuser(true)
            .with_password("admin");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("SUPERUSER"));
    }

    #[test]
    fn test_user_no_login() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("role_only").with_login(false);
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("NOLOGIN"));
        assert!(!sql.contains(" LOGIN"));
    }

    #[test]
    fn test_user_with_all_options() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("admin")
            .with_password("secret")
            .with_superuser(true)
            .with_create_db(true)
            .with_create_role(true)
            .with_inherit(false)
            .with_replication(true)
            .with_connection_limit(5)
            .with_valid_until("2025-12-31");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("SUPERUSER"));
        assert!(sql.contains("CREATEDB"));
        assert!(sql.contains("CREATEROLE"));
        assert!(sql.contains("NOINHERIT"));
        assert!(sql.contains("REPLICATION"));
        assert!(sql.contains("CONNECTION LIMIT 5"));
        assert!(sql.contains("VALID UNTIL '2025-12-31'"));
    }

    #[test]
    fn test_user_with_roles() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("app_user")
            .with_password("secret")
            .with_role("admin")
            .with_role("developer");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("CREATE ROLE app_user"));
        assert!(sql.contains("GRANT admin TO app_user"));
        assert!(sql.contains("GRANT developer TO app_user"));
    }
}

// ============================================================================
// MySQL CREATE USER Tests
// ============================================================================

mod mysql_create_user_tests {
    use super::*;

    #[test]
    fn test_simple_user() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let spec = UserSpec::new("app_user");
        let sql = service.build_create_user(&spec).unwrap();
        assert_eq!(sql, "CREATE USER app_user");
    }

    #[test]
    fn test_user_with_password() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let spec = UserSpec::new("app_user").with_password("secret");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("CREATE USER app_user"));
        assert!(sql.contains("IDENTIFIED BY 'secret'"));
    }

    #[test]
    fn test_user_with_connection_limit() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let spec = UserSpec::new("app_user")
            .with_password("secret")
            .with_connection_limit(100);
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("MAX_CONNECTIONS_PER_HOUR 100"));
    }

    #[test]
    fn test_user_locked() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let spec = UserSpec::new("disabled_user")
            .with_password("secret")
            .with_login(false);
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("ACCOUNT LOCK"));
    }

    #[test]
    fn test_user_with_roles() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let spec = UserSpec::new("app_user")
            .with_password("secret")
            .with_role("admin");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("GRANT admin TO app_user"));
    }
}

// ============================================================================
// MS SQL CREATE USER Tests
// ============================================================================

mod mssql_create_user_tests {
    use super::*;

    #[test]
    fn test_user_requires_password() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let spec = UserSpec::new("app_user");
        let result = service.build_create_user(&spec);
        assert_eq!(result, Err(UserError::PasswordRequired));
    }

    #[test]
    fn test_simple_user() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let spec = UserSpec::new("app_user").with_password("Secret123!");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("CREATE LOGIN app_user"));
        assert!(sql.contains("WITH PASSWORD = 'Secret123!'"));
    }

    #[test]
    fn test_user_disabled() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let spec = UserSpec::new("app_user")
            .with_password("Secret123!")
            .with_login(false);
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("ALTER LOGIN app_user DISABLE"));
    }

    #[test]
    fn test_user_with_server_roles() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let spec = UserSpec::new("app_user")
            .with_password("Secret123!")
            .with_role("sysadmin");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("ALTER SERVER ROLE sysadmin ADD MEMBER app_user"));
    }
}

// ============================================================================
// DROP USER Tests
// ============================================================================

mod drop_user_tests {
    use super::*;

    #[test]
    fn test_postgres_drop_user() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let sql = service.build_drop_user("app_user", false);
        assert_eq!(sql, "DROP ROLE app_user");
    }

    #[test]
    fn test_postgres_drop_user_if_exists() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let sql = service.build_drop_user("app_user", true);
        assert_eq!(sql, "DROP ROLE IF EXISTS app_user");
    }

    #[test]
    fn test_mysql_drop_user() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let sql = service.build_drop_user("app_user", false);
        assert_eq!(sql, "DROP USER app_user");
    }

    #[test]
    fn test_mysql_drop_user_if_exists() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let sql = service.build_drop_user("app_user", true);
        assert_eq!(sql, "DROP USER IF EXISTS app_user");
    }

    #[test]
    fn test_mssql_drop_user() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let sql = service.build_drop_user("app_user", false);
        assert_eq!(sql, "DROP LOGIN app_user");
    }

    #[test]
    fn test_mssql_drop_user_if_exists() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let sql = service.build_drop_user("app_user", true);
        assert!(sql.contains("IF EXISTS"));
        assert!(sql.contains("sys.server_principals"));
        assert!(sql.contains("DROP LOGIN app_user"));
    }
}

// ============================================================================
// ALTER PASSWORD Tests
// ============================================================================

mod alter_password_tests {
    use super::*;

    #[test]
    fn test_postgres_alter_password() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let sql = service.build_alter_password("app_user", "new_secret", false);
        assert_eq!(sql, "ALTER ROLE app_user WITH PASSWORD 'new_secret'");
    }

    #[test]
    fn test_mysql_alter_password() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let sql = service.build_alter_password("app_user", "new_secret", false);
        assert_eq!(sql, "ALTER USER app_user IDENTIFIED BY 'new_secret'");
    }

    #[test]
    fn test_mssql_alter_password() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let sql = service.build_alter_password("app_user", "new_secret", false);
        assert_eq!(sql, "ALTER LOGIN app_user WITH PASSWORD = 'new_secret'");
    }
}

// ============================================================================
// RENAME USER Tests
// ============================================================================

mod rename_user_tests {
    use super::*;

    #[test]
    fn test_postgres_rename_user() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let sql = service.build_rename_user("old_user", "new_user");
        assert_eq!(sql, "ALTER ROLE old_user RENAME TO new_user");
    }

    #[test]
    fn test_mysql_rename_user() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let sql = service.build_rename_user("old_user", "new_user");
        assert_eq!(sql, "RENAME USER old_user TO new_user");
    }

    #[test]
    fn test_mssql_rename_user() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let sql = service.build_rename_user("old_user", "new_user");
        assert_eq!(sql, "ALTER LOGIN old_user WITH NAME = new_user");
    }
}

// ============================================================================
// GRANT/REVOKE ROLE Tests
// ============================================================================

mod grant_revoke_tests {
    use super::*;

    #[test]
    fn test_postgres_grant_role() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let sql = service.build_grant_role("app_user", "admin");
        assert_eq!(sql, "GRANT admin TO app_user");
    }

    #[test]
    fn test_postgres_revoke_role() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let sql = service.build_revoke_role("app_user", "admin");
        assert_eq!(sql, "REVOKE admin FROM app_user");
    }

    #[test]
    fn test_mysql_grant_role() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let sql = service.build_grant_role("app_user", "admin");
        assert_eq!(sql, "GRANT admin TO app_user");
    }

    #[test]
    fn test_mysql_revoke_role() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let sql = service.build_revoke_role("app_user", "admin");
        assert_eq!(sql, "REVOKE admin FROM app_user");
    }

    #[test]
    fn test_mssql_grant_role() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let sql = service.build_grant_role("app_user", "db_owner");
        assert_eq!(sql, "ALTER ROLE db_owner ADD MEMBER app_user");
    }

    #[test]
    fn test_mssql_revoke_role() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let sql = service.build_revoke_role("app_user", "db_owner");
        assert_eq!(sql, "ALTER ROLE db_owner DROP MEMBER app_user");
    }
}

// ============================================================================
// LIST USERS Query Tests
// ============================================================================

mod list_users_tests {
    use super::*;

    #[test]
    fn test_postgres_list_users() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let sql = service.build_list_users_query();
        assert!(sql.contains("pg_catalog.pg_roles"));
        assert!(sql.contains("rolname"));
        assert!(sql.contains("NOT LIKE 'pg_%'"));
    }

    #[test]
    fn test_mysql_list_users() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let sql = service.build_list_users_query();
        assert!(sql.contains("mysql.user"));
        assert!(sql.contains("User AS name"));
    }

    #[test]
    fn test_mssql_list_users() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let sql = service.build_list_users_query();
        assert!(sql.contains("sys.server_principals"));
        assert!(sql.contains("type IN ('S', 'U', 'G')"));
    }
}

// ============================================================================
// COMMENT Tests
// ============================================================================

mod comment_tests {
    use super::*;

    #[test]
    fn test_postgres_comment() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let sql = service.build_comment("app_user", Some("Application user"));
        assert_eq!(
            sql,
            Some("COMMENT ON ROLE app_user IS 'Application user'".to_string())
        );
    }

    #[test]
    fn test_postgres_clear_comment() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let sql = service.build_comment("app_user", None);
        assert_eq!(sql, Some("COMMENT ON ROLE app_user IS NULL".to_string()));
    }

    #[test]
    fn test_mysql_comment_not_supported() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let sql = service.build_comment("app_user", Some("Comment"));
        assert!(sql.is_none());
    }

    #[test]
    fn test_mssql_comment_not_supported() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let sql = service.build_comment("app_user", Some("Comment"));
        assert!(sql.is_none());
    }
}

// ============================================================================
// ALTER LOGIN Tests
// ============================================================================

mod alter_login_tests {
    use super::*;

    #[test]
    fn test_postgres_enable_login() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let sql = service.build_alter_login("app_user", true);
        assert_eq!(sql, "ALTER ROLE app_user WITH LOGIN");
    }

    #[test]
    fn test_postgres_disable_login() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let sql = service.build_alter_login("app_user", false);
        assert_eq!(sql, "ALTER ROLE app_user WITH NOLOGIN");
    }

    #[test]
    fn test_mysql_enable_login() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let sql = service.build_alter_login("app_user", true);
        assert_eq!(sql, "ALTER USER app_user ACCOUNT UNLOCK");
    }

    #[test]
    fn test_mysql_disable_login() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let sql = service.build_alter_login("app_user", false);
        assert_eq!(sql, "ALTER USER app_user ACCOUNT LOCK");
    }

    #[test]
    fn test_mssql_enable_login() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let sql = service.build_alter_login("app_user", true);
        assert_eq!(sql, "ALTER LOGIN app_user ENABLE");
    }

    #[test]
    fn test_mssql_disable_login() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let sql = service.build_alter_login("app_user", false);
        assert_eq!(sql, "ALTER LOGIN app_user DISABLE");
    }
}

// ============================================================================
// Identifier Quoting Tests
// ============================================================================

mod quoting_tests {
    use super::*;

    #[test]
    fn test_postgres_quote_reserved_word() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("user").with_password("pass");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("\"user\""));
    }

    #[test]
    fn test_mysql_quote_reserved_word() {
        let service = UserManagementService::new(UserDialect::MySQL);
        let spec = UserSpec::new("select").with_password("pass");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("`select`"));
    }

    #[test]
    fn test_mssql_quote_reserved_word() {
        let service = UserManagementService::new(UserDialect::MsSql);
        let spec = UserSpec::new("select").with_password("pass");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains("[select]"));
    }

    #[test]
    fn test_no_quote_normal_name() {
        let service = UserManagementService::new(UserDialect::PostgreSQL);
        let spec = UserSpec::new("app_user").with_password("pass");
        let sql = service.build_create_user(&spec).unwrap();
        assert!(sql.contains(" app_user ") || sql.contains(" app_user\n"));
    }
}

// ============================================================================
// Error Display Tests
// ============================================================================

mod error_display_tests {
    use super::*;

    #[test]
    fn test_empty_name_error_display() {
        let err = UserError::EmptyName;
        assert_eq!(err.to_string(), "User name cannot be empty");
    }

    #[test]
    fn test_password_required_error_display() {
        let err = UserError::PasswordRequired;
        assert_eq!(err.to_string(), "Password is required");
    }

    #[test]
    fn test_not_supported_error_display() {
        let err = UserError::NotSupported("SUPERUSER".to_string());
        assert_eq!(
            err.to_string(),
            "SUPERUSER is not supported by this dialect"
        );
    }

    #[test]
    fn test_invalid_connection_limit_error_display() {
        let err = UserError::InvalidConnectionLimit;
        assert_eq!(err.to_string(), "Connection limit must be -1 or positive");
    }
}
