//! Tests for role management service

use super::*;

// ============================================================================
// RoleSpec Tests
// ============================================================================

mod role_spec_tests {
    use super::*;

    #[test]
    fn test_new_role_spec() {
        let spec = RoleSpec::new("read_only");
        assert_eq!(spec.name(), "read_only");
        assert!(spec.inherits());
        assert!(spec.member_of().is_empty());
        assert!(spec.members().is_empty());
        assert!(spec.admin_members().is_empty());
        assert!(spec.comment().is_none());
    }

    #[test]
    fn test_role_spec_with_members() {
        let spec = RoleSpec::new("dev_team")
            .with_member("alice")
            .with_member("bob");
        assert_eq!(spec.members(), &["alice", "bob"]);
    }

    #[test]
    fn test_role_spec_with_member_of() {
        let spec = RoleSpec::new("junior_dev")
            .in_role("dev_team")
            .in_role("read_only");
        assert_eq!(spec.member_of(), &["dev_team", "read_only"]);
    }

    #[test]
    fn test_role_spec_with_admin_members() {
        let spec = RoleSpec::new("team_lead").with_admin_member("manager");
        assert_eq!(spec.admin_members(), &["manager"]);
    }

    #[test]
    fn test_role_spec_with_inherit() {
        let spec = RoleSpec::new("special").with_inherit(false);
        assert!(!spec.inherits());
    }

    #[test]
    fn test_role_spec_with_comment() {
        let spec = RoleSpec::new("audit").with_comment("Audit access role");
        assert_eq!(spec.comment(), Some("Audit access role"));
    }
}

// ============================================================================
// Privilege Tests
// ============================================================================

mod privilege_tests {
    use super::*;

    #[test]
    fn test_privilege_as_sql() {
        assert_eq!(Privilege::Select.as_sql(), "SELECT");
        assert_eq!(Privilege::Insert.as_sql(), "INSERT");
        assert_eq!(Privilege::Update.as_sql(), "UPDATE");
        assert_eq!(Privilege::Delete.as_sql(), "DELETE");
        assert_eq!(Privilege::Truncate.as_sql(), "TRUNCATE");
        assert_eq!(Privilege::Execute.as_sql(), "EXECUTE");
        assert_eq!(Privilege::Usage.as_sql(), "USAGE");
        assert_eq!(Privilege::Create.as_sql(), "CREATE");
        assert_eq!(Privilege::All.as_sql(), "ALL PRIVILEGES");
    }
}

// ============================================================================
// ObjectType Tests
// ============================================================================

mod object_type_tests {
    use super::*;

    #[test]
    fn test_object_type_keyword() {
        assert_eq!(ObjectType::Table("users".into()).type_keyword(), "TABLE");
        assert_eq!(ObjectType::Schema("public".into()).type_keyword(), "SCHEMA");
        assert_eq!(
            ObjectType::Sequence("seq".into()).type_keyword(),
            "SEQUENCE"
        );
        assert_eq!(ObjectType::Function("fn".into()).type_keyword(), "FUNCTION");
        assert_eq!(
            ObjectType::AllTablesInSchema("public".into()).type_keyword(),
            "ALL TABLES IN SCHEMA"
        );
    }

    #[test]
    fn test_object_type_name() {
        assert_eq!(ObjectType::Table("users".into()).name(), "users");
        assert_eq!(ObjectType::Schema("public".into()).name(), "public");
    }
}

// ============================================================================
// RoleDialect Tests
// ============================================================================

mod role_dialect_tests {
    use super::*;

    #[test]
    fn test_postgresql_dialect_capabilities() {
        let dialect = RoleDialect::PostgreSQL;
        assert!(dialect.supports_inherit());
        assert!(dialect.supports_admin_option());
        assert!(dialect.supports_schema_grants());
        assert!(dialect.supports_grant_option());
        assert!(dialect.supports_all_in_schema());
    }

    #[test]
    fn test_mysql_dialect_capabilities() {
        let dialect = RoleDialect::MySQL;
        assert!(!dialect.supports_inherit());
        assert!(dialect.supports_admin_option());
        assert!(!dialect.supports_schema_grants());
        assert!(dialect.supports_grant_option());
        assert!(!dialect.supports_all_in_schema());
    }

    #[test]
    fn test_mssql_dialect_capabilities() {
        let dialect = RoleDialect::MsSql;
        assert!(!dialect.supports_inherit());
        assert!(!dialect.supports_admin_option());
        assert!(dialect.supports_schema_grants());
        assert!(dialect.supports_grant_option());
        assert!(!dialect.supports_all_in_schema());
    }
}

// ============================================================================
// Validation Tests
// ============================================================================

mod validation_tests {
    use super::*;

    #[test]
    fn test_validate_empty_name() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let spec = RoleSpec::new("");
        let result = service.validate(&spec);
        assert_eq!(result, Err(RoleError::EmptyName));
    }

    #[test]
    fn test_validate_whitespace_name() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let spec = RoleSpec::new("   ");
        let result = service.validate(&spec);
        assert_eq!(result, Err(RoleError::EmptyName));
    }

    #[test]
    fn test_validate_noinherit_not_supported() {
        let service = RoleManagementService::new(RoleDialect::MySQL);
        let spec = RoleSpec::new("role1").with_inherit(false);
        let result = service.validate(&spec);
        assert_eq!(
            result,
            Err(RoleError::NotSupported("NOINHERIT".to_string()))
        );
    }

    #[test]
    fn test_validate_admin_option_not_supported() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let spec = RoleSpec::new("role1").with_admin_member("admin");
        let result = service.validate(&spec);
        assert_eq!(
            result,
            Err(RoleError::NotSupported("WITH ADMIN OPTION".to_string()))
        );
    }

    #[test]
    fn test_validate_valid_spec() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let spec = RoleSpec::new("valid_role");
        let result = service.validate(&spec);
        assert!(result.is_ok());
    }
}

// ============================================================================
// PostgreSQL CREATE ROLE Tests
// ============================================================================

mod postgres_create_role_tests {
    use super::*;

    #[test]
    fn test_simple_role() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let spec = RoleSpec::new("read_only");
        let sql = service.build_create_role(&spec).unwrap();
        assert_eq!(sql, "CREATE ROLE read_only WITH NOLOGIN");
    }

    #[test]
    fn test_role_with_noinherit() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let spec = RoleSpec::new("special").with_inherit(false);
        let sql = service.build_create_role(&spec).unwrap();
        assert!(sql.contains("NOINHERIT"));
    }

    #[test]
    fn test_role_with_members() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let spec = RoleSpec::new("dev_team")
            .with_member("alice")
            .with_member("bob");
        let sql = service.build_create_role(&spec).unwrap();
        assert!(sql.contains("CREATE ROLE dev_team"));
        assert!(sql.contains("GRANT dev_team TO alice"));
        assert!(sql.contains("GRANT dev_team TO bob"));
    }

    #[test]
    fn test_role_with_member_of() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let spec = RoleSpec::new("junior").in_role("dev_team");
        let sql = service.build_create_role(&spec).unwrap();
        assert!(sql.contains("CREATE ROLE junior"));
        assert!(sql.contains("GRANT dev_team TO junior"));
    }

    #[test]
    fn test_role_with_admin_members() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let spec = RoleSpec::new("team").with_admin_member("manager");
        let sql = service.build_create_role(&spec).unwrap();
        assert!(sql.contains("WITH ADMIN OPTION"));
    }
}

// ============================================================================
// MySQL CREATE ROLE Tests
// ============================================================================

mod mysql_create_role_tests {
    use super::*;

    #[test]
    fn test_simple_role() {
        let service = RoleManagementService::new(RoleDialect::MySQL);
        let spec = RoleSpec::new("read_only");
        let sql = service.build_create_role(&spec).unwrap();
        assert_eq!(sql, "CREATE ROLE read_only");
    }

    #[test]
    fn test_role_with_members() {
        let service = RoleManagementService::new(RoleDialect::MySQL);
        let spec = RoleSpec::new("dev_team").with_member("alice");
        let sql = service.build_create_role(&spec).unwrap();
        assert!(sql.contains("CREATE ROLE dev_team"));
        assert!(sql.contains("GRANT dev_team TO alice"));
    }
}

// ============================================================================
// MS SQL Server CREATE ROLE Tests
// ============================================================================

mod mssql_create_role_tests {
    use super::*;

    #[test]
    fn test_simple_role() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let spec = RoleSpec::new("read_only");
        let sql = service.build_create_role(&spec).unwrap();
        assert_eq!(sql, "CREATE ROLE read_only");
    }

    #[test]
    fn test_role_with_members() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let spec = RoleSpec::new("dev_team").with_member("alice");
        let sql = service.build_create_role(&spec).unwrap();
        assert!(sql.contains("CREATE ROLE dev_team"));
        assert!(sql.contains("ALTER ROLE dev_team ADD MEMBER alice"));
    }
}

// ============================================================================
// DROP ROLE Tests
// ============================================================================

mod drop_role_tests {
    use super::*;

    #[test]
    fn test_postgres_drop_role() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_drop_role("old_role", false);
        assert_eq!(sql, "DROP ROLE old_role");
    }

    #[test]
    fn test_postgres_drop_role_if_exists() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_drop_role("old_role", true);
        assert_eq!(sql, "DROP ROLE IF EXISTS old_role");
    }

    #[test]
    fn test_mysql_drop_role() {
        let service = RoleManagementService::new(RoleDialect::MySQL);
        let sql = service.build_drop_role("old_role", true);
        assert_eq!(sql, "DROP ROLE IF EXISTS old_role");
    }

    #[test]
    fn test_mssql_drop_role() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let sql = service.build_drop_role("old_role", false);
        assert_eq!(sql, "DROP ROLE old_role");
    }

    #[test]
    fn test_mssql_drop_role_if_exists() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let sql = service.build_drop_role("old_role", true);
        assert!(sql.contains("IF EXISTS"));
        assert!(sql.contains("DROP ROLE old_role"));
    }
}

// ============================================================================
// GRANT Privileges Tests
// ============================================================================

mod grant_privileges_tests {
    use super::*;

    #[test]
    fn test_grant_select_on_table() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service
            .build_grant_privileges(
                &[Privilege::Select],
                &ObjectType::Table("users".into()),
                "read_only",
                false,
            )
            .unwrap();
        assert_eq!(sql, "GRANT SELECT ON TABLE users TO read_only");
    }

    #[test]
    fn test_grant_multiple_privileges() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service
            .build_grant_privileges(
                &[Privilege::Select, Privilege::Insert, Privilege::Update],
                &ObjectType::Table("users".into()),
                "app_user",
                false,
            )
            .unwrap();
        assert_eq!(
            sql,
            "GRANT SELECT, INSERT, UPDATE ON TABLE users TO app_user"
        );
    }

    #[test]
    fn test_grant_with_grant_option() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service
            .build_grant_privileges(
                &[Privilege::Select],
                &ObjectType::Table("users".into()),
                "admin",
                true,
            )
            .unwrap();
        assert!(sql.contains("WITH GRANT OPTION"));
    }

    #[test]
    fn test_grant_all_tables_in_schema() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service
            .build_grant_privileges(
                &[Privilege::Select],
                &ObjectType::AllTablesInSchema("public".into()),
                "read_only",
                false,
            )
            .unwrap();
        // "public" is a reserved keyword, so it gets quoted
        assert!(sql.contains("ALL TABLES IN SCHEMA \"public\""));
    }

    #[test]
    fn test_grant_execute_on_function() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service
            .build_grant_privileges(
                &[Privilege::Execute],
                &ObjectType::Function("my_func()".into()),
                "app_user",
                false,
            )
            .unwrap();
        assert!(sql.contains("EXECUTE"));
        assert!(sql.contains("FUNCTION"));
    }

    #[test]
    fn test_grant_usage_on_schema() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service
            .build_grant_privileges(
                &[Privilege::Usage],
                &ObjectType::Schema("app_schema".into()),
                "app_user",
                false,
            )
            .unwrap();
        assert_eq!(sql, "GRANT USAGE ON SCHEMA app_schema TO app_user");
    }

    #[test]
    fn test_grant_no_privileges_error() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let result =
            service.build_grant_privileges(&[], &ObjectType::Table("users".into()), "role", false);
        assert_eq!(result, Err(RoleError::NoPrivileges));
    }

    #[test]
    fn test_grant_empty_object_error() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let result = service.build_grant_privileges(
            &[Privilege::Select],
            &ObjectType::Table("".into()),
            "role",
            false,
        );
        assert_eq!(result, Err(RoleError::EmptyObjectName));
    }
}

// ============================================================================
// REVOKE Privileges Tests
// ============================================================================

mod revoke_privileges_tests {
    use super::*;

    #[test]
    fn test_revoke_select_on_table() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service
            .build_revoke_privileges(
                &[Privilege::Select],
                &ObjectType::Table("users".into()),
                "old_role",
                false,
            )
            .unwrap();
        assert_eq!(sql, "REVOKE SELECT ON TABLE users FROM old_role");
    }

    #[test]
    fn test_revoke_with_cascade() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service
            .build_revoke_privileges(
                &[Privilege::All],
                &ObjectType::Table("users".into()),
                "old_role",
                true,
            )
            .unwrap();
        assert!(sql.contains("CASCADE"));
    }

    #[test]
    fn test_revoke_no_cascade_for_mysql() {
        let service = RoleManagementService::new(RoleDialect::MySQL);
        let sql = service
            .build_revoke_privileges(
                &[Privilege::Select],
                &ObjectType::Table("users".into()),
                "old_role",
                true, // cascade ignored for MySQL
            )
            .unwrap();
        assert!(!sql.contains("CASCADE"));
    }
}

// ============================================================================
// GRANT/REVOKE Role Membership Tests
// ============================================================================

mod role_membership_tests {
    use super::*;

    #[test]
    fn test_grant_role_postgres() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_grant_role("dev_team", "alice", false);
        assert_eq!(sql, "GRANT dev_team TO alice");
    }

    #[test]
    fn test_grant_role_with_admin_option() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_grant_role("dev_team", "manager", true);
        assert_eq!(sql, "GRANT dev_team TO manager WITH ADMIN OPTION");
    }

    #[test]
    fn test_grant_role_mssql() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let sql = service.build_grant_role("dev_team", "alice", false);
        assert_eq!(sql, "ALTER ROLE dev_team ADD MEMBER alice");
    }

    #[test]
    fn test_revoke_role_postgres() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_revoke_role("dev_team", "alice");
        assert_eq!(sql, "REVOKE dev_team FROM alice");
    }

    #[test]
    fn test_revoke_role_mssql() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let sql = service.build_revoke_role("dev_team", "alice");
        assert_eq!(sql, "ALTER ROLE dev_team DROP MEMBER alice");
    }
}

// ============================================================================
// List Roles Query Tests
// ============================================================================

mod list_roles_tests {
    use super::*;

    #[test]
    fn test_postgres_list_roles_query() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_list_roles_query();
        assert!(sql.contains("pg_catalog.pg_roles"));
        assert!(sql.contains("rolname"));
    }

    #[test]
    fn test_mysql_list_roles_query() {
        let service = RoleManagementService::new(RoleDialect::MySQL);
        let sql = service.build_list_roles_query();
        assert!(sql.contains("mysql.user"));
    }

    #[test]
    fn test_mssql_list_roles_query() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let sql = service.build_list_roles_query();
        assert!(sql.contains("sys.database_principals"));
        assert!(sql.contains("type = 'R'"));
    }
}

// ============================================================================
// List Role Members Query Tests
// ============================================================================

mod list_role_members_tests {
    use super::*;

    #[test]
    fn test_postgres_list_members_query() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_list_role_members_query("dev_team");
        assert!(sql.contains("pg_auth_members"));
        assert!(sql.contains("dev_team"));
    }

    #[test]
    fn test_mysql_list_members_query() {
        let service = RoleManagementService::new(RoleDialect::MySQL);
        let sql = service.build_list_role_members_query("dev_team");
        assert!(sql.contains("role_edges"));
        assert!(sql.contains("dev_team"));
    }

    #[test]
    fn test_mssql_list_members_query() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let sql = service.build_list_role_members_query("dev_team");
        assert!(sql.contains("database_role_members"));
        assert!(sql.contains("dev_team"));
    }
}

// ============================================================================
// Comment Tests
// ============================================================================

mod comment_tests {
    use super::*;

    #[test]
    fn test_postgres_comment() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_comment("dev_team", Some("Development team role"));
        assert_eq!(
            sql,
            Some("COMMENT ON ROLE dev_team IS 'Development team role'".to_string())
        );
    }

    #[test]
    fn test_postgres_comment_null() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_comment("dev_team", None);
        assert_eq!(sql, Some("COMMENT ON ROLE dev_team IS NULL".to_string()));
    }

    #[test]
    fn test_mysql_comment_not_supported() {
        let service = RoleManagementService::new(RoleDialect::MySQL);
        let sql = service.build_comment("dev_team", Some("test"));
        assert!(sql.is_none());
    }

    #[test]
    fn test_mssql_comment_not_supported() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let sql = service.build_comment("dev_team", Some("test"));
        assert!(sql.is_none());
    }
}

// ============================================================================
// Rename Role Tests
// ============================================================================

mod rename_role_tests {
    use super::*;

    #[test]
    fn test_postgres_rename_role() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_rename_role("old_role", "new_role");
        assert_eq!(sql, "ALTER ROLE old_role RENAME TO new_role");
    }

    #[test]
    fn test_mysql_rename_role_comment() {
        let service = RoleManagementService::new(RoleDialect::MySQL);
        let sql = service.build_rename_role("old_role", "new_role");
        assert!(sql.contains("MySQL doesn't support"));
    }

    #[test]
    fn test_mssql_rename_role() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let sql = service.build_rename_role("old_role", "new_role");
        assert_eq!(sql, "ALTER ROLE old_role WITH NAME = new_role");
    }
}

// ============================================================================
// Alter Default Privileges Tests
// ============================================================================

mod alter_default_privileges_tests {
    use super::*;

    #[test]
    fn test_postgres_alter_default_privileges() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service
            .build_alter_default_privileges(
                &[Privilege::Select, Privilege::Insert],
                "TABLES",
                "public",
                "app_user",
            )
            .unwrap();
        assert!(sql.is_some());
        let sql = sql.unwrap();
        assert!(sql.contains("ALTER DEFAULT PRIVILEGES"));
        // "public" is a reserved keyword, so it gets quoted
        assert!(sql.contains("IN SCHEMA \"public\""));
        assert!(sql.contains("TABLES"));
        assert!(sql.contains("TO app_user"));
    }

    #[test]
    fn test_mysql_alter_default_privileges_not_supported() {
        let service = RoleManagementService::new(RoleDialect::MySQL);
        let result = service.build_alter_default_privileges(
            &[Privilege::Select],
            "TABLES",
            "public",
            "app_user",
        );
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_alter_default_no_privileges_error() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let result = service.build_alter_default_privileges(&[], "TABLES", "public", "app_user");
        assert_eq!(result, Err(RoleError::NoPrivileges));
    }
}

// ============================================================================
// Privilege Validation Tests
// ============================================================================

mod privilege_validation_tests {
    use super::*;

    #[test]
    fn test_truncate_only_postgres() {
        let pg_service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let result = pg_service.build_grant_privileges(
            &[Privilege::Truncate],
            &ObjectType::Table("users".into()),
            "role",
            false,
        );
        assert!(result.is_ok());

        let mysql_service = RoleManagementService::new(RoleDialect::MySQL);
        let result = mysql_service.build_grant_privileges(
            &[Privilege::Truncate],
            &ObjectType::Table("users".into()),
            "role",
            false,
        );
        assert!(matches!(result, Err(RoleError::InvalidPrivilege(_))));
    }

    #[test]
    fn test_execute_on_function_valid() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let result = service.build_grant_privileges(
            &[Privilege::Execute],
            &ObjectType::Function("my_func".into()),
            "role",
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_on_table_invalid() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let result = service.build_grant_privileges(
            &[Privilege::Execute],
            &ObjectType::Table("users".into()),
            "role",
            false,
        );
        assert!(matches!(result, Err(RoleError::InvalidPrivilege(_))));
    }

    #[test]
    fn test_usage_on_schema_valid() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let result = service.build_grant_privileges(
            &[Privilege::Usage],
            &ObjectType::Schema("public".into()),
            "role",
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_all_privileges_valid_everywhere() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);

        let result = service.build_grant_privileges(
            &[Privilege::All],
            &ObjectType::Table("users".into()),
            "role",
            false,
        );
        assert!(result.is_ok());

        let result = service.build_grant_privileges(
            &[Privilege::All],
            &ObjectType::Schema("public".into()),
            "role",
            false,
        );
        assert!(result.is_ok());

        let result = service.build_grant_privileges(
            &[Privilege::All],
            &ObjectType::Function("fn".into()),
            "role",
            false,
        );
        assert!(result.is_ok());
    }
}

// ============================================================================
// Quoting Tests
// ============================================================================

mod quoting_tests {
    use super::*;

    #[test]
    fn test_postgres_quotes_reserved_words() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_drop_role("select", false);
        assert_eq!(sql, "DROP ROLE \"select\"");
    }

    #[test]
    fn test_mysql_quotes_reserved_words() {
        let service = RoleManagementService::new(RoleDialect::MySQL);
        let sql = service.build_drop_role("select", false);
        assert_eq!(sql, "DROP ROLE `select`");
    }

    #[test]
    fn test_mssql_quotes_reserved_words() {
        let service = RoleManagementService::new(RoleDialect::MsSql);
        let sql = service.build_drop_role("select", false);
        assert_eq!(sql, "DROP ROLE [select]");
    }

    #[test]
    fn test_no_quotes_for_simple_names() {
        let service = RoleManagementService::new(RoleDialect::PostgreSQL);
        let sql = service.build_drop_role("simple_role", false);
        assert_eq!(sql, "DROP ROLE simple_role");
    }
}

// ============================================================================
// Error Display Tests
// ============================================================================

mod error_display_tests {
    use super::*;

    #[test]
    fn test_error_display_empty_name() {
        let err = RoleError::EmptyName;
        assert_eq!(format!("{}", err), "Role name cannot be empty");
    }

    #[test]
    fn test_error_display_no_privileges() {
        let err = RoleError::NoPrivileges;
        assert_eq!(format!("{}", err), "At least one privilege is required");
    }

    #[test]
    fn test_error_display_not_supported() {
        let err = RoleError::NotSupported("NOINHERIT".to_string());
        assert_eq!(
            format!("{}", err),
            "NOINHERIT is not supported by this dialect"
        );
    }

    #[test]
    fn test_error_display_invalid_privilege() {
        let err = RoleError::InvalidPrivilege("EXECUTE on TABLE".to_string());
        assert_eq!(format!("{}", err), "Invalid privilege: EXECUTE on TABLE");
    }
}
