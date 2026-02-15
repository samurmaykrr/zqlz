//! Tests for Row Level Security management service

use super::*;

mod policy_command_tests {
    use super::*;

    #[test]
    fn test_policy_command_as_sql() {
        assert_eq!(PolicyCommand::Select.as_sql(), "SELECT");
        assert_eq!(PolicyCommand::Insert.as_sql(), "INSERT");
        assert_eq!(PolicyCommand::Update.as_sql(), "UPDATE");
        assert_eq!(PolicyCommand::Delete.as_sql(), "DELETE");
        assert_eq!(PolicyCommand::All.as_sql(), "ALL");
    }

    #[test]
    fn test_policy_command_is_read_only() {
        assert!(PolicyCommand::Select.is_read_only());
        assert!(!PolicyCommand::Insert.is_read_only());
        assert!(!PolicyCommand::Update.is_read_only());
        assert!(!PolicyCommand::Delete.is_read_only());
        assert!(!PolicyCommand::All.is_read_only());
    }

    #[test]
    fn test_policy_command_is_write() {
        assert!(!PolicyCommand::Select.is_write());
        assert!(PolicyCommand::Insert.is_write());
        assert!(PolicyCommand::Update.is_write());
        assert!(PolicyCommand::Delete.is_write());
        assert!(!PolicyCommand::All.is_write());
    }

    #[test]
    fn test_policy_command_default() {
        let cmd: PolicyCommand = Default::default();
        assert_eq!(cmd, PolicyCommand::All);
    }
}

mod policy_type_tests {
    use super::*;

    #[test]
    fn test_policy_type_as_sql() {
        assert_eq!(PolicyType::Permissive.as_sql(), "PERMISSIVE");
        assert_eq!(PolicyType::Restrictive.as_sql(), "RESTRICTIVE");
    }

    #[test]
    fn test_policy_type_default() {
        let pt: PolicyType = Default::default();
        assert_eq!(pt, PolicyType::Permissive);
    }
}

mod rls_policy_tests {
    use super::*;

    #[test]
    fn test_policy_new() {
        let policy = RlsPolicy::new("test_policy", "users");
        assert_eq!(policy.name(), "test_policy");
        assert_eq!(policy.table(), "users");
        assert!(policy.schema().is_none());
        assert_eq!(policy.command(), PolicyCommand::All);
        assert_eq!(policy.policy_type(), PolicyType::Permissive);
        assert!(policy.roles().is_empty());
        assert!(policy.using_expr().is_none());
        assert!(policy.check_expr().is_none());
    }

    #[test]
    fn test_policy_with_schema() {
        let policy = RlsPolicy::new("test_policy", "users").with_schema("myschema");
        assert_eq!(policy.schema(), Some("myschema"));
        assert_eq!(policy.qualified_table(), "myschema.users");
    }

    #[test]
    fn test_policy_without_schema() {
        let policy = RlsPolicy::new("test_policy", "users");
        assert_eq!(policy.qualified_table(), "users");
    }

    #[test]
    fn test_policy_with_command() {
        let policy = RlsPolicy::new("test", "t").with_command(PolicyCommand::Select);
        assert_eq!(policy.command(), PolicyCommand::Select);
    }

    #[test]
    fn test_policy_with_policy_type() {
        let policy = RlsPolicy::new("test", "t").with_policy_type(PolicyType::Restrictive);
        assert_eq!(policy.policy_type(), PolicyType::Restrictive);
    }

    #[test]
    fn test_policy_with_roles() {
        let policy =
            RlsPolicy::new("test", "t").with_roles(vec!["admin".to_string(), "user".to_string()]);
        assert_eq!(policy.roles(), &["admin", "user"]);
    }

    #[test]
    fn test_policy_for_role() {
        let policy = RlsPolicy::new("test", "t")
            .for_role("admin")
            .for_role("user");
        assert_eq!(policy.roles(), &["admin", "user"]);
    }

    #[test]
    fn test_policy_with_using() {
        let policy = RlsPolicy::new("test", "t").with_using("user_id = current_user_id()");
        assert_eq!(policy.using_expr(), Some("user_id = current_user_id()"));
    }

    #[test]
    fn test_policy_with_check() {
        let policy = RlsPolicy::new("test", "t").with_check("user_id IS NOT NULL");
        assert_eq!(policy.check_expr(), Some("user_id IS NOT NULL"));
    }

    #[test]
    fn test_policy_full_builder() {
        let policy = RlsPolicy::new("row_owner", "documents")
            .with_schema("app")
            .with_command(PolicyCommand::Update)
            .with_policy_type(PolicyType::Restrictive)
            .with_roles(vec!["editors".to_string()])
            .with_using("owner_id = current_user_id()")
            .with_check("status = 'draft'");

        assert_eq!(policy.name(), "row_owner");
        assert_eq!(policy.table(), "documents");
        assert_eq!(policy.schema(), Some("app"));
        assert_eq!(policy.qualified_table(), "app.documents");
        assert_eq!(policy.command(), PolicyCommand::Update);
        assert_eq!(policy.policy_type(), PolicyType::Restrictive);
        assert_eq!(policy.roles(), &["editors"]);
        assert_eq!(policy.using_expr(), Some("owner_id = current_user_id()"));
        assert_eq!(policy.check_expr(), Some("status = 'draft'"));
    }
}

mod rls_error_tests {
    use super::*;

    #[test]
    fn test_error_display_empty_name() {
        let err = RlsError::EmptyName;
        assert_eq!(format!("{}", err), "Policy name cannot be empty");
    }

    #[test]
    fn test_error_display_empty_table() {
        let err = RlsError::EmptyTable;
        assert_eq!(format!("{}", err), "Table name cannot be empty");
    }

    #[test]
    fn test_error_display_no_expression() {
        let err = RlsError::NoExpression;
        assert!(format!("{}", err).contains("expression is required"));
    }

    #[test]
    fn test_error_display_insert_requires_check() {
        let err = RlsError::InsertRequiresCheck;
        assert!(format!("{}", err).contains("INSERT"));
        assert!(format!("{}", err).contains("WITH CHECK"));
    }

    #[test]
    fn test_error_display_not_supported() {
        let err = RlsError::NotSupported("MySQL RLS".to_string());
        assert_eq!(format!("{}", err), "MySQL RLS is not supported");
    }
}

mod validation_tests {
    use super::*;

    #[test]
    fn test_validate_empty_name() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("", "users").with_using("true");
        assert_eq!(service.validate(&policy), Err(RlsError::EmptyName));
    }

    #[test]
    fn test_validate_whitespace_name() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("   ", "users").with_using("true");
        assert_eq!(service.validate(&policy), Err(RlsError::EmptyName));
    }

    #[test]
    fn test_validate_empty_table() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("test", "").with_using("true");
        assert_eq!(service.validate(&policy), Err(RlsError::EmptyTable));
    }

    #[test]
    fn test_validate_select_requires_using() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("test", "users").with_command(PolicyCommand::Select);
        assert_eq!(service.validate(&policy), Err(RlsError::NoExpression));
    }

    #[test]
    fn test_validate_select_rejects_check() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("test", "users")
            .with_command(PolicyCommand::Select)
            .with_using("true")
            .with_check("true");
        assert_eq!(
            service.validate(&policy),
            Err(RlsError::SelectDeleteNoCheck)
        );
    }

    #[test]
    fn test_validate_delete_rejects_check() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("test", "users")
            .with_command(PolicyCommand::Delete)
            .with_using("true")
            .with_check("true");
        assert_eq!(
            service.validate(&policy),
            Err(RlsError::SelectDeleteNoCheck)
        );
    }

    #[test]
    fn test_validate_insert_requires_check() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("test", "users")
            .with_command(PolicyCommand::Insert)
            .with_using("true"); // USING alone not enough for INSERT
        assert_eq!(
            service.validate(&policy),
            Err(RlsError::InsertRequiresCheck)
        );
    }

    #[test]
    fn test_validate_insert_with_check_ok() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("test", "users")
            .with_command(PolicyCommand::Insert)
            .with_check("user_id IS NOT NULL");
        assert!(service.validate(&policy).is_ok());
    }

    #[test]
    fn test_validate_update_can_use_both() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("test", "users")
            .with_command(PolicyCommand::Update)
            .with_using("owner = current_user")
            .with_check("status != 'locked'");
        assert!(service.validate(&policy).is_ok());
    }

    #[test]
    fn test_validate_all_command_needs_expression() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("test", "users").with_command(PolicyCommand::All);
        assert_eq!(service.validate(&policy), Err(RlsError::NoExpression));
    }

    #[test]
    fn test_validate_empty_using_expression() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("test", "users")
            .with_command(PolicyCommand::Select)
            .with_using("   ");
        assert_eq!(service.validate(&policy), Err(RlsError::EmptyExpression));
    }

    #[test]
    fn test_validate_empty_check_expression() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("test", "users")
            .with_command(PolicyCommand::Insert)
            .with_check("  ");
        assert_eq!(service.validate(&policy), Err(RlsError::EmptyExpression));
    }
}

mod enable_disable_rls_tests {
    use super::*;

    #[test]
    fn test_enable_rls_simple() {
        let service = RlsService::new();
        let sql = service.build_enable_rls("users", None);
        assert_eq!(sql, "ALTER TABLE users ENABLE ROW LEVEL SECURITY");
    }

    #[test]
    fn test_enable_rls_with_schema() {
        let service = RlsService::new();
        let sql = service.build_enable_rls("users", Some("app"));
        assert_eq!(sql, "ALTER TABLE app.users ENABLE ROW LEVEL SECURITY");
    }

    #[test]
    fn test_disable_rls_simple() {
        let service = RlsService::new();
        let sql = service.build_disable_rls("users", None);
        assert_eq!(sql, "ALTER TABLE users DISABLE ROW LEVEL SECURITY");
    }

    #[test]
    fn test_disable_rls_with_schema() {
        let service = RlsService::new();
        let sql = service.build_disable_rls("users", Some("app"));
        assert_eq!(sql, "ALTER TABLE app.users DISABLE ROW LEVEL SECURITY");
    }

    #[test]
    fn test_force_rls() {
        let service = RlsService::new();
        let sql = service.build_force_rls("users", None);
        assert_eq!(sql, "ALTER TABLE users FORCE ROW LEVEL SECURITY");
    }

    #[test]
    fn test_no_force_rls() {
        let service = RlsService::new();
        let sql = service.build_no_force_rls("users", None);
        assert_eq!(sql, "ALTER TABLE users NO FORCE ROW LEVEL SECURITY");
    }

    #[test]
    fn test_force_rls_reserved_keyword_table() {
        let service = RlsService::new();
        let sql = service.build_force_rls("select", Some("user"));
        assert_eq!(
            sql,
            "ALTER TABLE \"user\".\"select\" FORCE ROW LEVEL SECURITY"
        );
    }
}

mod create_policy_tests {
    use super::*;

    #[test]
    fn test_create_policy_simple_select() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("user_read", "users")
            .with_command(PolicyCommand::Select)
            .with_using("user_id = current_user_id()");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains("CREATE POLICY user_read ON users"));
        assert!(sql.contains("FOR SELECT"));
        assert!(sql.contains("TO PUBLIC"));
        assert!(sql.contains("USING (user_id = current_user_id())"));
        assert!(!sql.contains("WITH CHECK"));
    }

    #[test]
    fn test_create_policy_insert_with_check() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("user_insert", "users")
            .with_command(PolicyCommand::Insert)
            .with_check("user_id IS NOT NULL");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains("CREATE POLICY user_insert ON users"));
        assert!(sql.contains("FOR INSERT"));
        assert!(sql.contains("WITH CHECK (user_id IS NOT NULL)"));
        assert!(!sql.contains("USING"));
    }

    #[test]
    fn test_create_policy_update_both_expressions() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("user_update", "users")
            .with_command(PolicyCommand::Update)
            .with_using("owner_id = current_user_id()")
            .with_check("status = 'active'");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains("FOR UPDATE"));
        assert!(sql.contains("USING (owner_id = current_user_id())"));
        assert!(sql.contains("WITH CHECK (status = 'active')"));
    }

    #[test]
    fn test_create_policy_all_command_omits_for() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("all_access", "users")
            .with_command(PolicyCommand::All)
            .with_using("true");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(!sql.contains("FOR ALL")); // ALL is default, omitted
        assert!(!sql.contains("FOR SELECT"));
        assert!(sql.contains("USING (true)"));
    }

    #[test]
    fn test_create_policy_restrictive() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("restrict", "secrets")
            .with_policy_type(PolicyType::Restrictive)
            .with_command(PolicyCommand::Select)
            .with_using("clearance_level >= required_level");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains("AS RESTRICTIVE"));
    }

    #[test]
    fn test_create_policy_permissive_omits_as() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("permissive", "data")
            .with_policy_type(PolicyType::Permissive)
            .with_command(PolicyCommand::Select)
            .with_using("true");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(!sql.contains("AS PERMISSIVE")); // Default, omitted
    }

    #[test]
    fn test_create_policy_with_roles() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("admin_only", "config")
            .with_command(PolicyCommand::Select)
            .with_roles(vec!["admin".to_string(), "superuser".to_string()])
            .with_using("true");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains("TO admin, superuser"));
        assert!(!sql.contains("TO PUBLIC"));
    }

    #[test]
    fn test_create_policy_single_role() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("read_only", "data")
            .with_command(PolicyCommand::Select)
            .for_role("viewer")
            .with_using("true");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains("TO viewer"));
    }

    #[test]
    fn test_create_policy_with_schema() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("tenant_isolation", "data")
            .with_schema("myapp")
            .with_command(PolicyCommand::Select)
            .with_using("tenant_id = current_tenant()");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains("ON myapp.data"));
    }

    #[test]
    fn test_create_policy_quoted_identifiers() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("select-policy", "User Data")
            .with_schema("My Schema")
            .with_command(PolicyCommand::Select)
            .with_using("true");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains("\"select-policy\""));
        assert!(sql.contains("\"My Schema\".\"User Data\""));
    }

    #[test]
    fn test_create_policy_reserved_keyword_role() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("test", "data")
            .with_command(PolicyCommand::Select)
            .for_role("select")
            .with_using("true");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains("TO \"select\""));
    }

    #[test]
    fn test_create_policy_complex_expression() {
        let service = RlsService::new();
        let policy = RlsPolicy::new("complex", "orders")
            .with_command(PolicyCommand::Select)
            .with_using(
                "(customer_id = current_user_id() OR role() = 'admin') AND status != 'deleted'",
            );

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains(
            "USING ((customer_id = current_user_id() OR role() = 'admin') AND status != 'deleted')"
        ));
    }
}

mod drop_policy_tests {
    use super::*;

    #[test]
    fn test_drop_policy_simple() {
        let service = RlsService::new();
        let sql = service.build_drop_policy("my_policy", "users", None, false);
        assert_eq!(sql, "DROP POLICY my_policy ON users");
    }

    #[test]
    fn test_drop_policy_if_exists() {
        let service = RlsService::new();
        let sql = service.build_drop_policy("my_policy", "users", None, true);
        assert_eq!(sql, "DROP POLICY IF EXISTS my_policy ON users");
    }

    #[test]
    fn test_drop_policy_with_schema() {
        let service = RlsService::new();
        let sql = service.build_drop_policy("my_policy", "users", Some("app"), true);
        assert_eq!(sql, "DROP POLICY IF EXISTS my_policy ON app.users");
    }

    #[test]
    fn test_drop_policy_quoted_names() {
        let service = RlsService::new();
        let sql = service.build_drop_policy("my-policy", "user data", Some("my schema"), false);
        assert!(sql.contains("\"my-policy\""));
        assert!(sql.contains("\"my schema\".\"user data\""));
    }
}

mod rename_policy_tests {
    use super::*;

    #[test]
    fn test_rename_policy() {
        let service = RlsService::new();
        let sql = service.build_rename_policy("old_name", "new_name", "users", None);
        assert_eq!(sql, "ALTER POLICY old_name ON users RENAME TO new_name");
    }

    #[test]
    fn test_rename_policy_with_schema() {
        let service = RlsService::new();
        let sql = service.build_rename_policy("old", "new", "data", Some("app"));
        assert_eq!(sql, "ALTER POLICY old ON app.data RENAME TO new");
    }

    #[test]
    fn test_rename_policy_quoted() {
        let service = RlsService::new();
        let sql = service.build_rename_policy("select", "update", "table", None);
        assert!(sql.contains("\"select\""));
        assert!(sql.contains("\"update\""));
        assert!(sql.contains("\"table\""));
    }
}

mod alter_policy_tests {
    use super::*;

    #[test]
    fn test_alter_policy_roles_single() {
        let service = RlsService::new();
        let sql = service.build_alter_policy_roles("pol", "tbl", None, &["admin".to_string()]);
        assert_eq!(sql, "ALTER POLICY pol ON tbl TO admin");
    }

    #[test]
    fn test_alter_policy_roles_multiple() {
        let service = RlsService::new();
        let sql = service.build_alter_policy_roles(
            "pol",
            "tbl",
            None,
            &["admin".to_string(), "editor".to_string()],
        );
        assert_eq!(sql, "ALTER POLICY pol ON tbl TO admin, editor");
    }

    #[test]
    fn test_alter_policy_roles_public() {
        let service = RlsService::new();
        let sql = service.build_alter_policy_roles("pol", "tbl", None, &[]);
        assert_eq!(sql, "ALTER POLICY pol ON tbl TO PUBLIC");
    }

    #[test]
    fn test_alter_policy_using() {
        let service = RlsService::new();
        let sql = service.build_alter_policy_using("pol", "tbl", None, Some("user_id = 1"));
        assert_eq!(sql, "ALTER POLICY pol ON tbl USING (user_id = 1)");
    }

    #[test]
    fn test_alter_policy_using_remove() {
        let service = RlsService::new();
        let sql = service.build_alter_policy_using("pol", "tbl", None, None);
        assert_eq!(sql, "ALTER POLICY pol ON tbl USING (true)");
    }

    #[test]
    fn test_alter_policy_check() {
        let service = RlsService::new();
        let sql = service.build_alter_policy_check("pol", "tbl", None, Some("status = 'active'"));
        assert_eq!(
            sql,
            "ALTER POLICY pol ON tbl WITH CHECK (status = 'active')"
        );
    }

    #[test]
    fn test_alter_policy_check_remove() {
        let service = RlsService::new();
        let sql = service.build_alter_policy_check("pol", "tbl", None, None);
        assert_eq!(sql, "ALTER POLICY pol ON tbl WITH CHECK (true)");
    }

    #[test]
    fn test_alter_policy_with_schema() {
        let service = RlsService::new();
        let sql = service.build_alter_policy_using("pol", "tbl", Some("app"), Some("true"));
        assert_eq!(sql, "ALTER POLICY pol ON app.tbl USING (true)");
    }
}

mod query_tests {
    use super::*;

    #[test]
    fn test_check_rls_enabled_query() {
        let service = RlsService::new();
        let sql = service.build_check_rls_enabled_query("users", None);
        assert!(sql.contains("pg_class"));
        assert!(sql.contains("relrowsecurity"));
        assert!(sql.contains("relforcerowsecurity"));
        assert!(sql.contains("relname = 'users'"));
    }

    #[test]
    fn test_check_rls_enabled_query_with_schema() {
        let service = RlsService::new();
        let sql = service.build_check_rls_enabled_query("users", Some("public"));
        assert!(sql.contains("nspname = 'public'"));
        assert!(sql.contains("relname = 'users'"));
    }

    #[test]
    fn test_list_policies_query() {
        let service = RlsService::new();
        let sql = service.build_list_policies_query("users", None);
        assert!(sql.contains("pg_policy"));
        assert!(sql.contains("polname"));
        assert!(sql.contains("polcmd"));
        assert!(sql.contains("polpermissive"));
        assert!(sql.contains("pg_get_expr"));
        assert!(sql.contains("polqual"));
        assert!(sql.contains("polwithcheck"));
        assert!(sql.contains("relname = 'users'"));
    }

    #[test]
    fn test_list_policies_query_with_schema() {
        let service = RlsService::new();
        let sql = service.build_list_policies_query("users", Some("myschema"));
        assert!(sql.contains("nspname = 'myschema'"));
    }

    #[test]
    fn test_list_rls_tables_query() {
        let service = RlsService::new();
        let sql = service.build_list_rls_tables_query(None);
        assert!(sql.contains("pg_class"));
        assert!(sql.contains("relrowsecurity = true"));
        assert!(sql.contains("NOT IN ('pg_catalog', 'information_schema')"));
    }

    #[test]
    fn test_list_rls_tables_query_with_schema() {
        let service = RlsService::new();
        let sql = service.build_list_rls_tables_query(Some("app"));
        assert!(sql.contains("nspname = 'app'"));
    }

    #[test]
    fn test_query_sql_injection_protection() {
        let service = RlsService::new();
        let sql = service.build_check_rls_enabled_query("users'; DROP TABLE users; --", None);
        assert!(sql.contains("users''; DROP TABLE users; --"));
    }
}

mod quoting_tests {
    use super::*;

    #[test]
    fn test_simple_identifier_not_quoted() {
        let service = RlsService::new();
        let sql = service.build_enable_rls("users", None);
        assert!(!sql.contains('"'));
        assert!(sql.contains("users"));
    }

    #[test]
    fn test_reserved_keyword_quoted() {
        let service = RlsService::new();
        let sql = service.build_enable_rls("select", None);
        assert!(sql.contains("\"select\""));
    }

    #[test]
    fn test_special_characters_quoted() {
        let service = RlsService::new();
        let sql = service.build_enable_rls("user-data", None);
        assert!(sql.contains("\"user-data\""));
    }

    #[test]
    fn test_space_in_name_quoted() {
        let service = RlsService::new();
        let sql = service.build_enable_rls("user data", None);
        assert!(sql.contains("\"user data\""));
    }

    #[test]
    fn test_starts_with_digit_quoted() {
        let service = RlsService::new();
        let sql = service.build_enable_rls("1users", None);
        assert!(sql.contains("\"1users\""));
    }

    #[test]
    fn test_double_quote_escaped() {
        let service = RlsService::new();
        let sql = service.build_enable_rls("user\"data", None);
        assert!(sql.contains("\"user\"\"data\""));
    }
}

mod service_tests {
    use super::*;

    #[test]
    fn test_service_new() {
        let service = RlsService::new();
        // Just verify it constructs without error
        let _ = service;
    }

    #[test]
    fn test_service_default() {
        let service: RlsService = Default::default();
        // Just verify it constructs without error
        let _ = service;
    }
}

mod integration_tests {
    use super::*;

    #[test]
    fn test_full_rls_setup_workflow() {
        let service = RlsService::new();

        // 1. Enable RLS on table
        let enable_sql = service.build_enable_rls("orders", Some("app"));
        assert!(enable_sql.contains("ENABLE ROW LEVEL SECURITY"));

        // 2. Create policy for normal users to see their own orders
        let user_policy = RlsPolicy::new("user_orders", "orders")
            .with_schema("app")
            .with_command(PolicyCommand::Select)
            .for_role("app_user")
            .with_using("customer_id = current_user_id()");

        let policy_sql = service.build_create_policy(&user_policy).unwrap();
        assert!(policy_sql.contains("CREATE POLICY user_orders ON app.orders"));
        assert!(policy_sql.contains("FOR SELECT"));
        assert!(policy_sql.contains("TO app_user"));
        assert!(policy_sql.contains("USING (customer_id = current_user_id())"));

        // 3. Create policy for admins to see all orders
        let admin_policy = RlsPolicy::new("admin_orders", "orders")
            .with_schema("app")
            .with_command(PolicyCommand::All)
            .for_role("admin")
            .with_using("true")
            .with_check("true");

        let admin_sql = service.build_create_policy(&admin_policy).unwrap();
        assert!(admin_sql.contains("CREATE POLICY admin_orders"));
        assert!(admin_sql.contains("TO admin"));

        // 4. Force RLS even for table owner
        let force_sql = service.build_force_rls("orders", Some("app"));
        assert!(force_sql.contains("FORCE ROW LEVEL SECURITY"));
    }

    #[test]
    fn test_tenant_isolation_pattern() {
        let service = RlsService::new();

        // Multi-tenant isolation using RLS
        let policy = RlsPolicy::new("tenant_isolation", "customer_data")
            .with_policy_type(PolicyType::Restrictive)
            .with_command(PolicyCommand::All)
            .with_using("tenant_id = current_setting('app.tenant_id')::uuid")
            .with_check("tenant_id = current_setting('app.tenant_id')::uuid");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains("AS RESTRICTIVE"));
        assert!(sql.contains("current_setting('app.tenant_id')"));
    }

    #[test]
    fn test_soft_delete_pattern() {
        let service = RlsService::new();

        // Hide soft-deleted rows
        let policy = RlsPolicy::new("hide_deleted", "items")
            .with_policy_type(PolicyType::Restrictive)
            .with_command(PolicyCommand::Select)
            .with_using("deleted_at IS NULL");

        let sql = service.build_create_policy(&policy).unwrap();
        assert!(sql.contains("USING (deleted_at IS NULL)"));
        assert!(sql.contains("AS RESTRICTIVE"));
    }
}
