//! Tests for the view manager module

use super::*;

mod view_spec_tests {
    use super::*;

    #[test]
    fn test_view_spec_creation() {
        let spec = ViewSpec::new("my_view", "SELECT * FROM users");
        assert_eq!(spec.name(), "my_view");
        assert_eq!(spec.query(), "SELECT * FROM users");
        assert!(!spec.is_materialized());
        assert!(spec.schema().is_none());
        assert!(spec.columns().is_empty());
        assert!(spec.check_option().is_none());
    }

    #[test]
    fn test_materialized_view_spec() {
        let spec = ViewSpec::materialized("mv_stats", "SELECT count(*) FROM events");
        assert_eq!(spec.name(), "mv_stats");
        assert!(spec.is_materialized());
    }

    #[test]
    fn test_view_spec_with_schema() {
        let spec = ViewSpec::new("users_view", "SELECT * FROM users").with_schema("public");
        assert_eq!(spec.schema(), Some("public"));
        assert_eq!(spec.qualified_name(), "public.users_view");
    }

    #[test]
    fn test_view_spec_with_columns() {
        let columns = vec!["user_id".to_string(), "user_name".to_string()];
        let spec = ViewSpec::new("v", "SELECT id, name FROM users").with_columns(columns.clone());
        assert_eq!(spec.columns(), &columns);
    }

    #[test]
    fn test_view_spec_with_check_option() {
        let spec = ViewSpec::new("v", "SELECT * FROM t").with_check_option(CheckOption::Cascaded);
        assert_eq!(spec.check_option(), Some(&CheckOption::Cascaded));
    }

    #[test]
    fn test_view_spec_with_comment() {
        let spec = ViewSpec::new("v", "SELECT 1").with_comment("Test view");
        assert_eq!(spec.comment(), Some("Test view"));
    }

    #[test]
    fn test_qualified_name_without_schema() {
        let spec = ViewSpec::new("my_view", "SELECT 1");
        assert_eq!(spec.qualified_name(), "my_view");
    }
}

mod view_dialect_tests {
    use super::*;

    #[test]
    fn test_postgresql_supports_materialized() {
        assert!(ViewDialect::PostgreSQL.supports_materialized_views());
    }

    #[test]
    fn test_mysql_no_materialized() {
        assert!(!ViewDialect::MySQL.supports_materialized_views());
    }

    #[test]
    fn test_sqlite_no_materialized() {
        assert!(!ViewDialect::SQLite.supports_materialized_views());
    }

    #[test]
    fn test_mssql_supports_materialized() {
        assert!(ViewDialect::MsSql.supports_materialized_views());
    }

    #[test]
    fn test_sqlite_no_check_option() {
        assert!(!ViewDialect::SQLite.supports_check_option());
    }

    #[test]
    fn test_postgresql_supports_check_option() {
        assert!(ViewDialect::PostgreSQL.supports_check_option());
    }
}

mod create_view_tests {
    use super::*;

    #[test]
    fn test_build_create_view_postgres() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("active_users", "SELECT * FROM users WHERE active = true");
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("CREATE VIEW active_users AS"));
        assert!(sql.contains("SELECT * FROM users WHERE active = true"));
    }

    #[test]
    fn test_build_create_view_with_schema_postgres() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("active_users", "SELECT * FROM users").with_schema("public");
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("public.active_users"));
    }

    #[test]
    fn test_build_create_view_with_columns() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("v", "SELECT id, name FROM users")
            .with_columns(vec!["user_id".into(), "user_name".into()]);
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("(user_id, user_name)"));
    }

    #[test]
    fn test_build_create_view_with_local_check() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("v", "SELECT * FROM t").with_check_option(CheckOption::Local);
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("WITH LOCAL CHECK OPTION"));
    }

    #[test]
    fn test_build_create_view_with_cascaded_check() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("v", "SELECT * FROM t").with_check_option(CheckOption::Cascaded);
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("WITH CASCADED CHECK OPTION"));
    }

    #[test]
    fn test_build_create_materialized_view_postgres() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::materialized("mv_stats", "SELECT count(*) FROM events");
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("CREATE MATERIALIZED VIEW mv_stats AS"));
    }

    #[test]
    fn test_build_create_view_sqlite() {
        let manager = ViewManager::new(ViewDialect::SQLite);
        let spec = ViewSpec::new("v", "SELECT 1");
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("CREATE VIEW v AS"));
    }

    #[test]
    fn test_build_create_view_mysql() {
        let manager = ViewManager::new(ViewDialect::MySQL);
        let spec = ViewSpec::new("my_view", "SELECT id FROM users");
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("CREATE VIEW my_view AS"));
    }

    #[test]
    fn test_build_create_view_mssql() {
        let manager = ViewManager::new(ViewDialect::MsSql);
        let spec = ViewSpec::new("v_users", "SELECT * FROM dbo.users");
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("CREATE VIEW v_users AS"));
    }
}

mod create_or_replace_tests {
    use super::*;

    #[test]
    fn test_build_create_or_replace_view() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("v", "SELECT 1");
        let sql = manager.build_create_or_replace_view(&spec).unwrap();

        assert!(sql.contains("CREATE OR REPLACE VIEW"));
    }

    #[test]
    fn test_create_or_replace_not_for_materialized() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::materialized("mv", "SELECT 1");
        let result = manager.build_create_or_replace_view(&spec);

        assert!(result.is_err());
    }
}

mod drop_view_tests {
    use super::*;

    #[test]
    fn test_build_drop_view_simple() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let sql = manager.build_drop_view("my_view", false, false, false);

        assert_eq!(sql, "DROP VIEW my_view");
    }

    #[test]
    fn test_build_drop_view_if_exists() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let sql = manager.build_drop_view("v", false, true, false);

        assert!(sql.contains("IF EXISTS"));
    }

    #[test]
    fn test_build_drop_view_cascade() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let sql = manager.build_drop_view("v", false, false, true);

        assert!(sql.contains("CASCADE"));
    }

    #[test]
    fn test_build_drop_materialized_view() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let sql = manager.build_drop_view("mv", true, true, true);

        assert!(sql.contains("DROP MATERIALIZED VIEW"));
        assert!(sql.contains("IF EXISTS"));
        assert!(sql.contains("CASCADE"));
    }

    #[test]
    fn test_drop_view_cascade_not_on_mysql() {
        let manager = ViewManager::new(ViewDialect::MySQL);
        let sql = manager.build_drop_view("v", false, false, true);

        assert!(!sql.contains("CASCADE"));
    }
}

mod refresh_materialized_view_tests {
    use super::*;

    #[test]
    fn test_refresh_materialized_view_postgres() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let sql = manager.build_refresh_materialized_view("mv_stats", false);

        assert_eq!(sql, Some("REFRESH MATERIALIZED VIEW mv_stats".to_string()));
    }

    #[test]
    fn test_refresh_materialized_view_concurrently() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let sql = manager.build_refresh_materialized_view("mv", true);

        assert!(sql.unwrap().contains("CONCURRENTLY"));
    }

    #[test]
    fn test_refresh_materialized_view_not_mysql() {
        let manager = ViewManager::new(ViewDialect::MySQL);
        let sql = manager.build_refresh_materialized_view("mv", false);

        assert!(sql.is_none());
    }

    #[test]
    fn test_refresh_materialized_view_not_sqlite() {
        let manager = ViewManager::new(ViewDialect::SQLite);
        let sql = manager.build_refresh_materialized_view("mv", false);

        assert!(sql.is_none());
    }
}

mod rename_view_tests {
    use super::*;

    #[test]
    fn test_rename_view_postgres() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let sql = manager.build_rename_view("old_view", "new_view");

        assert!(sql.contains("ALTER VIEW"));
        assert!(sql.contains("RENAME TO"));
    }

    #[test]
    fn test_rename_view_mysql() {
        let manager = ViewManager::new(ViewDialect::MySQL);
        let sql = manager.build_rename_view("old", "new");

        assert!(sql.contains("RENAME TABLE"));
    }

    #[test]
    fn test_rename_view_mssql() {
        let manager = ViewManager::new(ViewDialect::MsSql);
        let sql = manager.build_rename_view("old", "new");

        assert!(sql.contains("sp_rename"));
    }
}

mod comment_tests {
    use super::*;

    #[test]
    fn test_build_comment_postgres() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let sql = manager.build_comment("v", Some("Test view")).unwrap();

        assert!(sql.contains("COMMENT ON VIEW"));
        assert!(sql.contains("'Test view'"));
    }

    #[test]
    fn test_build_comment_null_postgres() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let sql = manager.build_comment("v", None);

        assert!(sql.unwrap().contains("IS NULL"));
    }

    #[test]
    fn test_comment_not_mysql() {
        let manager = ViewManager::new(ViewDialect::MySQL);
        let sql = manager.build_comment("v", Some("comment"));

        assert!(sql.is_none());
    }

    #[test]
    fn test_comment_escapes_quotes() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let sql = manager.build_comment("v", Some("It's a view")).unwrap();

        assert!(sql.contains("It''s a view"));
    }
}

mod validation_tests {
    use super::*;

    #[test]
    fn test_validate_empty_name() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("", "SELECT 1");

        assert!(matches!(manager.validate(&spec), Err(ViewError::EmptyName)));
    }

    #[test]
    fn test_validate_whitespace_name() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("   ", "SELECT 1");

        assert!(matches!(manager.validate(&spec), Err(ViewError::EmptyName)));
    }

    #[test]
    fn test_validate_empty_query() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("v", "");

        assert!(matches!(
            manager.validate(&spec),
            Err(ViewError::EmptyQuery)
        ));
    }

    #[test]
    fn test_validate_materialized_not_supported() {
        let manager = ViewManager::new(ViewDialect::MySQL);
        let spec = ViewSpec::materialized("mv", "SELECT 1");

        assert!(matches!(
            manager.validate(&spec),
            Err(ViewError::MaterializedViewNotSupported)
        ));
    }

    #[test]
    fn test_validate_check_option_not_supported() {
        let manager = ViewManager::new(ViewDialect::SQLite);
        let spec = ViewSpec::new("v", "SELECT 1").with_check_option(CheckOption::Local);

        assert!(matches!(
            manager.validate(&spec),
            Err(ViewError::CheckOptionNotSupported)
        ));
    }

    #[test]
    fn test_validate_success() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("v", "SELECT 1");

        assert!(manager.validate(&spec).is_ok());
    }
}

mod quoting_tests {
    use super::*;

    #[test]
    fn test_quote_reserved_keyword_postgres() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("select", "SELECT 1");
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("\"select\""));
    }

    #[test]
    fn test_quote_reserved_keyword_mysql() {
        let manager = ViewManager::new(ViewDialect::MySQL);
        let spec = ViewSpec::new("table", "SELECT 1");
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("`table`"));
    }

    #[test]
    fn test_quote_reserved_keyword_mssql() {
        let manager = ViewManager::new(ViewDialect::MsSql);
        let spec = ViewSpec::new("order", "SELECT 1");
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("[order]"));
    }

    #[test]
    fn test_quote_name_with_space() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("my view", "SELECT 1");
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("\"my view\""));
    }

    #[test]
    fn test_no_quote_simple_name() {
        let manager = ViewManager::new(ViewDialect::PostgreSQL);
        let spec = ViewSpec::new("users_view", "SELECT 1");
        let sql = manager.build_create_view(&spec).unwrap();

        assert!(sql.contains("users_view"));
        assert!(!sql.contains("\"users_view\""));
    }
}

mod error_display_tests {
    use super::*;

    #[test]
    fn test_error_display_empty_name() {
        let err = ViewError::EmptyName;
        assert_eq!(format!("{}", err), "View name cannot be empty");
    }

    #[test]
    fn test_error_display_empty_query() {
        let err = ViewError::EmptyQuery;
        assert_eq!(format!("{}", err), "View query cannot be empty");
    }

    #[test]
    fn test_error_display_materialized_not_supported() {
        let err = ViewError::MaterializedViewNotSupported;
        assert!(format!("{}", err).contains("Materialized views are not supported"));
    }

    #[test]
    fn test_error_display_check_option_not_supported() {
        let err = ViewError::CheckOptionNotSupported;
        assert!(format!("{}", err).contains("Check option is not supported"));
    }

    #[test]
    fn test_error_display_invalid_columns() {
        let err = ViewError::InvalidColumns("too many columns".into());
        assert!(format!("{}", err).contains("Invalid column specification"));
        assert!(format!("{}", err).contains("too many columns"));
    }
}
