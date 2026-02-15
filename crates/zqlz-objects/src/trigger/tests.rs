//! Tests for trigger manager

use super::*;

mod trigger_timing_tests {
    use super::*;

    #[test]
    fn test_before_timing() {
        let timing = TriggerTiming::Before;
        assert!(timing.is_before());
        assert!(!timing.is_after());
        assert!(!timing.is_instead_of());
        assert_eq!(timing.as_sql(), "BEFORE");
    }

    #[test]
    fn test_after_timing() {
        let timing = TriggerTiming::After;
        assert!(!timing.is_before());
        assert!(timing.is_after());
        assert!(!timing.is_instead_of());
        assert_eq!(timing.as_sql(), "AFTER");
    }

    #[test]
    fn test_instead_of_timing() {
        let timing = TriggerTiming::InsteadOf;
        assert!(!timing.is_before());
        assert!(!timing.is_after());
        assert!(timing.is_instead_of());
        assert_eq!(timing.as_sql(), "INSTEAD OF");
    }

    #[test]
    fn test_default_timing_is_after() {
        let timing = TriggerTiming::default();
        assert!(timing.is_after());
    }

    #[test]
    fn test_timing_serialization() {
        let timing = TriggerTiming::Before;
        let json = serde_json::to_string(&timing).unwrap();
        assert_eq!(json, "\"before\"");

        let deserialized: TriggerTiming = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, timing);
    }
}

mod trigger_event_tests {
    use super::*;

    #[test]
    fn test_insert_event() {
        let event = TriggerEvent::Insert;
        assert_eq!(event.as_sql(), "INSERT");
    }

    #[test]
    fn test_update_event() {
        let event = TriggerEvent::Update;
        assert_eq!(event.as_sql(), "UPDATE");
    }

    #[test]
    fn test_delete_event() {
        let event = TriggerEvent::Delete;
        assert_eq!(event.as_sql(), "DELETE");
    }

    #[test]
    fn test_truncate_event() {
        let event = TriggerEvent::Truncate;
        assert_eq!(event.as_sql(), "TRUNCATE");
    }

    #[test]
    fn test_contains_multiple_with_single() {
        let events = vec![TriggerEvent::Insert];
        assert!(!TriggerEvent::contains_multiple(&events));
    }

    #[test]
    fn test_contains_multiple_with_multiple() {
        let events = vec![TriggerEvent::Insert, TriggerEvent::Update];
        assert!(TriggerEvent::contains_multiple(&events));
    }

    #[test]
    fn test_event_serialization() {
        let event = TriggerEvent::Delete;
        let json = serde_json::to_string(&event).unwrap();
        assert_eq!(json, "\"delete\"");
    }
}

mod trigger_level_tests {
    use super::*;

    #[test]
    fn test_row_level() {
        let level = TriggerLevel::Row;
        assert_eq!(level.as_sql(), "FOR EACH ROW");
    }

    #[test]
    fn test_statement_level() {
        let level = TriggerLevel::Statement;
        assert_eq!(level.as_sql(), "FOR EACH STATEMENT");
    }

    #[test]
    fn test_default_level_is_row() {
        let level = TriggerLevel::default();
        assert_eq!(level, TriggerLevel::Row);
    }
}

mod trigger_spec_tests {
    use super::*;

    #[test]
    fn test_new_trigger_spec() {
        let spec = TriggerSpec::new("audit_trigger", "users");
        assert_eq!(spec.name(), "audit_trigger");
        assert_eq!(spec.table(), "users");
        assert!(spec.schema().is_none());
        assert!(spec.timing().is_after());
        assert_eq!(spec.events().len(), 1);
        assert_eq!(spec.level(), TriggerLevel::Row);
    }

    #[test]
    fn test_with_schema() {
        let spec = TriggerSpec::new("my_trigger", "my_table").with_schema("myschema");
        assert_eq!(spec.schema(), Some("myschema"));
        assert_eq!(spec.qualified_table(), "myschema.my_table");
    }

    #[test]
    fn test_with_timing() {
        let spec = TriggerSpec::new("t", "tab").with_timing(TriggerTiming::Before);
        assert!(spec.timing().is_before());
    }

    #[test]
    fn test_with_single_event() {
        let spec = TriggerSpec::new("t", "tab").with_event(TriggerEvent::Delete);
        assert_eq!(spec.events().len(), 1);
        assert_eq!(spec.events()[0], TriggerEvent::Delete);
    }

    #[test]
    fn test_with_multiple_events() {
        let spec = TriggerSpec::new("t", "tab").with_events(vec![
            TriggerEvent::Insert,
            TriggerEvent::Update,
            TriggerEvent::Delete,
        ]);
        assert_eq!(spec.events().len(), 3);
    }

    #[test]
    fn test_with_level() {
        let spec = TriggerSpec::new("t", "tab").with_level(TriggerLevel::Statement);
        assert_eq!(spec.level(), TriggerLevel::Statement);
    }

    #[test]
    fn test_with_when() {
        let spec = TriggerSpec::new("t", "tab").with_when("NEW.active = true");
        assert_eq!(spec.when_condition(), Some("NEW.active = true"));
    }

    #[test]
    fn test_with_function() {
        let spec = TriggerSpec::new("t", "tab").with_function("audit_func");
        assert_eq!(spec.function_name(), Some("audit_func"));
    }

    #[test]
    fn test_with_body() {
        let spec = TriggerSpec::new("t", "tab").with_body("INSERT INTO audit VALUES (NEW.id);");
        assert_eq!(spec.body(), Some("INSERT INTO audit VALUES (NEW.id);"));
    }

    #[test]
    fn test_with_update_columns() {
        let spec =
            TriggerSpec::new("t", "tab").with_update_columns(vec!["col1".into(), "col2".into()]);
        assert_eq!(spec.update_columns(), &["col1", "col2"]);
    }

    #[test]
    fn test_with_comment() {
        let spec = TriggerSpec::new("t", "tab").with_comment("Audit trigger");
        assert_eq!(spec.comment(), Some("Audit trigger"));
    }
}

mod trigger_dialect_tests {
    use super::*;

    #[test]
    fn test_postgres_supports_before() {
        let dialect = TriggerDialect::PostgreSQL;
        assert!(dialect.supports_before());
    }

    #[test]
    fn test_mssql_does_not_support_before() {
        let dialect = TriggerDialect::MsSql;
        assert!(!dialect.supports_before());
    }

    #[test]
    fn test_postgres_supports_instead_of() {
        let dialect = TriggerDialect::PostgreSQL;
        assert!(dialect.supports_instead_of());
    }

    #[test]
    fn test_mysql_does_not_support_instead_of() {
        let dialect = TriggerDialect::MySQL;
        assert!(!dialect.supports_instead_of());
    }

    #[test]
    fn test_postgres_requires_function() {
        let dialect = TriggerDialect::PostgreSQL;
        assert!(dialect.requires_function());
    }

    #[test]
    fn test_mysql_does_not_require_function() {
        let dialect = TriggerDialect::MySQL;
        assert!(!dialect.requires_function());
    }

    #[test]
    fn test_postgres_supports_truncate() {
        let dialect = TriggerDialect::PostgreSQL;
        assert!(dialect.supports_truncate());
    }

    #[test]
    fn test_mysql_does_not_support_truncate() {
        let dialect = TriggerDialect::MySQL;
        assert!(!dialect.supports_truncate());
    }

    #[test]
    fn test_postgres_supports_statement_level() {
        let dialect = TriggerDialect::PostgreSQL;
        assert!(dialect.supports_statement_level());
    }

    #[test]
    fn test_sqlite_does_not_support_statement_level() {
        let dialect = TriggerDialect::SQLite;
        assert!(!dialect.supports_statement_level());
    }

    #[test]
    fn test_postgres_supports_when_condition() {
        let dialect = TriggerDialect::PostgreSQL;
        assert!(dialect.supports_when_condition());
    }

    #[test]
    fn test_mysql_does_not_support_when_condition() {
        let dialect = TriggerDialect::MySQL;
        assert!(!dialect.supports_when_condition());
    }

    #[test]
    fn test_postgres_supports_update_columns() {
        let dialect = TriggerDialect::PostgreSQL;
        assert!(dialect.supports_update_columns());
    }

    #[test]
    fn test_mssql_does_not_support_update_columns() {
        let dialect = TriggerDialect::MsSql;
        assert!(!dialect.supports_update_columns());
    }
}

mod validation_tests {
    use super::*;

    #[test]
    fn test_empty_name_error() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("", "users").with_function("f");
        assert_eq!(manager.validate(&spec), Err(TriggerError::EmptyName));
    }

    #[test]
    fn test_empty_table_error() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("t", "").with_function("f");
        assert_eq!(manager.validate(&spec), Err(TriggerError::EmptyTable));
    }

    #[test]
    fn test_no_events_error() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("t", "tab")
            .with_events(vec![]) // Empty events
            .with_function("f");
        assert_eq!(manager.validate(&spec), Err(TriggerError::NoEvents));
    }

    #[test]
    fn test_before_not_supported_mssql() {
        let manager = TriggerManager::new(TriggerDialect::MsSql);
        let spec = TriggerSpec::new("t", "tab")
            .with_timing(TriggerTiming::Before)
            .with_body("SELECT 1");
        assert_eq!(
            manager.validate(&spec),
            Err(TriggerError::BeforeNotSupported)
        );
    }

    #[test]
    fn test_instead_of_not_supported_mysql() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let spec = TriggerSpec::new("t", "tab")
            .with_timing(TriggerTiming::InsteadOf)
            .with_body("SELECT 1");
        assert_eq!(
            manager.validate(&spec),
            Err(TriggerError::InsteadOfNotSupported)
        );
    }

    #[test]
    fn test_truncate_not_supported_mysql() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let spec = TriggerSpec::new("t", "tab")
            .with_event(TriggerEvent::Truncate)
            .with_body("SELECT 1");
        assert_eq!(
            manager.validate(&spec),
            Err(TriggerError::TruncateNotSupported)
        );
    }

    #[test]
    fn test_statement_level_not_supported_mysql() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let spec = TriggerSpec::new("t", "tab")
            .with_level(TriggerLevel::Statement)
            .with_body("SELECT 1");
        assert_eq!(
            manager.validate(&spec),
            Err(TriggerError::StatementLevelNotSupported)
        );
    }

    #[test]
    fn test_when_condition_not_supported_mysql() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let spec = TriggerSpec::new("t", "tab")
            .with_when("NEW.x > 0")
            .with_body("SELECT 1");
        assert_eq!(
            manager.validate(&spec),
            Err(TriggerError::WhenConditionNotSupported)
        );
    }

    #[test]
    fn test_update_columns_not_supported_mysql() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let spec = TriggerSpec::new("t", "tab")
            .with_update_columns(vec!["col1".into()])
            .with_body("SELECT 1");
        assert_eq!(
            manager.validate(&spec),
            Err(TriggerError::UpdateColumnsNotSupported)
        );
    }

    #[test]
    fn test_missing_function_postgres() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("t", "tab");
        assert_eq!(manager.validate(&spec), Err(TriggerError::MissingFunction));
    }

    #[test]
    fn test_missing_body_mysql() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let spec = TriggerSpec::new("t", "tab");
        assert_eq!(manager.validate(&spec), Err(TriggerError::MissingBody));
    }
}

mod postgres_trigger_tests {
    use super::*;

    #[test]
    fn test_simple_after_insert_trigger() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("audit_insert", "users")
            .with_timing(TriggerTiming::After)
            .with_event(TriggerEvent::Insert)
            .with_function("audit_log_func");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("CREATE TRIGGER audit_insert"));
        assert!(sql.contains("AFTER INSERT"));
        assert!(sql.contains("ON users"));
        assert!(sql.contains("FOR EACH ROW"));
        assert!(sql.contains("EXECUTE FUNCTION audit_log_func()"));
    }

    #[test]
    fn test_before_update_trigger() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("validate_update", "users")
            .with_timing(TriggerTiming::Before)
            .with_event(TriggerEvent::Update)
            .with_function("validate_func");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("BEFORE UPDATE"));
    }

    #[test]
    fn test_multiple_events() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("audit_changes", "users")
            .with_events(vec![
                TriggerEvent::Insert,
                TriggerEvent::Update,
                TriggerEvent::Delete,
            ])
            .with_function("audit_func");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("INSERT OR UPDATE OR DELETE"));
    }

    #[test]
    fn test_update_of_columns() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("track_email_change", "users")
            .with_event(TriggerEvent::Update)
            .with_update_columns(vec!["email".into(), "phone".into()])
            .with_function("track_func");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("UPDATE OF email, phone"));
    }

    #[test]
    fn test_when_condition() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("audit_active", "users")
            .with_event(TriggerEvent::Insert)
            .with_when("NEW.active = true")
            .with_function("audit_func");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("WHEN (NEW.active = true)"));
    }

    #[test]
    fn test_statement_level() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("batch_audit", "users")
            .with_event(TriggerEvent::Insert)
            .with_level(TriggerLevel::Statement)
            .with_function("batch_audit_func");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("FOR EACH STATEMENT"));
    }

    #[test]
    fn test_with_schema() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("audit_insert", "users")
            .with_schema("myschema")
            .with_event(TriggerEvent::Insert)
            .with_function("audit_func");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("ON myschema.users"));
    }

    #[test]
    fn test_instead_of_trigger() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("view_insert", "users_view")
            .with_timing(TriggerTiming::InsteadOf)
            .with_event(TriggerEvent::Insert)
            .with_function("handle_view_insert");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("INSTEAD OF INSERT"));
    }
}

mod mysql_trigger_tests {
    use super::*;

    #[test]
    fn test_simple_after_insert_trigger() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let spec = TriggerSpec::new("audit_insert", "users")
            .with_timing(TriggerTiming::After)
            .with_event(TriggerEvent::Insert)
            .with_body("INSERT INTO audit (user_id, action) VALUES (NEW.id, 'INSERT');");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(
            sql.contains("CREATE TRIGGER `audit_insert`")
                || sql.contains("CREATE TRIGGER audit_insert")
        );
        assert!(sql.contains("AFTER INSERT"));
        assert!(sql.contains("ON"));
        assert!(sql.contains("FOR EACH ROW"));
        assert!(sql.contains("BEGIN"));
        assert!(sql.contains("END"));
    }

    #[test]
    fn test_before_update_trigger() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let spec = TriggerSpec::new("validate_update", "users")
            .with_timing(TriggerTiming::Before)
            .with_event(TriggerEvent::Update)
            .with_body("SET NEW.updated_at = NOW();");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("BEFORE UPDATE"));
    }
}

mod sqlite_trigger_tests {
    use super::*;

    #[test]
    fn test_simple_after_insert_trigger() {
        let manager = TriggerManager::new(TriggerDialect::SQLite);
        let spec = TriggerSpec::new("audit_insert", "users")
            .with_timing(TriggerTiming::After)
            .with_event(TriggerEvent::Insert)
            .with_body("INSERT INTO audit VALUES (NEW.id, 'INSERT');");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("CREATE TRIGGER"));
        assert!(sql.contains("AFTER INSERT"));
        assert!(sql.contains("ON users"));
        assert!(sql.contains("FOR EACH ROW"));
        assert!(sql.contains("BEGIN"));
    }

    #[test]
    fn test_with_when_condition() {
        let manager = TriggerManager::new(TriggerDialect::SQLite);
        let spec = TriggerSpec::new("audit_active", "users")
            .with_event(TriggerEvent::Insert)
            .with_when("NEW.active = 1")
            .with_body("INSERT INTO audit VALUES (NEW.id);");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("WHEN NEW.active = 1"));
    }

    #[test]
    fn test_instead_of_trigger() {
        let manager = TriggerManager::new(TriggerDialect::SQLite);
        let spec = TriggerSpec::new("view_insert", "users_view")
            .with_timing(TriggerTiming::InsteadOf)
            .with_event(TriggerEvent::Insert)
            .with_body("INSERT INTO users VALUES (NEW.id, NEW.name);");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("INSTEAD OF INSERT"));
    }
}

mod mssql_trigger_tests {
    use super::*;

    #[test]
    fn test_simple_after_insert_trigger() {
        let manager = TriggerManager::new(TriggerDialect::MsSql);
        let spec = TriggerSpec::new("audit_insert", "users")
            .with_timing(TriggerTiming::After)
            .with_event(TriggerEvent::Insert)
            .with_body("INSERT INTO audit SELECT id, 'INSERT' FROM inserted;");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("CREATE TRIGGER"));
        assert!(sql.contains("ON"));
        assert!(sql.contains("AFTER INSERT"));
        assert!(sql.contains("AS"));
        assert!(sql.contains("BEGIN"));
    }

    #[test]
    fn test_multiple_events() {
        let manager = TriggerManager::new(TriggerDialect::MsSql);
        let spec = TriggerSpec::new("audit_changes", "users")
            .with_events(vec![TriggerEvent::Insert, TriggerEvent::Update])
            .with_body("PRINT 'Change detected';");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("INSERT, UPDATE"));
    }

    #[test]
    fn test_instead_of_trigger() {
        let manager = TriggerManager::new(TriggerDialect::MsSql);
        let spec = TriggerSpec::new("view_insert", "users_view")
            .with_timing(TriggerTiming::InsteadOf)
            .with_event(TriggerEvent::Insert)
            .with_body("INSERT INTO users SELECT * FROM inserted;");

        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("INSTEAD OF INSERT"));
    }
}

mod drop_trigger_tests {
    use super::*;

    #[test]
    fn test_postgres_drop_with_table() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let sql = manager.build_drop_trigger("audit_insert", Some("users"), None, false);
        assert!(sql.contains("DROP TRIGGER audit_insert ON users"));
    }

    #[test]
    fn test_postgres_drop_if_exists() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let sql = manager.build_drop_trigger("audit_insert", Some("users"), None, true);
        assert!(sql.contains("DROP TRIGGER IF EXISTS audit_insert ON users"));
    }

    #[test]
    fn test_mysql_drop() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let sql = manager.build_drop_trigger("audit_insert", Some("users"), None, false);
        assert!(sql.contains("DROP TRIGGER"));
    }

    #[test]
    fn test_mssql_drop_if_exists() {
        let manager = TriggerManager::new(TriggerDialect::MsSql);
        let sql = manager.build_drop_trigger("audit_insert", Some("users"), None, true);
        assert!(sql.contains("IF OBJECT_ID"));
        assert!(sql.contains("'TR'"));
        assert!(sql.contains("DROP TRIGGER"));
    }

    #[test]
    fn test_sqlite_drop() {
        let manager = TriggerManager::new(TriggerDialect::SQLite);
        let sql = manager.build_drop_trigger("audit_insert", None, None, false);
        assert_eq!(sql, "DROP TRIGGER audit_insert");
    }
}

mod enable_disable_tests {
    use super::*;

    #[test]
    fn test_postgres_disable_trigger() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let sql = manager.build_enable_disable("audit_insert", Some("users"), None, false);
        assert!(sql.is_some());
        assert!(sql.unwrap().contains("DISABLE TRIGGER audit_insert"));
    }

    #[test]
    fn test_postgres_enable_trigger() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let sql = manager.build_enable_disable("audit_insert", Some("users"), None, true);
        assert!(sql.is_some());
        assert!(sql.unwrap().contains("ENABLE TRIGGER audit_insert"));
    }

    #[test]
    fn test_mssql_disable_trigger() {
        let manager = TriggerManager::new(TriggerDialect::MsSql);
        let sql = manager.build_enable_disable("audit_insert", Some("users"), None, false);
        assert!(sql.is_some());
        assert!(sql.unwrap().contains("DISABLE TRIGGER"));
    }

    #[test]
    fn test_mysql_enable_disable_not_supported() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let sql = manager.build_enable_disable("audit_insert", Some("users"), None, true);
        assert!(sql.is_none());
    }

    #[test]
    fn test_sqlite_enable_disable_not_supported() {
        let manager = TriggerManager::new(TriggerDialect::SQLite);
        let sql = manager.build_enable_disable("audit_insert", Some("users"), None, false);
        assert!(sql.is_none());
    }
}

mod comment_tests {
    use super::*;

    #[test]
    fn test_postgres_comment() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let sql = manager.build_comment("audit_insert", "users", Some("Audit trigger for inserts"));
        assert!(sql.is_some());
        let sql = sql.unwrap();
        assert!(sql.contains("COMMENT ON TRIGGER"));
        assert!(sql.contains("Audit trigger for inserts"));
    }

    #[test]
    fn test_postgres_null_comment() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let sql = manager.build_comment("audit_insert", "users", None);
        assert!(sql.is_some());
        assert!(sql.unwrap().contains("IS NULL"));
    }

    #[test]
    fn test_mysql_comment_not_supported() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let sql = manager.build_comment("audit_insert", "users", Some("Test"));
        assert!(sql.is_none());
    }

    #[test]
    fn test_comment_escapes_quotes() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let sql = manager.build_comment("t", "tab", Some("O'Reilly's trigger"));
        assert!(sql.is_some());
        assert!(sql.unwrap().contains("O''Reilly''s trigger"));
    }
}

mod quoting_tests {
    use super::*;

    #[test]
    fn test_postgres_quoting_reserved_word() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("select", "table").with_function("check");
        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("\"select\""));
        assert!(sql.contains("\"table\""));
    }

    #[test]
    fn test_mysql_quoting() {
        let manager = TriggerManager::new(TriggerDialect::MySQL);
        let spec = TriggerSpec::new("select", "table").with_body("SELECT 1;");
        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("`select`"));
        assert!(sql.contains("`table`"));
    }

    #[test]
    fn test_mssql_quoting() {
        let manager = TriggerManager::new(TriggerDialect::MsSql);
        let spec = TriggerSpec::new("select", "table").with_body("SELECT 1;");
        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("[select]"));
        assert!(sql.contains("[table]"));
    }

    #[test]
    fn test_no_quoting_for_simple_names() {
        let manager = TriggerManager::new(TriggerDialect::PostgreSQL);
        let spec = TriggerSpec::new("my_trigger", "users").with_function("my_func");
        let sql = manager.build_create_trigger(&spec).unwrap();
        assert!(sql.contains("my_trigger"));
        assert!(!sql.contains("\"my_trigger\""));
    }
}

mod error_display_tests {
    use super::*;

    #[test]
    fn test_empty_name_display() {
        let err = TriggerError::EmptyName;
        assert_eq!(format!("{}", err), "Trigger name cannot be empty");
    }

    #[test]
    fn test_empty_table_display() {
        let err = TriggerError::EmptyTable;
        assert_eq!(format!("{}", err), "Table name cannot be empty");
    }

    #[test]
    fn test_no_events_display() {
        let err = TriggerError::NoEvents;
        assert_eq!(
            format!("{}", err),
            "At least one trigger event must be specified"
        );
    }

    #[test]
    fn test_before_not_supported_display() {
        let err = TriggerError::BeforeNotSupported;
        assert!(format!("{}", err).contains("BEFORE"));
    }

    #[test]
    fn test_missing_function_display() {
        let err = TriggerError::MissingFunction;
        assert!(format!("{}", err).contains("function name"));
    }

    #[test]
    fn test_missing_body_display() {
        let err = TriggerError::MissingBody;
        assert!(format!("{}", err).contains("body"));
    }
}
