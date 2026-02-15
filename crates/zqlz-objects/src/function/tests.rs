//! Tests for function manager

use super::*;

mod parameter_mode_tests {
    use super::*;

    #[test]
    fn test_parameter_mode_is_input() {
        assert!(FunctionParameterMode::In.is_input());
        assert!(FunctionParameterMode::InOut.is_input());
        assert!(FunctionParameterMode::Variadic.is_input());
        assert!(!FunctionParameterMode::Out.is_input());
    }

    #[test]
    fn test_parameter_mode_is_output() {
        assert!(FunctionParameterMode::Out.is_output());
        assert!(FunctionParameterMode::InOut.is_output());
        assert!(!FunctionParameterMode::In.is_output());
        assert!(!FunctionParameterMode::Variadic.is_output());
    }

    #[test]
    fn test_parameter_mode_as_sql() {
        assert_eq!(FunctionParameterMode::In.as_sql(), "IN");
        assert_eq!(FunctionParameterMode::Out.as_sql(), "OUT");
        assert_eq!(FunctionParameterMode::InOut.as_sql(), "INOUT");
        assert_eq!(FunctionParameterMode::Variadic.as_sql(), "VARIADIC");
    }

    #[test]
    fn test_parameter_mode_default() {
        assert_eq!(FunctionParameterMode::default(), FunctionParameterMode::In);
    }
}

mod function_param_tests {
    use super::*;

    #[test]
    fn test_function_param_new() {
        let param = FunctionParam::new("user_id", "INTEGER");
        assert_eq!(param.name(), "user_id");
        assert_eq!(param.data_type(), "INTEGER");
        assert_eq!(param.mode(), FunctionParameterMode::In);
        assert!(param.default_value().is_none());
    }

    #[test]
    fn test_function_param_with_mode() {
        let param = FunctionParam::new("result", "INTEGER").with_mode(FunctionParameterMode::Out);
        assert_eq!(param.mode(), FunctionParameterMode::Out);
    }

    #[test]
    fn test_function_param_with_default() {
        let param = FunctionParam::new("active", "BOOLEAN").with_default("TRUE");
        assert_eq!(param.default_value(), Some("TRUE"));
    }

    #[test]
    fn test_function_param_builder_chain() {
        let param = FunctionParam::new("count", "INTEGER")
            .with_mode(FunctionParameterMode::InOut)
            .with_default("0");
        assert_eq!(param.name(), "count");
        assert_eq!(param.data_type(), "INTEGER");
        assert_eq!(param.mode(), FunctionParameterMode::InOut);
        assert_eq!(param.default_value(), Some("0"));
    }
}

mod function_volatility_tests {
    use super::*;

    #[test]
    fn test_volatility_as_sql() {
        assert_eq!(FunctionVolatility::Immutable.as_sql(), "IMMUTABLE");
        assert_eq!(FunctionVolatility::Stable.as_sql(), "STABLE");
        assert_eq!(FunctionVolatility::Volatile.as_sql(), "VOLATILE");
    }

    #[test]
    fn test_volatility_default() {
        assert_eq!(FunctionVolatility::default(), FunctionVolatility::Volatile);
    }
}

mod null_behavior_tests {
    use super::*;

    #[test]
    fn test_null_behavior_as_sql() {
        assert_eq!(
            NullBehavior::CalledOnNullInput.as_sql(),
            "CALLED ON NULL INPUT"
        );
        assert_eq!(
            NullBehavior::ReturnsNullOnNullInput.as_sql(),
            "RETURNS NULL ON NULL INPUT"
        );
        assert_eq!(NullBehavior::Strict.as_sql(), "STRICT");
    }

    #[test]
    fn test_null_behavior_default() {
        assert_eq!(NullBehavior::default(), NullBehavior::CalledOnNullInput);
    }
}

mod security_mode_tests {
    use super::*;

    #[test]
    fn test_security_mode_as_sql() {
        assert_eq!(SecurityMode::Invoker.as_sql(), "SECURITY INVOKER");
        assert_eq!(SecurityMode::Definer.as_sql(), "SECURITY DEFINER");
    }

    #[test]
    fn test_security_mode_default() {
        assert_eq!(SecurityMode::default(), SecurityMode::Invoker);
    }
}

mod function_language_tests {
    use super::*;

    #[test]
    fn test_language_as_sql() {
        assert_eq!(FunctionLanguage::Sql.as_sql(), "SQL");
        assert_eq!(FunctionLanguage::PlPgSql.as_sql(), "plpgsql");
        assert_eq!(FunctionLanguage::Python.as_sql(), "plpython3u");
        assert_eq!(FunctionLanguage::JavaScript.as_sql(), "plv8");
        assert_eq!(
            FunctionLanguage::Custom("mylang".to_string()).as_sql(),
            "mylang"
        );
    }

    #[test]
    fn test_language_default() {
        assert_eq!(FunctionLanguage::default(), FunctionLanguage::Sql);
    }
}

mod function_spec_tests {
    use super::*;

    #[test]
    fn test_function_spec_new() {
        let spec = FunctionSpec::new("add_numbers", "INTEGER");
        assert_eq!(spec.name(), "add_numbers");
        assert_eq!(spec.return_type(), "INTEGER");
        assert!(spec.parameters().is_empty());
        assert!(!spec.is_set_returning());
        assert!(spec.table_columns().is_none());
        assert!(spec.body().is_none());
        assert_eq!(*spec.language(), FunctionLanguage::Sql);
    }

    #[test]
    fn test_function_spec_with_schema() {
        let spec = FunctionSpec::new("my_func", "TEXT").with_schema("public");
        assert_eq!(spec.schema(), Some("public"));
        assert_eq!(spec.qualified_name(), "public.my_func");
    }

    #[test]
    fn test_function_spec_with_parameters() {
        let spec = FunctionSpec::new("concat_strings", "TEXT")
            .with_parameter(FunctionParam::new("a", "TEXT"))
            .with_parameter(FunctionParam::new("b", "TEXT"));
        assert_eq!(spec.parameters().len(), 2);
        assert_eq!(spec.parameters()[0].name(), "a");
        assert_eq!(spec.parameters()[1].name(), "b");
    }

    #[test]
    fn test_function_spec_returns_set() {
        let spec = FunctionSpec::new("get_users", "users").returns_set();
        assert!(spec.is_set_returning());
    }

    #[test]
    fn test_function_spec_returns_table() {
        let spec = FunctionSpec::new("get_user_info", "TABLE").returns_table(vec![
            FunctionParam::new("id", "INTEGER"),
            FunctionParam::new("name", "VARCHAR"),
        ]);
        assert!(spec.is_set_returning());
        assert!(spec.table_columns().is_some());
        assert_eq!(spec.table_columns().unwrap().len(), 2);
    }

    #[test]
    fn test_function_spec_with_body() {
        let spec = FunctionSpec::new("increment", "INTEGER").with_body("RETURN x + 1");
        assert_eq!(spec.body(), Some("RETURN x + 1"));
    }

    #[test]
    fn test_function_spec_with_volatility() {
        let spec = FunctionSpec::new("pi", "DOUBLE PRECISION")
            .with_volatility(FunctionVolatility::Immutable);
        assert_eq!(spec.volatility(), FunctionVolatility::Immutable);
    }

    #[test]
    fn test_function_spec_with_null_behavior() {
        let spec =
            FunctionSpec::new("safe_func", "INTEGER").with_null_behavior(NullBehavior::Strict);
        assert_eq!(spec.null_behavior(), NullBehavior::Strict);
    }

    #[test]
    fn test_function_spec_with_security() {
        let spec = FunctionSpec::new("admin_func", "VOID").with_security(SecurityMode::Definer);
        assert_eq!(spec.security(), SecurityMode::Definer);
    }

    #[test]
    fn test_function_spec_parallel_safe() {
        let spec = FunctionSpec::new("parallel_func", "INTEGER").parallel_safe();
        assert!(spec.is_parallel_safe());
    }

    #[test]
    fn test_function_spec_with_cost_and_rows() {
        let spec = FunctionSpec::new("expensive_func", "INTEGER")
            .with_cost(1000)
            .with_rows(100);
        assert_eq!(spec.cost(), Some(1000));
        assert_eq!(spec.rows(), Some(100));
    }

    #[test]
    fn test_function_spec_with_comment() {
        let spec = FunctionSpec::new("documented_func", "INTEGER")
            .with_comment("This function does something useful");
        assert_eq!(spec.comment(), Some("This function does something useful"));
    }

    #[test]
    fn test_function_spec_qualified_name_no_schema() {
        let spec = FunctionSpec::new("my_func", "INTEGER");
        assert_eq!(spec.qualified_name(), "my_func");
    }
}

mod function_dialect_tests {
    use super::*;

    #[test]
    fn test_dialect_supports_functions() {
        assert!(FunctionDialect::PostgreSQL.supports_functions());
        assert!(FunctionDialect::MySQL.supports_functions());
        assert!(FunctionDialect::MsSql.supports_functions());
        assert!(!FunctionDialect::SQLite.supports_functions());
    }

    #[test]
    fn test_dialect_supports_out_parameters() {
        assert!(FunctionDialect::PostgreSQL.supports_out_parameters());
        assert!(FunctionDialect::MySQL.supports_out_parameters());
        assert!(FunctionDialect::MsSql.supports_out_parameters());
        assert!(!FunctionDialect::SQLite.supports_out_parameters());
    }

    #[test]
    fn test_dialect_supports_returns_table() {
        assert!(FunctionDialect::PostgreSQL.supports_returns_table());
        assert!(FunctionDialect::MsSql.supports_returns_table());
        assert!(!FunctionDialect::MySQL.supports_returns_table());
        assert!(!FunctionDialect::SQLite.supports_returns_table());
    }

    #[test]
    fn test_dialect_supports_volatility() {
        assert!(FunctionDialect::PostgreSQL.supports_volatility());
        assert!(!FunctionDialect::MySQL.supports_volatility());
        assert!(!FunctionDialect::MsSql.supports_volatility());
    }

    #[test]
    fn test_dialect_supports_security_mode() {
        assert!(FunctionDialect::PostgreSQL.supports_security_mode());
        assert!(FunctionDialect::MySQL.supports_security_mode());
        assert!(FunctionDialect::MsSql.supports_security_mode());
        assert!(!FunctionDialect::SQLite.supports_security_mode());
    }

    #[test]
    fn test_dialect_default_language() {
        assert_eq!(
            FunctionDialect::PostgreSQL.default_language(),
            FunctionLanguage::PlPgSql
        );
        assert_eq!(
            FunctionDialect::MySQL.default_language(),
            FunctionLanguage::Sql
        );
        assert_eq!(
            FunctionDialect::MsSql.default_language(),
            FunctionLanguage::Sql
        );
    }
}

mod validation_tests {
    use super::*;

    #[test]
    fn test_validate_empty_name() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("", "INTEGER").with_body("SELECT 1");
        assert_eq!(manager.validate(&spec), Err(FunctionError::EmptyName));
    }

    #[test]
    fn test_validate_empty_return_type() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("my_func", "").with_body("SELECT 1");
        assert_eq!(manager.validate(&spec), Err(FunctionError::EmptyReturnType));
    }

    #[test]
    fn test_validate_empty_body() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("my_func", "INTEGER");
        assert_eq!(manager.validate(&spec), Err(FunctionError::EmptyBody));
    }

    #[test]
    fn test_validate_functions_not_supported() {
        let manager = FunctionManager::new(FunctionDialect::SQLite);
        let spec = FunctionSpec::new("my_func", "INTEGER").with_body("SELECT 1");
        assert_eq!(
            manager.validate(&spec),
            Err(FunctionError::FunctionsNotSupported)
        );
    }

    #[test]
    fn test_validate_empty_parameter_name() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("my_func", "INTEGER")
            .with_parameter(FunctionParam::new("", "INTEGER"))
            .with_body("SELECT 1");
        assert_eq!(
            manager.validate(&spec),
            Err(FunctionError::EmptyParameterName)
        );
    }

    #[test]
    fn test_validate_empty_parameter_type() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("my_func", "INTEGER")
            .with_parameter(FunctionParam::new("x", ""))
            .with_body("SELECT 1");
        assert_eq!(
            manager.validate(&spec),
            Err(FunctionError::EmptyParameterType)
        );
    }

    #[test]
    fn test_validate_out_parameters_not_supported() {
        let manager = FunctionManager::new(FunctionDialect::SQLite);
        let spec = FunctionSpec::new("my_func", "INTEGER")
            .with_parameter(
                FunctionParam::new("result", "INTEGER").with_mode(FunctionParameterMode::Out),
            )
            .with_body("SELECT 1");
        let result = manager.validate(&spec);
        assert!(
            matches!(result, Err(FunctionError::FunctionsNotSupported))
                || matches!(result, Err(FunctionError::OutParametersNotSupported))
        );
    }

    #[test]
    fn test_validate_success() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("add_numbers", "INTEGER")
            .with_parameter(FunctionParam::new("a", "INTEGER"))
            .with_parameter(FunctionParam::new("b", "INTEGER"))
            .with_body("SELECT a + b");
        assert!(manager.validate(&spec).is_ok());
    }
}

mod postgres_function_tests {
    use super::*;

    #[test]
    fn test_simple_function() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("increment", "INTEGER")
            .with_parameter(FunctionParam::new("x", "INTEGER"))
            .with_body("SELECT x + 1")
            .with_language(FunctionLanguage::Sql);

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("CREATE FUNCTION increment(x INTEGER)"));
        assert!(sql.contains("RETURNS INTEGER"));
        assert!(sql.contains("LANGUAGE SQL"));
        assert!(sql.contains("SELECT x + 1"));
    }

    #[test]
    fn test_function_with_schema() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("my_func", "TEXT")
            .with_schema("utils")
            .with_body("SELECT 'hello'")
            .with_language(FunctionLanguage::Sql);

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("CREATE FUNCTION utils.my_func"));
    }

    #[test]
    fn test_function_immutable() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("double", "INTEGER")
            .with_parameter(FunctionParam::new("x", "INTEGER"))
            .with_body("SELECT x * 2")
            .with_volatility(FunctionVolatility::Immutable);

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("IMMUTABLE"));
    }

    #[test]
    fn test_function_strict() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("safe_length", "INTEGER")
            .with_parameter(FunctionParam::new("s", "TEXT"))
            .with_body("SELECT LENGTH(s)")
            .with_null_behavior(NullBehavior::Strict);

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("STRICT"));
    }

    #[test]
    fn test_function_security_definer() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("admin_operation", "VOID")
            .with_body("DELETE FROM logs WHERE created_at < NOW() - INTERVAL '30 days'")
            .with_security(SecurityMode::Definer);

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("SECURITY DEFINER"));
    }

    #[test]
    fn test_function_parallel_safe() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("pure_func", "INTEGER")
            .with_body("SELECT 42")
            .parallel_safe();

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("PARALLEL SAFE"));
    }

    #[test]
    fn test_function_with_cost() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("expensive_func", "INTEGER")
            .with_body("SELECT 1")
            .with_cost(1000);

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("COST 1000"));
    }

    #[test]
    fn test_function_returns_setof() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("get_users", "users")
            .with_body("SELECT * FROM users")
            .returns_set();

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("RETURNS SETOF users"));
    }

    #[test]
    fn test_function_returns_table() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("get_user_info", "TABLE")
            .returns_table(vec![
                FunctionParam::new("id", "INTEGER"),
                FunctionParam::new("name", "TEXT"),
            ])
            .with_body("SELECT id, name FROM users");

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("RETURNS TABLE (id INTEGER, name TEXT)"));
    }

    #[test]
    fn test_function_with_out_param() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("get_stats", "VOID")
            .with_parameter(
                FunctionParam::new("count_out", "INTEGER").with_mode(FunctionParameterMode::Out),
            )
            .with_body("SELECT COUNT(*) INTO count_out FROM users");

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("OUT count_out INTEGER"));
    }

    #[test]
    fn test_function_with_default_param() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("greet", "TEXT")
            .with_parameter(FunctionParam::new("name", "TEXT").with_default("'World'"))
            .with_body("SELECT 'Hello, ' || name");

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("name TEXT DEFAULT 'World'"));
    }

    #[test]
    fn test_function_plpgsql() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("add_user", "INTEGER")
            .with_parameter(FunctionParam::new("user_name", "TEXT"))
            .with_body("BEGIN\n    INSERT INTO users (name) VALUES (user_name);\n    RETURN currval('users_id_seq');\nEND")
            .with_language(FunctionLanguage::PlPgSql);

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("LANGUAGE plpgsql"));
        assert!(sql.contains("BEGIN"));
        assert!(sql.contains("END"));
    }
}

mod mysql_function_tests {
    use super::*;

    #[test]
    fn test_simple_function() {
        let manager = FunctionManager::new(FunctionDialect::MySQL);
        let spec = FunctionSpec::new("increment", "INTEGER")
            .with_parameter(FunctionParam::new("x", "INTEGER"))
            .with_body("RETURN x + 1");

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("CREATE FUNCTION increment(x INTEGER)"));
        assert!(sql.contains("RETURNS INTEGER"));
        assert!(sql.contains("BEGIN"));
        assert!(sql.contains("RETURN x + 1"));
        assert!(sql.contains("END"));
    }

    #[test]
    fn test_deterministic_function() {
        let manager = FunctionManager::new(FunctionDialect::MySQL);
        let spec = FunctionSpec::new("double", "INTEGER")
            .with_parameter(FunctionParam::new("x", "INTEGER"))
            .with_body("RETURN x * 2")
            .with_volatility(FunctionVolatility::Immutable);

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("DETERMINISTIC"));
    }

    #[test]
    fn test_security_definer() {
        let manager = FunctionManager::new(FunctionDialect::MySQL);
        let spec = FunctionSpec::new("admin_func", "INTEGER")
            .with_body("RETURN 1")
            .with_security(SecurityMode::Definer);

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("SQL SECURITY DEFINER"));
    }
}

mod mssql_function_tests {
    use super::*;

    #[test]
    fn test_scalar_function() {
        let manager = FunctionManager::new(FunctionDialect::MsSql);
        let spec = FunctionSpec::new("increment", "INTEGER")
            .with_parameter(FunctionParam::new("x", "INTEGER"))
            .with_body("RETURN @x + 1");

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("CREATE FUNCTION increment(@x INTEGER)"));
        assert!(sql.contains("RETURNS INTEGER"));
        assert!(sql.contains("BEGIN"));
        assert!(sql.contains("RETURN @x + 1"));
        assert!(sql.contains("END"));
    }

    #[test]
    fn test_function_with_schema() {
        let manager = FunctionManager::new(FunctionDialect::MsSql);
        let spec = FunctionSpec::new("my_func", "INT")
            .with_schema("dbo")
            .with_body("RETURN 1");

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("CREATE FUNCTION dbo.my_func"));
    }

    #[test]
    fn test_table_valued_function() {
        let manager = FunctionManager::new(FunctionDialect::MsSql);
        let spec = FunctionSpec::new("get_users", "TABLE")
            .returns_table(vec![
                FunctionParam::new("id", "INT"),
                FunctionParam::new("name", "VARCHAR(100)"),
            ])
            .with_body("SELECT id, name FROM users");

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("RETURNS TABLE"));
        assert!(sql.contains("id INT"));
        assert!(sql.contains("name VARCHAR(100)"));
    }

    #[test]
    fn test_parameter_with_output() {
        let manager = FunctionManager::new(FunctionDialect::MsSql);
        let spec = FunctionSpec::new("get_count", "INT")
            .with_parameter(
                FunctionParam::new("result", "INT").with_mode(FunctionParameterMode::Out),
            )
            .with_body("SET @result = 1\nRETURN @result");

        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("@result INT OUTPUT"));
    }
}

mod drop_function_tests {
    use super::*;

    #[test]
    fn test_postgres_drop() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let sql = manager.build_drop_function("my_func", Some(&["INTEGER", "TEXT"]), false, false);
        assert_eq!(sql, "DROP FUNCTION my_func(INTEGER, TEXT)");
    }

    #[test]
    fn test_postgres_drop_if_exists() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let sql = manager.build_drop_function("my_func", Some(&["INTEGER"]), true, false);
        assert!(sql.contains("IF EXISTS"));
    }

    #[test]
    fn test_postgres_drop_cascade() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let sql = manager.build_drop_function("my_func", None, false, true);
        assert!(sql.contains("CASCADE"));
    }

    #[test]
    fn test_mysql_drop() {
        let manager = FunctionManager::new(FunctionDialect::MySQL);
        let sql = manager.build_drop_function("my_func", None, false, false);
        assert_eq!(sql, "DROP FUNCTION my_func");
    }

    #[test]
    fn test_mysql_drop_if_exists() {
        let manager = FunctionManager::new(FunctionDialect::MySQL);
        let sql = manager.build_drop_function("my_func", None, true, false);
        assert!(sql.contains("IF EXISTS"));
    }

    #[test]
    fn test_mssql_drop() {
        let manager = FunctionManager::new(FunctionDialect::MsSql);
        let sql = manager.build_drop_function("my_func", None, false, false);
        assert_eq!(sql, "DROP FUNCTION my_func");
    }

    #[test]
    fn test_mssql_drop_if_exists() {
        let manager = FunctionManager::new(FunctionDialect::MsSql);
        let sql = manager.build_drop_function("dbo.my_func", None, true, false);
        assert!(sql.contains("IF OBJECT_ID"));
        assert!(sql.contains("'FN'"));
    }
}

mod create_or_replace_tests {
    use super::*;

    #[test]
    fn test_postgres_create_or_replace() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("my_func", "INTEGER").with_body("SELECT 1");

        let sql = manager.build_create_or_replace_function(&spec).unwrap();
        assert!(sql.contains("CREATE OR REPLACE FUNCTION"));
    }

    #[test]
    fn test_mssql_create_or_alter() {
        let manager = FunctionManager::new(FunctionDialect::MsSql);
        let spec = FunctionSpec::new("my_func", "INT").with_body("RETURN 1");

        let sql = manager.build_create_or_replace_function(&spec).unwrap();
        assert!(sql.contains("CREATE OR ALTER FUNCTION"));
    }

    #[test]
    fn test_mysql_create_or_replace_not_supported() {
        let manager = FunctionManager::new(FunctionDialect::MySQL);
        let spec = FunctionSpec::new("my_func", "INTEGER").with_body("RETURN 1");

        let result = manager.build_create_or_replace_function(&spec);
        assert!(result.is_err());
    }
}

mod comment_tests {
    use super::*;

    #[test]
    fn test_postgres_comment() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let sql = manager.build_comment("my_func", Some(&["INTEGER"]), Some("Adds one to input"));
        assert!(sql.is_some());
        assert!(sql.unwrap().contains("COMMENT ON FUNCTION"));
    }

    #[test]
    fn test_postgres_remove_comment() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let sql = manager.build_comment("my_func", None, None);
        assert!(sql.is_some());
        assert!(sql.unwrap().contains("IS NULL"));
    }

    #[test]
    fn test_mysql_comment_not_supported() {
        let manager = FunctionManager::new(FunctionDialect::MySQL);
        let sql = manager.build_comment("my_func", None, Some("Comment"));
        assert!(sql.is_none());
    }
}

mod alter_owner_tests {
    use super::*;

    #[test]
    fn test_postgres_alter_owner() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let sql = manager.build_alter_owner("my_func", Some(&["INTEGER"]), "admin");
        assert!(sql.is_some());
        assert!(
            sql.unwrap()
                .contains("ALTER FUNCTION my_func(INTEGER) OWNER TO admin")
        );
    }

    #[test]
    fn test_mysql_alter_owner_not_supported() {
        let manager = FunctionManager::new(FunctionDialect::MySQL);
        let sql = manager.build_alter_owner("my_func", None, "admin");
        assert!(sql.is_none());
    }
}

mod quoting_tests {
    use super::*;

    #[test]
    fn test_postgres_quote_reserved_keyword() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("select", "INTEGER").with_body("SELECT 1");
        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("\"select\""));
    }

    #[test]
    fn test_mysql_quote_backticks() {
        let manager = FunctionManager::new(FunctionDialect::MySQL);
        let spec = FunctionSpec::new("select", "INTEGER").with_body("RETURN 1");
        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("`select`"));
    }

    #[test]
    fn test_mssql_quote_brackets() {
        let manager = FunctionManager::new(FunctionDialect::MsSql);
        let spec = FunctionSpec::new("select", "INT").with_body("RETURN 1");
        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("[select]"));
    }

    #[test]
    fn test_no_quoting_needed() {
        let manager = FunctionManager::new(FunctionDialect::PostgreSQL);
        let spec = FunctionSpec::new("my_valid_func", "INTEGER").with_body("SELECT 1");
        let sql = manager.build_create_function(&spec).unwrap();
        assert!(sql.contains("my_valid_func("));
        assert!(!sql.contains("\"my_valid_func\""));
    }
}

mod error_display_tests {
    use super::*;

    #[test]
    fn test_error_display() {
        assert_eq!(
            FunctionError::EmptyName.to_string(),
            "Function name cannot be empty"
        );
        assert_eq!(
            FunctionError::EmptyReturnType.to_string(),
            "Return type cannot be empty"
        );
        assert_eq!(
            FunctionError::EmptyBody.to_string(),
            "Function body cannot be empty"
        );
        assert_eq!(
            FunctionError::FunctionsNotSupported.to_string(),
            "User-defined functions are not supported by this dialect"
        );
    }

    #[test]
    fn test_parameter_errors() {
        assert_eq!(
            FunctionError::EmptyParameterName.to_string(),
            "Parameter name cannot be empty"
        );
        assert_eq!(
            FunctionError::EmptyParameterType.to_string(),
            "Parameter type cannot be empty"
        );
        assert_eq!(
            FunctionError::InvalidParameter("test error".to_string()).to_string(),
            "Invalid parameter: test error"
        );
    }
}
