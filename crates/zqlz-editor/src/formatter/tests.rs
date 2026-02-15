//! Unit tests for SQL Formatter

use super::*;

// ============================================================================
// FormatterConfig Tests
// ============================================================================

mod config_tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = FormatterConfig::default();
        assert_eq!(config.indent_size(), 2);
        assert!(config.uppercase_keywords());
        assert_eq!(config.lines_between_queries(), 1);
    }

    #[test]
    fn test_config_builder_indent_size() {
        let config = FormatterConfig::new().with_indent_size(4);
        assert_eq!(config.indent_size(), 4);
    }

    #[test]
    fn test_config_builder_uppercase_keywords() {
        let config = FormatterConfig::new().with_uppercase_keywords(false);
        assert!(!config.uppercase_keywords());
    }

    #[test]
    fn test_config_builder_lines_between_queries() {
        let config = FormatterConfig::new().with_lines_between_queries(3);
        assert_eq!(config.lines_between_queries(), 3);
    }

    #[test]
    fn test_config_builder_chaining() {
        let config = FormatterConfig::new()
            .with_indent_size(4)
            .with_uppercase_keywords(false)
            .with_lines_between_queries(2);

        assert_eq!(config.indent_size(), 4);
        assert!(!config.uppercase_keywords());
        assert_eq!(config.lines_between_queries(), 2);
    }

    #[test]
    fn test_compact_preset() {
        let config = FormatterConfig::compact();
        assert_eq!(config.indent_size(), 0);
        assert!(config.uppercase_keywords());
        assert_eq!(config.lines_between_queries(), 0);
    }

    #[test]
    fn test_verbose_preset() {
        let config = FormatterConfig::verbose();
        assert_eq!(config.indent_size(), 4);
        assert!(config.uppercase_keywords());
        assert_eq!(config.lines_between_queries(), 2);
    }

    #[test]
    fn test_config_serialization() {
        let config = FormatterConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: FormatterConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_config_clone() {
        let config = FormatterConfig::default().with_indent_size(8);
        let cloned = config.clone();
        assert_eq!(config, cloned);
    }
}

// ============================================================================
// SqlFormatter Tests
// ============================================================================

mod format_tests {
    use super::*;

    #[test]
    fn test_format_simple_select() {
        let formatter = SqlFormatter::with_defaults();
        let result = formatter.format("select * from users").unwrap();
        assert!(result.contains("SELECT"));
        assert!(result.contains("FROM"));
        assert!(result.contains("users"));
    }

    #[test]
    fn test_format_uppercase_keywords() {
        let formatter = SqlFormatter::new(FormatterConfig::default().with_uppercase_keywords(true));
        let result = formatter
            .format("select id, name from users where active = true")
            .unwrap();
        assert!(result.contains("SELECT"));
        assert!(result.contains("FROM"));
        assert!(result.contains("WHERE"));
    }

    #[test]
    fn test_format_lowercase_keywords() {
        let formatter =
            SqlFormatter::new(FormatterConfig::default().with_uppercase_keywords(false));
        let result = formatter
            .format("SELECT ID, NAME FROM USERS WHERE ACTIVE = TRUE")
            .unwrap();
        assert!(result.contains("select"));
        assert!(result.contains("from"));
        assert!(result.contains("where"));
    }

    #[test]
    fn test_format_with_where_clause() {
        let formatter = SqlFormatter::with_defaults();
        let result = formatter
            .format("select * from users where id = 1 and active = true")
            .unwrap();
        assert!(result.contains("WHERE"));
        assert!(result.contains("AND"));
    }

    #[test]
    fn test_format_with_join() {
        let formatter = SqlFormatter::with_defaults();
        let sql = "select u.id, o.total from users u join orders o on u.id = o.user_id";
        let result = formatter.format(sql).unwrap();
        assert!(result.contains("JOIN"));
        assert!(result.contains("ON"));
    }

    #[test]
    fn test_format_insert_statement() {
        let formatter = SqlFormatter::with_defaults();
        let sql = "insert into users (id, name, email) values (1, 'John', 'john@example.com')";
        let result = formatter.format(sql).unwrap();
        assert!(result.contains("INSERT"));
        assert!(result.contains("INTO"));
        assert!(result.contains("VALUES"));
    }

    #[test]
    fn test_format_update_statement() {
        let formatter = SqlFormatter::with_defaults();
        let sql = "update users set name = 'Jane', updated_at = now() where id = 1";
        let result = formatter.format(sql).unwrap();
        assert!(result.contains("UPDATE"));
        assert!(result.contains("SET"));
        assert!(result.contains("WHERE"));
    }

    #[test]
    fn test_format_delete_statement() {
        let formatter = SqlFormatter::with_defaults();
        let sql = "delete from users where id = 1";
        let result = formatter.format(sql).unwrap();
        assert!(result.contains("DELETE"));
        assert!(result.contains("FROM"));
        assert!(result.contains("WHERE"));
    }

    #[test]
    fn test_format_create_table() {
        let formatter = SqlFormatter::with_defaults();
        let sql = "create table users (id int primary key, name varchar(255), email varchar(255))";
        let result = formatter.format(sql).unwrap();
        assert!(result.contains("CREATE"));
        assert!(result.contains("TABLE"));
    }

    #[test]
    fn test_format_empty_input_error() {
        let formatter = SqlFormatter::with_defaults();
        let result = formatter.format("");
        assert!(matches!(result, Err(FormatError::EmptyInput)));
    }

    #[test]
    fn test_format_whitespace_only_error() {
        let formatter = SqlFormatter::with_defaults();
        let result = formatter.format("   \n\t   ");
        assert!(matches!(result, Err(FormatError::EmptyInput)));
    }

    #[test]
    fn test_format_preserves_comments_single_line() {
        let formatter = SqlFormatter::with_defaults();
        let sql = "-- This is a comment\nselect * from users";
        let result = formatter.format_preserving_comments(sql).unwrap();
        assert!(result.contains("-- This is a comment"));
    }

    #[test]
    fn test_format_preserves_comments_block() {
        let formatter = SqlFormatter::with_defaults();
        let sql = "/* Multi-line\ncomment */ select * from users";
        let result = formatter.format_preserving_comments(sql).unwrap();
        assert!(result.contains("/*") || result.contains("Multi-line"));
    }

    #[test]
    fn test_format_complex_query() {
        let formatter = SqlFormatter::with_defaults();
        let sql = r#"
            select u.id, u.name, count(o.id) as order_count
            from users u
            left join orders o on u.id = o.user_id
            where u.active = true
            group by u.id, u.name
            having count(o.id) > 5
            order by order_count desc
            limit 10
        "#;
        let result = formatter.format(sql).unwrap();
        assert!(result.contains("SELECT"));
        assert!(result.contains("LEFT JOIN"));
        assert!(result.contains("GROUP BY"));
        assert!(result.contains("HAVING"));
        assert!(result.contains("ORDER BY"));
        assert!(result.contains("LIMIT"));
    }

    #[test]
    fn test_format_subquery() {
        let formatter = SqlFormatter::with_defaults();
        let sql = "select * from users where id in (select user_id from orders where total > 100)";
        let result = formatter.format(sql).unwrap();
        assert!(result.contains("SELECT"));
        assert!(result.contains("IN"));
    }

    #[test]
    fn test_format_cte() {
        let formatter = SqlFormatter::with_defaults();
        let sql = "with active_users as (select * from users where active = true) select * from active_users";
        let result = formatter.format(sql).unwrap();
        assert!(result.contains("WITH"));
        assert!(result.contains("AS"));
    }

    #[test]
    fn test_format_trailing_newline() {
        let formatter = SqlFormatter::with_defaults();
        let result = formatter.format("select 1").unwrap();
        assert!(result.ends_with('\n'));
        // Should only have one trailing newline
        assert!(!result.ends_with("\n\n"));
    }

    #[test]
    fn test_format_normalizes_line_endings() {
        let formatter = SqlFormatter::with_defaults();
        let sql = "select\r\n*\r\nfrom\r\nusers";
        let result = formatter.format(sql).unwrap();
        assert!(!result.contains("\r\n"));
    }
}

// ============================================================================
// Convenience Function Tests
// ============================================================================

mod convenience_function_tests {
    use super::*;

    #[test]
    fn test_format_sql_function() {
        let result = format_sql("select * from users").unwrap();
        assert!(result.contains("SELECT"));
        assert!(result.contains("FROM"));
    }

    #[test]
    fn test_format_sql_with_config_function() {
        let config = FormatterConfig::default().with_uppercase_keywords(false);
        let result = format_sql_with_config("SELECT * FROM USERS", config).unwrap();
        assert!(result.contains("select"));
    }

    #[test]
    fn test_format_sql_empty_error() {
        let result = format_sql("");
        assert!(matches!(result, Err(FormatError::EmptyInput)));
    }
}

// ============================================================================
// Validation Tests
// ============================================================================

mod validation_tests {
    use super::*;

    #[test]
    fn test_validate_valid_select() {
        let formatter = SqlFormatter::with_defaults();
        let result = formatter.validate("SELECT * FROM users");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_valid_insert() {
        let formatter = SqlFormatter::with_defaults();
        let result = formatter.validate("INSERT INTO users (name) VALUES ('test')");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_empty_input() {
        let formatter = SqlFormatter::with_defaults();
        let result = formatter.validate("");
        assert!(matches!(result, Err(FormatError::EmptyInput)));
    }

    #[test]
    fn test_validate_invalid_syntax() {
        let formatter = SqlFormatter::with_defaults();
        let result = formatter.validate("SELEKT * FORM users");
        assert!(matches!(result, Err(FormatError::InvalidSyntax(_))));
    }

    #[test]
    fn test_validate_incomplete_statement() {
        let formatter = SqlFormatter::with_defaults();
        let result = formatter.validate("SELECT FROM");
        assert!(matches!(result, Err(FormatError::InvalidSyntax(_))));
    }
}

// ============================================================================
// Multiple Statement Tests
// ============================================================================

mod multiple_statement_tests {
    use super::*;

    #[test]
    fn test_format_multiple_statements() {
        let formatter = SqlFormatter::with_defaults();
        let sql = "select * from users; select * from orders";
        let result = formatter.format_multiple(sql).unwrap();
        assert!(result.contains("users"));
        assert!(result.contains("orders"));
    }

    #[test]
    fn test_format_multiple_empty_error() {
        let formatter = SqlFormatter::with_defaults();
        let result = formatter.format_multiple("");
        assert!(matches!(result, Err(FormatError::EmptyInput)));
    }
}

// ============================================================================
// Error Display Tests
// ============================================================================

mod error_tests {
    use super::*;

    #[test]
    fn test_empty_input_error_display() {
        let error = FormatError::EmptyInput;
        assert_eq!(error.to_string(), "empty SQL input");
    }

    #[test]
    fn test_invalid_syntax_error_display() {
        let error = FormatError::InvalidSyntax("unexpected token".to_string());
        assert!(error.to_string().contains("unexpected token"));
    }

    #[test]
    fn test_formatting_failed_error_display() {
        let error = FormatError::FormattingFailed("internal error".to_string());
        assert!(error.to_string().contains("internal error"));
    }
}
