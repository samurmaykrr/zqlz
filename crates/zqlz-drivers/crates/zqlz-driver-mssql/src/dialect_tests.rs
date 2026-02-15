//! Tests for MssqlDialect

use super::*;

mod quote_identifier_tests {
    use super::*;

    #[test]
    fn test_quote_simple_identifier() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_identifier("users"), "[users]");
    }

    #[test]
    fn test_quote_identifier_with_space() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_identifier("my table"), "[my table]");
    }

    #[test]
    fn test_quote_identifier_with_closing_bracket() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_identifier("data]value"), "[data]]value]");
    }

    #[test]
    fn test_quote_identifier_with_multiple_brackets() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_identifier("a]b]c"), "[a]]b]]c]");
    }

    #[test]
    fn test_quote_identifier_empty() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_identifier(""), "[]");
    }

    #[test]
    fn test_quote_identifier_reserved_keyword() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_identifier("select"), "[select]");
        assert_eq!(dialect.quote_identifier("from"), "[from]");
        assert_eq!(dialect.quote_identifier("where"), "[where]");
    }
}

mod limit_clause_tests {
    use super::*;

    #[test]
    fn test_limit_clause_top_no_offset() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.limit_clause(10, None), "TOP 10");
    }

    #[test]
    fn test_limit_clause_top_with_zero_offset() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.limit_clause(5, Some(0)), "TOP 5");
    }

    #[test]
    fn test_limit_clause_offset_fetch() {
        let dialect = MssqlDialect::new();
        assert_eq!(
            dialect.limit_clause(10, Some(20)),
            "OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY"
        );
    }

    #[test]
    fn test_limit_clause_offset_fetch_single_row() {
        let dialect = MssqlDialect::new();
        assert_eq!(
            dialect.limit_clause(1, Some(5)),
            "OFFSET 5 ROWS FETCH NEXT 1 ROWS ONLY"
        );
    }

    #[test]
    fn test_limit_clause_large_values() {
        let dialect = MssqlDialect::new();
        assert_eq!(
            dialect.limit_clause(1000000, Some(5000000)),
            "OFFSET 5000000 ROWS FETCH NEXT 1000000 ROWS ONLY"
        );
    }

    #[test]
    fn test_limit_clause_for_subquery() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.limit_clause_for_subquery(100), "TOP 100");
    }
}

mod order_by_with_pagination_tests {
    use super::*;

    #[test]
    fn test_order_by_with_pagination() {
        let dialect = MssqlDialect::new();
        assert_eq!(
            dialect.order_by_with_pagination("id ASC", 10, 0),
            "ORDER BY id ASC OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY"
        );
    }

    #[test]
    fn test_order_by_with_pagination_and_offset() {
        let dialect = MssqlDialect::new();
        assert_eq!(
            dialect.order_by_with_pagination("created_at DESC", 25, 50),
            "ORDER BY created_at DESC OFFSET 50 ROWS FETCH NEXT 25 ROWS ONLY"
        );
    }

    #[test]
    fn test_order_by_with_pagination_multiple_columns() {
        let dialect = MssqlDialect::new();
        assert_eq!(
            dialect.order_by_with_pagination("last_name ASC, first_name ASC", 100, 200),
            "ORDER BY last_name ASC, first_name ASC OFFSET 200 ROWS FETCH NEXT 100 ROWS ONLY"
        );
    }
}

mod quote_string_tests {
    use super::*;

    #[test]
    fn test_quote_string_simple() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_string("hello"), "'hello'");
    }

    #[test]
    fn test_quote_string_with_single_quote() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_string("it's"), "'it''s'");
    }

    #[test]
    fn test_quote_string_with_multiple_quotes() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_string("it's Mike's"), "'it''s Mike''s'");
    }

    #[test]
    fn test_quote_string_empty() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_string(""), "''");
    }
}

mod needs_quoting_tests {
    use super::*;

    #[test]
    fn test_needs_quoting_simple_identifier() {
        let dialect = MssqlDialect::new();
        assert!(!dialect.needs_quoting("users"));
        assert!(!dialect.needs_quoting("my_table"));
        assert!(!dialect.needs_quoting("Column1"));
    }

    #[test]
    fn test_needs_quoting_reserved_keyword() {
        let dialect = MssqlDialect::new();
        assert!(dialect.needs_quoting("select"));
        assert!(dialect.needs_quoting("SELECT"));
        assert!(dialect.needs_quoting("from"));
        assert!(dialect.needs_quoting("where"));
        assert!(dialect.needs_quoting("order"));
        assert!(dialect.needs_quoting("table"));
    }

    #[test]
    fn test_needs_quoting_starts_with_digit() {
        let dialect = MssqlDialect::new();
        assert!(dialect.needs_quoting("123col"));
        assert!(dialect.needs_quoting("1table"));
    }

    #[test]
    fn test_needs_quoting_special_characters() {
        let dialect = MssqlDialect::new();
        assert!(dialect.needs_quoting("my table"));
        assert!(dialect.needs_quoting("user-name"));
        assert!(dialect.needs_quoting("value@key"));
        assert!(dialect.needs_quoting("data.column"));
    }

    #[test]
    fn test_needs_quoting_empty() {
        let dialect = MssqlDialect::new();
        assert!(dialect.needs_quoting(""));
    }
}

mod quote_identifier_if_needed_tests {
    use super::*;

    #[test]
    fn test_quote_identifier_if_needed_simple() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_identifier_if_needed("users"), "users");
        assert_eq!(dialect.quote_identifier_if_needed("my_column"), "my_column");
    }

    #[test]
    fn test_quote_identifier_if_needed_reserved() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_identifier_if_needed("select"), "[select]");
        assert_eq!(dialect.quote_identifier_if_needed("ORDER"), "[ORDER]");
    }

    #[test]
    fn test_quote_identifier_if_needed_special_chars() {
        let dialect = MssqlDialect::new();
        assert_eq!(dialect.quote_identifier_if_needed("my table"), "[my table]");
        assert_eq!(dialect.quote_identifier_if_needed("123abc"), "[123abc]");
    }
}

mod dialect_info_tests {
    use super::*;

    #[test]
    fn test_dialect_info_id() {
        let dialect = MssqlDialect::new();
        let info = dialect.dialect_info();
        assert_eq!(info.id.as_ref(), "mssql");
    }

    #[test]
    fn test_dialect_info_identifier_quote() {
        let dialect = MssqlDialect::new();
        let info = dialect.dialect_info();
        assert_eq!(info.identifier_quote, '[');
    }

    #[test]
    fn test_dialect_info_has_keywords() {
        let dialect = MssqlDialect::new();
        let info = dialect.dialect_info();
        assert!(!info.keywords.is_empty());
        assert!(info.keywords.iter().any(|k| k.keyword == "SELECT"));
        assert!(info.keywords.iter().any(|k| k.keyword == "TOP"));
        assert!(info.keywords.iter().any(|k| k.keyword == "MERGE"));
    }

    #[test]
    fn test_dialect_info_has_functions() {
        let dialect = MssqlDialect::new();
        let info = dialect.dialect_info();
        assert!(!info.functions.is_empty());
        assert!(info.functions.iter().any(|f| f.name == "GETDATE"));
        assert!(info.functions.iter().any(|f| f.name == "IIF"));
        assert!(info.functions.iter().any(|f| f.name == "JSON_VALUE"));
    }

    #[test]
    fn test_dialect_info_has_data_types() {
        let dialect = MssqlDialect::new();
        let info = dialect.dialect_info();
        assert!(!info.data_types.is_empty());
        assert!(info.data_types.iter().any(|t| t.name == "INT"));
        assert!(info.data_types.iter().any(|t| t.name == "DATETIME2"));
        assert!(info.data_types.iter().any(|t| t.name == "UNIQUEIDENTIFIER"));
    }
}
