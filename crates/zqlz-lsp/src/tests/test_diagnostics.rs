//! Tests for SQL diagnostics and error reporting

use super::test_helpers::*;
use lsp_types::{DiagnosticSeverity, NumberOrString};
use zqlz_core::dialect_config::{SyntaxHighlightingConfig, SyntaxOverlayModeName};
use zqlz_core::{DialectConfig, LanguageType, ParserConfig};
use zqlz_ui::widgets::input::Position;
use zqlz_ui::widgets::{Rope, RopeExt};

/// Helper to filter only syntax errors (not schema validation or best practices)
fn syntax_errors(diagnostics: &[lsp_types::Diagnostic]) -> Vec<&lsp_types::Diagnostic> {
    diagnostics
        .iter()
        .filter(|d| {
            d.severity == Some(DiagnosticSeverity::ERROR)
                && d.source.as_deref() != Some("schema")
                && d.source.as_deref() != Some("best-practices")
        })
        .collect()
}

fn best_practice_messages(diagnostics: &[lsp_types::Diagnostic]) -> Vec<&str> {
    diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.source.as_deref() == Some("best-practices"))
        .map(|diagnostic| diagnostic.message.as_str())
        .collect()
}

fn security_messages(diagnostics: &[lsp_types::Diagnostic]) -> Vec<&str> {
    diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.source.as_deref() == Some("security"))
        .map(|diagnostic| diagnostic.message.as_str())
        .collect()
}

fn diagnostic_code_is(diagnostic: &lsp_types::Diagnostic, expected_code: &str) -> bool {
    matches!(
        diagnostic.code.as_ref(),
        Some(NumberOrString::String(code)) if code == expected_code
    )
}

fn diagnostic_text<'a>(
    source: &'a str,
    diagnostic: &lsp_types::Diagnostic,
    text: &Rope,
) -> &'a str {
    let start = text.position_to_offset(&Position::new(
        diagnostic.range.start.line,
        diagnostic.range.start.character,
    ));
    let end = text.position_to_offset(&Position::new(
        diagnostic.range.end.line,
        diagnostic.range.end.character,
    ));
    &source[start..end]
}

#[test]
fn test_valid_sql_no_diagnostics() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");

    let diagnostics = lsp.validate_sql(&text);
    let errors = syntax_errors(&diagnostics);

    assert!(
        errors.is_empty(),
        "Valid SQL should have no syntax errors, got: {:?}",
        errors
    );
}

#[test]
fn test_select_wildcard_best_practice_uses_ast_projection() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT users.* FROM users");
    let diagnostics = lsp.validate_sql(&text);
    let best_practices = best_practice_messages(&diagnostics);

    assert!(
        best_practices
            .iter()
            .any(|message| message.contains("explicit column names")),
        "qualified SELECT wildcard should be detected from AST: {best_practices:?}"
    );
}

#[test]
fn sqlparser_syntax_range_uses_parser_token_location() {
    let mut lsp = create_test_lsp();
    let source = "SELECT * FORM users";
    let text = Rope::from(source);
    let diagnostics = lsp.validate_sql(&text);
    let sqlparser_error = diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic.source.as_deref() == Some("sqlparser")
                && diagnostic_code_is(
                    diagnostic,
                    crate::diagnostics::DIAGNOSTIC_CODE_SQLPARSER_SYNTAX,
                )
        })
        .expect("sqlparser syntax diagnostic");

    assert_eq!(
        diagnostic_text(source, sqlparser_error, &text),
        "FORM",
        "sqlparser syntax diagnostics should underline parser error token"
    );
}

#[test]
fn test_select_wildcard_diagnostic_has_stable_code() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");
    let diagnostics = lsp.validate_sql(&text);

    let wildcard_warning = diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic_code_is(
                diagnostic,
                crate::diagnostics::DIAGNOSTIC_CODE_SELECT_WILDCARD,
            )
        })
        .expect("SELECT wildcard diagnostic code");

    assert_eq!(wildcard_warning.source.as_deref(), Some("best-practices"));
}

#[test]
fn test_select_wildcard_range_skips_arithmetic_star() {
    let mut lsp = create_test_lsp();
    let sql = "SELECT price * qty, * FROM orders";
    let text = Rope::from(sql);
    let diagnostics = lsp.validate_sql(&text);
    let wildcard_warning = diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic.source.as_deref() == Some("best-practices")
                && diagnostic.message.contains("explicit column names")
        })
        .expect("wildcard warning");

    let wildcard_offset = sql.rfind('*').expect("projection wildcard");
    let wildcard_position = text.offset_to_position(wildcard_offset);

    assert_eq!(wildcard_warning.range.start.line, wildcard_position.line);
    assert_eq!(
        wildcard_warning.range.start.character, wildcard_position.character,
        "SELECT wildcard warning should underline projection wildcard, not arithmetic star"
    );
}

#[test]
fn test_select_star_inside_string_or_comment_is_not_wildcard_best_practice() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT 'select * from users' AS note -- select * from comments");
    let diagnostics = lsp.validate_sql(&text);
    let best_practices = best_practice_messages(&diagnostics);

    assert!(
        !best_practices
            .iter()
            .any(|message| message.contains("explicit column names")),
        "protected text should not trigger SELECT wildcard warning: {best_practices:?}"
    );
}

#[test]
fn test_mongodb_shell_query_skips_sql_diagnostics() {
    let mut lsp = create_test_lsp_with_dialect(crate::SqlDialect::MongoDB);
    let text = Rope::from(
        r#"db.products.find(
  { $text: { $search: "developer iot" } },
  { sku: 1, name: 1, tags: 1 }
).sort({ score: { $meta: "textScore" } }).limit(5)"#,
    );

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        diagnostics.is_empty(),
        "MongoDB shell query should not be validated as SQL, got: {:?}",
        diagnostics
    );
}

#[test]
fn test_mongodb_alias_uses_driver_bundle_for_diagnostics() {
    let mut lsp = create_test_lsp_with_dialect(crate::SqlDialect::MongoDB);
    lsp.driver_type = "mongo".to_string();
    let text = Rope::from(r#"db.users.find({ active: true }).limit(10)"#);

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        diagnostics.is_empty(),
        "MongoDB alias should use driver bundle and skip SQL diagnostics, got: {:?}",
        diagnostics
    );
}

#[test]
fn command_custom_validator_uses_syntax_profile_not_config_id() {
    let mut diagnostics = crate::SqlDiagnostics::new();
    let text = Rope::from("GET");
    let config = DialectConfig {
        id: "redis-compatible".to_string(),
        display_name: "Redis Compatible".to_string(),
        language_type: LanguageType::Command,
        parser: ParserConfig {
            skip_sql_validation: true,
            skip_tree_sitter_errors: true,
            custom_validator: true,
        },
        syntax_highlighting: SyntaxHighlightingConfig {
            profile: Some(" Redis ".to_string()),
            overlays: SyntaxOverlayModeName::Command,
            command_syntax: true,
            sql_overlays: false,
            ..SyntaxHighlightingConfig::default()
        },
        ..DialectConfig::default()
    };

    let diagnostics = diagnostics.analyze_with_dialect(&text, None, Some(&config));

    assert!(
        diagnostics.iter().any(|diagnostic| diagnostic
            .message
            .contains("GET requires exactly 1 argument")),
        "Redis-compatible profile should use Redis validator, got: {:?}",
        diagnostics
    );
}

#[test]
fn diagnostics_sqlparser_dialect_uses_core_driver_aliases() {
    let mut diagnostics = crate::SqlDiagnostics::new();
    let text = Rope::from("SELECT [display name] FROM users");
    let config = DialectConfig {
        id: "turso".to_string(),
        display_name: "Turso".to_string(),
        parser: ParserConfig {
            skip_sql_validation: false,
            skip_tree_sitter_errors: true,
            custom_validator: false,
        },
        ..DialectConfig::default()
    };

    let diagnostics = diagnostics.analyze_with_dialect(&text, None, Some(&config));

    assert!(
        syntax_errors(&diagnostics).is_empty(),
        "Turso alias should use SQLite sqlparser dialect, got: {:?}",
        diagnostics
    );
}

#[test]
fn schema_validation_uses_active_sqlparser_dialect() {
    let mut lsp = create_test_lsp_with_dialect(crate::SqlDialect::PostgreSQL);
    let text = Rope::from("SELECT missing_column::TEXT FROM users");

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        diagnostics.iter().any(|diagnostic| {
            diagnostic.source.as_deref() == Some("schema")
                && diagnostic.message.contains("missing_column")
        }),
        "PostgreSQL cast syntax should still run schema validation, got: {:?}",
        diagnostics
    );
}

#[test]
fn schema_validation_unknown_column_range_uses_token_location() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT missing_column FROM users");
    let diagnostics = lsp.validate_sql(&text);

    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic.source.as_deref() == Some("schema")
                && diagnostic.message.contains("missing_column")
        })
        .expect("missing column schema diagnostic");

    assert_eq!(diagnostic.range.start.line, 0);
    assert_eq!(diagnostic.range.start.character, 7);
    assert_eq!(diagnostic.range.end.character, 21);
}

#[test]
fn schema_validation_unknown_table_range_uses_token_location() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM missing_table");
    let diagnostics = lsp.validate_sql(&text);

    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic.source.as_deref() == Some("schema")
                && diagnostic.message.contains("missing_table")
        })
        .expect("missing table schema diagnostic");

    assert_eq!(diagnostic.range.start.line, 0);
    assert_eq!(diagnostic.range.start.character, 14);
    assert_eq!(diagnostic.range.end.character, 27);
}

#[test]
fn schema_validation_qualified_unknown_table_range_uses_leaf_token_location() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM public.missing_table");
    let diagnostics = lsp.validate_sql(&text);

    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic.source.as_deref() == Some("schema")
                && diagnostic.message.contains("missing_table")
        })
        .expect("qualified missing table schema diagnostic");

    assert_eq!(diagnostic.range.start.line, 0);
    assert_eq!(diagnostic.range.start.character, 21);
    assert_eq!(diagnostic.range.end.character, 34);
}

#[test]
fn schema_validation_repeated_unknown_column_ranges_use_token_occurrences() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT missing_column FROM users WHERE missing_column = 1");
    let diagnostics = lsp.validate_sql(&text);

    let ranges: Vec<_> = diagnostics
        .iter()
        .filter(|diagnostic| {
            diagnostic.source.as_deref() == Some("schema")
                && diagnostic.message.contains("missing_column")
        })
        .map(|diagnostic| diagnostic.range)
        .collect();

    assert_eq!(ranges.len(), 2, "expected two missing-column diagnostics");
    assert_eq!(ranges[0].start.character, 7);
    assert_eq!(ranges[0].end.character, 21);
    assert_eq!(ranges[1].start.character, 39);
    assert_eq!(ranges[1].end.character, 53);
}

#[test]
fn schema_validation_same_text_table_and_column_ranges_do_not_share_occurrences() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT ghost FROM ghost");
    let diagnostics = lsp.validate_sql(&text);

    let column_diagnostic = diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic.source.as_deref() == Some("schema")
                && diagnostic.message.contains("Column 'ghost'")
        })
        .expect("missing column schema diagnostic");
    let table_diagnostic = diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic.source.as_deref() == Some("schema")
                && diagnostic.message.contains("Table 'ghost'")
        })
        .expect("missing table schema diagnostic");

    assert_eq!(column_diagnostic.range.start.character, 7);
    assert_eq!(column_diagnostic.range.end.character, 12);
    assert_eq!(table_diagnostic.range.start.character, 18);
    assert_eq!(table_diagnostic.range.end.character, 23);
}

#[test]
fn schema_validation_qualified_column_range_uses_qualified_occurrence() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT missing_column, u.missing_column FROM users u");
    let diagnostics = lsp.validate_sql(&text);

    let ranges: Vec<_> = diagnostics
        .iter()
        .filter(|diagnostic| {
            diagnostic.source.as_deref() == Some("schema")
                && diagnostic.message.contains("missing_column")
        })
        .map(|diagnostic| diagnostic.range)
        .collect();

    assert_eq!(ranges.len(), 2, "expected two missing-column diagnostics");
    assert_eq!(ranges[0].start.character, 7);
    assert_eq!(ranges[0].end.character, 21);
    assert_eq!(ranges[1].start.character, 25);
    assert_eq!(ranges[1].end.character, 39);
}

#[test]
fn test_drop_inside_string_literal_is_not_sql_injection_diagnostic() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("INSERT INTO audit_log (message) VALUES ('; DROP TABLE users')");
    let diagnostics = lsp.validate_sql(&text);
    let security = security_messages(&diagnostics);

    assert!(
        security.is_empty(),
        "DROP text inside string literal should not trigger security diagnostic: {security:?}"
    );
}

#[test]
fn test_chained_drop_statement_reports_potential_sql_injection() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT 1; DROP TABLE users");
    let diagnostics = lsp.validate_sql(&text);
    let security = security_messages(&diagnostics);

    assert!(
        security
            .iter()
            .any(|message| message.contains("Potential SQL injection")),
        "actual chained DROP should trigger security diagnostic: {security:?}"
    );
}

#[test]
fn test_syntax_error_detected() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FORM users"); // FORM instead of FROM

    let diagnostics = lsp.validate_sql(&text);

    assert!(!diagnostics.is_empty(), "Syntax error should be detected");

    assert_eq!(
        diagnostics[0].severity,
        Some(DiagnosticSeverity::ERROR),
        "Should be an error"
    );
}

#[test]
fn test_unclosed_parenthesis() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT COUNT( FROM users");

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        !diagnostics.is_empty(),
        "Unclosed parenthesis should be detected"
    );
}

#[test]
fn test_unclosed_quote() {
    let mut lsp = create_test_lsp();
    let sql = "SELECT * FROM users WHERE name = 'john";
    let text = Rope::from(sql);

    let diagnostics = lsp.validate_sql(&text);

    assert!(!diagnostics.is_empty(), "Unclosed quote should be detected");
    let tokenizer_diagnostic = diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic.source.as_deref() == Some("sqlparser")
                && diagnostic.message.starts_with("SQL Tokenizer Error:")
        })
        .expect("structured tokenizer diagnostic");
    assert_eq!(
        tokenizer_diagnostic.range.start.character, 33,
        "unclosed quote range should use tokenizer location, not parser prose"
    );
}

#[test]
fn test_missing_select_clause() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("FROM users");

    let diagnostics = lsp.validate_sql(&text);

    assert!(!diagnostics.is_empty(), "Missing SELECT should be detected");
}

#[test]
fn test_multiple_syntax_errors() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT FORM users WERE name = 'test'");

    let diagnostics = lsp.validate_sql(&text);

    // May detect one or more errors depending on parser behavior
    assert!(!diagnostics.is_empty(), "Should detect syntax errors");
}

#[test]
fn test_incomplete_query() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM ");

    let diagnostics = lsp.validate_sql(&text);

    // Incomplete query may or may not be flagged as error depending on parser
    // Just ensure it doesn't panic
    println!("Diagnostics for incomplete query: {:?}", diagnostics);
}

#[test]
fn test_invalid_column_name_format() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT 123abc FROM users");

    let diagnostics = lsp.validate_sql(&text);

    // May or may not be flagged depending on dialect/parser
    // Mainly ensuring no panic
    println!("Diagnostics for invalid column name: {:?}", diagnostics);
}

#[test]
fn test_nested_query_syntax_error() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM (SELECT * FORM users)");

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        !diagnostics.is_empty(),
        "Syntax error in nested query should be detected"
    );
}

#[test]
fn test_join_without_on_clause() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users JOIN audit_log");

    let diagnostics = lsp.validate_sql(&text);

    // May or may not be an error depending on SQL dialect (some allow cross join syntax)
    println!("Diagnostics for JOIN without ON: {:?}", diagnostics);
}

#[test]
fn test_where_clause_missing_value() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE name =");

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        !diagnostics.is_empty(),
        "Missing value in WHERE clause should be detected"
    );
}

#[test]
fn test_group_by_without_aggregate() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT name FROM users GROUP BY");

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        !diagnostics.is_empty(),
        "Incomplete GROUP BY should be detected"
    );
}

#[test]
fn test_order_by_without_column() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users ORDER BY");

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        !diagnostics.is_empty(),
        "ORDER BY without column should be detected"
    );
}

#[test]
fn test_insert_without_values() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("INSERT INTO users (name)");

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        !diagnostics.is_empty(),
        "INSERT without VALUES should be detected"
    );
}

#[test]
fn test_update_without_set() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("UPDATE users WHERE id = 1");

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        !diagnostics.is_empty(),
        "UPDATE without SET should be detected"
    );
}

#[test]
fn test_delete_syntax() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("DELETE FROM users WHERE id = 1");

    let diagnostics = lsp.validate_sql(&text);

    assert!(diagnostics.is_empty(), "Valid DELETE should have no errors");
}

#[test]
fn test_create_table_syntax() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("CREATE TABLE test (id INTEGER PRIMARY KEY)");

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        diagnostics.is_empty(),
        "Valid CREATE TABLE should have no errors"
    );
}

#[test]
fn test_drop_table_syntax() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("DROP TABLE users");

    let diagnostics = lsp.validate_sql(&text);

    assert!(
        diagnostics.is_empty(),
        "Valid DROP TABLE should have no errors"
    );
}

#[test]
fn test_alter_table_syntax() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("ALTER TABLE users ADD COLUMN phone TEXT");

    let diagnostics = lsp.validate_sql(&text);

    // May vary by dialect
    println!("Diagnostics for ALTER TABLE: {:?}", diagnostics);
}

#[test]
fn test_case_expression_syntax() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users");

    let diagnostics = lsp.validate_sql(&text);
    let errors = syntax_errors(&diagnostics);

    assert!(
        errors.is_empty(),
        "Valid CASE expression should have no syntax errors, got: {:?}",
        errors
    );
}

#[test]
fn test_cte_syntax() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("WITH cte AS (SELECT * FROM users) SELECT * FROM cte");

    let diagnostics = lsp.validate_sql(&text);
    let errors = syntax_errors(&diagnostics);

    assert!(
        errors.is_empty(),
        "Valid CTE should have no syntax errors, got: {:?}",
        errors
    );
}

#[test]
fn test_union_syntax() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT name FROM users UNION SELECT name FROM locations");

    let diagnostics = lsp.validate_sql(&text);
    let errors = syntax_errors(&diagnostics);

    assert!(
        errors.is_empty(),
        "Valid UNION should have no syntax errors, got: {:?}",
        errors
    );
}

#[test]
fn test_window_function_syntax() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT name, ROW_NUMBER() OVER (ORDER BY created_at) FROM users");

    let diagnostics = lsp.validate_sql(&text);

    // May vary by SQL dialect support
    println!("Diagnostics for window function: {:?}", diagnostics);
}

#[test]
fn test_transaction_syntax() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("BEGIN; SELECT * FROM users; COMMIT;");

    let diagnostics = lsp.validate_sql(&text);

    // Multiple statements might not all be validated
    println!("Diagnostics for transaction: {:?}", diagnostics);
}

#[test]
fn test_multiple_statements() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users; SELECT * FROM locations;");

    let diagnostics = lsp.validate_sql(&text);
    let errors = syntax_errors(&diagnostics);

    // Should handle multiple statements without syntax errors
    assert!(
        errors.is_empty(),
        "Valid multiple statements should have no syntax errors, got: {:?}",
        errors
    );
}

// =============================================================================
// Common Real-World SQL Error Patterns
// =============================================================================

/// Helper to assert that diagnostics contain at least one error
fn assert_has_error(diagnostics: &[lsp_types::Diagnostic], context: &str) {
    assert!(
        !diagnostics.is_empty(),
        "{}: Expected diagnostics but got none",
        context
    );
    let has_error = diagnostics
        .iter()
        .any(|d| d.severity == Some(lsp_types::DiagnosticSeverity::ERROR));
    assert!(
        has_error,
        "{}: Expected at least one ERROR diagnostic",
        context
    );
}

/// Helper to assert no syntax errors in valid SQL
/// Note: This ignores schema validation errors since we don't have a real schema in tests
fn assert_no_syntax_errors(diagnostics: &[lsp_types::Diagnostic], context: &str) {
    let syntax_errors: Vec<_> = diagnostics
        .iter()
        .filter(|d| {
            d.severity == Some(lsp_types::DiagnosticSeverity::ERROR)
                && d.source.as_deref() != Some("schema")
        })
        .collect();
    assert!(
        syntax_errors.is_empty(),
        "{}: Expected no syntax errors but got: {:?}",
        context,
        syntax_errors
    );
}

// -----------------------------------------------------------------------------
// Typos in SQL Keywords (very common)
// -----------------------------------------------------------------------------

#[test]
fn test_typo_select_as_selct() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELCT * FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "SELCT typo");
}

#[test]
fn test_typo_from_as_form() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FORM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "FORM typo");
}

#[test]
fn test_typo_where_as_were() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "WERE typo");
}

#[test]
fn test_typo_insert_as_insret() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("INSRET INTO users VALUES (1)");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "INSRET typo");
}

#[test]
fn test_typo_update_as_udpate() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("UDPATE users SET name = 'test'");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "UDPATE typo");
}

#[test]
fn test_typo_delete_as_delet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("DELET FROM users WHERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "DELET typo");
}

// -----------------------------------------------------------------------------
// Missing Required Clauses
// -----------------------------------------------------------------------------

#[test]
fn test_select_without_from() {
    let mut lsp = create_test_lsp();
    // Just "SELECT name" without FROM - should be valid (selecting literal/expression)
    let text = Rope::from("SELECT 1 + 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "SELECT expression without FROM");
}

#[test]
fn test_update_missing_set() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("UPDATE users WHERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "UPDATE without SET");
}

#[test]
fn test_insert_missing_values() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("INSERT INTO users (name, email)");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "INSERT without VALUES");
}

#[test]
fn test_join_missing_on() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users INNER JOIN orders");
    let diagnostics = lsp.validate_sql(&text);
    // Note: Some dialects allow this as implicit cross join
    // We check that tree-sitter at least parses it
    println!("JOIN without ON diagnostics: {:?}", diagnostics);
}

// -----------------------------------------------------------------------------
// Incomplete Statements (user stopped typing mid-statement)
// -----------------------------------------------------------------------------

#[test]
fn test_incomplete_select_no_columns() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "SELECT with no columns");
}

#[test]
fn test_incomplete_select_trailing_comma() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT id, name, FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Trailing comma in SELECT");
}

#[test]
fn test_incomplete_where_no_condition() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "WHERE with no condition");
}

#[test]
fn test_incomplete_where_partial_condition() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id =");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Incomplete WHERE condition");
}

#[test]
fn test_incomplete_order_by() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users ORDER BY");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "ORDER BY with no column");
}

#[test]
fn test_incomplete_group_by() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT COUNT(*) FROM users GROUP BY");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "GROUP BY with no column");
}

// -----------------------------------------------------------------------------
// Unbalanced Delimiters (parentheses, quotes)
// -----------------------------------------------------------------------------

#[test]
fn test_unclosed_parenthesis_in_function() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT COUNT( FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Unclosed parenthesis in function");
}

#[test]
fn test_unclosed_parenthesis_in_subquery() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM (SELECT id FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Unclosed parenthesis in subquery");
}

#[test]
fn test_extra_closing_parenthesis() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users)");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Extra closing parenthesis");
}

#[test]
fn test_unclosed_single_quote() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE name = 'john");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Unclosed single quote");
}

#[test]
fn test_unclosed_double_quote() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE name = \"john");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Unclosed double quote");
}

// -----------------------------------------------------------------------------
// Trailing Garbage / Random Characters
// -----------------------------------------------------------------------------

#[test]
fn test_trailing_garbage_detected() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users 3");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Trailing number after table");
}

#[test]
fn test_garbage_after_semicolon_detected() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users; = x zx kd 3");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Garbage after semicolon");
}

#[test]
fn test_random_characters_in_query() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users @#$%");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Random special characters");
}

#[test]
fn test_incomplete_operator() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id <>");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Incomplete <> operator");
}

// -----------------------------------------------------------------------------
// Common Operator Mistakes
// -----------------------------------------------------------------------------

#[test]
fn test_assignment_instead_of_comparison() {
    let mut lsp = create_test_lsp();
    // Using single = is actually valid SQL for comparison
    let text = Rope::from("SELECT * FROM users WHERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Valid = comparison");
}

#[test]
fn test_double_equals_invalid() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id == 1");
    let diagnostics = lsp.validate_sql(&text);
    // Note: Some dialects (SQLite) accept == as valid comparison operator
    // This test documents the behavior rather than asserting error
    println!("== operator diagnostics: {:?}", diagnostics);
}

#[test]
fn test_javascript_not_equals() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id != 1");
    let diagnostics = lsp.validate_sql(&text);
    // != is valid in most SQL dialects
    println!("!= operator diagnostics: {:?}", diagnostics);
}

#[test]
fn test_javascript_strict_not_equals() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id !== 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Invalid !== operator");
}

// -----------------------------------------------------------------------------
// Multiline SQL Errors
// -----------------------------------------------------------------------------

#[test]
fn test_multiline_unclosed_string() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT *
FROM users
WHERE name = 'john
AND id = 1",
    );
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Multiline unclosed string");
}

#[test]
fn test_multiline_syntax_error_in_middle() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT id, name
FORM users
WHERE id = 1",
    );
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "FORM typo in multiline query");
}

#[test]
fn test_multiline_missing_comma() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT
    id
    name
    email
FROM users",
    );
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Missing commas between columns");
}

// -----------------------------------------------------------------------------
// Valid SQL (should have no errors)
// -----------------------------------------------------------------------------

#[test]
fn test_valid_simple_select() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT id, name FROM users WHERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Valid simple SELECT");
}

#[test]
fn test_sqlite_bracketed_identifiers_are_not_reported_as_syntax_errors() {
    let mut lsp = create_test_lsp_with_dialect(crate::SqlDialect::SQLite);
    let text = Rope::from(
        "CREATE VIEW [ ProductDetails_V ] AS
SELECT
    u.[user_id] AS [ UserId ],
    u.[username] AS [ UserName ]
FROM [ users ] u",
    );

    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(
        &diagnostics,
        "SQLite bracketed identifiers should parse without syntax errors",
    );
}

#[test]
fn test_sqlite_create_trigger_is_not_reported_as_syntax_error() {
    let mut lsp = create_test_lsp_with_dialect(crate::SqlDialect::SQLite);
    let text = Rope::from(
        r#"CREATE TRIGGER trg_user_sessions_insert_purge_old
AFTER
INSERT
    ON user_sessions
BEGIN
DELETE FROM
        user_sessions
WHERE
        deleted_at IS NOT NULL
        AND deleted_at < datetime('now', '-7 days');
DELETE FROM
        user_sessions
WHERE
        deleted_at IS NULL
        AND (
            (
                user_id IS NULL
                AND last_activity_time < datetime('now', '-30 days')
            )
            OR (
                user_id IS NOT NULL
                AND last_activity_time < datetime('now', '-180 days')
            )
        );
END"#,
    );

    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(
        &diagnostics,
        "SQLite CREATE TRIGGER DDL should not show parser diagnostics",
    );
}

#[test]
fn test_valid_select_with_join() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT u.id, u.name, o.total
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE u.active = 1",
    );
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Valid SELECT with JOIN");
}

#[test]
fn test_valid_insert() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')");
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Valid INSERT");
}

#[test]
fn test_valid_update() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("UPDATE users SET name = 'Jane' WHERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Valid UPDATE");
}

#[test]
fn test_create_table_updated_identifier_does_not_trigger_update_without_where() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        r#"CREATE TABLE "server_widget_queue" (
  "server_widget_queue_id" TEXT NOT NULL,
  "updated_at" TEXT DEFAULT NULL,
  "updated_by" TEXT DEFAULT NULL,
  PRIMARY KEY ("server_widget_queue_id")
)"#,
    );
    let diagnostics = lsp.validate_sql(&text);
    let best_practices = best_practice_messages(&diagnostics);

    assert!(
        !best_practices
            .iter()
            .any(|message| message.contains("UPDATE without WHERE")),
        "quoted column names containing updated should not trigger UPDATE warning: {best_practices:?}"
    );
}

#[test]
fn test_create_table_mixed_quoted_columns_do_not_trigger_dml_without_where() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        r#"CREATE TABLE "categories" (
  "category_id" text(255) NOT NULL,
  "title" text,
  "created_at" text,
  "created_by" text(255),
  "updated_at" text,
  "updated_by" text(255),
  "deleted_at" text,
  "deleted_by" text(255)
)"#,
    );
    let diagnostics = lsp.validate_sql(&text);
    let best_practices = best_practice_messages(&diagnostics);

    assert!(
        !best_practices
            .iter()
            .any(|message| message.contains("without WHERE")),
        "CREATE TABLE identifiers should not be treated as active DML statements: {best_practices:?}"
    );
}

#[test]
fn test_update_without_where_best_practice_uses_active_keyword_only() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        r#"-- UPDATE users SET name = 'commented'
UPDATE users SET updated_at = 'where is text literal'"#,
    );
    let diagnostics = lsp.validate_sql(&text);
    let best_practices = best_practice_messages(&diagnostics);

    assert!(
        best_practices
            .iter()
            .any(|message| message.contains("UPDATE without WHERE")),
        "real UPDATE without WHERE should still warn: {best_practices:?}"
    );
}

#[test]
fn test_update_without_where_diagnostic_has_stable_code() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("UPDATE users SET name = 'Jane'");
    let diagnostics = lsp.validate_sql(&text);

    let warning = diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic_code_is(
                diagnostic,
                crate::diagnostics::DIAGNOSTIC_CODE_DML_WITHOUT_WHERE,
            )
        })
        .expect("DML without WHERE diagnostic code");

    assert_eq!(warning.source.as_deref(), Some("best-practices"));
}

#[test]
fn test_update_without_where_range_after_expanding_unicode_prefix() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT 'İ'; UPDATE users SET name = 'Jane'");
    let diagnostics = lsp.validate_sql(&text);

    let warning = diagnostics
        .iter()
        .find(|diagnostic| {
            diagnostic.source.as_deref() == Some("best-practices")
                && diagnostic.message.contains("UPDATE without WHERE")
        })
        .expect("expected UPDATE without WHERE warning");

    assert_eq!(
        warning.range.start.character, 12,
        "warning should point at UPDATE after Unicode prefix, got {:?}",
        warning.range
    );
    assert_eq!(
        warning.range.end.character, 18,
        "warning should end after UPDATE, got {:?}",
        warning.range
    );
}

#[test]
fn test_where_inside_string_or_comment_does_not_hide_update_without_where() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        r#"UPDATE users
SET note = 'where only appears in string'
-- where only appears in comment"#,
    );
    let diagnostics = lsp.validate_sql(&text);
    let best_practices = best_practice_messages(&diagnostics);

    assert!(
        best_practices
            .iter()
            .any(|message| message.contains("UPDATE without WHERE")),
        "AST should report missing WHERE even when protected text contains where: {best_practices:?}"
    );
}

#[test]
fn test_valid_delete() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("DELETE FROM users WHERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Valid DELETE");
}

#[test]
fn test_valid_subquery() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)",
    );
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Valid subquery");
}

#[test]
fn test_valid_aggregate_with_group_by() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT department, COUNT(*) as cnt, AVG(salary) as avg_sal
FROM employees
GROUP BY department
HAVING COUNT(*) > 5
ORDER BY cnt DESC",
    );
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Valid aggregate query");
}

// =============================================================================
// Additional Common Invalid SQL Scenarios
// =============================================================================

// -----------------------------------------------------------------------------
// Wrong Clause Order
// -----------------------------------------------------------------------------

#[test]
fn test_where_before_from() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * WHERE id = 1 FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "WHERE before FROM");
}

#[test]
fn test_order_by_before_where() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users ORDER BY name WHERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "ORDER BY before WHERE");
}

#[test]
fn test_having_without_group_by() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users HAVING count(*) > 1");
    let diagnostics = lsp.validate_sql(&text);
    // Note: Some databases allow HAVING without GROUP BY
    println!("HAVING without GROUP BY: {:?}", diagnostics);
}

#[test]
fn test_group_by_after_order_by() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT name FROM users ORDER BY name GROUP BY name");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "GROUP BY after ORDER BY");
}

// -----------------------------------------------------------------------------
// Invalid Identifiers and Names
// -----------------------------------------------------------------------------

#[test]
fn test_table_name_starting_with_number() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM 123table");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Table name starting with number");
}

#[test]
fn test_reserved_word_as_unquoted_identifier() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT select FROM table");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Reserved words as identifiers");
}

#[test]
fn test_column_name_with_spaces_unquoted() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT first name FROM users");
    let diagnostics = lsp.validate_sql(&text);
    // Parser may interpret "first" and "name" as separate columns/aliases
    // This documents the actual behavior
    println!("Column with unquoted space: {:?}", diagnostics);
}

// -----------------------------------------------------------------------------
// Malformed Literals
// -----------------------------------------------------------------------------

#[test]
fn test_unclosed_bracket_identifier() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT [column name FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Unclosed bracket identifier");
}

#[test]
fn test_unclosed_backtick_identifier() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT `column name FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Unclosed backtick identifier");
}

#[test]
fn test_invalid_numeric_literal() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = 12.34.56");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Invalid numeric literal with multiple dots");
}

#[test]
fn test_invalid_hex_literal() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = 0xGHIJ");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Invalid hex literal");
}

// -----------------------------------------------------------------------------
// Malformed Expressions
// -----------------------------------------------------------------------------

#[test]
fn test_double_operator() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Double equals operator");
}

#[test]
fn test_missing_operator() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Missing operator between id and 1");
}

#[test]
fn test_dangling_and() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = 1 AND");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Dangling AND");
}

#[test]
fn test_dangling_or() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = 1 OR");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Dangling OR");
}

#[test]
fn test_empty_in_list() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id IN ()");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Empty IN list");
}

#[test]
fn test_in_without_parentheses() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id IN 1, 2, 3");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "IN without parentheses");
}

#[test]
fn test_between_without_and() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id BETWEEN 1 10");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "BETWEEN without AND");
}

#[test]
fn test_like_without_pattern() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE name LIKE");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "LIKE without pattern");
}

#[test]
fn test_is_without_null() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE name IS");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "IS without NULL/NOT NULL");
}

// -----------------------------------------------------------------------------
// Malformed JOINs
// -----------------------------------------------------------------------------

#[test]
fn test_join_on_without_condition() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users JOIN orders ON");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "JOIN ON without condition");
}

#[test]
fn test_join_using_unclosed() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users JOIN orders USING (user_id");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "USING with unclosed parenthesis");
}

#[test]
fn test_cross_join_with_on() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users CROSS JOIN orders ON users.id = orders.user_id");
    let diagnostics = lsp.validate_sql(&text);
    // Some databases allow this, others don't
    println!("CROSS JOIN with ON: {:?}", diagnostics);
}

#[test]
fn test_natural_join_with_on() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users NATURAL JOIN orders ON users.id = orders.user_id");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "NATURAL JOIN with ON clause");
}

// -----------------------------------------------------------------------------
// Malformed Subqueries
// -----------------------------------------------------------------------------

#[test]
fn test_subquery_without_alias() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM (SELECT * FROM users)");
    let diagnostics = lsp.validate_sql(&text);
    // Some databases require alias, some don't
    println!("Subquery without alias: {:?}", diagnostics);
}

#[test]
fn test_subquery_missing_select() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM (FROM users) AS sub");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Subquery missing SELECT");
}

#[test]
fn test_correlated_subquery_wrong_alias() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = x.id)",
    );
    let diagnostics = lsp.validate_sql(&text);
    // This should ideally be caught but may require semantic analysis
    println!("Wrong alias in correlated subquery: {:?}", diagnostics);
}

#[test]
fn test_valid_alias_reference_has_no_schema_diagnostic() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT u.user_id FROM users u WHERE u.username IS NOT NULL");
    let diagnostics = lsp.validate_sql(&text);

    assert!(
        !diagnostics.iter().any(|diagnostic| {
            diagnostic.source.as_deref() == Some("schema")
                && diagnostic.message.contains("Unknown table or alias")
        }),
        "Valid aliases should not produce schema alias diagnostics: {:?}",
        diagnostics
    );
}

// -----------------------------------------------------------------------------
// Malformed INSERT Statements
// -----------------------------------------------------------------------------

#[test]
fn test_insert_column_value_count_mismatch() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("INSERT INTO users (name, email, age) VALUES ('John', 'john@test.com')");
    let diagnostics = lsp.validate_sql(&text);
    // This is a semantic error, may or may not be caught
    println!("Column/value count mismatch: {:?}", diagnostics);
}

#[test]
fn test_insert_values_unclosed() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("INSERT INTO users (name) VALUES ('John'");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "INSERT VALUES unclosed");
}

#[test]
fn test_insert_duplicate_columns() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("INSERT INTO users (name, name) VALUES ('John', 'Jane')");
    let diagnostics = lsp.validate_sql(&text);
    // May or may not be caught at syntax level
    println!("Duplicate columns in INSERT: {:?}", diagnostics);
}

#[test]
fn test_insert_into_missing_table() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("INSERT INTO VALUES ('John')");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "INSERT INTO missing table name");
}

// -----------------------------------------------------------------------------
// Malformed UPDATE Statements
// -----------------------------------------------------------------------------

#[test]
fn test_update_set_missing_value() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("UPDATE users SET name =");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "UPDATE SET missing value");
}

#[test]
fn test_update_set_missing_column() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("UPDATE users SET = 'John'");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "UPDATE SET missing column");
}

#[test]
fn test_update_multiple_tables_wrong_syntax() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("UPDATE users, orders SET users.name = 'John'");
    let diagnostics = lsp.validate_sql(&text);
    // Multi-table UPDATE syntax varies by database
    println!("Multi-table UPDATE: {:?}", diagnostics);
}

// -----------------------------------------------------------------------------
// Malformed DELETE Statements
// -----------------------------------------------------------------------------

#[test]
fn test_delete_missing_from() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("DELETE users WHERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    // Some databases allow DELETE without FROM
    println!("DELETE without FROM: {:?}", diagnostics);
}

#[test]
fn test_delete_with_columns() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("DELETE name FROM users WHERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "DELETE with column names");
}

// -----------------------------------------------------------------------------
// Malformed CREATE TABLE
// -----------------------------------------------------------------------------

#[test]
fn test_create_table_no_columns() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("CREATE TABLE users ()");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "CREATE TABLE with no columns");
}

#[test]
fn test_create_table_missing_type() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("CREATE TABLE users (name, email VARCHAR(255))");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Column missing data type");
}

#[test]
fn test_create_table_unclosed() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("CREATE TABLE users (id INT, name VARCHAR(255)");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "CREATE TABLE unclosed parenthesis");
}

#[test]
fn test_create_table_trailing_comma() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("CREATE TABLE users (id INT, name VARCHAR(255),)");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "CREATE TABLE trailing comma");
}

// -----------------------------------------------------------------------------
// Malformed ALTER TABLE
// -----------------------------------------------------------------------------

#[test]
fn test_alter_table_missing_action() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("ALTER TABLE users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "ALTER TABLE missing action");
}

#[test]
fn test_alter_table_add_missing_column() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("ALTER TABLE users ADD");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "ALTER TABLE ADD missing column");
}

#[test]
fn test_alter_table_drop_missing_column() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("ALTER TABLE users DROP COLUMN");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "ALTER TABLE DROP COLUMN missing name");
}

// -----------------------------------------------------------------------------
// Malformed Functions
// -----------------------------------------------------------------------------

#[test]
fn test_function_missing_arguments() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT COALESCE() FROM users");
    let diagnostics = lsp.validate_sql(&text);
    // COALESCE() with no args is semantically wrong but may parse OK
    // Some databases allow it syntactically
    println!("COALESCE with no arguments: {:?}", diagnostics);
}

#[test]
fn test_aggregate_with_multiple_args() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT COUNT(id, name) FROM users");
    let diagnostics = lsp.validate_sql(&text);
    // COUNT with multiple args is invalid (except COUNT(*))
    println!("COUNT with multiple args: {:?}", diagnostics);
}

#[test]
fn test_nested_aggregate() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT MAX(COUNT(*)) FROM users GROUP BY name");
    let diagnostics = lsp.validate_sql(&text);
    // Nested aggregates are generally not allowed
    println!("Nested aggregate: {:?}", diagnostics);
}

// -----------------------------------------------------------------------------
// Malformed CASE Expressions
// -----------------------------------------------------------------------------

#[test]
fn test_case_without_when() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT CASE THEN 'yes' END FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "CASE without WHEN");
}

#[test]
fn test_case_without_then() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT CASE WHEN id = 1 'yes' END FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "CASE WHEN without THEN");
}

#[test]
fn test_case_without_end() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT CASE WHEN id = 1 THEN 'yes' FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "CASE without END");
}

#[test]
fn test_case_else_without_value() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT CASE WHEN id = 1 THEN 'yes' ELSE END FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "CASE ELSE without value");
}

// -----------------------------------------------------------------------------
// Common Copy-Paste Errors
// -----------------------------------------------------------------------------

#[test]
fn test_duplicate_keyword() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT SELECT * FROM users");
    let diagnostics = lsp.validate_sql(&text);
    // Parser treats second SELECT as identifier/column reference, so no syntax error
    // This documents current behavior - semantic analysis would catch this
    let _ = diagnostics;
}

#[test]
fn test_duplicate_from() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM FROM users");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Duplicate FROM keyword");
}

#[test]
fn test_duplicate_where() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE WHERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Duplicate WHERE keyword");
}

#[test]
fn test_sql_with_programming_syntax() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users; console.log('test');");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "JavaScript mixed with SQL");
}

#[test]
fn test_html_in_sql() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users <br> WHERE id = 1");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "HTML tag in SQL");
}

// -----------------------------------------------------------------------------
// Edge Cases and Boundary Conditions
// -----------------------------------------------------------------------------

#[test]
fn test_empty_query() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("");
    let diagnostics = lsp.validate_sql(&text);
    // Empty query might not be an error, just nothing to parse
    println!("Empty query diagnostics: {:?}", diagnostics);
}

#[test]
fn test_whitespace_only() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("   \n\t   \n   ");
    let diagnostics = lsp.validate_sql(&text);
    println!("Whitespace only diagnostics: {:?}", diagnostics);
}

#[test]
fn test_comment_only() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("-- this is a comment");
    let diagnostics = lsp.validate_sql(&text);
    // Just a comment is valid
    let errors = syntax_errors(&diagnostics);
    assert!(errors.is_empty(), "Comment only should not be syntax error");
}

#[test]
fn test_block_comment_unclosed() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users /* this comment is not closed");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Unclosed block comment");
}

#[test]
fn test_very_long_identifier() {
    let mut lsp = create_test_lsp();
    let long_name = "a".repeat(500);
    let text = Rope::from(format!("SELECT {} FROM users", long_name));
    let diagnostics = lsp.validate_sql(&text);
    // Very long identifiers might be valid syntactically
    println!("Very long identifier diagnostics: {:?}", diagnostics);
}

#[test]
fn test_deeply_nested_subqueries() {
    let mut lsp = create_test_lsp();
    let text =
        Rope::from("SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM users) a) b) c");
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Deeply nested subqueries");
}

#[test]
fn test_many_columns() {
    let mut lsp = create_test_lsp();
    let cols: Vec<String> = (1..=50).map(|i| format!("col{}", i)).collect();
    let text = Rope::from(format!("SELECT {} FROM users", cols.join(", ")));
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Many columns");
}

#[test]
fn test_unicode_in_strings() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE name = '日本語テスト'");
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Unicode in string literal");
}

#[test]
fn test_emoji_in_strings() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE status = '✅ Active 🚀'");
    let diagnostics = lsp.validate_sql(&text);
    assert_no_syntax_errors(&diagnostics, "Emoji in string literal");
}

// -----------------------------------------------------------------------------
// Common Mistakes from Other Languages
// -----------------------------------------------------------------------------

#[test]
fn test_python_string_format() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = {user_id}");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "Python f-string syntax");
}

#[test]
fn test_javascript_template_literal() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = ${userId}");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "JavaScript template literal syntax");
}

#[test]
fn test_csharp_string_interpolation() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE name = @name");
    let diagnostics = lsp.validate_sql(&text);
    // @ is valid in some SQL dialects as parameter prefix
    println!("C# style parameter: {:?}", diagnostics);
}

#[test]
fn test_php_variable() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = $id");
    let diagnostics = lsp.validate_sql(&text);
    assert_has_error(&diagnostics, "PHP variable syntax");
}

#[test]
fn test_ruby_symbol() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = :id");
    let diagnostics = lsp.validate_sql(&text);
    // : is valid in some SQL dialects as parameter prefix
    println!("Ruby/named parameter style: {:?}", diagnostics);
}
