//! Tests for go-to-definition functionality

use crate::tests::test_helpers::create_test_lsp;
use lsp_types::GotoDefinitionResponse;
use std::sync::Arc;
use zqlz_services::SchemaService;
use zqlz_ui::widgets::Rope;

#[test]
fn test_definition_for_table_name() {
    let mut lsp = create_test_lsp();

    // Test going to definition for a table name
    let text = Rope::from("SELECT * FROM users WHERE user_id = 1");
    let offset = text.to_string().find("users").unwrap() + 3; // Middle of "users"

    let result = lsp.get_definition(&text, offset);

    assert!(result.is_some(), "Should find definition for table 'users'");
    let response = result.unwrap();
    match response {
        GotoDefinitionResponse::Scalar(location) => {
            assert_eq!(location.uri.to_string(), "sql://internal");
        }
        _ => panic!("Expected scalar definition response"),
    }
}

#[test]
fn test_definition_for_column_name() {
    let mut lsp = create_test_lsp();

    // Test going to definition for a column name
    let text = Rope::from("SELECT username, email FROM users");
    let offset = text.to_string().find("username").unwrap() + 4; // Middle of "username"

    let result = lsp.get_definition(&text, offset);

    assert!(
        result.is_some(),
        "Should find definition for column 'username'"
    );
}

#[test]
fn test_definition_for_qualified_column() {
    let mut lsp = create_test_lsp();

    // Test going to definition for a qualified column (table.column)
    let text = Rope::from("SELECT users.username, audit_log.log_message FROM users JOIN audit_log ON users.user_id = audit_log.log_id");
    let offset = text.to_string().find("users.username").unwrap() + 6; // Middle of "username" in "users.username"

    let result = lsp.get_definition(&text, offset);

    assert!(
        result.is_some(),
        "Should find definition for qualified column 'users.username'"
    );
}

#[test]
fn test_definition_for_unknown_symbol() {
    let mut lsp = create_test_lsp();

    // Test going to definition for an unknown symbol
    let text = Rope::from("SELECT unknown_field FROM users");
    let offset = text.to_string().find("unknown_field").unwrap() + 5; // Middle of "unknown_field"

    let result = lsp.get_definition(&text, offset);

    assert!(
        result.is_none(),
        "Should not find definition for unknown symbol"
    );
}

#[test]
fn test_definition_at_empty_position() {
    let mut lsp = create_test_lsp();

    // Test going to definition at an empty position
    let text = Rope::from("SELECT * FROM users");
    let offset = 0; // At the beginning

    let result = lsp.get_definition(&text, offset);

    // Should return None or empty for empty word at position 0
    assert!(result.is_none() || matches!(result, Some(GotoDefinitionResponse::Scalar(_))));
}

#[test]
fn test_definition_for_keyword() {
    let mut lsp = create_test_lsp();

    // Test going to definition for a SQL keyword (should not find definition)
    let text = Rope::from("SELECT * FROM users WHERE user_id = 1");
    let offset = text.to_string().find("SELECT").unwrap() + 3; // Middle of "SELECT"

    let result = lsp.get_definition(&text, offset);

    // Keywords don't have definitions in schema
    assert!(
        result.is_none(),
        "Should not find definition for SQL keyword"
    );
}

#[test]
fn test_definition_with_no_schema() {
    // Create LSP without schema cache populated
    let schema_service = Arc::new(SchemaService::new());
    let lsp = crate::SqlLsp::new(schema_service);

    // Test going to definition with no schema loaded
    let text = Rope::from("SELECT * FROM users");
    let offset = text.to_string().find("users").unwrap();

    let result = lsp.get_definition(&text, offset);

    // Should return None when no schema is loaded
    assert!(
        result.is_none(),
        "Should not find definition without schema"
    );
}
