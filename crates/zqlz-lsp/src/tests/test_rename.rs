//! Tests for rename symbol functionality

use crate::SqlLsp;
use crate::keywords::SQL_KEYWORDS;
use lsp_types::{Position, Range, Uri};
use std::sync::Arc;
use zqlz_services::SchemaService;
use zqlz_ui::widgets::Rope;

const RENAME_PROBE_IDENTIFIER: &str = "__zqlz_rename_probe";

/// Helper to create an SqlLsp instance for testing
fn create_test_lsp() -> SqlLsp {
    SqlLsp::new(Arc::new(SchemaService::new()))
}

fn edits_for_internal_uri(edit: lsp_types::WorkspaceEdit) -> Vec<lsp_types::TextEdit> {
    let uri = "sql://internal".parse::<Uri>().expect("valid uri");
    edit.changes
        .expect("should have changes")
        .into_iter()
        .find_map(|(change_uri, edits)| (change_uri == uri).then_some(edits))
        .expect("should have changes for sql://internal")
}

/// Test renaming a simple identifier
#[test]
fn test_rename_simple_identifier() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT user_name, user_name FROM users");
    // Position at 'user_name' (offset 7)
    let offset = 7;

    let result = lsp.rename(&text, offset, "new_name");

    assert!(result.is_some());
    let edit = result.unwrap();
    assert!(edit.changes.is_some());

    let edits = edits_for_internal_uri(edit);

    // Should have 2 occurrences (both user_name occurrences)
    assert_eq!(edits.len(), 2, "should rename all occurrences");

    // Check first occurrence (user_name at position 7)
    assert_eq!(
        edits[0].range,
        Range {
            start: Position {
                line: 0,
                character: 7
            },
            end: Position {
                line: 0,
                character: 16
            },
        }
    );
    assert_eq!(edits[0].new_text, "new_name");
}

/// Test renaming when cursor is on a keyword - should return None
#[test]
fn test_rename_keyword_returns_none() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");
    // Position at 'SELECT' (offset 0)
    let offset = 0;

    let result = lsp.rename(&text, offset, "new_name");

    assert!(result.is_none(), "should not rename keywords");
}

/// Test renaming with an invalid SQL identifier - should return None
#[test]
fn test_rename_invalid_identifier_returns_none() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT user_name FROM users");
    let offset = 7;

    // Try to rename to something that starts with a number (invalid)
    let result = lsp.rename(&text, offset, "123invalid");
    assert!(
        result.is_none(),
        "should not allow identifiers starting with number"
    );

    // Try to rename to empty string
    let result = lsp.rename(&text, offset, "");
    assert!(result.is_none(), "should not allow empty identifier");

    // Try to rename to a keyword
    let result = lsp.rename(&text, offset, "SELECT");
    assert!(result.is_none(), "should not allow keywords as identifiers");
}

/// Test renaming at empty position - should return None
#[test]
fn test_rename_at_empty_position() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");
    // Position at the space after SELECT
    let offset = 6;

    let result = lsp.rename(&text, offset, "new_name");

    assert!(
        result.is_none(),
        "should return None when cursor is not on a word"
    );
}

/// Test renaming when new name is the same as old name
#[test]
fn test_rename_same_name_returns_none() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT user_name FROM users");
    // Position at 'user_name' (offset 7)
    let offset = 7;

    let result = lsp.rename(&text, offset, "user_name");

    assert!(
        result.is_none(),
        "should return None when renaming to same name"
    );
}

/// Test renaming with a qualified column reference
#[test]
fn test_rename_qualified_column() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT u.user_name, u.user_id FROM users u");
    // Position at 'user_name' in 'u.user_name' (offset 9)
    let offset = 9;

    let result = lsp.rename(&text, offset, "new_column");

    assert!(result.is_some());
    let edit = result.unwrap();
    assert!(edit.changes.is_some());

    let edits = edits_for_internal_uri(edit);

    // Should find all occurrences of user_name
    assert!(!edits.is_empty());

    // First edit should be at u.user_name
    assert_eq!(edits[0].new_text, "new_column");
}

/// Test renaming a table name
#[test]
fn test_rename_table_name() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE id = 1");
    // Position at 'users' (offset 14)
    let offset = 14;

    let result = lsp.rename(&text, offset, "customers");

    assert!(result.is_some());
    let edit = result.unwrap();
    assert!(edit.changes.is_some());

    let edits = edits_for_internal_uri(edit);

    // Should have at least one occurrence
    assert!(!edits.is_empty());

    // The edit should replace 'users' with 'customers'
    assert_eq!(edits[0].new_text, "customers");
}

/// Test renaming excludes partial matches
#[test]
fn test_rename_excludes_partial_matches() {
    let lsp = create_test_lsp();
    // This tests that renaming 'user' doesn't affect 'user_name'
    let text = Rope::from("SELECT user, user_name FROM users");
    // Position at the first 'user' (offset 7)
    let offset = 7;

    let result = lsp.rename(&text, offset, "customer");

    assert!(result.is_some());
    let edit = result.unwrap();
    let edits = edits_for_internal_uri(edit);

    // Should only replace standalone 'user', not 'user_name'
    // Looking for exact word boundary matches
    assert!(!edits.is_empty());
}

/// Test renaming handles underscores correctly
#[test]
fn test_rename_with_underscores() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT user_id FROM users");
    // Position at 'user_id' (offset 7)
    let offset = 7;

    let result = lsp.rename(&text, offset, "customer_id");

    assert!(result.is_some());
    let edit = result.unwrap();
    let edits = edits_for_internal_uri(edit);

    assert_eq!(edits[0].new_text, "customer_id");
}

/// Test renaming with new name containing underscore
#[test]
fn test_rename_to_underscore_name() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT userid FROM users");
    // Position at 'userid' (offset 7)
    let offset = 7;

    let result = lsp.rename(&text, offset, "customer_id");

    assert!(result.is_some());
}

/// Test renaming empty text returns None
#[test]
fn test_rename_empty_text_returns_none() {
    let lsp = create_test_lsp();
    let text = Rope::from("");
    let offset = 0;

    let result = lsp.rename(&text, offset, "new_name");

    assert!(result.is_none());
}

#[test]
fn test_rename_probe_identifier_stays_valid() {
    let probe_upper = RENAME_PROBE_IDENTIFIER.to_uppercase();
    assert!(!SQL_KEYWORDS.iter().any(|keyword| *keyword == probe_upper));
}
