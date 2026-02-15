//! Tests for code actions / quick fixes functionality

use crate::tests::test_helpers::create_test_lsp;
use lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};
use zqlz_ui::widgets::Rope;

#[test]
fn test_code_actions_add_semicolon() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");

    // No diagnostics, just context
    let diagnostics: Vec<Diagnostic> = Vec::new();

    let actions = lsp.get_code_actions(&text, 10, &diagnostics);

    // Should have at least the "Add semicolon" action
    assert!(!actions.is_empty(), "Expected at least one code action");

    // Check for semicolon action
    let has_semicolon_action = actions.iter().any(|a| a.title == "Add semicolon");
    assert!(has_semicolon_action, "Expected 'Add semicolon' action");
}

#[test]
fn test_code_actions_for_valid_query() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users;");

    let diagnostics: Vec<Diagnostic> = Vec::new();

    let actions = lsp.get_code_actions(&text, 10, &diagnostics);

    // Query already has semicolon, so no context-based actions expected
    assert!(
        actions.is_empty(),
        "Expected no context actions for valid query"
    );
}

#[test]
fn test_code_actions_with_error_diagnostics() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FR0M users");

    // Create a diagnostic for the typo "FR0M"
    let diagnostics = vec![Diagnostic {
        range: Range {
            start: Position {
                line: 0,
                character: 9,
            },
            end: Position {
                line: 0,
                character: 13,
            },
        },
        severity: Some(DiagnosticSeverity::ERROR),
        message: "Expected keyword FROM but found 'FR0M'".to_string(),
        ..Default::default()
    }];

    let actions = lsp.get_code_actions(&text, 10, &diagnostics);

    // Should have actions for the diagnostic
    assert!(
        !actions.is_empty(),
        "Expected code actions from diagnostics"
    );
}

#[test]
fn test_code_actions_empty_query() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("");

    let diagnostics: Vec<Diagnostic> = Vec::new();

    let actions = lsp.get_code_actions(&text, 0, &diagnostics);

    // Empty query - no context actions expected
    assert!(actions.is_empty(), "Expected no actions for empty query");
}

#[test]
fn test_code_actions_at_query_end() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");

    let diagnostics: Vec<Diagnostic> = Vec::new();

    // Position at the end
    let actions = lsp.get_code_actions(&text, text.len(), &diagnostics);

    // Should still offer semicolon action
    let has_semicolon = actions.iter().any(|a| a.title.contains("semicolon"));
    assert!(has_semicolon, "Expected semicolon action at end of text");
}
