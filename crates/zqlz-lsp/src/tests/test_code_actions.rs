//! Tests for code actions / quick fixes functionality

use crate::tests::test_helpers::create_test_lsp;
use lsp_types::{Diagnostic, DiagnosticSeverity, NumberOrString, Position, Range};
use zqlz_ui::widgets::Rope;

#[test]
fn test_code_actions_add_semicolon() {
    let lsp = create_test_lsp();
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
    let lsp = create_test_lsp();
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
fn test_external_diagnostic_prose_does_not_create_guess_actions() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FR0M users;");

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

    assert!(
        actions.is_empty(),
        "External diagnostic prose should not create guessed quick fixes. Got: {actions:?}"
    );
}

#[test]
fn test_code_actions_use_syntax_diagnostic_code_for_semicolon_fix() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");

    let diagnostics = vec![Diagnostic {
        range: Range {
            start: Position {
                line: 0,
                character: 19,
            },
            end: Position {
                line: 0,
                character: 19,
            },
        },
        severity: Some(DiagnosticSeverity::ERROR),
        code: Some(NumberOrString::String(
            crate::diagnostics::DIAGNOSTIC_CODE_SQLPARSER_SYNTAX.to_string(),
        )),
        message: "Parser wanted statement terminator ;".to_string(),
        ..Default::default()
    }];

    let actions = lsp.get_code_actions(&text, text.len(), &diagnostics);

    assert!(
        actions
            .iter()
            .any(|action| action.title == "Add missing semicolon"),
        "semicolon fix should use diagnostic code, not exact parser prose"
    );
}

#[test]
fn test_code_actions_close_unclosed_string_from_tokenizer_range() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE name = 'john");

    let diagnostics = vec![Diagnostic {
        range: Range {
            start: Position {
                line: 0,
                character: 33,
            },
            end: Position {
                line: 0,
                character: 34,
            },
        },
        severity: Some(DiagnosticSeverity::ERROR),
        code: Some(NumberOrString::String(
            crate::diagnostics::DIAGNOSTIC_CODE_SQL_TOKENIZER.to_string(),
        )),
        message: "SQL Tokenizer Error: token text intentionally irrelevant".to_string(),
        ..Default::default()
    }];

    let actions = lsp.get_code_actions(&text, 33, &diagnostics);
    let edit = actions
        .iter()
        .find(|action| action.title == "Close string literal")
        .and_then(|action| action.edit.as_ref())
        .and_then(|edit| edit.changes.as_ref())
        .and_then(|changes| changes.values().next())
        .and_then(|edits| edits.first())
        .expect("close string edit");

    assert_eq!(edit.range.start.character, 38);
    assert_eq!(edit.new_text, "'");
}

#[test]
fn test_internal_syntax_diagnostic_does_not_offer_identifier_quote_guess() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FR0M users");

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
        code: Some(NumberOrString::String(
            crate::diagnostics::DIAGNOSTIC_CODE_SQLPARSER_SYNTAX.to_string(),
        )),
        message: "Syntax error".to_string(),
        ..Default::default()
    }];

    let actions = lsp.get_code_actions(&text, 10, &diagnostics);

    assert!(
        actions
            .iter()
            .all(|action| !action.title.starts_with("Quote identifier")),
        "internal syntax diagnostics should not guess identifier quoting from parser errors"
    );
}

#[test]
fn test_external_reserved_keyword_diagnostic_does_not_offer_identifier_quote_guess() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT order FROM users");

    let diagnostics = vec![Diagnostic {
        range: Range {
            start: Position {
                line: 0,
                character: 7,
            },
            end: Position {
                line: 0,
                character: 12,
            },
        },
        severity: Some(DiagnosticSeverity::ERROR),
        message: "reserved keyword used as identifier".to_string(),
        ..Default::default()
    }];

    let actions = lsp.get_code_actions(&text, 8, &diagnostics);

    assert!(
        actions
            .iter()
            .all(|action| !action.title.starts_with("Quote identifier")),
        "external prose diagnostics should not guess identifier quoting"
    );
}

#[test]
fn test_code_actions_empty_query() {
    let lsp = create_test_lsp();
    let text = Rope::from("");

    let diagnostics: Vec<Diagnostic> = Vec::new();

    let actions = lsp.get_code_actions(&text, 0, &diagnostics);

    // Empty query - no context actions expected
    assert!(actions.is_empty(), "Expected no actions for empty query");
}

#[test]
fn test_code_actions_at_query_end() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");

    let diagnostics: Vec<Diagnostic> = Vec::new();

    // Position at the end
    let actions = lsp.get_code_actions(&text, text.len(), &diagnostics);

    // Should still offer semicolon action
    let has_semicolon = actions.iter().any(|a| a.title.contains("semicolon"));
    assert!(has_semicolon, "Expected semicolon action at end of text");
}

#[test]
fn test_add_semicolon_action_inserts_before_trailing_line_comment() {
    let lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users -- trailing comment");
    let diagnostics: Vec<Diagnostic> = Vec::new();

    let actions = lsp.get_code_actions(&text, 10, &diagnostics);
    let action = actions
        .iter()
        .find(|action| action.title == "Add semicolon")
        .expect("semicolon action");
    let edit = action
        .edit
        .as_ref()
        .and_then(|edit| edit.changes.as_ref())
        .and_then(|changes| changes.values().next())
        .and_then(|edits| edits.first())
        .expect("semicolon edit");

    assert_eq!(edit.range.start.line, 0);
    assert_eq!(
        edit.range.start.character, 19,
        "semicolon should be inserted before trailing line comment"
    );
}
