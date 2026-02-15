//! Tests for edge cases and error handling

use super::test_helpers::*;
use zqlz_ui::widgets::Rope;

#[test]
fn test_empty_filter_returns_all() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT ");
    let offset = 7;

    let completions = lsp.get_completions(&text, offset);

    // With empty filter after SELECT, should return columns + keywords
    assert!(
        !completions.is_empty(),
        "Empty filter should return results"
    );
}

#[test]
fn test_offset_at_start() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");
    let offset = 0; // At the very start

    let completions = lsp.get_completions(&text, offset);

    // Should not panic
    println!("Got {} completions at offset 0", completions.len());
}

#[test]
fn test_offset_at_end() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users");
    let offset = 19; // At the very end

    let completions = lsp.get_completions(&text, offset);

    // Should not panic, may return FROM context completions
    println!("Got {} completions at offset {}", completions.len(), offset);
}

#[test]
fn test_query_with_only_whitespace() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("   ");
    let offset = 2;

    let completions = lsp.get_completions(&text, offset);

    // Should return general context keywords
    println!("Got {} completions for whitespace", completions.len());
}

#[test]
fn test_multiline_query() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT *\nFROM users\nWHERE ");
    let offset = text.to_string().len(); // At the end

    let completions = lsp.get_completions(&text, offset);

    // Should handle multiline queries
    assert!(!completions.is_empty(), "Should handle multiline queries");
}

#[test]
fn test_completion_with_tabs() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT\t*\tFROM\t");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should handle tabs as whitespace
    println!("Got {} completions with tabs", completions.len());
}
