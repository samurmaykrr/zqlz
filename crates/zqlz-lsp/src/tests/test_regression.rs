//! Regression tests for previously fixed bugs

use super::test_helpers::*;
use zqlz_ui::widgets::Rope;

/// Regression test: "SELECT lo" should show COLUMNS (log_*, location_*) not the "locations" TABLE
/// Bug: Context analyzer was detecting this as General context instead of SelectList
#[test]
fn test_regression_select_lo_shows_columns_not_tables() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT lo");
    let offset = 9;

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should show columns starting with "lo"
    let has_log_cols = labels.iter().any(|l| l.starts_with("log_"));
    let has_location_cols = labels.iter().any(|l| l.starts_with("location_"));

    assert!(
        has_log_cols || has_location_cols,
        "Should show columns starting with 'lo'. Got: {:?}",
        labels
    );

    // Should NOT show the "locations" table itself
    assert!(
        !labels.contains(&"locations".to_string()),
        "Should NOT show 'locations' table in SELECT list context"
    );
}

/// Regression test: General context (empty query) should return keywords like SELECT, INSERT, etc.
#[test]
fn test_regression_general_context_returns_keywords() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("");
    let offset = 0;

    let completions = lsp.get_completions(&text, offset);

    assert!(
        !completions.is_empty(),
        "General context should return keyword completions"
    );

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should have SQL keywords
    let has_keywords = labels.iter().any(|l| {
        matches!(
            l.as_str(),
            "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "CREATE" | "DROP"
        )
    });

    assert!(
        has_keywords,
        "Should suggest SQL keywords in general context. Got: {:?}",
        labels
    );
}

/// Regression test: Cmd+Shift+F format action should work
/// This is tested via keybinding registration, not directly here
#[test]
#[ignore] // This requires UI context to test properly
fn test_regression_format_keybinding_works() {
    // This bug was fixed by wrapping Input widget in div with key_context("QueryEditor")
    // Testing would require full GPUI context
}
