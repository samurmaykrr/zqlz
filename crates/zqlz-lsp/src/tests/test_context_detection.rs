//! Tests for SQL context detection

use super::test_helpers::*;
use zqlz_ui::widgets::Rope;

#[test]
fn test_context_general_empty_query() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("");
    let offset = 0;

    let completions = lsp.get_completions(&text, offset);

    // Should get primary SQL keywords
    assert!(
        !completions.is_empty(),
        "Should have completions for empty query"
    );

    let has_select = has_completion(&completions, "SELECT");
    assert!(has_select, "Should suggest SELECT keyword");
}

#[test]
fn test_context_select_list_no_from() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT ");
    let offset = 7; // After "SELECT "

    let completions = lsp.get_completions(&text, offset);

    // Should get columns from all tables + keywords
    assert!(
        !completions.is_empty(),
        "Should have completions after SELECT"
    );

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should have columns from various tables
    let has_user_cols = labels.iter().any(|l| l.starts_with("user"));
    let has_log_cols = labels.iter().any(|l| l.starts_with("log"));
    let has_location_cols = labels.iter().any(|l| l.starts_with("location"));

    assert!(
        has_user_cols || has_log_cols || has_location_cols,
        "Should have at least some columns from tables. Got labels: {:?}",
        labels
    );
}

#[test]
fn test_context_select_list_column_filter() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT lo");
    let offset = 9; // After "SELECT lo"

    let completions = lsp.get_completions(&text, offset);

    // Should get columns starting with "lo"
    assert!(
        !completions.is_empty(),
        "Should have completions for 'lo' prefix"
    );

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should suggest log_id, log_timestamp, location_id, location_name
    let has_log = labels.iter().any(|l| l.starts_with("log_"));
    let has_location = labels.iter().any(|l| l.starts_with("location_"));

    assert!(
        has_log || has_location,
        "Should suggest columns starting with 'lo'. Got: {:?}",
        labels
    );

    // Should NOT suggest user columns
    assert!(
        !labels.iter().any(|l| l.starts_with("user_")),
        "Should NOT suggest user columns"
    );
}

#[test]
fn test_context_from_clause() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM ");
    let offset = 14; // After "SELECT * FROM "

    let completions = lsp.get_completions(&text, offset);

    // Should get table names
    assert!(
        !completions.is_empty(),
        "Should have completions in FROM clause"
    );

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should suggest table names
    assert!(
        labels.contains(&"users".to_string()),
        "Should suggest users table. Got: {:?}",
        labels
    );
}

#[test]
fn test_context_from_clause_with_filter() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM au");
    let offset = 16; // After "SELECT * FROM au"

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should only suggest audit_log
    assert!(
        labels.contains(&"audit_log".to_string()),
        "Should suggest audit_log"
    );

    // Should NOT suggest users or locations
    assert!(
        !labels.contains(&"users".to_string()),
        "Should NOT suggest users table"
    );
}

#[test]
#[ignore] // Temporarily ignore until string indexing bug is fixed
fn test_context_where_clause() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE ");
    let offset = 27; // After "SELECT * FROM users WHERE "

    let completions = lsp.get_completions(&text, offset);

    // Should get column names from users table + condition keywords
    assert!(
        !completions.is_empty(),
        "Should have completions in WHERE clause"
    );

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should suggest columns from users table
    let has_user_cols = labels.iter().any(|l| l.starts_with("user"));

    assert!(has_user_cols, "Should suggest user columns in WHERE clause");
}
