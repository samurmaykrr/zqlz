//! Tests for completion suggestions and filtering

use super::test_helpers::*;
use zqlz_ui::widgets::Rope;

#[test]
fn test_keyword_completion_single_char() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("s");
    let offset = 1;

    let completions = lsp.get_completions(&text, offset);

    println!(
        "Single char 's' returned {} completions:",
        completions.len()
    );
    for c in &completions {
        println!("  - {}", c.label);
    }

    assert!(!completions.is_empty(), "Should return completions for 's'");

    let has_select = has_completion(&completions, "SELECT");
    assert!(
        has_select,
        "Should suggest SELECT for 's'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_keyword_completion_multi_char() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("sel");
    let offset = 3;

    let completions = lsp.get_completions(&text, offset);

    assert!(
        !completions.is_empty(),
        "Should return completions for 'sel'"
    );

    let has_select = has_completion(&completions, "SELECT");
    assert!(
        has_select,
        "Should suggest SELECT for 'sel'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_completion_case_insensitive() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT US");
    let offset = 9;

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should match "username" even with uppercase "US"
    let has_username = labels.iter().any(|l| l == "username");
    let has_user_id = labels.iter().any(|l| l == "user_id");

    assert!(
        has_username || has_user_id,
        "Should match columns case-insensitively. Got: {:?}",
        labels
    );
}

#[test]
fn test_completion_prefix_matching() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT user");
    let offset = 11;

    let completions = lsp.get_completions(&text, offset);

    let labels: Vec<String> = completions.iter().map(|c| c.label.clone()).collect();

    // Should only match columns starting with "user"
    let has_user_id = labels.contains(&"user_id".to_string());
    let has_username = labels.contains(&"username".to_string());

    assert!(
        has_user_id && has_username,
        "Should match user_id and username. Got: {:?}",
        labels
    );

    // Should NOT match columns from other tables
    assert!(
        !labels.iter().any(|l| l.starts_with("log_")),
        "Should NOT match log columns"
    );
}

#[test]
fn test_no_completions_for_invalid_context() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("INVALID SQL QUERY !!!");
    let offset = 10;

    let completions = lsp.get_completions(&text, offset);

    // May return empty or general context - either is acceptable
    // The key is it shouldn't panic
    println!("Got {} completions for invalid SQL", completions.len());
}
