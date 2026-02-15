//! Tests for Common Table Expression (CTE) completions
//!
//! Tests the SQL LSP's ability to provide completions for WITH clauses,
//! CTE references, and nested CTEs.

use super::test_helpers::*;
use zqlz_ui::widgets::Rope;

#[test]
fn test_cte_basic_completion() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("WITH user_stats AS (SELECT * FROM users) SELECT * FROM ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest the CTE name along with regular tables
    assert!(
        completions.iter().any(|c| c.label == "user_stats"),
        "Should suggest CTE name 'user_stats'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
    assert!(
        completions.iter().any(|c| c.label == "users"),
        "Should also suggest regular tables. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_cte_column_references() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "WITH active_users AS (SELECT user_id, username FROM users WHERE user_id > 0) SELECT  FROM active_users",
    );
    let offset = text.to_string().find("SELECT ").unwrap() + 7; // After "SELECT "

    let completions = lsp.get_completions(&text, offset);

    // Should suggest columns from the CTE
    assert!(
        completions
            .iter()
            .any(|c| c.label == "user_id" || c.label == "username"),
        "Should suggest columns from CTE. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_multiple_ctes() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "WITH user_stats AS (SELECT * FROM users), log_stats AS (SELECT * FROM audit_log) SELECT * FROM ",
    );
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest both CTE names
    assert!(
        completions.iter().any(|c| c.label == "user_stats"),
        "Should suggest first CTE 'user_stats'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
    assert!(
        completions.iter().any(|c| c.label == "log_stats"),
        "Should suggest second CTE 'log_stats'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_nested_cte() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "WITH outer_cte AS (WITH inner_cte AS (SELECT * FROM users) SELECT * FROM inner_cte) SELECT * FROM ",
    );
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest the outer CTE
    assert!(
        completions.iter().any(|c| c.label == "outer_cte"),
        "Should suggest outer CTE. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_cte_in_join() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "WITH user_stats AS (SELECT user_id, COUNT(*) as count FROM audit_log GROUP BY user_id) SELECT * FROM users u JOIN ",
    );
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest CTE in JOIN context
    assert!(
        completions.iter().any(|c| c.label == "user_stats"),
        "Should suggest CTE in JOIN context. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_cte_with_alias() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("WITH stats AS (SELECT user_id FROM users) SELECT s. FROM stats s");
    let offset = text.to_string().find("s.").unwrap() + 2;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest columns when using CTE alias
    assert!(
        completions.iter().any(|c| c.label == "user_id"),
        "Should suggest columns from CTE using alias. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_cte_snippet_completion() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("wit");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest WITH/CTE snippet
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("with")
                || c.label.to_lowercase().contains("cte")),
        "Should suggest WITH/CTE snippet. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_recursive_cte_keyword() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("WITH RECUR");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest RECURSIVE keyword
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "RECURSIVE"),
        "Should suggest RECURSIVE keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_cte_with_union() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "WITH combined AS (SELECT user_id FROM users UNION SELECT user_id FROM audit_log) SELECT * FROM ",
    );
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest the CTE name
    assert!(
        completions.iter().any(|c| c.label == "combined"),
        "Should suggest CTE with UNION. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_cte_context_detection() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("WITH user_data AS (SELECT )");
    let offset = text.to_string().find("(SELECT ").unwrap() + 8;

    let completions = lsp.get_completions(&text, offset);

    // Should provide column/table completions inside CTE
    assert!(
        !completions.is_empty(),
        "Should provide completions inside CTE definition"
    );
}

#[test]
fn test_multiple_ctes_with_dependencies() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "WITH user_counts AS (SELECT user_id, COUNT(*) as count FROM audit_log GROUP BY user_id), \
         high_activity AS (SELECT * FROM user_counts WHERE count > 10) \
         SELECT * FROM ",
    );
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest both CTEs
    assert!(
        completions.iter().any(|c| c.label == "user_counts"),
        "Should suggest first CTE. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
    assert!(
        completions.iter().any(|c| c.label == "high_activity"),
        "Should suggest dependent CTE. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_cte_in_subquery() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "WITH stats AS (SELECT * FROM users) SELECT * FROM audit_log WHERE user_id IN (SELECT user_id FROM )",
    );
    let offset = text.to_string().len() - 1; // Before closing paren

    let completions = lsp.get_completions(&text, offset);

    // Should suggest CTE even inside subquery
    assert!(
        completions.iter().any(|c| c.label == "stats"),
        "Should suggest CTE in subquery. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_cte_with_insert() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "WITH new_users AS (SELECT * FROM users WHERE user_id > 100) INSERT INTO audit_log SELECT user_id FROM ",
    );
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest CTE in INSERT SELECT context
    assert!(
        completions.iter().any(|c| c.label == "new_users"),
        "Should suggest CTE with INSERT SELECT. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_cte_with_update() {
    let mut lsp = create_test_lsp();
    let text = Rope::from(
        "WITH active_ids AS (SELECT user_id FROM users WHERE username LIKE '%active%') UPDATE audit_log SET action = 'verified' WHERE user_id IN (SELECT user_id FROM )",
    );
    let offset = text.to_string().len() - 1;

    let completions = lsp.get_completions(&text, offset);

    // Should suggest CTE in UPDATE context
    assert!(
        completions.iter().any(|c| c.label == "active_ids"),
        "Should suggest CTE with UPDATE. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_cte_hover() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("WITH user_stats AS (SELECT * FROM users) SELECT * FROM user_stats");
    let offset = text.to_string().rfind("user_stats").unwrap() + 5; // Middle of "user_stats"

    let hover = lsp.get_hover(&text, offset);

    // Should provide hover info for CTE reference
    assert!(
        hover.is_some(),
        "Should provide hover info for CTE reference"
    );
}
