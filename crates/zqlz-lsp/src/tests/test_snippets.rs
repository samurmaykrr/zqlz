//! Tests for SQL snippet completions
//!
//! Tests the SQL LSP's snippet templates for common SQL patterns,
//! including SELECT, INSERT, UPDATE, DELETE, CREATE, JOIN, CTE, and more.

use super::test_helpers::*;
use lsp_types::InsertTextFormat;
use zqlz_ui::widgets::Rope;

#[test]
fn test_select_snippet_basic() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("sel");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest SELECT snippets
    let select_snippets: Vec<_> = completions
        .iter()
        .filter(|c| c.label.to_lowercase().contains("select"))
        .collect();

    assert!(
        !select_snippets.is_empty(),
        "Should suggest SELECT snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_snippet_has_correct_format() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("select");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Find a snippet completion
    let snippet = completions
        .iter()
        .find(|c| c.insert_text_format == Some(InsertTextFormat::SNIPPET));

    if let Some(snippet) = snippet {
        assert!(
            snippet.insert_text.is_some(),
            "Snippet should have insert_text"
        );
        assert!(
            snippet.insert_text.as_ref().unwrap().contains("${"),
            "Snippet should contain placeholder syntax"
        );
    }
}

#[test]
fn test_insert_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("ins");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest INSERT snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("insert")),
        "Should suggest INSERT snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_update_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("upd");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest UPDATE snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("update")),
        "Should suggest UPDATE snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_delete_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("del");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest DELETE snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("delete")),
        "Should suggest DELETE snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_join_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("join");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest JOIN-related snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("join")),
        "Should suggest JOIN snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_cte_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("with");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest WITH/CTE snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("with")
                || c.label.to_lowercase().contains("cte")),
        "Should suggest WITH/CTE snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_create_table_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("create");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest CREATE TABLE snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("create")),
        "Should suggest CREATE snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_group_by_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("group");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest GROUP BY related snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("group")),
        "Should suggest GROUP BY snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_order_by_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("order");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest ORDER BY related snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("order")),
        "Should suggest ORDER BY snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_case_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("case");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest CASE expression snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("case")),
        "Should suggest CASE snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_union_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("union");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest UNION snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("union")),
        "Should suggest UNION snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_transaction_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("trans");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest transaction snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("transaction")),
        "Should suggest transaction snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_exists_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("exists");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest EXISTS subquery snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("exists")),
        "Should suggest EXISTS snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_subquery_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("subq");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest subquery snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("subquery")),
        "Should suggest subquery snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_create_index_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("index");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest index creation snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("index")),
        "Should suggest index snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_left_join_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("left");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest LEFT JOIN snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("left")),
        "Should suggest LEFT JOIN snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_distinct_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("distinct");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest DISTINCT snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("distinct")),
        "Should suggest DISTINCT snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_limit_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("limit");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest LIMIT snippets
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("limit")),
        "Should suggest LIMIT snippets. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_snippet_contains_description() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("select");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Find snippets and verify they have descriptions
    let snippets: Vec<_> = completions
        .iter()
        .filter(|c| c.insert_text_format == Some(InsertTextFormat::SNIPPET))
        .collect();

    for snippet in snippets {
        assert!(
            snippet.detail.is_some() || snippet.documentation.is_some(),
            "Snippet '{}' should have description or documentation",
            snippet.label
        );
    }
}

#[test]
fn test_insert_multiple_rows_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("insert-mult");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest multiple row INSERT snippet
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_lowercase().contains("multiple")
                && c.label.to_lowercase().contains("insert")),
        "Should suggest INSERT multiple rows snippet. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_create_table_with_fk_snippet() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("create-table-fk");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should suggest CREATE TABLE with foreign key snippet
    assert!(
        completions.iter().any(|c| {
            let label = c.label.to_lowercase();
            (label.contains("create") || label.contains("table"))
                && (label.contains("fk") || label.contains("foreign"))
        }),
        "Should suggest CREATE TABLE with FK snippet. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_snippet_keyword_matching() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("aggregate");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should match snippets by keywords, not just label
    // "aggregate" keyword should match group-by snippet
    assert!(!completions.is_empty(), "Should match snippets by keywords");
}

#[test]
fn test_snippet_priority_in_completions() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("select");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Snippets should have sort_text starting with "z_" for lower priority
    let snippets: Vec<_> = completions
        .iter()
        .filter(|c| c.insert_text_format == Some(InsertTextFormat::SNIPPET))
        .collect();

    for snippet in snippets {
        if let Some(sort_text) = &snippet.sort_text {
            assert!(
                sort_text.starts_with("z_"),
                "Snippet '{}' should have lower priority (sort_text should start with 'z_'). Got: {:?}",
                snippet.label,
                sort_text
            );
        }
    }
}
