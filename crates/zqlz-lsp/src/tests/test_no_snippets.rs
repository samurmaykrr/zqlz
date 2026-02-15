//! Test to verify snippets are NOT included in completions

use super::test_helpers::*;
use lsp_types::CompletionItemKind;
use zqlz_ui::widgets::Rope;

#[test]
fn test_no_snippets_in_select_completions() {
    let mut lsp = create_test_lsp();

    // Type "SELECT "
    let text = Rope::from("SELECT ");
    let offset = 7;

    let completions = lsp.get_completions(&text, offset);

    // Check that NO snippet completions are included
    let has_snippets = completions.iter().any(|c| {
        c.kind == Some(CompletionItemKind::SNIPPET)
            || c.insert_text_format == Some(lsp_types::InsertTextFormat::SNIPPET)
    });

    assert!(
        !has_snippets,
        "Should NOT include snippets in completions. Found snippet-type completions: {:?}",
        completions
            .iter()
            .filter(|c| c.kind == Some(CompletionItemKind::SNIPPET)
                || c.insert_text_format == Some(lsp_types::InsertTextFormat::SNIPPET))
            .map(|c| &c.label)
            .collect::<Vec<_>>()
    );

    println!("✓ No snippets found in completions (verified)");
}

#[test]
fn test_no_snippets_in_general_completions() {
    let mut lsp = create_test_lsp();

    // Type just "s" to get general completions
    let text = Rope::from("s");
    let offset = 1;

    let completions = lsp.get_completions(&text, offset);

    // Should have completions (like SELECT keyword)
    assert!(!completions.is_empty(), "Should return some completions");

    // But NO snippets
    let snippet_items: Vec<_> = completions
        .iter()
        .filter(|c| {
            c.kind == Some(CompletionItemKind::SNIPPET)
                || c.insert_text_format == Some(lsp_types::InsertTextFormat::SNIPPET)
        })
        .collect();

    assert!(
        snippet_items.is_empty(),
        "Should NOT include snippets. Found: {:?}",
        snippet_items.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_no_snippet_format_in_any_completion() {
    let mut lsp = create_test_lsp();

    // Test multiple contexts
    let test_cases = vec![
        ("SELECT ", 7, "SELECT clause"),
        ("SELECT * FROM ", 14, "FROM clause"),
        ("SELECT * FROM users WHERE ", 26, "WHERE clause"),
        ("INSERT ", 7, "INSERT statement"),
    ];

    for (query, offset, context) in test_cases {
        let text = Rope::from(query);
        let completions = lsp.get_completions(&text, offset);

        let snippet_items: Vec<_> = completions
            .iter()
            .filter(|c| c.insert_text_format == Some(lsp_types::InsertTextFormat::SNIPPET))
            .collect();

        assert!(
            snippet_items.is_empty(),
            "{}: Should NOT have snippet format. Found: {:?}",
            context,
            snippet_items.iter().map(|c| &c.label).collect::<Vec<_>>()
        );
    }

    println!("✓ Verified no snippet formats in any completion context");
}
