//! Test for WHERE clause completion priority
//! Verify that columns appear before scalar functions, and aggregate functions are excluded

use super::test_helpers::*;
use lsp_types::CompletionItemKind;
use zqlz_ui::widgets::Rope;

#[test]
fn test_where_clause_shows_columns_first() {
    let mut lsp = create_test_lsp();

    // Type "SELECT * FROM users WHERE " (with trailing space)
    let text = Rope::from("SELECT * FROM users WHERE ");
    let offset = 26;

    let completions = lsp.get_completions(&text, offset);

    println!("Got {} completions:", completions.len());
    for (i, c) in completions.iter().enumerate() {
        println!(
            "  {}: {} ({:?}) sort_text={:?}",
            i, c.label, c.kind, c.sort_text
        );
    }

    // Find columns
    let columns: Vec<_> = completions
        .iter()
        .filter(|c| c.kind == Some(CompletionItemKind::FIELD))
        .collect();

    // Find functions
    let functions: Vec<_> = completions
        .iter()
        .filter(|c| c.kind == Some(CompletionItemKind::FUNCTION))
        .collect();

    // Find keywords
    let keywords: Vec<_> = completions
        .iter()
        .filter(|c| c.kind == Some(CompletionItemKind::KEYWORD))
        .collect();

    println!("\nColumns: {} (should be >0)", columns.len());
    println!(
        "Functions: {} (should include scalar functions only)",
        functions.len()
    );
    println!("Keywords: {} (AND, OR, NOT, etc.)", keywords.len());

    // Assert columns are suggested
    assert!(
        !columns.is_empty(),
        "Should suggest columns in WHERE clause"
    );

    // Check that user_id and username columns are suggested
    assert!(
        columns.iter().any(|c| c.label == "user_id"),
        "Should suggest user_id column"
    );
    assert!(
        columns.iter().any(|c| c.label == "username"),
        "Should suggest username column"
    );

    // Check priority: columns should come before functions
    // This is determined by sort_text (1_ for columns, 3_ for functions)
    if !functions.is_empty() {
        let first_column_sort = columns.first().unwrap().sort_text.as_ref().unwrap();
        let first_function_sort = functions.first().unwrap().sort_text.as_ref().unwrap();
        assert!(
            first_column_sort < first_function_sort,
            "Columns (sort: {}) should be prioritized over functions (sort: {})",
            first_column_sort,
            first_function_sort
        );
    }

    // Check that aggregate functions like COUNT, SUM, AVG are NOT suggested
    let has_count = functions
        .iter()
        .any(|c| c.label.to_uppercase().contains("COUNT"));
    let has_sum = functions
        .iter()
        .any(|c| c.label.to_uppercase().contains("SUM"));
    let has_avg = functions
        .iter()
        .any(|c| c.label.to_uppercase().contains("AVG"));

    assert!(
        !has_count,
        "Should NOT suggest COUNT in WHERE clause (aggregate function)"
    );
    assert!(
        !has_sum,
        "Should NOT suggest SUM in WHERE clause (aggregate function)"
    );
    assert!(
        !has_avg,
        "Should NOT suggest AVG in WHERE clause (aggregate function)"
    );

    // Check that scalar functions like UPPER, LOWER, TRIM are suggested
    let has_scalar = functions.iter().any(|c| {
        let label_upper = c.label.to_uppercase();
        label_upper.contains("UPPER")
            || label_upper.contains("LOWER")
            || label_upper.contains("TRIM")
    });

    if !functions.is_empty() {
        assert!(
            has_scalar,
            "Should suggest scalar functions (UPPER, LOWER, TRIM) in WHERE clause. Got: {:?}",
            functions.iter().map(|f| &f.label).collect::<Vec<_>>()
        );
    }
}

#[test]
fn test_where_clause_typing_column_name() {
    let mut lsp = create_test_lsp();

    // Type "SELECT * FROM users WHERE user" (typing column name)
    let text = Rope::from("SELECT * FROM users WHERE user");
    let offset = 30;

    let completions = lsp.get_completions(&text, offset);

    println!("Got {} completions when typing 'user':", completions.len());
    for c in &completions {
        println!("  {} ({:?})", c.label, c.kind);
    }

    // Should suggest user_id and username columns
    let has_user_id = completions.iter().any(|c| c.label == "user_id");
    let has_username = completions.iter().any(|c| c.label == "username");

    assert!(
        has_user_id || has_username,
        "Should suggest user_id or username when typing 'user'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_select_clause_shows_aggregate_functions() {
    let mut lsp = create_test_lsp();

    // Type "SELECT " (in SELECT clause, aggregates should be available)
    let text = Rope::from("SELECT ");
    let offset = 7;

    let completions = lsp.get_completions(&text, offset);

    let functions: Vec<_> = completions
        .iter()
        .filter(|c| c.kind == Some(CompletionItemKind::FUNCTION))
        .collect();

    // In SELECT clause, aggregate functions SHOULD be available
    let has_count = functions
        .iter()
        .any(|c| c.label.to_uppercase().contains("COUNT"));
    let has_sum = functions
        .iter()
        .any(|c| c.label.to_uppercase().contains("SUM"));

    println!(
        "SELECT clause functions: {:?}",
        functions.iter().map(|f| &f.label).collect::<Vec<_>>()
    );

    assert!(
        has_count || has_sum,
        "Should suggest aggregate functions (COUNT, SUM) in SELECT clause. Got: {:?}",
        functions.iter().map(|f| &f.label).collect::<Vec<_>>()
    );
}
