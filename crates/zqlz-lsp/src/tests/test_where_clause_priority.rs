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
fn test_where_clause_condition_keywords_use_driver_metadata() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE A");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    let and_keyword = completions
        .iter()
        .find(|completion| completion.label == "AND")
        .expect("AND keyword completion");

    assert_eq!(and_keyword.kind, Some(CompletionItemKind::KEYWORD));
    assert!(
        and_keyword
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("SQL Keyword")),
        "AND detail should come from dialect keyword metadata path: {:?}",
        and_keyword.detail
    );
}

#[test]
fn test_where_clause_operator_completions_use_core_syntax_metadata() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users WHERE user_id ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    let equals_operator = completions
        .iter()
        .find(|completion| completion.label == "= (equals)")
        .expect("equals operator completion");

    assert_eq!(equals_operator.kind, Some(CompletionItemKind::OPERATOR));
    assert_eq!(equals_operator.insert_text.as_deref(), Some("= "));
    assert_eq!(equals_operator.sort_text.as_deref(), Some("9_operator_eq"));
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

#[test]
fn test_where_clause_prefers_in_scope_columns_over_keywords() {
    let mut lsp = create_test_lsp();
    let text =
        Rope::from("SELECT * FROM users u JOIN audit_log a ON u.user_id = a.log_id WHERE us");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    let user_id_pos = completions
        .iter()
        .position(|c| c.label == "u.user_id" || c.label == "user_id");
    let where_pos = completions
        .iter()
        .position(|c| c.label.eq_ignore_ascii_case("WHERE"));

    assert!(
        user_id_pos.is_some(),
        "Should include an in-scope user column"
    );
    if let Some(keyword_pos) = where_pos {
        assert!(
            user_id_pos.unwrap() < keyword_pos,
            "In-scope columns should rank ahead of keywords in WHERE clause"
        );
    }
}

#[test]
fn test_where_clause_offers_json_operators_for_postgres() {
    let mut lsp = create_test_lsp_with_dialect(crate::SqlDialect::PostgreSQL);
    let text = Rope::from("SELECT * FROM users WHERE data->");
    let offset = text.to_string().len();

    let completions = lsp.get_completions_with_trigger(&text, offset, false);
    let arrow_text_operator = completions
        .iter()
        .find(|completion| completion.label.starts_with("->> "))
        .expect("->> operator completion for postgres");

    assert_eq!(arrow_text_operator.kind, Some(CompletionItemKind::OPERATOR));
    // The typed `->` stays in the document, so only the remainder is inserted.
    assert_eq!(arrow_text_operator.insert_text.as_deref(), Some(">"));

    assert!(
        !completions
            .iter()
            .any(|completion| completion.label.starts_with("@> ")),
        "operators not matching the typed run should be filtered out"
    );
}

#[test]
fn test_where_clause_offers_containment_operators_for_postgres() {
    let mut lsp = create_test_lsp_with_dialect(crate::SqlDialect::PostgreSQL);
    let text = Rope::from("SELECT * FROM users WHERE data @");
    let offset = text.to_string().len();

    let completions = lsp.get_completions_with_trigger(&text, offset, false);
    assert!(
        completions
            .iter()
            .any(|completion| completion.label.starts_with("@> ")),
        "@> operator completion for postgres"
    );
}

#[test]
fn test_where_clause_hides_json_operators_for_sqlite() {
    let mut lsp = create_test_lsp_with_dialect(crate::SqlDialect::SQLite);
    let text = Rope::from("SELECT * FROM users WHERE data->");
    let offset = text.to_string().len();

    let completions = lsp.get_completions_with_trigger(&text, offset, false);
    assert!(
        !completions
            .iter()
            .any(|completion| completion.label.starts_with("->> ")),
        "JSON operators should not be offered for sqlite"
    );
}

#[test]
fn test_select_list_offers_json_operators_for_postgres() {
    let mut lsp = create_test_lsp_with_dialect(crate::SqlDialect::PostgreSQL);
    let sql = "SELECT data-> FROM users";
    let text = Rope::from(sql);
    let offset = "SELECT data->".len();

    let completions = lsp.get_completions_with_trigger(&text, offset, false);
    assert!(
        completions
            .iter()
            .any(|completion| completion.label.starts_with("-> ")
                || completion.label.starts_with("->> ")),
        "JSON operator completions should be available in the SELECT list for postgres"
    );
}
