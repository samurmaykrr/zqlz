//! Tests for SQL context detection

use super::test_helpers::*;
use crate::{AstSqlContext, ContextAnalyzer};
use zqlz_ui::widgets::Rope;

#[test]
fn keyword_relevance_ignores_clause_keywords_inside_protected_text() {
    let analyzer = ContextAnalyzer::new().expect("context analyzer");
    let text = Rope::from("-- select from where\n");
    let context = analyzer.analyze(&text, text.len());

    assert!(
        !matches!(context, AstSqlContext::FromClause),
        "protected FROM text should not force table completion context: {context:?}"
    );
}

#[test]
fn keyword_relevance_ignores_clause_keywords_inside_quoted_identifiers() {
    let analyzer = ContextAnalyzer::new().expect("context analyzer");
    let text = Rope::from(r#"SELECT "from" "#);
    let context = analyzer.analyze(&text, text.len());

    assert!(
        !matches!(context, AstSqlContext::FromClause),
        "quoted FROM identifier must not force FROM-clause relevance: {context:?}"
    );

    let mut lsp = create_test_lsp();
    let completions = lsp.get_completions(&text, text.len());

    assert!(
        has_completion(&completions, "FROM"),
        "SELECT context should still suggest FROM after quoted identifier, got: {:?}",
        completions
            .iter()
            .map(|completion| &completion.label)
            .collect::<Vec<_>>()
    );
}

#[test]
fn context_lookback_ignores_clause_words_inside_string_literals() {
    let analyzer = ContextAnalyzer::new().expect("context analyzer");
    let text = Rope::from("SELECT '-- FROM users");
    let context = analyzer.analyze(&text, text.len());

    assert!(
        !matches!(context, AstSqlContext::FromClause),
        "string literal FROM must not force table completion context: {context:?}"
    );
}

#[test]
fn context_lookback_ignores_clause_words_inside_quoted_identifiers() {
    let analyzer = ContextAnalyzer::new().expect("context analyzer");
    let text = Rope::from("SELECT \"UPDATE\"");
    let context = analyzer.analyze(&text, text.len());

    assert!(
        !matches!(context, AstSqlContext::FromClause),
        "quoted UPDATE identifier must not force table completion context: {context:?}"
    );
}

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

#[test]
fn test_select_list_ranks_columns_above_functions() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT lo");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    let first_field = completions
        .iter()
        .position(|completion| completion.kind == Some(lsp_types::CompletionItemKind::FIELD))
        .expect("select list should offer columns");
    let first_function = completions
        .iter()
        .position(|completion| completion.kind == Some(lsp_types::CompletionItemKind::FUNCTION));

    if let Some(first_function) = first_function {
        assert!(
            first_field < first_function,
            "Columns should outrank functions in a select list. Got: {:?}",
            completions
                .iter()
                .map(|completion| (completion.label.clone(), completion.kind))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn test_select_list_without_cached_columns_explains_why() {
    // Mirrors the real failure: tables are known but their columns have not been
    // fetched, which previously left a menu of nothing but SQL functions.
    let mut lsp = create_test_lsp();
    lsp.schema_cache.columns_by_table.clear();

    let text = Rope::from("SELECT c");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    let hints: Vec<&str> = completions
        .iter()
        .filter(|completion| completion.kind == Some(lsp_types::CompletionItemKind::TEXT))
        .map(|completion| completion.label.as_str())
        .collect();

    assert_eq!(
        hints.len(),
        1,
        "Expected exactly one explanatory item. Got: {:?}",
        completions
            .iter()
            .map(|completion| completion.label.clone())
            .collect::<Vec<_>>()
    );
    assert!(
        hints[0].contains("column"),
        "Hint should name the missing columns. Got: {}",
        hints[0]
    );
    assert!(
        !completions
            .iter()
            .any(|completion| completion.kind == Some(lsp_types::CompletionItemKind::FIELD)),
        "No columns are cached, so none should be offered"
    );
}

#[test]
fn test_select_list_does_not_suggest_table_names() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT us");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);
    let labels: Vec<String> = completions
        .iter()
        .map(|completion| completion.label.clone())
        .collect();

    assert!(
        !labels.contains(&"users".to_string()),
        "Table names belong to FROM/JOIN, not the select list. Got: {:?}",
        labels
    );
}

#[test]
fn test_select_list_is_scoped_to_the_from_clause() {
    // The grammar keeps `from` as a sibling of `select`, and an empty projection
    // makes it fold into the select item entirely — both used to leave the select
    // list unscoped, so it offered every column in the schema.
    let analyzer = crate::ContextAnalyzer::new().expect("analyzer");

    let cases: [(&str, usize, Vec<(&str, Option<&str>)>); 6] = [
        ("select  from audit_log", 7, vec![("audit_log", None)]),
        ("select x from audit_log", 7, vec![("audit_log", None)]),
        ("select  from audit_log where 1=1", 7, vec![("audit_log", None)]),
        ("select a, from audit_log", 10, vec![("audit_log", None)]),
        ("select  from users u", 7, vec![("users", Some("u"))]),
        (
            "select  from audit_log al join users u on 1=1",
            7,
            vec![("audit_log", Some("al")), ("users", Some("u"))],
        ),
    ];

    for (sql, cursor, expected) in cases {
        let context = analyzer.analyze(&Rope::from(sql), cursor);
        let crate::AstSqlContext::SelectList { available_tables } = context else {
            panic!("{sql:?} should analyse as a select list, got {context:?}");
        };
        let actual: Vec<(&str, Option<&str>)> = available_tables
            .iter()
            .map(|table| (table.table_name.as_str(), table.alias.as_deref()))
            .collect();
        assert_eq!(actual, expected, "wrong tables in scope for {sql:?}");
    }
}

#[test]
fn test_select_list_offers_only_the_scoped_tables_columns() {
    let mut lsp = create_test_lsp();
    let sql = "select  from audit_log";
    let text = Rope::from(sql);

    let completions = lsp.get_completions(&text, "select ".len());
    let fields: Vec<String> = completions
        .iter()
        .filter(|item| item.kind == Some(lsp_types::CompletionItemKind::FIELD))
        .map(|item| item.label.clone())
        .collect();

    assert!(
        fields.iter().any(|name| name == "log_id"),
        "expected audit_log columns, got: {fields:?}"
    );
    assert!(
        !fields.iter().any(|name| name == "username"),
        "columns from unrelated tables must not be offered: {fields:?}"
    );
}
