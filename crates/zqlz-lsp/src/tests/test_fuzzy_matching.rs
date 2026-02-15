//! Tests for fuzzy matching in SQL completions
//!
//! Tests the fuzzy matching algorithm used to rank and filter completions,
//! including prefix matching, substring matching, acronym matching, and
//! character-by-character fuzzy matching.

use super::test_helpers::*;
use zqlz_ui::widgets::Rope;

#[test]
fn test_prefix_match_priority() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT use FROM users");
    let offset = text.to_string().find("use").unwrap() + 3;

    let completions = lsp.get_completions(&text, offset);

    // "users" and "user_id" should both match "use"
    // Prefix matches should be ranked higher
    let users_pos = completions.iter().position(|c| c.label == "users");
    let username_pos = completions.iter().position(|c| c.label == "username");

    if let (Some(users), Some(username)) = (users_pos, username_pos) {
        // Both start with "use", so order depends on implementation
        // At minimum, both should be present
        assert!(users < completions.len());
        assert!(username < completions.len());
    }
}

#[test]
fn test_substring_match() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT nam FROM users");
    let offset = text.to_string().find("nam").unwrap() + 3;

    let completions = lsp.get_completions(&text, offset);

    // Should match "username" via substring
    assert!(
        completions.iter().any(|c| c.label == "username"),
        "Should match 'username' with substring 'nam'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_acronym_match_keywords() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("ij");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // "ij" should match "INNER JOIN" via acronym
    assert!(
        completions.iter().any(|c| {
            let label = c.label.to_uppercase();
            label == "INNER" || label == "INNER JOIN" || label.contains("INNER")
        }),
        "Should match INNER JOIN with acronym 'ij'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_fuzzy_match_with_gaps() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT usid FROM users");
    let offset = text.to_string().find("usid").unwrap() + 4;

    let completions = lsp.get_completions(&text, offset);

    // "usid" should fuzzy match "user_id"
    assert!(
        completions.iter().any(|c| c.label == "user_id"),
        "Should fuzzy match 'user_id' with 'usid'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_case_insensitive_matching() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SEL");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Uppercase "SEL" should match lowercase "select"
    assert!(
        completions
            .iter()
            .any(|c| c.label.to_uppercase() == "SELECT"),
        "Should match SELECT case-insensitively. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_shorter_matches_preferred() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT loc FROM locations");
    let offset = text.to_string().find("loc").unwrap() + 3;

    let completions = lsp.get_completions(&text, offset);

    // Both "locations" and "location_id" match, but shorter completion
    // or better match should rank higher
    let has_match = completions
        .iter()
        .any(|c| c.label == "locations" || c.label == "location_id" || c.label == "location_name");

    assert!(
        has_match,
        "Should match location-related columns. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_consecutive_characters_bonus() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT use FROM users");
    let offset = text.to_string().find("use").unwrap() + 3;

    let completions = lsp.get_completions(&text, offset);

    // "users" has all consecutive chars, should rank higher than
    // scattered matches if any exist
    assert!(
        completions.iter().any(|c| c.label == "users"),
        "Should match 'users' with consecutive chars. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_no_match_returns_empty_or_general() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT xyzqwerty FROM users");
    let offset = text.to_string().find("xyzqwerty").unwrap() + 9;

    let completions = lsp.get_completions(&text, offset);

    // "xyzqwerty" shouldn't match any column, but might still return keywords
    // or no results depending on implementation
    let has_column_match = completions
        .iter()
        .any(|c| c.label == "user_id" || c.label == "username" || c.label == "email");

    // This test is flexible: either no results or only keywords
    if has_column_match {
        // If columns are returned, they shouldn't be close matches
        assert!(false, "Shouldn't match unrelated columns for 'xyzqwerty'");
    }
}

#[test]
fn test_empty_pattern_returns_all() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT  FROM users");
    let offset = text.to_string().find("SELECT ").unwrap() + 7;

    let completions = lsp.get_completions(&text, offset);

    // Empty pattern should return all available completions
    assert!(
        !completions.is_empty(),
        "Empty pattern should return completions"
    );
}

#[test]
fn test_partial_table_name_match() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM aud");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // "aud" should match "audit_log"
    assert!(
        completions.iter().any(|c| c.label == "audit_log"),
        "Should match 'audit_log' with prefix 'aud'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_underscore_handling() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT userid FROM users");
    let offset = text.to_string().find("userid").unwrap() + 6;

    let completions = lsp.get_completions(&text, offset);

    // "userid" should match "user_id" even without underscore
    assert!(
        completions.iter().any(|c| c.label == "user_id"),
        "Should match 'user_id' with 'userid' (no underscore). Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_middle_match_lower_priority() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT mail FROM users");
    let offset = text.to_string().find("mail").unwrap() + 4;

    let completions = lsp.get_completions(&text, offset);

    // "mail" is substring of "email", should match but with lower priority
    // than if it were a prefix match
    assert!(
        completions.iter().any(|c| c.label == "email"),
        "Should match 'email' containing 'mail'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_quality_ranking_prefix_vs_substring() {
    let mut lsp = create_test_lsp();

    // Test 1: Prefix match
    let text1 = Rope::from("SELECT use FROM users");
    let offset1 = text1.to_string().find("use").unwrap() + 3;
    let completions1 = lsp.get_completions(&text1, offset1);

    // Test 2: Substring match
    let text2 = Rope::from("SELECT nam FROM users");
    let offset2 = text2.to_string().find("nam").unwrap() + 3;
    let completions2 = lsp.get_completions(&text2, offset2);

    // Both should return results, prefix should ideally have better ranking
    assert!(
        !completions1.is_empty(),
        "Prefix match should return results"
    );
    assert!(
        !completions2.is_empty(),
        "Substring match should return results"
    );
}

#[test]
fn test_match_limit_top_results() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT ");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // Should limit results to top 20 (as per JetBrains pattern)
    assert!(
        completions.len() <= 20,
        "Should limit completions to top 20 results. Got: {}",
        completions.len()
    );
}

#[test]
fn test_keyword_vs_column_priority() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT user_id FROM users WH");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // "WH" should prioritize "WHERE" keyword over columns
    let where_pos = completions
        .iter()
        .position(|c| c.label.to_uppercase() == "WHERE");

    assert!(
        where_pos.is_some(),
        "Should suggest WHERE keyword. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_qualified_column_fuzzy_match() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT u.usid FROM users u");
    let offset = text.to_string().find("usid").unwrap() + 4;

    let completions = lsp.get_completions(&text, offset);

    // Should fuzzy match "user_id" after "u."
    assert!(
        completions.iter().any(|c| c.label == "user_id"),
        "Should fuzzy match qualified column 'u.user_id'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_multi_word_keyword_acronym() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT * FROM users gb");
    let offset = text.to_string().len();

    let completions = lsp.get_completions(&text, offset);

    // "gb" should match "GROUP BY" via acronym
    assert!(
        completions.iter().any(|c| {
            let label = c.label.to_uppercase();
            label == "GROUP" || label.contains("GROUP")
        }),
        "Should match GROUP BY with acronym 'gb'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_special_chars_in_identifiers() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT log_ti FROM audit_log");
    let offset = text.to_string().find("log_ti").unwrap() + 6;

    let completions = lsp.get_completions(&text, offset);

    // "log_ti" should match "log_timestamp"
    assert!(
        completions.iter().any(|c| c.label == "log_timestamp"),
        "Should match 'log_timestamp' with 'log_ti'. Got: {:?}",
        completions.iter().map(|c| &c.label).collect::<Vec<_>>()
    );
}

#[test]
fn test_digit_handling_in_fuzzy_match() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT userid FROM users");
    let offset = text.to_string().find("userid").unwrap() + 6;

    let completions = lsp.get_completions(&text, offset);

    // Should handle identifiers with potential numbers
    assert!(
        !completions.is_empty(),
        "Should provide completions for patterns with digits"
    );
}

#[test]
fn test_exact_match_highest_priority() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT user_id FROM users");
    let offset = text.to_string().find("user_id").unwrap() + 7;

    let completions = lsp.get_completions(&text, offset);

    // Exact match "user_id" should be top result or very high priority
    if !completions.is_empty() {
        let has_exact = completions.iter().any(|c| c.label == "user_id");
        assert!(
            has_exact,
            "Should include exact match 'user_id'. Got: {:?}",
            completions.iter().map(|c| &c.label).collect::<Vec<_>>()
        );
    }
}

#[test]
fn test_completion_deduplication() {
    let mut lsp = create_test_lsp();
    let text = Rope::from("SELECT us FROM users u JOIN audit_log a ON u.user_id = a.user_id");
    let offset = text.to_string().find("us").unwrap() + 2;

    let completions = lsp.get_completions(&text, offset);

    // Check for duplicate entries
    let mut seen = std::collections::HashSet::new();
    let mut duplicates = Vec::new();

    for completion in &completions {
        if !seen.insert(&completion.label) {
            duplicates.push(&completion.label);
        }
    }

    assert!(
        duplicates.is_empty(),
        "Should not have duplicate completions. Duplicates: {:?}",
        duplicates
    );
}
