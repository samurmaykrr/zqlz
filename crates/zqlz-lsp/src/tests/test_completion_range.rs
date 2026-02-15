//! Tests for completion text replacement ranges
//!
//! These tests verify that when accepting a completion:
//! 1. Only the typed word gets replaced (not entire query)
//! 2. The trigger_start_offset tracks correctly across multiple characters
//! 3. Moving to a new word resets the trigger appropriately

use super::test_helpers::*;
use lsp_types::CompletionTextEdit;
use zqlz_ui::widgets::Rope;

#[test]
fn test_basic_table_completion_has_text_edit() {
    let mut lsp = create_test_lsp();

    // Simulate typing "SELECT * FROM aud"
    let text = Rope::from("SELECT * FROM aud");
    let offset = 17; // After "aud"

    let completions = lsp.get_completions(&text, offset);

    // Should suggest "audit_log" table
    let audit_completion = completions
        .iter()
        .find(|c| c.label == "audit_log")
        .expect("Should suggest audit_log table");

    // The completion should have text_edit for proper range replacement
    assert!(
        audit_completion.text_edit.is_some(),
        "Completion should have text_edit for range replacement"
    );

    // Verify the replacement text is correct
    if let Some(text_edit) = &audit_completion.text_edit {
        match text_edit {
            CompletionTextEdit::Edit(edit) => {
                // Note: insert_text may include trailing space for UX (e.g., "audit_log ")
                assert!(
                    edit.new_text.starts_with("audit_log"),
                    "Should replace with audit_log, got: {}",
                    edit.new_text
                );

                // Range should replace only "aud" (character positions 14-17)
                assert_eq!(edit.range.start.line, 0, "Should be on first line");
                assert_eq!(
                    edit.range.start.character, 14,
                    "Should start at 'a' in 'aud'"
                );
                assert_eq!(edit.range.end.character, 17, "Should end after 'aud'");
            }
            _ => panic!("Expected Edit text_edit type"),
        }
    }
}

#[test]
fn test_column_completion_range() {
    let mut lsp = create_test_lsp();

    // Simulate typing "SELECT log_i FROM audit_log"
    let text = Rope::from("SELECT log_i FROM audit_log");
    let offset = 12; // After "log_i"

    let completions = lsp.get_completions(&text, offset);

    // Should suggest "log_id" column
    let log_id_completion = completions
        .iter()
        .find(|c| c.label == "log_id")
        .expect("Should suggest log_id column");

    // The text_edit should replace only "log_i" (positions 7-12)
    if let Some(CompletionTextEdit::Edit(edit)) = &log_id_completion.text_edit {
        assert_eq!(
            edit.range.start.character, 7,
            "Should start at 'l' in 'log_i'"
        );
        assert_eq!(edit.range.end.character, 12, "Should end after 'log_i'");
        assert_eq!(
            edit.new_text, "log_id",
            "Should replace with full column name"
        );
    } else {
        panic!("Completion should have Edit text_edit for range replacement");
    }
}

#[test]
fn test_keyword_completion_range() {
    let mut lsp = create_test_lsp();

    // Simulate typing "SEL"
    let text = Rope::from("SEL");
    let offset = 3; // After "SEL"

    let completions = lsp.get_completions(&text, offset);

    // Should suggest "SELECT" keyword
    let select_completion = completions
        .iter()
        .find(|c| c.label == "SELECT")
        .expect("Should suggest SELECT keyword");

    // The text_edit should replace only "SEL" (positions 0-3)
    if let Some(CompletionTextEdit::Edit(edit)) = &select_completion.text_edit {
        assert_eq!(edit.range.start.character, 0, "Should start at beginning");
        assert_eq!(edit.range.end.character, 3, "Should end after 'SEL'");
        assert!(
            edit.new_text.contains("SELECT"),
            "Should replace with SELECT keyword"
        );
    } else {
        panic!("Completion should have Edit text_edit for range replacement");
    }
}

#[test]
fn test_where_clause_completion_range() {
    let mut lsp = create_test_lsp();

    // Simulate typing "SELECT * FROM users WHERE user"
    let text = Rope::from("SELECT * FROM users WHERE user");
    let offset = 30; // After "user"

    let completions = lsp.get_completions(&text, offset);

    // Should suggest "user_id" or "username" column
    let user_column = completions
        .iter()
        .find(|c| c.label == "user_id" || c.label == "username")
        .expect("Should suggest user_id or username column");

    // The text_edit should replace only "user" (positions 26-30)
    if let Some(CompletionTextEdit::Edit(edit)) = &user_column.text_edit {
        assert_eq!(
            edit.range.start.character, 26,
            "Should start at 'u' in 'user'"
        );
        assert_eq!(edit.range.end.character, 30, "Should end after 'user'");
        assert!(
            edit.new_text.starts_with("user"),
            "Should replace with user* column"
        );
    } else {
        panic!("Completion should have Edit text_edit for range replacement");
    }
}

#[test]
fn test_single_character_completion_range() {
    let mut lsp = create_test_lsp();

    // Simulate typing "SELECT * FROM u"
    let text = Rope::from("SELECT * FROM u");
    let offset = 15; // After "u"

    let completions = lsp.get_completions(&text, offset);

    // Should suggest "users" table
    let users_completion = completions
        .iter()
        .find(|c| c.label == "users")
        .expect("Should suggest users table");

    // The text_edit should replace only "u" (positions 14-15)
    if let Some(CompletionTextEdit::Edit(edit)) = &users_completion.text_edit {
        assert_eq!(edit.range.start.character, 14, "Should start at 'u'");
        assert_eq!(edit.range.end.character, 15, "Should end after 'u'");
        assert!(
            edit.new_text.starts_with("users"),
            "Should replace with users table, got: {}",
            edit.new_text
        );
    } else {
        panic!("Completion should have Edit text_edit for range replacement");
    }
}

#[test]
fn test_multiline_completion_range() {
    let mut lsp = create_test_lsp();

    // Simulate typing on line 2:
    // SELECT *
    // FROM aud
    let text = Rope::from("SELECT *\nFROM aud");
    let offset = 17; // After "aud" on line 2

    let completions = lsp.get_completions(&text, offset);

    // Should suggest "audit_log" table
    let audit_completion = completions
        .iter()
        .find(|c| c.label == "audit_log")
        .expect("Should suggest audit_log table");

    // The text_edit should replace only "aud" on line 2
    if let Some(CompletionTextEdit::Edit(edit)) = &audit_completion.text_edit {
        assert_eq!(edit.range.start.line, 1, "Should be on line 2 (0-indexed)");
        assert_eq!(
            edit.range.start.character, 5,
            "Should start at 'a' in 'aud'"
        );
        assert_eq!(edit.range.end.line, 1, "Should end on line 2");
        assert_eq!(edit.range.end.character, 8, "Should end after 'aud'");
        assert!(
            edit.new_text.starts_with("audit_log"),
            "Should replace with audit_log, got: {}",
            edit.new_text
        );
    } else {
        panic!("Completion should have Edit text_edit for range replacement");
    }
}

#[test]
fn test_completion_range_with_prefix_match() {
    let mut lsp = create_test_lsp();

    // Simulate typing "SELECT * FROM audit_l"
    let text = Rope::from("SELECT * FROM audit_l");
    let offset = 21; // After "audit_l"

    let completions = lsp.get_completions(&text, offset);

    // Should suggest "audit_log" table
    let audit_completion = completions
        .iter()
        .find(|c| c.label == "audit_log")
        .expect("Should suggest audit_log table");

    // The text_edit should replace the entire "audit_l" (positions 14-21)
    if let Some(CompletionTextEdit::Edit(edit)) = &audit_completion.text_edit {
        assert_eq!(
            edit.range.start.character, 14,
            "Should start at 'a' in 'audit_l'"
        );
        assert_eq!(edit.range.end.character, 21, "Should end after 'audit_l'");
        assert!(
            edit.new_text.starts_with("audit_log"),
            "Should replace with audit_log, got: {}",
            edit.new_text
        );
    } else {
        panic!("Completion should have Edit text_edit for range replacement");
    }
}

#[test]
fn test_completion_range_does_not_affect_previous_words() {
    let mut lsp = create_test_lsp();

    // Simulate typing "SELECT log_id FROM aud"
    let text = Rope::from("SELECT log_id FROM aud");
    let offset = 22; // After "aud"

    let completions = lsp.get_completions(&text, offset);

    // Should suggest "audit_log" table
    let audit_completion = completions
        .iter()
        .find(|c| c.label == "audit_log")
        .expect("Should suggest audit_log table");

    // The text_edit should ONLY replace "aud", not "log_id" or anything before
    if let Some(CompletionTextEdit::Edit(edit)) = &audit_completion.text_edit {
        assert_eq!(
            edit.range.start.character, 19,
            "Should start at 'a' in 'aud'"
        );
        assert_eq!(edit.range.end.character, 22, "Should end after 'aud'");
        assert!(
            edit.new_text.starts_with("audit_log"),
            "Should replace with audit_log, got: {}",
            edit.new_text
        );

        // Verify the range doesn't include "log_id"
        assert!(
            edit.range.start.character > 7,
            "Should not include 'log_id' which starts at position 7"
        );
    } else {
        panic!("Completion should have Edit text_edit for range replacement");
    }
}

#[test]
fn test_completion_at_end_of_query() {
    let mut lsp = create_test_lsp();

    // Simulate typing "SELECT * FROM users WHERE user_i" (at end of query)
    let text = Rope::from("SELECT * FROM users WHERE user_i");
    let offset = 32; // After "user_i" - string is 32 chars (0-31)

    let completions = lsp.get_completions(&text, offset);

    // Debug: print what completions we got
    if completions.is_empty() {
        eprintln!("WARNING: No completions returned!");
    } else {
        eprintln!(
            "Got {} completions: {:?}",
            completions.len(),
            completions.iter().map(|c| &c.label).collect::<Vec<_>>()
        );
    }

    // Should suggest "user_id" column
    let user_id_completion = completions
        .iter()
        .find(|c| c.label == "user_id")
        .expect("Should suggest user_id column");

    // The text_edit should replace only "user_i" at the end
    if let Some(CompletionTextEdit::Edit(edit)) = &user_id_completion.text_edit {
        assert_eq!(
            edit.range.start.character, 26,
            "Should start at 'u' in 'user_i'"
        );
        assert_eq!(
            edit.range.end.character, 32,
            "Should end after 'user_i' at query end"
        );
        assert_eq!(
            edit.new_text, "user_id",
            "Should replace with full column name"
        );
    } else {
        panic!("Completion should have Edit text_edit for range replacement");
    }
}

#[test]
fn test_completion_range_with_dots() {
    let mut lsp = create_test_lsp();

    // Simulate typing "SELECT users.use" (qualified column name)
    let text = Rope::from("SELECT users.use");
    let offset = 16; // After "use"

    let completions = lsp.get_completions(&text, offset);

    // Should suggest columns like "user_id", "username", etc.
    let user_column = completions
        .iter()
        .find(|c| c.label == "user_id" || c.label == "username")
        .expect("Should suggest user_id or username column");

    // The text_edit should replace only "use" AFTER the dot (positions 13-16)
    if let Some(CompletionTextEdit::Edit(edit)) = &user_column.text_edit {
        assert_eq!(
            edit.range.start.character, 13,
            "Should start at 'u' in '.use'"
        );
        assert_eq!(edit.range.end.character, 16, "Should end after 'use'");
        assert!(
            edit.new_text.starts_with("user"),
            "Should start with 'user'"
        );
    } else {
        panic!("Completion should have Edit text_edit for range replacement");
    }
}

/// This is the CRITICAL test for the bug we fixed
/// Before fix: Would replace entire query or append incorrectly
/// After fix: Should replace only "aud" with "audit_log"
#[test]
fn test_trigger_offset_tracks_correctly_progressive_typing() {
    let mut lsp = create_test_lsp();

    // This simulates the user typing character by character: "a", "au", "aud"
    // The trigger_start_offset should remain at position 19 throughout

    // Step 1: Type "SELECT log_id FROM a"
    let text1 = Rope::from("SELECT log_id FROM a");
    let offset1 = 20;
    let completions1 = lsp.get_completions(&text1, offset1);

    // Should start suggesting tables starting with 'a'
    assert!(
        completions1.iter().any(|c| c.label == "audit_log"),
        "Should suggest audit_log when typing 'a'"
    );

    // Step 2: Type "SELECT log_id FROM au"
    let text2 = Rope::from("SELECT log_id FROM au");
    let offset2 = 21;
    let completions2 = lsp.get_completions(&text2, offset2);

    // Should still suggest audit_log
    let audit_completion2 = completions2
        .iter()
        .find(|c| c.label == "audit_log")
        .expect("Should still suggest audit_log when typing 'au'");

    // CRITICAL: Range should STILL start at position 19 (where 'a' started)
    if let Some(CompletionTextEdit::Edit(edit)) = &audit_completion2.text_edit {
        assert_eq!(
            edit.range.start.character, 19,
            "Range start should stay at original 'a' position (19)"
        );
        assert_eq!(
            edit.range.end.character, 21,
            "Range end should be at current cursor (21)"
        );
    }

    // Step 3: Type "SELECT log_id FROM aud"
    let text3 = Rope::from("SELECT log_id FROM aud");
    let offset3 = 22;
    let completions3 = lsp.get_completions(&text3, offset3);

    // Should still suggest audit_log
    let audit_completion3 = completions3
        .iter()
        .find(|c| c.label == "audit_log")
        .expect("Should still suggest audit_log when typing 'aud'");

    // CRITICAL: Range should STILL start at position 19 (where 'a' started)
    if let Some(CompletionTextEdit::Edit(edit)) = &audit_completion3.text_edit {
        assert_eq!(
            edit.range.start.character, 19,
            "Range start should STILL be at original 'a' position (19), NOT reset on every character!"
        );
        assert_eq!(
            edit.range.end.character, 22,
            "Range end should be at current cursor (22)"
        );
        assert!(
            edit.new_text.starts_with("audit_log"),
            "Should replace entire 'aud' with 'audit_log', got: {}",
            edit.new_text
        );
    } else {
        panic!("Completion should have Edit text_edit for range replacement");
    }
}

#[test]
fn test_completion_range_resets_after_space() {
    let mut lsp = create_test_lsp();

    // Type "SELECT * FROM users WHERE " (with trailing space)
    let text1 = Rope::from("SELECT * FROM users WHERE ");
    let offset1 = 26;
    let _completions1 = lsp.get_completions(&text1, offset1);

    // Now type "u" - this should start a NEW word
    let text2 = Rope::from("SELECT * FROM users WHERE u");
    let offset2 = 27;
    let completions2 = lsp.get_completions(&text2, offset2);

    // Should suggest columns starting with 'u'
    let user_completion = completions2
        .iter()
        .find(|c| c.label == "user_id")
        .expect("Should suggest user_id");

    // The range should start at 'u' (position 26), not include "WHERE "
    if let Some(CompletionTextEdit::Edit(edit)) = &user_completion.text_edit {
        assert_eq!(
            edit.range.start.character, 26,
            "Range should start at new word 'u', not include previous words"
        );
        assert_eq!(edit.range.end.character, 27, "Range should end after 'u'");
    } else {
        panic!("Completion should have Edit text_edit for range replacement");
    }
}
