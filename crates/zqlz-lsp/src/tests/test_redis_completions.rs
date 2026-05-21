use super::test_helpers::create_test_lsp_with_dialect;
use crate::SqlDialect;
use lsp_types::InsertTextFormat;
use zqlz_ui::widgets::Rope;

#[test]
fn redis_top_level_completion_shows_commands_only() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::Redis);
    let text = Rope::from("GE");
    let completions = lsp.get_completions(&text, text.to_string().len());

    let labels: Vec<_> = completions.iter().map(|item| item.label.as_str()).collect();
    assert!(labels.contains(&"GET"));
    assert!(!labels.contains(&"SELECT"));
}

#[test]
fn redis_subcommand_completion_after_space_stays_command_aware() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::Redis);
    let text = Rope::from("ACL ");
    let completions = lsp.get_completions(&text, text.to_string().len());

    let labels: Vec<_> = completions.iter().map(|item| item.label.as_str()).collect();
    assert!(labels.contains(&"LIST"));
    assert!(labels.contains(&"GETUSER"));
    assert!(!labels.contains(&"users"));
    assert!(!labels.contains(&"FROM"));
}

#[test]
fn redis_top_level_completion_uses_driver_snippet_metadata() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::Redis);
    let text = Rope::from("SE");
    let completions = lsp.get_completions(&text, text.to_string().len());

    let set = completions
        .iter()
        .find(|item| item.label == "SET")
        .expect("SET completion");
    assert_eq!(set.insert_text.as_deref(), Some("SET ${1:key} ${2:value}"));
    assert_eq!(set.insert_text_format, Some(InsertTextFormat::SNIPPET));
    assert!(
        set.detail
            .as_deref()
            .is_some_and(|detail| detail.contains("Set string value")),
        "SET detail should come from Redis driver metadata: {:?}",
        set.detail
    );
}

#[test]
fn redis_subcommand_completion_uses_driver_snippet_suffix() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::Redis);
    let text = Rope::from("CONFIG ");
    let completions = lsp.get_completions(&text, text.to_string().len());

    let get = completions
        .iter()
        .find(|item| item.label == "GET")
        .expect("CONFIG GET completion");
    assert_eq!(get.insert_text.as_deref(), Some("GET ${1:parameter}"));
    assert_eq!(get.insert_text_format, Some(InsertTextFormat::SNIPPET));
}

#[test]
fn redis_subcommand_candidates_prefer_driver_metadata_then_validator_coverage() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::Redis);
    let text = Rope::from("ACL ");
    let completions = lsp.get_completions(&text, text.to_string().len());

    let labels: Vec<_> = completions.iter().map(|item| item.label.as_str()).collect();
    let list_index = labels
        .iter()
        .position(|label| *label == "LIST")
        .expect("driver metadata ACL LIST");
    let getuser_index = labels
        .iter()
        .position(|label| *label == "GETUSER")
        .expect("validator fallback ACL GETUSER");
    assert!(
        list_index < getuser_index,
        "driver metadata candidates should rank before validator fallback: {labels:?}"
    );
}

#[test]
fn redis_partial_subcommand_completion_filters_results() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::Redis);
    let text = Rope::from("ACL LI");
    let completions = lsp.get_completions(&text, text.to_string().len());

    let labels: Vec<_> = completions.iter().map(|item| item.label.as_str()).collect();
    assert!(labels.contains(&"LIST"));
    assert!(!labels.contains(&"FROM"));
}

#[test]
fn redis_argument_position_does_not_fall_back_to_sql_noise() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::Redis);
    let text = Rope::from("SET mykey ");
    let completions = lsp.get_completions(&text, text.to_string().len());

    assert!(completions.is_empty());
}

#[test]
fn redis_completion_uses_command_tokenizer_for_quoted_tokens() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::Redis);
    let text = Rope::from("ACL \"LI");
    let completions = lsp.get_completions(&text, text.to_string().len());

    let labels: Vec<_> = completions.iter().map(|item| item.label.as_str()).collect();
    assert!(labels.contains(&"LIST"));
}

#[test]
fn command_completion_routing_uses_driver_syntax_capabilities() {
    let mut lsp = create_test_lsp_with_dialect(SqlDialect::Generic);
    lsp.driver_type = " Redis ".to_string();
    let text = Rope::from("GE");
    let completions = lsp.get_completions(&text, text.to_string().len());

    let labels: Vec<_> = completions.iter().map(|item| item.label.as_str()).collect();
    assert!(labels.contains(&"GET"));
    assert!(!labels.contains(&"SELECT"));
}
