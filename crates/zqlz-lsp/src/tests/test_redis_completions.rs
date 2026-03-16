use super::test_helpers::create_test_lsp_with_dialect;
use crate::SqlDialect;
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
