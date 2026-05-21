use crate::diagnostics::{
    DIAGNOSTIC_CODE_SQL_TOKENIZER, DIAGNOSTIC_CODE_SQLPARSER_SYNTAX,
    DIAGNOSTIC_CODE_TREE_SITTER_SYNTAX,
};
use lsp_types::{CodeAction, Diagnostic, NumberOrString, Position, Range, TextEdit, Uri};
use sqlparser::dialect::Dialect;
use std::collections::HashMap;
use zqlz_core::{
    line_column_for_offset, line_column_to_offset, semicolon_insertion_offset,
    unclosed_quote_insertion,
};
use zqlz_ui::widgets::Rope;

pub(crate) fn get_code_actions(
    text: &Rope,
    offset: usize,
    diagnostics: &[Diagnostic],
    dialect: &dyn Dialect,
) -> Vec<CodeAction> {
    tracing::debug!("get_code_actions: offset={}", offset);

    let mut code_actions = Vec::new();
    let text_str = text.to_string();

    for diagnostic in diagnostics {
        let range = &diagnostic.range;
        let Some(diagnostic_start) =
            line_column_to_offset(&text_str, range.start.line, range.start.character)
        else {
            continue;
        };
        let Some(diagnostic_end) =
            line_column_to_offset(&text_str, range.end.line, range.end.character)
        else {
            continue;
        };

        if offset >= diagnostic_start && offset <= diagnostic_end {
            code_actions.extend(generate_actions_for_diagnostic(
                diagnostic, &text_str, dialect,
            ));
        }
    }

    if code_actions.is_empty() {
        code_actions.extend(generate_context_actions(&text_str, dialect));
    }

    tracing::debug!("found {} code actions", code_actions.len());
    code_actions
}

fn generate_actions_for_diagnostic(
    diagnostic: &Diagnostic,
    text: &str,
    dialect: &dyn Dialect,
) -> Vec<CodeAction> {
    let mut actions = Vec::new();

    let is_internal_syntax_diagnostic = diagnostic_code_is(
        diagnostic,
        &[
            DIAGNOSTIC_CODE_SQL_TOKENIZER,
            DIAGNOSTIC_CODE_SQLPARSER_SYNTAX,
            DIAGNOSTIC_CODE_TREE_SITTER_SYNTAX,
        ],
    );
    let is_tokenizer_diagnostic = diagnostic_code_is(diagnostic, &[DIAGNOSTIC_CODE_SQL_TOKENIZER]);

    if is_internal_syntax_diagnostic
        && let Some(insert_offset) = semicolon_insertion_offset(text, dialect)
    {
        let (line, character) = line_column_for_offset(text, insert_offset);
        if let Some(action) =
            replace_text_action("Add missing semicolon", line, character, ";", true)
        {
            actions.push(action);
        }
    }

    if is_tokenizer_diagnostic
        && let Some((line, character, quote)) = unclosed_quote_insertion(
            text,
            diagnostic.range.start.line,
            diagnostic.range.start.character,
            dialect,
        )
        && let Some(action) = replace_text_action(
            "Close string literal",
            line,
            character,
            &quote.to_string(),
            true,
        )
    {
        actions.push(action);
    }

    actions
}

fn generate_context_actions(text: &str, dialect: &dyn Dialect) -> Vec<CodeAction> {
    let mut actions = Vec::new();

    if let Some(insert_offset) = semicolon_insertion_offset(text, dialect) {
        let (line, character) = line_column_for_offset(text, insert_offset);
        if let Some(action) = replace_text_action("Add semicolon", line, character, ";", false) {
            actions.push(action);
        }
    }

    actions
}

fn replace_text_action(
    title: &str,
    line: u32,
    character: u32,
    new_text: &str,
    is_preferred: bool,
) -> Option<CodeAction> {
    let Ok(uri) = "sql://internal".parse::<Uri>() else {
        return None;
    };

    Some(CodeAction {
        title: title.to_string(),
        kind: Some(lsp_types::CodeActionKind::QUICKFIX),
        diagnostics: None,
        edit: Some(lsp_types::WorkspaceEdit {
            changes: Some(HashMap::from([(
                uri,
                vec![TextEdit {
                    range: Range {
                        start: Position { line, character },
                        end: Position { line, character },
                    },
                    new_text: new_text.to_string(),
                }],
            )])),
            document_changes: None,
            change_annotations: None,
        }),
        command: None,
        is_preferred: Some(is_preferred),
        disabled: None,
        data: None,
    })
}

fn diagnostic_code_is(diagnostic: &Diagnostic, expected_codes: &[&str]) -> bool {
    let Some(NumberOrString::String(code)) = diagnostic.code.as_ref() else {
        return false;
    };

    expected_codes
        .iter()
        .any(|expected_code| code == expected_code)
}
