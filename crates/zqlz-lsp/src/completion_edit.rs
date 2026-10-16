use lsp_types::{CompletionItem, CompletionItemKind, Position};
use sqlparser::keywords::{Keyword, RESERVED_FOR_COLUMN_ALIAS, RESERVED_FOR_TABLE_ALIAS};
use sqlparser::tokenizer::Token;
use zqlz_core::sql_symbol_at_offset;
use zqlz_ui::widgets::Rope;
use zqlz_ui::widgets::input::RopeExt;

pub(crate) fn apply_completion_text_edits(
    completions: &mut [CompletionItem],
    text: &Rope,
    offset: usize,
) {
    let Some((start_position, end_position)) = word_range_at_offset(text, offset) else {
        return;
    };

    for completion in completions {
        if completion.text_edit.is_some() {
            continue;
        }

        let new_text = completion
            .insert_text
            .clone()
            .unwrap_or_else(|| completion.label.clone());

        completion.text_edit = Some(lsp_types::CompletionTextEdit::Edit(lsp_types::TextEdit {
            range: lsp_types::Range {
                start: start_position,
                end: end_position,
            },
            new_text,
        }));
    }
}

fn word_range_at_offset(text: &Rope, offset: usize) -> Option<(Position, Position)> {
    let sql = text.to_string();
    let offset = crate::clamp_to_char_boundary(&sql, offset);
    let symbol = sql_symbol_at_offset(&sql, offset)?;
    let start = text.offset_to_position(symbol.start);
    let end = text.offset_to_position(offset);

    Some((
        Position {
            line: start.line,
            character: start.character,
        },
        Position {
            line: end.line,
            character: end.character,
        },
    ))
}

/// Quotes table, view and column names that can't be written bare (`Table With Spaces`,
/// `select`, `1st_table`) with the dialect's identifier quote (backticks for MySQL).
pub(crate) fn quote_identifier_completions(completions: &mut [CompletionItem], quote: char) {
    for completion in completions {
        if !matches!(
            completion.kind,
            Some(
                CompletionItemKind::CLASS
                    | CompletionItemKind::INTERFACE
                    | CompletionItemKind::FIELD
            )
        ) {
            continue;
        }
        let Some(insert_text) = completion.insert_text.as_mut() else {
            continue;
        };
        let name = insert_text.trim_end();
        // Only plain names; anything else is a hand-built snippet.
        if name != completion.label {
            continue;
        }
        let trailing = insert_text[name.len()..].to_string();
        let quoted = name
            .split('.')
            .map(|part| quote_identifier_if_needed(part, quote))
            .collect::<Vec<_>>()
            .join(".");
        *insert_text = quoted + &trailing;
    }
}

pub(crate) fn quote_identifier_if_needed(name: &str, quote: char) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|first| first.is_alphabetic() || first == '_')
        && name
            .chars()
            .all(|character| character.is_alphanumeric() || matches!(character, '_' | '$'));
    let reserved = match Token::make_word(name, None) {
        Token::Word(word) => {
            word.keyword != Keyword::NoKeyword
                && (RESERVED_FOR_TABLE_ALIAS.contains(&word.keyword)
                    || RESERVED_FOR_COLUMN_ALIAS.contains(&word.keyword))
        }
        _ => false,
    };
    if plain && !reserved {
        return name.to_string();
    }
    let escaped = name.replace(quote, &format!("{quote}{quote}"));
    format!("{quote}{escaped}{quote}")
}
