use lsp_types::{CompletionItem, Position};
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
