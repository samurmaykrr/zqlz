use ropey::{Rope, iter::Chunks};
use tree_sitter::Node;
use zqlz_core::{
    SqlProtectedRange, sql_protected_ranges_with_unclosed_dollar_strings,
    unclosed_dollar_quoted_string_start,
};

pub(super) const ROPE_OVERLAY_CONTEXT_BYTES: usize = 64 * 1024;
const ROPE_DOLLAR_STRING_LOOKBEHIND_BYTES: usize = 1024 * 1024;

pub(super) struct RopeTextProvider<'a>(pub(super) &'a Rope);

pub(super) struct RopeByteChunks<'a> {
    chunks: Chunks<'a>,
    next_byte: usize,
    start: usize,
    end: usize,
}

impl<'a> tree_sitter::TextProvider<&'a [u8]> for RopeTextProvider<'a> {
    type I = RopeByteChunks<'a>;

    fn text(&mut self, node: Node) -> Self::I {
        let range = node.byte_range();
        let (chunks, chunk_byte, _, _) = self.0.chunks_at_byte(range.start);
        RopeByteChunks {
            chunks,
            next_byte: chunk_byte,
            start: range.start,
            end: range.end,
        }
    }
}

impl<'a> Iterator for RopeByteChunks<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_byte >= self.end {
            return None;
        }

        let chunk = self.chunks.next()?;
        let chunk_start = self.next_byte;
        self.next_byte += chunk.len();

        if chunk_start >= self.end {
            return None;
        }

        let start = self.start.saturating_sub(chunk_start).min(chunk.len());
        let end = (self.end - chunk_start).min(chunk.len());
        if start >= end {
            None
        } else {
            Some(&chunk.as_bytes()[start..end])
        }
    }
}

pub(super) fn clamp_rope_byte_range(
    text: &Rope,
    byte_range: std::ops::Range<usize>,
) -> std::ops::Range<usize> {
    let len = text.len_bytes();
    let start = clamp_rope_byte_to_char_boundary(text, byte_range.start.min(len));
    let end = clamp_rope_byte_to_char_boundary(text, byte_range.end.min(len));
    start..end
}

pub(super) fn clamp_rope_byte_to_char_boundary(text: &Rope, byte_offset: usize) -> usize {
    text.try_byte_to_char(byte_offset)
        .map(|char_index| text.char_to_byte(char_index))
        .unwrap_or_else(|_| {
            let mut byte_offset = byte_offset.min(text.len_bytes());
            while byte_offset > 0 && text.try_byte_to_char(byte_offset).is_err() {
                byte_offset -= 1;
            }
            byte_offset
        })
}

pub(super) fn rope_byte_range_to_string(text: &Rope, byte_range: std::ops::Range<usize>) -> String {
    let byte_range = clamp_rope_byte_range(text, byte_range);
    let char_start = text.byte_to_char(byte_range.start);
    let char_end = text.byte_to_char(byte_range.end);
    text.slice(char_start..char_end).to_string()
}

pub(super) fn rope_line_covering_byte_range(
    text: &Rope,
    byte_range: std::ops::Range<usize>,
) -> std::ops::Range<usize> {
    let byte_range = clamp_rope_byte_range(text, byte_range);
    let len_bytes = text.len_bytes();
    if byte_range.start >= byte_range.end || len_bytes == 0 {
        return byte_range;
    }

    let start_line = text.byte_to_line(byte_range.start);
    let end_offset = byte_range
        .end
        .saturating_sub(1)
        .min(len_bytes.saturating_sub(1));
    let end_line = text.byte_to_line(end_offset);
    let start = text.line_to_byte(start_line);
    let end = if end_line + 1 < text.len_lines() {
        text.line_to_byte(end_line + 1)
    } else {
        len_bytes
    };
    start..end
}

pub(super) fn sql_protected_ranges_for_rope_overlay(
    text: &Rope,
    overlay_range: std::ops::Range<usize>,
) -> Vec<SqlProtectedRange> {
    if overlay_range.start >= overlay_range.end {
        return Vec::new();
    }

    let context_start = rope_overlay_context_start(text, overlay_range.clone());
    let context = rope_byte_range_to_string(text, context_start..overlay_range.end);
    sql_protected_ranges_with_unclosed_dollar_strings(&context, true)
        .into_iter()
        .filter_map(|range| {
            let absolute_start = context_start + range.start;
            let absolute_end = context_start + range.end;
            if absolute_end <= overlay_range.start || absolute_start >= overlay_range.end {
                return None;
            }

            Some(SqlProtectedRange {
                start: absolute_start.saturating_sub(overlay_range.start),
                end: absolute_end.min(overlay_range.end) - overlay_range.start,
                kind: range.kind,
            })
        })
        .collect()
}

pub(super) fn clip_highlights_to_range(
    highlights: &mut Vec<super::Highlight>,
    byte_range: std::ops::Range<usize>,
) {
    for highlight in highlights.iter_mut() {
        highlight.start = highlight.start.max(byte_range.start);
        highlight.end = highlight.end.min(byte_range.end);
    }
    highlights.retain(|highlight| highlight.start < highlight.end);
}

pub(super) fn rope_overlay_context_start(
    text: &Rope,
    overlay_range: std::ops::Range<usize>,
) -> usize {
    let raw_start = overlay_range
        .end
        .saturating_sub(ROPE_OVERLAY_CONTEXT_BYTES)
        .min(overlay_range.start);
    let byte_start = rope_overlay_dollar_context_start(text, overlay_range.start)
        .unwrap_or_else(|| clamp_rope_byte_to_char_boundary(text, raw_start));
    let line = text.byte_to_line(byte_start);
    text.line_to_byte(line)
}

fn rope_overlay_dollar_context_start(text: &Rope, overlay_start: usize) -> Option<usize> {
    if overlay_start == 0 {
        return None;
    }

    let lookbehind_start = clamp_rope_byte_to_char_boundary(
        text,
        overlay_start.saturating_sub(ROPE_DOLLAR_STRING_LOOKBEHIND_BYTES),
    );
    let lookbehind = rope_byte_range_to_string(text, lookbehind_start..overlay_start);
    unclosed_dollar_quoted_string_start(&lookbehind).map(|start| lookbehind_start + start)
}
