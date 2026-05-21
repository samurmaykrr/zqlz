use super::{Highlight, HighlightKind};

const HIGHLIGHT_KIND_COUNT: usize = 14;

pub fn highlight_render_rank(kind: HighlightKind) -> u8 {
    match kind {
        HighlightKind::Error => 0,
        HighlightKind::Comment => 1,
        HighlightKind::Function => 2,
        HighlightKind::Type => 3,
        HighlightKind::Keyword => 4,
        HighlightKind::Parameter => 5,
        HighlightKind::Identifier => 6,
        HighlightKind::Operator => 7,
        HighlightKind::String => 8,
        HighlightKind::Number => 9,
        HighlightKind::Boolean => 10,
        HighlightKind::Null => 11,
        HighlightKind::Punctuation => 12,
        HighlightKind::Default => 13,
    }
}

pub fn render_highlight_runs(text: &str, highlights: &[Highlight]) -> Vec<Highlight> {
    if text.is_empty() {
        return Vec::new();
    }

    let mut events = Vec::new();
    let mut boundaries = Vec::with_capacity(highlights.len() * 2 + 2);
    boundaries.push(0);
    boundaries.push(text.len());
    for highlight in highlights {
        let run_start = text.floor_char_boundary(highlight.start.min(text.len()));
        let run_end = text.ceil_char_boundary(highlight.end.min(text.len()));

        if run_start < run_end {
            events.push((run_start, highlight.kind, 1i8));
            events.push((run_end, highlight.kind, -1i8));
            boundaries.push(run_start);
            boundaries.push(run_end);
        }
    }

    if events.is_empty() {
        return Vec::new();
    }

    boundaries.sort_unstable();
    boundaries.dedup();
    events.sort_unstable_by_key(|(offset, kind, delta)| {
        (*offset, highlight_render_rank(*kind), *delta)
    });

    let mut result = Vec::with_capacity(boundaries.len().saturating_sub(1));
    let mut active_counts = [0usize; HIGHLIGHT_KIND_COUNT];
    let mut event_ix = 0usize;

    for boundary_ix in 0..boundaries.len().saturating_sub(1) {
        let start = boundaries[boundary_ix];
        while let Some((offset, kind, delta)) = events.get(event_ix).copied()
            && offset == start
        {
            let rank = highlight_render_rank(kind) as usize;
            if delta > 0 {
                active_counts[rank] += delta as usize;
            } else {
                active_counts[rank] = active_counts[rank].saturating_sub((-delta) as usize);
            }
            event_ix += 1;
        }

        let end = boundaries[boundary_ix + 1];
        if start >= end {
            continue;
        }

        let kind = active_counts
            .iter()
            .position(|count| *count > 0)
            .map(highlight_kind_for_render_rank)
            .unwrap_or(HighlightKind::Default);

        push_render_segment(&mut result, start, end, kind);
    }

    result
}

fn highlight_kind_for_render_rank(rank: usize) -> HighlightKind {
    match rank {
        0 => HighlightKind::Error,
        1 => HighlightKind::Comment,
        2 => HighlightKind::Function,
        3 => HighlightKind::Type,
        4 => HighlightKind::Keyword,
        5 => HighlightKind::Parameter,
        6 => HighlightKind::Identifier,
        7 => HighlightKind::Operator,
        8 => HighlightKind::String,
        9 => HighlightKind::Number,
        10 => HighlightKind::Boolean,
        11 => HighlightKind::Null,
        12 => HighlightKind::Punctuation,
        _ => HighlightKind::Default,
    }
}

fn push_render_segment(result: &mut Vec<Highlight>, start: usize, end: usize, kind: HighlightKind) {
    if let Some(previous) = result.last_mut()
        && previous.end == start
        && previous.kind == kind
    {
        previous.end = end;
        return;
    }

    result.push(Highlight { start, end, kind });
}
