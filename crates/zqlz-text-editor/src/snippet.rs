use crate::{
    Selection,
    buffer::{Anchor, AnchoredRange, Bias, Position, TextBuffer},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnippetPlaceholder {
    pub index: usize,
    pub default_text: String,
    pub start: usize,
    pub end: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snippet {
    pub text: String,
    pub placeholders: Vec<SnippetPlaceholder>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveSnippet {
    pub placeholders: Vec<ActiveSnippetPlaceholder>,
    pub active_index: usize,
    pub insertion_anchor: Anchor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveSnippetPlaceholder {
    pub id: SnippetPlaceholderId,
    pub index: usize,
    pub default_text: String,
    pub range: AnchoredRange,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SnippetPlaceholderId {
    pub tab_stop: usize,
    pub ordinal: usize,
}

impl Snippet {
    pub fn parse(input: &str) -> Self {
        let mut text = String::new();
        let mut placeholders = Vec::new();
        let mut chars = input.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '$' && chars.peek() == Some(&'{') {
                chars.next();
                let mut body = String::new();
                for next in chars.by_ref() {
                    if next == '}' {
                        break;
                    }
                    body.push(next);
                }

                let mut parts = body.splitn(2, ':');
                let index = parts
                    .next()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(0);
                let default_text = parts.next().unwrap_or_default().to_string();
                let start = text.len();
                text.push_str(&default_text);
                let end = text.len();
                placeholders.push(SnippetPlaceholder {
                    index,
                    default_text,
                    start,
                    end,
                });
            } else {
                text.push(ch);
            }
        }

        placeholders.sort_by_key(|placeholder| placeholder.index);
        Self { text, placeholders }
    }
}

impl ActiveSnippet {
    pub fn new(snippet: &Snippet, buffer: &TextBuffer, insertion_offset: usize) -> Option<Self> {
        if snippet.placeholders.is_empty() {
            return None;
        }

        let insertion_anchor = buffer.anchor_at(insertion_offset, Bias::Left).ok()?;

        let placeholders = snippet
            .placeholders
            .iter()
            .enumerate()
            .map(|(ordinal, placeholder)| {
                let start = insertion_offset + placeholder.start;
                let end = insertion_offset + placeholder.end;
                Some(ActiveSnippetPlaceholder {
                    id: SnippetPlaceholderId {
                        tab_stop: placeholder.index,
                        ordinal,
                    },
                    index: placeholder.index,
                    default_text: placeholder.default_text.clone(),
                    range: buffer
                        .anchored_range(start..end, Bias::Left, Bias::Right)
                        .ok()?,
                })
            })
            .collect::<Option<Vec<_>>>()?;

        Some(Self {
            placeholders,
            active_index: 0,
            insertion_anchor,
        })
    }

    pub fn current_placeholder(&self) -> Option<&ActiveSnippetPlaceholder> {
        self.placeholders.get(self.active_index)
    }

    pub fn current_range(&self, buffer: &TextBuffer) -> Option<(usize, usize)> {
        let placeholder = self.current_placeholder()?;
        let range = buffer.resolve_anchored_range(placeholder.range).ok()?;
        Some((range.start, range.end))
    }

    pub fn advance(&mut self, buffer: &TextBuffer) -> Option<(usize, usize)> {
        if self.active_index + 1 >= self.placeholders.len() {
            return None;
        }

        self.active_index += 1;
        self.current_range(buffer)
    }

    pub fn current_positions(&self, buffer: &TextBuffer) -> Option<(Position, Position)> {
        let placeholder = self.current_placeholder()?;
        let range = buffer
            .resolve_anchored_position_range(placeholder.range)
            .ok()?;
        Some((range.start, range.end))
    }

    pub fn current_selection(&self, buffer: &TextBuffer) -> Option<Selection> {
        let placeholder = self.current_placeholder()?;
        let range = buffer
            .resolve_anchored_position_range(placeholder.range)
            .ok()?;
        Some(Selection::from_anchored_range(placeholder.range, range))
    }

    pub fn current_placeholder_id(&self) -> Option<&SnippetPlaceholderId> {
        self.current_placeholder()
            .map(|placeholder| &placeholder.id)
    }

    pub fn activate_placeholder(
        &mut self,
        placeholder_id: &SnippetPlaceholderId,
        buffer: &TextBuffer,
    ) -> Option<Selection> {
        let next_index = self
            .placeholders
            .iter()
            .position(|placeholder| &placeholder.id == placeholder_id)?;
        self.active_index = next_index;
        self.current_selection(buffer)
    }

    pub fn invalidate_if_stale(&mut self, buffer: &TextBuffer) -> bool {
        self.normalize(buffer)
            && self
                .current_placeholder()
                .and_then(|placeholder| buffer.resolve_anchored_range(placeholder.range).ok())
                .is_some()
    }

    pub fn normalize(&mut self, buffer: &TextBuffer) -> bool {
        let mut normalized = Vec::with_capacity(self.placeholders.len());
        for placeholder in &self.placeholders {
            let Ok(range) = buffer.rebase_anchored_range(placeholder.range) else {
                return false;
            };
            normalized.push(ActiveSnippetPlaceholder {
                id: placeholder.id.clone(),
                index: placeholder.index,
                default_text: placeholder.default_text.clone(),
                range,
            });
        }
        let Ok(insertion_anchor) = buffer.rebase_anchor(self.insertion_anchor) else {
            return false;
        };
        self.placeholders = normalized;
        self.insertion_anchor = insertion_anchor;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_snippet_with_placeholders() {
        let snippet = Snippet::parse("SELECT ${1:column} FROM ${2:table}");

        assert_eq!(snippet.text, "SELECT column FROM table");
        assert_eq!(snippet.placeholders.len(), 2);
        assert_eq!(snippet.placeholders[0].index, 1);
        assert_eq!(snippet.placeholders[0].default_text, "column");
        assert_eq!(snippet.placeholders[1].index, 2);
        assert_eq!(snippet.placeholders[1].default_text, "table");
    }

    #[test]
    fn test_active_snippet_advance_orders_by_tab_stop() {
        let snippet = Snippet::parse("${2:table} ${1:column}");
        let buffer = TextBuffer::new(&snippet.text);
        let mut active = ActiveSnippet::new(&snippet, &buffer, 0).expect("active snippet");

        assert_eq!(active.current_range(&buffer), Some((6, 12)));
        assert_eq!(active.advance(&buffer), Some((0, 5)));
        assert_eq!(active.advance(&buffer), None);
    }

    #[test]
    fn test_active_snippet_rebases_later_placeholders_after_edit() {
        let snippet = Snippet::parse("${1:column} ${2:table}");
        let mut buffer = TextBuffer::new(&snippet.text);
        let mut active = ActiveSnippet::new(&snippet, &buffer, 0).expect("active snippet");

        buffer
            .insert(3, "wide_")
            .expect("insert inside first placeholder");
        assert!(active.normalize(&buffer));

        assert_eq!(active.current_range(&buffer), Some((0, 11)));
        assert_eq!(active.advance(&buffer), Some((12, 17)));
    }

    #[test]
    fn test_active_snippet_placeholder_identity_survives_rebasing() {
        let snippet = Snippet::parse("${1:column} ${2:table}");
        let mut buffer = TextBuffer::new(&snippet.text);
        let mut active = ActiveSnippet::new(&snippet, &buffer, 0).expect("active snippet");
        let next_placeholder_id = active.placeholders[1].id.clone();

        buffer
            .insert(2, "ide_")
            .expect("insert inside first placeholder");
        assert!(active.invalidate_if_stale(&buffer));

        let selection = active
            .activate_placeholder(&next_placeholder_id, &buffer)
            .expect("activate rebased placeholder by identity");
        let range = selection.range();
        assert_eq!(range.start, Position::new(0, 11));
        assert_eq!(range.end, Position::new(0, 16));
    }
}
