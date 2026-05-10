use crate::{ActiveSnippet, Selection, SelectionsCollection, Snippet, TextBuffer, TextDocument};

#[derive(Default)]
pub(crate) struct EditorSnippetState {
    active: Option<ActiveSnippet>,
}

impl EditorSnippetState {
    pub(crate) fn clear(&mut self) {
        self.active = None;
    }

    pub(crate) fn retain_valid_for_document(&mut self, document: &TextDocument) {
        self.active = self.active.take().and_then(|mut snippet| {
            snippet
                .invalidate_if_stale(document.buffer())
                .then_some(snippet)
        });
    }

    pub(crate) fn reconcile_with_primary_selection(
        &mut self,
        document: &TextDocument,
        selections: &SelectionsCollection,
        has_secondary_cursors: bool,
    ) {
        self.active = self.active.take().and_then(|mut snippet| {
            if has_secondary_cursors
                || !Self::active_snippet_matches_primary_selection(
                    document,
                    selections,
                    &mut snippet,
                )
            {
                return None;
            }

            Some(snippet)
        });
    }

    pub(crate) fn activate_snippet(
        &mut self,
        snippet: &Snippet,
        buffer: &TextBuffer,
        trigger_offset: usize,
    ) -> Option<Selection> {
        self.active = ActiveSnippet::new(snippet, buffer, trigger_offset);
        self.active
            .as_ref()
            .and_then(|active| active.current_selection(buffer))
    }

    pub(crate) fn advance_placeholder(&mut self, buffer: &TextBuffer) -> Option<Selection> {
        let active = self.active.as_mut()?;
        if !active.invalidate_if_stale(buffer) {
            self.active = None;
            return None;
        }

        let selection = active
            .advance(buffer)
            .and_then(|_| active.current_selection(buffer));
        if selection.is_none() {
            self.active = None;
        }
        selection
    }

    pub(crate) fn active_snippet_matches_primary_selection(
        document: &TextDocument,
        selections: &SelectionsCollection,
        snippet: &mut ActiveSnippet,
    ) -> bool {
        if !snippet.invalidate_if_stale(document.buffer()) {
            return false;
        }

        let Some(primary) = selections.primary() else {
            return false;
        };

        snippet
            .current_selection(document.buffer())
            .is_some_and(|selection| primary.selection == selection)
    }
}

#[cfg(test)]
mod tests {
    use super::EditorSnippetState;
    use crate::{
        ActiveSnippet, Cursor, DocumentIdentity, Selection, SelectionsCollection, Snippet,
        TextDocument,
    };

    #[test]
    fn snippet_state_reconciles_against_primary_selection() {
        let snippet = Snippet::parse("${1:column}");
        let document = TextDocument::with_text(
            DocumentIdentity::internal().expect("internal document uri"),
            snippet.text.clone(),
        );
        let mut active_snippet =
            ActiveSnippet::new(&snippet, document.buffer(), 0).expect("active snippet");
        let primary_selection = active_snippet
            .current_selection(document.buffer())
            .expect("current selection");
        let selections = SelectionsCollection::single(Cursor::new(), primary_selection);

        assert!(
            EditorSnippetState::active_snippet_matches_primary_selection(
                &document,
                &selections,
                &mut active_snippet,
            )
        );

        let selections = SelectionsCollection::single(Cursor::new(), Selection::new());
        assert!(
            !EditorSnippetState::active_snippet_matches_primary_selection(
                &document,
                &selections,
                &mut active_snippet,
            )
        );
    }

    #[test]
    fn advance_clears_after_last_placeholder() {
        let snippet = Snippet::parse("${1:column}");
        let buffer = crate::TextBuffer::new(&snippet.text);
        let mut state = EditorSnippetState::default();

        assert!(state.activate_snippet(&snippet, &buffer, 0).is_some());
        assert!(state.advance_placeholder(&buffer).is_none());
        assert!(state.advance_placeholder(&buffer).is_none());
    }
}
