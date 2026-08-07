use gpui::{Entity, Subscription};

use crate::{
    FindMatch, FindOptions, FindState, TextDocument, find_replace::FindOptions as TextFindOptions,
    find_replace_panel,
};

#[derive(Clone, Debug)]
pub struct FindSnapshot {
    pub matches: Vec<FindMatch>,
    pub current_match: usize,
}

pub(crate) struct FindReplacementRequest {
    query: String,
    replacement: String,
    options: TextFindOptions,
}

impl FindReplacementRequest {
    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    pub(crate) fn replacement(&self) -> &str {
        &self.replacement
    }

    pub(crate) fn options(&self) -> &TextFindOptions {
        &self.options
    }
}

pub(crate) struct CurrentFindReplacementRequest {
    start: usize,
    end: usize,
    replacement: FindReplacementRequest,
}

impl CurrentFindReplacementRequest {
    pub(crate) fn range(&self) -> std::ops::Range<usize> {
        self.start..self.end
    }

    pub(crate) fn start(&self) -> usize {
        self.start
    }

    pub(crate) fn replacement(&self) -> &FindReplacementRequest {
        &self.replacement
    }
}

pub(crate) struct EditorFindState {
    state: Option<FindState>,
    panel: Option<Entity<find_replace_panel::FindReplacePanel>>,
    panel_subscriptions: Vec<Subscription>,
    search_wrap_enabled: bool,
    smartcase_search_enabled: bool,
}

impl Default for EditorFindState {
    fn default() -> Self {
        Self {
            state: None,
            panel: None,
            panel_subscriptions: Vec::new(),
            search_wrap_enabled: true,
            smartcase_search_enabled: true,
        }
    }
}

impl EditorFindState {
    pub(crate) fn ensure_state(&mut self, show_replace: bool) -> &mut FindState {
        self.state
            .get_or_insert_with(|| FindState::new(show_replace))
    }

    pub(crate) fn clear_state(&mut self) {
        self.state = None;
    }

    pub(crate) fn panel(&self) -> Option<&Entity<find_replace_panel::FindReplacePanel>> {
        self.panel.as_ref()
    }

    pub(crate) fn set_panel(
        &mut self,
        panel: Entity<find_replace_panel::FindReplacePanel>,
        subscriptions: Vec<Subscription>,
    ) {
        self.panel = Some(panel);
        self.panel_subscriptions = subscriptions;
    }

    pub(crate) fn clear(&mut self) {
        self.state = None;
        self.panel = None;
        self.panel_subscriptions.clear();
    }

    pub(crate) fn is_open(&self) -> bool {
        self.state.is_some()
    }

    pub(crate) fn snapshot(&self) -> Option<FindSnapshot> {
        self.state.as_ref().map(|state| FindSnapshot {
            matches: state.matches.clone(),
            current_match: state.current_match,
        })
    }

    pub(crate) fn match_info(&self) -> Option<(usize, usize, Option<String>)> {
        self.state.as_ref().map(|state| {
            let total = state.matches.len();
            let current = if total > 0 {
                state.current_match.min(total - 1) + 1
            } else {
                0
            };
            (total, current, state.regex_error.clone())
        })
    }

    pub(crate) fn set_query_from_panel(
        &mut self,
        query: String,
        options: FindOptions,
        document: &TextDocument,
    ) {
        let smartcase_search_enabled = self.smartcase_search_enabled;
        if let Some(state) = self.state.as_mut() {
            state.query = query;
            state.options = options;
            Self::apply_smartcase_if_enabled(state, smartcase_search_enabled);
            state.recompute_matches_in_buffer(document.buffer());
        }
    }

    pub(crate) fn set_initial_query(&mut self, query: String, document: &TextDocument) {
        let smartcase_search_enabled = self.smartcase_search_enabled;
        if let Some(state) = self.state.as_mut() {
            state.query = query;
            Self::apply_smartcase_if_enabled(state, smartcase_search_enabled);
            state.recompute_matches_in_buffer(document.buffer());
        }
    }

    pub(crate) fn set_replace_query(&mut self, replacement: String) {
        if let Some(state) = self.state.as_mut() {
            state.replace_query = replacement;
        }
    }

    pub(crate) fn input_char(&mut self, ch: char, document: &TextDocument) -> bool {
        let smartcase_search_enabled = self.smartcase_search_enabled;
        let Some(state) = self.state.as_mut() else {
            return false;
        };
        if state.search_field_focused {
            state.query.push(ch);
            Self::apply_smartcase_if_enabled(state, smartcase_search_enabled);
            state.recompute_matches_in_buffer(document.buffer());
            true
        } else {
            state.replace_query.push(ch);
            false
        }
    }

    pub(crate) fn input_backspace(&mut self, document: &TextDocument) -> bool {
        let smartcase_search_enabled = self.smartcase_search_enabled;
        let Some(state) = self.state.as_mut() else {
            return false;
        };
        if state.search_field_focused {
            state.query.pop();
            Self::apply_smartcase_if_enabled(state, smartcase_search_enabled);
            state.recompute_matches_in_buffer(document.buffer());
            true
        } else {
            state.replace_query.pop();
            false
        }
    }

    pub(crate) fn toggle_case_sensitive(&mut self, document: &TextDocument) -> bool {
        self.toggle_option(document, |options| {
            options.case_sensitive = !options.case_sensitive;
        })
    }

    pub(crate) fn toggle_whole_word(&mut self, document: &TextDocument) -> bool {
        self.toggle_option(document, |options| {
            options.whole_word = !options.whole_word;
        })
    }

    pub(crate) fn toggle_regex(&mut self, document: &TextDocument) -> bool {
        self.toggle_option(document, |options| {
            options.use_regex = !options.use_regex;
        })
    }

    pub(crate) fn toggle_field_focus(&mut self) -> bool {
        let Some(state) = self.state.as_mut() else {
            return false;
        };
        if !state.show_replace {
            return false;
        }
        state.search_field_focused = !state.search_field_focused;
        true
    }

    pub(crate) fn select_next_match(&mut self, cursor_offset: usize) -> Option<(usize, usize)> {
        let state = self.state.as_mut()?;
        if state.matches.is_empty() {
            return None;
        }
        state.search_backward = false;
        state.current_match = if let Some(next_match) = state
            .matches
            .iter()
            .position(|found| found.start > cursor_offset)
        {
            next_match
        } else if self.search_wrap_enabled {
            0
        } else {
            state.matches.len() - 1
        };
        Self::current_match_range(state)
    }

    pub(crate) fn select_previous_match(&mut self, cursor_offset: usize) -> Option<(usize, usize)> {
        let state = self.state.as_mut()?;
        if state.matches.is_empty() {
            return None;
        }
        state.search_backward = true;
        state.current_match = if let Some(previous_match) = state
            .matches
            .iter()
            .rposition(|found| found.start < cursor_offset)
        {
            previous_match
        } else if self.search_wrap_enabled {
            state.matches.len() - 1
        } else {
            0
        };
        Self::current_match_range(state)
    }

    pub(crate) fn select_nearest_match(&mut self, cursor_offset: usize) -> Option<(usize, usize)> {
        let state = self.state.as_mut()?;
        if state.matches.is_empty() {
            return None;
        }
        state.current_match = state
            .matches
            .iter()
            .position(|found| found.start >= cursor_offset)
            .unwrap_or(0);
        Self::current_match_range(state)
    }

    pub(crate) fn all_match_ranges(&self) -> Vec<(usize, usize)> {
        self.state
            .as_ref()
            .map(|state| {
                state
                    .matches
                    .iter()
                    .map(|found_match| (found_match.start, found_match.end))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn toggle_selection_boundary(
        &mut self,
        boundary: Option<(usize, usize)>,
        document: &TextDocument,
    ) -> bool {
        let Some(state) = self.state.as_mut() else {
            return false;
        };
        if state.selection_boundary.is_some() {
            state.selection_boundary = None;
        } else {
            state.selection_boundary = boundary;
        }
        state.recompute_matches_in_buffer(document.buffer());
        true
    }

    pub(crate) fn recompute_matches(&mut self, document: &TextDocument) {
        if let Some(state) = self.state.as_mut() {
            state.recompute_matches_in_buffer(document.buffer());
        }
    }

    pub(crate) fn recompute_after_replacement(
        &mut self,
        document: &TextDocument,
        next_offset: usize,
    ) -> Option<(usize, usize)> {
        let state = self.state.as_mut()?;
        state.recompute_matches_in_buffer(document.buffer());
        if state.matches.is_empty() {
            return None;
        }
        state.current_match = state
            .matches
            .iter()
            .position(|found| found.start >= next_offset)
            .unwrap_or(0);
        Self::current_match_range(state)
    }

    pub(crate) fn current_replacement_request(&self) -> Option<CurrentFindReplacementRequest> {
        let state = self.state.as_ref()?;
        if state.matches.is_empty() {
            return None;
        }
        let found_match = state.matches.get(state.current_match)?;
        Some(CurrentFindReplacementRequest {
            start: found_match.start,
            end: found_match.end,
            replacement: Self::replacement_request(state),
        })
    }

    pub(crate) fn all_replacement_matches(
        &self,
    ) -> Option<(FindReplacementRequest, Vec<FindMatch>)> {
        let state = self.state.as_ref()?;
        if state.matches.is_empty() {
            return None;
        }
        Some((Self::replacement_request(state), state.matches.clone()))
    }

    pub(crate) fn set_regex_error(&mut self, error: String) {
        if let Some(state) = self.state.as_mut() {
            state.regex_error = Some(error);
        }
    }

    fn current_match_range(state: &FindState) -> Option<(usize, usize)> {
        let found_match = state.matches.get(state.current_match)?;
        Some((found_match.start, found_match.end))
    }

    fn replacement_request(state: &FindState) -> FindReplacementRequest {
        FindReplacementRequest {
            query: state.query.clone(),
            replacement: state.replace_query.clone(),
            options: TextFindOptions {
                case_sensitive: state.options.case_sensitive,
                whole_word: state.options.whole_word,
                regex: state.options.use_regex,
                preserve_case: state.options.preserve_case,
            },
        }
    }

    fn toggle_option(
        &mut self,
        document: &TextDocument,
        toggle: impl FnOnce(&mut FindOptions),
    ) -> bool {
        let Some(state) = self.state.as_mut() else {
            return false;
        };
        toggle(&mut state.options);
        state.recompute_matches_in_buffer(document.buffer());
        true
    }

    fn apply_smartcase_if_enabled(state: &mut FindState, smartcase_search_enabled: bool) {
        if smartcase_search_enabled {
            state.options.case_sensitive = state.query.chars().any(char::is_uppercase);
        }
    }

    pub(crate) fn set_search_wrap_enabled(&mut self, enabled: bool) {
        self.search_wrap_enabled = enabled;
    }

    pub(crate) fn set_smartcase_search_enabled(&mut self, enabled: bool) {
        self.smartcase_search_enabled = enabled;
    }
}

#[cfg(test)]
mod tests {
    use super::EditorFindState;
    use crate::{DocumentIdentity, FindOptions, FindState, TextDocument};

    fn text_document(text: &str) -> TextDocument {
        TextDocument::with_text(
            DocumentIdentity::internal().expect("internal document"),
            text,
        )
    }

    #[test]
    fn default_find_state_is_closed_with_expected_settings() {
        let state = EditorFindState::default();

        assert!(!state.is_open());
        assert!(state.search_wrap_enabled);
        assert!(state.smartcase_search_enabled);
        assert!(state.snapshot().is_none());
    }

    #[test]
    fn ensure_state_reuses_existing_find_state() {
        let mut state = EditorFindState::default();

        state.ensure_state(false).query = "alpha".into();
        state.ensure_state(true);

        let find_state = state.state.as_ref().expect("find state");
        assert_eq!(find_state.query, "alpha");
        assert!(!find_state.show_replace);
    }

    #[test]
    fn clear_drops_find_state_and_panel_subscriptions() {
        let mut state = EditorFindState {
            state: Some(FindState::new(true)),
            ..Default::default()
        };
        state.clear();

        assert!(!state.is_open());
        assert!(state.panel.is_none());
        assert!(state.panel_subscriptions.is_empty());
    }

    #[test]
    fn settings_helpers_track_search_modes() {
        let mut state = EditorFindState::default();

        state.set_search_wrap_enabled(false);
        state.set_smartcase_search_enabled(false);

        assert!(!state.search_wrap_enabled);
        assert!(!state.smartcase_search_enabled);
    }

    #[test]
    fn clear_state_keeps_panel_lifecycle_intact() {
        let mut state = EditorFindState {
            state: Some(FindState::new(false)),
            ..Default::default()
        };

        state.clear_state();

        assert!(state.state.as_ref().is_none());
        assert!(state.panel().is_none());
    }

    #[test]
    fn query_helpers_recompute_matches_and_apply_smartcase() {
        let document = text_document("select SELECT");
        let mut state = EditorFindState {
            state: Some(FindState::new(false)),
            ..Default::default()
        };

        state.set_initial_query("SELECT".into(), &document);

        let find_state = state.state.as_ref().expect("find state");
        assert!(find_state.options.case_sensitive);
        assert_eq!(find_state.matches.len(), 1);
        assert_eq!(state.match_info(), Some((1, 1, None)));

        state.set_query_from_panel(
            "select".into(),
            FindOptions {
                case_sensitive: true,
                whole_word: false,
                use_regex: false,
                preserve_case: false,
            },
            &document,
        );
        let find_state = state.state.as_ref().expect("find state");
        assert!(!find_state.options.case_sensitive);
        assert_eq!(find_state.matches.len(), 2);
    }

    #[test]
    fn input_helpers_return_whether_search_matches_changed() {
        let document = text_document("abc abd");
        let mut state = EditorFindState {
            state: Some(FindState::new(true)),
            ..Default::default()
        };

        assert!(state.input_char('a', &document));
        assert_eq!(state.state.as_ref().expect("find state").matches.len(), 2);

        assert!(state.toggle_field_focus());
        assert!(!state.input_char('x', &document));
        assert_eq!(state.state.as_ref().expect("find state").replace_query, "x");

        assert!(!state.input_backspace(&document));
        assert_eq!(state.state.as_ref().expect("find state").replace_query, "");

        assert!(state.toggle_field_focus());
        assert!(state.input_backspace(&document));
        assert_eq!(state.state.as_ref().expect("find state").query, "");
    }

    #[test]
    fn toggle_helpers_update_options_and_recompute_matches() {
        let document = text_document("the theme the");
        let mut state = EditorFindState {
            state: Some(FindState::new(false)),
            smartcase_search_enabled: false,
            ..Default::default()
        };
        state.set_initial_query("the".into(), &document);
        assert_eq!(state.state.as_ref().expect("find state").matches.len(), 3);

        assert!(state.toggle_whole_word(&document));
        assert_eq!(state.state.as_ref().expect("find state").matches.len(), 2);

        assert!(state.toggle_case_sensitive(&document));
        assert!(
            state
                .state
                .as_ref()
                .expect("find state")
                .options
                .case_sensitive
        );

        assert!(state.toggle_regex(&document));
        assert!(state.state.as_ref().expect("find state").options.use_regex);
    }

    #[test]
    fn navigation_helpers_select_ranges_and_track_direction() {
        let document = text_document("one two one two");
        let mut state = EditorFindState {
            state: Some(FindState::new(false)),
            ..Default::default()
        };
        state.set_initial_query("two".into(), &document);

        assert_eq!(state.select_next_match(0), Some((4, 7)));
        assert!(!state.state.as_ref().expect("find state").search_backward);

        assert_eq!(state.select_next_match(7), Some((12, 15)));
        assert_eq!(state.select_next_match(15), Some((4, 7)));

        assert_eq!(state.select_previous_match(12), Some((4, 7)));
        assert!(state.state.as_ref().expect("find state").search_backward);

        assert_eq!(state.select_nearest_match(8), Some((12, 15)));
        assert_eq!(state.all_match_ranges(), vec![(4, 7), (12, 15)]);
    }

    #[test]
    fn navigation_helpers_respect_disabled_wrap() {
        let document = text_document("one two one two");
        let mut state = EditorFindState {
            state: Some(FindState::new(false)),
            search_wrap_enabled: false,
            ..Default::default()
        };
        state.set_initial_query("two".into(), &document);

        assert_eq!(state.select_next_match(15), Some((12, 15)));
        assert_eq!(state.select_previous_match(0), Some((4, 7)));
    }

    #[test]
    fn boundary_and_recompute_helpers_update_matches() {
        let document = text_document("one two one two");
        let mut state = EditorFindState {
            state: Some(FindState::new(false)),
            ..Default::default()
        };
        state.set_initial_query("one".into(), &document);
        assert_eq!(state.all_match_ranges(), vec![(0, 3), (8, 11)]);

        assert!(state.toggle_selection_boundary(Some((4, document.byte_len())), &document));
        assert_eq!(state.all_match_ranges(), vec![(8, 11)]);

        assert!(state.toggle_selection_boundary(None, &document));
        assert_eq!(state.all_match_ranges(), vec![(0, 3), (8, 11)]);
    }

    #[test]
    fn recompute_after_replacement_selects_next_match_at_or_after_offset() {
        let document = text_document("one one one");
        let mut state = EditorFindState {
            state: Some(FindState::new(false)),
            ..Default::default()
        };
        state.set_initial_query("one".into(), &document);

        assert_eq!(
            state.recompute_after_replacement(&document, 4),
            Some((4, 7))
        );
        assert_eq!(
            state.recompute_after_replacement(&document, 8),
            Some((8, 11))
        );
        assert_eq!(
            state.recompute_after_replacement(&document, 11),
            Some((0, 3))
        );
    }

    #[test]
    fn replacement_requests_snapshot_match_and_options() {
        let document = text_document("one two one");
        let mut state = EditorFindState {
            state: Some(FindState::new(true)),
            smartcase_search_enabled: false,
            ..Default::default()
        };
        state.set_initial_query("one".into(), &document);
        state.set_replace_query("three".into());
        assert!(state.toggle_case_sensitive(&document));

        let current = state
            .current_replacement_request()
            .expect("current request");
        assert_eq!(current.range(), 0..3);
        assert_eq!(current.replacement().query(), "one");
        assert_eq!(current.replacement().replacement(), "three");
        assert!(current.replacement().options().case_sensitive);

        let (request, matches) = state.all_replacement_matches().expect("all request");
        assert_eq!(request.replacement(), "three");
        assert_eq!(
            matches.iter().map(|m| (m.start, m.end)).collect::<Vec<_>>(),
            vec![(0, 3), (8, 11)]
        );

        state.set_regex_error("bad regex".into());
        assert_eq!(
            state
                .state
                .as_ref()
                .expect("find state")
                .regex_error
                .as_deref(),
            Some("bad regex")
        );
    }
}
