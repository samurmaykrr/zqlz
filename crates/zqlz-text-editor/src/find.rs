//! Find & Replace state management for the text editor.
//!
//! This module contains purely data-structure logic (no GPUI dependencies) so that
//! its unit tests can be compiled and run even when the GPUI proc-macro server is
//! not available or struggles with large files.
//!
//! The [`FindState`] struct is owned by `TextEditor` and driven by the keyboard
//! handler in `text_editor.rs`; the rendering code in `element.rs` reads it read-only
//! during prepaint to compute match highlight rectangles and paint the panel.

use crate::{
    TextBuffer,
    find_replace::{FindError as SearchError, FindOptions as SearchOptions, Match, SearchEngine},
};

/// Options that control how find matches are computed.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct FindOptions {
    /// Match regardless of letter case when false (default)
    pub case_sensitive: bool,
    /// Match only complete words (surrounded by non-word characters)
    pub whole_word: bool,
    /// Treat the query string as a regular expression
    pub use_regex: bool,
}

/// A contiguous byte range in the buffer that matches the search query.
pub type FindMatch = Match;

/// State of the active find (and optionally replace) panel.
///
/// When `find_state` is `Some` on `TextEditor`, the panel is visible. The cursor
/// navigates through `matches`; each match is highlighted in the editor.
#[derive(Clone, Debug)]
pub struct FindState {
    /// The text the user has entered in the search box
    pub query: String,
    /// Precomputed list of all byte-range matches in the buffer
    pub matches: Vec<FindMatch>,
    /// Index into `matches` that is currently "selected" (the primary highlight)
    pub current_match: usize,
    /// Search configuration
    pub options: FindOptions,
    /// Whether the find+replace panel is open (vs. find-only)
    pub show_replace: bool,
    /// The replacement text (only relevant when `show_replace` is true)
    pub replace_query: String,
    /// Which field has keyboard focus: true = search box, false = replace box
    pub search_field_focused: bool,
    /// Limit matches to a specific byte range in the buffer (feat-031).
    /// When `None`, the entire buffer is searched.
    pub selection_boundary: Option<(usize, usize)>,
    /// If `Some`, contains the most recent regex compile error so the UI
    /// can display an error indicator without crashing.
    pub regex_error: Option<String>,

    /// True when search navigation should prefer earlier matches.
    pub search_backward: bool,
}

impl FindState {
    /// Create a new `FindState`, optionally with the replace field showing.
    pub fn new(show_replace: bool) -> Self {
        Self {
            query: String::new(),
            matches: Vec::new(),
            current_match: 0,
            options: FindOptions::default(),
            show_replace,
            replace_query: String::new(),
            search_field_focused: true,
            selection_boundary: None,
            regex_error: None,
            search_backward: false,
        }
    }

    /// Search `text` for all occurrences of `query` and store the results.
    ///
    /// Supports both plain-text and regex modes controlled by `options.use_regex`.
    /// When `selection_boundary` is set, only matches within that byte range are
    /// kept. `current_match` is clamped to a valid index after recomputation.
    pub fn recompute_matches(&mut self, text: &str) {
        if let Some((start, end)) = self.selection_boundary {
            let range_start = start.min(text.len());
            let range_end = end.min(text.len());
            self.recompute_matches_in_range(&text[range_start..range_end], range_start);
        } else {
            self.recompute_matches_in_range(text, 0);
        }
    }

    pub fn recompute_matches_in_buffer(&mut self, buffer: &TextBuffer) {
        self.matches.clear();
        self.regex_error = None;

        if self.query.is_empty() {
            self.current_match = 0;
            return;
        }

        if let Some((start, end)) = self.selection_boundary {
            let Ok(search_text) = buffer.text_for_range(start..end) else {
                self.current_match = 0;
                return;
            };
            self.recompute_matches_in_range(&search_text, start);
        } else {
            self.recompute_matches_in_rope(buffer);
        }
    }

    fn recompute_matches_in_rope(&mut self, buffer: &TextBuffer) {
        let options = SearchOptions {
            case_sensitive: self.options.case_sensitive,
            whole_word: self.options.whole_word,
            regex: self.options.use_regex,
        };

        match SearchEngine::new(&self.query, &options) {
            Ok(engine) => {
                self.matches = engine.find_all_in_rope(&buffer.rope());
            }
            Err(SearchError::InvalidRegex(error)) => {
                self.regex_error = Some(error);
                return;
            }
        }

        if self.current_match >= self.matches.len() && !self.matches.is_empty() {
            self.current_match = 0;
        }
    }

    fn recompute_matches_in_range(&mut self, text: &str, base_offset: usize) {
        self.matches.clear();
        self.regex_error = None;

        if self.query.is_empty() {
            self.current_match = 0;
            return;
        }

        let options = SearchOptions {
            case_sensitive: self.options.case_sensitive,
            whole_word: self.options.whole_word,
            regex: self.options.use_regex,
        };
        match SearchEngine::new(&self.query, &options) {
            Ok(engine) => {
                self.matches = engine
                    .find_all(text)
                    .into_iter()
                    .map(|found_match| {
                        FindMatch::new(
                            base_offset + found_match.start,
                            base_offset + found_match.end,
                            found_match.text,
                        )
                    })
                    .collect();
            }
            Err(SearchError::InvalidRegex(error)) => {
                self.regex_error = Some(error);
                return;
            }
        }

        if self.current_match >= self.matches.len() && !self.matches.is_empty() {
            self.current_match = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_state_basic_match() {
        let mut state = FindState::new(false);
        state.query = "hello".to_string();
        state.recompute_matches("say hello world hello");
        assert_eq!(state.matches.len(), 2);
        assert_eq!(state.matches[0].start, 4);
        assert_eq!(state.matches[0].end, 9);
        assert_eq!(state.matches[1].start, 16);
        assert_eq!(state.matches[1].end, 21);
    }

    #[test]
    fn test_find_state_case_insensitive_by_default() {
        let mut state = FindState::new(false);
        state.query = "SELECT".to_string();
        state.recompute_matches("select SELect SELECT");
        // All three occurrences match when case_sensitive = false
        assert_eq!(state.matches.len(), 3);
    }

    #[test]
    fn test_find_state_case_insensitive_unicode() {
        let mut state = FindState::new(false);
        state.query = "é".to_string();
        state.recompute_matches("é É");
        assert_eq!(state.matches.len(), 2);
        assert_eq!(state.matches[0].start, 0);
        assert_eq!(state.matches[1].start, 3);
    }

    #[test]
    fn test_find_state_case_sensitive() {
        let mut state = FindState::new(false);
        state.options.case_sensitive = true;
        state.query = "SELECT".to_string();
        state.recompute_matches("select SELect SELECT");
        // Only exact case match
        assert_eq!(state.matches.len(), 1);
        assert_eq!(state.matches[0].start, 14);
    }

    #[test]
    fn test_find_state_no_match() {
        let mut state = FindState::new(false);
        state.query = "xyz".to_string();
        state.recompute_matches("hello world");
        assert_eq!(state.matches.len(), 0);
    }

    #[test]
    fn test_find_state_empty_query() {
        let mut state = FindState::new(false);
        state.query = String::new();
        state.recompute_matches("hello world");
        assert_eq!(state.matches.len(), 0);
    }

    #[test]
    fn test_find_state_whole_word() {
        let mut state = FindState::new(false);
        state.options.whole_word = true;
        state.options.case_sensitive = true;
        state.query = "the".to_string();
        state.recompute_matches("the theme the");
        // "the" at start, "theme" (not matched), "the" at end
        assert_eq!(state.matches.len(), 2);
    }

    #[test]
    fn test_find_state_current_match_wraps_on_smaller_len() {
        let mut state = FindState::new(false);
        state.query = "a".to_string();
        state.recompute_matches("aaa");
        assert_eq!(state.matches.len(), 3);
        state.current_match = 2;
        // Recompute with a shorter text that yields only 1 match
        state.recompute_matches("a");
        // current_match should clamp to 0
        assert_eq!(state.current_match, 0);
    }

    #[test]
    fn test_find_state_replace_query_field() {
        let state = FindState::new(true);
        assert!(state.show_replace);
        assert!(state.replace_query.is_empty());
        assert!(state.search_field_focused); // default focus is search field
    }

    // ─── Regex mode (feat-030) ──────────────────────────────────────────────

    #[test]
    fn test_find_state_regex_basic() {
        let mut state = FindState::new(false);
        state.options.use_regex = true;
        state.query = r"\d+".to_string();
        state.recompute_matches("abc 123 def 456");
        assert_eq!(state.matches.len(), 2);
        assert_eq!(state.matches[0].start, 4);
        assert_eq!(state.matches[0].end, 7);
    }

    #[test]
    fn test_find_state_regex_case_insensitive() {
        let mut state = FindState::new(false);
        state.options.use_regex = true;
        // case_sensitive defaults to false, so (?i) prefix is added
        state.query = "select".to_string();
        state.recompute_matches("SELECT select");
        assert_eq!(state.matches.len(), 2);
    }

    #[test]
    fn test_find_state_regex_invalid_reports_error() {
        let mut state = FindState::new(false);
        state.options.use_regex = true;
        state.query = "[invalid".to_string();
        state.recompute_matches("some text");
        assert!(state.regex_error.is_some());
        assert!(state.matches.is_empty());
    }

    // ─── Search in selection (feat-031) ─────────────────────────────────────

    #[test]
    fn test_find_state_selection_boundary() {
        let mut state = FindState::new(false);
        state.options.case_sensitive = true;
        state.query = "x".to_string();
        // "xxxxx xx  xxx": bytes 4..8 cover "x xx" — x at 4, space at 5, x at 6, x at 7
        state.selection_boundary = Some((4, 8));
        state.recompute_matches("xxxxx xx  xxx");
        // Only matches within [4, 8) are kept: offsets 4, 6, 7
        for m in &state.matches {
            assert!(
                m.start >= 4 && m.end <= 8,
                "match outside boundary: {:?}",
                m
            );
        }
        assert_eq!(state.matches.len(), 3);
    }

    #[test]
    fn test_find_state_uses_shared_search_engine_whole_word_semantics() {
        let mut state = FindState::new(false);
        state.query = "id".to_string();
        state.options.whole_word = true;
        state.recompute_matches("id userid id");

        assert_eq!(
            state.matches,
            vec![
                FindMatch::new(0, 2, "id".to_string()),
                FindMatch::new(10, 12, "id".to_string())
            ]
        );
    }

    #[test]
    fn test_find_state_recomputes_matches_in_buffer_without_materializing_whole_text() {
        let mut state = FindState::new(false);
        state.query = "needle".to_string();
        let mut text = String::from("prefix ");
        text.push_str(&"x".repeat(1024));
        text.push_str(" needle suffix needle");
        let buffer = TextBuffer::new(text);

        state.recompute_matches_in_buffer(&buffer);

        assert_eq!(state.matches.len(), 2);
        assert_eq!(state.matches[0].text, "needle");
        assert_eq!(state.matches[1].text, "needle");
    }
}
