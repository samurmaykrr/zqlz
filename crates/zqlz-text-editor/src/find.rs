//! Find & Replace state management for the text editor.
//!
//! This module contains purely data-structure logic (no GPUI dependencies) so that
//! its unit tests can be compiled and run even when the GPUI proc-macro server is
//! not available or struggles with large files.
//!
//! The [`FindState`] struct is owned by `TextEditor` and driven by the keyboard
//! handler in `text_editor.rs`; the rendering code in `element.rs` reads it read-only
//! during prepaint to compute match highlight rectangles and paint the panel.

/// Options that control how find matches are computed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FindOptions {
    /// Match regardless of letter case when false (default)
    pub case_sensitive: bool,
    /// Match only complete words (surrounded by non-word characters)
    pub whole_word: bool,
    /// Treat the query string as a regular expression
    pub use_regex: bool,
}

impl Default for FindOptions {
    fn default() -> Self {
        Self {
            case_sensitive: false,
            whole_word: false,
            use_regex: false,
        }
    }
}

/// A contiguous byte range in the buffer that matches the search query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FindMatch {
    /// Start byte offset (inclusive)
    pub start: usize,
    /// End byte offset (exclusive)
    pub end: usize,
}

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
        }
    }

    /// Search `text` for all occurrences of `query` and store the results.
    ///
    /// Supports both plain-text and regex modes controlled by `options.use_regex`.
    /// When `selection_boundary` is set, only matches within that byte range are
    /// kept. `current_match` is clamped to a valid index after recomputation.
    pub fn recompute_matches(&mut self, text: &str) {
        self.matches.clear();
        self.regex_error = None;

        if self.query.is_empty() {
            self.current_match = 0;
            return;
        }

        let search_range = self
            .selection_boundary
            .map(|(start, end)| start..end)
            .unwrap_or(0..text.len());

        // Clamp range to valid UTF-8 boundaries
        let range_start = search_range.start.min(text.len());
        let range_end = search_range.end.min(text.len());
        let search_text = &text[range_start..range_end];

        if self.options.use_regex {
            self.recompute_matches_regex(text, search_text, range_start);
        } else {
            self.recompute_matches_plain(text, search_text, range_start);
        }

        if self.current_match >= self.matches.len() && !self.matches.is_empty() {
            self.current_match = 0;
        }
    }

    fn recompute_matches_regex(&mut self, full_text: &str, search_text: &str, offset: usize) {
        let pattern = if self.options.case_sensitive {
            self.query.clone()
        } else {
            format!("(?i){}", self.query)
        };

        let regex = match regex::Regex::new(&pattern) {
            Ok(r) => r,
            Err(e) => {
                self.regex_error = Some(e.to_string());
                return;
            }
        };

        for mat in regex.find_iter(search_text) {
            let start = offset + mat.start();
            let end = offset + mat.end();

            if self.options.whole_word && !is_whole_word(full_text, start, end) {
                continue;
            }

            self.matches.push(FindMatch { start, end });
        }
    }

    fn recompute_matches_plain(&mut self, source_text: &str, search_text: &str, offset: usize) {
        let needle_bytes;
        let haystack_bytes;

        if self.options.case_sensitive {
            needle_bytes = self.query.as_bytes().to_vec();
            haystack_bytes = search_text.as_bytes().to_vec();
        } else {
            needle_bytes = self.query.to_lowercase().into_bytes();
            haystack_bytes = search_text.to_lowercase().into_bytes();
        };

        let mut pos = 0;
        while pos + needle_bytes.len() <= haystack_bytes.len() {
            if haystack_bytes[pos..].starts_with(&needle_bytes) {
                let start = offset + pos;
                let end = offset + pos + needle_bytes.len();

                if !self.options.whole_word || is_whole_word(source_text, start, end) {
                    self.matches.push(FindMatch { start, end });
                }

                pos += needle_bytes.len().max(1);
            } else {
                pos += 1;
            }
        }
    }
}

/// Returns true if the byte range `[start, end)` in `text` is surrounded by
/// non-word characters (or is at the document boundary).
fn is_whole_word(text: &str, start: usize, end: usize) -> bool {
    let before_ok = start == 0
        || !text[..start]
            .chars()
            .last()
            .map(|c| c.is_alphanumeric() || c == '_')
            .unwrap_or(false);
    let after_ok = end >= text.len()
        || !text[end..]
            .chars()
            .next()
            .map(|c| c.is_alphanumeric() || c == '_')
            .unwrap_or(false);
    before_ok && after_ok
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
}
