//! Find functionality with regex support for SQL text.
//!
//! Provides text search with support for:
//! - Case-sensitive/insensitive matching
//! - Whole word matching
//! - Regular expression patterns

use regex::{Regex, RegexBuilder};
use std::ops::Range;

/// Options for find operations.
#[derive(Debug, Clone, Default)]
pub struct FindOptions {
    /// Whether the search is case-sensitive.
    pub case_sensitive: bool,
    /// Whether to match whole words only.
    pub whole_word: bool,
    /// Whether the pattern is a regular expression.
    pub regex: bool,
}

impl FindOptions {
    /// Create new find options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set case sensitivity.
    pub fn case_sensitive(mut self, value: bool) -> Self {
        self.case_sensitive = value;
        self
    }

    /// Set whole word matching.
    pub fn whole_word(mut self, value: bool) -> Self {
        self.whole_word = value;
        self
    }

    /// Set regex mode.
    pub fn regex(mut self, value: bool) -> Self {
        self.regex = value;
        self
    }
}

/// A match found in text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    /// Byte offset of the start of the match.
    pub start: usize,
    /// Byte offset of the end of the match (exclusive).
    pub end: usize,
    /// The matched text.
    pub text: String,
}

impl Match {
    /// Create a new match.
    pub fn new(start: usize, end: usize, text: String) -> Self {
        Self { start, end, text }
    }

    /// Get the byte range of this match.
    pub fn range(&self) -> Range<usize> {
        self.start..self.end
    }

    /// Get the length of the match in bytes.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if the match is empty.
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

/// Find all matches of a pattern in text.
///
/// # Arguments
/// * `text` - The text to search in
/// * `pattern` - The search pattern (literal or regex depending on options)
/// * `options` - Search options
///
/// # Returns
/// A vector of all matches found, in order of occurrence.
///
/// # Errors
/// Returns an error if the regex pattern is invalid (when `options.regex` is true).
pub fn find_all(text: &str, pattern: &str, options: &FindOptions) -> Result<Vec<Match>, FindError> {
    if pattern.is_empty() {
        return Ok(Vec::new());
    }

    let regex = build_regex(pattern, options)?;
    let matches = regex
        .find_iter(text)
        .map(|m| Match::new(m.start(), m.end(), m.as_str().to_string()))
        .collect();

    Ok(matches)
}

/// Find the first match of a pattern in text.
pub fn find_first(
    text: &str,
    pattern: &str,
    options: &FindOptions,
) -> Result<Option<Match>, FindError> {
    if pattern.is_empty() {
        return Ok(None);
    }

    let regex = build_regex(pattern, options)?;
    Ok(regex
        .find(text)
        .map(|m| Match::new(m.start(), m.end(), m.as_str().to_string())))
}

/// Find the next match after a given position.
pub fn find_next(
    text: &str,
    pattern: &str,
    options: &FindOptions,
    start_pos: usize,
) -> Result<Option<Match>, FindError> {
    if pattern.is_empty() || start_pos >= text.len() {
        return Ok(None);
    }

    let regex = build_regex(pattern, options)?;
    let search_text = &text[start_pos..];

    Ok(regex.find(search_text).map(|m| {
        Match::new(
            start_pos + m.start(),
            start_pos + m.end(),
            m.as_str().to_string(),
        )
    }))
}

/// Count the number of matches in text.
pub fn count_matches(text: &str, pattern: &str, options: &FindOptions) -> Result<usize, FindError> {
    if pattern.is_empty() {
        return Ok(0);
    }

    let regex = build_regex(pattern, options)?;
    Ok(regex.find_iter(text).count())
}

/// Build a regex from a pattern and options.
pub(crate) fn build_regex(pattern: &str, options: &FindOptions) -> Result<Regex, FindError> {
    let pattern = if options.regex {
        if options.whole_word {
            format!(r"\b(?:{})\b", pattern)
        } else {
            pattern.to_string()
        }
    } else {
        let escaped = regex::escape(pattern);
        if options.whole_word {
            format!(r"\b{}\b", escaped)
        } else {
            escaped
        }
    };

    RegexBuilder::new(&pattern)
        .case_insensitive(!options.case_sensitive)
        .build()
        .map_err(|e| FindError::InvalidRegex(e.to_string()))
}

/// Errors that can occur during find operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FindError {
    /// Invalid regular expression pattern.
    #[error("Invalid regex pattern: {0}")]
    InvalidRegex(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_all_case_insensitive() {
        let text = "SELECT id FROM users WHERE ID = 1";
        let options = FindOptions::new().case_sensitive(false);
        let matches = find_all(text, "id", &options).unwrap();

        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].text, "id");
        assert_eq!(matches[0].start, 7);
        assert_eq!(matches[1].text, "ID");
        assert_eq!(matches[1].start, 27);
    }

    #[test]
    fn test_find_all_case_sensitive() {
        let text = "SELECT id FROM users WHERE ID = 1";
        let options = FindOptions::new().case_sensitive(true);
        let matches = find_all(text, "id", &options).unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].text, "id");
        assert_eq!(matches[0].start, 7);
    }

    #[test]
    fn test_find_all_regex() {
        let text = "SELECT col1, col2, col3 FROM table1";
        let options = FindOptions::new().regex(true);
        let matches = find_all(text, r"col\d", &options).unwrap();

        assert_eq!(matches.len(), 3);
        assert_eq!(matches[0].text, "col1");
        assert_eq!(matches[1].text, "col2");
        assert_eq!(matches[2].text, "col3");
    }

    #[test]
    fn test_whole_word_matching() {
        let text = "SELECT userid, user_id, id FROM users";
        let options = FindOptions::new().whole_word(true);
        let matches = find_all(text, "id", &options).unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].text, "id");
        assert_eq!(matches[0].start, 24);
    }

    #[test]
    fn test_find_first() {
        let text = "one two one three one";
        let options = FindOptions::new();
        let result = find_first(text, "one", &options).unwrap();

        assert!(result.is_some());
        let m = result.unwrap();
        assert_eq!(m.start, 0);
        assert_eq!(m.text, "one");
    }

    #[test]
    fn test_find_next() {
        let text = "one two one three one";
        let options = FindOptions::new();

        let first = find_first(text, "one", &options).unwrap().unwrap();
        assert_eq!(first.start, 0);

        let second = find_next(text, "one", &options, first.end)
            .unwrap()
            .unwrap();
        assert_eq!(second.start, 8);

        let third = find_next(text, "one", &options, second.end)
            .unwrap()
            .unwrap();
        assert_eq!(third.start, 18);

        let fourth = find_next(text, "one", &options, third.end).unwrap();
        assert!(fourth.is_none());
    }

    #[test]
    fn test_count_matches() {
        let text = "SELECT * FROM users WHERE name = 'test' AND email LIKE '%test%'";
        let options = FindOptions::new().case_sensitive(false);
        let count = count_matches(text, "test", &options).unwrap();

        assert_eq!(count, 2);
    }

    #[test]
    fn test_empty_pattern() {
        let text = "some text";
        let options = FindOptions::new();

        let matches = find_all(text, "", &options).unwrap();
        assert!(matches.is_empty());

        let first = find_first(text, "", &options).unwrap();
        assert!(first.is_none());

        let count = count_matches(text, "", &options).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_invalid_regex() {
        let text = "some text";
        let options = FindOptions::new().regex(true);
        let result = find_all(text, "[invalid", &options);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), FindError::InvalidRegex(_)));
    }

    #[test]
    fn test_match_range() {
        let text = "hello world";
        let options = FindOptions::new();
        let matches = find_all(text, "world", &options).unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].range(), 6..11);
        assert_eq!(matches[0].len(), 5);
        assert!(!matches[0].is_empty());
    }

    #[test]
    fn test_special_regex_characters_escaped() {
        let text = "price is $10.00 (USD)";
        let options = FindOptions::new().regex(false);
        let matches = find_all(text, "$10.00", &options).unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].text, "$10.00");
    }

    #[test]
    fn test_whole_word_with_regex() {
        let text = "col col1 col2 column";
        let options = FindOptions::new().regex(true).whole_word(true);
        let matches = find_all(text, "col", &options).unwrap();

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].text, "col");
        assert_eq!(matches[0].start, 0);
    }
}
