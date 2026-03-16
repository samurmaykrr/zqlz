//! Find functionality with regex support for SQL text.
//!
//! Provides text search with support for:
//! - Case-sensitive/insensitive matching
//! - Whole word matching
//! - Regular expression patterns

use regex::{Regex, RegexBuilder};
use ropey::Rope;
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

    Ok(SearchEngine::new(pattern, options)?.find_all(text))
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

    Ok(SearchEngine::new(pattern, options)?.find_first(text))
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

    Ok(SearchEngine::new(pattern, options)?.find_next(text, start_pos))
}

/// Count the number of matches in text.
pub fn count_matches(text: &str, pattern: &str, options: &FindOptions) -> Result<usize, FindError> {
    if pattern.is_empty() {
        return Ok(0);
    }

    Ok(SearchEngine::new(pattern, options)?.count_matches(text))
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceResult {
    pub text: String,
    pub count: usize,
}

impl ReplaceResult {
    pub fn new(text: String, count: usize) -> Self {
        Self { text, count }
    }
}

/// Shared compiled search plan used by both find and replace helpers.
pub struct SearchEngine {
    regex: Regex,
}

impl SearchEngine {
    pub fn new(pattern: &str, options: &FindOptions) -> Result<Self, FindError> {
        Ok(Self {
            regex: build_regex(pattern, options)?,
        })
    }

    pub fn find_all(&self, text: &str) -> Vec<Match> {
        self.regex
            .find_iter(text)
            .map(|matched| Match::new(matched.start(), matched.end(), matched.as_str().to_string()))
            .collect()
    }

    pub fn find_all_in_rope(&self, text: &Rope) -> Vec<Match> {
        let mut matches = Vec::new();
        let mut cursor = 0usize;

        while cursor < text.len_bytes() {
            let (chunk, chunk_byte_index, _, _) = text.chunk_at_byte(cursor);
            let chunk_start = cursor.max(chunk_byte_index);
            let local_start = chunk_start - chunk_byte_index;
            let search_chunk = &chunk[local_start..];

            for matched in self.regex.find_iter(search_chunk) {
                let start = chunk_start + matched.start();
                let end = chunk_start + matched.end();
                matches.push(Match::new(start, end, matched.as_str().to_string()));
            }

            cursor = chunk_byte_index + chunk.len();
        }

        matches.sort_by_key(|matched| matched.start);
        matches.dedup_by(|left, right| left.start == right.start && left.end == right.end);
        matches
    }

    pub fn find_first(&self, text: &str) -> Option<Match> {
        self.regex
            .find(text)
            .map(|matched| Match::new(matched.start(), matched.end(), matched.as_str().to_string()))
    }

    pub fn find_next(&self, text: &str, start_pos: usize) -> Option<Match> {
        if start_pos >= text.len() {
            return None;
        }

        let safe_start = text.floor_char_boundary(start_pos);
        self.regex.find(&text[safe_start..]).map(|matched| {
            Match::new(
                safe_start + matched.start(),
                safe_start + matched.end(),
                matched.as_str().to_string(),
            )
        })
    }

    pub fn count_matches(&self, text: &str) -> usize {
        self.regex.find_iter(text).count()
    }

    pub fn replace_all(&self, text: &str, replacement: &str) -> ReplaceResult {
        let mut count = 0;
        let text = self
            .regex
            .replace_all(text, |captures: &regex::Captures| {
                count += 1;
                expand_replacement(replacement, captures)
            })
            .into_owned();
        ReplaceResult::new(text, count)
    }

    pub fn replace_first(&self, text: &str, replacement: &str) -> ReplaceResult {
        if let Some(matched) = self.regex.find(text)
            && let Some(captures) = self.regex.captures(text)
        {
            let expanded = expand_replacement(replacement, &captures);
            let result = format!(
                "{}{}{}",
                &text[..matched.start()],
                expanded,
                &text[matched.end()..]
            );
            return ReplaceResult::new(result, 1);
        }

        ReplaceResult::new(text.to_string(), 0)
    }

    pub fn replace_next(&self, text: &str, replacement: &str, start_pos: usize) -> ReplaceResult {
        if start_pos >= text.len() {
            return ReplaceResult::new(text.to_string(), 0);
        }

        let safe_start = text.floor_char_boundary(start_pos);
        let search_text = &text[safe_start..];
        if let Some(matched) = self.regex.find(search_text)
            && let Some(captures) = self.regex.captures(search_text)
        {
            let expanded = expand_replacement(replacement, &captures);
            let absolute_start = safe_start + matched.start();
            let absolute_end = safe_start + matched.end();
            let result = format!(
                "{}{}{}",
                &text[..absolute_start],
                expanded,
                &text[absolute_end..]
            );
            return ReplaceResult::new(result, 1);
        }

        ReplaceResult::new(text.to_string(), 0)
    }
}

pub(crate) fn expand_replacement(replacement: &str, caps: &regex::Captures) -> String {
    let mut result = String::with_capacity(replacement.len() * 2);
    let mut chars = replacement.chars().peekable();

    while let Some(character) = chars.next() {
        if character != '$' {
            result.push(character);
            continue;
        }

        match chars.peek().copied() {
            Some('$') => {
                result.push('$');
                chars.next();
            }
            Some('&') => {
                if let Some(matched) = caps.get(0) {
                    result.push_str(matched.as_str());
                }
                chars.next();
            }
            Some(next) if next.is_ascii_digit() => {
                let mut digits = String::new();
                while let Some(digit) = chars.peek().copied() {
                    if !digit.is_ascii_digit() {
                        break;
                    }
                    digits.push(digit);
                    chars.next();
                }
                if let Ok(index) = digits.parse::<usize>()
                    && let Some(matched) = caps.get(index)
                {
                    result.push_str(matched.as_str());
                }
            }
            _ => result.push(character),
        }
    }

    result
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

    #[test]
    fn search_engine_reuses_match_semantics_for_find_and_replace() {
        let text = "id userid id";
        let options = FindOptions::new().whole_word(true);
        let engine = SearchEngine::new("id", &options).expect("search engine");

        let matches = engine.find_all(text);
        let replaced = engine.replace_all(text, "ID");

        assert_eq!(
            matches
                .iter()
                .map(|matched| matched.range())
                .collect::<Vec<_>>(),
            vec![0..2, 10..12]
        );
        assert_eq!(replaced.text, "ID userid ID");
        assert_eq!(replaced.count, matches.len());
    }

    #[test]
    fn search_engine_find_all_in_rope_matches_find_all_for_chunked_text() {
        let engine = SearchEngine::new("needle", &FindOptions::default()).unwrap();
        let text = format!("{}needle{}needle", "a".repeat(2048), "b".repeat(1024));
        let rope = Rope::from_str(&text);

        assert_eq!(engine.find_all_in_rope(&rope), engine.find_all(&text));
    }
}
