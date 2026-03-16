//! Replace functionality with regex capture group support.

use super::find::{FindError, FindOptions, ReplaceResult, SearchEngine};

/// Replace all occurrences of a pattern in text.
///
/// # Arguments
/// * `text` - The text to search in
/// * `pattern` - The search pattern (literal or regex depending on options)
/// * `replacement` - The replacement string (can contain $1, $2, etc. for capture groups)
/// * `options` - Search options
///
/// # Returns
/// A `ReplaceResult` containing the new text and replacement count.
///
/// # Errors
/// Returns an error if the regex pattern is invalid (when `options.regex` is true).
pub fn replace_all(
    text: &str,
    pattern: &str,
    replacement: &str,
    options: &FindOptions,
) -> Result<ReplaceResult, FindError> {
    if pattern.is_empty() {
        return Ok(ReplaceResult::new(text.to_string(), 0));
    }

    Ok(SearchEngine::new(pattern, options)?.replace_all(text, replacement))
}

/// Replace the first occurrence of a pattern in text.
pub fn replace_first(
    text: &str,
    pattern: &str,
    replacement: &str,
    options: &FindOptions,
) -> Result<ReplaceResult, FindError> {
    if pattern.is_empty() {
        return Ok(ReplaceResult::new(text.to_string(), 0));
    }

    Ok(SearchEngine::new(pattern, options)?.replace_first(text, replacement))
}

/// Replace the next occurrence of a pattern starting from a position.
pub fn replace_next(
    text: &str,
    pattern: &str,
    replacement: &str,
    options: &FindOptions,
    start_pos: usize,
) -> Result<ReplaceResult, FindError> {
    if pattern.is_empty() || start_pos >= text.len() {
        return Ok(ReplaceResult::new(text.to_string(), 0));
    }

    Ok(SearchEngine::new(pattern, options)?.replace_next(text, replacement, start_pos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace_all_simple() {
        let text = "hello world, hello universe";
        let options = FindOptions::default();
        let result = replace_all(text, "hello", "hi", &options).unwrap();

        assert_eq!(result.text, "hi world, hi universe");
        assert_eq!(result.count, 2);
    }

    #[test]
    fn test_replace_all_case_insensitive() {
        let text = "Hello HELLO hello";
        let options = FindOptions::new().case_sensitive(false);
        let result = replace_all(text, "hello", "hi", &options).unwrap();

        assert_eq!(result.text, "hi hi hi");
        assert_eq!(result.count, 3);
    }

    #[test]
    fn test_replace_all_regex_with_capture_groups() {
        let text = "col_name1, col_name2, col_name3";
        let options = FindOptions::new().regex(true);
        let result = replace_all(text, r"col_(\w+)", "column_$1", &options).unwrap();

        assert_eq!(result.text, "column_name1, column_name2, column_name3");
        assert_eq!(result.count, 3);
    }

    #[test]
    fn test_replace_first() {
        let text = "one two one three one";
        let options = FindOptions::default();
        let result = replace_first(text, "one", "1", &options).unwrap();

        assert_eq!(result.text, "1 two one three one");
        assert_eq!(result.count, 1);
    }

    #[test]
    fn test_replace_next() {
        let text = "one two one three one";
        let options = FindOptions::default();

        let result1 = replace_next(text, "one", "1", &options, 0).unwrap();
        assert_eq!(result1.text, "1 two one three one");

        let result2 = replace_next(text, "one", "1", &options, 4).unwrap();
        assert_eq!(result2.text, "one two 1 three one");
    }

    #[test]
    fn test_replace_next_regex_capture_group() {
        let text = "col_user col_order";
        let options = FindOptions::new().regex(true);
        let result = replace_next(text, r"col_(\w+)", "column_$1", &options, 0).unwrap();
        assert_eq!(result.text, "column_user col_order");
        assert_eq!(result.count, 1);
    }

    #[test]
    fn test_replace_empty_pattern() {
        let text = "some text";
        let options = FindOptions::default();
        let result = replace_all(text, "", "X", &options).unwrap();

        assert_eq!(result.text, "some text");
        assert_eq!(result.count, 0);
    }

    #[test]
    fn test_replace_no_match() {
        let text = "hello world";
        let options = FindOptions::default();
        let result = replace_all(text, "foo", "bar", &options).unwrap();

        assert_eq!(result.text, "hello world");
        assert_eq!(result.count, 0);
    }

    #[test]
    fn test_replace_with_dollar_sign() {
        let text = "price: 10";
        let options = FindOptions::default();
        let result = replace_all(text, "10", "$$20", &options).unwrap();

        assert_eq!(result.text, "price: $20");
        assert_eq!(result.count, 1);
    }

    #[test]
    fn test_replace_with_full_match_reference() {
        let text = "hello world";
        let options = FindOptions::new().regex(true);
        let result = replace_all(text, r"\w+", "[$&]", &options).unwrap();

        assert_eq!(result.text, "[hello] [world]");
        assert_eq!(result.count, 2);
    }

    #[test]
    fn test_replace_whole_word() {
        let text = "id userid user_id";
        let options = FindOptions::new().whole_word(true);
        let result = replace_all(text, "id", "ID", &options).unwrap();

        assert_eq!(result.text, "ID userid user_id");
        assert_eq!(result.count, 1);
    }

    #[test]
    fn test_replace_multiple_capture_groups() {
        let text = "2024-01-15, 2023-12-25";
        let options = FindOptions::new().regex(true);
        let result = replace_all(text, r"(\d{4})-(\d{2})-(\d{2})", "$2/$3/$1", &options).unwrap();

        assert_eq!(result.text, "01/15/2024, 12/25/2023");
        assert_eq!(result.count, 2);
    }

    #[test]
    fn test_replace_invalid_regex() {
        let text = "some text";
        let options = FindOptions::new().regex(true);
        let result = replace_all(text, "[invalid", "X", &options);

        assert!(result.is_err());
    }
}
