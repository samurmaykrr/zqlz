//! A small MySQL-aware SQL scanner: finds bind placeholders and the leading
//! keywords of a statement without being fooled by strings or comments.
//!
//! The generic lexer in `zqlz_core` follows the ANSI rules, which differ from
//! MySQL's in the ways that matter here:
//!
//! - `'...'` and `"..."` strings honour backslash escapes (`'it\'s'`), so the
//!   quote after a backslash does not end the string;
//! - `#` starts a line comment, and `--` only does when whitespace (or the end
//!   of the text) follows it - `5--3` is arithmetic;
//! - `/*! ... */` and `/*M! ... */` are *executable* version comments, so a
//!   placeholder inside one is real SQL, while any other `/* ... */` (including
//!   `/*+ ... */` optimizer hints) is a comment.
//!
//! ponytail: backslash escapes are always assumed on. A session running with
//! `NO_BACKSLASH_ESCAPES` would need the flag passed in; add it when someone
//! actually runs that mode (the literal renderer would need the same switch).

/// A bind placeholder found outside strings, comments and quoted identifiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Placeholder {
    pub start: usize,
    pub end: usize,
    pub kind: PlaceholderKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PlaceholderKind {
    /// `?`
    Question,
    /// `$1`, `$2`, ... (1-based)
    Dollar(usize),
}

fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte >= 0x80
}

/// End of a quoted run that starts after the opening quote at `from`.
/// `backslash` enables `\x` escapes; a doubled quote is always an escape.
fn skip_quoted(bytes: &[u8], from: usize, quote: u8, backslash: bool) -> usize {
    let mut index = from;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' if backslash => index += 2,
            byte if byte == quote => {
                if bytes.get(index + 1) == Some(&quote) {
                    index += 2;
                } else {
                    return index + 1;
                }
            }
            _ => index += 1,
        }
    }
    bytes.len()
}

fn skip_line(bytes: &[u8], from: usize) -> usize {
    bytes[from.min(bytes.len())..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map_or(bytes.len(), |offset| from + offset + 1)
}

/// `--` opens a comment only when followed by whitespace, a control character,
/// or the end of the text.
fn is_dash_comment(bytes: &[u8], index: usize) -> bool {
    bytes[index] == b'-'
        && bytes.get(index + 1) == Some(&b'-')
        && bytes.get(index + 2).is_none_or(|byte| *byte <= b' ')
}

/// If a block comment opens at `index`, return where scanning resumes: past the
/// opener for an executable `/*!` or `/*M!` comment (its body is SQL), past the
/// closer for an ordinary one.
fn skip_block_comment(bytes: &[u8], index: usize) -> Option<usize> {
    if bytes[index] != b'/' || bytes.get(index + 1) != Some(&b'*') {
        return None;
    }
    match (bytes.get(index + 2), bytes.get(index + 3)) {
        (Some(b'!'), _) => return Some(index + 3),
        (Some(b'M'), Some(b'!')) => return Some(index + 4),
        _ => {}
    }
    let mut cursor = index + 2;
    while cursor + 1 < bytes.len() {
        if bytes[cursor] == b'*' && bytes[cursor + 1] == b'/' {
            return Some(cursor + 2);
        }
        cursor += 1;
    }
    Some(bytes.len())
}

/// Where scanning resumes if a string, quoted identifier or comment starts at
/// `index`; `None` if the byte begins ordinary SQL text.
fn skip_non_code(bytes: &[u8], index: usize) -> Option<usize> {
    match bytes[index] {
        quote @ (b'\'' | b'"') => Some(skip_quoted(bytes, index + 1, quote, true)),
        b'`' => Some(skip_quoted(bytes, index + 1, b'`', false)),
        b'#' => Some(skip_line(bytes, index + 1)),
        b'-' if is_dash_comment(bytes, index) => Some(skip_line(bytes, index + 2)),
        b'/' => skip_block_comment(bytes, index),
        _ => None,
    }
}

/// Bind placeholders (`?` and `$N`) in source order.
pub(super) fn placeholders(sql: &str) -> Vec<Placeholder> {
    let bytes = sql.as_bytes();
    let mut found = Vec::new();
    let mut index = 0;

    while index < bytes.len() {
        if let Some(resume) = skip_non_code(bytes, index) {
            index = resume;
            continue;
        }

        match bytes[index] {
            b'?' => {
                found.push(Placeholder {
                    start: index,
                    end: index + 1,
                    kind: PlaceholderKind::Question,
                });
                index += 1;
            }
            b'$' if index == 0 || !is_identifier_byte(bytes[index - 1]) => {
                let digits_end = bytes[index + 1..]
                    .iter()
                    .position(|byte| !byte.is_ascii_digit())
                    .map_or(bytes.len(), |offset| index + 1 + offset);
                match sql[index + 1..digits_end].parse::<usize>() {
                    Ok(position) => {
                        found.push(Placeholder {
                            start: index,
                            end: digits_end,
                            kind: PlaceholderKind::Dollar(position),
                        });
                        index = digits_end;
                    }
                    Err(_) => index += 1,
                }
            }
            _ => index += 1,
        }
    }

    found
}

/// The first `limit` bare words of a statement, uppercased, skipping comments,
/// strings and punctuation.
pub(super) fn leading_words(sql: &str, limit: usize) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut words = Vec::new();
    let mut index = 0;

    while index < bytes.len() && words.len() < limit {
        if let Some(resume) = skip_non_code(bytes, index) {
            index = resume;
        } else if bytes[index].is_ascii_alphabetic() {
            let start = index;
            while index < bytes.len() && is_identifier_byte(bytes[index]) {
                index += 1;
            }
            words.push(sql[start..index].to_ascii_uppercase());
        } else {
            index += 1;
        }
    }

    words
}

/// `SHOW WARNINGS` / `SHOW ERRORS` read the diagnostics area, so reporting the
/// warnings they leave behind would just print the same list twice.
pub(super) fn is_show_diagnostics(sql: &str) -> bool {
    matches!(
        leading_words(sql, 2).as_slice(),
        [show, what] if show == "SHOW" && (what == "WARNINGS" || what == "ERRORS")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(sql: &str) -> Vec<PlaceholderKind> {
        placeholders(sql).into_iter().map(|p| p.kind).collect()
    }

    #[test]
    fn question_marks_in_code_are_placeholders() {
        assert_eq!(
            kinds("SELECT ?, ? FROM t WHERE a = ?"),
            vec![PlaceholderKind::Question; 3]
        );
    }

    #[test]
    fn backslash_escaped_quote_does_not_end_a_string() {
        // The `?` is inside the string: `'it\'s ? ok'` is one literal.
        assert!(kinds(r"SELECT 'it\'s ? ok'").is_empty());
        assert!(kinds(r#"SELECT "say \"?\" now""#).is_empty());
        // ...and a placeholder after the string is still found.
        assert_eq!(
            kinds(r"SELECT 'it\'s', ?"),
            vec![PlaceholderKind::Question]
        );
        // An escaped backslash does not escape the closing quote.
        assert_eq!(kinds(r"SELECT 'a\\', ?"), vec![PlaceholderKind::Question]);
    }

    #[test]
    fn doubled_quotes_stay_inside_the_string() {
        assert!(kinds("SELECT 'it''s ?'").is_empty());
        assert!(kinds("SELECT `we``ird ?`").is_empty());
    }

    #[test]
    fn backtick_identifiers_have_no_backslash_escapes() {
        assert_eq!(
            kinds(r"SELECT `a\` , ?"),
            vec![PlaceholderKind::Question],
            "a backslash is an ordinary character inside backticks"
        );
    }

    #[test]
    fn hash_and_dash_comments_hide_placeholders() {
        assert_eq!(kinds("SELECT 1 # why ?\n, ?"), vec![PlaceholderKind::Question]);
        assert_eq!(kinds("SELECT 1 -- why ?\n, ?"), vec![PlaceholderKind::Question]);
        assert_eq!(kinds("SELECT 1 --\t?\n, ?"), vec![PlaceholderKind::Question]);
    }

    #[test]
    fn double_dash_without_whitespace_is_not_a_comment() {
        // `5--?` is `5 - (-?)`: the placeholder is real SQL.
        assert_eq!(kinds("SELECT 5--?"), vec![PlaceholderKind::Question]);
        // `--` at the very end of the text is a comment.
        assert!(kinds("SELECT 1 --").is_empty());
    }

    #[test]
    fn block_comments_hide_placeholders_but_version_comments_do_not() {
        assert!(kinds("SELECT /* ? */ 1").is_empty());
        assert!(kinds("SELECT /*+ MAX_EXECUTION_TIME(?) */ 1").is_empty());
        assert_eq!(
            kinds("SELECT /*!50000 ? */ 1"),
            vec![PlaceholderKind::Question]
        );
        assert_eq!(kinds("SELECT /*M! ? */ 1"), vec![PlaceholderKind::Question]);
    }

    #[test]
    fn dollar_placeholders_need_a_boundary_and_digits() {
        assert_eq!(
            kinds("SELECT $2, $1"),
            vec![PlaceholderKind::Dollar(2), PlaceholderKind::Dollar(1)]
        );
        assert!(kinds("SELECT a$1, $x, '$1'").is_empty());
    }

    #[test]
    fn leading_words_skip_comments() {
        assert_eq!(
            leading_words("# hi\n/* c */ -- x\n  show   warnings;", 2),
            vec!["SHOW", "WARNINGS"]
        );
        assert_eq!(leading_words("'x' SELECT 1", 1), vec!["SELECT"]);
    }

    #[test]
    fn show_diagnostics_statements_are_recognised() {
        assert!(is_show_diagnostics("SHOW WARNINGS"));
        assert!(is_show_diagnostics("/* x */ show errors"));
        assert!(!is_show_diagnostics("SHOW TABLES"));
        assert!(!is_show_diagnostics("SELECT 'SHOW WARNINGS'"));
    }
}
