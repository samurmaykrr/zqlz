#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlProtectedRangeKind {
    StringLiteral,
    QuotedIdentifier,
    Comment,
    DollarQuotedString,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlProtectedRange {
    pub start: usize,
    pub end: usize,
    pub kind: SqlProtectedRangeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlStatementSpan {
    pub start: usize,
    pub end: usize,
    pub line: usize,
    pub column: usize,
    pub end_line: usize,
    pub end_column: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlParameterPlaceholderKind {
    Named(String),
    DollarPositional(usize),
    QuestionMark,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlParameterPlaceholder {
    pub start: usize,
    pub end: usize,
    pub kind: SqlParameterPlaceholderKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlSymbolWord {
    pub start: usize,
    pub end: usize,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlDocumentSymbol {
    pub label: String,
    pub line: usize,
    pub column: usize,
    pub source_range: std::ops::Range<usize>,
    pub target_range: Option<std::ops::Range<usize>>,
}

#[derive(Debug, PartialEq, Eq)]
struct SqlDocumentSymbolLabel {
    label: String,
    target_range: Option<std::ops::Range<usize>>,
}

impl SqlProtectedRange {
    pub fn contains(&self, position: usize) -> bool {
        position >= self.start && position < self.end
    }
}

impl SqlSymbolWord {
    pub fn contains_inclusive(&self, position: usize) -> bool {
        position >= self.start && position <= self.end
    }
}

pub fn sql_protected_ranges(sql: &str) -> Vec<SqlProtectedRange> {
    sql_protected_ranges_with_unclosed_dollar_strings(sql, false)
}

pub fn sql_protected_ranges_with_unclosed_dollar_strings(
    sql: &str,
    include_unclosed_dollar_strings: bool,
) -> Vec<SqlProtectedRange> {
    let bytes = sql.as_bytes();
    let mut ranges = Vec::new();
    let mut index = 0usize;

    while index < bytes.len() {
        match bytes[index] {
            b'N' | b'n'
                if bytes.get(index + 1) == Some(&b'\'')
                    && !is_identifier_tail(bytes.get(index.wrapping_sub(1)).copied()) =>
            {
                let end = skip_single_quoted_sql_string(bytes, index + 2);
                ranges.push(SqlProtectedRange {
                    start: index,
                    end,
                    kind: SqlProtectedRangeKind::StringLiteral,
                });
                index = end;
            }
            b'\'' => {
                let end = skip_single_quoted_sql_string(bytes, index + 1);
                ranges.push(SqlProtectedRange {
                    start: index,
                    end,
                    kind: SqlProtectedRangeKind::StringLiteral,
                });
                index = end;
            }
            b'"' | b'`' => {
                let quote = bytes[index];
                let end = skip_quoted_identifier(bytes, index + 1, quote);
                ranges.push(SqlProtectedRange {
                    start: index,
                    end,
                    kind: SqlProtectedRangeKind::QuotedIdentifier,
                });
                index = end;
            }
            b'[' => {
                let end = skip_bracket_quoted_identifier(bytes, index + 1);
                ranges.push(SqlProtectedRange {
                    start: index,
                    end,
                    kind: SqlProtectedRangeKind::QuotedIdentifier,
                });
                index = end;
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                let end = skip_line_comment(bytes, index + 2);
                ranges.push(SqlProtectedRange {
                    start: index,
                    end,
                    kind: SqlProtectedRangeKind::Comment,
                });
                index = end;
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                let end = skip_block_comment(bytes, index + 2);
                ranges.push(SqlProtectedRange {
                    start: index,
                    end,
                    kind: SqlProtectedRangeKind::Comment,
                });
                index = end;
            }
            b'$' => {
                let Some(delimiter_end) = dollar_quote_delimiter_end(bytes, index) else {
                    index += 1;
                    continue;
                };
                let delimiter = &sql[index..delimiter_end];
                let body_start = delimiter_end;
                if let Some(relative_end) = sql[body_start..].find(delimiter) {
                    let end = body_start + relative_end + delimiter.len();
                    ranges.push(SqlProtectedRange {
                        start: index,
                        end,
                        kind: SqlProtectedRangeKind::DollarQuotedString,
                    });
                    index = end;
                } else if include_unclosed_dollar_strings {
                    ranges.push(SqlProtectedRange {
                        start: index,
                        end: sql.len(),
                        kind: SqlProtectedRangeKind::DollarQuotedString,
                    });
                    break;
                } else {
                    index = delimiter_end;
                }
            }
            _ => index += 1,
        }
    }

    ranges
}

pub fn unclosed_dollar_quoted_string_start(sql: &str) -> Option<usize> {
    sql_protected_ranges_with_unclosed_dollar_strings(sql, true)
        .into_iter()
        .rev()
        .find(|range| {
            range.kind == SqlProtectedRangeKind::DollarQuotedString && range.end == sql.len()
        })
        .map(|range| range.start)
}

pub fn is_position_in_sql_ranges(position: usize, ranges: &[SqlProtectedRange]) -> bool {
    ranges.iter().any(|range| range.contains(position))
}

pub fn sql_protected_range_at<'a>(
    position: usize,
    ranges: &'a [SqlProtectedRange],
    range_index: &mut usize,
) -> Option<&'a SqlProtectedRange> {
    while ranges
        .get(*range_index)
        .is_some_and(|range| range.end <= position)
    {
        *range_index += 1;
    }

    ranges
        .get(*range_index)
        .filter(|range| range.contains(position))
}

pub fn split_sql_statements(sql: &str) -> Vec<String> {
    split_sql_statement_spans(sql)
        .into_iter()
        .map(|span| sql[span.start..span.end].to_string())
        .collect()
}

pub fn sql_parameter_placeholders(sql: &str) -> Vec<SqlParameterPlaceholder> {
    let protected_ranges = sql_protected_ranges(sql);
    sql_parameter_placeholders_with_ranges(sql, &protected_ranges)
}

pub fn sql_parameter_placeholders_with_ranges(
    sql: &str,
    protected_ranges: &[SqlProtectedRange],
) -> Vec<SqlParameterPlaceholder> {
    let bytes = sql.as_bytes();
    let mut placeholders = Vec::new();
    let mut index = 0usize;
    let mut protected_range_index = 0usize;

    while index < bytes.len() {
        if let Some(protected_range) =
            sql_protected_range_at(index, protected_ranges, &mut protected_range_index)
        {
            index = protected_range.end;
            continue;
        }

        match bytes[index] {
            b':' if is_identifier_start(bytes.get(index + 1).copied()) => {
                if bytes.get(index.wrapping_sub(1)) == Some(&b':') {
                    index += 1;
                    continue;
                }
                let end = scan_identifier_tail(bytes, index + 2);
                placeholders.push(SqlParameterPlaceholder {
                    start: index,
                    end,
                    kind: SqlParameterPlaceholderKind::Named(sql[index + 1..end].to_string()),
                });
                index = end;
            }
            b'@' if is_identifier_start(bytes.get(index + 1).copied()) => {
                let end = scan_identifier_tail(bytes, index + 2);
                placeholders.push(SqlParameterPlaceholder {
                    start: index,
                    end,
                    kind: SqlParameterPlaceholderKind::Named(sql[index + 1..end].to_string()),
                });
                index = end;
            }
            b'$' if is_identifier_start(bytes.get(index + 1).copied()) => {
                let end = scan_identifier_tail(bytes, index + 2);
                placeholders.push(SqlParameterPlaceholder {
                    start: index,
                    end,
                    kind: SqlParameterPlaceholderKind::Named(sql[index + 1..end].to_string()),
                });
                index = end;
            }
            b'$' if bytes.get(index + 1).is_some_and(u8::is_ascii_digit) => {
                let end = scan_digits(bytes, index + 2);
                if let Ok(position) = sql[index + 1..end].parse::<usize>() {
                    placeholders.push(SqlParameterPlaceholder {
                        start: index,
                        end,
                        kind: SqlParameterPlaceholderKind::DollarPositional(position),
                    });
                }
                index = end;
            }
            b'?' => {
                placeholders.push(SqlParameterPlaceholder {
                    start: index,
                    end: index + 1,
                    kind: SqlParameterPlaceholderKind::QuestionMark,
                });
                index += 1;
            }
            _ => index += 1,
        }
    }

    placeholders
}

pub fn sql_symbol_words(sql: &str, max_words: usize) -> Vec<String> {
    sql_symbol_word_spans(sql, max_words)
        .into_iter()
        .map(|word| word.text)
        .collect()
}

pub fn sql_document_symbols(sql: &str) -> Vec<SqlDocumentSymbol> {
    split_sql_statement_spans(sql)
        .into_iter()
        .filter_map(|span| {
            let symbol = sql_document_symbol_label(&sql[span.start..span.end])?;
            Some(SqlDocumentSymbol {
                label: symbol.label,
                line: span.line,
                column: span.column,
                source_range: span.start..span.end,
                target_range: symbol
                    .target_range
                    .map(|range| span.start + range.start..span.start + range.end),
            })
        })
        .collect()
}

fn sql_document_symbol_label(statement: &str) -> Option<SqlDocumentSymbolLabel> {
    if statement.is_empty() {
        return None;
    }

    let words = sql_symbol_word_spans(statement, 6);
    let first = words.first()?;
    if first.text.is_empty() {
        return None;
    }

    let action = first.text.to_uppercase();
    let (label, target_range) = match action.as_str() {
        "SELECT" | "WITH" => ("Query".to_string(), None),
        "INSERT" | "UPDATE" | "DELETE" | "CREATE" | "ALTER" | "DROP" | "TRUNCATE" => {
            let target = words.iter().skip(1).find(|word| {
                !matches!(
                    word.text.to_ascii_uppercase().as_str(),
                    "CONCURRENTLY"
                        | "DATABASE"
                        | "EXISTS"
                        | "FUNCTION"
                        | "IF"
                        | "INDEX"
                        | "INTO"
                        | "MATERIALIZED"
                        | "NOT"
                        | "OR"
                        | "PROCEDURE"
                        | "REPLACE"
                        | "SCHEMA"
                        | "TABLE"
                        | "TEMP"
                        | "TEMPORARY"
                        | "TRIGGER"
                        | "UNIQUE"
                        | "VIEW"
                )
            });
            if let Some(target) = target.filter(|target| !target.text.is_empty()) {
                (
                    format!("{action} {}", target.text),
                    Some(target.start..target.end),
                )
            } else {
                (action, None)
            }
        }
        _ => (action, None),
    };

    Some(SqlDocumentSymbolLabel {
        label,
        target_range,
    })
}

pub fn sql_symbol_word_spans(sql: &str, max_words: usize) -> Vec<SqlSymbolWord> {
    let protected_ranges = sql_protected_ranges(sql);
    let mut protected_range_index = 0usize;
    let mut raw_words = Vec::new();
    let mut index = 0usize;

    while index < sql.len() && raw_words.len() < max_words.saturating_mul(3) {
        if let Some(protected_range) =
            sql_protected_range_at(index, &protected_ranges, &mut protected_range_index)
        {
            if protected_range.kind == SqlProtectedRangeKind::QuotedIdentifier {
                let token = unquote_identifier(&sql[protected_range.start..protected_range.end]);
                if !token.is_empty() {
                    raw_words.push(SqlSymbolWord {
                        start: protected_range.start,
                        end: protected_range.end,
                        text: token.to_string(),
                    });
                }
            }
            index = protected_range.end;
            continue;
        }

        let Some(character) = sql[index..].chars().next() else {
            break;
        };
        if character == '.' {
            raw_words.push(SqlSymbolWord {
                start: index,
                end: index + character.len_utf8(),
                text: ".".to_string(),
            });
            index += character.len_utf8();
            continue;
        }
        if !sql_symbol_word_char(character) {
            index += character.len_utf8();
            continue;
        }

        let start = index;
        index += character.len_utf8();
        while index < sql.len() {
            let Some(character) = sql[index..].chars().next() else {
                break;
            };
            if !sql_symbol_word_char(character) {
                break;
            }
            index += character.len_utf8();
        }
        let token = sql[start..index].trim_matches(',');
        if !token.is_empty() {
            raw_words.push(SqlSymbolWord {
                start,
                end: index,
                text: token.to_string(),
            });
        }
    }

    merge_qualified_symbol_word_spans(raw_words)
        .into_iter()
        .take(max_words)
        .collect()
}

pub fn sql_symbol_at_offset(sql: &str, offset: usize) -> Option<SqlSymbolWord> {
    sql_symbol_word_spans(sql, usize::MAX)
        .into_iter()
        .find(|symbol| symbol.contains_inclusive(offset))
        .and_then(|symbol| sql_symbol_segment_at_offset(sql, symbol, offset))
}

pub fn qualified_sql_reference_prefix(text_before: &str) -> Option<String> {
    let mut end = text_before.trim_end().len();
    if end == 0 {
        return None;
    }
    let protected_ranges = sql_protected_ranges(text_before);
    if protected_ranges.iter().any(|range| {
        range.start < end
            && end <= range.end
            && !(range.kind == SqlProtectedRangeKind::QuotedIdentifier && range.start + 1 == end)
    }) {
        return None;
    }
    if text_before[..end]
        .chars()
        .next_back()
        .is_some_and(|character| matches!(character, '"' | '`' | '['))
    {
        end = text_before[..end]
            .char_indices()
            .next_back()
            .map(|(index, _)| index)
            .unwrap_or(0);
    }

    let (cursor, has_trailing_dot) = trim_end_char(text_before, end, '.');
    let qualifier_cursor = if has_trailing_dot {
        cursor
    } else {
        let (_, current_segment_start) = previous_identifier_segment(text_before, cursor)?;
        let (cursor, has_dot_before_segment) =
            trim_end_char(text_before, current_segment_start, '.');
        if !has_dot_before_segment {
            return None;
        }
        cursor
    };
    let (qualifier, _) = previous_identifier_segment(text_before, qualifier_cursor)?;

    if qualifier.is_empty() {
        None
    } else {
        Some(qualifier)
    }
}

fn trim_end_char(source: &str, end: usize, expected: char) -> (usize, bool) {
    source[..end]
        .char_indices()
        .next_back()
        .filter(|(_, character)| *character == expected)
        .map(|(index, _)| (index, true))
        .unwrap_or((end, false))
}

fn previous_identifier_segment(source: &str, end: usize) -> Option<(String, usize)> {
    let (last_index, last_character) = source[..end].char_indices().next_back()?;
    match last_character {
        '"' | '`' | ']' => {
            let open_quote = if last_character == ']' {
                '['
            } else {
                last_character
            };
            let start = source[..last_index]
                .char_indices()
                .rev()
                .find(|(_, character)| *character == open_quote)
                .map(|(index, _)| index)?;
            let raw = &source[start..end];
            let identifier = unquote_identifier(raw);
            (!identifier.is_empty()).then(|| (identifier.to_string(), start))
        }
        character if sql_identifier_segment_char(character) => {
            let mut start = last_index;
            for (index, character) in source[..last_index].char_indices().rev() {
                if !sql_identifier_segment_char(character) {
                    break;
                }
                start = index;
            }
            let identifier = &source[start..end];
            (!identifier.is_empty()).then(|| (identifier.to_string(), start))
        }
        _ => None,
    }
}

pub fn qualified_sql_reference_at_offset(sql: &str, offset: usize) -> Option<(String, String)> {
    let symbol = sql_symbol_word_spans(sql, usize::MAX)
        .into_iter()
        .find(|symbol| symbol.contains_inclusive(offset))?;
    let segment = sql_symbol_segment_at_offset(sql, symbol.clone(), offset)?;
    let (qualifier, _) = symbol.text.rsplit_once('.')?;
    if qualifier.is_empty() {
        return None;
    }

    Some((qualifier.to_string(), segment.text))
}

pub fn sql_matching_symbol_segments(sql: &str, word: &str) -> Vec<(usize, usize)> {
    let mut matches = Vec::new();

    for symbol in sql_symbol_word_spans(sql, usize::MAX) {
        if !symbol.text.contains('.') {
            if symbol.text.eq_ignore_ascii_case(word)
                && let Some((start, end)) = trim_identifier_quotes(sql, symbol.start, symbol.end)
            {
                matches.push((start, end));
            }
            continue;
        }

        let source = match sql.get(symbol.start..symbol.end) {
            Some(source) => source,
            None => continue,
        };
        let mut segment_start = symbol.start;
        for relative_dot in source
            .match_indices('.')
            .map(|(relative_index, _)| relative_index)
            .chain(std::iter::once(source.len()))
        {
            let segment_end = symbol.start + relative_dot;
            if let Some((start, end)) = trim_identifier_quotes(sql, segment_start, segment_end)
                && start < end
                && sql[start..end].eq_ignore_ascii_case(word)
            {
                matches.push((start, end));
            }
            segment_start = segment_end + 1;
        }
    }

    matches
}

pub fn sql_matching_qualified_symbol_segments(
    sql: &str,
    qualifier: &str,
    word: &str,
) -> Vec<(usize, usize)> {
    let mut matches = Vec::new();

    for symbol in sql_symbol_word_spans(sql, usize::MAX) {
        if !symbol.text.contains('.') {
            continue;
        }

        let Some((symbol_qualifier, _)) = symbol.text.rsplit_once('.') else {
            continue;
        };
        if !symbol_qualifier.eq_ignore_ascii_case(qualifier) {
            continue;
        }

        let source = match sql.get(symbol.start..symbol.end) {
            Some(source) => source,
            None => continue,
        };
        let Some(last_dot) = source.rfind('.') else {
            continue;
        };
        let segment_start = symbol.start + last_dot + 1;
        if let Some((start, end)) = trim_identifier_quotes(sql, segment_start, symbol.end)
            && start < end
            && sql[start..end].eq_ignore_ascii_case(word)
        {
            matches.push((start, end));
        }
    }

    matches
}

fn sql_symbol_segment_at_offset(
    sql: &str,
    symbol: SqlSymbolWord,
    offset: usize,
) -> Option<SqlSymbolWord> {
    let source = sql.get(symbol.start..symbol.end)?;
    if !source.contains('.') {
        return Some(symbol);
    }

    let mut segment_start = symbol.start;
    for relative_dot in source
        .match_indices('.')
        .map(|(relative_index, _)| relative_index)
        .chain(std::iter::once(source.len()))
    {
        let segment_end = symbol.start + relative_dot;
        if offset >= segment_start && offset <= segment_end {
            let (start, end) = trim_identifier_quotes(sql, segment_start, segment_end)?;
            return (start < end).then(|| SqlSymbolWord {
                start,
                end,
                text: sql[start..end].to_string(),
            });
        }
        segment_start = segment_end + 1;
    }

    None
}

fn unquote_identifier(identifier: &str) -> &str {
    identifier.trim_matches(|character| matches!(character, '"' | '`' | '[' | ']'))
}

fn trim_identifier_quotes(sql: &str, mut start: usize, mut end: usize) -> Option<(usize, usize)> {
    while start < end {
        let character = sql.get(start..)?.chars().next()?;
        if matches!(character, '"' | '`' | '[') {
            start += character.len_utf8();
        } else {
            break;
        }
    }

    while start < end {
        let character = sql.get(..end)?.chars().next_back()?;
        if matches!(character, '"' | '`' | ']') {
            end -= character.len_utf8();
        } else {
            break;
        }
    }

    Some((start, end))
}

fn merge_qualified_symbol_word_spans(raw_words: Vec<SqlSymbolWord>) -> Vec<SqlSymbolWord> {
    let mut words: Vec<SqlSymbolWord> = Vec::new();
    let mut index = 0usize;

    while index < raw_words.len() {
        if raw_words[index].text == "." {
            if let (Some(previous), Some(next)) = (words.pop(), raw_words.get(index + 1)) {
                words.push(SqlSymbolWord {
                    start: previous.start,
                    end: next.end,
                    text: format!("{}.{}", previous.text, next.text),
                });
                index += 2;
                continue;
            }
            index += 1;
            continue;
        }

        words.push(raw_words[index].clone());
        index += 1;
    }

    words
}

fn sql_symbol_word_char(character: char) -> bool {
    character.is_alphanumeric() || matches!(character, '_' | '.' | '$')
}

fn sql_identifier_segment_char(character: char) -> bool {
    character.is_alphanumeric() || matches!(character, '_' | '$')
}

/// Object kinds whose bodies are compound statements.
const ROUTINE_OBJECT_KEYWORDS: [&str; 4] = ["FUNCTION", "PROCEDURE", "TRIGGER", "EVENT"];

/// Object kinds that end the search immediately: reaching one of these means the statement is
/// something else, and a later `FUNCTION`-like word would be an identifier rather than the
/// object kind (e.g. `CREATE TABLE t (function VARCHAR(10))`).
const NON_ROUTINE_OBJECT_KEYWORDS: [&str; 11] = [
    "TABLE",
    "VIEW",
    "MATERIALIZED",
    "INDEX",
    "DATABASE",
    "SCHEMA",
    "USER",
    "ROLE",
    "SEQUENCE",
    "TYPE",
    "EXTENSION",
];

/// Words between `CREATE` and the object kind are modifiers we deliberately do not parse:
/// `OR REPLACE`, `DEFINER=x` / `DEFINER = x`, `ALGORITHM=...`, `SQL SECURITY INVOKER`,
/// `AGGREGATE`, and so on. Skipping them by count keeps the matcher independent of how each
/// dialect spells its clauses.
const MAX_MODIFIER_WORDS: usize = 12;

/// True when `sql` starts a `CREATE ... FUNCTION|PROCEDURE|TRIGGER|EVENT` statement.
///
/// Such statements carry a compound body that neither the SQL parsers nor a naive `;` split
/// handle correctly, so both diagnostics and statement splitting need to recognize them.
/// Leading comments are skipped.
pub fn is_compound_routine_header(sql: &str) -> bool {
    let sql = sql.trim_start_matches(|character: char| character.is_whitespace());

    if let Some(rest) = sql.strip_prefix("--") {
        let Some((_, after_comment)) = rest.split_once('\n') else {
            return false;
        };
        return is_compound_routine_header(after_comment);
    }

    if let Some(rest) = sql.strip_prefix("/*") {
        let Some((_, after_comment)) = rest.split_once("*/") else {
            return false;
        };
        return is_compound_routine_header(after_comment);
    }

    let mut words = sql.split_whitespace();
    if !words
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("CREATE"))
    {
        return false;
    }

    for word in words.take(MAX_MODIFIER_WORDS) {
        if ROUTINE_OBJECT_KEYWORDS
            .iter()
            .any(|keyword| word.eq_ignore_ascii_case(keyword))
        {
            return true;
        }

        if NON_ROUTINE_OBJECT_KEYWORDS
            .iter()
            .any(|keyword| word.eq_ignore_ascii_case(keyword))
        {
            return false;
        }
    }

    false
}

/// Find where a routine's compound body ends, so it is not split on its internal semicolons.
///
/// `SHOW CREATE PROCEDURE` and friends return a body without the `DELIMITER` wrapper a client
/// would otherwise need to send it back, so splitting on `;` yields fragments the server
/// rejects (`CREATE ... BEGIN ... INSERT ...;` followed by a bare `END`).
///
/// Returns the offset just past the `END` closing the outermost block, or `None` when the
/// routine has no block at all — a single-statement body ends at its first semicolon like
/// any other statement.
fn routine_body_end(
    sql: &str,
    start: usize,
    protected_ranges: &[SqlProtectedRange],
) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut index = start;
    let mut protected_range_index = 0usize;
    let mut depth = 0usize;
    let mut saw_block = false;

    while index < sql.len() {
        if let Some(protected_range) =
            sql_protected_range_at(index, protected_ranges, &mut protected_range_index)
        {
            index = protected_range.end;
            continue;
        }

        if is_identifier_start(bytes.get(index).copied()) {
            let word_start = index;
            index += 1;
            while is_identifier_tail(bytes.get(index).copied()) {
                index += 1;
            }
            let word = &sql[word_start..index];

            if word.eq_ignore_ascii_case("BEGIN") || word.eq_ignore_ascii_case("CASE") {
                depth += 1;
                saw_block = true;
            } else if word.eq_ignore_ascii_case("END") {
                // `END IF`, `END LOOP`, `END WHILE` and `END REPEAT` close constructs whose
                // openers were not counted, because those keywords also appear in expressions
                // (`IF(a, b, c)`) where they open nothing. A bare `END` and `END CASE` do
                // close a counted block; the trailing `CASE` is consumed so the next pass does
                // not read it as a fresh opener.
                match next_word(sql, index, protected_ranges) {
                    Some((next, next_end))
                        if next.eq_ignore_ascii_case("IF")
                            || next.eq_ignore_ascii_case("LOOP")
                            || next.eq_ignore_ascii_case("WHILE")
                            || next.eq_ignore_ascii_case("REPEAT") =>
                    {
                        index = next_end;
                        continue;
                    }
                    Some((next, next_end)) if next.eq_ignore_ascii_case("CASE") => {
                        index = next_end;
                    }
                    _ => {}
                }

                depth = depth.saturating_sub(1);
                if saw_block && depth == 0 {
                    return Some(index);
                }
            }
            continue;
        }

        if bytes[index] == b';' && !saw_block {
            return None;
        }

        index += 1;
    }

    saw_block.then_some(sql.len())
}

/// The next identifier-like word at or after `from`, with the offset just past it, skipping
/// whitespace and protected text.
fn next_word<'a>(
    sql: &'a str,
    from: usize,
    protected_ranges: &[SqlProtectedRange],
) -> Option<(&'a str, usize)> {
    let bytes = sql.as_bytes();
    let mut index = from;
    let mut protected_range_index = 0usize;

    while index < sql.len() {
        if let Some(protected_range) =
            sql_protected_range_at(index, protected_ranges, &mut protected_range_index)
        {
            index = protected_range.end;
            continue;
        }

        if bytes[index].is_ascii_whitespace() {
            index += 1;
            continue;
        }

        if !is_identifier_start(bytes.get(index).copied()) {
            return None;
        }

        let word_start = index;
        index += 1;
        while is_identifier_tail(bytes.get(index).copied()) {
            index += 1;
        }
        return Some((&sql[word_start..index], index));
    }

    None
}

pub fn split_sql_statement_spans(sql: &str) -> Vec<SqlStatementSpan> {
    let mut statements = Vec::new();
    let protected_ranges = sql_protected_ranges(sql);
    let mut protected_range_index = 0usize;
    let mut statement_start = None::<(usize, usize, usize)>;
    let mut statement_end = None::<(usize, usize, usize)>;
    let mut index = 0usize;
    let mut line = 0usize;
    let mut column = 0usize;

    while index < sql.len() {
        if let Some(protected_range) =
            sql_protected_range_at(index, &protected_ranges, &mut protected_range_index)
        {
            let protected_text = &sql[protected_range.start..protected_range.end];
            if statement_start.is_none() && !protected_text.trim().is_empty() {
                statement_start = Some((protected_range.start, line, column));
            }
            advance_text_position(protected_text, &mut line, &mut column);
            if !protected_text.trim().is_empty() {
                statement_end = Some((protected_range.end, line, column));
            }
            index = protected_range.end;
            continue;
        }

        let Some(character) = sql[index..].chars().next() else {
            break;
        };

        if character == ';' {
            if let (Some((start, start_line, start_column)), Some((end, end_line, end_column))) =
                (statement_start.take(), statement_end.take())
                && start < end
            {
                statements.push(SqlStatementSpan {
                    start,
                    end,
                    line: start_line,
                    column: start_column,
                    end_line,
                    end_column,
                });
            }
            index += character.len_utf8();
            advance_position(character, &mut line, &mut column);
            continue;
        }

        if statement_start.is_none() && !character.is_whitespace() {
            // A routine keeps its body intact rather than breaking at the first `;` inside it.
            if is_compound_routine_header(&sql[index..])
                && let Some(routine_end) = routine_body_end(sql, index, &protected_ranges)
            {
                let (start_line, start_column) = (line, column);
                advance_text_position(&sql[index..routine_end], &mut line, &mut column);
                statements.push(SqlStatementSpan {
                    start: index,
                    end: routine_end,
                    line: start_line,
                    column: start_column,
                    end_line: line,
                    end_column: column,
                });
                index = routine_end;
                continue;
            }

            statement_start = Some((index, line, column));
        }
        index += character.len_utf8();
        advance_position(character, &mut line, &mut column);
        if !character.is_whitespace() {
            statement_end = Some((index, line, column));
        }
    }

    if let (Some((start, start_line, start_column)), Some((end, end_line, end_column))) =
        (statement_start, statement_end)
        && start < end
    {
        statements.push(SqlStatementSpan {
            start,
            end,
            line: start_line,
            column: start_column,
            end_line,
            end_column,
        });
    }

    statements
}

pub fn sql_statement_span_at(sql: &str, cursor_offset: usize) -> Option<SqlStatementSpan> {
    let spans = split_sql_statement_spans(sql);
    if spans.len() <= 1 {
        return None;
    }

    let cursor_offset = cursor_offset.min(sql.len());
    let mut previous = None;
    for span in spans {
        if cursor_offset <= span.end {
            return Some(if cursor_offset < span.start {
                previous.unwrap_or(span)
            } else {
                span
            });
        }
        previous = Some(span);
    }

    previous
}

fn advance_text_position(text: &str, line: &mut usize, column: &mut usize) {
    for character in text.chars() {
        advance_position(character, line, column);
    }
}

fn advance_position(character: char, line: &mut usize, column: &mut usize) {
    if character == '\n' {
        *line += 1;
        *column = 0;
    } else {
        *column += 1;
    }
}

fn skip_single_quoted_sql_string(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() {
        if bytes[index] == b'\'' {
            if bytes.get(index + 1) == Some(&b'\'') {
                index += 2;
            } else if bytes.get(index.wrapping_sub(1)) == Some(&b'\\') {
                index += 1;
            } else {
                return index + 1;
            }
        } else {
            index += 1;
        }
    }
    index
}

fn scan_identifier_tail(bytes: &[u8], mut index: usize) -> usize {
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        index += 1;
    }
    index
}

fn scan_digits(bytes: &[u8], mut index: usize) -> usize {
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
    }
    index
}

fn is_identifier_start(byte: Option<u8>) -> bool {
    byte.is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
}

fn is_identifier_tail(byte: Option<u8>) -> bool {
    byte.is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn skip_quoted_identifier(bytes: &[u8], mut index: usize, quote: u8) -> usize {
    while index < bytes.len() {
        if bytes[index] == quote {
            if bytes.get(index + 1) == Some(&quote) {
                index += 2;
            } else {
                return index + 1;
            }
        } else {
            index += 1;
        }
    }
    index
}

fn skip_bracket_quoted_identifier(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() {
        if bytes[index] == b']' {
            if bytes.get(index + 1) == Some(&b']') {
                index += 2;
            } else {
                return index + 1;
            }
        } else {
            index += 1;
        }
    }
    index
}

fn skip_line_comment(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && bytes[index] != b'\n' {
        index += 1;
    }
    index
}

fn skip_block_comment(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() {
        if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
            return index + 2;
        }
        index += 1;
    }
    index
}

fn dollar_quote_delimiter_end(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'$') {
        return None;
    }

    let mut index = start + 1;
    if bytes.get(index) == Some(&b'$') {
        return Some(index + 1);
    }

    if !bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
    {
        return None;
    }

    index += 1;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        index += 1;
    }

    (bytes.get(index) == Some(&b'$')).then_some(index + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbol_heads(symbols: &[SqlDocumentSymbol]) -> Vec<(String, usize, usize)> {
        symbols
            .iter()
            .map(|symbol| (symbol.label.clone(), symbol.line, symbol.column))
            .collect()
    }

    #[test]
    fn protects_sql_literals_comments_and_quoted_identifiers() {
        let sql = "select ':id', \"@name\", `?`, [value]]?], -- $1\n/* :x */ where id = :id";
        let ranges = sql_protected_ranges(sql);

        for token in [":id'", "@name", "`?`", "[value", "$1", ":x"] {
            let position = sql.find(token).expect("token should exist");
            assert!(is_position_in_sql_ranges(position, &ranges), "{token}");
        }

        let live_position = sql.rfind(":id").expect("live parameter should exist");
        assert!(!is_position_in_sql_ranges(live_position, &ranges));

        assert_eq!(ranges[0].kind, SqlProtectedRangeKind::StringLiteral);
        assert_eq!(ranges[1].kind, SqlProtectedRangeKind::QuotedIdentifier);
        assert_eq!(ranges[2].kind, SqlProtectedRangeKind::QuotedIdentifier);
        assert_eq!(ranges[3].kind, SqlProtectedRangeKind::QuotedIdentifier);
        assert_eq!(ranges[4].kind, SqlProtectedRangeKind::Comment);
        assert_eq!(ranges[5].kind, SqlProtectedRangeKind::Comment);
    }

    #[test]
    fn protects_prefixed_unicode_string_literals() {
        let sql = "select N'literal @x', name from users where id = @id";
        let ranges = sql_protected_ranges(sql);
        let string = ranges
            .iter()
            .find(|range| sql[range.start..range.end].starts_with('N'))
            .expect("prefixed string range should exist");

        assert_eq!(string.kind, SqlProtectedRangeKind::StringLiteral);
        assert_eq!(&sql[string.start..string.end], "N'literal @x'");

        let placeholders = sql_parameter_placeholders_with_ranges(sql, &ranges);
        assert_eq!(placeholders.len(), 1);
        assert_eq!(&sql[placeholders[0].start..placeholders[0].end], "@id");
    }

    #[test]
    fn protects_postgres_dollar_quoted_bodies() {
        let sql = "select $fn$ begin return :ignored + $1; end $fn$, :live";
        let ranges = sql_protected_ranges(sql);

        let ignored_position = sql
            .find(":ignored")
            .expect("ignored parameter should exist");
        let live_position = sql.find(":live").expect("live parameter should exist");

        assert!(is_position_in_sql_ranges(ignored_position, &ranges));
        assert!(!is_position_in_sql_ranges(live_position, &ranges));
        assert_eq!(ranges[0].kind, SqlProtectedRangeKind::DollarQuotedString);
    }

    #[test]
    fn leaves_unclosed_dollar_tag_body_live() {
        let sql = "select $tag$ :still_live";
        let ranges = sql_protected_ranges(sql);
        let live_position = sql
            .find(":still_live")
            .expect("live parameter should exist");

        assert!(!is_position_in_sql_ranges(live_position, &ranges));
    }

    #[test]
    fn can_protect_unclosed_dollar_tag_body_for_visible_range_highlighting() {
        let sql = "select $tag$ :body";
        let ranges = sql_protected_ranges_with_unclosed_dollar_strings(sql, true);
        let body_position = sql.find(":body").expect("body should exist");

        assert!(is_position_in_sql_ranges(body_position, &ranges));
        assert_eq!(ranges[0].kind, SqlProtectedRangeKind::DollarQuotedString);
        assert_eq!(ranges[0].end, sql.len());
    }

    #[test]
    fn finds_unclosed_dollar_quoted_string_start() {
        let sql = "SELECT 1;\nDO $body$\nBEGIN\n  RAISE NOTICE ':ignored';";

        assert_eq!(unclosed_dollar_quoted_string_start(sql), sql.find("$body$"));
        assert_eq!(
            unclosed_dollar_quoted_string_start("SELECT $$closed$$;"),
            None
        );
    }

    #[test]
    fn range_at_advances_cursor_over_past_ranges() {
        let sql = "select ':x', ':y', :live";
        let ranges = sql_protected_ranges(sql);
        let mut range_index = 0usize;

        let first = sql_protected_range_at(sql.find(":x").expect("x"), &ranges, &mut range_index)
            .expect("first range");
        assert_eq!(first.kind, SqlProtectedRangeKind::StringLiteral);
        assert_eq!(range_index, 0);

        let second = sql_protected_range_at(sql.find(":y").expect("y"), &ranges, &mut range_index)
            .expect("second range");
        assert_eq!(second.kind, SqlProtectedRangeKind::StringLiteral);
        assert_eq!(range_index, 1);

        let live = sql.find(":live").expect("live");
        assert!(sql_protected_range_at(live, &ranges, &mut range_index).is_none());
    }

    #[test]
    fn parameter_placeholders_skip_protected_ranges_and_casts() {
        let sql = "select ':ignored', value::text, \"@quoted\", $body$? $1 :x$body$, :id, @name, $slug, $2, ?";
        let placeholders = sql_parameter_placeholders(sql);

        assert_eq!(
            placeholders
                .iter()
                .map(|placeholder| (&sql[placeholder.start..placeholder.end], &placeholder.kind))
                .collect::<Vec<_>>(),
            vec![
                (":id", &SqlParameterPlaceholderKind::Named("id".to_string())),
                (
                    "@name",
                    &SqlParameterPlaceholderKind::Named("name".to_string())
                ),
                (
                    "$slug",
                    &SqlParameterPlaceholderKind::Named("slug".to_string())
                ),
                ("$2", &SqlParameterPlaceholderKind::DollarPositional(2)),
                ("?", &SqlParameterPlaceholderKind::QuestionMark),
            ]
        );
    }

    #[test]
    fn symbol_words_skip_protected_ranges_and_preserve_qualified_identifiers() {
        let sql = "/* create table fake */ create table \"public\".\"audit log\" (id int)";
        assert_eq!(
            sql_symbol_words(sql, 6),
            vec!["create", "table", "public.audit log", "id", "int"]
        );

        let sql = "update [dbo].[Users] set note = '-- table orders'";
        assert_eq!(
            sql_symbol_words(sql, 6),
            vec!["update", "dbo.Users", "set", "note"]
        );
    }

    #[test]
    fn symbol_word_spans_track_source_byte_ranges() {
        let sql = "create table \"public\".\"audit log\" (\"café\" text)";
        let words = sql_symbol_word_spans(sql, 6);

        assert_eq!(
            words
                .iter()
                .map(|word| word.text.as_str())
                .collect::<Vec<_>>(),
            vec!["create", "table", "public.audit log", "café", "text"]
        );

        for word in &words {
            assert!(sql.is_char_boundary(word.start));
            assert!(sql.is_char_boundary(word.end));
            assert!(word.end <= sql.len());
        }

        let qualified = words
            .iter()
            .find(|word| word.text == "public.audit log")
            .expect("qualified table word");
        assert_eq!(
            &sql[qualified.start..qualified.end],
            r#""public"."audit log""#
        );

        let cafe = words
            .iter()
            .find(|word| word.text == "café")
            .expect("quoted unicode column word");
        assert_eq!(&sql[cafe.start..cafe.end], r#""café""#);
    }

    #[test]
    fn symbol_at_offset_returns_qualified_segment() {
        let sql = r#"select "public"."audit log"."café" from "public"."audit log""#;
        let offset = sql.find("café").expect("café segment") + 1;
        let symbol = sql_symbol_at_offset(sql, offset).expect("symbol at offset");

        assert_eq!(symbol.text, "café");
        assert_eq!(&sql[symbol.start..symbol.end], "café");
    }

    #[test]
    fn matching_symbol_segments_split_qualified_and_skip_protected_text() {
        let sql = r#"select users.id, "users"."id" from users where note = 'users.id'"#;
        let segments = sql_matching_symbol_segments(sql, "users");
        let sources: Vec<&str> = segments
            .iter()
            .map(|(start, end)| &sql[*start..*end])
            .collect();

        assert_eq!(sources, vec!["users", "users", "users"]);
    }

    #[test]
    fn matching_qualified_symbol_segments_stay_with_same_qualifier() {
        let sql = r#"select users.id, orders.id, "users"."id" from users where note = 'users.id'"#;
        let segments = sql_matching_qualified_symbol_segments(sql, "users", "id");
        let sources: Vec<&str> = segments
            .iter()
            .map(|(start, end)| &sql[*start..*end])
            .collect();

        assert_eq!(sources, vec!["id", "id"]);
        assert_eq!(
            &sql[segments[0].0.saturating_sub(6)..segments[0].0],
            "users."
        );
        assert_eq!(
            &sql[segments[1].0.saturating_sub(9)..segments[1].0],
            "\"users\".\""
        );
    }

    #[test]
    fn qualified_reference_prefix_uses_last_qualified_symbol() {
        assert_eq!(
            qualified_sql_reference_prefix(r#"select "public"."users"."#),
            Some("users".to_string())
        );
        assert_eq!(
            qualified_sql_reference_prefix("select users."),
            Some("users".to_string())
        );
        assert_eq!(qualified_sql_reference_prefix("select users"), None);
    }

    #[test]
    fn qualified_reference_at_offset_returns_qualifier_and_segment() {
        let sql = r#"select "public"."users"."id" from users"#;
        let offset = sql.find("id").expect("id segment") + 1;

        assert_eq!(
            qualified_sql_reference_at_offset(sql, offset),
            Some(("public.users".to_string(), "id".to_string()))
        );
    }

    #[test]
    fn document_symbols_split_statements_and_track_lines() {
        let symbols = sql_document_symbols(
            "select * from users;\n\ncreate table audit_log (id int);\nupdate users set name = 'a;b';",
        );

        assert_eq!(
            symbol_heads(&symbols),
            vec![
                ("Query".to_string(), 0, 0),
                ("CREATE audit_log".to_string(), 2, 0),
                ("UPDATE users".to_string(), 3, 0),
            ]
        );
    }

    #[test]
    fn document_symbols_ignore_protected_sql_and_preserve_targets() {
        let symbols = sql_document_symbols(
            "select $fn$ begin perform ';'; end $fn$;\n\
             update `user;table` set name = 'a;b';\n\
             /* create table fake_target */ create table \"public\".\"audit log\" (id int);",
        );

        assert_eq!(
            symbol_heads(&symbols),
            vec![
                ("Query".to_string(), 0, 0),
                ("UPDATE user;table".to_string(), 1, 0),
                ("CREATE public.audit log".to_string(), 2, 0),
            ]
        );
    }

    #[test]
    fn document_symbols_skip_ddl_modifiers_for_targets() {
        let symbols = sql_document_symbols(
            "create or replace view reporting.monthly_sales as select 1;\n\
             create unique index concurrently idx_sales on reporting.monthly_sales(id);\n\
             drop table if exists stale_sales;",
        );

        assert_eq!(
            symbol_heads(&symbols),
            vec![
                ("CREATE reporting.monthly_sales".to_string(), 0, 0),
                ("CREATE idx_sales".to_string(), 1, 0),
                ("DROP stale_sales".to_string(), 2, 0),
            ]
        );
    }

    #[test]
    fn routine_body_is_not_split_on_its_internal_semicolons() {
        let sql = r#"CREATE DEFINER=`root`@`localhost` TRIGGER `trg_orders_audit_insert` AFTER INSERT ON `orders_orders` FOR EACH ROW BEGIN
    INSERT INTO audit_event_log (
        tenant_id,
        table_name
    ) VALUES (
        NEW.tenant_id,
        'orders_orders'
    );
END"#;
        let statements = split_sql_statements(sql);

        assert_eq!(
            statements.len(),
            1,
            "the whole routine must stay one statement, got: {:?}",
            statements
        );
        assert!(statements[0].ends_with("END"));
    }

    #[test]
    fn routine_body_keeps_nested_blocks_together() {
        let sql = r#"CREATE PROCEDURE p()
BEGIN
    DECLARE done INT DEFAULT 0;
    IF done = 0 THEN
        SELECT 1;
    END IF;
    CASE done
        WHEN 0 THEN SELECT 2;
    END CASE;
    SELECT CASE WHEN done = 1 THEN 'a' ELSE 'b' END;
END;
SELECT 99;"#;
        let statements = split_sql_statements(sql);

        assert_eq!(
            statements.len(),
            2,
            "nested IF/CASE blocks must not end the routine early, got: {:?}",
            statements
        );
        assert!(statements[0].starts_with("CREATE PROCEDURE"));
        assert!(statements[0].ends_with("END"));
        assert_eq!(statements[1], "SELECT 99");
    }

    #[test]
    fn single_statement_trigger_body_still_ends_at_its_semicolon() {
        let sql =
            "CREATE TRIGGER t AFTER INSERT ON o FOR EACH ROW INSERT INTO a VALUES (1);\nSELECT 2;";
        let statements = split_sql_statements(sql);

        assert_eq!(statements.len(), 2, "got: {:?}", statements);
        assert_eq!(statements[1], "SELECT 2");
    }

    #[test]
    fn ordinary_statements_are_unaffected_by_routine_handling() {
        let sql = "CREATE TABLE t (id int);\nSELECT 1;\nUPDATE t SET id = 2;";
        let statements = split_sql_statements(sql);

        assert_eq!(statements.len(), 3, "got: {:?}", statements);
        assert_eq!(statements[1], "SELECT 1");
    }

    #[test]
    fn document_symbols_include_source_and_target_ranges() {
        let sql = "create table \"public\".\"audit log\" (id int);\nselect 1;";
        let symbols = sql_document_symbols(sql);

        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].source_range, 0..42);
        assert_eq!(
            &sql[symbols[0].source_range.clone()],
            "create table \"public\".\"audit log\" (id int)"
        );
        let target_range = symbols[0].target_range.clone().expect("target range");
        assert_eq!(&sql[target_range], r#""public"."audit log""#);
        assert_eq!(symbols[1].source_range, 44..52);
        assert_eq!(symbols[1].target_range, None);
    }

    #[test]
    fn split_statements_respects_protected_ranges() {
        let sql = "select ';'; select $body$ begin perform ';'; end $body$; select `semi;colon`; select [semi;colon]";
        let statements = split_sql_statements(sql);

        assert_eq!(
            statements,
            vec![
                "select ';'".to_string(),
                "select $body$ begin perform ';'; end $body$".to_string(),
                "select `semi;colon`".to_string(),
                "select [semi;colon]".to_string(),
            ]
        );
    }

    #[test]
    fn split_statement_spans_track_trimmed_byte_ranges() {
        let sql = "  select 1;\n\n  update users set note = 'a;b';";
        let spans = split_sql_statement_spans(sql);
        let update_start = sql.find("update").expect("update start");

        assert_eq!(
            spans,
            vec![
                SqlStatementSpan {
                    start: 2,
                    end: 10,
                    line: 0,
                    column: 2,
                    end_line: 0,
                    end_column: 10,
                },
                SqlStatementSpan {
                    start: update_start,
                    end: sql.len() - 1,
                    line: 2,
                    column: 2,
                    end_line: 2,
                    end_column: 31,
                },
            ]
        );
        assert_eq!(&sql[spans[0].start..spans[0].end], "select 1");
        assert_eq!(
            &sql[spans[1].start..spans[1].end],
            "update users set note = 'a;b'"
        );
    }

    #[test]
    fn split_statement_spans_track_positions_after_protected_ranges() {
        let sql = "select $$line 1\nline 2;$$;\n  create table audit_log (id int);";
        let spans = split_sql_statement_spans(sql);

        assert_eq!(spans.len(), 2);
        assert_eq!(
            &sql[spans[0].start..spans[0].end],
            "select $$line 1\nline 2;$$"
        );
        assert_eq!(spans[0].line, 0);
        assert_eq!(spans[0].column, 0);
        assert_eq!(spans[0].end_line, 1);
        assert_eq!(spans[0].end_column, 9);
        assert_eq!(
            &sql[spans[1].start..spans[1].end],
            "create table audit_log (id int)"
        );
        assert_eq!(spans[1].line, 2);
        assert_eq!(spans[1].column, 2);
        assert_eq!(spans[1].end_line, 2);
        assert_eq!(spans[1].end_column, 33);
    }

    #[test]
    fn statement_span_at_selects_cursor_statement() {
        let sql = "select 1;\nselect 1;\nselect 2;";
        let second_select = sql.rfind("select 1").expect("second select");
        let span = sql_statement_span_at(sql, second_select + "select".len()).expect("span");

        assert_eq!(&sql[span.start..span.end], "select 1");
        assert_eq!(span.line, 1);
        assert_eq!(span.column, 0);
        assert_eq!(span.end_line, 1);
        assert_eq!(span.end_column, 8);
    }

    #[test]
    fn statement_span_at_uses_nearest_statement_for_whitespace_gaps() {
        let sql = "select 1;\n\nselect 2;\n\n";

        let between = sql.find("\n\n").expect("gap") + 1;
        let between_span = sql_statement_span_at(sql, between).expect("between span");
        assert_eq!(&sql[between_span.start..between_span.end], "select 1");

        let after_last = sql.len();
        let after_span = sql_statement_span_at(sql, after_last).expect("after span");
        assert_eq!(&sql[after_span.start..after_span.end], "select 2");
    }
}
