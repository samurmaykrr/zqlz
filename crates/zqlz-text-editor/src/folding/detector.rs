//! Code folding region detection for query text.
//!
//! Detects foldable regions in SQL text:
//! - BEGIN...END blocks
//! - Driver-configured multi-line comments
//! - Function/procedure bodies
//! - Nested blocks

use std::ops::Range;

use crate::TextBuffer;
use zqlz_core::{
    SQL_FOLDING_RULES, SqlProtectedRange, SyntaxFoldingRules, sql_protected_range_at,
    sql_protected_ranges,
};

/// Type of foldable region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldKind {
    /// A BEGIN...END block (stored procedures, anonymous blocks).
    Block,
    /// A multi-line comment.
    Comment,
    /// A function or procedure definition.
    Function,
    /// A CASE...END expression.
    Case,
    /// Parenthesized expression spanning multiple lines.
    Parenthesis,
}

impl FoldKind {
    /// Get a human-readable label for the fold kind.
    pub fn label(&self) -> &'static str {
        match self {
            FoldKind::Block => "block",
            FoldKind::Comment => "comment",
            FoldKind::Function => "function",
            FoldKind::Case => "case",
            FoldKind::Parenthesis => "(...)",
        }
    }
}

/// A foldable region in the source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldRegion {
    /// Starting line (0-based).
    pub start_line: usize,
    /// Ending line (0-based, inclusive).
    pub end_line: usize,
    /// The type of fold region.
    pub kind: FoldKind,
}

impl FoldRegion {
    /// Create a new fold region.
    pub fn new(start_line: usize, end_line: usize, kind: FoldKind) -> Self {
        Self {
            start_line,
            end_line,
            kind,
        }
    }

    /// Get the line range of this region.
    pub fn line_range(&self) -> Range<usize> {
        self.start_line..self.end_line + 1
    }

    /// Get the number of lines in this region.
    pub fn line_count(&self) -> usize {
        self.end_line - self.start_line + 1
    }

    /// Check if this region is foldable (more than one line).
    pub fn is_foldable(&self) -> bool {
        self.end_line > self.start_line
    }

    /// Check if this region contains a given line.
    pub fn contains_line(&self, line: usize) -> bool {
        line >= self.start_line && line <= self.end_line
    }
}

/// Detects foldable regions in SQL text.
#[derive(Debug)]
pub struct FoldingDetector {
    /// Minimum lines for a region to be foldable.
    pub min_lines: usize,
    block_comment_delimiters: Option<(&'static str, &'static str)>,
    folding_rules: SyntaxFoldingRules,
}

impl Default for FoldingDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl FoldingDetector {
    /// Create a new folding detector with default settings.
    pub fn new() -> Self {
        Self {
            min_lines: 2,
            block_comment_delimiters: Some(("/*", "*/")),
            folding_rules: SQL_FOLDING_RULES,
        }
    }

    /// Set the minimum number of lines for a foldable region.
    pub fn min_lines(mut self, lines: usize) -> Self {
        self.min_lines = lines;
        self
    }

    /// Set the block comment delimiters used for comment folds.
    pub fn block_comment_delimiters(
        mut self,
        delimiters: Option<(&'static str, &'static str)>,
    ) -> Self {
        self.block_comment_delimiters = delimiters;
        self
    }

    pub fn folding_rules(mut self, folding_rules: SyntaxFoldingRules) -> Self {
        self.folding_rules = folding_rules;
        self
    }

    /// Detect all foldable regions in the given query text.
    pub fn detect(&self, text: &str) -> Vec<FoldRegion> {
        let lines = active_fold_lines(text);
        let mut regions = Vec::new();

        self.detect_multiline_comments(text, &mut regions);
        self.detect_configured_syntax_folds(&lines, &mut regions);

        regions.retain(|r| r.line_count() >= self.min_lines);
        regions.sort_by_key(|r| (r.start_line, std::cmp::Reverse(r.end_line)));
        regions
    }

    fn detect_configured_syntax_folds(&self, lines: &[FoldLine], regions: &mut Vec<FoldRegion>) {
        if self.folding_rules.begin_end_blocks {
            self.detect_begin_end_blocks(lines, regions);
        }
        if self.folding_rules.case_blocks {
            self.detect_case_blocks(lines, regions);
        }
        if self.folding_rules.function_definitions {
            self.detect_function_definitions(lines, regions);
        }
        if self.folding_rules.parenthesis_blocks {
            self.detect_parenthesis_blocks(lines, regions);
        }
    }

    fn detect_multiline_comments(&self, text: &str, regions: &mut Vec<FoldRegion>) {
        let lines = text.lines().map(str::to_string).collect::<Vec<_>>();
        self.detect_multiline_comments_in_lines(&lines, regions);
    }

    fn detect_multiline_comments_in_lines(&self, lines: &[String], regions: &mut Vec<FoldRegion>) {
        let Some((start_delimiter, end_delimiter)) = self.block_comment_delimiters else {
            return;
        };
        if start_delimiter.is_empty() || end_delimiter.is_empty() {
            return;
        }

        let mut in_comment = false;
        let mut comment_start_line = 0;
        let mut search_offset = 0;

        for (line_number, line) in lines.iter().enumerate() {
            search_offset = search_offset.min(line.len());

            loop {
                if !in_comment {
                    let Some(start_offset) = line[search_offset..]
                        .find(start_delimiter)
                        .map(|offset| search_offset + offset)
                    else {
                        search_offset = 0;
                        break;
                    };
                    in_comment = true;
                    comment_start_line = line_number;
                    search_offset = start_offset + start_delimiter.len();
                }

                let Some(end_offset) = line[search_offset..]
                    .find(end_delimiter)
                    .map(|offset| search_offset + offset)
                else {
                    search_offset = 0;
                    break;
                };
                in_comment = false;
                regions.push(FoldRegion::new(
                    comment_start_line,
                    line_number,
                    FoldKind::Comment,
                ));
                search_offset = end_offset + end_delimiter.len();
                if search_offset >= line.len() {
                    search_offset = 0;
                    break;
                }
            }
        }
    }

    /// Detect BEGIN...END blocks.
    fn detect_begin_end_blocks(&self, lines: &[FoldLine], regions: &mut Vec<FoldRegion>) {
        let mut stack: Vec<usize> = Vec::new();

        for line in lines {
            for token in &line.tokens {
                if token == "BEGIN" {
                    stack.push(line.line_number);
                } else if token == "END"
                    && let Some(start) = stack.pop()
                {
                    regions.push(FoldRegion::new(start, line.line_number, FoldKind::Block));
                }
            }
        }
    }

    /// Detect CASE...END expressions.
    fn detect_case_blocks(&self, lines: &[FoldLine], regions: &mut Vec<FoldRegion>) {
        let mut stack: Vec<usize> = Vec::new();

        for line in lines {
            for token in &line.tokens {
                if token == "CASE" {
                    stack.push(line.line_number);
                } else if token == "END"
                    && !stack.is_empty()
                    && let Some(start) = stack.pop()
                    && start != line.line_number
                {
                    regions.push(FoldRegion::new(start, line.line_number, FoldKind::Case));
                }
            }
        }
    }

    /// Detect function/procedure definitions.
    fn detect_function_definitions(&self, lines: &[FoldLine], regions: &mut Vec<FoldRegion>) {
        let mut func_start: Option<usize> = None;
        let mut in_function = false;
        let mut depth = 0;

        for line in lines {
            if !in_function && line_starts_function_definition(&line.tokens) {
                func_start = Some(line.line_number);
                in_function = true;
                depth = 0;
            }

            if in_function {
                for token in &line.tokens {
                    if token == "BEGIN" {
                        depth += 1;
                    } else if token == "END" {
                        if depth > 0 {
                            depth -= 1;
                        }
                        if depth == 0
                            && let Some(start) = func_start.take()
                        {
                            regions.push(FoldRegion::new(
                                start,
                                line.line_number,
                                FoldKind::Function,
                            ));
                            in_function = false;
                        }
                    }
                }

                if line.ends_with_semicolon
                    && depth == 0
                    && let Some(start) = func_start.take()
                {
                    regions.push(FoldRegion::new(start, line.line_number, FoldKind::Function));
                    in_function = false;
                }
            }
        }
    }

    /// Detect parenthesized blocks spanning multiple lines.
    fn detect_parenthesis_blocks(&self, lines: &[FoldLine], regions: &mut Vec<FoldRegion>) {
        let mut stack: Vec<usize> = Vec::new();

        for line in lines {
            for c in line.active_text.chars() {
                if c == '(' {
                    stack.push(line.line_number);
                } else if c == ')'
                    && let Some(start) = stack.pop()
                    && start != line.line_number
                {
                    regions.push(FoldRegion::new(
                        start,
                        line.line_number,
                        FoldKind::Parenthesis,
                    ));
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FoldLine {
    line_number: usize,
    active_text: String,
    tokens: Vec<String>,
    ends_with_semicolon: bool,
}

fn active_fold_lines(text: &str) -> Vec<FoldLine> {
    let protected_ranges = sql_protected_ranges(text);
    let mut protected_range_index = 0usize;
    let mut line_start = 0usize;
    let mut lines = Vec::new();

    for (line_number, line) in text.lines().enumerate() {
        let active_text = active_line_text(
            text,
            line_start,
            line,
            &protected_ranges,
            &mut protected_range_index,
        );
        let tokens = active_sql_words(&active_text);
        let ends_with_semicolon = active_text.trim_end().ends_with(';');
        lines.push(FoldLine {
            line_number,
            active_text,
            tokens,
            ends_with_semicolon,
        });
        line_start += line.len() + 1;
    }

    lines
}

fn active_line_text(
    text: &str,
    line_start: usize,
    line: &str,
    protected_ranges: &[SqlProtectedRange],
    protected_range_index: &mut usize,
) -> String {
    let mut active = String::with_capacity(line.len());
    let line_end = line_start + line.len();
    let mut byte_offset = line_start;

    while byte_offset < line_end {
        if let Some(protected_range) =
            sql_protected_range_at(byte_offset, protected_ranges, protected_range_index)
        {
            let protected_end = protected_range.end.min(line_end);
            active.extend(std::iter::repeat_n(
                ' ',
                protected_end.saturating_sub(byte_offset),
            ));
            byte_offset = protected_end;
            continue;
        }

        let Some(character) = text[byte_offset..line_end].chars().next() else {
            break;
        };
        active.push(character);
        byte_offset += character.len_utf8();
    }

    active
}

fn active_sql_words(text: &str) -> Vec<String> {
    text.split(|character: char| !is_sql_word_character(character))
        .filter(|word| !word.is_empty())
        .map(|word| word.to_ascii_uppercase())
        .collect()
}

fn is_sql_word_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

fn line_starts_function_definition(tokens: &[String]) -> bool {
    matches!(
        tokens,
        [create, function, ..] if create == "CREATE" && (function == "FUNCTION" || function == "PROCEDURE")
    ) || matches!(
        tokens,
        [create, or, replace, function, ..]
            if create == "CREATE"
                && or == "OR"
                && replace == "REPLACE"
                && (function == "FUNCTION" || function == "PROCEDURE")
    )
}

pub fn detect_folds_in_buffer(buffer: &TextBuffer) -> Vec<FoldRegion> {
    detect_folds_in_buffer_with_block_comments(buffer, Some(("/*", "*/")))
}

pub fn detect_folds_in_buffer_with_block_comments(
    buffer: &TextBuffer,
    block_comment_delimiters: Option<(&'static str, &'static str)>,
) -> Vec<FoldRegion> {
    detect_folds_in_buffer_with_rules(buffer, block_comment_delimiters, SQL_FOLDING_RULES)
}

pub fn detect_folds_in_buffer_with_rules(
    buffer: &TextBuffer,
    block_comment_delimiters: Option<(&'static str, &'static str)>,
    folding_rules: SyntaxFoldingRules,
) -> Vec<FoldRegion> {
    let detector = FoldingDetector::new()
        .block_comment_delimiters(block_comment_delimiters)
        .folding_rules(folding_rules);
    detector.detect(&buffer.text())
}

pub fn detect_folds_in_range(buffer: &TextBuffer, line_range: Range<usize>) -> Vec<FoldRegion> {
    detect_folds_in_range_with_block_comments(buffer, line_range, Some(("/*", "*/")))
}

pub fn detect_folds_in_range_with_block_comments(
    buffer: &TextBuffer,
    line_range: Range<usize>,
    block_comment_delimiters: Option<(&'static str, &'static str)>,
) -> Vec<FoldRegion> {
    detect_folds_in_range_with_rules(
        buffer,
        line_range,
        block_comment_delimiters,
        SQL_FOLDING_RULES,
    )
}

pub fn detect_folds_in_range_with_rules(
    buffer: &TextBuffer,
    line_range: Range<usize>,
    block_comment_delimiters: Option<(&'static str, &'static str)>,
    folding_rules: SyntaxFoldingRules,
) -> Vec<FoldRegion> {
    detect_folds_in_buffer_with_rules(buffer, block_comment_delimiters, folding_rules)
        .into_iter()
        .filter(|region| region.end_line >= line_range.start && region.start_line < line_range.end)
        .collect()
}

pub fn detect_folds(buffer: &TextBuffer) -> Vec<FoldRegion> {
    detect_folds_in_buffer(buffer)
}

pub fn detect_folds_with_block_comments(
    buffer: &TextBuffer,
    block_comment_delimiters: Option<(&'static str, &'static str)>,
) -> Vec<FoldRegion> {
    detect_folds_in_buffer_with_block_comments(buffer, block_comment_delimiters)
}

pub fn detect_folds_with_rules(
    buffer: &TextBuffer,
    block_comment_delimiters: Option<(&'static str, &'static str)>,
    folding_rules: SyntaxFoldingRules,
) -> Vec<FoldRegion> {
    detect_folds_in_buffer_with_rules(buffer, block_comment_delimiters, folding_rules)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_begin_end_block() {
        let sql = r#"BEGIN
    SELECT 1;
    SELECT 2;
END;"#;
        let regions = detect_folds(&TextBuffer::new(sql));

        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].kind, FoldKind::Block);
        assert_eq!(regions[0].start_line, 0);
        assert_eq!(regions[0].end_line, 3);
    }

    #[test]
    fn test_detect_multiline_comment() {
        let sql = r#"SELECT *
/* This is a
   multi-line
   comment */
FROM users;"#;
        let regions = detect_folds(&TextBuffer::new(sql));

        let comments: Vec<_> = regions
            .iter()
            .filter(|r| r.kind == FoldKind::Comment)
            .collect();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].start_line, 1);
        assert_eq!(comments[0].end_line, 3);
    }

    #[test]
    fn test_detect_multiline_comments_in_lines_matches_text_detector() {
        let sql = "SELECT 1\n/* comment\nstill comment */\nSELECT 2";
        let detector = FoldingDetector::new();
        let mut from_text = Vec::new();
        detector.detect_multiline_comments(sql, &mut from_text);
        let mut from_lines = Vec::new();
        let lines = sql
            .lines()
            .map(|line| format!("{line}\n"))
            .collect::<Vec<_>>();

        detector.detect_multiline_comments_in_lines(&lines, &mut from_lines);

        assert_eq!(from_lines, from_text);
    }

    #[test]
    fn test_detect_multiline_comment_uses_configured_delimiters() {
        let text = r#"SELECT 1
<!-- note
still note -->
SELECT 2"#;
        let detector = FoldingDetector::new().block_comment_delimiters(Some(("<!--", "-->")));
        let regions = detector.detect(text);

        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].kind, FoldKind::Comment);
        assert_eq!(regions[0].start_line, 1);
        assert_eq!(regions[0].end_line, 2);
    }

    #[test]
    fn test_detect_multiline_comment_respects_disabled_driver_delimiters() {
        let sql = "SELECT 1\n/* comment\nstill comment */\nSELECT 2";
        let detector = FoldingDetector::new().block_comment_delimiters(None);
        let regions = detector.detect(sql);

        assert!(
            regions
                .iter()
                .all(|region| region.kind != FoldKind::Comment)
        );
    }

    #[test]
    fn test_detect_respects_configured_folding_rules() {
        let sql = "BEGIN\nSELECT (\n1\n);\nEND;";
        let detector = FoldingDetector::new().folding_rules(zqlz_core::SyntaxFoldingRules {
            begin_end_blocks: false,
            case_blocks: false,
            function_definitions: false,
            parenthesis_blocks: true,
        });
        let regions = detector.detect(sql);

        assert!(regions.iter().all(|region| region.kind != FoldKind::Block));
        assert!(
            regions
                .iter()
                .any(|region| region.kind == FoldKind::Parenthesis)
        );
    }

    #[test]
    fn test_syntax_folds_ignore_strings_and_comments() {
        let sql = r#"SELECT 'BEGIN';
-- CASE
SELECT '(';
SELECT 1;
-- END
SELECT ')';"#;
        let regions = detect_folds(&TextBuffer::new(sql));

        assert!(
            regions
                .iter()
                .all(|region| region.kind == FoldKind::Comment),
            "strings/comments should not create syntax folds: {regions:?}"
        );
    }

    #[test]
    fn test_function_definition_ignores_comment_prefix() {
        let sql = r#"-- CREATE FUNCTION fake()
SELECT 1;
-- END;"#;
        let regions = detect_folds(&TextBuffer::new(sql));

        assert!(
            regions
                .iter()
                .all(|region| region.kind != FoldKind::Function),
            "commented function header should not create function fold: {regions:?}"
        );
    }

    #[test]
    fn test_detect_nested_blocks() {
        let sql = r#"BEGIN
    BEGIN
        SELECT 1;
    END;
END;"#;
        let regions = detect_folds(&TextBuffer::new(sql));

        let blocks: Vec<_> = regions
            .iter()
            .filter(|r| r.kind == FoldKind::Block)
            .collect();
        assert_eq!(blocks.len(), 2);

        let outer = blocks.iter().find(|r| r.start_line == 0).unwrap();
        assert_eq!(outer.end_line, 4);

        let inner = blocks.iter().find(|r| r.start_line == 1).unwrap();
        assert_eq!(inner.end_line, 3);
    }

    #[test]
    fn test_detect_case_block() {
        let sql = r#"SELECT
    CASE
        WHEN x > 0 THEN 'positive'
        WHEN x < 0 THEN 'negative'
        ELSE 'zero'
    END as sign
FROM numbers;"#;
        let regions = detect_folds(&TextBuffer::new(sql));

        let cases: Vec<_> = regions
            .iter()
            .filter(|r| r.kind == FoldKind::Case)
            .collect();
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].start_line, 1);
        assert_eq!(cases[0].end_line, 5);
    }

    #[test]
    fn test_detect_folds_in_range_limits_results() {
        let sql = r#"BEGIN
    SELECT 1;
END;

SELECT (
    1 + 2
);"#;

        let regions = detect_folds_in_range(&TextBuffer::new(sql), 3..6);
        assert!(
            regions
                .iter()
                .all(|region| region.end_line >= 3 && region.start_line < 6)
        );
        assert!(
            regions
                .iter()
                .any(|region| region.kind == FoldKind::Parenthesis)
        );
        assert!(regions.iter().all(|region| region.kind != FoldKind::Block));
    }

    #[test]
    fn test_detect_function_definition() {
        let sql = r#"CREATE OR REPLACE FUNCTION get_user(p_id INTEGER)
RETURNS TEXT AS $$
BEGIN
    RETURN (SELECT name FROM users WHERE id = p_id);
END;
$$ LANGUAGE plpgsql;"#;
        let detector = FoldingDetector::new();
        let regions = detector.detect(sql);

        let funcs: Vec<_> = regions
            .iter()
            .filter(|r| r.kind == FoldKind::Function)
            .collect();
        assert!(!funcs.is_empty());
        assert_eq!(funcs[0].start_line, 0);
    }

    #[test]
    fn test_detect_parenthesis_block() {
        let sql = r#"SELECT * FROM (
    SELECT id, name
    FROM users
    WHERE active = true
) AS active_users;"#;
        let regions = detect_folds(&TextBuffer::new(sql));

        let parens: Vec<_> = regions
            .iter()
            .filter(|r| r.kind == FoldKind::Parenthesis)
            .collect();
        assert_eq!(parens.len(), 1);
        assert_eq!(parens[0].start_line, 0);
        assert_eq!(parens[0].end_line, 4);
    }

    #[test]
    fn test_fold_region_methods() {
        let region = FoldRegion::new(5, 10, FoldKind::Block);

        assert_eq!(region.line_range(), 5..11);
        assert_eq!(region.line_count(), 6);
        assert!(region.is_foldable());
        assert!(region.contains_line(5));
        assert!(region.contains_line(7));
        assert!(region.contains_line(10));
        assert!(!region.contains_line(4));
        assert!(!region.contains_line(11));
    }

    #[test]
    fn test_single_line_not_foldable() {
        let sql = "SELECT * FROM users;";
        let regions = detect_folds(&TextBuffer::new(sql));
        assert!(regions.is_empty());
    }

    #[test]
    fn test_min_lines_setting() {
        let sql = r#"BEGIN
    SELECT 1;
END;"#;
        let detector = FoldingDetector::new().min_lines(5);
        let regions = detector.detect(sql);

        assert!(regions.is_empty());
    }

    #[test]
    fn test_fold_kind_label() {
        assert_eq!(FoldKind::Block.label(), "block");
        assert_eq!(FoldKind::Comment.label(), "comment");
        assert_eq!(FoldKind::Function.label(), "function");
        assert_eq!(FoldKind::Case.label(), "case");
        assert_eq!(FoldKind::Parenthesis.label(), "(...)");
    }

    #[test]
    fn test_complex_sql_with_multiple_folds() {
        let sql = r#"/*
 * User management functions
 */
CREATE FUNCTION get_users()
RETURNS TABLE(id INT, name TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT u.id,
           CASE
               WHEN u.title IS NOT NULL THEN u.title || ' ' || u.name
               ELSE u.name
           END
    FROM users u
    WHERE u.active = true;
END;
$$ LANGUAGE plpgsql;"#;

        let regions = detect_folds(&TextBuffer::new(sql));

        assert!(regions.iter().any(|r| r.kind == FoldKind::Comment));
        assert!(
            regions
                .iter()
                .any(|r| r.kind == FoldKind::Function || r.kind == FoldKind::Block)
        );
    }
}
