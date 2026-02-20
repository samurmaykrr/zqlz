//! Code folding region detection for SQL.
//!
//! Detects foldable regions in SQL text:
//! - BEGIN...END blocks
//! - Multi-line comments
//! - Function/procedure bodies
//! - Nested blocks

use std::ops::Range;

/// Type of foldable region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldKind {
    /// A BEGIN...END block (stored procedures, anonymous blocks).
    Block,
    /// A multi-line comment (/* ... */).
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
#[derive(Debug, Default)]
pub struct FoldingDetector {
    /// Minimum lines for a region to be foldable.
    pub min_lines: usize,
}

impl FoldingDetector {
    /// Create a new folding detector with default settings.
    pub fn new() -> Self {
        Self { min_lines: 2 }
    }

    /// Set the minimum number of lines for a foldable region.
    pub fn min_lines(mut self, lines: usize) -> Self {
        self.min_lines = lines;
        self
    }

    /// Detect all foldable regions in the given SQL text.
    pub fn detect(&self, text: &str) -> Vec<FoldRegion> {
        let lines: Vec<&str> = text.lines().collect();
        let mut regions = Vec::new();

        self.detect_multiline_comments(text, &mut regions);
        self.detect_begin_end_blocks(&lines, &mut regions);
        self.detect_case_blocks(&lines, &mut regions);
        self.detect_function_definitions(&lines, &mut regions);
        self.detect_parenthesis_blocks(&lines, &mut regions);

        regions.retain(|r| r.line_count() >= self.min_lines);
        regions.sort_by_key(|r| (r.start_line, std::cmp::Reverse(r.end_line)));
        regions
    }

    /// Detect multi-line comments (/* ... */).
    fn detect_multiline_comments(&self, text: &str, regions: &mut Vec<FoldRegion>) {
        let mut in_comment = false;
        let mut comment_start_line = 0;
        let mut line_num = 0;
        let mut chars = text.chars().peekable();
        let mut prev_char = None;

        while let Some(c) = chars.next() {
            if c == '\n' {
                line_num += 1;
                prev_char = Some(c);
                continue;
            }

            if !in_comment && c == '/' && chars.peek() == Some(&'*') {
                in_comment = true;
                comment_start_line = line_num;
                chars.next();
            } else if in_comment && prev_char == Some('*') && c == '/' {
                in_comment = false;
                regions.push(FoldRegion::new(
                    comment_start_line,
                    line_num,
                    FoldKind::Comment,
                ));
            }

            prev_char = Some(c);
        }
    }

    /// Detect BEGIN...END blocks.
    fn detect_begin_end_blocks(&self, lines: &[&str], regions: &mut Vec<FoldRegion>) {
        let mut stack: Vec<usize> = Vec::new();

        for (line_num, line) in lines.iter().enumerate() {
            let upper = line.to_uppercase();
            let tokens: Vec<&str> = upper.split_whitespace().collect();

            for token in &tokens {
                if *token == "BEGIN" {
                    stack.push(line_num);
                } else if *token == "END" || token.starts_with("END;") || token.starts_with("END ")
                {
                    if let Some(start) = stack.pop() {
                        regions.push(FoldRegion::new(start, line_num, FoldKind::Block));
                    }
                }
            }
        }
    }

    /// Detect CASE...END expressions.
    fn detect_case_blocks(&self, lines: &[&str], regions: &mut Vec<FoldRegion>) {
        let mut stack: Vec<usize> = Vec::new();

        for (line_num, line) in lines.iter().enumerate() {
            let upper = line.to_uppercase();

            for word in upper.split(|c: char| !c.is_alphanumeric()) {
                if word == "CASE" {
                    stack.push(line_num);
                } else if word == "END" && !stack.is_empty() {
                    if let Some(start) = stack.pop() {
                        if start != line_num {
                            regions.push(FoldRegion::new(start, line_num, FoldKind::Case));
                        }
                    }
                }
            }
        }
    }

    /// Detect function/procedure definitions.
    fn detect_function_definitions(&self, lines: &[&str], regions: &mut Vec<FoldRegion>) {
        let mut func_start: Option<usize> = None;
        let mut in_function = false;
        let mut depth = 0;

        for (line_num, line) in lines.iter().enumerate() {
            let upper = line.to_uppercase();
            let trimmed = upper.trim();

            if !in_function
                && (trimmed.starts_with("CREATE FUNCTION")
                    || trimmed.starts_with("CREATE OR REPLACE FUNCTION")
                    || trimmed.starts_with("CREATE PROCEDURE")
                    || trimmed.starts_with("CREATE OR REPLACE PROCEDURE"))
            {
                func_start = Some(line_num);
                in_function = true;
                depth = 0;
            }

            if in_function {
                for word in upper.split(|c: char| !c.is_alphanumeric()) {
                    if word == "BEGIN" {
                        depth += 1;
                    } else if word == "END" {
                        if depth > 0 {
                            depth -= 1;
                        }
                        if depth == 0 {
                            if let Some(start) = func_start.take() {
                                regions.push(FoldRegion::new(start, line_num, FoldKind::Function));
                                in_function = false;
                            }
                        }
                    }
                }

                if trimmed.ends_with(";") && depth == 0 {
                    if let Some(start) = func_start.take() {
                        regions.push(FoldRegion::new(start, line_num, FoldKind::Function));
                        in_function = false;
                    }
                }
            }
        }
    }

    /// Detect parenthesized blocks spanning multiple lines.
    fn detect_parenthesis_blocks(&self, lines: &[&str], regions: &mut Vec<FoldRegion>) {
        let mut stack: Vec<usize> = Vec::new();

        for (line_num, line) in lines.iter().enumerate() {
            for c in line.chars() {
                if c == '(' {
                    stack.push(line_num);
                } else if c == ')' {
                    if let Some(start) = stack.pop() {
                        if start != line_num {
                            regions.push(FoldRegion::new(start, line_num, FoldKind::Parenthesis));
                        }
                    }
                }
            }
        }
    }
}

/// Convenience function to detect fold regions with default settings.
pub fn detect_folds(text: &str) -> Vec<FoldRegion> {
    FoldingDetector::new().detect(text)
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
        let regions = detect_folds(sql);

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
        let regions = detect_folds(sql);

        let comments: Vec<_> = regions
            .iter()
            .filter(|r| r.kind == FoldKind::Comment)
            .collect();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].start_line, 1);
        assert_eq!(comments[0].end_line, 3);
    }

    #[test]
    fn test_detect_nested_blocks() {
        let sql = r#"BEGIN
    BEGIN
        SELECT 1;
    END;
END;"#;
        let regions = detect_folds(sql);

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
        let regions = detect_folds(sql);

        let cases: Vec<_> = regions
            .iter()
            .filter(|r| r.kind == FoldKind::Case)
            .collect();
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].start_line, 1);
        assert_eq!(cases[0].end_line, 5);
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
        let regions = detect_folds(sql);

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
        let regions = detect_folds(sql);
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

        let regions = detect_folds(sql);

        assert!(regions.iter().any(|r| r.kind == FoldKind::Comment));
        assert!(regions
            .iter()
            .any(|r| r.kind == FoldKind::Function || r.kind == FoldKind::Block));
    }
}
