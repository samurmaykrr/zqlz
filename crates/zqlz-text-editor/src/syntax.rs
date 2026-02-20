//! SQL Syntax highlighting using tree-sitter.
//!
//! This module provides syntax highlighting for SQL queries using tree-sitter
//! with the tree-sitter-sequel grammar. It parses SQL text into a syntax tree
//! and maps nodes to highlight styles.
//!
//! ## Supported Syntax Elements
//!
//! - **Keywords**: SELECT, FROM, WHERE, INSERT, UPDATE, DELETE, CREATE, DROP, etc.
//! - **Strings**: Single-quoted and double-quoted string literals
//! - **Comments**: -- line comments and /* block comments */
//! - **Numbers**: Integer and floating-point literals
//! - **Identifiers**: Table names, column names, aliases
//! - **Operators**: +, -, *, /, =, <, >, <=, >=, !=, etc.
//! - **Built-in functions**: COUNT, SUM, AVG, MAX, MIN, etc.
//!
//! ## Usage
//!
//! ```rust
//! use zqlz_text_editor::syntax::{SyntaxHighlighter, Highlight};
//!
//! let mut highlighter = SyntaxHighlighter::new().expect("SQL grammar should load");
//! let text = "SELECT name FROM users WHERE age > 18";
//! let highlights = highlighter.highlight(text);
//! ```

use std::collections::HashMap;
use tree_sitter::Node;
use tree_sitter::Parser;

/// SQL syntax highlighting colors
///
/// Each variant represents a different syntactic element in SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HighlightKind {
    /// SQL keywords (SELECT, FROM, WHERE, etc.)
    Keyword,
    /// String literals ('hello', "world")
    String,
    /// Comments (-- comment, /* comment */)
    Comment,
    /// Numeric literals (123, 3.14)
    Number,
    /// Identifiers (table names, column names)
    Identifier,
    /// Operators (+, -, *, /, =, <, >, etc.)
    Operator,
    /// Built-in functions (COUNT, SUM, etc.)
    Function,
    /// Punctuation (, ; ( ) etc.)
    Punctuation,
    /// Boolean literals (TRUE, FALSE)
    Boolean,
    /// NULL literal
    Null,
    /// Syntax errors (shown with red underline)
    Error,
    /// Default text
    Default,
}

impl Default for HighlightKind {
    fn default() -> Self {
        HighlightKind::Default
    }
}

/// A highlight range representing a styled segment of text
#[derive(Debug, Clone)]
pub struct Highlight {
    /// The start byte offset
    pub start: usize,
    /// The end byte offset
    pub end: usize,
    /// The kind of highlight
    pub kind: HighlightKind,
}

/// SQL syntax highlighter using tree-sitter
///
/// This struct provides syntax highlighting for SQL queries. It uses the
/// tree-sitter-sequel grammar to parse SQL text into a syntax tree, then
/// maps tree nodes to highlight styles.
///
/// # Performance
///
/// The highlighter is designed to be reused. Creating a new parser for each
/// highlight operation is expensive. Instead, create one `SyntaxHighlighter`
/// and reuse it for multiple highlight operations.
///
/// # Example
///
/// ```rust
/// use zqlz_text_editor::syntax::SyntaxHighlighter;
///
/// let mut highlighter = SyntaxHighlighter::new().expect("SQL grammar should load");
/// let text = "SELECT name FROM users";
/// let highlights = highlighter.highlight(text);
/// for h in &highlights {
///     println!("{:?}: {}..{} = {:?}", h.kind, h.start, h.end, &text[h.start..h.end]);
/// }
/// ```
pub struct SyntaxHighlighter {
    parser: Parser,
    /// Cached mapping from tree-sitter node types to highlight kinds
    node_type_map: HashMap<&'static str, HighlightKind>,
}

impl SyntaxHighlighter {
    /// Creates a new SQL syntax highlighter.
    ///
    /// This initializes the tree-sitter parser with the SQL (sequel) grammar.
    ///
    /// # Errors
    ///
    /// Returns an error if the tree-sitter-sequel grammar cannot be loaded.
    pub fn new() -> Result<Self, String> {
        let mut parser = Parser::new();
        let language = tree_sitter::Language::new(tree_sitter_sequel::LANGUAGE);
        parser
            .set_language(&language)
            .map_err(|e| format!("Failed to load SQL grammar: {}", e))?;

        // Map tree-sitter-sequel node type names to highlight kinds.
        //
        // The sequel grammar uses `keyword_*` prefixed nodes for SQL keywords
        // (e.g., `keyword_select`, `keyword_from`). Other important node types:
        //   - `comment`    — line/block comments
        //   - `invocation` — function calls (COUNT(...), SUM(...), etc.)
        //   - `literal`    — handled dynamically in collect_highlights() based on text
        //   - `ERROR`      — tree-sitter error recovery nodes
        let mut node_type_map = HashMap::new();

        // Comments
        node_type_map.insert("comment", HighlightKind::Comment);
        node_type_map.insert("comment_statement", HighlightKind::Comment);

        // Function calls
        node_type_map.insert("invocation", HighlightKind::Function);

        // Errors
        node_type_map.insert("ERROR", HighlightKind::Error);

        // Identifiers (table names, column names, aliases)
        node_type_map.insert("identifier", HighlightKind::Identifier);

        // Boolean keyword literals get their own highlight kind
        node_type_map.insert("keyword_true", HighlightKind::Boolean);
        node_type_map.insert("keyword_false", HighlightKind::Boolean);
        node_type_map.insert("keyword_null", HighlightKind::Null);

        // All other `keyword_*` nodes are SQL keywords. We register a broad set
        // here; any keyword_ node not listed falls through in collect_highlights
        // to the prefix check.
        let keywords: &[&str] = &[
            "keyword_select",
            "keyword_from",
            "keyword_where",
            "keyword_and",
            "keyword_or",
            "keyword_not",
            "keyword_in",
            "keyword_like",
            "keyword_between",
            "keyword_insert",
            "keyword_into",
            "keyword_values",
            "keyword_update",
            "keyword_set",
            "keyword_delete",
            "keyword_create",
            "keyword_table",
            "keyword_drop",
            "keyword_alter",
            "keyword_index",
            "keyword_join",
            "keyword_left",
            "keyword_right",
            "keyword_inner",
            "keyword_outer",
            "keyword_full",
            "keyword_cross",
            "keyword_on",
            "keyword_group",
            "keyword_by",
            "keyword_having",
            "keyword_order",
            "keyword_asc",
            "keyword_desc",
            "keyword_limit",
            "keyword_offset",
            "keyword_distinct",
            "keyword_all",
            "keyword_union",
            "keyword_intersect",
            "keyword_except",
            "keyword_as",
            "keyword_case",
            "keyword_when",
            "keyword_then",
            "keyword_else",
            "keyword_end",
            "keyword_is",
            "keyword_exists",
            "keyword_with",
            "keyword_recursive",
            "keyword_over",
            "keyword_partition",
            "keyword_window",
            "keyword_begin",
            "keyword_commit",
            "keyword_rollback",
            "keyword_transaction",
            "keyword_view",
            "keyword_natural",
            "keyword_using",
            "keyword_lateral",
            "keyword_filter",
            "keyword_returning",
            "keyword_replace",
            "keyword_ignore",
            "keyword_if",
        ];
        for kw in keywords {
            node_type_map.insert(kw, HighlightKind::Keyword);
        }

        Ok(Self {
            parser,
            node_type_map,
        })
    }

    /// Highlights the given SQL text.
    ///
    /// This parses the text into a syntax tree and returns a list of highlight
    /// ranges representing different syntactic elements.
    ///
    /// # Arguments
    ///
    /// * `text` - The SQL text to highlight
    ///
    /// # Returns
    ///
    /// A vector of `Highlight` structs, each representing a styled range of text.
    /// The highlights are returned in order by their start position.
    ///
    /// # Example
    ///
    /// ```rust
    /// use zqlz_text_editor::syntax::{SyntaxHighlighter, HighlightKind};
    ///
    /// let mut highlighter = SyntaxHighlighter::new().unwrap();
    /// let text = "SELECT * FROM users WHERE name = 'John'";
    /// let highlights = highlighter.highlight(text);
    ///
    /// for h in &highlights {
    ///     if h.kind == HighlightKind::Keyword {
    ///         println!("Keyword: {}", &text[h.start..h.end]);
    ///     }
    /// }
    /// ```
    pub fn highlight(&mut self, text: &str) -> Vec<Highlight> {
        let tree = match self.parser.parse(text, None) {
            Some(t) => t,
            None => return Vec::new(),
        };

        let mut highlights = Vec::new();
        self.collect_highlights(tree.root_node(), text, &mut highlights);

        // Sort by start position
        highlights.sort_by_key(|h| h.start);

        // Merge overlapping ranges (prefer earlier/high-priority highlights)
        let merged = Self::merge_overlapping(&highlights);

        merged
    }

    /// Recursively collect highlights from the syntax tree.
    ///
    /// Handles the tree-sitter-sequel grammar's conventions:
    /// - All `keyword_*` nodes are SQL keywords
    /// - `literal` nodes cover strings, numbers, and boolean/null literals;
    ///   we inspect the text to determine the specific kind
    /// - Once a node is classified we do not recurse into its children to avoid
    ///   emitting duplicate or conflicting ranges for nested leaf nodes
    fn collect_highlights(&self, node: Node, text: &str, highlights: &mut Vec<Highlight>) {
        let node_kind = node.kind();

        // Classify this node, handling special cases first.
        let kind = if node_kind == "literal" {
            // The `literal` node covers strings, numbers, and keyword literals
            // (TRUE/FALSE/NULL). Look at the raw text to distinguish them.
            let literal_text = &text[node.start_byte()..node.end_byte()];
            if literal_text.starts_with('\'') || literal_text.starts_with('"') {
                HighlightKind::String
            } else if literal_text
                .chars()
                .next()
                .map(|c| c.is_ascii_digit())
                .unwrap_or(false)
            {
                HighlightKind::Number
            } else {
                // Boolean/null literals have keyword_ children handled below
                HighlightKind::Default
            }
        } else if let Some(&mapped) = self.node_type_map.get(node_kind) {
            mapped
        } else if node_kind.starts_with("keyword_") {
            // Any keyword_ node not explicitly mapped is still a SQL keyword.
            HighlightKind::Keyword
        } else {
            HighlightKind::Default
        };

        if kind != HighlightKind::Default {
            highlights.push(Highlight {
                start: node.start_byte(),
                end: node.end_byte(),
                kind,
            });
            // Do not recurse once we've coloured this node — its children would
            // produce overlapping ranges that confuse the merge step.
            return;
        }

        // The sequel grammar uses anonymous terminal tokens for operators rather
        // than named node types, so they won't be caught by the node_type_map above.
        // Detect them by inspecting unnamed leaf nodes directly.
        if !node.is_named() && node.child_count() == 0 {
            let text_slice = &text[node.start_byte()..node.end_byte()];
            match text_slice {
                "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" | "+" | "-" | "*" | "/" | "%" | "^"
                | "||" | "&" | "|" | "~" => {
                    highlights.push(Highlight {
                        start: node.start_byte(),
                        end: node.end_byte(),
                        kind: HighlightKind::Operator,
                    });
                    return;
                }
                _ => {}
            }
        }

        // Recurse into children for unclassified container nodes.
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.collect_highlights(child, text, highlights);
        }
    }

    /// Merge overlapping highlight ranges
    ///
    /// When ranges overlap, the earlier range takes precedence.
    fn merge_overlapping(highlights: &[Highlight]) -> Vec<Highlight> {
        if highlights.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::new();
        let mut current = highlights[0].clone();

        for next in highlights.iter().skip(1) {
            if next.start >= current.end {
                // No overlap, push current and start new
                result.push(current);
                current = next.clone();
            } else {
                // Overlap - keep current (earlier takes precedence)
                // Extend current to cover the union if needed
                if next.end > current.end {
                    current.end = next.end;
                }
            }
        }

        result.push(current);
        result
    }

    /// Get the highlight kind for a specific position in the text.
    ///
    /// This is useful for getting the highlight at the cursor position.
    ///
    /// # Arguments
    ///
    /// * `text` - The SQL text
    /// * `offset` - The byte offset to query
    ///
    /// # Returns
    ///
    /// The highlight kind at the given position, or `HighlightKind::Default`.
    pub fn highlight_at(&mut self, text: &str, offset: usize) -> HighlightKind {
        let highlights = self.highlight(text);

        for h in highlights {
            if offset >= h.start && offset < h.end {
                return h.kind;
            }
        }

        HighlightKind::Default
    }

    /// Get all error highlights from the text.
    ///
    /// This is useful for rendering diagnostic squiggles for syntax errors.
    ///
    /// # Arguments
    ///
    /// * `text` - The SQL text
    ///
    /// # Returns
    ///
    /// A vector of `Highlight` structs representing error ranges.
    pub fn get_errors(&mut self, text: &str) -> Vec<Highlight> {
        self.highlight(text)
            .into_iter()
            .filter(|h| h.kind == HighlightKind::Error)
            .collect()
    }
}

impl Default for SyntaxHighlighter {
    fn default() -> Self {
        Self::new().expect("Failed to create SyntaxHighlighter")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_highlighter_creation() {
        let highlighter = SyntaxHighlighter::new();
        assert!(highlighter.is_ok());
    }

    #[test]
    fn test_highlight_keywords() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = "SELECT name FROM users";
        let highlights = highlighter.highlight(text);

        // Find keyword highlights
        let keyword_highlights: Vec<_> = highlights
            .iter()
            .filter(|h| h.kind == HighlightKind::Keyword)
            .collect();

        // Should have SELECT, FROM as keywords
        assert!(!keyword_highlights.is_empty());
    }

    #[test]
    fn test_highlight_string() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = "SELECT * FROM users WHERE name = 'John'";
        let highlights = highlighter.highlight(text);

        let string_highlights: Vec<_> = highlights
            .iter()
            .filter(|h| h.kind == HighlightKind::String)
            .collect();

        assert!(!string_highlights.is_empty());
    }

    #[test]
    fn test_highlight_comment() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = "SELECT * FROM users -- this is a comment";
        let highlights = highlighter.highlight(text);

        let comment_highlights: Vec<_> = highlights
            .iter()
            .filter(|h| h.kind == HighlightKind::Comment)
            .collect();

        assert!(!comment_highlights.is_empty());
    }

    #[test]
    fn test_highlight_number() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = "SELECT * FROM users WHERE age > 18";
        let highlights = highlighter.highlight(text);

        let number_highlights: Vec<_> = highlights
            .iter()
            .filter(|h| h.kind == HighlightKind::Number)
            .collect();

        assert!(!number_highlights.is_empty());
    }

    #[test]
    fn test_highlight_at() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = "SELECT name";

        // "SELECT" starts at 0
        assert_eq!(highlighter.highlight_at(text, 0), HighlightKind::Keyword);
        // "name" is at position 7
        assert_eq!(highlighter.highlight_at(text, 7), HighlightKind::Identifier);
    }

    #[test]
    fn test_highlight_empty() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let highlights = highlighter.highlight("");

        assert!(highlights.is_empty());
    }

    #[test]
    fn test_highlight_complex_query() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = r#"
            SELECT
                u.id,
                u.name,
                COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.age > 18
            GROUP BY u.id, u.name
            HAVING COUNT(o.id) > 5
            ORDER BY order_count DESC
            LIMIT 10
        "#;

        let highlights = highlighter.highlight(text);

        // Should have keywords
        let keywords: Vec<_> = highlights
            .iter()
            .filter(|h| h.kind == HighlightKind::Keyword)
            .collect();
        assert!(!keywords.is_empty());

        // Should have functions
        let functions: Vec<_> = highlights
            .iter()
            .filter(|h| h.kind == HighlightKind::Function)
            .collect();
        assert!(!functions.is_empty());

        // Should have numbers
        let numbers: Vec<_> = highlights
            .iter()
            .filter(|h| h.kind == HighlightKind::Number)
            .collect();
        assert!(!numbers.is_empty());
    }
}
