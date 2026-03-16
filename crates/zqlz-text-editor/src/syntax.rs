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

use crate::buffer::Change;
use ropey::Rope;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tree_sitter::Node;
use tree_sitter::Parser;

/// SQL syntax highlighting colors
///
/// Each variant represents a different syntactic element in SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
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
    #[default]
    Default,
}

/// A highlight range representing a styled segment of text
#[derive(Debug, Clone, PartialEq, Eq)]
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
/// We currently reparse from scratch on each refresh.
///
/// Tree-sitter can reuse an earlier parse tree, but only after the old tree has
/// been updated with precise edit deltas. The editor pipeline does not currently
/// plumb those edits into the highlighter, and reusing a stale tree causes
/// highlights to drift or disappear while typing. We therefore prefer a correct
/// full reparse until incremental edit application is implemented.
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
    function_like_nodes: HashSet<&'static str>,
    identifier_like_nodes: HashSet<&'static str>,
    punctuation_like_nodes: HashSet<&'static str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SyntaxRefreshStrategy {
    Disabled,
    FullDocument,
    VisibleRange(std::ops::Range<usize>),
}

impl SyntaxRefreshStrategy {
    pub fn into_visible_range(self) -> Option<std::ops::Range<usize>> {
        match self {
            Self::VisibleRange(byte_range) => Some(byte_range),
            Self::Disabled | Self::FullDocument => None,
        }
    }
}

/// Immutable syntax state used by rendering and async refinement.
#[derive(Debug, Clone)]
pub struct SyntaxSnapshot {
    highlights: Arc<Vec<Highlight>>,
    revision: usize,
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
        node_type_map.insert("function_name", HighlightKind::Function);

        // Errors
        node_type_map.insert("ERROR", HighlightKind::Error);

        // Identifiers (table names, column names, aliases)
        node_type_map.insert("identifier", HighlightKind::Identifier);
        node_type_map.insert("object_reference", HighlightKind::Identifier);
        node_type_map.insert("all_fields", HighlightKind::Identifier);

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

        let function_like_nodes = HashSet::from(["invocation", "function_name"]);

        let identifier_like_nodes = HashSet::from(["identifier", "object_reference", "all_fields"]);

        let punctuation_like_nodes = HashSet::from(["(", ")", "[", "]", "{", "}", ",", ";", "."]);

        Ok(Self {
            parser,
            node_type_map,
            function_like_nodes,
            identifier_like_nodes,
            punctuation_like_nodes,
        })
    }

    /// Discards the cached parse tree, forcing a full re-parse on the next
    /// call to `highlight()`.
    ///
    /// This is currently a no-op because we intentionally avoid tree reuse
    /// until edit deltas are applied correctly.
    pub fn invalidate_tree(&mut self) {}

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

        Self::normalize_highlights(highlights)
    }

    pub fn highlight_rope(&mut self, text: &Rope) -> Vec<Highlight> {
        let tree = match self.parser.parse_with_options(
            &mut move |offset, _| {
                if offset >= text.len_bytes() {
                    ""
                } else {
                    let (chunk, chunk_byte_index, _, _) = text.chunk_at_byte(offset);
                    &chunk[offset - chunk_byte_index..]
                }
            },
            None,
            None,
        ) {
            Some(tree) => tree,
            None => return Vec::new(),
        };

        let mut highlights = Vec::new();
        self.collect_highlights_in_rope(tree.root_node(), text, 0, &mut highlights);

        Self::normalize_highlights(highlights)
    }

    pub fn highlight_rope_range(
        &mut self,
        text: &Rope,
        byte_range: std::ops::Range<usize>,
    ) -> Vec<Highlight> {
        let clamped_start = byte_range.start.min(text.len_bytes());
        let clamped_end = byte_range.end.min(text.len_bytes());
        if clamped_start >= clamped_end {
            return Vec::new();
        }

        let char_start = text.byte_to_char(clamped_start);
        let char_end = text.byte_to_char(clamped_end);
        let local_text = text.slice(char_start..char_end).to_string();

        self.highlight(&local_text)
            .into_iter()
            .map(|highlight| Highlight {
                start: highlight.start + clamped_start,
                end: highlight.end + clamped_start,
                kind: highlight.kind,
            })
            .collect()
    }

    pub fn snapshot(&mut self, text: &str, revision: usize) -> SyntaxSnapshot {
        SyntaxSnapshot {
            highlights: Arc::new(self.highlight(text)),
            revision,
        }
    }

    pub fn snapshot_rope(&mut self, text: &Rope, revision: usize) -> SyntaxSnapshot {
        SyntaxSnapshot {
            highlights: Arc::new(self.highlight_rope(text)),
            revision,
        }
    }

    pub fn snapshot_rope_for_range(
        &mut self,
        text: &Rope,
        revision: usize,
        byte_range: std::ops::Range<usize>,
    ) -> SyntaxSnapshot {
        SyntaxSnapshot {
            highlights: Arc::new(self.highlight_rope_range(text, byte_range)),
            revision,
        }
    }

    fn normalize_highlights(mut highlights: Vec<Highlight>) -> Vec<Highlight> {
        highlights.retain(|highlight| highlight.start < highlight.end);
        highlights.sort_by(|left, right| {
            left.start
                .cmp(&right.start)
                .then_with(|| {
                    let left_len = left.end.saturating_sub(left.start);
                    let right_len = right.end.saturating_sub(right.start);
                    left_len.cmp(&right_len)
                })
                .then_with(|| {
                    Self::highlight_rank(left.kind).cmp(&Self::highlight_rank(right.kind))
                })
        });

        let mut normalized = Vec::with_capacity(highlights.len());
        for highlight in highlights {
            if normalized.iter().any(|existing: &Highlight| {
                existing.kind == highlight.kind
                    && existing.start <= highlight.start
                    && existing.end >= highlight.end
            }) {
                continue;
            }
            normalized.push(highlight);
        }

        merge_adjacent_same_kind(&normalized)
    }

    fn highlight_rank(kind: HighlightKind) -> u8 {
        match kind {
            HighlightKind::Error => 0,
            HighlightKind::Keyword => 1,
            HighlightKind::Function => 2,
            HighlightKind::String => 3,
            HighlightKind::Number => 4,
            HighlightKind::Boolean => 5,
            HighlightKind::Null => 6,
            HighlightKind::Comment => 7,
            HighlightKind::Operator => 8,
            HighlightKind::Punctuation => 9,
            HighlightKind::Identifier => 10,
            HighlightKind::Default => 11,
        }
    }

    fn classify_non_literal_node(&self, node: Node) -> HighlightKind {
        let node_kind = node.kind();

        if node_kind == "identifier" && self.identifier_is_function_name(node) {
            return HighlightKind::Function;
        }

        if let Some(&mapped) = self.node_type_map.get(node_kind) {
            return mapped;
        }

        if node_kind.starts_with("keyword_") {
            return HighlightKind::Keyword;
        }

        if self.function_like_nodes.contains(node_kind) {
            return HighlightKind::Function;
        }

        if self.identifier_like_nodes.contains(node_kind) {
            return HighlightKind::Identifier;
        }

        HighlightKind::Default
    }

    fn identifier_is_function_name(&self, node: Node) -> bool {
        let Some(parent) = node.parent() else {
            return false;
        };

        if self.function_like_nodes.contains(parent.kind()) {
            return true;
        }

        if parent.kind() != "object_reference" {
            return false;
        }

        let Some(function_name) = parent.child_by_field_name("name") else {
            return false;
        };
        if function_name.start_byte() != node.start_byte()
            || function_name.end_byte() != node.end_byte()
        {
            return false;
        }

        let Some(grandparent) = parent.parent() else {
            return false;
        };
        self.function_like_nodes.contains(grandparent.kind())
    }

    fn classify_literal_text(text: &str) -> HighlightKind {
        if text.starts_with('\'') || text.starts_with('"') {
            HighlightKind::String
        } else if text.eq_ignore_ascii_case("true") || text.eq_ignore_ascii_case("false") {
            HighlightKind::Boolean
        } else if text.eq_ignore_ascii_case("null") {
            HighlightKind::Null
        } else if text
            .chars()
            .next()
            .map(|character| character.is_ascii_digit())
            .unwrap_or(false)
        {
            HighlightKind::Number
        } else {
            HighlightKind::Default
        }
    }

    fn classify_terminal_text(&self, text: &str) -> HighlightKind {
        match text {
            "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" | "+" | "-" | "*" | "/" | "%" | "^"
            | "||" | "&" | "|" | "~" | ":=" => HighlightKind::Operator,
            _ if self.punctuation_like_nodes.contains(text) => HighlightKind::Punctuation,
            _ => HighlightKind::Default,
        }
    }

    fn should_emit_highlight(node: Node, kind: HighlightKind) -> bool {
        if kind == HighlightKind::Default {
            return false;
        }

        match kind {
            HighlightKind::Function => node.child_count() == 0,
            HighlightKind::Identifier => node.child_count() == 0 || node.kind() == "all_fields",
            HighlightKind::Comment => true,
            _ => true,
        }
    }

    /// Recursively collect highlights from the syntax tree.
    fn collect_highlights(&self, node: Node, text: &str, highlights: &mut Vec<Highlight>) {
        let node_kind = node.kind();

        let kind = if node_kind == "literal" {
            let start = node.start_byte();
            let end = node.end_byte();
            if start > text.len()
                || end > text.len()
                || !text.is_char_boundary(start)
                || !text.is_char_boundary(end)
            {
                HighlightKind::Default
            } else {
                Self::classify_literal_text(&text[start..end])
            }
        } else {
            self.classify_non_literal_node(node)
        };

        if Self::should_emit_highlight(node, kind) {
            highlights.push(Highlight {
                start: node.start_byte(),
                end: node.end_byte(),
                kind,
            });
        }

        if node.child_count() == 0 {
            let start = node.start_byte();
            let end = node.end_byte();
            if start <= text.len()
                && end <= text.len()
                && text.is_char_boundary(start)
                && text.is_char_boundary(end)
            {
                let text_slice = &text[start..end];
                let terminal_kind = self.classify_terminal_text(text_slice);
                if terminal_kind != HighlightKind::Default {
                    highlights.push(Highlight {
                        start,
                        end,
                        kind: terminal_kind,
                    });
                }
            }
            return;
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.collect_highlights(child, text, highlights);
        }
    }

    fn collect_highlights_in_rope(
        &self,
        node: Node,
        text: &Rope,
        base_offset: usize,
        highlights: &mut Vec<Highlight>,
    ) {
        let node_kind = node.kind();

        let kind = if node_kind == "literal" {
            Self::classify_literal_from_rope(text, node.start_byte(), node.end_byte())
                .unwrap_or(HighlightKind::Default)
        } else {
            self.classify_non_literal_node(node)
        };

        if Self::should_emit_highlight(node, kind) {
            highlights.push(Highlight {
                start: base_offset + node.start_byte(),
                end: base_offset + node.end_byte(),
                kind,
            });
        }

        if node.child_count() == 0 {
            if let Some(text_slice) =
                Self::rope_text_range(text, node.start_byte(), node.end_byte())
            {
                let terminal_kind = self.classify_terminal_text(text_slice.as_str());
                if terminal_kind != HighlightKind::Default {
                    highlights.push(Highlight {
                        start: base_offset + node.start_byte(),
                        end: base_offset + node.end_byte(),
                        kind: terminal_kind,
                    });
                }
            }
            return;
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.collect_highlights_in_rope(child, text, base_offset, highlights);
        }
    }

    fn classify_literal_from_rope(text: &Rope, start: usize, end: usize) -> Option<HighlightKind> {
        let literal_text = Self::rope_text_range(text, start, end)?;
        Some(Self::classify_literal_text(&literal_text))
    }

    fn rope_text_range(text: &Rope, start: usize, end: usize) -> Option<String> {
        if start > end || end > text.len_bytes() {
            return None;
        }

        let char_start = text.byte_to_char(start);
        let char_end = text.byte_to_char(end);
        Some(text.slice(char_start..char_end).to_string())
    }

    fn merge_overlapping(highlights: &[Highlight]) -> Vec<Highlight> {
        Self::normalize_highlights(highlights.to_vec())
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
}

impl SyntaxSnapshot {
    pub fn new(highlights: Vec<Highlight>, revision: usize) -> Self {
        Self {
            highlights: Arc::new(highlights),
            revision,
        }
    }

    pub fn empty(revision: usize) -> Self {
        Self::new(Vec::new(), revision)
    }

    pub fn revision(&self) -> usize {
        self.revision
    }

    pub fn highlights(&self) -> Arc<Vec<Highlight>> {
        self.highlights.clone()
    }

    pub fn interpolate(&self, changes: &[Change], next_revision: usize) -> Self {
        if changes.is_empty() {
            return Self {
                highlights: self.highlights(),
                revision: next_revision,
            };
        }

        let mut highlights = self.highlights.as_ref().clone();
        for change in changes {
            highlights = interpolate_highlights_for_change(&highlights, change);
        }

        Self::new(highlights, next_revision)
    }
}

fn interpolate_highlights_for_change(highlights: &[Highlight], change: &Change) -> Vec<Highlight> {
    let replaced_start = change.offset;
    let replaced_end = change.offset + change.old_text.len();
    let inserted_len = change.new_text.len();
    let old_len = change.old_text.len();
    let delta = inserted_len as isize - old_len as isize;

    let mut next = Vec::with_capacity(highlights.len());
    for highlight in highlights {
        if highlight.end <= replaced_start {
            next.push(highlight.clone());
            continue;
        }

        if highlight.start >= replaced_end {
            next.push(shift_highlight(highlight, delta));
            continue;
        }

        if highlight.start < replaced_start {
            next.push(Highlight {
                start: highlight.start,
                end: replaced_start,
                kind: highlight.kind,
            });
        }

        if highlight.end > replaced_end {
            let shifted_start = ((replaced_start as isize) + inserted_len as isize).max(0) as usize;
            let shifted_end = ((highlight.end as isize) + delta).max(0) as usize;
            if shifted_start < shifted_end {
                next.push(Highlight {
                    start: shifted_start,
                    end: shifted_end,
                    kind: highlight.kind,
                });
            }
        }
    }

    next.retain(|highlight| highlight.start < highlight.end);
    next.sort_by_key(|highlight| highlight.start);
    merge_adjacent_same_kind(&SyntaxHighlighter::merge_overlapping(&next))
}

fn shift_highlight(highlight: &Highlight, delta: isize) -> Highlight {
    Highlight {
        start: ((highlight.start as isize) + delta).max(0) as usize,
        end: ((highlight.end as isize) + delta).max(0) as usize,
        kind: highlight.kind,
    }
}

fn merge_adjacent_same_kind(highlights: &[Highlight]) -> Vec<Highlight> {
    if highlights.is_empty() {
        return Vec::new();
    }

    let mut merged = Vec::with_capacity(highlights.len());
    let mut current = highlights[0].clone();

    for next in highlights.iter().skip(1) {
        if current.kind == next.kind && current.end == next.start {
            current.end = next.end;
        } else {
            merged.push(current);
            current = next.clone();
        }
    }

    merged.push(current);
    merged
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
    fn test_highlight_lowercase_keywords() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = "select name from users";
        let highlights = highlighter.highlight(text);

        let keyword_highlights: Vec<_> = highlights
            .iter()
            .filter(|highlight| highlight.kind == HighlightKind::Keyword)
            .collect();

        assert!(
            keyword_highlights
                .iter()
                .any(|highlight| &text[highlight.start..highlight.end] == "select")
        );
        assert!(
            keyword_highlights
                .iter()
                .any(|highlight| &text[highlight.start..highlight.end] == "from")
        );
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

    #[test]
    fn test_function_and_punctuation_tokens_are_not_flattened() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = "SELECT count(*) FROM xya";
        let highlights = highlighter.highlight(text);

        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Keyword
                && &text[highlight.start..highlight.end] == "SELECT"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Function
                && &text[highlight.start..highlight.end] == "count"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Punctuation
                && &text[highlight.start..highlight.end] == "("
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Operator
                && &text[highlight.start..highlight.end] == "*"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Punctuation
                && &text[highlight.start..highlight.end] == ")"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Keyword
                && &text[highlight.start..highlight.end] == "FROM"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Identifier
                && &text[highlight.start..highlight.end] == "xya"
        }));
    }

    #[test]
    fn test_object_reference_keeps_identifiers_separate() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = "SELECT schema_name.table_name FROM schema_name.table_name";
        let highlights = highlighter.highlight(text);

        let identifier_texts = highlights
            .iter()
            .filter(|highlight| highlight.kind == HighlightKind::Identifier)
            .map(|highlight| &text[highlight.start..highlight.end])
            .collect::<Vec<_>>();

        assert!(identifier_texts.contains(&"schema_name"));
        assert!(identifier_texts.contains(&"table_name"));
        assert!(!identifier_texts.contains(&"schema_name.table_name"));
        assert!(
            highlights
                .iter()
                .any(|highlight| highlight.kind == HighlightKind::Punctuation
                    && &text[highlight.start..highlight.end] == ".")
        );
    }

    #[test]
    fn test_boolean_and_null_literals_are_classified_from_literal_nodes() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = "SELECT TRUE, false, NULL";
        let highlights = highlighter.highlight(text);

        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Boolean
                && &text[highlight.start..highlight.end] == "TRUE"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Boolean
                && &text[highlight.start..highlight.end] == "false"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Null && &text[highlight.start..highlight.end] == "NULL"
        }));
    }

    #[test]
    fn test_syntax_snapshot_interpolate_shifts_unaffected_regions() {
        let snapshot = SyntaxSnapshot::new(
            vec![
                Highlight {
                    start: 0,
                    end: 6,
                    kind: HighlightKind::Keyword,
                },
                Highlight {
                    start: 12,
                    end: 17,
                    kind: HighlightKind::Identifier,
                },
            ],
            1,
        );

        let interpolated = snapshot.interpolate(&[Change::insert(7, "very ")], 2);
        let highlights = interpolated.highlights();

        assert_eq!(highlights[0].start, 0);
        assert_eq!(highlights[0].end, 6);
        assert_eq!(highlights[1].start, 17);
        assert_eq!(highlights[1].end, 22);
    }

    #[test]
    fn test_syntax_snapshot_interpolate_trims_overlapping_ranges() {
        let snapshot = SyntaxSnapshot::new(
            vec![Highlight {
                start: 0,
                end: 10,
                kind: HighlightKind::Keyword,
            }],
            1,
        );

        let interpolated = snapshot.interpolate(&[Change::delete(4, "XX")], 2);
        let highlights = interpolated.highlights();

        assert_eq!(highlights.len(), 1);
        assert_eq!(highlights[0].start, 0);
        assert_eq!(highlights[0].end, 8);
    }

    #[test]
    fn test_highlight_rope_matches_string_highlighting() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = Rope::from_str("SELECT name FROM users WHERE id = 1");

        assert_eq!(
            highlighter.highlight_rope(&text),
            highlighter.highlight("SELECT name FROM users WHERE id = 1")
        );
    }

    #[test]
    fn test_snapshot_rope_for_range_shifts_offsets_into_buffer_coordinates() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = Rope::from_str("alpha\nSELECT value\nomega");
        let snapshot = highlighter.snapshot_rope_for_range(&text, 5, 6..18);

        assert_eq!(snapshot.revision(), 5);
        assert!(snapshot.highlights().iter().any(|highlight| {
            highlight.start == 6 && highlight.end == 12 && highlight.kind == HighlightKind::Keyword
        }));
        assert!(
            snapshot
                .highlights()
                .iter()
                .all(|highlight| highlight.start >= 6)
        );
        assert!(
            snapshot
                .highlights()
                .iter()
                .all(|highlight| highlight.end <= 18)
        );
    }

    #[test]
    fn test_highlight_rope_handles_count_star_with_quoted_identifier() {
        let query = "SELECT COUNT(*) FROM \"_database_functions\"";
        let rope = Rope::from_str(query);
        let mut highlighter = SyntaxHighlighter::new().unwrap();

        let string_highlights = highlighter.highlight(query);
        highlighter.invalidate_tree();
        let rope_highlights = highlighter.highlight_rope(&rope);

        assert_eq!(rope_highlights, string_highlights);
        assert!(rope_highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Keyword
                && &query[highlight.start..highlight.end] == "SELECT"
        }));
        assert!(rope_highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Keyword
                && &query[highlight.start..highlight.end] == "FROM"
        }));
        assert!(rope_highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Function
                && &query[highlight.start..highlight.end] == "COUNT"
        }));
    }

    #[test]
    fn test_incremental_typing_preserves_sql_highlights() {
        let query = "SELECT COUNT(*) FROM \"_database_functions\"";
        let mut highlighter = SyntaxHighlighter::new().unwrap();

        for end in 1..=query.len() {
            let prefix = &query[..end];
            let _ = highlighter.highlight(prefix);
        }

        let incremental_highlights = highlighter.highlight(query);

        highlighter.invalidate_tree();
        let fresh_highlights = highlighter.highlight(query);

        assert_eq!(incremental_highlights, fresh_highlights);
        assert!(incremental_highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Keyword
                && &query[highlight.start..highlight.end] == "SELECT"
        }));
        assert!(incremental_highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Keyword
                && &query[highlight.start..highlight.end] == "FROM"
        }));
    }
}
