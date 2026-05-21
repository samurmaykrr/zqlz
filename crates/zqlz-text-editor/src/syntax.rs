//! SQL Syntax highlighting using tree-sitter.
//!
//! This module provides syntax highlighting for SQL queries using tree-sitter
//! with the tree-sitter-sequel grammar. It parses SQL text into a syntax tree
//! and maps nodes to highlight styles.
//!
//! ## Supported Syntax Elements
//!
//! - **Keywords**: SELECT, FROM, WHERE, INSERT, UPDATE, DELETE, CREATE, DROP, etc.
//! - **Strings**: Single-quoted string literals
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
    sync::{Arc, LazyLock},
    time::Instant,
};
use tree_sitter::{Node, Parser, Query, QueryCursor, StreamingIterator};
use zqlz_core::dialect_config::SyntaxTermsConfig;
use zqlz_core::{
    DriverSyntaxOverlayKind, HighlightQueryLanguage, SqlProtectedRange, SqlProtectedRangeKind,
    SyntaxDriverCapabilities, SyntaxOverlayMode, TreeSitterGrammar,
    driver_syntax_overlay_tokens_for_capabilities, get_highlight_query_language,
    get_syntax_driver_capabilities, get_syntax_term_profile, get_tree_sitter_grammar,
    normalize_syntax_profile, paints_quoted_identifier, sql_parameter_placeholders_with_ranges,
    sql_protected_range_at, sql_protected_ranges, supported_syntax_profiles,
};

mod syntax_render;
mod syntax_rope;

pub use syntax_render::{highlight_render_rank, render_highlight_runs};
#[cfg(test)]
use syntax_rope::{ROPE_OVERLAY_CONTEXT_BYTES, rope_overlay_context_start};
use syntax_rope::{
    RopeTextProvider, clamp_rope_byte_range, clip_highlights_to_range, rope_byte_range_to_string,
    rope_line_covering_byte_range, sql_protected_ranges_for_rope_overlay,
};

/// SQL syntax highlighting colors
///
/// Each variant represents a different syntactic element in SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum HighlightKind {
    /// SQL keywords (SELECT, FROM, WHERE, etc.)
    Keyword,
    /// String literals ('hello')
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
    /// Query bind parameters and placeholders (:name, @name, $1, ?)
    Parameter,
    /// Data types and dialect-specific type names (UUID, JSONB, UInt64, etc.)
    Type,
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

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct HighlightCoverage {
    pub styled_non_ws_bytes: usize,
    pub non_ws_bytes: usize,
    pub percent: f64,
    pub gaps: Vec<HighlightGap>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct HighlightGap {
    pub start: usize,
    pub end: usize,
    pub text: String,
}

pub type ExpectedHighlightToken = (&'static str, HighlightKind);

#[derive(Debug, Clone, Copy)]
pub struct SyntaxQualityFixture {
    pub profile: &'static str,
    pub text: &'static str,
    pub expected_tokens: &'static [ExpectedHighlightToken],
    pub rejected_tokens: &'static [ExpectedHighlightToken],
}

pub fn syntax_quality_fixtures() -> &'static [SyntaxQualityFixture] {
    &[
        SyntaxQualityFixture {
            profile: "postgresql",
            text: r#"CREATE TABLE "categories" (
  "category_id" text(255) NOT NULL,
  payload JSONB DEFAULT '{}'::jsonb,
  owner_id UUID REFERENCES users(id),
  amount NUMERIC(10, 2) CHECK (amount > 0),
  updated_at TIMESTAMPTZ
);
SELECT * FROM "categories", LATERAL jsonb_each(payload) AS entry
WHERE owner_id = $1 AND payload ? 'seo';
SELECT row_number() OVER (PARTITION BY owner_id ORDER BY updated_at DESC) FROM "categories";
REINDEX INDEX CONCURRENTLY idx_categories_owner;"#,
            expected_tokens: &[
                ("CREATE", HighlightKind::Keyword),
                ("TABLE", HighlightKind::Keyword),
                ("\"categories\"", HighlightKind::Identifier),
                ("\"category_id\"", HighlightKind::Identifier),
                ("text", HighlightKind::Type),
                ("NOT", HighlightKind::Keyword),
                ("NULL", HighlightKind::Null),
                ("JSONB", HighlightKind::Type),
                ("DEFAULT", HighlightKind::Keyword),
                ("UUID", HighlightKind::Type),
                ("users", HighlightKind::Identifier),
                ("CHECK", HighlightKind::Keyword),
                ("NUMERIC", HighlightKind::Type),
                ("TIMESTAMPTZ", HighlightKind::Type),
                ("LATERAL", HighlightKind::Keyword),
                ("jsonb_each", HighlightKind::Function),
                ("row_number", HighlightKind::Function),
                ("OVER", HighlightKind::Keyword),
                ("PARTITION", HighlightKind::Keyword),
                ("DESC", HighlightKind::Keyword),
                ("$1", HighlightKind::Parameter),
                ("?", HighlightKind::Operator),
                ("'seo'", HighlightKind::String),
                ("REINDEX", HighlightKind::Keyword),
                ("INDEX", HighlightKind::Keyword),
                ("CONCURRENTLY", HighlightKind::Keyword),
            ],
            rejected_tokens: &[
                ("\"categories\"", HighlightKind::String),
                ("users", HighlightKind::Function),
            ],
        },
        SyntaxQualityFixture {
            profile: "mysql",
            text: r#"CREATE TABLE `orders` (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  status ENUM('new', 'paid') NOT NULL,
  notes LONGTEXT,
  total DECIMAL(12, 2),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
 ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
SELECT GROUP_CONCAT(status) FROM `orders` WHERE id BETWEEN 10 AND 20 AND id = ?;"#,
            expected_tokens: &[
                ("CREATE", HighlightKind::Keyword),
                ("`orders`", HighlightKind::Identifier),
                ("BIGINT", HighlightKind::Type),
                ("UNSIGNED", HighlightKind::Type),
                ("AUTO_INCREMENT", HighlightKind::Keyword),
                ("ENUM", HighlightKind::Type),
                ("'paid'", HighlightKind::String),
                ("LONGTEXT", HighlightKind::Type),
                ("DECIMAL", HighlightKind::Type),
                ("DATETIME", HighlightKind::Type),
                ("CURRENT_TIMESTAMP", HighlightKind::Keyword),
                ("ENGINE", HighlightKind::Keyword),
                ("CHARSET", HighlightKind::Keyword),
                ("COLLATE", HighlightKind::Keyword),
                ("BETWEEN", HighlightKind::Keyword),
                ("GROUP_CONCAT", HighlightKind::Function),
                ("?", HighlightKind::Parameter),
            ],
            rejected_tokens: &[("UNSIGNED", HighlightKind::Keyword)],
        },
        SyntaxQualityFixture {
            profile: "sqlite",
            text: r#"CREATE TABLE "categories" (
  "category_id" text(255) NOT NULL,
  "child_count" integer,
  "sort_order" real(14, 3),
  "metadata" text
);
ATTACH DATABASE 'analytics.db' AS analytics;
CREATE INDEX IF NOT EXISTS idx_categories_metadata ON "categories"(json_extract("metadata", '$.seo'));
SELECT json_extract("metadata", '$.seo') FROM "categories" WHERE "category_id" = :category_id;"#,
            expected_tokens: &[
                ("CREATE", HighlightKind::Keyword),
                ("\"categories\"", HighlightKind::Identifier),
                ("\"category_id\"", HighlightKind::Identifier),
                ("text", HighlightKind::Type),
                ("integer", HighlightKind::Type),
                ("real", HighlightKind::Type),
                ("ATTACH", HighlightKind::Keyword),
                ("DATABASE", HighlightKind::Keyword),
                ("INDEX", HighlightKind::Keyword),
                ("IF", HighlightKind::Keyword),
                ("EXISTS", HighlightKind::Keyword),
                ("idx_categories_metadata", HighlightKind::Identifier),
                ("json_extract", HighlightKind::Function),
                ("'$.seo'", HighlightKind::String),
                (":category_id", HighlightKind::Parameter),
            ],
            rejected_tokens: &[
                ("\"category_id\"", HighlightKind::String),
                ("\"categories\"", HighlightKind::Function),
            ],
        },
        SyntaxQualityFixture {
            profile: "duckdb",
            text: r#"COPY (
  SELECT LIST_VALUE(user_id) AS ids, MEDIAN(total) AS p50
  FROM read_parquet('orders.parquet')
  QUALIFY row_number() OVER (PARTITION BY user_id ORDER BY created_at) = 1
) TO 'orders.csv';
CREATE TABLE analytics.events (payload STRUCT(id UUID, tags LIST(TEXT)));"#,
            expected_tokens: &[
                ("COPY", HighlightKind::Keyword),
                ("LIST_VALUE", HighlightKind::Function),
                ("MEDIAN", HighlightKind::Function),
                ("read_parquet", HighlightKind::Function),
                ("QUALIFY", HighlightKind::Keyword),
                ("row_number", HighlightKind::Function),
                ("OVER", HighlightKind::Keyword),
                ("PARTITION", HighlightKind::Keyword),
                ("analytics", HighlightKind::Identifier),
                ("events", HighlightKind::Identifier),
                ("STRUCT", HighlightKind::Type),
                ("UUID", HighlightKind::Type),
                ("TEXT", HighlightKind::Type),
            ],
            rejected_tokens: &[
                ("OVER", HighlightKind::Function),
                ("events", HighlightKind::Function),
            ],
        },
        SyntaxQualityFixture {
            profile: "mssql",
            text: r#"SELECT TOP 10 [User Name], JSON_VALUE(payload, '$.id') AS id
FROM [dbo].[Events]
WITH (NOLOCK)
WHERE amount BETWEEN 1 AND 20
ORDER BY created_at OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;
DECLARE @id UNIQUEIDENTIFIER = NEWID();
PRINT CONVERT(NVARCHAR(36), @id);"#,
            expected_tokens: &[
                ("SELECT", HighlightKind::Keyword),
                ("TOP", HighlightKind::Keyword),
                ("[User Name]", HighlightKind::Identifier),
                ("JSON_VALUE", HighlightKind::Function),
                ("[dbo]", HighlightKind::Identifier),
                ("[Events]", HighlightKind::Identifier),
                ("WITH", HighlightKind::Keyword),
                ("NOLOCK", HighlightKind::Keyword),
                ("BETWEEN", HighlightKind::Keyword),
                ("DECLARE", HighlightKind::Keyword),
                ("OFFSET", HighlightKind::Keyword),
                ("FETCH", HighlightKind::Keyword),
                ("ROWS", HighlightKind::Keyword),
                ("ONLY", HighlightKind::Keyword),
                ("UNIQUEIDENTIFIER", HighlightKind::Type),
                ("NEWID", HighlightKind::Function),
                ("PRINT", HighlightKind::Keyword),
                ("CONVERT", HighlightKind::Function),
                ("NVARCHAR", HighlightKind::Type),
            ],
            rejected_tokens: &[],
        },
        SyntaxQualityFixture {
            profile: "clickhouse",
            text: r#"CREATE TABLE events (
  id UInt64,
  tags Array(String),
  created_at DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (id, created_at);
SELECT uniq(id), countIf(id > 0) FROM events FINAL;"#,
            expected_tokens: &[
                ("CREATE", HighlightKind::Keyword),
                ("UInt64", HighlightKind::Type),
                ("Array", HighlightKind::Type),
                ("String", HighlightKind::Type),
                ("DateTime64", HighlightKind::Type),
                ("ENGINE", HighlightKind::Keyword),
                ("MergeTree", HighlightKind::Type),
                ("PARTITION", HighlightKind::Keyword),
                ("toYYYYMM", HighlightKind::Function),
                ("ORDER", HighlightKind::Keyword),
                ("uniq", HighlightKind::Function),
                ("countIf", HighlightKind::Function),
                ("FINAL", HighlightKind::Keyword),
            ],
            rejected_tokens: &[("MergeTree", HighlightKind::Function)],
        },
        SyntaxQualityFixture {
            profile: "mongodb",
            text: r#"db.orders.aggregate([
  { "$match": { "status": "paid", "total": { "$gte": 100 } } },
  { "$group": { "_id": "$customerId", "revenue": { "$sum": "$total" } } },
  { "$sort": { "revenue": -1 } }
]);
db.orders.createIndex({ "customerId": 1 }, { unique: true })"#,
            expected_tokens: &[
                ("db", HighlightKind::Identifier),
                ("orders", HighlightKind::Identifier),
                ("aggregate", HighlightKind::Function),
                ("createIndex", HighlightKind::Function),
                ("$match", HighlightKind::Operator),
                ("$gte", HighlightKind::Operator),
                ("$group", HighlightKind::Operator),
                ("$sort", HighlightKind::Operator),
                ("$customerId", HighlightKind::Operator),
                ("$sum", HighlightKind::Operator),
                ("$total", HighlightKind::Operator),
                ("unique", HighlightKind::Identifier),
                ("true", HighlightKind::Boolean),
                ("100", HighlightKind::Number),
            ],
            rejected_tokens: &[],
        },
        SyntaxQualityFixture {
            profile: "redis",
            text: "HGETALL user:42\nJSON.SET user:42 $.profile \"Ada Lovelace\"\nJSON.GET user:42 $.profile\nSET user:42:name \"Ada Lovelace\" NX PX 30000 # cached profile",
            expected_tokens: &[
                ("HGETALL", HighlightKind::Keyword),
                ("JSON.SET", HighlightKind::Keyword),
                ("JSON.GET", HighlightKind::Keyword),
                ("SET", HighlightKind::Keyword),
                ("NX", HighlightKind::Keyword),
                ("PX", HighlightKind::Keyword),
                ("30000", HighlightKind::Number),
                ("\"Ada Lovelace\"", HighlightKind::String),
                ("# cached profile", HighlightKind::Comment),
            ],
            rejected_tokens: &[],
        },
    ]
}

pub fn highlight_coverage(text: &str, highlights: &[Highlight]) -> HighlightCoverage {
    let mut styled = vec![false; text.len()];
    for highlight in highlights {
        for byte in highlight.start..highlight.end {
            if let Some(styled_byte) = styled.get_mut(byte) {
                *styled_byte = true;
            }
        }
    }

    let mut styled_non_ws_bytes = 0usize;
    let mut non_ws_bytes = 0usize;
    let mut gaps = Vec::new();
    let mut gap_start = None;

    for (index, character) in text.char_indices() {
        let end = index + character.len_utf8();
        let is_non_ws = !character.is_whitespace();
        let is_styled = styled[index..end].iter().any(|byte| *byte);

        if is_non_ws {
            non_ws_bytes += character.len_utf8();
            if is_styled {
                styled_non_ws_bytes += character.len_utf8();
                if let Some(start) = gap_start.take() {
                    push_highlight_gap(text, start, index, &mut gaps);
                }
            } else {
                gap_start.get_or_insert(index);
            }
        } else if let Some(start) = gap_start.take() {
            push_highlight_gap(text, start, index, &mut gaps);
        }
    }

    if let Some(start) = gap_start {
        push_highlight_gap(text, start, text.len(), &mut gaps);
    }

    let percent = if non_ws_bytes == 0 {
        100.0
    } else {
        styled_non_ws_bytes as f64 * 100.0 / non_ws_bytes as f64
    };

    HighlightCoverage {
        styled_non_ws_bytes,
        non_ws_bytes,
        percent,
        gaps,
    }
}

fn push_highlight_gap(text: &str, start: usize, end: usize, gaps: &mut Vec<HighlightGap>) {
    if start < end {
        gaps.push(HighlightGap {
            start,
            end,
            text: text[start..end].escape_debug().to_string(),
        });
    }
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
    highlight_query: Option<Query>,
    language_profile: &'static str,
    syntax_capabilities: SyntaxDriverCapabilities,
    syntax_capabilities_override: Option<SyntaxDriverCapabilities>,
    /// Cached mapping from tree-sitter node types to highlight kinds
    node_type_map: HashMap<&'static str, HighlightKind>,
    function_like_nodes: HashSet<&'static str>,
    identifier_like_nodes: HashSet<&'static str>,
    punctuation_like_nodes: HashSet<&'static str>,
    dialect_terms: Arc<DialectTermSet>,
}

#[derive(Debug)]
struct DialectTermSet {
    keywords: HashSet<String>,
    functions: HashSet<String>,
    types: HashSet<String>,
}

#[derive(Debug, Default)]
struct HighlightQueryTermSet {
    keywords: HashSet<String>,
    functions: HashSet<String>,
    types: HashSet<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyntaxTermOverrides {
    pub keywords: Vec<String>,
    pub functions: Vec<String>,
    pub types: Vec<String>,
}

impl SyntaxTermOverrides {
    pub fn from_config(config: &SyntaxTermsConfig) -> Option<Self> {
        if config.keywords.is_empty() && config.functions.is_empty() && config.types.is_empty() {
            return None;
        }

        Some(Self {
            keywords: config.keywords.clone(),
            functions: config.functions.clone(),
            types: config.types.clone(),
        })
    }
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

#[derive(Debug, Clone)]
pub struct SyntaxHighlightPhaseTiming {
    pub name: &'static str,
    pub elapsed_ms: f64,
}

#[derive(Debug, Clone)]
pub struct SyntaxHighlightProfile {
    pub highlights: Vec<Highlight>,
    pub phases: Vec<SyntaxHighlightPhaseTiming>,
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
        let language = language_for_profile("sql");
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
        let language_profile = "sql";
        let highlight_query = Self::build_highlight_query(language_profile, &language);
        let syntax_capabilities = get_syntax_driver_capabilities(language_profile);

        Ok(Self {
            parser,
            highlight_query,
            language_profile,
            syntax_capabilities,
            syntax_capabilities_override: None,
            node_type_map,
            function_like_nodes,
            identifier_like_nodes,
            punctuation_like_nodes,
            dialect_terms: dialect_terms_for(language_profile),
        })
    }

    pub fn language_profile(&self) -> &'static str {
        self.language_profile
    }

    pub fn set_language_profile(&mut self, language_profile: &'static str) {
        let language_profile = normalize_language_profile(language_profile);
        if self.language_profile == language_profile {
            return;
        }

        match self.set_parser_language(language_profile) {
            Ok(language) => {
                self.highlight_query = Self::build_highlight_query(language_profile, &language);
            }
            Err(error) => {
                tracing::error!(%error, language_profile, "Failed to switch syntax parser language");
                return;
            }
        }

        self.language_profile = language_profile;
        if let Some(capabilities) = self.syntax_capabilities_override.clone() {
            self.apply_syntax_capabilities(capabilities);
        } else {
            self.syntax_capabilities = get_syntax_driver_capabilities(language_profile);
        }
        self.dialect_terms = dialect_terms_for(language_profile);
        self.invalidate_tree();
    }

    pub fn set_syntax_capabilities_override(&mut self, capabilities: SyntaxDriverCapabilities) {
        self.syntax_capabilities_override = Some(capabilities.clone());
        self.apply_syntax_capabilities(capabilities);
    }

    pub fn clear_syntax_capabilities_override(&mut self) {
        self.syntax_capabilities_override = None;
        let capabilities = get_syntax_driver_capabilities(self.language_profile);
        self.apply_syntax_capabilities(capabilities);
    }

    fn apply_syntax_capabilities(&mut self, capabilities: SyntaxDriverCapabilities) {
        match self.set_parser_capabilities(&capabilities) {
            Ok(language) => {
                self.highlight_query = Self::build_highlight_query_for(
                    capabilities.profile,
                    capabilities.highlight_query_language,
                    &language,
                );
                self.syntax_capabilities = capabilities;
                self.invalidate_tree();
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    profile = capabilities.profile,
                    "Failed to apply syntax capabilities"
                );
            }
        }
    }

    pub fn set_syntax_term_overrides(&mut self, overrides: SyntaxTermOverrides) {
        let mut terms = build_dialect_term_set(self.language_profile);
        terms.keywords.extend(
            overrides
                .keywords
                .into_iter()
                .map(|term| normalize_dialect_term(&term)),
        );
        terms.functions.extend(
            overrides
                .functions
                .into_iter()
                .map(|term| normalize_dialect_term(&term)),
        );
        terms.types.extend(
            overrides
                .types
                .into_iter()
                .map(|term| normalize_dialect_term(&term)),
        );
        self.dialect_terms = Arc::new(terms);
    }

    pub fn set_driver_syntax_terms(&mut self, overrides: SyntaxTermOverrides) {
        let mut terms = build_base_term_set(self.language_profile);
        terms.keywords.extend(
            overrides
                .keywords
                .into_iter()
                .map(|term| normalize_dialect_term(&term)),
        );
        terms.functions.extend(
            overrides
                .functions
                .into_iter()
                .map(|term| normalize_dialect_term(&term)),
        );
        terms.types.extend(
            overrides
                .types
                .into_iter()
                .map(|term| normalize_dialect_term(&term)),
        );
        self.dialect_terms = Arc::new(terms);
    }

    pub fn clear_syntax_term_overrides(&mut self) {
        self.dialect_terms = dialect_terms_for(self.language_profile);
    }

    fn set_parser_language(
        &mut self,
        language_profile: &str,
    ) -> Result<tree_sitter::Language, String> {
        let language = language_for_profile(language_profile);
        self.parser
            .set_language(&language)
            .map_err(|error| format!("Failed to load {language_profile} grammar: {error}"))?;
        Ok(language)
    }

    fn set_parser_capabilities(
        &mut self,
        capabilities: &SyntaxDriverCapabilities,
    ) -> Result<tree_sitter::Language, String> {
        let language = language_for_grammar(&capabilities.tree_sitter_grammar);
        self.parser.set_language(&language).map_err(|error| {
            format!(
                "Failed to load {:?} grammar for {}: {error}",
                capabilities.tree_sitter_grammar, capabilities.profile
            )
        })?;
        Ok(language)
    }

    fn build_highlight_query(
        language_profile: &str,
        language: &tree_sitter::Language,
    ) -> Option<Query> {
        let query_language =
            get_highlight_query_language(normalize_language_profile(language_profile));
        Self::build_highlight_query_for(language_profile, query_language, language)
    }

    fn build_highlight_query_for(
        language_profile: &str,
        query_language: HighlightQueryLanguage,
        language: &tree_sitter::Language,
    ) -> Option<Query> {
        let query_source = highlight_query_source_for_language(query_language);
        Query::new(language, query_source)
            .map_err(|error| {
                tracing::warn!(%error, language_profile, "Failed to load dialect highlight query");
                error
            })
            .ok()
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
        self.highlight_profiled(text).highlights
    }

    pub fn highlight_profiled(&mut self, text: &str) -> SyntaxHighlightProfile {
        let mut phases = Vec::new();
        if self.uses_command_overlays() {
            let mut highlights = Vec::new();
            let started = Instant::now();
            self.collect_driver_syntax_overlay_highlights(text, &mut highlights);
            phases.push(elapsed_phase("driver_overlay", started));
            let started = Instant::now();
            let highlights = Self::normalize_highlights(highlights);
            phases.push(elapsed_phase("normalize", started));
            return SyntaxHighlightProfile { highlights, phases };
        }

        if self.should_use_sql_overlay_only() {
            let mut highlights = Vec::new();
            let started = Instant::now();
            let protected_ranges = sql_protected_ranges(text);
            phases.push(elapsed_phase("protected_ranges", started));
            let started = Instant::now();
            self.collect_sql_protected_highlights_with_ranges(
                0,
                &protected_ranges,
                &mut highlights,
            );
            self.collect_parameter_highlights_with_ranges(
                text,
                0,
                &protected_ranges,
                &mut highlights,
            );
            self.collect_dialect_word_highlights_with_ranges(
                text,
                0,
                &protected_ranges,
                &mut highlights,
            );
            self.collect_driver_syntax_overlay_highlights(text, &mut highlights);
            phases.push(elapsed_phase("overlays", started));
            let started = Instant::now();
            let highlights = Self::normalize_highlights(highlights);
            phases.push(elapsed_phase("normalize", started));
            return SyntaxHighlightProfile { highlights, phases };
        }

        let started = Instant::now();
        let tree = match self.parser.parse(text, None) {
            Some(t) => t,
            None => {
                phases.push(elapsed_phase("parse", started));
                return SyntaxHighlightProfile {
                    highlights: Vec::new(),
                    phases,
                };
            }
        };
        phases.push(elapsed_phase("parse", started));

        let mut highlights = Vec::new();
        let started = Instant::now();
        self.collect_highlights(tree.root_node(), text, &mut highlights);
        phases.push(elapsed_phase("tree_walk", started));
        let started = Instant::now();
        self.collect_query_highlights(tree.root_node(), text.as_bytes(), &mut highlights);
        phases.push(elapsed_phase("query", started));
        let started = Instant::now();
        let protected_ranges = sql_protected_ranges(text);
        phases.push(elapsed_phase("protected_ranges", started));
        let started = Instant::now();
        self.collect_sql_protected_highlights_with_ranges(0, &protected_ranges, &mut highlights);
        self.collect_parameter_highlights_with_ranges(text, 0, &protected_ranges, &mut highlights);
        self.collect_dialect_word_highlights_with_ranges(
            text,
            0,
            &protected_ranges,
            &mut highlights,
        );
        self.collect_driver_syntax_overlay_highlights(text, &mut highlights);
        phases.push(elapsed_phase("overlays", started));

        let started = Instant::now();
        let highlights = Self::normalize_highlights(highlights);
        phases.push(elapsed_phase("normalize", started));

        SyntaxHighlightProfile { highlights, phases }
    }

    pub fn highlight_rope(&mut self, text: &Rope) -> Vec<Highlight> {
        if self.uses_command_overlays() {
            let mut highlights = Vec::new();
            self.collect_driver_syntax_overlay_highlights_in_rope(text, &mut highlights);
            return Self::normalize_highlights(highlights);
        }

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
        self.collect_query_highlights_in_rope(tree.root_node(), text, &mut highlights);
        let overlay_text = text.to_string();
        let overlay_protected_ranges = sql_protected_ranges(&overlay_text);
        self.collect_sql_protected_highlights_with_ranges(
            0,
            &overlay_protected_ranges,
            &mut highlights,
        );
        self.collect_parameter_highlights_with_ranges(
            &overlay_text,
            0,
            &overlay_protected_ranges,
            &mut highlights,
        );
        self.collect_dialect_word_highlights_with_ranges(
            &overlay_text,
            0,
            &overlay_protected_ranges,
            &mut highlights,
        );
        self.collect_driver_syntax_overlay_highlights_in_rope(text, &mut highlights);

        Self::normalize_highlights(highlights)
    }

    pub fn highlight_rope_range(
        &mut self,
        text: &Rope,
        byte_range: std::ops::Range<usize>,
    ) -> Vec<Highlight> {
        self.highlight_rope_range_profiled(text, byte_range)
            .highlights
    }

    pub fn highlight_rope_range_profiled(
        &mut self,
        text: &Rope,
        byte_range: std::ops::Range<usize>,
    ) -> SyntaxHighlightProfile {
        let mut phases = Vec::new();
        let byte_range = clamp_rope_byte_range(text, byte_range);
        if byte_range.start >= byte_range.end {
            return SyntaxHighlightProfile {
                highlights: Vec::new(),
                phases,
            };
        }
        if self.uses_command_overlays() {
            let mut highlights = Vec::new();
            let started = Instant::now();
            self.collect_driver_syntax_overlay_highlights_in_rope_range(
                text,
                byte_range.clone(),
                &mut highlights,
            );
            clip_highlights_to_range(&mut highlights, byte_range);
            phases.push(elapsed_phase("driver_overlay", started));
            let started = Instant::now();
            let highlights = Self::normalize_highlights(highlights);
            phases.push(elapsed_phase("normalize", started));
            return SyntaxHighlightProfile { highlights, phases };
        }

        if self.should_use_sql_overlay_only() {
            let mut highlights = Vec::new();
            let started = Instant::now();
            let overlay_range = rope_line_covering_byte_range(text, byte_range.clone());
            let overlay_text = rope_byte_range_to_string(text, overlay_range.clone());
            phases.push(elapsed_phase("slice", started));
            let started = Instant::now();
            let overlay_protected_ranges =
                sql_protected_ranges_for_rope_overlay(text, overlay_range.clone());
            phases.push(elapsed_phase("protected_ranges", started));
            let started = Instant::now();
            self.collect_sql_protected_highlights_with_ranges(
                overlay_range.start,
                &overlay_protected_ranges,
                &mut highlights,
            );
            self.collect_parameter_highlights_with_ranges(
                &overlay_text,
                overlay_range.start,
                &overlay_protected_ranges,
                &mut highlights,
            );
            self.collect_dialect_word_highlights_with_ranges(
                &overlay_text,
                overlay_range.start,
                &overlay_protected_ranges,
                &mut highlights,
            );
            self.collect_driver_syntax_overlay_highlights_in_rope_range(
                text,
                byte_range.clone(),
                &mut highlights,
            );
            clip_highlights_to_range(&mut highlights, byte_range);
            phases.push(elapsed_phase("overlays", started));
            let started = Instant::now();
            let highlights = Self::normalize_highlights(highlights);
            phases.push(elapsed_phase("normalize", started));
            return SyntaxHighlightProfile { highlights, phases };
        }

        let mut highlights = Vec::new();
        let started = Instant::now();
        let overlay_range = rope_line_covering_byte_range(text, byte_range.clone());
        let overlay_text = rope_byte_range_to_string(text, overlay_range.clone());
        phases.push(elapsed_phase("slice", started));
        let started = Instant::now();
        if let Some(tree) = self.parser.parse(&overlay_text, None) {
            phases.push(elapsed_phase("parse", started));
            let mut syntax_highlights = Vec::new();
            let started = Instant::now();
            self.collect_highlights(tree.root_node(), &overlay_text, &mut syntax_highlights);
            phases.push(elapsed_phase("tree_walk", started));
            let started = Instant::now();
            self.collect_query_highlights(
                tree.root_node(),
                overlay_text.as_bytes(),
                &mut syntax_highlights,
            );
            phases.push(elapsed_phase("query", started));
            highlights.extend(syntax_highlights.into_iter().map(|highlight| Highlight {
                start: overlay_range.start + highlight.start,
                end: overlay_range.start + highlight.end,
                kind: highlight.kind,
            }));
        } else {
            phases.push(elapsed_phase("parse", started));
        }
        let started = Instant::now();
        let overlay_protected_ranges =
            sql_protected_ranges_for_rope_overlay(text, overlay_range.clone());
        phases.push(elapsed_phase("protected_ranges", started));
        let started = Instant::now();
        self.collect_sql_protected_highlights_with_ranges(
            overlay_range.start,
            &overlay_protected_ranges,
            &mut highlights,
        );
        self.collect_parameter_highlights_with_ranges(
            &overlay_text,
            overlay_range.start,
            &overlay_protected_ranges,
            &mut highlights,
        );
        self.collect_dialect_word_highlights_with_ranges(
            &overlay_text,
            overlay_range.start,
            &overlay_protected_ranges,
            &mut highlights,
        );
        self.collect_driver_syntax_overlay_highlights_in_rope_range(
            text,
            byte_range.clone(),
            &mut highlights,
        );

        clip_highlights_to_range(&mut highlights, byte_range);
        phases.push(elapsed_phase("overlays", started));
        let started = Instant::now();
        let highlights = Self::normalize_highlights(highlights);
        phases.push(elapsed_phase("normalize", started));

        SyntaxHighlightProfile { highlights, phases }
    }

    fn should_use_sql_overlay_only(&self) -> bool {
        self.syntax_capabilities.sql_overlays
            && matches!(self.syntax_capabilities.overlays, SyntaxOverlayMode::Sql)
    }

    fn uses_command_overlays(&self) -> bool {
        matches!(
            self.syntax_capabilities.overlays,
            SyntaxOverlayMode::Command
        )
    }

    fn uses_document_overlays(&self) -> bool {
        matches!(
            self.syntax_capabilities.overlays,
            SyntaxOverlayMode::Document
        )
    }

    fn uses_driver_overlays(&self) -> bool {
        matches!(
            self.syntax_capabilities.overlays,
            SyntaxOverlayMode::Command | SyntaxOverlayMode::Document
        )
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

        let mut normalized: Vec<Highlight> = Vec::with_capacity(highlights.len());
        let mut active_start_index = 0usize;
        for highlight in highlights {
            active_start_index = active_start_index.min(normalized.len());
            while active_start_index < normalized.len()
                && normalized[active_start_index].end <= highlight.start
            {
                active_start_index += 1;
            }

            if normalized[active_start_index..].iter().any(|existing| {
                existing.start == highlight.start
                    && existing.end == highlight.end
                    && Self::highlight_rank(existing.kind) <= Self::highlight_rank(highlight.kind)
            }) {
                continue;
            }

            if normalized[active_start_index..].iter().any(|existing| {
                existing.kind == HighlightKind::Parameter
                    && existing.start <= highlight.start
                    && existing.end >= highlight.end
            }) {
                continue;
            }

            if matches!(
                highlight.kind,
                HighlightKind::Identifier | HighlightKind::String
            ) {
                let mut active_highlights = normalized.split_off(active_start_index);
                active_highlights.retain(|existing: &Highlight| {
                    !(highlight.start <= existing.start
                        && highlight.end >= existing.end
                        && matches!(
                            existing.kind,
                            HighlightKind::Identifier | HighlightKind::Punctuation
                        ))
                });
                normalized.append(&mut active_highlights);
                active_start_index = active_start_index.min(normalized.len());
            }

            if normalized[active_start_index..].iter().any(|existing| {
                matches!(
                    existing.kind,
                    HighlightKind::Identifier | HighlightKind::String
                ) && existing.start <= highlight.start
                    && existing.end >= highlight.end
                    && matches!(
                        highlight.kind,
                        HighlightKind::Identifier | HighlightKind::Punctuation
                    )
            }) {
                continue;
            }

            if normalized[active_start_index..].iter().any(|existing| {
                existing.kind == highlight.kind
                    && existing.start <= highlight.start
                    && existing.end >= highlight.end
            }) {
                continue;
            }
            if normalized[active_start_index..].iter().any(|existing| {
                existing.kind == highlight.kind
                    && highlight.start <= existing.start
                    && highlight.end >= existing.end
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
            HighlightKind::Type => 1,
            HighlightKind::Keyword => 2,
            HighlightKind::Parameter => 3,
            HighlightKind::Function => 4,
            HighlightKind::String => 5,
            HighlightKind::Number => 6,
            HighlightKind::Boolean => 7,
            HighlightKind::Null => 8,
            HighlightKind::Comment => 9,
            HighlightKind::Operator => 10,
            HighlightKind::Punctuation => 11,
            HighlightKind::Identifier => 12,
            HighlightKind::Default => 13,
        }
    }

    fn classify_non_literal_node(&self, node: Node) -> HighlightKind {
        let node_kind = node.kind();

        if self.uses_document_overlays() {
            return self.classify_mongodb_node(node);
        }

        if node_kind == "ERROR" {
            return HighlightKind::Default;
        }

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

    fn classify_mongodb_node(&self, node: Node) -> HighlightKind {
        let node_kind = node.kind();

        match node_kind {
            "comment" => HighlightKind::Comment,
            "string" | "string_fragment" | "template_string" => HighlightKind::String,
            "number" => HighlightKind::Number,
            "true" | "false" => HighlightKind::Boolean,
            "null" | "undefined" => HighlightKind::Null,
            "identifier" if self.mongodb_identifier_is_function_name(node) => {
                HighlightKind::Function
            }
            "identifier" => HighlightKind::Identifier,
            "property_identifier" if self.mongodb_identifier_is_function_name(node) => {
                HighlightKind::Function
            }
            "property_identifier" | "shorthand_property_identifier" => HighlightKind::Identifier,
            "ERROR" => HighlightKind::Default,
            _ => HighlightKind::Default,
        }
    }

    fn mongodb_identifier_is_function_name(&self, node: Node) -> bool {
        let Some(parent) = node.parent() else {
            return false;
        };

        if parent.kind() == "call_expression" {
            return true;
        }

        if parent.kind() != "member_expression" {
            return false;
        }

        let Some(grandparent) = parent.parent() else {
            return false;
        };

        grandparent.kind() == "call_expression" && grandparent.start_byte() == parent.start_byte()
    }

    fn classify_identifier_text(&self, text: &str) -> HighlightKind {
        let normalized = normalize_dialect_term(text);
        if normalized == "TRUE" || normalized == "FALSE" {
            HighlightKind::Boolean
        } else if normalized == "NULL" {
            HighlightKind::Null
        } else if self.dialect_terms.types.contains(&normalized) {
            HighlightKind::Type
        } else if self.dialect_terms.functions.contains(&normalized) {
            HighlightKind::Function
        } else if self.dialect_terms.keywords.contains(&normalized) {
            HighlightKind::Keyword
        } else {
            HighlightKind::Default
        }
    }

    fn identifier_text_is_function_term(&self, text: &str) -> bool {
        self.dialect_terms
            .functions
            .contains(&normalize_dialect_term(text))
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
        if matches!(text, "[]" | "[ ]") {
            HighlightKind::Punctuation
        } else if text.starts_with('\'') {
            HighlightKind::String
        } else if text.starts_with('"') {
            HighlightKind::Identifier
        } else if text.eq_ignore_ascii_case("true") || text.eq_ignore_ascii_case("false") {
            HighlightKind::Boolean
        } else if text.eq_ignore_ascii_case("null") {
            HighlightKind::Null
        } else if Self::looks_like_number_literal(text) {
            HighlightKind::Number
        } else {
            HighlightKind::Default
        }
    }

    fn classify_literal_text_for_profile(&self, text: &str) -> HighlightKind {
        if text.starts_with('"') && !paints_quoted_identifier(self.language_profile, text) {
            return HighlightKind::String;
        }

        Self::classify_literal_text(text)
    }

    fn looks_like_number_literal(text: &str) -> bool {
        let mut text = text.trim();
        if let Some(stripped) = text.strip_prefix('+').or_else(|| text.strip_prefix('-')) {
            text = stripped;
        }

        let lower = text.to_ascii_lowercase();
        if let Some(hex) = lower.strip_prefix("0x") {
            return !hex.is_empty() && hex.chars().all(|character| character.is_ascii_hexdigit());
        }
        if let Some(binary) = lower.strip_prefix("0b") {
            return !binary.is_empty()
                && binary
                    .chars()
                    .all(|character| matches!(character, '0' | '1'));
        }

        let mut chars = text.chars().peekable();
        let mut digits_before_decimal = false;
        while chars
            .peek()
            .is_some_and(|character| character.is_ascii_digit())
        {
            digits_before_decimal = true;
            chars.next();
        }

        let mut digits_after_decimal = false;
        if chars.peek() == Some(&'.') {
            chars.next();
            while chars
                .peek()
                .is_some_and(|character| character.is_ascii_digit())
            {
                digits_after_decimal = true;
                chars.next();
            }
        }

        if !digits_before_decimal && !digits_after_decimal {
            return false;
        }

        if chars
            .peek()
            .is_some_and(|character| matches!(character, 'e' | 'E'))
        {
            chars.next();
            if chars
                .peek()
                .is_some_and(|character| matches!(character, '+' | '-'))
            {
                chars.next();
            }

            let mut exponent_digits = false;
            while chars
                .peek()
                .is_some_and(|character| character.is_ascii_digit())
            {
                exponent_digits = true;
                chars.next();
            }
            if !exponent_digits {
                return false;
            }
        }

        chars.next().is_none()
    }

    fn classify_terminal_text(&self, text: &str) -> HighlightKind {
        match text {
            "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" | "+" | "-" | "*" | "/" | "%" | "^"
            | "||" | "&" | "|" | "~" | ":=" => HighlightKind::Operator,
            _ if self.punctuation_like_nodes.contains(text) => HighlightKind::Punctuation,
            _ => HighlightKind::Default,
        }
    }

    fn refine_query_capture_kind(&self, text: &str, kind: HighlightKind) -> HighlightKind {
        if !matches!(
            kind,
            HighlightKind::Default
                | HighlightKind::Identifier
                | HighlightKind::Keyword
                | HighlightKind::Type
        ) {
            return kind;
        }

        let dialect_kind = self.classify_identifier_text(text);
        if dialect_kind == HighlightKind::Default {
            kind
        } else {
            dialect_kind
        }
    }

    fn query_capture_text_is_plausible(text: &str, kind: HighlightKind) -> bool {
        if !matches!(
            kind,
            HighlightKind::Keyword | HighlightKind::Type | HighlightKind::Function
        ) {
            return true;
        }

        text.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '_' | '$' | '#' | '@' | '.' | '`')
        })
    }

    fn range_is_inside_delimited_identifier_bytes(text: &[u8], start: usize, end: usize) -> bool {
        if start == 0 || end >= text.len() {
            return false;
        }

        matches!(
            (text[start - 1], text[end]),
            (b'[', b']') | (b'`', b'`') | (b'"', b'"')
        )
    }

    fn collect_query_highlights(
        &self,
        root_node: Node,
        source: &[u8],
        highlights: &mut Vec<Highlight>,
    ) {
        let Some(query) = self.highlight_query.as_ref() else {
            return;
        };

        let mut cursor = QueryCursor::new();
        let mut matches = cursor.matches(query, root_node, source);
        while let Some(query_match) = matches.next() {
            for capture in query_match.captures {
                let Some(capture_name) = query.capture_names().get(capture.index as usize) else {
                    continue;
                };
                if *capture_name == "string" && capture.node.kind() == "literal" {
                    continue;
                }
                if *capture_name == "type.builtin" && capture.node.kind() == "keyword_null" {
                    continue;
                }
                let Some(mut kind) = highlight_kind_for_query_capture(capture_name) else {
                    continue;
                };
                if kind == HighlightKind::Parameter
                    && !self
                        .syntax_capabilities
                        .parameter_placeholders
                        .question_mark
                    && source.get(capture.node.start_byte()..capture.node.end_byte()) == Some(b"?")
                {
                    continue;
                }
                if let Ok(text) =
                    std::str::from_utf8(&source[capture.node.start_byte()..capture.node.end_byte()])
                {
                    kind = self.refine_query_capture_kind(text, kind);
                    if !Self::query_capture_text_is_plausible(text, kind) {
                        continue;
                    }
                    if kind == HighlightKind::Function
                        && Self::range_is_inside_delimited_identifier_bytes(
                            source,
                            capture.node.start_byte(),
                            capture.node.end_byte(),
                        )
                    {
                        continue;
                    }
                }
                highlights.push(Highlight {
                    start: capture.node.start_byte(),
                    end: capture.node.end_byte(),
                    kind,
                });
            }
        }
    }

    fn collect_query_highlights_in_rope(
        &self,
        root_node: Node,
        source: &Rope,
        highlights: &mut Vec<Highlight>,
    ) {
        let Some(query) = self.highlight_query.as_ref() else {
            return;
        };

        let mut cursor = QueryCursor::new();
        let mut matches = cursor.matches(query, root_node, RopeTextProvider(source));
        while let Some(query_match) = matches.next() {
            for capture in query_match.captures {
                let Some(capture_name) = query.capture_names().get(capture.index as usize) else {
                    continue;
                };
                if *capture_name == "string" && capture.node.kind() == "literal" {
                    continue;
                }
                if *capture_name == "type.builtin" && capture.node.kind() == "keyword_null" {
                    continue;
                }
                let Some(mut kind) = highlight_kind_for_query_capture(capture_name) else {
                    continue;
                };
                if kind == HighlightKind::Parameter
                    && !self
                        .syntax_capabilities
                        .parameter_placeholders
                        .question_mark
                    && Self::rope_text_range(
                        source,
                        capture.node.start_byte(),
                        capture.node.end_byte(),
                    )
                    .as_deref()
                        == Some("?")
                {
                    continue;
                }
                if let Some(text) = Self::rope_text_range(
                    source,
                    capture.node.start_byte(),
                    capture.node.end_byte(),
                ) {
                    kind = self.refine_query_capture_kind(&text, kind);
                    if !Self::query_capture_text_is_plausible(&text, kind) {
                        continue;
                    }
                    if kind == HighlightKind::Function
                        && capture.node.start_byte() > 0
                        && capture.node.end_byte() < source.len_bytes()
                        && let Some(text_slice) = Self::rope_text_range(
                            source,
                            capture.node.start_byte() - 1,
                            capture.node.end_byte() + 1,
                        )
                        && Self::range_is_inside_delimited_identifier_bytes(
                            text_slice.as_bytes(),
                            1,
                            text_slice.len() - 1,
                        )
                    {
                        continue;
                    }
                }
                highlights.push(Highlight {
                    start: capture.node.start_byte(),
                    end: capture.node.end_byte(),
                    kind,
                });
            }
        }
    }

    fn collect_driver_syntax_overlay_highlights(
        &self,
        text: &str,
        highlights: &mut Vec<Highlight>,
    ) {
        self.collect_driver_syntax_overlay_highlights_with_base(text, 0, highlights);
    }

    fn collect_driver_syntax_overlay_highlights_with_base(
        &self,
        text: &str,
        base_offset: usize,
        highlights: &mut Vec<Highlight>,
    ) {
        for token in driver_syntax_overlay_tokens_for_capabilities(&self.syntax_capabilities, text)
        {
            let kind = match token.kind {
                DriverSyntaxOverlayKind::Keyword => HighlightKind::Keyword,
                DriverSyntaxOverlayKind::Function => HighlightKind::Function,
                DriverSyntaxOverlayKind::Type => HighlightKind::Type,
                DriverSyntaxOverlayKind::Operator => HighlightKind::Operator,
                DriverSyntaxOverlayKind::Comment => HighlightKind::Comment,
                DriverSyntaxOverlayKind::String => HighlightKind::String,
                DriverSyntaxOverlayKind::Number => HighlightKind::Number,
                DriverSyntaxOverlayKind::Identifier => HighlightKind::Identifier,
                DriverSyntaxOverlayKind::Punctuation => HighlightKind::Punctuation,
            };
            if kind == HighlightKind::Operator {
                let start = base_offset + token.start;
                let end = base_offset + token.end;
                highlights.retain(|highlight| !(highlight.start == start && highlight.end == end));
            }
            highlights.push(Highlight {
                start: base_offset + token.start,
                end: base_offset + token.end,
                kind,
            });
        }
    }

    fn collect_parameter_highlights_with_ranges(
        &self,
        text: &str,
        base_offset: usize,
        protected_ranges: &[SqlProtectedRange],
        highlights: &mut Vec<Highlight>,
    ) {
        let parameter_capability = self.syntax_capabilities.parameter_placeholders;
        if !parameter_capability.enabled {
            return;
        }

        for placeholder in sql_parameter_placeholders_with_ranges(text, protected_ranges) {
            let range = placeholder.start..placeholder.end;
            if !parameter_capability.question_mark && &text[range.clone()] == "?" {
                continue;
            }

            let absolute_range = base_offset + range.start..base_offset + range.end;
            highlights.push(Highlight {
                start: absolute_range.start,
                end: absolute_range.end,
                kind: HighlightKind::Parameter,
            });
        }
    }

    fn collect_sql_protected_highlights_with_ranges(
        &self,
        base_offset: usize,
        protected_ranges: &[SqlProtectedRange],
        highlights: &mut Vec<Highlight>,
    ) {
        for range in protected_ranges {
            let kind = match range.kind {
                SqlProtectedRangeKind::Comment => HighlightKind::Comment,
                SqlProtectedRangeKind::StringLiteral => HighlightKind::String,
                SqlProtectedRangeKind::DollarQuotedString
                    if self.syntax_capabilities.dollar_quoted_strings =>
                {
                    HighlightKind::String
                }
                _ => continue,
            };

            let absolute_range = base_offset + range.start..base_offset + range.end;
            highlights.push(Highlight {
                start: absolute_range.start,
                end: absolute_range.end,
                kind,
            });
        }
    }

    fn collect_dialect_word_highlights_with_ranges(
        &self,
        text: &str,
        base_offset: usize,
        protected_ranges: &[SqlProtectedRange],
        highlights: &mut Vec<Highlight>,
    ) {
        if !self.syntax_capabilities.sql_overlays {
            return;
        }

        let bytes = text.as_bytes();
        let mut index = 0usize;
        let mut protected_range_index = 0usize;

        while index < bytes.len() {
            if let Some(protected_range) =
                sql_protected_range_at(index, protected_ranges, &mut protected_range_index)
            {
                let protected_text = &text[protected_range.start..protected_range.end];
                if protected_text == "[]" {
                    let absolute_range =
                        base_offset + protected_range.start..base_offset + protected_range.end;
                    highlights.push(Highlight {
                        start: absolute_range.start,
                        end: absolute_range.end,
                        kind: HighlightKind::Punctuation,
                    });
                    index = protected_range.end;
                    continue;
                }

                let paints_quoted_identifier = protected_range.kind
                    == SqlProtectedRangeKind::QuotedIdentifier
                    && paints_quoted_identifier(self.language_profile, protected_text);

                if paints_quoted_identifier
                    || protected_range.kind == SqlProtectedRangeKind::QuotedIdentifier
                {
                    let absolute_range =
                        base_offset + protected_range.start..base_offset + protected_range.end;
                    highlights.push(Highlight {
                        start: absolute_range.start,
                        end: absolute_range.end,
                        kind: if paints_quoted_identifier {
                            HighlightKind::Identifier
                        } else {
                            HighlightKind::String
                        },
                    });
                }

                index = protected_range.end;
                continue;
            }

            match bytes[index] {
                b'[' if bytes.get(index + 1) == Some(&b']') => {
                    let absolute_range = base_offset + index..base_offset + index + 2;
                    highlights.push(Highlight {
                        start: absolute_range.start,
                        end: absolute_range.end,
                        kind: HighlightKind::Punctuation,
                    });
                    index += 2;
                }
                b':' if bytes.get(index + 1) == Some(&b':') => {
                    highlights.push(Highlight {
                        start: base_offset + index,
                        end: base_offset + index + 2,
                        kind: HighlightKind::Operator,
                    });
                    index += 2;
                }
                b'!' | b'<' | b'>' if bytes.get(index + 1) == Some(&b'=') => {
                    highlights.push(Highlight {
                        start: base_offset + index,
                        end: base_offset + index + 2,
                        kind: HighlightKind::Operator,
                    });
                    index += 2;
                }
                b'=' | b'<' | b'>' | b'+' | b'-' | b'*' | b'/' | b'%' | b'^' | b'~' => {
                    highlights.push(Highlight {
                        start: base_offset + index,
                        end: base_offset + index + 1,
                        kind: HighlightKind::Operator,
                    });
                    index += 1;
                }
                b':' | b',' | b';' | b'(' | b')' | b'{' | b'}' | b'[' | b']' | b'.' | b'?' => {
                    let absolute_range = base_offset + index..base_offset + index + 1;
                    let kind = if bytes[index] == b'?'
                        && !self
                            .syntax_capabilities
                            .parameter_placeholders
                            .question_mark
                    {
                        HighlightKind::Operator
                    } else {
                        HighlightKind::Punctuation
                    };
                    highlights.push(Highlight {
                        start: absolute_range.start,
                        end: absolute_range.end,
                        kind,
                    });
                    index += 1;
                }
                _ if bytes[index].is_ascii_digit() => {
                    let end = scan_number_literal_tail(bytes, index + 1);
                    if Self::looks_like_number_literal(&text[index..end]) {
                        let absolute_range = base_offset + index..base_offset + end;
                        highlights.push(Highlight {
                            start: absolute_range.start,
                            end: absolute_range.end,
                            kind: HighlightKind::Number,
                        });
                    }
                    index = end;
                }
                _ if is_identifier_start(bytes.get(index).copied()) => {
                    let end = scan_identifier_tail(bytes, index + 1);
                    let mut kind = self.classify_identifier_text(&text[index..end]);
                    let is_function_call = next_non_whitespace_byte(bytes, end) == Some(b'(')
                        && !identifier_before_non_function_parenthesized_context(bytes, index);
                    if is_function_call && self.identifier_text_is_function_term(&text[index..end])
                    {
                        kind = HighlightKind::Function;
                    } else if kind == HighlightKind::Default {
                        kind = if is_function_call {
                            HighlightKind::Function
                        } else {
                            HighlightKind::Identifier
                        };
                    }
                    let absolute_range = base_offset + index..base_offset + end;
                    highlights.push(Highlight {
                        start: absolute_range.start,
                        end: absolute_range.end,
                        kind,
                    });
                    index = end;
                }
                _ => {
                    index += 1;
                }
            }
        }
    }

    fn collect_driver_syntax_overlay_highlights_in_rope(
        &self,
        text: &Rope,
        highlights: &mut Vec<Highlight>,
    ) {
        if self.uses_command_overlays() {
            self.collect_command_syntax_overlay_highlights_in_rope(text, highlights);
            return;
        }

        self.collect_driver_syntax_overlay_highlights_with_base(&text.to_string(), 0, highlights);
    }

    fn collect_command_syntax_overlay_highlights_in_rope(
        &self,
        text: &Rope,
        highlights: &mut Vec<Highlight>,
    ) {
        for line_index in 0..text.len_lines() {
            let line_start = text.line_to_byte(line_index);
            let line_text = text.line(line_index).to_string();
            self.collect_driver_syntax_overlay_highlights_with_base(
                &line_text, line_start, highlights,
            );
        }
    }

    fn collect_driver_syntax_overlay_highlights_in_rope_range(
        &self,
        text: &Rope,
        byte_range: std::ops::Range<usize>,
        highlights: &mut Vec<Highlight>,
    ) {
        if !self.uses_driver_overlays() {
            return;
        }

        let overlay_range = rope_line_covering_byte_range(text, byte_range);
        let overlay_text = rope_byte_range_to_string(text, overlay_range.clone());
        self.collect_driver_syntax_overlay_highlights_with_base(
            &overlay_text,
            overlay_range.start,
            highlights,
        );
    }

    fn should_emit_highlight(node: Node, kind: HighlightKind) -> bool {
        if kind == HighlightKind::Default {
            return false;
        }

        match kind {
            HighlightKind::Function => node.child_count() == 0,
            HighlightKind::Type => node.child_count() == 0,
            HighlightKind::Identifier => node.child_count() == 0 || node.kind() == "all_fields",
            HighlightKind::Comment => true,
            _ => true,
        }
    }

    /// Recursively collect highlights from the syntax tree.
    fn collect_highlights(&self, node: Node, text: &str, highlights: &mut Vec<Highlight>) {
        let node_kind = node.kind();

        let mut kind = if node_kind == "literal" {
            let start = node.start_byte();
            let end = node.end_byte();
            if start > text.len()
                || end > text.len()
                || !text.is_char_boundary(start)
                || !text.is_char_boundary(end)
            {
                HighlightKind::Default
            } else {
                self.classify_literal_text_for_profile(&text[start..end])
            }
        } else {
            self.classify_non_literal_node(node)
        };

        if matches!(
            kind,
            HighlightKind::Default | HighlightKind::Identifier | HighlightKind::Keyword
        ) {
            let start = node.start_byte();
            let end = node.end_byte();
            if start <= text.len()
                && end <= text.len()
                && text.is_char_boundary(start)
                && text.is_char_boundary(end)
            {
                let dialect_kind = self.classify_identifier_text(&text[start..end]);
                if dialect_kind != HighlightKind::Default {
                    kind = dialect_kind;
                }
            }
        }

        if matches!(
            kind,
            HighlightKind::Keyword | HighlightKind::Type | HighlightKind::Function
        ) {
            let start = node.start_byte();
            let end = node.end_byte();
            if start <= text.len()
                && end <= text.len()
                && text.is_char_boundary(start)
                && text.is_char_boundary(end)
                && !Self::query_capture_text_is_plausible(&text[start..end], kind)
            {
                kind = HighlightKind::Default;
            }
        }
        if kind == HighlightKind::Function
            && Self::range_is_inside_delimited_identifier_bytes(
                text.as_bytes(),
                node.start_byte(),
                node.end_byte(),
            )
        {
            kind = HighlightKind::Default;
        }

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

        let mut kind = if node_kind == "literal" {
            self.classify_literal_from_rope(text, node.start_byte(), node.end_byte())
                .unwrap_or(HighlightKind::Default)
        } else {
            self.classify_non_literal_node(node)
        };

        if matches!(
            kind,
            HighlightKind::Default | HighlightKind::Identifier | HighlightKind::Keyword
        ) && let Some(text_slice) =
            Self::rope_text_range(text, node.start_byte(), node.end_byte())
        {
            let dialect_kind = self.classify_identifier_text(&text_slice);
            if dialect_kind != HighlightKind::Default {
                kind = dialect_kind;
            }
        }

        if matches!(
            kind,
            HighlightKind::Keyword | HighlightKind::Type | HighlightKind::Function
        ) && let Some(text_slice) =
            Self::rope_text_range(text, node.start_byte(), node.end_byte())
            && !Self::query_capture_text_is_plausible(&text_slice, kind)
        {
            kind = HighlightKind::Default;
        }
        if kind == HighlightKind::Function
            && node.start_byte() > 0
            && node.end_byte() < text.len_bytes()
            && let Some(text_slice) =
                Self::rope_text_range(text, node.start_byte() - 1, node.end_byte() + 1)
            && Self::range_is_inside_delimited_identifier_bytes(
                text_slice.as_bytes(),
                1,
                text_slice.len() - 1,
            )
        {
            kind = HighlightKind::Default;
        }

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

    fn classify_literal_from_rope(
        &self,
        text: &Rope,
        start: usize,
        end: usize,
    ) -> Option<HighlightKind> {
        let literal_text = Self::rope_text_range(text, start, end)?;
        Some(self.classify_literal_text_for_profile(&literal_text))
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

fn elapsed_phase(name: &'static str, started: Instant) -> SyntaxHighlightPhaseTiming {
    SyntaxHighlightPhaseTiming {
        name,
        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
    }
}

fn normalize_language_profile(language_profile: &str) -> &'static str {
    normalize_syntax_profile(language_profile)
}

fn language_for_profile(language_profile: &str) -> tree_sitter::Language {
    language_for_grammar(&get_tree_sitter_grammar(normalize_language_profile(
        language_profile,
    )))
}

fn language_for_grammar(grammar: &TreeSitterGrammar) -> tree_sitter::Language {
    match grammar {
        TreeSitterGrammar::Javascript => {
            tree_sitter::Language::new(tree_sitter_javascript::LANGUAGE)
        }
        TreeSitterGrammar::None | TreeSitterGrammar::Sql | TreeSitterGrammar::Custom(_) => {
            tree_sitter::Language::new(tree_sitter_sequel::LANGUAGE)
        }
    }
}

fn highlight_kind_for_query_capture(capture_name: &str) -> Option<HighlightKind> {
    let root = capture_name.split('.').next().unwrap_or(capture_name);
    match capture_name {
        "function.call" | "constructor" => Some(HighlightKind::Function),
        "type.builtin" | "type.qualifier" => Some(HighlightKind::Type),
        "keyword.operator" => Some(HighlightKind::Operator),
        "punctuation.bracket"
        | "punctuation.delimiter"
        | "punctuation.special"
        | "punctuation.list_marker" => Some(HighlightKind::Punctuation),
        "variable.special" => Some(HighlightKind::Identifier),
        "string.escape" | "string.regex" | "string.special" | "string.special.symbol" => {
            Some(HighlightKind::String)
        }
        "comment.doc" => Some(HighlightKind::Comment),
        _ => match root {
            "attribute" | "conditional" | "keyword" | "storageclass" => {
                Some(HighlightKind::Keyword)
            }
            "boolean" => Some(HighlightKind::Boolean),
            "comment" => Some(HighlightKind::Comment),
            "constant" => Some(HighlightKind::Identifier),
            "parameter" => Some(HighlightKind::Parameter),
            "field" | "property" | "variable" => Some(HighlightKind::Identifier),
            "function" => Some(HighlightKind::Function),
            "number" | "float" => Some(HighlightKind::Number),
            "operator" => Some(HighlightKind::Operator),
            "punctuation" => Some(HighlightKind::Punctuation),
            "string" => Some(HighlightKind::String),
            "type" => None,
            _ => None,
        },
    }
}

fn normalize_dialect_term(text: &str) -> String {
    text.trim_matches(|character: char| {
        matches!(character, '"' | '`' | '\'' | '[' | ']') || character.is_whitespace()
    })
    .to_ascii_uppercase()
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

fn next_non_whitespace_byte(bytes: &[u8], mut index: usize) -> Option<u8> {
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        index += 1;
    }
    bytes.get(index).copied()
}

fn identifier_before_non_function_parenthesized_context(bytes: &[u8], index: usize) -> bool {
    let Some(previous_word) = previous_context_word_before_identifier(bytes, index) else {
        return false;
    };

    matches!(
        previous_word.as_str(),
        "REFERENCES" | "INTO" | "UPDATE" | "TABLE" | "VIEW" | "INDEX" | "KEY" | "ON"
    )
}

fn previous_context_word_before_identifier(bytes: &[u8], index: usize) -> Option<String> {
    let mut end = index;

    loop {
        while end > 0 && bytes[end - 1].is_ascii_whitespace() {
            end -= 1;
        }

        if end > 0 && bytes[end - 1] == b'.' {
            end -= 1;
            while end > 0 && bytes[end - 1].is_ascii_whitespace() {
                end -= 1;
            }
            while end > 0 && (bytes[end - 1].is_ascii_alphanumeric() || bytes[end - 1] == b'_') {
                end -= 1;
            }
            continue;
        }

        while end > 0 && matches!(bytes[end - 1], b',' | b';' | b'(' | b')') {
            end -= 1;
            while end > 0 && bytes[end - 1].is_ascii_whitespace() {
                end -= 1;
            }
        }

        let mut start = end;
        while start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
            start -= 1;
        }

        return (start < end)
            .then(|| String::from_utf8_lossy(&bytes[start..end]).to_ascii_uppercase());
    }
}

fn scan_number_literal_tail(bytes: &[u8], mut index: usize) -> usize {
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
    }

    if bytes.get(index) == Some(&b'.') && bytes.get(index + 1).is_some_and(u8::is_ascii_digit) {
        index += 1;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
    }

    if bytes
        .get(index)
        .is_some_and(|byte| matches!(byte, b'e' | b'E'))
    {
        let exponent_start = index;
        index += 1;
        if bytes
            .get(index)
            .is_some_and(|byte| matches!(byte, b'+' | b'-'))
        {
            index += 1;
        }

        let digits_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == digits_start {
            index = exponent_start;
        }
    }

    index
}

fn is_identifier_start(byte: Option<u8>) -> bool {
    byte.is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
}

fn dialect_terms_for(language_profile: &str) -> Arc<DialectTermSet> {
    static TERMS: LazyLock<HashMap<&'static str, Arc<DialectTermSet>>> = LazyLock::new(|| {
        supported_syntax_profiles()
            .iter()
            .copied()
            .map(|profile| (profile, Arc::new(build_dialect_term_set(profile))))
            .collect()
    });

    TERMS
        .get(normalize_language_profile(language_profile))
        .cloned()
        .unwrap_or_else(|| TERMS["sql"].clone())
}

fn build_dialect_term_set(language_profile: &str) -> DialectTermSet {
    let mut terms = build_base_term_set(language_profile);
    let profile = get_syntax_term_profile(language_profile);
    terms
        .keywords
        .extend(term_slice_set(profile.dialect_keywords));
    terms
        .functions
        .extend(term_slice_set(profile.dialect_functions));
    terms.types.extend(term_slice_set(profile.dialect_types));
    terms
}

fn build_base_term_set(language_profile: &str) -> DialectTermSet {
    let profile = get_syntax_term_profile(language_profile);
    let query_terms = highlight_query_term_set(language_profile);

    let mut keywords = term_slice_set(profile.base_keywords);
    keywords.extend(query_terms.keywords);

    let mut functions = term_slice_set(profile.base_functions);
    functions.extend(query_terms.functions);

    let mut types = term_slice_set(profile.base_types);
    types.extend(query_terms.types);

    DialectTermSet {
        keywords,
        functions,
        types,
    }
}

fn term_slice_set(terms: &[&'static str]) -> HashSet<String> {
    terms.iter().copied().map(normalize_dialect_term).collect()
}

fn highlight_query_for(language_profile: &str) -> &'static str {
    highlight_query_for_language(get_highlight_query_language(normalize_language_profile(
        language_profile,
    )))
}

fn highlight_query_for_language(query_language: HighlightQueryLanguage) -> &'static str {
    match query_language {
        HighlightQueryLanguage::PostgreSql => {
            include_str!(
                "../../zqlz-ui/src/widgets/highlighter/languages/postgresql/highlights.scm"
            )
        }
        HighlightQueryLanguage::MySql => {
            include_str!("../../zqlz-ui/src/widgets/highlighter/languages/mysql/highlights.scm")
        }
        HighlightQueryLanguage::Sqlite => {
            include_str!("../../zqlz-ui/src/widgets/highlighter/languages/sqlite/highlights.scm")
        }
        HighlightQueryLanguage::ClickHouse => {
            include_str!(
                "../../zqlz-ui/src/widgets/highlighter/languages/clickhouse/highlights.scm"
            )
        }
        HighlightQueryLanguage::MongoDb => {
            include_str!("../../zqlz-ui/src/widgets/highlighter/languages/mongodb/highlights.scm")
        }
        HighlightQueryLanguage::Redis => {
            include_str!("../../zqlz-ui/src/widgets/highlighter/languages/redis/highlights.scm")
        }
        HighlightQueryLanguage::None | HighlightQueryLanguage::Sql => {
            include_str!("../../zqlz-ui/src/widgets/highlighter/languages/sql/highlights.scm")
        }
    }
}

#[cfg(test)]
fn highlight_query_source_for_parser(language_profile: &str) -> &'static str {
    highlight_query_source_for_language(get_highlight_query_language(normalize_language_profile(
        language_profile,
    )))
}

fn highlight_query_source_for_language(query_language: HighlightQueryLanguage) -> &'static str {
    static QUERIES: LazyLock<HashMap<&'static str, String>> = LazyLock::new(|| {
        supported_syntax_profiles()
            .iter()
            .copied()
            .filter(|profile| {
                !matches!(
                    get_highlight_query_language(profile),
                    HighlightQueryLanguage::None | HighlightQueryLanguage::Redis
                )
            })
            .map(|profile| {
                (
                    profile,
                    strip_literal_capture_lists(highlight_query_for_language(
                        get_highlight_query_language(profile),
                    )),
                )
            })
            .collect()
    });

    let query_language = query_language_key(query_language);
    QUERIES
        .get(query_language)
        .map(String::as_str)
        .unwrap_or_else(|| QUERIES.get("sql").map(String::as_str).unwrap_or(""))
}

fn query_language_key(query_language: HighlightQueryLanguage) -> &'static str {
    match query_language {
        HighlightQueryLanguage::None | HighlightQueryLanguage::Sql => "sql",
        HighlightQueryLanguage::PostgreSql => "postgresql",
        HighlightQueryLanguage::MySql => "mysql",
        HighlightQueryLanguage::Sqlite => "sqlite",
        HighlightQueryLanguage::ClickHouse => "clickhouse",
        HighlightQueryLanguage::MongoDb => "mongodb",
        HighlightQueryLanguage::Redis => "redis",
    }
}

fn strip_literal_capture_lists(query: &str) -> String {
    let mut output = String::with_capacity(query.len());
    let mut pending_lines = Vec::new();
    let mut in_string_list = false;
    let mut string_list_only = true;

    for line in query.lines() {
        let trimmed = line.trim();
        if trimmed == "[" {
            in_string_list = true;
            string_list_only = true;
            pending_lines.clear();
            pending_lines.push(line);
            continue;
        }

        if in_string_list {
            pending_lines.push(line);

            if trimmed.starts_with(']') {
                if !string_list_only {
                    for pending in pending_lines.drain(..) {
                        output.push_str(pending);
                        output.push('\n');
                    }
                }
                in_string_list = false;
                continue;
            }

            if !(trimmed.is_empty()
                || trimmed.starts_with(';')
                || trimmed.starts_with('"') && trimmed.ends_with('"'))
            {
                string_list_only = false;
            }
            continue;
        }

        output.push_str(line);
        output.push('\n');
    }

    if in_string_list {
        for pending in pending_lines {
            output.push_str(pending);
            output.push('\n');
        }
    }

    output
}

fn highlight_query_term_set(language_profile: &str) -> HighlightQueryTermSet {
    let query = highlight_query_for(language_profile);
    let mut terms = HighlightQueryTermSet::default();
    let mut pending_terms = HashSet::new();
    let mut in_string_list = false;

    for line in query.lines() {
        let trimmed = line.trim();
        if trimmed == "[" {
            in_string_list = true;
            continue;
        }
        if in_string_list && trimmed.starts_with(']') {
            extend_highlight_query_terms(trimmed, &mut pending_terms, &mut terms);
            pending_terms.clear();
            in_string_list = false;
            continue;
        }
        if in_string_list
            && let Some(term) = trimmed
                .strip_prefix('"')
                .and_then(|rest| rest.strip_suffix('"'))
        {
            pending_terms.insert(normalize_dialect_term(term));
        }
    }

    terms
}

fn extend_highlight_query_terms(
    capture_line: &str,
    pending_terms: &mut HashSet<String>,
    terms: &mut HighlightQueryTermSet,
) {
    if capture_line.contains("@function.call") || capture_line.contains("@function") {
        terms.functions.extend(pending_terms.iter().cloned());
    }
    if capture_line.contains("@type.builtin") || capture_line.contains("@type.qualifier") {
        terms.types.extend(pending_terms.iter().cloned());
    }
    if [
        "@attribute",
        "@conditional",
        "@keyword",
        "@keyword.operator",
        "@storageclass",
    ]
    .iter()
    .any(|capture| capture_line.contains(capture))
    {
        terms.keywords.extend(pending_terms.iter().cloned());
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

    fn highlighted_tokens<'a>(
        text: &'a str,
        highlights: &'a [Highlight],
    ) -> Vec<(&'a str, HighlightKind)> {
        highlights
            .iter()
            .map(|highlight| (&text[highlight.start..highlight.end], highlight.kind))
            .collect()
    }

    fn assigned_kinds_for_token(
        text: &str,
        highlights: &[Highlight],
        token: &str,
    ) -> Vec<HighlightKind> {
        let mut kinds = highlights
            .iter()
            .filter(|highlight| &text[highlight.start..highlight.end] == token)
            .map(|highlight| highlight.kind)
            .collect::<Vec<_>>();
        kinds.sort_by_key(|kind| SyntaxHighlighter::highlight_rank(*kind));
        kinds.dedup();
        kinds
    }

    fn assignment_snapshot_for_tokens(
        text: &str,
        highlights: &[Highlight],
        tokens: &[ExpectedHighlightToken],
    ) -> Vec<(&'static str, Vec<HighlightKind>)> {
        tokens
            .iter()
            .map(|(token, _)| (*token, assigned_kinds_for_token(text, highlights, token)))
            .collect()
    }

    fn assert_highlighted_token(
        text: &str,
        highlights: &[Highlight],
        token: &str,
        kind: HighlightKind,
    ) {
        assert!(
            highlights.iter().any(|highlight| {
                highlight.kind == kind && &text[highlight.start..highlight.end] == token
            }),
            "expected {token:?} as {kind:?}, got {:?}",
            highlighted_tokens(text, highlights)
        );
    }

    fn assert_no_highlighted_token(
        text: &str,
        highlights: &[Highlight],
        token: &str,
        kind: HighlightKind,
    ) {
        assert!(
            !highlights.iter().any(|highlight| {
                highlight.kind == kind && &text[highlight.start..highlight.end] == token
            }),
            "did not expect {token:?} as {kind:?}, got {:?}",
            highlighted_tokens(text, highlights)
        );
    }

    fn clipped_full_highlights_for_range(
        mut highlights: Vec<Highlight>,
        byte_range: std::ops::Range<usize>,
    ) -> Vec<Highlight> {
        highlights.retain(|highlight| {
            highlight.start < byte_range.end && highlight.end > byte_range.start
        });
        for highlight in &mut highlights {
            highlight.start = highlight.start.max(byte_range.start);
            highlight.end = highlight.end.min(byte_range.end);
        }
        SyntaxHighlighter::normalize_highlights(highlights)
    }

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
    fn test_classify_numeric_literal_variants() {
        for literal in ["1.5", ".5", "1e-9", "0xFF", "0b1010", "-42", "+3.14"] {
            assert_eq!(
                SyntaxHighlighter::classify_literal_text(literal),
                HighlightKind::Number,
                "{literal}"
            );
        }

        for literal in ["1e", "0x", "0b102", "+"] {
            assert_ne!(
                SyntaxHighlighter::classify_literal_text(literal),
                HighlightKind::Number,
                "{literal}"
            );
        }
    }

    #[test]
    fn test_double_quoted_sql_names_are_identifiers() {
        assert_eq!(
            SyntaxHighlighter::classify_literal_text("\"category_id\""),
            HighlightKind::Identifier
        );
    }

    #[test]
    fn test_language_profile_term_sets_are_shared_and_alias_normalized() {
        let postgres = dialect_terms_for("postgres");
        let postgresql = dialect_terms_for("postgresql");
        let mariadb = dialect_terms_for("mariadb");
        let mysql = dialect_terms_for("mysql");

        assert!(Arc::ptr_eq(&postgres, &postgresql));
        assert!(Arc::ptr_eq(&mariadb, &mysql));
        assert!(postgres.types.contains("JSONB"));
        assert!(mysql.keywords.contains("FORCE"));
    }

    #[test]
    fn test_double_quoted_sql_names_stay_identifiers_after_query_overlay() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = r#"CREATE TABLE "categories" ("category_id" text)"#;
        let highlights = highlighter.highlight(text);

        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Identifier
                && &text[highlight.start..highlight.end] == "\"categories\""
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Identifier
                && &text[highlight.start..highlight.end] == "\"category_id\""
        }));
        assert!(!highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::String
                && matches!(
                    &text[highlight.start..highlight.end],
                    "\"categories\"" | "\"category_id\""
                )
        }));
    }

    #[test]
    fn test_create_table_recovery_nodes_do_not_paint_error_text() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = r#"CREATE TABLE "categories" (
  "category_id" text(255) NOT NULL,
  "category_type" text(255)
)"#;
        let highlights = highlighter.highlight(text);

        assert!(
            highlights
                .iter()
                .all(|highlight| highlight.kind != HighlightKind::Error)
        );
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Identifier
                && &text[highlight.start..highlight.end] == "\"category_id\""
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Type && &text[highlight.start..highlight.end] == "text"
        }));
        assert_highlighted_token(text, &highlights, "255", HighlightKind::Number);
    }

    #[test]
    fn test_adjacent_quoted_column_and_type_are_assigned_separately() {
        let text = r#"CREATE TABLE "categories" (
  "title"text,
  "list_title"text,
  "sort_order"real(14, 3)
)"#;
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("sqlite");
        let highlights = highlighter.highlight(text);

        for column in ["\"title\"", "\"list_title\"", "\"sort_order\""] {
            assert_highlighted_token(text, &highlights, column, HighlightKind::Identifier);
            assert_no_highlighted_token(text, &highlights, column, HighlightKind::String);
        }

        assert_highlighted_token(text, &highlights, "text", HighlightKind::Type);
        assert_highlighted_token(text, &highlights, "real", HighlightKind::Type);
        assert_highlighted_token(text, &highlights, "14", HighlightKind::Number);
        assert_highlighted_token(text, &highlights, "3", HighlightKind::Number);
        assert_no_highlighted_token(text, &highlights, "real(14, 3)", HighlightKind::Type);
    }

    #[test]
    fn test_parameter_body_does_not_get_dialect_word_highlight() {
        let text = "SELECT payload ? :key FROM events WHERE id = $1;";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let highlights = highlighter.highlight(text);

        assert_highlighted_token(text, &highlights, ":key", HighlightKind::Parameter);
        assert_no_highlighted_token(text, &highlights, "key", HighlightKind::Keyword);
    }

    #[test]
    fn test_sql_overlay_styles_recovery_identifiers_and_common_keywords() {
        let text = "SELECT user_id FROM events QUALIFY row_number() OVER (PARTITION BY user_id) = 1 TO sink";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("duckdb");
        let highlights = highlighter.highlight(text);

        assert_highlighted_token(text, &highlights, "BY", HighlightKind::Keyword);
        assert_highlighted_token(text, &highlights, "TO", HighlightKind::Keyword);
        assert_highlighted_token(text, &highlights, "user_id", HighlightKind::Identifier);
        assert_highlighted_token(text, &highlights, "sink", HighlightKind::Identifier);
    }

    #[test]
    fn test_sql_overlay_styles_postgres_cast_operator() {
        let text = "SELECT '{}'::jsonb";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let highlights = highlighter.highlight(text);

        assert_highlighted_token(text, &highlights, "::", HighlightKind::Operator);
        assert_highlighted_token(text, &highlights, "jsonb", HighlightKind::Type);
    }

    #[test]
    fn test_sql_overlay_styles_is_as_keyword() {
        for profile in ["postgresql", "mysql", "mssql"] {
            let text = "SELECT * FROM users WHERE deleted_at IS NULL AND active IS NOT FALSE;";
            let mut highlighter = SyntaxHighlighter::new().unwrap();
            highlighter.set_language_profile(profile);
            let highlights = highlighter.highlight(text);

            assert_highlighted_token(text, &highlights, "IS", HighlightKind::Keyword);
            assert_no_highlighted_token(text, &highlights, "IS", HighlightKind::Identifier);
        }
    }

    #[test]
    fn test_sql_overlay_styles_operator_words_as_keywords() {
        for profile in ["postgresql", "mysql", "sqlite"] {
            let text = "SELECT * FROM users WHERE id IN (1, 2) AND name LIKE 'A%' AND role NOT IN ('admin');";
            let mut highlighter = SyntaxHighlighter::new().unwrap();
            highlighter.set_language_profile(profile);
            let highlights = highlighter.highlight(text);

            assert_highlighted_token(text, &highlights, "IN", HighlightKind::Keyword);
            assert_highlighted_token(text, &highlights, "LIKE", HighlightKind::Keyword);
            assert_no_highlighted_token(text, &highlights, "IN", HighlightKind::Function);
            assert_no_highlighted_token(text, &highlights, "LIKE", HighlightKind::Identifier);
        }
    }

    #[test]
    fn test_sql_overlay_styles_join_modifiers_as_keywords() {
        for profile in ["postgresql", "mysql", "sqlite"] {
            let text = "SELECT * FROM orders INNER JOIN customers ON orders.customer_id = customers.id CROSS JOIN regions FULL OUTER JOIN audits ON audits.id = orders.id;";
            let mut highlighter = SyntaxHighlighter::new().unwrap();
            highlighter.set_language_profile(profile);
            let highlights = highlighter.highlight(text);

            for keyword in ["INNER", "JOIN", "ON", "CROSS", "FULL", "OUTER"] {
                assert_highlighted_token(text, &highlights, keyword, HighlightKind::Keyword);
                assert_no_highlighted_token(text, &highlights, keyword, HighlightKind::Identifier);
            }
        }
    }

    #[test]
    fn test_sql_overlay_styles_set_and_case_keywords() {
        for profile in ["postgresql", "mysql", "sqlite"] {
            let text = "SELECT DISTINCT CASE WHEN status IN ('paid') THEN total ELSE 0 END AS amount FROM orders UNION ALL SELECT 0 EXCEPT SELECT 1 INTERSECT SELECT 2;";
            let mut highlighter = SyntaxHighlighter::new().unwrap();
            highlighter.set_language_profile(profile);
            let highlights = highlighter.highlight(text);

            for keyword in [
                "DISTINCT",
                "CASE",
                "WHEN",
                "IN",
                "THEN",
                "ELSE",
                "END",
                "UNION",
                "ALL",
                "EXCEPT",
                "INTERSECT",
            ] {
                assert_highlighted_token(text, &highlights, keyword, HighlightKind::Keyword);
                assert_no_highlighted_token(text, &highlights, keyword, HighlightKind::Identifier);
            }
            assert_no_highlighted_token(text, &highlights, "IN", HighlightKind::Function);
        }
    }

    #[test]
    fn test_sql_overlay_styles_join_window_and_filter_keywords() {
        for profile in ["postgresql", "mysql", "sqlite"] {
            let text = "SELECT * FROM users NATURAL LEFT JOIN teams USING (team_id) WINDOW recent AS (PARTITION BY team_id ORDER BY created_at) SELECT count(*) FILTER (WHERE active IS FALSE) OVER recent FROM users;";
            let mut highlighter = SyntaxHighlighter::new().unwrap();
            highlighter.set_language_profile(profile);
            let highlights = highlighter.highlight(text);

            for keyword in ["NATURAL", "USING", "WINDOW", "FILTER", "PARTITION", "OVER"] {
                assert_highlighted_token(text, &highlights, keyword, HighlightKind::Keyword);
                assert_no_highlighted_token(text, &highlights, keyword, HighlightKind::Identifier);
                assert_no_highlighted_token(text, &highlights, keyword, HighlightKind::Function);
            }
            assert_highlighted_token(text, &highlights, "FALSE", HighlightKind::Boolean);
        }
    }

    #[test]
    fn test_sql_overlay_styles_recursive_paging_keywords() {
        for profile in ["postgresql", "mysql", "sqlite"] {
            let text = "WITH RECURSIVE org AS (SELECT id FROM teams UNION ALL SELECT id FROM archived_teams) SELECT id FROM org ORDER BY id ASC LIMIT 10 OFFSET 5;";
            let mut highlighter = SyntaxHighlighter::new().unwrap();
            highlighter.set_language_profile(profile);
            let highlights = highlighter.highlight(text);

            for keyword in [
                "WITH",
                "RECURSIVE",
                "UNION",
                "ALL",
                "ORDER",
                "ASC",
                "LIMIT",
                "OFFSET",
            ] {
                assert_highlighted_token(text, &highlights, keyword, HighlightKind::Keyword);
                assert_no_highlighted_token(text, &highlights, keyword, HighlightKind::Identifier);
            }
        }
    }

    #[test]
    fn test_sql_overlay_styles_ddl_dml_and_privilege_keywords() {
        for profile in ["postgresql", "mysql", "sqlite"] {
            let text = "ALTER TABLE users ADD COLUMN archived_at TIMESTAMP; CREATE UNIQUE INDEX idx_users_email ON users(email); INSERT INTO users (id) VALUES (1); TRUNCATE TABLE logs RESTART IDENTITY CASCADE; GRANT SELECT ON TABLE users TO app_role; REVOKE DELETE ON TABLE users FROM app_role;";
            let mut highlighter = SyntaxHighlighter::new().unwrap();
            highlighter.set_language_profile(profile);
            let highlights = highlighter.highlight(text);

            for keyword in [
                "ALTER", "ADD", "COLUMN", "UNIQUE", "INSERT", "INTO", "TRUNCATE", "RESTART",
                "IDENTITY", "CASCADE", "GRANT", "REVOKE",
            ] {
                assert_highlighted_token(text, &highlights, keyword, HighlightKind::Keyword);
                assert_no_highlighted_token(text, &highlights, keyword, HighlightKind::Identifier);
            }
        }
    }

    #[test]
    fn test_mysql_driver_keywords_and_index_names() {
        let text = "CREATE TABLE `orders` (id BIGINT, KEY idx_status (status)) ENGINE=InnoDB; INSERT IGNORE INTO `orders` VALUES (1); SELECT SQL_CALC_FOUND_ROWS * FROM `orders` WHERE status REGEXP '^p' ON DUPLICATE KEY UPDATE id = VALUES(id);";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("mysql");
        let highlights = highlighter.highlight(text);

        for keyword in [
            "ENGINE",
            "IGNORE",
            "SQL_CALC_FOUND_ROWS",
            "REGEXP",
            "DUPLICATE",
        ] {
            assert_highlighted_token(text, &highlights, keyword, HighlightKind::Keyword);
            assert_no_highlighted_token(text, &highlights, keyword, HighlightKind::Identifier);
        }
        assert_highlighted_token(text, &highlights, "idx_status", HighlightKind::Identifier);
        assert_no_highlighted_token(text, &highlights, "idx_status", HighlightKind::Function);
    }

    #[test]
    fn test_sqlite_driver_virtual_table_and_match_keywords() {
        let text = "CREATE VIRTUAL TABLE docs USING fts5(title, body); INSERT OR REPLACE INTO docs(rowid, title, body) VALUES (1, 'a', 'b'); SELECT rowid FROM docs WHERE docs MATCH 'hello' LIMIT 5 OFFSET 1;";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("sqlite");
        let highlights = highlighter.highlight(text);

        for keyword in ["VIRTUAL", "USING", "REPLACE", "MATCH", "OFFSET"] {
            assert_highlighted_token(text, &highlights, keyword, HighlightKind::Keyword);
            assert_no_highlighted_token(text, &highlights, keyword, HighlightKind::Identifier);
        }
        assert_highlighted_token(text, &highlights, "fts5", HighlightKind::Function);
    }

    #[test]
    fn test_mssql_merge_keywords() {
        let text = "MERGE INTO [Users] AS target USING [StagingUsers] AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name);";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("mssql");
        let highlights = highlighter.highlight(text);

        for keyword in ["MERGE", "INTO", "USING", "WHEN", "MATCHED", "THEN"] {
            assert_highlighted_token(text, &highlights, keyword, HighlightKind::Keyword);
            assert_no_highlighted_token(text, &highlights, keyword, HighlightKind::Identifier);
        }
    }

    #[test]
    fn test_clickhouse_interval_rollup_and_format_keywords() {
        let text = "CREATE TABLE events (id UInt64, ts DateTime64(3)) ENGINE = MergeTree() TTL ts + INTERVAL 7 DAY SETTINGS index_granularity = 8192; SELECT uniqExact(id) FROM events PREWHERE ts >= now() - INTERVAL 1 DAY GROUP BY id WITH ROLLUP FORMAT JSONEachRow;";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("clickhouse");
        let highlights = highlighter.highlight(text);

        for keyword in [
            "TTL", "INTERVAL", "DAY", "SETTINGS", "PREWHERE", "ROLLUP", "FORMAT",
        ] {
            assert_highlighted_token(text, &highlights, keyword, HighlightKind::Keyword);
            assert_no_highlighted_token(text, &highlights, keyword, HighlightKind::Identifier);
        }
        assert_highlighted_token(text, &highlights, "JSONEachRow", HighlightKind::Type);
    }

    #[test]
    fn test_duckdb_copy_sample_keywords_and_day_alias() {
        let text = "CREATE TEMP TABLE events AS SELECT * FROM read_parquet('orders.parquet'); SELECT date_part('day', ts) AS day FROM events SAMPLE 10 PERCENT; COPY events TO 'events.csv' (HEADER, DELIMITER ',');";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("duckdb");
        let highlights = highlighter.highlight(text);

        for keyword in ["TEMP", "SAMPLE", "PERCENT", "COPY", "HEADER", "DELIMITER"] {
            assert_highlighted_token(text, &highlights, keyword, HighlightKind::Keyword);
            assert_no_highlighted_token(text, &highlights, keyword, HighlightKind::Identifier);
        }
        assert_highlighted_token(text, &highlights, "date_part", HighlightKind::Function);
        assert_highlighted_token(text, &highlights, "day", HighlightKind::Identifier);
        assert_no_highlighted_token(text, &highlights, "day", HighlightKind::Keyword);
    }

    #[test]
    fn test_mysql_double_quoted_literals_are_strings() {
        let text = r#"SELECT JSON_EXTRACT(payload, "$.name") FROM `orders`;"#;
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("mysql");
        let highlights = highlighter.highlight(text);

        assert_highlighted_token(text, &highlights, r#""$.name""#, HighlightKind::String);
        assert_no_highlighted_token(text, &highlights, r#""$.name""#, HighlightKind::Identifier);
    }

    #[test]
    fn test_sqlite_bracket_identifiers_are_single_identifier_ranges() {
        let text = "SELECT [weird name], `other name`, [col:name] FROM t;";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("sqlite");
        let highlights = highlighter.highlight(text);

        assert_highlighted_token(text, &highlights, "[weird name]", HighlightKind::Identifier);
        assert_highlighted_token(text, &highlights, "`other name`", HighlightKind::Identifier);
        assert_highlighted_token(text, &highlights, "[col:name]", HighlightKind::Identifier);
        assert_no_highlighted_token(text, &highlights, "weird", HighlightKind::Identifier);
        assert_no_highlighted_token(text, &highlights, "name", HighlightKind::Identifier);
        assert_no_highlighted_token(text, &highlights, "name", HighlightKind::Keyword);
    }

    #[test]
    fn test_sqlite_function_call_context_wins_over_type_name() {
        let text = "CREATE TABLE events (created_at DATETIME); SELECT datetime('now'), date('now') FROM events;";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("sqlite");
        let highlights = highlighter.highlight(text);

        assert_highlighted_token(text, &highlights, "DATETIME", HighlightKind::Type);
        assert_highlighted_token(text, &highlights, "datetime", HighlightKind::Function);
        assert_highlighted_token(text, &highlights, "date", HighlightKind::Function);
    }

    #[test]
    fn test_clickhouse_backtick_identifier_and_typed_placeholder() {
        let text = "SELECT `user id` FROM events WHERE id = {id:UInt64};";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("clickhouse");
        let highlights = highlighter.highlight(text);

        assert_highlighted_token(text, &highlights, "`user id`", HighlightKind::Identifier);
        assert_no_highlighted_token(text, &highlights, ":UInt64", HighlightKind::Parameter);
    }

    #[test]
    fn test_sql_prefixed_string_literal_protects_parameter_like_text() {
        let text = "SELECT N'literal @x', 'literal :y';";
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("mssql");
        let highlights = highlighter.highlight(text);

        assert_highlighted_token(text, &highlights, "N'literal @x'", HighlightKind::String);
        assert_highlighted_token(text, &highlights, "'literal :y'", HighlightKind::String);
        assert_no_highlighted_token(text, &highlights, "@x", HighlightKind::Parameter);
        assert_no_highlighted_token(text, &highlights, ":y", HighlightKind::Parameter);
    }

    #[test]
    fn test_dialect_profile_classifies_extra_keywords() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        assert_eq!(highlighter.language_profile(), "sql");
        assert_eq!(
            highlighter.classify_identifier_text("jsonb"),
            HighlightKind::Default
        );

        highlighter.set_language_profile("postgresql");
        assert_eq!(highlighter.language_profile(), "postgresql");
        assert_eq!(
            highlighter.classify_identifier_text("jsonb"),
            HighlightKind::Type
        );
    }

    #[test]
    fn test_language_profile_aliases_use_core_syntax_normalization() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();

        for (alias, normalized, token, kind) in [
            ("postgres", "postgresql", "jsonb", HighlightKind::Type),
            ("mariadb", "mysql", "FORCE", HighlightKind::Keyword),
            ("turso", "sqlite", "INDEXED", HighlightKind::Keyword),
            ("sqlserver", "mssql", "TOP", HighlightKind::Keyword),
            ("mongo", "mongodb", "NumberLong", HighlightKind::Type),
        ] {
            highlighter.set_language_profile(alias);
            assert_eq!(highlighter.language_profile(), normalized);
            assert_eq!(
                highlighter.classify_identifier_text(token),
                kind,
                "{alias} should classify {token}"
            );
        }
    }

    #[test]
    fn test_syntax_capabilities_are_cached_with_profile() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        assert_eq!(highlighter.syntax_capabilities.profile, "sql");
        assert!(highlighter.syntax_capabilities.sql_overlays);
        assert!(!highlighter.syntax_capabilities.command_syntax);

        highlighter.set_language_profile("redis");
        assert_eq!(highlighter.syntax_capabilities.profile, "redis");
        assert!(highlighter.syntax_capabilities.command_syntax);
        assert!(!highlighter.syntax_capabilities.sql_overlays);

        highlighter.set_language_profile("mongo");
        assert_eq!(highlighter.syntax_capabilities.profile, "mongodb");
        assert!(highlighter.syntax_capabilities.document_syntax);
        assert!(!highlighter.syntax_capabilities.command_syntax);
    }

    #[test]
    fn test_syntax_capabilities_override_drives_overlay_profile() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_syntax_capabilities_override(get_syntax_driver_capabilities("redis"));

        let text = "GET user:1 # cached";
        let highlights = highlighter.highlight(text);

        assert_eq!(highlighter.language_profile(), "sql");
        assert_eq!(highlighter.syntax_capabilities.profile, "redis");
        assert_highlighted_token(text, &highlights, "GET", HighlightKind::Keyword);
        assert_highlighted_token(text, &highlights, "# cached", HighlightKind::Comment);

        highlighter.clear_syntax_capabilities_override();
        assert_eq!(highlighter.syntax_capabilities.profile, "sql");
        assert_no_highlighted_token(
            text,
            &highlighter.highlight(text),
            "GET",
            HighlightKind::Keyword,
        );
    }

    #[test]
    fn test_syntax_capabilities_select_parser_and_query_language() {
        let capabilities = get_syntax_driver_capabilities("mongodb");
        let language = language_for_grammar(&capabilities.tree_sitter_grammar);
        let query_source =
            highlight_query_source_for_language(capabilities.highlight_query_language);

        Query::new(&language, query_source).expect("mongodb query compiles against JS grammar");

        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_syntax_capabilities_override(capabilities);
        let text = "db.users.find({ _id: ObjectId(\"abc\") })";
        let highlights = highlighter.highlight(text);

        assert_eq!(highlighter.syntax_capabilities.profile, "mongodb");
        assert_highlighted_token(text, &highlights, "find", HighlightKind::Function);
        assert_highlighted_token(text, &highlights, "ObjectId", HighlightKind::Type);
    }

    #[test]
    fn test_all_dialect_highlight_queries_compile_for_selected_parser() {
        for profile in [
            "sql",
            "postgresql",
            "mysql",
            "sqlite",
            "duckdb",
            "mssql",
            "clickhouse",
            "mongodb",
            "redis",
        ] {
            let language = language_for_profile(profile);
            let query_source = highlight_query_source_for_parser(profile);
            Query::new(&language, query_source).unwrap_or_else(|error| {
                panic!("{profile} highlight query should compile: {error}")
            });
        }
    }

    #[test]
    fn test_driver_fixture_highlight_assignments_are_satisfactory() {
        for fixture in syntax_quality_fixtures() {
            let mut highlighter = SyntaxHighlighter::new().unwrap();
            highlighter.set_language_profile(fixture.profile);
            let highlights = highlighter.highlight(fixture.text);
            let coverage = highlight_coverage(fixture.text, &highlights);
            let assignment_snapshot =
                assignment_snapshot_for_tokens(fixture.text, &highlights, fixture.expected_tokens);

            assert!(
                coverage.percent >= 99.5,
                "{profile} fixture coverage too low: {:.2}% ({}/{}), gaps: {:?}, assignment snapshot: {assignment_snapshot:?}",
                coverage.percent,
                coverage.styled_non_ws_bytes,
                coverage.non_ws_bytes,
                coverage.gaps,
                profile = fixture.profile,
            );

            for (token, kind) in fixture.expected_tokens {
                let assigned = assigned_kinds_for_token(fixture.text, &highlights, token);
                assert!(
                    assigned.contains(kind),
                    "{profile} expected {token:?} as {kind:?}, assignment snapshot: {assignment_snapshot:?}",
                    profile = fixture.profile,
                );
            }

            for (token, kind) in fixture.rejected_tokens {
                let assigned = assigned_kinds_for_token(fixture.text, &highlights, token);
                assert!(
                    !assigned.contains(kind),
                    "{profile} did not expect {token:?} as {kind:?}, assignment snapshot: {assignment_snapshot:?}",
                    profile = fixture.profile,
                );
            }
        }
    }

    #[test]
    fn test_viewport_range_highlights_match_full_document_for_driver_fixtures() {
        let fixtures = [
            (
                "postgresql",
                "CREATE TABLE events (\n  id UUID,\n  payload JSONB,\n  title text\n);\nSELECT * FROM events WHERE id = $1;\n",
                "payload JSONB",
            ),
            (
                "mysql",
                "CREATE TABLE orders (\n  id BIGINT UNSIGNED,\n  status ENUM('new', 'paid')\n);\nSELECT GROUP_CONCAT(status) FROM orders;\n",
                "id BIGINT UNSIGNED",
            ),
            (
                "sqlite",
                "CREATE TABLE categories (\n  child_count integer,\n  sort_order real(14, 3)\n);\nSELECT json_extract(metadata, '$.seo') FROM categories;\n",
                "sort_order real",
            ),
            (
                "duckdb",
                "COPY (SELECT LIST_VALUE(user_id) AS ids FROM read_parquet('orders.parquet') QUALIFY row_number() OVER (PARTITION BY user_id) = 1) TO 'orders.csv';\n",
                "QUALIFY row_number()",
            ),
            (
                "mssql",
                "SELECT TOP 10 [User Name], JSON_VALUE(payload, '$.id') AS id FROM [dbo].[Events] ORDER BY created_at OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;\n",
                "[User Name], JSON_VALUE",
            ),
            (
                "clickhouse",
                "CREATE TABLE events (\n  id UInt64,\n  created_at DateTime64(3)\n) ENGINE = MergeTree()\nORDER BY id;\n",
                "ENGINE = MergeTree()",
            ),
            (
                "mongodb",
                "db.orders.aggregate([\n  { \"$match\": { \"status\": \"paid\" } },\n  { \"$group\": { \"_id\": \"$customerId\" } }\n])\n",
                "\"$group\"",
            ),
            (
                "redis",
                "PING\nHGETALL user:42\nJSON.GET user:42 $.profile\n",
                "JSON.GET",
            ),
        ];

        for (profile, source, viewport_text) in fixtures {
            let mut full_highlighter = SyntaxHighlighter::new().unwrap();
            full_highlighter.set_language_profile(profile);
            let full_highlights = full_highlighter.highlight(source);

            let mut range_highlighter = SyntaxHighlighter::new().unwrap();
            range_highlighter.set_language_profile(profile);
            let rope = Rope::from_str(source);
            let start = source.find(viewport_text).expect("viewport text");
            let end = start + viewport_text.len();
            let range_highlights = range_highlighter.highlight_rope_range(&rope, start..end);
            let clipped_full = clipped_full_highlights_for_range(full_highlights, start..end);

            assert_eq!(
                range_highlights, clipped_full,
                "{profile} range highlights should match clipped full highlights for {viewport_text:?}"
            );
        }
    }

    #[test]
    fn test_large_document_viewport_highlighting_stays_local_and_complete() {
        let mut source = String::new();
        for index in 0..2_000 {
            source.push_str(&format!("SELECT {index} AS value_{index};\n"));
        }
        source.push_str(
            "CREATE TABLE events (\n  id UUID,\n  payload JSONB,\n  created_at TIMESTAMPTZ\n);\n",
        );
        for index in 0..2_000 {
            source.push_str(&format!("SELECT value_{index} FROM archive_{index};\n"));
        }

        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let rope = Rope::from_str(&source);
        let viewport_start = source.find("payload JSONB").expect("payload line");
        let viewport_end = source[viewport_start..]
            .find('\n')
            .map_or(source.len(), |index| viewport_start + index);

        let highlights = highlighter.highlight_rope_range(&rope, viewport_start..viewport_end);

        assert_highlighted_token(&source, &highlights, "payload", HighlightKind::Identifier);
        assert_highlighted_token(&source, &highlights, "JSONB", HighlightKind::Type);
        assert!(
            highlights
                .iter()
                .all(|highlight| highlight.start >= viewport_start && highlight.end <= viewport_end)
        );
        assert!(
            highlights.len() <= 4,
            "viewport-only highlight should not return unrelated document tokens: {:?}",
            highlighted_tokens(&source, &highlights)
        );
    }

    #[test]
    fn test_dialect_profile_classifies_types_and_functions() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();

        highlighter.set_language_profile("clickhouse");
        assert_eq!(
            highlighter.classify_identifier_text("LowCardinality"),
            HighlightKind::Type
        );
        assert_eq!(
            highlighter.classify_identifier_text("uniq"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("arrayJoin"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("toDateTime64"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("Ring"),
            HighlightKind::Type
        );

        highlighter.set_language_profile("postgresql");
        assert_eq!(
            highlighter.classify_identifier_text("JSONB_SET"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("generate_series"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("TSVECTOR"),
            HighlightKind::Type
        );
        assert_eq!(
            highlighter.classify_identifier_text("MACADDR8"),
            HighlightKind::Type
        );

        highlighter.set_language_profile("mysql");
        assert_eq!(
            highlighter.classify_identifier_text("GROUP_CONCAT"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("JSON_OBJECT"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("ROW_NUMBER"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("TINYINT"),
            HighlightKind::Type
        );
        assert_eq!(
            highlighter.classify_identifier_text("UNSIGNED"),
            HighlightKind::Type
        );

        highlighter.set_language_profile("sqlite");
        assert_eq!(
            highlighter.classify_identifier_text("json_object"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("julianday"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("typeof"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("ANY"),
            HighlightKind::Type
        );

        highlighter.set_language_profile("duckdb");
        assert_eq!(
            highlighter.classify_identifier_text("list_contains"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("UUID"),
            HighlightKind::Type
        );

        highlighter.set_language_profile("mssql");
        assert_eq!(
            highlighter.classify_identifier_text("PATINDEX"),
            HighlightKind::Function
        );
        assert_eq!(
            highlighter.classify_identifier_text("SYSNAME"),
            HighlightKind::Type
        );

        highlighter.set_language_profile("clickhouse");
        assert_eq!(
            highlighter.classify_identifier_text("ENGINE"),
            HighlightKind::Keyword
        );
        assert_eq!(
            highlighter.classify_identifier_text("MergeTree"),
            HighlightKind::Type
        );
    }

    #[test]
    fn test_clickhouse_engine_names_highlight_as_types() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("clickhouse");

        let text = "CREATE TABLE events (id UInt64) ENGINE = MergeTree()";
        let highlights = highlighter.highlight(text);

        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Keyword
                && &text[highlight.start..highlight.end] == "ENGINE"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Type
                && &text[highlight.start..highlight.end] == "UInt64"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Type
                && &text[highlight.start..highlight.end] == "MergeTree"
        }));
        assert!(!highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Function
                && &text[highlight.start..highlight.end] == "MergeTree"
        }));
    }

    #[test]
    fn test_dialect_query_maps_type_qualifiers() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("mysql");
        let text = "CREATE TABLE users (id INT UNSIGNED)";
        let highlights = highlighter.highlight(text);

        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Type
                && &text[highlight.start..highlight.end] == "UNSIGNED"
        }));
    }

    #[test]
    fn test_sql_parameter_placeholders_are_highlighted() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let text =
            "SELECT * FROM users WHERE id = $1 AND email = :email AND role = @role AND active = ?";
        let highlights = highlighter.highlight(text);

        for parameter in ["$1", ":email", "@role"] {
            assert!(
                highlights.iter().any(|highlight| {
                    highlight.kind == HighlightKind::Parameter
                        && &text[highlight.start..highlight.end] == parameter
                }),
                "{parameter} should be highlighted as a parameter"
            );
        }
        assert!(!highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Parameter
                && &text[highlight.start..highlight.end] == "?"
        }));

        highlighter.set_language_profile("mysql");
        let mysql_text = "SELECT * FROM users WHERE active = ?";
        let mysql_highlights = highlighter.highlight(mysql_text);
        assert!(mysql_highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Parameter
                && &mysql_text[highlight.start..highlight.end] == "?"
        }));
    }

    #[test]
    fn test_sql_parameter_overlay_ignores_strings_comments_and_casts() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = "SELECT ':nope', value::text, \":quoted\", `@tick`, [@bracket] FROM users -- @ignored ?\nWHERE id = :id";
        let highlights = highlighter.highlight(text);

        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Parameter
                && &text[highlight.start..highlight.end] == ":id"
        }));
        for ignored in [
            ":nope", ":text", ":quoted", "@tick", "@bracket", "@ignored", "?",
        ] {
            assert!(
                !highlights.iter().any(|highlight| {
                    highlight.kind == HighlightKind::Parameter
                        && &text[highlight.start..highlight.end] == ignored
                }),
                "{ignored} should not be highlighted as a parameter"
            );
        }
    }

    #[test]
    fn test_postgres_dollar_quoted_strings_protect_parameters() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let text = r#"CREATE FUNCTION demo() RETURNS trigger AS $body$
BEGIN
  NEW.payload := jsonb_build_object('id', $1, 'name', :name);
  RETURN NEW;
END;
$body$ LANGUAGE plpgsql;
SELECT $1;"#;
        let highlights = highlighter.highlight(text);

        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::String
                && text[highlight.start..highlight.end].starts_with("$body$")
                && text[highlight.start..highlight.end].ends_with("$body$")
        }));
        assert_highlighted_token(text, &highlights, "plpgsql", HighlightKind::Keyword);
        assert_highlighted_token(text, &highlights, "$1", HighlightKind::Parameter);
        assert_no_highlighted_token(text, &highlights, ":name", HighlightKind::Parameter);
    }

    #[test]
    fn test_postgres_dollar_quoted_strings_protect_range_highlighting() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let source = "CREATE FUNCTION demo() RETURNS void AS $$\nSELECT :inside_body;\n$$ LANGUAGE plpgsql;\nSELECT :outside_body;\n";
        let text = Rope::from_str(source);
        let range_start = source.find(":inside_body").expect("inside token");
        let range_end = range_start + ":inside_body".len();
        let highlights = highlighter.highlight_rope_range(&text, range_start..range_end);

        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::String
                && highlight.start <= range_start
                && highlight.end >= range_end
        }));
        assert_no_highlighted_token(
            source,
            &highlights,
            ":inside_body",
            HighlightKind::Parameter,
        );
    }

    #[test]
    fn test_postgres_dollar_range_highlighting_skips_unrelated_tail() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let mut source =
            "CREATE FUNCTION demo() RETURNS void AS $$\nSELECT :inside_body;\n$$ LANGUAGE plpgsql;\n"
                .to_string();
        for index in 0..5000 {
            source.push_str(&format!("SELECT {index} AS value;\n"));
        }
        let text = Rope::from_str(&source);
        let range_start = source.find(":inside_body").expect("inside token");
        let range_end = range_start + ":inside_body".len();
        let range_highlights = highlighter.highlight_rope_range(&text, range_start..range_end);
        let full_highlights = highlighter.highlight_rope(&text);

        assert_highlighted_token(
            &source,
            &range_highlights,
            ":inside_body",
            HighlightKind::String,
        );
        assert_no_highlighted_token(
            &source,
            &range_highlights,
            ":inside_body",
            HighlightKind::Parameter,
        );
        let clipped_full_highlights = full_highlights
            .into_iter()
            .filter_map(|highlight| {
                (highlight.start < range_end && highlight.end > range_start).then_some(Highlight {
                    start: highlight.start.max(range_start),
                    end: highlight.end.min(range_end),
                    kind: highlight.kind,
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            highlighted_tokens(&source, &range_highlights),
            highlighted_tokens(&source, &clipped_full_highlights)
        );
    }

    #[test]
    fn test_sql_parameter_rope_range_ignores_multiline_block_comment() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let source = "SELECT 1\n/* ignored :comment\nstill ignored $2 */\nWHERE id = :id";
        let text = Rope::from_str(source);
        let range_start = source.find("still ignored").expect("comment line");
        let range_end = source.len();

        let highlights = highlighter.highlight_rope_range(&text, range_start..range_end);

        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Parameter
                && text.byte_slice(highlight.start..highlight.end) == ":id"
        }));
        assert!(!highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Parameter
                && text.byte_slice(highlight.start..highlight.end) == "$2"
        }));
    }

    #[test]
    fn test_dialect_rope_range_ignores_multiline_block_comment_context() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let source =
            "SELECT 1\n/* ignored JSONB\nstill ignored UUID */\nCREATE TABLE events (id UUID)";
        let text = Rope::from_str(source);
        let range_start = source.find("still ignored").expect("comment line");
        let range_end = source.len();

        let highlights = highlighter.highlight_rope_range(&text, range_start..range_end);
        let full_highlights = highlighter.highlight_rope(&text);

        let comment_end = source.find("*/").expect("comment end") + "*/".len();
        for highlights in [&highlights, &full_highlights] {
            assert!(
                !highlights.iter().any(|highlight| {
                    highlight.kind == HighlightKind::Type
                        && highlight.start < comment_end
                        && highlight.end > range_start
                }),
                "comment body should not contain type highlights: {:?}",
                highlighted_tokens(source, highlights)
            );
        }
        assert_highlighted_token(source, &highlights, "CREATE", HighlightKind::Keyword);
        assert_highlighted_token(source, &highlights, "UUID", HighlightKind::Type);
        assert_highlighted_token(source, &full_highlights, "CREATE", HighlightKind::Keyword);
        assert_highlighted_token(source, &full_highlights, "UUID", HighlightKind::Type);
    }

    #[test]
    fn test_dialect_profile_highlights_create_table_types() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let text = "CREATE TABLE events (id UUID, payload JSONB, created_at TIMESTAMPTZ)";
        let highlights = highlighter.highlight(text);

        for type_name in ["UUID", "JSONB", "TIMESTAMPTZ"] {
            assert!(
                highlights.iter().any(|highlight| {
                    highlight.kind == HighlightKind::Type
                        && &text[highlight.start..highlight.end] == type_name
                }),
                "{type_name} should be highlighted as a type"
            );
        }
    }

    #[test]
    fn test_mongodb_profile_uses_document_syntax() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("mongodb");

        assert_eq!(highlighter.language_profile(), "mongodb");

        let text = r#"db.products.find(
  { $text: { $search: "developer iot" } },
  { sku: 1, name: 1, tags: 1 }
).sort({ score: { $meta: "textScore" } }).limit(5)"#;
        let highlights = highlighter.highlight(text);

        assert!(
            highlights
                .iter()
                .all(|highlight| highlight.kind != HighlightKind::Error)
        );
        assert!(
            highlights
                .iter()
                .any(|highlight| highlight.kind == HighlightKind::Function)
        );
        assert!(
            highlights
                .iter()
                .any(|highlight| highlight.kind == HighlightKind::String)
        );
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Operator
                && &text[highlight.start..highlight.end] == "$text"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Function
                && &text[highlight.start..highlight.end] == "find"
        }));
    }

    #[test]
    fn test_mongodb_profile_highlights_quoted_pipeline_operators() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("mongodb");
        let text = r#"db.orders.aggregate([{ "$match": { "status": "paid" } }, { "$group": { "_id": "$customerId" } }])"#;
        let highlights = highlighter.highlight(text);

        for operator in ["$match", "$group", "$customerId"] {
            assert!(
                highlights.iter().any(|highlight| {
                    highlight.kind == HighlightKind::Operator
                        && &text[highlight.start..highlight.end] == operator
                }),
                "{operator} should be highlighted as a MongoDB operator"
            );
        }
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Function
                && &text[highlight.start..highlight.end] == "aggregate"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Punctuation
                && &text[highlight.start..highlight.end] == ":"
        }));
    }

    #[test]
    fn test_mongodb_profile_keeps_string_values_and_comments_protected() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("mongodb");
        let text =
            r#"db.logs.find({ "$match": "aggregate should stay string" }) // createIndex ignored"#;
        let highlights = render_highlight_runs(text, &highlighter.highlight(text));

        assert_highlighted_token(text, &highlights, "$match", HighlightKind::Operator);
        assert_highlighted_token(
            text,
            &highlights,
            r#""aggregate should stay string""#,
            HighlightKind::String,
        );
        assert_highlighted_token(
            text,
            &highlights,
            "// createIndex ignored",
            HighlightKind::Comment,
        );
        assert_no_highlighted_token(text, &highlights, "aggregate", HighlightKind::Function);
        assert_no_highlighted_token(text, &highlights, "createIndex", HighlightKind::Function);
    }

    #[test]
    fn test_mongodb_range_highlighting_does_not_paint_parser_errors_over_document_tokens() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("mongodb");
        let text = Rope::from_str(
            r#"db.orders.aggregate([{ $match: { status: "paid" } }, { $project: { total: 1 } }])"#,
        );
        let highlights = render_highlight_runs(
            &text.to_string(),
            &highlighter.highlight_rope_range(&text, 0..42),
        );

        assert_highlighted_token(
            &text.to_string(),
            &highlights,
            "db",
            HighlightKind::Identifier,
        );
        assert_highlighted_token(
            &text.to_string(),
            &highlights,
            "$match",
            HighlightKind::Operator,
        );
        assert_no_highlighted_token(&text.to_string(), &highlights, "db", HighlightKind::Error);
    }

    #[test]
    fn test_mongodb_profile_does_not_inherit_sql_base_keywords() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("mongodb");
        let text = "db.orders.createIndex({ email: 1 }, { unique: true })";
        let highlights = render_highlight_runs(text, &highlighter.highlight(text));

        assert_highlighted_token(text, &highlights, "createIndex", HighlightKind::Function);
        assert_highlighted_token(text, &highlights, "unique", HighlightKind::Identifier);
        assert_no_highlighted_token(text, &highlights, "unique", HighlightKind::Keyword);
        assert_highlighted_token(text, &highlights, "true", HighlightKind::Boolean);
    }

    #[test]
    fn test_mongodb_rope_range_highlights_visible_operator_only() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("mongodb");
        let source = "db.orders.aggregate([\n  { \"$match\": { \"status\": \"paid\" } },\n  { \"$group\": { \"_id\": \"$customerId\" } }\n])";
        let text = Rope::from_str(source);
        let range_start = source.find("\"$group\"").expect("group operator");
        let range_end = source[range_start..]
            .find('\n')
            .map_or(source.len(), |index| range_start + index);

        let highlights = highlighter.highlight_rope_range(&text, range_start..range_end);

        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Operator
                && text.byte_slice(highlight.start..highlight.end) == "$group"
        }));
        assert!(!highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Operator
                && text.byte_slice(highlight.start..highlight.end) == "$match"
        }));
    }

    #[test]
    fn test_redis_profile_highlights_commands() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("redis");

        assert_eq!(highlighter.language_profile(), "redis");
        assert_eq!(
            highlighter.classify_identifier_text("HGETALL"),
            HighlightKind::Keyword
        );
        assert_eq!(
            highlighter.classify_identifier_text("WITHSCORES"),
            HighlightKind::Keyword
        );

        let text = "ZRANGE leaderboard 0 -1 WITHSCORES";
        let highlights = highlighter.highlight(text);
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Keyword
                && &text[highlight.start..highlight.end] == "ZRANGE"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Keyword
                && &text[highlight.start..highlight.end] == "WITHSCORES"
        }));
    }

    #[test]
    fn test_redis_rope_range_uses_core_command_tokenizer() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("redis");
        let source =
            "PING\n  SET user:42:name \"Ada Lovelace\" # cached profile\nGET user:42:name\n";
        let text = Rope::from_str(source);
        let range_start = source.find("SET").expect("SET command");
        let range_end = source.find("GET").expect("GET command");

        let highlights = highlighter.highlight_rope_range(&text, range_start..range_end);

        assert_highlighted_token(source, &highlights, "SET", HighlightKind::Keyword);
        assert_highlighted_token(
            source,
            &highlights,
            "\"Ada Lovelace\"",
            HighlightKind::String,
        );
        assert_highlighted_token(
            source,
            &highlights,
            "# cached profile",
            HighlightKind::Comment,
        );
        assert_no_highlighted_token(source, &highlights, "GET", HighlightKind::Keyword);
    }

    #[test]
    fn test_redis_full_rope_highlighting_matches_string_highlighting() {
        let source = "HSET user:1 name \"Ada\"\n# cached profile\nZRANGE queue 0 -1 WITHSCORES\n";
        let rope = Rope::from_str(source);
        let mut string_highlighter = SyntaxHighlighter::new().unwrap();
        string_highlighter.set_language_profile("redis");
        let string_highlights = string_highlighter.highlight(source);

        let mut rope_highlighter = SyntaxHighlighter::new().unwrap();
        rope_highlighter.set_language_profile("redis");
        let rope_highlights = rope_highlighter.highlight_rope(&rope);

        assert_eq!(rope_highlights, string_highlights);
        assert_highlighted_token(
            source,
            &rope_highlights,
            "# cached profile",
            HighlightKind::Comment,
        );
        assert_highlighted_token(
            source,
            &rope_highlights,
            "WITHSCORES",
            HighlightKind::Keyword,
        );
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
    fn test_render_highlight_runs_resolves_ranked_overlaps() {
        let text = r#"CREATE TABLE "categories" ("category_id" text)"#;
        let categories = text.find(r#""categories""#).expect("categories");
        let categories_end = categories + r#""categories""#.len();
        let text_type = text.find("text").expect("text type");
        let text_type_end = text_type + "text".len();
        let highlights = vec![
            Highlight {
                start: categories,
                end: categories_end,
                kind: HighlightKind::String,
            },
            Highlight {
                start: categories,
                end: categories_end,
                kind: HighlightKind::Identifier,
            },
            Highlight {
                start: text_type,
                end: text_type_end,
                kind: HighlightKind::Identifier,
            },
            Highlight {
                start: text_type,
                end: text_type_end,
                kind: HighlightKind::Type,
            },
        ];

        let rendered = render_highlight_runs(text, &highlights);

        assert!(rendered.iter().any(|highlight| {
            highlight.kind == HighlightKind::Identifier
                && &text[highlight.start..highlight.end] == r#""categories""#
        }));
        assert!(rendered.iter().any(|highlight| {
            highlight.kind == HighlightKind::Type && &text[highlight.start..highlight.end] == "text"
        }));
        assert!(!rendered.iter().any(|highlight| {
            highlight.kind == HighlightKind::String
                && &text[highlight.start..highlight.end] == r#""categories""#
        }));
    }

    #[test]
    fn test_render_highlight_runs_handles_dense_overlaps() {
        let text = "SELECT ".repeat(256);
        let mut highlights = Vec::new();
        for start in (0..text.len()).step_by(3) {
            let end = (start + 24).min(text.len());
            if start < end {
                highlights.push(Highlight {
                    start,
                    end,
                    kind: HighlightKind::String,
                });
                highlights.push(Highlight {
                    start,
                    end,
                    kind: HighlightKind::Keyword,
                });
            }
        }

        let rendered = render_highlight_runs(&text, &highlights);

        assert!(!rendered.is_empty());
        assert!(
            rendered
                .iter()
                .all(|highlight| highlight.start < highlight.end)
        );
        assert!(
            rendered
                .iter()
                .any(|highlight| highlight.kind == HighlightKind::Keyword)
        );
        assert!(
            !rendered
                .iter()
                .any(|highlight| highlight.kind == HighlightKind::String)
        );
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
    fn test_highlight_rope_range_clamps_utf8_byte_boundaries() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = Rope::from_str("SELECT 'éclair' FROM café");
        let split_inside_e = "SELECT '".len() + 1;

        let highlights = highlighter.highlight_rope_range(&text, split_inside_e..text.len_bytes());

        assert!(
            highlights
                .iter()
                .all(|highlight| text.try_byte_to_char(highlight.start).is_ok()
                    && text.try_byte_to_char(highlight.end).is_ok())
        );
    }

    #[test]
    fn test_highlight_rope_range_keeps_dialect_context_from_full_document() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let text = Rope::from_str("CREATE TABLE events (\n  id UUID,\n  payload JSONB\n)");
        let jsonb_start = text.to_string().find("JSONB").expect("jsonb");

        let highlights = highlighter.highlight_rope_range(&text, jsonb_start..jsonb_start + 5);

        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Type
                && text.byte_slice(highlight.start..highlight.end) == "JSONB"
        }));
    }

    #[test]
    fn test_clear_syntax_term_overrides_restores_profile_terms() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        let text = "SELECT value::JSONPATH";

        assert!(!highlighter.highlight(text).iter().any(|highlight| {
            highlight.kind == HighlightKind::Type
                && &text[highlight.start..highlight.end] == "JSONPATH"
        }));

        highlighter.set_syntax_term_overrides(SyntaxTermOverrides {
            types: vec!["JSONPATH".to_string()],
            ..Default::default()
        });

        assert!(highlighter.highlight(text).iter().any(|highlight| {
            highlight.kind == HighlightKind::Type
                && &text[highlight.start..highlight.end] == "JSONPATH"
        }));

        highlighter.clear_syntax_term_overrides();

        assert!(!highlighter.highlight(text).iter().any(|highlight| {
            highlight.kind == HighlightKind::Type
                && &text[highlight.start..highlight.end] == "JSONPATH"
        }));
    }

    #[test]
    fn syntax_term_overrides_from_config_skips_empty_terms() {
        let empty = SyntaxTermsConfig::default();
        assert_eq!(SyntaxTermOverrides::from_config(&empty), None);

        let config = SyntaxTermsConfig {
            keywords: vec!["GET".to_string()],
            functions: vec!["JSON.GET".to_string()],
            types: vec!["hash".to_string()],
        };

        assert_eq!(
            SyntaxTermOverrides::from_config(&config),
            Some(SyntaxTermOverrides {
                keywords: vec!["GET".to_string()],
                functions: vec!["JSON.GET".to_string()],
                types: vec!["hash".to_string()],
            })
        );
    }

    #[test]
    fn test_driver_syntax_terms_replace_profile_dialect_terms() {
        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let text = "CREATE TABLE events (name CITEXT, payload JSONB)";

        assert!(highlighter.highlight(text).iter().any(|highlight| {
            highlight.kind == HighlightKind::Type
                && &text[highlight.start..highlight.end] == "CITEXT"
        }));

        highlighter.set_driver_syntax_terms(SyntaxTermOverrides {
            types: vec!["JSONB".to_string()],
            ..Default::default()
        });

        let highlights = highlighter.highlight(text);
        assert!(!highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Type
                && &text[highlight.start..highlight.end] == "CITEXT"
        }));
        assert!(highlights.iter().any(|highlight| {
            highlight.kind == HighlightKind::Type
                && &text[highlight.start..highlight.end] == "JSONB"
        }));
    }

    #[test]
    fn test_highlight_rope_range_bounds_sql_overlay_context() {
        let mut source = String::new();
        for index in 0..3_000 {
            source.push_str(&format!("SELECT {index} AS value_{index};\n"));
        }
        source.push_str("SELECT payload JSONB FROM events WHERE id = $1;\n");
        let rope = Rope::from_str(&source);
        let viewport_start = source.find("payload JSONB").expect("viewport token");
        let viewport_end = viewport_start + "payload JSONB".len();
        let overlay_range = rope_line_covering_byte_range(&rope, viewport_start..viewport_end);
        let context_start = rope_overlay_context_start(&rope, overlay_range.clone());

        assert!(context_start > 0);
        assert!(context_start <= overlay_range.start);
        assert!(overlay_range.end - context_start <= ROPE_OVERLAY_CONTEXT_BYTES + 512);

        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let highlights = highlighter.highlight_rope_range(&rope, viewport_start..viewport_end);

        assert_highlighted_token(&source, &highlights, "payload", HighlightKind::Identifier);
        assert_highlighted_token(&source, &highlights, "JSONB", HighlightKind::Type);
    }

    #[test]
    fn test_highlight_rope_range_extends_context_for_nearby_dollar_string() {
        let mut source = String::new();
        for index in 0..2_000 {
            source.push_str(&format!("SELECT {index} AS value_{index};\n"));
        }
        source.push_str("DO $body$\nBEGIN\n");
        let dollar_start = source.find("$body$").expect("dollar tag");
        while source.len() - dollar_start < ROPE_OVERLAY_CONTEXT_BYTES + 8 * 1024 {
            source.push_str("  RAISE NOTICE ':ignored';\n");
        }
        source.push_str("  RAISE NOTICE ':viewport';\nEND\n");

        let rope = Rope::from_str(&source);
        let viewport_start = source.find(":viewport").expect("viewport token");
        let viewport_end = viewport_start + ":viewport".len();
        let overlay_range = rope_line_covering_byte_range(&rope, viewport_start..viewport_end);
        let context_start = rope_overlay_context_start(&rope, overlay_range.clone());

        assert_eq!(
            context_start,
            rope.line_to_byte(rope.byte_to_line(dollar_start))
        );

        let mut highlighter = SyntaxHighlighter::new().unwrap();
        highlighter.set_language_profile("postgresql");
        let highlights = highlighter.highlight_rope_range(&rope, viewport_start..viewport_end);

        assert_highlighted_token(&source, &highlights, ":viewport", HighlightKind::String);
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
