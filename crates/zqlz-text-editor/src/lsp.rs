//! LSP integration for the text editor
//!
//! This module provides Language Server Protocol (LSP) integration for code intelligence features
//! like completions, diagnostics, hover info, and go-to-definition.
//!
//! The implementation follows the LSP specification:
//! https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/

use anyhow::Result;
use gpui::{App, Context, Task, Window};
use lsp_types::{CompletionContext, CompletionItem, CompletionItemKind, CompletionResponse, Hover};
use ropey::Rope;
use std::rc::Rc;

use crate::TextEditor;

/// SQL keywords for basic completion
const SQL_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "AND",
    "OR",
    "NOT",
    "IN",
    "LIKE",
    "BETWEEN",
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "CREATE",
    "TABLE",
    "DROP",
    "ALTER",
    "INDEX",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "FULL",
    "CROSS",
    "ON",
    "GROUP",
    "BY",
    "HAVING",
    "ORDER",
    "ASC",
    "DESC",
    "LIMIT",
    "OFFSET",
    "DISTINCT",
    "ALL",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "AS",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "NULL",
    "IS",
    "TRUE",
    "FALSE",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "PRIMARY",
    "KEY",
    "FOREIGN",
    "REFERENCES",
    "CONSTRAINT",
    "UNIQUE",
    "DEFAULT",
    "INTEGER",
    "TEXT",
    "VARCHAR",
    "BOOLEAN",
    "REAL",
    "BLOB",
    "IF",
    "EXISTS",
    "AUTOINCREMENT",
];

/// SQL functions for basic completion
const SQL_FUNCTIONS: &[&str] = &[
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "COALESCE",
    "NULLIF",
    "CAST",
    "UPPER",
    "LOWER",
    "LENGTH",
    "SUBSTR",
    "TRIM",
    "LTRIM",
    "RTRIM",
    "ABS",
    "ROUND",
    "CEIL",
    "FLOOR",
    "MOD",
    "POWER",
    "SQRT",
    "DATE",
    "TIME",
    "DATETIME",
    "STRFTIME",
    "JULIANDAY",
    "IFNULL",
    "IIF",
    "TYPEOF",
    "PRINTF",
    "INSTR",
    "GLOB",
    "HEX",
    "QUOTE",
    "RANDOM",
    "RANDOMBLOB",
    "ZEROBLOB",
    "UNICODE",
    "ROW_NUMBER",
    "RANK",
    "DENSE_RANK",
    "NTILE",
    "ROWID",
    "LAST_INSERT_ROWID",
    "CHANGES",
    "TOTAL",
    "GROUP_CONCAT",
];

/// Get documentation for a SQL keyword
fn get_keyword_documentation(keyword: &str) -> Option<String> {
    let docs = match keyword {
        "SELECT" => "Retrieves data from one or more tables.",
        "FROM" => "Specifies the table(s) to retrieve data from.",
        "WHERE" => "Filters rows based on a condition.",
        "INSERT" => "Inserts new rows into a table.",
        "UPDATE" => "Modifies existing rows in a table.",
        "DELETE" => "Removes rows from a table.",
        "JOIN" | "LEFT" | "RIGHT" | "INNER" | "OUTER" => "Combines rows from two or more tables.",
        "GROUP BY" => "Groups rows that have the same values in specified columns.",
        "ORDER BY" => "Sorts the result set.",
        "HAVING" => "Filters groups based on a condition.",
        "DISTINCT" => "Removes duplicate rows from the result set.",
        "LIMIT" => "Limits the number of rows returned.",
        "OFFSET" => "Skips a specified number of rows.",
        "UNION" => "Combines the result sets of two or more SELECT statements.",
        "CREATE" => "Creates a new database object (table, index, etc.).",
        "ALTER" => "Modifies an existing database object.",
        "DROP" => "Deletes a database object.",
        "NULL" => "Represents a missing or unknown value.",
        "PRIMARY KEY" => "A column or set of columns that uniquely identifies each row.",
        "FOREIGN KEY" => "A column that references the primary key of another table.",
        _ => return None,
    };
    Some(docs.to_string())
}

/// Get documentation for a SQL function
fn get_function_documentation(func: &str) -> Option<String> {
    let docs = match func {
        "COUNT" => "Returns the number of rows that match a condition.",
        "SUM" => "Returns the sum of a numeric column.",
        "AVG" => "Returns the average value of a numeric column.",
        "MIN" => "Returns the minimum value in a column.",
        "MAX" => "Returns the maximum value in a column.",
        "COALESCE" => "Returns the first non-null value in a list.",
        "NULLIF" => "Returns NULL if two expressions are equal.",
        "CAST" => "Converts a value from one data type to another.",
        "UPPER" => "Converts a string to uppercase.",
        "LOWER" => "Converts a string to lowercase.",
        "LENGTH" => "Returns the length of a string.",
        "SUBSTR" => "Returns a substring from a string.",
        "TRIM" => "Removes leading and trailing spaces from a string.",
        "ABS" => "Returns the absolute value of a number.",
        "ROUND" => "Rounds a number to a specified number of decimals.",
        "DATE" => "Returns the current date.",
        "TIME" => "Returns the current time.",
        "DATETIME" => "Returns the current date and time.",
        "IFNULL" => "Returns an alternative value if an expression is NULL.",
        "TYPEOF" => "Returns the data type of an expression.",
        _ => return None,
    };
    Some(docs.to_string())
}

/// Basic SQL completion provider
pub struct SqlCompletionProvider;

impl SqlCompletionProvider {
    pub fn new() -> Self {
        Self
    }

    /// Get completions for the current word
    pub fn get_word_completions(&self, prefix: &str) -> Vec<CompletionItem> {
        let prefix_lower = prefix.to_lowercase();
        let mut items = Vec::new();

        // Add matching keywords
        for keyword in SQL_KEYWORDS {
            if keyword.to_lowercase().starts_with(&prefix_lower) {
                items.push(CompletionItem {
                    label: keyword.to_string(),
                    kind: Some(CompletionItemKind::KEYWORD),
                    detail: Some("SQL Keyword".to_string()),
                    ..Default::default()
                });
            }
        }

        // Add matching functions
        for func in SQL_FUNCTIONS {
            if func.to_lowercase().starts_with(&prefix_lower) {
                items.push(CompletionItem {
                    label: func.to_string(),
                    kind: Some(CompletionItemKind::FUNCTION),
                    detail: Some("SQL Function".to_string()),
                    ..Default::default()
                });
            }
        }

        items
    }

    /// Get hover documentation for a word
    pub fn get_hover_documentation(&self, word: &str) -> Option<String> {
        let word_upper = word.to_uppercase();

        // Check if it's a keyword
        for keyword in SQL_KEYWORDS {
            if *keyword == word_upper {
                return get_keyword_documentation(keyword);
            }
        }

        // Check if it's a function
        for func in SQL_FUNCTIONS {
            if *func == word_upper {
                return get_function_documentation(func);
            }
        }

        None
    }
}

impl Default for SqlCompletionProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for providing code completions
///
/// This trait is implemented by LSP providers to offer context-aware completions
/// based on the current cursor position and document content.
pub trait CompletionProvider: 'static {
    /// Fetch completions for the given position (async)
    ///
    /// # Arguments
    /// * `text` - The current text content as a Rope
    /// * `offset` - The cursor position in bytes
    /// * `trigger` - The completion trigger context (manual, trigger character, etc.)
    /// * `window` - The GPUI window
    /// * `cx` - The application context
    ///
    /// # Returns
    /// An async task that resolves to a list of completion items
    fn completions(
        &self,
        text: &Rope,
        offset: usize,
        trigger: CompletionContext,
        window: &mut Window,
        cx: &mut Context<TextEditor>,
    ) -> Task<Result<CompletionResponse>>;

    /// Get completions for a word prefix (synchronous, basic implementation)
    ///
    /// This is a convenience method for getting completions without async context.
    /// The default implementation returns empty completions; providers should override
    /// this to provide synchronous keyword/function completions.
    ///
    /// For full schema-aware completions, use the async `completions()` method instead.
    fn get_word_completions(&self, _prefix: &str) -> Vec<CompletionItem> {
        // Default implementation returns empty - providers should override
        Vec::new()
    }

    /// Check if completion should be triggered for the given text insertion
    ///
    /// # Arguments
    /// * `offset` - The position where text was inserted
    /// * `new_text` - The text that was just inserted
    /// * `cx` - The editor context
    ///
    /// # Returns
    /// true if completions should be shown, false otherwise
    fn is_completion_trigger(
        &self,
        offset: usize,
        new_text: &str,
        cx: &mut Context<TextEditor>,
    ) -> bool;
}

/// Implementation of CompletionProvider using SqlCompletionProvider
impl CompletionProvider for SqlCompletionProvider {
    fn completions(
        &self,
        text: &Rope,
        offset: usize,
        _trigger: CompletionContext,
        _window: &mut Window,
        _cx: &mut Context<TextEditor>,
    ) -> Task<Result<CompletionResponse>> {
        // Get the word prefix at the current position
        let prefix = get_word_at_offset(text, offset);

        let items = self.get_word_completions(&prefix);

        Task::ready(Ok(CompletionResponse::Array(items)))
    }

    fn get_word_completions(&self, prefix: &str) -> Vec<CompletionItem> {
        // Forward to the SqlCompletionProvider's implementation
        SqlCompletionProvider::get_word_completions(self, prefix)
    }

    fn is_completion_trigger(
        &self,
        _offset: usize,
        new_text: &str,
        _cx: &mut Context<TextEditor>,
    ) -> bool {
        // Trigger on alphanumeric characters (typing)
        if new_text.len() == 1 {
            let Some(ch) = new_text.chars().next() else {
                return false;
            };
            return ch.is_alphanumeric() || ch == '_';
        }

        // Multi-character input (paste or rapid typing)
        new_text.chars().any(|c| c.is_alphanumeric())
    }
}

/// Get the word at the given cursor offset
pub fn get_word_at_cursor(text: &Rope, offset: usize) -> String {
    get_word_at_offset(text, offset)
}

/// Get the word prefix at the given byte offset.
fn get_word_at_offset(text: &Rope, byte_offset: usize) -> String {
    // All ropey char-indexed APIs require a char index, not a byte offset. Clamp
    // to len_chars() so that a cursor sitting exactly at end-of-buffer (where
    // byte_offset == len_bytes()) maps safely to the last char position.
    let char_end = if byte_offset >= text.len_bytes() {
        text.len_chars()
    } else {
        text.byte_to_char(byte_offset)
    };

    // Scan backwards in char space to find the start of the current word.
    let mut char_start = char_end;
    while char_start > 0 {
        let prev = char_start - 1;
        match text.get_char(prev) {
            Some(ch) if ch.is_alphanumeric() || ch == '_' => char_start = prev,
            _ => break,
        }
    }

    // Extract the word slice using char indices (safe, no unwrap).
    text.slice(char_start..char_end).to_string()
}

/// Trait for providing hover information
///
/// This trait is implemented by LSP providers to show documentation, type information,
/// or other contextual information when hovering over text.
pub trait HoverProvider: 'static {
    /// Get hover information for the given position
    ///
    /// # Arguments
    /// * `text` - The current text content as a Rope
    /// * `offset` - The cursor position in bytes
    /// * `window` - The GPUI window
    /// * `cx` - The application context
    ///
    /// # Returns
    /// An async task that resolves to hover information (or None if nothing to show)
    fn hover(
        &self,
        text: &Rope,
        offset: usize,
        window: &mut Window,
        cx: &App,
    ) -> Task<Result<Option<Hover>>>;
}

/// Provider for go-to-definition requests (feat-046).
///
/// Implementors resolve a byte offset in the buffer to a target position within
/// the same buffer. Returning `None` means "no definition found."
pub trait DefinitionProvider: 'static {
    /// Return the byte offset of the definition for the symbol at `offset`, if known.
    fn definition(&self, text: &Rope, offset: usize) -> Option<usize>;
}

/// Provider for find-references requests (feat-047).
///
/// Implementors return all byte ranges where the symbol at `offset` is used.
/// An empty `Vec` means "no references found."
pub trait ReferencesProvider: 'static {
    /// Return byte ranges of all usages of the symbol at `offset`.
    fn references(&self, text: &Rope, offset: usize) -> Vec<std::ops::Range<usize>>;
}

/// Container for all LSP providers
///
/// This struct holds optional references to various LSP providers. Only the providers
/// that are set will be used. This allows for flexible LSP configuration.
pub struct Lsp {
    /// Optional completion provider for code completions
    pub completion_provider: Option<Rc<dyn CompletionProvider>>,

    /// Optional hover provider for hover tooltips
    pub hover_provider: Option<Rc<dyn HoverProvider>>,

    /// Optional definition provider for go-to-definition
    pub definition_provider: Option<Rc<dyn DefinitionProvider>>,

    /// Optional references provider for find-references
    pub references_provider: Option<Rc<dyn ReferencesProvider>>,

    /// Held so it isn't dropped (dropping a Task cancels it).
    pub completion_task: Task<Result<()>>,

    /// Held so it isn't dropped (dropping a Task cancels it).
    pub hover_task: Task<Result<()>>,
}

impl Default for Lsp {
    fn default() -> Self {
        Self {
            completion_provider: None,
            hover_provider: None,
            definition_provider: None,
            references_provider: None,
            completion_task: Task::ready(Ok(())),
            hover_task: Task::ready(Ok(())),
        }
    }
}

impl Lsp {
    /// Create a new empty LSP container with no providers
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset all LSP state
    ///
    /// This clears any ongoing tasks and resets internal state.
    pub fn reset(&mut self) {
        self.completion_task = Task::ready(Ok(()));
        self.hover_task = Task::ready(Ok(()));
    }

    /// Check if completions are available
    pub fn has_completions(&self) -> bool {
        self.completion_provider.is_some()
    }

    /// Check if hover info is available
    pub fn has_hover(&self) -> bool {
        self.hover_provider.is_some()
    }

    /// Check if go-to-definition is available
    pub fn has_definition(&self) -> bool {
        self.definition_provider.is_some()
    }

    /// Check if find-references is available
    pub fn has_references(&self) -> bool {
        self.references_provider.is_some()
    }
}
