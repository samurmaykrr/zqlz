//! LSP integration for the text editor
//!
//! This module provides Language Server Protocol (LSP) integration for code intelligence features
//! like completions, diagnostics, hover info, and go-to-definition.
//!
//! The implementation follows the LSP specification:
//! https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/

use anyhow::Result;
use gpui::{App, Context, Task, Window};
use lsp_types::{
    CodeActionOrCommand, CompletionContext, CompletionItem, CompletionItemKind, CompletionResponse,
    Hover, InsertTextFormat, WorkspaceEdit,
};
use ropey::Rope;
use std::rc::Rc;
use std::sync::Arc;

use crate::{DocumentContext, TextEditor};

#[derive(Clone, Debug)]
pub struct CompletionRequestContext {
    pub revision: usize,
    pub cursor_offset: usize,
    pub trigger_offset: usize,
    pub current_prefix: String,
}

#[derive(Clone, Debug)]
pub enum CompletionResolution {
    CachedFilter {
        items: Vec<CompletionItem>,
        trigger_offset: usize,
    },
    Clear,
    Provider {
        request: RequestToken,
        trigger_offset: usize,
        trigger_prefix: String,
    },
}

#[derive(Clone, Debug)]
pub struct HoverRequestContext {
    pub revision: usize,
    pub cursor_offset: usize,
    pub offset: usize,
    pub word_target: Option<crate::WordTarget>,
}

#[derive(Clone, Debug)]
pub enum HoverResolution {
    Provider { request: RequestToken },
    Fallback(Option<HoverState>),
    Clear,
}

#[derive(Clone, Debug)]
pub struct HoverState {
    pub word: String,
    pub documentation: String,
    pub range: std::ops::Range<usize>,
}

#[derive(Clone, Debug)]
pub struct CompletionMenuData {
    pub items: Vec<CompletionItem>,
    pub selected_index: usize,
    pub scroll_offset: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct CompletionMenuState {
    pub(crate) items: Vec<CompletionItem>,
    pub(crate) trigger_offset: usize,
    pub(crate) selected_index: usize,
    pub(crate) scroll_offset: usize,
    pub(crate) scroll_accumulator: f32,
}

#[derive(Clone, Debug)]
pub(crate) struct CompletionCache {
    pub(crate) all_items: Vec<CompletionItem>,
    pub(crate) trigger_prefix: String,
    pub(crate) trigger_offset: usize,
}

#[derive(Default)]
pub struct LspUiState {
    completion_menu: Option<CompletionMenuState>,
    completion_cache: Option<CompletionCache>,
    hover_state: Option<HoverState>,
}

impl LspUiState {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn completion_menu_state(&self) -> Option<&CompletionMenuState> {
        self.completion_menu.as_ref()
    }

    pub(crate) fn completion_menu_state_mut(&mut self) -> Option<&mut CompletionMenuState> {
        self.completion_menu.as_mut()
    }

    pub(crate) fn completion_cache(&self) -> Option<&CompletionCache> {
        self.completion_cache.as_ref()
    }

    pub(crate) fn set_completion_cache(&mut self, cache: CompletionCache) {
        self.completion_cache = Some(cache);
    }

    pub(crate) fn set_completion_items(
        &mut self,
        items: Vec<CompletionItem>,
        trigger_offset: usize,
    ) {
        if items.is_empty() {
            self.completion_menu = None;
            return;
        }

        self.completion_menu = Some(CompletionMenuState {
            items,
            trigger_offset,
            selected_index: 0,
            scroll_offset: 0,
            scroll_accumulator: 0.0,
        });
    }

    pub(crate) fn take_completion_menu_state(&mut self) -> Option<CompletionMenuState> {
        self.completion_menu.take()
    }

    pub fn clear_completion_menu(&mut self) {
        self.completion_menu = None;
    }

    pub fn clear_completion(&mut self) {
        self.completion_menu = None;
        self.completion_cache = None;
    }

    pub fn has_completion_menu(&self) -> bool {
        self.completion_menu.is_some()
    }

    pub fn completion_menu(&self) -> Option<CompletionMenuData> {
        self.completion_menu
            .as_ref()
            .map(|menu| CompletionMenuData {
                items: menu.items.clone(),
                selected_index: menu.selected_index,
                scroll_offset: menu.scroll_offset,
            })
    }

    pub fn hover_state(&self) -> Option<HoverState> {
        self.hover_state.clone()
    }

    pub fn set_hover_state(&mut self, hover_state: HoverState) {
        self.hover_state = Some(hover_state);
    }

    pub fn clear_hover_state(&mut self) {
        self.hover_state = None;
    }

    pub fn has_hover(&self) -> bool {
        self.hover_state.is_some()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestToken {
    pub revision: usize,
    pub cursor_offset: usize,
    pub generation: u64,
}

#[derive(Default)]
struct RequestTracker {
    generation: u64,
}

impl RequestTracker {
    fn begin(&mut self, revision: usize, cursor_offset: usize) -> RequestToken {
        self.generation = self.generation.saturating_add(1);
        RequestToken {
            revision,
            cursor_offset,
            generation: self.generation,
        }
    }

    fn matches(&self, token: RequestToken, revision: usize, cursor_offset: usize) -> bool {
        revision == token.revision
            && cursor_offset == token.cursor_offset
            && self.generation == token.generation
    }
}

pub struct LspRequestState {
    completion: RequestTracker,
    hover: RequestTracker,
    completion_debounce_task: Task<Result<()>>,
    completion_task: Task<Result<()>>,
    hover_task: Task<Result<()>>,
    completion_pending: bool,
    completion_pending_context: Option<CompletionContext>,
}

impl Default for LspRequestState {
    fn default() -> Self {
        Self {
            completion: RequestTracker::default(),
            hover: RequestTracker::default(),
            completion_debounce_task: Task::ready(Ok(())),
            completion_task: Task::ready(Ok(())),
            hover_task: Task::ready(Ok(())),
            completion_pending: false,
            completion_pending_context: None,
        }
    }
}

impl LspRequestState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin_completion(&mut self, revision: usize, cursor_offset: usize) -> RequestToken {
        self.completion.begin(revision, cursor_offset)
    }

    pub fn begin_hover(&mut self, revision: usize, cursor_offset: usize) -> RequestToken {
        self.hover.begin(revision, cursor_offset)
    }

    pub fn matches_completion(
        &self,
        token: RequestToken,
        revision: usize,
        cursor_offset: usize,
    ) -> bool {
        self.completion.matches(token, revision, cursor_offset)
    }

    pub fn matches_hover(
        &self,
        token: RequestToken,
        revision: usize,
        cursor_offset: usize,
    ) -> bool {
        self.hover.matches(token, revision, cursor_offset)
    }

    pub fn replace_completion_debounce_task(&mut self, task: Task<Result<()>>) {
        self.completion_debounce_task = task;
    }

    pub fn replace_completion_task(&mut self, task: Task<Result<()>>) {
        self.completion_task = task;
    }

    pub fn replace_hover_task(&mut self, task: Task<Result<()>>) {
        self.hover_task = task;
    }

    pub fn queue_completion_refresh(&mut self, trigger: CompletionContext) {
        self.completion_pending = true;
        self.completion_pending_context = Some(trigger);
    }

    pub fn completion_pending(&self) -> bool {
        self.completion_pending
    }

    pub fn take_pending_completion_context(&mut self) -> Option<CompletionContext> {
        self.completion_pending = false;
        self.completion_pending_context.take()
    }

    pub fn clear_pending_completion(&mut self) {
        self.completion_pending = false;
        self.completion_pending_context = None;
    }

    pub fn reset(&mut self) {
        self.completion_debounce_task = Task::ready(Ok(()));
        self.completion_task = Task::ready(Ok(()));
        self.hover_task = Task::ready(Ok(()));
        self.completion_pending = false;
        self.completion_pending_context = None;
    }

    pub(crate) fn resolve_completion_request(
        &mut self,
        completion_cache: Option<&CompletionCache>,
        allow_provider_requests: bool,
        provider_available: bool,
        context: CompletionRequestContext,
    ) -> CompletionResolution {
        if let Some(cache) = completion_cache
            && cache.trigger_offset == context.trigger_offset
            && context
                .current_prefix
                .to_lowercase()
                .starts_with(&cache.trigger_prefix.to_lowercase())
        {
            let prefix_lower = context.current_prefix.to_lowercase();
            let filtered = cache
                .all_items
                .iter()
                .filter(|item| item.label.to_lowercase().contains(&prefix_lower))
                .cloned()
                .collect();
            return CompletionResolution::CachedFilter {
                items: filtered,
                trigger_offset: context.trigger_offset,
            };
        }

        if !provider_available || !allow_provider_requests {
            return CompletionResolution::Clear;
        }

        CompletionResolution::Provider {
            request: self.begin_completion(context.revision, context.cursor_offset),
            trigger_offset: context.trigger_offset,
            trigger_prefix: context.current_prefix,
        }
    }

    pub(crate) fn resolve_hover_request(
        &mut self,
        allow_provider_requests: bool,
        provider_available: bool,
        fallback_hover: Option<HoverState>,
        context: HoverRequestContext,
    ) -> HoverResolution {
        if provider_available && allow_provider_requests {
            return HoverResolution::Provider {
                request: self.begin_hover(context.revision, context.cursor_offset),
            };
        }

        if let Some(hover) = fallback_hover {
            return HoverResolution::Fallback(Some(hover));
        }

        if context.word_target.is_none() {
            HoverResolution::Clear
        } else {
            HoverResolution::Fallback(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CompletionRequestContext, CompletionResolution, HoverRequestContext, HoverResolution,
        HoverState, LspRequestState, LspUiState,
    };
    use lsp_types::CompletionItem;

    #[test]
    fn request_state_rejects_stale_completion_tokens() {
        let mut state = LspRequestState::new();
        let stale = state.begin_completion(1, 4);
        let current = state.begin_completion(2, 5);

        assert!(!state.matches_completion(stale, 2, 5));
        assert!(state.matches_completion(current, 2, 5));
    }

    #[test]
    fn request_state_rejects_stale_hover_tokens_after_cursor_move() {
        let mut state = LspRequestState::new();
        let stale = state.begin_hover(3, 7);
        let current = state.begin_hover(3, 9);

        assert!(!state.matches_hover(stale, 3, 9));
        assert!(state.matches_hover(current, 3, 9));
    }

    #[test]
    fn ui_state_keeps_completion_cache_when_menu_is_filtered() {
        let mut state = LspUiState::new();
        state.set_completion_cache(super::CompletionCache {
            all_items: vec![CompletionItem {
                label: "select".to_string(),
                ..Default::default()
            }],
            trigger_prefix: "se".to_string(),
            trigger_offset: 3,
        });
        state.set_completion_items(
            vec![CompletionItem {
                label: "select".to_string(),
                ..Default::default()
            }],
            3,
        );

        assert!(state.has_completion_menu());
        assert_eq!(
            state
                .completion_cache()
                .expect("completion cache")
                .trigger_offset,
            3
        );

        state.clear_completion_menu();

        assert!(state.completion_cache().is_some());
        assert!(!state.has_completion_menu());
    }

    #[test]
    fn ui_state_tracks_hover_state_separately_from_provider_availability() {
        let mut state = LspUiState::new();
        state.set_hover_state(HoverState {
            word: "select".to_string(),
            documentation: "keyword".to_string(),
            range: 0..6,
        });

        assert!(state.has_hover());
        assert_eq!(state.hover_state().expect("hover state").word, "select");

        state.clear_hover_state();

        assert!(!state.has_hover());
    }

    #[test]
    fn request_state_prefers_cached_completion_filter_when_prefix_extends_trigger() {
        let mut state = LspRequestState::new();
        let cache = super::CompletionCache {
            all_items: vec![CompletionItem {
                label: "select".to_string(),
                ..Default::default()
            }],
            trigger_prefix: "se".to_string(),
            trigger_offset: 3,
        };

        let resolution = state.resolve_completion_request(
            Some(&cache),
            true,
            true,
            CompletionRequestContext {
                revision: 1,
                cursor_offset: 5,
                trigger_offset: 3,
                current_prefix: "sel".to_string(),
            },
        );

        match resolution {
            CompletionResolution::CachedFilter {
                items,
                trigger_offset,
            } => {
                assert_eq!(trigger_offset, 3);
                assert_eq!(items.len(), 1);
            }
            other => panic!("expected cached completion filter, got {other:?}"),
        }
    }

    #[test]
    fn request_state_clears_completions_without_provider_or_cache() {
        let mut state = LspRequestState::new();

        let resolution = state.resolve_completion_request(
            None,
            true,
            false,
            CompletionRequestContext {
                revision: 1,
                cursor_offset: 5,
                trigger_offset: 3,
                current_prefix: "se".to_string(),
            },
        );

        assert!(matches!(resolution, CompletionResolution::Clear));
    }

    #[test]
    fn request_state_returns_provider_hover_when_provider_is_available() {
        let mut state = LspRequestState::new();

        let resolution = state.resolve_hover_request(
            true,
            true,
            None,
            HoverRequestContext {
                revision: 2,
                cursor_offset: 8,
                offset: 8,
                word_target: None,
            },
        );

        match resolution {
            HoverResolution::Provider { request } => {
                assert_eq!(request.revision, 2);
                assert_eq!(request.cursor_offset, 8);
            }
            other => panic!("expected provider hover request, got {other:?}"),
        }
    }

    #[test]
    fn request_state_clears_completions_when_policy_blocks_provider_requests() {
        let mut state = LspRequestState::new();

        let resolution = state.resolve_completion_request(
            None,
            false,
            true,
            CompletionRequestContext {
                revision: 1,
                cursor_offset: 4,
                trigger_offset: 0,
                current_prefix: "sel".to_string(),
            },
        );

        assert!(matches!(resolution, CompletionResolution::Clear));
    }

    #[test]
    fn request_state_uses_fallback_hover_when_policy_blocks_provider_requests() {
        let mut state = LspRequestState::new();

        let resolution = state.resolve_hover_request(
            false,
            true,
            Some(HoverState {
                word: "select".to_string(),
                documentation: "keyword".to_string(),
                range: 0..6,
            }),
            HoverRequestContext {
                revision: 1,
                cursor_offset: 4,
                offset: 4,
                word_target: None,
            },
        );

        assert!(matches!(resolution, HoverResolution::Fallback(Some(_))));
    }
}

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
                    insert_text: Some(format!("{}(${{1:value}})", func)),
                    insert_text_format: Some(InsertTextFormat::SNIPPET),
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

    /// Check if completion should be triggered for the given text insertion
    ///
    /// # Arguments
    /// * `offset` - The position where text was inserted
    /// * `new_text` - The text that was just inserted
    /// * `cx` - The editor context
    ///
    /// # Returns
    /// LSP trigger metadata when completion should be shown.
    fn completion_trigger_context(
        &self,
        offset: usize,
        new_text: &str,
        cx: &mut Context<TextEditor>,
    ) -> Option<CompletionContext>;
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

    fn completion_trigger_context(
        &self,
        _offset: usize,
        new_text: &str,
        _cx: &mut Context<TextEditor>,
    ) -> Option<CompletionContext> {
        if new_text.len() == 1 {
            let ch = new_text.chars().next()?;

            if matches!(ch, '.' | ' ' | '(' | ',') {
                return Some(CompletionContext {
                    trigger_kind: lsp_types::CompletionTriggerKind::TRIGGER_CHARACTER,
                    trigger_character: Some(ch.to_string()),
                });
            }

            if ch.is_alphanumeric() || ch == '_' {
                return Some(CompletionContext {
                    trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
                    trigger_character: None,
                });
            }

            return None;
        }

        new_text
            .chars()
            .any(|c| c.is_alphanumeric())
            .then_some(CompletionContext {
                trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
                trigger_character: None,
            })
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
    fn definition(&self, text: &Rope, offset: usize, document: &DocumentContext) -> Option<usize>;
}

/// Provider for find-references requests (feat-047).
///
/// Implementors return all byte ranges where the symbol at `offset` is used.
/// An empty `Vec` means "no references found."
pub trait ReferencesProvider: 'static {
    /// Return byte ranges of all usages of the symbol at `offset`.
    fn references(
        &self,
        text: &Rope,
        offset: usize,
        document: &DocumentContext,
    ) -> Vec<std::ops::Range<usize>>;
}

/// Provider for symbol rename operations.
pub trait RenameProvider: 'static {
    /// Build the workspace edit required to rename the symbol at `offset`.
    fn rename(
        &self,
        text: &Rope,
        offset: usize,
        new_name: &str,
        document: &DocumentContext,
    ) -> Option<WorkspaceEdit>;
}

/// Provider for code actions at the current cursor position.
pub trait CodeActionProvider: 'static {
    /// Return available code actions for the symbol or diagnostic under `offset`.
    fn code_actions(
        &self,
        text: &Rope,
        offset: usize,
        document: &DocumentContext,
    ) -> Vec<CodeActionOrCommand>;
}

/// Preferred side of the anchor position to render an inlay hint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InlayHintSide {
    Before,
    After,
}

/// Semantic kind for an editor-owned inlay hint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InlayHintKind {
    Type,
    Parameter,
}

/// Normalized inlay hint data used by the editor render pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EditorInlayHint {
    /// Byte offset in the buffer that anchors the hint.
    pub byte_offset: usize,
    /// Plain-text label rendered for the hint.
    pub label: String,
    /// Preferred side of the anchor position.
    pub side: InlayHintSide,
    /// Optional semantic kind used for styling.
    pub kind: Option<InlayHintKind>,
    /// Whether to leave visual padding before the label.
    pub padding_left: bool,
    /// Whether to leave visual padding after the label.
    pub padding_right: bool,
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

    /// Optional rename provider for symbol rename.
    pub rename_provider: Option<Rc<dyn RenameProvider>>,

    /// Optional code action provider.
    pub code_action_provider: Option<Rc<dyn CodeActionProvider>>,

    /// Back-compat SQL LSP handle set through `TextEditor::set_sql_lsp`.
    ///
    /// The editor itself does not depend on a concrete `zqlz-lsp` type; this handle
    /// is retained so legacy integrations can still indicate "connected" status.
    pub legacy_sql_lsp: Option<Arc<dyn std::any::Any + Send + Sync>>,

    pub request_state: LspRequestState,
    pub ui_state: LspUiState,
}

impl Default for Lsp {
    fn default() -> Self {
        Self {
            completion_provider: None,
            hover_provider: None,
            definition_provider: None,
            references_provider: None,
            rename_provider: None,
            code_action_provider: None,
            legacy_sql_lsp: None,
            request_state: LspRequestState::new(),
            ui_state: LspUiState::new(),
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
        self.request_state.reset();
        self.ui_state.clear_completion();
        self.ui_state.clear_hover_state();
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

    /// Check if rename is available.
    pub fn has_rename(&self) -> bool {
        self.rename_provider.is_some()
    }
    pub fn hover_state(&self) -> Option<HoverState> {
        self.ui_state.hover_state()
    }

    pub fn completion_menu(&self) -> Option<CompletionMenuData> {
        self.ui_state.completion_menu()
    }
}
