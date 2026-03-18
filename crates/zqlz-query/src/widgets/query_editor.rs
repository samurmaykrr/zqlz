//! Query editor panel
//!
//! SQL editor with syntax highlighting, IntelliSense, and execution controls.
//! Supports both regular SQL mode and Template mode (MiniJinja).
//!
//! This editor is reusable for different database object types:
//! - Queries: Ad-hoc SQL queries
//! - Views: CREATE/ALTER VIEW definitions
//! - Stored Procedures: CREATE/ALTER PROCEDURE definitions
//! - Functions: CREATE/ALTER FUNCTION definitions
//! - Triggers: CREATE/ALTER TRIGGER definitions

use crate::ai_completion::{AiProviderFactory, CompletionRequest};
use crate::batch::split_statements;
use gpui::prelude::FluentBuilder;
use gpui::*;
use lsp_types::DiagnosticSeverity;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::Connection;
use zqlz_lsp::SqlLsp;
use zqlz_services::SchemaService;
use zqlz_settings::{
    CursorBlink, CursorShape, EditorSettings, InlineSuggestionProvider, ScrollBeyondLastLine,
    SearchWrap, ZqlzSettings,
};
use zqlz_templates::TemplateEngine;
use zqlz_text_editor::{DocumentIdentity, TextDocument, TextEditor, TextEditorEvent};
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, RopeExt, Selectable, Sizable, StyledExt, Theme, ZqlzIcon,
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    kbd::Kbd,
    menu::DropdownMenu,
    scroll::ScrollableElement,
    v_flex,
};

use super::actions::{
    AcceptCompletion, AcceptInlineSuggestion, CancelCompletion, CancelCompletionMenu,
    ConfirmCompletion, DismissInlineSuggestion, FormatQuery, NextCompletion, NextProblem,
    PreviousCompletion, PreviousProblem, SaveQuery, ShowCodeActions, ShowHover, TriggerCompletion,
    TriggerParameterHints,
};
use crate::schema_metadata::{SchemaMetadata, SchemaMetadataProvider, SchemaSymbolInfo};

/// Convert serde_json::Value to minijinja::Value
fn json_to_minijinja_value(value: serde_json::Value) -> minijinja::Value {
    match value {
        serde_json::Value::Null => minijinja::Value::UNDEFINED,
        serde_json::Value::Bool(b) => minijinja::Value::from(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                minijinja::Value::from(i)
            } else if let Some(f) = n.as_f64() {
                minijinja::Value::from(f)
            } else {
                minijinja::Value::from(n.to_string())
            }
        }
        serde_json::Value::String(s) => minijinja::Value::from(s),
        serde_json::Value::Array(arr) => {
            let values: Vec<minijinja::Value> =
                arr.into_iter().map(json_to_minijinja_value).collect();
            minijinja::Value::from(values)
        }
        serde_json::Value::Object(map) => {
            let map: std::collections::BTreeMap<String, minijinja::Value> = map
                .into_iter()
                .map(|(k, v)| (k, json_to_minijinja_value(v)))
                .collect();
            minijinja::Value::from_iter(map)
        }
    }
}

/// Maps a database driver type name to the corresponding syntax highlight language.
fn driver_type_to_highlight_language(driver_type: Option<&str>) -> &'static str {
    // Use DialectProfile to determine the correct language for syntax highlighting
    driver_type
        .map(zqlz_core::dialects::get_highlight_language)
        .unwrap_or("sql")
}

fn driver_type_to_sqlformat_dialect(driver_type: Option<&str>) -> sqlformat::Dialect {
    match driver_type.map(str::to_ascii_lowercase).as_deref() {
        Some("postgres") | Some("postgresql") => sqlformat::Dialect::PostgreSql,
        _ => sqlformat::Dialect::Generic,
    }
}

/// Adapter that implements HoverProvider for SqlLsp
///
/// Wraps `SqlLsp` and exposes it as a `zqlz_text_editor::HoverProvider` so that
/// the TextEditor can request schema-aware hover documentation.
struct SqlLspHoverAdapter {
    sql_lsp: Arc<RwLock<SqlLsp>>,
}

impl SqlLspHoverAdapter {
    fn new(sql_lsp: Arc<RwLock<SqlLsp>>) -> Self {
        Self { sql_lsp }
    }
}

impl zqlz_text_editor::HoverProvider for SqlLspHoverAdapter {
    fn hover(
        &self,
        text: &ropey::Rope,
        offset: usize,
        _window: &mut Window,
        _cx: &App,
    ) -> Task<anyhow::Result<Option<lsp_types::Hover>>> {
        let text_string = text.to_string();
        let ui_rope = zqlz_ui::widgets::Rope::from(text_string.as_str());
        let result = self.sql_lsp.read().get_hover(&ui_rope, offset);
        Task::ready(Ok(result))
    }
}

fn byte_offset_for_lsp_position(
    text: &ropey::Rope,
    position: lsp_types::Position,
) -> Option<usize> {
    let line = usize::try_from(position.line).ok()?;
    if line >= text.len_lines() {
        return None;
    }

    let line_start = text.line_to_char(line);
    let line_end = if line + 1 < text.len_lines() {
        text.line_to_char(line + 1)
    } else {
        text.len_chars()
    };
    let character = usize::try_from(position.character).ok()?;
    let char_index = (line_start + character).min(line_end);
    Some(text.char_to_byte(char_index))
}

fn first_location_from_definition_response(
    response: &lsp_types::GotoDefinitionResponse,
) -> Option<lsp_types::Location> {
    match response {
        lsp_types::GotoDefinitionResponse::Scalar(location) => Some(location.clone()),
        lsp_types::GotoDefinitionResponse::Array(locations) => locations.first().cloned(),
        lsp_types::GotoDefinitionResponse::Link(links) => {
            links.first().map(|link| lsp_types::Location {
                uri: link.target_uri.clone(),
                range: link.target_selection_range,
            })
        }
    }
}

/// Adapter that implements DefinitionProvider for SqlLsp.
struct SqlLspDefinitionAdapter {
    sql_lsp: Arc<RwLock<SqlLsp>>,
}

impl SqlLspDefinitionAdapter {
    fn new(sql_lsp: Arc<RwLock<SqlLsp>>) -> Self {
        Self { sql_lsp }
    }
}

impl zqlz_text_editor::DefinitionProvider for SqlLspDefinitionAdapter {
    fn definition(
        &self,
        text: &ropey::Rope,
        offset: usize,
        _document: &zqlz_text_editor::DocumentContext,
    ) -> Option<usize> {
        let text_string = text.to_string();
        let ui_rope = zqlz_ui::widgets::Rope::from(text_string.as_str());

        if let Some(definition) = self.sql_lsp.read().get_definition(&ui_rope, offset)
            && let Some(location) = first_location_from_definition_response(&definition)
            && let Some(target_offset) = byte_offset_for_lsp_position(text, location.range.start)
        {
            return Some(target_offset);
        }

        self.sql_lsp
            .read()
            .get_references(&ui_rope, offset)
            .into_iter()
            .find_map(|location| byte_offset_for_lsp_position(text, location.range.start))
    }
}

/// Adapter that implements ReferencesProvider for SqlLsp.
struct SqlLspReferencesAdapter {
    sql_lsp: Arc<RwLock<SqlLsp>>,
}

impl SqlLspReferencesAdapter {
    fn new(sql_lsp: Arc<RwLock<SqlLsp>>) -> Self {
        Self { sql_lsp }
    }
}

impl zqlz_text_editor::ReferencesProvider for SqlLspReferencesAdapter {
    fn references(
        &self,
        text: &ropey::Rope,
        offset: usize,
        _document: &zqlz_text_editor::DocumentContext,
    ) -> Vec<std::ops::Range<usize>> {
        let text_string = text.to_string();
        let ui_rope = zqlz_ui::widgets::Rope::from(text_string.as_str());

        self.sql_lsp
            .read()
            .get_references(&ui_rope, offset)
            .into_iter()
            .filter_map(|location| {
                let start = byte_offset_for_lsp_position(text, location.range.start)?;
                let end = byte_offset_for_lsp_position(text, location.range.end)?;
                if end > start { Some(start..end) } else { None }
            })
            .collect()
    }
}

/// Adapter that implements RenameProvider for SqlLsp.
struct SqlLspRenameAdapter {
    sql_lsp: Arc<RwLock<SqlLsp>>,
}

impl SqlLspRenameAdapter {
    fn new(sql_lsp: Arc<RwLock<SqlLsp>>) -> Self {
        Self { sql_lsp }
    }
}

impl zqlz_text_editor::RenameProvider for SqlLspRenameAdapter {
    fn rename(
        &self,
        text: &ropey::Rope,
        offset: usize,
        new_name: &str,
        _document: &zqlz_text_editor::DocumentContext,
    ) -> Option<lsp_types::WorkspaceEdit> {
        let text_string = text.to_string();
        let ui_rope = zqlz_ui::widgets::Rope::from(text_string.as_str());
        self.sql_lsp.read().rename(&ui_rope, offset, new_name)
    }
}

struct SqlLspCodeActionAdapter {
    sql_lsp: Arc<RwLock<SqlLsp>>,
}

impl SqlLspCodeActionAdapter {
    fn new(sql_lsp: Arc<RwLock<SqlLsp>>) -> Self {
        Self { sql_lsp }
    }
}

impl zqlz_text_editor::CodeActionProvider for SqlLspCodeActionAdapter {
    fn code_actions(
        &self,
        text: &ropey::Rope,
        offset: usize,
        _document: &zqlz_text_editor::DocumentContext,
    ) -> Vec<lsp_types::CodeActionOrCommand> {
        let text_string = text.to_string();
        let ui_rope = zqlz_ui::widgets::Rope::from(text_string.as_str());
        let mut lsp = self.sql_lsp.write();
        let diagnostics = lsp.validate_sql(&ui_rope);
        lsp.get_code_actions(&ui_rope, offset, &diagnostics)
            .into_iter()
            .map(lsp_types::CodeActionOrCommand::CodeAction)
            .collect()
    }
}

///
/// This adapter wraps the zqlz-lsp SqlLsp instance and provides completions to the TextEditor.
/// It enables schema-aware completions (table names, column names, etc.) by delegating to SqlLsp.
struct SqlLspCompletionAdapter {
    sql_lsp: Arc<RwLock<SqlLsp>>,
}

impl SqlLspCompletionAdapter {
    fn new(sql_lsp: Arc<RwLock<SqlLsp>>) -> Self {
        Self { sql_lsp }
    }
}

impl zqlz_text_editor::CompletionProvider for SqlLspCompletionAdapter {
    fn completions(
        &self,
        text: &ropey::Rope,
        offset: usize,
        trigger: lsp_types::CompletionContext,
        _window: &mut Window,
        _cx: &mut Context<zqlz_text_editor::TextEditor>,
    ) -> Task<Result<lsp_types::CompletionResponse, anyhow::Error>> {
        // Convert ropey 1.x Rope to zqlz_ui Rope for SqlLsp, then delegate
        let text_string = text.to_string();
        let ui_rope = zqlz_ui::widgets::Rope::from(text_string.as_str());
        let mut lsp = self.sql_lsp.write();
        let items = lsp.get_completions_with_trigger(
            &ui_rope,
            offset,
            trigger.trigger_kind == lsp_types::CompletionTriggerKind::INVOKED,
        );
        Task::ready(Ok(lsp_types::CompletionResponse::Array(items)))
    }

    fn completion_trigger_context(
        &self,
        _offset: usize,
        new_text: &str,
        _cx: &mut Context<zqlz_text_editor::TextEditor>,
    ) -> Option<lsp_types::CompletionContext> {
        if new_text.len() == 1 {
            let character = new_text.chars().next()?;
            if matches!(character, '.' | ' ' | '(' | ',') {
                return Some(lsp_types::CompletionContext {
                    trigger_kind: lsp_types::CompletionTriggerKind::TRIGGER_CHARACTER,
                    trigger_character: Some(character.to_string()),
                });
            }

            if character.is_alphanumeric() || character == '_' {
                return Some(lsp_types::CompletionContext {
                    trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
                    trigger_character: None,
                });
            }

            return None;
        }

        new_text
            .chars()
            .any(|c| c.is_alphanumeric())
            .then_some(lsp_types::CompletionContext {
                trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
                trigger_character: None,
            })
    }
}

/// Editor mode - SQL or Template (MiniJinja)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum EditorMode {
    #[default]
    Sql,
    Template,
}

/// The type of database object being edited
/// This determines the toolbar actions, save behavior, and DDL generation
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum EditorObjectType {
    /// Ad-hoc query (default) - Run, Explain, no Save to database
    #[default]
    Query,
    /// View definition - shows Save button, generates CREATE/ALTER VIEW
    View {
        /// The view name (None for new views)
        name: Option<String>,
        /// The schema/database the view belongs to
        schema: Option<String>,
        /// Whether this is a new view or editing existing
        is_new: bool,
    },
    /// Stored procedure - generates CREATE/ALTER PROCEDURE
    Procedure {
        name: Option<String>,
        schema: Option<String>,
        is_new: bool,
    },
    /// Function - generates CREATE/ALTER FUNCTION
    Function {
        name: Option<String>,
        schema: Option<String>,
        is_new: bool,
    },
    /// Trigger - generates CREATE/ALTER TRIGGER
    Trigger {
        name: Option<String>,
        schema: Option<String>,
        is_new: bool,
    },
}

impl EditorObjectType {
    /// Get a display name for the object type
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Query => "Query",
            Self::View { .. } => "View",
            Self::Procedure { .. } => "Procedure",
            Self::Function { .. } => "Function",
            Self::Trigger { .. } => "Trigger",
        }
    }

    /// Get the object name if available
    pub fn object_name(&self) -> Option<&str> {
        match self {
            Self::Query => None,
            Self::View { name, .. } => name.as_deref(),
            Self::Procedure { name, .. } => name.as_deref(),
            Self::Function { name, .. } => name.as_deref(),
            Self::Trigger { name, .. } => name.as_deref(),
        }
    }

    /// Check if this is a new object (not yet saved to database)
    pub fn is_new(&self) -> bool {
        match self {
            Self::Query => true,
            Self::View { is_new, .. } => *is_new,
            Self::Procedure { is_new, .. } => *is_new,
            Self::Function { is_new, .. } => *is_new,
            Self::Trigger { is_new, .. } => *is_new,
        }
    }

    /// Check if this object type supports saving to the database
    pub fn supports_save(&self) -> bool {
        !matches!(self, Self::Query)
    }

    /// Whether the content is procedural SQL (PL/pgSQL, MySQL stored routines, etc.)
    /// that contains dollar-quoted or delimiter-wrapped blocks
    pub fn is_procedural(&self) -> bool {
        matches!(
            self,
            Self::Function { .. } | Self::Procedure { .. } | Self::Trigger { .. }
        )
    }

    /// Create a new View editor context
    pub fn new_view() -> Self {
        Self::View {
            name: None,
            schema: None,
            is_new: true,
        }
    }

    /// Create an editor context for editing an existing view
    pub fn edit_view(name: String, schema: Option<String>) -> Self {
        Self::View {
            name: Some(name),
            schema,
            is_new: false,
        }
    }

    /// Create a new Trigger editor context
    pub fn new_trigger() -> Self {
        Self::Trigger {
            name: None,
            schema: None,
            is_new: true,
        }
    }

    /// Create an editor context for editing an existing trigger
    pub fn edit_trigger(name: String, schema: Option<String>) -> Self {
        Self::Trigger {
            name: Some(name),
            schema,
            is_new: false,
        }
    }
}

/// Information about a diagnostic for external display (e.g., Problems panel)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiagnosticInfo {
    /// Start line (0-indexed)
    pub line: usize,
    /// Start column (0-indexed)
    pub column: usize,
    /// End line (0-indexed)
    pub end_line: usize,
    /// End column (0-indexed)
    pub end_column: usize,
    /// Severity level
    pub severity: DiagnosticInfoSeverity,
    /// Error message
    pub message: String,
    /// Source of the diagnostic (e.g., "sqlparser", "tree-sitter")
    pub source: Option<String>,
}

/// Diagnostic severity for external use
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiagnosticInfoSeverity {
    Error,
    Warning,
    Info,
    Hint,
}

impl From<zqlz_ui::widgets::highlighter::DiagnosticSeverity> for DiagnosticInfoSeverity {
    fn from(severity: zqlz_ui::widgets::highlighter::DiagnosticSeverity) -> Self {
        match severity {
            zqlz_ui::widgets::highlighter::DiagnosticSeverity::Error => Self::Error,
            zqlz_ui::widgets::highlighter::DiagnosticSeverity::Warning => Self::Warning,
            zqlz_ui::widgets::highlighter::DiagnosticSeverity::Info => Self::Info,
            zqlz_ui::widgets::highlighter::DiagnosticSeverity::Hint => Self::Hint,
        }
    }
}

/// Events emitted by the query editor
#[derive(Clone, Debug)]
pub enum QueryEditorEvent {
    /// User requested to execute the current query
    ExecuteQuery {
        sql: String,
        connection_id: Option<Uuid>,
    },
    /// User requested to execute selected text or current statement
    ExecuteSelection {
        sql: String,
        connection_id: Option<Uuid>,
    },
    /// User requested to explain the current query
    ExplainQuery {
        sql: String,
        connection_id: Option<Uuid>,
    },
    /// User requested to explain selected text or current statement
    ExplainSelection {
        sql: String,
        connection_id: Option<Uuid>,
    },
    /// User requested to cancel the currently executing query
    CancelQuery,
    /// User requested to save a database object (view, procedure, function, trigger)
    SaveObject {
        connection_id: Uuid,
        object_type: EditorObjectType,
        /// The SQL definition (e.g., SELECT statement for views)
        definition: String,
    },
    /// User requested to preview the DDL that would be generated
    PreviewDdl {
        object_type: EditorObjectType,
        definition: String,
    },
    /// User requested to save the query (Cmd+S / Ctrl+S)
    SaveQuery {
        /// The query ID if this is an existing saved query
        saved_query_id: Option<Uuid>,
        /// The connection ID this query is associated with
        connection_id: Option<Uuid>,
        /// The current SQL content
        sql: String,
    },
    /// Diagnostics have changed (for updating Problems panel)
    DiagnosticsChanged {
        /// List of current diagnostics
        diagnostics: Vec<DiagnosticInfo>,
    },
    /// Canonical document metadata changed and workspace state should refresh.
    DocumentStateChanged,
    /// User requested to switch to a different connection
    SwitchConnection {
        /// The new connection ID to switch to
        connection_id: Uuid,
    },
    /// User requested to switch to a different database
    SwitchDatabase {
        /// The database name to switch to
        database_name: String,
    },
}

/// Query editor panel for writing and executing SQL
pub struct QueryEditor {
    /// Panel name/title
    name: String,

    /// Optional connection this editor is bound to
    connection_id: Option<Uuid>,

    /// Name of the connected database/connection (for display)
    connection_name: Option<String>,

    /// If this editor is for a saved query, the query ID
    saved_query_id: Option<Uuid>,

    /// The text editor for SQL code editing
    editor: Entity<TextEditor>,

    /// SQL LSP instance for IntelliSense
    sql_lsp: Arc<RwLock<SqlLsp>>,

    /// Last known driver type from the active connection.
    driver_type: Option<String>,

    /// Whether a query is currently executing
    is_executing: bool,

    /// Current editor mode (SQL or Template)
    editor_mode: EditorMode,

    /// The type of database object being edited (Query, View, Procedure, etc.)
    object_type: EditorObjectType,

    /// Template engine for MiniJinja rendering
    template_engine: TemplateEngine,

    /// Own focus handle so track_focus on the outer wrapper doesn't steal the
    /// inner TextEditor's handle. Focus is forwarded to the editor on click.
    focus_handle: FocusHandle,

    /// Template parameters as JSON editor for MiniJinja variable values
    template_params: Entity<TextEditor>,

    /// Last rendered SQL from template (for preview)
    rendered_sql: Option<String>,

    /// Template rendering error message
    template_error: Option<String>,

    /// Currently selected database name (for display in dropdown)
    current_database: Option<String>,

    /// Available connections (for connection switcher dropdown)
    available_connections: Vec<(Uuid, String)>,

    /// Available databases (for database switcher dropdown)
    available_databases: Vec<String>,

    /// Debounce task for auto-triggering completions
    _completion_debounce: Option<gpui::Task<()>>,

    /// Current hover popover content (shown when ShowHover action is triggered)
    hover_content: Option<String>,

    /// Current inline suggestion (ghost text shown while typing)
    inline_suggestion: Option<InlineSuggestionState>,

    /// Debounce timer for inline suggestions
    _inline_suggestion_debounce: Option<gpui::Task<()>>,

    /// Debounce timer for diagnostics (triggers after typing stops)
    _diagnostics_debounce: Option<gpui::Task<()>>,

    /// Last text content (used to detect changes for diagnostics)
    _last_diagnostics_text: Option<String>,

    /// Cached diagnostics from the last explicit validation pass.
    cached_diagnostics: Vec<lsp_types::Diagnostic>,

    /// Monotonic generation used to ignore stale async inline suggestions.
    inline_suggestion_generation: u64,

    /// Schema metadata provider for hover overlay (created on demand)
    schema_metadata: Option<SchemaMetadata>,

    /// Current schema symbol info (for metadata overlay)
    schema_symbol_info: Option<SchemaSymbolInfo>,

    /// Subscriptions to keep alive
    _subscriptions: Vec<Subscription>,
}

/// Represents the current inline suggestion state
#[derive(Debug, Clone)]
pub struct InlineSuggestionState {
    /// The suggested text to insert
    pub suggestion: String,
    /// Start position of the suggestion (byte offset)
    pub start_offset: usize,
    /// End position of the suggestion (byte offset)
    pub end_offset: usize,
    /// Source of the suggestion (LSP or AI)
    pub source: InlineSuggestionSource,
}

/// Source of an inline suggestion
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InlineSuggestionSource {
    /// Suggestion from LSP completion
    Lsp,
    /// Suggestion from AI provider
    Ai,
}

impl QueryEditor {
    fn diagnostic_infos_from_lsp_diagnostics(
        diagnostics: &[lsp_types::Diagnostic],
    ) -> Vec<DiagnosticInfo> {
        diagnostics
            .iter()
            .map(|diagnostic| DiagnosticInfo {
                line: diagnostic.range.start.line as usize,
                column: diagnostic.range.start.character as usize,
                end_line: diagnostic.range.end.line as usize,
                end_column: diagnostic.range.end.character as usize,
                severity: match diagnostic.severity {
                    Some(DiagnosticSeverity::ERROR) => DiagnosticInfoSeverity::Error,
                    Some(DiagnosticSeverity::WARNING) => DiagnosticInfoSeverity::Warning,
                    Some(DiagnosticSeverity::INFORMATION) => DiagnosticInfoSeverity::Info,
                    Some(DiagnosticSeverity::HINT) => DiagnosticInfoSeverity::Hint,
                    _ => DiagnosticInfoSeverity::Error,
                },
                message: diagnostic.message.clone(),
                source: diagnostic.source.clone(),
            })
            .collect()
    }

    fn ui_diagnostics_from_lsp_diagnostics(
        diagnostics: &[lsp_types::Diagnostic],
    ) -> Vec<zqlz_ui::widgets::highlighter::Diagnostic> {
        diagnostics.iter().cloned().map(Into::into).collect()
    }

    fn diagnostic_counts_from_lsp_diagnostics(
        diagnostics: &[lsp_types::Diagnostic],
    ) -> (usize, usize, usize) {
        let mut errors = 0;
        let mut warnings = 0;
        let mut hints_infos = 0;

        for diagnostic in diagnostics {
            match diagnostic.severity {
                Some(DiagnosticSeverity::ERROR) => errors += 1,
                Some(DiagnosticSeverity::WARNING) => warnings += 1,
                Some(DiagnosticSeverity::INFORMATION) | Some(DiagnosticSeverity::HINT) => {
                    hints_infos += 1
                }
                _ => errors += 1,
            }
        }

        (errors, warnings, hints_infos)
    }

    fn internal_text_document(text: impl AsRef<str>) -> TextDocument {
        TextDocument::with_text(
            DocumentIdentity::internal_with_label("query-editor").expect("internal document uri"),
            text.as_ref(),
        )
    }

    fn build_primary_editor(
        document: TextDocument,
        editor_settings: &EditorSettings,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<TextEditor> {
        let editor = cx.new(|cx| TextEditor::with_document(document, window, cx));

        editor.update(cx, |text_editor, cx| {
            Self::apply_editor_settings(text_editor, editor_settings, cx);
            text_editor.set_autofocus_on_open(true);
        });

        editor
    }

    fn apply_editor_settings(
        text_editor: &mut TextEditor,
        settings: &EditorSettings,
        cx: &mut Context<TextEditor>,
    ) {
        text_editor.set_indent_settings(settings.tab_size as usize, settings.insert_spaces, cx);
        text_editor.set_show_line_numbers(settings.show_line_numbers, cx);
        text_editor.set_soft_wrap_enabled(settings.word_wrap, cx);
        text_editor.set_highlight_current_line(settings.highlight_current_line, cx);
        text_editor.set_show_inline_diagnostics(settings.show_inline_diagnostics, cx);
        text_editor.set_show_folding(settings.show_folding, cx);
        text_editor.set_highlight_enabled(settings.highlight_enabled, cx);
        text_editor.set_bracket_matching_enabled(settings.bracket_matching, cx);
        text_editor.set_relative_line_numbers(settings.relative_line_numbers, cx);
        text_editor.set_show_gutter_diagnostics(settings.show_gutter_diagnostics, cx);
        text_editor.set_cursor_shape(
            match settings.cursor_shape {
                CursorShape::Block => zqlz_text_editor::CursorShapeStyle::Block,
                CursorShape::Line => zqlz_text_editor::CursorShapeStyle::Line,
                CursorShape::Underline => zqlz_text_editor::CursorShapeStyle::Underline,
            },
            cx,
        );
        text_editor
            .set_cursor_blink_enabled(!matches!(settings.cursor_blink, CursorBlink::Off), cx);
        text_editor.set_selection_highlight_enabled(settings.selection_highlight, cx);
        text_editor.set_rounded_selection(settings.rounded_selection, cx);
        text_editor.set_search_wrap_enabled(!matches!(
            settings.search_wrap,
            SearchWrap::Disabled | SearchWrap::NoWrap
        ));
        text_editor.set_smartcase_search_enabled(settings.use_smartcase_search);
        text_editor.set_autoscroll_on_clicks(settings.autoscroll_on_clicks);
        text_editor.set_vertical_scroll_margin(settings.vertical_scroll_margin as usize);
        text_editor.set_horizontal_scroll_margin(settings.horizontal_scroll_margin as usize);
        text_editor.set_scroll_sensitivity(settings.scroll_sensitivity);
        text_editor.set_scroll_beyond_last_line(!matches!(
            settings.scroll_beyond_last_line,
            ScrollBeyondLastLine::Disabled
        ));
        text_editor.set_auto_indent_enabled(settings.auto_indent, cx);
        text_editor.set_large_file_thresholds(
            settings.large_file_line_threshold as usize,
            settings.large_file_byte_threshold as usize,
        );
    }

    fn sync_lsp_settings(&mut self, cx: &mut Context<Self>) {
        let settings = ZqlzSettings::global(cx).editor.clone();

        self.editor.update(cx, |text_editor, cx| {
            Self::apply_editor_settings(text_editor, &settings, cx);

            if settings.lsp_enabled && settings.lsp_completions_enabled {
                text_editor.set_completion_provider(std::rc::Rc::new(
                    SqlLspCompletionAdapter::new(self.sql_lsp.clone()),
                ));
            } else {
                text_editor.clear_completion_provider();
            }

            if settings.lsp_enabled && settings.lsp_hover_enabled {
                text_editor.set_hover_provider(std::rc::Rc::new(SqlLspHoverAdapter::new(
                    self.sql_lsp.clone(),
                )));
            } else {
                text_editor.clear_hover_provider();
            }

            text_editor.set_definition_provider(std::rc::Rc::new(SqlLspDefinitionAdapter::new(
                self.sql_lsp.clone(),
            )));
            text_editor.set_references_provider(std::rc::Rc::new(SqlLspReferencesAdapter::new(
                self.sql_lsp.clone(),
            )));

            if settings.lsp_enabled && settings.lsp_rename_enabled {
                text_editor.set_rename_provider(std::rc::Rc::new(SqlLspRenameAdapter::new(
                    self.sql_lsp.clone(),
                )));
            } else {
                text_editor.clear_rename_provider();
            }

            if settings.lsp_enabled && settings.lsp_code_actions_enabled {
                text_editor.set_code_action_provider(std::rc::Rc::new(
                    SqlLspCodeActionAdapter::new(self.sql_lsp.clone()),
                ));
            } else {
                text_editor.clear_code_action_provider();
            }
        });

        self.template_params.update(cx, |text_editor, cx| {
            Self::apply_editor_settings(text_editor, &settings, cx);
        });

        if !settings.lsp_enabled || !settings.lsp_diagnostics_enabled {
            self.editor.update(cx, |editor, cx| {
                editor.set_diagnostics(Vec::new(), cx);
            });
        }
    }

    fn build_settings_subscription(
        editor: &Entity<TextEditor>,
        template_params: &Entity<TextEditor>,
        cx: &mut Context<Self>,
    ) -> Subscription {
        let _editor = editor;
        let _template_params = template_params;

        cx.observe_global::<ZqlzSettings>(move |this, cx| {
            this.sync_lsp_settings(cx);
            cx.notify();
        })
    }

    fn build_editor_subscriptions(
        editor: &Entity<TextEditor>,
        template_params: &Entity<TextEditor>,
        cx: &mut Context<Self>,
    ) -> Vec<Subscription> {
        vec![
            cx.subscribe(editor, |this, _, event: &TextEditorEvent, cx| {
                if matches!(event, TextEditorEvent::ContentChanged) {
                    this.handle_primary_editor_changed(cx);
                }
            }),
            cx.subscribe(template_params, |this, _, event: &TextEditorEvent, cx| {
                if matches!(event, TextEditorEvent::ContentChanged)
                    && this.editor_mode == EditorMode::Template
                {
                    this.update_template_preview(cx);
                }
            }),
        ]
    }

    pub fn new(
        name: String,
        connection_id: Option<Uuid>,
        schema_service: Arc<SchemaService>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        tracing::debug!(name = %name, connection_id = ?connection_id, "QueryEditor initialization");

        // Initialize SQL LSP with SchemaService
        let sql_lsp = Arc::new(RwLock::new(SqlLsp::new(schema_service)));

        let editor_settings = ZqlzSettings::global(cx).editor.clone();
        tracing::debug!("Creating TextEditor for SQL code editing");
        let editor = Self::build_primary_editor(
            Self::internal_text_document(""),
            &editor_settings,
            window,
            cx,
        );

        let initial_text = editor.read(cx).get_text(cx);

        // Create template parameters JSON editor (multi-line, plain TextEditor)
        let template_params = cx.new(|cx| {
            let mut editor = TextEditor::new(window, cx);
            Self::apply_editor_settings(&mut editor, &editor_settings, cx);
            editor.set_text("{\n  \n}".to_string(), window, cx);
            editor
        });

        let mut _subscriptions = Self::build_editor_subscriptions(&editor, &template_params, cx);
        _subscriptions.push(Self::build_settings_subscription(
            &editor,
            &template_params,
            cx,
        ));

        let mut query_editor = Self {
            name,
            connection_id,
            connection_name: None,
            saved_query_id: None,
            editor,
            sql_lsp,
            driver_type: None,
            is_executing: false,
            editor_mode: EditorMode::Sql,
            object_type: EditorObjectType::Query,
            template_engine: TemplateEngine::new(),
            template_params,
            focus_handle: cx.focus_handle(),
            rendered_sql: None,
            template_error: None,
            current_database: None,
            available_connections: Vec::new(),
            available_databases: Vec::new(),
            hover_content: None,
            inline_suggestion: None,
            _inline_suggestion_debounce: None,
            _completion_debounce: None,
            _diagnostics_debounce: None,
            _last_diagnostics_text: Some(initial_text.to_string()),
            cached_diagnostics: Vec::new(),
            inline_suggestion_generation: 0,
            schema_metadata: None,
            schema_symbol_info: None,
            _subscriptions,
        };

        query_editor.sync_lsp_settings(cx);
        query_editor
    }

    /// Create a new editor for a database object (view, procedure, function, trigger)
    /// This is used when designing/editing database objects that can be saved
    pub fn new_for_object(
        name: String,
        connection_id: Uuid,
        object_type: EditorObjectType,
        initial_content: Option<String>,
        schema_service: Arc<SchemaService>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let document = Self::internal_text_document(initial_content.as_deref().unwrap_or(""));
        Self::new_for_object_with_document(
            name,
            connection_id,
            object_type,
            document,
            schema_service,
            window,
            cx,
        )
    }

    pub fn new_with_document(
        name: String,
        connection_id: Option<Uuid>,
        document: TextDocument,
        schema_service: Arc<SchemaService>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        tracing::debug!(name = %name, connection_id = ?connection_id, "QueryEditor initialization");

        let sql_lsp = Arc::new(RwLock::new(SqlLsp::new(schema_service)));
        let editor_settings = ZqlzSettings::global(cx).editor.clone();
        let editor = Self::build_primary_editor(document, &editor_settings, window, cx);
        let initial_text = editor.read(cx).get_text(cx);

        let template_params = cx.new(|cx| {
            let mut editor = TextEditor::new(window, cx);
            Self::apply_editor_settings(&mut editor, &editor_settings, cx);
            editor.set_text("{\n  \n}".to_string(), window, cx);
            editor
        });

        let mut _subscriptions = Self::build_editor_subscriptions(&editor, &template_params, cx);
        _subscriptions.push(Self::build_settings_subscription(
            &editor,
            &template_params,
            cx,
        ));

        let mut query_editor = Self {
            name,
            connection_id,
            connection_name: None,
            saved_query_id: None,
            editor,
            sql_lsp,
            driver_type: None,
            is_executing: false,
            editor_mode: EditorMode::Sql,
            object_type: EditorObjectType::Query,
            template_engine: TemplateEngine::new(),
            template_params,
            focus_handle: cx.focus_handle(),
            rendered_sql: None,
            template_error: None,
            current_database: None,
            available_connections: Vec::new(),
            available_databases: Vec::new(),
            hover_content: None,
            inline_suggestion: None,
            _inline_suggestion_debounce: None,
            _completion_debounce: None,
            _diagnostics_debounce: None,
            _last_diagnostics_text: Some(initial_text.to_string()),
            cached_diagnostics: Vec::new(),
            inline_suggestion_generation: 0,
            schema_metadata: None,
            schema_symbol_info: None,
            _subscriptions,
        };

        query_editor.sync_lsp_settings(cx);
        query_editor
    }

    pub fn new_for_object_with_document(
        name: String,
        connection_id: Uuid,
        object_type: EditorObjectType,
        document: TextDocument,
        schema_service: Arc<SchemaService>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let _placeholder = match &object_type {
            EditorObjectType::View { .. } => {
                "-- Enter the SELECT statement for the view\nSELECT column1, column2\nFROM table_name\nWHERE condition"
            }
            EditorObjectType::Procedure { .. } => "-- Enter the procedure body",
            EditorObjectType::Function { .. } => "-- Enter the function body",
            EditorObjectType::Trigger { .. } => "-- Enter the trigger body",
            EditorObjectType::Query => "Write your SQL query here...",
        };

        tracing::debug!(name = %name, connection_id = ?connection_id, object_type = ?object_type, "QueryEditor initialization for object");

        let sql_lsp = Arc::new(RwLock::new(SqlLsp::new(schema_service)));
        let editor_settings = ZqlzSettings::global(cx).editor.clone();
        let editor = Self::build_primary_editor(document, &editor_settings, window, cx);
        let initial_text = editor.read(cx).get_text(cx);

        let template_params = cx.new(|cx| {
            let mut editor = TextEditor::new(window, cx);
            Self::apply_editor_settings(&mut editor, &editor_settings, cx);
            editor.set_text("{\n  \n}".to_string(), window, cx);
            editor
        });

        let mut _subscriptions = Self::build_editor_subscriptions(&editor, &template_params, cx);
        _subscriptions.push(Self::build_settings_subscription(
            &editor,
            &template_params,
            cx,
        ));

        let mut query_editor = Self {
            name,
            connection_id: Some(connection_id),
            connection_name: None,
            saved_query_id: None,
            editor,
            sql_lsp,
            driver_type: None,
            is_executing: false,
            editor_mode: EditorMode::Sql,
            object_type,
            template_engine: TemplateEngine::new(),
            template_params,
            focus_handle: cx.focus_handle(),
            rendered_sql: None,
            template_error: None,
            current_database: None,
            available_connections: Vec::new(),
            available_databases: Vec::new(),
            hover_content: None,
            inline_suggestion: None,
            _inline_suggestion_debounce: None,
            _completion_debounce: None,
            _diagnostics_debounce: None,
            _last_diagnostics_text: Some(initial_text.to_string()),
            cached_diagnostics: Vec::new(),
            inline_suggestion_generation: 0,
            schema_metadata: None,
            schema_symbol_info: None,
            _subscriptions,
        };

        query_editor.sync_lsp_settings(cx);
        query_editor
    }

    /// Set the connection for this editor and refresh schema
    pub fn set_connection(
        &mut self,
        connection_id: Option<Uuid>,
        connection_name: Option<String>,
        connection: Option<Arc<dyn Connection>>,
        driver_type: Option<String>,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!(connection_id = ?connection_id, connection_name = ?connection_name, driver_type = ?driver_type, "Setting connection for editor");
        self.connection_id = connection_id;
        self.connection_name = connection_name;
        self.driver_type = driver_type.clone();
        self.schema_metadata = None;
        self.schema_symbol_info = None;
        self.hover_content = None;
        self.editor.update(cx, |editor, cx| {
            editor.clear_code_actions(cx);
        });

        // Dialect determines syntax highlighting language; currently unused since
        // the TextEditor's tree-sitter highlighter handles generic SQL.
        let _dialect_language = driver_type_to_highlight_language(driver_type.as_deref());

        // Update SQL LSP with new connection and driver type
        {
            let mut lsp = self.sql_lsp.write();
            lsp.set_connection(connection_id, connection.clone(), driver_type.clone());
            lsp.set_active_database(self.current_database.clone());

            // If a cache was persisted from the last session, apply it immediately so
            // completions work from the first keystroke.  The background refresh below
            // always runs regardless (stale-while-revalidate).
            if let Some(conn_id) = connection_id
                && let Some(cached) =
                    load_schema_cache_from_disk(conn_id, self.current_database.as_deref())
            {
                lsp.apply_schema_cache(cached);
            }
        }

        // Refresh schema in background, then re-validate diagnostics once loaded.
        //
        // The write lock must NEVER be held across any await point: doing so
        // blocks the GPUI foreground thread for the entire duration of remote I/O,
        // freezing input processing and rendering. Instead we:
        //   1. Extract the needed handles with a brief read lock.
        //   2. Run all database I/O on a background thread (no lock held).
        //   3. Apply the completed cache with a brief write lock.
        if let Some(_conn) = connection {
            tracing::debug!("Starting schema refresh in background");
            let lsp = self.sql_lsp.clone();

            let (connection_for_refresh, connection_id_for_refresh, schema_service) = {
                let guard = lsp.read();
                (
                    guard.connection(),
                    guard.connection_id(),
                    guard.schema_service(),
                )
            };

            let active_database_for_refresh = lsp.read().active_database();

            if let (Some(connection_for_refresh), Some(connection_id_for_refresh)) =
                (connection_for_refresh, connection_id_for_refresh)
            {
                let epoch = lsp.write().next_fetch_epoch();
                cx.spawn(async move |this, cx| {
                    // Attempt the schema fetch up to 3 times with an exponential back-off.
                    const MAX_ATTEMPTS: u32 = 3;
                    let mut attempt = 0u32;

                    let result = loop {
                        let active_database = active_database_for_refresh.clone();
                        let fetch_result = cx
                            .background_spawn({
                                let schema_service: Arc<SchemaService> = schema_service.clone();
                                let conn: Arc<dyn Connection> = connection_for_refresh.clone();
                                async move {
                                    SqlLsp::fetch_schema_cache(
                                        conn,
                                        connection_id_for_refresh,
                                        active_database,
                                        &schema_service,
                                    )
                                    .await
                                }
                            })
                            .await;

                        match fetch_result {
                            Ok(cache) => break Ok(cache),
                            Err(e) => {
                                attempt += 1;
                                if attempt >= MAX_ATTEMPTS {
                                    break Err(e.to_string());
                                }
                                let delay = std::time::Duration::from_secs(2u64.pow(attempt - 1));
                                tracing::warn!(
                                    attempt,
                                    delay_secs = delay.as_secs(),
                                    error = %e,
                                    "Schema fetch failed, retrying"
                                );
                                cx.background_spawn(async move {
                                    smol::Timer::after(delay).await;
                                })
                                .await;
                            }
                        }
                    };

                    match result {
                        Ok(cache) => {
                            save_schema_cache_to_disk(
                                connection_id_for_refresh,
                                active_database_for_refresh.as_deref(),
                                &cache,
                            );
                            lsp.write().apply_schema_cache_if_current(cache, epoch);
                            tracing::debug!("SQL schema refreshed successfully");
                _ = this.update(cx, |editor, cx| {
                    editor.update_diagnostics(cx);
                });
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Failed to refresh SQL schema after retries");
                            lsp.write().schema_loading = false;
                        }
                    }
                })
                .detach();
            }
        } else {
            tracing::debug!("No connection provided, skipping schema refresh");
        }

        cx.notify();
    }

    /// Seeds the LSP schema cache with bare table names as soon as the sidebar's
    /// `load_tables_only` call completes — well before the per-table column-detail
    /// fetches finish.  This makes FROM-clause completions available immediately,
    /// without waiting for the full [`SqlLsp::fetch_schema_cache`] round-trip.
    ///
    /// No-op if the full cache has already been applied (`schema_loading == false`).
    pub fn notify_tables_available(&mut self, table_names: Vec<String>, _cx: &mut Context<Self>) {
        let mut lsp = self.sql_lsp.write();
        if lsp.schema_loading {
            lsp.pre_populate_tables(&table_names);
        }
    }

    /// Called after a query successfully executes so the LSP can react to schema changes.
    ///
    /// DDL statements (CREATE, ALTER, DROP, etc.) may invalidate the cached schema, so this
    /// deletes the disk cache and kicks off a silent background re-fetch to pick up the new
    /// structure for future completions.
    pub fn notify_query_executed(&mut self, sql: &str, cx: &mut Context<Self>) {
        if !crate::QueryEngine::new().is_schema_modifying(sql) {
            return;
        }
        self.schema_metadata = None;
        self.schema_symbol_info = None;
        if let Some(conn_id) = self.connection_id
            && let Some(path) = schema_cache_path(conn_id, self.current_database.as_deref())
        {
            std::fs::remove_file(path).ok();
        }
        // Mark schema as loading so completions don't surface stale objects
        // (e.g. a just-dropped table) during the background re-fetch window.
        self.sql_lsp.write().schema_loading = true;
        self.trigger_lsp_schema_refresh(cx);
    }

    /// Re-fetches the full schema cache in the background and applies the result without
    /// interrupting existing completions (does NOT set `schema_loading = true`).
    ///
    /// Called after `prefetch_all_table_details` completes so that column data already
    /// warmed into `SchemaService`'s per-table cache is picked up by the LSP immediately.
    pub fn trigger_lsp_schema_refresh(&mut self, cx: &mut Context<Self>) {
        let (connection, connection_id, schema_service, active_database) = {
            let guard = self.sql_lsp.read();
            (
                guard.connection(),
                guard.connection_id(),
                guard.schema_service(),
                guard.active_database(),
            )
        };

        let (Some(connection), Some(connection_id)) = (connection, connection_id) else {
            return;
        };

        let lsp = self.sql_lsp.clone();
        let epoch = lsp.write().next_fetch_epoch();
        let active_database_for_disk = active_database.clone();
        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_spawn({
                    let schema_service = schema_service.clone();
                    let connection = connection.clone();
                    async move {
                        SqlLsp::fetch_schema_cache(
                            connection,
                            connection_id,
                            active_database.clone(),
                            &schema_service,
                        )
                        .await
                    }
                })
                .await;

            match result {
                Ok(cache) => {
                    save_schema_cache_to_disk(
                        connection_id,
                        active_database_for_disk.as_deref(),
                        &cache,
                    );
                    lsp.write().apply_schema_cache_if_current(cache, epoch);
                    tracing::debug!("Schema cache refreshed after prefetch completion");
                    _ = _this.update(cx, |this, cx| {
                        this.update_diagnostics(cx);
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Schema refresh after prefetch failed");
                }
            }
        })
        .detach();
    }

    /// Set the list of available connections for the connection switcher
    pub fn set_available_connections(
        &mut self,
        connections: Vec<(Uuid, String)>,
        cx: &mut Context<Self>,
    ) {
        self.available_connections = connections;
        cx.notify();
    }

    /// Set the list of available databases for the database switcher
    pub fn set_available_databases(&mut self, databases: Vec<String>, cx: &mut Context<Self>) {
        self.available_databases = databases;
        cx.notify();
    }

    /// Set the current database name
    pub fn set_current_database(&mut self, database: Option<String>, cx: &mut Context<Self>) {
        self.current_database = database;
        self.sql_lsp
            .write()
            .set_active_database(self.current_database.clone());
        cx.notify();
    }

    /// Set the SQL content
    pub fn set_content(&mut self, content: String, window: &mut Window, cx: &mut Context<Self>) {
        self.dismiss_inline_suggestion(cx);
        self.editor.update(cx, |editor, cx| {
            editor.set_text(content.clone(), window, cx);
        });
        self._last_diagnostics_text = Some(content);
        cx.notify();
    }

    fn handle_primary_editor_changed(&mut self, cx: &mut Context<Self>) {
        let current_text = self.content(cx).to_string();
        self._last_diagnostics_text = Some(current_text);
        cx.emit(QueryEditorEvent::DocumentStateChanged);

        if self.editor_mode == EditorMode::Template {
            self.update_template_preview(cx);
        }

        self.update_diagnostics(cx);
        cx.notify();
    }

    /// Trigger diagnostics update with debounce (for automatic updates on typing)
    /// This is called when text changes to avoid updating diagnostics on every keystroke
    pub fn trigger_diagnostics_debounced(
        &mut self,
        new_text: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_diagnostics_enabled {
            self._diagnostics_debounce = None;
            self._last_diagnostics_text = Some(new_text);
            self.cached_diagnostics.clear();
            self.editor.update(cx, |editor, cx| {
                editor.set_diagnostics(Vec::new(), cx);
            });
            cx.emit(QueryEditorEvent::DiagnosticsChanged {
                diagnostics: Vec::new(),
            });
            cx.notify();
            return;
        }

        if !self.editor.read(cx).diagnostics_enabled() {
            self._diagnostics_debounce = None;
            self._last_diagnostics_text = Some(new_text);
            self.cached_diagnostics.clear();
            self.editor.update(cx, |editor, cx| {
                editor.set_diagnostics(Vec::new(), cx);
            });
            cx.emit(QueryEditorEvent::DiagnosticsChanged {
                diagnostics: Vec::new(),
            });
            cx.notify();
            return;
        }

        // Cancel any existing debounce task
        self._diagnostics_debounce = None;

        // Spawn a new debounced task
        let editor = self.editor.clone();
        let sql_lsp = self.sql_lsp.clone();

        self._diagnostics_debounce = Some(cx.spawn_in(window, async move |this, cx| {
            // Wait for typing to stop (300ms debounce)
            cx.background_executor()
                .timer(std::time::Duration::from_millis(300))
                .await;

            // Update diagnostics after debounce
            let _ = this.update_in(cx, |this, _window, cx| {
                // Get fresh text (in case it changed during debounce)
                let text_content = editor.read(cx).get_text(cx);
                let rope = zqlz_ui::widgets::Rope::from(text_content.as_str());

                // Run LSP validation
                let lsp_diagnostics = {
                    let mut lsp = sql_lsp.write();
                    lsp.validate_sql(&rope)
                };

                // Convert to TextEditor format
                let text_editor_diagnostics: Vec<zqlz_text_editor::Diagnostic> = lsp_diagnostics
                    .iter()
                    .map(|lsp_diag| {
                        use lsp_types::DiagnosticSeverity;
                        use zqlz_text_editor::DiagnosticLevel;

                        let start_offset = rope.position_to_offset(&lsp_diag.range.start);
                        let end_offset = rope.position_to_offset(&lsp_diag.range.end);

                        let severity = match lsp_diag.severity {
                            Some(DiagnosticSeverity::ERROR) => DiagnosticLevel::Error,
                            Some(DiagnosticSeverity::WARNING) => DiagnosticLevel::Warning,
                            Some(DiagnosticSeverity::INFORMATION) => DiagnosticLevel::Info,
                            Some(DiagnosticSeverity::HINT) => DiagnosticLevel::Hint,
                            _ => DiagnosticLevel::Error,
                        };

                        // Convert offsets to line/column for TextEditor Diagnostic
                        let start_line = rope.offset_to_position(start_offset).line as usize;
                        let start_column = rope.offset_to_position(start_offset).character as usize;
                        let end_line = rope.offset_to_position(end_offset).line as usize;
                        let end_column = rope.offset_to_position(end_offset).character as usize;

                        zqlz_text_editor::Diagnostic {
                            line: start_line,
                            column: start_column,
                            end_line: Some(end_line),
                            end_column: Some(end_column),
                            severity,
                            message: lsp_diag.message.clone(),
                            source: lsp_diag.source.clone(),
                        }
                    })
                    .collect();

                // Update editor diagnostics
                let text_editor_diagnostics_clone = text_editor_diagnostics.clone();
                editor.update(cx, |editor, cx| {
                    editor.set_diagnostics(text_editor_diagnostics_clone, cx);
                });
                this.cached_diagnostics = lsp_diagnostics.clone();

                // Update tracked text
                this._last_diagnostics_text = Some(text_content.to_string());

                // Emit diagnostics changed event for Problems panel
                let diagnostic_infos =
                    Self::diagnostic_infos_from_lsp_diagnostics(&lsp_diagnostics);
                cx.emit(QueryEditorEvent::DiagnosticsChanged {
                    diagnostics: diagnostic_infos,
                });

                cx.notify();
            });
        }));
    }

    /// Update SQL diagnostics based on current content
    /// Re-run LSP validation against the current buffer and emit `DiagnosticsChanged`.
    ///
    /// Called by the Reload button in the Problems panel so users can force a
    /// fresh diagnostic pass after a schema refresh.
    pub fn reload_diagnostics(&mut self, cx: &mut Context<Self>) {
        self.update_diagnostics(cx);
    }

    fn update_diagnostics(&mut self, cx: &mut Context<Self>) {
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_diagnostics_enabled {
            self.cached_diagnostics.clear();
            self.editor.update(cx, |editor, cx| {
                editor.set_diagnostics(Vec::new(), cx);
            });
            cx.emit(QueryEditorEvent::DiagnosticsChanged {
                diagnostics: Vec::new(),
            });
            cx.notify();
            return;
        }

        if !self.editor.read(cx).diagnostics_enabled() {
            self.cached_diagnostics.clear();
            self.editor.update(cx, |editor, cx| {
                editor.set_diagnostics(Vec::new(), cx);
            });
            cx.emit(QueryEditorEvent::DiagnosticsChanged {
                diagnostics: Vec::new(),
            });
            cx.notify();
            return;
        }

        // Get current editor text as a Rope for LSP analysis
        let text_content = self.editor.read(cx).get_text(cx);
        let rope = zqlz_ui::widgets::Rope::from(text_content.as_str());

        // Run LSP validation to get diagnostics
        let lsp_diagnostics = {
            let mut lsp = self.sql_lsp.write();
            lsp.validate_sql(&rope)
        };

        // Convert LSP diagnostics (lsp_types::Diagnostic) to TextEditor format
        let text_editor_diagnostics: Vec<zqlz_text_editor::Diagnostic> = lsp_diagnostics
            .iter()
            .map(|lsp_diag| {
                // Convert LSP Range (line/col) to byte offsets using rope
                let start_offset = rope.position_to_offset(&lsp_diag.range.start);
                let end_offset = rope.position_to_offset(&lsp_diag.range.end);

                // Convert LSP severity to TextEditor severity
                let severity = match lsp_diag.severity {
                    Some(DiagnosticSeverity::ERROR) => zqlz_text_editor::DiagnosticLevel::Error,
                    Some(DiagnosticSeverity::WARNING) => zqlz_text_editor::DiagnosticLevel::Warning,
                    Some(DiagnosticSeverity::INFORMATION) => {
                        zqlz_text_editor::DiagnosticLevel::Info
                    }
                    Some(DiagnosticSeverity::HINT) => zqlz_text_editor::DiagnosticLevel::Hint,
                    _ => zqlz_text_editor::DiagnosticLevel::Error,
                };

                // Convert offsets to line/column for TextEditor Diagnostic
                let start_line = rope.offset_to_position(start_offset).line as usize;
                let start_column = rope.offset_to_position(start_offset).character as usize;
                let end_line = rope.offset_to_position(end_offset).line as usize;
                let end_column = rope.offset_to_position(end_offset).character as usize;

                zqlz_text_editor::Diagnostic {
                    line: start_line,
                    column: start_column,
                    end_line: Some(end_line),
                    end_column: Some(end_column),
                    severity,
                    message: lsp_diag.message.clone(),
                    source: lsp_diag.source.clone(),
                }
            })
            .collect();

        // Update editor diagnostics
        self.editor.update(cx, |editor, cx| {
            editor.set_diagnostics(text_editor_diagnostics, cx);
        });

        self.cached_diagnostics = lsp_diagnostics.clone();
        let diagnostic_infos = Self::diagnostic_infos_from_lsp_diagnostics(&lsp_diagnostics);
        cx.emit(QueryEditorEvent::DiagnosticsChanged {
            diagnostics: diagnostic_infos,
        });

        cx.notify();
    }

    /// Get the current SQL content
    pub fn content(&self, cx: &App) -> SharedString {
        self.editor.read(cx).get_text(cx)
    }

    /// Get diagnostic counts from the current editor state
    ///
    /// Returns (errors, warnings, hints/infos)
    pub fn diagnostic_counts(&self, cx: &App) -> (usize, usize, usize) {
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_diagnostics_enabled {
            return (0, 0, 0);
        }

        if !self.editor.read(cx).diagnostics_enabled() {
            return (0, 0, 0);
        }

        Self::diagnostic_counts_from_lsp_diagnostics(&self.cached_diagnostics)
    }

    /// Get all diagnostics as a list for external display (e.g., Problems panel)
    pub fn get_diagnostics(&self, cx: &App) -> Vec<zqlz_ui::widgets::highlighter::Diagnostic> {
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_diagnostics_enabled {
            return Vec::new();
        }

        if !self.editor.read(cx).diagnostics_enabled() {
            return Vec::new();
        }

        Self::ui_diagnostics_from_lsp_diagnostics(&self.cached_diagnostics)
    }

    fn problem_index_for_cursor(
        problem_positions: &[(u32, u32)],
        cursor_line: u32,
        cursor_column: u32,
        forward: bool,
    ) -> Option<usize> {
        if problem_positions.is_empty() {
            return None;
        }

        if forward {
            problem_positions
                .iter()
                .position(|(line, column)| (*line, *column) > (cursor_line, cursor_column))
                .or(Some(0))
        } else {
            problem_positions
                .iter()
                .rposition(|(line, column)| (*line, *column) < (cursor_line, cursor_column))
                .or(Some(problem_positions.len().saturating_sub(1)))
        }
    }

    fn navigate_problem(&mut self, forward: bool, window: &mut Window, cx: &mut Context<Self>) {
        let mut diagnostics: Vec<(u32, u32, u32, u32)> = self
            .get_diagnostics(cx)
            .into_iter()
            .map(|diagnostic| {
                (
                    diagnostic.range.start.line,
                    diagnostic.range.start.character,
                    diagnostic.range.end.line,
                    diagnostic.range.end.character,
                )
            })
            .collect();
        diagnostics.sort_unstable_by_key(|(line, column, _, _)| (*line, *column));

        let cursor = self.editor.read(cx).get_cursor_position(cx);
        let positions: Vec<(u32, u32)> = diagnostics
            .iter()
            .map(|(line, column, _, _)| (*line, *column))
            .collect();
        let Some(index) = Self::problem_index_for_cursor(
            &positions,
            cursor.line as u32,
            cursor.column as u32,
            forward,
        ) else {
            return;
        };

        let (line, column, end_line, end_column) = diagnostics[index];
        let focus_handle = self.editor.read(cx).focus_handle(cx);
        focus_handle.focus(window, cx);
        self.editor.update(cx, |editor, cx| {
            editor.navigate_to(
                line as usize,
                column as usize,
                Some(end_line as usize),
                Some(end_column as usize),
                window,
                cx,
            );
        });
    }

    /// Navigate to a specific line and column (0-indexed)
    pub fn go_to_line(
        &mut self,
        line: usize,
        column: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editor.update(cx, |editor, cx| {
            editor.set_cursor_position(zqlz_text_editor::Position { line, column }, window, cx);
            editor.scroll_to_cursor();
        });
    }

    /// Set the SQL content
    pub fn set_text(&mut self, sql: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.dismiss_inline_suggestion(cx);
        self.editor.update(cx, |editor, cx| {
            editor.set_text(sql.to_string(), window, cx)
        });
        self._last_diagnostics_text = Some(sql.to_string());
    }

    /// Get the SQL to execute - either raw SQL or rendered template
    fn get_executable_sql(&self, cx: &App) -> String {
        match self.editor_mode {
            EditorMode::Sql => self.content(cx).to_string(),
            EditorMode::Template => {
                // In template mode, use the rendered SQL if available
                self.rendered_sql
                    .clone()
                    .unwrap_or_else(|| self.content(cx).to_string())
            }
        }
    }

    /// Toggle between SQL and Template mode
    fn toggle_editor_mode(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.editor_mode = match self.editor_mode {
            EditorMode::Sql => EditorMode::Template,
            EditorMode::Template => EditorMode::Sql,
        };

        // Update template preview if switching to template mode
        if self.editor_mode == EditorMode::Template {
            self.update_template_preview(cx);
        } else {
            self.rendered_sql = None;
            self.template_error = None;
        }

        cx.notify();
    }

    /// Update the template preview based on current content and params
    fn update_template_preview(&mut self, cx: &mut Context<Self>) {
        let template = self.content(cx).to_string();
        let params_json = self.template_params.read(cx).get_text(cx).to_string();

        // Parse JSON params
        let context: HashMap<String, minijinja::Value> = match serde_json::from_str(&params_json) {
            Ok(json_value) => {
                // Convert serde_json::Value to minijinja::Value HashMap
                if let serde_json::Value::Object(map) = json_value {
                    map.into_iter()
                        .map(|(k, v)| (k, json_to_minijinja_value(v)))
                        .collect()
                } else {
                    HashMap::new()
                }
            }
            Err(e) => {
                self.template_error = Some(format!("JSON parse error: {}", e));
                self.rendered_sql = None;
                return;
            }
        };

        // Render template
        match self.template_engine.render(&template, &context) {
            Ok(sql) => {
                self.rendered_sql = Some(sql);
                self.template_error = None;
            }
            Err(e) => {
                self.template_error = Some(format!("Template error: {}", e));
                self.rendered_sql = None;
            }
        }
    }

    /// Get the current editor mode
    pub fn editor_mode(&self) -> EditorMode {
        self.editor_mode
    }

    pub fn editor(&self) -> &Entity<TextEditor> {
        &self.editor
    }

    /// Get selected text, or entire content if nothing is selected
    pub fn selected_or_all_content(&self, cx: &App) -> String {
        // Get selected text from TextEditor, or fall back to all content if no selection
        self.editor
            .read(cx)
            .get_selected_text(cx)
            .unwrap_or_else(|| self.content(cx))
            .to_string()
    }

    /// Get the selected SQL, or the statement containing the cursor when there
    /// is no selection.
    pub fn selected_or_current_statement(&self, cx: &App) -> String {
        if let Some(selected_text) = self.editor.read(cx).get_selected_text(cx) {
            let selected_text = selected_text.to_string();
            if !selected_text.trim().is_empty() {
                return selected_text;
            }
        }

        if self.editor_mode == EditorMode::Template {
            return self.get_executable_sql(cx);
        }

        let full_sql = self.get_executable_sql(cx);
        let statements = split_statements(&full_sql);
        if statements.len() <= 1 {
            return full_sql;
        }

        let cursor_offset = self.editor.read(cx).get_cursor_offset(cx);
        let mut search_start = 0;

        for statement in statements {
            if let Some(relative_start) = full_sql[search_start..].find(&statement) {
                let statement_start = search_start + relative_start;
                let statement_end = statement_start + statement.len();

                if cursor_offset >= statement_start && cursor_offset <= statement_end {
                    return statement;
                }

                search_start = statement_end;
            }
        }

        full_sql
    }

    /// Navigate to a specific position in the editor
    ///
    /// Moves the cursor to the specified line and column (0-indexed).
    /// Optionally selects a range if end_line and end_column are provided.
    pub fn navigate_to(
        &mut self,
        line: usize,
        column: usize,
        end_line: usize,
        end_column: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let end_line_opt = if end_line > 0 { Some(end_line) } else { None };
        let end_col_opt = if end_column > 0 {
            Some(end_column)
        } else {
            None
        };

        self.editor.update(cx, |editor, cx| {
            editor.navigate_to(line, column, end_line_opt, end_col_opt, window, cx);
        });
    }

    /// Get focus handle for the editor
    pub fn editor_focus_handle(&self, cx: &App) -> FocusHandle {
        self.editor.read(cx).focus_handle(cx)
    }

    /// Get the editor name
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Check if the editor content is empty
    fn is_content_empty(&self, cx: &App) -> bool {
        self.content(cx).trim().is_empty()
    }

    /// Emit execute query event (entire content)
    pub fn emit_execute_query(&mut self, cx: &mut Context<Self>) {
        // Use rendered SQL in template mode, raw content in SQL mode
        let sql = self.get_executable_sql(cx);
        if sql.trim().is_empty() {
            return;
        }

        // In template mode, check for template errors
        if self.editor_mode == EditorMode::Template && self.template_error.is_some() {
            tracing::warn!("Cannot execute: template has errors");
            return;
        }

        // Don't set is_executing here - let the handler manage it
        // This prevents stuck state if there's no handler
        cx.emit(QueryEditorEvent::ExecuteQuery {
            sql,
            connection_id: self.connection_id,
        });
    }

    /// Emit execute selection event (selected text or entire content)
    pub fn emit_execute_selection(&mut self, cx: &mut Context<Self>) {
        if self.editor_mode == EditorMode::Template && self.template_error.is_some() {
            tracing::warn!("Cannot execute selection: template has errors");
            return;
        }

        let sql = self.selected_or_current_statement(cx);
        if sql.trim().is_empty() {
            return;
        }

        // Don't set is_executing here - let the handler manage it
        // This prevents stuck state if there's no handler
        cx.emit(QueryEditorEvent::ExecuteSelection {
            sql,
            connection_id: self.connection_id,
        });
    }

    /// Called when query execution completes
    pub fn set_executing(&mut self, executing: bool, cx: &mut Context<Self>) {
        self.is_executing = executing;
        cx.notify();
    }

    /// Emit cancel query event
    fn emit_cancel_query(&mut self, cx: &mut Context<Self>) {
        if self.is_executing {
            cx.emit(QueryEditorEvent::CancelQuery);
        }
    }

    /// Emit explain query event (entire content)
    pub fn emit_explain_query(&mut self, cx: &mut Context<Self>) {
        let sql = self.get_executable_sql(cx);
        if sql.trim().is_empty() {
            return;
        }

        // In template mode, check for template errors
        if self.editor_mode == EditorMode::Template && self.template_error.is_some() {
            tracing::warn!("Cannot explain: template has errors");
            return;
        }

        // Don't set is_executing here - let the handler manage it
        cx.emit(QueryEditorEvent::ExplainQuery {
            sql,
            connection_id: self.connection_id,
        });
    }

    /// Emit explain selection event (selected text or entire content)
    pub fn emit_explain_selection(&mut self, cx: &mut Context<Self>) {
        if self.editor_mode == EditorMode::Template && self.template_error.is_some() {
            tracing::warn!("Cannot explain selection: template has errors");
            return;
        }

        let sql = self.selected_or_current_statement(cx);
        if sql.trim().is_empty() {
            return;
        }

        cx.emit(QueryEditorEvent::ExplainSelection {
            sql,
            connection_id: self.connection_id,
        });
    }

    /// Emit save object event (for views, procedures, functions, triggers)
    fn emit_save_object(&mut self, cx: &mut Context<Self>) {
        let Some(connection_id) = self.connection_id else {
            tracing::warn!("Cannot save: no connection");
            return;
        };

        if !self.object_type.supports_save() {
            tracing::warn!("Cannot save: object type does not support save");
            return;
        }

        let definition = self.content(cx).to_string();
        if definition.trim().is_empty() {
            tracing::warn!("Cannot save: empty definition");
            return;
        }

        cx.emit(QueryEditorEvent::SaveObject {
            connection_id,
            object_type: self.object_type.clone(),
            definition,
        });
    }

    /// Get the object type being edited
    pub fn object_type(&self) -> &EditorObjectType {
        &self.object_type
    }

    /// Set the object type (useful when saving a new object with a name)
    pub fn set_object_type(&mut self, object_type: EditorObjectType, cx: &mut Context<Self>) {
        self.object_type = object_type;
        cx.notify();
    }

    /// Mark the editor as clean (no unsaved changes)
    pub fn mark_clean(&mut self, cx: &mut Context<Self>) {
        self.editor.update(cx, |editor, cx| {
            editor.mark_saved(cx);
        });
        cx.emit(QueryEditorEvent::DocumentStateChanged);
        cx.notify();
    }

    pub fn is_dirty(&self, cx: &App) -> bool {
        self.editor.read(cx).is_dirty()
    }

    pub fn document_context(&self, cx: &App) -> zqlz_text_editor::DocumentContext {
        self.editor.read(cx).document_context()
    }

    pub fn document_identity(&self, cx: &App) -> zqlz_text_editor::DocumentIdentity {
        self.editor.read(cx).document_identity().clone()
    }

    /// Get the saved query ID if this editor is editing a saved query
    pub fn saved_query_id(&self) -> Option<Uuid> {
        self.saved_query_id
    }

    /// Get the connection associated with this editor, if any.
    pub fn connection_id(&self) -> Option<Uuid> {
        self.connection_id
    }

    /// Set the saved query ID (used when a query is saved)
    pub fn set_saved_query_id(&mut self, id: Option<Uuid>, cx: &mut Context<Self>) {
        self.saved_query_id = id;
        cx.notify();
    }

    /// Set the editor name/title
    pub fn set_name(&mut self, name: &str, cx: &mut Context<Self>) {
        self.name = name.to_string();
        cx.emit(QueryEditorEvent::DocumentStateChanged);
        cx.emit(PanelEvent::LayoutChanged);
        cx.notify();
    }

    /// Handle TriggerCompletion action - manually trigger completion popup
    fn handle_trigger_completion(
        &mut self,
        _action: &TriggerCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editor.update(cx, |editor, cx| {
            editor.handle_completion_action(&TriggerCompletion, window, cx);
        });
    }

    /// Handle AcceptCompletion action - accepts completion if menu is open, otherwise indents
    fn handle_accept_completion(
        &mut self,
        _action: &AcceptCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // If there's an inline suggestion, accept it
        if let Some(_inline_suggestion) = &self.inline_suggestion {
            self.editor.update(cx, |editor, cx| {
                editor.accept_inline_suggestion(window, cx);
            });
            self.inline_suggestion = None;
            return;
        }

        self.editor.update(cx, |editor, cx| {
            editor.handle_completion_action(&AcceptCompletion, window, cx);
        });
    }

    /// Handle CancelCompletion action - hides completion menu
    fn handle_cancel_completion(
        &mut self,
        _action: &CancelCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editor.update(cx, |editor, cx| {
            editor.handle_completion_action(
                &zqlz_text_editor::actions::DismissCompletion,
                window,
                cx,
            );
        });
        self.inline_suggestion = None;
    }

    /// Navigates to the next completion item when the menu is open.
    /// Propagates the event to the editor (for cursor movement) when closed.
    fn handle_next_completion(
        &mut self,
        _action: &NextCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.editor.read(cx).is_completion_menu_open(cx) {
            cx.propagate();
            return;
        }
        self.editor.update(cx, |editor, cx| {
            editor.handle_completion_action(
                &zqlz_text_editor::actions::SelectNextCompletion,
                window,
                cx,
            );
        });
    }

    /// Navigates to the previous completion item when the menu is open.
    /// Propagates the event to the editor (for cursor movement) when closed.
    fn handle_previous_completion(
        &mut self,
        _action: &PreviousCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.editor.read(cx).is_completion_menu_open(cx) {
            cx.propagate();
            return;
        }
        self.editor.update(cx, |editor, cx| {
            editor.handle_completion_action(
                &zqlz_text_editor::actions::SelectPreviousCompletion,
                window,
                cx,
            );
        });
    }

    fn handle_next_problem(
        &mut self,
        _action: &NextProblem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_problem(true, window, cx);
    }

    fn handle_previous_problem(
        &mut self,
        _action: &PreviousProblem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.navigate_problem(false, window, cx);
    }

    /// Confirms the selected completion item when the menu is open.
    /// Propagates the event (for newline/tab) when the menu is closed.
    fn handle_confirm_completion(
        &mut self,
        _action: &ConfirmCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.editor.read(cx).is_completion_menu_open(cx) {
            cx.propagate();
            return;
        }
        self.editor.update(cx, |editor, cx| {
            editor.handle_completion_action(&AcceptCompletion, window, cx);
        });
    }

    /// Cancels/hides the completion menu. Propagates the event when the menu is closed.
    fn handle_cancel_completion_menu(
        &mut self,
        _action: &CancelCompletionMenu,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.editor.read(cx).is_completion_menu_open(cx) {
            cx.propagate();
            return;
        }
        self.editor.update(cx, |editor, cx| {
            editor.handle_completion_action(
                &zqlz_text_editor::actions::DismissCompletion,
                window,
                cx,
            );
        });
    }

    /// Handle AcceptInlineSuggestion action - accepts the current inline suggestion
    fn handle_accept_inline_suggestion(
        &mut self,
        _action: &AcceptInlineSuggestion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.inline_suggestion.is_some() {
            self.accept_inline_suggestion(window, cx);
            tracing::debug!("Accepted inline suggestion via action");
        }
    }

    /// Handle DismissInlineSuggestion action - dismisses the current inline suggestion
    fn handle_dismiss_inline_suggestion(
        &mut self,
        _action: &DismissInlineSuggestion,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dismiss_inline_suggestion(cx);
        tracing::debug!("Dismissed inline suggestion via action");
    }

    /// Trigger inline suggestion at current cursor position.
    ///
    /// LSP suggestions are resolved synchronously (they use cached completion state).
    /// AI suggestions are dispatched as a background task so the GPUI foreground
    /// thread is never blocked on a network call.
    pub fn trigger_inline_suggestion(&mut self, cx: &mut Context<Self>) {
        let settings = ZqlzSettings::global(cx);

        if !settings.editor.inline_suggestions_enabled {
            return;
        }

        let inline_suggestions_delay = settings.editor.inline_suggestions_delay_ms;

        let cursor_offset = self.editor.read(cx).get_cursor_offset(cx);
        let text = self.editor.read(cx).get_text(cx);

        let prefix = if cursor_offset <= text.len() {
            text[..cursor_offset].to_string()
        } else {
            text.to_string()
        };
        let suffix = if cursor_offset < text.len() {
            text[cursor_offset..].to_string()
        } else {
            String::new()
        };

        let provider_setting = settings.editor.inline_suggestions_provider;

        // LSP path is synchronous — uses cached completion state, no I/O.
        let lsp_suggestion = match provider_setting {
            InlineSuggestionProvider::LspOnly | InlineSuggestionProvider::Both => {
                self.get_lsp_inline_suggestion(prefix.clone(), suffix.clone(), cursor_offset, cx)
            }
            InlineSuggestionProvider::AiOnly => None,
        };

        if let Some((suggestion_text, start, end, _)) = lsp_suggestion {
            self.inline_suggestion = Some(InlineSuggestionState {
                suggestion: suggestion_text.clone(),
                start_offset: start,
                end_offset: end,
                source: InlineSuggestionSource::Lsp,
            });
            self.editor.update(cx, |editor, cx| {
                editor.set_inline_suggestion(suggestion_text, start, cx);
            });
            cx.notify();
            return;
        }

        // AI path — kick off a background task to avoid blocking the UI thread.
        let needs_ai = matches!(
            provider_setting,
            InlineSuggestionProvider::AiOnly | InlineSuggestionProvider::Both
        );
        if !needs_ai {
            return;
        }

        let settings = ZqlzSettings::global(cx);
        let ai_provider = AiProviderFactory::create_provider(
            settings.editor.ai_provider,
            settings.editor.ai_api_key.clone(),
            settings.editor.ai_model.clone(),
            settings.editor.ai_temperature,
        );
        let Some(ai_provider) = ai_provider else {
            return;
        };
        if !ai_provider.is_available() {
            return;
        }

        self.inline_suggestion_generation = self.inline_suggestion_generation.wrapping_add(1);
        let inline_suggestion_generation = self.inline_suggestion_generation;

        let request = CompletionRequest {
            prefix: prefix.clone().into(),
            suffix: suffix.clone().into(),
            cursor_offset,
            schema_context: None,
            dialect: None,
        };

        // Spawn on the background executor so the network call doesn't block the
        // GPUI foreground thread, then update state on the foreground via WeakEntity.
        self._inline_suggestion_debounce = Some(cx.spawn(async move |this, cx| {
            cx.background_executor()
                .timer(std::time::Duration::from_millis(
                    inline_suggestions_delay as u64,
                ))
                .await;

            let result = cx
                .background_spawn(async move { ai_provider.suggest(request).await })
                .await;

            let Ok(response) = result else {
                return;
            };
            let suggestion = response.suggestion;
            if suggestion.is_empty() {
                return;
            }

            this.update(&mut cx.clone(), |this, cx| {
                if this.inline_suggestion_generation != inline_suggestion_generation {
                    return;
                }

                let editor = this.editor.read(cx);
                let current_cursor_offset = editor.get_cursor_offset(cx);
                let current_text = editor.get_text(cx);
                if current_cursor_offset != cursor_offset {
                    return;
                }
                if current_cursor_offset > current_text.len() {
                    return;
                }
                if current_text[..current_cursor_offset] != prefix
                    || current_text[current_cursor_offset..] != suffix
                {
                    return;
                }

                this.inline_suggestion = Some(InlineSuggestionState {
                    suggestion: suggestion.to_string(),
                    start_offset: cursor_offset,
                    end_offset: cursor_offset + suggestion.len(),
                    source: InlineSuggestionSource::Ai,
                });
                this.editor.update(cx, |editor, cx| {
                    editor.set_inline_suggestion(suggestion.to_string(), cursor_offset, cx);
                });
                cx.notify();
            })
            .ok();
        }));
    }

    /// Get inline suggestion from LSP completions (synchronous — uses cached state).
    fn get_lsp_inline_suggestion(
        &self,
        _prefix: String,
        _suffix: String,
        cursor_offset: usize,
        cx: &App,
    ) -> Option<(String, usize, usize, &'static str)> {
        let settings = ZqlzSettings::global(cx);

        // Check if LSP completions are enabled
        if !settings.editor.lsp_enabled || !settings.editor.lsp_completions_enabled {
            return None;
        }

        if self.editor.read(cx).is_completion_menu_open(cx) {
            return None;
        }

        let completions = self.editor.read(cx).get_completions(cx);

        // Find the best completion for inline suggestion
        for completion in completions.iter() {
            if let Some(insert_text) = &completion.insert_text
                && !insert_text.is_empty()
            {
                return Some((
                    insert_text.clone(),
                    cursor_offset,
                    cursor_offset + insert_text.len(),
                    "LSP",
                ));
            }
        }

        None
    }

    /// Accept the current inline suggestion
    pub fn accept_inline_suggestion(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(inline_suggestion) = self.inline_suggestion.take() {
            self.editor.update(cx, |editor, cx| {
                editor.accept_inline_suggestion(window, cx);
            });

            tracing::debug!(
                "Inline suggestion accepted: {}",
                inline_suggestion.suggestion
            );
            cx.notify();
        }
    }

    /// Dismiss the current inline suggestion
    pub fn dismiss_inline_suggestion(&mut self, cx: &mut Context<Self>) {
        self.inline_suggestion_generation = self.inline_suggestion_generation.wrapping_add(1);
        if self.inline_suggestion.is_some() {
            self.inline_suggestion = None;
            self.editor.update(cx, |editor, cx| {
                editor.clear_inline_suggestion(cx);
            });
            tracing::debug!("Inline suggestion dismissed");
            cx.notify();
        }
    }

    /// Check if there's a current inline suggestion
    pub fn has_inline_suggestion(&self) -> bool {
        self.inline_suggestion.is_some()
    }

    /// Get the current inline suggestion
    pub fn get_inline_suggestion(&self) -> Option<&InlineSuggestionState> {
        self.inline_suggestion.as_ref()
    }

    /// Handle ShowHover action - show hover documentation for symbol under cursor
    /// First checks for schema symbols (tables, columns), then falls back to LSP hover
    fn handle_show_hover(
        &mut self,
        _action: &ShowHover,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_hover_enabled {
            self.hover_content = None;
            self.schema_symbol_info = None;
            self.editor.update(cx, |editor, _editor_cx| {
                editor.clear_hover();
            });
            cx.notify();
            return;
        }

        tracing::debug!("ShowHover action triggered");

        // First, try to find a schema symbol at the cursor position
        let cursor_offset = self.editor.read(cx).get_cursor_offset(cx);
        let text = self.editor.read(cx).get_text(cx);

        // Try schema metadata lookup first
        let schema_symbol = self.find_schema_symbol_at_cursor(&text, cursor_offset, cx);

        if let Some(symbol_info) = schema_symbol {
            // Found a schema symbol - show schema metadata overlay
            tracing::info!(
                "Schema symbol found: {} ({})",
                symbol_info.name,
                symbol_info.symbol_type_name()
            );
            self.schema_symbol_info = Some(symbol_info.clone());
            self.hover_content = Some(Self::format_schema_symbol(&symbol_info));
        } else {
            // No schema symbol found - fall back to LSP hover
            self.schema_symbol_info = None;
            let text = self.editor.read(cx).get_text(cx).to_string();
            let rope = zqlz_ui::widgets::Rope::from(text.as_str());
            let hover = self.sql_lsp.read().get_hover(&rope, cursor_offset);

            if let Some(hover) = hover {
                let content = match &hover.contents {
                    lsp_types::HoverContents::Scalar(scalar) => match scalar {
                        lsp_types::MarkedString::String(s) => s.clone(),
                        lsp_types::MarkedString::LanguageString(ls) => ls.value.clone(),
                    },
                    lsp_types::HoverContents::Array(arr) => arr
                        .iter()
                        .map(|item| match item {
                            lsp_types::MarkedString::String(s) => s.clone(),
                            lsp_types::MarkedString::LanguageString(ls) => ls.value.clone(),
                        })
                        .collect::<Vec<_>>()
                        .join("\n\n"),
                    lsp_types::HoverContents::Markup(markup) => markup.value.clone(),
                };

                tracing::info!("LSP Hover content: {}", content);
                self.hover_content = Some(content);
            } else {
                self.hover_content = None;
            }
        }

        cx.notify();
    }

    /// Find a schema symbol at the cursor position
    fn find_schema_symbol_at_cursor(
        &mut self,
        text: &str,
        offset: usize,
        _cx: &mut Context<Self>,
    ) -> Option<SchemaSymbolInfo> {
        // Ensure schema metadata is initialized
        if self.schema_metadata.is_none() {
            // Try to get schema from LSP
            let lsp = self.sql_lsp.read();
            let db_schema = lsp.get_schema_for_metadata();
            drop(lsp);

            // Only create if we have tables (schema is loaded)
            if !db_schema.tables.is_empty() {
                self.schema_metadata = Some(SchemaMetadata::new(db_schema));
            } else {
                return None;
            }
        }

        // Find symbol at offset
        self.schema_metadata
            .as_ref()?
            .find_symbol_at_offset(text, offset)
    }

    /// Format schema symbol info for display in hover popover
    fn format_schema_symbol(symbol_info: &SchemaSymbolInfo) -> String {
        let mut content = String::new();

        // Header with symbol type
        content.push_str(&format!(
            "**{}**: `{}`\n\n",
            symbol_info.symbol_type_name(),
            symbol_info.name
        ));

        // Add details if available
        if let Some(details) = &symbol_info.details {
            // Table/view details
            if let Some(columns) = &details.columns {
                content.push_str("**Columns**:\n");
                for col in columns.iter().take(10) {
                    let pk_marker = if col.is_primary_key { " PK" } else { "" };
                    let _nullable = if col.nullable { "?" } else { "" };
                    content.push_str(&format!(
                        "- `{}`: {}{}\n",
                        col.name, col.data_type, pk_marker
                    ));
                }
                if columns.len() > 10 {
                    content.push_str(&format!("... and {} more\n", columns.len() - 10));
                }
            }

            // Column details
            if let Some(table_name) = &details.table_name {
                content.push_str(&format!("**Table**: `{}`\n", table_name));
            }
            if let Some(data_type) = &details.data_type {
                content.push_str(&format!("**Type**: {}\n", data_type));
            }
            if let Some(nullable) = details.nullable {
                content.push_str(&format!(
                    "**Nullable**: {}\n",
                    if nullable { "Yes" } else { "No" }
                ));
            }
            if let Some(is_pk) = details.is_primary_key
                && is_pk
            {
                content.push_str("**Primary Key**: Yes\n");
            }
            if let Some(row_count) = details.row_count {
                content.push_str(&format!("**Rows**: ~{}\n", row_count));
            }
        }

        content
    }

    /// Clear the hover popover
    pub fn clear_hover(&mut self, cx: &mut Context<Self>) {
        self.hover_content = None;
        self.schema_symbol_info = None;
        self.editor.update(cx, |editor, editor_cx| {
            editor.clear_hover();
            editor.clear_signature_help(editor_cx);
        });
        cx.notify();
    }

    /// Handle TriggerParameterHints action - show signature help for function under cursor
    fn handle_trigger_parameter_hints(
        &mut self,
        _action: &TriggerParameterHints,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("TriggerParameterHints action triggered");

        let signature_help = self.editor.read(cx).get_signature_help(cx);
        let anchor_offset = self.editor.read(cx).get_cursor_offset(cx);

        if let Some(sig_help) = signature_help {
            let content = Self::format_signature_help(&sig_help);
            tracing::info!("Signature help content: {}", content);
            self.editor.update(cx, |editor, cx| {
                editor.set_signature_help(content, anchor_offset, cx);
            });
        } else {
            self.editor.update(cx, |editor, cx| {
                editor.clear_signature_help(cx);
            });
        }

        cx.notify();
    }

    /// Handle ShowCodeActions action - show available code actions at cursor
    fn handle_show_code_actions(
        &mut self,
        _action: &ShowCodeActions,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_code_actions_enabled {
            self.clear_code_actions(cx);
            return;
        }

        tracing::debug!("ShowCodeActions action triggered");
        self.editor.update(cx, |editor, cx| {
            editor.clear_signature_help(cx);
            editor.update_code_actions(cx);
        });
        cx.notify();
    }

    /// Apply a code action by index
    /// Clear the code actions list
    pub fn clear_code_actions(&mut self, cx: &mut Context<Self>) {
        self.editor.update(cx, |editor, cx| {
            editor.clear_code_actions(cx);
            editor.clear_signature_help(cx);
        });
        cx.notify();
    }

    /// Format signature help for display
    fn format_signature_help(sig_help: &lsp_types::SignatureHelp) -> String {
        let mut result = String::new();

        if let Some(signatures) = &sig_help.signatures.first() {
            result.push_str(&format!("**{}**\n\n", signatures.label));

            if let Some(params) = &signatures.parameters {
                result.push_str("**Parameters:**\n");
                let active_param = sig_help.active_parameter.unwrap_or(0) as usize;

                for (i, param) in params.iter().enumerate() {
                    let marker = if i == active_param { "◀" } else { " " };
                    if let lsp_types::ParameterLabel::Simple(label) = &param.label {
                        result.push_str(&format!("{} `{}`\n", marker, label));
                    }
                }
            }
        }

        result
    }

    /// Clear the signature help popover
    pub fn clear_signature_help(&mut self, cx: &mut Context<Self>) {
        self.editor.update(cx, |editor, cx| {
            editor.clear_signature_help(cx);
        });
        cx.notify();
    }

    // ====================
    // Code Editing Actions
    // ====================

    /// Format the SQL query using production-level formatter
    fn handle_format_query(
        &mut self,
        _action: &FormatQuery,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("🔍 handle_format_query called!");
        self.format_query(window, cx);
    }

    /// Format the SQL query using production-level formatter
    fn format_query(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        tracing::info!("🔍 format_query called!");
        let driver_type = self.driver_type.as_deref();
        self.editor.update(cx, |editor, cx| {
            if self.object_type.is_procedural() {
                let content = editor.get_text(cx).to_string();
                let formatted = Self::format_sql_with_dollar_quoting(&content, driver_type);
                if formatted != content {
                    editor.replace_all_text(&formatted, cx);
                }
            } else {
                editor.format_sql(cx);
            }
        });
        self.update_diagnostics(cx);
        self._last_diagnostics_text = Some(self.content(cx).to_string());
        cx.notify();
        tracing::info!("🔍 format_query completed successfully");
    }

    /// Handle SaveQuery action (Cmd+S / Ctrl+S)
    /// For database objects (Views, Functions, etc.), this triggers SaveObject instead
    fn handle_save_query(
        &mut self,
        _action: &SaveQuery,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("SaveQuery action triggered");

        // For database objects (Views, Functions, etc.), use SaveObject
        if self.object_type.supports_save() {
            tracing::info!("Redirecting to SaveObject for {:?}", self.object_type);
            self.emit_save_object(cx);
            return;
        }

        // For regular queries, emit SaveQuery event
        let sql = self.content(cx).to_string();
        cx.emit(QueryEditorEvent::SaveQuery {
            saved_query_id: self.saved_query_id,
            connection_id: self.connection_id,
            sql,
        });
    }

    fn format_sql(sql: &str, driver_type: Option<&str>) -> String {
        use sqlformat::{FormatOptions, Indent, QueryParams, format};

        let options = FormatOptions {
            indent: Indent::Spaces(4),
            uppercase: Some(true),
            lines_between_queries: 1,
            ignore_case_convert: None,
            inline: false,
            max_inline_block: 50,
            max_inline_arguments: None,
            max_inline_top_level: None,
            joins_as_top_level: false,
            dialect: driver_type_to_sqlformat_dialect(driver_type),
        };

        format(sql, &QueryParams::None, &options)
    }

    /// Format SQL that may contain dollar-quoted procedural blocks (PL/pgSQL).
    ///
    /// Splits on dollar-quote delimiters (`$tag$`), formats only the outer DDL
    /// parts with the standard SQL formatter, and preserves the procedural body
    /// verbatim so that PL/pgSQL `DECLARE`/`BEGIN`/`END` blocks are not mangled.
    fn format_sql_with_dollar_quoting(sql: &str, driver_type: Option<&str>) -> String {
        // Match dollar-quote delimiters like $$, $function$, $body$, $BODY$, etc.
        let delimiter_pattern =
            regex::Regex::new(r"\$([a-zA-Z_][a-zA-Z0-9_]*)?\$").unwrap_or_else(|_| {
                // Fallback: return unformatted if regex fails
                regex::Regex::new(r"^\b$").expect("infallible regex")
            });

        let delimiters: Vec<_> = delimiter_pattern.find_iter(sql).collect();

        // Dollar-quoted blocks come in pairs: opening and closing use the same tag
        if delimiters.len() >= 2 {
            let open = delimiters[0];
            let open_tag = open.as_str();

            // Find the matching close delimiter (same tag)
            if let Some(close) = delimiters[1..].iter().find(|d| d.as_str() == open_tag) {
                let before_body = &sql[..open.start()];
                let body = &sql[open.start()..close.end()];
                let after_body = &sql[close.end()..];

                let formatted_before = Self::format_sql(before_body, driver_type);
                let formatted_after = if after_body.trim().is_empty() {
                    after_body.to_string()
                } else {
                    Self::format_sql(after_body, driver_type)
                };

                return format!(
                    "{}\n{}\n{}",
                    formatted_before.trim_end(),
                    body,
                    formatted_after
                );
            }
        }

        // MySQL-style DELIMITER blocks or no dollar quoting found:
        // check for BEGIN/END procedural blocks (MySQL stored routines)
        let sql_upper = sql.to_uppercase();
        if sql_upper.contains("CREATE") && sql_upper.contains("BEGIN") {
            // Don't format at all — MySQL procedural bodies break the formatter
            return sql.to_string();
        }

        // No procedural content detected, format normally
        Self::format_sql(sql, driver_type)
    }

    /// Render the toolbar with execution controls
    fn render_toolbar(&self, window: &Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let is_empty = self.is_content_empty(cx);
        let has_template_error = self.template_error.is_some();
        let supports_save = self.object_type.supports_save();
        let has_connection = self.connection_id.is_some();

        h_flex()
            .id("query-editor-toolbar")
            .on_action(cx.listener(Self::handle_accept_inline_suggestion))
            .on_action(cx.listener(Self::handle_dismiss_inline_suggestion))
            .on_action(cx.listener(Self::handle_format_query))
            .on_action(cx.listener(Self::handle_save_query))
            .on_action(cx.listener(Self::handle_show_hover))
            .on_action(cx.listener(Self::handle_trigger_parameter_hints))
            .on_action(cx.listener(Self::handle_show_code_actions))
            .w_full()
            .h(px(36.0))
            .px_2()
            .gap_2()
            .items_center()
            .border_b_1()
            .border_color(theme.border)
            // Save button for database objects (views, procedures, etc.)
            .when(supports_save, |this| {
                this.child(
                    Button::new("save")
                        .primary()
                        .small()
                        .icon(ZqlzIcon::FloppyDisk)
                        .label("Save")
                        .tooltip_with_action("Save", &SaveQuery, None)
                        .disabled(!has_connection || is_empty || !self.editor.read(cx).is_dirty())
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.emit_save_object(cx);
                        })),
                )
            })
            // Save query button for regular queries (icon only)
            .when(!supports_save && has_connection, |this| {
                this.child(
                    Button::new("save-query")
                        .ghost()
                        .small()
                        .icon(ZqlzIcon::FloppyDisk)
                        .tooltip_with_action("Save Query", &SaveQuery, None)
                        .disabled(is_empty)
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.handle_save_query(&SaveQuery, window, cx);
                        })),
                )
            })
            // Run button with Play icon
            .child(
                Button::new("execute")
                    .when(supports_save, |b| b.ghost())
                    .when(!supports_save, |b| b.primary())
                    .small()
                    .icon(ZqlzIcon::Play)
                    .tooltip(Self::run_query_tooltip_text())
                    .disabled(self.is_executing || is_empty || has_template_error)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.emit_execute_query(cx);
                    })),
            )
            .child(
                Button::new("explain")
                    .ghost()
                    .small()
                    .icon(ZqlzIcon::Lightbulb)
                    .tooltip("Explain Query")
                    .disabled(self.is_executing || is_empty || has_template_error)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.emit_explain_query(cx);
                    })),
            )
            // Stop button with icon (only shown when executing)
            .when(self.is_executing, |this| {
                this.child(
                    Button::new("stop")
                        .danger()
                        .small()
                        .icon(ZqlzIcon::Stop)
                        .tooltip({
                            #[cfg(target_os = "macos")]
                            {
                                "Stop Query (⌘Esc)"
                            }
                            #[cfg(not(target_os = "macos"))]
                            {
                                "Stop Query (Esc)"
                            }
                        })
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.emit_cancel_query(cx);
                        })),
                )
            })
            .child(
                Button::new("format")
                    .ghost()
                    .small()
                    .icon(ZqlzIcon::TextIndent)
                    .tooltip_with_action("Format SQL", &FormatQuery, None)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.format_query(window, cx);
                    })),
            )
            .child(div().h(px(20.0)).w(px(1.0)).bg(theme.border).mx_1())
            // Only show SQL/Template toggle for regular queries
            .when(!supports_save, |this| {
                this.child({
                    let is_active = self.editor_mode == EditorMode::Sql;
                    let btn = Button::new("mode-sql")
                        .small()
                        .icon(ZqlzIcon::Code)
                        .tooltip("SQL Mode")
                        .on_click(cx.listener(|this, _, window, cx| {
                            if this.editor_mode != EditorMode::Sql {
                                this.toggle_editor_mode(window, cx);
                            }
                        }));
                    if is_active {
                        btn.secondary_primary().selected(true)
                    } else {
                        btn.ghost()
                    }
                })
                .child({
                    let is_active = self.editor_mode == EditorMode::Template;
                    let btn = Button::new("mode-template")
                        .small()
                        .icon(ZqlzIcon::BracketsCurly)
                        .tooltip("Template Mode")
                        .on_click(cx.listener(|this, _, window, cx| {
                            if this.editor_mode != EditorMode::Template {
                                this.toggle_editor_mode(window, cx);
                            }
                        }));
                    if is_active {
                        btn.secondary_primary().selected(true)
                    } else {
                        btn.ghost()
                    }
                })
            })
            // Show object type indicator for database objects
            .when(supports_save, |this| {
                this.child(
                    div()
                        .text_xs()
                        .font_weight(gpui::FontWeight::MEDIUM)
                        .text_color(theme.accent)
                        .child(self.object_type.display_name()),
                )
            })
            .child(div().flex_1())
            .when(
                self.editor_mode == EditorMode::Template && self.template_error.is_some(),
                |this| {
                    this.child(
                        div()
                            .text_xs()
                            .text_color(theme.danger)
                            .max_w(px(300.0))
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(self.template_error.clone().unwrap_or_default()),
                    )
                },
            )
            // Connection switcher dropdown (only shown if there are available connections)
            .when(!self.available_connections.is_empty(), |this| {
                let connection_label = if let Some(name) = &self.connection_name {
                    name.clone()
                } else if self.connection_id.is_some() {
                    "Connected".to_string()
                } else {
                    "Select Connection".to_string()
                };

                let available_connections = self.available_connections.clone();
                let current_connection_id = self.connection_id;
                let entity = cx.entity().downgrade();

                this.child(
                    Button::new("connection-switcher")
                        .small()
                        .ghost()
                        .icon(ZqlzIcon::Database)
                        .label(connection_label.clone())
                        .dropdown_menu(move |mut menu, _window, _cx| {
                            use zqlz_ui::widgets::menu::PopupMenuItem;
                            menu = menu.max_h(px(300.0)).scrollable(true);
                            for (conn_id, conn_name) in &available_connections {
                                let is_current = current_connection_id == Some(*conn_id);
                                let conn_id = *conn_id;
                                let conn_name_clone = conn_name.clone();
                                let entity = entity.clone();
                                menu = menu.item(
                                    PopupMenuItem::new(conn_name_clone)
                                        .checked(is_current)
                                        .on_click(move |_event, _window, cx| {
                                            _ = entity.update(cx, |_this, cx| {
                                                cx.emit(QueryEditorEvent::SwitchConnection {
                                                    connection_id: conn_id,
                                                });
                                            });
                                        }),
                                );
                            }
                            menu
                        }),
                )
            })
            // Database switcher dropdown (only shown if there are available databases)
            .when(!self.available_databases.is_empty(), |this| {
                let database_label = if let Some(db) = &self.current_database {
                    db.clone()
                } else {
                    "Select Database".to_string()
                };

                let available_databases = self.available_databases.clone();
                let current_database = self.current_database.clone();
                let entity = cx.entity().downgrade();

                this.child(
                    Button::new("database-switcher")
                        .small()
                        .ghost()
                        .icon(ZqlzIcon::Table)
                        .label(database_label.clone())
                        .dropdown_menu(move |mut menu, _window, _cx| {
                            use zqlz_ui::widgets::menu::PopupMenuItem;
                            menu = menu.max_h(px(300.0)).scrollable(true);
                            for db_name in &available_databases {
                                let is_current = current_database.as_ref() == Some(db_name);
                                let db_name_clone = db_name.clone();
                                let entity = entity.clone();
                                menu = menu.item(
                                    PopupMenuItem::new(db_name_clone.clone())
                                        .checked(is_current)
                                        .on_click(move |_event, _window, cx| {
                                            _ = entity.update(cx, |_this, cx| {
                                                cx.emit(QueryEditorEvent::SwitchDatabase {
                                                    database_name: db_name_clone.clone(),
                                                });
                                            });
                                        }),
                                );
                            }
                            menu
                        }),
                )
            })
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .child(self.render_toolbar_shortcut_hints(window, cx))
                    // Error/warning count badge
                    .map(|this| {
                        let (errors, warnings, _) = self.diagnostic_counts(cx);
                        if errors > 0 {
                            this.child(
                                h_flex()
                                    .gap_1()
                                    .items_center()
                                    .child(div().size_2().rounded_full().bg(theme.danger))
                                    .child(div().text_xs().text_color(theme.danger).child(format!(
                                        "{} error{}",
                                        errors,
                                        if errors == 1 { "" } else { "s" }
                                    )))
                                    .child(self.render_problem_shortcut_hints(window, cx)),
                            )
                        } else if warnings > 0 {
                            this.child(
                                h_flex()
                                    .gap_1()
                                    .items_center()
                                    .child(div().size_2().rounded_full().bg(theme.warning))
                                    .child(div().text_xs().text_color(theme.warning).child(
                                        format!(
                                            "{} warning{}",
                                            warnings,
                                            if warnings == 1 { "" } else { "s" }
                                        ),
                                    ))
                                    .child(self.render_problem_shortcut_hints(window, cx)),
                            )
                        } else {
                            this
                        }
                    })
                    .child(if let Some(name) = &self.connection_name {
                        name.clone()
                    } else if self.connection_id.is_some() {
                        "Connected".to_string()
                    } else {
                        "No Connection".to_string()
                    }),
            )
    }

    fn render_toolbar_shortcut_hints(
        &self,
        window: &Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let mut hints: Vec<AnyElement> = Vec::new();

        #[cfg(target_os = "macos")]
        let run_shortcut = Self::kbd_for_keystroke("cmd-enter");
        #[cfg(not(target_os = "macos"))]
        let run_shortcut = Self::kbd_for_keystroke("ctrl-enter");

        if let Some(kbd) = run_shortcut {
            hints.push(self.render_toolbar_shortcut_hint("Run", kbd, cx));
        }
        if let Some(kbd) = Kbd::binding_for_action_in(
            &zqlz_text_editor::actions::TriggerCompletion,
            &self.focus_handle,
            window,
        ) {
            hints.push(self.render_toolbar_shortcut_hint("Complete", kbd, cx));
        }
        if let Some(kbd) = Kbd::binding_for_action_in(&FormatQuery, &self.focus_handle, window) {
            hints.push(self.render_toolbar_shortcut_hint("Format", kbd, cx));
        }
        if let Some(kbd) = Kbd::binding_for_action_in(&ShowHover, &self.focus_handle, window) {
            hints.push(self.render_toolbar_shortcut_hint("Hover", kbd, cx));
        }
        if let Some(kbd) =
            Kbd::binding_for_action_in(&TriggerParameterHints, &self.focus_handle, window)
        {
            hints.push(self.render_toolbar_shortcut_hint("Params", kbd, cx));
        }
        if let Some(kbd) = Kbd::binding_for_action_in(&ShowCodeActions, &self.focus_handle, window)
        {
            hints.push(self.render_toolbar_shortcut_hint("Actions", kbd, cx));
        }

        h_flex().gap_3().items_center().children(hints)
    }

    fn render_toolbar_shortcut_hint(
        &self,
        label: impl Into<SharedString>,
        kbd: Kbd,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let label = label.into();
        h_flex()
            .gap_1()
            .items_center()
            .child(
                div()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground)
                    .child(label),
            )
            .child(kbd)
            .into_any_element()
    }

    fn render_problem_shortcut_hints(&self, window: &Window, cx: &mut Context<Self>) -> AnyElement {
        let mut hints: Vec<AnyElement> = Vec::new();

        if let Some(kbd) = Kbd::binding_for_action_in(&NextProblem, &self.focus_handle, window) {
            hints.push(self.render_toolbar_shortcut_hint("Next", kbd, cx));
        }
        if let Some(kbd) = Kbd::binding_for_action_in(&PreviousProblem, &self.focus_handle, window)
        {
            hints.push(self.render_toolbar_shortcut_hint("Prev", kbd, cx));
        }
        if let Some(kbd) = Kbd::binding_for_action_in(
            &super::actions::ToggleProblemsPanel,
            &self.focus_handle,
            window,
        ) {
            hints.push(self.render_toolbar_shortcut_hint("Panel", kbd, cx));
        }

        h_flex()
            .gap_2()
            .items_center()
            .children(hints)
            .into_any_element()
    }

    fn kbd_for_keystroke(stroke: &str) -> Option<Kbd> {
        Keystroke::parse(stroke).ok().map(Kbd::new)
    }

    fn formatted_keystroke(stroke: &str) -> Option<String> {
        Keystroke::parse(stroke).ok().map(|key| Kbd::format(&key))
    }

    fn run_query_tooltip_text() -> SharedString {
        #[cfg(target_os = "macos")]
        {
            Self::formatted_keystroke("cmd-enter")
                .map(|shortcut| format!("Run Query ({shortcut})"))
                .unwrap_or_else(|| "Run Query".into())
                .into()
        }

        #[cfg(not(target_os = "macos"))]
        {
            match (
                Self::formatted_keystroke("ctrl-enter"),
                Self::formatted_keystroke("f5"),
            ) {
                (Some(primary), Some(secondary)) => {
                    format!("Run Query ({primary} / {secondary})").into()
                }
                (Some(primary), None) => format!("Run Query ({primary})").into(),
                (None, Some(secondary)) => format!("Run Query ({secondary})").into(),
                (None, None) => "Run Query".into(),
            }
        }
    }

    /// Render the SQL editor area using the custom TextEditor
    fn render_editor(&self, _cx: &mut Context<Self>) -> impl IntoElement {
        self.editor.clone().into_any_element()
    }

    /// Parse and format hover content with markdown-like styling
    /// Supports: headings (#), code blocks (```), inline code (`), bold (**)
    fn format_hover_content(&self, content: &str, cx: &mut Context<Self>) -> gpui::AnyElement {
        let theme = cx.theme().clone();

        if content.contains("```") {
            self.render_markdown_with_code_blocks(content, &theme)
        } else {
            self.render_simple_markdown(content, &theme)
        }
    }

    fn render_markdown_with_code_blocks(&self, content: &str, theme: &Theme) -> gpui::AnyElement {
        let mut elements: Vec<gpui::AnyElement> = Vec::new();
        let mut in_code_block = false;
        let mut code_block_lines: Vec<&str> = Vec::new();

        for line in content.lines() {
            if line.starts_with("```") {
                if !in_code_block {
                    in_code_block = true;
                    code_block_lines.clear();
                } else {
                    let code_text = code_block_lines.join("\n");
                    elements.push(
                        div()
                            .w_full()
                            .bg(theme.muted)
                            .rounded_sm()
                            .p_2()
                            .mb_2()
                            .font_family(theme.mono_font_family.clone())
                            .text_xs()
                            .text_color(theme.foreground)
                            .overflow_x_scrollbar()
                            .child(code_text)
                            .into_any_element(),
                    );
                    in_code_block = false;
                    code_block_lines.clear();
                }
            } else if in_code_block {
                code_block_lines.push(line);
            } else {
                let styled = self.render_markdown_line(line, theme);
                elements.push(div().w_full().mb_1().child(styled).into_any_element());
            }
        }

        if in_code_block && !code_block_lines.is_empty() {
            let code_text = code_block_lines.join("\n");
            elements.push(
                div()
                    .w_full()
                    .bg(theme.muted)
                    .rounded_sm()
                    .p_2()
                    .mb_2()
                    .font_family(theme.mono_font_family.clone())
                    .text_xs()
                    .text_color(theme.foreground)
                    .overflow_x_scrollbar()
                    .child(code_text)
                    .into_any_element(),
            );
        }

        if elements.is_empty() {
            div().child(content.to_string()).into_any_element()
        } else {
            div()
                .children(
                    elements
                        .into_iter()
                        .map(gpui::IntoElement::into_any_element),
                )
                .into_any_element()
        }
    }

    fn render_markdown_line(&self, line: &str, theme: &Theme) -> gpui::AnyElement {
        if line.starts_with("### ") {
            return div()
                .text_sm()
                .font_weight(gpui::FontWeight::from(700.0))
                .text_color(theme.foreground)
                .mb_1()
                .child(line.trim_start_matches("### ").to_string())
                .into_any_element();
        }
        if line.starts_with("## ") {
            return div()
                .text_base()
                .font_weight(gpui::FontWeight::from(700.0))
                .text_color(theme.foreground)
                .mb_1()
                .child(line.trim_start_matches("## ").to_string())
                .into_any_element();
        }
        if line.starts_with("# ") {
            return div()
                .text_base()
                .font_weight(gpui::FontWeight::from(700.0))
                .text_color(theme.foreground)
                .mb_2()
                .child(line.trim_start_matches("# ").to_string())
                .into_any_element();
        }

        if line.contains('`') {
            return self.render_inline_code(line, theme);
        }

        if line.contains("**") {
            return self.render_bold_text(line, theme);
        }

        div()
            .text_xs()
            .text_color(theme.popover_foreground)
            .child(line.to_string())
            .into_any_element()
    }

    fn render_inline_code(&self, line: &str, theme: &Theme) -> gpui::AnyElement {
        let mut parts: Vec<gpui::AnyElement> = Vec::new();
        let mut remaining = line;

        while let Some(start) = remaining.find('`') {
            if start > 0 {
                parts.push(
                    div()
                        .text_xs()
                        .text_color(theme.popover_foreground)
                        .child(remaining[..start].to_string())
                        .into_any_element(),
                );
            }

            if let Some(end) = remaining[start + 1..].find('`') {
                let code = &remaining[start + 1..start + 1 + end];
                parts.push(
                    div()
                        .bg(theme.muted)
                        .rounded_sm()
                        .px_1()
                        .font_family(theme.mono_font_family.clone())
                        .text_xs()
                        .text_color(theme.accent)
                        .child(code.to_string())
                        .into_any_element(),
                );
                remaining = &remaining[start + 1 + end + 1..];
            } else {
                parts.push(
                    div()
                        .text_xs()
                        .text_color(theme.popover_foreground)
                        .child(remaining.to_string())
                        .into_any_element(),
                );
                break;
            }
        }

        if !remaining.is_empty() {
            parts.push(
                div()
                    .text_xs()
                    .text_color(theme.popover_foreground)
                    .child(remaining.to_string())
                    .into_any_element(),
            );
        }

        div()
            .children(parts.into_iter().map(gpui::IntoElement::into_any_element))
            .into_any_element()
    }

    fn render_bold_text(&self, line: &str, theme: &Theme) -> gpui::AnyElement {
        let mut parts: Vec<gpui::AnyElement> = Vec::new();
        let mut remaining = line;

        while let Some(start) = remaining.find("**") {
            if start > 0 {
                parts.push(
                    div()
                        .text_xs()
                        .text_color(theme.popover_foreground)
                        .child(remaining[..start].to_string())
                        .into_any_element(),
                );
            }

            if let Some(end) = remaining[start + 2..].find("**") {
                let bold = &remaining[start + 2..start + 2 + end];
                parts.push(
                    div()
                        .text_xs()
                        .font_weight(gpui::FontWeight::from(700.0))
                        .text_color(theme.foreground)
                        .child(bold.to_string())
                        .into_any_element(),
                );
                remaining = &remaining[start + 2 + end + 2..];
            } else {
                parts.push(
                    div()
                        .text_xs()
                        .text_color(theme.popover_foreground)
                        .child(remaining.to_string())
                        .into_any_element(),
                );
                break;
            }
        }

        if !remaining.is_empty() {
            parts.push(
                div()
                    .text_xs()
                    .text_color(theme.popover_foreground)
                    .child(remaining.to_string())
                    .into_any_element(),
            );
        }

        div()
            .children(parts.into_iter().map(gpui::IntoElement::into_any_element))
            .into_any_element()
    }

    fn render_simple_markdown(&self, content: &str, theme: &Theme) -> gpui::AnyElement {
        let mut elements: Vec<gpui::AnyElement> = Vec::new();

        for line in content.lines() {
            let styled = self.render_markdown_line(line, theme);
            elements.push(div().w_full().mb_1().child(styled).into_any_element());
        }

        if elements.is_empty() {
            div().child(content.to_string()).into_any_element()
        } else {
            div()
                .children(
                    elements
                        .into_iter()
                        .map(gpui::IntoElement::into_any_element),
                )
                .into_any_element()
        }
    }

    /// Render the hover popover if there's content to show
    fn render_hover_popover(&self, cx: &mut Context<Self>) -> Option<impl IntoElement> {
        let content = self.hover_content.as_ref()?;
        let theme = cx.theme();

        Some(
            div()
                .absolute()
                .top_8()
                .left_4()
                .max_w(px(500.0))
                .max_h(px(400.0))
                .overflow_y_scrollbar()
                .overflow_x_hidden()
                .bg(theme.popover)
                .border_1()
                .border_color(theme.info)
                .rounded_md()
                .shadow_lg()
                .p_3()
                .child(self.format_hover_content(content, cx))
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.clear_hover(cx);
                })),
        )
    }

    /// Render the template params panel (JSON editor) - shown only in template mode
    fn render_template_params(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let settings = ZqlzSettings::global(cx);

        v_flex()
            .w(px(280.0))
            .h_full()
            .border_l_1()
            .border_color(theme.border)
            .bg(theme.background)
            .child(
                h_flex()
                    .w_full()
                    .h(px(28.0))
                    .px_2()
                    .items_center()
                    .border_b_1()
                    .border_color(theme.border)
                    .bg(theme.muted)
                    .child(
                        div()
                            .text_xs()
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .text_color(theme.muted_foreground)
                            .child("Template Parameters (JSON)"),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .child(div().h_full().w_full().child(self.template_params.clone())),
            )
            .when(self.rendered_sql.is_some(), |this| {
                this.child(
                    v_flex()
                        .w_full()
                        .max_h(px(150.0))
                        .border_t_1()
                        .border_color(theme.border)
                        .child(
                            h_flex()
                                .w_full()
                                .h(px(24.0))
                                .px_2()
                                .items_center()
                                .bg(theme.muted)
                                .child(
                                    div()
                                        .text_xs()
                                        .font_weight(gpui::FontWeight::MEDIUM)
                                        .text_color(theme.muted_foreground)
                                        .child("Rendered SQL Preview"),
                                ),
                        )
                        .child(
                            div()
                                .flex_1()
                                .w_full()
                                .p_2()
                                .overflow_y_scrollbar()
                                .text_xs()
                                .font_family(settings.fonts.editor_font_family.clone())
                                .text_color(theme.foreground)
                                .child(self.rendered_sql.clone().unwrap_or_default()),
                        ),
                )
            })
    }
}

impl Render for QueryEditor {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let bg_color = cx.theme().background;
        let indicator_bg = cx.theme().primary.opacity(0.1);

        let is_template_mode = self.editor_mode == EditorMode::Template;
        let hover_content = self.hover_content.clone();

        let inline_suggestion_text = self
            .inline_suggestion
            .as_ref()
            .map(|s| s.suggestion.clone());
        let muted_foreground = cx.theme().muted_foreground;

        v_flex()
            .id("query-editor")
            // Track the QueryEditor's own handle, not the TextEditor's. Focus is
            // forwarded to the inner editor on click (see on_click below).
            .track_focus(&self.focus_handle)
            .key_context("Editor")
            .size_full()
            .bg(bg_color)
            .on_action(cx.listener(Self::handle_trigger_completion))
            .on_action(cx.listener(Self::handle_accept_completion))
            .on_action(cx.listener(Self::handle_cancel_completion))
            .on_action(cx.listener(Self::handle_next_completion))
            .on_action(cx.listener(Self::handle_previous_completion))
            .on_action(cx.listener(Self::handle_next_problem))
            .on_action(cx.listener(Self::handle_previous_problem))
            .on_action(cx.listener(Self::handle_confirm_completion))
            .on_action(cx.listener(Self::handle_cancel_completion_menu))
            .on_key_down(cx.listener(|this, event: &gpui::KeyDownEvent, window, cx| {
                let key = event.keystroke.key.as_str();
                let modifiers = event.keystroke.modifiers;

                let is_manual_completion_shortcut = key == "."
                    && !modifiers.shift
                    && !modifiers.alt
                    && (modifiers.platform || modifiers.control);

                if is_manual_completion_shortcut && !this.is_executing {
                    this.dismiss_inline_suggestion(cx);
                    this.handle_trigger_completion(&TriggerCompletion, window, cx);
                    window.prevent_default();
                    cx.stop_propagation();
                    return;
                }

                // Query execution shortcuts:
                // - Cmd+Enter and F5 execute the current query
                // - Cmd/Ctrl+Shift+Enter executes selection/current statement
                // - Escape / Cmd+Escape cancels the running query via app keymap
                if (this.is_executing && key == "escape" && !modifiers.shift && !modifiers.alt)
                    || (this.is_executing
                        && key == "escape"
                        && modifiers.platform
                        && !modifiers.shift
                        && !modifiers.alt)
                {
                    this.emit_cancel_query(cx);
                    window.prevent_default();
                    cx.stop_propagation();
                    return;
                }
                if modifiers.shift && key == "enter" && (modifiers.platform || modifiers.control) {
                    this.emit_execute_selection(cx);
                    return;
                }
                if key == "f5"
                    || (modifiers.platform && key == "enter")
                    || (modifiers.control && key == "r")
                {
                    this.emit_execute_query(cx);
                    return;
                }

                // Any typed character invalidates the current ghost-text suggestion.
                if key.len() == 1
                    && let Some(ch) = key.chars().next()
                    && !ch.is_control()
                {
                    this.dismiss_inline_suggestion(cx);
                }

                // Allow the event to continue propagating so the inner TextEditor
                // also receives it and can insert the typed character.
                cx.propagate();
            }))
            .child(self.render_toolbar(_window, cx))
            .child(
                h_flex()
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .child(
                        div()
                            .flex_1()
                            .h_full()
                            .overflow_hidden()
                            .relative()
                            .child(self.render_editor(cx))
                            .when_some(hover_content, |this, _content| {
                                this.children(self.render_hover_popover(cx))
                            })
                            .when_some(inline_suggestion_text, |this, suggestion_text| {
                                this.child(
                                    div()
                                        .absolute()
                                        .bottom_2()
                                        .right_2()
                                        .px_3()
                                        .py_1()
                                        .rounded_md()
                                        .bg(indicator_bg)
                                        .text_sm()
                                        .font_medium()
                                        .child(
                                            h_flex()
                                                .gap_1()
                                                .items_center()
                                                .child("⟪")
                                                .child(suggestion_text)
                                                .child(
                                                    div()
                                                        .text_xs()
                                                        .text_color(muted_foreground)
                                                        .child("accept"),
                                                )
                                                .when_some(
                                                    Self::kbd_for_keystroke("tab"),
                                                    |this, kbd| this.child(kbd),
                                                )
                                                .child(
                                                    div()
                                                        .text_xs()
                                                        .text_color(muted_foreground)
                                                        .child("dismiss"),
                                                )
                                                .when_some(
                                                    Self::kbd_for_keystroke("escape"),
                                                    |this, kbd| this.child(kbd),
                                                ),
                                        ),
                                )
                            }),
                    )
                    .when(is_template_mode, |this| {
                        this.child(self.render_template_params(cx))
                    }),
            )
    }
}

impl Focusable for QueryEditor {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for QueryEditor {}
impl EventEmitter<QueryEditorEvent> for QueryEditor {}

impl Panel for QueryEditor {
    fn panel_name(&self) -> &'static str {
        "QueryEditor"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if self.editor.read(_cx).is_dirty() {
            format!("{}*", self.name)
        } else {
            self.name.clone()
        }
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }

    fn has_unsaved_changes(&self, _cx: &App) -> bool {
        self.editor.read(_cx).is_dirty()
    }
}

/// Returns the path where the schema cache for a given connection is persisted on disk.
///
/// Layout: `{data_local_dir}/zqlz/schema_cache/{connection_id}.json`
/// This is intentionally per-connection so different databases never share cached schemas.
fn schema_cache_path(connection_id: Uuid, scope: Option<&str>) -> Option<std::path::PathBuf> {
    let scope = scope
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("__default_scope__");
    let scope_hash = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        scope.hash(&mut hasher);
        hasher.finish()
    };

    dirs::data_local_dir().map(|base| {
        base.join("zqlz")
            .join("schema_cache")
            .join(format!("{connection_id}_{scope_hash:016x}.json"))
    })
}

/// Attempts to read and deserialize a previously saved schema cache from disk.
/// Returns `None` on any failure (missing file, corrupt JSON, etc.) without logging noise —
/// a missing or invalid cache is a normal condition on first run or after a schema update.
fn load_schema_cache_from_disk(
    connection_id: Uuid,
    scope: Option<&str>,
) -> Option<zqlz_lsp::SchemaCache> {
    let path = schema_cache_path(connection_id, scope)?;
    let json = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&json).ok()
}

/// Serializes the schema cache to disk, creating parent directories as needed.
/// Errors are logged but not propagated — a failed disk write is non-fatal.
fn save_schema_cache_to_disk(
    connection_id: Uuid,
    scope: Option<&str>,
    cache: &zqlz_lsp::SchemaCache,
) {
    let Some(path) = schema_cache_path(connection_id, scope) else {
        return;
    };
    if let Some(parent) = path.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        tracing::warn!(error = %e, "Failed to create schema cache directory");
        return;
    }
    match serde_json::to_string(cache) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                tracing::warn!(error = %e, "Failed to write schema cache to disk");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to serialize schema cache");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DiagnosticInfo, DiagnosticInfoSeverity, QueryEditor};
    use lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};
    use std::path::Path;
    use zqlz_text_editor::DocumentIdentity;

    #[test]
    fn problem_index_forward_wraps_to_start() {
        let problems = vec![(2, 0), (4, 5), (10, 1)];
        assert_eq!(
            QueryEditor::problem_index_for_cursor(&problems, 10, 2, true),
            Some(0)
        );
    }

    #[test]
    fn problem_index_forward_picks_next() {
        let problems = vec![(2, 0), (4, 5), (10, 1)];
        assert_eq!(
            QueryEditor::problem_index_for_cursor(&problems, 2, 0, true),
            Some(1)
        );
    }

    #[test]
    fn problem_index_backward_wraps_to_end() {
        let problems = vec![(2, 0), (4, 5), (10, 1)];
        assert_eq!(
            QueryEditor::problem_index_for_cursor(&problems, 1, 0, false),
            Some(2)
        );
    }

    #[test]
    fn problem_index_backward_picks_previous() {
        let problems = vec![(2, 0), (4, 5), (10, 1)];
        assert_eq!(
            QueryEditor::problem_index_for_cursor(&problems, 8, 0, false),
            Some(1)
        );
    }

    #[test]
    fn internal_text_document_starts_with_internal_identity() {
        let document = QueryEditor::internal_text_document("select 1");

        assert_eq!(document.buffer.text(), "select 1");
        match document.identity() {
            DocumentIdentity::Internal { uri } => {
                assert!(uri.as_str().starts_with("sql://internal/"));
            }
            identity => panic!("expected internal identity, got {identity:?}"),
        }
    }

    #[test]
    fn document_identity_from_path_can_be_used_for_document_first_construction() {
        let identity = DocumentIdentity::from_path("/tmp/query.sql").expect("external identity");

        assert_eq!(identity.path(), Some(Path::new("/tmp/query.sql")));
    }

    #[test]
    fn cached_diagnostic_counts_match_lsp_severities() {
        let diagnostics = vec![
            Diagnostic {
                range: Range::new(Position::new(0, 0), Position::new(0, 6)),
                severity: Some(DiagnosticSeverity::ERROR),
                ..Diagnostic::default()
            },
            Diagnostic {
                range: Range::new(Position::new(1, 0), Position::new(1, 4)),
                severity: Some(DiagnosticSeverity::WARNING),
                ..Diagnostic::default()
            },
            Diagnostic {
                range: Range::new(Position::new(2, 0), Position::new(2, 3)),
                severity: Some(DiagnosticSeverity::INFORMATION),
                ..Diagnostic::default()
            },
            Diagnostic {
                range: Range::new(Position::new(3, 0), Position::new(3, 2)),
                severity: Some(DiagnosticSeverity::HINT),
                ..Diagnostic::default()
            },
        ];

        assert_eq!(
            QueryEditor::diagnostic_counts_from_lsp_diagnostics(&diagnostics),
            (1, 1, 2)
        );
    }

    #[test]
    fn cached_diagnostic_infos_preserve_ranges_and_messages() {
        let diagnostics = vec![Diagnostic {
            range: Range::new(Position::new(4, 2), Position::new(4, 8)),
            severity: Some(DiagnosticSeverity::WARNING),
            message: "check predicate".to_string(),
            source: Some("sqlparser".to_string()),
            ..Diagnostic::default()
        }];

        assert_eq!(
            QueryEditor::diagnostic_infos_from_lsp_diagnostics(&diagnostics),
            vec![DiagnosticInfo {
                line: 4,
                column: 2,
                end_line: 4,
                end_column: 8,
                severity: DiagnosticInfoSeverity::Warning,
                message: "check predicate".to_string(),
                source: Some("sqlparser".to_string()),
            }]
        );
    }
}
