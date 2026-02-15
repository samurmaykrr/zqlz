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

use gpui::prelude::FluentBuilder;
use gpui::*;
use lsp_types::DiagnosticSeverity;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::Connection;
use zqlz_lsp::SqlLsp;
use crate::ai_completion::{
    AiProviderFactory, CompletionRequest, CompletionResponse,
};
use zqlz_services::SchemaService;
use zqlz_settings::{InlineSuggestionProvider, ZqlzSettings};
use zqlz_templates::TemplateEngine;
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, RopeExt, Sizable, StyledExt, Theme, ZqlzIcon,
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    menu::DropdownMenu,
    scroll::ScrollableElement,
    v_flex,
};
use zqlz_zed_adapter::{editor_wrapper::EditorWrapper, Confirm, SelectDown, SelectUp};

use super::actions::{AcceptCompletion, AcceptInlineSuggestion, CancelCompletion, CancelCompletionMenu, ConfirmCompletion, DismissInlineSuggestion, FindReferences, FormatQuery, GoToDefinition, NextCompletion, PreviousCompletion, RenameSymbol, SaveQuery, ShowCodeActions, ShowHover, TriggerCompletion, TriggerParameterHints};
use super::zed_input::{ZedInput, ZedInputEvent, ZedInputState};

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
        .map(|driver| zqlz_core::dialects::get_highlight_language(driver))
        .unwrap_or("sql")
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

/// A reference location found by FindReferences
#[derive(Clone, Debug)]
pub struct ReferenceItem {
    /// The URI/location of the reference
    pub uri: String,
    /// Line number (0-indexed, displayed as 1-indexed)
    pub line: u32,
    /// Column number (0-indexed, displayed as 1-indexed)
    pub column: u32,
    /// The text of the reference (for display)
    pub text: Option<String>,
}

/// A code action available at the current cursor position
#[derive(Clone, Debug)]
pub struct CodeActionItem {
    /// The title/description of the action
    pub title: String,
    /// The kind of code action (e.g., QUICKFIX, REFACTOR)
    pub kind: Option<String>,
    /// Index into the code actions list for applying the action
    pub index: usize,
}

/// State for the rename UI
#[derive(Clone, Debug)]
pub struct RenameState {
    /// The original identifier being renamed
    pub original_name: String,
    /// The current input value for the new name
    pub input_value: String,
    /// The cursor offset where rename was triggered
    pub trigger_offset: usize,
    /// Error message if rename validation failed
    pub error_message: Option<String>,
}

/// Information about a diagnostic for external display (e.g., Problems panel)
#[derive(Clone, Debug)]
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

    /// The Zed editor wrapper for SQL code editing
    editor: Entity<EditorWrapper>,

    /// SQL LSP instance for IntelliSense
    sql_lsp: Arc<RwLock<SqlLsp>>,

    /// Whether the content has unsaved changes
    is_dirty: bool,

    /// Whether a query is currently executing
    is_executing: bool,

    /// Current editor mode (SQL or Template)
    editor_mode: EditorMode,

    /// The type of database object being edited (Query, View, Procedure, etc.)
    object_type: EditorObjectType,

    /// Template engine for MiniJinja rendering
    template_engine: TemplateEngine,

    /// Template parameters as JSON string (using ZedInput for code editing)
    template_params: Entity<ZedInputState>,

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

    /// Current signature help content (shown when TriggerParameterHints action is triggered)
    signature_help_content: Option<String>,

    /// Current definition content (shown when GoToDefinition action is triggered)
    definition_content: Option<String>,

    /// Current references list (shown when FindReferences action is triggered)
    references_content: Option<Vec<ReferenceItem>>,

    /// Current rename state (shown when RenameSymbol action is triggered)
    rename_content: Option<RenameState>,

    /// Current code actions list (shown when ShowCodeActions action is triggered)
    code_actions_content: Option<Vec<CodeActionItem>>,

    /// Current inline suggestion (ghost text shown while typing)
    inline_suggestion: Option<InlineSuggestionState>,

    /// Debounce timer for inline suggestions
    _inline_suggestion_debounce: Option<gpui::Task<()>>,

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

        // Get editor settings from global settings
        let editor_settings = &ZqlzSettings::global(cx).editor;
        let _show_line_numbers = editor_settings.show_line_numbers;
        let _word_wrap = editor_settings.word_wrap;
        let _show_inline_diagnostics = editor_settings.show_inline_diagnostics;

        tracing::debug!("Creating EditorWrapper for Zed editor");
        let editor = cx.new(|cx| {
            EditorWrapper::new(window, cx)
        });

        editor.update(cx, |wrapper, cx| {
            wrapper.set_sql_lsp(sql_lsp.clone());
            tracing::info!("QueryEditor: set_sql_lsp called on EditorWrapper");
            let _ = cx;
        });

        // Create template parameters input (JSON editor using ZedInput)
        let template_params = cx.new(|cx| {
            ZedInputState::new(window, cx)
                .with_value("{\n  \n}")
                .with_placeholder("{\n  \"variable\": \"value\"\n}")
        });

        // TODO: Subscribe to editor events in Phase 3 task-3.3
        // For now, we'll track changes through direct editor usage
        let _subscriptions = vec![
            // Editor event subscription will be added here
        ];

        Self {
            name,
            connection_id,
            connection_name: None,
            saved_query_id: None,
            editor,
            sql_lsp,
            is_dirty: false,
            is_executing: false,
            editor_mode: EditorMode::Sql,
            object_type: EditorObjectType::Query,
            template_engine: TemplateEngine::new(),
            template_params,
            rendered_sql: None,
            template_error: None,
            current_database: None,
            available_connections: Vec::new(),
            available_databases: Vec::new(),
            hover_content: None,
            signature_help_content: None,
            definition_content: None,
            references_content: None,
            rename_content: None,
            code_actions_content: None,
            inline_suggestion: None,
            _inline_suggestion_debounce: None,
            _completion_debounce: None,
            _subscriptions,
        }
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
        let editor_settings = &ZqlzSettings::global(cx).editor;
        let _show_line_numbers = editor_settings.show_line_numbers;
        let _word_wrap = editor_settings.word_wrap;
        let _show_inline_diagnostics = editor_settings.show_inline_diagnostics;

        let editor = cx.new(|cx| {
            EditorWrapper::new(window, cx)
        });

        editor.update(cx, |wrapper, cx| {
            wrapper.set_sql_lsp(sql_lsp.clone());
            let _ = cx;
        });

        if let Some(content) = initial_content {
            editor.update(cx, |editor, cx| {
                editor.set_text(content, window, cx);
            });
        }

        let template_params = cx.new(|cx| {
            ZedInputState::new(window, cx)
                .with_value("{\n  \n}")
                .with_placeholder("{\n  \"variable\": \"value\"\n}")
        });

        // TODO: Subscribe to editor events in Phase 3 task-3.3
        let _subscriptions = vec![];

        Self {
            name,
            connection_id: Some(connection_id),
            connection_name: None,
            saved_query_id: None,
            editor,
            sql_lsp,
            is_dirty: false,
            is_executing: false,
            editor_mode: EditorMode::Sql,
            object_type,
            template_engine: TemplateEngine::new(),
            template_params,
            rendered_sql: None,
            template_error: None,
            current_database: None,
            available_connections: Vec::new(),
            available_databases: Vec::new(),
            hover_content: None,
            signature_help_content: None,
            definition_content: None,
            references_content: None,
            rename_content: None,
            code_actions_content: None,
            inline_suggestion: None,
            _inline_suggestion_debounce: None,
            _completion_debounce: None,
            _subscriptions,
        }
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

        // TODO: Syntax highlighter will be configured via Zed's language system in Phase 5
        let _dialect_language = driver_type_to_highlight_language(driver_type.as_deref());

        // Update SQL LSP with new connection and driver type
        {
            let mut lsp = self.sql_lsp.write();
            lsp.set_connection(connection_id, connection.clone(), driver_type.clone());
        }

        // Refresh schema in background, then re-validate diagnostics once loaded
        if let Some(_conn) = connection {
            tracing::debug!("Starting schema refresh in background");
            let lsp = self.sql_lsp.clone();
            cx.spawn(async move |this, cx| {
                {
                    let mut lsp = lsp.write();
                    if let Err(e) = lsp.refresh_schema().await {
                        tracing::error!(error = %e, "Failed to refresh SQL schema");
                        return;
                    }
                    tracing::debug!("SQL schema refreshed successfully");
                }
                // RwLock is dropped here before calling back into the editor,
                // since update_diagnostics also acquires the sql_lsp lock.
                _ = this.update(cx, |editor, cx| {
                    editor.update_diagnostics(cx);
                });
            })
            .detach();
        } else {
            tracing::debug!("No connection provided, skipping schema refresh");
        }

        cx.notify();
    }

    /// Set the list of available connections for the connection switcher
    pub fn set_available_connections(&mut self, connections: Vec<(Uuid, String)>, cx: &mut Context<Self>) {
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
        cx.notify();
    }


    /// Set the SQL content
    pub fn set_content(&mut self, content: String, window: &mut Window, cx: &mut Context<Self>) {
        self.editor.update(cx, |editor, cx| {
            editor.set_text(content, window, cx);
        });
        self.is_dirty = true;
        self.update_diagnostics(cx);
        cx.notify();
    }

    /// Update SQL diagnostics based on current content
    fn update_diagnostics(&mut self, cx: &mut Context<Self>) {
        // Get current editor text as a Rope for LSP analysis
        let text_content = self.editor.read(cx).get_text(cx);
        let rope = zqlz_ui::widgets::Rope::from(text_content.as_str());
        
        // Run LSP validation to get diagnostics
        let lsp_diagnostics = {
            let mut lsp = self.sql_lsp.write();
            lsp.validate_sql(&rope)
        };
        
        // Convert LSP diagnostics (lsp_types::Diagnostic) to Zed format
        let zed_diagnostics: Vec<zqlz_zed_adapter::editor_wrapper::Diagnostic> = lsp_diagnostics
            .iter()
            .filter_map(|lsp_diag| {
                // Convert LSP Range (line/col) to byte offsets using rope
                let start_offset = rope.position_to_offset(&lsp_diag.range.start);
                let end_offset = rope.position_to_offset(&lsp_diag.range.end);
                
                // Convert LSP severity to Zed severity
                let severity = match lsp_diag.severity {
                    Some(DiagnosticSeverity::ERROR) => {
                        zqlz_zed_adapter::editor_wrapper::DiagnosticLevel::Error
                    }
                    Some(DiagnosticSeverity::WARNING) => {
                        zqlz_zed_adapter::editor_wrapper::DiagnosticLevel::Warning
                    }
                    Some(DiagnosticSeverity::INFORMATION) => {
                        zqlz_zed_adapter::editor_wrapper::DiagnosticLevel::Info
                    }
                    Some(DiagnosticSeverity::HINT) => {
                        zqlz_zed_adapter::editor_wrapper::DiagnosticLevel::Hint
                    }
                    _ => zqlz_zed_adapter::editor_wrapper::DiagnosticLevel::Error,
                };
                
                Some(zqlz_zed_adapter::editor_wrapper::Diagnostic {
                    range: start_offset..end_offset,
                    severity,
                    message: lsp_diag.message.clone(),
                })
            })
            .collect();
        
        // Update editor diagnostics
        self.editor.update(cx, |editor, cx| {
            editor.set_diagnostics(zed_diagnostics, cx);
        });
        
        // Emit diagnostics changed event
        let diagnostic_infos = self.collect_diagnostic_infos(cx);
        cx.emit(QueryEditorEvent::DiagnosticsChanged {
            diagnostics: diagnostic_infos,
        });
        
        cx.notify();
    }

    /// Collect diagnostics as DiagnosticInfo for external consumers
    fn collect_diagnostic_infos(&self, cx: &App) -> Vec<DiagnosticInfo> {
        let text_content = self.editor.read(cx).get_text(cx);
        let rope = zqlz_ui::widgets::Rope::from(text_content.as_str());
        
        // Get diagnostics from LSP
        let lsp_diagnostics = {
            let mut lsp = self.sql_lsp.write();
            lsp.validate_sql(&rope)
        };
        
        // Convert to DiagnosticInfo
        lsp_diagnostics
            .into_iter()
            .map(|lsp_diag| {
                let severity = match lsp_diag.severity {
                    Some(DiagnosticSeverity::ERROR) => DiagnosticInfoSeverity::Error,
                    Some(DiagnosticSeverity::WARNING) => DiagnosticInfoSeverity::Warning,
                    Some(DiagnosticSeverity::INFORMATION) => DiagnosticInfoSeverity::Info,
                    Some(DiagnosticSeverity::HINT) => DiagnosticInfoSeverity::Hint,
                    _ => DiagnosticInfoSeverity::Error,
                };
                
                DiagnosticInfo {
                    line: lsp_diag.range.start.line as usize,
                    column: lsp_diag.range.start.character as usize,
                    end_line: lsp_diag.range.end.line as usize,
                    end_column: lsp_diag.range.end.character as usize,
                    severity,
                    message: lsp_diag.message.clone(),
                    source: lsp_diag.source.clone(),
                }
            })
            .collect()
    }

    /// Get the current SQL content
    pub fn content(&self, cx: &App) -> SharedString {
        self.editor.read(cx).get_text(cx).into()
    }

    /// Get diagnostic counts from the current editor state
    ///
    /// Returns (errors, warnings, hints/infos)
    pub fn diagnostic_counts(&self, cx: &App) -> (usize, usize, usize) {
        let text_content = self.editor.read(cx).get_text(cx);
        let rope = zqlz_ui::widgets::Rope::from(text_content.as_str());
        
        let lsp_diagnostics = {
            let mut lsp = self.sql_lsp.write();
            lsp.validate_sql(&rope)
        };
        
        let mut errors = 0;
        let mut warnings = 0;
        let mut hints_infos = 0;
        
        for diag in lsp_diagnostics {
            match diag.severity {
                Some(DiagnosticSeverity::ERROR) => errors += 1,
                Some(DiagnosticSeverity::WARNING) => warnings += 1,
                Some(DiagnosticSeverity::INFORMATION) | 
                Some(DiagnosticSeverity::HINT) => hints_infos += 1,
                _ => errors += 1,
            }
        }
        
        (errors, warnings, hints_infos)
    }

    /// Get all diagnostics as a list for external display (e.g., Problems panel)
    pub fn get_diagnostics(&self, cx: &App) -> Vec<zqlz_ui::widgets::highlighter::Diagnostic> {
        let text_content = self.editor.read(cx).get_text(cx);
        let rope = zqlz_ui::widgets::Rope::from(text_content.as_str());
        
        let lsp_diagnostics = {
            let mut lsp = self.sql_lsp.write();
            lsp.validate_sql(&rope)
        };
        
        lsp_diagnostics
            .into_iter()
            .map(|lsp_diag| {
                // Use the From trait to convert lsp_types::Diagnostic
                zqlz_ui::widgets::highlighter::Diagnostic::from(lsp_diag)
            })
            .collect()
    }

    /// Navigate to a specific line and column (0-indexed)
    pub fn go_to_line(
        &mut self,
        _line: usize,
        _column: usize,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
        // TODO: Navigation will be implemented using Zed's editor API in Phase 3 task-3.3
    }

    /// Set the SQL content
    pub fn set_text(&mut self, sql: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.editor
            .update(cx, |editor, cx| editor.set_text(sql.to_string(), window, cx));
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

        // TODO: Update placeholder via Zed editor API
        let _placeholder = match self.editor_mode {
            EditorMode::Sql => "Write your SQL query here...",
            EditorMode::Template => {
                "Write your MiniJinja template here...\n\nExample:\nSELECT * FROM {{ table }}\nWHERE id IN {{ ids|inclause }}"
            }
        };
        // Placeholder setting will be implemented with full editor integration

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
        let params_json = self.template_params.read(cx).value().to_string();

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

    /// Get selected text, or entire content if nothing is selected
    pub fn selected_or_all_content(&self, cx: &App) -> String {
        // Get selected text from Zed editor, or fall back to all content if no selection
        self.editor
            .read(cx)
            .get_selected_text(cx)
            .unwrap_or_else(|| self.content(cx).to_string())
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
        let end_col_opt = if end_column > 0 { Some(end_column) } else { None };
        
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
        let sql = self.selected_or_all_content(cx);
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
        let sql = self.selected_or_all_content(cx);
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
        self.is_dirty = false;
        cx.notify();
    }

    /// Get the saved query ID if this editor is editing a saved query
    pub fn saved_query_id(&self) -> Option<Uuid> {
        self.saved_query_id
    }

    /// Set the saved query ID (used when a query is saved)
    pub fn set_saved_query_id(&mut self, id: Option<Uuid>, cx: &mut Context<Self>) {
        self.saved_query_id = id;
        cx.notify();
    }

    /// Set the editor name/title
    pub fn set_name(&mut self, name: &str, cx: &mut Context<Self>) {
        self.name = name.to_string();
        cx.notify();
    }

    /// Trigger auto-completion with debouncing
    ///
    /// This is called when the user types alphanumeric characters.
    /// After a short delay, completions are automatically shown.
    fn trigger_auto_completion(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_completions_enabled {
            return;
        }

        let editor = self.editor.clone();
        self._completion_debounce = Some(cx.spawn_in(window, async move |this, cx| {
            cx.background_executor().timer(std::time::Duration::from_millis(150)).await;
            
            _ = this.update_in(cx, |this, window, cx| {
                if this.editor.read(cx).is_completion_menu_open(cx) {
                    editor.update(cx, |editor, cx| {
                        editor.refresh_completions(window, cx);
                    });
                } else {
                    editor.update(cx, |editor, cx| {
                        editor.show_completions_auto(window, cx);
                    });
                }
            });
        }));
    }

    /// Handle TriggerCompletion action - manually trigger completion popup
    fn handle_trigger_completion(
        &mut self,
        _action: &TriggerCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!("TriggerCompletion action triggered");
        
        self.editor.update(cx, |editor, cx| {
            editor.show_completions(window, cx);
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
        if let Some(inline_suggestion) = &self.inline_suggestion {
            self.accept_inline_suggestion(window, cx);
            return;
        }

        // Check if completion menu is open and handle the action
        let handled = self.editor.update(cx, |editor, cx| {
            editor.handle_completion_action(Box::new(Confirm), window, cx)
        });

        if !handled {
            tracing::debug!("AcceptCompletion: no completion menu open");
        }
    }

    /// Handle CancelCompletion action - hides completion menu
    fn handle_cancel_completion(
        &mut self,
        _action: &CancelCompletion,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editor.update(cx, |editor, cx| {
            editor.hide_completions(cx);
        });
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
            editor.handle_completion_action(Box::new(SelectDown), window, cx);
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
            editor.handle_completion_action(Box::new(SelectUp), window, cx);
        });
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
        let handled = self.editor.update(cx, |editor, cx| {
            editor.handle_completion_action(Box::new(Confirm), window, cx)
        });
        if !handled {
            self.editor.update(cx, |editor, cx| {
                editor.hide_completions(cx);
            });
        }
    }

    /// Cancels/hides the completion menu. Propagates the event when the menu is closed.
    fn handle_cancel_completion_menu(
        &mut self,
        _action: &CancelCompletionMenu,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.editor.read(cx).is_completion_menu_open(cx) {
            cx.propagate();
            return;
        }
        self.editor.update(cx, |editor, cx| {
            editor.hide_completions(cx);
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

    /// Trigger inline suggestion at current cursor position
    /// This is called automatically when typing or explicitly via trigger action
    pub fn trigger_inline_suggestion(&mut self, cx: &mut Context<Self>) {
        let settings = ZqlzSettings::global(cx);

        // Check if inline suggestions are enabled
        if !settings.editor.inline_suggestions_enabled {
            return;
        }

        let cursor_offset = self.editor.read(cx).get_cursor_offset(cx);
        let text = self.editor.read(cx).get_text(cx);

        // Get prefix (text before cursor) and suffix (text after cursor)
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

        // Get inline suggestion based on provider setting
        let provider = settings.editor.inline_suggestions_provider;
        let suggestion = match provider {
            InlineSuggestionProvider::LspOnly => {
                self.get_lsp_inline_suggestion(prefix.clone(), suffix.clone(), cursor_offset, cx)
            }
            InlineSuggestionProvider::AiOnly => {
                self.get_ai_inline_suggestion(prefix.clone(), suffix.clone(), cursor_offset, cx)
            }
            InlineSuggestionProvider::Both => {
                // Try LSP first, then fall back to AI
                if let Some(suggestion) =
                    self.get_lsp_inline_suggestion(prefix.clone(), suffix.clone(), cursor_offset, cx)
                {
                    Some(suggestion)
                } else {
                    self.get_ai_inline_suggestion(prefix, suffix, cursor_offset, cx)
                }
            }
        };

        if let Some((suggestion_text, start, end, source)) = suggestion {
            let source = match source {
                "LSP" => InlineSuggestionSource::Lsp,
                _ => InlineSuggestionSource::Ai,
            };
            self.inline_suggestion = Some(InlineSuggestionState {
                suggestion: suggestion_text,
                start_offset: start,
                end_offset: end,
                source,
            });
            cx.notify();
            tracing::debug!(
                "Inline suggestion shown: {} (source: {:?})",
                self.inline_suggestion.as_ref().map(|s| &s.suggestion).unwrap_or(&"".to_string()),
                self.inline_suggestion.as_ref().map(|s| s.source)
            );
        }
    }

    /// Get inline suggestion from LSP completions
    fn get_lsp_inline_suggestion(
        &self,
        prefix: String,
        suffix: String,
        cursor_offset: usize,
        cx: &App,
    ) -> Option<(String, usize, usize, &'static str)> {
        let settings = ZqlzSettings::global(cx);

        // Check if LSP completions are enabled
        if !settings.editor.lsp_enabled || !settings.editor.lsp_completions_enabled {
            return None;
        }

        // Get completions from editor wrapper
        let completions = self.editor.read(cx).get_completions(cx);

        // Find the best completion for inline suggestion
        for completion in completions.iter() {
            if let Some(insert_text) = &completion.insert_text {
                if !insert_text.is_empty() {
                    return Some((
                        insert_text.clone(),
                        cursor_offset,
                        cursor_offset + insert_text.len(),
                        "LSP",
                    ));
                }
            }
        }

        None
    }

    /// Get inline suggestion from AI provider
    fn get_ai_inline_suggestion(
        &self,
        prefix: String,
        suffix: String,
        cursor_offset: usize,
        cx: &App,
    ) -> Option<(String, usize, usize, &'static str)> {
        let settings = ZqlzSettings::global(cx);

        // Create AI provider based on settings
        let provider = AiProviderFactory::create_provider(
            settings.editor.ai_provider,
            settings.editor.ai_api_key.clone(),
            settings.editor.ai_model.clone(),
            settings.editor.ai_temperature,
        )?;

        // Check if provider is available
        if !provider.is_available() {
            return None;
        }

        // Build completion request
        let request = CompletionRequest {
            prefix: prefix.into(),
            suffix: suffix.into(),
            cursor_offset,
            schema_context: None,
            dialect: None,
        };

        // Get suggestion - this is synchronous for now
        // In a real implementation, this would be async
        let runtime = match tokio::runtime::Runtime::new() {
            Ok(r) => r,
            Err(_) => return None,
        };
        
        let result: Result<CompletionResponse, _> = runtime.block_on(async {
            provider.suggest(request).await
        });
        
        if let Ok(response) = result {
            let suggestion = response.suggestion;
            if !suggestion.is_empty() {
                return Some((
                    suggestion.to_string(),
                    cursor_offset,
                    cursor_offset + suggestion.len(),
                    "AI",
                ));
            }
        }

        None
    }

    /// Accept the current inline suggestion
    pub fn accept_inline_suggestion(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(inline_suggestion) = self.inline_suggestion.take() {
            // Insert the suggestion text at the current position
            let current_text = self.editor.read(cx).get_text(cx);
            let cursor_offset = self.editor.read(cx).get_cursor_offset(cx);

            // Build new text with suggestion inserted
            let mut new_text = String::new();
            new_text.push_str(&current_text[..cursor_offset]);
            new_text.push_str(&inline_suggestion.suggestion);
            new_text.push_str(&current_text[cursor_offset..]);

            // Set the new text
            self.editor.update(cx, |editor, cx| {
                editor.set_text(new_text, window, cx);
            });

            // Note: Cursor positioning after accepting inline suggestion
            // could be improved in a future iteration
            // For now, the cursor remains at the insertion point

            tracing::debug!(
                "Inline suggestion accepted: {}",
                inline_suggestion.suggestion
            );
            cx.notify();
        }
    }

    /// Dismiss the current inline suggestion
    pub fn dismiss_inline_suggestion(&mut self, cx: &mut Context<Self>) {
        if self.inline_suggestion.is_some() {
            self.inline_suggestion = None;
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
    fn handle_show_hover(
        &mut self,
        _action: &ShowHover,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("ShowHover action triggered");
        
        let hover = self.editor.read(cx).get_hover(cx);
        
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
            
            tracing::info!("Hover content: {}", content);
            self.hover_content = Some(content);
        } else {
            self.hover_content = None;
        }
        
        cx.notify();
    }

    /// Clear the hover popover
    pub fn clear_hover(&mut self, cx: &mut Context<Self>) {
        self.hover_content = None;
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

        if let Some(sig_help) = signature_help {
            let content = Self::format_signature_help(&sig_help);
            tracing::info!("Signature help content: {}", content);
            self.signature_help_content = Some(content);
        } else {
            self.signature_help_content = None;
        }

        cx.notify();
    }

    /// Handle GoToDefinition action - navigate to definition of symbol under cursor
    fn handle_go_to_definition(
        &mut self,
        _action: &GoToDefinition,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("GoToDefinition action triggered");

        let definition = self.editor.read(cx).get_definition(cx);

        if let Some(def_response) = definition {
            let content = Self::format_definition_response(&def_response);
            tracing::info!("Definition found: {}", content);
            self.definition_content = Some(content);
        } else {
            self.definition_content = None;
            tracing::debug!("No definition found at cursor position");
        }

        cx.notify();
    }

    /// Format definition response for display
    fn format_definition_response(def_response: &lsp_types::GotoDefinitionResponse) -> String {
        match def_response {
            lsp_types::GotoDefinitionResponse::Scalar(location) => {
                format!(
                    "**Definition**\n\nFile: {}\nLine: {}, Column: {}",
                    location.uri.as_str(),
                    location.range.start.line + 1,
                    location.range.start.character + 1
                )
            }
            lsp_types::GotoDefinitionResponse::Array(locations) => {
                let locations_str: Vec<String> = locations
                    .iter()
                    .map(|loc| {
                        format!(
                            "- {} (Line {}, Column {})",
                            loc.uri.as_str(),
                            loc.range.start.line + 1,
                            loc.range.start.character + 1
                        )
                    })
                    .collect();
                format!("**Definitions ({} found)**\n\n{}", locations.len(), locations_str.join("\n"))
            }
            lsp_types::GotoDefinitionResponse::Link(links) => {
                let links_str: Vec<String> = links
                    .iter()
                    .map(|link| {
                        let target = link.target_uri.as_str();
                        let start = link.origin_selection_range.map(|r| {
                            format!("Line {}, Column {}", r.start.line + 1, r.start.character + 1)
                        }).unwrap_or_else(|| "cursor position".to_string());
                        format!("- {} ({})", target, start)
                    })
                    .collect();
                format!("**Definition Links ({} found)**\n\n{}", links.len(), links_str.join("\n"))
            }
        }
    }

    /// Clear the definition popover
    pub fn clear_definition(&mut self, cx: &mut Context<Self>) {
        self.definition_content = None;
        cx.notify();
    }

    /// Handle FindReferences action - find all references to symbol under cursor
    fn handle_find_references(
        &mut self,
        _action: &FindReferences,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("FindReferences action triggered");

        let references = self.editor.read(cx).get_references(cx);

        if references.is_empty() {
            tracing::debug!("No references found at cursor position");
            self.references_content = None;
        } else {
            let ref_items: Vec<ReferenceItem> = references
                .iter()
                .map(|loc| ReferenceItem {
                    uri: loc.uri.to_string(),
                    line: loc.range.start.line,
                    column: loc.range.start.character,
                    text: None,
                })
                .collect();
            tracing::info!("Found {} references", ref_items.len());
            self.references_content = Some(ref_items);
        }

        cx.notify();
    }

    /// Clear the references list
    pub fn clear_references(&mut self, cx: &mut Context<Self>) {
        self.references_content = None;
        cx.notify();
    }

    /// Handle RenameSymbol action - rename symbol under cursor
    fn handle_rename_symbol(
        &mut self,
        _action: &RenameSymbol,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("RenameSymbol action triggered");

        // Get the current word under cursor
        let offset = self.editor.read(cx).get_cursor_offset(cx);
        let text = self.editor.read(cx).get_text(cx);

        // Extract the word at the cursor position
        let word = Self::extract_word_at_offset(&text, offset);

        if let Some(word) = word {
            if word.is_empty() {
                tracing::debug!("No word at cursor position for rename");
                self.rename_content = None;
            } else {
                tracing::info!("Rename initiated for: {}", word);
                self.rename_content = Some(RenameState {
                    original_name: word.clone(),
                    input_value: word,
                    trigger_offset: offset,
                    error_message: None,
                });
            }
        } else {
            self.rename_content = None;
        }

        cx.notify();
    }

    /// Extract a word at a given byte offset in the text
    fn extract_word_at_offset(text: &str, offset: usize) -> Option<String> {
        if offset > text.len() {
            return None;
        }

        let chars: Vec<char> = text.chars().collect();
        if chars.is_empty() {
            return None;
        }

        // Find start of word
        let mut start = offset;
        while start > 0 {
            let prev_idx = start - 1;
            if chars[prev_idx].is_alphanumeric() || chars[prev_idx] == '_' {
                start -= 1;
            } else {
                break;
            }
        }

        // Find end of word
        let mut end = offset;
        while end < chars.len() {
            if chars[end].is_alphanumeric() || chars[end] == '_' {
                end += 1;
            } else {
                break;
            }
        }

        if start < end {
            Some(chars[start..end].iter().collect())
        } else {
            None
        }
    }

    /// Apply the rename with the new name
    pub fn apply_rename(&mut self, new_name: String, window: &mut Window, cx: &mut Context<Self>) {
        let rename_state = match &self.rename_content {
            Some(state) => state.clone(),
            None => return,
        };

        // Check if new name is the same as original
        if new_name.to_lowercase() == rename_state.original_name.to_lowercase() {
            self.rename_content = None;
            cx.notify();
            return;
        }

        // Call the LSP rename
        let workspace_edit = self.editor.read(cx).rename(&new_name, cx);

        match workspace_edit {
            Some(edit) => {
                tracing::info!("Rename workspace edit received, applying {} text edits", 
                    edit.changes.as_ref().map(|c| c.len()).unwrap_or(0));
                
                // Apply the edits to the editor (need mutable access)
                let success = self.editor.update(cx, |editor, cx| {
                    editor.apply_workspace_edit(&edit, window, cx)
                });
                
                if success {
                    tracing::info!("Rename applied successfully");
                } else {
                    tracing::warn!("Failed to apply rename edits");
                }
                
                // Clear the rename UI
                self.rename_content = None;
            }
            None => {
                tracing::debug!("Rename returned no edits (may be invalid identifier or keyword)");
                self.rename_content = Some(RenameState {
                    original_name: rename_state.original_name,
                    input_value: new_name,
                    trigger_offset: rename_state.trigger_offset,
                    error_message: Some("Cannot rename: invalid identifier or keyword".to_string()),
                });
            }
        }

        cx.notify();
    }

    /// Cancel the rename operation
    pub fn cancel_rename(&mut self, cx: &mut Context<Self>) {
        self.rename_content = None;
        cx.notify();
    }

    /// Handle ShowCodeActions action - show available code actions at cursor
    fn handle_show_code_actions(
        &mut self,
        _action: &ShowCodeActions,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::debug!("ShowCodeActions action triggered");

        // Get code actions from the LSP
        let code_actions = self.editor.read(cx).get_code_actions(cx);

        if code_actions.is_empty() {
            tracing::debug!("No code actions available at cursor position");
            self.code_actions_content = None;
        } else {
            let action_items: Vec<CodeActionItem> = code_actions
                .iter()
                .enumerate()
                .map(|(idx, action)| CodeActionItem {
                    title: action.title.clone(),
                    kind: action.kind.as_ref().map(|k| format!("{:?}", k)),
                    index: idx,
                })
                .collect();
            tracing::info!("Found {} code actions", action_items.len());
            self.code_actions_content = Some(action_items);
        }

        cx.notify();
    }

    /// Apply a code action by index
    pub fn apply_code_action(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        // Get the code actions list
        let code_actions = self.editor.read(cx).get_code_actions(cx);
        
        // Find the action at the given index
        if let Some(action) = code_actions.get(index) {
            if let Some(edit) = &action.edit {
                tracing::info!("Applying code action: {}", action.title);
                
                // Apply the edits to the editor
                let success = self.editor.update(cx, |editor, cx| {
                    editor.apply_workspace_edit(edit, window, cx)
                });
                
                if success {
                    tracing::info!("Code action applied successfully");
                } else {
                    tracing::warn!("Failed to apply code action edits");
                }
            } else {
                tracing::debug!("Code action has no edits to apply: {}", action.title);
            }
        } else {
            tracing::warn!("Code action index out of bounds: {}", index);
        }
        
        // Clear the code actions UI
        self.code_actions_content = None;
        cx.notify();
    }

    /// Clear the code actions list
    pub fn clear_code_actions(&mut self, cx: &mut Context<Self>) {
        self.code_actions_content = None;
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
        self.signature_help_content = None;
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
    fn format_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        tracing::info!("🔍 format_query called!");
        let content = self.content(cx).to_string();
        tracing::info!("🔍 Content length: {} bytes", content.len());

        let formatted = if self.object_type.is_procedural() {
            Self::format_sql_with_dollar_quoting(&content)
        } else {
            Self::format_sql(&content)
        };
        tracing::info!("🔍 Formatted content length: {} bytes", formatted.len());

        self.editor.update(cx, |editor, cx| {
            editor.set_text(formatted, window, cx);
        });
        self.is_dirty = true;

        // TODO: Update diagnostics after formatting (Phase 3 task-3.4)
        // self.update_diagnostics(cx);
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

    fn format_sql(sql: &str) -> String {
        use sqlformat::{Dialect, FormatOptions, Indent, QueryParams, format};

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
            dialect: Dialect::Generic,
        };

        format(sql, &QueryParams::None, &options)
    }

    /// Format SQL that may contain dollar-quoted procedural blocks (PL/pgSQL).
    ///
    /// Splits on dollar-quote delimiters (`$tag$`), formats only the outer DDL
    /// parts with the standard SQL formatter, and preserves the procedural body
    /// verbatim so that PL/pgSQL `DECLARE`/`BEGIN`/`END` blocks are not mangled.
    fn format_sql_with_dollar_quoting(sql: &str) -> String {
        // Match dollar-quote delimiters like $$, $function$, $body$, $BODY$, etc.
        let delimiter_pattern =
            regex::Regex::new(r"\$([a-zA-Z_][a-zA-Z0-9_]*)?\$").unwrap_or_else(|_| {
                // Fallback: return unformatted if regex fails
                return regex::Regex::new(r"^\b$").expect("infallible regex");
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

                let formatted_before = Self::format_sql(before_body);
                let formatted_after = if after_body.trim().is_empty() {
                    after_body.to_string()
                } else {
                    Self::format_sql(after_body)
                };

                return format!("{}\n{}\n{}", formatted_before.trim_end(), body, formatted_after);
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
        Self::format_sql(sql)
    }

    /// Render the toolbar with execution controls
    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
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
            .on_action(cx.listener(Self::handle_go_to_definition))
            .on_action(cx.listener(Self::handle_find_references))
            .on_action(cx.listener(Self::handle_rename_symbol))
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
                        .disabled(!has_connection || is_empty || !self.is_dirty)
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
                    .tooltip("Run Query")
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
                        this.emit_explain_selection(cx);
                    })),
            )
            // Stop button with icon (only shown when executing)
            .when(self.is_executing, |this| {
                this.child(
                    Button::new("stop")
                        .danger()
                        .small()
                        .icon(ZqlzIcon::Stop)
                        .tooltip("Stop Query")
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
                    let mut btn = Button::new("mode-sql")
                        .small()
                        .icon(ZqlzIcon::Code)
                        .tooltip("SQL Mode")
                        .on_click(cx.listener(|this, _, window, cx| {
                            if this.editor_mode != EditorMode::Sql {
                                this.toggle_editor_mode(window, cx);
                            }
                        }));
                    btn = if is_active { btn.primary() } else { btn.ghost() };
                    btn
                })
                .child({
                    let is_active = self.editor_mode == EditorMode::Template;
                    let mut btn = Button::new("mode-template")
                        .small()
                        .icon(ZqlzIcon::BracketsCurly)
                        .tooltip("Template Mode")
                        .on_click(cx.listener(|this, _, window, cx| {
                            if this.editor_mode != EditorMode::Template {
                                this.toggle_editor_mode(window, cx);
                            }
                        }));
                    btn = if is_active { btn.primary() } else { btn.ghost() };
                    btn
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
                                                cx.emit(QueryEditorEvent::SwitchConnection { connection_id: conn_id });
                                            });
                                        })
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
                                                cx.emit(QueryEditorEvent::SwitchDatabase { database_name: db_name_clone.clone() });
                                            });
                                        })
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
                    // Error/warning count badge
                    .map(|this| {
                        let (errors, warnings, _) = self.diagnostic_counts(cx);
                        if errors > 0 {
                            this.child(
                                h_flex()
                                    .gap_1()
                                    .items_center()
                                    .child(div().size_2().rounded_full().bg(theme.danger))
                                    .child(div().text_xs().text_color(theme.danger).child(
                                        format!(
                                            "{} error{}",
                                            errors,
                                            if errors == 1 { "" } else { "s" }
                                        ),
                                    )),
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
                                    )),
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

    /// Render the SQL editor area
    fn render_editor(&self, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .h_full()
            .child(self.editor.read(cx).editor())
    }

    /// Parse and format hover content with markdown-like styling
    /// Supports: headings (#), code blocks (```), inline code (`), bold (**)
    fn format_hover_content(&self, content: &str, cx: &mut Context<Self>) -> gpui::AnyElement {
        let theme = cx.theme().clone();
        
        // Check if content has code blocks (```)
        let has_code_blocks = content.contains("```");
        
        if has_code_blocks {
            // Render with code block support
            self.render_markdown_with_code_blocks(content, &theme)
        } else {
            // Simple rendering for plain text
            self.render_simple_markdown(content, &theme)
        }
    }

    /// Render content with code block support
    fn render_markdown_with_code_blocks(&self, content: &str, theme: &Theme) -> gpui::AnyElement {
        let mut elements: Vec<gpui::AnyElement> = Vec::new();
        let mut in_code_block = false;
        let mut code_block_lines: Vec<&str> = Vec::new();
        
        for line in content.lines() {
            if line.starts_with("```") {
                if !in_code_block {
                    // Start of code block
                    in_code_block = true;
                    code_block_lines.clear();
                } else {
                    // End of code block - render it
                    let code_text = code_block_lines.join("\n");
                    elements.push(
                        div()
                            .w_full()
                            .bg(theme.muted)
                            .rounded_sm()
                            .p_2()
                            .mb_2()
                            .font_family("SF Mono, Menlo, Monaco, Consolas, monospace".to_string())
                            .text_xs()
                            .text_color(theme.foreground)
                            .overflow_x_scrollbar()
                            .child(code_text)
                            .into_any_element()
                    );
                    in_code_block = false;
                    code_block_lines.clear();
                }
            } else if in_code_block {
                code_block_lines.push(line);
            } else {
                // Regular content - render with markdown styling
                let styled = self.render_markdown_line(line, theme);
                elements.push(div().w_full().mb_1().child(styled).into_any_element());
            }
        }
        
        // Handle remaining code block if not closed
        if in_code_block && !code_block_lines.is_empty() {
            let code_text = code_block_lines.join("\n");
            elements.push(
                div()
                    .w_full()
                    .bg(theme.muted)
                    .rounded_sm()
                    .p_2()
                    .mb_2()
                    .font_family("SF Mono, Menlo, Monaco, Consolas, monospace".to_string())
                    .text_xs()
                    .text_color(theme.foreground)
                    .overflow_x_scrollbar()
                    .child(code_text)
                    .into_any_element()
            );
        }
        
        // If no elements were created (empty content), show the raw content
        if elements.is_empty() {
            div().child(content.to_string()).into_any_element()
        } else {
            div()
                .children(elements.into_iter().map(gpui::IntoElement::into_any_element))
                .into_any_element()
        }
    }

    /// Render a single line with markdown-like styling
    fn render_markdown_line(&self, line: &str, theme: &Theme) -> gpui::AnyElement {
        // Check for headings
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
        
        // Check for inline code
        if line.contains('`') {
            return self.render_inline_code(line, theme);
        }
        
        // Check for bold text
        if line.contains("**") {
            return self.render_bold_text(line, theme);
        }
        
        // Regular text - wrap in div
        div()
            .text_xs()
            .text_color(theme.popover_foreground)
            .child(line.to_string())
            .into_any_element()
    }

    /// Render inline code (text wrapped in backticks)
    fn render_inline_code(&self, line: &str, theme: &Theme) -> gpui::AnyElement {
        let mut parts: Vec<gpui::AnyElement> = Vec::new();
        let mut remaining = line;
        
        while let Some(start) = remaining.find('`') {
            // Add text before the backtick
            if start > 0 {
                parts.push(div()
                    .text_xs()
                    .text_color(theme.popover_foreground)
                    .child(remaining[..start].to_string())
                    .into_any_element());
            }
            
            if let Some(end) = remaining[start+1..].find('`') {
                // Found inline code
                let code = &remaining[start+1..start+1+end];
                parts.push(div()
                    .bg(theme.muted)
                    .rounded_sm()
                    .px_1()
                    .font_family("SF Mono, Menlo, Monaco, Consolas, monospace".to_string())
                    .text_xs()
                    .text_color(theme.accent)
                    .child(code.to_string())
                    .into_any_element());
                remaining = &remaining[start+1+end+1..];
            } else {
                // No closing backtick, treat rest as text
                parts.push(div()
                    .text_xs()
                    .text_color(theme.popover_foreground)
                    .child(remaining.to_string())
                    .into_any_element());
                break;
            }
        }
        
        // Add any remaining text
        if !remaining.is_empty() {
            parts.push(div()
                .text_xs()
                .text_color(theme.popover_foreground)
                .child(remaining.to_string())
                .into_any_element());
        }
        
        div()
            .children(parts.into_iter().map(gpui::IntoElement::into_any_element))
            .into_any_element()
    }

    /// Render text with bold markers (**text**)
    fn render_bold_text(&self, line: &str, theme: &Theme) -> gpui::AnyElement {
        let mut parts: Vec<gpui::AnyElement> = Vec::new();
        let mut remaining = line;
        
        while let Some(start) = remaining.find("**") {
            // Add text before the bold marker
            if start > 0 {
                parts.push(div()
                    .text_xs()
                    .text_color(theme.popover_foreground)
                    .child(remaining[..start].to_string())
                    .into_any_element());
            }
            
            if let Some(end) = remaining[start+2..].find("**") {
                // Found bold text
                let bold = &remaining[start+2..start+2+end];
                parts.push(div()
                    .text_xs()
                    .font_weight(gpui::FontWeight::from(700.0))
                    .text_color(theme.foreground)
                    .child(bold.to_string())
                    .into_any_element());
                remaining = &remaining[start+2+end+2..];
            } else {
                // No closing marker, treat rest as text
                parts.push(div()
                    .text_xs()
                    .text_color(theme.popover_foreground)
                    .child(remaining.to_string())
                    .into_any_element());
                break;
            }
        }
        
        // Add any remaining text
        if !remaining.is_empty() {
            parts.push(div()
                .text_xs()
                .text_color(theme.popover_foreground)
                .child(remaining.to_string())
                .into_any_element());
        }
        
        div()
            .children(parts.into_iter().map(gpui::IntoElement::into_any_element))
            .into_any_element()
    }

    /// Render simple markdown (headings and basic styling without code blocks)
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
                .children(elements.into_iter().map(gpui::IntoElement::into_any_element))
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
                }))
        )
    }

    /// Render the signature help popover if there's content to show
    fn render_signature_help_popover(&self, cx: &mut Context<Self>) -> Option<impl IntoElement> {
        let content = self.signature_help_content.as_ref()?;
        let theme = cx.theme();
        
        // Parse and render signature help with styled elements
        let lines: Vec<&str> = content.lines().collect();
        let mut elements: Vec<gpui::AnyElement> = Vec::new();
        
        let mut in_parameters = false;
        let mut active_param_index = 0;
        
        for (line_idx, line) in lines.iter().enumerate() {
            if line.starts_with("**") && line.ends_with("**") && !line.contains("Parameters") {
                // Function name header
                let func_name = line.trim_start_matches("**").trim_end_matches("**");
                elements.push(
                    div()
                        .text_sm()
                        .font_weight(gpui::FontWeight::from(700.0))
                        .text_color(theme.foreground)
                        .mb_2()
                        .child(func_name.to_string())
                        .into_any_element()
                );
            } else if line.contains("**Parameters:**") {
                // Parameters header
                in_parameters = true;
                elements.push(
                    div()
                        .text_xs()
                        .font_weight(gpui::FontWeight::from(600.0))
                        .text_color(theme.muted_foreground)
                        .mb_1()
                        .mt_2()
                        .child("Parameters")
                        .into_any_element()
                );
            } else if in_parameters && !line.is_empty() {
                // Parameter line - check for active marker
                let is_active = line.starts_with("◀");
                let param_text = line.trim_start_matches("◀").trim_start_matches(' ');
                
                if is_active {
                    active_param_index += 1;
                    // Active parameter - highlight with background
                    elements.push(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .bg(theme.muted)
                            .rounded_sm()
                            .px_2()
                            .py_1()
                            .mb_1()
                            .children([
                                div()
                                    .text_xs()
                                    .text_color(theme.accent)
                                    .child("◀")
                                    .into_any_element(),
                                div()
                                    .text_xs()
                                    .font_weight(gpui::FontWeight::from(600.0))
                                    .text_color(theme.foreground)
                                    .font_family("SF Mono, Menlo, Monaco, Consolas, monospace".to_string())
                                    .child(param_text.to_string())
                                    .into_any_element(),
                            ])
                            .into_any_element()
                    );
                } else {
                    // Inactive parameter - regular styling
                    elements.push(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .text_xs()
                            .text_color(theme.popover_foreground)
                            .mb_1()
                            .children([
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .w_3()
                                    .child(" ")
                                    .into_any_element(),
                                div()
                                    .text_xs()
                                    .text_color(theme.popover_foreground)
                                    .font_family("SF Mono, Menlo, Monaco, Consolas, monospace".to_string())
                                    .child(param_text.to_string())
                                    .into_any_element(),
                            ])
                            .into_any_element()
                    );
                }
            }
        }
        
        Some(
            div()
                .absolute()
                .bottom_8()
                .left_4()
                .max_w(px(500.0))
                .max_h(px(300.0))
                .overflow_y_scrollbar()
                .bg(theme.popover)
                .border_1()
                .border_color(theme.info)
                .rounded_md()
                .shadow_lg()
                .p_3()
                .text_xs()
                .child(
                    div()
                        .children(elements.into_iter().map(gpui::IntoElement::into_any_element))
                )
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.clear_signature_help(cx);
                }))
        )
    }

    /// Render the definition popover if there's content to show
    fn render_definition_popover(&self, cx: &mut Context<Self>) -> Option<impl IntoElement> {
        let content = self.definition_content.as_ref()?;
        let theme = cx.theme();
        
        Some(
            div()
                .absolute()
                .top_8()
                .right_4()
                .max_w(px(400.0))
                .max_h(px(300.0))
                .overflow_y_scrollbar()
                .bg(theme.popover)
                .border_1()
                .border_color(theme.info)
                .rounded_md()
                .shadow_lg()
                .p_3()
                .text_xs()
                .text_color(theme.popover_foreground)
                .child(content.clone())
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.clear_definition(cx);
                }))
        )
    }

    /// Render the references list popover if there's content to show
    fn render_references_popover(&self, cx: &mut Context<Self>) -> Option<impl IntoElement> {
        let references = self.references_content.as_ref()?;
        let theme = cx.theme();
        let refs_count = references.len();
        
        Some(
            div()
                .absolute()
                .top_8()
                .right_4()
                .max_w(px(400.0))
                .max_h(px(400.0))
                .overflow_y_scrollbar()
                .bg(theme.popover)
                .border_1()
                .border_color(theme.info)
                .rounded_md()
                .shadow_lg()
                .p_3()
                .text_xs()
                .child(
                    div()
                        .font_weight(gpui::FontWeight::MEDIUM)
                        .text_color(theme.popover_foreground)
                        .mb_2()
                        .child(if refs_count == 0 {
                            "No references found".to_string()
                        } else {
                            format!("References ({} found)", refs_count)
                        })
                )
                .when(refs_count > 0, |this| {
                    this.child(
                        v_flex()
                            .gap_1()
                            .children(references.iter().enumerate().map(|(idx, r)| {
                                div()
                                    .p_1()
                                    .rounded_sm()
                                    .hover(|s| s.bg(theme.muted))
                                    .cursor_pointer()
                                    .child(
                                        div()
                                            .text_color(theme.popover_foreground)
                                            .child(format!("Line {}, Column {}", r.line + 1, r.column + 1))
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(theme.muted_foreground)
                                            .child(r.uri.clone())
                                    )
                            }))
                    )
                })
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.clear_references(cx);
                }))
        )
    }

    /// Render the rename popover if rename is active
    fn render_rename_popover(&self, window: &mut Window, cx: &mut Context<Self>) -> Option<impl IntoElement> {
        let rename_state = self.rename_content.as_ref()?;
        let theme = cx.theme();

        Some(
            div()
                .absolute()
                .top_8()
                .right_4()
                .w(px(300.0))
                .bg(theme.popover)
                .border_1()
                .border_color(theme.info)
                .rounded_md()
                .shadow_lg()
                .p_3()
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.cancel_rename(cx);
                }))
                .child(
                    div()
                        .text_xs()
                        .font_weight(gpui::FontWeight::MEDIUM)
                        .text_color(theme.popover_foreground)
                        .mb_2()
                        .child(format!("Rename '{}' to:", rename_state.original_name))
                )
                .child(
                    div()
                        .text_sm()
                        .text_color(theme.popover_foreground)
                        .mb_2()
                        .child(rename_state.input_value.clone())
                )
                .when_some(rename_state.error_message.as_ref(), |this, error| {
                    this.child(
                        div()
                            .text_xs()
                            .text_color(theme.danger)
                            .mb_2()
                            .child(error.clone())
                    )
                })
                .child(
                    h_flex()
                        .gap_2()
                        .justify_end()
                        .child(
                            Button::new("rename-cancel")
                                .ghost()
                                .small()
                                .label("Cancel")
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.cancel_rename(cx);
                                }))
                        )
                        .child(
                            Button::new("rename-confirm")
                                .primary()
                                .small()
                                .label("Rename")
                                .on_click(cx.listener(|this, _, window, cx| {
                                    let value = this.rename_content.as_ref().map(|s| s.input_value.clone()).unwrap_or_default();
                                    this.apply_rename(value, window, cx);
                                }))
                        )
                )
        )
    }

    /// Render the code actions popover if there are actions available
    fn render_code_actions_popover(&self, _window: &mut Window, cx: &mut Context<Self>) -> Option<impl IntoElement> {
        let actions = self.code_actions_content.as_ref()?;
        let theme = cx.theme();
        let actions_count = actions.len();

        Some(
            div()
                .absolute()
                .top_8()
                .right_4()
                .max_w(px(400.0))
                .max_h(px(300.0))
                .overflow_y_scrollbar()
                .bg(theme.popover)
                .border_1()
                .border_color(theme.info)
                .rounded_md()
                .shadow_lg()
                .p_3()
                .on_mouse_down_out(cx.listener(|this, _, _, cx| {
                    this.clear_code_actions(cx);
                }))
                .child(
                    div()
                        .font_weight(gpui::FontWeight::MEDIUM)
                        .text_color(theme.popover_foreground)
                        .mb_2()
                        .child(if actions_count == 0 {
                            "No code actions available".to_string()
                        } else {
                            format!("Code Actions ({} available) - Cmd+Shift+. to apply", actions_count)
                        })
                )
                .when(actions_count > 0, |this| {
                    this.child(
                        v_flex()
                            .gap_1()
                            .children(actions.iter().map(|action| {
                                div()
                                    .p_2()
                                    .rounded_sm()
                                    .hover(|s| s.bg(theme.muted))
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(theme.popover_foreground)
                                            .child(action.title.clone())
                                    )
                                    .when_some(action.kind.as_ref(), |this, kind| {
                                        this.child(
                                            div()
                                                .text_xs()
                                                .text_color(theme.muted_foreground)
                                                .child(kind.clone())
                                        )
                                    })
                            }))
                    )
                })
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
                div().flex_1().w_full().overflow_hidden().child(
                    div().h_full().w_full().child(
                        ZedInput::new(&self.template_params)
                    ),
                ),
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
        let signature_help_content = self.signature_help_content.clone();
        let definition_content = self.definition_content.clone();
        let references_content = self.references_content.clone();
        let code_actions_content = self.code_actions_content.clone();
        
        let inline_suggestion_text = self.inline_suggestion.as_ref().map(|s| s.suggestion.clone());
        
        let completion_menu = self.editor.read(cx).completion_menu().cloned();

        v_flex()
            .id("query-editor")
            .track_focus(&self.focus_handle(cx))
            .key_context("Editor")
            .size_full()
            .bg(bg_color)
            .on_action(cx.listener(Self::handle_trigger_completion))
            .on_action(cx.listener(Self::handle_accept_completion))
            .on_action(cx.listener(Self::handle_cancel_completion))
            .on_action(cx.listener(Self::handle_next_completion))
            .on_action(cx.listener(Self::handle_previous_completion))
            .on_action(cx.listener(Self::handle_confirm_completion))
            .on_action(cx.listener(Self::handle_cancel_completion_menu))
            .on_key_down(cx.listener(|this, event: &gpui::KeyDownEvent, window, cx| {
                let key = event.keystroke.key.as_str();

                // Trigger auto-completion when typing identifier-like characters.
                // Navigation keys (up/down/enter/tab/escape) are handled by
                // action handlers registered above.
                if key.len() == 1 {
                    if let Some(ch) = key.chars().next() {
                        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' {
                            this.trigger_auto_completion(window, cx);
                        }
                    }
                }
            }))
            .child(self.render_toolbar(cx))
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
                            .when_some(signature_help_content, |this, _content| {
                                this.children(self.render_signature_help_popover(cx))
                            })
                            .when_some(definition_content, |this, _content| {
                                this.children(self.render_definition_popover(cx))
                            })
                            .when_some(references_content, |this, _content| {
                                this.children(self.render_references_popover(cx))
                            })
                            .when_some(self.rename_content.as_ref(), |this, _content| {
                                this.children(self.render_rename_popover(_window, cx))
                            })
                            .when_some(code_actions_content, |this, _content| {
                                this.children(self.render_code_actions_popover(_window, cx))
                            })
                            .when_some(completion_menu, |this, menu| {
                                this.child(menu)
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
                                        .child(format!(
                                            "⟪ {} (Tab to accept, Esc to dismiss)",
                                            suggestion_text
                                        )),
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
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.editor.read(cx).focus_handle(cx)
    }
}

impl EventEmitter<PanelEvent> for QueryEditor {}
impl EventEmitter<QueryEditorEvent> for QueryEditor {}

impl Panel for QueryEditor {
    fn panel_name(&self) -> &'static str {
        "QueryEditor"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if self.is_dirty {
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
        self.is_dirty
    }
}
