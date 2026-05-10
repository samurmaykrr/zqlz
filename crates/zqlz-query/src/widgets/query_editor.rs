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

use crate::batch::split_statements;
use crate::schema_metadata::{SchemaMetadata, SchemaMetadataProvider, SchemaSymbolInfo};
use gpui::prelude::FluentBuilder;
use gpui::*;
use lsp_types::DiagnosticSeverity;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{
    Connection, DriverCategory, FormatRequest, Value, driver_category_from_driver_name,
    formatter_provider_for_driver,
};
use zqlz_lsp::SqlLsp;
use zqlz_services::{DatabaseSchema, SchemaService};
use zqlz_settings::{
    CursorBlink, CursorShape, EditorSettings as AppEditorSettings, ScrollBeyondLastLine,
    SearchWrap, ZqlzSettings,
};
use zqlz_templates::TemplateEngine;
use zqlz_text_editor::{
    CompletionSettings, CursorSettings, DocumentIdentity, DocumentSettings, EditorAppearance,
    EditorLanguageProviders, EditorSettings as TextEditorSettings, FormatProvider, GutterSettings,
    ScrollSettings, SearchSettings, SoftWrapMode, TextDocument, TextEditor, TextEditorEvent,
};
use zqlz_ui::widgets::{
    ActiveTheme, Disableable, Icon, Sizable, ZqlzIcon,
    button::{Button, ButtonVariants, DropdownButton},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    menu::DropdownMenu,
    scroll::ScrollableElement,
    v_flex,
};

use super::actions::{
    FormatQuery, NextProblem, PreviousProblem, SaveQuery, ShowCodeActions, ShowHover,
    TriggerParameterHints,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryDocumentSymbol {
    pub label: String,
    pub line: usize,
    pub column: usize,
}

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

fn query_document_symbols(sql: &str) -> Vec<QueryDocumentSymbol> {
    let mut symbols = Vec::new();
    let mut current = String::new();
    let mut start_line = 0usize;
    let mut start_column = 0usize;
    let mut line = 0usize;
    let mut column = 0usize;
    let mut in_string = false;
    let mut string_char = '\0';
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut statement_started = false;
    let mut chars = sql.chars().peekable();

    while let Some(character) = chars.next() {
        let next = chars.peek().copied();

        if !statement_started && !character.is_whitespace() {
            start_line = line;
            start_column = column;
            statement_started = true;
        }

        if !in_string && !in_block_comment && character == '-' && next == Some('-') {
            in_line_comment = true;
            current.push(character);
            advance_position(character, &mut line, &mut column);
            continue;
        }

        if in_line_comment {
            current.push(character);
            if character == '\n' {
                in_line_comment = false;
            }
            advance_position(character, &mut line, &mut column);
            continue;
        }

        if !in_string && !in_line_comment && character == '/' && next == Some('*') {
            in_block_comment = true;
            current.push(character);
            advance_position(character, &mut line, &mut column);
            continue;
        }

        if in_block_comment {
            current.push(character);
            advance_position(character, &mut line, &mut column);
            if character == '*' && next == Some('/') {
                current.push('/');
                chars.next();
                in_block_comment = false;
                advance_position('/', &mut line, &mut column);
            }
            continue;
        }

        if !in_string && (character == '\'' || character == '"') {
            in_string = true;
            string_char = character;
            current.push(character);
            advance_position(character, &mut line, &mut column);
            continue;
        }

        if in_string {
            current.push(character);
            advance_position(character, &mut line, &mut column);
            if character == string_char {
                if next == Some(string_char) {
                    current.push(string_char);
                    chars.next();
                    advance_position(string_char, &mut line, &mut column);
                    continue;
                }
                in_string = false;
            }
            continue;
        }

        if character == ';' {
            if let Some(label) = query_document_symbol_label(&current) {
                symbols.push(QueryDocumentSymbol {
                    label,
                    line: start_line,
                    column: start_column,
                });
            }
            current.clear();
            statement_started = false;
            advance_position(character, &mut line, &mut column);
            continue;
        }

        current.push(character);
        advance_position(character, &mut line, &mut column);
    }

    if let Some(label) = query_document_symbol_label(&current) {
        symbols.push(QueryDocumentSymbol {
            label,
            line: start_line,
            column: start_column,
        });
    }

    symbols
}

fn advance_position(character: char, line: &mut usize, column: &mut usize) {
    if character == '\n' {
        *line += 1;
        *column = 0;
    } else {
        *column += 1;
    }
}

fn query_document_symbol_label(statement: &str) -> Option<String> {
    let statement = statement.trim();
    if statement.is_empty() {
        return None;
    }

    let words: Vec<&str> = statement
        .split_whitespace()
        .filter(|word| !word.starts_with("--"))
        .take(4)
        .collect();
    let first = words
        .first()?
        .trim_matches(|ch: char| !ch.is_alphanumeric());
    if first.is_empty() {
        return None;
    }

    let action = first.to_uppercase();
    let label = match action.as_str() {
        "SELECT" | "WITH" => "Query".to_string(),
        "INSERT" | "UPDATE" | "DELETE" | "CREATE" | "ALTER" | "DROP" | "TRUNCATE" => {
            let target = words
                .iter()
                .skip(1)
                .find(|word| {
                    !matches!(
                        word.to_ascii_uppercase().as_str(),
                        "INTO" | "TABLE" | "VIEW" | "INDEX" | "FUNCTION" | "PROCEDURE" | "TRIGGER"
                    )
                })
                .map(|word| word.trim_matches(|ch: char| ch == '"' || ch == '`' || ch == ','));
            if let Some(target) = target.filter(|target| !target.is_empty()) {
                format!("{action} {target}")
            } else {
                action
            }
        }
        _ => action,
    };

    Some(label)
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
        let schema_hover = {
            let lsp = self.sql_lsp.read();
            let db_schema = lsp.get_schema_for_metadata();
            if db_schema.tables.is_empty() {
                None
            } else {
                let schema_metadata = SchemaMetadata::new(db_schema);
                schema_metadata
                    .find_symbol_at_offset(&text_string, offset)
                    .map(|symbol| {
                        let documentation = schema_symbol_hover_documentation(&symbol);
                        lsp_types::Hover {
                            contents: lsp_types::HoverContents::Markup(lsp_types::MarkupContent {
                                kind: lsp_types::MarkupKind::Markdown,
                                value: documentation,
                            }),
                            range: None,
                        }
                    })
            }
        };
        if schema_hover.is_some() {
            return Task::ready(Ok(schema_hover));
        }

        let ui_rope = zqlz_ui::widgets::Rope::from(text_string.as_str());
        let result = self.sql_lsp.read().get_hover(&ui_rope, offset);
        Task::ready(Ok(result))
    }
}

fn schema_symbol_hover_documentation(symbol_info: &SchemaSymbolInfo) -> String {
    let mut content = String::new();

    content.push_str(&format!(
        "**{}**: `{}`\n\n",
        symbol_info.symbol_type_name(),
        symbol_info.name
    ));

    if let Some(details) = &symbol_info.details {
        if let Some(columns) = &details.columns {
            content.push_str("**Columns**:\n");
            for col in columns.iter().take(10) {
                let pk_marker = if col.is_primary_key { " PK" } else { "" };
                content.push_str(&format!(
                    "- `{}`: {}{}\n",
                    col.name, col.data_type, pk_marker
                ));
            }
            if columns.len() > 10 {
                content.push_str(&format!("... and {} more\n", columns.len() - 10));
            }
        }

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

struct SqlLspDiagnosticAdapter {
    sql_lsp: Arc<RwLock<SqlLsp>>,
}

impl SqlLspDiagnosticAdapter {
    fn new(sql_lsp: Arc<RwLock<SqlLsp>>) -> Self {
        Self { sql_lsp }
    }
}

impl zqlz_text_editor::DiagnosticProvider for SqlLspDiagnosticAdapter {
    fn diagnostics(
        &self,
        text: &ropey::Rope,
        _document: &zqlz_text_editor::DocumentContext,
    ) -> Task<anyhow::Result<Vec<lsp_types::Diagnostic>>> {
        let text_string = text.to_string();
        let ui_rope = zqlz_ui::widgets::Rope::from(text_string.as_str());
        let diagnostics = self.sql_lsp.write().validate_sql(&ui_rope);
        Task::ready(Ok(diagnostics))
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
pub enum QueryExecutionParams {
    Positional(Vec<Value>),
    Named(HashMap<String, Value>),
}

#[derive(Clone, Debug)]
pub enum QueryEditorEvent {
    /// User requested to execute the current query
    ExecuteQuery {
        sql: String,
        connection_id: Option<Uuid>,
        database_name: Option<String>,
        params: Option<QueryExecutionParams>,
    },
    /// User requested to execute selected text or current statement
    ExecuteSelection {
        sql: String,
        connection_id: Option<Uuid>,
        database_name: Option<String>,
        params: Option<QueryExecutionParams>,
    },
    /// User requested to explain the current query
    ExplainQuery {
        sql: String,
        connection_id: Option<Uuid>,
        database_name: Option<String>,
    },
    /// User requested to explain selected text or current statement
    ExplainSelection {
        sql: String,
        connection_id: Option<Uuid>,
        database_name: Option<String>,
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

    /// Whether the active connection exposes a real query cancel handle.
    can_cancel_execution: bool,

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

    /// Currently selected schema label for the local query console context.
    current_schema: Option<String>,

    /// Real schema labels derived from loaded metadata. Empty means hide selector.
    available_schemas: Vec<String>,

    /// Debounce timer for diagnostics (triggers after typing stops)
    _diagnostics_debounce: Option<gpui::Task<()>>,

    /// Last text content (used to detect changes for diagnostics)
    _last_diagnostics_text: Option<String>,

    /// Subscriptions to keep alive
    _subscriptions: Vec<Subscription>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg(test)]
struct QueryEditorStatusLabels {
    cursor: String,
    selection: Option<String>,
    mode: String,
    connection: String,
    database: String,
    diagnostics: String,
    execution: Option<String>,
}

#[cfg(test)]
struct QueryEditorStatusInput<'a> {
    cursor_line: usize,
    cursor_column: usize,
    selection_chars: Option<usize>,
    mode: EditorMode,
    connection_name: Option<&'a str>,
    has_connection: bool,
    database: Option<&'a str>,
    diagnostics: (usize, usize, usize),
    is_executing: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryEditorRunMenuAction {
    Run,
    RunCurrentStatement,
    ContinueOnError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct QueryEditorRunMenuEntry {
    label: &'static str,
    action: QueryEditorRunMenuAction,
    checked: bool,
    disabled: bool,
    separator_before: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct QueryEditorRunTargetPreview {
    label: String,
    detail: String,
    preview: String,
    is_selection: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg(test)]
struct QueryEditorHeaderVisibility {
    show_mode_segment: bool,
    show_problems_badge: bool,
    show_template_error: bool,
    show_keymap_hints: bool,
    show_repeated_connection_label: bool,
}

impl QueryEditor {
    fn json_to_execution_value(value: &serde_json::Value) -> Value {
        match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(boolean) => Value::Bool(*boolean),
            serde_json::Value::Number(number) => {
                if let Some(integer) = number.as_i64() {
                    Value::Int64(integer)
                } else if let Some(float) = number.as_f64() {
                    Value::Float64(float)
                } else {
                    Value::Decimal(number.to_string())
                }
            }
            serde_json::Value::String(string) => Value::String(string.clone()),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                Value::Json(value.clone())
            }
        }
    }

    fn parse_sql_parameters(text: &str) -> Result<QueryExecutionParams, String> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(QueryExecutionParams::Named(HashMap::new()));
        }

        let parsed = serde_json::from_str::<serde_json::Value>(trimmed)
            .map_err(|error| format!("Parameter JSON parse error: {}", error))?;

        match parsed {
            serde_json::Value::Array(array) => Ok(QueryExecutionParams::Positional(
                array.iter().map(Self::json_to_execution_value).collect(),
            )),
            serde_json::Value::Object(map) => Ok(QueryExecutionParams::Named(
                map.into_iter()
                    .map(|(key, value)| (key, Self::json_to_execution_value(&value)))
                    .collect(),
            )),
            _ => Err("Parameters must be a JSON array or object".to_string()),
        }
    }

    fn execution_params(&self, cx: &App) -> Option<QueryExecutionParams> {
        if self.editor_mode != EditorMode::Template {
            return None;
        }

        let params_text = self.template_params.read(cx).get_text(cx).to_string();
        match Self::parse_sql_parameters(&params_text) {
            Ok(QueryExecutionParams::Positional(params)) if params.is_empty() => None,
            Ok(QueryExecutionParams::Named(params)) if params.is_empty() => None,
            Ok(params) => Some(params),
            Err(error) => {
                tracing::warn!(error = %error, "Invalid template parameters for SQL execution");
                None
            }
        }
    }

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

    fn run_menu_entries() -> Vec<QueryEditorRunMenuEntry> {
        vec![
            QueryEditorRunMenuEntry {
                label: "Run",
                action: QueryEditorRunMenuAction::Run,
                checked: false,
                disabled: false,
                separator_before: false,
            },
            QueryEditorRunMenuEntry {
                label: "Run Current Statement",
                action: QueryEditorRunMenuAction::RunCurrentStatement,
                checked: false,
                disabled: false,
                separator_before: false,
            },
            QueryEditorRunMenuEntry {
                label: "Continue on Error",
                action: QueryEditorRunMenuAction::ContinueOnError,
                checked: true,
                disabled: true,
                separator_before: true,
            },
        ]
    }

    fn compact_sql_preview(sql: &str, max_chars: usize) -> String {
        let compact = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        if compact.is_empty() {
            return "Empty SQL".to_string();
        }

        let char_count = compact.chars().count();
        if char_count <= max_chars {
            return compact;
        }

        let keep_chars = max_chars.saturating_sub(3);
        let mut preview: String = compact.chars().take(keep_chars).collect();
        preview.push_str("...");
        preview
    }

    fn sql_line_count(sql: &str) -> usize {
        sql.lines().count().max(1)
    }

    fn line_count_label(line_count: usize) -> String {
        format!(
            "{} line{}",
            line_count,
            if line_count == 1 { "" } else { "s" }
        )
    }

    fn line_range_label_for_byte_range(text: &str, start: usize, end: usize) -> String {
        let start = start.min(text.len());
        let end = end.min(text.len());
        let start_line = text[..start].bytes().filter(|byte| *byte == b'\n').count() + 1;
        let end_line = text[..end].bytes().filter(|byte| *byte == b'\n').count() + 1;

        if start_line == end_line {
            format!("Ln {start_line}")
        } else {
            format!("Ln {start_line}-{end_line}")
        }
    }

    fn current_statement_for_preview(
        full_sql: &str,
        cursor_offset: usize,
    ) -> (String, Option<(usize, usize)>) {
        let statements = split_statements(full_sql);
        if statements.len() <= 1 {
            return (full_sql.to_string(), Some((0, full_sql.len())));
        }

        let cursor_offset = cursor_offset.min(full_sql.len());
        let mut search_start = 0;
        for statement in statements {
            if let Some(relative_start) = full_sql[search_start..].find(&statement) {
                let statement_start = search_start + relative_start;
                let statement_end = statement_start + statement.len();

                if cursor_offset >= statement_start && cursor_offset <= statement_end {
                    return (statement, Some((statement_start, statement_end)));
                }

                search_start = statement_end;
            }
        }

        (full_sql.to_string(), Some((0, full_sql.len())))
    }

    fn run_target_preview_from_parts(
        executable_sql: &str,
        selected_sql: Option<&str>,
        cursor_offset: usize,
        mode: EditorMode,
    ) -> QueryEditorRunTargetPreview {
        if let Some(selected_sql) = selected_sql.map(str::trim).filter(|sql| !sql.is_empty()) {
            return QueryEditorRunTargetPreview {
                label: "Run Selection".to_string(),
                detail: Self::line_count_label(Self::sql_line_count(selected_sql)),
                preview: Self::compact_sql_preview(selected_sql, 96),
                is_selection: true,
            };
        }

        if mode == EditorMode::Template {
            return QueryEditorRunTargetPreview {
                label: "Run Rendered Template".to_string(),
                detail: Self::line_count_label(Self::sql_line_count(executable_sql)),
                preview: Self::compact_sql_preview(executable_sql, 96),
                is_selection: false,
            };
        }

        let (statement, range) = Self::current_statement_for_preview(executable_sql, cursor_offset);
        let range_label = range
            .map(|(start, end)| Self::line_range_label_for_byte_range(executable_sql, start, end))
            .unwrap_or_else(|| "Current".to_string());

        QueryEditorRunTargetPreview {
            label: "Run Current Statement".to_string(),
            detail: range_label,
            preview: Self::compact_sql_preview(&statement, 96),
            is_selection: false,
        }
    }

    fn run_target_preview(&self, cx: &App) -> QueryEditorRunTargetPreview {
        let selected_sql = self
            .editor
            .read(cx)
            .get_selected_text(cx)
            .map(|text| text.to_string());
        let executable_sql = self.get_executable_sql(cx);
        let cursor_offset = self.editor.read(cx).get_cursor_offset(cx);

        Self::run_target_preview_from_parts(
            &executable_sql,
            selected_sql.as_deref(),
            cursor_offset,
            self.editor_mode,
        )
    }

    #[cfg(test)]
    fn header_visibility() -> QueryEditorHeaderVisibility {
        QueryEditorHeaderVisibility {
            show_mode_segment: false,
            show_problems_badge: false,
            show_template_error: false,
            show_keymap_hints: false,
            show_repeated_connection_label: false,
        }
    }

    fn unique_non_empty_strings(values: impl IntoIterator<Item = String>) -> Vec<String> {
        let mut seen = HashSet::new();
        values
            .into_iter()
            .filter_map(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    return None;
                }
                let value = trimmed.to_string();
                if seen.insert(value.clone()) {
                    Some(value)
                } else {
                    None
                }
            })
            .collect()
    }

    fn schema_labels_from_parts(
        schema_names: impl IntoIterator<Item = String>,
        table_schemas: impl IntoIterator<Item = Option<String>>,
    ) -> Vec<String> {
        let schemas = Self::unique_non_empty_strings(schema_names);
        if !schemas.is_empty() {
            return schemas;
        }

        Self::unique_non_empty_strings(table_schemas.into_iter().flatten())
    }

    fn schema_labels_from_metadata(schema: &DatabaseSchema) -> Vec<String> {
        Self::schema_labels_from_parts(
            schema.schema_names.clone(),
            schema.table_infos.iter().map(|table| table.schema.clone()),
        )
    }

    fn reconcile_current_schema(&mut self) {
        if self.available_schemas.is_empty() {
            self.current_schema = None;
            return;
        }

        if let Some(current_schema) = &self.current_schema
            && self
                .available_schemas
                .iter()
                .any(|schema| schema == current_schema)
        {
            return;
        }

        self.current_schema = self.available_schemas.first().cloned();
    }

    fn clear_schema_dependent_ui_state(&mut self) {
        // Schema-bound editor UI is owned by provider-backed TextEditor state.
    }

    fn update_schema_selector_from_lsp(&mut self, cx: &mut Context<Self>) {
        let previous_schema = self.current_schema.clone();
        let schema = self.sql_lsp.read().get_schema_for_metadata();
        self.available_schemas = Self::schema_labels_from_metadata(&schema);
        if self.current_schema.is_none()
            && let Some(resolved_schema) = schema
                .schema_name
                .map(|schema_name| schema_name.trim().to_string())
                .filter(|schema_name| !schema_name.is_empty())
            && self
                .available_schemas
                .iter()
                .any(|schema_name| schema_name == &resolved_schema)
        {
            self.current_schema = Some(resolved_schema);
        }
        self.reconcile_current_schema();
        if self.current_schema != previous_schema {
            self.clear_schema_dependent_ui_state();
        }
        cx.notify();
    }

    fn selector_label(label: impl Into<SharedString>) -> impl IntoElement {
        div()
            .min_w_0()
            .flex_grow()
            .truncate()
            .line_height(relative(1.0))
            .child(label.into())
    }

    #[cfg(test)]
    fn status_labels_from_parts(input: QueryEditorStatusInput<'_>) -> QueryEditorStatusLabels {
        let (errors, warnings, infos) = input.diagnostics;
        let diagnostics = if errors > 0 {
            format!("{} error{}", errors, if errors == 1 { "" } else { "s" })
        } else if warnings > 0 {
            format!(
                "{} warning{}",
                warnings,
                if warnings == 1 { "" } else { "s" }
            )
        } else if infos > 0 {
            format!("{} info", infos)
        } else {
            "No Problems".to_string()
        };

        QueryEditorStatusLabels {
            cursor: format!(
                "Ln {}, Col {}",
                input.cursor_line + 1,
                input.cursor_column + 1
            ),
            selection: input
                .selection_chars
                .filter(|chars| *chars > 0)
                .map(|chars| format!("{chars} selected")),
            mode: match input.mode {
                EditorMode::Sql => "SQL".to_string(),
                EditorMode::Template => "Template".to_string(),
            },
            connection: input
                .connection_name
                .map(ToString::to_string)
                .unwrap_or_else(|| {
                    if input.has_connection {
                        "Connected".to_string()
                    } else {
                        "No Connection".to_string()
                    }
                }),
            database: input
                .database
                .map(ToString::to_string)
                .unwrap_or_else(|| "No Database".to_string()),
            diagnostics,
            execution: input.is_executing.then(|| "Running".to_string()),
        }
    }

    fn internal_text_document(text: impl AsRef<str>) -> TextDocument {
        TextDocument::with_text(
            DocumentIdentity::internal_with_label("query-editor").expect("internal document uri"),
            text.as_ref(),
        )
    }

    fn build_primary_editor(
        document: TextDocument,
        editor_settings: &AppEditorSettings,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<TextEditor> {
        let editor = cx.new(|cx| TextEditor::with_document(document, window, cx));

        editor.update(cx, |text_editor, cx| {
            Self::apply_editor_settings(text_editor, editor_settings, cx);
            text_editor.set_appearance(EditorAppearance::QueryConsole, cx);
            text_editor.set_autofocus_on_open(true);
        });

        editor
    }

    fn apply_editor_settings(
        text_editor: &mut TextEditor,
        settings: &AppEditorSettings,
        cx: &mut Context<TextEditor>,
    ) {
        let text_editor_settings = Self::text_editor_settings_from_app(settings);
        text_editor.apply_editor_settings(&text_editor_settings, cx);
        text_editor.set_highlight_enabled(settings.highlight_enabled, cx);
        text_editor.set_bracket_matching_enabled(settings.bracket_matching, cx);
        text_editor.set_show_gutter_diagnostics(settings.show_gutter_diagnostics, cx);
        text_editor.set_rounded_selection(settings.rounded_selection, cx);
        text_editor.set_search_wrap_enabled(!matches!(
            settings.search_wrap,
            SearchWrap::Disabled | SearchWrap::NoWrap
        ));
        text_editor.set_smartcase_search_enabled(settings.use_smartcase_search);
        text_editor.set_autoscroll_on_clicks(settings.autoscroll_on_clicks);
        text_editor.set_auto_indent_enabled(settings.auto_indent, cx);
        text_editor.set_large_file_thresholds(
            settings.large_file_line_threshold as usize,
            settings.large_file_byte_threshold as usize,
        );
    }

    fn text_editor_settings_from_app(settings: &AppEditorSettings) -> TextEditorSettings {
        TextEditorSettings {
            cursor: CursorSettings {
                blink: !matches!(settings.cursor_blink, CursorBlink::Off),
                shape: match settings.cursor_shape {
                    CursorShape::Block => zqlz_text_editor::CursorShape::Block,
                    CursorShape::Line => zqlz_text_editor::CursorShape::Bar,
                    CursorShape::Underline => zqlz_text_editor::CursorShape::Underline,
                },
            },
            highlight_current_line: false,
            highlight_selection_matches: settings.selection_highlight,
            gutter: GutterSettings {
                show_line_numbers: true,
                show_relative_line_numbers: settings.relative_line_numbers,
                show_gutter: true,
                show_fold_controls: settings.show_folding,
            },
            show_diagnostics: settings.show_inline_diagnostics || settings.show_gutter_diagnostics,
            show_folding: settings.show_folding,
            soft_wrap: if settings.word_wrap {
                SoftWrapMode::EditorWidth
            } else {
                SoftWrapMode::None
            },
            document: DocumentSettings {
                indent_size: settings.tab_size.max(1) as usize,
                use_tabs: !settings.insert_spaces,
            },
            search: SearchSettings {
                case_sensitive: false,
                whole_word: false,
                use_regex: false,
                search_in_selection: false,
            },
            hover_delay: std::time::Duration::from_millis(settings.hover_delay_ms as u64),
            completion: CompletionSettings {
                automatically_show: settings.lsp_enabled && settings.lsp_completions_enabled,
                accept_on_enter: true,
                commit_characters: true,
            },
            scroll: ScrollSettings {
                vertical_margin_lines: settings.vertical_scroll_margin as usize,
                horizontal_margin_columns: settings.horizontal_scroll_margin as usize,
                sensitivity: (settings.scroll_sensitivity.max(0.1) * 100.0).round() as u16,
                scroll_beyond_last_line: !matches!(
                    settings.scroll_beyond_last_line,
                    ScrollBeyondLastLine::Disabled
                ),
            },
        }
    }

    fn format_provider_for_current_context(&self) -> Option<FormatProvider> {
        let driver_type = self.driver_type.clone()?;
        formatter_provider_for_driver(&driver_type)?;

        let object_type = self.object_type.display_name().to_string();
        Some(Rc::new(move |source| {
            let formatter = formatter_provider_for_driver(&driver_type)?;
            let request = FormatRequest::new(source.to_string(), driver_type.clone())
                .with_object_type(object_type.clone());
            formatter
                .format(&request)
                .ok()
                .map(|outcome| outcome.source)
        }))
    }

    fn apply_format_provider_to_editor(&self, cx: &mut Context<Self>) {
        let settings = ZqlzSettings::global(cx).editor.clone();
        let language_providers = self.language_providers_for_current_context(&settings);
        self.editor.update(cx, |editor, cx| {
            editor.set_language_providers(language_providers, cx);
        });
    }

    fn language_providers_for_current_context(
        &self,
        settings: &AppEditorSettings,
    ) -> EditorLanguageProviders {
        let supports_sql_lsp = self
            .driver_type
            .as_deref()
            .map(driver_category_from_driver_name)
            .is_none_or(|category| matches!(category, DriverCategory::Relational));

        if !supports_sql_lsp || !settings.lsp_enabled {
            return EditorLanguageProviders {
                format: self.format_provider_for_current_context(),
                ..EditorLanguageProviders::default()
            };
        }

        EditorLanguageProviders {
            completion: settings.lsp_completions_enabled.then(|| {
                Rc::new(SqlLspCompletionAdapter::new(self.sql_lsp.clone()))
                    as Rc<dyn zqlz_text_editor::CompletionProvider>
            }),
            hover: settings.lsp_hover_enabled.then(|| {
                Rc::new(SqlLspHoverAdapter::new(self.sql_lsp.clone()))
                    as Rc<dyn zqlz_text_editor::HoverProvider>
            }),
            definition: Some(Rc::new(SqlLspDefinitionAdapter::new(self.sql_lsp.clone()))
                as Rc<dyn zqlz_text_editor::DefinitionProvider>),
            references: Some(Rc::new(SqlLspReferencesAdapter::new(self.sql_lsp.clone()))
                as Rc<dyn zqlz_text_editor::ReferencesProvider>),
            rename: settings.lsp_rename_enabled.then(|| {
                Rc::new(SqlLspRenameAdapter::new(self.sql_lsp.clone()))
                    as Rc<dyn zqlz_text_editor::RenameProvider>
            }),
            code_actions: settings.lsp_code_actions_enabled.then(|| {
                Rc::new(SqlLspCodeActionAdapter::new(self.sql_lsp.clone()))
                    as Rc<dyn zqlz_text_editor::CodeActionProvider>
            }),
            diagnostics: settings.lsp_diagnostics_enabled.then(|| {
                Rc::new(SqlLspDiagnosticAdapter::new(self.sql_lsp.clone()))
                    as Rc<dyn zqlz_text_editor::DiagnosticProvider>
            }),
            format: self.format_provider_for_current_context(),
        }
    }

    fn sync_lsp_settings(&mut self, cx: &mut Context<Self>) {
        let settings = ZqlzSettings::global(cx).editor.clone();
        let language_providers = self.language_providers_for_current_context(&settings);

        self.editor.update(cx, |text_editor, cx| {
            Self::apply_editor_settings(text_editor, &settings, cx);
            text_editor.set_language_providers(language_providers, cx);
        });

        self.template_params.update(cx, |text_editor, cx| {
            Self::apply_editor_settings(text_editor, &settings, cx);
        });

        if !settings.lsp_enabled || !settings.lsp_diagnostics_enabled {
            self.editor.update(cx, |editor, cx| {
                editor.set_lsp_diagnostics(Vec::new(), cx);
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
            can_cancel_execution: false,
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
            current_schema: None,
            available_schemas: Vec::new(),
            _diagnostics_debounce: None,
            _last_diagnostics_text: Some(initial_text.to_string()),
            _subscriptions,
        };

        query_editor.apply_format_provider_to_editor(cx);
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
            can_cancel_execution: false,
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
            current_schema: None,
            available_schemas: Vec::new(),
            _diagnostics_debounce: None,
            _last_diagnostics_text: Some(initial_text.to_string()),
            _subscriptions,
        };

        query_editor.apply_format_provider_to_editor(cx);
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
            can_cancel_execution: false,
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
            current_schema: None,
            available_schemas: Vec::new(),
            _diagnostics_debounce: None,
            _last_diagnostics_text: Some(initial_text.to_string()),
            _subscriptions,
        };

        query_editor.apply_format_provider_to_editor(cx);
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
        self.can_cancel_execution = connection
            .as_ref()
            .is_some_and(|connection| connection.cancel_handle().is_some());
        self.apply_format_provider_to_editor(cx);
        self.clear_schema_dependent_ui_state();
        self.current_schema = None;
        self.available_schemas.clear();
        let dialect_language = driver_type_to_highlight_language(driver_type.as_deref());
        self.editor.update(cx, |editor, cx| {
            editor.set_syntax_language_profile(dialect_language, cx);
            editor.clear_code_actions(cx);
        });

        // Update SQL LSP with new connection and driver type
        {
            let mut lsp = self.sql_lsp.write();
            lsp.set_connection(connection_id, connection.clone(), driver_type.clone());
            lsp.set_active_database(self.current_database.clone());
            lsp.set_active_schema(self.current_schema.clone());

            // If a cache was persisted from the last session, apply it immediately so
            // completions work from the first keystroke.  The background refresh below
            // always runs regardless (stale-while-revalidate).
            if let Some(conn_id) = connection_id {
                let scope = schema_cache_scope(
                    self.current_database.as_deref(),
                    self.current_schema.as_deref(),
                );
                if let Some(cached) = load_schema_cache_from_disk(conn_id, scope.as_deref()) {
                    lsp.apply_schema_cache(cached);
                }
            }
        }
        self.update_schema_selector_from_lsp(cx);

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

            let (active_database_for_refresh, active_schema_for_refresh) = {
                let guard = lsp.read();
                (guard.active_database(), guard.active_schema())
            };

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
                        let active_schema = active_schema_for_refresh.clone();
                        let fetch_result = cx
                            .background_spawn({
                                let schema_service: Arc<SchemaService> = schema_service.clone();
                                let conn: Arc<dyn Connection> = connection_for_refresh.clone();
                                async move {
                                    SqlLsp::fetch_schema_cache(
                                        conn,
                                        connection_id_for_refresh,
                                        active_database,
                                        active_schema,
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
                            let disk_scope = schema_cache_scope(
                                active_database_for_refresh.as_deref(),
                                active_schema_for_refresh.as_deref(),
                            );
                            save_schema_cache_to_disk(
                                connection_id_for_refresh,
                                disk_scope.as_deref(),
                                &cache,
                            );
                            lsp.write().apply_schema_cache_if_current(cache, epoch);
                            tracing::debug!("SQL schema refreshed successfully");
                _ = this.update(cx, |editor, cx| {
                    editor.update_schema_selector_from_lsp(cx);
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

    /// Refresh editor and provider behavior from current app settings.
    pub fn refresh_settings(&mut self, cx: &mut Context<Self>) {
        self.sync_lsp_settings(cx);
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
        self.clear_schema_dependent_ui_state();
        if let Some(conn_id) = self.connection_id {
            let scope = schema_cache_scope(
                self.current_database.as_deref(),
                self.current_schema.as_deref(),
            );
            if let Some(path) = schema_cache_path(conn_id, scope.as_deref()) {
                std::fs::remove_file(path).ok();
            }
        }
        // Mark schema as loading so completions don't surface stale objects
        // (e.g. a just-dropped table) during the background re-fetch window.
        self.sql_lsp.write().schema_loading = true;
        self.trigger_lsp_schema_refresh(cx);
    }

    /// Re-fetches the full schema cache in the background and applies the result without
    /// interrupting existing completions (does NOT set `schema_loading = true`).
    ///
    /// Applies a silent schema refresh after DDL so the LSP stops using stale objects.
    /// Table details already present in `SchemaService` are reused without DB prefetch.
    pub fn trigger_lsp_schema_refresh(&mut self, cx: &mut Context<Self>) {
        let (connection, connection_id, schema_service, active_database, active_schema) = {
            let guard = self.sql_lsp.read();
            (
                guard.connection(),
                guard.connection_id(),
                guard.schema_service(),
                guard.active_database(),
                guard.active_schema(),
            )
        };

        let (Some(connection), Some(connection_id)) = (connection, connection_id) else {
            return;
        };

        let lsp = self.sql_lsp.clone();
        let epoch = lsp.write().next_fetch_epoch();
        let active_database_for_disk = active_database.clone();
        let active_schema_for_disk = active_schema.clone();
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
                            active_schema.clone(),
                            &schema_service,
                        )
                        .await
                    }
                })
                .await;

            match result {
                Ok(cache) => {
                    let disk_scope = schema_cache_scope(
                        active_database_for_disk.as_deref(),
                        active_schema_for_disk.as_deref(),
                    );
                    save_schema_cache_to_disk(connection_id, disk_scope.as_deref(), &cache);
                    lsp.write().apply_schema_cache_if_current(cache, epoch);
                    tracing::debug!("Schema cache refreshed after prefetch completion");
                    _ = _this.update(cx, |this, cx| {
                        this.update_schema_selector_from_lsp(cx);
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
        self.current_schema = None;
        self.clear_schema_dependent_ui_state();
        {
            let mut lsp = self.sql_lsp.write();
            lsp.set_active_database(self.current_database.clone());
            lsp.set_active_schema(None);
            lsp.schema_loading = true;
        }
        self.update_schema_selector_from_lsp(cx);
        self.trigger_lsp_schema_refresh(cx);
        cx.notify();
    }

    /// Start a database switch while caller resolves the physical connection.
    pub fn begin_current_database_switch(
        &mut self,
        database: Option<String>,
        cx: &mut Context<Self>,
    ) {
        self.current_database = database;
        self.current_schema = None;
        self.clear_schema_dependent_ui_state();
        self.available_schemas.clear();
        {
            let mut lsp = self.sql_lsp.write();
            lsp.set_active_database(self.current_database.clone());
            lsp.set_active_schema(None);
            lsp.schema_loading = true;
        }
        cx.notify();
    }

    pub fn set_schema_loading(&mut self, loading: bool, cx: &mut Context<Self>) {
        self.sql_lsp.write().schema_loading = loading;
        cx.notify();
    }

    /// Get the selected database label for stale async result guards.
    pub fn current_database(&self) -> Option<String> {
        self.current_database.clone()
    }

    /// Set the SQL content
    pub fn set_content(&mut self, content: String, window: &mut Window, cx: &mut Context<Self>) {
        self.editor.update(cx, |editor, cx| {
            editor.clear_inline_suggestion(cx);
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
            self.editor.update(cx, |editor, cx| {
                editor.set_lsp_diagnostics(Vec::new(), cx);
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
            self.editor.update(cx, |editor, cx| {
                editor.set_lsp_diagnostics(Vec::new(), cx);
            });
            cx.emit(QueryEditorEvent::DiagnosticsChanged {
                diagnostics: Vec::new(),
            });
            cx.notify();
            return;
        }

        self._diagnostics_debounce = None;

        self._diagnostics_debounce = Some(cx.spawn_in(window, async move |this, cx| {
            cx.background_executor()
                .timer(std::time::Duration::from_millis(300))
                .await;

            let Some((text_content, diagnostics_task)) = this
                .update_in(cx, |this, _window, cx| {
                    let text_content = this.editor.read(cx).get_text(cx).to_string();
                    let diagnostics_task = this.editor.read(cx).request_lsp_diagnostics(cx)?;
                    Some((text_content, diagnostics_task))
                })
                .ok()
                .flatten()
            else {
                return;
            };

            let Ok(lsp_diagnostics) = diagnostics_task.await else {
                return;
            };

            let _ = this.update_in(cx, |this, _window, cx| {
                this.apply_lsp_diagnostics(text_content, lsp_diagnostics, cx);
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
            self._diagnostics_debounce = None;
            self.editor.update(cx, |editor, cx| {
                editor.set_lsp_diagnostics(Vec::new(), cx);
            });
            cx.emit(QueryEditorEvent::DiagnosticsChanged {
                diagnostics: Vec::new(),
            });
            cx.notify();
            return;
        }

        if !self.editor.read(cx).diagnostics_enabled() {
            self._diagnostics_debounce = None;
            self.editor.update(cx, |editor, cx| {
                editor.set_lsp_diagnostics(Vec::new(), cx);
            });
            cx.emit(QueryEditorEvent::DiagnosticsChanged {
                diagnostics: Vec::new(),
            });
            cx.notify();
            return;
        }

        let text_content = self.editor.read(cx).get_text(cx).to_string();
        let Some(diagnostics_task) = self.editor.read(cx).request_lsp_diagnostics(cx) else {
            self.editor.update(cx, |editor, cx| {
                editor.set_lsp_diagnostics(Vec::new(), cx);
            });
            cx.emit(QueryEditorEvent::DiagnosticsChanged {
                diagnostics: Vec::new(),
            });
            cx.notify();
            return;
        };

        self._diagnostics_debounce = Some(cx.spawn(async move |this, cx| {
            let Ok(lsp_diagnostics) = diagnostics_task.await else {
                return;
            };

            let _ = this.update(cx, |this, cx| {
                this.apply_lsp_diagnostics(text_content, lsp_diagnostics, cx);
            });
        }));
    }

    fn apply_lsp_diagnostics(
        &mut self,
        text_content: String,
        lsp_diagnostics: Vec<lsp_types::Diagnostic>,
        cx: &mut Context<Self>,
    ) {
        self.editor.update(cx, |editor, cx| {
            editor.set_lsp_diagnostics(lsp_diagnostics.clone(), cx);
        });
        self._last_diagnostics_text = Some(text_content);
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

    pub fn document_symbols(&self, cx: &App) -> Vec<QueryDocumentSymbol> {
        query_document_symbols(self.content(cx).as_ref())
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

        self.editor.read(cx).lsp_diagnostic_counts()
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

        self.editor
            .read(cx)
            .lsp_diagnostics()
            .iter()
            .cloned()
            .map(Into::into)
            .collect()
    }

    fn navigate_problem(&mut self, forward: bool, window: &mut Window, cx: &mut Context<Self>) {
        let focus_handle = self.editor.read(cx).focus_handle(cx);
        focus_handle.focus(window, cx);
        self.editor.update(cx, |editor, cx| {
            editor.navigate_lsp_diagnostic(forward, window, cx);
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
        self.editor.update(cx, |editor, cx| {
            editor.clear_inline_suggestion(cx);
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

    fn focus_inner_editor(&self, window: &mut Window, cx: &mut App) {
        let focus_handle = self.editor.read(cx).focus_handle(cx);
        focus_handle.focus(window, cx);
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
            database_name: self.current_database.clone(),
            params: self.execution_params(cx),
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
            database_name: self.current_database.clone(),
            params: self.execution_params(cx),
        });
    }

    /// Called when query execution completes
    pub fn set_executing(&mut self, executing: bool, cx: &mut Context<Self>) {
        self.is_executing = executing;
        cx.notify();
    }

    /// Emit cancel query event
    fn emit_cancel_query(&mut self, cx: &mut Context<Self>) {
        if self.is_executing && self.can_cancel_execution {
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
            database_name: self.current_database.clone(),
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
            database_name: self.current_database.clone(),
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

    fn emit_preview_ddl(&mut self, cx: &mut Context<Self>) {
        if !self.object_type.supports_save() {
            tracing::warn!("Cannot preview DDL: object type does not support save");
            return;
        }

        let definition = self.content(cx).to_string();
        if definition.trim().is_empty() {
            tracing::warn!("Cannot preview DDL: empty definition");
            return;
        }

        cx.emit(QueryEditorEvent::PreviewDdl {
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
        self.apply_format_provider_to_editor(cx);
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

    /// Handle ShowHover action - asks the central editor hover provider for cursor docs.
    fn handle_show_hover(
        &mut self,
        _action: &ShowHover,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_hover_enabled {
            self.editor.update(cx, |editor, _editor_cx| {
                editor.clear_hover();
            });
            cx.notify();
            return;
        }

        self.editor.update(cx, |editor, cx| {
            editor.update_hover_at_cursor(window, cx);
        });
    }

    /// Clear editor overlays owned by the central TextEditor.
    pub fn clear_hover(&mut self, cx: &mut Context<Self>) {
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
        self.format_query(window, cx);
    }

    /// Format the SQL query using production-level formatter
    fn format_query(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.editor.update(cx, |editor, cx| {
            editor.format_sql(cx);
        });
        self.update_diagnostics(cx);
        self._last_diagnostics_text = Some(self.content(cx).to_string());
        cx.notify();
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

    fn render_toolbar(&self, _window: &Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();
        let is_empty = self.is_content_empty(cx);
        let has_template_error = self.template_error.is_some();
        let supports_save = self.object_type.supports_save();
        let has_connection = self.connection_id.is_some();
        let save_disabled = if supports_save {
            !has_connection || is_empty || !self.editor.read(cx).is_dirty()
        } else {
            !has_connection || is_empty
        };
        let execute_disabled = self.is_executing || is_empty || has_template_error;
        let run_target_preview = self.run_target_preview(cx);

        v_flex()
            .id("query-editor-toolbar")
            .on_action(cx.listener(Self::handle_format_query))
            .on_action(cx.listener(Self::handle_save_query))
            .on_action(cx.listener(Self::handle_show_hover))
            .on_action(cx.listener(Self::handle_trigger_parameter_hints))
            .on_action(cx.listener(Self::handle_show_code_actions))
            .w_full()
            .h(px(42.0))
            .border_b_1()
            .border_color(theme.border.opacity(0.65))
            .bg(theme.background)
            .child(
                h_flex()
                    .w_full()
                    .h(px(40.0))
                    .px_2()
                    .gap_2()
                    .items_center()
                    .child(
                        Button::new("save-query")
                            .ghost()
                            .xsmall()
                            .icon(ZqlzIcon::FloppyDisk)
                            .tooltip("Save")
                            .disabled(save_disabled)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.handle_save_query(&SaveQuery, window, cx);
                                this.focus_inner_editor(window, cx);
                            })),
                    )
                    .child(
                        Button::new("format")
                            .ghost()
                            .xsmall()
                            .icon(ZqlzIcon::TextIndent)
                            .tooltip("Format")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.format_query(window, cx);
                                this.focus_inner_editor(window, cx);
                            })),
                    )
                    .child(
                        Button::new("assist")
                            .ghost()
                            .xsmall()
                            .icon(ZqlzIcon::MagicWand)
                            .tooltip("Assist")
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.handle_show_code_actions(&ShowCodeActions, window, cx);
                                this.focus_inner_editor(window, cx);
                            })),
                    )
                    .child(
                        Button::new("refresh-schema")
                            .ghost()
                            .xsmall()
                            .icon(ZqlzIcon::ArrowsClockwise)
                            .tooltip("Refresh Schema")
                            .disabled(!has_connection)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.trigger_lsp_schema_refresh(cx);
                                this.focus_inner_editor(window, cx);
                            })),
                    )
                    .child(div().w(px(1.0)).h_5().bg(theme.border.opacity(0.65)))
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
                                .xsmall()
                                .ghost()
                                .icon(ZqlzIcon::Plug)
                                .tooltip(connection_label.clone())
                                .w(px(220.0))
                                .flex_shrink()
                                .child(Self::selector_label(connection_label))
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
                                                        cx.emit(
                                                            QueryEditorEvent::SwitchConnection {
                                                                connection_id: conn_id,
                                                            },
                                                        );
                                                    });
                                                }),
                                        );
                                    }
                                    menu
                                }),
                        )
                    })
                    .when(self.available_databases.len() > 1, |this| {
                        let database_label = self
                            .current_database
                            .clone()
                            .unwrap_or_else(|| "Select Database".to_string());
                        let available_databases = self.available_databases.clone();
                        let current_database = self.current_database.clone();
                        let entity = cx.entity().downgrade();

                        this.child(
                            Button::new("database-switcher")
                                .xsmall()
                                .ghost()
                                .icon(ZqlzIcon::Database)
                                .tooltip(database_label.clone())
                                .w(px(180.0))
                                .flex_shrink()
                                .child(Self::selector_label(database_label))
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
                    .when(!self.available_schemas.is_empty(), |this| {
                        let schema_label = self
                            .current_schema
                            .clone()
                            .unwrap_or_else(|| "Select Schema".to_string());
                        let available_schemas = self.available_schemas.clone();
                        let current_schema = self.current_schema.clone();
                        let entity = cx.entity().downgrade();

                        this.child(
                            Button::new("schema-switcher")
                                .xsmall()
                                .ghost()
                                .icon(ZqlzIcon::Stack)
                                .tooltip(schema_label.clone())
                                .w(px(180.0))
                                .flex_shrink()
                                .child(Self::selector_label(schema_label))
                                .dropdown_menu(move |mut menu, _window, _cx| {
                                    use zqlz_ui::widgets::menu::PopupMenuItem;
                                    menu = menu.max_h(px(300.0)).scrollable(true);
                                    for schema_name in &available_schemas {
                                        let is_current = current_schema.as_ref() == Some(schema_name);
                                        let schema_name_clone = schema_name.clone();
                                        let entity = entity.clone();
                                        menu = menu.item(
                                            PopupMenuItem::new(schema_name_clone.clone())
                                                .checked(is_current)
                                                .on_click(move |_event, _window, cx| {
                                                    _ = entity.update(cx, |this, cx| {
                                                        this.current_schema =
                                                            Some(schema_name_clone.clone());
                                                        this.clear_schema_dependent_ui_state();
                                                        {
                                                            let mut lsp = this.sql_lsp.write();
                                                            lsp.set_active_schema(
                                                                this.current_schema.clone(),
                                                            );
                                                            lsp.schema_loading = true;
                                                        }
                                                        this.trigger_lsp_schema_refresh(cx);
                                                        cx.notify();
                                                    });
                                                }),
                                        );
                                    }
                                    menu
                                }),
                        )
                    })
                    .child(
                        DropdownButton::new("execute-menu")
                            .button(
                                Button::new("execute")
                                    .primary()
                                    .xsmall()
                                    .icon(ZqlzIcon::Play)
                                    .label("Run")
                                    .tooltip("Run")
                                    .disabled(execute_disabled)
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.emit_execute_query(cx);
                                        this.focus_inner_editor(window, cx);
                                    })),
                            )
                            .dropdown_menu({
                                let entity = cx.entity().downgrade();
                                let run_target_preview = run_target_preview.clone();
                                move |mut menu, _window, _cx| {
                                    use zqlz_ui::widgets::menu::PopupMenuItem;
                                    menu = menu.min_w(px(320.0)).max_w(px(420.0));
                                    for entry in QueryEditor::run_menu_entries() {
                                        if entry.separator_before {
                                            menu = menu.item(PopupMenuItem::separator());
                                        }
                                        let entity = entity.clone();
                                        let action = entry.action;
                                        let disabled = execute_disabled || entry.disabled;
                                        let item = if action
                                            == QueryEditorRunMenuAction::RunCurrentStatement
                                        {
                                            let preview = run_target_preview.clone();
                                            PopupMenuItem::element(move |_window, cx| {
                                                let theme = cx.theme().clone();
                                                v_flex()
                                                    .w_full()
                                                    .py_1()
                                                    .gap_0p5()
                                                    .child(
                                                        h_flex()
                                                            .w_full()
                                                            .items_center()
                                                            .justify_between()
                                                            .gap_2()
                                                            .child(
                                                                div()
                                                                    .flex_none()
                                                                    .font_weight(
                                                                        gpui::FontWeight::MEDIUM,
                                                                    )
                                                                    .child(preview.label.clone()),
                                                            )
                                                            .child(
                                                                div()
                                                                    .flex_none()
                                                                    .text_xs()
                                                                    .text_color(
                                                                        theme.muted_foreground,
                                                                    )
                                                                    .child(preview.detail.clone()),
                                                            ),
                                                    )
                                                    .child(
                                                        div()
                                                            .w_full()
                                                            .min_w_0()
                                                            .truncate()
                                                            .text_xs()
                                                            .text_color(theme.muted_foreground)
                                                            .child(preview.preview.clone()),
                                                    )
                                            })
                                            .icon(Icon::new(ZqlzIcon::Play).size_3())
                                        } else {
                                            PopupMenuItem::new(entry.label)
                                        };
                                        let item = item
                                            .checked(entry.checked)
                                            .disabled(disabled)
                                            .on_click(move |_event, _window, cx| {
                                                _ = entity.update(cx, |this, cx| match action {
                                                    QueryEditorRunMenuAction::Run => {
                                                        this.emit_execute_query(cx);
                                                    }
                                                    QueryEditorRunMenuAction::RunCurrentStatement => {
                                                        this.emit_execute_selection(cx);
                                                    }
                                                    QueryEditorRunMenuAction::ContinueOnError => {}
                                                });
                                            });
                                        menu = menu.item(item);
                                    }
                                    menu
                                }
                            })
                            .primary()
                            .xsmall()
                            .disabled(execute_disabled),
                    )
                    .child(
                        Button::new("stop")
                            .ghost()
                            .xsmall()
                            .icon(ZqlzIcon::Stop)
                            .tooltip("Stop")
                            .disabled(!(self.is_executing && self.can_cancel_execution))
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.emit_cancel_query(cx);
                                this.focus_inner_editor(window, cx);
                            })),
                    )
                    .child(
                        Button::new("explain")
                            .ghost()
                            .xsmall()
                            .icon(ZqlzIcon::Lightbulb)
                            .tooltip("Explain")
                            .disabled(execute_disabled)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.emit_explain_query(cx);
                                this.focus_inner_editor(window, cx);
                            })),
                    )
                    .when(supports_save, |this| {
                        this.child(
                            Button::new("preview-ddl")
                                .ghost()
                                .xsmall()
                                .icon(ZqlzIcon::Eye)
                                .tooltip("Preview DDL")
                                .disabled(is_empty)
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.emit_preview_ddl(cx);
                                    this.focus_inner_editor(window, cx);
                                })),
                        )
                    }),
            )
    }
    /// Render the SQL editor area using the custom TextEditor
    fn render_editor(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let fonts = ZqlzSettings::global(cx).fonts.clone();
        div()
            .size_full()
            .text_size(px(fonts.editor_font_size))
            .font_family(fonts.editor_font_family.clone())
            .font_weight(gpui::FontWeight::from(fonts.editor_font_weight as f32))
            .child(self.editor.clone())
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

        let is_template_mode = self.editor_mode == EditorMode::Template;

        v_flex()
            .id("query-editor")
            // Track the QueryEditor's own handle, not the TextEditor's. Focus is
            // forwarded to the inner editor on click (see on_click below).
            .track_focus(&self.focus_handle)
            .key_context("Editor")
            .size_full()
            .bg(bg_color)
            .on_action(cx.listener(Self::handle_next_problem))
            .on_action(cx.listener(Self::handle_previous_problem))
            .on_key_down(cx.listener(|this, event: &gpui::KeyDownEvent, window, cx| {
                let key = event.keystroke.key.as_str();
                let modifiers = event.keystroke.modifiers;

                let is_manual_completion_shortcut = key == "."
                    && !modifiers.shift
                    && !modifiers.alt
                    && (modifiers.platform || modifiers.control);

                if is_manual_completion_shortcut && !this.is_executing {
                    this.editor.update(cx, |editor, cx| {
                        editor.clear_inline_suggestion(cx);
                        editor.trigger_completion_action(window, cx);
                    });
                    window.prevent_default();
                    cx.stop_propagation();
                    return;
                }

                // Query execution shortcuts:
                // - Cmd+Enter and F5 execute the current query
                // - Cmd/Ctrl+Shift+Enter executes selection/current statement
                // - Escape / Cmd+Escape cancels the running query via app keymap
                if (this.is_executing
                    && this.can_cancel_execution
                    && key == "escape"
                    && !modifiers.shift
                    && !modifiers.alt)
                    || (this.is_executing
                        && this.can_cancel_execution
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
                    this.editor.update(cx, |editor, cx| {
                        editor.clear_inline_suggestion(cx);
                    });
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
                            .child(self.render_editor(cx)),
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

    fn tab_name(&self, _cx: &App) -> Option<SharedString> {
        Some(self.name.clone().into())
    }

    fn tab_icon(&self, _cx: &App) -> Option<ZqlzIcon> {
        Some(ZqlzIcon::FileSql)
    }

    fn tab_tooltip(&self, _cx: &App) -> Option<SharedString> {
        let mut parts = vec![self.name.clone()];
        if let Some(connection_name) = &self.connection_name {
            parts.push(connection_name.clone());
        }
        if let Some(database) = &self.current_database {
            parts.push(database.clone());
        }
        if let Some(schema) = &self.current_schema {
            parts.push(schema.clone());
        }

        Some(parts.join(" / ").into())
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        true
    }

    fn can_split(&self, _cx: &App) -> bool {
        true
    }

    fn can_move_to_new_window(&self, _cx: &App) -> bool {
        true
    }

    fn has_unsaved_changes(&self, _cx: &App) -> bool {
        self.editor.read(_cx).is_dirty()
    }
}

fn schema_cache_scope(database: Option<&str>, schema: Option<&str>) -> Option<String> {
    let database = database.map(str::trim).filter(|value| !value.is_empty());
    let schema = schema.map(str::trim).filter(|value| !value.is_empty());

    match (database, schema) {
        (Some(database), Some(schema)) => Some(format!("db:{database}|schema:{schema}")),
        (Some(database), None) => Some(format!("db:{database}")),
        (None, Some(schema)) => Some(format!("schema:{schema}")),
        (None, None) => None,
    }
}

/// Returns the path where the schema cache for a given connection is persisted on disk.
///
/// Layout: `~/.config/zqlz/schema_cache/{connection_id}.json`
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

    zqlz_core::paths::schema_cache_dir()
        .ok()
        .map(|base| base.join(format!("{connection_id}_{scope_hash:016x}.json")))
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
    use super::{
        AppEditorSettings, CursorBlink, CursorShape, DiagnosticInfo, DiagnosticInfoSeverity,
        EditorMode, QueryDocumentSymbol, QueryEditor, QueryEditorRunMenuAction,
        QueryEditorStatusInput, ScrollBeyondLastLine, SoftWrapMode, query_document_symbols,
        schema_cache_scope,
    };
    use lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};
    use std::path::Path;
    use zqlz_text_editor::{DocumentIdentity, TextEditor};

    #[test]
    fn problem_index_forward_wraps_to_start() {
        let problems = vec![(2, 0), (4, 5), (10, 1)];
        assert_eq!(
            TextEditor::lsp_diagnostic_index_for_position(&problems, 10, 2, true),
            Some(0)
        );
    }

    #[test]
    fn problem_index_forward_picks_next() {
        let problems = vec![(2, 0), (4, 5), (10, 1)];
        assert_eq!(
            TextEditor::lsp_diagnostic_index_for_position(&problems, 2, 0, true),
            Some(1)
        );
    }

    #[test]
    fn problem_index_backward_wraps_to_end() {
        let problems = vec![(2, 0), (4, 5), (10, 1)];
        assert_eq!(
            TextEditor::lsp_diagnostic_index_for_position(&problems, 1, 0, false),
            Some(2)
        );
    }

    #[test]
    fn problem_index_backward_picks_previous() {
        let problems = vec![(2, 0), (4, 5), (10, 1)];
        assert_eq!(
            TextEditor::lsp_diagnostic_index_for_position(&problems, 8, 0, false),
            Some(1)
        );
    }

    #[test]
    fn internal_text_document_starts_with_internal_identity() {
        let document = QueryEditor::internal_text_document("select 1");

        assert_eq!(document.text(), "select 1");
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
            TextEditor::lsp_diagnostic_counts_from(&diagnostics),
            (1, 1, 2)
        );
    }

    #[test]
    fn status_labels_show_cursor_context_and_empty_connection_state() {
        let labels = QueryEditor::status_labels_from_parts(QueryEditorStatusInput {
            cursor_line: 2,
            cursor_column: 4,
            selection_chars: Some(12),
            mode: EditorMode::Sql,
            connection_name: None,
            has_connection: false,
            database: None,
            diagnostics: (0, 0, 0),
            is_executing: false,
        });

        assert_eq!(labels.cursor, "Ln 3, Col 5");
        assert_eq!(labels.selection.as_deref(), Some("12 selected"));
        assert_eq!(labels.mode, "SQL");
        assert_eq!(labels.connection, "No Connection");
        assert_eq!(labels.database, "No Database");
        assert_eq!(labels.diagnostics, "No Problems");
        assert_eq!(labels.execution, None);
    }

    #[test]
    fn status_labels_prioritize_errors_and_running_state() {
        let labels = QueryEditor::status_labels_from_parts(QueryEditorStatusInput {
            cursor_line: 0,
            cursor_column: 0,
            selection_chars: None,
            mode: EditorMode::Template,
            connection_name: Some("postgres@localhost"),
            has_connection: true,
            database: Some("erp_lab"),
            diagnostics: (1, 3, 2),
            is_executing: true,
        });

        assert_eq!(labels.cursor, "Ln 1, Col 1");
        assert_eq!(labels.selection, None);
        assert_eq!(labels.mode, "Template");
        assert_eq!(labels.connection, "postgres@localhost");
        assert_eq!(labels.database, "erp_lab");
        assert_eq!(labels.diagnostics, "1 error");
        assert_eq!(labels.execution.as_deref(), Some("Running"));
    }

    #[test]
    fn run_menu_entries_match_navicat_v1_actions() {
        let entries = QueryEditor::run_menu_entries();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].label, "Run");
        assert_eq!(entries[0].action, QueryEditorRunMenuAction::Run);
        assert!(!entries[0].checked);
        assert!(!entries[0].disabled);
        assert!(!entries[0].separator_before);
        assert_eq!(entries[1].label, "Run Current Statement");
        assert_eq!(
            entries[1].action,
            QueryEditorRunMenuAction::RunCurrentStatement
        );
        assert_eq!(entries[2].label, "Continue on Error");
        assert_eq!(entries[2].action, QueryEditorRunMenuAction::ContinueOnError);
        assert!(entries[2].checked);
        assert!(entries[2].disabled);
        assert!(entries[2].separator_before);
    }

    #[test]
    fn run_target_preview_describes_selected_sql() {
        let preview = QueryEditor::run_target_preview_from_parts(
            "select 1;",
            Some("select *\nfrom customers"),
            0,
            EditorMode::Sql,
        );

        assert_eq!(preview.label, "Run Selection");
        assert!(preview.is_selection);
        assert_eq!(preview.detail, "2 lines");
        assert_eq!(preview.preview, "select * from customers");
    }

    #[test]
    fn run_target_preview_describes_current_statement() {
        let sql = "select 1;\n\nselect * from accounts where id = 1;\nselect 3;";
        let cursor_offset = sql.find("accounts").expect("cursor target");
        let preview =
            QueryEditor::run_target_preview_from_parts(sql, None, cursor_offset, EditorMode::Sql);

        assert_eq!(preview.label, "Run Current Statement");
        assert!(!preview.is_selection);
        assert!(preview.detail.contains("Ln 3"));
        assert!(preview.preview.contains("accounts"));
        assert!(!preview.preview.contains("select 3"));
    }

    #[test]
    fn run_target_preview_describes_rendered_template_without_selection() {
        let preview = QueryEditor::run_target_preview_from_parts(
            "select * from {{ table_name }}",
            None,
            0,
            EditorMode::Template,
        );

        assert_eq!(preview.label, "Run Rendered Template");
        assert_eq!(preview.detail, "1 line");
        assert_eq!(preview.preview, "select * from {{ table_name }}");
    }

    #[test]
    fn query_header_hides_mode_problems_template_and_keymap_noise() {
        let visibility = QueryEditor::header_visibility();

        assert!(!visibility.show_mode_segment);
        assert!(!visibility.show_problems_badge);
        assert!(!visibility.show_template_error);
        assert!(!visibility.show_keymap_hints);
        assert!(!visibility.show_repeated_connection_label);
    }

    #[test]
    fn schema_labels_prefer_schema_names_then_table_schemas() {
        assert_eq!(
            QueryEditor::schema_labels_from_parts(
                vec![
                    "public".to_string(),
                    " ".to_string(),
                    "sales".to_string(),
                    "public".to_string()
                ],
                vec![Some("ignored".to_string())],
            ),
            vec!["public".to_string(), "sales".to_string()]
        );

        assert_eq!(
            QueryEditor::schema_labels_from_parts(
                Vec::<String>::new(),
                vec![
                    Some("public".to_string()),
                    None,
                    Some("sales".to_string()),
                    Some("public".to_string()),
                    Some("".to_string()),
                ],
            ),
            vec!["public".to_string(), "sales".to_string()]
        );

        assert!(QueryEditor::schema_labels_from_parts(Vec::<String>::new(), vec![None]).is_empty());
    }

    #[test]
    fn schema_cache_scope_keeps_database_and_schema_snapshots_separate() {
        assert_eq!(
            schema_cache_scope(Some("erp_lab"), Some("zqlz_audit")).as_deref(),
            Some("db:erp_lab|schema:zqlz_audit")
        );
        assert_eq!(
            schema_cache_scope(Some("erp_lab"), None).as_deref(),
            Some("db:erp_lab")
        );
        assert_eq!(
            schema_cache_scope(None, Some("public")).as_deref(),
            Some("schema:public")
        );
        assert_eq!(schema_cache_scope(Some(" "), Some("")), None);
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

    #[test]
    fn app_editor_settings_map_to_text_editor_settings() {
        let app_settings = AppEditorSettings {
            tab_size: 2,
            insert_spaces: false,
            show_line_numbers: false,
            word_wrap: true,
            cursor_blink: CursorBlink::Off,
            cursor_shape: CursorShape::Underline,
            selection_highlight: false,
            relative_line_numbers: true,
            scroll_beyond_last_line: ScrollBeyondLastLine::Enabled,
            vertical_scroll_margin: 7,
            horizontal_scroll_margin: 9,
            scroll_sensitivity: 1.5,
            hover_delay_ms: 250,
            lsp_completions_enabled: false,
            ..AppEditorSettings::default()
        };

        let text_settings = QueryEditor::text_editor_settings_from_app(&app_settings);

        assert_eq!(text_settings.document.indent_size, 2);
        assert!(text_settings.document.use_tabs);
        assert!(text_settings.gutter.show_line_numbers);
        assert!(text_settings.gutter.show_relative_line_numbers);
        assert_eq!(text_settings.soft_wrap, SoftWrapMode::EditorWidth);
        assert!(!text_settings.cursor.blink);
        assert_eq!(
            text_settings.cursor.shape,
            zqlz_text_editor::CursorShape::Underline
        );
        assert!(!text_settings.highlight_selection_matches);
        assert!(text_settings.scroll.scroll_beyond_last_line);
        assert_eq!(text_settings.scroll.vertical_margin_lines, 7);
        assert_eq!(text_settings.scroll.horizontal_margin_columns, 9);
        assert_eq!(text_settings.scroll.sensitivity, 150);
        assert_eq!(
            text_settings.hover_delay,
            std::time::Duration::from_millis(250)
        );
        assert!(!text_settings.completion.automatically_show);
    }

    #[test]
    fn query_document_symbols_split_statements_and_track_lines() {
        let symbols = query_document_symbols(
            "select * from users;\n\ncreate table audit_log (id int);\nupdate users set name = 'a;b';",
        );

        assert_eq!(
            symbols,
            vec![
                QueryDocumentSymbol {
                    label: "Query".to_string(),
                    line: 0,
                    column: 0,
                },
                QueryDocumentSymbol {
                    label: "CREATE audit_log".to_string(),
                    line: 2,
                    column: 0,
                },
                QueryDocumentSymbol {
                    label: "UPDATE users".to_string(),
                    line: 3,
                    column: 0,
                },
            ]
        );
    }
}
