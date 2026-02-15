//! Wraps Zed Editor with ZQLZ-specific state and APIs
//!
//! EditorWrapper is an Entity that contains a Zed Editor and provides
//! simple methods for text operations, diagnostics management, and completion support.
//!
//! SQL Syntax Highlighting:
//! The editor uses tree-sitter-sequel grammar with its bundled HIGHLIGHTS_QUERY
//! for syntax highlighting via Zed's Language system. The syntax theme must be
//! applied via `Language::set_theme()` for highlight colors to take effect.
//! Call `refresh_syntax_theme()` when the Zed theme changes.
//!
//! Completions:
//! The editor supports SQL completions via zqlz-lsp. Call `set_completion_provider()`
//! to enable completion support. Completions are triggered via Ctrl+Space or automatically
//! as you type (if enabled in settings).
//!
//! Hover:
//! The editor supports hover documentation via zqlz-lsp. Call `get_hover()` with
//! a byte offset to retrieve hover information for the symbol at that position.

use editor::Editor;
use gpui::{
    px, App, AppContext as _, Context, Entity, FocusHandle, Focusable, Pixels, Point, SharedString,
    Subscription, Task, Window,
};
use language::{
    BlockCommentConfig, BracketPair, BracketPairConfig, Buffer, DiagnosticEntry, DiagnosticSet,
    DiagnosticSeverity, DiagnosticSourceKind, Language, LanguageConfig, LanguageName,
};
use lsp::LanguageServerId;
use lsp_types::{CompletionItem, GotoDefinitionResponse, Hover, SignatureHelp, WorkspaceEdit};
use multi_buffer::{MultiBuffer, ToOffset};
use parking_lot::RwLock;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use text::Anchor;
use theme::ActiveTheme;
use tree_sitter::Language as TreeSitterLanguage;
use zqlz_lsp::SqlLsp;
use zqlz_settings::ZqlzSettings;
use zqlz_ui::widgets::{Rope, RopeExt};

use crate::completion_menu::{CompletionMenu, CompletionMenuEditor};

/// Wraps Zed's Editor component with ZQLZ-specific APIs
///
/// This is the primary interface for ZQLZ to interact with the Zed editor.
/// It hides Zed-specific details and provides a clean API for:
/// - Setting and getting text content
/// - Managing diagnostics
/// - Rendering the editor
/// - SQL completions via zqlz-lsp
pub struct EditorWrapper {
    editor: Entity<Editor>,
    buffer: Entity<MultiBuffer>,
    sql_lsp: Option<Arc<RwLock<SqlLsp>>>,
    completion_menu: Option<Entity<CompletionMenu<EditorWrapper>>>,
    _completion_task: Task<()>,
    _subscriptions: Vec<Subscription>,
}

impl EditorWrapper {
    /// Creates a new EditorWrapper with SQL language support
    ///
    /// The buffer is configured with SQL language settings including:
    /// - Tree-sitter grammar (tree-sitter-sequel) for syntax highlighting
    /// - HIGHLIGHTS_QUERY for keyword, string, comment, etc. coloring
    /// - Line comment prefix: `-- `
    /// - Block comment delimiters: `/*` and `*/`
    ///
    /// The current Zed syntax theme is applied to the language immediately
    /// so that highlight colors are active from the start.
    ///
    /// # Arguments
    /// * `window` - The GPUI window context
    /// * `cx` - The GPUI context for this entity
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        // Create a new empty buffer for local editing
        let buffer = cx.new(|cx| Buffer::local("", cx));

        // Set SQL language on the buffer for comment toggling and syntax highlighting
        let sql_language = Arc::new(Self::create_sql_language());

        // Log grammar state before applying theme
        if let Some(grammar) = sql_language.grammar() {
            let has_highlights_config = grammar.highlights_config.is_some();
            tracing::info!(
                has_highlights_config,
                "SQL language grammar state before set_theme"
            );
            if let Some(config) = &grammar.highlights_config {
                let capture_names = config.query.capture_names();
                tracing::info!(
                    capture_count = capture_names.len(),
                    "Highlights query capture names: {:?}",
                    &capture_names[..capture_names.len().min(20)]
                );
            }
        } else {
            tracing::warn!("SQL language has no grammar!");
        }

        // Apply the current Zed syntax theme to the language so the highlight map
        // maps tree-sitter capture names (@keyword, @string, etc.) to highlight IDs.
        // Without this, all tokens render with the default (unstyled) color.
        let syntax_theme = &cx.theme().styles.syntax;
        tracing::info!(
            theme_highlight_count = syntax_theme.highlights.len(),
            "Syntax theme entries: {:?}",
            syntax_theme
                .highlights
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
        );
        sql_language.set_theme(syntax_theme);

        // Log highlight map state after applying theme
        if let Some(grammar) = sql_language.grammar() {
            let highlight_map = grammar.highlight_map();
            tracing::info!("Highlight map after set_theme: {:?}", highlight_map);
        }

        buffer.update(cx, |buf, cx| {
            buf.set_language(Some(sql_language), cx);
        });

        // Wrap in MultiBuffer for editor compatibility
        let multi_buffer = cx.new(|cx| MultiBuffer::singleton(buffer, cx));

        // Create the editor with the buffer
        // Use full mode for multi-line SQL editing with all features
        let editor = cx.new(|cx| {
            Editor::new(
                editor::EditorMode::full(),
                multi_buffer.clone(),
                None, // project - not needed for standalone editor
                window,
                cx,
            )
        });

        Self {
            editor,
            buffer: multi_buffer,
            sql_lsp: None,
            completion_menu: None,
            _completion_task: Task::ready(()),
            _subscriptions: Vec::new(),
        }
    }

    /// Sets the SQL LSP instance for completions
    ///
    /// Call this after creating the EditorWrapper to enable SQL completions.
    /// The LSP provides table names, column names, keywords, and functions.
    ///
    /// # Arguments
    /// * `lsp` - Shared SQL LSP instance
    pub fn set_sql_lsp(&mut self, lsp: Arc<RwLock<SqlLsp>>) {
        tracing::info!("EditorWrapper: SQL LSP set");
        self.sql_lsp = Some(lsp);
    }

    /// Check if the LSP is connected and ready
    pub fn is_lsp_connected(&self) -> bool {
        self.sql_lsp.is_some()
    }

    /// Gets completions at the current cursor position
    ///
    /// Returns a list of completion items from the SQL LSP if available.
    /// If no LSP is set, returns an empty vector.
    ///
    /// Completions are only returned if LSP is enabled in settings.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// Vector of LSP CompletionItems (table names, column names, keywords, functions)
    pub fn get_completions(&self, cx: &App) -> Vec<CompletionItem> {
        self.get_completions_internal(cx, false)
    }

    /// Gets completions with explicit manual trigger mode
    ///
    /// When is_manual_trigger is true, completions are always shown regardless
    /// of trigger conditions (like minimum character count).
    pub fn get_completions_manual(&self, cx: &App) -> Vec<CompletionItem> {
        self.get_completions_internal(cx, true)
    }

    fn get_completions_internal(&self, cx: &App, is_manual_trigger: bool) -> Vec<CompletionItem> {
        let Some(lsp) = &self.sql_lsp else {
            tracing::warn!("get_completions: No LSP set on EditorWrapper!");
            return Vec::new();
        };

        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_completions_enabled {
            tracing::debug!("get_completions: LSP disabled in settings");
            return Vec::new();
        }

        let text = self.get_text(cx);
        let offset = self.get_cursor_offset(cx);

        tracing::info!(
            "get_completions: requesting at offset {} with text length {} (manual={})",
            offset,
            text.len(),
            is_manual_trigger
        );

        let rope = Rope::from(text.as_str());
        let mut lsp_guard = lsp.write();
        let completions = lsp_guard.get_completions_with_trigger(&rope, offset, is_manual_trigger);

        tracing::info!("get_completions: got {} items", completions.len());

        for (i, comp) in completions.iter().take(5).enumerate() {
            tracing::debug!(
                "  completion[{}]: {} (kind: {:?})",
                i,
                comp.label,
                comp.kind
            );
        }

        completions
    }

    /// Shows the completion menu at the current cursor position (manual trigger)
    ///
    /// This is called when the user explicitly requests completions via Ctrl+Space.
    /// Uses manual trigger mode which bypasses trigger condition checks.
    ///
    /// # Arguments
    /// * `window` - The GPUI window context
    /// * `cx` - The GPUI context for this entity
    pub fn show_completions(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.show_completions_internal(window, cx, true);
    }

    /// Shows completions automatically (auto-trigger mode)
    ///
    /// This is called as the user types. Respects trigger conditions like
    /// minimum character count and trigger characters.
    pub fn show_completions_auto(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.show_completions_internal(window, cx, false);
    }

    fn show_completions_internal(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        is_manual_trigger: bool,
    ) {
        tracing::info!("show_completions: called (manual={})", is_manual_trigger);

        let completions = self.get_completions_internal(cx, is_manual_trigger);

        tracing::info!("show_completions: got {} completions", completions.len());

        if completions.is_empty() {
            if is_manual_trigger {
                tracing::info!("show_completions: no completions available");
            }
            return;
        }

        let offset = self.get_cursor_offset(cx);
        let text = self.get_text(cx);

        let trigger_start = self.find_word_start(&text, offset);
        let query: SharedString = if trigger_start < offset {
            text[trigger_start..offset].to_string().into()
        } else {
            "".into()
        };

        tracing::info!(
            "show_completions: offset={}, trigger_start={}, query={:?}",
            offset,
            trigger_start,
            query
        );

        let completion_menu = match &self.completion_menu {
            Some(menu) => menu.clone(),
            None => {
                let menu = CompletionMenu::new(cx.entity(), window, cx);
                self.completion_menu = Some(menu.clone());
                menu
            }
        };

        completion_menu.update(cx, |menu, _| {
            menu.update_query(trigger_start, query);
        });

        completion_menu.update(cx, |menu, cx| {
            menu.show(offset, completions, window, cx);
        });

        cx.notify();
    }

    fn find_word_start(&self, text: &str, offset: usize) -> usize {
        let bytes = text.as_bytes();
        let mut start = offset;

        while start > 0 {
            let prev = start - 1;
            if let Some(&ch) = bytes.get(prev) {
                if ch.is_ascii_alphanumeric() || ch == b'_' {
                    start = prev;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        start
    }

    /// Hides the completion menu if it's open
    ///
    /// # Arguments
    /// * `cx` - The GPUI context for this entity
    pub fn hide_completions(&mut self, cx: &mut Context<Self>) {
        if let Some(menu) = &self.completion_menu {
            menu.update(cx, |menu, cx| {
                menu.hide(cx);
            });
            cx.notify();
        }
    }

    /// Checks if the completion menu is currently open
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// True if the completion menu is open, false otherwise
    pub fn is_completion_menu_open(&self, cx: &App) -> bool {
        self.completion_menu
            .as_ref()
            .map(|m| m.read(cx).is_open())
            .unwrap_or(false)
    }

    /// Refreshes the completion filter for the current cursor position.
    ///
    /// Re-runs fuzzy matching on the existing completion items using the
    /// updated query prefix. Does not re-fetch items from the LSP — the
    /// item set stays fixed from the initial `show_completions` call.
    pub fn refresh_completions(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(menu) = &self.completion_menu else {
            return;
        };

        if !menu.read(cx).is_open() {
            return;
        }

        let offset = self.get_cursor_offset(cx);
        let text = self.get_text(cx);
        let trigger_start = self.find_word_start(&text, offset);
        let query: SharedString = if trigger_start < offset {
            text[trigger_start..offset].to_string().into()
        } else {
            "".into()
        };

        menu.update(cx, |menu, _| {
            menu.update_query(trigger_start, query);
        });

        menu.update(cx, |menu, cx| {
            menu.refilter(offset, window, cx);
        });

        cx.notify();
    }

    /// Handles a keyboard action for the completion menu
    ///
    /// Should be called for navigation keys (Up/Down/Enter/Escape) when
    /// the completion menu is open.
    ///
    /// # Arguments
    /// * `action` - The action to handle
    /// * `window` - The GPUI window context
    /// * `cx` - The GPUI context for this entity
    ///
    /// # Returns
    /// True if the action was handled by the completion menu, false otherwise
    pub fn handle_completion_action(
        &mut self,
        action: Box<dyn gpui::Action>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        use crate::completion_menu::{Cancel, Confirm, SelectDown, SelectUp};

        let Some(menu) = &self.completion_menu else {
            return false;
        };

        if !menu.read(cx).is_open() {
            return false;
        }

        let menu = menu.clone();
        if action.partial_eq(&Confirm) {
            menu.update(cx, |menu, cx| menu.confirm_selection(window, cx));
            true
        } else if action.partial_eq(&SelectUp) {
            menu.update(cx, |menu, cx| menu.select_prev(window, cx));
            true
        } else if action.partial_eq(&SelectDown) {
            menu.update(cx, |menu, cx| menu.select_next(window, cx));
            true
        } else if action.partial_eq(&Cancel) {
            menu.update(cx, |menu, cx| menu.hide(cx));
            true
        } else {
            false
        }
    }

    /// Sets the cursor position to a specific byte offset
    ///
    /// # Arguments
    /// * `offset` - The byte offset to move the cursor to
    /// * `window` - The GPUI window context
    /// * `cx` - The GPUI context for this entity
    pub fn set_cursor_offset(
        &mut self,
        offset: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use multi_buffer::MultiBufferOffset;

        let point = {
            let buffer_snapshot = self.buffer.read(cx).read(cx);
            buffer_snapshot.offset_to_point(MultiBufferOffset(offset))
        };

        self.editor.update(cx, |editor, cx| {
            editor.change_selections(
                editor::SelectionEffects::no_scroll(),
                window,
                cx,
                |selections| {
                    selections.select_ranges([point..point]);
                },
            );
        });
    }

    /// Gets the completion menu entity for rendering
    ///
    /// # Returns
    /// An optional reference to the completion menu entity
    pub fn completion_menu(&self) -> Option<&Entity<CompletionMenu<EditorWrapper>>> {
        self.completion_menu.as_ref()
    }

    /// Gets hover information at the current cursor position
    ///
    /// Returns hover information from the SQL LSP if available.
    /// If no LSP is set, returns None.
    ///
    /// Hover is only returned if LSP is enabled in settings.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// Optional LSP Hover (documentation for the symbol under cursor)
    pub fn get_hover(&self, cx: &App) -> Option<Hover> {
        // Check if LSP is set
        let Some(lsp) = &self.sql_lsp else {
            tracing::warn!("get_hover: No LSP set on EditorWrapper!");
            return None;
        };

        // Check if LSP hover is enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_hover_enabled {
            tracing::debug!("get_hover: LSP disabled in settings");
            return None;
        }

        let text = self.get_text(cx);
        let offset = self.get_cursor_offset(cx);

        tracing::info!("get_hover: requesting at offset {}", offset);

        let rope = Rope::from(text.as_str());
        let lsp_guard = lsp.read();
        let hover = lsp_guard.get_hover(&rope, offset);

        if hover.is_some() {
            tracing::info!("get_hover: got hover content");
        } else {
            tracing::debug!("get_hover: no hover content");
        }

        hover
    }

    /// Gets hover information at a specific byte offset
    ///
    /// Returns hover information from the SQL LSP if available.
    /// If no LSP is set, returns None.
    ///
    /// Hover is only returned if LSP is enabled in settings.
    ///
    /// # Arguments
    /// * `offset` - The byte offset in the buffer
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// Optional LSP Hover (documentation for the symbol at offset)
    pub fn get_hover_at(&self, offset: usize, cx: &App) -> Option<Hover> {
        // Check if LSP hover is enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_hover_enabled {
            return None;
        }

        let lsp = self.sql_lsp.as_ref()?;
        let text = self.get_text(cx);

        let rope = Rope::from(text.as_str());
        let lsp_guard = lsp.read();
        lsp_guard.get_hover(&rope, offset)
    }

    /// Gets signature help at the current cursor position
    ///
    /// Returns signature help from the SQL LSP if available and cursor is inside a function call.
    /// If no LSP is set or cursor is not inside a function call, returns None.
    ///
    /// Signature help is only returned if LSP is enabled in settings.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// Optional LSP SignatureHelp (function signature and parameter info)
    pub fn get_signature_help(&self, cx: &App) -> Option<SignatureHelp> {
        // Check if LSP hover is enabled in settings (signature help uses hover capability)
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_hover_enabled {
            return None;
        }

        let lsp = self.sql_lsp.as_ref()?;
        let text = self.get_text(cx);
        let offset = self.get_cursor_offset(cx);

        let rope = Rope::from(text.as_str());
        let lsp_guard = lsp.read();
        lsp_guard.get_signature_help(&rope, offset)
    }

    /// Gets signature help at a specific byte offset
    ///
    /// Returns signature help from the SQL LSP if available and offset is inside a function call.
    /// If no LSP is set or offset is not inside a function call, returns None.
    ///
    /// Signature help is only returned if LSP is enabled in settings.
    ///
    /// # Arguments
    /// * `offset` - The byte offset in the buffer
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// Optional LSP SignatureHelp (function signature and parameter info)
    pub fn get_signature_help_at(&self, offset: usize, cx: &App) -> Option<SignatureHelp> {
        // Check if LSP hover is enabled in settings (signature help uses hover capability)
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_hover_enabled {
            return None;
        }

        let lsp = self.sql_lsp.as_ref()?;
        let text = self.get_text(cx);

        let rope = Rope::from(text.as_str());
        let lsp_guard = lsp.read();
        lsp_guard.get_signature_help(&rope, offset)
    }

    /// Gets the definition of the symbol at the current cursor position.
    ///
    /// This method calls the SQL LSP to find the definition of the symbol
    /// under the cursor. For SQL queries, this returns information about
    /// schema objects (tables, columns, views, functions, etc.).
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// Optional GotoDefinitionResponse if a definition is found
    pub fn get_definition(&self, cx: &App) -> Option<GotoDefinitionResponse> {
        // Check if LSP is enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled {
            return None;
        }

        let lsp = self.sql_lsp.as_ref()?;
        let text = self.get_text(cx);

        let rope = Rope::from(text.as_str());
        let offset = self.get_cursor_offset(cx);

        let lsp_guard = lsp.read();
        lsp_guard.get_definition(&rope, offset)
    }

    /// Gets the definition at a specific byte offset in the buffer.
    ///
    /// # Arguments
    /// * `offset` - The byte offset in the buffer
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// Optional GotoDefinitionResponse if a definition is found
    pub fn get_definition_at(&self, offset: usize, cx: &App) -> Option<GotoDefinitionResponse> {
        // Check if LSP is enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled {
            return None;
        }

        let lsp = self.sql_lsp.as_ref()?;
        let text = self.get_text(cx);

        let rope = Rope::from(text.as_str());

        let lsp_guard = lsp.read();
        lsp_guard.get_definition(&rope, offset)
    }

    /// Find all references to a symbol at the current cursor position.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// Vector of Location objects representing all references found
    pub fn get_references(&self, cx: &App) -> Vec<lsp_types::Location> {
        // Check if LSP is enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled {
            return Vec::new();
        }

        let lsp = match self.sql_lsp.as_ref() {
            Some(l) => l,
            None => return Vec::new(),
        };
        let text = self.get_text(cx);

        let rope = Rope::from(text.as_str());
        let offset = self.get_cursor_offset(cx);

        let lsp_guard = lsp.read();
        lsp_guard.get_references(&rope, offset)
    }

    /// Find all references at a specific byte offset in the buffer.
    ///
    /// # Arguments
    /// * `offset` - The byte offset in the buffer
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// Vector of Location objects representing all references found
    pub fn get_references_at(&self, offset: usize, cx: &App) -> Vec<lsp_types::Location> {
        // Check if LSP is enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled {
            return Vec::new();
        }

        let lsp = match self.sql_lsp.as_ref() {
            Some(l) => l,
            None => return Vec::new(),
        };
        let text = self.get_text(cx);

        let rope = Rope::from(text.as_str());

        let lsp_guard = lsp.read();
        lsp_guard.get_references(&rope, offset)
    }

    /// Rename a symbol at the current cursor position.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    /// * `new_name` - The new name for the symbol
    ///
    /// # Returns
    /// A WorkspaceEdit containing the text edits to rename all occurrences,
    /// or None if the rename is not valid
    pub fn rename(&self, new_name: &str, cx: &App) -> Option<WorkspaceEdit> {
        // Check if LSP is enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_rename_enabled {
            return None;
        }

        let lsp = match self.sql_lsp.as_ref() {
            Some(l) => l,
            None => return None,
        };
        let text = self.get_text(cx);

        let rope = Rope::from(text.as_str());
        let offset = self.get_cursor_offset(cx);

        let lsp_guard = lsp.read();
        lsp_guard.rename(&rope, offset, new_name)
    }

    /// Rename a symbol at a specific byte offset in the buffer.
    ///
    /// # Arguments
    /// * `offset` - The byte offset in the buffer
    /// * `new_name` - The new name for the symbol
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// A WorkspaceEdit containing the text edits to rename all occurrences,
    /// or None if the rename is not valid
    pub fn rename_at(&self, offset: usize, new_name: &str, cx: &App) -> Option<WorkspaceEdit> {
        // Check if LSP is enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_rename_enabled {
            return None;
        }

        let lsp = match self.sql_lsp.as_ref() {
            Some(l) => l,
            None => return None,
        };
        let text = self.get_text(cx);

        let rope = Rope::from(text.as_str());

        let lsp_guard = lsp.read();
        lsp_guard.rename(&rope, offset, new_name)
    }

    /// Get code actions at the current cursor position.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// Vector of CodeAction objects representing available quick fixes
    pub fn get_code_actions(&self, cx: &App) -> Vec<lsp_types::CodeAction> {
        // Check if LSP is enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_code_actions_enabled {
            return Vec::new();
        }

        let lsp = match self.sql_lsp.as_ref() {
            Some(l) => l,
            None => return Vec::new(),
        };
        let text = self.get_text(cx);
        let offset = self.get_cursor_offset(cx);

        let rope = Rope::from(text.as_str());

        // Pass empty diagnostics - the LSP will run diagnostics if needed
        // and generate context-based actions
        let diagnostics: Vec<lsp_types::Diagnostic> = Vec::new();

        let lsp_guard = lsp.read();
        lsp_guard.get_code_actions(&rope, offset, &diagnostics)
    }

    /// Get code actions at a specific byte offset.
    ///
    /// # Arguments
    /// * `offset` - The byte offset in the buffer
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// Vector of CodeAction objects representing available quick fixes
    pub fn get_code_actions_at(&self, offset: usize, cx: &App) -> Vec<lsp_types::CodeAction> {
        // Check if LSP is enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_code_actions_enabled {
            return Vec::new();
        }

        let lsp = match self.sql_lsp.as_ref() {
            Some(l) => l,
            None => return Vec::new(),
        };
        let text = self.get_text(cx);

        let rope = Rope::from(text.as_str());

        // Pass empty diagnostics - the LSP will run diagnostics if needed
        // and generate context-based actions
        let diagnostics: Vec<lsp_types::Diagnostic> = Vec::new();

        let lsp_guard = lsp.read();
        lsp_guard.get_code_actions(&rope, offset, &diagnostics)
    }

    /// Gets the current cursor offset in the buffer
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// The byte offset of the cursor position
    pub fn get_cursor_offset(&self, cx: &App) -> usize {
        let editor = self.editor.read(cx);
        let buffer_snapshot = self.buffer.read(cx).read(cx);

        let selections = editor.selections.disjoint_anchors();
        if selections.is_empty() {
            return 0;
        }

        let selection = &selections[selections.len() - 1];
        selection.head().to_offset(&buffer_snapshot).0
    }

    /// Navigate to a specific position in the editor
    ///
    /// Moves the cursor to the specified line and column (0-indexed).
    /// Optionally selects a range if end_line and end_column are provided.
    ///
    /// # Arguments
    /// * `line` - Target line number (0-indexed)
    /// * `column` - Target column number (0-indexed)
    /// * `end_line` - Optional end line for selection (0-indexed)
    /// * `end_column` - Optional end column for selection (0-indexed)
    /// * `window` - The GPUI window
    /// * `cx` - The GPUI app context
    pub fn navigate_to(
        &self,
        line: usize,
        column: usize,
        end_line: Option<usize>,
        end_column: Option<usize>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use text::Point;

        // Get the buffer and move the cursor using buffer edit
        let buffer = self.buffer.clone();

        // Create Point range for the edit (this moves cursor)
        let start = Point::new(line as u32, column as u32);
        let end = if let (Some(end_l), Some(end_c)) = (end_line, end_column) {
            Point::new(end_l as u32, end_c as u32)
        } else {
            start
        };

        // Use buffer edit to simulate cursor movement - empty edit moves cursor
        buffer.update(cx, |buffer, cx| {
            buffer.edit([(start..end, "")], None, cx);
        });
    }

    /// Creates a SQL Language configuration for Zed editor
    ///
    /// This configures:
    /// - Tree-sitter grammar from tree-sitter-sequel for syntax highlighting
    /// - HIGHLIGHTS_QUERY for syntax token highlighting (keywords, strings, etc.)
    /// - Line comments with `-- ` prefix
    /// - Block comments with `/* */` delimiters
    /// - Tab size of 4 spaces (standard SQL indentation)
    /// - Bracket pairs for auto-close: (), [], {}, '', ""
    fn create_sql_language() -> Language {
        use std::num::NonZero;

        let bracket_pairs = vec![
            BracketPair {
                start: "(".to_string(),
                end: ")".to_string(),
                close: true,
                surround: true,
                newline: true,
            },
            BracketPair {
                start: "[".to_string(),
                end: "]".to_string(),
                close: true,
                surround: true,
                newline: true,
            },
            BracketPair {
                start: "{".to_string(),
                end: "}".to_string(),
                close: true,
                surround: true,
                newline: true,
            },
            BracketPair {
                start: "'".to_string(),
                end: "'".to_string(),
                close: true,
                surround: true,
                newline: false,
            },
            BracketPair {
                start: "\"".to_string(),
                end: "\"".to_string(),
                close: true,
                surround: true,
                newline: false,
            },
        ];

        let config = LanguageConfig {
            name: LanguageName::new("SQL"),
            line_comments: vec![Arc::from("-- ")],
            block_comment: Some(BlockCommentConfig {
                start: Arc::from("/*"),
                end: Arc::from("*/"),
                prefix: Arc::from(" "),
                tab_size: 0,
            }),
            tab_size: NonZero::new(4),
            brackets: BracketPairConfig {
                pairs: bracket_pairs,
                disabled_scopes_by_bracket_ix: vec![
                    vec![],
                    vec![],
                    vec![],
                    vec!["string".to_string()],
                    vec!["string".to_string()],
                ],
            },
            ..Default::default()
        };

        let ts_language = TreeSitterLanguage::new(tree_sitter_sequel::LANGUAGE);

        Language::new(config, Some(ts_language))
            .with_highlights_query(tree_sitter_sequel::HIGHLIGHTS_QUERY)
            .expect("SQL highlights query should compile")
    }

    /// Re-applies the current syntax theme to the SQL language on the buffer.
    ///
    /// Call this when the Zed theme changes so syntax highlighting colors
    /// are updated to match the new theme.
    pub fn refresh_syntax_theme(&self, cx: &App) {
        let syntax_theme = &cx.theme().styles.syntax;

        if let Some(buffer) = self.buffer.read(cx).as_singleton() {
            let buffer_ref = buffer.read(cx);
            if let Some(language) = buffer_ref.language() {
                language.set_theme(syntax_theme);
                tracing::debug!(
                    "Refreshed syntax theme on SQL language (theme has {} highlight entries)",
                    syntax_theme.highlights.len()
                );
            } else {
                tracing::warn!("refresh_syntax_theme: no language set on buffer");
            }
        } else {
            tracing::warn!("refresh_syntax_theme: no singleton buffer found");
        }
    }

    /// Sets the text content of the editor
    ///
    /// Replaces the entire buffer content with the provided text.
    ///
    /// # Arguments
    /// * `text` - The new text content
    /// * `window` - The GPUI window context  
    /// * `cx` - The GPUI context for this entity
    pub fn set_text(&mut self, text: String, window: &mut Window, cx: &mut Context<Self>) {
        // Use the Editor's set_text method which handles everything properly
        self.editor.update(cx, |editor, cx| {
            editor.set_text(text, window, cx);
        });
    }

    /// Applies a workspace edit to the editor content.
    ///
    /// This takes a WorkspaceEdit from the LSP rename request and applies
    /// all text edits to the current buffer content.
    ///
    /// # Arguments
    /// * `edit` - The workspace edit containing text edits to apply
    /// * `window` - The GPUI window context
    /// * `cx` - The GPUI context for this entity
    ///
    /// # Returns
    /// True if the edit was applied successfully, false otherwise
    pub fn apply_workspace_edit(
        &mut self,
        edit: &lsp_types::WorkspaceEdit,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // Get the current text
        let current_text = self.get_text(cx);

        // Get the text edits from the workspace edit
        let text_edits = match &edit.changes {
            Some(changes) => {
                // Get edits for our internal URI (sql://internal)
                let uri = lsp_types::Uri::from_str("sql://internal").ok();
                match uri {
                    Some(u) => changes.get(&u).cloned().unwrap_or_default(),
                    None => Vec::new(),
                }
            }
            None => Vec::new(),
        };

        if text_edits.is_empty() {
            tracing::debug!("No text edits to apply");
            return false;
        }

        // Apply edits in reverse order (from end to start) to preserve positions
        let new_text = current_text;

        // Sort edits by position (line, character) in reverse order
        let mut sorted_edits: Vec<_> = text_edits.clone();
        sorted_edits.sort_by(|a, b| {
            let line_cmp = b.range.start.line.cmp(&a.range.start.line);
            if line_cmp == std::cmp::Ordering::Equal {
                b.range.start.character.cmp(&a.range.start.character)
            } else {
                line_cmp
            }
        });

        // Convert text to lines for easier manipulation
        let mut lines: Vec<String> = new_text.lines().map(|s| s.to_string()).collect();

        for edit in sorted_edits {
            let start_line = edit.range.start.line as usize;
            let start_char = edit.range.start.character as usize;
            let end_line = edit.range.end.line as usize;
            let end_char = edit.range.end.character as usize;

            if start_line >= lines.len() || end_line >= lines.len() {
                tracing::warn!(
                    "Edit out of bounds: line {} to {} but only {} lines",
                    start_line,
                    end_line,
                    lines.len()
                );
                continue;
            }

            // Get the lines to modify
            let line_text = &mut lines[start_line];

            if start_line == end_line {
                // Single line edit
                if start_char <= line_text.len() && end_char <= line_text.len() {
                    line_text.replace_range(start_char..end_char, &edit.new_text);
                }
            } else {
                // Multi-line edit - this is more complex, handle each part
                // Replace from start_char to end of start line
                if start_char <= line_text.len() {
                    line_text.replace_range(start_char.., "");
                }

                // Replace from start of end line to end_char
                let end_line_text = &mut lines[end_line];
                if end_char <= end_line_text.len() {
                    end_line_text.replace_range(0..end_char, &edit.new_text);
                }

                // Remove any lines between start and end
                if end_line > start_line + 1 {
                    lines.drain(start_line + 1..end_line);
                }
            }
        }

        // Reconstruct the text
        let new_text = lines.join("\n");

        // Set the new text
        self.set_text(new_text, window, cx);

        tracing::info!(
            "Applied workspace edit with {} text edits",
            text_edits.len()
        );
        true
    }

    /// Gets the current text content of the editor
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// The current text content as a String
    pub fn get_text(&self, cx: &App) -> String {
        self.buffer.read(cx).read(cx).text()
    }

    /// Updates the diagnostics displayed in the editor
    ///
    /// Converts ZQLZ diagnostics to Zed's format and updates the buffer's diagnostic set.
    ///
    /// # Arguments
    /// * `diagnostics` - Vector of diagnostics to display
    /// * `cx` - The GPUI context for this entity
    pub fn set_diagnostics(&mut self, diagnostics: Vec<Diagnostic>, cx: &mut Context<Self>) {
        // Get the singleton buffer from the MultiBuffer
        if let Some(buffer) = self.buffer.read(cx).as_singleton() {
            buffer.update(cx, |buffer, cx| {
                let buffer_snapshot = buffer.text_snapshot();

                // Convert ZQLZ diagnostics to Zed format
                let zed_diagnostics: Vec<DiagnosticEntry<Anchor>> = diagnostics
                    .into_iter()
                    .enumerate()
                    .map(|(group_id, diag)| {
                        // Convert byte offsets to anchors for stable positions
                        let start_anchor = buffer_snapshot.anchor_before(diag.range.start);
                        let end_anchor = buffer_snapshot.anchor_after(diag.range.end);

                        DiagnosticEntry {
                            range: start_anchor..end_anchor,
                            diagnostic: language::Diagnostic {
                                source: Some("zqlz-lsp".to_string()),
                                registration_id: None,
                                code: None,
                                code_description: None,
                                severity: match diag.severity {
                                    DiagnosticLevel::Error => DiagnosticSeverity::ERROR,
                                    DiagnosticLevel::Warning => DiagnosticSeverity::WARNING,
                                    DiagnosticLevel::Info => DiagnosticSeverity::INFORMATION,
                                    DiagnosticLevel::Hint => DiagnosticSeverity::HINT,
                                },
                                message: diag.message.clone(),
                                markdown: Some(diag.message),
                                group_id,
                                is_primary: true,
                                is_disk_based: false,
                                is_unnecessary: false,
                                source_kind: DiagnosticSourceKind::Other,
                                data: None,
                                underline: true,
                            },
                        }
                    })
                    .collect();

                // Create DiagnosticSet from the entries
                let diagnostic_set =
                    DiagnosticSet::from_sorted_entries(zed_diagnostics, &buffer_snapshot);

                // Update buffer diagnostics with a fake language server ID
                buffer.update_diagnostics(LanguageServerId(0), diagnostic_set, cx);
            });
        }
    }

    /// Runs LSP diagnostics on the current text and updates the editor
    ///
    /// This method validates the SQL text using the LSP and displays any
    /// diagnostics in the editor. Diagnostics are only run if LSP is enabled
    /// in settings.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    pub fn run_diagnostics(&mut self, cx: &mut Context<Self>) {
        // Check if LSP diagnostics are enabled in settings
        let settings = ZqlzSettings::global(cx);
        if !settings.editor.lsp_enabled || !settings.editor.lsp_diagnostics_enabled {
            // Clear diagnostics if LSP is disabled
            self.set_diagnostics(Vec::new(), cx);
            return;
        }

        // Run diagnostics only if LSP is configured
        let Some(lsp) = &self.sql_lsp else {
            return;
        };

        let text = self.get_text(cx);
        let rope = Rope::from(text.as_str());

        // Run LSP validation
        let lsp_diagnostics = {
            let mut lsp_guard = lsp.write();
            lsp_guard.validate_sql(&rope)
        };

        // Convert LSP diagnostics to Zed format
        let diagnostics: Vec<Diagnostic> = lsp_diagnostics
            .iter()
            .filter_map(|lsp_diag| {
                use lsp_types::DiagnosticSeverity;
                // Convert LSP Range (line/col) to byte offsets using rope
                let start_offset = rope.position_to_offset(&lsp_diag.range.start);
                let end_offset = rope.position_to_offset(&lsp_diag.range.end);

                // Convert LSP severity to Zed severity
                let severity = match lsp_diag.severity {
                    Some(DiagnosticSeverity::ERROR) => DiagnosticLevel::Error,
                    Some(DiagnosticSeverity::WARNING) => DiagnosticLevel::Warning,
                    Some(DiagnosticSeverity::INFORMATION) => DiagnosticLevel::Info,
                    Some(DiagnosticSeverity::HINT) => DiagnosticLevel::Hint,
                    _ => DiagnosticLevel::Error,
                };

                Some(Diagnostic {
                    range: start_offset..end_offset,
                    severity,
                    message: lsp_diag.message.clone(),
                })
            })
            .collect();

        // Update editor with diagnostics
        self.set_diagnostics(diagnostics, cx);
    }

    /// Gets a reference to the underlying Zed Editor entity
    ///
    /// This allows the caller to access the editor directly for rendering
    /// or other advanced operations.
    ///
    /// Note: Returns a cloned Entity handle (Entity is an Arc, so cloning is cheap).
    /// This is needed so the Entity can be used directly as a child element in GPUI rendering.
    ///
    /// # Returns
    /// A clone of the editor entity
    pub fn editor(&self) -> Entity<Editor> {
        self.editor.clone()
    }

    /// Gets a reference to the underlying MultiBuffer entity
    ///
    /// This allows the caller to access the buffer directly if needed.
    ///
    /// # Returns
    /// A reference to the buffer entity
    pub fn buffer(&self) -> &Entity<MultiBuffer> {
        &self.buffer
    }

    /// Gets the focus handle for the editor
    ///
    /// This handle should be focused to allow the editor to receive keyboard input.
    /// When the editor is focused, it can handle typing, navigation, selection,
    /// and all other keyboard-driven editing operations.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// A clone of the editor's focus handle
    ///
    /// # Example
    /// ```ignore
    /// // Focus the editor so it can receive keyboard input
    /// let focus_handle = editor_wrapper.focus_handle(cx);
    /// focus_handle.focus(window, cx);
    /// ```
    pub fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.editor.read(cx).focus_handle(cx).clone()
    }

    /// Gets the currently selected text, or None if no selection
    ///
    /// If there are multiple selections, returns the text of the newest (primary) selection.
    ///
    /// # Arguments
    /// * `cx` - The GPUI app context
    ///
    /// # Returns
    /// The selected text as Some(String), or None if there is no selection
    pub fn get_selected_text(&self, cx: &App) -> Option<String> {
        let editor = self.editor.read(cx);
        let buffer_snapshot = self.buffer.read(cx).read(cx);

        // Get all selections in buffer space (as anchors)
        let selections = editor.selections.disjoint_anchors();
        if selections.is_empty() {
            return None;
        }

        // Get the newest (primary) selection
        let selection = &selections[selections.len() - 1];

        // Convert anchors to offsets
        let start = selection.start.to_offset(&buffer_snapshot);
        let end = selection.end.to_offset(&buffer_snapshot);

        // If selection is empty (cursor with no range), return None
        if start == end {
            return None;
        }

        // Extract the text from the buffer
        let text: String = buffer_snapshot.text_for_range(start..end).collect();

        Some(text)
    }
}

impl CompletionMenuEditor for EditorWrapper {
    fn completion_replace_text_in_range(
        &mut self,
        range: std::ops::Range<usize>,
        new_text: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use multi_buffer::MultiBufferOffset;

        let start_point = {
            let snapshot = self.buffer.read(cx).read(cx);
            snapshot.offset_to_point(MultiBufferOffset(range.start))
        };
        let end_point = {
            let snapshot = self.buffer.read(cx).read(cx);
            snapshot.offset_to_point(MultiBufferOffset(range.end))
        };

        self.buffer.update(cx, |buffer, cx| {
            buffer.edit([(start_point..end_point, new_text)], None, cx);
        });

        let new_cursor_offset = range.start + new_text.len();
        self.set_cursor_offset(new_cursor_offset, window, cx);
    }

    fn completion_cursor_offset(&self, cx: &App) -> usize {
        self.get_cursor_offset(cx)
    }

    fn completion_text_string(&self, cx: &App) -> String {
        self.get_text(cx)
    }

    fn completion_origin(&self, _cx: &App) -> Option<Point<Pixels>> {
        // The completion menu is rendered as a child of the editor container
        // in QueryEditor, positioned via absolute layout. We place it just
        // below the first line for now; cursor-tracked positioning requires
        // pixel-level cursor info from the Zed Editor which is not easily
        // exposed.
        Some(Point::new(px(0.), px(24.)))
    }

    fn completion_focus_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let focus_handle = self.editor.read(cx).focus_handle(cx).clone();
        focus_handle.focus(window, cx);
    }
}

/// ZQLZ diagnostic structure
///
/// This is the interface that ZQLZ uses to report diagnostics.
/// It gets converted to Zed's diagnostic format internally.
#[derive(Clone)]
pub struct Diagnostic {
    /// The byte range in the buffer where this diagnostic applies
    pub range: Range<usize>,
    /// The severity level of the diagnostic
    pub severity: DiagnosticLevel,
    /// The human-readable error message
    pub message: String,
}

/// Diagnostic severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticLevel {
    /// Critical errors that prevent execution
    Error,
    /// Warnings that should be addressed
    Warning,
    /// Informational messages
    Info,
    /// Subtle hints for improvement
    Hint,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Basic test to verify the EditorWrapper compiles and can be created
    // Note: Full functional tests require window context which is complex to set up in unit tests
    // For comprehensive testing, use integration tests or manual testing
    #[test]
    fn test_diagnostic_types_compile() {
        // Test that our diagnostic types can be created
        let _diag = Diagnostic {
            range: 0..10,
            severity: DiagnosticLevel::Error,
            message: "Test error".to_string(),
        };

        assert_eq!(DiagnosticLevel::Error, DiagnosticLevel::Error);
        assert_eq!(DiagnosticLevel::Warning, DiagnosticLevel::Warning);
        assert_eq!(DiagnosticLevel::Info, DiagnosticLevel::Info);
        assert_eq!(DiagnosticLevel::Hint, DiagnosticLevel::Hint);
    }

    #[test]
    fn test_diagnostic_levels() {
        let error = DiagnosticLevel::Error;
        let warning = DiagnosticLevel::Warning;
        let info = DiagnosticLevel::Info;
        let hint = DiagnosticLevel::Hint;

        // Test that different levels are not equal
        assert_ne!(error, warning);
        assert_ne!(warning, info);
        assert_ne!(info, hint);
    }

    #[test]
    fn test_diagnostic_creation() {
        let diag1 = Diagnostic {
            range: 5..15,
            severity: DiagnosticLevel::Warning,
            message: "Unused variable".to_string(),
        };

        assert_eq!(diag1.range.start, 5);
        assert_eq!(diag1.range.end, 15);
        assert_eq!(diag1.severity, DiagnosticLevel::Warning);
        assert_eq!(diag1.message, "Unused variable");

        let diag2 = Diagnostic {
            range: 0..1,
            severity: DiagnosticLevel::Error,
            message: "Syntax error".to_string(),
        };

        assert_eq!(diag2.severity, DiagnosticLevel::Error);
        assert!(diag2.message.contains("Syntax"));
    }
}
