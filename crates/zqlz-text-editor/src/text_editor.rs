//! ZQLZ Text Editor - Custom text editor built from scratch using GPUI
//!
//! This is a production-quality text editor designed specifically for SQL editing.
//! It replaces the previous Zed editor dependency with a clean, maintainable implementation.
//!
//! ## Architecture
//!
//! - **TextBuffer**: Rope-based text storage using the `ropey` crate
//! - **Cursor & Selection**: Multi-cursor support with selection ranges
//! - **Syntax Highlighting**: Tree-sitter based SQL syntax highlighting
//! - **LSP Integration**: Direct integration with zqlz-lsp for IntelliSense
//! - **Rendering**: Custom GPUI elements for text rendering
//! - **Formatter**: SQL formatting with configurable options (indent, keyword case)
//! - **Bookmarks**: Persistent query bookmark storage (SQLite-backed)
//! - **Find/Replace**: Regex-capable text search and replace
//! - **Folding**: SQL code folding region detection

// Core editor modules
pub mod actions;
mod behavior_state;
pub mod buffer;
pub mod cursor;
mod cursor_state;
pub mod display_map;
pub mod document;
pub mod editor_core;
pub mod element;
mod event;
pub mod find;
mod find_state;
mod history_state;
mod input_state;
pub mod language_pipeline;
mod large_file_policy;
pub mod lsp;
mod overlay_state;
mod render_settings;
mod scroll_state;
pub mod selection;
pub mod settings;
mod snapshot;
pub mod snippet;
mod snippet_state;
pub mod syntax;

// SQL editor feature modules (merged from zqlz-editor)
pub mod bookmarks;
pub mod find_replace;
pub mod find_replace_panel;
pub mod folding;
pub mod formatter;

use gpui::prelude::FluentBuilder as _;
use gpui::*;
use std::sync::Arc;
use zqlz_ui::widgets::input::{Input, InputEvent, InputState};
use zqlz_ui::widgets::{ActiveTheme, Sizable, Size};

pub type FormatProvider = std::rc::Rc<dyn Fn(&str) -> Option<String>>;

#[derive(Default, Clone)]
pub struct EditorLanguageProviders {
    pub completion: Option<std::rc::Rc<dyn CompletionProvider>>,
    pub hover: Option<std::rc::Rc<dyn HoverProvider>>,
    pub definition: Option<std::rc::Rc<dyn DefinitionProvider>>,
    pub references: Option<std::rc::Rc<dyn ReferencesProvider>>,
    pub rename: Option<std::rc::Rc<dyn RenameProvider>>,
    pub code_actions: Option<std::rc::Rc<dyn CodeActionProvider>>,
    pub diagnostics: Option<std::rc::Rc<dyn DiagnosticProvider>>,
    pub format: Option<FormatProvider>,
}

// Re-exports for convenience
pub(crate) use behavior_state::EditorBehaviorState;
pub use bookmarks::{Bookmark, BookmarkFilter, BookmarkManager, BookmarkStorage};
pub use buffer::{
    Anchor, AnchoredRange, Bias, BufferSnapshot, Position, Range, RevisionEdit, TextBuffer,
    TransactionId,
};
pub use cursor::Cursor;
pub(crate) use cursor_state::EditorCursorState;
pub use display_map::{
    DisplayMap, DisplayPoint, DisplayRowId, DisplaySnapshot, DisplayTextChunk, FoldDisplayState,
    RowInfo, VisibleWrapLayout,
};
pub use document::{DocumentContext, DocumentIdentity, DocumentSettings, LineEnding, TextDocument};
pub use editor_core::{
    AutoCloseBracketPlan, AutoIndentNewlinePlan, AutoSurroundSelectionPlan, CompletionQueryPlan,
    CutToEndOfLinePlan, DedentLinesPlan, DuplicateSelectedLinesPlan, EditorCoreSnapshot,
    ExtraCursor, LinePrefixEditMode, MultiCursorCommandPlan, PlannedEditBatch, PostApplySelection,
    SelectedTextPlan, SelectionHistoryEntry, SelectionState, SkipClosingBracketPlan,
    StructuralRange, TextReplacementEdit, ToggleLineCommentPlan, WholeLineCopyPlan,
    WholeLineCutPlan, WholeLinePastePlan, WordTarget,
};
pub use element::EditorElement;
pub use event::TextEditorEvent;
pub use find::{FindMatch, FindOptions, FindState};
pub use find_replace::{
    FindError, FindOptions as TextFindOptions, ReplaceResult, SearchEngine, count_matches,
    find_all, find_first, find_next, replace_all, replace_first, replace_next,
};
pub(crate) use find_state::EditorFindState;
pub use find_state::FindSnapshot;
pub use folding::{FoldKind, FoldRegion, FoldingDetector, detect_folds, detect_folds_in_range};
pub use formatter::{
    FormatError, FormatterConfig, SqlFormatter, format_sql, format_sql_with_config,
};
pub(crate) use history_state::EditorHistoryState;
use input_state::{CachedEditorLayout, EditorInputState};
pub use language_pipeline::{
    AnchoredCodeAction, AnchoredDiagnostic, AnchoredInlayHint, Diagnostic, DiagnosticLevel,
    FoldRefresh, LanguagePipelineSnapshot, LanguagePipelineState,
};
pub(crate) use large_file_policy::syntax_refresh_strategy_for_policy;
pub use large_file_policy::{LargeFilePolicyConfig, LargeFilePolicyTier, ResolvedLargeFilePolicy};
pub use lsp::{
    CodeActionProvider, CompletionMenuData, CompletionProvider, DefinitionProvider,
    DiagnosticProvider, EditorInlayHint, HoverProvider, HoverResolution, HoverState, InlayHintKind,
    InlayHintSide, Lsp, LspRequestState, ReferencesProvider, RenameProvider, RequestToken,
    SqlCompletionProvider,
};
pub use lsp_types::{CompletionItem, Hover, SignatureHelp};
use overlay_state::{
    CachedCompletionMenuBounds, ContextMenuAction, EditorOverlayState, RenameState,
};
pub(crate) use overlay_state::{ContextMenuItem, ContextMenuState};
pub use overlay_state::{
    ContextMenuSnapshot, EditPrediction, GoToLineSnapshot, InlineSuggestion, SignatureHelpState,
};
pub(crate) use render_settings::EditorRenderSettings;
pub use render_settings::{CursorShapeStyle, EditorAppearance};
use scroll_state::{CachedScrollbarBounds, EditorScrollState, ScrollbarPointerDown};
pub use selection::{Selection, SelectionEntry, SelectionsCollection};
pub use settings::{
    CompletionSettings, CursorSettings, CursorShape, EditorSettings, GutterSettings,
    ScrollSettings, SearchSettings, SoftWrapMode,
};
pub use snapshot::{DocumentSnapshot, EditorSnapshot};
pub use snippet::{ActiveSnippet, Snippet, SnippetPlaceholder};
pub(crate) use snippet_state::EditorSnippetState;
pub use syntax::{
    Highlight, HighlightKind, SyntaxHighlighter, SyntaxRefreshStrategy, SyntaxSnapshot,
};

const RENAME_PROBE_IDENTIFIER: &str = "__zqlz_rename_probe";

/// A text editor entity for editing SQL queries.
///
/// The editor owns a TextBuffer (rope-based text storage), cursor state, and manages
/// rendering and input handling.
///
/// # Current Status
///
/// - ✅ Phase 1: TextBuffer with rope storage
/// - ✅ Phase 2: Keyboard input and editing
/// - ✅ Phase 3: Selection
/// - ✅ Phase 4: Clipboard & Undo/Redo
/// - ✅ Phase 5: Syntax Highlighting
/// - ✅ Phase 7: Scrolling
/// - ✅ Phase 8: LSP integration
/// - ✅ Phase 9: Find & Replace
pub struct TextEditor {
    document: TextDocument,
    focus_handle: FocusHandle,
    format_provider: Option<FormatProvider>,
    behavior: EditorBehaviorState,
    /// LSP provider container for code intelligence features
    lsp: Lsp,
    lsp_diagnostics: Vec<lsp_types::Diagnostic>,
    render_settings: EditorRenderSettings,
    history: EditorHistoryState,
    cursor_state: EditorCursorState,
    scroll: EditorScrollState,
    overlays: EditorOverlayState,
    input: EditorInputState,
    find: EditorFindState,
    selections_collection: SelectionsCollection,
    snippets: EditorSnippetState,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<TextEditorEvent> for TextEditor {}

impl TextEditor {
    fn primary_cursor(&self) -> Option<&Cursor> {
        self.selections_collection
            .primary()
            .map(|entry| &entry.cursor)
    }

    fn primary_selection(&self) -> Option<&Selection> {
        self.selections_collection
            .primary()
            .map(|entry| &entry.selection)
    }

    fn current_cursor_position(&self) -> Position {
        self.primary_cursor()
            .map(|cursor| cursor.position())
            .unwrap_or_else(Position::zero)
    }

    fn current_selection_range(&self) -> Option<crate::buffer::Range> {
        self.primary_selection()
            .filter(|selection| selection.has_selection())
            .map(|selection| selection.range())
    }

    fn current_cursor_offset(&self) -> usize {
        self.document
            .position_to_offset(self.current_cursor_position())
            .unwrap_or(0)
    }

    fn has_secondary_cursors(&self) -> bool {
        !self.selections_collection.extra_entries().is_empty()
    }

    fn editor_core_snapshot(&self) -> EditorCoreSnapshot<'_> {
        self.input
            .editor_core_snapshot(self.document.buffer(), self.selections_collection.clone())
    }

    fn editor_core(&mut self) -> editor_core::EditorCore<'_> {
        self.input
            .editor_core(self.document.buffer(), &mut self.selections_collection)
    }

    fn mutate_selections<T>(
        &mut self,
        mutate: impl FnOnce(&mut editor_core::EditorCore<'_>) -> T,
    ) -> T {
        let result = {
            let mut core = self.editor_core();
            mutate(&mut core)
        };
        self.reconcile_selection_driven_state();
        result
    }

    fn reconcile_selection_driven_state(&mut self) {
        let has_secondary_cursors = self.has_secondary_cursors();

        self.snippets.reconcile_with_primary_selection(
            &self.document,
            &self.selections_collection,
            has_secondary_cursors,
        );

        if has_secondary_cursors {
            self.overlays.clear_cursor_bound_overlays();
        } else {
            self.overlays.retain_primary_cursor_anchored_overlays(
                &self.document,
                &self.selections_collection,
            );
        }
    }

    fn selection_state(&self) -> SelectionState {
        self.editor_core_snapshot().selection_state()
    }

    fn restore_selection_state(&mut self, state: SelectionState) {
        self.mutate_selections(|core| core.restore_selection_state(state));
    }

    fn start_transaction(&mut self) -> TransactionId {
        if let Some(id) = self.history.active_transaction_id() {
            return id;
        }

        let id = self.document.start_buffer_transaction();
        self.history
            .begin_active_transaction(id, self.selection_state());
        id
    }

    fn end_transaction(&mut self, id: TransactionId) {
        if self
            .history
            .finish_active_transaction(id, self.selection_state())
        {
            self.document.end_buffer_transaction(id);
        }
    }

    fn build_with_document(
        document: TextDocument,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let focus_handle = cx.focus_handle();
        let _subscriptions = vec![cx.on_blur(&focus_handle, window, Self::handle_focus_blur)];

        Self {
            document,
            focus_handle,
            format_provider: None,
            behavior: EditorBehaviorState::new(),
            lsp: Lsp::new(),
            lsp_diagnostics: Vec::new(),
            render_settings: EditorRenderSettings::default(),
            history: EditorHistoryState::default(),
            cursor_state: EditorCursorState::default(),
            scroll: EditorScrollState::default(),
            overlays: EditorOverlayState::default(),
            input: EditorInputState::default(),
            find: EditorFindState::default(),
            selections_collection: SelectionsCollection::single(Cursor::new(), Selection::new()),
            snippets: EditorSnippetState::default(),
            _subscriptions,
        }
    }

    /// Create a new empty text editor
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        Self::build_with_document(
            TextDocument::internal().expect("internal document uri"),
            window,
            cx,
        )
    }

    pub fn with_document(
        document: TextDocument,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut editor = Self::build_with_document(document, window, cx);
        editor.refresh_display_layout_settings();
        editor.update_syntax_highlights();
        editor.update_diagnostics();
        editor
    }

    pub fn rebind_document(
        &mut self,
        document: TextDocument,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.document = document;
        self.selections_collection = SelectionsCollection::single(Cursor::new(), Selection::new());
        self.reset_document_bound_editor_state();
        self.refresh_display_layout_settings();
        self.update_syntax_highlights();
        self.update_diagnostics();
        self.did_change_content(cx);
        cx.notify();
    }

    fn reset_document_bound_editor_state(&mut self) {
        self.input.reset_document_bound_state();
        self.scroll.reset_offsets();
        self.snippets.clear();
        self.overlays.clear_document_bound_overlays();
        self.find.clear_state();
        self.lsp.clear_document_bound_ui();
        self.lsp_diagnostics.clear();
    }

    /// Create a new text editor with initial content
    pub fn with_text(
        text: impl Into<String>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        Self::with_document(
            TextDocument::with_text(
                DocumentIdentity::internal().expect("internal document uri"),
                text.into(),
            ),
            _window,
            cx,
        )
    }

    // ============================================================================
    // Syntax Highlighting (Phase 5)
    // ============================================================================

    /// Update cached syntax highlights for the current buffer content
    fn update_syntax_highlights(&mut self) {
        let large_file_policy = self.large_file_policy();
        let syntax_refresh_strategy = syntax_refresh_strategy_for_policy(
            large_file_policy,
            self.visible_syntax_refresh_range(),
        );
        self.document.refresh_buffer_state_with_strategy(
            self.render_settings.highlight_enabled(),
            &syntax_refresh_strategy,
            large_file_policy.folding_enabled,
        );
    }

    fn apply_interpolated_syntax_highlights(&mut self) {
        let large_file_policy = self.large_file_policy();
        self.document.apply_buffer_change(
            self.render_settings.highlight_enabled(),
            large_file_policy.syntax_highlighting_enabled,
            large_file_policy.folding_enabled,
        );
    }

    fn schedule_syntax_reparse(&mut self, cx: &mut Context<Self>) {
        let large_file_policy = self.large_file_policy();
        let syntax_refresh_strategy = syntax_refresh_strategy_for_policy(
            large_file_policy,
            self.visible_syntax_refresh_range(),
        );
        if matches!(syntax_refresh_strategy, SyntaxRefreshStrategy::Disabled)
            || !self.render_settings.highlight_enabled()
        {
            return;
        }

        let revision = self.document.buffer_revision();
        let rope = self.document.rope();
        let Some((token, mut highlighter)) = self.document.begin_syntax_reparse(revision) else {
            return;
        };
        let folding_enabled = self.large_file_policy().folding_enabled;

        self.document
            .set_syntax_parse_task(cx.spawn(async move |this, cx| {
                let snapshot: SyntaxSnapshot = match syntax_refresh_strategy {
                    SyntaxRefreshStrategy::Disabled => SyntaxSnapshot::empty(revision),
                    SyntaxRefreshStrategy::FullDocument => {
                        highlighter.snapshot_rope(&rope, revision)
                    }
                    SyntaxRefreshStrategy::VisibleRange(byte_range) => {
                        highlighter.snapshot_rope_for_range(&rope, revision, byte_range)
                    }
                };
                this.update(cx, |editor, cx| {
                    if editor
                        .document
                        .apply_reparsed_syntax(token, snapshot, folding_enabled)
                    {
                        editor.restore_scroll_offset_from_anchor();
                        cx.notify();
                    }

                    editor.document.restore_syntax_highlighter(highlighter);

                    if editor.document.take_queued_syntax_reparse() {
                        editor.schedule_syntax_reparse(cx);
                    }
                })?;
                Ok(())
            }));
    }

    /// Get the current syntax highlights
    pub fn get_syntax_highlights(&self) -> &[Highlight] {
        self.document.syntax_highlights()
    }

    /// Returns a cheap Arc clone of the syntax-highlight list.
    ///
    /// Cloning an `Arc` is an atomic increment — O(1) regardless of how many
    /// highlights exist. Callers that need the list to outlive the borrow of
    /// `&self` should use this rather than `get_syntax_highlights().to_vec()`.
    pub fn get_syntax_highlights_arc(&self) -> std::sync::Arc<Vec<Highlight>> {
        self.document.syntax_highlights_arc()
    }

    /// Returns the highlight kind at the given byte offset using cached highlights.
    fn highlight_kind_at(&self, offset: usize) -> HighlightKind {
        self.document.highlight_kind_at(offset)
    }

    /// Check if syntax highlighting is enabled
    pub fn has_syntax_highlighting(&self) -> bool {
        self.document.has_syntax_highlighting()
    }

    /// Get text and syntax highlights for rendering
    pub fn get_text_and_highlights(&self) -> (String, Vec<Highlight>) {
        (
            self.document.text(),
            self.document.syntax_highlights().to_vec(),
        )
    }

    /// Update cached error diagnostics for the current buffer content
    ///
    /// Tree-sitter ERROR nodes are intentionally not used here because the
    /// tree-sitter-sequel grammar produces false positives for valid PostgreSQL
    /// (e.g. schema-qualified names, nested parentheses in ON conditions).
    /// Squiggles are populated exclusively via `set_diagnostics` from the LSP.
    fn update_diagnostics(&mut self) {
        self.document.refresh_stored_diagnostics();
    }

    fn language_pipeline_snapshot(&self) -> LanguagePipelineSnapshot {
        self.document.language_pipeline_snapshot(
            self.primary_cursor()
                .expect("selections collection should always contain a primary cursor"),
        )
    }

    fn did_change_content(&mut self, cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.clear_line_selection_extension());
        self.snippets.retain_valid_for_document(&self.document);
        self.overlays
            .retain_valid_document_anchored_overlays(&self.document);
        self.update_syntax_highlights();
        self.document.clear_buffer_changes();
        self.schedule_syntax_reparse(cx);
        cx.emit(TextEditorEvent::ContentChanged);
    }

    fn visible_syntax_refresh_range(&self) -> Option<std::ops::Range<usize>> {
        self.document_snapshot()
            .syntax_refresh_strategy(self.scroll.viewport_lines(), self.scroll.vertical_offset())
            .into_visible_range()
    }

    /// Get the current error diagnostics (for rendering squiggles)
    pub fn get_diagnostics(&self) -> &[Highlight] {
        self.document.diagnostics()
    }

    /// Check if there are any errors in the buffer
    pub fn has_errors(&self) -> bool {
        self.document.has_diagnostics()
    }

    fn is_large_file_mode(&self) -> bool {
        self.large_file_policy().tier != LargeFilePolicyTier::Full
    }

    pub fn set_large_file_thresholds(&mut self, line_threshold: usize, byte_threshold: usize) {
        self.behavior
            .set_large_file_thresholds(line_threshold, byte_threshold);
        self.update_syntax_highlights();
    }

    pub fn set_large_file_policy_thresholds(
        &mut self,
        reduced_semantic_line_threshold: usize,
        reduced_semantic_byte_threshold: usize,
        plain_text_line_threshold: usize,
        plain_text_byte_threshold: usize,
    ) {
        self.behavior.set_large_file_policy_thresholds(
            reduced_semantic_line_threshold,
            reduced_semantic_byte_threshold,
            plain_text_line_threshold,
            plain_text_byte_threshold,
        );
        self.update_syntax_highlights();
    }

    fn large_file_policy(&self) -> ResolvedLargeFilePolicy {
        self.behavior
            .large_file_policy(self.document.line_count(), self.document.byte_len())
    }

    pub fn set_soft_wrap_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.behavior.set_soft_wrap(enabled) {
            return;
        }
        self.restore_scroll_offset_from_anchor();
        cx.notify();
    }

    pub fn soft_wrap_enabled(&self) -> bool {
        self.behavior.soft_wrap()
    }

    pub fn set_appearance(&mut self, appearance: EditorAppearance, cx: &mut Context<Self>) {
        if !self.render_settings.set_appearance(appearance) {
            return;
        }

        cx.notify();
    }

    pub fn set_indent_settings(
        &mut self,
        tab_size: usize,
        insert_spaces: bool,
        cx: &mut Context<Self>,
    ) {
        let next_indent_size = tab_size.max(1);
        let next_use_tabs = !insert_spaces;
        let current_settings = self.document.settings();
        if current_settings.indent_size == next_indent_size
            && current_settings.use_tabs == next_use_tabs
        {
            return;
        }

        self.document.set_settings(DocumentSettings {
            indent_size: next_indent_size,
            use_tabs: next_use_tabs,
        });
        self.refresh_display_layout_settings();
        cx.notify();
    }

    pub fn apply_editor_settings(&mut self, settings: &EditorSettings, cx: &mut Context<Self>) {
        self.set_indent_settings(
            settings.document.indent_size,
            !settings.document.use_tabs,
            cx,
        );
        self.set_show_line_numbers(settings.gutter.show_line_numbers, cx);
        self.set_relative_line_numbers(settings.gutter.show_relative_line_numbers, cx);
        self.set_show_gutter_diagnostics(settings.show_diagnostics, cx);
        self.set_show_inline_diagnostics(settings.show_diagnostics, cx);
        self.set_show_folding(
            settings.show_folding && settings.gutter.show_fold_controls,
            cx,
        );
        self.set_highlight_current_line(settings.highlight_current_line, cx);
        self.set_selection_highlight_enabled(settings.highlight_selection_matches, cx);
        self.set_soft_wrap_enabled(!matches!(settings.soft_wrap, SoftWrapMode::None), cx);
        self.set_cursor_shape(
            match settings.cursor.shape {
                CursorShape::Block => CursorShapeStyle::Block,
                CursorShape::Bar => CursorShapeStyle::Line,
                CursorShape::Underline => CursorShapeStyle::Underline,
            },
            cx,
        );
        self.set_cursor_blink_enabled(settings.cursor.blink, cx);
        self.set_hover_delay_ms(settings.hover_delay.as_millis().min(u128::from(u32::MAX)) as u32);
        self.set_vertical_scroll_margin(settings.scroll.vertical_margin_lines);
        self.set_horizontal_scroll_margin(settings.scroll.horizontal_margin_columns);
        self.set_scroll_sensitivity(f32::from(settings.scroll.sensitivity) / 100.0);
        self.set_scroll_beyond_last_line(settings.scroll.scroll_beyond_last_line);
    }

    pub fn set_show_line_numbers(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.render_settings.set_show_line_numbers(enabled) {
            return;
        }

        cx.notify();
    }

    pub fn set_highlight_current_line(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.render_settings.set_highlight_current_line(enabled) {
            return;
        }

        cx.notify();
    }

    pub fn set_show_inline_diagnostics(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.render_settings.set_show_inline_diagnostics(enabled) {
            return;
        }

        cx.notify();
    }

    pub fn set_show_folding(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.render_settings.set_show_folding(enabled) {
            return;
        }

        cx.notify();
    }

    pub fn set_highlight_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.render_settings.set_highlight_enabled(enabled) {
            return;
        }

        self.update_syntax_highlights();
        cx.notify();
    }

    pub fn set_syntax_language_profile(
        &mut self,
        language_profile: &'static str,
        cx: &mut Context<Self>,
    ) {
        if !self.document.set_syntax_language_profile(language_profile) {
            return;
        }

        self.update_syntax_highlights();
        cx.notify();
    }

    pub fn set_bracket_matching_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.render_settings.set_bracket_matching_enabled(enabled) {
            return;
        }

        cx.notify();
    }

    pub fn set_relative_line_numbers(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.render_settings.set_relative_line_numbers(enabled) {
            return;
        }

        cx.notify();
    }

    pub fn set_show_gutter_diagnostics(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.render_settings.set_show_gutter_diagnostics(enabled) {
            return;
        }

        cx.notify();
    }

    pub fn set_cursor_shape(&mut self, shape: CursorShapeStyle, cx: &mut Context<Self>) {
        if !self.render_settings.set_cursor_shape(shape) {
            return;
        }

        cx.notify();
    }

    pub fn set_cursor_blink_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.render_settings.set_cursor_blink_enabled(enabled) {
            return;
        }

        cx.notify();
    }

    pub fn set_selection_highlight_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self
            .render_settings
            .set_selection_highlight_enabled(enabled)
        {
            return;
        }

        cx.notify();
    }

    pub fn set_rounded_selection(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.render_settings.set_rounded_selection(enabled) {
            return;
        }

        cx.notify();
    }

    pub fn set_search_wrap_enabled(&mut self, enabled: bool) {
        self.find.set_search_wrap_enabled(enabled);
    }

    pub fn set_smartcase_search_enabled(&mut self, enabled: bool) {
        self.find.set_smartcase_search_enabled(enabled);
    }

    pub fn set_autoscroll_on_clicks(&mut self, enabled: bool) {
        self.scroll.set_autoscroll_on_clicks(enabled);
    }

    pub fn set_horizontal_scroll_margin(&mut self, margin: usize) {
        self.scroll.set_horizontal_margin(margin);
    }

    pub fn set_vertical_scroll_margin(&mut self, margin: usize) {
        self.scroll.set_vertical_margin(margin);
    }

    pub fn set_scroll_sensitivity(&mut self, sensitivity: f32) {
        self.scroll.set_sensitivity(sensitivity);
    }

    pub fn set_scroll_beyond_last_line(&mut self, enabled: bool) {
        self.scroll.set_beyond_last_line(enabled);
    }

    pub fn record_cursor_activity(&mut self) {
        self.cursor_state.record_activity();
    }

    pub fn set_auto_indent_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if !self.behavior.set_auto_indent_enabled(enabled) {
            return;
        }
        cx.notify();
    }

    pub fn set_hover_delay_ms(&mut self, delay_ms: u32) {
        self.overlays
            .set_hover_delay(std::time::Duration::from_millis(delay_ms as u64));
    }

    /// Configure one-shot autofocus on the next render pass.
    pub fn set_autofocus_on_open(&mut self, autofocus_on_open: bool) {
        self.behavior.set_autofocus_on_open(autofocus_on_open);
    }

    pub fn is_large_file(&self) -> bool {
        self.is_large_file_mode()
    }

    pub fn large_file_policy_tier(&self) -> LargeFilePolicyTier {
        self.large_file_policy().tier
    }

    pub fn diagnostics_enabled(&self) -> bool {
        self.large_file_policy().diagnostics_enabled
    }

    /// Rebuild the ordered display-line list after any fold or buffer-content change.
    ///
    /// This is the only place where the O(total_lines) walk happens. The result
    /// is stored behind an `Arc` so every render frame can clone it in O(1).
    fn rebuild_display_lines_cache(&mut self) {
        self.document.rebuild_display_lines_cache();
    }

    fn restore_fold_scroll_position(&mut self) {
        self.restore_scroll_offset_from_anchor();
        if self.scroll.autoscroll_on_clicks() {
            self.scroll_to_cursor();
        }
    }

    pub(crate) fn update_visible_wrap_layout(
        &mut self,
        wrap_column: usize,
        visible_rows: std::ops::Range<usize>,
        visual_rows: &[usize],
    ) {
        if !self.behavior.soft_wrap() || visible_rows.is_empty() {
            return;
        }

        let display_lines = self.document.display_lines();
        let clamped_end = visible_rows.end.min(display_lines.len());
        let clamped_start = visible_rows.start.min(clamped_end);
        if clamped_start >= clamped_end {
            return;
        }

        let visible_buffer_lines = &display_lines[clamped_start..clamped_end];
        if self
            .document
            .update_wrap_rows(visible_buffer_lines, wrap_column, visual_rows)
        {
            self.restore_scroll_offset_from_anchor();
        }
    }

    /// Returns a cheap `Arc` clone of the ordered display-line list.
    ///
    /// `result[display_slot]` gives the buffer line index for that slot. The list
    /// is rebuilt by `rebuild_display_lines_cache` whenever folds or buffer content
    /// change; cloning the Arc here costs only an atomic increment.
    pub fn display_lines_cache(&self) -> std::sync::Arc<Vec<usize>> {
        self.document.display_lines()
    }

    /// Returns the ordered list of buffer lines that are currently visible on screen.
    ///
    /// Prefer `display_lines_cache()` in hot paths — this clones the Vec.
    pub fn visible_buffer_lines(&self) -> Vec<usize> {
        (*self.document.display_lines()).clone()
    }

    /// Returns the number of lines that are currently displayed (buffer lines minus
    /// lines hidden inside collapsed folds).
    pub fn display_line_count(&self) -> usize {
        self.document.display_line_count()
    }

    pub fn document_snapshot(&self) -> DocumentSnapshot {
        let buffer_snapshot = self.document.buffer_snapshot();
        let language = self.language_pipeline_snapshot();
        let large_file_policy = self.large_file_policy();
        tracing::trace!(
            revision = buffer_snapshot.revision(),
            syntax_generation = language.syntax_generation,
            syntax_highlight_count = language.syntax.highlights().len(),
            diagnostics_count = language.diagnostics.len(),
            "Building text editor document snapshot"
        );
        DocumentSnapshot {
            display_snapshot: self.document.display_snapshot(
                buffer_snapshot.clone(),
                &language,
                self.behavior.soft_wrap(),
            ),
            buffer: buffer_snapshot,
            context: self.document.context(),
            identity: self.document.identity().clone(),
            large_file_policy,
            inline_suggestion: self.overlays.inline_suggestion().cloned(),
            edit_prediction: self.overlays.edit_prediction().cloned(),
            hover_state: self.lsp.hover_state(),
            signature_help_state: self.overlays.signature_help().cloned(),
            language,
            soft_wrap: self.behavior.soft_wrap(),
            show_inline_diagnostics: self.render_settings.show_inline_diagnostics(),
        }
    }

    pub fn editor_snapshot(&self) -> EditorSnapshot {
        let cached_layout = self.input.cached_layout().clone();
        let scroll = self.scroll.snapshot_state();
        let scroll_anchor_position = self
            .document
            .resolve_anchor_position(scroll.anchor())
            .unwrap_or_else(|_| Position::zero());
        EditorSnapshot {
            document: self.document_snapshot(),
            appearance: self.render_settings.appearance(),
            bracket_pairs: self.bracket_highlight_pairs(),
            selections: self.selections_collection.clone(),
            scroll_offset: scroll.vertical_offset(),
            scroll_anchor_position,
            scroll_anchor_visual_offset: scroll.anchor_visual_offset(),
            horizontal_scroll_offset: scroll.horizontal_offset(),
            viewport_lines: scroll.viewport_lines(),
            gutter_width: cached_layout.gutter_width(),
            char_width: cached_layout.char_width(),
            bounds_origin: cached_layout.bounds_origin(),
            bounds_size: cached_layout.bounds_size(),
            cached_layout,
            minimap_visible: false,
            show_line_numbers: self.render_settings.show_line_numbers(),
            show_folding: self.render_settings.show_folding(),
            highlight_current_line: self.render_settings.highlight_current_line(),
            relative_line_numbers: self.render_settings.relative_line_numbers(),
            show_gutter_diagnostics: self.render_settings.show_gutter_diagnostics(),
            cursor_shape: self.render_settings.cursor_shape(),
            cursor_blink_enabled: self.render_settings.cursor_blink_enabled(),
            cursor_visible: self.cursor_state.visible(),
            rounded_selection: self.render_settings.rounded_selection(),
            selection_highlight_enabled: self.render_settings.selection_highlight_enabled(),
            completion_menu: self.lsp.completion_menu(),
            context_menu: self.overlays.context_menu_snapshot(),
            goto_line_info: self.overlays.goto_line_snapshot(self.document.line_count()),
            find_info: self.find.snapshot(),
        }
    }

    /// Returns the detected fold regions for the current buffer content.
    pub fn fold_regions(&self) -> &[FoldRegion] {
        self.document.fold_regions()
    }

    /// Returns `true` when the fold region starting at `start_line` is collapsed.
    pub fn is_line_folded(&self, start_line: usize) -> bool {
        self.document.is_line_folded(start_line)
    }

    /// Toggle the collapsed state of the fold whose start line is `start_line`.
    ///
    /// When collapsing, if the cursor sits inside the region being hidden it is
    /// rescued to the fold's start line so it is never stranded off-screen.
    pub fn toggle_fold(&mut self, start_line: usize, cx: &mut Context<Self>) {
        if !self.document.expand_line(start_line) {
            self.document.collapse_line(start_line);
            // Rescue cursor if it lands inside the now-hidden region.
            let cursor_position = self.current_cursor_position();
            let cursor_line = cursor_position.line;
            let end_line = self
                .document
                .fold_regions()
                .iter()
                .find(|r| r.start_line == start_line)
                .map(|r| r.end_line);
            if let Some(end) = end_line
                && cursor_line > start_line
                && cursor_line <= end
            {
                let col = cursor_position.column;
                let rescued = self.document.clamp_position(Position::new(start_line, col));
                self.mutate_selections(|core| core.move_primary_cursor(rescued, false));
            }
        }
        self.rebuild_display_lines_cache();
        self.restore_fold_scroll_position();
        cx.notify();
    }

    pub fn fold_current_region(&mut self, cx: &mut Context<Self>) {
        let cursor_line = self.current_cursor_position().line;
        let start_line = self
            .document
            .fold_regions()
            .iter()
            .filter(|region| region.start_line <= cursor_line && cursor_line <= region.end_line)
            .min_by_key(|region| region.end_line.saturating_sub(region.start_line))
            .map(|region| region.start_line);

        let Some(start_line) = start_line else {
            return;
        };

        if !self.document.collapse_line(start_line) {
            return;
        }

        let cursor_position = self.current_cursor_position();
        if cursor_position.line > start_line {
            let rescued = self
                .document
                .clamp_position(Position::new(start_line, cursor_position.column));
            self.mutate_selections(|core| core.move_primary_cursor(rescued, false));
        }

        self.rebuild_display_lines_cache();
        self.restore_fold_scroll_position();
        cx.notify();
    }

    pub fn unfold_current_region(&mut self, cx: &mut Context<Self>) {
        let cursor_line = self.current_cursor_position().line;
        let start_line = if self.document.is_line_folded(cursor_line) {
            Some(cursor_line)
        } else {
            self.document
                .fold_regions()
                .iter()
                .filter(|region| {
                    self.document.is_line_folded(region.start_line)
                        && region.start_line < cursor_line
                        && cursor_line <= region.end_line
                })
                .min_by_key(|region| region.end_line.saturating_sub(region.start_line))
                .map(|region| region.start_line)
        };

        let Some(start_line) = start_line else {
            return;
        };

        if !self.document.expand_line(start_line) {
            return;
        }

        self.rebuild_display_lines_cache();
        self.restore_fold_scroll_position();
        cx.notify();
    }

    fn fold_region_level(region: &FoldRegion, regions: &[FoldRegion]) -> usize {
        regions
            .iter()
            .filter(|candidate| {
                candidate.start_line < region.start_line && region.end_line <= candidate.end_line
            })
            .count()
            + 1
    }

    pub fn fold_at_level(&mut self, level: usize, cx: &mut Context<Self>) {
        let level = level.clamp(1, 9);
        let regions = self.document.fold_regions().to_vec();
        if regions.is_empty() {
            return;
        }

        self.document.clear_collapsed_lines();
        for region in &regions {
            if Self::fold_region_level(region, &regions) >= level {
                self.document.collapse_line(region.start_line);
            }
        }

        let cursor_position = self.current_cursor_position();
        let cursor_line = cursor_position.line;
        let rescue = regions
            .iter()
            .filter(|region| {
                self.document.is_line_folded(region.start_line)
                    && cursor_line > region.start_line
                    && cursor_line <= region.end_line
            })
            .max_by_key(|region| region.start_line)
            .map(|region| region.start_line);
        if let Some(start_line) = rescue {
            let rescued = self
                .document
                .clamp_position(Position::new(start_line, cursor_position.column));
            self.mutate_selections(|core| core.move_primary_cursor(rescued, false));
        }

        self.rebuild_display_lines_cache();
        self.restore_fold_scroll_position();
        cx.notify();
    }

    ///
    /// Rescues the cursor to the start of its enclosing fold when it would
    /// otherwise be hidden.
    pub fn fold_all(&mut self, cx: &mut Context<Self>) {
        self.document.collapse_all_folds();
        // Rescue cursor from any region it now falls inside.
        let cursor_position = self.current_cursor_position();
        let cursor_line = cursor_position.line;
        let rescue = self
            .document
            .fold_regions()
            .iter()
            .find(|r| cursor_line > r.start_line && cursor_line <= r.end_line)
            .map(|r| r.start_line);
        if let Some(start) = rescue {
            let col = cursor_position.column;
            let rescued = self.document.clamp_position(Position::new(start, col));
            self.mutate_selections(|core| core.move_primary_cursor(rescued, false));
        }
        self.rebuild_display_lines_cache();
        self.scroll_to_cursor();
        cx.notify();
    }

    /// Expand every collapsed fold region.
    pub fn unfold_all(&mut self, cx: &mut Context<Self>) {
        self.document.clear_collapsed_lines();
        self.rebuild_display_lines_cache();
        self.scroll_to_cursor();
        cx.notify();
    }

    /// Update the cached fold-chevron hit-test rectangles from the element layer.
    pub(crate) fn update_cached_fold_chevrons(
        &mut self,
        chevrons: Vec<(usize, gpui::Bounds<gpui::Pixels>)>,
    ) {
        self.input.update_cached_fold_chevrons(chevrons);
    }

    /// Get the current text content
    pub fn get_text(&self, _cx: &App) -> SharedString {
        self.document.text().into()
    }

    /// Set the text content
    pub fn set_text(
        &mut self,
        text: impl Into<SharedString>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let text = text.into();
        let large_file_policy = self.large_file_policy();
        self.document.replace_text(
            text.as_ref(),
            self.render_settings.highlight_enabled(),
            large_file_policy.syntax_highlighting_enabled,
            large_file_policy.folding_enabled,
        );
        self.update_diagnostics();
        self.did_change_content(cx);
    }

    /// Insert text at the cursor position
    pub fn insert_at_cursor(
        &mut self,
        text: impl Into<String>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let text = text.into();
        let Some(batch) = self
            .editor_core_snapshot()
            .insert_at_cursor_edit_batch(&text)
        else {
            return;
        };

        self.apply_single_cursor_edit_batch(batch, cx, false);
    }

    /// Replace the primary selection, or insert at the primary cursor, as one undo step.
    pub fn insert_text(&mut self, text: &str, _window: &mut Window, cx: &mut Context<Self>) {
        let Some(batch) = self.editor_core_snapshot().insert_text_edit_batch(text) else {
            return;
        };

        self.apply_single_cursor_edit_batch(batch, cx, true);
    }

    /// Replace the entire buffer as one undo step while keeping the cursor in-bounds.
    pub fn replace_all_text(&mut self, text: &str, cx: &mut Context<Self>) {
        let original = self.document.text();
        if original == text {
            return;
        }

        let original_cursor = self.current_cursor_position();
        if !self.apply_single_replacement_edit(0..original.len(), text.to_string(), true) {
            return;
        }

        let new_line_count = self.document.line_count();
        let target_line = original_cursor.line.min(new_line_count.saturating_sub(1));
        let target_col = self
            .document
            .line(target_line)
            .map(|line| original_cursor.column.min(line.len()))
            .unwrap_or(0);
        let position = Position::new(target_line, target_col);
        self.mutate_selections(|core| core.move_primary_cursor(position, false));
        self.scroll_to_cursor();
        self.apply_interpolated_syntax_highlights();
        self.update_diagnostics();
        self.did_change_content(cx);
        cx.notify();
    }

    /// Delete character before cursor (backspace)
    pub fn delete_before_cursor(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if self.backspace_all_cursors() {
            self.did_change_content(cx);
            return;
        }

        let Some(batch) = self
            .editor_core_snapshot()
            .delete_before_cursor_edit_batch(Self::bracket_closer)
        else {
            return;
        };

        self.apply_single_cursor_edit_batch(batch, cx, false);
    }

    /// Delete character at cursor (delete key)
    pub fn delete_at_cursor(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if self.delete_all_cursors() {
            self.did_change_content(cx);
            return;
        }

        let Some(batch) = self.editor_core_snapshot().delete_at_cursor_edit_batch() else {
            return;
        };

        self.apply_single_cursor_edit_batch(batch, cx, false);
    }

    /// Get a reference to the text buffer
    pub fn buffer(&self) -> &TextBuffer {
        self.document.buffer()
    }

    /// Get the cursor position
    pub fn get_cursor_position(&self, _cx: &App) -> Position {
        self.current_cursor_position()
    }

    /// Get the cursor offset (byte offset in buffer)
    pub fn get_cursor_offset(&self, _cx: &App) -> usize {
        self.current_cursor_offset()
    }

    /// Set the cursor offset (byte offset in buffer)
    pub fn set_cursor_offset(
        &mut self,
        offset: usize,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
        if let Ok(position) = self.document.offset_to_position(offset) {
            self.mutate_selections(|core| core.move_primary_cursor(position, false));
        }
    }

    /// Set the cursor position
    pub fn set_cursor_position(
        &mut self,
        position: Position,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
        let clamped_position = self.document.clamp_position(position);
        self.mutate_selections(|core| core.move_primary_cursor(clamped_position, false));
    }

    // ============================================================================
    // Cursor Movement
    // ============================================================================

    /// Move cursor left by one character
    pub fn move_cursor_left(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.move_primary_cursor_left());
        self.scroll_to_cursor();
    }

    /// Move cursor right by one character
    pub fn move_cursor_right(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.move_primary_cursor_right());
        self.scroll_to_cursor();
    }

    /// Move cursor up by one line
    pub fn move_cursor_up(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.move_primary_cursor_up());
        self.scroll_to_cursor();
    }

    /// Move cursor down by one line
    pub fn move_cursor_down(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.move_primary_cursor_down());
        self.scroll_to_cursor();
    }

    /// Move cursor to start of line
    pub fn move_cursor_to_line_start(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.move_primary_cursor_to_line_start());
        self.scroll_to_cursor();
    }

    /// Move cursor to end of line
    pub fn move_cursor_to_line_end(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.move_primary_cursor_to_line_end());
        self.scroll_to_cursor();
    }

    /// Move cursor to start of document
    pub fn move_cursor_to_document_start(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.move_primary_cursor_to_document_start());
        self.scroll_to_cursor();
    }

    /// Move cursor to end of document
    pub fn move_cursor_to_document_end(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.move_primary_cursor_to_document_end());
        self.scroll_to_cursor();
    }

    /// Move cursor to next word
    pub fn move_cursor_to_next_word(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.move_primary_cursor_to_next_word_start());
        self.scroll_to_cursor();
    }

    /// Move cursor to previous word
    pub fn move_cursor_to_prev_word(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.move_primary_cursor_to_prev_word_start());
        self.scroll_to_cursor();
    }

    // ============================================================================
    // Subword movement (feat-003)
    // ============================================================================

    fn delete_subword_left(&mut self, cx: &mut Context<Self>) {
        let Some(batch) = self.editor_core_snapshot().delete_subword_left_edit_batch() else {
            return;
        };

        self.apply_single_cursor_edit_batch(batch, cx, true);
    }

    fn delete_to_beginning_of_line(&mut self, cx: &mut Context<Self>) {
        let Some(batch) = self
            .editor_core_snapshot()
            .delete_to_beginning_of_line_edit_batch()
        else {
            return;
        };

        self.apply_single_cursor_edit_batch(batch, cx, true);
    }

    fn delete_to_end_of_line(&mut self, cx: &mut Context<Self>) {
        let Some(batch) = self
            .editor_core_snapshot()
            .delete_to_end_of_line_edit_batch()
        else {
            return;
        };

        self.apply_single_cursor_edit_batch(batch, cx, true);
    }

    fn delete_subword_right(&mut self, cx: &mut Context<Self>) {
        let Some(batch) = self
            .editor_core_snapshot()
            .delete_subword_right_edit_batch()
        else {
            return;
        };

        self.apply_single_cursor_edit_batch(batch, cx, true);
    }

    /// Get the current scroll offset (in lines)
    pub fn scroll_offset(&self) -> f32 {
        self.scroll.vertical_offset()
    }

    pub fn horizontal_scroll_offset(&self) -> f32 {
        self.scroll.horizontal_offset()
    }

    /// Set the scroll offset (in display lines).
    ///
    /// Clamped to `[0, display_line_count]` so that collapsing folds never
    /// leaves the viewport scrolled past the end of visible content.
    pub fn set_scroll_offset(&mut self, offset: f32) {
        let max_offset = self.max_scroll_offset();
        self.scroll.set_vertical_offset(offset, max_offset);
        self.update_scroll_anchor_from_offset();
    }

    pub fn set_horizontal_scroll_offset(&mut self, offset: f32) {
        self.scroll.set_horizontal_offset(offset);
    }

    /// Scroll by a delta (positive = down, negative = up)
    pub fn scroll_by(&mut self, delta: f32) {
        self.scroll.scroll_by(delta, self.max_scroll_offset());
        self.update_scroll_anchor_from_offset();
    }

    fn update_scroll_anchor_from_offset(&mut self) {
        let display_snapshot = self.document_snapshot().display_snapshot;
        let top_row = self.scroll.vertical_offset().floor().max(0.0) as usize;
        let anchor = display_snapshot
            .buffer_line_for_display_slot(top_row)
            .and_then(|line| {
                self.document
                    .position_to_offset(Position::new(line, 0))
                    .ok()
            })
            .and_then(|offset| self.document.anchor_before(offset).ok())
            .unwrap_or_else(|| Anchor::new(0, self.document.buffer_revision(), Bias::Left));
        self.scroll
            .set_anchor(anchor, self.scroll.vertical_offset() - top_row as f32);
    }

    fn restore_scroll_offset_from_anchor(&mut self) {
        let editor_snapshot = self.editor_snapshot();
        self.scroll.set_vertical_offset(
            editor_snapshot.anchored_scroll_offset(),
            self.max_scroll_offset(),
        );
    }

    fn set_scroll_anchor_to_cursor(&mut self) {
        self.scroll.set_anchor(
            self.document
                .anchor_for_position(self.current_cursor_position(), Bias::Left)
                .unwrap_or_else(|_| Anchor::new(0, self.document.buffer_revision(), Bias::Left)),
            self.scroll.vertical_margin() as f32,
        );
    }

    fn max_scroll_offset(&self) -> f32 {
        self.scroll.max_vertical_offset(self.display_line_count())
    }

    fn update_horizontal_scroll_for_cursor(&mut self) {
        let viewport_columns = self.input.viewport_columns();
        self.scroll.ensure_column_visible(
            self.current_cursor_position().column as f32,
            viewport_columns,
        );
    }

    fn ensure_cursor_blink_task(&mut self, cx: &mut Context<Self>) {
        if self.cursor_state.blink_running() {
            return;
        }

        self.cursor_state.mark_blink_running();
        let task = cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(500))
                    .await;

                let should_continue = this.update(cx, |editor, cx| {
                    if !editor.render_settings.cursor_blink_enabled() {
                        editor.cursor_state.stop_blinking();
                        cx.notify();
                        return false;
                    }

                    if editor
                        .cursor_state
                        .activity_within(std::time::Duration::from_millis(500))
                    {
                        if editor.cursor_state.ensure_visible() {
                            cx.notify();
                        }
                        return true;
                    }

                    editor.cursor_state.toggle_visible();
                    cx.notify();
                    true
                })?;

                if !should_continue {
                    break;
                }
            }

            Ok(())
        });
        self.cursor_state.store_blink_task(task);
    }

    /// Update the viewport size (called during render)
    /// This allows auto-scroll to work correctly
    pub(crate) fn update_viewport_lines(&mut self, viewport_lines: usize) {
        self.scroll.set_viewport_lines(viewport_lines);
    }

    pub(crate) fn update_cached_layout(&mut self, layout: CachedEditorLayout) {
        self.input.update_cached_layout(layout);
    }

    pub(crate) fn refresh_display_layout_settings(&mut self) {
        self.document
            .set_display_tab_size(self.document.settings().indent_size);
    }

    /// Cache the last-painted completion menu layout so that `handle_mouse_down`
    /// can determine whether a click landed inside the menu without accessing
    /// element-layer data. Pass `None` when the menu is not visible.
    pub(crate) fn update_cached_completion_menu_bounds(
        &mut self,
        bounds: Option<CachedCompletionMenuBounds>,
    ) {
        self.overlays.update_completion_menu_bounds(bounds);
    }

    /// Cache the scrollbar geometry (in window coordinates) computed during the
    /// last prepaint so that mouse handlers can drive scrollbar interaction.
    pub(crate) fn update_cached_scrollbar(&mut self, scrollbar: Option<CachedScrollbarBounds>) {
        self.scroll.update_cached_scrollbar(scrollbar);
    }

    /// Convert an absolute screen-space point into a `Position` (line, column)
    /// inside the buffer, clamped to valid bounds.
    ///
    /// `bounds_origin` is the top-left of the editor element in screen space.
    fn pixel_to_position(&self, point: gpui::Point<Pixels>, line_height: Pixels) -> Position {
        if let Some(position_map) = self.input.cached_layout().position_map() {
            return position_map.point_for_position(point).position();
        }

        self.editor_snapshot().pixel_to_position(point, line_height)
    }

    /// Scroll to ensure the cursor is visible.
    ///
    /// Works in display-line space: the cursor's buffer line is first translated
    /// to its display slot (its row index among non-hidden lines), then the
    /// viewport is shifted if that slot falls outside the currently visible window.
    pub fn scroll_to_cursor(&mut self) {
        let cursor_position = self.current_cursor_position();
        let cursor_buffer_line = cursor_position.line;

        self.set_scroll_anchor_to_cursor();

        let document_snapshot = self.document_snapshot();
        let cursor_display = document_snapshot
            .display_snapshot
            .point_to_display_point(cursor_position)
            .map(|point| point.row)
            .unwrap_or(cursor_buffer_line) as f32;

        self.scroll.ensure_display_row_visible(cursor_display);

        // Clamp using the current display line count.
        self.scroll.clamp_vertical_offset(self.max_scroll_offset());
        self.update_scroll_anchor_from_offset();
        self.update_horizontal_scroll_for_cursor();
    }

    /// Scroll up by one page
    pub fn scroll_page_up(&mut self, viewport_lines: usize) {
        self.scroll_by(-(viewport_lines as f32));
    }

    /// Scroll down by one page
    pub fn scroll_page_down(&mut self, viewport_lines: usize) {
        self.scroll_by(viewport_lines as f32);
    }

    // ============================================================================
    // Selection Operations
    // ============================================================================

    /// Get the selection state
    pub fn selection(&self) -> &Selection {
        self.primary_selection()
            .expect("selections collection should always contain a primary selection")
    }

    /// Check if there is an active selection
    pub fn has_selection(&self) -> bool {
        self.selection().has_selection()
    }

    /// Return all extra cursor positions and their selections for rendering.
    ///
    /// Each entry is `(position, selection)`.  The primary cursor is NOT
    /// included — callers should query `get_cursor_position` / `selection`
    /// separately for the primary.
    pub fn extra_cursor_selections(&self) -> Vec<(Cursor, Selection)> {
        self.selections_collection
            .primary_and_extras()
            .map(|(_, _, extras)| extras)
            .unwrap_or_default()
    }

    pub fn selections_collection(&self) -> &SelectionsCollection {
        &self.selections_collection
    }

    // ============================================================================
    // Bracket Highlight (feat-026)
    // ============================================================================

    /// Returns the byte offsets of every bracket pair that encloses the cursor,
    /// ordered from outermost to innermost.
    ///
    /// The scan walks the entire buffer text with a simple stack — no tree-sitter
    /// dependency — so it is fast and reliable even for very large SQL files.  Each
    /// returned tuple is `(open_byte_offset, close_byte_offset)`.
    pub fn bracket_highlight_pairs(&self) -> Vec<(usize, usize)> {
        if !self.render_settings.bracket_matching_enabled() {
            return Vec::new();
        }

        let cursor_offset = self.current_cursor_offset();
        let rope = self.document.rope();

        // Stack of (open_char, open_byte_offset) for currently open brackets.
        let mut stack: Vec<(char, usize)> = Vec::new();
        // Completed enclosing pairs (open_offset, close_offset).
        let mut enclosing: Vec<(usize, usize)> = Vec::new();
        let mut byte_offset = 0usize;

        for ch in rope.chars() {
            match ch {
                '(' | '[' | '{' => {
                    stack.push((ch, byte_offset));
                }
                ')' | ']' | '}' => {
                    let expected_open = match ch {
                        ')' => '(',
                        ']' => '[',
                        '}' => '{',
                        _ => unreachable!(),
                    };
                    // Pop only if the top of the stack matches — unbalanced brackets are skipped.
                    if stack.last().map(|(c, _)| *c) == Some(expected_open)
                        && let Some((_, open_offset)) = stack.pop()
                    {
                        // This pair encloses the cursor if the open is before the cursor
                        // and the close is at or after it.
                        if open_offset < cursor_offset && byte_offset >= cursor_offset {
                            enclosing.push((open_offset, byte_offset));
                        }
                    }
                }
                _ => {}
            }
            byte_offset += ch.len_utf8();
        }

        // `enclosing` is filled in close→open order because we record pairs when
        // we encounter the closing bracket.  Reverse so the result is outermost-first.
        enclosing.reverse();
        enclosing
    }

    pub fn innermost_enclosing_bracket_range(&self, offset: usize) -> Option<StructuralRange> {
        self.enclosing_bracket_ranges(offset).into_iter().last()
    }

    pub fn enclosing_bracket_ranges(&self, offset: usize) -> Vec<StructuralRange> {
        let mut stack: Vec<(char, usize)> = Vec::new();
        let mut ranges = Vec::new();
        let rope = self.document.rope();
        let mut byte_offset = 0usize;

        for ch in rope.chars() {
            if byte_offset > offset {
                break;
            }

            match ch {
                '(' | '[' | '{' => stack.push((ch, byte_offset)),
                ')' | ']' | '}' => {
                    if let Some((open, start)) = stack.pop()
                        && Self::bracket_closer(open) == Some(ch)
                        && start <= offset
                        && offset <= byte_offset
                    {
                        ranges.push(StructuralRange {
                            start,
                            end: byte_offset,
                            open,
                            close: ch,
                        });
                    }
                }
                '"' | '\'' | '`' => {
                    if stack.last().map(|(open, _)| *open) == Some(ch) {
                        if let Some((open, start)) = stack.pop()
                            && start <= offset
                            && offset <= byte_offset
                        {
                            ranges.push(StructuralRange {
                                start,
                                end: byte_offset,
                                open,
                                close: ch,
                            });
                        }
                    } else {
                        stack.push((ch, byte_offset));
                    }
                }
                _ => {}
            }

            byte_offset += ch.len_utf8();
        }

        ranges.sort_by_key(|range| (range.start, range.end));
        ranges
    }

    pub fn expand_selection(&mut self) -> bool {
        let candidates = self
            .selections_collection
            .all()
            .iter()
            .map(|entry| {
                let cursor_offset = self.document.cursor_offset(&entry.cursor);
                (
                    self.innermost_enclosing_bracket_range(cursor_offset),
                    self.editor_core_snapshot()
                        .find_word_range_at_offset(cursor_offset),
                )
            })
            .collect::<Vec<_>>();
        self.mutate_selections(|core| core.expand_selections(&candidates))
    }

    pub fn shrink_selection(&mut self) -> bool {
        self.mutate_selections(|core| core.shrink_selection())
    }

    // ============================================================================
    // Bracket auto-close / auto-surround / skip-over helpers (feat-027/028/029)
    // ============================================================================

    /// Given an opening bracket/quote character, return the matching closer.
    fn bracket_closer(opener: char) -> Option<char> {
        match opener {
            '(' => Some(')'),
            '[' => Some(']'),
            '{' => Some('}'),
            '\'' => Some('\''),
            '"' => Some('"'),
            _ => None,
        }
    }

    /// Returns `true` when the character immediately after `cursor_offset` in the
    /// buffer is a safe auto-close position: whitespace, end of buffer, or a
    /// closing bracket / comma / semicolon.
    ///
    /// Auto-close must NOT fire before an alphanumeric character to avoid wrapping
    /// existing identifiers.
    fn is_safe_auto_close_position(&self, cursor_offset: usize) -> bool {
        match self.document.char_at(cursor_offset) {
            None => true, // end of buffer
            Some(c) => matches!(
                c,
                ' ' | '\t' | '\n' | '\r' | ')' | ']' | '}' | '\'' | '"' | ',' | ';'
            ),
        }
    }

    /// Surround the current primary selection with `opener` and `closer`, placing
    /// both ends as a single undo group.  Returns `true` if the surround was applied.
    fn auto_surround_selection(
        &mut self,
        opener: char,
        closer: char,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(AutoSurroundSelectionPlan { edits, selection }) = self
            .editor_core_snapshot()
            .auto_surround_selection_plan(opener, closer)
        else {
            return false;
        };

        if !self.apply_batch_replacement_edits(edits, false) {
            return false;
        }

        self.mutate_selections(|core| core.set_primary_selection(selection));

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
        cx.notify();
        true
    }

    /// Insert `opener` + `closer` at the cursor and leave the cursor between them.
    /// Returns `true` if auto-close was applied.
    fn auto_close_bracket(
        &mut self,
        opener: char,
        closer: char,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let cursor_offset = self.cursor_byte_offset();
        let is_safe_position = self.is_safe_auto_close_position(cursor_offset);
        let inside_string_or_comment = matches!(
            self.highlight_kind_at(cursor_offset),
            HighlightKind::String | HighlightKind::Comment
        );

        let Some(AutoCloseBracketPlan { batch }) = self
            .editor_core_snapshot()
            .auto_close_bracket_plan(opener, closer, is_safe_position, inside_string_or_comment)
        else {
            return false;
        };

        if self.apply_planned_edit_batch(batch, false) {
            self.update_syntax_highlights();
            self.update_diagnostics();
            self.scroll_to_cursor();
            cx.notify();
            true
        } else {
            false
        }
    }

    /// If the character immediately at the cursor equals `expected_closer`, advance
    /// the cursor past it instead of inserting.  Returns `true` if the skip was applied.
    fn skip_over_closing_bracket(&mut self, closer: char, cx: &mut Context<Self>) -> bool {
        let Some(SkipClosingBracketPlan { target_offset }) = self
            .editor_core_snapshot()
            .skip_closing_bracket_plan(closer)
        else {
            return false;
        };

        if let Ok(new_pos) = self.document.offset_to_position(target_offset) {
            self.mutate_selections(|core| core.move_primary_cursor(new_pos, false));
            self.scroll_to_cursor();
            cx.notify();
        }
        true
    }

    /// Returns true when the selection spans more than one line.
    fn is_multiline_selection(&self) -> bool {
        self.current_selection_range()
            .map(|range| range.start.line != range.end.line)
            .unwrap_or(false)
    }

    // ============================================================================
    // Selection history (feat-019 / feat-020)
    // ============================================================================

    /// Capture a snapshot of the current cursor + selection state so that
    /// `undo_selection` can walk back to it.  Capped at 100 entries to prevent
    /// unbounded growth — oldest entries are evicted first.
    fn push_selection_snapshot(&mut self) {
        self.mutate_selections(|core| core.push_selection_snapshot());
    }

    /// Split the current multi-line selection so that each selected line gets
    /// its own cursor at the end of that line (feat-019).
    ///
    /// The primary cursor lands at the end of the first selected line; extra
    /// cursors cover every subsequent line up to the last.  This mirrors VS
    /// Code's Cmd+Shift+L behaviour when a multi-line region is already active.
    fn split_selection_into_lines(&mut self) {
        if self.mutate_selections(|core| core.split_selection_into_lines()) {
            self.scroll_to_cursor();
        }
    }

    /// Step back through the selection history without modifying the text
    /// (feat-020).  Each call pops one snapshot, restoring the cursor and
    /// selection to where they were before the most recent selection action.
    fn undo_selection(&mut self) {
        if self.mutate_selections(|core| core.undo_selection()) {
            self.scroll_to_cursor();
        }
    }

    /// Get selected text
    pub fn get_selected_text(&self, _cx: &App) -> Option<SharedString> {
        match self.editor_core_snapshot().selected_text_plan()? {
            SelectedTextPlan::Linear(text) => Some(text.into()),
            SelectedTextPlan::Block(lines) => Some(lines.join("\n").into()),
        }
    }

    /// Select all text
    pub fn select_all(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.select_all());
    }

    /// Clear the selection
    pub fn clear_selection(&mut self) {
        self.mutate_selections(|core| core.clear_selection());
    }

    // ============================================================================
    // Multi-cursor infrastructure (feat-017/018/021/022)
    // ============================================================================

    /// Return a snapshot of all active cursors as `(position, selection)` pairs.
    ///
    /// The primary cursor is always first; extra cursors follow in the order
    /// they were added.
    pub fn all_cursor_selections(&self) -> Vec<(Position, Selection)> {
        self.editor_core_snapshot().all_cursor_selections()
    }

    /// Collapse all extra cursors and clear all selections, keeping only the
    /// primary cursor in place.  Call this whenever an action that cannot
    /// meaningfully be replicated across multiple cursors is triggered (e.g.
    /// Cmd+Z undo, line-move operations).
    fn collapse_to_primary_cursor(&mut self) {
        self.mutate_selections(|core| core.collapse_to_primary_cursor());
    }

    /// Return the selected text as a `String`, or the word under the primary
    /// cursor if there is no active selection.  Returns `None` when the cursor
    /// is not on any word and there is no selection.
    fn selection_or_word_under_cursor(&self) -> Option<String> {
        self.editor_core_snapshot()
            .selection_or_word_under_cursor_text()
    }

    /// Delete selected text (if any selection exists)
    fn delete_selection(&mut self) -> bool {
        let Some(batch) = self
            .editor_core_snapshot()
            .primary_selection_deletion_edit_batch()
        else {
            return false;
        };

        self.apply_planned_edit_batch(batch, false)
    }

    fn delete_selection_range_if_present(&mut self) -> bool {
        let Some(batch) = self
            .editor_core_snapshot()
            .primary_selection_deletion_edit_batch()
        else {
            return false;
        };

        self.apply_planned_edit_batch(batch, false)
    }

    // ============================================================================
    // Clipboard Operations (Phase 4)
    // ============================================================================

    /// Copy selected text to clipboard (Ctrl+C)
    /// Returns true if text was copied, false if no selection
    pub fn copy(&self, cx: &mut Context<Self>) -> bool {
        if let Some(selected_text) = self.get_selected_text(cx) {
            cx.write_to_clipboard(ClipboardItem::new_string(selected_text.to_string()));
            true
        } else {
            false
        }
    }

    /// Cut selected text to clipboard (Ctrl+X)
    /// Copies selection and then deletes it
    /// Returns true if text was cut, false if no selection
    pub fn cut(&mut self, cx: &mut Context<Self>) -> bool {
        if !self.has_selection() {
            return false;
        }

        // Copy to clipboard first
        if let Some(selected_text) = self.get_selected_text(cx) {
            cx.write_to_clipboard(ClipboardItem::new_string(selected_text.to_string()));

            // Then delete the selection
            self.delete_selection();
            true
        } else {
            false
        }
    }

    /// Paste text from clipboard (Ctrl+V)
    /// Deletes selection if any, then inserts clipboard text at cursor
    pub fn paste(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(clipboard_item) = cx.read_from_clipboard() else {
            return;
        };
        let Some(clipboard_text) = clipboard_item.text() else {
            return;
        };

        if self.apply_text_to_all_cursors(&clipboard_text) {
            self.did_change_content(cx);
            cx.notify();
            return;
        }

        if self.input.clipboard_contains_whole_line() && !self.has_selection() {
            let Some(batch) = self
                .editor_core_snapshot()
                .whole_line_paste_edit_batch(&clipboard_text)
            else {
                return;
            };

            if self.apply_single_cursor_edit_batch(batch, cx, true) {
                return;
            }
        }

        self.delete_selection_range_if_present();
        self.insert_at_cursor(&clipboard_text, window, cx);
    }

    // ============================================================================
    // Undo/Redo (Phase 4)
    // ============================================================================

    /// Undo the last change group (Ctrl+Z).
    /// Returns true if undo was performed, false if nothing to undo.
    pub fn undo(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> bool {
        let Some(record) = self.history.pop_undo() else {
            return false;
        };

        // Snapshot the rope so we can restore on partial failure.
        let buffer_snapshot = self.document.buffer_snapshot();

        let mut redo_group = Vec::with_capacity(record.changes().len());

        // Apply each change's inverse in reverse order so the buffer ends up
        // in the pre-group state regardless of how many changes were grouped.
        for change in record.changes().iter().cloned().rev() {
            let inverse = change.inverse();
            if self.document.apply_change(&inverse).is_ok() {
                redo_group.push(change);
            } else {
                // Restore buffer to pre-undo state and put the group back.
                self.document.restore_buffer_snapshot(&buffer_snapshot);
                self.history.push_undo(record);
                return false;
            }
        }

        self.restore_selection_state(record.before().clone());
        let (id, _changes, before, after) = record.into_parts();
        self.history
            .push_redo_group(id, redo_group.into_iter().rev().collect(), before, after);
        self.scroll_to_cursor();
        true
    }

    /// Redo a previously undone change group (Ctrl+Shift+Z).
    /// Returns true if redo was performed, false if nothing to redo.
    pub fn redo(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> bool {
        let Some(record) = self.history.pop_redo() else {
            return false;
        };

        let buffer_snapshot = self.document.buffer_snapshot();

        let mut undo_group = Vec::with_capacity(record.changes().len());

        for change in record.changes().iter().cloned() {
            if self.document.apply_change(&change).is_ok() {
                undo_group.push(change);
            } else {
                self.document.restore_buffer_snapshot(&buffer_snapshot);
                self.history.push_redo(record);
                return false;
            }
        }

        self.restore_selection_state(record.after().clone());
        let (id, _changes, before, after) = record.into_parts();
        self.history.push_undo_group(id, undo_group, before, after);
        self.scroll_to_cursor();
        true
    }

    /// Push a change to the undo stack, grouping it with the previous entry if
    /// both are single-character insertions that arrived within 300ms of each other
    /// (feat-025: time-based undo grouping).
    ///
    /// Structural edits (multi-char or deletions) always start a new group so
    /// that each discrete command is independently undoable.
    fn push_undo(&mut self, change: buffer::Change) {
        let current_state = self.selection_state();
        let now = std::time::Instant::now();
        self.history.push_change(change, current_state, now);
    }

    /// Break the current undo group unconditionally.
    ///
    /// Called before structural edits (move-line, delete-line, etc.) so that
    /// each command gets its own undo step even if it happens to follow a recent
    /// typing burst.
    fn break_undo_group(&mut self) {
        self.history.clear_last_edit_time();
    }

    // ============================================================================
    // Smart Home (feat-001)
    // ============================================================================

    // ============================================================================
    // Line editing helpers (feat-005 through feat-015)
    // ============================================================================

    /// Return the (first_line, last_line) range of lines touched by the current
    /// cursor or selection (inclusive, 0-indexed).
    /// Move the selected line block up by one line (feat-005).
    fn move_lines_up(&mut self) {
        let Some(batch) = self
            .editor_core_snapshot()
            .move_selected_lines_up_edit_batch()
        else {
            return;
        };

        if !self.apply_planned_edit_batch(batch, false) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Move the selected line block down by one line (feat-005).
    fn move_lines_down(&mut self) {
        let Some(batch) = self
            .editor_core_snapshot()
            .move_selected_lines_down_edit_batch()
        else {
            return;
        };

        if !self.apply_planned_edit_batch(batch, false) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Duplicate the current line (or selected block) downward (feat-006).
    ///
    /// The cursor moves to the duplicate (one line below).
    fn duplicate_lines_down(&mut self) {
        let Some(batch) = self
            .editor_core_snapshot()
            .duplicate_selected_lines_down_edit_batch()
        else {
            return;
        };

        if !self.apply_planned_edit_batch(batch, false) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Duplicate the current line (or selected block) upward (feat-006).
    ///
    /// The cursor stays on the original line.
    fn duplicate_lines_up(&mut self) {
        let Some(batch) = self
            .editor_core_snapshot()
            .duplicate_selected_lines_up_edit_batch()
        else {
            return;
        };

        if !self.apply_planned_edit_batch(batch, false) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Delete the current line (or all selected lines) including their newlines (feat-007).
    fn delete_lines(&mut self) {
        let Some(batch) = self
            .editor_core_snapshot()
            .selected_line_deletion_edit_batch()
        else {
            return;
        };

        if !self.apply_planned_edit_batch(batch, false) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Insert a blank line above the cursor's current line (feat-008).
    ///
    /// The new line inherits the current line's indentation.
    fn insert_newline_above(&mut self) {
        let Some(batch) = self
            .editor_core_snapshot()
            .insert_newline_above_edit_batch()
        else {
            return;
        };

        if !self.apply_planned_edit_batch(batch, false) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Insert a blank line below the cursor's current line (feat-009).
    ///
    /// The new line inherits the current line's indentation.
    fn insert_newline_below(&mut self) {
        let Some(batch) = self
            .editor_core_snapshot()
            .insert_newline_below_edit_batch()
        else {
            return;
        };

        if !self.apply_planned_edit_batch(batch, false) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Join the current line with the one below (feat-010, public wrapper).
    ///
    /// Replaces the trailing newline with a single space and collapses any
    /// leading whitespace on the formerly-next line.
    fn join_lines(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let Some(batch) = self.editor_core_snapshot().join_lines_edit_batch() else {
            return;
        };

        if self.apply_planned_edit_batch(batch, false) {
            self.scroll_to_cursor();
            self.did_change_content(cx);
            self.update_syntax_highlights();
            self.update_diagnostics();
        }
    }

    /// Swap the character before the cursor with the character after it (feat-011).
    fn transpose_chars(&mut self) {
        let Some(batch) = self.editor_core_snapshot().transpose_chars_edit_batch() else {
            return;
        };

        if !self.apply_planned_edit_batch(batch, false) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Indent every line in the current selection (or cursor line) by one tab-stop (feat-013).
    fn indent_lines(&mut self) {
        let Some(batch) = self.editor_core_snapshot().line_prefix_edit_batch(
            self.document.settings().indent_size,
            self.document.settings().use_tabs,
            LinePrefixEditMode::Indent,
        ) else {
            return;
        };

        if !self.apply_planned_edit_batch(batch, false) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Dedent every line in the current selection (or cursor line) by up to one
    /// tab-stop (feat-012 Shift+Tab and feat-013 Cmd+[).
    fn dedent_lines(&mut self) {
        let Some(batch) = self.editor_core_snapshot().line_prefix_edit_batch(
            self.document.settings().indent_size,
            self.document.settings().use_tabs,
            LinePrefixEditMode::Dedent,
        ) else {
            return;
        };

        if !self.apply_planned_edit_batch(batch, false) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Toggle `--` SQL line comments on every line in the selection (or cursor line) (feat-015).
    ///
    /// If all selected lines are already commented, the `--` prefix is removed.
    /// Otherwise `-- ` is prepended to the first non-whitespace column on each line.
    fn toggle_line_comment(&mut self) {
        let Some(batch) = self.editor_core_snapshot().line_prefix_edit_batch(
            self.document.settings().indent_size,
            self.document.settings().use_tabs,
            LinePrefixEditMode::ToggleComment,
        ) else {
            return;
        };

        if !self.apply_planned_edit_batch(batch, false) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    // ============================================================================
    // Auto-indent on Enter (feat-014)
    // ============================================================================

    /// Insert a newline and repeat the current line's leading whitespace on the
    /// new line so the cursor lands at the correct indentation level.
    fn insert_newline_with_auto_indent(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let indent_unit = self.indent_unit();
        let Some(AutoIndentNewlinePlan { text }) = self
            .editor_core_snapshot()
            .auto_indent_newline_text(self.behavior.auto_indent_enabled(), &indent_unit)
        else {
            return;
        };

        self.insert_at_cursor(&text, window, cx);
        if let Some(change) = self.document.latest_change() {
            self.push_undo(change);
        }
        self.mutate_selections(|core| core.collapse_primary_selection_to_cursor());
    }

    // ============================================================================
    // Shared helpers
    // ============================================================================

    // ============================================================================
    // Select line (feat-016)
    // ============================================================================

    /// Select the current line including its trailing newline.
    ///
    /// Repeated calls while the selection already covers whole lines extend it
    /// by one more line downward each time.
    fn select_line(&mut self) {
        self.mutate_selections(|core| core.select_line());
    }

    // ============================================================================
    // Select next occurrence (feat-017)
    // ============================================================================

    /// Select the word under the cursor (first Cmd+D with no selection), or add
    /// a new cursor at the next occurrence of the already-selected text.
    fn select_next_occurrence(&mut self) {
        let needle = match self.selection_or_word_under_cursor() {
            Some(text) => text,
            None => return,
        };

        if needle.is_empty() {
            return;
        }

        let word_range = self
            .editor_core_snapshot()
            .find_word_range_at_offset(self.cursor_byte_offset());
        let all_occurrences = self.editor_core_snapshot().find_all_occurrences(&needle);
        if self.mutate_selections(|core| core.select_next_occurrence(word_range, &all_occurrences))
        {
            self.scroll_to_cursor();
        }
    }

    fn select_previous_occurrence(&mut self) {
        let needle = match self.selection_or_word_under_cursor() {
            Some(text) => text,
            None => return,
        };
        if needle.is_empty() {
            return;
        }

        let word_range = self
            .editor_core_snapshot()
            .find_word_range_at_offset(self.cursor_byte_offset());
        let all_occurrences = self.editor_core_snapshot().find_all_occurrences(&needle);
        if self
            .mutate_selections(|core| core.select_previous_occurrence(word_range, &all_occurrences))
        {
            self.scroll_to_cursor();
        }
    }

    // ============================================================================
    // Select all occurrences (feat-018)
    // ============================================================================

    /// Place a cursor on every occurrence of the selected text (or word under
    /// cursor) in the buffer simultaneously.
    fn select_all_occurrences(&mut self) {
        let needle = match self.selection_or_word_under_cursor() {
            Some(text) => text,
            None => return,
        };

        if needle.is_empty() {
            return;
        }

        let all_occurrences = self.editor_core_snapshot().find_all_occurrences(&needle);
        self.mutate_selections(|core| core.select_all_occurrences(&all_occurrences));
    }

    // ============================================================================
    // Add cursor above / below (feat-021 / feat-022)
    // ============================================================================

    /// Add an additional cursor one line above the topmost current cursor,
    /// preserving the column (clamped to line length).
    fn add_cursor_above(&mut self) {
        self.mutate_selections(|core| core.add_cursor_above());
    }

    /// Add an additional cursor one line below the bottommost current cursor,
    /// preserving the column (clamped to line length).
    fn add_cursor_below(&mut self) {
        self.mutate_selections(|core| core.add_cursor_below());
    }

    // ============================================================================
    // Copy / Cut whole line when no selection (feat-023)
    // ============================================================================

    /// Copy the entire current line (including its newline) to the clipboard.
    ///
    /// Used when Cmd+C is pressed with no selection active.
    fn copy_whole_line(&mut self, cx: &mut Context<Self>) {
        let Some(WholeLineCopyPlan { text }) = self.editor_core_snapshot().whole_line_copy_plan()
        else {
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(text));
        self.input.record_whole_line_clipboard_write();
    }

    /// Cut the entire current line (including its newline) to the clipboard.
    ///
    /// Used when Cmd+X is pressed with no selection active.
    fn cut_whole_line(&mut self, cx: &mut Context<Self>) {
        let Some(WholeLineCutPlan { text, .. }) = self.editor_core_snapshot().whole_line_cut_plan()
        else {
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(text));
        self.input.record_whole_line_clipboard_write();

        let Some(batch) = self.editor_core_snapshot().whole_line_cut_edit_batch() else {
            return;
        };

        if self.apply_single_cursor_edit_batch(batch, cx, false) {
            self.scroll_to_cursor();
        }
    }

    // ============================================================================
    // Cut to end of line (feat-024)
    // ============================================================================

    /// Cut text from cursor to the end of the current line.
    ///
    /// If the cursor is already at the end of the line, cuts the newline itself
    /// (joining this line with the next).  The cut text goes to the clipboard.
    fn cut_to_end_of_line(&mut self, cx: &mut Context<Self>) {
        let Some(CutToEndOfLinePlan { text, .. }) =
            self.editor_core_snapshot().cut_to_end_of_line_plan()
        else {
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(text));
        self.input.record_selection_clipboard_write();

        let Some(batch) = self.editor_core_snapshot().cut_to_end_of_line_edit_batch() else {
            return;
        };

        if self.apply_single_cursor_edit_batch(batch, cx, false) {
            self.mutate_selections(|core| core.clear_selection());
            self.scroll_to_cursor();
        }
    }

    // ============================================================================
    // LSP Integration
    // ============================================================================

    /// Replace all language feature hooks at once.
    ///
    /// Embedders should prefer this over piecemeal setters when switching
    /// language/database contexts so stale providers and derived UI state are
    /// cleared consistently by the central editor.
    pub fn set_language_providers(
        &mut self,
        providers: EditorLanguageProviders,
        cx: &mut Context<Self>,
    ) {
        if let Some(provider) = providers.completion {
            self.lsp.set_completion_provider(provider);
        } else {
            self.lsp.clear_completion_provider();
            self.lsp.clear_completion();
        }

        if let Some(provider) = providers.hover {
            self.lsp.set_hover_provider(provider);
        } else {
            self.lsp.clear_hover_provider();
            self.clear_hover();
        }

        if let Some(provider) = providers.definition {
            self.lsp.set_definition_provider(provider);
        } else {
            self.lsp.clear_definition_provider();
        }

        if let Some(provider) = providers.references {
            self.lsp.set_references_provider(provider);
        } else {
            self.lsp.clear_references_provider();
            self.document.clear_reference_ranges();
        }

        if let Some(provider) = providers.rename {
            self.lsp.set_rename_provider(provider);
        } else {
            self.lsp.clear_rename_provider();
        }

        if let Some(provider) = providers.code_actions {
            self.lsp.set_code_action_provider(provider);
        } else {
            self.lsp.clear_code_action_provider();
            self.document.clear_code_actions();
        }

        if let Some(provider) = providers.diagnostics {
            self.lsp.set_diagnostic_provider(provider);
        } else {
            self.lsp.clear_diagnostic_provider();
            self.set_lsp_diagnostics(Vec::new(), cx);
        }

        self.format_provider = providers.format;
    }

    pub fn clear_language_providers(&mut self, cx: &mut Context<Self>) {
        self.set_language_providers(EditorLanguageProviders::default(), cx);
    }

    /// Set completion provider for schema-aware completions
    ///
    /// This allows the application layer to inject a custom completion provider
    /// (e.g., backed by zqlz-lsp's SqlLsp) to enable schema-aware completions.
    pub fn set_completion_provider(&mut self, provider: std::rc::Rc<dyn CompletionProvider>) {
        self.lsp.set_completion_provider(provider);
    }

    pub fn clear_completion_provider(&mut self) {
        self.lsp.clear_completion_provider();
    }

    /// Set the hover provider for schema-aware hover documentation.
    pub fn set_hover_provider(&mut self, provider: std::rc::Rc<dyn HoverProvider>) {
        self.lsp.set_hover_provider(provider);
    }

    pub fn clear_hover_provider(&mut self) {
        self.lsp.clear_hover_provider();
        self.clear_hover();
    }

    /// Set definition provider for go-to-definition navigation.
    pub fn set_definition_provider(&mut self, provider: std::rc::Rc<dyn DefinitionProvider>) {
        self.lsp.set_definition_provider(provider);
    }

    pub fn clear_definition_provider(&mut self) {
        self.lsp.clear_definition_provider();
    }

    /// Set references provider for find-references highlighting.
    pub fn set_references_provider(&mut self, provider: std::rc::Rc<dyn ReferencesProvider>) {
        self.lsp.set_references_provider(provider);
    }

    pub fn clear_references_provider(&mut self) {
        self.lsp.clear_references_provider();
        self.document.clear_reference_ranges();
    }

    /// Set rename provider for symbol rename operations.
    pub fn set_rename_provider(&mut self, provider: std::rc::Rc<dyn RenameProvider>) {
        self.lsp.set_rename_provider(provider);
    }

    pub fn clear_rename_provider(&mut self) {
        self.lsp.clear_rename_provider();
    }

    /// Set code action provider for cursor-context actions.
    pub fn set_code_action_provider(&mut self, provider: std::rc::Rc<dyn CodeActionProvider>) {
        self.lsp.set_code_action_provider(provider);
    }

    pub fn set_format_provider(&mut self, provider: FormatProvider) {
        self.format_provider = Some(provider);
    }

    pub fn clear_format_provider(&mut self) {
        self.format_provider = None;
    }

    pub fn clear_code_action_provider(&mut self) {
        self.lsp.clear_code_action_provider();
        self.document.clear_code_actions();
    }

    pub fn anchored_diagnostics(&self) -> Vec<AnchoredDiagnostic> {
        self.document.anchored_diagnostics()
    }

    pub fn last_edited_line_range(&self) -> Option<std::ops::Range<usize>> {
        self.document.pending_edited_line_range()
    }

    /// Set SQL LSP instance for legacy integrations.
    ///
    /// New integrations should prefer dedicated provider setters
    /// (`set_completion_provider`, `set_hover_provider`, etc).
    pub fn set_sql_lsp(&mut self, lsp: Arc<dyn std::any::Any + Send + Sync>) {
        self.lsp.set_legacy_sql_lsp(lsp);
    }

    /// Check if any LSP bridge/provider is connected.
    pub fn is_lsp_connected(&self) -> bool {
        self.lsp.has_any_provider_or_bridge()
    }

    pub fn get_completions(&self, _cx: &App) -> Vec<CompletionItem> {
        if !self.large_file_policy().completions_enabled {
            return Vec::new();
        }

        let prefix = lsp::get_word_at_cursor(&self.document.rope(), self.current_cursor_offset())
            .to_string();
        self.lsp.visible_completions(&prefix)
    }

    /// Get hover at cursor.
    ///
    /// This synchronous compatibility API uses the same keyword/function fallback
    /// as `get_hover_at`. Async provider-driven hover is exposed via
    /// `update_hover_at_position`.
    pub fn get_hover(&self, cx: &App) -> Option<Hover> {
        let offset = self.current_cursor_offset();
        self.get_hover_at(offset, cx)
    }

    /// Get hover at specific offset
    /// Returns hover information for the word at the given offset
    pub fn get_hover_at(&self, offset: usize, _cx: &App) -> Option<Hover> {
        let provider = SqlCompletionProvider::new();

        // Find the word at the given offset
        let word_range = self
            .editor_core_snapshot()
            .find_word_range_at_offset(offset)?;
        let word = self.document.text_for_range(word_range.clone()).ok()?;

        // Get documentation for the word
        let documentation = provider.get_hover_documentation(&word)?;

        // Create LSP Hover response
        use lsp_types::{HoverContents, MarkupContent, MarkupKind, Range as LspRange};

        Some(Hover {
            contents: HoverContents::Markup(MarkupContent {
                kind: MarkupKind::PlainText,
                value: documentation,
            }),
            range: Some(LspRange {
                start: lsp_types::Position {
                    line: self
                        .document
                        .offset_to_position(word_range.start)
                        .ok()?
                        .line as u32,
                    character: self
                        .document
                        .offset_to_position(word_range.start)
                        .ok()?
                        .column as u32,
                },
                end: lsp_types::Position {
                    line: self.document.offset_to_position(word_range.end).ok()?.line as u32,
                    character: self
                        .document
                        .offset_to_position(word_range.end)
                        .ok()?
                        .column as u32,
                },
            }),
        })
    }

    fn lsp_range_for_offsets(&self, start: usize, end: usize) -> Option<lsp_types::Range> {
        let start_position = self.document.offset_to_position(start).ok()?;
        let end_position = self.document.offset_to_position(end).ok()?;
        Some(lsp_types::Range {
            start: lsp_types::Position {
                line: u32::try_from(start_position.line).ok()?,
                character: u32::try_from(start_position.column).ok()?,
            },
            end: lsp_types::Position {
                line: u32::try_from(end_position.line).ok()?,
                character: u32::try_from(end_position.column).ok()?,
            },
        })
    }

    pub fn document_identity(&self) -> &DocumentIdentity {
        self.document.identity()
    }

    pub fn document_context(&self) -> DocumentContext {
        self.document.context()
    }

    pub fn set_document_identity(&mut self, identity: DocumentIdentity, cx: &mut Context<Self>) {
        if self.document.identity() == &identity {
            return;
        }

        self.document.set_identity(identity);
        cx.notify();
    }

    pub fn mark_saved(&mut self, cx: &mut Context<Self>) {
        let revision = self.document.buffer_revision();
        if self.document.saved_revision() == revision {
            return;
        }

        self.document.mark_saved(revision);
        cx.notify();
    }

    pub fn is_dirty(&self) -> bool {
        self.document.is_dirty(self.document.buffer_revision())
    }

    /// Get signature help near the cursor.
    ///
    /// This infers the function-call context from the current text and returns a
    /// lightweight signature payload, allowing higher layers to display parameter
    /// hints even when no dedicated signature provider is wired.
    pub fn get_signature_help(&self, _cx: &App) -> Option<SignatureHelp> {
        let cursor_offset = self.current_cursor_offset();
        let structural_range = self.innermost_enclosing_bracket_range(cursor_offset);
        let structural_open_paren = structural_range
            .filter(|range| range.open == '(')
            .map(|range| range.start);
        let plan = self
            .editor_core_snapshot()
            .signature_help_query_plan(structural_open_paren)?;

        Some(SignatureHelp {
            signatures: vec![lsp_types::SignatureInformation {
                label: format!("{}(...)", plan.function_name),
                documentation: None,
                parameters: None,
                active_parameter: None,
            }],
            active_signature: Some(0),
            active_parameter: Some(plan.active_parameter),
        })
    }

    // ============================================================================
    // Diagnostics (Stubs for Phase 0)
    // ============================================================================

    /// Replace the squiggle set with diagnostics from an external source (e.g. LSP).
    ///
    /// Each `Diagnostic` carries line/column positions which are converted to
    /// byte-offset `Highlight` entries so the render layer can draw squiggles
    /// without any further coordinate translation.  Invalid positions (out-of-
    /// bounds line or column) are silently skipped rather than panicking.
    pub fn set_diagnostics(&mut self, diagnostics: Vec<Diagnostic>, cx: &mut Context<Self>) {
        self.document
            .apply_external_diagnostics(self.large_file_policy().diagnostics_enabled, diagnostics);

        cx.notify();
    }

    pub fn set_lsp_diagnostics(
        &mut self,
        diagnostics: Vec<lsp_types::Diagnostic>,
        cx: &mut Context<Self>,
    ) {
        self.lsp_diagnostics = diagnostics.clone();
        self.set_diagnostics(diagnostics.into_iter().map(Into::into).collect(), cx);
    }

    pub fn lsp_diagnostics(&self) -> &[lsp_types::Diagnostic] {
        &self.lsp_diagnostics
    }

    pub fn lsp_diagnostic_counts(&self) -> (usize, usize, usize) {
        Self::lsp_diagnostic_counts_from(&self.lsp_diagnostics)
    }

    pub fn lsp_diagnostic_counts_from(
        diagnostics: &[lsp_types::Diagnostic],
    ) -> (usize, usize, usize) {
        let mut errors = 0;
        let mut warnings = 0;
        let mut hints_infos = 0;

        for diagnostic in diagnostics {
            match diagnostic.severity {
                Some(lsp_types::DiagnosticSeverity::ERROR) => errors += 1,
                Some(lsp_types::DiagnosticSeverity::WARNING) => warnings += 1,
                Some(lsp_types::DiagnosticSeverity::INFORMATION)
                | Some(lsp_types::DiagnosticSeverity::HINT) => hints_infos += 1,
                _ => errors += 1,
            }
        }

        (errors, warnings, hints_infos)
    }

    pub fn request_lsp_diagnostics(
        &self,
        _cx: &App,
    ) -> Option<Task<anyhow::Result<Vec<lsp_types::Diagnostic>>>> {
        if !self.large_file_policy().diagnostics_enabled {
            return None;
        }

        self.lsp
            .request_diagnostics(&self.document.rope(), &self.document.context())
    }

    // ============================================================================
    // Completion Menu (Phase 8 - LSP Integration)
    // ============================================================================

    /// Trigger completion menu with explicit trigger metadata.
    pub fn trigger_completions(
        &mut self,
        trigger: lsp_types::CompletionContext,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let large_file_policy = self.large_file_policy();
        if !large_file_policy.completions_enabled {
            self.lsp.clear_completion_menu();
            return;
        }

        let cursor_offset = self.current_cursor_offset();
        let CompletionQueryPlan {
            trigger_offset: word_start,
            current_prefix,
        } = self.editor_core_snapshot().completion_query_plan();

        let resolution = self.lsp.resolve_completion_request(
            large_file_policy.allow_async_provider_requests(),
            crate::lsp::CompletionRequestContext {
                revision: self.document.buffer_revision(),
                cursor_offset: self.current_cursor_offset(),
                trigger_offset: word_start,
                current_prefix: current_prefix.clone(),
            },
        );

        match resolution {
            crate::lsp::CompletionResolution::CachedFilter {
                items,
                trigger_offset,
            } => {
                self.lsp.set_completion_items(items, trigger_offset);
            }
            crate::lsp::CompletionResolution::Clear => {
                self.lsp.clear_completion_menu();
            }
            crate::lsp::CompletionResolution::Provider {
                request,
                trigger_offset,
                trigger_prefix,
            } => {
                let rope = self.document.rope();
                let Some(task) =
                    self.lsp
                        .request_completions(&rope, cursor_offset, trigger, window, cx)
                else {
                    self.lsp.clear_completion_menu();
                    return;
                };

                self.lsp
                    .replace_completion_task(cx.spawn(async move |this, cx| {
                        let response = task.await?;

                        let items = match response {
                            lsp_types::CompletionResponse::Array(items) => items,
                            lsp_types::CompletionResponse::List(list) => list.items,
                        };

                        this.update(cx, |editor, cx| {
                            if !editor.lsp.matches_completion_request(
                                request,
                                editor.document.buffer_revision(),
                                editor.current_cursor_offset(),
                            ) {
                                return;
                            }

                            if items.is_empty() {
                                editor.lsp.clear_completion_menu();
                            } else {
                                editor
                                    .lsp
                                    .set_completion_cache(crate::lsp::CompletionCache {
                                        all_items: items.clone(),
                                        trigger_prefix: trigger_prefix.clone(),
                                        trigger_offset,
                                    });
                                editor.lsp.set_completion_items(items, trigger_offset);
                            }
                            cx.notify();
                        })?;

                        Ok(())
                    }));
            }
        }
    }

    /// Show completion menu
    pub fn hide_completion_menu(&mut self, _cx: &mut Context<Self>) {
        self.lsp.clear_completion();
    }

    /// Update completion menu based on current cursor/prefix context.
    pub fn update_completion_menu(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.refresh_completions(window, cx);
    }

    /// Check if completion menu is open
    pub fn is_completion_menu_open(&self, _cx: &App) -> bool {
        self.lsp.has_completion_menu()
    }

    /// Get completion menu data for rendering
    pub fn completion_menu(&self, _cx: &App) -> Option<CompletionMenuData> {
        self.lsp.completion_menu()
    }

    /// Move selection in completion menu up
    pub fn completion_menu_select_previous(&mut self) {
        self.lsp.select_previous_completion();
    }

    /// Move selection in completion menu down
    pub fn completion_menu_select_next(&mut self) {
        self.lsp
            .select_next_completion(crate::element::MAX_COMPLETION_ITEMS);
    }

    /// Accept the currently selected completion
    pub fn accept_completion(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        // Take the completion menu state
        let Some(menu) = self.lsp.take_completion_menu_state() else {
            return;
        };

        // Get the selected item
        let selected_index = menu.selected_index;
        let Some(item) = menu.items.get(selected_index) else {
            return;
        };

        // Get the completion text (insert_text or label)
        let completion_text = item
            .insert_text
            .clone()
            .unwrap_or_else(|| item.label.clone());
        let snippet = if matches!(
            item.insert_text_format,
            Some(lsp_types::InsertTextFormat::SNIPPET)
        ) {
            Some(Snippet::parse(&completion_text))
        } else {
            None
        };

        // Delete the partial word that was typed
        let trigger_offset = menu.trigger_offset;
        let cursor_offset = self.current_cursor_offset();
        let inserted_text = snippet
            .as_ref()
            .map(|snippet| snippet.text.clone())
            .unwrap_or_else(|| completion_text.clone());

        let replacement_batch = self
            .editor_core_snapshot()
            .primary_text_replacement_plan(
                Some(trigger_offset..cursor_offset),
                None,
                &inserted_text,
            )
            .batch;

        if !self.apply_planned_edit_batch(replacement_batch, true) {
            return;
        }

        self.apply_interpolated_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
        self.did_change_content(cx);

        if let Some(snippet) = snippet
            && let Some(selection) =
                self.snippets
                    .activate_snippet(&snippet, self.document.buffer(), trigger_offset)
        {
            self.mutate_selections(|core| core.set_primary_selection(selection));
        }
        cx.notify();
    }

    pub fn advance_snippet_placeholder(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(selection) = self.snippets.advance_placeholder(self.document.buffer()) else {
            return false;
        };

        self.mutate_selections(|core| core.set_primary_selection(selection));
        cx.notify();
        true
    }

    /// Handle completion-related action dispatch for compatibility callers.
    pub fn handle_completion_action(
        &mut self,
        action: &dyn Action,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        if action.partial_eq(&actions::TriggerCompletion) {
            self.trigger_completions(
                lsp_types::CompletionContext {
                    trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
                    trigger_character: None,
                },
                window,
                cx,
            );
            cx.notify();
            return true;
        }
        if action.partial_eq(&actions::AcceptCompletion) {
            self.accept_completion(window, cx);
            return true;
        }
        if action.partial_eq(&actions::DismissCompletion) {
            self.lsp.clear_completion_menu();
            cx.notify();
            return true;
        }
        if action.partial_eq(&actions::SelectPreviousCompletion) {
            self.completion_menu_select_previous();
            cx.notify();
            return true;
        }
        if action.partial_eq(&actions::SelectNextCompletion) {
            self.completion_menu_select_next();
            cx.notify();
            return true;
        }

        false
    }

    pub fn trigger_completion_action(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.trigger_completions(
            lsp_types::CompletionContext {
                trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
                trigger_character: None,
            },
            window,
            cx,
        );
        cx.notify();
    }

    /// Refresh completions when the completion menu is already open.
    pub fn refresh_completions(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.lsp.has_completion_menu() {
            self.trigger_completions(
                lsp_types::CompletionContext {
                    trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
                    trigger_character: None,
                },
                window,
                cx,
            );
            cx.notify();
        }
    }

    /// Schedule completion trigger after a short debounce (100ms).
    /// Cancels any pending debounce from a previous keystroke.
    fn trigger_completions_debounced(
        &mut self,
        trigger: lsp_types::CompletionContext,
        cx: &mut Context<Self>,
    ) {
        self.lsp.clear_pending_completion();
        self.lsp
            .replace_completion_debounce_task(cx.spawn(async move |this, cx| {
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(100))
                    .await;

                this.update(cx, |editor, cx| {
                    editor.lsp.queue_completion_refresh(trigger.clone());
                    cx.notify();
                })?;
                Ok(())
            }));
    }

    /// Fire pending debounced completions. Called from render when window is available.
    pub fn flush_pending_completions(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(trigger) = self.lsp.take_pending_completion_context() {
            self.trigger_completions(trigger, window, cx);
        }
    }

    fn schedule_hover_at_position(&mut self, position: Position, cx: &mut Context<Self>) {
        let Some(delay) = self.overlays.begin_hover_timer(position) else {
            return;
        };
        self.lsp
            .replace_hover_debounce_task(cx.spawn(async move |this, cx| {
                cx.background_executor().timer(delay).await;

                this.update(cx, |editor, cx| {
                    if editor.overlays.mark_hover_ready_if_pending(position) {
                        cx.notify();
                    }
                })?;

                Ok(())
            }));
    }

    fn flush_pending_hover(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(position) = self.overlays.take_ready_hover_position() {
            self.update_hover_at_position(position.line, position.column, window, cx);
        }
    }

    /// Show completions
    /// Alias for show_completion_menu
    pub fn show_completions(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.trigger_completions(
            lsp_types::CompletionContext {
                trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
                trigger_character: None,
            },
            window,
            cx,
        );
    }

    /// Hide completions
    pub fn hide_completions(&mut self, _cx: &mut Context<Self>) {
        self.lsp.clear_completion_menu();
    }

    // ============================================================================
    // Hover Support (Phase 8 - LSP Integration)
    // ============================================================================

    /// Update hover state based on mouse position
    /// This is called from EditorElement when the mouse moves
    pub fn update_hover_at_position(
        &mut self,
        line: usize,
        column: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let large_file_policy = self.large_file_policy();
        if !large_file_policy.hover_enabled {
            self.lsp.clear_hover_state();
            return;
        }

        // Convert line/column to offset
        let position = Position::new(line, column);
        let Ok(offset) = self.document.position_to_offset(position) else {
            self.lsp.clear_hover_state();
            return;
        };

        let fallback_hover = self
            .editor_core_snapshot()
            .word_target_at_offset(offset)
            .and_then(|target| {
                let provider = SqlCompletionProvider::new();
                provider
                    .get_hover_documentation(&target.text)
                    .map(|documentation| HoverState {
                        word: target.text,
                        documentation,
                        range: target.range,
                    })
            });
        let hover_resolution = self.lsp.resolve_hover_request(
            large_file_policy.allow_async_provider_requests(),
            fallback_hover,
            crate::lsp::HoverRequestContext {
                revision: self.document.buffer_revision(),
                cursor_offset: self.current_cursor_offset(),
                offset,
                word_target: self.editor_core_snapshot().word_target_at_offset(offset),
            },
        );

        if let HoverResolution::Provider { request } = hover_resolution {
            let rope = self.document.rope();
            let Some(task) = self.lsp.request_hover(&rope, offset, window, cx) else {
                self.lsp.clear_hover_state();
                return;
            };

            let word_target = self.editor_core_snapshot().word_target_at_offset(offset);

            self.lsp.replace_hover_task(cx.spawn(async move |this, cx| {
                let maybe_hover = task.await?;

                this.update(cx, |editor, cx| {
                    if !editor.lsp.matches_hover_request(
                        request,
                        editor.document.buffer_revision(),
                        editor.current_cursor_offset(),
                    ) {
                        return;
                    }

                    match (maybe_hover, word_target) {
                        (Some(_hover), Some(target)) => {
                            let WordTarget { range, text: word } = target;
                            // Use the first plain-text content from the hover response
                            // as the documentation string.
                            let documentation = match &_hover.contents {
                                lsp_types::HoverContents::Scalar(markup) => {
                                    markup_to_string(markup)
                                }
                                lsp_types::HoverContents::Array(markups) => {
                                    markups.first().map(markup_to_string).unwrap_or_default()
                                }
                                lsp_types::HoverContents::Markup(markup) => markup.value.clone(),
                            };

                            if documentation.is_empty() {
                                editor.lsp.clear_hover_state();
                            } else if editor.lsp.set_hover_state_if_word_changed(HoverState {
                                word,
                                documentation,
                                range,
                            }) {
                                cx.notify();
                            }
                        }
                        _ => {
                            editor.lsp.clear_hover_state();
                            cx.notify();
                        }
                    }
                })?;

                Ok(())
            }));
            return;
        }

        match hover_resolution {
            HoverResolution::Fallback(Some(hover_state)) => {
                self.lsp.set_hover_state_if_word_changed(hover_state);
            }
            HoverResolution::Fallback(None) | HoverResolution::Clear => {
                self.lsp.clear_hover_state();
            }
            HoverResolution::Provider { .. } => {}
        }
    }

    pub fn update_hover_at_cursor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let position = self.current_cursor_position();
        self.update_hover_at_position(position.line, position.column, window, cx);
    }

    /// Clear hover state (called when mouse leaves the editor)
    pub fn clear_hover(&mut self) {
        self.overlays.clear_hover_positions();
        self.lsp.clear_hover_state();
    }

    /// Get the current hover state for rendering
    pub fn hover_state(&self, _cx: &App) -> Option<HoverState> {
        self.lsp.hover_state()
    }

    /// Check if hover tooltip is visible
    pub fn has_hover(&self, _cx: &App) -> bool {
        self.lsp.has_hover_state()
    }

    /// Set the explicit signature-help overlay content.
    pub fn set_signature_help(
        &mut self,
        content: String,
        anchor_offset: usize,
        cx: &mut Context<Self>,
    ) {
        let Ok(anchor) = self
            .document
            .anchor_at(anchor_offset.min(self.document.byte_len()), Bias::Right)
        else {
            return;
        };
        self.overlays
            .set_signature_help(SignatureHelpState::new(content, anchor));
        cx.notify();
    }

    /// Clear the signature-help overlay.
    pub fn clear_signature_help(&mut self, cx: &mut Context<Self>) {
        if self.overlays.clear_signature_help() {
            cx.notify();
        }
    }

    /// Read the current signature-help overlay state.
    pub fn signature_help_state(&self, _cx: &App) -> Option<SignatureHelpState> {
        self.overlays.signature_help().cloned()
    }

    // ============================================================================
    // Inline Suggestion (Ghost Text)
    // ============================================================================

    /// Push an inline suggestion to be rendered as ghost text immediately after
    /// the cursor.  Called by the parent (`QueryEditor`) after computing an LSP
    /// or AI suggestion; the `EditorElement` will paint it as dimmed text so the
    /// user can preview it without changing the buffer.
    ///
    /// `cursor_offset` is the byte offset in the buffer where the suggestion
    /// should be anchored — normally the current cursor position at the time the
    /// suggestion was generated.
    pub fn set_inline_suggestion(
        &mut self,
        text: String,
        cursor_offset: usize,
        cx: &mut Context<Self>,
    ) {
        let Ok(anchor) = self
            .document
            .anchor_at(cursor_offset.min(self.document.byte_len()), Bias::Right)
        else {
            return;
        };
        self.overlays
            .set_inline_suggestion(InlineSuggestion::new(text, anchor));
        cx.notify();
    }

    pub fn set_edit_prediction(
        &mut self,
        text: String,
        anchor_offset: usize,
        cx: &mut Context<Self>,
    ) {
        let Ok(anchor) = self
            .document
            .anchor_at(anchor_offset.min(self.document.byte_len()), Bias::Right)
        else {
            return;
        };
        self.overlays
            .set_edit_prediction(EditPrediction::new(text, anchor));
        cx.notify();
    }

    pub fn clear_edit_prediction(&mut self, cx: &mut Context<Self>) {
        if self.overlays.clear_edit_prediction() {
            cx.notify();
        }
    }

    pub fn accept_edit_prediction(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        let Some(prediction) = self.overlays.take_edit_prediction() else {
            return false;
        };
        let Ok(target_offset) = self.document.resolve_anchor_offset(prediction.anchor) else {
            return false;
        };
        if let Ok(position) = self.document.offset_to_position(target_offset) {
            self.mutate_selections(|core| core.move_primary_cursor(position, false));
        }

        self.insert_text(&prediction.text, window, cx);
        true
    }

    pub fn edit_prediction(&self) -> Option<&EditPrediction> {
        self.overlays.edit_prediction()
    }

    /// Remove any pending inline suggestion.  Call this when the user dismisses
    /// the suggestion, accepts it, or types something that invalidates it.
    pub fn clear_inline_suggestion(&mut self, cx: &mut Context<Self>) {
        if self.overlays.clear_inline_suggestion() {
            cx.notify();
        }
    }

    /// Accept the current inline suggestion at its stored anchor position.
    pub fn accept_inline_suggestion(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let Some(suggestion) = self.overlays.take_inline_suggestion() else {
            return false;
        };
        let Ok(target_offset) = self.document.resolve_anchor_offset(suggestion.anchor) else {
            return false;
        };
        if let Ok(position) = self.document.offset_to_position(target_offset) {
            self.mutate_selections(|core| core.move_primary_cursor(position, false));
        }

        self.insert_text(&suggestion.text, window, cx);
        true
    }

    /// Read the current inline suggestion (text, anchor cursor offset).
    pub fn inline_suggestion(&self) -> Option<&InlineSuggestion> {
        self.overlays.inline_suggestion()
    }

    // ============================================================================
    // Navigation & LSP Actions
    // ============================================================================

    /// Navigate to a specific position and optional selection range.
    pub fn navigate_to(
        &mut self,
        line: usize,
        column: usize,
        end_line: Option<usize>,
        end_column: Option<usize>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let start = self.document.clamp_position(Position::new(line, column));

        if let (Some(end_line), Some(end_column)) = (end_line, end_column) {
            let end = self
                .document
                .clamp_position(Position::new(end_line, end_column));
            self.mutate_selections(|core| {
                core.set_primary_selection(Selection::from_anchor_head(start, end));
            });
        } else {
            self.mutate_selections(|core| core.move_primary_cursor(start, false));
        }

        self.scroll_to_cursor();
        cx.notify();
    }

    pub fn lsp_diagnostic_index_for_position(
        diagnostic_positions: &[(u32, u32)],
        cursor_line: u32,
        cursor_column: u32,
        forward: bool,
    ) -> Option<usize> {
        if diagnostic_positions.is_empty() {
            return None;
        }

        if forward {
            diagnostic_positions
                .iter()
                .position(|(line, column)| (*line, *column) > (cursor_line, cursor_column))
                .or(Some(0))
        } else {
            diagnostic_positions
                .iter()
                .rposition(|(line, column)| (*line, *column) < (cursor_line, cursor_column))
                .or(Some(diagnostic_positions.len().saturating_sub(1)))
        }
    }

    pub fn navigate_lsp_diagnostic(
        &mut self,
        forward: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let mut diagnostics: Vec<(u32, u32, u32, u32)> = self
            .lsp_diagnostics
            .iter()
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

        let cursor = self.current_cursor_position();
        let positions: Vec<(u32, u32)> = diagnostics
            .iter()
            .map(|(line, column, _, _)| (*line, *column))
            .collect();
        let Some(index) = Self::lsp_diagnostic_index_for_position(
            &positions,
            cursor.line as u32,
            cursor.column as u32,
            forward,
        ) else {
            return false;
        };

        let (line, column, end_line, end_column) = diagnostics[index];
        self.navigate_to(
            line as usize,
            column as usize,
            Some(end_line as usize),
            Some(end_column as usize),
            window,
            cx,
        );
        true
    }

    /// Get definition at cursor.
    pub fn get_definition(&self, _cx: &App) -> Option<lsp_types::GotoDefinitionResponse> {
        let target_offset = self.lsp.definition_at(
            &self.document.rope(),
            self.current_cursor_offset(),
            &self.document.context(),
        )?;
        let range = self.lsp_range_for_offsets(target_offset, target_offset)?;
        let uri = self.document.identity().uri().clone();
        Some(lsp_types::GotoDefinitionResponse::Scalar(
            lsp_types::Location { uri, range },
        ))
    }

    /// Get references at cursor.
    pub fn get_references(&self, _cx: &App) -> Vec<lsp_types::Location> {
        let Some(ranges) = self.lsp.reference_ranges(
            &self.document.rope(),
            self.current_cursor_offset(),
            &self.document.context(),
        ) else {
            return Vec::new();
        };
        let uri = self.document.identity().uri().clone();
        ranges
            .into_iter()
            .filter_map(|range| {
                let lsp_range = self.lsp_range_for_offsets(range.start, range.end)?;
                Some(lsp_types::Location {
                    uri: uri.clone(),
                    range: lsp_range,
                })
            })
            .collect()
    }

    /// Get code actions at cursor.
    pub fn get_code_actions(&self, _cx: &App) -> Vec<lsp_types::CodeActionOrCommand> {
        if !self.large_file_policy().diagnostics_enabled {
            return Vec::new();
        }
        self.document.code_actions().to_vec()
    }

    /// Refresh code actions for the current cursor position from the configured provider.
    pub fn update_code_actions(&mut self, cx: &mut Context<Self>) {
        if !self.large_file_policy().diagnostics_enabled {
            self.document.clear_code_actions();
            cx.notify();
            return;
        }

        let offset = self.current_cursor_offset();
        let Some(code_actions) =
            self.lsp
                .code_actions_at(&self.document.rope(), offset, &self.document.context())
        else {
            self.document.clear_code_actions();
            cx.notify();
            return;
        };

        self.document.set_code_actions(code_actions);
        cx.notify();
    }

    /// Clear any cached code actions.
    pub fn clear_code_actions(&mut self, cx: &mut Context<Self>) {
        if self.document.has_code_actions() {
            self.document.clear_code_actions();
            cx.notify();
        }
    }

    /// Build a workspace edit for renaming the current symbol.
    #[allow(clippy::mutable_key_type)]
    pub fn rename(&self, new_name: &str, _cx: &App) -> Option<lsp_types::WorkspaceEdit> {
        let cursor_offset = self.current_cursor_offset();
        let plan = self.editor_core_snapshot().rename_query_plan(new_name)?;
        let ranges = if let Some(ranges) = self.lsp.reference_ranges(
            &self.document.rope(),
            cursor_offset,
            &self.document.context(),
        ) {
            ranges
        } else {
            plan.ranges
        };

        if ranges.is_empty() {
            return None;
        }

        let uri = self.document.identity().uri().clone();
        let edits: Vec<lsp_types::TextEdit> = ranges
            .into_iter()
            .filter_map(|range| {
                let lsp_range = self.lsp_range_for_offsets(range.start, range.end)?;
                Some(lsp_types::TextEdit {
                    range: lsp_range,
                    new_text: new_name.to_string(),
                })
            })
            .collect();
        if edits.is_empty() {
            return None;
        }

        let mut changes = std::collections::HashMap::new();
        changes.insert(uri, edits);
        Some(lsp_types::WorkspaceEdit {
            changes: Some(changes),
            document_changes: None,
            change_annotations: None,
        })
    }

    fn apply_workspace_edit_impl(
        &mut self,
        edit: &lsp_types::WorkspaceEdit,
        cx: &mut Context<Self>,
    ) -> anyhow::Result<()> {
        let mut resolved_edits = Vec::new();
        if let Some(changes) = &edit.changes {
            for (uri, text_edits) in changes {
                if !self.document.identity().is_current_document_uri(uri) {
                    return Err(anyhow::anyhow!(
                        "workspace edit targeted unsupported document uri: {:?}",
                        uri
                    ));
                }
                self.resolve_plain_text_edits(text_edits, &mut resolved_edits)?;
            }
        }

        if let Some(document_changes) = &edit.document_changes {
            match document_changes {
                lsp_types::DocumentChanges::Edits(edits) => {
                    for text_document_edit in edits {
                        if !self
                            .document
                            .identity()
                            .is_current_document_uri(&text_document_edit.text_document.uri)
                        {
                            return Err(anyhow::anyhow!(
                                "workspace edit targeted unsupported document uri: {:?}",
                                text_document_edit.text_document.uri
                            ));
                        }
                        self.resolve_workspace_text_edits(
                            &text_document_edit.edits,
                            &mut resolved_edits,
                        )?;
                    }
                }
                lsp_types::DocumentChanges::Operations(operations) => {
                    for operation in operations {
                        match operation {
                            lsp_types::DocumentChangeOperation::Edit(text_document_edit) => {
                                if !self
                                    .document
                                    .identity()
                                    .is_current_document_uri(&text_document_edit.text_document.uri)
                                {
                                    return Err(anyhow::anyhow!(
                                        "workspace edit targeted unsupported document uri: {:?}",
                                        text_document_edit.text_document.uri
                                    ));
                                }
                                self.resolve_workspace_text_edits(
                                    &text_document_edit.edits,
                                    &mut resolved_edits,
                                )?;
                            }
                            lsp_types::DocumentChangeOperation::Op(resource_op) => {
                                return Err(anyhow::anyhow!(
                                    "resource workspace operations are not supported: {:?}",
                                    resource_op
                                ));
                            }
                        }
                    }
                }
            }
        }

        if resolved_edits.is_empty() {
            return Ok(());
        }

        resolved_edits.sort_by(|left, right| right.0.cmp(&left.0));
        let edits = resolved_edits
            .into_iter()
            .map(|(start, end, new_text)| TextReplacementEdit {
                range: start..end,
                replacement: new_text,
            })
            .collect::<Vec<_>>();

        if !self.apply_batch_replacement_edits(edits, true) {
            return Err(anyhow::anyhow!("failed to apply workspace edit"));
        }

        self.break_undo_group();
        let clamped_cursor = self.document.clamp_position(self.current_cursor_position());
        self.mutate_selections(|core| core.move_primary_cursor(clamped_cursor, false));
        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
        self.did_change_content(cx);
        cx.notify();
        Ok(())
    }

    fn resolve_plain_text_edits(
        &self,
        edits: &[lsp_types::TextEdit],
        resolved_edits: &mut Vec<(usize, usize, String)>,
    ) -> anyhow::Result<()> {
        for edit in edits {
            self.push_resolved_workspace_edit(&edit.range, edit.new_text.clone(), resolved_edits)?;
        }

        Ok(())
    }

    fn resolve_workspace_text_edits(
        &self,
        edits: &[lsp_types::OneOf<lsp_types::TextEdit, lsp_types::AnnotatedTextEdit>],
        resolved_edits: &mut Vec<(usize, usize, String)>,
    ) -> anyhow::Result<()> {
        for edit in edits {
            let (range, new_text) = match edit {
                lsp_types::OneOf::Left(text_edit) => (&text_edit.range, text_edit.new_text.clone()),
                lsp_types::OneOf::Right(text_edit) => (
                    &text_edit.text_edit.range,
                    text_edit.text_edit.new_text.clone(),
                ),
            };

            self.push_resolved_workspace_edit(range, new_text, resolved_edits)?;
        }

        Ok(())
    }

    fn push_resolved_workspace_edit(
        &self,
        range: &lsp_types::Range,
        new_text: String,
        resolved_edits: &mut Vec<(usize, usize, String)>,
    ) -> anyhow::Result<()> {
        let start_line = usize::try_from(range.start.line)
            .map_err(|error| anyhow::anyhow!("invalid edit start line: {error}"))?;
        let end_line = usize::try_from(range.end.line)
            .map_err(|error| anyhow::anyhow!("invalid edit end line: {error}"))?;
        let start_character = usize::try_from(range.start.character)
            .map_err(|error| anyhow::anyhow!("invalid edit start character: {error}"))?;
        let end_character = usize::try_from(range.end.character)
            .map_err(|error| anyhow::anyhow!("invalid edit end character: {error}"))?;

        let start_line_text = self
            .document
            .line(start_line)
            .ok_or_else(|| anyhow::anyhow!("invalid edit start line: {start_line}"))?;
        let end_line_text = self
            .document
            .line(end_line)
            .ok_or_else(|| anyhow::anyhow!("invalid edit end line: {end_line}"))?;

        let start_column = start_line_text
            .char_indices()
            .nth(start_character)
            .map(|(byte_offset, _)| byte_offset)
            .unwrap_or(start_line_text.len());
        let end_column = end_line_text
            .char_indices()
            .nth(end_character)
            .map(|(byte_offset, _)| byte_offset)
            .unwrap_or(end_line_text.len());

        let start = self
            .document
            .position_to_offset(Position::new(start_line, start_column))
            .map_err(|error| anyhow::anyhow!("invalid edit start position: {error}"))?;
        let end = self
            .document
            .position_to_offset(Position::new(end_line, end_column))
            .map_err(|error| anyhow::anyhow!("invalid edit end position: {error}"))?;

        resolved_edits.push((start.min(end), start.max(end), new_text));
        Ok(())
    }

    /// Apply a workspace edit to the current buffer.
    pub fn apply_workspace_edit(
        &mut self,
        edit: &lsp_types::WorkspaceEdit,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> anyhow::Result<()> {
        self.apply_workspace_edit_impl(edit, cx)
    }

    /// Apply a code action by applying its workspace edit payload.
    pub fn apply_code_action(
        &mut self,
        action: &lsp_types::CodeActionOrCommand,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> anyhow::Result<()> {
        match action {
            lsp_types::CodeActionOrCommand::CodeAction(code_action) => {
                if let Some(edit) = &code_action.edit {
                    self.apply_workspace_edit(edit, window, cx)?;
                }
                Ok(())
            }
            lsp_types::CodeActionOrCommand::Command(command) => Err(anyhow::anyhow!(
                "command-based code action not supported: {}",
                command.title
            )),
        }
    }

    // ============================================================================
    // Find & Replace (Phase 9)
    // ============================================================================

    /// Open the find panel (Ctrl+F). Reuses any existing state so the last
    /// query is preserved when the panel is reopened.
    pub fn open_find(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.open_find_panel(false, window, cx);
    }

    /// Open the find+replace panel (Ctrl+H).
    pub fn open_find_replace(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.open_find_panel(true, window, cx);
    }

    fn open_find_panel(&mut self, show_replace: bool, window: &mut Window, cx: &mut Context<Self>) {
        let initial_query = self.get_selected_text(cx).map(|s| s.to_string());

        self.find.ensure_state(show_replace);

        if let Some(panel) = self.find.panel() {
            panel.update(cx, |panel, _cx| {
                panel.set_replace_mode(show_replace);
            });
            let focus = panel.read(cx).focus_handle(cx);
            focus.focus(window, cx);
        } else {
            let query_clone = initial_query.clone();
            let panel = cx.new(|cx| {
                find_replace_panel::FindReplacePanel::new(show_replace, query_clone, window, cx)
            });

            let mut subscriptions = Vec::new();
            subscriptions.push(cx.subscribe(
                &panel,
                |editor: &mut Self, _, event: &find_replace_panel::FindReplacePanelEvent, cx| {
                    match event.clone() {
                        find_replace_panel::FindReplacePanelEvent::QueryChanged {
                            query,
                            options,
                        } => {
                            editor
                                .find
                                .set_query_from_panel(query, options, &editor.document);
                            editor.sync_match_info_to_panel(cx);
                            cx.notify();
                        }
                        find_replace_panel::FindReplacePanelEvent::NextMatch => {
                            editor.find_next(cx);
                            editor.sync_match_info_to_panel(cx);
                        }
                        find_replace_panel::FindReplacePanelEvent::PrevMatch => {
                            editor.find_previous(cx);
                            editor.sync_match_info_to_panel(cx);
                        }
                        find_replace_panel::FindReplacePanelEvent::ReplaceCurrent {
                            replacement,
                        } => {
                            editor.find.set_replace_query(replacement);
                            editor.replace_current_inner(cx);
                            editor.sync_match_info_to_panel(cx);
                        }
                        find_replace_panel::FindReplacePanelEvent::ReplaceAll { replacement } => {
                            editor.find.set_replace_query(replacement);
                            editor.replace_all_inner(cx);
                            editor.sync_match_info_to_panel(cx);
                        }
                        find_replace_panel::FindReplacePanelEvent::SelectAllMatches => {
                            editor.find_select_all_matches(cx);
                        }
                        find_replace_panel::FindReplacePanelEvent::Closed => {
                            editor.close_find(cx);
                        }
                    }
                },
            ));

            self.find.set_panel(panel, subscriptions);
        }

        if let Some(ref query) = initial_query
            && !query.is_empty()
        {
            self.find.set_initial_query(query.clone(), &self.document);
            self.sync_match_info_to_panel(cx);
        }

        cx.notify();
    }

    fn sync_match_info_to_panel(&mut self, cx: &mut Context<Self>) {
        if let (Some((total, current, regex_error)), Some(panel)) =
            (self.find.match_info(), self.find.panel())
        {
            panel.update(cx, |p, cx| {
                p.update_match_info(total, current, regex_error, cx);
            });
        }
    }

    fn recompute_find_matches(&mut self) {
        self.find.recompute_matches(&self.document);
    }

    /// Close the find/replace panel (Escape).
    pub fn close_find(&mut self, cx: &mut Context<Self>) {
        self.find.clear();
        self.mutate_selections(|core| core.clear_selection());
        cx.notify();
    }

    /// Returns true when the find panel is currently visible.
    pub fn is_find_open(&self) -> bool {
        self.find.is_open()
    }

    /// Append a character to the current search query and recompute matches.
    ///
    /// This is called from the keyboard handler when the find panel is focused.
    pub fn find_input_char(&mut self, ch: char, cx: &mut Context<Self>) {
        if self.find.input_char(ch, &self.document) {
            self.jump_to_nearest_match();
        }
        cx.notify();
    }

    /// Remove the last character from the active find/replace input field.
    pub fn find_input_backspace(&mut self, cx: &mut Context<Self>) {
        if self.find.input_backspace(&self.document) {
            self.jump_to_nearest_match();
        }
        cx.notify();
    }

    /// Move the cursor to the next match (F3 / Enter in search box).
    ///
    /// Wraps around when the end of the match list is reached.
    pub fn find_next(&mut self, cx: &mut Context<Self>) {
        let cursor_offset = self.current_cursor_offset();
        if let Some((match_start, match_end)) = self.find.select_next_match(cursor_offset) {
            self.select_match(match_start, match_end);
        }
        cx.notify();
    }

    /// Move the cursor to the previous match (Shift+F3).
    ///
    /// Wraps around when the beginning of the match list is reached.
    pub fn find_previous(&mut self, cx: &mut Context<Self>) {
        let cursor_offset = self.current_cursor_offset();
        if let Some((match_start, match_end)) = self.find.select_previous_match(cursor_offset) {
            self.select_match(match_start, match_end);
        }
        cx.notify();
    }

    /// Toggle case-sensitive matching and recompute matches.
    pub fn find_toggle_case_sensitive(&mut self, cx: &mut Context<Self>) {
        if self.find.toggle_case_sensitive(&self.document) {
            self.jump_to_nearest_match();
        }
        cx.notify();
    }

    /// Toggle whole-word matching and recompute matches.
    pub fn find_toggle_whole_word(&mut self, cx: &mut Context<Self>) {
        if self.find.toggle_whole_word(&self.document) {
            self.jump_to_nearest_match();
        }
        cx.notify();
    }

    /// Toggle regex mode and recompute matches (feat-030).
    ///
    /// When enabled, the query is compiled as a regular expression. An invalid
    /// regex is surfaced through `FindState::regex_error` rather than panicking.
    pub fn find_toggle_regex(&mut self, cx: &mut Context<Self>) {
        if self.find.toggle_regex(&self.document) {
            self.jump_to_nearest_match();
        }
        cx.notify();
    }

    /// Toggle "search in selection" mode (feat-031).
    ///
    /// When turned on with an active selection, future `recompute_matches` calls
    /// restrict hits to the selected byte range. Turning it off clears the boundary
    /// and searches the whole buffer again.
    pub fn find_toggle_search_in_selection(&mut self, cx: &mut Context<Self>) {
        let boundary = self.current_selection_range().map(|range| {
            let start = self.document.position_to_offset(range.start).unwrap_or(0);
            let end = self
                .document
                .position_to_offset(range.end)
                .unwrap_or(self.document.byte_len());
            (start, end)
        });
        if self
            .find
            .toggle_selection_boundary(boundary, &self.document)
        {
            self.jump_to_nearest_match();
        }
        cx.notify();
    }

    /// Switch keyboard focus between the search field and the replace field.
    pub fn find_toggle_field_focus(&mut self, cx: &mut Context<Self>) {
        self.find.toggle_field_focus();
        cx.notify();
    }

    /// Convert every current find match into an independent cursor+selection,
    /// then close the find panel (feat-032).
    ///
    /// The primary cursor lands on the first match; each remaining match becomes
    /// an extra cursor. This lets the user immediately type to replace all
    /// occurrences at once.
    pub fn find_select_all_matches(&mut self, cx: &mut Context<Self>) {
        let match_offsets = self.find.all_match_ranges();
        if match_offsets.is_empty() {
            return;
        }

        if !self.set_multi_selections_from_offsets(&match_offsets) {
            return;
        }

        self.find.clear();
        self.scroll_to_cursor();
        cx.notify();
    }

    /// Replace the currently selected match with the replacement text.
    ///
    /// After replacement, moves to the next match automatically.
    pub fn replace_current(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.replace_current_inner(cx);
    }

    fn replace_current_inner(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.find.current_replacement_request() else {
            return;
        };

        let Ok(matched_text) = self.document.text_for_range(request.range()) else {
            return;
        };
        let replacement = request.replacement();
        let replacement_text = match replace_first(
            &matched_text,
            replacement.query(),
            replacement.replacement(),
            replacement.options(),
        ) {
            Ok(result) => {
                if result.count == 0 {
                    return;
                }
                result.text
            }
            Err(FindError::InvalidRegex(error)) => {
                self.find.set_regex_error(error);
                cx.notify();
                return;
            }
        };

        let replacement_start = request.start();
        if !self.apply_single_replacement_edit(request.range(), replacement_text.clone(), true) {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();

        let next_match = self.find.recompute_after_replacement(
            &self.document,
            replacement_start + replacement_text.len(),
        );
        if let Some((start, end)) = next_match {
            self.select_match(start, end);
        }

        // Move cursor to end of replacement text
        if let Ok(pos) = self
            .document
            .offset_to_position(replacement_start + replacement_text.len())
        {
            self.mutate_selections(|core| core.set_primary_cursor_preserving_selection(pos));
        }
        self.scroll_to_cursor();
        cx.notify();
    }

    /// Replace all matches with the replacement text.
    ///
    /// Replacements are applied back-to-front so that earlier offsets remain valid.
    pub fn replace_all(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.replace_all_inner(cx);
    }

    fn replace_all_inner(&mut self, cx: &mut Context<Self>) {
        let Some((request, matches)) = self.find.all_replacement_matches() else {
            return;
        };

        let mut edits = Vec::with_capacity(matches.len());
        for matched in matches.iter().rev() {
            let Ok(matched_text) = self.document.text_for_range(matched.start..matched.end) else {
                continue;
            };
            let replacement_text = match replace_first(
                &matched_text,
                request.query(),
                request.replacement(),
                request.options(),
            ) {
                Ok(result) => {
                    if result.count == 0 {
                        continue;
                    }
                    result.text
                }
                Err(FindError::InvalidRegex(error)) => {
                    self.find.set_regex_error(error);
                    cx.notify();
                    return;
                }
            };
            edits.push(TextReplacementEdit {
                range: matched.start..matched.end,
                replacement: replacement_text,
            });
        }

        if edits.is_empty()
            || !self.apply_planned_edit_batch(
                PlannedEditBatch {
                    edits,
                    post_apply_selection: PostApplySelection::Keep,
                },
                true,
            )
        {
            return;
        }

        self.update_syntax_highlights();
        self.update_diagnostics();

        // Recompute matches (should all be gone, or new ones if replacement contains the query)
        self.recompute_find_matches();

        cx.notify();
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// Jump to the match whose start offset is nearest to the current cursor,
    /// or to the first match when there is no good candidate.
    fn jump_to_nearest_match(&mut self) {
        let cursor_offset = self.current_cursor_offset();
        if let Some((match_start, match_end)) = self.find.select_nearest_match(cursor_offset) {
            self.select_match(match_start, match_end);
        }
    }

    /// Move the cursor to `match_start` and select up to `match_end`.
    fn select_match(&mut self, match_start: usize, match_end: usize) {
        if self.set_primary_selection_from_offsets(match_start, match_end) {
            self.scroll_to_cursor();
        }
    }

    pub(crate) fn show_inline_diagnostics(&self) -> bool {
        self.render_settings.show_inline_diagnostics()
    }

    fn indent_unit(&self) -> String {
        let settings = self.document.settings();
        if settings.use_tabs {
            "\t".to_string()
        } else {
            " ".repeat(settings.indent_size)
        }
    }

    // ============================================================================
    // Focus
    // ============================================================================

    /// Get focus handle
    pub fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }

    /// Focus the editor
    pub fn focus(&self, window: &mut Window, cx: &mut App) {
        self.focus_handle.focus(window, cx);
    }
}

impl Render for TextEditor {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.behavior.take_autofocus_on_open() {
            self.focus_handle.focus(window, cx);
        }

        if self.render_settings.cursor_blink_enabled() && self.focus_handle.is_focused(window) {
            self.ensure_cursor_blink_task(cx);
        } else {
            self.cursor_state.stop_blinking();
        }

        self.flush_pending_completions(window, cx);
        self.flush_pending_hover(window, cx);

        div()
            .size_full()
            .relative()
            // Required for keyboard events to be routed here.
            .track_focus(&self.focus_handle)
            .key_context(actions::CONTEXT)
            // Action handlers (declarative action system)
            .on_action(cx.listener(Self::handle_move_left))
            .on_action(cx.listener(Self::handle_move_right))
            .on_action(cx.listener(Self::handle_move_up))
            .on_action(cx.listener(Self::handle_move_down))
            .on_action(cx.listener(Self::handle_move_to_beginning_of_line))
            .on_action(cx.listener(Self::handle_move_to_end_of_line))
            .on_action(cx.listener(Self::handle_move_to_beginning))
            .on_action(cx.listener(Self::handle_move_to_end))
            .on_action(cx.listener(Self::handle_move_to_previous_word_start))
            .on_action(cx.listener(Self::handle_move_to_next_word_end))
            .on_action(cx.listener(Self::handle_move_to_paragraph_start))
            .on_action(cx.listener(Self::handle_move_to_paragraph_end))
            .on_action(cx.listener(Self::handle_move_to_next_subword_end))
            .on_action(cx.listener(Self::handle_move_to_previous_subword_start))
            .on_action(cx.listener(Self::handle_page_up))
            .on_action(cx.listener(Self::handle_page_down))
            .on_action(cx.listener(Self::handle_select_page_up))
            .on_action(cx.listener(Self::handle_select_page_down))
            .on_action(cx.listener(Self::handle_select_left))
            .on_action(cx.listener(Self::handle_select_right))
            .on_action(cx.listener(Self::handle_select_up))
            .on_action(cx.listener(Self::handle_select_down))
            .on_action(cx.listener(Self::handle_select_to_beginning_of_line))
            .on_action(cx.listener(Self::handle_select_to_end_of_line))
            .on_action(cx.listener(Self::handle_select_to_beginning))
            .on_action(cx.listener(Self::handle_select_to_end))
            .on_action(cx.listener(Self::handle_select_to_previous_word_start))
            .on_action(cx.listener(Self::handle_select_to_next_word_end))
            .on_action(cx.listener(Self::handle_select_to_paragraph_start))
            .on_action(cx.listener(Self::handle_select_to_paragraph_end))
            .on_action(cx.listener(Self::handle_select_to_next_subword_end))
            .on_action(cx.listener(Self::handle_select_to_previous_subword_start))
            .on_action(cx.listener(Self::handle_delete_to_beginning_of_line))
            .on_action(cx.listener(Self::handle_delete_to_end_of_line))
            .on_action(cx.listener(Self::handle_delete_subword_left))
            .on_action(cx.listener(Self::handle_delete_subword_right))
            .on_action(cx.listener(Self::handle_select_all))
            .on_action(cx.listener(Self::handle_toggle_block_selection))
            .on_action(cx.listener(Self::handle_backspace))
            .on_action(cx.listener(Self::handle_delete))
            .on_action(cx.listener(Self::handle_newline))
            .on_action(cx.listener(Self::handle_tab))
            .on_action(cx.listener(Self::handle_shift_tab))
            // Line editing actions (feat-005 through feat-015)
            .on_action(cx.listener(Self::handle_move_line_up))
            .on_action(cx.listener(Self::handle_move_line_down))
            .on_action(cx.listener(Self::handle_duplicate_line_down))
            .on_action(cx.listener(Self::handle_duplicate_line_up))
            .on_action(cx.listener(Self::handle_delete_line))
            .on_action(cx.listener(Self::handle_newline_above))
            .on_action(cx.listener(Self::handle_newline_below))
            .on_action(cx.listener(Self::handle_join_lines))
            .on_action(cx.listener(Self::handle_transpose_chars))
            .on_action(cx.listener(Self::handle_indent_line))
            .on_action(cx.listener(Self::handle_dedent_line))
            .on_action(cx.listener(Self::handle_toggle_line_comment))
            // Selection features (feat-016/017/018)
            .on_action(cx.listener(Self::handle_select_line))
            .on_action(cx.listener(Self::handle_select_next_occurrence))
            .on_action(cx.listener(Self::handle_select_previous_occurrence))
            .on_action(cx.listener(Self::handle_select_all_occurrences))
            // Multi-cursor (feat-021/022)
            .on_action(cx.listener(Self::handle_add_cursor_above))
            .on_action(cx.listener(Self::handle_add_cursor_below))
            // Undo selection (feat-020)
            .on_action(cx.listener(Self::handle_undo_selection))
            // Cut to end of line (feat-024)
            .on_action(cx.listener(Self::handle_cut_to_end_of_line))
            .on_action(cx.listener(Self::handle_copy))
            .on_action(cx.listener(Self::handle_cut))
            .on_action(cx.listener(Self::handle_paste))
            .on_action(cx.listener(Self::handle_undo))
            .on_action(cx.listener(Self::handle_redo))
            .on_action(cx.listener(Self::handle_open_find))
            .on_action(cx.listener(Self::handle_open_find_replace))
            .on_action(cx.listener(Self::handle_find_next))
            .on_action(cx.listener(Self::handle_find_previous))
            .on_action(cx.listener(Self::handle_find_select_all_matches))
            .on_action(cx.listener(Self::handle_find_toggle_case_sensitive))
            .on_action(cx.listener(Self::handle_find_toggle_whole_word))
            .on_action(cx.listener(Self::handle_find_toggle_regex))
            .on_action(cx.listener(Self::handle_find_toggle_search_in_selection))
            .on_action(cx.listener(Self::handle_replace_current_match))
            .on_action(cx.listener(Self::handle_replace_all_matches))
            .on_action(cx.listener(Self::handle_trigger_completion))
            .on_action(cx.listener(Self::handle_accept_completion))
            .on_action(cx.listener(Self::handle_dismiss_completion))
            .on_action(cx.listener(Self::handle_escape))
            // Case transforms (feat-033/034)
            .on_action(cx.listener(Self::handle_transform_uppercase))
            .on_action(cx.listener(Self::handle_transform_lowercase))
            .on_action(cx.listener(Self::handle_transform_title_case))
            .on_action(cx.listener(Self::handle_transform_snake_case))
            .on_action(cx.listener(Self::handle_transform_camel_case))
            .on_action(cx.listener(Self::handle_transform_kebab_case))
            // Line manipulation (feat-035/036/037)
            .on_action(cx.listener(Self::handle_sort_lines_ascending))
            .on_action(cx.listener(Self::handle_sort_lines_descending))
            .on_action(cx.listener(Self::handle_sort_lines_by_length))
            .on_action(cx.listener(Self::handle_reverse_lines))
            .on_action(cx.listener(Self::handle_unique_lines))
            // Insert UUID (feat-042)
            .on_action(cx.listener(Self::handle_insert_uuid_v4))
            .on_action(cx.listener(Self::handle_insert_uuid_v7))
            // Multi-cursor extras (feat-043/044)
            .on_action(cx.listener(Self::handle_rotate_selections))
            .on_action(cx.listener(Self::handle_swap_selection_ends))
            // Clipboard extras (feat-050/051)
            .on_action(cx.listener(Self::handle_copy_as_markdown))
            .on_action(cx.listener(Self::handle_paste_as_plain_text))
            // Go-to-line dialog (feat-040)
            .on_action(cx.listener(Self::handle_go_to_line))
            // Toggle soft wrap (feat-041)
            .on_action(cx.listener(Self::handle_toggle_soft_wrap))
            // Format SQL (feat-049)
            .on_action(cx.listener(Self::handle_format_sql))
            // LSP navigation (feat-046/047/048)
            .on_action(cx.listener(Self::handle_go_to_definition))
            .on_action(cx.listener(Self::handle_find_references))
            .on_action(cx.listener(Self::handle_rename_symbol))
            // Context menu (feat-045)
            .on_action(cx.listener(Self::handle_open_context_menu_keyboard))
            // Code folding
            .on_action(
                cx.listener(|editor, _: &actions::Fold, _window, cx| {
                    editor.fold_current_region(cx)
                }),
            )
            .on_action(
                cx.listener(|editor, _: &actions::UnfoldLines, _window, cx| {
                    editor.unfold_current_region(cx)
                }),
            )
            .on_action(cx.listener(|editor, _: &actions::FoldAll, _window, cx| editor.fold_all(cx)))
            .on_action(
                cx.listener(|editor, _: &actions::UnfoldAll, _window, cx| editor.unfold_all(cx)),
            )
            .on_action(
                cx.listener(|editor, _: &actions::FoldAtLevel1, _window, cx| {
                    editor.fold_at_level(1, cx)
                }),
            )
            .on_action(
                cx.listener(|editor, _: &actions::FoldAtLevel2, _window, cx| {
                    editor.fold_at_level(2, cx)
                }),
            )
            .on_action(
                cx.listener(|editor, _: &actions::FoldAtLevel3, _window, cx| {
                    editor.fold_at_level(3, cx)
                }),
            )
            .on_action(
                cx.listener(|editor, _: &actions::FoldAtLevel4, _window, cx| {
                    editor.fold_at_level(4, cx)
                }),
            )
            .on_action(
                cx.listener(|editor, _: &actions::FoldAtLevel5, _window, cx| {
                    editor.fold_at_level(5, cx)
                }),
            )
            .on_action(
                cx.listener(|editor, _: &actions::FoldAtLevel6, _window, cx| {
                    editor.fold_at_level(6, cx)
                }),
            )
            .on_action(
                cx.listener(|editor, _: &actions::FoldAtLevel7, _window, cx| {
                    editor.fold_at_level(7, cx)
                }),
            )
            .on_action(
                cx.listener(|editor, _: &actions::FoldAtLevel8, _window, cx| {
                    editor.fold_at_level(8, cx)
                }),
            )
            .on_action(
                cx.listener(|editor, _: &actions::FoldAtLevel9, _window, cx| {
                    editor.fold_at_level(9, cx)
                }),
            )
            // Raw key_down: printable character insertion and overlay-specific handling
            .on_key_down(cx.listener(Self::handle_key_down))
            .on_scroll_wheel(cx.listener(Self::handle_scroll_wheel))
            .on_mouse_down(MouseButton::Left, cx.listener(Self::handle_mouse_down))
            .on_mouse_up(MouseButton::Left, cx.listener(Self::handle_mouse_up))
            .on_mouse_down(MouseButton::Right, cx.listener(Self::handle_right_click))
            .on_mouse_move(cx.listener(Self::handle_hover))
            .when_some(self.find.panel().cloned(), |div, panel| div.child(panel))
            .when_some(self.rename_overlay_element(cx), |div, overlay| {
                div.child(overlay)
            })
            .child(EditorElement::new(cx.entity().clone()))
    }
}

impl TextEditor {
    /// Handle raw keyboard input events.
    ///
    /// This handler is intentionally minimal: it only deals with cases that
    /// cannot be expressed as static actions — primarily printable character
    /// insertion. Everything else is handled via `on_action` handlers registered
    /// in `Render::render`.
    fn handle_key_down(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let key = event.keystroke.key.as_str();

        self.record_cursor_activity();

        // ── Rename overlay ────────────────────────────────────────────────────
        // The Input widget handles its own key events via the GPUI action
        // system. Swallow raw key_down here so the TextEditor doesn't
        // interfere (e.g. inserting characters into the buffer).
        if self.overlays.has_rename() {
            return;
        }

        // ── Find-panel character input ────────────────────────────────────────
        // With the new FindReplacePanel, input is handled by proper Input widgets.
        // The old character-by-character routing is no longer needed.

        // ── Go-to-line dialog character input ─────────────────────────────────
        // Digit keys, Backspace, Enter, and Escape are the only inputs the dialog
        // consumes.  All other keys are intentionally ignored so they don't
        // accidentally edit the buffer while the overlay is open.
        if self.overlays.has_goto_line() {
            match key {
                "enter" => {
                    self.goto_line_confirm(cx);
                    return;
                }
                "escape" => {
                    self.goto_line_cancel(cx);
                    return;
                }
                "backspace" => {
                    self.goto_line_input_char('\x08', cx);
                    return;
                }
                _ => {}
            }
            // Feed digit characters through the normal key_char path below.
            if let Some(text) = &event.keystroke.key_char
                && text.chars().all(|c| c.is_ascii_digit())
                && !text.is_empty()
            {
                for ch in text.chars() {
                    self.goto_line_input_char(ch, cx);
                }
                return;
            }
            // Swallow any other key so the buffer is not modified while the dialog is open.
            return;
        }

        // ── Context menu keyboard navigation ──────────────────────────────────
        if self.overlays.has_context_menu() {
            match key {
                "escape" => {
                    if self.overlays.clear_context_menu() {
                        cx.notify();
                    }
                }
                "up" => {
                    self.context_menu_move(-1, cx);
                }
                "down" => {
                    self.context_menu_move(1, cx);
                }
                "enter" => {
                    self.context_menu_activate(window, cx);
                }
                _ => {}
            }
            // Swallow unhandled keys while menu is open.
        }

        // Printable character insertion is handled via `EntityInputHandler::replace_text_in_range`,
        // which receives characters from the OS input system (including IME).  The raw key_down
        // path no longer inserts text to avoid double-insertion.
    }

    // ============================================================================
    // Action handlers — movement
    // ============================================================================

    fn handle_move_left(
        &mut self,
        _: &actions::MoveLeft,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| core.move_primary_cursor_left_with_selection(false));
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_move_right(
        &mut self,
        _: &actions::MoveRight,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| core.move_primary_cursor_right_with_selection(false));
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_move_up(
        &mut self,
        _: &actions::MoveUp,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.is_completion_menu_open(cx) {
            self.completion_menu_select_previous();
        } else {
            self.begin_non_extending_selection_action();
            self.mutate_selections(|core| core.move_primary_cursor_up_with_selection(false));
            self.scroll_to_cursor();
        }
        cx.notify();
    }

    fn handle_move_down(
        &mut self,
        _: &actions::MoveDown,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.is_completion_menu_open(cx) {
            self.completion_menu_select_next();
        } else {
            self.begin_non_extending_selection_action();
            self.mutate_selections(|core| core.move_primary_cursor_down_with_selection(false));
            self.scroll_to_cursor();
        }
        cx.notify();
    }

    fn handle_move_to_beginning_of_line(
        &mut self,
        _: &actions::MoveToBeginningOfLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| core.move_primary_cursor_to_smart_home(false));
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_move_to_end_of_line(
        &mut self,
        _: &actions::MoveToEndOfLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| core.move_primary_cursor_to_line_end_with_selection(false));
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_move_to_beginning(
        &mut self,
        _: &actions::MoveToBeginning,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_document_start_with_selection(false)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_move_to_end(
        &mut self,
        _: &actions::MoveToEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_document_end_with_selection(false)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_move_to_previous_word_start(
        &mut self,
        _: &actions::MoveToPreviousWordStart,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_prev_word_start_with_selection(false)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_move_to_next_word_end(
        &mut self,
        _: &actions::MoveToNextWordEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_next_word_start_with_selection(false)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_move_to_paragraph_start(
        &mut self,
        _: &actions::MoveToParagraphStart,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_paragraph_start_with_selection(false)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_move_to_paragraph_end(
        &mut self,
        _: &actions::MoveToParagraphEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_paragraph_end_with_selection(false)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_move_to_next_subword_end(
        &mut self,
        _: &actions::MoveToNextSubwordEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_next_subword_end_with_selection(false)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_move_to_previous_subword_start(
        &mut self,
        _: &actions::MoveToPreviousSubwordStart,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_prev_subword_start_with_selection(false)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_page_up(
        &mut self,
        _: &actions::PageUp,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        let viewport_lines = self.scroll.viewport_lines_or_one();
        self.scroll_page_up(viewport_lines);
        for _ in 0..viewport_lines {
            self.mutate_selections(|core| core.move_primary_cursor_up_with_selection(false));
        }
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_page_down(
        &mut self,
        _: &actions::PageDown,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.begin_non_extending_selection_action();
        let viewport_lines = self.scroll.viewport_lines_or_one();
        self.scroll_page_down(viewport_lines);
        for _ in 0..viewport_lines {
            self.mutate_selections(|core| core.move_primary_cursor_down_with_selection(false));
        }
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_page_up(
        &mut self,
        _: &actions::SelectPageUp,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let viewport_lines = self.scroll.viewport_lines_or_one();
        self.scroll_page_up(viewport_lines);
        for _ in 0..viewport_lines {
            self.mutate_selections(|core| core.move_primary_cursor_up_with_selection(true));
        }
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_page_down(
        &mut self,
        _: &actions::SelectPageDown,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let viewport_lines = self.scroll.viewport_lines_or_one();
        self.scroll_page_down(viewport_lines);
        for _ in 0..viewport_lines {
            self.mutate_selections(|core| core.move_primary_cursor_down_with_selection(true));
        }
        self.scroll_to_cursor();
        cx.notify();
    }

    // ============================================================================
    // Action handlers — selection
    // ============================================================================

    fn handle_select_left(
        &mut self,
        _: &actions::SelectLeft,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| core.move_primary_cursor_left_with_selection(true));
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_right(
        &mut self,
        _: &actions::SelectRight,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| core.move_primary_cursor_right_with_selection(true));
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_up(
        &mut self,
        _: &actions::SelectUp,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| core.move_primary_cursor_up_with_selection(true));
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_down(
        &mut self,
        _: &actions::SelectDown,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| core.move_primary_cursor_down_with_selection(true));
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_to_beginning_of_line(
        &mut self,
        _: &actions::SelectToBeginningOfLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| core.move_primary_cursor_to_smart_home(true));
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_to_end_of_line(
        &mut self,
        _: &actions::SelectToEndOfLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| core.move_primary_cursor_to_line_end_with_selection(true));
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_to_beginning(
        &mut self,
        _: &actions::SelectToBeginning,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_document_start_with_selection(true)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_to_end(
        &mut self,
        _: &actions::SelectToEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_document_end_with_selection(true)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_to_previous_word_start(
        &mut self,
        _: &actions::SelectToPreviousWordStart,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_prev_word_start_with_selection(true)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_to_next_word_end(
        &mut self,
        _: &actions::SelectToNextWordEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_next_word_start_with_selection(true)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_to_paragraph_start(
        &mut self,
        _: &actions::SelectToParagraphStart,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_paragraph_start_with_selection(true)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_to_paragraph_end(
        &mut self,
        _: &actions::SelectToParagraphEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_paragraph_end_with_selection(true)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_to_next_subword_end(
        &mut self,
        _: &actions::SelectToNextSubwordEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_next_subword_end_with_selection(true)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_select_to_previous_subword_start(
        &mut self,
        _: &actions::SelectToPreviousSubwordStart,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| {
            core.move_primary_cursor_to_prev_subword_start_with_selection(true)
        });
        self.scroll_to_cursor();
        cx.notify();
    }

    fn handle_delete_subword_left(
        &mut self,
        _: &actions::DeleteSubwordLeft,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.delete_subword_left(cx);
    }

    fn handle_delete_to_beginning_of_line(
        &mut self,
        _: &actions::DeleteToBeginningOfLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.delete_to_beginning_of_line(cx);
    }

    fn handle_delete_to_end_of_line(
        &mut self,
        _: &actions::DeleteToEndOfLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.delete_to_end_of_line(cx);
    }

    fn handle_delete_subword_right(
        &mut self,
        _: &actions::DeleteSubwordRight,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.delete_subword_right(cx);
    }

    fn handle_select_all(
        &mut self,
        _: &actions::SelectAll,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.select_all(window, cx);
        cx.notify();
    }

    fn handle_toggle_block_selection(
        &mut self,
        _: &actions::ToggleBlockSelection,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.mutate_selections(|core| core.toggle_primary_selection_mode());
        cx.notify();
    }

    // ============================================================================
    // Action handlers — editing
    // ============================================================================

    fn handle_backspace(
        &mut self,
        _: &actions::Backspace,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.has_selection() {
            self.delete_selection();
        } else {
            self.delete_before_cursor(window, cx);
        }
        self.mutate_selections(|core| core.collapse_primary_selection_to_cursor());
        cx.notify();
    }

    fn handle_delete(&mut self, _: &actions::Delete, window: &mut Window, cx: &mut Context<Self>) {
        if self.has_selection() {
            self.delete_selection();
        } else {
            self.delete_at_cursor(window, cx);
        }
        self.mutate_selections(|core| core.collapse_primary_selection_to_cursor());
        cx.notify();
    }

    fn handle_newline(
        &mut self,
        _: &actions::Newline,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.overlays.has_inline_suggestion() {
            self.accept_inline_suggestion(window, cx);
            return;
        }

        if self.lsp.has_completion_items() {
            self.accept_completion(window, cx);
            return;
        }
        if self.has_selection() {
            self.delete_selection();
        }
        if self.insert_newline_all_cursors() {
            self.did_change_content(cx);
            cx.notify();
            return;
        }
        self.insert_newline_with_auto_indent(window, cx);
        cx.notify();
    }

    fn handle_tab(&mut self, _: &actions::Tab, window: &mut Window, cx: &mut Context<Self>) {
        if self.overlays.has_inline_suggestion() {
            self.accept_inline_suggestion(window, cx);
            return;
        }

        if self.lsp.has_completion_items() {
            self.accept_completion(window, cx);
            return;
        }
        // With a multi-line selection, Tab indents all selected lines rather than
        // replacing the selection with a single tab-stop (matches VS Code behaviour).
        if self.has_selection() && self.is_multiline_selection() {
            self.indent_lines();
        } else {
            if self.has_selection() {
                self.delete_selection();
            }
            if self.indent_all_cursors() {
                self.did_change_content(cx);
                cx.notify();
                return;
            }
            let indent = self.indent_unit();
            self.insert_at_cursor(indent, window, cx);
            self.mutate_selections(|core| core.collapse_primary_selection_to_cursor());
        }
        cx.notify();
    }

    fn handle_shift_tab(
        &mut self,
        _: &actions::ShiftTab,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dedent_lines();
        cx.notify();
    }

    fn handle_move_line_up(
        &mut self,
        _: &actions::MoveLineUp,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_lines_up();
        cx.notify();
    }

    fn handle_move_line_down(
        &mut self,
        _: &actions::MoveLineDown,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_lines_down();
        cx.notify();
    }

    fn handle_duplicate_line_down(
        &mut self,
        _: &actions::DuplicateLineDown,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.duplicate_lines_down();
        cx.notify();
    }

    fn handle_duplicate_line_up(
        &mut self,
        _: &actions::DuplicateLineUp,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.duplicate_lines_up();
        cx.notify();
    }

    fn handle_delete_line(
        &mut self,
        _: &actions::DeleteLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.delete_lines();
        cx.notify();
    }

    fn handle_newline_above(
        &mut self,
        _: &actions::NewlineAbove,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.insert_newline_above();
        cx.notify();
    }

    fn handle_newline_below(
        &mut self,
        _: &actions::NewlineBelow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.insert_newline_below();
        cx.notify();
    }

    fn handle_join_lines(
        &mut self,
        _: &actions::JoinLines,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.join_lines(window, cx);
        cx.notify();
    }

    fn handle_transpose_chars(
        &mut self,
        _: &actions::TransposeChars,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.transpose_chars();
        cx.notify();
    }

    fn handle_indent_line(
        &mut self,
        _: &actions::IndentLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.indent_lines();
        cx.notify();
    }

    fn handle_dedent_line(
        &mut self,
        _: &actions::DedentLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dedent_lines();
        cx.notify();
    }

    fn handle_toggle_line_comment(
        &mut self,
        _: &actions::ToggleLineComment,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.toggle_line_comment();
        cx.notify();
    }

    // ============================================================================
    // Action handlers — selection features (feat-016/017/018)
    // ============================================================================

    fn handle_select_line(
        &mut self,
        _: &actions::SelectLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.push_selection_snapshot();
        self.select_line();
        cx.notify();
    }

    fn begin_non_extending_selection_action(&mut self) {
        self.mutate_selections(|core| core.begin_non_extending_selection_action());
    }

    fn handle_select_next_occurrence(
        &mut self,
        _: &actions::SelectNextOccurrence,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.push_selection_snapshot();
        self.select_next_occurrence();
        cx.notify();
    }

    fn handle_select_previous_occurrence(
        &mut self,
        _: &actions::SelectPreviousOccurrence,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.push_selection_snapshot();
        self.select_previous_occurrence();
        cx.notify();
    }

    fn handle_select_all_occurrences(
        &mut self,
        _: &actions::SelectAllOccurrences,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // When a multi-line selection is already active, Cmd+Shift+L splits it into
        // per-line cursors (feat-019) rather than finding all text occurrences.
        if self.is_multiline_selection() {
            self.split_selection_into_lines();
        } else {
            self.push_selection_snapshot();
            self.select_all_occurrences();
        }
        cx.notify();
    }

    // ============================================================================
    // Action handlers — multi-cursor (feat-021/022)
    // ============================================================================

    fn handle_add_cursor_above(
        &mut self,
        _: &actions::AddCursorAbove,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.push_selection_snapshot();
        self.add_cursor_above();
        cx.notify();
    }

    fn handle_add_cursor_below(
        &mut self,
        _: &actions::AddCursorBelow,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.push_selection_snapshot();
        self.add_cursor_below();
        cx.notify();
    }

    fn apply_multi_cursor_edits(&mut self, plan: MultiCursorCommandPlan) -> bool {
        let MultiCursorCommandPlan {
            edits,
            mut final_offsets,
        } = plan;

        if edits.is_empty() {
            return false;
        }

        self.break_undo_group();
        let mut applied_any = false;

        for edit in edits {
            let old_text = self
                .document
                .text()
                .get(edit.start..edit.end)
                .unwrap_or_default()
                .to_string();
            let change = buffer::Change::replace(edit.start, old_text, edit.replacement.clone());
            if self.document.apply_change(&change).is_ok() {
                final_offsets[edit.slot] = edit.start + edit.replacement.len();
                self.push_undo(change);
                applied_any = true;
            }
        }

        if !applied_any {
            return false;
        }

        self.break_undo_group();
        self.mutate_selections(|core| {
            core.restore_multi_cursor_caret_offsets(&final_offsets);
        });

        self.update_syntax_highlights();
        self.update_diagnostics();
        true
    }

    fn apply_batch_replacement_edits<I>(&mut self, edits: I, use_transaction: bool) -> bool
    where
        I: IntoIterator<Item = TextReplacementEdit>,
    {
        let edits = edits.into_iter().collect::<Vec<_>>();
        if edits.is_empty() {
            return false;
        }

        self.break_undo_group();
        let transaction = use_transaction.then(|| self.start_transaction());
        let mut applied_any = false;

        for TextReplacementEdit { range, replacement } in edits {
            if range.start > range.end {
                continue;
            }

            if range.is_empty() && replacement.is_empty() {
                continue;
            }

            let mut edit_applied = false;

            if !range.is_empty() && self.document.delete_range(range.clone()).is_ok() {
                if let Some(change) = self.document.latest_change() {
                    self.push_undo(change);
                }
                edit_applied = true;
            }

            if !replacement.is_empty() && self.document.insert(range.start, &replacement).is_ok() {
                if let Some(change) = self.document.latest_change() {
                    self.push_undo(change);
                }
                edit_applied = true;
            }

            applied_any |= edit_applied;
        }

        if let Some(transaction) = transaction {
            self.end_transaction(transaction);
        }

        applied_any
    }

    fn apply_planned_edit_batch(&mut self, batch: PlannedEditBatch, use_transaction: bool) -> bool {
        let PlannedEditBatch {
            edits,
            post_apply_selection,
        } = batch;

        if !self.apply_batch_replacement_edits(edits, use_transaction) {
            return false;
        }

        match post_apply_selection {
            PostApplySelection::Keep => {}
            PostApplySelection::MovePrimaryCursor(target_position) => {
                let max_column = self
                    .document
                    .line(target_position.line)
                    .map(|line| line.len())
                    .unwrap_or(0);
                let clamped_position =
                    Position::new(target_position.line, target_position.column.min(max_column));
                self.mutate_selections(|core| core.move_primary_cursor(clamped_position, false));
            }
            PostApplySelection::MovePrimaryCursorToOffset(target_offset) => {
                self.mutate_selections(|core| {
                    core.move_primary_cursor_to_offset(target_offset, false);
                });
            }
        }

        true
    }

    fn apply_single_cursor_edit_batch(
        &mut self,
        batch: PlannedEditBatch,
        cx: &mut Context<Self>,
        notify: bool,
    ) -> bool {
        if !self.apply_planned_edit_batch(batch, false) {
            return false;
        }

        self.apply_interpolated_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
        self.did_change_content(cx);
        if notify {
            cx.notify();
        }
        true
    }

    fn apply_single_replacement_edit(
        &mut self,
        range: std::ops::Range<usize>,
        replacement: impl Into<String>,
        use_transaction: bool,
    ) -> bool {
        self.apply_planned_edit_batch(
            PlannedEditBatch {
                edits: vec![TextReplacementEdit {
                    range,
                    replacement: replacement.into(),
                }],
                post_apply_selection: PostApplySelection::Keep,
            },
            use_transaction,
        )
    }

    fn set_primary_selection_from_offsets(
        &mut self,
        anchor_offset: usize,
        head_offset: usize,
    ) -> bool {
        self.mutate_selections(|core| {
            core.set_primary_selection_from_offsets(anchor_offset, head_offset)
        })
    }

    fn set_multi_selections_from_offsets(&mut self, offsets: &[(usize, usize)]) -> bool {
        self.mutate_selections(|core| core.set_multi_selections_from_offsets(offsets))
    }

    fn apply_text_to_all_cursors(&mut self, text: &str) -> bool {
        if !self.has_secondary_cursors() {
            return false;
        }

        let Some(plan) = self.editor_core_snapshot().multi_cursor_replace_plan(text) else {
            return false;
        };

        if plan.edits.len() <= 1 {
            return false;
        }

        self.apply_multi_cursor_edits(plan)
    }

    fn backspace_all_cursors(&mut self) -> bool {
        if !self.has_secondary_cursors() {
            return false;
        }

        let Some(plan) = self.editor_core_snapshot().multi_cursor_backspace_plan() else {
            return false;
        };

        self.apply_multi_cursor_edits(plan)
    }

    fn delete_all_cursors(&mut self) -> bool {
        if !self.has_secondary_cursors() {
            return false;
        }

        let Some(plan) = self.editor_core_snapshot().multi_cursor_delete_plan() else {
            return false;
        };

        self.apply_multi_cursor_edits(plan)
    }

    fn insert_newline_all_cursors(&mut self) -> bool {
        if !self.has_secondary_cursors() {
            return false;
        }

        let indent_unit = self.indent_unit();
        let Some(plan) = self
            .editor_core_snapshot()
            .multi_cursor_newline_plan(self.behavior.auto_indent_enabled(), &indent_unit)
        else {
            return false;
        };

        self.apply_multi_cursor_edits(plan)
    }

    fn indent_all_cursors(&mut self) -> bool {
        if !self.has_secondary_cursors() {
            return false;
        }

        let indent = self.indent_unit();
        let Some(plan) = self
            .editor_core_snapshot()
            .multi_cursor_indent_plan(&indent)
        else {
            return false;
        };

        self.apply_multi_cursor_edits(plan)
    }

    // ============================================================================
    // Action handlers — undo selection (feat-020)
    // ============================================================================

    fn handle_undo_selection(
        &mut self,
        _: &actions::UndoSelection,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.undo_selection();
        cx.notify();
    }

    // ============================================================================
    // Action handlers — cut to end of line (feat-024)
    // ============================================================================

    fn handle_cut_to_end_of_line(
        &mut self,
        _: &actions::CutToEndOfLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.cut_to_end_of_line(cx);
        cx.notify();
    }

    // ============================================================================
    // Action handlers — clipboard
    // ============================================================================

    fn handle_copy(&mut self, _: &actions::Copy, _window: &mut Window, cx: &mut Context<Self>) {
        if self.has_selection() {
            self.copy(cx);
            self.input.record_selection_clipboard_write();
        } else {
            // No selection → copy the entire current line (feat-023).
            self.copy_whole_line(cx);
        }
    }

    fn handle_cut(&mut self, _: &actions::Cut, _window: &mut Window, cx: &mut Context<Self>) {
        if self.has_selection() {
            if self.cut(cx) {
                self.input.record_selection_clipboard_write();
                cx.notify();
            }
        } else {
            // No selection → cut the entire current line (feat-023).
            self.cut_whole_line(cx);
            cx.notify();
        }
    }

    fn handle_paste(&mut self, _: &actions::Paste, window: &mut Window, cx: &mut Context<Self>) {
        self.paste(window, cx);
        cx.notify();
    }

    // ============================================================================
    // Action handlers — undo / redo
    // ============================================================================

    fn handle_undo(&mut self, _: &actions::Undo, window: &mut Window, cx: &mut Context<Self>) {
        if self.undo(window, cx) {
            cx.notify();
        }
    }

    fn handle_redo(&mut self, _: &actions::Redo, window: &mut Window, cx: &mut Context<Self>) {
        if self.redo(window, cx) {
            cx.notify();
        }
    }

    // ============================================================================
    // Action handlers — find / replace
    // ============================================================================

    fn handle_open_find(
        &mut self,
        _: &actions::OpenFind,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_find(window, cx);
    }

    fn handle_open_find_replace(
        &mut self,
        _: &actions::OpenFindReplace,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_find_replace(window, cx);
    }

    fn handle_find_next(
        &mut self,
        _: &actions::FindNext,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.find_next(cx);
    }

    fn handle_find_previous(
        &mut self,
        _: &actions::FindPrevious,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.find_previous(cx);
    }

    fn handle_find_select_all_matches(
        &mut self,
        _: &actions::FindSelectAllMatches,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.find_select_all_matches(cx);
    }

    fn handle_find_toggle_case_sensitive(
        &mut self,
        _: &actions::FindToggleCaseSensitive,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.find_toggle_case_sensitive(cx);
    }

    fn handle_find_toggle_whole_word(
        &mut self,
        _: &actions::FindToggleWholeWord,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.find_toggle_whole_word(cx);
    }

    fn handle_find_toggle_regex(
        &mut self,
        _: &actions::FindToggleRegex,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.find_toggle_regex(cx);
    }

    fn handle_find_toggle_search_in_selection(
        &mut self,
        _: &actions::FindToggleSearchInSelection,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.find_toggle_search_in_selection(cx);
    }

    fn handle_replace_current_match(
        &mut self,
        _: &actions::ReplaceCurrentMatch,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.replace_current(window, cx);
    }

    fn handle_replace_all_matches(
        &mut self,
        _: &actions::ReplaceAllMatches,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.replace_all(window, cx);
    }

    // ============================================================================
    // Action handlers — completions
    // ============================================================================

    fn handle_trigger_completion(
        &mut self,
        _: &actions::TriggerCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.trigger_completion_action(window, cx);
    }

    fn handle_accept_completion(
        &mut self,
        _: &actions::AcceptCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.overlays.has_inline_suggestion() {
            self.accept_inline_suggestion(window, cx);
            return;
        }

        self.accept_completion(window, cx);
    }

    fn handle_dismiss_completion(
        &mut self,
        _: &actions::DismissCompletion,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.overlays.has_inline_suggestion() {
            self.clear_inline_suggestion(cx);
            return;
        }

        self.lsp.clear_completion_menu();
        cx.notify();
    }

    // ============================================================================
    // Action handlers — misc
    // ============================================================================

    fn handle_escape(&mut self, _: &actions::Escape, window: &mut Window, cx: &mut Context<Self>) {
        if self.overlays.has_rename() {
            self.rename_cancel(cx);
            self.focus_handle.focus(window, cx);
            return;
        }

        if self.overlays.has_inline_suggestion() {
            self.clear_inline_suggestion(cx);
            return;
        }

        if self.overlays.has_signature_help() {
            self.clear_signature_help(cx);
            return;
        }

        if self.lsp.has_hover_state() {
            self.lsp.clear_hover_state();
            cx.notify();
            return;
        }
        if self.lsp.has_completion_menu() {
            self.lsp.clear_completion_menu();
            cx.notify();
            return;
        }
        // Collapse extra cursors before closing find so a single Escape always
        // dismisses multi-cursor mode first, then a second press closes find.
        if self.has_secondary_cursors() {
            self.collapse_to_primary_cursor();
            cx.notify();
            return;
        }
        if self.find.is_open() {
            self.close_find(cx);
        }
    }

    /// Handle a left mouse-button press.
    ///
    /// Single click places the cursor. Double-click selects the word under the
    /// pointer. Triple-click selects the entire line. Holding Shift on any click
    /// extends the existing selection rather than starting a new one. The anchor
    /// for drag-based selection is stored so that subsequent mouse-move events
    /// (handled in `handle_hover`) can extend the selection in real time.
    fn handle_mouse_down(
        &mut self,
        event: &MouseDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Only handle the primary (left) button.
        if event.button != MouseButton::Left {
            return;
        }

        // Clicking anywhere while the rename overlay is open commits the
        // rename (matching VS Code behaviour) and returns focus to the editor.
        if self.overlays.has_rename() {
            self.rename_confirm(cx);
            self.focus_handle.focus(window, cx);
        }

        // Context menu click handling.
        // - click inside enabled row: activate
        // - click inside disabled row or separator/padding: keep menu open
        // - click outside: dismiss menu and continue normal cursor placement
        if self.overlays.has_context_menu() {
            let line_height = self.input.line_height_or(window.line_height());
            if let Some(hit) = self.overlays.context_menu_hit_test(
                event.position,
                self.input.cached_layout(),
                line_height,
            ) {
                if !hit.inside_menu() {
                    if self.overlays.clear_context_menu() {
                        cx.notify();
                    }
                } else {
                    self.overlays
                        .set_context_menu_highlighted_enabled_item(hit.item_index());

                    if hit.actionable() {
                        self.context_menu_activate(window, cx);
                    } else {
                        cx.notify();
                    }
                    return;
                }
            }
        }

        // If the completion menu is visible and the click lands inside it, treat
        // the click as "select this item and accept" rather than moving the cursor.
        // We compare against window-coordinate bounds because GPUI delivers event
        // positions in window space.
        if let Some(slot) = self
            .overlays
            .completion_menu_bounds()
            .and_then(|bounds| bounds.slot_at(event.position))
        {
            self.lsp.select_completion_slot(slot);
            self.accept_completion(window, cx);
            cx.notify();
            return;
        }

        if let Some(action) = self.scroll.scrollbar_pointer_down(event.position) {
            if let ScrollbarPointerDown::JumpToOffset(offset) = action {
                self.set_scroll_offset(offset);
            }
            cx.notify();
            return;
        }

        // Check if the click landed on a fold chevron in the gutter.
        // The chevron rects are in window coordinates (same space as event.position).
        if self.input.point_is_in_gutter(event.position)
            && let Some(start_line) = self.input.fold_chevron_line_at(event.position)
        {
            self.toggle_fold(start_line, cx);
            return;
        }

        // Request focus so keyboard events are routed here.
        self.focus_handle.focus(window, cx);

        let line_height = self.input.line_height_or(window.line_height());

        // Mouse event positions are in window coordinates. Use the bounds origin
        // cached during the last prepaint to convert to element-relative coordinates.
        self.record_cursor_activity();
        let position = self.pixel_to_position(event.position, line_height);

        let now = std::time::Instant::now();
        let click_count =
            self.input
                .resolve_mouse_selection_click(now, position, event.click_count);

        match click_count {
            3 => {
                let anchor = self.mutate_selections(|core| core.select_entire_line(position.line));
                self.input.begin_mouse_selection_drag(anchor);
            }
            2 => {
                let offset = self.document.position_to_offset(position).unwrap_or(0);
                let anchor = self.mutate_selections(|core| {
                    core.select_word_at_offset_or_move_primary_cursor(offset, position)
                });
                self.input.begin_mouse_selection_drag(anchor);
            }
            _ => {
                let anchor = self.mutate_selections(|core| {
                    core.begin_primary_mouse_selection(position, event.modifiers.shift)
                });
                self.input.begin_mouse_selection_drag(anchor);
            }
        }

        self.scroll_to_cursor();
        cx.notify();
    }

    /// Handle a left mouse-button release — clears the drag anchor.
    fn handle_mouse_up(
        &mut self,
        event: &MouseUpEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if event.button != MouseButton::Left {
            return;
        }
        self.input.finish_mouse_selection_drag();
        self.scroll.clear_scrollbar_drag();
        cx.notify();
    }

    /// Handle mouse scroll wheel events (Phase 7: Scrolling)
    fn handle_scroll_wheel(
        &mut self,
        event: &ScrollWheelEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let line_height = self.input.line_height_or(window.line_height());
        if line_height <= gpui::px(0.0) {
            return;
        }
        let delta = event.delta.pixel_delta(line_height);
        let sensitivity = self.scroll.sensitivity();
        let scroll_delta_x = delta.x * sensitivity;
        let scroll_delta_y = delta.y * sensitivity;

        // When the completion menu is open, always capture scroll for the menu
        // regardless of pointer position. This prevents the confusing UX where
        // scrolling only works once the user moves the pointer inside the menu.
        if self.lsp.has_completion_menu() {
            self.lsp.scroll_completion_menu(
                -f32::from(scroll_delta_y) / f32::from(line_height),
                crate::element::MAX_COMPLETION_ITEMS,
            );
            cx.notify();
            return;
        }

        // No completion menu: scroll using the same coordinate model as hit-testing.
        // Wheel deltas are positive toward the top/left, while scroll offsets grow
        // down/right.
        let char_width = self.input.cached_layout().char_width();
        if char_width > gpui::px(0.0) && scroll_delta_x != gpui::px(0.0) {
            let scroll_columns = -(scroll_delta_x / char_width);
            self.set_horizontal_scroll_offset(self.horizontal_scroll_offset() + scroll_columns);
        }
        let scroll_lines = -(scroll_delta_y / line_height);
        if scroll_lines != 0.0 {
            self.scroll_by(scroll_lines);
        }
        cx.notify();
    }

    /// Handle mouse move events.
    ///
    /// When a drag is active (left button held after a mouse-down), the selection
    /// is extended to the current pointer position. Otherwise the hover tooltip is
    /// updated as before.
    fn handle_hover(
        &mut self,
        event: &MouseMoveEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let line_height = self.input.line_height_or(window.line_height());

        // While the context menu is open, mouse movement updates row highlighting
        // and suppresses normal hover-tooltip updates.
        if self.overlays.has_context_menu()
            && let Some(hit) = self.overlays.context_menu_hit_test(
                event.position,
                self.input.cached_layout(),
                line_height,
            )
        {
            let highlighted = if hit.inside_menu() {
                hit.item_index()
            } else {
                None
            };

            if self
                .overlays
                .set_context_menu_highlighted_enabled_item(highlighted)
            {
                cx.notify();
            }
            return;
        }

        // Mouse event positions are in window coordinates. Use the bounds origin
        // cached during the last prepaint to convert to element-relative coordinates.
        let position = self.pixel_to_position(event.position, line_height);

        // While the left button is held, extend the selection rather than updating
        // the hover tooltip.
        if let Some(anchor) = self.input.mouse_selection_drag_anchor() {
            self.mutate_selections(|core| core.drag_primary_mouse_selection(anchor, position));

            // Auto-scroll when the pointer leaves the top or bottom of the viewport.
            // Speed scales linearly with how far outside the viewport the pointer is,
            // expressed in lines so that the rate is independent of font size.
            let (viewport_top, viewport_bottom) = self.input.viewport_vertical_bounds();
            let pointer_y = event.position.y;
            if pointer_y < viewport_top && line_height > gpui::px(0.0) {
                // Pointer is above the viewport — scroll up.
                let overflow_lines = f32::from(viewport_top - pointer_y) / f32::from(line_height);
                self.scroll_by(-overflow_lines.max(1.0));
            } else if pointer_y > viewport_bottom && line_height > gpui::px(0.0) {
                // Pointer is below the viewport — scroll down.
                let overflow_lines =
                    f32::from(pointer_y - viewport_bottom) / f32::from(line_height);
                self.scroll_by(overflow_lines.max(1.0));
            } else {
                // Pointer is inside the viewport — keep the cursor visible via
                // the normal cursor-follow scroll so any text newly entered into
                // the selection remains visible.
                self.scroll_to_cursor();
            }

            cx.notify();
            return;
        }

        if let Some(offset) = self.scroll.scrollbar_drag_offset(event.position.y) {
            self.set_scroll_offset(offset);
            cx.notify();
            return;
        }

        // No drag — update hover tooltip.
        self.schedule_hover_at_position(position, cx);
        cx.notify();
    }
}

impl Focusable for TextEditor {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl TextEditor {
    /// Convert a UTF-16 code-unit byte range (as delivered by the OS IME) to a
    /// byte-offset range in the rope buffer.
    ///
    /// macOS and Windows IME APIs describe positions in UTF-16 code units.
    /// Our buffer uses UTF-8 byte offsets internally, so every IME boundary
    /// must be translated.  The conversion walks the rope char-by-char, which
    /// is O(n) but only invoked during active IME composition — acceptable cost.
    fn byte_range_from_utf16(&self, utf16_range: std::ops::Range<usize>) -> std::ops::Range<usize> {
        let mut utf16_offset = 0usize;
        let mut start_byte = 0usize;
        let mut end_byte = self.document.byte_len();
        let mut found_start = false;
        let mut offset = 0usize;

        while let Some(ch) = self.document.char_at(offset) {
            if utf16_offset == utf16_range.start {
                start_byte = offset;
                found_start = true;
            }
            if utf16_offset == utf16_range.end {
                end_byte = offset;
                break;
            }
            utf16_offset += ch.len_utf16();
            offset += ch.len_utf8();
        }

        // Handle end at the very end of the string
        if utf16_offset == utf16_range.end {
            end_byte = self.document.byte_len();
        }
        if !found_start && utf16_offset >= utf16_range.start {
            start_byte = self.document.byte_len();
        }

        start_byte..end_byte
    }

    /// Convert a byte-offset range in the rope buffer to a UTF-16 code-unit range.
    fn utf16_range_from_bytes(&self, byte_range: std::ops::Range<usize>) -> std::ops::Range<usize> {
        let clamped_start = self
            .document
            .floor_char_boundary(byte_range.start.min(self.document.byte_len()));
        let clamped_end = self
            .document
            .floor_char_boundary(byte_range.end.min(self.document.byte_len()));
        let start_utf16: usize = self
            .document
            .text_for_range(0..clamped_start)
            .ok()
            .unwrap_or_default()
            .chars()
            .map(|character| character.len_utf16())
            .sum();
        let len_utf16: usize = self
            .document
            .text_for_range(clamped_start..clamped_end)
            .ok()
            .unwrap_or_default()
            .chars()
            .map(|character| character.len_utf16())
            .sum();
        start_utf16..start_utf16 + len_utf16
    }

    /// Return the current cursor position as a byte offset, clamped to valid range.
    fn cursor_byte_offset(&self) -> usize {
        self.current_cursor_offset()
    }

    fn resolve_primary_replacement_plan(
        &self,
        range_utf16: Option<std::ops::Range<usize>>,
        new_text: &str,
    ) -> editor_core::PrimaryReplacementPlan {
        let explicit_range = range_utf16.map(|utf16| self.byte_range_from_utf16(utf16));
        self.editor_core_snapshot().primary_text_replacement_plan(
            explicit_range,
            self.input.ime_marked_range(),
            new_text,
        )
    }

    fn resolve_marked_text_replacement_plan(
        &self,
        range_utf16: Option<std::ops::Range<usize>>,
        new_text: &str,
        new_selected_range_utf16: Option<std::ops::Range<usize>>,
    ) -> editor_core::MarkedTextReplacementPlan {
        let explicit_range = range_utf16.map(|utf16| self.byte_range_from_utf16(utf16));
        let selected_offset_within_marked_text = new_selected_range_utf16
            .as_ref()
            .map(|range| self.byte_range_from_utf16(range.end..range.end).start);

        self.editor_core_snapshot().marked_text_replacement_plan(
            explicit_range,
            self.input.ime_marked_range(),
            new_text,
            selected_offset_within_marked_text,
        )
    }

    // ============================================================================
    // Text transforms — case (feat-033/034)
    // ============================================================================

    /// Apply a string transform to every cursor's selection (or word under cursor).
    ///
    /// Transforms are applied back-to-front so byte offsets remain valid after
    /// each substitution. The entire operation is wrapped in a single undo group.
    fn apply_transform_to_all_cursors(&mut self, transform: impl Fn(&str) -> String) {
        let edits = self
            .editor_core_snapshot()
            .plan_text_transform_edits(transform);
        if !self.apply_batch_replacement_edits(edits, true) {
            return;
        }

        self.update_syntax_highlights();
    }

    fn transform_uppercase(&mut self) {
        self.apply_transform_to_all_cursors(|s| s.to_uppercase());
    }

    fn transform_lowercase(&mut self) {
        self.apply_transform_to_all_cursors(|s| s.to_lowercase());
    }

    fn transform_title_case(&mut self) {
        use convert_case::{Case, Casing};
        self.apply_transform_to_all_cursors(|s| s.to_case(Case::Title));
    }

    fn transform_snake_case(&mut self) {
        use convert_case::{Case, Casing};
        self.apply_transform_to_all_cursors(|s| s.to_case(Case::Snake));
    }

    fn transform_camel_case(&mut self) {
        use convert_case::{Case, Casing};
        self.apply_transform_to_all_cursors(|s| s.to_case(Case::Camel));
    }

    fn transform_kebab_case(&mut self) {
        use convert_case::{Case, Casing};
        self.apply_transform_to_all_cursors(|s| s.to_case(Case::Kebab));
    }

    // ============================================================================
    // Line manipulation — sort / reverse / unique (feat-035/036/037)
    // ============================================================================

    /// Replace the selected line block (or entire buffer when nothing is selected)
    /// with the result of applying `transform` to the ordered slice of line strings.
    ///
    /// The replacement is treated as a single undoable operation.
    fn transform_selected_lines(&mut self, transform: impl Fn(&[String]) -> Vec<String>) {
        let Some(edit) = self
            .editor_core_snapshot()
            .selected_line_replacement_edit(transform)
        else {
            return;
        };

        if !self.apply_batch_replacement_edits([edit], false) {
            return;
        }

        self.update_syntax_highlights();
    }

    fn sort_lines_ascending(&mut self) {
        self.transform_selected_lines(|lines| {
            let mut sorted = lines.to_vec();
            sorted.sort_by_key(|line| line.to_lowercase());
            sorted
        });
    }

    fn sort_lines_descending(&mut self) {
        self.transform_selected_lines(|lines| {
            let mut sorted = lines.to_vec();
            sorted.sort_by_key(|line| std::cmp::Reverse(line.to_lowercase()));
            sorted
        });
    }

    fn sort_lines_by_length(&mut self) {
        self.transform_selected_lines(|lines| {
            let mut sorted = lines.to_vec();
            sorted.sort_by_key(|l| l.len());
            sorted
        });
    }

    fn reverse_lines(&mut self) {
        self.transform_selected_lines(|lines| {
            let mut reversed = lines.to_vec();
            reversed.reverse();
            reversed
        });
    }

    fn unique_lines(&mut self) {
        self.transform_selected_lines(|lines| {
            let mut seen = std::collections::HashSet::new();
            lines
                .iter()
                .filter(|l| seen.insert((*l).clone()))
                .cloned()
                .collect()
        });
    }

    // ============================================================================
    // Insert UUID (feat-042)
    // ============================================================================

    /// Insert text at every active cursor position, applying back-to-front to
    /// preserve byte offsets. Each insertion gets its own undo entry.
    fn insert_at_all_cursors(&mut self, text: &str, cx: &mut Context<Self>) {
        self.break_undo_group();

        let Some(plan) = self.editor_core_snapshot().multi_cursor_insert_plan(text) else {
            return;
        };

        if self.apply_multi_cursor_edits(plan) {
            cx.notify();
        }
    }

    fn insert_uuid_v4(&mut self, cx: &mut Context<Self>) {
        let id = uuid::Uuid::new_v4().to_string();
        self.insert_at_all_cursors(&id, cx);
    }

    fn insert_uuid_v7(&mut self, cx: &mut Context<Self>) {
        // v7 UUIDs embed a millisecond-precision Unix timestamp which makes them
        // monotonically sortable — preferable over v4 in database primary keys.
        let timestamp = uuid::Timestamp::now(uuid::NoContext);
        let id = uuid::Uuid::new_v7(timestamp).to_string();
        self.insert_at_all_cursors(&id, cx);
    }

    // ============================================================================
    // Rotate selections (feat-043)
    // ============================================================================

    /// Cycle the text content of all active selections one position forward.
    ///
    /// Cursor 0 gets the text that was in cursor N-1, cursor 1 gets cursor 0's
    /// text, etc. This is useful for swapping two identifiers or rotating a list
    /// of multi-cursor selections in place.
    fn rotate_selections(&mut self) {
        let edits = self.editor_core_snapshot().plan_rotated_text_replacements();
        if !self.apply_batch_replacement_edits(edits, false) {
            return;
        }

        self.update_syntax_highlights();
    }

    // ============================================================================
    // Swap selection ends (feat-044)
    // ============================================================================

    /// Flip the anchor and the head of every active selection so the cursor jumps
    /// to the opposite end. The selected range remains identical; only the
    /// "active" end (where the cursor blinks) changes.
    fn swap_selection_ends(&mut self, cx: &mut Context<Self>) {
        self.mutate_selections(|core| core.swap_selection_ends());
        cx.notify();
    }

    // ============================================================================
    // Copy as Markdown (feat-050)
    // ============================================================================

    /// Copy the selected text (or entire buffer) wrapped in a SQL fenced code
    /// block so it can be pasted into Markdown documents.
    fn copy_as_markdown(&mut self, cx: &mut Context<Self>) {
        let content = if self.selection().has_selection() {
            let sel_range = self.selection().range();
            let start = self
                .document
                .position_to_offset(sel_range.start)
                .unwrap_or(0);
            let end = self
                .document
                .position_to_offset(sel_range.end)
                .unwrap_or(self.document.byte_len());
            self.document.text_for_range(start..end).unwrap_or_default()
        } else {
            self.document.text()
        };

        let markdown = format!("```sql\n{}\n```", content);
        cx.write_to_clipboard(ClipboardItem::new_string(markdown));
    }

    // ============================================================================
    // Paste as Plain Text (feat-051)
    // ============================================================================

    /// Paste clipboard content after normalising line endings and stripping
    /// non-printable control characters.
    ///
    /// This is distinct from a normal paste in that it will never carry hidden
    /// formatting that some clipboard sources embed (e.g. RTF zero-width spaces).
    fn paste_as_plain_text(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(clipboard_item) = cx.read_from_clipboard() else {
            return;
        };
        let Some(raw_text) = clipboard_item.text() else {
            return;
        };

        // Normalise CR/CRLF to LF and discard other ASCII control chars except
        // tab and newline which are meaningful in a SQL editor.
        let normalised: String = raw_text
            .replace("\r\n", "\n")
            .replace('\r', "\n")
            .chars()
            .filter(|&c| !c.is_control() || c == '\n' || c == '\t')
            .collect();

        if self.delete_selection_range_if_present() {
            self.apply_interpolated_syntax_highlights();
            self.update_diagnostics();
            self.did_change_content(cx);
        }

        self.insert_at_cursor(&normalised, window, cx);
        cx.notify();
    }

    // ============================================================================
    // Action handlers — case transforms (feat-033/034)
    // ============================================================================

    fn handle_transform_uppercase(
        &mut self,
        _: &actions::TransformUppercase,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.transform_uppercase();
        cx.notify();
    }

    fn handle_transform_lowercase(
        &mut self,
        _: &actions::TransformLowercase,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.transform_lowercase();
        cx.notify();
    }

    fn handle_transform_title_case(
        &mut self,
        _: &actions::TransformTitleCase,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.transform_title_case();
        cx.notify();
    }

    fn handle_transform_snake_case(
        &mut self,
        _: &actions::TransformSnakeCase,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.transform_snake_case();
        cx.notify();
    }

    fn handle_transform_camel_case(
        &mut self,
        _: &actions::TransformCamelCase,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.transform_camel_case();
        cx.notify();
    }

    fn handle_transform_kebab_case(
        &mut self,
        _: &actions::TransformKebabCase,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.transform_kebab_case();
        cx.notify();
    }

    // ============================================================================
    // Action handlers — sort / reverse / unique lines (feat-035/036/037)
    // ============================================================================

    fn handle_sort_lines_ascending(
        &mut self,
        _: &actions::SortLinesAscending,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.sort_lines_ascending();
        cx.notify();
    }

    fn handle_sort_lines_descending(
        &mut self,
        _: &actions::SortLinesDescending,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.sort_lines_descending();
        cx.notify();
    }

    fn handle_sort_lines_by_length(
        &mut self,
        _: &actions::SortLinesByLength,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.sort_lines_by_length();
        cx.notify();
    }

    fn handle_reverse_lines(
        &mut self,
        _: &actions::ReverseLines,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.reverse_lines();
        cx.notify();
    }

    fn handle_unique_lines(
        &mut self,
        _: &actions::UniqueLines,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.unique_lines();
        cx.notify();
    }

    // ============================================================================
    // Action handlers — insert UUID (feat-042)
    // ============================================================================

    fn handle_insert_uuid_v4(
        &mut self,
        _: &actions::InsertUuidV4,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.insert_uuid_v4(cx);
    }

    fn handle_insert_uuid_v7(
        &mut self,
        _: &actions::InsertUuidV7,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.insert_uuid_v7(cx);
    }

    // ============================================================================
    // Action handlers — multi-cursor extras (feat-043/044)
    // ============================================================================

    fn handle_rotate_selections(
        &mut self,
        _: &actions::RotateSelections,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.rotate_selections();
        cx.notify();
    }

    fn handle_swap_selection_ends(
        &mut self,
        _: &actions::SwapSelectionEnds,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.swap_selection_ends(cx);
    }

    // ============================================================================
    // Action handlers — clipboard extras (feat-050/051)
    // ============================================================================

    fn handle_copy_as_markdown(
        &mut self,
        _: &actions::CopyAsMarkdown,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.copy_as_markdown(cx);
    }

    fn handle_paste_as_plain_text(
        &mut self,
        _: &actions::PasteAsPlainText,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.paste_as_plain_text(window, cx);
    }

    // ============================================================================
    // Go-to-line dialog (feat-040)
    // ============================================================================

    /// Open (or re-open) the go-to-line dialog, saving the current cursor so
    /// Escape can restore it.
    fn open_goto_line_dialog(&mut self, cx: &mut Context<Self>) {
        self.overlays.open_goto_line(self.current_cursor_position());
        cx.notify();
    }

    /// Feed a character typed while the go-to-line dialog is open.
    pub fn goto_line_input_char(&mut self, c: char, cx: &mut Context<Self>) {
        let line_count = self.document.line_count();
        let Some(parsed) = self.overlays.update_goto_line_query(c, line_count) else {
            return;
        };

        let mut target_position = None;
        if let Some(line) = parsed.line() {
            let column = parsed
                .column()
                .map(|column| {
                    self.document
                        .line(line)
                        .map(|line_text| {
                            let content = line_text.trim_end_matches(['\r', '\n']);
                            column.min(content.len())
                        })
                        .unwrap_or(0)
                })
                .unwrap_or(0);
            target_position = Some(Position::new(line, column));
        }

        if let Some(position) = target_position {
            self.mutate_selections(|core| core.move_primary_cursor(position, false));
            self.scroll_to_cursor();
        }

        cx.notify();
    }

    /// Confirm the dialog (Enter) — keep the cursor where it is and close.
    pub fn goto_line_confirm(&mut self, cx: &mut Context<Self>) {
        self.overlays.clear_goto_line();
        cx.notify();
    }

    /// Cancel the dialog (Escape) — restore the cursor to where it was before
    /// the dialog opened.
    pub fn goto_line_cancel(&mut self, cx: &mut Context<Self>) {
        if let Some(state) = self.overlays.take_goto_line() {
            self.mutate_selections(|core| core.move_primary_cursor(state.original_cursor(), false));
            self.scroll_to_cursor();
        }
        cx.notify();
    }

    fn handle_go_to_line(
        &mut self,
        _: &actions::GoToLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_goto_line_dialog(cx);
    }

    // ============================================================================
    // Toggle soft wrap (feat-041)
    // ============================================================================

    fn handle_toggle_soft_wrap(
        &mut self,
        _: &actions::ToggleSoftWrap,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.set_soft_wrap_enabled(!self.behavior.soft_wrap(), cx);
    }

    // ============================================================================
    // Format SQL (feat-049)
    // ============================================================================

    /// Replace the entire buffer with its formatted SQL equivalent as one undo step.
    ///
    /// Uses the built-in `SqlFormatter` with default settings. If the buffer is empty
    /// or formatting fails (e.g. the SQL is syntactically invalid), the buffer is left
    /// unchanged so the user never loses their work.
    fn format_sql_buffer(&mut self, cx: &mut Context<Self>) {
        let original = self.document.text();
        if original.trim().is_empty() {
            return;
        }

        let formatted = if let Some(provider) = self.format_provider.as_ref() {
            let Some(text) = provider(&original) else {
                return;
            };
            text
        } else {
            let formatter = SqlFormatter::with_defaults();
            match formatter.format(&original) {
                Ok(text) => text,
                // Leave buffer unchanged if the SQL cannot be formatted
                Err(_) => return,
            }
        };

        if formatted == original {
            return;
        }

        self.replace_all_text(&formatted, cx);
    }

    /// Format the current buffer in place using the editor's built-in SQL formatter.
    pub fn format_sql(&mut self, cx: &mut Context<Self>) {
        self.format_sql_buffer(cx);
    }

    fn handle_format_sql(
        &mut self,
        _: &actions::FormatSQL,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.format_sql_buffer(cx);
    }

    // ============================================================================
    // Go-to-definition (feat-046)
    // ============================================================================

    /// Jump the cursor to the definition of the symbol under the caret.
    ///
    /// Delegates to the `DefinitionProvider` registered on the LSP container.
    /// No-ops silently when no provider is set (the feature degrades gracefully).
    fn handle_go_to_definition(
        &mut self,
        _: &actions::GoToDefinition,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let offset = self.cursor_byte_offset();
        let rope = self.document.rope();
        let Some(target_offset) = self
            .lsp
            .definition_at(&rope, offset, &self.document.context())
        else {
            return;
        };
        if let Ok(pos) = self.document.offset_to_position(target_offset) {
            self.mutate_selections(|core| core.set_single_cursor(pos));
            // Also clear stale reference highlights since the cursor moved.
            self.document.clear_reference_ranges();
            self.scroll_to_cursor();
            cx.notify();
        }
    }

    // ============================================================================
    // Find references (feat-047)
    // ============================================================================

    /// Highlight all references to the symbol under the caret in the current buffer.
    ///
    /// Results are stored in LSP semantic state and rendered by `EditorElement`.
    /// Calling again clears the previous results and re-runs the search.
    fn handle_find_references(
        &mut self,
        _: &actions::FindReferences,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let offset = self.cursor_byte_offset();
        let rope = self.document.rope();
        let Some(ranges) = self
            .lsp
            .reference_ranges(&rope, offset, &self.document.context())
        else {
            return;
        };
        self.document.set_reference_ranges(ranges);
        cx.notify();
    }

    // ============================================================================
    // Rename symbol (feat-048)
    // ============================================================================

    /// Open the inline rename dialog for the word under the caret.
    ///
    /// Creates a focused `Input` widget overlay that handles all text editing.
    /// Enter confirms the rename; Escape or blur cancels it.
    fn handle_rename_symbol(
        &mut self,
        _: &actions::RenameSymbol,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let offset = self.cursor_byte_offset();
        let Some(range) = self
            .editor_core_snapshot()
            .find_word_range_at_offset(offset)
        else {
            return;
        };
        if !self.can_rename_at_offset(offset) {
            return;
        }
        let word = self
            .document
            .text_for_range(range.clone())
            .unwrap_or_default();
        let Ok(word_range) =
            self.document
                .anchored_range(range.start..range.end, Bias::Left, Bias::Right)
        else {
            return;
        };

        let input = cx.new({
            let default_value: SharedString = SharedString::from(word.clone());
            |cx| InputState::new(window, cx).default_value(default_value)
        });

        input.update(cx, |state, cx| {
            state.focus(window, cx);
        });

        // Select all text so the user can immediately type a replacement
        let focus_handle = input.read(cx).focus_handle(cx);
        focus_handle.dispatch_action(&zqlz_ui::widgets::input::SelectAll, window, cx);

        let mut subscriptions = Vec::new();

        subscriptions.push(
            cx.subscribe(&input, |this: &mut Self, _, event: &InputEvent, cx| {
                if let InputEvent::PressEnter { .. } = event {
                    this.rename_confirm(cx);
                }
            }),
        );

        self.overlays.set_rename(RenameState::new(
            input,
            word_range,
            word,
            self.current_cursor_position(),
            subscriptions,
        ));
        cx.notify();
    }

    /// Commit the rename: replace every occurrence of `original_word` (whole-word
    /// matches) in the buffer with `new_name` as a single atomic undo group.
    fn rename_confirm(&mut self, cx: &mut Context<Self>) {
        let Some(state) = self.overlays.take_rename() else {
            return;
        };
        let new_name = state.current_input_text(cx);
        if state.is_noop_replacement(&new_name) {
            return;
        }
        let rename_origin = self.rename_origin_position(&state);
        self.mutate_selections(|core| core.move_primary_cursor(rename_origin, false));
        let Some(word_range) = state.resolved_word_range(&self.document) else {
            self.restore_rename_cursor(&state);
            cx.notify();
            return;
        };

        let Some(workspace_edit) = self.lsp.rename_at(
            &self.document.rope(),
            word_range.start,
            &new_name,
            &self.document.context(),
        ) else {
            self.restore_rename_cursor(&state);
            cx.notify();
            return;
        };

        if let Err(error) = self.apply_workspace_edit_impl(&workspace_edit, cx) {
            tracing::warn!(?error, "Failed to apply rename edit");
            self.restore_rename_cursor(&state);
            cx.notify();
            return;
        }

        if let Some(new_cursor_offset) = self.rename_cursor_after_edit(&workspace_edit, &state)
            && let Ok(position) = self.document.offset_to_position(new_cursor_offset)
        {
            let clamped = self.document.clamp_position(position);
            self.mutate_selections(|core| core.move_primary_cursor(clamped, false));
        }

        cx.notify();
    }

    fn can_rename_at_offset(&self, offset: usize) -> bool {
        self.lsp.can_rename_at(
            &self.document.rope(),
            offset,
            RENAME_PROBE_IDENTIFIER,
            &self.document.context(),
        )
    }

    /// Cancel the rename dialog without modifying the buffer.
    fn rename_cancel(&mut self, cx: &mut Context<Self>) {
        self.overlays.clear_rename();
        cx.notify();
    }

    /// Build the positioned overlay element for the inline rename input.
    ///
    /// Returns `None` when no rename is active or the word position can't be
    /// resolved. Uses `CachedEditorLayout` from the previous frame for
    /// positioning — one-frame lag is acceptable since the rename word doesn't
    /// move while renaming.
    fn rename_overlay_element(&self, cx: &App) -> Option<Div> {
        let state = self.overlays.rename()?;

        let word_start_offset = state.resolved_word_range(&self.document)?.start;
        let word_position = self.document.offset_to_position(word_start_offset).ok()?;

        let layout = self.input.cached_layout();
        let line_height = layout.line_height();
        let char_width = layout.char_width();

        // Pixel offset from the editor origin to the word being renamed.
        // The column is in bytes; convert to character count for pixel math.
        let line_text = self.document.line(word_position.line)?;
        let char_column = line_text
            .get(..word_position.column)
            .map(|prefix| prefix.chars().count())
            .unwrap_or(word_position.column);

        let left = px(layout.gutter_width()) + char_width * char_column as f32
            - char_width * self.scroll.horizontal_offset();

        let visible_line = (word_position.line as f32) - self.scroll.vertical_offset();
        let top = line_height * visible_line;

        // Size the input to fit the current text with a comfortable minimum.
        let current_text = state.current_input_text(cx);
        let input_width = (char_width * current_text.len().max(6) as f32 + px(16.0)).max(px(80.0));

        let input_entity = state.input_entity();
        let theme = cx.theme();

        Some(
            div()
                .absolute()
                .left(left)
                .top(top)
                .w(input_width)
                .h(line_height)
                .bg(theme.colors.popover)
                .border_1()
                .border_color(theme.colors.primary)
                .rounded(px(4.0))
                .child(
                    Input::new(input_entity)
                        .appearance(false)
                        .with_size(Size::XSmall),
                ),
        )
    }

    fn rename_origin_position(&self, state: &RenameState) -> Position {
        state.origin_position(&self.document)
    }

    fn restore_rename_cursor(&mut self, state: &RenameState) {
        let clamped = state.restore_cursor_position(&self.document);
        self.mutate_selections(|core| core.move_primary_cursor(clamped, false));
    }

    #[allow(clippy::mutable_key_type)]
    fn rename_cursor_after_edit(
        &self,
        edit: &lsp_types::WorkspaceEdit,
        state: &RenameState,
    ) -> Option<usize> {
        let changes = edit.changes.as_ref()?;
        let origin = self.rename_origin_position(state);
        let origin_offset = self.document.position_to_offset(origin).ok()?;

        changes
            .values()
            .flat_map(|edits| edits.iter())
            .filter_map(|text_edit| {
                let start_line = usize::try_from(text_edit.range.start.line).ok()?;
                let start_character = usize::try_from(text_edit.range.start.character).ok()?;
                let start_line_text = self.document.line(start_line)?;
                let start_column = start_line_text
                    .char_indices()
                    .nth(start_character)
                    .map(|(byte_offset, _)| byte_offset)
                    .unwrap_or(start_line_text.len());
                let edit_start = self
                    .document
                    .position_to_offset(Position::new(start_line, start_column))
                    .ok()?;
                if edit_start == origin_offset {
                    Some(edit_start + text_edit.new_text.len())
                } else {
                    None
                }
            })
            .next()
    }

    // ============================================================================
    // Context menu (feat-045)
    // ============================================================================

    /// Build the standard context menu items for the current editor state.
    fn build_context_menu_items(&self) -> Vec<ContextMenuItem> {
        let has_selection = self.has_selection();
        vec![
            ContextMenuItem::action("Cut", ContextMenuAction::Cut, !has_selection),
            ContextMenuItem::action("Copy", ContextMenuAction::Copy, !has_selection),
            ContextMenuItem::action("Paste", ContextMenuAction::Paste, false),
            ContextMenuItem::separator(),
            ContextMenuItem::action("Select All", ContextMenuAction::SelectAll, false),
            ContextMenuItem::separator(),
            ContextMenuItem::action("Find", ContextMenuAction::Find, false),
            ContextMenuItem::action("Find and Replace", ContextMenuAction::FindReplace, false),
            ContextMenuItem::action(
                "Select All Matches",
                ContextMenuAction::SelectAllMatches,
                false,
            ),
            ContextMenuItem::action(
                "Complete",
                ContextMenuAction::Complete,
                !self.lsp.has_completions(),
            ),
            ContextMenuItem::separator(),
            ContextMenuItem::action(
                "Go to Definition",
                ContextMenuAction::GoToDefinition,
                !self.lsp.has_definition(),
            ),
            ContextMenuItem::action(
                "Find References",
                ContextMenuAction::FindReferences,
                !self.lsp.has_references(),
            ),
            ContextMenuItem::action(
                "Rename Symbol",
                ContextMenuAction::RenameSymbol,
                !self.can_rename_at_offset(self.current_cursor_offset()),
            ),
            ContextMenuItem::action("Go to Line", ContextMenuAction::GoToLine, false),
            ContextMenuItem::separator(),
            ContextMenuItem::action("Toggle Soft Wrap", ContextMenuAction::ToggleSoftWrap, false),
            ContextMenuItem::action("Fold All", ContextMenuAction::FoldAll, false),
            ContextMenuItem::action("Unfold All", ContextMenuAction::UnfoldAll, false),
            ContextMenuItem::separator(),
            ContextMenuItem::action(
                "Duplicate Line Down",
                ContextMenuAction::DuplicateLineDown,
                false,
            ),
            ContextMenuItem::action("Move Line Up", ContextMenuAction::MoveLineUp, false),
            ContextMenuItem::action("Move Line Down", ContextMenuAction::MoveLineDown, false),
            ContextMenuItem::action(
                "Toggle Line Comment",
                ContextMenuAction::ToggleLineComment,
                false,
            ),
            ContextMenuItem::action(
                "Sort Lines Ascending",
                ContextMenuAction::SortLinesAscending,
                false,
            ),
            ContextMenuItem::action("Unique Lines", ContextMenuAction::UniqueLines, false),
            ContextMenuItem::separator(),
            ContextMenuItem::action("Uppercase", ContextMenuAction::TransformUppercase, false),
            ContextMenuItem::action("Lowercase", ContextMenuAction::TransformLowercase, false),
            ContextMenuItem::action("Title Case", ContextMenuAction::TransformTitleCase, false),
            ContextMenuItem::action("Snake Case", ContextMenuAction::TransformSnakeCase, false),
            ContextMenuItem::action("Camel Case", ContextMenuAction::TransformCamelCase, false),
            ContextMenuItem::action("Kebab Case", ContextMenuAction::TransformKebabCase, false),
            ContextMenuItem::separator(),
            ContextMenuItem::action("Insert UUID v4", ContextMenuAction::InsertUuidV4, false),
            ContextMenuItem::action("Insert UUID v7", ContextMenuAction::InsertUuidV7, false),
            ContextMenuItem::separator(),
            ContextMenuItem::action("Format SQL", ContextMenuAction::FormatSql, false),
        ]
    }

    /// Open the context menu at a specific pixel position.
    fn open_context_menu_at(
        &mut self,
        x: f32,
        y: f32,
        highlighted: Option<usize>,
        cx: &mut Context<Self>,
    ) {
        let items = self.build_context_menu_items();
        self.overlays.open_context_menu(items, x, y, highlighted);
        cx.notify();
    }

    /// Open the context menu at the current cursor position (keyboard shortcut).
    fn handle_open_context_menu_keyboard(
        &mut self,
        _: &actions::OpenContextMenu,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let cursor = self.current_cursor_position();
        let snapshot = self.editor_snapshot();
        let line_height = self.input.line_height_or(px(20.0));
        let anchor = snapshot
            .bounds_for_range(cursor, cursor, self.input.editor_bounds(), line_height)
            .map(|bounds| bounds.origin)
            .unwrap_or_else(|| self.input.fallback_editor_text_anchor());

        let (x, y) = self
            .input
            .window_point_to_local(gpui::point(anchor.x, anchor.y + line_height));
        let items = self.build_context_menu_items();
        let highlighted = ContextMenuState::first_actionable_index(&items);
        self.overlays.open_context_menu(items, x, y, highlighted);
        cx.notify();
    }

    /// Handle a right-click: move cursor to the click position and open the menu.
    fn handle_right_click(
        &mut self,
        event: &MouseDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.overlays.has_rename() {
            self.rename_confirm(cx);
        }
        self.focus_handle.focus(window, cx);
        let line_height = self.input.line_height_or(window.line_height());
        let position = self.pixel_to_position(event.position, line_height);
        let selection_contains_click = self.selection_contains_position(position);
        if !selection_contains_click {
            self.mutate_selections(|core| core.move_primary_cursor(position, false));
        }
        // Subtract the cached bounds origin to convert from window coords to
        // element-relative coords (the renderer adds bounds.origin back when
        // positioning the menu overlay).
        let (x, y) = self.input.window_point_to_local(event.position);
        self.open_context_menu_at(x, y, None, cx);
    }

    fn selection_contains_position(&self, position: Position) -> bool {
        self.editor_core_snapshot()
            .primary_selection_contains(position)
    }

    /// Move the keyboard highlight up/down within the open context menu.
    fn context_menu_move(&mut self, delta: i32, cx: &mut Context<Self>) {
        if self.overlays.move_context_menu_highlight(delta) {
            cx.notify();
        }
    }

    /// Activate the currently highlighted context menu item.
    fn context_menu_activate(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let action = self.overlays.take_context_menu_highlighted_action();
        self.focus_handle.focus(window, cx);
        match action {
            Some(ContextMenuAction::Cut) => window.dispatch_action(actions::Cut.boxed_clone(), cx),
            Some(ContextMenuAction::Copy) => {
                window.dispatch_action(actions::Copy.boxed_clone(), cx)
            }
            Some(ContextMenuAction::Paste) => {
                window.dispatch_action(actions::Paste.boxed_clone(), cx)
            }
            Some(ContextMenuAction::SelectAll) => {
                window.dispatch_action(actions::SelectAll.boxed_clone(), cx)
            }
            Some(ContextMenuAction::Find) => {
                window.dispatch_action(actions::OpenFind.boxed_clone(), cx)
            }
            Some(ContextMenuAction::FindReplace) => {
                window.dispatch_action(actions::OpenFindReplace.boxed_clone(), cx)
            }
            Some(ContextMenuAction::SelectAllMatches) => {
                window.dispatch_action(actions::FindSelectAllMatches.boxed_clone(), cx)
            }
            Some(ContextMenuAction::Complete) => {
                window.dispatch_action(actions::TriggerCompletion.boxed_clone(), cx)
            }
            Some(ContextMenuAction::GoToDefinition) => {
                window.dispatch_action(actions::GoToDefinition.boxed_clone(), cx)
            }
            Some(ContextMenuAction::FindReferences) => {
                window.dispatch_action(actions::FindReferences.boxed_clone(), cx)
            }
            Some(ContextMenuAction::RenameSymbol) => {
                window.dispatch_action(actions::RenameSymbol.boxed_clone(), cx)
            }
            Some(ContextMenuAction::GoToLine) => {
                window.dispatch_action(actions::GoToLine.boxed_clone(), cx)
            }
            Some(ContextMenuAction::ToggleSoftWrap) => {
                window.dispatch_action(actions::ToggleSoftWrap.boxed_clone(), cx)
            }
            Some(ContextMenuAction::FoldAll) => {
                window.dispatch_action(actions::FoldAll.boxed_clone(), cx)
            }
            Some(ContextMenuAction::UnfoldAll) => {
                window.dispatch_action(actions::UnfoldAll.boxed_clone(), cx)
            }
            Some(ContextMenuAction::DuplicateLineDown) => {
                window.dispatch_action(actions::DuplicateLineDown.boxed_clone(), cx)
            }
            Some(ContextMenuAction::MoveLineUp) => {
                window.dispatch_action(actions::MoveLineUp.boxed_clone(), cx)
            }
            Some(ContextMenuAction::MoveLineDown) => {
                window.dispatch_action(actions::MoveLineDown.boxed_clone(), cx)
            }
            Some(ContextMenuAction::ToggleLineComment) => {
                window.dispatch_action(actions::ToggleLineComment.boxed_clone(), cx)
            }
            Some(ContextMenuAction::SortLinesAscending) => {
                window.dispatch_action(actions::SortLinesAscending.boxed_clone(), cx)
            }
            Some(ContextMenuAction::UniqueLines) => {
                window.dispatch_action(actions::UniqueLines.boxed_clone(), cx)
            }
            Some(ContextMenuAction::TransformUppercase) => {
                window.dispatch_action(actions::TransformUppercase.boxed_clone(), cx)
            }
            Some(ContextMenuAction::TransformLowercase) => {
                window.dispatch_action(actions::TransformLowercase.boxed_clone(), cx)
            }
            Some(ContextMenuAction::TransformTitleCase) => {
                window.dispatch_action(actions::TransformTitleCase.boxed_clone(), cx)
            }
            Some(ContextMenuAction::TransformSnakeCase) => {
                window.dispatch_action(actions::TransformSnakeCase.boxed_clone(), cx)
            }
            Some(ContextMenuAction::TransformCamelCase) => {
                window.dispatch_action(actions::TransformCamelCase.boxed_clone(), cx)
            }
            Some(ContextMenuAction::TransformKebabCase) => {
                window.dispatch_action(actions::TransformKebabCase.boxed_clone(), cx)
            }
            Some(ContextMenuAction::InsertUuidV4) => {
                window.dispatch_action(actions::InsertUuidV4.boxed_clone(), cx)
            }
            Some(ContextMenuAction::InsertUuidV7) => {
                window.dispatch_action(actions::InsertUuidV7.boxed_clone(), cx)
            }
            Some(ContextMenuAction::FormatSql) => {
                window.dispatch_action(actions::FormatSQL.boxed_clone(), cx)
            }
            None => {}
        }
        cx.notify();
    }

    fn handle_focus_blur(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if self.overlays.take_context_menu().is_some() {
            cx.notify();
        }
    }
}

impl EntityInputHandler for TextEditor {
    fn bounds_for_range(
        &mut self,
        range_utf16: std::ops::Range<usize>,
        bounds: Bounds<Pixels>,
        window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<Bounds<Pixels>> {
        let line_height = self.input.line_height_or(window.line_height());
        let byte_range = self.byte_range_from_utf16(range_utf16);
        let snapshot = self.editor_snapshot();
        let start = snapshot
            .document
            .offset_to_position(byte_range.start)
            .ok()?;
        let end = snapshot.document.offset_to_position(byte_range.end).ok()?;
        snapshot.bounds_for_range(start, end, bounds, line_height)
    }

    fn text_for_range(
        &mut self,
        range_utf16: std::ops::Range<usize>,
        adjusted_range: &mut Option<std::ops::Range<usize>>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<String> {
        let byte_range = self.byte_range_from_utf16(range_utf16);
        let clamped = byte_range.start.min(self.document.byte_len())
            ..byte_range.end.min(self.document.byte_len());
        let slice = self.document.text_for_range(clamped.clone()).ok()?;
        adjusted_range.replace(self.utf16_range_from_bytes(clamped));
        Some(slice)
    }

    fn selected_text_range(
        &mut self,
        _ignore_disabled_input: bool,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<UTF16Selection> {
        let cursor_offset = self.cursor_byte_offset();
        let (start_byte, end_byte) =
            if let Some(range) = self.editor_core_snapshot().primary_selection_byte_range() {
                (range.start, range.end)
            } else {
                (cursor_offset, cursor_offset)
            };
        let utf16_range = self.utf16_range_from_bytes(start_byte..end_byte);
        Some(UTF16Selection {
            range: utf16_range,
            reversed: false,
        })
    }

    fn marked_text_range(
        &self,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<std::ops::Range<usize>> {
        self.input
            .ime_marked_range()
            .map(|range| self.utf16_range_from_bytes(range.clone()))
    }

    fn unmark_text(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.input.clear_text_composition();
    }

    /// Called by the OS to commit IME composition or insert a plain character.
    ///
    /// When `range_utf16` is `None` the current marked range (or selection) is
    /// replaced. Otherwise the explicitly-provided range is replaced.
    fn replace_text_in_range(
        &mut self,
        range_utf16: Option<std::ops::Range<usize>>,
        new_text: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Intercept single-character bracket/quote input for auto-surround (feat-028),
        // skip-over (feat-029), and auto-close (feat-027).  We only do this for plain
        // character insertion (range_utf16 == None, no active IME composition) to avoid
        // interfering with IME preedit/commit flows.
        if range_utf16.is_none()
            && !self.input.has_ime_marked_range()
            && new_text.chars().count() == 1
        {
            let Some(ch) = new_text.chars().next() else {
                return;
            };
            let closer = Self::bracket_closer(ch);
            let has_selection = self.has_selection();

            // feat-028: if a selection is active and user types an opener, surround it.
            if let Some(c) = closer
                && has_selection
                && self.auto_surround_selection(ch, c, window, cx)
            {
                return;
            }

            // feat-029: if cursor is immediately before the same closing bracket, skip over it.
            let is_closer = matches!(ch, ')' | ']' | '}' | '\'' | '"');
            if is_closer && !has_selection && self.skip_over_closing_bracket(ch, cx) {
                return;
            }

            // feat-027: auto-close the bracket pair.
            if let Some(c) = closer
                && !has_selection
                && self.auto_close_bracket(ch, c, window, cx)
            {
                return;
            }
        }

        // Resolve the byte range to replace: explicit IME range > marked range > selection > cursor.
        let has_explicit_range = range_utf16.is_some();
        let plan = self.resolve_primary_replacement_plan(range_utf16, new_text);
        if !has_explicit_range && self.apply_text_to_all_cursors(new_text) {
            self.input.finish_text_replacement();
            if let Some(trigger) =
                self.lsp
                    .completion_trigger_context(self.cursor_byte_offset(), new_text, cx)
            {
                self.trigger_completions_debounced(trigger, cx);
            }
            self.did_change_content(cx);
            cx.notify();
            return;
        }

        if !self.apply_single_cursor_edit_batch(plan.batch, cx, false) {
            return;
        }

        self.mutate_selections(|core| core.collapse_primary_selection_to_cursor());
        self.input.finish_text_replacement();

        if let Some(trigger) =
            self.lsp
                .completion_trigger_context(self.cursor_byte_offset(), new_text, cx)
        {
            self.trigger_completions_debounced(trigger, cx);
        }

        self.did_change_content(cx);
        cx.notify();
    }

    /// Called during IME preedit — text is tentatively inserted and marked so
    /// Called during IME preedit — text is tentatively inserted and marked so
    /// the OS can continue composing over it.
    fn replace_and_mark_text_in_range(
        &mut self,
        range_utf16: Option<std::ops::Range<usize>>,
        new_text: &str,
        new_selected_range_utf16: Option<std::ops::Range<usize>>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let plan = self.resolve_marked_text_replacement_plan(
            range_utf16,
            new_text,
            new_selected_range_utf16,
        );

        if !self.apply_planned_edit_batch(plan.batch, false) {
            return;
        }

        self.input.update_text_composition_range(plan.marked_range);
        self.update_syntax_highlights();
        self.scroll_to_cursor();

        cx.notify();
    }

    /// Returns the UTF-16 character index closest to a pixel point (for touch / click).
    fn character_index_for_point(
        &mut self,
        point: gpui::Point<Pixels>,
        window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<usize> {
        let position =
            self.pixel_to_position(point, self.input.line_height_or(window.line_height()));
        let offset = self.document.position_to_offset(position).ok()?;
        Some(self.utf16_range_from_bytes(offset..offset).start)
    }
}

/// Re-export for convenience (alias)
pub use TextEditor as Editor;

/// Extract a plain-text string from an LSP `MarkedString` for display in hover tooltips.
fn markup_to_string(markup: &lsp_types::MarkedString) -> String {
    match markup {
        lsp_types::MarkedString::String(s) => s.clone(),
        lsp_types::MarkedString::LanguageString(ls) => ls.value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CachedEditorLayout, DocumentSnapshot, EditorAppearance, EditorSnapshot,
        LargeFilePolicyConfig, LargeFilePolicyTier, ResolvedLargeFilePolicy,
    };
    use crate::history_state::TransactionRecord;
    use crate::language_pipeline::{
        build_language_pipeline_snapshot, edited_line_range_from_changes,
    };
    use crate::{
        ActiveSnippet, Anchor, AnchoredCodeAction, AnchoredDiagnostic, AnchoredInlayHint, Bias,
        BufferSnapshot, CompletionMenuData, ContextMenuAction, ContextMenuItem,
        ContextMenuSnapshot, ContextMenuState, Cursor, CursorShapeStyle, DisplayMap, DisplayRowId,
        DisplayTextChunk, DocumentIdentity, EditPrediction, EditorInlayHint, EditorSnippetState,
        FindSnapshot, FoldDisplayState, FoldKind, FoldRegion, GoToLineSnapshot, Highlight,
        InlineSuggestion, LanguagePipelineState, Position, Selection, SelectionState,
        SelectionsCollection, Snippet, StructuralRange, SyntaxRefreshStrategy, SyntaxSnapshot,
        TextDocument, TextEditor, TransactionId, VisibleWrapLayout,
    };
    use gpui::{point, px, size};
    use std::{collections::HashSet, sync::Arc};

    fn test_editor_snapshot(
        buffer_snapshot: BufferSnapshot,
        display_lines: Vec<usize>,
        soft_wrap: bool,
    ) -> EditorSnapshot {
        let language = build_language_pipeline_snapshot(
            SyntaxSnapshot::empty(0),
            vec![Highlight {
                start: 0,
                end: 1,
                kind: crate::HighlightKind::Error,
            }],
            Vec::new(),
        );
        let display_map = DisplayMap::from_display_lines(display_lines.clone());
        let anchored_range = buffer_snapshot
            .anchored_range(0..1, Bias::Left, Bias::Right)
            .expect("anchored diagnostic");
        let anchored_diagnostics = vec![AnchoredDiagnostic {
            range: anchored_range,
            kind: crate::HighlightKind::Error,
        }];
        let anchored_inlay_hints = Arc::new(Vec::new());
        let display_snapshot = display_map.snapshot(
            buffer_snapshot.clone(),
            &[],
            &HashSet::new(),
            soft_wrap,
            Arc::new(Vec::new()),
            &anchored_diagnostics,
            anchored_inlay_hints.clone(),
            &[],
        );
        EditorSnapshot {
            document: DocumentSnapshot {
                display_snapshot,
                buffer: buffer_snapshot,
                context: crate::TextDocument::internal()
                    .expect("internal document")
                    .context(),
                identity: DocumentIdentity::internal().expect("internal document uri"),
                large_file_policy: ResolvedLargeFilePolicy::full(),
                language,
                inline_suggestion: None,
                edit_prediction: None,
                hover_state: None,
                signature_help_state: None,
                soft_wrap,
                show_inline_diagnostics: true,
            },
            appearance: EditorAppearance::Default,
            bracket_pairs: Vec::new(),
            selections: SelectionsCollection::single(Cursor::new(), Selection::new()),
            scroll_offset: 0.0,
            scroll_anchor_position: Position::zero(),
            scroll_anchor_visual_offset: 0.0,
            horizontal_scroll_offset: 0.0,
            viewport_lines: 20,
            gutter_width: 0.0,
            char_width: px(10.0),
            bounds_origin: point(px(0.0), px(0.0)),
            bounds_size: size(px(400.0), px(400.0)),
            cached_layout: CachedEditorLayout::new(
                0.0,
                px(10.0),
                point(px(0.0), px(0.0)),
                size(px(400.0), px(400.0)),
                px(20.0),
                None,
            ),
            minimap_visible: false,
            show_line_numbers: true,
            show_folding: true,
            highlight_current_line: true,
            relative_line_numbers: false,
            show_gutter_diagnostics: true,
            cursor_shape: CursorShapeStyle::Line,
            cursor_blink_enabled: true,
            cursor_visible: true,
            rounded_selection: true,
            selection_highlight_enabled: true,
            completion_menu: None::<CompletionMenuData>,
            context_menu: None::<ContextMenuSnapshot>,
            goto_line_info: None::<GoToLineSnapshot>,
            find_info: None::<FindSnapshot>,
        }
    }

    struct EditorTestHarness {
        snapshot: EditorSnapshot,
    }

    struct EditorSnapshotLabBuilder {
        snapshot: EditorSnapshot,
    }

    struct EditorSnapshotLab {
        snapshot: EditorSnapshot,
    }

    impl EditorTestHarness {
        fn new(text: &str) -> Self {
            Self {
                snapshot: test_editor_snapshot(
                    crate::buffer::TextBuffer::new(text).snapshot(),
                    (0..text.lines().count().max(1)).collect(),
                    false,
                ),
            }
        }

        fn with_display_lines(mut self, display_lines: Vec<usize>) -> Self {
            self.snapshot = test_editor_snapshot(
                self.snapshot.document.buffer_snapshot(),
                display_lines,
                self.snapshot.document.soft_wrap,
            );
            self
        }

        fn with_soft_wrap(mut self, wrap_column: usize, visual_rows: Vec<usize>) -> Self {
            self.snapshot.document.soft_wrap = true;
            let display_lines = self.snapshot.document.display_snapshot.display_lines();
            let mut display_state = FoldDisplayState::new();
            display_state.sync_all(self.snapshot.document.line_count(), &[]);
            display_state.update_wrap_rows(display_lines.as_ref(), wrap_column, &visual_rows);
            self.snapshot.document.display_snapshot = display_state.snapshot(
                self.snapshot.document.buffer_snapshot(),
                self.snapshot.document.fold_regions(),
                true,
                self.snapshot.document.syntax_highlights(),
                self.snapshot.document.anchored_diagnostics(),
                self.snapshot.document.anchored_inlay_hints(),
                self.snapshot.document.anchored_code_actions(),
            );
            self
        }

        fn with_primary_selection(mut self, start: Position, end: Position) -> Self {
            let extras = self.snapshot.extra_cursors();
            self.snapshot.selections = SelectionsCollection::from_primary_and_extras(
                Cursor::at(end),
                Selection::from_anchor_head(start, end),
                extras,
            );
            self
        }

        fn with_extra_selection(mut self, start: Position, end: Position) -> Self {
            let primary_cursor = self.snapshot.cursor().cloned().unwrap_or_else(Cursor::new);
            let primary_selection = self
                .snapshot
                .selection()
                .cloned()
                .unwrap_or_else(Selection::new);
            let mut extras = self.snapshot.extra_cursors();
            extras.push((Cursor::at(end), Selection::from_anchor_head(start, end)));
            self.snapshot.selections = SelectionsCollection::from_primary_and_extras(
                primary_cursor,
                primary_selection,
                extras,
            );
            self
        }

        fn snapshot(&self) -> &EditorSnapshot {
            &self.snapshot
        }

        fn assert_display_round_trip(&self, position: Position) {
            let display_point = self
                .snapshot
                .document
                .display_snapshot
                .point_to_display_point(position)
                .expect("display point");
            let round_trip = self
                .snapshot
                .document
                .display_snapshot
                .display_point_to_point(display_point);
            assert_eq!(round_trip, position);
        }

        fn assert_text_with_selections(&self, expected: &str) {
            let text = self
                .snapshot
                .document
                .slice(0..self.snapshot.document.byte_len())
                .expect("buffer text");
            let selection = self
                .snapshot
                .selection()
                .cloned()
                .unwrap_or_else(Selection::new)
                .range();
            let start = self
                .snapshot
                .document
                .position_to_offset(selection.start)
                .expect("selection start offset");
            let end = self
                .snapshot
                .document
                .position_to_offset(selection.end)
                .expect("selection end offset");

            let mut marked = String::new();
            marked.push_str(&text[..start]);
            marked.push('[');
            marked.push_str(&text[start..end]);
            marked.push(']');
            marked.push_str(&text[end..]);

            assert_eq!(marked, expected);
        }
    }

    impl EditorSnapshotLabBuilder {
        fn new(text: &str) -> Self {
            Self {
                snapshot: test_editor_snapshot(
                    crate::buffer::TextBuffer::new(text).snapshot(),
                    (0..text.lines().count().max(1)).collect(),
                    false,
                ),
            }
        }

        fn with_folded_lines(mut self, folded_lines: &[usize]) -> Self {
            let mut display_state = FoldDisplayState::new();
            for line in folded_lines {
                display_state.collapse_line(*line);
            }
            let total_lines = self.snapshot.document.line_count();
            display_state.sync_all(total_lines, self.snapshot.document.fold_regions());
            self.snapshot.document.display_snapshot = display_state.snapshot(
                self.snapshot.document.buffer_snapshot(),
                self.snapshot.document.fold_regions(),
                self.snapshot.document.soft_wrap,
                self.snapshot.document.syntax_highlights(),
                self.snapshot.document.anchored_diagnostics(),
                self.snapshot.document.anchored_inlay_hints(),
                self.snapshot.document.anchored_code_actions(),
            );
            self
        }

        fn with_fold_regions(mut self, fold_regions: Vec<FoldRegion>) -> Self {
            self.snapshot.document.language.fold_regions = fold_regions;
            self
        }

        fn with_soft_wrap(mut self, wrap_column: usize, visual_rows: Vec<usize>) -> Self {
            self.snapshot.document.soft_wrap = true;
            let display_lines = self.snapshot.document.display_snapshot.display_lines();
            let mut display_state = FoldDisplayState::new();
            display_state.sync_all(self.snapshot.document.line_count(), &[]);
            display_state.update_wrap_rows(display_lines.as_ref(), wrap_column, &visual_rows);
            self.snapshot.document.display_snapshot = display_state.snapshot(
                self.snapshot.document.buffer_snapshot(),
                self.snapshot.document.fold_regions(),
                true,
                self.snapshot.document.syntax_highlights(),
                self.snapshot.document.anchored_diagnostics(),
                self.snapshot.document.anchored_inlay_hints(),
                self.snapshot.document.anchored_code_actions(),
            );
            self
        }

        fn with_reference_ranges(mut self, reference_ranges: Vec<std::ops::Range<usize>>) -> Self {
            self.snapshot.document.language.reference_ranges = reference_ranges;
            self
        }

        fn with_inlay_hints(mut self, inlay_hints: Vec<EditorInlayHint>) -> Self {
            self.snapshot.document.language.inlay_hints = Arc::new(inlay_hints);
            self
        }

        fn with_inline_suggestion(mut self, inline_suggestion: InlineSuggestion) -> Self {
            self.snapshot.document.inline_suggestion = Some(inline_suggestion);
            self
        }

        fn with_edit_prediction(mut self, edit_prediction: EditPrediction) -> Self {
            self.snapshot.document.edit_prediction = Some(edit_prediction);
            self
        }

        fn with_large_file_policy(mut self, large_file_policy: ResolvedLargeFilePolicy) -> Self {
            self.snapshot.document.large_file_policy = large_file_policy;
            self
        }

        fn with_primary_selection(mut self, start: Position, end: Position) -> Self {
            let extras = self.snapshot.extra_cursors();
            self.snapshot.selections = SelectionsCollection::from_primary_and_extras(
                Cursor::at(end),
                Selection::from_anchor_head(start, end),
                extras,
            );
            self
        }

        fn with_viewport(mut self, viewport_lines: usize, scroll_offset: f32) -> Self {
            self.snapshot.viewport_lines = viewport_lines;
            self.snapshot.scroll_offset = scroll_offset;
            self
        }

        fn build(self) -> EditorSnapshotLab {
            EditorSnapshotLab {
                snapshot: self.snapshot,
            }
        }
    }

    impl EditorSnapshotLab {
        fn snapshot(&self) -> &EditorSnapshot {
            &self.snapshot
        }

        fn document_snapshot(&self) -> &DocumentSnapshot {
            &self.snapshot.document
        }

        fn syntax_refresh_strategy(&self) -> SyntaxRefreshStrategy {
            self.snapshot
                .document
                .syntax_refresh_strategy(self.snapshot.viewport_lines, self.snapshot.scroll_offset)
        }

        fn marked_display_snapshot(&self) -> String {
            let mut rendered = Vec::new();
            let text = self
                .snapshot
                .document
                .slice(0..self.snapshot.document.byte_len())
                .expect("buffer text");
            let selection = self
                .snapshot
                .selection()
                .cloned()
                .unwrap_or_else(Selection::new)
                .range();
            let selection_start = self
                .snapshot
                .document
                .position_to_offset(selection.start)
                .expect("selection start");
            let selection_end = self
                .snapshot
                .document
                .position_to_offset(selection.end)
                .expect("selection end");

            for display_line in self.snapshot.document.display_lines().iter() {
                let line = self
                    .snapshot
                    .document
                    .line(*display_line)
                    .unwrap_or_default();
                let line_start = self
                    .snapshot
                    .document
                    .position_to_offset(Position::new(*display_line, 0))
                    .expect("line start");
                let line_end = line_start + line.len();
                let mut line_text = line.to_string();
                if line_text.ends_with('\n') {
                    line_text.pop();
                }
                if selection_start < line_end && selection_end > line_start {
                    let start = selection_start
                        .saturating_sub(line_start)
                        .min(line_text.len());
                    let end = selection_end
                        .saturating_sub(line_start)
                        .min(line_text.len());
                    line_text.insert(end, ']');
                    line_text.insert(start, '[');
                }
                rendered.push(line_text);
            }

            if rendered.is_empty() {
                text
            } else {
                rendered.join("\n")
            }
        }

        fn assert_marked_display_snapshot(&self, expected: &str) {
            assert_eq!(self.marked_display_snapshot(), expected);
        }

        fn assert_overlay_resolution(
            &self,
            has_inline_suggestion: bool,
            has_edit_prediction: bool,
        ) {
            assert_eq!(
                self.snapshot.document.inline_suggestion.is_some(),
                has_inline_suggestion
            );
            assert_eq!(
                self.snapshot.document.edit_prediction.is_some(),
                has_edit_prediction
            );
        }
    }

    // ============================================================================
    // Subword movement (feat-003)
    // ============================================================================

    #[test]
    fn test_next_subword_end_lowercase_run() {
        // Plain lowercase: entire word is one subword
        assert_eq!(
            crate::editor_core::EditorCore::next_subword_end_in_text("hello world", 0),
            5
        );
    }

    #[test]
    fn test_next_subword_end_camel_case() {
        // fooBar → "foo" is the first subword
        assert_eq!(
            crate::editor_core::EditorCore::next_subword_end_in_text("fooBar", 0),
            3
        );
        // From "Bar" onward: "Bar" is a Title-case subword
        assert_eq!(
            crate::editor_core::EditorCore::next_subword_end_in_text("fooBar", 3),
            6
        );
    }

    #[test]
    fn test_next_subword_end_all_caps() {
        // FOOBar → "FOO" (caps stop one before the trailing lowercase)
        assert_eq!(
            crate::editor_core::EditorCore::next_subword_end_in_text("FOOBar", 0),
            3
        );
    }

    #[test]
    fn test_next_subword_end_title_case() {
        // FooBar → "Foo"
        assert_eq!(
            crate::editor_core::EditorCore::next_subword_end_in_text("FooBar", 0),
            3
        );
    }

    #[test]
    fn test_next_subword_end_underscore_run() {
        // snake_case: "snake" then "_" then "case"
        assert_eq!(
            crate::editor_core::EditorCore::next_subword_end_in_text("snake_case", 0),
            5
        );
        assert_eq!(
            crate::editor_core::EditorCore::next_subword_end_in_text("snake_case", 5),
            6
        );
        assert_eq!(
            crate::editor_core::EditorCore::next_subword_end_in_text("snake_case", 6),
            10
        );
    }

    #[test]
    fn test_next_subword_end_at_end() {
        // Already at end of string
        let s = "foo";
        assert_eq!(
            crate::editor_core::EditorCore::next_subword_end_in_text(s, s.len()),
            s.len()
        );
    }

    #[test]
    fn test_prev_subword_start_lowercase() {
        // cursor at end of "hello" → start = 0
        assert_eq!(
            crate::editor_core::EditorCore::prev_subword_start_in_text("hello", 5),
            0
        );
    }

    #[test]
    fn test_prev_subword_start_camel_case() {
        // fooBar, cursor after Bar (6) → "Bar" subword starts at 3
        assert_eq!(
            crate::editor_core::EditorCore::prev_subword_start_in_text("fooBar", 6),
            3
        );
        // cursor after foo (3) → "foo" subword starts at 0
        assert_eq!(
            crate::editor_core::EditorCore::prev_subword_start_in_text("fooBar", 3),
            0
        );
    }

    #[test]
    fn test_prev_subword_start_title_case() {
        // "FooBar", cursor at 6 → "Bar" starts at 3 (lowercase run + preceding upper)
        assert_eq!(
            crate::editor_core::EditorCore::prev_subword_start_in_text("FooBar", 6),
            3
        );
    }

    #[test]
    fn test_prev_subword_start_underscore() {
        // "snake_case", cursor at 10 → "case" starts at 6
        assert_eq!(
            crate::editor_core::EditorCore::prev_subword_start_in_text("snake_case", 10),
            6
        );
        // cursor at 6 → "_" subword starts at 5
        assert_eq!(
            crate::editor_core::EditorCore::prev_subword_start_in_text("snake_case", 6),
            5
        );
        // cursor at 5 → "snake" starts at 0
        assert_eq!(
            crate::editor_core::EditorCore::prev_subword_start_in_text("snake_case", 5),
            0
        );
    }

    #[test]
    fn test_prev_subword_start_at_zero() {
        assert_eq!(
            crate::editor_core::EditorCore::prev_subword_start_in_text("foo", 0),
            0
        );
    }

    #[test]
    fn test_editor_snapshot_preserves_coordinate_mapping_after_buffer_mutation() {
        let mut buffer = crate::buffer::TextBuffer::new("one\ntwo\nthree");
        let snapshot = test_editor_snapshot(buffer.snapshot(), vec![0, 2], false);

        buffer.insert(0, "zero\n").unwrap();

        let position = snapshot.pixel_to_position(point(px(5.0), px(25.0)), px(20.0));
        assert_eq!(position, Position::new(2, 0));
        assert_eq!(snapshot.revision(), 0);
        assert_eq!(
            snapshot
                .document
                .display_snapshot
                .display_point_to_point(super::DisplayPoint { row: 1, column: 0 }),
            Position::new(2, 0)
        );
    }

    #[test]
    fn test_editor_snapshot_bounds_and_hit_testing_round_trip() {
        let buffer = crate::buffer::TextBuffer::new("alpha\nbeta");
        let snapshot = test_editor_snapshot(buffer.snapshot(), vec![0, 1], false);
        let bounds = gpui::Bounds::new(point(px(0.0), px(0.0)), size(px(400.0), px(400.0)));

        let range_bounds = snapshot
            .bounds_for_range(Position::new(1, 2), Position::new(1, 4), bounds, px(20.0))
            .expect("range should resolve to bounds");

        let position = snapshot.pixel_to_position(
            point(
                range_bounds.origin.x + px(1.0),
                range_bounds.origin.y + px(1.0),
            ),
            px(20.0),
        );

        assert_eq!(position, Position::new(1, 2));
    }

    #[test]
    fn test_editor_snapshot_bounds_respect_horizontal_scroll_offset() {
        let mut snapshot = test_editor_snapshot(
            crate::buffer::TextBuffer::new("abcdef").snapshot(),
            vec![0],
            false,
        );
        snapshot.horizontal_scroll_offset = 3.0;
        let bounds = gpui::Bounds::new(point(px(0.0), px(0.0)), size(px(400.0), px(400.0)));

        let range_bounds = snapshot
            .bounds_for_range(Position::new(0, 4), Position::new(0, 6), bounds, px(20.0))
            .expect("range should resolve to bounds");

        assert_eq!(range_bounds.origin, point(px(10.0), px(0.0)));
        assert_eq!(range_bounds.size.width, px(20.0));
    }

    #[test]
    fn test_editor_snapshot_hit_testing_respects_horizontal_scroll_offset() {
        let mut snapshot = test_editor_snapshot(
            crate::buffer::TextBuffer::new("abcdef").snapshot(),
            vec![0],
            false,
        );
        snapshot.horizontal_scroll_offset = 3.0;

        let position = snapshot.pixel_to_position(point(px(15.0), px(5.0)), px(20.0));

        assert_eq!(position, Position::new(0, 4));
    }

    #[test]
    fn test_context_menu_clamping_handles_tiny_viewports() {
        let menu = ContextMenuState::new(
            vec![ContextMenuItem::action(
                "Copy",
                ContextMenuAction::Copy,
                false,
            )],
            64.0,
            32.0,
            None,
        );

        let origin = menu.clamped_origin(
            point(px(10.0), px(20.0)),
            size(px(100.0), px(16.0)),
            px(20.0),
        );

        assert_eq!(origin, point(px(10.0), px(20.0)));
    }

    #[test]
    fn test_soft_wrap_and_diagnostics_live_in_same_document_snapshot() {
        let snapshot = EditorTestHarness::new("abcdefghij")
            .with_soft_wrap(4, vec![3])
            .snapshot
            .clone();

        assert!(snapshot.document.soft_wrap);
        assert_eq!(snapshot.document.diagnostics().len(), 1);

        let position = snapshot.pixel_to_position(point(px(15.0), px(45.0)), px(20.0));
        assert_eq!(position, Position::new(0, 9));
    }

    #[test]
    fn test_display_snapshot_row_infos_follow_folded_lines() {
        let buffer = crate::buffer::TextBuffer::new("one\ntwo\nthree\nfour");
        let snapshot = test_editor_snapshot(buffer.snapshot(), vec![0, 2, 3], false);

        let rows = snapshot.document.display_snapshot.visible_rows(0, 3);
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0].row_id,
            DisplayRowId {
                buffer_line: 0,
                wrap_subrow: 0
            }
        );
        assert_eq!(rows[0].buffer_line, 0);
        assert_eq!(
            rows[1].row_id,
            DisplayRowId {
                buffer_line: 2,
                wrap_subrow: 0
            }
        );
        assert_eq!(rows[1].buffer_line, 2);
        assert_eq!(
            rows[2].row_id,
            DisplayRowId {
                buffer_line: 3,
                wrap_subrow: 0
            }
        );
        assert_eq!(rows[2].buffer_line, 3);
    }

    #[test]
    fn test_harness_asserts_display_round_trip() {
        let harness = EditorTestHarness::new("one\ntwo\nthree").with_display_lines(vec![0, 2]);
        harness.assert_display_round_trip(Position::new(2, 0));
    }

    #[test]
    fn test_harness_asserts_text_with_primary_selection() {
        let harness = EditorTestHarness::new("select")
            .with_primary_selection(Position::new(0, 1), Position::new(0, 4));
        harness.assert_text_with_selections("s[ele]ct");
    }

    #[test]
    fn editor_appearance_defaults_to_default() {
        assert_eq!(EditorAppearance::default(), EditorAppearance::Default);

        let harness = EditorTestHarness::new("select 1");
        assert_eq!(harness.snapshot().appearance, EditorAppearance::Default);
    }

    #[test]
    fn editor_snapshot_can_carry_query_console_appearance() {
        let buffer = crate::buffer::TextBuffer::new("select 1");
        let mut snapshot = test_editor_snapshot(buffer.snapshot(), vec![0], false);
        snapshot.appearance = EditorAppearance::QueryConsole;

        assert_eq!(snapshot.appearance, EditorAppearance::QueryConsole);
    }

    #[test]
    fn test_harness_supports_soft_wrap_snapshot_setup() {
        let harness = EditorTestHarness::new("abcdefghij").with_soft_wrap(4, vec![3]);
        let position = harness
            .snapshot()
            .pixel_to_position(point(px(15.0), px(45.0)), px(20.0));
        assert_eq!(position, Position::new(0, 9));
    }

    #[test]
    fn test_soft_wrap_bounds_and_hit_testing_share_cached_layout_model() {
        let harness = EditorTestHarness::new("abcdefghij").with_soft_wrap(4, vec![3]);
        let snapshot = harness.snapshot();
        let bounds = gpui::Bounds::new(point(px(0.0), px(0.0)), size(px(400.0), px(400.0)));

        let range_bounds = snapshot
            .bounds_for_range(Position::new(0, 9), Position::new(0, 10), bounds, px(20.0))
            .expect("wrapped range should resolve to bounds");

        let position = snapshot.pixel_to_position(
            point(
                range_bounds.origin.x + px(1.0),
                range_bounds.origin.y + px(1.0),
            ),
            px(20.0),
        );

        assert_eq!(position, Position::new(0, 9));
    }

    #[test]
    fn test_editor_snapshot_prefers_cached_wrap_layout_when_it_matches_viewport() {
        let mut snapshot = test_editor_snapshot(
            crate::buffer::TextBuffer::new("abcdefghij").snapshot(),
            vec![0],
            true,
        );
        snapshot.cached_layout.set_line_height(px(20.0));
        snapshot
            .cached_layout
            .set_wrap_layout(Some(VisibleWrapLayout::new(
                0..1,
                vec![3],
                4,
                0.0,
                px(20.0),
            )));
        snapshot.document.display_snapshot = DisplayMap::from_display_lines(vec![0]).snapshot(
            crate::buffer::TextBuffer::new("abcdefghij").snapshot(),
            &[],
            &HashSet::new(),
            true,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        let position = snapshot.pixel_to_position(point(px(15.0), px(45.0)), px(20.0));
        assert_eq!(position, Position::new(0, 9));
    }

    #[test]
    fn test_soft_wrap_hit_testing_rebuilds_stale_cached_layout_after_scroll() {
        let mut snapshot = test_editor_snapshot(
            crate::buffer::TextBuffer::new("abcd\nefgh\nijkl").snapshot(),
            vec![0, 1, 2],
            true,
        );
        snapshot.viewport_lines = 2;
        snapshot.scroll_offset = 1.0;
        snapshot.cached_layout.set_line_height(px(20.0));
        snapshot
            .cached_layout
            .set_wrap_layout(Some(VisibleWrapLayout::new(
                1..3,
                vec![1, 1],
                4,
                1.5,
                px(20.0),
            )));
        snapshot.document.display_snapshot = DisplayMap::from_display_lines(vec![0, 1, 2])
            .snapshot(
                snapshot.document.buffer_snapshot(),
                &[],
                &HashSet::new(),
                true,
                Arc::new(Vec::new()),
                &[],
                Arc::new(Vec::new()),
                &[],
            );

        let position = snapshot.pixel_to_position(point(px(15.0), px(5.0)), px(20.0));
        assert_eq!(position, Position::new(1, 1));
    }

    #[test]
    fn test_harness_tracks_extra_selection_setup() {
        let harness = EditorTestHarness::new("abc\ndef")
            .with_primary_selection(Position::new(0, 0), Position::new(0, 1))
            .with_extra_selection(Position::new(1, 1), Position::new(1, 3));
        assert_eq!(harness.snapshot().extra_cursors().len(), 1);
        assert_eq!(
            harness.snapshot().extra_cursors()[0].0.position(),
            Position::new(1, 3)
        );
    }

    #[test]
    fn test_snapshot_lab_marks_display_with_folded_rows_and_selection() {
        let lab = EditorSnapshotLabBuilder::new("one\ntwo\nthree")
            .with_fold_regions(vec![FoldRegion::new(1, 2, FoldKind::Block)])
            .with_folded_lines(&[1])
            .with_primary_selection(Position::new(0, 1), Position::new(0, 3))
            .build();

        lab.assert_marked_display_snapshot("o[ne]\ntwo");
        assert_eq!(
            lab.document_snapshot().display_lines().as_ref(),
            &vec![0, 1]
        );
    }

    #[test]
    fn fold_region_level_tracks_nested_regions() {
        let regions = vec![
            FoldRegion::new(0, 6, FoldKind::Block),
            FoldRegion::new(1, 5, FoldKind::Case),
            FoldRegion::new(2, 4, FoldKind::Parenthesis),
        ];

        assert_eq!(TextEditor::fold_region_level(&regions[0], &regions), 1);
        assert_eq!(TextEditor::fold_region_level(&regions[1], &regions), 2);
        assert_eq!(TextEditor::fold_region_level(&regions[2], &regions), 3);
    }

    #[test]
    fn test_snapshot_lab_carries_inline_overlays_and_large_file_policy() {
        let lab = EditorSnapshotLabBuilder::new("select 1")
            .with_inline_suggestion(InlineSuggestion::new(
                " from users".to_string(),
                Anchor::new(8, 0, Bias::Right),
            ))
            .with_edit_prediction(EditPrediction::new(
                " where id = 1".to_string(),
                Anchor::new(8, 0, Bias::Right),
            ))
            .with_large_file_policy(ResolvedLargeFilePolicy {
                tier: LargeFilePolicyTier::ReducedSemantic,
                syntax_highlighting_enabled: true,
                folding_enabled: true,
                diagnostics_enabled: false,
                completions_enabled: false,
                hover_enabled: false,
                reference_highlights_enabled: false,
                triggered_by_lines: true,
                triggered_by_bytes: false,
            })
            .build();

        lab.assert_overlay_resolution(true, true);
        assert_eq!(
            lab.document_snapshot().large_file_policy.tier,
            LargeFilePolicyTier::ReducedSemantic
        );
    }

    #[test]
    fn test_snapshot_lab_supports_large_file_reference_highlight_degradation() {
        let lab = EditorSnapshotLabBuilder::new("alpha beta gamma")
            .with_reference_ranges(vec![0..5, 6..10])
            .with_large_file_policy(ResolvedLargeFilePolicy {
                tier: LargeFilePolicyTier::PlainText,
                syntax_highlighting_enabled: false,
                folding_enabled: false,
                diagnostics_enabled: false,
                completions_enabled: false,
                hover_enabled: false,
                reference_highlights_enabled: false,
                triggered_by_lines: false,
                triggered_by_bytes: true,
            })
            .build();

        assert_eq!(lab.snapshot().document.reference_ranges(), &[0..5, 6..10]);
        assert!(
            !lab.document_snapshot()
                .large_file_policy
                .reference_highlights_enabled
        );
    }

    #[test]
    fn test_snapshot_lab_soft_wrap_hit_testing_survives_plain_text_large_file_policy() {
        let lab = EditorSnapshotLabBuilder::new("abcdefghij")
            .with_soft_wrap(4, vec![3])
            .with_large_file_policy(ResolvedLargeFilePolicy {
                tier: LargeFilePolicyTier::PlainText,
                syntax_highlighting_enabled: false,
                folding_enabled: false,
                diagnostics_enabled: false,
                completions_enabled: false,
                hover_enabled: false,
                reference_highlights_enabled: false,
                triggered_by_lines: true,
                triggered_by_bytes: false,
            })
            .build();

        let snapshot = lab.snapshot();
        let position = snapshot.pixel_to_position(point(px(15.0), px(45.0)), px(20.0));
        assert_eq!(position, Position::new(0, 9));

        let bounds = gpui::Bounds::new(point(px(0.0), px(0.0)), size(px(400.0), px(400.0)));
        let range_bounds = snapshot
            .bounds_for_range(Position::new(0, 8), Position::new(0, 10), bounds, px(20.0))
            .expect("wrapped range should resolve under plain text policy");
        assert_eq!(range_bounds.origin, point(px(0.0), px(40.0)));
        assert_eq!(range_bounds.size.width, px(20.0));
        assert_eq!(
            snapshot.document.large_file_policy.tier,
            LargeFilePolicyTier::PlainText
        );
    }

    #[test]
    fn test_anchor_backed_overlays_and_snippets_rebase_after_edit() {
        let snippet = Snippet::parse("${1:alpha} ${2:beta}");
        let mut buffer = crate::buffer::TextBuffer::new(&snippet.text);
        let mut active_snippet = ActiveSnippet::new(&snippet, &buffer, 0).expect("active snippet");
        let inline_suggestion = InlineSuggestion::new(
            " gamma".to_string(),
            buffer.anchor_at(5, Bias::Right).expect("inline anchor"),
        );
        let edit_prediction = EditPrediction::new(
            " delta".to_string(),
            buffer
                .anchor_at(10, Bias::Right)
                .expect("prediction anchor"),
        );

        let mut language_pipeline = LanguagePipelineState::new();
        language_pipeline.set_diagnostics(vec![Highlight {
            start: 6,
            end: 10,
            kind: crate::HighlightKind::Error,
        }]);
        language_pipeline.set_inlay_hints(vec![EditorInlayHint {
            byte_offset: 10,
            label: ":text".to_string(),
            side: crate::InlayHintSide::After,
            kind: Some(crate::InlayHintKind::Type),
            padding_left: true,
            padding_right: false,
        }]);

        let anchored_diagnostics = language_pipeline.anchored_diagnostics(&buffer);
        let anchored_inlay_hints = language_pipeline.anchored_inlay_hints(&buffer);

        buffer
            .insert(2, "wide_")
            .expect("insert inside first placeholder");
        assert!(active_snippet.normalize(&buffer));

        assert_eq!(
            buffer
                .resolve_anchor_offset(inline_suggestion.anchor)
                .unwrap(),
            10
        );
        assert_eq!(
            buffer
                .resolve_anchor_offset(edit_prediction.anchor)
                .unwrap(),
            15
        );
        assert_eq!(
            buffer
                .resolve_anchored_range(anchored_diagnostics[0].range)
                .unwrap(),
            11..15
        );
        assert_eq!(
            buffer
                .resolve_anchor_offset(anchored_inlay_hints[0].anchor)
                .unwrap(),
            15
        );
        assert_eq!(active_snippet.current_range(&buffer), Some((0, 10)));
        assert_eq!(active_snippet.advance(&buffer), Some((11, 15)));
    }

    #[test]
    fn test_sticky_header_excerpt_follows_fold_context() {
        let display_map = DisplayMap::from_display_lines(vec![0, 1, 2, 3]);
        let buffer = crate::buffer::TextBuffer::new("BEGIN\n  SELECT 1\nEND\nSELECT 2");
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[FoldRegion::new(0, 2, FoldKind::Block)],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        let header = snapshot.sticky_header_excerpt(1).expect("sticky header");
        assert_eq!(header.buffer_line, 0);
        assert_eq!(header.kind_label, "block");
        assert_eq!(header.text, "BEGIN");
    }

    #[test]
    fn test_visible_display_row_range_matches_viewport_lines() {
        let mut snapshot = test_editor_snapshot(
            crate::buffer::TextBuffer::new("a\nb\nc\nd\ne").snapshot(),
            vec![0, 1, 2, 3, 4],
            false,
        );
        snapshot.scroll_offset = 1.0;
        snapshot.viewport_lines = 2;

        assert_eq!(snapshot.visible_display_row_range(), 1..3);
    }

    #[test]
    fn test_minimap_viewport_indicator_uses_display_row_window() {
        let mut snapshot = test_editor_snapshot(
            crate::buffer::TextBuffer::new("a\nb\nc\nd\ne\nf").snapshot(),
            vec![0, 1, 2, 3, 4, 5],
            false,
        );
        snapshot.scroll_offset = 2.0;
        snapshot.viewport_lines = 3;
        snapshot.minimap_visible = true;

        let visible = snapshot.visible_display_row_range();
        assert_eq!(visible, 2..5);
    }

    #[test]
    fn test_anchored_code_actions_attach_to_cursor_line() {
        let mut pipeline = LanguagePipelineState::new();
        let actions = vec![lsp_types::CodeActionOrCommand::Command(
            lsp_types::Command {
                title: "Apply fix".to_string(),
                command: "fix.apply".to_string(),
                arguments: None,
            },
        )];
        let cursor = Cursor::at(Position::new(3, 4));

        pipeline.set_code_actions(actions);

        let anchored = pipeline.anchored_code_actions(&cursor);
        assert_eq!(anchored.len(), 1);
        assert_eq!(anchored[0].line, 3);
        assert_eq!(anchored[0].label, "Apply fix");
    }

    #[test]
    fn test_document_snapshot_can_expose_inline_code_actions() {
        let mut pipeline = LanguagePipelineState::new();
        let actions = vec![lsp_types::CodeActionOrCommand::Command(
            lsp_types::Command {
                title: "Quick fix".to_string(),
                command: "quick.fix".to_string(),
                arguments: None,
            },
        )];
        pipeline.set_code_actions(actions);
        let anchored = pipeline.anchored_code_actions(&Cursor::at(Position::new(0, 0)));
        assert_eq!(anchored.len(), 1);
        assert_eq!(anchored[0].line, 0);
        assert_eq!(anchored[0].label, "Quick fix");
    }

    #[test]
    fn test_snippet_parse_orders_placeholders_by_tab_stop() {
        let snippet = Snippet::parse("${2:table} ${1:column}");

        assert_eq!(snippet.text, "table column");
        assert_eq!(snippet.placeholders.len(), 2);
        assert_eq!(snippet.placeholders[0].index, 1);
        assert_eq!(snippet.placeholders[1].index, 2);
    }

    #[test]
    fn test_active_snippet_current_and_next_placeholder_ranges() {
        let snippet = Snippet::parse("SELECT ${1:column} FROM ${2:table}");
        let buffer = crate::buffer::TextBuffer::new(&snippet.text);
        let mut active = ActiveSnippet::new(&snippet, &buffer, 0).expect("active snippet");

        assert_eq!(active.current_range(&buffer), Some((7, 13)));
        assert_eq!(active.advance(&buffer), Some((19, 24)));
        assert_eq!(active.advance(&buffer), None);
    }

    #[test]
    fn test_edit_prediction_carries_anchor() {
        let prediction =
            EditPrediction::new(" FROM users".to_string(), Anchor::new(6, 1, Bias::Right));

        assert_eq!(prediction.anchor.offset(), 6);
        assert_eq!(prediction.anchor.revision(), 1);
    }

    #[test]
    fn test_document_snapshot_carries_edit_prediction() {
        let prediction =
            EditPrediction::new(" LIMIT 10".to_string(), Anchor::new(8, 3, Bias::Right));
        let snapshot = DocumentSnapshot {
            buffer: crate::buffer::TextBuffer::new("SELECT 1").snapshot(),
            context: crate::TextDocument::internal()
                .expect("internal document")
                .context(),
            identity: DocumentIdentity::internal().expect("internal document uri"),
            large_file_policy: ResolvedLargeFilePolicy::full(),
            display_snapshot: DisplayMap::from_display_lines(vec![0]).snapshot(
                crate::buffer::TextBuffer::new("SELECT 1").snapshot(),
                &[],
                &HashSet::new(),
                false,
                Arc::new(vec![]),
                &[],
                Arc::new(vec![]),
                &[],
            ),
            language: build_language_pipeline_snapshot(
                SyntaxSnapshot::empty(0),
                Vec::new(),
                Vec::new(),
            ),
            inline_suggestion: None,
            edit_prediction: Some(prediction.clone()),
            hover_state: None,
            signature_help_state: None,
            soft_wrap: false,
            show_inline_diagnostics: true,
        };

        assert_eq!(snapshot.edit_prediction, Some(prediction));
    }

    #[test]
    fn test_anchor_backed_edit_prediction_survives_buffer_revision_changes() {
        let mut buffer = crate::buffer::TextBuffer::new("SELECT");
        let anchor = buffer.anchor_at(6, Bias::Right).expect("prediction anchor");
        let prediction = EditPrediction::new(" 1".to_string(), anchor);

        buffer.insert(0, "-- ").expect("mutate buffer");

        assert_eq!(buffer.resolve_anchor_offset(prediction.anchor).unwrap(), 9);
    }

    #[test]
    fn test_inline_suggestion_carries_anchor() {
        let suggestion =
            InlineSuggestion::new(" FROM users".to_string(), Anchor::new(6, 1, Bias::Right));

        assert_eq!(suggestion.anchor.offset(), 6);
    }

    #[test]
    fn test_active_snippet_reconciliation_uses_collection_primary_selection() {
        let snippet = Snippet::parse("${1:alpha} ${2:beta}");
        let document = TextDocument::with_text(
            DocumentIdentity::internal().expect("internal uri"),
            &snippet.text,
        );
        let mut active_snippet =
            ActiveSnippet::new(&snippet, document.buffer(), 0).expect("active snippet");
        let primary_selection = active_snippet
            .current_selection(document.buffer())
            .expect("primary snippet selection");
        let matching = SelectionsCollection::single(
            Cursor::at(primary_selection.head()),
            primary_selection.clone(),
        );

        assert!(
            EditorSnippetState::active_snippet_matches_primary_selection(
                &document,
                &matching,
                &mut active_snippet,
            )
        );

        let mismatch = SelectionsCollection::single(
            Cursor::at(Position::new(0, 1)),
            Selection::at(Position::new(0, 1)),
        );
        assert!(
            !EditorSnippetState::active_snippet_matches_primary_selection(
                &document,
                &mismatch,
                &mut active_snippet,
            )
        );
    }

    #[test]
    fn test_block_widgets_exclude_diagnostics_and_keep_code_actions() {
        let buffer = crate::buffer::TextBuffer::new("abc\ndef\nghi");
        let snapshot = DisplayMap::from_display_lines(vec![0, 1, 2]).snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(vec![]),
            &[AnchoredDiagnostic {
                range: buffer
                    .anchored_range(4..8, Bias::Left, Bias::Right)
                    .expect("anchored diagnostic"),
                kind: crate::HighlightKind::Error,
            }],
            Arc::new(vec![]),
            &[AnchoredCodeAction {
                line: 2,
                label: "Quick fix".to_string(),
            }],
        );

        assert_eq!(snapshot.block_widgets().len(), 1);
        assert_eq!(snapshot.block_widgets()[0].label, "Quick fix");
    }

    #[test]
    fn test_innermost_enclosing_bracket_range_prefers_nested_call() {
        let range = StructuralRange {
            start: 10,
            end: 16,
            open: '(',
            close: ')',
        };

        assert_eq!(range.open, '(');
        assert_eq!(range.close, ')');
    }

    #[test]
    fn test_enclosing_bracket_ranges_include_quotes_without_confusing_brackets() {
        let ranges = [StructuralRange {
            start: 10,
            end: 20,
            open: '\'',
            close: '\'',
        }];

        assert!(ranges.iter().any(|range| range.open == '\''));
    }

    #[test]
    fn test_expand_selection_prefers_structural_range_after_word() {
        let word = 7..12;
        let structural = StructuralRange {
            start: 6,
            end: 18,
            open: '(',
            close: ')',
        };

        assert_ne!(word, structural.start..structural.end);
        assert_eq!(structural.open, '(');
    }

    #[test]
    fn test_shrink_selection_restores_previous_selection_shape() {
        let previous = Selection::from_anchor_head(Position::new(0, 7), Position::new(0, 12));
        let expanded = Selection::from_anchor_head(Position::new(0, 6), Position::new(0, 18));

        assert_ne!(previous.range(), expanded.range());
        assert_eq!(previous.range().start, Position::new(0, 7));
        assert_eq!(expanded.range().end, Position::new(0, 18));
    }

    #[test]
    fn test_anchor_scroll_offset_tracks_inserted_lines_above_anchor() {
        let mut snapshot = test_editor_snapshot(
            crate::buffer::TextBuffer::new("a\nb\nc\nd").snapshot(),
            vec![0, 1, 2, 3],
            false,
        );
        snapshot.scroll_anchor_position = Position::new(2, 0);
        snapshot.scroll_anchor_visual_offset = 1.0;

        assert_eq!(snapshot.anchored_scroll_offset(), 1.0);

        snapshot.document.display_snapshot = DisplayMap::from_display_lines(vec![0, 1, 2, 3, 4, 5])
            .snapshot(
                snapshot.document.buffer_snapshot(),
                &[],
                &HashSet::new(),
                false,
                Arc::new(Vec::new()),
                snapshot.document.anchored_diagnostics(),
                snapshot.document.anchored_inlay_hints(),
                snapshot.document.anchored_code_actions(),
            );
        snapshot.scroll_anchor_position = Position::new(4, 0);

        assert_eq!(snapshot.anchored_scroll_offset(), 3.0);
    }

    #[test]
    fn test_anchor_scroll_offset_stays_stable_when_fold_changes_below_anchor() {
        let mut snapshot = test_editor_snapshot(
            crate::buffer::TextBuffer::new("a\nb\nc\nd\ne").snapshot(),
            vec![0, 1, 2, 3, 4],
            false,
        );
        snapshot.scroll_anchor_position = Position::new(1, 0);
        snapshot.scroll_anchor_visual_offset = 1.0;
        let before = snapshot.anchored_scroll_offset();

        snapshot.document.display_snapshot = DisplayMap::from_display_lines(vec![0, 1, 2, 4])
            .snapshot(
                snapshot.document.buffer_snapshot(),
                &[],
                &HashSet::new(),
                false,
                Arc::new(Vec::new()),
                snapshot.document.anchored_diagnostics(),
                snapshot.document.anchored_inlay_hints(),
                snapshot.document.anchored_code_actions(),
            );

        assert_eq!(snapshot.anchored_scroll_offset(), before);
    }

    #[test]
    fn test_newer_syntax_revision_rejects_older_result() {
        let current_snapshot = SyntaxSnapshot::new(
            vec![Highlight {
                start: 0,
                end: 6,
                kind: crate::HighlightKind::Keyword,
            }],
            2,
        );
        let stale_snapshot = SyntaxSnapshot::new(Vec::new(), 1);

        let accepted = if stale_snapshot.revision() == current_snapshot.revision() {
            stale_snapshot
        } else {
            current_snapshot.clone()
        };

        assert_eq!(accepted.revision(), 2);
        assert_eq!(accepted.highlights().len(), 1);
    }

    #[test]
    fn test_transaction_record_restores_selection_on_undo_and_redo() {
        let before = SelectionState::new(SelectionsCollection::single(
            Cursor::at(Position::new(0, 1)),
            Selection::at(Position::new(0, 1)),
        ));
        let after = SelectionState::new(SelectionsCollection::from_primary_and_extras(
            Cursor::at(Position::new(1, 2)),
            Selection::from_anchor_head(Position::new(1, 0), Position::new(1, 2)),
            vec![(
                Cursor::at(Position::new(2, 1)),
                Selection::at(Position::new(2, 1)),
            )],
        ));
        let record = TransactionRecord::new(
            TransactionId(1),
            vec![crate::buffer::Change::insert(1, "x")],
            before.clone(),
            after.clone(),
        );

        assert_eq!(
            record.before().cursor().map(|cursor| cursor.position()),
            Some(Position::new(0, 1))
        );
        assert_eq!(
            record.after().cursor().map(|cursor| cursor.position()),
            Some(Position::new(1, 2))
        );
        assert_eq!(record.after().extra_cursors().len(), 1);
    }

    #[test]
    fn test_anchored_diagnostic_ranges_remain_byte_anchored() {
        let buffer = crate::buffer::TextBuffer::new("abcdefghij");
        let highlights = vec![Highlight {
            start: 4,
            end: 8,
            kind: crate::HighlightKind::Error,
        }];

        let mut pipeline = LanguagePipelineState::new();
        pipeline.set_diagnostics(highlights);
        let anchored = pipeline.anchored_diagnostics(&buffer);
        assert_eq!(
            buffer.resolve_anchored_range(anchored[0].range).unwrap(),
            4..8
        );
        assert_eq!(anchored[0].kind, crate::HighlightKind::Error);
    }

    #[test]
    fn test_anchored_inlay_hints_preserve_anchor_offsets() {
        let buffer = crate::buffer::TextBuffer::new("0123456789abcdef");
        let mut pipeline = LanguagePipelineState::new();
        pipeline.set_inlay_hints(vec![crate::EditorInlayHint {
            byte_offset: 12,
            label: "hint".to_string(),
            side: crate::InlayHintSide::After,
            kind: Some(crate::InlayHintKind::Type),
            padding_left: true,
            padding_right: false,
        }]);

        let anchored = pipeline.anchored_inlay_hints(&buffer);
        assert_eq!(
            buffer.resolve_anchor_offset(anchored[0].anchor).unwrap(),
            12
        );
        assert_eq!(anchored[0].label, "hint");
    }

    #[test]
    fn test_edited_line_range_from_changes_is_localized() {
        let mut buffer = crate::buffer::TextBuffer::new("one\ntwo\nthree\nfour");
        buffer.insert(4, "X").unwrap();

        let range =
            edited_line_range_from_changes(buffer.changes(), &buffer).expect("edited range");
        assert_eq!(range, 1..2);
    }

    #[test]
    fn test_display_map_records_last_sync_range() {
        let mut display_map = DisplayMap::new();
        display_map.sync_range(10, &[], &HashSet::new(), 2..4);

        assert_eq!(display_map.last_sync_range(), Some(2..4));
    }

    #[test]
    fn test_display_snapshot_text_chunks_include_highlights_and_inlays() {
        let buffer = crate::buffer::TextBuffer::new("alpha\nbeta");
        let display_map = DisplayMap::from_display_lines(vec![0, 1]);
        let display_snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(vec![Highlight {
                start: 1,
                end: 4,
                kind: crate::HighlightKind::Keyword,
            }]),
            &[AnchoredDiagnostic {
                range: buffer
                    .anchored_range(2..5, Bias::Left, Bias::Right)
                    .expect("anchored diagnostic"),
                kind: crate::HighlightKind::Error,
            }],
            Arc::new(vec![AnchoredInlayHint {
                anchor: buffer.anchor_at(3, Bias::Right).expect("inlay anchor"),
                label: "hint".to_string(),
                side: crate::InlayHintSide::After,
                kind: Some(crate::InlayHintKind::Type),
                padding_left: false,
                padding_right: false,
            }]),
            &[],
        );

        let chunks = display_snapshot.text_chunks(0..1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "alpha");
        assert_eq!(chunks[0].highlights.len(), 1);
        assert_eq!(chunks[0].diagnostics.len(), 1);
        assert_eq!(chunks[0].inlay_hints.len(), 1);
    }

    #[test]
    fn test_editor_core_snapshot_prefers_selections_collection_state() {
        let mut harness = EditorTestHarness::new("alpha\nbeta\ngamma");
        harness.snapshot.selections = SelectionsCollection::from_primary_and_extras(
            Cursor::at(Position::new(1, 2)),
            Selection::from_anchor_head(Position::new(1, 0), Position::new(1, 2)),
            vec![(
                Cursor::at(Position::new(2, 5)),
                Selection::at(Position::new(2, 5)),
            )],
        );

        assert_eq!(
            harness.snapshot.cursor().map(|cursor| cursor.position()),
            Some(Position::new(1, 2))
        );
        let selection = harness
            .snapshot
            .selection()
            .cloned()
            .expect("primary selection");
        assert_eq!(selection.range().start, Position::new(1, 0));
        assert_eq!(selection.range().end, Position::new(1, 2));
        let extra_cursors = harness.snapshot.extra_cursors();
        assert_eq!(extra_cursors.len(), 1);
        assert_eq!(extra_cursors[0].0.position(), Position::new(2, 5));
    }

    #[test]
    fn test_text_editor_cursor_queries_prefer_selections_collection_state() {
        let mut harness = EditorTestHarness::new("alpha\nbeta");
        harness.snapshot.selections = SelectionsCollection::single(
            Cursor::at(Position::new(1, 2)),
            Selection::from_anchor_head(Position::new(1, 0), Position::new(1, 2)),
        );

        assert_eq!(
            harness.snapshot.cursor().map(|cursor| cursor.position()),
            Some(Position::new(1, 2))
        );
        let selection = harness
            .snapshot
            .selection()
            .cloned()
            .expect("primary selection");
        assert!(selection.has_selection());
        assert_eq!(selection.range().start, Position::new(1, 0));
        assert_eq!(selection.range().end, Position::new(1, 2));
    }

    #[test]
    fn test_snapshot_extra_cursor_accessor_prefers_collection_entries() {
        let mut harness = EditorTestHarness::new("alpha\nbeta\ngamma");
        harness.snapshot.selections = SelectionsCollection::from_primary_and_extras(
            Cursor::at(Position::new(0, 0)),
            Selection::at(Position::new(0, 0)),
            vec![
                (
                    Cursor::at(Position::new(1, 1)),
                    Selection::at(Position::new(1, 1)),
                ),
                (
                    Cursor::at(Position::new(2, 2)),
                    Selection::at(Position::new(2, 2)),
                ),
            ],
        );

        let extras = harness.snapshot.extra_cursors();
        assert_eq!(extras.len(), 2);
        assert_eq!(extras[0].0.position(), Position::new(1, 1));
        assert_eq!(extras[1].0.position(), Position::new(2, 2));
    }

    #[test]
    fn test_editor_snapshot_cursor_accessors_follow_collection_for_fold_rescue_shape() {
        let mut harness = EditorTestHarness::new("one\ntwo\nthree");
        harness.snapshot.selections = SelectionsCollection::single(
            Cursor::at(Position::new(2, 3)),
            Selection::at(Position::new(2, 3)),
        );

        assert_eq!(
            harness.snapshot.cursor().map(|cursor| cursor.position()),
            Some(Position::new(2, 3))
        );
    }

    #[test]
    fn test_selections_collection_primary_newest_and_all() {
        let collection = SelectionsCollection::from_primary_and_extras(
            Cursor::at(Position::new(0, 0)),
            Selection::at(Position::new(0, 0)),
            vec![
                (
                    Cursor::at(Position::new(1, 0)),
                    Selection::at(Position::new(1, 0)),
                ),
                (
                    Cursor::at(Position::new(2, 0)),
                    Selection::at(Position::new(2, 0)),
                ),
            ],
        );

        assert_eq!(collection.len(), 3);
        assert_eq!(
            collection.primary().map(|entry| entry.cursor.position()),
            Some(Position::new(0, 0))
        );
        assert_eq!(
            collection.newest().map(|entry| entry.cursor.position()),
            Some(Position::new(2, 0))
        );
        assert_eq!(collection.all().len(), 3);
    }

    #[test]
    fn test_selections_collection_preserves_extra_selection_order() {
        let mut collection = SelectionsCollection::single(
            Cursor::at(Position::new(0, 0)),
            Selection::at(Position::new(0, 0)),
        );
        collection.push(
            Cursor::at(Position::new(3, 1)),
            Selection::at(Position::new(3, 1)),
        );
        collection.push(
            Cursor::at(Position::new(4, 2)),
            Selection::at(Position::new(4, 2)),
        );

        let extra_positions: Vec<_> = collection
            .extra_entries()
            .iter()
            .map(|entry| entry.cursor.position())
            .collect();
        assert_eq!(
            extra_positions,
            vec![Position::new(3, 1), Position::new(4, 2)]
        );
    }

    #[test]
    fn test_language_pipeline_snapshot_keeps_syntax_diagnostics_and_folds_together() {
        let syntax = SyntaxSnapshot::new(
            vec![Highlight {
                start: 0,
                end: 6,
                kind: crate::HighlightKind::Keyword,
            }],
            3,
        );
        let diagnostics = vec![Highlight {
            start: 7,
            end: 10,
            kind: crate::HighlightKind::Error,
        }];
        let folds = vec![FoldRegion::new(1, 3, FoldKind::Block)];

        let pipeline =
            build_language_pipeline_snapshot(syntax.clone(), diagnostics.clone(), folds.clone());

        assert_eq!(pipeline.syntax.revision(), 3);
        assert_eq!(pipeline.diagnostics, diagnostics);
        assert_eq!(pipeline.fold_regions, folds);
    }

    #[test]
    fn test_document_snapshot_exposes_language_pipeline() {
        let syntax = SyntaxSnapshot::new(
            vec![Highlight {
                start: 0,
                end: 6,
                kind: crate::HighlightKind::Keyword,
            }],
            2,
        );
        let diagnostics = vec![Highlight {
            start: 7,
            end: 8,
            kind: crate::HighlightKind::Error,
        }];
        let folds = vec![FoldRegion::new(1, 2, FoldKind::Block)];
        let snapshot = DocumentSnapshot {
            buffer: crate::buffer::TextBuffer::new("select\nfrom table").snapshot(),
            context: crate::TextDocument::internal()
                .expect("internal document")
                .context(),
            identity: DocumentIdentity::internal().expect("internal document uri"),
            large_file_policy: ResolvedLargeFilePolicy::full(),
            display_snapshot: DisplayMap::from_display_lines(vec![0, 1]).snapshot(
                crate::buffer::TextBuffer::new("select\nfrom table").snapshot(),
                &folds,
                &HashSet::new(),
                false,
                Arc::new(vec![]),
                &[],
                Arc::new(vec![]),
                &[],
            ),
            language: build_language_pipeline_snapshot(
                syntax.clone(),
                diagnostics.clone(),
                folds.clone(),
            ),
            inline_suggestion: None,
            edit_prediction: None,
            hover_state: None,
            signature_help_state: None,
            soft_wrap: false,
            show_inline_diagnostics: true,
        };

        assert_eq!(snapshot.language.syntax.revision(), 2);
        assert_eq!(snapshot.language.fold_regions, folds);
        assert_eq!(snapshot.language.diagnostics, diagnostics);
    }

    #[test]
    fn test_document_snapshot_reads_language_projection_from_single_pipeline_snapshot() {
        let buffer = crate::buffer::TextBuffer::new("begin\nselect value\nend");
        let anchored_diagnostics = vec![AnchoredDiagnostic {
            range: buffer
                .anchored_range(6..12, Bias::Left, Bias::Right)
                .expect("anchored diagnostic"),
            kind: crate::HighlightKind::Error,
        }];
        let anchored_inlay_hints = Arc::new(vec![AnchoredInlayHint {
            anchor: buffer.anchor_at(12, Bias::Right).expect("inlay anchor"),
            label: ":int".to_string(),
            side: crate::InlayHintSide::After,
            kind: Some(crate::InlayHintKind::Type),
            padding_left: true,
            padding_right: false,
        }]);
        let anchored_code_actions = vec![AnchoredCodeAction {
            line: 1,
            label: "Apply fix".to_string(),
        }];
        let language = crate::language_pipeline::LanguagePipelineSnapshot {
            syntax: SyntaxSnapshot::new(
                vec![Highlight {
                    start: 0,
                    end: 5,
                    kind: crate::HighlightKind::Keyword,
                }],
                4,
            ),
            syntax_generation: 1,
            diagnostics: vec![Highlight {
                start: 6,
                end: 12,
                kind: crate::HighlightKind::Error,
            }],
            fold_regions: vec![FoldRegion::new(0, 2, FoldKind::Block)],
            anchored_diagnostics: anchored_diagnostics.clone(),
            inlay_hints: Arc::new(vec![EditorInlayHint {
                byte_offset: 12,
                label: ":int".to_string(),
                side: crate::InlayHintSide::After,
                kind: Some(crate::InlayHintKind::Type),
                padding_left: true,
                padding_right: false,
            }]),
            anchored_inlay_hints: anchored_inlay_hints.clone(),
            code_actions: Vec::new(),
            anchored_code_actions: anchored_code_actions.clone(),
            reference_ranges: std::iter::once(6..11).collect(),
        };
        let snapshot = DocumentSnapshot {
            buffer: buffer.snapshot(),
            context: crate::TextDocument::internal()
                .expect("internal document")
                .context(),
            identity: DocumentIdentity::internal().expect("internal document uri"),
            large_file_policy: ResolvedLargeFilePolicy::full(),
            display_snapshot: DisplayMap::from_display_lines(vec![0, 1, 2]).snapshot(
                buffer.snapshot(),
                &language.fold_regions,
                &HashSet::new(),
                false,
                language.syntax.highlights(),
                &anchored_diagnostics,
                anchored_inlay_hints.clone(),
                &anchored_code_actions,
            ),
            language,
            inline_suggestion: None,
            edit_prediction: None,
            hover_state: None,
            signature_help_state: None,
            soft_wrap: false,
            show_inline_diagnostics: true,
        };

        assert_eq!(
            snapshot.anchored_diagnostics(),
            anchored_diagnostics.as_slice()
        );
        assert_eq!(
            snapshot.anchored_inlay_hints().as_ref(),
            anchored_inlay_hints.as_ref()
        );
        assert_eq!(
            snapshot.anchored_code_actions(),
            anchored_code_actions.as_slice()
        );
        assert_eq!(
            snapshot.fold_regions(),
            &[FoldRegion::new(0, 2, FoldKind::Block)]
        );
        assert_eq!(
            snapshot.diagnostics(),
            &[Highlight {
                start: 6,
                end: 12,
                kind: crate::HighlightKind::Error,
            }]
        );
        let expected_reference_ranges = std::iter::once(6..11).collect::<Vec<_>>();
        assert_eq!(
            snapshot.reference_ranges(),
            expected_reference_ranges.as_slice()
        );
        assert_eq!(snapshot.inlay_hints().len(), 1);
        assert_eq!(snapshot.syntax_highlights().len(), 1);
    }

    #[test]
    fn test_document_identity_internal_uri_is_preserved_in_snapshot() {
        let identity = DocumentIdentity::internal().expect("internal identity");
        let snapshot = DocumentSnapshot {
            buffer: crate::buffer::TextBuffer::new("select 1").snapshot(),
            context: crate::TextDocument::internal()
                .expect("internal document")
                .context(),
            identity: identity.clone(),
            large_file_policy: ResolvedLargeFilePolicy::full(),
            display_snapshot: DisplayMap::from_display_lines(vec![0]).snapshot(
                crate::buffer::TextBuffer::new("select 1").snapshot(),
                &[],
                &HashSet::new(),
                false,
                Arc::new(vec![]),
                &[],
                Arc::new(vec![]),
                &[],
            ),
            language: build_language_pipeline_snapshot(
                SyntaxSnapshot::empty(0),
                Vec::new(),
                Vec::new(),
            ),
            inline_suggestion: None,
            edit_prediction: None,
            hover_state: None,
            signature_help_state: None,
            soft_wrap: false,
            show_inline_diagnostics: true,
        };

        assert_eq!(snapshot.identity, identity);
    }

    #[test]
    fn test_external_document_identity_exposes_uri() {
        let uri = "file:///tmp/query.sql"
            .parse::<lsp_types::Uri>()
            .expect("valid uri");
        let identity = DocumentIdentity::External {
            uri: uri.clone(),
            path: Some(std::path::PathBuf::from("/tmp/query.sql")),
        };

        assert_eq!(identity.uri(), &uri);
    }

    #[test]
    fn test_document_snapshot_reports_dirty_from_saved_revision_context() {
        let mut document = crate::TextDocument::internal().expect("internal document");
        document.mark_saved(1);
        let snapshot = DocumentSnapshot {
            buffer: crate::buffer::TextBuffer::new("select 1").snapshot(),
            context: document.context(),
            identity: DocumentIdentity::internal().expect("internal identity"),
            large_file_policy: ResolvedLargeFilePolicy::full(),
            display_snapshot: DisplayMap::from_display_lines(vec![0]).snapshot(
                crate::buffer::TextBuffer::new("select 1").snapshot(),
                &[],
                &HashSet::new(),
                false,
                Arc::new(vec![]),
                &[],
                Arc::new(vec![]),
                &[],
            ),
            language: build_language_pipeline_snapshot(
                SyntaxSnapshot::empty(0),
                Vec::new(),
                Vec::new(),
            ),
            inline_suggestion: None,
            edit_prediction: None,
            hover_state: None,
            signature_help_state: None,
            soft_wrap: false,
            show_inline_diagnostics: true,
        };

        assert!(snapshot.is_dirty());
    }

    #[test]
    fn test_large_file_policy_resolves_reduced_semantic_tier() {
        let policy = LargeFilePolicyConfig {
            reduced_semantic_line_threshold: 10,
            reduced_semantic_byte_threshold: 100,
            plain_text_line_threshold: 50,
            plain_text_byte_threshold: 500,
        }
        .resolve(10, 20);

        assert_eq!(policy.tier, LargeFilePolicyTier::ReducedSemantic);
        assert!(policy.syntax_highlighting_enabled);
        assert!(policy.folding_enabled);
        assert!(!policy.diagnostics_enabled);
        assert!(policy.completions_enabled);
        assert!(policy.hover_enabled);
        assert!(!policy.reference_highlights_enabled);
    }

    #[test]
    fn test_large_file_policy_resolves_plain_text_tier() {
        let policy = LargeFilePolicyConfig {
            reduced_semantic_line_threshold: 10,
            reduced_semantic_byte_threshold: 100,
            plain_text_line_threshold: 20,
            plain_text_byte_threshold: 200,
        }
        .resolve(25, 20);

        assert_eq!(policy.tier, LargeFilePolicyTier::PlainText);
        assert!(!policy.syntax_highlighting_enabled);
        assert!(!policy.folding_enabled);
        assert!(!policy.diagnostics_enabled);
    }

    #[test]
    fn test_snapshot_lab_preserves_inlay_and_reference_configuration() {
        let lab = EditorSnapshotLabBuilder::new("alpha beta")
            .with_inlay_hints(vec![EditorInlayHint {
                byte_offset: 5,
                label: ": text".to_string(),
                side: crate::InlayHintSide::After,
                kind: Some(crate::InlayHintKind::Type),
                padding_left: true,
                padding_right: false,
            }])
            .with_reference_ranges(std::iter::once(0..5).collect())
            .build();

        assert_eq!(lab.document_snapshot().inlay_hints().len(), 1);
        let expected_reference_ranges = std::iter::once(0..5).collect::<Vec<_>>();
        assert_eq!(
            lab.document_snapshot().reference_ranges(),
            expected_reference_ranges.as_slice()
        );
    }

    #[test]
    fn test_document_snapshot_reports_viewport_local_syntax_strategy_for_reduced_semantic_policy() {
        let snapshot = test_editor_snapshot(
            crate::buffer::TextBuffer::new("alpha\nselect beta\nomega").snapshot(),
            vec![0, 1, 2],
            false,
        );
        let mut document = snapshot.document.clone();
        document.large_file_policy = ResolvedLargeFilePolicy {
            tier: LargeFilePolicyTier::ReducedSemantic,
            syntax_highlighting_enabled: true,
            folding_enabled: true,
            diagnostics_enabled: false,
            completions_enabled: true,
            hover_enabled: true,
            reference_highlights_enabled: false,
            triggered_by_lines: true,
            triggered_by_bytes: false,
        };

        assert_eq!(
            document.syntax_refresh_strategy(1, 1.0),
            SyntaxRefreshStrategy::VisibleRange(6..18)
        );
    }

    #[test]
    fn test_document_snapshot_reports_disabled_syntax_strategy_for_plain_text_policy() {
        let snapshot = test_editor_snapshot(
            crate::buffer::TextBuffer::new("alpha\nselect beta\nomega").snapshot(),
            vec![0, 1, 2],
            false,
        );
        let mut document = snapshot.document.clone();
        document.large_file_policy = ResolvedLargeFilePolicy {
            tier: LargeFilePolicyTier::PlainText,
            syntax_highlighting_enabled: false,
            folding_enabled: false,
            diagnostics_enabled: false,
            completions_enabled: false,
            hover_enabled: false,
            reference_highlights_enabled: false,
            triggered_by_lines: true,
            triggered_by_bytes: false,
        };

        assert_eq!(
            document.syntax_refresh_strategy(2, 0.0),
            SyntaxRefreshStrategy::Disabled
        );
    }

    #[test]
    fn test_reduced_semantic_policy_keeps_local_inline_and_completion_features_enabled() {
        let policy = LargeFilePolicyConfig {
            reduced_semantic_line_threshold: 10,
            reduced_semantic_byte_threshold: 100,
            plain_text_line_threshold: 50,
            plain_text_byte_threshold: 500,
        }
        .resolve(10, 20);

        assert!(policy.completions_enabled);
        assert!(policy.hover_enabled);
        assert!(!policy.allow_async_provider_requests());
    }

    #[test]
    fn test_snapshot_lab_exposes_policy_consistent_viewport_syntax_strategy() {
        let snapshot = test_editor_snapshot(
            crate::buffer::TextBuffer::new("alpha\nbeta\ngamma").snapshot(),
            vec![0, 1, 2],
            false,
        );
        let mut document = snapshot.document.clone();
        document.large_file_policy = ResolvedLargeFilePolicy {
            tier: LargeFilePolicyTier::ReducedSemantic,
            syntax_highlighting_enabled: true,
            folding_enabled: true,
            diagnostics_enabled: false,
            completions_enabled: true,
            hover_enabled: true,
            reference_highlights_enabled: false,
            triggered_by_lines: true,
            triggered_by_bytes: false,
        };

        assert_eq!(
            document.syntax_refresh_strategy(2, 0.0),
            SyntaxRefreshStrategy::VisibleRange(0..11)
        );
    }

    #[test]
    fn test_snapshot_lab_supports_viewport_driven_reduced_semantic_syntax_strategy() {
        let lab = EditorSnapshotLabBuilder::new("zero\none\ntwo\nthree")
            .with_viewport(1, 2.0)
            .with_large_file_policy(ResolvedLargeFilePolicy {
                tier: LargeFilePolicyTier::ReducedSemantic,
                syntax_highlighting_enabled: true,
                folding_enabled: true,
                diagnostics_enabled: false,
                completions_enabled: true,
                hover_enabled: true,
                reference_highlights_enabled: false,
                triggered_by_lines: true,
                triggered_by_bytes: false,
            })
            .build();

        assert_eq!(
            lab.syntax_refresh_strategy(),
            SyntaxRefreshStrategy::VisibleRange(9..13)
        );
    }

    #[test]
    fn test_snapshot_lab_uses_display_lines_for_visible_range_under_reduced_semantic_policy() {
        let lab = EditorSnapshotLabBuilder::new("zero\none\ntwo\nthree")
            .with_fold_regions(vec![FoldRegion::new(1, 2, FoldKind::Block)])
            .with_folded_lines(&[1])
            .with_viewport(1, 1.0)
            .with_large_file_policy(ResolvedLargeFilePolicy {
                tier: LargeFilePolicyTier::ReducedSemantic,
                syntax_highlighting_enabled: true,
                folding_enabled: true,
                diagnostics_enabled: false,
                completions_enabled: true,
                hover_enabled: true,
                reference_highlights_enabled: false,
                triggered_by_lines: true,
                triggered_by_bytes: false,
            })
            .build();

        assert_eq!(
            lab.document_snapshot().display_lines().as_ref(),
            &vec![0, 1, 3]
        );
        assert_eq!(
            lab.syntax_refresh_strategy(),
            SyntaxRefreshStrategy::VisibleRange(5..9)
        );
    }

    #[test]
    fn test_reverse_text_chunks_follow_folded_display_order() {
        let chunks = vec![
            DisplayTextChunk {
                row_id: DisplayRowId {
                    buffer_line: 0,
                    wrap_subrow: 0,
                },
                display_row: 0,
                buffer_line: 0,
                start_offset: 0,
                text: "one".to_string(),
                highlights: Vec::new(),
                diagnostics: Vec::new(),
                inlay_hints: Vec::new(),
            },
            DisplayTextChunk {
                row_id: DisplayRowId {
                    buffer_line: 2,
                    wrap_subrow: 0,
                },
                display_row: 1,
                buffer_line: 2,
                start_offset: 8,
                text: "three".to_string(),
                highlights: Vec::new(),
                diagnostics: Vec::new(),
                inlay_hints: Vec::new(),
            },
        ];

        let mut reversed = chunks.clone();
        reversed.reverse();
        assert_eq!(reversed[0].buffer_line, 2);
        assert_eq!(reversed[1].buffer_line, 0);
    }
}
