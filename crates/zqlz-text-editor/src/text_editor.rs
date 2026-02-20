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
pub mod buffer;
pub mod cursor;
pub mod element;
pub mod find;
pub mod lsp;
pub mod selection;
pub mod syntax;
pub mod vim;

// SQL editor feature modules (merged from zqlz-editor)
pub mod bookmarks;
pub mod find_replace;
pub mod folding;
pub mod formatter;

use gpui::*;
use std::sync::Arc;

// Re-exports for convenience
pub use bookmarks::{Bookmark, BookmarkFilter, BookmarkManager, BookmarkStorage};
pub use buffer::{Position, Range, TextBuffer};
pub use cursor::Cursor;
pub use element::EditorElement;
pub use find::{FindMatch, FindOptions, FindState};
pub use find_replace::{
    count_matches, find_all, find_first, find_next, replace_all, replace_first, replace_next,
    FindError, FindOptions as TextFindOptions, Match as TextMatch, ReplaceResult,
};
pub use folding::{detect_folds, FoldKind, FoldRegion, FoldingDetector};
pub use formatter::{
    format_sql, format_sql_with_config, FormatError, FormatterConfig, SqlFormatter,
};
pub use lsp::{CompletionProvider, DefinitionProvider, HoverProvider, Lsp, ReferencesProvider, SqlCompletionProvider};
pub use lsp_types::{CompletionItem, Hover, SignatureHelp};
pub use selection::Selection;
pub use syntax::{Highlight, HighlightKind, SyntaxHighlighter};
pub use vim::{VimMode, VimState};

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
/// - ✅ Phase 11: Vim modal editing (optional)
pub struct TextEditor {
    /// The underlying text buffer using rope data structure
    buffer: TextBuffer,
    /// Cursor position and movement state
    cursor: Cursor,
    /// Text selection state
    selection: Selection,
    focus_handle: FocusHandle,
    /// LSP provider container for code intelligence features
    pub lsp: Lsp,
    /// SQL syntax highlighter for visual highlighting
    syntax_highlighter: Option<SyntaxHighlighter>,
    /// Cached syntax highlights for current buffer (updated on text change)
    cached_highlights: Vec<Highlight>,
    /// Cached error diagnostics (syntax errors) for rendering squiggles
    cached_errors: Vec<Highlight>,
    /// Undo stack — each entry is a *group* of changes that are undone atomically.
    /// Consecutive single-character insertions within 300ms are coalesced into one
    /// group so that Cmd+Z undoes a whole typed "word" rather than one character.
    undo_stack: Vec<Vec<buffer::Change>>,
    /// Redo stack — mirrors undo_stack structure.
    redo_stack: Vec<Vec<buffer::Change>>,
    /// Timestamp of the most recent user edit, used to decide whether to extend the
    /// current undo group or open a new one (feat-025: 300ms grouping window).
    last_edit_time: Option<std::time::Instant>,
    /// Vertical scroll offset in lines (Phase 7: Scrolling)
    scroll_offset: f32,
    /// Last known viewport size in lines (updated during render, used for auto-scroll)
    last_viewport_lines: usize,
    /// Completion menu state
    completion_menu: Option<CompletionMenuState>,
    /// Hover tooltip state (Phase 8: LSP Integration)
    hover_state: Option<HoverState>,
    /// Find & Replace panel state (Phase 9). None when the panel is closed.
    pub find_state: Option<FindState>,
    /// Vim modal editing state (Phase 11). None when vim mode is disabled.
    vim_state: Option<VimState>,
    /// Position where a mouse drag started, used to extend the selection as the
    /// mouse moves. Cleared on mouse-up.
    mouse_drag_anchor: Option<Position>,
    /// Cached gutter width from the last render pass, used by mouse handlers to
    /// convert screen-space X coordinates into buffer column indices correctly.
    cached_gutter_width: f32,
    /// Cached monospace character width from the last render pass. Storing this
    /// here (rather than re-measuring in each event handler) guarantees that the
    /// same measurement is used during both painting and hit-testing, eliminating
    /// any per-call variance that could shift the computed column by ±1.
    cached_char_width: gpui::Pixels,
    /// Cached bounds origin (top-left of the editor element in window space) from
    /// the last render pass. GPUI delivers mouse positions in window coordinates,
    /// so every mouse handler must subtract this origin before computing a buffer
    /// position from pixel coordinates.
    cached_bounds_origin: gpui::Point<gpui::Pixels>,
    /// Cached completion menu layout from the last paint pass.
    ///
    /// Used by `handle_mouse_down` to hit-test whether a click lands inside the
    /// floating menu so it can select the item and accept it rather than moving
    /// the cursor into the text. Set to `None` when the menu is not painted.
    cached_completion_menu_bounds: Option<CachedCompletionMenuBounds>,
    /// Timestamp of the last mouse-down event (in milliseconds since epoch) and
    /// the position it landed on — used to detect double/triple clicks.
    last_click: Option<(std::time::Instant, Position)>,
    /// Ghost-text inline suggestion set by the parent (e.g. `QueryEditor`).
    ///
    /// `(text, cursor_offset)` where `cursor_offset` is the byte offset in the
    /// buffer at which the ghost text should be anchored.  The suggestion is
    /// shown as dimmed text immediately after the real text at that position.
    /// It is intentionally not an editor-owned computation — the parent pushes
    /// it here so that `EditorElement` can paint it without knowing about the
    /// higher-level `QueryEditor`.
    inline_suggestion: Option<(String, usize)>,
    /// Byte range of the current IME composition string in the buffer.
    ///
    /// Set by `replace_and_mark_text_in_range` (IME preedit) and cleared when
    /// the composition is committed (`replace_text_in_range`) or cancelled
    /// (`unmark_text`).  None when no IME composition is in progress.
    ime_marked_range: Option<std::ops::Range<usize>>,
    /// Additional cursors for multi-cursor editing (feat-017/018/021/022).
    ///
    /// The *primary* cursor lives in `self.cursor` / `self.selection`.  Each
    /// secondary cursor is stored as `(Cursor, Selection)` alongside its own
    /// independent selection anchor so that all the usual movement helpers can
    /// be reused on any cursor uniformly.
    extra_cursors: Vec<(Cursor, Selection)>,
    /// Set to `true` when the most recent Cmd+L added a line selection so that
    /// the next Cmd+L can extend it downward rather than re-selecting the same
    /// line (feat-016).
    last_select_line_was_extend: bool,
    /// Whether the whole-line clipboard flag is set — `true` means the last
    /// copy/cut had no selection and captured the whole line, so the next
    /// paste should insert above the cursor line rather than at the cursor
    /// (feat-023).
    clipboard_is_whole_line: bool,
    /// Snapshots of (primary_cursor, primary_selection, extra_cursors) captured
    /// before each selection-modifying action so that Cmd+U can walk back through
    /// selection history without touching the text (feat-020).
    selection_history: Vec<(Cursor, Selection, Vec<(Cursor, Selection)>)>,
    /// Go-to-line dialog state (feat-040). None when the dialog is closed.
    pub goto_line_state: Option<GoToLineState>,
    /// Whether soft-wrapping is enabled (feat-041). Default: false.
    ///
    /// When true, the element layer wraps visual lines at the available width;
    /// the underlying buffer and cursor model remain line-based (logical lines).
    pub soft_wrap: bool,
    /// Highlighted reference byte ranges from the last find-references call (feat-047).
    /// Cleared when the cursor moves or a new search is triggered.
    pub reference_ranges: Vec<std::ops::Range<usize>>,
    /// Inline rename dialog state (feat-048). None when not renaming.
    pub rename_state: Option<RenameState>,
    /// Context menu state (feat-045). None when no menu is open.
    pub context_menu: Option<ContextMenuState>,
    /// Set of buffer start-line indices for currently collapsed fold regions.
    folded_lines: std::collections::HashSet<usize>,
    /// Fold regions detected from the current buffer content (refreshed on every text change).
    fold_regions_cache: Vec<FoldRegion>,
    /// Fold chevron hit-test rects cached from the last paint pass.
    ///
    /// Each entry is `(start_line, rect)` in window coordinates so that
    /// `handle_mouse_down` can determine which fold was clicked without
    /// needing access to element-layer data.
    pub(crate) cached_fold_chevrons: Vec<(usize, gpui::Bounds<gpui::Pixels>)>,
}

/// State for the completion menu
struct CompletionMenuState {
    /// The completion items to display
    items: Vec<CompletionItem>,
    /// The offset where the completion was triggered
    trigger_offset: usize,
    /// The currently selected item index
    selected_index: usize,
    /// First visible item index — updated when selection moves out of the visible window
    scroll_offset: usize,
    /// Fractional scroll delta carried over between frames to prevent small wheel
    /// movements from being silently discarded by integer truncation.
    scroll_accumulator: f32,
}

/// Layout snapshot of the completion menu as last painted, cached so that
/// `handle_mouse_down` can hit-test clicks against the menu without needing
/// access to element-layer layout information.
pub(crate) struct CachedCompletionMenuBounds {
    /// Full bounding rect of the menu in window coordinates.
    pub(crate) bounds: Bounds<Pixels>,
    /// Uniform row height (every item is the same height).
    pub(crate) item_height: Pixels,
    /// Number of items that were painted.
    pub(crate) item_count: usize,
}

/// State for the go-to-line dialog overlay (feat-040).
///
/// While this is `Some`, printable digit input goes to the dialog rather than
/// the buffer. Escape restores the original cursor; Enter confirms.
pub struct GoToLineState {
    /// The text the user has typed so far (digits only)
    pub query: String,
    /// Cursor position at the time the dialog was opened, restored on Escape
    pub original_cursor: Position,
    /// Whether the current query parses to a valid, in-range line number
    pub is_valid: bool,
}

/// State for the inline rename dialog (feat-048).
///
/// While this is `Some`, printable input goes to the dialog rather than the buffer.
/// Escape restores the original text; Enter commits all occurrences atomically.
pub struct RenameState {
    /// The text the user has typed as the new name
    pub new_name: String,
    /// Byte offset of the start of the word being renamed in the buffer
    pub word_start: usize,
    /// Byte offset of the end of the original word
    pub word_end: usize,
    /// The original word text, kept so Escape can avoid any buffer mutation
    pub original_word: String,
}

/// A single entry in the right-click context menu (feat-045).
#[derive(Clone)]
pub struct ContextMenuItem {
    /// Human-readable display label
    pub label: String,
    /// Whether this item is inapplicable in the current context
    pub disabled: bool,
    /// True when the item is just a visual divider (label/action are ignored)
    pub is_separator: bool,
}

/// State for the right-click context menu overlay (feat-045).
///
/// While this is `Some`, Escape and mouse-outside dismiss the menu; clicking
/// an enabled item triggers the matching action and dismisses.
pub struct ContextMenuState {
    /// Ordered list of items to show
    pub items: Vec<ContextMenuItem>,
    /// Pixel X coordinate where the menu was opened (relative to editor bounds)
    pub origin_x: f32,
    /// Pixel Y coordinate where the menu was opened (relative to editor bounds)
    pub origin_y: f32,
    /// Index of the currently highlighted item (for keyboard navigation)
    pub highlighted: Option<usize>,
}

/// State for the hover tooltip
#[derive(Clone, Debug)]
pub struct HoverState {
    /// The word being hovered
    pub word: String,
    /// The documentation to display
    pub documentation: String,
    /// The range of the word (for highlighting)
    pub range: std::ops::Range<usize>,
}

/// Public completion menu data for rendering
pub struct CompletionMenuData {
    /// The completion items to display
    pub items: Vec<CompletionItem>,
    /// The currently selected item index
    pub selected_index: usize,
    /// First visible item index within the scrollable window
    pub scroll_offset: usize,
}

impl TextEditor {
    /// Create a new empty text editor
    pub fn new(_window: &mut Window, cx: &mut Context<Self>) -> Self {
        let syntax_highlighter = SyntaxHighlighter::new().ok();
        Self {
            buffer: TextBuffer::empty(),
            cursor: Cursor::new(),
            selection: Selection::new(),
            focus_handle: cx.focus_handle(),
            lsp: Lsp::new(),
            syntax_highlighter,
            cached_highlights: Vec::new(),
            cached_errors: Vec::new(),
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            last_edit_time: None,
            scroll_offset: 0.0,
            last_viewport_lines: 20, // Default estimate
            completion_menu: None,
            hover_state: None,
            find_state: None,
            vim_state: None,
            mouse_drag_anchor: None,
            cached_gutter_width: 0.0,
            cached_char_width: gpui::px(0.0),
            cached_bounds_origin: gpui::Point::default(),
            cached_completion_menu_bounds: None,
            last_click: None,
            inline_suggestion: None,
            ime_marked_range: None,
            extra_cursors: Vec::new(),
            last_select_line_was_extend: false,
            clipboard_is_whole_line: false,
            selection_history: Vec::new(),
            goto_line_state: None,
            soft_wrap: false,
            reference_ranges: Vec::new(),
            rename_state: None,
            context_menu: None,
            folded_lines: std::collections::HashSet::new(),
            fold_regions_cache: Vec::new(),
            cached_fold_chevrons: Vec::new(),
        }
    }

    /// Create a new text editor with initial content
    pub fn with_text(
        text: impl Into<String>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let text = text.into();
        let syntax_highlighter = SyntaxHighlighter::new().ok();
        let mut editor = Self {
            buffer: TextBuffer::new(text.clone()),
            cursor: Cursor::new(),
            selection: Selection::new(),
            focus_handle: cx.focus_handle(),
            lsp: Lsp::new(),
            syntax_highlighter,
            cached_highlights: Vec::new(),
            cached_errors: Vec::new(),
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            last_edit_time: None,
            scroll_offset: 0.0,
            last_viewport_lines: 20, // Default estimate
            completion_menu: None,
            hover_state: None,
            find_state: None,
            vim_state: None,
             mouse_drag_anchor: None,
             cached_gutter_width: 0.0,
             cached_char_width: gpui::px(0.0),
             cached_bounds_origin: gpui::Point::default(),
             cached_completion_menu_bounds: None,
             last_click: None,
            inline_suggestion: None,
            ime_marked_range: None,
            extra_cursors: Vec::new(),
            last_select_line_was_extend: false,
            clipboard_is_whole_line: false,
            selection_history: Vec::new(),
            goto_line_state: None,
            soft_wrap: false,
            reference_ranges: Vec::new(),
            rename_state: None,
            context_menu: None,
            folded_lines: std::collections::HashSet::new(),
            fold_regions_cache: Vec::new(),
            cached_fold_chevrons: Vec::new(),
        };
        // Initial syntax highlighting and diagnostics
        editor.update_syntax_highlights();
        editor.update_diagnostics();
        editor
    }

    // ============================================================================
    // Syntax Highlighting (Phase 5)
    // ============================================================================

    /// Update cached syntax highlights for the current buffer content
    fn update_syntax_highlights(&mut self) {
        if let Some(ref mut highlighter) = self.syntax_highlighter {
            let text = self.buffer.text();
            self.cached_highlights = highlighter.highlight(&text);
        }
        // Keep fold regions consistent with the buffer on every text change.
        self.update_fold_regions();
    }

    /// Get the current syntax highlights
    pub fn get_syntax_highlights(&self) -> &[Highlight] {
        &self.cached_highlights
    }

    /// Check if syntax highlighting is enabled
    pub fn has_syntax_highlighting(&self) -> bool {
        self.syntax_highlighter.is_some()
    }

    /// Get text and syntax highlights for rendering
    pub fn get_text_and_highlights(&self) -> (String, Vec<Highlight>) {
        (self.buffer.text(), self.cached_highlights.clone())
    }

    /// Update cached error diagnostics for the current buffer content
    fn update_diagnostics(&mut self) {
        if let Some(ref mut highlighter) = self.syntax_highlighter {
            let text = self.buffer.text();
            self.cached_errors = highlighter.get_errors(&text);
        }
    }

    /// Get the current error diagnostics (for rendering squiggles)
    pub fn get_diagnostics(&self) -> &[Highlight] {
        &self.cached_errors
    }

    /// Check if there are any errors in the buffer
    pub fn has_errors(&self) -> bool {
        !self.cached_errors.is_empty()
    }

    // ============================================================================
    // Code Folding
    // ============================================================================

    /// Refresh the fold region cache from the current buffer text.
    ///
    /// Called alongside `update_syntax_highlights` so the cache is always
    /// consistent with the current buffer state.
    fn update_fold_regions(&mut self) {
        self.fold_regions_cache = detect_folds(&self.buffer.text());
    }

    /// Returns the detected fold regions for the current buffer content.
    pub fn fold_regions(&self) -> &[FoldRegion] {
        &self.fold_regions_cache
    }

    /// Returns `true` when the fold region starting at `start_line` is collapsed.
    pub fn is_line_folded(&self, start_line: usize) -> bool {
        self.folded_lines.contains(&start_line)
    }

    /// Returns the ordered list of buffer lines that are currently visible on screen.
    ///
    /// Lines hidden inside a collapsed fold region are excluded. The index of each
    /// entry is its "display slot" (the row it occupies on screen); the value is
    /// the corresponding 0-based buffer line index.
    pub fn visible_buffer_lines(&self) -> Vec<usize> {
        let total = self.buffer.line_count();
        let mut hidden = std::collections::HashSet::new();
        for region in &self.fold_regions_cache {
            if self.folded_lines.contains(&region.start_line) {
                for line in (region.start_line + 1)..=region.end_line {
                    if line < total {
                        hidden.insert(line);
                    }
                }
            }
        }
        (0..total).filter(|l| !hidden.contains(l)).collect()
    }

    /// Returns the number of lines that are currently displayed (buffer lines minus
    /// lines hidden inside collapsed folds).
    pub fn display_line_count(&self) -> usize {
        let total = self.buffer.line_count();
        let hidden: usize = self.fold_regions_cache
            .iter()
            .filter(|r| self.folded_lines.contains(&r.start_line))
            .map(|r| r.end_line.saturating_sub(r.start_line).min(total.saturating_sub(r.start_line + 1)))
            .sum();
        total.saturating_sub(hidden)
    }

    /// Toggle the collapsed state of the fold whose start line is `start_line`.
    ///
    /// When collapsing, if the cursor sits inside the region being hidden it is
    /// rescued to the fold's start line so it is never stranded off-screen.
    pub fn toggle_fold(&mut self, start_line: usize, cx: &mut Context<Self>) {
        if self.folded_lines.contains(&start_line) {
            self.folded_lines.remove(&start_line);
        } else {
            self.folded_lines.insert(start_line);
            // Rescue cursor if it lands inside the now-hidden region.
            let cursor_line = self.cursor.position().line;
            let end_line = self.fold_regions_cache
                .iter()
                .find(|r| r.start_line == start_line)
                .map(|r| r.end_line);
            if let Some(end) = end_line {
                if cursor_line > start_line && cursor_line <= end {
                    let col = self.cursor.position().column;
                    let rescued = self.buffer.clamp_position(Position::new(start_line, col));
                    self.cursor.set_position(rescued);
                }
            }
        }
        self.scroll_to_cursor();
        cx.notify();
    }

    /// Collapse every detected fold region.
    ///
    /// Rescues the cursor to the start of its enclosing fold when it would
    /// otherwise be hidden.
    pub fn fold_all(&mut self, cx: &mut Context<Self>) {
        for region in &self.fold_regions_cache {
            self.folded_lines.insert(region.start_line);
        }
        // Rescue cursor from any region it now falls inside.
        let cursor_line = self.cursor.position().line;
        let rescue = self.fold_regions_cache
            .iter()
            .find(|r| cursor_line > r.start_line && cursor_line <= r.end_line)
            .map(|r| r.start_line);
        if let Some(start) = rescue {
            let col = self.cursor.position().column;
            let rescued = self.buffer.clamp_position(Position::new(start, col));
            self.cursor.set_position(rescued);
        }
        self.scroll_to_cursor();
        cx.notify();
    }

    /// Expand every collapsed fold region.
    pub fn unfold_all(&mut self, cx: &mut Context<Self>) {
        self.folded_lines.clear();
        self.scroll_to_cursor();
        cx.notify();
    }

    /// Update the cached fold-chevron hit-test rectangles from the element layer.
    pub(crate) fn update_cached_fold_chevrons(
        &mut self,
        chevrons: Vec<(usize, gpui::Bounds<gpui::Pixels>)>,
    ) {
        self.cached_fold_chevrons = chevrons;
    }

    /// Get the current text content
    pub fn get_text(&self, _cx: &App) -> SharedString {
        self.buffer.text().into()
    }

    /// Set the text content
    pub fn set_text(
        &mut self,
        text: impl Into<SharedString>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
        self.buffer = TextBuffer::new(text.into().to_string());
        self.update_syntax_highlights();
        self.update_diagnostics();
    }

    /// Insert text at the cursor position
    pub fn insert_at_cursor(
        &mut self,
        text: impl Into<String>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
        let offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .unwrap_or(0);

        let text = text.into();
        let text_len = text.len();

        if self.buffer.insert(offset, &text).is_ok() {
            // Move cursor forward by the number of bytes inserted
            if let Ok(new_pos) = self.buffer.offset_to_position(offset + text_len) {
                self.cursor.set_position(new_pos);
            }

            // Update syntax highlights and diagnostics after text change
            self.update_syntax_highlights();
            self.update_diagnostics();

            // Ensure cursor is visible
            self.scroll_to_cursor();
        }
    }

    /// Delete character before cursor (backspace)
    pub fn delete_before_cursor(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        let cursor_pos = self.cursor.position();

        // Can't delete if at start of buffer
        if cursor_pos.line == 0 && cursor_pos.column == 0 {
            return;
        }

        let cursor_offset = self.buffer.position_to_offset(cursor_pos).unwrap_or(0);

        if cursor_offset == 0 {
            return;
        }

        // Find the start of the previous character (handle multi-byte UTF-8)
        let text = self.buffer.text();
        let mut prev_offset = cursor_offset.saturating_sub(1);

        // Walk back to find UTF-8 character boundary
        while prev_offset > 0 && !text.is_char_boundary(prev_offset) {
            prev_offset -= 1;
        }

        // Delete the character
        if self.buffer.delete(prev_offset..cursor_offset).is_ok() {
            // Track change for undo
            if let Some(change) = self.buffer.changes().last() {
                self.push_undo(change.clone());
            }

            // Move cursor back to the deletion point
            if let Ok(new_pos) = self.buffer.offset_to_position(prev_offset) {
                self.cursor.set_position(new_pos);
            }

            // Update syntax highlights and diagnostics after text change
            self.update_syntax_highlights();
            self.update_diagnostics();

            // Ensure cursor is visible
            self.scroll_to_cursor();
        }
    }

    /// Delete character at cursor (delete key)
    pub fn delete_at_cursor(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        let cursor_offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .unwrap_or(0);

        let text = self.buffer.text();

        // Can't delete if at end of buffer
        if cursor_offset >= text.len() {
            return;
        }

        // Find the end of the current character (handle multi-byte UTF-8)
        let mut next_offset = cursor_offset + 1;

        // Walk forward to find UTF-8 character boundary
        while next_offset < text.len() && !text.is_char_boundary(next_offset) {
            next_offset += 1;
        }

        // Delete the character (cursor stays in place)
        if self.buffer.delete(cursor_offset..next_offset).is_ok() {
            // Track change for undo
            if let Some(change) = self.buffer.changes().last() {
                self.push_undo(change.clone());
            }

            // Update syntax highlights and diagnostics after text change
            self.update_syntax_highlights();
            self.update_diagnostics();
        }
    }

    /// Get a reference to the text buffer
    pub fn buffer(&self) -> &TextBuffer {
        &self.buffer
    }

    /// Get a mutable reference to the text buffer
    pub fn buffer_mut(&mut self) -> &mut TextBuffer {
        &mut self.buffer
    }

    /// Get the cursor position
    pub fn get_cursor_position(&self, _cx: &App) -> Position {
        self.cursor.position()
    }

    /// Get the cursor offset (byte offset in buffer)
    pub fn get_cursor_offset(&self, _cx: &App) -> usize {
        self.buffer
            .position_to_offset(self.cursor.position())
            .unwrap_or(0)
    }

    /// Set the cursor offset (byte offset in buffer)
    pub fn set_cursor_offset(
        &mut self,
        offset: usize,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
        if let Ok(position) = self.buffer.offset_to_position(offset) {
            self.cursor.set_position(position);
        }
    }

    /// Set the cursor position
    pub fn set_cursor_position(
        &mut self,
        position: Position,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
        self.cursor
            .set_position(self.buffer.clamp_position(position));
    }

    // ============================================================================
    // Cursor Movement
    // ============================================================================

    /// Move cursor left by one character
    pub fn move_cursor_left(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.cursor.move_left(&self.buffer);
        self.scroll_to_cursor();
    }

    /// Move cursor right by one character
    pub fn move_cursor_right(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.cursor.move_right(&self.buffer);
        self.scroll_to_cursor();
    }

    /// Move cursor up by one line
    pub fn move_cursor_up(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.cursor.move_up(&self.buffer);
        self.scroll_to_cursor();
    }

    /// Move cursor down by one line
    pub fn move_cursor_down(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.cursor.move_down(&self.buffer);
        self.scroll_to_cursor();
    }

    /// Move cursor to start of line
    pub fn move_cursor_to_line_start(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.cursor.move_to_line_start();
        self.scroll_to_cursor();
    }

    /// Move cursor to end of line
    pub fn move_cursor_to_line_end(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.cursor.move_to_line_end(&self.buffer);
        self.scroll_to_cursor();
    }

    /// Move cursor to start of document
    pub fn move_cursor_to_document_start(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.cursor.move_to_document_start();
        self.scroll_to_cursor();
    }

    /// Move cursor to end of document
    pub fn move_cursor_to_document_end(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.cursor.move_to_document_end(&self.buffer);
        self.scroll_to_cursor();
    }

    /// Move cursor to next word
    pub fn move_cursor_to_next_word(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.cursor.move_to_next_word_start(&self.buffer);
        self.scroll_to_cursor();
    }

    /// Move cursor to previous word
    pub fn move_cursor_to_prev_word(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.cursor.move_to_prev_word_start(&self.buffer);
        self.scroll_to_cursor();
    }

    // ============================================================================
    // Paragraph movement (feat-002)
    // ============================================================================

    /// Move the cursor to the first line of the previous paragraph.
    ///
    /// A paragraph boundary is defined as the transition between blank and
    /// non-blank lines. This matches the behavior of most prose editors: jumping
    /// backward skips any blank separator lines and lands on the start of the
    /// preceding non-blank block.
    fn move_to_paragraph_start(&mut self) {
        let total_lines = self.buffer.line_count();
        if total_lines == 0 {
            return;
        }
        let is_blank = |l: usize| -> bool {
            self.buffer
                .line(l)
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
        };
        let mut line = self.cursor.position().line;
        // Step backward past the current non-blank block to reach the blank gap
        while line > 0 && !is_blank(line) {
            line -= 1;
        }
        // Step backward past blank separator lines
        while line > 0 && is_blank(line) {
            line -= 1;
        }
        // Walk back to the first line of that paragraph
        while line > 0 && !is_blank(line - 1) {
            line -= 1;
        }
        self.cursor
            .set_position(crate::buffer::Position::new(line, 0));
        self.scroll_to_cursor();
    }

    /// Move the cursor to the first line of the next paragraph.
    fn move_to_paragraph_end(&mut self) {
        let total_lines = self.buffer.line_count();
        if total_lines == 0 {
            return;
        }
        let is_blank = |l: usize| -> bool {
            self.buffer
                .line(l)
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
        };
        let last_line = total_lines.saturating_sub(1);
        let mut line = self.cursor.position().line;
        // Step forward past the current non-blank block
        while line < last_line && !is_blank(line) {
            line += 1;
        }
        // Step forward past blank separator lines
        while line < last_line && is_blank(line) {
            line += 1;
        }
        self.cursor
            .set_position(crate::buffer::Position::new(line, 0));
        self.scroll_to_cursor();
    }

    // ============================================================================
    // Subword movement (feat-003)
    // ============================================================================

    /// Returns the byte offset of the end of the next subword starting at `offset`.
    ///
    /// Subword boundaries follow these rules (highest priority first):
    /// - Underscore runs: each `_` run is its own subword
    /// - Non-word-character runs (punctuation, whitespace, etc.)
    /// - ALL-CAPS runs: `FOO` or `FOOBar` — the caps form one subword, stopping
    ///   one char before a trailing lowercase suffix (`FOO` in `FOOBar`)
    /// - Title-case words starting with a single uppercase: `FooBar` → `Foo`
    /// - Lowercase/digit runs
    fn next_subword_end_in_text(text: &str, offset: usize) -> usize {
        let tail = &text[offset..];
        let mut chars = tail.char_indices().peekable();

        let (_, first_char) = match chars.next() {
            Some(pair) => pair,
            None => return offset,
        };

        if first_char == '_' {
            // Consume the entire run of underscores
            while chars.peek().map(|(_, c)| *c == '_').unwrap_or(false) {
                chars.next();
            }
        } else if !first_char.is_alphanumeric() {
            // Consume run of non-alphanumeric, non-underscore characters
            while chars
                .peek()
                .map(|(_, c)| !c.is_alphanumeric() && *c != '_')
                .unwrap_or(false)
            {
                chars.next();
            }
        } else if first_char.is_uppercase() {
            // Peek at second char to decide ALL-CAPS vs Title-case
            let second_is_upper = chars.peek().map(|(_, c)| c.is_uppercase()).unwrap_or(false);
            if second_is_upper {
                // ALL-CAPS run: consume uppers, but stop one before a lowercase follows
                let mut prev_idx = 0;
                let mut prev_was_upper = true;
                loop {
                    match chars.peek() {
                        Some(&(idx, c)) if c.is_uppercase() => {
                            prev_idx = idx;
                            prev_was_upper = true;
                            chars.next();
                        }
                        Some(&(_, c)) if c.is_lowercase() || c.is_ascii_digit() => {
                            // Back up by one if there was a preceding uppercase
                            if prev_was_upper && prev_idx > 0 {
                                return offset + prev_idx;
                            }
                            break;
                        }
                        _ => break,
                    }
                }
            } else {
                // Title-case: consume one uppercase then all following lowercase/digits
                while chars
                    .peek()
                    .map(|(_, c)| c.is_lowercase() || c.is_ascii_digit())
                    .unwrap_or(false)
                {
                    chars.next();
                }
            }
        } else {
            // Lowercase or digit run — consume all lowercase and digits
            while chars
                .peek()
                .map(|(_, c)| c.is_lowercase() || c.is_ascii_digit())
                .unwrap_or(false)
            {
                chars.next();
            }
        }

        // The end is either the next char's index or the end of the string
        match chars.peek() {
            Some(&(idx, _)) => offset + idx,
            None => text.len(),
        }
    }

    /// Returns the byte offset of the start of the subword ending at `offset`.
    fn prev_subword_start_in_text(text: &str, offset: usize) -> usize {
        if offset == 0 {
            return 0;
        }
        let head = &text[..offset];
        // Collect chars in reverse for clean pattern matching
        let chars: Vec<char> = head.chars().collect();
        let mut idx = chars.len();

        let first = chars[idx - 1];

        if first == '_' {
            while idx > 0 && chars[idx - 1] == '_' {
                idx -= 1;
            }
        } else if !first.is_alphanumeric() {
            while idx > 0 && !chars[idx - 1].is_alphanumeric() && chars[idx - 1] != '_' {
                idx -= 1;
            }
        } else if first.is_uppercase() {
            // Single uppercase at end — consume run of uppers
            while idx > 0 && chars[idx - 1].is_uppercase() {
                idx -= 1;
            }
        } else {
            // Lowercase/digit run — consume, then optionally one leading uppercase (Title-case)
            while idx > 0 && (chars[idx - 1].is_lowercase() || chars[idx - 1].is_ascii_digit()) {
                idx -= 1;
            }
            if idx > 0 && chars[idx - 1].is_uppercase() {
                idx -= 1;
            }
        }

        // Convert char index back to byte offset
        head.char_indices()
            .nth(idx)
            .map(|(byte_idx, _)| byte_idx)
            .unwrap_or(0)
    }

    fn move_to_next_subword(&mut self) {
        let offset = self.cursor_byte_offset();
        let text = self.buffer.text();
        let new_offset = Self::next_subword_end_in_text(&text, offset);
        if let Ok(pos) = self.buffer.offset_to_position(new_offset) {
            self.cursor.set_position(pos);
            self.scroll_to_cursor();
        }
    }

    fn move_to_prev_subword(&mut self) {
        let offset = self.cursor_byte_offset();
        let text = self.buffer.text();
        let new_offset = Self::prev_subword_start_in_text(&text, offset);
        if let Ok(pos) = self.buffer.offset_to_position(new_offset) {
            self.cursor.set_position(pos);
            self.scroll_to_cursor();
        }
    }

    fn delete_subword_left(&mut self, cx: &mut Context<Self>) {
        let offset = self.cursor_byte_offset();
        let text = self.buffer.text();
        let start = Self::prev_subword_start_in_text(&text, offset);
        if start < offset {
            self.break_undo_group();
            if self.buffer.delete(start..offset).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
                if let Ok(pos) = self.buffer.offset_to_position(start) {
                    self.cursor.set_position(pos);
                    self.selection.set_position(pos);
                }
                self.update_syntax_highlights();
                cx.notify();
            }
        }
    }

    fn delete_subword_right(&mut self, cx: &mut Context<Self>) {
        let offset = self.cursor_byte_offset();
        let text = self.buffer.text();
        let end = Self::next_subword_end_in_text(&text, offset);
        if end > offset {
            self.break_undo_group();
            if self.buffer.delete(offset..end).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
                self.update_syntax_highlights();
                cx.notify();
            }
        }
    }

    /// Get the current scroll offset (in lines)
    pub fn scroll_offset(&self) -> f32 {
        self.scroll_offset
    }

    /// Set the scroll offset (in display lines).
    ///
    /// Clamped to `[0, display_line_count]` so that collapsing folds never
    /// leaves the viewport scrolled past the end of visible content.
    pub fn set_scroll_offset(&mut self, offset: f32) {
        let max_offset = self.display_line_count() as f32;
        self.scroll_offset = offset.clamp(0.0, max_offset);
    }

    /// Scroll by a delta (positive = down, negative = up)
    pub fn scroll_by(&mut self, delta: f32) {
        self.set_scroll_offset(self.scroll_offset + delta);
    }

    /// Update the viewport size (called during render)
    /// This allows auto-scroll to work correctly
    pub(crate) fn update_viewport_lines(&mut self, viewport_lines: usize) {
        self.last_viewport_lines = viewport_lines;
    }

    /// Cache the gutter width computed during the last render pass so that mouse
    /// event handlers (which run outside the element's paint phase) can correctly
    /// subtract it when converting screen-space X coordinates to column indices.
    pub(crate) fn update_cached_gutter_width(&mut self, gutter_width: f32) {
        self.cached_gutter_width = gutter_width;
    }

    /// Cache the monospace character width measured during the last render pass.
    ///
    /// Using the render-time measurement for mouse hit-testing ensures the same
    /// value is used for both painting and column-index calculations, preventing
    /// any per-call shaping variance from shifting the resolved column.
    pub(crate) fn update_cached_char_width(&mut self, char_width: gpui::Pixels) {
        self.cached_char_width = char_width;
    }

    /// Cache the element's top-left corner in window space so that mouse
    /// handlers can subtract it from raw window-coordinate event positions.
    pub(crate) fn update_cached_bounds_origin(
        &mut self,
        origin: gpui::Point<gpui::Pixels>,
    ) {
        self.cached_bounds_origin = origin;
    }

    /// Cache the last-painted completion menu layout so that `handle_mouse_down`
    /// can determine whether a click landed inside the menu without accessing
    /// element-layer data. Pass `None` when the menu is not visible.
    pub(crate) fn update_cached_completion_menu_bounds(
        &mut self,
        bounds: Option<CachedCompletionMenuBounds>,
    ) {
        self.cached_completion_menu_bounds = bounds;
    }

    /// Convert an absolute screen-space point into a `Position` (line, column)
    /// inside the buffer, clamped to valid bounds.
    ///
    /// `bounds_origin` is the top-left of the editor element in screen space.
    fn pixel_to_position(
        &self,
        point: gpui::Point<Pixels>,
        bounds_origin: gpui::Point<Pixels>,
        line_height: Pixels,
    ) -> Position {
        let relative_x = (point.x - bounds_origin.x - px(self.cached_gutter_width)).max(px(0.0));
        let relative_y = (point.y - bounds_origin.y).max(px(0.0));

        // Y maps to a display slot; then we look up the corresponding buffer line.
        let display_slot = ((relative_y / line_height) + self.scroll_offset) as usize;
        let display_lines = self.visible_buffer_lines();
        let buffer_line = display_lines
            .get(display_slot)
            .or_else(|| display_lines.last())
            .copied()
            .unwrap_or(0);

        let column = (relative_x / self.cached_char_width).max(0.0) as usize;
        self.buffer.clamp_position(Position::new(buffer_line, column))
    }

    /// Scroll to ensure the cursor is visible.
    ///
    /// Works in display-line space: the cursor's buffer line is first translated
    /// to its display slot (its row index among non-hidden lines), then the
    /// viewport is shifted if that slot falls outside the currently visible window.
    pub fn scroll_to_cursor(&mut self) {
        let viewport_lines = self.last_viewport_lines;
        let cursor_buffer_line = self.cursor.position().line;

        // Find the cursor's display slot. Fall back to the buffer line index
        // (identical when no folds are active) if it isn't in the display list.
        let display_lines = self.visible_buffer_lines();
        let cursor_display = display_lines
            .iter()
            .position(|&b| b == cursor_buffer_line)
            .unwrap_or(cursor_buffer_line) as f32;

        let visible_start = self.scroll_offset;
        let visible_end = self.scroll_offset + viewport_lines as f32;

        if cursor_display < visible_start {
            self.scroll_offset = cursor_display;
        } else if cursor_display >= visible_end {
            self.scroll_offset = cursor_display - viewport_lines as f32 + 1.0;
        }

        // Clamp using the current display line count.
        let max_offset = display_lines.len() as f32;
        self.scroll_offset = self.scroll_offset.clamp(0.0, max_offset);
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
        &self.selection
    }

    /// Check if there is an active selection
    pub fn has_selection(&self) -> bool {
        self.selection.has_selection()
    }

    /// Return all extra cursor positions and their selections for rendering.
    ///
    /// Each entry is `(position, selection)`.  The primary cursor is NOT
    /// included — callers should query `get_cursor_position` / `selection`
    /// separately for the primary.
    pub fn extra_cursor_selections(&self) -> &[(Cursor, Selection)] {
        &self.extra_cursors
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
        let text = self.buffer.text();
        let cursor_offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .unwrap_or(0);

        // Stack of (open_char, open_byte_offset) for currently open brackets.
        let mut stack: Vec<(char, usize)> = Vec::new();
        // Completed enclosing pairs (open_offset, close_offset).
        let mut enclosing: Vec<(usize, usize)> = Vec::new();

        for (byte_offset, ch) in text.char_indices() {
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
                    if stack.last().map(|(c, _)| *c) == Some(expected_open) {
                        if let Some((_, open_offset)) = stack.pop() {
                            // This pair encloses the cursor if the open is before the cursor
                            // and the close is at or after it.
                            if open_offset < cursor_offset && byte_offset >= cursor_offset {
                                enclosing.push((open_offset, byte_offset));
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // `enclosing` is filled in close→open order because we record pairs when
        // we encounter the closing bracket.  Reverse so the result is outermost-first.
        enclosing.reverse();
        enclosing
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
        let text = self.buffer.text();
        match text[cursor_offset..].chars().next() {
            None => true, // end of buffer
            Some(c) => matches!(c, ' ' | '\t' | '\n' | '\r' | ')' | ']' | '}' | '\'' | '"' | ',' | ';'),
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
        if !self.selection.has_selection() {
            return false;
        }

        let range = self.selection.range();
        let Ok(start_offset) = self.buffer.position_to_offset(range.start) else {
            return false;
        };
        let Ok(end_offset) = self.buffer.position_to_offset(range.end) else {
            return false;
        };

        // Insert closer first so the start offset stays valid.
        let mut closer_str = String::new();
        closer_str.push(closer);
        let mut opener_str = String::new();
        opener_str.push(opener);

        self.break_undo_group();

        if self.buffer.insert(end_offset, &closer_str).is_ok() {
            if let Some(change) = self.buffer.changes().last() {
                self.push_undo(change.clone());
            }
        }
        if self.buffer.insert(start_offset, &opener_str).is_ok() {
            if let Some(change) = self.buffer.changes().last() {
                self.push_undo(change.clone());
            }
        }

        // Keep the selection covering the wrapped text (shift right by 1 for the opener).
        let new_start = range.start.column + 1;
        let new_end_col = if range.start.line == range.end.line {
            range.end.column + 1
        } else {
            range.end.column
        };

        let anchor = Position::new(range.start.line, new_start);
        let head = Position::new(range.end.line, new_end_col);
        self.selection = Selection::from_anchor_head(anchor, head);
        self.cursor.set_position(head);

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

        if !self.is_safe_auto_close_position(cursor_offset) {
            return false;
        }

        let mut pair = String::new();
        pair.push(opener);
        pair.push(closer);

        self.break_undo_group();
        if self.buffer.insert(cursor_offset, &pair).is_ok() {
            if let Some(change) = self.buffer.changes().last() {
                self.push_undo(change.clone());
            }
            // Position cursor between the two inserted characters.
            if let Ok(between) = self.buffer.offset_to_position(cursor_offset + opener.len_utf8()) {
                self.cursor.set_position(between);
                self.selection.set_position(between);
            }
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
        let cursor_offset = self.cursor_byte_offset();
        let text = self.buffer.text();
        match text[cursor_offset..].chars().next() {
            Some(c) if c == closer => {
                if let Ok(new_pos) =
                    self.buffer.offset_to_position(cursor_offset + closer.len_utf8())
                {
                    self.cursor.set_position(new_pos);
                    self.selection.set_position(new_pos);
                    self.scroll_to_cursor();
                    cx.notify();
                }
                true
            }
            _ => false,
        }
    }

    /// Returns true when the selection spans more than one line.
    fn is_multiline_selection(&self) -> bool {
        if !self.selection.has_selection() {
            return false;
        }
        let range = self.selection.range();
        range.start.line != range.end.line
    }

    // ============================================================================
    // Selection history (feat-019 / feat-020)
    // ============================================================================

    /// Capture a snapshot of the current cursor + selection state so that
    /// `undo_selection` can walk back to it.  Capped at 100 entries to prevent
    /// unbounded growth — oldest entries are evicted first.
    fn push_selection_snapshot(&mut self) {
        self.selection_history.push((
            self.cursor.clone(),
            self.selection.clone(),
            self.extra_cursors.clone(),
        ));
        if self.selection_history.len() > 100 {
            self.selection_history.remove(0);
        }
    }

    /// Split the current multi-line selection so that each selected line gets
    /// its own cursor at the end of that line (feat-019).
    ///
    /// The primary cursor lands at the end of the first selected line; extra
    /// cursors cover every subsequent line up to the last.  This mirrors VS
    /// Code's Cmd+Shift+L behaviour when a multi-line region is already active.
    fn split_selection_into_lines(&mut self) {
        if !self.is_multiline_selection() {
            return;
        }
        self.push_selection_snapshot();

        let range = self.selection.range();
        let first_line = range.start.line;
        let last_line = range.end.line;

        // Primary cursor: spans from the selection-start column on the first
        // line to the end of that same line.
        let first_line_len = self
            .buffer
            .line(first_line)
            .map(|l| l.len())
            .unwrap_or(0);
        let first_head = Position::new(first_line, first_line_len);
        let first_anchor = Position::new(first_line, range.start.column);
        self.cursor.set_position(first_head);
        self.selection = Selection::from_anchor_head(first_anchor, first_head);

        // Extra cursors: one per remaining line, spanning the whole line (or
        // just up to `range.end.column` on the last line).
        self.extra_cursors.clear();
        for line in (first_line + 1)..=last_line {
            let end_col = if line == last_line {
                range.end.column
            } else {
                self.buffer.line(line).map(|l| l.len()).unwrap_or(0)
            };
            let head = Position::new(line, end_col);
            let anchor = Position::new(line, 0);
            self.extra_cursors
                .push((Cursor::at(head), Selection::from_anchor_head(anchor, head)));
        }

        self.normalize_extra_cursors();
        self.scroll_to_cursor();
    }

    /// Step back through the selection history without modifying the text
    /// (feat-020).  Each call pops one snapshot, restoring the cursor and
    /// selection to where they were before the most recent selection action.
    fn undo_selection(&mut self) {
        let Some((saved_cursor, saved_selection, saved_extras)) =
            self.selection_history.pop()
        else {
            return;
        };
        self.cursor = saved_cursor;
        self.selection = saved_selection;
        self.extra_cursors = saved_extras;
        self.scroll_to_cursor();
    }

    /// Get selected text
    pub fn get_selected_text(&self, _cx: &App) -> Option<SharedString> {
        if !self.selection.has_selection() {
            return None;
        }

        let range = self.selection.range();
        let start_offset = self.buffer.position_to_offset(range.start).ok()?;
        let end_offset = self.buffer.position_to_offset(range.end).ok()?;

        let text = self.buffer.text();
        let selected = text[start_offset..end_offset].to_string();
        Some(selected.into())
    }

    /// Select all text
    pub fn select_all(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        let buffer_end = Position::new(
            self.buffer.line_count().saturating_sub(1),
            self.buffer
                .line(self.buffer.line_count().saturating_sub(1))
                .map(|l| l.len())
                .unwrap_or(0),
        );
        self.selection.select_all(buffer_end);
        self.cursor.set_position(buffer_end);
    }

    /// Clear the selection
    pub fn clear_selection(&mut self) {
        self.selection.clear();
    }

    // ============================================================================
    // Multi-cursor infrastructure (feat-017/018/021/022)
    // ============================================================================

    /// Return a snapshot of all active cursors as `(position, selection)` pairs.
    ///
    /// The primary cursor is always first; extra cursors follow in the order
    /// they were added.
    pub fn all_cursor_selections(&self) -> Vec<(Position, Selection)> {
        let mut result = vec![(self.cursor.position(), self.selection.clone())];
        for (cur, sel) in &self.extra_cursors {
            result.push((cur.position(), sel.clone()));
        }
        result
    }

    /// Collapse all extra cursors and clear all selections, keeping only the
    /// primary cursor in place.  Call this whenever an action that cannot
    /// meaningfully be replicated across multiple cursors is triggered (e.g.
    /// Cmd+Z undo, line-move operations).
    fn collapse_to_primary_cursor(&mut self) {
        self.extra_cursors.clear();
        self.selection.clear();
    }

    /// Add an extra cursor at `position`.  Duplicate positions are silently
    /// ignored to avoid stacking invisible cursors on top of each other.
    fn add_extra_cursor(&mut self, position: Position, with_selection: Option<Selection>) {
        let already_exists = self.cursor.position() == position
            || self.extra_cursors.iter().any(|(c, _)| c.position() == position);
        if already_exists {
            return;
        }
        let cursor = Cursor::at(position);
        let selection = with_selection.unwrap_or_else(|| Selection::at(position));
        self.extra_cursors.push((cursor, selection));
    }

    /// Remove any extra cursors that have the same position as another cursor
    /// (primary or extra), and sort extras top-to-bottom for deterministic
    /// iteration order during edits.
    fn normalize_extra_cursors(&mut self) {
        self.extra_cursors
            .sort_by_key(|(c, _)| (c.position().line, c.position().column));
        self.extra_cursors.dedup_by_key(|(c, _)| c.position());
        // Also remove any that collide with the primary.
        let primary = self.cursor.position();
        self.extra_cursors.retain(|(c, _)| c.position() != primary);
    }

    /// Find all byte ranges in the buffer where `needle` occurs.  Returns
    /// ranges in document order.
    fn find_all_occurrences(&self, needle: &str) -> Vec<std::ops::Range<usize>> {
        if needle.is_empty() {
            return Vec::new();
        }
        let text = self.buffer.text();
        let mut ranges = Vec::new();
        let mut search_start = 0;
        while search_start <= text.len() {
            match text[search_start..].find(needle) {
                Some(relative_offset) => {
                    let start = search_start + relative_offset;
                    let end = start + needle.len();
                    ranges.push(start..end);
                    // Advance past this match — avoid infinite loops on empty needle
                    search_start = end.max(start + 1);
                }
                None => break,
            }
        }
        ranges
    }

    /// Return the selected text as a `String`, or the word under the primary
    /// cursor if there is no active selection.  Returns `None` when the cursor
    /// is not on any word and there is no selection.
    fn selection_or_word_under_cursor(&self) -> Option<String> {
        if self.selection.has_selection() {
            let range = self.selection.range();
            let start = self.buffer.position_to_offset(range.start).unwrap_or(0);
            let end = self.buffer.position_to_offset(range.end).unwrap_or(0);
            let text = self.buffer.text();
            Some(text[start..end].to_string())
        } else {
            let offset = self.cursor_byte_offset();
            let word_range = self.find_word_range_at_offset(offset)?;
            let text = self.buffer.text();
            Some(text[word_range].to_string())
        }
    }

    /// Delete selected text (if any selection exists)
    fn delete_selection(&mut self) -> bool {
        if !self.selection.has_selection() {
            return false;
        }

        let range = self.selection.range();
        let start_offset = self.buffer.position_to_offset(range.start).unwrap_or(0);
        let end_offset = self.buffer.position_to_offset(range.end).unwrap_or(0);

        if self.buffer.delete(start_offset..end_offset).is_ok() {
            // Track change for undo
            if let Some(change) = self.buffer.changes().last() {
                self.push_undo(change.clone());
            }

            // Move cursor to start of selection
            self.cursor.set_position(range.start);
            self.selection.clear();
            true
        } else {
            false
        }
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

        // Paste is always its own undo step, separate from any preceding typing.
        self.break_undo_group();

        // Delete the selection if present, capturing old text for the undo entry.
        let (delete_offset, deleted_text) = if self.has_selection() {
            let range = self.selection.range();
            let start = self.buffer.position_to_offset(range.start).unwrap_or(0);
            let end = self.buffer.position_to_offset(range.end).unwrap_or(0);
            if self.buffer.delete(start..end).is_ok() {
                let old = self
                    .buffer
                    .changes()
                    .last()
                    .map(|c| c.old_text.clone())
                    .unwrap_or_default();
                if let Ok(pos) = self.buffer.offset_to_position(start) {
                    self.cursor.set_position(pos);
                    self.selection.clear();
                }
                (start, old)
            } else {
                (self.cursor_byte_offset(), String::new())
            }
        } else {
            (self.cursor_byte_offset(), String::new())
        };

        self.insert_at_cursor(&clipboard_text, window, cx);

        // Record the entire paste (optional selection delete + insert) as one undo entry.
        self.push_undo(buffer::Change::replace(
            delete_offset,
            deleted_text,
            clipboard_text,
        ));
    }

    // ============================================================================
    // Undo/Redo (Phase 4)
    // ============================================================================

    /// Undo the last change group (Ctrl+Z).
    /// Returns true if undo was performed, false if nothing to undo.
    pub fn undo(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> bool {
        let Some(group) = self.undo_stack.pop() else {
            return false;
        };

        let mut redo_group = Vec::with_capacity(group.len());

        // Apply each change's inverse in reverse order so the buffer ends up
        // in the pre-group state regardless of how many changes were grouped.
        for change in group.into_iter().rev() {
            let inverse = change.inverse();
            if self.buffer.apply_change(&inverse).is_ok() {
                redo_group.push(change);
                if let Ok(position) = self.buffer.offset_to_position(inverse.offset) {
                    self.cursor.set_position(position);
                    self.selection.set_position(position);
                }
            } else {
                // Restore what we already undid back into the redo group and bail.
                // This is a safety valve; in practice apply_change should not fail.
                redo_group.reverse();
                self.redo_stack.push(redo_group);
                return false;
            }
        }

        redo_group.reverse();
        self.redo_stack.push(redo_group);
        self.scroll_to_cursor();
        true
    }

    /// Redo a previously undone change group (Ctrl+Shift+Z).
    /// Returns true if redo was performed, false if nothing to redo.
    pub fn redo(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> bool {
        let Some(group) = self.redo_stack.pop() else {
            return false;
        };

        let mut undo_group = Vec::with_capacity(group.len());

        for change in group.into_iter() {
            let new_end_offset = change.new_range().end;
            if self.buffer.apply_change(&change).is_ok() {
                if let Ok(position) = self.buffer.offset_to_position(new_end_offset) {
                    self.cursor.set_position(position);
                    self.selection.set_position(position);
                }
                undo_group.push(change);
            } else {
                undo_group.reverse();
                self.undo_stack.push(undo_group);
                return false;
            }
        }

        self.undo_stack.push(undo_group);
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
        self.redo_stack.clear();

        let now = std::time::Instant::now();
        let is_single_char_insert =
            change.new_text.chars().count() == 1 && change.old_text.is_empty();
        let is_single_char_delete =
            change.old_text.chars().count() == 1 && change.new_text.is_empty();

        let within_window = self
            .last_edit_time
            .map(|t| now.duration_since(t).as_millis() < 300)
            .unwrap_or(false);

        // Merge consecutive same-kind single-character edits within the 300 ms typing window
        // so that undo reverts a natural word-sized chunk rather than one keystroke at a time.
        // Insertions only merge with insertions; deletions only merge with deletions — switching
        // direction always starts a new group.
        if (is_single_char_insert || is_single_char_delete) && within_window {
            if let Some(last_group) = self.undo_stack.last_mut() {
                let last_is_same_kind = last_group
                    .last()
                    .map(|last: &buffer::Change| {
                        if is_single_char_insert {
                            last.is_insertion()
                        } else {
                            last.is_deletion()
                        }
                    })
                    .unwrap_or(false);

                if last_is_same_kind {
                    last_group.push(change);
                    self.last_edit_time = Some(now);
                    return;
                }
            }
        }

        self.undo_stack.push(vec![change]);
        self.last_edit_time = Some(now);
    }

    /// Break the current undo group unconditionally.
    ///
    /// Called before structural edits (move-line, delete-line, etc.) so that
    /// each command gets its own undo step even if it happens to follow a recent
    /// typing burst.
    fn break_undo_group(&mut self) {
        self.last_edit_time = None;
    }

    // ============================================================================
    // Smart Home (feat-001)
    // ============================================================================

    /// Implement the two-stop Home key behaviour.
    ///
    /// First press: move to the first non-whitespace column on the current line.
    /// If the cursor is already there (or the line is all whitespace), move to column 0.
    /// When `extend_selection` is true the selection is extended instead of cleared.
    fn smart_home(&mut self, extend_selection: bool) {
        let line = self.cursor.position().line;
        let current_col = self.cursor.position().column;

        let first_nonws_col = self
            .buffer
            .line(line)
            .map(|text| {
                text.chars()
                    .take_while(|c| c.is_whitespace())
                    .count()
            })
            .unwrap_or(0);

        let target_col = if current_col != first_nonws_col {
            first_nonws_col
        } else {
            0
        };

        let new_pos = Position::new(line, target_col);

        if extend_selection {
            if !self.selection.has_selection() {
                self.selection.start_selection(self.cursor.position());
            }
            self.cursor.set_position(new_pos);
            self.selection.extend_to(new_pos);
        } else {
            self.cursor.set_position(new_pos);
            self.selection.set_position(new_pos);
        }
        self.scroll_to_cursor();
    }

    // ============================================================================
    // Line editing helpers (feat-005 through feat-015)
    // ============================================================================

    /// Return the (first_line, last_line) range of lines touched by the current
    /// cursor or selection (inclusive, 0-indexed).
    fn selected_line_range(&self) -> (usize, usize) {
        if self.selection.has_selection() {
            let range = self.selection.range();
            (range.start.line, range.end.line)
        } else {
            let line = self.cursor.position().line;
            (line, line)
        }
    }

    /// Move the selected line block up by one line (feat-005).
    fn move_lines_up(&mut self) {
        self.break_undo_group();
        let (first, last) = self.selected_line_range();
        if first == 0 {
            return;
        }
        let line_count = self.buffer.line_count();
        if last >= line_count {
            return;
        }

        // Collect the text of all selected lines including their trailing newlines.
        let block_text = self.collect_lines_text(first, last);
        // Text of the line above (without its newline, we'll re-add it).
        let above_line = match self.buffer.line(first - 1) {
            Some(t) => t,
            None => return,
        };

        // The region we are swapping: from start of above_line to end of last selected line.
        let region_start = self
            .buffer
            .position_to_offset(Position::new(first - 1, 0))
            .unwrap_or(0);
        let last_line_len = self.buffer.line(last).map(|l| l.len()).unwrap_or(0);
        let region_end = self
            .buffer
            .position_to_offset(Position::new(last, last_line_len))
            .unwrap_or(0);

        // Build replacement: block_text first, then above_line.
        // The block_text already ends with '\n'; we append the above line text.
        let replacement = format!("{}{}", block_text, above_line);

        if self
            .buffer
            .delete(region_start..region_end)
            .is_ok()
        {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }
        if self.buffer.insert(region_start, &replacement).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }

        // Move cursor to match the block's new position (one line up).
        let new_cursor_line = self.cursor.position().line.saturating_sub(1);
        let new_cursor_col = self.cursor.position().column.min(
            self.buffer.line(new_cursor_line).map(|l| l.len()).unwrap_or(0),
        );
        let new_pos = Position::new(new_cursor_line, new_cursor_col);
        self.cursor.set_position(new_pos);
        self.selection.set_position(new_pos);

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Move the selected line block down by one line (feat-005).
    fn move_lines_down(&mut self) {
        self.break_undo_group();
        let (first, last) = self.selected_line_range();
        let line_count = self.buffer.line_count();
        if last + 1 >= line_count {
            return;
        }

        let block_text = self.collect_lines_text(first, last);
        let below_line = match self.buffer.line(last + 1) {
            Some(t) => t,
            None => return,
        };

        let region_start = self
            .buffer
            .position_to_offset(Position::new(first, 0))
            .unwrap_or(0);
        let below_line_len = below_line.len();
        let region_end = self
            .buffer
            .position_to_offset(Position::new(last + 1, below_line_len))
            .unwrap_or(0);

        // Replacement: below_line first, then newline, then block_text.
        let replacement = format!("{}\n{}", below_line, block_text.trim_end_matches('\n'));
        // Preserve trailing newline if the original block had one.
        let replacement = if block_text.ends_with('\n') {
            format!("{}\n", replacement)
        } else {
            replacement
        };

        if self.buffer.delete(region_start..region_end).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }
        if self.buffer.insert(region_start, &replacement).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }

        let new_cursor_line = (self.cursor.position().line + 1).min(self.buffer.line_count().saturating_sub(1));
        let new_cursor_col = self.cursor.position().column.min(
            self.buffer.line(new_cursor_line).map(|l| l.len()).unwrap_or(0),
        );
        let new_pos = Position::new(new_cursor_line, new_cursor_col);
        self.cursor.set_position(new_pos);
        self.selection.set_position(new_pos);

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Duplicate the current line (or selected block) downward (feat-006).
    ///
    /// The cursor moves to the duplicate (one line below).
    fn duplicate_lines_down(&mut self) {
        self.break_undo_group();
        let (first, last) = self.selected_line_range();
        let block_text = self.collect_lines_text(first, last);

        // Insert the duplicate immediately after the last selected line.
        let insert_line = last + 1;
        let insert_offset = if insert_line < self.buffer.line_count() {
            self.buffer
                .position_to_offset(Position::new(insert_line, 0))
                .unwrap_or(0)
        } else {
            // Append after last line — need to add a newline first.
            let buf_len = self.buffer.len();
            let text = self.buffer.text();
            if !text.ends_with('\n') {
                if self.buffer.insert(buf_len, "\n").is_ok() {
                    if let Some(change) = self.buffer.changes().last().cloned() {
                        self.push_undo(change);
                    }
                }
            }
            self.buffer.len()
        };

        let to_insert = if block_text.ends_with('\n') {
            block_text
        } else {
            format!("{}\n", block_text)
        };

        if self.buffer.insert(insert_offset, &to_insert).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }

        let block_line_count = last - first + 1;
        let new_line = first + block_line_count;
        let col = self.cursor.position().column.min(
            self.buffer.line(new_line).map(|l| l.len()).unwrap_or(0),
        );
        let new_pos = Position::new(new_line, col);
        self.cursor.set_position(new_pos);
        self.selection.set_position(new_pos);

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Duplicate the current line (or selected block) upward (feat-006).
    ///
    /// The cursor stays on the original line.
    fn duplicate_lines_up(&mut self) {
        self.break_undo_group();
        let (first, last) = self.selected_line_range();
        let block_text = self.collect_lines_text(first, last);

        let insert_offset = self
            .buffer
            .position_to_offset(Position::new(first, 0))
            .unwrap_or(0);

        let to_insert = if block_text.ends_with('\n') {
            block_text
        } else {
            format!("{}\n", block_text)
        };

        if self.buffer.insert(insert_offset, &to_insert).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }

        // Cursor stays on the original line which is now shifted down by block_line_count.
        let block_line_count = last - first + 1;
        let new_line = first + block_line_count;
        let col = self.cursor.position().column.min(
            self.buffer.line(new_line).map(|l| l.len()).unwrap_or(0),
        );
        let new_pos = Position::new(new_line, col);
        self.cursor.set_position(new_pos);
        self.selection.set_position(new_pos);

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Delete the current line (or all selected lines) including their newlines (feat-007).
    fn delete_lines(&mut self) {
        self.break_undo_group();
        let (first, last) = self.selected_line_range();
        let line_count = self.buffer.line_count();

        let start_offset = self
            .buffer
            .position_to_offset(Position::new(first, 0))
            .unwrap_or(0);

        // Include the trailing newline of the last line so the block is fully removed.
        // If deleting the very last line (no following newline), we also eat the
        // preceding newline so we don't leave a blank trailing line.
        let end_offset = if last + 1 < line_count {
            self.buffer
                .position_to_offset(Position::new(last + 1, 0))
                .unwrap_or(0)
        } else if first > 0 {
            // Last line(s) in file — eat the preceding newline instead.
            let preceding_newline = self
                .buffer
                .position_to_offset(Position::new(first, 0))
                .unwrap_or(0)
                .saturating_sub(1);
            let last_line_end = self.buffer.len();
            // Adjust start so we delete from the preceding newline.
            let _ = preceding_newline; // will recalculate below
            last_line_end
        } else {
            self.buffer.len()
        };

        // Recalculate with correct start when deleting the last line(s).
        let (start_offset, end_offset) = if last + 1 >= line_count && first > 0 {
            let new_start = self
                .buffer
                .position_to_offset(Position::new(first, 0))
                .unwrap_or(0)
                .saturating_sub(1); // include preceding '\n'
            (new_start, self.buffer.len())
        } else {
            (start_offset, end_offset)
        };

        if self.buffer.delete(start_offset..end_offset).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }

        // Land cursor at the beginning of where the block was (clamped).
        let new_line = first.min(self.buffer.line_count().saturating_sub(1));
        let new_pos = Position::new(new_line, 0);
        self.cursor.set_position(new_pos);
        self.selection.set_position(new_pos);

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Insert a blank line above the cursor's current line (feat-008).
    ///
    /// The new line inherits the current line's indentation.
    fn insert_newline_above(&mut self) {
        self.break_undo_group();
        let line = self.cursor.position().line;
        let indent = self.line_leading_whitespace(line);

        let insert_offset = self
            .buffer
            .position_to_offset(Position::new(line, 0))
            .unwrap_or(0);
        let text = format!("{}\n", indent);

        if self.buffer.insert(insert_offset, &text).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }

        let new_pos = Position::new(line, indent.len());
        self.cursor.set_position(new_pos);
        self.selection.set_position(new_pos);

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Insert a blank line below the cursor's current line (feat-009).
    ///
    /// The new line inherits the current line's indentation.
    fn insert_newline_below(&mut self) {
        self.break_undo_group();
        let line = self.cursor.position().line;
        let indent = self.line_leading_whitespace(line);
        let line_len = self.buffer.line(line).map(|l| l.len()).unwrap_or(0);

        let insert_offset = self
            .buffer
            .position_to_offset(Position::new(line, line_len))
            .unwrap_or(0);
        let text = format!("\n{}", indent);

        if self.buffer.insert(insert_offset, &text).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }

        let new_line = line + 1;
        let new_pos = Position::new(new_line, indent.len());
        self.cursor.set_position(new_pos);
        self.selection.set_position(new_pos);

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Join the current line with the one below (feat-010, public wrapper).
    ///
    /// Replaces the trailing newline with a single space and collapses any
    /// leading whitespace on the formerly-next line.
    fn join_lines(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.break_undo_group();
        self.join_current_line_with_next(window, cx);
    }

    /// Swap the character before the cursor with the character after it (feat-011).
    fn transpose_chars(&mut self) {
        self.break_undo_group();
        let cursor_offset = self
            .buffer
            .position_to_offset(self.cursor.position())
            .unwrap_or(0);
        let text = self.buffer.text();
        let buf_len = text.len();

        if buf_len < 2 {
            return;
        }

        // When at line end, transpose the two characters before the cursor.
        let (before_start, mid, after_end) = if cursor_offset >= buf_len {
            // Find the two chars before end.
            let mut mid = buf_len - 1;
            while mid > 0 && !text.is_char_boundary(mid) {
                mid -= 1;
            }
            let mut before = mid - 1;
            while before > 0 && !text.is_char_boundary(before) {
                before -= 1;
            }
            (before, mid, buf_len)
        } else {
            // Normal case: swap char before cursor with char at cursor.
            if cursor_offset == 0 {
                return;
            }
            let mut before = cursor_offset - 1;
            while before > 0 && !text.is_char_boundary(before) {
                before -= 1;
            }
            let mut after = cursor_offset + 1;
            while after < buf_len && !text.is_char_boundary(after) {
                after += 1;
            }
            (before, cursor_offset, after)
        };

        let before_char = text[before_start..mid].to_string();
        let after_char = text[mid..after_end].to_string();
        let swapped = format!("{}{}", after_char, before_char);

        if self.buffer.delete(before_start..after_end).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }
        if self.buffer.insert(before_start, &swapped).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }

        // Cursor ends up after the transposed pair.
        if let Ok(pos) = self.buffer.offset_to_position(before_start + swapped.len()) {
            self.cursor.set_position(pos);
            self.selection.set_position(pos);
        }
        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Indent every line in the current selection (or cursor line) by one tab-stop (feat-013).
    fn indent_lines(&mut self) {
        self.break_undo_group();
        let (first, last) = self.selected_line_range();
        for line in first..=last {
            let line_start = self
                .buffer
                .position_to_offset(Position::new(line, 0))
                .unwrap_or(0);
            if self.buffer.insert(line_start, "    ").is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
            }
        }
        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Dedent every line in the current selection (or cursor line) by up to one
    /// tab-stop (feat-012 Shift+Tab and feat-013 Cmd+[).
    fn dedent_lines(&mut self) {
        self.break_undo_group();
        let (first, last) = self.selected_line_range();
        for line in first..=last {
            let line_start = self
                .buffer
                .position_to_offset(Position::new(line, 0))
                .unwrap_or(0);
            let text = self.buffer.text();
            let spaces = text[line_start..]
                .chars()
                .take(4)
                .take_while(|&c| c == ' ')
                .count();
            if spaces > 0 {
                if self
                    .buffer
                    .delete(line_start..line_start + spaces)
                    .is_ok()
                {
                    if let Some(change) = self.buffer.changes().last().cloned() {
                        self.push_undo(change);
                    }
                }
            }
        }
        // Clamp cursor column to the (now-shorter) line.
        let cursor_line = self.cursor.position().line;
        let max_col = self.buffer.line(cursor_line).map(|l| l.len()).unwrap_or(0);
        let col = self.cursor.position().column.min(max_col);
        let new_pos = Position::new(cursor_line, col);
        self.cursor.set_position(new_pos);
        self.selection.set_position(new_pos);

        self.update_syntax_highlights();
        self.update_diagnostics();
        self.scroll_to_cursor();
    }

    /// Toggle `--` SQL line comments on every line in the selection (or cursor line) (feat-015).
    ///
    /// If all selected lines are already commented, the `--` prefix is removed.
    /// Otherwise `-- ` is prepended to the first non-whitespace column on each line.
    fn toggle_line_comment(&mut self) {
        self.break_undo_group();
        let (first, last) = self.selected_line_range();

        // Determine whether all lines are already commented.
        let all_commented = (first..=last).all(|line| {
            self.buffer
                .line(line)
                .map(|text| text.trim_start().starts_with("--"))
                .unwrap_or(false)
        });

        if all_commented {
            // Remove the `--` (and an optional following space) from each line.
            for line in first..=last {
                let line_start = self
                    .buffer
                    .position_to_offset(Position::new(line, 0))
                    .unwrap_or(0);
                let text = self.buffer.text();
                let line_text = &text[line_start..];
                let leading_ws = line_text.chars().take_while(|c| c.is_whitespace()).count();
                let after_ws = line_start + leading_ws;
                // Strip `-- ` or `--`.
                let strip_len = if text[after_ws..].starts_with("-- ") {
                    3
                } else if text[after_ws..].starts_with("--") {
                    2
                } else {
                    0
                };
                if strip_len > 0 {
                    if self.buffer.delete(after_ws..after_ws + strip_len).is_ok() {
                        if let Some(change) = self.buffer.changes().last().cloned() {
                            self.push_undo(change);
                        }
                    }
                }
            }
        } else {
            // Prepend `-- ` at the first non-whitespace column of each line.
            // We iterate in reverse so that byte offsets of earlier lines remain valid.
            for line in (first..=last).rev() {
                let line_start = self
                    .buffer
                    .position_to_offset(Position::new(line, 0))
                    .unwrap_or(0);
                let text = self.buffer.text();
                let leading_ws = text[line_start..]
                    .chars()
                    .take_while(|c| c.is_whitespace())
                    .count();
                let insert_offset = line_start + leading_ws;
                if self.buffer.insert(insert_offset, "-- ").is_ok() {
                    if let Some(change) = self.buffer.changes().last().cloned() {
                        self.push_undo(change);
                    }
                }
            }
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
        let line = self.cursor.position().line;
        let indent = self.line_leading_whitespace(line);
        let text = format!("\n{}", indent);
        self.insert_at_cursor(&text, window, cx);
        // push_undo is skipped here because insert_at_cursor doesn't call it;
        // the caller (handle_newline) has already cleared the selection and the
        // buffer.changes() entry for the newline is pushed below.
        if let Some(change) = self.buffer.changes().last().cloned() {
            self.push_undo(change);
        }
        self.selection.set_position(self.cursor.position());
    }

    // ============================================================================
    // Shared helpers
    // ============================================================================

    /// Collect the raw text of lines `first..=last` including their trailing newlines.
    fn collect_lines_text(&self, first: usize, last: usize) -> String {
        let mut result = String::new();
        for line in first..=last {
            if let Some(text) = self.buffer.line(line) {
                result.push_str(&text);
                // Add the newline that the buffer strips from line() output,
                // unless this is the very last line of the document.
                if line + 1 < self.buffer.line_count() {
                    result.push('\n');
                }
            }
        }
        result
    }

    /// Return the leading whitespace (spaces/tabs) of the given line as a String.
    fn line_leading_whitespace(&self, line: usize) -> String {
        self.buffer
            .line(line)
            .map(|text| {
                text.chars()
                    .take_while(|c| c.is_whitespace() && *c != '\n')
                    .collect()
            })
            .unwrap_or_default()
    }

    // ============================================================================
    // Select line (feat-016)
    // ============================================================================

    /// Select the current line including its trailing newline.
    ///
    /// Repeated calls while the selection already covers whole lines extend it
    /// by one more line downward each time.
    fn select_line(&mut self) {
        let line = self.cursor.position().line;
        let line_count = self.buffer.line_count();

        if self.last_select_line_was_extend && self.selection.has_selection() {
            // Extend: the head of the selection is the start of the line after
            // the currently selected block, so we select one more line.
            let current_end_line = self.selection.end().line;
            let next_line = (current_end_line + 1).min(line_count.saturating_sub(1));
            let next_line_end = self
                .buffer
                .line(next_line)
                .map(|t| t.len())
                .unwrap_or(0);
            let end_col = if next_line + 1 < line_count {
                // End at the start of the following line (i.e. include the \n)
                next_line_end + 1
            } else {
                next_line_end
            };
            let end_pos = Position::new(next_line, end_col.min(next_line_end));
            // Re-anchor to col 0 of the first selected line.
            let anchor = Position::new(self.selection.start().line, 0);
            self.selection = Selection::from_anchor_head(anchor, end_pos);
            self.cursor.set_position(end_pos);
        } else {
            // First press: select the whole current line.
            let line_len = self.buffer.line(line).map(|t| t.len()).unwrap_or(0);
            let end_col = if line + 1 < line_count {
                line_len + 1
            } else {
                line_len
            };
            let start = Position::new(line, 0);
            let end = Position::new(line, end_col.min(line_len));
            self.selection = Selection::from_anchor_head(start, end);
            self.cursor.set_position(end);
            self.last_select_line_was_extend = true;
            return;
        }
        self.last_select_line_was_extend = true;
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

        // If we have no selection yet, select the current word first.
        if !self.selection.has_selection() {
            let offset = self.cursor_byte_offset();
            if let Some(word_range) = self.find_word_range_at_offset(offset) {
                if let (Ok(start), Ok(end)) = (
                    self.buffer.offset_to_position(word_range.start),
                    self.buffer.offset_to_position(word_range.end),
                ) {
                    self.selection = Selection::from_anchor_head(start, end);
                    self.cursor.set_position(end);
                }
            }
            return;
        }

        // Find all occurrences in the buffer.
        let all_occurrences = self.find_all_occurrences(&needle);
        if all_occurrences.is_empty() {
            return;
        }

        // Collect the byte ranges already claimed by the primary and extra cursors.
        let claimed: Vec<std::ops::Range<usize>> = {
            let mut claimed = Vec::new();
            if self.selection.has_selection() {
                let range = self.selection.range();
                let start = self.buffer.position_to_offset(range.start).unwrap_or(0);
                let end = self.buffer.position_to_offset(range.end).unwrap_or(0);
                claimed.push(start..end);
            }
            for (_, sel) in &self.extra_cursors {
                if sel.has_selection() {
                    let range = sel.range();
                    let start = self.buffer.position_to_offset(range.start).unwrap_or(0);
                    let end = self.buffer.position_to_offset(range.end).unwrap_or(0);
                    claimed.push(start..end);
                }
            }
            claimed
        };

        // The search starts after the last claimed occurrence to preserve
        // document order as the user presses Cmd+D repeatedly.
        let search_after = claimed
            .iter()
            .map(|r| r.end)
            .max()
            .unwrap_or(0);

        // Find the first unclaimed occurrence after the current selection,
        // wrapping around the document if necessary.
        let next = all_occurrences
            .iter()
            .find(|r| r.start >= search_after && !claimed.iter().any(|c| c.start == r.start))
            .or_else(|| {
                // Wrap around: look from the beginning.
                all_occurrences
                    .iter()
                    .find(|r| !claimed.iter().any(|c| c.start == r.start))
            });

        if let Some(byte_range) = next {
            if let (Ok(start), Ok(end)) = (
                self.buffer.offset_to_position(byte_range.start),
                self.buffer.offset_to_position(byte_range.end),
            ) {
                let new_sel = Selection::from_anchor_head(start, end);
                self.add_extra_cursor(end, Some(new_sel));
                self.normalize_extra_cursors();
                // Scroll the primary cursor to the newest extra cursor so it
                // is visible.
                self.cursor.set_position(end);
                self.scroll_to_cursor();
                // Restore primary cursor to its previous position without
                // stealing focus from the new extra cursor.
                if let Some(first_claimed) = claimed.first() {
                    if let Ok(primary_end) = self.buffer.offset_to_position(first_claimed.end) {
                        self.cursor.set_position(primary_end);
                    }
                }
            }
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

        let all_occurrences = self.find_all_occurrences(&needle);
        if all_occurrences.is_empty() {
            return;
        }

        // Place the primary cursor on the first occurrence and add extras for
        // the rest.
        self.extra_cursors.clear();
        let mut first = true;
        for byte_range in &all_occurrences {
            if let (Ok(start), Ok(end)) = (
                self.buffer.offset_to_position(byte_range.start),
                self.buffer.offset_to_position(byte_range.end),
            ) {
                let sel = Selection::from_anchor_head(start, end);
                if first {
                    self.cursor.set_position(end);
                    self.selection = sel;
                    first = false;
                } else {
                    self.extra_cursors.push((Cursor::at(end), sel));
                }
            }
        }
        self.normalize_extra_cursors();
    }

    // ============================================================================
    // Add cursor above / below (feat-021 / feat-022)
    // ============================================================================

    /// Add an additional cursor one line above the topmost current cursor,
    /// preserving the column (clamped to line length).
    fn add_cursor_above(&mut self) {
        // Find the topmost line among all cursors.
        let top_line = {
            let mut min_line = self.cursor.position().line;
            for (c, _) in &self.extra_cursors {
                min_line = min_line.min(c.position().line);
            }
            min_line
        };

        if top_line == 0 {
            return; // Already at the top.
        }

        let target_line = top_line - 1;
        // Use the primary cursor's column as the preferred column.
        let preferred_col = self.cursor.position().column;
        let line_len = self
            .buffer
            .line(target_line)
            .map(|t| t.len())
            .unwrap_or(0);
        let col = preferred_col.min(line_len);
        let position = Position::new(target_line, col);
        self.add_extra_cursor(position, None);
        self.normalize_extra_cursors();
    }

    /// Add an additional cursor one line below the bottommost current cursor,
    /// preserving the column (clamped to line length).
    fn add_cursor_below(&mut self) {
        let line_count = self.buffer.line_count();

        // Find the bottommost line among all cursors.
        let bottom_line = {
            let mut max_line = self.cursor.position().line;
            for (c, _) in &self.extra_cursors {
                max_line = max_line.max(c.position().line);
            }
            max_line
        };

        if bottom_line + 1 >= line_count {
            return; // Already at the bottom.
        }

        let target_line = bottom_line + 1;
        let preferred_col = self.cursor.position().column;
        let line_len = self
            .buffer
            .line(target_line)
            .map(|t| t.len())
            .unwrap_or(0);
        let col = preferred_col.min(line_len);
        let position = Position::new(target_line, col);
        self.add_extra_cursor(position, None);
        self.normalize_extra_cursors();
    }

    // ============================================================================
    // Copy / Cut whole line when no selection (feat-023)
    // ============================================================================

    /// Copy the entire current line (including its newline) to the clipboard.
    ///
    /// Used when Cmd+C is pressed with no selection active.
    fn copy_whole_line(&mut self, cx: &mut Context<Self>) {
        let line = self.cursor.position().line;
        let line_count = self.buffer.line_count();
        let line_text = self.buffer.line(line).unwrap_or_default();
        let text = if line + 1 < line_count {
            format!("{}\n", line_text)
        } else {
            line_text.to_string()
        };
        cx.write_to_clipboard(ClipboardItem::new_string(text));
        self.clipboard_is_whole_line = true;
    }

    /// Cut the entire current line (including its newline) to the clipboard.
    ///
    /// Used when Cmd+X is pressed with no selection active.
    fn cut_whole_line(&mut self, cx: &mut Context<Self>) {
        let line = self.cursor.position().line;
        let line_count = self.buffer.line_count();

        // Compute the byte range for the full line including its newline.
        let line_start_offset = self
            .buffer
            .position_to_offset(Position::new(line, 0))
            .unwrap_or(0);

        // Capture text before deletion.
        let line_text = self.buffer.line(line).unwrap_or_default();
        let text = if line + 1 < line_count {
            format!("{}\n", line_text)
        } else {
            line_text.to_string()
        };

        let delete_end = if line + 1 < line_count {
            self.buffer
                .position_to_offset(Position::new(line + 1, 0))
                .unwrap_or(line_start_offset + text.len())
        } else {
            line_start_offset + text.len()
        };

        cx.write_to_clipboard(ClipboardItem::new_string(text));
        self.clipboard_is_whole_line = true;

        self.break_undo_group();
        if self.buffer.delete(line_start_offset..delete_end).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
            // Place cursor at the beginning of the same line number (now the
            // line that was below the deleted one, or last line).
            let new_line = line.min(self.buffer.line_count().saturating_sub(1));
            self.cursor.set_position(Position::new(new_line, 0));
            self.selection.clear();
            self.update_syntax_highlights();
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
        let pos = self.cursor.position();
        let line_text = match self.buffer.line(pos.line) {
            Some(t) => t,
            None => return,
        };
        // The editable end of the line (excluding the implicit \n that
        // buffer.line() strips).
        let line_end_col = line_text.len();

        let cursor_offset = self.cursor_byte_offset();
        let (cut_text, delete_range) = if pos.column < line_end_col {
            // Cut from cursor to end of line (not the newline).
            let end_offset = self
                .buffer
                .position_to_offset(Position::new(pos.line, line_end_col))
                .unwrap_or(cursor_offset);
            let text = self.buffer.text()[cursor_offset..end_offset].to_string();
            (text, cursor_offset..end_offset)
        } else {
            // At line end — cut the newline to join with the next line.
            let line_count = self.buffer.line_count();
            if pos.line + 1 >= line_count {
                return; // Nothing after the last line.
            }
            let next_line_offset = self
                .buffer
                .position_to_offset(Position::new(pos.line + 1, 0))
                .unwrap_or(cursor_offset);
            let text = "\n".to_string();
            (text, cursor_offset..next_line_offset)
        };

        cx.write_to_clipboard(ClipboardItem::new_string(cut_text));
        self.clipboard_is_whole_line = false;

        self.break_undo_group();
        if self.buffer.delete(delete_range).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
            self.selection.clear();
            self.update_syntax_highlights();
            self.scroll_to_cursor();
        }
    }

    // ============================================================================
    // LSP Integration (Stubs for Phase 0)
    // ============================================================================

    /// Set completion provider for schema-aware completions
    ///
    /// This allows the application layer to inject a custom completion provider
    /// (e.g., backed by zqlz-lsp's SqlLsp) to enable schema-aware completions.
    pub fn set_completion_provider(&mut self, provider: std::rc::Rc<dyn CompletionProvider>) {
        self.lsp.completion_provider = Some(provider);
    }

    /// Set the hover provider for schema-aware hover documentation.
    pub fn set_hover_provider(&mut self, provider: std::rc::Rc<dyn HoverProvider>) {
        self.lsp.hover_provider = Some(provider);
    }

    /// Set SQL LSP instance (stub - no-op for now)
    pub fn set_sql_lsp(&mut self, _lsp: Arc<dyn std::any::Any + Send + Sync>) {
        // TODO: Implement in Phase 2
        // This is kept for backward compatibility but is now superseded by set_completion_provider
    }

    /// Check if LSP is connected (stub - always returns false for now)
    pub fn is_lsp_connected(&self) -> bool {
        false
    }

    /// Get completions at cursor
    ///
    /// This method returns completions using the injected completion provider if available,
    /// falling back to the basic SQL keyword/function provider otherwise.
    ///
    /// To enable schema-aware completions (table names, column names, etc.),
    /// the application layer should set a completion provider backed by zqlz-lsp's SqlLsp.
    pub fn get_completions(&self, _cx: &App) -> Vec<CompletionItem> {
        // Get the cursor offset
        let offset = self.cursor.offset(&self.buffer);

        // If a completion provider is injected, use it for schema-aware completions
        if let Some(ref provider) = self.lsp.completion_provider {
            // Create a rope from the buffer text
            let rope = ropey::Rope::from(self.buffer.text());
            let prefix = lsp::get_word_at_cursor(&rope, offset);

            // Use the injected provider
            // Note: We return basic completions synchronously here
            // For async completions, the provider's completions() method should be used
            provider.get_word_completions(&prefix)
        } else {
            // Fallback to the default SQL keyword/function provider
            let provider = SqlCompletionProvider::new();

            // Get the word at cursor for filtering
            let rope = ropey::Rope::from(self.buffer.text());
            let prefix = lsp::get_word_at_cursor(&rope, offset);
            provider.get_word_completions(&prefix)
        }
    }

    /// Get hover at cursor (stub - always returns None for now)
    pub fn get_hover(&self, _cx: &App) -> Option<Hover> {
        None
    }

    /// Get hover at specific offset
    /// Returns hover information for the word at the given offset
    pub fn get_hover_at(&self, offset: usize, _cx: &App) -> Option<Hover> {
        let provider = SqlCompletionProvider::new();
        let text = self.buffer.text();

        // Find the word at the given offset
        let word_range = self.find_word_range_at_offset(offset)?;
        let word = &text[word_range.clone()];

        // Get documentation for the word
        let documentation = provider.get_hover_documentation(word)?;

        // Create LSP Hover response
        use lsp_types::{HoverContents, MarkupContent, MarkupKind, Range as LspRange};

        Some(Hover {
            contents: HoverContents::Markup(MarkupContent {
                kind: MarkupKind::PlainText,
                value: documentation,
            }),
            range: Some(LspRange {
                start: lsp_types::Position {
                    line: self.buffer.offset_to_position(word_range.start).ok()?.line as u32,
                    character: self
                        .buffer
                        .offset_to_position(word_range.start)
                        .ok()?
                        .column as u32,
                },
                end: lsp_types::Position {
                    line: self.buffer.offset_to_position(word_range.end).ok()?.line as u32,
                    character: self.buffer.offset_to_position(word_range.end).ok()?.column as u32,
                },
            }),
        })
    }

    /// Find the word range at a given offset
    fn find_word_range_at_offset(&self, offset: usize) -> Option<std::ops::Range<usize>> {
        let text = self.buffer.text();

        if offset > text.len() {
            return None;
        }

        // Find start of word
        let mut start = offset;
        while start > 0 {
            let prev = start.saturating_sub(1);
            if let Some(ch) = text.chars().nth(prev) {
                if ch.is_alphanumeric() || ch == '_' {
                    start = prev;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        // Find end of word
        let mut end = offset;
        for (idx, ch) in text.char_indices().skip(offset) {
            if ch.is_alphanumeric() || ch == '_' {
                end = idx + ch.len_utf8();
            } else {
                break;
            }
        }

        if start < end {
            Some(start..end)
        } else {
            None
        }
    }

    /// Get signature help (stub - always returns None for now)
    pub fn get_signature_help(&self, _cx: &App) -> Option<SignatureHelp> {
        None
    }

    // ============================================================================
    // Diagnostics (Stubs for Phase 0)
    // ============================================================================

    /// Set diagnostics (stub - no-op for now)
    pub fn set_diagnostics(
        &mut self,
        _diagnostics: Vec<Diagnostic>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
        // TODO: Implement in Phase 2
    }

    // ============================================================================
    // Completion Menu (Phase 8 - LSP Integration)
    // ============================================================================

    /// Trigger completion menu manually (Ctrl+Space)
    pub fn trigger_completions(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let cursor_offset = self.cursor.offset(&self.buffer);
        // The word start is where we'll delete back to on accept, so any text
        // the user has already typed (the prefix) gets replaced cleanly.
        let word_start = self
            .find_word_range_at_offset(cursor_offset)
            .map(|r| r.start)
            .unwrap_or(cursor_offset);

        let Some(provider) = self.lsp.completion_provider.clone() else {
            // No real LSP provider — fall back to the built-in keyword list.
            let items = SqlCompletionProvider::new()
                .get_word_completions(&lsp::get_word_at_cursor(
                    &ropey::Rope::from(self.buffer.text()),
                    cursor_offset,
                ));
            if items.is_empty() {
                self.completion_menu = None;
            } else {
                self.completion_menu = Some(CompletionMenuState {
                    items,
                    trigger_offset: word_start,
                    selected_index: 0,
                    scroll_offset: 0,
                    scroll_accumulator: 0.0,
                });
            }
            return;
        };

        let rope = ropey::Rope::from(self.buffer.text());
        let trigger = lsp_types::CompletionContext {
            trigger_kind: lsp_types::CompletionTriggerKind::INVOKED,
            trigger_character: None,
        };

        // Call the provider — for SqlLspCompletionAdapter this resolves synchronously
        // (Task::ready), so awaiting it in cx.spawn costs nothing extra.
        let task = provider.completions(&rope, cursor_offset, trigger, window, cx);

        self.lsp.completion_task = cx.spawn(async move |this, cx| {
            let response = task.await?;

            let items = match response {
                lsp_types::CompletionResponse::Array(items) => items,
                lsp_types::CompletionResponse::List(list) => list.items,
            };

            this.update(cx, |editor, cx| {
                if items.is_empty() {
                    editor.completion_menu = None;
                } else {
                    editor.completion_menu = Some(CompletionMenuState {
                        items,
                        trigger_offset: word_start,
                        selected_index: 0,
                        scroll_offset: 0,
                        scroll_accumulator: 0.0,
                    });
                }
                cx.notify();
            })?;

            Ok(())
        });
    }

    /// Show completion menu
    pub fn hide_completion_menu(&mut self, _cx: &mut Context<Self>) {
        self.completion_menu = None;
    }

    /// Update completion menu (stub - no-op for now)
    pub fn update_completion_menu(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        // Could update with new completions based on cursor position
    }

    /// Check if completion menu is open
    pub fn is_completion_menu_open(&self, _cx: &App) -> bool {
        self.completion_menu.is_some()
    }

    /// Get completion menu data for rendering
    pub fn completion_menu(&self, _cx: &App) -> Option<CompletionMenuData> {
        self.completion_menu
            .as_ref()
            .map(|menu| CompletionMenuData {
                items: menu.items.clone(),
                selected_index: menu.selected_index,
                scroll_offset: menu.scroll_offset,
            })
    }

    /// Move selection in completion menu up
    pub fn completion_menu_select_previous(&mut self) {
        if let Some(ref mut menu) = self.completion_menu {
            if menu.selected_index > 0 {
                menu.selected_index -= 1;
                // Scroll viewport up if the selection is now above the visible window.
                if menu.selected_index < menu.scroll_offset {
                    menu.scroll_offset = menu.selected_index;
                }
            }
        }
    }

    /// Move selection in completion menu down
    pub fn completion_menu_select_next(&mut self) {
        if let Some(ref mut menu) = self.completion_menu {
            if menu.selected_index < menu.items.len().saturating_sub(1) {
                menu.selected_index += 1;
                // Scroll viewport down if the selection is now below the visible window.
                let visible_end = menu.scroll_offset + crate::element::MAX_COMPLETION_ITEMS;
                if menu.selected_index >= visible_end {
                    menu.scroll_offset = menu.selected_index + 1 - crate::element::MAX_COMPLETION_ITEMS;
                }
            }
        }
    }

    /// Accept the currently selected completion
    pub fn accept_completion(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        // Take the completion menu state
        let Some(menu) = self.completion_menu.take() else {
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

        // Delete the partial word that was typed
        let trigger_offset = menu.trigger_offset;
        let cursor_offset = self.cursor.offset(&self.buffer);

        // Delete the partial word the user typed (from trigger position to cursor),
        // capturing it so the undo entry can restore it.
        let deleted_text = if cursor_offset > trigger_offset {
            let range = trigger_offset..cursor_offset;
            if self.buffer.delete(range).is_ok() {
                let old = self
                    .buffer
                    .changes()
                    .last()
                    .map(|c| c.old_text.clone())
                    .unwrap_or_default();
                if let Ok(pos) = self.buffer.offset_to_position(trigger_offset) {
                    self.cursor.set_position(pos);
                }
                old
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        self.insert_at_cursor(&completion_text, window, cx);

        // Record partial-word deletion + completion insertion as one atomic undo entry,
        // always starting a fresh group so it stays distinct from prior typing.
        self.break_undo_group();
        self.push_undo(buffer::Change::replace(
            trigger_offset,
            deleted_text,
            completion_text,
        ));

        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    /// Handle completion action (stub - always returns false for now)
    pub fn handle_completion_action(
        &mut self,
        _action: &dyn Action,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> bool {
        false
    }

    /// Refresh completions (stub - no-op for now)
    /// Alias for update_completion_menu for backward compatibility
    pub fn refresh_completions(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        // TODO: Implement in Phase 3
    }

    /// Show completions automatically (stub - no-op for now)
    /// Alias for show_completion_menu with auto-trigger behavior
    pub fn show_completions_auto(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.trigger_completions(window, cx);
    }

    /// Show completions
    /// Alias for show_completion_menu
    pub fn show_completions(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.trigger_completions(window, cx);
    }

    /// Hide completions
    pub fn hide_completions(&mut self, _cx: &mut Context<Self>) {
        self.completion_menu = None;
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
        // Convert line/column to offset
        let position = Position::new(line, column);
        let Ok(offset) = self.buffer.position_to_offset(position) else {
            self.hover_state = None;
            return;
        };

        // If a real hover provider is wired, use it asynchronously.
        if let Some(provider) = self.lsp.hover_provider.clone() {
            let rope = ropey::Rope::from(self.buffer.text());
            let word_range = self.find_word_range_at_offset(offset);
            let task = provider.hover(&rope, offset, window, cx);

            self.lsp.hover_task = cx.spawn(async move |this, cx| {
                let maybe_hover = task.await?;

            this.update(cx, |editor, cx| {
                match (maybe_hover, word_range) {
                        (Some(_hover), Some(range)) => {
                            // Extract the word to use as the hover label
                            let text = editor.buffer.text();
                            let word = text[range.clone()].to_string();
                            // Use the first plain-text content from the hover response
                            // as the documentation string.
                            let documentation = match &_hover.contents {
                                lsp_types::HoverContents::Scalar(markup) => {
                                    markup_to_string(markup)
                                }
                                lsp_types::HoverContents::Array(markups) => markups
                                    .first()
                                    .map(markup_to_string)
                                    .unwrap_or_default(),
                                lsp_types::HoverContents::Markup(markup) => {
                                    markup.value.clone()
                                }
                            };

                            if documentation.is_empty() {
                                editor.hover_state = None;
                            } else {
                                let should_update = match &editor.hover_state {
                                    Some(current) => current.word != word,
                                    None => true,
                                };
                                if should_update {
                                    editor.hover_state = Some(HoverState {
                                        word,
                                        documentation,
                                        range,
                                    });
                                    cx.notify();
                                }
                            }
                        }
                        _ => {
                            editor.hover_state = None;
                            cx.notify();
                        }
                    }
                })?;

                Ok(())
            });
            return;
        }

        // Fallback: keyword/function hover from the built-in provider.
        if let Some(word_range) = self.find_word_range_at_offset(offset) {
            let text = self.buffer.text();
            let word = &text[word_range.clone()];

            let provider = SqlCompletionProvider::new();
            if let Some(documentation) = provider.get_hover_documentation(word) {
                let should_update = match &self.hover_state {
                    Some(current) => current.word != word,
                    None => true,
                };
                if should_update {
                    self.hover_state = Some(HoverState {
                        word: word.to_string(),
                        documentation,
                        range: word_range,
                    });
                }
            } else {
                self.hover_state = None;
            }
        } else {
            self.hover_state = None;
        }
    }

    /// Clear hover state (called when mouse leaves the editor)
    pub fn clear_hover(&mut self) {
        self.hover_state = None;
    }

    /// Get the current hover state for rendering
    pub fn hover_state(&self, _cx: &App) -> Option<HoverState> {
        self.hover_state.clone()
    }

    /// Check if hover tooltip is visible
    pub fn has_hover(&self, _cx: &App) -> bool {
        self.hover_state.is_some()
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
        self.inline_suggestion = Some((text, cursor_offset));
        cx.notify();
    }

    /// Remove any pending inline suggestion.  Call this when the user dismisses
    /// the suggestion, accepts it, or types something that invalidates it.
    pub fn clear_inline_suggestion(&mut self, cx: &mut Context<Self>) {
        if self.inline_suggestion.is_some() {
            self.inline_suggestion = None;
            cx.notify();
        }
    }

    /// Read the current inline suggestion (text, anchor cursor offset).
    pub fn inline_suggestion(&self) -> Option<&(String, usize)> {
        self.inline_suggestion.as_ref()
    }

    // ============================================================================
    // Navigation & LSP Actions (Stubs for Phase 0)
    // ============================================================================

    /// Navigate to specific position (stub - no-op for now)
    pub fn navigate_to(
        &mut self,
        _line: usize,
        _column: usize,
        _end_line: Option<usize>,
        _end_column: Option<usize>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
        // TODO: Implement in Phase 1 (cursor positioning)
    }

    /// Get definition at cursor (stub - always returns None for now)
    pub fn get_definition(&self, _cx: &App) -> Option<lsp_types::GotoDefinitionResponse> {
        // TODO: Implement in Phase 2 (LSP integration)
        None
    }

    /// Get references at cursor (stub - always returns empty for now)
    pub fn get_references(&self, _cx: &App) -> Vec<lsp_types::Location> {
        // TODO: Implement in Phase 2 (LSP integration)
        Vec::new()
    }

    /// Get code actions at cursor (stub - always returns empty for now)
    pub fn get_code_actions(&self, _cx: &App) -> Vec<lsp_types::CodeActionOrCommand> {
        // TODO: Implement in Phase 2 (LSP integration)
        Vec::new()
    }

    /// Rename symbol at cursor (stub - always returns None for now)
    pub fn rename(&self, _new_name: &str, _cx: &App) -> Option<lsp_types::WorkspaceEdit> {
        // TODO: Implement in Phase 2 (LSP integration)
        None
    }

    /// Apply workspace edit (stub - always returns Ok for now)
    pub fn apply_workspace_edit(
        &mut self,
        _edit: &lsp_types::WorkspaceEdit,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> anyhow::Result<()> {
        // TODO: Implement in Phase 2 (LSP integration)
        Ok(())
    }

    /// Apply code action (stub - always returns Ok for now)
    pub fn apply_code_action(
        &mut self,
        _action: &lsp_types::CodeActionOrCommand,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> anyhow::Result<()> {
        // TODO: Implement in Phase 2 (LSP integration)
        Ok(())
    }

    // ============================================================================
    // Find & Replace (Phase 9)
    // ============================================================================

    /// Open the find panel (Ctrl+F). Reuses any existing state so the last
    /// query is preserved when the panel is reopened.
    pub fn open_find(&mut self, cx: &mut Context<Self>) {
        if self.find_state.is_none() {
            self.find_state = Some(FindState::new(false));
        }
        if let Some(ref mut state) = self.find_state {
            state.show_replace = false;
            state.search_field_focused = true;
        }
        cx.notify();
    }

    /// Open the find+replace panel (Ctrl+H).
    pub fn open_find_replace(&mut self, cx: &mut Context<Self>) {
        if self.find_state.is_none() {
            self.find_state = Some(FindState::new(true));
        }
        if let Some(ref mut state) = self.find_state {
            state.show_replace = true;
            state.search_field_focused = true;
        }
        cx.notify();
    }

    /// Close the find/replace panel (Escape).
    pub fn close_find(&mut self, cx: &mut Context<Self>) {
        self.find_state = None;
        // Clear any find-related selection so the user ends up at the cursor
        self.selection.clear();
        cx.notify();
    }

    /// Returns true when the find panel is currently visible.
    pub fn is_find_open(&self) -> bool {
        self.find_state.is_some()
    }

    /// Append a character to the current search query and recompute matches.
    ///
    /// This is called from the keyboard handler when the find panel is focused.
    pub fn find_input_char(&mut self, ch: char, cx: &mut Context<Self>) {
        let text = self.buffer.text();
        if let Some(ref mut state) = self.find_state {
            if state.search_field_focused {
                state.query.push(ch);
                state.recompute_matches(&text);
                // Jump to the first match closest to the current cursor position
                self.jump_to_nearest_match();
            } else {
                state.replace_query.push(ch);
            }
        }
        cx.notify();
    }

    /// Remove the last character from the active find/replace input field.
    pub fn find_input_backspace(&mut self, cx: &mut Context<Self>) {
        let text = self.buffer.text();
        if let Some(ref mut state) = self.find_state {
            if state.search_field_focused {
                state.query.pop();
                state.recompute_matches(&text);
                self.jump_to_nearest_match();
            } else {
                state.replace_query.pop();
            }
        }
        cx.notify();
    }

    /// Move the cursor to the next match (F3 / Enter in search box).
    ///
    /// Wraps around when the end of the match list is reached.
    pub fn find_next(&mut self, cx: &mut Context<Self>) {
        if let Some(ref mut state) = self.find_state {
            if state.matches.is_empty() {
                return;
            }
            state.current_match = (state.current_match + 1) % state.matches.len();
            let match_start = state.matches[state.current_match].start;
            let match_end = state.matches[state.current_match].end;
            self.select_match(match_start, match_end);
        }
        cx.notify();
    }

    /// Move the cursor to the previous match (Shift+F3).
    ///
    /// Wraps around when the beginning of the match list is reached.
    pub fn find_previous(&mut self, cx: &mut Context<Self>) {
        if let Some(ref mut state) = self.find_state {
            if state.matches.is_empty() {
                return;
            }
            if state.current_match == 0 {
                state.current_match = state.matches.len() - 1;
            } else {
                state.current_match -= 1;
            }
            let match_start = state.matches[state.current_match].start;
            let match_end = state.matches[state.current_match].end;
            self.select_match(match_start, match_end);
        }
        cx.notify();
    }

    /// Toggle case-sensitive matching and recompute matches.
    pub fn find_toggle_case_sensitive(&mut self, cx: &mut Context<Self>) {
        let text = self.buffer.text();
        if let Some(ref mut state) = self.find_state {
            state.options.case_sensitive = !state.options.case_sensitive;
            state.recompute_matches(&text);
            self.jump_to_nearest_match();
        }
        cx.notify();
    }

    /// Toggle whole-word matching and recompute matches.
    pub fn find_toggle_whole_word(&mut self, cx: &mut Context<Self>) {
        let text = self.buffer.text();
        if let Some(ref mut state) = self.find_state {
            state.options.whole_word = !state.options.whole_word;
            state.recompute_matches(&text);
            self.jump_to_nearest_match();
        }
        cx.notify();
    }

    /// Toggle regex mode and recompute matches (feat-030).
    ///
    /// When enabled, the query is compiled as a regular expression. An invalid
    /// regex is surfaced through `FindState::regex_error` rather than panicking.
    pub fn find_toggle_regex(&mut self, cx: &mut Context<Self>) {
        let text = self.buffer.text();
        if let Some(ref mut state) = self.find_state {
            state.options.use_regex = !state.options.use_regex;
            state.recompute_matches(&text);
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
        let text = self.buffer.text();
        if let Some(ref mut state) = self.find_state {
            if state.selection_boundary.is_some() {
                // Turn off: search the whole buffer
                state.selection_boundary = None;
            } else {
                // Turn on: capture the current selection range as the boundary
                if self.selection.has_selection() {
                    let range = self.selection.range();
                    let start = self
                        .buffer
                        .position_to_offset(range.start)
                        .unwrap_or(0);
                    let end = self
                        .buffer
                        .position_to_offset(range.end)
                        .unwrap_or(text.len());
                    state.selection_boundary = Some((start, end));
                }
            }
            state.recompute_matches(&text);
            self.jump_to_nearest_match();
        }
        cx.notify();
    }

    /// Switch keyboard focus between the search field and the replace field.
    pub fn find_toggle_field_focus(&mut self, cx: &mut Context<Self>) {
        if let Some(ref mut state) = self.find_state {
            if state.show_replace {
                state.search_field_focused = !state.search_field_focused;
            }
        }
        cx.notify();
    }

    /// Convert every current find match into an independent cursor+selection,
    /// then close the find panel (feat-032).
    ///
    /// The primary cursor lands on the first match; each remaining match becomes
    /// an extra cursor. This lets the user immediately type to replace all
    /// occurrences at once.
    pub fn find_select_all_matches(&mut self, cx: &mut Context<Self>) {
        let matches = match &self.find_state {
            Some(state) if !state.matches.is_empty() => state.matches.clone(),
            _ => return,
        };

        // Convert byte offsets to positions for each match
        let mut positions: Vec<(Position, Position)> = Vec::with_capacity(matches.len());
        for m in &matches {
            let Ok(start_pos) = self.buffer.offset_to_position(m.start) else {
                continue;
            };
            let Ok(end_pos) = self.buffer.offset_to_position(m.end) else {
                continue;
            };
            positions.push((start_pos, end_pos));
        }

        if positions.is_empty() {
            return;
        }

        // Place the primary cursor on the first match
        let (first_start, first_end) = positions[0];
        self.cursor.set_position(first_end);
        self.selection.start_selection(first_start);
        self.selection.extend_to(first_end);

        // Each remaining match becomes an extra cursor with its own selection
        self.extra_cursors.clear();
        for &(start_pos, end_pos) in &positions[1..] {
            let cursor = Cursor::at(end_pos);
            let mut sel = Selection::at(start_pos);
            sel.start_selection(start_pos);
            sel.extend_to(end_pos);
            self.extra_cursors.push((cursor, sel));
        }
        self.normalize_extra_cursors();

        self.find_state = None;
        self.scroll_to_cursor();
        cx.notify();
    }

    /// Replace the currently selected match with the replacement text.
    ///
    /// After replacement, moves to the next match automatically.
    pub fn replace_current(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let (match_start, match_end, replacement) = {
            let Some(ref state) = self.find_state else {
                return;
            };
            if state.matches.is_empty() {
                return;
            }
            let m = &state.matches[state.current_match];
            (m.start, m.end, state.replace_query.clone())
        };

        // Delete the matched range and insert the replacement
        if self.buffer.delete(match_start..match_end).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }
        if self.buffer.insert(match_start, &replacement).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }

        self.update_syntax_highlights();
        self.update_diagnostics();

        // Recompute matches after replacement
        let text = self.buffer.text();
        let next_match = if let Some(ref mut state) = self.find_state {
            state.recompute_matches(&text);
            if !state.matches.is_empty() {
                let idx = state.current_match.min(state.matches.len() - 1);
                state.current_match = idx;
                let m = &state.matches[idx];
                Some((m.start, m.end))
            } else {
                None
            }
        } else {
            None
        };
        if let Some((start, end)) = next_match {
            self.select_match(start, end);
        }

        // Move cursor to end of replacement text
        if let Ok(pos) = self
            .buffer
            .offset_to_position(match_start + replacement.len())
        {
            self.cursor.set_position(pos);
        }
        self.scroll_to_cursor();
        _ = window; // unused but kept for future use
        cx.notify();
    }

    /// Replace all matches with the replacement text.
    ///
    /// Replacements are applied back-to-front so that earlier offsets remain valid.
    pub fn replace_all(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let (matches, replacement) = {
            let Some(ref state) = self.find_state else {
                return;
            };
            if state.matches.is_empty() {
                return;
            }
            (state.matches.clone(), state.replace_query.clone())
        };

        // Apply replacements from last to first so byte offsets stay valid
        for m in matches.iter().rev() {
            if self.buffer.delete(m.start..m.end).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
            }
            if self.buffer.insert(m.start, &replacement).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
            }
        }

        self.update_syntax_highlights();
        self.update_diagnostics();

        // Recompute matches (should all be gone, or new ones if replacement contains the query)
        let text = self.buffer.text();
        if let Some(ref mut state) = self.find_state {
            state.recompute_matches(&text);
        }

        cx.notify();
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// Jump to the match whose start offset is nearest to the current cursor,
    /// or to the first match when there is no good candidate.
    fn jump_to_nearest_match(&mut self) {
        let Some(ref mut state) = self.find_state else {
            return;
        };
        if state.matches.is_empty() {
            return;
        }
        let cursor_offset = self.cursor.offset(&self.buffer);
        // Find the first match whose start is >= cursor position
        let best = state
            .matches
            .iter()
            .position(|m| m.start >= cursor_offset)
            .unwrap_or(0);
        state.current_match = best;
        let m = &state.matches[best];
        let (match_start, match_end) = (m.start, m.end);
        self.select_match(match_start, match_end);
    }

    /// Move the cursor to `match_start` and select up to `match_end`.
    fn select_match(&mut self, match_start: usize, match_end: usize) {
        if let Ok(start_pos) = self.buffer.offset_to_position(match_start) {
            if let Ok(end_pos) = self.buffer.offset_to_position(match_end) {
                self.cursor.set_position(start_pos);
                self.selection.start_selection(start_pos);
                self.selection.extend_to(end_pos);
                self.scroll_to_cursor();
            }
        }
    }

    // ============================================================================
    // Vim Mode (Phase 11)
    // ============================================================================

    /// Enable vim modal editing. Subsequent key events will be processed through
    /// the vim state machine before reaching normal editing logic.
    pub fn enable_vim_mode(&mut self) {
        self.vim_state = Some(VimState::new());
    }

    /// Disable vim modal editing and return to standard editor behaviour.
    pub fn disable_vim_mode(&mut self) {
        self.vim_state = None;
    }

    /// Returns `true` when vim mode is currently enabled.
    pub fn is_vim_mode_enabled(&self) -> bool {
        self.vim_state.is_some()
    }

    /// Returns the current vim mode label (`"NORMAL"`, `"INSERT"`, etc.), or
    /// `None` when vim mode is disabled.
    pub fn vim_mode_label(&self) -> Option<&'static str> {
        self.vim_state.as_ref().map(|s| s.mode_label())
    }

    /// Returns the current vim mode, or `None` when vim mode is disabled.
    pub fn vim_mode(&self) -> Option<VimMode> {
        self.vim_state.as_ref().map(|s| s.mode)
    }

    /// Process a key event through the vim state machine and execute any resulting
    /// action against the buffer / cursor. Returns `true` if the event was fully
    /// consumed by vim handling (the caller should not apply normal editing logic).
    fn handle_vim_key(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // VimState is owned; we need to take it, call handle_key, then put it back.
        let Some(mut vim) = self.vim_state.take() else {
            return false;
        };

        let key = event.keystroke.key.as_str();
        let key_char = event.keystroke.key_char.as_deref();
        let shift = event.keystroke.modifiers.shift;

        // In Insert mode, all regular keys go straight to the normal editor path
        // (VimState only handles Escape in Insert mode).
        let was_insert = vim.mode == VimMode::Insert;

        let action = vim.handle_key(key, key_char, shift);

        // Restore state before executing the action (the action may borrow self).
        self.vim_state = Some(vim);

        use vim::VimAction;

        match action {
            VimAction::None => {
                // In Insert mode a None means "let the normal editor handle it".
                if was_insert {
                    return false;
                }
                // In other modes an unknown key is consumed silently.
                return true;
            }

            VimAction::EnterInsert => {
                cx.notify();
                return true;
            }
            VimAction::EnterNormal => {
                // Clear any selection when returning to Normal.
                self.selection.clear();
                cx.notify();
                return true;
            }
            VimAction::EnterVisual => {
                // Start a new selection anchored at the current cursor position.
                self.selection.start_selection(self.cursor.position());
                cx.notify();
                return true;
            }
            VimAction::EnterCommand => {
                cx.notify();
                return true;
            }

            VimAction::MoveCursor(motion) => {
                let new_pos = vim::resolve_motion(&motion, &self.cursor, &self.buffer);
                self.cursor.set_position(new_pos);
                self.selection.set_position(new_pos);
                self.scroll_to_cursor();
                cx.notify();
                return true;
            }

            VimAction::ExtendSelection(motion) => {
                if !self.selection.has_selection() {
                    self.selection.start_selection(self.cursor.position());
                }
                let new_pos = vim::resolve_motion(&motion, &self.cursor, &self.buffer);
                self.cursor.set_position(new_pos);
                self.selection.extend_to(new_pos);
                self.scroll_to_cursor();
                cx.notify();
                return true;
            }

            VimAction::DeleteMotion(motion) => {
                let start = self.cursor.position();
                let end = vim::resolve_motion(&motion, &self.cursor, &self.buffer);
                self.apply_operator_delete(start, end);
                cx.notify();
                return true;
            }

            VimAction::ChangeMotion(motion) => {
                let start = self.cursor.position();
                let end = vim::resolve_motion(&motion, &self.cursor, &self.buffer);
                self.apply_operator_delete(start, end);
                // ChangeMotion transitions to Insert mode.
                if let Some(ref mut v) = self.vim_state {
                    v.mode = VimMode::Insert;
                }
                cx.notify();
                return true;
            }

            VimAction::YankMotion(motion) => {
                let start = self.cursor.position();
                let end = vim::resolve_motion(&motion, &self.cursor, &self.buffer);
                let text = self.yank_range(start, end);
                cx.write_to_clipboard(ClipboardItem::new_string(text));
                cx.notify();
                return true;
            }

            VimAction::DeleteSelection => {
                self.delete_selection();
                self.update_syntax_highlights();
                self.update_diagnostics();
                cx.notify();
                return true;
            }

            VimAction::ChangeSelection => {
                self.delete_selection();
                self.update_syntax_highlights();
                self.update_diagnostics();
                if let Some(ref mut v) = self.vim_state {
                    v.mode = VimMode::Insert;
                }
                cx.notify();
                return true;
            }

            VimAction::YankSelection => {
                if let Some(text) = self.get_selected_text(cx) {
                    cx.write_to_clipboard(ClipboardItem::new_string(text.to_string()));
                }
                self.selection.clear();
                cx.notify();
                return true;
            }

            VimAction::PasteAfter => {
                // Move right one position (after cursor), then paste.
                self.cursor.move_right(&self.buffer);
                self.paste(window, cx);
                cx.notify();
                return true;
            }

            VimAction::PasteBefore => {
                self.paste(window, cx);
                cx.notify();
                return true;
            }

            VimAction::Undo => {
                self.undo(window, cx);
                cx.notify();
                return true;
            }

            VimAction::Redo => {
                self.redo(window, cx);
                cx.notify();
                return true;
            }

            VimAction::ExecuteCommand(cmd) => {
                self.execute_vim_command(&cmd, window, cx);
                cx.notify();
                return true;
            }

            VimAction::JoinLines => {
                self.join_current_line_with_next(window, cx);
                cx.notify();
                return true;
            }

            VimAction::Indent => {
                self.indent_current_line(window, cx);
                cx.notify();
                return true;
            }

            VimAction::Dedent => {
                self.dedent_current_line(window, cx);
                cx.notify();
                return true;
            }

            VimAction::OpenLineBelow => {
                self.cursor.move_to_line_end(&self.buffer);
                self.insert_at_cursor("\n", window, cx);
                self.selection.set_position(self.cursor.position());
                cx.notify();
                return true;
            }

            VimAction::OpenLineAbove => {
                self.cursor.move_to_line_start();
                self.insert_at_cursor("\n", window, cx);
                // Move cursor back up to the newly created line.
                self.cursor.move_up(&self.buffer);
                self.selection.set_position(self.cursor.position());
                cx.notify();
                return true;
            }

            VimAction::DeleteCharAtCursor => {
                self.delete_at_cursor(window, cx);
                self.selection.set_position(self.cursor.position());
                cx.notify();
                return true;
            }

            VimAction::ReplaceChar(ch) => {
                // Delete the char under cursor and insert the replacement.
                self.delete_at_cursor(window, cx);
                self.insert_at_cursor(ch.to_string(), window, cx);
                // Move cursor back one so it rests on the replacement char.
                self.cursor.move_left(&self.buffer);
                self.selection.set_position(self.cursor.position());
                cx.notify();
                return true;
            }

            VimAction::TransformCase { .. } => {
                // Stub: case transformation not yet implemented; just move on.
                cx.notify();
                return true;
            }
        }
    }

    // ── Vim helpers ──────────────────────────────────────────────────────────

    /// Delete the text between two positions (ordered min..max) and track for undo.
    fn apply_operator_delete(&mut self, a: Position, b: Position) {
        let start = a.min(b);
        let end = a.max(b);
        let start_offset = self.buffer.position_to_offset(start).unwrap_or(0);
        let end_offset = self.buffer.position_to_offset(end).unwrap_or(0);

        if start_offset >= end_offset {
            return;
        }

        if self.buffer.delete(start_offset..end_offset).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
            if let Ok(pos) = self.buffer.offset_to_position(start_offset) {
                self.cursor.set_position(pos);
                self.selection.set_position(pos);
            }
            self.update_syntax_highlights();
            self.update_diagnostics();
            self.scroll_to_cursor();
        }
    }

    /// Return the text between two positions as a `String` (for yanking).
    fn yank_range(&self, a: Position, b: Position) -> String {
        let start = a.min(b);
        let end = a.max(b);
        let start_offset = self.buffer.position_to_offset(start).unwrap_or(0);
        let end_offset = self.buffer.position_to_offset(end).unwrap_or(0);
        let text = self.buffer.text();
        text[start_offset..end_offset.min(text.len())].to_owned()
    }

    /// Join the current line with the line below it by replacing the trailing
    /// newline with a space.
    fn join_current_line_with_next(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let line = self.cursor.position().line;
        if line + 1 >= self.buffer.line_count() {
            return;
        }
        // Find the offset of the newline at the end of the current line.
        let line_text = match self.buffer.line(line) {
            Some(t) => t,
            None => return,
        };
        let line_end_offset = self
            .buffer
            .position_to_offset(Position::new(line, line_text.len()))
            .unwrap_or(0);

        // Replace the newline (1 byte) with a space.
        if self
            .buffer
            .delete(line_end_offset..line_end_offset + 1)
            .is_ok()
        {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
            self.insert_at_cursor(" ", window, cx);
            self.update_syntax_highlights();
            self.update_diagnostics();
        }
    }

    /// Indent the current line by inserting one tab-stop (4 spaces) at the line start.
    fn indent_current_line(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let line = self.cursor.position().line;
        let line_start_offset = self
            .buffer
            .position_to_offset(Position::new(line, 0))
            .unwrap_or(0);
        if self.buffer.insert(line_start_offset, "    ").is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
            self.update_syntax_highlights();
            self.update_diagnostics();
        }
        _ = cx; // notify happens in the caller
    }

    /// Dedent the current line by removing up to one tab-stop (4 spaces) from the line start.
    fn dedent_current_line(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        let line = self.cursor.position().line;
        let line_start_offset = self
            .buffer
            .position_to_offset(Position::new(line, 0))
            .unwrap_or(0);
        let text = self.buffer.text();

        // Count leading spaces (up to 4).
        let spaces = text[line_start_offset..]
            .chars()
            .take(4)
            .take_while(|&c| c == ' ')
            .count();

        if spaces > 0 {
            if self
                .buffer
                .delete(line_start_offset..line_start_offset + spaces)
                .is_ok()
            {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
                // Move cursor to start of line so it stays in-bounds.
                self.cursor.set_position(
                    self.buffer
                        .offset_to_position(line_start_offset)
                        .unwrap_or(Position::new(line, 0)),
                );
                self.update_syntax_highlights();
                self.update_diagnostics();
            }
        }
    }

    /// Execute a vim `:` command. Handles `:w`, `:q`, `:wq`, `nohl`, and `%s/pat/rep/g`.
    fn execute_vim_command(&mut self, cmd: &str, window: &mut Window, cx: &mut Context<Self>) {
        use vim::execute_command;
        use vim::CommandResult;
        match execute_command(cmd) {
            CommandResult::Ok => {}
            CommandResult::FindReplace {
                pattern,
                replacement,
            } => {
                // Wire into the Find & Replace system: open a panel state with the
                // given pattern, apply replace-all, then close the panel.
                self.find_state = Some(FindState::new(true));
                if let Some(ref mut state) = self.find_state {
                    state.query = pattern;
                    state.replace_query = replacement;
                    let text = self.buffer.text();
                    state.recompute_matches(&text);
                }
                self.replace_all(window, cx);
                self.find_state = None;
            }
            CommandResult::Unknown(_) => {
                // Unknown commands are silently ignored for now.
            }
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
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .size_full()
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
            .on_action(cx.listener(Self::handle_delete_subword_left))
            .on_action(cx.listener(Self::handle_delete_subword_right))
            .on_action(cx.listener(Self::handle_select_all))
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
             .on_action(cx.listener(|editor, _: &actions::FoldAll, _window, cx| editor.fold_all(cx)))
             .on_action(cx.listener(|editor, _: &actions::UnfoldAll, _window, cx| editor.unfold_all(cx)))
             // Raw key_down: vim routing + printable character insertion only
             .on_key_down(cx.listener(Self::handle_key_down))
             .on_scroll_wheel(cx.listener(Self::handle_scroll_wheel))
             .on_mouse_down(MouseButton::Left, cx.listener(Self::handle_mouse_down))
             .on_mouse_up(MouseButton::Left, cx.listener(Self::handle_mouse_up))
             .on_mouse_down(MouseButton::Right, cx.listener(Self::handle_right_click))
             .on_mouse_move(cx.listener(Self::handle_hover))
            .child(EditorElement::new(cx.entity().clone()))
    }
}

impl TextEditor {
    /// Handle raw keyboard input events.
    ///
    /// This handler is intentionally minimal: it only deals with cases that
    /// cannot be expressed as static actions — vim mode routing and printable
    /// character (text) insertion. Everything else is handled via `on_action`
    /// handlers registered in `Render::render`.
    fn handle_key_down(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let key = event.keystroke.key.as_str();
        let ctrl_or_cmd = event.keystroke.modifiers.platform || event.keystroke.modifiers.control;

        // ── Vim mode routing ──────────────────────────────────────────────────
        // ctrl-r (redo) is a special vim binding; route it through vim when active.
        let is_ctrl_r = ctrl_or_cmd && key == "r";
        if self.vim_state.is_some() {
            // Always let the action-bound shortcuts (ctrl+c/v/x/z/a/f/h/space)
            // fall through to their on_action handlers.
            let bypass_vim = ctrl_or_cmd
                && matches!(key, "c" | "v" | "x" | "z" | "a" | "f" | "h" | "space")
                && !is_ctrl_r;

            if !bypass_vim && self.handle_vim_key(event, window, cx) {
                return;
            }
        }

        // ── Find-panel character input ────────────────────────────────────────
        // When the find panel is open, printable characters feed the panel's
        // search / replace input field.  Non-character keys (backspace, enter,
        // escape, tab, f3) are handled by their dedicated on_action handlers
        // which check `find_state.is_some()` first.
        if self.find_state.is_some() && !ctrl_or_cmd {
            let alt = event.keystroke.modifiers.alt;

            // Alt+R toggles regex mode; Alt+L toggles search-in-selection.
            // These must be intercepted before the printable-char path below so
            // that the letters 'r' / 'l' are not fed into the search box.
            if alt && key == "r" {
                self.find_toggle_regex(cx);
                return;
            }
            if alt && key == "l" {
                self.find_toggle_search_in_selection(cx);
                return;
            }
            // Alt+Enter converts all matches to multi-cursor selections (feat-032).
            if alt && key == "enter" {
                self.find_select_all_matches(cx);
                return;
            }

            if let Some(text) = &event.keystroke.key_char {
                if !text.is_empty() && !text.chars().any(|c| c.is_control()) {
                    for ch in text.chars() {
                        self.find_input_char(ch, cx);
                    }
                    return;
                }
            }
        }

        // ── Go-to-line dialog character input ─────────────────────────────────
        // Digit keys, Backspace, Enter, and Escape are the only inputs the dialog
        // consumes.  All other keys are intentionally ignored so they don't
        // accidentally edit the buffer while the overlay is open.
        if self.goto_line_state.is_some() {
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
            if let Some(text) = &event.keystroke.key_char {
                if text.chars().all(|c| c.is_ascii_digit()) && !text.is_empty() {
                    for ch in text.chars() {
                        self.goto_line_input_char(ch, cx);
                    }
                    return;
                }
            }
            // Swallow any other key so the buffer is not modified while the dialog is open.
            return;
        }

        // ── Rename dialog character input ──────────────────────────────────────
        // While the inline rename overlay is visible, all input feeds the new
        // name field; Enter commits, Escape cancels, other keys edit the name.
        if self.rename_state.is_some() {
            match key {
                "enter" => {
                    self.rename_confirm(cx);
                    return;
                }
                "escape" => {
                    self.rename_cancel(cx);
                    return;
                }
                "backspace" => {
                    self.rename_input_char('\x08', cx);
                    return;
                }
                _ => {}
            }
            if let Some(text) = &event.keystroke.key_char {
                if !text.is_empty() && !text.chars().any(|c| c.is_control()) {
                    for ch in text.chars() {
                        self.rename_input_char(ch, cx);
                    }
                    return;
                }
            }
            // Swallow everything else so the buffer isn't edited while renaming.
            return;
        }

        // ── Context menu keyboard navigation ──────────────────────────────────
        if self.context_menu.is_some() {
            match key {
                "escape" => {
                    self.context_menu = None;
                    cx.notify();
                    return;
                }
                "up" => {
                    self.context_menu_move(-1, cx);
                    return;
                }
                "down" => {
                    self.context_menu_move(1, cx);
                    return;
                }
                "enter" => {
                    self.context_menu_activate(window, cx);
                    return;
                }
                _ => {}
            }
            return; // Swallow unhandled keys while menu is open
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
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_cursor_left(window, cx);
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_move_right(
        &mut self,
        _: &actions::MoveRight,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_cursor_right(window, cx);
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_move_up(&mut self, _: &actions::MoveUp, window: &mut Window, cx: &mut Context<Self>) {
        if self.is_completion_menu_open(cx) {
            self.completion_menu_select_previous();
        } else {
            self.move_cursor_up(window, cx);
            self.selection.set_position(self.cursor.position());
        }
        cx.notify();
    }

    fn handle_move_down(
        &mut self,
        _: &actions::MoveDown,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.is_completion_menu_open(cx) {
            self.completion_menu_select_next();
        } else {
            self.move_cursor_down(window, cx);
            self.selection.set_position(self.cursor.position());
        }
        cx.notify();
    }

    fn handle_move_to_beginning_of_line(
        &mut self,
        _: &actions::MoveToBeginningOfLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.smart_home(false);
        cx.notify();
    }

    fn handle_move_to_end_of_line(
        &mut self,
        _: &actions::MoveToEndOfLine,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_cursor_to_line_end(window, cx);
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_move_to_beginning(
        &mut self,
        _: &actions::MoveToBeginning,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_cursor_to_document_start(window, cx);
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_move_to_end(
        &mut self,
        _: &actions::MoveToEnd,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_cursor_to_document_end(window, cx);
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_move_to_previous_word_start(
        &mut self,
        _: &actions::MoveToPreviousWordStart,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_cursor_to_prev_word(window, cx);
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_move_to_next_word_end(
        &mut self,
        _: &actions::MoveToNextWordEnd,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_cursor_to_next_word(window, cx);
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_move_to_paragraph_start(
        &mut self,
        _: &actions::MoveToParagraphStart,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_to_paragraph_start();
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_move_to_paragraph_end(
        &mut self,
        _: &actions::MoveToParagraphEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_to_paragraph_end();
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_move_to_next_subword_end(
        &mut self,
        _: &actions::MoveToNextSubwordEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_to_next_subword();
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_move_to_previous_subword_start(
        &mut self,
        _: &actions::MoveToPreviousSubwordStart,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.move_to_prev_subword();
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_page_up(&mut self, _: &actions::PageUp, window: &mut Window, cx: &mut Context<Self>) {
        let viewport_lines = self.last_viewport_lines.max(1);
        self.scroll_page_up(viewport_lines);
        for _ in 0..viewport_lines {
            self.move_cursor_up(window, cx);
        }
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_page_down(
        &mut self,
        _: &actions::PageDown,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let viewport_lines = self.last_viewport_lines.max(1);
        self.scroll_page_down(viewport_lines);
        for _ in 0..viewport_lines {
            self.move_cursor_down(window, cx);
        }
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    // ============================================================================
    // Action handlers — selection
    // ============================================================================

    fn handle_select_left(
        &mut self,
        _: &actions::SelectLeft,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_cursor_left(window, cx);
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_right(
        &mut self,
        _: &actions::SelectRight,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_cursor_right(window, cx);
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_up(
        &mut self,
        _: &actions::SelectUp,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_cursor_up(window, cx);
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_down(
        &mut self,
        _: &actions::SelectDown,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_cursor_down(window, cx);
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_to_beginning_of_line(
        &mut self,
        _: &actions::SelectToBeginningOfLine,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.smart_home(true);
        cx.notify();
    }

    fn handle_select_to_end_of_line(
        &mut self,
        _: &actions::SelectToEndOfLine,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_cursor_to_line_end(window, cx);
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_to_beginning(
        &mut self,
        _: &actions::SelectToBeginning,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_cursor_to_document_start(window, cx);
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_to_end(
        &mut self,
        _: &actions::SelectToEnd,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_cursor_to_document_end(window, cx);
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_to_previous_word_start(
        &mut self,
        _: &actions::SelectToPreviousWordStart,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_cursor_to_prev_word(window, cx);
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_to_next_word_end(
        &mut self,
        _: &actions::SelectToNextWordEnd,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_cursor_to_next_word(window, cx);
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_to_paragraph_start(
        &mut self,
        _: &actions::SelectToParagraphStart,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_to_paragraph_start();
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_to_paragraph_end(
        &mut self,
        _: &actions::SelectToParagraphEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_to_paragraph_end();
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_to_next_subword_end(
        &mut self,
        _: &actions::SelectToNextSubwordEnd,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_to_next_subword();
        self.selection.extend_to(self.cursor.position());
        cx.notify();
    }

    fn handle_select_to_previous_subword_start(
        &mut self,
        _: &actions::SelectToPreviousSubwordStart,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.selection.has_selection() {
            self.selection.start_selection(self.cursor.position());
        }
        self.move_to_prev_subword();
        self.selection.extend_to(self.cursor.position());
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

    // ============================================================================
    // Action handlers — editing
    // ============================================================================

    fn handle_backspace(
        &mut self,
        _: &actions::Backspace,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.find_state.is_some() {
            self.find_input_backspace(cx);
            return;
        }
        if self.has_selection() {
            self.delete_selection();
        } else {
            self.delete_before_cursor(window, cx);
        }
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_delete(&mut self, _: &actions::Delete, window: &mut Window, cx: &mut Context<Self>) {
        if self.has_selection() {
            self.delete_selection();
        } else {
            self.delete_at_cursor(window, cx);
        }
        self.selection.set_position(self.cursor.position());
        cx.notify();
    }

    fn handle_newline(
        &mut self,
        _: &actions::Newline,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.find_state.is_some() {
            self.find_next(cx);
            return;
        }
        if let Some(ref menu) = self.completion_menu {
            if !menu.items.is_empty() {
                self.accept_completion(window, cx);
                return;
            }
        }
        if self.has_selection() {
            self.delete_selection();
        }
        self.insert_newline_with_auto_indent(window, cx);
        cx.notify();
    }

    fn handle_tab(&mut self, _: &actions::Tab, window: &mut Window, cx: &mut Context<Self>) {
        if self.find_state.is_some() {
            self.find_toggle_field_focus(cx);
            return;
        }
        if let Some(ref menu) = self.completion_menu {
            if !menu.items.is_empty() {
                self.accept_completion(window, cx);
                return;
            }
        }
        // With a multi-line selection, Tab indents all selected lines rather than
        // replacing the selection with a single tab-stop (matches VS Code behaviour).
        if self.has_selection() && self.is_multiline_selection() {
            self.indent_lines();
        } else {
            if self.has_selection() {
                self.delete_selection();
            }
            self.insert_at_cursor("    ", window, cx);
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
            self.selection.set_position(self.cursor.position());
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
            self.clipboard_is_whole_line = false;
        } else {
            // No selection → copy the entire current line (feat-023).
            self.copy_whole_line(cx);
        }
    }

    fn handle_cut(&mut self, _: &actions::Cut, _window: &mut Window, cx: &mut Context<Self>) {
        if self.has_selection() {
            if self.cut(cx) {
                self.clipboard_is_whole_line = false;
                cx.notify();
            }
        } else {
            // No selection → cut the entire current line (feat-023).
            self.cut_whole_line(cx);
            cx.notify();
        }
    }

    fn handle_paste(&mut self, _: &actions::Paste, window: &mut Window, cx: &mut Context<Self>) {
        if self.clipboard_is_whole_line && !self.has_selection() {
            // Whole-line paste: insert the line above the cursor line (feat-023).
            let Some(clipboard_item) = cx.read_from_clipboard() else {
                return;
            };
            let Some(clipboard_text) = clipboard_item.text() else {
                return;
            };
            let line = self.cursor.position().line;
            let insert_offset = self
                .buffer
                .position_to_offset(Position::new(line, 0))
                .unwrap_or(0);
            // Ensure there's a newline at the end of the pasted line so it
            // occupies its own line.
            let text_to_insert = if clipboard_text.ends_with('\n') {
                clipboard_text.to_string()
            } else {
                format!("{}\n", clipboard_text)
            };
            self.break_undo_group();
            if self.buffer.insert(insert_offset, &text_to_insert).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
                // Cursor stays on the original content which shifted down one line.
                let new_pos = Position::new(line + 1, self.cursor.position().column);
                self.cursor.set_position(new_pos);
                self.selection.set_position(new_pos);
                self.update_syntax_highlights();
                self.scroll_to_cursor();
            }
        } else {
            self.paste(window, cx);
            self.clipboard_is_whole_line = false;
        }
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
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_find(cx);
    }

    fn handle_open_find_replace(
        &mut self,
        _: &actions::OpenFindReplace,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_find_replace(cx);
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

    // ============================================================================
    // Action handlers — completions
    // ============================================================================

    fn handle_trigger_completion(
        &mut self,
        _: &actions::TriggerCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.trigger_completions(window, cx);
        cx.notify();
    }

    fn handle_accept_completion(
        &mut self,
        _: &actions::AcceptCompletion,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.accept_completion(window, cx);
    }

    fn handle_dismiss_completion(
        &mut self,
        _: &actions::DismissCompletion,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.completion_menu = None;
        cx.notify();
    }

    // ============================================================================
    // Action handlers — misc
    // ============================================================================

    fn handle_escape(&mut self, _: &actions::Escape, _window: &mut Window, cx: &mut Context<Self>) {
        if self.hover_state.is_some() {
            self.hover_state = None;
            cx.notify();
            return;
        }
        if self.completion_menu.is_some() {
            self.completion_menu = None;
            cx.notify();
            return;
        }
        // Collapse extra cursors before closing find so a single Escape always
        // dismisses multi-cursor mode first, then a second press closes find.
        if !self.extra_cursors.is_empty() {
            self.collapse_to_primary_cursor();
            cx.notify();
            return;
        }
        if self.find_state.is_some() {
            self.close_find(cx);
        }
    }

    /// Check if completions should be auto-triggered based on the inserted text
    /// and current context (word prefix at cursor)
    fn should_auto_trigger_completions(
        text: &str,
        buffer: &TextBuffer,
        cursor_offset: usize,
    ) -> bool {
        // Trigger on dot (for table.column completions)
        if text == "." {
            return true;
        }

        // Trigger on space (for keyword-based completions)
        if text == " " {
            return true;
        }

        // Trigger after typing a trigger keyword
        let trigger_keywords = [
            "SELECT", "FROM", "WHERE", "JOIN", "ORDER", "GROUP", "HAVING", "UPDATE", "INSERT",
            "DELETE", "CREATE", "ALTER", "DROP", "SET", "INTO", "VALUES", "BY", "ON", "AS", "AND",
            "OR", "NOT", "IN", "LIKE", "BETWEEN", "NULL", "IS", "TRUE", "FALSE", "CASE", "WHEN",
            "THEN", "ELSE", "END",
        ];
        for keyword in trigger_keywords {
            if text.eq_ignore_ascii_case(keyword) {
                return true;
            }
        }

        // Also trigger if there's a word prefix (1+ characters) at cursor
        // This provides basic auto-completion after typing
        if cursor_offset > 0 {
            // Look back to find the start of the word
            let text_content = buffer.text();
            let mut start = cursor_offset.saturating_sub(1);
            let mut found_word_char = false;

            // Convert to char indices
            let chars: Vec<char> = text_content.chars().collect();

            while start > 0 {
                if let Some(&ch) = chars.get(start) {
                    if ch.is_alphanumeric() || ch == '_' {
                        found_word_char = true;
                        start = start.saturating_sub(1);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }

            // If we found a word and it's at least 1 character, trigger completions
            if found_word_char || cursor_offset > 0 {
                // Check if the prefix is at least 1 character
                let prefix_start = if found_word_char { start + 1 } else { start };
                if cursor_offset > prefix_start {
                    return true;
                }
            }
        }

        false
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

        // If the completion menu is visible and the click lands inside it, treat
        // the click as "select this item and accept" rather than moving the cursor.
        // We compare against window-coordinate bounds because GPUI delivers event
        // positions in window space.
        if let Some(ref menu_bounds) = self.cached_completion_menu_bounds {
            if menu_bounds.bounds.contains(&event.position) {
                let relative_y = event.position.y - menu_bounds.bounds.origin.y;
                let slot = (f32::from(relative_y) / f32::from(menu_bounds.item_height)).floor() as usize;
                let slot = slot.min(menu_bounds.item_count.saturating_sub(1));
                if let Some(ref mut menu) = self.completion_menu {
                    // `slot` is relative to the visible window; add scroll_offset to get
                    // the absolute index into the full item list.
                    menu.selected_index = menu.scroll_offset + slot;
                }
                self.accept_completion(window, cx);
                cx.notify();
                return;
            }
        }

        // Check if the click landed on a fold chevron in the gutter.
        // The chevron rects are in window coordinates (same space as event.position).
        {
            let click_x = event.position.x - self.cached_bounds_origin.x;
            if click_x < gpui::px(self.cached_gutter_width) {
                let chevrons = self.cached_fold_chevrons.clone();
                for (start_line, rect) in &chevrons {
                    if rect.contains(&event.position) {
                        self.toggle_fold(*start_line, cx);
                        return;
                    }
                }
            }
        }

        // Request focus so keyboard events are routed here.
        self.focus_handle.focus(window, cx);

        let line_height = window.line_height();

        // Mouse event positions are in window coordinates. Use the bounds origin
        // cached during the last prepaint to convert to element-relative coordinates.
        let position = self.pixel_to_position(
            event.position,
            self.cached_bounds_origin,
            line_height,
        );

        let now = std::time::Instant::now();
        let click_count = match &self.last_click {
            Some((last_time, last_pos))
                if last_time.elapsed() < std::time::Duration::from_millis(500)
                    && *last_pos == position =>
            {
                // Increment – but cap at 3 so repeated clicks cycle triple→single.
                (event.click_count).min(3)
            }
            _ => 1,
        };
        self.last_click = Some((now, position));

        match click_count {
            3 => {
                // Triple-click: select the entire line.
                let line = position.line;
                let line_start = Position::new(line, 0);
                let line_end = if line + 1 < self.buffer.line_count() {
                    Position::new(line + 1, 0)
                } else {
                    // Last line — go to end of content.
                    let line_len = self
                        .buffer
                        .line(line)
                        .map(|l| l.trim_end_matches('\n').len())
                        .unwrap_or(0);
                    Position::new(line, line_len)
                };
                self.cursor.set_position(line_start);
                self.selection.start_selection(line_start);
                self.selection.extend_to(line_end);
                self.mouse_drag_anchor = Some(line_start);
            }
            2 => {
                // Double-click: select the word under the pointer.
                let offset = self.buffer.position_to_offset(position).unwrap_or(0);
                if let Some(word_range) = self.find_word_range_at_offset(offset) {
                    let start_pos = self
                        .buffer
                        .offset_to_position(word_range.start)
                        .unwrap_or(position);
                    let end_pos = self
                        .buffer
                        .offset_to_position(word_range.end)
                        .unwrap_or(position);
                    self.cursor.set_position(end_pos);
                    self.selection.start_selection(start_pos);
                    self.selection.extend_to(end_pos);
                    self.mouse_drag_anchor = Some(start_pos);
                } else {
                    self.cursor.set_position(position);
                    self.selection.set_position(position);
                    self.mouse_drag_anchor = Some(position);
                }
            }
            _ => {
                // Single click.
                if event.modifiers.shift && self.selection.has_selection() {
                    // Shift-click: extend the existing selection from its anchor.
                    self.cursor.set_position(position);
                    self.selection.extend_to(position);
                } else {
                    self.cursor.set_position(position);
                    self.selection.set_position(position);
                    self.mouse_drag_anchor = Some(position);
                }
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
        self.mouse_drag_anchor = None;
        cx.notify();
    }

    /// Handle mouse scroll wheel events (Phase 7: Scrolling)
    fn handle_scroll_wheel(
        &mut self,
        event: &ScrollWheelEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let line_height = window.line_height();
        let delta = event.delta.pixel_delta(line_height);

        // When the completion menu is open, always capture scroll for the menu
        // regardless of pointer position. This prevents the confusing UX where
        // scrolling only works once the user moves the pointer inside the menu.
        if self.completion_menu.is_some() {
            if let Some(ref mut menu) = self.completion_menu {
                // Accumulate fractional deltas so small wheel movements are not
                // silently lost to integer truncation.
                menu.scroll_accumulator -= delta.y / line_height;
                let steps = menu.scroll_accumulator.trunc() as i32;
                menu.scroll_accumulator -= steps as f32;

                if steps != 0 {
                    let max_offset = menu
                        .items
                        .len()
                        .saturating_sub(crate::element::MAX_COMPLETION_ITEMS);
                    let new_offset = (menu.scroll_offset as i32 + steps)
                        .clamp(0, max_offset as i32) as usize;
                    menu.scroll_offset = new_offset;
                    // Keep the selection inside the visible window.
                    if menu.selected_index < new_offset {
                        menu.selected_index = new_offset;
                    } else if menu.selected_index
                        >= new_offset + crate::element::MAX_COMPLETION_ITEMS
                    {
                        menu.selected_index =
                            new_offset + crate::element::MAX_COMPLETION_ITEMS - 1;
                    }
                }
            }
            cx.notify();
            return;
        }

        // No completion menu — scroll the editor buffer.
        // delta.y is positive for scroll up, negative for scroll down;
        // negate so positive scroll_lines moves the view downward.
        let scroll_lines = -(delta.y / line_height);
        self.scroll_by(scroll_lines);
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
        let line_height = window.line_height();

        // Mouse event positions are in window coordinates. Use the bounds origin
        // cached during the last prepaint to convert to element-relative coordinates.
        let position = self.pixel_to_position(
            event.position,
            self.cached_bounds_origin,
            line_height,
        );

        // While the left button is held, extend the selection rather than updating
        // the hover tooltip.
        if let Some(anchor) = self.mouse_drag_anchor {
            self.cursor.set_position(position);
            self.selection.start_selection(anchor);
            self.selection.extend_to(position);
            self.scroll_to_cursor();
            cx.notify();
            return;
        }

        // No drag — update hover tooltip.
        self.update_hover_at_position(position.line, position.column, window, cx);
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
        let text = self.buffer.text();
        let mut utf16_offset = 0usize;
        let mut start_byte = 0usize;
        let mut end_byte = text.len();
        let mut found_start = false;

        for (byte_pos, ch) in text.char_indices() {
            if utf16_offset == utf16_range.start {
                start_byte = byte_pos;
                found_start = true;
            }
            if utf16_offset == utf16_range.end {
                end_byte = byte_pos;
                break;
            }
            utf16_offset += ch.len_utf16();
        }

        // Handle end at the very end of the string
        if utf16_offset == utf16_range.end {
            end_byte = text.len();
        }
        if !found_start && utf16_offset >= utf16_range.start {
            start_byte = text.len();
        }

        start_byte..end_byte
    }

    /// Convert a byte-offset range in the rope buffer to a UTF-16 code-unit range.
    fn utf16_range_from_bytes(&self, byte_range: std::ops::Range<usize>) -> std::ops::Range<usize> {
        let text = self.buffer.text();
        let clamped_start = byte_range.start.min(text.len());
        let clamped_end = byte_range.end.min(text.len());
        let before_start = &text[..clamped_start];
        let segment = &text[clamped_start..clamped_end];
        let start_utf16: usize = before_start.chars().map(|c| c.len_utf16()).sum();
        let len_utf16: usize = segment.chars().map(|c| c.len_utf16()).sum();
        start_utf16..start_utf16 + len_utf16
    }

    /// Return the current cursor position as a byte offset, clamped to valid range.
    fn cursor_byte_offset(&self) -> usize {
        self.buffer
            .position_to_offset(self.cursor.position())
            .unwrap_or(0)
    }

    // ============================================================================
    // Text transforms — case (feat-033/034)
    // ============================================================================

    /// Apply a string transform to every cursor's selection (or word under cursor).
    ///
    /// Transforms are applied back-to-front so byte offsets remain valid after
    /// each substitution. The entire operation is wrapped in a single undo group.
    fn apply_transform_to_all_cursors(&mut self, transform: impl Fn(&str) -> String) {
        self.break_undo_group();
        let text = self.buffer.text();

        // Collect (byte_start, byte_end) for every cursor. The primary cursor
        // is included first, then extra cursors.
        let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();

        let primary_range = if self.selection.has_selection() {
            let sel_range = self.selection.range();
            let start = self
                .buffer
                .position_to_offset(sel_range.start)
                .unwrap_or(0);
            let end = self
                .buffer
                .position_to_offset(sel_range.end)
                .unwrap_or(start);
            start..end
        } else {
            let offset = self.cursor_byte_offset();
            self.find_word_range_at_offset(offset)
                .unwrap_or(offset..offset)
        };
        ranges.push(primary_range);

        for (extra_cursor, extra_selection) in &self.extra_cursors {
            let range = if extra_selection.has_selection() {
                let sel_range = extra_selection.range();
                let start = self
                    .buffer
                    .position_to_offset(sel_range.start)
                    .unwrap_or(0);
                let end = self
                    .buffer
                    .position_to_offset(sel_range.end)
                    .unwrap_or(start);
                start..end
            } else {
                let offset = self
                    .buffer
                    .position_to_offset(extra_cursor.position())
                    .unwrap_or(0);
                self.find_word_range_at_offset(offset)
                    .unwrap_or(offset..offset)
            };
            ranges.push(range);
        }

        // Sort back-to-front and deduplicate so overlapping ranges are skipped.
        ranges.sort_by(|a, b| b.start.cmp(&a.start));
        ranges.dedup_by_key(|r| r.start);

        for range in ranges {
            if range.start >= range.end {
                continue;
            }
            let original = text[range.clone()].to_string();
            let transformed = transform(&original);
            if transformed == original {
                continue;
            }
            if self.buffer.delete(range.clone()).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
            }
            if self.buffer.insert(range.start, &transformed).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
            }
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
        self.break_undo_group();
        let (first, last) = self.selected_line_range();
        let line_count = self.buffer.line_count();

        let lines: Vec<String> = (first..=last)
            .filter_map(|l| self.buffer.line(l).map(|s| s.to_string()))
            .collect();

        let transformed_lines = transform(&lines);

        // Reconstruct the text block, preserving whether the last line has a
        // trailing newline (it does unless it is the final line of the document).
        let new_text = if last + 1 < line_count {
            transformed_lines.join("\n") + "\n"
        } else {
            transformed_lines.join("\n")
        };

        let start = self
            .buffer
            .position_to_offset(Position::new(first, 0))
            .unwrap_or(0);
        let end = if last + 1 < line_count {
            self.buffer
                .position_to_offset(Position::new(last + 1, 0))
                .unwrap_or(self.buffer.text().len())
        } else {
            self.buffer.text().len()
        };

        if self.buffer.delete(start..end).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }
        if self.buffer.insert(start, &new_text).is_ok() {
            if let Some(change) = self.buffer.changes().last().cloned() {
                self.push_undo(change);
            }
        }

        self.update_syntax_highlights();
    }

    fn sort_lines_ascending(&mut self) {
        self.transform_selected_lines(|lines| {
            let mut sorted = lines.to_vec();
            sorted.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()));
            sorted
        });
    }

    fn sort_lines_descending(&mut self) {
        self.transform_selected_lines(|lines| {
            let mut sorted = lines.to_vec();
            sorted.sort_by(|a, b| b.to_lowercase().cmp(&a.to_lowercase()));
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

        // Gather byte offsets for all cursors (extra first, then primary).
        let mut offsets: Vec<usize> = self
            .extra_cursors
            .iter()
            .map(|(cursor, _)| {
                self.buffer
                    .position_to_offset(cursor.position())
                    .unwrap_or(0)
            })
            .collect();
        offsets.push(self.cursor_byte_offset());
        // Apply back-to-front so earlier offsets remain valid.
        offsets.sort_by(|a, b| b.cmp(a));

        for offset in offsets {
            if self.buffer.insert(offset, text).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
            }
        }

        // Advance the primary cursor past the inserted text.
        let new_offset = self.cursor_byte_offset() + text.len();
        if let Ok(new_pos) = self.buffer.offset_to_position(new_offset) {
            self.cursor.set_position(new_pos);
            self.selection.set_position(new_pos);
        }

        self.update_syntax_highlights();
        cx.notify();
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
        if self.extra_cursors.is_empty() {
            // Nothing to rotate with only one cursor.
            return;
        }
        self.break_undo_group();

        // Collect byte ranges for every cursor in document order.
        let mut cursor_ranges: Vec<std::ops::Range<usize>> = Vec::new();

        let primary_range = if self.selection.has_selection() {
            let sel_range = self.selection.range();
            let start = self
                .buffer
                .position_to_offset(sel_range.start)
                .unwrap_or(0);
            let end = self
                .buffer
                .position_to_offset(sel_range.end)
                .unwrap_or(start);
            start..end
        } else {
            let offset = self.cursor_byte_offset();
            self.find_word_range_at_offset(offset)
                .unwrap_or(offset..offset)
        };
        cursor_ranges.push(primary_range);

        for (extra_cursor, extra_selection) in &self.extra_cursors {
            let range = if extra_selection.has_selection() {
                let sel_range = extra_selection.range();
                let start = self
                    .buffer
                    .position_to_offset(sel_range.start)
                    .unwrap_or(0);
                let end = self
                    .buffer
                    .position_to_offset(sel_range.end)
                    .unwrap_or(start);
                start..end
            } else {
                let offset = self
                    .buffer
                    .position_to_offset(extra_cursor.position())
                    .unwrap_or(0);
                self.find_word_range_at_offset(offset)
                    .unwrap_or(offset..offset)
            };
            cursor_ranges.push(range);
        }

        // Sort into document order before extracting text.
        cursor_ranges.sort_by_key(|r| r.start);

        let text = self.buffer.text();
        let texts: Vec<String> = cursor_ranges
            .iter()
            .map(|r| text[r.clone()].to_string())
            .collect();

        let count = texts.len();
        // Each slot receives the text from the previous slot (wrapping around),
        // which rotates content "forward" by one position.
        let rotated: Vec<String> = (0..count)
            .map(|i| texts[(i + count - 1) % count].clone())
            .collect();

        // Apply substitutions back-to-front so earlier offsets remain stable.
        let mut indexed: Vec<(std::ops::Range<usize>, String)> = cursor_ranges
            .into_iter()
            .zip(rotated)
            .collect();
        indexed.sort_by(|(a, _), (b, _)| b.start.cmp(&a.start));

        for (range, new_text) in indexed {
            if range.start >= range.end {
                continue;
            }
            if self.buffer.delete(range.clone()).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
            }
            if self.buffer.insert(range.start, &new_text).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
            }
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
        if self.selection.has_selection() {
            let sel_range = self.selection.range();
            let current_head = self.cursor.position();
            // The anchor is whichever endpoint is NOT the cursor.
            let anchor = if current_head == sel_range.end {
                sel_range.start
            } else {
                sel_range.end
            };
            self.cursor.set_position(anchor);
            self.selection.start_selection(current_head);
            self.selection.extend_to(anchor);
        }

        for (extra_cursor, extra_selection) in &mut self.extra_cursors {
            if extra_selection.has_selection() {
                let sel_range = extra_selection.range();
                let current_head = extra_cursor.position();
                let anchor = if current_head == sel_range.end {
                    sel_range.start
                } else {
                    sel_range.end
                };
                extra_cursor.set_position(anchor);
                extra_selection.start_selection(current_head);
                extra_selection.extend_to(anchor);
            }
        }

        cx.notify();
    }

    // ============================================================================
    // Copy as Markdown (feat-050)
    // ============================================================================

    /// Copy the selected text (or entire buffer) wrapped in a SQL fenced code
    /// block so it can be pasted into Markdown documents.
    fn copy_as_markdown(&mut self, cx: &mut Context<Self>) {
        let content = if self.selection.has_selection() {
            let sel_range = self.selection.range();
            let start = self
                .buffer
                .position_to_offset(sel_range.start)
                .unwrap_or(0);
            let end = self
                .buffer
                .position_to_offset(sel_range.end)
                .unwrap_or(self.buffer.text().len());
            self.buffer.text()[start..end].to_string()
        } else {
            self.buffer.text()
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

        if self.has_selection() {
            self.delete_selection();
        }
        self.insert_at_cursor(&normalised, window, cx);
        if let Some(change) = self.buffer.changes().last().cloned() {
            self.push_undo(change);
        }
        self.update_syntax_highlights();
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
        self.goto_line_state = Some(GoToLineState {
            query: String::new(),
            original_cursor: self.cursor.position(),
            is_valid: true,
        });
        cx.notify();
    }

    /// Feed a character typed while the go-to-line dialog is open.
    ///
    /// Only digit characters are accepted; other input is silently ignored so
    /// the dialog stays clean.
    pub fn goto_line_input_char(&mut self, c: char, cx: &mut Context<Self>) {
        let Some(state) = self.goto_line_state.as_mut() else {
            return;
        };

        if c.is_ascii_digit() {
            state.query.push(c);
        } else if c == '\x08' {
            // Backspace within the dialog
            state.query.pop();
        } else {
            return;
        }

        let line_count = self.buffer.line_count();
        let parsed = state.query.parse::<usize>();
        state.is_valid = parsed
            .as_ref()
            .map(|&n| n >= 1 && n <= line_count)
            .unwrap_or(true); // empty query is "valid" (no red border)

        // Live preview: scroll to the typed line if it is in range
        if let Ok(n) = parsed {
            let target_line = (n.saturating_sub(1)).min(line_count.saturating_sub(1));
            let new_pos = Position::new(target_line, 0);
            self.cursor.set_position(new_pos);
            self.selection.set_position(new_pos);
            self.scroll_to_cursor();
        }

        cx.notify();
    }

    /// Confirm the dialog (Enter) — keep the cursor where it is and close.
    pub fn goto_line_confirm(&mut self, cx: &mut Context<Self>) {
        self.goto_line_state = None;
        cx.notify();
    }

    /// Cancel the dialog (Escape) — restore the cursor to where it was before
    /// the dialog opened.
    pub fn goto_line_cancel(&mut self, cx: &mut Context<Self>) {
        if let Some(state) = self.goto_line_state.take() {
            self.cursor.set_position(state.original_cursor);
            self.selection.set_position(state.original_cursor);
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
        self.soft_wrap = !self.soft_wrap;
        cx.notify();
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
        let original = self.buffer.text().to_string();
        if original.trim().is_empty() {
            return;
        }

        let formatter = SqlFormatter::with_defaults();
        let formatted = match formatter.format(&original) {
            Ok(text) => text,
            // Leave buffer unchanged if the SQL cannot be formatted
            Err(_) => return,
        };

        if formatted == original {
            return;
        }

        // Record cursor position before the replace so we can try to restore it
        let original_cursor = self.cursor.position();

        self.break_undo_group();

        // A whole-buffer replace is a single Change: delete old, insert new
        let change = buffer::Change::replace(0, original.clone(), formatted.clone());
        if self.buffer.apply_change(&change).is_ok() {
            self.push_undo(change);

            // Best-effort cursor restore: keep the same line if it still exists,
            // otherwise clamp to the last line.
            let new_line_count = self.buffer.line_count();
            let target_line = original_cursor.line.min(new_line_count.saturating_sub(1));
            let target_col = {
                let line_len = self.buffer.line(target_line).map(|l| l.len()).unwrap_or(0);
                original_cursor.column.min(line_len)
            };
            let new_pos = Position::new(target_line, target_col);
            self.cursor.set_position(new_pos);
            self.selection.set_position(new_pos);
            self.scroll_to_cursor();
            self.update_syntax_highlights();
            cx.notify();
        }
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
        let provider = self.lsp.definition_provider.clone();
        let Some(provider) = provider else { return };
        let rope = ropey::Rope::from_str(&self.buffer.text());
        let Some(target_offset) = provider.definition(&rope, offset) else { return };
        if let Ok(pos) = self.buffer.offset_to_position(target_offset) {
            // Clear extra cursors — navigating to a definition is a single-point op.
            self.extra_cursors.clear();
            self.cursor.set_position(pos);
            self.selection.set_position(pos);
            // Also clear stale reference highlights since the cursor moved.
            self.reference_ranges.clear();
            self.scroll_to_cursor();
            cx.notify();
        }
    }

    // ============================================================================
    // Find references (feat-047)
    // ============================================================================

    /// Highlight all references to the symbol under the caret in the current buffer.
    ///
    /// Results are stored in `self.reference_ranges` and rendered by `EditorElement`.
    /// Calling again clears the previous results and re-runs the search.
    fn handle_find_references(
        &mut self,
        _: &actions::FindReferences,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let offset = self.cursor_byte_offset();
        let provider = self.lsp.references_provider.clone();
        let Some(provider) = provider else { return };
        let rope = ropey::Rope::from_str(&self.buffer.text());
        self.reference_ranges = provider.references(&rope, offset);
        cx.notify();
    }

    // ============================================================================
    // Rename symbol (feat-048)
    // ============================================================================

    /// Open the inline rename dialog for the word under the caret.
    ///
    /// The dialog shows a small input overlay painted by `EditorElement`. While it
    /// is open, key input is routed to `rename_input_char` / `rename_confirm` /
    /// `rename_cancel` instead of the buffer.
    fn handle_rename_symbol(
        &mut self,
        _: &actions::RenameSymbol,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let offset = self.cursor_byte_offset();
        let Some(range) = self.find_word_range_at_offset(offset) else { return };
        let word = self.buffer.text()[range.clone()].to_string();
        self.rename_state = Some(RenameState {
            new_name: word.clone(),
            word_start: range.start,
            word_end: range.end,
            original_word: word,
        });
        cx.notify();
    }

    /// Append or delete a character in the rename dialog's input field.
    ///
    /// `'\x08'` (ASCII backspace) removes the last character.
    fn rename_input_char(&mut self, ch: char, cx: &mut Context<Self>) {
        let Some(state) = self.rename_state.as_mut() else { return };
        if ch == '\x08' {
            state.new_name.pop();
        } else {
            state.new_name.push(ch);
        }
        cx.notify();
    }

    /// Commit the rename: replace every occurrence of `original_word` (whole-word
    /// matches) in the buffer with `new_name` as a single atomic undo group.
    fn rename_confirm(&mut self, cx: &mut Context<Self>) {
        let Some(state) = self.rename_state.take() else { return };
        if state.new_name.is_empty() || state.new_name == state.original_word {
            return;
        }

        // Collect all byte ranges where the original word appears, from back to
        // front so that applying replacements doesn't shift earlier offsets.
        let text = self.buffer.text();
        let mut ranges: Vec<std::ops::Range<usize>> = text
            .match_indices(&state.original_word as &str)
            .map(|(start, s)| start..start + s.len())
            .collect();
        ranges.sort_by(|a, b| b.start.cmp(&a.start));

        self.break_undo_group();
        for range in ranges {
            // Delete the old word, then insert the new name at the same offset.
            if self.buffer.delete(range.clone()).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
            }
            if self.buffer.insert(range.start, &state.new_name).is_ok() {
                if let Some(change) = self.buffer.changes().last().cloned() {
                    self.push_undo(change);
                }
            }
        }
        self.break_undo_group();

        self.update_syntax_highlights();
        self.update_diagnostics();
        cx.notify();
    }

    /// Cancel the rename dialog without modifying the buffer.
    fn rename_cancel(&mut self, cx: &mut Context<Self>) {
        self.rename_state = None;
        cx.notify();
    }

    // ============================================================================
    // Context menu (feat-045)
    // ============================================================================

    /// Build the standard context menu items for the current editor state.
    fn build_context_menu_items(&self) -> Vec<ContextMenuItem> {
        let has_selection = self.selection.has_selection();
        vec![
            ContextMenuItem { label: "Cut".into(), disabled: !has_selection, is_separator: false },
            ContextMenuItem { label: "Copy".into(), disabled: !has_selection, is_separator: false },
            ContextMenuItem { label: "Paste".into(), disabled: false, is_separator: false },
            ContextMenuItem { label: "".into(), disabled: false, is_separator: true },
            ContextMenuItem { label: "Select All".into(), disabled: false, is_separator: false },
            ContextMenuItem { label: "".into(), disabled: false, is_separator: true },
            ContextMenuItem {
                label: "Go to Definition".into(),
                disabled: self.lsp.definition_provider.is_none(),
                is_separator: false,
            },
            ContextMenuItem {
                label: "Find References".into(),
                disabled: self.lsp.references_provider.is_none(),
                is_separator: false,
            },
            ContextMenuItem {
                label: "Rename Symbol".into(),
                disabled: self.find_word_range_at_offset(self.buffer
                    .position_to_offset(self.cursor.position())
                    .unwrap_or(0))
                    .is_none(),
                is_separator: false,
            },
            ContextMenuItem { label: "".into(), disabled: false, is_separator: true },
            ContextMenuItem { label: "Format SQL".into(), disabled: false, is_separator: false },
        ]
    }

    /// Open the context menu at a specific pixel position.
    fn open_context_menu_at(&mut self, x: f32, y: f32, cx: &mut Context<Self>) {
        let items = self.build_context_menu_items();
        self.context_menu = Some(ContextMenuState {
            items,
            origin_x: x,
            origin_y: y,
            highlighted: None,
        });
        cx.notify();
    }

    /// Open the context menu at the current cursor position (keyboard shortcut).
    fn handle_open_context_menu_keyboard(
        &mut self,
        _: &actions::OpenContextMenu,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Position near the cursor line — the element layer will clamp it into view.
        let line = self.cursor.position().line as f32;
        // Use approximate pixel coords; the element will adjust during prepaint.
        self.open_context_menu_at(80.0, line * 20.0, cx);
    }

    /// Handle a right-click: move cursor to the click position and open the menu.
    fn handle_right_click(
        &mut self,
        event: &MouseDownEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Subtract the cached bounds origin to convert from window coords to
        // element-relative coords (the renderer adds bounds.origin back when
        // positioning the menu overlay).
        let x: f32 = (event.position.x - self.cached_bounds_origin.x).into();
        let y: f32 = (event.position.y - self.cached_bounds_origin.y).into();
        self.open_context_menu_at(x, y, cx);
    }

    /// Move the keyboard highlight up/down within the open context menu.
    fn context_menu_move(&mut self, delta: i32, cx: &mut Context<Self>) {
        let Some(state) = self.context_menu.as_mut() else { return };
        let non_sep: Vec<usize> = state
            .items
            .iter()
            .enumerate()
            .filter(|(_, item)| !item.is_separator && !item.disabled)
            .map(|(i, _)| i)
            .collect();
        if non_sep.is_empty() {
            return;
        }
        let current_pos = state
            .highlighted
            .and_then(|h| non_sep.iter().position(|&i| i == h));
        let next_pos = match current_pos {
            None => {
                if delta >= 0 { 0 } else { non_sep.len() - 1 }
            }
            Some(p) => {
                let len = non_sep.len() as i32;
                ((p as i32 + delta).rem_euclid(len)) as usize
            }
        };
        state.highlighted = Some(non_sep[next_pos]);
        cx.notify();
    }

    /// Activate the currently highlighted context menu item.
    fn context_menu_activate(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(state) = self.context_menu.take() else { return };
        let Some(highlighted) = state.highlighted else { return };
        let Some(item) = state.items.get(highlighted) else { return };
        if item.disabled || item.is_separator {
            return;
        }
        match item.label.as_str() {
            "Cut" => window.dispatch_action(actions::Cut.boxed_clone(), cx),
            "Copy" => window.dispatch_action(actions::Copy.boxed_clone(), cx),
            "Paste" => window.dispatch_action(actions::Paste.boxed_clone(), cx),
            "Select All" => window.dispatch_action(actions::SelectAll.boxed_clone(), cx),
            "Go to Definition" => {
                window.dispatch_action(actions::GoToDefinition.boxed_clone(), cx)
            }
            "Find References" => {
                window.dispatch_action(actions::FindReferences.boxed_clone(), cx)
            }
            "Rename Symbol" => window.dispatch_action(actions::RenameSymbol.boxed_clone(), cx),
            "Format SQL" => window.dispatch_action(actions::FormatSQL.boxed_clone(), cx),
            _ => {}
        }
        cx.notify();
    }
}

impl EntityInputHandler for TextEditor {
    fn text_for_range(
        &mut self,
        range_utf16: std::ops::Range<usize>,
        adjusted_range: &mut Option<std::ops::Range<usize>>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<String> {
        let byte_range = self.byte_range_from_utf16(range_utf16);
        let text = self.buffer.text();
        let clamped = byte_range.start.min(text.len())..byte_range.end.min(text.len());
        let slice = text[clamped.clone()].to_string();
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
        let (start_byte, end_byte) = if self.selection.has_selection() {
            let range = self.selection.range();
            let start = self.buffer.position_to_offset(range.start).unwrap_or(0);
            let end = self.buffer.position_to_offset(range.end).unwrap_or(0);
            (start.min(end), start.max(end))
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
        self.ime_marked_range
            .as_ref()
            .map(|range| self.utf16_range_from_bytes(range.clone()))
    }

    fn unmark_text(&mut self, _window: &mut Window, _cx: &mut Context<Self>) {
        self.ime_marked_range = None;
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
            && self.ime_marked_range.is_none()
            && new_text.chars().count() == 1
        {
            let Some(ch) = new_text.chars().next() else { return; };
            let closer = Self::bracket_closer(ch);

            // feat-028: if a selection is active and user types an opener, surround it.
            if let Some(c) = closer {
                if self.selection.has_selection() {
                    if self.auto_surround_selection(ch, c, window, cx) {
                        return;
                    }
                }
            }

            // feat-029: if cursor is immediately before the same closing bracket, skip over it.
            let is_closer = matches!(ch, ')' | ']' | '}' | '\'' | '"');
            if is_closer && !self.selection.has_selection() {
                if self.skip_over_closing_bracket(ch, cx) {
                    return;
                }
            }

            // feat-027: auto-close the bracket pair.
            if let Some(c) = closer {
                if !self.selection.has_selection() {
                    if self.auto_close_bracket(ch, c, window, cx) {
                        return;
                    }
                }
            }
        }

        // Resolve the byte range to replace: explicit IME range > marked range > selection > cursor.
        let byte_range = if let Some(utf16) = range_utf16 {
            self.byte_range_from_utf16(utf16)
        } else if let Some(marked) = self.ime_marked_range.clone() {
            marked
        } else if self.selection.has_selection() {
            let range = self.selection.range();
            let start = self.buffer.position_to_offset(range.start).unwrap_or(0);
            let end = self.buffer.position_to_offset(range.end).unwrap_or(0);
            start.min(end)..start.max(end)
        } else {
            let offset = self.cursor_byte_offset();
            offset..offset
        };

        let buf_len = self.buffer.len();
        let clamped = byte_range.start.min(buf_len)..byte_range.end.min(buf_len);

        // Delete the replaced range first (if non-empty), capturing old text for undo.
        let deleted_text = if !clamped.is_empty() {
            if self.buffer.delete(clamped.clone()).is_ok() {
                // Grab what was deleted so the undo entry can restore it.
                let old = self
                    .buffer
                    .changes()
                    .last()
                    .map(|c| c.old_text.clone())
                    .unwrap_or_default();
                if let Ok(pos) = self.buffer.offset_to_position(clamped.start) {
                    self.cursor.set_position(pos);
                    self.selection.clear();
                }
                old
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        // Insert the new text at the (now-current) cursor.
        if !new_text.is_empty() {
            self.insert_at_cursor(new_text, window, cx);
        }

        // Record the entire operation (delete + insert) as one atomic undo entry.
        // Previously, typed characters were never pushed to the undo stack at all;
        // selection-replaces only tracked the deletion, not the insertion.
        if !new_text.is_empty() || !deleted_text.is_empty() {
            self.push_undo(buffer::Change::replace(
                clamped.start,
                deleted_text,
                new_text.to_string(),
            ));
        }

        self.selection.set_position(self.cursor.position());
        self.ime_marked_range = None;

        if Self::should_auto_trigger_completions(new_text, &self.buffer, self.cursor_byte_offset())
        {
            self.trigger_completions(window, cx);
        }

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
        // Resolve the byte range to replace (same logic as replace_text_in_range).
        let byte_range = if let Some(utf16) = range_utf16 {
            self.byte_range_from_utf16(utf16)
        } else if let Some(marked) = self.ime_marked_range.clone() {
            marked
        } else if self.selection.has_selection() {
            let range = self.selection.range();
            let start = self.buffer.position_to_offset(range.start).unwrap_or(0);
            let end = self.buffer.position_to_offset(range.end).unwrap_or(0);
            start.min(end)..start.max(end)
        } else {
            let offset = self.cursor_byte_offset();
            offset..offset
        };

        let buf_len = self.buffer.len();
        let clamped = byte_range.start.min(buf_len)..byte_range.end.min(buf_len);
        let insert_at = clamped.start;

        if !clamped.is_empty() {
            if self.buffer.delete(clamped).is_ok() {
                if let Ok(pos) = self.buffer.offset_to_position(insert_at) {
                    self.cursor.set_position(pos);
                    self.selection.clear();
                }
            }
        }

        if new_text.is_empty() {
            self.ime_marked_range = None;
        } else {
            if self.buffer.insert(insert_at, new_text).is_ok() {
                let marked_end = insert_at + new_text.len();
                self.ime_marked_range = Some(insert_at..marked_end);

                // Position cursor per OS hint, or at end of composition string.
                let cursor_offset = new_selected_range_utf16
                    .as_ref()
                    .map(|r| {
                        let byte_r = self.byte_range_from_utf16(r.end..r.end);
                        (insert_at + byte_r.start).min(marked_end)
                    })
                    .unwrap_or(marked_end);

                if let Ok(pos) = self.buffer.offset_to_position(cursor_offset) {
                    self.cursor.set_position(pos);
                    self.selection.clear();
                }
                self.update_syntax_highlights();
                self.scroll_to_cursor();
            }
        }

        cx.notify();
    }

    /// Returns the pixel bounds of a UTF-16 range for IME candidate window positioning.
    fn bounds_for_range(
        &mut self,
        range_utf16: std::ops::Range<usize>,
        bounds: Bounds<Pixels>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<Bounds<Pixels>> {
        // We use the cursor position as a proxy — precise per-character layout
        // data is not cached between prepaint and this callback.
        let _ = range_utf16;
        Some(bounds)
    }

    /// Returns the UTF-16 character index closest to a pixel point (for touch / click).
    fn character_index_for_point(
        &mut self,
        point: gpui::Point<Pixels>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> Option<usize> {
        // Not used for IME positioning in our editor; return None.
        let _ = point;
        None
    }
}

/// Diagnostic information for syntax errors and LSP diagnostics
#[derive(Clone, Debug)]
pub struct Diagnostic {
    pub line: usize,
    pub column: usize,
    pub end_line: Option<usize>,
    pub end_column: Option<usize>,
    pub message: String,
    pub severity: DiagnosticLevel,
    pub source: Option<String>,
}

/// Diagnostic severity level
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiagnosticLevel {
    Error,
    Warning,
    Info,
    Hint,
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
    use super::TextEditor;

    // ============================================================================
    // Subword movement (feat-003)
    // ============================================================================

    #[test]
    fn test_next_subword_end_lowercase_run() {
        // Plain lowercase: entire word is one subword
        assert_eq!(TextEditor::next_subword_end_in_text("hello world", 0), 5);
    }

    #[test]
    fn test_next_subword_end_camel_case() {
        // fooBar → "foo" is the first subword
        assert_eq!(TextEditor::next_subword_end_in_text("fooBar", 0), 3);
        // From "Bar" onward: "Bar" is a Title-case subword
        assert_eq!(TextEditor::next_subword_end_in_text("fooBar", 3), 6);
    }

    #[test]
    fn test_next_subword_end_all_caps() {
        // FOOBar → "FOO" (caps stop one before the trailing lowercase)
        assert_eq!(TextEditor::next_subword_end_in_text("FOOBar", 0), 3);
    }

    #[test]
    fn test_next_subword_end_title_case() {
        // FooBar → "Foo"
        assert_eq!(TextEditor::next_subword_end_in_text("FooBar", 0), 3);
    }

    #[test]
    fn test_next_subword_end_underscore_run() {
        // snake_case: "snake" then "_" then "case"
        assert_eq!(TextEditor::next_subword_end_in_text("snake_case", 0), 5);
        assert_eq!(TextEditor::next_subword_end_in_text("snake_case", 5), 6);
        assert_eq!(TextEditor::next_subword_end_in_text("snake_case", 6), 10);
    }

    #[test]
    fn test_next_subword_end_at_end() {
        // Already at end of string
        let s = "foo";
        assert_eq!(TextEditor::next_subword_end_in_text(s, s.len()), s.len());
    }

    #[test]
    fn test_prev_subword_start_lowercase() {
        // cursor at end of "hello" → start = 0
        assert_eq!(TextEditor::prev_subword_start_in_text("hello", 5), 0);
    }

    #[test]
    fn test_prev_subword_start_camel_case() {
        // fooBar, cursor after Bar (6) → "Bar" subword starts at 3
        assert_eq!(TextEditor::prev_subword_start_in_text("fooBar", 6), 3);
        // cursor after foo (3) → "foo" subword starts at 0
        assert_eq!(TextEditor::prev_subword_start_in_text("fooBar", 3), 0);
    }

    #[test]
    fn test_prev_subword_start_title_case() {
        // "FooBar", cursor at 6 → "Bar" starts at 3 (lowercase run + preceding upper)
        assert_eq!(TextEditor::prev_subword_start_in_text("FooBar", 6), 3);
    }

    #[test]
    fn test_prev_subword_start_underscore() {
        // "snake_case", cursor at 10 → "case" starts at 6
        assert_eq!(TextEditor::prev_subword_start_in_text("snake_case", 10), 6);
        // cursor at 6 → "_" subword starts at 5
        assert_eq!(TextEditor::prev_subword_start_in_text("snake_case", 6), 5);
        // cursor at 5 → "snake" starts at 0
        assert_eq!(TextEditor::prev_subword_start_in_text("snake_case", 5), 0);
    }

    #[test]
    fn test_prev_subword_start_at_zero() {
        assert_eq!(TextEditor::prev_subword_start_in_text("foo", 0), 0);
    }
}
