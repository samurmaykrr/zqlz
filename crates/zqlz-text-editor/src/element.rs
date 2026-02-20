//! EditorElement - Renders the text editor in GPUI
//!
//! This module implements the rendering logic for the TextEditor, including:
//! - Text line rendering with syntax highlighting
//! - Line number gutter (Phase 6)
//! - Selection highlighting (Phase 3)
//! - Cursor rendering
//! - Viewport calculation (only render visible lines)
//! - Completion menu and hover tooltips (Phase 8)
//! - Find & Replace panel (Phase 9)

use gpui::*;
use std::ops::Range;
use zqlz_ui::widgets::ActiveTheme;

use crate::{buffer::Position, syntax::HighlightKind, TextEditor};

/// The width of the cursor in pixels
const CURSOR_WIDTH: Pixels = px(1.5);

/// Padding on each side inside the gutter (between gutter edge and line number text)
const GUTTER_PADDING: Pixels = px(8.0);

/// Width of the separator line between gutter and text content
const GUTTER_SEPARATOR_WIDTH: Pixels = px(1.0);

/// Dedicated horizontal zone reserved for fold chevrons, sitting between the
/// right edge of the line-number text and the separator. This prevents the
/// triangle from overlapping the digits regardless of how many digits are shown.
const FOLD_CHEVRON_ZONE: Pixels = px(16.0);

/// Maximum number of completion items to show in the menu at once
pub(crate) const MAX_COMPLETION_ITEMS: usize = 10;

/// Completion item data for rendering (simplified)
struct CompletionItemData {
    /// The label to display
    label: String,
    /// Short kind badge text (e.g. "fn", "kw", "tbl") – used only for the
    /// left-edge accent dot color; no longer rendered as text.
    kind_badge: Option<String>,
    /// Shortened detail string (parenthetical dialect info stripped)
    detail: Option<String>,
}

/// Inline (ghost-text) suggestion render data
struct InlineSuggestionRenderData {
    /// The ghost text to paint after the cursor
    text: String,
    /// Top-left corner at which to start painting the ghost text
    origin: Point<Pixels>,
    /// Line height (for vertical positioning)
    line_height: Pixels,
}

/// Completion menu to render
struct CompletionMenuRenderData {
    /// The completion items
    items: Vec<CompletionItemData>,
    /// Cursor position for menu placement
    cursor_bounds: Bounds<Pixels>,
    /// Currently selected index (absolute, not relative to scroll_offset)
    selected_index: usize,
    /// First visible item index within the scrollable window
    scroll_offset: usize,
    /// Line height for item sizing
    line_height: Pixels,
}

/// Element that renders a TextEditor
pub struct EditorElement {
    editor: Entity<TextEditor>,
}

/// Per-line fold chevron data produced during prepaint.
struct FoldChevronData {
    /// Buffer start line this fold region corresponds to.
    start_line: usize,
    /// True when this region is currently collapsed.
    is_folded: bool,
    /// Hit-test rect in window coordinates (for caching to the editor).
    rect: Bounds<Pixels>,
}

impl EditorElement {
    /// Resolve the display color for a syntax highlight kind from the active theme.
    ///
    /// Each kind maps to a named slot in `SyntaxColors` (via `HighlightTheme`).
    /// When a theme doesn't define a slot, we fall back to a semantic color from
    /// the main theme palette (e.g. `primary` for keywords, `danger` for errors).
    fn highlight_color(kind: HighlightKind, cx: &App) -> Hsla {
        let ht = &cx.theme().highlight_theme;
        match kind {
            HighlightKind::Keyword => ht
                .keyword
                .and_then(|s| s.color)
                .unwrap_or(cx.theme().colors.primary),
            HighlightKind::String => ht
                .string
                .and_then(|s| s.color)
                .unwrap_or_else(|| rgb(0xce9178).into()),
            HighlightKind::Comment => ht
                .comment
                .and_then(|s| s.color)
                .unwrap_or(cx.theme().colors.muted_foreground),
            HighlightKind::Number => ht
                .number
                .and_then(|s| s.color)
                .unwrap_or_else(|| rgb(0xb5cea8).into()),
            HighlightKind::Identifier => ht
                .variable
                .and_then(|s| s.color)
                .unwrap_or(cx.theme().colors.foreground),
            HighlightKind::Operator => ht
                .operator
                .and_then(|s| s.color)
                .unwrap_or(cx.theme().colors.foreground),
            HighlightKind::Function => ht
                .function
                .and_then(|s| s.color)
                .unwrap_or(cx.theme().colors.primary),
            HighlightKind::Punctuation => ht
                .punctuation
                .and_then(|s| s.color)
                .unwrap_or(cx.theme().colors.foreground),
            HighlightKind::Boolean => ht
                .boolean
                .and_then(|s| s.color)
                .or_else(|| ht.keyword.and_then(|s| s.color))
                .unwrap_or(cx.theme().colors.primary),
            HighlightKind::Null => ht
                .keyword
                .and_then(|s| s.color)
                .unwrap_or(cx.theme().colors.primary),
            HighlightKind::Error => cx.theme().colors.danger,
            HighlightKind::Default => cx.theme().colors.foreground,
        }
    }

    /// Calculate the gutter width based on the total number of lines.
    ///
    /// The gutter must be wide enough to display the widest line number, plus
    /// padding on both sides and a separator. We compute by shaping the widest
    /// possible number string (all nines of the same digit count).
    fn calculate_gutter_width(
        total_lines: usize,
        font_size: Pixels,
        window: &mut Window,
    ) -> Pixels {
        // Determine how many digits we need for the highest line number (1-indexed)
        let max_line_number = total_lines.max(1);
        let digit_count = max_line_number.ilog10() as usize + 1;

        // Shape a string of the same digit count (using "9" for widest digit in most fonts)
        let sample: String = "9".repeat(digit_count);
        let text_run = TextRun {
            len: sample.len(),
            font: Font::default(),
            color: gpui::white(),
            background_color: None,
            underline: None,
            strikethrough: None,
        };
        let shaped = window
            .text_system()
            .shape_line(sample.into(), font_size, &[text_run], None);

        shaped.width + GUTTER_PADDING * 2.0 + FOLD_CHEVRON_ZONE + GUTTER_SEPARATOR_WIDTH
    }
}

impl EditorElement {
    pub fn new(editor: Entity<TextEditor>) -> Self {
        Self { editor }
    }

    /// Calculate which lines are visible in the viewport
    fn calculate_visible_range(
        &self,
        total_lines: usize,
        line_height: Pixels,
        viewport_height: Pixels,
        scroll_offset: f32,
    ) -> Range<usize> {
        // Calculate how many lines fit in the viewport
        let visible_lines = (viewport_height / line_height).ceil() as usize;

        // Calculate start line from scroll offset
        let start_line = scroll_offset.floor() as usize;
        let start_line = start_line.min(total_lines.saturating_sub(1));

        // Calculate end line
        let end_line = (start_line + visible_lines).min(total_lines);

        start_line..end_line
    }

    /// Get the cursor position in pixels, offset into the text content area
    /// (already accounts for gutter_width so the caller adds bounds.origin).
    ///
    /// `display_slot` is the cursor line's index in the display-line list (not the
    /// raw buffer line), so folded lines are automatically handled by the caller
    /// resolving the slot via `buf_to_display` before calling here.
    fn cursor_pixel_position(
        &self,
        cursor_pos: Position,
        line_height: Pixels,
        char_width: Pixels,
        scroll_offset: f32,
        gutter_width: Pixels,
        display_slot: usize,
    ) -> Point<Pixels> {
        point(
            gutter_width + char_width * (cursor_pos.column as f32),
            line_height * (display_slot as f32 - scroll_offset),
        )
    }
}

impl IntoElement for EditorElement {
    type Element = Self;

    fn into_element(self) -> Self::Element {
        self
    }
}

/// PrepaintState holds pre-calculated layout information
pub struct PrepaintState {
    bounds: Bounds<Pixels>,
    line_height: Pixels,
    shaped_lines: Vec<ShapedLine>,
    cursor_bounds: Option<Bounds<Pixels>>,
    /// Selection rectangles to paint (if any)
    selection_rects: Vec<Bounds<Pixels>>,
    /// Pixel bounds for each extra cursor (multi-cursor mode, feat-017/018/021/022).
    extra_cursor_bounds: Vec<Bounds<Pixels>>,
    /// Selection rectangles for extra cursors.
    extra_selection_rects: Vec<Bounds<Pixels>>,
    /// Bracket highlight rectangles per nesting level (feat-026).
    ///
    /// Each entry is a pair of rects `(open_rect, close_rect)` for one enclosing
    /// bracket pair.  The outer index corresponds to nesting depth (outermost first).
    bracket_highlight_rects: Vec<(Bounds<Pixels>, Bounds<Pixels>)>,
    /// Completion menu data (if menu is open)
    completion_menu: Option<CompletionMenuRenderData>,
    /// Error diagnostic ranges for rendering squiggles
    diagnostics: Vec<(crate::syntax::Highlight, Bounds<Pixels>)>,
    /// Hover tooltip data (if hovering over a word)
    hover_tooltip: Option<HoverTooltipData>,
    /// Find/replace panel data (if the panel is open)
    find_panel: Option<FindPanelData>,
    // ── Gutter (Phase 6) ──────────────────────────────────────────────────────
    /// Pixel width of the line-number gutter (includes padding and separator)
    gutter_width: Pixels,
    /// Pre-shaped line number labels for each visible line
    gutter_lines: Vec<GutterLine>,
    /// Ghost-text inline suggestion to paint after the cursor (if any)
    inline_suggestion: Option<InlineSuggestionRenderData>,
    /// Go-to-line overlay dialog (if open)
    goto_line_panel: Option<GoToLinePanelData>,
    /// Indent guide X-coordinates (one per indent level) for visible lines
    indent_guides: Vec<IndentGuideData>,
    /// Pre-calculated scrollbar geometry (None when content fits in viewport)
    scrollbar: Option<ScrollbarData>,
    /// Whether the editor is in soft-wrap mode (affects line painting)
    #[allow(dead_code)]
    soft_wrap: bool,
    /// Pixel rectangles for each visible reference highlight (feat-047)
    reference_highlights: Vec<ReferenceHighlightData>,
    /// Inline rename overlay data (feat-048), present while rename dialog is open
    rename_overlay: Option<RenameOverlayData>,
    /// Right-click context menu render data (feat-045)
    context_menu: Option<ContextMenuRenderData>,
    /// Fractional-scroll sub-line Y offset (in pixels).
    ///
    /// When `scroll_offset` has a fractional part (e.g. `2.7`), the first
    /// visible line is partially scrolled off the top by this many pixels.
    /// Subtracting this from every slot-indexed Y coordinate keeps overlays
    /// (cursor, selections, hover anchor, etc.) aligned with the rendered text.
    sub_line_offset: Pixels,
    /// Fold chevron hit-rects and state for each visible foldable line.
    fold_chevrons: Vec<FoldChevronData>,
}

/// Hover tooltip data for rendering
struct HoverTooltipData {
    /// The documentation text to display
    documentation: String,
    /// Position to anchor the tooltip (cursor position or word position)
    anchor_bounds: Bounds<Pixels>,
}

/// Data the renderer needs to paint the find/replace panel and match highlights.
struct FindPanelData {
    /// The search query text to display in the input field
    query: String,
    /// The replacement text (empty string when replace panel is hidden)
    replace_query: String,
    /// Whether the replace row is visible
    show_replace: bool,
    /// Whether the search field has focus (vs. replace field)
    search_field_focused: bool,
    /// Pixel rectangles for every match in the visible viewport, paired with a flag
    /// indicating whether that match is the currently-selected (primary) match.
    match_rects: Vec<(Bounds<Pixels>, bool)>,
    /// Match count / current index for the status label
    total_matches: usize,
    /// 1-based index of the current match for the status label (0 = no match)
    current_match_display: usize,
    /// Line height, used for sizing the panel
    line_height: Pixels,
}

/// A pre-shaped line number label for one visible line in the gutter.
struct GutterLine {
    /// The shaped text ready to paint
    shaped: ShapedLine,
    /// Whether this is the cursor (active) line, drawn with a brighter color
    is_active: bool,
}

/// Data needed to paint the go-to-line overlay dialog.
struct GoToLinePanelData {
    /// The digit string the user has typed so far
    query: String,
    /// False when the typed number is out of the valid 1..=total_lines range, triggering a red border
    is_valid: bool,
    /// Total line count of the buffer (for the placeholder hint)
    total_lines: usize,
    /// Editor line height (for sizing the panel)
    line_height: Pixels,
}

/// Scrollbar geometry, pre-calculated during prepaint.
struct ScrollbarData {
    /// The full track rectangle
    track: Bounds<Pixels>,
    /// The draggable thumb rectangle
    thumb: Bounds<Pixels>,
}

/// A single vertical indent guide line to paint over the text area.
struct IndentGuideData {
    /// X pixel coordinate (absolute, includes gutter offset)
    x: Pixels,
    /// Y start of the guide (top of first indented line)
    y_start: Pixels,
    /// Y end of the guide (bottom of last indented line)
    y_end: Pixels,
    /// True when the cursor column sits within this guide's indent level, making it brighter
    is_active: bool,
}

/// Data for rendering a single reference-highlight rectangle (feat-047).
struct ReferenceHighlightData {
    /// The pixel rectangle to tint
    rect: Bounds<Pixels>,
}

/// Data for the inline rename-symbol overlay (feat-048).
struct RenameOverlayData {
    /// The text currently typed as the new name
    new_name: String,
    /// Pixel bounds of the word being renamed (anchor for the overlay box)
    word_rect: Bounds<Pixels>,
    /// Line height (for sizing the box)
    line_height: Pixels,
}

/// Data for rendering the right-click context menu (feat-045).
struct ContextMenuRenderData {
    /// Ordered items to display (separators have `is_separator = true`)
    items: Vec<(String, bool, bool)>, // (label, is_separator, is_disabled)
    /// Pixel origin (top-left of the menu box)
    origin: Point<Pixels>,
    /// Index of the highlighted item, if any
    highlighted: Option<usize>,
    /// Line height for menu item sizing
    line_height: Pixels,
}

impl Element for EditorElement {
    type RequestLayoutState = ();
    type PrepaintState = PrepaintState;

    fn id(&self) -> Option<ElementId> {
        Some(ElementId::Name("text-editor".into()))
    }

    fn source_location(&self) -> Option<&'static std::panic::Location<'static>> {
        None
    }

    fn request_layout(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        window: &mut Window,
        cx: &mut App,
    ) -> (LayoutId, Self::RequestLayoutState) {
        let mut style = Style::default();
        style.size.width = relative(1.).into();
        style.size.height = relative(1.).into();

        (window.request_layout(style, [], cx), ())
    }

    fn prepaint(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        window: &mut Window,
        cx: &mut App,
    ) -> Self::PrepaintState {
        let line_height = window.line_height();
        let text_style = window.text_style();
        let font = text_style.font();
        let font_size = text_style.font_size.to_pixels(window.rem_size());

        // Calculate viewport lines for auto-scroll
        let viewport_lines = (bounds.size.height / line_height).ceil() as usize;

        let (
            buffer,
            cursor_pos,
            scroll_offset,
            has_selection,
            sel_start,
            sel_end,
            total_lines,
            completion_menu_cursor_pos,
            syntax_highlights,
            diagnostics,
            hover_state,
            find_info,
            inline_suggestion_snapshot,
            extra_cursors_snapshot,
            bracket_pairs_snapshot,
            goto_line_info,
            soft_wrap,
            reference_ranges_snapshot,
            rename_snapshot,
            context_menu_snapshot,
        ) = {
            let editor = self.editor.read(cx);
            let buffer = editor.buffer();
            let cursor_pos = editor.get_cursor_position(cx);
            let scroll_offset = editor.scroll_offset();
            let total_lines = buffer.line_count();
            let syntax_highlights = editor.get_syntax_highlights().to_vec();
            let diagnostics = editor.get_diagnostics().to_vec();
            let hover_state = editor.hover_state(cx);
            let bracket_pairs_snapshot = editor.bracket_highlight_pairs();

            let (has_selection, sel_start, sel_end) = if editor.has_selection() {
                let selection = editor.selection();
                let sel_range = selection.range();
                (true, sel_range.start, sel_range.end)
            } else {
                (false, Position::new(0, 0), Position::new(0, 0))
            };

            // Snapshot extra cursor positions and their selection ranges for multi-cursor rendering.
            // We only need the position and optional selection range — Cursor/Selection are Clone.
            let extra_cursors_snapshot: Vec<(
                crate::buffer::Position,
                Option<(crate::buffer::Position, crate::buffer::Position)>,
            )> = editor
                .extra_cursor_selections()
                .iter()
                .map(|(cursor, selection)| {
                    let sel_range = if selection.has_selection() {
                        Some((selection.range().start, selection.range().end))
                    } else {
                        None
                    };
                    (cursor.position(), sel_range)
                })
                .collect();

            // Get completion menu data (store cursor position for now, calculate bounds later)
            let completion_menu_cursor_pos = if editor.is_completion_menu_open(cx) {
                Some(cursor_pos)
            } else {
                None
            };

            // Snapshot the inline suggestion (text + anchor offset) for ghost-text painting
            let inline_suggestion_snapshot = editor
                .inline_suggestion()
                .map(|(text, offset)| (text.clone(), *offset));

            // Snapshot the find state so we can build render data below
            let find_info = editor.find_state.as_ref().map(|fs| {
                (
                    fs.query.clone(),
                    fs.replace_query.clone(),
                    fs.show_replace,
                    fs.search_field_focused,
                    fs.matches.clone(),
                    fs.current_match,
                )
            });

            // Snapshot go-to-line dialog state so we can paint the overlay
            let goto_line_info = editor
                .goto_line_state
                .as_ref()
                .map(|s| (s.query.clone(), s.is_valid, buffer.line_count()));

            let soft_wrap = editor.soft_wrap;

            // Snapshot reference highlight ranges (feat-047)
            let reference_ranges_snapshot = editor.reference_ranges.clone();

            // Snapshot rename overlay state (feat-048)
            let rename_snapshot = editor
                .rename_state
                .as_ref()
                .map(|s| (s.new_name.clone(), s.word_start, s.word_end));

            // Snapshot context menu state (feat-045)
            let context_menu_snapshot = editor.context_menu.as_ref().map(|m| {
                let items: Vec<(String, bool, bool)> = m
                    .items
                    .iter()
                    .map(|item| (item.label.clone(), item.is_separator, item.disabled))
                    .collect();
                (items, m.origin_x, m.origin_y, m.highlighted)
            });

            (
                buffer.clone(),
                cursor_pos,
                scroll_offset,
                has_selection,
                sel_start,
                sel_end,
                total_lines,
                completion_menu_cursor_pos,
                syntax_highlights,
                diagnostics,
                hover_state,
                find_info,
                inline_suggestion_snapshot,
                extra_cursors_snapshot,
                bracket_pairs_snapshot,
                goto_line_info,
                soft_wrap,
                reference_ranges_snapshot,
                rename_snapshot,
                context_menu_snapshot,
            )
        };

        // Update viewport lines for auto-scroll (after read lock is released)
        self.editor.update(cx, |editor, _cx| {
            editor.update_viewport_lines(viewport_lines);
        });

        // Compute display-line mapping. Lines hidden inside collapsed fold regions
        // are omitted so that the renderer sees a contiguous list of visible rows.
        //   display_lines[slot] = buffer line index
        //   buf_to_display[buffer_line] = display slot (absent when hidden)
        let (display_lines, buf_to_display) = {
            let editor = self.editor.read(cx);
            let dl = editor.visible_buffer_lines();
            let btd: std::collections::HashMap<usize, usize> = dl
                .iter()
                .enumerate()
                .map(|(slot, &buf)| (buf, slot))
                .collect();
            (dl, btd)
        };
        let display_line_count = display_lines.len();

        // Calculate visible range (in display-slot space, not buffer-line space)
        let visible_range = self.calculate_visible_range(
            display_line_count,
            line_height,
            bounds.size.height,
            scroll_offset,
        );

        // Fractional part of scroll_offset expressed as pixels. When
        // scroll_offset = 2.7 the top of the viewport is 0.7 lines into line
        // 2, so every slot-indexed Y must be shifted up by this amount.
        let sub_line_offset = (scroll_offset - visible_range.start as f32) * line_height;

        // Closure (not a nested fn) so it can capture `cx` for theme-aware colors.
        let get_line_text_runs = |line_text: &str,
                                  line_start_offset: usize,
                                  syntax_highlights: &[crate::syntax::Highlight],
                                  font: &Font,
                                  default_color: Hsla|
         -> Vec<TextRun> {
            if syntax_highlights.is_empty() || line_text.is_empty() {
                // No highlights - return single default run
                return vec![TextRun {
                    len: line_text.len(),
                    font: font.clone(),
                    color: default_color,
                    background_color: None,
                    underline: None,
                    strikethrough: None,
                }];
            }

            let mut runs = Vec::new();
            let line_end_offset = line_start_offset + line_text.len();

            // Find highlights that overlap with this line
            for highlight in syntax_highlights {
                let hl_start = highlight.start;
                let hl_end = highlight.end;

                // Skip highlights that don't overlap with this line
                if hl_end <= line_start_offset || hl_start >= line_end_offset {
                    continue;
                }

                // Calculate the overlap range within this line
                let run_start = hl_start.saturating_sub(line_start_offset);
                let run_end = (hl_end - line_start_offset).min(line_text.len());

                if run_start < run_end {
                    runs.push((run_start, run_end, highlight.kind));
                }
            }

            if runs.is_empty() {
                return vec![TextRun {
                    len: line_text.len(),
                    font: font.clone(),
                    color: default_color,
                    background_color: None,
                    underline: None,
                    strikethrough: None,
                }];
            }

            // Sort by start position
            runs.sort_by_key(|(start, _, _)| *start);

            // Build runs, filling gaps with default color
            let mut result = Vec::new();
            let mut current_pos = 0;

            for (start, end, kind) in runs {
                // Add gap if there's space before this run
                if start > current_pos {
                    result.push(TextRun {
                        len: start - current_pos,
                        font: font.clone(),
                        color: default_color,
                        background_color: None,
                        underline: None,
                        strikethrough: None,
                    });
                }

                // Add the highlight run
                result.push(TextRun {
                    len: end - start,
                    font: font.clone(),
                    color: EditorElement::highlight_color(kind, cx),
                    background_color: None,
                    underline: None,
                    strikethrough: None,
                });

                current_pos = end;
            }

            // Add remaining gap at end of line
            if current_pos < line_text.len() {
                result.push(TextRun {
                    len: line_text.len() - current_pos,
                    font: font.clone(),
                    color: default_color,
                    background_color: None,
                    underline: None,
                    strikethrough: None,
                });
            }

            result
        };

        let default_text_color = cx.theme().colors.foreground;

        // Pre-compute the buffer byte offset at which each line starts.
        // This ensures syntax-highlight ranges (which are buffer-absolute) align
        // correctly with visible lines regardless of how far the user has scrolled.
        let line_byte_offsets: Vec<usize> = {
            let mut offsets = Vec::with_capacity(total_lines + 1);
            let mut off = 0usize;
            for i in 0..total_lines {
                offsets.push(off);
                let line_text = buffer.line(i).unwrap_or_default();
                let line_len = line_text
                    .trim_end_matches('\n')
                    .trim_end_matches('\r')
                    .len();
                off += line_len + 1;
            }
            offsets
        };

        // Shape visible lines with syntax highlighting.
        // `visible_range` is in display-slot space; `display_lines[slot]` gives the
        // actual buffer line for each slot, skipping any lines hidden by folds.
        let mut shaped_lines = Vec::new();
        for display_slot in visible_range.clone() {
            let buffer_line = display_lines[display_slot];
            // Get line text and strip trailing newline (ropey includes it)
            let line_text = buffer
                .line(buffer_line)
                .unwrap_or_default()
                .trim_end_matches('\n')
                .trim_end_matches('\r')
                .to_string();

            let line_start_offset = line_byte_offsets.get(buffer_line).copied().unwrap_or(0);

            // Get text runs with syntax highlighting
            let text_runs = get_line_text_runs(
                &line_text,
                line_start_offset,
                &syntax_highlights,
                &font,
                default_text_color,
            );

            let shaped_line =
                window
                    .text_system()
                    .shape_line(line_text.into(), font_size, &text_runs, None);

            shaped_lines.push(shaped_line);
        }

        // Calculate character width (use monospace assumption)
        // Shape a single character to get its width
        let single_char = window.text_system().shape_line(
            "M".into(),
            font_size,
            &[TextRun {
                len: 1,
                font: font.clone(),
                color: gpui::white(),
                background_color: None,
                underline: None,
                strikethrough: None,
            }],
            None,
        );
        let char_width = single_char.width;

        // ── Gutter (Phase 6) ──────────────────────────────────────────────────
        // Compute gutter width from the number of digits needed for the last line.
        let gutter_width = Self::calculate_gutter_width(total_lines, font_size, window);

        // Push the gutter width and the element's bounds origin back to the
        // editor so that mouse handlers — which receive positions in window
        // coordinates — can correctly subtract both offsets when converting
        // a pixel point to a buffer (line, column) position.
        self.editor.update(cx, |editor, _cx| {
            editor.update_cached_gutter_width(f32::from(gutter_width));
            editor.update_cached_char_width(char_width);
            editor.update_cached_bounds_origin(bounds.origin);
        });

        // Shape a line-number label for every visible line.
        let mut gutter_lines: Vec<GutterLine> = Vec::with_capacity(visible_range.len());
        for display_slot in visible_range.clone() {
            let buffer_line = display_lines[display_slot];
            let line_number = buffer_line + 1; // 1-based display
            let label = line_number.to_string();
            let is_active = buffer_line == cursor_pos.line;
            let color: Hsla = if is_active {
                cx.theme().colors.foreground
            } else {
                cx.theme().colors.muted_foreground
            };
            let text_run = TextRun {
                len: label.len(),
                font: font.clone(),
                color,
                background_color: None,
                underline: None,
                strikethrough: None,
            };
            let shaped =
                window
                    .text_system()
                    .shape_line(label.into(), font_size, &[text_run], None);
            gutter_lines.push(GutterLine { shaped, is_active });
        }
        // ─────────────────────────────────────────────────────────────────────

        // ── Fold chevrons ─────────────────────────────────────────────────────
        // Snapshot fold state under a read lock so we don't hold the borrow
        // while computing geometry below.
        let (fold_regions_snap, folded_lines_snap) = {
            let editor = self.editor.read(cx);
            (editor.fold_regions().to_vec(), editor.folded_lines.clone())
        };

        let chevron_size = line_height * 0.55;
        // Center the chevron within the dedicated FOLD_CHEVRON_ZONE that sits between
        // the line-number text and the separator — no overlap with digits possible.
        let chevron_zone_origin =
            bounds.origin.x + gutter_width - GUTTER_SEPARATOR_WIDTH - FOLD_CHEVRON_ZONE;
        let chevron_x = chevron_zone_origin + (FOLD_CHEVRON_ZONE - chevron_size) / 2.0;

        let mut fold_chevrons: Vec<FoldChevronData> = Vec::new();
        for (slot_offset, display_slot) in visible_range.clone().enumerate() {
            let buffer_line = display_lines[display_slot];
            if let Some(region) = fold_regions_snap
                .iter()
                .find(|r| r.start_line == buffer_line)
            {
                let is_folded = folded_lines_snap.contains(&buffer_line);
                let line_y = bounds.origin.y + line_height * (slot_offset as f32) - sub_line_offset;
                let rect = Bounds::new(
                    point(chevron_x, line_y + (line_height - chevron_size) / 2.0),
                    size(chevron_size, chevron_size),
                );
                fold_chevrons.push(FoldChevronData {
                    start_line: region.start_line,
                    is_folded,
                    rect,
                });
            }
        }
        // ─────────────────────────────────────────────────────────────────────

        // Calculate selection rectangles (offset by gutter_width into text area)
        let mut selection_rects = Vec::new();
        if has_selection {
            for line_idx in sel_start.line..=sel_end.line {
                let Some(&display_slot) = buf_to_display.get(&line_idx) else {
                    continue;
                };
                let line_len = buffer.line(line_idx).map(|l| l.len()).unwrap_or(0);
                let start_col = if line_idx == sel_start.line {
                    sel_start.column
                } else {
                    0
                };
                let end_col = if line_idx == sel_end.line {
                    sel_end.column
                } else {
                    line_len
                };
                if start_col < end_col {
                    selection_rects.push(Bounds::new(
                        point(
                            bounds.origin.x + gutter_width + char_width * (start_col as f32),
                            bounds.origin.y + line_height * (display_slot as f32 - scroll_offset),
                        ),
                        size(char_width * ((end_col - start_col) as f32), line_height),
                    ));
                }
            }
        }

        // Calculate diagnostic (error) bounds for squiggles
        let mut diagnostic_bounds = Vec::new();
        for highlight in &diagnostics {
            if let Ok(start_pos) = buffer.offset_to_position(highlight.start) {
                if let Ok(end_pos) = buffer.offset_to_position(highlight.end) {
                    let start_line = start_pos.line as usize;
                    let end_line = end_pos.line as usize;
                    // Only include when at least the start line is visible (not hidden by a fold).
                    if buf_to_display.contains_key(&start_line)
                        || buf_to_display.contains_key(&end_line)
                    {
                        diagnostic_bounds.push((
                            highlight.clone(),
                            start_line,
                            start_pos.column,
                            end_pos.column,
                        ));
                    }
                }
            }
        }

        // Calculate cursor bounds — only when the cursor's buffer line is visible.
        let cursor_height = line_height * 0.85; // 85% of line height, centered
        let cursor_bounds = buf_to_display.get(&cursor_pos.line).map(|&slot| {
            let cursor_pixel_pos = self.cursor_pixel_position(
                cursor_pos,
                line_height,
                char_width,
                scroll_offset,
                gutter_width,
                slot,
            );
            Bounds::new(
                point(
                    bounds.origin.x + cursor_pixel_pos.x,
                    bounds.origin.y + cursor_pixel_pos.y + (line_height - cursor_height) / 2.0,
                ),
                size(CURSOR_WIDTH, cursor_height),
            )
        });

        // Compute pixel bounds for each extra cursor and its selection (multi-cursor rendering).
        let mut extra_cursor_bounds: Vec<Bounds<Pixels>> = Vec::new();
        let mut extra_selection_rects: Vec<Bounds<Pixels>> = Vec::new();
        for (extra_pos, extra_sel_range) in &extra_cursors_snapshot {
            if let Some(&extra_slot) = buf_to_display.get(&extra_pos.line) {
                let extra_pixel_pos = self.cursor_pixel_position(
                    *extra_pos,
                    line_height,
                    char_width,
                    scroll_offset,
                    gutter_width,
                    extra_slot,
                );
                extra_cursor_bounds.push(Bounds::new(
                    point(
                        bounds.origin.x + extra_pixel_pos.x,
                        bounds.origin.y + extra_pixel_pos.y + (line_height - cursor_height) / 2.0,
                    ),
                    size(CURSOR_WIDTH, cursor_height),
                ));
            }

            if let Some((sel_start_pos, sel_end_pos)) = extra_sel_range {
                for line_idx in sel_start_pos.line..=sel_end_pos.line {
                    let Some(&slot) = buf_to_display.get(&line_idx) else {
                        continue;
                    };
                    let line_len = buffer.line(line_idx).map(|l| l.len()).unwrap_or(0);
                    let start_col = if line_idx == sel_start_pos.line {
                        sel_start_pos.column
                    } else {
                        0
                    };
                    let end_col = if line_idx == sel_end_pos.line {
                        sel_end_pos.column
                    } else {
                        line_len
                    };
                    if start_col < end_col {
                        extra_selection_rects.push(Bounds::new(
                            point(
                                bounds.origin.x + gutter_width + char_width * (start_col as f32),
                                bounds.origin.y + line_height * (slot as f32 - scroll_offset),
                            ),
                            size(
                                char_width * (end_col.saturating_sub(start_col) as f32),
                                line_height,
                            ),
                        ));
                    }
                }
            }
        }

        // Compute bracket highlight rects for all enclosing pairs (feat-026).
        let mut bracket_highlight_rects: Vec<(Bounds<Pixels>, Bounds<Pixels>)> = Vec::new();
        for (open_offset, close_offset) in &bracket_pairs_snapshot {
            if let (Ok(open_pos), Ok(close_pos)) = (
                buffer.offset_to_position(*open_offset),
                buffer.offset_to_position(*close_offset),
            ) {
                // Hidden lines (inside a collapsed fold) return None from this closure.
                let make_rect = |pos: Position| -> Option<Bounds<Pixels>> {
                    let &slot = buf_to_display.get(&pos.line)?;
                    Some(Bounds::new(
                        point(
                            bounds.origin.x + gutter_width + char_width * (pos.column as f32),
                            bounds.origin.y + line_height * (slot as f32 - scroll_offset),
                        ),
                        size(char_width, line_height),
                    ))
                };
                let open_rect = make_rect(open_pos);
                let close_rect = make_rect(close_pos);
                if open_rect.is_some() || close_rect.is_some() {
                    let open_rect = open_rect
                        .unwrap_or_else(|| Bounds::new(bounds.origin, size(px(0.0), px(0.0))));
                    let close_rect = close_rect
                        .unwrap_or_else(|| Bounds::new(bounds.origin, size(px(0.0), px(0.0))));
                    bracket_highlight_rects.push((open_rect, close_rect));
                }
            }
        }

        // Calculate completion menu data if needed
        let completion_menu = if let Some(menu_cursor_pos) = completion_menu_cursor_pos {
            if let Some(menu) = self.editor.read(cx).completion_menu(cx) {
                let menu_slot = buf_to_display
                    .get(&menu_cursor_pos.line)
                    .copied()
                    .unwrap_or(menu_cursor_pos.line);
                let cursor_pixel_pos = self.cursor_pixel_position(
                    menu_cursor_pos,
                    line_height,
                    char_width,
                    scroll_offset,
                    gutter_width,
                    menu_slot,
                );
                let menu_cursor_bounds = Bounds::new(
                    point(
                        bounds.origin.x + cursor_pixel_pos.x,
                        bounds.origin.y + cursor_pixel_pos.y,
                    ),
                    size(char_width, line_height),
                );
                Some(CompletionMenuRenderData {
                    items: menu
                        .items
                        .into_iter()
                        .map(|item| CompletionItemData {
                            kind_badge: item.kind.map(completion_kind_badge),
                            // Strip the parenthetical dialect suffix (e.g. " (MySQL)") from
                            // detail strings like "SQL Keyword (MySQL)" → "Keyword".
                            detail: item.detail.clone().map(|d| {
                                let trimmed = d.trim_start_matches("SQL ").to_string();
                                if let Some(paren) = trimmed.find('(') {
                                    trimmed[..paren].trim_end().to_string()
                                } else {
                                    trimmed
                                }
                            }),
                            label: item.label,
                        })
                        .collect(),
                    cursor_bounds: menu_cursor_bounds,
                    selected_index: menu.selected_index,
                    scroll_offset: menu.scroll_offset,
                    line_height,
                })
            } else {
                None
            }
        } else {
            None
        };

        // Build diagnostic bounds for rendering squiggles (offset into text area by gutter_width)
        let mut diag_bounds = Vec::new();
        for (highlight, line_idx, start_col, end_col) in diagnostic_bounds {
            if let Some(&display_slot) = buf_to_display.get(&line_idx) {
                let rect = Bounds::new(
                    point(
                        bounds.origin.x + gutter_width + char_width * (start_col as f32),
                        bounds.origin.y + line_height * (display_slot as f32 - scroll_offset),
                    ),
                    size(char_width * ((end_col - start_col) as f32), line_height),
                );
                diag_bounds.push((highlight, rect));
            }
        }

        // Calculate hover tooltip bounds if hovering (offset into text area by gutter_width)
        let hover_tooltip = if let Some(ref hover) = hover_state {
            // Get the position of the hovered word
            if let Ok(start_pos) = buffer.offset_to_position(hover.range.start) {
                if let Ok(end_pos) = buffer.offset_to_position(hover.range.end) {
                    // Check if the hovered word is on a visible (non-folded) line
                    if let Some(&display_slot) = buf_to_display.get(&start_pos.line) {
                        let anchor_bounds = Bounds::new(
                            point(
                                bounds.origin.x
                                    + gutter_width
                                    + char_width * (start_pos.column as f32),
                                bounds.origin.y
                                    + line_height * (display_slot as f32 - scroll_offset),
                            ),
                            size(
                                char_width
                                    * (end_pos.column.saturating_sub(start_pos.column) as f32),
                                line_height,
                            ),
                        );
                        Some(HoverTooltipData {
                            documentation: hover.documentation.clone(),
                            anchor_bounds,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Build find panel render data if the panel is open
        let find_panel = find_info.map(
            |(query, replace_query, show_replace, search_field_focused, matches, current_match)| {
                let total_matches = matches.len();
                let current_match_display = if total_matches == 0 {
                    0
                } else {
                    current_match + 1
                };

                // Compute pixel rects for each match that is within the visible viewport,
                // paired with a flag marking the currently-selected match.
                let mut match_rects: Vec<(Bounds<Pixels>, bool)> = Vec::new();

                for (idx, m) in matches.iter().enumerate() {
                    let is_current = idx == current_match;
                    if let (Ok(start_pos), Ok(end_pos)) = (
                        buffer.offset_to_position(m.start),
                        buffer.offset_to_position(m.end),
                    ) {
                        // Only render matches that are on the same line and on a visible (non-folded) line
                        if start_pos.line == end_pos.line {
                            if let Some(&display_slot) = buf_to_display.get(&start_pos.line) {
                                let rect = Bounds::new(
                                    point(
                                        bounds.origin.x
                                            + gutter_width
                                            + char_width * (start_pos.column as f32),
                                        bounds.origin.y
                                            + line_height * (display_slot as f32 - scroll_offset),
                                    ),
                                    size(
                                        char_width
                                            * (end_pos.column.saturating_sub(start_pos.column)
                                                as f32)
                                                .max(1.0),
                                        line_height,
                                    ),
                                );
                                match_rects.push((rect, is_current));
                            }
                        }
                    }
                }

                FindPanelData {
                    query,
                    replace_query,
                    show_replace,
                    search_field_focused,
                    match_rects,
                    total_matches,
                    current_match_display,
                    line_height,
                }
            },
        );

        // Compute inline suggestion render data — the ghost text is painted right
        // after the real text on the cursor's line, at the cursor X position.
        let inline_suggestion = inline_suggestion_snapshot.and_then(|(text, anchor_offset)| {
            // Resolve the anchor offset to a line/column so we can compute the pixel origin.
            let Ok(anchor_pos) = buffer.offset_to_position(anchor_offset) else {
                return None;
            };
            // Only paint when the anchor line is visible (not folded) in the viewport.
            let &display_slot = buf_to_display.get(&anchor_pos.line)?;
            let pixel_x = bounds.origin.x + gutter_width + char_width * (anchor_pos.column as f32);
            let pixel_y = bounds.origin.y + line_height * (display_slot as f32 - scroll_offset);
            Some(InlineSuggestionRenderData {
                text,
                origin: point(pixel_x, pixel_y),
                line_height,
            })
        });

        // Build go-to-line overlay data if the dialog is open
        let goto_line_panel =
            goto_line_info.map(|(query, is_valid, total_lines)| GoToLinePanelData {
                query,
                is_valid,
                total_lines,
                line_height,
            });

        // ── Indent guides (feat-038) ──────────────────────────────────────────
        // For each visible line we compute the indentation depth (in 4-space stops).
        // We then collect runs of consecutive lines that share the same depth (or
        // deeper) and emit a thin vertical rule at each active indent-stop column.
        //
        // To keep the algorithm simple and cheap we build one `IndentGuideData` per
        // (indent-level, continuous-run) pair rather than merging across the whole
        // viewport.
        let indent_tab_size: usize = 4;
        let indent_guides = {
            // For every visible display slot record its indent depth in tab-stops.
            // We use `display_lines[slot]` to get the buffer line index, which
            // correctly skips folded lines.
            let mut line_depths: Vec<usize> = Vec::with_capacity(visible_range.len());
            for display_slot in visible_range.clone() {
                let buffer_line = display_lines[display_slot];
                let text = buffer.line(buffer_line).unwrap_or_default();
                let leading_spaces = text.chars().take_while(|c| *c == ' ').count();
                line_depths.push(leading_spaces / indent_tab_size);
            }

            let cursor_depth = {
                let text = buffer.line(cursor_pos.line).unwrap_or_default();
                let leading = text.chars().take_while(|c| *c == ' ').count();
                leading / indent_tab_size
            };

            let mut guides: Vec<IndentGuideData> = Vec::new();
            // For each indent level in 1..=max_depth, find runs of lines that
            // have at least that depth and emit a guide spanning those runs.
            let max_depth = line_depths.iter().copied().max().unwrap_or(0);
            for level in 1..=max_depth {
                let guide_x = bounds.origin.x
                    + gutter_width
                    + char_width * ((level * indent_tab_size) as f32);
                let is_active = level <= cursor_depth;

                let mut run_start: Option<usize> = None;
                for (slot, &depth) in line_depths.iter().enumerate() {
                    if depth >= level {
                        if run_start.is_none() {
                            run_start = Some(slot);
                        }
                    } else if let Some(start) = run_start.take() {
                        guides.push(IndentGuideData {
                            x: guide_x,
                            y_start: bounds.origin.y + line_height * (start as f32),
                            y_end: bounds.origin.y + line_height * (slot as f32),
                            is_active,
                        });
                    }
                }
                // Close any open run at the bottom of the viewport
                if let Some(start) = run_start.take() {
                    guides.push(IndentGuideData {
                        x: guide_x,
                        y_start: bounds.origin.y + line_height * (start as f32),
                        y_end: bounds.origin.y + line_height * (line_depths.len() as f32),
                        is_active,
                    });
                }
            }
            guides
        };

        // ── Scrollbar (feat-039) ──────────────────────────────────────────────
        // Paint a minimal 6-px scrollbar on the right edge only when the content
        // is taller than the viewport.
        const SCROLLBAR_WIDTH: f32 = 6.0;
        let scrollbar = if display_line_count > visible_range.len() {
            let track_x = bounds.origin.x + bounds.size.width - px(SCROLLBAR_WIDTH);
            let track = Bounds::new(
                point(track_x, bounds.origin.y),
                size(px(SCROLLBAR_WIDTH), bounds.size.height),
            );

            // Thumb height proportional to the visible fraction
            let visible_fraction = visible_range.len() as f32 / display_line_count as f32;
            let thumb_height = (bounds.size.height * visible_fraction).max(px(20.0));

            // Thumb top proportional to the scroll position
            let scroll_fraction = scroll_offset / display_line_count as f32;
            let max_thumb_y = bounds.size.height - thumb_height;
            let thumb_y = (bounds.size.height * scroll_fraction).min(max_thumb_y);

            let thumb = Bounds::new(
                point(track_x, bounds.origin.y + thumb_y),
                size(px(SCROLLBAR_WIDTH), thumb_height),
            );
            Some(ScrollbarData { track, thumb })
        } else {
            None
        };

        // ── Reference highlights (feat-047) ──────────────────────────────────
        // Convert buffer byte ranges to pixel rects for visible ranges only.
        let reference_highlights: Vec<ReferenceHighlightData> = reference_ranges_snapshot
            .iter()
            .filter_map(|range| {
                // Map start byte to line/col
                let Ok(start_pos) = buffer.offset_to_position(range.start) else {
                    return None;
                };
                let Ok(end_pos) = buffer.offset_to_position(range.end) else {
                    return None;
                };
                // Only show if the start of the range is on a visible (non-folded) line
                let &display_slot = buf_to_display.get(&start_pos.line)?;
                let x = bounds.origin.x + gutter_width + char_width * (start_pos.column as f32);
                let y = bounds.origin.y + line_height * (display_slot as f32 - scroll_offset);
                let width = char_width
                    * if start_pos.line == end_pos.line {
                        (end_pos.column.saturating_sub(start_pos.column)) as f32
                    } else {
                        // Multi-line: just highlight to end of line
                        let line_text = buffer.line(start_pos.line).unwrap_or_default();
                        (line_text.chars().count().saturating_sub(start_pos.column)) as f32
                    };
                Some(ReferenceHighlightData {
                    rect: Bounds::new(point(x, y), size(width.max(char_width), line_height)),
                })
            })
            .collect();

        // ── Rename overlay (feat-048) ──────────────────────────────────────────
        // Compute pixel bounds of the word being renamed so the overlay box can
        // be painted directly over it.
        let rename_overlay = rename_snapshot.and_then(|(new_name, word_start, _word_end)| {
            let Ok(start_pos) = buffer.offset_to_position(word_start) else {
                return None;
            };
            // Only show when the renamed word's line is visible (not folded)
            let &display_slot = buf_to_display.get(&start_pos.line)?;
            let x = bounds.origin.x + gutter_width + char_width * (start_pos.column as f32);
            let y = bounds.origin.y + line_height * (display_slot as f32 - scroll_offset);
            let box_width = (char_width * new_name.len().max(6) as f32 + px(16.0)).max(px(80.0));
            Some(RenameOverlayData {
                new_name,
                word_rect: Bounds::new(point(x, y), size(box_width, line_height)),
                line_height,
            })
        });

        // ── Context menu (feat-045) ────────────────────────────────────────────
        let context_menu = context_menu_snapshot.map(|(items, origin_x, origin_y, highlighted)| {
            ContextMenuRenderData {
                items,
                origin: point(
                    bounds.origin.x + px(origin_x),
                    bounds.origin.y + px(origin_y),
                ),
                highlighted,
                line_height,
            }
        });

        PrepaintState {
            bounds,
            line_height,
            shaped_lines,
            cursor_bounds,
            selection_rects,
            extra_cursor_bounds,
            extra_selection_rects,
            bracket_highlight_rects,
            completion_menu,
            diagnostics: diag_bounds,
            hover_tooltip,
            find_panel,
            gutter_width,
            gutter_lines,
            inline_suggestion,
            goto_line_panel,
            indent_guides,
            scrollbar,
            soft_wrap,
            reference_highlights,
            rename_overlay,
            context_menu,
            sub_line_offset,
            fold_chevrons,
        }
    }

    fn paint(
        &mut self,
        _id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        _bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        prepaint: &mut Self::PrepaintState,
        window: &mut Window,
        cx: &mut App,
    ) {
        let editor = self.editor.read(cx);
        let focus_handle = editor.focus_handle(cx);
        let is_focused = focus_handle.is_focused(window);
        // End the borrow of `editor` before calling `handle_input`, which needs
        // exclusive access to update the entity via EntityInputHandler.
        let _ = editor;

        // Register this element as the IME / character input handler for the window.
        // This routes OS-level text input (including IME composition on macOS/Linux/Windows)
        // through `EntityInputHandler` rather than the raw `key_down` event path.
        window.handle_input(
            &focus_handle,
            ElementInputHandler::new(prepaint.bounds, self.editor.clone()),
            cx,
        );

        // Paint background for the full editor area
        window.paint_quad(fill(prepaint.bounds, cx.theme().colors.background));

        // ── Gutter (Phase 6) ──────────────────────────────────────────────────
        let gutter_width = prepaint.gutter_width;
        let gutter_bounds = Bounds::new(
            prepaint.bounds.origin,
            size(
                gutter_width - GUTTER_SEPARATOR_WIDTH,
                prepaint.bounds.size.height,
            ),
        );
        // Gutter background – same as the editor background
        window.paint_quad(fill(gutter_bounds, cx.theme().colors.background));

        // Paint line numbers right-aligned inside the gutter padding, excluding the chevron zone
        let gutter_text_area_width =
            gutter_width - GUTTER_SEPARATOR_WIDTH - FOLD_CHEVRON_ZONE - GUTTER_PADDING * 2.0;
        for (slot_idx, gutter_line) in prepaint.gutter_lines.iter().enumerate() {
            let line_y = prepaint.bounds.origin.y + prepaint.line_height * (slot_idx as f32)
                - prepaint.sub_line_offset;

            // Highlight the current-line row across the entire gutter
            if gutter_line.is_active {
                let active_row_bounds = Bounds::new(
                    point(prepaint.bounds.origin.x, line_y),
                    size(gutter_width - GUTTER_SEPARATOR_WIDTH, prepaint.line_height),
                );
                window.paint_quad(fill(active_row_bounds, cx.theme().colors.list_hover));
            }

            // Right-align: start so that the text ends at GUTTER_PADDING from separator
            let text_x = prepaint.bounds.origin.x
                + GUTTER_PADDING
                + (gutter_text_area_width - gutter_line.shaped.width).max(px(0.0));

            let _ = gutter_line.shaped.paint(
                point(text_x, line_y),
                prepaint.line_height,
                TextAlign::Left,
                None,
                window,
                cx,
            );
        }

        // Separator line between gutter and text content
        let separator_bounds = Bounds::new(
            point(
                prepaint.bounds.origin.x + gutter_width - GUTTER_SEPARATOR_WIDTH,
                prepaint.bounds.origin.y,
            ),
            size(GUTTER_SEPARATOR_WIDTH, prepaint.bounds.size.height),
        );
        window.paint_quad(fill(separator_bounds, cx.theme().colors.border));

        // ── Fold chevrons ─────────────────────────────────────────────────────
        // Geometric triangles drawn with paint_path so they scale criply at any
        // DPI. The triangle points right (▶) when folded and down (▼) when open.
        // The chevron brightens to full foreground when the cursor is over it.
        {
            let mouse_pos = window.mouse_position();
            let normal_color = cx.theme().colors.muted_foreground.opacity(0.55);
            let hover_color = cx.theme().colors.foreground;

            for chevron in &prepaint.fold_chevrons {
                let rect = chevron.rect;
                let center = rect.center();
                // Use ~40% of the bounding rect as the triangle's half-extent so
                // there is breathing room and the shape doesn't look crowded.
                let half = rect.size.width * 0.4;
                let color = if rect.contains(&mouse_pos) {
                    hover_color
                } else {
                    normal_color
                };

                // Each path starts at a real corner vertex so it's a proper
                // 3-vertex triangle (not a 4-sided shape with an interior start point).
                let path = if chevron.is_folded {
                    // Right-pointing ▶: top-left → right-tip → bottom-left
                    let mut p = Path::new(point(center.x - half, center.y - half));
                    p.line_to(point(center.x + half, center.y));
                    p.line_to(point(center.x - half, center.y + half));
                    p
                } else {
                    // Down-pointing ▼: top-left → top-right → bottom-tip
                    let mut p = Path::new(point(center.x - half, center.y - half * 0.6));
                    p.line_to(point(center.x + half, center.y - half * 0.6));
                    p.line_to(point(center.x, center.y + half));
                    p
                };
                window.paint_path(path, color);
            }

            // Push hit-test rects so mouse handlers can resolve clicks.
            let chevrons_for_cache: Vec<(usize, Bounds<Pixels>)> = prepaint
                .fold_chevrons
                .iter()
                .map(|c| (c.start_line, c.rect))
                .collect();
            self.editor.update(cx, |editor, _cx| {
                editor.update_cached_fold_chevrons(chevrons_for_cache);
            });
        }
        // ─────────────────────────────────────────────────────────────────────

        // Paint selection highlighting (already offset for gutter in prepaint)
        for sel_rect in &prepaint.selection_rects {
            window.paint_quad(fill(*sel_rect, cx.theme().colors.selection));
        }

        // Paint extra cursor selection highlights with the same color as the primary selection.
        for sel_rect in &prepaint.extra_selection_rects {
            window.paint_quad(fill(*sel_rect, cx.theme().colors.selection));
        }

        // Paint bracket pair highlights (feat-026).
        // Each enclosing bracket pair gets a distinct tint; outermost pairs use the first color.
        // Zero-size sentinel rects (for off-screen brackets) are skipped by the size check.
        let bracket_highlight_colors = [
            hsla(0.55, 0.70, 0.70, 0.25), // blue-ish — outermost
            hsla(0.35, 0.70, 0.70, 0.25), // green-ish
            hsla(0.10, 0.70, 0.70, 0.25), // orange-ish
        ];
        for (depth, (open_rect, close_rect)) in prepaint.bracket_highlight_rects.iter().enumerate()
        {
            let color = bracket_highlight_colors[depth % bracket_highlight_colors.len()];
            if open_rect.size.width > px(0.0) {
                window.paint_quad(fill(*open_rect, color));
            }
            if close_rect.size.width > px(0.0) {
                window.paint_quad(fill(*close_rect, color));
            }
        }

        // Paint find-match highlights BEFORE text so text is readable on top
        if let Some(ref panel) = prepaint.find_panel {
            for (rect, is_current) in panel.match_rects.iter() {
                // Current match: orange; other matches: subtle yellow
                let color: Hsla = if *is_current {
                    hsla(0.08, 0.9, 0.55, 0.55) // orange
                } else {
                    hsla(0.14, 0.7, 0.6, 0.30) // muted yellow
                };
                window.paint_quad(fill(*rect, color));
            }
        }

        // Paint visible text lines, starting after the gutter
        let mut offset_y = -prepaint.sub_line_offset;
        let text_origin_x = prepaint.bounds.origin.x + gutter_width;
        let origin_y = prepaint.bounds.origin.y;

        for shaped_line in prepaint.shaped_lines.iter() {
            let line_origin = point(text_origin_x, origin_y + offset_y);

            let _ = shaped_line.paint(
                line_origin,
                prepaint.line_height,
                TextAlign::Left,
                None,
                window,
                cx,
            );

            offset_y += prepaint.line_height;
        }

        // Paint cursor if focused (already offset for gutter in prepaint)
        if is_focused {
            if let Some(cursor_bounds) = prepaint.cursor_bounds {
                window.paint_quad(fill(cursor_bounds, cx.theme().colors.caret));
            }

            // Paint extra cursors (multi-cursor) with the same style as the primary.
            for extra_bounds in &prepaint.extra_cursor_bounds {
                window.paint_quad(fill(*extra_bounds, cx.theme().colors.caret));
            }
        }

        // Paint ghost-text inline suggestion right after the cursor position.
        // The text is dimmed to distinguish it from real buffer content and
        // give the user a clear preview of what will be inserted.
        if let Some(ref suggestion) = prepaint.inline_suggestion {
            self.paint_inline_suggestion(suggestion, window, cx);
        }

        // Paint error squiggles (diagnostics) using pre-calculated bounds
        if !prepaint.diagnostics.is_empty() {
            for (_highlight, squiggle_bounds) in &prepaint.diagnostics {
                let squiggle_color = cx.theme().colors.danger;
                let squiggle_height = px(2.0);
                let squiggle_spacing = px(2.0);
                let squiggle_y =
                    squiggle_bounds.origin.y + squiggle_bounds.size.height - squiggle_height;

                let mut x = squiggle_bounds.origin.x;
                while x < squiggle_bounds.origin.x + squiggle_bounds.size.width {
                    let marker_width =
                        (squiggle_bounds.origin.x + squiggle_bounds.size.width - x).min(px(4.0));
                    let marker_bounds =
                        Bounds::new(point(x, squiggle_y), size(marker_width, squiggle_height));
                    window.paint_quad(fill(marker_bounds, squiggle_color));
                    x += squiggle_spacing;
                }
            }
        }

        // Paint completion menu if open
        if let Some(ref menu) = prepaint.completion_menu {
            self.paint_completion_menu(menu, window, cx);
        } else {
            // Clear stale bounds so clicks never land inside a menu that isn't visible.
            self.editor.update(cx, |editor, _cx| {
                editor.update_cached_completion_menu_bounds(None);
            });
        }

        // Paint hover tooltip if hovering
        if let Some(ref hover) = prepaint.hover_tooltip {
            self.paint_hover_tooltip(hover, window, cx);
        }

        // Paint find/replace panel (top-right overlay)
        if let Some(ref panel) = prepaint.find_panel {
            self.paint_find_panel(panel, prepaint.bounds, window, cx);
        }

        // Paint indent guides (feat-038) — thin vertical rules between text and gutter
        self.paint_indent_guides(&prepaint.indent_guides, window, cx);

        // Paint scrollbar (feat-039)
        if let Some(ref scrollbar) = prepaint.scrollbar {
            self.paint_scrollbar(scrollbar, window, cx);
        }

        // Paint go-to-line overlay (feat-040)
        if let Some(ref panel) = prepaint.goto_line_panel {
            self.paint_goto_line_panel(panel, prepaint.bounds, window, cx);
        }

        // Paint reference highlights (feat-047) — semi-transparent amber tint
        self.paint_reference_highlights(&prepaint.reference_highlights, window);

        // Paint rename overlay (feat-048) — inline input box over the word
        if let Some(ref overlay) = prepaint.rename_overlay {
            self.paint_rename_overlay(overlay, window, cx);
        }

        // Paint context menu (feat-045) — floating popup
        if let Some(ref menu) = prepaint.context_menu {
            self.paint_context_menu(menu, window, cx);
        }
    }
}

/// Pre-shaped text data for a single completion menu item, produced before
/// any painting so that the `text_system` borrow is dropped before `window`
/// is borrowed mutably for painting.
struct ShapedCompletionItem {
    /// Shaped label text
    label: ShapedLine,
    /// Shaped separator + detail text ("· Keyword"), if detail is present
    detail: Option<ShapedLine>,
    /// Accent color for the 3-px left-edge pill derived from the item kind
    accent: Hsla,
}

/// Map an LSP `CompletionItemKind` to a short human-readable badge string shown
/// in the left column of the completion menu (e.g. "kw", "fn", "tbl").
fn completion_kind_badge(kind: lsp_types::CompletionItemKind) -> String {
    use lsp_types::CompletionItemKind as K;
    match kind {
        K::KEYWORD => "kw",
        K::FUNCTION => "fn",
        K::METHOD => "mth",
        K::FIELD => "col",
        K::VARIABLE => "var",
        K::MODULE => "mod",
        K::CLASS => "cls",
        K::INTERFACE => "ifc",
        K::STRUCT => "str",
        K::ENUM => "enm",
        K::ENUM_MEMBER => "enm",
        K::CONSTANT => "con",
        K::PROPERTY => "prp",
        K::CONSTRUCTOR => "ctor",
        K::SNIPPET => "snp",
        K::TEXT => "txt",
        K::VALUE => "val",
        K::UNIT => "unt",
        K::FILE => "file",
        K::REFERENCE => "ref",
        K::FOLDER => "dir",
        K::COLOR => "clr",
        K::TYPE_PARAMETER => "tpr",
        K::OPERATOR => "op",
        K::EVENT => "evt",
        _ => "?",
    }
    .to_string()
}

/// Map the kind badge string to a muted accent color used for the left-edge pill.
fn kind_accent_color(badge: Option<&str>) -> Hsla {
    match badge {
        Some("kw") => hsla(0.60, 0.55, 0.58, 0.85), // muted blue – keywords
        Some("fn") | Some("mth") | Some("ctor") => hsla(0.08, 0.65, 0.58, 0.85), // amber – functions
        Some("col") | Some("prp") => hsla(0.36, 0.50, 0.55, 0.85), // teal – columns/fields
        Some("tbl") | Some("mod") | Some("dir") | Some("file") => hsla(0.12, 0.55, 0.58, 0.85), // orange – tables
        Some("snp") => hsla(0.75, 0.45, 0.60, 0.85), // purple – snippets
        Some("con") => hsla(0.55, 0.50, 0.55, 0.85), // cyan – constants
        _ => hsla(0.0, 0.0, 0.45, 0.85),             // neutral gray
    }
}

impl EditorElement {
    /// Paint the ghost-text inline suggestion at the pre-calculated origin.
    ///
    /// The suggestion is rendered in a dimmed color so the user can clearly
    /// distinguish it from real buffer content. Only the first line is shown
    /// (multi-line suggestions are truncated at the first newline) to keep the
    /// single-line visual contract of the cursor row.
    fn paint_inline_suggestion(
        &self,
        suggestion: &InlineSuggestionRenderData,
        window: &mut Window,
        cx: &mut App,
    ) {
        // Only show the first line so the suggestion stays on the cursor row.
        let display_text = suggestion.text.lines().next().unwrap_or("").to_string();

        if display_text.is_empty() {
            return;
        }

        let text_system = window.text_system();
        let font_size = suggestion.line_height * 0.9;
        let text_run = TextRun {
            len: display_text.len(),
            font: Font::default(),
            // Dimmed color to visually separate ghost text from real content.
            color: cx.theme().colors.muted_foreground.opacity(0.5),
            background_color: None,
            underline: None,
            strikethrough: None,
        };
        let shaped = text_system.shape_line(display_text.into(), font_size, &[text_run], None);
        let _ = shaped.paint(
            suggestion.origin,
            suggestion.line_height,
            TextAlign::Left,
            None,
            window,
            cx,
        );
    }

    /// Paint the completion menu.
    ///
    /// Each item row has a 3-px colored left-edge accent pill that reflects the
    /// item kind, followed by the label in normal weight and a muted `· detail`
    /// suffix. The selected row is highlighted with a faint blue tint plus a
    /// 2-px solid accent bar on the left edge.
    fn paint_completion_menu(
        &self,
        menu: &CompletionMenuRenderData,
        window: &mut Window,
        cx: &mut App,
    ) {
        let total_items = menu.items.len();
        let item_count = total_items.min(MAX_COMPLETION_ITEMS);
        if item_count == 0 {
            return;
        }

        // ── Layout constants ────────────────────────────────────────────────
        // Rows are slightly taller than the editor line height for breathing room.
        let item_height = (menu.line_height * 1.25).max(px(22.0));
        let left_inset = px(12.0); // space from the accent pill to the label
        let accent_pill_width = px(3.0);
        let right_padding = px(12.0);
        let detail_gap = px(8.0);
        let font_size = (menu.line_height * 0.80).max(px(11.5));

        // When the list is taller than the visible window, reserve space on the
        // right edge for a thin scrollbar track.
        let scrollbar_width = if total_items > MAX_COMPLETION_ITEMS {
            px(5.0)
        } else {
            px(0.0)
        };

        // ── Colors ──────────────────────────────────────────────────────────
        let bg = cx.theme().colors.popover;
        let border = cx.theme().colors.border;
        let selected_tint = cx.theme().colors.list_hover;
        let selected_bar = cx.theme().colors.primary;
        let label_color = cx.theme().colors.popover_foreground;
        let detail_color = cx.theme().colors.muted_foreground;

        // ── Pre-shape ALL items ──────────────────────────────────────────────
        // We must shape every item (not just the visible window) so that the
        // required menu width is stable across scroll positions and does not jump
        // frame-to-frame as different items scroll into view.
        let visible_start = menu.scroll_offset;
        let shaped_items: Vec<ShapedCompletionItem> = {
            let text_system = window.text_system();
            menu.items
                .iter()
                .map(|item| {
                    let accent = kind_accent_color(item.kind_badge.as_deref());

                    let label_run = TextRun {
                        len: item.label.len(),
                        font: Font::default(),
                        color: label_color,
                        background_color: None,
                        underline: None,
                        strikethrough: None,
                    };
                    let label = text_system.shape_line(
                        item.label.clone().into(),
                        font_size,
                        &[label_run],
                        None,
                    );

                    let detail = item.detail.as_ref().map(|d| {
                        // Prefix with an interpunct to visually separate from the label.
                        let text = format!("· {d}");
                        let run = TextRun {
                            len: text.len(),
                            font: Font::default(),
                            color: detail_color,
                            background_color: None,
                            underline: None,
                            strikethrough: None,
                        };
                        text_system.shape_line(text.into(), font_size * 0.93, &[run], None)
                    });

                    ShapedCompletionItem {
                        label,
                        detail,
                        accent,
                    }
                })
                .collect()
        }; // text_system borrow released; window is exclusively ours again.

        // Compute the minimum width needed to show the widest item without clipping,
        // then clamp: at least 240px, at most 600px. Beyond 600px we let items truncate
        // naturally — identifiers that long are genuinely rare.
        let required_width = shaped_items.iter().fold(px(0.0), |acc, s| {
            let detail_w = s
                .detail
                .as_ref()
                .map(|d| d.width + detail_gap)
                .unwrap_or(px(0.0));
            let row_w = left_inset + s.label.width + detail_w + right_padding + scrollbar_width;
            if row_w > acc {
                row_w
            } else {
                acc
            }
        });
        let menu_width = required_width.max(px(240.0)).min(px(600.0));

        let menu_height = item_height * item_count as f32;

        let menu_x = menu.cursor_bounds.origin.x;
        let menu_y = menu.cursor_bounds.origin.y + menu.cursor_bounds.size.height + px(4.0);

        let menu_bounds = Bounds::new(point(menu_x, menu_y), size(menu_width, menu_height));

        // ── Drop shadow (two semi-transparent dark quads offset below-right) ─
        let shadow_color = hsla(0.0, 0.0, 0.0, 0.35);
        window.paint_quad(fill(
            Bounds::new(
                point(menu_x + px(2.0), menu_y + px(4.0)),
                size(menu_width, menu_height),
            ),
            shadow_color,
        ));
        window.paint_quad(fill(
            Bounds::new(
                point(menu_x + px(1.0), menu_y + px(2.0)),
                size(menu_width, menu_height),
            ),
            hsla(0.0, 0.0, 0.0, 0.20),
        ));

        // ── Background + border ─────────────────────────────────────────────
        window.paint_quad(fill(menu_bounds, bg));
        // Border as four 1-px quads (GPUI has no stroke primitive)
        let bw = px(1.0);
        window.paint_quad(fill(
            Bounds::new(point(menu_x, menu_y), size(menu_width, bw)),
            border,
        ));
        window.paint_quad(fill(
            Bounds::new(
                point(menu_x, menu_y + menu_height - bw),
                size(menu_width, bw),
            ),
            border,
        ));
        window.paint_quad(fill(
            Bounds::new(point(menu_x, menu_y), size(bw, menu_height)),
            border,
        ));
        window.paint_quad(fill(
            Bounds::new(
                point(menu_x + menu_width - bw, menu_y),
                size(bw, menu_height),
            ),
            border,
        ));

        for (slot_idx, shaped) in shaped_items
            .iter()
            .skip(visible_start)
            .take(MAX_COMPLETION_ITEMS)
            .enumerate()
        {
            // `absolute_idx` is this item's position in the full (unsliced) list.
            let absolute_idx = visible_start + slot_idx;
            let item_y = menu_y + item_height * slot_idx as f32;
            let is_selected = absolute_idx == menu.selected_index;

            // ── Selected-row highlight ───────────────────────────────────────
            if is_selected {
                window.paint_quad(fill(
                    Bounds::new(point(menu_x, item_y), size(menu_width, item_height)),
                    selected_tint,
                ));
                // 2-px left accent bar (replaces the old badge column separator)
                window.paint_quad(fill(
                    Bounds::new(point(menu_x, item_y), size(px(2.0), item_height)),
                    selected_bar,
                ));
            } else {
                // Unselected rows: 3-px colored pill for item kind
                window.paint_quad(fill(
                    Bounds::new(
                        point(menu_x, item_y + item_height * 0.2),
                        size(accent_pill_width, item_height * 0.6),
                    ),
                    shaped.accent,
                ));
            }

            // Text is vertically centered within item_height.
            let text_y = item_y + (item_height - font_size) / 2.0 - px(1.0);

            // ── Label ────────────────────────────────────────────────────────
            let label_x = menu_x + left_inset;
            let _ = shaped.label.paint(
                point(label_x, text_y),
                item_height,
                TextAlign::Left,
                None,
                window,
                cx,
            );

            // ── Detail (immediately after label with a gap) ──────────────────
            if let Some(ref detail) = shaped.detail {
                let detail_x = label_x + shaped.label.width + px(6.0);
                // Only paint if it fits inside the menu (leave room for scrollbar)
                let right_clip = menu_x + menu_width - scrollbar_width - px(8.0);
                if detail_x + detail.width < right_clip {
                    let _ = detail.paint(
                        point(detail_x, text_y + px(0.5)), // nudge down half-px to optically align
                        item_height,
                        TextAlign::Left,
                        None,
                        window,
                        cx,
                    );
                }
            }
        }

        // ── Scrollbar ───────────────────────────────────────────────────────
        // Paint a thin thumb on the right edge only when there are more items
        // than the visible window can show.
        if total_items > MAX_COMPLETION_ITEMS {
            let track_x = menu_x + menu_width - scrollbar_width;
            let track_bounds =
                Bounds::new(point(track_x, menu_y), size(scrollbar_width, menu_height));
            window.paint_quad(fill(track_bounds, hsla(0.0, 0.0, 0.5, 0.15)));

            let scroll_fraction = visible_start as f32 / total_items as f32;
            let visible_fraction = MAX_COMPLETION_ITEMS as f32 / total_items as f32;
            let thumb_height = (menu_height * visible_fraction).max(px(16.0));
            let max_thumb_y = menu_height - thumb_height;
            let thumb_y = menu_y + (menu_height * scroll_fraction).min(max_thumb_y);
            window.paint_quad(fill(
                Bounds::new(
                    point(track_x + px(1.0), thumb_y),
                    size(scrollbar_width - px(2.0), thumb_height),
                ),
                hsla(0.0, 0.0, 0.6, 0.55),
            ));
        }

        // Push the menu's layout snapshot to the editor so `handle_mouse_down`
        // can hit-test clicks in window space without accessing element data.
        self.editor.update(cx, |editor, _cx| {
            editor.update_cached_completion_menu_bounds(Some(crate::CachedCompletionMenuBounds {
                bounds: menu_bounds,
                item_height,
                item_count,
            }));
        });
    }

    /// Paint the hover tooltip.
    ///
    /// LSP hover responses use Markdown formatting that the text system cannot
    /// render directly. We preprocess the documentation string to extract
    /// structure (bold headings, inline code spans) and render each segment
    /// with the appropriate color and font weight using low-level `shape_line`
    /// calls stacked vertically inside a shared rounded background quad.
    fn paint_hover_tooltip(&self, hover: &HoverTooltipData, window: &mut Window, cx: &mut App) {
        // ── Layout constants ─────────────────────────────────────────────────
        let pad_x = px(12.0);
        let pad_y = px(10.0);
        let max_content_width = px(420.0);
        let corner_radius = px(6.0);
        let title_font_size = px(12.5);
        let body_font_size = px(12.0);
        let title_line_height = px(20.0);
        let body_line_height = px(17.0);
        let blank_gap = px(5.0);
        // Guard against enormous tooltips from verbose LSP responses.
        let max_lines = 15usize;

        // ── Colors ───────────────────────────────────────────────────────────
        let bg_color = cx.theme().colors.popover;
        let border_color = cx.theme().colors.border;
        let title_color = cx.theme().colors.popover_foreground;
        let body_color = cx.theme().colors.muted_foreground;
        // Cyan for backtick code spans — visually separates identifiers from prose.
        let code_color: Hsla = rgb(0x7dcfff).into();

        let lines = preprocess_hover_text(&hover.documentation);
        if lines.is_empty() {
            return;
        }
        let lines = &lines[..lines.len().min(max_lines)];

        let text_system = window.text_system();

        // ── Shape each line; record its intrinsic height ─────────────────────
        struct ShapedRow {
            shaped: Option<ShapedLine>,
            height: Pixels,
        }

        let rows: Vec<ShapedRow> = lines
            .iter()
            .map(|line| match line {
                TooltipLine::Blank => ShapedRow {
                    shaped: None,
                    height: blank_gap,
                },
                TooltipLine::Title(text) => {
                    let bold_font = Font {
                        weight: FontWeight::BOLD,
                        ..Font::default()
                    };
                    let safe_text: SharedString = if text.is_empty() {
                        " ".into()
                    } else {
                        text.clone().into()
                    };
                    let run_len = safe_text.len();
                    let shaped = text_system.shape_line(
                        safe_text,
                        title_font_size,
                        &[TextRun {
                            len: run_len,
                            font: bold_font,
                            color: title_color,
                            background_color: None,
                            underline: None,
                            strikethrough: None,
                        }],
                        None,
                    );
                    ShapedRow {
                        shaped: Some(shaped),
                        height: title_line_height,
                    }
                }
                TooltipLine::Body(text) => {
                    let (display_text, runs) = build_body_runs(text, body_color, code_color);
                    let shaped = text_system.shape_line(display_text, body_font_size, &runs, None);
                    ShapedRow {
                        shaped: Some(shaped),
                        height: body_line_height,
                    }
                }
            })
            .collect();

        if rows.is_empty() {
            return;
        }

        // ── Measure ─────────────────────────────────────────────────────────
        let content_width = rows
            .iter()
            .filter_map(|r| r.shaped.as_ref())
            .map(|sl| sl.width)
            .fold(px(0.0), |acc, w| if w > acc { w } else { acc })
            .min(max_content_width);
        let content_height = rows.iter().fold(px(0.0), |acc, r| acc + r.height);

        let tooltip_width = content_width + pad_x * 2.0;
        let tooltip_height = content_height + pad_y * 2.0;

        // ── Position ─────────────────────────────────────────────────────────
        // Prefer showing above the hovered word; fall back to below when
        // there isn't enough vertical room between the word and the window top.
        let gap = px(6.0);
        let above_y = hover.anchor_bounds.origin.y - tooltip_height - gap;
        let below_y = hover.anchor_bounds.origin.y + hover.anchor_bounds.size.height + gap;
        let origin_y = if above_y > px(0.0) { above_y } else { below_y };
        let tooltip_origin = point(hover.anchor_bounds.origin.x, origin_y);
        let tooltip_bounds = Bounds::new(tooltip_origin, size(tooltip_width, tooltip_height));

        // ── Background (rounded, bordered) ────────────────────────────────────
        window.paint_quad(quad(
            tooltip_bounds,
            Corners::all(corner_radius),
            bg_color,
            Edges::all(px(1.0)),
            border_color,
            BorderStyle::default(),
        ));

        // ── Text rows ────────────────────────────────────────────────────────
        let text_x = tooltip_origin.x + pad_x;
        let mut current_y = tooltip_origin.y + pad_y;

        for row in &rows {
            if let Some(ref shaped_line) = row.shaped {
                let _ = shaped_line.paint(
                    point(text_x, current_y),
                    row.height,
                    TextAlign::Left,
                    None,
                    window,
                    cx,
                );
            }
            current_y = current_y + row.height;
        }
    }

    /// Paint the find/replace panel as a floating overlay anchored to the top-right of the editor.
    ///
    /// Layout (from top):
    ///   Row 0 – search field + match counter
    ///   Row 1 – replace field (only when `show_replace` is true)
    fn paint_find_panel(
        &self,
        panel: &FindPanelData,
        editor_bounds: Bounds<Pixels>,
        window: &mut Window,
        cx: &mut App,
    ) {
        let panel_width = px(340.0);
        let row_height = panel.line_height * 1.4;
        let padding = px(8.0);
        let rows = if panel.show_replace { 2 } else { 1 };
        let panel_height = row_height * rows as f32 + padding * 2.0;
        let font_size = panel.line_height * 0.82;

        // Position in the top-right corner of the editor
        let panel_x = editor_bounds.origin.x + editor_bounds.size.width - panel_width - px(4.0);
        let panel_y = editor_bounds.origin.y + px(4.0);
        let panel_origin = point(panel_x, panel_y);

        // Panel background with a subtle border
        let panel_bounds = Bounds::new(panel_origin, size(panel_width, panel_height));
        window.paint_quad(fill(panel_bounds, cx.theme().colors.popover));

        // Thin border around panel
        let border_width = px(1.0);
        let panel_border = cx.theme().colors.border;
        // Top border
        window.paint_quad(fill(
            Bounds::new(panel_origin, size(panel_width, border_width)),
            panel_border,
        ));
        // Bottom border
        window.paint_quad(fill(
            Bounds::new(
                point(panel_x, panel_y + panel_height - border_width),
                size(panel_width, border_width),
            ),
            panel_border,
        ));
        // Left border
        window.paint_quad(fill(
            Bounds::new(panel_origin, size(border_width, panel_height)),
            panel_border,
        ));
        // Right border
        window.paint_quad(fill(
            Bounds::new(
                point(panel_x + panel_width - border_width, panel_y),
                size(border_width, panel_height),
            ),
            panel_border,
        ));

        // ── Row 0: Search field ─────────────────────────────────────────────
        let row0_y = panel_y + padding;
        let field_width = panel_width - padding * 2.0 - px(80.0); // reserve space for status label

        // Search field background (slightly lighter to show focus)
        let search_bg: Hsla = if panel.search_field_focused {
            cx.theme().colors.input
        } else {
            cx.theme().colors.muted
        };
        let search_field_bounds = Bounds::new(
            point(panel_x + padding, row0_y),
            size(field_width, row_height),
        );
        window.paint_quad(fill(search_field_bounds, search_bg));

        // Search text (query + blinking cursor indicator "|" when focused)
        let display_query = if panel.search_field_focused {
            format!("{}_", panel.query)
        } else {
            panel.query.clone()
        };
        let search_label = if display_query.is_empty() {
            "Find...".to_string()
        } else {
            display_query
        };
        let search_color: Hsla = if panel.query.is_empty() && !panel.search_field_focused {
            cx.theme().colors.muted_foreground
        } else {
            cx.theme().colors.popover_foreground
        };
        Self::paint_panel_text(
            &search_label,
            point(
                panel_x + padding + px(4.0),
                row0_y + (row_height - panel.line_height) / 2.0,
            ),
            font_size,
            search_color,
            window,
            cx,
        );

        // Match count label (e.g. "3/12") anchored to the right side of the row
        let status_text = if panel.total_matches == 0 {
            if panel.query.is_empty() {
                String::new()
            } else {
                "No results".to_string()
            }
        } else {
            format!("{}/{}", panel.current_match_display, panel.total_matches)
        };
        if !status_text.is_empty() {
            let status_color: Hsla = if panel.total_matches == 0 {
                cx.theme().colors.danger
            } else {
                cx.theme().colors.muted_foreground
            };
            Self::paint_panel_text(
                &status_text,
                point(
                    panel_x + panel_width - padding - px(70.0),
                    row0_y + (row_height - panel.line_height) / 2.0,
                ),
                font_size,
                status_color,
                window,
                cx,
            );
        }

        // ── Row 1: Replace field (only when show_replace is true) ───────────
        if panel.show_replace {
            let row1_y = row0_y + row_height + px(2.0);
            let replace_bg: Hsla = if !panel.search_field_focused {
                cx.theme().colors.input
            } else {
                cx.theme().colors.muted
            };
            let replace_field_bounds = Bounds::new(
                point(panel_x + padding, row1_y),
                size(field_width, row_height),
            );
            window.paint_quad(fill(replace_field_bounds, replace_bg));

            let display_replace = if !panel.search_field_focused {
                format!("{}_", panel.replace_query)
            } else {
                panel.replace_query.clone()
            };
            let replace_label = if display_replace.is_empty() {
                "Replace...".to_string()
            } else {
                display_replace
            };
            let replace_color: Hsla =
                if panel.replace_query.is_empty() && panel.search_field_focused {
                    cx.theme().colors.muted_foreground
                } else {
                    cx.theme().colors.popover_foreground
                };
            Self::paint_panel_text(
                &replace_label,
                point(
                    panel_x + padding + px(4.0),
                    row1_y + (row_height - panel.line_height) / 2.0,
                ),
                font_size,
                replace_color,
                window,
                cx,
            );
        }
    }

    /// Helper: shape and paint a single-line text string inside the find panel.
    fn paint_panel_text(
        text: &str,
        origin: Point<Pixels>,
        font_size: Pixels,
        color: Hsla,
        window: &mut Window,
        cx: &mut App,
    ) {
        if text.is_empty() {
            return;
        }
        let text_run = TextRun {
            len: text.len(),
            font: Font::default(),
            color,
            background_color: None,
            underline: None,
            strikethrough: None,
        };
        let shaped =
            window
                .text_system()
                .shape_line(text.to_string().into(), font_size, &[text_run], None);
        let _ = shaped.paint(origin, font_size * 1.4, TextAlign::Left, None, window, cx);
    }

    /// Paint vertical indent-guide lines over the text area (feat-038).
    ///
    /// Each guide is a 1-px wide line spanning the vertical run of lines that
    /// share at least that indent depth.  The guide for the cursor's current
    /// indent level is painted brighter so the active scope is easy to spot.
    fn paint_indent_guides(&self, guides: &[IndentGuideData], window: &mut Window, cx: &App) {
        for guide in guides {
            let color: Hsla = if guide.is_active {
                cx.theme().colors.border
            } else {
                cx.theme().colors.border.opacity(0.5)
            };
            let height = guide.y_end - guide.y_start;
            if height > px(0.0) {
                window.paint_quad(fill(
                    Bounds::new(point(guide.x, guide.y_start), size(px(1.0), height)),
                    color,
                ));
            }
        }
    }

    /// Paint a minimal overlay scrollbar on the right edge (feat-039).
    ///
    /// The track is a translucent strip; the thumb is opaque enough to be
    /// visible without being distracting.
    fn paint_scrollbar(&self, scrollbar: &ScrollbarData, window: &mut Window, cx: &App) {
        // Track: very faint background
        window.paint_quad(fill(scrollbar.track, cx.theme().colors.scrollbar));
        // Thumb
        window.paint_quad(fill(scrollbar.thumb, cx.theme().colors.scrollbar_thumb));
    }

    /// Paint the go-to-line input overlay, centered near the top of the editor (feat-040).
    ///
    /// The border turns red when the typed number is out of the valid line range
    /// so the user gets immediate feedback without any modal dialog.
    fn paint_goto_line_panel(
        &self,
        panel: &GoToLinePanelData,
        editor_bounds: Bounds<Pixels>,
        window: &mut Window,
        cx: &mut App,
    ) {
        let panel_width = px(200.0);
        let font_size = panel.line_height * 0.82;
        let padding = px(8.0);
        let panel_height = panel.line_height * 1.4 + padding * 2.0;

        // Center horizontally at the very top of the editor
        let panel_x = editor_bounds.origin.x + (editor_bounds.size.width - panel_width) / 2.0;
        let panel_y = editor_bounds.origin.y + px(4.0);
        let panel_origin = point(panel_x, panel_y);
        let panel_bounds = Bounds::new(panel_origin, size(panel_width, panel_height));

        // Background
        window.paint_quad(fill(panel_bounds, cx.theme().colors.popover));

        // Border — red when the typed number is invalid, gray otherwise
        let border_color: Hsla = if panel.is_valid {
            cx.theme().colors.border
        } else {
            cx.theme().colors.danger
        };
        let bw = px(1.0);
        window.paint_quad(fill(
            Bounds::new(panel_origin, size(panel_width, bw)),
            border_color,
        ));
        window.paint_quad(fill(
            Bounds::new(
                point(panel_x, panel_y + panel_height - bw),
                size(panel_width, bw),
            ),
            border_color,
        ));
        window.paint_quad(fill(
            Bounds::new(panel_origin, size(bw, panel_height)),
            border_color,
        ));
        window.paint_quad(fill(
            Bounds::new(
                point(panel_x + panel_width - bw, panel_y),
                size(bw, panel_height),
            ),
            border_color,
        ));

        // Input text — either the typed digits with a cursor, or a placeholder hint
        let row_y = panel_y + padding;
        let (display, text_color): (String, Hsla) = if panel.query.is_empty() {
            (
                format!("Go to line (1–{})…", panel.total_lines),
                cx.theme().colors.muted_foreground,
            )
        } else {
            (
                format!("{}_", panel.query),
                cx.theme().colors.popover_foreground,
            )
        };
        Self::paint_panel_text(
            &display,
            point(panel_x + padding, row_y),
            font_size,
            text_color,
            window,
            cx,
        );
    }

    /// Paint semi-transparent amber rectangles behind each reference occurrence (feat-047).
    ///
    /// The color is intentionally different from the blue selection highlight so
    /// the user can tell references and selections apart at a glance.
    fn paint_reference_highlights(
        &self,
        highlights: &[ReferenceHighlightData],
        window: &mut Window,
    ) {
        // Amber tint: hue ~0.11 (orange-yellow), low saturation so it doesn't clash with syntax
        let color = hsla(0.11, 0.70, 0.55, 0.25);
        for highlight in highlights {
            window.paint_quad(fill(highlight.rect, color));
        }
    }

    /// Paint the inline rename-symbol input box over the word being renamed (feat-048).
    ///
    /// The box is painted directly over the word with a solid background and
    /// bright border so it is visually distinct from surrounding text. A blinking
    /// `_` cursor is appended to the new name text.
    fn paint_rename_overlay(&self, overlay: &RenameOverlayData, window: &mut Window, cx: &mut App) {
        let padding = px(4.0);
        let font_size = overlay.line_height * 0.82;
        let box_bounds = Bounds::new(
            point(
                overlay.word_rect.origin.x - padding,
                overlay.word_rect.origin.y,
            ),
            size(
                overlay.word_rect.size.width + padding * 2.0,
                overlay.line_height,
            ),
        );

        // Dark background + primary-colored border signals "you are renaming".
        // Rounded corners match the context menu and tooltip style.
        window.paint_quad(quad(
            box_bounds,
            Corners::all(px(4.0)),
            cx.theme().colors.popover,
            Edges::all(px(1.0)),
            cx.theme().colors.primary,
            BorderStyle::default(),
        ));

        // Input text with a cursor indicator
        let display = format!("{}_", overlay.new_name);
        Self::paint_panel_text(
            &display,
            point(box_bounds.origin.x + padding, box_bounds.origin.y),
            font_size,
            cx.theme().colors.popover_foreground,
            window,
            cx,
        );
    }

    /// Paint the right-click context menu as a floating popup (feat-045).
    ///
    /// Each enabled item is painted as a clickable row; disabled items are
    /// dimmed; separator items are thin horizontal rules.
    fn paint_context_menu(&self, menu: &ContextMenuRenderData, window: &mut Window, cx: &mut App) {
        let item_height = menu.line_height * 1.2;
        let padding_x = px(12.0);
        let font_size = menu.line_height * 0.82;
        let menu_width = px(180.0);

        // Calculate total height
        let total_height: Pixels = menu.items.iter().fold(px(0.0), |acc, (_, is_sep, _)| {
            acc + if *is_sep { px(8.0) } else { item_height }
        });

        let corner_radius = px(6.0);
        let menu_bounds = Bounds::new(menu.origin, size(menu_width, total_height + px(8.0)));

        // Background + border + rounded corners — matches the hover tooltip style.
        window.paint_quad(quad(
            menu_bounds,
            Corners::all(corner_radius),
            cx.theme().colors.popover,
            Edges::all(px(1.0)),
            cx.theme().colors.border,
            BorderStyle::default(),
        ));

        // Items
        let mut y = menu.origin.y + px(4.0);
        for (index, (label, is_separator, is_disabled)) in menu.items.iter().enumerate() {
            if *is_separator {
                let sep_y = y + px(4.0);
                window.paint_quad(fill(
                    Bounds::new(
                        point(menu.origin.x + px(6.0), sep_y),
                        size(menu_width - px(12.0), px(1.0)),
                    ),
                    cx.theme().colors.border,
                ));
                y += px(8.0);
                continue;
            }

            // Highlight the hovered/keyboard-selected item
            if menu.highlighted == Some(index) {
                window.paint_quad(fill(
                    Bounds::new(point(menu.origin.x, y), size(menu_width, item_height)),
                    cx.theme().colors.list_hover,
                ));
            }

            let text_color: Hsla = if *is_disabled {
                cx.theme().colors.muted_foreground
            } else {
                cx.theme().colors.popover_foreground
            };

            Self::paint_panel_text(
                label,
                point(
                    menu.origin.x + padding_x,
                    y + (item_height - font_size) / 2.0,
                ),
                font_size,
                text_color,
                window,
                cx,
            );

            y += item_height;
        }
    }
}

/// Line classification produced by markdown preprocessing.
enum TooltipLine {
    /// A line fully wrapped in `**...**` — render bold and bright.
    Title(String),
    /// A normal body line — may contain inline backtick code spans.
    Body(String),
    /// A collapsed vertical gap between content sections.
    Blank,
}

/// Parse hover documentation into typed lines.
///
/// Strips markdown structure (bold headings) and collapses consecutive blank
/// lines to a single gap so the tooltip stays compact regardless of how
/// verbose the LSP response is.
fn preprocess_hover_text(raw: &str) -> Vec<TooltipLine> {
    let mut result = Vec::new();
    let mut last_was_blank = true; // suppress leading blanks

    for raw_line in raw.trim_end_matches('\n').split('\n') {
        let line = raw_line.trim_end_matches('\r');

        if line.trim().is_empty() {
            if !last_was_blank {
                result.push(TooltipLine::Blank);
                last_was_blank = true;
            }
            continue;
        }

        last_was_blank = false;

        // Detect fully-bolded heading: a line that is exactly `**text**`.
        let trimmed = line.trim();
        if trimmed.starts_with("**") && trimmed.ends_with("**") && trimmed.len() > 4 {
            let inner = &trimmed[2..trimmed.len() - 2];
            if !inner.is_empty() && !inner.contains("**") {
                result.push(TooltipLine::Title(inner.to_string()));
                continue;
            }
        }

        result.push(TooltipLine::Body(line.to_string()));
    }

    // Remove trailing blank separator.
    while matches!(result.last(), Some(TooltipLine::Blank)) {
        result.pop();
    }

    result
}

/// Build display text and `TextRun` slices for a body line.
///
/// Backtick-delimited spans (`` `code` ``) are rendered in a distinct cyan
/// color to visually separate identifiers and keywords from prose.
fn build_body_runs(text: &str, body_color: Hsla, code_color: Hsla) -> (SharedString, Vec<TextRun>) {
    let mut display = String::new();
    let mut runs: Vec<TextRun> = Vec::new();
    let mut rest = text;

    while !rest.is_empty() {
        match rest.find('`') {
            Some(tick_pos) => {
                if tick_pos > 0 {
                    let pre = &rest[..tick_pos];
                    runs.push(TextRun {
                        len: pre.len(),
                        font: Font::default(),
                        color: body_color,
                        background_color: None,
                        underline: None,
                        strikethrough: None,
                    });
                    display.push_str(pre);
                }
                let after = &rest[tick_pos + 1..];
                match after.find('`') {
                    Some(close_pos) => {
                        let code = &after[..close_pos];
                        if !code.is_empty() {
                            runs.push(TextRun {
                                len: code.len(),
                                font: Font::default(),
                                color: code_color,
                                background_color: None,
                                underline: None,
                                strikethrough: None,
                            });
                            display.push_str(code);
                        }
                        rest = &after[close_pos + 1..];
                    }
                    None => {
                        // Unclosed backtick — treat rest as body text.
                        runs.push(TextRun {
                            len: rest.len(),
                            font: Font::default(),
                            color: body_color,
                            background_color: None,
                            underline: None,
                            strikethrough: None,
                        });
                        display.push_str(rest);
                        break;
                    }
                }
            }
            None => {
                runs.push(TextRun {
                    len: rest.len(),
                    font: Font::default(),
                    color: body_color,
                    background_color: None,
                    underline: None,
                    strikethrough: None,
                });
                display.push_str(rest);
                break;
            }
        }
    }

    if display.is_empty() {
        display.push(' ');
        runs.push(TextRun {
            len: 1,
            font: Font::default(),
            color: body_color,
            background_color: None,
            underline: None,
            strikethrough: None,
        });
    }

    (display.into(), runs)
}
