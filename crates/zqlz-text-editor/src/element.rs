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
use std::{cell::RefCell, collections::HashMap, ops::Range, sync::Arc};
use zqlz_ui::widgets::{ActiveTheme, ThemeColor, ThemeMode, highlighter::HighlightTheme};

use crate::{
    CachedScrollbarBounds, CursorShapeStyle, Selection, TextEditor, VisibleWrapLayout,
    buffer::Position, display_map::DisplayViewport, syntax::HighlightKind,
};

/// The width of the cursor in pixels
const CURSOR_WIDTH: Pixels = px(1.5);

/// Padding on each side inside the gutter (between gutter edge and line number text)
const GUTTER_PADDING: Pixels = px(8.0);

/// Width of the separator line between gutter and text content
const GUTTER_SEPARATOR_WIDTH: Pixels = px(1.0);

/// Dedicated horizontal zone reserved for fold chevrons, sitting between the
/// right edge of the line-number text and the separator. This prevents the
/// triangle from overlapping the digits regardless of how many digits are shown.
const FOLD_CHEVRON_ZONE: Pixels = px(14.0);

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
    /// Shaped ghost text ready to paint.
    shaped: Arc<ShapedLine>,
    /// Top-left corner at which to start painting the ghost text
    origin: Point<Pixels>,
    /// Line height (for vertical positioning)
    line_height: Pixels,
}

struct EditPredictionRenderData {
    shaped: Arc<ShapedLine>,
    origin: Point<Pixels>,
    line_height: Pixels,
}

/// Inlay hint render data prepared during prepaint.
struct InlayHintRenderData {
    /// Background chip bounds, if this hint should render as a pill.
    background_bounds: Option<Bounds<Pixels>>,
    /// Shaped label text.
    shaped: Arc<ShapedLine>,
    /// Top-left corner at which to paint the hint.
    origin: Point<Pixels>,
    /// Height used for vertical centering and paint.
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
    renderer_caches: RefCell<RendererCaches>,
}

#[derive(Default)]
struct RendererCaches {
    line_shapes: HashMap<LineShapeCacheKey, Arc<ShapedLine>>,
    wrapped_line_shapes: HashMap<WrappedLineShapeCacheKey, Arc<WrappedLine>>,
    viewport_layouts: HashMap<ViewportLayoutCacheKey, Arc<CachedViewportLayout>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ColorCacheKey {
    h: u32,
    s: u32,
    l: u32,
    a: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TextRunCacheKey {
    len: usize,
    font: Font,
    color: ColorCacheKey,
    background_color: Option<ColorCacheKey>,
    underline: Option<UnderlineStyle>,
    strikethrough: Option<StrikethroughStyle>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct LineShapeCacheKey {
    text: String,
    font_size_bits: u32,
    runs: Vec<TextRunCacheKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct WrappedLineShapeCacheKey {
    line: LineShapeCacheKey,
    wrap_width_bits: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ViewportTextStyleCacheKey {
    font: Font,
    default_text_color: Hsla,
    active_gutter_color: Hsla,
    inactive_gutter_color: Hsla,
    keyword_color: Hsla,
    string_color: Hsla,
    comment_color: Hsla,
    number_color: Hsla,
    identifier_color: Hsla,
    operator_color: Hsla,
    function_color: Hsla,
    punctuation_color: Hsla,
    boolean_color: Hsla,
    null_color: Hsla,
    error_color: Hsla,
}

impl From<Hsla> for ColorCacheKey {
    fn from(color: Hsla) -> Self {
        Self {
            h: color.h.to_bits(),
            s: color.s.to_bits(),
            l: color.l.to_bits(),
            a: color.a.to_bits(),
        }
    }
}

impl RendererCaches {
    fn line_key(text: &str, font_size: Pixels, runs: &[TextRun]) -> LineShapeCacheKey {
        LineShapeCacheKey {
            text: text.to_string(),
            font_size_bits: f32::from(font_size).to_bits(),
            runs: runs
                .iter()
                .map(|run| TextRunCacheKey {
                    len: run.len,
                    font: run.font.clone(),
                    color: run.color.into(),
                    background_color: run.background_color.map(Into::into),
                    underline: run.underline,
                    strikethrough: run.strikethrough,
                })
                .collect(),
        }
    }

    fn prune_if_needed(&mut self) {
        if self.line_shapes.len() > 4_096 {
            self.line_shapes.clear();
        }
        if self.wrapped_line_shapes.len() > 2_048 {
            self.wrapped_line_shapes.clear();
        }
        if self.viewport_layouts.len() > 128 {
            self.viewport_layouts.clear();
        }
    }
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

struct ViewportLayoutBuildParams<'a> {
    visible_range: Range<usize>,
    relative_line_numbers: bool,
    cursor_line: usize,
    font_size: Pixels,
    text_style: &'a ViewportTextStyleCacheKey,
    wrap_width: Option<Pixels>,
    wrap_column: usize,
    scroll_offset: f32,
    line_height: Pixels,
}

struct ViewportLayoutKeyInput {
    revision: usize,
    syntax_generation: u64,
    visible_rows: Range<usize>,
    relative_line_numbers: bool,
    cursor_line: usize,
    text_style: ViewportTextStyleCacheKey,
    scroll_offset: f32,
    line_height: Pixels,
    font_size: Pixels,
    gutter_width: Pixels,
    char_width: Pixels,
    bounds: Bounds<Pixels>,
    soft_wrap: bool,
}

struct WrappedPositionContext<'a> {
    wrap_layout: Option<&'a VisibleWrapLayout>,
    display_snapshot: &'a crate::DisplaySnapshot,
    bounds: Bounds<Pixels>,
    gutter_width: Pixels,
    char_width: Pixels,
    horizontal_scroll_offset: f32,
    scroll_offset: f32,
    line_height: Pixels,
}

struct CursorPixelLayout {
    line_height: Pixels,
    char_width: Pixels,
    horizontal_scroll_offset: f32,
    scroll_offset: f32,
    gutter_width: Pixels,
}

impl EditorElement {
    fn chunk_text_runs(
        chunk: &crate::display_map::DisplayTextChunk,
        text_style: &ViewportTextStyleCacheKey,
    ) -> Vec<TextRun> {
        if chunk.highlights.is_empty() || chunk.text.is_empty() {
            return vec![TextRun {
                len: chunk.text.len(),
                font: text_style.font.clone(),
                color: text_style.default_text_color,
                background_color: None,
                underline: None,
                strikethrough: None,
            }];
        }

        let mut runs = Vec::new();
        for highlight in &chunk.highlights {
            let run_start = chunk
                .text
                .floor_char_boundary(highlight.start.min(chunk.text.len()));
            let run_end = chunk
                .text
                .ceil_char_boundary(highlight.end.min(chunk.text.len()));

            if run_start < run_end {
                runs.push((run_start, run_end, highlight.kind));
            }
        }

        if runs.is_empty() {
            return vec![TextRun {
                len: chunk.text.len(),
                font: text_style.font.clone(),
                color: text_style.default_text_color,
                background_color: None,
                underline: None,
                strikethrough: None,
            }];
        }

        runs.sort_by(|left, right| left.0.cmp(&right.0));

        let mut result = Vec::new();
        let mut current_pos = 0;
        for (start, end, kind) in runs {
            if end <= current_pos {
                continue;
            }

            let start = start.max(current_pos);

            if start > current_pos {
                result.push(TextRun {
                    len: start - current_pos,
                    font: text_style.font.clone(),
                    color: text_style.default_text_color,
                    background_color: None,
                    underline: None,
                    strikethrough: None,
                });
            }

            result.push(TextRun {
                len: end - start,
                font: text_style.font.clone(),
                color: match kind {
                    HighlightKind::Keyword => text_style.keyword_color,
                    HighlightKind::String => text_style.string_color,
                    HighlightKind::Comment => text_style.comment_color,
                    HighlightKind::Number => text_style.number_color,
                    HighlightKind::Identifier => text_style.identifier_color,
                    HighlightKind::Operator => text_style.operator_color,
                    HighlightKind::Function => text_style.function_color,
                    HighlightKind::Punctuation => text_style.punctuation_color,
                    HighlightKind::Boolean => text_style.boolean_color,
                    HighlightKind::Null => text_style.null_color,
                    HighlightKind::Error => text_style.error_color,
                    HighlightKind::Default => text_style.default_text_color,
                },
                background_color: None,
                underline: None,
                strikethrough: None,
            });

            current_pos = end;
        }

        if current_pos < chunk.text.len() {
            result.push(TextRun {
                len: chunk.text.len() - current_pos,
                font: text_style.font.clone(),
                color: text_style.default_text_color,
                background_color: None,
                underline: None,
                strikethrough: None,
            });
        }

        result
    }

    fn build_viewport_layout_cache(
        &self,
        viewport: DisplayViewport,
        params: ViewportLayoutBuildParams<'_>,
        window: &mut Window,
    ) -> CachedViewportLayout {
        let text_chunks = viewport.text_chunks();
        let row_infos = viewport.row_infos();

        let ViewportLayoutBuildParams {
            visible_range,
            relative_line_numbers,
            cursor_line,
            font_size,
            text_style,
            wrap_width,
            wrap_column,
            scroll_offset,
            line_height,
        } = params;

        let (shaped_lines, wrapped_shaped_lines, wrap_layout) = if let Some(wrap_width) = wrap_width
        {
            let wrapped_shaped_lines = text_chunks
                .iter()
                .filter_map(|chunk| {
                    let text_runs = Self::chunk_text_runs(chunk, text_style);
                    self.shape_wrapped_line_cached(
                        &chunk.text,
                        font_size,
                        &text_runs,
                        wrap_width,
                        window,
                    )
                })
                .collect::<Vec<_>>();
            let visual_rows = wrapped_shaped_lines
                .iter()
                .map(|line| line.wrap_boundaries().len().saturating_add(1))
                .collect::<Vec<_>>();
            (
                Vec::new(),
                Some(wrapped_shaped_lines),
                Some(VisibleWrapLayout::new(
                    visible_range.clone(),
                    visual_rows,
                    wrap_column,
                    scroll_offset,
                    line_height,
                )),
            )
        } else {
            let shaped_lines = text_chunks
                .iter()
                .map(|chunk| {
                    let text_runs = Self::chunk_text_runs(chunk, text_style);
                    self.shape_line_cached(&chunk.text, font_size, &text_runs, window)
                })
                .collect::<Vec<_>>();
            (shaped_lines, None, None)
        };

        let gutter_lines = row_infos
            .iter()
            .map(|row_info| {
                let buffer_line = row_info.buffer_line;
                let line_number = if relative_line_numbers {
                    if buffer_line == cursor_line {
                        buffer_line + 1
                    } else {
                        buffer_line.abs_diff(cursor_line)
                    }
                } else {
                    buffer_line + 1
                };
                let label = line_number.to_string();
                let is_active = buffer_line == cursor_line;
                let has_diagnostics = text_chunks
                    .get(row_info.display_row.saturating_sub(visible_range.start))
                    .map(|chunk| !chunk.diagnostics.is_empty())
                    .unwrap_or(false);
                let color: Hsla = if is_active {
                    text_style.active_gutter_color
                } else {
                    text_style.inactive_gutter_color
                };
                let text_run = TextRun {
                    len: label.len(),
                    font: text_style.font.clone(),
                    color,
                    background_color: None,
                    underline: None,
                    strikethrough: None,
                };
                let shaped = self.shape_line_cached(&label, font_size, &[text_run], window);
                GutterLine {
                    shaped,
                    is_active,
                    has_diagnostics,
                }
            })
            .collect::<Vec<_>>();

        CachedViewportLayout {
            shaped_lines,
            wrapped_shaped_lines,
            gutter_lines,
            viewport,
            wrap_layout,
        }
    }

    fn viewport_layout_key(input: ViewportLayoutKeyInput) -> ViewportLayoutCacheKey {
        let ViewportLayoutKeyInput {
            revision,
            syntax_generation,
            visible_rows,
            relative_line_numbers,
            cursor_line,
            text_style,
            scroll_offset,
            line_height,
            font_size,
            gutter_width,
            char_width,
            bounds,
            soft_wrap,
        } = input;

        ViewportLayoutCacheKey {
            revision,
            syntax_generation,
            visible_rows,
            relative_line_numbers,
            cursor_line,
            text_style,
            scroll_offset_bits: scroll_offset.to_bits(),
            line_height_bits: f32::from(line_height).to_bits(),
            font_size_bits: f32::from(font_size).to_bits(),
            gutter_width_bits: f32::from(gutter_width).to_bits(),
            char_width_bits: f32::from(char_width).to_bits(),
            bounds_width_bits: f32::from(bounds.size.width).to_bits(),
            bounds_height_bits: f32::from(bounds.size.height).to_bits(),
            soft_wrap,
        }
    }

    fn viewport_text_style_key(font: &Font, cx: &App) -> ViewportTextStyleCacheKey {
        let theme = cx.theme();
        let highlight_theme = theme.highlight_theme.as_ref();

        ViewportTextStyleCacheKey {
            font: font.clone(),
            default_text_color: Self::editor_foreground_color(highlight_theme, &theme.colors),
            active_gutter_color: Self::gutter_line_number_color(
                highlight_theme,
                &theme.colors,
                true,
            ),
            inactive_gutter_color: Self::gutter_line_number_color(
                highlight_theme,
                &theme.colors,
                false,
            ),
            keyword_color: Self::highlight_color(HighlightKind::Keyword, cx),
            string_color: Self::highlight_color(HighlightKind::String, cx),
            comment_color: Self::highlight_color(HighlightKind::Comment, cx),
            number_color: Self::highlight_color(HighlightKind::Number, cx),
            identifier_color: Self::highlight_color(HighlightKind::Identifier, cx),
            operator_color: Self::highlight_color(HighlightKind::Operator, cx),
            function_color: Self::highlight_color(HighlightKind::Function, cx),
            punctuation_color: Self::highlight_color(HighlightKind::Punctuation, cx),
            boolean_color: Self::highlight_color(HighlightKind::Boolean, cx),
            null_color: Self::highlight_color(HighlightKind::Null, cx),
            error_color: Self::highlight_color(HighlightKind::Error, cx),
        }
    }

    fn line_y_for_slot(
        wrap_layout: Option<&VisibleWrapLayout>,
        display_slot: usize,
        scroll_offset: f32,
        line_height: Pixels,
    ) -> Pixels {
        wrap_layout
            .and_then(|layout| layout.line_y_offset_for_slot(display_slot))
            .unwrap_or(line_height * (display_slot as f32 - scroll_offset))
    }

    fn wrapped_position_origin(
        context: &WrappedPositionContext<'_>,
        position: Position,
        display_slot: usize,
    ) -> Point<Pixels> {
        let display_column = context
            .display_snapshot
            .display_column_for_position(position)
            .unwrap_or(position.column);
        let scroll_x = context.char_width * context.horizontal_scroll_offset.max(0.0);

        if let Some(wrap_layout) = context.wrap_layout
            && let Some(row_y) = wrap_layout.line_y_offset_for_slot(display_slot)
        {
            let wrap_column = wrap_layout.wrap_column().max(1);
            let visual_row = display_column / wrap_column;
            let visual_column = display_column % wrap_column;
            return point(
                context.bounds.origin.x
                    + context.gutter_width
                    + context.char_width * (visual_column as f32)
                    - scroll_x,
                context.bounds.origin.y + row_y + context.line_height * (visual_row as f32),
            );
        }

        point(
            context.bounds.origin.x
                + context.gutter_width
                + context.char_width * (display_column as f32)
                - scroll_x,
            context.bounds.origin.y
                + context.line_height * (display_slot as f32 - context.scroll_offset),
        )
    }

    #[cfg(test)]
    pub(crate) fn test_visible_range(
        total_lines: usize,
        line_height: Pixels,
        viewport_height: Pixels,
        scroll_offset: f32,
    ) -> Range<usize> {
        let visible_lines = (viewport_height / line_height).ceil() as usize;
        let start_line = (scroll_offset.floor() as usize).min(total_lines.saturating_sub(1));
        let end_line = (start_line + visible_lines).min(total_lines);
        start_line..end_line
    }

    #[cfg(test)]
    pub(crate) fn test_cursor_pixel_position(
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

    /// Resolve the display color for a syntax highlight kind from the active theme.
    ///
    /// Each kind maps to a named slot in `SyntaxColors` (via `HighlightTheme`).
    /// When a theme doesn't define a slot, we fall back to a semantic color from
    /// the main theme palette (e.g. `primary` for keywords, `danger` for errors).
    fn highlight_color(kind: HighlightKind, cx: &App) -> Hsla {
        let theme = cx.theme();
        let fallback_theme = match theme.mode {
            ThemeMode::Dark => HighlightTheme::default_dark(),
            ThemeMode::Light => HighlightTheme::default_light(),
        };
        Self::resolve_highlight_color(
            kind,
            theme.highlight_theme.as_ref(),
            fallback_theme.as_ref(),
            &theme.colors,
            Self::editor_foreground_color(theme.highlight_theme.as_ref(), &theme.colors),
        )
    }

    fn editor_background_color(active_theme: &HighlightTheme, colors: &ThemeColor) -> Hsla {
        active_theme
            .style
            .editor_background
            .unwrap_or(colors.background)
    }

    fn editor_foreground_color(active_theme: &HighlightTheme, colors: &ThemeColor) -> Hsla {
        active_theme
            .style
            .editor_foreground
            .unwrap_or(colors.foreground)
    }

    fn gutter_line_number_color(
        active_theme: &HighlightTheme,
        colors: &ThemeColor,
        active: bool,
    ) -> Hsla {
        if active {
            active_theme
                .style
                .editor_active_line_number
                .unwrap_or_else(|| Self::editor_foreground_color(active_theme, colors))
        } else {
            active_theme
                .style
                .editor_line_number
                .unwrap_or(colors.muted_foreground)
        }
    }

    fn active_line_background_color(active_theme: &HighlightTheme, colors: &ThemeColor) -> Hsla {
        active_theme
            .style
            .editor_active_line
            .unwrap_or(colors.list_hover)
    }

    fn default_syntax_palette_color(kind: HighlightKind, colors: &ThemeColor) -> Hsla {
        match kind {
            HighlightKind::Keyword => colors.blue,
            HighlightKind::String => colors.green,
            HighlightKind::Comment => colors.muted_foreground,
            HighlightKind::Number => colors.green,
            HighlightKind::Identifier => colors.foreground,
            HighlightKind::Operator => colors.foreground,
            HighlightKind::Function => colors.cyan,
            HighlightKind::Punctuation => colors.foreground,
            HighlightKind::Boolean => colors.green,
            HighlightKind::Null => colors.magenta,
            HighlightKind::Error => colors.danger,
            HighlightKind::Default => colors.foreground,
        }
    }

    fn resolve_highlight_color(
        kind: HighlightKind,
        active_theme: &HighlightTheme,
        fallback_theme: &HighlightTheme,
        colors: &ThemeColor,
        default_text_color: Hsla,
    ) -> Hsla {
        let active_color = Self::syntax_theme_color(kind, active_theme);
        let fallback_color = Self::syntax_theme_color(kind, fallback_theme);
        let default_palette_color = Self::default_syntax_palette_color(kind, colors);

        active_color
            .filter(|color| *color != default_text_color)
            .or(fallback_color.filter(|color| *color != default_text_color))
            .or(active_color)
            .or(fallback_color)
            .unwrap_or_else(|| {
                Self::generic_highlight_fallback_color(
                    kind,
                    colors,
                    default_text_color,
                    default_palette_color,
                )
            })
    }

    fn syntax_theme_color(kind: HighlightKind, theme: &HighlightTheme) -> Option<Hsla> {
        match kind {
            HighlightKind::Keyword => theme.keyword.and_then(|style| style.color),
            HighlightKind::String => theme.string.and_then(|style| style.color),
            HighlightKind::Comment => theme.comment.and_then(|style| style.color),
            HighlightKind::Number => theme.number.and_then(|style| style.color),
            HighlightKind::Identifier => theme.variable.and_then(|style| style.color),
            HighlightKind::Operator => theme.operator.and_then(|style| style.color),
            HighlightKind::Function => theme.function.and_then(|style| style.color),
            HighlightKind::Punctuation => theme.punctuation.and_then(|style| style.color),
            HighlightKind::Boolean => theme
                .boolean
                .and_then(|style| style.color)
                .or_else(|| theme.keyword.and_then(|style| style.color)),
            HighlightKind::Null => theme.keyword.and_then(|style| style.color),
            HighlightKind::Error => None,
            HighlightKind::Default => None,
        }
    }

    fn generic_highlight_fallback_color(
        kind: HighlightKind,
        colors: &ThemeColor,
        default_text_color: Hsla,
        default_palette_color: Hsla,
    ) -> Hsla {
        match kind {
            HighlightKind::Identifier
            | HighlightKind::Operator
            | HighlightKind::Punctuation
            | HighlightKind::Default => default_text_color,
            HighlightKind::Keyword
            | HighlightKind::String
            | HighlightKind::Comment
            | HighlightKind::Number
            | HighlightKind::Function
            | HighlightKind::Boolean
            | HighlightKind::Null => default_palette_color,
            HighlightKind::Error => colors.danger,
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
        show_line_numbers: bool,
        show_folding: bool,
    ) -> Pixels {
        if !show_line_numbers && !show_folding {
            return px(0.0);
        }

        let line_number_width = if show_line_numbers {
            let max_line_number = total_lines.max(1);
            let digit_count = max_line_number.ilog10() as usize + 1;
            let sample: String = "9".repeat(digit_count);
            let text_run = TextRun {
                len: sample.len(),
                font: Font::default(),
                color: gpui::white(),
                background_color: None,
                underline: None,
                strikethrough: None,
            };
            let shaped =
                window
                    .text_system()
                    .shape_line(sample.into(), font_size, &[text_run], None);

            shaped.width + GUTTER_PADDING * 2.0
        } else {
            px(0.0)
        };

        let fold_width = if show_folding {
            FOLD_CHEVRON_ZONE
        } else {
            px(0.0)
        };
        let separator_width = if show_line_numbers || show_folding {
            GUTTER_SEPARATOR_WIDTH
        } else {
            px(0.0)
        };

        line_number_width + fold_width + separator_width
    }
}

impl EditorElement {
    pub fn new(editor: Entity<TextEditor>) -> Self {
        Self {
            editor,
            renderer_caches: RefCell::new(RendererCaches::default()),
        }
    }

    fn shape_line_cached(
        &self,
        text: &str,
        font_size: Pixels,
        runs: &[TextRun],
        window: &mut Window,
    ) -> Arc<ShapedLine> {
        let key = RendererCaches::line_key(text, font_size, runs);
        if let Some(cached) = self.renderer_caches.borrow().line_shapes.get(&key).cloned() {
            return cached;
        }

        let shaped = Arc::new(window.text_system().shape_line(
            text.to_string().into(),
            font_size,
            runs,
            None,
        ));
        let mut caches = self.renderer_caches.borrow_mut();
        caches.prune_if_needed();
        caches.line_shapes.insert(key, shaped.clone());
        shaped
    }

    fn shape_wrapped_line_cached(
        &self,
        text: &str,
        font_size: Pixels,
        runs: &[TextRun],
        wrap_width: Pixels,
        window: &mut Window,
    ) -> Option<Arc<WrappedLine>> {
        let key = WrappedLineShapeCacheKey {
            line: RendererCaches::line_key(text, font_size, runs),
            wrap_width_bits: f32::from(wrap_width).to_bits(),
        };
        if let Some(cached) = self
            .renderer_caches
            .borrow()
            .wrapped_line_shapes
            .get(&key)
            .cloned()
        {
            return Some(cached);
        }

        let wrapped = window
            .text_system()
            .shape_text(
                text.to_string().into(),
                font_size,
                runs,
                Some(wrap_width),
                None,
            )
            .ok()?
            .pop()
            .map(Arc::new)?;
        let mut caches = self.renderer_caches.borrow_mut();
        caches.prune_if_needed();
        caches.wrapped_line_shapes.insert(key, wrapped.clone());
        Some(wrapped)
    }

    fn paint_cursor_shape(
        bounds: Bounds<Pixels>,
        shape: CursorShapeStyle,
        window: &mut Window,
        cx: &mut App,
    ) {
        match shape {
            CursorShapeStyle::Line => {
                window.paint_quad(fill(bounds, cx.theme().colors.caret));
            }
            CursorShapeStyle::Block => {
                window.paint_quad(fill(bounds, cx.theme().colors.caret.opacity(0.5)));
            }
            CursorShapeStyle::Underline => {
                let underline_bounds = Bounds::new(
                    point(
                        bounds.origin.x,
                        bounds.origin.y + bounds.size.height - px(2.0),
                    ),
                    size(bounds.size.width.max(px(8.0)), px(2.0)),
                );
                window.paint_quad(fill(underline_bounds, cx.theme().colors.caret));
            }
        }
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
        layout: CursorPixelLayout,
        display_slot: usize,
    ) -> Point<Pixels> {
        point(
            layout.gutter_width + layout.char_width * (cursor_pos.column as f32)
                - layout.char_width * layout.horizontal_scroll_offset.max(0.0),
            layout.line_height * (display_slot as f32 - layout.scroll_offset),
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
    shaped_lines: Vec<Arc<ShapedLine>>,
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
    /// Signature-help tooltip data (if explicitly requested)
    signature_help_tooltip: Option<SignatureHelpTooltipData>,
    /// Find/replace panel data (if the panel is open)
    find_panel: Option<FindPanelData>,
    // ── Gutter (Phase 6) ──────────────────────────────────────────────────────
    /// Pixel width of the line-number gutter (includes padding and separator)
    gutter_width: Pixels,
    /// Whether line numbers are enabled for this frame.
    show_line_numbers: bool,
    /// Whether fold chevrons are enabled for this frame.
    show_folding: bool,
    /// Whether current-line highlighting is enabled for this frame.
    highlight_current_line: bool,
    /// Whether gutter diagnostics should be painted.
    show_gutter_diagnostics: bool,
    /// Cursor style for the current frame.
    cursor_shape: CursorShapeStyle,
    /// Whether the cursor should be painted as solid.
    cursor_blink_enabled: bool,
    /// Whether selection fills should be rounded.
    rounded_selection: bool,
    /// Whether non-primary reference highlights should be rendered.
    selection_highlight_enabled: bool,
    /// Whether the blinking caret is currently visible.
    cursor_visible: bool,
    /// Pre-shaped line number labels for each visible line
    gutter_lines: Vec<GutterLine>,
    /// Ghost-text inline suggestion to paint after the cursor (if any)
    inline_suggestion: Option<InlineSuggestionRenderData>,
    edit_prediction: Option<EditPredictionRenderData>,
    /// Inlay hints visible in the viewport.
    inlay_hints: Vec<InlayHintRenderData>,
    /// Go-to-line overlay dialog (if open)
    goto_line_panel: Option<GoToLinePanelData>,
    /// Indent guide X-coordinates (one per indent level) for visible lines
    indent_guides: Vec<IndentGuideData>,
    /// Pre-calculated scrollbar geometry (None when content fits in viewport)
    scrollbar: Option<ScrollbarData>,
    /// When soft_wrap is on, shaped lines stored as WrappedLine (with wrap boundaries).
    /// When None, the `shaped_lines` Vec is used instead.
    wrapped_shaped_lines: Option<Vec<Arc<gpui::WrappedLine>>>,
    /// Y-offset (from bounds.origin.y) for each shaped/wrapped line entry.
    /// Accounts for scroll offset and any extra height from wrapped lines.
    line_y_offsets: Vec<Pixels>,
    horizontal_scroll_pixels: Pixels,
    /// Pixel rectangles for each visible reference highlight (feat-047)
    reference_highlights: Vec<ReferenceHighlightData>,
    /// Right-click context menu render data (feat-045)
    context_menu: Option<ContextMenuRenderData>,
    /// Sticky structural header pinned to the top of the viewport.
    sticky_header: Option<StickyHeaderRenderData>,
    minimap: Option<MinimapRenderData>,
    inline_code_actions: Vec<InlineCodeActionRenderData>,
    block_widgets: Vec<BlockWidgetRenderData>,
    /// Fold chevron hit-rects and state for each visible foldable line.
    fold_chevrons: Vec<FoldChevronData>,
}

struct HoverTooltipData {
    documentation: String,
    anchor_bounds: Bounds<Pixels>,
}

/// Signature-help overlay data for rendering near the invocation site.
struct SignatureHelpTooltipData {
    content: String,
    anchor_bounds: Bounds<Pixels>,
}

/// Data the renderer needs to paint find/replace match highlights.
struct FindPanelData {
    /// Pixel rectangles for every match in the visible viewport, paired with a flag
    /// indicating whether that match is the currently-selected (primary) match.
    match_rects: Vec<(Bounds<Pixels>, bool)>,
}

/// A pre-shaped line number label for one visible line in the gutter.
#[derive(Clone)]
struct GutterLine {
    /// The shaped text ready to paint
    shaped: Arc<ShapedLine>,
    /// Whether this is the cursor (active) line, drawn with a brighter color
    is_active: bool,
    /// Whether this line has any diagnostics to mark in the gutter.
    has_diagnostics: bool,
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

struct StickyHeaderRenderData {
    shaped: Arc<ShapedLine>,
    bounds: Bounds<Pixels>,
}

struct MinimapRenderData {
    bounds: Bounds<Pixels>,
    viewport_bounds: Bounds<Pixels>,
}

struct InlineCodeActionRenderData {
    shaped: Arc<ShapedLine>,
    bounds: Bounds<Pixels>,
}

struct BlockWidgetRenderData {
    shaped: Arc<ShapedLine>,
    bounds: Bounds<Pixels>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ViewportLayoutCacheKey {
    revision: usize,
    syntax_generation: u64,
    visible_rows: Range<usize>,
    relative_line_numbers: bool,
    cursor_line: usize,
    text_style: ViewportTextStyleCacheKey,
    scroll_offset_bits: u32,
    line_height_bits: u32,
    font_size_bits: u32,
    gutter_width_bits: u32,
    char_width_bits: u32,
    bounds_width_bits: u32,
    bounds_height_bits: u32,
    soft_wrap: bool,
}

#[derive(Clone)]
struct CachedViewportLayout {
    shaped_lines: Vec<Arc<ShapedLine>>,
    wrapped_shaped_lines: Option<Vec<Arc<gpui::WrappedLine>>>,
    gutter_lines: Vec<GutterLine>,
    viewport: DisplayViewport,
    wrap_layout: Option<VisibleWrapLayout>,
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

/// Data for rendering the right-click context menu (feat-045).
struct ContextMenuRenderData {
    /// Ordered items to display (separators have `is_separator = true`)
    items: Vec<(String, bool, bool)>, // (label, is_separator, is_disabled)
    /// Pixel bounds after clamping into the editor viewport.
    bounds: Bounds<Pixels>,
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

        let editor_snapshot = {
            let editor = self.editor.read(cx);
            editor.editor_snapshot()
        };
        let document_snapshot = editor_snapshot.document.clone();
        let buffer = document_snapshot.buffer.clone();
        let cursor_pos = editor_snapshot
            .cursor()
            .map(|cursor| cursor.position())
            .unwrap_or_else(Position::zero);
        let scroll_offset = editor_snapshot.scroll_offset;
        let total_lines = buffer.line_count();
        let diagnostics = document_snapshot
            .display_snapshot
            .diagnostics()
            .iter()
            .filter_map(|diagnostic| {
                let range = buffer.resolve_anchored_range(diagnostic.range).ok()?;
                Some(crate::syntax::Highlight {
                    start: range.start,
                    end: range.end,
                    kind: diagnostic.kind,
                })
            })
            .collect::<Vec<_>>();
        let hover_state = document_snapshot.hover_state.clone();
        let signature_help_state = document_snapshot.signature_help_state.clone();
        let bracket_pairs_snapshot = editor_snapshot.bracket_pairs.clone();

        let (has_selection, sel_start, sel_end, block_ranges) = if editor_snapshot
            .selection()
            .map(|selection| selection.has_selection())
            .unwrap_or(false)
        {
            let selection = editor_snapshot
                .selection()
                .cloned()
                .unwrap_or_else(Selection::new);
            let sel_range = selection.range();
            let block = selection.block_ranges();
            (true, sel_range.start, sel_range.end, block)
        } else {
            (false, Position::new(0, 0), Position::new(0, 0), Vec::new())
        };

        let extra_cursors_snapshot: Vec<(
            crate::buffer::Position,
            Option<(crate::buffer::Position, crate::buffer::Position)>,
        )> = editor_snapshot
            .extra_cursors()
            .into_iter()
            .map(|(cursor, selection)| {
                let sel_range = if selection.has_selection() {
                    Some((selection.range().start, selection.range().end))
                } else {
                    None
                };
                (cursor.position(), sel_range)
            })
            .collect();

        let completion_menu_cursor_pos =
            editor_snapshot.completion_menu.as_ref().map(|_| cursor_pos);
        let inline_suggestion_snapshot = document_snapshot.inline_suggestion.clone();
        let edit_prediction_snapshot = document_snapshot.edit_prediction.clone();
        let find_info = editor_snapshot
            .find_info
            .as_ref()
            .map(|snapshot| (snapshot.matches.clone(), snapshot.current_match));
        let goto_line_info = editor_snapshot.goto_line_info.as_ref().map(|snapshot| {
            (
                snapshot.query.clone(),
                snapshot.is_valid,
                snapshot.total_lines,
            )
        });
        let soft_wrap = document_snapshot.soft_wrap;
        let show_line_numbers = editor_snapshot.show_line_numbers;
        let show_folding = editor_snapshot.show_folding;
        let highlight_current_line = editor_snapshot.highlight_current_line;
        let relative_line_numbers = editor_snapshot.relative_line_numbers;
        let show_gutter_diagnostics = editor_snapshot.show_gutter_diagnostics;
        let horizontal_scroll_offset = editor_snapshot.horizontal_scroll_offset;
        let cursor_shape = editor_snapshot.cursor_shape;
        let cursor_blink_enabled = editor_snapshot.cursor_blink_enabled;
        let cursor_visible = editor_snapshot.cursor_visible;
        let rounded_selection = editor_snapshot.rounded_selection;
        let selection_highlight_enabled = editor_snapshot.selection_highlight_enabled;
        let reference_ranges_snapshot = if !document_snapshot
            .large_file_policy
            .reference_highlights_enabled
        {
            Vec::new()
        } else {
            document_snapshot.reference_ranges().to_vec()
        };
        let context_menu_snapshot = editor_snapshot.context_menu.as_ref().map(|snapshot| {
            (
                snapshot.items.clone(),
                snapshot.origin_x,
                snapshot.origin_y,
                snapshot.highlighted,
            )
        });

        // Update viewport lines for auto-scroll (after read lock is released)
        self.editor.update(cx, |editor, _cx| {
            editor.update_viewport_lines(viewport_lines);
        });

        // Obtain the display-line list via an Arc clone — O(1) regardless of
        // file size. The Vec is rebuilt in TextEditor only when the buffer or
        // fold state changes.
        let fold_snapshot = document_snapshot.display_snapshot.fold_snapshot();
        let display_lines = fold_snapshot.display_lines();
        let display_line_count = display_lines.len();

        // Calculate visible range (in display-slot space, not buffer-line space)
        let visible_range = self.calculate_visible_range(
            display_line_count,
            line_height,
            bounds.size.height,
            scroll_offset,
        );

        let visible_display_slot = |buffer_line: usize| {
            document_snapshot
                .display_snapshot
                .display_slot_for_buffer_line_in_rows(buffer_line, &visible_range)
        };

        // Calculate character width (use monospace assumption) — needed before shaping
        // loop to compute wrap column for soft-wrap.
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
        let gutter_width = Self::calculate_gutter_width(
            total_lines,
            font_size,
            window,
            show_line_numbers,
            show_folding,
        );

        // Soft-wrap: compute the available text area width and wrap_width for shape_text
        let wrap_width: Option<Pixels> = if soft_wrap {
            let text_area = bounds.size.width - gutter_width;
            if f32::from(text_area) > f32::from(char_width) {
                Some(text_area)
            } else {
                None
            }
        } else {
            None
        };

        let wrap_snapshot = document_snapshot.display_snapshot.wrap_snapshot();
        let wrap_layout =
            wrap_snapshot.layout_for_rows(visible_range.clone(), scroll_offset, line_height);
        let viewport = document_snapshot
            .display_snapshot
            .viewport(visible_range.clone());
        let viewport_text_style = Self::viewport_text_style_key(&font, cx);
        let viewport_layout_key = Self::viewport_layout_key(ViewportLayoutKeyInput {
            revision: document_snapshot.revision(),
            syntax_generation: document_snapshot.language.syntax_generation,
            visible_rows: visible_range.clone(),
            relative_line_numbers,
            cursor_line: cursor_pos.line,
            text_style: viewport_text_style.clone(),
            scroll_offset,
            line_height,
            font_size,
            gutter_width,
            char_width,
            bounds,
            soft_wrap,
        });
        let cached_viewport_layout = if let Some(cached) = self
            .renderer_caches
            .borrow()
            .viewport_layouts
            .get(&viewport_layout_key)
            .cloned()
        {
            cached
        } else {
            let layout = Arc::new(self.build_viewport_layout_cache(
                viewport,
                ViewportLayoutBuildParams {
                    visible_range: visible_range.clone(),
                    relative_line_numbers,
                    cursor_line: cursor_pos.line,
                    font_size,
                    text_style: &viewport_text_style,
                    wrap_width,
                    wrap_column: wrap_layout.wrap_column(),
                    scroll_offset,
                    line_height,
                },
                window,
            ));
            let mut caches = self.renderer_caches.borrow_mut();
            caches.prune_if_needed();
            caches
                .viewport_layouts
                .insert(viewport_layout_key, layout.clone());
            layout
        };
        let visible_chunks = cached_viewport_layout.viewport.text_chunks();
        tracing::trace!(
            visible_chunk_count = visible_chunks.len(),
            total_visible_highlights = visible_chunks
                .iter()
                .map(|chunk| chunk.highlights.len())
                .sum::<usize>(),
            syntax_generation = document_snapshot.language.syntax_generation,
            "Preparing editor element prepaint"
        );
        let effective_wrap_layout = cached_viewport_layout
            .wrap_layout
            .clone()
            .unwrap_or_else(|| wrap_layout.clone());
        let line_y_offsets = effective_wrap_layout.line_y_offsets();
        let wrap_layout = effective_wrap_layout.clone();
        let horizontal_scroll_pixels = char_width * horizontal_scroll_offset.max(0.0);
        let wrapped_position_context = WrappedPositionContext {
            wrap_layout: soft_wrap.then_some(&wrap_layout),
            display_snapshot: &document_snapshot.display_snapshot,
            bounds,
            gutter_width,
            char_width,
            horizontal_scroll_offset,
            scroll_offset,
            line_height,
        };
        let zero_origin_wrapped_position_context = WrappedPositionContext {
            wrap_layout: soft_wrap.then_some(&wrap_layout),
            display_snapshot: &document_snapshot.display_snapshot,
            bounds: Bounds::new(point(px(0.0), px(0.0)), bounds.size),
            gutter_width,
            char_width,
            horizontal_scroll_offset,
            scroll_offset,
            line_height,
        };
        let cursor_pixel_layout = CursorPixelLayout {
            line_height,
            char_width,
            horizontal_scroll_offset,
            scroll_offset,
            gutter_width,
        };

        // Push the gutter width and the element's bounds origin back to the
        // editor so that mouse handlers can correctly convert pixel positions.
        self.editor.update(cx, |editor, _cx| {
            editor.update_cached_layout(crate::CachedEditorLayout {
                gutter_width: f32::from(gutter_width),
                char_width,
                bounds_origin: bounds.origin,
                bounds_size: bounds.size,
                line_height,
                wrap_layout: soft_wrap.then_some(effective_wrap_layout.clone()),
            });
            editor.refresh_display_layout_settings();
        });

        let shaped_lines = cached_viewport_layout.shaped_lines.clone();
        let wrapped_shaped_lines = cached_viewport_layout.wrapped_shaped_lines.clone();
        let gutter_lines = cached_viewport_layout.gutter_lines.clone();

        if soft_wrap {
            self.editor.update(cx, |editor, _cx| {
                editor.update_visible_wrap_layout(
                    effective_wrap_layout.wrap_column(),
                    visible_range.clone(),
                    effective_wrap_layout.visual_rows().as_ref(),
                );
            });
        }
        // ─────────────────────────────────────────────────────────────────────

        // Calculate selection rectangles (offset by gutter_width into text area)
        // Helper to compute y-offset for a given display slot, accounting for soft-wrap
        let slot_y = |display_slot: usize| -> Pixels {
            Self::line_y_for_slot(
                soft_wrap.then_some(&wrap_layout),
                display_slot,
                scroll_offset,
                line_height,
            )
        };

        // ── Fold chevrons ─────────────────────────────────────────────────────
        // Snapshot fold state under a read lock so we don't hold the borrow
        // while computing geometry below.
        let fold_regions_snap = fold_snapshot.fold_regions().to_vec();
        let folded_lines_snap = fold_snapshot.folded_lines().clone();

        let chevron_size = line_height * 0.48;
        // Center the chevron within the dedicated FOLD_CHEVRON_ZONE that sits between
        // the line-number text and the separator — no overlap with digits possible.
        let chevron_zone_origin =
            bounds.origin.x + gutter_width - GUTTER_SEPARATOR_WIDTH - FOLD_CHEVRON_ZONE;
        let chevron_x = chevron_zone_origin + (FOLD_CHEVRON_ZONE - chevron_size) / 2.0;

        let mut fold_chevrons: Vec<FoldChevronData> = Vec::new();
        for display_slot in visible_range.clone() {
            let buffer_line = display_lines[display_slot];
            if let Some(region) = fold_regions_snap
                .iter()
                .find(|r| r.start_line == buffer_line)
            {
                let is_folded = folded_lines_snap.contains(&buffer_line);
                let line_y = bounds.origin.y + slot_y(display_slot);
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

        let mut selection_rects = Vec::new();
        if has_selection {
            if !block_ranges.is_empty() {
                // Block (rectangular/column) selection: each entry is (line, left_col, right_col)
                for (line_idx, start_col, end_col) in &block_ranges {
                    let Some(display_slot) = visible_display_slot(*line_idx) else {
                        continue;
                    };
                    if start_col < end_col {
                        selection_rects.push(Bounds::new(
                            point(
                                bounds.origin.x + gutter_width + char_width * (*start_col as f32)
                                    - horizontal_scroll_pixels,
                                bounds.origin.y + slot_y(display_slot),
                            ),
                            size(char_width * ((*end_col - *start_col) as f32), line_height),
                        ));
                    }
                }
            } else {
                for line_idx in sel_start.line..=sel_end.line {
                    let Some(display_slot) = visible_display_slot(line_idx) else {
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
                                bounds.origin.x + gutter_width + char_width * (start_col as f32)
                                    - horizontal_scroll_pixels,
                                bounds.origin.y + slot_y(display_slot),
                            ),
                            size(char_width * ((end_col - start_col) as f32), line_height),
                        ));
                    }
                }
            }
        }

        // Calculate diagnostic (error) bounds for squiggles.
        // Each diagnostic is broken into per-line segments so that start_col and end_col are
        // always on the same buffer line, preventing unsigned underflow when a diagnostic spans
        // multiple lines and the end column is smaller than the start column.
        let mut diagnostic_bounds = Vec::new();
        for highlight in &diagnostics {
            if let Ok(start_pos) = buffer.offset_to_position(highlight.start)
                && let Ok(end_pos) = buffer.offset_to_position(highlight.end)
            {
                let start_line = start_pos.line;
                let end_line = end_pos.line;
                for line_idx in start_line..=end_line {
                    if visible_display_slot(line_idx).is_none() {
                        continue;
                    }
                    let line_len = buffer.line(line_idx).map(|l| l.len()).unwrap_or(0);
                    let start_col = if line_idx == start_line {
                        start_pos.column
                    } else {
                        0
                    };
                    let end_col = if line_idx == end_line {
                        end_pos.column
                    } else {
                        line_len
                    };
                    if start_col < end_col {
                        diagnostic_bounds.push((highlight.clone(), line_idx, start_col, end_col));
                    }
                }
            }
        }

        // Calculate cursor bounds — only when the cursor's buffer line is visible.
        let cursor_height = line_height * 0.85; // 85% of line height, centered
        let cursor_bounds = visible_display_slot(cursor_pos.line).map(|slot| {
            let cursor_pixel_pos = Self::wrapped_position_origin(
                &zero_origin_wrapped_position_context,
                cursor_pos,
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
            if let Some(extra_slot) = visible_display_slot(extra_pos.line) {
                let extra_pixel_pos = Self::wrapped_position_origin(
                    &zero_origin_wrapped_position_context,
                    *extra_pos,
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
                    let Some(slot) = visible_display_slot(line_idx) else {
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
                        let line_y = slot_y(slot);
                        extra_selection_rects.push(Bounds::new(
                            point(
                                bounds.origin.x + gutter_width + char_width * (start_col as f32)
                                    - horizontal_scroll_pixels,
                                bounds.origin.y + line_y,
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
                    let slot = visible_display_slot(pos.line)?;
                    let y = slot_y(slot);
                    Some(Bounds::new(
                        point(
                            bounds.origin.x + gutter_width + char_width * (pos.column as f32)
                                - horizontal_scroll_pixels,
                            bounds.origin.y + y,
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
            if let Some(menu) = editor_snapshot.completion_menu.clone() {
                let menu_slot =
                    visible_display_slot(menu_cursor_pos.line).unwrap_or(menu_cursor_pos.line);
                let cursor_pixel_pos =
                    self.cursor_pixel_position(menu_cursor_pos, cursor_pixel_layout, menu_slot);
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
            if let Some(display_slot) = visible_display_slot(line_idx) {
                let start_position = Position::new(line_idx, start_col);
                let end_position = Position::new(line_idx, end_col);
                let origin = Self::wrapped_position_origin(
                    &wrapped_position_context,
                    start_position,
                    display_slot,
                );
                let width_columns = document_snapshot
                    .display_snapshot
                    .display_column_for_position(end_position)
                    .unwrap_or(end_col)
                    .saturating_sub(
                        document_snapshot
                            .display_snapshot
                            .display_column_for_position(start_position)
                            .unwrap_or(start_col),
                    )
                    .max(1);
                let rect = Bounds::new(
                    origin,
                    size(char_width * (width_columns as f32), line_height),
                );
                diag_bounds.push((highlight, rect));
            }
        }

        let hover_tooltip = if let Some(ref hover) = hover_state {
            if let (Ok(start_pos), Ok(end_pos)) = (
                buffer.offset_to_position(hover.range.start),
                buffer.offset_to_position(hover.range.end),
            ) {
                if let Some(display_slot) = visible_display_slot(start_pos.line) {
                    let origin = Self::wrapped_position_origin(
                        &wrapped_position_context,
                        start_pos,
                        display_slot,
                    );
                    let width_columns = document_snapshot
                        .display_snapshot
                        .display_column_for_position(end_pos)
                        .unwrap_or(end_pos.column)
                        .saturating_sub(
                            document_snapshot
                                .display_snapshot
                                .display_column_for_position(start_pos)
                                .unwrap_or(start_pos.column),
                        )
                        .max(1);
                    let anchor_bounds = Bounds::new(
                        origin,
                        size(char_width * (width_columns as f32), line_height),
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
        };

        let signature_help_tooltip = if let Some(signature_help) = signature_help_state {
            if let Ok(anchor_pos) = buffer.resolve_anchor_position(signature_help.anchor) {
                if let Some(display_slot) = visible_display_slot(anchor_pos.line) {
                    let origin = Self::wrapped_position_origin(
                        &wrapped_position_context,
                        anchor_pos,
                        display_slot,
                    );
                    let anchor_bounds =
                        Bounds::new(origin, size(char_width.max(px(1.0)), line_height));
                    Some(SignatureHelpTooltipData {
                        content: signature_help.content,
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
        };

        // Build find match highlight render data if the panel is open
        let find_panel = find_info.map(|(matches, current_match)| {
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
                    if start_pos.line == end_pos.line
                        && let Some(display_slot) = visible_display_slot(start_pos.line)
                    {
                        let origin = Self::wrapped_position_origin(
                            &wrapped_position_context,
                            start_pos,
                            display_slot,
                        );
                        let width_columns = document_snapshot
                            .display_snapshot
                            .display_column_for_position(end_pos)
                            .unwrap_or(end_pos.column)
                            .saturating_sub(
                                document_snapshot
                                    .display_snapshot
                                    .display_column_for_position(start_pos)
                                    .unwrap_or(start_pos.column),
                            )
                            .max(1);
                        let rect = Bounds::new(
                            origin,
                            size(char_width * (width_columns as f32), line_height),
                        );
                        match_rects.push((rect, is_current));
                    }
                }
            }

            FindPanelData { match_rects }
        });

        // Compute inline suggestion render data — the ghost text is painted right
        // after the real text on the cursor's line, at the cursor X position.
        let inline_suggestion = inline_suggestion_snapshot.and_then(|suggestion| {
            // Resolve the anchor offset to a line/column so we can compute the pixel origin.
            let Ok(anchor_pos) = buffer.resolve_anchor_position(suggestion.anchor) else {
                return None;
            };
            // Only paint when the anchor line is visible (not folded) in the viewport.
            let display_slot = visible_display_slot(anchor_pos.line)?;
            let origin =
                Self::wrapped_position_origin(&wrapped_position_context, anchor_pos, display_slot);
            let display_text = suggestion.text.lines().next().unwrap_or("");
            if display_text.is_empty() {
                return None;
            }
            let shaped = self.shape_line_cached(
                display_text,
                line_height * 0.9,
                &[TextRun {
                    len: display_text.len(),
                    font: Font::default(),
                    color: cx.theme().colors.muted_foreground.opacity(0.5),
                    background_color: None,
                    underline: None,
                    strikethrough: None,
                }],
                window,
            );
            Some(InlineSuggestionRenderData {
                shaped,
                origin,
                line_height,
            })
        });

        let edit_prediction = edit_prediction_snapshot.and_then(|prediction| {
            let Ok(anchor_pos) = buffer.resolve_anchor_position(prediction.anchor) else {
                return None;
            };
            let display_slot = visible_display_slot(anchor_pos.line)?;
            let origin =
                Self::wrapped_position_origin(&wrapped_position_context, anchor_pos, display_slot);
            let display_text = prediction.text.lines().next().unwrap_or("");
            if display_text.is_empty() {
                return None;
            }
            let shaped = self.shape_line_cached(
                display_text,
                line_height * 0.9,
                &[TextRun {
                    len: display_text.len(),
                    font: Font::default(),
                    color: cx.theme().colors.muted_foreground.opacity(0.5),
                    background_color: None,
                    underline: None,
                    strikethrough: None,
                }],
                window,
            );
            Some(EditPredictionRenderData {
                shaped,
                origin,
                line_height,
            })
        });

        let inlay_hints = visible_chunks
            .iter()
            .flat_map(|chunk| chunk.inlay_hints.iter())
            .filter_map(|hint| {
                let Ok(anchor_pos) = buffer.resolve_anchor_position(hint.anchor) else {
                    return None;
                };
                let display_slot = visible_display_slot(anchor_pos.line)?;
                let mut label = hint.label.clone();
                if hint.padding_left {
                    label.insert(0, ' ');
                }
                if hint.padding_right {
                    label.push(' ');
                }
                if label.is_empty() {
                    return None;
                }

                let color = match hint.kind {
                    Some(crate::InlayHintKind::Type) => {
                        cx.theme().colors.muted_foreground.opacity(0.85)
                    }
                    Some(crate::InlayHintKind::Parameter) => {
                        cx.theme().colors.primary.opacity(0.75)
                    }
                    None => cx.theme().colors.muted_foreground.opacity(0.75),
                };
                let shaped = self.shape_line_cached(
                    &label,
                    font_size * 0.92,
                    &[TextRun {
                        len: hint.label.len()
                            + usize::from(hint.padding_left)
                            + usize::from(hint.padding_right),
                        font: font.clone(),
                        color,
                        background_color: None,
                        underline: None,
                        strikethrough: None,
                    }],
                    window,
                );

                let base_origin = Self::wrapped_position_origin(
                    &wrapped_position_context,
                    anchor_pos,
                    display_slot,
                );

                let origin_x = match hint.side {
                    crate::InlayHintSide::After => base_origin.x + char_width * 0.35,
                    crate::InlayHintSide::Before => base_origin.x + char_width * 0.35,
                };

                let background_bounds = match hint.kind {
                    Some(crate::InlayHintKind::Type) => Some(Bounds::new(
                        point(origin_x - px(4.0), base_origin.y + px(1.0)),
                        size(
                            shaped.width + px(8.0),
                            (line_height - px(2.0)).max(px(12.0)),
                        ),
                    )),
                    _ => None,
                };

                Some(InlayHintRenderData {
                    background_bounds,
                    shaped,
                    origin: point(origin_x, base_origin.y),
                    line_height,
                })
            })
            .collect::<Vec<_>>();

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
                    + char_width * ((level * indent_tab_size) as f32)
                    - horizontal_scroll_pixels;
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

        let sticky_header = document_snapshot
            .display_snapshot
            .sticky_header_excerpt(visible_range.start)
            .map(|header| {
                // Keep sticky headers visually quiet: showing fold-kind prefixes (e.g. "block:")
                // adds noise while scrolling and makes folded contexts harder to read quickly.
                // The source line itself already carries enough context.
                let label = header.text;
                let shaped = self.shape_line_cached(
                    &label,
                    font_size * 0.92,
                    &[TextRun {
                        len: label.len(),
                        font: font.clone(),
                        color: cx.theme().colors.muted_foreground,
                        background_color: None,
                        underline: None,
                        strikethrough: None,
                    }],
                    window,
                );
                StickyHeaderRenderData {
                    shaped,
                    bounds: Bounds::new(
                        point(
                            bounds.origin.x + gutter_width + px(6.0) - horizontal_scroll_pixels,
                            bounds.origin.y,
                        ),
                        size(
                            (bounds.size.width - gutter_width - px(12.0)).max(px(0.0)),
                            line_height,
                        ),
                    ),
                }
            });

        let minimap = if editor_snapshot.minimap_visible && display_line_count > 0 {
            let minimap_width = px(56.0);
            let bounds = Bounds::new(
                point(bounds.right() - minimap_width, bounds.origin.y),
                size(minimap_width, bounds.size.height),
            );
            let visible_rows = editor_snapshot.visible_display_row_range();
            let row_height = bounds.size.height / display_line_count.max(1) as f32;
            let viewport_bounds = Bounds::new(
                point(
                    bounds.origin.x,
                    bounds.origin.y + row_height * visible_rows.start as f32,
                ),
                size(
                    bounds.size.width,
                    (row_height * (visible_rows.end.saturating_sub(visible_rows.start)) as f32)
                        .max(px(8.0)),
                ),
            );

            Some(MinimapRenderData {
                bounds,
                viewport_bounds,
            })
        } else {
            None
        };

        let inline_code_actions = Vec::new();

        let block_widgets = cached_viewport_layout
            .viewport
            .block_widgets()
            .iter()
            .map(|(display_slot, block)| {
                let shaped = self.shape_line_cached(
                    &block.label,
                    line_height * 0.8,
                    &[TextRun {
                        len: block.label.len(),
                        font: window.text_style().font(),
                        color: cx.theme().colors.muted_foreground,
                        background_color: None,
                        underline: None,
                        strikethrough: None,
                    }],
                    window,
                );
                BlockWidgetRenderData {
                    shaped,
                    bounds: Bounds::new(
                        point(
                            bounds.origin.x + gutter_width + px(8.0) - horizontal_scroll_pixels,
                            bounds.origin.y + slot_y(*display_slot) + line_height,
                        ),
                        size(px(180.0), line_height),
                    ),
                }
            })
            .collect::<Vec<_>>();

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
                let display_slot = visible_display_slot(start_pos.line)?;
                let origin = Self::wrapped_position_origin(
                    &wrapped_position_context,
                    start_pos,
                    display_slot,
                );
                let width = char_width
                    * if start_pos.line == end_pos.line {
                        document_snapshot
                            .display_snapshot
                            .display_column_for_position(end_pos)
                            .unwrap_or(end_pos.column)
                            .saturating_sub(
                                document_snapshot
                                    .display_snapshot
                                    .display_column_for_position(start_pos)
                                    .unwrap_or(start_pos.column),
                            ) as f32
                    } else {
                        // Multi-line: just highlight to end of line
                        let line_text = buffer.line(start_pos.line).unwrap_or_default();
                        let end_column = document_snapshot
                            .display_snapshot
                            .tab_snapshot()
                            .display_column_for_text_column(&line_text, line_text.chars().count());
                        end_column.saturating_sub(
                            document_snapshot
                                .display_snapshot
                                .display_column_for_position(start_pos)
                                .unwrap_or(start_pos.column),
                        ) as f32
                    };
                Some(ReferenceHighlightData {
                    rect: Bounds::new(origin, size(width.max(char_width), line_height)),
                })
            })
            .collect();

        // ── Context menu (feat-045) ────────────────────────────────────────────
        let context_menu = context_menu_snapshot.map(|(items, origin_x, origin_y, highlighted)| {
            let item_height = line_height * 1.2;
            let total_height: Pixels = items.iter().fold(px(8.0), |height, (_, is_sep, _)| {
                height + if *is_sep { px(8.0) } else { item_height }
            });
            let menu_width = px(180.0);
            let margin = px(8.0);
            let max_x = (bounds.size.width - menu_width - margin).max(px(0.0));
            let max_y = (bounds.size.height - total_height - margin).max(px(0.0));
            let min_x = margin.min(max_x);
            let min_y = margin.min(max_y);
            let clamped_origin = point(
                bounds.origin.x + px(origin_x).clamp(min_x, max_x),
                bounds.origin.y + px(origin_y).clamp(min_y, max_y),
            );

            ContextMenuRenderData {
                items,
                bounds: Bounds::new(clamped_origin, size(menu_width, total_height)),
                highlighted,
                line_height,
            }
        });

        // Push scrollbar geometry into the editor so that mouse handlers can
        // hit-test and drive scrollbar interaction without element-layer access.
        {
            let cached = scrollbar.as_ref().map(|s| CachedScrollbarBounds {
                track: s.track,
                thumb: s.thumb,
                display_line_count,
            });
            self.editor.update(cx, |editor, _cx| {
                editor.update_cached_scrollbar(cached);
            });
        }

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
            signature_help_tooltip,
            find_panel,
            gutter_width,
            show_line_numbers,
            show_folding,
            highlight_current_line,
            show_gutter_diagnostics,
            cursor_shape,
            cursor_blink_enabled,
            cursor_visible,
            rounded_selection,
            selection_highlight_enabled,
            gutter_lines,
            inline_suggestion,
            edit_prediction,
            inlay_hints,
            goto_line_panel,
            indent_guides,
            scrollbar,
            wrapped_shaped_lines,
            line_y_offsets: line_y_offsets.as_ref().clone(),
            horizontal_scroll_pixels,
            reference_highlights,
            context_menu,
            sticky_header,
            minimap,
            inline_code_actions,
            block_widgets,
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

        let editor_background =
            Self::editor_background_color(cx.theme().highlight_theme.as_ref(), &cx.theme().colors);
        let active_line_background = Self::active_line_background_color(
            cx.theme().highlight_theme.as_ref(),
            &cx.theme().colors,
        );

        // Paint background for the full editor area
        window.paint_quad(fill(prepaint.bounds, editor_background));

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
        window.paint_quad(fill(gutter_bounds, editor_background));

        // Paint line numbers right-aligned inside the gutter padding, excluding the chevron zone
        let fold_chevron_zone = if prepaint.show_folding {
            FOLD_CHEVRON_ZONE
        } else {
            px(0.0)
        };
        let gutter_text_area_width =
            (gutter_width - GUTTER_SEPARATOR_WIDTH - fold_chevron_zone - GUTTER_PADDING * 2.0)
                .max(px(0.0));
        for (slot_idx, gutter_line) in prepaint.gutter_lines.iter().enumerate() {
            let line_y = prepaint.bounds.origin.y
                + prepaint
                    .line_y_offsets
                    .get(slot_idx)
                    .copied()
                    .unwrap_or(prepaint.line_height * (slot_idx as f32));

            // Highlight the current-line row across the entire gutter
            if prepaint.highlight_current_line && gutter_line.is_active {
                let active_row_bounds = Bounds::new(
                    point(prepaint.bounds.origin.x, line_y),
                    size(gutter_width - GUTTER_SEPARATOR_WIDTH, prepaint.line_height),
                );
                window.paint_quad(fill(active_row_bounds, active_line_background));
            }

            if !prepaint.show_line_numbers {
                if prepaint.show_gutter_diagnostics && gutter_line.has_diagnostics {
                    let marker_bounds = Bounds::new(
                        point(
                            prepaint.bounds.origin.x + px(3.0),
                            line_y + (prepaint.line_height - px(6.0)) / 2.0,
                        ),
                        size(px(6.0), px(6.0)),
                    );
                    window.paint_quad(fill(marker_bounds, cx.theme().colors.danger));
                }
                continue;
            }

            // Right-align: start so that the text ends at GUTTER_PADDING from separator
            let text_x = prepaint.bounds.origin.x
                + GUTTER_PADDING
                + (gutter_text_area_width - gutter_line.shaped.width).max(px(0.0));

            _ = gutter_line.shaped.paint(
                point(text_x, line_y),
                prepaint.line_height,
                TextAlign::Left,
                None,
                window,
                cx,
            );

            if prepaint.show_gutter_diagnostics && gutter_line.has_diagnostics {
                let marker_bounds = Bounds::new(
                    point(
                        prepaint.bounds.origin.x + px(3.0),
                        line_y + (prepaint.line_height - px(6.0)) / 2.0,
                    ),
                    size(px(6.0), px(6.0)),
                );
                window.paint_quad(fill(marker_bounds, cx.theme().colors.danger));
            }
        }

        // Separator line between gutter and text content
        let separator_bounds = Bounds::new(
            point(
                prepaint.bounds.origin.x + gutter_width - GUTTER_SEPARATOR_WIDTH,
                prepaint.bounds.origin.y,
            ),
            size(GUTTER_SEPARATOR_WIDTH, prepaint.bounds.size.height),
        );
        if prepaint.gutter_width > px(0.0) {
            window.paint_quad(fill(separator_bounds, cx.theme().colors.border));
        }

        if let Some(sticky_header) = &prepaint.sticky_header {
            window.paint_quad(fill(sticky_header.bounds, editor_background.opacity(0.96)));
            let underline = Bounds::new(
                point(
                    sticky_header.bounds.origin.x,
                    sticky_header.bounds.bottom() - px(1.0),
                ),
                size(sticky_header.bounds.size.width, px(1.0)),
            );
            window.paint_quad(fill(underline, cx.theme().colors.border));
            _ = sticky_header.shaped.paint(
                point(
                    sticky_header.bounds.origin.x + px(4.0),
                    sticky_header.bounds.origin.y,
                ),
                prepaint.line_height,
                TextAlign::Left,
                None,
                window,
                cx,
            );
        }

        if let Some(minimap) = &prepaint.minimap {
            window.paint_quad(fill(minimap.bounds, editor_background.opacity(0.9)));
            window.paint_quad(fill(
                minimap.viewport_bounds,
                cx.theme().colors.primary.opacity(0.25),
            ));
        }

        for action in &prepaint.inline_code_actions {
            window.paint_quad(fill(action.bounds, cx.theme().colors.primary.opacity(0.12)));
            _ = action.shaped.paint(
                point(action.bounds.origin.x + px(6.0), action.bounds.origin.y),
                prepaint.line_height,
                TextAlign::Left,
                None,
                window,
                cx,
            );
        }

        for block in &prepaint.block_widgets {
            window.paint_quad(fill(block.bounds, editor_background.opacity(0.85)));
            _ = block.shaped.paint(
                point(block.bounds.origin.x + px(6.0), block.bounds.origin.y),
                prepaint.line_height,
                TextAlign::Left,
                None,
                window,
                cx,
            );
        }

        // ── Fold chevrons ─────────────────────────────────────────────────────
        // Geometric triangles drawn with paint_path so they scale criply at any
        // DPI. The triangle points right (▶) when folded and down (▼) when open.
        // The chevron brightens to full foreground when the cursor is over it.
        if prepaint.show_folding {
            let mouse_pos = window.mouse_position();
            let normal_color = cx.theme().colors.muted_foreground.opacity(0.45);
            let hover_color = cx.theme().colors.foreground.opacity(0.9);

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
        } else {
            self.editor.update(cx, |editor, _cx| {
                editor.update_cached_fold_chevrons(Vec::new());
            });
        }
        // ─────────────────────────────────────────────────────────────────────

        // Paint selection highlighting (already offset for gutter in prepaint)
        for sel_rect in &prepaint.selection_rects {
            if prepaint.rounded_selection {
                window.paint_quad(quad(
                    *sel_rect,
                    Corners::all(px(3.0)),
                    cx.theme().colors.selection,
                    px(0.0),
                    transparent_black(),
                    BorderStyle::Solid,
                ));
            } else {
                window.paint_quad(fill(*sel_rect, cx.theme().colors.selection));
            }
        }

        // Paint extra cursor selection highlights with the same color as the primary selection.
        for sel_rect in &prepaint.extra_selection_rects {
            if prepaint.rounded_selection {
                window.paint_quad(quad(
                    *sel_rect,
                    Corners::all(px(3.0)),
                    cx.theme().colors.selection,
                    px(0.0),
                    transparent_black(),
                    BorderStyle::Solid,
                ));
            } else {
                window.paint_quad(fill(*sel_rect, cx.theme().colors.selection));
            }
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
        let text_origin_x =
            prepaint.bounds.origin.x + gutter_width - prepaint.horizontal_scroll_pixels;
        let origin_y = prepaint.bounds.origin.y;

        if let Some(ref wrapped_lines) = prepaint.wrapped_shaped_lines {
            for (i, wrapped_line) in wrapped_lines.iter().enumerate() {
                let y = prepaint
                    .line_y_offsets
                    .get(i)
                    .copied()
                    .unwrap_or(gpui::px(0.0));
                let line_origin = point(text_origin_x, origin_y + y);
                _ = wrapped_line.paint(
                    line_origin,
                    prepaint.line_height,
                    TextAlign::Left,
                    None,
                    window,
                    cx,
                );
            }
        } else {
            for (index, shaped_line) in prepaint.shaped_lines.iter().enumerate() {
                let offset_y = prepaint
                    .line_y_offsets
                    .get(index)
                    .copied()
                    .unwrap_or(prepaint.line_height * (index as f32));
                let line_origin = point(text_origin_x, origin_y + offset_y);
                _ = shaped_line.paint(
                    line_origin,
                    prepaint.line_height,
                    TextAlign::Left,
                    None,
                    window,
                    cx,
                );
            }
        }

        // Paint cursor if focused (already offset for gutter in prepaint)
        let cursor_is_solid =
            is_focused && prepaint.cursor_blink_enabled && prepaint.cursor_visible;
        if cursor_is_solid {
            if let Some(cursor_bounds) = prepaint.cursor_bounds {
                Self::paint_cursor_shape(cursor_bounds, prepaint.cursor_shape, window, cx);
            }

            // Paint extra cursors (multi-cursor) with the same style as the primary.
            for extra_bounds in &prepaint.extra_cursor_bounds {
                Self::paint_cursor_shape(*extra_bounds, prepaint.cursor_shape, window, cx);
            }
        } else {
            // When unfocused, render a hollow (outline) cursor so the insertion
            // point remains visible without implying keyboard focus.
            let caret_color = cx.theme().colors.caret.opacity(0.6);
            if let Some(cursor_bounds) = prepaint.cursor_bounds {
                window.paint_quad(quad(
                    cursor_bounds,
                    px(0.0),
                    transparent_black(),
                    px(1.0),
                    caret_color,
                    BorderStyle::Solid,
                ));
            }
        }

        // Paint ghost-text inline suggestion right after the cursor position.
        // The text is dimmed to distinguish it from real buffer content and
        // give the user a clear preview of what will be inserted.
        if let Some(ref suggestion) = prepaint.inline_suggestion {
            self.paint_inline_suggestion(suggestion, window, cx);
        }

        if let Some(ref prediction) = prepaint.edit_prediction {
            self.paint_inline_suggestion(
                &InlineSuggestionRenderData {
                    shaped: prediction.shaped.clone(),
                    origin: prediction.origin,
                    line_height: prediction.line_height,
                },
                window,
                cx,
            );
        }

        for hint in &prepaint.inlay_hints {
            if let Some(background_bounds) = hint.background_bounds {
                window.paint_quad(quad(
                    background_bounds,
                    Corners::all(px(4.0)),
                    cx.theme().colors.selection.opacity(0.16),
                    Edges::all(px(1.0)),
                    cx.theme().colors.border.opacity(0.35),
                    BorderStyle::default(),
                ));
            }

            _ = hint.shaped.paint(
                hint.origin,
                hint.line_height,
                TextAlign::Left,
                None,
                window,
                cx,
            );
        }

        // Paint error squiggles (diagnostics) using pre-calculated bounds
        if self.editor.read(cx).show_inline_diagnostics() && !prepaint.diagnostics.is_empty() {
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

        if let Some(ref hover) = prepaint.hover_tooltip {
            self.paint_hover_tooltip(hover, window, cx);
        }

        if let Some(ref signature_help) = prepaint.signature_help_tooltip {
            self.paint_signature_help_tooltip(signature_help, window, cx);
        }

        // Find/replace panel is now rendered as a GPUI component child of TextEditor's div,
        // not painted here. Match highlights are still painted above.

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
        if prepaint.selection_highlight_enabled {
            self.paint_reference_highlights(&prepaint.reference_highlights, window);
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
        _ = suggestion.shaped.paint(
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

        // ── Estimate menu width from all items (cheap), shape only visible ─
        // Using character count × approximate char width avoids shaping every
        // item just for width measurement, keeping large completion lists fast.
        let visible_start = menu.scroll_offset;
        let visible_end = (visible_start + item_count).min(total_items);

        let approx_char_width = font_size * 0.6;
        let required_width = menu.items.iter().fold(px(0.0), |acc, item| {
            let label_w = approx_char_width * item.label.len() as f32;
            let detail_w = item
                .detail
                .as_ref()
                .map(|d| approx_char_width * (d.len() as f32 + 2.0) + detail_gap)
                .unwrap_or(px(0.0));
            let row_w = left_inset + label_w + detail_w + right_padding + scrollbar_width;
            if row_w > acc { row_w } else { acc }
        });

        let shaped_items: Vec<(usize, ShapedCompletionItem)> = {
            let text_system = window.text_system();
            menu.items[visible_start..visible_end]
                .iter()
                .enumerate()
                .map(|(i, item)| {
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

                    (
                        visible_start + i,
                        ShapedCompletionItem {
                            label,
                            detail,
                            accent,
                        },
                    )
                })
                .collect()
        }; // text_system borrow released; window is exclusively ours again.

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

        let mouse_pos = window.mouse_position();

        for (slot_idx, (absolute_idx, shaped)) in shaped_items.iter().enumerate() {
            let item_y = menu_y + item_height * slot_idx as f32;
            let is_selected = *absolute_idx == menu.selected_index;
            let item_rect = Bounds::new(point(menu_x, item_y), size(menu_width, item_height));
            let is_hovered = !is_selected && item_rect.contains(&mouse_pos);

            // ── Selected-row highlight ───────────────────────────────────────
            if is_selected {
                window.paint_quad(fill(item_rect, selected_tint));
                // 2-px left accent bar (replaces the old badge column separator)
                window.paint_quad(fill(
                    Bounds::new(point(menu_x, item_y), size(px(2.0), item_height)),
                    selected_bar,
                ));
            } else {
                if is_hovered {
                    window.paint_quad(fill(item_rect, selected_tint.opacity(0.5)));
                }
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
            _ = shaped.label.paint(
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
                    _ = detail.paint(
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

    fn paint_signature_help_tooltip(
        &self,
        tooltip: &SignatureHelpTooltipData,
        window: &mut Window,
        cx: &mut App,
    ) {
        let pad_x = px(12.0);
        let pad_y = px(10.0);
        let font_size = px(12.0);
        let line_height = px(17.0);
        let max_width = px(420.0);
        let lines: Vec<&str> = tooltip
            .content
            .lines()
            .filter(|line| !line.is_empty())
            .collect();
        if lines.is_empty() {
            return;
        }

        let text_system = window.text_system();
        let rows: Vec<ShapedLine> = lines
            .iter()
            .map(|line| {
                text_system.shape_line(
                    (*line).to_string().into(),
                    font_size,
                    &[TextRun {
                        len: line.len(),
                        font: Font::default(),
                        color: cx.theme().colors.popover_foreground,
                        background_color: None,
                        underline: None,
                        strikethrough: None,
                    }],
                    None,
                )
            })
            .collect();

        let content_width = rows
            .iter()
            .map(|row| row.width)
            .fold(px(0.0), |acc, width| if width > acc { width } else { acc })
            .min(max_width);
        let content_height = line_height * (rows.len() as f32);
        let panel_size = size(content_width + pad_x * 2.0, content_height + pad_y * 2.0);
        let origin = point(
            tooltip.anchor_bounds.origin.x,
            tooltip.anchor_bounds.origin.y + tooltip.anchor_bounds.size.height + px(6.0),
        );
        let bounds = Bounds::new(origin, panel_size);

        window.paint_quad(quad(
            bounds,
            Corners::all(px(6.0)),
            cx.theme().colors.popover,
            Edges::all(px(1.0)),
            cx.theme().colors.border,
            BorderStyle::default(),
        ));

        let mut y = origin.y + pad_y;
        for row in rows {
            _ = row.paint(
                point(origin.x + pad_x, y),
                line_height,
                TextAlign::Left,
                None,
                window,
                cx,
            );
            y += line_height;
        }
    }

    fn paint_hover_tooltip(&self, hover: &HoverTooltipData, window: &mut Window, cx: &mut App) {
        let pad_x = px(12.0);
        let pad_y = px(10.0);
        let max_content_width = px(420.0);
        let corner_radius = px(6.0);
        let title_font_size = px(12.5);
        let body_font_size = px(12.0);
        let title_line_height = px(20.0);
        let body_line_height = px(17.0);
        let blank_gap = px(5.0);
        let max_lines = 15usize;

        let bg_color = cx.theme().colors.popover;
        let border_color = cx.theme().colors.border;
        let title_color = cx.theme().colors.popover_foreground;
        let body_color = cx.theme().colors.muted_foreground;
        let emphasis_color = cx.theme().colors.popover_foreground;
        let code_color: Hsla = rgb(0x7dcfff).into();
        let code_block_color = cx.theme().colors.popover_foreground;
        let mono_font_family = cx.theme().mono_font_family.clone();

        let lines = preprocess_hover_text(&hover.documentation);
        if lines.is_empty() {
            return;
        }
        let lines = &lines[..lines.len().min(max_lines)];

        let text_system = window.text_system();

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
                    let (display_text, runs) =
                        build_body_runs(text, body_color, emphasis_color, code_color);
                    let shaped = text_system.shape_line(display_text, body_font_size, &runs, None);
                    ShapedRow {
                        shaped: Some(shaped),
                        height: body_line_height,
                    }
                }
                TooltipLine::Code(text) => {
                    let (display_text, runs) =
                        build_code_block_runs(text, code_block_color, mono_font_family.clone());
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

        let content_width = rows
            .iter()
            .filter_map(|row| row.shaped.as_ref())
            .map(|row| row.width)
            .fold(px(0.0), |acc, width| if width > acc { width } else { acc })
            .min(max_content_width);
        let content_height = rows.iter().fold(px(0.0), |acc, row| acc + row.height);

        let tooltip_width = content_width + pad_x * 2.0;
        let tooltip_height = content_height + pad_y * 2.0;

        let gap = px(6.0);
        let above_y = hover.anchor_bounds.origin.y - tooltip_height - gap;
        let below_y = hover.anchor_bounds.origin.y + hover.anchor_bounds.size.height + gap;
        let origin_y = if above_y > px(0.0) { above_y } else { below_y };
        let tooltip_origin = point(hover.anchor_bounds.origin.x, origin_y);
        let tooltip_bounds = Bounds::new(tooltip_origin, size(tooltip_width, tooltip_height));

        window.paint_quad(quad(
            tooltip_bounds,
            Corners::all(corner_radius),
            bg_color,
            Edges::all(px(1.0)),
            border_color,
            BorderStyle::default(),
        ));

        let text_x = tooltip_origin.x + pad_x;
        let mut current_y = tooltip_origin.y + pad_y;

        for row in &rows {
            if let Some(ref shaped_line) = row.shaped {
                _ = shaped_line.paint(
                    point(text_x, current_y),
                    row.height,
                    TextAlign::Left,
                    None,
                    window,
                    cx,
                );
            }
            current_y += row.height;
        }
    }

    /// Helper: shape and paint a single-line text string inside an overlay panel.
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
        _ = shaped.paint(origin, font_size * 1.4, TextAlign::Left, None, window, cx);
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

    /// Paint the right-click context menu as a floating popup (feat-045).
    ///
    /// Each enabled item is painted as a clickable row; disabled items are
    /// dimmed; separator items are thin horizontal rules.
    fn paint_context_menu(&self, menu: &ContextMenuRenderData, window: &mut Window, cx: &mut App) {
        let item_height = menu.line_height * 1.2;
        let padding_x = px(12.0);
        let font_size = menu.line_height * 0.82;
        let corner_radius = px(6.0);
        let menu_bounds = menu.bounds;
        let menu_width = menu_bounds.size.width;

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
        let mut y = menu_bounds.origin.y + px(4.0);
        for (index, (label, is_separator, is_disabled)) in menu.items.iter().enumerate() {
            if *is_separator {
                let sep_y = y + px(4.0);
                window.paint_quad(fill(
                    Bounds::new(
                        point(menu_bounds.origin.x + px(6.0), sep_y),
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
                    Bounds::new(
                        point(menu_bounds.origin.x, y),
                        size(menu_width, item_height),
                    ),
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
                    menu_bounds.origin.x + padding_x,
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

enum TooltipLine {
    Title(String),
    Body(String),
    Code(String),
    Blank,
}

fn preprocess_hover_text(raw: &str) -> Vec<TooltipLine> {
    let mut result = Vec::new();
    let mut last_was_blank = true;
    let mut in_code_block = false;

    for raw_line in raw.trim_end_matches('\n').split('\n') {
        let line = raw_line.trim_end_matches('\r');

        if line.trim_start().starts_with("```") {
            if in_code_block {
                if !last_was_blank {
                    result.push(TooltipLine::Blank);
                    last_was_blank = true;
                }
                in_code_block = false;
            } else {
                if !last_was_blank {
                    result.push(TooltipLine::Blank);
                }
                in_code_block = true;
                last_was_blank = true;
            }
            continue;
        }

        if in_code_block {
            result.push(TooltipLine::Code(line.to_string()));
            last_was_blank = false;
            continue;
        }

        if line.trim().is_empty() {
            if !last_was_blank {
                result.push(TooltipLine::Blank);
                last_was_blank = true;
            }
            continue;
        }

        last_was_blank = false;

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

    while matches!(result.last(), Some(TooltipLine::Blank)) {
        result.pop();
    }

    result
}

fn build_body_runs(
    text: &str,
    body_color: Hsla,
    bold_color: Hsla,
    code_color: Hsla,
) -> (SharedString, Vec<TextRun>) {
    let mut display = String::new();
    let mut runs = Vec::new();
    let mut rest = text;
    let default_font = Font::default();
    let bold_font = Font {
        weight: FontWeight::BOLD,
        ..Font::default()
    };

    while !rest.is_empty() {
        let next_code = rest.find('`');
        let next_bold = rest.find("**");
        let next_marker = match (next_code, next_bold) {
            (Some(code), Some(bold)) if code <= bold => Some((code, Marker::Code)),
            (Some(_), Some(bold)) => Some((bold, Marker::Bold)),
            (Some(code), None) => Some((code, Marker::Code)),
            (None, Some(bold)) => Some((bold, Marker::Bold)),
            (None, None) => None,
        };

        match next_marker {
            Some((marker_start, marker_kind)) => {
                if marker_start > 0 {
                    push_tooltip_run(
                        &mut display,
                        &mut runs,
                        &rest[..marker_start],
                        default_font.clone(),
                        body_color,
                    );
                }

                match marker_kind {
                    Marker::Code => {
                        let after = &rest[marker_start + 1..];
                        match after.find('`') {
                            Some(close_pos) => {
                                let code = &after[..close_pos];
                                if !code.is_empty() {
                                    push_tooltip_run(
                                        &mut display,
                                        &mut runs,
                                        code,
                                        default_font.clone(),
                                        code_color,
                                    );
                                }
                                rest = &after[close_pos + 1..];
                            }
                            None => {
                                push_tooltip_run(
                                    &mut display,
                                    &mut runs,
                                    &rest[marker_start..],
                                    default_font.clone(),
                                    body_color,
                                );
                                break;
                            }
                        }
                    }
                    Marker::Bold => {
                        let after = &rest[marker_start + 2..];
                        match after.find("**") {
                            Some(close_pos) => {
                                let bold = &after[..close_pos];
                                if !bold.is_empty() {
                                    push_tooltip_run(
                                        &mut display,
                                        &mut runs,
                                        bold,
                                        bold_font.clone(),
                                        bold_color,
                                    );
                                }
                                rest = &after[close_pos + 2..];
                            }
                            None => {
                                push_tooltip_run(
                                    &mut display,
                                    &mut runs,
                                    &rest[marker_start..],
                                    default_font.clone(),
                                    body_color,
                                );
                                break;
                            }
                        }
                    }
                }
            }
            None => {
                push_tooltip_run(
                    &mut display,
                    &mut runs,
                    rest,
                    default_font.clone(),
                    body_color,
                );
                break;
            }
        }
    }

    if display.is_empty() {
        push_tooltip_run(&mut display, &mut runs, " ", default_font, body_color);
    }

    (display.into(), runs)
}

fn build_code_block_runs(
    text: &str,
    color: Hsla,
    font_family: SharedString,
) -> (SharedString, Vec<TextRun>) {
    let mut display = text.to_string();
    if display.is_empty() {
        display.push(' ');
    }

    (
        display.clone().into(),
        vec![TextRun {
            len: display.len(),
            font: Font {
                family: font_family,
                ..Font::default()
            },
            color,
            background_color: None,
            underline: None,
            strikethrough: None,
        }],
    )
}

fn push_tooltip_run(
    display: &mut String,
    runs: &mut Vec<TextRun>,
    text: &str,
    font: Font,
    color: Hsla,
) {
    if text.is_empty() {
        return;
    }

    display.push_str(text);
    runs.push(TextRun {
        len: text.len(),
        font,
        color,
        background_color: None,
        underline: None,
        strikethrough: None,
    });
}

#[derive(Clone, Copy)]
enum Marker {
    Code,
    Bold,
}

#[cfg(test)]
mod tests {
    use super::{
        CachedViewportLayout, EditorElement, TooltipLine, ViewportLayoutKeyInput,
        ViewportTextStyleCacheKey, build_body_runs, preprocess_hover_text,
    };
    use crate::buffer::Position;
    use crate::display_map::{ChunkHighlight, DisplayRowId, DisplayTextChunk};
    use crate::{DisplayMap, HighlightKind, VisibleWrapLayout};
    use gpui::{Font, FontWeight, SharedString, point, px, rgb};
    use zqlz_ui::widgets::{
        ThemeColor, ThemeMode,
        highlighter::{HighlightTheme, HighlightThemeStyle, SyntaxColors, ThemeStyle},
    };

    fn test_viewport_text_style_key() -> ViewportTextStyleCacheKey {
        ViewportTextStyleCacheKey {
            font: Font::default(),
            default_text_color: rgb(0xffffff).into(),
            active_gutter_color: rgb(0xffffff).into(),
            inactive_gutter_color: rgb(0x999999).into(),
            keyword_color: rgb(0x569cd6).into(),
            string_color: rgb(0xce9178).into(),
            comment_color: rgb(0x6a9955).into(),
            number_color: rgb(0xb5cea8).into(),
            identifier_color: rgb(0xd4d4d4).into(),
            operator_color: rgb(0xd4d4d4).into(),
            function_color: rgb(0xdcdcaa).into(),
            punctuation_color: rgb(0xd4d4d4).into(),
            boolean_color: rgb(0x569cd6).into(),
            null_color: rgb(0x569cd6).into(),
            error_color: rgb(0xf44747).into(),
        }
    }

    #[test]
    fn visible_range_clamps_to_last_available_line() {
        let visible = EditorElement::test_visible_range(5, px(20.0), px(45.0), 4.8);

        assert_eq!(visible, 4..5);
    }

    #[test]
    fn cursor_pixel_position_uses_gutter_and_visual_slot_offsets() {
        let pixel_position = EditorElement::test_cursor_pixel_position(
            Position::new(7, 4),
            px(20.0),
            px(10.0),
            1.0,
            px(32.0),
            3,
        );

        assert_eq!(pixel_position, point(px(72.0), px(40.0)));
    }

    #[test]
    fn preprocess_hover_text_collapses_blank_sections_and_titles() {
        let lines = preprocess_hover_text("\n**SELECT**\n\nbody\n\n\n`sql`\n");

        assert!(matches!(lines[0], TooltipLine::Title(ref title) if title == "SELECT"));
        assert!(matches!(lines[1], TooltipLine::Blank));
        assert!(matches!(lines[2], TooltipLine::Body(ref body) if body == "body"));
        assert!(matches!(lines[3], TooltipLine::Blank));
        assert!(matches!(lines[4], TooltipLine::Body(ref body) if body == "`sql`"));
        assert_eq!(lines.len(), 5);
    }

    #[test]
    fn preprocess_hover_text_treats_code_fences_as_code_block_lines() {
        let lines = preprocess_hover_text("**Definition:**\n```sql\nSELECT *\nFROM users\n```\n");

        assert!(matches!(lines[0], TooltipLine::Title(ref title) if title == "Definition:"));
        assert!(matches!(lines[1], TooltipLine::Blank));
        assert!(matches!(lines[2], TooltipLine::Code(ref line) if line == "SELECT *"));
        assert!(matches!(lines[3], TooltipLine::Code(ref line) if line == "FROM users"));
    }

    #[test]
    fn build_body_runs_splits_inline_code_and_bold_into_distinct_text_runs() {
        let body_color = rgb(0xffffff).into();
        let bold_color = rgb(0xffaa00).into();
        let code_color = rgb(0x00ffff).into();
        let (display, runs) = build_body_runs(
            "before `code` **PK** after",
            body_color,
            bold_color,
            code_color,
        );

        assert_eq!(display, SharedString::from("before code PK after"));
        assert_eq!(runs.len(), 5);
        assert_eq!(runs[0].len, 7);
        assert_eq!(runs[0].color, body_color);
        assert_eq!(runs[1].len, 4);
        assert_eq!(runs[1].color, code_color);
        assert_eq!(runs[2].len, 1);
        assert_eq!(runs[2].color, body_color);
        assert_eq!(runs[3].len, 2);
        assert_eq!(runs[3].color, bold_color);
        assert_eq!(runs[3].font.weight, FontWeight::BOLD);
        assert_eq!(runs[4].len, 6);
        assert_eq!(runs[4].color, body_color);
    }

    #[test]
    fn resolve_highlight_color_falls_back_to_default_syntax_palette() {
        let active_theme = HighlightTheme {
            name: "Active".into(),
            appearance: ThemeMode::Light,
            style: HighlightThemeStyle::default(),
        };
        let fallback_theme = HighlightTheme {
            name: "Fallback".into(),
            appearance: ThemeMode::Light,
            style: HighlightThemeStyle {
                syntax: SyntaxColors {
                    keyword: Some(ThemeStyle {
                        color: Some(rgb(0x0433ff).into()),
                        font_style: None,
                        font_weight: None,
                    }),
                    ..SyntaxColors::default()
                },
                ..HighlightThemeStyle::default()
            },
        };

        assert_eq!(
            EditorElement::resolve_highlight_color(
                crate::HighlightKind::Keyword,
                &active_theme,
                &fallback_theme,
                &ThemeColor::default(),
                rgb(0x111111).into(),
            ),
            rgb(0x0433ff).into()
        );
    }

    #[test]
    fn generic_highlight_fallback_uses_palette_for_semantic_tokens() {
        assert_eq!(
            EditorElement::generic_highlight_fallback_color(
                crate::HighlightKind::Keyword,
                &ThemeColor::default(),
                rgb(0x222222).into(),
                rgb(0x333333).into(),
            ),
            rgb(0x333333).into()
        );
        assert_eq!(
            EditorElement::generic_highlight_fallback_color(
                crate::HighlightKind::Identifier,
                &ThemeColor::default(),
                rgb(0x222222).into(),
                rgb(0x333333).into(),
            ),
            rgb(0x222222).into()
        );
    }

    #[test]
    fn resolve_highlight_color_avoids_monochrome_active_theme_slots() {
        let foreground = rgb(0x111111).into();
        let active_theme = HighlightTheme {
            name: "Active".into(),
            appearance: ThemeMode::Light,
            style: HighlightThemeStyle {
                editor_foreground: Some(foreground),
                syntax: SyntaxColors {
                    keyword: Some(ThemeStyle {
                        color: Some(foreground),
                        font_style: None,
                        font_weight: None,
                    }),
                    ..SyntaxColors::default()
                },
                ..HighlightThemeStyle::default()
            },
        };
        let fallback_theme = HighlightTheme {
            name: "Fallback".into(),
            appearance: ThemeMode::Light,
            style: HighlightThemeStyle {
                syntax: SyntaxColors {
                    keyword: Some(ThemeStyle {
                        color: Some(rgb(0x0433ff).into()),
                        font_style: None,
                        font_weight: None,
                    }),
                    ..SyntaxColors::default()
                },
                ..HighlightThemeStyle::default()
            },
        };

        assert_eq!(
            EditorElement::resolve_highlight_color(
                crate::HighlightKind::Keyword,
                &active_theme,
                &fallback_theme,
                &ThemeColor::default(),
                foreground,
            ),
            rgb(0x0433ff).into()
        );
    }

    #[test]
    fn default_syntax_palette_colors_are_visibly_distinct() {
        let theme = ThemeColor::light();
        let colors = theme.as_ref();

        assert_ne!(
            EditorElement::default_syntax_palette_color(HighlightKind::Keyword, colors),
            colors.foreground
        );
        assert_ne!(
            EditorElement::default_syntax_palette_color(HighlightKind::Function, colors),
            colors.foreground
        );
        assert_ne!(
            EditorElement::default_syntax_palette_color(HighlightKind::String, colors),
            colors.foreground
        );
        assert_ne!(
            EditorElement::default_syntax_palette_color(HighlightKind::Null, colors),
            colors.foreground
        );
        assert_eq!(
            EditorElement::default_syntax_palette_color(HighlightKind::Comment, colors),
            colors.muted_foreground
        );
        assert_eq!(
            EditorElement::default_syntax_palette_color(HighlightKind::Function, colors),
            colors.cyan
        );
        assert_eq!(
            EditorElement::default_syntax_palette_color(HighlightKind::Keyword, colors),
            colors.blue
        );
    }

    #[test]
    fn editor_color_helpers_prefer_editor_theme_slots() {
        let theme_colors = ThemeColor::default();
        let highlight_theme = HighlightTheme {
            name: "Active".into(),
            appearance: ThemeMode::Light,
            style: HighlightThemeStyle {
                editor_background: Some(rgb(0xf0f0f0).into()),
                editor_foreground: Some(rgb(0x101010).into()),
                editor_line_number: Some(rgb(0x777777).into()),
                editor_active_line_number: Some(rgb(0x202020).into()),
                editor_active_line: Some(rgb(0xe0e0e0).into()),
                syntax: SyntaxColors::default(),
                ..HighlightThemeStyle::default()
            },
        };

        assert_eq!(
            EditorElement::editor_background_color(&highlight_theme, &theme_colors),
            rgb(0xf0f0f0).into()
        );
        assert_eq!(
            EditorElement::editor_foreground_color(&highlight_theme, &theme_colors),
            rgb(0x101010).into()
        );
        assert_eq!(
            EditorElement::gutter_line_number_color(&highlight_theme, &theme_colors, false),
            rgb(0x777777).into()
        );
        assert_eq!(
            EditorElement::gutter_line_number_color(&highlight_theme, &theme_colors, true),
            rgb(0x202020).into()
        );
        assert_eq!(
            EditorElement::active_line_background_color(&highlight_theme, &theme_colors),
            rgb(0xe0e0e0).into()
        );
    }

    #[test]
    fn chunk_text_runs_assigns_distinct_colors_for_sql_highlights() {
        let chunk = DisplayTextChunk {
            row_id: DisplayRowId {
                buffer_line: 0,
                wrap_subrow: 0,
            },
            display_row: 0,
            buffer_line: 0,
            start_offset: 0,
            text: "SELECT * from web_html".to_string(),
            highlights: vec![
                ChunkHighlight {
                    start: 0,
                    end: 6,
                    kind: HighlightKind::Keyword,
                },
                ChunkHighlight {
                    start: 7,
                    end: 8,
                    kind: HighlightKind::Operator,
                },
                ChunkHighlight {
                    start: 9,
                    end: 13,
                    kind: HighlightKind::Keyword,
                },
                ChunkHighlight {
                    start: 14,
                    end: 22,
                    kind: HighlightKind::Identifier,
                },
            ],
            diagnostics: Vec::new(),
            inlay_hints: Vec::new(),
        };

        let style = test_viewport_text_style_key();
        let runs = EditorElement::chunk_text_runs(&chunk, &style);

        assert!(runs.len() >= 4);
        assert_eq!(
            runs.iter().map(|run| run.len).sum::<usize>(),
            chunk.text.len()
        );
        assert_eq!(runs[0].color, style.keyword_color);
        assert!(runs.iter().any(|run| run.color == style.operator_color));
        assert!(runs.iter().any(|run| run.color == style.identifier_color));
        assert!(runs.iter().any(|run| run.color != style.default_text_color));
    }

    #[test]
    fn chunk_text_runs_clips_overlapping_highlights_to_valid_ranges() {
        let text = "SELECT COUNT(*) FROM \"_database_functions\"".to_string();
        let chunk = DisplayTextChunk {
            row_id: DisplayRowId {
                buffer_line: 0,
                wrap_subrow: 0,
            },
            display_row: 0,
            buffer_line: 0,
            start_offset: 0,
            text: text.clone(),
            highlights: vec![
                ChunkHighlight {
                    start: 0,
                    end: 6,
                    kind: HighlightKind::Keyword,
                },
                ChunkHighlight {
                    start: 7,
                    end: 12,
                    kind: HighlightKind::Function,
                },
                ChunkHighlight {
                    start: 7,
                    end: 13,
                    kind: HighlightKind::Identifier,
                },
                ChunkHighlight {
                    start: 12,
                    end: 15,
                    kind: HighlightKind::Punctuation,
                },
                ChunkHighlight {
                    start: 16,
                    end: text.len(),
                    kind: HighlightKind::Keyword,
                },
                ChunkHighlight {
                    start: 21,
                    end: text.len(),
                    kind: HighlightKind::Identifier,
                },
            ],
            diagnostics: Vec::new(),
            inlay_hints: Vec::new(),
        };

        let style = test_viewport_text_style_key();
        let runs = EditorElement::chunk_text_runs(&chunk, &style);

        assert!(!runs.is_empty());
        assert!(runs.iter().all(|run| run.len > 0));
        assert_eq!(
            runs.iter().map(|run| run.len).sum::<usize>(),
            chunk.text.len()
        );
        assert_eq!(runs[0].color, style.keyword_color);
        assert!(runs.iter().any(|run| run.color == style.function_color));
        assert!(runs.iter().any(|run| run.color == style.punctuation_color));
        assert!(runs.iter().any(|run| run.color == style.identifier_color));
    }

    #[test]
    fn viewport_layout_key_changes_only_when_layout_inputs_change() {
        let bounds = gpui::Bounds::new(point(px(0.0), px(0.0)), gpui::size(px(400.0), px(200.0)));
        let style = test_viewport_text_style_key();
        let base = EditorElement::viewport_layout_key(ViewportLayoutKeyInput {
            revision: 7,
            syntax_generation: 1,
            visible_rows: 2..6,
            relative_line_numbers: false,
            cursor_line: 3,
            text_style: style.clone(),
            scroll_offset: 1.5,
            line_height: px(20.0),
            font_size: px(14.0),
            gutter_width: px(32.0),
            char_width: px(10.0),
            bounds,
            soft_wrap: true,
        });
        let same = EditorElement::viewport_layout_key(ViewportLayoutKeyInput {
            revision: 7,
            syntax_generation: 1,
            visible_rows: 2..6,
            relative_line_numbers: false,
            cursor_line: 3,
            text_style: style.clone(),
            scroll_offset: 1.5,
            line_height: px(20.0),
            font_size: px(14.0),
            gutter_width: px(32.0),
            char_width: px(10.0),
            bounds,
            soft_wrap: true,
        });
        let resized = EditorElement::viewport_layout_key(ViewportLayoutKeyInput {
            revision: 7,
            syntax_generation: 1,
            visible_rows: 2..6,
            relative_line_numbers: false,
            cursor_line: 3,
            text_style: style,
            scroll_offset: 1.5,
            line_height: px(20.0),
            font_size: px(14.0),
            gutter_width: px(32.0),
            char_width: px(10.0),
            bounds: gpui::Bounds::new(point(px(0.0), px(0.0)), gpui::size(px(500.0), px(200.0))),
            soft_wrap: true,
        });

        assert_eq!(base, same);
        assert_ne!(base, resized);
    }

    #[test]
    fn viewport_layout_key_tracks_style_and_cursor_inputs() {
        let bounds = gpui::Bounds::new(point(px(0.0), px(0.0)), gpui::size(px(400.0), px(200.0)));
        let base = EditorElement::viewport_layout_key(ViewportLayoutKeyInput {
            revision: 7,
            syntax_generation: 1,
            visible_rows: 2..6,
            relative_line_numbers: false,
            cursor_line: 3,
            text_style: test_viewport_text_style_key(),
            scroll_offset: 1.5,
            line_height: px(20.0),
            font_size: px(14.0),
            gutter_width: px(32.0),
            char_width: px(10.0),
            bounds,
            soft_wrap: true,
        });
        let relative = EditorElement::viewport_layout_key(ViewportLayoutKeyInput {
            revision: 7,
            syntax_generation: 1,
            visible_rows: 2..6,
            relative_line_numbers: true,
            cursor_line: 3,
            text_style: test_viewport_text_style_key(),
            scroll_offset: 1.5,
            line_height: px(20.0),
            font_size: px(14.0),
            gutter_width: px(32.0),
            char_width: px(10.0),
            bounds,
            soft_wrap: true,
        });
        let moved_cursor = EditorElement::viewport_layout_key(ViewportLayoutKeyInput {
            revision: 7,
            syntax_generation: 1,
            visible_rows: 2..6,
            relative_line_numbers: false,
            cursor_line: 4,
            text_style: test_viewport_text_style_key(),
            scroll_offset: 1.5,
            line_height: px(20.0),
            font_size: px(14.0),
            gutter_width: px(32.0),
            char_width: px(10.0),
            bounds,
            soft_wrap: true,
        });

        assert_ne!(base, relative);
        assert_ne!(base, moved_cursor);
    }

    #[test]
    fn viewport_layout_key_tracks_syntax_generation() {
        let bounds = gpui::Bounds::new(point(px(0.0), px(0.0)), gpui::size(px(400.0), px(200.0)));
        let base = EditorElement::viewport_layout_key(ViewportLayoutKeyInput {
            revision: 7,
            syntax_generation: 1,
            visible_rows: 2..6,
            relative_line_numbers: false,
            cursor_line: 3,
            text_style: test_viewport_text_style_key(),
            scroll_offset: 1.5,
            line_height: px(20.0),
            font_size: px(14.0),
            gutter_width: px(32.0),
            char_width: px(10.0),
            bounds,
            soft_wrap: true,
        });
        let updated_syntax = EditorElement::viewport_layout_key(ViewportLayoutKeyInput {
            revision: 7,
            syntax_generation: 2,
            visible_rows: 2..6,
            relative_line_numbers: false,
            cursor_line: 3,
            text_style: test_viewport_text_style_key(),
            scroll_offset: 1.5,
            line_height: px(20.0),
            font_size: px(14.0),
            gutter_width: px(32.0),
            char_width: px(10.0),
            bounds,
            soft_wrap: true,
        });

        assert_ne!(base, updated_syntax);
    }

    #[test]
    fn cached_viewport_layout_can_carry_measured_wrap_layout() {
        let viewport = DisplayMap::from_display_lines(vec![0])
            .snapshot(
                crate::buffer::TextBuffer::new("abcdefghij").snapshot(),
                &[],
                &std::collections::HashSet::new(),
                true,
                std::sync::Arc::new(Vec::new()),
                &[],
                std::sync::Arc::new(Vec::new()),
                &[],
            )
            .viewport(0..1);
        let cache = CachedViewportLayout {
            shaped_lines: Vec::new(),
            wrapped_shaped_lines: None,
            gutter_lines: Vec::new(),
            viewport,
            wrap_layout: Some(VisibleWrapLayout::new(0..1, vec![3], 4, 0.0, px(20.0))),
        };

        assert_eq!(
            cache
                .wrap_layout
                .expect("wrap layout")
                .visual_rows()
                .as_ref(),
            &vec![3]
        );
    }
}
