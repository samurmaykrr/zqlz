use crate::{
    AnchoredCodeAction, AnchoredDiagnostic, AnchoredInlayHint, BufferSnapshot, CompletionMenuData,
    ContextMenuSnapshot, Cursor, CursorShapeStyle, DisplaySnapshot, DocumentContext,
    DocumentIdentity, EditPrediction, EditorAppearance, EditorInlayHint, FindSnapshot, FoldRegion,
    GoToLineSnapshot, Highlight, HoverState, InlineSuggestion, LanguagePipelineSnapshot,
    LargeFilePolicyTier, Position, ResolvedLargeFilePolicy, Selection, SelectionsCollection,
    SignatureHelpState, SyntaxRefreshStrategy, VisibleWrapLayout, input_state::CachedEditorLayout,
    syntax_refresh_strategy_for_policy,
};
use gpui::{Bounds, Pixels, px};

#[derive(Clone, Debug)]
pub struct DocumentSnapshot {
    pub buffer: BufferSnapshot,
    pub context: DocumentContext,
    pub identity: DocumentIdentity,
    pub large_file_policy: ResolvedLargeFilePolicy,
    pub display_snapshot: DisplaySnapshot,
    pub language: LanguagePipelineSnapshot,
    pub inline_suggestion: Option<InlineSuggestion>,
    pub edit_prediction: Option<EditPrediction>,
    pub hover_state: Option<HoverState>,
    pub signature_help_state: Option<SignatureHelpState>,
    pub soft_wrap: bool,
    pub show_inline_diagnostics: bool,
}

impl DocumentSnapshot {
    pub fn buffer_snapshot(&self) -> BufferSnapshot {
        self.buffer.clone()
    }

    pub fn revision(&self) -> usize {
        self.buffer.revision()
    }

    pub fn line_count(&self) -> usize {
        self.buffer.line_count()
    }

    pub fn byte_len(&self) -> usize {
        self.buffer.len()
    }

    pub fn line(&self, line: usize) -> Option<String> {
        self.buffer.line(line)
    }

    pub fn slice(&self, range: std::ops::Range<usize>) -> anyhow::Result<String> {
        self.buffer.slice(range)
    }

    pub fn position_to_offset(&self, position: Position) -> anyhow::Result<usize> {
        self.buffer.position_to_offset(position)
    }

    pub fn offset_to_position(&self, offset: usize) -> anyhow::Result<Position> {
        self.buffer.offset_to_position(offset)
    }

    pub fn display_line_count(&self) -> usize {
        self.display_snapshot.display_line_count()
    }

    pub fn display_lines(&self) -> std::sync::Arc<Vec<usize>> {
        self.display_snapshot.display_lines()
    }

    pub fn visible_buffer_lines(&self) -> &[usize] {
        self.display_snapshot.visible_buffer_lines()
    }

    pub fn folded_lines(&self) -> &std::collections::HashSet<usize> {
        self.display_snapshot.folded_lines()
    }

    pub fn buffer_line_for_display_slot(&self, display_slot: usize) -> Option<usize> {
        self.display_snapshot
            .buffer_line_for_display_slot(display_slot)
    }

    pub fn display_slot_for_buffer_line(&self, buffer_line: usize) -> Option<usize> {
        self.display_snapshot
            .display_slot_for_buffer_line(buffer_line)
    }

    pub fn block_widgets(&self) -> &[crate::display_map::BlockWidgetChunk] {
        self.display_snapshot.block_widgets()
    }

    pub fn anchored_diagnostics(&self) -> &[AnchoredDiagnostic] {
        &self.language.anchored_diagnostics
    }

    pub fn anchored_inlay_hints(&self) -> std::sync::Arc<Vec<AnchoredInlayHint>> {
        self.language.anchored_inlay_hints.clone()
    }

    pub fn anchored_code_actions(&self) -> &[AnchoredCodeAction] {
        &self.language.anchored_code_actions
    }

    pub fn fold_regions(&self) -> &[FoldRegion] {
        &self.language.fold_regions
    }

    pub fn syntax_highlights(&self) -> std::sync::Arc<Vec<Highlight>> {
        self.language.syntax.highlights()
    }

    pub fn diagnostics(&self) -> &[Highlight] {
        &self.language.diagnostics
    }

    pub fn inlay_hints(&self) -> std::sync::Arc<Vec<EditorInlayHint>> {
        self.language.inlay_hints.clone()
    }

    pub fn reference_ranges(&self) -> &[std::ops::Range<usize>] {
        &self.language.reference_ranges
    }

    pub fn is_dirty(&self) -> bool {
        self.context.saved_revision != self.buffer.revision()
    }

    pub fn is_large_file(&self) -> bool {
        self.large_file_policy.tier != LargeFilePolicyTier::Full
    }

    pub fn syntax_refresh_strategy(
        &self,
        viewport_lines: usize,
        scroll_offset: f32,
    ) -> SyntaxRefreshStrategy {
        syntax_refresh_strategy_for_policy(
            self.large_file_policy,
            self.display_snapshot
                .visible_byte_range(scroll_offset, viewport_lines),
        )
    }
}

#[derive(Clone, Debug)]
pub struct EditorSnapshot {
    pub document: DocumentSnapshot,
    pub appearance: EditorAppearance,
    pub bracket_pairs: Vec<(usize, usize)>,
    pub selections: SelectionsCollection,
    pub scroll_offset: f32,
    pub scroll_anchor_position: Position,
    pub scroll_anchor_visual_offset: f32,
    pub horizontal_scroll_offset: f32,
    pub viewport_lines: usize,
    pub gutter_width: f32,
    pub char_width: Pixels,
    pub bounds_origin: gpui::Point<Pixels>,
    pub bounds_size: gpui::Size<Pixels>,
    pub(crate) cached_layout: CachedEditorLayout,
    pub minimap_visible: bool,
    pub show_line_numbers: bool,
    pub show_folding: bool,
    pub highlight_current_line: bool,
    pub relative_line_numbers: bool,
    pub show_gutter_diagnostics: bool,
    pub cursor_shape: CursorShapeStyle,
    pub cursor_blink_enabled: bool,
    pub cursor_visible: bool,
    pub rounded_selection: bool,
    pub selection_highlight_enabled: bool,
    pub completion_menu: Option<CompletionMenuData>,
    pub context_menu: Option<ContextMenuSnapshot>,
    pub goto_line_info: Option<GoToLineSnapshot>,
    pub find_info: Option<FindSnapshot>,
}

impl EditorSnapshot {
    pub fn cursor(&self) -> Option<&Cursor> {
        self.selections.primary().map(|entry| &entry.cursor)
    }

    pub fn selection(&self) -> Option<&Selection> {
        self.selections.primary().map(|entry| &entry.selection)
    }

    pub fn extra_cursors(&self) -> Vec<(Cursor, Selection)> {
        self.selections
            .primary_and_extras()
            .map(|(_, _, extras)| extras)
            .unwrap_or_default()
    }

    pub fn revision(&self) -> usize {
        self.document.revision()
    }

    pub fn anchor_display_row(&self) -> usize {
        self.document
            .display_snapshot
            .point_to_display_point(self.scroll_anchor_position)
            .map(|point| point.row)
            .unwrap_or(self.scroll_anchor_position.line)
    }

    pub fn anchored_scroll_offset(&self) -> f32 {
        (self.anchor_display_row() as f32 - self.scroll_anchor_visual_offset).max(0.0)
    }

    pub fn visible_display_row_range(&self) -> std::ops::Range<usize> {
        let start = self.scroll_offset.floor().max(0.0) as usize;
        let end = (start + self.viewport_lines.max(1)).min(self.document.display_line_count());
        start..end
    }

    pub fn position_to_display_slot(&self, position: Position) -> Option<usize> {
        self.document.display_slot_for_buffer_line(position.line)
    }

    pub fn pixel_to_position(&self, point: gpui::Point<Pixels>, line_height: Pixels) -> Position {
        let horizontal_scroll_pixels = self.char_width * self.horizontal_scroll_offset.max(0.0);
        let relative_x = (point.x - self.bounds_origin.x - px(self.gutter_width)
            + horizontal_scroll_pixels)
            .max(px(0.0));
        let relative_y = (point.y - self.bounds_origin.y).max(px(0.0));
        let display_snapshot = &self.document.display_snapshot;

        if let Some(wrap_layout) = self.wrap_layout_for_viewport(line_height) {
            let display_column = if self.char_width > px(0.0) {
                (relative_x / self.char_width).max(0.0) as usize
            } else {
                0
            };

            if let Some((display_slot, wrap_subrow)) =
                wrap_layout.display_slot_and_subrow_for_y(relative_y, line_height)
                && let Some(buffer_line) =
                    display_snapshot.buffer_line_for_display_slot(display_slot)
            {
                let resolved_display_column = wrap_subrow
                    .saturating_mul(wrap_layout.wrap_column().max(1))
                    .saturating_add(display_column);
                return display_snapshot
                    .position_for_display_column(buffer_line, resolved_display_column);
            }
        }

        let display_slot = ((relative_y / line_height) + self.scroll_offset) as usize;
        let buffer_line = self
            .document
            .buffer_line_for_display_slot(display_slot)
            .or_else(|| self.document.visible_buffer_lines().last().copied())
            .unwrap_or(0);
        let column = if self.char_width > px(0.0) {
            (relative_x / self.char_width).max(0.0) as usize
        } else {
            0
        };

        display_snapshot.position_for_display_column(buffer_line, column)
    }

    pub fn bounds_for_range(
        &self,
        start: Position,
        end: Position,
        bounds: Bounds<Pixels>,
        line_height: Pixels,
    ) -> Option<Bounds<Pixels>> {
        let char_width = self.char_width;
        let gutter_width = gpui::px(self.gutter_width);
        let scroll_x = char_width * self.horizontal_scroll_offset.max(0.0);
        let display_slot = self.position_to_display_slot(start)?;
        let display_snapshot = &self.document.display_snapshot;
        let start_display_column = display_snapshot.display_column_for_position(start)?;
        let end_display_column = display_snapshot.display_column_for_position(end)?;

        let (origin_x, origin_y, width_columns) =
            if let Some(wrap_layout) = self.wrap_layout_for_viewport(line_height) {
                let row_y = wrap_layout.line_y_offset_for_slot(display_slot)?;
                let wrap_column = wrap_layout.wrap_column().max(1);
                let visual_row = start_display_column / wrap_column;
                let visual_column = start_display_column % wrap_column;
                let end_subrow = end_display_column / wrap_column;
                let end_visual_column = end_display_column % wrap_column;
                let width_columns = if start.line == end.line && visual_row == end_subrow {
                    end_visual_column.saturating_sub(visual_column).max(1)
                } else {
                    1
                };
                (
                    bounds.origin.x + gutter_width + char_width * (visual_column as f32) - scroll_x,
                    bounds.origin.y + row_y + line_height * (visual_row as f32),
                    width_columns,
                )
            } else {
                (
                    bounds.origin.x + gutter_width + char_width * (start_display_column as f32)
                        - scroll_x,
                    bounds.origin.y + line_height * (display_slot as f32 - self.scroll_offset),
                    if start.line == end.line {
                        end_display_column
                            .saturating_sub(start_display_column)
                            .max(1)
                    } else {
                        1
                    },
                )
            };

        Some(Bounds::new(
            gpui::point(origin_x, origin_y),
            gpui::size(char_width * (width_columns as f32), line_height),
        ))
    }

    fn wrap_layout_for_viewport(&self, line_height: Pixels) -> Option<VisibleWrapLayout> {
        if !self.document.soft_wrap {
            return None;
        }

        if self.cached_wrap_layout_matches(line_height) {
            return self.document_wrap_layout_cache();
        }

        let visible_rows = self.visible_display_row_range();
        if visible_rows.is_empty() {
            return None;
        }

        let wrap_layout = self
            .document
            .display_snapshot
            .wrap_snapshot()
            .layout_for_rows(visible_rows, self.scroll_offset, line_height);
        (wrap_layout.wrap_column() > 0).then_some(wrap_layout)
    }

    fn cached_wrap_layout_matches(&self, line_height: Pixels) -> bool {
        let visible_rows = self.visible_display_row_range();
        if visible_rows.is_empty() {
            return false;
        }

        let expected_first_offset =
            -((self.scroll_offset - visible_rows.start as f32) * line_height);

        self.cached_layout.line_height() == line_height
            && self
                .cached_layout
                .wrap_layout()
                .map(|layout| {
                    layout.visible_rows() == visible_rows
                        && layout.line_y_offset_for_slot(visible_rows.start)
                            == Some(expected_first_offset)
                })
                .unwrap_or(false)
    }

    fn document_wrap_layout_cache(&self) -> Option<VisibleWrapLayout> {
        self.cached_layout
            .wrap_layout()
            .cloned()
            .filter(|layout| layout.wrap_column() > 0)
    }
}
