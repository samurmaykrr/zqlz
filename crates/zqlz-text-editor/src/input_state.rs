use crate::{
    DisplaySnapshot, EditorCoreSnapshot, Position, SelectionHistoryEntry, SelectionsCollection,
    TextBuffer, VisibleWrapLayout, editor_core,
};
use gpui::{Pixels, ShapedLine, WrappedLine, px};
use std::{ops::Range, sync::Arc};

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct HitTestPoint {
    pub(crate) previous_valid: Position,
    pub(crate) next_valid: Position,
    pub(crate) exact_unclipped: Position,
    pub(crate) column_overshoot_after_line_end: usize,
}

impl HitTestPoint {
    pub(crate) fn position(&self) -> Position {
        self.previous_valid
    }
}

#[derive(Clone, Debug)]
pub(crate) struct EditorPositionMap {
    bounds_origin: gpui::Point<Pixels>,
    bounds_size: gpui::Size<Pixels>,
    gutter_width: Pixels,
    line_height: Pixels,
    char_width: Pixels,
    vertical_scroll_offset: f32,
    horizontal_scroll_offset: f32,
    visible_rows: Range<usize>,
    display_snapshot: DisplaySnapshot,
    wrap_layout: Option<VisibleWrapLayout>,
    shaped_lines: Vec<Arc<ShapedLine>>,
    wrapped_lines: Option<Vec<Arc<WrappedLine>>>,
}

impl EditorPositionMap {
    pub(crate) fn new(
        bounds_origin: gpui::Point<Pixels>,
        bounds_size: gpui::Size<Pixels>,
        gutter_width: Pixels,
        line_height: Pixels,
        char_width: Pixels,
        vertical_scroll_offset: f32,
        horizontal_scroll_offset: f32,
        visible_rows: Range<usize>,
        display_snapshot: DisplaySnapshot,
        wrap_layout: Option<VisibleWrapLayout>,
        shaped_lines: Vec<Arc<ShapedLine>>,
        wrapped_lines: Option<Vec<Arc<WrappedLine>>>,
    ) -> Self {
        Self {
            bounds_origin,
            bounds_size,
            gutter_width,
            line_height,
            char_width,
            vertical_scroll_offset,
            horizontal_scroll_offset,
            visible_rows,
            display_snapshot,
            wrap_layout,
            shaped_lines,
            wrapped_lines,
        }
    }

    pub(crate) fn point_for_position(&self, point: gpui::Point<Pixels>) -> HitTestPoint {
        let relative_y = (point.y - self.bounds_origin.y)
            .max(px(0.0))
            .min(self.bounds_size.height);
        let relative_x = (point.x - self.bounds_origin.x - self.gutter_width).max(px(0.0))
            + self.char_width * self.horizontal_scroll_offset.max(0.0);

        let (display_slot, display_column, overshoot) =
            if let Some(wrap_layout) = self.wrap_layout.as_ref() {
                self.wrapped_point_for_position(relative_x, relative_y, wrap_layout)
            } else {
                self.unwrapped_point_for_position(relative_x, relative_y)
            };

        let buffer_line = self
            .display_snapshot
            .buffer_line_for_display_slot(display_slot)
            .or_else(|| self.display_snapshot.visible_buffer_lines().last().copied())
            .unwrap_or(0);
        let exact_unclipped = self
            .display_snapshot
            .position_for_display_column(buffer_line, display_column + overshoot);
        let previous_valid = self
            .display_snapshot
            .position_for_display_column(buffer_line, display_column);
        let next_valid = self
            .display_snapshot
            .position_for_display_column(buffer_line, display_column.saturating_add(1));

        HitTestPoint {
            previous_valid,
            next_valid,
            exact_unclipped,
            column_overshoot_after_line_end: overshoot,
        }
    }

    fn unwrapped_point_for_position(&self, x: Pixels, y: Pixels) -> (usize, usize, usize) {
        let display_slot = ((y / self.line_height) + self.vertical_scroll_offset)
            .floor()
            .max(0.0) as usize;
        let visible_index = display_slot.saturating_sub(self.visible_rows.start);
        let (column, overshoot) = self
            .shaped_lines
            .get(visible_index)
            .map(|line| self.column_and_overshoot_for_x(line.as_ref(), x))
            .unwrap_or_else(|| self.fallback_column_for_x(x));
        (display_slot, column, overshoot)
    }

    fn wrapped_point_for_position(
        &self,
        x: Pixels,
        y: Pixels,
        wrap_layout: &VisibleWrapLayout,
    ) -> (usize, usize, usize) {
        let Some((display_slot, wrap_subrow)) =
            wrap_layout.display_slot_and_subrow_for_y(y, self.line_height)
        else {
            return self.unwrapped_point_for_position(x, y);
        };
        let visible_index = display_slot.saturating_sub(wrap_layout.visible_rows().start);
        let wrap_column = wrap_layout.wrap_column().max(1);
        let shaped_column = self
            .wrapped_lines
            .as_ref()
            .and_then(|lines| lines.get(visible_index))
            .map(|line| {
                let position = gpui::point(x, self.line_height * wrap_subrow as f32);
                line.closest_index_for_position(position, self.line_height)
                    .unwrap_or_else(|index| index)
            })
            .unwrap_or_else(|| self.fallback_column_for_x(x).0);
        let fallback_column = wrap_subrow
            .saturating_mul(wrap_column)
            .saturating_add(self.fallback_column_for_x(x).0);
        let display_column = if self.wrapped_lines.is_some() {
            shaped_column
        } else {
            fallback_column
        };
        (display_slot, display_column, 0)
    }

    fn column_and_overshoot_for_x(&self, line: &ShapedLine, x: Pixels) -> (usize, usize) {
        if let Some(column) = line.index_for_x(x) {
            (column, 0)
        } else {
            let overshoot = if self.char_width > px(0.0) {
                ((x - line.width()).max(px(0.0)) / self.char_width) as usize
            } else {
                0
            };
            (line.len(), overshoot)
        }
    }

    fn fallback_column_for_x(&self, x: Pixels) -> (usize, usize) {
        if self.char_width > px(0.0) {
            ((x / self.char_width).max(0.0) as usize, 0)
        } else {
            (0, 0)
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CachedEditorLayout {
    gutter_width: f32,
    char_width: gpui::Pixels,
    bounds_origin: gpui::Point<gpui::Pixels>,
    bounds_size: gpui::Size<gpui::Pixels>,
    line_height: gpui::Pixels,
    wrap_layout: Option<VisibleWrapLayout>,
    position_map: Option<EditorPositionMap>,
}

impl Default for CachedEditorLayout {
    fn default() -> Self {
        Self {
            gutter_width: 0.0,
            char_width: gpui::px(0.0),
            bounds_origin: gpui::Point::default(),
            bounds_size: gpui::Size::default(),
            line_height: gpui::px(0.0),
            wrap_layout: None,
            position_map: None,
        }
    }
}

impl CachedEditorLayout {
    pub(crate) fn new(
        gutter_width: f32,
        char_width: gpui::Pixels,
        bounds_origin: gpui::Point<gpui::Pixels>,
        bounds_size: gpui::Size<gpui::Pixels>,
        line_height: gpui::Pixels,
        wrap_layout: Option<VisibleWrapLayout>,
    ) -> Self {
        Self {
            gutter_width,
            char_width,
            bounds_origin,
            bounds_size,
            line_height,
            wrap_layout,
            position_map: None,
        }
    }

    pub(crate) fn gutter_width(&self) -> f32 {
        self.gutter_width
    }

    pub(crate) fn char_width(&self) -> gpui::Pixels {
        self.char_width
    }

    pub(crate) fn bounds_origin(&self) -> gpui::Point<gpui::Pixels> {
        self.bounds_origin
    }

    pub(crate) fn bounds_size(&self) -> gpui::Size<gpui::Pixels> {
        self.bounds_size
    }

    pub(crate) fn line_height(&self) -> gpui::Pixels {
        self.line_height
    }

    pub(crate) fn wrap_layout(&self) -> Option<&VisibleWrapLayout> {
        self.wrap_layout.as_ref()
    }

    pub(crate) fn position_map(&self) -> Option<&EditorPositionMap> {
        self.position_map.as_ref()
    }

    pub(crate) fn set_position_map(&mut self, position_map: EditorPositionMap) {
        self.position_map = Some(position_map);
    }

    #[cfg(test)]
    pub(crate) fn set_line_height(&mut self, line_height: gpui::Pixels) {
        self.line_height = line_height;
    }

    #[cfg(test)]
    pub(crate) fn set_wrap_layout(&mut self, wrap_layout: Option<VisibleWrapLayout>) {
        self.wrap_layout = wrap_layout;
    }
}

#[derive(Default)]
pub(crate) struct EditorInputState {
    mouse_drag_anchor: Option<Position>,
    cached_layout: CachedEditorLayout,
    cached_fold_chevrons: Vec<(usize, gpui::Bounds<gpui::Pixels>)>,
    last_click: Option<(std::time::Instant, Position)>,
    ime_marked_range: Option<std::ops::Range<usize>>,
    last_select_line_was_extend: bool,
    clipboard_is_whole_line: bool,
    selection_history: Vec<SelectionHistoryEntry>,
}

impl EditorInputState {
    pub(crate) fn clear_selection_history_extension(&mut self) {
        self.last_select_line_was_extend = false;
        self.selection_history.clear();
    }

    pub(crate) fn reset_document_bound_state(&mut self) {
        self.clear_selection_history_extension();
        self.finish_text_replacement();
    }

    pub(crate) fn editor_core_snapshot<'a>(
        &self,
        buffer: &'a TextBuffer,
        selections_collection: SelectionsCollection,
    ) -> EditorCoreSnapshot<'a> {
        EditorCoreSnapshot::new(
            buffer,
            selections_collection,
            self.last_select_line_was_extend,
            self.selection_history.clone(),
        )
    }

    pub(crate) fn editor_core<'a>(
        &'a mut self,
        buffer: &'a TextBuffer,
        selections_collection: &'a mut SelectionsCollection,
    ) -> editor_core::EditorCore<'a> {
        editor_core::EditorCore::from_collection(
            buffer,
            selections_collection,
            &mut self.last_select_line_was_extend,
            &mut self.selection_history,
        )
    }

    pub(crate) fn update_cached_layout(&mut self, layout: CachedEditorLayout) {
        self.cached_layout = layout;
    }

    pub(crate) fn update_cached_fold_chevrons(
        &mut self,
        chevrons: Vec<(usize, gpui::Bounds<gpui::Pixels>)>,
    ) {
        self.cached_fold_chevrons = chevrons;
    }

    pub(crate) fn fold_chevron_line_at(&self, point: gpui::Point<gpui::Pixels>) -> Option<usize> {
        self.cached_fold_chevrons
            .iter()
            .find_map(|(start_line, rect)| rect.contains(&point).then_some(*start_line))
    }

    pub(crate) fn cached_layout(&self) -> &CachedEditorLayout {
        &self.cached_layout
    }

    pub(crate) fn editor_bounds(&self) -> gpui::Bounds<gpui::Pixels> {
        gpui::Bounds::new(
            self.cached_layout.bounds_origin,
            self.cached_layout.bounds_size,
        )
    }

    pub(crate) fn line_height_or(&self, default: gpui::Pixels) -> gpui::Pixels {
        self.cached_layout.line_height.max(default)
    }

    pub(crate) fn fallback_editor_text_anchor(&self) -> gpui::Point<gpui::Pixels> {
        gpui::point(
            self.cached_layout.bounds_origin.x + gpui::px(self.cached_layout.gutter_width),
            self.cached_layout.bounds_origin.y,
        )
    }

    pub(crate) fn window_point_to_local(&self, point: gpui::Point<gpui::Pixels>) -> (f32, f32) {
        (
            f32::from(point.x - self.cached_layout.bounds_origin.x),
            f32::from(point.y - self.cached_layout.bounds_origin.y),
        )
    }

    pub(crate) fn point_is_in_gutter(&self, point: gpui::Point<gpui::Pixels>) -> bool {
        point.x - self.cached_layout.bounds_origin.x < gpui::px(self.cached_layout.gutter_width)
    }

    pub(crate) fn viewport_vertical_bounds(&self) -> (gpui::Pixels, gpui::Pixels) {
        let top = self.cached_layout.bounds_origin.y;
        (top, top + self.cached_layout.bounds_size.height)
    }

    pub(crate) fn viewport_columns(&self) -> usize {
        if self.cached_layout.char_width <= gpui::px(0.0) {
            return 1;
        }

        ((self.cached_layout.bounds_size.width - gpui::px(self.cached_layout.gutter_width))
            / self.cached_layout.char_width)
            .floor()
            .max(1.0) as usize
    }

    pub(crate) fn clear_text_composition(&mut self) {
        self.ime_marked_range = None;
    }

    pub(crate) fn finish_text_replacement(&mut self) {
        self.clear_text_composition();
    }

    pub(crate) fn ime_marked_range(&self) -> Option<std::ops::Range<usize>> {
        self.ime_marked_range.clone()
    }

    pub(crate) fn has_ime_marked_range(&self) -> bool {
        self.ime_marked_range.is_some()
    }

    pub(crate) fn update_text_composition_range(&mut self, range: Option<std::ops::Range<usize>>) {
        self.ime_marked_range = range;
    }

    pub(crate) fn record_whole_line_clipboard_write(&mut self) {
        self.clipboard_is_whole_line = true;
    }

    pub(crate) fn record_selection_clipboard_write(&mut self) {
        self.clipboard_is_whole_line = false;
    }

    pub(crate) fn clipboard_contains_whole_line(&self) -> bool {
        self.clipboard_is_whole_line
    }

    pub(crate) fn record_click(
        &mut self,
        now: std::time::Instant,
        position: Position,
        click_count: usize,
    ) -> usize {
        let resolved_click_count = match &self.last_click {
            Some((last_time, last_position))
                if *last_position == position
                    && now
                        .checked_duration_since(*last_time)
                        .is_some_and(|elapsed| elapsed < std::time::Duration::from_millis(500)) =>
            {
                click_count.min(3)
            }
            _ => 1,
        };
        self.last_click = Some((now, position));
        resolved_click_count
    }

    pub(crate) fn resolve_mouse_selection_click(
        &mut self,
        now: std::time::Instant,
        position: Position,
        click_count: usize,
    ) -> usize {
        self.record_click(now, position, click_count)
    }

    pub(crate) fn begin_mouse_selection_drag(&mut self, anchor: Position) {
        self.mouse_drag_anchor = Some(anchor);
    }

    pub(crate) fn mouse_selection_drag_anchor(&self) -> Option<Position> {
        self.mouse_drag_anchor
    }

    pub(crate) fn finish_mouse_selection_drag(&mut self) {
        self.mouse_drag_anchor = None;
    }
}

#[cfg(test)]
mod tests {
    use super::{EditorInputState, EditorPositionMap};
    use crate::{
        CachedEditorLayout, Cursor, DisplayMap, Position, Selection, SelectionHistoryEntry,
        SelectionsCollection,
    };
    use gpui::{point, px, size};

    #[test]
    fn clear_selection_history_extension_resets_line_extension_state() {
        let mut state = EditorInputState {
            last_select_line_was_extend: true,
            selection_history: vec![SelectionHistoryEntry::new(SelectionsCollection::single(
                Cursor::at(Position::new(1, 2)),
                Selection::from_anchor_head(Position::new(1, 0), Position::new(1, 2)),
            ))],
            ..Default::default()
        };

        state.clear_selection_history_extension();

        assert!(!state.last_select_line_was_extend);
        assert!(state.selection_history.is_empty());
    }

    #[test]
    fn cached_layout_update_replaces_previous_geometry() {
        let mut state = EditorInputState::default();
        let layout = CachedEditorLayout::new(
            42.0,
            px(9.0),
            point(px(1.0), px(2.0)),
            size(px(300.0), px(200.0)),
            px(18.0),
            None,
        );

        state.update_cached_layout(layout.clone());

        assert_eq!(state.cached_layout().gutter_width(), 42.0);
        assert_eq!(state.cached_layout().char_width(), px(9.0));
        assert_eq!(
            state.cached_layout().bounds_origin(),
            layout.bounds_origin()
        );
        assert_eq!(state.cached_layout().bounds_size(), layout.bounds_size());
        assert_eq!(state.cached_layout().line_height(), px(18.0));
        assert_eq!(state.cached_layout().gutter_width(), 42.0);
        assert_eq!(state.viewport_columns(), 28);
        assert_eq!(
            state.editor_bounds(),
            gpui::Bounds::new(point(px(1.0), px(2.0)), size(px(300.0), px(200.0)))
        );
        assert_eq!(state.line_height_or(px(20.0)), px(20.0));
        assert_eq!(
            state.fallback_editor_text_anchor(),
            point(px(43.0), px(2.0))
        );
        assert_eq!(
            state.window_point_to_local(point(px(11.0), px(22.0))),
            (10.0, 20.0)
        );
        assert!(state.point_is_in_gutter(point(px(20.0), px(2.0))));
        assert_eq!(state.viewport_vertical_bounds(), (px(2.0), px(202.0)));
    }

    #[test]
    fn position_map_uses_current_scroll_offsets_for_hit_testing() {
        let buffer = crate::TextBuffer::new("zero\none\ntwo\nthree");
        let display_snapshot = DisplayMap::from_display_lines(vec![0, 1, 2, 3]).snapshot(
            buffer.snapshot(),
            &[],
            &Default::default(),
            false,
            Default::default(),
            &[],
            Default::default(),
            &[],
        );
        let position_map = EditorPositionMap::new(
            point(px(10.0), px(20.0)),
            size(px(300.0), px(80.0)),
            px(30.0),
            px(20.0),
            px(10.0),
            2.0,
            3.0,
            2..4,
            display_snapshot,
            None,
            Vec::new(),
            None,
        );

        let hit = position_map.point_for_position(point(px(50.0), px(25.0)));

        assert_eq!(hit.previous_valid, Position::new(2, 4));
    }

    #[test]
    fn fold_chevron_cache_resolves_window_points() {
        let mut state = EditorInputState::default();
        state.update_cached_fold_chevrons(vec![(
            7,
            gpui::Bounds::new(point(px(10.0), px(20.0)), size(px(16.0), px(16.0))),
        )]);

        assert_eq!(
            state.fold_chevron_line_at(point(px(15.0), px(25.0))),
            Some(7)
        );
        assert_eq!(state.fold_chevron_line_at(point(px(40.0), px(25.0))), None);

        state.update_cached_fold_chevrons(Vec::new());

        assert_eq!(state.fold_chevron_line_at(point(px(15.0), px(25.0))), None);
    }

    #[test]
    fn ime_and_clipboard_helpers_track_modes() {
        let mut state = EditorInputState {
            ime_marked_range: Some(2..5),
            ..Default::default()
        };

        state.clear_text_composition();
        assert!(state.ime_marked_range().is_none());

        state.update_text_composition_range(Some(1..3));
        assert_eq!(state.ime_marked_range(), Some(1..3));
        assert!(state.has_ime_marked_range());

        state.record_whole_line_clipboard_write();
        assert!(state.clipboard_contains_whole_line());

        state.record_selection_clipboard_write();
        assert!(!state.clipboard_contains_whole_line());
    }

    #[test]
    fn click_and_drag_helpers_track_mouse_memory() {
        let mut state = EditorInputState::default();
        let now = std::time::Instant::now();
        let position = Position::new(2, 3);

        assert_eq!(state.record_click(now, position, 1), 1);
        assert_eq!(state.record_click(now, position, 2), 2);
        assert_eq!(
            state.record_click(now + std::time::Duration::from_millis(500), position, 3),
            1
        );

        state.begin_mouse_selection_drag(position);
        assert_eq!(state.mouse_selection_drag_anchor(), Some(position));

        state.finish_mouse_selection_drag();
        assert!(state.mouse_selection_drag_anchor().is_none());
    }
}
