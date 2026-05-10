use gpui::{Bounds, Pixels, Point};

use crate::buffer::{Anchor, Bias};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ScrollAnchor {
    anchor: Anchor,
    visual_offset: f32,
}

impl ScrollAnchor {
    pub(crate) fn top() -> Self {
        Self {
            anchor: Anchor::new(0, 0, Bias::Left),
            visual_offset: 0.0,
        }
    }
}

/// Scrollbar geometry cached from the last paint pass.
///
/// Stored in window coordinates so that mouse handlers can hit-test without
/// touching any element-layer state.
pub(crate) struct CachedScrollbarBounds {
    track: Bounds<Pixels>,
    thumb: Bounds<Pixels>,
    display_line_count: usize,
}

impl CachedScrollbarBounds {
    pub(crate) fn new(
        track: Bounds<Pixels>,
        thumb: Bounds<Pixels>,
        display_line_count: usize,
    ) -> Self {
        Self {
            track,
            thumb,
            display_line_count,
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum ScrollbarPointerDown {
    BeginDrag,
    JumpToOffset(f32),
}

pub(crate) struct EditorScrollState {
    vertical_offset: f32,
    anchor: ScrollAnchor,
    horizontal_offset: f32,
    last_viewport_lines: usize,
    cached_scrollbar: Option<CachedScrollbarBounds>,
    scrollbar_drag_start: Option<(Pixels, f32)>,
    autoscroll_on_clicks: bool,
    vertical_margin: usize,
    horizontal_margin: usize,
    sensitivity: f32,
    beyond_last_line: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ScrollSnapshotState {
    anchor: Anchor,
    anchor_visual_offset: f32,
    vertical_offset: f32,
    horizontal_offset: f32,
    viewport_lines: usize,
}

impl ScrollSnapshotState {
    pub(crate) fn anchor(&self) -> Anchor {
        self.anchor
    }

    pub(crate) fn anchor_visual_offset(&self) -> f32 {
        self.anchor_visual_offset
    }

    pub(crate) fn vertical_offset(&self) -> f32 {
        self.vertical_offset
    }

    pub(crate) fn horizontal_offset(&self) -> f32 {
        self.horizontal_offset
    }

    pub(crate) fn viewport_lines(&self) -> usize {
        self.viewport_lines
    }
}

impl Default for EditorScrollState {
    fn default() -> Self {
        Self {
            vertical_offset: 0.0,
            anchor: ScrollAnchor::top(),
            horizontal_offset: 0.0,
            last_viewport_lines: 20,
            cached_scrollbar: None,
            scrollbar_drag_start: None,
            autoscroll_on_clicks: true,
            vertical_margin: 3,
            horizontal_margin: 3,
            sensitivity: 1.0,
            beyond_last_line: false,
        }
    }
}

impl EditorScrollState {
    pub(crate) fn reset_offsets(&mut self) {
        self.vertical_offset = 0.0;
        self.anchor = ScrollAnchor::top();
        self.horizontal_offset = 0.0;
    }

    pub(crate) fn vertical_offset(&self) -> f32 {
        self.vertical_offset
    }

    pub(crate) fn horizontal_offset(&self) -> f32 {
        self.horizontal_offset
    }

    pub(crate) fn viewport_lines(&self) -> usize {
        self.last_viewport_lines
    }

    pub(crate) fn viewport_lines_or_one(&self) -> usize {
        self.last_viewport_lines.max(1)
    }

    pub(crate) fn snapshot_state(&self) -> ScrollSnapshotState {
        ScrollSnapshotState {
            anchor: self.anchor.anchor,
            anchor_visual_offset: self.anchor.visual_offset,
            vertical_offset: self.vertical_offset,
            horizontal_offset: self.horizontal_offset,
            viewport_lines: self.last_viewport_lines,
        }
    }

    pub(crate) fn autoscroll_on_clicks(&self) -> bool {
        self.autoscroll_on_clicks
    }

    pub(crate) fn vertical_margin(&self) -> usize {
        self.vertical_margin
    }

    pub(crate) fn sensitivity(&self) -> f32 {
        self.sensitivity
    }

    pub(crate) fn set_autoscroll_on_clicks(&mut self, enabled: bool) {
        self.autoscroll_on_clicks = enabled;
    }

    pub(crate) fn set_horizontal_margin(&mut self, margin: usize) {
        self.horizontal_margin = margin;
    }

    pub(crate) fn set_vertical_margin(&mut self, margin: usize) {
        self.vertical_margin = margin;
    }

    pub(crate) fn set_sensitivity(&mut self, sensitivity: f32) {
        self.sensitivity = sensitivity.max(0.1);
    }

    pub(crate) fn set_beyond_last_line(&mut self, enabled: bool) {
        self.beyond_last_line = enabled;
    }

    pub(crate) fn set_vertical_offset(&mut self, offset: f32, max_offset: f32) {
        self.vertical_offset = offset.clamp(0.0, max_offset);
    }

    pub(crate) fn max_vertical_offset(&self, display_line_count: usize) -> f32 {
        let display_line_count = display_line_count as f32;
        if self.beyond_last_line {
            display_line_count
        } else {
            (display_line_count - self.last_viewport_lines as f32).max(0.0)
        }
    }

    pub(crate) fn scroll_by(&mut self, delta: f32, max_offset: f32) {
        self.set_vertical_offset(self.vertical_offset + delta, max_offset);
    }

    pub(crate) fn clamp_vertical_offset(&mut self, max_offset: f32) {
        self.set_vertical_offset(self.vertical_offset, max_offset);
    }

    pub(crate) fn ensure_display_row_visible(&mut self, display_row: f32) {
        let viewport_lines = self.last_viewport_lines;
        let margin = self.vertical_margin.min(viewport_lines.saturating_div(2)) as f32;
        let visible_start = self.vertical_offset + margin;
        let visible_end = self.vertical_offset + viewport_lines as f32 - margin;

        if display_row < visible_start {
            self.set_vertical_offset(display_row - margin, f32::MAX);
        } else if display_row >= visible_end {
            self.set_vertical_offset(display_row - viewport_lines as f32 + margin + 1.0, f32::MAX);
        }
    }

    pub(crate) fn set_horizontal_offset(&mut self, offset: f32) {
        self.horizontal_offset = offset.max(0.0);
    }

    pub(crate) fn ensure_column_visible(&mut self, cursor_column: f32, viewport_columns: usize) {
        let margin = self
            .horizontal_margin
            .min(viewport_columns.saturating_div(2)) as f32;
        let visible_start = self.horizontal_offset + margin;
        let visible_end = self.horizontal_offset + viewport_columns as f32 - margin;

        if cursor_column < visible_start {
            self.set_horizontal_offset(cursor_column - margin);
        } else if cursor_column >= visible_end {
            self.set_horizontal_offset(cursor_column - viewport_columns as f32 + margin + 1.0);
        }
    }

    pub(crate) fn set_anchor(&mut self, anchor: Anchor, visual_offset: f32) {
        self.anchor = ScrollAnchor {
            anchor,
            visual_offset,
        };
    }

    pub(crate) fn set_viewport_lines(&mut self, viewport_lines: usize) {
        self.last_viewport_lines = viewport_lines;
    }

    pub(crate) fn update_cached_scrollbar(&mut self, scrollbar: Option<CachedScrollbarBounds>) {
        self.cached_scrollbar = scrollbar;
    }

    pub(crate) fn begin_scrollbar_drag(&mut self, pointer_y: Pixels) {
        self.scrollbar_drag_start = Some((pointer_y, self.vertical_offset));
    }

    pub(crate) fn clear_scrollbar_drag(&mut self) {
        self.scrollbar_drag_start = None;
    }

    pub(crate) fn scrollbar_pointer_down(
        &mut self,
        pointer: Point<Pixels>,
    ) -> Option<ScrollbarPointerDown> {
        let scrollbar = self.cached_scrollbar.as_ref()?;
        if !scrollbar.track.contains(&pointer) {
            return None;
        }

        if scrollbar.thumb.contains(&pointer) {
            self.begin_scrollbar_drag(pointer.y);
            return Some(ScrollbarPointerDown::BeginDrag);
        }

        Some(ScrollbarPointerDown::JumpToOffset(
            Self::scrollbar_offset_for_track_pointer(
                scrollbar,
                pointer.y,
                self.last_viewport_lines,
            ),
        ))
    }

    pub(crate) fn scrollbar_drag_offset(&self, pointer_y: Pixels) -> Option<f32> {
        let (drag_start_y, offset_at_drag_start) = self.scrollbar_drag_start?;
        let scrollbar = self.cached_scrollbar.as_ref()?;
        let scrollable_track_height = Self::scrollbar_scrollable_track_height(scrollbar);
        let max_offset = Self::scrollbar_max_offset(scrollbar, self.last_viewport_lines);
        if scrollable_track_height <= 0.0 || max_offset <= 0.0 {
            return Some(0.0);
        }

        let delta_y = f32::from(pointer_y - drag_start_y);
        let line_delta = delta_y / scrollable_track_height * max_offset;
        Some(offset_at_drag_start + line_delta)
    }

    fn scrollbar_offset_for_track_pointer(
        scrollbar: &CachedScrollbarBounds,
        pointer_y: Pixels,
        viewport_lines: usize,
    ) -> f32 {
        let scrollable_track_height = Self::scrollbar_scrollable_track_height(scrollbar);
        let max_offset = Self::scrollbar_max_offset(scrollbar, viewport_lines);
        if scrollable_track_height <= 0.0 || max_offset <= 0.0 {
            return 0.0;
        }

        let thumb_height = f32::from(scrollbar.thumb.size.height);
        let click_y = f32::from(pointer_y - scrollbar.track.origin.y) - thumb_height / 2.0;
        let click_fraction = (click_y / scrollable_track_height).clamp(0.0, 1.0);
        click_fraction * max_offset
    }

    fn scrollbar_scrollable_track_height(scrollbar: &CachedScrollbarBounds) -> f32 {
        let track_height = f32::from(scrollbar.track.size.height);
        let thumb_height = f32::from(scrollbar.thumb.size.height);
        (track_height - thumb_height).max(0.0)
    }

    fn scrollbar_max_offset(scrollbar: &CachedScrollbarBounds, viewport_lines: usize) -> f32 {
        scrollbar.display_line_count.saturating_sub(viewport_lines) as f32
    }
}

#[cfg(test)]
mod tests {
    use super::{CachedScrollbarBounds, EditorScrollState, ScrollAnchor, ScrollbarPointerDown};
    use gpui::{Bounds, point, px, size};

    #[test]
    fn scroll_state_defaults_match_editor_contract() {
        let state = EditorScrollState::default();

        assert_eq!(state.vertical_offset(), 0.0);
        assert_eq!(state.horizontal_offset(), 0.0);
        assert_eq!(state.snapshot_state().anchor(), ScrollAnchor::top().anchor);
        assert_eq!(state.viewport_lines(), 20);
        assert_eq!(state.viewport_lines_or_one(), 20);
        assert!(state.autoscroll_on_clicks());
        assert_eq!(state.vertical_margin(), 3);
        assert_eq!(state.horizontal_margin, 3);
        assert_eq!(state.sensitivity(), 1.0);
        assert!(!state.beyond_last_line);
    }

    #[test]
    fn scroll_state_helpers_clamp_offsets_and_settings() {
        let mut state = EditorScrollState::default();

        state.set_vertical_offset(15.0, 10.0);
        state.set_horizontal_offset(-4.0);
        state.set_sensitivity(0.0);
        state.set_viewport_lines(0);

        assert_eq!(state.vertical_offset(), 10.0);
        assert_eq!(state.horizontal_offset(), 0.0);
        assert_eq!(state.sensitivity(), 0.1);
        assert_eq!(state.viewport_lines_or_one(), 1);

        state.reset_offsets();
        assert_eq!(state.vertical_offset(), 0.0);
        assert_eq!(state.horizontal_offset(), 0.0);
        assert_eq!(state.snapshot_state().anchor(), ScrollAnchor::top().anchor);
    }

    #[test]
    fn scroll_snapshot_state_collects_editor_snapshot_inputs() {
        let mut state = EditorScrollState::default();
        let anchor = crate::buffer::Anchor::new(4, 7, crate::buffer::Bias::Right);
        state.set_vertical_offset(6.0, 20.0);
        state.set_horizontal_offset(3.0);
        state.set_anchor(anchor, 1.5);
        state.set_viewport_lines(42);

        let snapshot = state.snapshot_state();

        assert_eq!(snapshot.anchor(), anchor);
        assert_eq!(snapshot.anchor_visual_offset(), 1.5);
        assert_eq!(snapshot.vertical_offset, 6.0);
        assert_eq!(snapshot.horizontal_offset, 3.0);
        assert_eq!(snapshot.viewport_lines, 42);
    }

    #[test]
    fn scroll_state_helpers_manage_vertical_visibility() {
        let mut state = EditorScrollState::default();

        assert_eq!(state.max_vertical_offset(100), 80.0);
        state.set_beyond_last_line(true);
        assert_eq!(state.max_vertical_offset(100), 100.0);

        state.set_beyond_last_line(false);
        state.scroll_by(50.0, state.max_vertical_offset(100));
        assert_eq!(state.vertical_offset(), 50.0);

        state.ensure_display_row_visible(10.0);
        assert_eq!(state.vertical_offset(), 7.0);

        state.ensure_display_row_visible(40.0);
        assert_eq!(state.vertical_offset(), 24.0);

        state.clamp_vertical_offset(12.0);
        assert_eq!(state.vertical_offset(), 12.0);
    }

    #[test]
    fn scroll_state_helpers_manage_horizontal_visibility() {
        let mut state = EditorScrollState::default();

        state.ensure_column_visible(20.0, 10);
        assert_eq!(state.horizontal_offset(), 14.0);

        state.ensure_column_visible(2.0, 10);
        assert_eq!(state.horizontal_offset(), 0.0);
    }

    #[test]
    fn scrollbar_cache_and_drag_helpers_track_lifecycle() {
        let mut state = EditorScrollState::default();
        let bounds = Bounds::new(point(px(0.0), px(0.0)), size(px(10.0), px(50.0)));

        state.update_cached_scrollbar(Some(CachedScrollbarBounds::new(bounds, bounds, 100)));
        assert!(state.cached_scrollbar.is_some());

        state.set_vertical_offset(7.0, 100.0);
        state.begin_scrollbar_drag(px(12.0));
        assert_eq!(state.scrollbar_drag_start, Some((px(12.0), 7.0)));

        state.clear_scrollbar_drag();
        assert!(state.scrollbar_drag_start.is_none());
    }

    #[test]
    fn scrollbar_pointer_helpers_begin_drag_and_jump_to_track_position() {
        let mut state = EditorScrollState::default();
        state.set_viewport_lines(20);
        state.update_cached_scrollbar(Some(CachedScrollbarBounds::new(
            Bounds::new(point(px(0.0), px(0.0)), size(px(10.0), px(100.0))),
            Bounds::new(point(px(0.0), px(20.0)), size(px(10.0), px(20.0))),
            100,
        )));
        state.set_vertical_offset(10.0, 80.0);

        assert_eq!(
            state.scrollbar_pointer_down(point(px(5.0), px(25.0))),
            Some(ScrollbarPointerDown::BeginDrag)
        );
        assert_eq!(state.scrollbar_drag_start, Some((px(25.0), 10.0)));

        assert_eq!(
            state.scrollbar_pointer_down(point(px(5.0), px(90.0))),
            Some(ScrollbarPointerDown::JumpToOffset(80.0))
        );
        assert_eq!(state.scrollbar_drag_offset(px(65.0)), Some(50.0));
        assert_eq!(
            state.scrollbar_pointer_down(point(px(20.0), px(90.0))),
            None
        );
    }
}
