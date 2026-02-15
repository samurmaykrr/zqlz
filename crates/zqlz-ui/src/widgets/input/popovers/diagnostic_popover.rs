use std::ops::Range;

use gpui::{
    anchored, deferred, div, point, px, App, Entity, IntoElement, ParentElement as _, Pixels,
    Point, RenderOnce, StatefulInteractiveElement as _, Styled as _, Window,
};

use crate::widgets::{
    highlighter::{DiagnosticEntry, DiagnosticSeverity},
    input::popovers::editor_popover,
    text::{TextView, TextViewState},
    Anchor,
};

const MAX_POPOVER_WIDTH: Pixels = px(500.);
const MAX_POPOVER_HEIGHT: Pixels = px(320.);
const POPOVER_GAP: Pixels = px(4.);

/// Data needed to render a diagnostic popover.
/// This is stored in InputState and used to render the popover inline.
#[derive(Clone)]
pub struct DiagnosticPopoverData {
    /// The diagnostic range in byte offsets
    pub range: Range<usize>,
    /// The diagnostic message
    pub message: String,
    /// The diagnostic severity
    pub severity: DiagnosticSeverity,
}

impl DiagnosticPopoverData {
    pub fn from_entry(entry: &DiagnosticEntry) -> Self {
        Self {
            range: entry.range.clone(),
            message: entry.message.to_string(),
            severity: entry.severity,
        }
    }

    /// Check if this diagnostic data matches the given entry
    pub fn matches(&self, entry: &DiagnosticEntry) -> bool {
        self.range == entry.range
    }
}

/// A diagnostic popover element that renders inline.
/// Uses RenderOnce pattern - no Entity creation on render.
#[derive(IntoElement)]
pub struct DiagnosticPopover {
    data: DiagnosticPopoverData,
    text_view_state: Entity<TextViewState>,
    origin: Point<Pixels>,
}

impl DiagnosticPopover {
    /// Create a new diagnostic popover element for rendering.
    pub fn new(
        data: &DiagnosticPopoverData,
        text_view_state: &Entity<TextViewState>,
        origin: Point<Pixels>,
    ) -> Self {
        Self {
            data: data.clone(),
            text_view_state: text_view_state.clone(),
            origin,
        }
    }
}

impl RenderOnce for DiagnosticPopover {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let (border, bg, fg) = (
            self.data.severity.border(cx),
            self.data.severity.bg(cx),
            self.data.severity.fg(cx),
        );

        let content = editor_popover("diagnostic-popover", cx)
            .w(MAX_POPOVER_WIDTH)
            .min_w(px(200.))
            .max_h(MAX_POPOVER_HEIGHT)
            .overflow_y_scroll()
            .text_xs()
            .px_1()
            .py_0p5()
            .bg(bg)
            .text_color(fg)
            .border_1()
            .border_color(border)
            .child(TextView::new(&self.text_view_state));

        deferred(
            anchored()
                .snap_to_window_with_margin(px(8.))
                .anchor(Anchor::TopLeft.into())
                .position(self.origin)
                .child(div().relative().child(content)),
        )
        .with_priority(1)
    }
}

/// Calculate the origin position for a diagnostic popover.
pub fn diagnostic_popover_origin(
    range_start: usize,
    range_end: usize,
    line_height: Pixels,
    last_bounds_origin: Point<Pixels>,
    start_pos: Point<Pixels>,
    end_pos: Point<Pixels>,
) -> Point<Pixels> {
    point(
        last_bounds_origin.x + start_pos.x,
        last_bounds_origin.y + end_pos.y + line_height + POPOVER_GAP,
    )
}
