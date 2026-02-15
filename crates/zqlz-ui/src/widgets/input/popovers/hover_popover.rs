use std::ops::Range;

use gpui::{
    anchored, deferred, div, point, px, App, Entity, IntoElement, ParentElement as _, Pixels,
    Point, RenderOnce, StatefulInteractiveElement as _, Styled, Window,
};

use crate::widgets::{
    input::popovers::editor_popover,
    text::{TextView, TextViewState},
    Anchor,
};

const MAX_POPOVER_WIDTH: Pixels = px(500.);
const MAX_POPOVER_HEIGHT: Pixels = px(320.);
const POPOVER_GAP: Pixels = px(4.);

/// Data needed to render a hover popover.
/// This is stored in InputState and used to render the popover inline.
#[derive(Clone)]
pub struct HoverPopoverData {
    /// The symbol range byte of the hover trigger.
    pub symbol_range: Range<usize>,
    /// The hover content (markdown text)
    pub content: String,
}

impl HoverPopoverData {
    pub fn new(symbol_range: Range<usize>, hover: &lsp_types::Hover) -> Self {
        Self {
            symbol_range,
            content: Self::extract_hover_contents(hover),
        }
    }

    /// Check if the given offset is within the symbol range
    pub fn contains_offset(&self, offset: usize) -> bool {
        self.symbol_range.contains(&offset)
    }

    /// Extract content string from lsp_types::Hover
    fn extract_hover_contents(hover: &lsp_types::Hover) -> String {
        match &hover.contents {
            lsp_types::HoverContents::Scalar(scalar) => match scalar {
                lsp_types::MarkedString::String(s) => s.clone(),
                lsp_types::MarkedString::LanguageString(ls) => ls.value.clone(),
            },
            lsp_types::HoverContents::Array(arr) => arr
                .iter()
                .map(|item| match item {
                    lsp_types::MarkedString::String(s) => s.clone(),
                    lsp_types::MarkedString::LanguageString(ls) => ls.value.clone(),
                })
                .collect::<Vec<_>>()
                .join("\n\n"),
            lsp_types::HoverContents::Markup(markup) => markup.value.clone(),
        }
    }
}

/// A hover popover element that renders inline.
/// Uses RenderOnce pattern - no Entity creation on render.
#[derive(IntoElement)]
pub struct HoverPopover {
    text_view_state: Entity<TextViewState>,
    origin: Point<Pixels>,
}

impl HoverPopover {
    /// Create a new hover popover element for rendering.
    pub fn new(text_view_state: &Entity<TextViewState>, origin: Point<Pixels>) -> Self {
        Self {
            text_view_state: text_view_state.clone(),
            origin,
        }
    }
}

impl RenderOnce for HoverPopover {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let content = editor_popover("hover-popover", cx)
            .w(MAX_POPOVER_WIDTH)
            .min_w(px(200.))
            .max_h(MAX_POPOVER_HEIGHT)
            .overflow_y_scroll()
            .text_xs()
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

/// Calculate the origin position for a hover popover.
pub fn hover_popover_origin(
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
