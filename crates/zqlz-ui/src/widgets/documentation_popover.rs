use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use gpui::{
    App, Bounds, ElementId, IntoElement, ParentElement, Pixels, Point, RenderOnce, SharedString,
    Size, Styled, Window, div, point, px,
};

use crate::widgets::{
    Anchor, StyledExt as _, clipboard::Clipboard, scroll::ScrollableElement, text::CodeBlock,
    text::TextView,
};

const DEFAULT_GAP: Pixels = px(6.0);
const DEFAULT_MARGIN: Pixels = px(8.0);
const MIN_PREFERRED_VISIBLE_HEIGHT: Pixels = px(220.0);

#[derive(Clone, Copy, Debug)]
pub struct DocumentationPopoverPlacement {
    pub anchor: Anchor,
    pub position: Point<Pixels>,
}

pub fn documentation_popover_placement(
    anchor_bounds: Bounds<Pixels>,
    viewport_size: Size<Pixels>,
    max_height: Pixels,
) -> DocumentationPopoverPlacement {
    let below_position = point(
        anchor_bounds.origin.x,
        anchor_bounds.origin.y + anchor_bounds.size.height + DEFAULT_GAP,
    );
    let above_position = point(anchor_bounds.origin.x, anchor_bounds.origin.y - DEFAULT_GAP);
    let space_below = (viewport_size.height - below_position.y - DEFAULT_MARGIN).max(px(0.0));
    let space_above = (above_position.y - DEFAULT_MARGIN).max(px(0.0));
    let preferred_visible_height = max_height.min(MIN_PREFERRED_VISIBLE_HEIGHT);

    if space_below < preferred_visible_height && space_above > space_below {
        DocumentationPopoverPlacement {
            anchor: Anchor::BottomLeft,
            position: above_position,
        }
    } else {
        DocumentationPopoverPlacement {
            anchor: Anchor::TopLeft,
            position: below_position,
        }
    }
}

pub fn documentation_popover_margin() -> Pixels {
    DEFAULT_MARGIN
}

fn code_block_copy_id(code_block: &CodeBlock) -> ElementId {
    if let Some(span) = code_block.span {
        return ElementId::Name(format!("documentation-copy-{}-{}", span.start, span.end).into());
    }

    let mut hasher = DefaultHasher::new();
    code_block.code().as_ref().hash(&mut hasher);
    code_block
        .lang()
        .as_ref()
        .map(SharedString::as_ref)
        .hash(&mut hasher);

    ElementId::Name(format!("documentation-copy-{:x}", hasher.finish()).into())
}

fn code_block_copy_action(
    code_block: &CodeBlock,
    _window: &mut Window,
    _cx: &mut App,
) -> Clipboard {
    Clipboard::new(code_block_copy_id(code_block)).value(code_block.code())
}

/// A bounded, selectable markdown surface for documentation overlays.
///
/// This wraps [`TextView`] so callers can render rich hover content with
/// selection and scrolling instead of painting static text into a canvas.
#[derive(IntoElement)]
pub struct DocumentationPopover {
    id: ElementId,
    markdown: SharedString,
    max_width: Pixels,
    max_height: Pixels,
}

impl DocumentationPopover {
    /// Create a new documentation popover for the provided markdown content.
    pub fn new(id: impl Into<ElementId>, markdown: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            markdown: markdown.into(),
            max_width: px(520.0),
            max_height: px(360.0),
        }
    }

    /// Set the maximum width of the popover.
    pub fn max_width(mut self, max_width: Pixels) -> Self {
        self.max_width = max_width;
        self
    }

    /// Set the maximum height of the popover.
    pub fn max_height(mut self, max_height: Pixels) -> Self {
        self.max_height = max_height;
        self
    }
}

impl RenderOnce for DocumentationPopover {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let markdown_view_id = ElementId::Name(format!("{}/markdown", self.id).into());

        div()
            .popover_style(cx)
            .max_w(self.max_width)
            .max_h(self.max_height)
            .overflow_hidden()
            .child(
                div()
                    .w_full()
                    .max_h(self.max_height)
                    .overflow_y_scrollbar()
                    .p_3()
                    .child(
                        TextView::markdown(markdown_view_id, self.markdown)
                            .selectable(true)
                            .code_block_actions(code_block_copy_action),
                    ),
            )
    }
}
