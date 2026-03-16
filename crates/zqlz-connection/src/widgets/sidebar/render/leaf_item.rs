//! Leaf item rendering
//!
//! Renders terminal nodes in the tree hierarchy (tables, views, functions, etc.).

use gpui::*;

use super::LeafItemProps;
use crate::widgets::sidebar::ConnectionSidebar;
use zqlz_ui::widgets::h_flex;

impl ConnectionSidebar {
    /// Render a leaf item in the tree (a single table, view, function, etc.).
    ///
    /// Leaf items are the terminal nodes in the tree hierarchy representing
    /// individual database objects. They support:
    /// - Single click to open/view the object
    /// - Right click to show context menu with object-specific actions
    /// - Hover highlighting for better UX
    ///
    /// # Visual Structure
    ///
    /// ```text
    ///     [Icon] object_name            # Indented based on depth
    /// ```
    ///
    /// # Parameters
    ///
    /// - `element_id`: Unique ID for this leaf element
    /// - `icon`: Icon representing the object type
    /// - `label`: Object name to display
    /// - `on_click`: Callback for left click (typically opens the object)
    /// - `on_right_click`: Optional callback for right click (shows context menu)
    /// - `list_hover`: Theme color for hover state
    /// - `depth`: Indentation level (affects left padding)
    /// - `cx`: App context
    ///
    /// # Indentation
    ///
    /// Like section headers, indentation is `8 + depth * 12` pixels.
    /// Leaf items typically render at `depth + 1` relative to their section header.
    pub(super) fn render_leaf_item<Icon, OnClick, OnRightClick>(
        props: LeafItemProps<Icon, OnClick, OnRightClick>,
        cx: &mut Context<Self>,
    ) -> Stateful<Div>
    where
        Icon: Into<AnyElement>,
        OnClick: for<'a, 'b, 'c, 'd> Fn(
                &'a mut Self,
                &'b ClickEvent,
                &'c mut Window,
                &'d mut Context<Self>,
            ) + 'static,
        OnRightClick: for<'a, 'b, 'c, 'd> Fn(
                &'a mut Self,
                &'b MouseDownEvent,
                &'c mut Window,
                &'d mut Context<Self>,
            ) + 'static,
    {
        let LeafItemProps {
            element_id,
            icon,
            label,
            on_click,
            on_right_click,
            list_hover,
            depth,
        } = props;
        let indent = px(8.0 + depth as f32 * 12.0);

        let row = h_flex()
            .id(element_id)
            .w_full()
            .pl(indent)
            .pr_2()
            .h(px(26.0))
            .gap_1p5()
            .items_center()
            .cursor_pointer()
            .text_sm()
            .hover(|el| el.bg(list_hover))
            .on_click(cx.listener(on_click))
            .child(icon.into())
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .text_ellipsis()
                    .whitespace_nowrap()
                    .child(label),
            );

        if let Some(handler) = on_right_click {
            row.on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    cx.stop_propagation();
                    handler(this, event, window, cx);
                }),
            )
        } else {
            row
        }
    }
}
