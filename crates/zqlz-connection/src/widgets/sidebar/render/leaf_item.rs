//! Leaf item rendering
//!
//! Renders terminal nodes in the tree hierarchy (tables, views, functions, etc.).

use gpui::*;

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
    pub(super) fn render_leaf_item(
        element_id: SharedString,
        icon: impl Into<AnyElement>,
        label: String,
        on_click: impl Fn(&mut Self, &ClickEvent, &mut Window, &mut Context<Self>) + 'static,
        on_right_click: Option<
            impl Fn(&mut Self, &MouseDownEvent, &mut Window, &mut Context<Self>) + 'static,
        >,
        list_hover: Hsla,
        depth: usize,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
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
            row.on_mouse_down(MouseButton::Right, cx.listener(handler))
        } else {
            row
        }
    }
}
