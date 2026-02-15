//! Section header rendering
//!
//! Renders collapsible section headers with count indicators.

use gpui::prelude::FluentBuilder;
use gpui::*;

use crate::widgets::sidebar::ConnectionSidebar;
use zqlz_ui::widgets::{h_flex, Icon, IconName};

impl ConnectionSidebar {
    /// Render a collapsible section header with count indicator.
    ///
    /// Section headers are used throughout the tree to group related objects.
    /// They display:
    /// - Expand/collapse chevron
    /// - Icon representing the section type
    /// - Label text
    /// - Count indicator (filtered/total when searching, just total otherwise)
    ///
    /// # Visual Structure
    ///
    /// ```text
    /// [v] [Icon] Tables (5)           # No search
    /// [v] [Icon] Tables (3/5)         # With search (3 matches out of 5 total)
    /// ```
    ///
    /// # Parameters
    ///
    /// - `element_id`: Unique ID for this header element
    /// - `icon`: Icon element to display before the label
    /// - `label`: Section name (e.g., "Tables", "Views", "Functions")
    /// - `total_count`: Total number of items in this section
    /// - `filtered_count`: Number of items matching current search (if any)
    /// - `is_expanded`: Whether the section is currently expanded
    /// - `on_click`: Callback invoked when header is clicked (typically toggles expansion)
    /// - `muted_foreground`: Theme color for text
    /// - `list_hover`: Theme color for hover state
    /// - `depth`: Indentation level (affects left padding)
    /// - `cx`: App context
    ///
    /// # Indentation
    ///
    /// Indentation is calculated as `8 + depth * 12` pixels, allowing nested
    /// sections to be visually distinguished in the tree hierarchy.
    pub(super) fn render_section_header(
        &self,
        element_id: SharedString,
        icon: impl Into<AnyElement>,
        label: &str,
        total_count: usize,
        filtered_count: usize,
        is_expanded: bool,
        on_click: impl Fn(&mut Self, &ClickEvent, &mut Window, &mut Context<Self>) + 'static,
        muted_foreground: Hsla,
        list_hover: Hsla,
        depth: usize,
        cx: &mut Context<Self>,
    ) -> Stateful<Div> {
        let has_search = !self.search_query.is_empty();
        let indent = px(8.0 + depth as f32 * 12.0);

        h_flex()
            .id(element_id)
            .w_full()
            .pl(indent)
            .pr_2()
            .h(px(24.0))
            .gap_1p5()
            .items_center()
            .text_xs()
            .text_color(muted_foreground)
            .cursor_pointer()
            .hover(|el| el.bg(list_hover))
            .on_click(cx.listener(on_click))
            .child(
                Icon::new(if is_expanded {
                    IconName::ChevronDown
                } else {
                    IconName::ChevronRight
                })
                .size_3(),
            )
            .child(icon.into())
            .child(if has_search {
                format!("{} ({}/{})", label, filtered_count, total_count)
            } else {
                format!("{} ({})", label, total_count)
            })
    }
}
