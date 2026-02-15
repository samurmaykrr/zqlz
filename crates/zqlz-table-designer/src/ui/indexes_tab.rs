use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{v_flex, ActiveTheme};

use crate::panel::TableDesignerPanel;

/// Render the indexes tab content
pub(in crate::panel) fn render_indexes_tab(
    this: &mut TableDesignerPanel,
    cx: &mut Context<TableDesignerPanel>,
) -> impl IntoElement {
    let theme = cx.theme();
    let selected_index_index = this.selected_index_index;
    let muted_fg = theme.muted_foreground;
    let has_indexes = !this.design.indexes.is_empty();

    // Collect all data first to avoid borrow issues with closures
    let index_data: Vec<_> = this
        .design
        .indexes
        .iter()
        .enumerate()
        .map(|(idx, index)| {
            (
                idx,
                selected_index_index == Some(idx),
                index.name.clone(),
                index.columns.join(", "),
                index.index_type.clone(),
                index.is_unique,
            )
        })
        .collect();

    // Now build elements using a for loop - convert to AnyElement to avoid lifetime capture issues
    let mut index_row_elements: Vec<AnyElement> = Vec::with_capacity(index_data.len());
    for (idx, is_selected, name, columns, index_type, is_unique) in index_data {
        let element = this
            .build_index_row_element(idx, is_selected, name, columns, index_type, is_unique, cx)
            .into_any_element();
        index_row_elements.push(element);
    }

    let toolbar = this.render_toolbar(cx).into_any_element();
    let header = this.render_index_header(cx).into_any_element();

    v_flex().size_full().child(toolbar).child(
        div()
            .id("indexes-content")
            .flex_1()
            .overflow_y_scroll()
            .child(
                v_flex()
                    .w_full()
                    .p_2()
                    .gap_1()
                    .child(header)
                    .children(index_row_elements)
                    .when(!has_indexes, |this| {
                        this.child(
                            div()
                                .w_full()
                                .py_8()
                                .text_center()
                                .text_sm()
                                .text_color(muted_fg)
                                .child("No indexes defined. Click + to add one."),
                        )
                    }),
            ),
    )
}
