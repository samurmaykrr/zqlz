use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{ActiveTheme, v_flex};

use crate::panel::TableDesignerPanel;

/// Render the fields tab. This function was extracted from panel.rs to keep the
/// file smaller. It relies on TableDesignerPanel methods like build_column_row_element.
pub(in crate::panel) fn render_fields_tab(
    this: &mut TableDesignerPanel,
    cx: &mut Context<TableDesignerPanel>,
) -> impl IntoElement {
    let selected_column_index = this.selected_column_index;
    let has_columns = !this.design.columns.is_empty();

    // First, collect all data we need - fully owned, no borrows of self remaining
    let column_data: Vec<_> = this
        .design
        .columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            (
                idx,
                selected_column_index == Some(idx),
                col.nullable,
                col.is_primary_key,
                col.is_unique,
                col.is_auto_increment,
                this.column_name_inputs.get(idx).cloned(),
                this.column_length_inputs.get(idx).cloned(),
                this.column_scale_inputs.get(idx).cloned(),
                this.column_default_inputs.get(idx).cloned(),
                this.column_type_selects.get(idx).cloned(),
                this.column_comment_inputs.get(idx).cloned(),
                this.column_generated_inputs.get(idx).cloned(),
            )
        })
        .collect();

    // Now build elements - convert to AnyElement to avoid lifetime capture issues
    let mut column_row_elements: Vec<AnyElement> = Vec::with_capacity(column_data.len());
    for (
        idx,
        is_selected,
        nullable,
        is_primary_key,
        is_unique,
        is_auto_increment,
        name_input,
        length_input,
        scale_input,
        default_input,
        type_select,
        comment_input,
        generated_input,
    ) in column_data
    {
        let element = this
            .build_column_row_element(
                idx,
                is_selected,
                nullable,
                is_primary_key,
                is_unique,
                is_auto_increment,
                name_input,
                length_input,
                scale_input,
                default_input,
                type_select,
                comment_input,
                generated_input,
                cx,
            )
            .into_any_element();
        column_row_elements.push(element);
    }

    let toolbar = this.render_toolbar(cx).into_any_element();
    let header = this.render_column_header(cx).into_any_element();

    v_flex().size_full().child(toolbar).child(
        div().id("fields-content").flex_1().overflow_scroll().child(
            v_flex()
                .min_w(px(1200.0))
                .w_full()
                .p_2()
                .child(header)
                .children(column_row_elements)
                .when(!has_columns, |this| {
                    this.child(
                        v_flex()
                            .w_full()
                            .py_10()
                            .items_center()
                            .gap_2()
                            .child(
                                div()
                                    .text_sm()
                                    .text_color(cx.theme().muted_foreground)
                                    .child("No columns yet. Click + to add your first column."),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(cx.theme().muted_foreground)
                                    .child("Use Cmd/Ctrl+S to save once the table is valid."),
                            ),
                    )
                }),
        ),
    )
}
