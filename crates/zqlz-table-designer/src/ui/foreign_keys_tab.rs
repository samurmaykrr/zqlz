use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{v_flex, ActiveTheme};

use crate::panel::TableDesignerPanel;
use crate::service::fk_action_to_sql;

/// Render the foreign keys tab content
pub(in crate::panel) fn render_foreign_keys_tab(
    this: &mut TableDesignerPanel,
    cx: &mut Context<TableDesignerPanel>,
) -> impl IntoElement {
    let theme = cx.theme();
    let selected_fk_index = this.selected_fk_index;
    let muted_fg = theme.muted_foreground;

    // Collect FK data to avoid borrow issues
    let fk_data: Vec<_> = this
        .design
        .foreign_keys
        .iter()
        .enumerate()
        .map(|(idx, fk)| {
            (
                idx,
                selected_fk_index == Some(idx),
                fk.name.clone().unwrap_or_else(|| "(unnamed)".to_string()),
                fk.columns.join(", "),
                if fk.referenced_table.is_empty() {
                    "(select table)".to_string()
                } else {
                    fk.referenced_table.clone()
                },
                fk.referenced_columns.join(", "),
                fk_action_to_sql(&fk.on_delete),
                fk_action_to_sql(&fk.on_update),
            )
        })
        .collect();

    let has_fks = !this.design.foreign_keys.is_empty();

    // Build row elements using a for loop - convert to AnyElement to avoid lifetime capture issues
    let mut fk_row_elements: Vec<AnyElement> = Vec::with_capacity(fk_data.len());
    for (
        idx,
        is_selected,
        name,
        columns,
        referenced_table,
        referenced_columns,
        on_delete,
        on_update,
    ) in fk_data
    {
        let element = this
            .render_fk_row_inner(
                idx,
                is_selected,
                name,
                columns,
                referenced_table,
                referenced_columns,
                on_delete,
                on_update,
                cx,
            )
            .into_any_element();
        fk_row_elements.push(element);
    }

    let toolbar = this.render_toolbar(cx).into_any_element();
    let header = this.render_fk_header(cx).into_any_element();

    v_flex().size_full().child(toolbar).child(
        div().id("fk-content").flex_1().overflow_y_scroll().child(
            v_flex()
                .w_full()
                .p_2()
                .gap_1()
                .child(header)
                .children(fk_row_elements)
                .when(!has_fks, |this| {
                    this.child(
                        div()
                            .w_full()
                            .py_8()
                            .text_center()
                            .text_sm()
                            .text_color(muted_fg)
                            .child("No foreign keys defined. Click + to add one."),
                    )
                }),
        ),
    )
}
