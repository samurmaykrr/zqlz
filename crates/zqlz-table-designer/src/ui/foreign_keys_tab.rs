use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{ActiveTheme, input::InputState, select::SelectState, v_flex};

use crate::panel::TableDesignerPanel;

type ForeignKeySelect = Option<Entity<SelectState<Vec<&'static str>>>>;
type ForeignKeyInput = Option<Entity<InputState>>;

struct ForeignKeyRowData {
    index: usize,
    is_selected: bool,
    name: String,
    columns: String,
    referenced_table: String,
    referenced_columns: String,
    on_delete_select: ForeignKeySelect,
    on_update_select: ForeignKeySelect,
    name_input: ForeignKeyInput,
    columns_input: ForeignKeyInput,
    ref_table_input: ForeignKeyInput,
    ref_columns_input: ForeignKeyInput,
}

/// Render the foreign keys tab content
pub(in crate::panel) fn render_foreign_keys_tab(
    this: &mut TableDesignerPanel,
    cx: &mut Context<TableDesignerPanel>,
) -> impl IntoElement {
    let theme = cx.theme();
    let selected_fk_index = this.selected_fk_index;
    let muted_fg = theme.muted_foreground;

    // Collect FK data to avoid borrow issues
    let fk_data: Vec<ForeignKeyRowData> = this
        .design
        .foreign_keys
        .iter()
        .enumerate()
        .map(|(idx, fk)| ForeignKeyRowData {
            index: idx,
            is_selected: selected_fk_index == Some(idx),
            name: fk.name.clone().unwrap_or_else(|| "(unnamed)".to_string()),
            columns: fk.columns.join(", "),
            referenced_table: if fk.referenced_table.is_empty() {
                "(select table)".to_string()
            } else {
                fk.referenced_table.clone()
            },
            referenced_columns: fk.referenced_columns.join(", "),
            on_delete_select: this.fk_on_delete_selects.get(idx).cloned(),
            on_update_select: this.fk_on_update_selects.get(idx).cloned(),
            name_input: this.fk_name_inputs.get(idx).cloned(),
            columns_input: this.fk_columns_inputs.get(idx).cloned(),
            ref_table_input: this.fk_ref_table_inputs.get(idx).cloned(),
            ref_columns_input: this.fk_ref_columns_inputs.get(idx).cloned(),
        })
        .collect();

    let has_fks = !this.design.foreign_keys.is_empty();

    // Build row elements using a for loop - convert to AnyElement to avoid lifetime capture issues
    let mut fk_row_elements: Vec<AnyElement> = Vec::with_capacity(fk_data.len());
    for row in fk_data {
        let element = this
            .render_fk_row_inner(
                row.index,
                row.is_selected,
                row.name,
                row.columns,
                row.referenced_table,
                row.referenced_columns,
                row.on_delete_select,
                row.on_update_select,
                row.name_input,
                row.columns_input,
                row.ref_table_input,
                row.ref_columns_input,
                cx,
            )
            .into_any_element();
        fk_row_elements.push(element);
    }

    let toolbar = this.render_toolbar(cx).into_any_element();
    let header = this.render_fk_header(cx).into_any_element();

    v_flex().size_full().child(toolbar).child(
        div().id("fk-content").flex_1().overflow_scroll().child(
            v_flex()
                .min_w(px(800.0))
                .w_full()
                .p_2()
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
