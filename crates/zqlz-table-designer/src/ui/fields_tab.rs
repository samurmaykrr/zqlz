use gpui::*;
use zqlz_ui::widgets::v_flex;

use crate::panel::TableDesignerPanel;

/// Render the fields tab. This function was extracted from panel.rs to keep the
/// file smaller. It relies on TableDesignerPanel methods like build_column_row_element.
pub(in crate::panel) fn render_fields_tab(
    this: &mut TableDesignerPanel,
    cx: &mut Context<TableDesignerPanel>,
) -> impl IntoElement {
    let selected_column_index = this.selected_column_index;

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
                this.column_name_inputs.get(idx).cloned(),
                this.column_length_inputs.get(idx).cloned(),
                this.column_default_inputs.get(idx).cloned(),
                this.column_type_selects.get(idx).cloned(),
                this.column_comment_inputs.get(idx).cloned(),
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
        name_input,
        length_input,
        default_input,
        type_select,
        comment_input,
    ) in column_data
    {
        let element = this
            .build_column_row_element(
                idx,
                is_selected,
                nullable,
                is_primary_key,
                is_unique,
                name_input,
                length_input,
                default_input,
                type_select,
                comment_input,
                cx,
            )
            .into_any_element();
        column_row_elements.push(element);
    }

    let toolbar = this.render_toolbar(cx).into_any_element();
    let header = this.render_column_header(cx).into_any_element();

    v_flex().size_full().child(toolbar).child(
        div()
            .id("fields-content")
            .flex_1()
            .overflow_y_scroll()
            .child(
                v_flex()
                    .w_full()
                    .p_2()
                    .gap_1()
                    .child(header)
                    .children(column_row_elements),
            ),
    )
}
