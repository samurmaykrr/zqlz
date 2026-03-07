use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{v_flex, ActiveTheme};

use crate::panel::TableDesignerPanel;

/// Render the check constraints tab content
pub(in crate::panel) fn render_check_constraints_tab(
    this: &mut TableDesignerPanel,
    cx: &mut Context<TableDesignerPanel>,
) -> impl IntoElement {
    let theme = cx.theme();
    let selected_check_index = this.selected_check_index;
    let muted_fg = theme.muted_foreground;
    let has_checks = !this.design.check_constraints.is_empty();

    let check_data: Vec<_> = this
        .design
        .check_constraints
        .iter()
        .enumerate()
        .map(|(idx, _cc)| {
            (
                idx,
                selected_check_index == Some(idx),
                this.check_name_inputs.get(idx).cloned(),
                this.check_expression_inputs.get(idx).cloned(),
            )
        })
        .collect();

    let mut row_elements: Vec<AnyElement> = Vec::with_capacity(check_data.len());
    for (idx, is_selected, name_input, expr_input) in check_data {
        let element = this
            .build_check_row_element(idx, is_selected, name_input, expr_input, cx)
            .into_any_element();
        row_elements.push(element);
    }

    let toolbar = this.render_toolbar(cx).into_any_element();
    let header = this.render_check_header(cx).into_any_element();

    v_flex().size_full().child(toolbar).child(
        div()
            .id("checks-content")
            .flex_1()
            .overflow_y_scroll()
            .child(
                v_flex()
                    .w_full()
                    .p_2()
                    .gap_1()
                    .child(header)
                    .children(row_elements)
                    .when(!has_checks, |this| {
                        this.child(
                            div()
                                .w_full()
                                .py_8()
                                .text_center()
                                .text_sm()
                                .text_color(muted_fg)
                                .child(
                                    "No check constraints defined. Click + to add one.",
                                ),
                        )
                    }),
            ),
    )
}
