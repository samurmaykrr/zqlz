use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_ui::widgets::{
    button::{Button, ButtonVariants},
    h_flex, Disableable, IconName, Sizable,
};

use crate::panel::TableDesignerPanel;
use crate::DesignerTab;

/// Render the toolbar with add/remove/move buttons. Extracted from panel.rs
pub(in crate::panel) fn render_toolbar(
    this: &mut TableDesignerPanel,
    cx: &mut Context<TableDesignerPanel>,
) -> impl IntoElement {
    let has_selection = match this.active_tab {
        DesignerTab::Fields => this.selected_column_index.is_some(),
        DesignerTab::Indexes => this.selected_index_index.is_some(),
        DesignerTab::ForeignKeys => this.selected_fk_index.is_some(),
        _ => false,
    };

    let can_move_up = match this.active_tab {
        DesignerTab::Fields => this.selected_column_index.map(|i| i > 0).unwrap_or(false),
        _ => false,
    };

    let can_move_down = match this.active_tab {
        DesignerTab::Fields => this
            .selected_column_index
            .map(|i| i < this.design.columns.len().saturating_sub(1))
            .unwrap_or(false),
        _ => false,
    };

    h_flex()
        .gap_1()
        .p_1()
        .child(
            Button::new("add")
                .icon(IconName::Plus)
                .xsmall()
                .ghost()
                .tooltip("Add")
                .on_click(cx.listener(|this, _, window, cx| match this.active_tab {
                    DesignerTab::Fields => this.add_column(window, cx),
                    DesignerTab::Indexes => this.add_index(cx),
                    DesignerTab::ForeignKeys => this.add_foreign_key(cx),
                    _ => {}
                })),
        )
        .child(
            Button::new("remove")
                .icon(IconName::Minus)
                .xsmall()
                .ghost()
                .tooltip("Remove")
                .disabled(!has_selection)
                .on_click(cx.listener(|this, _, _window, cx| match this.active_tab {
                    DesignerTab::Fields => this.remove_column(cx),
                    DesignerTab::Indexes => this.remove_index(cx),
                    DesignerTab::ForeignKeys => this.remove_foreign_key(cx),
                    _ => {}
                })),
        )
        .when(this.active_tab == DesignerTab::Fields, |this| {
            this.child(
                Button::new("move-up")
                    .icon(IconName::ArrowUp)
                    .xsmall()
                    .ghost()
                    .tooltip("Move Up")
                    .disabled(!can_move_up)
                    .on_click(cx.listener(|this, _, _window, cx| {
                        this.move_column_up(cx);
                    })),
            )
            .child(
                Button::new("move-down")
                    .icon(IconName::ArrowDown)
                    .xsmall()
                    .ghost()
                    .tooltip("Move Down")
                    .disabled(!can_move_down)
                    .on_click(cx.listener(|this, _, _window, cx| {
                        this.move_column_down(cx);
                    })),
            )
        })
}
