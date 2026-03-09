use gpui::*;
use zqlz_ui::widgets::{
    ActiveTheme, h_flex,
    tab::{Tab, TabBar},
};

use crate::DesignerTab;
use crate::panel::TableDesignerPanel;

/// Render the tab bar. This extracts the logic from panel.rs so the panel can
/// call it without holding large render functions inline.
pub(in crate::panel) fn render_tab_bar(
    this: &mut TableDesignerPanel,
    cx: &mut Context<TableDesignerPanel>,
) -> impl IntoElement {
    let theme = cx.theme();
    let active_tab = this.active_tab;

    h_flex()
        .w_full()
        .justify_center()
        .py_2()
        .border_b_1()
        .border_color(theme.border)
        .child(
            TabBar::new("designer-tabs")
                .pill()
                .selected_index(match active_tab {
                    DesignerTab::Fields => 0,
                    DesignerTab::Indexes => 1,
                    DesignerTab::ForeignKeys => 2,
                    DesignerTab::CheckConstraints => 3,
                    DesignerTab::Options => 4,
                    DesignerTab::SqlPreview => 5,
                    DesignerTab::Triggers => 6,
                })
                .on_click(cx.listener(|this, ix: &usize, _window, cx| {
                    this.active_tab = match ix {
                        0 => DesignerTab::Fields,
                        1 => DesignerTab::Indexes,
                        2 => DesignerTab::ForeignKeys,
                        3 => DesignerTab::CheckConstraints,
                        4 => DesignerTab::Options,
                        5 => DesignerTab::SqlPreview,
                        _ => DesignerTab::Fields,
                    };
                    if this.active_tab == DesignerTab::SqlPreview {
                        this.generate_ddl_preview(cx);
                    }
                    cx.notify();
                }))
                .child(Tab::new().label("Fields"))
                .child(Tab::new().label("Indexes"))
                .child(Tab::new().label("Foreign Keys"))
                .child(Tab::new().label("Checks"))
                .child(Tab::new().label("Options"))
                .child(Tab::new().label("SQL Preview")),
        )
}
