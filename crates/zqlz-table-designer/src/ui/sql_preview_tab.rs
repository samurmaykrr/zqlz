use gpui::*;
use zqlz_ui::widgets::{button::Button, h_flex, typography::code, v_flex, ActiveTheme, Sizable};

use crate::panel::TableDesignerPanel;

/// Render the SQL preview tab content
pub(in crate::panel) fn render_sql_preview_tab(
    this: &mut TableDesignerPanel,
    cx: &mut Context<TableDesignerPanel>,
) -> impl IntoElement {
    let theme = cx.theme();
    let ddl = this
        .ddl_preview
        .clone()
        .unwrap_or_else(|| "-- Click to generate DDL preview".to_string());

    v_flex()
        .size_full()
        .p_2()
        .child(
            div()
                .id("sql-preview")
                .flex_1()
                .overflow_scroll()
                .p_3()
                .rounded_md()
                .bg(theme.secondary)
                .border_1()
                .border_color(theme.border)
                .text_sm()
                .child(code(&ddl)),
        )
        .child(
            h_flex().justify_end().pt_2().child(
                Button::new("copy-ddl")
                    .label("Copy to Clipboard")
                    .small()
                    .on_click(cx.listener(|this, _, _window, cx| {
                        if let Some(ref ddl) = this.ddl_preview {
                            cx.write_to_clipboard(ClipboardItem::new_string(ddl.clone()));
                            tracing::info!("DDL copied to clipboard");
                        }
                    })),
            ),
        )
}
