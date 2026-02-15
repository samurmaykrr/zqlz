use gpui::*;
use zqlz_ui::widgets::v_flex;

use crate::models::DatabaseDialect;
use crate::panel::TableDesignerPanel;

/// Render options tab by delegating to dialect-specific option renderers
pub(in crate::panel) fn render_options_tab(
    this: &mut TableDesignerPanel,
    cx: &mut Context<TableDesignerPanel>,
) -> impl IntoElement {
    let dialect = this.design.dialect;
    let dialect_name = dialect.name().to_string();

    // Render dialect-specific options
    let options_content = match dialect {
        DatabaseDialect::Sqlite => this.render_sqlite_options(cx).into_any_element(),
        DatabaseDialect::Mysql => this.render_mysql_options(cx).into_any_element(),
        DatabaseDialect::Postgres => this.render_postgres_options(cx).into_any_element(),
    };

    v_flex().size_full().p_4().gap_4().child(
        v_flex()
            .gap_2()
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .child(format!("{} Options", dialect_name)),
            )
            .child(options_content),
    )
}
