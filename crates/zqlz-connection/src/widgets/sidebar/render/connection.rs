//! Connection entry rendering
//!
//! Renders top-level connection entries with their schema trees.

use gpui::prelude::FluentBuilder;
use gpui::*;

use crate::widgets::sidebar::{ConnectionEntry, ConnectionSidebar, ConnectionSidebarEvent};
use zqlz_ui::widgets::{
    caption, h_flex, typography::body_small, v_flex, ActiveTheme, Icon, IconName, Sizable, ZqlzIcon,
};

impl ConnectionSidebar {
    /// Render a single connection entry with its schema tree.
    ///
    /// This is the top-level rendering function for each connection in the sidebar.
    /// It displays:
    /// - Connection name and database type (with logo/icon)
    /// - Connection status indicator (connected/disconnected/connecting)
    /// - Action buttons (New Query, Disconnect, Connect)
    /// - Expandable schema tree (when connected)
    ///
    /// # Visual Structure
    ///
    /// ```text
    /// [>] [Logo] Connection Name               [●] [SQL] [×]
    ///     └─ Schema tree (when expanded and connected)
    /// ```
    ///
    /// # Parameters
    ///
    /// - `conn`: Connection data including name, type, status, and schema
    /// - `is_last`: Whether this is the last connection (affects bottom border)
    /// - `window`: Window context for rendering
    /// - `cx`: App context for theme and event handling
    ///
    /// # Events
    ///
    /// Emits various `ConnectionSidebarEvent`s based on user interaction:
    /// - Left click: Select connection
    /// - Right click: Show context menu
    /// - Expand/collapse: Toggle schema tree visibility
    /// - Button clicks: New query, connect, disconnect
    pub(in crate::widgets) fn render_connection(
        &self,
        conn: &ConnectionEntry,
        is_last: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let is_selected = self.selected_connection == Some(conn.id);
        let conn_id = conn.id;
        let conn_name = conn.name.clone();
        let db_type = conn.db_type.clone();
        let is_connected = conn.is_connected;
        let is_connecting = conn.is_connecting;
        let is_expanded = conn.is_expanded;
        let is_redis = conn.is_redis();
        let tables = conn.tables.clone();
        let views = conn.views.clone();
        let materialized_views = conn.materialized_views.clone();
        let triggers = conn.triggers.clone();
        let functions = conn.functions.clone();
        let procedures = conn.procedures.clone();
        let queries = conn.queries.clone();
        let tables_expanded = conn.tables_expanded;
        let views_expanded = conn.views_expanded;
        let materialized_views_expanded = conn.materialized_views_expanded;
        let triggers_expanded = conn.triggers_expanded;
        let functions_expanded = conn.functions_expanded;
        let procedures_expanded = conn.procedures_expanded;
        let queries_expanded = conn.queries_expanded;
        let redis_databases = conn.redis_databases.clone();
        let redis_databases_expanded = conn.redis_databases_expanded;
        let databases = conn.databases.clone();
        let schema_name = conn.schema_name.clone();
        let schema_expanded = conn.schema_expanded;

        let db_icon = self.get_db_icon(&db_type);
        let db_logo = self.get_db_logo(&db_type);
        let theme = cx.theme();
        let border_color = theme.border.opacity(0.5);

        v_flex()
            .w_full()
            .child(
                h_flex()
                    .id(SharedString::from(format!("conn-{}", conn_id)))
                    .group("conn-row")
                    .w_full()
                    .px_2()
                    .py_1()
                    .gap_2()
                    .items_center()
                    .rounded_md()
                    .cursor_pointer()
                    .when(is_selected, |this| this.bg(theme.list_active))
                    .hover(|this| this.bg(theme.list_hover))
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.select_connection(conn_id, cx);
                    }))
                    .on_mouse_down(
                        MouseButton::Right,
                        cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                            this.show_connection_context_menu(conn_id, event.position, window, cx);
                        }),
                    )
                    .when(is_connected, |this| {
                        this.child(
                            div()
                                .id(SharedString::from(format!("expand-{}", conn_id)))
                                .flex()
                                .items_center()
                                .justify_center()
                                .cursor_pointer()
                                .on_click(cx.listener(move |this, _event: &ClickEvent, _, cx| {
                                    cx.stop_propagation();
                                    this.toggle_expand(conn_id, cx);
                                }))
                                .child(
                                    Icon::new(if is_expanded {
                                        IconName::ChevronDown
                                    } else {
                                        IconName::ChevronRight
                                    })
                                    .size_3()
                                    .text_color(theme.muted_foreground),
                                ),
                        )
                    })
                    .child(
                        div()
                            .size_4()
                            .flex()
                            .items_center()
                            .justify_center()
                            .when_some(db_logo, |this, logo| this.child(logo.small()))
                            .when(db_logo.is_none(), |this| {
                                this.child(Icon::new(db_icon).size_4())
                            }),
                    )
                    .child(body_small(conn_name).truncate().flex_1())
                    .child(
                        zqlz_ui::widgets::StatusDot::new()
                            .status(if is_connecting {
                                zqlz_ui::widgets::ConnectionStatus::Connecting
                            } else if is_connected {
                                zqlz_ui::widgets::ConnectionStatus::Connected
                            } else {
                                zqlz_ui::widgets::ConnectionStatus::Disconnected
                            })
                            .with_size(zqlz_ui::widgets::Size::XSmall)
                            .into_any_element(),
                    )
                    .child(
                        h_flex()
                            .gap_1()
                            .when(is_connected, |this| {
                                this.invisible()
                                    .group_hover("conn-row", |el| el.visible())
                                    .child(
                                        div()
                                            .id(SharedString::from(format!(
                                                "new-query-{}",
                                                conn_id
                                            )))
                                            .size_5()
                                            .rounded_sm()
                                            .flex()
                                            .items_center()
                                            .justify_center()
                                            .cursor_pointer()
                                            .hover(|el| el.bg(theme.muted))
                                            .on_click(cx.listener(
                                                move |_this, _event: &ClickEvent, _, cx| {
                                                    cx.stop_propagation();
                                                    cx.emit(ConnectionSidebarEvent::NewQuery(
                                                        conn_id,
                                                    ));
                                                },
                                            ))
                                            .child(
                                                Icon::new(ZqlzIcon::FileSql)
                                                    .size_3p5()
                                                    .text_color(theme.muted_foreground),
                                            ),
                                    )
                                    .child(
                                        div()
                                            .id(SharedString::from(format!(
                                                "disconnect-{}",
                                                conn_id
                                            )))
                                            .size_5()
                                            .rounded_sm()
                                            .flex()
                                            .items_center()
                                            .justify_center()
                                            .cursor_pointer()
                                            .hover(|el| el.bg(theme.danger.opacity(0.15)))
                                            .on_click(cx.listener(
                                                move |_this, _event: &ClickEvent, _, cx| {
                                                    cx.stop_propagation();
                                                    cx.emit(ConnectionSidebarEvent::Disconnect(
                                                        conn_id,
                                                    ));
                                                },
                                            ))
                                            .child(
                                                Icon::new(ZqlzIcon::X)
                                                    .size_3p5()
                                                    .text_color(theme.muted_foreground),
                                            ),
                                    )
                            })
                            .when(is_connecting, |this| {
                                this.child(
                                    div()
                                        .px_2()
                                        .py(px(2.0))
                                        .rounded_sm()
                                        .bg(theme.accent.opacity(0.6))
                                        .child(caption("Connecting...").color(gpui::white())),
                                )
                            })
                            .when(!is_connected && !is_connecting, |this| {
                                this.child(
                                    div()
                                        .id(SharedString::from(format!("connect-{}", conn_id)))
                                        .px_2()
                                        .py(px(2.0))
                                        .rounded_sm()
                                        .bg(theme.accent)
                                        .cursor_pointer()
                                        .hover(|el| el.bg(theme.accent.opacity(0.8)))
                                        .on_click(cx.listener(
                                            move |_this, _event: &ClickEvent, _, cx| {
                                                cx.stop_propagation();
                                                tracing::info!(
                                                    "Connect button clicked for connection: {}",
                                                    conn_id
                                                );
                                                cx.emit(ConnectionSidebarEvent::Connect(conn_id));
                                            },
                                        ))
                                        .child(caption("Connect").color(gpui::white())),
                                )
                            }),
                    ),
            )
            .when(is_expanded && is_connected && is_redis, |this| {
                this.child(self.render_redis_schema_tree(
                    conn_id,
                    &redis_databases,
                    redis_databases_expanded,
                    &queries,
                    queries_expanded,
                    window,
                    cx,
                ))
            })
            .when(is_expanded && is_connected && !is_redis, |this| {
                this.child(self.render_schema_tree(
                    conn_id,
                    &tables,
                    &views,
                    &materialized_views,
                    &triggers,
                    &functions,
                    &procedures,
                    &queries,
                    tables_expanded,
                    views_expanded,
                    materialized_views_expanded,
                    triggers_expanded,
                    functions_expanded,
                    procedures_expanded,
                    queries_expanded,
                    &databases,
                    schema_name.as_deref(),
                    schema_expanded,
                    window,
                    cx,
                ))
            })
            .when(!is_last, |this| {
                if is_expanded && is_connected {
                    this.pb_2().mb_1().border_b_1().border_color(border_color)
                } else {
                    this.border_b_1().border_color(border_color)
                }
            })
            .into_any_element()
    }
}
