use gpui::prelude::FluentBuilder;
use gpui::*;
use std::sync::OnceLock;
use zqlz_drivers::DriverRegistry;
use zqlz_ui::widgets::{
    ActiveTheme, DatabaseLogo, Icon, IconName, ZqlzIcon, h_flex,
    input::{Input, InputState},
    v_flex,
};

use crate::SavedConnection;

/// Metadata for a database driver option shown in the connection picker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatabaseType {
    /// Internal driver identifier.
    pub id: &'static str,
    /// User-facing driver name.
    pub name: &'static str,
    /// Branded logo displayed in picker options.
    pub logo: DatabaseLogo,
    /// Whether this option can be selected in the current build.
    pub supported: bool,
}

impl DatabaseType {
    /// Returns the underlying driver id used for schema and capability lookups.
    pub fn driver_id(self) -> &'static str {
        if self.id == "mariadb" {
            "mysql"
        } else {
            self.id
        }
    }

    /// Returns whether this picker option corresponds to a saved connection driver id.
    pub fn matches_saved_driver(self, saved_driver: &str) -> bool {
        self.id == saved_driver || (self.id == "mariadb" && saved_driver == "mysql")
    }

    /// Resolves the picker option for a saved connection.
    pub fn for_saved_connection(saved_connection: &SavedConnection) -> Option<Self> {
        Self::all()
            .into_iter()
            .find(|database_type| database_type.matches_saved_driver(&saved_connection.driver))
    }

    /// Returns the set of selectable database types backed by compiled drivers.
    pub fn all() -> Vec<Self> {
        let registry = driver_registry();

        let all_types = vec![
            Self {
                id: "sqlite",
                name: "SQLite",
                logo: DatabaseLogo::SQLite,
                supported: true,
            },
            Self {
                id: "turso",
                name: "Turso",
                logo: DatabaseLogo::Turso,
                supported: true,
            },
            Self {
                id: "postgres",
                name: "PostgreSQL",
                logo: DatabaseLogo::PostgreSQL,
                supported: true,
            },
            Self {
                id: "mysql",
                name: "MySQL",
                logo: DatabaseLogo::MySQL,
                supported: true,
            },
            Self {
                id: "mariadb",
                name: "MariaDB",
                logo: DatabaseLogo::MariaDB,
                supported: true,
            },
            Self {
                id: "mssql",
                name: "SQL Server",
                logo: DatabaseLogo::MsSql,
                supported: true,
            },
            Self {
                id: "duckdb",
                name: "DuckDB",
                logo: DatabaseLogo::DuckDB,
                supported: true,
            },
            Self {
                id: "redis",
                name: "Redis",
                logo: DatabaseLogo::Redis,
                supported: true,
            },
            Self {
                id: "mongodb",
                name: "MongoDB",
                logo: DatabaseLogo::MongoDB,
                supported: true,
            },
            Self {
                id: "clickhouse",
                name: "ClickHouse",
                logo: DatabaseLogo::ClickHouse,
                supported: true,
            },
        ];

        all_types
            .into_iter()
            .filter(|database_type| registry.has(database_type.driver_id()))
            .collect()
    }

    /// Returns database types whose identifier or display name matches the query.
    pub fn filter(query: &str) -> Vec<Self> {
        let query = query.to_lowercase();
        Self::all()
            .into_iter()
            .filter(|database_type| {
                database_type.name.to_lowercase().contains(&query)
                    || database_type.id.contains(&query)
            })
            .collect()
    }
}

pub(crate) fn driver_registry() -> &'static DriverRegistry {
    static DRIVER_REGISTRY: OnceLock<DriverRegistry> = OnceLock::new();
    DRIVER_REGISTRY.get_or_init(DriverRegistry::with_defaults)
}

#[derive(Clone, Debug)]
pub enum ConnectionPickerEvent {
    /// User selected a supported database type.
    Selected(DatabaseType),
}

/// UI widget for choosing a database driver using grid or list layout.
pub struct ConnectionPicker {
    search_input: Entity<InputState>,
    search_query: String,
    grid_view: bool,
}

impl ConnectionPicker {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Search databases..."));

        Self {
            search_input,
            search_query: String::new(),
            grid_view: true,
        }
    }

    fn filtered_database_types(&self) -> Vec<DatabaseType> {
        if self.search_query.is_empty() {
            DatabaseType::all()
        } else {
            DatabaseType::filter(&self.search_query)
        }
    }

    fn render_view_toggle(&self, cx: &Context<Self>) -> impl IntoElement {
        h_flex()
            .gap_0()
            .border_1()
            .border_color(cx.theme().border)
            .rounded(cx.theme().radius)
            .overflow_hidden()
            .child(
                div()
                    .id("grid-view-btn")
                    .px_2()
                    .py_1()
                    .cursor_pointer()
                    .flex()
                    .items_center()
                    .justify_center()
                    .when(self.grid_view, |this| this.bg(cx.theme().primary))
                    .hover(|this| this.bg(cx.theme().muted))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.grid_view = true;
                        cx.notify();
                    }))
                    .child(Icon::new(IconName::LayoutDashboard).size_4().text_color(
                        if self.grid_view {
                            cx.theme().primary_foreground
                        } else {
                            cx.theme().muted_foreground
                        },
                    )),
            )
            .child(
                div()
                    .id("list-view-btn")
                    .px_2()
                    .py_1()
                    .cursor_pointer()
                    .flex()
                    .items_center()
                    .justify_center()
                    .when(!self.grid_view, |this| this.bg(cx.theme().primary))
                    .hover(|this| this.bg(cx.theme().muted))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.grid_view = false;
                        cx.notify();
                    }))
                    .child(Icon::new(ZqlzIcon::ListBullets).size_4().text_color(
                        if !self.grid_view {
                            cx.theme().primary_foreground
                        } else {
                            cx.theme().muted_foreground
                        },
                    )),
            )
    }

    fn render_db_grid(
        &self,
        database_types: Vec<DatabaseType>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex()
            .flex_wrap()
            .gap_3()
            .p_2()
            .children(database_types.into_iter().map(|database_type| {
                let database_type_for_click = database_type;
                div()
                    .id(SharedString::from(format!("db-{}", database_type.id)))
                    .w(px(160.0))
                    .min_h(px(120.0))
                    .p_3()
                    .flex()
                    .flex_col()
                    .items_center()
                    .justify_center()
                    .gap_2()
                    .border_1()
                    .border_color(cx.theme().border)
                    .rounded(cx.theme().radius)
                    .cursor_pointer()
                    .hover(|this| this.bg(cx.theme().muted).border_color(cx.theme().accent))
                    .when(!database_type.supported, |this| this.opacity(0.5))
                    .on_click(cx.listener(move |_, _, _, cx| {
                        if database_type_for_click.supported {
                            cx.emit(ConnectionPickerEvent::Selected(database_type_for_click));
                        }
                    }))
                    .child(
                        div()
                            .flex_none()
                            .h(px(40.0))
                            .flex()
                            .items_center()
                            .justify_center()
                            .child(database_type.logo.large().flex_none()),
                    )
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(cx.theme().foreground)
                            .whitespace_nowrap()
                            .text_center()
                            .child(database_type.name),
                    )
            }))
    }

    fn render_db_list(
        &self,
        database_types: Vec<DatabaseType>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        v_flex()
            .gap_1()
            .p_2()
            .children(database_types.into_iter().map(|database_type| {
                let database_type_for_click = database_type;
                h_flex()
                    .id(SharedString::from(format!("db-list-{}", database_type.id)))
                    .w_full()
                    .px_3()
                    .py_2()
                    .gap_3()
                    .items_center()
                    .border_1()
                    .border_color(cx.theme().border)
                    .rounded(cx.theme().radius)
                    .cursor_pointer()
                    .hover(|this| this.bg(cx.theme().muted).border_color(cx.theme().accent))
                    .when(!database_type.supported, |this| this.opacity(0.5))
                    .on_click(cx.listener(move |_, _, _, cx| {
                        if database_type_for_click.supported {
                            cx.emit(ConnectionPickerEvent::Selected(database_type_for_click));
                        }
                    }))
                    .child(database_type.logo.medium())
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(cx.theme().foreground)
                            .child(database_type.name),
                    )
            }))
    }
}

impl Render for ConnectionPicker {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let current_search = self.search_input.read(cx).text().to_string();
        if current_search != self.search_query {
            self.search_query = current_search;
        }

        let database_types = self.filtered_database_types();

        v_flex()
            .size_full()
            .p_4()
            .gap_4()
            .child(
                h_flex()
                    .justify_between()
                    .items_center()
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("Select Database Type"),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(self.render_view_toggle(cx))
                            .child(div().w(px(200.0)).child(Input::new(&self.search_input))),
                    ),
            )
            .child(
                div()
                    .id("db-list-container")
                    .flex_1()
                    .overflow_y_scroll()
                    .child(if self.grid_view {
                        self.render_db_grid(database_types, cx).into_any_element()
                    } else {
                        self.render_db_list(database_types, cx).into_any_element()
                    }),
            )
    }
}

impl EventEmitter<ConnectionPickerEvent> for ConnectionPicker {}
