//! Database objects tree rendering
//!
//! Renders trees of schema objects (tables, views, triggers, functions, procedures, queries).

use gpui::*;

use super::{LeafItemProps, SectionHeaderProps};
use crate::widgets::sidebar::{
    ConnectionSidebar, ConnectionSidebarEvent, SavedQueryInfo, SidebarObjectCapabilities,
};
use zqlz_ui::widgets::{ActiveTheme, Icon, ZqlzIcon, v_flex};

impl ConnectionSidebar {
    fn current_schema_for(&self, database_name: Option<&str>) -> Option<String> {
        self.selected_connection.and_then(|selected_connection| {
            self.connections
                .iter()
                .find(|connection| connection.id == selected_connection)
                .and_then(|connection| {
                    database_name
                        .and_then(|database_name| {
                            connection
                                .databases
                                .iter()
                                .find(|database| database.name == database_name)
                                .and_then(|database| {
                                    database
                                        .schema
                                        .as_ref()
                                        .and_then(|schema| schema.schema_name.clone())
                                })
                        })
                        .or_else(|| connection.schema_name.clone())
                })
        })
    }

    /// Build a tree of database schema objects (tables, views, triggers, etc.).
    ///
    /// This is a reusable tree builder that renders all standard SQL objects
    /// in a consistent format. It's used both for single-database connections
    /// and within each database node in multi-database mode.
    ///
    /// # Sections Rendered
    ///
    /// The function renders up to 7 collapsible sections (when non-empty):
    /// 1. Tables
    /// 2. Views
    /// 3. Materialized Views
    /// 4. Triggers
    /// 5. Functions
    /// 6. Procedures
    /// 7. Saved Queries
    ///
    /// # Search Behavior
    ///
    /// When `self.search_query` is active:
    /// - Each section filters its items to matches only
    /// - Sections with no matches are hidden
    /// - Sections with matches are auto-expanded
    /// - Count shows as "filtered/total" in headers
    ///
    /// # Parameters
    ///
    /// - `conn_id`: Connection UUID for event routing
    /// - `id_suffix`: Unique suffix for element IDs (e.g., connection ID or database name)
    /// - `database_name`: Optional database name (for multi-database connections)
    /// - `tables`, `views`, etc.: Lists of object names to render
    /// - `*_expanded`: Expansion state for each section
    /// - `toggle_section`: Callback to toggle section expansion, receives section key
    /// - `depth`: Starting depth for section headers (leaf items render at depth+1)
    /// - `cx`: App context
    ///
    /// # Section Keys
    ///
    /// The `toggle_section` callback receives these keys:
    /// - "tables", "views", "materialized_views", "triggers"
    /// - "functions", "procedures", "queries"
    ///
    /// This allows the same tree builder to work with both connection-level
    /// and per-database toggle handlers.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn render_objects_tree(
        &self,
        conn_id: uuid::Uuid,
        object_capabilities: SidebarObjectCapabilities,
        id_suffix: &str,
        database_name: Option<String>,
        tables: &[String],
        views: &[String],
        materialized_views: &[String],
        triggers: &[String],
        functions: &[String],
        procedures: &[String],
        queries: &[SavedQueryInfo],
        tables_expanded: bool,
        views_expanded: bool,
        materialized_views_expanded: bool,
        triggers_expanded: bool,
        functions_expanded: bool,
        procedures_expanded: bool,
        queries_expanded: bool,
        tables_loading: bool,
        views_loading: bool,
        materialized_views_loading: bool,
        triggers_loading: bool,
        functions_loading: bool,
        procedures_loading: bool,
        toggle_section: impl Fn(&mut Self, &str, &mut Context<Self>) + Clone + 'static,
        depth: usize,
        cx: &mut Context<Self>,
    ) -> Div {
        let muted_foreground = cx.theme().muted_foreground;
        let list_hover = cx.theme().list_hover;
        let font_family = cx.theme().font_family.clone();
        let has_search = !self.search_query.is_empty();

        let filtered_tables = self.filter_by_search(tables);
        let filtered_views = self.filter_by_search(views);
        let filtered_mat_views = self.filter_by_search(materialized_views);
        let filtered_triggers = self.filter_by_search(triggers);
        let filtered_functions = self.filter_by_search(functions);
        let filtered_procedures = self.filter_by_search(procedures);
        let filtered_queries: Vec<_> = queries
            .iter()
            .filter(|q| self.matches_search(&q.name))
            .collect();

        let tables_expanded = tables_expanded || (has_search && !filtered_tables.is_empty());
        let views_expanded = views_expanded || (has_search && !filtered_views.is_empty());
        let mat_views_expanded =
            materialized_views_expanded || (has_search && !filtered_mat_views.is_empty());
        let triggers_expanded = triggers_expanded || (has_search && !filtered_triggers.is_empty());
        let functions_expanded =
            functions_expanded || (has_search && !filtered_functions.is_empty());
        let procedures_expanded =
            procedures_expanded || (has_search && !filtered_procedures.is_empty());
        let queries_expanded = queries_expanded || (has_search && !filtered_queries.is_empty());

        let mut tree = v_flex().w_full().gap_px().font_family(font_family);

        let leaf_depth = depth + 1;

        // ── Tables ──────────────────────────────────────────────────────
        if !has_search || tables_loading || !filtered_tables.is_empty() {
            let toggle = toggle_section.clone();
            let header = self.render_section_header(
                SectionHeaderProps {
                    element_id: SharedString::from(format!("tables-header-{}", id_suffix)),
                    icon: Icon::new(ZqlzIcon::Table).size_3().into_any_element(),
                    label: "Tables",
                    total_count: tables.len(),
                    filtered_count: filtered_tables.len(),
                    is_expanded: tables_expanded,
                    on_click: move |this: &mut Self,
                                    _: &ClickEvent,
                                    _: &mut Window,
                                    cx: &mut Context<Self>| {
                        toggle(this, "tables", cx)
                    },
                    on_right_click: Some(
                        move |this: &mut Self,
                              event: &MouseDownEvent,
                              window: &mut Window,
                              cx: &mut Context<Self>| {
                            this.show_section_context_menu(
                                conn_id,
                                "tables",
                                event.position,
                                window,
                                cx,
                            );
                        },
                    ),
                    muted_foreground,
                    list_hover,
                    depth,
                },
                cx,
            );

            let mut section = v_flex().w_full().child(header);
            if tables_expanded {
                if tables_loading {
                    section = section.child(Self::render_loading_row(
                        id_suffix,
                        "tables",
                        muted_foreground,
                        depth + 1,
                    ));
                } else {
                    for table_name in &filtered_tables {
                        let table = (*table_name).clone();
                        let name_for_menu = (*table_name).clone();
                        let object_schema = self.current_schema_for(database_name.as_deref());
                        let object_schema_for_menu = object_schema.clone();
                        let db_name_for_click = database_name.clone();
                        let db_name_for_menu = database_name.clone();
                        section = section.child(Self::render_leaf_item(
                            LeafItemProps {
                                element_id: SharedString::from(format!(
                                    "table-{}-{}",
                                    id_suffix, table_name
                                )),
                                icon: Icon::new(ZqlzIcon::Table)
                                    .size_3()
                                    .text_color(muted_foreground)
                                    .into_any_element(),
                                label: (*table_name).clone(),
                                on_click: move |_this: &mut Self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>| {
                                    cx.emit(ConnectionSidebarEvent::OpenTable {
                                        connection_id: conn_id,
                                        table_name: table.clone(),
                                        database_name: db_name_for_click.clone(),
                                    });
                                },
                                on_right_click: Some(
                                    move |this: &mut Self,
                                          event: &MouseDownEvent,
                                          window: &mut Window,
                                          cx: &mut Context<Self>| {
                                        this.show_table_context_menu(
                                            conn_id,
                                            name_for_menu.clone(),
                                            object_schema_for_menu.clone(),
                                            db_name_for_menu.clone(),
                                            event.position,
                                            window,
                                            cx,
                                        );
                                    },
                                ),
                                list_hover,
                                depth: leaf_depth,
                            },
                            cx,
                        ));
                    }
                }
            }
            tree = tree.child(section);
        }

        // ── Views ───────────────────────────────────────────────────────
        if object_capabilities.supports_views
            && (!has_search || views_loading || !filtered_views.is_empty())
        {
            let toggle = toggle_section.clone();
            let header = self.render_section_header(
                SectionHeaderProps {
                    element_id: SharedString::from(format!("views-header-{}", id_suffix)),
                    icon: Icon::new(ZqlzIcon::Eye).size_3().into_any_element(),
                    label: "Views",
                    total_count: views.len(),
                    filtered_count: filtered_views.len(),
                    is_expanded: views_expanded,
                    on_click: move |this: &mut Self,
                                    _: &ClickEvent,
                                    _: &mut Window,
                                    cx: &mut Context<Self>| {
                        toggle(this, "views", cx)
                    },
                    on_right_click: Some(
                        move |this: &mut Self,
                              event: &MouseDownEvent,
                              window: &mut Window,
                              cx: &mut Context<Self>| {
                            this.show_section_context_menu(
                                conn_id,
                                "views",
                                event.position,
                                window,
                                cx,
                            );
                        },
                    ),
                    muted_foreground,
                    list_hover,
                    depth,
                },
                cx,
            );

            let mut section = v_flex().w_full().child(header);
            if views_expanded {
                if views_loading {
                    section = section.child(Self::render_loading_row(
                        id_suffix,
                        "views",
                        muted_foreground,
                        depth + 1,
                    ));
                } else {
                    for view_name in &filtered_views {
                        let view = (*view_name).clone();
                        let name_for_menu = (*view_name).clone();
                        let object_schema = self.current_schema_for(database_name.as_deref());
                        let object_schema_for_menu = object_schema.clone();
                        let db_name_for_click = database_name.clone();
                        let db_name_for_menu = database_name.clone();
                        section = section.child(Self::render_leaf_item(
                            LeafItemProps {
                                element_id: SharedString::from(format!(
                                    "view-{}-{}",
                                    id_suffix, view_name
                                )),
                                icon: Icon::new(ZqlzIcon::Eye)
                                    .size_3()
                                    .text_color(muted_foreground)
                                    .into_any_element(),
                                label: (*view_name).clone(),
                                on_click: move |_this: &mut Self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>| {
                                    cx.emit(ConnectionSidebarEvent::OpenView {
                                        connection_id: conn_id,
                                        view_name: view.clone(),
                                        database_name: db_name_for_click.clone(),
                                    });
                                },
                                on_right_click: Some(
                                    move |this: &mut Self,
                                          event: &MouseDownEvent,
                                          window: &mut Window,
                                          cx: &mut Context<Self>| {
                                        this.show_view_context_menu(
                                            conn_id,
                                            name_for_menu.clone(),
                                            object_schema_for_menu.clone(),
                                            db_name_for_menu.clone(),
                                            event.position,
                                            window,
                                            cx,
                                        );
                                    },
                                ),
                                list_hover,
                                depth: leaf_depth,
                            },
                            cx,
                        ));
                    }
                }
            }
            tree = tree.child(section);
        }

        // ── Materialized Views ──────────────────────────────────────────
        if object_capabilities.supports_materialized_views
            && (!has_search || materialized_views_loading || !filtered_mat_views.is_empty())
        {
            let toggle = toggle_section.clone();
            let header = self.render_section_header(
                SectionHeaderProps {
                    element_id: SharedString::from(format!("matviews-header-{}", id_suffix)),
                    icon: Icon::new(ZqlzIcon::TreeStructure)
                        .size_3()
                        .into_any_element(),
                    label: "Materialized Views",
                    total_count: materialized_views.len(),
                    filtered_count: filtered_mat_views.len(),
                    is_expanded: mat_views_expanded,
                    on_click: move |this: &mut Self,
                                    _: &ClickEvent,
                                    _: &mut Window,
                                    cx: &mut Context<Self>| {
                        toggle(this, "materialized_views", cx)
                    },
                    on_right_click: Some(
                        move |this: &mut Self,
                              event: &MouseDownEvent,
                              window: &mut Window,
                              cx: &mut Context<Self>| {
                            this.show_section_context_menu(
                                conn_id,
                                "materialized_views",
                                event.position,
                                window,
                                cx,
                            );
                        },
                    ),
                    muted_foreground,
                    list_hover,
                    depth,
                },
                cx,
            );

            let mut section = v_flex().w_full().child(header);
            if mat_views_expanded {
                if materialized_views_loading {
                    section = section.child(Self::render_loading_row(
                        id_suffix,
                        "matviews",
                        muted_foreground,
                        depth + 1,
                    ));
                } else {
                    for view_name in &filtered_mat_views {
                        let view = (*view_name).clone();
                        let name_for_menu = (*view_name).clone();
                        let db_name_for_click = database_name.clone();
                        let db_name_for_menu = database_name.clone();
                        section = section.child(Self::render_leaf_item(
                            LeafItemProps {
                                element_id: SharedString::from(format!(
                                    "matview-{}-{}",
                                    id_suffix, view_name
                                )),
                                icon: Icon::new(ZqlzIcon::TreeStructure)
                                    .size_3()
                                    .text_color(muted_foreground)
                                    .into_any_element(),
                                label: (*view_name).clone(),
                                on_click: move |_this: &mut Self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>| {
                                    cx.emit(ConnectionSidebarEvent::OpenView {
                                        connection_id: conn_id,
                                        view_name: view.clone(),
                                        database_name: db_name_for_click.clone(),
                                    });
                                },
                                on_right_click: Some(
                                    move |this: &mut Self,
                                          event: &MouseDownEvent,
                                          window: &mut Window,
                                          cx: &mut Context<Self>| {
                                        this.show_materialized_view_context_menu(
                                            conn_id,
                                            name_for_menu.clone(),
                                            db_name_for_menu.clone(),
                                            event.position,
                                            window,
                                            cx,
                                        );
                                    },
                                ),
                                list_hover,
                                depth: leaf_depth,
                            },
                            cx,
                        ));
                    }
                }
            }
            tree = tree.child(section);
        }

        // ── Triggers ────────────────────────────────────────────────────
        if object_capabilities.supports_triggers
            && (!has_search || triggers_loading || !filtered_triggers.is_empty())
        {
            let toggle = toggle_section.clone();
            let header = self.render_section_header(
                SectionHeaderProps {
                    element_id: SharedString::from(format!("triggers-header-{}", id_suffix)),
                    icon: Icon::new(ZqlzIcon::LightningBolt)
                        .size_3()
                        .into_any_element(),
                    label: "Triggers",
                    total_count: triggers.len(),
                    filtered_count: filtered_triggers.len(),
                    is_expanded: triggers_expanded,
                    on_click: move |this: &mut Self,
                                    _: &ClickEvent,
                                    _: &mut Window,
                                    cx: &mut Context<Self>| {
                        toggle(this, "triggers", cx)
                    },
                    on_right_click: Some(
                        move |this: &mut Self,
                              event: &MouseDownEvent,
                              window: &mut Window,
                              cx: &mut Context<Self>| {
                            this.show_section_context_menu(
                                conn_id,
                                "triggers",
                                event.position,
                                window,
                                cx,
                            );
                        },
                    ),
                    muted_foreground,
                    list_hover,
                    depth,
                },
                cx,
            );

            let mut section = v_flex().w_full().child(header);
            if triggers_expanded {
                if triggers_loading {
                    section = section.child(Self::render_loading_row(
                        id_suffix,
                        "triggers",
                        muted_foreground,
                        depth + 1,
                    ));
                } else {
                    for trigger_name in &filtered_triggers {
                        let trig = (*trigger_name).clone();
                        let name_for_menu = (*trigger_name).clone();
                        let object_schema = self.current_schema_for(database_name.as_deref());
                        let object_schema_for_click = object_schema.clone();
                        let object_schema_for_menu = object_schema.clone();
                        section = section.child(Self::render_leaf_item(
                            LeafItemProps {
                                element_id: SharedString::from(format!(
                                    "trigger-{}-{}",
                                    id_suffix, trigger_name
                                )),
                                icon: Icon::new(ZqlzIcon::LightningBolt)
                                    .size_3()
                                    .text_color(muted_foreground)
                                    .into_any_element(),
                                label: (*trigger_name).clone(),
                                on_click: move |_this: &mut Self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>| {
                                    cx.emit(ConnectionSidebarEvent::DesignTrigger {
                                        connection_id: conn_id,
                                        trigger_name: trig.clone(),
                                        object_schema: object_schema_for_click.clone(),
                                    });
                                },
                                on_right_click: Some(
                                    move |this: &mut Self,
                                          event: &MouseDownEvent,
                                          window: &mut Window,
                                          cx: &mut Context<Self>| {
                                        this.show_trigger_context_menu(
                                            conn_id,
                                            name_for_menu.clone(),
                                            object_schema_for_menu.clone(),
                                            event.position,
                                            window,
                                            cx,
                                        );
                                    },
                                ),
                                list_hover,
                                depth: leaf_depth,
                            },
                            cx,
                        ));
                    }
                }
            }
            tree = tree.child(section);
        }

        // ── Functions ───────────────────────────────────────────────────
        if object_capabilities.supports_functions
            && (!has_search || functions_loading || !filtered_functions.is_empty())
        {
            let toggle = toggle_section.clone();
            let header = self.render_section_header(
                SectionHeaderProps {
                    element_id: SharedString::from(format!("functions-header-{}", id_suffix)),
                    icon: Icon::new(ZqlzIcon::Function).size_3().into_any_element(),
                    label: "Functions",
                    total_count: functions.len(),
                    filtered_count: filtered_functions.len(),
                    is_expanded: functions_expanded,
                    on_click: move |this: &mut Self,
                                    _: &ClickEvent,
                                    _: &mut Window,
                                    cx: &mut Context<Self>| {
                        toggle(this, "functions", cx)
                    },
                    on_right_click: Some(
                        move |this: &mut Self,
                              event: &MouseDownEvent,
                              window: &mut Window,
                              cx: &mut Context<Self>| {
                            this.show_section_context_menu(
                                conn_id,
                                "functions",
                                event.position,
                                window,
                                cx,
                            );
                        },
                    ),
                    muted_foreground,
                    list_hover,
                    depth,
                },
                cx,
            );

            let mut section = v_flex().w_full().child(header);
            if functions_expanded {
                if functions_loading {
                    section = section.child(Self::render_loading_row(
                        id_suffix,
                        "functions",
                        muted_foreground,
                        depth + 1,
                    ));
                } else {
                    for function_name in &filtered_functions {
                        let func = (*function_name).clone();
                        let name_for_menu = (*function_name).clone();
                        let object_schema = self.current_schema_for(database_name.as_deref());
                        let object_schema_for_click = object_schema.clone();
                        let object_schema_for_menu = object_schema.clone();
                        section = section.child(Self::render_leaf_item(
                            LeafItemProps {
                                element_id: SharedString::from(format!(
                                    "function-{}-{}",
                                    id_suffix, function_name
                                )),
                                icon: Icon::new(ZqlzIcon::Function)
                                    .size_3()
                                    .text_color(muted_foreground)
                                    .into_any_element(),
                                label: (*function_name).clone(),
                                on_click: move |_this: &mut Self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>| {
                                    cx.emit(ConnectionSidebarEvent::OpenFunction {
                                        connection_id: conn_id,
                                        function_name: func.clone(),
                                        object_schema: object_schema_for_click.clone(),
                                    });
                                },
                                on_right_click: Some(
                                    move |this: &mut Self,
                                          event: &MouseDownEvent,
                                          window: &mut Window,
                                          cx: &mut Context<Self>| {
                                        this.show_function_context_menu(
                                            conn_id,
                                            name_for_menu.clone(),
                                            object_schema_for_menu.clone(),
                                            event.position,
                                            window,
                                            cx,
                                        );
                                    },
                                ),
                                list_hover,
                                depth: leaf_depth,
                            },
                            cx,
                        ));
                    }
                }
            }
            tree = tree.child(section);
        }

        // ── Procedures ──────────────────────────────────────────────────
        if object_capabilities.supports_procedures
            && (!has_search || procedures_loading || !filtered_procedures.is_empty())
        {
            let toggle = toggle_section.clone();
            let header = self.render_section_header(
                SectionHeaderProps {
                    element_id: SharedString::from(format!("procedures-header-{}", id_suffix)),
                    icon: Icon::new(ZqlzIcon::Gear).size_3().into_any_element(),
                    label: "Procedures",
                    total_count: procedures.len(),
                    filtered_count: filtered_procedures.len(),
                    is_expanded: procedures_expanded,
                    on_click: move |this: &mut Self,
                                    _: &ClickEvent,
                                    _: &mut Window,
                                    cx: &mut Context<Self>| {
                        toggle(this, "procedures", cx)
                    },
                    on_right_click: Some(
                        move |this: &mut Self,
                              event: &MouseDownEvent,
                              window: &mut Window,
                              cx: &mut Context<Self>| {
                            this.show_section_context_menu(
                                conn_id,
                                "procedures",
                                event.position,
                                window,
                                cx,
                            );
                        },
                    ),
                    muted_foreground,
                    list_hover,
                    depth,
                },
                cx,
            );

            let mut section = v_flex().w_full().child(header);
            if procedures_expanded {
                if procedures_loading {
                    section = section.child(Self::render_loading_row(
                        id_suffix,
                        "procedures",
                        muted_foreground,
                        depth + 1,
                    ));
                } else {
                    for procedure_name in &filtered_procedures {
                        let proc = (*procedure_name).clone();
                        let name_for_menu = (*procedure_name).clone();
                        let object_schema = self.current_schema_for(database_name.as_deref());
                        let object_schema_for_click = object_schema.clone();
                        let object_schema_for_menu = object_schema.clone();
                        section = section.child(Self::render_leaf_item(
                            LeafItemProps {
                                element_id: SharedString::from(format!(
                                    "procedure-{}-{}",
                                    id_suffix, procedure_name
                                )),
                                icon: Icon::new(ZqlzIcon::Gear)
                                    .size_3()
                                    .text_color(muted_foreground)
                                    .into_any_element(),
                                label: (*procedure_name).clone(),
                                on_click: move |_this: &mut Self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>| {
                                    cx.emit(ConnectionSidebarEvent::OpenProcedure {
                                        connection_id: conn_id,
                                        procedure_name: proc.clone(),
                                        object_schema: object_schema_for_click.clone(),
                                    });
                                },
                                on_right_click: Some(
                                    move |this: &mut Self,
                                          event: &MouseDownEvent,
                                          window: &mut Window,
                                          cx: &mut Context<Self>| {
                                        this.show_procedure_context_menu(
                                            conn_id,
                                            name_for_menu.clone(),
                                            object_schema_for_menu.clone(),
                                            event.position,
                                            window,
                                            cx,
                                        );
                                    },
                                ),
                                list_hover,
                                depth: leaf_depth,
                            },
                            cx,
                        ));
                    }
                }
            }
            tree = tree.child(section);
        }

        // ── Saved Queries ───────────────────────────────────────────────
        if !queries.is_empty() && (!has_search || !filtered_queries.is_empty()) {
            let toggle = toggle_section.clone();
            let header = self.render_section_header(
                SectionHeaderProps {
                    element_id: SharedString::from(format!("queries-header-{}", id_suffix)),
                    icon: Icon::new(ZqlzIcon::FileSql).size_3().into_any_element(),
                    label: "Queries",
                    total_count: queries.len(),
                    filtered_count: filtered_queries.len(),
                    is_expanded: queries_expanded,
                    on_click: move |this: &mut Self,
                                    _: &ClickEvent,
                                    _: &mut Window,
                                    cx: &mut Context<Self>| {
                        toggle(this, "queries", cx)
                    },
                    on_right_click: Some(
                        move |this: &mut Self,
                              event: &MouseDownEvent,
                              window: &mut Window,
                              cx: &mut Context<Self>| {
                            this.show_section_context_menu(
                                conn_id,
                                "queries",
                                event.position,
                                window,
                                cx,
                            );
                        },
                    ),
                    muted_foreground,
                    list_hover,
                    depth,
                },
                cx,
            );

            let mut section = v_flex().w_full().child(header);
            if queries_expanded {
                for query in &filtered_queries {
                    let query_id = query.id;
                    let query_name = query.name.clone();
                    let name_for_click = query.name.clone();
                    let name_for_menu = query.name.clone();
                    section = section.child(Self::render_leaf_item(
                        LeafItemProps {
                            element_id: SharedString::from(format!("query-{}-{}", id_suffix, query_id)),
                            icon: Icon::new(ZqlzIcon::FileSql)
                                .size_3()
                                .text_color(muted_foreground)
                                .into_any_element(),
                            label: query_name,
                            on_click: move |_this: &mut Self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>| {
                                tracing::info!(query_id = %query_id, query_name = %name_for_click, "Sidebar query item clicked");
                                cx.emit(ConnectionSidebarEvent::OpenSavedQuery {
                                    connection_id: conn_id,
                                    query_id,
                                    query_name: name_for_click.clone(),
                                });
                            },
                            on_right_click: Some(
                                move |this: &mut Self,
                                      event: &MouseDownEvent,
                                      window: &mut Window,
                                      cx: &mut Context<Self>| {
                                    this.show_query_context_menu(
                                        conn_id,
                                        query_id,
                                        name_for_menu.clone(),
                                        event.position,
                                        window,
                                        cx,
                                    );
                                },
                            ),
                            list_hover,
                            depth: leaf_depth,
                        },
                        cx,
                    ));
                }
            }
            tree = tree.child(section);
        }

        tree
    }

    /// Render a "Loading..." placeholder row for a section that is being fetched.
    fn render_loading_row(
        id_suffix: &str,
        section: &str,
        muted_foreground: gpui::Hsla,
        depth: usize,
    ) -> impl IntoElement {
        use zqlz_ui::widgets::h_flex;
        let indent = px(8.0 + depth as f32 * 12.0);
        h_flex()
            .id(SharedString::from(format!(
                "loading-{}-{}",
                section, id_suffix
            )))
            .w_full()
            .pl(indent)
            .pr_2()
            .h(px(24.0))
            .items_center()
            .text_xs()
            .text_color(muted_foreground.opacity(0.6))
            .child("Loading...")
    }
}
