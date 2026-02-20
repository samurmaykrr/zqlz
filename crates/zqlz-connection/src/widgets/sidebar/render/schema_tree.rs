//! SQL schema tree rendering
//!
//! Handles rendering of SQL database schema trees with support for both
//! single-database and multi-database modes.

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;

use crate::widgets::sidebar::{ConnectionSidebar, SavedQueryInfo, SidebarDatabaseInfo};
use zqlz_ui::widgets::{caption, h_flex, v_flex, ActiveTheme, Icon, IconName, ZqlzIcon};

impl ConnectionSidebar {
    /// Render the schema tree for SQL database connections.
    ///
    /// This function handles both simple (single-database) and complex
    /// (multi-database) scenarios:
    ///
    /// **Single-database mode** (SQLite, single PostgreSQL database):
    /// - Renders schema objects directly under the connection
    /// - No intermediate database nodes
    ///
    /// **Multi-database mode** (PostgreSQL with multiple DBs, MySQL):
    /// - Renders a database node for each database on the server
    /// - Each database can be expanded to show its schema
    /// - Schema is loaded on-demand when user expands a database
    /// - Active database shows existing connection-level schema
    ///
    /// # Schema Node Optimization
    ///
    /// In some cases, the schema folder is redundant and skipped:
    /// - MySQL: The schema name typically matches the database name
    /// - When schema name == database name, objects render directly under database
    /// - This reduces unnecessary nesting in the tree
    ///
    /// # Parameters
    ///
    /// - `conn_id`: Connection UUID
    /// - `tables`, `views`, etc.: Connection-level schema (for active DB or single DB)
    /// - `*_expanded`: Expansion states for connection-level sections
    /// - `databases`: List of all databases on server (empty for single-DB mode)
    /// - `schema_name`: Schema name for hierarchy display (e.g., "public")
    /// - `schema_expanded`: Whether schema-level node is expanded
    /// - `_window`: Window context (unused but kept for consistency)
    /// - `cx`: App context
    ///
    /// # Returns
    ///
    /// An `AnyElement` containing the complete schema tree for this connection.
    pub(super) fn render_schema_tree(
        &self,
        conn_id: Uuid,
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
        databases: &[SidebarDatabaseInfo],
        schema_name: Option<&str>,
        schema_expanded: bool,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        // Simple case: no multi-database — render objects tree directly
        if databases.is_empty() {
            let objects_tree = self.render_objects_tree(
                conn_id,
                &conn_id.to_string(),
                None,
                tables,
                views,
                materialized_views,
                triggers,
                functions,
                procedures,
                queries,
                tables_expanded,
                views_expanded,
                materialized_views_expanded,
                triggers_expanded,
                functions_expanded,
                procedures_expanded,
                queries_expanded,
                tables_loading,
                views_loading,
                materialized_views_loading,
                triggers_loading,
                functions_loading,
                procedures_loading,
                move |this: &mut ConnectionSidebar, section, cx| match section {
                    "tables" => this.toggle_tables_expand(conn_id, cx),
                    "views" => this.toggle_views_expand(conn_id, cx),
                    "materialized_views" => this.toggle_materialized_views_expand(conn_id, cx),
                    "triggers" => this.toggle_triggers_expand(conn_id, cx),
                    "functions" => this.toggle_functions_expand(conn_id, cx),
                    "procedures" => this.toggle_procedures_expand(conn_id, cx),
                    "queries" => this.toggle_queries_expand(conn_id, cx),
                    _ => {}
                },
                1,
                cx,
            );
            return objects_tree.into_any_element();
        }

        // Multi-database: build per-database tree nodes
        let muted_fg = cx.theme().muted_foreground;
        let muted_fg_half = muted_fg.opacity(0.5);
        let muted_fg_dim = muted_fg.opacity(0.4);
        let list_hover = cx.theme().list_hover;

        let mut db_nodes: Vec<AnyElement> = Vec::with_capacity(databases.len());

        // Build fallback tree for active database
        let active_db_name = databases
            .iter()
            .find(|d| d.is_active)
            .map(|d| d.name.as_str());
        let fallback_shows_schema =
            schema_name.is_some_and(|s| active_db_name.map_or(true, |db| s != db));
        let fallback_depth: usize = if fallback_shows_schema { 3 } else { 2 };

        let mut fallback_tree: Option<AnyElement> = Some(
            self.render_objects_tree(
                conn_id,
                &conn_id.to_string(),
                active_db_name.map(|s| s.to_string()),
                tables,
                views,
                materialized_views,
                triggers,
                functions,
                procedures,
                queries,
                tables_expanded,
                views_expanded,
                materialized_views_expanded,
                triggers_expanded,
                functions_expanded,
                procedures_expanded,
                queries_expanded,
                tables_loading,
                views_loading,
                materialized_views_loading,
                triggers_loading,
                functions_loading,
                procedures_loading,
                move |this: &mut ConnectionSidebar, section, cx| match section {
                    "tables" => this.toggle_tables_expand(conn_id, cx),
                    "views" => this.toggle_views_expand(conn_id, cx),
                    "materialized_views" => this.toggle_materialized_views_expand(conn_id, cx),
                    "triggers" => this.toggle_triggers_expand(conn_id, cx),
                    "functions" => this.toggle_functions_expand(conn_id, cx),
                    "procedures" => this.toggle_procedures_expand(conn_id, cx),
                    "queries" => this.toggle_queries_expand(conn_id, cx),
                    _ => {}
                },
                fallback_depth,
                cx,
            )
            .into_any_element(),
        );

        for db in databases {
            let db_name = db.name.clone();
            let db_name_click = db.name.clone();
            let is_expanded = db.is_expanded;
            let has_schema = db.schema.is_some();
            let size_label = db.size_bytes.map(Self::format_database_size);

            let mut node = v_flex().w_full();

            // Database row
            let db_row = h_flex()
                .id(SharedString::from(format!(
                    "db-node-{}-{}",
                    conn_id, &db_name
                )))
                .w_full()
                .pl(px(20.0))
                .pr_2()
                .h(px(24.0))
                .gap_1p5()
                .items_center()
                .text_xs()
                .text_color(if has_schema { muted_fg } else { muted_fg_half })
                .cursor_pointer()
                .hover(|el| el.bg(list_hover))
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.toggle_database_expand(conn_id, &db_name_click, cx);
                }))
                .child(
                    Icon::new(if is_expanded {
                        IconName::ChevronDown
                    } else {
                        IconName::ChevronRight
                    })
                    .size_3(),
                )
                .child(
                    Icon::new(ZqlzIcon::Database)
                        .size_3()
                        .when(!has_schema, |el| el.text_color(muted_fg_half)),
                )
                .child(db_name.clone())
                .when_some(size_label, |el, size| {
                    el.child(caption(size).color(muted_fg_dim))
                });

            node = node.child(db_row);

            if is_expanded {
                if has_schema || db.is_active {
                    let db_schema = db.schema.as_ref();
                    let sch_name = db_schema
                        .and_then(|s| s.schema_name.clone())
                        .or_else(|| schema_name.map(|s| s.to_string()));
                    let sch_expanded = db_schema.map_or(schema_expanded, |s| s.schema_expanded);
                    let db_name_for_toggle = db.name.clone();

                    let show_schema_node = sch_name.as_ref().map_or(false, |s| s != &db_name);
                    let objects_depth: usize = if show_schema_node { 3 } else { 2 };

                    let tree: Option<AnyElement> = if let Some(schema) = db_schema {
                        let db_name_for_closure = db.name.clone();
                        Some(
                            self.render_objects_tree(
                                conn_id,
                                &format!("{}-{}", conn_id, db.name),
                                Some(db_name.clone()),
                                &schema.tables,
                                &schema.views,
                                &schema.materialized_views,
                                &schema.triggers,
                                &schema.functions,
                                &schema.procedures,
                                queries,
                                schema.tables_expanded,
                                schema.views_expanded,
                                schema.materialized_views_expanded,
                                schema.triggers_expanded,
                                schema.functions_expanded,
                                schema.procedures_expanded,
                                queries_expanded,
                                schema.tables_loading,
                                schema.views_loading,
                                schema.materialized_views_loading,
                                schema.triggers_loading,
                                schema.functions_loading,
                                schema.procedures_loading,
                                move |this: &mut ConnectionSidebar, section, cx| {
                                    this.toggle_db_section(
                                        conn_id,
                                        &db_name_for_closure,
                                        section,
                                        cx,
                                    );
                                },
                                objects_depth,
                                cx,
                            )
                            .into_any_element(),
                        )
                    } else {
                        fallback_tree.take()
                    };

                    if show_schema_node {
                        node = node.child(
                            v_flex()
                                .w_full()
                                .when_some(sch_name, |el, sch| {
                                    el.child(
                                        h_flex()
                                            .id(SharedString::from(format!(
                                                "schema-node-{}-{}",
                                                conn_id, &db_name
                                            )))
                                            .w_full()
                                            .pl(px(32.0))
                                            .pr_2()
                                            .h(px(24.0))
                                            .gap_1p5()
                                            .items_center()
                                            .text_xs()
                                            .text_color(muted_fg)
                                            .cursor_pointer()
                                            .hover(|el| el.bg(list_hover))
                                            .on_click(cx.listener(move |this, _, _, cx| {
                                                this.toggle_db_section(
                                                    conn_id,
                                                    &db_name_for_toggle,
                                                    "schema",
                                                    cx,
                                                );
                                            }))
                                            .child(
                                                Icon::new(if sch_expanded {
                                                    IconName::ChevronDown
                                                } else {
                                                    IconName::ChevronRight
                                                })
                                                .size_3(),
                                            )
                                            .child(Icon::new(IconName::Folder).size_3())
                                            .child(sch),
                                    )
                                })
                                .when_some(tree, |el, objects| {
                                    el.when(sch_expanded, |el| el.child(objects))
                                }),
                        );
                    } else {
                        node = node.when_some(tree, |el, objects| el.child(objects));
                    }
                } else {
                    node = node.child(
                        h_flex()
                            .w_full()
                            .pl(px(32.0))
                            .pr_2()
                            .h(px(24.0))
                            .gap_1p5()
                            .items_center()
                            .text_xs()
                            .text_color(muted_fg_dim)
                            .child(caption("Loading schema...").color(muted_fg_dim)),
                    );
                }
            }

            db_nodes.push(node.into_any_element());
        }

        v_flex()
            .w_full()
            .gap_px()
            .children(db_nodes)
            .into_any_element()
    }

    /// Format byte count as a human-readable database size string.
    ///
    /// Converts raw byte counts into user-friendly units (B, KB, MB, GB)
    /// with appropriate decimal precision.
    ///
    /// # Examples
    ///
    /// ```
    /// format_database_size(512);           // "512 B"
    /// format_database_size(1536);          // "1.5 KB"
    /// format_database_size(1048576);       // "1.0 MB"
    /// format_database_size(5368709120);    // "5.0 GB"
    /// ```
    ///
    /// # Parameters
    ///
    /// - `bytes`: Size in bytes
    ///
    /// # Returns
    ///
    /// A formatted string with appropriate unit (B, KB, MB, or GB)
    fn format_database_size(bytes: i64) -> String {
        if bytes < 1024 {
            format!("{} B", bytes)
        } else if bytes < 1024 * 1024 {
            format!("{:.1} KB", bytes as f64 / 1024.0)
        } else if bytes < 1024 * 1024 * 1024 {
            format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
        } else {
            format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        }
    }
}
