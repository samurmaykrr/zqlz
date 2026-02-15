//! Redis schema tree rendering
//!
//! Handles rendering of Redis-specific database trees with numbered databases.

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;

use crate::widgets::sidebar::{
    ConnectionSidebar, ConnectionSidebarEvent, RedisDatabaseInfo, SavedQueryInfo,
};
use zqlz_ui::widgets::{caption, h_flex, v_flex, ActiveTheme, Icon, Sizable, ZqlzIcon};

impl ConnectionSidebar {
    /// Render the Redis-specific schema tree.
    ///
    /// Redis connections have a fundamentally different structure from SQL databases:
    /// - No tables, views, or SQL objects
    /// - Instead, Redis has numbered databases (db0, db1, db2, ...)
    /// - Each database contains keys (not organized into tables)
    ///
    /// # Visual Structure
    ///
    /// ```text
    /// [v] Databases (16)
    ///     db0 (1234)              # Key count shown if known
    ///     db1 (0)
    ///     ...
    /// [v] Saved Queries (3)       # User-created queries still work
    ///     Query 1
    ///     Query 2
    /// ```
    ///
    /// # Lazy Loading
    ///
    /// Keys are loaded on-demand when a database is clicked:
    /// 1. User clicks on a Redis database node
    /// 2. Event `LoadRedisKeys` is emitted
    /// 3. Parent component fetches keys from Redis
    /// 4. Keys are populated via `set_redis_keys`
    ///
    /// This prevents loading potentially millions of keys upfront for all databases.
    ///
    /// # Parameters
    ///
    /// - `conn_id`: Connection UUID
    /// - `databases`: List of Redis databases with their key counts
    /// - `databases_expanded`: Whether databases section is expanded
    /// - `queries`: Saved queries for this connection
    /// - `queries_expanded`: Whether queries section is expanded
    /// - `_window`: Window context (unused but kept for consistency)
    /// - `cx`: App context
    ///
    /// # Returns
    ///
    /// A tree element containing databases and saved queries sections.
    pub(super) fn render_redis_schema_tree(
        &self,
        conn_id: Uuid,
        databases: &[RedisDatabaseInfo],
        databases_expanded: bool,
        queries: &[SavedQueryInfo],
        queries_expanded: bool,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let muted_foreground = cx.theme().muted_foreground;
        let list_hover = cx.theme().list_hover;
        let font_family = cx.theme().font_family.clone();

        let has_search = !self.search_query.is_empty();
        let search_lower = self.search_query.to_lowercase();

        let filtered_databases: Vec<_> = databases
            .iter()
            .filter(|db| {
                if !has_search {
                    return true;
                }
                let db_name = format!("db{}", db.index);
                if db_name.contains(&search_lower) {
                    return true;
                }
                db.keys
                    .iter()
                    .any(|k| k.to_lowercase().contains(&search_lower))
            })
            .collect();

        let filtered_queries: Vec<_> = queries
            .iter()
            .filter(|q| self.matches_search(&q.name))
            .collect();

        let databases_expanded =
            databases_expanded || (has_search && !filtered_databases.is_empty());
        let queries_expanded = queries_expanded || (has_search && !filtered_queries.is_empty());

        let mut tree = v_flex().w_full().gap_px().font_family(font_family);

        // ── Databases section ─────────────────────────────────────────
        if !has_search || !filtered_databases.is_empty() {
            let header = self.render_section_header(
                SharedString::from(format!("databases-header-{}", conn_id)),
                Icon::new(ZqlzIcon::Database).size_3().into_any_element(),
                "Databases",
                databases.len(),
                filtered_databases.len(),
                databases_expanded,
                move |this, _, _, cx| {
                    this.toggle_redis_databases_expand(conn_id, cx);
                },
                muted_foreground,
                list_hover,
                1,
                cx,
            );

            let mut section = v_flex().w_full().child(header);
            if databases_expanded {
                for db in &filtered_databases {
                    let db_index = db.index;
                    let db_loading = db.is_loading;
                    let key_count = db.key_count;

                    let label = format!(
                        "db{}{}",
                        db_index,
                        key_count.map(|c| format!(" ({})", c)).unwrap_or_default()
                    );

                    let mut row = Self::render_leaf_item(
                        SharedString::from(format!("redis-db-{}-{}", conn_id, db_index)),
                        Icon::new(ZqlzIcon::Database)
                            .size_3()
                            .text_color(muted_foreground)
                            .into_any_element(),
                        label,
                        move |_this, _, _, cx| {
                            cx.emit(ConnectionSidebarEvent::OpenRedisDatabase {
                                connection_id: conn_id,
                                database_index: db_index,
                            });
                        },
                        None::<fn(&mut Self, &MouseDownEvent, &mut Window, &mut Context<Self>)>,
                        list_hover,
                        2,
                        cx,
                    );

                    if db_loading {
                        row = row.child(caption("...").color(muted_foreground));
                    }

                    section = section.child(row);
                }
            }
            tree = tree.child(section);
        }

        // ── Saved Queries section ─────────────────────────────────────
        if !queries.is_empty() && (!has_search || !filtered_queries.is_empty()) {
            let header = self.render_section_header(
                SharedString::from(format!("queries-header-{}", conn_id)),
                Icon::new(ZqlzIcon::FileSql).size_3().into_any_element(),
                "Saved Queries",
                queries.len(),
                filtered_queries.len(),
                queries_expanded,
                move |this, _, _, cx| {
                    this.toggle_queries_expand(conn_id, cx);
                },
                muted_foreground,
                list_hover,
                1,
                cx,
            );

            let mut section = v_flex().w_full().mt_1().child(header);
            if queries_expanded {
                for query in &filtered_queries {
                    let query_id = query.id;
                    let query_name = query.name.clone();
                    let name_for_click = query.name.clone();
                    let name_for_menu = query.name.clone();
                    section = section.child(Self::render_leaf_item(
                        SharedString::from(format!("query-{}-{}", conn_id, query_id)),
                        Icon::new(ZqlzIcon::FileSql)
                            .size_3()
                            .text_color(muted_foreground)
                            .into_any_element(),
                        query_name,
                        move |_this, _, _, cx| {
                            cx.emit(ConnectionSidebarEvent::OpenSavedQuery {
                                connection_id: conn_id,
                                query_id,
                                query_name: name_for_click.clone(),
                            });
                        },
                        Some(
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
                        2,
                        cx,
                    ));
                }
            }
            tree = tree.child(section);
        }

        tree
    }
}
