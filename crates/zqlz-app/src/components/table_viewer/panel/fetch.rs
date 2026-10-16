use std::sync::Arc;

use zqlz_services::{
    FetchMode, FetchOutcome, FetchPlan, KnownTotal, QuerySnapshot, RowWindow, ServiceResult,
    plan_fetch,
};
use zqlz_ui::widgets::{WindowExt, dialog::DialogButtonProps, notification::Notification};

use super::*;
use crate::app::AppState;
use crate::main_view::table_handlers_utils::sql::{
    build_search_clause_for_columns, resolve_search_columns,
};

/// Turning the limit off on a table larger than this, or of unknown size, asks
/// first: every row lands in the grid at once.
const CONFIRM_UNLIMITED_FETCH_ABOVE_ROWS: u64 = 100_000;

/// A planned row fetch that is running but not yet applied.
pub(crate) struct PendingFetch {
    pub generation: u64,
    pub plan: FetchPlan,
    pub rows: Task<ServiceResult<FetchOutcome>>,
}

impl TableViewerPanel {
    /// Load rows for `mode`. Replacing the stored task cancels any fetch still in
    /// flight, so only the latest request can land.
    pub fn reload(&mut self, mode: FetchMode, window: &mut Window, cx: &mut Context<Self>) {
        // Key-value and document viewers hold all their rows client-side.
        if matches!(
            self.driver_category,
            DriverCategory::KeyValue | DriverCategory::Document
        ) {
            return;
        }
        let Some(connection_id) = self.connection_id else {
            return;
        };
        let Some(app_state) = cx.try_global::<AppState>() else {
            return;
        };
        // The open set `table.database` only when the table needs its own
        // per-database connection; otherwise the main connection serves it.
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(
                connection_id,
                self.table
                    .as_ref()
                    .and_then(|table| table.database.as_deref()),
            )
        else {
            self.finish_failed_fetch(
                "Connection is no longer open".to_string(),
                mode == FetchMode::LoadMore,
                window,
                cx,
            );
            return;
        };
        let Some(fetch) = self.start_fetch(mode, connection, cx) else {
            return;
        };
        self.fetch_task = Some(cx.spawn_in(window, async move |this, cx| {
            let outcome = fetch.rows.await;
            if let Err(error) = this.update_in(cx, |this, window, cx| {
                this.apply_fetch(fetch.generation, outcome, window, cx);
            }) {
                tracing::debug!(%error, "Fetched rows arrived after the viewer closed");
            }
        }));
    }

    /// The limit checkbox was cleared. Reload every row, after asking when the
    /// table is big or its size unknown; cancelling turns the limit back on.
    pub(super) fn limit_turned_off(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let (total, is_estimated) = self
            .pagination_state
            .as_ref()
            .map(|pagination| {
                let pagination = pagination.read(cx);
                (pagination.total_records, pagination.is_estimated)
            })
            .unwrap_or_default();
        if total.is_some_and(|rows| rows <= CONFIRM_UNLIMITED_FETCH_ABOVE_ROWS) {
            self.reload(FetchMode::Page(1), window, cx);
            return;
        }

        let message = match total {
            Some(rows) if is_estimated => {
                format!(
                    "This table has about {rows} rows. All of them will load into the grid at once."
                )
            }
            Some(rows) => {
                format!("This table has {rows} rows. All of them will load into the grid at once.")
            }
            None => {
                "The row count is unknown. Every row will load into the grid at once.".to_string()
            }
        };
        let viewer = cx.entity().downgrade();
        window.open_dialog(cx, move |dialog, _window, _cx| {
            dialog
                .title("Load every row?")
                .child(message.clone())
                .button_props(DialogButtonProps::default().ok_text("Load All"))
                .on_ok({
                    let viewer = viewer.clone();
                    move |_, window, cx| {
                        if let Err(error) = viewer.update(cx, |viewer, cx| {
                            viewer.reload(FetchMode::Page(1), window, cx)
                        }) {
                            tracing::debug!(%error, "Viewer closed before loading every row");
                        }
                        true
                    }
                })
                .on_cancel({
                    let viewer = viewer.clone();
                    move |_, _window, cx| {
                        if let Err(error) = viewer.update(cx, |viewer, cx| {
                            // Set directly: emitting LimitEnabledChanged would refetch
                            // the page that is already showing.
                            if let Some(pagination) = &viewer.pagination_state {
                                pagination.update(cx, |pagination, cx| {
                                    pagination.limit_enabled = true;
                                    cx.notify();
                                });
                            }
                        }) {
                            tracing::debug!(%error, "Viewer closed before restoring its limit");
                        }
                        true
                    }
                })
                .confirm()
        });
    }

    /// Plan a fetch from the viewer's state and start it on `connection`. The
    /// caller hands the result to [`Self::apply_fetch`]; `reload` does both, and
    /// the open flow does it itself so it can fall back to a schema-only grid.
    pub(crate) fn start_fetch(
        &mut self,
        mode: FetchMode,
        connection: Arc<dyn zqlz_core::Connection>,
        cx: &mut Context<Self>,
    ) -> Option<PendingFetch> {
        // Not resolved yet: the open in flight fetches once it is.
        let table = self.table.clone()?;
        let table_service = cx.try_global::<AppState>()?.table_service.clone();
        let query = self.query_snapshot(mode, &connection, cx);
        let Some(plan) = plan_fetch(mode, &query) else {
            self.set_load_more_pending(false, cx);
            return None;
        };
        let generation = if plan.append {
            self.active_request_generation
        } else {
            self.begin_data_request(cx)
        };
        let rows = cx.background_spawn({
            let plan = plan.clone();
            async move {
                table_service
                    .fetch_rows(connection, &table, &query, plan)
                    .await
            }
        });
        Some(PendingFetch {
            generation,
            plan,
            rows,
        })
    }

    /// Record what an open resolved, so fetches run against it.
    pub(crate) fn set_resolved_table(
        &mut self,
        table: zqlz_core::TableRef,
        database_name: Option<String>,
        connection_name: String,
        is_view: bool,
        driver_category: DriverCategory,
        primary_key_columns: Vec<String>,
    ) {
        self.database_name = database_name;
        self.table = Some(table);
        self.connection_name = Some(connection_name);
        self.is_view = is_view;
        self.driver_category = driver_category;
        self.primary_key_columns = primary_key_columns;
    }

    /// The only place viewer state is turned into a query.
    fn query_snapshot(
        &self,
        mode: FetchMode,
        connection: &Arc<dyn zqlz_core::Connection>,
        cx: &App,
    ) -> QuerySnapshot {
        let (filters, sorts) = self
            .filter_panel_state
            .as_ref()
            .map(|state| {
                let state = state.read(cx);
                (state.get_filter_conditions(), state.get_sort_criteria())
            })
            .unwrap_or_default();

        let mut where_clauses: Vec<String> =
            crate::components::table_viewer::filter_types::combine_filter_sql(&filters, |filter| {
                filter.to_sql_for_connection(connection.as_ref())
            })
            .into_iter()
            .collect();
        let search_columns = resolve_search_columns(
            &self.column_meta,
            self.performance_profile
                .as_ref()
                .map(|profile| profile.searchable_columns.clone()),
        );
        if let Some(search_clause) =
            build_search_clause_for_columns(connection, &search_columns, &self.search_text, false)
        {
            where_clauses.push(search_clause);
        }

        let visible_columns = self
            .column_visibility_state
            .as_ref()
            .map(|state| state.read(cx).visible_columns())
            .unwrap_or_else(|| self.column_meta.iter().map(|c| c.name.clone()).collect());

        let pagination = self.pagination_state.as_ref().map(|state| state.read(cx));
        let known_total = pagination.and_then(|pagination| {
            pagination.total_records.map(|rows| KnownTotal {
                rows,
                is_estimated: pagination.is_estimated,
                where_clauses: self.counted_where_clauses.clone(),
            })
        });
        // Only load-more needs the grid's rows, and it always runs deferred. Other
        // reloads can start inside the table's own update (clearing a header sort),
        // where reading it would panic.
        let loaded_rows = match (mode, &self.table_state) {
            (FetchMode::LoadMore, Some(table)) => table.read(cx).delegate().rows.len(),
            _ => 0,
        };

        QuerySnapshot {
            where_clauses,
            order_by_clauses: sorts
                .iter()
                .map(|sort| sort.to_sql_for_connection(connection.as_ref()))
                .collect(),
            visible_columns,
            pk_columns: self
                .primary_key_columns
                .iter()
                .map(|column| connection.quote_identifier(column))
                .collect(),
            page_size: match pagination {
                Some(pagination) => pagination
                    .limit_enabled
                    .then_some(pagination.records_per_page),
                None => Some(1000),
            },
            current_page: pagination.map_or(1, |pagination| pagination.current_page),
            loaded_rows,
            known_total,
            fast_exact_count: connection.supports_fast_exact_count(),
            is_loading: self.is_loading,
        }
    }

    /// The single place fetched rows land, and where every failure clears its
    /// spinners.
    pub(crate) fn apply_fetch(
        &mut self,
        request_generation: u64,
        outcome: ServiceResult<FetchOutcome>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.is_current_request(request_generation) {
            return;
        }
        let FetchOutcome {
            result,
            total,
            plan,
        } = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.finish_failed_fetch(format!("Failed to load rows: {error}"), true, window, cx);
                return;
            }
        };

        if plan.append {
            self.append_rows(result, &plan, cx);
            return;
        }

        self.counted_where_clauses = total.map(|total| total.where_clauses).unwrap_or_default();
        if let Some(pagination) = &self.pagination_state {
            pagination.update(cx, |pagination, _cx| pagination.current_page = plan.page);
        }
        let (Some(connection_id), Some(table_name)) = (self.connection_id, self.table_name.clone())
        else {
            return;
        };
        self.load_table(
            connection_id,
            self.connection_name.clone().unwrap_or_default(),
            table_name,
            self.database_name.clone(),
            self.is_view,
            result,
            self.driver_category,
            window,
            cx,
        );
    }

    fn append_rows(&mut self, result: QueryResult, plan: &FetchPlan, cx: &mut Context<Self>) {
        let RowWindow::Forward { limit, offset } = plan.window else {
            return;
        };
        let rows_loaded = result.rows.len();
        let has_more = rows_loaded >= limit;
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, cx| {
                table.delegate_mut().append_rows(
                    result.rows.into_iter().map(|row| row.values).collect(),
                    has_more,
                );
                // append_rows widens the row-number column; push that to the widget.
                table.refresh(cx);
            });
        }
        if let Some(pagination) = &self.pagination_state {
            pagination.update(cx, |pagination, cx| {
                pagination.records_in_current_page = offset + rows_loaded;
                pagination.has_more = has_more;
                cx.notify();
            });
        }
        cx.notify();
    }

    fn finish_failed_fetch(
        &mut self,
        message: String,
        clear_load_more: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::error!("{message}");
        self.set_loading(false, cx);
        if clear_load_more {
            self.set_load_more_pending(false, cx);
        }
        window.push_notification(Notification::error(message), cx);
    }

    fn set_load_more_pending(&self, pending: bool, cx: &mut Context<Self>) {
        if let Some(table_state) = &self.table_state {
            table_state.update(cx, |table, _cx| {
                table.delegate_mut().is_loading_more = pending
            });
        }
    }
}
