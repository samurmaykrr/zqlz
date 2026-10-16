//! This module contains standalone event handlers for table viewer lifecycle events (activation/deactivation).

use gpui::*;
use uuid::Uuid;
use zqlz_core::TableRef;
use zqlz_query::ResultsPanel;

use crate::app::AppState;
use crate::components::{InspectorPanel, InspectorView, SchemaDetailsPanel};
use crate::main_view::table_handlers_utils::conversion::convert_to_schema_details;
use crate::workspace::WorkspaceController;

pub(in crate::main_view) struct BecameActiveRequest {
    pub connection_id: Uuid,
    /// The resolved table for relational viewers; the key or collection for the
    /// others.
    pub table: TableRef,
}

pub(in crate::main_view) fn handle_became_active_event(
    request: BecameActiveRequest,
    schema_details_panel: Entity<SchemaDetailsPanel>,
    results_panel: Entity<ResultsPanel>,
    workspace_controller: &Entity<WorkspaceController>,
    inspector_panel: &Entity<InspectorPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "Table viewer became active: table={}, connection={}",
        request.table.display_name(),
        request.connection_id
    );

    // Clear problems when switching to a table viewer (no active query editor)
    results_panel.update(cx, |panel, cx| {
        panel.set_problems(Vec::new(), cx);
    });

    workspace_controller.update(cx, |workspace, cx| {
        workspace.reveal_panel(
            "InspectorPanel",
            zqlz_ui::widgets::dock::DockPlacement::Right,
            window,
            cx,
        );
    });

    // Switch to Schema view only if inspector is not currently showing
    // the Cell Editor (which the user explicitly opened via a cell click)
    let current_view = inspector_panel.read(cx).active_view();
    if current_view != InspectorView::CellEditor {
        inspector_panel.update(cx, |panel, cx| {
            panel.set_active_view(InspectorView::Schema, cx);
        });
    }

    let needs_update = schema_details_panel.read_with(cx, |panel, _cx| {
        !panel.is_showing_table(request.connection_id, &request.table)
    });

    if !needs_update {
        return;
    }

    let connection_id = request.connection_id;
    let table = request.table;
    let schema_panel = schema_details_panel.clone();

    let Some((connection, schema_service)) = cx.try_global::<AppState>().and_then(|app_state| {
        let connection = app_state
            .connections
            .get_for_database_cached(connection_id, table.database.as_deref())?;
        Some((connection, app_state.schema_service.clone()))
    }) else {
        tracing::error!("Failed to get connection or services");
        return;
    };

    // Activation is a view change, not a data change. Re-fetching the whole dossier
    // every time a tab regains focus put a spinner and a burst of catalog queries in
    // front of a panel whose contents had not changed. Serve the warm cache directly
    // and only reach for the database when nothing is cached.
    if let Some(table_details) = schema_service.peek_table_details_cache(
        connection.as_ref(),
        connection_id,
        &table.name,
        table.schema.as_deref(),
    ) {
        let create_statement = schema_service.peek_ddl_cache(
            connection.as_ref(),
            connection_id,
            &table.name,
            table.schema.as_deref(),
        );
        let details =
            convert_to_schema_details(connection_id, &table, table_details, create_statement);
        schema_details_panel.update(cx, |panel, cx| {
            panel.set_details(details, cx);
        });
        return;
    }

    window
        .spawn(cx, async move |cx| {
            let conn = connection;

            schema_panel.update(cx, |panel, cx| {
                panel.set_loading_for_table(connection_id, &table, cx);
            });

            match schema_service
                .get_table_details(
                    conn.clone(),
                    connection_id,
                    &table.name,
                    table.schema.as_deref(),
                )
                .await
            {
                Ok(table_details) => {
                    // Columns are what the panel mostly shows, and they are ready now.
                    // The DDL costs its own round-trip, so render twice rather than
                    // holding the spinner over content that has already arrived.
                    let details = convert_to_schema_details(
                        connection_id,
                        &table,
                        table_details.clone(),
                        None,
                    );
                    schema_panel.update(cx, |panel, cx| {
                        panel.set_details(details, cx);
                    });

                    let create_statement = schema_service
                        .get_or_generate_ddl(
                            &conn,
                            connection_id,
                            &table.name,
                            table.schema.as_deref(),
                            None,
                        )
                        .await;

                    if create_statement.is_some() {
                        let details = convert_to_schema_details(
                            connection_id,
                            &table,
                            table_details,
                            create_statement,
                        );
                        schema_panel.update(cx, |panel, cx| {
                            panel.set_details(details, cx);
                        });
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to load schema details: {}", e);
                    schema_panel.update(cx, |panel, cx| {
                        panel.set_loading(false, cx);
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
}

pub(in crate::main_view) fn handle_became_inactive_event(
    connection_id: Uuid,
    table: &TableRef,
    schema_details_panel: &Entity<SchemaDetailsPanel>,
    cx: &mut App,
) {
    tracing::info!(
        "Table viewer became inactive: table={}, connection={}",
        table.display_name(),
        connection_id
    );

    let should_clear = schema_details_panel.read_with(cx, |panel, _cx| {
        panel.is_showing_table(connection_id, table)
    });

    if should_clear {
        schema_details_panel.update(cx, |panel, cx| {
            panel.clear(cx);
        });
    }
}
