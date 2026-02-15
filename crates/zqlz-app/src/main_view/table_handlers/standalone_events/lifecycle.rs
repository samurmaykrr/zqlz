//! This module contains standalone event handlers for table viewer lifecycle events (activation/deactivation).

use gpui::*;
use uuid::Uuid;
use zqlz_query::ResultsPanel;
use zqlz_ui::widgets::WindowExt;

use crate::app::AppState;
use crate::components::{InspectorPanel, InspectorView, SchemaDetailsPanel};
use crate::main_view::table_handlers_utils::{
    conversion::{convert_to_schema_details, resolve_schema_qualifier},
    generate_ddl_for_table,
};

pub(in crate::main_view) fn handle_became_active_event(
    connection_id: Uuid,
    table_name: &str,
    database_name: Option<&str>,
    schema_details_panel: Entity<SchemaDetailsPanel>,
    results_panel: Entity<ResultsPanel>,
    dock_area: &Entity<zqlz_ui::widgets::dock::DockArea>,
    inspector_panel: &Entity<InspectorPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "Table viewer became active: table={}, connection={}",
        table_name,
        connection_id
    );

    // Clear problems when switching to a table viewer (no active query editor)
    results_panel.update(cx, |panel, cx| {
        panel.set_problems(Vec::new(), cx);
    });

    // Ensure Inspector panel is visible when a table becomes active
    _ = dock_area.update(cx, |area, cx| {
        area.activate_panel(
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
        _ = inspector_panel.update(cx, |panel, cx| {
            panel.set_active_view(InspectorView::Schema, cx);
        });
    }

    let needs_update = schema_details_panel.read_with(cx, |panel, _cx| {
        !panel.is_showing_table(connection_id, table_name)
    });

    if !needs_update {
        return;
    }

    let table_name = table_name.to_string();
    let database_name = database_name.map(|s| s.to_string());
    let schema_panel = schema_details_panel.clone();

    window
        .spawn(cx, async move |cx| {
            let (conn, schema_service) = match cx.update(|_window, cx| {
                let app_state = cx.try_global::<AppState>()?;
                let conn = app_state.connections.get(connection_id);
                let schema_service = app_state.schema_service.clone();
                Some((conn?, schema_service))
            }) {
                Ok(Some(result)) => result,
                _ => {
                    tracing::error!("Failed to get connection or services");
                    return anyhow::Ok(());
                }
            };

            _ = schema_panel.update(cx, |panel, cx| {
                panel.set_loading_for_table(connection_id, &table_name, cx);
            });

            let schema_qualifier =
                resolve_schema_qualifier(conn.driver_name(), &database_name);

            match schema_service
                .get_table_details(conn.clone(), connection_id, &table_name, schema_qualifier.as_deref())
                .await
            {
                Ok(table_details) => {
                    let create_statement = generate_ddl_for_table(&conn, &table_name).await;
                    let details = convert_to_schema_details(
                        connection_id,
                        &table_name,
                        table_details,
                        create_statement,
                    );

                    _ = schema_panel.update(cx, |panel, cx| {
                        panel.set_details(details, cx);
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to load schema details: {}", e);
                    _ = schema_panel.update(cx, |panel, cx| {
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
    table_name: &str,
    schema_details_panel: &Entity<SchemaDetailsPanel>,
    cx: &mut App,
) {
    tracing::info!(
        "Table viewer became inactive: table={}, connection={}",
        table_name,
        connection_id
    );

    let should_clear = schema_details_panel.read_with(cx, |panel, _cx| {
        panel.is_showing_table(connection_id, table_name)
    });

    if should_clear {
        _ = schema_details_panel.update(cx, |panel, cx| {
            panel.clear(cx);
        });
    }
}
