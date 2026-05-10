// This module handles table renaming operations.

use gpui::*;
use uuid::Uuid;

use crate::app::AppState;
use crate::main_view::MainView;
use crate::main_view::rename_window::RenameWindow;

impl MainView {
    pub(in crate::main_view) fn rename_table(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Rename table: {} on connection {}",
            table_name,
            connection_id
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let table_service = app_state.table_service.clone();

        let driver_name = app_state
            .connection_service
            .get_saved_connection_driver(connection_id)
            .unwrap_or_else(|| connection.driver_name().to_string());

        RenameWindow::open_table(
            connection_id,
            table_name,
            driver_name,
            connection.clone(),
            table_service,
            cx.entity().downgrade(),
            cx,
        );
    }
}
