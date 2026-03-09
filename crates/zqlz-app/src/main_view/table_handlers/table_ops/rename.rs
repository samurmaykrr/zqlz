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

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let driver_name = app_state
            .saved_connections()
            .into_iter()
            .find(|c| c.id == connection_id)
            .map(|c| c.driver.clone())
            .unwrap_or_else(|| "sqlite".to_string());

        RenameWindow::open_table(
            connection_id,
            table_name,
            driver_name,
            connection.clone(),
            cx.entity().downgrade(),
            cx,
        );
    }
}
