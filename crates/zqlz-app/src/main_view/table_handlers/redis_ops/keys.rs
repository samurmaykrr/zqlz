//! This module handles loading Redis keys from a database.

use gpui::{Context, Window};
use uuid::Uuid;
use zqlz_services::LoadKeyValueKeysRequest;

use crate::app::AppState;
use crate::main_view::MainView;

impl MainView {
    pub(in crate::main_view) fn load_redis_keys(
        &mut self,
        connection_id: Uuid,
        database_index: u16,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Loading Redis keys for connection {} database {}",
            connection_id,
            database_index
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let database_name = format!("db{}", database_index);

        let connection_service = app_state.connection_service.clone();
        let key_value_service = app_state.key_value_service.clone();
        let connection_sidebar = self.connection_sidebar.clone();

        cx.spawn_in(window, async move |_this, cx| {
            let connection = match connection_service
                .get_connection_for_database(connection_id, &database_name)
                .await
            {
                Ok(connection) => connection,
                Err(error) => {
                    tracing::error!(
                        connection_id = %connection_id,
                        database = %database_name,
                        error = %error,
                        "Failed to get Redis database-specific connection"
                    );
                    return anyhow::Ok(());
                }
            };

            let key_load_request = LoadKeyValueKeysRequest {
                database_index,
                limit: 1000,
                scan_batch_size: 100,
            };
            let keys = match key_value_service
                .load_keys(connection, key_load_request)
                .await
            {
                Ok(outcome) => outcome.keys,
                Err(error) => {
                    tracing::error!(
                        connection_id = %connection_id,
                        database_index,
                        error = %error,
                        "Failed to load Redis keys via table service"
                    );
                    Vec::new()
                }
            };

            tracing::info!(
                "Loaded {} keys for Redis database {}",
                keys.len(),
                database_index
            );

            // Update sidebar with keys
            connection_sidebar.update(cx, |sidebar, cx| {
                sidebar.set_redis_keys(connection_id, database_index, keys, cx);
            });

            anyhow::Ok(())
        })
        .detach();
    }
}
