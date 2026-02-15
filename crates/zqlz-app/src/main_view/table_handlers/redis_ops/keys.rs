//! This module handles loading Redis keys from a database.

use gpui::{Context, Window};
use uuid::Uuid;

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

        let Some(connection) = app_state.connections.get(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let connection_sidebar = self.connection_sidebar.clone();

        cx.spawn_in(window, async move |_this, cx| {
            // First, SELECT the database
            let select_cmd = format!("SELECT {}", database_index);
            if let Err(e) = connection.execute(&select_cmd, &[]).await {
                tracing::error!("Failed to select Redis database {}: {}", database_index, e);
                return anyhow::Ok(());
            }

            // Then, list keys using SCAN
            let mut keys = Vec::new();
            let mut cursor = 0u64;
            let limit = 1000usize; // Limit to prevent overwhelming large databases

            loop {
                let scan_cmd = format!("SCAN {} MATCH * COUNT 100", cursor);
                let result = match connection.query(&scan_cmd, &[]).await {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!("Failed to scan Redis keys: {}", e);
                        break;
                    }
                };

                if result.rows.len() < 2 {
                    break;
                }

                // First row contains the new cursor
                let new_cursor = result.rows[0]
                    .get_by_name("value")
                    .and_then(|v| match v {
                        zqlz_core::Value::String(s) => s.parse::<u64>().ok(),
                        zqlz_core::Value::Int64(n) => Some(*n as u64),
                        _ => None,
                    })
                    .unwrap_or(0);

                // Remaining rows are keys
                for row in result.rows.iter().skip(1) {
                    if let Some(zqlz_core::Value::String(key)) = row.get_by_name("value") {
                        keys.push(key.clone());
                        if keys.len() >= limit {
                            break;
                        }
                    }
                }

                cursor = new_cursor;
                if cursor == 0 || keys.len() >= limit {
                    break;
                }
            }

            // Sort keys for consistent display
            keys.sort();

            tracing::info!(
                "Loaded {} keys for Redis database {}",
                keys.len(),
                database_index
            );

            // Update sidebar with keys
            _ = connection_sidebar.update(cx, |sidebar, cx| {
                sidebar.set_redis_keys(connection_id, database_index, keys, cx);
            });

            anyhow::Ok(())
        })
        .detach();
    }
}
