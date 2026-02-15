//! This module contains standalone event handlers for refreshing table data.

use gpui::*;
use uuid::Uuid;
use zqlz_core::DriverCategory;
use zqlz_services::TableService;

use crate::app::AppState;
use crate::components::TableViewerPanel;
use crate::main_view::table_handlers_utils::conversion::resolve_schema_qualifier;
use crate::main_view::table_handlers_utils::formatting::{format_bytes, format_ttl_seconds};

pub(in crate::main_view) fn handle_refresh_table_event(
    connection_id: Uuid,
    table_name: &str,
    driver_category: DriverCategory,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    tracing::info!(
        "RefreshTable event: table={}, connection={}, driver={:?}",
        table_name,
        connection_id,
        driver_category
    );

    let Some(app_state) = cx.try_global::<AppState>() else {
        tracing::error!("No AppState available");
        return;
    };

    let Some(connection) = app_state.connections.get(connection_id) else {
        tracing::error!("Connection not found: {}", connection_id);
        return;
    };

    let table_name = table_name.to_string();
    let connection = connection.clone();
    let connection_name = app_state
        .connection_manager()
        .get_saved(connection_id)
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "Unknown".to_string());

    let table_service = app_state.table_service.clone();

    let is_view = viewer_entity.read(cx).is_view();
    let database_name = viewer_entity.read(cx).database_name();

    _ = viewer_entity.update(cx, |viewer, cx| {
        viewer.set_loading(true, cx);
    });

    match driver_category {
        DriverCategory::KeyValue => {
            handle_refresh_keyvalue_table(
                connection_id,
                connection,
                connection_name,
                table_name,
                driver_category,
                viewer_entity,
                window,
                cx,
            );
        }
        _ => {
            handle_refresh_sql_table(
                connection_id,
                connection,
                connection_name,
                table_name,
                database_name,
                is_view,
                driver_category,
                table_service,
                viewer_entity,
                window,
                cx,
            );
        }
    }
}

pub(in crate::main_view) fn handle_refresh_sql_table(
    connection_id: Uuid,
    connection: std::sync::Arc<dyn zqlz_core::Connection>,
    connection_name: String,
    table_name: String,
    database_name: Option<String>,
    is_view: bool,
    driver_category: DriverCategory,
    table_service: std::sync::Arc<TableService>,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    let schema_qualifier = resolve_schema_qualifier(connection.driver_name(), &database_name);
    
    // Preserve active filters and sorts when refreshing
    let (filters, sorts, visible_columns, search_text) = viewer_entity.read_with(cx, |viewer, cx| {
        let (filters, sorts) = viewer.filter_panel_state
            .as_ref()
            .map(|state| state.read_with(cx, |s, _| (s.get_filter_conditions(), s.get_sort_criteria())))
            .unwrap_or_else(|| (Vec::new(), Vec::new()));
        
        let visible_columns = viewer.column_visibility_state
            .as_ref()
            .map(|state| state.read(cx).visible_columns())
            .unwrap_or_else(|| viewer.column_meta.iter().map(|c| c.name.clone()).collect());
        
        let search_text = viewer.search_text.clone();
        
        (filters, sorts, visible_columns, search_text)
    });
    
    // Build WHERE clauses from filters
    let mut where_clauses: Vec<String> = filters.iter().filter_map(|f| f.to_sql()).collect();
    
    // Build search WHERE clause if search text is present
    if !search_text.is_empty() {
        let column_meta = viewer_entity.read(cx).column_meta.clone();
        let searchable_columns: Vec<String> = column_meta
            .iter()
            .filter(|col| TableService::is_string_type(&col.data_type.to_lowercase()))
            .map(|col| col.name.clone())
            .collect();

        if !searchable_columns.is_empty() {
            let escaped_search = search_text.replace("'", "''").replace("%", "\\%").replace("_", "\\_");
            let column_conditions: Vec<String> = searchable_columns
                .iter()
                .map(|col_name| {
                    let escaped_col = format!("\"{}\"", col_name.replace("\"", "\"\""));
                    format!(
                        "CAST({} AS TEXT) LIKE '%{}%' ESCAPE '\\'",
                        escaped_col,
                        escaped_search
                    )
                })
                .collect();
            where_clauses.push(format!("({})", column_conditions.join(" OR ")));
        }
    }
    
    // Build ORDER BY clauses from sorts
    let order_by_clauses: Vec<String> = sorts.iter().map(|s| s.to_sql()).collect();
    
    let filter_count = where_clauses.len();
    let sort_count = order_by_clauses.len();
    
    tracing::info!(
        "Refreshing table '{}' with {} filter(s), {} sort(s), search='{}'",
        table_name,
        filter_count,
        sort_count,
        search_text
    );
    
    window
        .spawn(cx, async move |cx| {
            let result = if where_clauses.is_empty() && order_by_clauses.is_empty() && visible_columns.is_empty() {
                // No filters/sorts - use simple browse_table
                table_service
                    .browse_table(connection, &table_name, schema_qualifier.as_deref(), None, None)
                    .await
            } else {
                // Filters/sorts active - use browse_table_with_filters to preserve them
                table_service
                    .browse_table_with_filters(
                        connection,
                        &table_name,
                        schema_qualifier.as_deref(),
                        where_clauses,
                        order_by_clauses,
                        visible_columns,
                        None,
                        None,
                        None,
                    )
                    .await
            };
            
            match result {
                Ok(result) => {
                    tracing::info!(
                        "Table reloaded successfully: {} rows (filters={}, sorts={})",
                        result.rows.len(),
                        filter_count,
                        sort_count
                    );

                    _ = viewer_entity.update_in(cx, |viewer, window, cx| {
                        viewer.load_table(
                            connection_id,
                            connection_name.clone(),
                            table_name.clone(),
                            database_name.clone(),
                            is_view,
                            result,
                            driver_category,
                            window,
                            cx,
                        );
                    });
                }
                Err(e) => {
                    tracing::error!("Failed to refresh table: {}", e);

                    _ = viewer_entity.update(cx, |viewer, cx| {
                        viewer.set_loading(false, cx);
                    });
                }
            }

            anyhow::Ok(())
        })
        .detach();
}

pub(in crate::main_view) fn handle_refresh_keyvalue_table(
    connection_id: Uuid,
    connection: std::sync::Arc<dyn zqlz_core::Connection>,
    connection_name: String,
    table_name: String,
    driver_category: DriverCategory,
    viewer_entity: Entity<TableViewerPanel>,
    window: &mut Window,
    cx: &mut App,
) {
    window
        .spawn(cx, async move |cx| {
            let table_infos =
                if let Some(schema_introspection) = connection.as_schema_introspection() {
                    match schema_introspection.list_tables(None).await {
                        Ok(tables) => tables,
                        Err(e) => {
                            tracing::error!("Failed to list keys: {}", e);
                            _ = viewer_entity.update(cx, |viewer, cx| {
                                viewer.set_loading(false, cx);
                            });
                            return anyhow::Ok(());
                        }
                    }
                } else {
                    tracing::error!("Connection does not support schema introspection");
                    _ = viewer_entity.update(cx, |viewer, cx| {
                        viewer.set_loading(false, cx);
                    });
                    return anyhow::Ok(());
                };

            tracing::info!("Reloaded {} keys for key-value database", table_infos.len());

            let columns = vec![
                zqlz_core::ColumnMeta {
                    name: "Key".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    ordinal: 0,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: Some("Key name".to_string()),
                    enum_values: None,
                },
                zqlz_core::ColumnMeta {
                    name: "Type".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    ordinal: 1,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: Some("Data type".to_string()),
                    enum_values: None,
                },
                zqlz_core::ColumnMeta {
                    name: "Value".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    ordinal: 2,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: Some("Value preview".to_string()),
                    enum_values: None,
                },
                zqlz_core::ColumnMeta {
                    name: "Size".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    ordinal: 3,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: Some("Memory size".to_string()),
                    enum_values: None,
                },
                zqlz_core::ColumnMeta {
                    name: "TTL".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    ordinal: 4,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: Some("Time to live".to_string()),
                    enum_values: None,
                },
            ];

            let column_names = vec![
                "Key".to_string(),
                "Type".to_string(),
                "Value".to_string(),
                "Size".to_string(),
                "TTL".to_string(),
            ];

            let rows: Vec<zqlz_core::Row> = table_infos
                .iter()
                .map(|info| {
                    let key_value_info = info.key_value_info.as_ref();

                    let key = zqlz_core::Value::String(info.name.clone());

                    let key_type = key_value_info
                        .map(|kv| zqlz_core::Value::String(kv.key_type.clone()))
                        .unwrap_or(zqlz_core::Value::Null);

                    let value = key_value_info
                        .and_then(|kv| kv.value_preview.as_ref())
                        .map(|v| zqlz_core::Value::String(v.clone()))
                        .unwrap_or(zqlz_core::Value::Null);

                    let size = key_value_info
                        .and_then(|kv| kv.size_bytes)
                        .map(|bytes| zqlz_core::Value::String(format_bytes(bytes)))
                        .unwrap_or(zqlz_core::Value::Null);

                    let ttl = key_value_info
                        .and_then(|kv| kv.ttl_seconds)
                        .map(|ttl| {
                            zqlz_core::Value::String(if ttl == -1 {
                                "No TTL".to_string()
                            } else if ttl == -2 {
                                "Not Found".to_string()
                            } else {
                                format_ttl_seconds(ttl)
                            })
                        })
                        .unwrap_or(zqlz_core::Value::String("No TTL".to_string()));

                    zqlz_core::Row::new(column_names.clone(), vec![key, key_type, value, size, ttl])
                })
                .collect();

            let total_rows = rows.len();

            let query_result = zqlz_core::QueryResult {
                id: uuid::Uuid::new_v4(),
                columns,
                rows,
                total_rows: Some(total_rows as u64),
                is_estimated_total: false,
                affected_rows: 0,
                execution_time_ms: 0,
                warnings: vec![],
            };

            _ = viewer_entity.update_in(cx, |viewer, window, cx| {
                viewer.load_table(
                    connection_id,
                    connection_name,
                    table_name,
                    None,
                    true,
                    query_result,
                    driver_category,
                    window,
                    cx,
                );

                if let Some(table_state) = &viewer.table_state {
                    table_state.update(cx, |table, _cx| {
                        table.delegate_mut().set_disable_inline_edit(true);
                    });
                }
            });

            anyhow::Ok(())
        })
        .detach();
}
