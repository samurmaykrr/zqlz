//! This module handles data import/export operations and SQL dump generation.

use gpui::{Context, Window};
use uuid::Uuid;
use zqlz_core::DriverCategory;
use zqlz_interchange::widgets::{
    ExportWizard, ExportWizardState, ImportWizard, ImportWizardState, TableExportConfig,
};
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::main_view::MainView;
use crate::main_view::table_handlers_utils::conversion::driver_name_to_category;

impl MainView {
    /// Exports data from the given tables, or all tables if the list is empty.
    ///
    /// Delegates directly to `export_data` so that every call site — toolbar
    /// button, context menu (single or multi), connection sidebar — goes through
    /// the same code path.
    pub(in crate::main_view) fn export_tables(
        &mut self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.export_data(connection_id, table_names, window, cx);
    }

    /// Dumps SQL for multiple tables (CREATE + optional INSERT statements)
    pub(in crate::main_view) fn dump_tables_sql(
        &mut self,
        connection_id: Uuid,
        table_names: Vec<String>,
        include_data: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if table_names.is_empty() {
            return;
        }

        tracing::info!(
            "Dump SQL for {} tables (include_data={}): {:?}",
            table_names.len(),
            include_data,
            table_names
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

        cx.spawn_in(window, async move |_this, cx| {
            let mut all_sql: Vec<String> = Vec::new();

            for table_name in &table_names {
                let mut table_sql_parts: Vec<String> = Vec::new();

                // Add comment header for this table
                table_sql_parts.push(format!("-- Table: {}", table_name));
                table_sql_parts.push(format!("-- Generated: {}", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")));
                table_sql_parts.push(String::new());

                // Get table structure
                if let Some(schema_introspection) = connection.as_schema_introspection() {
                    match schema_introspection.get_columns(None, table_name).await {
                        Ok(columns) => {
                            let column_defs: Vec<String> = columns
                                .iter()
                                .map(|col| {
                                    let nullable = if col.nullable { "" } else { " NOT NULL" };
                                    let default = col
                                        .default_value
                                        .as_ref()
                                        .map(|d| format!(" DEFAULT {}", d))
                                        .unwrap_or_default();
                                    format!("    \"{}\" {}{}{}", col.name, col.data_type, nullable, default)
                                })
                                .collect();

                            let create_table = format!(
                                "CREATE TABLE \"{}\" (\n{}\n);",
                                table_name,
                                column_defs.join(",\n")
                            );
                            table_sql_parts.push(create_table);
                        }
                        Err(e) => {
                            tracing::error!("Failed to get columns for {}: {}", table_name, e);
                            table_sql_parts.push(format!("-- Error getting structure: {}", e));
                        }
                    }
                }

                // Get data if requested
                if include_data {
                    let query = format!("SELECT * FROM \"{}\"", table_name);
                    match connection.query(&query, &[]).await {
                        Ok(result) => {
                            if !result.rows.is_empty() {
                                table_sql_parts.push(String::new());
                                let column_names: Vec<String> =
                                    result.columns.iter().map(|c| c.name.clone()).collect();

                                for row in &result.rows {
                                    let values: Vec<String> = row
                                        .values
                                        .iter()
                                        .map(|v| match v {
                                            zqlz_core::Value::Null => "NULL".to_string(),
                                            zqlz_core::Value::String(s) => {
                                                format!("'{}'", s.replace("'", "''"))
                                            }
                                            zqlz_core::Value::Int64(n) => n.to_string(),
                                            zqlz_core::Value::Float64(n) => n.to_string(),
                                            zqlz_core::Value::Bool(b) => {
                                                if *b { "TRUE" } else { "FALSE" }.to_string()
                                            }
                                            zqlz_core::Value::Bytes(b) => {
                                                // Hex encode bytes without external crate
                                                let hex_str: String = b.iter()
                                                    .map(|byte| format!("{:02x}", byte))
                                                    .collect();
                                                format!("X'{}'", hex_str)
                                            }
                                            _ => v.to_string(),
                                        })
                                        .collect();

                                    let insert_sql = format!(
                                        "INSERT INTO \"{}\" ({}) VALUES ({});",
                                        table_name,
                                        column_names
                                            .iter()
                                            .map(|n| format!("\"{}\"", n))
                                            .collect::<Vec<_>>()
                                            .join(", "),
                                        values.join(", ")
                                    );
                                    table_sql_parts.push(insert_sql);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to get data for {}: {}", table_name, e);
                            table_sql_parts.push(format!("-- Error getting data: {}", e));
                        }
                    }
                }

                all_sql.push(table_sql_parts.join("\n"));
            }

            let full_sql = all_sql.join("\n\n");
            let table_count = table_names.len();

            cx.update(|window, cx| {
                cx.write_to_clipboard(gpui::ClipboardItem::new_string(full_sql));
                window.push_notification(
                    Notification::success(format!("SQL for {} table(s) copied to clipboard", table_count)),
                    cx,
                );
            })?;

            anyhow::Ok(())
        })
        .detach();
    }

    /// Opens the import data wizard
    pub(in crate::main_view) fn import_data(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Import data to table: {} on connection {}",
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

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let table_name_for_wizard = table_name.clone();

        // Fetch table columns for field mapping
        cx.spawn_in(window, async move |_this, cx| {
            // Get table details to populate field mappings
            let columns = match schema_service
                .get_table_details(connection.clone(), connection_id, &table_name_for_wizard, None)
                .await
            {
                Ok(details) => details
                    .columns
                    .into_iter()
                    .map(|c| c.name)
                    .collect::<Vec<_>>(),
                Err(e) => {
                    tracing::warn!("Could not fetch columns for import: {}", e);
                    Vec::new()
                }
            };

            let connection_for_wizard = connection.clone();

            // Open the import wizard in a new window on the UI thread
            cx.update(|_window, cx| {
                // Create initial wizard state with target table info
                let mut state = ImportWizardState::new();
                state
                    .target_configs
                    .push(zqlz_interchange::widgets::TargetTableConfig {
                        source_index: 0,
                        source_name: table_name_for_wizard.clone(),
                        target_table: table_name_for_wizard.clone(),
                        create_new_table: false,
                    });

                // Store columns in field mappings.  The services layer's ColumnInfo does
                // not carry auto_increment metadata, so we conservatively default to false
                // here — the UDIF and CSV importers derive the flag from ColumnDefinition
                // and FieldMapping respectively when they have richer schema information.
                let mappings: Vec<zqlz_interchange::widgets::FieldMapping> = columns
                    .iter()
                    .map(|name| zqlz_interchange::widgets::FieldMapping {
                        source_field: name.clone(),
                        target_field: name.clone(),
                        is_primary_key: false,
                        skip: false,
                        is_auto_increment: false,
                    })
                    .collect();
                state.field_mappings.insert(0, mappings);

                ImportWizard::open(state, Some(connection_for_wizard), cx);
            })?;

            anyhow::Ok(())
        })
        .detach();
    }

    /// Opens the Export Wizard pre-populated with table/column information.
    ///
    /// `table_names` controls which tables are loaded into the wizard:
    /// - Empty list → all tables in the connected database are fetched and
    ///   pre-loaded, matching the behaviour of the toolbar "Export Wizard…" button.
    /// - Non-empty list → only the specified tables are loaded, matching the
    ///   context-menu behaviour for one or more selected tables.
    pub(in crate::main_view) fn export_data(
        &mut self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Open Export Wizard for {} on connection {} (tables: {:?})",
            if table_names.is_empty() { "all tables".to_string() } else { format!("{} table(s)", table_names.len()) },
            connection_id,
            table_names,
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
        let schema_service = app_state.schema_service.clone();
        let export_all_tables = table_names.is_empty();
        let driver_category = driver_name_to_category(connection.driver_name());

        cx.spawn_in(window, async move |_this, cx| {
            let mut table_configs: Vec<TableExportConfig> = Vec::new();

            if export_all_tables {
                // Fetch all items from the database — behaviour varies by driver category.
                if let Some(schema_introspection) = connection.as_schema_introspection() {
                    match driver_category {
                        DriverCategory::KeyValue => {
                            match schema_introspection.list_databases().await {
                                Ok(databases) => {
                                    for db_info in databases {
                                        let config = TableExportConfig::new(
                                            db_info.name,
                                            vec![
                                                "key".to_string(),
                                                "value".to_string(),
                                                "type".to_string(),
                                                "ttl".to_string(),
                                            ],
                                        );
                                        table_configs.push(config);
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Could not list databases for export: {}", e);
                                }
                            }
                        }
                        _ => {
                            match schema_introspection.list_tables(None).await {
                                Ok(tables) => {
                                    for table_info in tables {
                                        let columns = match schema_service
                                            .get_table_details(
                                                connection.clone(),
                                                connection_id,
                                                &table_info.name,
                                                None,
                                            )
                                            .await
                                        {
                                            Ok(details) => details
                                                .columns
                                                .into_iter()
                                                .map(|c| c.name)
                                                .collect::<Vec<_>>(),
                                            Err(e) => {
                                                tracing::warn!(
                                                    "Could not fetch columns for '{}': {}",
                                                    table_info.name,
                                                    e
                                                );
                                                Vec::new()
                                            }
                                        };
                                        table_configs.push(TableExportConfig::new(
                                            table_info.name,
                                            columns,
                                        ));
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Could not list tables for export: {}", e);
                                }
                            }
                        }
                    }
                }
            } else {
                // Fetch only the requested tables.
                for table_name in &table_names {
                    let columns = match schema_service
                        .get_table_details(connection.clone(), connection_id, table_name, None)
                        .await
                    {
                        Ok(details) => details
                            .columns
                            .into_iter()
                            .map(|c| c.name)
                            .collect::<Vec<_>>(),
                        Err(e) => {
                            tracing::warn!(
                                "Could not fetch columns for '{}': {}",
                                table_name,
                                e
                            );
                            Vec::new()
                        }
                    };
                    table_configs.push(TableExportConfig::new(table_name.clone(), columns));
                }
            }

            let connection_for_wizard = connection.clone();

            cx.update(|_window, cx| {
                let mut state = ExportWizardState::new();

                if let Some(docs_dir) = dirs::document_dir() {
                    state.output_folder = docs_dir;
                }

                for config in table_configs {
                    state.add_table(config);
                }

                ExportWizard::open(state, Some(connection_for_wizard), cx);
            })?;

            anyhow::Ok(())
        })
        .detach();
    }

    /// Dumps table SQL (structure and optionally data)
    pub(in crate::main_view) fn dump_table_sql(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        include_data: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Dump SQL for table: {} on connection {} (include_data={})",
            table_name,
            connection_id,
            include_data
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
        let table_service = app_state.table_service.clone();

        cx.spawn_in(window, async move |_this, cx| {
            let mut sql_parts: Vec<String> = Vec::new();

            // Get the CREATE statement
            if let Some(schema_introspection) = connection.as_schema_introspection() {
                use zqlz_core::{DatabaseObject, ObjectType};
                let db_object = DatabaseObject {
                    object_type: ObjectType::Table,
                    schema: None,
                    name: table_name.clone(),
                };
                if let Ok(create_sql) = schema_introspection.generate_ddl(&db_object).await {
                    sql_parts.push(create_sql);
                }
            }

            // Get INSERT statements if including data
            if include_data {
                match table_service
                    .browse_table(connection.clone(), &table_name, None, None, None)
                    .await
                {
                    Ok(result) => {
                        if !result.rows.is_empty() {
                            sql_parts.push(String::new()); // Empty line separator
                            sql_parts.push(format!("-- Data for table: {}", table_name));

                            let column_names: Vec<&str> =
                                result.columns.iter().map(|c| c.name.as_str()).collect();

                            for row in &result.rows {
                                let values: Vec<String> = row
                                    .values
                                    .iter()
                                    .map(|v| {
                                        if v.is_null() {
                                            "NULL".to_string()
                                        } else {
                                            let s = v.to_string();
                                            format!("'{}'", s.replace("'", "''"))
                                        }
                                    })
                                    .collect();

                                let insert_sql = format!(
                                    "INSERT INTO \"{}\" ({}) VALUES ({});",
                                    table_name,
                                    column_names
                                        .iter()
                                        .map(|n| format!("\"{}\"", n))
                                        .collect::<Vec<_>>()
                                        .join(", "),
                                    values.join(", ")
                                );
                                sql_parts.push(insert_sql);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to fetch table data for dump: {}", e);
                    }
                }
            }

            let full_sql = sql_parts.join("\n");

            if full_sql.is_empty() {
                tracing::warn!("Could not generate SQL for table");
                return anyhow::Ok(());
            }

            // Copy to clipboard
            cx.update(|_window, cx| {
                cx.write_to_clipboard(gpui::ClipboardItem::new_string(full_sql.clone()));
            })?;

            let msg = if include_data {
                format!(
                    "Table '{}' SQL (structure + data) copied to clipboard",
                    table_name
                )
            } else {
                format!(
                    "Table '{}' SQL (structure only) copied to clipboard",
                    table_name
                )
            };

            tracing::info!("{}", msg);

            anyhow::Ok(())
        })
        .detach();
    }
}
