//! This module handles data import/export operations and SQL dump generation.

use gpui::{Context, Window};
use uuid::Uuid;
use zqlz_core::DriverCategory;
use zqlz_interchange::widgets::{
    ExportWizard, ExportWizardState, ImportWizard, ImportWizardState, TableExportConfig,
};
use zqlz_services::DumpTablesSqlRequest;
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use crate::app::AppState;
use crate::main_view::MainView;

impl MainView {
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

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let table_service = app_state.table_service.clone();

        cx.spawn_in(window, async move |_this, cx| {
            let outcome = table_service
                .dump_tables_sql(
                    connection,
                    DumpTablesSqlRequest {
                        table_names,
                        include_data,
                    },
                )
                .await;

            let table_count = outcome.processed_table_names.len();

            cx.update(|window, cx| {
                if outcome.sql.is_empty() {
                    window.push_notification(
                        Notification::warning("Could not generate SQL for selected table(s)"),
                        cx,
                    );
                    return;
                }

                cx.write_to_clipboard(gpui::ClipboardItem::new_string(outcome.sql));
                window.push_notification(
                    Notification::success(format!(
                        "SQL for {} table(s) copied to clipboard",
                        table_count
                    )),
                    cx,
                );

                if !outcome.errors.is_empty() {
                    tracing::warn!(
                        errors = %outcome.errors.join("; "),
                        "Table SQL dump completed with partial failures"
                    );
                }
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

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
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
                .get_table_details(
                    connection.clone(),
                    connection_id,
                    &table_name_for_wizard,
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
            if table_names.is_empty() {
                "all tables".to_string()
            } else {
                format!("{} table(s)", table_names.len())
            },
            connection_id,
            table_names,
        );

        let Some(app_state) = cx.try_global::<AppState>() else {
            tracing::error!("No AppState available");
            return;
        };

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let schema_service = app_state.schema_service.clone();
        let export_all_tables = table_names.is_empty();
        let driver_category = connection.driver_category();

        cx.spawn_in(window, async move |_this, cx| {
            let connection_for_wizard = connection.clone();
            let table_names_for_placeholders = table_names.clone();
            let wizard_task = cx.update(|_window, cx| {
                let mut state = ExportWizardState::new();

                if let Some(docs_dir) = dirs::document_dir() {
                    state.output_folder = docs_dir;
                }

                state.tables_loading = true;

                for table_name in table_names_for_placeholders {
                    state.add_table(TableExportConfig::new(table_name, Vec::new()));
                }

                ExportWizard::open(state, Some(connection_for_wizard), cx)
            })?;

            let wizard_handle = wizard_task.await?;
            let mut table_configs: Vec<TableExportConfig> = Vec::new();

            let load_result: anyhow::Result<()> = if export_all_tables {
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
                                    Ok(())
                                }
                                Err(e) => Err(anyhow::anyhow!(
                                    "Could not list databases for export: {}",
                                    e
                                )),
                            }
                        }
                        _ => match schema_introspection.list_tables(None).await {
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
                                    table_configs
                                        .push(TableExportConfig::new(table_info.name, columns));
                                }
                                Ok(())
                            }
                            Err(e) => {
                                Err(anyhow::anyhow!("Could not list tables for export: {}", e))
                            }
                        },
                    }
                } else {
                    Err(anyhow::anyhow!(
                        "Schema introspection is not supported by this connection"
                    ))
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
                            tracing::warn!("Could not fetch columns for '{}': {}", table_name, e);
                            Vec::new()
                        }
                    };
                    table_configs.push(TableExportConfig::new(table_name.clone(), columns));
                }
                Ok(())
            };

            match load_result {
                Ok(()) => {
                    let wizard = wizard_handle.wizard.clone();
                    wizard_handle.window.update(cx, |_, window, cx| {
                        wizard.update(cx, |this, cx| {
                            this.set_tables(table_configs, window, cx);
                        });
                    })?;
                }
                Err(error) => {
                    tracing::error!("{}", error);
                    let wizard = wizard_handle.wizard.clone();
                    wizard_handle.window.update(cx, |_, _window, cx| {
                        wizard.update(cx, |this, cx| {
                            this.set_tables_load_error(error.to_string(), cx);
                        });
                    })?;
                }
            }

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

        let Some(connection) = app_state.connection_service.get_connection(connection_id) else {
            tracing::error!("Connection not found: {}", connection_id);
            return;
        };

        let connection = connection.clone();
        let table_service = app_state.table_service.clone();

        cx.spawn_in(window, async move |_this, cx| {
            let outcome = table_service
                .dump_tables_sql(
                    connection,
                    DumpTablesSqlRequest {
                        table_names: vec![table_name.clone()],
                        include_data,
                    },
                )
                .await;

            let full_sql = outcome.sql;

            if full_sql.is_empty() {
                tracing::warn!("Could not generate SQL for table");
                return anyhow::Ok(());
            }

            // Copy to clipboard
            cx.update(|_window, cx| {
                cx.write_to_clipboard(gpui::ClipboardItem::new_string(full_sql.clone()));
            })?;

            if !outcome.errors.is_empty() {
                tracing::warn!(
                    errors = %outcome.errors.join("; "),
                    table_name = %table_name,
                    "Single-table SQL dump completed with partial failures"
                );
            }

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
