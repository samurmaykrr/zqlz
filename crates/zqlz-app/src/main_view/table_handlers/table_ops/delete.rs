use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    button::ButtonVariant,
    dialog::DialogButtonProps,
    v_flex,
};

use crate::app::AppState;
use crate::components::ObjectsPanelEvent;
use crate::MainView;

impl MainView {
    #[allow(dead_code)]
    pub(in crate::main_view) fn delete_table(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Delete table: {} on connection {}",
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
        let connection_sidebar = self.connection_sidebar.downgrade();
        let objects_panel = self.objects_panel.downgrade();
        let table_name_for_dialog = table_name.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let connection_sidebar = connection_sidebar.clone();
            let objects_panel = objects_panel.clone();
            let table_name = table_name_for_dialog.clone();

            dialog
                .title("Delete Table")
                .child(
                    v_flex()
                        .gap_2()
                        .child(
                            div().child(format!(
                                "Are you sure you want to delete table '{}'?",
                                table_name
                            )),
                        )
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("This action cannot be undone. All data in the table will be permanently lost."),
                        ),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Delete")
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let connection_sidebar = connection_sidebar.clone();
                    let objects_panel = objects_panel.clone();
                    let table_name = table_name.clone();

                    cx.spawn(async move |cx| {
                        let sql = format!("DROP TABLE \"{}\"", table_name);
                        match connection.execute(&sql, &[]).await {
                            Ok(_) => {
                                tracing::info!("Table '{}' deleted successfully", table_name);

                                cx.update(|cx| {
                                    _ = connection_sidebar.update(cx, |_, cx| {
                                        cx.emit(crate::components::ConnectionSidebarEvent::RefreshSchema { 
                                            connection_id 
                                        });
                                    });
                                    _ = objects_panel.update(cx, |_, cx| {
                                        cx.emit(ObjectsPanelEvent::Refresh);
                                    });
                                })
;
                            }
                            Err(e) => {
                                tracing::error!("Failed to delete table: {}", e);
                            }
                        }
                    })
                    .detach();

                    true
                })
                .confirm()
        });
    }

    pub(in crate::main_view) fn delete_tables(
        &mut self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use std::cell::RefCell;
        use std::rc::Rc;
        use zqlz_ui::widgets::checkbox::Checkbox;

        if table_names.is_empty() {
            return;
        }

        let count = table_names.len();
        let is_multi = count > 1;

        tracing::info!(
            "Delete {} table(s): {:?} on connection {}",
            count,
            table_names,
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
        let connection_sidebar = self.connection_sidebar.downgrade();
        let objects_panel = self.objects_panel.downgrade();
        let schema_service = app_state.schema_service.clone();

        let continue_on_error = Rc::new(RefCell::new(false));

        let title = if is_multi {
            format!("Delete {} Tables", count)
        } else {
            "Delete Table".to_string()
        };

        let message = if is_multi {
            format!("Are you sure you want to delete these {} tables?", count)
        } else {
            format!("Are you sure you want to delete table '{}'?", table_names[0])
        };

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let connection_sidebar = connection_sidebar.clone();
            let objects_panel = objects_panel.clone();
            let schema_service = schema_service.clone();
            let table_names = table_names.clone();
            let continue_on_error = continue_on_error.clone();
            let continue_on_error_for_ok = continue_on_error.clone();

            dialog
                .title(title.clone())
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(message.clone()))
                        .when(is_multi, |this| {
                            this.child(
                                div()
                                    .text_sm()
                                    .font_family(cx.theme().mono_font_family.clone())
                                    .text_color(cx.theme().muted_foreground)
                                    .child(table_names.join(", ")),
                            )
                        })
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("This action cannot be undone. All data in the table(s) will be permanently lost."),
                        )
                        .when(is_multi, |this| {
                            let continue_on_error = continue_on_error.clone();
                            this.child(
                                Checkbox::new("continue-on-error")
                                    .label("Continue on error")
                                    .checked(false)
                                    .on_click(move |checked, _window, _cx| {
                                        *continue_on_error.borrow_mut() = *checked;
                                    }),
                            )
                        }),
                )
                .button_props(
                    DialogButtonProps::default()
                        .ok_text("Delete")
                        .ok_variant(ButtonVariant::Danger),
                )
                .on_ok(move |_, _window, cx| {
                    let connection = connection.clone();
                    let connection_sidebar = connection_sidebar.clone();
                    let objects_panel = objects_panel.clone();
                    let schema_service = schema_service.clone();
                    let table_names = table_names.clone();
                    let continue_on_error = *continue_on_error_for_ok.borrow();

                    cx.spawn(async move |cx| {
                        let mut errors: Vec<String> = Vec::new();
                        let mut deleted_tables: Vec<String> = Vec::new();

                        for table_name in &table_names {
                            let sql = format!("DROP TABLE \"{}\"", table_name);
                            match connection.execute(&sql, &[]).await {
                                Ok(_) => {
                                    tracing::info!("Table '{}' deleted successfully", table_name);
                                    deleted_tables.push(table_name.clone());
                                }
                                Err(e) => {
                                    let error_msg = format!("'{}': {}", table_name, e);
                                    tracing::error!("Failed to delete table {}", error_msg);

                                    if continue_on_error {
                                        errors.push(error_msg);
                                    } else {
                                        return;
                                    }
                                }
                            }
                        }

                        if !deleted_tables.is_empty() {
                            schema_service.invalidate_connection_cache(connection_id);

                            cx.update(|cx| {
                                _ = connection_sidebar.update(cx, |_, cx| {
                                    cx.emit(crate::components::ConnectionSidebarEvent::RefreshSchema { 
                                        connection_id 
                                    });
                                });
                                _ = objects_panel.update(cx, |_, cx| {
                                    cx.emit(ObjectsPanelEvent::Refresh);
                                });
                            })
;
                        }

                        if !errors.is_empty() {
                            tracing::warn!(
                                "Deleted {} of {} tables. Errors: {}",
                                deleted_tables.len(),
                                table_names.len(),
                                errors.join("; ")
                            );
                        } else if !deleted_tables.is_empty() {
                            tracing::info!("Successfully deleted {} table(s)", deleted_tables.len());
                        }
                    })
                    .detach();

                    true
                })
                .confirm()
        });
    }
}
