//! This module handles table duplication operations (structure and data).

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme as _, WindowExt,
    checkbox::Checkbox,
    input::{Input, InputState},
    v_flex,
};

use crate::app::AppState;
use crate::components::ObjectsPanelEvent;
use crate::main_view::table_handlers_utils::validation::validate_table_name;
use crate::MainView;

impl MainView {
    /// Duplicates a table (creates a copy with a new name)
    pub(in crate::main_view) fn duplicate_table(
        &mut self,
        connection_id: Uuid,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Duplicate table: {} on connection {}",
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
        let source_table_name = table_name.clone();

        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("New table name"));
        name_input.update(cx, |input, cx| {
            input.set_value(format!("{}_copy", table_name), window, cx);
        });

        let error_message: Entity<Option<String>> = cx.new(|_| None);

        cx.subscribe(&name_input, {
            let error_message = error_message.clone();
            move |_this, _input, event, cx| {
                if matches!(event, zqlz_ui::widgets::input::InputEvent::Change) {
                    error_message.update(cx, |msg, cx| {
                        if msg.is_some() {
                            *msg = None;
                            cx.notify();
                        }
                    });
                }
            }
        })
        .detach();

        window.open_dialog(cx, {
            let name_input = name_input.clone();
            let source_table_name = source_table_name.clone();
            let error_message = error_message.clone();

            move |dialog, _window, cx| {
                let connection = connection.clone();
                let connection_sidebar = connection_sidebar.clone();
                let objects_panel = objects_panel.clone();
                let source_table_name = source_table_name.clone();
                let name_input = name_input.clone();
                let error_message = error_message.clone();
                let error_message_for_ok = error_message.clone();

                dialog
                    .title("Duplicate Table")
                    .w(px(400.0))
                    .child(
                        v_flex()
                            .gap_2()
                            .child(
                                div()
                                    .text_sm()
                                    .child(format!("Create a copy of table '{}' as:", source_table_name)),
                            )
                            .child(Input::new(&name_input))
                            .child({
                                let error = error_message.read(cx).clone();
                                div()
                                    .text_xs()
                                    .when_some(error, |this, err| {
                                        this.text_color(gpui::red()).child(err)
                                    })
                                    .when(error_message.read(cx).is_none(), |this| {
                                        this.text_color(cx.theme().muted_foreground)
                                            .child("The new table will include all data from the source table.")
                                    })
                            }),
                    )
                    .on_ok(move |_, _window, cx| {
                        let new_table_name = name_input.read(cx).text().to_string().trim().to_string();

                        if let Some(err) = validate_table_name(&new_table_name) {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some(err.to_string());
                                cx.notify();
                            });
                            return false;
                        }

                        if new_table_name == source_table_name {
                            error_message_for_ok.update(cx, |msg, cx| {
                                *msg = Some("New name must be different from the original".to_string());
                                cx.notify();
                            });
                            return false;
                        }

                        let connection = connection.clone();
                        let connection_sidebar = connection_sidebar.clone();
                        let objects_panel = objects_panel.clone();
                        let source_table_name = source_table_name.clone();

                        cx.spawn(async move |cx| {
                            let sql = format!(
                                "CREATE TABLE \"{}\" AS SELECT * FROM \"{}\"",
                                new_table_name, source_table_name
                            );

                            match connection.execute(&sql, &[]).await {
                                Ok(_) => {
                                    tracing::info!(
                                        "Table '{}' duplicated as '{}'",
                                        source_table_name,
                                        new_table_name
                                    );

                                    cx.update(|cx| {
                                        _ = connection_sidebar.update(cx, |sidebar, cx| {
                                            sidebar.add_table(connection_id, new_table_name.clone(), cx);
                                        });
                                        _ = objects_panel.update(cx, |_, cx| {
                                            cx.emit(ObjectsPanelEvent::Refresh);
                                        });
                                    })
;
                                }
                                Err(e) => {
                                    tracing::error!("Failed to duplicate table: {}", e);
                                }
                            }
                        })
                        .detach();

                        true
                    })
                    .confirm()
            }
        });

        name_input.focus_handle(cx).focus(window, cx);
    }

    /// Duplicates multiple tables with auto-generated names
    pub(in crate::main_view) fn duplicate_tables(
        &mut self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if table_names.is_empty() {
            return;
        }

        if table_names.len() == 1 {
            self.duplicate_table(connection_id, table_names.into_iter().next().unwrap(), window, cx);
            return;
        }

        let count = table_names.len();
        tracing::info!(
            "Duplicate {} tables: {:?} on connection {}",
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
        let continue_on_error = Rc::new(RefCell::new(true));
        let new_names: Vec<String> = table_names.iter().map(|n| format!("{}_copy", n)).collect();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let connection_sidebar = connection_sidebar.clone();
            let objects_panel = objects_panel.clone();
            let schema_service = schema_service.clone();
            let table_names = table_names.clone();
            let continue_on_error = continue_on_error.clone();
            let continue_on_error_for_ok = continue_on_error.clone();

            dialog
                .title(format!("Duplicate {} Tables", count))
                .child(
                    v_flex()
                        .gap_2()
                        .child(div().child(format!(
                            "Create copies of {} tables with '_copy' suffix:",
                            count
                        )))
                        .child(
                            div()
                                .text_sm()
                                .font_family(cx.theme().mono_font_family.clone())
                                .text_color(cx.theme().muted_foreground)
                                .child(new_names.join(", ")),
                        )
                        .child({
                            let continue_on_error = continue_on_error.clone();
                            Checkbox::new("continue-on-error")
                                .label("Continue on error")
                                .checked(true)
                                .on_click(move |checked, _window, _cx| {
                                    *continue_on_error.borrow_mut() = *checked;
                                })
                        }),
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
                        let mut duplicated_tables: Vec<String> = Vec::new();

                        for table_name in &table_names {
                            let new_name = format!("{}_copy", table_name);

                            let sql = format!(
                                "CREATE TABLE \"{}\" AS SELECT * FROM \"{}\"",
                                new_name, table_name
                            );

                            match connection.execute(&sql, &[]).await {
                                Ok(_) => {
                                    tracing::info!(
                                        "Table '{}' duplicated as '{}'",
                                        table_name,
                                        new_name
                                    );
                                    duplicated_tables.push(new_name.clone());

                                    cx.update(|cx| {
                                        _ = connection_sidebar.update(cx, |sidebar, cx| {
                                            sidebar.add_table(connection_id, new_name, cx);
                                        });
                                    })
;
                                }
                                Err(e) => {
                                    let error_msg = format!("'{}': {}", table_name, e);
                                    tracing::error!("Failed to duplicate table {}", error_msg);

                                    if continue_on_error {
                                        errors.push(error_msg);
                                    } else {
                                        return;
                                    }
                                }
                            }
                        }

                        if !duplicated_tables.is_empty() {
                            schema_service.invalidate_connection_cache(connection_id);

                            cx.update(|cx| {
                                _ = objects_panel.update(cx, |_, cx| {
                                    cx.emit(ObjectsPanelEvent::Refresh);
                                });
                            })
;
                        }

                        if errors.is_empty() {
                            tracing::info!("Duplicated {} table(s)", duplicated_tables.len());
                        } else {
                            tracing::warn!(
                                "Duplicated {} of {} tables. Errors: {}",
                                duplicated_tables.len(),
                                table_names.len(),
                                errors.join("; ")
                            );
                        }
                    })
                    .detach();

                    true
                })
                .confirm()
        });
    }
}
