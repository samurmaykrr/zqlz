//! This module handles Redis key deletion operations.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::cell::RefCell;
use std::rc::Rc;
use uuid::Uuid;
use zqlz_ui::widgets::{
    ActiveTheme as _,
    button::{ButtonVariant, ButtonVariants as _},
    dialog::DialogButtonProps,
    v_flex,
    WindowExt,
    checkbox::Checkbox,
};

use crate::app::AppState;
use crate::main_view::MainView;

impl MainView {
    pub(in crate::main_view) fn delete_keys(
        &mut self,
        connection_id: Uuid,
        key_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if key_names.is_empty() {
            return;
        }

        let count = key_names.len();
        let is_multi = count > 1;

        tracing::info!(
            "Delete {} key(s): {:?} on connection {}",
            count,
            key_names,
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
        let continue_on_error = Rc::new(RefCell::new(false));

        let title = if is_multi {
            format!("Delete {} Keys", count)
        } else {
            "Delete Key".to_string()
        };

        let message = if is_multi {
            format!("Are you sure you want to delete these {} keys?", count)
        } else {
            format!("Are you sure you want to delete key '{}'?", key_names[0])
        };

        window.open_dialog(cx, move |dialog, _window, cx| {
            let connection = connection.clone();
            let key_names = key_names.clone();
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
                                    .child(key_names.join(", ")),
                            )
                        })
                        .child(
                            div()
                                .text_sm()
                                .text_color(cx.theme().muted_foreground)
                                .child("This action cannot be undone."),
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
                    let key_names = key_names.clone();
                    let continue_on_error = *continue_on_error_for_ok.borrow();

                    cx.spawn(async move |_cx| {
                        let mut errors: Vec<String> = Vec::new();
                        let mut deleted_keys: Vec<String> = Vec::new();

                        for key_name in &key_names {
                            let cmd = format!("DEL {}", key_name);
                            match connection.execute(&cmd, &[]).await {
                                Ok(_) => {
                                    tracing::info!("Key '{}' deleted successfully", key_name);
                                    deleted_keys.push(key_name.clone());
                                }
                                Err(e) => {
                                    let error_msg = format!("'{}': {}", key_name, e);
                                    tracing::error!("Failed to delete key {}", error_msg);

                                    if continue_on_error {
                                        errors.push(error_msg);
                                    } else {
                                        return;
                                    }
                                }
                            }
                        }

                        if errors.is_empty() {
                            tracing::info!("Deleted {} key(s)", deleted_keys.len());
                        } else {
                            tracing::warn!(
                                "Deleted {} of {} keys. Errors: {}",
                                deleted_keys.len(),
                                key_names.len(),
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
