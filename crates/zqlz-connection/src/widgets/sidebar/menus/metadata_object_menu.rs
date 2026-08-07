use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

use super::driver_object_menu::DriverObjectMenuContext;
use super::state::ContextMenuState;

impl ConnectionSidebar {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::widgets) fn show_metadata_object_context_menu(
        &mut self,
        conn_id: Uuid,
        object_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
        object_type: String,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.selected_connection != Some(conn_id) {
            self.select_connection(conn_id, cx);
        }

        if self.metadata_object_context_menu.is_none() {
            self.metadata_object_context_menu = Some(ContextMenuState::new(window, cx));
        }

        let sidebar_weak = cx.entity().downgrade();
        let action_context = self.focus_handle.clone();
        let driver_actions = self.driver_row_actions(conn_id, &object_type);
        let object_features = self.connection_object_features(conn_id);
        let driver_context = DriverObjectMenuContext {
            connection_id: conn_id,
            object_name: object_name.clone(),
            object_schema: object_schema.clone(),
            object_type: object_type.clone(),
            database_name: database_name.clone(),
        };
        let qualified_name = object_schema
            .as_ref()
            .map(|schema| format!("{schema}.{object_name}"))
            .unwrap_or_else(|| object_name.clone());

        if let Some(menu_state) = &self.metadata_object_context_menu {
            menu_state.update(cx, |state, cx| {
                state.menu_subscription.take();
                state.position = position;
                let new_menu = PopupMenu::build(window, cx, |menu, _, _| {
                    if let Some(actions) = driver_actions.clone() {
                        return Self::apply_driver_object_actions_to_menu(
                            menu.action_context(action_context.clone()).max_h(px(400.0)),
                            actions,
                            driver_context.clone(),
                            object_features.clone(),
                            sidebar_weak.clone(),
                        );
                    }

                    menu.action_context(action_context.clone())
                        .item(PopupMenuItem::new("Copy Name").on_click({
                            let object_name = object_name.clone();
                            move |_event, _window, cx| {
                                cx.write_to_clipboard(ClipboardItem::new_string(
                                    object_name.clone(),
                                ));
                            }
                        }))
                        .item(PopupMenuItem::new("Copy Qualified Name").on_click({
                            let qualified_name = qualified_name.clone();
                            move |_event, _window, cx| {
                                cx.write_to_clipboard(ClipboardItem::new_string(
                                    qualified_name.clone(),
                                ));
                            }
                        }))
                        .separator()
                        .item(PopupMenuItem::new("Refresh").on_click({
                            let sidebar = sidebar_weak.clone();
                            move |_event, _window, cx| {
                                if let Err(error) = sidebar.update(cx, |_sidebar, cx| {
                                    cx.emit(ConnectionSidebarEvent::RefreshSchema {
                                        connection_id: conn_id,
                                    });
                                }) {
                                    tracing::debug!(
                                        error = %error,
                                        object_type = %object_type,
                                        "Metadata object refresh skipped"
                                    );
                                }
                            }
                        }))
                });

                let menu_entity = new_menu.clone();
                let menu_state_entity = cx.entity().clone();
                state.menu_subscription = Some(cx.subscribe(
                    &menu_entity,
                    move |_state, _, _event: &DismissEvent, cx| {
                        let menu_state = menu_state_entity.clone();
                        cx.defer(move |cx| {
                            menu_state.update(cx, |state, cx| {
                                state.open = false;
                                cx.notify();
                            });
                        });
                    },
                ));
                state.menu = new_menu.clone();
                state.open = true;

                if !new_menu.focus_handle(cx).contains_focused(window, cx) {
                    new_menu.focus_handle(cx).focus(window, cx);
                }

                cx.notify();
            });
        }
    }
}
