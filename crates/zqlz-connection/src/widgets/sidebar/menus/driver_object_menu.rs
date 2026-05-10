use gpui::*;
use uuid::Uuid;
use zqlz_core::{ObjectFormMode, ObjectsPanelAction, ObjectsPanelObjectRef};
use zqlz_ui::widgets::menu::{PopupMenu, PopupMenuItem};

use crate::widgets::sidebar::{ConnectionSidebar, ConnectionSidebarEvent};

#[derive(Clone, Debug)]
pub(in crate::widgets) struct DriverObjectMenuContext {
    pub connection_id: Uuid,
    pub object_name: String,
    pub object_schema: Option<String>,
    pub object_type: String,
    pub database_name: Option<String>,
}

impl DriverObjectMenuContext {
    pub fn qualified_name(&self) -> String {
        self.object_schema
            .as_ref()
            .map(|schema| format!("{schema}.{}", self.object_name))
            .unwrap_or_else(|| self.object_name.clone())
    }

    fn object_ref(&self) -> ObjectsPanelObjectRef {
        ObjectsPanelObjectRef::new(
            ConnectionSidebar::normalized_manifest_object_type(&self.object_type),
            self.object_name.clone(),
        )
        .with_database_option(self.database_name.clone())
        .with_schema_option(self.object_schema.clone())
    }
}

impl ConnectionSidebar {
    pub(in crate::widgets) fn driver_row_actions(
        &self,
        connection_id: Uuid,
        object_type: &str,
    ) -> Option<Vec<ObjectsPanelAction>> {
        let object_type = Self::normalized_manifest_object_type(object_type);
        let manifest = self
            .connections()
            .iter()
            .find(|connection| connection.id == connection_id)
            .and_then(|connection| connection.objects_panel_manifest.as_ref())?;

        manifest
            .object_kinds
            .iter()
            .find(|kind| kind.id == object_type)
            .map(|kind| {
                kind.row_actions
                    .iter()
                    .filter(|action| Self::supports_driver_object_action(&action.id, object_type))
                    .cloned()
                    .collect()
            })
            .filter(|actions: &Vec<ObjectsPanelAction>| !actions.is_empty())
    }

    pub(in crate::widgets) fn apply_driver_object_actions_to_menu(
        mut menu: PopupMenu,
        actions: Vec<ObjectsPanelAction>,
        context: DriverObjectMenuContext,
        sidebar: WeakEntity<Self>,
    ) -> PopupMenu {
        let mut previous_group: Option<String> = None;
        for action in actions {
            if previous_group.is_some() && previous_group != action.group {
                menu = menu.separator();
            }
            previous_group = action.group.clone();

            let action_id = action.id.clone();
            let context = context.clone();
            let sidebar = sidebar.clone();
            menu = menu.item(PopupMenuItem::new(action.label).on_click(
                move |_event, _window, cx| {
                    _ = sidebar.update(cx, |sidebar, cx| {
                        sidebar.invoke_driver_object_action(&action_id, context.clone(), cx);
                    });
                },
            ));
        }

        menu
    }

    pub(in crate::widgets) fn driver_toolbar_actions(
        &self,
        connection_id: Uuid,
    ) -> Vec<ObjectsPanelAction> {
        self.connections()
            .iter()
            .find(|connection| connection.id == connection_id)
            .and_then(|connection| connection.objects_panel_manifest.as_ref())
            .map(|manifest| {
                manifest
                    .toolbar_actions
                    .iter()
                    .filter(|action| Self::supports_driver_toolbar_action(&action.id))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(in crate::widgets) fn apply_driver_toolbar_actions_to_menu(
        mut menu: PopupMenu,
        actions: Vec<ObjectsPanelAction>,
        connection_id: Uuid,
        sidebar: WeakEntity<Self>,
    ) -> PopupMenu {
        if actions.is_empty() {
            return menu;
        }

        menu = menu.separator();

        for action in actions {
            let action_id = action.id.clone();
            let label = if action.id == "refresh" {
                "Refresh Schema".into()
            } else {
                action.label
            };
            let sidebar = sidebar.clone();
            menu = menu.item(
                PopupMenuItem::new(label).on_click(move |_event, _window, cx| {
                    _ = sidebar.update(cx, |sidebar, cx| {
                        sidebar.invoke_driver_toolbar_action(&action_id, connection_id, cx);
                    });
                }),
            );
        }

        menu
    }

    fn supports_driver_object_action(action_id: &str, object_type: &str) -> bool {
        match action_id {
            "copy_name" | "copy_qualified_name" | "refresh" => true,
            "open" => true,
            "design" => matches!(
                object_type,
                "table"
                    | "partitioned_table"
                    | "foreign_table"
                    | "view"
                    | "materialized_view"
                    | "function"
                    | "procedure"
                    | "trigger"
                    | "event"
            ),
            "rename" | "duplicate" | "delete" => matches!(
                object_type,
                "table"
                    | "partitioned_table"
                    | "foreign_table"
                    | "view"
                    | "materialized_view"
                    | "trigger"
                    | "event"
            ),
            "empty" | "import" | "dump_sql_structure_data" | "dump_sql_structure" => {
                matches!(object_type, "table" | "partitioned_table" | "foreign_table")
            }
            "export" => matches!(
                object_type,
                "table" | "partitioned_table" | "foreign_table" | "view" | "materialized_view"
            ),
            "view_history" => matches!(
                object_type,
                "table"
                    | "partitioned_table"
                    | "foreign_table"
                    | "view"
                    | "materialized_view"
                    | "function"
                    | "procedure"
                    | "trigger"
                    | "event"
            ),
            _ => false,
        }
    }

    fn supports_driver_toolbar_action(action_id: &str) -> bool {
        matches!(
            action_id,
            "refresh"
                | "new_table"
                | "new_view"
                | "new_trigger"
                | "new_function"
                | "new_procedure"
                | "new_event"
        )
    }

    pub(in crate::widgets) fn normalized_manifest_object_type(object_type: &str) -> &str {
        match object_type {
            "tables" => "table",
            "views" => "view",
            "materialized_views" => "materialized_view",
            "events" => "event",
            "sequences" => "sequence",
            "domains" => "domain",
            "types" => "type",
            "extensions" => "extension",
            other => other,
        }
    }

    fn invoke_driver_object_action(
        &mut self,
        action_id: &str,
        context: DriverObjectMenuContext,
        cx: &mut Context<Self>,
    ) {
        let context = DriverObjectMenuContext {
            object_type: Self::normalized_manifest_object_type(&context.object_type).to_string(),
            ..context
        };

        match action_id {
            "open" => self.invoke_open_object(context, cx),
            "design" => self.invoke_design_object(context, cx),
            "rename" => self.invoke_rename_object(context, cx),
            "duplicate" => self.invoke_duplicate_object(context, cx),
            "delete" => self.invoke_delete_object(context, cx),
            "empty" => cx.emit(ConnectionSidebarEvent::EmptyTable {
                connection_id: context.connection_id,
                table_name: context.object_name,
            }),
            "import" => cx.emit(ConnectionSidebarEvent::ImportData {
                connection_id: context.connection_id,
                table_name: context.object_name,
            }),
            "export" => cx.emit(ConnectionSidebarEvent::ExportData {
                connection_id: context.connection_id,
                table_name: context.object_name,
            }),
            "dump_sql_structure_data" => cx.emit(ConnectionSidebarEvent::DumpTableSql {
                connection_id: context.connection_id,
                table_name: context.object_name,
                include_data: true,
            }),
            "dump_sql_structure" => cx.emit(ConnectionSidebarEvent::DumpTableSql {
                connection_id: context.connection_id,
                table_name: context.object_name,
                include_data: false,
            }),
            "copy_name" => cx.write_to_clipboard(ClipboardItem::new_string(context.object_name)),
            "copy_qualified_name" => {
                cx.write_to_clipboard(ClipboardItem::new_string(context.qualified_name()));
            }
            "view_history" => cx.emit(ConnectionSidebarEvent::ViewHistory {
                connection_id: context.connection_id,
                object_name: context.object_name,
                object_schema: context.object_schema,
                object_type: context.object_type,
            }),
            "refresh" => cx.emit(ConnectionSidebarEvent::RefreshSchema {
                connection_id: context.connection_id,
            }),
            _ => {
                tracing::debug!(
                    action_id,
                    object_type = %context.object_type,
                    "Unsupported sidebar driver action"
                );
            }
        }
    }

    fn invoke_driver_toolbar_action(
        &mut self,
        action_id: &str,
        connection_id: Uuid,
        cx: &mut Context<Self>,
    ) {
        match action_id {
            "refresh" => cx.emit(ConnectionSidebarEvent::RefreshSchema { connection_id }),
            "new_table" => cx.emit(ConnectionSidebarEvent::NewTable { connection_id }),
            "new_view" => cx.emit(ConnectionSidebarEvent::NewView { connection_id }),
            "new_trigger" => cx.emit(ConnectionSidebarEvent::NewTrigger { connection_id }),
            "new_function" | "new_procedure" | "new_event" => {
                let kind_id = action_id.trim_start_matches("new_").to_string();
                cx.emit(ConnectionSidebarEvent::OpenObjectDesigner {
                    connection_id,
                    kind_id,
                    mode: ObjectFormMode::Create,
                    object_ref: None,
                });
            }
            _ => {}
        }
    }

    fn invoke_open_object(&mut self, context: DriverObjectMenuContext, cx: &mut Context<Self>) {
        match context.object_type.as_str() {
            "table" | "partitioned_table" | "foreign_table" => {
                cx.emit(ConnectionSidebarEvent::OpenTable {
                    connection_id: context.connection_id,
                    table_name: context.object_name,
                    database_name: context.database_name,
                });
            }
            "view" | "materialized_view" => cx.emit(ConnectionSidebarEvent::OpenView {
                connection_id: context.connection_id,
                view_name: context.object_name,
                database_name: context.database_name,
            }),
            "function" => cx.emit(ConnectionSidebarEvent::OpenFunction {
                connection_id: context.connection_id,
                function_name: context.object_name,
                object_schema: context.object_schema,
            }),
            "procedure" => cx.emit(ConnectionSidebarEvent::OpenProcedure {
                connection_id: context.connection_id,
                procedure_name: context.object_name,
                object_schema: context.object_schema,
            }),
            "trigger" => cx.emit(ConnectionSidebarEvent::DesignTrigger {
                connection_id: context.connection_id,
                trigger_name: context.object_name,
                object_schema: context.object_schema,
            }),
            _ => cx.emit(ConnectionSidebarEvent::OpenGenericObjectDefinition {
                connection_id: context.connection_id,
                object_ref: context.object_ref(),
            }),
        }
    }

    fn invoke_design_object(&mut self, context: DriverObjectMenuContext, cx: &mut Context<Self>) {
        match context.object_type.as_str() {
            "table" | "partitioned_table" | "foreign_table" => {
                cx.emit(ConnectionSidebarEvent::DesignTable {
                    connection_id: context.connection_id,
                    table_name: context.object_name,
                });
            }
            "view" | "materialized_view" => cx.emit(ConnectionSidebarEvent::DesignView {
                connection_id: context.connection_id,
                view_name: context.object_name,
                object_schema: context.object_schema,
            }),
            "function" => cx.emit(ConnectionSidebarEvent::OpenFunction {
                connection_id: context.connection_id,
                function_name: context.object_name,
                object_schema: context.object_schema,
            }),
            "procedure" => cx.emit(ConnectionSidebarEvent::OpenProcedure {
                connection_id: context.connection_id,
                procedure_name: context.object_name,
                object_schema: context.object_schema,
            }),
            "trigger" => cx.emit(ConnectionSidebarEvent::DesignTrigger {
                connection_id: context.connection_id,
                trigger_name: context.object_name,
                object_schema: context.object_schema,
            }),
            "event" => cx.emit(ConnectionSidebarEvent::OpenObjectDesigner {
                connection_id: context.connection_id,
                kind_id: "event".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: Some(context.object_ref()),
            }),
            _ => {}
        }
    }

    fn invoke_rename_object(&mut self, context: DriverObjectMenuContext, cx: &mut Context<Self>) {
        match context.object_type.as_str() {
            "table" | "partitioned_table" | "foreign_table" => {
                cx.emit(ConnectionSidebarEvent::RenameTable {
                    connection_id: context.connection_id,
                    table_name: context.object_name,
                });
            }
            "view" | "materialized_view" => cx.emit(ConnectionSidebarEvent::RenameView {
                connection_id: context.connection_id,
                view_name: context.object_name,
            }),
            _ => {}
        }
    }

    fn invoke_duplicate_object(
        &mut self,
        context: DriverObjectMenuContext,
        cx: &mut Context<Self>,
    ) {
        match context.object_type.as_str() {
            "table" | "partitioned_table" | "foreign_table" => {
                cx.emit(ConnectionSidebarEvent::DuplicateTable {
                    connection_id: context.connection_id,
                    table_name: context.object_name,
                });
            }
            "view" | "materialized_view" => cx.emit(ConnectionSidebarEvent::DuplicateView {
                connection_id: context.connection_id,
                view_name: context.object_name,
            }),
            _ => {}
        }
    }

    fn invoke_delete_object(&mut self, context: DriverObjectMenuContext, cx: &mut Context<Self>) {
        match context.object_type.as_str() {
            "table" | "partitioned_table" | "foreign_table" => {
                cx.emit(ConnectionSidebarEvent::DeleteTable {
                    connection_id: context.connection_id,
                    table_name: context.object_name,
                });
            }
            "view" | "materialized_view" => cx.emit(ConnectionSidebarEvent::DeleteView {
                connection_id: context.connection_id,
                view_name: context.object_name,
            }),
            "trigger" => cx.emit(ConnectionSidebarEvent::DeleteTrigger {
                connection_id: context.connection_id,
                trigger_name: context.object_name,
            }),
            "event" => cx.emit(ConnectionSidebarEvent::OpenObjectDesigner {
                connection_id: context.connection_id,
                kind_id: "event".to_string(),
                mode: ObjectFormMode::Drop,
                object_ref: Some(context.object_ref()),
            }),
            _ => {}
        }
    }
}
