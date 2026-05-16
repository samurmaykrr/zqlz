use super::MainView;
use crate::app::AppState;
use crate::workspace_state::RefreshScope;
use gpui::{ClipboardItem, Context, Window};
use uuid::Uuid;
use zqlz_ui::widgets::{WindowExt, notification::Notification};

use zqlz_core::DatabaseObject;
use zqlz_services::object_type_for_kind_id;
pub(super) use zqlz_services::{
    ObjectsPanelActionRegistry, ResolvedObjectsPanelAction, SelectedObjectRef,
    classify_objects_panel_action_resolution, manifest_action_coverage_gaps,
    objects_panel_action_feature_availability, objects_panel_action_issue_message,
};

fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(super) trait ResolvedObjectsPanelActionExt {
    fn execute(
        self,
        main_view: &mut MainView,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<MainView>,
    );
}

impl ResolvedObjectsPanelActionExt for ResolvedObjectsPanelAction {
    fn execute(
        self,
        main_view: &mut MainView,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<MainView>,
    ) {
        match self {
            Self::Noop => {}
            Self::Refresh => {
                main_view.request_refresh(RefreshScope::ActiveConnectionSurfaces, cx);
            }
            Self::NewTable => {
                main_view.new_table(connection_id, window, cx);
            }
            Self::NewView => {
                main_view.new_view(connection_id, window, cx);
            }
            Self::NewTrigger => {
                main_view.new_trigger(connection_id, window, cx);
            }
            Self::OpenObjectForm {
                kind_id,
                mode,
                object_ref,
            } => {
                main_view.open_object_designer(
                    connection_id,
                    kind_id,
                    mode,
                    object_ref,
                    window,
                    cx,
                );
            }
            Self::ImportWithoutSelection => {
                main_view.import_data(connection_id, String::new(), window, cx);
            }
            Self::Export { object_names } => {
                main_view.export_data(connection_id, object_names, window, cx);
            }
            Self::OpenRedisDatabase { database_index } => {
                main_view.open_redis_database(connection_id, database_index, window, cx);
            }
            Self::OpenDocumentCollection {
                database_name,
                collection_name,
            } => {
                main_view.open_document_collection_viewer(
                    connection_id,
                    database_name,
                    collection_name,
                    None,
                    window,
                    cx,
                );
            }
            Self::OpenTables {
                object_names,
                database_name,
            } => {
                main_view.open_tables(
                    connection_id,
                    object_names,
                    database_name,
                    false,
                    window,
                    cx,
                );
            }
            Self::OpenViews {
                object_names,
                database_name,
            } => {
                main_view.open_tables(connection_id, object_names, database_name, true, window, cx);
            }
            Self::OpenFunction { function_ref } | Self::DesignFunction { function_ref } => {
                main_view.open_function_definition(
                    connection_id,
                    function_ref.name,
                    function_ref.schema,
                    None,
                    function_ref.signature,
                    window,
                    cx,
                );
            }
            Self::OpenProcedure { procedure_ref } | Self::DesignProcedure { procedure_ref } => {
                main_view.open_procedure_definition(
                    connection_id,
                    procedure_ref.name,
                    procedure_ref.schema,
                    None,
                    procedure_ref.signature,
                    window,
                    cx,
                );
            }
            Self::OpenSequence { sequence_ref } => {
                main_view.open_sequence_definition(
                    connection_id,
                    sequence_ref.name,
                    sequence_ref.schema,
                    window,
                    cx,
                );
            }
            Self::OpenExtension { extension_ref } => {
                let name = quote_postgres_identifier(&extension_ref.name);
                let schema_clause = extension_ref
                    .schema
                    .as_ref()
                    .map_or(String::new(), |schema| {
                        format!(" WITH SCHEMA {}", quote_postgres_identifier(schema))
                    });
                main_view.open_query_editor_with_content(
                    format!("[Extension] {}", extension_ref.name),
                    format!(
                        "CREATE EXTENSION IF NOT EXISTS {}{};\n",
                        name, schema_clause
                    ),
                    None,
                    Some(connection_id),
                    window,
                    cx,
                );
            }
            Self::DesignTables { object_names } => {
                main_view.design_tables(connection_id, object_names, window, cx);
            }
            Self::DesignViews { object_names } => {
                main_view.design_views(connection_id, object_names, window, cx);
            }
            Self::DesignTrigger { trigger_ref } => {
                main_view.open_trigger_designer(
                    connection_id,
                    Some(trigger_ref.name),
                    trigger_ref.schema,
                    trigger_ref.associated_table,
                    window,
                    cx,
                );
            }
            Self::DesignSequence { sequence_ref } => {
                main_view.open_sequence_designer(
                    connection_id,
                    Some(sequence_ref.name),
                    sequence_ref.schema,
                    window,
                    cx,
                );
            }
            Self::OpenGenericDdl {
                object_ref,
                kind_id,
            } => {
                main_view.open_generic_object_definition(
                    connection_id,
                    kind_id,
                    object_ref,
                    window,
                    cx,
                );
            }
            Self::OpenTriggerDdl { trigger_ref } => {
                main_view.design_trigger(
                    connection_id,
                    trigger_ref.name,
                    trigger_ref.schema,
                    trigger_ref.associated_table,
                    window,
                    cx,
                );
            }
            Self::DeleteTrigger { trigger_ref } => {
                main_view.delete_trigger(connection_id, trigger_ref.name, window, cx);
            }
            Self::RenameView { view_name } => {
                main_view.rename_view(connection_id, view_name, window, cx);
            }
            Self::RenameTable { table_name } => {
                main_view.rename_table(connection_id, table_name, window, cx);
            }
            Self::DeleteTables { object_names } => {
                main_view.delete_tables(connection_id, object_names, window, cx);
            }
            Self::DeleteViews { object_names } => {
                main_view.delete_views(connection_id, object_names, window, cx);
            }
            Self::DeleteExtension { extension_ref } => {
                main_view.delete_extension(connection_id, extension_ref.name, window, cx);
            }
            Self::DeleteKeys { key_names } => {
                main_view.delete_keys(connection_id, key_names, window, cx);
            }
            Self::DuplicateTables { object_names } => {
                main_view.duplicate_tables(connection_id, object_names, window, cx);
            }
            Self::DuplicateViews { object_names } => {
                main_view.duplicate_views(connection_id, object_names, window, cx);
            }
            Self::EmptyTable { table_name } => {
                main_view.empty_table(connection_id, table_name, window, cx);
            }
            Self::EmptyTables { table_names } => {
                main_view.empty_tables(connection_id, table_names, window, cx);
            }
            Self::ImportTable { table_name } => {
                main_view.import_data(connection_id, table_name, window, cx);
            }
            Self::DumpSqlStructureDataSingle { table_name } => {
                main_view.dump_table_sql(connection_id, table_name, true, window, cx);
            }
            Self::DumpSqlStructureDataBatch { table_names } => {
                main_view.dump_tables_sql(connection_id, table_names, true, window, cx);
            }
            Self::DumpSqlStructureSingle { table_name } => {
                main_view.dump_table_sql(connection_id, table_name, false, window, cx);
            }
            Self::DumpSqlStructureBatch { table_names } => {
                main_view.dump_tables_sql(connection_id, table_names, false, window, cx);
            }
            Self::CopyObjectNames { names } => {
                if !names.is_empty() {
                    cx.write_to_clipboard(ClipboardItem::new_string(names.join("\n")));
                }
            }
            Self::CopyQualifiedNames { names } => {
                cx.write_to_clipboard(ClipboardItem::new_string(names.join("\n")));
            }
            Self::ViewHistory {
                object_name,
                object_schema,
                db_object_type,
            } => {
                main_view.versioning_facade_show_version_history(
                    connection_id,
                    object_name,
                    object_schema,
                    db_object_type,
                    window,
                    cx,
                );
            }
            Self::UnsupportedViewHistoryKind {
                action_id,
                object_kind,
            } => {
                tracing::warn!(
                    action_id,
                    object_kind = %object_kind,
                    "Unsupported object kind for view_history action"
                );
            }
            Self::UnknownAction {
                action_id,
                object_count,
            } => {
                tracing::warn!(
                    action_id,
                    object_count,
                    "Unknown generic objects panel action"
                );
            }
        }
    }
}

impl MainView {
    pub(super) fn open_generic_object_definition(
        &mut self,
        connection_id: Uuid,
        kind_id: String,
        object_ref: SelectedObjectRef,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = cx.try_global::<AppState>() else {
            window.push_notification(Notification::error("Application state not available"), cx);
            return;
        };

        let target_database = object_ref.database.clone().or_else(|| {
            self.workspace_state
                .read(cx)
                .active_database()
                .map(ToString::to_string)
        });
        let Some(connection) = app_state
            .connection_service
            .get_connection_for_database_cached(connection_id, target_database.as_deref())
            .or_else(|| app_state.connection_service.get_connection(connection_id))
        else {
            window.push_notification(Notification::error("Connection not available"), cx);
            return;
        };

        let title = format!("[{}] {}", kind_id, object_ref.name);
        let connection = connection.clone();
        let kind_id_for_task = kind_id.clone();
        let object_ref_for_task = object_ref.clone();

        cx.spawn_in(window, async move |this, cx| {
            let ddl = if let Some(metadata_query) =
                document_metadata_query(&kind_id_for_task, &object_ref_for_task)
            {
                metadata_query
            } else if let Some(object_type) = object_type_for_kind_id(&kind_id_for_task) {
                match connection.as_schema_introspection() {
                    Some(schema) => {
                        let object = DatabaseObject {
                            object_type,
                            schema: object_ref_for_task.schema.clone(),
                            name: object_ref_for_task.name.clone(),
                            signature: object_ref_for_task.signature.clone(),
                        };
                        match schema.generate_ddl(&object).await {
                            Ok(ddl) => ddl,
                            Err(error) => format!(
                                "-- DDL unavailable for {kind} {name}: {error}\n-- schema: {schema}\n-- signature: {signature}",
                                kind = kind_id_for_task,
                                name = object_ref_for_task.name,
                                schema = object_ref_for_task.schema.as_deref().unwrap_or("-"),
                                signature = object_ref_for_task.signature.as_deref().unwrap_or("-"),
                            ),
                        }
                    }
                    None => format!("-- Schema introspection unavailable for {kind_id_for_task}"),
                }
            } else {
                format!(
                    "-- {kind} metadata\n-- name: {name}\n-- schema: {schema}\n-- signature: {signature}",
                    kind = kind_id_for_task,
                    name = object_ref_for_task.name,
                    schema = object_ref_for_task.schema.as_deref().unwrap_or("-"),
                    signature = object_ref_for_task.signature.as_deref().unwrap_or("-"),
                )
            };

            cx.update(|window, cx| {
                _ = this.update(cx, |main_view, cx| {
                    main_view.open_query_editor_with_content(
                        title,
                        ddl,
                        None,
                        Some(connection_id),
                        window,
                        cx,
                    );
                });
            })?;

            anyhow::Ok(())
        })
        .detach();
    }
}

fn document_metadata_query(kind_id: &str, object_ref: &SelectedObjectRef) -> Option<String> {
    let database = object_ref.schema.as_deref().unwrap_or("<collection>");
    match kind_id {
        "document_collection" => Some(format!(
            "{{\n  \"listCollections\": 1,\n  \"filter\": {{ \"name\": {name:?} }},\n  \"nameOnly\": false\n}}",
            name = object_ref.name,
        )),
        "document_view" => Some(format!(
            "{{\n  \"listCollections\": 1,\n  \"filter\": {{ \"name\": {name:?}, \"type\": \"view\" }},\n  \"nameOnly\": false\n}}",
            name = object_ref.name,
        )),
        "document_index" => Some(format!(
            "{{\n  \"listIndexes\": {collection:?},\n  \"comment\": \"Inspect index {index}\"\n}}",
            collection = database,
            index = object_ref.name,
        )),
        "document_function" => Some(format!(
            "{{\n  \"find\": \"system.js\",\n  \"filter\": {{ \"_id\": {name:?} }},\n  \"limit\": 1\n}}",
            name = object_ref.name,
        )),
        "document_gridfs_bucket" => Some(format!(
            "{{\n  \"find\": {files_collection:?},\n  \"sort\": {{ \"uploadDate\": -1 }}\n}}",
            files_collection = object_ref.schema.as_deref().unwrap_or("<bucket>.files"),
        )),
        "document_user" => Some(format!(
            "{{\n  \"usersInfo\": {user:?},\n  \"showPrivileges\": true\n}}",
            user = object_ref.name,
        )),
        "document_role" => Some(format!(
            "{{\n  \"rolesInfo\": {role:?},\n  \"showPrivileges\": true,\n  \"showBuiltinRoles\": true\n}}",
            role = object_ref.name,
        )),
        "document_search_index" | "document_vector_index" => object_ref
            .name
            .rsplit_once('.')
            .map(|(collection, index)| {
                format!(
                    "{{\n  \"aggregate\": {collection:?},\n  \"pipeline\": [\n    {{ \"$listSearchIndexes\": {{ \"name\": {index:?} }} }}\n  ],\n  \"cursor\": {{}}\n}}"
                )
            })
            .or_else(|| {
                Some(format!(
                    "db.getCollectionNames().flatMap(collection => db.getCollection(collection).aggregate([{{ $listSearchIndexes: {{}} }}]).toArray().filter(index => `${{collection}}.${{index.name}}` === {index:?}))",
                    index = object_ref.name,
                ))
            }),
        "document_server" => Some(format!(
            "{{\n  \"$db\": \"admin\",\n  {command:?}: 1\n}}",
            command = object_ref.name,
        )),
        "document_sharding" if object_ref.schema.as_deref() == Some("sharded_collection") => {
            Some(format!(
                "{{\n  \"$db\": \"config\",\n  \"find\": \"collections\",\n  \"filter\": {{ \"_id\": {namespace:?} }},\n  \"limit\": 1\n}}",
                namespace = object_ref.name,
            ))
        }
        "document_sharding" if object_ref.schema.as_deref() == Some("chunk") => Some(format!(
            "{{\n  \"$db\": \"config\",\n  \"aggregate\": \"chunks\",\n  \"pipeline\": [\n    {{ \"$lookup\": {{ \"from\": \"collections\", \"localField\": \"uuid\", \"foreignField\": \"uuid\", \"as\": \"collection\" }} }},\n    {{ \"$match\": {{ \"$or\": [{{ \"_id\": {chunk_id:?} }}, {{ \"ns\": {namespace:?} }}, {{ \"collection._id\": {namespace:?} }}] }} }}\n  ],\n  \"cursor\": {{}}\n}}",
            chunk_id = object_ref.name,
            namespace = object_ref
                .name
                .split(" chunk ")
                .next()
                .unwrap_or(&object_ref.name),
        )),
        "document_sharding" if object_ref.schema.as_deref() == Some("zone") => Some(format!(
            "{{\n  \"$db\": \"config\",\n  \"find\": \"tags\",\n  \"filter\": {{ \"ns\": {namespace:?} }}\n}}",
            namespace = object_ref
                .name
                .split(" zone ")
                .next()
                .unwrap_or(&object_ref.name),
        )),
        "document_sharding" if object_ref.name.contains('.') => Some(format!(
            "{{\n  \"$db\": \"config\",\n  \"find\": \"collections\",\n  \"filter\": {{ \"_id\": {namespace:?} }},\n  \"limit\": 1\n}}",
            namespace = object_ref.name,
        )),
        "document_sharding" => Some("{\n  \"$db\": \"admin\",\n  \"listShards\": 1\n}".to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_core::{
        FeatureAvailability, ObjectFeatureSet, ObjectFormMode, ObjectsPanelManifest,
        ObjectsPanelObjectKind, format_qualified_object_name, parse_redis_database_index,
    };
    use zqlz_services::{ActionRegistryError, ObjectsPanelActionResolutionTelemetry};
    use zqlz_versioning::DatabaseObjectType;

    fn object_ref(kind_id: &str, name: &str) -> zqlz_core::ObjectsPanelObjectRef {
        zqlz_core::ObjectsPanelObjectRef::new(kind_id, name)
    }

    fn schema_object_ref(
        kind_id: &str,
        schema: &str,
        name: &str,
    ) -> zqlz_core::ObjectsPanelObjectRef {
        zqlz_core::ObjectsPanelObjectRef::new(kind_id, name)
            .with_schema_option(Some(schema.to_string()))
    }

    fn trigger_object_ref(
        schema: &str,
        table_name: &str,
        trigger_name: &str,
    ) -> zqlz_core::ObjectsPanelObjectRef {
        zqlz_core::ObjectsPanelObjectRef::new("trigger", trigger_name)
            .with_schema_option(Some(schema.to_string()))
            .with_signature_option(Some(table_name.to_string()))
    }

    fn routine_object_ref(
        kind_id: &str,
        schema: &str,
        routine_name: &str,
        signature: &str,
    ) -> zqlz_core::ObjectsPanelObjectRef {
        zqlz_core::ObjectsPanelObjectRef::new(kind_id, routine_name)
            .with_schema_option(Some(schema.to_string()))
            .with_signature_option(Some(signature.to_string()))
    }

    fn object_features() -> ObjectFeatureSet {
        ObjectFeatureSet {
            browse_objects: FeatureAvailability::available(),
            create_objects: FeatureAvailability::unavailable("create blocked"),
            edit_objects: FeatureAvailability::available(),
            delete_objects: FeatureAvailability::unavailable("delete blocked"),
            available_kinds: vec!["table".to_string()],
            available_actions: vec![
                "open".to_string(),
                "new_table".to_string(),
                "rename".to_string(),
                "delete".to_string(),
            ],
        }
    }

    #[test]
    fn document_metadata_query_opens_gridfs_bucket_files() {
        let collection = SelectedObjectRef {
            database: None,
            name: "customers".to_string(),
            schema: None,
            signature: None,
            associated_table: None,
        };
        let view = SelectedObjectRef {
            database: None,
            name: "activeCustomers".to_string(),
            schema: None,
            signature: None,
            associated_table: None,
        };
        let object_ref = SelectedObjectRef {
            database: None,
            name: "fs".to_string(),
            schema: Some("fs.files".to_string()),
            signature: None,
            associated_table: None,
        };

        assert_eq!(
            document_metadata_query("document_collection", &collection).as_deref(),
            Some(
                "{\n  \"listCollections\": 1,\n  \"filter\": { \"name\": \"customers\" },\n  \"nameOnly\": false\n}"
            )
        );
        assert_eq!(
            document_metadata_query("document_view", &view).as_deref(),
            Some(
                "{\n  \"listCollections\": 1,\n  \"filter\": { \"name\": \"activeCustomers\", \"type\": \"view\" },\n  \"nameOnly\": false\n}"
            )
        );
        assert_eq!(
            document_metadata_query("document_gridfs_bucket", &object_ref).as_deref(),
            Some("{\n  \"find\": \"fs.files\",\n  \"sort\": { \"uploadDate\": -1 }\n}")
        );
    }

    #[test]
    fn document_metadata_query_uses_runnable_command_json_for_index_and_function() {
        let index = SelectedObjectRef {
            database: None,
            name: "orders_placed_at_idx".to_string(),
            schema: Some("orders".to_string()),
            signature: None,
            associated_table: None,
        };
        let function = SelectedObjectRef {
            database: None,
            name: "formatCurrency".to_string(),
            schema: Some("function".to_string()),
            signature: None,
            associated_table: None,
        };

        assert_eq!(
            document_metadata_query("document_index", &index).as_deref(),
            Some(
                "{\n  \"listIndexes\": \"orders\",\n  \"comment\": \"Inspect index orders_placed_at_idx\"\n}"
            )
        );
        assert_eq!(
            document_metadata_query("document_function", &function).as_deref(),
            Some(
                "{\n  \"find\": \"system.js\",\n  \"filter\": { \"_id\": \"formatCurrency\" },\n  \"limit\": 1\n}"
            )
        );
    }

    #[test]
    fn document_metadata_query_uses_runnable_command_json_for_search_indexes() {
        let search_index = SelectedObjectRef {
            database: None,
            name: "documents.documents_text_idx".to_string(),
            schema: Some("search_index".to_string()),
            signature: None,
            associated_table: None,
        };
        let vector_index = SelectedObjectRef {
            database: None,
            name: "documents.documents_vector_idx".to_string(),
            schema: Some("vector_search_index".to_string()),
            signature: None,
            associated_table: None,
        };

        assert_eq!(
            document_metadata_query("document_search_index", &search_index).as_deref(),
            Some(
                "{\n  \"aggregate\": \"documents\",\n  \"pipeline\": [\n    { \"$listSearchIndexes\": { \"name\": \"documents_text_idx\" } }\n  ],\n  \"cursor\": {}\n}"
            )
        );
        assert_eq!(
            document_metadata_query("document_vector_index", &vector_index).as_deref(),
            Some(
                "{\n  \"aggregate\": \"documents\",\n  \"pipeline\": [\n    { \"$listSearchIndexes\": { \"name\": \"documents_vector_idx\" } }\n  ],\n  \"cursor\": {}\n}"
            )
        );
    }

    #[test]
    fn document_metadata_query_uses_runnable_command_json_for_admin_objects() {
        let user = SelectedObjectRef {
            database: None,
            name: "reporter".to_string(),
            schema: Some("user".to_string()),
            signature: None,
            associated_table: None,
        };
        let server = SelectedObjectRef {
            database: None,
            name: "buildInfo".to_string(),
            schema: Some("server".to_string()),
            signature: None,
            associated_table: None,
        };

        assert_eq!(
            document_metadata_query("document_user", &user).as_deref(),
            Some("{\n  \"usersInfo\": \"reporter\",\n  \"showPrivileges\": true\n}")
        );
        assert_eq!(
            document_metadata_query("document_server", &server).as_deref(),
            Some("{\n  \"$db\": \"admin\",\n  \"buildInfo\": 1\n}")
        );
    }

    #[test]
    fn document_metadata_query_opens_sharding_config_by_kind() {
        let sharded_collection = SelectedObjectRef {
            database: None,
            name: "zqlz_feature_lab.orders".to_string(),
            schema: Some("sharded_collection".to_string()),
            signature: None,
            associated_table: None,
        };
        let chunk = SelectedObjectRef {
            database: None,
            name: "zqlz_feature_lab.orders chunk 3".to_string(),
            schema: Some("chunk".to_string()),
            signature: None,
            associated_table: None,
        };
        let zone = SelectedObjectRef {
            database: None,
            name: "zqlz_feature_lab.orders zone east".to_string(),
            schema: Some("zone".to_string()),
            signature: None,
            associated_table: None,
        };

        assert_eq!(
            document_metadata_query("document_sharding", &sharded_collection).as_deref(),
            Some(
                "{\n  \"$db\": \"config\",\n  \"find\": \"collections\",\n  \"filter\": { \"_id\": \"zqlz_feature_lab.orders\" },\n  \"limit\": 1\n}"
            )
        );
        assert_eq!(
            document_metadata_query("document_sharding", &chunk).as_deref(),
            Some(
                "{\n  \"$db\": \"config\",\n  \"aggregate\": \"chunks\",\n  \"pipeline\": [\n    { \"$lookup\": { \"from\": \"collections\", \"localField\": \"uuid\", \"foreignField\": \"uuid\", \"as\": \"collection\" } },\n    { \"$match\": { \"$or\": [{ \"_id\": \"zqlz_feature_lab.orders chunk 3\" }, { \"ns\": \"zqlz_feature_lab.orders\" }, { \"collection._id\": \"zqlz_feature_lab.orders\" }] } }\n  ],\n  \"cursor\": {}\n}"
            )
        );
        assert_eq!(
            document_metadata_query("document_sharding", &zone).as_deref(),
            Some(
                "{\n  \"$db\": \"config\",\n  \"find\": \"tags\",\n  \"filter\": { \"ns\": \"zqlz_feature_lab.orders\" }\n}"
            )
        );
        assert_eq!(
            document_metadata_query(
                "document_sharding",
                &SelectedObjectRef {
                    database: None,
                    name: "listShards".to_string(),
                    schema: Some("sharding".to_string()),
                    signature: None,
                    associated_table: None,
                },
            )
            .as_deref(),
            Some("{\n  \"$db\": \"admin\",\n  \"listShards\": 1\n}")
        );
    }

    #[test]
    fn objects_panel_action_feature_availability_rejects_unadvertised_action() {
        let availability = objects_panel_action_feature_availability(&object_features(), "drop");

        assert_eq!(
            availability,
            FeatureAvailability::unavailable(
                "The 'drop' action is not advertised by this connection"
            )
        );
    }

    #[test]
    fn objects_panel_action_feature_availability_maps_action_to_feature_group() {
        let features = object_features();

        assert_eq!(
            objects_panel_action_feature_availability(&features, "open"),
            FeatureAvailability::available()
        );
        assert_eq!(
            objects_panel_action_feature_availability(&features, "new_table"),
            FeatureAvailability::unavailable("create blocked")
        );
        assert_eq!(
            objects_panel_action_feature_availability(&features, "rename"),
            FeatureAvailability::available()
        );
        assert_eq!(
            objects_panel_action_feature_availability(&features, "delete"),
            FeatureAvailability::unavailable("delete blocked")
        );
    }

    #[test]
    fn parse_redis_database_index_handles_expected_formats() {
        assert_eq!(parse_redis_database_index("db0"), Some(0));
        assert_eq!(parse_redis_database_index("db15"), Some(15));
        assert_eq!(parse_redis_database_index("db"), None);
        assert_eq!(parse_redis_database_index("database1"), None);
        assert_eq!(parse_redis_database_index("db99999"), None);
    }

    #[test]
    fn format_qualified_object_name_formats_database_schema_and_signature() {
        let table_name = format_qualified_object_name(
            &object_ref("table", "users")
                .with_database_option(Some("mydb".to_string()))
                .with_schema_option(Some("public".to_string())),
        );
        assert_eq!(table_name, "mydb.public.users");

        let schema_only = format_qualified_object_name(
            &object_ref("table", "users").with_schema_option(Some("public".to_string())),
        );
        assert_eq!(schema_only, "public.users");

        let db_only = format_qualified_object_name(
            &object_ref("table", "users").with_database_option(Some("mydb".to_string())),
        );
        assert_eq!(db_only, "mydb.users");

        let function_name = format_qualified_object_name(
            &object_ref("function", "do_work")
                .with_database_option(Some("mydb".to_string()))
                .with_schema_option(Some("public".to_string()))
                .with_signature_option(Some("integer,text".to_string())),
        );
        assert_eq!(function_name, "mydb.public.do_work(integer,text)");

        let function_without_signature = format_qualified_object_name(
            &object_ref("function", "do_work")
                .with_schema_option(Some("public".to_string()))
                .with_signature_option(Some(String::new())),
        );
        assert_eq!(function_without_signature, "public.do_work");
    }

    #[test]
    fn action_registry_resolves_view_history_kinds() {
        let registry = ObjectsPanelActionRegistry::default();

        assert_eq!(
            registry.resolve_versioned_object_type("view_history", "table"),
            Ok(DatabaseObjectType::Table)
        );
        assert_eq!(
            registry.resolve_versioned_object_type("view_history", "partitioned_table"),
            Ok(DatabaseObjectType::Table)
        );
        assert_eq!(
            registry.resolve_versioned_object_type("view_history", "foreign_table"),
            Ok(DatabaseObjectType::Table)
        );
        assert_eq!(
            registry.resolve_versioned_object_type("view_history", "view"),
            Ok(DatabaseObjectType::View)
        );
        assert_eq!(
            registry.resolve_versioned_object_type("view_history", "materialized_view"),
            Ok(DatabaseObjectType::View)
        );
        assert_eq!(
            registry.resolve_versioned_object_type("view_history", "function"),
            Ok(DatabaseObjectType::Function)
        );
        assert_eq!(
            registry.resolve_versioned_object_type("view_history", "procedure"),
            Ok(DatabaseObjectType::Procedure)
        );
        assert_eq!(
            registry.resolve_versioned_object_type("view_history", "trigger"),
            Ok(DatabaseObjectType::Trigger)
        );
        assert_eq!(
            registry.resolve_versioned_object_type("view_history", "event"),
            Ok(DatabaseObjectType::Event)
        );
    }

    #[test]
    fn action_registry_returns_typed_missing_binding_error() {
        let registry = ObjectsPanelActionRegistry::default();

        assert_eq!(
            registry.resolve_versioned_object_type("view_history", "sequence"),
            Err(ActionRegistryError {
                action_id: "view_history".to_string(),
                kind_id: "sequence".to_string(),
            })
        );
    }

    #[test]
    fn action_registry_resolves_open_table_action_resolution() {
        let registry = ObjectsPanelActionRegistry::default();

        let action_resolution = registry.resolve_objects_panel_action(
            "open",
            &[object_ref("table", "users")],
            Some("main".to_string()),
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::OpenTables {
                object_names: vec!["users".to_string()],
                database_name: Some("main".to_string()),
            }
        );
    }

    #[test]
    fn action_registry_resolves_open_table_with_schema_qualified_name() {
        let registry = ObjectsPanelActionRegistry::default();

        let action_resolution = registry.resolve_objects_panel_action(
            "open",
            &[schema_object_ref("table", "public", "event_log")],
            Some("erp_lab".to_string()),
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::OpenTables {
                object_names: vec!["public.event_log".to_string()],
                database_name: Some("erp_lab".to_string()),
            }
        );
    }

    #[test]
    fn action_registry_does_not_double_qualify_schema_prefixed_table_name() {
        let registry = ObjectsPanelActionRegistry::default();

        let action_resolution = registry.resolve_objects_panel_action(
            "open",
            &[schema_object_ref(
                "table",
                "analytics",
                "analytics.dim_dates",
            )],
            Some("erp_lab".to_string()),
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::OpenTables {
                object_names: vec!["analytics.dim_dates".to_string()],
                database_name: Some("erp_lab".to_string()),
            }
        );
    }

    #[test]
    fn action_registry_resolves_open_view_action_resolution() {
        let registry = ObjectsPanelActionRegistry::default();

        let action_resolution = registry.resolve_objects_panel_action(
            "open",
            &[object_ref("view", "users_view")],
            Some("main".to_string()),
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::OpenViews {
                object_names: vec!["users_view".to_string()],
                database_name: Some("main".to_string()),
            }
        );
    }

    #[test]
    fn action_registry_resolves_open_view_with_schema_qualified_name() {
        let registry = ObjectsPanelActionRegistry::default();

        let action_resolution = registry.resolve_objects_panel_action(
            "open",
            &[schema_object_ref("view", "analytics", "orders_view")],
            Some("erp_lab".to_string()),
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::OpenViews {
                object_names: vec!["analytics.orders_view".to_string()],
                database_name: Some("erp_lab".to_string()),
            }
        );
    }

    #[test]
    fn action_registry_routes_trigger_open_to_ddl_editor() {
        let registry = ObjectsPanelActionRegistry::default();

        let action_resolution = registry.resolve_objects_panel_action(
            "open",
            &[trigger_object_ref(
                "public",
                "payments",
                "trg_payment_audit",
            )],
            None,
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::OpenTriggerDdl {
                trigger_ref: SelectedObjectRef {
                    database: None,
                    name: "trg_payment_audit".to_string(),
                    schema: Some("public".to_string()),
                    signature: Some("payments".to_string()),
                    associated_table: Some("payments".to_string()),
                },
            }
        );
    }

    #[test]
    fn action_registry_routes_trigger_design_to_visual_designer() {
        let registry = ObjectsPanelActionRegistry::default();

        let action_resolution = registry.resolve_objects_panel_action(
            "design",
            &[trigger_object_ref(
                "public",
                "payments",
                "trg_payment_audit",
            )],
            None,
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::DesignTrigger {
                trigger_ref: SelectedObjectRef {
                    database: None,
                    name: "trg_payment_audit".to_string(),
                    schema: Some("public".to_string()),
                    signature: Some("payments".to_string()),
                    associated_table: Some("payments".to_string()),
                },
            }
        );
    }

    #[test]
    fn action_registry_preserves_routine_signature_for_open() {
        let registry = ObjectsPanelActionRegistry::default();

        let action_resolution = registry.resolve_objects_panel_action(
            "open",
            &[routine_object_ref(
                "function",
                "audit",
                "log_row_change",
                "integer, text",
            )],
            None,
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::OpenFunction {
                function_ref: SelectedObjectRef {
                    database: None,
                    name: "log_row_change".to_string(),
                    schema: Some("audit".to_string()),
                    signature: Some("integer, text".to_string()),
                    associated_table: None,
                },
            }
        );
    }

    #[test]
    fn action_registry_routes_sequence_open_and_design_separately() {
        let registry = ObjectsPanelActionRegistry::default();
        let sequence_ref = schema_object_ref("sequence", "public", "orders_id_seq");

        assert_eq!(
            registry.resolve_objects_panel_action(
                "open",
                std::slice::from_ref(&sequence_ref),
                None
            ),
            ResolvedObjectsPanelAction::OpenSequence {
                sequence_ref: SelectedObjectRef {
                    database: None,
                    name: "orders_id_seq".to_string(),
                    schema: Some("public".to_string()),
                    signature: None,
                    associated_table: None,
                },
            }
        );
        assert_eq!(
            registry.resolve_objects_panel_action("design", &[sequence_ref], None),
            ResolvedObjectsPanelAction::DesignSequence {
                sequence_ref: SelectedObjectRef {
                    database: None,
                    name: "orders_id_seq".to_string(),
                    schema: Some("public".to_string()),
                    signature: None,
                    associated_table: None,
                },
            }
        );
    }

    #[test]
    fn action_registry_routes_document_collection_open() {
        let registry = ObjectsPanelActionRegistry::default();
        let collection_ref = zqlz_core::ObjectsPanelObjectRef::new("document_collection", "users")
            .with_database_option(Some("app".to_string()));
        let view_ref = zqlz_core::ObjectsPanelObjectRef::new("document_view", "activeCustomers")
            .with_database_option(Some("app".to_string()));

        assert_eq!(
            registry.resolve_objects_panel_action("open", &[collection_ref], None),
            ResolvedObjectsPanelAction::OpenDocumentCollection {
                database_name: "app".to_string(),
                collection_name: "users".to_string(),
            }
        );
        assert_eq!(
            registry.resolve_objects_panel_action("open", &[view_ref], None),
            ResolvedObjectsPanelAction::OpenDocumentCollection {
                database_name: "app".to_string(),
                collection_name: "activeCustomers".to_string(),
            }
        );
        let inspect_ref = zqlz_core::ObjectsPanelObjectRef::new("document_collection", "users")
            .with_database_option(Some("app".to_string()));
        assert_eq!(
            registry.resolve_objects_panel_action("inspect", &[inspect_ref], None),
            ResolvedObjectsPanelAction::OpenGenericDdl {
                kind_id: "document_collection".to_string(),
                object_ref: SelectedObjectRef {
                    database: Some("app".to_string()),
                    name: "users".to_string(),
                    schema: None,
                    signature: None,
                    associated_table: None,
                },
            }
        );
    }

    #[test]
    fn action_registry_routes_enum_and_domain_forms() {
        let registry = ObjectsPanelActionRegistry::default();
        let enum_ref = schema_object_ref("enum", "public", "order_status");
        let domain_ref = schema_object_ref("domain", "public", "email_address");

        assert_eq!(
            registry.resolve_objects_panel_action("new_enum", &[], None),
            ResolvedObjectsPanelAction::OpenObjectForm {
                kind_id: "enum".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
            }
        );
        assert_eq!(
            registry.resolve_objects_panel_action("design", std::slice::from_ref(&enum_ref), None),
            ResolvedObjectsPanelAction::OpenObjectForm {
                kind_id: "enum".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: Some(enum_ref),
            }
        );
        assert_eq!(
            registry.resolve_objects_panel_action(
                "delete",
                std::slice::from_ref(&domain_ref),
                None
            ),
            ResolvedObjectsPanelAction::OpenObjectForm {
                kind_id: "domain".to_string(),
                mode: ObjectFormMode::Drop,
                object_ref: Some(domain_ref),
            }
        );
    }

    fn action(action_id: &str) -> zqlz_core::ObjectsPanelAction {
        zqlz_core::ObjectsPanelAction::new(action_id, action_id.to_uppercase())
    }

    fn kind(
        kind_id: &str,
        row_actions: Vec<zqlz_core::ObjectsPanelAction>,
    ) -> ObjectsPanelObjectKind {
        ObjectsPanelObjectKind::new(kind_id, kind_id, format!("{}s", kind_id))
            .columns(Vec::new())
            .row_actions(row_actions)
            .default_row_action("open")
    }

    #[test]
    fn manifest_action_coverage_reports_no_gaps_for_driver_style_manifest() {
        let table_actions = vec![
            action("open"),
            action("design"),
            action("rename"),
            action("duplicate"),
            action("empty"),
            action("import"),
            action("export"),
            action("dump_sql_structure_data"),
            action("dump_sql_structure"),
            action("copy_name"),
            action("copy_qualified_name"),
            action("view_history"),
            action("delete"),
            action("refresh"),
        ];
        let relation_actions = vec![
            action("open"),
            action("design"),
            action("rename"),
            action("duplicate"),
            action("copy_name"),
            action("copy_qualified_name"),
            action("view_history"),
            action("export"),
            action("delete"),
            action("refresh"),
        ];
        let routine_actions = vec![
            action("open"),
            action("design"),
            action("copy_name"),
            action("copy_qualified_name"),
            action("view_history"),
            action("refresh"),
        ];
        let metadata_actions = vec![
            action("copy_name"),
            action("copy_qualified_name"),
            action("refresh"),
        ];
        let enum_domain_actions = vec![
            action("design"),
            action("copy_name"),
            action("copy_qualified_name"),
            action("delete"),
            action("refresh"),
        ];
        let sequence_actions = vec![
            action("open"),
            action("design"),
            action("copy_name"),
            action("copy_qualified_name"),
            action("refresh"),
        ];
        let extension_actions = vec![
            action("open"),
            action("design"),
            action("copy_name"),
            action("copy_qualified_name"),
            action("delete"),
            action("refresh"),
        ];

        let manifest = ObjectsPanelManifest {
            object_kinds: vec![
                kind("table", table_actions.clone()),
                kind("partitioned_table", table_actions.clone()),
                kind("foreign_table", table_actions.clone()),
                kind("view", relation_actions.clone()),
                kind("materialized_view", relation_actions),
                kind("function", routine_actions.clone()),
                kind("procedure", routine_actions.clone()),
                kind("trigger", routine_actions),
                kind("sequence", sequence_actions),
                kind("enum", enum_domain_actions.clone()),
                kind("domain", enum_domain_actions),
                kind("range", metadata_actions.clone()),
                kind("composite_type", metadata_actions.clone()),
                kind("type", metadata_actions.clone()),
                kind("index", metadata_actions.clone()),
                kind("schema", metadata_actions.clone()),
                kind("extension", extension_actions),
                kind("foreign_server", metadata_actions.clone()),
                kind("foreign_data_wrapper", metadata_actions.clone()),
                kind("policy", metadata_actions.clone()),
                kind("publication", metadata_actions.clone()),
                kind("subscription", metadata_actions.clone()),
                kind("event_trigger", metadata_actions.clone()),
                kind("language", metadata_actions.clone()),
                kind("collation", metadata_actions.clone()),
                kind("tablespace", metadata_actions),
            ],
            toolbar_actions: vec![
                action("refresh"),
                action("new_table"),
                action("new_view"),
                action("new_enum"),
                action("new_domain"),
                action("import"),
                action("export"),
            ],
        };

        let gaps = manifest_action_coverage_gaps(&manifest);

        assert!(
            gaps.is_empty(),
            "unexpected objects-panel action coverage gaps: {gaps:?}"
        );
    }

    #[test]
    fn manifest_action_coverage_reports_unknown_actions() {
        let manifest = ObjectsPanelManifest {
            object_kinds: vec![
                ObjectsPanelObjectKind::new("table", "Table", "Tables")
                    .columns(Vec::new())
                    .row_actions(vec![
                        action("open"),
                        zqlz_core::ObjectsPanelAction::new("custom_archive", "Custom Archive"),
                    ])
                    .default_row_action("open"),
            ],
            toolbar_actions: vec![action("refresh")],
        };

        let gaps = manifest_action_coverage_gaps(&manifest);

        assert!(
            gaps.iter().any(|gap| gap.contains("custom_archive")),
            "expected an unsupported-action coverage gap, got: {gaps:?}"
        );
    }

    #[test]
    fn manifest_action_coverage_reports_actions_that_noop_in_specific_contexts() {
        let manifest = ObjectsPanelManifest {
            object_kinds: vec![kind(
                "sequence",
                vec![
                    action("open"),
                    action("design"),
                    action("delete"),
                    action("refresh"),
                ],
            )],
            toolbar_actions: vec![action("open"), action("refresh")],
        };

        let gaps = manifest_action_coverage_gaps(&manifest);

        assert!(
            !gaps
                .iter()
                .any(|gap| gap.contains("kind 'sequence'") && gap.contains("'open'")),
            "sequence open should be supported, got: {gaps:?}"
        );
        assert!(
            !gaps
                .iter()
                .any(|gap| gap.contains("kind 'sequence'") && gap.contains("'design'")),
            "sequence design should be supported, got: {gaps:?}"
        );
        assert!(
            gaps.iter()
                .any(|gap| gap.contains("kind 'sequence'") && gap.contains("'delete'")),
            "expected a sequence delete-action coverage gap, got: {gaps:?}"
        );
        assert!(
            !gaps
                .iter()
                .any(|gap| gap.contains("toolbar") && gap.contains("'open'")),
            "toolbar open should be supported through representative selections, got: {gaps:?}"
        );
        assert!(
            !gaps
                .iter()
                .any(|gap| gap.contains("refresh") && gap.contains("sequence")),
            "refresh should remain supported for the sequence kind, got: {gaps:?}"
        );
    }

    #[test]
    fn manifest_action_coverage_reports_toolbar_actions_against_representative_selections() {
        let manifest = ObjectsPanelManifest {
            object_kinds: vec![kind(
                "table",
                vec![action("open"), action("copy_name"), action("view_history")],
            )],
            toolbar_actions: vec![action("open")],
        };

        let gaps = manifest_action_coverage_gaps(&manifest);

        assert!(
            gaps.is_empty(),
            "selection-aware toolbar actions should be validated against representative selections, got: {gaps:?}"
        );
    }

    #[test]
    fn manifest_object_form_toolbar_action_routes_without_registry_binding() {
        let registry = ObjectsPanelActionRegistry::default();
        let manifest = ObjectsPanelManifest {
            object_kinds: Vec::new(),
            toolbar_actions: vec![
                zqlz_core::ObjectsPanelAction::new("create_custom_enum", "New Enum")
                    .object_form("enum", ObjectFormMode::Create),
            ],
        };

        let action_resolution = registry.resolve_objects_panel_action_with_manifest(
            "create_custom_enum",
            &[],
            None,
            Some(&manifest),
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::OpenObjectForm {
                kind_id: "enum".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
            }
        );
        assert!(
            manifest_action_coverage_gaps(&manifest).is_empty(),
            "manifest object-form actions should not require static registry ids"
        );
    }

    #[test]
    fn manifest_object_form_row_action_routes_selected_object() {
        let registry = ObjectsPanelActionRegistry::default();
        let domain_ref = object_ref("domain", "email_address");
        let manifest = ObjectsPanelManifest {
            object_kinds: vec![
                ObjectsPanelObjectKind::new("domain", "Domain", "Domains").row_actions(vec![
                    zqlz_core::ObjectsPanelAction::new("alter_domain_form", "Alter Domain")
                        .object_form("domain", ObjectFormMode::Edit)
                        .single_selection(),
                ]),
            ],
            toolbar_actions: Vec::new(),
        };

        let action_resolution = registry.resolve_objects_panel_action_with_manifest(
            "alter_domain_form",
            std::slice::from_ref(&domain_ref),
            None,
            Some(&manifest),
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::OpenObjectForm {
                kind_id: "domain".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: Some(domain_ref),
            }
        );
        assert!(
            manifest_action_coverage_gaps(&manifest).is_empty(),
            "manifest row object-form actions should not require static registry ids"
        );
    }

    #[test]
    fn objects_panel_action_registry_routes_open_table_action() {
        let registry = ObjectsPanelActionRegistry::default();
        let action_resolution = registry.resolve_objects_panel_action(
            "open",
            &[object_ref("table", "users")],
            Some("main".to_string()),
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::OpenTables {
                object_names: vec!["users".to_string()],
                database_name: Some("main".to_string()),
            }
        );
    }

    #[test]
    fn objects_panel_action_registry_routes_export_action_for_selected_objects() {
        let registry = ObjectsPanelActionRegistry::default();
        let action_resolution = registry.resolve_objects_panel_action(
            "export",
            &[
                object_ref("table", "users"),
                object_ref("view", "users_view"),
            ],
            None,
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::Export {
                object_names: vec!["users".to_string(), "users_view".to_string()],
            }
        );
    }

    #[test]
    fn objects_panel_action_registry_routes_export_action_without_selection() {
        let registry = ObjectsPanelActionRegistry::default();
        let action_resolution = registry.resolve_objects_panel_action("export", &[], None);

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::Export {
                object_names: Vec::new(),
            }
        );
    }

    #[test]
    fn objects_panel_action_registry_flags_unsupported_view_history_kind() {
        let registry = ObjectsPanelActionRegistry::default();
        let action_resolution = registry.resolve_objects_panel_action(
            "view_history",
            &[object_ref("sequence", "seq")],
            None,
        );

        assert_eq!(
            action_resolution,
            ResolvedObjectsPanelAction::UnsupportedViewHistoryKind {
                action_id: "view_history".to_string(),
                object_kind: "sequence".to_string(),
            }
        );
    }

    #[test]
    fn objects_panel_action_issue_message_formats_unsupported_view_history_kind() {
        let message = objects_panel_action_issue_message(
            &ResolvedObjectsPanelAction::UnsupportedViewHistoryKind {
                action_id: "view_history".to_string(),
                object_kind: "sequence".to_string(),
            },
        );

        assert_eq!(
            message,
            Some("The 'view_history' action is not available for 'sequence' objects.".to_string())
        );
    }

    #[test]
    fn objects_panel_action_issue_message_formats_unknown_action() {
        let message =
            objects_panel_action_issue_message(&ResolvedObjectsPanelAction::UnknownAction {
                action_id: "merge_rows".to_string(),
                object_count: 2,
            });

        assert_eq!(
            message,
            Some(
                "The 'merge_rows' action is not available in this objects panel context."
                    .to_string()
            )
        );
    }

    #[test]
    fn objects_panel_action_issue_message_ignores_handled_actions() {
        assert_eq!(
            objects_panel_action_issue_message(&ResolvedObjectsPanelAction::Refresh),
            None
        );
        assert_eq!(
            objects_panel_action_issue_message(&ResolvedObjectsPanelAction::Noop),
            None
        );
    }

    #[test]
    fn classify_objects_panel_action_resolution_distinguishes_outcomes() {
        assert_eq!(
            classify_objects_panel_action_resolution(&ResolvedObjectsPanelAction::Refresh),
            ObjectsPanelActionResolutionTelemetry {
                resolution: "handled",
                handler_registered: true,
                unknown_or_unsupported: false,
            }
        );
        assert_eq!(
            classify_objects_panel_action_resolution(&ResolvedObjectsPanelAction::Noop),
            ObjectsPanelActionResolutionTelemetry {
                resolution: "noop",
                handler_registered: false,
                unknown_or_unsupported: false,
            }
        );
        assert_eq!(
            classify_objects_panel_action_resolution(&ResolvedObjectsPanelAction::UnknownAction {
                action_id: "merge_rows".to_string(),
                object_count: 3,
            }),
            ObjectsPanelActionResolutionTelemetry {
                resolution: "unknown_action",
                handler_registered: false,
                unknown_or_unsupported: true,
            }
        );
        assert_eq!(
            classify_objects_panel_action_resolution(
                &ResolvedObjectsPanelAction::UnsupportedViewHistoryKind {
                    action_id: "view_history".to_string(),
                    object_kind: "sequence".to_string(),
                },
            ),
            ObjectsPanelActionResolutionTelemetry {
                resolution: "unsupported_kind",
                handler_registered: false,
                unknown_or_unsupported: true,
            }
        );
    }
}
