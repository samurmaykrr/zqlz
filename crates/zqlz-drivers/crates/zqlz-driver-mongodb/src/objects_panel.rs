use zqlz_core::{
    DocumentAdminObjectInfo, DocumentCollectionInfo, DocumentDatabaseObjects, DocumentFunctionInfo,
    DocumentGridFsBucketInfo, DocumentIndexInfo, ObjectFormMode, ObjectsPanelAction,
    ObjectsPanelColumn, ObjectsPanelData, ObjectsPanelManifest, ObjectsPanelObjectRef,
    ObjectsPanelRow,
};

pub fn document_databases_and_objects(
    _databases: Vec<(String, Option<i64>)>,
    objects: DocumentDatabaseObjects,
) -> ObjectsPanelData {
    let columns = vec![
        ObjectsPanelColumn::new("name", "Name")
            .width(300.0)
            .min_width(150.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("database", "Database")
            .width(180.0)
            .min_width(120.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("type", "Type")
            .width(120.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("options", "Options")
            .width(180.0)
            .min_width(120.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("document_count", "Documents")
            .width(100.0)
            .min_width(70.0)
            .resizable(false)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("index_count", "Indexes")
            .width(80.0)
            .min_width(60.0)
            .resizable(false)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("size", "Size")
            .width(90.0)
            .min_width(60.0)
            .resizable(false)
            .sortable()
            .text_right(),
    ];

    let mut rows = Vec::new();
    let DocumentDatabaseObjects {
        collections,
        indexes,
        functions,
        gridfs_buckets,
        users,
        roles,
        search_indexes,
        vector_indexes,
        server,
        sharding,
    } = objects;

    for collection in collections {
        let mut values = std::collections::BTreeMap::new();
        values.insert("name".to_string(), collection.name.clone());
        values.insert("database".to_string(), collection.database.clone());
        values.insert("type".to_string(), collection.collection_type.clone());
        values.insert(
            "options".to_string(),
            document_collection_options_summary(&collection),
        );
        values.insert(
            "document_count".to_string(),
            collection
                .document_count
                .map(|count| count.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
        values.insert(
            "index_count".to_string(),
            collection
                .index_count
                .map(|count| count.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
        values.insert(
            "size".to_string(),
            collection
                .size_bytes
                .map(|size| size.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );

        let object_type = if collection.collection_type == "view" {
            "document_view"
        } else {
            "document_collection"
        };
        rows.push(ObjectsPanelRow {
            name: collection.name.clone(),
            schema: Some(collection.database.clone()),
            object_type: object_type.to_string(),
            object_ref: Some(
                ObjectsPanelObjectRef::new(object_type, collection.name)
                    .with_database_option(Some(collection.database)),
            ),
            values,
            redis_database_index: None,
            key_value_info: None,
        });
    }

    for index in indexes {
        push_document_index_row(&mut rows, index);
    }

    for function in functions {
        push_document_function_row(&mut rows, function);
    }

    for bucket in gridfs_buckets {
        push_document_gridfs_row(&mut rows, bucket);
    }

    for object in users {
        push_document_admin_row(&mut rows, object, "document_user");
    }

    for object in roles {
        push_document_admin_row(&mut rows, object, "document_role");
    }

    for object in search_indexes {
        push_document_admin_row(&mut rows, object, "document_search_index");
    }

    for object in vector_indexes {
        push_document_admin_row(&mut rows, object, "document_vector_index");
    }

    for object in server {
        push_document_admin_row(&mut rows, object, "document_server");
    }

    for object in sharding {
        push_document_admin_row(&mut rows, object, "document_sharding");
    }

    ObjectsPanelData { columns, rows }
}

fn document_collection_options_summary(collection: &DocumentCollectionInfo) -> String {
    let mut options = Vec::new();
    if collection.validator_json.is_some() {
        options.push("validator");
    }
    if collection.collation_json.is_some() {
        options.push("collation");
    }
    if collection.timeseries_json.is_some() {
        options.push("time-series");
    }
    if collection
        .options_json
        .as_ref()
        .and_then(|options| options.get("capped"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        options.push("capped");
    }
    if collection.clustered_index_json.is_some() {
        options.push("clustered");
    }
    if collection.change_stream_pre_and_post_images == Some(true) {
        options.push("pre/post-images");
    }

    if options.is_empty() {
        "-".to_string()
    } else {
        options.join(", ")
    }
}

fn document_index_options_summary(index: &DocumentIndexInfo) -> String {
    let mut options = Vec::new();
    if index.unique {
        options.push("unique");
    }
    if index.sparse {
        options.push("sparse");
    }
    if index.ttl_seconds.is_some() {
        options.push("ttl");
    }
    if index.partial_filter_json.is_some() {
        options.push("partial");
    }
    if index.collation_json.is_some() {
        options.push("collation");
    }
    for option_name in ["wildcardProjection", "weights", "hidden"] {
        if index.options_json.get(option_name).is_some() {
            options.push(option_name);
        }
    }

    if options.is_empty() {
        "-".to_string()
    } else {
        options.join(", ")
    }
}

fn document_admin_options_summary(object: &DocumentAdminObjectInfo) -> String {
    if object.unavailable_reason.is_some() {
        return "unavailable".to_string();
    }

    let mut options = Vec::new();
    if object
        .details_json
        .get("roles")
        .and_then(|value| value.as_array())
        .is_some_and(|roles| !roles.is_empty())
    {
        options.push("roles");
    }
    if object
        .details_json
        .get("privileges")
        .and_then(|value| value.as_array())
        .is_some_and(|privileges| !privileges.is_empty())
    {
        options.push("privileges");
    }
    if object.details_json.get("latestDefinition").is_some()
        || object.details_json.get("mappings").is_some()
        || object.details_json.get("synonyms").is_some()
    {
        options.push("definition");
    }
    if object.details_json.get("key").is_some() {
        options.push("shard key");
    }
    if object.details_json.get("chunks").is_some() || object.details_json.get("chunk").is_some() {
        options.push("chunks");
    }
    if object.details_json.get("zones").is_some() || object.details_json.get("zone").is_some() {
        options.push("zones");
    }
    if object.kind == "server" && !object.details_json.is_null() {
        options.push("metadata");
    }

    if options.is_empty() {
        "-".to_string()
    } else {
        options.join(", ")
    }
}

fn push_document_index_row(rows: &mut Vec<ObjectsPanelRow>, index: DocumentIndexInfo) {
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), index.name.clone());
    values.insert("database".to_string(), index.database.clone());
    values.insert("type".to_string(), format!("index:{}", index.kind));
    values.insert(
        "options".to_string(),
        document_index_options_summary(&index),
    );
    values.insert("document_count".to_string(), "-".to_string());
    values.insert("index_count".to_string(), "-".to_string());
    values.insert("size".to_string(), "-".to_string());

    rows.push(ObjectsPanelRow {
        name: index.name.clone(),
        schema: Some(index.collection.clone()),
        object_type: "document_index".to_string(),
        object_ref: Some(
            ObjectsPanelObjectRef::new("document_index", index.name)
                .with_database_option(Some(index.database))
                .with_schema_option(Some(index.collection)),
        ),
        values,
        redis_database_index: None,
        key_value_info: None,
    });
}

fn push_document_function_row(rows: &mut Vec<ObjectsPanelRow>, function: DocumentFunctionInfo) {
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), function.name.clone());
    values.insert("database".to_string(), function.database.clone());
    values.insert("type".to_string(), "function".to_string());
    values.insert("options".to_string(), "-".to_string());
    values.insert("document_count".to_string(), "-".to_string());
    values.insert("index_count".to_string(), "-".to_string());
    values.insert("size".to_string(), "-".to_string());

    rows.push(ObjectsPanelRow {
        name: function.name.clone(),
        schema: Some("system.js".to_string()),
        object_type: "document_function".to_string(),
        object_ref: Some(
            ObjectsPanelObjectRef::new("document_function", function.name)
                .with_database_option(Some(function.database)),
        ),
        values,
        redis_database_index: None,
        key_value_info: None,
    });
}

fn push_document_gridfs_row(rows: &mut Vec<ObjectsPanelRow>, bucket: DocumentGridFsBucketInfo) {
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), bucket.name.clone());
    values.insert("database".to_string(), bucket.database.clone());
    values.insert("type".to_string(), "gridfs_bucket".to_string());
    values.insert("options".to_string(), "-".to_string());
    values.insert(
        "document_count".to_string(),
        bucket
            .file_count
            .map(|count| count.to_string())
            .unwrap_or_else(|| "-".to_string()),
    );
    values.insert("index_count".to_string(), "-".to_string());
    values.insert(
        "size".to_string(),
        bucket
            .size_bytes
            .map(|size| size.to_string())
            .unwrap_or_else(|| "-".to_string()),
    );

    rows.push(ObjectsPanelRow {
        name: bucket.name.clone(),
        schema: Some(bucket.files_collection.clone()),
        object_type: "document_gridfs_bucket".to_string(),
        object_ref: Some(
            ObjectsPanelObjectRef::new("document_gridfs_bucket", bucket.name)
                .with_database_option(Some(bucket.database))
                .with_schema_option(Some(bucket.files_collection)),
        ),
        values,
        redis_database_index: None,
        key_value_info: None,
    });
}

fn push_document_admin_row(
    rows: &mut Vec<ObjectsPanelRow>,
    object: DocumentAdminObjectInfo,
    object_type: &'static str,
) {
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), object.name.clone());
    values.insert("database".to_string(), object.database.clone());
    values.insert("type".to_string(), object.kind.clone());
    values.insert(
        "options".to_string(),
        document_admin_options_summary(&object),
    );
    values.insert("document_count".to_string(), "-".to_string());
    values.insert("index_count".to_string(), "-".to_string());
    values.insert("size".to_string(), "-".to_string());

    rows.push(ObjectsPanelRow {
        name: object.name.clone(),
        schema: Some(object.kind.clone()),
        object_type: object_type.to_string(),
        object_ref: Some(
            ObjectsPanelObjectRef::new(object_type, object.name)
                .with_database_option(Some(object.database))
                .with_schema_option(Some(object.kind)),
        ),
        values,
        redis_database_index: None,
        key_value_info: None,
    });
}

pub fn document_databases_and_objects_with_manifest(
    databases: Vec<(String, Option<i64>)>,
    objects: DocumentDatabaseObjects,
) -> (ObjectsPanelData, ObjectsPanelManifest) {
    let data = document_databases_and_objects(databases, objects);
    let mut manifest = ObjectsPanelManifest::from_data(&data);
    manifest.toolbar_actions = vec![
        ObjectsPanelAction::new("refresh", "Refresh")
            .icon_key("refresh")
            .refreshes_objects_panel(),
        ObjectsPanelAction::new("new_document_collection", "New Collection")
            .icon_key("create")
            .object_form("document_collection", ObjectFormMode::Create),
        ObjectsPanelAction::new("new_document_view", "New View")
            .icon_key("create")
            .object_form("document_view", ObjectFormMode::Create),
        ObjectsPanelAction::new("new_document_index", "New Index")
            .icon_key("create")
            .object_form("document_index", ObjectFormMode::Create),
        ObjectsPanelAction::new("new_document_user", "New User")
            .icon_key("create")
            .object_form("document_user", ObjectFormMode::Create),
        ObjectsPanelAction::new("new_document_role", "New Role")
            .icon_key("create")
            .object_form("document_role", ObjectFormMode::Create),
    ];
    if let Some(collection_kind) = manifest
        .object_kinds
        .iter_mut()
        .find(|kind| kind.id == "document_collection")
    {
        collection_kind.row_actions = vec![
            ObjectsPanelAction::new("open", "Open"),
            ObjectsPanelAction::new("inspect", "Inspect"),
            ObjectsPanelAction::new("design", "Design")
                .single_selection()
                .object_form("document_collection", ObjectFormMode::Edit),
            ObjectsPanelAction::new("copy_name", "Copy Name"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name"),
            ObjectsPanelAction::new("delete", "Drop")
                .destructive()
                .object_form("document_collection", ObjectFormMode::Drop),
            ObjectsPanelAction::new("refresh", "Refresh"),
        ];
    }
    if let Some(view_kind) = manifest
        .object_kinds
        .iter_mut()
        .find(|kind| kind.id == "document_view")
    {
        view_kind.row_actions = vec![
            ObjectsPanelAction::new("open", "Open"),
            ObjectsPanelAction::new("inspect", "Inspect"),
            ObjectsPanelAction::new("design", "Design")
                .single_selection()
                .object_form("document_view", ObjectFormMode::Edit),
            ObjectsPanelAction::new("copy_name", "Copy Name"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name"),
            ObjectsPanelAction::new("delete", "Drop")
                .destructive()
                .object_form("document_view", ObjectFormMode::Drop),
            ObjectsPanelAction::new("refresh", "Refresh"),
        ];
    }
    for kind_id in [
        "document_function",
        "document_search_index",
        "document_vector_index",
        "document_server",
        "document_sharding",
    ] {
        if let Some(kind) = manifest
            .object_kinds
            .iter_mut()
            .find(|kind| kind.id == kind_id)
        {
            kind.row_actions = document_read_only_actions();
        }
    }
    if let Some(kind) = manifest
        .object_kinds
        .iter_mut()
        .find(|kind| kind.id == "document_gridfs_bucket")
    {
        kind.row_actions = vec![
            ObjectsPanelAction::new("open", "Open"),
            ObjectsPanelAction::new("inspect", "Inspect"),
            ObjectsPanelAction::new("copy_name", "Copy Name"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name"),
            ObjectsPanelAction::new("refresh", "Refresh"),
        ];
    }
    for kind_id in ["document_index", "document_user", "document_role"] {
        if let Some(kind) = manifest
            .object_kinds
            .iter_mut()
            .find(|kind| kind.id == kind_id)
        {
            let mut actions = vec![ObjectsPanelAction::new("open", "Open")];
            if kind_id == "document_user" || kind_id == "document_role" {
                actions.push(
                    ObjectsPanelAction::new("design", "Edit")
                        .single_selection()
                        .object_form(kind_id, ObjectFormMode::Edit),
                );
            }
            actions.extend([
                ObjectsPanelAction::new("copy_name", "Copy Name"),
                ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name"),
                ObjectsPanelAction::new("delete", "Drop")
                    .destructive()
                    .object_form(kind_id, ObjectFormMode::Drop),
                ObjectsPanelAction::new("refresh", "Refresh"),
            ]);
            kind.row_actions = actions;
        }
    }
    (data, manifest)
}

fn document_read_only_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("open", "Open"),
        ObjectsPanelAction::new("copy_name", "Copy Name"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name"),
        ObjectsPanelAction::new("refresh", "Refresh"),
    ]
}
