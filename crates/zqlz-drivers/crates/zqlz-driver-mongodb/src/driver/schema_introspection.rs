use super::*;

fn object_form_string(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    field_id: &str,
) -> Option<String> {
    values
        .get(field_id)
        .and_then(ObjectFormValue::as_string)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn object_form_bool(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    field_id: &str,
) -> bool {
    values
        .get(field_id)
        .and_then(ObjectFormValue::as_bool)
        .unwrap_or(false)
}

fn parse_json_form_field(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    field_id: &str,
) -> Result<Option<serde_json::Value>> {
    let Some(value) = object_form_string(values, field_id) else {
        return Ok(None);
    };
    serde_json::from_str::<serde_json::Value>(&value)
        .map(Some)
        .map_err(|error| ZqlzError::Driver(format!("Invalid JSON in {field_id}: {error}")))
}

fn text_field(id: &str, label: &str, default_value: impl Into<String>) -> ObjectFormField {
    ObjectFormField::new(id, label, ObjectFormFieldKind::Text)
        .default_value(ObjectFormValue::String(default_value.into()))
}

fn json_field(id: &str, label: &str, default_value: impl Into<String>) -> ObjectFormField {
    ObjectFormField::new(id, label, ObjectFormFieldKind::TextArea)
        .default_value(ObjectFormValue::String(default_value.into()))
}

fn read_only_field(id: &str, label: &str, value: impl Into<String>) -> ObjectFormField {
    text_field(id, label, value).read_only()
}

fn render_mongodb_command(command: serde_json::Value) -> Result<String> {
    serde_json::to_string_pretty(&command)
        .map_err(|error| ZqlzError::Driver(format!("Failed to render MongoDB command: {error}")))
}

#[async_trait]
impl SchemaIntrospection for MongoDbConnection {
    fn object_kind_support(&self) -> Option<ObjectKindSupport> {
        Some(OBJECT_KINDS)
    }

    fn default_schema_name(&self) -> Option<String> {
        None
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        Ok(self
            .list_document_databases()
            .await?
            .into_iter()
            .map(|database| DatabaseInfo {
                name: database.name,
                owner: None,
                encoding: None,
                size_bytes: database.size_bytes.map(|size| size as i64),
                comment: None,
            })
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        Ok(Vec::new())
    }
    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<TableInfo>> {
        Ok(Vec::new())
    }
    async fn list_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        Ok(Vec::new())
    }
    async fn list_tables_extended(&self, _schema: Option<&str>) -> Result<ObjectsPanelData> {
        let databases = self.list_document_databases().await?;
        let mut objects = DocumentDatabaseObjects::default();

        for database in &databases {
            let database_objects = self.list_database_objects(&database.name).await?;
            objects.collections.extend(database_objects.collections);
            objects.indexes.extend(database_objects.indexes);
            objects.functions.extend(database_objects.functions);
            objects
                .gridfs_buckets
                .extend(database_objects.gridfs_buckets);
            objects.users.extend(database_objects.users);
            objects.roles.extend(database_objects.roles);
            objects
                .search_indexes
                .extend(database_objects.search_indexes);
            objects
                .vector_indexes
                .extend(database_objects.vector_indexes);
            objects.server.extend(database_objects.server);
            objects.sharding.extend(database_objects.sharding);
        }

        let databases = databases
            .into_iter()
            .map(|database| {
                (
                    database.name,
                    database
                        .size_bytes
                        .and_then(|size| i64::try_from(size).ok()),
                )
            })
            .collect();

        Ok(objects_panel::document_databases_and_objects(
            databases, objects,
        ))
    }

    async fn list_objects_panel_manifest(
        &self,
        _schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        let databases = self.list_document_databases().await?;
        let mut objects = DocumentDatabaseObjects::default();

        for database in &databases {
            let database_objects = self.list_database_objects(&database.name).await?;
            objects.collections.extend(database_objects.collections);
            objects.indexes.extend(database_objects.indexes);
            objects.functions.extend(database_objects.functions);
            objects
                .gridfs_buckets
                .extend(database_objects.gridfs_buckets);
            objects.users.extend(database_objects.users);
            objects.roles.extend(database_objects.roles);
            objects
                .search_indexes
                .extend(database_objects.search_indexes);
            objects
                .vector_indexes
                .extend(database_objects.vector_indexes);
            objects.server.extend(database_objects.server);
            objects.sharding.extend(database_objects.sharding);
        }

        let databases = databases
            .into_iter()
            .map(|database| {
                (
                    database.name,
                    database
                        .size_bytes
                        .and_then(|size| i64::try_from(size).ok()),
                )
            })
            .collect();

        let (_, manifest) =
            objects_panel::document_databases_and_objects_with_manifest(databases, objects);
        Ok(manifest)
    }
    async fn get_table(&self, _schema: Option<&str>, name: &str) -> Result<TableDetails> {
        Err(ZqlzError::NotSupported(format!(
            "MongoDB table details are not supported for {name}"
        )))
    }
    async fn get_columns(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<zqlz_core::ColumnInfo>> {
        Ok(Vec::new())
    }
    async fn get_indexes(&self, _schema: Option<&str>, _table: &str) -> Result<Vec<IndexInfo>> {
        Ok(Vec::new())
    }
    async fn get_foreign_keys(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        Ok(Vec::new())
    }
    async fn get_primary_key(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        Ok(None)
    }
    async fn get_constraints(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        Ok(Vec::new())
    }
    async fn list_functions(&self, _schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        Ok(Vec::new())
    }
    async fn list_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        Ok(Vec::new())
    }
    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        _table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        Ok(Vec::new())
    }
    async fn list_sequences(&self, _schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        Ok(Vec::new())
    }
    async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        Ok(Vec::new())
    }
    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        Ok(format!("-- MongoDB metadata\n-- name: {}\n", object.name))
    }
    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        Ok(Vec::new())
    }

    async fn object_form_spec(
        &self,
        request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        let name = request
            .object_ref
            .as_ref()
            .map(|object_ref| object_ref.name.clone())
            .unwrap_or_default();
        let collection = request
            .object_ref
            .as_ref()
            .and_then(|object_ref| object_ref.schema.clone())
            .unwrap_or_default();
        let spec = match (request.kind_id.as_str(), request.mode) {
            ("document_collection", ObjectFormMode::Create) => ObjectFormSpec::new(
                "document_collection",
                request.mode,
                "Create MongoDB Collection",
            )
            .sections(vec![ObjectFormSection::new(vec![
                text_field("name", "Name", "").required(),
                json_field("validator", "Validator JSON", ""),
                text_field("validation_level", "Validation Level", "strict"),
                text_field("validation_action", "Validation Action", "error"),
                json_field("collation", "Collation JSON", ""),
                json_field("timeseries", "Time Series JSON", ""),
                json_field("clustered_index", "Clustered Index JSON", ""),
                ObjectFormField::new("capped", "Capped", ObjectFormFieldKind::Checkbox)
                    .default_value(ObjectFormValue::Bool(false)),
                text_field("size", "Capped Size Bytes", ""),
                text_field("max", "Capped Max Documents", ""),
                ObjectFormField::new(
                    "change_stream_pre_post_images",
                    "Change Stream Pre/Post Images",
                    ObjectFormFieldKind::Checkbox,
                )
                .default_value(ObjectFormValue::Bool(false)),
            ])]),
            ("document_collection", ObjectFormMode::Edit) => ObjectFormSpec::new(
                "document_collection",
                request.mode,
                "Modify MongoDB Collection",
            )
            .sections(vec![ObjectFormSection::new(vec![
                read_only_field("name", "Name", name),
                json_field("validator", "Validator JSON", ""),
                text_field("validation_level", "Validation Level", ""),
                text_field("validation_action", "Validation Action", ""),
                text_field("expire_after_seconds", "Time Series TTL Seconds", ""),
                text_field("timeseries_granularity", "Time Series Granularity", ""),
                text_field("capped_size", "Capped Size Bytes", ""),
                text_field("capped_max", "Capped Max Documents", ""),
                ObjectFormField::new(
                    "apply_change_stream_pre_post_images",
                    "Apply Change Stream Pre/Post Images",
                    ObjectFormFieldKind::Checkbox,
                )
                .default_value(ObjectFormValue::Bool(false)),
                ObjectFormField::new(
                    "change_stream_pre_post_images",
                    "Change Stream Pre/Post Images",
                    ObjectFormFieldKind::Checkbox,
                )
                .default_value(ObjectFormValue::Bool(false)),
            ])]),
            (
                "document_collection"
                | "document_view"
                | "document_index"
                | "document_user"
                | "document_role",
                ObjectFormMode::Drop,
            ) => ObjectFormSpec::new(&request.kind_id, request.mode, "Drop MongoDB Object")
                .sections(vec![ObjectFormSection::new(vec![
                    read_only_field("collection", "Collection", collection),
                    read_only_field("name", "Name", name),
                    ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox)
                        .default_value(ObjectFormValue::Bool(false)),
                ])]),
            ("document_view", ObjectFormMode::Create) => {
                ObjectFormSpec::new("document_view", request.mode, "Create MongoDB View").sections(
                    vec![ObjectFormSection::new(vec![
                        text_field("name", "View Name", name).required(),
                        text_field("view_on", "Source Collection", "").required(),
                        json_field("pipeline", "Pipeline JSON Array", "[]").required(),
                        json_field("collation", "Collation JSON", ""),
                    ])],
                )
            }
            ("document_view", ObjectFormMode::Edit) => {
                ObjectFormSpec::new("document_view", request.mode, "Modify MongoDB View").sections(
                    vec![ObjectFormSection::new(vec![
                        read_only_field("name", "View Name", name),
                        text_field("view_on", "Source Collection", "").required(),
                        json_field("pipeline", "Pipeline JSON Array", "[]").required(),
                    ])],
                )
            }
            ("document_index", ObjectFormMode::Create) => {
                ObjectFormSpec::new("document_index", request.mode, "Create MongoDB Index")
                    .sections(vec![ObjectFormSection::new(vec![
                        text_field("collection", "Collection", collection).required(),
                        text_field("name", "Index Name", "").required(),
                        json_field("keys", "Keys JSON", "{\"field\": 1}").required(),
                        ObjectFormField::new("unique", "Unique", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                        ObjectFormField::new("sparse", "Sparse", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                        text_field("expire_after_seconds", "TTL Seconds", ""),
                        json_field("partial_filter", "Partial Filter JSON", ""),
                        json_field("collation", "Collation JSON", ""),
                        json_field("additional_options", "Additional Options JSON", ""),
                    ])])
            }
            ("document_user", ObjectFormMode::Create) => {
                ObjectFormSpec::new("document_user", request.mode, "Create MongoDB User").sections(
                    vec![ObjectFormSection::new(vec![
                        text_field("name", "User", "").required(),
                        text_field("password", "Password", "").required(),
                        json_field("roles", "Roles JSON Array", "[]"),
                        ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                    ])],
                )
            }
            ("document_user", ObjectFormMode::Edit) => {
                ObjectFormSpec::new("document_user", request.mode, "Update MongoDB User").sections(
                    vec![ObjectFormSection::new(vec![
                        read_only_field("name", "User", name),
                        text_field("password", "New Password", ""),
                        json_field("roles", "Roles JSON Array", ""),
                        ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                    ])],
                )
            }
            ("document_role", ObjectFormMode::Create) => {
                ObjectFormSpec::new("document_role", request.mode, "Create MongoDB Role").sections(
                    vec![ObjectFormSection::new(vec![
                        text_field("name", "Role", "").required(),
                        json_field("privileges", "Privileges JSON Array", "[]"),
                        json_field("roles", "Inherited Roles JSON Array", "[]"),
                        ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                    ])],
                )
            }
            ("document_role", ObjectFormMode::Edit) => {
                ObjectFormSpec::new("document_role", request.mode, "Update MongoDB Role").sections(
                    vec![ObjectFormSection::new(vec![
                        read_only_field("name", "Role", name),
                        json_field("privileges", "Privileges JSON Array", ""),
                        json_field("roles", "Inherited Roles JSON Array", ""),
                        ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox)
                            .default_value(ObjectFormValue::Bool(false)),
                    ])],
                )
            }
            _ => return Ok(None),
        };
        Ok(Some(spec))
    }

    async fn generate_object_form_ddl(
        &self,
        request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        let values = &request.values;
        let name = object_form_string(values, "name")
            .or_else(|| {
                request
                    .object_ref
                    .as_ref()
                    .map(|object_ref| object_ref.name.clone())
            })
            .ok_or_else(|| ZqlzError::Driver("Name is required".to_string()))?;
        let confirmed = || object_form_bool(values, "confirm");
        let command = match (request.kind_id.as_str(), request.mode) {
            ("document_collection", ObjectFormMode::Create) => {
                let mut command = serde_json::json!({ "create": name });
                for (field_id, command_field) in [
                    ("validator", "validator"),
                    ("collation", "collation"),
                    ("timeseries", "timeseries"),
                    ("clustered_index", "clusteredIndex"),
                ] {
                    if let Some(value) = parse_json_form_field(values, field_id)? {
                        command[command_field] = value;
                    }
                }
                if let Some(value) = object_form_string(values, "validation_level") {
                    command["validationLevel"] = serde_json::Value::String(value);
                }
                if let Some(value) = object_form_string(values, "validation_action") {
                    command["validationAction"] = serde_json::Value::String(value);
                }
                if object_form_bool(values, "capped") {
                    command["capped"] = serde_json::Value::Bool(true);
                    if let Some(value) = object_form_string(values, "size")
                        .and_then(|value| value.parse::<i64>().ok())
                    {
                        command["size"] = value.into();
                    }
                    if let Some(value) = object_form_string(values, "max")
                        .and_then(|value| value.parse::<i64>().ok())
                    {
                        command["max"] = value.into();
                    }
                }
                if object_form_bool(values, "change_stream_pre_post_images") {
                    command["changeStreamPreAndPostImages"] =
                        serde_json::json!({ "enabled": true });
                }
                command
            }
            ("document_collection", ObjectFormMode::Edit) => {
                let mut command = serde_json::json!({ "collMod": name });
                if let Some(value) = parse_json_form_field(values, "validator")? {
                    command["validator"] = value;
                }
                if let Some(value) = object_form_string(values, "validation_level") {
                    command["validationLevel"] = value.into();
                }
                if let Some(value) = object_form_string(values, "validation_action") {
                    command["validationAction"] = value.into();
                }
                if let Some(value) = object_form_string(values, "expire_after_seconds") {
                    if value == "off" {
                        command["expireAfterSeconds"] = value.into();
                    } else if let Ok(seconds) = value.parse::<i64>() {
                        command["expireAfterSeconds"] = seconds.into();
                    }
                }
                if let Some(granularity) = object_form_string(values, "timeseries_granularity") {
                    command["timeseries"] = serde_json::json!({ "granularity": granularity });
                }
                if let Some(size) = object_form_string(values, "capped_size")
                    .and_then(|value| value.parse::<i64>().ok())
                {
                    command["cappedSize"] = size.into();
                }
                if let Some(max) = object_form_string(values, "capped_max")
                    .and_then(|value| value.parse::<i64>().ok())
                {
                    command["cappedMax"] = max.into();
                }
                if object_form_bool(values, "apply_change_stream_pre_post_images") {
                    command["changeStreamPreAndPostImages"] = serde_json::json!({ "enabled": object_form_bool(values, "change_stream_pre_post_images") });
                }
                if command.get("timeseries").is_some()
                    && command.as_object().is_some_and(|fields| fields.len() > 2)
                {
                    return Err(ZqlzError::Driver(
                        "MongoDB time-series collMod changes must be submitted without other collection modifications".to_string(),
                    ));
                }
                command
            }
            ("document_view", ObjectFormMode::Create) => {
                let mut command = serde_json::json!({
                    "create": name,
                    "viewOn": object_form_string(values, "view_on").ok_or_else(|| ZqlzError::Driver("Source collection is required".to_string()))?,
                    "pipeline": parse_json_form_field(values, "pipeline")?.unwrap_or_else(|| serde_json::json!([])),
                });
                if let Some(value) = parse_json_form_field(values, "collation")? {
                    command["collation"] = value;
                }
                command
            }
            ("document_view", ObjectFormMode::Edit) => {
                serde_json::json!({
                    "collMod": name,
                    "viewOn": object_form_string(values, "view_on").ok_or_else(|| ZqlzError::Driver("Source collection is required".to_string()))?,
                    "pipeline": parse_json_form_field(values, "pipeline")?.unwrap_or_else(|| serde_json::json!([])),
                })
            }
            ("document_index", ObjectFormMode::Create) => {
                let collection = object_form_string(values, "collection")
                    .ok_or_else(|| ZqlzError::Driver("Collection is required".to_string()))?;
                let mut index = serde_json::json!({ "key": parse_json_form_field(values, "keys")?.ok_or_else(|| ZqlzError::Driver("Index keys are required".to_string()))?, "name": name });
                if object_form_bool(values, "unique") {
                    index["unique"] = true.into();
                }
                if object_form_bool(values, "sparse") {
                    index["sparse"] = true.into();
                }
                if let Some(value) = object_form_string(values, "expire_after_seconds")
                    .and_then(|value| value.parse::<i64>().ok())
                {
                    index["expireAfterSeconds"] = value.into();
                }
                if let Some(value) = parse_json_form_field(values, "partial_filter")? {
                    index["partialFilterExpression"] = value;
                }
                if let Some(value) = parse_json_form_field(values, "collation")? {
                    index["collation"] = value;
                }
                if let Some(value) = parse_json_form_field(values, "additional_options")? {
                    let options = value.as_object().ok_or_else(|| {
                        ZqlzError::Driver(
                            "Additional index options must be a JSON object".to_string(),
                        )
                    })?;
                    for (key, value) in options {
                        index[key] = value.clone();
                    }
                }
                serde_json::json!({ "createIndexes": collection, "indexes": [index] })
            }
            ("document_collection" | "document_view", ObjectFormMode::Drop) if confirmed() => {
                serde_json::json!({ "drop": name })
            }
            ("document_index", ObjectFormMode::Drop) if confirmed() => {
                let collection = object_form_string(values, "collection")
                    .or_else(|| {
                        request
                            .object_ref
                            .as_ref()
                            .and_then(|object_ref| object_ref.schema.clone())
                    })
                    .ok_or_else(|| ZqlzError::Driver("Collection is required".to_string()))?;
                serde_json::json!({ "dropIndexes": collection, "index": name })
            }
            ("document_user", ObjectFormMode::Create) if confirmed() => {
                serde_json::json!({ "createUser": name, "pwd": object_form_string(values, "password").ok_or_else(|| ZqlzError::Driver("Password is required".to_string()))?, "roles": parse_json_form_field(values, "roles")?.unwrap_or_else(|| serde_json::json!([])) })
            }
            ("document_user", ObjectFormMode::Edit) if confirmed() => {
                let mut command = serde_json::json!({ "updateUser": name });
                if let Some(password) = object_form_string(values, "password")
                    && !password.is_empty()
                {
                    command["pwd"] = password.into();
                }
                if let Some(roles) = parse_json_form_field(values, "roles")? {
                    command["roles"] = roles;
                }
                if command.as_object().is_some_and(|fields| fields.len() == 1) {
                    return Err(ZqlzError::Driver(
                        "At least one user change is required".to_string(),
                    ));
                }
                command
            }
            ("document_user", ObjectFormMode::Drop) if confirmed() => {
                serde_json::json!({ "dropUser": name })
            }
            ("document_role", ObjectFormMode::Create) if confirmed() => {
                serde_json::json!({ "createRole": name, "privileges": parse_json_form_field(values, "privileges")?.unwrap_or_else(|| serde_json::json!([])), "roles": parse_json_form_field(values, "roles")?.unwrap_or_else(|| serde_json::json!([])) })
            }
            ("document_role", ObjectFormMode::Edit) if confirmed() => {
                let mut command = serde_json::json!({ "updateRole": name });
                if let Some(privileges) = parse_json_form_field(values, "privileges")? {
                    command["privileges"] = privileges;
                }
                if let Some(roles) = parse_json_form_field(values, "roles")? {
                    command["roles"] = roles;
                }
                if command.as_object().is_some_and(|fields| fields.len() == 1) {
                    return Err(ZqlzError::Driver(
                        "At least one role change is required".to_string(),
                    ));
                }
                command
            }
            ("document_role", ObjectFormMode::Drop) if confirmed() => {
                serde_json::json!({ "dropRole": name })
            }
            _ => {
                return Err(ZqlzError::Driver(
                    "Confirm the destructive/admin action first".to_string(),
                ));
            }
        };
        Ok(vec![render_mongodb_command(command)?])
    }
}
