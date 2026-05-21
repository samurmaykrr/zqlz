//! ClickHouse schema introspection implementation

use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, ConstraintType, DatabaseInfo, DatabaseObject,
    Dependency, ForeignKeyInfo, FunctionInfo, IndexInfo, ObjectFormDdlRequest, ObjectFormField,
    ObjectFormFieldKind, ObjectFormMode, ObjectFormOption, ObjectFormSection, ObjectFormSpec,
    ObjectFormSpecRequest, ObjectType, ObjectsPanelAction, ObjectsPanelColumn, ObjectsPanelData,
    ObjectsPanelManifest, ObjectsPanelObjectKind, ObjectsPanelObjectRef, ObjectsPanelRow,
    PrimaryKeyInfo, ProcedureInfo, Result, SchemaInfo, SchemaIntrospection, SequenceInfo,
    TableDetails, TableInfo, TableType, TriggerInfo, TypeInfo, TypeKind, Value, ViewInfo,
    ZqlzError,
};

use super::ClickHouseConnection;

#[async_trait]
impl SchemaIntrospection for ClickHouseConnection {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .query("SELECT name, engine FROM system.databases", &[])
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| DatabaseInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                owner: None,
                encoding: None,
                size_bytes: None,
                comment: row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .map(|s| format!("Engine: {}", s)),
            })
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        // ClickHouse databases act as schemas
        let dbs = self.list_databases().await?;
        Ok(dbs
            .into_iter()
            .map(|db| SchemaInfo {
                name: db.name,
                owner: None,
                comment: db.comment,
            })
            .collect())
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let database = schema.unwrap_or(self.database());
        let result = self
            .query(
                "SELECT name, engine, total_rows, total_bytes, comment
                     FROM system.tables
                     WHERE database = ? AND is_temporary = 0 AND engine NOT LIKE '%View%'
                     ORDER BY name",
                &[Value::String(database.to_string())],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| TableInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                schema: Some(database.to_string()),
                table_type: TableType::Table,
                owner: None,
                row_count: row.get(2).and_then(|v| v.as_i64()),
                size_bytes: row.get(3).and_then(|v| v.as_i64()),
                comment: row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string()),
                index_count: None,
                trigger_count: None,
                key_value_info: None,
            })
            .collect())
    }

    async fn list_objects_panel_data_for_kind(
        &self,
        schema: Option<&str>,
        kind_id: &str,
    ) -> Result<ObjectsPanelData> {
        match kind_id {
            "database" => clickhouse_database_objects_panel_data(self.list_databases().await?),
            "table" => clickhouse_table_objects_panel_data(self.list_tables(schema).await?),
            "view" | "materialized_view" => {
                clickhouse_view_objects_panel_data(self.list_views(schema).await?, kind_id)
            }
            "type" => clickhouse_type_objects_panel_data(self.list_types(schema).await?),
            _ => Ok(ObjectsPanelData::new(
                clickhouse_generic_objects_panel_columns(),
            )),
        }
    }

    async fn list_objects_panel_manifest(
        &self,
        _schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        Ok(clickhouse_objects_panel_manifest())
    }

    async fn object_form_spec(
        &self,
        request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        Ok(clickhouse_object_form_spec(request))
    }

    async fn generate_object_form_ddl(
        &self,
        request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        clickhouse_object_form_ddl(request)
    }

    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let database = schema.unwrap_or(self.database());
        let result = self
            .query(
                "SELECT name, engine, as_select, comment
                     FROM system.tables
                     WHERE database = ? AND engine LIKE '%View%'
                     ORDER BY name",
                &[Value::String(database.to_string())],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| {
                let engine = row.get(1).and_then(|v| v.as_str()).unwrap_or("");
                ViewInfo {
                    name: row
                        .get(0)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    schema: Some(database.to_string()),
                    is_materialized: engine.contains("Materialized"),
                    definition: row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string()),
                    owner: None,
                    comment: row.get(3).and_then(|v| v.as_str()).map(|s| s.to_string()),
                }
            })
            .collect())
    }

    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails> {
        let database = schema.unwrap_or(self.database());
        let tables = self.list_tables(Some(database)).await?;
        let info = tables
            .into_iter()
            .find(|t| t.name == name)
            .ok_or_else(|| ZqlzError::NotFound(format!("Table '{}' not found", name)))?;

        Ok(TableDetails {
            info,
            columns: self.get_columns(Some(database), name).await?,
            primary_key: self.get_primary_key(Some(database), name).await?,
            foreign_keys: Vec::new(), // ClickHouse has no foreign keys
            indexes: self.get_indexes(Some(database), name).await?,
            constraints: self.get_constraints(Some(database), name).await?,
            triggers: Vec::new(), // ClickHouse has no triggers
        })
    }

    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let database = schema.unwrap_or(self.database());
        let result = self
            .query(
                "SELECT name, position, type, default_kind, default_expression, comment, is_in_primary_key
                     FROM system.columns
                     WHERE database = ? AND table = ?
                     ORDER BY position",
                &[
                    Value::String(database.to_string()),
                    Value::String(table.to_string()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| {
                let data_type = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let nullable = data_type.starts_with("Nullable");
                let default_kind = row.get(3).and_then(|v| v.as_str()).unwrap_or("");
                let default_expr = row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string());
                let is_pk = row.get(6).and_then(|v| v.as_i64()).unwrap_or(0) == 1;

                ColumnInfo {
                    name: row
                        .get(0)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    ordinal: row.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize,
                    data_type,
                    nullable,
                    default_value: if default_kind.is_empty() {
                        None
                    } else {
                        default_expr
                    },
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: is_pk,
                    is_auto_increment: false, // ClickHouse doesn't have auto-increment
                    is_unique: false,
                    foreign_key: None,
                    comment: row.get(5).and_then(|v| v.as_str()).map(|s| s.to_string()),
                    ..Default::default()
                }
            })
            .collect())
    }

    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        let database = schema.unwrap_or(self.database());
        let result = self
            .query(
                "SELECT name, expr, type
                     FROM system.data_skipping_indices
                     WHERE database = ? AND table = ?",
                &[
                    Value::String(database.to_string()),
                    Value::String(table.to_string()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| IndexInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                columns: vec![], // Expression-based, not column-based
                is_unique: false,
                is_primary: false,
                index_type: row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                comment: row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .map(|s| format!("expr: {}", s)),
                ..Default::default()
            })
            .collect())
    }

    async fn get_foreign_keys(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        // ClickHouse does not support foreign keys
        Ok(Vec::new())
    }

    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        let database = schema.unwrap_or(self.database());
        // Get primary key columns from system.columns
        let result = self
            .query(
                &format!(
                    "SELECT name FROM system.columns
                     WHERE database = '{}' AND table = '{}' AND is_in_primary_key = 1
                     ORDER BY position",
                    database, table
                ),
                &[],
            )
            .await?;

        if result.rows.is_empty() {
            return Ok(None);
        }

        let columns: Vec<String> = result
            .rows
            .iter()
            .filter_map(|row| row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        Ok(Some(PrimaryKeyInfo {
            name: None,
            columns,
        }))
    }

    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        // ClickHouse has limited constraint support - mainly through ASSUME expressions
        let pk = self.get_primary_key(schema, table).await?;
        let mut constraints = Vec::new();

        if let Some(pk) = pk {
            constraints.push(ConstraintInfo {
                name: "PRIMARY KEY".to_string(),
                constraint_type: ConstraintType::PrimaryKey,
                columns: pk.columns,
                definition: None,
            });
        }

        Ok(constraints)
    }

    async fn list_functions(&self, _schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        // ClickHouse has built-in functions but user-defined functions are limited
        // List system functions from system.functions
        Ok(Vec::new())
    }

    async fn list_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        // ClickHouse does not support stored procedures
        Ok(Vec::new())
    }

    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        _table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        // ClickHouse does not support triggers
        Ok(Vec::new())
    }

    async fn list_sequences(&self, _schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        // ClickHouse does not have sequences
        Ok(Vec::new())
    }

    async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        // List Enum types from system.data_type_families
        let result = self
            .query(
                "SELECT name FROM system.data_type_families WHERE alias_to = '' AND name LIKE 'Enum%'",
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| TypeInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                schema: None,
                type_kind: TypeKind::Enum,
                values: None,
                definition: None,
                owner: None,
                comment: None,
            })
            .collect())
    }

    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        match object.object_type {
            ObjectType::Table | ObjectType::View => {
                let database = object.schema.as_deref().unwrap_or(self.database());
                let result = self
                    .query(
                        &format!("SHOW CREATE TABLE `{}`.`{}`", database, object.name),
                        &[],
                    )
                    .await?;

                if let Some(row) = result.rows.first()
                    && let Some(ddl) = row.get(0).and_then(|v| v.as_str())
                {
                    return Ok(ddl.to_string());
                }
                Err(ZqlzError::NotFound(format!(
                    "DDL not found for {}.{}",
                    database, object.name
                )))
            }
            ObjectType::Database => Ok(format!("CREATE DATABASE `{}`", object.name)),
            _ => Err(ZqlzError::NotImplemented(format!(
                "DDL generation not implemented for {:?}",
                object.object_type
            ))),
        }
    }

    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        // ClickHouse doesn't expose dependency information
        Ok(Vec::new())
    }
}

fn clickhouse_generic_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
    vec![
        ObjectsPanelColumn::new("name", "Name")
            .width(280.0)
            .min_width(120.0)
            .sortable(),
        ObjectsPanelColumn::new("database", "Database")
            .width(160.0)
            .min_width(90.0)
            .sortable(),
        ObjectsPanelColumn::new("engine", "Engine")
            .width(180.0)
            .min_width(100.0)
            .sortable(),
        ObjectsPanelColumn::new("rows", "Rows")
            .width(90.0)
            .min_width(60.0)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("bytes", "Bytes")
            .width(100.0)
            .min_width(70.0)
            .sortable()
            .text_right(),
    ]
}

fn clickhouse_row(
    kind_id: &str,
    name: String,
    database: Option<String>,
    values: std::collections::BTreeMap<String, String>,
) -> ObjectsPanelRow {
    ObjectsPanelRow {
        name: name.clone(),
        schema: database.clone(),
        object_type: kind_id.to_string(),
        object_ref: Some(ObjectsPanelObjectRef::new(kind_id, name).with_schema_option(database)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

fn clickhouse_database_objects_panel_data(
    databases: Vec<DatabaseInfo>,
) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(clickhouse_generic_objects_panel_columns());
    for database in databases {
        let mut values = std::collections::BTreeMap::new();
        values.insert("name".to_string(), database.name.clone());
        values.insert("database".to_string(), database.name.clone());
        values.insert(
            "engine".to_string(),
            database.comment.unwrap_or_else(|| "-".to_string()),
        );
        values.insert("rows".to_string(), "-".to_string());
        values.insert(
            "bytes".to_string(),
            database
                .size_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
        data.rows
            .push(clickhouse_row("database", database.name, None, values));
    }
    Ok(data)
}

fn clickhouse_table_objects_panel_data(tables: Vec<TableInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(clickhouse_generic_objects_panel_columns());
    for table in tables {
        let database = table.schema.clone();
        let mut values = std::collections::BTreeMap::new();
        values.insert("name".to_string(), table.name.clone());
        values.insert(
            "database".to_string(),
            database.clone().unwrap_or_else(|| "-".to_string()),
        );
        values.insert(
            "engine".to_string(),
            table.comment.unwrap_or_else(|| "-".to_string()),
        );
        values.insert(
            "rows".to_string(),
            table
                .row_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
        values.insert(
            "bytes".to_string(),
            table
                .size_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
        );
        data.rows
            .push(clickhouse_row("table", table.name, database, values));
    }
    Ok(data)
}

fn clickhouse_view_objects_panel_data(
    views: Vec<ViewInfo>,
    requested_kind_id: &str,
) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(clickhouse_generic_objects_panel_columns());
    for view in views {
        let kind_id = if view.is_materialized {
            "materialized_view"
        } else {
            "view"
        };
        if kind_id != requested_kind_id {
            continue;
        }
        let database = view.schema.clone();
        let mut values = std::collections::BTreeMap::new();
        values.insert("name".to_string(), view.name.clone());
        values.insert(
            "database".to_string(),
            database.clone().unwrap_or_else(|| "-".to_string()),
        );
        values.insert("engine".to_string(), kind_id.to_string());
        values.insert("rows".to_string(), "-".to_string());
        values.insert("bytes".to_string(), "-".to_string());
        data.rows
            .push(clickhouse_row(kind_id, view.name, database, values));
    }
    Ok(data)
}

fn clickhouse_type_objects_panel_data(types: Vec<TypeInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(clickhouse_generic_objects_panel_columns());
    for data_type in types {
        let mut values = std::collections::BTreeMap::new();
        values.insert("name".to_string(), data_type.name.clone());
        values.insert("database".to_string(), "-".to_string());
        values.insert("engine".to_string(), format!("{:?}", data_type.type_kind));
        values.insert("rows".to_string(), "-".to_string());
        values.insert("bytes".to_string(), "-".to_string());
        data.rows
            .push(clickhouse_row("type", data_type.name, None, values));
    }
    Ok(data)
}

pub(crate) fn clickhouse_objects_panel_manifest() -> ObjectsPanelManifest {
    let columns = clickhouse_generic_objects_panel_columns();
    let open_actions = |kind_id: &str| {
        vec![
            ObjectsPanelAction::new("open", "Open").group("open"),
            ObjectsPanelAction::new("design", "Design")
                .group("open")
                .single_selection(),
            ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                .group("clipboard"),
            ObjectsPanelAction::new("delete", "Drop")
                .group("danger")
                .destructive()
                .object_form(kind_id, ObjectFormMode::Drop),
            ObjectsPanelAction::new("refresh", "Refresh").group("system"),
        ]
    };
    let metadata_actions = vec![
        ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name").group("clipboard"),
        ObjectsPanelAction::new("refresh", "Refresh").group("system"),
    ];

    ObjectsPanelManifest {
        object_kinds: vec![
            ObjectsPanelObjectKind::new("database", "Database", "Databases")
                .icon_key("database")
                .columns(columns.clone())
                .row_actions(open_actions("database"))
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("table", "Table", "Tables")
                .icon_key("table")
                .columns(columns.clone())
                .row_actions(open_actions("table"))
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("view", "View", "Views")
                .icon_key("view")
                .columns(columns.clone())
                .row_actions(open_actions("view"))
                .default_row_action("open"),
            ObjectsPanelObjectKind::new(
                "materialized_view",
                "Materialized View",
                "Materialized Views",
            )
            .icon_key("view")
            .columns(columns.clone())
            .row_actions(open_actions("materialized_view"))
            .default_row_action("open"),
            ObjectsPanelObjectKind::new("type", "Type", "Types")
                .icon_key("type")
                .columns(columns)
                .row_actions(metadata_actions),
        ],
        toolbar_actions: vec![
            ObjectsPanelAction::new("refresh", "Refresh")
                .icon_key("refresh")
                .refreshes_objects_panel(),
            ObjectsPanelAction::new("new_database", "New Database")
                .icon_key("create")
                .create_object_kind("database")
                .object_form("database", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_table", "New Table")
                .icon_key("create")
                .create_object_kind("table")
                .object_form("table", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_view", "New View")
                .icon_key("create")
                .create_object_kind("view")
                .object_form("view", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_materialized_view", "New Materialized View")
                .icon_key("create")
                .create_object_kind("materialized_view")
                .object_form("materialized_view", ObjectFormMode::Create),
        ],
    }
}

fn clickhouse_object_form_spec(request: &ObjectFormSpecRequest) -> Option<ObjectFormSpec> {
    let title = match request.mode {
        ObjectFormMode::Create => format!("Create ClickHouse {}", request.kind_id),
        ObjectFormMode::Edit => format!("Edit ClickHouse {}", request.kind_id),
        ObjectFormMode::Drop => format!("Drop ClickHouse {}", request.kind_id),
    };
    let database = request
        .object_ref
        .as_ref()
        .and_then(|object_ref| object_ref.schema.clone())
        .unwrap_or_else(|| "default".to_string());
    let name = request
        .object_ref
        .as_ref()
        .map(|object_ref| object_ref.name.clone())
        .unwrap_or_default();

    let fields = match (request.kind_id.as_str(), request.mode) {
        ("database", ObjectFormMode::Create) => vec![
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
            ObjectFormField::new("engine", "Engine", ObjectFormFieldKind::Select)
                .default_value(zqlz_core::ObjectFormValue::String("Atomic".to_string()))
                .options(vec![
                    ObjectFormOption {
                        value: "Atomic".to_string(),
                        label: "Atomic".to_string(),
                    },
                    ObjectFormOption {
                        value: "Ordinary".to_string(),
                        label: "Ordinary".to_string(),
                    },
                ]),
        ],
        ("table", ObjectFormMode::Create) => vec![
            ObjectFormField::new("database", "Database", ObjectFormFieldKind::Text)
                .default_value(zqlz_core::ObjectFormValue::String(database)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
            ObjectFormField::new("columns", "Columns", ObjectFormFieldKind::TextArea)
                .default_value(zqlz_core::ObjectFormValue::String(
                    "id UInt64,\ncreated_at DateTime".to_string(),
                ))
                .required(),
            ObjectFormField::new("engine", "Engine", ObjectFormFieldKind::Text)
                .default_value(zqlz_core::ObjectFormValue::String("MergeTree".to_string()))
                .required(),
            ObjectFormField::new("order_by", "ORDER BY", ObjectFormFieldKind::SqlExpression)
                .default_value(zqlz_core::ObjectFormValue::String("id".to_string()))
                .required(),
            ObjectFormField::new(
                "partition_by",
                "PARTITION BY",
                ObjectFormFieldKind::SqlExpression,
            ),
        ],
        ("view" | "materialized_view", ObjectFormMode::Create) => vec![
            ObjectFormField::new("database", "Database", ObjectFormFieldKind::Text)
                .default_value(zqlz_core::ObjectFormValue::String(database)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
            ObjectFormField::new("query", "Query", ObjectFormFieldKind::SqlExpression)
                .default_value(zqlz_core::ObjectFormValue::String(
                    "SELECT 1 AS value".to_string(),
                ))
                .required(),
        ],
        (_, ObjectFormMode::Drop) => vec![
            ObjectFormField::new("database", "Database", ObjectFormFieldKind::Text)
                .default_value(zqlz_core::ObjectFormValue::String(database))
                .read_only(),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                .default_value(zqlz_core::ObjectFormValue::String(name))
                .read_only(),
            ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox).required(),
        ],
        _ => return None,
    };

    Some(
        ObjectFormSpec::new(&request.kind_id, request.mode, title)
            .sections(vec![ObjectFormSection::new(fields)]),
    )
}

fn clickhouse_form_string(
    values: &std::collections::BTreeMap<String, zqlz_core::ObjectFormValue>,
    key: &str,
) -> String {
    values
        .get(key)
        .and_then(|value| value.as_string())
        .unwrap_or("")
        .trim()
        .to_string()
}

pub(crate) fn clickhouse_quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn clickhouse_qualified_name(database: &str, name: &str) -> String {
    format!(
        "{}.{}",
        clickhouse_quote_identifier(database),
        clickhouse_quote_identifier(name)
    )
}

pub(crate) fn clickhouse_object_form_ddl(request: &ObjectFormDdlRequest) -> Result<Vec<String>> {
    let name = clickhouse_form_string(&request.values, "name");
    let database = clickhouse_form_string(&request.values, "database");
    let database = if database.is_empty() {
        "default"
    } else {
        &database
    };

    match (request.kind_id.as_str(), request.mode) {
        ("database", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Database name is required".to_string()));
            }
            let engine = clickhouse_form_string(&request.values, "engine");
            let engine = if engine.is_empty() { "Atomic" } else { &engine };
            Ok(vec![format!(
                "CREATE DATABASE {} ENGINE = {}",
                clickhouse_quote_identifier(&name),
                engine
            )])
        }
        ("table", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Table name is required".to_string()));
            }
            let columns = clickhouse_form_string(&request.values, "columns");
            if columns.is_empty() {
                return Err(ZqlzError::Driver("Columns are required".to_string()));
            }
            let engine = clickhouse_form_string(&request.values, "engine");
            let engine = if engine.is_empty() {
                "MergeTree"
            } else {
                &engine
            };
            let order_by = clickhouse_form_string(&request.values, "order_by");
            if order_by.is_empty() {
                return Err(ZqlzError::Driver("ORDER BY is required".to_string()));
            }
            let partition_by = clickhouse_form_string(&request.values, "partition_by");
            let partition_clause = if partition_by.is_empty() {
                String::new()
            } else {
                format!("\nPARTITION BY {partition_by}")
            };
            Ok(vec![format!(
                "CREATE TABLE {} (\n{}\n)\nENGINE = {}\nORDER BY {}{}",
                clickhouse_qualified_name(database, &name),
                columns,
                engine,
                order_by,
                partition_clause
            )])
        }
        ("view", ObjectFormMode::Create) | ("materialized_view", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("View name is required".to_string()));
            }
            let query = clickhouse_form_string(&request.values, "query");
            if query.is_empty() {
                return Err(ZqlzError::Driver("Query is required".to_string()));
            }
            let materialized = if request.kind_id == "materialized_view" {
                "MATERIALIZED "
            } else {
                ""
            };
            Ok(vec![format!(
                "CREATE {materialized}VIEW {} AS {}",
                clickhouse_qualified_name(database, &name),
                query
            )])
        }
        (_, ObjectFormMode::Drop) => {
            let confirmed = request
                .values
                .get("confirm")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            if !confirmed {
                return Err(ZqlzError::Driver(
                    "Drop confirmation is required".to_string(),
                ));
            }
            let object_name = if name.is_empty() {
                request
                    .object_ref
                    .as_ref()
                    .map(|object_ref| object_ref.name.as_str())
                    .unwrap_or("")
            } else {
                &name
            };
            let object_database = request
                .object_ref
                .as_ref()
                .and_then(|object_ref| object_ref.schema.as_deref())
                .unwrap_or(database);
            if object_name.is_empty() {
                return Err(ZqlzError::Driver("Object name is required".to_string()));
            }
            let statement = match request.kind_id.as_str() {
                "database" => format!("DROP DATABASE {}", clickhouse_quote_identifier(object_name)),
                "table" => format!(
                    "DROP TABLE {}",
                    clickhouse_qualified_name(object_database, object_name)
                ),
                "view" | "materialized_view" => {
                    format!(
                        "DROP VIEW {}",
                        clickhouse_qualified_name(object_database, object_name)
                    )
                }
                _ => {
                    return Err(ZqlzError::NotSupported(format!(
                        "Dropping ClickHouse {} is not supported",
                        request.kind_id
                    )));
                }
            };
            Ok(vec![statement])
        }
        _ => Err(ZqlzError::NotSupported(format!(
            "ClickHouse object form is not supported for {} {:?}",
            request.kind_id, request.mode
        ))),
    }
}
