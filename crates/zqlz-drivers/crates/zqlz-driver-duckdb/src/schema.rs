//! DuckDB schema introspection implementation

use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, ConstraintType, DatabaseInfo, DatabaseObject,
    Dependency, ForeignKeyAction, ForeignKeyInfo, FunctionInfo, IndexInfo, ObjectFormDdlRequest,
    ObjectFormField, ObjectFormFieldKind, ObjectFormMode, ObjectFormSection, ObjectFormSpec,
    ObjectFormSpecRequest, ObjectFormValue, ObjectType, ObjectsPanelAction, ObjectsPanelColumn,
    ObjectsPanelData, ObjectsPanelManifest, ObjectsPanelObjectKind, ObjectsPanelObjectRef,
    ObjectsPanelRow, PrimaryKeyInfo, ProcedureInfo, Result, SchemaInfo, SchemaIntrospection,
    SequenceInfo, TableDetails, TableInfo, TableType, TriggerInfo, TypeInfo, TypeKind, Value,
    ViewInfo, ZqlzError,
};

use super::DuckDbConnection;

#[async_trait]
impl SchemaIntrospection for DuckDbConnection {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .query("SELECT database_name, path FROM duckdb_databases()", &[])
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
                comment: row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string()),
            })
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let result = self
            .query(
                "SELECT schema_name FROM information_schema.schemata 
                 WHERE catalog_name = current_database() ORDER BY schema_name",
                &[],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| SchemaInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                owner: None,
                comment: None,
            })
            .collect())
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                "SELECT table_name, estimated_size, column_count 
                     FROM duckdb_tables() WHERE schema_name = ?",
                &[Value::String(schema.to_string())],
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
                schema: Some(schema.to_string()),
                table_type: TableType::Table,
                owner: None,
                row_count: row.get(1).and_then(|v| v.as_i64()),
                size_bytes: None,
                comment: None,
                index_count: None,
                trigger_count: None,
                key_value_info: None,
            })
            .collect())
    }

    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                "SELECT view_name, sql FROM duckdb_views() WHERE schema_name = ?",
                &[Value::String(schema.to_string())],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| ViewInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                schema: Some(schema.to_string()),
                is_materialized: false,
                definition: row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string()),
                owner: None,
                comment: None,
            })
            .collect())
    }

    async fn list_objects_panel_data_for_kind(
        &self,
        schema: Option<&str>,
        kind_id: &str,
    ) -> Result<ObjectsPanelData> {
        match kind_id {
            "schema" => duckdb_schema_objects_panel_data(self.list_schemas().await?),
            "table" => duckdb_table_objects_panel_data(self.list_tables(schema).await?),
            "view" => duckdb_view_objects_panel_data(self.list_views(schema).await?),
            "sequence" => duckdb_sequence_objects_panel_data(self.list_sequences(schema).await?),
            "type" => duckdb_type_objects_panel_data(self.list_types(schema).await?),
            _ => Ok(ObjectsPanelData::new(duckdb_objects_panel_columns())),
        }
    }

    async fn list_objects_panel_manifest(
        &self,
        _schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        Ok(duckdb_objects_panel_manifest())
    }

    async fn object_form_spec(
        &self,
        request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        Ok(duckdb_object_form_spec(request))
    }

    async fn generate_object_form_ddl(
        &self,
        request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        duckdb_object_form_ddl(request)
    }

    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails> {
        let schema = schema.unwrap_or("main");
        let tables = self.list_tables(Some(schema)).await?;
        let info = tables
            .into_iter()
            .find(|t| t.name == name)
            .ok_or_else(|| ZqlzError::NotFound(format!("Table '{}' not found", name)))?;

        Ok(TableDetails {
            info,
            columns: self.get_columns(Some(schema), name).await?,
            primary_key: self.get_primary_key(Some(schema), name).await?,
            foreign_keys: self.get_foreign_keys(Some(schema), name).await?,
            indexes: self.get_indexes(Some(schema), name).await?,
            constraints: self.get_constraints(Some(schema), name).await?,
            triggers: Vec::new(), // DuckDB has no triggers
        })
    }

    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                "SELECT column_name, column_index, data_type, is_nullable, column_default
                     FROM duckdb_columns() 
                     WHERE schema_name = ? AND table_name = ?
                     ORDER BY column_index",
                &[
                    Value::String(schema.to_string()),
                    Value::String(table.to_string()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| ColumnInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                ordinal: row.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize,
                data_type: row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                nullable: row.get(3).and_then(|v| v.as_bool()).unwrap_or(true),
                default_value: row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string()),
                max_length: None,
                precision: None,
                scale: None,
                is_primary_key: false, // Set via constraints
                is_auto_increment: false,
                is_unique: false,
                foreign_key: None,
                comment: None,
                ..Default::default()
            })
            .collect())
    }

    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                "SELECT index_name, is_unique, is_primary, sql
                     FROM duckdb_indexes() 
                     WHERE schema_name = ? AND table_name = ?",
                &[
                    Value::String(schema.to_string()),
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
                columns: Vec::new(), // DuckDB doesn't expose index columns easily
                is_unique: row.get(1).and_then(|v| v.as_bool()).unwrap_or(false),
                is_primary: row.get(2).and_then(|v| v.as_bool()).unwrap_or(false),
                index_type: "ART".to_string(), // DuckDB uses ART indexes
                comment: None,
                ..Default::default()
            })
            .collect())
    }

    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                "SELECT 
                        tc.constraint_name,
                        kcu.column_name,
                        ccu.table_name AS ref_table,
                        ccu.table_schema AS ref_schema,
                        ccu.column_name AS ref_column
                     FROM information_schema.table_constraints tc
                     JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                     JOIN information_schema.constraint_column_usage ccu
                        ON tc.constraint_name = ccu.constraint_name
                     WHERE tc.constraint_type = 'FOREIGN KEY' 
                        AND tc.table_schema = ? AND tc.table_name = ?",
                &[
                    Value::String(schema.to_string()),
                    Value::String(table.to_string()),
                ],
            )
            .await?;

        let mut fk_map: std::collections::HashMap<String, ForeignKeyInfo> =
            std::collections::HashMap::new();

        for row in result.rows.iter() {
            let name = row
                .get(0)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let column = row
                .get(1)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ref_table = row
                .get(2)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let ref_schema = row.get(3).and_then(|v| v.as_str()).map(|s| s.to_string());
            let ref_column = row
                .get(4)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            fk_map
                .entry(name.clone())
                .and_modify(|fk| {
                    fk.columns.push(column.clone());
                    fk.referenced_columns.push(ref_column.clone());
                })
                .or_insert(ForeignKeyInfo {
                    name,
                    columns: vec![column],
                    referenced_table: ref_table,
                    referenced_schema: ref_schema,
                    referenced_columns: vec![ref_column],
                    on_update: ForeignKeyAction::NoAction,
                    on_delete: ForeignKeyAction::NoAction,
                    is_deferrable: false,
                    initially_deferred: false,
                });
        }

        Ok(fk_map.into_values().collect())
    }

    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                "SELECT constraint_name, column_name
                     FROM information_schema.key_column_usage kcu
                     JOIN information_schema.table_constraints tc 
                        ON kcu.constraint_name = tc.constraint_name
                     WHERE tc.constraint_type = 'PRIMARY KEY' 
                        AND tc.table_schema = ? AND tc.table_name = ?
                     ORDER BY kcu.ordinal_position",
                &[
                    Value::String(schema.to_string()),
                    Value::String(table.to_string()),
                ],
            )
            .await?;

        if result.rows.is_empty() {
            return Ok(None);
        }

        let name = result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let columns: Vec<String> = result
            .rows
            .iter()
            .filter_map(|row| row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        Ok(Some(PrimaryKeyInfo { name, columns }))
    }

    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                "SELECT constraint_name, constraint_type
                     FROM information_schema.table_constraints
                     WHERE table_schema = ? AND table_name = ?",
                &[
                    Value::String(schema.to_string()),
                    Value::String(table.to_string()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let constraint_type = match row.get(1).and_then(|v| v.as_str()).unwrap_or("") {
                    "PRIMARY KEY" => ConstraintType::PrimaryKey,
                    "FOREIGN KEY" => ConstraintType::ForeignKey,
                    "UNIQUE" => ConstraintType::Unique,
                    "CHECK" => ConstraintType::Check,
                    _ => ConstraintType::Check,
                };
                ConstraintInfo {
                    name,
                    constraint_type,
                    columns: Vec::new(),
                    definition: None,
                }
            })
            .collect())
    }

    async fn list_functions(&self, _schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        // DuckDB has built-in functions but no user-defined functions in the traditional sense
        Ok(Vec::new())
    }

    async fn list_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        // DuckDB does not support stored procedures
        Ok(Vec::new())
    }

    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        _table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        // DuckDB does not support triggers
        Ok(Vec::new())
    }

    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                "SELECT sequence_name, start_value, min_value, max_value, increment_by
                     FROM duckdb_sequences() WHERE schema_name = ?",
                &[Value::String(schema.to_string())],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .map(|row| SequenceInfo {
                name: row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                schema: Some(schema.to_string()),
                data_type: "BIGINT".to_string(),
                start_value: row.get(1).and_then(|v| v.as_i64()).unwrap_or(1),
                min_value: row.get(2).and_then(|v| v.as_i64()).unwrap_or(1),
                max_value: row.get(3).and_then(|v| v.as_i64()).unwrap_or(i64::MAX),
                increment_by: row.get(4).and_then(|v| v.as_i64()).unwrap_or(1),
                current_value: None,
                owner: None,
                comment: None,
            })
            .collect())
    }

    async fn list_types(&self, schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        let schema = schema.unwrap_or("main");
        let result = self
            .query(
                "SELECT type_name, type_category FROM duckdb_types() 
                     WHERE schema_name = ? AND type_category = 'ENUM'",
                &[Value::String(schema.to_string())],
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
                schema: Some(schema.to_string()),
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
            ObjectType::Table => {
                let schema = object.schema.as_deref().unwrap_or("main");
                let table = self.get_table(Some(schema), &object.name).await?;
                Ok(generate_table_ddl(&table, schema))
            }
            ObjectType::View => {
                let schema = object.schema.as_deref().unwrap_or("main");
                let views = self.list_views(Some(schema)).await?;
                let view = views
                    .into_iter()
                    .find(|v| v.name == object.name)
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!("View '{}' not found", object.name))
                    })?;
                Ok(view.definition.unwrap_or_else(|| {
                    format!(
                        "-- View definition not available for {}.{}",
                        schema, object.name
                    )
                }))
            }
            _ => Err(ZqlzError::NotImplemented(format!(
                "DDL generation not implemented for {:?}",
                object.object_type
            ))),
        }
    }

    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        // DuckDB doesn't expose dependency information easily
        Ok(Vec::new())
    }
}

fn duckdb_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
    vec![
        ObjectsPanelColumn::new("name", "Name")
            .width(280.0)
            .min_width(120.0)
            .sortable(),
        ObjectsPanelColumn::new("schema", "Schema")
            .width(160.0)
            .min_width(90.0)
            .sortable(),
        ObjectsPanelColumn::new("type", "Type")
            .width(150.0)
            .min_width(90.0)
            .sortable(),
        ObjectsPanelColumn::new("rows", "Rows")
            .width(90.0)
            .min_width(60.0)
            .sortable()
            .text_right(),
    ]
}

fn duckdb_row(
    kind_id: &str,
    name: String,
    schema: Option<String>,
    object_type_label: String,
    rows: Option<i64>,
) -> ObjectsPanelRow {
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), name.clone());
    values.insert(
        "schema".to_string(),
        schema.clone().unwrap_or_else(|| "-".to_string()),
    );
    values.insert("type".to_string(), object_type_label);
    values.insert(
        "rows".to_string(),
        rows.map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
    );

    ObjectsPanelRow {
        name: name.clone(),
        schema: schema.clone(),
        object_type: kind_id.to_string(),
        object_ref: Some(ObjectsPanelObjectRef::new(kind_id, name).with_schema_option(schema)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

fn duckdb_schema_objects_panel_data(schemas: Vec<SchemaInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(duckdb_objects_panel_columns());
    for schema in schemas {
        data.rows.push(duckdb_row(
            "schema",
            schema.name,
            None,
            "Schema".to_string(),
            None,
        ));
    }
    Ok(data)
}

fn duckdb_table_objects_panel_data(tables: Vec<TableInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(duckdb_objects_panel_columns());
    for table in tables {
        data.rows.push(duckdb_row(
            "table",
            table.name,
            table.schema,
            "Table".to_string(),
            table.row_count,
        ));
    }
    Ok(data)
}

fn duckdb_view_objects_panel_data(views: Vec<ViewInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(duckdb_objects_panel_columns());
    for view in views {
        data.rows.push(duckdb_row(
            "view",
            view.name,
            view.schema,
            "View".to_string(),
            None,
        ));
    }
    Ok(data)
}

fn duckdb_sequence_objects_panel_data(sequences: Vec<SequenceInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(duckdb_objects_panel_columns());
    for sequence in sequences {
        data.rows.push(duckdb_row(
            "sequence",
            sequence.name,
            sequence.schema,
            "Sequence".to_string(),
            sequence.current_value,
        ));
    }
    Ok(data)
}

fn duckdb_type_objects_panel_data(types: Vec<TypeInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(duckdb_objects_panel_columns());
    for data_type in types {
        data.rows.push(duckdb_row(
            "type",
            data_type.name,
            data_type.schema,
            format!("{:?}", data_type.type_kind),
            None,
        ));
    }
    Ok(data)
}

pub(crate) fn duckdb_objects_panel_manifest() -> ObjectsPanelManifest {
    let columns = duckdb_objects_panel_columns();
    let row_actions = |kind_id: &str| {
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
    ObjectsPanelManifest {
        object_kinds: vec![
            ObjectsPanelObjectKind::new("schema", "Schema", "Schemas")
                .icon_key("schema")
                .columns(columns.clone())
                .row_actions(row_actions("schema")),
            ObjectsPanelObjectKind::new("table", "Table", "Tables")
                .icon_key("table")
                .columns(columns.clone())
                .row_actions(row_actions("table"))
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("view", "View", "Views")
                .icon_key("view")
                .columns(columns.clone())
                .row_actions(row_actions("view"))
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("sequence", "Sequence", "Sequences")
                .icon_key("sequence")
                .columns(columns.clone())
                .row_actions(row_actions("sequence")),
            ObjectsPanelObjectKind::new("type", "Type", "Types")
                .icon_key("type")
                .columns(columns)
                .row_actions(row_actions("type")),
        ],
        toolbar_actions: vec![
            ObjectsPanelAction::new("refresh", "Refresh")
                .icon_key("refresh")
                .refreshes_objects_panel(),
            ObjectsPanelAction::new("new_schema", "New Schema")
                .icon_key("create")
                .create_object_kind("schema")
                .object_form("schema", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_table", "New Table")
                .icon_key("create")
                .create_object_kind("table")
                .object_form("table", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_view", "New View")
                .icon_key("create")
                .create_object_kind("view")
                .object_form("view", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_sequence", "New Sequence")
                .icon_key("create")
                .create_object_kind("sequence")
                .object_form("sequence", ObjectFormMode::Create),
        ],
    }
}

fn duckdb_object_form_spec(request: &ObjectFormSpecRequest) -> Option<ObjectFormSpec> {
    let schema = request
        .object_ref
        .as_ref()
        .and_then(|object_ref| object_ref.schema.clone())
        .unwrap_or_else(|| "main".to_string());
    let name = request
        .object_ref
        .as_ref()
        .map(|object_ref| object_ref.name.clone())
        .unwrap_or_default();
    let fields = match (request.kind_id.as_str(), request.mode) {
        ("schema", ObjectFormMode::Create) => {
            vec![ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required()]
        }
        ("table", ObjectFormMode::Create) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
            ObjectFormField::new("columns", "Columns", ObjectFormFieldKind::TextArea)
                .default_value(ObjectFormValue::String(
                    "id BIGINT PRIMARY KEY,\ncreated_at TIMESTAMP".to_string(),
                ))
                .required(),
        ],
        ("view", ObjectFormMode::Create) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
            ObjectFormField::new("query", "Query", ObjectFormFieldKind::SqlExpression)
                .default_value(ObjectFormValue::String("SELECT 1 AS value".to_string()))
                .required(),
        ],
        ("sequence", ObjectFormMode::Create) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
        ],
        (_, ObjectFormMode::Drop) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema))
                .read_only(),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(name))
                .read_only(),
            ObjectFormField::new("confirm", "Confirm", ObjectFormFieldKind::Checkbox).required(),
        ],
        _ => return None,
    };
    Some(
        ObjectFormSpec::new(&request.kind_id, request.mode, "DuckDB Object")
            .sections(vec![ObjectFormSection::new(fields)]),
    )
}

fn duckdb_form_string(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    key: &str,
) -> String {
    values
        .get(key)
        .and_then(|value| value.as_string())
        .unwrap_or("")
        .trim()
        .to_string()
}

pub(crate) fn duckdb_quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn duckdb_qualified_name(schema: &str, name: &str) -> String {
    format!(
        "{}.{}",
        duckdb_quote_identifier(schema),
        duckdb_quote_identifier(name)
    )
}

pub(crate) fn duckdb_object_form_ddl(request: &ObjectFormDdlRequest) -> Result<Vec<String>> {
    let name = duckdb_form_string(&request.values, "name");
    let schema = duckdb_form_string(&request.values, "schema");
    let schema = if schema.is_empty() { "main" } else { &schema };
    match (request.kind_id.as_str(), request.mode) {
        ("schema", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Schema name is required".to_string()));
            }
            Ok(vec![format!(
                "CREATE SCHEMA {}",
                duckdb_quote_identifier(&name)
            )])
        }
        ("table", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Table name is required".to_string()));
            }
            let columns = duckdb_form_string(&request.values, "columns");
            if columns.is_empty() {
                return Err(ZqlzError::Driver("Columns are required".to_string()));
            }
            Ok(vec![format!(
                "CREATE TABLE {} (\n{}\n)",
                duckdb_qualified_name(schema, &name),
                columns
            )])
        }
        ("view", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("View name is required".to_string()));
            }
            let query = duckdb_form_string(&request.values, "query");
            if query.is_empty() {
                return Err(ZqlzError::Driver("Query is required".to_string()));
            }
            Ok(vec![format!(
                "CREATE VIEW {} AS {}",
                duckdb_qualified_name(schema, &name),
                query
            )])
        }
        ("sequence", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Sequence name is required".to_string()));
            }
            Ok(vec![format!(
                "CREATE SEQUENCE {}",
                duckdb_qualified_name(schema, &name)
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
            let object_schema = request
                .object_ref
                .as_ref()
                .and_then(|object_ref| object_ref.schema.as_deref())
                .unwrap_or(schema);
            if object_name.is_empty() {
                return Err(ZqlzError::Driver("Object name is required".to_string()));
            }
            let kind = match request.kind_id.as_str() {
                "schema" => {
                    return Ok(vec![format!(
                        "DROP SCHEMA {}",
                        duckdb_quote_identifier(object_name)
                    )]);
                }
                "table" => "TABLE",
                "view" => "VIEW",
                "sequence" => "SEQUENCE",
                _ => {
                    return Err(ZqlzError::NotSupported(format!(
                        "Dropping DuckDB {} is not supported",
                        request.kind_id
                    )));
                }
            };
            Ok(vec![format!(
                "DROP {} {}",
                kind,
                duckdb_qualified_name(object_schema, object_name)
            )])
        }
        _ => Err(ZqlzError::NotSupported(format!(
            "DuckDB object form is not supported for {} {:?}",
            request.kind_id, request.mode
        ))),
    }
}

fn generate_table_ddl(table: &TableDetails, schema: &str) -> String {
    let mut ddl = format!("CREATE TABLE \"{}\".\"{}\" (\n", schema, table.info.name);

    for (i, col) in table.columns.iter().enumerate() {
        let comma = if i < table.columns.len() - 1 || table.primary_key.is_some() {
            ","
        } else {
            ""
        };
        let null_str = if col.nullable { "" } else { " NOT NULL" };
        let default_str = col
            .default_value
            .as_ref()
            .map(|d| format!(" DEFAULT {}", d))
            .unwrap_or_default();

        ddl.push_str(&format!(
            "    \"{}\" {}{}{}{}\n",
            col.name, col.data_type, null_str, default_str, comma
        ));
    }

    if let Some(pk) = &table.primary_key {
        let pk_cols = pk
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");
        ddl.push_str(&format!("    PRIMARY KEY ({})\n", pk_cols));
    }

    ddl.push_str(");\n");
    ddl
}
