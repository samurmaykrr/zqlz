use zqlz_core::{
    DatabaseInfo, FunctionInfo, ObjectFormMode, ObjectsPanelAction, ObjectsPanelColumn,
    ObjectsPanelData, ObjectsPanelManifest, ObjectsPanelObjectKind, ObjectsPanelObjectRef,
    ObjectsPanelRow, ProcedureInfo, Result, SchemaInfo, SequenceInfo, TableInfo, TriggerInfo,
    TypeInfo, ViewInfo,
};

pub(crate) fn mssql_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
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
            .width(160.0)
            .min_width(90.0)
            .sortable(),
        ObjectsPanelColumn::new("rows", "Rows")
            .width(90.0)
            .min_width(60.0)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("size", "Size")
            .width(100.0)
            .min_width(70.0)
            .sortable()
            .text_right(),
    ]
}

fn mssql_row(
    kind_id: &str,
    name: String,
    schema: Option<String>,
    type_label: String,
    rows: Option<i64>,
    size: Option<i64>,
) -> ObjectsPanelRow {
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), name.clone());
    values.insert(
        "schema".to_string(),
        schema.clone().unwrap_or_else(|| "-".to_string()),
    );
    values.insert("type".to_string(), type_label);
    values.insert(
        "rows".to_string(),
        rows.map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
    );
    values.insert(
        "size".to_string(),
        size.map(|value| value.to_string())
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

pub(crate) fn mssql_database_objects_panel_data(databases: Vec<DatabaseInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for database in databases {
        data.rows.push(mssql_row(
            "database",
            database.name,
            None,
            "Database".to_string(),
            None,
            database.size_bytes,
        ));
    }
    Ok(data)
}

pub(crate) fn mssql_schema_objects_panel_data(schemas: Vec<SchemaInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for schema in schemas {
        data.rows.push(mssql_row(
            "schema",
            schema.name,
            None,
            "Schema".to_string(),
            None,
            None,
        ));
    }
    Ok(data)
}

pub(crate) fn mssql_table_objects_panel_data(tables: Vec<TableInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for table in tables {
        data.rows.push(mssql_row(
            "table",
            table.name,
            table.schema,
            "Table".to_string(),
            table.row_count,
            table.size_bytes,
        ));
    }
    Ok(data)
}

pub(crate) fn mssql_view_objects_panel_data(views: Vec<ViewInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for view in views {
        data.rows.push(mssql_row(
            "view",
            view.name,
            view.schema,
            "View".to_string(),
            None,
            None,
        ));
    }
    Ok(data)
}

pub(crate) fn mssql_function_objects_panel_data(functions: Vec<FunctionInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for function in functions {
        data.rows.push(mssql_row(
            "function",
            function.name,
            function.schema,
            "Function".to_string(),
            None,
            None,
        ));
    }
    Ok(data)
}

pub(crate) fn mssql_procedure_objects_panel_data(procedures: Vec<ProcedureInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for procedure in procedures {
        data.rows.push(mssql_row(
            "procedure",
            procedure.name,
            procedure.schema,
            "Procedure".to_string(),
            None,
            None,
        ));
    }
    Ok(data)
}

pub(crate) fn mssql_trigger_objects_panel_data(triggers: Vec<TriggerInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for trigger in triggers {
        data.rows.push(mssql_row(
            "trigger",
            trigger.name,
            trigger.schema,
            "Trigger".to_string(),
            None,
            None,
        ));
    }
    Ok(data)
}

pub(crate) fn mssql_sequence_objects_panel_data(sequences: Vec<SequenceInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for sequence in sequences {
        data.rows.push(mssql_row(
            "sequence",
            sequence.name,
            sequence.schema,
            "Sequence".to_string(),
            sequence.current_value,
            None,
        ));
    }
    Ok(data)
}

pub(crate) fn mssql_type_objects_panel_data(types: Vec<TypeInfo>) -> Result<ObjectsPanelData> {
    let mut data = ObjectsPanelData::new(mssql_objects_panel_columns());
    for data_type in types {
        data.rows.push(mssql_row(
            "type",
            data_type.name,
            data_type.schema,
            format!("{:?}", data_type.type_kind),
            None,
            None,
        ));
    }
    Ok(data)
}

pub(crate) fn mssql_objects_panel_manifest() -> ObjectsPanelManifest {
    let columns = mssql_objects_panel_columns();
    let row_actions = |kind_id: &str| {
        vec![
            ObjectsPanelAction::new("open", "Open").group("open"),
            ObjectsPanelAction::new("design", "Design")
                .group("open")
                .single_selection(),
            ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                .group("clipboard"),
            ObjectsPanelAction::new("view_history", "View History")
                .group("metadata")
                .single_selection(),
            ObjectsPanelAction::new("delete", "Drop")
                .group("danger")
                .destructive()
                .object_form(kind_id, ObjectFormMode::Drop),
            ObjectsPanelAction::new("refresh", "Refresh").group("system"),
        ]
    };
    let kinds = vec![
        ("database", "Database", "Databases", "database"),
        ("schema", "Schema", "Schemas", "schema"),
        ("table", "Table", "Tables", "table"),
        ("view", "View", "Views", "view"),
        ("function", "Function", "Functions", "function"),
        ("procedure", "Procedure", "Procedures", "procedure"),
        ("trigger", "Trigger", "Triggers", "trigger"),
        ("sequence", "Sequence", "Sequences", "sequence"),
        ("type", "Type", "Types", "type"),
    ]
    .into_iter()
    .map(|(id, singular, plural, icon)| {
        ObjectsPanelObjectKind::new(id, singular, plural)
            .icon_key(icon)
            .columns(columns.clone())
            .row_actions(row_actions(id))
            .default_row_action("open")
    })
    .collect();

    ObjectsPanelManifest {
        object_kinds: kinds,
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
            ObjectsPanelAction::new("new_procedure", "New Procedure")
                .icon_key("create")
                .create_object_kind("procedure")
                .object_form("procedure", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_function", "New Function")
                .icon_key("create")
                .create_object_kind("function")
                .object_form("function", ObjectFormMode::Create),
            ObjectsPanelAction::new("new_trigger", "New Trigger")
                .icon_key("create")
                .create_object_kind("trigger")
                .object_form("trigger", ObjectFormMode::Create),
        ],
    }
}
