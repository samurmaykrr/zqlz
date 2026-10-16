//! Objects-panel column/action definitions, row builders, object-form DDL
//! rendering, and the objects-panel manifest.
use super::*;

pub(crate) fn mysql_object_form_string<'a>(
    values: &'a std::collections::BTreeMap<String, ObjectFormValue>,
    id: &str,
) -> &'a str {
    values
        .get(id)
        .and_then(ObjectFormValue::as_string)
        .unwrap_or_default()
        .trim()
}

pub(crate) fn mysql_object_form_bool(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    id: &str,
) -> bool {
    values
        .get(id)
        .and_then(ObjectFormValue::as_bool)
        .unwrap_or(false)
}

pub(crate) fn mysql_object_form_string_list(
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
    id: &str,
) -> Vec<String> {
    values
        .get(id)
        .and_then(ObjectFormValue::as_string_list)
        .map(|values| {
            values
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn mysql_function_form_sql(
    schema: Option<&str>,
    name: &str,
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
) -> Result<String> {
    let function_return_type = mysql_object_form_string(values, "returns");
    let body = mysql_object_form_string(values, "body");
    if function_return_type.is_empty() {
        return Err(ZqlzError::Schema(
            "Function return type is required".to_string(),
        ));
    }
    if body.is_empty() {
        return Err(ZqlzError::Schema("Function body is required".to_string()));
    }

    let parameters = mysql_object_form_string_list(values, "parameters");
    let mut sql = format!(
        "CREATE FUNCTION {}({})\nRETURNS {}",
        mysql_qualified_name(schema, name),
        parameters.join(", "),
        function_return_type
    );
    if mysql_object_form_bool(values, "deterministic") {
        sql.push_str("\nDETERMINISTIC");
    }
    let sql_data_access = mysql_object_form_string(values, "sql_data_access");
    if !sql_data_access.is_empty() {
        sql.push_str(&format!("\n{}", sql_data_access));
    }
    sql.push_str(&format!("\n{}", body));
    Ok(format!("{};", sql.trim_end_matches(';')))
}

pub(crate) fn mysql_procedure_form_sql(
    schema: Option<&str>,
    name: &str,
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
) -> Result<String> {
    let body = mysql_object_form_string(values, "body");
    if body.is_empty() {
        return Err(ZqlzError::Schema("Procedure body is required".to_string()));
    }

    let parameters = mysql_object_form_string_list(values, "parameters");
    let sql = format!(
        "CREATE PROCEDURE {}({})\n{}",
        mysql_qualified_name(schema, name),
        parameters.join(", "),
        body
    );
    Ok(format!("{};", sql.trim_end_matches(';')))
}

pub(crate) fn mysql_trigger_form_sql(
    schema: Option<&str>,
    name: &str,
    values: &std::collections::BTreeMap<String, ObjectFormValue>,
) -> Result<String> {
    let table = mysql_object_form_string(values, "table");
    let body = mysql_object_form_string(values, "body");
    if table.is_empty() {
        return Err(ZqlzError::Schema("Trigger table is required".to_string()));
    }
    if body.is_empty() {
        return Err(ZqlzError::Schema("Trigger body is required".to_string()));
    }

    let timing = mysql_object_form_string(values, "timing");
    let timing = if timing.is_empty() { "BEFORE" } else { timing };
    let event = mysql_object_form_string(values, "event");
    let event = if event.is_empty() { "INSERT" } else { event };
    let sql = format!(
        "CREATE TRIGGER {}\n{} {} ON {}\nFOR EACH ROW\n{}",
        mysql_qualified_name(schema, name),
        timing,
        event,
        mysql_qualified_name(schema, table),
        body
    );
    Ok(format!("{};", sql.trim_end_matches(';')))
}

/// Key parts as written in an index definition: `a`, `name(10)`, `(expr) DESC`.
///
/// Prefix lengths arrive as `"name(10)"` entries in `include_columns`. A
/// functional key part is stored as its parenthesised expression instead of a
/// column name and must not be identifier-quoted.
pub(crate) fn mysql_index_key_parts(
    columns: &[String],
    descending: &[bool],
    include_columns: &[String],
    quote: bool,
) -> Vec<String> {
    columns
        .iter()
        .enumerate()
        .map(|(position, column)| {
            let is_expression = column.starts_with('(');
            let mut text = if quote && !is_expression {
                mysql_quote_identifier(column)
            } else {
                column.clone()
            };
            let prefix = include_columns.iter().find_map(|entry| {
                entry
                    .strip_prefix(column.as_str())
                    .and_then(|rest| rest.strip_prefix('('))
                    .and_then(|rest| rest.strip_suffix(')'))
            });
            if let Some(prefix) = prefix {
                text.push_str(&format!("({prefix})"));
            }
            if descending.get(position).copied().unwrap_or(false) {
                text.push_str(" DESC");
            }
            text
        })
        .collect()
}

pub(crate) fn mysql_index_ddl(table: &str, index: &IndexInfo) -> String {
    let columns = mysql_index_key_parts(
        &index.columns,
        &index.column_descending,
        &index.include_columns,
        true,
    )
    .join(", ");
    if index.is_primary {
        return format!(
            "ALTER TABLE {} ADD PRIMARY KEY ({});",
            mysql_quote_identifier(table),
            columns
        );
    }
    let index_type = index.index_type.to_ascii_uppercase();
    let create_keyword = if index_type == "FULLTEXT" || index_type == "SPATIAL" {
        format!("CREATE {index_type} INDEX")
    } else if index.is_unique {
        "CREATE UNIQUE INDEX".to_string()
    } else {
        "CREATE INDEX".to_string()
    };
    // `USING` is only legal after the key parts (or before `ON`), never between
    // the table name and the key parts.
    let using_clause = if matches!(index_type.as_str(), "BTREE" | "HASH") {
        format!(" USING {index_type}")
    } else {
        String::new()
    };
    let comment_clause = index
        .comment
        .as_ref()
        .filter(|comment| !comment.is_empty())
        .map(|comment| format!(" COMMENT {}", mysql_quote_literal(comment)))
        .unwrap_or_default();

    format!(
        "{} {} ON {} ({}){}{};",
        create_keyword,
        mysql_quote_identifier(&index.name),
        mysql_quote_identifier(table),
        columns,
        using_clause,
        comment_clause
    )
}

pub(crate) fn mysql_constraint_ddl(table: &str, constraint: &ConstraintInfo) -> Result<String> {
    match constraint.constraint_type {
        ConstraintType::Unique => Ok(format!(
            "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({});",
            mysql_quote_identifier(table),
            mysql_quote_identifier(&constraint.name),
            constraint
                .columns
                .iter()
                .map(|column| mysql_quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        ConstraintType::Check => {
            let Some(definition) = constraint.definition.as_deref() else {
                return Err(ZqlzError::Query(format!(
                    "Could not reconstruct CHECK constraint '{}' without definition",
                    constraint.name
                )));
            };
            Ok(format!(
                "ALTER TABLE {} ADD CONSTRAINT {} CHECK ({});",
                mysql_quote_identifier(table),
                mysql_quote_identifier(&constraint.name),
                definition
            ))
        }
        ConstraintType::PrimaryKey => Ok(format!(
            "ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({});",
            mysql_quote_identifier(table),
            mysql_quote_identifier(&constraint.name),
            constraint
                .columns
                .iter()
                .map(|column| mysql_quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        ConstraintType::ForeignKey | ConstraintType::Exclusion => {
            Err(ZqlzError::NotImplemented(format!(
                "DDL generation for {:?} constraints is not supported in MySQL",
                constraint.constraint_type
            )))
        }
    }
}

pub(crate) fn mysql_foreign_key_action_clause(prefix: &str, action: ForeignKeyAction) -> &'static str {
    match (prefix, action) {
        ("ON UPDATE", ForeignKeyAction::Cascade) => " ON UPDATE CASCADE",
        ("ON UPDATE", ForeignKeyAction::SetNull) => " ON UPDATE SET NULL",
        ("ON UPDATE", ForeignKeyAction::SetDefault) => " ON UPDATE SET DEFAULT",
        ("ON UPDATE", ForeignKeyAction::Restrict) => " ON UPDATE RESTRICT",
        ("ON DELETE", ForeignKeyAction::Cascade) => " ON DELETE CASCADE",
        ("ON DELETE", ForeignKeyAction::SetNull) => " ON DELETE SET NULL",
        ("ON DELETE", ForeignKeyAction::SetDefault) => " ON DELETE SET DEFAULT",
        ("ON DELETE", ForeignKeyAction::Restrict) => " ON DELETE RESTRICT",
        _ => "",
    }
}

pub(crate) fn mysql_foreign_key_ddl(table: &str, foreign_key: &ForeignKeyInfo) -> String {
    let referenced_table = match foreign_key.referenced_schema.as_deref() {
        Some(schema) if !schema.is_empty() => {
            format!(
                "{}.{}",
                mysql_quote_identifier(schema),
                mysql_quote_identifier(&foreign_key.referenced_table)
            )
        }
        _ => mysql_quote_identifier(&foreign_key.referenced_table),
    };

    format!(
        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}){}{};",
        mysql_quote_identifier(table),
        mysql_quote_identifier(&foreign_key.name),
        foreign_key
            .columns
            .iter()
            .map(|column| mysql_quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", "),
        referenced_table,
        foreign_key
            .referenced_columns
            .iter()
            .map(|column| mysql_quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", "),
        mysql_foreign_key_action_clause("ON UPDATE", foreign_key.on_update),
        mysql_foreign_key_action_clause("ON DELETE", foreign_key.on_delete)
    )
}

pub(crate) fn mysql_metadata_kind_degrades_on_error(kind_id: &str) -> bool {
    matches!(
        kind_id,
        "tablespace"
            | "user"
            | "role"
            | "grant"
            | "plugin"
            | "variable"
            | "status_variable"
            | "process"
            | "replica_status"
    )
}

pub(crate) fn mysql_metadata_unavailable_row(kind_id: &str, error: &ZqlzError) -> ObjectsPanelRow {
    mysql_metadata_row(
        kind_id,
        "Metadata unavailable".to_string(),
        None,
        None,
        "Privilege or catalog error",
        error.to_string(),
        "Unavailable",
        "Grant access to the admin catalog or hide this metadata kind.",
    )
}

/// Keep the MySQL objects-panel column set in one place so the manifest and
/// row loader stay aligned when metadata changes.
pub(crate) fn mysql_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
    vec![
        ObjectsPanelColumn::new("name", "Name")
            .width(250.0)
            .min_width(120.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("row_count", "Rows")
            .width(100.0)
            .min_width(50.0)
            .resizable(true)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("data_length", "Data Length")
            .width(100.0)
            .min_width(60.0)
            .resizable(true)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("engine", "Engine")
            .width(80.0)
            .min_width(50.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("row_format", "Row Format")
            .width(100.0)
            .min_width(60.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("auto_increment", "Auto Increment")
            .width(120.0)
            .min_width(70.0)
            .resizable(true)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("created_date", "Created Date")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("modified_date", "Modified Date")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("collation", "Collation")
            .width(140.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("comment", "Comment")
            .width(200.0)
            .min_width(80.0)
            .resizable(true),
        ObjectsPanelColumn::new("create_options", "Create Options")
            .width(180.0)
            .min_width(80.0)
            .resizable(true),
    ]
}

/// Tables expose the MySQL parity action palette directly from metadata so we
/// do not fall back to driver-agnostic defaults in the panel layer.
pub(crate) fn mysql_table_row_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("open", "Open").group("open"),
        ObjectsPanelAction::new("design", "Design")
            .group("open")
            .single_selection(),
        ObjectsPanelAction::new("rename", "Rename")
            .group("modify")
            .single_selection(),
        ObjectsPanelAction::new("duplicate", "Duplicate").group("modify"),
        ObjectsPanelAction::new("empty", "Empty")
            .group("danger")
            .destructive(),
        ObjectsPanelAction::new("import", "Import")
            .group("data")
            .single_selection(),
        ObjectsPanelAction::new("export", "Export").group("data"),
        ObjectsPanelAction::new("dump_sql_structure_data", "Dump SQL (Structure + Data)")
            .group("data"),
        ObjectsPanelAction::new("dump_sql_structure", "Dump SQL (Structure Only)").group("data"),
        ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name").group("clipboard"),
        ObjectsPanelAction::new("view_history", "View History")
            .group("metadata")
            .single_selection(),
        ObjectsPanelAction::new("delete", "Delete")
            .group("danger")
            .destructive(),
        ObjectsPanelAction::new("refresh", "Refresh").group("system"),
    ]
}

/// Views keep the same explicit metadata-driven contract as tables, but with
/// the smaller action set that MySQL actually supports for views.
pub(crate) fn mysql_view_row_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("open", "Open").group("open"),
        ObjectsPanelAction::new("design", "Design")
            .group("open")
            .single_selection(),
        ObjectsPanelAction::new("rename", "Rename")
            .group("modify")
            .single_selection(),
        ObjectsPanelAction::new("duplicate", "Duplicate").group("modify"),
        ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name").group("clipboard"),
        ObjectsPanelAction::new("view_history", "View History")
            .group("metadata")
            .single_selection(),
        ObjectsPanelAction::new("export", "Export").group("data"),
        ObjectsPanelAction::new("delete", "Delete")
            .group("danger")
            .destructive(),
        ObjectsPanelAction::new("refresh", "Refresh").group("system"),
    ]
}

/// The toolbar remains explicit so unsupported actions are omitted by contract
/// instead of being filtered later by UI branching.
pub(crate) fn mysql_toolbar_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("refresh", "Refresh")
            .icon_key("refresh")
            .refreshes_objects_panel(),
        ObjectsPanelAction::new("new_database", "New Database")
            .icon_key("create")
            .create_object_kind("database")
            .object_form("database", ObjectFormMode::Create),
        ObjectsPanelAction::new("new_table", "New Table")
            .icon_key("create")
            .create_object_kind("table"),
        ObjectsPanelAction::new("new_view", "New View")
            .icon_key("create")
            .create_object_kind("view"),
        ObjectsPanelAction::new("new_function", "New Function")
            .icon_key("create")
            .create_object_kind("function")
            .object_form("function", ObjectFormMode::Create),
        ObjectsPanelAction::new("new_procedure", "New Procedure")
            .icon_key("create")
            .create_object_kind("procedure")
            .object_form("procedure", ObjectFormMode::Create),
        ObjectsPanelAction::new("new_event", "New Event")
            .icon_key("create")
            .create_object_kind("event")
            .object_form("event", ObjectFormMode::Create),
        ObjectsPanelAction::new("import", "Import Wizard...").icon_key("import"),
        ObjectsPanelAction::new("export", "Export Wizard...").icon_key("export"),
    ]
}

/// Keep MySQL routine kinds aligned on the same identity columns so their
/// manifests only need to describe kind-specific details.
pub(crate) fn mysql_named_object_columns(extra_columns: Vec<ObjectsPanelColumn>) -> Vec<ObjectsPanelColumn> {
    let mut columns = vec![
        ObjectsPanelColumn::new("name", "Name")
            .width(250.0)
            .min_width(120.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("schema_name", "Schema")
            .width(140.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
    ];

    columns.extend(extra_columns);
    columns
}

pub(crate) fn mysql_function_columns() -> Vec<ObjectsPanelColumn> {
    mysql_named_object_columns(vec![
        ObjectsPanelColumn::new("return_type", "Return Type")
            .width(140.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("parameters", "Parameters")
            .width(220.0)
            .min_width(100.0)
            .resizable(true),
        ObjectsPanelColumn::new("deterministic", "Deterministic")
            .width(120.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("sql_data_access", "SQL Data Access")
            .width(150.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("security_type", "Security")
            .width(110.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("definer", "Definer")
            .width(170.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("created", "Created")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("modified", "Modified")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("sql_mode", "SQL Mode")
            .width(180.0)
            .min_width(100.0)
            .resizable(true),
        ObjectsPanelColumn::new("definition", "Definition")
            .width(240.0)
            .min_width(120.0)
            .resizable(true),
        ObjectsPanelColumn::new("comment", "Comment")
            .width(200.0)
            .min_width(80.0)
            .resizable(true),
    ])
}

pub(crate) fn mysql_procedure_columns() -> Vec<ObjectsPanelColumn> {
    mysql_named_object_columns(vec![
        ObjectsPanelColumn::new("parameters", "Parameters")
            .width(220.0)
            .min_width(100.0)
            .resizable(true),
        ObjectsPanelColumn::new("sql_data_access", "SQL Data Access")
            .width(150.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("security_type", "Security")
            .width(110.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("definer", "Definer")
            .width(170.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("created", "Created")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("modified", "Modified")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("sql_mode", "SQL Mode")
            .width(180.0)
            .min_width(100.0)
            .resizable(true),
        ObjectsPanelColumn::new("definition", "Definition")
            .width(240.0)
            .min_width(120.0)
            .resizable(true),
        ObjectsPanelColumn::new("comment", "Comment")
            .width(200.0)
            .min_width(80.0)
            .resizable(true),
    ])
}

pub(crate) fn mysql_trigger_columns() -> Vec<ObjectsPanelColumn> {
    mysql_named_object_columns(vec![
        ObjectsPanelColumn::new("table_name", "Table")
            .width(180.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("timing", "Timing")
            .width(100.0)
            .min_width(70.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("event", "Event")
            .width(100.0)
            .min_width(70.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("action_order", "Order")
            .width(80.0)
            .min_width(60.0)
            .resizable(true)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("definer", "Definer")
            .width(170.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("created", "Created")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("sql_mode", "SQL Mode")
            .width(180.0)
            .min_width(100.0)
            .resizable(true),
        ObjectsPanelColumn::new("definition", "Definition")
            .width(200.0)
            .min_width(80.0)
            .resizable(true),
        ObjectsPanelColumn::new("character_set_client", "Client Charset")
            .width(130.0)
            .min_width(90.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("collation_connection", "Connection Collation")
            .width(170.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
    ])
}

pub(crate) fn mysql_database_columns() -> Vec<ObjectsPanelColumn> {
    vec![
        ObjectsPanelColumn::new("name", "Name")
            .width(250.0)
            .min_width(120.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("default_charset", "Charset")
            .width(120.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("default_collation", "Collation")
            .width(180.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("table_count", "Tables")
            .width(90.0)
            .min_width(60.0)
            .resizable(true)
            .sortable()
            .text_right(),
        ObjectsPanelColumn::new("size", "Size")
            .width(100.0)
            .min_width(70.0)
            .resizable(true)
            .sortable()
            .text_right(),
    ]
}

pub(crate) fn mysql_event_columns() -> Vec<ObjectsPanelColumn> {
    mysql_named_object_columns(vec![
        ObjectsPanelColumn::new("status", "Status")
            .width(100.0)
            .min_width(70.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("schedule", "Schedule")
            .width(220.0)
            .min_width(120.0)
            .resizable(true),
        ObjectsPanelColumn::new("definer", "Definer")
            .width(180.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("created", "Created")
            .width(160.0)
            .min_width(100.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("comment", "Comment")
            .width(200.0)
            .min_width(80.0)
            .resizable(true),
    ])
}

pub(crate) fn mysql_metadata_columns() -> Vec<ObjectsPanelColumn> {
    mysql_named_object_columns(vec![
        ObjectsPanelColumn::new("table_name", "Table")
            .width(160.0)
            .min_width(90.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("kind", "Kind")
            .width(130.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("detail", "Detail")
            .width(260.0)
            .min_width(120.0)
            .resizable(true),
        ObjectsPanelColumn::new("state", "State")
            .width(120.0)
            .min_width(80.0)
            .resizable(true)
            .sortable(),
        ObjectsPanelColumn::new("comment", "Comment")
            .width(220.0)
            .min_width(100.0)
            .resizable(true),
    ])
}

/// MySQL routines and triggers share the same compact action palette so the
/// manifest stays metadata-driven across all routine kinds.
pub(crate) fn mysql_routine_row_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("open", "Open").group("open"),
        ObjectsPanelAction::new("design", "Design")
            .group("open")
            .single_selection(),
        ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name").group("clipboard"),
        ObjectsPanelAction::new("view_history", "View History")
            .group("metadata")
            .single_selection(),
        ObjectsPanelAction::new("refresh", "Refresh").group("system"),
    ]
}

pub(crate) fn mysql_metadata_row_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("open", "Open").group("open"),
        ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name").group("clipboard"),
        ObjectsPanelAction::new("refresh", "Refresh").group("system"),
    ]
}

pub(crate) fn mysql_event_row_actions() -> Vec<ObjectsPanelAction> {
    vec![
        ObjectsPanelAction::new("open", "Open").group("open"),
        ObjectsPanelAction::new("design", "Design")
            .group("open")
            .single_selection(),
        ObjectsPanelAction::new("delete", "Drop")
            .group("danger")
            .single_selection()
            .destructive(),
        ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
        ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name").group("clipboard"),
        ObjectsPanelAction::new("view_history", "View History")
            .group("metadata")
            .single_selection(),
        ObjectsPanelAction::new("refresh", "Refresh").group("system"),
    ]
}

pub(crate) fn mysql_display_name(
    qualify_with_schema: bool,
    schema_name: Option<&str>,
    object_name: &str,
) -> String {
    if qualify_with_schema {
        schema_name
            .map(|schema_name| format!("{schema_name}.{object_name}"))
            .unwrap_or_else(|| object_name.to_string())
    } else {
        object_name.to_string()
    }
}

pub(crate) fn mysql_routine_object_ref(
    kind_id: &str,
    name: String,
    schema_name: Option<String>,
) -> ObjectsPanelObjectRef {
    ObjectsPanelObjectRef::new(kind_id, name).with_schema_option(schema_name)
}

pub(crate) fn mysql_display_value(row: &zqlz_core::Row, index: usize) -> String {
    row.get(index)
        .map(ToString::to_string)
        .filter(|value| value != "NULL" && !value.is_empty())
        .unwrap_or_else(|| "-".to_string())
}

pub(crate) fn mysql_routine_parameters_label(parameters: &[ParameterInfo]) -> String {
    if parameters.is_empty() {
        return "-".to_string();
    }

    parameters
        .iter()
        .map(|parameter| {
            let mode = match parameter.mode {
                ParameterMode::In => "IN",
                ParameterMode::Out => "OUT",
                ParameterMode::InOut => "INOUT",
                ParameterMode::Variadic => "VARIADIC",
            };
            match parameter.name.as_deref() {
                Some(name) => format!("{mode} {name} {}", parameter.data_type),
                None => format!("{mode} {}", parameter.data_type),
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn mysql_routine_panel_row(
    kind_id: &str,
    row: &zqlz_core::Row,
    parameters: Vec<ParameterInfo>,
    qualify_with_schema: bool,
) -> ObjectsPanelRow {
    let schema = row
        .get(0)
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let name = row
        .get(1)
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let return_type = row.get(2).and_then(|value| value.as_str()).unwrap_or("-");
    let definition = row
        .get(3)
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("-");
    let comment = row
        .get(4)
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("-");
    let definer = row.get(5).and_then(|value| value.as_str()).unwrap_or("-");
    let security_type = row.get(6).and_then(|value| value.as_str()).unwrap_or("-");
    let deterministic = row.get(7).and_then(|value| value.as_str()).unwrap_or("-");
    let sql_data_access = row.get(8).and_then(|value| value.as_str()).unwrap_or("-");
    let sql_mode = row.get(9).and_then(|value| value.as_str()).unwrap_or("-");
    let created = mysql_display_value(row, 10);
    let modified = mysql_display_value(row, 11);

    let display_name = mysql_display_name(qualify_with_schema, schema.as_deref(), &name);
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("return_type".to_string(), return_type.to_string());
    values.insert(
        "parameters".to_string(),
        mysql_routine_parameters_label(&parameters),
    );
    values.insert("deterministic".to_string(), deterministic.to_string());
    values.insert("sql_data_access".to_string(), sql_data_access.to_string());
    values.insert("security_type".to_string(), security_type.to_string());
    values.insert("definer".to_string(), definer.to_string());
    values.insert("created".to_string(), created);
    values.insert("modified".to_string(), modified);
    values.insert("sql_mode".to_string(), sql_mode.to_string());
    values.insert("definition".to_string(), definition.to_string());
    values.insert("comment".to_string(), comment.to_string());

    ObjectsPanelRow {
        name: display_name,
        schema: schema.clone(),
        object_type: kind_id.to_string(),
        object_ref: Some(mysql_routine_object_ref(kind_id, name, schema)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

#[cfg(test)]
pub(crate) fn mysql_function_row(function: FunctionInfo, qualify_with_schema: bool) -> ObjectsPanelRow {
    let FunctionInfo {
        schema,
        name,
        return_type,
        definition,
        comment,
        ..
    } = function;

    let schema_name = schema.clone();
    let display_name = mysql_display_name(qualify_with_schema, schema_name.as_deref(), &name);
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema_name.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("return_type".to_string(), return_type);
    values.insert(
        "definition".to_string(),
        definition.unwrap_or_else(|| "-".to_string()),
    );
    values.insert(
        "comment".to_string(),
        comment.unwrap_or_else(|| "-".to_string()),
    );

    ObjectsPanelRow {
        name: display_name,
        schema,
        object_type: "function".to_string(),
        object_ref: Some(mysql_routine_object_ref("function", name, schema_name)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

#[cfg(test)]
pub(crate) fn mysql_procedure_row(procedure: ProcedureInfo, qualify_with_schema: bool) -> ObjectsPanelRow {
    let ProcedureInfo {
        schema,
        name,
        definition,
        comment,
        ..
    } = procedure;

    let schema_name = schema.clone();
    let display_name = mysql_display_name(qualify_with_schema, schema_name.as_deref(), &name);
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema_name.as_deref().unwrap_or("-").to_string(),
    );
    values.insert(
        "definition".to_string(),
        definition.unwrap_or_else(|| "-".to_string()),
    );
    values.insert(
        "comment".to_string(),
        comment.unwrap_or_else(|| "-".to_string()),
    );

    ObjectsPanelRow {
        name: display_name,
        schema,
        object_type: "procedure".to_string(),
        object_ref: Some(mysql_routine_object_ref("procedure", name, schema_name)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

#[cfg(test)]
pub(crate) fn mysql_trigger_timing_label(timing: TriggerTiming) -> &'static str {
    match timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    }
}

#[cfg(test)]
pub(crate) fn mysql_trigger_event_label(events: &[TriggerEvent]) -> String {
    let labels: Vec<&str> = events
        .iter()
        .map(|event| match event {
            TriggerEvent::Insert => "INSERT",
            TriggerEvent::Update => "UPDATE",
            TriggerEvent::Delete => "DELETE",
            TriggerEvent::Truncate => "TRUNCATE",
        })
        .collect();

    if labels.is_empty() {
        "-".to_string()
    } else {
        labels.join(", ")
    }
}

#[cfg(test)]
pub(crate) fn mysql_trigger_row(trigger: TriggerInfo, qualify_with_schema: bool) -> ObjectsPanelRow {
    let TriggerInfo {
        schema,
        name,
        table_name,
        timing,
        events,
        definition,
        comment,
        enabled: _,
        ..
    } = trigger;

    let schema_name = schema.clone();
    let display_name = mysql_display_name(qualify_with_schema, schema_name.as_deref(), &name);
    let associated_table = table_name.clone();
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema_name.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("table_name".to_string(), table_name);
    values.insert(
        "timing".to_string(),
        mysql_trigger_timing_label(timing).to_string(),
    );
    values.insert("event".to_string(), mysql_trigger_event_label(&events));
    values.insert(
        "definition".to_string(),
        definition.unwrap_or_else(|| "-".to_string()),
    );
    values.insert(
        "comment".to_string(),
        comment.unwrap_or_else(|| "-".to_string()),
    );

    ObjectsPanelRow {
        name: display_name,
        schema,
        object_type: "trigger".to_string(),
        object_ref: Some(
            mysql_routine_object_ref("trigger", name, schema_name)
                .with_signature_option(Some(associated_table)),
        ),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

pub(crate) fn mysql_trigger_panel_row(row: &zqlz_core::Row, qualify_with_schema: bool) -> ObjectsPanelRow {
    let schema = row
        .get(0)
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let name = row
        .get(1)
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let table_name = row
        .get(2)
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let display_name = mysql_display_name(qualify_with_schema, schema.as_deref(), &name);

    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("table_name".to_string(), table_name.clone());
    values.insert("timing".to_string(), mysql_display_value(row, 3));
    values.insert("event".to_string(), mysql_display_value(row, 4));
    values.insert("action_order".to_string(), mysql_display_value(row, 5));
    values.insert("definition".to_string(), mysql_display_value(row, 6));
    values.insert("orientation".to_string(), mysql_display_value(row, 7));
    values.insert("definer".to_string(), mysql_display_value(row, 8));
    values.insert("sql_mode".to_string(), mysql_display_value(row, 9));
    values.insert("created".to_string(), mysql_display_value(row, 10));
    values.insert(
        "character_set_client".to_string(),
        mysql_display_value(row, 11),
    );
    values.insert(
        "collation_connection".to_string(),
        mysql_display_value(row, 12),
    );
    values.insert(
        "database_collation".to_string(),
        mysql_display_value(row, 13),
    );

    ObjectsPanelRow {
        name: display_name,
        schema: schema.clone(),
        object_type: "trigger".to_string(),
        object_ref: Some(
            mysql_routine_object_ref("trigger", name, schema)
                .with_signature_option(Some(table_name)),
        ),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn mysql_metadata_row(
    kind_id: &str,
    name: String,
    schema: Option<String>,
    table_name: Option<String>,
    kind: impl Into<String>,
    detail: impl Into<String>,
    state: impl Into<String>,
    comment: impl Into<String>,
) -> ObjectsPanelRow {
    let display_name = mysql_display_name(true, schema.as_deref(), &name);
    let associated_table = table_name;
    let table_name = associated_table
        .as_deref()
        .filter(|table_name| !table_name.is_empty())
        .unwrap_or("-")
        .to_string();
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("table_name".to_string(), table_name);
    values.insert("kind".to_string(), kind.into());
    values.insert("detail".to_string(), detail.into());
    values.insert("state".to_string(), state.into());
    values.insert("comment".to_string(), comment.into());

    ObjectsPanelRow {
        name: display_name,
        schema: schema.clone(),
        object_type: kind_id.to_string(),
        object_ref: Some(
            ObjectsPanelObjectRef::new(kind_id, name)
                .with_schema_option(schema)
                .with_signature_option(associated_table),
        ),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

/// Override the schema-qualified name `mysql_metadata_row` derived.
pub(crate) fn mysql_set_display_name(row: &mut ObjectsPanelRow, name: String) {
    row.values.insert("name".to_string(), name.clone());
    row.name = name;
}

pub(crate) fn mysql_event_row(row: &zqlz_core::Row, qualify_with_schema: bool) -> ObjectsPanelRow {
    let schema = row
        .get(0)
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let name = row
        .get(1)
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let status = row.get(2).and_then(|value| value.as_str()).unwrap_or("-");
    let interval_value = row.get(3).and_then(|value| value.as_str());
    let interval_field = row.get(4).and_then(|value| value.as_str());
    let execute_at = row.get(5).map(ToString::to_string);
    let starts = row.get(6).map(ToString::to_string);
    let ends = row.get(7).map(ToString::to_string);
    let definer = row.get(8).and_then(|value| value.as_str()).unwrap_or("-");
    let created = row
        .get(9)
        .map(ToString::to_string)
        .filter(|value| value != "NULL")
        .unwrap_or_else(|| "-".to_string());
    let comment = row
        .get(10)
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("-");

    let schedule = match (interval_value, interval_field, execute_at) {
        (Some(value), Some(field), _) if !value.is_empty() && !field.is_empty() => {
            format!("EVERY {value} {field}")
        }
        (_, _, Some(value)) if value != "NULL" => format!("AT {value}"),
        _ => "-".to_string(),
    };
    let schedule = match (starts, ends) {
        (Some(starts), Some(ends)) if starts != "NULL" && ends != "NULL" => {
            format!("{schedule}; STARTS {starts}; ENDS {ends}")
        }
        (Some(starts), _) if starts != "NULL" => format!("{schedule}; STARTS {starts}"),
        (_, Some(ends)) if ends != "NULL" => format!("{schedule}; ENDS {ends}"),
        _ => schedule,
    };

    let display_name = mysql_display_name(qualify_with_schema, schema.as_deref(), &name);
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), display_name.clone());
    values.insert(
        "schema_name".to_string(),
        schema.as_deref().unwrap_or("-").to_string(),
    );
    values.insert("status".to_string(), status.to_string());
    values.insert("schedule".to_string(), schedule);
    values.insert("definer".to_string(), definer.to_string());
    values.insert("created".to_string(), created);
    values.insert("comment".to_string(), comment.to_string());

    ObjectsPanelRow {
        name: display_name,
        schema: schema.clone(),
        object_type: "event".to_string(),
        object_ref: Some(ObjectsPanelObjectRef::new("event", name).with_schema_option(schema)),
        values,
        redis_database_index: None,
        key_value_info: None,
    }
}

/// Build the MySQL manifest from driver-owned metadata so parity validation can
/// exercise a stable contract rather than an inferred fallback manifest.
pub(crate) fn mysql_objects_panel_manifest(features: &MysqlServerFeatures) -> ObjectsPanelManifest {
    let routine_actions = mysql_routine_row_actions();
    let metadata_columns = mysql_metadata_columns();
    let metadata_actions = mysql_metadata_row_actions();

    let mut object_kinds = vec![
        ObjectsPanelObjectKind::new("database", "Database", "Databases")
            .icon_key("database")
            .columns(mysql_database_columns())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("table", "Table", "Tables")
            .icon_key("table")
            .columns(mysql_objects_panel_columns())
            .row_actions(mysql_table_row_actions())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("view", "View", "Views")
            .icon_key("view")
            .columns(mysql_objects_panel_columns())
            .row_actions(mysql_view_row_actions())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("function", "Function", "Functions")
            .icon_key("function")
            .columns(mysql_function_columns())
            .row_actions(routine_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("procedure", "Procedure", "Procedures")
            .icon_key("procedure")
            .columns(mysql_procedure_columns())
            .row_actions(routine_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("trigger", "Trigger", "Triggers")
            .icon_key("trigger")
            .columns(mysql_trigger_columns())
            .row_actions(routine_actions)
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("event", "Event", "Events")
            .icon_key("event")
            .columns(mysql_event_columns())
            .row_actions(mysql_event_row_actions())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("index", "Index", "Indexes")
            .icon_key("index")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("constraint", "Constraint", "Constraints")
            .icon_key("constraint")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("foreign_key", "Foreign Key", "Foreign Keys")
            .icon_key("foreign_key")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("partition", "Partition", "Partitions")
            .icon_key("partition")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("tablespace", "Tablespace", "Tablespaces")
            .icon_key("tablespace")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("engine", "Engine", "Engines")
            .icon_key("engine")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("charset", "Charset", "Charsets")
            .icon_key("charset")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("collation", "Collation", "Collations")
            .icon_key("collation")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("user", "User", "Users")
            .icon_key("user")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("role", "Role", "Roles")
            .icon_key("role")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("grant", "Grant", "Grants")
            .icon_key("grant")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("plugin", "Plugin", "Plugins")
            .icon_key("plugin")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("variable", "Variable", "Variables")
            .icon_key("variable")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("status_variable", "Status Variable", "Status Variables")
            .icon_key("variable")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("process", "Process", "Processes")
            .icon_key("process")
            .columns(metadata_columns.clone())
            .row_actions(metadata_actions.clone())
            .default_row_action("open"),
        ObjectsPanelObjectKind::new("replica_status", "Replica Status", "Replica Status")
            .icon_key("replica")
            .columns(metadata_columns)
            .row_actions(metadata_actions)
            .default_row_action("open"),
    ];

    if features.supports_sequences() {
        object_kinds.insert(
            11,
            ObjectsPanelObjectKind::new("sequence", "Sequence", "Sequences")
                .icon_key("sequence")
                .columns(mysql_metadata_columns())
                .row_actions(mysql_metadata_row_actions())
                .default_row_action("open"),
        );
    }

    ObjectsPanelManifest {
        object_kinds,
        toolbar_actions: mysql_toolbar_actions(),
    }
}

