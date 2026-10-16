use zqlz_core::{
    ObjectFormDdlRequest, ObjectFormField, ObjectFormFieldKind, ObjectFormMode, ObjectFormSection,
    ObjectFormSpec, ObjectFormSpecRequest, ObjectFormValue, Result, TableDetails, ZqlzError,
};

pub(crate) fn mssql_object_form_spec(request: &ObjectFormSpecRequest) -> Option<ObjectFormSpec> {
    let schema = request
        .object_ref
        .as_ref()
        .and_then(|object_ref| object_ref.schema.clone())
        .unwrap_or_else(|| "dbo".to_string());
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
                    "[Id] int IDENTITY(1,1) NOT NULL,\n[Name] nvarchar(255) NOT NULL".to_string(),
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
        ("procedure" | "function" | "trigger", ObjectFormMode::Create) => vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .default_value(ObjectFormValue::String(schema)),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text).required(),
            ObjectFormField::new("body", "Body", ObjectFormFieldKind::TextArea).required(),
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
        ObjectFormSpec::new(&request.kind_id, request.mode, "SQL Server Object")
            .sections(vec![ObjectFormSection::new(fields)]),
    )
}

fn mssql_form_string(
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

pub(crate) fn mssql_quote_identifier(identifier: &str) -> String {
    format!("[{}]", identifier.replace(']', "]]"))
}

fn mssql_qualified_name(schema: &str, name: &str) -> String {
    format!(
        "{}.{}",
        mssql_quote_identifier(schema),
        mssql_quote_identifier(name)
    )
}

pub(crate) fn mssql_object_form_ddl(request: &ObjectFormDdlRequest) -> Result<Vec<String>> {
    let name = mssql_form_string(&request.values, "name");
    let schema = mssql_form_string(&request.values, "schema");
    let schema = if schema.is_empty() { "dbo" } else { &schema };
    match (request.kind_id.as_str(), request.mode) {
        ("schema", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Schema name is required".to_string()));
            }
            Ok(vec![format!(
                "CREATE SCHEMA {}",
                mssql_quote_identifier(&name)
            )])
        }
        ("table", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("Table name is required".to_string()));
            }
            let columns = mssql_form_string(&request.values, "columns");
            if columns.is_empty() {
                return Err(ZqlzError::Driver("Columns are required".to_string()));
            }
            Ok(vec![format!(
                "CREATE TABLE {} (\n{}\n)",
                mssql_qualified_name(schema, &name),
                columns
            )])
        }
        ("view", ObjectFormMode::Create) => {
            if name.is_empty() {
                return Err(ZqlzError::Driver("View name is required".to_string()));
            }
            let query = mssql_form_string(&request.values, "query");
            if query.is_empty() {
                return Err(ZqlzError::Driver("Query is required".to_string()));
            }
            Ok(vec![format!(
                "CREATE VIEW {} AS {}",
                mssql_qualified_name(schema, &name),
                query
            )])
        }
        ("procedure", ObjectFormMode::Create)
        | ("function", ObjectFormMode::Create)
        | ("trigger", ObjectFormMode::Create) => {
            let body = mssql_form_string(&request.values, "body");
            if body.is_empty() {
                return Err(ZqlzError::Driver("Body is required".to_string()));
            }
            Ok(vec![body])
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
                        mssql_quote_identifier(object_name)
                    )]);
                }
                "table" => "TABLE",
                "view" => "VIEW",
                "procedure" => "PROCEDURE",
                "function" => "FUNCTION",
                "trigger" => "TRIGGER",
                "sequence" => "SEQUENCE",
                _ => {
                    return Err(ZqlzError::NotSupported(format!(
                        "Dropping SQL Server {} is not supported",
                        request.kind_id
                    )));
                }
            };
            Ok(vec![format!(
                "DROP {} {}",
                kind,
                mssql_qualified_name(object_schema, object_name)
            )])
        }
        _ => Err(ZqlzError::NotSupported(format!(
            "SQL Server object form is not supported for {} {:?}",
            request.kind_id, request.mode
        ))),
    }
}

pub(crate) fn generate_ddl_not_found_message(
    object_kind: &str,
    schema: &str,
    object_name: &str,
) -> String {
    format!("{} '[{}].[{}]' not found", object_kind, schema, object_name)
}

pub(crate) fn generate_ddl_definition_unavailable_message(
    object_kind: &str,
    schema: &str,
    object_name: &str,
) -> String {
    format!(
        "DDL definition for {} '[{}].[{}]' is unavailable",
        object_kind, schema, object_name
    )
}

/// Parse SQL Server foreign key action

/// Generate CREATE TABLE DDL from TableDetails
pub(crate) fn generate_table_ddl(table: &TableDetails, schema: &str) -> String {
    let mut ddl = format!("CREATE TABLE [{}].[{}] (\n", schema, table.info.name);

    // Add columns
    for (i, col) in table.columns.iter().enumerate() {
        let comma = if i < table.columns.len() - 1
            || table.primary_key.is_some()
            || !table.constraints.is_empty()
        {
            ","
        } else {
            ""
        };

        let null_str = if col.nullable { "NULL" } else { "NOT NULL" };
        let identity_str = if col.is_auto_increment {
            " IDENTITY(1,1)"
        } else {
            ""
        };
        let default_str = col
            .default_value
            .as_ref()
            .map(|d| format!(" DEFAULT {}", d))
            .unwrap_or_default();

        ddl.push_str(&format!(
            "    [{}] {}{}{} {}{}\n",
            col.name, col.data_type, identity_str, default_str, null_str, comma
        ));
    }

    // Add primary key constraint
    if let Some(pk) = &table.primary_key {
        let pk_name = pk.name.as_deref().unwrap_or("PK_unnamed");
        let pk_cols = pk
            .columns
            .iter()
            .map(|c| format!("[{}]", c))
            .collect::<Vec<_>>()
            .join(", ");
        let comma = if !table.constraints.is_empty() {
            ","
        } else {
            ""
        };
        ddl.push_str(&format!(
            "    CONSTRAINT [{}] PRIMARY KEY ({}){}\n",
            pk_name, pk_cols, comma
        ));
    }

    ddl.push_str(");\n");
    ddl
}
