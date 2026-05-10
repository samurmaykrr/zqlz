//! PostgreSQL schema introspection implementation

use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, ConstraintType, DatabaseInfo, DatabaseObject,
    Dependency, ForeignKeyAction, ForeignKeyInfo, FunctionInfo, IndexInfo, ObjectFormDdlRequest,
    ObjectFormField, ObjectFormFieldKind, ObjectFormMode, ObjectFormSection, ObjectFormSpec,
    ObjectFormSpecRequest, ObjectFormValue, ObjectType, ObjectsPanelAction, ObjectsPanelColumn,
    ObjectsPanelData, ObjectsPanelManifest, ObjectsPanelObjectKind, ObjectsPanelObjectRef,
    ObjectsPanelRow, PrimaryKeyInfo, ProcedureInfo, Result, SchemaInfo, SchemaIntrospection,
    SequenceInfo, TableDetails, TableInfo, TableType, TriggerEvent, TriggerForEach, TriggerInfo,
    TriggerTiming, TypeInfo, TypeKind, ViewInfo, ZqlzError,
};

use crate::PostgresConnection;

const POSTGRES_COLUMNS_SQL: &str = "SELECT
                    a.attname,
                    a.attnum,
                    pg_catalog.format_type(a.atttypid, a.atttypmod),
                    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END,
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid),
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    CASE WHEN a.attidentity <> '' THEN 'YES' ELSE 'NO' END,
                    (
                        SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)
                        FROM pg_catalog.pg_type t
                        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                        JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
                        WHERE t.oid = a.atttypid
                    )
                 FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_class cls ON cls.oid = a.attrelid
                 JOIN pg_catalog.pg_namespace ns ON ns.oid = cls.relnamespace
                 LEFT JOIN pg_catalog.pg_attrdef d
                    ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                 LEFT JOIN information_schema.columns c
                    ON c.table_schema = ns.nspname
                    AND c.table_name = cls.relname
                    AND c.column_name = a.attname
                 WHERE ns.nspname = $1
                    AND cls.relname = $2
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                 ORDER BY a.attnum";

impl PostgresConnection {
    fn postgres_type_kind_id(type_kind: TypeKind) -> &'static str {
        match type_kind {
            TypeKind::Enum => "enum",
            TypeKind::Domain => "domain",
            TypeKind::Range => "range",
            TypeKind::Composite => "composite_type",
            TypeKind::Base => "type",
        }
    }

    fn postgres_type_kind_label(type_kind: TypeKind) -> &'static str {
        match type_kind {
            TypeKind::Enum => "Enum",
            TypeKind::Domain => "Domain",
            TypeKind::Range => "Range",
            TypeKind::Composite => "Composite Type",
            TypeKind::Base => "Type",
        }
    }

    fn postgres_type_object_ref(
        type_kind: TypeKind,
        name: String,
        schema: String,
    ) -> ObjectsPanelObjectRef {
        ObjectsPanelObjectRef::new(Self::postgres_type_kind_id(type_kind), name)
            .with_schema_option(Some(schema))
    }

    fn quote_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    fn qualified_type_name(&self, schema: Option<&str>, name: &str) -> String {
        match schema.filter(|schema| !schema.trim().is_empty()) {
            Some(schema) => format!(
                "{}.{}",
                self.quote_identifier(schema),
                self.quote_identifier(name)
            ),
            None => self.quote_identifier(name),
        }
    }

    fn object_form_string<'a>(
        values: &'a std::collections::BTreeMap<String, ObjectFormValue>,
        id: &str,
    ) -> &'a str {
        values
            .get(id)
            .and_then(ObjectFormValue::as_string)
            .unwrap_or_default()
            .trim()
    }

    fn object_form_bool(
        values: &std::collections::BTreeMap<String, ObjectFormValue>,
        id: &str,
    ) -> bool {
        values
            .get(id)
            .and_then(ObjectFormValue::as_bool)
            .unwrap_or(false)
    }

    fn object_form_string_list(
        values: &std::collections::BTreeMap<String, ObjectFormValue>,
        id: &str,
    ) -> Vec<String> {
        values
            .get(id)
            .and_then(ObjectFormValue::as_string_list)
            .unwrap_or_default()
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect()
    }

    fn validate_unique_values(kind: &str, values: &[String]) -> Result<()> {
        let mut seen = std::collections::BTreeSet::new();
        for value in values {
            if !seen.insert(value.as_str()) {
                return Err(ZqlzError::Schema(format!(
                    "{} contains duplicate value '{}'",
                    kind, value
                )));
            }
        }
        Ok(())
    }

    fn enum_labels_to_append(labels: &[String], current_labels: &[String]) -> Vec<String> {
        if labels.starts_with(current_labels) {
            return labels.iter().skip(current_labels.len()).cloned().collect();
        }

        let current_labels: std::collections::BTreeSet<&str> =
            current_labels.iter().map(String::as_str).collect();
        labels
            .iter()
            .filter(|label| !current_labels.contains(label.as_str()))
            .cloned()
            .collect()
    }

    fn enum_form_spec(
        mode: ObjectFormMode,
        schema: Option<String>,
        name: Option<String>,
        labels: Vec<String>,
    ) -> ObjectFormSpec {
        let title = match mode {
            ObjectFormMode::Create => "New Enum",
            ObjectFormMode::Edit => "Design Enum",
            ObjectFormMode::Drop => "Drop Enum",
        };

        let name_field = ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
            .required()
            .default_value(ObjectFormValue::String(name.unwrap_or_default()));
        let name_field = if matches!(mode, ObjectFormMode::Create) {
            name_field
        } else {
            name_field.read_only()
        };

        ObjectFormSpec::new("enum", mode, title).sections(vec![ObjectFormSection::new(vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .placeholder("public")
                .default_value(ObjectFormValue::String(
                    schema.unwrap_or_else(|| "public".to_string()),
                )),
            name_field,
            ObjectFormField::new("labels", "Labels", ObjectFormFieldKind::StringList)
                .required()
                .help_text("One enum label per line")
                .default_value(ObjectFormValue::StringList(labels)),
            ObjectFormField::new("comment", "Comment", ObjectFormFieldKind::TextArea),
        ])])
    }

    fn domain_form_spec(
        mode: ObjectFormMode,
        schema: Option<String>,
        name: Option<String>,
        base_type: Option<String>,
        not_null: bool,
        default_value: Option<String>,
        check_expression: Option<String>,
    ) -> ObjectFormSpec {
        let title = match mode {
            ObjectFormMode::Create => "New Domain",
            ObjectFormMode::Edit => "Design Domain",
            ObjectFormMode::Drop => "Drop Domain",
        };

        let name_field = ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
            .required()
            .default_value(ObjectFormValue::String(name.unwrap_or_default()));
        let name_field = if matches!(mode, ObjectFormMode::Create) {
            name_field
        } else {
            name_field.read_only()
        };

        ObjectFormSpec::new("domain", mode, title).sections(vec![ObjectFormSection::new(vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .placeholder("public")
                .default_value(ObjectFormValue::String(
                    schema.unwrap_or_else(|| "public".to_string()),
                )),
            name_field,
            ObjectFormField::new("base_type", "Base Type", ObjectFormFieldKind::Text)
                .required()
                .placeholder("text")
                .default_value(ObjectFormValue::String(base_type.unwrap_or_default())),
            ObjectFormField::new("not_null", "Not Null", ObjectFormFieldKind::Checkbox)
                .default_value(ObjectFormValue::Bool(not_null)),
            ObjectFormField::new("default", "Default", ObjectFormFieldKind::SqlExpression)
                .default_value(ObjectFormValue::String(default_value.unwrap_or_default())),
            ObjectFormField::new("check", "Check", ObjectFormFieldKind::SqlExpression)
                .placeholder("VALUE <> ''")
                .default_value(ObjectFormValue::String(
                    check_expression.unwrap_or_default(),
                )),
            ObjectFormField::new("comment", "Comment", ObjectFormFieldKind::TextArea),
        ])])
    }

    fn procedure_form_spec(
        mode: ObjectFormMode,
        schema: Option<String>,
        name: Option<String>,
    ) -> ObjectFormSpec {
        let title = match mode {
            ObjectFormMode::Create => "New Procedure",
            ObjectFormMode::Edit => "Design Procedure",
            ObjectFormMode::Drop => "Drop Procedure",
        };

        ObjectFormSpec::new("procedure", mode, title).sections(vec![ObjectFormSection::new(vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .placeholder("public")
                .default_value(ObjectFormValue::String(
                    schema.unwrap_or_else(|| "public".to_string()),
                )),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                .required()
                .default_value(ObjectFormValue::String(name.unwrap_or_default())),
            ObjectFormField::new("arguments", "Arguments", ObjectFormFieldKind::StringList)
                .help_text("One argument per line, e.g. user_id integer"),
            ObjectFormField::new("language", "Language", ObjectFormFieldKind::Text)
                .required()
                .placeholder("plpgsql")
                .default_value(ObjectFormValue::String("plpgsql".to_string())),
            ObjectFormField::new("body", "Body", ObjectFormFieldKind::SqlExpression)
                .required()
                .default_value(ObjectFormValue::String(
                    "BEGIN\n    -- procedure body\nEND".to_string(),
                )),
            ObjectFormField::new("comment", "Comment", ObjectFormFieldKind::TextArea),
        ])])
    }

    fn postgres_table_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
        vec![
            ObjectsPanelColumn::new("name", "Name")
                .width(250.0)
                .min_width(120.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("oid", "OID")
                .width(80.0)
                .min_width(50.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("owner", "Owner")
                .width(110.0)
                .min_width(70.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("acl", "ACL")
                .width(150.0)
                .min_width(80.0)
                .resizable(true),
            ObjectsPanelColumn::new("table_type", "Table Type")
                .width(120.0)
                .min_width(70.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("partition_of", "Partition Of")
                .width(120.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("row_count", "Rows")
                .width(80.0)
                .min_width(50.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("primary_key", "Primary Key")
                .width(140.0)
                .min_width(80.0)
                .resizable(true),
            ObjectsPanelColumn::new("has_oids", "Has OIDs")
                .width(90.0)
                .min_width(60.0)
                .resizable(true),
            ObjectsPanelColumn::new("foreign_server", "Foreign Server")
                .width(120.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("foreign_schema", "Foreign Schema")
                .width(120.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("foreign_table", "Foreign Table")
                .width(120.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("options", "Options")
                .width(160.0)
                .min_width(80.0)
                .resizable(true),
            ObjectsPanelColumn::new("inherits_tables", "Inherits Tables")
                .width(140.0)
                .min_width(80.0)
                .resizable(true),
            ObjectsPanelColumn::new("inherited_tables_count", "Inherited Tables Count")
                .width(150.0)
                .min_width(90.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("fill_factor", "Fill Factor")
                .width(100.0)
                .min_width(60.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("unlogged", "Unlogged")
                .width(90.0)
                .min_width(60.0)
                .resizable(true),
            ObjectsPanelColumn::new("system_table", "System Table")
                .width(110.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("comment", "Comment")
                .width(220.0)
                .min_width(100.0)
                .resizable(true),
        ]
    }

    fn postgres_routine_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
        vec![
            ObjectsPanelColumn::new("name", "Name")
                .width(250.0)
                .min_width(120.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("function_type", "Function Type")
                .width(120.0)
                .min_width(80.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("owner", "Owner")
                .width(110.0)
                .min_width(70.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("parameter", "Parameter")
                .width(180.0)
                .min_width(80.0)
                .resizable(true),
            ObjectsPanelColumn::new("language", "Language")
                .width(110.0)
                .min_width(70.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("return_type", "Return Type")
                .width(150.0)
                .min_width(80.0)
                .resizable(true),
            ObjectsPanelColumn::new("volatility", "Volatility")
                .width(110.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("security", "Security")
                .width(100.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("returns_set", "Returns Set")
                .width(100.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("strict", "Strict")
                .width(80.0)
                .min_width(50.0)
                .resizable(true),
            ObjectsPanelColumn::new("estimated_cost", "Estimated Cost")
                .width(120.0)
                .min_width(80.0)
                .resizable(true)
                .text_right(),
            ObjectsPanelColumn::new("estimated_rows", "Estimated Rows")
                .width(120.0)
                .min_width(80.0)
                .resizable(true)
                .text_right(),
            ObjectsPanelColumn::new("configuration_parameters", "Configuration Parameters")
                .width(200.0)
                .min_width(100.0)
                .resizable(true),
            ObjectsPanelColumn::new("comment", "Comment")
                .width(220.0)
                .min_width(100.0)
                .resizable(true),
        ]
    }

    fn postgres_trigger_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
        vec![
            ObjectsPanelColumn::new("name", "Name")
                .width(250.0)
                .min_width(120.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("oid", "OID")
                .width(80.0)
                .min_width(50.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("trigger_type", "Type")
                .width(110.0)
                .min_width(70.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("table_name", "Table Name")
                .width(170.0)
                .min_width(90.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("constraint", "Constraint")
                .width(110.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("fire", "Fire")
                .width(100.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("for_each", "For Each")
                .width(100.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("function_name", "Function Name")
                .width(200.0)
                .min_width(100.0)
                .resizable(true),
            ObjectsPanelColumn::new("function_schema", "Function Schema")
                .width(140.0)
                .min_width(80.0)
                .resizable(true),
            ObjectsPanelColumn::new("deferrable", "Deferrable")
                .width(100.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("initially_deferred", "Initially Deferred")
                .width(140.0)
                .min_width(90.0)
                .resizable(true),
            ObjectsPanelColumn::new("comment", "Comment")
                .width(220.0)
                .min_width(100.0)
                .resizable(true),
        ]
    }

    fn postgres_extension_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
        vec![
            ObjectsPanelColumn::new("name", "Name")
                .width(250.0)
                .min_width(120.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("oid", "OID")
                .width(80.0)
                .min_width(50.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("version", "Version")
                .width(110.0)
                .min_width(70.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("schema_name", "Schema")
                .width(140.0)
                .min_width(80.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("owner", "Owner")
                .width(110.0)
                .min_width(70.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("relocatable", "Relocatable")
                .width(110.0)
                .min_width(70.0)
                .resizable(true),
            ObjectsPanelColumn::new("config_tables", "Config Tables")
                .width(220.0)
                .min_width(100.0)
                .resizable(true),
            ObjectsPanelColumn::new("conditions", "Conditions")
                .width(220.0)
                .min_width(100.0)
                .resizable(true),
            ObjectsPanelColumn::new("comment", "Comment")
                .width(220.0)
                .min_width(100.0)
                .resizable(true),
        ]
    }

    fn postgres_generic_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
        vec![
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
            ObjectsPanelColumn::new("table_type", "Type")
                .width(140.0)
                .min_width(80.0)
                .resizable(true)
                .sortable(),
        ]
    }

    async fn postgres_relation_objects_panel_data(
        &self,
        schema: Option<&str>,
    ) -> Result<ObjectsPanelData> {
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT
                    n.nspname AS schema_name,
                    c.oid,
                    c.relname AS name,
                    r.rolname AS owner,
                    COALESCE(c.relacl::text, '-') AS acl,
                    CASE c.relkind
                        WHEN 'r' THEN 'Normal'
                        WHEN 'v' THEN 'View'
                        WHEN 'm' THEN 'Materialized View'
                        WHEN 'f' THEN 'Foreign Table'
                        WHEN 'p' THEN 'Partitioned'
                        ELSE 'Other'
                    END AS table_type,
                    COALESCE(
                        (SELECT pn.nspname || '.' || p.relname
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         JOIN pg_namespace pn ON pn.oid = p.relnamespace
                         WHERE inh.inhrelid = c.oid
                         LIMIT 1),
                        '-'
                    ) AS partition_of,
                    COALESCE(s.n_live_tup, 0) AS row_count,
                    COALESCE(
                        (SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                         FROM pg_index i
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                         WHERE i.indrelid = c.oid AND i.indisprimary),
                        '-'
                    ) AS primary_key,
                    'No' AS has_oids,
                    COALESCE(fs.srvname, '-') AS foreign_server,
                    COALESCE((SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'schema_name'), '-') AS foreign_schema,
                    COALESCE((SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'table_name'), '-') AS foreign_table,
                    COALESCE(array_to_string(c.reloptions, ', '), '-') AS options,
                    COALESCE(
                        (SELECT string_agg(p.relname, ', ')
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         WHERE inh.inhrelid = c.oid),
                        '-'
                    ) AS inherits_tables,
                    (SELECT count(*) FROM pg_inherits inh WHERE inh.inhparent = c.oid) AS inherited_tables_count,
                    COALESCE(
                        (SELECT split_part(option_value, '=', 2)
                         FROM unnest(c.reloptions) AS option_value
                         WHERE option_value LIKE 'fillfactor=%'
                         LIMIT 1),
                        '-1'
                    ) AS fill_factor,
                    CASE WHEN c.relpersistence = 'u' THEN 'Yes' ELSE 'No' END AS unlogged,
                    CASE WHEN n.nspname IN ('pg_catalog', 'information_schema') THEN 'Yes' ELSE 'No' END AS system_table,
                    COALESCE(obj_description(c.oid, 'pg_class'), '-') AS comment
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_roles r ON r.oid = c.relowner
                 LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                 LEFT JOIN pg_foreign_table ft ON ft.ftrelid = c.oid
                 LEFT JOIN pg_foreign_server fs ON fs.oid = ft.ftserver
                 WHERE n.nspname = $1
                   AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
                 ORDER BY c.relname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT
                    n.nspname AS schema_name,
                    c.oid,
                    c.relname AS name,
                    r.rolname AS owner,
                    COALESCE(c.relacl::text, '-') AS acl,
                    CASE c.relkind
                        WHEN 'r' THEN 'Normal'
                        WHEN 'v' THEN 'View'
                        WHEN 'm' THEN 'Materialized View'
                        WHEN 'f' THEN 'Foreign Table'
                        WHEN 'p' THEN 'Partitioned'
                        ELSE 'Other'
                    END AS table_type,
                    COALESCE(
                        (SELECT pn.nspname || '.' || p.relname
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         JOIN pg_namespace pn ON pn.oid = p.relnamespace
                         WHERE inh.inhrelid = c.oid
                         LIMIT 1),
                        '-'
                    ) AS partition_of,
                    COALESCE(s.n_live_tup, 0) AS row_count,
                    COALESCE(
                        (SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                         FROM pg_index i
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                         WHERE i.indrelid = c.oid AND i.indisprimary),
                        '-'
                    ) AS primary_key,
                    'No' AS has_oids,
                    COALESCE(fs.srvname, '-') AS foreign_server,
                    COALESCE((SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'schema_name'), '-') AS foreign_schema,
                    COALESCE((SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'table_name'), '-') AS foreign_table,
                    COALESCE(array_to_string(c.reloptions, ', '), '-') AS options,
                    COALESCE(
                        (SELECT string_agg(p.relname, ', ')
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         WHERE inh.inhrelid = c.oid),
                        '-'
                    ) AS inherits_tables,
                    (SELECT count(*) FROM pg_inherits inh WHERE inh.inhparent = c.oid) AS inherited_tables_count,
                    COALESCE(
                        (SELECT split_part(option_value, '=', 2)
                         FROM unnest(c.reloptions) AS option_value
                         WHERE option_value LIKE 'fillfactor=%'
                         LIMIT 1),
                        '-1'
                    ) AS fill_factor,
                    CASE WHEN c.relpersistence = 'u' THEN 'Yes' ELSE 'No' END AS unlogged,
                    CASE WHEN n.nspname IN ('pg_catalog', 'information_schema') THEN 'Yes' ELSE 'No' END AS system_table,
                    COALESCE(obj_description(c.oid, 'pg_class'), '-') AS comment
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_roles r ON r.oid = c.relowner
                 LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                 LEFT JOIN pg_foreign_table ft ON ft.ftrelid = c.oid
                 LEFT JOIN pg_foreign_server fs ON fs.oid = ft.ftserver
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
                 ORDER BY n.nspname, c.relname",
                &[],
            )
            .await?
        };

        let column_ids = [
            "schema_name",
            "oid",
            "name",
            "owner",
            "acl",
            "table_type",
            "partition_of",
            "row_count",
            "primary_key",
            "has_oids",
            "foreign_server",
            "foreign_schema",
            "foreign_table",
            "options",
            "inherits_tables",
            "inherited_tables_count",
            "fill_factor",
            "unlogged",
            "system_table",
            "comment",
        ];

        let rows = result
            .rows
            .iter()
            .map(|row| {
                let mut values = std::collections::BTreeMap::new();
                for (query_idx, column_id) in column_ids.iter().enumerate() {
                    let display_value = row
                        .get(query_idx)
                        .map(|value| value.to_string())
                        .filter(|value| value != "NULL")
                        .unwrap_or_else(|| "-".to_string());
                    values.insert(column_id.to_string(), display_value);
                }

                let name = values.get("name").cloned().unwrap_or_default();
                let schema_name = values
                    .get("schema_name")
                    .cloned()
                    .unwrap_or_else(|| "public".to_string());
                let display_name = if schema.is_some() {
                    name.clone()
                } else {
                    schema_qualified_name(&schema_name, &name)
                };
                let table_type = values.get("table_type").map(String::as_str).unwrap_or("");
                let object_type = match table_type {
                    "View" => "view",
                    "Materialized View" => "materialized_view",
                    "Foreign Table" => "foreign_table",
                    "Partitioned" => "partitioned_table",
                    _ => "table",
                };

                ObjectsPanelRow {
                    name: display_name,
                    schema: Some(schema_name.clone()),
                    object_type: object_type.to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new(object_type, name)
                            .with_schema_option(Some(schema_name)),
                    ),
                    values,
                    redis_database_index: None,
                    key_value_info: None,
                }
            })
            .collect();

        Ok(ObjectsPanelData {
            columns: Self::postgres_table_objects_panel_columns(),
            rows,
        })
    }

    async fn postgres_routine_objects_panel_data(
        &self,
        schema: Option<&str>,
        kind_id: &str,
        prokind: &str,
        function_type: &str,
    ) -> Result<ObjectsPanelData> {
        let result = if let Some(schema_name) = schema {
            self.query(
                "SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), r.rolname,
                        l.lanname,
                        pg_catalog.format_type(p.prorettype, NULL),
                        CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' ELSE 'VOLATILE' END,
                        CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END,
                        CASE WHEN p.proretset THEN 'Yes' ELSE 'No' END,
                        CASE WHEN p.proisstrict THEN 'Yes' ELSE 'No' END,
                        p.procost::text,
                        p.prorows::text,
                        COALESCE(array_to_string(p.proconfig, ', '), '-'),
                        COALESCE(obj_description(p.oid, 'pg_proc'), '-')
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 LEFT JOIN pg_roles r ON r.oid = p.proowner
                 JOIN pg_language l ON l.oid = p.prolang
                 WHERE n.nspname = $1
                   AND p.prokind = $2
                 ORDER BY n.nspname, p.proname",
                &[
                    zqlz_core::Value::String(schema_name.to_string()),
                    zqlz_core::Value::String(prokind.to_string()),
                ],
            )
            .await?
        } else {
            self.query(
                "SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), r.rolname,
                        l.lanname,
                        pg_catalog.format_type(p.prorettype, NULL),
                        CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' ELSE 'VOLATILE' END,
                        CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END,
                        CASE WHEN p.proretset THEN 'Yes' ELSE 'No' END,
                        CASE WHEN p.proisstrict THEN 'Yes' ELSE 'No' END,
                        p.procost::text,
                        p.prorows::text,
                        COALESCE(array_to_string(p.proconfig, ', '), '-'),
                        COALESCE(obj_description(p.oid, 'pg_proc'), '-')
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 LEFT JOIN pg_roles r ON r.oid = p.proowner
                 JOIN pg_language l ON l.oid = p.prolang
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND p.prokind = $1
                 ORDER BY n.nspname, p.proname",
                &[zqlz_core::Value::String(prokind.to_string())],
            )
            .await?
        };

        let rows = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("public")
                    .to_string();
                let routine_name = row
                    .get(1)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let signature = row
                    .get(2)
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
                    .filter(|value| !value.is_empty());
                let owner = row.get(3).and_then(|value| value.as_str()).unwrap_or("-");

                let display_name = {
                    let base = if schema.is_some() {
                        routine_name.clone()
                    } else {
                        schema_qualified_name(&schema_name, &routine_name)
                    };
                    signature
                        .as_ref()
                        .map(|value| format!("{}({})", base, value))
                        .unwrap_or(base)
                };

                let mut values = std::collections::BTreeMap::new();
                values.insert("name".to_string(), display_name.clone());
                values.insert("function_type".to_string(), function_type.to_string());
                values.insert("owner".to_string(), owner.to_string());
                values.insert(
                    "parameter".to_string(),
                    signature.clone().unwrap_or_else(|| "-".to_string()),
                );
                values.insert(
                    "language".to_string(),
                    row.get(4)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );
                values.insert(
                    "return_type".to_string(),
                    row.get(5)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );
                values.insert(
                    "volatility".to_string(),
                    row.get(6)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );
                values.insert(
                    "security".to_string(),
                    row.get(7)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );
                values.insert(
                    "returns_set".to_string(),
                    row.get(8)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );
                values.insert(
                    "strict".to_string(),
                    row.get(9)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );
                values.insert(
                    "estimated_cost".to_string(),
                    row.get(10)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );
                values.insert(
                    "estimated_rows".to_string(),
                    row.get(11)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );
                values.insert(
                    "configuration_parameters".to_string(),
                    row.get(12)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );
                values.insert(
                    "comment".to_string(),
                    row.get(13)
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                        .to_string(),
                );

                ObjectsPanelRow {
                    name: display_name,
                    schema: Some(schema_name.clone()),
                    object_type: kind_id.to_string(),
                    object_ref: Some(Self::postgres_routine_object_ref(
                        kind_id,
                        routine_name,
                        schema_name,
                        signature,
                    )),
                    values,
                    redis_database_index: None,
                    key_value_info: None,
                }
            })
            .collect();

        Ok(ObjectsPanelData {
            columns: Self::postgres_routine_objects_panel_columns(),
            rows,
        })
    }

    async fn postgres_trigger_objects_panel_data(
        &self,
        schema: Option<&str>,
    ) -> Result<ObjectsPanelData> {
        let query = "SELECT
                    n.nspname AS schema_name,
                    t.tgname,
                    t.oid,
                    'Table' AS trigger_type,
                    c.relname AS table_name,
                    CASE WHEN t.tgconstraint <> 0 THEN 'Yes' ELSE 'No' END AS is_constraint,
                    CASE
                        WHEN (t.tgtype & 64) <> 0 THEN 'INSTEAD OF'
                        WHEN (t.tgtype & 2) <> 0 THEN 'BEFORE'
                        ELSE 'AFTER'
                    END AS fire,
                    CASE WHEN (t.tgtype & 1) <> 0 THEN 'ROW' ELSE 'STATEMENT' END AS for_each,
                    p.proname AS function_name,
                    pn.nspname AS function_schema,
                    CASE WHEN COALESCE(con.condeferrable, false) THEN 'Yes' ELSE 'No' END AS deferrable,
                    CASE WHEN COALESCE(con.condeferred, false) THEN 'Yes' ELSE 'No' END AS initially_deferred,
                    COALESCE(obj_description(t.oid, 'pg_trigger'), '-') AS comment
                 FROM pg_trigger t
                 JOIN pg_class c ON c.oid = t.tgrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 JOIN pg_proc p ON p.oid = t.tgfoid
                 JOIN pg_namespace pn ON pn.oid = p.pronamespace
                 LEFT JOIN pg_constraint con ON con.oid = t.tgconstraint
                 WHERE NOT t.tgisinternal";

        let result = if let Some(schema_name) = schema {
            self.query(
                &format!("{} AND n.nspname = $1 ORDER BY n.nspname, t.tgname", query),
                &[zqlz_core::Value::String(schema_name.to_string())],
            )
            .await?
        } else {
            self.query(
                &format!(
                    "{} AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                       AND n.nspname NOT LIKE 'pg_temp_%'
                       AND n.nspname NOT LIKE 'pg_toast_temp_%'
                     ORDER BY n.nspname, t.tgname",
                    query
                ),
                &[],
            )
            .await?
        };

        let column_ids = [
            "schema_name",
            "name",
            "oid",
            "trigger_type",
            "table_name",
            "constraint",
            "fire",
            "for_each",
            "function_name",
            "function_schema",
            "deferrable",
            "initially_deferred",
            "comment",
        ];

        let rows = result
            .rows
            .iter()
            .map(|row| {
                let mut values = std::collections::BTreeMap::new();
                for (query_idx, column_id) in column_ids.iter().enumerate() {
                    let display_value = row
                        .get(query_idx)
                        .map(|value| value.to_string())
                        .filter(|value| value != "NULL")
                        .unwrap_or_else(|| "-".to_string());
                    values.insert(column_id.to_string(), display_value);
                }

                let schema_name = values
                    .get("schema_name")
                    .cloned()
                    .unwrap_or_else(|| "public".to_string());
                let trigger_name = values.get("name").cloned().unwrap_or_default();
                let table_name = values.get("table_name").cloned().unwrap_or_default();
                let display_name = if schema.is_some() {
                    trigger_name.clone()
                } else {
                    schema_qualified_name(&schema_name, &trigger_name)
                };
                values.insert("name".to_string(), display_name.clone());

                ObjectsPanelRow {
                    name: display_name,
                    schema: Some(schema_name.clone()),
                    object_type: "trigger".to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new("trigger", trigger_name)
                            .with_schema_option(Some(schema_name))
                            .with_signature_option(Some(table_name)),
                    ),
                    values,
                    redis_database_index: None,
                    key_value_info: None,
                }
            })
            .collect();

        Ok(ObjectsPanelData {
            columns: Self::postgres_trigger_objects_panel_columns(),
            rows,
        })
    }

    async fn postgres_extension_objects_panel_data(
        &self,
        schema: Option<&str>,
    ) -> Result<ObjectsPanelData> {
        let result = self
            .query(
                "SELECT
                    e.extname,
                    e.oid,
                    e.extversion,
                    n.nspname,
                    r.rolname,
                    CASE WHEN e.extrelocatable THEN 'Yes' ELSE 'No' END,
                    COALESCE(
                        (SELECT string_agg(c.oid::regclass::text, ', ')
                         FROM unnest(e.extconfig) AS c(oid)),
                        '-'
                    ),
                    COALESCE(array_to_string(e.extcondition, ', '), '-'),
                    COALESCE(obj_description(e.oid, 'pg_extension'), '-')
                 FROM pg_extension e
                 JOIN pg_namespace n ON n.oid = e.extnamespace
                 LEFT JOIN pg_roles r ON r.oid = e.extowner
                 ORDER BY n.nspname, e.extname",
                &[],
            )
            .await?;

        let column_ids = [
            "name",
            "oid",
            "version",
            "schema_name",
            "owner",
            "relocatable",
            "config_tables",
            "conditions",
            "comment",
        ];

        let rows = result
            .rows
            .iter()
            .filter_map(|row| {
                let schema_name = row
                    .get(3)
                    .and_then(|value| value.as_str())
                    .unwrap_or("public")
                    .to_string();

                if let Some(target_schema) = schema
                    && target_schema != schema_name
                {
                    return None;
                }

                let mut values = std::collections::BTreeMap::new();
                for (query_idx, column_id) in column_ids.iter().enumerate() {
                    let display_value = row
                        .get(query_idx)
                        .map(|value| value.to_string())
                        .filter(|value| value != "NULL")
                        .unwrap_or_else(|| "-".to_string());
                    values.insert(column_id.to_string(), display_value);
                }

                let extension_name = values.get("name").cloned().unwrap_or_default();
                let display_name = if schema.is_some() {
                    extension_name.clone()
                } else {
                    schema_qualified_name(&schema_name, &extension_name)
                };
                values.insert("name".to_string(), display_name.clone());

                Some(ObjectsPanelRow {
                    name: display_name,
                    schema: Some(schema_name.clone()),
                    object_type: "extension".to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new("extension", extension_name)
                            .with_schema_option(Some(schema_name)),
                    ),
                    values,
                    redis_database_index: None,
                    key_value_info: None,
                })
            })
            .collect();

        Ok(ObjectsPanelData {
            columns: Self::postgres_extension_objects_panel_columns(),
            rows,
        })
    }

    /// Build a canonical identity for PostgreSQL routine rows so overloaded signatures stay distinct.
    fn postgres_routine_object_ref(
        kind_id: &str,
        name: String,
        schema_name: String,
        signature: Option<String>,
    ) -> ObjectsPanelObjectRef {
        ObjectsPanelObjectRef::new(kind_id, name)
            .with_schema_option(Some(schema_name))
            .with_signature_option(signature)
    }

    /// Build the explicit PostgreSQL objects-panel matrix from driver-owned metadata.
    fn postgres_objects_panel_manifest() -> ObjectsPanelManifest {
        let table_columns = Self::postgres_table_objects_panel_columns();
        let routine_columns = Self::postgres_routine_objects_panel_columns();
        let trigger_columns = Self::postgres_trigger_objects_panel_columns();
        let extension_columns = Self::postgres_extension_objects_panel_columns();
        let generic_columns = Self::postgres_generic_objects_panel_columns();

        let relation_actions = vec![
            ObjectsPanelAction::new("open", "Open").group("open"),
            ObjectsPanelAction::new("design", "Design")
                .group("open")
                .single_selection(),
            ObjectsPanelAction::new("rename", "Rename")
                .group("modify")
                .single_selection(),
            ObjectsPanelAction::new("duplicate", "Duplicate").group("modify"),
            ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                .group("clipboard"),
            ObjectsPanelAction::new("view_history", "View History")
                .group("metadata")
                .single_selection(),
            ObjectsPanelAction::new("export", "Export").group("data"),
            ObjectsPanelAction::new("delete", "Delete")
                .group("danger")
                .destructive(),
            ObjectsPanelAction::new("refresh", "Refresh").group("system"),
        ];

        let table_actions = vec![
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
            ObjectsPanelAction::new("dump_sql_structure", "Dump SQL (Structure Only)")
                .group("data"),
            ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                .group("clipboard"),
            ObjectsPanelAction::new("view_history", "View History")
                .group("metadata")
                .single_selection(),
            ObjectsPanelAction::new("delete", "Delete")
                .group("danger")
                .destructive(),
            ObjectsPanelAction::new("refresh", "Refresh").group("system"),
        ];

        let routine_actions = vec![
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
            ObjectsPanelAction::new("refresh", "Refresh").group("system"),
        ];

        let metadata_actions = vec![
            ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                .group("clipboard"),
            ObjectsPanelAction::new("refresh", "Refresh").group("system"),
        ];
        let enum_domain_actions = vec![
            ObjectsPanelAction::new("design", "Design")
                .group("open")
                .single_selection(),
            ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                .group("clipboard"),
            ObjectsPanelAction::new("delete", "Drop")
                .group("destructive")
                .single_selection()
                .destructive(),
            ObjectsPanelAction::new("refresh", "Refresh").group("system"),
        ];
        let sequence_actions = vec![
            ObjectsPanelAction::new("open", "Open").group("open"),
            ObjectsPanelAction::new("design", "Design")
                .group("open")
                .single_selection(),
            ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
            ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                .group("clipboard"),
            ObjectsPanelAction::new("refresh", "Refresh").group("system"),
        ];

        let object_kinds = vec![
            ObjectsPanelObjectKind::new("table", "Table", "Tables")
                .icon_key("table")
                .columns(table_columns.clone())
                .row_actions(table_actions.clone())
                .default_row_action("open"),
            ObjectsPanelObjectKind::new(
                "partitioned_table",
                "Partitioned Table",
                "Partitioned Tables",
            )
            .icon_key("table")
            .columns(table_columns.clone())
            .row_actions(table_actions.clone())
            .default_row_action("open"),
            ObjectsPanelObjectKind::new("foreign_table", "Foreign Table", "Foreign Tables")
                .icon_key("table")
                .columns(table_columns.clone())
                .row_actions(table_actions)
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("view", "View", "Views")
                .icon_key("view")
                .columns(generic_columns.clone())
                .row_actions(relation_actions.clone())
                .default_row_action("open"),
            ObjectsPanelObjectKind::new(
                "materialized_view",
                "Materialized View",
                "Materialized Views",
            )
            .icon_key("view")
            .columns(generic_columns.clone())
            .row_actions(relation_actions)
            .default_row_action("open"),
            ObjectsPanelObjectKind::new("function", "Function", "Functions")
                .icon_key("function")
                .columns(routine_columns.clone())
                .row_actions(routine_actions.clone())
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("procedure", "Procedure", "Procedures")
                .icon_key("procedure")
                .columns(routine_columns.clone())
                .row_actions(routine_actions.clone())
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("trigger", "Trigger", "Triggers")
                .icon_key("trigger")
                .columns(trigger_columns)
                .row_actions(routine_actions)
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("sequence", "Sequence", "Sequences")
                .icon_key("sequence")
                .columns(generic_columns.clone())
                .row_actions(sequence_actions)
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("enum", "Enum", "Enums")
                .icon_key("type")
                .columns(generic_columns.clone())
                .row_actions(enum_domain_actions.clone())
                .default_row_action("design"),
            ObjectsPanelObjectKind::new("domain", "Domain", "Domains")
                .icon_key("type")
                .columns(generic_columns.clone())
                .row_actions(enum_domain_actions)
                .default_row_action("design"),
            ObjectsPanelObjectKind::new("range", "Range", "Ranges")
                .icon_key("type")
                .columns(generic_columns.clone())
                .row_actions(metadata_actions.clone()),
            ObjectsPanelObjectKind::new("composite_type", "Composite Type", "Composite Types")
                .icon_key("type")
                .columns(generic_columns.clone())
                .row_actions(metadata_actions.clone()),
            ObjectsPanelObjectKind::new("type", "Type", "Types")
                .icon_key("type")
                .columns(generic_columns.clone())
                .row_actions(metadata_actions.clone()),
            ObjectsPanelObjectKind::new("index", "Index", "Indexes")
                .icon_key("index")
                .columns(generic_columns.clone())
                .row_actions(metadata_actions.clone()),
            ObjectsPanelObjectKind::new("schema", "Schema", "Schemas")
                .icon_key("schema")
                .columns(generic_columns.clone())
                .row_actions(metadata_actions),
            ObjectsPanelObjectKind::new("extension", "Extension", "Extensions")
                .icon_key("extension")
                .columns(extension_columns)
                .row_actions(vec![
                    ObjectsPanelAction::new("open", "Open"),
                    ObjectsPanelAction::new("design", "Design"),
                    ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
                    ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                        .group("clipboard"),
                    ObjectsPanelAction::new("refresh", "Refresh").group("system"),
                ])
                .default_row_action("open"),
            ObjectsPanelObjectKind::new("foreign_server", "Foreign Server", "Foreign Servers")
                .icon_key("foreign_server")
                .columns(vec![
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
                    ObjectsPanelColumn::new("table_type", "Type")
                        .width(140.0)
                        .min_width(80.0)
                        .resizable(true)
                        .sortable(),
                ])
                .row_actions(vec![
                    ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
                    ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                        .group("clipboard"),
                    ObjectsPanelAction::new("refresh", "Refresh").group("system"),
                ]),
            ObjectsPanelObjectKind::new(
                "foreign_data_wrapper",
                "Foreign Data Wrapper",
                "Foreign Data Wrappers",
            )
            .icon_key("foreign_data_wrapper")
            .columns(vec![
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
                ObjectsPanelColumn::new("table_type", "Type")
                    .width(140.0)
                    .min_width(80.0)
                    .resizable(true)
                    .sortable(),
            ])
            .row_actions(vec![
                ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
                ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                    .group("clipboard"),
                ObjectsPanelAction::new("refresh", "Refresh").group("system"),
            ]),
            ObjectsPanelObjectKind::new("policy", "Policy", "Policies")
                .icon_key("policy")
                .columns(vec![
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
                    ObjectsPanelColumn::new("table_type", "Type")
                        .width(140.0)
                        .min_width(80.0)
                        .resizable(true)
                        .sortable(),
                ])
                .row_actions(vec![
                    ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
                    ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                        .group("clipboard"),
                    ObjectsPanelAction::new("refresh", "Refresh").group("system"),
                ]),
            ObjectsPanelObjectKind::new("publication", "Publication", "Publications")
                .icon_key("publication")
                .columns(vec![
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
                    ObjectsPanelColumn::new("table_type", "Type")
                        .width(140.0)
                        .min_width(80.0)
                        .resizable(true)
                        .sortable(),
                ])
                .row_actions(vec![
                    ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
                    ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                        .group("clipboard"),
                    ObjectsPanelAction::new("refresh", "Refresh").group("system"),
                ]),
            ObjectsPanelObjectKind::new("subscription", "Subscription", "Subscriptions")
                .icon_key("subscription")
                .columns(vec![
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
                    ObjectsPanelColumn::new("table_type", "Type")
                        .width(140.0)
                        .min_width(80.0)
                        .resizable(true)
                        .sortable(),
                ])
                .row_actions(vec![
                    ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
                    ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                        .group("clipboard"),
                    ObjectsPanelAction::new("refresh", "Refresh").group("system"),
                ]),
            ObjectsPanelObjectKind::new("event_trigger", "Event Trigger", "Event Triggers")
                .icon_key("event_trigger")
                .columns(vec![
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
                    ObjectsPanelColumn::new("table_type", "Type")
                        .width(140.0)
                        .min_width(80.0)
                        .resizable(true)
                        .sortable(),
                ])
                .row_actions(vec![
                    ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
                    ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                        .group("clipboard"),
                    ObjectsPanelAction::new("refresh", "Refresh").group("system"),
                ]),
            ObjectsPanelObjectKind::new("language", "Language", "Languages")
                .icon_key("language")
                .columns(vec![
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
                    ObjectsPanelColumn::new("table_type", "Type")
                        .width(140.0)
                        .min_width(80.0)
                        .resizable(true)
                        .sortable(),
                ])
                .row_actions(vec![
                    ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
                    ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                        .group("clipboard"),
                    ObjectsPanelAction::new("refresh", "Refresh").group("system"),
                ]),
            ObjectsPanelObjectKind::new("collation", "Collation", "Collations")
                .icon_key("collation")
                .columns(vec![
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
                    ObjectsPanelColumn::new("table_type", "Type")
                        .width(140.0)
                        .min_width(80.0)
                        .resizable(true)
                        .sortable(),
                ])
                .row_actions(vec![
                    ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
                    ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                        .group("clipboard"),
                    ObjectsPanelAction::new("refresh", "Refresh").group("system"),
                ]),
            ObjectsPanelObjectKind::new("tablespace", "Tablespace", "Tablespaces")
                .icon_key("tablespace")
                .columns(vec![
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
                    ObjectsPanelColumn::new("table_type", "Type")
                        .width(140.0)
                        .min_width(80.0)
                        .resizable(true)
                        .sortable(),
                ])
                .row_actions(vec![
                    ObjectsPanelAction::new("copy_name", "Copy Name").group("clipboard"),
                    ObjectsPanelAction::new("copy_qualified_name", "Copy Qualified Name")
                        .group("clipboard"),
                    ObjectsPanelAction::new("refresh", "Refresh").group("system"),
                ]),
        ];

        ObjectsPanelManifest {
            object_kinds,
            toolbar_actions: vec![
                ObjectsPanelAction::new("refresh", "Refresh")
                    .icon_key("refresh")
                    .refreshes_objects_panel(),
                ObjectsPanelAction::new("new_table", "New Table")
                    .icon_key("create")
                    .create_object_kind("table"),
                ObjectsPanelAction::new("new_view", "New View")
                    .icon_key("create")
                    .create_object_kind("view"),
                ObjectsPanelAction::new("new_enum", "New Enum")
                    .icon_key("create")
                    .create_object_kind("enum")
                    .object_form("enum", ObjectFormMode::Create),
                ObjectsPanelAction::new("new_domain", "New Domain")
                    .icon_key("create")
                    .create_object_kind("domain")
                    .object_form("domain", ObjectFormMode::Create),
                ObjectsPanelAction::new("import", "Import Wizard...").icon_key("import"),
                ObjectsPanelAction::new("export", "Export Wizard...").icon_key("export"),
            ],
        }
    }
}

#[async_trait]
impl SchemaIntrospection for PostgresConnection {
    #[tracing::instrument(skip(self))]
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .query(
                "SELECT datname, pg_database_size(datname) as size_bytes, pg_encoding_to_char(encoding) as encoding
                 FROM pg_database 
                 WHERE datistemplate = false 
                 ORDER BY datname",
                &[],
            )
            .await?;

        let databases = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let size_bytes = row.get(1).and_then(|v| v.as_i64());
                let encoding = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                DatabaseInfo {
                    name,
                    owner: None,
                    encoding,
                    size_bytes,
                    comment: None,
                }
            })
            .collect();

        Ok(databases)
    }

    #[tracing::instrument(skip(self))]
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let result = self
            .query(
                "SELECT schema_name 
                 FROM information_schema.schemata 
                 WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                 ORDER BY schema_name",
                &[],
            )
            .await?;

        let schemas = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                SchemaInfo {
                    name,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(schemas)
    }

    #[tracing::instrument(skip(self))]
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT
                    n.nspname,
                    c.relname,
                    c.relkind,
                    CASE WHEN c.relkind IN ('v', 'm') THEN NULL ELSE s.n_live_tup END AS row_count,
                    CASE WHEN c.relkind IN ('v', 'm') THEN NULL ELSE pg_total_relation_size(c.oid) END AS size_bytes,
                    obj_description(c.oid, 'pg_class') AS comment
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
                 WHERE n.nspname = $1
                   AND c.relkind IN ('r', 'p', 'f')
                 ORDER BY c.relname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT
                    n.nspname,
                    c.relname,
                    c.relkind,
                    CASE WHEN c.relkind IN ('v', 'm') THEN NULL ELSE s.n_live_tup END AS row_count,
                    CASE WHEN c.relkind IN ('v', 'm') THEN NULL ELSE pg_total_relation_size(c.oid) END AS size_bytes,
                    obj_description(c.oid, 'pg_class') AS comment
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND c.relkind IN ('r', 'p', 'f')
                 ORDER BY n.nspname, c.relname",
                &[],
            )
            .await?
        };

        let tables = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let relkind = row.get(2).and_then(|v| v.as_str()).unwrap_or("r");
                let row_count = row.get(3).and_then(|v| v.as_i64());
                let size_bytes = row.get(4).and_then(|v| v.as_i64());
                let comment = row.get(5).and_then(|v| v.as_str()).map(|s| s.to_string());

                let table_type = match relkind {
                    "p" => TableType::PartitionedTable,
                    "f" => TableType::ForeignTable,
                    _ => TableType::Table,
                };

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                TableInfo {
                    name: display_name,
                    schema: Some(schema_name),
                    table_type,
                    owner: None,
                    row_count,
                    size_bytes,
                    comment,
                    index_count: None,
                    trigger_count: None,
                    key_value_info: None,
                }
            })
            .collect();

        Ok(tables)
    }

    #[tracing::instrument(skip(self))]
    async fn list_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT table_schema, table_name, view_definition
                 FROM information_schema.views
                 WHERE table_schema = $1
                 ORDER BY table_name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT table_schema, table_name, view_definition
                 FROM information_schema.views
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND table_schema NOT LIKE 'pg_temp_%'
                   AND table_schema NOT LIKE 'pg_toast_temp_%'
                 ORDER BY table_schema, table_name",
                &[],
            )
            .await?
        };

        let views = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                ViewInfo {
                    name: display_name,
                    schema: Some(schema_name),
                    is_materialized: false,
                    definition,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(views)
    }

    #[tracing::instrument(skip(self))]
    async fn list_materialized_views(&self, schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT schemaname, matviewname, definition
                 FROM pg_matviews
                 WHERE schemaname = $1
                 ORDER BY matviewname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT schemaname, matviewname, definition
                 FROM pg_matviews
                 WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND schemaname NOT LIKE 'pg_temp_%'
                   AND schemaname NOT LIKE 'pg_toast_temp_%'
                 ORDER BY schemaname, matviewname",
                &[],
            )
            .await?
        };

        let views = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                ViewInfo {
                    name: display_name,
                    schema: Some(schema_name),
                    is_materialized: true,
                    definition,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(views)
    }

    #[tracing::instrument(skip(self))]
    async fn get_table(&self, schema: Option<&str>, name: &str) -> Result<TableDetails> {
        let (schema, table_name) = resolve_relation_identifiers(schema, name, "public");
        let tables = self.list_tables(Some(schema.as_str())).await?;
        let info = tables
            .into_iter()
            .find(|table| table.name == table_name)
            .ok_or_else(|| ZqlzError::NotFound(format!("Table '{}' not found", name)))?;

        let columns = self
            .get_columns(Some(schema.as_str()), table_name.as_str())
            .await?;
        let indexes = self
            .get_indexes(Some(schema.as_str()), table_name.as_str())
            .await?;
        let foreign_keys = self
            .get_foreign_keys(Some(schema.as_str()), table_name.as_str())
            .await?;
        let primary_key = self
            .get_primary_key(Some(schema.as_str()), table_name.as_str())
            .await?;
        let constraints = self
            .get_constraints(Some(schema.as_str()), table_name.as_str())
            .await?;
        let triggers = self
            .list_triggers(Some(schema.as_str()), Some(table_name.as_str()))
            .await?;

        Ok(TableDetails {
            info,
            columns,
            primary_key,
            foreign_keys,
            indexes,
            constraints,
            triggers,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let (schema, table_name) = resolve_relation_identifiers(schema, table, "public");
        let result = self
            .query(
                POSTGRES_COLUMNS_SQL,
                &[
                    zqlz_core::Value::String(schema.clone()),
                    zqlz_core::Value::String(table_name),
                ],
            )
            .await?;

        let columns = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let ordinal = row.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
                let data_type = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let is_nullable = row.get(3).and_then(|v| v.as_str()).unwrap_or("NO") == "YES";
                let default_value = row.get(4).and_then(|v| v.as_str()).map(|s| s.to_string());
                let max_length = row.get(5).and_then(|v| v.as_i64());
                let precision = row.get(6).and_then(|v| v.as_i64()).map(|i| i as i32);
                let scale = row.get(7).and_then(|v| v.as_i64()).map(|i| i as i32);
                let is_identity = row.get(8).and_then(|v| v.as_str()).unwrap_or("NO") == "YES";
                let enum_values = row.get(9).and_then(|value| value.as_string_array());
                let is_auto_increment = is_identity
                    || default_value
                        .as_ref()
                        .map(|default| default.to_lowercase().contains("nextval("))
                        .unwrap_or(false);

                ColumnInfo {
                    name,
                    ordinal,
                    data_type,
                    nullable: is_nullable,
                    default_value,
                    max_length,
                    precision,
                    scale,
                    is_primary_key: false, // Will be filled by get_primary_key
                    is_auto_increment,
                    is_unique: false,
                    foreign_key: None,
                    comment: None,
                    enum_values,
                    ..Default::default()
                }
            })
            .collect();

        Ok(columns)
    }

    #[tracing::instrument(skip(self))]
    async fn get_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexInfo>> {
        let (schema, table_name) = resolve_relation_identifiers(schema, table, "public");
        // indnkeyatts is the number of key columns (introduced in PostgreSQL 11).
        // Columns beyond that index are non-key INCLUDE columns.  We use a fallback
        // of array_length(ix.indkey, 1) so the query works on older PostgreSQL versions.
        let result = self
            .query(
                "SELECT
                    i.relname AS index_name,
                    ix.indisunique AS is_unique,
                    ix.indisprimary AS is_primary,
                    array_agg(
                        a.attname
                        ORDER BY array_position(ix.indkey, a.attnum)
                    ) FILTER (
                        WHERE a.attnum <= coalesce(ix.indnkeyatts, array_length(ix.indkey, 1))
                    ) AS key_columns,
                    array_agg(
                        a.attname
                        ORDER BY array_position(ix.indkey, a.attnum)
                    ) FILTER (
                        WHERE a.attnum > coalesce(ix.indnkeyatts, array_length(ix.indkey, 1))
                    ) AS include_columns,
                    am.amname AS index_method,
                    pg_get_expr(ix.indpred, ix.indrelid) AS where_clause
                 FROM pg_class t
                 JOIN pg_index ix ON t.oid = ix.indrelid
                 JOIN pg_class i ON i.oid = ix.indexrelid
                 JOIN pg_am am ON am.oid = i.relam
                 JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                 JOIN pg_namespace n ON n.oid = t.relnamespace
                 WHERE n.nspname = $1 AND t.relname = $2
                 GROUP BY i.relname, ix.indisunique, ix.indisprimary, ix.indnkeyatts,
                          ix.indkey, ix.indpred, ix.indrelid, am.amname
                 ORDER BY i.relname",
                &[
                    zqlz_core::Value::String(schema),
                    zqlz_core::Value::String(table_name),
                ],
            )
            .await?;

        let indexes = result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get(0).and_then(|v| v.as_str())?.to_string();
                let is_unique = row.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
                let is_primary = row.get(2).and_then(|v| v.as_bool()).unwrap_or(false);
                let columns = row
                    .get(3)
                    .and_then(|v| v.as_string_array())
                    .unwrap_or_default();
                let include_columns = row
                    .get(4)
                    .and_then(|v| v.as_string_array())
                    .unwrap_or_default();
                let index_type = row
                    .get(5)
                    .and_then(|v| v.as_str())
                    .unwrap_or("btree")
                    .to_string();
                let where_clause = row.get(6).and_then(|v| v.as_str()).map(|s| s.to_string());

                Some(IndexInfo {
                    name,
                    columns,
                    is_unique,
                    is_primary,
                    index_type,
                    comment: None,
                    where_clause,
                    include_columns,
                    column_descending: vec![],
                })
            })
            .collect();

        Ok(indexes)
    }

    #[tracing::instrument(skip(self))]
    async fn get_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let (schema, table_name) = resolve_relation_identifiers(schema, table, "public");
        let result = self
            .query(
                "SELECT 
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    rc.update_rule,
                    rc.delete_rule
                 FROM information_schema.table_constraints AS tc
                 JOIN information_schema.key_column_usage AS kcu
                   ON tc.constraint_name = kcu.constraint_name
                   AND tc.table_schema = kcu.table_schema
                 JOIN information_schema.constraint_column_usage AS ccu
                   ON ccu.constraint_name = tc.constraint_name
                   AND ccu.table_schema = tc.table_schema
                 JOIN information_schema.referential_constraints AS rc
                   ON rc.constraint_name = tc.constraint_name
                 WHERE tc.constraint_type = 'FOREIGN KEY'
                   AND tc.table_schema = $1
                   AND tc.table_name = $2",
                &[
                    zqlz_core::Value::String(schema.clone()),
                    zqlz_core::Value::String(table_name),
                ],
            )
            .await?;

        let fks = result
            .rows
            .iter()
            .map(|row| {
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
                let ref_column = row
                    .get(3)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let on_update_str = row.get(4).and_then(|v| v.as_str()).unwrap_or("NO ACTION");
                let on_delete_str = row.get(5).and_then(|v| v.as_str()).unwrap_or("NO ACTION");

                ForeignKeyInfo {
                    name,
                    columns: vec![column],
                    referenced_table: ref_table,
                    referenced_schema: Some(schema.to_string()),
                    referenced_columns: vec![ref_column],
                    on_update: parse_fk_action(on_update_str),
                    on_delete: parse_fk_action(on_delete_str),
                    is_deferrable: false,
                    initially_deferred: false,
                }
            })
            .collect();

        Ok(fks)
    }

    async fn get_primary_key(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        let (schema, table_name) = resolve_relation_identifiers(schema, table, "public");
        let result = self
            .query(
                "SELECT 
                    tc.constraint_name,
                    array_agg(kcu.column_name::text ORDER BY kcu.ordinal_position) as columns
                 FROM information_schema.table_constraints tc
                 JOIN information_schema.key_column_usage kcu
                   ON tc.constraint_name = kcu.constraint_name
                   AND tc.table_schema = kcu.table_schema
                 WHERE tc.constraint_type = 'PRIMARY KEY'
                   AND tc.table_schema = $1
                   AND tc.table_name = $2
                 GROUP BY tc.constraint_name",
                &[
                    zqlz_core::Value::String(schema),
                    zqlz_core::Value::String(table_name),
                ],
            )
            .await?;

        if let Some(row) = result.rows.first() {
            let name = row.get(0).and_then(|v| v.as_str()).map(|s| s.to_string());
            // Parse the array_agg column which returns a string array
            let columns = row
                .get(1)
                .and_then(|v| v.as_string_array())
                .unwrap_or_default();

            Ok(Some(PrimaryKeyInfo { name, columns }))
        } else {
            Ok(None)
        }
    }

    async fn get_constraints(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        let (schema, table_name) = resolve_relation_identifiers(schema, table, "public");
        // pg_get_constraintdef returns the full constraint definition including the CHECK keyword;
        // we store it as-is so importers have the verbatim expression without needing to
        // reconstruct it from raw column-level data.
        let result = self
            .query(
                "SELECT
                    con.conname AS constraint_name,
                    pg_get_constraintdef(con.oid) AS definition,
                    array_agg(att.attname ORDER BY att.attnum) AS columns
                 FROM pg_constraint con
                 JOIN pg_class rel ON rel.oid = con.conrelid
                 JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                 LEFT JOIN pg_attribute att
                   ON att.attrelid = rel.oid
                   AND att.attnum = ANY(con.conkey)
                 WHERE con.contype = 'c'
                   AND nsp.nspname = $1
                   AND rel.relname = $2
                 GROUP BY con.conname, con.oid
                 ORDER BY con.conname",
                &[
                    zqlz_core::Value::String(schema),
                    zqlz_core::Value::String(table_name),
                ],
            )
            .await?;

        let constraints = result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.get(0).and_then(|v| v.as_str())?.to_string();
                let definition = row.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                let columns = row
                    .get(2)
                    .and_then(|v| v.as_string_array())
                    .unwrap_or_default();

                Some(ConstraintInfo {
                    name,
                    constraint_type: ConstraintType::Check,
                    columns,
                    definition,
                })
            })
            .collect();

        Ok(constraints)
    }

    async fn list_functions(&self, schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT
                    n.nspname,
                    p.proname as function_name,
                    pg_get_function_identity_arguments(p.oid) as arguments,
                    t.typname as return_type
                 FROM pg_proc p
                 JOIN pg_namespace n ON p.pronamespace = n.oid
                 JOIN pg_type t ON p.prorettype = t.oid
                 WHERE n.nspname = $1
                   AND p.prokind = 'f'
                 ORDER BY p.proname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT
                    n.nspname,
                    p.proname as function_name,
                    pg_get_function_identity_arguments(p.oid) as arguments,
                    t.typname as return_type
                 FROM pg_proc p
                 JOIN pg_namespace n ON p.pronamespace = n.oid
                 JOIN pg_type t ON p.prorettype = t.oid
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND p.prokind = 'f'
                 ORDER BY n.nspname, p.proname",
                &[],
            )
            .await?
        };

        let functions = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let _arguments = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let return_type = row
                    .get(3)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                FunctionInfo {
                    name: display_name,
                    schema: Some(schema_name),
                    language: "sql".to_string(), // Default to SQL
                    return_type,
                    parameters: vec![], // TODO: Parse parameters properly
                    definition: None,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(functions)
    }

    async fn list_procedures(&self, schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT
                    n.nspname,
                    p.proname as procedure_name,
                    pg_get_function_identity_arguments(p.oid) as arguments
                 FROM pg_proc p
                 JOIN pg_namespace n ON p.pronamespace = n.oid
                 WHERE n.nspname = $1
                   AND p.prokind = 'p'
                 ORDER BY p.proname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT
                    n.nspname,
                    p.proname as procedure_name,
                    pg_get_function_identity_arguments(p.oid) as arguments
                 FROM pg_proc p
                 JOIN pg_namespace n ON p.pronamespace = n.oid
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND p.prokind = 'p'
                 ORDER BY n.nspname, p.proname",
                &[],
            )
            .await?
        };

        let procedures = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let _arguments = row
                    .get(2)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                ProcedureInfo {
                    name: display_name,
                    schema: Some(schema_name),
                    language: "sql".to_string(), // Default to SQL
                    parameters: vec![],          // TODO: Parse parameters properly
                    definition: None,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(procedures)
    }

    async fn list_triggers(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        let default_schema = schema.unwrap_or("public");
        let (resolved_schema, table_name_filter) = table
            .map(|table_name| resolve_relation_identifiers(schema, table_name, default_schema))
            .map_or_else(
                || (schema.map(ToString::to_string), None),
                |(resolved_schema, resolved_table_name)| {
                    (Some(resolved_schema), Some(resolved_table_name))
                },
            );

        let result = if let Some(tbl) = table_name_filter {
            let schema = resolved_schema.unwrap_or_else(|| default_schema.to_string());
            self.query(
                "SELECT 
                    t.tgname as trigger_name,
                    c.relname as table_name,
                    pg_get_triggerdef(t.oid) as definition
                 FROM pg_trigger t
                 JOIN pg_class c ON t.tgrelid = c.oid
                 JOIN pg_namespace n ON c.relnamespace = n.oid
                 WHERE n.nspname = $1 AND c.relname = $2
                   AND NOT t.tgisinternal
                 ORDER BY t.tgname",
                &[
                    zqlz_core::Value::String(schema.to_string()),
                    zqlz_core::Value::String(tbl),
                ],
            )
            .await?
        } else if let Some(schema) = resolved_schema {
            self.query(
                "SELECT 
                    t.tgname as trigger_name,
                    c.relname as table_name,
                    pg_get_triggerdef(t.oid) as definition
                 FROM pg_trigger t
                 JOIN pg_class c ON t.tgrelid = c.oid
                 JOIN pg_namespace n ON c.relnamespace = n.oid
                 WHERE n.nspname = $1
                   AND NOT t.tgisinternal
                 ORDER BY t.tgname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT 
                    t.tgname as trigger_name,
                    c.relname as table_name,
                    pg_get_triggerdef(t.oid) as definition
                 FROM pg_trigger t
                 JOIN pg_class c ON t.tgrelid = c.oid
                 JOIN pg_namespace n ON c.relnamespace = n.oid
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND NOT t.tgisinternal
                 ORDER BY n.nspname, t.tgname",
                &[],
            )
            .await?
        };

        let triggers = result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let table_name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let definition = row.get(2).and_then(|v| v.as_str()).map(|s| s.to_string());

                TriggerInfo {
                    name,
                    schema: schema.map(str::to_string),
                    table_name,
                    timing: TriggerTiming::Before, // TODO: Parse from definition
                    events: vec![TriggerEvent::Insert], // TODO: Parse from definition
                    for_each: TriggerForEach::Row,
                    definition,
                    enabled: true,
                    comment: None,
                }
            })
            .collect();

        Ok(triggers)
    }

    async fn list_sequences(&self, schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT sequence_schema, sequence_name
                 FROM information_schema.sequences
                 WHERE sequence_schema = $1
                 ORDER BY sequence_name",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT sequence_schema, sequence_name
                 FROM information_schema.sequences
                 WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND sequence_schema NOT LIKE 'pg_temp_%'
                   AND sequence_schema NOT LIKE 'pg_toast_temp_%'
                 ORDER BY sequence_schema, sequence_name",
                &[],
            )
            .await?
        };

        let sequences = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                SequenceInfo {
                    name: display_name,
                    schema: Some(schema_name),
                    data_type: "bigint".to_string(),
                    start_value: 1,
                    min_value: 1,
                    max_value: i64::MAX,
                    increment_by: 1,
                    current_value: None,
                    owner: None,
                    comment: None,
                }
            })
            .collect();

        Ok(sequences)
    }

    async fn list_types(&self, schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT
                    n.nspname,
                    t.typname,
                    t.typtype,
                    r.rolname,
                    (
                        SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)
                        FROM pg_enum e
                        WHERE e.enumtypid = t.oid
                    ) AS enum_values
                 FROM pg_type t
                 JOIN pg_namespace n ON t.typnamespace = n.oid
                 LEFT JOIN pg_roles r ON r.oid = t.typowner
                 LEFT JOIN pg_class c ON c.oid = t.typrelid
                 WHERE n.nspname = $1
                   AND t.typtype IN ('e', 'd', 'r', 'c')
                   AND (t.typrelid = 0 OR c.relkind = 'c')
                 ORDER BY t.typname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT
                    n.nspname,
                    t.typname,
                    t.typtype,
                    r.rolname,
                    (
                        SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)
                        FROM pg_enum e
                        WHERE e.enumtypid = t.oid
                    ) AS enum_values
                 FROM pg_type t
                 JOIN pg_namespace n ON t.typnamespace = n.oid
                 LEFT JOIN pg_roles r ON r.oid = t.typowner
                 LEFT JOIN pg_class c ON c.oid = t.typrelid
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND t.typtype IN ('e', 'd', 'r', 'c')
                   AND (t.typrelid = 0 OR c.relkind = 'c')
                 ORDER BY n.nspname, t.typname",
                &[],
            )
            .await?
        };

        let types = result
            .rows
            .iter()
            .map(|row| {
                let schema_name = row
                    .get(0)
                    .and_then(|v| v.as_str())
                    .unwrap_or("public")
                    .to_string();
                let name = row
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let typtype = row.get(2).and_then(|v| v.as_str()).unwrap_or("e");
                let owner = row.get(3).and_then(|v| v.as_str()).map(str::to_string);
                let values = row.get(4).and_then(|value| value.as_string_array());

                let display_name = if schema.is_some() {
                    name
                } else {
                    schema_qualified_name(&schema_name, &name)
                };
                let type_kind = postgres_type_kind_from_typtype(typtype);

                TypeInfo {
                    name: display_name,
                    schema: Some(schema_name),
                    type_kind,
                    values,
                    definition: None,
                    owner,
                    comment: None,
                }
            })
            .collect();

        Ok(types)
    }

    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        let (schema, relation_name) =
            resolve_relation_identifiers(object.schema.as_deref(), object.name.as_str(), "public");
        let name = relation_name.as_str();
        let schema_param = zqlz_core::Value::String(schema.to_string());
        let name_param = zqlz_core::Value::String(name.to_string());

        match object.object_type {
            ObjectType::Table => {
                // Columns with precise type information via pg_catalog functions.
                // format_type() returns the full type string including length/precision
                // modifiers (e.g. "character varying(255)" instead of just "character varying"),
                // which information_schema.columns does not reliably provide.
                let col_result = self
                    .query(
                        "SELECT
                             a.attname,
                             pg_catalog.format_type(a.atttypid, a.atttypmod),
                             a.attnotnull,
                             pg_catalog.pg_get_expr(d.adbin, d.adrelid),
                             a.attidentity,
                             a.attgenerated
                         FROM pg_catalog.pg_attribute a
                         LEFT JOIN pg_catalog.pg_attrdef d
                             ON a.attrelid = d.adrelid AND a.attnum = d.adnum
                         WHERE a.attrelid = (
                             SELECT c.oid FROM pg_catalog.pg_class c
                             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                             WHERE c.relname = $2 AND n.nspname = $1
                         )
                         AND a.attnum > 0 AND NOT a.attisdropped
                         ORDER BY a.attnum",
                        &[schema_param.clone(), name_param.clone()],
                    )
                    .await?;

                if col_result.rows.is_empty() {
                    return Err(ZqlzError::NotFound(format!(
                        "Table '{}.{}' not found",
                        schema, name
                    )));
                }

                let mut parts: Vec<String> = col_result
                    .rows
                    .iter()
                    .map(|row| {
                        let col_name = row
                            .get(0)
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let col_type = row
                            .get(1)
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let not_null = row.get(2).and_then(|v| v.as_bool()).unwrap_or(false);
                        let col_default =
                            row.get(3).and_then(|v| v.as_str()).map(|s| s.to_string());
                        let identity = row.get(4).and_then(|v| v.as_str()).unwrap_or("");
                        let generated = row.get(5).and_then(|v| v.as_str()).unwrap_or("");

                        let mut def = format!("    \"{}\" {}", col_name, col_type);

                        // Identity and generated columns have their own constraint syntax
                        // that supersedes a plain DEFAULT clause.
                        if identity == "a" {
                            def.push_str(" GENERATED ALWAYS AS IDENTITY");
                        } else if identity == "d" {
                            def.push_str(" GENERATED BY DEFAULT AS IDENTITY");
                        } else if generated == "s" {
                            if let Some(expr) = &col_default {
                                def.push_str(&format!(" GENERATED ALWAYS AS ({}) STORED", expr));
                            }
                        } else {
                            if let Some(default) = &col_default {
                                def.push_str(&format!(" DEFAULT {}", default));
                            }
                            if not_null {
                                def.push_str(" NOT NULL");
                            }
                        }

                        def
                    })
                    .collect();

                // pg_get_constraintdef() produces authoritative constraint DDL for all
                // inline constraint types: primary key, unique, check, and foreign key.
                let con_result = self
                    .query(
                        "SELECT pg_catalog.pg_get_constraintdef(c.oid, true)
                         FROM pg_catalog.pg_constraint c
                         JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
                         JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                         WHERE t.relname = $2 AND n.nspname = $1
                           AND c.contype IN ('p', 'u', 'c', 'f')
                         ORDER BY c.contype, c.conname",
                        &[schema_param.clone(), name_param.clone()],
                    )
                    .await?;

                for row in &con_result.rows {
                    if let Some(condef) = row.get(0).and_then(|v| v.as_str()) {
                        parts.push(format!("    {}", condef));
                    }
                }

                let qualified = format!("\"{}\".\"{}\"", schema, name);
                let mut ddl = format!("CREATE TABLE {} (\n{}\n);", qualified, parts.join(",\n"));

                // Non-PK indexes are emitted as separate CREATE INDEX statements
                // following the CREATE TABLE, which is the canonical pg_dump format.
                let idx_result = self
                    .query(
                        "SELECT pg_catalog.pg_get_indexdef(i.indexrelid, 0, true)
                         FROM pg_catalog.pg_index i
                         JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
                         JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                         WHERE t.relname = $2 AND n.nspname = $1
                           AND NOT i.indisprimary
                         ORDER BY i.indexrelid",
                        &[schema_param, name_param],
                    )
                    .await?;

                for row in &idx_result.rows {
                    if let Some(idx_ddl) = row.get(0).and_then(|v| v.as_str()) {
                        ddl.push('\n');
                        ddl.push_str(idx_ddl);
                        ddl.push(';');
                    }
                }

                Ok(ddl)
            }

            ObjectType::View | ObjectType::MaterializedView => {
                let is_materialized = object.object_type == ObjectType::MaterializedView;
                let kind = if is_materialized {
                    "MATERIALIZED VIEW"
                } else {
                    "VIEW"
                };

                let result = self
                    .query(
                        "SELECT pg_catalog.pg_get_viewdef(c.oid, true)
                         FROM pg_catalog.pg_class c
                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                         WHERE c.relname = $2 AND n.nspname = $1",
                        &[schema_param, name_param],
                    )
                    .await?;

                let view_def = result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!("{} '{}.{}' not found", kind, schema, name))
                    })?;

                // Materialized views do not support OR REPLACE.
                let or_replace = if is_materialized { "" } else { "OR REPLACE " };
                Ok(format!(
                    "CREATE {}{}  \"{}\".\"{}\" AS\n{};",
                    or_replace,
                    kind,
                    schema,
                    name,
                    view_def.trim_end()
                ))
            }

            ObjectType::Function | ObjectType::Procedure => {
                let result = if let Some(signature) = object
                    .signature
                    .as_deref()
                    .map(str::trim)
                    .filter(|signature| !signature.is_empty())
                {
                    self.query(
                        "SELECT pg_catalog.pg_get_functiondef(p.oid)
                         FROM pg_catalog.pg_proc p
                         JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                         WHERE p.proname = $2
                           AND n.nspname = $1
                           AND pg_catalog.pg_get_function_identity_arguments(p.oid) = $3
                         LIMIT 1",
                        &[
                            schema_param.clone(),
                            name_param.clone(),
                            zqlz_core::Value::String(signature.to_string()),
                        ],
                    )
                    .await?
                } else {
                    self.query(
                        "SELECT pg_catalog.pg_get_functiondef(p.oid)
                         FROM pg_catalog.pg_proc p
                         JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                         WHERE p.proname = $2 AND n.nspname = $1
                         LIMIT 1",
                        &[schema_param, name_param],
                    )
                    .await?
                };

                result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!(
                            "Function or procedure '{}.{}' not found",
                            schema, name
                        ))
                    })
            }

            ObjectType::Trigger => {
                let scoped_result = self
                    .query(
                        "SELECT pg_catalog.pg_get_triggerdef(t.oid, true)
                         FROM pg_catalog.pg_trigger t
                         JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                         WHERE t.tgname = $2 AND n.nspname = $1
                           AND NOT t.tgisinternal",
                        &[schema_param, name_param],
                    )
                    .await?;

                if let Some(definition) = scoped_result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                {
                    return Ok(format!("{};", definition));
                }

                let fallback_result = self
                    .query(
                        "SELECT pg_catalog.pg_get_triggerdef(t.oid, true)
                         FROM pg_catalog.pg_trigger t
                         JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                         WHERE t.tgname = $1
                           AND NOT t.tgisinternal
                           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                           AND n.nspname NOT LIKE 'pg_temp_%'
                           AND n.nspname NOT LIKE 'pg_toast_temp_%'
                         ORDER BY n.nspname, c.relname
                         LIMIT 1",
                        &[zqlz_core::Value::String(name.to_string())],
                    )
                    .await?;

                fallback_result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                    .map(|s| format!("{};", s))
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!(
                            "Trigger '{}' not found in schema '{}'",
                            name, schema
                        ))
                    })
            }

            ObjectType::Index => {
                let result = self
                    .query(
                        "SELECT pg_catalog.pg_get_indexdef(c.oid, 0, true)
                         FROM pg_catalog.pg_class c
                         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                         WHERE c.relname = $2 AND n.nspname = $1 AND c.relkind = 'i'",
                        &[schema_param, name_param],
                    )
                    .await?;

                result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                    .map(|s| format!("{};", s))
                    .ok_or_else(|| {
                        ZqlzError::NotFound(format!("Index '{}.{}' not found", schema, name))
                    })
            }

            ObjectType::Sequence => {
                let result = self
                    .query(
                        "SELECT data_type::text, start_value, min_value, max_value,
                                increment_by, cycle, cache_size
                         FROM pg_catalog.pg_sequences
                         WHERE sequencename = $2 AND schemaname = $1",
                        &[schema_param, name_param],
                    )
                    .await?;

                let row = result.rows.first().ok_or_else(|| {
                    ZqlzError::NotFound(format!("Sequence '{}.{}' not found", schema, name))
                })?;

                let data_type = row.get(0).and_then(|v| v.as_str()).unwrap_or("bigint");
                let start = row.get(1).and_then(|v| v.as_i64()).unwrap_or(1);
                let min_val = row.get(2).and_then(|v| v.as_i64()).unwrap_or(1);
                let max_val = row.get(3).and_then(|v| v.as_i64());
                let increment = row.get(4).and_then(|v| v.as_i64()).unwrap_or(1);
                let cycle = row.get(5).and_then(|v| v.as_bool()).unwrap_or(false);
                let cache = row.get(6).and_then(|v| v.as_i64()).unwrap_or(1);

                let max_clause = match max_val {
                    Some(max) => format!("MAXVALUE {}", max),
                    None => "NO MAXVALUE".to_string(),
                };
                let cycle_clause = if cycle { "CYCLE" } else { "NO CYCLE" };

                Ok(format!(
                    "CREATE SEQUENCE \"{}\".\"{}\" AS {} INCREMENT BY {} MINVALUE {} {} START {} CACHE {} {};",
                    schema,
                    name,
                    data_type,
                    increment,
                    min_val,
                    max_clause,
                    start,
                    cache,
                    cycle_clause
                ))
            }

            ObjectType::Type => {
                // Enum types store their labels in pg_enum ordered by enumsortorder.
                let enum_result = self
                    .query(
                        "SELECT string_agg(quote_literal(e.enumlabel), ', '
                                ORDER BY e.enumsortorder)
                         FROM pg_catalog.pg_type t
                         JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                         JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
                         WHERE t.typname = $2 AND n.nspname = $1 AND t.typtype = 'e'
                         GROUP BY t.oid",
                        &[schema_param.clone(), name_param.clone()],
                    )
                    .await?;

                if let Some(labels) = enum_result
                    .rows
                    .first()
                    .and_then(|row| row.get(0))
                    .and_then(|v| v.as_str())
                {
                    return Ok(format!(
                        "CREATE TYPE \"{}\".\"{}\" AS ENUM ({});",
                        schema, name, labels
                    ));
                }

                // Composite types are backed by a pg_class row of relkind = 'c'.
                let comp_result = self
                    .query(
                        "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
                         FROM pg_catalog.pg_type t
                         JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                         JOIN pg_catalog.pg_class c ON c.oid = t.typrelid
                         JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                         WHERE t.typname = $2 AND n.nspname = $1 AND t.typtype = 'c'
                           AND a.attnum > 0 AND NOT a.attisdropped
                         ORDER BY a.attnum",
                        &[schema_param, name_param],
                    )
                    .await?;

                if !comp_result.rows.is_empty() {
                    let fields: Vec<String> = comp_result
                        .rows
                        .iter()
                        .filter_map(|row| {
                            let fname = row.get(0).and_then(|v| v.as_str())?;
                            let ftype = row.get(1).and_then(|v| v.as_str())?;
                            Some(format!("    \"{}\" {}", fname, ftype))
                        })
                        .collect();
                    return Ok(format!(
                        "CREATE TYPE \"{}\".\"{}\" AS (\n{}\n);",
                        schema,
                        name,
                        fields.join(",\n")
                    ));
                }

                Err(ZqlzError::NotFound(format!(
                    "Type '{}.{}' not found or is of an unsupported kind",
                    schema, name
                )))
            }

            _ => Err(ZqlzError::NotImplemented(format!(
                "DDL generation for {:?} is not supported in PostgreSQL",
                object.object_type
            ))),
        }
    }

    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        Ok(Vec::new())
    }

    #[tracing::instrument(skip(self))]
    async fn list_tables_extended(&self, schema: Option<&str>) -> Result<ObjectsPanelData> {
        let result = if let Some(schema) = schema {
            self.query(
                "SELECT
                    n.nspname AS schema_name,
                    c.oid,
                    c.relname AS name,
                    r.rolname AS owner,
                    COALESCE(c.relacl::text, '-') AS acl,
                    CASE c.relkind
                        WHEN 'r' THEN 'Normal'
                        WHEN 'v' THEN 'View'
                        WHEN 'm' THEN 'Materialized View'
                        WHEN 'f' THEN 'Foreign Table'
                        WHEN 'p' THEN 'Partitioned'
                        ELSE 'Other'
                    END AS table_type,
                    COALESCE(
                        (SELECT pn.nspname || '.' || p.relname
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         JOIN pg_namespace pn ON pn.oid = p.relnamespace
                         WHERE inh.inhrelid = c.oid
                         LIMIT 1),
                        '-'
                    ) AS partition_of,
                    COALESCE(s.n_live_tup, 0) AS row_count,
                    COALESCE(
                        (SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                         FROM pg_index i
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                         WHERE i.indrelid = c.oid AND i.indisprimary),
                        '-'
                    ) AS primary_key,
                    'No' AS has_oids,
                    COALESCE(fs.srvname, '-') AS foreign_server,
                    COALESCE(
                        (SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'schema_name'),
                        '-'
                    ) AS foreign_schema,
                    COALESCE(
                        (SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'table_name'),
                        '-'
                    ) AS foreign_table,
                    COALESCE(array_to_string(c.reloptions, ', '), '-') AS options,
                    COALESCE(
                        (SELECT string_agg(p.relname, ', ')
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         WHERE inh.inhrelid = c.oid),
                        '-'
                    ) AS inherits_tables,
                    (SELECT count(*) FROM pg_inherits inh WHERE inh.inhparent = c.oid) AS inherited_tables_count,
                    COALESCE(
                        (SELECT split_part(option_value, '=', 2)
                         FROM unnest(c.reloptions) AS option_value
                         WHERE option_value LIKE 'fillfactor=%'
                         LIMIT 1),
                        '-1'
                    ) AS fill_factor,
                    CASE WHEN c.relpersistence = 'u' THEN 'Yes' ELSE 'No' END AS unlogged,
                    CASE WHEN n.nspname IN ('pg_catalog', 'information_schema') THEN 'Yes' ELSE 'No' END AS system_table,
                    COALESCE(obj_description(c.oid, 'pg_class'), '-') AS comment
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_roles r ON r.oid = c.relowner
                 LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                 LEFT JOIN pg_foreign_table ft ON ft.ftrelid = c.oid
                 LEFT JOIN pg_foreign_server fs ON fs.oid = ft.ftserver
                 WHERE n.nspname = $1
                   AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
                 ORDER BY c.relname",
                &[zqlz_core::Value::String(schema.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT
                    n.nspname AS schema_name,
                    c.oid,
                    c.relname AS name,
                    r.rolname AS owner,
                    COALESCE(c.relacl::text, '-') AS acl,
                    CASE c.relkind
                        WHEN 'r' THEN 'Normal'
                        WHEN 'v' THEN 'View'
                        WHEN 'm' THEN 'Materialized View'
                        WHEN 'f' THEN 'Foreign Table'
                        WHEN 'p' THEN 'Partitioned'
                        ELSE 'Other'
                    END AS table_type,
                    COALESCE(
                        (SELECT pn.nspname || '.' || p.relname
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         JOIN pg_namespace pn ON pn.oid = p.relnamespace
                         WHERE inh.inhrelid = c.oid
                         LIMIT 1),
                        '-'
                    ) AS partition_of,
                    COALESCE(s.n_live_tup, 0) AS row_count,
                    COALESCE(
                        (SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                         FROM pg_index i
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                         WHERE i.indrelid = c.oid AND i.indisprimary),
                        '-'
                    ) AS primary_key,
                    'No' AS has_oids,
                    COALESCE(fs.srvname, '-') AS foreign_server,
                    COALESCE(
                        (SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'schema_name'),
                        '-'
                    ) AS foreign_schema,
                    COALESCE(
                        (SELECT option_value FROM pg_options_to_table(ft.ftoptions) WHERE option_name = 'table_name'),
                        '-'
                    ) AS foreign_table,
                    COALESCE(array_to_string(c.reloptions, ', '), '-') AS options,
                    COALESCE(
                        (SELECT string_agg(p.relname, ', ')
                         FROM pg_inherits inh
                         JOIN pg_class p ON p.oid = inh.inhparent
                         WHERE inh.inhrelid = c.oid),
                        '-'
                    ) AS inherits_tables,
                    (SELECT count(*) FROM pg_inherits inh WHERE inh.inhparent = c.oid) AS inherited_tables_count,
                    COALESCE(
                        (SELECT split_part(option_value, '=', 2)
                         FROM unnest(c.reloptions) AS option_value
                         WHERE option_value LIKE 'fillfactor=%'
                         LIMIT 1),
                        '-1'
                    ) AS fill_factor,
                    CASE WHEN c.relpersistence = 'u' THEN 'Yes' ELSE 'No' END AS unlogged,
                    CASE WHEN n.nspname IN ('pg_catalog', 'information_schema') THEN 'Yes' ELSE 'No' END AS system_table,
                    COALESCE(obj_description(c.oid, 'pg_class'), '-') AS comment
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_roles r ON r.oid = c.relowner
                 LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                 LEFT JOIN pg_foreign_table ft ON ft.ftrelid = c.oid
                 LEFT JOIN pg_foreign_server fs ON fs.oid = ft.ftserver
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
                 ORDER BY n.nspname, c.relname",
                &[],
            )
            .await?
        };

        let columns = Self::postgres_table_objects_panel_columns();

        // Column indices in the query result
        let col_ids = [
            "schema_name",
            "oid",
            "name",
            "owner",
            "acl",
            "table_type",
            "partition_of",
            "row_count",
            "primary_key",
            "has_oids",
            "foreign_server",
            "foreign_schema",
            "foreign_table",
            "options",
            "inherits_tables",
            "inherited_tables_count",
            "fill_factor",
            "unlogged",
            "system_table",
            "comment",
        ];

        let build_metadata_values = |schema_name: String,
                                     display_name: String,
                                     owner: Option<String>,
                                     table_type: &str,
                                     options: Option<String>| {
            let mut values = std::collections::BTreeMap::new();
            values.insert("schema_name".to_string(), schema_name);
            values.insert("oid".to_string(), "-".to_string());
            values.insert("name".to_string(), display_name);
            values.insert(
                "owner".to_string(),
                owner.unwrap_or_else(|| "-".to_string()),
            );
            values.insert("table_type".to_string(), table_type.to_string());
            values.insert("partitioned".to_string(), "-".to_string());
            values.insert("row_count".to_string(), "-".to_string());
            values.insert("size".to_string(), "-".to_string());
            values.insert("primary_key".to_string(), "-".to_string());
            values.insert("foreign_server".to_string(), "-".to_string());
            values.insert(
                "options".to_string(),
                options.unwrap_or_else(|| "-".to_string()),
            );
            values.insert("inherits_tables".to_string(), "-".to_string());
            values.insert("inherited_tables_count".to_string(), "-".to_string());
            values.insert("unlogged".to_string(), "-".to_string());
            values.insert("system_table".to_string(), "No".to_string());
            values.insert("comment".to_string(), "-".to_string());
            values
        };

        let mut rows: Vec<ObjectsPanelRow> = result
            .rows
            .iter()
            .map(|row| {
                let mut values = std::collections::BTreeMap::new();

                for (query_idx, col_id) in col_ids.iter().enumerate() {
                    let display_value = row
                        .get(query_idx)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".to_string());

                    let display_value = if display_value == "NULL" {
                        "-".to_string()
                    } else {
                        display_value
                    };

                    values.insert(col_id.to_string(), display_value);
                }

                let name = values.get("name").cloned().unwrap_or_default();
                let schema_name = values
                    .get("schema_name")
                    .cloned()
                    .unwrap_or_else(|| "public".to_string());
                let object_name = name.clone();
                let object_schema = schema_name.clone();

                let display_name = if schema.is_some() {
                    name.clone()
                } else {
                    schema_qualified_name(&schema_name, &name)
                };

                let table_type_str = values.get("table_type").map(|s| s.as_str()).unwrap_or("");
                let object_type = match table_type_str {
                    "View" => "view",
                    "Materialized View" => "materialized_view",
                    "Foreign Table" => "foreign_table",
                    "Partitioned" => "partitioned_table",
                    _ => "table",
                };

                ObjectsPanelRow {
                    name: display_name,
                    schema: Some(schema_name),
                    object_type: object_type.to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new(object_type, object_name)
                            .with_schema_option(Some(object_schema)),
                    ),
                    values,
                    redis_database_index: None,
                    key_value_info: None,
                }
            })
            .collect();

        let function_result = if let Some(schema_name) = schema {
            self.query(
                "SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), r.rolname,
                        l.lanname,
                        pg_catalog.format_type(p.prorettype, NULL),
                        CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' ELSE 'VOLATILE' END,
                        CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END,
                        CASE WHEN p.proretset THEN 'Yes' ELSE 'No' END,
                        CASE WHEN p.proisstrict THEN 'Yes' ELSE 'No' END,
                        p.procost::text,
                        p.prorows::text,
                        COALESCE(array_to_string(p.proconfig, ', '), '-'),
                        COALESCE(obj_description(p.oid, 'pg_proc'), '-')
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 LEFT JOIN pg_roles r ON r.oid = p.proowner
                 JOIN pg_language l ON l.oid = p.prolang
                 WHERE n.nspname = $1
                   AND p.prokind = 'f'
                 ORDER BY n.nspname, p.proname",
                &[zqlz_core::Value::String(schema_name.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), r.rolname,
                        l.lanname,
                        pg_catalog.format_type(p.prorettype, NULL),
                        CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' ELSE 'VOLATILE' END,
                        CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END,
                        CASE WHEN p.proretset THEN 'Yes' ELSE 'No' END,
                        CASE WHEN p.proisstrict THEN 'Yes' ELSE 'No' END,
                        p.procost::text,
                        p.prorows::text,
                        COALESCE(array_to_string(p.proconfig, ', '), '-'),
                        COALESCE(obj_description(p.oid, 'pg_proc'), '-')
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 LEFT JOIN pg_roles r ON r.oid = p.proowner
                 JOIN pg_language l ON l.oid = p.prolang
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND p.prokind = 'f'
                 ORDER BY n.nspname, p.proname",
                &[],
            )
            .await?
        };

        for row in &function_result.rows {
            let schema_name = row
                .get(0)
                .and_then(|value| value.as_str())
                .unwrap_or("public")
                .to_string();
            let function_name = row
                .get(1)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let signature = row
                .get(2)
                .and_then(|value| value.as_str())
                .map(str::to_string)
                .filter(|value| !value.is_empty());
            let owner = row
                .get(3)
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let language = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
            let return_type = row.get(5).and_then(|value| value.as_str()).unwrap_or("-");
            let volatility = row.get(6).and_then(|value| value.as_str()).unwrap_or("-");
            let security = row.get(7).and_then(|value| value.as_str()).unwrap_or("-");
            let returns_set = row.get(8).and_then(|value| value.as_str()).unwrap_or("-");
            let strict = row.get(9).and_then(|value| value.as_str()).unwrap_or("-");
            let estimated_cost = row.get(10).and_then(|value| value.as_str()).unwrap_or("-");
            let estimated_rows = row.get(11).and_then(|value| value.as_str()).unwrap_or("-");
            let configuration_parameters =
                row.get(12).and_then(|value| value.as_str()).unwrap_or("-");
            let comment = row.get(13).and_then(|value| value.as_str()).unwrap_or("-");

            let display_name = {
                let base = if schema.is_some() {
                    function_name.clone()
                } else {
                    schema_qualified_name(&schema_name, &function_name)
                };
                signature
                    .as_ref()
                    .map(|value| format!("{}({})", base, value))
                    .unwrap_or(base)
            };

            let mut values = std::collections::BTreeMap::new();
            values.insert("name".to_string(), display_name.clone());
            values.insert("function_type".to_string(), "Function".to_string());
            values.insert(
                "owner".to_string(),
                owner.clone().unwrap_or_else(|| "-".to_string()),
            );
            values.insert(
                "parameter".to_string(),
                signature.clone().unwrap_or_else(|| "-".to_string()),
            );
            values.insert("language".to_string(), language.to_string());
            values.insert("return_type".to_string(), return_type.to_string());
            values.insert("volatility".to_string(), volatility.to_string());
            values.insert("security".to_string(), security.to_string());
            values.insert("returns_set".to_string(), returns_set.to_string());
            values.insert("strict".to_string(), strict.to_string());
            values.insert("estimated_cost".to_string(), estimated_cost.to_string());
            values.insert("estimated_rows".to_string(), estimated_rows.to_string());
            values.insert(
                "configuration_parameters".to_string(),
                configuration_parameters.to_string(),
            );
            values.insert("comment".to_string(), comment.to_string());

            rows.push(ObjectsPanelRow {
                name: display_name,
                schema: Some(schema_name.clone()),
                object_type: "function".to_string(),
                object_ref: Some(Self::postgres_routine_object_ref(
                    "function",
                    function_name,
                    schema_name,
                    signature,
                )),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        let procedure_result = if let Some(schema_name) = schema {
            self.query(
                "SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), r.rolname,
                        l.lanname,
                        pg_catalog.format_type(p.prorettype, NULL),
                        CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' ELSE 'VOLATILE' END,
                        CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END,
                        CASE WHEN p.proretset THEN 'Yes' ELSE 'No' END,
                        CASE WHEN p.proisstrict THEN 'Yes' ELSE 'No' END,
                        p.procost::text,
                        p.prorows::text,
                        COALESCE(array_to_string(p.proconfig, ', '), '-'),
                        COALESCE(obj_description(p.oid, 'pg_proc'), '-')
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 LEFT JOIN pg_roles r ON r.oid = p.proowner
                 JOIN pg_language l ON l.oid = p.prolang
                 WHERE n.nspname = $1
                   AND p.prokind = 'p'
                 ORDER BY n.nspname, p.proname",
                &[zqlz_core::Value::String(schema_name.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), r.rolname,
                        l.lanname,
                        pg_catalog.format_type(p.prorettype, NULL),
                        CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' ELSE 'VOLATILE' END,
                        CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END,
                        CASE WHEN p.proretset THEN 'Yes' ELSE 'No' END,
                        CASE WHEN p.proisstrict THEN 'Yes' ELSE 'No' END,
                        p.procost::text,
                        p.prorows::text,
                        COALESCE(array_to_string(p.proconfig, ', '), '-'),
                        COALESCE(obj_description(p.oid, 'pg_proc'), '-')
                 FROM pg_proc p
                 JOIN pg_namespace n ON n.oid = p.pronamespace
                 LEFT JOIN pg_roles r ON r.oid = p.proowner
                 JOIN pg_language l ON l.oid = p.prolang
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                   AND p.prokind = 'p'
                 ORDER BY n.nspname, p.proname",
                &[],
            )
            .await?
        };

        for row in &procedure_result.rows {
            let schema_name = row
                .get(0)
                .and_then(|value| value.as_str())
                .unwrap_or("public")
                .to_string();
            let procedure_name = row
                .get(1)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let signature = row
                .get(2)
                .and_then(|value| value.as_str())
                .map(str::to_string)
                .filter(|value| !value.is_empty());
            let owner = row
                .get(3)
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let language = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
            let return_type = row.get(5).and_then(|value| value.as_str()).unwrap_or("-");
            let volatility = row.get(6).and_then(|value| value.as_str()).unwrap_or("-");
            let security = row.get(7).and_then(|value| value.as_str()).unwrap_or("-");
            let returns_set = row.get(8).and_then(|value| value.as_str()).unwrap_or("-");
            let strict = row.get(9).and_then(|value| value.as_str()).unwrap_or("-");
            let estimated_cost = row.get(10).and_then(|value| value.as_str()).unwrap_or("-");
            let estimated_rows = row.get(11).and_then(|value| value.as_str()).unwrap_or("-");
            let configuration_parameters =
                row.get(12).and_then(|value| value.as_str()).unwrap_or("-");
            let comment = row.get(13).and_then(|value| value.as_str()).unwrap_or("-");

            let display_name = {
                let base = if schema.is_some() {
                    procedure_name.clone()
                } else {
                    schema_qualified_name(&schema_name, &procedure_name)
                };
                signature
                    .as_ref()
                    .map(|value| format!("{}({})", base, value))
                    .unwrap_or(base)
            };

            let mut values = std::collections::BTreeMap::new();
            values.insert("name".to_string(), display_name.clone());
            values.insert("function_type".to_string(), "Procedure".to_string());
            values.insert(
                "owner".to_string(),
                owner.clone().unwrap_or_else(|| "-".to_string()),
            );
            values.insert(
                "parameter".to_string(),
                signature.clone().unwrap_or_else(|| "-".to_string()),
            );
            values.insert("language".to_string(), language.to_string());
            values.insert("return_type".to_string(), return_type.to_string());
            values.insert("volatility".to_string(), volatility.to_string());
            values.insert("security".to_string(), security.to_string());
            values.insert("returns_set".to_string(), returns_set.to_string());
            values.insert("strict".to_string(), strict.to_string());
            values.insert("estimated_cost".to_string(), estimated_cost.to_string());
            values.insert("estimated_rows".to_string(), estimated_rows.to_string());
            values.insert(
                "configuration_parameters".to_string(),
                configuration_parameters.to_string(),
            );
            values.insert("comment".to_string(), comment.to_string());

            rows.push(ObjectsPanelRow {
                name: display_name,
                schema: Some(schema_name.clone()),
                object_type: "procedure".to_string(),
                object_ref: Some(Self::postgres_routine_object_ref(
                    "procedure",
                    procedure_name,
                    schema_name,
                    signature,
                )),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        let trigger_rows = if let Some(schema_name) = schema {
            self.query(
                "SELECT n.nspname, t.tgname, c.relname
                 FROM pg_trigger t
                 JOIN pg_class c ON c.oid = t.tgrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE NOT t.tgisinternal
                   AND n.nspname = $1
                 ORDER BY n.nspname, t.tgname",
                &[zqlz_core::Value::String(schema_name.to_string())],
            )
            .await?
        } else {
            self.query(
                "SELECT n.nspname, t.tgname, c.relname
                 FROM pg_trigger t
                 JOIN pg_class c ON c.oid = t.tgrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE NOT t.tgisinternal
                   AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                 ORDER BY n.nspname, t.tgname",
                &[],
            )
            .await?
        };

        for row in &trigger_rows.rows {
            let schema_name = row
                .get(0)
                .and_then(|value| value.as_str())
                .unwrap_or("public")
                .to_string();
            let trigger_name = row
                .get(1)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let table_name = row
                .get(2)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();

            let display_name = if schema.is_some() {
                trigger_name.clone()
            } else {
                schema_qualified_name(&schema_name, &trigger_name)
            };

            let values = build_metadata_values(
                schema_name.clone(),
                display_name.clone(),
                None,
                "Trigger",
                Some(table_name.clone()),
            );

            rows.push(ObjectsPanelRow {
                name: display_name,
                schema: Some(schema_name.clone()),
                object_type: "trigger".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("trigger", trigger_name)
                        .with_schema_option(Some(schema_name))
                        .with_signature_option(Some(table_name)),
                ),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        let sequences = self.list_sequences(schema).await?;
        for sequence in sequences {
            let schema_name = sequence
                .schema
                .clone()
                .unwrap_or_else(|| "public".to_string());
            let name = sequence.name.clone();

            let values = build_metadata_values(
                schema_name.clone(),
                name.clone(),
                sequence.owner.clone(),
                "Sequence",
                Some(sequence.data_type.clone()),
            );

            let object_name = name
                .split('.')
                .next_back()
                .map(str::to_string)
                .unwrap_or_else(|| name.clone());

            rows.push(ObjectsPanelRow {
                name,
                schema: Some(schema_name.clone()),
                object_type: "sequence".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("sequence", object_name)
                        .with_schema_option(Some(schema_name)),
                ),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        let types = self.list_types(schema).await?;
        for data_type in types {
            let schema_name = data_type
                .schema
                .clone()
                .unwrap_or_else(|| "public".to_string());
            let name = data_type.name.clone();

            let values = build_metadata_values(
                schema_name.clone(),
                name.clone(),
                data_type.owner.clone(),
                Self::postgres_type_kind_label(data_type.type_kind),
                Some(format!("{:?}", data_type.type_kind)),
            );

            let object_name = name
                .split('.')
                .next_back()
                .map(str::to_string)
                .unwrap_or_else(|| name.clone());

            rows.push(ObjectsPanelRow {
                name,
                schema: Some(schema_name.clone()),
                object_type: Self::postgres_type_kind_id(data_type.type_kind).to_string(),
                object_ref: Some(Self::postgres_type_object_ref(
                    data_type.type_kind,
                    object_name,
                    schema_name,
                )),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        let schema_infos = self.list_schemas().await?;
        for schema_info in schema_infos {
            if let Some(target_schema) = schema
                && target_schema != schema_info.name
            {
                continue;
            }

            let values = build_metadata_values(
                schema_info.name.clone(),
                schema_info.name.clone(),
                schema_info.owner.clone(),
                "Schema",
                None,
            );

            rows.push(ObjectsPanelRow {
                name: schema_info.name.clone(),
                schema: None,
                object_type: "schema".to_string(),
                object_ref: Some(ObjectsPanelObjectRef::new("schema", schema_info.name)),
                values,
                redis_database_index: None,
                key_value_info: None,
            });
        }

        match self
            .query(
                "SELECT e.extname, n.nspname, r.rolname
                 FROM pg_extension e
                 JOIN pg_namespace n ON n.oid = e.extnamespace
                 LEFT JOIN pg_roles r ON r.oid = e.extowner
                 ORDER BY n.nspname, e.extname",
                &[],
            )
            .await
        {
            Ok(extension_result) => {
                for row in &extension_result.rows {
                    let schema_name = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .unwrap_or("public")
                        .to_string();

                    if let Some(target_schema) = schema
                        && target_schema != schema_name
                    {
                        continue;
                    }

                    let extension_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let owner = row
                        .get(2)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);

                    let display_name = if schema.is_some() {
                        extension_name.clone()
                    } else {
                        schema_qualified_name(&schema_name, &extension_name)
                    };

                    let values = build_metadata_values(
                        schema_name.clone(),
                        display_name.clone(),
                        owner,
                        "Extension",
                        None,
                    );

                    rows.push(ObjectsPanelRow {
                        name: display_name,
                        schema: Some(schema_name.clone()),
                        object_type: "extension".to_string(),
                        object_ref: Some(
                            ObjectsPanelObjectRef::new("extension", extension_name)
                                .with_schema_option(Some(schema_name)),
                        ),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to list PostgreSQL extensions for objects panel");
            }
        }

        match self
            .query(
                "SELECT s.srvname, f.fdwname, r.rolname
                 FROM pg_foreign_server s
                 JOIN pg_foreign_data_wrapper f ON f.oid = s.srvfdw
                 LEFT JOIN pg_roles r ON r.oid = s.srvowner
                 ORDER BY s.srvname",
                &[],
            )
            .await
        {
            Ok(server_result) => {
                for row in &server_result.rows {
                    let server_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let fdw_name = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);
                    let owner = row
                        .get(2)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);

                    let values = build_metadata_values(
                        "-".to_string(),
                        server_name.clone(),
                        owner,
                        "Foreign Server",
                        fdw_name,
                    );

                    rows.push(ObjectsPanelRow {
                        name: server_name.clone(),
                        schema: None,
                        object_type: "foreign_server".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new("foreign_server", server_name)),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL foreign servers for objects panel"
                );
            }
        }

        match self
            .query(
                "SELECT f.fdwname, r.rolname
                 FROM pg_foreign_data_wrapper f
                 LEFT JOIN pg_roles r ON r.oid = f.fdwowner
                 ORDER BY f.fdwname",
                &[],
            )
            .await
        {
            Ok(fdw_result) => {
                for row in &fdw_result.rows {
                    let fdw_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let owner = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);

                    let values = build_metadata_values(
                        "-".to_string(),
                        fdw_name.clone(),
                        owner,
                        "Foreign Data Wrapper",
                        None,
                    );

                    rows.push(ObjectsPanelRow {
                        name: fdw_name.clone(),
                        schema: None,
                        object_type: "foreign_data_wrapper".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new(
                            "foreign_data_wrapper",
                            fdw_name,
                        )),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL foreign data wrappers for objects panel"
                );
            }
        }

        let policy_query = if let Some(schema_name) = schema {
            self.query(
                "SELECT n.nspname, c.relname, p.polname
                 FROM pg_policy p
                 JOIN pg_class c ON c.oid = p.polrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1
                 ORDER BY n.nspname, c.relname, p.polname",
                &[zqlz_core::Value::String(schema_name.to_string())],
            )
            .await
        } else {
            self.query(
                "SELECT n.nspname, c.relname, p.polname
                 FROM pg_policy p
                 JOIN pg_class c ON c.oid = p.polrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                 ORDER BY n.nspname, c.relname, p.polname",
                &[],
            )
            .await
        };

        match policy_query {
            Ok(policy_result) => {
                for row in &policy_result.rows {
                    let schema_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("public")
                        .to_string();
                    let table_name = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let policy_name = row
                        .get(2)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();

                    let display_name = if schema.is_some() {
                        policy_name.clone()
                    } else {
                        schema_qualified_name(&schema_name, &policy_name)
                    };

                    let values = build_metadata_values(
                        schema_name.clone(),
                        display_name.clone(),
                        None,
                        "Policy",
                        Some(table_name),
                    );

                    rows.push(ObjectsPanelRow {
                        name: display_name,
                        schema: Some(schema_name.clone()),
                        object_type: "policy".to_string(),
                        object_ref: Some(
                            ObjectsPanelObjectRef::new("policy", policy_name)
                                .with_schema_option(Some(schema_name)),
                        ),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to list PostgreSQL policies for objects panel");
            }
        }

        match self
            .query(
                "SELECT pubname, puballtables
                 FROM pg_publication
                 ORDER BY pubname",
                &[],
            )
            .await
        {
            Ok(publication_result) => {
                for row in &publication_result.rows {
                    let publication_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let all_tables = row
                        .get(1)
                        .and_then(|value| value.as_bool())
                        .map(|value| if value { "all tables" } else { "filtered" })
                        .unwrap_or("-")
                        .to_string();

                    let values = build_metadata_values(
                        "-".to_string(),
                        publication_name.clone(),
                        None,
                        "Publication",
                        Some(all_tables),
                    );

                    rows.push(ObjectsPanelRow {
                        name: publication_name.clone(),
                        schema: None,
                        object_type: "publication".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new(
                            "publication",
                            publication_name,
                        )),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL publications for objects panel"
                );
            }
        }

        match self
            .query(
                "SELECT subname, subenabled
                 FROM pg_subscription
                 ORDER BY subname",
                &[],
            )
            .await
        {
            Ok(subscription_result) => {
                for row in &subscription_result.rows {
                    let subscription_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let enabled = row
                        .get(1)
                        .and_then(|value| value.as_bool())
                        .map(|value| if value { "enabled" } else { "disabled" })
                        .unwrap_or("-")
                        .to_string();

                    let values = build_metadata_values(
                        "-".to_string(),
                        subscription_name.clone(),
                        None,
                        "Subscription",
                        Some(enabled),
                    );

                    rows.push(ObjectsPanelRow {
                        name: subscription_name.clone(),
                        schema: None,
                        object_type: "subscription".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new(
                            "subscription",
                            subscription_name,
                        )),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL subscriptions for objects panel"
                );
            }
        }

        match self
            .query(
                "SELECT evtname, evtenabled, evtevent
                 FROM pg_event_trigger
                 ORDER BY evtname",
                &[],
            )
            .await
        {
            Ok(event_trigger_result) => {
                for row in &event_trigger_result.rows {
                    let event_trigger_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let event_type = row
                        .get(2)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);

                    let values = build_metadata_values(
                        "-".to_string(),
                        event_trigger_name.clone(),
                        None,
                        "Event Trigger",
                        event_type,
                    );

                    rows.push(ObjectsPanelRow {
                        name: event_trigger_name.clone(),
                        schema: None,
                        object_type: "event_trigger".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new(
                            "event_trigger",
                            event_trigger_name,
                        )),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL event triggers for objects panel"
                );
            }
        }

        match self
            .query(
                "SELECT lanname, lanpltrusted
                 FROM pg_language
                 ORDER BY lanname",
                &[],
            )
            .await
        {
            Ok(language_result) => {
                for row in &language_result.rows {
                    let language_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let trusted = row
                        .get(1)
                        .and_then(|value| value.as_bool())
                        .map(|value| if value { "trusted" } else { "untrusted" })
                        .unwrap_or("-")
                        .to_string();

                    let values = build_metadata_values(
                        "-".to_string(),
                        language_name.clone(),
                        None,
                        "Language",
                        Some(trusted),
                    );

                    rows.push(ObjectsPanelRow {
                        name: language_name.clone(),
                        schema: None,
                        object_type: "language".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new("language", language_name)),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to list PostgreSQL languages for objects panel");
            }
        }

        let collation_query = if let Some(schema_name) = schema {
            self.query(
                "SELECT n.nspname, c.collname
                 FROM pg_collation c
                 JOIN pg_namespace n ON n.oid = c.collnamespace
                 WHERE n.nspname = $1
                 ORDER BY n.nspname, c.collname",
                &[zqlz_core::Value::String(schema_name.to_string())],
            )
            .await
        } else {
            self.query(
                "SELECT n.nspname, c.collname
                 FROM pg_collation c
                 JOIN pg_namespace n ON n.oid = c.collnamespace
                 WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   AND n.nspname NOT LIKE 'pg_temp_%'
                   AND n.nspname NOT LIKE 'pg_toast_temp_%'
                 ORDER BY n.nspname, c.collname",
                &[],
            )
            .await
        };

        match collation_query {
            Ok(collation_result) => {
                for row in &collation_result.rows {
                    let schema_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("public")
                        .to_string();
                    let collation_name = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();

                    let display_name = if schema.is_some() {
                        collation_name.clone()
                    } else {
                        schema_qualified_name(&schema_name, &collation_name)
                    };

                    let values = build_metadata_values(
                        schema_name.clone(),
                        display_name.clone(),
                        None,
                        "Collation",
                        None,
                    );

                    rows.push(ObjectsPanelRow {
                        name: display_name,
                        schema: Some(schema_name.clone()),
                        object_type: "collation".to_string(),
                        object_ref: Some(
                            ObjectsPanelObjectRef::new("collation", collation_name)
                                .with_schema_option(Some(schema_name)),
                        ),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to list PostgreSQL collations for objects panel");
            }
        }

        match self
            .query(
                "SELECT spcname, pg_get_userbyid(spcowner)
                 FROM pg_tablespace
                 ORDER BY spcname",
                &[],
            )
            .await
        {
            Ok(tablespace_result) => {
                for row in &tablespace_result.rows {
                    let tablespace_name = row
                        .get(0)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string();
                    let owner = row
                        .get(1)
                        .and_then(|value| value.as_str())
                        .map(str::to_string);

                    let values = build_metadata_values(
                        "-".to_string(),
                        tablespace_name.clone(),
                        owner,
                        "Tablespace",
                        None,
                    );

                    rows.push(ObjectsPanelRow {
                        name: tablespace_name.clone(),
                        schema: None,
                        object_type: "tablespace".to_string(),
                        object_ref: Some(ObjectsPanelObjectRef::new("tablespace", tablespace_name)),
                        values,
                        redis_database_index: None,
                        key_value_info: None,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to list PostgreSQL tablespaces for objects panel"
                );
            }
        }

        rows.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(ObjectsPanelData { columns, rows })
    }

    async fn list_objects_panel_data_for_kind(
        &self,
        schema: Option<&str>,
        kind_id: &str,
    ) -> Result<ObjectsPanelData> {
        match kind_id {
            "table" => {
                let mut data = self.postgres_relation_objects_panel_data(schema).await?;
                for row in &mut data.rows {
                    if matches!(
                        row.object_type.as_str(),
                        "partitioned_table" | "foreign_table"
                    ) {
                        row.object_type = "table".to_string();
                        if let Some(object_ref) = row.object_ref.as_ref() {
                            row.object_ref = Some(
                                ObjectsPanelObjectRef::new("table", object_ref.name.clone())
                                    .with_schema_option(object_ref.schema.clone()),
                            );
                        }
                    }
                }
                Ok(data.for_kind_and_scope(kind_id, None))
            }
            "partitioned_table" | "foreign_table" | "view" | "materialized_view" => Ok(self
                .postgres_relation_objects_panel_data(schema)
                .await?
                .for_kind_and_scope(kind_id, None)),
            "function" => Ok(self
                .postgres_routine_objects_panel_data(schema, kind_id, "f", "Function")
                .await?),
            "procedure" => Ok(self
                .postgres_routine_objects_panel_data(schema, kind_id, "p", "Procedure")
                .await?),
            "trigger" => self.postgres_trigger_objects_panel_data(schema).await,
            "extension" => self.postgres_extension_objects_panel_data(schema).await,
            "enum" | "domain" | "range" | "composite_type" => Ok(self
                .list_tables_extended(schema)
                .await?
                .for_kind_and_scope(kind_id, None)),
            _ => Ok(self
                .list_tables_extended(schema)
                .await?
                .for_kind_and_scope(kind_id, None)),
        }
    }

    async fn list_objects_panel_manifest(
        &self,
        _schema: Option<&str>,
    ) -> Result<ObjectsPanelManifest> {
        Ok(Self::postgres_objects_panel_manifest())
    }

    async fn object_form_spec(
        &self,
        request: &ObjectFormSpecRequest,
    ) -> Result<Option<ObjectFormSpec>> {
        let object_ref = request.object_ref.as_ref();
        let schema = object_ref
            .and_then(|object_ref| object_ref.schema.clone())
            .or_else(|| Some("public".to_string()));
        let name = object_ref.map(|object_ref| object_ref.name.clone());

        match (request.kind_id.as_str(), request.mode) {
            ("enum", ObjectFormMode::Create) => Ok(Some(Self::enum_form_spec(
                request.mode,
                schema,
                name,
                Vec::new(),
            ))),
            ("enum", ObjectFormMode::Edit | ObjectFormMode::Drop) => {
                let labels = if let Some(object_ref) = object_ref {
                    self.list_types(object_ref.schema.as_deref())
                        .await?
                        .into_iter()
                        .find(|info| {
                            info.type_kind == TypeKind::Enum
                                && info
                                    .name
                                    .split('.')
                                    .next_back()
                                    .is_some_and(|type_name| type_name == object_ref.name)
                        })
                        .and_then(|info| info.values)
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                Ok(Some(Self::enum_form_spec(
                    request.mode,
                    schema,
                    name,
                    labels,
                )))
            }
            ("domain", ObjectFormMode::Create) => Ok(Some(Self::domain_form_spec(
                request.mode,
                schema,
                name,
                None,
                false,
                None,
                None,
            ))),
            ("domain", ObjectFormMode::Edit | ObjectFormMode::Drop) => {
                let mut base_type = None;
                let mut not_null = false;
                let mut default_value = None;
                let mut check_expression = None;

                if let Some(object_ref) = object_ref {
                    let result = self
                        .query(
                            "SELECT
                                pg_catalog.format_type(t.typbasetype, t.typtypmod),
                                t.typnotnull,
                                pg_catalog.pg_get_expr(t.typdefaultbin, 0),
                                (
                                    SELECT pg_catalog.pg_get_constraintdef(c.oid)
                                    FROM pg_catalog.pg_constraint c
                                    WHERE c.contypid = t.oid AND c.contype = 'c'
                                    ORDER BY c.conname
                                    LIMIT 1
                                )
                             FROM pg_catalog.pg_type t
                             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                             WHERE n.nspname = $1 AND t.typname = $2 AND t.typtype = 'd'",
                            &[
                                zqlz_core::Value::String(
                                    object_ref
                                        .schema
                                        .clone()
                                        .unwrap_or_else(|| "public".to_string()),
                                ),
                                zqlz_core::Value::String(object_ref.name.clone()),
                            ],
                        )
                        .await?;

                    if let Some(row) = result.rows.first() {
                        base_type = row
                            .get(0)
                            .and_then(|value| value.as_str())
                            .map(str::to_string);
                        not_null = row
                            .get(1)
                            .and_then(|value| value.as_bool())
                            .unwrap_or(false);
                        default_value = row
                            .get(2)
                            .and_then(|value| value.as_str())
                            .map(str::to_string);
                        check_expression = row
                            .get(3)
                            .and_then(|value| value.as_str())
                            .map(str::to_string);
                    }
                }

                Ok(Some(Self::domain_form_spec(
                    request.mode,
                    schema,
                    name,
                    base_type,
                    not_null,
                    default_value,
                    check_expression,
                )))
            }
            ("procedure", ObjectFormMode::Create) => {
                Ok(Some(Self::procedure_form_spec(request.mode, schema, name)))
            }
            _ => Ok(None),
        }
    }

    async fn generate_object_form_ddl(
        &self,
        request: &ObjectFormDdlRequest,
    ) -> Result<Vec<String>> {
        let schema = Self::object_form_string(&request.values, "schema");
        let schema = if schema.is_empty() { "public" } else { schema };
        let name = Self::object_form_string(&request.values, "name");
        if name.is_empty() {
            return Err(ZqlzError::Schema("Object name is required".to_string()));
        }

        let qualified_name = self.qualified_type_name(Some(schema), name);

        match (request.kind_id.as_str(), request.mode) {
            ("enum", ObjectFormMode::Create) => {
                let labels = Self::object_form_string_list(&request.values, "labels");
                if labels.is_empty() {
                    return Err(ZqlzError::Schema("Enum labels are required".to_string()));
                }
                Self::validate_unique_values("Enum", &labels)?;
                let label_sql = labels
                    .iter()
                    .map(|label| Self::quote_literal(label))
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut statements = vec![format!(
                    "CREATE TYPE {qualified_name} AS ENUM ({label_sql});"
                )];
                let comment = Self::object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    statements.push(format!(
                        "COMMENT ON TYPE {qualified_name} IS {};",
                        Self::quote_literal(comment)
                    ));
                }
                Ok(statements)
            }
            ("enum", ObjectFormMode::Edit) => {
                let labels = Self::object_form_string_list(&request.values, "labels");
                if labels.is_empty() {
                    return Err(ZqlzError::Schema("Enum labels are required".to_string()));
                }
                Self::validate_unique_values("Enum", &labels)?;
                let current_labels = if let Some(object_ref) = request.object_ref.as_ref() {
                    self.list_types(object_ref.schema.as_deref())
                        .await?
                        .into_iter()
                        .find(|info| {
                            info.type_kind == TypeKind::Enum
                                && info
                                    .name
                                    .split('.')
                                    .next_back()
                                    .is_some_and(|type_name| type_name == object_ref.name)
                        })
                        .and_then(|info| info.values)
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                let labels_to_append = Self::enum_labels_to_append(&labels, &current_labels);
                let mut statements = labels_to_append
                    .iter()
                    .map(|label| {
                        format!(
                            "ALTER TYPE {qualified_name} ADD VALUE {};",
                            Self::quote_literal(label)
                        )
                    })
                    .collect::<Vec<_>>();
                let comment = Self::object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    statements.push(format!(
                        "COMMENT ON TYPE {qualified_name} IS {};",
                        Self::quote_literal(comment)
                    ));
                }
                Ok(statements)
            }
            ("enum", ObjectFormMode::Drop) => Ok(vec![format!("DROP TYPE {qualified_name};")]),
            ("domain", ObjectFormMode::Create) => {
                let base_type = Self::object_form_string(&request.values, "base_type");
                if base_type.is_empty() {
                    return Err(ZqlzError::Schema(
                        "Domain base type is required".to_string(),
                    ));
                }
                let mut ddl = format!("CREATE DOMAIN {qualified_name} AS {base_type}");
                let default_value = Self::object_form_string(&request.values, "default");
                if !default_value.is_empty() {
                    ddl.push_str(&format!(" DEFAULT {default_value}"));
                }
                if Self::object_form_bool(&request.values, "not_null") {
                    ddl.push_str(" NOT NULL");
                }
                let check = Self::object_form_string(&request.values, "check");
                if !check.is_empty() {
                    ddl.push_str(&format!(" CHECK ({check})"));
                }
                ddl.push(';');
                let mut statements = vec![ddl];
                let comment = Self::object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    statements.push(format!(
                        "COMMENT ON DOMAIN {qualified_name} IS {};",
                        Self::quote_literal(comment)
                    ));
                }
                Ok(statements)
            }
            ("domain", ObjectFormMode::Edit) => {
                let mut statements = Vec::new();
                let default_value = Self::object_form_string(&request.values, "default");
                if default_value.is_empty() {
                    statements.push(format!("ALTER DOMAIN {qualified_name} DROP DEFAULT;"));
                } else {
                    statements.push(format!(
                        "ALTER DOMAIN {qualified_name} SET DEFAULT {default_value};"
                    ));
                }
                if Self::object_form_bool(&request.values, "not_null") {
                    statements.push(format!("ALTER DOMAIN {qualified_name} SET NOT NULL;"));
                } else {
                    statements.push(format!("ALTER DOMAIN {qualified_name} DROP NOT NULL;"));
                }
                let check = Self::object_form_string(&request.values, "check");
                if !check.is_empty() {
                    let constraint_name = format!("{}_check", name);
                    statements.push(format!(
                        "ALTER DOMAIN {qualified_name} ADD CONSTRAINT {} CHECK ({check});",
                        self.quote_identifier(&constraint_name)
                    ));
                }
                let comment = Self::object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    statements.push(format!(
                        "COMMENT ON DOMAIN {qualified_name} IS {};",
                        Self::quote_literal(comment)
                    ));
                }
                Ok(statements)
            }
            ("domain", ObjectFormMode::Drop) => Ok(vec![format!("DROP DOMAIN {qualified_name};")]),
            ("procedure", ObjectFormMode::Create) => {
                let language = Self::object_form_string(&request.values, "language");
                if language.is_empty() {
                    return Err(ZqlzError::Schema(
                        "Procedure language is required".to_string(),
                    ));
                }
                let body = Self::object_form_string(&request.values, "body");
                if body.is_empty() {
                    return Err(ZqlzError::Schema("Procedure body is required".to_string()));
                }
                let arguments =
                    Self::object_form_string_list(&request.values, "arguments").join(", ");
                let mut statements = vec![format!(
                    "CREATE OR REPLACE PROCEDURE {qualified_name}({arguments})\nLANGUAGE {}\nAS $$\n{body}\n$$;",
                    self.quote_identifier(language)
                )];
                let comment = Self::object_form_string(&request.values, "comment");
                if !comment.is_empty() {
                    statements.push(format!(
                        "COMMENT ON PROCEDURE {qualified_name}({arguments}) IS {};",
                        Self::quote_literal(comment)
                    ));
                }
                Ok(statements)
            }
            _ => Err(ZqlzError::NotSupported(format!(
                "Object form DDL is not supported for {}",
                request.kind_id
            ))),
        }
    }
}

fn schema_qualified_name(schema_name: &str, object_name: &str) -> String {
    format!("{schema_name}.{object_name}")
}

fn resolve_relation_identifiers(
    schema: Option<&str>,
    relation_name: &str,
    default_schema: &str,
) -> (String, String) {
    if let Some((schema_name, object_name)) = relation_name.split_once('.')
        && !schema_name.is_empty()
        && !object_name.is_empty()
    {
        return (schema_name.to_string(), object_name.to_string());
    }

    if let Some(schema_name) = schema {
        return (schema_name.to_string(), relation_name.to_string());
    }

    (default_schema.to_string(), relation_name.to_string())
}

fn postgres_type_kind_from_typtype(typtype: &str) -> TypeKind {
    match typtype {
        "d" => TypeKind::Domain,
        "r" => TypeKind::Range,
        "c" => TypeKind::Composite,
        "e" => TypeKind::Enum,
        _ => TypeKind::Base,
    }
}

fn parse_fk_action(action: &str) -> ForeignKeyAction {
    match action.to_uppercase().as_str() {
        "CASCADE" => ForeignKeyAction::Cascade,
        "SET NULL" => ForeignKeyAction::SetNull,
        "SET DEFAULT" => ForeignKeyAction::SetDefault,
        "RESTRICT" => ForeignKeyAction::Restrict,
        _ => ForeignKeyAction::NoAction,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_kind_actions(
        manifest: &ObjectsPanelManifest,
        kind_id: &str,
        expected_action_ids: &[&str],
        expected_default_row_action_id: Option<&str>,
    ) {
        let kind = manifest
            .object_kinds
            .iter()
            .find(|kind| kind.id == kind_id)
            .unwrap_or_else(|| panic!("missing kind '{}'", kind_id));

        let actual_action_ids: Vec<&str> = kind
            .row_actions
            .iter()
            .map(|action| action.id.as_str())
            .collect();

        assert_eq!(
            actual_action_ids, expected_action_ids,
            "unexpected action matrix for kind '{}'",
            kind_id
        );
        assert_eq!(
            kind.default_row_action_id.as_deref(),
            expected_default_row_action_id
        );
    }

    #[test]
    fn postgres_objects_panel_manifest_exposes_full_matrix() {
        let manifest = PostgresConnection::postgres_objects_panel_manifest();

        manifest
            .validate()
            .expect("PostgreSQL objects panel manifest should satisfy manifest invariants");

        let kind_ids: Vec<&str> = manifest
            .object_kinds
            .iter()
            .map(|kind| kind.id.as_str())
            .collect();
        assert_eq!(
            kind_ids,
            vec![
                "table",
                "partitioned_table",
                "foreign_table",
                "view",
                "materialized_view",
                "function",
                "procedure",
                "trigger",
                "sequence",
                "enum",
                "domain",
                "range",
                "composite_type",
                "type",
                "index",
                "schema",
                "extension",
                "foreign_server",
                "foreign_data_wrapper",
                "policy",
                "publication",
                "subscription",
                "event_trigger",
                "language",
                "collation",
                "tablespace",
            ]
        );

        let toolbar_action_ids: Vec<&str> = manifest
            .toolbar_actions
            .iter()
            .map(|action| action.id.as_str())
            .collect();
        assert_eq!(
            toolbar_action_ids,
            vec![
                "refresh",
                "new_table",
                "new_view",
                "new_enum",
                "new_domain",
                "import",
                "export"
            ]
        );

        let column_ids_for = |kind_id: &str| -> Vec<&str> {
            manifest
                .object_kinds
                .iter()
                .find(|kind| kind.id == kind_id)
                .unwrap_or_else(|| panic!("missing kind '{}'", kind_id))
                .columns
                .iter()
                .map(|column| column.id.as_str())
                .collect()
        };
        assert_eq!(
            column_ids_for("table"),
            vec![
                "name",
                "oid",
                "owner",
                "acl",
                "table_type",
                "partition_of",
                "row_count",
                "primary_key",
                "has_oids",
                "foreign_server",
                "foreign_schema",
                "foreign_table",
                "options",
                "inherits_tables",
                "inherited_tables_count",
                "fill_factor",
                "unlogged",
                "system_table",
                "comment",
            ]
        );
        assert_eq!(
            column_ids_for("function"),
            vec![
                "name",
                "function_type",
                "owner",
                "parameter",
                "language",
                "return_type",
                "volatility",
                "security",
                "returns_set",
                "strict",
                "estimated_cost",
                "estimated_rows",
                "configuration_parameters",
                "comment",
            ]
        );
        assert_eq!(column_ids_for("procedure"), column_ids_for("function"));
        assert_eq!(
            column_ids_for("trigger"),
            vec![
                "name",
                "oid",
                "trigger_type",
                "table_name",
                "constraint",
                "fire",
                "for_each",
                "function_name",
                "function_schema",
                "deferrable",
                "initially_deferred",
                "comment",
            ]
        );
        assert_eq!(
            column_ids_for("extension"),
            vec![
                "name",
                "oid",
                "version",
                "schema_name",
                "owner",
                "relocatable",
                "config_tables",
                "conditions",
                "comment",
            ]
        );

        let table_actions = [
            "open",
            "design",
            "rename",
            "duplicate",
            "empty",
            "import",
            "export",
            "dump_sql_structure_data",
            "dump_sql_structure",
            "copy_name",
            "copy_qualified_name",
            "view_history",
            "delete",
            "refresh",
        ];
        let relation_actions = [
            "open",
            "design",
            "rename",
            "duplicate",
            "copy_name",
            "copy_qualified_name",
            "view_history",
            "export",
            "delete",
            "refresh",
        ];
        let routine_actions = [
            "open",
            "design",
            "copy_name",
            "copy_qualified_name",
            "view_history",
            "refresh",
        ];
        let sequence_actions = [
            "open",
            "design",
            "copy_name",
            "copy_qualified_name",
            "refresh",
        ];
        let enum_domain_actions = [
            "design",
            "copy_name",
            "copy_qualified_name",
            "delete",
            "refresh",
        ];
        let metadata_actions = ["copy_name", "copy_qualified_name", "refresh"];
        let extension_actions = [
            "open",
            "design",
            "copy_name",
            "copy_qualified_name",
            "refresh",
        ];

        for kind_id in ["table", "partitioned_table", "foreign_table"] {
            assert_kind_actions(&manifest, kind_id, &table_actions, Some("open"));
        }

        for kind_id in ["view", "materialized_view"] {
            assert_kind_actions(&manifest, kind_id, &relation_actions, Some("open"));
        }

        for kind_id in ["function", "procedure", "trigger"] {
            assert_kind_actions(&manifest, kind_id, &routine_actions, Some("open"));
        }

        assert_kind_actions(&manifest, "sequence", &sequence_actions, Some("open"));
        assert_kind_actions(&manifest, "enum", &enum_domain_actions, Some("design"));
        assert_kind_actions(&manifest, "domain", &enum_domain_actions, Some("design"));

        for kind_id in [
            "range",
            "composite_type",
            "type",
            "index",
            "schema",
            "foreign_server",
            "foreign_data_wrapper",
            "policy",
            "publication",
            "subscription",
            "event_trigger",
            "language",
            "collation",
            "tablespace",
        ] {
            assert_kind_actions(&manifest, kind_id, &metadata_actions, None);
        }
        assert_kind_actions(&manifest, "extension", &extension_actions, Some("open"));
    }

    #[test]
    fn postgres_type_kind_mapping_covers_custom_types() {
        assert_eq!(postgres_type_kind_from_typtype("e"), TypeKind::Enum);
        assert_eq!(postgres_type_kind_from_typtype("d"), TypeKind::Domain);
        assert_eq!(postgres_type_kind_from_typtype("r"), TypeKind::Range);
        assert_eq!(postgres_type_kind_from_typtype("c"), TypeKind::Composite);
        assert_eq!(postgres_type_kind_from_typtype("b"), TypeKind::Base);
    }

    #[test]
    fn postgres_type_kind_ids_are_first_class_object_kinds() {
        assert_eq!(
            PostgresConnection::postgres_type_kind_id(TypeKind::Enum),
            "enum"
        );
        assert_eq!(
            PostgresConnection::postgres_type_kind_id(TypeKind::Domain),
            "domain"
        );
        assert_eq!(
            PostgresConnection::postgres_type_kind_id(TypeKind::Range),
            "range"
        );
        assert_eq!(
            PostgresConnection::postgres_type_kind_id(TypeKind::Composite),
            "composite_type"
        );
        assert_eq!(
            PostgresConnection::postgres_type_kind_id(TypeKind::Base),
            "type"
        );
    }

    #[test]
    fn object_form_validation_rejects_duplicate_values() {
        let duplicate_values = vec!["active".to_string(), "active".to_string()];

        assert!(PostgresConnection::validate_unique_values("Enum", &duplicate_values).is_err());
    }

    #[test]
    fn enum_edit_accepts_full_list_or_append_only_list() {
        let current = vec!["email".to_string(), "sms".to_string()];
        let full_list = vec!["email".to_string(), "sms".to_string(), "push".to_string()];
        let append_only = vec!["push".to_string()];

        assert_eq!(
            PostgresConnection::enum_labels_to_append(&full_list, &current),
            vec!["push".to_string()]
        );
        assert_eq!(
            PostgresConnection::enum_labels_to_append(&append_only, &current),
            vec!["push".to_string()]
        );
    }

    #[test]
    fn procedure_create_form_has_executable_defaults() {
        let spec = PostgresConnection::procedure_form_spec(
            ObjectFormMode::Create,
            Some("public".to_string()),
            None,
        );

        assert_eq!(spec.kind_id, "procedure");
        let field_ids = spec.sections[0]
            .fields
            .iter()
            .map(|field| field.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            field_ids,
            vec!["schema", "name", "arguments", "language", "body", "comment"]
        );
    }

    #[test]
    fn postgres_columns_query_uses_format_type_for_native_column_types() {
        assert!(POSTGRES_COLUMNS_SQL.contains("pg_catalog.format_type(a.atttypid, a.atttypmod)"));
        assert!(POSTGRES_COLUMNS_SQL.contains("FROM pg_catalog.pg_attribute a"));
        assert!(!POSTGRES_COLUMNS_SQL.contains("WHEN c.data_type = 'USER-DEFINED'"));
    }

    #[test]
    fn postgres_routine_object_refs_preserve_overloaded_identity() {
        let integer_function_ref = PostgresConnection::postgres_routine_object_ref(
            "function",
            "get_user".to_string(),
            "public".to_string(),
            Some("integer".to_string()),
        );
        let text_function_ref = PostgresConnection::postgres_routine_object_ref(
            "function",
            "get_user".to_string(),
            "public".to_string(),
            Some("text".to_string()),
        );

        assert_eq!(integer_function_ref.kind_id, "function");
        assert_eq!(integer_function_ref.schema.as_deref(), Some("public"));
        assert_eq!(integer_function_ref.signature.as_deref(), Some("integer"));
        assert_eq!(
            integer_function_ref.identity_key,
            ObjectsPanelObjectRef::build_identity_key(
                "function",
                None,
                Some("public"),
                "get_user",
                Some("integer"),
            )
        );
        assert_ne!(
            integer_function_ref.identity_key,
            text_function_ref.identity_key
        );

        let procedure_ref = PostgresConnection::postgres_routine_object_ref(
            "procedure",
            "run_job".to_string(),
            "ops".to_string(),
            Some("bigint".to_string()),
        );

        assert_eq!(procedure_ref.signature.as_deref(), Some("bigint"));
        assert_eq!(
            procedure_ref.identity_key,
            ObjectsPanelObjectRef::build_identity_key(
                "procedure",
                None,
                Some("ops"),
                "run_job",
                Some("bigint"),
            )
        );
    }
}
