//! Objects-panel column layouts, row builders, and the panel manifest for
//! the `SchemaIntrospection` objects-panel methods.

use zqlz_core::{
    Connection, ObjectFormMode, ObjectsPanelAction, ObjectsPanelColumn, ObjectsPanelData,
    ObjectsPanelManifest, ObjectsPanelObjectKind, ObjectsPanelObjectRef, ObjectsPanelRow, Result,
};

use crate::PostgresConnection;

use super::schema_qualified_name;

impl PostgresConnection {
    pub(crate) fn postgres_table_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
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

    pub(crate) fn postgres_routine_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
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

    pub(crate) fn postgres_trigger_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
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

    pub(crate) fn postgres_extension_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
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

    pub(crate) fn postgres_generic_objects_panel_columns() -> Vec<ObjectsPanelColumn> {
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

    pub(crate) async fn postgres_relation_objects_panel_data(
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

    pub(crate) async fn postgres_routine_objects_panel_data(
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

    pub(crate) async fn postgres_trigger_objects_panel_data(
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

    pub(crate) async fn postgres_extension_objects_panel_data(
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
    pub(crate) fn postgres_routine_object_ref(
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
    pub(crate) fn postgres_objects_panel_manifest() -> ObjectsPanelManifest {
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
                .advanced()
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
            .advanced()
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
                .advanced()
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
                .advanced()
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
                .advanced()
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
                .advanced()
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
                .advanced()
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
                .advanced()
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
                .advanced()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn marks_only_internal_catalog_kinds_as_advanced() {
        let manifest = PostgresConnection::postgres_objects_panel_manifest();

        let advanced_kind_ids: BTreeSet<&str> = manifest
            .object_kinds
            .iter()
            .filter(|kind| kind.advanced)
            .map(|kind| kind.id.as_str())
            .collect();

        let expected: BTreeSet<&str> = [
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
        .into_iter()
        .collect();

        assert_eq!(advanced_kind_ids, expected);
    }
}
