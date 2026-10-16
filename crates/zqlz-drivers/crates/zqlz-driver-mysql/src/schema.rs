//! MySQL schema introspection implementation

use std::collections::HashMap;
use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, ConstraintInfo, ConstraintType, DatabaseInfo, DatabaseObject,
    Dependency, DependencyType, ForeignKeyAction, ForeignKeyInfo, FunctionInfo, IndexInfo,
    ObjectFormDdlRequest, ObjectFormField, ObjectFormFieldKind, ObjectFormMode, ObjectFormOption,
    ObjectFormSection, ObjectFormSpec, ObjectFormSpecRequest, ObjectFormValue, ObjectType,
    ObjectsPanelAction, ObjectsPanelColumn, ObjectsPanelData, ObjectsPanelManifest,
    ObjectsPanelObjectKind, ObjectsPanelObjectRef, ObjectsPanelRow, ParameterInfo, ParameterMode,
    PrimaryKeyInfo, ProcedureInfo, Result, SchemaInfo, SchemaIntrospection, SequenceInfo,
    TableDetails, TableInfo, TableType, TriggerEvent, TriggerForEach, TriggerInfo, TriggerTiming,
    TypeInfo, Value, ViewInfo, ZqlzError,
};

use crate::MySqlConnection;

/// Read a value as text, tolerating MySQL `GROUP_CONCAT`/blob results that come
/// back as raw bytes rather than a string.
fn mysql_text(value: Option<&zqlz_core::Value>) -> Option<String> {
    match value? {
        zqlz_core::Value::String(text) => Some(text.clone()),
        zqlz_core::Value::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
        _ => None,
    }
}

mod admin_rows;
mod catalog;
mod introspection;
mod objects_panel_fetchers;
mod panel_definitions;

pub(crate) use catalog::MySqlCatalog;
use panel_definitions::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MysqlServerFeatures {
    is_mariadb: bool,
    major: u16,
    minor: u16,
}

impl MysqlServerFeatures {
    fn supports_events(&self) -> bool {
        true
    }

    pub(crate) fn supports_sequences(&self) -> bool {
        self.is_mariadb
    }
}

fn parse_mysql_server_features(version: &str, version_comment: &str) -> MysqlServerFeatures {
    let version_lower = version.to_ascii_lowercase();
    let comment_lower = version_comment.to_ascii_lowercase();
    let is_mariadb = version_lower.contains("mariadb") || comment_lower.contains("mariadb");
    let numeric_prefix = version
        .split(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .find(|part| part.chars().any(|ch| ch.is_ascii_digit()))
        .unwrap_or(version);
    let mut parts = numeric_prefix.split('.');
    let major = parts
        .next()
        .and_then(|part| part.parse::<u16>().ok())
        .unwrap_or(0);
    let minor = parts
        .next()
        .and_then(|part| part.parse::<u16>().ok())
        .unwrap_or(0);

    MysqlServerFeatures {
        is_mariadb,
        major,
        minor,
    }
}

/// Schemas the server owns; listed like any other database but flagged.
fn mysql_is_system_schema(name: &str) -> bool {
    ["information_schema", "mysql", "performance_schema", "sys"]
        .iter()
        .any(|system| name.eq_ignore_ascii_case(system))
}

fn mysql_quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn mysql_quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn mysql_qualified_name(schema: Option<&str>, name: &str) -> String {
    match schema.filter(|schema| !schema.trim().is_empty()) {
        Some(schema) => format!(
            "{}.{}",
            mysql_quote_identifier(schema),
            mysql_quote_identifier(name)
        ),
        None => mysql_quote_identifier(name),
    }
}

/// Split a possibly-qualified object name into `(schema, name)`.
///
/// Callers reach the driver with the schema either in its own field or embedded in
/// `name` as `"schema.object"`. Quoting an embedded form as one identifier produces
/// `` `db.table` ``, which MySQL then resolves against the current database as
/// `db.db.table`. An explicit schema wins; a redundant prefix repeating it is dropped.
fn mysql_split_object_name<'a>(schema: Option<&'a str>, name: &'a str) -> (Option<&'a str>, &'a str) {
    let schema = schema.filter(|schema| !schema.trim().is_empty());

    if let Some(schema_name) = schema {
        return match name
            .strip_prefix(schema_name)
            .and_then(|rest| rest.strip_prefix('.'))
        {
            Some(bare_name) if !bare_name.is_empty() => (Some(schema_name), bare_name),
            _ => (Some(schema_name), name),
        };
    }

    match name.split_once('.') {
        Some((namespace, bare_name)) if !namespace.is_empty() && !bare_name.is_empty() => {
            (Some(namespace), bare_name)
        }
        _ => (None, name),
    }
}

/// Render a `DatabaseObject` as a properly qualified, quoted MySQL name.
fn mysql_object_qualified_name(object: &DatabaseObject) -> String {
    let (schema, name) = mysql_split_object_name(object.schema.as_deref(), &object.name);
    mysql_qualified_name(schema, name)
}


fn format_bytes_human_readable(bytes: i64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    const TB: f64 = GB * 1024.0;

    let bytes_f64 = bytes as f64;
    if bytes_f64 >= TB {
        format!("{:.1} TB", bytes_f64 / TB)
    } else if bytes_f64 >= GB {
        format!("{:.1} GB", bytes_f64 / GB)
    } else if bytes_f64 >= MB {
        format!("{:.1} MB", bytes_f64 / MB)
    } else if bytes_f64 >= KB {
        format!("{:.1} KB", bytes_f64 / KB)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mysql_form_values(
        values: &[(&str, ObjectFormValue)],
    ) -> std::collections::BTreeMap<String, ObjectFormValue> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    fn mysql_features() -> MysqlServerFeatures {
        MysqlServerFeatures {
            is_mariadb: false,
            major: 8,
            minor: 0,
        }
    }

    fn database_object(schema: Option<&str>, name: &str) -> DatabaseObject {
        DatabaseObject {
            object_type: ObjectType::Table,
            schema: schema.map(ToOwned::to_owned),
            name: name.to_string(),
            signature: None,
        }
    }

    #[test]
    fn qualified_name_splits_embedded_schema() {
        assert_eq!(
            mysql_object_qualified_name(&database_object(
                None,
                "seekunique-dev.accounting_payments"
            )),
            "`seekunique-dev`.`accounting_payments`"
        );
    }

    #[test]
    fn qualified_name_uses_explicit_schema() {
        assert_eq!(
            mysql_object_qualified_name(&database_object(
                Some("seekunique-dev"),
                "accounting_payments"
            )),
            "`seekunique-dev`.`accounting_payments`"
        );
    }

    #[test]
    fn qualified_name_drops_prefix_repeating_the_explicit_schema() {
        assert_eq!(
            mysql_object_qualified_name(&database_object(
                Some("seekunique-dev"),
                "seekunique-dev.accounting_payments"
            )),
            "`seekunique-dev`.`accounting_payments`"
        );
    }

    #[test]
    fn qualified_name_leaves_unqualified_names_alone() {
        assert_eq!(
            mysql_object_qualified_name(&database_object(None, "accounting_payments")),
            "`accounting_payments`"
        );
    }

    #[test]
    fn qualified_name_escapes_backticks() {
        assert_eq!(
            mysql_object_qualified_name(&database_object(Some("we`ird"), "ta`ble")),
            "`we``ird`.`ta``ble`"
        );
    }

    fn mariadb_features() -> MysqlServerFeatures {
        MysqlServerFeatures {
            is_mariadb: true,
            major: 10,
            minor: 11,
        }
    }

    #[test]
    fn mariadb_objects_panel_manifest_exposes_explicit_parity_actions() {
        let manifest = mysql_objects_panel_manifest(&mariadb_features());

        manifest
            .validate()
            .expect("MySQL objects panel manifest should satisfy manifest invariants");

        let kind_ids: Vec<&str> = manifest
            .object_kinds
            .iter()
            .map(|kind| kind.id.as_str())
            .collect();
        assert_eq!(
            kind_ids,
            vec![
                "database",
                "table",
                "view",
                "function",
                "procedure",
                "trigger",
                "event",
                "index",
                "constraint",
                "foreign_key",
                "partition",
                "sequence",
                "tablespace",
                "engine",
                "charset",
                "collation",
                "user",
                "role",
                "grant",
                "plugin",
                "variable",
                "status_variable",
                "process",
                "replica_status",
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
                "new_database",
                "new_table",
                "new_view",
                "new_function",
                "new_procedure",
                "new_event",
                "import",
                "export",
            ]
        );

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
                .expect("kind should be present");

            let actual_action_ids: Vec<&str> = kind
                .row_actions
                .iter()
                .map(|action| action.id.as_str())
                .collect();

            assert_eq!(actual_action_ids, expected_action_ids);
            assert_eq!(
                kind.default_row_action_id.as_deref(),
                expected_default_row_action_id
            );
        }

        assert_kind_actions(
            &manifest,
            "table",
            &[
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
            ],
            Some("open"),
        );
        assert_kind_actions(
            &manifest,
            "view",
            &[
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
            ],
            Some("open"),
        );
        assert_kind_actions(
            &manifest,
            "function",
            &[
                "open",
                "design",
                "copy_name",
                "copy_qualified_name",
                "view_history",
                "refresh",
            ],
            Some("open"),
        );
        assert_kind_actions(
            &manifest,
            "procedure",
            &[
                "open",
                "design",
                "copy_name",
                "copy_qualified_name",
                "view_history",
                "refresh",
            ],
            Some("open"),
        );
        assert_kind_actions(
            &manifest,
            "trigger",
            &[
                "open",
                "design",
                "copy_name",
                "copy_qualified_name",
                "view_history",
                "refresh",
            ],
            Some("open"),
        );
        assert_kind_actions(
            &manifest,
            "event",
            &[
                "open",
                "design",
                "delete",
                "copy_name",
                "copy_qualified_name",
                "view_history",
                "refresh",
            ],
            Some("open"),
        );
        for kind_id in [
            "database",
            "index",
            "constraint",
            "foreign_key",
            "partition",
            "sequence",
            "tablespace",
            "engine",
            "charset",
            "collation",
            "user",
            "role",
            "grant",
            "plugin",
            "variable",
            "status_variable",
            "process",
            "replica_status",
        ] {
            assert_kind_actions(
                &manifest,
                kind_id,
                &["open", "copy_name", "copy_qualified_name", "refresh"],
                Some("open"),
            );
        }
    }

    #[test]
    fn mysql_objects_panel_manifest_hides_mariadb_only_sequence_kind() {
        let manifest = mysql_objects_panel_manifest(&mysql_features());

        manifest
            .validate()
            .expect("MySQL objects panel manifest should satisfy manifest invariants");

        let kind_ids: Vec<&str> = manifest
            .object_kinds
            .iter()
            .map(|kind| kind.id.as_str())
            .collect();

        assert!(!kind_ids.contains(&"sequence"));
        assert_eq!(kind_ids.len(), 23);
    }

    #[test]
    fn mysql_objects_panel_routine_rows_preserve_kind_and_identity() {
        let function_row = mysql_function_row(
            FunctionInfo {
                schema: Some("analytics".to_string()),
                name: "do_work".to_string(),
                language: "SQL".to_string(),
                return_type: "integer".to_string(),
                parameters: vec![],
                definition: Some("RETURN 1".to_string()),
                owner: Some("root".to_string()),
                comment: Some("Example function".to_string()),
            },
            false,
        );
        assert_eq!(function_row.object_kind_id(), "function");
        assert_eq!(function_row.name, "do_work");
        assert_eq!(function_row.object_schema(), Some("analytics"));
        assert_eq!(
            function_row.values.get("name").map(String::as_str),
            Some("do_work")
        );
        assert_eq!(
            function_row
                .object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("function::::analytics::do_work::")
        );

        let procedure_row = mysql_procedure_row(
            ProcedureInfo {
                schema: Some("ops".to_string()),
                name: "run_job".to_string(),
                language: "SQL".to_string(),
                parameters: vec![],
                definition: Some("CALL work()".to_string()),
                owner: None,
                comment: None,
            },
            true,
        );
        assert_eq!(procedure_row.object_kind_id(), "procedure");
        assert_eq!(procedure_row.name, "ops.run_job");
        assert_eq!(procedure_row.object_schema(), Some("ops"));
        assert_eq!(
            procedure_row.values.get("name").map(String::as_str),
            Some("ops.run_job")
        );
        assert_eq!(
            procedure_row
                .object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("procedure::::ops::run_job::")
        );

        let trigger_row = mysql_trigger_row(
            TriggerInfo {
                schema: Some("public".to_string()),
                name: "users_audit".to_string(),
                table_name: "users".to_string(),
                timing: TriggerTiming::Before,
                events: vec![TriggerEvent::Insert, TriggerEvent::Truncate],
                definition: Some("SET NEW.updated_at = NOW()".to_string()),
                for_each: TriggerForEach::Row,
                enabled: true,
                comment: None,
            },
            false,
        );
        assert_eq!(trigger_row.object_kind_id(), "trigger");
        assert_eq!(trigger_row.object_name(), "users_audit");
        assert_eq!(
            trigger_row.values.get("event").map(String::as_str),
            Some("INSERT, TRUNCATE")
        );
        assert_eq!(
            trigger_row
                .object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("trigger::::public::users_audit::users")
        );
    }

    #[test]
    fn mysql_trigger_panel_row_exposes_catalog_metadata() {
        let row = zqlz_core::Row::new(
            vec![
                "TRIGGER_SCHEMA".to_string(),
                "TRIGGER_NAME".to_string(),
                "EVENT_OBJECT_TABLE".to_string(),
                "ACTION_TIMING".to_string(),
                "EVENT_MANIPULATION".to_string(),
                "ACTION_ORDER".to_string(),
                "ACTION_STATEMENT".to_string(),
                "ACTION_ORIENTATION".to_string(),
                "DEFINER".to_string(),
                "SQL_MODE".to_string(),
                "CREATED".to_string(),
                "CHARACTER_SET_CLIENT".to_string(),
                "COLLATION_CONNECTION".to_string(),
                "DATABASE_COLLATION".to_string(),
            ],
            vec![
                Value::String("app".to_string()),
                Value::String("users_touch".to_string()),
                Value::String("users".to_string()),
                Value::String("BEFORE".to_string()),
                Value::String("UPDATE".to_string()),
                Value::Int64(1),
                Value::String("SET NEW.updated_at = NOW()".to_string()),
                Value::String("ROW".to_string()),
                Value::String("root@localhost".to_string()),
                Value::String("STRICT_TRANS_TABLES".to_string()),
                Value::String("2026-05-03 12:00:00".to_string()),
                Value::String("utf8mb4".to_string()),
                Value::String("utf8mb4_0900_ai_ci".to_string()),
                Value::String("utf8mb4_0900_ai_ci".to_string()),
            ],
        );

        let trigger_row = mysql_trigger_panel_row(&row, true);

        assert_eq!(trigger_row.name, "app.users_touch");
        assert_eq!(trigger_row.object_name(), "users_touch");
        assert_eq!(
            trigger_row.values.get("definer").map(String::as_str),
            Some("root@localhost")
        );
        assert_eq!(
            trigger_row.values.get("action_order").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            trigger_row
                .object_ref
                .as_ref()
                .and_then(|object_ref| object_ref.signature.as_deref()),
            Some("users")
        );
        assert_eq!(
            trigger_row
                .object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("trigger::::app::users_touch::users")
        );
    }

    #[test]
    fn mysql_routine_parameters_label_formats_modes_and_defaults_empty() {
        assert_eq!(mysql_routine_parameters_label(&[]), "-");
        assert_eq!(
            mysql_routine_parameters_label(&[
                ParameterInfo {
                    name: Some("user_id".to_string()),
                    data_type: "bigint unsigned".to_string(),
                    mode: ParameterMode::In,
                    default_value: None,
                    ordinal: 1,
                },
                ParameterInfo {
                    name: Some("total".to_string()),
                    data_type: "decimal(10,2)".to_string(),
                    mode: ParameterMode::Out,
                    default_value: None,
                    ordinal: 2,
                },
            ]),
            "IN user_id bigint unsigned, OUT total decimal(10,2)"
        );
    }

    #[test]
    fn mysql_routine_panel_row_exposes_catalog_metadata() {
        let row = zqlz_core::Row::new(
            vec![
                "ROUTINE_SCHEMA".to_string(),
                "ROUTINE_NAME".to_string(),
                "DATA_TYPE".to_string(),
                "ROUTINE_DEFINITION".to_string(),
                "ROUTINE_COMMENT".to_string(),
                "DEFINER".to_string(),
                "SECURITY_TYPE".to_string(),
                "IS_DETERMINISTIC".to_string(),
                "SQL_DATA_ACCESS".to_string(),
                "SQL_MODE".to_string(),
                "CREATED".to_string(),
                "LAST_ALTERED".to_string(),
            ],
            vec![
                Value::String("app".to_string()),
                Value::String("score".to_string()),
                Value::String("bigint".to_string()),
                Value::String("RETURN user_id".to_string()),
                Value::String("User score".to_string()),
                Value::String("root@localhost".to_string()),
                Value::String("DEFINER".to_string()),
                Value::String("YES".to_string()),
                Value::String("READS SQL DATA".to_string()),
                Value::String("STRICT_TRANS_TABLES".to_string()),
                Value::String("2026-05-03 10:00:00".to_string()),
                Value::String("2026-05-03 11:00:00".to_string()),
            ],
        );

        let routine_row = mysql_routine_panel_row(
            "function",
            &row,
            vec![ParameterInfo {
                name: Some("user_id".to_string()),
                data_type: "bigint".to_string(),
                mode: ParameterMode::In,
                default_value: None,
                ordinal: 1,
            }],
            true,
        );

        assert_eq!(routine_row.name, "app.score");
        assert_eq!(routine_row.object_kind_id(), "function");
        assert_eq!(routine_row.object_name(), "score");
        assert_eq!(
            routine_row.values.get("parameters").map(String::as_str),
            Some("IN user_id bigint")
        );
        assert_eq!(
            routine_row
                .values
                .get("sql_data_access")
                .map(String::as_str),
            Some("READS SQL DATA")
        );
        assert_eq!(
            routine_row.values.get("created").map(String::as_str),
            Some("2026-05-03 10:00:00")
        );
        assert_eq!(
            routine_row
                .object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("function::::app::score::")
        );
    }

    #[test]
    fn mysql_metadata_rows_preserve_table_context_in_signature() {
        let row = mysql_metadata_row(
            "index",
            "idx_users_email".to_string(),
            Some("app".to_string()),
            Some("users".to_string()),
            "Index",
            "BTREE: email",
            "-",
            "-",
        );

        assert_eq!(row.object_kind_id(), "index");
        assert_eq!(
            row.object_ref
                .as_ref()
                .and_then(|object_ref| object_ref.signature.as_deref()),
            Some("users")
        );
        assert_eq!(
            row.object_ref
                .as_ref()
                .map(|object_ref| object_ref.identity_key.as_str()),
            Some("index::::app::idx_users_email::users")
        );
    }

    #[test]
    fn mysql_index_ddl_reconstructs_create_statement() {
        let ddl = mysql_index_ddl(
            "posts",
            &IndexInfo {
                name: "idx_posts_title".to_string(),
                columns: vec!["title".to_string(), "created_at".to_string()],
                is_unique: false,
                is_primary: false,
                index_type: "BTREE".to_string(),
                comment: Some("fast lookup".to_string()),
                column_descending: vec![false, true],
                ..Default::default()
            },
        );

        assert_eq!(
            ddl,
            "CREATE INDEX `idx_posts_title` ON `posts` (`title`, `created_at` DESC) USING BTREE COMMENT 'fast lookup';"
        );
    }

    #[test]
    fn mysql_index_ddl_handles_prefix_expression_and_primary_parts() {
        let index = |name: &str, is_primary: bool, columns: &[&str], include: &[&str]| IndexInfo {
            name: name.to_string(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            is_primary,
            is_unique: is_primary,
            index_type: "BTREE".to_string(),
            include_columns: include.iter().map(|c| c.to_string()).collect(),
            column_descending: vec![false; columns.len()],
            ..Default::default()
        };

        assert_eq!(
            mysql_index_ddl("t", &index("pfx", false, &["name", "age"], &["name(10)"])),
            "CREATE INDEX `pfx` ON `t` (`name`(10), `age`) USING BTREE;"
        );
        assert_eq!(
            mysql_index_ddl("t", &index("fx", false, &["(`a` + `b`)"], &[])),
            "CREATE INDEX `fx` ON `t` ((`a` + `b`)) USING BTREE;"
        );
        assert_eq!(
            mysql_index_ddl("t", &index("PRIMARY", true, &["a", "b"], &[])),
            "ALTER TABLE `t` ADD PRIMARY KEY (`a`, `b`);"
        );
    }

    #[test]
    fn mysql_constraint_ddl_reconstructs_unique_and_check() {
        let unique = mysql_constraint_ddl(
            "users",
            &ConstraintInfo {
                name: "uq_users_email".to_string(),
                constraint_type: ConstraintType::Unique,
                columns: vec!["email".to_string()],
                definition: None,
            },
        )
        .unwrap();
        assert_eq!(
            unique,
            "ALTER TABLE `users` ADD CONSTRAINT `uq_users_email` UNIQUE (`email`);"
        );

        let check = mysql_constraint_ddl(
            "users",
            &ConstraintInfo {
                name: "chk_users_age".to_string(),
                constraint_type: ConstraintType::Check,
                columns: Vec::new(),
                definition: Some("age >= 0".to_string()),
            },
        )
        .unwrap();
        assert_eq!(
            check,
            "ALTER TABLE `users` ADD CONSTRAINT `chk_users_age` CHECK (age >= 0);"
        );
    }

    #[test]
    fn mysql_foreign_key_ddl_reconstructs_alter_table_statement() {
        let ddl = mysql_foreign_key_ddl(
            "orders",
            &ForeignKeyInfo {
                name: "fk_orders_user".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_schema: Some("app".to_string()),
                referenced_columns: vec!["id".to_string()],
                on_update: ForeignKeyAction::Cascade,
                on_delete: ForeignKeyAction::Restrict,
                is_deferrable: false,
                initially_deferred: false,
            },
        );

        assert_eq!(
            ddl,
            "ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `app`.`users` (`id`) ON UPDATE CASCADE ON DELETE RESTRICT;"
        );
    }

    #[test]
    fn mysql_server_feature_parser_detects_mysql_and_mariadb() {
        let mysql = parse_mysql_server_features("8.0.36", "MySQL Community Server - GPL");
        assert!(!mysql.is_mariadb);
        assert_eq!((mysql.major, mysql.minor), (8, 0));
        assert!(mysql.supports_events());

        let mariadb = parse_mysql_server_features("10.11.7-MariaDB", "MariaDB Server");
        assert!(mariadb.is_mariadb);
        assert!(mariadb.supports_sequences());
    }

    #[test]
    fn mysql_function_form_sql_renders_create_statement() {
        let values = mysql_form_values(&[
            (
                "parameters",
                ObjectFormValue::StringList(vec![
                    "IN user_id BIGINT UNSIGNED".to_string(),
                    "IN flag TINYINT(1)".to_string(),
                ]),
            ),
            ("returns", ObjectFormValue::String("BIGINT".to_string())),
            ("deterministic", ObjectFormValue::Bool(true)),
            (
                "sql_data_access",
                ObjectFormValue::String("READS SQL DATA".to_string()),
            ),
            (
                "body",
                ObjectFormValue::String("RETURN user_id + flag".to_string()),
            ),
        ]);

        assert_eq!(
            mysql_function_form_sql(Some("analytics"), "score", &values).unwrap(),
            "CREATE FUNCTION `analytics`.`score`(IN user_id BIGINT UNSIGNED, IN flag TINYINT(1))\nRETURNS BIGINT\nDETERMINISTIC\nREADS SQL DATA\nRETURN user_id + flag;"
        );
    }

    #[test]
    fn mysql_procedure_form_sql_renders_create_statement() {
        let values = mysql_form_values(&[
            (
                "parameters",
                ObjectFormValue::StringList(vec!["IN job_id BIGINT".to_string()]),
            ),
            (
                "body",
                ObjectFormValue::String(
                    "BEGIN\n  UPDATE jobs SET ran_at = NOW() WHERE id = job_id;\nEND".to_string(),
                ),
            ),
        ]);

        assert_eq!(
            mysql_procedure_form_sql(Some("ops"), "run_job", &values).unwrap(),
            "CREATE PROCEDURE `ops`.`run_job`(IN job_id BIGINT)\nBEGIN\n  UPDATE jobs SET ran_at = NOW() WHERE id = job_id;\nEND;"
        );
    }

    #[test]
    fn mysql_trigger_form_sql_renders_defaults_and_qualified_table() {
        let values = mysql_form_values(&[
            ("table", ObjectFormValue::String("users".to_string())),
            (
                "body",
                ObjectFormValue::String("SET NEW.updated_at = NOW()".to_string()),
            ),
        ]);

        assert_eq!(
            mysql_trigger_form_sql(Some("app"), "users_touch", &values).unwrap(),
            "CREATE TRIGGER `app`.`users_touch`\nBEFORE INSERT ON `app`.`users`\nFOR EACH ROW\nSET NEW.updated_at = NOW();"
        );
    }

    #[test]
    fn mysql_routine_and_trigger_form_sql_validate_required_fields() {
        assert!(matches!(
            mysql_function_form_sql(
                None,
                "missing_return",
                &mysql_form_values(&[("body", ObjectFormValue::String("RETURN 1".to_string()))])
            ),
            Err(ZqlzError::Schema(message)) if message == "Function return type is required"
        ));
        assert!(matches!(
            mysql_procedure_form_sql(None, "missing_body", &mysql_form_values(&[])),
            Err(ZqlzError::Schema(message)) if message == "Procedure body is required"
        ));
        assert!(matches!(
            mysql_trigger_form_sql(
                None,
                "missing_table",
                &mysql_form_values(&[(
                    "body",
                    ObjectFormValue::String("SET @value = 1".to_string()),
                )])
            ),
            Err(ZqlzError::Schema(message)) if message == "Trigger table is required"
        ));
    }

    #[test]
    fn mysql_metadata_unavailable_row_is_visible_for_admin_catalog_errors() {
        assert!(mysql_metadata_kind_degrades_on_error("user"));
        assert!(mysql_metadata_kind_degrades_on_error("process"));
        assert!(!mysql_metadata_kind_degrades_on_error("index"));

        let row = mysql_metadata_unavailable_row(
            "user",
            &ZqlzError::Query("SELECT command denied".to_string()),
        );

        assert_eq!(row.object_kind_id(), "user");
        assert_eq!(row.name, "Metadata unavailable");
        assert_eq!(
            row.values.get("kind").map(String::as_str),
            Some("Privilege or catalog error")
        );
        assert_eq!(
            row.values.get("state").map(String::as_str),
            Some("Unavailable")
        );
        assert_eq!(
            row.values.get("detail").map(String::as_str),
            Some("Query error: SELECT command denied")
        );
    }
}
