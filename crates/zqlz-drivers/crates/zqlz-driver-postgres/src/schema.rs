//! PostgreSQL schema introspection implementation

mod catalog;
mod introspection;
mod object_forms;
mod objects_panel;

pub use catalog::PostgresCatalog;

use zqlz_core::TypeKind;

fn schema_qualified_name(schema_name: &str, object_name: &str) -> String {
    format!("{schema_name}.{object_name}")
}

/// An explicit schema means the caller already split the name, so a dot left in
/// it belongs to the name; only a repeated `schema.` prefix is dropped.
fn resolve_relation_identifiers(
    schema: Option<&str>,
    relation_name: &str,
    default_schema: &str,
) -> (String, String) {
    if let Some(schema_name) = schema.filter(|schema_name| !schema_name.is_empty()) {
        let object_name = relation_name
            .strip_prefix(schema_name)
            .and_then(|remainder| remainder.strip_prefix('.'))
            .filter(|remainder| !remainder.is_empty())
            .unwrap_or(relation_name);
        return (schema_name.to_string(), object_name.to_string());
    }

    if let Some((schema_name, object_name)) = relation_name.split_once('.')
        && !schema_name.is_empty()
        && !object_name.is_empty()
    {
        return (schema_name.to_string(), object_name.to_string());
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

#[cfg(test)]
mod tests {
    use super::*;
    use super::catalog::{
        postgres_all_schemas_columns_sql, postgres_all_schemas_foreign_keys_sql,
        postgres_columns_sql, postgres_foreign_keys_sql,
    };
    use crate::PostgresConnection;
    use zqlz_core::{ObjectFormMode, ObjectsPanelManifest, ObjectsPanelObjectRef};

    #[test]
    fn relation_identifiers_keep_dots_inside_a_scoped_name() {
        let owned = |schema: &str, name: &str| (schema.to_string(), name.to_string());
        assert_eq!(
            resolve_relation_identifiers(Some("gui_a"), "odd.name", "public"),
            owned("gui_a", "odd.name")
        );
        assert_eq!(
            resolve_relation_identifiers(Some("gui_a"), "gui_a.items", "public"),
            owned("gui_a", "items")
        );
        assert_eq!(
            resolve_relation_identifiers(None, "sales.items", "public"),
            owned("sales", "items")
        );
        assert_eq!(resolve_relation_identifiers(None, "items", "public"), owned("public", "items"));
    }

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
        let single = postgres_columns_sql(true);
        assert!(single.contains("pg_catalog.format_type(a.atttypid, a.atttypmod)"));
        assert!(single.contains("FROM pg_catalog.pg_attribute a"));
        assert!(!single.contains("WHEN c.data_type = 'USER-DEFINED'"));
    }

    #[test]
    fn postgres_bulk_columns_query_drops_the_relation_predicate_and_filters_relkind() {
        let single = postgres_columns_sql(true);
        let all = postgres_columns_sql(false);

        assert!(single.contains("AND cls.relname = $2"));
        assert!(!all.contains("cls.relname = $2"));
        // Without a relation predicate, pg_attribute also yields index, TOAST and
        // composite-type attributes.
        assert!(all.contains("cls.relkind IN ('r', 'p', 'f', 'v', 'm')"));
        assert!(all.contains("ORDER BY cls.relname, a.attnum"));
    }

    #[test]
    fn postgres_bulk_foreign_keys_query_drops_the_relation_predicate() {
        let single = postgres_foreign_keys_sql(true);
        let all = postgres_foreign_keys_sql(false);

        assert!(single.contains("AND tc.table_name = $2"));
        assert!(!all.contains("tc.table_name = $2"));
        assert!(all.contains("ORDER BY tc.table_name"));
    }

    #[test]
    fn postgres_all_schema_queries_take_no_schema_parameter_and_skip_system_schemas() {
        for sql in [
            postgres_all_schemas_columns_sql(),
            postgres_all_schemas_foreign_keys_sql(),
        ] {
            assert!(!sql.contains("$1"), "{sql}");
            assert!(sql.contains("NOT IN ('pg_catalog', 'information_schema', 'pg_toast')"));
            assert!(sql.contains("NOT LIKE 'pg_temp_%'"));
        }
        let columns = postgres_all_schemas_columns_sql();
        assert!(columns.contains("cls.relkind IN ('r', 'p', 'f', 'v', 'm')"));
        assert!(columns.contains("ORDER BY ns.nspname, cls.relname, a.attnum"));
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
