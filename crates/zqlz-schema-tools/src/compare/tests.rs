//! Tests for schema comparison functionality

use std::collections::HashMap;

use zqlz_core::{
    ColumnInfo, ConstraintInfo, ConstraintType, ForeignKeyAction, ForeignKeyInfo, FunctionInfo,
    IndexInfo, PrimaryKeyInfo, ProcedureInfo, SequenceInfo, TableDetails, TableInfo, TableType,
    TriggerEvent, TriggerForEach, TriggerInfo, TriggerTiming, TypeInfo, TypeKind, ViewInfo,
};

use super::comparator::{CompareConfig, SchemaComparator};
use super::diff::{PrimaryKeyChange, SchemaDiff};

fn create_test_column(name: &str, data_type: &str, nullable: bool) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        ordinal: 0,
        data_type: data_type.to_string(),
        nullable,
        default_value: None,
        max_length: None,
        precision: None,
        scale: None,
        is_primary_key: false,
        is_auto_increment: false,
        is_unique: false,
        foreign_key: None,
        comment: None,
        ..Default::default()
    }
}

fn create_test_table_info(name: &str, schema: Option<&str>) -> TableInfo {
    TableInfo {
        name: name.to_string(),
        schema: schema.map(|s| s.to_string()),
        table_type: TableType::Table,
        owner: None,
        row_count: None,
        size_bytes: None,
        comment: None,
        index_count: None,
        trigger_count: None,
        key_value_info: None,
    }
}

fn create_test_table_details(
    name: &str,
    schema: Option<&str>,
    columns: Vec<ColumnInfo>,
) -> TableDetails {
    TableDetails {
        info: create_test_table_info(name, schema),
        columns,
        primary_key: None,
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        constraints: Vec::new(),
        triggers: Vec::new(),
    }
}

#[cfg(test)]
mod schema_diff_tests {
    use super::*;

    #[test]
    fn test_schema_diff_new_is_empty() {
        let diff = SchemaDiff::new();
        assert!(diff.is_empty());
        assert_eq!(diff.change_count(), 0);
        assert!(!diff.has_breaking_changes());
    }

    #[test]
    fn test_schema_diff_with_added_table_not_empty() {
        let mut diff = SchemaDiff::new();
        diff.added_tables
            .push(create_test_table_info("users", None));

        assert!(!diff.is_empty());
        assert_eq!(diff.change_count(), 1);
        assert!(!diff.has_breaking_changes());
    }

    #[test]
    fn test_schema_diff_removed_table_is_breaking() {
        let mut diff = SchemaDiff::new();
        diff.removed_tables
            .push(create_test_table_info("users", None));

        assert!(!diff.is_empty());
        assert!(diff.has_breaking_changes());
    }
}

#[cfg(test)]
mod compare_config_tests {
    use super::*;

    #[test]
    fn test_default_config_all_enabled() {
        let config = CompareConfig::default();
        assert!(config.compare_comments);
        assert!(config.compare_indexes);
        assert!(config.compare_foreign_keys);
        assert!(config.compare_constraints);
        assert!(config.compare_triggers);
        assert!(!config.ignore_column_order);
        assert!(config.case_sensitive);
    }

    #[test]
    fn test_config_builder_chain() {
        let config = CompareConfig::new()
            .without_comments()
            .without_indexes()
            .ignore_column_order()
            .case_insensitive();

        assert!(!config.compare_comments);
        assert!(!config.compare_indexes);
        assert!(config.ignore_column_order);
        assert!(!config.case_sensitive);
    }
}

#[cfg(test)]
mod comparator_construction_tests {
    use super::*;

    #[test]
    fn test_comparator_default() {
        let comparator = SchemaComparator::default();
        assert!(comparator.config().compare_comments);
    }

    #[test]
    fn test_comparator_with_config() {
        let config = CompareConfig::new().without_comments();
        let comparator = SchemaComparator::with_config(config);
        assert!(!comparator.config().compare_comments);
    }
}

#[cfg(test)]
mod table_comparison_tests {
    use super::*;

    #[test]
    fn test_detect_added_table() {
        let comparator = SchemaComparator::new();

        let source = vec![
            create_test_table_info("users", None),
            create_test_table_info("orders", None),
        ];
        let target = vec![create_test_table_info("users", None)];

        let source_details = HashMap::from([
            (
                "users".to_string(),
                create_test_table_details("users", None, vec![]),
            ),
            (
                "orders".to_string(),
                create_test_table_details("orders", None, vec![]),
            ),
        ]);
        let target_details = HashMap::from([(
            "users".to_string(),
            create_test_table_details("users", None, vec![]),
        )]);

        let diff = comparator.compare_tables(&source, &target, &source_details, &target_details);

        assert_eq!(diff.added_tables.len(), 1);
        assert_eq!(diff.added_tables[0].name, "orders");
        assert!(diff.removed_tables.is_empty());
    }

    #[test]
    fn test_detect_removed_table() {
        let comparator = SchemaComparator::new();

        let source = vec![create_test_table_info("users", None)];
        let target = vec![
            create_test_table_info("users", None),
            create_test_table_info("orders", None),
        ];

        let source_details = HashMap::from([(
            "users".to_string(),
            create_test_table_details("users", None, vec![]),
        )]);
        let target_details = HashMap::from([
            (
                "users".to_string(),
                create_test_table_details("users", None, vec![]),
            ),
            (
                "orders".to_string(),
                create_test_table_details("orders", None, vec![]),
            ),
        ]);

        let diff = comparator.compare_tables(&source, &target, &source_details, &target_details);

        assert!(diff.added_tables.is_empty());
        assert_eq!(diff.removed_tables.len(), 1);
        assert_eq!(diff.removed_tables[0].name, "orders");
    }

    #[test]
    fn test_detect_modified_column() {
        let comparator = SchemaComparator::new();

        let source = vec![create_test_table_info("users", None)];
        let target = vec![create_test_table_info("users", None)];

        let source_details = HashMap::from([(
            "users".to_string(),
            create_test_table_details(
                "users",
                None,
                vec![create_test_column("name", "VARCHAR(100)", false)],
            ),
        )]);
        let target_details = HashMap::from([(
            "users".to_string(),
            create_test_table_details(
                "users",
                None,
                vec![create_test_column("name", "VARCHAR(50)", false)],
            ),
        )]);

        let diff = comparator.compare_tables(&source, &target, &source_details, &target_details);

        assert_eq!(diff.modified_tables.len(), 1);
        assert_eq!(diff.modified_tables[0].modified_columns.len(), 1);
        assert_eq!(
            diff.modified_tables[0].modified_columns[0].column_name,
            "name"
        );
        assert!(diff.modified_tables[0].modified_columns[0]
            .type_change
            .is_some());
    }

    #[test]
    fn test_case_insensitive_comparison() {
        let config = CompareConfig::new().case_insensitive();
        let comparator = SchemaComparator::with_config(config);

        let source = vec![create_test_table_info("USERS", None)];
        let target = vec![create_test_table_info("users", None)];

        let source_details = HashMap::from([(
            "USERS".to_string(),
            create_test_table_details("USERS", None, vec![]),
        )]);
        let target_details = HashMap::from([(
            "users".to_string(),
            create_test_table_details("users", None, vec![]),
        )]);

        let diff = comparator.compare_tables(&source, &target, &source_details, &target_details);

        assert!(diff.added_tables.is_empty());
        assert!(diff.removed_tables.is_empty());
    }
}

#[cfg(test)]
mod column_comparison_tests {
    use super::*;

    #[test]
    fn test_detect_added_column() {
        let comparator = SchemaComparator::new();

        let source_cols = vec![
            create_test_column("id", "INT", false),
            create_test_column("name", "VARCHAR(100)", true),
        ];
        let target_cols = vec![create_test_column("id", "INT", false)];

        let source = create_test_table_details("users", None, source_cols);
        let target = create_test_table_details("users", None, target_cols);

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert_eq!(diff.added_columns.len(), 1);
        assert_eq!(diff.added_columns[0].name, "name");
    }

    #[test]
    fn test_detect_removed_column() {
        let comparator = SchemaComparator::new();

        let source_cols = vec![create_test_column("id", "INT", false)];
        let target_cols = vec![
            create_test_column("id", "INT", false),
            create_test_column("name", "VARCHAR(100)", true),
        ];

        let source = create_test_table_details("users", None, source_cols);
        let target = create_test_table_details("users", None, target_cols);

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert_eq!(diff.removed_columns.len(), 1);
        assert_eq!(diff.removed_columns[0].name, "name");
    }

    #[test]
    fn test_detect_nullable_change() {
        let comparator = SchemaComparator::new();

        let source_cols = vec![create_test_column("name", "VARCHAR(100)", false)];
        let target_cols = vec![create_test_column("name", "VARCHAR(100)", true)];

        let source = create_test_table_details("users", None, source_cols);
        let target = create_test_table_details("users", None, target_cols);

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert_eq!(diff.modified_columns.len(), 1);
        assert_eq!(
            diff.modified_columns[0].nullable_change,
            Some((false, true))
        );
    }

    #[test]
    fn test_nullable_to_not_null_is_unsafe() {
        let comparator = SchemaComparator::new();

        let source_cols = vec![create_test_column("name", "VARCHAR(100)", true)];
        let target_cols = vec![create_test_column("name", "VARCHAR(100)", false)];

        let source = create_test_table_details("users", None, source_cols);
        let target = create_test_table_details("users", None, target_cols);

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert!(!diff.is_safe());
    }
}

#[cfg(test)]
mod index_comparison_tests {
    use super::*;

    fn create_test_index(name: &str, columns: Vec<&str>, is_unique: bool) -> IndexInfo {
        IndexInfo {
            name: name.to_string(),
            columns: columns.into_iter().map(|s| s.to_string()).collect(),
            is_unique,
            is_primary: false,
            index_type: "btree".to_string(),
            comment: None,
            ..Default::default()
        }
    }

    #[test]
    fn test_detect_added_index() {
        let comparator = SchemaComparator::new();

        let mut source = create_test_table_details("users", None, vec![]);
        source.indexes = vec![
            create_test_index("idx_name", vec!["name"], false),
            create_test_index("idx_email", vec!["email"], true),
        ];

        let mut target = create_test_table_details("users", None, vec![]);
        target.indexes = vec![create_test_index("idx_name", vec!["name"], false)];

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert_eq!(diff.added_indexes.len(), 1);
        assert_eq!(diff.added_indexes[0].name, "idx_email");
    }

    #[test]
    fn test_detect_removed_index() {
        let comparator = SchemaComparator::new();

        let mut source = create_test_table_details("users", None, vec![]);
        source.indexes = vec![create_test_index("idx_name", vec!["name"], false)];

        let mut target = create_test_table_details("users", None, vec![]);
        target.indexes = vec![
            create_test_index("idx_name", vec!["name"], false),
            create_test_index("idx_email", vec!["email"], true),
        ];

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert_eq!(diff.removed_indexes.len(), 1);
        assert_eq!(diff.removed_indexes[0].name, "idx_email");
    }

    #[test]
    fn test_detect_modified_index() {
        let comparator = SchemaComparator::new();

        let mut source = create_test_table_details("users", None, vec![]);
        source.indexes = vec![create_test_index("idx_name", vec!["name", "email"], false)];

        let mut target = create_test_table_details("users", None, vec![]);
        target.indexes = vec![create_test_index("idx_name", vec!["name"], false)];

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert_eq!(diff.modified_indexes.len(), 1);
        assert_eq!(diff.modified_indexes[0].index_name, "idx_name");
    }

    #[test]
    fn test_skip_index_comparison_when_disabled() {
        let config = CompareConfig::new().without_indexes();
        let comparator = SchemaComparator::with_config(config);

        let mut source = create_test_table_details("users", None, vec![]);
        source.indexes = vec![create_test_index("idx_name", vec!["name"], false)];

        let target = create_test_table_details("users", None, vec![]);

        let diff = comparator.compare_table_details(&source, &target);

        assert!(diff.is_none());
    }
}

#[cfg(test)]
mod foreign_key_comparison_tests {
    use super::*;

    fn create_test_fk(name: &str, columns: Vec<&str>, ref_table: &str) -> ForeignKeyInfo {
        ForeignKeyInfo {
            name: name.to_string(),
            columns: columns.into_iter().map(|s| s.to_string()).collect(),
            referenced_table: ref_table.to_string(),
            referenced_schema: None,
            referenced_columns: vec!["id".to_string()],
            on_update: ForeignKeyAction::NoAction,
            on_delete: ForeignKeyAction::Cascade,
            is_deferrable: false,
            initially_deferred: false,
        }
    }

    #[test]
    fn test_detect_added_foreign_key() {
        let comparator = SchemaComparator::new();

        let mut source = create_test_table_details("orders", None, vec![]);
        source.foreign_keys = vec![create_test_fk("fk_user", vec!["user_id"], "users")];

        let target = create_test_table_details("orders", None, vec![]);

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert_eq!(diff.added_foreign_keys.len(), 1);
        assert_eq!(diff.added_foreign_keys[0].name, "fk_user");
    }

    #[test]
    fn test_detect_fk_action_change() {
        let comparator = SchemaComparator::new();

        let mut source = create_test_table_details("orders", None, vec![]);
        let mut fk = create_test_fk("fk_user", vec!["user_id"], "users");
        fk.on_delete = ForeignKeyAction::SetNull;
        source.foreign_keys = vec![fk];

        let mut target = create_test_table_details("orders", None, vec![]);
        target.foreign_keys = vec![create_test_fk("fk_user", vec!["user_id"], "users")];

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert_eq!(diff.modified_foreign_keys.len(), 1);
        assert!(diff.modified_foreign_keys[0].on_delete_change.is_some());
    }
}

#[cfg(test)]
mod primary_key_comparison_tests {
    use super::*;

    fn create_test_pk(columns: Vec<&str>) -> PrimaryKeyInfo {
        PrimaryKeyInfo {
            name: Some("pk_test".to_string()),
            columns: columns.into_iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn test_detect_added_primary_key() {
        let comparator = SchemaComparator::new();

        let mut source = create_test_table_details("users", None, vec![]);
        source.primary_key = Some(create_test_pk(vec!["id"]));

        let target = create_test_table_details("users", None, vec![]);

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert!(matches!(
            diff.primary_key_change,
            Some(PrimaryKeyChange::Added(_))
        ));
    }

    #[test]
    fn test_detect_removed_primary_key() {
        let comparator = SchemaComparator::new();

        let source = create_test_table_details("users", None, vec![]);

        let mut target = create_test_table_details("users", None, vec![]);
        target.primary_key = Some(create_test_pk(vec!["id"]));

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert!(matches!(
            diff.primary_key_change,
            Some(PrimaryKeyChange::Removed(_))
        ));
    }

    #[test]
    fn test_detect_modified_primary_key() {
        let comparator = SchemaComparator::new();

        let mut source = create_test_table_details("users", None, vec![]);
        source.primary_key = Some(create_test_pk(vec!["id", "tenant_id"]));

        let mut target = create_test_table_details("users", None, vec![]);
        target.primary_key = Some(create_test_pk(vec!["id"]));

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert!(matches!(
            diff.primary_key_change,
            Some(PrimaryKeyChange::Modified { .. })
        ));
    }
}

#[cfg(test)]
mod view_comparison_tests {
    use super::*;

    fn create_test_view(name: &str, definition: Option<&str>, materialized: bool) -> ViewInfo {
        ViewInfo {
            name: name.to_string(),
            schema: None,
            is_materialized: materialized,
            definition: definition.map(|s| s.to_string()),
            owner: None,
            comment: None,
        }
    }

    #[test]
    fn test_detect_added_view() {
        let comparator = SchemaComparator::new();

        let source = vec![
            create_test_view("v_users", Some("SELECT * FROM users"), false),
            create_test_view("v_orders", Some("SELECT * FROM orders"), false),
        ];
        let target = vec![create_test_view(
            "v_users",
            Some("SELECT * FROM users"),
            false,
        )];

        let diff = comparator.compare_views(&source, &target);

        assert_eq!(diff.added_views.len(), 1);
        assert_eq!(diff.added_views[0].name, "v_orders");
    }

    #[test]
    fn test_detect_view_definition_change() {
        let comparator = SchemaComparator::new();

        let source = vec![create_test_view(
            "v_users",
            Some("SELECT id, name FROM users"),
            false,
        )];
        let target = vec![create_test_view(
            "v_users",
            Some("SELECT * FROM users"),
            false,
        )];

        let diff = comparator.compare_views(&source, &target);

        assert_eq!(diff.modified_views.len(), 1);
        assert!(diff.modified_views[0].definition_change.is_some());
    }

    #[test]
    fn test_detect_materialized_change() {
        let comparator = SchemaComparator::new();

        let source = vec![create_test_view(
            "v_users",
            Some("SELECT * FROM users"),
            true,
        )];
        let target = vec![create_test_view(
            "v_users",
            Some("SELECT * FROM users"),
            false,
        )];

        let diff = comparator.compare_views(&source, &target);

        assert_eq!(diff.modified_views.len(), 1);
        assert_eq!(
            diff.modified_views[0].materialized_change,
            Some((true, false))
        );
    }
}

#[cfg(test)]
mod function_comparison_tests {
    use super::*;

    fn create_test_function(name: &str, return_type: &str, lang: &str) -> FunctionInfo {
        FunctionInfo {
            name: name.to_string(),
            schema: None,
            language: lang.to_string(),
            return_type: return_type.to_string(),
            parameters: vec![],
            definition: Some("BEGIN RETURN 1; END".to_string()),
            owner: None,
            comment: None,
        }
    }

    #[test]
    fn test_detect_added_function() {
        let comparator = SchemaComparator::new();

        let source = vec![
            create_test_function("get_user", "INT", "plpgsql"),
            create_test_function("get_order", "INT", "plpgsql"),
        ];
        let target = vec![create_test_function("get_user", "INT", "plpgsql")];

        let diff = comparator.compare_functions(&source, &target);

        assert_eq!(diff.added_functions.len(), 1);
        assert_eq!(diff.added_functions[0].name, "get_order");
    }

    #[test]
    fn test_detect_return_type_change() {
        let comparator = SchemaComparator::new();

        let source = vec![create_test_function("get_user", "BIGINT", "plpgsql")];
        let target = vec![create_test_function("get_user", "INT", "plpgsql")];

        let diff = comparator.compare_functions(&source, &target);

        assert_eq!(diff.modified_functions.len(), 1);
        assert!(diff.modified_functions[0].return_type_change.is_some());
    }
}

#[cfg(test)]
mod sequence_comparison_tests {
    use super::*;

    fn create_test_sequence(name: &str, start: i64, increment: i64) -> SequenceInfo {
        SequenceInfo {
            name: name.to_string(),
            schema: None,
            data_type: "BIGINT".to_string(),
            start_value: start,
            min_value: 1,
            max_value: i64::MAX,
            increment_by: increment,
            current_value: None,
            owner: None,
            comment: None,
        }
    }

    #[test]
    fn test_detect_added_sequence() {
        let comparator = SchemaComparator::new();

        let source = vec![
            create_test_sequence("user_id_seq", 1, 1),
            create_test_sequence("order_id_seq", 1, 1),
        ];
        let target = vec![create_test_sequence("user_id_seq", 1, 1)];

        let diff = comparator.compare_sequences(&source, &target);

        assert_eq!(diff.added_sequences.len(), 1);
        assert_eq!(diff.added_sequences[0].name, "order_id_seq");
    }

    #[test]
    fn test_detect_increment_change() {
        let comparator = SchemaComparator::new();

        let source = vec![create_test_sequence("user_id_seq", 1, 10)];
        let target = vec![create_test_sequence("user_id_seq", 1, 1)];

        let diff = comparator.compare_sequences(&source, &target);

        assert_eq!(diff.modified_sequences.len(), 1);
        assert_eq!(diff.modified_sequences[0].increment_change, Some((10, 1)));
    }
}

#[cfg(test)]
mod trigger_comparison_tests {
    use super::*;

    fn create_test_trigger(name: &str, table: &str, enabled: bool) -> TriggerInfo {
        TriggerInfo {
            name: name.to_string(),
            schema: None,
            table_name: table.to_string(),
            timing: TriggerTiming::Before,
            events: vec![TriggerEvent::Insert],
            for_each: TriggerForEach::Row,
            definition: Some("EXECUTE FUNCTION test()".to_string()),
            enabled,
            comment: None,
        }
    }

    #[test]
    fn test_detect_added_trigger() {
        let comparator = SchemaComparator::new();

        let source = vec![
            create_test_trigger("trg_audit", "users", true),
            create_test_trigger("trg_notify", "users", true),
        ];
        let target = vec![create_test_trigger("trg_audit", "users", true)];

        let diff = comparator.compare_triggers(&source, &target);

        assert_eq!(diff.added_triggers.len(), 1);
        assert_eq!(diff.added_triggers[0].name, "trg_notify");
    }

    #[test]
    fn test_detect_trigger_enabled_change() {
        let comparator = SchemaComparator::new();

        let source = vec![create_test_trigger("trg_audit", "users", false)];
        let target = vec![create_test_trigger("trg_audit", "users", true)];

        let diff = comparator.compare_triggers(&source, &target);

        assert_eq!(diff.modified_triggers.len(), 1);
        assert_eq!(
            diff.modified_triggers[0].enabled_change,
            Some((false, true))
        );
    }

    #[test]
    fn test_skip_trigger_comparison_when_disabled() {
        let config = CompareConfig::new().without_triggers();
        let comparator = SchemaComparator::with_config(config);

        let source = vec![create_test_trigger("trg_audit", "users", true)];
        let target = vec![];

        let diff = comparator.compare_triggers(&source, &target);

        assert!(diff.is_empty());
    }
}

#[cfg(test)]
mod type_comparison_tests {
    use super::*;

    fn create_test_type(name: &str, values: Option<Vec<&str>>) -> TypeInfo {
        TypeInfo {
            name: name.to_string(),
            schema: None,
            type_kind: TypeKind::Enum,
            values: values.map(|v| v.into_iter().map(|s| s.to_string()).collect()),
            definition: None,
            owner: None,
            comment: None,
        }
    }

    #[test]
    fn test_detect_added_type() {
        let comparator = SchemaComparator::new();

        let source = vec![
            create_test_type("status", Some(vec!["active", "inactive"])),
            create_test_type("priority", Some(vec!["low", "medium", "high"])),
        ];
        let target = vec![create_test_type("status", Some(vec!["active", "inactive"]))];

        let diff = comparator.compare_types(&source, &target);

        assert_eq!(diff.added_types.len(), 1);
        assert_eq!(diff.added_types[0].name, "priority");
    }

    #[test]
    fn test_detect_enum_values_change() {
        let comparator = SchemaComparator::new();

        let source = vec![create_test_type(
            "status",
            Some(vec!["active", "inactive", "pending"]),
        )];
        let target = vec![create_test_type("status", Some(vec!["active", "inactive"]))];

        let diff = comparator.compare_types(&source, &target);

        assert_eq!(diff.modified_types.len(), 1);
        assert!(diff.modified_types[0].values_change.is_some());
    }
}

#[cfg(test)]
mod merge_diffs_tests {
    use super::*;

    #[test]
    fn test_merge_multiple_diffs() {
        let comparator = SchemaComparator::new();

        let mut diff1 = SchemaDiff::new();
        diff1
            .added_tables
            .push(create_test_table_info("users", None));

        let mut diff2 = SchemaDiff::new();
        diff2
            .added_tables
            .push(create_test_table_info("orders", None));
        diff2
            .removed_tables
            .push(create_test_table_info("legacy", None));

        let merged = comparator.merge_diffs(vec![diff1, diff2]);

        assert_eq!(merged.added_tables.len(), 2);
        assert_eq!(merged.removed_tables.len(), 1);
    }
}

#[cfg(test)]
mod constraint_comparison_tests {
    use super::*;

    fn create_test_constraint(name: &str, columns: Vec<&str>) -> ConstraintInfo {
        ConstraintInfo {
            name: name.to_string(),
            constraint_type: ConstraintType::Check,
            columns: columns.into_iter().map(|s| s.to_string()).collect(),
            definition: Some("age > 0".to_string()),
        }
    }

    #[test]
    fn test_detect_added_constraint() {
        let comparator = SchemaComparator::new();

        let mut source = create_test_table_details("users", None, vec![]);
        source.constraints = vec![create_test_constraint("chk_age", vec!["age"])];

        let target = create_test_table_details("users", None, vec![]);

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert_eq!(diff.added_constraints.len(), 1);
        assert_eq!(diff.added_constraints[0].name, "chk_age");
    }

    #[test]
    fn test_detect_removed_constraint() {
        let comparator = SchemaComparator::new();

        let source = create_test_table_details("users", None, vec![]);

        let mut target = create_test_table_details("users", None, vec![]);
        target.constraints = vec![create_test_constraint("chk_age", vec!["age"])];

        let diff = comparator.compare_table_details(&source, &target).unwrap();

        assert_eq!(diff.removed_constraints.len(), 1);
        assert_eq!(diff.removed_constraints[0].name, "chk_age");
    }
}

#[cfg(test)]
mod table_diff_tests {
    #[test]
    fn test_qualified_name_with_schema() {
        use super::super::diff::TableDiff;
        let diff = TableDiff::new("users", Some("public".to_string()));
        assert_eq!(diff.qualified_name(), "public.users");
    }

    #[test]
    fn test_qualified_name_without_schema() {
        use super::super::diff::TableDiff;
        let diff = TableDiff::new("users", None);
        assert_eq!(diff.qualified_name(), "users");
    }
}

#[cfg(test)]
mod procedure_comparison_tests {
    use super::*;

    fn create_test_procedure(name: &str, lang: &str) -> ProcedureInfo {
        ProcedureInfo {
            name: name.to_string(),
            schema: None,
            language: lang.to_string(),
            parameters: vec![],
            definition: Some("BEGIN END".to_string()),
            owner: None,
            comment: None,
        }
    }

    #[test]
    fn test_detect_added_procedure() {
        let comparator = SchemaComparator::new();

        let source = vec![
            create_test_procedure("proc_audit", "plpgsql"),
            create_test_procedure("proc_notify", "plpgsql"),
        ];
        let target = vec![create_test_procedure("proc_audit", "plpgsql")];

        let diff = comparator.compare_procedures(&source, &target);

        assert_eq!(diff.added_procedures.len(), 1);
        assert_eq!(diff.added_procedures[0].name, "proc_notify");
    }

    #[test]
    fn test_detect_procedure_language_change() {
        let comparator = SchemaComparator::new();

        let source = vec![create_test_procedure("proc_audit", "sql")];
        let target = vec![create_test_procedure("proc_audit", "plpgsql")];

        let diff = comparator.compare_procedures(&source, &target);

        assert_eq!(diff.modified_procedures.len(), 1);
        assert!(diff.modified_procedures[0].language_change.is_some());
    }
}
