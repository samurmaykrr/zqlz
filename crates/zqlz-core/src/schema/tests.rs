use super::*;
#[test]
fn objects_panel_object_ref_identity_key_tracks_optional_parts() {
    let object_ref = ObjectsPanelObjectRef::new("function", "sum_total")
        .with_database_option(Some("app".to_string()))
        .with_schema_option(Some("public".to_string()))
        .with_signature_option(Some("integer,integer".to_string()));

    assert_eq!(
        object_ref.identity_key,
        "function::app::public::sum_total::integer,integer"
    );

    assert_eq!(
        ObjectsPanelObjectRef::build_identity_key("table", None, None, "users", None),
        "table::::::users::"
    );
}

#[test]
fn objects_panel_row_prefers_canonical_object_ref_identity() {
    let row = ObjectsPanelRow {
        name: "legacy_name".to_string(),
        schema: Some("legacy_schema".to_string()),
        object_type: "table".to_string(),
        object_ref: Some(
            ObjectsPanelObjectRef::new("view", "canonical_name")
                .with_schema_option(Some("canonical_schema".to_string())),
        ),
        values: std::collections::BTreeMap::new(),
        redis_database_index: None,
        key_value_info: None,
    };

    assert_eq!(row.object_kind_id(), "view");
    assert_eq!(row.object_name(), "canonical_name");
    assert_eq!(row.object_schema(), Some("canonical_schema"));
}

#[test]
fn objects_panel_data_for_kind_and_scope_filters_rows_without_changing_columns() {
    let data = ObjectsPanelData {
        columns: vec![ObjectsPanelColumn::new("name", "Name")],
        rows: vec![
            ObjectsPanelRow {
                name: "users".to_string(),
                schema: Some("public".to_string()),
                object_type: "table".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("table", "users")
                        .with_schema_option(Some("public".to_string())),
                ),
                values: std::collections::BTreeMap::new(),
                redis_database_index: None,
                key_value_info: None,
            },
            ObjectsPanelRow {
                name: "audit_log".to_string(),
                schema: Some("archive".to_string()),
                object_type: "table".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("table", "audit_log")
                        .with_schema_option(Some("archive".to_string())),
                ),
                values: std::collections::BTreeMap::new(),
                redis_database_index: None,
                key_value_info: None,
            },
            ObjectsPanelRow {
                name: "active_users".to_string(),
                schema: Some("public".to_string()),
                object_type: "view".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("view", "active_users")
                        .with_schema_option(Some("public".to_string())),
                ),
                values: std::collections::BTreeMap::new(),
                redis_database_index: None,
                key_value_info: None,
            },
        ],
    };

    let scoped = data.for_kind_and_scope("table", Some("public"));

    let original_column_ids: Vec<&str> = data
        .columns
        .iter()
        .map(|column| column.id.as_str())
        .collect();
    let scoped_column_ids: Vec<&str> = scoped
        .columns
        .iter()
        .map(|column| column.id.as_str())
        .collect();
    assert_eq!(scoped_column_ids, original_column_ids);
    assert_eq!(scoped.rows.len(), 1);
    assert_eq!(scoped.rows[0].object_kind_id(), "table");
    assert_eq!(scoped.rows[0].object_name(), "users");
    assert_eq!(scoped.rows[0].object_schema(), Some("public"));
}

#[test]
fn objects_panel_data_from_table_infos_maps_types_and_counts() {
    let data = ObjectsPanelData::from_table_infos(vec![
        TableInfo {
            schema: Some("public".to_string()),
            name: "users".to_string(),
            table_type: TableType::Table,
            owner: None,
            row_count: Some(10),
            size_bytes: None,
            comment: None,
            index_count: Some(2),
            trigger_count: Some(1),
            key_value_info: None,
        },
        TableInfo {
            schema: Some("public".to_string()),
            name: "active_users".to_string(),
            table_type: TableType::View,
            owner: None,
            row_count: None,
            size_bytes: None,
            comment: None,
            index_count: None,
            trigger_count: None,
            key_value_info: None,
        },
    ]);

    let column_ids: Vec<&str> = data
        .columns
        .iter()
        .map(|column| column.id.as_str())
        .collect();
    assert_eq!(
        column_ids,
        vec!["name", "row_count", "index_count", "trigger_count"]
    );

    assert_eq!(data.rows.len(), 2);
    assert_eq!(data.rows[0].object_kind_id(), "table");
    assert_eq!(data.rows[1].object_kind_id(), "view");
    assert_eq!(
        data.rows[1]
            .values
            .get("row_count")
            .map(|value| value.as_str()),
        Some("-")
    );
    assert!(data.rows[0].object_ref.is_some());
    assert!(data.rows[1].object_ref.is_some());
}

#[test]
fn objects_panel_manifest_from_data_deduplicates_kind_and_sets_defaults() {
    let mut values = std::collections::BTreeMap::new();
    values.insert("name".to_string(), "users".to_string());

    let data = ObjectsPanelData {
        columns: vec![ObjectsPanelColumn::new("name", "Name")],
        rows: vec![
            ObjectsPanelRow {
                name: "users".to_string(),
                schema: Some("public".to_string()),
                object_type: "table".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("table", "users")
                        .with_schema_option(Some("public".to_string())),
                ),
                values: values.clone(),
                redis_database_index: None,
                key_value_info: None,
            },
            ObjectsPanelRow {
                name: "archive_users".to_string(),
                schema: Some("public".to_string()),
                object_type: "table".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("table", "archive_users")
                        .with_schema_option(Some("public".to_string())),
                ),
                values: values.clone(),
                redis_database_index: None,
                key_value_info: None,
            },
            ObjectsPanelRow {
                name: "get_user(int)".to_string(),
                schema: Some("public".to_string()),
                object_type: "function".to_string(),
                object_ref: Some(
                    ObjectsPanelObjectRef::new("function", "get_user")
                        .with_schema_option(Some("public".to_string()))
                        .with_signature_option(Some("integer".to_string())),
                ),
                values,
                redis_database_index: None,
                key_value_info: None,
            },
        ],
    };

    let manifest = ObjectsPanelManifest::from_data(&data);
    assert_eq!(manifest.object_kinds.len(), 2);

    let table_kind = manifest
        .object_kinds
        .iter()
        .find(|kind| kind.id == "table")
        .expect("table kind should exist");
    assert_eq!(table_kind.default_row_action_id.as_deref(), Some("open"));
    assert!(
        table_kind
            .row_actions
            .iter()
            .any(|action| action.id == "view_history")
    );

    let toolbar_action_ids: Vec<&str> = manifest
        .toolbar_actions
        .iter()
        .map(|action| action.id.as_str())
        .collect();
    assert!(toolbar_action_ids.contains(&"refresh"));
    assert!(toolbar_action_ids.contains(&"new_table"));
    assert!(toolbar_action_ids.contains(&"new_view"));
    assert!(toolbar_action_ids.contains(&"import"));
    assert!(toolbar_action_ids.contains(&"export"));
}

#[test]
fn objects_panel_manifest_from_empty_data_provides_usable_defaults() {
    let manifest = ObjectsPanelManifest::from_data(&ObjectsPanelData {
        columns: Vec::new(),
        rows: Vec::new(),
    });

    assert_eq!(manifest.object_kinds.len(), 1);
    let default_kind = &manifest.object_kinds[0];
    assert_eq!(default_kind.id, "table");
    assert!(!default_kind.columns.is_empty());
    assert!(
        default_kind
            .row_actions
            .iter()
            .any(|action| action.id == "open")
    );
    assert!(
        default_kind
            .row_actions
            .iter()
            .any(|action| action.id == "delete")
    );
    assert!(
        default_kind
            .row_actions
            .iter()
            .any(|action| action.id == "refresh")
    );
}

#[test]
fn objects_panel_manifest_validate_rejects_duplicate_kind_ids() {
    let manifest = ObjectsPanelManifest {
        object_kinds: vec![
            ObjectsPanelObjectKind::new("table", "Table", "Tables")
                .row_actions(vec![ObjectsPanelAction::new("open", "Open")]),
            ObjectsPanelObjectKind::new("table", "Table", "Tables")
                .row_actions(vec![ObjectsPanelAction::new("open", "Open")]),
        ],
        toolbar_actions: Vec::new(),
    };

    let error = manifest
        .validate()
        .expect_err("duplicate kind ids should fail validation");
    assert!(matches!(error, ZqlzError::Schema(message) if message.contains("duplicate kind_id")));
}

#[test]
fn objects_panel_manifest_validate_rejects_duplicate_action_ids_per_kind() {
    let manifest = ObjectsPanelManifest {
        object_kinds: vec![
            ObjectsPanelObjectKind::new("table", "Table", "Tables").row_actions(vec![
                ObjectsPanelAction::new("open", "Open"),
                ObjectsPanelAction::new("open", "Open Again"),
            ]),
        ],
        toolbar_actions: Vec::new(),
    };

    let error = manifest
        .validate()
        .expect_err("duplicate action ids should fail validation");
    assert!(matches!(error, ZqlzError::Schema(message) if message.contains("duplicate action_id")));
}

#[test]
fn objects_panel_manifest_validate_rejects_unknown_default_row_action_id() {
    let manifest = ObjectsPanelManifest {
        object_kinds: vec![
            ObjectsPanelObjectKind::new("table", "Table", "Tables")
                .row_actions(vec![ObjectsPanelAction::new("open", "Open")])
                .default_row_action("design"),
        ],
        toolbar_actions: Vec::new(),
    };

    let error = manifest
        .validate()
        .expect_err("unknown default row action id should fail validation");
    assert!(
        matches!(error, ZqlzError::Schema(message) if message.contains("default_row_action_id"))
    );
}

#[test]
fn objects_panel_manifest_validate_accepts_driver_fallback_manifest() {
    let manifest =
        ObjectsPanelManifest::from_data(&ObjectsPanelData::from_table_infos(vec![TableInfo {
            schema: Some("public".to_string()),
            name: "users".to_string(),
            table_type: TableType::Table,
            owner: None,
            row_count: Some(5),
            size_bytes: None,
            comment: None,
            index_count: Some(1),
            trigger_count: Some(0),
            key_value_info: None,
        }]));

    manifest
        .validate()
        .expect("fallback manifest should pass validation");
}
