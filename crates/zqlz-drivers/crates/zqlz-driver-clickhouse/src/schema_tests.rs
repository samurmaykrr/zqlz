use super::schema::*;
use zqlz_core::{ObjectFormDdlRequest, ObjectFormMode, ObjectFormValue};

fn form_values(
    entries: &[(&str, ObjectFormValue)],
) -> std::collections::BTreeMap<String, ObjectFormValue> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

#[test]
fn clickhouse_manifest_exposes_core_kinds_and_forms() {
    let manifest = clickhouse_objects_panel_manifest();
    manifest.validate().expect("valid manifest");
    let kind_ids: Vec<&str> = manifest
        .object_kinds
        .iter()
        .map(|kind| kind.id.as_str())
        .collect();
    assert!(kind_ids.contains(&"database"));
    assert!(kind_ids.contains(&"table"));
    assert!(kind_ids.contains(&"view"));
    assert!(kind_ids.contains(&"materialized_view"));
    assert!(
        manifest
            .toolbar_actions
            .iter()
            .any(|action| action.object_form.is_some())
    );
}

#[test]
fn clickhouse_identifier_quoting_escapes_backticks() {
    assert_eq!(clickhouse_quote_identifier("a`b"), "`a``b`");
}

#[test]
fn clickhouse_table_form_generates_engine_and_order_by() {
    let ddl = clickhouse_object_form_ddl(&ObjectFormDdlRequest {
        kind_id: "table".to_string(),
        mode: ObjectFormMode::Create,
        object_ref: None,
        values: form_values(&[
            ("database", ObjectFormValue::String("analytics".to_string())),
            ("name", ObjectFormValue::String("events".to_string())),
            ("columns", ObjectFormValue::String("id UInt64".to_string())),
            ("engine", ObjectFormValue::String("MergeTree".to_string())),
            ("order_by", ObjectFormValue::String("id".to_string())),
        ]),
    })
    .expect("ddl");

    assert_eq!(ddl.len(), 1);
    assert!(ddl[0].contains("CREATE TABLE `analytics`.`events`"));
    assert!(ddl[0].contains("ENGINE = MergeTree"));
    assert!(ddl[0].contains("ORDER BY id"));
}

#[test]
fn clickhouse_drop_form_requires_confirmation() {
    let error = clickhouse_object_form_ddl(&ObjectFormDdlRequest {
        kind_id: "table".to_string(),
        mode: ObjectFormMode::Drop,
        object_ref: None,
        values: form_values(&[
            ("database", ObjectFormValue::String("analytics".to_string())),
            ("name", ObjectFormValue::String("events".to_string())),
        ]),
    })
    .expect_err("confirmation required");

    assert!(error.to_string().contains("Drop confirmation"));
}
