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
fn duckdb_manifest_exposes_core_kinds_and_forms() {
    let manifest = duckdb_objects_panel_manifest();
    manifest.validate().expect("valid manifest");
    let kind_ids: Vec<&str> = manifest
        .object_kinds
        .iter()
        .map(|kind| kind.id.as_str())
        .collect();
    assert!(kind_ids.contains(&"schema"));
    assert!(kind_ids.contains(&"table"));
    assert!(kind_ids.contains(&"view"));
    assert!(kind_ids.contains(&"sequence"));
    assert!(
        manifest
            .toolbar_actions
            .iter()
            .any(|action| action.object_form.is_some())
    );
}

#[test]
fn duckdb_identifier_quoting_escapes_quotes() {
    assert_eq!(duckdb_quote_identifier("a\"b"), "\"a\"\"b\"");
}

#[test]
fn duckdb_view_form_generates_qualified_view() {
    let ddl = duckdb_object_form_ddl(&ObjectFormDdlRequest {
        kind_id: "view".to_string(),
        mode: ObjectFormMode::Create,
        object_ref: None,
        values: form_values(&[
            ("schema", ObjectFormValue::String("main".to_string())),
            ("name", ObjectFormValue::String("active_orders".to_string())),
            (
                "query",
                ObjectFormValue::String("SELECT 1 AS id".to_string()),
            ),
        ]),
    })
    .expect("ddl");

    assert_eq!(
        ddl,
        vec!["CREATE VIEW \"main\".\"active_orders\" AS SELECT 1 AS id"]
    );
}

#[test]
fn duckdb_drop_form_requires_confirmation() {
    let error = duckdb_object_form_ddl(&ObjectFormDdlRequest {
        kind_id: "view".to_string(),
        mode: ObjectFormMode::Drop,
        object_ref: None,
        values: form_values(&[
            ("schema", ObjectFormValue::String("main".to_string())),
            ("name", ObjectFormValue::String("active_orders".to_string())),
        ]),
    })
    .expect_err("confirmation required");

    assert!(error.to_string().contains("Drop confirmation"));
}
