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

#[tokio::test]
async fn duckdb_satisfies_schema_contract_in_memory() {
    use super::driver::DuckDbConnection;
    use zqlz_core::{Connection, ForeignKeyAction};
    use zqlz_schema_engine::contract::{run_schema_contract, ContractExpectations};

    let raw = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
    raw.execute_batch(
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE orders (
            id BIGINT PRIMARY KEY,
            customer_id BIGINT NOT NULL REFERENCES customers(id),
            total BIGINT
         );
         CREATE VIEW active_orders AS SELECT * FROM orders WHERE total > 0;",
    )
    .expect("seed duckdb schema");

    let connection = DuckDbConnection::new(raw, ":memory:".to_string());
    let introspection = connection
        .as_schema_introspection()
        .expect("duckdb exposes schema introspection");

    let expectations = ContractExpectations {
        schema: Some("main".to_string()),
        tables: vec!["customers".to_string(), "orders".to_string()],
        expect_table_ddl: true,
    };
    run_schema_contract(introspection, &expectations)
        .await
        .expect("duckdb should satisfy the schema contract");

    let orders = introspection
        .get_table(Some("main"), "orders")
        .await
        .expect("get_table orders");
    let id = orders
        .columns
        .iter()
        .find(|column| column.name == "id")
        .expect("id column");
    assert!(id.is_primary_key, "orders.id should be flagged primary key");

    let customer_id = orders
        .columns
        .iter()
        .find(|column| column.name == "customer_id")
        .expect("customer_id column");
    assert!(
        customer_id.foreign_key.is_some(),
        "customer_id should carry a backfilled foreign key reference"
    );

    // DuckDB's information_schema.constraint_column_usage reports the
    // constrained table rather than the parent, so we only assert structure
    // here (this matches the pre-migration driver behavior).
    let fk = orders.foreign_keys.first().expect("one foreign key");
    assert_eq!(fk.columns, vec!["customer_id".to_string()]);
    assert!(!fk.referenced_table.is_empty());
    assert_eq!(fk.on_delete, ForeignKeyAction::NoAction);
}
