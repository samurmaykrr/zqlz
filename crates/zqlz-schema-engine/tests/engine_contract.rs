//! DB-free engine coverage: drive the engine with a recorded snapshot and run
//! the shared contract plus targeted normalization/composition assertions.

use std::sync::Arc;

use zqlz_core::{
    AutoIncrementRules, CatalogCapabilities, ForeignKeyAction, NamespaceModel, ObjectKindSupport,
    RawColumnRow, RawConstraintRow, RawForeignKeyRow, RawIdentity, RawIndexRow, RawRelationRow,
    RelationRef, SchemaIntrospection, TableType,
};
use zqlz_schema_engine::{
    contract::{run_schema_contract, ContractExpectations},
    CatalogSnapshot, DefaultDialect, RecordedCatalog, SchemaEngine,
};

fn postgres_like_capabilities() -> CatalogCapabilities {
    CatalogCapabilities {
        driver_id: "fixture-pg".to_string(),
        server_version: Some("16.0".to_string()),
        namespaces: NamespaceModel::DatabasesAndSchemas {
            default_schema: "public".to_string(),
        },
        objects: ObjectKindSupport::ALL_RELATIONAL,
        auto_increment: AutoIncrementRules {
            default_markers: vec!["nextval(".to_string()],
            integer_primary_key: false,
        },
        stored_source: Vec::new(),
        deferrable_constraints: true,
        panel_extras: Vec::new(),
    }
}

fn orders_relation() -> RelationRef {
    RelationRef::new(Some("public".to_string()), "orders")
}

fn build_snapshot() -> CatalogSnapshot {
    let orders = orders_relation();
    CatalogSnapshot::new(postgres_like_capabilities())
        .with_relations(
            Some("public"),
            vec![{
                let mut row = RawRelationRow::new("orders", TableType::Table);
                row.schema = Some("public".to_string());
                row.row_estimate = Some(42);
                row.index_count = Some(1);
                row
            }],
        )
        .with_columns(
            &orders,
            vec![
                RawColumnRow {
                    name: "id".to_string(),
                    ordinal: 1,
                    data_type: "integer".to_string(),
                    is_nullable: false,
                    default_value: Some("nextval('orders_id_seq'::regclass)".to_string()),
                    identity: RawIdentity::Unknown,
                    ..Default::default()
                },
                RawColumnRow {
                    name: "customer_id".to_string(),
                    ordinal: 2,
                    data_type: "integer".to_string(),
                    is_nullable: true,
                    identity: RawIdentity::None,
                    ..Default::default()
                },
            ],
        )
        .with_constraints(
            &orders,
            vec![RawConstraintRow {
                name: "orders_pkey".to_string(),
                kind: "PRIMARY KEY".to_string(),
                columns: vec!["id".to_string()],
                definition: None,
            }],
        )
        .with_indexes(
            &orders,
            vec![RawIndexRow {
                name: "orders_pkey".to_string(),
                columns: vec!["id".to_string()],
                is_unique: true,
                is_primary: true,
                ..Default::default()
            }],
        )
        .with_foreign_keys(
            &orders,
            vec![RawForeignKeyRow {
                name: "orders_customer_id_fkey".to_string(),
                columns: vec!["customer_id".to_string()],
                referenced_table: "customers".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some("CASCADE".to_string()),
                on_update: Some("a".to_string()),
                ..Default::default()
            }],
        )
}

fn engine() -> SchemaEngine {
    let source = Arc::new(RecordedCatalog::new(build_snapshot()));
    SchemaEngine::new(source, Arc::new(DefaultDialect))
}

#[tokio::test]
async fn recorded_engine_satisfies_contract() {
    let engine = engine();
    let expectations = ContractExpectations {
        schema: Some("public".to_string()),
        tables: vec!["orders".to_string()],
        expect_table_ddl: false,
    };
    run_schema_contract(&engine, &expectations)
        .await
        .expect("contract should hold");
}

#[tokio::test]
async fn engine_applies_auto_increment_and_fk_heuristics() {
    let engine = engine();
    let details = engine
        .get_table(Some("public"), "orders")
        .await
        .expect("get_table");

    let id = details
        .columns
        .iter()
        .find(|column| column.name == "id")
        .expect("id column");
    assert!(id.is_primary_key, "id should be flagged primary key");
    assert!(
        id.is_auto_increment,
        "id should be auto-increment via the nextval( marker"
    );

    let customer_id = details
        .columns
        .iter()
        .find(|column| column.name == "customer_id")
        .expect("customer_id column");
    assert!(
        customer_id.foreign_key.is_some(),
        "customer_id should carry a backfilled foreign key reference"
    );

    let foreign_key = details.foreign_keys.first().expect("one foreign key");
    assert_eq!(foreign_key.on_delete, ForeignKeyAction::Cascade);
    assert_eq!(
        foreign_key.on_update,
        ForeignKeyAction::NoAction,
        "the 'a' code should parse to NO ACTION"
    );
}
