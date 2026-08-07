//! SQLite is the always-live contract target: open an in-memory database,
//! seed a representative schema, and run the shared schema-introspection
//! contract against the real engine end-to-end with no environment needed.

use zqlz_core::Connection;
use zqlz_driver_sqlite::SqliteConnection;
use zqlz_schema_engine::contract::{run_schema_contract, ContractExpectations};

const CONTRACT_DDL: &[&str] = &[
    "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    "CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
        total INTEGER
    )",
    "CREATE UNIQUE INDEX orders_customer_total_idx ON orders(customer_id, total)",
    "CREATE VIEW active_orders AS SELECT * FROM orders WHERE total > 0",
    "CREATE TRIGGER orders_ai AFTER INSERT ON orders BEGIN SELECT 1; END",
];

async fn seeded_connection() -> SqliteConnection {
    let connection = SqliteConnection::open(":memory:").expect("open in-memory sqlite");
    for statement in CONTRACT_DDL {
        connection
            .execute(statement, &[])
            .await
            .unwrap_or_else(|error| panic!("seed statement failed ({statement}): {error}"));
    }
    connection
}

#[tokio::test]
async fn sqlite_satisfies_schema_contract() {
    let connection = seeded_connection().await;
    let introspection = connection
        .as_schema_introspection()
        .expect("sqlite exposes schema introspection");

    let expectations = ContractExpectations {
        schema: None,
        tables: vec!["customers".to_string(), "orders".to_string()],
        expect_table_ddl: true,
    };

    run_schema_contract(introspection, &expectations)
        .await
        .expect("sqlite should satisfy the schema contract");
}

#[tokio::test]
async fn sqlite_resolves_foreign_keys_and_auto_increment() {
    let connection = seeded_connection().await;
    let introspection = connection
        .as_schema_introspection()
        .expect("sqlite exposes schema introspection");

    let orders = introspection
        .get_table(None, "orders")
        .await
        .expect("get_table orders");

    let id = orders
        .columns
        .iter()
        .find(|column| column.name == "id")
        .expect("id column");
    assert!(id.is_primary_key, "orders.id should be the primary key");
    assert!(
        id.is_auto_increment,
        "INTEGER PRIMARY KEY should be treated as auto-increment"
    );

    let customer_id = orders
        .columns
        .iter()
        .find(|column| column.name == "customer_id")
        .expect("customer_id column");
    let foreign_key = customer_id
        .foreign_key
        .as_ref()
        .expect("customer_id should carry a backfilled foreign key");
    assert_eq!(foreign_key.table, "customers");
    assert_eq!(foreign_key.column, "id");

    let fk = orders.foreign_keys.first().expect("one foreign key");
    assert_eq!(fk.on_delete, zqlz_core::ForeignKeyAction::Cascade);

    // The composite unique index marks neither column unique on its own.
    assert!(!customer_id.is_unique);

    // Triggers are now composed into get_table (they were dropped before).
    assert!(
        orders.triggers.iter().any(|trigger| trigger.name == "orders_ai"),
        "orders trigger should be present in table details"
    );
}
