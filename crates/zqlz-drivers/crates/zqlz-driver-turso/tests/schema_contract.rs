//! Live Turso/libSQL contract test. Gated on `TURSO_CONTRACT_INTEGRATION=1`;
//! connects using `TURSO_CONTRACT_URL` / `TURSO_CONTRACT_AUTH_TOKEN` (defaults
//! target the local libsql-server container started for the migration).

use std::sync::Arc;

use zqlz_core::{Connection, ConnectionConfig, DatabaseDriver, ForeignKeyAction};
use zqlz_driver_turso::TursoDriver;
use zqlz_schema_engine::contract::{run_schema_contract, ContractExpectations};

fn enabled() -> bool {
    std::env::var("TURSO_CONTRACT_INTEGRATION").ok().as_deref() == Some("1")
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

async fn connect() -> Arc<dyn Connection> {
    let url = env("TURSO_CONTRACT_URL", "http://127.0.0.1:58080");
    let auth_token = env("TURSO_CONTRACT_AUTH_TOKEN", "local-dev-token");

    let mut config = ConnectionConfig::new("turso", "Turso");
    config.params.insert("url".to_string(), url);
    config.params.insert("auth_token".to_string(), auth_token);

    TursoDriver::new()
        .connect(&config)
        .await
        .expect("connect to turso/libsql")
}

async fn exec(connection: &Arc<dyn Connection>, sql: &str) {
    connection
        .execute(sql, &[])
        .await
        .unwrap_or_else(|error| panic!("exec failed ({sql}): {error}"));
}

#[tokio::test]
async fn turso_satisfies_schema_contract() {
    if !enabled() {
        eprintln!("skipping; set TURSO_CONTRACT_INTEGRATION=1");
        return;
    }

    let connection = connect().await;
    exec(&connection, "DROP TABLE IF EXISTS contract_orders").await;
    exec(&connection, "DROP TABLE IF EXISTS contract_customers").await;
    exec(
        &connection,
        "CREATE TABLE contract_customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    )
    .await;
    exec(
        &connection,
        "CREATE TABLE contract_orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES contract_customers(id) ON DELETE CASCADE,
            total INTEGER
        )",
    )
    .await;

    let introspection = connection
        .as_schema_introspection()
        .expect("turso exposes schema introspection");

    let expectations = ContractExpectations {
        schema: None,
        tables: vec![
            "contract_customers".to_string(),
            "contract_orders".to_string(),
        ],
        expect_table_ddl: true,
    };
    run_schema_contract(introspection, &expectations)
        .await
        .expect("turso should satisfy the schema contract");

    let orders = introspection
        .get_table(None, "contract_orders")
        .await
        .expect("get_table contract_orders");

    let id = orders.columns.iter().find(|c| c.name == "id").expect("id column");
    assert!(id.is_primary_key, "orders.id should be flagged primary key");
    assert!(id.is_auto_increment, "INTEGER PRIMARY KEY is auto-increment");

    let customer_id = orders
        .columns
        .iter()
        .find(|c| c.name == "customer_id")
        .expect("customer_id column");
    let fk_ref = customer_id
        .foreign_key
        .as_ref()
        .expect("customer_id should carry a backfilled foreign key");
    assert_eq!(fk_ref.table, "contract_customers");

    let fk = orders.foreign_keys.first().expect("one foreign key");
    assert_eq!(fk.on_delete, ForeignKeyAction::Cascade);

    exec(&connection, "DROP TABLE IF EXISTS contract_orders").await;
    exec(&connection, "DROP TABLE IF EXISTS contract_customers").await;
}
