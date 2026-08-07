//! Live ClickHouse contract test. Gated on `CH_CONTRACT_INTEGRATION=1`; connects
//! using `CH_CONTRACT_{HOST,PORT,USER,PASSWORD,DATABASE}` (defaults target the
//! local docker container started for the schema-engine migration).

use std::sync::Arc;

use zqlz_core::{Connection, ConnectionConfig, DatabaseDriver};
use zqlz_driver_clickhouse::ClickHouseDriver;
use zqlz_schema_engine::contract::{run_schema_contract, ContractExpectations};

fn enabled() -> bool {
    std::env::var("CH_CONTRACT_INTEGRATION").ok().as_deref() == Some("1")
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

async fn connect() -> Arc<dyn Connection> {
    let host = env("CH_CONTRACT_HOST", "127.0.0.1");
    let port: u16 = env("CH_CONTRACT_PORT", "58123").parse().expect("port");
    let database = env("CH_CONTRACT_DATABASE", "default");
    let user = env("CH_CONTRACT_USER", "default");
    let password = env("CH_CONTRACT_PASSWORD", "zqlz");

    let mut config = ConnectionConfig::new("clickhouse", "ClickHouse");
    config.port = port;
    config.database = Some(database);
    config.username = Some(user);
    config.password = Some(password);
    config.params.insert("host".to_string(), host);

    ClickHouseDriver::new()
        .connect(&config)
        .await
        .expect("connect to clickhouse")
}

async fn exec(connection: &Arc<dyn Connection>, sql: &str) {
    connection
        .execute(sql, &[])
        .await
        .unwrap_or_else(|error| panic!("exec failed ({sql}): {error}"));
}

#[tokio::test]
async fn clickhouse_satisfies_schema_contract() {
    if !enabled() {
        eprintln!("skipping; set CH_CONTRACT_INTEGRATION=1");
        return;
    }

    let connection = connect().await;
    exec(&connection, "DROP TABLE IF EXISTS contract_orders").await;
    exec(
        &connection,
        "CREATE TABLE contract_orders (
            id UInt64,
            customer_id UInt64,
            total Decimal(10,2),
            note Nullable(String)
        ) ENGINE = MergeTree() ORDER BY (id, customer_id)",
    )
    .await;

    let introspection = connection
        .as_schema_introspection()
        .expect("clickhouse exposes schema introspection");

    let expectations = ContractExpectations {
        schema: Some("default".to_string()),
        tables: vec!["contract_orders".to_string()],
        expect_table_ddl: true,
    };
    run_schema_contract(introspection, &expectations)
        .await
        .expect("clickhouse should satisfy the schema contract");

    let orders = introspection
        .get_table(Some("default"), "contract_orders")
        .await
        .expect("get_table contract_orders");

    // ORDER BY (id, customer_id) is the MergeTree primary key.
    let id = orders.columns.iter().find(|c| c.name == "id").expect("id column");
    assert!(id.is_primary_key, "id should be flagged primary key");
    let customer_id = orders
        .columns
        .iter()
        .find(|c| c.name == "customer_id")
        .expect("customer_id column");
    assert!(customer_id.is_primary_key, "customer_id is part of the primary key");

    let note = orders.columns.iter().find(|c| c.name == "note").expect("note column");
    assert!(note.nullable, "Nullable(String) should be nullable");

    // ClickHouse synthesizes a PRIMARY KEY constraint from is_in_primary_key.
    assert!(
        orders
            .constraints
            .iter()
            .any(|c| c.name == "PRIMARY KEY"),
        "primary key constraint should be present"
    );

    exec(&connection, "DROP TABLE IF EXISTS contract_orders").await;
}
