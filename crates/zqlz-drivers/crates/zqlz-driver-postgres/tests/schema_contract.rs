//! Live Postgres contract test. Gated on `PG_CONTRACT_INTEGRATION=1`; connects
//! using `PG_CONTRACT_{HOST,PORT,USER,PASSWORD,DATABASE}` (defaults target the
//! local docker container started for the schema-engine migration).

use std::sync::Arc;

use zqlz_core::{Connection, ConnectionConfig, DatabaseDriver, ForeignKeyAction};
use zqlz_driver_postgres::PostgresDriver;
use zqlz_schema_engine::contract::{run_schema_contract, ContractExpectations};

fn enabled() -> bool {
    std::env::var("PG_CONTRACT_INTEGRATION").ok().as_deref() == Some("1")
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

async fn connect() -> Arc<dyn Connection> {
    let host = env("PG_CONTRACT_HOST", "127.0.0.1");
    let port: u16 = env("PG_CONTRACT_PORT", "55432").parse().expect("port");
    let database = env("PG_CONTRACT_DATABASE", "zqlz");
    let user = env("PG_CONTRACT_USER", "zqlz");
    let password = env("PG_CONTRACT_PASSWORD", "zqlz");

    let mut config = ConnectionConfig::new_postgres(&host, port, &database, &user);
    config.password = Some(password);

    PostgresDriver::new()
        .connect(&config)
        .await
        .expect("connect to postgres")
}

async fn exec(connection: &Arc<dyn Connection>, sql: &str) {
    connection
        .execute(sql, &[])
        .await
        .unwrap_or_else(|error| panic!("exec failed ({sql}): {error}"));
}

#[tokio::test]
async fn postgres_satisfies_schema_contract() {
    if !enabled() {
        eprintln!("skipping; set PG_CONTRACT_INTEGRATION=1");
        return;
    }

    let connection = connect().await;
    exec(&connection, "DROP TABLE IF EXISTS contract_orders CASCADE").await;
    exec(&connection, "DROP TABLE IF EXISTS contract_customers CASCADE").await;
    exec(
        &connection,
        "CREATE TABLE contract_customers (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
    )
    .await;
    exec(
        &connection,
        "CREATE TABLE contract_orders (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES contract_customers(id) ON DELETE CASCADE,
            total NUMERIC(10,2),
            CONSTRAINT total_positive CHECK (total >= 0)
        )",
    )
    .await;
    exec(
        &connection,
        "CREATE UNIQUE INDEX contract_orders_total_idx ON contract_orders(total)",
    )
    .await;

    let introspection = connection
        .as_schema_introspection()
        .expect("postgres exposes schema introspection");

    let expectations = ContractExpectations {
        schema: Some("public".to_string()),
        tables: vec![
            "contract_customers".to_string(),
            "contract_orders".to_string(),
        ],
        expect_table_ddl: true,
    };
    run_schema_contract(introspection, &expectations)
        .await
        .expect("postgres should satisfy the schema contract");

    let orders = introspection
        .get_table(Some("public"), "contract_orders")
        .await
        .expect("get_table contract_orders");

    // SERIAL/IDENTITY auto-increment heuristic (now owned by the engine).
    let id = orders.columns.iter().find(|c| c.name == "id").expect("id column");
    assert!(id.is_primary_key, "orders.id should be flagged primary key");
    assert!(id.is_auto_increment, "GENERATED IDENTITY should be auto-increment");

    // Foreign-key action parsing + single-column backfill (engine-owned).
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
    assert_eq!(fk_ref.column, "id");

    let fk = orders.foreign_keys.first().expect("one foreign key");
    assert_eq!(fk.on_delete, ForeignKeyAction::Cascade);

    // CHECK constraint surfaced through the engine's constraint normalization.
    assert!(
        orders
            .constraints
            .iter()
            .any(|c| c.name == "total_positive"),
        "CHECK constraint should be present"
    );

    exec(&connection, "DROP TABLE IF EXISTS contract_orders CASCADE").await;
    exec(&connection, "DROP TABLE IF EXISTS contract_customers CASCADE").await;
}
