//! Live MySQL contract test. Gated on `MYSQL_CONTRACT_INTEGRATION=1`; connects
//! using `MYSQL_CONTRACT_{HOST,PORT,USER,PASSWORD,DATABASE}` (defaults target the
//! local docker container started for the schema-engine migration).

use std::sync::Arc;

use zqlz_core::{Connection, ConnectionConfig, DatabaseDriver, ForeignKeyAction};
use zqlz_driver_mysql::MySqlDriver;
use zqlz_schema_engine::contract::{run_schema_contract, ContractExpectations};

fn enabled() -> bool {
    std::env::var("MYSQL_CONTRACT_INTEGRATION").ok().as_deref() == Some("1")
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

async fn connect() -> Arc<dyn Connection> {
    let host = env("MYSQL_CONTRACT_HOST", "127.0.0.1");
    let port: u16 = env("MYSQL_CONTRACT_PORT", "53306").parse().expect("port");
    let database = env("MYSQL_CONTRACT_DATABASE", "zqlz");
    let user = env("MYSQL_CONTRACT_USER", "root");
    let password = env("MYSQL_CONTRACT_PASSWORD", "zqlz");

    let mut config = ConnectionConfig::new_mysql(&host, port, &database, &user);
    config.password = Some(password);

    MySqlDriver::new()
        .connect(&config)
        .await
        .expect("connect to mysql")
}

async fn exec(connection: &Arc<dyn Connection>, sql: &str) {
    connection
        .execute(sql, &[])
        .await
        .unwrap_or_else(|error| panic!("exec failed ({sql}): {error}"));
}

#[tokio::test]
async fn mysql_satisfies_schema_contract() {
    if !enabled() {
        eprintln!("skipping; set MYSQL_CONTRACT_INTEGRATION=1");
        return;
    }

    let connection = connect().await;
    exec(&connection, "DROP TABLE IF EXISTS contract_orders").await;
    exec(&connection, "DROP TABLE IF EXISTS contract_customers").await;
    exec(
        &connection,
        "CREATE TABLE contract_customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)",
    )
    .await;
    exec(
        &connection,
        "CREATE TABLE contract_orders (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            customer_id INT NOT NULL,
            total DECIMAL(10,2),
            CONSTRAINT total_positive CHECK (total >= 0),
            CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES contract_customers(id) ON DELETE CASCADE
        )",
    )
    .await;

    let introspection = connection
        .as_schema_introspection()
        .expect("mysql exposes schema introspection");

    let expectations = ContractExpectations {
        schema: None,
        tables: vec![
            "contract_customers".to_string(),
            "contract_orders".to_string(),
        ],
        expect_table_ddl: false,
    };
    run_schema_contract(introspection, &expectations)
        .await
        .expect("mysql should satisfy the schema contract");

    let orders = introspection
        .get_table(None, "contract_orders")
        .await
        .expect("get_table contract_orders");

    let id = orders.columns.iter().find(|c| c.name == "id").expect("id column");
    assert!(id.is_primary_key, "orders.id should be flagged primary key");
    assert!(id.is_auto_increment, "AUTO_INCREMENT should be detected");

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
