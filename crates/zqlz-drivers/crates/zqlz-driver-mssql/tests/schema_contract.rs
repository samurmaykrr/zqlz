//! Live MS SQL Server contract test. Gated on `MSSQL_CONTRACT_INTEGRATION=1`;
//! connects using `MSSQL_CONTRACT_{HOST,PORT,USER,PASSWORD,DATABASE}` (defaults
//! target the local docker container started for the schema-engine migration).

use std::sync::Arc;

use zqlz_core::{Connection, ConnectionConfig, DatabaseDriver, ForeignKeyAction};
use zqlz_driver_mssql::MssqlDriver;
use zqlz_schema_engine::contract::{run_schema_contract, ContractExpectations};

fn enabled() -> bool {
    std::env::var("MSSQL_CONTRACT_INTEGRATION").ok().as_deref() == Some("1")
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

async fn connect() -> Arc<dyn Connection> {
    let host = env("MSSQL_CONTRACT_HOST", "127.0.0.1");
    let port: u16 = env("MSSQL_CONTRACT_PORT", "51433").parse().expect("port");
    let database = env("MSSQL_CONTRACT_DATABASE", "zqlz");
    let user = env("MSSQL_CONTRACT_USER", "sa");
    let password = env("MSSQL_CONTRACT_PASSWORD", "Zqlz_Passw0rd");

    let mut config = ConnectionConfig::new("mssql", "MSSQL");
    config.host = host;
    config.port = port;
    config.database = Some(database);
    config.username = Some(user);
    config.password = Some(password);
    config
        .params
        .insert("trust_certificate".to_string(), "true".to_string());

    MssqlDriver::new()
        .connect(&config)
        .await
        .expect("connect to mssql")
}

async fn exec(connection: &Arc<dyn Connection>, sql: &str) {
    connection
        .execute(sql, &[])
        .await
        .unwrap_or_else(|error| panic!("exec failed ({sql}): {error}"));
}

#[tokio::test]
async fn mssql_satisfies_schema_contract() {
    if !enabled() {
        eprintln!("skipping; set MSSQL_CONTRACT_INTEGRATION=1");
        return;
    }

    let connection = connect().await;
    exec(&connection, "IF OBJECT_ID('dbo.contract_orders','U') IS NOT NULL DROP TABLE dbo.contract_orders").await;
    exec(&connection, "IF OBJECT_ID('dbo.contract_customers','U') IS NOT NULL DROP TABLE dbo.contract_customers").await;
    exec(
        &connection,
        "CREATE TABLE dbo.contract_customers (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(255) NOT NULL)",
    )
    .await;
    exec(
        &connection,
        "CREATE TABLE dbo.contract_orders (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            customer_id INT NOT NULL,
            total DECIMAL(10,2),
            CONSTRAINT total_positive CHECK (total >= 0),
            CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES dbo.contract_customers(id) ON DELETE CASCADE
        )",
    )
    .await;

    let introspection = connection
        .as_schema_introspection()
        .expect("mssql exposes schema introspection");

    let expectations = ContractExpectations {
        schema: Some("dbo".to_string()),
        tables: vec![
            "contract_customers".to_string(),
            "contract_orders".to_string(),
        ],
        expect_table_ddl: false,
    };
    run_schema_contract(introspection, &expectations)
        .await
        .expect("mssql should satisfy the schema contract");

    let orders = introspection
        .get_table(Some("dbo"), "contract_orders")
        .await
        .expect("get_table contract_orders");

    let id = orders.columns.iter().find(|c| c.name == "id").expect("id column");
    assert!(id.is_primary_key, "orders.id should be flagged primary key");
    assert!(id.is_auto_increment, "IDENTITY should be detected as auto-increment");

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

    exec(&connection, "IF OBJECT_ID('dbo.contract_orders','U') IS NOT NULL DROP TABLE dbo.contract_orders").await;
    exec(&connection, "IF OBJECT_ID('dbo.contract_customers','U') IS NOT NULL DROP TABLE dbo.contract_customers").await;
}
