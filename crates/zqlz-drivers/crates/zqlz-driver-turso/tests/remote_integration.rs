use zqlz_core::{ConnectionConfig, DatabaseDriver, Value};
use zqlz_driver_turso::TursoDriver;

fn turso_config_from_env() -> Option<ConnectionConfig> {
    let url = std::env::var("TURSO_DATABASE_URL").ok()?;
    let auth_token = std::env::var("TURSO_AUTH_TOKEN").ok()?;
    let config = ConnectionConfig::new("turso", "Turso Integration")
        .with_param("url", url)
        .with_param("auth_token", auth_token);
    Some(config)
}

#[tokio::test]
async fn turso_remote_smoke_test_when_env_is_available() {
    let Some(config) = turso_config_from_env() else {
        eprintln!("Skipping Turso integration test: TURSO_DATABASE_URL/TURSO_AUTH_TOKEN unset");
        return;
    };

    let driver = TursoDriver::new();
    let connection = driver.connect(&config).await.expect("connect to Turso");
    let table_name = format!("zqlz_turso_test_{}", uuid::Uuid::new_v4().simple());
    let quoted_table = connection.quote_identifier(&table_name);

    let setup_sql = format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        quoted_table
    );
    connection
        .execute(&setup_sql, &[])
        .await
        .expect("create Turso test table");

    let insert_sql = format!("INSERT INTO {} (name) VALUES (?)", quoted_table);
    connection
        .execute(&insert_sql, &[Value::String("Ada".to_string())])
        .await
        .expect("insert Turso test row");

    let select_sql = format!("SELECT name FROM {} WHERE name = ?", quoted_table);
    let result = connection
        .query(&select_sql, &[Value::String("Ada".to_string())])
        .await
        .expect("query Turso test row");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get(0).and_then(Value::as_str), Some("Ada"));

    let schema = connection
        .as_schema_introspection()
        .expect("Turso should support schema introspection");
    let tables = schema.list_tables(None).await.expect("list Turso tables");
    assert!(tables.iter().any(|table| table.name == table_name));
    let columns = schema
        .get_columns(None, &table_name)
        .await
        .expect("list Turso columns");
    assert!(columns.iter().any(|column| column.name == "name"));

    let drop_sql = format!("DROP TABLE {}", quoted_table);
    connection
        .execute(&drop_sql, &[])
        .await
        .expect("drop Turso test table");
}
