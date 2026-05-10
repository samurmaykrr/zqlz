use zqlz_core::{
    CellUpdateRequest, Connection, ConnectionConfig, ConnectionFeatureSet, DatabaseDriver,
    DocumentDeleteRequest, DocumentQueryRequest, DocumentSaveRequest, DriverCategory,
    KeyValueDeleteRequest, KeyValueKind, KeyValueSaveRequest, KeyValueScanRequest, RowIdentifier,
    Value,
};
use zqlz_drivers::{
    mongodb::MongoDbDriver, mysql::MySqlDriver, postgres::PostgresDriver, redis::RedisDriver,
    sqlite::SqliteConnection,
};

fn env_value(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

fn env_port(key: &str, default: u16) -> u16 {
    env_value(key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn unique_name(prefix: &str) -> String {
    format!("{}_{}", prefix, uuid::Uuid::new_v4().simple())
}

fn postgres_config_from_env() -> Option<ConnectionConfig> {
    let host = env_value("ZQLZ_SMOKE_POSTGRES_HOST")?;
    let database = env_value("ZQLZ_SMOKE_POSTGRES_DATABASE").unwrap_or_else(|| "postgres".into());
    let username = env_value("ZQLZ_SMOKE_POSTGRES_USER").unwrap_or_else(|| "postgres".into());
    let mut config = ConnectionConfig::new_postgres(
        &host,
        env_port("ZQLZ_SMOKE_POSTGRES_PORT", 5432),
        &database,
        &username,
    );
    config.password = env_value("ZQLZ_SMOKE_POSTGRES_PASSWORD");
    Some(config.with_param(
        "ssl_mode",
        env_value("ZQLZ_SMOKE_POSTGRES_SSL_MODE").unwrap_or_else(|| "prefer".into()),
    ))
}

fn mysql_config_from_env() -> Option<ConnectionConfig> {
    let host = env_value("ZQLZ_SMOKE_MYSQL_HOST")?;
    let database = env_value("ZQLZ_SMOKE_MYSQL_DATABASE").unwrap_or_else(|| "mysql".into());
    let username = env_value("ZQLZ_SMOKE_MYSQL_USER").unwrap_or_else(|| "root".into());
    let mut config = ConnectionConfig::new_mysql(
        &host,
        env_port("ZQLZ_SMOKE_MYSQL_PORT", 3306),
        &database,
        &username,
    );
    config.password = env_value("ZQLZ_SMOKE_MYSQL_PASSWORD");
    Some(config)
}

fn redis_config_from_env() -> Option<ConnectionConfig> {
    let host = env_value("ZQLZ_SMOKE_REDIS_HOST")?;
    let mut config = ConnectionConfig::new("redis", "Redis smoke");
    config.host = host;
    config.port = env_port("ZQLZ_SMOKE_REDIS_PORT", 6379);
    config.password = env_value("ZQLZ_SMOKE_REDIS_PASSWORD");
    Some(config.with_param(
        "database",
        env_value("ZQLZ_SMOKE_REDIS_DATABASE").unwrap_or_else(|| "0".into()),
    ))
}

fn mongodb_config_from_env() -> Option<ConnectionConfig> {
    let host = env_value("ZQLZ_SMOKE_MONGODB_HOST")?;
    let mut config = ConnectionConfig::new("mongodb", "MongoDB smoke");
    config.host = host;
    config.port = env_port("ZQLZ_SMOKE_MONGODB_PORT", 27017);
    config.database =
        Some(env_value("ZQLZ_SMOKE_MONGODB_DATABASE").unwrap_or_else(|| "zqlz_smoke".into()));
    config.username = env_value("ZQLZ_SMOKE_MONGODB_USER");
    config.password = env_value("ZQLZ_SMOKE_MONGODB_PASSWORD");
    if let Some(auth_source) = env_value("ZQLZ_SMOKE_MONGODB_AUTH_SOURCE") {
        config = config.with_param("authSource", auth_source);
    }
    Some(config)
}

async fn relational_smoke(connection: &dyn Connection, table_name: &str) {
    let create_sql =
        format!("CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name VARCHAR(64) NOT NULL)");
    connection
        .execute(&create_sql, &[])
        .await
        .expect("create smoke table");

    let insert_sql = format!("INSERT INTO {table_name} (id, name) VALUES (1, 'Ada')");
    connection
        .execute(&insert_sql, &[])
        .await
        .expect("insert smoke row");

    let query_sql = format!("SELECT name FROM {table_name} WHERE id = 1");
    let query_result = connection
        .query(&query_sql, &[])
        .await
        .expect("query smoke row");
    assert_eq!(query_result.row_count(), 1);

    let missing_error = connection
        .query("SELECT * FROM zqlz_missing_smoke_table", &[])
        .await
        .expect_err("missing table should error");
    assert!(!missing_error.to_string().is_empty());

    let drop_sql = format!("DROP TABLE {table_name}");
    connection
        .execute(&drop_sql, &[])
        .await
        .expect("drop smoke table");
}

#[tokio::test]
async fn sqlite_smoke_covers_core_supported_capabilities() {
    let connection = SqliteConnection::open(":memory:").expect("open sqlite memory database");

    let features = ConnectionFeatureSet::from_connection(&connection, None);
    assert_eq!(features.driver_name, "sqlite");
    assert_eq!(features.driver_category, DriverCategory::Relational);
    assert!(features.schema_introspection.available);
    assert!(features.query.execute.available);
    assert!(features.query.explain.available);
    assert!(features.query.cancel.available);
    assert!(features.data_editing.browse_rows.available);
    assert!(features.data_editing.edit_cells.available);
    assert!(!features.stores.key_value.available);
    assert!(!features.stores.document.available);

    connection
        .execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("create smoke table");
    connection
        .execute("INSERT INTO users (id, name) VALUES (1, 'Ada')", &[])
        .await
        .expect("insert smoke row");

    let schema = connection
        .as_schema_introspection()
        .expect("sqlite schema introspection");
    let tables = schema.list_tables(None).await.expect("list sqlite tables");
    assert!(tables.iter().any(|table| table.name == "users"));

    let query_result = connection
        .query("SELECT name FROM users WHERE id = 1", &[])
        .await
        .expect("query smoke row");
    assert_eq!(query_result.row_count(), 1);
    assert_eq!(
        query_result.rows[0].get_by_name("name"),
        Some(&Value::String("Ada".to_string()))
    );

    let affected = connection
        .update_cell(CellUpdateRequest {
            table_name: "users".to_string(),
            column_name: "name".to_string(),
            column_type: Some("TEXT".to_string()),
            row_column_types: vec![("id".to_string(), "INTEGER".to_string())],
            new_value: Some(Value::String("Grace".to_string())),
            row_identifier: RowIdentifier::PrimaryKey(vec![("id".to_string(), Value::Int64(1))]),
        })
        .await
        .expect("update sqlite cell");
    assert_eq!(affected, 1);

    let updated = connection
        .query("SELECT name FROM users WHERE id = 1", &[])
        .await
        .expect("query updated row");
    assert_eq!(
        updated.rows[0].get_by_name("name"),
        Some(&Value::String("Grace".to_string()))
    );

    let error = connection
        .query("SELECT * FROM missing_table", &[])
        .await
        .expect_err("missing table should error");
    assert!(!error.to_string().is_empty());
}

#[tokio::test]
async fn postgres_smoke_covers_env_gated_supported_capabilities() {
    let Some(config) = postgres_config_from_env() else {
        eprintln!("skipping postgres smoke; set ZQLZ_SMOKE_POSTGRES_HOST");
        return;
    };

    let connection = PostgresDriver::new()
        .connect(&config)
        .await
        .expect("connect postgres smoke database");
    let features = ConnectionFeatureSet::from_connection(connection.as_ref(), None);
    assert_eq!(features.driver_category, DriverCategory::Relational);
    assert!(features.query.execute.available);
    assert!(features.query.explain.available);
    assert!(features.query.cancel.available);
    assert!(features.schema_introspection.available);

    relational_smoke(connection.as_ref(), &unique_name("zqlz_pg_smoke")).await;
}

#[tokio::test]
async fn mysql_smoke_covers_env_gated_supported_capabilities() {
    let Some(config) = mysql_config_from_env() else {
        eprintln!("skipping mysql smoke; set ZQLZ_SMOKE_MYSQL_HOST");
        return;
    };

    let connection = MySqlDriver::new()
        .connect(&config)
        .await
        .expect("connect mysql smoke database");
    let features = ConnectionFeatureSet::from_connection(connection.as_ref(), None);
    assert_eq!(features.driver_category, DriverCategory::Relational);
    assert!(features.query.execute.available);
    assert!(features.query.explain.available);
    assert!(features.query.cancel.available);
    assert!(features.schema_introspection.available);

    relational_smoke(connection.as_ref(), &unique_name("zqlz_mysql_smoke")).await;
}

#[tokio::test]
async fn redis_smoke_covers_env_gated_supported_capabilities() {
    let Some(config) = redis_config_from_env() else {
        eprintln!("skipping redis smoke; set ZQLZ_SMOKE_REDIS_HOST");
        return;
    };

    let connection = RedisDriver::new()
        .connect(&config)
        .await
        .expect("connect redis smoke database");
    let features = ConnectionFeatureSet::from_connection(connection.as_ref(), None);
    assert_eq!(features.driver_category, DriverCategory::KeyValue);
    assert!(features.query.execute.available);
    assert!(features.stores.key_value.available);
    assert!(!features.stores.document.available);

    let store = connection
        .as_key_value_store()
        .expect("redis key-value store");
    let key = unique_name("zqlz:smoke");
    store
        .save_key(KeyValueSaveRequest {
            original_key: String::new(),
            new_key: key.clone(),
            kind: KeyValueKind::String,
            serialized_value: "value".to_string(),
            ttl_seconds: Some(60),
            temporary_key: format!("{key}:tmp"),
        })
        .await
        .expect("save redis smoke key");

    let entry = store
        .read_key(&key, 100)
        .await
        .expect("read redis smoke key");
    assert_eq!(entry.key, key);

    let scan = store
        .scan_keys(KeyValueScanRequest {
            database_index: env_port("ZQLZ_SMOKE_REDIS_DATABASE", 0),
            limit: 10_000,
            scan_batch_size: 500,
        })
        .await
        .expect("scan redis smoke keys");
    assert!(scan.keys.iter().any(|summary| summary == &key));

    let deleted = store
        .delete_keys(KeyValueDeleteRequest {
            key_names: vec![key.clone()],
            continue_on_error: false,
        })
        .await
        .expect("delete redis smoke key");
    assert_eq!(deleted.deleted_key_names, vec![key]);
}

#[tokio::test]
async fn mongodb_smoke_covers_env_gated_supported_capabilities() {
    let Some(config) = mongodb_config_from_env() else {
        eprintln!("skipping mongodb smoke; set ZQLZ_SMOKE_MONGODB_HOST");
        return;
    };

    let database = config
        .database
        .clone()
        .expect("mongodb database configured");
    let collection = unique_name("zqlz_smoke");
    let connection = MongoDbDriver::new()
        .connect(&config)
        .await
        .expect("connect mongodb smoke database");
    let features = ConnectionFeatureSet::from_connection(connection.as_ref(), None);
    assert_eq!(features.driver_category, DriverCategory::Document);
    assert!(features.query.execute.available);
    assert!(features.stores.document.available);
    assert!(!features.stores.key_value.available);

    let store = connection
        .as_document_store()
        .expect("mongodb document store");
    let inserted_id = store
        .insert_document(DocumentSaveRequest {
            database: database.clone(),
            collection: collection.clone(),
            document_json: r#"{"name":"Ada"}"#.to_string(),
        })
        .await
        .expect("insert mongodb smoke document");
    let inserted_id = match inserted_id {
        Value::String(value) => value,
        other => panic!("expected MongoDB ObjectId string, got {other:?}"),
    };

    let query_result = store
        .query_documents(DocumentQueryRequest {
            database: database.clone(),
            collection: collection.clone(),
            filter_json: Some(r#"{"name":"Ada"}"#.to_string()),
            projection_json: None,
            sort_json: None,
            skip: 0,
            limit: 10,
        })
        .await
        .expect("query mongodb smoke document");
    assert!(query_result.row_count() >= 1);

    let deleted = store
        .delete_documents(DocumentDeleteRequest {
            database,
            collection,
            ids_json: vec![inserted_id],
            continue_on_error: false,
        })
        .await
        .expect("delete mongodb smoke document");
    assert_eq!(deleted.deleted_ids.len(), 1);
}
