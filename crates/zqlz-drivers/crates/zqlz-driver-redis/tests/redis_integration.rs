use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{
    ConnectionConfig, DatabaseDriver, KeyValueKind, KeyValueSaveRequest, KeyValueScanRequest, Value,
};
use zqlz_driver_redis::RedisDriver;

fn integration_enabled() -> bool {
    std::env::var("ZQLZ_REDIS_INTEGRATION").ok().as_deref() == Some("1")
}

fn redis_config(database: u16) -> ConnectionConfig {
    let mut config = ConnectionConfig::new("redis", "Redis integration")
        .with_param(
            "host",
            std::env::var("ZQLZ_REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
        )
        .with_param(
            "port",
            std::env::var("ZQLZ_REDIS_PORT").unwrap_or_else(|_| "6379".to_string()),
        )
        .with_param("database", database.to_string());

    if let Ok(username) = std::env::var("ZQLZ_REDIS_USERNAME")
        && !username.is_empty()
    {
        config.username = Some(username);
    }
    if let Ok(password) = std::env::var("ZQLZ_REDIS_PASSWORD")
        && !password.is_empty()
    {
        config.password = Some(password);
    }

    config
}

fn redis_quoted(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

async fn connect_redis(database: u16) -> Arc<dyn zqlz_core::Connection> {
    RedisDriver::new()
        .connect(&redis_config(database))
        .await
        .expect("connect to Redis")
}

#[tokio::test]
async fn live_redis_key_value_roundtrip() {
    if !integration_enabled() {
        eprintln!("skipping Redis integration test; set ZQLZ_REDIS_INTEGRATION=1");
        return;
    }

    let connection = connect_redis(3).await;
    let store = connection
        .as_key_value_store()
        .expect("Redis exposes key-value store");
    let prefix = format!("zqlz:test:{}:", Uuid::new_v4());
    let string_key = format!("{}string key", prefix);
    let list_key = format!("{}list", prefix);
    let hash_key = format!("{}hash", prefix);
    let set_key = format!("{}set", prefix);
    let zset_key = format!("{}zset", prefix);
    let stream_key = format!("{}stream", prefix);

    connection
        .execute(
            &format!("SET {} \"hello world\"", redis_quoted(&string_key)),
            &[],
        )
        .await
        .expect("set string with spaces");
    connection
        .execute(&format!("RPUSH {} one two", redis_quoted(&list_key)), &[])
        .await
        .expect("create list");
    connection
        .execute(
            &format!("HSET {} field value", redis_quoted(&hash_key)),
            &[],
        )
        .await
        .expect("create hash");
    connection
        .execute(&format!("SADD {} member", redis_quoted(&set_key)), &[])
        .await
        .expect("create set");
    connection
        .execute(&format!("ZADD {} 1 member", redis_quoted(&zset_key)), &[])
        .await
        .expect("create zset");
    connection
        .execute(
            &format!("XADD {} * message hello", redis_quoted(&stream_key)),
            &[],
        )
        .await
        .expect("create stream");
    connection
        .execute(&format!("EXPIRE {} 60", redis_quoted(&string_key)), &[])
        .await
        .expect("set ttl");

    let scan = store
        .scan_keys(KeyValueScanRequest {
            database_index: 3,
            limit: 1000,
            scan_batch_size: 100,
        })
        .await
        .expect("scan keys");
    assert!(scan.keys.iter().any(|key| key == &string_key));

    let entry = store.read_key(&string_key, 10).await.expect("read string");
    assert_eq!(entry.kind, KeyValueKind::String);
    assert_eq!(
        entry.data.rows[0].get_by_name("value"),
        Some(&Value::String("hello world".to_string()))
    );
    assert!(entry.ttl_seconds.unwrap_or_default() > 0);

    let summaries = store.load_key_summaries(3).await.expect("summaries");
    assert!(
        summaries
            .iter()
            .any(|summary| summary.key == hash_key && summary.preview.is_some())
    );

    let cleanup_keys = [
        string_key.as_str(),
        list_key.as_str(),
        hash_key.as_str(),
        set_key.as_str(),
        zset_key.as_str(),
        stream_key.as_str(),
    ];
    connection
        .execute(
            &format!(
                "DEL {}",
                cleanup_keys
                    .iter()
                    .map(|key| redis_quoted(key))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            &[],
        )
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn live_redis_database_scope_is_enforced() {
    if !integration_enabled() {
        eprintln!("skipping Redis integration test; set ZQLZ_REDIS_INTEGRATION=1");
        return;
    }

    let db0_connection = connect_redis(0).await;
    let db3_connection = connect_redis(3).await;
    let db3_store = db3_connection
        .as_key_value_store()
        .expect("Redis exposes key-value store");
    let prefix = format!("zqlz:test:{}:", Uuid::new_v4());
    let db0_key = format!("{}db0", prefix);
    let db3_key = format!("{}db3", prefix);

    db0_connection
        .execute(&format!("SET {} db0", redis_quoted(&db0_key)), &[])
        .await
        .expect("set db0 key");
    db3_connection
        .execute(&format!("SET {} db3", redis_quoted(&db3_key)), &[])
        .await
        .expect("set db3 key");

    let wrong_scope = db3_store
        .scan_keys(KeyValueScanRequest {
            database_index: 0,
            limit: 100,
            scan_batch_size: 100,
        })
        .await
        .expect_err("scoped db3 connection must reject db0 scan");
    assert!(wrong_scope.to_string().contains("database 3"));

    let db3_scan = db3_store
        .scan_keys(KeyValueScanRequest {
            database_index: 3,
            limit: 100,
            scan_batch_size: 100,
        })
        .await
        .expect("scan db3");
    assert!(db3_scan.keys.iter().any(|key| key == &db3_key));
    assert!(!db3_scan.keys.iter().any(|key| key == &db0_key));

    db0_connection
        .execute(&format!("DEL {}", redis_quoted(&db0_key)), &[])
        .await
        .expect("cleanup db0");
    db3_connection
        .execute(&format!("DEL {}", redis_quoted(&db3_key)), &[])
        .await
        .expect("cleanup db3");
}

#[tokio::test]
async fn live_redis_save_key_refuses_target_collision() {
    if !integration_enabled() {
        eprintln!("skipping Redis integration test; set ZQLZ_REDIS_INTEGRATION=1");
        return;
    }

    let connection = connect_redis(3).await;
    let store = connection
        .as_key_value_store()
        .expect("Redis exposes key-value store");
    let prefix = format!("zqlz:test:{}:", Uuid::new_v4());
    let original_key = format!("{}original", prefix);
    let target_key = format!("{}target", prefix);
    let temporary_key = format!("{}temporary", prefix);

    connection
        .execute(
            &format!("SET {} original", redis_quoted(&original_key)),
            &[],
        )
        .await
        .expect("set original key");
    connection
        .execute(&format!("SET {} target", redis_quoted(&target_key)), &[])
        .await
        .expect("set target key");

    let error = store
        .save_key(KeyValueSaveRequest {
            original_key: original_key.clone(),
            new_key: target_key.clone(),
            kind: KeyValueKind::String,
            serialized_value: "new value".to_string(),
            ttl_seconds: None,
            temporary_key: temporary_key.clone(),
        })
        .await
        .expect_err("rename must not overwrite target key");
    assert!(error.to_string().contains("already exists"));

    let original = store
        .read_key(&original_key, 10)
        .await
        .expect("read original");
    assert_eq!(
        original.data.rows[0].get_by_name("value"),
        Some(&Value::String("original".to_string()))
    );
    let target = store.read_key(&target_key, 10).await.expect("read target");
    assert_eq!(
        target.data.rows[0].get_by_name("value"),
        Some(&Value::String("target".to_string()))
    );
    let temporary = store
        .read_key(&temporary_key, 10)
        .await
        .expect("read temporary key");
    assert_eq!(temporary.kind, KeyValueKind::None);

    connection
        .execute(
            &format!(
                "DEL {} {} {}",
                redis_quoted(&original_key),
                redis_quoted(&target_key),
                redis_quoted(&temporary_key)
            ),
            &[],
        )
        .await
        .expect("cleanup");
}
