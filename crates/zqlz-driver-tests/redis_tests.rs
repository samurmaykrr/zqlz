//! Redis key-value operation tests and data structure tests
//!
//! Tests Redis-specific operations including:
//! - SET/GET operations
//! - Expiry and TTL handling
//! - Binary data storage
//! - Key deletion and existence checks
//! - Increment/decrement operations
//! - Multi-key operations (MGET/MSET)
//! - Key pattern matching and scanning
//! - Key renaming
//! - Advanced operations (GETSET, SETNX, APPEND, etc.)
//! - List operations (LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc.)
//! - Set operations (SADD, SREM, SMEMBERS, SUNION, SINTER, SDIFF, etc.)
//! - Hash operations (HSET, HGET, HGETALL, HINCRBY, etc.)
//! - Sorted set operations (ZADD, ZRANGE, ZRANK, ZSCORE, etc.)

#[cfg(test)]
mod tests {
    use crate::fixtures::{test_connection, TestDriver};
    use anyhow::{Context, Result};
    use zqlz_core::{Connection, Value};

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_set_get(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:set_get:key1 value1", &[])
            .await
            .context("SET operation failed")?;

        let result = conn
            .query("GET test:set_get:key1", &[])
            .await
            .context("GET operation failed")?;

        let value = result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .context("No value returned")?;

        assert_eq!(value.as_str(), Some("value1"), "GET returned wrong value");

        conn.execute("DEL test:set_get:key1", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_set_with_expiry(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:expiry:key1 value1 EX 10", &[])
            .await
            .context("SET with EX failed")?;

        let ttl_result = conn
            .query("TTL test:expiry:key1", &[])
            .await
            .context("TTL query failed")?;

        let ttl = ttl_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("TTL not returned as int")?;

        assert!(ttl > 0 && ttl <= 10, "TTL should be between 1 and 10");

        conn.execute("DEL test:expiry:key1", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_set_get_binary(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let binary_data = "binary\0data\x01\x02\x03";

        conn.execute(&format!("SET test:binary:key1 {}", binary_data), &[])
            .await
            .context("SET binary failed")?;

        let result = conn
            .query("GET test:binary:key1", &[])
            .await
            .context("GET binary failed")?;

        let value = result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_str())
            .context("Binary value not returned as string")?;

        assert_eq!(value, binary_data, "Binary data roundtrip failed");

        conn.execute("DEL test:binary:key1", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_delete_key(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:del:key1 value1", &[])
            .await
            .context("SET failed")?;

        let del_result = conn
            .execute("DEL test:del:key1", &[])
            .await
            .context("DEL failed")?;

        assert_eq!(
            del_result.affected_rows, 1,
            "DEL should return 1 for deleted key"
        );

        let exists_result = conn
            .query("EXISTS test:del:key1", &[])
            .await
            .context("EXISTS query failed")?;

        let exists = exists_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("EXISTS not returned as int")?;

        assert_eq!(exists, 0, "Key should not exist after DEL");

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_delete_multiple_keys(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:del_multi:key1 value1", &[])
            .await
            .context("SET key1 failed")?;
        conn.execute("SET test:del_multi:key2 value2", &[])
            .await
            .context("SET key2 failed")?;
        conn.execute("SET test:del_multi:key3 value3", &[])
            .await
            .context("SET key3 failed")?;

        let del_result = conn
            .execute("DEL test:del_multi:key1 test:del_multi:key2 test:del_multi:key3", &[])
            .await
            .context("DEL multiple keys failed")?;

        assert_eq!(
            del_result.affected_rows, 3,
            "DEL should return 3 for three deleted keys"
        );

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_exists_key(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let exists_before = conn
            .query("EXISTS test:exists:key1", &[])
            .await
            .context("EXISTS query before SET failed")?;

        let exists_count = exists_before
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("EXISTS not returned as int")?;

        assert_eq!(exists_count, 0, "Key should not exist initially");

        conn.execute("SET test:exists:key1 value1", &[])
            .await
            .context("SET failed")?;

        let exists_after = conn
            .query("EXISTS test:exists:key1", &[])
            .await
            .context("EXISTS query after SET failed")?;

        let exists_count = exists_after
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("EXISTS not returned as int")?;

        assert_eq!(exists_count, 1, "Key should exist after SET");

        conn.execute("DEL test:exists:key1", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_get_nonexistent_key(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let result = conn
            .query("GET test:nonexistent:key_that_does_not_exist", &[])
            .await
            .context("GET nonexistent key failed")?;

        let value = result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"));

        assert_eq!(
            value,
            Some(&Value::Null),
            "GET nonexistent key should return NULL"
        );

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_increment_decrement(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:incr:counter 10", &[])
            .await
            .context("SET counter failed")?;

        let incr_result = conn
            .query("INCR test:incr:counter", &[])
            .await
            .context("INCR failed")?;

        let value = incr_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("INCR result not returned as int")?;

        assert_eq!(value, 11, "INCR should increment to 11");

        let decr_result = conn
            .query("DECR test:incr:counter", &[])
            .await
            .context("DECR failed")?;

        let value = decr_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("DECR result not returned as int")?;

        assert_eq!(value, 10, "DECR should decrement back to 10");

        conn.execute("DEL test:incr:counter", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_mget_mset(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("MSET test:mset:key1 value1 test:mset:key2 value2 test:mset:key3 value3", &[])
            .await
            .context("MSET failed")?;

        let mget_result = conn
            .query("MGET test:mset:key1 test:mset:key2 test:mset:key3", &[])
            .await
            .context("MGET failed")?;

        assert_eq!(mget_result.rows.len(), 3, "MGET should return 3 values");

        let values: Vec<String> = mget_result
            .rows
            .iter()
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(values, vec!["value1", "value2", "value3"]);

        conn.execute("DEL test:mset:key1 test:mset:key2 test:mset:key3", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_rename_key(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:rename:old_name value1", &[])
            .await
            .context("SET failed")?;

        conn.execute("RENAME test:rename:old_name test:rename:new_name", &[])
            .await
            .context("RENAME failed")?;

        let old_exists = conn
            .query("EXISTS test:rename:old_name", &[])
            .await
            .context("EXISTS old_name query failed")?;

        let old_count = old_exists
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("EXISTS not returned as int")?;

        assert_eq!(old_count, 0, "Old key should not exist after RENAME");

        let new_value = conn
            .query("GET test:rename:new_name", &[])
            .await
            .context("GET new_name failed")?;

        let value = new_value
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_str())
            .context("New key value not found")?;

        assert_eq!(value, "value1", "New key should have old value");

        conn.execute("DEL test:rename:new_name", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_key_pattern_matching(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:pattern:user:1 value1", &[])
            .await
            .context("SET user:1 failed")?;
        conn.execute("SET test:pattern:user:2 value2", &[])
            .await
            .context("SET user:2 failed")?;
        conn.execute("SET test:pattern:admin:1 value3", &[])
            .await
            .context("SET admin:1 failed")?;

        let keys_result = conn
            .query("KEYS test:pattern:user:*", &[])
            .await
            .context("KEYS query failed")?;

        let keys: Vec<String> = keys_result
            .rows
            .iter()
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(keys.len(), 2, "Should find 2 keys matching user:* pattern");
        assert!(keys.contains(&"test:pattern:user:1".to_string()));
        assert!(keys.contains(&"test:pattern:user:2".to_string()));

        conn.execute("DEL test:pattern:user:1 test:pattern:user:2 test:pattern:admin:1", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_ttl_operations(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:ttl:key1 value1", &[])
            .await
            .context("SET failed")?;

        let ttl_before = conn
            .query("TTL test:ttl:key1", &[])
            .await
            .context("TTL before EXPIRE failed")?;

        let ttl = ttl_before
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("TTL not returned as int")?;

        assert_eq!(ttl, -1, "TTL should be -1 for key without expiry");

        conn.execute("EXPIRE test:ttl:key1 100", &[])
            .await
            .context("EXPIRE failed")?;

        let ttl_after = conn
            .query("TTL test:ttl:key1", &[])
            .await
            .context("TTL after EXPIRE failed")?;

        let ttl = ttl_after
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("TTL not returned as int")?;

        assert!(
            ttl > 0 && ttl <= 100,
            "TTL should be between 1 and 100 after EXPIRE"
        );

        conn.execute("DEL test:ttl:key1", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_scan_cursor_iteration(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        for i in 1..=5 {
            conn.execute(&format!("SET test:scan:key{} value{}", i, i), &[])
                .await
                .context(format!("SET key{} failed", i))?;
        }

        let scan_result = conn
            .query("SCAN 0 MATCH test:scan:* COUNT 10", &[])
            .await
            .context("SCAN query failed")?;

        assert!(
            scan_result.rows.len() >= 2,
            "SCAN should return cursor and at least one key"
        );

        let keys: Vec<String> = scan_result
            .rows
            .iter()
            .skip(1)
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert!(!keys.is_empty(), "SCAN should return at least one key");

        conn.execute("DEL test:scan:key1 test:scan:key2 test:scan:key3 test:scan:key4 test:scan:key5", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_expire_persist_behavior(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:persist:key1 value1 EX 100", &[])
            .await
            .context("SET with EX failed")?;

        let ttl_before = conn
            .query("TTL test:persist:key1", &[])
            .await
            .context("TTL before PERSIST failed")?;

        let ttl = ttl_before
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("TTL not returned as int")?;

        assert!(ttl > 0, "TTL should be positive before PERSIST");

        conn.execute("PERSIST test:persist:key1", &[])
            .await
            .context("PERSIST failed")?;

        let ttl_after = conn
            .query("TTL test:persist:key1", &[])
            .await
            .context("TTL after PERSIST failed")?;

        let ttl = ttl_after
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("TTL not returned as int")?;

        assert_eq!(ttl, -1, "TTL should be -1 after PERSIST");

        conn.execute("DEL test:persist:key1", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_getset(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:getset:key1 oldvalue", &[])
            .await
            .context("SET failed")?;

        let getset_result = conn
            .query("GETSET test:getset:key1 newvalue", &[])
            .await
            .context("GETSET failed")?;

        let old_value = getset_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_str())
            .context("GETSET did not return old value")?;

        assert_eq!(old_value, "oldvalue", "GETSET should return old value");

        let get_result = conn
            .query("GET test:getset:key1", &[])
            .await
            .context("GET after GETSET failed")?;

        let new_value = get_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_str())
            .context("GET did not return new value")?;

        assert_eq!(new_value, "newvalue", "Key should have new value");

        conn.execute("DEL test:getset:key1", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_setnx(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        let setnx_result1 = conn
            .query("SETNX test:setnx:key1 value1", &[])
            .await
            .context("SETNX first call failed")?;

        let success1 = setnx_result1
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("SETNX result not returned as int")?;

        assert_eq!(success1, 1, "SETNX should succeed on nonexistent key");

        let setnx_result2 = conn
            .query("SETNX test:setnx:key1 value2", &[])
            .await
            .context("SETNX second call failed")?;

        let success2 = setnx_result2
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("SETNX result not returned as int")?;

        assert_eq!(success2, 0, "SETNX should fail on existing key");

        conn.execute("DEL test:setnx:key1", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_append(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:append:key1 Hello", &[])
            .await
            .context("SET failed")?;

        let append_result = conn
            .query("APPEND test:append:key1 World", &[])
            .await
            .context("APPEND failed")?;

        let length = append_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("APPEND result not returned as int")?;

        assert_eq!(length, 10, "APPEND should return new length (10)");

        let get_result = conn
            .query("GET test:append:key1", &[])
            .await
            .context("GET after APPEND failed")?;

        let value = get_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_str())
            .context("GET did not return value")?;

        assert_eq!(value, "HelloWorld", "APPEND should concatenate strings");

        conn.execute("DEL test:append:key1", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_strlen(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:strlen:key1 HelloWorld", &[])
            .await
            .context("SET failed")?;

        let strlen_result = conn
            .query("STRLEN test:strlen:key1", &[])
            .await
            .context("STRLEN failed")?;

        let length = strlen_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("STRLEN result not returned as int")?;

        assert_eq!(length, 10, "STRLEN should return 10");

        conn.execute("DEL test:strlen:key1", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn integration_test_redis_basic_ops(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SET test:integration:key value", &[])
            .await
            .context("SET failed")?;

        let result = conn
            .query("GET test:integration:key", &[])
            .await
            .context("GET failed")?;

        assert_eq!(result.rows.len(), 1, "GET should return one row");

        conn.execute("DEL test:integration:key", &[])
            .await
            .context("DEL failed")?;

        Ok(())
    }

    // ========== LIST OPERATIONS ==========

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_list_operations(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("RPUSH test:list:mylist value1 value2 value3", &[])
            .await
            .context("RPUSH failed")?;

        let llen_result = conn
            .query("LLEN test:list:mylist", &[])
            .await
            .context("LLEN failed")?;

        let length = llen_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("LLEN not returned as int")?;

        assert_eq!(length, 3, "List should have 3 elements");

        conn.execute("LPUSH test:list:mylist value0", &[])
            .await
            .context("LPUSH failed")?;

        let lrange_result = conn
            .query("LRANGE test:list:mylist 0 -1", &[])
            .await
            .context("LRANGE failed")?;

        assert_eq!(
            lrange_result.rows.len(),
            4,
            "LRANGE should return 4 elements"
        );

        let values: Vec<String> = lrange_result
            .rows
            .iter()
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(
            values,
            vec!["value0", "value1", "value2", "value3"],
            "LPUSH should prepend to list"
        );

        let lpop_result = conn
            .query("LPOP test:list:mylist", &[])
            .await
            .context("LPOP failed")?;

        let popped = lpop_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_str())
            .context("LPOP did not return value")?;

        assert_eq!(popped, "value0", "LPOP should return first element");

        let rpop_result = conn
            .query("RPOP test:list:mylist", &[])
            .await
            .context("RPOP failed")?;

        let popped = rpop_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_str())
            .context("RPOP did not return value")?;

        assert_eq!(popped, "value3", "RPOP should return last element");

        conn.execute("DEL test:list:mylist", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_list_trim(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("RPUSH test:list:trim one two three four five", &[])
            .await
            .context("RPUSH failed")?;

        conn.execute("LTRIM test:list:trim 1 3", &[])
            .await
            .context("LTRIM failed")?;

        let lrange_result = conn
            .query("LRANGE test:list:trim 0 -1", &[])
            .await
            .context("LRANGE after LTRIM failed")?;

        let values: Vec<String> = lrange_result
            .rows
            .iter()
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(
            values,
            vec!["two", "three", "four"],
            "LTRIM should keep only elements in range"
        );

        conn.execute("DEL test:list:trim", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_list_range(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("RPUSH test:list:range one two three four five", &[])
            .await
            .context("RPUSH failed")?;

        let lrange_result = conn
            .query("LRANGE test:list:range 1 3", &[])
            .await
            .context("LRANGE failed")?;

        let values: Vec<String> = lrange_result
            .rows
            .iter()
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(
            values,
            vec!["two", "three", "four"],
            "LRANGE should return elements in specified range"
        );

        conn.execute("DEL test:list:range", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_list_index(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("RPUSH test:list:index one two three", &[])
            .await
            .context("RPUSH failed")?;

        let lindex_result = conn
            .query("LINDEX test:list:index 1", &[])
            .await
            .context("LINDEX failed")?;

        let value = lindex_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_str())
            .context("LINDEX did not return value")?;

        assert_eq!(value, "two", "LINDEX should return element at index 1");

        conn.execute("DEL test:list:index", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    // ========== SET OPERATIONS ==========

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_set_operations(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SADD test:set:myset member1 member2 member3", &[])
            .await
            .context("SADD failed")?;

        let scard_result = conn
            .query("SCARD test:set:myset", &[])
            .await
            .context("SCARD failed")?;

        let cardinality = scard_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("SCARD not returned as int")?;

        assert_eq!(cardinality, 3, "Set should have 3 members");

        let smembers_result = conn
            .query("SMEMBERS test:set:myset", &[])
            .await
            .context("SMEMBERS failed")?;

        let members: Vec<String> = smembers_result
            .rows
            .iter()
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(members.len(), 3, "SMEMBERS should return 3 members");
        assert!(members.contains(&"member1".to_string()));
        assert!(members.contains(&"member2".to_string()));
        assert!(members.contains(&"member3".to_string()));

        conn.execute("SREM test:set:myset member2", &[])
            .await
            .context("SREM failed")?;

        let scard_after = conn
            .query("SCARD test:set:myset", &[])
            .await
            .context("SCARD after SREM failed")?;

        let cardinality = scard_after
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("SCARD not returned as int")?;

        assert_eq!(cardinality, 2, "Set should have 2 members after SREM");

        conn.execute("DEL test:set:myset", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_set_membership(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SADD test:set:member apple banana cherry", &[])
            .await
            .context("SADD failed")?;

        let sismember_result = conn
            .query("SISMEMBER test:set:member banana", &[])
            .await
            .context("SISMEMBER failed")?;

        let is_member = sismember_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("SISMEMBER not returned as int")?;

        assert_eq!(is_member, 1, "banana should be a member");

        let not_member_result = conn
            .query("SISMEMBER test:set:member grape", &[])
            .await
            .context("SISMEMBER for non-member failed")?;

        let is_member = not_member_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("SISMEMBER not returned as int")?;

        assert_eq!(is_member, 0, "grape should not be a member");

        conn.execute("DEL test:set:member", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_set_union_intersection(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SADD test:set:set1 a b c", &[])
            .await
            .context("SADD set1 failed")?;

        conn.execute("SADD test:set:set2 b c d", &[])
            .await
            .context("SADD set2 failed")?;

        let sunion_result = conn
            .query("SUNION test:set:set1 test:set:set2", &[])
            .await
            .context("SUNION failed")?;

        let union_members: Vec<String> = sunion_result
            .rows
            .iter()
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(union_members.len(), 4, "SUNION should return 4 members");
        assert!(union_members.contains(&"a".to_string()));
        assert!(union_members.contains(&"b".to_string()));
        assert!(union_members.contains(&"c".to_string()));
        assert!(union_members.contains(&"d".to_string()));

        let sinter_result = conn
            .query("SINTER test:set:set1 test:set:set2", &[])
            .await
            .context("SINTER failed")?;

        let inter_members: Vec<String> = sinter_result
            .rows
            .iter()
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(inter_members.len(), 2, "SINTER should return 2 members");
        assert!(inter_members.contains(&"b".to_string()));
        assert!(inter_members.contains(&"c".to_string()));

        conn.execute("DEL test:set:set1 test:set:set2", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_set_difference(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("SADD test:set:diff1 a b c d", &[])
            .await
            .context("SADD diff1 failed")?;

        conn.execute("SADD test:set:diff2 c d e f", &[])
            .await
            .context("SADD diff2 failed")?;

        let sdiff_result = conn
            .query("SDIFF test:set:diff1 test:set:diff2", &[])
            .await
            .context("SDIFF failed")?;

        let diff_members: Vec<String> = sdiff_result
            .rows
            .iter()
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(diff_members.len(), 2, "SDIFF should return 2 members");
        assert!(diff_members.contains(&"a".to_string()));
        assert!(diff_members.contains(&"b".to_string()));

        conn.execute("DEL test:set:diff1 test:set:diff2", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    // ========== HASH OPERATIONS ==========

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_hash_operations(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("HSET test:hash:user name John age 30 city NYC", &[])
            .await
            .context("HSET failed")?;

        let hget_result = conn
            .query("HGET test:hash:user name", &[])
            .await
            .context("HGET failed")?;

        let name = hget_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_str())
            .context("HGET did not return value")?;

        assert_eq!(name, "John", "HGET should return field value");

        let hgetall_result = conn
            .query("HGETALL test:hash:user", &[])
            .await
            .context("HGETALL failed")?;

        assert_eq!(
            hgetall_result.rows.len(),
            6,
            "HGETALL should return 6 values (3 fields × 2 for key-value pairs)"
        );

        let hlen_result = conn
            .query("HLEN test:hash:user", &[])
            .await
            .context("HLEN failed")?;

        let field_count = hlen_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("HLEN not returned as int")?;

        assert_eq!(field_count, 3, "Hash should have 3 fields");

        conn.execute("HDEL test:hash:user city", &[])
            .await
            .context("HDEL failed")?;

        let hlen_after = conn
            .query("HLEN test:hash:user", &[])
            .await
            .context("HLEN after HDEL failed")?;

        let field_count = hlen_after
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("HLEN not returned as int")?;

        assert_eq!(field_count, 2, "Hash should have 2 fields after HDEL");

        conn.execute("DEL test:hash:user", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_hash_increment(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("HSET test:hash:counter count 10", &[])
            .await
            .context("HSET failed")?;

        let hincrby_result = conn
            .query("HINCRBY test:hash:counter count 5", &[])
            .await
            .context("HINCRBY failed")?;

        let new_value = hincrby_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("HINCRBY not returned as int")?;

        assert_eq!(new_value, 15, "HINCRBY should increment field value");

        conn.execute("DEL test:hash:counter", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_hash_exists(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("HSET test:hash:exists field1 value1", &[])
            .await
            .context("HSET failed")?;

        let hexists_result = conn
            .query("HEXISTS test:hash:exists field1", &[])
            .await
            .context("HEXISTS failed")?;

        let exists = hexists_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("HEXISTS not returned as int")?;

        assert_eq!(exists, 1, "field1 should exist");

        let not_exists_result = conn
            .query("HEXISTS test:hash:exists field2", &[])
            .await
            .context("HEXISTS for non-existent field failed")?;

        let exists = not_exists_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("HEXISTS not returned as int")?;

        assert_eq!(exists, 0, "field2 should not exist");

        conn.execute("DEL test:hash:exists", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_hash_length(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("HSET test:hash:len f1 v1 f2 v2 f3 v3", &[])
            .await
            .context("HSET failed")?;

        let hlen_result = conn
            .query("HLEN test:hash:len", &[])
            .await
            .context("HLEN failed")?;

        let length = hlen_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("HLEN not returned as int")?;

        assert_eq!(length, 3, "Hash should have 3 fields");

        conn.execute("DEL test:hash:len", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    // ========== SORTED SET OPERATIONS ==========

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_sorted_set_operations(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("ZADD test:zset:scores 100 player1 85 player2 95 player3", &[])
            .await
            .context("ZADD failed")?;

        let zcard_result = conn
            .query("ZCARD test:zset:scores", &[])
            .await
            .context("ZCARD failed")?;

        let cardinality = zcard_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("ZCARD not returned as int")?;

        assert_eq!(cardinality, 3, "Sorted set should have 3 members");

        let zrange_result = conn
            .query("ZRANGE test:zset:scores 0 -1", &[])
            .await
            .context("ZRANGE failed")?;

        let members: Vec<String> = zrange_result
            .rows
            .iter()
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(
            members,
            vec!["player2", "player3", "player1"],
            "ZRANGE should return members in ascending score order"
        );

        conn.execute("DEL test:zset:scores", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_sorted_set_range_by_score(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("ZADD test:zset:range 10 a 20 b 30 c 40 d 50 e", &[])
            .await
            .context("ZADD failed")?;

        let zrangebyscore_result = conn
            .query("ZRANGEBYSCORE test:zset:range 20 40", &[])
            .await
            .context("ZRANGEBYSCORE failed")?;

        let members: Vec<String> = zrangebyscore_result
            .rows
            .iter()
            .filter_map(|row| {
                row.get_by_name("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        assert_eq!(
            members,
            vec!["b", "c", "d"],
            "ZRANGEBYSCORE should return members in score range"
        );

        conn.execute("DEL test:zset:range", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_sorted_set_rank(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("ZADD test:zset:rank 100 alice 85 bob 95 charlie", &[])
            .await
            .context("ZADD failed")?;

        let zrank_result = conn
            .query("ZRANK test:zset:rank alice", &[])
            .await
            .context("ZRANK failed")?;

        let rank = zrank_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("ZRANK not returned as int")?;

        assert_eq!(rank, 2, "alice should have rank 2 (0-indexed)");

        let zrevrank_result = conn
            .query("ZREVRANK test:zset:rank alice", &[])
            .await
            .context("ZREVRANK failed")?;

        let rev_rank = zrevrank_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("ZREVRANK not returned as int")?;

        assert_eq!(rev_rank, 0, "alice should have reverse rank 0");

        conn.execute("DEL test:zset:rank", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_sorted_set_score(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("ZADD test:zset:score 42.5 member1", &[])
            .await
            .context("ZADD failed")?;

        let zscore_result = conn
            .query("ZSCORE test:zset:score member1", &[])
            .await
            .context("ZSCORE failed")?;

        let score = zscore_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_str())
            .context("ZSCORE not returned as string")?;

        assert_eq!(score, "42.5", "ZSCORE should return member score");

        conn.execute("DEL test:zset:score", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_sorted_set_increment(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("ZADD test:zset:incr 10 member1", &[])
            .await
            .context("ZADD failed")?;

        let zincrby_result = conn
            .query("ZINCRBY test:zset:incr 5 member1", &[])
            .await
            .context("ZINCRBY failed")?;

        let new_score = zincrby_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_str())
            .context("ZINCRBY not returned as string")?;

        assert_eq!(new_score, "15", "ZINCRBY should increment member score");

        conn.execute("DEL test:zset:incr", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn test_sorted_set_remove(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("ZADD test:zset:rem 1 a 2 b 3 c", &[])
            .await
            .context("ZADD failed")?;

        conn.execute("ZREM test:zset:rem b", &[])
            .await
            .context("ZREM failed")?;

        let zcard_result = conn
            .query("ZCARD test:zset:rem", &[])
            .await
            .context("ZCARD after ZREM failed")?;

        let cardinality = zcard_result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| v.as_i64())
            .context("ZCARD not returned as int")?;

        assert_eq!(cardinality, 2, "Sorted set should have 2 members after ZREM");

        conn.execute("DEL test:zset:rem", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }

    #[rstest::rstest]
    #[case::redis(TestDriver::Redis)]
    #[tokio::test]
    async fn integration_test_redis_data_structures(#[case] driver: TestDriver) -> Result<()> {
        let conn = test_connection(driver).await?;

        conn.execute("LPUSH test:integration:list item1", &[])
            .await
            .context("LPUSH failed")?;

        conn.execute("SADD test:integration:set member1", &[])
            .await
            .context("SADD failed")?;

        conn.execute("HSET test:integration:hash field1 value1", &[])
            .await
            .context("HSET failed")?;

        conn.execute("ZADD test:integration:zset 1 item1", &[])
            .await
            .context("ZADD failed")?;

        let list_result = conn
            .query("LRANGE test:integration:list 0 -1", &[])
            .await
            .context("LRANGE failed")?;
        assert_eq!(list_result.rows.len(), 1, "List should have 1 item");

        let set_result = conn
            .query("SMEMBERS test:integration:set", &[])
            .await
            .context("SMEMBERS failed")?;
        assert_eq!(set_result.rows.len(), 1, "Set should have 1 member");

        let hash_result = conn
            .query("HGET test:integration:hash field1", &[])
            .await
            .context("HGET failed")?;
        assert_eq!(hash_result.rows.len(), 1, "Hash should return 1 field");

        let zset_result = conn
            .query("ZRANGE test:integration:zset 0 -1", &[])
            .await
            .context("ZRANGE failed")?;
        assert_eq!(zset_result.rows.len(), 1, "Sorted set should have 1 member");

        conn.execute("DEL test:integration:list test:integration:set test:integration:hash test:integration:zset", &[])
            .await
            .context("DEL cleanup failed")?;

        Ok(())
    }
}
