//! Redis-specific utilities for table handlers.
//!
//! This module provides functions for working with Redis data types,
//! parsing TTL values, and fetching Redis key values.

use std::sync::Arc;
use crate::components::RedisValueType;

/// Parse human-readable TTL string (e.g., "30s", "5m 30s", "1h 15m") into seconds
pub(in crate::main_view) fn parse_human_readable_ttl(ttl_str: &str) -> Option<i64> {
    let ttl_str = ttl_str.trim();
    if ttl_str.is_empty() || ttl_str == "No TTL" {
        return None;
    }

    let mut total_seconds: i64 = 0;
    let mut current_num = String::new();

    for c in ttl_str.chars() {
        if c.is_ascii_digit() {
            current_num.push(c);
        } else if !current_num.is_empty() {
            let num: i64 = current_num.parse().ok()?;
            current_num.clear();

            match c {
                's' => total_seconds += num,
                'm' => total_seconds += num * 60,
                'h' => total_seconds += num * 3600,
                'd' => total_seconds += num * 86400,
                _ => {}
            }
        }
    }

    // Handle bare numbers (assume seconds)
    if !current_num.is_empty() {
        total_seconds += current_num.parse::<i64>().ok()?;
    }

    if total_seconds > 0 {
        Some(total_seconds)
    } else {
        None
    }
}

/// Fetch the full value of a Redis key based on its type
pub(in crate::main_view) async fn fetch_redis_key_value(
    connection: &Arc<dyn zqlz_core::Connection>,
    key: &str,
    value_type: RedisValueType,
) -> Option<String> {
    let query_result = match value_type {
        RedisValueType::String => {
            // GET key
            connection.query(&format!("GET {}", key), &[]).await.ok()
        }
        RedisValueType::List => {
            // LRANGE key 0 -1 (get all elements)
            connection
                .query(&format!("LRANGE {} 0 -1", key), &[])
                .await
                .ok()
        }
        RedisValueType::Set => {
            // SMEMBERS key (get all members)
            connection
                .query(&format!("SMEMBERS {}", key), &[])
                .await
                .ok()
        }
        RedisValueType::ZSet => {
            // ZRANGE key 0 -1 WITHSCORES (get all members with scores)
            connection
                .query(&format!("ZRANGE {} 0 -1 WITHSCORES", key), &[])
                .await
                .ok()
        }
        RedisValueType::Hash => {
            // HGETALL key (get all fields and values)
            connection
                .query(&format!("HGETALL {}", key), &[])
                .await
                .ok()
        }
        RedisValueType::Json => {
            // JSON.GET key (if using RedisJSON module)
            // Try JSON.GET first, fallback to GET
            match connection.query(&format!("JSON.GET {}", key), &[]).await {
                Ok(result) => Some(result),
                Err(_) => connection.query(&format!("GET {}", key), &[]).await.ok(),
            }
        }
        RedisValueType::Stream => {
            // Streams are complex, just return None for now
            None
        }
    };

    // Convert the result to a JSON string format that our parser expects
    query_result.and_then(|result| {
        match value_type {
            RedisValueType::String | RedisValueType::Json => {
                // Single value - just return it
                result
                    .rows
                    .first()
                    .and_then(|r| r.get_by_name("value").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
            }
            RedisValueType::List | RedisValueType::Set => {
                // Array of values - serialize to JSON array
                let items: Vec<String> = result
                    .rows
                    .iter()
                    .filter_map(|r| r.get_by_name("value").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .collect();
                Some(serde_json::to_string(&items).unwrap_or_default())
            }
            RedisValueType::ZSet => {
                // Alternating member, score pairs - serialize to JSON object
                let mut map = serde_json::Map::new();
                let items: Vec<String> = result
                    .rows
                    .iter()
                    .filter_map(|r| r.get_by_name("value").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .collect();
                for chunk in items.chunks(2) {
                    if chunk.len() == 2 {
                        let member = &chunk[0];
                        let score: f64 = chunk[1].parse().unwrap_or(0.0);
                        map.insert(
                            member.clone(),
                            serde_json::Value::Number(
                                serde_json::Number::from_f64(score)
                                    .unwrap_or(serde_json::Number::from(0)),
                            ),
                        );
                    }
                }
                Some(serde_json::to_string(&map).unwrap_or_default())
            }
            RedisValueType::Hash => {
                // Alternating field, value pairs - serialize to JSON object
                let mut map = serde_json::Map::new();
                let items: Vec<String> = result
                    .rows
                    .iter()
                    .filter_map(|r| r.get_by_name("value").and_then(|v| v.as_str()))
                    .map(|s| s.to_string())
                    .collect();
                for chunk in items.chunks(2) {
                    if chunk.len() == 2 {
                        let field = &chunk[0];
                        let value = &chunk[1];
                        map.insert(field.clone(), serde_json::Value::String(value.clone()));
                    }
                }
                Some(serde_json::to_string(&map).unwrap_or_default())
            }
            RedisValueType::Stream => None,
        }
    })
}
