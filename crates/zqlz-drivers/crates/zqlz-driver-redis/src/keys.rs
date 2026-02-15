//! Redis key introspection module
//!
//! This module provides functionality for browsing and inspecting Redis keys:
//! - List keys using SCAN (cursor-based iteration)
//! - Get key types
//! - Get key TTL (time-to-live)
//! - Get key metadata

use crate::RedisConnection;
use serde::{Deserialize, Serialize};
use zqlz_core::{Result, ZqlzError};

/// Information about a Redis key
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KeyInfo {
    /// The key name
    pub name: String,
    /// The Redis data type (string, list, set, zset, hash, stream, none)
    pub key_type: KeyType,
    /// Time-to-live in seconds (-1 = no expiry, -2 = key doesn't exist)
    pub ttl: i64,
    /// Memory usage in bytes (if available, requires MEMORY USAGE command)
    pub memory_bytes: Option<i64>,
}

impl KeyInfo {
    /// Create a new KeyInfo
    pub fn new(name: impl Into<String>, key_type: KeyType, ttl: i64) -> Self {
        Self {
            name: name.into(),
            key_type,
            ttl,
            memory_bytes: None,
        }
    }

    /// Set memory usage
    pub fn with_memory(mut self, bytes: i64) -> Self {
        self.memory_bytes = Some(bytes);
        self
    }

    /// Check if key has an expiry set
    pub fn has_expiry(&self) -> bool {
        self.ttl >= 0
    }

    /// Check if key exists
    pub fn exists(&self) -> bool {
        self.ttl != -2 && self.key_type != KeyType::None
    }
}

/// Redis key types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyType {
    /// String value
    String,
    /// List (linked list)
    List,
    /// Set (unordered unique strings)
    Set,
    /// Sorted set (ordered by score)
    Zset,
    /// Hash (field-value pairs)
    Hash,
    /// Stream (append-only log)
    Stream,
    /// Key doesn't exist
    None,
}

impl KeyType {
    /// Parse from Redis TYPE command response
    pub fn from_redis_type(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "string" => KeyType::String,
            "list" => KeyType::List,
            "set" => KeyType::Set,
            "zset" => KeyType::Zset,
            "hash" => KeyType::Hash,
            "stream" => KeyType::Stream,
            "none" => KeyType::None,
            _ => KeyType::None,
        }
    }

    /// Get the Redis type string
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyType::String => "string",
            KeyType::List => "list",
            KeyType::Set => "set",
            KeyType::Zset => "zset",
            KeyType::Hash => "hash",
            KeyType::Stream => "stream",
            KeyType::None => "none",
        }
    }
}

impl std::fmt::Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Options for listing keys
#[derive(Debug, Clone, Default)]
pub struct ListKeysOptions {
    /// Pattern to match (default: "*" for all keys)
    pub pattern: String,
    /// Maximum number of keys to return (0 = no limit)
    pub limit: usize,
    /// Count hint for SCAN command (keys per iteration)
    pub scan_count: usize,
    /// Include key types in results
    pub include_types: bool,
    /// Include TTL in results
    pub include_ttl: bool,
}

impl ListKeysOptions {
    /// Create options with default pattern "*"
    pub fn new() -> Self {
        Self {
            pattern: "*".to_string(),
            limit: 0,
            scan_count: 100,
            include_types: false,
            include_ttl: false,
        }
    }

    /// Set the pattern to match
    pub fn with_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = pattern.into();
        self
    }

    /// Set maximum number of keys to return
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Set SCAN count hint
    pub fn with_scan_count(mut self, count: usize) -> Self {
        self.scan_count = count;
        self
    }

    /// Include key types in results
    pub fn include_types(mut self) -> Self {
        self.include_types = true;
        self
    }

    /// Include TTL in results
    pub fn include_ttl(mut self) -> Self {
        self.include_ttl = true;
        self
    }
}

/// List keys matching a pattern using SCAN
///
/// Uses cursor-based iteration to avoid blocking Redis with KEYS command.
pub async fn list_keys(conn: &RedisConnection, pattern: &str) -> Result<Vec<String>> {
    list_keys_with_options(conn, &ListKeysOptions::new().with_pattern(pattern)).await
}

/// List keys with options
pub async fn list_keys_with_options(
    conn: &RedisConnection,
    options: &ListKeysOptions,
) -> Result<Vec<String>> {
    use zqlz_core::Connection;

    let mut keys = Vec::new();
    let mut cursor = 0u64;

    loop {
        // Build SCAN command: SCAN cursor MATCH pattern COUNT count
        let cmd = format!(
            "SCAN {} MATCH {} COUNT {}",
            cursor,
            options.pattern,
            options.scan_count.max(10)
        );

        let result = conn.query(&cmd, &[]).await?;

        if result.rows.len() < 2 {
            break;
        }

        // First row contains the new cursor
        let new_cursor = result.rows[0]
            .get_by_name("value")
            .and_then(|v| match v {
                zqlz_core::Value::String(s) => s.parse::<u64>().ok(),
                zqlz_core::Value::Int64(n) => Some(*n as u64),
                _ => None,
            })
            .unwrap_or(0);

        // Remaining rows are keys
        for row in result.rows.iter().skip(1) {
            if let Some(zqlz_core::Value::String(key)) = row.get_by_name("value") {
                keys.push(key.clone());

                // Check limit
                if options.limit > 0 && keys.len() >= options.limit {
                    return Ok(keys);
                }
            }
        }

        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }

    Ok(keys)
}

/// Get the type of a key
pub async fn get_key_type(conn: &RedisConnection, key: &str) -> Result<KeyType> {
    use zqlz_core::Connection;

    let cmd = format!("TYPE {}", key);
    let result = conn.query(&cmd, &[]).await?;

    let type_str = result
        .rows
        .first()
        .and_then(|row| row.get_by_name("value"))
        .and_then(|v| match v {
            zqlz_core::Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .unwrap_or("none");

    Ok(KeyType::from_redis_type(type_str))
}

/// Get the TTL of a key in seconds
///
/// Returns:
/// - Positive value: TTL in seconds
/// - -1: Key exists but has no expiry
/// - -2: Key does not exist
pub async fn get_key_ttl(conn: &RedisConnection, key: &str) -> Result<i64> {
    use zqlz_core::Connection;

    let cmd = format!("TTL {}", key);
    let result = conn.query(&cmd, &[]).await?;

    let ttl = result
        .rows
        .first()
        .and_then(|row| row.get_by_name("value"))
        .and_then(|v| match v {
            zqlz_core::Value::Int64(n) => Some(*n),
            zqlz_core::Value::String(s) => s.parse().ok(),
            _ => None,
        })
        .unwrap_or(-2);

    Ok(ttl)
}

/// Get the TTL of a key in milliseconds
///
/// Returns:
/// - Positive value: TTL in milliseconds
/// - -1: Key exists but has no expiry
/// - -2: Key does not exist
pub async fn get_key_pttl(conn: &RedisConnection, key: &str) -> Result<i64> {
    use zqlz_core::Connection;

    let cmd = format!("PTTL {}", key);
    let result = conn.query(&cmd, &[]).await?;

    let pttl = result
        .rows
        .first()
        .and_then(|row| row.get_by_name("value"))
        .and_then(|v| match v {
            zqlz_core::Value::Int64(n) => Some(*n),
            zqlz_core::Value::String(s) => s.parse().ok(),
            _ => None,
        })
        .unwrap_or(-2);

    Ok(pttl)
}

/// Check if a key exists
pub async fn key_exists(conn: &RedisConnection, key: &str) -> Result<bool> {
    use zqlz_core::Connection;

    let cmd = format!("EXISTS {}", key);
    let result = conn.query(&cmd, &[]).await?;

    let exists = result
        .rows
        .first()
        .and_then(|row| row.get_by_name("value"))
        .and_then(|v| match v {
            zqlz_core::Value::Int64(n) => Some(*n > 0),
            zqlz_core::Value::String(s) => s.parse::<i64>().ok().map(|n| n > 0),
            _ => None,
        })
        .unwrap_or(false);

    Ok(exists)
}

/// Get full key information including type, TTL, and optionally memory usage
pub async fn get_key_info(conn: &RedisConnection, key: &str) -> Result<KeyInfo> {
    let key_type = get_key_type(conn, key).await?;
    let ttl = get_key_ttl(conn, key).await?;

    Ok(KeyInfo::new(key, key_type, ttl))
}

/// Get key information with memory usage (requires MEMORY USAGE command)
pub async fn get_key_info_with_memory(conn: &RedisConnection, key: &str) -> Result<KeyInfo> {
    use zqlz_core::Connection;

    let key_type = get_key_type(conn, key).await?;
    let ttl = get_key_ttl(conn, key).await?;

    // Try to get memory usage (may not be available on all Redis versions)
    let memory = match conn.query(&format!("MEMORY USAGE {}", key), &[]).await {
        Ok(result) => result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| match v {
                zqlz_core::Value::Int64(n) => Some(*n),
                zqlz_core::Value::String(s) => s.parse().ok(),
                _ => None,
            }),
        Err(_) => None,
    };

    let mut info = KeyInfo::new(key, key_type, ttl);
    if let Some(bytes) = memory {
        info = info.with_memory(bytes);
    }

    Ok(info)
}

/// List keys with full information
pub async fn list_keys_with_info(
    conn: &RedisConnection,
    options: &ListKeysOptions,
) -> Result<Vec<KeyInfo>> {
    let keys = list_keys_with_options(conn, options).await?;

    let mut results = Vec::with_capacity(keys.len());
    for key in keys {
        let key_type = if options.include_types {
            get_key_type(conn, &key).await.unwrap_or(KeyType::None)
        } else {
            KeyType::None
        };

        let ttl = if options.include_ttl {
            get_key_ttl(conn, &key).await.unwrap_or(-2)
        } else {
            -1
        };

        results.push(KeyInfo::new(&key, key_type, ttl));
    }

    Ok(results)
}

/// Get the number of keys in the current database
pub async fn get_database_size(conn: &RedisConnection) -> Result<u64> {
    use zqlz_core::Connection;

    let result = conn.query("DBSIZE", &[]).await?;

    let count = result
        .rows
        .first()
        .and_then(|row| row.get_by_name("value"))
        .and_then(|v| match v {
            zqlz_core::Value::Int64(n) => Some(*n as u64),
            zqlz_core::Value::String(s) => s.parse().ok(),
            _ => None,
        })
        .unwrap_or(0);

    Ok(count)
}

/// Rename a key
pub async fn rename_key(conn: &RedisConnection, old_name: &str, new_name: &str) -> Result<()> {
    use zqlz_core::Connection;

    let cmd = format!("RENAME {} {}", old_name, new_name);
    let result = conn.execute(&cmd, &[]).await?;

    if result.affected_rows == 0 {
        return Err(ZqlzError::Driver(format!(
            "Failed to rename key: {}",
            old_name
        )));
    }

    Ok(())
}

/// Delete one or more keys
pub async fn delete_keys(conn: &RedisConnection, keys: &[&str]) -> Result<u64> {
    use zqlz_core::Connection;

    if keys.is_empty() {
        return Ok(0);
    }

    let cmd = format!("DEL {}", keys.join(" "));
    let result = conn.execute(&cmd, &[]).await?;

    Ok(result.affected_rows)
}

/// Set expiry on a key in seconds
pub async fn set_key_expiry(conn: &RedisConnection, key: &str, seconds: u64) -> Result<bool> {
    use zqlz_core::Connection;

    let cmd = format!("EXPIRE {} {}", key, seconds);
    let result = conn.query(&cmd, &[]).await?;

    let success = result
        .rows
        .first()
        .and_then(|row| row.get_by_name("value"))
        .and_then(|v| match v {
            zqlz_core::Value::Int64(n) => Some(*n == 1),
            zqlz_core::Value::String(s) => s.parse::<i64>().ok().map(|n| n == 1),
            _ => None,
        })
        .unwrap_or(false);

    Ok(success)
}

/// Remove expiry from a key (make it persistent)
pub async fn persist_key(conn: &RedisConnection, key: &str) -> Result<bool> {
    use zqlz_core::Connection;

    let cmd = format!("PERSIST {}", key);
    let result = conn.query(&cmd, &[]).await?;

    let success = result
        .rows
        .first()
        .and_then(|row| row.get_by_name("value"))
        .and_then(|v| match v {
            zqlz_core::Value::Int64(n) => Some(*n == 1),
            zqlz_core::Value::String(s) => s.parse::<i64>().ok().map(|n| n == 1),
            _ => None,
        })
        .unwrap_or(false);

    Ok(success)
}

/// Get a preview of a key's value (truncated to max_len characters)
/// Works for all key types, returning an appropriate preview
pub async fn get_key_value_preview(
    conn: &RedisConnection,
    key: &str,
    key_type: KeyType,
    max_len: usize,
) -> Result<String> {
    use zqlz_core::Connection;

    let preview = match key_type {
        KeyType::String => {
            // Use GETRANGE for efficient partial fetch
            let cmd = format!("GETRANGE {} 0 {}", key, max_len.saturating_sub(1));
            let result = conn.query(&cmd, &[]).await?;
            result
                .rows
                .first()
                .and_then(|row| row.get_by_name("value"))
                .and_then(|v| v.as_str())
                .map(|s| {
                    if s.len() >= max_len {
                        format!("{}...", s)
                    } else {
                        s.to_string()
                    }
                })
                .unwrap_or_default()
        }
        KeyType::Hash => {
            // Get first few fields with HSCAN
            let cmd = format!("HSCAN {} 0 COUNT 3", key);
            let result = conn.query(&cmd, &[]).await?;
            let fields: Vec<String> = result
                .rows
                .iter()
                .skip(1) // Skip cursor
                .take(6) // 3 field-value pairs
                .filter_map(|row| row.get_by_name("value").and_then(|v| v.as_str()))
                .map(|s| s.to_string())
                .collect();
            // Format as "field1: val1, field2: val2"
            let mut pairs = Vec::new();
            for chunk in fields.chunks(2) {
                if chunk.len() == 2 {
                    let field = truncate_str(&chunk[0], 15);
                    let value = truncate_str(&chunk[1], 20);
                    pairs.push(format!("{}: {}", field, value));
                }
            }
            pairs.join(", ")
        }
        KeyType::List => {
            // Get first few elements
            let cmd = format!("LRANGE {} 0 2", key);
            let result = conn.query(&cmd, &[]).await?;
            let items: Vec<String> = result
                .rows
                .iter()
                .filter_map(|row| row.get_by_name("value").and_then(|v| v.as_str()))
                .map(|s| truncate_str(s, 20))
                .collect();
            format!("[{}]", items.join(", "))
        }
        KeyType::Set => {
            // Get first few members with SSCAN
            let cmd = format!("SSCAN {} 0 COUNT 3", key);
            let result = conn.query(&cmd, &[]).await?;
            let members: Vec<String> = result
                .rows
                .iter()
                .skip(1) // Skip cursor
                .take(3)
                .filter_map(|row| row.get_by_name("value").and_then(|v| v.as_str()))
                .map(|s| truncate_str(s, 20))
                .collect();
            format!("{{{}}}", members.join(", "))
        }
        KeyType::Zset => {
            // Get first few members with scores
            let cmd = format!("ZRANGE {} 0 2 WITHSCORES", key);
            let result = conn.query(&cmd, &[]).await?;
            let items: Vec<String> = result
                .rows
                .iter()
                .filter_map(|row| row.get_by_name("value").and_then(|v| v.as_str()))
                .map(|s| s.to_string())
                .collect();
            // Format as "member(score)"
            let mut pairs = Vec::new();
            for chunk in items.chunks(2) {
                if chunk.len() == 2 {
                    pairs.push(format!("{}({})", truncate_str(&chunk[0], 15), &chunk[1]));
                }
            }
            pairs.join(", ")
        }
        KeyType::Stream => {
            // Get stream info
            let cmd = format!("XINFO STREAM {} FULL COUNT 1", key);
            match conn.query(&cmd, &[]).await {
                Ok(result) => {
                    // Just show last entry ID if available
                    result
                        .rows
                        .iter()
                        .find_map(|row| {
                            let key = row.get_by_name("key").and_then(|v| v.as_str());
                            let value = row.get_by_name("value").and_then(|v| v.as_str());
                            if key == Some("last-generated-id") {
                                value.map(|v| format!("last: {}", v))
                            } else {
                                None
                            }
                        })
                        .unwrap_or_else(|| "<stream>".to_string())
                }
                Err(_) => "<stream>".to_string(),
            }
        }
        KeyType::None => "<none>".to_string(),
    };

    Ok(preview)
}

/// Truncate a string to max length with ellipsis
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}
