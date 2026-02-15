//! Redis schema introspection
//!
//! Redis doesn't have traditional database schemas, but we map its concepts:
//! - Databases: Redis databases 0-15 (default 16 databases)
//! - Tables: Redis keys (each key is treated as a "table")
//! - Columns: For keys, we show type, TTL, and value preview
//!
//! This allows the schema browser to display Redis data in a familiar format.

use crate::RedisConnection;
use crate::keys::{
    KeyType, ListKeysOptions, get_database_size, get_key_info_with_memory, get_key_ttl,
    get_key_type, get_key_value_preview, list_keys_with_options,
};
use async_trait::async_trait;
use zqlz_core::{
    ColumnInfo, Connection, ConstraintInfo, DatabaseInfo, DatabaseObject, Dependency,
    ForeignKeyInfo, FunctionInfo, IndexInfo, KeyValueInfo, PrimaryKeyInfo, ProcedureInfo, Result,
    SchemaInfo, SchemaIntrospection, SequenceInfo, TableDetails, TableInfo, TableType, TriggerInfo,
    TypeInfo, ViewInfo, ZqlzError,
};

/// Default number of Redis databases (can be configured in redis.conf)
const DEFAULT_DATABASE_COUNT: u16 = 16;

#[async_trait]
impl SchemaIntrospection for RedisConnection {
    /// List Redis databases (0-N by default)
    ///
    /// Redis databases are numbered from 0 to N-1 (default N=16, but configurable).
    /// We try to get the actual count from CONFIG GET databases.
    #[tracing::instrument(skip(self))]
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        tracing::debug!("listing Redis databases");

        // Try to get the actual database count from config
        // CONFIG GET databases returns: ["databases", "<count>"]
        let database_count = match self.query("CONFIG GET databases", &[]).await {
            Ok(result) => {
                tracing::debug!("CONFIG GET databases returned {} rows", result.rows.len());

                // Redis CONFIG GET returns an array with [key, value] pairs
                // Our redis_value_to_rows converts this to rows with "key" and "value" columns
                // The second row (index 1) should have the count as the value
                let count = result
                    .rows
                    .iter()
                    .find_map(|row| {
                        // Try to get the value - it could be in different formats
                        // Format 1: key="databases", value="<count>"
                        let key = row.get_by_name("key").and_then(|v| v.as_str());
                        let value = row.get_by_name("value").and_then(|v| v.as_str());

                        tracing::debug!("Row: key={:?}, value={:?}", key, value);

                        if key == Some("databases") {
                            return value.and_then(|s| s.parse::<u16>().ok());
                        }

                        // Format 2: The value itself is the count (second element in array)
                        // Check if value is a number directly
                        if let Some(v) = value {
                            if let Ok(n) = v.parse::<u16>() {
                                return Some(n);
                            }
                        }

                        None
                    })
                    .unwrap_or(DEFAULT_DATABASE_COUNT);

                tracing::info!("Redis database count from config: {}", count);
                count
            }
            Err(e) => {
                let error_msg = e.to_string();
                // Check for authentication errors - these should be propagated, not silently ignored
                if error_msg.contains("NOAUTH") || error_msg.contains("Authentication") {
                    tracing::error!("Redis authentication error while listing databases: {}", e);
                    return Err(e);
                }
                // For other errors (like CONFIG being disabled), fall back to default
                tracing::warn!(
                    "Failed to get database count from CONFIG: {}, using default {}",
                    e,
                    DEFAULT_DATABASE_COUNT
                );
                DEFAULT_DATABASE_COUNT
            }
        };

        let mut databases = Vec::with_capacity(database_count as usize);

        for db_index in 0..database_count {
            // For the currently connected database, try to get size
            let size_bytes = if db_index == self.database() {
                get_database_size(self).await.ok().map(|count| count as i64)
            } else {
                None
            };

            databases.push(DatabaseInfo {
                name: format!("db{}", db_index),
                owner: None,
                encoding: None,
                size_bytes,
                comment: Some(format!("Redis database {}", db_index)),
            });
        }

        tracing::debug!(count = databases.len(), "databases listed");
        Ok(databases)
    }

    /// Redis doesn't have schemas, return empty
    #[tracing::instrument(skip(self))]
    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        Ok(vec![])
    }

    /// List Redis keys as "tables"
    ///
    /// Each key is represented as a table with its type and metadata.
    /// The schema parameter can be used as a pattern filter (e.g., "user:*").
    #[tracing::instrument(skip(self))]
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<TableInfo>> {
        tracing::debug!(pattern = ?schema, "listing Redis keys as tables");

        let pattern = schema.unwrap_or("*");
        let options = ListKeysOptions::new()
            .with_pattern(pattern)
            .with_limit(1000) // Limit to prevent overwhelming large databases
            .include_types();

        let keys = list_keys_with_options(self, &options).await?;

        let mut tables = Vec::with_capacity(keys.len());

        for key in keys {
            // Get key type
            let key_type = get_key_type(self, &key).await.unwrap_or(KeyType::None);

            // Get key size (element count) based on type
            let row_count = self.get_key_element_count(&key, key_type).await.ok();

            // Get key info with memory usage
            let key_info = get_key_info_with_memory(self, &key).await.ok();

            // Get value preview
            let value_preview = get_key_value_preview(self, &key, key_type, 50).await.ok();

            // Get TTL
            let ttl = get_key_ttl(self, &key).await.unwrap_or(-2);

            // Build KeyValueInfo
            let key_value_info = Some(
                KeyValueInfo::new(key_type.as_str())
                    .with_ttl(ttl)
                    .with_value_preview(value_preview.unwrap_or_default())
                    .with_size(key_info.as_ref().and_then(|i| i.memory_bytes).unwrap_or(0)),
            );

            tables.push(TableInfo {
                name: key,
                schema: Some(format!("db{}", self.database())),
                table_type: TableType::Table,
                owner: None,
                row_count,
                size_bytes: key_info.as_ref().and_then(|i| i.memory_bytes),
                comment: Some(format!("Redis {} key", key_type.as_str())),
                index_count: None,
                trigger_count: None,
                key_value_info,
            });
        }

        tracing::debug!(count = tables.len(), "keys listed as tables");
        Ok(tables)
    }

    /// Redis doesn't have views, return empty
    #[tracing::instrument(skip(self))]
    async fn list_views(&self, _schema: Option<&str>) -> Result<Vec<ViewInfo>> {
        Ok(vec![])
    }

    /// Get detailed info for a Redis key
    #[tracing::instrument(skip(self))]
    async fn get_table(&self, _schema: Option<&str>, name: &str) -> Result<TableDetails> {
        let key_type = get_key_type(self, name).await?;
        let ttl = get_key_ttl(self, name).await?;
        let row_count = self.get_key_element_count(name, key_type).await.ok();

        let info = TableInfo {
            name: name.to_string(),
            schema: Some(format!("db{}", self.database())),
            table_type: TableType::Table,
            owner: None,
            row_count,
            size_bytes: None,
            comment: Some(format!(
                "Redis {} key (TTL: {})",
                key_type.as_str(),
                format_ttl(ttl)
            )),
            index_count: None,
            trigger_count: None,
            key_value_info: None,
        };

        // Get columns based on key type
        let columns = self.get_columns(None, name).await?;

        Ok(TableDetails {
            info,
            columns,
            primary_key: None,
            foreign_keys: vec![],
            indexes: vec![],
            constraints: vec![],
            triggers: vec![],
        })
    }

    /// Get "columns" for a Redis key based on its type
    ///
    /// Different key types have different column structures:
    /// - string: value
    /// - hash: field, value
    /// - list: index, value
    /// - set: value
    /// - zset: value, score
    /// - stream: id, field, value
    #[tracing::instrument(skip(self))]
    async fn get_columns(&self, _schema: Option<&str>, table: &str) -> Result<Vec<ColumnInfo>> {
        let key_type = get_key_type(self, table).await?;

        let columns = match key_type {
            KeyType::String => vec![ColumnInfo {
                name: "value".to_string(),
                ordinal: 0,
                data_type: "string".to_string(),
                nullable: true,
                default_value: None,
                max_length: None,
                precision: None,
                scale: None,
                is_primary_key: false,
                is_auto_increment: false,
                is_unique: false,
                foreign_key: None,
                comment: Some("String value".to_string()),
            }],
            KeyType::Hash => vec![
                ColumnInfo {
                    name: "field".to_string(),
                    ordinal: 0,
                    data_type: "string".to_string(),
                    nullable: false,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: true,
                    is_auto_increment: false,
                    is_unique: true,
                    foreign_key: None,
                    comment: Some("Hash field name".to_string()),
                },
                ColumnInfo {
                    name: "value".to_string(),
                    ordinal: 1,
                    data_type: "string".to_string(),
                    nullable: true,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: false,
                    is_auto_increment: false,
                    is_unique: false,
                    foreign_key: None,
                    comment: Some("Hash field value".to_string()),
                },
            ],
            KeyType::List => vec![
                ColumnInfo {
                    name: "index".to_string(),
                    ordinal: 0,
                    data_type: "integer".to_string(),
                    nullable: false,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: true,
                    is_auto_increment: false,
                    is_unique: true,
                    foreign_key: None,
                    comment: Some("List index (0-based)".to_string()),
                },
                ColumnInfo {
                    name: "value".to_string(),
                    ordinal: 1,
                    data_type: "string".to_string(),
                    nullable: true,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: false,
                    is_auto_increment: false,
                    is_unique: false,
                    foreign_key: None,
                    comment: Some("List element value".to_string()),
                },
            ],
            KeyType::Set => vec![ColumnInfo {
                name: "member".to_string(),
                ordinal: 0,
                data_type: "string".to_string(),
                nullable: false,
                default_value: None,
                max_length: None,
                precision: None,
                scale: None,
                is_primary_key: true,
                is_auto_increment: false,
                is_unique: true,
                foreign_key: None,
                comment: Some("Set member".to_string()),
            }],
            KeyType::Zset => vec![
                ColumnInfo {
                    name: "member".to_string(),
                    ordinal: 0,
                    data_type: "string".to_string(),
                    nullable: false,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: true,
                    is_auto_increment: false,
                    is_unique: true,
                    foreign_key: None,
                    comment: Some("Sorted set member".to_string()),
                },
                ColumnInfo {
                    name: "score".to_string(),
                    ordinal: 1,
                    data_type: "double".to_string(),
                    nullable: false,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: false,
                    is_auto_increment: false,
                    is_unique: false,
                    foreign_key: None,
                    comment: Some("Sorted set score".to_string()),
                },
            ],
            KeyType::Stream => vec![
                ColumnInfo {
                    name: "id".to_string(),
                    ordinal: 0,
                    data_type: "string".to_string(),
                    nullable: false,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: true,
                    is_auto_increment: false,
                    is_unique: true,
                    foreign_key: None,
                    comment: Some("Stream entry ID (timestamp-sequence)".to_string()),
                },
                ColumnInfo {
                    name: "fields".to_string(),
                    ordinal: 1,
                    data_type: "json".to_string(),
                    nullable: false,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_primary_key: false,
                    is_auto_increment: false,
                    is_unique: false,
                    foreign_key: None,
                    comment: Some("Stream entry fields".to_string()),
                },
            ],
            KeyType::None => vec![],
        };

        Ok(columns)
    }

    /// Redis doesn't have indexes in the traditional sense
    #[tracing::instrument(skip(self))]
    async fn get_indexes(&self, _schema: Option<&str>, _table: &str) -> Result<Vec<IndexInfo>> {
        Ok(vec![])
    }

    /// Redis doesn't have foreign keys
    #[tracing::instrument(skip(self))]
    async fn get_foreign_keys(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        Ok(vec![])
    }

    /// Redis doesn't have primary keys in the traditional sense
    #[tracing::instrument(skip(self))]
    async fn get_primary_key(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Option<PrimaryKeyInfo>> {
        Ok(None)
    }

    /// Redis doesn't have constraints
    #[tracing::instrument(skip(self))]
    async fn get_constraints(
        &self,
        _schema: Option<&str>,
        _table: &str,
    ) -> Result<Vec<ConstraintInfo>> {
        Ok(vec![])
    }

    /// Redis doesn't have functions (but has Lua scripts)
    #[tracing::instrument(skip(self))]
    async fn list_functions(&self, _schema: Option<&str>) -> Result<Vec<FunctionInfo>> {
        Ok(vec![])
    }

    /// Redis doesn't have procedures
    #[tracing::instrument(skip(self))]
    async fn list_procedures(&self, _schema: Option<&str>) -> Result<Vec<ProcedureInfo>> {
        Ok(vec![])
    }

    /// Redis doesn't have triggers
    #[tracing::instrument(skip(self))]
    async fn list_triggers(
        &self,
        _schema: Option<&str>,
        _table: Option<&str>,
    ) -> Result<Vec<TriggerInfo>> {
        Ok(vec![])
    }

    /// Redis doesn't have sequences
    #[tracing::instrument(skip(self))]
    async fn list_sequences(&self, _schema: Option<&str>) -> Result<Vec<SequenceInfo>> {
        Ok(vec![])
    }

    /// Redis doesn't have custom types
    #[tracing::instrument(skip(self))]
    async fn list_types(&self, _schema: Option<&str>) -> Result<Vec<TypeInfo>> {
        Ok(vec![])
    }

    /// Generate Redis command to recreate a key
    #[tracing::instrument(skip(self))]
    async fn generate_ddl(&self, object: &DatabaseObject) -> Result<String> {
        // For Redis, we generate the commands to recreate the key
        let key = &object.name;
        let key_type = get_key_type(self, key).await?;
        let ttl = get_key_ttl(self, key).await?;

        let ddl = match key_type {
            KeyType::String => {
                let result = self.query(&format!("GET {}", key), &[]).await?;
                let value = result
                    .rows
                    .first()
                    .and_then(|r| r.get_by_name("value"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                format!("SET {} \"{}\"", key, escape_redis_string(value))
            }
            KeyType::Hash => {
                let result = self.query(&format!("HGETALL {}", key), &[]).await?;
                let mut cmd = format!("HMSET {}", key);
                for row in &result.rows {
                    if let (Some(field), Some(value)) = (
                        row.get_by_name("key").and_then(|v| v.as_str()),
                        row.get_by_name("value").and_then(|v| v.as_str()),
                    ) {
                        cmd.push_str(&format!(" {} \"{}\"", field, escape_redis_string(value)));
                    }
                }
                cmd
            }
            KeyType::List => {
                let result = self.query(&format!("LRANGE {} 0 -1", key), &[]).await?;
                let values: Vec<String> = result
                    .rows
                    .iter()
                    .filter_map(|r| r.get_by_name("value").and_then(|v| v.as_str()))
                    .map(|s| format!("\"{}\"", escape_redis_string(s)))
                    .collect();
                format!("RPUSH {} {}", key, values.join(" "))
            }
            KeyType::Set => {
                let result = self.query(&format!("SMEMBERS {}", key), &[]).await?;
                let members: Vec<String> = result
                    .rows
                    .iter()
                    .filter_map(|r| r.get_by_name("value").and_then(|v| v.as_str()))
                    .map(|s| format!("\"{}\"", escape_redis_string(s)))
                    .collect();
                format!("SADD {} {}", key, members.join(" "))
            }
            KeyType::Zset => {
                let result = self
                    .query(&format!("ZRANGE {} 0 -1 WITHSCORES", key), &[])
                    .await?;
                let mut cmd = format!("ZADD {}", key);
                // ZRANGE WITHSCORES returns alternating member, score
                let mut iter = result.rows.iter();
                while let Some(member_row) = iter.next() {
                    if let Some(score_row) = iter.next() {
                        if let (Some(member), Some(score)) = (
                            member_row.get_by_name("value").and_then(|v| v.as_str()),
                            score_row.get_by_name("value").and_then(|v| v.as_str()),
                        ) {
                            cmd.push_str(&format!(
                                " {} \"{}\"",
                                score,
                                escape_redis_string(member)
                            ));
                        }
                    }
                }
                cmd
            }
            KeyType::Stream | KeyType::None => {
                return Err(ZqlzError::NotSupported(format!(
                    "DDL generation not supported for {} keys",
                    key_type.as_str()
                )));
            }
        };

        // Add TTL command if key has expiry
        let full_ddl = if ttl > 0 {
            format!("{}\nEXPIRE {} {}", ddl, key, ttl)
        } else {
            ddl
        };

        Ok(full_ddl)
    }

    /// Redis keys don't have dependencies
    #[tracing::instrument(skip(self))]
    async fn get_dependencies(&self, _object: &DatabaseObject) -> Result<Vec<Dependency>> {
        Ok(vec![])
    }
}

impl RedisConnection {
    /// Get element count for a key based on its type
    async fn get_key_element_count(&self, key: &str, key_type: KeyType) -> Result<i64> {
        let cmd = match key_type {
            KeyType::String => return Ok(1), // String is always 1 element
            KeyType::Hash => format!("HLEN {}", key),
            KeyType::List => format!("LLEN {}", key),
            KeyType::Set => format!("SCARD {}", key),
            KeyType::Zset => format!("ZCARD {}", key),
            KeyType::Stream => format!("XLEN {}", key),
            KeyType::None => return Ok(0),
        };

        let result = self.query(&cmd, &[]).await?;

        let count = result
            .rows
            .first()
            .and_then(|row| row.get_by_name("value"))
            .and_then(|v| match v {
                zqlz_core::Value::Int64(n) => Some(*n),
                zqlz_core::Value::String(s) => s.parse().ok(),
                _ => None,
            })
            .unwrap_or(0);

        Ok(count)
    }
}

/// Format TTL for display
fn format_ttl(ttl: i64) -> String {
    if ttl == -1 {
        "no expiry".to_string()
    } else if ttl == -2 {
        "key not found".to_string()
    } else if ttl < 60 {
        format!("{}s", ttl)
    } else if ttl < 3600 {
        format!("{}m {}s", ttl / 60, ttl % 60)
    } else if ttl < 86400 {
        format!("{}h {}m", ttl / 3600, (ttl % 3600) / 60)
    } else {
        format!("{}d {}h", ttl / 86400, (ttl % 86400) / 3600)
    }
}

/// Escape special characters in Redis string values
fn escape_redis_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}
