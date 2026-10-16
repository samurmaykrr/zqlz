//! Redis connection: command execution, key-value store, and schema-security glue

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use uuid::Uuid;
use zqlz_core::{
    ColumnMeta, Connection, ConnectionConfig, ConnectionScope, ConnectionSecurity,
    DriverCategory, DropTableOptions, DropTriggerOptions, DropViewOptions, ExplainParserKind,
    KeyValueCellUpdateRequest, KeyValueDatabaseInfo, KeyValueDeleteOutcome,
    KeyValueDeleteRequest, KeyValueEntry, KeyValueKeySummary, KeyValueKind, KeyValueSaveOutcome,
    KeyValueSaveRequest, KeyValueScanRequest, KeyValueScanResult, KeyValueStore, QueryResult,
    ResolvedConnectionScope, Result, Row, SchemaIntrospection, SqlObjectName, StatementResult,
    Transaction, Value, ZqlzError, parse_redis_database_index,
    security::{TlsConfig, TlsMode},
};

use crate::RedisSshTunnel;

use super::parsing::{
    StreamEntries, array_preview, bytes_to_display_string, hash_scan_preview,
    parse_single_redis_command, parse_stream_entries, scan_items_preview, stream_entries_to_json,
    stream_preview, string_key_preview, truncate_text, zset_preview,
};
use super::rows::{redis_value_to_rows, redis_value_to_zqlz_value};

/// Liveness command issued by connection heartbeats.
pub(super) const REDIS_PING_COMMAND: &str = "PING";

pub struct RedisConnection {
    connection: Mutex<redis::aio::MultiplexedConnection>,
    config: ConnectionConfig,
    tls_config: Option<TlsConfig>,
    ssh_tunnel: Option<RedisSshTunnel>,
    closed: AtomicBool,
}

impl RedisConnection {
    /// Create a new Redis connection wrapper
    pub fn new(
        connection: redis::aio::MultiplexedConnection,
        config: ConnectionConfig,
        tls_config: Option<TlsConfig>,
        ssh_tunnel: Option<RedisSshTunnel>,
    ) -> Self {
        Self {
            connection: Mutex::new(connection),
            config,
            tls_config,
            ssh_tunnel,
            closed: AtomicBool::new(false),
        }
    }

    /// Get the current database number (0-15)
    pub fn database(&self) -> u16 {
        self.config
            .get_string("database")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    fn ensure_not_closed(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ZqlzError::Driver("Connection is closed".to_string()));
        }
        Ok(())
    }

    async fn run_redis_command(
        &self,
        command: &str,
        configure: impl FnOnce(&mut redis::Cmd),
    ) -> Result<redis::Value> {
        self.ensure_not_closed()?;
        let mut connection = self.connection.lock().await;
        let mut command_builder = redis::cmd(command);
        configure(&mut command_builder);
        command_builder
            .query_async(&mut *connection)
            .await
            .map_err(|error| ZqlzError::Driver(format!("Redis command failed: {}", error)))
    }

    async fn run_redis_int_command(
        &self,
        command: &str,
        configure: impl FnOnce(&mut redis::Cmd),
    ) -> Result<i64> {
        let value = self.run_redis_command(command, configure).await?;
        match value {
            redis::Value::Int(value) => Ok(value),
            redis::Value::BulkString(bytes) => String::from_utf8_lossy(&bytes)
                .parse::<i64>()
                .map_err(|error| ZqlzError::Driver(format!("Invalid Redis integer: {}", error))),
            redis::Value::SimpleString(value) => value
                .parse::<i64>()
                .map_err(|error| ZqlzError::Driver(format!("Invalid Redis integer: {}", error))),
            redis::Value::Nil => Ok(0),
            other => Err(ZqlzError::Driver(format!(
                "Unexpected Redis integer response: {:?}",
                other
            ))),
        }
    }

    fn key_value_kind_from_type(value_type: &str) -> KeyValueKind {
        match value_type.to_ascii_lowercase().as_str() {
            "string" => KeyValueKind::String,
            "list" => KeyValueKind::List,
            "set" => KeyValueKind::Set,
            "zset" => KeyValueKind::ZSet,
            "hash" => KeyValueKind::Hash,
            "stream" => KeyValueKind::Stream,
            "json" | "rejson-rl" => KeyValueKind::Json,
            _ => KeyValueKind::None,
        }
    }

    pub(super) fn value_to_string(value: &redis::Value) -> String {
        match value {
            redis::Value::Nil => String::new(),
            redis::Value::Int(value) => value.to_string(),
            redis::Value::BulkString(bytes) => bytes_to_display_string(bytes),
            redis::Value::SimpleString(value) => value.clone(),
            redis::Value::Okay => "OK".to_string(),
            redis::Value::Double(value) => value.to_string(),
            redis::Value::Boolean(value) => value.to_string(),
            redis::Value::Array(values) => values
                .iter()
                .map(Self::value_to_string)
                .collect::<Vec<_>>()
                .join(", "),
            other => format!("{:?}", other),
        }
    }

    fn string_column(name: &str, ordinal: usize) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            data_type: "TEXT".to_string(),
            nullable: true,
            ordinal,
            max_length: None,
            precision: None,
            scale: None,
            auto_increment: false,
            default_value: None,
            comment: None,
            enum_values: None,
        }
    }

    fn rows_from_values(values: Vec<Value>, key_column_name: &str) -> QueryResult {
        let columns = vec![
            Self::string_column(key_column_name, 0),
            Self::string_column("value", 1),
        ];
        let column_names = vec![key_column_name.to_string(), "value".to_string()];
        let rows = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                Row::new(
                    column_names.clone(),
                    vec![Value::String(index.to_string()), value],
                )
            })
            .collect::<Vec<_>>();
        QueryResult {
            id: Uuid::new_v4(),
            columns,
            total_rows: Some(rows.len() as u64),
            rows,
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms: 0,
            warnings: Vec::new(),
        }
    }

    fn parse_collection_items(value: &str) -> Vec<String> {
        serde_json::from_str::<Vec<serde_json::Value>>(value)
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| match item {
                        serde_json::Value::String(value) => value,
                        other => other.to_string(),
                    })
                    .collect()
            })
            .unwrap_or_else(|_| {
                value
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(ToOwned::to_owned)
                    .collect()
            })
    }

    fn parse_hash_fields(value: &str) -> Result<Vec<(String, String)>> {
        let json = serde_json::from_str::<serde_json::Value>(value)
            .map_err(|error| ZqlzError::Driver(format!("Invalid hash payload JSON: {}", error)))?;
        let object = json
            .as_object()
            .ok_or_else(|| ZqlzError::Driver("Hash payload must be a JSON object".to_string()))?;
        Ok(object
            .iter()
            .map(|(key, value)| {
                let value = match value {
                    serde_json::Value::String(value) => value.clone(),
                    other => other.to_string(),
                };
                (key.clone(), value)
            })
            .collect())
    }

    pub(super) fn parse_zset_items(value: &str) -> Result<Vec<(f64, String)>> {
        let json = serde_json::from_str::<serde_json::Value>(value)
            .map_err(|error| ZqlzError::Driver(format!("Invalid zset payload JSON: {}", error)))?;

        if let Some(object) = json.as_object() {
            return object
                .iter()
                .map(|(member, score)| {
                    let score = score.as_f64().ok_or_else(|| {
                        ZqlzError::Driver(format!("ZSet member '{}' has invalid score", member))
                    })?;
                    Ok((score, member.clone()))
                })
                .collect();
        }

        let array = json.as_array().ok_or_else(|| {
            ZqlzError::Driver("ZSet payload must be a JSON object or array".to_string())
        })?;

        if array.iter().all(|item| item.as_object().is_some()) {
            return array
                .iter()
                .map(|item| {
                    let object = item.as_object().ok_or_else(|| {
                        ZqlzError::Driver("ZSet members must be JSON objects".to_string())
                    })?;
                    let score = object
                        .get("score")
                        .and_then(|value| value.as_f64())
                        .ok_or_else(|| {
                            ZqlzError::Driver("ZSet member missing score".to_string())
                        })?;
                    let member = object
                        .get("member")
                        .and_then(|value| value.as_str())
                        .ok_or_else(|| {
                            ZqlzError::Driver("ZSet member missing member".to_string())
                        })?;
                    Ok((score, member.to_string()))
                })
                .collect();
        }

        array
            .chunks(2)
            .map(|chunk| {
                if chunk.len() != 2 {
                    return Err(ZqlzError::Driver(
                        "ZSet array payload must contain score/member pairs".to_string(),
                    ));
                }
                let score = chunk[0].as_f64().ok_or_else(|| {
                    ZqlzError::Driver("ZSet array pair missing score".to_string())
                })?;
                let member = chunk[1].as_str().ok_or_else(|| {
                    ZqlzError::Driver("ZSet array pair missing member".to_string())
                })?;
                Ok((score, member.to_string()))
            })
            .collect()
    }

    pub(super) fn parse_stream_entries(value: &str) -> Result<StreamEntries> {
        parse_stream_entries(value)
    }

    fn ensure_key_value_database(&self, database_index: u16) -> Result<()> {
        let current_database = self.database();
        if database_index != current_database {
            return Err(ZqlzError::Driver(format!(
                "Redis connection is scoped to database {}, but request targeted database {}",
                current_database, database_index
            )));
        }
        Ok(())
    }

    async fn keyspace_database_counts(&self) -> Result<HashMap<u16, i64>> {
        let value = self
            .run_redis_command("INFO", |command| {
                command.arg("keyspace");
            })
            .await?;
        let text = Self::value_to_string(&value);
        let mut counts = HashMap::new();

        for line in text.lines() {
            let Some((database_name, stats)) = line.split_once(':') else {
                continue;
            };
            let Some(index) = parse_redis_database_index(database_name) else {
                continue;
            };
            let key_count = stats
                .split(',')
                .find_map(|part| {
                    let (key, value) = part.split_once('=')?;
                    (key == "keys").then(|| value.parse::<i64>().ok()).flatten()
                })
                .unwrap_or(0);
            counts.insert(index, key_count);
        }

        Ok(counts)
    }

    async fn key_preview(&self, key: &str, kind: KeyValueKind) -> Option<String> {
        match kind {
            KeyValueKind::String => self
                .run_redis_command("GETRANGE", |command| {
                    command.arg(key).arg(0).arg(80);
                })
                .await
                .ok()
                .map(|value| string_key_preview(key, &value)),
            KeyValueKind::Json => self
                .run_redis_command("GETRANGE", |command| {
                    command.arg(key).arg(0).arg(120);
                })
                .await
                .ok()
                .map(|value| truncate_text(&Self::value_to_string(&value), 120)),
            KeyValueKind::List => self
                .run_redis_command("LRANGE", |command| {
                    command.arg(key).arg(0).arg(2);
                })
                .await
                .ok()
                .map(|value| array_preview(value, "[", "]")),
            KeyValueKind::Set => self
                .run_redis_command("SSCAN", |command| {
                    command.arg(key).arg(0).arg("COUNT").arg(3);
                })
                .await
                .ok()
                .and_then(scan_items_preview)
                .map(|items| format!("{{{}}}", items.join(", "))),
            KeyValueKind::ZSet => self
                .run_redis_command("ZRANGE", |command| {
                    command.arg(key).arg(0).arg(2).arg("WITHSCORES");
                })
                .await
                .ok()
                .map(zset_preview),
            KeyValueKind::Hash => self
                .run_redis_command("HSCAN", |command| {
                    command.arg(key).arg(0).arg("COUNT").arg(3);
                })
                .await
                .ok()
                .and_then(hash_scan_preview),
            KeyValueKind::Stream => {
                let length = self
                    .run_redis_int_command("XLEN", |command| {
                        command.arg(key);
                    })
                    .await
                    .ok()
                    .unwrap_or(0);
                self.run_redis_command("XREVRANGE", |command| {
                    command.arg(key).arg("+").arg("-").arg("COUNT").arg(1);
                })
                .await
                .ok()
                .map(|value| stream_preview(value, length))
            }
            KeyValueKind::None => None,
        }
    }
}

#[async_trait]
impl Connection for RedisConnection {
    fn driver_name(&self) -> &str {
        "redis"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("redis")
    }

    fn requires_database_scoped_connection(&self) -> bool {
        true
    }

    fn normalize_database_scope_name(&self, database_name: &str) -> String {
        parse_redis_database_index(database_name)
            .map(|index| index.to_string())
            .unwrap_or_else(|| database_name.trim().to_string())
    }

    fn driver_category(&self) -> DriverCategory {
        DriverCategory::KeyValue
    }

    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        let database_index = match scope {
            ConnectionScope::Default => self.database(),
            ConnectionScope::KeyValueDatabase(index) => index,
            ConnectionScope::Database(database_name)
            | ConnectionScope::Namespace(database_name) => {
                parse_redis_database_index(&database_name).ok_or_else(|| {
                    ZqlzError::Driver(format!("Invalid Redis database scope: {}", database_name))
                })?
            }
        };

        let database_name = database_index.to_string();
        resolved.normalized_scope = ConnectionScope::KeyValueDatabase(database_index);
        resolved.effective_database = Some(database_name.clone());
        resolved.physical_database_key = Some(database_name);
        resolved.requires_dedicated_connection = true;

        Ok(resolved)
    }

    async fn current_database_name(&self) -> Result<Option<String>> {
        Ok(Some(self.database().to_string()))
    }

    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::None
    }

    fn rename_table_sql(
        &self,
        _table_name: &SqlObjectName,
        _new_table_name: &str,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL table rename operations".to_string(),
        ))
    }

    fn drop_table_sql(
        &self,
        _table_name: &SqlObjectName,
        _options: DropTableOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL DROP TABLE operations".to_string(),
        ))
    }

    fn drop_view_sql(
        &self,
        _view_name: &SqlObjectName,
        _options: DropViewOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL DROP VIEW operations".to_string(),
        ))
    }

    fn drop_trigger_sql(
        &self,
        _trigger_name: &SqlObjectName,
        _table_name: Option<&SqlObjectName>,
        _options: DropTriggerOptions,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL DROP TRIGGER operations".to_string(),
        ))
    }

    fn truncate_table_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL TRUNCATE TABLE operations".to_string(),
        ))
    }

    fn duplicate_table_sql(
        &self,
        _source_table_name: &SqlObjectName,
        _new_table_name: &SqlObjectName,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL table duplication operations".to_string(),
        ))
    }

    fn clear_table_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL table clear operations".to_string(),
        ))
    }

    fn table_has_rows_sql(&self, _table_name: &SqlObjectName) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL table row-existence queries".to_string(),
        ))
    }

    fn select_rows_sql(
        &self,
        _table_name: &SqlObjectName,
        _projected_columns: &[String],
        _where_clause_sql: Option<&str>,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL row-selection queries".to_string(),
        ))
    }

    fn select_distinct_rows_sql(
        &self,
        _table_name: &SqlObjectName,
        _projected_columns: &[String],
        _where_clause_sql: Option<&str>,
        _order_by_columns: &[String],
        _limit: u64,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL distinct-row queries".to_string(),
        ))
    }

    fn insert_row_sql(
        &self,
        _table_name: &SqlObjectName,
        _column_names: &[String],
        _value_count: usize,
    ) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL INSERT-row statement generation".to_string(),
        ))
    }

    fn performance_metrics_query_sql(&self) -> Result<String> {
        Err(ZqlzError::NotSupported(
            "Redis does not support SQL performance metrics queries".to_string(),
        ))
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        self.ensure_not_closed()?;
        let start = std::time::Instant::now();

        let mut conn = self.connection.lock().await;

        let (command, args) = parse_single_redis_command(sql)?;
        let mut cmd = redis::cmd(&command);

        for arg in args {
            cmd.arg(arg);
        }

        // Add any additional parameters
        for param in params {
            match param {
                Value::String(s) => cmd.arg(s.as_str()),
                Value::Int64(n) => cmd.arg(*n),
                Value::Float64(f) => cmd.arg(*f),
                Value::Bool(b) => cmd.arg(if *b { "1" } else { "0" }),
                Value::Bytes(b) => cmd.arg(b.as_slice()),
                Value::Null => cmd.arg(""),
                _ => cmd.arg(param.to_string().as_str()),
            };
        }

        let result: redis::RedisResult<redis::Value> = cmd.query_async(&mut *conn).await;

        let execution_time = start.elapsed();

        match result {
            Ok(value) => {
                tracing::debug!(
                    command = %command,
                    duration_ms = execution_time.as_millis() as u64,
                    "execute completed"
                );

                // Determine affected count based on return value
                let affected = match &value {
                    redis::Value::Int(n) => *n as u64,
                    redis::Value::Okay => 1,
                    redis::Value::Nil => 0,
                    _ => 1,
                };

                Ok(StatementResult {
                    is_query: false,
                    result: None,
                    affected_rows: affected,
                    error: None,
                })
            }
            Err(e) => Err(ZqlzError::Driver(format!("Redis command failed: {}", e))),
        }
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.ensure_not_closed()?;
        let start = std::time::Instant::now();

        let mut conn = self.connection.lock().await;

        let (command, args) = parse_single_redis_command(sql)?;
        let mut cmd = redis::cmd(&command);

        for arg in args {
            cmd.arg(arg);
        }

        for param in params {
            match param {
                Value::String(s) => cmd.arg(s.as_str()),
                Value::Int64(n) => cmd.arg(*n),
                Value::Float64(f) => cmd.arg(*f),
                Value::Bool(b) => cmd.arg(if *b { "1" } else { "0" }),
                Value::Bytes(b) => cmd.arg(b.as_slice()),
                Value::Null => cmd.arg(""),
                _ => cmd.arg(param.to_string().as_str()),
            };
        }

        let result: redis::RedisResult<redis::Value> = cmd.query_async(&mut *conn).await;

        let execution_time_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(value) => {
                let (columns, rows) = redis_value_to_rows(&value);

                tracing::debug!(
                    command = %command,
                    row_count = rows.len(),
                    duration_ms = execution_time_ms,
                    "query completed"
                );

                Ok(QueryResult {
                    id: Uuid::new_v4(),
                    columns,
                    rows,
                    total_rows: None,
                    is_estimated_total: false,
                    affected_rows: 0,
                    execution_time_ms,
                    warnings: Vec::new(),
                })
            }
            Err(e) => Err(ZqlzError::Driver(format!("Redis command failed: {}", e))),
        }
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        self.ensure_not_closed()?;
        Err(ZqlzError::NotImplemented(
            "Redis transactions (MULTI/EXEC) will be implemented in a future update".into(),
        ))
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        tracing::debug!("Redis connection closed");
        Ok(())
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn ping_query_sql(&self) -> &'static str {
        REDIS_PING_COMMAND
    }

    fn is_busy(&self) -> bool {
        self.connection.try_lock().is_err()
    }


    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }

    fn as_key_value_store(&self) -> Option<&dyn KeyValueStore> {
        Some(self)
    }
}

impl ConnectionSecurity for RedisConnection {
    fn supports_ssh(&self) -> bool {
        true
    }

    fn supports_tls(&self) -> bool {
        true
    }

    fn tls_config(&self) -> Option<&TlsConfig> {
        self.tls_config.as_ref()
    }

    fn is_encrypted(&self) -> bool {
        self.tls_config
            .as_ref()
            .map(|config| config.mode != TlsMode::Disable)
            .unwrap_or(false)
    }

    fn is_tunneled(&self) -> bool {
        self.ssh_tunnel.is_some()
    }
}

#[async_trait]
impl KeyValueStore for RedisConnection {
    async fn list_key_value_databases(&self) -> Result<Vec<KeyValueDatabaseInfo>> {
        let keyspace_counts = self.keyspace_database_counts().await.unwrap_or_default();
        let databases = self
            .as_schema_introspection()
            .ok_or_else(|| ZqlzError::Driver("Redis schema introspection unavailable".to_string()))?
            .list_databases()
            .await?;
        Ok(databases
            .into_iter()
            .filter_map(|database| {
                parse_redis_database_index(&database.name).map(|index| KeyValueDatabaseInfo {
                    index,
                    size_bytes: keyspace_counts.get(&index).copied().or(database.size_bytes),
                })
            })
            .collect())
    }

    async fn scan_keys(&self, request: KeyValueScanRequest) -> Result<KeyValueScanResult> {
        self.ensure_key_value_database(request.database_index)?;
        let effective_limit = request.limit.max(1);
        let effective_scan_batch_size = request.scan_batch_size.max(1);
        let mut keys = Vec::new();
        let mut cursor = 0u64;

        loop {
            let value = self
                .run_redis_command("SCAN", |command| {
                    command
                        .arg(cursor)
                        .arg("MATCH")
                        .arg("*")
                        .arg("COUNT")
                        .arg(effective_scan_batch_size);
                })
                .await?;
            let redis::Value::Array(items) = value else {
                break;
            };
            if items.len() < 2 {
                break;
            }

            cursor = Self::value_to_string(&items[0]).parse::<u64>().unwrap_or(0);
            if let redis::Value::Array(batch) = &items[1] {
                for key in batch {
                    keys.push(Self::value_to_string(key));
                    if keys.len() >= effective_limit {
                        break;
                    }
                }
            }

            if cursor == 0 || keys.len() >= effective_limit {
                break;
            }
        }

        keys.sort();
        Ok(KeyValueScanResult { keys })
    }

    async fn load_key_summaries(&self, database_index: u16) -> Result<Vec<KeyValueKeySummary>> {
        self.ensure_key_value_database(database_index)?;
        let keys = self
            .scan_keys(KeyValueScanRequest {
                database_index,
                limit: 1000,
                scan_batch_size: 100,
            })
            .await?
            .keys;

        let mut summaries = Vec::with_capacity(keys.len());
        for key in keys {
            let key_type = Self::value_to_string(
                &self
                    .run_redis_command("TYPE", |command| {
                        command.arg(&key);
                    })
                    .await?,
            );
            let kind = Self::key_value_kind_from_type(&key_type);
            let ttl_seconds = self
                .run_redis_int_command("TTL", |command| {
                    command.arg(&key);
                })
                .await
                .ok();
            let size_bytes = self
                .run_redis_int_command("MEMORY", |command| {
                    command.arg("USAGE").arg(&key);
                })
                .await
                .ok();
            let preview = self.key_preview(&key, kind).await;
            summaries.push(KeyValueKeySummary {
                key,
                kind,
                ttl_seconds,
                size_bytes,
                preview,
            });
        }

        Ok(summaries)
    }

    async fn read_key(&self, key: &str, limit: usize) -> Result<KeyValueEntry> {
        let key_type = Self::value_to_string(
            &self
                .run_redis_command("TYPE", |command| {
                    command.arg(key);
                })
                .await?,
        );
        let kind = Self::key_value_kind_from_type(&key_type);
        let limit = limit.max(1);
        let ttl_seconds = self
            .run_redis_int_command("TTL", |command| {
                command.arg(key);
            })
            .await
            .ok();

        let data = match kind {
            KeyValueKind::String => {
                let value = self
                    .run_redis_command("GET", |command| {
                        command.arg(key);
                    })
                    .await?;
                Self::rows_from_values(vec![redis_value_to_zqlz_value(&value)], "index")
            }
            KeyValueKind::Json => {
                let value = self
                    .run_redis_command("GET", |command| {
                        command.arg(key);
                    })
                    .await?;
                Self::rows_from_values(vec![Value::String(Self::value_to_string(&value))], "index")
            }
            KeyValueKind::List => {
                let end = limit.saturating_sub(1);
                let value = self
                    .run_redis_command("LRANGE", |command| {
                        command.arg(key).arg(0).arg(end);
                    })
                    .await?;
                let values = match value {
                    redis::Value::Array(items) => items
                        .iter()
                        .map(|item| Value::String(Self::value_to_string(item)))
                        .collect(),
                    _ => Vec::new(),
                };
                Self::rows_from_values(values, "index")
            }
            KeyValueKind::Set => {
                let value = self
                    .run_redis_command("SMEMBERS", |command| {
                        command.arg(key);
                    })
                    .await?;
                let values = match value {
                    redis::Value::Array(items) => items
                        .iter()
                        .take(limit)
                        .map(|item| Value::String(Self::value_to_string(item)))
                        .collect(),
                    _ => Vec::new(),
                };
                Self::rows_from_values(values, "member")
            }
            KeyValueKind::ZSet => {
                let end = limit.saturating_sub(1);
                let value = self
                    .run_redis_command("ZRANGE", |command| {
                        command.arg(key).arg(0).arg(end).arg("WITHSCORES");
                    })
                    .await?;
                let items = match value {
                    redis::Value::Array(items) => items,
                    _ => Vec::new(),
                };
                let columns = vec![
                    Self::string_column("member", 0),
                    Self::string_column("score", 1),
                ];
                let column_names = vec!["member".to_string(), "score".to_string()];
                let rows = items
                    .chunks(2)
                    .filter(|chunk| chunk.len() == 2)
                    .map(|chunk| {
                        Row::new(
                            column_names.clone(),
                            vec![
                                Value::String(Self::value_to_string(&chunk[0])),
                                Value::String(Self::value_to_string(&chunk[1])),
                            ],
                        )
                    })
                    .collect::<Vec<_>>();
                QueryResult {
                    id: Uuid::new_v4(),
                    columns,
                    total_rows: Some(rows.len() as u64),
                    rows,
                    is_estimated_total: false,
                    affected_rows: 0,
                    execution_time_ms: 0,
                    warnings: Vec::new(),
                }
            }
            KeyValueKind::Hash => {
                let value = self
                    .run_redis_command("HGETALL", |command| {
                        command.arg(key);
                    })
                    .await?;
                let items = match value {
                    redis::Value::Array(items) => items,
                    _ => Vec::new(),
                };
                let columns = vec![
                    Self::string_column("field", 0),
                    Self::string_column("value", 1),
                ];
                let column_names = vec!["field".to_string(), "value".to_string()];
                let rows = items
                    .chunks(2)
                    .filter(|chunk| chunk.len() == 2)
                    .map(|chunk| {
                        Row::new(
                            column_names.clone(),
                            vec![
                                Value::String(Self::value_to_string(&chunk[0])),
                                Value::String(Self::value_to_string(&chunk[1])),
                            ],
                        )
                    })
                    .collect::<Vec<_>>();
                QueryResult {
                    id: Uuid::new_v4(),
                    columns,
                    total_rows: Some(rows.len() as u64),
                    rows,
                    is_estimated_total: false,
                    affected_rows: 0,
                    execution_time_ms: 0,
                    warnings: Vec::new(),
                }
            }
            KeyValueKind::Stream => {
                let value = self
                    .run_redis_command("XRANGE", |command| {
                        command.arg(key).arg("-").arg("+").arg("COUNT").arg(limit);
                    })
                    .await?;
                let serialized = stream_entries_to_json(&value)?;
                Self::rows_from_values(vec![Value::String(serialized)], "entry")
            }
            KeyValueKind::None => QueryResult::empty(),
        };

        Ok(KeyValueEntry {
            key: key.to_string(),
            kind,
            ttl_seconds,
            data,
        })
    }

    async fn save_key(&self, request: KeyValueSaveRequest) -> Result<KeyValueSaveOutcome> {
        let temporary_key = &request.temporary_key;
        let should_protect_target =
            request.original_key.is_empty() || request.original_key != request.new_key;
        let was_renamed =
            !request.original_key.is_empty() && request.original_key != request.new_key;
        if should_protect_target
            && self
                .run_redis_int_command("EXISTS", |command| {
                    command.arg(&request.new_key);
                })
                .await?
                > 0
        {
            return Err(ZqlzError::Driver(format!(
                "Redis key '{}' already exists",
                request.new_key
            )));
        }

        let save_result = async {
            match request.kind {
                KeyValueKind::String => {
                    self.run_redis_command("SET", |command| {
                        command.arg(temporary_key).arg(&request.serialized_value);
                    })
                    .await?;
                }
                KeyValueKind::Json => {
                    self.run_redis_command("SET", |command| {
                        command.arg(temporary_key).arg(&request.serialized_value);
                    })
                    .await?;
                }
                KeyValueKind::List => {
                    let items = Self::parse_collection_items(&request.serialized_value);
                    if items.is_empty() {
                        return Err(ZqlzError::Driver(
                            "Cannot save an empty Redis list".to_string(),
                        ));
                    }
                    self.run_redis_command("RPUSH", |command| {
                        command.arg(temporary_key);
                        for item in items {
                            command.arg(item);
                        }
                    })
                    .await?;
                }
                KeyValueKind::Set => {
                    let items = Self::parse_collection_items(&request.serialized_value);
                    if items.is_empty() {
                        return Err(ZqlzError::Driver(
                            "Cannot save an empty Redis set".to_string(),
                        ));
                    }
                    self.run_redis_command("SADD", |command| {
                        command.arg(temporary_key);
                        for item in items {
                            command.arg(item);
                        }
                    })
                    .await?;
                }
                KeyValueKind::ZSet => {
                    let items = Self::parse_zset_items(&request.serialized_value)?;
                    if items.is_empty() {
                        return Err(ZqlzError::Driver(
                            "Cannot save an empty Redis sorted set".to_string(),
                        ));
                    }
                    self.run_redis_command("ZADD", |command| {
                        command.arg(temporary_key);
                        for (score, member) in items {
                            command.arg(score).arg(member);
                        }
                    })
                    .await?;
                }
                KeyValueKind::Hash => {
                    let fields = Self::parse_hash_fields(&request.serialized_value)?;
                    if fields.is_empty() {
                        return Err(ZqlzError::Driver(
                            "Cannot save an empty Redis hash".to_string(),
                        ));
                    }
                    self.run_redis_command("HSET", |command| {
                        command.arg(temporary_key);
                        for (field, value) in fields {
                            command.arg(field).arg(value);
                        }
                    })
                    .await?;
                }
                KeyValueKind::Stream => {
                    let entries = Self::parse_stream_entries(&request.serialized_value)?;
                    if entries.is_empty() {
                        return Err(ZqlzError::Driver(
                            "Cannot save an empty Redis stream".to_string(),
                        ));
                    }
                    for (id, fields) in entries {
                        self.run_redis_command("XADD", |command| {
                            command.arg(temporary_key).arg(id);
                            for (field, value) in fields {
                                command.arg(field).arg(value);
                            }
                        })
                        .await?;
                    }
                }
                KeyValueKind::None => {
                    return Err(ZqlzError::Driver(
                        "Cannot save unknown Redis type".to_string(),
                    ));
                }
            }

            if let Some(ttl_seconds) = request.ttl_seconds {
                self.run_redis_command("EXPIRE", |command| {
                    command.arg(temporary_key).arg(ttl_seconds);
                })
                .await?;
            } else if let Err(error) = self
                .run_redis_command("PERSIST", |command| {
                    command.arg(temporary_key);
                })
                .await
            {
                tracing::debug!(
                    key = temporary_key,
                    error = %error,
                    "failed to clear Redis key TTL before rename"
                );
            }

            self.run_redis_command("RENAME", |command| {
                command.arg(temporary_key).arg(&request.new_key);
            })
            .await?;

            if was_renamed {
                self.run_redis_command("DEL", |command| {
                    command.arg(&request.original_key);
                })
                .await?;
            }

            Ok(())
        }
        .await;

        if let Err(error) = save_result {
            if let Err(cleanup_error) = self
                .run_redis_command("DEL", |command| {
                    command.arg(temporary_key);
                })
                .await
            {
                tracing::warn!(
                    key = temporary_key,
                    error = %cleanup_error,
                    "failed to clean up temporary Redis key after save failure"
                );
            }
            return Err(error);
        }

        Ok(KeyValueSaveOutcome { was_renamed })
    }

    async fn update_key_cell(&self, request: KeyValueCellUpdateRequest) -> Result<()> {
        let new_value = request
            .new_value
            .as_ref()
            .map(|value| value.display_for_editor())
            .unwrap_or_default();
        match request.kind {
            KeyValueKind::String | KeyValueKind::Json => {
                self.run_redis_command("SET", |command| {
                    command.arg(&request.key).arg(&new_value);
                })
                .await?;
            }
            KeyValueKind::Hash => {
                let field = request
                    .row_values
                    .first()
                    .and_then(Value::as_str)
                    .ok_or_else(|| ZqlzError::Driver("No hash field selected".to_string()))?;
                self.run_redis_command("HSET", |command| {
                    command.arg(&request.key).arg(field).arg(new_value);
                })
                .await?;
            }
            KeyValueKind::List => {
                let index = request
                    .row_values
                    .first()
                    .and_then(Value::as_str)
                    .ok_or_else(|| ZqlzError::Driver("No list index selected".to_string()))?;
                self.run_redis_command("LSET", |command| {
                    command.arg(&request.key).arg(index).arg(new_value);
                })
                .await?;
            }
            KeyValueKind::ZSet if request.column_name == "score" => {
                let member = request
                    .row_values
                    .first()
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ZqlzError::Driver("No sorted-set member selected".to_string())
                    })?;
                self.run_redis_command("ZADD", |command| {
                    command.arg(&request.key).arg(new_value).arg(member);
                })
                .await?;
            }
            KeyValueKind::Set => {
                return Err(ZqlzError::Driver(
                    "Set members cannot be updated directly".to_string(),
                ));
            }
            other => {
                return Err(ZqlzError::Driver(format!(
                    "Update not supported for key type {}",
                    other.as_str()
                )));
            }
        }

        Ok(())
    }

    async fn delete_keys(&self, request: KeyValueDeleteRequest) -> Result<KeyValueDeleteOutcome> {
        let mut outcome = KeyValueDeleteOutcome::default();
        for key_name in request.key_names {
            match self
                .run_redis_command("DEL", |command| {
                    command.arg(&key_name);
                })
                .await
            {
                Ok(_) => outcome.deleted_key_names.push(key_name),
                Err(error) => {
                    outcome.errors.push(format!("'{}': {}", key_name, error));
                    if !request.continue_on_error {
                        return Ok(outcome);
                    }
                }
            }
        }
        Ok(outcome)
    }
}

impl std::fmt::Debug for RedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConnection")
            .field("database", &self.database())
            .field("closed", &self.closed.load(Ordering::SeqCst))
            .finish()
    }
}

