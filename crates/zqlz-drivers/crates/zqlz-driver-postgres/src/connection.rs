//! PostgreSQL connection implementation

use async_trait::async_trait;
use bytes::BytesMut;
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::fs;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use tokio_postgres::{
    types::{FromSql, ToSql},
    CancelToken, Client, NoTls, Row as PgRow,
};
use zqlz_core::{
    CellUpdateRequest, ColumnMeta, Connection, QueryCancelHandle, QueryResult, Result, Row,
    RowIdentifier, SchemaIntrospection, StatementResult, Transaction, Value, ZqlzError,
};

/// Global Tokio runtime for PostgreSQL operations.
///
/// tokio-postgres requires a Tokio runtime for DNS resolution and networking.
/// GPUI uses its own async runtime, so we provide a dedicated Tokio runtime
/// for PostgreSQL operations.
fn get_postgres_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("zqlz-postgres-runtime")
            .build()
            .expect("Failed to create Tokio runtime for PostgreSQL driver")
    })
}

/// Cancel handle for PostgreSQL queries.
///
/// This wraps the tokio-postgres `CancelToken` and can be called from any thread
/// to send a cancel request to the PostgreSQL server.
pub struct PostgresCancelHandle {
    cancel_token: CancelToken,
}

impl QueryCancelHandle for PostgresCancelHandle {
    fn cancel(&self) {
        tracing::debug!("Sending cancel request to PostgreSQL server");
        let cancel_token = self.cancel_token.clone();
        // Spawn a task on the dedicated PostgreSQL runtime to send the cancel request
        // Use NoTls for cancellation as it's a simple operation
        get_postgres_runtime().spawn(async move {
            if let Err(e) = cancel_token.cancel_query(NoTls).await {
                tracing::warn!(error = %e, "Failed to cancel PostgreSQL query");
            } else {
                tracing::debug!("PostgreSQL cancel request sent successfully");
            }
        });
    }
}

fn format_postgres_error(error: &tokio_postgres::Error) -> String {
    let Some(db_error) = error.as_db_error() else {
        return error.to_string();
    };

    let code = db_error.code();
    let mut message = db_error.message().to_string();

    if let Some(detail) = db_error.detail() {
        if !detail.trim().is_empty() {
            message.push_str(&format!(" (detail: {})", detail));
        }
    }

    if let Some(hint) = db_error.hint() {
        if !hint.trim().is_empty() {
            message.push_str(&format!(" (hint: {})", hint));
        }
    }

    if let Some(column) = db_error.column() {
        if !column.trim().is_empty() {
            message.push_str(&format!(" (column: {})", column));
        }
    }

    match code.code() {
        "23505" => format!("duplicate value violates unique constraint: {}", message),
        "23503" => format!("foreign key violation: {}", message),
        "23502" => format!("null value violates not-null constraint: {}", message),
        "22007" => format!("invalid datetime format: {}", message),
        "22P02" => format!("invalid input syntax: {}", message),
        _ => format!("{} (code: {:?})", message, code),
    }
}

/// PostgreSQL connection wrapper
pub struct PostgresConnection {
    client: Arc<Mutex<Client>>,
    cancel_token: CancelToken,
}

impl PostgresConnection {
    /// Connect to a PostgreSQL database
    pub async fn connect(
        host: &str,
        port: u16,
        database: &str,
        user: Option<&str>,
        password: Option<&str>,
        ssl_mode: &str,
        ssl_ca_cert: Option<&str>,
        ssl_client_cert: Option<&str>,
        ssl_client_key: Option<&str>,
    ) -> Result<Self> {
        tracing::info!(
            host = %host, 
            port = %port, 
            database = %database, 
            ssl_mode = %ssl_mode,
            "connecting to PostgreSQL database"
        );

        // Build connection config
        let mut config = tokio_postgres::Config::new();
        config.host(host).port(port).dbname(database);

        if let Some(u) = user {
            config.user(u);
        }
        if let Some(p) = password {
            config.password(p);
        }

        // Configure SSL mode based on the provided mode
        let ssl_mode_enum = match ssl_mode.to_lowercase().as_str() {
            "disable" => tokio_postgres::config::SslMode::Disable,
            "allow" => tokio_postgres::config::SslMode::Prefer,
            "prefer" => tokio_postgres::config::SslMode::Prefer,
            "require" => tokio_postgres::config::SslMode::Require,
            "verify-ca" | "verify_ca" => tokio_postgres::config::SslMode::Require,
            "verify-full" | "verify_full" => tokio_postgres::config::SslMode::Require,
            _ => tokio_postgres::config::SslMode::Prefer,
        };
        config.ssl_mode(ssl_mode_enum);

        // Get the dedicated PostgreSQL runtime
        let runtime = get_postgres_runtime();

        // Determine whether to use TLS or NoTls based on ssl_mode
        let use_tls = ssl_mode != "disable";
        
        let (client, cancel_token) = if use_tls {
            // Build TLS connector
            let mut tls_builder = TlsConnector::builder();
            
            // Load CA certificate if provided
            if let Some(ca_cert_path) = ssl_ca_cert {
                if !ca_cert_path.is_empty() {
                    let ca_cert_data = fs::read(ca_cert_path)
                        .map_err(|e| ZqlzError::Connection(format!("Failed to read CA certificate: {}", e)))?;
                    let ca_cert = Certificate::from_pem(&ca_cert_data)
                        .map_err(|e| ZqlzError::Connection(format!("Failed to parse CA certificate: {}", e)))?;
                    tls_builder.add_root_certificate(ca_cert);
                }
            }
            
            // Load client certificate and key if provided
            if let (Some(client_cert_path), Some(client_key_path)) = (ssl_client_cert, ssl_client_key) {
                if !client_cert_path.is_empty() && !client_key_path.is_empty() {
                    let client_cert_data = fs::read(client_cert_path)
                        .map_err(|e| ZqlzError::Connection(format!("Failed to read client certificate: {}", e)))?;
                    let client_key_data = fs::read(client_key_path)
                        .map_err(|e| ZqlzError::Connection(format!("Failed to read client key: {}", e)))?;
                    
                    // Combine cert and key into PKCS12 identity
                    let identity = Identity::from_pkcs8(&client_cert_data, &client_key_data)
                        .map_err(|e| ZqlzError::Connection(format!("Failed to create identity from certificate and key: {}", e)))?;
                    tls_builder.identity(identity);
                }
            }
            
            // For verify-full mode, enable hostname verification
            let danger_accept_invalid = match ssl_mode.to_lowercase().as_str() {
                "require" => true,
                "verify-ca" | "verify_ca" => true,
                _ => false,
            };
            tls_builder.danger_accept_invalid_hostnames(danger_accept_invalid);
            
            // For require mode without CA cert, accept invalid certs
            let danger_accept_invalid_certs = match ssl_mode.to_lowercase().as_str() {
                "require" if ssl_ca_cert.is_none() => true,
                _ => false,
            };
            tls_builder.danger_accept_invalid_certs(danger_accept_invalid_certs);
            
            let tls_connector = tls_builder
                .build()
                .map_err(|e| ZqlzError::Connection(format!("Failed to build TLS connector: {}", e)))?;
            let tls = MakeTlsConnector::new(tls_connector);
            
            let (client, connection) = runtime
                .spawn({
                    let config = config.clone();
                    async move { config.connect(tls).await }
                })
                .await
                .map_err(|e| ZqlzError::Connection(format!("PostgreSQL connection task failed: {}", e)))?
                .map_err(|e| ZqlzError::Connection(format!("Failed to connect to PostgreSQL: {}", e)))?;
            
            let cancel_token = client.cancel_token();
            
            // Spawn connection task
            runtime.spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!(error = %e, "PostgreSQL connection error");
                }
            });
            
            (client, cancel_token)
        } else {
            // No TLS
            let (client, connection) = runtime
                .spawn({
                    let config = config.clone();
                    async move { config.connect(NoTls).await }
                })
                .await
                .map_err(|e| ZqlzError::Connection(format!("PostgreSQL connection task failed: {}", e)))?
                .map_err(|e| ZqlzError::Connection(format!("Failed to connect to PostgreSQL: {}", e)))?;
            
            let cancel_token = client.cancel_token();
            
            // Spawn connection task
            runtime.spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!(error = %e, "PostgreSQL connection error");
                }
            });
            
            (client, cancel_token)
        };

        tracing::info!(
            host = %host, 
            port = %port, 
            database = %database, 
            ssl_mode = %ssl_mode,
            "PostgreSQL connection established"
        );
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            cancel_token,
        })
    }
}

/// Escape a value for SQL literal inclusion (for PostgreSQL)
fn value_to_pg_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(v) => if *v { "TRUE" } else { "FALSE" }.to_string(),
        Value::Int8(v) => v.to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float32(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::String(v) => format!("'{}'", v.replace("'", "''")),
        Value::Bytes(v) => {
            // Convert bytes to hex string for PostgreSQL bytea
            let hex: String = v.iter().map(|b| format!("{:02x}", b)).collect();
            format!("E'\\\\x{}'", hex)
        }
        Value::Uuid(v) => format!("'{}'", v),
        Value::Json(v) => format!("'{}'", v.to_string().replace("'", "''")),
        Value::DateTimeUtc(v) => format!("'{}'", v.to_rfc3339()),
        Value::Date(v) => format!("'{}'", v),
        Value::Time(v) => format!("'{}'", v),
        Value::DateTime(v) => format!("'{}'", v),
        Value::Decimal(v) => v.to_string(),
        Value::Array(arr) => {
            // Format as PostgreSQL array literal: ARRAY[val1, val2, ...]
            let values: Vec<String> = arr.iter().map(value_to_pg_literal).collect();
            format!("ARRAY[{}]", values.join(", "))
        }
    }
}

/// Wrapper enum for converting zqlz_core::Value to types implementing ToSql.
/// This is needed because tokio-postgres requires owned values that implement ToSql.
#[derive(Debug)]
enum PgValue {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Json(serde_json::Value),
    DateTimeUtc(chrono::DateTime<chrono::Utc>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    DateTime(chrono::NaiveDateTime),
}

#[derive(Debug)]
struct PgNumericString(String);
#[derive(Debug)]
struct PgFallbackString(String);

impl PgNumericString {
    fn parse(raw: &[u8]) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() < 8 {
            return Err("invalid NUMERIC payload: too short".into());
        }

        let ndigits = i16::from_be_bytes([raw[0], raw[1]]) as usize;
        let weight = i16::from_be_bytes([raw[2], raw[3]]);
        let sign = u16::from_be_bytes([raw[4], raw[5]]);
        let dscale = i16::from_be_bytes([raw[6], raw[7]]) as usize;
        let expected_len = 8 + ndigits * 2;

        if raw.len() < expected_len {
            return Err("invalid NUMERIC payload: truncated digits".into());
        }

        if sign == 0xC000 {
            return Ok("NaN".to_string());
        }

        let mut digits = Vec::with_capacity(ndigits);
        for index in 0..ndigits {
            let offset = 8 + index * 2;
            let group = u16::from_be_bytes([raw[offset], raw[offset + 1]]);
            if group > 9999 {
                return Err("invalid NUMERIC payload: group out of range".into());
            }
            digits.push(group);
        }

        if digits.is_empty() {
            return Ok("0".to_string());
        }

        let integer_group_count = if weight >= 0 {
            (weight as usize) + 1
        } else {
            0
        };

        let mut integer_text = String::new();
        if integer_group_count == 0 {
            integer_text.push('0');
        } else {
            for group_index in 0..integer_group_count {
                let group = digits.get(group_index).copied().unwrap_or(0);
                if group_index == 0 {
                    integer_text.push_str(&group.to_string());
                } else {
                    integer_text.push_str(&format!("{group:04}"));
                }
            }
        }

        let mut fraction_text = String::new();
        if dscale > 0 {
            let start = integer_group_count.min(digits.len());
            for group in digits.iter().skip(start) {
                fraction_text.push_str(&format!("{group:04}"));
            }

            if fraction_text.len() < dscale {
                fraction_text.push_str(&"0".repeat(dscale - fraction_text.len()));
            } else {
                fraction_text.truncate(dscale);
            }

            while fraction_text.ends_with('0') {
                fraction_text.pop();
            }
        }

        let mut output = String::new();
        if sign == 0x4000 && integer_text != "0" {
            output.push('-');
        }
        output.push_str(&integer_text);
        if !fraction_text.is_empty() {
            output.push('.');
            output.push_str(&fraction_text);
        }

        Ok(output)
    }
}

impl<'a> FromSql<'a> for PgNumericString {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(Self::parse(raw)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::NUMERIC
    }
}

impl<'a> FromSql<'a> for PgFallbackString {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let text = String::from_utf8(raw.to_vec())?;
        Ok(Self(text))
    }

    fn accepts(_: &tokio_postgres::types::Type) -> bool {
        true
    }
}

impl PgValue {
    /// Convert a zqlz_core::Value into a PgValue that matches the target
    /// PostgreSQL column type. This ensures tokio-postgres writes the correct
    /// binary width (e.g. 4 bytes for INT4, not 8 bytes from an i64).
    fn from_value_for_type(value: &Value, target_type: &tokio_postgres::types::Type) -> Self {
        use tokio_postgres::types::Type;

        match value {
            Value::Null => PgValue::Null,
            Value::Bool(v) => PgValue::Bool(*v),

            Value::Int8(v) => Self::coerce_int(*v as i64, target_type),
            Value::Int16(v) => Self::coerce_int(*v as i64, target_type),
            Value::Int32(v) => Self::coerce_int(*v as i64, target_type),
            Value::Int64(v) => Self::coerce_int(*v, target_type),

            Value::Float32(v) => match *target_type {
                Type::FLOAT8 => PgValue::Float64(*v as f64),
                _ => PgValue::Float32(*v),
            },
            Value::Float64(v) => match *target_type {
                Type::FLOAT4 => PgValue::Float32(*v as f32),
                _ => PgValue::Float64(*v),
            },

            Value::Decimal(v) => PgValue::String(v.clone()),
            Value::String(v) => Self::coerce_string(v, target_type),
            Value::Bytes(v) => PgValue::Bytes(v.clone()),
            Value::Uuid(v) => PgValue::Uuid(*v),
            Value::Json(v) => PgValue::Json(v.clone()),
            Value::DateTimeUtc(v) => PgValue::DateTimeUtc(*v),
            Value::Date(v) => PgValue::Date(*v),
            Value::Time(v) => PgValue::Time(*v),
            Value::DateTime(v) => PgValue::DateTime(*v),
            Value::Array(_) => PgValue::String(value.to_string()),
        }
    }

    /// Pick the PgValue integer variant that matches the target column type
    /// so tokio-postgres writes the correct number of bytes.
    fn coerce_int(value: i64, target_type: &tokio_postgres::types::Type) -> Self {
        use tokio_postgres::types::Type;
        match *target_type {
            Type::INT2 => PgValue::Int16(value as i16),
            Type::INT4 => PgValue::Int32(value as i32),
            Type::INT8 => PgValue::Int64(value),
            _ => PgValue::Int64(value),
        }
    }

    /// Coerce string literals into strongly typed PostgreSQL parameter values
    /// when the prepared statement provides a concrete target type.
    fn coerce_string(value: &str, target_type: &tokio_postgres::types::Type) -> Self {
        use tokio_postgres::types::Type;

        match *target_type {
            Type::JSON | Type::JSONB => serde_json::from_str::<serde_json::Value>(value)
                .map(PgValue::Json)
                .unwrap_or_else(|_| PgValue::String(value.to_string())),
            Type::DATE => chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map(PgValue::Date)
                .unwrap_or_else(|_| PgValue::String(value.to_string())),
            Type::TIME => chrono::NaiveTime::parse_from_str(value, "%H:%M:%S")
                .or_else(|_| chrono::NaiveTime::parse_from_str(value, "%H:%M:%S%.f"))
                .map(PgValue::Time)
                .unwrap_or_else(|_| PgValue::String(value.to_string())),
            Type::TIMESTAMP => {
                let parsed = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .or_else(|| {
                        chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").ok()
                    })
                    .or_else(|| {
                        chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                            .ok()
                            .and_then(|date| chrono::NaiveTime::from_hms_opt(0, 0, 0).map(|time| date.and_time(time)))
                    });
                parsed
                    .map(PgValue::DateTime)
                    .unwrap_or_else(|| PgValue::String(value.to_string()))
            }
            Type::TIMESTAMPTZ => {
                let parsed = chrono::DateTime::parse_from_rfc3339(value)
                    .ok()
                    .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
                    .or_else(|| {
                        chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                            .ok()
                            .or_else(|| {
                                chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                                    .ok()
                            })
                            .map(|timestamp| {
                                chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(timestamp, chrono::Utc)
                            })
                    })
                    .or_else(|| {
                        chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                            .ok()
                            .and_then(|date| {
                                chrono::NaiveTime::from_hms_opt(0, 0, 0).map(|time| {
                                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                                        date.and_time(time),
                                        chrono::Utc,
                                    )
                                })
                            })
                    });
                parsed
                    .map(PgValue::DateTimeUtc)
                    .unwrap_or_else(|| PgValue::String(value.to_string()))
            }
            _ => PgValue::String(value.to_string()),
        }
    }

    /// Fallback used when we don't know the target column type (e.g. raw queries).
    fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => PgValue::Null,
            Value::Bool(v) => PgValue::Bool(*v),
            Value::Int8(v) => PgValue::Int16(*v as i16),
            Value::Int16(v) => PgValue::Int16(*v),
            Value::Int32(v) => PgValue::Int32(*v),
            Value::Int64(v) => PgValue::Int64(*v),
            Value::Float32(v) => PgValue::Float32(*v),
            Value::Float64(v) => PgValue::Float64(*v),
            Value::Decimal(v) => PgValue::String(v.clone()),
            Value::String(v) => PgValue::String(v.clone()),
            Value::Bytes(v) => PgValue::Bytes(v.clone()),
            Value::Uuid(v) => PgValue::Uuid(*v),
            Value::Json(v) => PgValue::Json(v.clone()),
            Value::DateTimeUtc(v) => PgValue::DateTimeUtc(*v),
            Value::Date(v) => PgValue::Date(*v),
            Value::Time(v) => PgValue::Time(*v),
            Value::DateTime(v) => PgValue::DateTime(*v),
            Value::Array(_) => PgValue::String(value.to_string()),
        }
    }
}

impl ToSql for PgValue {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut BytesMut,
    ) -> std::result::Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            PgValue::Null => Ok(postgres_types::IsNull::Yes),
            PgValue::Bool(v) => v.to_sql(ty, out),
            PgValue::Int16(v) => v.to_sql(ty, out),
            PgValue::Int32(v) => v.to_sql(ty, out),
            PgValue::Int64(v) => v.to_sql(ty, out),
            PgValue::Float32(v) => v.to_sql(ty, out),
            PgValue::Float64(v) => v.to_sql(ty, out),
            PgValue::String(v) => v.to_sql(ty, out),
            PgValue::Bytes(v) => v.to_sql(ty, out),
            PgValue::Uuid(v) => v.to_sql(ty, out),
            PgValue::Json(v) => v.to_sql(ty, out),
            PgValue::DateTimeUtc(v) => v.to_sql(ty, out),
            PgValue::Date(v) => v.to_sql(ty, out),
            PgValue::Time(v) => v.to_sql(ty, out),
            PgValue::DateTime(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_: &tokio_postgres::types::Type) -> bool {
        true
    }

    postgres_types::to_sql_checked!();
}

/// PostgreSQL transaction wrapper
///
/// This transaction holds an exclusive lock on the PostgreSQL client for the entire
/// duration of the transaction. This ensures that all operations within the transaction
/// execute in the correct order and that no other operations can interfere.
pub struct PostgresTransaction {
    client: Arc<Mutex<Client>>,
    committed: bool,
    rolled_back: bool,
}

impl Drop for PostgresTransaction {
    fn drop(&mut self) {
        // If transaction is neither committed nor rolled back, automatically roll back
        if !self.committed && !self.rolled_back {
            tracing::warn!("PostgreSQL transaction dropped without commit or rollback, auto-rolling back");
            // We can't async rollback in Drop, but the BEGIN will auto-rollback when connection is reused
        }
    }
}

#[async_trait]
impl Transaction for PostgresTransaction {
    async fn commit(mut self: Box<Self>) -> Result<()> {
        tracing::debug!("committing PostgreSQL transaction");
        
        if self.rolled_back {
            return Err(ZqlzError::Query("Transaction already rolled back".into()));
        }
        
        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }
        
        let client = self.client.lock().await;
        client.execute("COMMIT", &[]).await.map_err(|e| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to commit transaction: {}", message))
        })?;
        
        self.committed = true;
        tracing::debug!("PostgreSQL transaction committed successfully");
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        tracing::debug!("rolling back PostgreSQL transaction");
        
        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }
        
        if self.rolled_back {
            return Ok(()); // Already rolled back, that's fine
        }
        
        let client = self.client.lock().await;
        client.execute("ROLLBACK", &[]).await.map_err(|e| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to rollback transaction: {}", message))
        })?;
        
        self.rolled_back = true;
        tracing::debug!("PostgreSQL transaction rolled back successfully");
        Ok(())
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing query in transaction");
        
        let start_time = std::time::Instant::now();
        let client = self.client.lock().await;

        // Prepare first so we know the target column types for each parameter
        let statement = client.prepare(sql).await.map_err(|e| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to prepare query: {}", message))
        })?;

        let param_types = statement.params();
        let pg_params: Vec<PgValue> = params
            .iter()
            .enumerate()
            .map(|(i, value)| {
                if let Some(target_type) = param_types.get(i) {
                    PgValue::from_value_for_type(value, target_type)
                } else {
                    PgValue::from_value(value)
                }
            })
            .collect();
        let param_refs: Vec<&(dyn ToSql + Sync)> =
            pg_params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

        let pg_rows = client
            .query(&statement, &param_refs)
            .await
            .map_err(|e| {
                let message = format_postgres_error(&e);
                ZqlzError::Query(format!("Failed to execute query: {}", message))
            })?;

        // Get column metadata from prepared statement so empty result sets still include columns.
        let mut columns = Vec::new();
        let mut column_names = Vec::new();
        for (idx, col) in statement.columns().iter().enumerate() {
            let name = col.name().to_string();
            column_names.push(name.clone());
            columns.push(ColumnMeta {
                name,
                data_type: format!("{:?}", col.type_()),
                nullable: true,
                ordinal: idx,
                max_length: None,
                precision: None,
                scale: None,
                auto_increment: false,
                default_value: None,
                comment: None,
                enum_values: None,
            });
        }

        // Convert rows
        let mut rows = Vec::new();
        for pg_row in &pg_rows {
            let mut values = Vec::new();
            for idx in 0..columns.len() {
                let value = postgres_to_value(pg_row, idx)?;
                values.push(value);
            }
            rows.push(Row::new(column_names.clone(), values));
        }

        let execution_time_ms = start_time.elapsed().as_millis() as u64;
        let total_rows = rows.len();

        Ok(QueryResult {
            id: uuid::Uuid::new_v4(),
            columns,
            rows,
            total_rows: Some(total_rows as u64),
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms,
            warnings: Vec::new(),
        })
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing statement in transaction");
        
        let client = self.client.lock().await;

        // Prepare first so we know the target column types for each parameter
        let statement = client.prepare(sql).await.map_err(|e| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to prepare statement: {}", message))
        })?;

        let param_types = statement.params();
        let pg_params: Vec<PgValue> = params
            .iter()
            .enumerate()
            .map(|(i, value)| {
                if let Some(target_type) = param_types.get(i) {
                    PgValue::from_value_for_type(value, target_type)
                } else {
                    PgValue::from_value(value)
                }
            })
            .collect();
        let param_refs: Vec<&(dyn ToSql + Sync)> =
            pg_params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

        let rows_affected = client
            .execute(&statement, &param_refs)
            .await
            .map_err(|e| {
                let message = format_postgres_error(&e);
                ZqlzError::Query(format!("Failed to execute statement: {}", message))
            })?;

        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows: rows_affected,
            error: None,
        })
    }
}

#[async_trait]
impl Connection for PostgresConnection {
    fn driver_name(&self) -> &str {
        "postgresql"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("postgresql")
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        let client = self.client.lock().await;

        // Prepare first so we know the target column types for each parameter
        let statement = client.prepare(sql).await.map_err(|e| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to prepare statement: {}", message))
        })?;

        let param_types = statement.params();
        let pg_params: Vec<PgValue> = params
            .iter()
            .enumerate()
            .map(|(i, value)| {
                if let Some(target_type) = param_types.get(i) {
                    PgValue::from_value_for_type(value, target_type)
                } else {
                    PgValue::from_value(value)
                }
            })
            .collect();
        let param_refs: Vec<&(dyn ToSql + Sync)> =
            pg_params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

        let rows_affected = client
            .execute(&statement, &param_refs)
            .await
            .map_err(|e| {
                let message = format_postgres_error(&e);
                ZqlzError::Query(format!("Failed to execute statement: {}", message))
            })?;

        tracing::debug!(affected_rows = rows_affected, "statement executed");
        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows: rows_affected,
            error: None,
        })
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let start_time = std::time::Instant::now();

        let client = self.client.lock().await;

        // Prepare first so we know the target column types for each parameter
        let statement = client.prepare(sql).await.map_err(|e| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to prepare query: {}", message))
        })?;

        let param_types = statement.params();
        let pg_params: Vec<PgValue> = params
            .iter()
            .enumerate()
            .map(|(i, value)| {
                if let Some(target_type) = param_types.get(i) {
                    PgValue::from_value_for_type(value, target_type)
                } else {
                    PgValue::from_value(value)
                }
            })
            .collect();
        let param_refs: Vec<&(dyn ToSql + Sync)> =
            pg_params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

        let pg_rows = client
            .query(&statement, &param_refs)
            .await
            .map_err(|e| {
                let message = format_postgres_error(&e);
                ZqlzError::Query(format!("Failed to execute query: {}", message))
            })?;

        // Get column metadata from prepared statement so empty result sets still include columns.
        let mut columns = Vec::new();
        let mut column_names = Vec::new();
        for (idx, col) in statement.columns().iter().enumerate() {
            let name = col.name().to_string();
            column_names.push(name.clone());
            columns.push(ColumnMeta {
                name,
                data_type: format!("{:?}", col.type_()),
                nullable: true, // PostgreSQL doesn't provide this info easily
                ordinal: idx,
                max_length: None,
                precision: None,
                scale: None,
                auto_increment: false,
                default_value: None,
                comment: None,
                enum_values: None,
            });
        }

        // Convert rows
        let mut rows = Vec::new();
        for pg_row in &pg_rows {
            let mut values = Vec::new();
            for idx in 0..columns.len() {
                let value = postgres_to_value(pg_row, idx)?;
                values.push(value);
            }
            rows.push(Row::new(column_names.clone(), values));
        }

        let execution_time_ms = start_time.elapsed().as_millis() as u64;
        let total_rows = rows.len();

        tracing::debug!(
            row_count = total_rows,
            execution_time_ms = execution_time_ms,
            "query executed successfully"
        );

        Ok(QueryResult {
            id: uuid::Uuid::new_v4(),
            columns,
            rows,
            total_rows: Some(total_rows as u64),
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms,
            warnings: Vec::new(),
        })
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        tracing::debug!("beginning PostgreSQL transaction");
        
        let client = self.client.lock().await;
        client.execute("BEGIN", &[]).await.map_err(|e| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to begin transaction: {}", message))
        })?;
        
        // Release the lock and return the transaction
        drop(client);
        
        tracing::debug!("PostgreSQL transaction begun successfully");
        Ok(Box::new(PostgresTransaction {
            client: Arc::clone(&self.client),
            committed: false,
            rolled_back: false,
        }))
    }

    async fn close(&self) -> Result<()> {
        tracing::info!("closing PostgreSQL connection");
        Ok(())
    }

    fn is_closed(&self) -> bool {
        false
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }

    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        Some(Arc::new(PostgresCancelHandle {
            cancel_token: self.cancel_token.clone(),
        }))
    }

    /// Override update_cell to use SQL literals instead of parameters
    /// since parameterized queries aren't fully implemented yet
    async fn update_cell(&self, request: CellUpdateRequest) -> Result<u64> {
        tracing::debug!(
            table = %request.table_name,
            column = %request.column_name,
            "updating cell value (PostgreSQL)"
        );

        // Escape table name (may include schema.table format)
        let table_identifier = escape_table_name_pg(&request.table_name);

        // Build WHERE clause with literal values
        let where_clause = match &request.row_identifier {
            RowIdentifier::RowIndex(_) => {
                return Err(ZqlzError::NotSupported(
                    "Row index-based updates not supported. Use primary key or full row identifier.".to_string()
                ));
            }
            RowIdentifier::PrimaryKey(pk_values) => pk_values
                .iter()
                .map(|(col, val)| {
                    format!(
                        "{} = {}",
                        escape_identifier_pg(col),
                        value_to_pg_literal(val)
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND "),
            RowIdentifier::FullRow(row_values) => row_values
                .iter()
                .map(|(col, val)| {
                    if val == &Value::Null {
                        format!("{} IS NULL", escape_identifier_pg(col))
                    } else {
                        format!(
                            "{} = {}",
                            escape_identifier_pg(col),
                            value_to_pg_literal(val)
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(" AND "),
        };

        // Build UPDATE statement with literal value
        let set_value = match &request.new_value {
            Some(val) => value_to_pg_literal(val),
            None => "NULL".to_string(),
        };

        let sql = format!(
            "UPDATE {} SET {} = {} WHERE {}",
            table_identifier,
            escape_identifier_pg(&request.column_name),
            set_value,
            where_clause
        );

        tracing::debug!("PostgreSQL update SQL: {}", sql);

        let client = self.client.lock().await;
        let rows_affected = client
            .execute(&sql, &[])
            .await
            .map_err(|e| {
                tracing::error!("PostgreSQL cell update error: {:?}", e);
                tracing::error!("Error details: {}", e);
                if let Some(db_error) = e.as_db_error() {
                    tracing::error!(
                        "Database error details - Code: {:?}, Message: {}, Detail: {:?}, Hint: {:?}",
                        db_error.code(),
                        db_error.message(),
                        db_error.detail(),
                        db_error.hint()
                    );
                }
                ZqlzError::Query(format!("Failed to update cell: {}", e))
            })?;

        tracing::debug!(affected_rows = rows_affected, "cell update completed");
        Ok(rows_affected)
    }
}

/// Convert PostgreSQL row value to our Value type
fn postgres_to_value(row: &PgRow, idx: usize) -> Result<Value> {
    let col = &row.columns()[idx];
    let type_name = col.type_().name();

    // Try to extract value based on type
    let value = match type_name {
        "bool" => row
            .try_get::<_, Option<bool>>(idx)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        "int2" | "smallint" => row
            .try_get::<_, Option<i16>>(idx)
            .ok()
            .flatten()
            .map(Value::Int16)
            .unwrap_or(Value::Null),
        "int4" | "int" | "integer" => row
            .try_get::<_, Option<i32>>(idx)
            .ok()
            .flatten()
            .map(Value::Int32)
            .unwrap_or(Value::Null),
        "int8" | "bigint" => row
            .try_get::<_, Option<i64>>(idx)
            .ok()
            .flatten()
            .map(Value::Int64)
            .unwrap_or(Value::Null),
        "float4" | "real" => row
            .try_get::<_, Option<f32>>(idx)
            .ok()
            .flatten()
            .map(Value::Float32)
            .unwrap_or(Value::Null),
        "float8" | "double precision" => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .map(Value::Float64)
            .unwrap_or(Value::Null),
        "text" | "varchar" | "char" | "bpchar" | "name" => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .map(Value::String)
            .unwrap_or(Value::Null),
        "bytea" => row
            .try_get::<_, Option<Vec<u8>>>(idx)
            .ok()
            .flatten()
            .map(Value::Bytes)
            .unwrap_or(Value::Null),
        "uuid" => row
            .try_get::<_, Option<uuid::Uuid>>(idx)
            .ok()
            .flatten()
            .map(Value::Uuid)
            .unwrap_or(Value::Null),
        "json" | "jsonb" => row
            .try_get::<_, Option<serde_json::Value>>(idx)
            .ok()
            .flatten()
            .map(Value::Json)
            .unwrap_or(Value::Null),
        "date" => row
            .try_get::<_, Option<chrono::NaiveDate>>(idx)
            .ok()
            .flatten()
            .map(Value::Date)
            .unwrap_or(Value::Null),
        "time" => row
            .try_get::<_, Option<chrono::NaiveTime>>(idx)
            .ok()
            .flatten()
            .map(Value::Time)
            .unwrap_or(Value::Null),
        "timestamp" => row
            .try_get::<_, Option<chrono::NaiveDateTime>>(idx)
            .ok()
            .flatten()
            .map(Value::DateTime)
            .unwrap_or(Value::Null),
        "timestamptz" => row
            .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx)
            .ok()
            .flatten()
            .map(Value::DateTimeUtc)
            .unwrap_or(Value::Null),
        "numeric" | "decimal" => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .map(Value::Float64)
            .or_else(|| {
                row.try_get::<_, Option<PgNumericString>>(idx)
                    .ok()
                    .flatten()
                    .map(|value| Value::Decimal(value.0))
            })
            .unwrap_or(Value::Null),
        // Array types - PostgreSQL prefixes array type names with underscore
        "_text" | "_varchar" | "_bpchar" | "_name" => row
            .try_get::<_, Option<Vec<String>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::String).collect()))
            .unwrap_or(Value::Null),
        "_int2" => row
            .try_get::<_, Option<Vec<i16>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::Int16).collect()))
            .unwrap_or(Value::Null),
        "_int4" => row
            .try_get::<_, Option<Vec<i32>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::Int32).collect()))
            .unwrap_or(Value::Null),
        "_int8" => row
            .try_get::<_, Option<Vec<i64>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::Int64).collect()))
            .unwrap_or(Value::Null),
        _ => {
            // Fallback for custom PostgreSQL types (e.g., enums): decode raw UTF-8 payload.
            row.try_get::<_, Option<PgFallbackString>>(idx)
                .ok()
                .flatten()
                .map(|value| Value::String(value.0))
                .unwrap_or(Value::Null)
        }
    };

    Ok(value)
}

/// Escape a PostgreSQL identifier (column name, etc.)
fn escape_identifier_pg(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace("\"", "\"\""))
}

/// Escape a table name which may include schema (e.g., "schema.table")
fn escape_table_name_pg(table_name: &str) -> String {
    if table_name.contains('.') {
        // Handle schema.table format
        let parts: Vec<&str> = table_name.splitn(2, '.').collect();
        if parts.len() == 2 {
            format!(
                "{}.{}",
                escape_identifier_pg(parts[0]),
                escape_identifier_pg(parts[1])
            )
        } else {
            escape_identifier_pg(table_name)
        }
    } else {
        escape_identifier_pg(table_name)
    }
}
