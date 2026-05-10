//! PostgreSQL connection implementation

use async_trait::async_trait;
use bytes::BytesMut;
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::fs;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_postgres::{
    CancelToken, Client, NoTls, Row as PgRow,
    types::{FromSql, ToSql},
};

use crate::PostgresSshTunnel;
use zqlz_core::{
    BindPlaceholderPolicy, CellUpdateRequest, CheckConstraintEnforcement, ColumnMeta, Connection,
    ConnectionScope, DropTableOptions, DropTriggerOptions, DropViewOptions, ExplainConfig,
    ExplainParserKind, ForeignKeyChecksSql, ImportIndexCapabilities, ImportSemanticDefault,
    QueryCancelHandle, QueryResult, ResolvedConnectionScope, Result, Row, RowIdentifier,
    SchemaIntrospection, SqlObjectName, StatementResult, TableType, Transaction, Value, ZqlzError,
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

fn is_postgres_missing_user_mapping_error(error_message: &str) -> bool {
    let normalized = error_message.to_ascii_lowercase();
    normalized.contains("user mapping not found")
}

fn is_postgres_foreign_table_browse_error(error_message: &str) -> bool {
    let normalized = error_message.to_ascii_lowercase();
    is_postgres_missing_user_mapping_error(error_message)
        || normalized.contains("could not connect to server")
        || normalized.contains("connection to server")
        || normalized.contains("foreign-data wrapper")
        || normalized.contains("foreign table")
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

    if let Some(detail) = db_error.detail()
        && !detail.trim().is_empty()
    {
        message.push_str(&format!(" (detail: {})", detail));
    }

    if let Some(hint) = db_error.hint()
        && !hint.trim().is_empty()
    {
        message.push_str(&format!(" (hint: {})", hint));
    }

    if let Some(column) = db_error.column()
        && !column.trim().is_empty()
    {
        message.push_str(&format!(" (column: {})", column));
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

fn format_postgres_cell_update_error(
    error: &tokio_postgres::Error,
    request: &CellUpdateRequest,
) -> String {
    let Some(db_error) = error.as_db_error() else {
        return format_postgres_error(error);
    };

    if db_error.code().code() == "23503" {
        return format_postgres_foreign_key_cell_update_error(
            request,
            db_error.table(),
            db_error.constraint(),
            db_error.detail(),
        );
    }

    format_postgres_error(error)
}

fn format_postgres_foreign_key_cell_update_error(
    request: &CellUpdateRequest,
    referenced_table: Option<&str>,
    constraint: Option<&str>,
    detail: Option<&str>,
) -> String {
    let referenced_table = referenced_table
        .filter(|table| !table.trim().is_empty())
        .unwrap_or("another table");
    let constraint = constraint
        .filter(|constraint| !constraint.trim().is_empty())
        .unwrap_or("a foreign key constraint");

    let mut message = format!(
        "Cannot update {}.{} because {} still references this value via {}. Change the FK to ON UPDATE CASCADE or update referencing rows manually.",
        request.table_name, request.column_name, referenced_table, constraint
    );

    if let Some(detail) = detail.filter(|detail| !detail.trim().is_empty()) {
        message.push_str(&format!(" Detail: {}", detail));
    }

    message
}

/// PostgreSQL connection wrapper
pub struct PostgresConnection {
    client: Arc<Mutex<Client>>,
    cancel_token: CancelToken,
    _ssh_tunnel: Option<PostgresSshTunnel>,
}

#[derive(Debug)]
pub struct PostgresConnectOptions {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub ssl_mode: String,
    pub ssl_ca_cert: Option<String>,
    pub ssl_client_cert: Option<String>,
    pub ssl_client_key: Option<String>,
    pub connect_timeout_seconds: Option<u64>,
    pub application_name: Option<String>,
    pub search_path: Option<String>,
    pub keepalive: bool,
    pub ssh_tunnel: Option<PostgresSshTunnel>,
}

impl PostgresConnection {
    /// Connect to a PostgreSQL database
    pub async fn connect(options: PostgresConnectOptions) -> Result<Self> {
        tracing::info!(
            host = %options.host,
            port = %options.port,
            database = %options.database,
            ssl_mode = %options.ssl_mode,
            "connecting to PostgreSQL database"
        );

        // Build connection config
        let mut config = tokio_postgres::Config::new();
        config
            .host(&options.host)
            .port(options.port)
            .dbname(&options.database)
            .keepalives(options.keepalive);

        if let Some(u) = options.user.as_deref() {
            config.user(u);
        }
        if let Some(p) = options.password.as_deref() {
            config.password(p);
        }
        if let Some(seconds) = options.connect_timeout_seconds {
            config.connect_timeout(Duration::from_secs(seconds));
        }
        if let Some(application_name) = options.application_name.as_deref() {
            config.application_name(application_name);
        }
        if let Some(search_path) = options.search_path.as_deref() {
            config.options(format!("-c search_path={}", search_path));
        }

        // Configure SSL mode based on the provided mode
        let ssl_mode_enum = match options.ssl_mode.to_lowercase().as_str() {
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
        let use_tls = !options.ssl_mode.eq_ignore_ascii_case("disable");

        let (client, cancel_token) = if use_tls {
            // Build TLS connector
            let mut tls_builder = TlsConnector::builder();

            // Load CA certificate if provided
            if let Some(ca_cert_path) = options.ssl_ca_cert.as_deref()
                && !ca_cert_path.is_empty()
            {
                let ca_cert_data = fs::read(ca_cert_path).map_err(|e| {
                    ZqlzError::Connection(format!("Failed to read CA certificate: {}", e))
                })?;
                let ca_cert = Certificate::from_pem(&ca_cert_data).map_err(|e| {
                    ZqlzError::Connection(format!("Failed to parse CA certificate: {}", e))
                })?;
                tls_builder.add_root_certificate(ca_cert);
            }

            // Load client certificate and key if provided
            if let (Some(client_cert_path), Some(client_key_path)) = (
                options.ssl_client_cert.as_deref(),
                options.ssl_client_key.as_deref(),
            ) && !client_cert_path.is_empty()
                && !client_key_path.is_empty()
            {
                let client_cert_data = fs::read(client_cert_path).map_err(|e| {
                    ZqlzError::Connection(format!("Failed to read client certificate: {}", e))
                })?;
                let client_key_data = fs::read(client_key_path).map_err(|e| {
                    ZqlzError::Connection(format!("Failed to read client key: {}", e))
                })?;

                // Combine cert and key into PKCS12 identity
                let identity =
                    Identity::from_pkcs8(&client_cert_data, &client_key_data).map_err(|e| {
                        ZqlzError::Connection(format!(
                            "Failed to create identity from certificate and key: {}",
                            e
                        ))
                    })?;
                tls_builder.identity(identity);
            }

            // For verify-full mode, enable hostname verification
            let danger_accept_invalid = matches!(
                options.ssl_mode.to_lowercase().as_str(),
                "require" | "verify-ca" | "verify_ca"
            );
            tls_builder.danger_accept_invalid_hostnames(danger_accept_invalid);

            // For require mode without CA cert, accept invalid certs
            let danger_accept_invalid_certs =
                options.ssl_mode.to_lowercase() == "require" && options.ssl_ca_cert.is_none();
            tls_builder.danger_accept_invalid_certs(danger_accept_invalid_certs);

            let tls_connector = tls_builder.build().map_err(|e| {
                ZqlzError::Connection(format!("Failed to build TLS connector: {}", e))
            })?;
            let tls = MakeTlsConnector::new(tls_connector);

            let (client, connection) = runtime
                .spawn({
                    let config = config.clone();
                    async move { config.connect(tls).await }
                })
                .await
                .map_err(|e| {
                    ZqlzError::Connection(format!("PostgreSQL connection task failed: {}", e))
                })?
                .map_err(|e| {
                    ZqlzError::Connection(format!("Failed to connect to PostgreSQL: {}", e))
                })?;

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
                .map_err(|e| {
                    ZqlzError::Connection(format!("PostgreSQL connection task failed: {}", e))
                })?
                .map_err(|e| {
                    ZqlzError::Connection(format!("Failed to connect to PostgreSQL: {}", e))
                })?;

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
            host = %options.host,
            port = %options.port,
            database = %options.database,
            ssl_mode = %options.ssl_mode,
            "PostgreSQL connection established"
        );
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            cancel_token,
            _ssh_tunnel: options.ssh_tunnel,
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

fn pg_array_cast_type(column_type: Option<&str>, value: &Value) -> Option<String> {
    if let Some(column_type) = column_type {
        let normalized = Value::normalize_data_type(column_type);
        if let Some(element_type) = Value::array_element_type(&normalized) {
            return Some(format!("{}[]", pg_array_element_cast_type(&element_type)));
        }
    }

    let Value::Array(values) = value else {
        return None;
    };

    values
        .iter()
        .find(|value| !value.is_null())
        .map(pg_array_element_cast_type_for_value)
        .map(|element_type| format!("{}[]", element_type))
}

fn pg_array_element_cast_type(element_type: &str) -> String {
    match element_type {
        "text" | "string" => "text",
        "bool" | "boolean" => "bool",
        "int2" | "smallint" => "int2",
        "int4" | "int" | "integer" => "int4",
        "int8" | "bigint" => "int8",
        "float4" | "real" | "float" => "float4",
        "float8" | "double" | "double precision" => "float8",
        "numeric" | "decimal" => "numeric",
        "money" => "money",
        "bytea" | "binary" | "varbinary" => "bytea",
        "uuid" => "uuid",
        "date" => "date",
        "time" => "time",
        "timetz" | "time with time zone" => "timetz",
        "timestamp" | "datetime" => "timestamp",
        "timestamptz" | "timestamp with time zone" => "timestamptz",
        "json" => "json",
        "jsonb" => "jsonb",
        other => other,
    }
    .to_string()
}

fn pg_array_element_cast_type_for_value(value: &Value) -> String {
    match value {
        Value::Bool(_) => "bool",
        Value::Int8(_) | Value::Int16(_) => "int2",
        Value::Int32(_) => "int4",
        Value::Int64(_) => "int8",
        Value::Float32(_) => "float4",
        Value::Float64(_) => "float8",
        Value::Decimal(_) => "numeric",
        Value::Bytes(_) => "bytea",
        Value::Uuid(_) => "uuid",
        Value::Date(_) => "date",
        Value::Time(_) => "time",
        Value::DateTime(_) => "timestamp",
        Value::DateTimeUtc(_) => "timestamptz",
        Value::Json(_) => "jsonb",
        _ => "text",
    }
    .to_string()
}

fn pg_scalar_cast_type(column_type: Option<&str>) -> Option<String> {
    let column_type = column_type?.trim();
    let normalized = Value::normalize_data_type(column_type);

    if Value::is_array_data_type(&normalized) || Value::is_json_data_type(&normalized) {
        return None;
    }

    let base_type = normalized
        .split_once('(')
        .map_or(normalized.as_str(), |(base, _)| base);
    let is_builtin_without_cast = matches!(
        base_type,
        "text"
            | "varchar"
            | "char"
            | "bpchar"
            | "name"
            | "citext"
            | "character varying"
            | "character"
            | "bool"
            | "int2"
            | "int4"
            | "int8"
            | "float4"
            | "float8"
            | "numeric"
            | "uuid"
            | "date"
            | "time"
            | "timetz"
            | "timestamp"
            | "timestamptz"
            | "bytea"
    );

    if is_builtin_without_cast {
        return None;
    }

    Some(column_type.to_string())
}

fn value_to_pg_literal_for_type(value: &Value, column_type: Option<&str>) -> String {
    match value {
        Value::Array(values) => {
            let values: Vec<String> = values.iter().map(value_to_pg_literal).collect();
            let array_literal = format!("ARRAY[{}]", values.join(", "));
            match pg_array_cast_type(column_type, value) {
                Some(array_type) => format!("{}::{}", array_literal, array_type),
                None => array_literal,
            }
        }
        Value::String(_) => match pg_scalar_cast_type(column_type) {
            Some(column_type) => format!("{}::{}", value_to_pg_literal(value), column_type),
            None => value_to_pg_literal(value),
        },
        _ => value_to_pg_literal(value),
    }
}

fn row_column_type<'a>(column_types: &'a [(String, String)], column_name: &str) -> Option<&'a str> {
    column_types
        .iter()
        .find(|(name, _)| name == column_name)
        .map(|(_, data_type)| data_type.as_str())
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
#[derive(Debug)]
struct PgInetString(String);
#[derive(Debug)]
struct PgTsvectorString(String);
#[derive(Debug)]
struct PgTstzRangeString(String);
#[derive(Debug)]
struct PgBinaryDisplayString(String);
#[derive(Debug)]
struct PgArrayDisplay(Vec<String>);

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

impl<'a> FromSql<'a> for PgInetString {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(format_postgres_network_value(ty.name(), raw)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(ty.name(), "inet" | "cidr")
    }
}

impl<'a> FromSql<'a> for PgTsvectorString {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(format_postgres_tsvector_value(raw)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        ty.name() == "tsvector"
    }
}

impl<'a> FromSql<'a> for PgTstzRangeString {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(format_postgres_tstzrange_value(raw)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        ty.name() == "tstzrange"
    }
}

impl<'a> FromSql<'a> for PgBinaryDisplayString {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(format_postgres_binary_display_value(ty.name(), raw)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        matches!(
            ty.name(),
            "hstore"
                | "macaddr"
                | "macaddr8"
                | "point"
                | "line"
                | "lseg"
                | "box"
                | "path"
                | "polygon"
                | "circle"
                | "money"
                | "interval"
                | "timetz"
                | "bit"
                | "varbit"
                | "oid"
                | "xid"
                | "cid"
                | "regclass"
                | "regtype"
                | "regproc"
                | "regprocedure"
                | "regoper"
                | "regoperator"
                | "regnamespace"
                | "regrole"
                | "pg_lsn"
                | "int4range"
                | "int8range"
                | "numrange"
                | "tsrange"
                | "tstzrange"
                | "daterange"
        )
    }
}

impl<'a> FromSql<'a> for PgArrayDisplay {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(format_postgres_array_values(ty.name(), raw)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        postgres_array_element_type_name(ty.name()).is_some()
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
                            .and_then(|date| {
                                chrono::NaiveTime::from_hms_opt(0, 0, 0)
                                    .map(|time| date.and_time(time))
                            })
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
                                chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                                    timestamp,
                                    chrono::Utc,
                                )
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

fn format_postgres_network_value(
    type_name: &str,
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("invalid network payload: too short".into());
    }

    let family = raw[0];
    let prefix_bits = raw[1];
    let address_length = raw[3] as usize;
    let address = raw
        .get(4..4 + address_length)
        .ok_or("invalid network payload: truncated address")?;

    let (address, full_prefix_bits) = match family {
        2 if address_length == 4 => {
            let octets: [u8; 4] = address.try_into()?;
            (std::net::Ipv4Addr::from(octets).to_string(), 32)
        }
        3 if address_length == 16 => {
            let octets: [u8; 16] = address.try_into()?;
            (std::net::Ipv6Addr::from(octets).to_string(), 128)
        }
        _ => return Err("invalid network payload: unsupported address family".into()),
    };

    if type_name == "cidr" || prefix_bits != full_prefix_bits {
        Ok(format!("{address}/{prefix_bits}"))
    } else {
        Ok(address)
    }
}

fn format_postgres_tsvector_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("invalid tsvector payload: too short".into());
    }

    let lexeme_count = i32::from_be_bytes(raw[0..4].try_into()?);
    if lexeme_count < 0 {
        return Err("invalid tsvector payload: negative lexeme count".into());
    }

    let mut offset = 4;
    let mut lexemes = Vec::with_capacity(lexeme_count as usize);
    for _ in 0..lexeme_count {
        let lexeme_end = raw
            .get(offset..)
            .and_then(|bytes| bytes.iter().position(|byte| *byte == 0))
            .map(|position| offset + position)
            .ok_or("invalid tsvector payload: unterminated lexeme")?;
        let lexeme = std::str::from_utf8(&raw[offset..lexeme_end])?;
        offset = lexeme_end + 1;

        let position_count_bytes = raw
            .get(offset..offset + 2)
            .ok_or("invalid tsvector payload: missing position count")?;
        let position_count = i16::from_be_bytes(position_count_bytes.try_into()?);
        if position_count < 0 {
            return Err("invalid tsvector payload: negative position count".into());
        }
        offset += 2;

        let mut positions = Vec::with_capacity(position_count as usize);
        for _ in 0..position_count {
            let position_bytes = raw
                .get(offset..offset + 2)
                .ok_or("invalid tsvector payload: truncated position")?;
            let encoded = u16::from_be_bytes(position_bytes.try_into()?);
            let position = encoded & 0x3fff;
            let weight = match encoded >> 14 {
                0 => "",
                1 => "C",
                2 => "B",
                3 => "A",
                _ => "",
            };
            positions.push(format!("{position}{weight}"));
            offset += 2;
        }

        let escaped = lexeme.replace('\'', "''");
        if positions.is_empty() {
            lexemes.push(format!("'{escaped}'"));
        } else {
            lexemes.push(format!("'{escaped}':{}", positions.join(",")));
        }
    }

    Ok(lexemes.join(" "))
}

fn format_postgres_tstzrange_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    format_postgres_range_value("tstzrange", raw)
}

fn format_postgres_binary_display_value(
    type_name: &str,
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    match type_name {
        "hstore" => format_postgres_hstore_value(raw),
        "macaddr" => format_postgres_macaddr_value(raw, 6),
        "macaddr8" => format_postgres_macaddr_value(raw, 8),
        "point" => format_postgres_point_value(raw),
        "line" => format_postgres_line_value(raw),
        "lseg" => format_postgres_lseg_value(raw),
        "box" => format_postgres_box_value(raw),
        "path" => format_postgres_path_value(raw),
        "polygon" => format_postgres_polygon_value(raw),
        "circle" => format_postgres_circle_value(raw),
        "money" => format_postgres_money_value(raw),
        "interval" => format_postgres_interval_value(raw),
        "timetz" => format_postgres_timetz_value(raw),
        "bit" | "varbit" => format_postgres_bit_value(raw),
        "oid" | "xid" | "cid" | "regclass" | "regtype" | "regproc" | "regprocedure" | "regoper"
        | "regoperator" | "regnamespace" | "regrole" => format_postgres_u32_value(raw),
        "pg_lsn" => format_postgres_lsn_value(raw),
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
            format_postgres_range_value(type_name, raw)
        }
        _ => Err("unsupported PostgreSQL binary display type".into()),
    }
}

fn postgres_array_element_type_name(array_type_name: &str) -> Option<&str> {
    match array_type_name {
        "_inet" => Some("inet"),
        "_cidr" => Some("cidr"),
        "_tsvector" => Some("tsvector"),
        "_hstore" => Some("hstore"),
        "_macaddr" => Some("macaddr"),
        "_macaddr8" => Some("macaddr8"),
        "_point" => Some("point"),
        "_line" => Some("line"),
        "_lseg" => Some("lseg"),
        "_box" => Some("box"),
        "_path" => Some("path"),
        "_polygon" => Some("polygon"),
        "_circle" => Some("circle"),
        "_money" => Some("money"),
        "_interval" => Some("interval"),
        "_timetz" => Some("timetz"),
        "_bit" => Some("bit"),
        "_varbit" => Some("varbit"),
        "_oid" => Some("oid"),
        "_xid" => Some("xid"),
        "_cid" => Some("cid"),
        "_regclass" => Some("regclass"),
        "_regtype" => Some("regtype"),
        "_regproc" => Some("regproc"),
        "_regprocedure" => Some("regprocedure"),
        "_regoper" => Some("regoper"),
        "_regoperator" => Some("regoperator"),
        "_regnamespace" => Some("regnamespace"),
        "_regrole" => Some("regrole"),
        "_pg_lsn" => Some("pg_lsn"),
        "_int4range" => Some("int4range"),
        "_int8range" => Some("int8range"),
        "_numrange" => Some("numrange"),
        "_tsrange" => Some("tsrange"),
        "_tstzrange" => Some("tstzrange"),
        "_daterange" => Some("daterange"),
        _ => None,
    }
}

fn format_postgres_array_values(
    array_type_name: &str,
    raw: &[u8],
) -> std::result::Result<Vec<String>, Box<dyn std::error::Error + Sync + Send>> {
    let element_type_name = postgres_array_element_type_name(array_type_name)
        .ok_or("unsupported PostgreSQL array display type")?;

    if raw.len() < 12 {
        return Err("invalid array payload: too short".into());
    }

    let dimension_count = i32::from_be_bytes(raw[0..4].try_into()?);
    if dimension_count < 0 {
        return Err("invalid array payload: negative dimension count".into());
    }

    let mut offset = 12;
    let mut element_count = 1usize;
    for _ in 0..dimension_count {
        let dimension_length_bytes = raw
            .get(offset..offset + 4)
            .ok_or("invalid array payload: missing dimension length")?;
        let dimension_length = i32::from_be_bytes(dimension_length_bytes.try_into()?);
        if dimension_length < 0 {
            return Err("invalid array payload: negative dimension length".into());
        }
        element_count = element_count
            .checked_mul(dimension_length as usize)
            .ok_or("invalid array payload: dimension overflow")?;
        offset += 8;
    }

    if dimension_count == 0 {
        return Ok(Vec::new());
    }

    let mut values = Vec::with_capacity(element_count);
    for _ in 0..element_count {
        let element_length_bytes = raw
            .get(offset..offset + 4)
            .ok_or("invalid array payload: missing element length")?;
        let element_length = i32::from_be_bytes(element_length_bytes.try_into()?);
        offset += 4;

        if element_length < 0 {
            values.push("NULL".to_string());
            continue;
        }

        let element_length = element_length as usize;
        let element = raw
            .get(offset..offset + element_length)
            .ok_or("invalid array payload: truncated element")?;
        values.push(format_postgres_known_binary_value(
            element_type_name,
            element,
        )?);
        offset += element_length;
    }

    Ok(values)
}

fn format_postgres_known_binary_value(
    type_name: &str,
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    match type_name {
        "inet" | "cidr" => format_postgres_network_value(type_name, raw),
        "tsvector" => format_postgres_tsvector_value(raw),
        "hstore" | "macaddr" | "macaddr8" | "point" | "line" | "lseg" | "box" | "path"
        | "polygon" | "circle" | "money" | "interval" | "timetz" | "bit" | "varbit" | "oid"
        | "xid" | "cid" | "regclass" | "regtype" | "regproc" | "regprocedure" | "regoper"
        | "regoperator" | "regnamespace" | "regrole" | "pg_lsn" | "int4range" | "int8range"
        | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
            format_postgres_binary_display_value(type_name, raw)
        }
        _ => Err("unsupported PostgreSQL binary display type".into()),
    }
}

fn format_postgres_hstore_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("invalid hstore payload: too short".into());
    }

    let pair_count = i32::from_be_bytes(raw[0..4].try_into()?);
    if pair_count < 0 {
        return Err("invalid hstore payload: negative pair count".into());
    }

    let mut offset = 4;
    let mut pairs = Vec::with_capacity(pair_count as usize);
    for _ in 0..pair_count {
        let (key, next_offset) = parse_postgres_length_prefixed_text(raw, offset, "hstore key")?;
        offset = next_offset;

        let value_length_bytes = raw
            .get(offset..offset + 4)
            .ok_or("invalid hstore payload: missing value length")?;
        let value_length = i32::from_be_bytes(value_length_bytes.try_into()?);
        offset += 4;

        let key = quote_postgres_hstore_part(&key);
        if value_length < 0 {
            pairs.push(format!("{key}=>NULL"));
        } else {
            let value_length = value_length as usize;
            let value_bytes = raw
                .get(offset..offset + value_length)
                .ok_or("invalid hstore payload: truncated value")?;
            let value = std::str::from_utf8(value_bytes)?;
            offset += value_length;
            pairs.push(format!("{key}=>{}", quote_postgres_hstore_part(value)));
        }
    }

    Ok(pairs.join(", "))
}

fn parse_postgres_length_prefixed_text(
    raw: &[u8],
    offset: usize,
    label: &str,
) -> std::result::Result<(String, usize), Box<dyn std::error::Error + Sync + Send>> {
    let length_bytes = raw
        .get(offset..offset + 4)
        .ok_or_else(|| format!("invalid payload: missing {label} length"))?;
    let length = i32::from_be_bytes(length_bytes.try_into()?);
    if length < 0 {
        return Err(format!("invalid payload: negative {label} length").into());
    }
    let text_offset = offset + 4;
    let length = length as usize;
    let text_bytes = raw
        .get(text_offset..text_offset + length)
        .ok_or_else(|| format!("invalid payload: truncated {label}"))?;
    Ok((
        std::str::from_utf8(text_bytes)?.to_string(),
        text_offset + length,
    ))
}

fn quote_postgres_hstore_part(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn format_postgres_macaddr_value(
    raw: &[u8],
    expected_length: usize,
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != expected_length {
        return Err("invalid macaddr payload length".into());
    }

    Ok(raw
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(":"))
}

fn format_postgres_point_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let (x, y) = parse_postgres_point(raw, 0)?;
    Ok(format!("({},{})", format_float(x), format_float(y)))
}

fn format_postgres_line_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let values = parse_postgres_f64_values(raw, 3, "line")?;
    Ok(format!(
        "{{{},{},{}}}",
        format_float(values[0]),
        format_float(values[1]),
        format_float(values[2])
    ))
}

fn format_postgres_lseg_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let values = parse_postgres_f64_values(raw, 4, "lseg")?;
    Ok(format!(
        "[({},{}) , ({},{})]",
        format_float(values[0]),
        format_float(values[1]),
        format_float(values[2]),
        format_float(values[3])
    ))
    .map(|value| value.replace(" ,", ","))
}

fn format_postgres_box_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let values = parse_postgres_f64_values(raw, 4, "box")?;
    Ok(format!(
        "({},{}) , ({},{})",
        format_float(values[0]),
        format_float(values[1]),
        format_float(values[2]),
        format_float(values[3])
    )
    .replace(" ,", ","))
}

fn format_postgres_path_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 5 {
        return Err("invalid path payload: too short".into());
    }
    let closed = raw[0] != 0;
    let point_count = i32::from_be_bytes(raw[1..5].try_into()?);
    if point_count < 0 {
        return Err("invalid path payload: negative point count".into());
    }
    let points = parse_postgres_points(raw, 5, point_count as usize)?;
    let joined = points.join(",");
    if closed {
        Ok(format!("({joined})"))
    } else {
        Ok(format!("[{joined}]"))
    }
}

fn format_postgres_polygon_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("invalid polygon payload: too short".into());
    }
    let point_count = i32::from_be_bytes(raw[0..4].try_into()?);
    if point_count < 0 {
        return Err("invalid polygon payload: negative point count".into());
    }
    Ok(format!(
        "({})",
        parse_postgres_points(raw, 4, point_count as usize)?.join(",")
    ))
}

fn format_postgres_circle_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let values = parse_postgres_f64_values(raw, 3, "circle")?;
    Ok(format!(
        "<({},{}) , {}>",
        format_float(values[0]),
        format_float(values[1]),
        format_float(values[2])
    )
    .replace(" ,", ","))
}

fn format_postgres_money_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 8 {
        return Err("invalid money payload: expected 8 bytes".into());
    }

    let value = i64::from_be_bytes(raw.try_into()?);
    let sign = if value < 0 { "-" } else { "" };
    let absolute = value.unsigned_abs();
    Ok(format!("{sign}{}.{:02}", absolute / 100, absolute % 100))
}

fn format_postgres_interval_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 16 {
        return Err("invalid interval payload: expected 16 bytes".into());
    }

    let micros = i64::from_be_bytes(raw[0..8].try_into()?);
    let days = i32::from_be_bytes(raw[8..12].try_into()?);
    let months = i32::from_be_bytes(raw[12..16].try_into()?);
    let mut parts = Vec::new();

    if months != 0 {
        parts.push(format!("{months} mons"));
    }
    if days != 0 {
        parts.push(format!("{days} days"));
    }
    if micros != 0 || parts.is_empty() {
        parts.push(format_postgres_duration_micros(micros));
    }

    Ok(parts.join(" "))
}

fn format_postgres_timetz_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 12 {
        return Err("invalid timetz payload: expected 12 bytes".into());
    }

    let micros = i64::from_be_bytes(raw[0..8].try_into()?);
    let zone_seconds_west = i32::from_be_bytes(raw[8..12].try_into()?);
    let time = format_postgres_time_micros(micros);
    let offset_seconds = -zone_seconds_west;
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let absolute = offset_seconds.abs();
    Ok(format!(
        "{time}{sign}{:02}:{:02}",
        absolute / 3600,
        (absolute % 3600) / 60
    ))
}

fn format_postgres_bit_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() < 4 {
        return Err("invalid bit payload: too short".into());
    }

    let bit_count = i32::from_be_bytes(raw[0..4].try_into()?);
    if bit_count < 0 {
        return Err("invalid bit payload: negative bit count".into());
    }

    let bit_count = bit_count as usize;
    let bytes = raw
        .get(4..)
        .ok_or("invalid bit payload: missing bit bytes")?;
    if bytes.len() * 8 < bit_count {
        return Err("invalid bit payload: truncated bit bytes".into());
    }

    let mut output = String::with_capacity(bit_count);
    for index in 0..bit_count {
        let byte = bytes[index / 8];
        let mask = 1 << (7 - (index % 8));
        output.push(if byte & mask == 0 { '0' } else { '1' });
    }
    Ok(output)
}

fn format_postgres_u32_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 4 {
        return Err("invalid oid-like payload: expected 4 bytes".into());
    }
    Ok(u32::from_be_bytes(raw.try_into()?).to_string())
}

fn format_postgres_lsn_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 8 {
        return Err("invalid pg_lsn payload: expected 8 bytes".into());
    }
    let value = u64::from_be_bytes(raw.try_into()?);
    Ok(format!("{:X}/{:X}", value >> 32, value & 0xffff_ffff))
}

fn format_postgres_duration_micros(micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let absolute = micros.unsigned_abs();
    let total_seconds = absolute / 1_000_000;
    let fractional_micros = absolute % 1_000_000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    if fractional_micros == 0 {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}.{fractional_micros:06}")
            .trim_end_matches('0')
            .to_string()
    }
}

fn format_postgres_time_micros(micros: i64) -> String {
    format_postgres_duration_micros(micros)
}

fn parse_postgres_points(
    raw: &[u8],
    offset: usize,
    point_count: usize,
) -> std::result::Result<Vec<String>, Box<dyn std::error::Error + Sync + Send>> {
    let expected_length = offset + point_count * 16;
    if raw.len() < expected_length {
        return Err("invalid geometry payload: truncated points".into());
    }

    let mut points = Vec::with_capacity(point_count);
    for index in 0..point_count {
        let (x, y) = parse_postgres_point(raw, offset + index * 16)?;
        points.push(format!("({},{})", format_float(x), format_float(y)));
    }
    Ok(points)
}

fn parse_postgres_point(
    raw: &[u8],
    offset: usize,
) -> std::result::Result<(f64, f64), Box<dyn std::error::Error + Sync + Send>> {
    let values = parse_postgres_f64_values(
        raw.get(offset..).ok_or("invalid point payload: offset")?,
        2,
        "point",
    )?;
    Ok((values[0], values[1]))
}

fn parse_postgres_f64_values(
    raw: &[u8],
    count: usize,
    label: &str,
) -> std::result::Result<Vec<f64>, Box<dyn std::error::Error + Sync + Send>> {
    let expected_length = count * 8;
    if raw.len() < expected_length {
        return Err(format!("invalid {label} payload: too short").into());
    }

    let mut values = Vec::with_capacity(count);
    for chunk in raw[..expected_length].chunks_exact(8) {
        values.push(f64::from_be_bytes(chunk.try_into()?));
    }
    Ok(values)
}

fn format_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn format_postgres_range_value(
    type_name: &str,
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let flags = *raw.first().ok_or("invalid range payload: too short")?;
    if flags & 0x01 != 0 {
        return Ok("empty".to_string());
    }

    let mut offset = 1;
    let lower_infinite = flags & 0x08 != 0;
    let upper_infinite = flags & 0x10 != 0;

    let lower = if lower_infinite {
        String::new()
    } else {
        let (bound, next_offset) = parse_postgres_range_bound(type_name, raw, offset)?;
        offset = next_offset;
        bound
    };

    let upper = if upper_infinite {
        String::new()
    } else {
        let (bound, _next_offset) = parse_postgres_range_bound(type_name, raw, offset)?;
        bound
    };

    let lower_bracket = if flags & 0x02 != 0 { "[" } else { "(" };
    let upper_bracket = if flags & 0x04 != 0 { "]" } else { ")" };
    Ok(format!("{lower_bracket}{lower},{upper}{upper_bracket}"))
}

fn parse_postgres_range_bound(
    type_name: &str,
    raw: &[u8],
    offset: usize,
) -> std::result::Result<(String, usize), Box<dyn std::error::Error + Sync + Send>> {
    let length_bytes = raw
        .get(offset..offset + 4)
        .ok_or("invalid range payload: missing bound length")?;
    let length = i32::from_be_bytes(length_bytes.try_into()?);
    if length < 0 {
        return Err("invalid range payload: negative bound length".into());
    }
    let value_offset = offset + 4;
    let length = length as usize;
    let value = raw
        .get(value_offset..value_offset + length)
        .ok_or("invalid range payload: truncated bound")?;

    let formatted = match type_name {
        "int4range" if length == 4 => i32::from_be_bytes(value.try_into()?).to_string(),
        "int8range" if length == 8 => i64::from_be_bytes(value.try_into()?).to_string(),
        "numrange" => PgNumericString::parse(value)?,
        "tsrange" => parse_postgres_timestamp_value(value)?,
        "tstzrange" => parse_postgres_timestamptz_value(value)?,
        "daterange" => parse_postgres_date_value(value)?,
        _ => return Err("unsupported range subtype payload".into()),
    };

    Ok((formatted, value_offset + length))
}

fn parse_postgres_timestamp_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 8 {
        return Err("invalid timestamp payload: expected 8 bytes".into());
    }
    format_postgres_timestamp_micros(i64::from_be_bytes(raw.try_into()?), false)
}

fn parse_postgres_timestamptz_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 8 {
        return Err("invalid timestamptz payload: expected 8 bytes".into());
    }
    format_postgres_timestamp_micros(i64::from_be_bytes(raw.try_into()?), true)
}

fn format_postgres_timestamp_micros(
    micros: i64,
    utc: bool,
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .ok_or("invalid postgres epoch")?;
    let timestamp = epoch
        .checked_add_signed(chrono::Duration::microseconds(micros))
        .ok_or("invalid tstzrange payload: timestamp out of range")?;
    let timestamp =
        chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(timestamp, chrono::Utc);

    if utc {
        Ok(timestamp.to_rfc3339())
    } else {
        Ok(timestamp
            .naive_utc()
            .format("%Y-%m-%d %H:%M:%S%.f")
            .to_string())
    }
}

fn parse_postgres_date_value(
    raw: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Sync + Send>> {
    if raw.len() != 4 {
        return Err("invalid date payload: expected 4 bytes".into());
    }
    let days = i32::from_be_bytes(raw.try_into()?);
    let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).ok_or("invalid postgres date epoch")?;
    Ok(epoch
        .checked_add_signed(chrono::Duration::days(days as i64))
        .ok_or("invalid date payload: date out of range")?
        .to_string())
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
            tracing::warn!(
                "PostgreSQL transaction dropped without commit or rollback, auto-rolling back"
            );
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

        let pg_rows = client.query(&statement, &param_refs).await.map_err(|e| {
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

        let rows_affected = client.execute(&statement, &param_refs).await.map_err(|e| {
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

    fn explain_config(&self) -> ExplainConfig {
        ExplainConfig::postgresql()
    }

    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::PostgreSql
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        escape_identifier_pg(identifier)
    }

    fn requires_database_scoped_connection(&self) -> bool {
        true
    }

    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        match scope {
            ConnectionScope::Default => {
                resolved.normalized_scope = ConnectionScope::Default;
                resolved.effective_database = self.current_database_name().await?;
                resolved.effective_namespace = self.current_namespace_name().await?;
            }
            ConnectionScope::Database(database_name) => {
                let database_name = database_name.trim().to_string();
                resolved.normalized_scope = ConnectionScope::Database(database_name.clone());
                resolved.effective_database = Some(database_name.clone());
                resolved.physical_database_key = Some(database_name);
                resolved.requires_dedicated_connection = true;
                resolved.effective_namespace = self.current_namespace_name().await?;
            }
            ConnectionScope::Namespace(namespace) => {
                let namespace = namespace.trim().to_string();
                resolved.normalized_scope = ConnectionScope::Namespace(namespace.clone());
                resolved.effective_database = self.current_database_name().await?;
                resolved.effective_namespace = Some(namespace.clone());
                resolved.introspection_scope = Some(namespace);
            }
            ConnectionScope::KeyValueDatabase(index) => {
                resolved.normalized_scope = ConnectionScope::KeyValueDatabase(index);
            }
        }

        Ok(resolved)
    }

    fn bind_placeholder_policy(&self) -> BindPlaceholderPolicy {
        BindPlaceholderPolicy::DollarNumbered
    }

    fn max_bind_parameters(&self) -> usize {
        65_535
    }

    fn search_text_cast_expression(&self, expression_sql: &str) -> String {
        format!("({})::text", expression_sql)
    }

    fn normalize_import_check_expression(&self, expression_sql: &str) -> String {
        expression_sql.to_string()
    }

    fn generated_column_storage_keyword(&self, _requested_stored: bool) -> &'static str {
        "STORED"
    }

    fn semantic_default_sql(&self, kind: ImportSemanticDefault) -> Option<String> {
        match kind {
            ImportSemanticDefault::CurrentUser => Some("CURRENT_USER".to_string()),
            ImportSemanticDefault::GeneratedUuid => Some("gen_random_uuid()".to_string()),
        }
    }

    fn supports_partial_indexes(&self) -> bool {
        true
    }

    fn supports_include_indexes(&self) -> bool {
        true
    }

    fn supports_nulls_ordering_in_indexes(&self) -> bool {
        true
    }

    fn import_index_capabilities(&self) -> ImportIndexCapabilities {
        ImportIndexCapabilities {
            supports_hash: true,
            supports_gin: true,
            supports_gist: true,
            supports_spgist: true,
            supports_brin: true,
            supports_fulltext: false,
            supports_spatial: false,
            supports_partial: true,
            supports_include: true,
            supports_nulls_ordering: true,
        }
    }

    fn rename_table_sql(&self, table_name: &SqlObjectName, new_table_name: &str) -> Result<String> {
        Ok(format!(
            "ALTER TABLE {} RENAME TO {}",
            self.render_qualified_name(table_name),
            self.quote_identifier(new_table_name)
        ))
    }

    fn drop_table_sql(
        &self,
        table_name: &SqlObjectName,
        options: DropTableOptions,
    ) -> Result<String> {
        let mut sql = String::from("DROP TABLE");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(table_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn drop_view_sql(&self, view_name: &SqlObjectName, options: DropViewOptions) -> Result<String> {
        let mut sql = String::from("DROP VIEW");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(view_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn drop_trigger_sql(
        &self,
        trigger_name: &SqlObjectName,
        table_name: Option<&SqlObjectName>,
        options: DropTriggerOptions,
    ) -> Result<String> {
        let table_name = table_name.ok_or_else(|| {
            ZqlzError::NotSupported("PostgreSQL requires a table name for DROP TRIGGER".to_string())
        })?;

        let mut sql = String::from("DROP TRIGGER");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(trigger_name));
        sql.push_str(" ON ");
        sql.push_str(&self.render_qualified_name(table_name));
        if options.cascade {
            sql.push_str(" CASCADE");
        }
        Ok(sql)
    }

    fn truncate_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "TRUNCATE TABLE {}",
            self.render_qualified_name(table_name)
        ))
    }

    fn duplicate_table_sql(
        &self,
        source_table_name: &SqlObjectName,
        new_table_name: &SqlObjectName,
    ) -> Result<String> {
        Ok(format!(
            "CREATE TABLE {} AS SELECT * FROM {}",
            self.render_qualified_name(new_table_name),
            self.render_qualified_name(source_table_name)
        ))
    }

    fn clear_table_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        self.truncate_table_sql(table_name)
    }

    fn table_has_rows_sql(&self, table_name: &SqlObjectName) -> Result<String> {
        Ok(format!(
            "SELECT 1 FROM {} LIMIT 1",
            self.render_qualified_name(table_name)
        ))
    }

    fn select_rows_sql(
        &self,
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
    ) -> Result<String> {
        let projection = if projected_columns.is_empty() {
            "*".to_string()
        } else {
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut sql = format!(
            "SELECT {} FROM {}",
            projection,
            self.render_qualified_name(table_name)
        );

        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }

        Ok(sql)
    }

    fn select_distinct_rows_sql(
        &self,
        table_name: &SqlObjectName,
        projected_columns: &[String],
        where_clause_sql: Option<&str>,
        order_by_columns: &[String],
        limit: u64,
    ) -> Result<String> {
        let mut sql = format!(
            "SELECT DISTINCT {} FROM {}",
            projected_columns
                .iter()
                .map(|column| self.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", "),
            self.render_qualified_name(table_name)
        );

        if let Some(where_clause_sql) = where_clause_sql {
            sql.push_str(" WHERE ");
            sql.push_str(where_clause_sql);
        }

        if !order_by_columns.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(
                &order_by_columns
                    .iter()
                    .map(|column| self.quote_identifier(column))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }

        sql.push_str(&format!(" LIMIT {}", limit));
        Ok(sql)
    }

    fn insert_row_sql(
        &self,
        table_name: &SqlObjectName,
        column_names: &[String],
        value_count: usize,
    ) -> Result<String> {
        let placeholders = (0..value_count)
            .map(|index| self.format_bind_placeholder(index))
            .collect::<Vec<_>>()
            .join(", ");
        let columns = column_names
            .iter()
            .map(|column| self.quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");

        Ok(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.render_qualified_name(table_name),
            columns,
            placeholders
        ))
    }

    fn performance_metrics_query_sql(&self) -> Result<String> {
        Ok(
            r#"
        SELECT
            (SELECT sum(calls) FROM pg_stat_statements) as total_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'SELECT%') as select_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'INSERT%') as insert_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'UPDATE%') as update_queries,
            (SELECT count(*) FROM pg_stat_statements WHERE query ILIKE 'DELETE%') as delete_queries,
            (SELECT avg(mean_exec_time) FROM pg_stat_statements WHERE calls > 0) as avg_time_ms,
            (SELECT max(max_exec_time) FROM pg_stat_statements) as max_time_ms,
            (SELECT
                CASE WHEN (blks_hit + blks_read) = 0 THEN 0
                ELSE blks_hit::float / (blks_hit + blks_read)::float END
             FROM pg_stat_database WHERE datname = current_database()) as cache_hit_ratio,
            (SELECT setting::bigint * 8192 FROM pg_settings WHERE name = 'shared_buffers') as shared_buffers,
            (SELECT blks_hit FROM pg_stat_database WHERE datname = current_database()) as blks_hit,
            (SELECT blks_read FROM pg_stat_database WHERE datname = current_database()) as blks_read
        "#
            .to_string(),
        )
    }

    fn restore_sequence_sql(&self, sequence_name: &str, current_value: i64) -> Option<String> {
        Some(format!(
            "SELECT setval('{}', {}, true)",
            sequence_name.replace('\'', "''"),
            current_value
        ))
    }

    async fn export_sequence_current_value(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Result<Option<i64>> {
        let sequence_lookup_sql = "SELECT pg_get_serial_sequence($1, $2)";
        let sequence_lookup = self
            .query(
                sequence_lookup_sql,
                &[
                    Value::String(table_name.to_string()),
                    Value::String(column_name.to_string()),
                ],
            )
            .await?;

        let Some(sequence_name) = sequence_lookup
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(|value| value.as_str())
            .filter(|name| !name.is_empty())
            .map(ToString::to_string)
        else {
            return Ok(None);
        };

        let sequence_sql = format!(
            "SELECT last_value, is_called FROM {}",
            escape_table_name_pg(&sequence_name)
        );
        let sequence_result = match self.query(&sequence_sql, &[]).await {
            Ok(result) => result,
            Err(error) => {
                tracing::debug!(
                    sequence_name = %sequence_name,
                    error = %error,
                    "Could not read sequence current value; skipping sequence export"
                );
                return Ok(None);
            }
        };

        let Some(row) = sequence_result.rows.first() else {
            return Ok(None);
        };

        let last_value = row.values.first().and_then(|value| value.as_i64());
        let is_called = row
            .values
            .get(1)
            .and_then(|value| value.as_bool())
            .unwrap_or(false);

        Ok(if is_called { last_value } else { None })
    }

    async fn export_named_enum_definitions(&self) -> Result<Vec<(String, Vec<String>)>> {
        let sql = "SELECT t.typname, e.enumlabel \
                   FROM pg_type t \
                   JOIN pg_enum e ON t.oid = e.enumtypid \
                   JOIN pg_namespace n ON t.typnamespace = n.oid \
                   WHERE n.nspname = current_schema() \
                   ORDER BY t.typname, e.enumsortorder";
        let result = self.query(sql, &[]).await?;

        let mut enum_values: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for row in result.rows {
            let Some(type_name) = row
                .values
                .first()
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
            else {
                continue;
            };
            let Some(label) = row
                .values
                .get(1)
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
            else {
                continue;
            };

            if !enum_values.contains_key(&type_name) {
                order.push(type_name.clone());
            }
            enum_values.entry(type_name).or_default().push(label);
        }

        Ok(order
            .into_iter()
            .map(|name| {
                let values = enum_values.remove(&name).unwrap_or_default();
                (name, values)
            })
            .collect())
    }

    async fn resolve_session_namespace(&self) -> Result<Option<String>> {
        let result = self.query("SELECT current_schema()", &[]).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_str())
            .map(ToString::to_string))
    }

    async fn current_database_name(&self) -> Result<Option<String>> {
        let result = self.query("SELECT current_database()", &[]).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_str())
            .map(ToString::to_string))
    }

    fn supports_top_level_triggers(&self) -> bool {
        false
    }

    fn supports_materialized_views(&self) -> bool {
        true
    }

    fn supports_import_named_enum_types(&self) -> bool {
        true
    }

    fn check_constraint_enforcement(&self) -> CheckConstraintEnforcement {
        CheckConstraintEnforcement::Enforced
    }

    fn foreign_key_checks_sql(&self) -> Option<ForeignKeyChecksSql> {
        None
    }

    fn supports_fast_exact_count(&self) -> bool {
        false
    }

    fn should_use_schema_only_table_browse_fallback(
        &self,
        table_type: TableType,
        error_message: &str,
    ) -> bool {
        is_postgres_missing_user_mapping_error(error_message)
            || (table_type == TableType::ForeignTable
                && is_postgres_foreign_table_browse_error(error_message))
    }

    async fn estimated_row_count(&self, table_name: &SqlObjectName) -> Result<Option<u64>> {
        let result = match table_name.namespace.as_deref() {
            Some(namespace) => {
                self.query(
                    "SELECT CASE WHEN c.reltuples < 0 THEN NULL ELSE c.reltuples::bigint END \
                     FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p', 'm') \
                     LIMIT 1",
                    &[
                        Value::String(namespace.to_string()),
                        Value::String(table_name.name.clone()),
                    ],
                )
                .await?
            }
            None => {
                self.query(
                    "SELECT CASE WHEN c.reltuples < 0 THEN NULL ELSE c.reltuples::bigint END \
                     FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = current_schema() AND c.relname = $1 AND c.relkind IN ('r', 'p', 'm') \
                     LIMIT 1",
                    &[Value::String(table_name.name.clone())],
                )
                .await?
            }
        };

        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_i64())
            .and_then(|value| u64::try_from(value).ok()))
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

        let rows_affected = client.execute(&statement, &param_refs).await.map_err(|e| {
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

        let pg_rows = client.query(&statement, &param_refs).await.map_err(|e| {
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
                        value_to_pg_literal_for_type(
                            val,
                            row_column_type(&request.row_column_types, col)
                        )
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
                            value_to_pg_literal_for_type(
                                val,
                                row_column_type(&request.row_column_types, col)
                            )
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(" AND "),
        };

        // Build UPDATE statement with literal value
        let set_value = match &request.new_value {
            Some(val) => value_to_pg_literal_for_type(val, request.column_type.as_deref()),
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
        let rows_affected = client.execute(&sql, &[]).await.map_err(|e| {
            tracing::error!("PostgreSQL cell update error: {:?}", e);
            let message = format_postgres_cell_update_error(&e, &request);
            tracing::error!("Error details: {}", message);
            if let Some(db_error) = e.as_db_error() {
                tracing::error!(
                    "Database error details - Code: {:?}, Message: {}, Detail: {:?}, Hint: {:?}",
                    db_error.code(),
                    db_error.message(),
                    db_error.detail(),
                    db_error.hint()
                );
            }
            ZqlzError::Query(format!("Failed to update cell: {}", message))
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
            .try_get::<_, Option<PgNumericString>>(idx)
            .ok()
            .flatten()
            .map(|value| Value::Decimal(value.0))
            .or_else(|| {
                row.try_get::<_, Option<f64>>(idx)
                    .ok()
                    .flatten()
                    .map(Value::Float64)
            })
            .unwrap_or(Value::Null),
        "inet" | "cidr" => row
            .try_get::<_, Option<PgInetString>>(idx)
            .ok()
            .flatten()
            .map(|value| Value::String(value.0))
            .unwrap_or(Value::Null),
        "tsvector" => row
            .try_get::<_, Option<PgTsvectorString>>(idx)
            .ok()
            .flatten()
            .map(|value| Value::String(value.0))
            .unwrap_or(Value::Null),
        "tstzrange" => row
            .try_get::<_, Option<PgTstzRangeString>>(idx)
            .ok()
            .flatten()
            .map(|value| Value::String(value.0))
            .unwrap_or(Value::Null),
        "hstore" | "macaddr" | "macaddr8" | "point" | "line" | "lseg" | "box" | "path"
        | "polygon" | "circle" | "int4range" | "int8range" | "numrange" | "tsrange"
        | "daterange" | "money" | "interval" | "timetz" | "bit" | "varbit" | "oid" | "xid"
        | "cid" | "regclass" | "regtype" | "regproc" | "regprocedure" | "regoper"
        | "regoperator" | "regnamespace" | "regrole" | "pg_lsn" => row
            .try_get::<_, Option<PgBinaryDisplayString>>(idx)
            .ok()
            .flatten()
            .map(|value| Value::String(value.0))
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
        "_bool" => row
            .try_get::<_, Option<Vec<bool>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::Bool).collect()))
            .unwrap_or(Value::Null),
        "_float4" => row
            .try_get::<_, Option<Vec<f32>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::Float32).collect()))
            .unwrap_or(Value::Null),
        "_float8" => row
            .try_get::<_, Option<Vec<f64>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::Float64).collect()))
            .unwrap_or(Value::Null),
        "_numeric" => row
            .try_get::<_, Option<Vec<PgNumericString>>>(idx)
            .ok()
            .flatten()
            .map(|arr| {
                Value::Array(
                    arr.into_iter()
                        .map(|value| Value::Decimal(value.0))
                        .collect(),
                )
            })
            .unwrap_or(Value::Null),
        "_uuid" => row
            .try_get::<_, Option<Vec<uuid::Uuid>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::Uuid).collect()))
            .unwrap_or(Value::Null),
        "_date" => row
            .try_get::<_, Option<Vec<chrono::NaiveDate>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::Date).collect()))
            .unwrap_or(Value::Null),
        "_time" => row
            .try_get::<_, Option<Vec<chrono::NaiveTime>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::Time).collect()))
            .unwrap_or(Value::Null),
        "_timestamp" => row
            .try_get::<_, Option<Vec<chrono::NaiveDateTime>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::DateTime).collect()))
            .unwrap_or(Value::Null),
        "_timestamptz" => row
            .try_get::<_, Option<Vec<chrono::DateTime<chrono::Utc>>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::DateTimeUtc).collect()))
            .unwrap_or(Value::Null),
        "_bytea" => row
            .try_get::<_, Option<Vec<Vec<u8>>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::Bytes).collect()))
            .unwrap_or(Value::Null),
        "_json" | "_jsonb" => row
            .try_get::<_, Option<Vec<serde_json::Value>>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.into_iter().map(Value::Json).collect()))
            .unwrap_or(Value::Null),
        array_type if postgres_array_element_type_name(array_type).is_some() => row
            .try_get::<_, Option<PgArrayDisplay>>(idx)
            .ok()
            .flatten()
            .map(|arr| Value::Array(arr.0.into_iter().map(Value::String).collect()))
            .unwrap_or(Value::Null),
        array_type if array_type.starts_with('_') => row
            .try_get::<_, Option<Vec<PgFallbackString>>>(idx)
            .ok()
            .flatten()
            .map(|arr| {
                Value::Array(
                    arr.into_iter()
                        .map(|value| Value::String(value.0))
                        .collect(),
                )
            })
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

#[cfg(test)]
mod tests {
    use super::{
        format_postgres_array_values, format_postgres_binary_display_value,
        format_postgres_foreign_key_cell_update_error, format_postgres_network_value,
        format_postgres_tstzrange_value, format_postgres_tsvector_value,
        is_postgres_foreign_table_browse_error, is_postgres_missing_user_mapping_error,
        value_to_pg_literal_for_type,
    };
    use zqlz_core::{CellUpdateRequest, RowIdentifier, Value};

    #[test]
    fn postgres_array_literals_include_cast_when_type_known() {
        assert_eq!(
            value_to_pg_literal_for_type(
                &Value::Array(vec![Value::String("one".to_string())]),
                Some("text[]")
            ),
            "ARRAY['one']::text[]"
        );
        assert_eq!(
            value_to_pg_literal_for_type(&Value::Array(vec![]), Some("TextArray")),
            "ARRAY[]::text[]"
        );
        assert_eq!(
            value_to_pg_literal_for_type(
                &Value::Array(vec![Value::Json(serde_json::json!({ "enabled": true }))]),
                Some("jsonb[]")
            ),
            "ARRAY['{\"enabled\":true}']::jsonb[]"
        );
        assert_eq!(
            value_to_pg_literal_for_type(
                &Value::Array(vec![Value::Float64(12.5)]),
                Some("numeric[]")
            ),
            "ARRAY[12.5]::numeric[]"
        );
        assert_eq!(
            value_to_pg_literal_for_type(&Value::Array(vec![]), Some("public.status[]")),
            "ARRAY[]::public.status[]"
        );
    }

    #[test]
    fn postgres_special_string_literals_include_cast_when_type_known() {
        assert_eq!(
            value_to_pg_literal_for_type(&Value::String("active".to_string()), Some("status")),
            "'active'::status"
        );
        assert_eq!(
            value_to_pg_literal_for_type(&Value::String("[1,4)".to_string()), Some("int4range")),
            "'[1,4)'::int4range"
        );
        assert_eq!(
            value_to_pg_literal_for_type(&Value::String("192.168.0.1".to_string()), Some("inet")),
            "'192.168.0.1'::inet"
        );
        assert_eq!(
            value_to_pg_literal_for_type(&Value::String("plain".to_string()), Some("text")),
            "'plain'"
        );
    }

    #[test]
    fn postgres_foreign_table_browse_fallback_matches_user_mapping_failure() {
        assert!(is_postgres_foreign_table_browse_error(
            "Table operation failed: Query error: Failed to execute query: user mapping not found for user \"postgres\", server \"zqlz_feature_lab_postgres_server\" (code: SqlState(E42704))"
        ));
        assert!(is_postgres_missing_user_mapping_error(
            "Table operation failed: Query error: Failed to execute query: user mapping not found for user \"postgres\", server \"zqlz_feature_lab_postgres_server\" (code: SqlState(E42704))"
        ));
    }

    #[test]
    fn postgres_foreign_table_browse_fallback_ignores_regular_syntax_error() {
        assert!(!is_postgres_foreign_table_browse_error(
            "syntax error at or near \"select\""
        ));
    }

    #[test]
    fn postgres_network_binary_values_render_like_postgres_text() {
        assert_eq!(
            format_postgres_network_value("inet", &[2, 32, 0, 4, 192, 168, 10, 20]).unwrap(),
            "192.168.10.20"
        );
        assert_eq!(
            format_postgres_network_value("cidr", &[2, 24, 1, 4, 192, 168, 10, 0]).unwrap(),
            "192.168.10.0/24"
        );
    }

    #[test]
    fn postgres_tsvector_binary_value_renders_lexemes_and_positions() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&1_i32.to_be_bytes());
        raw.extend_from_slice(b"ada");
        raw.push(0);
        raw.extend_from_slice(&1_i16.to_be_bytes());
        raw.extend_from_slice(&(0xc000_u16 | 1).to_be_bytes());

        assert_eq!(format_postgres_tsvector_value(&raw).unwrap(), "'ada':1A");
    }

    #[test]
    fn postgres_tstzrange_binary_value_renders_range_bounds() {
        let mut raw = Vec::new();
        raw.push(0x12);
        raw.extend_from_slice(&8_i32.to_be_bytes());
        raw.extend_from_slice(&789_004_800_000_000_i64.to_be_bytes());

        assert_eq!(
            format_postgres_tstzrange_value(&raw).unwrap(),
            "[2025-01-01T00:00:00+00:00,)"
        );
    }

    #[test]
    fn postgres_hstore_binary_value_renders_key_value_pairs() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&2_i32.to_be_bytes());
        for (key, value) in [("source", "api"), ("segment", "developer")] {
            raw.extend_from_slice(&(key.len() as i32).to_be_bytes());
            raw.extend_from_slice(key.as_bytes());
            raw.extend_from_slice(&(value.len() as i32).to_be_bytes());
            raw.extend_from_slice(value.as_bytes());
        }

        assert_eq!(
            format_postgres_binary_display_value("hstore", &raw).unwrap(),
            "\"source\"=>\"api\", \"segment\"=>\"developer\""
        );
    }

    #[test]
    fn postgres_geometry_binary_values_render_text_shapes() {
        let mut point = Vec::new();
        point.extend_from_slice(&26.0_f64.to_be_bytes());
        point.extend_from_slice(&42.0_f64.to_be_bytes());
        assert_eq!(
            format_postgres_binary_display_value("point", &point).unwrap(),
            "(26,42)"
        );

        let mut polygon = Vec::new();
        polygon.extend_from_slice(&2_i32.to_be_bytes());
        for value in [0.0_f64, 0.0, 2.0, 2.0] {
            polygon.extend_from_slice(&value.to_be_bytes());
        }
        assert_eq!(
            format_postgres_binary_display_value("polygon", &polygon).unwrap(),
            "((0,0),(2,2))"
        );
    }

    #[test]
    fn postgres_range_binary_values_render_by_subtype() {
        let mut int4range = Vec::new();
        int4range.push(0x06);
        int4range.extend_from_slice(&4_i32.to_be_bytes());
        int4range.extend_from_slice(&1_i32.to_be_bytes());
        int4range.extend_from_slice(&4_i32.to_be_bytes());
        int4range.extend_from_slice(&4_i32.to_be_bytes());
        assert_eq!(
            format_postgres_binary_display_value("int4range", &int4range).unwrap(),
            "[1,4]"
        );

        let mut daterange = Vec::new();
        daterange.push(0x12);
        daterange.extend_from_slice(&4_i32.to_be_bytes());
        daterange.extend_from_slice(&9132_i32.to_be_bytes());
        assert_eq!(
            format_postgres_binary_display_value("daterange", &daterange).unwrap(),
            "[2025-01-01,)"
        );
    }

    #[test]
    fn postgres_binary_array_values_render_elements() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&1_i32.to_be_bytes());
        raw.extend_from_slice(&0_i32.to_be_bytes());
        raw.extend_from_slice(&869_i32.to_be_bytes());
        raw.extend_from_slice(&2_i32.to_be_bytes());
        raw.extend_from_slice(&1_i32.to_be_bytes());

        for address in [[192, 168, 10, 20], [10, 0, 1, 42]] {
            raw.extend_from_slice(&8_i32.to_be_bytes());
            raw.extend_from_slice(&[2, 32, 0, 4]);
            raw.extend_from_slice(&address);
        }

        assert_eq!(
            format_postgres_array_values("_inet", &raw).unwrap(),
            vec!["192.168.10.20".to_string(), "10.0.1.42".to_string()]
        );
    }

    #[test]
    fn postgres_misc_binary_values_render_text() {
        assert_eq!(
            format_postgres_binary_display_value("money", &12345_i64.to_be_bytes()).unwrap(),
            "123.45"
        );

        let mut interval = Vec::new();
        interval.extend_from_slice(&3_600_000_000_i64.to_be_bytes());
        interval.extend_from_slice(&2_i32.to_be_bytes());
        interval.extend_from_slice(&1_i32.to_be_bytes());
        assert_eq!(
            format_postgres_binary_display_value("interval", &interval).unwrap(),
            "1 mons 2 days 01:00:00"
        );

        let mut bits = Vec::new();
        bits.extend_from_slice(&4_i32.to_be_bytes());
        bits.push(0b1010_0000);
        assert_eq!(
            format_postgres_binary_display_value("bit", &bits).unwrap(),
            "1010"
        );

        assert_eq!(
            format_postgres_binary_display_value("pg_lsn", &0x16_B374_D848_u64.to_be_bytes())
                .unwrap(),
            "16/B374D848"
        );
    }

    #[test]
    fn postgres_foreign_key_cell_update_error_names_dependent_table_and_constraint() {
        let request = CellUpdateRequest {
            table_name: "zqlz_lab.tenants".to_string(),
            column_name: "tenant_id".to_string(),
            column_type: Some("uuid".to_string()),
            row_column_types: vec![("tenant_id".to_string(), "uuid".to_string())],
            new_value: Some(Value::String(
                "00000000-0000-0000-0000-000000020001".to_string(),
            )),
            row_identifier: RowIdentifier::PrimaryKey(vec![(
                "tenant_id".to_string(),
                Value::String("00000000-0000-0000-0000-000000000001".to_string()),
            )]),
        };

        let message = format_postgres_foreign_key_cell_update_error(
            &request,
            Some("customers"),
            Some("customers_tenant_id_fkey"),
            Some(
                "Key (tenant_id)=(00000000-0000-0000-0000-000000000001) is still referenced from table \"customers\".",
            ),
        );

        assert!(message.contains("Cannot update zqlz_lab.tenants.tenant_id"));
        assert!(message.contains("customers still references this value"));
        assert!(message.contains("customers_tenant_id_fkey"));
        assert!(message.contains("ON UPDATE CASCADE"));
        assert!(message.contains("Key (tenant_id)="));
    }
}
