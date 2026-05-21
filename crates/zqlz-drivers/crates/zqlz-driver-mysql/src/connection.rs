//! MySQL connection implementation

use async_trait::async_trait;
use mysql_async::{
    Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Row as MySqlRow, SslOpts,
    consts::{ColumnFlags, ColumnType},
    prelude::*,
};
use std::sync::Arc;
use std::sync::OnceLock;
use zqlz_core::{
    BindPlaceholderPolicy, CellUpdateRequest, CheckConstraintEnforcement, ColumnMeta, Connection,
    ConnectionScope, DropTableOptions, DropTriggerOptions, DropViewOptions, ExplainConfig,
    ExplainParserKind, ForeignKeyChecksSql, ImportIndexCapabilities, ImportSemanticDefault,
    QueryCancelHandle, QueryResult, ResolvedConnectionScope, Result, Row, RowIdentifier,
    SchemaIntrospection, SqlObjectName, StatementResult, Transaction, Value, ZqlzError,
};

use crate::MysqlSshTunnel;

fn strip_pg_casts(expr: &str) -> String {
    let mut result = expr.to_owned();
    loop {
        let Some(cast_start) = result.rfind("::") else {
            break;
        };
        let after = &result[cast_start + 2..];
        let ident_len = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ' ')
            .map(char::len_utf8)
            .sum::<usize>();
        if ident_len == 0 {
            break;
        }
        let mut end = cast_start + 2 + ident_len;
        if result[end..].starts_with("[]") {
            end += 2;
        }
        result.replace_range(cast_start..end, "");
    }
    result
}

/// Global Tokio runtime for MySQL operations.
///
/// mysql_async internally calls `tokio::spawn` for connection pooling and networking,
/// which requires a Tokio runtime context. GPUI uses its own async runtime, so we
/// provide a dedicated Tokio runtime for all MySQL operations.
fn get_mysql_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("zqlz-mysql-runtime")
            .build()
            .expect("Failed to create Tokio runtime for MySQL driver")
    })
}

fn mysql_column_type_display(
    column_type: ColumnType,
    column_length: Option<u32>,
    decimals: Option<u8>,
    flags: Option<ColumnFlags>,
) -> String {
    let unsigned = flags.is_some_and(|flags| flags.contains(ColumnFlags::UNSIGNED_FLAG));
    let with_unsigned = |base: &str| {
        if unsigned {
            format!("{} unsigned", base)
        } else {
            base.to_string()
        }
    };

    match column_type {
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            match (column_length, decimals) {
                (Some(length), Some(decimals)) if decimals != 0 && decimals != 0x1f => {
                    with_unsigned(&format!("decimal({},{})", length, decimals))
                }
                (Some(length), _) if length > 0 => with_unsigned(&format!("decimal({})", length)),
                _ => with_unsigned("decimal"),
            }
        }
        ColumnType::MYSQL_TYPE_TINY => with_unsigned("tinyint"),
        ColumnType::MYSQL_TYPE_SHORT => with_unsigned("smallint"),
        ColumnType::MYSQL_TYPE_LONG => with_unsigned("int"),
        ColumnType::MYSQL_TYPE_LONGLONG => with_unsigned("bigint"),
        ColumnType::MYSQL_TYPE_INT24 => with_unsigned("mediumint"),
        ColumnType::MYSQL_TYPE_FLOAT => with_unsigned("float"),
        ColumnType::MYSQL_TYPE_DOUBLE => with_unsigned("double"),
        ColumnType::MYSQL_TYPE_NULL => "null".to_string(),
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
            "timestamp".to_string()
        }
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => "date".to_string(),
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => "time".to_string(),
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => {
            "datetime".to_string()
        }
        ColumnType::MYSQL_TYPE_YEAR => "year".to_string(),
        ColumnType::MYSQL_TYPE_VARCHAR | ColumnType::MYSQL_TYPE_VAR_STRING => match column_length {
            Some(length) if length > 0 => format!("varchar({})", length),
            _ => "varchar".to_string(),
        },
        ColumnType::MYSQL_TYPE_STRING => {
            if flags.is_some_and(|flags| {
                flags.contains(ColumnFlags::BINARY_FLAG)
                    && !flags.contains(ColumnFlags::ENUM_FLAG)
                    && !flags.contains(ColumnFlags::SET_FLAG)
            }) {
                match column_length {
                    Some(length) if length > 0 => format!("binary({})", length),
                    _ => "binary".to_string(),
                }
            } else {
                "char".to_string()
            }
        }
        ColumnType::MYSQL_TYPE_BIT => "bit".to_string(),
        ColumnType::MYSQL_TYPE_TYPED_ARRAY => "array".to_string(),
        ColumnType::MYSQL_TYPE_VECTOR => "vector".to_string(),
        ColumnType::MYSQL_TYPE_UNKNOWN => "unknown".to_string(),
        ColumnType::MYSQL_TYPE_JSON => "json".to_string(),
        ColumnType::MYSQL_TYPE_ENUM => "enum".to_string(),
        ColumnType::MYSQL_TYPE_SET => "set".to_string(),
        ColumnType::MYSQL_TYPE_TINY_BLOB => "tinyblob".to_string(),
        ColumnType::MYSQL_TYPE_MEDIUM_BLOB => "mediumblob".to_string(),
        ColumnType::MYSQL_TYPE_LONG_BLOB => "longblob".to_string(),
        ColumnType::MYSQL_TYPE_BLOB => "blob".to_string(),
        ColumnType::MYSQL_TYPE_GEOMETRY => "geometry".to_string(),
    }
}

/// Cancel handle for MySQL queries.
///
/// MySQL doesn't have native query cancellation like PostgreSQL,
/// so we use a flag to signal cancellation and check it periodically.
pub struct MySqlCancelHandle {
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl QueryCancelHandle for MySqlCancelHandle {
    fn cancel(&self) {
        tracing::debug!("Setting MySQL query cancellation flag");
        self.cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// MySQL connection wrapper
pub struct MySqlConnection {
    pool: Pool,
    /// Stored at connect time so schema introspection methods can resolve
    /// `schema: None` to a concrete database name instead of relying on
    /// `DATABASE()` which returns NULL when no database was selected.
    database_name: Option<String>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    _ssh_tunnel: Option<MysqlSshTunnel>,
}

#[derive(Debug)]
pub struct MySqlConnectOptions {
    pub host: String,
    pub port: u16,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub ssl_opts: Option<SslOpts>,
    pub ssh_tunnel: Option<MysqlSshTunnel>,
}

impl MySqlConnection {
    /// Connect to a MySQL database
    pub async fn connect(options: MySqlConnectOptions) -> Result<Self> {
        tracing::info!(host = %options.host, port = %options.port, database = ?options.database, "connecting to MySQL database");

        let mut opts_builder = OptsBuilder::from_opts(Opts::default())
            .ip_or_hostname(options.host.as_str())
            .tcp_port(options.port);

        if let Some(db) = options.database.as_deref() {
            opts_builder = opts_builder.db_name(Some(db));
        }
        if let Some(u) = options.user.as_deref() {
            opts_builder = opts_builder.user(Some(u));
        }
        if let Some(p) = options.password.as_deref() {
            opts_builder = opts_builder.pass(Some(p));
        }
        if options.ssl_opts.is_some() {
            opts_builder = opts_builder.ssl_opts(options.ssl_opts);
        }

        let constraints = PoolConstraints::new(1, 1).ok_or_else(|| {
            ZqlzError::Connection(
                "Failed to configure MySQL pool constraints (min=1, max=1)".into(),
            )
        })?;

        let pool_opts = PoolOpts::default()
            .with_constraints(constraints)
            .with_reset_connection(false);
        opts_builder = opts_builder.pool_opts(pool_opts);

        let opts: Opts = opts_builder.into();

        // Pool creation and initial connection test must run on the Tokio runtime
        // because mysql_async internally uses tokio::spawn for pool management.
        let runtime = get_mysql_runtime();
        let pool = runtime
            .spawn(async move {
                let pool = Pool::new(opts);
                // Verify connectivity by acquiring and releasing a connection
                let _conn = pool.get_conn().await.map_err(|e| {
                    ZqlzError::Connection(format!("Failed to connect to MySQL: {}", e))
                })?;
                Ok::<Pool, ZqlzError>(pool)
            })
            .await
            .map_err(|e| ZqlzError::Connection(format!("MySQL connection task failed: {}", e)))??;

        // Resolve the active database name so schema introspection can use a
        // concrete value instead of relying on DATABASE() at query time.
        let database_name = if let Some(db) = options.database.as_deref() {
            Some(db.to_string())
        } else {
            let pool_clone = pool.clone();
            get_mysql_runtime()
                .spawn(async move {
                    let mut conn = pool_clone.get_conn().await.map_err(|e| {
                        ZqlzError::Connection(format!(
                            "Failed to get connection for DATABASE() query: {}",
                            e
                        ))
                    })?;
                    let row: Option<(Option<String>,)> =
                        conn.query_first("SELECT DATABASE()").await.map_err(|e| {
                            ZqlzError::Query(format!("Failed to query DATABASE(): {}", e))
                        })?;
                    Ok::<Option<String>, ZqlzError>(row.and_then(|(db,)| db))
                })
                .await
                .map_err(|e| ZqlzError::Connection(format!("MySQL DATABASE() task failed: {}", e)))?
                .unwrap_or(None)
        };

        tracing::info!(host = %options.host, port = %options.port, database = ?database_name, "MySQL connection established");
        Ok(Self {
            pool,
            database_name,
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            _ssh_tunnel: options.ssh_tunnel,
        })
    }

    /// Get a connection from the pool, dispatched on the MySQL Tokio runtime
    async fn get_conn(&self) -> Result<Conn> {
        let pool = self.pool.clone();
        get_mysql_runtime()
            .spawn(async move { pool.get_conn().await })
            .await
            .map_err(|e| ZqlzError::Connection(format!("MySQL get_conn task failed: {}", e)))?
            .map_err(|e| ZqlzError::Connection(format!("Failed to get MySQL connection: {}", e)))
    }

    /// Reset the cancellation flag
    fn reset_cancellation(&self) {
        self.cancelled
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Returns the stored database name for use by schema introspection.
    ///
    /// In MySQL, "schema" and "database" are synonymous. This mirrors
    /// PostgreSQL's `schema.unwrap_or("public")` pattern — callers pass
    /// `schema: None` through the trait, and this method provides the
    /// concrete database name to use in information_schema queries.
    pub fn default_database(&self) -> Option<&str> {
        self.database_name.as_deref()
    }
}

/// Escape a value for SQL literal inclusion (for MySQL)
fn value_to_mysql_literal(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(v) => if *v { "TRUE" } else { "FALSE" }.to_string(),
        Value::Int8(v) => v.to_string(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float32(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::String(v) => format!("'{}'", v.replace("'", "''").replace("\\", "\\\\")),
        Value::Bytes(v) => {
            // Convert bytes to hex string for MySQL
            let hex: String = v.iter().map(|b| format!("{:02x}", b)).collect();
            format!("X'{}'", hex)
        }
        Value::Uuid(v) => format!("'{}'", v),
        Value::Json(v) => format!(
            "'{}'",
            v.to_string().replace("'", "''").replace("\\", "\\\\")
        ),
        Value::DateTimeUtc(v) => format!("'{}'", v.format("%Y-%m-%d %H:%M:%S")),
        Value::Date(v) => format!("'{}'", v),
        Value::Time(v) => format!("'{}'", v),
        Value::DateTime(v) => format!("'{}'", v.format("%Y-%m-%d %H:%M:%S")),
        Value::Decimal(v) => v.to_string(),
        Value::Array(arr) => {
            // MySQL doesn't have native array support, convert to JSON
            let json = serde_json::to_string(&serde_json::Value::Array(
                arr.iter().map(Value::to_json_value).collect(),
            ))
            .map_err(|error| ZqlzError::Query(format!("Failed to serialize array: {}", error)))?;
            format!("'{}'", json.replace("'", "''"))
        }
    })
}

fn render_mysql_sql_with_params(sql: &str, params: &[Value]) -> Result<String> {
    if params.is_empty() {
        return Ok(sql.to_string());
    }

    let uses_numbered_params = params
        .iter()
        .enumerate()
        .any(|(index, _)| sql.contains(&format!("${}", index + 1)));

    let mut result = sql.to_string();
    if uses_numbered_params {
        for (index, param) in params.iter().enumerate() {
            let placeholder = format!("${}", index + 1);
            let value_str = value_to_mysql_literal(param)?;
            result = result.replacen(&placeholder, &value_str, 1);
        }
    } else {
        for param in params {
            let value_str = value_to_mysql_literal(param)?;
            result = result.replacen("?", &value_str, 1);
        }
    }

    Ok(result)
}

fn value_to_mysql_literal_for_type(value: &Value, column_type: Option<&str>) -> Result<String> {
    if column_type.map(Value::normalize_data_type).as_deref() == Some("set")
        && let Value::Array(values) = value
    {
        let rendered_values = values
            .iter()
            .map(|value| match value {
                Value::String(value) => value.clone(),
                other => other.display_for_editor(),
            })
            .collect::<Vec<_>>()
            .join(",");
        return Ok(format!(
            "'{}'",
            rendered_values.replace("'", "''").replace("\\", "\\\\")
        ));
    }

    value_to_mysql_literal(value)
}

/// Convert mysql_async Value to our Value type, using column type metadata
/// to correctly interpret byte strings from the text protocol.
fn mysql_value_to_value(
    val: mysql_async::Value,
    col_type: ColumnType,
    flags: Option<ColumnFlags>,
) -> Value {
    match val {
        mysql_async::Value::NULL => Value::Null,
        mysql_async::Value::Bytes(bytes) => {
            if col_type == ColumnType::MYSQL_TYPE_BIT {
                return Value::String(format_mysql_bit_value(&bytes));
            }

            if col_type == ColumnType::MYSQL_TYPE_GEOMETRY {
                return Value::String(format_mysql_geometry_value(&bytes));
            }

            if matches!(
                col_type,
                ColumnType::MYSQL_TYPE_BLOB
                    | ColumnType::MYSQL_TYPE_TINY_BLOB
                    | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                    | ColumnType::MYSQL_TYPE_LONG_BLOB
            ) {
                return Value::Bytes(bytes);
            }

            if col_type == ColumnType::MYSQL_TYPE_STRING
                && flags.is_some_and(|flags| {
                    flags.contains(ColumnFlags::BINARY_FLAG)
                        && !flags.contains(ColumnFlags::ENUM_FLAG)
                        && !flags.contains(ColumnFlags::SET_FLAG)
                })
            {
                return Value::Bytes(bytes);
            }

            if let Ok(s) = String::from_utf8(bytes.clone()) {
                match col_type {
                    ColumnType::MYSQL_TYPE_TINY
                    | ColumnType::MYSQL_TYPE_SHORT
                    | ColumnType::MYSQL_TYPE_LONG
                    | ColumnType::MYSQL_TYPE_LONGLONG
                    | ColumnType::MYSQL_TYPE_INT24
                    | ColumnType::MYSQL_TYPE_YEAR => s
                        .parse::<i64>()
                        .map(Value::Int64)
                        .unwrap_or(Value::String(s)),
                    ColumnType::MYSQL_TYPE_FLOAT => s
                        .parse::<f32>()
                        .map(Value::Float32)
                        .unwrap_or(Value::String(s)),
                    ColumnType::MYSQL_TYPE_DOUBLE => s
                        .parse::<f64>()
                        .map(Value::Float64)
                        .unwrap_or(Value::String(s)),
                    ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                        Value::Decimal(s)
                    }
                    ColumnType::MYSQL_TYPE_JSON => serde_json::from_str::<serde_json::Value>(&s)
                        .map(Value::Json)
                        .unwrap_or(Value::String(s)),
                    _ => Value::String(s),
                }
            } else {
                Value::Bytes(bytes)
            }
        }
        mysql_async::Value::Int(i) => Value::Int64(i),
        mysql_async::Value::UInt(u) => {
            if u <= i64::MAX as u64 {
                Value::Int64(u as i64)
            } else {
                Value::String(u.to_string())
            }
        }
        mysql_async::Value::Float(f) => Value::Float32(f),
        mysql_async::Value::Double(d) => Value::Float64(d),
        mysql_async::Value::Date(year, month, day, hour, min, sec, micro) => {
            if hour == 0 && min == 0 && sec == 0 && micro == 0 {
                // Date only
                if let Some(date) =
                    chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                {
                    Value::Date(date)
                } else {
                    Value::String(format!("{:04}-{:02}-{:02}", year, month, day))
                }
            } else {
                // DateTime
                if let Some(dt) =
                    chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32).and_then(
                        |d| d.and_hms_micro_opt(hour as u32, min as u32, sec as u32, micro),
                    )
                {
                    Value::DateTime(dt)
                } else {
                    Value::String(format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                        year, month, day, hour, min, sec
                    ))
                }
            }
        }
        mysql_async::Value::Time(negative, days, hours, mins, secs, micros) => {
            let total_hours = days * 24 + (hours as u32);
            let sign = if negative { "-" } else { "" };
            Value::String(format!(
                "{}{:02}:{:02}:{:02}.{:06}",
                sign, total_hours, mins, secs, micros
            ))
        }
    }
}

fn mysql_column_metadata(
    mysql_columns: &[mysql_async::Column],
) -> (
    Vec<ColumnMeta>,
    Vec<String>,
    Vec<ColumnType>,
    Vec<ColumnFlags>,
) {
    let mut columns = Vec::new();
    let mut column_names = Vec::new();
    let mut column_types = Vec::new();
    let mut column_flags = Vec::new();

    for (idx, column) in mysql_columns.iter().enumerate() {
        let name = column.name_str().to_string();
        column_names.push(name.clone());
        column_types.push(column.column_type());
        column_flags.push(column.flags());

        columns.push(ColumnMeta {
            name,
            data_type: mysql_column_type_display(
                column.column_type(),
                Some(column.column_length()),
                Some(column.decimals()),
                Some(column.flags()),
            ),
            nullable: true,
            ordinal: idx,
            max_length: Some(column.column_length() as i64),
            precision: None,
            scale: None,
            auto_increment: false,
            default_value: None,
            comment: None,
            enum_values: None,
        });
    }

    (columns, column_names, column_types, column_flags)
}

fn format_mysql_bit_value(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }

    let mut output = String::with_capacity(bytes.len() * 8);
    for byte in bytes {
        output.push_str(&format!("{byte:08b}"));
    }

    output
}

fn format_mysql_geometry_value(bytes: &[u8]) -> String {
    match parse_mysql_geometry_value(bytes) {
        Ok(value) => value,
        Err(_) => {
            let hex = bytes
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>();
            format!("0x{hex}")
        }
    }
}

fn parse_mysql_geometry_value(
    bytes: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if bytes.len() < 5 {
        return Err("invalid MySQL geometry payload: too short".into());
    }

    parse_wkb_geometry(&bytes[4..])
}

fn parse_wkb_geometry(
    bytes: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut offset = 0;
    parse_wkb_geometry_at(bytes, &mut offset)
}

fn parse_wkb_geometry_at(
    bytes: &[u8],
    offset: &mut usize,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let byte_order = read_u8(bytes, offset)?;
    let little_endian = match byte_order {
        0 => false,
        1 => true,
        _ => return Err("invalid WKB byte order".into()),
    };
    let geometry_type = read_u32(bytes, offset, little_endian)?;

    match geometry_type {
        1 => parse_wkb_point(bytes, offset, little_endian),
        2 => parse_wkb_linestring(bytes, offset, little_endian),
        3 => parse_wkb_polygon(bytes, offset, little_endian),
        4 => parse_wkb_geometry_collection("MULTIPOINT", bytes, offset, little_endian),
        5 => parse_wkb_geometry_collection("MULTILINESTRING", bytes, offset, little_endian),
        6 => parse_wkb_geometry_collection("MULTIPOLYGON", bytes, offset, little_endian),
        7 => parse_wkb_geometry_collection("GEOMETRYCOLLECTION", bytes, offset, little_endian),
        _ => Err("unsupported WKB geometry type".into()),
    }
}

fn parse_wkb_point(
    bytes: &[u8],
    offset: &mut usize,
    little_endian: bool,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let x = read_f64(bytes, offset, little_endian)?;
    let y = read_f64(bytes, offset, little_endian)?;
    Ok(format!(
        "POINT({} {})",
        format_mysql_float(x),
        format_mysql_float(y)
    ))
}

fn parse_wkb_linestring(
    bytes: &[u8],
    offset: &mut usize,
    little_endian: bool,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let point_count = read_u32(bytes, offset, little_endian)? as usize;
    let mut points = Vec::with_capacity(point_count);
    for _ in 0..point_count {
        let x = read_f64(bytes, offset, little_endian)?;
        let y = read_f64(bytes, offset, little_endian)?;
        points.push(format!(
            "{} {}",
            format_mysql_float(x),
            format_mysql_float(y)
        ));
    }
    Ok(format!("LINESTRING({})", points.join(",")))
}

fn parse_wkb_polygon(
    bytes: &[u8],
    offset: &mut usize,
    little_endian: bool,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let ring_count = read_u32(bytes, offset, little_endian)? as usize;
    let mut rings = Vec::with_capacity(ring_count);
    for _ in 0..ring_count {
        let point_count = read_u32(bytes, offset, little_endian)? as usize;
        let mut points = Vec::with_capacity(point_count);
        for _ in 0..point_count {
            let x = read_f64(bytes, offset, little_endian)?;
            let y = read_f64(bytes, offset, little_endian)?;
            points.push(format!(
                "{} {}",
                format_mysql_float(x),
                format_mysql_float(y)
            ));
        }
        rings.push(format!("({})", points.join(",")));
    }
    Ok(format!("POLYGON({})", rings.join(",")))
}

fn parse_wkb_geometry_collection(
    name: &str,
    bytes: &[u8],
    offset: &mut usize,
    little_endian: bool,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let geometry_count = read_u32(bytes, offset, little_endian)? as usize;
    let mut geometries = Vec::with_capacity(geometry_count);
    for _ in 0..geometry_count {
        geometries.push(parse_wkb_geometry_at(bytes, offset)?);
    }
    Ok(format!("{name}({})", geometries.join(",")))
}

fn read_u8(
    bytes: &[u8],
    offset: &mut usize,
) -> std::result::Result<u8, Box<dyn std::error::Error + Send + Sync>> {
    let value = *bytes
        .get(*offset)
        .ok_or("invalid WKB payload: missing byte")?;
    *offset += 1;
    Ok(value)
}

fn read_u32(
    bytes: &[u8],
    offset: &mut usize,
    little_endian: bool,
) -> std::result::Result<u32, Box<dyn std::error::Error + Send + Sync>> {
    let raw: [u8; 4] = bytes
        .get(*offset..*offset + 4)
        .ok_or("invalid WKB payload: truncated u32")?
        .try_into()?;
    *offset += 4;
    Ok(if little_endian {
        u32::from_le_bytes(raw)
    } else {
        u32::from_be_bytes(raw)
    })
}

fn read_f64(
    bytes: &[u8],
    offset: &mut usize,
    little_endian: bool,
) -> std::result::Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    let raw: [u8; 8] = bytes
        .get(*offset..*offset + 8)
        .ok_or("invalid WKB payload: truncated f64")?
        .try_into()?;
    *offset += 8;
    Ok(if little_endian {
        f64::from_le_bytes(raw)
    } else {
        f64::from_be_bytes(raw)
    })
}

fn format_mysql_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

#[async_trait]
impl Connection for MySqlConnection {
    fn driver_name(&self) -> &str {
        "mysql"
    }

    fn dialect_id(&self) -> Option<&'static str> {
        Some("mysql")
    }

    async fn resolve_scope(&self, scope: ConnectionScope) -> Result<ResolvedConnectionScope> {
        let mut resolved = ResolvedConnectionScope::default_scope();
        resolved.requested_scope = scope.clone();

        match scope {
            ConnectionScope::Default => {
                resolved.normalized_scope = ConnectionScope::Default;
                let database_name = self.current_database_name().await?;
                resolved.effective_database = database_name.clone();
                resolved.effective_namespace = database_name.clone();
                resolved.introspection_scope = database_name;
            }
            ConnectionScope::Database(database_name)
            | ConnectionScope::Namespace(database_name) => {
                let database_name = database_name.trim().to_string();
                resolved.normalized_scope = ConnectionScope::Database(database_name.clone());
                resolved.effective_database = Some(database_name.clone());
                resolved.effective_namespace = Some(database_name.clone());
                resolved.introspection_scope = Some(database_name);
            }
            ConnectionScope::KeyValueDatabase(index) => {
                resolved.normalized_scope = ConnectionScope::KeyValueDatabase(index);
            }
        }

        Ok(resolved)
    }

    fn explain_config(&self) -> ExplainConfig {
        ExplainConfig::mysql()
    }

    fn explain_parser_kind(&self) -> ExplainParserKind {
        ExplainParserKind::MySql
    }

    fn quote_identifier(&self, identifier: &str) -> String {
        escape_identifier_mysql(identifier)
    }

    fn bind_placeholder_policy(&self) -> BindPlaceholderPolicy {
        BindPlaceholderPolicy::QuestionMark
    }

    fn max_bind_parameters(&self) -> usize {
        65_535
    }

    fn limited_select_sql(&self, base_sql: &str, limit: u64) -> String {
        format!("{} LIMIT {}", base_sql, limit)
    }

    fn search_text_cast_expression(&self, expression_sql: &str) -> String {
        format!("CAST({} AS CHAR)", expression_sql)
    }

    fn normalize_import_check_expression(&self, expression_sql: &str) -> String {
        strip_pg_casts(expression_sql)
    }

    fn semantic_default_sql(&self, kind: ImportSemanticDefault) -> Option<String> {
        match kind {
            ImportSemanticDefault::CurrentUser => Some("CURRENT_USER".to_string()),
            ImportSemanticDefault::GeneratedUuid => Some("(UUID())".to_string()),
        }
    }

    fn supports_partial_indexes(&self) -> bool {
        false
    }

    fn supports_include_indexes(&self) -> bool {
        false
    }

    fn supports_nulls_ordering_in_indexes(&self) -> bool {
        false
    }

    fn import_index_capabilities(&self) -> ImportIndexCapabilities {
        ImportIndexCapabilities {
            supports_hash: false,
            supports_gin: false,
            supports_gist: false,
            supports_spgist: false,
            supports_brin: false,
            supports_fulltext: true,
            supports_spatial: true,
            supports_partial: false,
            supports_include: false,
            supports_nulls_ordering: false,
        }
    }

    fn rename_table_sql(&self, table_name: &SqlObjectName, new_table_name: &str) -> Result<String> {
        let new_name = match table_name.namespace.as_deref() {
            Some(namespace) => format!(
                "{}.{}",
                self.quote_identifier(namespace),
                self.quote_identifier(new_table_name)
            ),
            None => self.quote_identifier(new_table_name),
        };

        Ok(format!(
            "RENAME TABLE {} TO {}",
            self.render_qualified_name(table_name),
            new_name
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
            return Err(ZqlzError::NotSupported(
                "MySQL does not support DROP TABLE ... CASCADE".to_string(),
            ));
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
            return Err(ZqlzError::NotSupported(
                "MySQL does not support DROP VIEW ... CASCADE".to_string(),
            ));
        }
        Ok(sql)
    }

    fn drop_trigger_sql(
        &self,
        trigger_name: &SqlObjectName,
        _table_name: Option<&SqlObjectName>,
        options: DropTriggerOptions,
    ) -> Result<String> {
        let mut sql = String::from("DROP TRIGGER");
        if options.if_exists {
            sql.push_str(" IF EXISTS");
        }
        sql.push(' ');
        sql.push_str(&self.render_qualified_name(trigger_name));
        if options.cascade {
            return Err(ZqlzError::NotSupported(
                "MySQL does not support DROP TRIGGER ... CASCADE".to_string(),
            ));
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
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Questions') as total_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_select') as select_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_insert') as insert_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_update') as update_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Com_delete') as delete_queries,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_bytes_data') as buffer_pool_pages_data,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_read_requests') as buffer_pool_read_requests,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_reads') as buffer_pool_reads,
            (SELECT @@innodb_buffer_pool_size) as buffer_pool_size
        "#
            .to_string(),
        )
    }

    fn restore_sequence_sql(&self, sequence_name: &str, current_value: i64) -> Option<String> {
        let table_name = sequence_name
            .split_once('.')
            .map(|(table, _)| table)
            .unwrap_or(sequence_name);
        Some(format!(
            "ALTER TABLE {} AUTO_INCREMENT = {}",
            self.quote_identifier(table_name),
            current_value + 1
        ))
    }

    async fn export_sequence_current_value(
        &self,
        table_name: &str,
        _column_name: &str,
    ) -> Result<Option<i64>> {
        let result = self
            .query(
                "SELECT AUTO_INCREMENT
                 FROM information_schema.TABLES
                 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
                &[Value::String(table_name.to_string())],
            )
            .await?;
        let Some(next_value) = result
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(|value| value.as_i64())
        else {
            return Ok(None);
        };

        Ok(Some(next_value - 1).filter(|value| *value > 0))
    }

    fn normalize_create_view_sql(&self, sql: &str) -> String {
        let trimmed = sql.trim();
        if trimmed
            .to_ascii_uppercase()
            .starts_with("CREATE OR REPLACE VIEW")
        {
            trimmed.to_string()
        } else if trimmed.to_ascii_uppercase().starts_with("CREATE VIEW") {
            trimmed.replacen("CREATE VIEW", "CREATE OR REPLACE VIEW", 1)
        } else {
            trimmed.to_string()
        }
    }

    async fn resolve_session_namespace(&self) -> Result<Option<String>> {
        let result = self.query("SELECT DATABASE()", &[]).await?;
        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_str())
            .map(ToString::to_string)
            .or_else(|| self.default_database().map(ToString::to_string)))
    }

    async fn current_database_name(&self) -> Result<Option<String>> {
        self.resolve_session_namespace().await
    }

    fn supports_fast_exact_count(&self) -> bool {
        false
    }

    fn check_constraint_enforcement(&self) -> CheckConstraintEnforcement {
        CheckConstraintEnforcement::Unknown
    }

    fn foreign_key_checks_sql(&self) -> Option<ForeignKeyChecksSql> {
        Some(ForeignKeyChecksSql {
            disable_sql: "SET FOREIGN_KEY_CHECKS=0".to_string(),
            enable_sql: "SET FOREIGN_KEY_CHECKS=1".to_string(),
        })
    }

    async fn estimated_row_count(&self, table_name: &SqlObjectName) -> Result<Option<u64>> {
        let schema = table_name
            .namespace
            .clone()
            .or_else(|| self.default_database().map(ToString::to_string));

        let Some(schema_name) = schema else {
            return Ok(None);
        };

        let result = self
            .query(
                "SELECT TABLE_ROWS FROM information_schema.tables WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? LIMIT 1",
                &[
                    Value::String(schema_name),
                    Value::String(table_name.name.clone()),
                ],
            )
            .await?;

        Ok(result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(|value| value.as_i64())
            .and_then(|value| u64::try_from(value).ok()))
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        self.reset_cancellation();

        let mut conn = self.get_conn().await?;

        let final_sql = render_mysql_sql_with_params(sql, params)?;

        let affected_rows = get_mysql_runtime()
            .spawn(async move {
                conn.query_drop(&final_sql)
                    .await
                    .map_err(|e| ZqlzError::Query(format!("Failed to execute statement: {}", e)))?;
                Ok::<u64, ZqlzError>(conn.affected_rows())
            })
            .await
            .map_err(|e| ZqlzError::Query(format!("MySQL execute task failed: {}", e)))??;

        tracing::debug!(affected_rows = affected_rows, "statement executed");
        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows,
            error: None,
        })
    }

    #[tracing::instrument(skip(self, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.reset_cancellation();
        let start_time = std::time::Instant::now();

        let mut conn = self.get_conn().await?;

        let final_sql = render_mysql_sql_with_params(sql, params)?;

        let cancelled = self.cancelled.clone();
        let (columns, _column_names, rows) = get_mysql_runtime()
            .spawn(async move {
                let result = conn
                    .query_iter(&final_sql)
                    .await
                    .map_err(|e| ZqlzError::Query(format!("Failed to execute query: {}", e)))?;

                let (columns, column_names, column_types, column_flags) =
                    mysql_column_metadata(result.columns_ref());
                let mysql_rows: Vec<MySqlRow> = result.collect_and_drop().await.map_err(|e| {
                    ZqlzError::Query(format!("Failed to collect query rows: {}", e))
                })?;

                let mut rows = Vec::new();
                for mysql_row in mysql_rows {
                    if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                        tracing::debug!("Query cancelled by user");
                        break;
                    }

                    let mut values = Vec::new();
                    for idx in 0..columns.len() {
                        let mysql_val: mysql_async::Value =
                            mysql_row.get(idx).unwrap_or(mysql_async::Value::NULL);
                        let col_type = column_types
                            .get(idx)
                            .copied()
                            .unwrap_or(ColumnType::MYSQL_TYPE_STRING);
                        let value = mysql_value_to_value(
                            mysql_val,
                            col_type,
                            column_flags.get(idx).copied(),
                        );
                        values.push(value);
                    }
                    rows.push(Row::new(column_names.clone(), values));
                }

                Ok::<(Vec<ColumnMeta>, Vec<String>, Vec<Row>), ZqlzError>((
                    columns,
                    column_names,
                    rows,
                ))
            })
            .await
            .map_err(|e| ZqlzError::Query(format!("MySQL query task failed: {}", e)))??;

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
        tracing::debug!("beginning MySQL transaction");

        // Get a connection from the pool for the duration of the transaction
        let mut conn = self.get_conn().await?;

        // Begin the transaction
        let conn = get_mysql_runtime()
            .spawn(async move {
                conn.query_drop("START TRANSACTION")
                    .await
                    .map_err(|e| ZqlzError::Query(format!("Failed to begin transaction: {}", e)))?;
                Ok::<Conn, ZqlzError>(conn)
            })
            .await
            .map_err(|e| {
                ZqlzError::Connection(format!("MySQL begin transaction task failed: {}", e))
            })??;

        tracing::debug!("MySQL transaction begun successfully");
        Ok(Box::new(MySqlTransaction {
            conn: Arc::new(tokio::sync::Mutex::new(Some(conn))),
            committed: false,
            rolled_back: false,
        }))
    }

    async fn close(&self) -> Result<()> {
        tracing::info!("closing MySQL connection pool");
        let pool = self.pool.clone();
        get_mysql_runtime()
            .spawn(async move { pool.disconnect().await })
            .await
            .map_err(|e| ZqlzError::Connection(format!("MySQL close task failed: {}", e)))?
            .map_err(|e| {
                ZqlzError::Connection(format!("Failed to close MySQL connection: {}", e))
            })?;
        Ok(())
    }

    fn is_closed(&self) -> bool {
        false
    }

    fn as_schema_introspection(&self) -> Option<&dyn SchemaIntrospection> {
        Some(self)
    }

    fn cancel_handle(&self) -> Option<Arc<dyn QueryCancelHandle>> {
        Some(Arc::new(MySqlCancelHandle {
            cancelled: self.cancelled.clone(),
        }))
    }

    /// Override update_cell to use SQL literals instead of parameters
    async fn update_cell(&self, request: CellUpdateRequest) -> Result<u64> {
        tracing::debug!(
            table = %request.table_name,
            column = %request.column_name,
            "updating cell value (MySQL)"
        );

        // Escape table name (may include schema.table format)
        let table_identifier = escape_table_name_mysql(&request.table_name);

        // Build WHERE clause with literal values
        let where_clause = match &request.row_identifier {
            RowIdentifier::RowIndex(_) => {
                return Err(ZqlzError::NotSupported(
                    "Row index-based updates not supported. Use primary key or full row identifier."
                        .to_string(),
                ));
            }
            RowIdentifier::PrimaryKey(pk_values) => pk_values
                .iter()
                .map(|(col, val)| {
                    Ok(format!(
                        "{} = {}",
                        escape_identifier_mysql(col),
                        value_to_mysql_literal(val)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?
                .join(" AND "),
            RowIdentifier::FullRow(row_values) => row_values
                .iter()
                .map(|(col, val)| {
                    if val == &Value::Null {
                        Ok(format!("{} IS NULL", escape_identifier_mysql(col)))
                    } else {
                        Ok(format!(
                            "{} = {}",
                            escape_identifier_mysql(col),
                            value_to_mysql_literal(val)?
                        ))
                    }
                })
                .collect::<Result<Vec<_>>>()?
                .join(" AND "),
        };

        let set_value = match &request.new_value {
            Some(val) => value_to_mysql_literal_for_type(val, request.column_type.as_deref())?,
            None => "NULL".to_string(),
        };

        let sql = format!(
            "UPDATE {} SET {} = {} WHERE {}",
            table_identifier,
            escape_identifier_mysql(&request.column_name),
            set_value,
            where_clause
        );

        tracing::debug!("MySQL update SQL: {}", sql);

        let mut conn = self.get_conn().await?;
        let rows_affected = get_mysql_runtime()
            .spawn(async move {
                conn.query_drop(&sql)
                    .await
                    .map_err(|e| ZqlzError::Query(format!("Failed to update cell: {}", e)))?;
                Ok::<u64, ZqlzError>(conn.affected_rows())
            })
            .await
            .map_err(|e| ZqlzError::Query(format!("MySQL update task failed: {}", e)))??;

        tracing::debug!(affected_rows = rows_affected, "cell update completed");
        Ok(rows_affected)
    }
}

/// Escape a MySQL identifier (column name, etc.)
fn escape_identifier_mysql(identifier: &str) -> String {
    format!("`{}`", identifier.replace("`", "``"))
}

/// Escape a table name which may include schema (e.g., "schema.table")
fn escape_table_name_mysql(table_name: &str) -> String {
    if table_name.contains('.') {
        // Handle schema.table format
        let parts: Vec<&str> = table_name.splitn(2, '.').collect();
        if parts.len() == 2 {
            format!(
                "{}.{}",
                escape_identifier_mysql(parts[0]),
                escape_identifier_mysql(parts[1])
            )
        } else {
            escape_identifier_mysql(table_name)
        }
    } else {
        escape_identifier_mysql(table_name)
    }
}

/// MySQL transaction implementation
///
/// Manages a MySQL transaction lifecycle using a dedicated connection from the pool.
/// The transaction begins with START TRANSACTION and can be committed or rolled back.
/// The connection is held for the duration of the transaction and returned to the pool when done.
pub struct MySqlTransaction {
    conn: Arc<tokio::sync::Mutex<Option<Conn>>>,
    committed: bool,
    rolled_back: bool,
}

#[async_trait]
impl Transaction for MySqlTransaction {
    async fn commit(mut self: Box<Self>) -> Result<()> {
        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }
        if self.rolled_back {
            return Err(ZqlzError::Query("Transaction already rolled back".into()));
        }

        tracing::debug!("committing MySQL transaction");

        let conn_mutex = self.conn.clone();
        get_mysql_runtime()
            .spawn(async move {
                let mut guard = conn_mutex.lock().await;
                if let Some(mut conn) = guard.take() {
                    conn.query_drop("COMMIT").await.map_err(|e| {
                        ZqlzError::Query(format!("Failed to commit transaction: {}", e))
                    })?;
                    // Connection returns to pool when dropped
                }
                Ok::<(), ZqlzError>(())
            })
            .await
            .map_err(|e| ZqlzError::Connection(format!("MySQL commit task failed: {}", e)))??;

        self.committed = true;
        tracing::debug!("MySQL transaction committed");
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        if self.committed {
            return Err(ZqlzError::Query("Transaction already committed".into()));
        }
        if self.rolled_back {
            return Err(ZqlzError::Query("Transaction already rolled back".into()));
        }

        tracing::debug!("rolling back MySQL transaction");

        let conn_mutex = self.conn.clone();
        get_mysql_runtime()
            .spawn(async move {
                let mut guard = conn_mutex.lock().await;
                if let Some(mut conn) = guard.take() {
                    conn.query_drop("ROLLBACK").await.map_err(|e| {
                        ZqlzError::Query(format!("Failed to rollback transaction: {}", e))
                    })?;
                    // Connection returns to pool when dropped
                }
                Ok::<(), ZqlzError>(())
            })
            .await
            .map_err(|e| ZqlzError::Connection(format!("MySQL rollback task failed: {}", e)))??;

        self.rolled_back = true;
        tracing::debug!("MySQL transaction rolled back");
        Ok(())
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<StatementResult> {
        if self.committed {
            return Err(ZqlzError::Query(
                "Cannot execute on committed transaction".into(),
            ));
        }
        if self.rolled_back {
            return Err(ZqlzError::Query(
                "Cannot execute on rolled back transaction".into(),
            ));
        }

        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing statement in transaction");

        let conn_mutex = self.conn.clone();
        let sql = render_mysql_sql_with_params(sql, params)?;

        let rows_affected = get_mysql_runtime()
            .spawn(async move {
                let mut guard = conn_mutex.lock().await;
                if let Some(ref mut conn) = *guard {
                    conn.query_drop(&sql).await.map_err(|e| {
                        ZqlzError::Query(format!("Failed to execute statement: {}", e))
                    })?;
                    Ok::<u64, ZqlzError>(conn.affected_rows())
                } else {
                    Err(ZqlzError::Query(
                        "Transaction connection no longer available".into(),
                    ))
                }
            })
            .await
            .map_err(|e| ZqlzError::Query(format!("MySQL execute task failed: {}", e)))??;

        tracing::debug!(
            affected_rows = rows_affected,
            "statement executed in transaction"
        );
        Ok(StatementResult {
            is_query: false,
            result: None,
            affected_rows: rows_affected,
            error: None,
        })
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        if self.committed {
            return Err(ZqlzError::Query(
                "Cannot query on committed transaction".into(),
            ));
        }
        if self.rolled_back {
            return Err(ZqlzError::Query(
                "Cannot query on rolled back transaction".into(),
            ));
        }

        tracing::debug!(sql_preview = %sql.chars().take(100).collect::<String>(), "executing query in transaction");

        let conn_mutex = self.conn.clone();
        let sql = render_mysql_sql_with_params(sql, params)?;
        let start_time = std::time::Instant::now();

        let (rows_data, columns, column_names, column_types, column_flags) = get_mysql_runtime()
            .spawn(async move {
                let mut guard = conn_mutex.lock().await;
                if let Some(ref mut conn) = *guard {
                    let result = conn
                        .query_iter(&sql)
                        .await
                        .map_err(|e| ZqlzError::Query(format!("Failed to execute query: {}", e)))?;

                    let (columns, column_names, column_types, column_flags) =
                        mysql_column_metadata(result.columns_ref());
                    let rows: Vec<MySqlRow> = result.collect_and_drop().await.map_err(|e| {
                        ZqlzError::Query(format!("Failed to collect query rows: {}", e))
                    })?;

                    Ok::<
                        (
                            Vec<MySqlRow>,
                            Vec<ColumnMeta>,
                            Vec<String>,
                            Vec<ColumnType>,
                            Vec<ColumnFlags>,
                        ),
                        ZqlzError,
                    >((rows, columns, column_names, column_types, column_flags))
                } else {
                    Err(ZqlzError::Query(
                        "Transaction connection no longer available".into(),
                    ))
                }
            })
            .await
            .map_err(|e| ZqlzError::Query(format!("MySQL query task failed: {}", e)))??;

        // Convert MySQL rows to ZQLZ rows
        let mut rows = Vec::new();
        for mysql_row in &rows_data {
            let mut values = Vec::new();
            for idx in 0..column_names.len() {
                let mysql_val: mysql_async::Value =
                    mysql_row.get(idx).unwrap_or(mysql_async::Value::NULL);
                let col_type = column_types
                    .get(idx)
                    .copied()
                    .unwrap_or(ColumnType::MYSQL_TYPE_STRING);
                let value =
                    mysql_value_to_value(mysql_val, col_type, column_flags.get(idx).copied());
                values.push(value);
            }
            rows.push(Row::new(column_names.clone(), values));
        }

        let execution_time_ms = start_time.elapsed().as_millis() as u64;
        let total_rows = rows.len();

        tracing::debug!(
            row_count = total_rows,
            execution_time_ms = execution_time_ms,
            "query executed successfully in transaction"
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
}

impl Drop for MySqlTransaction {
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            tracing::warn!(
                "MySQL transaction dropped without commit or rollback - will auto-rollback"
            );
            let conn_mutex = self.conn.clone();
            std::thread::spawn(move || {
                get_mysql_runtime().block_on(async move {
                    let mut guard = conn_mutex.lock().await;
                    if let Some(ref mut conn) = *guard
                        && let Err(e) = conn.query_drop("ROLLBACK").await
                    {
                        tracing::error!("Failed to rollback dropped transaction: {}", e);
                    }
                });
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_column_type_display_uses_sql_type_names() {
        assert_eq!(
            mysql_column_type_display(ColumnType::MYSQL_TYPE_VAR_STRING, Some(255), Some(0), None,),
            "varchar(255)"
        );
        assert_eq!(
            mysql_column_type_display(ColumnType::MYSQL_TYPE_NEWDECIMAL, Some(18), Some(4), None,),
            "decimal(18,4)"
        );
        assert_eq!(
            mysql_column_type_display(
                ColumnType::MYSQL_TYPE_LONGLONG,
                None,
                None,
                Some(ColumnFlags::UNSIGNED_FLAG),
            ),
            "bigint unsigned"
        );
        assert_eq!(
            mysql_column_type_display(ColumnType::MYSQL_TYPE_JSON, None, None, None),
            "json"
        );
        assert_eq!(
            mysql_column_type_display(ColumnType::MYSQL_TYPE_DATETIME2, None, None, None),
            "datetime"
        );
        assert_eq!(
            mysql_column_type_display(
                ColumnType::MYSQL_TYPE_STRING,
                Some(16),
                None,
                Some(ColumnFlags::BINARY_FLAG),
            ),
            "binary(16)"
        );
        assert_eq!(
            mysql_column_type_display(
                ColumnType::MYSQL_TYPE_STRING,
                Some(16),
                None,
                Some(ColumnFlags::BINARY_FLAG | ColumnFlags::ENUM_FLAG),
            ),
            "char"
        );
    }

    #[test]
    fn mysql_value_conversion_preserves_decimal_and_text_types() {
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Bytes(b"1234567890.123456".to_vec()),
                ColumnType::MYSQL_TYPE_NEWDECIMAL,
                None,
            ),
            Value::Decimal("1234567890.123456".to_string())
        );
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Bytes(br#"{"enabled":true}"#.to_vec()),
                ColumnType::MYSQL_TYPE_JSON,
                None,
            ),
            Value::Json(serde_json::json!({ "enabled": true }))
        );
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Bytes(b"active".to_vec()),
                ColumnType::MYSQL_TYPE_STRING,
                Some(ColumnFlags::ENUM_FLAG),
            ),
            Value::String("active".to_string())
        );
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Bytes(vec![0xff, 0x00]),
                ColumnType::MYSQL_TYPE_STRING,
                Some(ColumnFlags::BINARY_FLAG),
            ),
            Value::Bytes(vec![0xff, 0x00])
        );
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Bytes(vec![0xff, 0x00]),
                ColumnType::MYSQL_TYPE_VAR_STRING,
                None,
            ),
            Value::Bytes(vec![0xff, 0x00])
        );
    }

    #[test]
    fn mysql_bit_value_renders_binary_text() {
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Bytes(vec![0b0000_1010]),
                ColumnType::MYSQL_TYPE_BIT,
                None,
            ),
            Value::String("00001010".to_string())
        );
    }

    #[test]
    fn mysql_geometry_value_renders_wkt_or_hex_fallback() {
        let mut point = Vec::new();
        point.extend_from_slice(&4326_u32.to_le_bytes());
        point.push(1);
        point.extend_from_slice(&1_u32.to_le_bytes());
        point.extend_from_slice(&26.0_f64.to_le_bytes());
        point.extend_from_slice(&42.0_f64.to_le_bytes());

        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Bytes(point),
                ColumnType::MYSQL_TYPE_GEOMETRY,
                None,
            ),
            Value::String("POINT(26 42)".to_string())
        );

        let mut linestring = Vec::new();
        linestring.extend_from_slice(&0_u32.to_le_bytes());
        linestring.push(1);
        linestring.extend_from_slice(&2_u32.to_le_bytes());
        linestring.extend_from_slice(&2_u32.to_le_bytes());
        for value in [0.0_f64, 0.0, 2.0, 2.0] {
            linestring.extend_from_slice(&value.to_le_bytes());
        }

        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Bytes(linestring),
                ColumnType::MYSQL_TYPE_GEOMETRY,
                None,
            ),
            Value::String("LINESTRING(0 0,2 2)".to_string())
        );

        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Bytes(vec![1, 2, 3]),
                ColumnType::MYSQL_TYPE_GEOMETRY,
                None,
            ),
            Value::String("0x010203".to_string())
        );
    }

    #[test]
    fn mysql_array_literal_uses_json_text() {
        let literal = value_to_mysql_literal(&Value::Array(vec![
            Value::String("one".to_string()),
            Value::Int32(2),
            Value::Null,
        ]))
        .expect("array serializes");

        assert_eq!(literal, "'[\"one\",2,null]'");
    }

    #[test]
    fn mysql_params_render_question_mark_placeholders() {
        let sql = render_mysql_sql_with_params(
            "SELECT ? AS name, ? AS active",
            &[Value::String("O'Reilly".to_string()), Value::Bool(true)],
        )
        .expect("params render");

        assert_eq!(sql, "SELECT 'O''Reilly' AS name, TRUE AS active");
    }

    #[test]
    fn mysql_params_render_numbered_placeholders() {
        let sql = render_mysql_sql_with_params(
            "SELECT $2 AS second, $1 AS first",
            &[Value::Int32(10), Value::Int32(20)],
        )
        .expect("params render");

        assert_eq!(sql, "SELECT 20 AS second, 10 AS first");
    }

    #[test]
    fn mysql_set_literal_uses_comma_separated_text() {
        let literal = value_to_mysql_literal_for_type(
            &Value::Array(vec![
                Value::String("read".to_string()),
                Value::String("write".to_string()),
            ]),
            Some("set"),
        )
        .expect("set serializes");

        assert_eq!(literal, "'read,write'");
    }
}
