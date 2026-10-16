//! MySQL connection implementation

use async_trait::async_trait;
use mysql_async::{
    Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Row as MySqlRow, SslOpts,
    consts::{ColumnFlags, ColumnType},
    prelude::*,
};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use zqlz_core::{
    BindPlaceholderPolicy, CellUpdateRequest, CheckConstraintEnforcement, ColumnMeta, Connection,
    ConnectionScope, DropTableOptions, DropTriggerOptions, DropViewOptions, ExplainConfig,
    ExplainParserKind, ForeignKeyChecksSql, ImportIndexCapabilities, ImportSemanticDefault,
    QueryCancelHandle, QueryResult, ResolvedConnectionScope, Result, Row, RowIdentifier,
    SchemaIntrospection, SingleRowDmlScope, SqlObjectName,
    StatementResult, Transaction, Value, ZqlzError,
};

use zqlz_schema_engine::{DefaultDialect, SchemaEngine};

use crate::schema::MySqlCatalog;
use crate::MysqlSshTunnel;

mod query_execution;
mod session;
mod sql_text;

#[cfg(test)]
mod live_tests;

use session::Session;

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

/// Idle connections the pool keeps warm.
const MIN_POOL_CONNECTIONS: usize = 1;

/// Concurrent connections the pool may open.
///
/// Every query checks out a connection for its whole round trip, so a pool of one
/// makes the whole application single-file: background schema introspection, the
/// row fetch the user is waiting on, and a second tab's edit all queue behind each
/// other. Against a remote host where one round trip costs ~200ms that turns a
/// handful of metadata queries into tens of seconds of spinner. A small ceiling
/// keeps user-visible work off the back of background work without opening enough
/// sessions to matter to the server.
const MAX_POOL_CONNECTIONS: usize = 5;

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

/// The charset id MySQL reports for binary strings (`BINARY`, `VARBINARY`,
/// `BLOB`). Every other id is a real character set, even on the BLOB type codes
/// that `TEXT` columns share with `BLOB`.
const MYSQL_BINARY_CHARSET: u16 = 63;

/// Longest value, in bytes, a real column can hold. Anything longer is an
/// expression result (`@@sql_mode`, `CONCAT(...)`) whose protocol length says
/// nothing about a declared size.
const MYSQL_MAX_DECLARED_BYTES: u32 = 65535;

/// Geographic SRIDs MySQL prints latitude-first. `ST_AsText` swaps the stored
/// x/y for these. ponytail: common ones only; other geographic SRIDs print x y
/// as stored, add the id here (or query information_schema) if one matters.
const MYSQL_LAT_LONG_SRIDS: &[u32] = &[
    4019, 4135, 4148, 4152, 4230, 4258, 4267, 4269, 4283, 4322, 4326, 4490, 4612, 4617, 4674, 4759,
];

/// Everything the column definition says about a result column. The text
/// protocol sends every value as a string, so the type, charset, flags, length
/// and decimals together are the only way to tell a `TEXT` from a `BLOB`.
#[derive(Debug, Clone, Copy)]
struct MySqlColumnInfo {
    column_type: ColumnType,
    flags: ColumnFlags,
    charset: u16,
    length: u32,
    decimals: u8,
}

impl MySqlColumnInfo {
    fn is_binary(&self) -> bool {
        self.charset == MYSQL_BINARY_CHARSET
            && !self
                .flags
                .intersects(ColumnFlags::ENUM_FLAG | ColumnFlags::SET_FLAG)
    }

    fn is_unsigned(&self) -> bool {
        self.flags.contains(ColumnFlags::UNSIGNED_FLAG)
    }

    /// Declared length in characters: the protocol length is bytes, which is
    /// characters times the charset's widest character.
    fn char_length(&self) -> u32 {
        self.length / mysql_charset_max_bytes(self.charset)
    }

    /// Declared `DECIMAL(p,s)` precision. The protocol length also counts the
    /// sign (signed columns only) and the decimal point (when there is a scale).
    fn decimal_precision(&self) -> u32 {
        let extra = u32::from(!self.is_unsigned()) + u32::from(self.decimals > 0);
        self.length
            .saturating_sub(extra)
            .max(u32::from(self.decimals))
            .max(1)
    }
}

/// Widest encoding of one character in a charset, by collation id.
fn mysql_charset_max_bytes(charset: u16) -> u32 {
    match charset {
        45 | 46 | 54 | 55 | 56 | 60 | 61 | 62 | 101..=124 | 160..=183 | 224..=250 | 255..=324 => 4,
        12 | 33 | 76 | 83 | 91 | 97 | 98 | 192..=215 | 223 => 3,
        1 | 13 | 19 | 24 | 28 | 35 | 84..=88 | 90 | 95 | 96 | 128..=151 => 2,
        _ => 1,
    }
}

/// The SQL type the server declared for a result column, as `COLUMN_TYPE`
/// would show it where the protocol carries enough to say so.
fn mysql_column_type_display(info: &MySqlColumnInfo) -> String {
    let with_unsigned = |base: String| {
        if info.is_unsigned() {
            format!("{} unsigned", base)
        } else {
            base
        }
    };
    // `(n)` for fractional-second types; 0 and the 0x1f "not fixed" marker print nothing.
    let with_fsp = |base: &str| match info.decimals {
        1..=6 => format!("{}({})", base, info.decimals),
        _ => base.to_string(),
    };
    // Lengths beyond what a column can hold belong to expressions: say no length.
    let sized = |base: &str, length: u32| {
        if length > 0 && info.length <= MYSQL_MAX_DECLARED_BYTES {
            format!("{}({})", base, length)
        } else {
            base.to_string()
        }
    };

    match info.column_type {
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            if info.length == 0 {
                with_unsigned("decimal".to_string())
            } else {
                with_unsigned(format!(
                    "decimal({},{})",
                    info.decimal_precision(),
                    info.decimals
                ))
            }
        }
        // BOOLEAN is TINYINT(1); the display width is kept only for that one.
        ColumnType::MYSQL_TYPE_TINY if info.length == 1 => with_unsigned("tinyint(1)".to_string()),
        ColumnType::MYSQL_TYPE_TINY => with_unsigned("tinyint".to_string()),
        ColumnType::MYSQL_TYPE_SHORT => with_unsigned("smallint".to_string()),
        ColumnType::MYSQL_TYPE_LONG => with_unsigned("int".to_string()),
        ColumnType::MYSQL_TYPE_LONGLONG => with_unsigned("bigint".to_string()),
        ColumnType::MYSQL_TYPE_INT24 => with_unsigned("mediumint".to_string()),
        ColumnType::MYSQL_TYPE_FLOAT => with_unsigned("float".to_string()),
        ColumnType::MYSQL_TYPE_DOUBLE => with_unsigned("double".to_string()),
        ColumnType::MYSQL_TYPE_NULL => "null".to_string(),
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
            with_fsp("timestamp")
        }
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => "date".to_string(),
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => with_fsp("time"),
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => with_fsp("datetime"),
        ColumnType::MYSQL_TYPE_YEAR => "year".to_string(),
        ColumnType::MYSQL_TYPE_VARCHAR | ColumnType::MYSQL_TYPE_VAR_STRING => {
            if info.is_binary() {
                sized("varbinary", info.length)
            } else {
                sized("varchar", info.char_length())
            }
        }
        ColumnType::MYSQL_TYPE_STRING => {
            if info.flags.contains(ColumnFlags::ENUM_FLAG) {
                "enum".to_string()
            } else if info.flags.contains(ColumnFlags::SET_FLAG) {
                "set".to_string()
            } else if info.is_binary() {
                sized("binary", info.length)
            } else {
                sized("char", info.char_length())
            }
        }
        ColumnType::MYSQL_TYPE_BIT => sized("bit", info.length),
        ColumnType::MYSQL_TYPE_TYPED_ARRAY => "array".to_string(),
        ColumnType::MYSQL_TYPE_VECTOR => "vector".to_string(),
        ColumnType::MYSQL_TYPE_UNKNOWN => "unknown".to_string(),
        ColumnType::MYSQL_TYPE_JSON => "json".to_string(),
        ColumnType::MYSQL_TYPE_ENUM => "enum".to_string(),
        ColumnType::MYSQL_TYPE_SET => "set".to_string(),
        // TEXT shares the BLOB type codes, and the server sends the plain BLOB
        // code for every size: the charset says text or blob, the length the size.
        ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB => {
            let (size, family) = if info.is_binary() {
                (info.length, "blob")
            } else {
                (info.char_length(), "text")
            };
            let prefix = match info.column_type {
                ColumnType::MYSQL_TYPE_TINY_BLOB => "tiny",
                ColumnType::MYSQL_TYPE_MEDIUM_BLOB => "medium",
                ColumnType::MYSQL_TYPE_LONG_BLOB => "long",
                _ if size <= 255 => "tiny",
                _ if size <= 65535 => "",
                _ if size <= 16_777_215 => "medium",
                _ => "long",
            };
            format!("{prefix}{family}")
        }
        ColumnType::MYSQL_TYPE_GEOMETRY => "geometry".to_string(),
    }
}

/// Counts operations currently holding a pooled MySQL connection.
///
/// A heartbeat ping that queues behind a long-running query hits its own deadline
/// and gets misread as a dead transport, so heartbeats skip a connection that is
/// already working. mysql_async offers no non-blocking pool checkout, so occupancy
/// is tracked here instead and reported through `Connection::is_busy`.
#[derive(Clone, Default)]
pub(crate) struct InFlight(Arc<AtomicUsize>);

impl InFlight {
    /// Mark the connection occupied until the returned guard is dropped.
    fn enter(&self) -> InFlightGuard {
        self.0.fetch_add(1, Ordering::SeqCst);
        InFlightGuard(Arc::clone(&self.0))
    }

    fn is_busy(&self) -> bool {
        self.0.load(Ordering::SeqCst) > 0
    }
}

/// Releases one unit of occupancy when dropped, including on early return.
pub(crate) struct InFlightGuard(Arc<AtomicUsize>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// MySQL connection wrapper
pub struct MySqlConnection {
    pool: Pool,
    /// Stored at connect time so schema introspection methods can resolve
    /// `schema: None` to a concrete database name instead of relying on
    /// `DATABASE()` which returns NULL when no database was selected.
    database_name: Option<String>,
    cancel_epoch: Arc<AtomicU64>,
    /// The one connection user `query`/`execute` calls run on; see `session.rs`.
    /// The pool above serves schema introspection only.
    pub(crate) session: Arc<Session>,
    _ssh_tunnel: Option<MysqlSshTunnel>,
    pub(crate) schema_engine: SchemaEngine,
    closed: AtomicBool,
    in_flight: InFlight,
    /// Server flavor/version, populated on first access. See
    /// `MySqlConnection::mysql_server_features`.
    pub(crate) server_features: tokio::sync::OnceCell<crate::schema::MysqlServerFeatures>,
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

        // Without CLIENT_FOUND_ROWS, MySQL reports rows *changed* rather than rows
        // *matched*, so an UPDATE that sets a column to the value it already holds
        // returns 0 and is indistinguishable from a row that no longer exists.
        let mut opts_builder = OptsBuilder::from_opts(Opts::default())
            .ip_or_hostname(options.host.as_str())
            .tcp_port(options.port)
            .client_found_rows(true);

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

        let constraints = PoolConstraints::new(MIN_POOL_CONNECTIONS, MAX_POOL_CONNECTIONS)
            .ok_or_else(|| {
                ZqlzError::Connection(format!(
                    "Failed to configure MySQL pool constraints (min={}, max={})",
                    MIN_POOL_CONNECTIONS, MAX_POOL_CONNECTIONS
                ))
            })?;

        let pool_opts = PoolOpts::default()
            .with_constraints(constraints)
            .with_reset_connection(false);
        opts_builder = opts_builder.pool_opts(pool_opts);

        let opts: Opts = opts_builder.into();
        let connect_opts = opts.clone();
        let session_opts = opts.clone();

        // Pool creation and initial connection test must run on the Tokio runtime
        // because mysql_async internally uses tokio::spawn for pool management.
        let runtime = get_mysql_runtime();
        let pool = runtime
            .spawn(async move {
                let pool = Pool::new(opts);
                // Verify connectivity by acquiring and releasing a connection
                let _conn = pool
                    .get_conn()
                    .await
                    .map_err(|e| session::connect_error(&e, &connect_opts))?;
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
        let cancel_epoch = Arc::new(AtomicU64::new(0));
        let in_flight = InFlight::default();
        let schema_engine = SchemaEngine::new(
            Arc::new(MySqlCatalog::new(
                pool.clone(),
                database_name.clone(),
                cancel_epoch.clone(),
                in_flight.clone(),
            )),
            Arc::new(DefaultDialect),
        );
        let session = Arc::new(Session::new(
            session_opts,
            cancel_epoch.clone(),
            in_flight.clone(),
        ));
        Ok(Self {
            pool,
            database_name,
            cancel_epoch,
            session,
            _ssh_tunnel: options.ssh_tunnel,
            schema_engine,
            closed: AtomicBool::new(false),
            in_flight,
            server_features: tokio::sync::OnceCell::new(),
        })
    }

    /// Run `sql` on a pool connection, away from the user's session.
    ///
    /// Schema introspection goes through here: it never waits for a running user
    /// statement and never sees the session's `USE`, variables or open
    /// transaction. Within this crate `self.query(...)` resolves to this method
    /// (an inherent method shadows the `Connection` trait method of the same
    /// name); user-facing calls go through the trait, which uses the session.
    pub(crate) async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        run_mysql_query(
            self.pool.clone(),
            self.cancel_epoch.clone(),
            self.in_flight.clone(),
            sql,
            params,
        )
        .await
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
        // Emitted unquoted to preserve precision, so it must be a real number.
        Value::Decimal(v) => {
            if !Value::is_sql_numeric_literal(v) {
                return Err(ZqlzError::Query(format!(
                    "Invalid decimal literal: {}",
                    v
                )));
            }
            v.to_string()
        }
        Value::Array(arr) => {
            // MySQL doesn't have native array support, convert to JSON
            let json = serde_json::to_string(&serde_json::Value::Array(
                arr.iter().map(Value::to_json_value).collect(),
            ))
            .map_err(|error| ZqlzError::Query(format!("Failed to serialize array: {}", error)))?;
            format!("'{}'", json.replace("'", "''").replace("\\", "\\\\"))
        }
    })
}

/// Execute a query on a pooled MySQL connection and collect the result. Shared
/// by `MySqlConnection::query` and the schema-introspection adapter.
///
/// `_cancel_epoch` is accepted for the callers' sake but deliberately unused:
/// user cancellation targets the pinned session, and must not abort catalog
/// queries that happen to be running alongside it.
pub(crate) async fn run_mysql_query(
    pool: Pool,
    _cancel_epoch: Arc<AtomicU64>,
    in_flight: InFlight,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult> {
    let _in_flight = in_flight.enter();
    let final_sql = render_mysql_sql_with_params(sql, params)?;

    get_mysql_runtime()
        .spawn(async move {
            let mut conn = pool
                .get_conn()
                .await
                .map_err(|e| ZqlzError::Connection(format!("Failed to get MySQL connection: {}", e)))?;
            // Catalog queries raise no warnings worth a second round trip.
            session::run_statement(&mut conn, &final_sql, None, false).await
        })
        .await
        .map_err(|e| ZqlzError::Query(format!("MySQL query task failed: {}", e)))?
}

/// Substitute parameter values into MySQL SQL text.
///
/// Placeholders are located with a MySQL-aware scanner (`sql_text`) rather than
/// by string search, so a `?` or `$1` inside a string literal (including one
/// with a backslash-escaped quote), a `#`/`-- ` comment or a backtick identifier
/// is left alone. A plain `replacen` would rewrite the contents of
/// `WHERE note = '?'` and silently corrupt the statement.
fn render_mysql_sql_with_params(sql: &str, params: &[Value]) -> Result<String> {
    if params.is_empty() {
        return Ok(sql.to_string());
    }

    let placeholders = sql_text::placeholders(sql);
    if placeholders.is_empty() {
        return Ok(sql.to_string());
    }

    let mut result = String::with_capacity(sql.len());
    let mut last_end = 0usize;
    let mut next_positional = 0usize;

    for placeholder in &placeholders {
        let param_index = match &placeholder.kind {
            sql_text::PlaceholderKind::Dollar(position) => position.saturating_sub(1),
            sql_text::PlaceholderKind::Question => {
                let index = next_positional;
                next_positional += 1;
                index
            }
        };

        let Some(param) = params.get(param_index) else {
            continue;
        };

        result.push_str(&sql[last_end..placeholder.start]);
        result.push_str(&value_to_mysql_literal(param)?);
        last_end = placeholder.end;
    }

    result.push_str(&sql[last_end..]);

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

/// Convert mysql_async Value to our Value type, using column metadata to
/// correctly interpret byte strings from the text protocol.
fn mysql_value_to_value(val: mysql_async::Value, info: &MySqlColumnInfo) -> Value {
    match val {
        mysql_async::Value::NULL => Value::Null,
        mysql_async::Value::Bytes(bytes) => {
            let col_type = info.column_type;
            if col_type == ColumnType::MYSQL_TYPE_BIT {
                return Value::String(format_mysql_bit_value(&bytes, info.length));
            }

            if col_type == ColumnType::MYSQL_TYPE_GEOMETRY {
                return Value::String(format_mysql_geometry_value(&bytes));
            }

            // TEXT uses the BLOB type codes, and VARBINARY the VARCHAR ones: only
            // the binary charset means the bytes are not text.
            if matches!(
                col_type,
                ColumnType::MYSQL_TYPE_BLOB
                    | ColumnType::MYSQL_TYPE_TINY_BLOB
                    | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                    | ColumnType::MYSQL_TYPE_LONG_BLOB
                    | ColumnType::MYSQL_TYPE_VARCHAR
                    | ColumnType::MYSQL_TYPE_VAR_STRING
                    | ColumnType::MYSQL_TYPE_STRING
            ) && info.is_binary()
            {
                return Value::Bytes(bytes);
            }

            match String::from_utf8(bytes) {
                Ok(s) => match col_type {
                    ColumnType::MYSQL_TYPE_TINY
                    | ColumnType::MYSQL_TYPE_SHORT
                    | ColumnType::MYSQL_TYPE_LONG
                    | ColumnType::MYSQL_TYPE_LONGLONG
                    | ColumnType::MYSQL_TYPE_INT24
                    | ColumnType::MYSQL_TYPE_YEAR => match s.parse::<i64>() {
                        Ok(n) => Value::Int64(n),
                        // BIGINT UNSIGNED above i64::MAX: keep the exact digits as a number.
                        Err(_) if s.parse::<u64>().is_ok() => Value::Decimal(s),
                        Err(_) => Value::String(s),
                    },
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
                    // JSON stays as the server wrote it: re-serializing would
                    // drop its spacing, reorder keys and round big numbers.
                    _ => Value::String(s),
                },
                Err(error) => Value::Bytes(error.into_bytes()),
            }
        }
        mysql_async::Value::Int(i) => Value::Int64(i),
        mysql_async::Value::UInt(u) => match i64::try_from(u) {
            Ok(i) => Value::Int64(i),
            Err(_) => Value::Decimal(u.to_string()),
        },
        mysql_async::Value::Float(f) => Value::Float32(f),
        mysql_async::Value::Double(d) => Value::Float64(d),
        mysql_async::Value::Date(year, month, day, hour, min, sec, micro) => {
            let date = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32);
            if matches!(
                info.column_type,
                ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE
            ) {
                // A zero date ('0000-00-00') has no calendar date to hold.
                return date.map(Value::Date).unwrap_or_else(|| {
                    Value::String(format!("{:04}-{:02}-{:02}", year, month, day))
                });
            }
            // DATETIME/TIMESTAMP stay date-times even at exactly midnight.
            match date.and_then(|d| d.and_hms_micro_opt(hour as u32, min as u32, sec as u32, micro))
            {
                Some(dt) => Value::DateTime(dt),
                None => Value::String(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    year, month, day, hour, min, sec
                )),
            }
        }
        mysql_async::Value::Time(negative, days, hours, mins, secs, micros) => {
            let total_hours = days * 24 + (hours as u32);
            let sign = if negative { "-" } else { "" };
            let mut text = format!("{}{:02}:{:02}:{:02}", sign, total_hours, mins, secs);
            // Fractional digits follow the column's declared precision.
            if (1..=6).contains(&info.decimals) {
                let fraction = format!("{:06}", micros);
                text.push('.');
                text.push_str(&fraction[..info.decimals as usize]);
            }
            Value::String(text)
        }
    }
}

fn mysql_column_metadata(
    mysql_columns: &[mysql_async::Column],
) -> (Vec<ColumnMeta>, Vec<String>, Vec<MySqlColumnInfo>) {
    let mut columns = Vec::new();
    let mut column_names = Vec::new();
    let mut column_info = Vec::new();

    for (idx, column) in mysql_columns.iter().enumerate() {
        let name = column.name_str().to_string();
        column_names.push(name.clone());
        let info = MySqlColumnInfo {
            column_type: column.column_type(),
            flags: column.flags(),
            charset: column.character_set(),
            length: column.column_length(),
            decimals: column.decimals(),
        };
        column_info.push(info);

        let is_decimal = matches!(
            info.column_type,
            ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL
        );
        let is_char = matches!(
            info.column_type,
            ColumnType::MYSQL_TYPE_VARCHAR
                | ColumnType::MYSQL_TYPE_VAR_STRING
                | ColumnType::MYSQL_TYPE_STRING
        ) && !info.is_binary();

        columns.push(ColumnMeta {
            name,
            data_type: mysql_column_type_display(&info),
            nullable: true,
            ordinal: idx,
            max_length: Some(i64::from(if is_char {
                info.char_length()
            } else {
                info.length
            })),
            precision: is_decimal.then(|| info.decimal_precision() as i32),
            scale: is_decimal.then_some(i32::from(info.decimals)),
            auto_increment: false,
            default_value: None,
            comment: None,
            enum_values: None,
        });
    }

    (columns, column_names, column_info)
}

/// Render a BIT value as binary text as wide as the declared `BIT(n)`, so
/// `BIT(1)` reads `1` and `BIT(8)` holding 5 reads `00000101`.
fn format_mysql_bit_value(bytes: &[u8], width: u32) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    if bytes.len() > 8 {
        return bytes.iter().map(|byte| format!("{byte:08b}")).collect();
    }

    let value = bytes
        .iter()
        .fold(0_u64, |acc, byte| (acc << 8) | u64::from(*byte));
    let width = if width == 0 {
        bytes.len() * 8
    } else {
        width as usize
    };
    format!("{value:0width$b}")
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

type GeometryResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Where a WKB read is: the buffer, the offset, and whether x/y print swapped
/// (latitude-first geographic SRIDs).
struct WkbReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    swap_xy: bool,
}

/// Render the internal geometry format (4-byte SRID + WKB) the way `ST_AsText` does.
fn parse_mysql_geometry_value(bytes: &[u8]) -> GeometryResult<String> {
    if bytes.len() < 5 {
        return Err("invalid MySQL geometry payload: too short".into());
    }

    let srid = u32::from_le_bytes(bytes[..4].try_into()?);
    let mut reader = WkbReader {
        bytes: &bytes[4..],
        offset: 0,
        swap_xy: MYSQL_LAT_LONG_SRIDS.contains(&srid),
    };
    let (tag, body) = parse_wkb_geometry_at(&mut reader)?;
    Ok(wkt_with_tag(tag, &body))
}

/// `TAG(body)`, or `TAG EMPTY` for an empty collection (whose body is empty).
fn wkt_with_tag(tag: &str, body: &str) -> String {
    if body.is_empty() {
        format!("{tag} EMPTY")
    } else {
        format!("{tag}{body}")
    }
}

/// Parse one WKB geometry into its WKT tag and parenthesized body. The body is
/// kept apart from the tag because the members of a MULTIPOINT,
/// MULTILINESTRING or MULTIPOLYGON are written without theirs.
fn parse_wkb_geometry_at(reader: &mut WkbReader) -> GeometryResult<(&'static str, String)> {
    let little_endian = match read_u8(reader)? {
        0 => false,
        1 => true,
        _ => return Err("invalid WKB byte order".into()),
    };
    let geometry_type = read_u32(reader, little_endian)?;

    match geometry_type {
        1 => Ok(("POINT", format!("({})", read_xy(reader, little_endian)?))),
        2 => Ok(("LINESTRING", parse_wkb_points(reader, little_endian)?)),
        3 => {
            let ring_count = read_u32(reader, little_endian)? as usize;
            let mut rings = Vec::new();
            for _ in 0..ring_count {
                rings.push(parse_wkb_points(reader, little_endian)?);
            }
            Ok(("POLYGON", format!("({})", rings.join(","))))
        }
        4 => Ok(("MULTIPOINT", parse_wkb_members(reader, little_endian, false)?)),
        5 => Ok((
            "MULTILINESTRING",
            parse_wkb_members(reader, little_endian, false)?,
        )),
        6 => Ok((
            "MULTIPOLYGON",
            parse_wkb_members(reader, little_endian, false)?,
        )),
        7 => Ok((
            "GEOMETRYCOLLECTION",
            parse_wkb_members(reader, little_endian, true)?,
        )),
        _ => Err("unsupported WKB geometry type".into()),
    }
}

/// A point count followed by that many points, as `(x y,x y)`.
fn parse_wkb_points(reader: &mut WkbReader, little_endian: bool) -> GeometryResult<String> {
    let point_count = read_u32(reader, little_endian)? as usize;
    let mut points = Vec::new();
    for _ in 0..point_count {
        points.push(read_xy(reader, little_endian)?);
    }
    Ok(format!("({})", points.join(",")))
}

/// A member count followed by that many geometries. Collection members keep
/// their tag (`keep_tags`), the other multi geometries' members do not.
/// Empty yields an empty body, which `wkt_with_tag` renders as `EMPTY`.
fn parse_wkb_members(
    reader: &mut WkbReader,
    little_endian: bool,
    keep_tags: bool,
) -> GeometryResult<String> {
    let geometry_count = read_u32(reader, little_endian)? as usize;
    if geometry_count == 0 {
        return Ok(String::new());
    }
    let mut geometries = Vec::new();
    for _ in 0..geometry_count {
        let (tag, body) = parse_wkb_geometry_at(reader)?;
        geometries.push(if keep_tags {
            wkt_with_tag(tag, &body)
        } else {
            body
        });
    }
    Ok(format!("({})", geometries.join(",")))
}

fn read_xy(reader: &mut WkbReader, little_endian: bool) -> GeometryResult<String> {
    let x = format_mysql_float(read_f64(reader, little_endian)?);
    let y = format_mysql_float(read_f64(reader, little_endian)?);
    Ok(if reader.swap_xy {
        format!("{y} {x}")
    } else {
        format!("{x} {y}")
    })
}

fn read_u8(reader: &mut WkbReader) -> GeometryResult<u8> {
    let value = *reader
        .bytes
        .get(reader.offset)
        .ok_or("invalid WKB payload: missing byte")?;
    reader.offset += 1;
    Ok(value)
}

fn read_u32(reader: &mut WkbReader, little_endian: bool) -> GeometryResult<u32> {
    let raw: [u8; 4] = reader
        .bytes
        .get(reader.offset..reader.offset + 4)
        .ok_or("invalid WKB payload: truncated u32")?
        .try_into()?;
    reader.offset += 4;
    Ok(if little_endian {
        u32::from_le_bytes(raw)
    } else {
        u32::from_be_bytes(raw)
    })
}

fn read_f64(reader: &mut WkbReader, little_endian: bool) -> GeometryResult<f64> {
    let raw: [u8; 8] = reader
        .bytes
        .get(reader.offset..reader.offset + 8)
        .ok_or("invalid WKB payload: truncated f64")?
        .try_into()?;
    reader.offset += 8;
    Ok(if little_endian {
        f64::from_le_bytes(raw)
    } else {
        f64::from_be_bytes(raw)
    })
}

/// Coordinates print as MySQL does: shortest round-trip digits, switching to
/// exponent form from 1e15 up (`1e20`, `1.2345678901234568e17`).
fn format_mysql_float(value: f64) -> String {
    if value.abs() >= 1e15 {
        format!("{value:e}")
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_flight_tracks_nested_occupancy() {
        let in_flight = InFlight::default();
        assert!(!in_flight.is_busy());

        let first = in_flight.enter();
        assert!(in_flight.is_busy());

        let second = in_flight.enter();
        drop(first);
        assert!(
            in_flight.is_busy(),
            "connection is still occupied while a second guard is held"
        );

        drop(second);
        assert!(!in_flight.is_busy());
    }

    fn column_info(
        column_type: ColumnType,
        charset: u16,
        length: u32,
        decimals: u8,
        flags: ColumnFlags,
    ) -> MySqlColumnInfo {
        MySqlColumnInfo {
            column_type,
            flags,
            charset,
            length,
            decimals,
        }
    }

    const UTF8MB4: u16 = 255;
    const BINARY: u16 = 63;

    #[test]
    fn mysql_column_type_display_uses_sql_type_names() {
        let none = ColumnFlags::empty();
        let display = |column_type, charset, length, decimals, flags| {
            mysql_column_type_display(&column_info(column_type, charset, length, decimals, flags))
        };
        // VARCHAR(50) utf8mb4: the protocol length is bytes.
        assert_eq!(display(ColumnType::MYSQL_TYPE_VAR_STRING, UTF8MB4, 200, 0, none), "varchar(50)");
        assert_eq!(display(ColumnType::MYSQL_TYPE_VAR_STRING, 8, 255, 0, none), "varchar(255)");
        assert_eq!(display(ColumnType::MYSQL_TYPE_VAR_STRING, BINARY, 10, 0, none), "varbinary(10)");
        assert_eq!(display(ColumnType::MYSQL_TYPE_STRING, UTF8MB4, 20, 0, none), "char(5)");
        assert_eq!(display(ColumnType::MYSQL_TYPE_STRING, BINARY, 4, 0, ColumnFlags::BINARY_FLAG), "binary(4)");
        assert_eq!(display(ColumnType::MYSQL_TYPE_STRING, UTF8MB4, 12, 0, ColumnFlags::ENUM_FLAG), "enum");
        assert_eq!(display(ColumnType::MYSQL_TYPE_STRING, UTF8MB4, 12, 0, ColumnFlags::SET_FLAG), "set");
        // Expressions report absurd lengths; claim none.
        assert_eq!(display(ColumnType::MYSQL_TYPE_VAR_STRING, 33, 262140, 0, none), "varchar");
        // TEXT shares the BLOB codes; the charset says which it is.
        assert_eq!(display(ColumnType::MYSQL_TYPE_BLOB, UTF8MB4, 262140, 0, none), "text");
        assert_eq!(display(ColumnType::MYSQL_TYPE_BLOB, BINARY, 65535, 0, none), "blob");
        assert_eq!(display(ColumnType::MYSQL_TYPE_BLOB, UTF8MB4, 1020, 0, none), "tinytext");
        assert_eq!(display(ColumnType::MYSQL_TYPE_BLOB, UTF8MB4, 67_108_860, 0, none), "mediumtext");
        assert_eq!(display(ColumnType::MYSQL_TYPE_BLOB, UTF8MB4, u32::MAX, 0, none), "longtext");
        assert_eq!(display(ColumnType::MYSQL_TYPE_BLOB, BINARY, 255, 0, none), "tinyblob");
        assert_eq!(display(ColumnType::MYSQL_TYPE_BLOB, BINARY, u32::MAX, 0, none), "longblob");
        // DECIMAL(20,6) / DECIMAL(10,0): the length counts sign and point.
        assert_eq!(display(ColumnType::MYSQL_TYPE_NEWDECIMAL, BINARY, 22, 6, none), "decimal(20,6)");
        assert_eq!(display(ColumnType::MYSQL_TYPE_NEWDECIMAL, BINARY, 11, 0, none), "decimal(10,0)");
        assert_eq!(
            display(ColumnType::MYSQL_TYPE_NEWDECIMAL, BINARY, 21, 6, ColumnFlags::UNSIGNED_FLAG),
            "decimal(20,6) unsigned"
        );
        assert_eq!(display(ColumnType::MYSQL_TYPE_TINY, BINARY, 1, 0, none), "tinyint(1)");
        assert_eq!(display(ColumnType::MYSQL_TYPE_TINY, BINARY, 4, 0, none), "tinyint");
        assert_eq!(
            display(ColumnType::MYSQL_TYPE_LONGLONG, BINARY, 20, 0, ColumnFlags::UNSIGNED_FLAG),
            "bigint unsigned"
        );
        assert_eq!(display(ColumnType::MYSQL_TYPE_BIT, BINARY, 8, 0, none), "bit(8)");
        assert_eq!(display(ColumnType::MYSQL_TYPE_DATETIME, BINARY, 26, 6, none), "datetime(6)");
        assert_eq!(display(ColumnType::MYSQL_TYPE_DATETIME, BINARY, 19, 0, none), "datetime");
        assert_eq!(display(ColumnType::MYSQL_TYPE_TIME, BINARY, 14, 3, none), "time(3)");
        assert_eq!(display(ColumnType::MYSQL_TYPE_JSON, UTF8MB4, 0, 0, none), "json");
    }

    #[test]
    fn mysql_value_conversion_preserves_decimal_and_text_types() {
        let convert = |value: &[u8], column_type, charset| {
            mysql_value_to_value(
                mysql_async::Value::Bytes(value.to_vec()),
                &column_info(column_type, charset, 0, 0, ColumnFlags::empty()),
            )
        };
        assert_eq!(
            convert(b"1234567890.123456", ColumnType::MYSQL_TYPE_NEWDECIMAL, BINARY),
            Value::Decimal("1234567890.123456".to_string())
        );
        // JSON keeps the server's own text (spacing, key order).
        let json = br#"{"b": [1, 2, {"c": null}], "aa": "d"}"#;
        assert_eq!(
            convert(json, ColumnType::MYSQL_TYPE_JSON, UTF8MB4),
            Value::String(String::from_utf8(json.to_vec()).unwrap())
        );
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Bytes(b"active".to_vec()),
                &column_info(
                    ColumnType::MYSQL_TYPE_STRING,
                    BINARY,
                    0,
                    0,
                    ColumnFlags::ENUM_FLAG
                ),
            ),
            Value::String("active".to_string())
        );
        // TEXT in every size is text, BLOB is bytes.
        for column_type in [
            ColumnType::MYSQL_TYPE_TINY_BLOB,
            ColumnType::MYSQL_TYPE_BLOB,
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
            ColumnType::MYSQL_TYPE_LONG_BLOB,
        ] {
            assert_eq!(
                convert("text wörld".as_bytes(), column_type, UTF8MB4),
                Value::String("text wörld".to_string())
            );
            assert_eq!(convert(b"", column_type, UTF8MB4), Value::String(String::new()));
            assert_eq!(convert(b"ab", column_type, BINARY), Value::Bytes(b"ab".to_vec()));
        }
        // BINARY / VARBINARY are bytes even when they happen to be valid UTF-8.
        for column_type in [
            ColumnType::MYSQL_TYPE_STRING,
            ColumnType::MYSQL_TYPE_VAR_STRING,
            ColumnType::MYSQL_TYPE_VARCHAR,
        ] {
            assert_eq!(convert(b"\n\x0b", column_type, BINARY), Value::Bytes(b"\n\x0b".to_vec()));
            assert_eq!(convert(&[0xff, 0x00], column_type, BINARY), Value::Bytes(vec![0xff, 0x00]));
        }
        // Text that is not valid UTF-8 falls back to bytes rather than losing data.
        assert_eq!(
            convert(&[0xff, 0x00], ColumnType::MYSQL_TYPE_VAR_STRING, UTF8MB4),
            Value::Bytes(vec![0xff, 0x00])
        );
        // Integers keep their digits, including BIGINT UNSIGNED beyond i64.
        assert_eq!(convert(b"-5", ColumnType::MYSQL_TYPE_LONG, BINARY), Value::Int64(-5));
        assert_eq!(
            convert(b"18446744073709551615", ColumnType::MYSQL_TYPE_LONGLONG, BINARY),
            Value::Decimal("18446744073709551615".to_string())
        );
        assert_eq!(convert(b"3.14159", ColumnType::MYSQL_TYPE_FLOAT, BINARY).to_string(), "3.14159");
        // Dates are the server's own text, zero dates included.
        assert_eq!(
            convert(b"0000-00-00", ColumnType::MYSQL_TYPE_DATE, BINARY),
            Value::String("0000-00-00".to_string())
        );
        assert_eq!(
            convert(b"-838:59:59", ColumnType::MYSQL_TYPE_TIME, BINARY),
            Value::String("-838:59:59".to_string())
        );
    }

    #[test]
    fn mysql_binary_protocol_values_follow_the_column_type() {
        let info = |column_type, decimals| {
            column_info(column_type, BINARY, 0, decimals, ColumnFlags::empty())
        };
        let midnight = chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        // A DATETIME at midnight is still a DATETIME.
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Date(2024, 2, 29, 0, 0, 0, 0),
                &info(ColumnType::MYSQL_TYPE_DATETIME, 0),
            ),
            Value::DateTime(midnight.and_hms_opt(0, 0, 0).unwrap())
        );
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Date(2024, 2, 29, 0, 0, 0, 0),
                &info(ColumnType::MYSQL_TYPE_DATE, 0),
            ),
            Value::Date(midnight)
        );
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Date(0, 0, 0, 0, 0, 0, 0),
                &info(ColumnType::MYSQL_TYPE_DATE, 0),
            ),
            Value::String("0000-00-00".to_string())
        );
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Time(true, 34, 22, 59, 59, 0),
                &info(ColumnType::MYSQL_TYPE_TIME, 0),
            ),
            Value::String("-838:59:59".to_string())
        );
        assert_eq!(
            mysql_value_to_value(
                mysql_async::Value::Time(false, 0, 0, 0, 0, 0),
                &info(ColumnType::MYSQL_TYPE_TIME, 3),
            ),
            Value::String("00:00:00.000".to_string())
        );
        assert_eq!(
            mysql_value_to_value(mysql_async::Value::UInt(u64::MAX), &info(ColumnType::MYSQL_TYPE_LONGLONG, 0)),
            Value::Decimal(u64::MAX.to_string())
        );
    }

    #[test]
    fn mysql_bit_value_renders_binary_text() {
        let bit = |bytes: &[u8], width: u32| {
            mysql_value_to_value(
                mysql_async::Value::Bytes(bytes.to_vec()),
                &column_info(ColumnType::MYSQL_TYPE_BIT, BINARY, width, 0, ColumnFlags::empty()),
            )
        };
        // Without a declared width, every byte prints in full.
        assert_eq!(bit(&[0b0000_1010], 0), Value::String("00001010".to_string()));
        assert_eq!(bit(&[1], 1), Value::String("1".to_string()));
        assert_eq!(bit(&[0], 1), Value::String("0".to_string()));
        assert_eq!(bit(&[0b1010_1010], 8), Value::String("10101010".to_string()));
        assert_eq!(bit(&[0, 5], 12), Value::String("000000000101".to_string()));
        assert_eq!(bit(&[0; 8], 64), Value::String("0".repeat(64)));
        assert_eq!(bit(&[0xff; 8], 64), Value::String("1".repeat(64)));
    }

    fn wkb_header(srid: u32) -> Vec<u8> {
        srid.to_le_bytes().to_vec()
    }

    fn wkb_geometry(out: &mut Vec<u8>, geometry_type: u32) {
        out.push(1);
        out.extend_from_slice(&geometry_type.to_le_bytes());
    }

    fn wkb_count(out: &mut Vec<u8>, count: u32) {
        out.extend_from_slice(&count.to_le_bytes());
    }

    fn wkb_point(out: &mut Vec<u8>, x: f64, y: f64) {
        out.extend_from_slice(&x.to_le_bytes());
        out.extend_from_slice(&y.to_le_bytes());
    }

    fn wkb_member_point(out: &mut Vec<u8>, x: f64, y: f64) {
        wkb_geometry(out, 1);
        wkb_point(out, x, y);
    }

    fn wkb_member_line(out: &mut Vec<u8>, points: &[(f64, f64)]) {
        wkb_geometry(out, 2);
        wkb_count(out, points.len() as u32);
        for (x, y) in points {
            wkb_point(out, *x, *y);
        }
    }

    fn wkt(bytes: Vec<u8>) -> String {
        match mysql_value_to_value(
            mysql_async::Value::Bytes(bytes),
            &column_info(ColumnType::MYSQL_TYPE_GEOMETRY, BINARY, 0, 0, ColumnFlags::empty()),
        ) {
            Value::String(text) => text,
            other => panic!("geometry decoded to {other:?}"),
        }
    }

    #[test]
    fn mysql_geometry_value_renders_wkt_or_hex_fallback() {
        let mut point = wkb_header(0);
        wkb_member_point(&mut point, 26.0, 42.0);
        assert_eq!(wkt(point), "POINT(26 42)");

        let mut linestring = wkb_header(0);
        wkb_member_line(&mut linestring, &[(0.0, 0.0), (2.0, 2.0)]);
        assert_eq!(wkt(linestring), "LINESTRING(0 0,2 2)");

        assert_eq!(wkt(vec![1, 2, 3]), "0x010203");

        // Fractions, negatives and exponent form like ST_AsText.
        let mut point = wkb_header(0);
        wkb_member_point(&mut point, -4.25, 0.1);
        assert_eq!(wkt(point), "POINT(-4.25 0.1)");
        let mut point = wkb_header(0);
        wkb_member_point(&mut point, 1e20, 1.5e-7);
        assert_eq!(wkt(point), "POINT(1e20 0.00000015)");
        let mut point = wkb_header(0);
        wkb_member_point(&mut point, 123456789012345678.0, 100000000000000.0);
        assert_eq!(wkt(point), "POINT(1.2345678901234568e17 100000000000000)");
    }

    #[test]
    fn mysql_geometry_value_matches_st_as_text_for_collections() {
        // MULTIPOINT((0 0),(1 1)): members drop their tag.
        let mut multipoint = wkb_header(0);
        wkb_geometry(&mut multipoint, 4);
        wkb_count(&mut multipoint, 2);
        wkb_member_point(&mut multipoint, 0.0, 0.0);
        wkb_member_point(&mut multipoint, 1.0, 1.0);
        assert_eq!(wkt(multipoint), "MULTIPOINT((0 0),(1 1))");

        // MULTILINESTRING((0 0,1 1),(2 2,3 3))
        let mut multiline = wkb_header(0);
        wkb_geometry(&mut multiline, 5);
        wkb_count(&mut multiline, 2);
        wkb_member_line(&mut multiline, &[(0.0, 0.0), (1.0, 1.0)]);
        wkb_member_line(&mut multiline, &[(2.0, 2.0), (3.0, 3.0)]);
        assert_eq!(wkt(multiline), "MULTILINESTRING((0 0,1 1),(2 2,3 3))");

        // MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((5 5,6 5,6 6,5 5),(5 5,6 5,6 6,5 5)))
        let ring = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)];
        let mut multipolygon = wkb_header(0);
        wkb_geometry(&mut multipolygon, 6);
        wkb_count(&mut multipolygon, 2);
        for ring_count in [1_u32, 2] {
            wkb_geometry(&mut multipolygon, 3);
            wkb_count(&mut multipolygon, ring_count);
            for _ in 0..ring_count {
                wkb_count(&mut multipolygon, ring.len() as u32);
                for (x, y) in ring {
                    wkb_point(&mut multipolygon, x, y);
                }
            }
        }
        assert_eq!(
            wkt(multipolygon),
            "MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((0 0,1 0,1 1,0 0),(0 0,1 0,1 1,0 0)))"
        );

        // Collection members keep their tags, nested collections included.
        let mut collection = wkb_header(0);
        wkb_geometry(&mut collection, 7);
        wkb_count(&mut collection, 3);
        wkb_member_point(&mut collection, 1.0, 1.0);
        wkb_member_line(&mut collection, &[(0.0, 0.0), (1.0, 1.0)]);
        wkb_geometry(&mut collection, 7);
        wkb_count(&mut collection, 1);
        wkb_geometry(&mut collection, 4);
        wkb_count(&mut collection, 1);
        wkb_member_point(&mut collection, 2.0, 3.0);
        assert_eq!(
            wkt(collection),
            "GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1),GEOMETRYCOLLECTION(MULTIPOINT((2 3))))"
        );

        // Empty collections.
        let mut empty = wkb_header(0);
        wkb_geometry(&mut empty, 7);
        wkb_count(&mut empty, 0);
        assert_eq!(wkt(empty.clone()), "GEOMETRYCOLLECTION EMPTY");
        let mut nested = wkb_header(0);
        wkb_geometry(&mut nested, 7);
        wkb_count(&mut nested, 1);
        nested.extend_from_slice(&empty[4..]);
        assert_eq!(wkt(nested), "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY)");
    }

    #[test]
    fn mysql_geometry_value_swaps_axes_for_latitude_first_srids() {
        // ST_GeomFromText('POINT(1 2)', 4326) stores x=2, y=1 and prints POINT(1 2).
        let mut point = wkb_header(4326);
        wkb_member_point(&mut point, 2.0, 1.0);
        assert_eq!(wkt(point), "POINT(1 2)");
        // A projected SRID prints as stored.
        let mut point = wkb_header(3857);
        wkb_member_point(&mut point, 10.0, 20.0);
        assert_eq!(wkt(point), "POINT(10 20)");
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
    fn mysql_decimal_literal_rejects_sql_payload() {
        let result = value_to_mysql_literal(&Value::Decimal("0 WHERE 1=1 -- ".to_string()));

        assert!(
            result.is_err(),
            "a non-numeric decimal must not be emitted unquoted"
        );
    }

    #[test]
    fn mysql_decimal_literal_preserves_precision() {
        let literal = value_to_mysql_literal(&Value::Decimal("-6.0000".to_string()))
            .expect("numeric decimal serializes");

        assert_eq!(literal, "-6.0000");
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

    /// Substitution used to be a plain `replacen`, which rewrote any `?` it
    /// found — including ones inside string literals, where they are data.
    #[test]
    fn mysql_params_ignore_placeholders_inside_string_literals() {
        let sql = render_mysql_sql_with_params(
            "SELECT * FROM t WHERE note = '?' AND id = ?",
            &[Value::Int32(7)],
        )
        .expect("params render");

        assert_eq!(sql, "SELECT * FROM t WHERE note = '?' AND id = 7");
    }

    #[test]
    fn mysql_params_ignore_placeholders_inside_comments() {
        let sql = render_mysql_sql_with_params(
            "SELECT id -- what about ?\nFROM t WHERE id = ?",
            &[Value::Int32(3)],
        )
        .expect("params render");

        assert_eq!(sql, "SELECT id -- what about ?\nFROM t WHERE id = 3");
    }

    /// A literal dollar amount is not a positional placeholder.
    #[test]
    fn mysql_params_ignore_dollar_text_inside_literals() {
        let sql = render_mysql_sql_with_params(
            "SELECT * FROM t WHERE price = '$1' AND id = ?",
            &[Value::Int32(5)],
        )
        .expect("params render");

        assert_eq!(sql, "SELECT * FROM t WHERE price = '$1' AND id = 5");
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
