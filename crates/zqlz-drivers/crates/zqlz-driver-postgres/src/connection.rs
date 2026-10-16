//! PostgreSQL connection implementation

mod connection_impl;
mod money;
mod pg_value;
mod row_conversion;
mod transaction;
mod value_format;
mod value_literal;

use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::fs;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_postgres::{CancelToken, Client, NoTls, SimpleQueryMessage, types::ToSql};

use zqlz_schema_engine::{DefaultDialect, SchemaEngine};

use crate::PostgresSshTunnel;
use crate::schema::PostgresCatalog;
use zqlz_core::{
    CellUpdateRequest, ColumnMeta, QueryCancelHandle, QueryResult, Result, Row, StatementResult,
    Value, ZqlzError, sql_protected_range_at, sql_protected_ranges,
};

use pg_value::PgValue;
use money::{MoneyFormatCache, with_money_format};
use row_conversion::{money_format_for, postgres_to_value, resolve_oid_alias_names};

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

/// `tokio_postgres::Error`'s Display drops its cause ("error connecting to
/// server", "error communicating with the server"), so append the source chain.
fn postgres_error_with_causes(error: &tokio_postgres::Error) -> String {
    let mut message = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

/// Connect-time failures: the server's own message (wrong database, bad password,
/// pg_hba rejection, too many connections) or the io cause (refused, DNS, timeout),
/// prefixed with the target so the user knows what was attempted.
fn format_postgres_connect_error(error: &tokio_postgres::Error, host: &str, port: u16) -> String {
    let detail = if let Some(db_error) = error.as_db_error() {
        let mut message = format!("{} (SQLSTATE {})", db_error.message(), db_error.code().code());
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
        message
    } else {
        postgres_error_with_causes(error)
    };
    format!("Failed to connect to PostgreSQL at {host}:{port}: {detail}")
}

fn format_postgres_error(error: &tokio_postgres::Error) -> String {
    let Some(db_error) = error.as_db_error() else {
        return postgres_error_with_causes(error);
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

/// Collect the leading bare (non string/comment/quoted-identifier) words of a
/// statement, uppercased. Bounded so that scanning a large script stays cheap.
fn leading_sql_words(sql: &str, limit: usize) -> Vec<String> {
    let ranges = sql_protected_ranges(sql);
    let mut range_index = 0usize;
    let bytes = sql.as_bytes();
    let mut words = Vec::new();
    let mut index = 0usize;

    while index < bytes.len() && words.len() < limit {
        if let Some(range) = sql_protected_range_at(index, &ranges, &mut range_index) {
            index = range.end;
            continue;
        }

        if bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_' {
            let start = index;
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                && sql_protected_range_at(index, &ranges, &mut range_index).is_none()
            {
                index += 1;
            }
            words.push(sql[start..index].to_ascii_uppercase());
        } else {
            index += 1;
        }
    }

    words
}

/// First bare keyword of a statement, uppercased, ignoring leading whitespace
/// and comments.
fn leading_sql_keyword(sql: &str) -> Option<String> {
    leading_sql_words(sql, 1).into_iter().next()
}

/// PostgreSQL statements that cannot be executed through the extended query
/// protocol inside a transaction block, and are therefore run through the
/// simple query protocol as standalone statements.
fn requires_simple_query_protocol(sql: &str) -> bool {
    let words = leading_sql_words(sql, 8);
    let Some(first) = words.first().map(String::as_str) else {
        return false;
    };

    match first {
        "VACUUM" | "REINDEX" | "CLUSTER" | "DISCARD" => return true,
        "CREATE" | "DROP" | "ALTER" => {}
        _ => return false,
    }

    let mut index = 1usize;
    while let Some(word) = words.get(index) {
        match word.as_str() {
            "OR" | "REPLACE" | "IF" | "NOT" | "EXISTS" | "TEMP" | "TEMPORARY" | "UNLOGGED"
            | "GLOBAL" | "LOCAL" | "UNIQUE" => index += 1,
            _ => break,
        }
    }

    let object = words.get(index).map(String::as_str);

    if matches!(
        object,
        Some("DATABASE") | Some("TABLESPACE") | Some("SUBSCRIPTION")
    ) {
        return true;
    }

    if first == "ALTER" && object == Some("SYSTEM") {
        return true;
    }

    words
        .get(index..(index + 3).min(words.len()))
        .is_some_and(|window| window.iter().any(|word| word == "CONCURRENTLY"))
}

/// Run a statement through the simple query protocol, returning the number of
/// affected rows reported by the server.
async fn run_postgres_simple_statement(client: &Client, sql: &str) -> Result<u64> {
    let messages = client.simple_query(sql).await.map_err(|e| {
        let message = format_postgres_error(&e);
        ZqlzError::Query(format!("Failed to execute statement: {}", message))
    })?;

    let mut affected_rows = 0u64;
    for message in messages {
        if let SimpleQueryMessage::CommandComplete(rows) = message {
            affected_rows = affected_rows.saturating_add(rows);
        }
    }

    Ok(affected_rows)
}

fn simple_query_parameters_error(sql: &str) -> ZqlzError {
    let keyword = leading_sql_keyword(sql).unwrap_or_else(|| "This".to_string());
    ZqlzError::Query(format!(
        "{} statements do not accept bound parameters; inline the values into the SQL text",
        keyword
    ))
}

/// Server notices (`RAISE NOTICE`, `VACUUM VERBOSE`, ...) received on the
/// connection task, waiting to be attached to the statement that raised them.
pub(crate) type NoticeBuffer = Arc<std::sync::Mutex<Vec<String>>>;

/// Upper bound on buffered notices so a runaway loop of `RAISE NOTICE` cannot
/// grow memory without limit.
const MAX_BUFFERED_NOTICES: usize = 1_000;

/// Upper bound on rows collected from `COPY ... TO STDOUT`.
const COPY_OUT_ROW_LIMIT: usize = 10_000;

/// Render a server notice like psql does: `SEVERITY: message`, then any
/// `DETAIL:` / `HINT:` lines.
fn format_postgres_notice(notice: &tokio_postgres::error::DbError) -> String {
    let mut text = format!("{}: {}", notice.severity(), notice.message());
    if let Some(detail) = notice.detail().filter(|detail| !detail.trim().is_empty()) {
        text.push_str(&format!("\nDETAIL: {}", detail));
    }
    if let Some(hint) = notice.hint().filter(|hint| !hint.trim().is_empty()) {
        text.push_str(&format!("\nHINT: {}", hint));
    }
    text
}

fn take_postgres_notices(notices: &NoticeBuffer) -> Vec<String> {
    std::mem::take(&mut *notices.lock().unwrap_or_else(|e| e.into_inner()))
}

/// Drive the tokio-postgres connection, recording server notices and marking
/// the connection closed once it terminates (after which the client is
/// permanently unusable, which `is_closed` and the heartbeat rely on).
fn spawn_postgres_connection_task<T>(
    runtime: &tokio::runtime::Runtime,
    mut connection: tokio_postgres::Connection<tokio_postgres::Socket, T>,
    closed: Arc<AtomicBool>,
    notices: NoticeBuffer,
) where
    T: tokio_postgres::tls::TlsStream + Unpin + Send + 'static,
{
    use futures::StreamExt;

    runtime.spawn(async move {
        let mut messages = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
        let outcome = loop {
            match messages.next().await {
                Some(Ok(tokio_postgres::AsyncMessage::Notice(notice))) => {
                    let text = format_postgres_notice(&notice);
                    tracing::debug!(notice = %text, "PostgreSQL notice");
                    let mut buffer = notices.lock().unwrap_or_else(|e| e.into_inner());
                    if buffer.len() < MAX_BUFFERED_NOTICES {
                        buffer.push(text);
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => break Err(e),
                None => break Ok(()),
            }
        };
        match outcome {
            Ok(()) => tracing::info!("PostgreSQL connection task ended"),
            Err(e) => tracing::error!(error = %e, "PostgreSQL connection error"),
        }
        closed.store(true, Ordering::SeqCst);
    });
}

#[derive(Debug, PartialEq, Eq)]
enum CopyStdioDirection {
    ToStdout,
    FromStdin,
}

/// Detect `COPY ... TO STDOUT` / `COPY ... FROM STDIN`. Those switch the
/// connection into COPY mode, which the row-oriented extended protocol path
/// cannot drive. File/program COPY is an ordinary command and returns `None`.
fn copy_stdio_direction(sql: &str) -> Option<CopyStdioDirection> {
    if leading_sql_keyword(sql).as_deref() != Some("COPY") {
        return None;
    }

    let words = leading_sql_words(sql, usize::MAX);
    words.windows(2).find_map(|pair| match (pair[0].as_str(), pair[1].as_str()) {
        ("TO", "STDOUT") => Some(CopyStdioDirection::ToStdout),
        ("FROM", "STDIN") => Some(CopyStdioDirection::FromStdin),
        _ => None,
    })
}

fn postgres_text_column(name: &str) -> ColumnMeta {
    ColumnMeta {
        name: name.to_string(),
        data_type: "text".to_string(),
        nullable: true,
        ordinal: 0,
        max_length: None,
        precision: None,
        scale: None,
        auto_increment: false,
        default_value: None,
        comment: None,
        enum_values: None,
    }
}

/// Run `COPY ... TO STDOUT` and return the emitted text lines as a one-column
/// result, truncated to `COPY_OUT_ROW_LIMIT` rows.
async fn run_postgres_copy_out(client: &Client, sql: &str) -> Result<QueryResult> {
    use futures::TryStreamExt;

    let start_time = std::time::Instant::now();
    let stream = client.copy_out(sql).await.map_err(|e| {
        ZqlzError::Query(format!(
            "Failed to execute statement: {}",
            format_postgres_error(&e)
        ))
    })?;
    futures::pin_mut!(stream);

    let mut lines: Vec<String> = Vec::new();
    let mut pending = String::new();
    let mut truncated = false;
    'chunks: while let Some(chunk) = stream.try_next().await.map_err(|e| {
        ZqlzError::Query(format!(
            "Failed to execute statement: {}",
            format_postgres_error(&e)
        ))
    })? {
        pending.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(newline) = pending.find('\n') {
            if lines.len() >= COPY_OUT_ROW_LIMIT {
                truncated = true;
                break 'chunks;
            }
            lines.push(pending[..newline].to_string());
            pending.drain(..=newline);
        }
    }
    if !truncated && !pending.is_empty() {
        lines.push(pending);
    }

    let rows: Vec<Row> = lines
        .into_iter()
        .map(|line| Row::new(vec!["copy".to_string()], vec![Value::String(line)]))
        .collect();
    let total_rows = rows.len() as u64;
    // Dropping the stream mid-copy is safe: the connection task keeps paging
    // through the remaining server messages.
    let warnings = if truncated {
        vec![format!(
            "COPY output truncated to the first {} rows",
            COPY_OUT_ROW_LIMIT
        )]
    } else {
        Vec::new()
    };

    Ok(QueryResult {
        id: uuid::Uuid::new_v4(),
        columns: vec![postgres_text_column("copy")],
        rows,
        total_rows: Some(total_rows),
        is_estimated_total: false,
        affected_rows: 0,
        execution_time_ms: start_time.elapsed().as_millis() as u64,
        warnings,
    })
}

/// Execute one statement and return whatever the server answered with.
///
/// Whether the statement produces a result set is decided by the server (the
/// prepared statement's columns), never by the leading keyword: `INSERT ...
/// RETURNING`, `CALL` with OUT/INOUT arguments, `EXECUTE`, `FETCH`, data-modifying
/// CTEs and `MERGE ... RETURNING` all yield rows, while commands without a
/// result set report `affected_rows` instead. Notices raised while it ran are
/// returned in `warnings`. The caller must hold the client lock. `noun`
/// ("query" or "statement") only shapes error messages.
pub(crate) async fn run_postgres_statement(
    client: &Client,
    notices: &NoticeBuffer,
    money_format: &MoneyFormatCache,
    sql: &str,
    params: &[Value],
    noun: &str,
) -> Result<QueryResult> {
    use futures::TryStreamExt;

    // `lc_monetary` is session state, so a change invalidates the probed layout.
    if sql.to_ascii_lowercase().contains("lc_monetary")
        || matches!(leading_sql_keyword(sql).as_deref(), Some("DISCARD" | "RESET"))
    {
        *money_format.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    // Anything buffered so far belongs to earlier (e.g. catalog) traffic.
    take_postgres_notices(notices);

    let start_time = std::time::Instant::now();
    let mut result = if requires_simple_query_protocol(sql) {
        if !params.is_empty() {
            return Err(simple_query_parameters_error(sql));
        }

        let affected_rows = run_postgres_simple_statement(client, sql).await?;
        QueryResult {
            id: uuid::Uuid::new_v4(),
            columns: Vec::new(),
            rows: Vec::new(),
            total_rows: Some(0),
            is_estimated_total: false,
            affected_rows,
            execution_time_ms: 0,
            warnings: Vec::new(),
        }
    } else if let Some(direction) = copy_stdio_direction(sql) {
        match direction {
            CopyStdioDirection::FromStdin => {
                return Err(ZqlzError::Query(
                    "COPY FROM STDIN needs data; use the Import wizard".to_string(),
                ));
            }
            CopyStdioDirection::ToStdout => {
                if !params.is_empty() {
                    return Err(simple_query_parameters_error(sql));
                }
                run_postgres_copy_out(client, sql).await?
            }
        }
    } else {
        let statement = client.prepare(sql).await.map_err(|e| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to prepare {}: {}", noun, message))
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

        let execute_error = |e: tokio_postgres::Error| {
            let message = format_postgres_error(&e);
            ZqlzError::Query(format!("Failed to execute {}: {}", noun, message))
        };
        let stream = client
            .query_raw(&statement, param_refs.iter().copied())
            .await
            .map_err(execute_error)?;
        futures::pin_mut!(stream);
        let mut pg_rows = Vec::new();
        while let Some(pg_row) = stream.try_next().await.map_err(execute_error)? {
            pg_rows.push(pg_row);
        }
        let rows_affected = stream.rows_affected().unwrap_or(0);

        let mut columns = Vec::new();
        let mut column_names = Vec::new();
        for (idx, col) in statement.columns().iter().enumerate() {
            let name = col.name().to_string();
            column_names.push(name.clone());
            columns.push(ColumnMeta {
                name,
                data_type: col.type_().name().to_string(),
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

        let money = money_format_for(client, money_format, statement.columns()).await;
        let mut rows = Vec::new();
        for pg_row in &pg_rows {
            let values = with_money_format(&money, || {
                (0..columns.len())
                    .map(|idx| postgres_to_value(pg_row, idx))
                    .collect::<Result<Vec<_>>>()
            })?;
            rows.push(Row::new(column_names.clone(), values));
        }
        resolve_oid_alias_names(client, &columns, &mut rows).await;

        let total_rows = rows.len();
        QueryResult {
            id: uuid::Uuid::new_v4(),
            // A statement with no columns has no result set; the command tag's
            // row count is the only thing it reports.
            affected_rows: if columns.is_empty() { rows_affected } else { 0 },
            columns,
            rows,
            total_rows: Some(total_rows as u64),
            is_estimated_total: false,
            execution_time_ms: 0,
            warnings: Vec::new(),
        }
    };

    result.execution_time_ms = start_time.elapsed().as_millis() as u64;
    result.warnings.extend(take_postgres_notices(notices));
    Ok(result)
}

/// Shape a statement outcome for `Connection::execute`: a result set comes
/// back as rows (with `affected_rows` = the rows it produced); a plain command
/// reports only `affected_rows`, plus an empty result carrying notices if the
/// server raised any.
pub(crate) fn statement_result_from_query_result(result: QueryResult) -> StatementResult {
    if !result.columns.is_empty() {
        return StatementResult {
            is_query: true,
            affected_rows: result.rows.len() as u64,
            result: Some(result),
            error: None,
        };
    }

    StatementResult {
        is_query: false,
        affected_rows: result.affected_rows,
        result: (!result.warnings.is_empty()).then_some(result),
        error: None,
    }
}

/// Execute a query against a shared PostgreSQL client and collect the result.
/// Used by the schema-introspection adapter, which has no use for notices.
pub(crate) async fn run_postgres_query(
    client: &Arc<Mutex<Client>>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult> {
    let client = client.lock().await;
    run_postgres_statement(
        &client,
        &NoticeBuffer::default(),
        &MoneyFormatCache::default(),
        sql,
        params,
        "query",
    )
    .await
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
    /// Set once the tokio-postgres connection task terminates, which is the only
    /// authoritative signal that the client can no longer reach the server.
    closed: Arc<AtomicBool>,
    /// Notices raised by the server, drained into each statement's warnings.
    notices: NoticeBuffer,
    /// `money` layout probed from this session's `lc_monetary`.
    money_format: MoneyFormatCache,
    _ssh_tunnel: Option<PostgresSshTunnel>,
    pub(crate) schema_engine: SchemaEngine,
    /// The database this session connected to.
    database: String,
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

        let closed = Arc::new(AtomicBool::new(false));
        let notices = NoticeBuffer::default();

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
                    ZqlzError::Connection(format_postgres_connect_error(
                        &e,
                        &options.host,
                        options.port,
                    ))
                })?;

            let cancel_token = client.cancel_token();

            spawn_postgres_connection_task(
                runtime,
                connection,
                Arc::clone(&closed),
                Arc::clone(&notices),
            );

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
                    ZqlzError::Connection(format_postgres_connect_error(
                        &e,
                        &options.host,
                        options.port,
                    ))
                })?;

            let cancel_token = client.cancel_token();

            spawn_postgres_connection_task(
                runtime,
                connection,
                Arc::clone(&closed),
                Arc::clone(&notices),
            );

            (client, cancel_token)
        };

        tracing::info!(
            host = %options.host,
            port = %options.port,
            database = %options.database,
            ssl_mode = %options.ssl_mode,
            "PostgreSQL connection established"
        );
        let client = Arc::new(Mutex::new(client));
        let schema_engine = SchemaEngine::new(
            Arc::new(PostgresCatalog::new(client.clone())),
            Arc::new(DefaultDialect),
        );
        Ok(Self {
            client,
            cancel_token,
            closed,
            notices,
            money_format: MoneyFormatCache::default(),
            _ssh_tunnel: options.ssh_tunnel,
            schema_engine,
            database: options.database,
        })
    }
}

/// Escape a value for SQL literal inclusion (for PostgreSQL)
#[cfg(test)]
mod tests {
    use super::{
        format_postgres_foreign_key_cell_update_error, is_postgres_foreign_table_browse_error,
        is_postgres_missing_user_mapping_error, leading_sql_keyword, requires_simple_query_protocol,
    };
    use super::pg_value::{PgArrayValue, PgNumericString, encode_pg_numeric};
    use postgres_types::FromSql;
    use super::value_format::{
        format_postgres_binary_display_value,
        format_postgres_network_value, format_postgres_tstzrange_value,
        format_postgres_tsvector_value,
    };
    use super::value_literal::value_to_pg_literal_for_type;
    use bytes::BytesMut;
    use zqlz_core::{CellUpdateRequest, RowIdentifier, Value};

    /// The encoder must be the inverse of the decoder the driver already uses to
    /// read NUMERIC columns.
    ///
    /// Compared numerically because the decoder trims trailing fraction zeros,
    /// so `-6.0000` legitimately comes back as `-6`.
    #[track_caller]
    fn assert_numeric_round_trips(text: &str) {
        let mut buffer = BytesMut::new();
        encode_pg_numeric(text, &mut buffer).expect("numeric encodes");
        let decoded = PgNumericString::parse(&buffer).expect("numeric decodes");
        assert!(
            Value::Decimal(decoded.clone()).is_equivalent_to(&Value::Decimal(text.to_string())),
            "round trip changed the value: {} became {}",
            text,
            decoded
        );
    }

    #[test]
    fn pg_numeric_encoding_round_trips() {
        assert_numeric_round_trips("0");
        assert_numeric_round_trips("1");
        assert_numeric_round_trips("-1");
        assert_numeric_round_trips("12345.6789");
        assert_numeric_round_trips("-6.0000");
        assert_numeric_round_trips("0.5");
        assert_numeric_round_trips("1000000");
        assert_numeric_round_trips("0.0001");
    }

    /// The whole reason decimals travel as text: these do not fit in an f64.
    #[test]
    fn pg_numeric_encoding_preserves_wide_values() {
        assert_numeric_round_trips("20000000000000000001.5");
        assert_numeric_round_trips("123456789012345678901234567890.123456789");
    }

    #[test]
    fn pg_numeric_encoding_rejects_non_numeric_text() {
        let mut buffer = BytesMut::new();
        assert!(encode_pg_numeric("0 WHERE 1=1 -- ", &mut buffer).is_err());
        assert!(encode_pg_numeric("abc", &mut buffer).is_err());
        assert!(encode_pg_numeric("", &mut buffer).is_err());
    }

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
            "[\"2025-01-01 00:00:00+00\",)"
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
            PgArrayValue::from_sql(&postgres_types::Type::INET_ARRAY, &raw).unwrap().0,
            Value::Array(vec![
                Value::String("192.168.10.20".to_string()),
                Value::String("10.0.1.42".to_string()),
            ])
        );
    }

    #[test]
    fn postgres_misc_binary_values_render_text() {
        assert_eq!(
            format_postgres_binary_display_value("money", &(-123456789_i64).to_be_bytes()).unwrap(),
            "-$1,234,567.89"
        );

        let mut interval = Vec::new();
        interval.extend_from_slice(&3_600_000_000_i64.to_be_bytes());
        interval.extend_from_slice(&2_i32.to_be_bytes());
        interval.extend_from_slice(&1_i32.to_be_bytes());
        assert_eq!(
            format_postgres_binary_display_value("interval", &interval).unwrap(),
            "1 mon 2 days 01:00:00"
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

    #[test]
    fn leading_sql_keyword_skips_whitespace_and_comments() {
        assert_eq!(leading_sql_keyword("VACUUM t").as_deref(), Some("VACUUM"));
        assert_eq!(leading_sql_keyword("  \n\t vacuum t").as_deref(), Some("VACUUM"));
        assert_eq!(
            leading_sql_keyword("-- warm up\nVACUUM t").as_deref(),
            Some("VACUUM")
        );
        assert_eq!(
            leading_sql_keyword("/* warm up */ VACUUM t").as_deref(),
            Some("VACUUM")
        );
        assert_eq!(leading_sql_keyword("   ").as_deref(), None);
        assert_eq!(leading_sql_keyword("-- only a comment").as_deref(), None);
    }

    #[test]
    fn requires_simple_query_protocol_detects_non_transactable_statements() {
        assert!(requires_simple_query_protocol("VACUUM users;"));
        assert!(requires_simple_query_protocol("vacuum (analyze) users"));
        assert!(requires_simple_query_protocol("-- x\nVACUUM t"));
        assert!(requires_simple_query_protocol("/* x */ VACUUM t"));
        assert!(requires_simple_query_protocol("\n\n   REINDEX TABLE users"));
        assert!(requires_simple_query_protocol("CLUSTER users USING idx"));
        assert!(requires_simple_query_protocol("DISCARD ALL"));
        assert!(requires_simple_query_protocol("CREATE DATABASE demo"));
        assert!(requires_simple_query_protocol("DROP DATABASE IF EXISTS demo"));
        assert!(requires_simple_query_protocol("ALTER SYSTEM SET work_mem = '64MB'"));
        assert!(requires_simple_query_protocol("CREATE TABLESPACE ts LOCATION '/x'"));
        assert!(requires_simple_query_protocol(
            "CREATE INDEX CONCURRENTLY idx ON users (id)"
        ));
        assert!(requires_simple_query_protocol(
            "CREATE UNIQUE INDEX CONCURRENTLY idx ON users (id)"
        ));
        assert!(requires_simple_query_protocol("DROP INDEX CONCURRENTLY idx"));
    }

    #[test]
    fn requires_simple_query_protocol_ignores_ordinary_statements() {
        assert!(!requires_simple_query_protocol("SELECT 1"));
        assert!(!requires_simple_query_protocol("INSERT INTO t VALUES (1)"));
        assert!(!requires_simple_query_protocol("UPDATE t SET a = 1"));
        assert!(!requires_simple_query_protocol("CREATE INDEX idx ON users (id)"));
        assert!(!requires_simple_query_protocol("CREATE TABLE t (id int)"));
        assert!(!requires_simple_query_protocol(
            "SELECT 'VACUUM users' AS note"
        ));
        assert!(!requires_simple_query_protocol(
            "INSERT INTO logs (message) VALUES ('VACUUM users')"
        ));
        assert!(!requires_simple_query_protocol("SELECT \"VACUUM\" FROM t"));
        assert!(!requires_simple_query_protocol("-- VACUUM users\nSELECT 1"));
        assert!(!requires_simple_query_protocol(""));
    }

    /// Connects to the opt-in test server described by the `ZQLZ_PG_TEST_*` env vars.
    async fn connect_live_test_server(application_name: &str) -> super::PostgresConnection {
        use super::{PostgresConnectOptions, PostgresConnection};

        let port: u16 = std::env::var("ZQLZ_PG_TEST_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(5432);

        let options = PostgresConnectOptions {
            host: std::env::var("ZQLZ_PG_TEST_HOST").unwrap_or_else(|_| "localhost".to_string()),
            port,
            database: std::env::var("ZQLZ_PG_TEST_DB").unwrap_or_else(|_| "postgres".to_string()),
            user: Some(std::env::var("ZQLZ_PG_TEST_USER").unwrap_or_else(|_| "postgres".to_string())),
            password: Some(
                std::env::var("ZQLZ_PG_TEST_PASSWORD").unwrap_or_else(|_| "postgres".to_string()),
            ),
            ssl_mode: "disable".to_string(),
            ssl_ca_cert: None,
            ssl_client_cert: None,
            ssl_client_key: None,
            connect_timeout_seconds: Some(10),
            application_name: Some(application_name.to_string()),
            search_path: None,
            keepalive: true,
            ssh_tunnel: None,
        };

        PostgresConnection::connect(options)
            .await
            .expect("connect to test PostgreSQL server")
    }

    /// Opt-in end-to-end check against a real PostgreSQL server. Set the
    /// `ZQLZ_PG_TEST_*` env vars and run with `--ignored`.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL server"]
    async fn vacuum_runs_outside_transaction_and_is_refused_inside_one() {
        use zqlz_core::Connection as _;

        let connection = connect_live_test_server("zqlz-vacuum-test").await;

        connection
            .execute("DROP TABLE IF EXISTS zqlz_vacuum_demo", &[])
            .await
            .expect("drop demo table");
        connection
            .execute("CREATE TABLE zqlz_vacuum_demo (id int primary key)", &[])
            .await
            .expect("create demo table");
        connection
            .execute("INSERT INTO zqlz_vacuum_demo VALUES (1), (2), (3)", &[])
            .await
            .expect("seed demo table");

        connection
            .execute("VACUUM zqlz_vacuum_demo", &[])
            .await
            .expect("VACUUM must succeed outside a transaction");
        connection
            .execute("-- warm up\nVACUUM (ANALYZE) zqlz_vacuum_demo", &[])
            .await
            .expect("commented VACUUM (ANALYZE) must succeed");
        connection
            .query("VACUUM zqlz_vacuum_demo", &[])
            .await
            .expect("VACUUM through query() must succeed");

        let transaction = connection
            .begin_transaction()
            .await
            .expect("begin transaction");
        let error = transaction
            .execute("VACUUM zqlz_vacuum_demo", &[])
            .await
            .expect_err("VACUUM inside a transaction must be refused");
        assert!(
            error
                .to_string()
                .contains("cannot run inside a transaction block"),
            "unexpected error: {error}"
        );
        transaction.rollback().await.expect("rollback");

        connection
            .execute("SELECT 1", &[])
            .await
            .expect("connection still usable after refusal");
        connection
            .execute("DROP TABLE zqlz_vacuum_demo", &[])
            .await
            .expect("cleanup demo table");
    }

    #[test]
    fn copy_stdio_direction_detects_only_stdio_copies() {
        use super::{CopyStdioDirection, copy_stdio_direction};

        assert_eq!(
            copy_stdio_direction("COPY (SELECT 1) TO STDOUT"),
            Some(CopyStdioDirection::ToStdout)
        );
        assert_eq!(
            copy_stdio_direction("/* x */ copy t to stdout with (format csv)"),
            Some(CopyStdioDirection::ToStdout)
        );
        assert_eq!(
            copy_stdio_direction("COPY t (a, b) FROM STDIN"),
            Some(CopyStdioDirection::FromStdin)
        );
        assert_eq!(copy_stdio_direction("COPY t TO '/tmp/out.csv'"), None);
        assert_eq!(copy_stdio_direction("COPY t FROM PROGRAM 'cat x'"), None);
        assert_eq!(copy_stdio_direction("SELECT 'COPY t TO STDOUT'"), None);
        assert_eq!(copy_stdio_direction("SELECT 1 -- COPY t TO STDOUT"), None);
        assert_eq!(copy_stdio_direction("COPY t TO \"stdout\""), None);
    }

    #[test]
    fn statement_result_follows_the_result_set_not_the_verb() {
        use super::statement_result_from_query_result;
        use zqlz_core::{ColumnMeta, QueryResult, Row, Value};

        let result = |columns: usize, rows: usize, affected_rows: u64, warnings: &[&str]| {
            let columns: Vec<ColumnMeta> = (0..columns)
                .map(|i| ColumnMeta {
                    name: format!("c{i}"),
                    data_type: "int4".to_string(),
                    nullable: true,
                    ordinal: i,
                    max_length: None,
                    precision: None,
                    scale: None,
                    auto_increment: false,
                    default_value: None,
                    comment: None,
                    enum_values: None,
                })
                .collect::<Vec<_>>();
            let rows = (0..rows)
                .map(|_| Row::new(vec!["c0".to_string()], vec![Value::Int32(1)]))
                .collect::<Vec<_>>();
            QueryResult {
                id: uuid::Uuid::new_v4(),
                columns,
                total_rows: Some(rows.len() as u64),
                rows,
                is_estimated_total: false,
                affected_rows,
                execution_time_ms: 0,
                warnings: warnings.iter().map(|w| w.to_string()).collect(),
            }
        };

        // INSERT ... RETURNING: columns present, so the rows come back.
        let returning = statement_result_from_query_result(result(1, 2, 0, &[]));
        assert!(returning.is_query);
        assert_eq!(returning.affected_rows, 2);
        assert_eq!(returning.result.expect("rows").rows.len(), 2);

        // Zero-row RETURNING still reports its columns.
        let empty = statement_result_from_query_result(result(1, 0, 0, &[]));
        assert!(empty.is_query && empty.result.is_some());

        // Plain command: only the affected row count.
        let command = statement_result_from_query_result(result(0, 0, 5, &[]));
        assert!(!command.is_query && command.result.is_none());
        assert_eq!(command.affected_rows, 5);

        // Plain command that raised notices keeps them on an empty result.
        let noisy = statement_result_from_query_result(result(0, 0, 0, &["NOTICE: hi"]));
        assert!(!noisy.is_query);
        assert_eq!(noisy.result.expect("warnings").warnings, vec!["NOTICE: hi"]);
    }

    /// Opt-in: result-set-vs-command is decided by the server, for every verb,
    /// on a plain connection and inside a transaction; notices and COPY work.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL server"]
    async fn result_sets_notices_and_copy_work_for_every_statement_kind() {
        use zqlz_core::{Connection as _, Value};

        let connection = connect_live_test_server("zqlz-dispatch-test").await;
        for sql in [
            "CREATE SCHEMA IF NOT EXISTS zqlz_test",
            "CREATE OR REPLACE PROCEDURE zqlz_test.p(INOUT x int) LANGUAGE plpgsql \
             AS $$ BEGIN x := x * 2; END $$",
            "DROP TABLE IF EXISTS zqlz_test.c_dml",
            "DROP TABLE IF EXISTS zqlz_test.c_into",
            "CREATE TABLE zqlz_test.c_dml (id serial PRIMARY KEY, v text UNIQUE, n numeric)",
            "CREATE OR REPLACE FUNCTION zqlz_test.c_noisy() RETURNS int LANGUAGE plpgsql \
             AS $$ BEGIN RAISE NOTICE 'from fn'; RETURN 1; END $$",
        ] {
            connection.execute(sql, &[]).await.expect(sql);
        }

        // The same scenarios run against the connection and against a transaction.
        macro_rules! scenarios {
            ($t:expr, $tag:expr) => {{
                let t = &$t;
                let tag = $tag;
                let s = |text: &str| Value::String(text.to_string());

                // Write verbs with RETURNING return rows through execute() and query().
                let r = t
                    .execute(&format!("INSERT INTO zqlz_test.c_dml (v, n) VALUES ('a{tag}', 1.5), ('b{tag}', 2) RETURNING id, v, n"), &[])
                    .await
                    .expect("insert returning");
                assert!(r.is_query, "insert returning must be a result set");
                let rows = r.result.expect("rows").rows;
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].values[1], s(&format!("a{tag}")));
                assert_eq!(rows[0].values[2], Value::Decimal("1.5".to_string()));

                let q = t
                    .query(&format!("UPDATE zqlz_test.c_dml SET n = n + 1 WHERE v LIKE '%{tag}' RETURNING v, n"), &[])
                    .await
                    .expect("update returning");
                assert_eq!(q.rows.len(), 2);
                let r = t
                    .execute(&format!("UPDATE zqlz_test.c_dml SET n = n + 1 WHERE v LIKE '%{tag}' RETURNING v"), &[])
                    .await
                    .expect("update returning via execute");
                assert_eq!(r.result.expect("rows").rows.len(), 2);

                let r = t
                    .execute(&format!("INSERT INTO zqlz_test.c_dml (v, n) VALUES ('a{tag}', 9) ON CONFLICT (v) DO UPDATE SET n = EXCLUDED.n RETURNING v"), &[])
                    .await
                    .expect("upsert returning");
                assert_eq!(r.result.expect("rows").rows[0].values[0], s(&format!("a{tag}")));

                let r = t
                    .execute(&format!("WITH ins AS (INSERT INTO zqlz_test.c_dml (v) VALUES ('c{tag}') RETURNING id, v) SELECT v FROM ins"), &[])
                    .await
                    .expect("data-modifying CTE");
                assert_eq!(r.result.expect("rows").rows[0].values[0], s(&format!("c{tag}")));

                let r = t
                    .execute(&format!("MERGE INTO zqlz_test.c_dml d USING (VALUES ('m{tag}')) src(v) ON d.v = src.v WHEN NOT MATCHED THEN INSERT (v) VALUES (src.v) RETURNING d.v"), &[])
                    .await
                    .expect("merge returning");
                assert_eq!(r.result.expect("rows").rows[0].values[0], s(&format!("m{tag}")));

                let r = t
                    .execute(&format!("DELETE FROM zqlz_test.c_dml WHERE v = 'c{tag}' RETURNING id"), &[])
                    .await
                    .expect("delete returning");
                assert_eq!(r.result.expect("rows").rows.len(), 1);

                // Without RETURNING only the affected row count comes back.
                let r = t
                    .execute(&format!("INSERT INTO zqlz_test.c_dml (v) VALUES ('p{tag}')"), &[])
                    .await
                    .expect("plain insert");
                assert!(!r.is_query && r.result.is_none());
                assert_eq!(r.affected_rows, 1);
                let q = t
                    .query(&format!("DELETE FROM zqlz_test.c_dml WHERE v = 'p{tag}'"), &[])
                    .await
                    .expect("plain delete through query");
                assert!(q.columns.is_empty());
                assert_eq!(q.affected_rows, 1);

                // CALL with an INOUT argument returns its row.
                let r = t.execute("CALL zqlz_test.p(21)", &[]).await.expect("call");
                assert_eq!(r.result.expect("call row").rows[0].values[0], Value::Int32(42));
                let q = t.query("CALL zqlz_test.p(21)", &[]).await.expect("call query");
                assert_eq!(q.rows[0].values[0], Value::Int32(42));

                // PREPARE / EXECUTE / DEALLOCATE.
                let prepare = t
                    .execute(&format!("PREPARE q_{tag}(int) AS SELECT $1 + 1 AS r"), &[])
                    .await
                    .expect("prepare");
                assert!(!prepare.is_query);
                let r = t.execute(&format!("EXECUTE q_{tag}(41)"), &[]).await.expect("execute");
                assert_eq!(r.result.expect("execute row").rows[0].values[0], Value::Int32(42));
                t.execute(&format!("DEALLOCATE q_{tag}"), &[]).await.expect("deallocate");
                t.execute(&format!("PREPARE qi_{tag}(text) AS INSERT INTO zqlz_test.c_dml (v) VALUES ($1) RETURNING id"), &[])
                    .await
                    .expect("prepare insert");
                let r = t.execute(&format!("EXECUTE qi_{tag}('e{tag}')"), &[]).await.expect("execute insert");
                assert_eq!(r.result.expect("execute insert row").rows.len(), 1);
                t.execute(&format!("DEALLOCATE qi_{tag}"), &[]).await.expect("deallocate");

                // Other row-returning commands.
                for sql in ["SHOW search_path", "TABLE zqlz_test.c_dml", "VALUES (1), (2)", "EXPLAIN SELECT 1"] {
                    let r = t.execute(sql, &[]).await.expect(sql);
                    assert!(r.is_query && !r.result.expect(sql).rows.is_empty(), "{sql}");
                }

                // SELECT ... INTO has no result set, just a row count.
                let r = t
                    .execute("SELECT * INTO zqlz_test.c_into FROM zqlz_test.c_dml", &[])
                    .await
                    .expect("select into");
                assert!(!r.is_query && r.affected_rows > 0);
                t.execute("DROP TABLE zqlz_test.c_into", &[]).await.expect("drop select-into table");

                // LISTEN / DO produce no rows.
                for sql in ["LISTEN zqlz_dispatch_test", "UNLISTEN zqlz_dispatch_test", "DO $$ BEGIN NULL; END $$"] {
                    assert!(!t.execute(sql, &[]).await.expect(sql).is_query, "{sql}");
                }

                // RAISE NOTICE / WARNING / INFO are captured per statement.
                let r = t
                    .execute("DO $$ BEGIN RAISE NOTICE 'hello %', 1; RAISE WARNING 'careful'; RAISE INFO 'fyi' USING DETAIL = 'det', HINT = 'hnt'; END $$", &[])
                    .await
                    .expect("do raise");
                assert!(!r.is_query);
                assert_eq!(
                    r.result.expect("notices").warnings,
                    vec![
                        "NOTICE: hello 1".to_string(),
                        "WARNING: careful".to_string(),
                        "INFO: fyi\nDETAIL: det\nHINT: hnt".to_string(),
                    ]
                );
                let q = t.query("SELECT zqlz_test.c_noisy()", &[]).await.expect("noisy function");
                assert_eq!(q.warnings, vec!["NOTICE: from fn".to_string()]);
                let q = t.query("SELECT 1", &[]).await.expect("quiet");
                assert!(q.warnings.is_empty(), "notices must not leak into later statements");

                // COPY ... TO STDOUT returns text lines; COPY FROM STDIN is refused kindly.
                let q = t
                    .query("COPY (SELECT g AS a, 'x' AS b FROM generate_series(1, 3) g) TO STDOUT", &[])
                    .await
                    .expect("copy out");
                assert_eq!(q.columns[0].name, "copy");
                let lines: Vec<_> = q.rows.iter().map(|row| row.values[0].clone()).collect();
                assert_eq!(lines, vec![s("1\tx"), s("2\tx"), s("3\tx")]);
                let r = t
                    .execute("COPY (SELECT 1, 'a,b') TO STDOUT WITH (FORMAT csv)", &[])
                    .await
                    .expect("copy out csv via execute");
                assert!(r.is_query);
                assert_eq!(r.result.expect("rows").rows[0].values[0], s("1,\"a,b\""));
                let error = t
                    .execute("COPY zqlz_test.c_dml FROM STDIN", &[])
                    .await
                    .expect_err("copy from stdin");
                assert!(error.to_string().contains("Import wizard"), "{error}");
                let q = t
                    .query("COPY (SELECT generate_series(1, 10050)) TO STDOUT", &[])
                    .await
                    .expect("big copy out");
                assert_eq!(q.rows.len(), 10_000);
                assert!(q.warnings.iter().any(|w| w.contains("truncated")), "{:?}", q.warnings);
                let q = t.query("SELECT 1", &[]).await.expect("connection still usable after truncated copy");
                assert_eq!(q.rows.len(), 1);
            }};
        }

        scenarios!(connection, "c");

        // Cursors on the plain connection: BEGIN is session state.
        connection.execute("BEGIN", &[]).await.expect("begin");
        connection
            .execute("DECLARE zqlz_c CURSOR FOR SELECT generate_series(1, 5)", &[])
            .await
            .expect("declare");
        let fetched = connection.execute("FETCH 2 FROM zqlz_c", &[]).await.expect("fetch");
        assert_eq!(fetched.result.expect("fetch rows").rows.len(), 2);
        let moved = connection.execute("MOVE 1 FROM zqlz_c", &[]).await.expect("move");
        assert!(!moved.is_query);
        assert_eq!(moved.affected_rows, 1);
        connection.execute("ROLLBACK", &[]).await.expect("rollback");

        let transaction = connection.begin_transaction().await.expect("begin");
        scenarios!(transaction, "t");
        transaction
            .execute("DECLARE zqlz_c CURSOR FOR SELECT generate_series(1, 5)", &[])
            .await
            .expect("declare in transaction");
        let fetched = transaction.query("FETCH 2 FROM zqlz_c", &[]).await.expect("fetch");
        assert_eq!(fetched.rows.len(), 2);
        transaction.rollback().await.expect("rollback");

        for sql in [
            "DROP TABLE zqlz_test.c_dml",
            "DROP FUNCTION zqlz_test.c_noisy()",
        ] {
            connection.execute(sql, &[]).await.expect(sql);
        }
    }

    #[tokio::test]
    #[ignore = "requires a live PostgreSQL server"]
    async fn numeric_array_and_temporal_values_match_psql() {
        use zqlz_core::{Connection as _, Value};

        let connection = connect_live_test_server("zqlz-numeric-array-test").await;
        let sql = "SELECT (-0.5)::numeric, 0.0000001::numeric, (-0.000100)::numeric, \
                   1.50::numeric, 0.00::numeric(10,2), 1.000::numeric, 'NaN'::numeric, \
                   'Infinity'::numeric, '-Infinity'::numeric, \
                   12345678901234567890.0123456789::numeric, \
                   ARRAY[1.10, 2.25]::numeric[], ARRAY[1, NULL, 3], ARRAY[[1, 2], [3, 4]], \
                   ARRAY['a', NULL, 'b c'], '{}'::int[], ARRAY['infinity'::date], \
                   'infinity'::date, '-infinity'::timestamp, 'infinity'::timestamptz, \
                   '0044-03-15 BC'::date, '0044-03-15 10:00:00 BC'::timestamp";
        let decimal = |text: &str| Value::Decimal(text.to_string());
        let string = |text: &str| Value::String(text.to_string());
        let ints = |values: &[i32]| Value::Array(values.iter().map(|v| Value::Int32(*v)).collect());
        let expected = vec![
            decimal("-0.5"),
            decimal("0.0000001"),
            decimal("-0.000100"),
            decimal("1.50"),
            decimal("0.00"),
            decimal("1.000"),
            decimal("NaN"),
            decimal("Infinity"),
            decimal("-Infinity"),
            decimal("12345678901234567890.0123456789"),
            Value::Array(vec![decimal("1.10"), decimal("2.25")]),
            Value::Array(vec![Value::Int32(1), Value::Null, Value::Int32(3)]),
            Value::Array(vec![ints(&[1, 2]), ints(&[3, 4])]),
            Value::Array(vec![string("a"), Value::Null, string("b c")]),
            Value::Array(vec![]),
            Value::Array(vec![string("infinity")]),
            string("infinity"),
            string("-infinity"),
            string("infinity"),
            string("0044-03-15 BC"),
            string("0044-03-15 10:00:00 BC"),
        ];

        let result = connection.query(sql, &[]).await.expect("query values");
        assert_eq!(result.rows[0].values, expected);

        let transaction = connection.begin_transaction().await.expect("begin");
        let result = transaction.query(sql, &[]).await.expect("query in transaction");
        assert_eq!(result.rows[0].values, expected);
        transaction.rollback().await.expect("rollback");

        // Values the decoder shows must also be writable back through bound parameters.
        let result = connection
            .query(
                "SELECT $1::numeric, $2::numeric, $3::date, $4::timestamp, $5::date",
                &[
                    decimal("-0.000100"),
                    decimal("NaN"),
                    string("infinity"),
                    string("0044-03-15 10:00:00 BC"),
                    string("0044-03-15 BC"),
                ],
            )
            .await
            .expect("round trip through parameters");
        assert_eq!(
            result.rows[0].values,
            vec![
                decimal("-0.000100"),
                decimal("NaN"),
                string("infinity"),
                string("0044-03-15 10:00:00 BC"),
                string("0044-03-15 BC"),
            ]
        );
    }

    /// Composite, record, tsquery, multirange, snapshot, interval, range and
    /// geometric values must read exactly like the server's own text output
    /// (what psql prints), which `expr::text` gives us as the oracle.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL server"]
    async fn composite_and_special_types_display_like_psql() {
        use zqlz_core::{Connection as _, Value};

        let connection = connect_live_test_server("zqlz-special-values-test").await;
        let expressions = [
            "(SELECT price FROM zqlz_lab.products WHERE price IS NOT NULL ORDER BY price::text LIMIT 1)",
            "ROW(1, 'a')",
            "ROW(NULL, '', 'a,b', 'c\"d', 'e\\f', 'g h', '(x)', true)",
            "ROW(ROW(1, 2), ARRAY[1, 2], ARRAY['a b', NULL])",
            "ROW(DATE '2025-01-01', TIMESTAMP '2025-01-01 10:00:00.5', 1.50::numeric, 2.5::float8, '\\xdead'::bytea, '550e8400-e29b-41d4-a716-446655440000'::uuid)",
            "ROW(NULL::int)",
            "'quick & fox'::tsquery",
            "'a | (b & c)'::tsquery",
            "'(a | b) & c'::tsquery",
            "'a <-> (b <-> c)'::tsquery",
            "'a <3> b'::tsquery",
            "'!(a & b) & c:*AB'::tsquery",
            "'it''s'::tsquery",
            "'{[1,3),[5,7)}'::int4multirange",
            "'{[1,3),[5,7)}'::int8multirange",
            "'{[1.5,3.5),[5,7)}'::nummultirange",
            "'{[2025-01-01 10:00, 2025-01-01 12:00:00.5)}'::tsmultirange",
            "'{[2025-01-01 10:00+00, 2025-01-01 12:00+00)}'::tstzmultirange",
            "'{[2025-01-01, 2025-02-01), [2025-03-01,)}'::datemultirange",
            "'{}'::int4multirange",
            "'[2025-01-01 10:00, 2025-01-01 12:00)'::tsrange",
            "'[2025-01-01 10:00:00.25+00, infinity)'::tstzrange",
            "'(,5]'::int4range",
            "'10:20:10,14,15'::txid_snapshot",
            "pg_current_snapshot()",
            "'1 year 2 mons 3 days 04:05:06.7'::interval",
            "'-3 days -00:00:01'::interval",
            "'-14 mons'::interval",
            "'1 mon -1 day +01:00:00'::interval",
            "'[(0,0),(1,1)]'::lseg",
            "'(1,1),(0,0)'::box",
            "'<(0,0),2>'::circle",
            "'((1,2),(3,4))'::path",
            "'[(1.5,2e20),(1,1)]'::path",
        ];
        let mut mismatches = Vec::new();
        for expression in expressions {
            let sql = format!("SELECT {expression} AS value, ({expression})::text AS expected");
            let result = connection.query(&sql, &[]).await.expect("query special value");
            let expected = match &result.rows[0].values[1] {
                Value::String(text) => text.clone(),
                other => panic!("expected text for {expression}, got {other:?}"),
            };
            // pg_current_snapshot() differs between statements, so only the shape is checked.
            if expression == "pg_current_snapshot()" {
                let Value::String(actual) = &result.rows[0].values[0] else {
                    panic!("snapshot did not decode to a string");
                };
                assert_eq!(actual.split(':').count(), 3, "snapshot shape: {actual}");
                continue;
            }
            if result.rows[0].values[0] != Value::String(expected.clone()) {
                mismatches.push(format!(
                    "{expression}: got {:?}, psql prints {expected:?}",
                    result.rows[0].values[0]
                ));
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));

        let result = connection
            .query(
                "SELECT ARRAY[ROW('a,b', 1.5)::zqlz_lab.currency_amount, NULL] AS composites, \
                        ARRAY[ROW(1, 'x')] AS records",
                &[],
            )
            .await
            .expect("query composite arrays");
        assert_eq!(
            result.rows[0].values,
            vec![
                Value::Array(vec![Value::String("(\"a,b\",1.50)".to_string()), Value::Null]),
                Value::Array(vec![Value::String("(1,x)".to_string())]),
            ]
        );

        let transaction = connection.begin_transaction().await.expect("begin");
        let result = transaction
            .query("SELECT ROW(1, 'a') AS r, 'a & b'::tsquery AS q", &[])
            .await
            .expect("query in transaction");
        assert_eq!(
            result.rows[0].values,
            vec![Value::String("(1,a)".to_string()), Value::String("'a' & 'b'".to_string())]
        );
        transaction.rollback().await.expect("rollback");
    }

    #[tokio::test]
    #[ignore = "requires a live PostgreSQL server"]
    async fn catalog_char_and_regclass_values_display_like_psql() {
        use zqlz_core::{Connection as _, Value};

        let connection = connect_live_test_server("zqlz-catalog-values-test").await;
        let sql = "SELECT relkind, oid::regclass AS rel, relnamespace::regnamespace AS ns \
                   FROM pg_class WHERE relname = 'pg_class'";
        let expected = vec![
            Value::String("r".to_string()),
            Value::String("pg_class".to_string()),
            Value::String("pg_catalog".to_string()),
        ];

        let result = connection.query(sql, &[]).await.expect("query catalog");
        assert_eq!(result.rows[0].values, expected);

        let transaction = connection.begin_transaction().await.expect("begin");
        let result = transaction.query(sql, &[]).await.expect("query in transaction");
        assert_eq!(result.rows[0].values, expected);
        transaction.rollback().await.expect("rollback");
    }

    /// `money` must read exactly like psql in every container: scalars, arrays and
    /// composite fields, on a plain connection and inside a transaction, and the
    /// displayed text must be accepted back on write.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL server"]
    async fn money_values_display_like_psql_and_round_trip() {
        use zqlz_core::{Connection as _, Value};

        let connection = connect_live_test_server("zqlz-money-test").await;
        let amounts = "(0.00), (0.05), (-0.05), (12.34), (-1234567.89), (1234567890123.45), (-92233720368547758.08)";
        let sql = format!(
            "SELECT m, m::text AS expected, ARRAY[m, NULL, m] AS arr, \
             ROW(m, 1)::text AS rec \
             FROM (SELECT v::money AS m FROM (VALUES {amounts}) t(v)) q"
        );
        let check = |result: zqlz_core::QueryResult| {
            assert_eq!(result.rows.len(), 7);
            for row in &result.rows {
                let Value::String(shown) = &row.values[0] else {
                    panic!("money must be a string, got {:?}", row.values[0]);
                };
                assert_eq!(Value::String(shown.clone()), row.values[1], "scalar money");
                assert_eq!(
                    row.values[2],
                    Value::Array(vec![
                        Value::String(shown.clone()),
                        Value::Null,
                        Value::String(shown.clone())
                    ]),
                    "money[] elements"
                );
            }
        };
        check(connection.query(&sql, &[]).await.expect("query money"));

        let transaction = connection.begin_transaction().await.expect("begin");
        check(transaction.query(&sql, &[]).await.expect("query money in tx"));
        transaction.rollback().await.expect("rollback");

        // A changed lc_monetary invalidates the cached layout.
        connection.execute("SET lc_monetary = 'C'", &[]).await.expect("set C");
        check(connection.query(&sql, &[]).await.expect("query money in C"));
        connection.execute("RESET lc_monetary", &[]).await.expect("reset");

        // Money inside a composite (and an array of them) uses the same layout.
        connection
            .execute("CREATE TYPE pg_temp.zqlz_money_pair AS (m money, n int)", &[])
            .await
            .expect("create composite");
        let composite = connection
            .query(
                "SELECT ROW((-1234.5)::money, 1)::pg_temp.zqlz_money_pair AS c, \
                 ARRAY[ROW(7.5::money, 2)::pg_temp.zqlz_money_pair] AS ca",
                &[],
            )
            .await
            .expect("query composite money");
        let rendered = format!("{:?}", composite.rows[0].values);
        assert!(rendered.contains("-$1,234.50"), "composite: {rendered}");
        assert!(rendered.contains("$7.50"), "composite array: {rendered}");

        // Edit round trip: the text shown is accepted back by the server.
        connection
            .execute("CREATE TEMP TABLE zqlz_money_rt (id int primary key, m money)", &[])
            .await
            .expect("create temp table");
        for shown in ["$1,234.56", "-$1,234,567.89", "$0.05"] {
            let literal = super::value_literal::value_to_pg_literal_for_type(
                &Value::String(shown.to_string()),
                Some("money"),
            );
            connection
                .execute(&format!("INSERT INTO zqlz_money_rt VALUES (1, {literal}) ON CONFLICT (id) DO UPDATE SET m = EXCLUDED.m"), &[])
                .await
                .expect("write displayed money");
            let back = connection
                .query("SELECT m FROM zqlz_money_rt WHERE id = 1", &[])
                .await
                .expect("read back");
            assert_eq!(back.rows[0].values[0], Value::String(shown.to_string()));
        }
    }

    /// The completions warm-up must see tables in every user schema even though the
    /// session's search_path is just `public`.
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL server with the feature-lab data"]
    async fn bulk_columns_cover_every_user_schema() {
        use zqlz_core::SchemaIntrospection as _;

        let connection = connect_live_test_server("zqlz-bulk-columns-test").await;
        let introspection = &connection;

        let all = introspection
            .list_all_columns(None)
            .await
            .expect("bulk columns")
            .expect("postgres has a bulk form");
        for relation in [
            "zqlz_lab.customers",
            "zqlz_lab.orders",
            "zqlz_lab.documents",
            "zqlz_weird_names.select",
        ] {
            assert!(
                all.get(relation).is_some_and(|columns| !columns.is_empty()),
                "missing columns for {relation}; got {:?}",
                all.keys().collect::<Vec<_>>()
            );
        }
        assert!(
            all.keys()
                .all(|key| !key.starts_with("pg_catalog.") && !key.starts_with("information_schema.")),
            "system schemas must stay out"
        );

        // An explicit schema keeps bare keys.
        let lab = introspection
            .list_all_columns(Some("zqlz_lab"))
            .await
            .expect("bulk columns for one schema")
            .expect("postgres has a bulk form");
        assert!(lab.contains_key("customers"));
        assert_eq!(
            lab.get("customers").map(Vec::len),
            all.get("zqlz_lab.customers").map(Vec::len)
        );

        let foreign_keys = introspection
            .list_all_foreign_keys(None)
            .await
            .expect("bulk foreign keys")
            .expect("postgres has a bulk form");
        assert!(
            foreign_keys.keys().any(|key| key.starts_with("zqlz_lab.")),
            "expected qualified foreign key owners, got {:?}",
            foreign_keys.keys().collect::<Vec<_>>()
        );
    }

    /// Connect failures must surface the server's / OS's own message, not
    /// tokio-postgres' bare "db error" / "error connecting to server".
    #[tokio::test]
    #[ignore = "requires a live PostgreSQL server"]
    async fn connect_errors_carry_server_and_io_details() {
        use super::{PostgresConnectOptions, PostgresConnection};

        let env = |key: &str, default: &str| {
            std::env::var(key).unwrap_or_else(|_| default.to_string())
        };
        let options = |host: &str, port: u16, database: &str, password: &str| {
            PostgresConnectOptions {
                host: host.to_string(),
                port,
                database: database.to_string(),
                user: Some(env("ZQLZ_PG_TEST_USER", "postgres")),
                password: Some(password.to_string()),
                ssl_mode: "disable".to_string(),
                ssl_ca_cert: None,
                ssl_client_cert: None,
                ssl_client_key: None,
                connect_timeout_seconds: Some(5),
                application_name: None,
                search_path: None,
                keepalive: true,
                ssh_tunnel: None,
            }
        };
        let host = env("ZQLZ_PG_TEST_HOST", "localhost");
        let port: u16 = env("ZQLZ_PG_TEST_PORT", "5432").parse().expect("port");
        let password = env("ZQLZ_PG_TEST_PASSWORD", "postgres");
        let error_of = |result: zqlz_core::Result<PostgresConnection>| match result {
            Ok(_) => panic!("connect unexpectedly succeeded"),
            Err(error) => error.to_string(),
        };

        let missing_database = error_of(
            PostgresConnection::connect(options(&host, port, "zqlz_does_not_exist", &password))
                .await,
        );
        assert!(
            missing_database.contains("database \"zqlz_does_not_exist\" does not exist")
                && missing_database.contains("3D000")
                && missing_database.contains(&format!("{host}:{port}")),
            "unexpected error: {missing_database}"
        );

        let bad_password = error_of(
            PostgresConnection::connect(options(
                &host,
                port,
                &env("ZQLZ_PG_TEST_DB", "postgres"),
                "definitely-not-the-password",
            ))
            .await,
        );
        assert!(
            bad_password.contains("password authentication failed")
                && bad_password.contains("28P01"),
            "unexpected error: {bad_password}"
        );
        assert!(!bad_password.contains("definitely-not-the-password"));

        let refused = error_of(
            PostgresConnection::connect(options("127.0.0.1", 1, "postgres", &password)).await,
        );
        assert!(
            refused.contains("127.0.0.1:1") && refused.to_lowercase().contains("refused"),
            "unexpected error: {refused}"
        );
    }
}
