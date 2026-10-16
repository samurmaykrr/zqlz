//! The pinned user session, and the statement runner it shares with the pool.
//!
//! User `query`/`execute` calls all run on ONE dedicated connection so session
//! state survives between calls: `SET @x`, `USE db`, temporary tables, and
//! `BEGIN` ... `COMMIT` across separate statements, exactly like a `mysql`
//! prompt (and like the PostgreSQL driver's single client). The pool is left to
//! schema introspection, so catalog queries never queue behind a running user
//! statement and never inherit user session state or an open transaction.
use super::sql_text;
use super::*;
use std::sync::atomic::AtomicU32;
use std::time::{Duration, Instant};

/// An idle session is pinged before use once it has been quiet this long, so a
/// connection the server or a NAT gateway dropped is replaced up front instead
/// of failing the user's statement.
const IDLE_PING_AFTER: Duration = Duration::from_secs(10);

/// A ping that has not been answered by now means the transport is gone.
const PING_TIMEOUT: Duration = Duration::from_secs(5);

/// `SHOW WARNINGS` can list up to `max_error_count` (1024) rows; a bulk insert
/// that truncates every row would bury the result panel.
const MAX_REPORTED_WARNINGS: usize = 50;

const RECONNECT_NOTICE: &str = "The server connection was lost and has been re-established; \
session state (variables, current database, open transaction) was reset";

/// Shared between the session and its cancel handle.
#[derive(Default)]
pub(crate) struct SessionControl {
    /// Server-side thread id of the session connection (0 until connected).
    connection_id: AtomicU32,
    /// A user statement is executing right now.
    running: AtomicBool,
    closed: AtomicBool,
}

struct SessionSlot {
    conn: Option<Conn>,
    last_used: Instant,
    /// A connection existed before; if `conn` is `None` again it was lost.
    was_established: bool,
}

pub(crate) struct Session {
    opts: Opts,
    slot: Arc<tokio::sync::Mutex<SessionSlot>>,
    control: Arc<SessionControl>,
    cancel_epoch: Arc<AtomicU64>,
    in_flight: InFlight,
}

impl Session {
    pub(crate) fn new(opts: Opts, cancel_epoch: Arc<AtomicU64>, in_flight: InFlight) -> Self {
        Self {
            opts,
            slot: Arc::new(tokio::sync::Mutex::new(SessionSlot {
                conn: None,
                last_used: Instant::now(),
                was_established: false,
            })),
            control: Arc::new(SessionControl::default()),
            cancel_epoch,
            in_flight,
        }
    }

    pub(crate) fn cancel_handle(&self) -> MySqlCancelHandle {
        MySqlCancelHandle {
            cancel_epoch: self.cancel_epoch.clone(),
            control: self.control.clone(),
            opts: self.opts.clone(),
        }
    }

    /// Run `sql` on the session, waiting for any statement already running.
    pub(crate) async fn run(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.run_inner(sql, params, true)
            .await
            .map(|result| result.expect("a waiting run always produces a result"))
    }

    /// Run `sql` only if the session is free right now; `None` if a statement
    /// is running. For background lookups that must not queue behind the user.
    pub(crate) async fn run_if_idle(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        self.run_inner(sql, params, false).await
    }

    async fn run_inner(
        &self,
        sql: &str,
        params: &[Value],
        wait: bool,
    ) -> Result<Option<QueryResult>> {
        if self.control.closed.load(Ordering::SeqCst) {
            return Err(ZqlzError::Connection(
                "MySQL connection is closed".to_string(),
            ));
        }
        let final_sql = render_mysql_sql_with_params(sql, params)?;
        let _in_flight = self.in_flight.enter();
        let slot = self.slot.clone();
        let control = self.control.clone();
        let opts = self.opts.clone();
        let cancel_epoch = self.cancel_epoch.clone();

        get_mysql_runtime()
            .spawn(async move {
                let mut slot = if wait {
                    slot.lock_owned().await
                } else {
                    match slot.try_lock_owned() {
                        Ok(slot) => slot,
                        Err(_) => return Ok(None),
                    }
                };

                let notice = slot.ensure_connected(&opts, &control).await?;
                // Taken after the lock so a cancel aimed at the statement ahead
                // of this one in the queue does not abort this one too.
                let epoch_at_start = cancel_epoch.load(Ordering::SeqCst);
                let conn = slot.conn.as_mut().expect("connected above");

                control.running.store(true, Ordering::SeqCst);
                let outcome = run_statement(
                    conn,
                    &final_sql,
                    Some((&cancel_epoch, epoch_at_start)),
                    true,
                )
                .await;
                control.running.store(false, Ordering::SeqCst);
                slot.last_used = Instant::now();

                // Only a dead transport maps to `Connection`; SQL errors keep the
                // session (and whatever state it holds) intact.
                if matches!(outcome, Err(ZqlzError::Connection(_))) {
                    slot.drop_connection(&control);
                }

                outcome.map(|mut result| {
                    if let Some(notice) = notice {
                        result.warnings.insert(0, notice);
                    }
                    Some(result)
                })
            })
            .await
            .map_err(|e| ZqlzError::Query(format!("MySQL query task failed: {}", e)))?
    }

    /// Disconnect the session. A statement still running is left to finish; its
    /// connection is released when the last reference goes away.
    pub(crate) async fn close(&self) {
        self.control.closed.store(true, Ordering::SeqCst);
        let slot = self.slot.clone();
        let _ = get_mysql_runtime()
            .spawn(async move {
                let conn = slot.try_lock().ok().and_then(|mut slot| slot.conn.take());
                if let Some(conn) = conn {
                    let _ = conn.disconnect().await;
                }
            })
            .await;
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // mysql_async closes a dropped `Conn` on the Tokio runtime, which this
        // thread (often the UI's) is not part of.
        let conn = self.slot.try_lock().ok().and_then(|mut slot| slot.conn.take());
        if let Some(conn) = conn {
            get_mysql_runtime().spawn(async move {
                let _ = conn.disconnect().await;
            });
        }
    }
}

impl SessionSlot {
    fn drop_connection(&mut self, control: &SessionControl) {
        self.conn = None;
        control.connection_id.store(0, Ordering::SeqCst);
    }

    /// Make sure `self.conn` is a live connection, (re)connecting when needed.
    /// Returns a user-facing notice when an earlier session was replaced,
    /// because that silently discarded its state.
    async fn ensure_connected(
        &mut self,
        opts: &Opts,
        control: &SessionControl,
    ) -> Result<Option<String>> {
        if let Some(conn) = self.conn.as_mut()
            && self.last_used.elapsed() >= IDLE_PING_AFTER
        {
            let alive = matches!(
                tokio::time::timeout(PING_TIMEOUT, conn.ping()).await,
                Ok(Ok(()))
            );
            if !alive {
                tracing::info!("idle MySQL session connection stopped answering; replacing it");
                self.drop_connection(control);
            }
        }

        if self.conn.is_some() {
            return Ok(None);
        }

        let conn = Conn::new(opts.clone())
            .await
            .map_err(|e| connect_error(&e, opts))?;
        control.connection_id.store(conn.id(), Ordering::SeqCst);
        self.conn = Some(conn);

        let replaced = std::mem::replace(&mut self.was_established, true);
        if replaced {
            tracing::info!(
                "MySQL session connection was lost and re-established; session state was reset"
            );
            return Ok(Some(RECONNECT_NOTICE.to_string()));
        }
        Ok(None)
    }
}

/// Cancels the statement running on the pinned session.
///
/// MySQL has no out-of-band cancel packet, so a second connection issues
/// `KILL QUERY <thread id>`. The session itself survives: only the statement
/// dies, with error 1317.
pub struct MySqlCancelHandle {
    pub(super) cancel_epoch: Arc<AtomicU64>,
    pub(super) control: Arc<SessionControl>,
    pub(super) opts: Opts,
}

impl QueryCancelHandle for MySqlCancelHandle {
    fn cancel(&self) {
        // Moving the epoch also stops a row loop that is still converting.
        self.cancel_epoch.fetch_add(1, Ordering::SeqCst);

        let connection_id = self.control.connection_id.load(Ordering::SeqCst);
        if connection_id == 0 || !self.control.running.load(Ordering::SeqCst) {
            tracing::debug!("MySQL cancel requested with no statement running");
            return;
        }

        tracing::debug!(connection_id, "killing running MySQL statement");
        let opts = self.opts.clone();
        get_mysql_runtime().spawn(async move {
            // A fresh connection: the pool may be saturated by introspection, and
            // the session connection is the one that is busy.
            let result = async {
                let mut killer = Conn::new(opts).await?;
                killer
                    .query_drop(format!("KILL QUERY {connection_id}"))
                    .await?;
                killer.disconnect().await
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, connection_id, "failed to cancel MySQL statement");
            }
        });
    }
}

/// Run one statement on a connection and gather everything it produced.
///
/// What the statement is does not matter: if the server answers with a result
/// set (columns), those are rows; otherwise the OK packet's `affected_rows` is
/// all it reports. That makes `CALL`, `EXECUTE`, `SHOW`, `EXPLAIN`, `TABLE t`,
/// `VALUES ROW(1)` and `WITH ... SELECT` come out right without guessing from
/// the leading keyword.
///
/// A statement can produce several result sets (`CALL`, a multi-statement
/// string). `QueryResult` carries one, so the first is returned and each other
/// is reported in `warnings` rather than dropped silently.
///
/// `cancel` is `(epoch, epoch_at_start)`; when the epoch moves the row loop
/// stops. `report_warnings` runs `SHOW WARNINGS` when the server counted any.
pub(super) async fn run_statement(
    conn: &mut Conn,
    sql: &str,
    cancel: Option<(&AtomicU64, u64)>,
    report_warnings: bool,
) -> Result<QueryResult> {
    let started = Instant::now();
    let mut result = conn.query_iter(sql).await.map_err(|e| mysql_error(&e))?;

    let mut shown: Option<(Vec<ColumnMeta>, Vec<Row>)> = None;
    let mut warnings = Vec::new();
    let mut affected_rows = 0u64;
    let mut set_number = 0usize;

    // `columns()` is `None` once every result set has been consumed.
    while let Some(columns) = result.columns() {
        set_number += 1;
        if columns.is_empty() {
            // An OK packet (DML, DDL, the tail of a CALL).
            affected_rows = result.affected_rows();
            result.next().await.map_err(|e| mysql_error(&e))?;
            continue;
        }

        let (meta, names, info) = mysql_column_metadata(&columns);
        let keep = shown.is_none();
        let mut rows = Vec::new();
        let mut row_count = 0usize;
        while let Some(row) = result.next().await.map_err(|e| mysql_error(&e))? {
            if cancel.is_some_and(|(epoch, at_start)| epoch.load(Ordering::SeqCst) != at_start) {
                return Err(ZqlzError::Cancelled);
            }
            row_count += 1;
            if keep {
                rows.push(convert_row(row, &names, &info));
            }
        }

        if keep {
            shown = Some((meta, rows));
        } else {
            warnings.push(format!(
                "Result set {set_number} ({row_count} {}, {} {}) was not shown: \
a statement displays only its first result set",
                if row_count == 1 { "row" } else { "rows" },
                meta.len(),
                if meta.len() == 1 { "column" } else { "columns" },
            ));
        }
    }

    let warning_count = result.warnings();
    drop(result);
    if report_warnings && warning_count > 0 && !sql_text::is_show_diagnostics(sql) {
        warnings.extend(fetch_warnings(conn).await);
    }

    let (columns, rows) = shown.unwrap_or_default();
    let total_rows = rows.len() as u64;
    Ok(QueryResult {
        id: uuid::Uuid::new_v4(),
        // A statement with a result set reports rows, not an affected count.
        affected_rows: if columns.is_empty() { affected_rows } else { 0 },
        columns,
        rows,
        total_rows: Some(total_rows),
        is_estimated_total: false,
        execution_time_ms: started.elapsed().as_millis() as u64,
        warnings,
    })
}

fn convert_row(row: MySqlRow, names: &[String], info: &[MySqlColumnInfo]) -> Row {
    let values = row
        .unwrap()
        .into_iter()
        .zip(info)
        .map(|(value, info)| mysql_value_to_value(value, info))
        .collect();
    Row::new(names.to_vec(), values)
}

/// `Level Code: Message` for every entry in the server's diagnostics area.
/// Failing here must never fail the statement that already succeeded.
async fn fetch_warnings(conn: &mut Conn) -> Vec<String> {
    let rows = match conn.query::<(String, u32, String), _>("SHOW WARNINGS").await {
        Ok(rows) => rows,
        Err(error) => {
            tracing::debug!(%error, "SHOW WARNINGS failed; statement warnings not reported");
            return Vec::new();
        }
    };

    let total = rows.len();
    let mut lines: Vec<String> = rows
        .into_iter()
        .take(MAX_REPORTED_WARNINGS)
        .map(|(level, code, message)| format!("{level} {code}: {message}"))
        .collect();
    if total > MAX_REPORTED_WARNINGS {
        lines.push(format!(
            "... and {} more warnings",
            total - MAX_REPORTED_WARNINGS
        ));
    }
    lines
}

/// A bare `ERROR <sqlstate> (<code>): <message>` line for server errors, so
/// the user reads what the `mysql` client would print. Transport failures map
/// to `Connection` (which also tells the session to reconnect); anything else
/// to `Query`.
///
/// Server errors use `ZqlzError::Other` because its display adds no prefix;
/// `Query` would render as `Query error: ERROR 42S02 ...`.
pub(super) fn mysql_error(error: &mysql_async::Error) -> ZqlzError {
    use mysql_async::Error;

    match error {
        Error::Server(server) => ZqlzError::Other(server.to_string()),
        error if error.is_fatal() => ZqlzError::Connection(describe_mysql_error(error)),
        error => ZqlzError::Query(describe_mysql_error(error)),
    }
}

/// The failure itself, without mysql_async's `Input/output error: `...`'` wrappers.
fn describe_mysql_error(error: &mysql_async::Error) -> String {
    use mysql_async::Error;

    match error {
        Error::Io(error) => error.to_string(),
        Error::Driver(error) => error.to_string(),
        Error::Other(error) => error.to_string(),
        Error::Server(error) => error.to_string(),
        Error::Url(error) => error.to_string(),
    }
}

/// A connect failure: the server's own message (`Access denied`, `Unknown
/// database`) or the io cause (refused, unreachable), prefixed with the target
/// so the user knows what was attempted.
pub(super) fn connect_error(error: &mysql_async::Error, opts: &Opts) -> ZqlzError {
    ZqlzError::Connection(format!(
        "Failed to connect to MySQL at {}:{}: {}",
        opts.ip_or_hostname(),
        opts.tcp_port(),
        describe_mysql_error(error)
    ))
}
