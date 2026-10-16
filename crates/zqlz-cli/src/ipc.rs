//! IPC protocol types and platform IPC client
//!
//! Sends JSON-encoded `Request` messages to a running ZQLZ GUI instance and
//! deserializes the `Response`.  A length-prefixed framing protocol is used:
//! each message is preceded by a 4-byte big-endian `u32` payload length.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(windows)]
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
#[cfg(windows)]
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use crate::standalone::SavedConnection;

// ---------------------------------------------------------------------------
// Serializable result types used by both IPC and output formatting
// ---------------------------------------------------------------------------

/// A single column in a query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
}

/// A single row in a query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// Display text per cell. Servers that send `cells` get it rewritten from
    /// them by [`Row::normalize`]; older servers only send this.
    pub values: Vec<String>,
    /// Typed cells; SQL NULL is `null`. Absent from older servers.
    #[serde(default)]
    pub cells: Vec<serde_json::Value>,
}

impl Row {
    /// Rebuild `values` from the typed `cells` so every output format renders
    /// NULL as `NULL` and structured cells (arrays, JSON) as compact JSON.
    pub fn normalize(&mut self) {
        if self.cells.is_empty() {
            return;
        }
        self.values = self.cells.iter().map(cell_text).collect();
    }
}

/// Text for one typed cell: `NULL`, a string as-is, anything else as compact JSON.
pub fn cell_text(cell: &serde_json::Value) -> String {
    match cell {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// Result of a single SQL statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementResult {
    pub sql: String,
    pub duration_ms: u64,
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Row>,
    pub affected_rows: u64,
    pub error: Option<String>,
    /// Server notices (`RAISE NOTICE`, ...). Absent from older servers.
    #[serde(default)]
    pub warnings: Vec<String>,
    /// Not run because an earlier statement failed. Absent from older servers.
    #[serde(default)]
    pub skipped: bool,
}

/// Result of executing one or more SQL statements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecution {
    pub sql: String,
    pub duration_ms: u64,
    pub statements: Vec<StatementResult>,
}

/// A single query history entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub id: Uuid,
    pub sql: String,
    pub connection_id: Option<Uuid>,
    pub executed_at: chrono::DateTime<chrono::Utc>,
    pub duration_ms: u64,
    pub row_count: Option<u64>,
    pub success: bool,
    pub error: Option<String>,
}

/// Column description for schema browsing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
}

/// One table or view in a listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationEntry {
    pub name: String,
    /// `table`, `view` or `materialized view`.
    pub kind: String,
}

/// View description for schema browsing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewSummary {
    pub name: String,
    pub schema: Option<String>,
    pub is_materialized: bool,
}

/// Index description for schema browsing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSummary {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    pub index_type: String,
}

/// Function or procedure description for schema browsing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSummary {
    pub name: String,
    pub schema: Option<String>,
    pub language: String,
    pub return_type: String,
}

/// Saved connection description (mirrors `SavedConnection`
/// but serializable without GPUI widget baggage).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionSummary {
    pub id: Uuid,
    pub name: String,
    pub driver: String,
    pub host: Option<String>,
    pub port: Option<String>,
    pub database: Option<String>,
    pub username: Option<String>,
    pub folder: Option<String>,
    pub color: Option<String>,
}

impl ConnectionSummary {
    pub fn from_saved(conn: &SavedConnection) -> Self {
        Self {
            id: conn.id,
            name: conn.name.clone(),
            driver: conn.driver.clone(),
            host: conn.params.get("host").cloned(),
            port: conn.params.get("port").cloned(),
            database: conn.params.get("database").cloned(),
            username: conn.params.get("username").cloned(),
            folder: conn.folder.clone(),
            color: conn.color.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// IPC protocol
// ---------------------------------------------------------------------------

/// Requests the CLI sends to the GUI server.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum Request {
    ListConnections,
    SaveConnection(SavedConnection),
    DeleteConnection(Uuid),
    TestConnection(String),
    ExecuteQuery {
        connection: String,
        database: Option<String>,
        sql: String,
        /// Keep running after a failed statement. Omitted (stop at the first
        /// error) unless set, so older servers see the request they know.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        continue_on_error: Option<bool>,
    },
    ListDatabases(String),
    ListTables {
        connection: String,
        database: Option<String>,
        /// Also list views, with their kind. Only sent when asked, so older
        /// servers see the request they know (and answer with tables only).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        include_views: bool,
    },
    ListColumns {
        connection: String,
        database: Option<String>,
        table: String,
    },
    QueryHistory {
        connection: Option<String>,
        limit: usize,
        search: Option<String>,
    },
    QueryHistoryEntry {
        id: Uuid,
    },
    OpenTargets {
        targets: Vec<String>,
    },
    AgentCall {
        name: String,
        arguments: serde_json::Value,
    },
}

/// Responses the GUI server sends back.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum Response {
    Ok,
    Error(String),
    Connections(Vec<ConnectionSummary>),
    QueryResult(QueryExecution),
    StringList(Vec<String>),
    /// Tables and views with their kind; the answer to `include_views`.
    Relations(Vec<RelationEntry>),
    Columns(Vec<ColumnInfo>),
    History(Vec<HistoryEntry>),
    HistoryEntry(HistoryEntry),
    Json(serde_json::Value),
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Send a single request to the running GUI server and return the response.
pub async fn send_request(socket_path: &Path, request: Request) -> Result<Response> {
    #[cfg(unix)]
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("connecting to IPC socket at {}", socket_path.display()))?;

    #[cfg(windows)]
    let mut stream = connect_windows_named_pipe(socket_path)
        .await
        .with_context(|| format!("connecting to IPC named pipe at {}", socket_path.display()))?;

    let payload = serde_json::to_vec(&request).context("serializing IPC request")?;

    // Write length-prefixed frame
    let len = payload.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .context("writing IPC frame length")?;
    stream
        .write_all(&payload)
        .await
        .context("writing IPC request payload")?;

    // Read length-prefixed response
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .context("reading IPC response length")?;
    let response_len = u32::from_be_bytes(len_buf) as usize;

    if response_len > 64 * 1024 * 1024 {
        bail!("IPC response too large ({} bytes)", response_len);
    }

    let mut response_buf = vec![0u8; response_len];
    stream
        .read_exact(&mut response_buf)
        .await
        .context("reading IPC response payload")?;

    let mut response: Response =
        serde_json::from_slice(&response_buf).context("deserializing IPC response")?;

    if let Response::QueryResult(execution) = &mut response {
        for row in execution
            .statements
            .iter_mut()
            .flat_map(|s| s.rows.iter_mut())
        {
            row.normalize();
        }
    }

    Ok(response)
}

#[cfg(windows)]
async fn connect_windows_named_pipe(socket_path: &Path) -> Result<NamedPipeClient> {
    const PIPE_BUSY: i32 = 231;

    for _ in 0..20 {
        match ClientOptions::new().open(socket_path) {
            Ok(client) => return Ok(client),
            Err(error) if error.raw_os_error() == Some(PIPE_BUSY) => {
                sleep(Duration::from_millis(25)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }

    Err(anyhow::anyhow!(
        "timed out waiting for named pipe server availability"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_result_from_an_older_server_still_parses() {
        let statement: StatementResult = serde_json::from_value(serde_json::json!({
            "sql": "SELECT 1", "duration_ms": 1, "columns": [], "rows": [],
            "affected_rows": 0, "error": null
        }))
        .unwrap();
        assert!(statement.warnings.is_empty() && !statement.skipped);
    }

    #[test]
    fn list_tables_request_only_carries_include_views_when_asked() {
        let request = |include_views| {
            serde_json::to_value(Request::ListTables {
                connection: "c".into(),
                database: None,
                include_views,
            })
            .unwrap()
        };
        assert!(request(false)["payload"].get("include_views").is_none());
        assert_eq!(request(true)["payload"]["include_views"], true);
    }

    #[test]
    fn execute_request_only_carries_continue_on_error_when_asked() {
        let request = |continue_on_error| {
            serde_json::to_value(Request::ExecuteQuery {
                connection: "c".into(),
                database: None,
                sql: "SELECT 1".into(),
                continue_on_error,
            })
            .unwrap()
        };
        assert!(request(None)["payload"].get("continue_on_error").is_none());
        assert_eq!(request(Some(true))["payload"]["continue_on_error"], true);
    }
}
