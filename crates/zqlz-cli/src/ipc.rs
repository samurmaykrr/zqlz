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
    pub values: Vec<String>,
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
    },
    ListDatabases(String),
    ListTables {
        connection: String,
        database: Option<String>,
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
    Columns(Vec<ColumnInfo>),
    History(Vec<HistoryEntry>),
    HistoryEntry(HistoryEntry),
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

    let response: Response =
        serde_json::from_slice(&response_buf).context("deserializing IPC response")?;

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
