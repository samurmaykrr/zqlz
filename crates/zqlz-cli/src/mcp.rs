//! `zqlz mcp`: a Model Context Protocol server over stdio.
//!
//! Dual-era per the MCP versioning rules. Modern clients (2026-07-28) send
//! protocol metadata in every request's `_meta` and may probe with
//! `server/discover`. Legacy clients (2025-11-25 and earlier) open with an
//! `initialize` handshake. Tools run inside the ZQLZ GUI over IPC, so agents
//! act in the app the user is watching instead of behind its back.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::ipc::{Request, Response, send_request};

const MODERN_VERSIONS: &[&str] = &["2026-07-28"];
/// Legacy revisions whose server obligations this implementation meets.
/// 2025-03-26 is left out because it requires accepting JSON-RPC batches.
const LEGACY_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2024-11-05"];

const META_PROTOCOL_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
const META_CLIENT_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";
const META_SERVER_INFO: &str = "io.modelcontextprotocol/serverInfo";

const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const PARSE_ERROR: i64 = -32700;
const UNSUPPORTED_PROTOCOL_VERSION: i64 = -32022;

const INSTRUCTIONS: &str = "These tools drive the ZQLZ database IDE the user has open. Start with \
    get_context. Everything you run shows up live in ZQLZ, and writes go through apply_changes, \
    where the user may be asked to approve them.";
const NOT_RUNNING: &str = "ZQLZ is not running. Open ZQLZ and try again.";

pub async fn run(socket_path: &Path) -> Result<()> {
    let server = Arc::new(Server::new(socket_path.to_path_buf()));
    let in_flight = InFlight::default();
    let (outgoing, mut outgoing_receiver) = mpsc::unbounded_channel::<Value>();
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(message) = outgoing_receiver.recv().await {
            // serde_json never emits raw newlines, which keeps one message per line.
            let mut line = message.to_string();
            line.push('\n');
            stdout
                .write_all(line.as_bytes())
                .await
                .context("writing MCP response")?;
            stdout.flush().await.context("flushing MCP response")?;
        }
        anyhow::Ok(())
    });

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await.context("reading MCP request")? {
        if line.trim().is_empty() {
            continue;
        }
        let message: Value = match serde_json::from_str(&line) {
            Ok(message) => message,
            Err(error) => {
                send(
                    &outgoing,
                    error_response(Value::Null, PARSE_ERROR, &error.to_string(), None),
                );
                continue;
            }
        };

        if message.get("method").and_then(Value::as_str) == Some("notifications/cancelled") {
            if let Some(request_id) = message.pointer("/params/requestId") {
                in_flight.cancel(request_id);
            }
            continue;
        }

        // Requests run concurrently: an apply_changes call can wait minutes for
        // the user to approve it, and must not block other requests.
        let request_id = message.get("id").cloned();
        let server = server.clone();
        let outgoing = outgoing.clone();
        let in_flight_for_task = in_flight.clone();
        in_flight.spawn(request_id.clone(), async move {
            let Some(response) = server.respond(message).await else {
                return;
            };
            if in_flight_for_task.finish(request_id.as_ref()) {
                send(&outgoing, response);
            }
        });
    }

    // stdin closing is the client's shutdown signal; drop pending work and exit.
    in_flight.cancel_all();
    drop(outgoing);
    writer.await.context("MCP writer task panicked")?
}

fn send(outgoing: &mpsc::UnboundedSender<Value>, message: Value) {
    if outgoing.send(message).is_err() {
        tracing::warn!("MCP writer stopped; dropping response");
    }
}

/// Requests still running, so `notifications/cancelled` can stop them. Exactly
/// one of `finish` and `cancel` wins for a request, so a cancelled request
/// never gets a response.
#[derive(Clone, Default)]
struct InFlight(Arc<Mutex<HashMap<String, AbortHandle>>>);

impl InFlight {
    fn spawn(&self, request_id: Option<Value>, task: impl Future<Output = ()> + Send + 'static) {
        // Holding the lock across spawn keeps a fast task from finishing
        // before its handle is registered.
        let mut requests = self.lock();
        let handle = tokio::spawn(task).abort_handle();
        if let Some(request_id) = request_id {
            requests.insert(request_id.to_string(), handle);
        }
    }

    /// Returns whether the request was still live and may be answered.
    fn finish(&self, request_id: Option<&Value>) -> bool {
        request_id.is_none_or(|request_id| self.lock().remove(&request_id.to_string()).is_some())
    }

    fn cancel(&self, request_id: &Value) {
        if let Some(handle) = self.lock().remove(&request_id.to_string()) {
            handle.abort();
        }
    }

    fn cancel_all(&self) {
        for (_, handle) in self.lock().drain() {
            handle.abort();
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, AbortHandle>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

struct RpcError {
    code: i64,
    message: String,
    data: Option<Value>,
}

impl RpcError {
    fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

struct Server {
    socket_path: PathBuf,
    /// Set once a legacy client has sent `initialize`; its later requests carry
    /// no per-request protocol metadata.
    legacy_initialized: AtomicBool,
}

impl Server {
    fn new(socket_path: PathBuf) -> Self {
        Self {
            socket_path,
            legacy_initialized: AtomicBool::new(false),
        }
    }

    /// Answers one JSON-RPC message. Notifications and stray responses get no answer.
    async fn respond(&self, message: Value) -> Option<Value> {
        if message.get("result").is_some() || message.get("error").is_some() {
            return None;
        }
        let id = message.get("id").cloned();
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return Some(error_response(
                id.unwrap_or(Value::Null),
                INVALID_REQUEST,
                "Invalid Request: missing method",
                None,
            ));
        };
        let id = id?;
        if !(id.is_string() || id.is_i64() || id.is_u64()) {
            return Some(error_response(
                Value::Null,
                INVALID_REQUEST,
                "Invalid Request: id must be a string or integer",
                None,
            ));
        }
        let params = message.get("params").cloned().unwrap_or_else(|| json!({}));

        Some(match self.handle(method, &params).await {
            Ok(mut result) => {
                if let Some(result) = result.as_object_mut() {
                    result.insert("resultType".into(), json!("complete"));
                    result.insert("_meta".into(), json!({ META_SERVER_INFO: server_info() }));
                }
                json!({ "jsonrpc": "2.0", "id": id, "result": result })
            }
            Err(error) => error_response(id, error.code, &error.message, error.data),
        })
    }

    async fn handle(&self, method: &str, params: &Value) -> Result<Value, RpcError> {
        if method == "initialize" {
            return Ok(self.initialize(params));
        }
        self.check_request_meta(method, params)?;

        match method {
            "server/discover" => Ok(json!({
                "supportedVersions": MODERN_VERSIONS,
                "capabilities": { "tools": {} },
                "instructions": INSTRUCTIONS,
            })),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({ "tools": tool_definitions() })),
            "tools/call" => self.call_tool(params).await,
            _ => Err(RpcError::new(
                METHOD_NOT_FOUND,
                format!("Method not found: {method}"),
            )),
        }
    }

    /// Legacy handshake: echo the client's version when we support it,
    /// otherwise offer our latest legacy version.
    fn initialize(&self, params: &Value) -> Value {
        let version = params
            .get("protocolVersion")
            .and_then(Value::as_str)
            .filter(|version| LEGACY_VERSIONS.contains(version))
            .unwrap_or(LEGACY_VERSIONS[0]);
        self.legacy_initialized.store(true, Ordering::SeqCst);
        json!({
            "protocolVersion": version,
            "capabilities": { "tools": {} },
            "serverInfo": server_info(),
            "instructions": INSTRUCTIONS,
        })
    }

    /// Modern requests must carry their protocol version and capabilities.
    /// Requests without them are only valid from an initialized legacy client.
    fn check_request_meta(&self, method: &str, params: &Value) -> Result<(), RpcError> {
        let meta = params.get("_meta");
        let Some(version) = meta.and_then(|meta| meta.get(META_PROTOCOL_VERSION)) else {
            if method == "ping" || self.legacy_initialized.load(Ordering::SeqCst) {
                return Ok(());
            }
            return Err(RpcError::new(
                INVALID_PARAMS,
                format!(
                    "Missing _meta[\"{META_PROTOCOL_VERSION}\"]. Legacy clients must send \
                     initialize first."
                ),
            ));
        };

        let version = version.as_str().unwrap_or_default();
        if !MODERN_VERSIONS.contains(&version) {
            return Err(RpcError {
                code: UNSUPPORTED_PROTOCOL_VERSION,
                message: "Unsupported protocol version".into(),
                data: Some(json!({ "supported": MODERN_VERSIONS, "requested": version })),
            });
        }
        if !meta
            .and_then(|meta| meta.get(META_CLIENT_CAPABILITIES))
            .is_some_and(Value::is_object)
        {
            return Err(RpcError::new(
                INVALID_PARAMS,
                format!("Missing _meta[\"{META_CLIENT_CAPABILITIES}\"]."),
            ));
        }
        Ok(())
    }

    async fn call_tool(&self, params: &Value) -> Result<Value, RpcError> {
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return Err(RpcError::new(
                INVALID_PARAMS,
                "tools/call requires a string `name`",
            ));
        };
        let is_known = tool_definitions()
            .as_array()
            .is_some_and(|tools| tools.iter().any(|tool| tool["name"] == name));
        if !is_known {
            return Err(RpcError::new(
                INVALID_PARAMS,
                format!("Unknown tool: {name}"),
            ));
        }
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if !arguments.is_object() {
            return Err(RpcError::new(
                INVALID_PARAMS,
                "`arguments` must be an object",
            ));
        }

        // Failures past this point are tool execution errors: the model can
        // read them and adjust, so they go in the result with isError.
        let request = Request::AgentCall {
            name: name.to_string(),
            arguments,
        };
        let (text, is_error) = match send_request(&self.socket_path, request).await {
            Ok(Response::Json(value)) => (
                serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string()),
                false,
            ),
            Ok(Response::Error(error)) => (error, true),
            Ok(_) => ("Unexpected response from ZQLZ.".to_string(), true),
            Err(_) => (NOT_RUNNING.to_string(), true),
        };
        Ok(json!({ "content": [{ "type": "text", "text": text }], "isError": is_error }))
    }
}

fn server_info() -> Value {
    json!({ "name": "zqlz", "title": "ZQLZ", "version": env!("CARGO_PKG_VERSION") })
}

fn error_response(id: Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut error = json!({ "code": code, "message": message });
    if let Some(data) = data {
        error["data"] = data;
    }
    json!({ "jsonrpc": "2.0", "id": id, "error": error })
}

fn tool_definitions() -> Value {
    let connection = json!({
        "type": "string",
        "description": "Saved connection name (case-insensitive) or id, as listed by get_context."
    });
    let database = json!({
        "type": "string",
        "description": "Database to target. Defaults to the connection's current database."
    });
    let table = json!({ "type": "string", "description": "Table name." });
    let sql = json!({ "type": "string", "description": "SQL to run." });

    json!([
        {
            "name": "get_context",
            "description": "What the user is looking at in ZQLZ: saved connections (connected state and \
                agent_access level), the active connection and database, and the active tab. For a \
                query tab this includes its SQL and selection, so you can work on \"this query\". \
                Call this first.",
            "inputSchema": { "type": "object", "additionalProperties": false }
        },
        {
            "name": "connect",
            "description": "Connect a saved connection in ZQLZ; the user sees it connect in the sidebar. \
                Required before running SQL on a disconnected connection.",
            "inputSchema": {
                "type": "object",
                "properties": { "connection": connection },
                "required": ["connection"]
            }
        },
        {
            "name": "describe_schema",
            "description": "Without `table`, list the tables. With `table`, list its columns (name, \
                type, nullable, default, primary key).",
            "inputSchema": {
                "type": "object",
                "properties": { "connection": connection, "database": database, "table": table },
                "required": ["connection"]
            }
        },
        {
            "name": "run_query",
            "description": "Run read-only SQL (SELECT, WITH, SHOW, EXPLAIN) in the connection's Agent \
                tab. The user sees the SQL and the results grid. Returns up to 100 rows per statement; cells \
                are typed JSON (SQL NULL is null, arrays and JSON are structured, bytes are \"\\x<hex>\"). \
                Anything that could write is refused (on PostgreSQL and SQLite the database itself also \
                runs it read-only; on MySQL only the SQL check applies); use apply_changes for writes.",
            "inputSchema": {
                "type": "object",
                "properties": { "connection": connection, "sql": sql, "database": database },
                "required": ["connection", "sql"]
            }
        },
        {
            "name": "apply_changes",
            "description": "Run SQL that changes data or schema (INSERT, UPDATE, DELETE, CREATE, ALTER, \
                DROP, ...). Depending on the connection's agent_access the user first approves it in a \
                dialog (ask), it runs immediately (auto), or it is refused (read_only). Open table \
                grids and the schema sidebar refresh afterwards.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "connection": connection,
                    "sql": sql,
                    "reason": {
                        "type": "string",
                        "description": "One sentence shown to the user in the approval dialog: what this does and why."
                    },
                    "database": database
                },
                "required": ["connection", "sql", "reason"]
            }
        },
        {
            "name": "explain_query",
            "description": "Show the query plan for `sql` in ZQLZ's visual EXPLAIN view and return the \
                plan rows. Does not run the query.",
            "inputSchema": {
                "type": "object",
                "properties": { "connection": connection, "sql": sql, "database": database },
                "required": ["connection", "sql"]
            }
        },
        {
            "name": "edit_editor",
            "description": "Replace the text of the user's query tab (the focused one, or their most \
                recent one if the Agent tab is focused). One undo step, so the user can Cmd+Z. Does \
                not run it.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sql": { "type": "string", "description": "New text." },
                    "replace_selection": {
                        "type": "boolean",
                        "description": "Replace only the selected text instead of the whole tab."
                    }
                },
                "required": ["sql"]
            }
        },
        {
            "name": "open_table",
            "description": "Open a table's data grid for the user.",
            "inputSchema": {
                "type": "object",
                "properties": { "connection": connection, "table": table, "database": database },
                "required": ["connection", "table"]
            }
        },
        {
            "name": "open_designer",
            "description": "Open the table designer on an existing table for the user.",
            "inputSchema": {
                "type": "object",
                "properties": { "connection": connection, "table": table },
                "required": ["connection", "table"]
            }
        }
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn modern_meta() -> Value {
        json!({
            META_PROTOCOL_VERSION: "2026-07-28",
            META_CLIENT_CAPABILITIES: {},
            "io.modelcontextprotocol/clientInfo": { "name": "test", "version": "1" }
        })
    }

    async fn request(server: &Server, id: i64, method: &str, params: Value) -> Value {
        server
            .respond(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await
            .expect("requests are answered")
    }

    fn not_running_server() -> Server {
        Server::new(PathBuf::from("/nonexistent/zqlz-test.sock"))
    }

    #[tokio::test]
    async fn modern_requests_carry_their_own_protocol_metadata() {
        let server = not_running_server();

        let discover = request(
            &server,
            1,
            "server/discover",
            json!({ "_meta": modern_meta() }),
        )
        .await;
        assert_eq!(discover["result"]["resultType"], "complete");
        assert_eq!(
            discover["result"]["supportedVersions"],
            json!(MODERN_VERSIONS)
        );
        assert!(discover["result"]["_meta"][META_SERVER_INFO]["name"].is_string());

        let tools = request(&server, 2, "tools/list", json!({ "_meta": modern_meta() })).await;
        assert!(
            tools["result"]["tools"]
                .as_array()
                .is_some_and(|tools| tools.len() == 9)
        );

        let missing = request(&server, 3, "tools/list", json!({})).await;
        assert_eq!(missing["error"]["code"], INVALID_PARAMS);

        let mut old = modern_meta();
        old[META_PROTOCOL_VERSION] = json!("1900-01-01");
        let unsupported = request(&server, 4, "tools/list", json!({ "_meta": old })).await;
        assert_eq!(unsupported["error"]["code"], UNSUPPORTED_PROTOCOL_VERSION);
        assert_eq!(unsupported["error"]["data"]["requested"], "1900-01-01");

        let mut no_capabilities = modern_meta();
        no_capabilities
            .as_object_mut()
            .map(|meta| meta.remove(META_CLIENT_CAPABILITIES));
        let malformed = request(
            &server,
            5,
            "tools/list",
            json!({ "_meta": no_capabilities }),
        )
        .await;
        assert_eq!(malformed["error"]["code"], INVALID_PARAMS);
    }

    #[tokio::test]
    async fn legacy_clients_negotiate_with_initialize() {
        let server = not_running_server();

        let echoed = request(
            &server,
            1,
            "initialize",
            json!({ "protocolVersion": "2025-06-18" }),
        )
        .await;
        assert_eq!(echoed["result"]["protocolVersion"], "2025-06-18");
        assert!(echoed["result"]["capabilities"]["tools"].is_object());

        let fallback = request(
            &server,
            2,
            "initialize",
            json!({ "protocolVersion": "2025-03-26" }),
        )
        .await;
        assert_eq!(fallback["result"]["protocolVersion"], LEGACY_VERSIONS[0]);

        let tools = request(&server, 3, "tools/list", json!({})).await;
        assert!(tools["result"]["tools"].is_array());
    }

    #[tokio::test]
    async fn tool_errors_are_split_between_protocol_and_execution() {
        let server = not_running_server();
        let params = |name: &str| json!({ "_meta": modern_meta(), "name": name, "arguments": {} });

        let unknown = request(&server, 1, "tools/call", params("drop_everything")).await;
        assert_eq!(unknown["error"]["code"], INVALID_PARAMS);

        let offline = request(&server, 2, "tools/call", params("get_context")).await;
        assert_eq!(offline["result"]["isError"], true);
        assert_eq!(offline["result"]["content"][0]["text"], NOT_RUNNING);

        let unknown_method = request(&server, 3, "nope", json!({ "_meta": modern_meta() })).await;
        assert_eq!(unknown_method["error"]["code"], METHOD_NOT_FOUND);

        let notification = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
        assert!(server.respond(notification).await.is_none());
    }

    #[tokio::test]
    async fn cancelled_requests_are_never_answered() {
        let in_flight = InFlight::default();
        let request_id = json!(7);
        in_flight.spawn(Some(request_id.clone()), std::future::pending());
        in_flight.cancel(&request_id);
        assert!(!in_flight.finish(Some(&request_id)));

        in_flight.spawn(Some(json!(8)), async {});
        assert!(in_flight.finish(Some(&json!(8))));
        assert!(in_flight.finish(None));
    }
}
