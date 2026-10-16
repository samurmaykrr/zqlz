//! `zqlz query` against a fake GUI that answers over the IPC socket.
#![cfg(unix)]

use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::process::Command;

/// Serve one framed request with `response`; returns the request the CLI sent.
fn serve_once(listener: UnixListener, response: serde_json::Value) -> serde_json::Value {
    let (mut stream, _) = listener.accept().unwrap();
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).unwrap();
    let mut request = vec![0u8; u32::from_be_bytes(len) as usize];
    stream.read_exact(&mut request).unwrap();

    let payload = serde_json::to_vec(&response).unwrap();
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&payload).unwrap();
    serde_json::from_slice(&request).unwrap()
}

fn statement(sql: &str, error: Option<&str>, skipped: bool) -> serde_json::Value {
    serde_json::json!({
        "sql": sql, "duration_ms": 1, "columns": [], "rows": [],
        "affected_rows": 0, "error": error, "warnings": [], "skipped": skipped,
    })
}

fn run_against_fake_gui(extra: &[&str]) -> (std::process::Output, serde_json::Value) {
    // Unix socket paths are limited to ~100 bytes; $TMPDIR on macOS is longer.
    let dir = std::path::PathBuf::from("/tmp").join(format!(
        "zqlz-ipc-{}",
        &uuid::Uuid::new_v4().to_string()[..8]
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let socket = dir.join("ipc.sock");
    let listener = UnixListener::bind(&socket).unwrap();

    let response = serde_json::json!({
        "type": "QueryResult",
        "payload": {
            "sql": "bad; next", "duration_ms": 2,
            "statements": [statement("bad", Some("boom"), false), statement("next", None, true)],
        },
    });
    let server = std::thread::spawn(move || serve_once(listener, response));

    let output = Command::new(env!("CARGO_BIN_EXE_zqlz"))
        .env("HOME", &dir)
        .arg("--socket")
        .arg(&socket)
        .args(["query", "-c", "any"])
        .args(extra)
        .arg("bad; next")
        .output()
        .unwrap();
    let request = server.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    (output, request)
}

#[test]
fn ipc_run_with_a_failed_statement_exits_non_zero_and_shows_skipped() {
    let (output, request) = run_against_fake_gui(&[]);

    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Error: boom"), "{stdout}");
    assert!(stdout.contains("Skipped: next"), "{stdout}");
    assert!(
        request["payload"].get("continue_on_error").is_none(),
        "{request}"
    );
}

#[test]
fn continue_on_error_is_forwarded_to_the_gui() {
    let (_, request) = run_against_fake_gui(&["--continue-on-error"]);
    assert_eq!(request["payload"]["continue_on_error"], true);
}

/// `schema tables` against a fake GUI that answers with `response`.
fn schema_tables_against_fake_gui(
    extra: &[&str],
    response: serde_json::Value,
) -> (std::process::Output, serde_json::Value) {
    let dir = std::path::PathBuf::from("/tmp").join(format!(
        "zqlz-ipc-{}",
        &uuid::Uuid::new_v4().to_string()[..8]
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let socket = dir.join("ipc.sock");
    let listener = UnixListener::bind(&socket).unwrap();

    // `schema` resolves the connection locally first.
    let added = Command::new(env!("CARGO_BIN_EXE_zqlz"))
        .env("HOME", &dir)
        .arg("--standalone")
        .args(["connections", "add", "--name", "any", "--driver", "sqlite", "--path"])
        .arg(dir.join("t.sqlite"))
        .output()
        .unwrap();
    assert!(added.status.success());

    let server = std::thread::spawn(move || serve_once(listener, response));
    let output = Command::new(env!("CARGO_BIN_EXE_zqlz"))
        .env("HOME", &dir)
        .arg("--socket")
        .arg(&socket)
        .args(["schema", "-c", "any", "tables"])
        .args(extra)
        .output()
        .unwrap();
    let request = server.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    (output, request)
}

#[test]
fn schema_tables_asks_for_views_only_when_told_to() {
    let names = serde_json::json!({"type": "StringList", "payload": ["a", "b"]});
    let (output, request) = schema_tables_against_fake_gui(&[], names);
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "a\nb\n");
    assert!(request["payload"].get("include_views").is_none(), "{request}");
}

#[test]
fn schema_tables_views_prints_the_kind_of_each_relation() {
    let relations = serde_json::json!({
        "type": "Relations",
        "payload": [{"name": "t", "kind": "table"}, {"name": "v", "kind": "view"}],
    });
    let (output, request) = schema_tables_against_fake_gui(&["--views"], relations);
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "t\ttable\nv\tview\n");
    assert_eq!(request["payload"]["include_views"], true);
}

#[test]
fn schema_tables_views_against_an_older_gui_still_lists_its_tables() {
    let names = serde_json::json!({"type": "StringList", "payload": ["a"]});
    let (output, _) = schema_tables_against_fake_gui(&["--views"], names);
    assert_eq!(String::from_utf8_lossy(&output.stdout), "a\ttable\n");
}
