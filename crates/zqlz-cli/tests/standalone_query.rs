//! End-to-end checks of `zqlz --standalone query` against a throwaway SQLite
//! database, run with a private `$HOME` so the real storage is never touched.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct Sandbox {
    home: PathBuf,
}

impl Sandbox {
    fn new() -> Self {
        let home = std::env::temp_dir().join(format!("zqlz-cli-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&home).unwrap();
        let sandbox = Self { home };
        let db = sandbox.home.join("test.sqlite");
        let added = sandbox.run(&[
            "connections",
            "add",
            "--name",
            "t",
            "--driver",
            "sqlite",
            "--path",
            db.to_str().unwrap(),
        ]);
        assert!(added.status.success(), "{}", text(&added.stderr));
        sandbox
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_zqlz"))
            .env("HOME", &self.home)
            .env_remove("ZQLZ_IPC_SOCKET")
            .arg("--standalone")
            .args(args)
            .output()
            .unwrap()
    }

    fn query(&self, extra: &[&str], sql: &str) -> Output {
        let mut args = vec!["query", "-c", "t"];
        args.extend_from_slice(extra);
        args.push(sql);
        self.run(&args)
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        remove(&self.home);
    }
}

fn remove(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[test]
fn a_failing_statement_stops_the_run_and_exits_non_zero() {
    let sandbox = Sandbox::new();
    let out = sandbox.query(
        &[],
        "SELECT 1 AS one; SELECT * FROM no_such_table; SELECT 3 AS three",
    );

    assert_eq!(out.status.code(), Some(1));
    let stdout = text(&out.stdout);
    assert!(stdout.contains("one"), "{stdout}");
    assert!(
        stdout.contains("Error:") && stdout.contains("no_such_table"),
        "{stdout}"
    );
    assert!(stdout.contains("Skipped: SELECT 3 AS three"), "{stdout}");
    assert!(
        !stdout.contains("three  ") && !stdout.contains("│ 3"),
        "{stdout}"
    );
}

#[test]
fn continue_on_error_runs_the_rest_but_still_exits_non_zero() {
    let sandbox = Sandbox::new();
    let out = sandbox.query(
        &["--continue-on-error"],
        "SELECT 1 AS one; SELECT * FROM no_such_table; SELECT 3 AS three",
    );

    assert_eq!(out.status.code(), Some(1));
    let stdout = text(&out.stdout);
    assert!(stdout.contains("three"), "{stdout}");
    assert!(!stdout.contains("Skipped"), "{stdout}");
}

#[test]
fn a_clean_run_exits_zero_and_json_is_one_object_per_statement() {
    let sandbox = Sandbox::new();
    let out = sandbox.query(
        &["-f", "json"],
        "SELECT NULL AS a, 'NULL' AS b; SELECT 2 AS c",
    );

    assert!(out.status.success(), "{}", text(&out.stderr));
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(parsed[0]["rows"][0]["a"], serde_json::Value::Null);
    assert_eq!(parsed[0]["rows"][0]["b"], "NULL");
    assert_eq!(parsed[1]["rows"][0]["c"], 2);
    assert_eq!(parsed[1]["skipped"], false);
}

#[test]
fn json_reports_skipped_statements() {
    let sandbox = Sandbox::new();
    let out = sandbox.query(&["-f", "json"], "SELECT * FROM no_such_table; SELECT 2");

    assert_eq!(out.status.code(), Some(1));
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(parsed[0]["error"].is_string());
    assert_eq!(parsed[1]["skipped"], true);
}

#[test]
fn single_transaction_rolls_back_when_a_statement_fails() {
    let sandbox = Sandbox::new();
    let out = sandbox.query(
        &["--single-transaction"],
        "CREATE TABLE tx (id INT); INSERT INTO tx VALUES (1); SELECT * FROM no_such_table",
    );
    assert_eq!(out.status.code(), Some(1));

    let out = sandbox.query(&[], "SELECT * FROM tx");
    assert_eq!(
        out.status.code(),
        Some(1),
        "the table must not have survived"
    );
}

/// Needs the docker Postgres from the shots setup (127.0.0.1:55432, nimbus).
#[test]
#[ignore = "needs the live Postgres on 127.0.0.1:55432"]
fn live_postgres_keeps_returning_rows_notices_and_connect_errors() {
    let sandbox = Sandbox::new();
    let add = |name: &str, database: &str| {
        let added = sandbox.run(&[
            "connections",
            "add",
            "--name",
            name,
            "--driver",
            "postgres",
            "--host",
            "127.0.0.1",
            "--port",
            "55432",
            "--username",
            "nimbus",
            "--password",
            "nimbus-demo",
            "--database",
            database,
        ]);
        assert!(added.status.success());
    };
    add("pg", "postgres");
    add("nodb", "zqlz_no_such_db");

    let out = sandbox.run(&[
        "query",
        "-c",
        "pg",
        "-f",
        "json",
        "CREATE TEMP TABLE t (id int, s text); \
         INSERT INTO t VALUES (1, NULL), (2, 'NULL') RETURNING id, s; \
         DO $$ BEGIN RAISE NOTICE 'hello'; END $$",
    ]);
    assert!(out.status.success(), "{}", text(&out.stderr));
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        parsed[1]["rows"][0],
        serde_json::json!({"id": 1, "s": null})
    );
    assert_eq!(
        parsed[1]["rows"][1],
        serde_json::json!({"id": 2, "s": "NULL"})
    );
    assert_eq!(parsed[2]["warnings"], serde_json::json!(["NOTICE: hello"]));

    let out = sandbox.run(&["connect", "nodb"]);
    assert_eq!(out.status.code(), Some(1));
    let stderr = text(&out.stderr);
    assert!(
        stderr.contains("does not exist (SQLSTATE 3D000)"),
        "{stderr}"
    );
}

#[cfg(unix)]
#[test]
fn a_damaged_saved_connection_does_not_hide_the_others() {
    let sandbox = Sandbox::new();
    let storage = sandbox.home.join(".config/zqlz/storage.db");
    let db = zqlz_internal_storage::rusqlite::Connection::open(&storage).unwrap();
    db.execute(
        "INSERT INTO connections (id, name, driver, params_json, created_at, updated_at)
         VALUES ('not-a-uuid', 'broken', 'mysql', '{}', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')",
        [],
    )
    .unwrap();
    drop(db);

    let out = sandbox.query(&[], "SELECT 1 AS one");
    assert!(out.status.success(), "{}", text(&out.stderr));
    assert!(text(&out.stderr).contains("skipping saved connection 'broken'"));
}

/// Live MySQL, configured like the other opt-in tests: `ZQLZ_MYSQL_HOST`
/// (127.0.0.1), `ZQLZ_MYSQL_PORT` (3307), `ZQLZ_MYSQL_USER` (root),
/// `ZQLZ_MYSQL_PASSWORD` (root) and `ZQLZ_MYSQL_DATABASE` (a scratch database
/// this test creates and drops, default `zqlz_cli_test`).
#[test]
#[ignore = "needs a live MySQL (see ZQLZ_MYSQL_* in the doc comment)"]
fn live_mysql_scripts_values_views_and_unknown_databases() {
    let env = |key: &str, default: &str| std::env::var(key).unwrap_or_else(|_| default.to_string());
    let database = env("ZQLZ_MYSQL_DATABASE", "zqlz_cli_test");
    let sandbox = Sandbox::new();
    let added = sandbox.run(&[
        "connections", "add", "--name", "my", "--driver", "mysql",
        "--host", &env("ZQLZ_MYSQL_HOST", "127.0.0.1"),
        "--port", &env("ZQLZ_MYSQL_PORT", "3307"),
        "--username", &env("ZQLZ_MYSQL_USER", "root"),
        "--password", &env("ZQLZ_MYSQL_PASSWORD", "root"),
    ]);
    assert!(added.status.success(), "{}", text(&added.stderr));
    let my = |args: &[&str]| {
        let mut full = vec![args[0], "-c", "my"];
        full.extend_from_slice(&args[1..]);
        sandbox.run(&full)
    };
    let json = |out: &Output| -> serde_json::Value {
        serde_json::from_slice(&out.stdout).unwrap_or_else(|e| panic!("{e}: {}", text(&out.stdout)))
    };

    // DELIMITER, `#` comments, backslash escapes and session state in one script.
    let out = my(&["query", "-f", "json", &format!(
        "DROP DATABASE IF EXISTS `{database}`; CREATE DATABASE `{database}`; USE `{database}`;
         CREATE TABLE `select` (`from` INT PRIMARY KEY, big BIGINT UNSIGNED, d DECIMAL(10,2), n TEXT, b VARBINARY(4), j JSON);
         INSERT INTO `select` VALUES (1, 18446744073709551615, 1.50, NULL, X'DEADBEEF', JSON_OBJECT('a', 1));
         # not a statement; it\\'s a comment
         CREATE VIEW `View With Spaces` AS SELECT `from` FROM `select`;
         DELIMITER //
         CREATE PROCEDURE p() BEGIN SELECT 1 AS one; END//
         DELIMITER ;
         SET @x = 'a\\'b'; SELECT @x AS x, DATABASE() AS db;
         SELECT * FROM `select`"
    )]);
    assert!(out.status.success(), "{}", text(&out.stdout));
    let statements = json(&out);
    let statements = statements.as_array().unwrap();
    let last = statements.last().unwrap();
    let mut row = last["rows"][0].clone();
    // The JSON column is an object or its text, depending on how the server types it.
    if let Some(text) = row["j"].as_str() {
        row["j"] = serde_json::from_str(text).unwrap();
    }
    assert_eq!(
        row,
        serde_json::json!({
            "from": 1, "big": "18446744073709551615", "d": "1.50", "n": null,
            "b": "\\xdeadbeef", "j": {"a": 1},
        })
    );
    let session = &statements[statements.len() - 2]["rows"][0];
    assert_eq!(session, &serde_json::json!({"x": "a'b", "db": database}));

    // Tables, and with --views the views and their kind.
    let tables = my(&["schema", "tables", "-d", &database]);
    assert_eq!(text(&tables.stdout), "select\n");
    let relations = my(&["schema", "tables", "-d", &database, "--views"]);
    assert_eq!(text(&relations.stdout), "select\ttable\nView With Spaces\tview\n");

    // An unknown database is an error, not the default database.
    let out = my(&["query", "-d", "zqlz_no_such_database", "SELECT DATABASE()"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(text(&out.stderr).contains("Unknown database"), "{}", text(&out.stderr));

    let out = my(&["query", &format!("DROP DATABASE `{database}`")]);
    assert!(out.status.success());
}
