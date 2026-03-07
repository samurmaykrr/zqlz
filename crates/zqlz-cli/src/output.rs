//! Terminal output formatting
//!
//! Renders query results, connection lists, schema info, and history in one of
//! several output formats: ASCII/Unicode table (default), JSON, CSV, or TSV.
//! Formatting behaviour is further controlled by [`OutputOptions`].

use std::fmt::Write as _;

use comfy_table::{presets::UTF8_FULL, Cell, Table};

use crate::ipc::{
    ColumnInfo, ConnectionSummary, FunctionSummary, HistoryEntry, IndexSummary, QueryExecution,
    StatementResult, ViewSummary,
};
use crate::OutputFormat;

// ---------------------------------------------------------------------------
// Output options
// ---------------------------------------------------------------------------

/// Rendering modifiers that augment the chosen output format.
#[derive(Debug, Default)]
pub struct OutputOptions {
    /// Suppress column headers in table, CSV, and TSV output.
    pub no_header: bool,
    /// Render each row as vertical `column = value` pairs instead of a grid.
    pub expanded: bool,
    /// Plain pipe-separated (`|`) output with no Unicode box-drawing characters.
    /// Takes precedence over `expanded`.
    pub no_align: bool,
    /// Append an explicit `Time: X ms` line after each statement in all formats.
    pub timing: bool,
    /// Suppress row-count and status footer lines. When combined with `timing`,
    /// only the timing line is emitted (row counts are still suppressed).
    pub quiet: bool,
}

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------

pub fn print_connections(connections: &[ConnectionSummary]) {
    if connections.is_empty() {
        println!("No saved connections.");
        return;
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(["ID", "Name", "Driver", "Host", "Database", "Folder"]);

    for conn in connections {
        table.add_row([
            conn.id.to_string(),
            conn.name.clone(),
            conn.driver.clone(),
            conn.host.clone().unwrap_or_default(),
            conn.database.clone().unwrap_or_default(),
            conn.folder.clone().unwrap_or_default(),
        ]);
    }

    println!("{table}");
}

pub fn print_connection_detail(conn: &ConnectionSummary) {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(["Field", "Value"]);

    let rows: &[(&str, String)] = &[
        ("ID", conn.id.to_string()),
        ("Name", conn.name.clone()),
        ("Driver", conn.driver.clone()),
        ("Host", conn.host.clone().unwrap_or_default()),
        ("Port", conn.port.clone().unwrap_or_default()),
        ("Database", conn.database.clone().unwrap_or_default()),
        ("Username", conn.username.clone().unwrap_or_default()),
        ("Folder", conn.folder.clone().unwrap_or_default()),
        ("Color", conn.color.clone().unwrap_or_default()),
    ];

    for (field, value) in rows {
        table.add_row([*field, value.as_str()]);
    }

    println!("{table}");
}

// ---------------------------------------------------------------------------
// Query results
// ---------------------------------------------------------------------------

/// Render query execution results to a `String`.
///
/// The caller decides the destination — use [`print_query_execution`] to write
/// to stdout or write the returned string to a file for `--output`.
pub fn render_query_execution(
    execution: &QueryExecution,
    format: &OutputFormat,
    limit: usize,
    opts: &OutputOptions,
) -> String {
    let mut out = String::new();

    for statement in &execution.statements {
        if let Some(error) = &statement.error {
            writeln!(out, "Error: {error}").ok();
            writeln!(out, "  SQL: {}", statement.sql).ok();
            continue;
        }

        let rows_to_display = if limit == 0 {
            statement.rows.len()
        } else {
            statement.rows.len().min(limit)
        };
        let truncated = rows_to_display < statement.rows.len();

        match format {
            OutputFormat::Table => {
                // DML statement — no result set
                if statement.columns.is_empty() {
                    if !opts.quiet {
                        writeln!(
                            out,
                            "Statement OK. {} row(s) affected. ({} ms)",
                            statement.affected_rows, statement.duration_ms
                        )
                        .ok();
                    }
                    if opts.timing {
                        writeln!(out, "Time: {} ms", statement.duration_ms).ok();
                    }
                    continue;
                }

                if opts.no_align {
                    render_no_align(&mut out, statement, rows_to_display, opts);
                } else if opts.expanded {
                    render_expanded(&mut out, statement, rows_to_display, opts);
                } else {
                    render_table(&mut out, statement, rows_to_display, opts);
                }

                if !opts.quiet {
                    if truncated {
                        writeln!(
                            out,
                            "  ({} rows, showing first {}. Use --limit to change.)",
                            statement.rows.len(),
                            rows_to_display
                        )
                        .ok();
                    } else {
                        writeln!(
                            out,
                            "  {} row(s) ({} ms)",
                            statement.rows.len(),
                            statement.duration_ms
                        )
                        .ok();
                    }
                }

                if opts.timing {
                    writeln!(out, "Time: {} ms", statement.duration_ms).ok();
                }
            }

            OutputFormat::Json => {
                let records: Vec<serde_json::Value> = statement
                    .rows
                    .iter()
                    .take(rows_to_display)
                    .map(|row| {
                        let mut obj = serde_json::Map::new();
                        for (col, val) in statement.columns.iter().zip(row.values.iter()) {
                            obj.insert(col.name.clone(), serde_json::Value::String(val.clone()));
                        }
                        serde_json::Value::Object(obj)
                    })
                    .collect();

                writeln!(
                    out,
                    "{}",
                    serde_json::to_string_pretty(&records)
                        .unwrap_or_else(|e| format!("{{\"error\": \"{e}\"}}"))
                )
                .ok();

                if opts.timing {
                    writeln!(out, "Time: {} ms", statement.duration_ms).ok();
                }
            }

            OutputFormat::Csv => {
                if !opts.no_header && !statement.columns.is_empty() {
                    let headers: Vec<&str> =
                        statement.columns.iter().map(|c| c.name.as_str()).collect();
                    writeln!(out, "{}", csv_row(&headers)).ok();
                }
                for row in statement.rows.iter().take(rows_to_display) {
                    let vals: Vec<&str> = row.values.iter().map(|v| v.as_str()).collect();
                    writeln!(out, "{}", csv_row(&vals)).ok();
                }
                if opts.timing {
                    // Prefix with `#` so downstream tools can treat it as a comment
                    writeln!(out, "# Time: {} ms", statement.duration_ms).ok();
                }
            }

            OutputFormat::Tsv => {
                if !opts.no_header && !statement.columns.is_empty() {
                    let headers: Vec<&str> =
                        statement.columns.iter().map(|c| c.name.as_str()).collect();
                    writeln!(out, "{}", headers.join("\t")).ok();
                }
                for row in statement.rows.iter().take(rows_to_display) {
                    writeln!(out, "{}", row.values.join("\t")).ok();
                }
                if opts.timing {
                    writeln!(out, "# Time: {} ms", statement.duration_ms).ok();
                }
            }
        }
    }

    out
}

// ---------------------------------------------------------------------------
// Render helpers (table-format variants)
// ---------------------------------------------------------------------------

/// Render a result set as a Unicode box-drawing table (default table mode).
fn render_table(
    out: &mut String,
    statement: &StatementResult,
    rows_to_display: usize,
    opts: &OutputOptions,
) {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);

    if !opts.no_header {
        table.set_header(
            statement
                .columns
                .iter()
                .map(|c| Cell::new(&c.name))
                .collect::<Vec<_>>(),
        );
    }

    for row in statement.rows.iter().take(rows_to_display) {
        table.add_row(row.values.iter().map(Cell::new).collect::<Vec<_>>());
    }

    writeln!(out, "{table}").ok();
}

/// Render a result set as vertical `column = value` pairs, one record at a time
/// (analogous to psql's `\x` expanded mode).
fn render_expanded(
    out: &mut String,
    statement: &StatementResult,
    rows_to_display: usize,
    opts: &OutputOptions,
) {
    for (record_idx, row) in statement.rows.iter().take(rows_to_display).enumerate() {
        if !opts.no_header {
            writeln!(out, "-[ RECORD {} ]{}", record_idx + 1, "-".repeat(30)).ok();
        }
        for (col, val) in statement.columns.iter().zip(row.values.iter()) {
            writeln!(out, "{} = {}", col.name, val).ok();
        }
    }
}

/// Render a result set as plain pipe-separated rows with no box-drawing characters.
fn render_no_align(
    out: &mut String,
    statement: &StatementResult,
    rows_to_display: usize,
    opts: &OutputOptions,
) {
    if !opts.no_header && !statement.columns.is_empty() {
        let headers: Vec<&str> = statement.columns.iter().map(|c| c.name.as_str()).collect();
        writeln!(out, "{}", headers.join("|")).ok();
    }
    for row in statement.rows.iter().take(rows_to_display) {
        writeln!(out, "{}", row.values.join("|")).ok();
    }
}

// ---------------------------------------------------------------------------
// Schema: columns
// ---------------------------------------------------------------------------

pub fn print_columns(columns: &[ColumnInfo]) {
    if columns.is_empty() {
        println!("No columns found.");
        return;
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(["Column", "Type", "Nullable", "Default", "PK"]);

    for col in columns {
        table.add_row([
            col.name.as_str(),
            col.data_type.as_str(),
            if col.nullable { "YES" } else { "NO" },
            col.default_value.as_deref().unwrap_or(""),
            if col.is_primary_key { "✓" } else { "" },
        ]);
    }

    println!("{table}");
}

pub fn print_views(views: &[ViewSummary]) {
    if views.is_empty() {
        println!("No views found.");
        return;
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(["Name", "Schema", "Materialized"]);

    for view in views {
        table.add_row([
            view.name.as_str(),
            view.schema.as_deref().unwrap_or(""),
            if view.is_materialized { "YES" } else { "NO" },
        ]);
    }

    println!("{table}");
}

pub fn print_indexes(indexes: &[IndexSummary]) {
    if indexes.is_empty() {
        println!("No indexes found.");
        return;
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(["Name", "Columns", "Unique", "Primary", "Type"]);

    for idx in indexes {
        table.add_row([
            idx.name.as_str(),
            idx.columns.join(", ").as_str(),
            if idx.is_unique { "YES" } else { "NO" },
            if idx.is_primary { "YES" } else { "NO" },
            idx.index_type.as_str(),
        ]);
    }

    println!("{table}");
}

pub fn print_functions(functions: &[FunctionSummary]) {
    if functions.is_empty() {
        println!("No functions found.");
        return;
    }

    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(["Name", "Schema", "Language", "Returns"]);

    for func in functions {
        table.add_row([
            func.name.as_str(),
            func.schema.as_deref().unwrap_or(""),
            func.language.as_str(),
            func.return_type.as_str(),
        ]);
    }

    println!("{table}");
}

// ---------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------

pub fn print_history(entries: &[HistoryEntry], format: &OutputFormat) {
    if entries.is_empty() {
        println!("No history entries.");
        return;
    }

    match format {
        OutputFormat::Table => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(["Time", "Duration (ms)", "Rows", "Status", "SQL"]);

            for entry in entries {
                let status = if entry.success { "OK" } else { "ERR" };
                let row_count = entry.row_count.map(|n| n.to_string()).unwrap_or_default();
                let sql_preview = sql_one_line(&entry.sql, 80);
                table.add_row([
                    entry
                        .executed_at
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string()
                        .as_str(),
                    entry.duration_ms.to_string().as_str(),
                    row_count.as_str(),
                    status,
                    sql_preview.as_str(),
                ]);
            }

            println!("{table}");
        }

        OutputFormat::Json => {
            let records: Vec<serde_json::Value> = entries
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "id": e.id.to_string(),
                        "sql": e.sql,
                        "connection_id": e.connection_id.map(|id| id.to_string()),
                        "executed_at": e.executed_at.to_rfc3339(),
                        "duration_ms": e.duration_ms,
                        "row_count": e.row_count,
                        "success": e.success,
                        "error": e.error,
                    })
                })
                .collect();

            println!(
                "{}",
                serde_json::to_string_pretty(&records)
                    .unwrap_or_else(|e| format!("{{\"error\": \"{e}\"}}"))
            );
        }

        OutputFormat::Csv => {
            println!("id,sql,connection_id,executed_at,duration_ms,row_count,success,error");
            for e in entries {
                println!(
                    "{}",
                    csv_row(&[
                        &e.id.to_string(),
                        &e.sql,
                        &e.connection_id.map(|id| id.to_string()).unwrap_or_default(),
                        &e.executed_at.to_rfc3339(),
                        &e.duration_ms.to_string(),
                        &e.row_count.map(|n| n.to_string()).unwrap_or_default(),
                        if e.success { "true" } else { "false" },
                        e.error.as_deref().unwrap_or(""),
                    ])
                );
            }
        }

        OutputFormat::Tsv => {
            println!("id\tsql\tconnection_id\texecuted_at\tduration_ms\trow_count\tsuccess\terror");
            for e in entries {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    e.id,
                    e.sql.replace('\t', " "),
                    e.connection_id.map(|id| id.to_string()).unwrap_or_default(),
                    e.executed_at.to_rfc3339(),
                    e.duration_ms,
                    e.row_count.map(|n| n.to_string()).unwrap_or_default(),
                    e.success,
                    e.error.as_deref().unwrap_or(""),
                );
            }
        }
    }
}

/// Print full details for a single history entry, including the complete SQL.
///
/// Used by `history show <id>` to display the untruncated query text alongside
/// all available metadata in a human-readable key/value layout.
pub fn print_history_entry(entry: &HistoryEntry) {
    let status = if entry.success { "OK" } else { "ERR" };
    println!("ID:           {}", entry.id);
    println!(
        "Executed at:  {}",
        entry.executed_at.format("%Y-%m-%d %H:%M:%S UTC")
    );
    println!("Duration:     {} ms", entry.duration_ms);
    println!("Status:       {}", status);
    if let Some(rows) = entry.row_count {
        println!("Rows:         {}", rows);
    }
    if let Some(conn_id) = entry.connection_id {
        println!("Connection:   {}", conn_id);
    }
    if let Some(err) = &entry.error {
        println!("Error:        {}", err);
    }
    println!();
    println!("{}", entry.sql);
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

/// Collapse all whitespace runs (including newlines and tabs) in `sql` to
/// single spaces, then truncate to `max_chars`, appending "…" when cut.
///
/// Used for single-line SQL previews in table cells so that multiline queries
/// don't expand the row height.
fn sql_one_line(sql: &str, max_chars: usize) -> String {
    let flat: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");

    if flat.chars().count() <= max_chars {
        return flat;
    }

    let cutoff = flat
        .char_indices()
        .nth(max_chars.saturating_sub(1))
        .map(|(i, _)| i)
        .unwrap_or(flat.len());
    format!("{}…", &flat[..cutoff])
}

/// Encode a row as a CSV line (RFC 4180: fields containing commas, quotes or
/// newlines are wrapped in double-quotes with internal quotes doubled).
fn csv_row(fields: &[&str]) -> String {
    fields
        .iter()
        .map(|field| {
            if field.contains([',', '"', '\n', '\r']) {
                format!("\"{}\"", field.replace('"', "\"\""))
            } else {
                (*field).to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}
