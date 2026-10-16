//! Query execution engine

use sqlparser::ast::{Query, SetExpr, Statement};
use sqlparser::dialect::{GenericDialect, MySqlDialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::sync::Arc;
use zqlz_core::{
    Connection, QueryResult, Result, SqlLexDialect, StatementResult, Value,
    split_sql_statements_for_dialect, sql_protected_ranges_for_dialect,
};

/// Information about a destructive operation that requires confirmation
#[derive(Clone, Debug)]
pub struct DestructiveOperationWarning {
    /// The type of operation (DELETE, UPDATE, DROP, TRUNCATE)
    pub operation_type: DestructiveOperationType,
    /// The table or object being affected
    pub affected_object: String,
    /// Why this operation is considered destructive
    pub reason: String,
    /// The full SQL statement
    pub sql: String,
}

/// Types of destructive operations
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DestructiveOperationType {
    /// DELETE without WHERE clause
    DeleteWithoutWhere,
    /// UPDATE without WHERE clause
    UpdateWithoutWhere,
    /// DROP TABLE/DATABASE statement
    Drop,
    /// TRUNCATE TABLE statement
    Truncate,
}

impl DestructiveOperationType {
    /// Get a human-readable name for the operation type
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::DeleteWithoutWhere => "DELETE without WHERE",
            Self::UpdateWithoutWhere => "UPDATE without WHERE",
            Self::Drop => "DROP",
            Self::Truncate => "TRUNCATE",
        }
    }
}

/// Bad user SQL (syntax error, division by zero, ...) is expected input, not an app fault, so
/// only connection-level failures are logged at error.
fn log_execution_failure(e: &zqlz_core::ZqlzError, msg: &str) {
    use zqlz_core::ZqlzError;
    match e {
        ZqlzError::Connection(_) | ZqlzError::Io(_) | ZqlzError::Timeout(_) => {
            tracing::error!(error = %e, "{msg}")
        }
        _ => tracing::warn!(error = %e, "{msg}"),
    }
}

/// Query execution engine
pub struct QueryEngine;

impl QueryEngine {
    /// Create a new query engine
    pub fn new() -> Self {
        Self
    }

    /// Execute a query and return results
    #[tracing::instrument(skip(self, conn, sql), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    pub async fn execute_query(
        &self,
        conn: &Arc<dyn Connection>,
        sql: &str,
    ) -> Result<QueryResult> {
        tracing::info!("executing query");
        let result = conn.query(sql, &[]).await.map_err(|e| {
            log_execution_failure(&e, "query execution failed");
            e
        })?;
        tracing::info!(
            rows = result.rows.len(),
            execution_time_ms = result.execution_time_ms,
            "query executed successfully"
        );
        Ok(result)
    }

    /// Execute a query with bound parameter values.
    #[tracing::instrument(skip(self, conn, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>(), param_count = params.len()))]
    pub async fn execute_query_with_params(
        &self,
        conn: &Arc<dyn Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult> {
        tracing::info!("executing query with parameters");
        let result = conn.query(sql, params).await.map_err(|e| {
            log_execution_failure(&e, "query execution failed");
            e
        })?;
        tracing::info!(
            rows = result.rows.len(),
            execution_time_ms = result.execution_time_ms,
            "query executed successfully"
        );
        Ok(result)
    }

    /// Execute a statement (INSERT, UPDATE, DELETE, etc.)
    #[tracing::instrument(skip(self, conn, sql), fields(sql_preview = %sql.chars().take(100).collect::<String>()))]
    pub async fn execute_statement(
        &self,
        conn: &Arc<dyn Connection>,
        sql: &str,
    ) -> Result<StatementResult> {
        tracing::info!("executing statement");
        let result = conn.execute(sql, &[]).await.map_err(|e| {
            log_execution_failure(&e, "statement execution failed");
            e
        })?;
        tracing::info!(
            affected_rows = result.affected_rows,
            "statement executed successfully"
        );
        Ok(result)
    }

    /// Execute a statement (INSERT, UPDATE, DELETE, etc.) with bound parameter values.
    #[tracing::instrument(skip(self, conn, sql, params), fields(sql_preview = %sql.chars().take(100).collect::<String>(), param_count = params.len()))]
    pub async fn execute_statement_with_params(
        &self,
        conn: &Arc<dyn Connection>,
        sql: &str,
        params: &[Value],
    ) -> Result<StatementResult> {
        tracing::info!("executing statement with parameters");
        let result = conn.execute(sql, params).await.map_err(|e| {
            log_execution_failure(&e, "statement execution failed");
            e
        })?;
        tracing::info!(
            affected_rows = result.affected_rows,
            "statement executed successfully"
        );
        Ok(result)
    }

    /// Parse SQL and determine if it's a query or statement
    pub fn is_query(&self, sql: &str) -> bool {
        tracing::trace!(sql_preview = %sql.chars().take(50).collect::<String>(), "checking if SQL is query");
        let dialect = GenericDialect {};
        let sql = strip_leading_comments(sql);
        match Parser::parse_sql(&dialect, sql) {
            Ok(statements) => statements.first().is_some_and(statement_returns_rows),
            Err(_) => first_sql_word(sql, &dialect).is_some_and(|word| {
                matches!(
                    word.as_str(),
                    "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "EXPLAIN"
                )
            }),
        }
    }

    /// Returns `true` only when every statement in `sql` provably reads without
    /// writing. Unparseable SQL is treated as a write, so callers that gate
    /// unattended execution on this fail closed.
    ///
    /// Catches data-modifying CTEs, `SELECT ... INTO`, row locks
    /// (`FOR UPDATE`), `EXPLAIN ANALYZE` of writes, and calls to well-known
    /// side-effecting built-ins (`setval`, `pg_terminate_backend`, `dblink`,
    /// ...; see [`SIDE_EFFECT_FUNCTIONS`]).
    ///
    /// It cannot see side effects inside user-defined functions, so this is
    /// not a security boundary on its own: the agent `run_query` path also
    /// runs the SQL in a read-only transaction where the database supports
    /// one. Other callers use this only as a best-effort classifier.
    pub fn is_read_only(&self, sql: &str) -> bool {
        self.is_read_only_for_dialect(sql, None)
    }

    /// [`Self::is_read_only`] using the lexing rules of `dialect_id`
    /// (`Connection::dialect_id`). MySQL needs its own pass: `#` comments, backslash string
    /// escapes and `--` needing trailing whitespace decide where statements end, and
    /// `SHOW`/`DESCRIBE`/`USE` are reads even though no generic parser understands them.
    pub fn is_read_only_for_dialect(&self, sql: &str, dialect_id: Option<&str>) -> bool {
        if SqlLexDialect::from_profile_id(dialect_id) == SqlLexDialect::MySql {
            return mysql_is_read_only(sql);
        }
        // MySQL runs the content of `/*! ... */`, which a parser sees as a comment.
        !sql.contains("/*!") && generic_is_read_only(&GenericDialect {}, strip_leading_comments(sql))
    }

    /// Returns `true` if the SQL contains any DDL statement that modifies the
    /// database schema (CREATE, ALTER, DROP, RENAME, TRUNCATE).
    ///
    /// Callers should invalidate their schema cache after executing SQL for
    /// which this returns `true`.
    pub fn is_schema_modifying(&self, sql: &str) -> bool {
        let dialect = GenericDialect {};
        let sql = strip_leading_comments(sql);
        let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
            return first_sql_word(sql, &dialect).is_some_and(|word| {
                matches!(
                    word.as_str(),
                    "CREATE" | "ALTER" | "DROP" | "RENAME" | "TRUNCATE"
                )
            });
        };

        statements.iter().any(statement_modifies_schema)
    }

    /// Analyze SQL statement for destructive operations that require confirmation
    pub fn analyze_for_destructive_operations(
        &self,
        sql: &str,
    ) -> Option<DestructiveOperationWarning> {
        tracing::trace!(sql_preview = %sql.chars().take(100).collect::<String>(), "analyzing SQL for destructive operations");

        let dialect = GenericDialect {};
        let Ok(statements) = Parser::parse_sql(&dialect, strip_leading_comments(sql)) else {
            tracing::debug!("failed to parse SQL, skipping destructive operation check");
            return None;
        };

        for statement in statements {
            match statement {
                Statement::Delete(delete) => {
                    if delete.selection.is_none() {
                        let table_name = delete
                            .tables
                            .first()
                            .map(|t| t.to_string())
                            .unwrap_or_else(|| "unknown".to_string());

                        return Some(DestructiveOperationWarning {
                            operation_type: DestructiveOperationType::DeleteWithoutWhere,
                            affected_object: table_name.clone(),
                            reason: format!(
                                "This DELETE statement will remove ALL rows from table '{}'",
                                table_name
                            ),
                            sql: sql.to_string(),
                        });
                    }
                }
                Statement::Update {
                    table, selection, ..
                } => {
                    if selection.is_none() {
                        let table_name = table.relation.to_string();

                        return Some(DestructiveOperationWarning {
                            operation_type: DestructiveOperationType::UpdateWithoutWhere,
                            affected_object: table_name.clone(),
                            reason: format!(
                                "This UPDATE statement will modify ALL rows in table '{}'",
                                table_name
                            ),
                            sql: sql.to_string(),
                        });
                    }
                }
                Statement::Drop {
                    object_type, names, ..
                } => {
                    let object_names = names
                        .iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");

                    return Some(DestructiveOperationWarning {
                        operation_type: DestructiveOperationType::Drop,
                        affected_object: object_names.clone(),
                        reason: format!(
                            "This DROP statement will permanently delete {} '{}'",
                            object_type, object_names
                        ),
                        sql: sql.to_string(),
                    });
                }
                Statement::Truncate { table_names, .. } => {
                    let table_names_str = table_names
                        .iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");

                    return Some(DestructiveOperationWarning {
                        operation_type: DestructiveOperationType::Truncate,
                        affected_object: table_names_str.clone(),
                        reason: format!(
                            "This TRUNCATE statement will remove ALL rows from table(s) '{}'",
                            table_names_str
                        ),
                        sql: sql.to_string(),
                    });
                }
                _ => {
                    // Not a destructive operation, continue checking other statements
                }
            }
        }

        tracing::trace!("no destructive operations detected");
        None
    }

    /// Generate a DELETE statement with primary key WHERE clause
    ///
    /// Builds a parameterized DELETE statement using primary key columns for the WHERE clause.
    /// This is safer and more efficient than using full row matching.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to delete from
    /// * `pk_columns` - Names of the primary key columns
    /// * `pk_values` - Values for the primary key columns (must match order of pk_columns)
    ///
    /// # Returns
    /// A tuple of (SQL string, parameter values) ready for execution
    ///
    /// # Example
    /// ```ignore
    /// let (sql, params) = engine.generate_delete_by_pk(
    ///     "users",
    ///     &["id"],
    ///     &[Value::Integer(123)]
    /// );
    /// // sql: "DELETE FROM users WHERE id = ?"
    /// // params: [Value::Integer(123)]
    /// ```
    pub fn generate_delete_by_pk(
        &self,
        table_name: &str,
        pk_columns: &[String],
        pk_values: &[zqlz_core::Value],
    ) -> (String, Vec<zqlz_core::Value>) {
        if pk_columns.is_empty() || pk_values.is_empty() {
            tracing::warn!("Cannot generate DELETE with empty primary key columns or values");
            return (String::new(), Vec::new());
        }

        if pk_columns.len() != pk_values.len() {
            tracing::warn!(
                "Primary key column count ({}) does not match value count ({})",
                pk_columns.len(),
                pk_values.len()
            );
            return (String::new(), Vec::new());
        }

        let where_conditions: Vec<String> = pk_columns
            .iter()
            .map(|col| format!("\"{}\" = ?", col.replace('"', "\"\"")))
            .collect();

        let sql = format!(
            "DELETE FROM \"{}\" WHERE {}",
            table_name.replace('"', "\"\""),
            where_conditions.join(" AND ")
        );

        let params = pk_values.to_vec();

        tracing::debug!(
            table = %table_name,
            pk_columns = ?pk_columns,
            "generated DELETE statement with primary key"
        );

        (sql, params)
    }

    /// Generate a DELETE statement using all column values for matching
    ///
    /// Builds a DELETE statement that matches on all column values. This is used when
    /// no primary key is available. NULL values are matched with IS NULL.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table to delete from
    /// * `column_names` - Names of all columns in the row
    /// * `column_values` - Values for all columns (must match order of column_names)
    ///
    /// # Returns
    /// A tuple of (SQL string, parameter values) ready for execution
    pub fn generate_delete_by_full_row(
        &self,
        table_name: &str,
        column_names: &[String],
        column_values: &[zqlz_core::Value],
    ) -> (String, Vec<zqlz_core::Value>) {
        if column_names.is_empty() || column_values.is_empty() {
            tracing::warn!("Cannot generate DELETE with empty columns or values");
            return (String::new(), Vec::new());
        }

        if column_names.len() != column_values.len() {
            tracing::warn!(
                "Column count ({}) does not match value count ({})",
                column_names.len(),
                column_values.len()
            );
            return (String::new(), Vec::new());
        }

        let mut where_conditions = Vec::new();
        let mut params = Vec::new();

        for (col_name, col_value) in column_names.iter().zip(column_values.iter()) {
            if matches!(col_value, zqlz_core::Value::Null) {
                where_conditions.push(format!("\"{}\" IS NULL", col_name.replace('"', "\"\"")));
            } else {
                where_conditions.push(format!("\"{}\" = ?", col_name.replace('"', "\"\"")));
                params.push(col_value.clone());
            }
        }

        let sql = format!(
            "DELETE FROM \"{}\" WHERE {}",
            table_name.replace('"', "\"\""),
            where_conditions.join(" AND ")
        );

        tracing::debug!(
            table = %table_name,
            column_count = column_names.len(),
            "generated DELETE statement with full row match"
        );

        (sql, params)
    }

    /// Generate a DROP TABLE statement
    ///
    /// Returns a DROP TABLE statement. The caller is responsible for executing this
    /// through proper confirmation flows (Feature 113 guardrails).
    pub fn generate_drop_table(&self, table_name: &str, if_exists: bool) -> String {
        let if_exists_clause = if if_exists { " IF EXISTS" } else { "" };
        let sql = format!(
            "DROP TABLE{} \"{}\"",
            if_exists_clause,
            table_name.replace('"', "\"\"")
        );

        tracing::debug!(table = %table_name, "generated DROP TABLE statement");
        sql
    }

    /// Generate a TRUNCATE TABLE statement
    ///
    /// Returns a TRUNCATE TABLE statement. The caller is responsible for executing this
    /// through proper confirmation flows (Feature 113 guardrails).
    pub fn generate_truncate_table(&self, table_name: &str) -> String {
        let sql = format!("TRUNCATE TABLE \"{}\"", table_name.replace('"', "\"\""));

        tracing::debug!(table = %table_name, "generated TRUNCATE TABLE statement");
        sql
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Drops leading whitespace and comments (`# ...`, `-- ...`, `/* ... */`) so classification
/// sees the first real keyword. `/*! ... */` is not skipped: MySQL executes its content.
///
/// A `#` cannot start a statement in any dialect, so stripping it is safe outside MySQL too.
fn strip_leading_comments(mut sql: &str) -> &str {
    loop {
        sql = sql.trim_start();
        let rest = if sql.starts_with('#') || sql.starts_with("--") {
            sql.find('\n').map(|newline| &sql[newline + 1..])
        } else if sql.starts_with("/*") && !sql.starts_with("/*!") {
            sql.find("*/").map(|end| &sql[end + 2..])
        } else {
            return sql;
        };
        match rest {
            Some(rest) => sql = rest,
            None => return "",
        }
    }
}

fn generic_is_read_only(dialect: &dyn sqlparser::dialect::Dialect, sql: &str) -> bool {
    let shorthand = table_shorthand(sql);
    let sql = shorthand.as_deref().unwrap_or(sql);
    match Parser::parse_sql(dialect, sql) {
        Ok(statements) => {
            !statements.is_empty()
                && statements.iter().all(statement_is_read_only)
                && !calls_side_effecting_function(dialect, sql)
        }
        Err(_) => false,
    }
}

/// MySQL scripts are split with MySQL's own lexing rules and each statement is judged on its
/// own. Text the generic parser would read differently from the server (`--x`, executable
/// `/*! ... */` comments) fails closed instead of being parsed.
fn mysql_is_read_only(sql: &str) -> bool {
    let statements = split_sql_statements_for_dialect(sql, SqlLexDialect::MySql);
    !statements.is_empty() && statements.iter().all(|statement| mysql_statement_is_read_only(statement))
}

fn mysql_statement_is_read_only(statement: &str) -> bool {
    if statement.contains("/*!") || statement.contains("/*M!") || has_double_dash_operator(statement)
    {
        return false;
    }
    let statement = strip_leading_comments(statement);
    let first_word = statement
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .next()
        .unwrap_or_default();
    // Everything `SHOW` can list is a read, and `USE` only switches the session's database.
    // `EXPLAIN` and everything else go through the parser below.
    if ["SHOW", "DESCRIBE", "DESC", "USE"]
        .iter()
        .any(|keyword| first_word.eq_ignore_ascii_case(keyword))
    {
        return true;
    }
    generic_is_read_only(&MySqlDialect {}, statement)
}

/// MySQL reads `--` as two minus signs unless whitespace follows, but SQL parsers read it as
/// a comment that hides the rest of the line.
fn has_double_dash_operator(sql: &str) -> bool {
    let ranges = sql_protected_ranges_for_dialect(sql, SqlLexDialect::MySql);
    let bytes = sql.as_bytes();
    (0..bytes.len().saturating_sub(1)).any(|index| {
        bytes[index] == b'-'
            && bytes[index + 1] == b'-'
            && !ranges.iter().any(|range| range.contains(index))
    })
}

fn first_sql_word(sql: &str, dialect: &dyn sqlparser::dialect::Dialect) -> Option<String> {
    let mut tokenizer = Tokenizer::new(dialect, sql);
    let tokens = tokenizer.tokenize().ok()?;
    tokens.into_iter().find_map(|token| match token {
        Token::Word(word) if word.quote_style.is_none() => Some(word.value.to_ascii_uppercase()),
        _ => None,
    })
}

fn statement_returns_rows(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Query(_)
            | Statement::ShowFunctions { .. }
            | Statement::ShowVariable { .. }
            | Statement::ShowStatus { .. }
            | Statement::ShowVariables { .. }
            | Statement::ShowCreate { .. }
            | Statement::ShowColumns { .. }
            | Statement::ShowDatabases { .. }
            | Statement::ShowSchemas { .. }
            | Statement::ShowTables { .. }
            | Statement::ShowViews { .. }
            | Statement::ShowCollation { .. }
            | Statement::ExplainTable { .. }
            | Statement::Explain { .. }
    )
}

/// Postgres' `TABLE x` is `SELECT * FROM x`, but the generic dialect cannot
/// parse it. Only a lone statement naming a plain (optionally quoted,
/// schema-qualified) identifier is rewritten; anything else stays unparseable
/// and therefore fails closed.
fn table_shorthand(sql: &str) -> Option<String> {
    let statement = sql.trim().trim_end_matches(';').trim();
    let (keyword, name) = statement.split_once(char::is_whitespace)?;
    let name = name.trim();
    let is_identifier = |part: &str| match part.strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
        Some(quoted) => !quoted.is_empty() && !quoted.contains('"'),
        None => !part.is_empty() && part.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '$'),
    };
    (keyword.eq_ignore_ascii_case("table") && name.split('.').all(is_identifier))
        .then(|| format!("SELECT * FROM {name}"))
}

fn statement_is_read_only(statement: &Statement) -> bool {
    match statement {
        Statement::Query(query) => query_is_read_only(query),
        // `EXPLAIN (ANALYZE) ...` carries the flag in `options`, not `analyze`.
        Statement::Explain {
            analyze,
            options,
            statement,
            ..
        } => (!analyze && options.is_none()) || statement_is_read_only(statement),
        other => statement_returns_rows(other),
    }
}

fn query_is_read_only(query: &Query) -> bool {
    let ctes_read_only = query.with.as_ref().is_none_or(|with| {
        with.cte_tables
            .iter()
            .all(|cte| query_is_read_only(&cte.query))
    });
    // `FOR UPDATE` / `FOR SHARE` take row locks, which block writers.
    ctes_read_only && query.locks.is_empty() && set_expr_is_read_only(&query.body)
}

fn set_expr_is_read_only(set_expr: &SetExpr) -> bool {
    match set_expr {
        SetExpr::Select(select) => select.into.is_none(),
        SetExpr::Query(query) => query_is_read_only(query),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_is_read_only(left) && set_expr_is_read_only(right)
        }
        SetExpr::Values(_) | SetExpr::Table(_) => true,
        _ => false,
    }
}

/// Built-ins that change state without being a write statement, so a
/// read-only transaction does not stop them (or, for `nextval`/`setval`, they
/// fail only at run time). Matched case-insensitively on the bare function
/// name, whatever schema qualifies it.
///
/// Unknown and user-defined functions are deliberately allowed: refusing every
/// function we cannot classify would break ordinary reads (`count`, `lower`,
/// `my_schema.my_view_fn()`), and the read-only transaction covers their writes.
const SIDE_EFFECT_FUNCTIONS: &[&str] = &[
    // sequences and transaction ids
    "nextval",
    "setval",
    "txid_current",
    "pg_current_xact_id",
    // session and server state
    "set_config",
    "pg_sleep",
    "pg_sleep_for",
    "pg_sleep_until",
    "pg_notify",
    "pg_reload_conf",
    "pg_rotate_logfile",
    "pg_switch_wal",
    "pg_promote",
    "pg_export_snapshot",
    "pg_import_system_collations",
    "pg_start_backup",
    "pg_stop_backup",
    // large objects
    "loread",
    "lowrite",
    // MySQL
    "get_lock",
    "release_lock",
    "release_all_locks",
    "sleep",
    "load_file",
    "benchmark",
];

/// Prefixes of function families that are all side-effecting.
const SIDE_EFFECT_FUNCTION_PREFIXES: &[&str] = &[
    "pg_terminate_",
    "pg_cancel_",
    "pg_advisory_",
    "pg_try_advisory_",
    "pg_create_",
    "pg_drop_",
    "pg_replication_",
    "pg_logical_",
    "pg_wal_replay_",
    "pg_backup_",
    "pg_stat_reset",
    "pg_read_",
    "pg_ls_",
    "pg_stat_file",
    "pg_file_",
    "lo_",
    "dblink",
    // run the SQL text they are given
    "query_to_xml",
];

/// True when `sql` calls (`name (`) a function in the deny-lists, or uses syntax that writes
/// outside the statement's own result: `INTO` (`SELECT ... INTO OUTFILE|DUMPFILE|@var`),
/// `LOCK IN SHARE MODE` and `@var := ...`. Works on tokens, so string literals and comments
/// never match and a call is found anywhere: select list, FROM, subquery, CTE or EXPLAIN
/// target.
fn calls_side_effecting_function(dialect: &dyn sqlparser::dialect::Dialect, sql: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(dialect, sql).tokenize() else {
        return true;
    };
    let mut significant = tokens
        .iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .peekable();
    while let Some(token) = significant.next() {
        if matches!(token, Token::Assignment) {
            return true;
        }
        let Token::Word(word) = token else {
            continue;
        };
        if word.quote_style.is_none() {
            if word.value.eq_ignore_ascii_case("into") {
                return true;
            }
            if word.value.eq_ignore_ascii_case("lock")
                && matches!(significant.peek(), Some(Token::Word(next)) if next.value.eq_ignore_ascii_case("in"))
            {
                return true;
            }
        }
        if matches!(significant.peek(), Some(Token::LParen)) {
            let name = word.value.to_ascii_lowercase();
            if SIDE_EFFECT_FUNCTIONS.contains(&name.as_str())
                || SIDE_EFFECT_FUNCTION_PREFIXES
                    .iter()
                    .any(|prefix| name.starts_with(prefix))
            {
                return true;
            }
        }
    }
    false
}

fn statement_modifies_schema(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::CreateTable { .. }
            | Statement::CreateView { .. }
            | Statement::CreateIndex { .. }
            | Statement::CreateFunction { .. }
            | Statement::CreateProcedure { .. }
            | Statement::CreateTrigger { .. }
            | Statement::AlterTable { .. }
            | Statement::AlterView { .. }
            | Statement::AlterIndex { .. }
            | Statement::Drop { .. }
            | Statement::DropFunction { .. }
            | Statement::Truncate { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_sql_errors_log_at_warn_and_connection_errors_at_error() {
        use std::sync::Mutex;
        use tracing::{Level, Metadata, span, subscriber::Interest};
        use zqlz_core::ZqlzError;

        struct Levels(Mutex<Vec<Level>>);
        impl tracing::Subscriber for &'static Levels {
            fn enabled(&self, _: &Metadata<'_>) -> bool {
                true
            }
            fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
                Interest::always()
            }
            fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
                span::Id::from_u64(1)
            }
            fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
            fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
            fn event(&self, event: &tracing::Event<'_>) {
                self.0.lock().unwrap().push(*event.metadata().level());
            }
            fn enter(&self, _: &span::Id) {}
            fn exit(&self, _: &span::Id) {}
        }

        let levels: &'static Levels = Box::leak(Box::new(Levels(Mutex::new(Vec::new()))));
        tracing::subscriber::with_default(levels, || {
            log_execution_failure(&ZqlzError::Query("syntax error".into()), "m");
            log_execution_failure(&ZqlzError::Other("division by zero".into()), "m");
            log_execution_failure(&ZqlzError::Connection("closed".into()), "m");
        });
        assert_eq!(
            *levels.0.lock().unwrap(),
            [Level::WARN, Level::WARN, Level::ERROR]
        );
    }

    #[test]
    fn is_read_only_rejects_hidden_writes() {
        let engine = QueryEngine::new();
        assert!(engine.is_read_only("SELECT * FROM users WHERE id = 1"));
        assert!(engine.is_read_only("WITH recent AS (SELECT 1) SELECT * FROM recent"));
        assert!(engine.is_read_only("EXPLAIN SELECT 1"));
        assert!(engine.is_read_only("SELECT 1; SELECT 2"));

        assert!(!engine.is_read_only("DELETE FROM users"));
        assert!(!engine.is_read_only("SELECT 1; DROP TABLE users"));
        assert!(!engine.is_read_only("SELECT * INTO backup FROM users"));
        assert!(!engine.is_read_only("EXPLAIN ANALYZE DELETE FROM users"));
        assert!(
            !engine
                .is_read_only("WITH gone AS (DELETE FROM users RETURNING id) SELECT * FROM gone")
        );
        assert!(!engine.is_read_only("PRAGMA user_version = 3"));
        assert!(!engine.is_read_only("PRAGMA foreign_keys(ON)"));
        assert!(!engine.is_read_only("this is not sql"));
        assert!(!engine.is_read_only(""));
    }

    #[test]
    fn is_read_only_table() {
        let engine = QueryEngine::new();
        let cases: &[(&str, bool)] = &[
            // plain reads
            ("SELECT 1", true),
            ("select * from zqlz_lab.customers where id = 1", true),
            ("WITH r AS (SELECT 1 AS x) SELECT * FROM r", true),
            ("EXPLAIN SELECT 1", true),
            ("EXPLAIN (FORMAT JSON) SELECT 1", true),
            ("EXPLAIN DELETE FROM users", true),
            ("SHOW search_path", true),
            ("VALUES (1), (2)", true),
            ("SELECT lower(name), count(*) FROM users GROUP BY 1", true),
            ("SELECT my_schema.my_fn(1)", true),
            ("SELECT currval('s')", true),
            ("SELECT current_setting('search_path')", true),
            ("SELECT 'DELETE FROM users; SELECT setval(''s'', 1)'", true),
            ("SELECT 1 -- DROP TABLE users", true),
            ("SELECT /* setval('s', 1) */ 1", true),
            ("SELECT 1; SELECT 2", true),
            ("SELECT 1 AS nextval", true),
            // volatile built-ins
            ("SELECT setval('zqlz_lab.customer_number_seq', 1)", false),
            ("SELECT nextval('s')", false),
            ("select pg_catalog.nextval('s')", false),
            ("SELECT * FROM nextval ('s')", false),
            ("SELECT pg_terminate_backend(0)", false),
            ("SELECT pg_cancel_backend(1)", false),
            ("SELECT pg_reload_conf()", false),
            ("SELECT pg_rotate_logfile()", false),
            ("SELECT pg_advisory_lock(1)", false),
            ("SELECT pg_try_advisory_xact_lock(1)", false),
            ("SELECT set_config('search_path', 'x', false)", false),
            ("SELECT pg_sleep(100)", false),
            ("SELECT lo_import('/etc/passwd')", false),
            ("SELECT lo_unlink(1)", false),
            ("SELECT pg_read_file('/etc/passwd')", false),
            ("SELECT pg_ls_dir('.')", false),
            ("SELECT dblink_exec('dbname=x', 'DELETE FROM t')", false),
            ("SELECT * FROM dblink('dbname=x', 'SELECT 1') AS t(a int)", false),
            ("SELECT pg_switch_wal()", false),
            ("SELECT pg_create_restore_point('x')", false),
            ("SELECT txid_current()", false),
            ("SELECT query_to_xml('delete from t', true, false, '')", false),
            ("SELECT GET_LOCK('a', 1)", false),
            ("SELECT \"setval\"('s', 1)", false),
            ("EXPLAIN SELECT setval('s', 1)", false),
            ("WITH x AS (SELECT nextval('s')) SELECT * FROM x", false),
            ("SELECT * FROM t WHERE id IN (SELECT nextval('s'))", false),
            // locks and writes
            ("SELECT * FROM users FOR UPDATE", false),
            ("SELECT * FROM users FOR SHARE", false),
            ("SELECT * INTO newtable FROM users", false),
            ("WITH d AS (DELETE FROM t RETURNING 1) SELECT * FROM d", false),
            ("EXPLAIN ANALYZE DELETE FROM users", false),
            ("EXPLAIN (ANALYZE) DELETE FROM users", false),
            ("EXPLAIN (ANALYZE, BUFFERS) INSERT INTO t VALUES (1)", false),
            ("SELECT 1; DELETE FROM users", false),
            // statements that are not reads
            ("DO $$ BEGIN PERFORM 1; END $$", false),
            ("CALL do_things()", false),
            ("COPY users TO PROGRAM 'cat'", false),
            ("COPY (SELECT 1) TO PROGRAM 'cat'", false),
            ("SET search_path = x", false),
            ("RESET ALL", false),
            ("LISTEN chan", false),
            ("NOTIFY chan", false),
            ("PREPARE p AS DELETE FROM t", false),
            ("EXECUTE p", false),
            // sqlparser's generic dialect cannot parse a bare `TABLE x`; it is normalised first
            ("TABLE users", true),
            ("table zqlz_lab.customers;", true),
            ("TABLE \"My Schema\".\"Tab\"", true),
            ("TABLE users; DROP TABLE users", false),
            ("TABLE users FOR UPDATE", false),
            ("TABLE", false),
            ("TABLE \"a.b\"", false),
            ("VALUES (1),(2)", true),
            ("SHOW TABLES", true),
            ("EXPLAIN SELECT * FROM t", true),
            ("EXPLAIN (VERBOSE) SELECT * FROM t", true),
            ("BEGIN", false),
            ("COMMIT", false),
        ];
        let wrong: Vec<_> = cases
            .iter()
            .filter(|(sql, expected)| engine.is_read_only(sql) != *expected)
            .map(|(sql, expected)| format!("{sql} (expected {expected})"))
            .collect();
        assert!(wrong.is_empty(), "misclassified:\n{}", wrong.join("\n"));
    }

    #[test]
    fn is_read_only_for_mysql() {
        let engine = QueryEngine::new();
        let cases: &[(&str, bool)] = &[
            ("SELECT 1", true),
            ("# hi\nSELECT 1", true),
            ("-- hi\nSELECT 1", true),
            ("/* x */ # y\n-- z\nSELECT 1", true),
            ("SELECT 1 # INTO OUTFILE '/tmp/x'", true),
            ("SELECT 'INTO OUTFILE; SLEEP(9)'", true),
            ("SELECT 'a\\'; DROP TABLE t; -- '", true),
            ("SELECT `into` FROM t", true),
            ("SELECT /*+ MAX_EXECUTION_TIME(100) */ * FROM t", true),
            ("WITH c AS (SELECT 1) SELECT * FROM c", true),
            ("SHOW TABLES", true),
            ("SHOW CREATE TABLE t", true),
            ("SHOW FULL PROCESSLIST", true),
            ("SHOW GRANTS FOR 'a'@'b'", true),
            ("SHOW ENGINE INNODB STATUS", true),
            ("# c\nSHOW VARIABLES LIKE 'max%'", true),
            ("DESCRIBE t", true),
            ("DESC t", true),
            ("USE db", true),
            ("TABLE t", true),
            ("VALUES ROW(1, 2), ROW(3, 4)", true),
            ("EXPLAIN SELECT 1", true),
            ("EXPLAIN DELETE FROM t", true),
            ("EXPLAIN ANALYZE SELECT 1", true),
            ("SELECT 1; SELECT 2", true),
            // file and variable sinks
            ("SELECT * FROM t INTO OUTFILE '/tmp/x'", false),
            ("SELECT 1 INTO DUMPFILE '/tmp/x'", false),
            ("SELECT 1 INTO @v", false),
            ("SELECT a INTO @a FROM t", false),
            ("SELECT @a := 5", false),
            // locks
            ("SELECT * FROM t FOR UPDATE", false),
            ("SELECT * FROM t FOR SHARE", false),
            ("SELECT * FROM t FOR UPDATE SKIP LOCKED", false),
            ("SELECT * FROM t LOCK IN SHARE MODE", false),
            ("LOCK TABLES t WRITE", false),
            ("UNLOCK TABLES", false),
            // side-effecting built-ins
            ("SELECT SLEEP(5)", false),
            ("SELECT sleep (5)", false),
            ("SELECT /*+ x */ SLEEP(1)", false),
            ("SELECT GET_LOCK('a', 1)", false),
            ("SELECT RELEASE_LOCK('a')", false),
            ("SELECT LOAD_FILE('/etc/passwd')", false),
            ("SELECT BENCHMARK(1000000, MD5('a'))", false),
            // executable comments run on the server
            ("SELECT /*!50000 SLEEP(100) */ 1", false),
            ("SELECT 1 /*! INTO OUTFILE '/tmp/x' */", false),
            // `--x` is `- -x` in MySQL, not a comment
            ("SELECT 1--1; DROP TABLE t", false),
            ("SELECT 1--x FROM t INTO OUTFILE '/tmp/y'", false),
            ("SELECT 'a\\\\'; DROP TABLE t", false),
            // statements that are not reads
            ("SET @x = 1", false),
            ("SET autocommit = 0", false),
            ("CALL p()", false),
            ("DO SLEEP(1)", false),
            ("HANDLER t OPEN", false),
            ("ANALYZE TABLE t", false),
            ("OPTIMIZE TABLE t", false),
            ("REPAIR TABLE t", false),
            ("FLUSH TABLES", false),
            ("WITH d AS (SELECT 1) DELETE FROM t", false),
            ("EXPLAIN ANALYZE DELETE FROM t WHERE id > 0", false),
            ("PREPARE s FROM 'DELETE FROM t'", false),
            ("EXECUTE s", false),
            ("DEALLOCATE PREPARE s", false),
            ("KILL 1", false),
            ("START TRANSACTION", false),
            ("BEGIN", false),
            ("SELECT 1; KILL 2", false),
            ("SHOW TABLES; DROP TABLE t", false),
            ("", false),
            ("# only a comment", false),
        ];
        let wrong: Vec<_> = cases
            .iter()
            .filter(|(sql, expected)| {
                engine.is_read_only_for_dialect(sql, Some("mysql")) != *expected
            })
            .map(|(sql, expected)| format!("{sql} (expected {expected})"))
            .collect();
        assert!(wrong.is_empty(), "misclassified:\n{}", wrong.join("\n"));
    }

    #[test]
    fn generic_read_only_blocks_mysql_sinks_and_hidden_comments() {
        let engine = QueryEngine::new();
        assert!(engine.is_read_only("# hi\nSELECT 1"));
        assert!(!engine.is_read_only("SELECT 1 INTO OUTFILE '/tmp/x'"));
        assert!(!engine.is_read_only("SELECT 1 INTO @v"));
        assert!(!engine.is_read_only("SELECT * FROM t LOCK IN SHARE MODE"));
        assert!(!engine.is_read_only("SELECT /*!50000 SLEEP(100) */ 1"));
    }

    #[test]
    fn classification_skips_leading_hash_and_dash_comments() {
        let engine = QueryEngine::new();
        assert!(engine.is_query("# hi\nSELECT 1"));
        assert!(engine.is_query("-- x\n# y\n/* z */ SELECT 1"));
        assert!(engine.is_query("# hi\nSHOW TABLES"));
        assert!(!engine.is_query("# SELECT\nDELETE FROM t"));
        assert!(!engine.is_query("# only a comment"));
        assert!(engine.is_schema_modifying("# hi\nCREATE TABLE t (id INT)"));
        assert!(!engine.is_schema_modifying("# CREATE\nSELECT 1"));
        assert!(
            engine
                .analyze_for_destructive_operations("# hi\nDELETE FROM t")
                .is_some()
        );
        assert_eq!(strip_leading_comments("  # a\n -- b\n/* c */ /*! d */ x"), "/*! d */ x");
    }

    #[test]
    fn is_query_uses_parser_and_tokenizer_not_raw_prefixes() {
        let engine = QueryEngine::new();

        assert!(engine.is_query("/* route me */ SELECT * FROM users"));
        assert!(engine.is_query("WITH active AS (SELECT 1) SELECT * FROM active"));
        assert!(!engine.is_query("/* SELECT */ DROP TABLE users"));
        assert!(!engine.is_query("'SELECT'"));
        assert!(!engine.is_query("selective value"));
    }

    #[test]
    fn schema_modifying_uses_parser_and_tokenizer_not_raw_prefixes() {
        let engine = QueryEngine::new();

        assert!(engine.is_schema_modifying("/* migrate */ CREATE TABLE users (id INT)"));
        assert!(engine.is_schema_modifying("ALTER TABLE users ADD COLUMN name TEXT"));
        assert!(!engine.is_schema_modifying("/* DROP */ SELECT * FROM users"));
        assert!(!engine.is_schema_modifying("'DROP TABLE users'"));
        assert!(!engine.is_schema_modifying("dropper value"));
    }

    #[test]
    fn test_generate_delete_by_pk_single_column() {
        let engine = QueryEngine::new();
        let pk_columns = vec!["id".to_string()];
        let pk_values = vec![zqlz_core::Value::Int64(123)];

        let (sql, params) = engine.generate_delete_by_pk("users", &pk_columns, &pk_values);

        assert_eq!(sql, "DELETE FROM \"users\" WHERE \"id\" = ?");
        assert_eq!(params.len(), 1);
        assert!(matches!(params[0], zqlz_core::Value::Int64(123)));
    }

    #[test]
    fn test_generate_delete_by_pk_composite_key() {
        let engine = QueryEngine::new();
        let pk_columns = vec!["user_id".to_string(), "role_id".to_string()];
        let pk_values = vec![zqlz_core::Value::Int64(10), zqlz_core::Value::Int64(20)];

        let (sql, params) = engine.generate_delete_by_pk("user_roles", &pk_columns, &pk_values);

        assert_eq!(
            sql,
            "DELETE FROM \"user_roles\" WHERE \"user_id\" = ? AND \"role_id\" = ?"
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_generate_delete_by_pk_with_quotes_in_table_name() {
        let engine = QueryEngine::new();
        let pk_columns = vec!["id".to_string()];
        let pk_values = vec![zqlz_core::Value::Int64(1)];

        let (sql, _) = engine.generate_delete_by_pk("test\"table", &pk_columns, &pk_values);

        assert_eq!(sql, "DELETE FROM \"test\"\"table\" WHERE \"id\" = ?");
    }

    #[test]
    fn test_generate_delete_by_pk_empty_columns() {
        let engine = QueryEngine::new();
        let pk_columns: Vec<String> = vec![];
        let pk_values: Vec<zqlz_core::Value> = vec![];

        let (sql, params) = engine.generate_delete_by_pk("users", &pk_columns, &pk_values);

        assert_eq!(sql, "");
        assert_eq!(params.len(), 0);
    }

    #[test]
    fn test_generate_delete_by_pk_mismatched_counts() {
        let engine = QueryEngine::new();
        let pk_columns = vec!["id".to_string()];
        let pk_values = vec![zqlz_core::Value::Int64(1), zqlz_core::Value::Int64(2)];

        let (sql, params) = engine.generate_delete_by_pk("users", &pk_columns, &pk_values);

        assert_eq!(sql, "");
        assert_eq!(params.len(), 0);
    }

    #[test]
    fn test_generate_delete_by_full_row() {
        let engine = QueryEngine::new();
        let column_names = vec!["name".to_string(), "age".to_string()];
        let column_values = vec![
            zqlz_core::Value::String("Alice".to_string()),
            zqlz_core::Value::Int64(30),
        ];

        let (sql, params) =
            engine.generate_delete_by_full_row("users", &column_names, &column_values);

        assert_eq!(
            sql,
            "DELETE FROM \"users\" WHERE \"name\" = ? AND \"age\" = ?"
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_generate_delete_by_full_row_with_nulls() {
        let engine = QueryEngine::new();
        let column_names = vec![
            "name".to_string(),
            "middle_name".to_string(),
            "age".to_string(),
        ];
        let column_values = vec![
            zqlz_core::Value::String("Alice".to_string()),
            zqlz_core::Value::Null,
            zqlz_core::Value::Int64(30),
        ];

        let (sql, params) =
            engine.generate_delete_by_full_row("users", &column_names, &column_values);

        assert_eq!(
            sql,
            "DELETE FROM \"users\" WHERE \"name\" = ? AND \"middle_name\" IS NULL AND \"age\" = ?"
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_generate_drop_table() {
        let engine = QueryEngine::new();
        let sql = engine.generate_drop_table("users", false);
        assert_eq!(sql, "DROP TABLE \"users\"");
    }

    #[test]
    fn test_generate_drop_table_if_exists() {
        let engine = QueryEngine::new();
        let sql = engine.generate_drop_table("users", true);
        assert_eq!(sql, "DROP TABLE IF EXISTS \"users\"");
    }

    #[test]
    fn test_generate_drop_table_with_quotes() {
        let engine = QueryEngine::new();
        let sql = engine.generate_drop_table("test\"table", false);
        assert_eq!(sql, "DROP TABLE \"test\"\"table\"");
    }

    #[test]
    fn test_generate_truncate_table() {
        let engine = QueryEngine::new();
        let sql = engine.generate_truncate_table("users");
        assert_eq!(sql, "TRUNCATE TABLE \"users\"");
    }

    #[test]
    fn test_generate_truncate_table_with_quotes() {
        let engine = QueryEngine::new();
        let sql = engine.generate_truncate_table("test\"table");
        assert_eq!(sql, "TRUNCATE TABLE \"test\"\"table\"");
    }
}
