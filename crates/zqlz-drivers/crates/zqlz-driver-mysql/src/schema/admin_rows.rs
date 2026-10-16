//! Objects-panel data fetchers for MySQL/MariaDB admin catalogs (users,
//! roles, grants, plugins, variables, processes, replication status) and the
//! batched routine-parameter and panel-row fetchers for functions,
//! procedures, and triggers.
use super::*;

impl MySqlConnection {
    pub(crate) async fn mysql_user_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = match self
            .query(
                "SELECT User, Host, plugin, account_locked
                 FROM mysql.user
                 ORDER BY User, Host",
                &[],
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(%error, "Failed to load MySQL users");
                return Err(error);
            }
        };
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let user = row.get(0).and_then(|value| value.as_str()).unwrap_or("");
                let host = row.get(1).and_then(|value| value.as_str()).unwrap_or("%");
                let plugin = row.get(2).and_then(|value| value.as_str()).unwrap_or("-");
                let locked = row.get(3).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row(
                    "user",
                    format!("{user}@{host}"),
                    None,
                    None,
                    plugin,
                    host,
                    locked,
                    "-",
                )
            })
            .collect())
    }

    pub(crate) async fn mysql_role_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        // MySQL 8 has no `is_role` column: a role is a locked, password-less
        // account, or any account that appears as the granted side of a role edge.
        // MariaDB flags roles explicitly.
        let result = match self
            .query(
                "SELECT User, Host FROM mysql.user
                 WHERE (account_locked = 'Y' AND password_expired = 'Y' AND authentication_string = '')
                    OR (User, Host) IN (SELECT FROM_USER, FROM_HOST FROM mysql.role_edges)
                 ORDER BY User, Host",
                &[],
            )
            .await
        {
            Ok(result) => result,
            Err(_) => {
                self.query(
                    "SELECT User, Host FROM mysql.user WHERE is_role = 'Y' ORDER BY User, Host",
                    &[],
                )
                .await?
            }
        };
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let user = mysql_text(row.get(0)).unwrap_or_default();
                let host = mysql_text(row.get(1)).unwrap_or_else(|| "%".to_string());
                mysql_metadata_row(
                    "role",
                    format!("{user}@{host}"),
                    None,
                    None,
                    "Role",
                    host,
                    "-",
                    "-",
                )
            })
            .collect())
    }

    /// Privileges per grantee and level (global, schema, table), aggregated so a
    /// user with only global grants such as `root` still shows up.
    pub(crate) async fn mysql_grant_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = match self
            .query(
                "SELECT GRANTEE, 'Global' AS GRANT_LEVEL, NULL AS TABLE_SCHEMA, NULL AS TABLE_NAME,
                        GROUP_CONCAT(PRIVILEGE_TYPE ORDER BY PRIVILEGE_TYPE SEPARATOR ', '),
                        MAX(IS_GRANTABLE)
                 FROM information_schema.USER_PRIVILEGES
                 GROUP BY GRANTEE
                 HAVING SUM(PRIVILEGE_TYPE <> 'USAGE') > 0
                 UNION ALL
                 SELECT GRANTEE, 'Schema', TABLE_SCHEMA, NULL,
                        GROUP_CONCAT(PRIVILEGE_TYPE ORDER BY PRIVILEGE_TYPE SEPARATOR ', '),
                        MAX(IS_GRANTABLE)
                 FROM information_schema.SCHEMA_PRIVILEGES
                 GROUP BY GRANTEE, TABLE_SCHEMA
                 UNION ALL
                 SELECT GRANTEE, 'Table', TABLE_SCHEMA, TABLE_NAME,
                        GROUP_CONCAT(PRIVILEGE_TYPE ORDER BY PRIVILEGE_TYPE SEPARATOR ', '),
                        MAX(IS_GRANTABLE)
                 FROM information_schema.TABLE_PRIVILEGES
                 GROUP BY GRANTEE, TABLE_SCHEMA, TABLE_NAME
                 ORDER BY GRANTEE, GRANT_LEVEL = 'Global' DESC, TABLE_SCHEMA, TABLE_NAME",
                &[],
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(%error, "Failed to load MySQL grants");
                return Err(error);
            }
        };
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let grantee = mysql_text(row.get(0)).unwrap_or_default();
                let level = mysql_text(row.get(1)).unwrap_or_default();
                // SCHEMA_PRIVILEGES stores the LIKE-escaped pattern (`erp\_lab`).
                let schema = mysql_text(row.get(2))
                    .map(|schema| schema.replace("\\_", "_").replace("\\%", "%"));
                let table = mysql_text(row.get(3));
                let privileges = mysql_text(row.get(4)).unwrap_or_else(|| "-".to_string());
                let grantable = mysql_text(row.get(5)).as_deref() == Some("YES");
                // Mirror GRANT syntax: `... ON *.*`, `... ON db.*`, `... ON db.tbl`.
                let scope = match (schema.as_deref(), table.as_deref()) {
                    (Some(schema), Some(table)) => format!("{schema}.{table}"),
                    (Some(schema), None) => format!("{schema}.*"),
                    _ => "*.*".to_string(),
                };
                let mut grant_row = mysql_metadata_row(
                    "grant",
                    format!("{grantee} ON {scope}"),
                    schema,
                    table,
                    level,
                    privileges,
                    if grantable { "WITH GRANT OPTION" } else { "-" },
                    "-",
                );
                // The scope is already in the name; qualifying it again with the
                // schema would print `db.'u'@'%' ON db.tbl`.
                mysql_set_display_name(&mut grant_row, format!("{grantee} ON {scope}"));
                grant_row
            })
            .collect())
    }

    pub(crate) async fn mysql_plugin_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = self.query("SHOW PLUGINS", &[]).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let status = row.get(1).and_then(|value| value.as_str()).unwrap_or("-");
                let kind = row
                    .get(2)
                    .and_then(|value| value.as_str())
                    .unwrap_or("Plugin");
                let library = row.get(3).and_then(|value| value.as_str()).unwrap_or("-");
                let license = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row("plugin", name, None, None, kind, library, status, license)
            })
            .collect())
    }

    pub(crate) async fn mysql_variable_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = self.query("SHOW VARIABLES", &[]).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let value = row
                    .get(1)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_string());
                mysql_metadata_row("variable", name, None, None, "Variable", value, "-", "-")
            })
            .collect())
    }

    pub(crate) async fn mysql_status_variable_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = self.query("SHOW STATUS", &[]).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let name = row
                    .get(0)
                    .and_then(|value| value.as_str())
                    .unwrap_or("")
                    .to_string();
                let value = row
                    .get(1)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_string());
                mysql_metadata_row(
                    "status_variable",
                    name,
                    None,
                    None,
                    "Status",
                    value,
                    "-",
                    "-",
                )
            })
            .collect())
    }

    pub(crate) async fn mysql_process_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = self.query("SHOW PROCESSLIST", &[]).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| {
                let id = row
                    .get(0)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "".to_string());
                let user = row.get(1).and_then(|value| value.as_str()).unwrap_or("-");
                let host = row.get(2).and_then(|value| value.as_str()).unwrap_or("-");
                let db = row
                    .get(3)
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let command = row.get(4).and_then(|value| value.as_str()).unwrap_or("-");
                let state = row.get(6).and_then(|value| value.as_str()).unwrap_or("-");
                let info = row.get(7).and_then(|value| value.as_str()).unwrap_or("-");
                let time = row
                    .get(5)
                    .map(ToString::to_string)
                    .filter(|value| value != "NULL")
                    .unwrap_or_else(|| "-".to_string());
                let mut process_row = mysql_metadata_row(
                    "process",
                    id.clone(),
                    db,
                    None,
                    command,
                    host,
                    state,
                    format!("{user}; {time}s; {info}"),
                );
                // The database already has its own column; `db.<id>` as a name
                // reads like a table.
                mysql_set_display_name(&mut process_row, id);
                process_row
            })
            .collect())
    }

    pub(crate) async fn mysql_replica_status_rows(&self) -> Result<Vec<ObjectsPanelRow>> {
        let result = match self.query("SHOW REPLICA STATUS", &[]).await {
            Ok(result) => result,
            Err(_) => match self.query("SHOW SLAVE STATUS", &[]).await {
                Ok(result) => result,
                Err(error) => {
                    tracing::warn!(%error, "Failed to load MySQL replica status");
                    return Err(error);
                }
            },
        };
        Ok(result
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                let source = row.get(1).and_then(|value| value.as_str()).unwrap_or("-");
                let io_state = row.get(0).and_then(|value| value.as_str()).unwrap_or("-");
                let io_running = row.get(10).and_then(|value| value.as_str()).unwrap_or("-");
                let sql_running = row.get(11).and_then(|value| value.as_str()).unwrap_or("-");
                mysql_metadata_row(
                    "replica_status",
                    format!("replica_{}", index + 1),
                    None,
                    None,
                    "Replica",
                    source,
                    format!("IO {io_running}; SQL {sql_running}"),
                    io_state,
                )
            })
            .collect())
    }

    /// Fetch parameters for every routine in a schema with one query, keyed by
    /// `(SPECIFIC_NAME, ROUTINE_TYPE)`.
    ///
    /// A per-routine lookup costs one information_schema round-trip per routine,
    /// which serializes on the single pooled connection and wedges remote
    /// connections for minutes on schemas with hundreds of routines.
    pub(crate) async fn mysql_routine_parameters_by_routine(
        &self,
        schema: Option<&str>,
    ) -> Result<HashMap<(String, String), Vec<ParameterInfo>>> {
        let schema = schema.or(self.default_database());
        let Some(schema_name) = schema else {
            return Ok(HashMap::new());
        };

        let result = self
            .query(
                "SELECT SPECIFIC_NAME, ROUTINE_TYPE, PARAMETER_NAME, DTD_IDENTIFIER, PARAMETER_MODE, ORDINAL_POSITION
                 FROM information_schema.PARAMETERS
                 WHERE SPECIFIC_SCHEMA = ?
                   AND ORDINAL_POSITION > 0
                 ORDER BY SPECIFIC_NAME, ORDINAL_POSITION",
                &[Value::String(schema_name.to_string())],
            )
            .await?;

        let mut parameters_by_routine: HashMap<(String, String), Vec<ParameterInfo>> =
            HashMap::new();
        for row in &result.rows {
            let routine_name = row
                .get(0)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let routine_type = row
                .get(1)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let mode = match row.get(4).and_then(|value| value.as_str()).unwrap_or("IN") {
                "OUT" => ParameterMode::Out,
                "INOUT" => ParameterMode::InOut,
                _ => ParameterMode::In,
            };

            parameters_by_routine
                .entry((routine_name, routine_type))
                .or_default()
                .push(ParameterInfo {
                    name: row
                        .get(2)
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                    data_type: row
                        .get(3)
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string(),
                    mode,
                    default_value: None,
                    ordinal: row
                        .get(5)
                        .and_then(|value| value.as_i64())
                        .and_then(|value| usize::try_from(value).ok())
                        .unwrap_or(0),
                });
        }

        Ok(parameters_by_routine)
    }

    pub(crate) async fn mysql_routine_panel_rows(
        &self,
        schema: Option<&str>,
        routine_type: &str,
        qualify_with_schema: bool,
    ) -> Result<Vec<ObjectsPanelRow>> {
        let schema = schema.or(self.default_database());
        let (query, params) = if let Some(schema_name) = schema {
            (
                "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, DTD_IDENTIFIER, ROUTINE_DEFINITION,
                        ROUTINE_COMMENT, DEFINER, SECURITY_TYPE, IS_DETERMINISTIC,
                        SQL_DATA_ACCESS, SQL_MODE, CREATED, LAST_ALTERED
                 FROM information_schema.ROUTINES
                 WHERE ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = ?
                 ORDER BY ROUTINE_NAME",
                vec![
                    Value::String(schema_name.to_string()),
                    Value::String(routine_type.to_string()),
                ],
            )
        } else {
            (
                "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, DTD_IDENTIFIER, ROUTINE_DEFINITION,
                        ROUTINE_COMMENT, DEFINER, SECURITY_TYPE, IS_DETERMINISTIC,
                        SQL_DATA_ACCESS, SQL_MODE, CREATED, LAST_ALTERED
                 FROM information_schema.ROUTINES
                 WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = ?
                 ORDER BY ROUTINE_NAME",
                vec![Value::String(routine_type.to_string())],
            )
        };
        let result = self.query(query, &params).await?;
        let kind_id = if routine_type.eq_ignore_ascii_case("FUNCTION") {
            "function"
        } else {
            "procedure"
        };

        let mut parameters_by_routine = self
            .mysql_routine_parameters_by_routine(schema)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(
                    %error,
                    routine_type,
                    "Failed to load MySQL routine parameters for objects panel"
                );
                HashMap::new()
            });

        let mut rows = Vec::with_capacity(result.rows.len());
        for row in &result.rows {
            let name = row
                .get(1)
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            let parameters = parameters_by_routine
                .remove(&(name, routine_type.to_string()))
                .unwrap_or_default();
            rows.push(mysql_routine_panel_row(
                kind_id,
                row,
                parameters,
                qualify_with_schema,
            ));
        }

        Ok(rows)
    }

    pub(crate) async fn mysql_trigger_panel_rows(
        &self,
        schema: Option<&str>,
        table: Option<&str>,
        qualify_with_schema: bool,
    ) -> Result<Vec<ObjectsPanelRow>> {
        let schema = schema.or(self.default_database());
        let select_sql = "SELECT
                    TRIGGER_SCHEMA,
                    TRIGGER_NAME,
                    EVENT_OBJECT_TABLE,
                    ACTION_TIMING,
                    EVENT_MANIPULATION,
                    ACTION_ORDER,
                    ACTION_STATEMENT,
                    ACTION_ORIENTATION,
                    DEFINER,
                    SQL_MODE,
                    CREATED,
                    CHARACTER_SET_CLIENT,
                    COLLATION_CONNECTION,
                    DATABASE_COLLATION
                 FROM information_schema.TRIGGERS";
        let (query, params) = match (schema, table) {
            (Some(schema_name), Some(table_name)) => (
                format!(
                    "{select_sql}
                     WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?
                     ORDER BY TRIGGER_NAME"
                ),
                vec![
                    Value::String(schema_name.to_string()),
                    Value::String(table_name.to_string()),
                ],
            ),
            (Some(schema_name), None) => (
                format!(
                    "{select_sql}
                     WHERE TRIGGER_SCHEMA = ?
                     ORDER BY TRIGGER_NAME"
                ),
                vec![Value::String(schema_name.to_string())],
            ),
            (None, Some(table_name)) => (
                format!(
                    "{select_sql}
                     WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = ?
                     ORDER BY TRIGGER_NAME"
                ),
                vec![Value::String(table_name.to_string())],
            ),
            (None, None) => (
                format!(
                    "{select_sql}
                     WHERE TRIGGER_SCHEMA = DATABASE()
                     ORDER BY TRIGGER_NAME"
                ),
                Vec::new(),
            ),
        };

        let result = self.query(&query, &params).await?;
        Ok(result
            .rows
            .iter()
            .map(|row| mysql_trigger_panel_row(row, qualify_with_schema))
            .collect())
    }
}
