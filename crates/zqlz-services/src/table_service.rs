//! Table operations service
//!
//! Provides table browsing, data retrieval, and cell editing operations.

use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{
    resolve_row_identity, unique_key_candidates, AffectedRowCountFidelity, CellUpdateRequest,
    Connection, DatabaseObject, DriverCategory, DropTableOptions, ObjectType, QueryResult,
    RowIdentifier, RowIdentity, RowIdentityColumn, SchemaIntrospection, SingleRowDmlScope,
    SqlNumericKind, SqlObjectName, SqlTypeFamily, SqlTypeInfo, TableRef, Value,
};
pub use zqlz_table_workflows::{
    plan_fetch, CountPlan, FetchMode, FetchPlan, KnownTotal, QuerySnapshot, RowWindow,
};

use crate::error::{ServiceError, ServiceResult};
use crate::schema_service::SchemaService;
use crate::view_models::TableDetails;

/// Determine whether table browsing should degrade to schema-only mode when
/// the driver declares that metadata can still be shown after a browse error.
pub fn should_use_schema_only_table_browse_fallback(
    connection: &dyn Connection,
    error: &ServiceError,
    details: &TableDetails,
) -> bool {
    connection.should_use_schema_only_table_browse_fallback(details.table_type, &error.to_string())
}

/// Build an empty query result from schema details when data browsing is not
/// available but table metadata can still be shown.
pub fn build_schema_only_query_result(details: &TableDetails) -> QueryResult {
    let columns = details
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, column)| zqlz_core::ColumnMeta {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            ordinal,
            max_length: column.max_length,
            precision: column.precision,
            scale: column.scale,
            auto_increment: column.is_auto_increment,
            default_value: column.default_value.clone(),
            comment: column.comment.clone(),
            enum_values: column.enum_values.clone(),
        })
        .collect();

    let warning = match details.table_type {
        zqlz_core::TableType::ForeignTable => {
            "Schema-only fallback: foreign table metadata loaded, but data browsing failed"
        }
        zqlz_core::TableType::VirtualTable => {
            "Schema-only fallback: virtual table metadata loaded, but data browsing failed"
        }
        _ => "Schema-only fallback: metadata loaded, but data browsing failed",
    };

    QueryResult {
        id: Uuid::new_v4(),
        columns,
        rows: Vec::new(),
        total_rows: Some(0),
        is_estimated_total: false,
        affected_rows: 0,
        execution_time_ms: 0,
        warnings: vec![warning.to_string()],
    }
}

/// Schema payload returned by open-viewer initial load.
pub struct OpenViewerSchemaLoad {
    pub table_details: TableDetails,
    pub create_statement: Option<String>,
}

/// Non-UI schema metadata that the table viewer needs after schema load.
pub struct OpenViewerSchemaViewerMetadata {
    pub foreign_keys_for_viewer: Vec<zqlz_core::ForeignKeyInfo>,
    pub schema_columns: Vec<crate::view_models::ColumnInfo>,
    pub primary_key_columns: Vec<String>,
    /// Columns the viewer matches on when a row is edited or deleted.
    pub row_identity: RowIdentity,
}

/// How drivers that take a raw table string name one: SQLite's `main` bare,
/// anything else `schema.name`.
fn raw_driver_table_name(table: &TableRef) -> String {
    match table.schema.as_deref() {
        Some("main") | None => table.name.clone(),
        Some(schema) => format!("{schema}.{}", table.name),
    }
}

/// The schema and kind of the relation the catalog lists as exactly `name` in
/// `schema` (every schema when `None`).
async fn find_relation(
    connection: &dyn Connection,
    schema: Option<&str>,
    name: &str,
) -> Option<(Option<String>, bool)> {
    let introspection = connection.as_schema_introspection()?;
    // Some catalogs ignore a schema they do not know and list their default one.
    let listed = |listed_name: &str, listed_schema: Option<&str>| {
        let in_schema = match (schema, listed_schema) {
            (Some(requested), Some(listed_schema)) => requested == listed_schema,
            _ => true,
        };
        in_schema
            && TableRef::from_listing(None, listed_schema.map(Into::into), listed_name).name == name
    };
    if let Ok(tables) = introspection.list_tables(schema).await {
        if let Some(table) = tables
            .into_iter()
            .find(|table| listed(&table.name, table.schema.as_deref()))
        {
            return Some((table.schema, false));
        }
    }
    if let Ok(views) = introspection.list_views(schema).await {
        if let Some(view) = views
            .into_iter()
            .find(|view| listed(&view.name, view.schema.as_deref()))
        {
            return Some((view.schema, true));
        }
    }
    if connection.supports_materialized_views() {
        if let Ok(views) = introspection.list_materialized_views(schema).await {
            if let Some(view) = views
                .into_iter()
                .find(|view| listed(&view.name, view.schema.as_deref()))
            {
                return Some((view.schema, true));
            }
        }
    }
    None
}

/// Build table-viewer metadata from loaded schema details.
///
/// The app uses this to update table-viewer state while keeping schema-shaping
/// rules outside the UI layer.
pub fn build_open_viewer_schema_viewer_metadata(
    schema_load: &OpenViewerSchemaLoad,
) -> OpenViewerSchemaViewerMetadata {
    let foreign_keys_for_viewer: Vec<zqlz_core::ForeignKeyInfo> = schema_load
        .table_details
        .foreign_keys
        .iter()
        .map(|foreign_key| zqlz_core::ForeignKeyInfo {
            name: foreign_key.name.clone(),
            columns: foreign_key.columns.clone(),
            referenced_table: foreign_key.referenced_table.clone(),
            referenced_schema: foreign_key.referenced_schema.clone(),
            referenced_columns: foreign_key.referenced_columns.clone(),
            on_update: foreign_key.on_update,
            on_delete: foreign_key.on_delete,
            is_deferrable: false,
            initially_deferred: false,
        })
        .collect();

    let identity_columns: Vec<RowIdentityColumn<'_>> = schema_load
        .table_details
        .columns
        .iter()
        .map(|column| RowIdentityColumn {
            name: column.name.as_str(),
            nullable: column.nullable,
        })
        .collect();

    OpenViewerSchemaViewerMetadata {
        foreign_keys_for_viewer,
        schema_columns: schema_load.table_details.columns.clone(),
        primary_key_columns: schema_load.table_details.primary_key_columns.clone(),
        row_identity: resolve_row_identity(
            schema_load.table_details.table_type,
            &identity_columns,
            &schema_load.table_details.primary_key_columns,
            &schema_load.table_details.constraints,
            &schema_load.table_details.indexes,
        ),
    }
}

/// Service for table-level operations
///
/// Handles:
/// - Table data browsing with pagination
/// - Cell updates with proper row identification
/// - Value parsing and type conversion
pub struct TableService {
    default_limit: usize,
}

impl TableService {
    fn ensure_relational_connection(connection: &dyn Connection) -> ServiceResult<()> {
        match connection.driver_category() {
            DriverCategory::KeyValue | DriverCategory::Document => {
                Err(ServiceError::TableOperationFailed(
                    "TableService only supports relational connections".to_string(),
                ))
            }
            _ => Ok(()),
        }
    }

    /// Fill in the schema a [`TableRef`] is missing, once per open.
    ///
    /// A known schema is trusted without a round-trip. Otherwise the name is
    /// matched exactly against the catalog, the session's own schema first. A name
    /// an old session stored joined (`"schema.name"`) is split only when the
    /// catalog confirms that split; anything unmatched is returned unchanged.
    pub async fn resolve_table(
        &self,
        connection: Arc<dyn Connection>,
        table: TableRef,
    ) -> TableRef {
        if table.schema.is_some() || connection.as_schema_introspection().is_none() {
            return table;
        }
        // MySQL's database is its namespace; elsewhere the session's schema is
        // searched before every schema, as the server's search path would.
        let preferred_schema = match connection.table_sql_name(&table).namespace {
            Some(namespace) => Some(namespace),
            None => connection
                .current_namespace_name()
                .await
                .unwrap_or_else(|error| {
                    tracing::debug!(%error, "Session schema unavailable; searching every schema");
                    None
                }),
        };
        for schema in [preferred_schema.as_deref(), None] {
            if let Some((found_schema, is_view)) =
                find_relation(connection.as_ref(), schema, &table.name).await
            {
                return TableRef {
                    schema: found_schema.or_else(|| schema.map(ToString::to_string)),
                    is_view: is_view || table.is_view,
                    ..table
                };
            }
            if preferred_schema.is_none() {
                break;
            }
        }
        if let Some((schema, name)) = table.name.split_once('.') {
            if let Some((_, is_view)) = find_relation(connection.as_ref(), Some(schema), name).await
            {
                return TableRef {
                    schema: Some(schema.to_string()),
                    name: name.to_string(),
                    is_view: is_view || table.is_view,
                    ..table
                };
            }
        }
        table
    }

    /// The table's primary key columns, unquoted; empty when it has none or the
    /// lookup fails.
    ///
    /// Every page sorts by the key, so the first page needs it before the schema
    /// load that otherwise supplies it has landed.
    pub async fn primary_key_columns(
        &self,
        connection: Arc<dyn Connection>,
        table: &TableRef,
    ) -> Vec<String> {
        let Some(introspection) = connection.as_schema_introspection() else {
            return Vec::new();
        };
        introspection
            .get_primary_key(table.schema.as_deref(), &table.name)
            .await
            .unwrap_or_else(|error| {
                tracing::debug!(%error, "Primary key lookup failed; first page is unordered");
                None
            })
            .map(|primary_key| primary_key.columns)
            .unwrap_or_default()
    }

    /// Load the schema metadata that decorates the table viewer.
    ///
    /// Runs alongside the row fetch and updates the UI when it lands.
    pub async fn load_open_viewer_schema(
        &self,
        schema_service: Arc<SchemaService>,
        connection: Arc<dyn Connection>,
        connection_id: Uuid,
        table: &TableRef,
        is_view: bool,
    ) -> ServiceResult<OpenViewerSchemaLoad> {
        let schema_ref = table.schema.as_deref();
        let table_details = schema_service
            .get_table_details(connection.clone(), connection_id, &table.name, schema_ref)
            .await?;
        let create_statement = schema_service
            .get_or_generate_ddl(
                &connection,
                connection_id,
                &table.name,
                schema_ref,
                if is_view {
                    Some(zqlz_core::ObjectType::View)
                } else {
                    None
                },
            )
            .await;

        Ok(OpenViewerSchemaLoad {
            table_details,
            create_statement,
        })
    }

    /// Whether a failed browse can still render from schema alone.
    ///
    /// Some relations (for example a driver's system views) refuse a plain `SELECT`
    /// but still describe their columns, so the viewer shows an empty, correctly
    /// shaped grid rather than an error.
    pub fn schema_only_browse_fallback(
        connection: &dyn Connection,
        browse_error: &ServiceError,
        schema_load: &OpenViewerSchemaLoad,
    ) -> Option<QueryResult> {
        should_use_schema_only_table_browse_fallback(
            connection,
            browse_error,
            &schema_load.table_details,
        )
        .then(|| build_schema_only_query_result(&schema_load.table_details))
    }

    /// Create a new table service
    ///
    /// # Arguments
    ///
    /// * `default_limit` - Default number of rows to return when browsing tables
    pub fn new(default_limit: usize) -> Self {
        Self { default_limit }
    }

    /// Fetch the rows a [`FetchPlan`] describes, with the total it asks for.
    ///
    /// Queries run one after another: `tokio::join!` on one async-mutex-guarded
    /// connection causes waker contention on GPUI's executor and spurious
    /// "connection closed" errors.
    #[tracing::instrument(skip(self, connection, query))]
    pub async fn fetch_rows(
        &self,
        connection: Arc<dyn Connection>,
        table: &TableRef,
        query: &QuerySnapshot,
        plan: FetchPlan,
    ) -> ServiceResult<FetchOutcome> {
        Self::ensure_relational_connection(connection.as_ref())?;
        let qualified = Self::qualified_relation_name(connection.as_ref(), table);
        let where_sql = if query.where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", query.where_clauses.join(" AND "))
        };

        let plan = if plan.window == RowWindow::LastPage {
            let rows = Self::count_rows_where(connection.as_ref(), &qualified, &where_sql).await?;
            let counted = QuerySnapshot {
                known_total: Some(KnownTotal {
                    rows,
                    is_estimated: false,
                    where_clauses: query.where_clauses.clone(),
                }),
                ..query.clone()
            };
            plan_fetch(FetchMode::Last, &counted).ok_or_else(|| {
                ServiceError::TableOperationFailed("No last page to fetch".to_string())
            })?
        } else {
            plan
        };

        let columns = if query.visible_columns.is_empty() {
            "*".to_string()
        } else {
            query
                .visible_columns
                .iter()
                .map(|column| connection.quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut base_sql = format!("SELECT {columns} FROM {qualified}{where_sql}");
        if !plan.order_by.is_empty() {
            base_sql.push_str(" ORDER BY ");
            base_sql.push_str(&plan.order_by.join(", "));
        }
        let data_sql = match plan.window {
            RowWindow::All | RowWindow::LastPage => base_sql,
            RowWindow::Forward { limit, offset } => {
                connection.paginated_select_sql(&base_sql, limit as u64, offset as u64)
            }
            RowWindow::FromEnd { limit, skip } => {
                connection.paginated_select_sql(&base_sql, limit as u64, skip as u64)
            }
        };
        tracing::debug!(sql = %data_sql, "Fetching table rows");

        let mut result = connection
            .query(&data_sql, &[])
            .await
            .map_err(|error| ServiceError::TableOperationFailed(error.to_string()))?;
        if matches!(plan.window, RowWindow::FromEnd { .. }) {
            result.rows.reverse();
        }

        let total = match &plan.count {
            _ if plan.window == RowWindow::All => Some(KnownTotal {
                rows: result.rows.len() as u64,
                is_estimated: false,
                where_clauses: query.where_clauses.clone(),
            }),
            CountPlan::Reuse(total) => Some(total.clone()),
            CountPlan::Exact => {
                match Self::count_rows_where(connection.as_ref(), &qualified, &where_sql).await {
                    Ok(rows) => Some(KnownTotal {
                        rows,
                        is_estimated: false,
                        where_clauses: query.where_clauses.clone(),
                    }),
                    Err(error) => {
                        tracing::warn!(%error, "COUNT(*) failed, pagination total unavailable");
                        None
                    }
                }
            }
            CountPlan::Estimate => {
                match self
                    .estimate_resolved_relation_row_count(connection.clone(), table)
                    .await
                {
                    Ok(estimate) => estimate.map(|(rows, is_estimated)| KnownTotal {
                        rows,
                        is_estimated,
                        where_clauses: Vec::new(),
                    }),
                    Err(error) => {
                        tracing::warn!(%error, "Row estimate failed, pagination total unavailable");
                        None
                    }
                }
            }
            CountPlan::Skip => None,
        };
        // Stale table statistics (MySQL caches them for a day) can claim fewer
        // rows than have already been read, which would disable Next. Fall back to
        // judging more pages by whether this one came back full.
        let rows_read = match plan.window {
            RowWindow::Forward { offset, .. } => offset + result.rows.len(),
            _ => result.rows.len(),
        } as u64;
        let total = total.filter(|total| !total.is_estimated || total.rows >= rows_read);

        result.total_rows = total.as_ref().map(|total| total.rows);
        result.is_estimated_total = total.as_ref().is_some_and(|total| total.is_estimated);

        tracing::info!(
            table_name = %table.name,
            rows = result.rows.len(),
            total = ?result.total_rows,
            window = ?plan.window,
            "Table rows fetched"
        );

        Ok(FetchOutcome {
            result,
            total,
            plan,
        })
    }

    async fn count_rows_where(
        connection: &dyn Connection,
        qualified: &str,
        where_sql: &str,
    ) -> ServiceResult<u64> {
        let count_sql = connection.paginated_select_sql(
            &format!("SELECT COUNT(*) FROM {qualified}{where_sql}"),
            1,
            0,
        );
        connection
            .query(&count_sql, &[])
            .await
            .map_err(|error| ServiceError::TableOperationFailed(error.to_string()))?
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(|value| value.as_i64())
            .map(|count| count as u64)
            .ok_or_else(|| {
                ServiceError::TableOperationFailed("COUNT(*) query returned no result".to_string())
            })
    }

    /// Get an estimated row count from database metadata instead of COUNT(*).
    ///
    /// For MySQL and ClickHouse, uses `information_schema.TABLES` which stores
    /// an approximate row count maintained by the storage engine. For PostgreSQL,
    /// uses `pg_class.reltuples` which is updated by ANALYZE. For MSSQL, uses
    /// `sys.dm_db_partition_stats`. These are all O(1) lookups.
    ///
    /// For drivers where COUNT(*) is already fast (sqlite, duckdb), falls back
    /// to an exact COUNT(*).
    #[tracing::instrument(skip(self, connection))]
    pub async fn estimate_row_count(
        &self,
        connection: Arc<dyn Connection>,
        table: &TableRef,
    ) -> ServiceResult<Option<(u64, bool)>> {
        Self::ensure_relational_connection(connection.as_ref())?;
        self.estimate_resolved_relation_row_count(connection, table)
            .await
    }

    /// Count rows of a relation whose name has already been resolved.
    ///
    /// Resolution can cost several catalog round-trips, so callers that already
    /// paid for it pass the resolved form through rather than re-deriving it.
    async fn estimate_resolved_relation_row_count(
        &self,
        connection: Arc<dyn Connection>,
        table: &TableRef,
    ) -> ServiceResult<Option<(u64, bool)>> {
        if connection.supports_fast_exact_count() {
            let qualified = Self::qualified_relation_name(connection.as_ref(), table);
            let count_base_sql = format!("SELECT COUNT(*) FROM {}", qualified);
            let count_sql = connection.paginated_select_sql(&count_base_sql, 1, 0);
            let count_result = connection
                .query(&count_sql, &[])
                .await
                .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

            let total = count_result
                .rows
                .first()
                .and_then(|row| row.values.first())
                .and_then(|v| v.as_i64())
                .map(|i| i as u64)
                .ok_or_else(|| {
                    ServiceError::TableOperationFailed(
                        "COUNT(*) query returned no result".to_string(),
                    )
                })?;

            return Ok(Some((total, false)));
        }

        let qualified_table_name = connection.table_sql_name(table);

        let estimated = connection
            .estimated_row_count(&qualified_table_name)
            .await
            .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

        let Some(estimated) = estimated else {
            tracing::debug!(
                table_name = %qualified_table_name.name,
                schema = ?qualified_table_name.namespace,
                "Estimated row count unavailable from metadata"
            );
            return Ok(None);
        };

        tracing::info!(
            table_name = %qualified_table_name.name,
            estimated = estimated,
            "Estimated row count from metadata"
        );

        Ok(Some((estimated, true)))
    }

    /// Update a cell value in a table
    ///
    /// This method automatically determines the best row identifier (primary key
    /// or full row) and performs the update.
    ///
    /// # Arguments
    ///
    /// * `connection` - Database connection
    /// * `table_name` - Name of the table
    /// * `cell_data` - Cell update data including column, value, and row context
    ///
    /// # Returns
    ///
    /// A [`CellUpdateOutcome`] if the update succeeds
    #[tracing::instrument(skip(self, connection, cell_data), fields(table_name = %table.name))]
    pub async fn update_cell(
        &self,
        connection: Arc<dyn Connection>,
        table: &TableRef,
        cell_data: CellUpdateData,
    ) -> ServiceResult<CellUpdateOutcome> {
        self.update_cell_inner(connection, table, cell_data, true)
            .await
    }

    /// Update a cell without re-reading the stored value.
    ///
    /// For callers that refresh the whole table afterwards, the per-cell
    /// read-back is redundant and would add one round trip per cell.
    pub async fn update_cell_without_read_back(
        &self,
        connection: Arc<dyn Connection>,
        table: &TableRef,
        cell_data: CellUpdateData,
    ) -> ServiceResult<CellUpdateOutcome> {
        self.update_cell_inner(connection, table, cell_data, false)
            .await
    }

    async fn update_cell_inner(
        &self,
        connection: Arc<dyn Connection>,
        table: &TableRef,
        cell_data: CellUpdateData,
        read_back: bool,
    ) -> ServiceResult<CellUpdateOutcome> {
        Self::ensure_relational_connection(connection.as_ref())?;
        tracing::debug!("Updating cell in table {}", table.name);
        // Build row identifier (use primary key if available)
        let row_identifier = self
            .build_row_identifier(connection.clone(), table, &cell_data)
            .await?;

        // Parse new value using the target column's type
        let target_col_type = cell_data
            .all_column_names
            .iter()
            .position(|c| c == &cell_data.column_name)
            .and_then(|idx| cell_data.all_column_types.get(idx))
            .map(|s| s.as_str());

        let new_value = match &cell_data.new_value {
            None => None,
            Some(Value::String(val)) => Some(self.parse_value(val, target_col_type)?),
            Some(other) => Some(other.clone()),
        };

        let update_request = CellUpdateRequest {
            table_name: raw_driver_table_name(table),
            column_name: cell_data.column_name.clone(),
            column_type: target_col_type.map(str::to_string),
            row_column_types: cell_data
                .all_column_names
                .iter()
                .cloned()
                .zip(cell_data.all_column_types.iter().cloned())
                .collect(),
            new_value,
            row_identifier: row_identifier.clone(),
        };

        let affected_rows = connection
            .update_cell(update_request)
            .await
            .map_err(|e| ServiceError::UpdateFailed(e.to_string()))?;

        // A zero count only proves the row is gone on drivers that report matched
        // rows. Treating it as failure elsewhere turns a harmless no-op update into
        // a phantom "modified by another user" error.
        let unconfirmed_reason = match connection.affected_row_count_fidelity() {
            AffectedRowCountFidelity::Exact if affected_rows == 0 => {
                return Err(ServiceError::UpdateFailed(
                    "No rows matched - the row may have been modified or deleted by another user"
                        .to_string(),
                ));
            }
            AffectedRowCountFidelity::Exact => None,
            AffectedRowCountFidelity::BestEffort if affected_rows == 0 => Some(
                "The database did not report a row count for this update, so the change could not be confirmed."
                    .to_string(),
            ),
            AffectedRowCountFidelity::BestEffort => None,
            AffectedRowCountFidelity::Unavailable => Some(format!(
                "{} does not report update row counts, so the change could not be confirmed.",
                connection.driver_name()
            )),
        };

        let stored_value = if read_back {
            self.read_back_cell(
                connection.as_ref(),
                table,
                &cell_data.column_name,
                &row_identifier,
            )
            .await
        } else {
            None
        };

        tracing::info!(
            table_name = %table.name,
            column = %cell_data.column_name,
            affected_rows = affected_rows,
            unconfirmed = unconfirmed_reason.is_some(),
            "Cell updated successfully"
        );

        Ok(CellUpdateOutcome {
            affected_rows,
            unconfirmed_reason,
            stored_value,
        })
    }

    /// Re-read the column that was just written so callers can show what the
    /// database actually stored rather than what the client sent.
    ///
    /// Databases silently adjust values on write — rounding a DECIMAL to its
    /// declared scale, truncating a VARCHAR, coercing a type — and without this
    /// the grid would keep displaying the typed value indefinitely.
    ///
    /// Returns `None` (never an error) when the value cannot be re-read: the
    /// write itself already succeeded, so a failed confirmation must not fail
    /// the update.
    async fn read_back_cell(
        &self,
        connection: &dyn Connection,
        table: &TableRef,
        column_name: &str,
        row_identifier: &RowIdentifier,
    ) -> Option<Value> {
        // A FullRow identifier matches on the pre-update value of the column we
        // just changed, so it would select nothing after the write.
        if !matches!(row_identifier, RowIdentifier::PrimaryKey(_)) {
            return None;
        }

        let (where_clause, params) = self
            .build_where_clause(connection, row_identifier)
            .map_err(|error| {
                tracing::warn!("Could not build cell read-back query: {}", error);
                error
            })
            .ok()?;

        let sql = format!(
            "SELECT {} FROM {} WHERE {}",
            connection.quote_identifier(column_name),
            Self::qualified_relation_name(connection, table),
            where_clause
        );

        match connection.query(&sql, &params).await {
            Ok(result) => result
                .rows
                .first()
                .and_then(|row| row.values.first())
                .cloned(),
            Err(error) => {
                tracing::warn!(
                    column = %column_name,
                    "Cell update succeeded but the stored value could not be read back: {}",
                    error
                );
                None
            }
        }
    }

    /// Build row identifier (prefer primary key, then a unique key, then the full row)
    async fn build_row_identifier(
        &self,
        connection: Arc<dyn Connection>,
        table: &TableRef,
        cell_data: &CellUpdateData,
    ) -> ServiceResult<RowIdentifier> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        if let Ok(Some(pk_info)) = schema
            .get_primary_key(table.schema.as_deref(), &table.name)
            .await
        {
            if let Some(key_values) = self.key_values_for_row(
                &pk_info.columns,
                &cell_data.all_column_names,
                &cell_data.all_row_values,
                &cell_data.all_column_types,
            )? {
                tracing::debug!("Using primary key for row identification");
                return Ok(RowIdentifier::PrimaryKey(key_values));
            }
        }

        // A unique key identifies the row just as precisely as a primary key,
        // and keeps the statement off every other column of the row.
        for (key_name, key_columns) in self.unique_keys_for_relation(schema, table).await {
            if let Some(key_values) = self.key_values_for_row(
                &key_columns,
                &cell_data.all_column_names,
                &cell_data.all_row_values,
                &cell_data.all_column_types,
            )? {
                tracing::debug!(
                    unique_key = %key_name,
                    "Using unique key for row identification"
                );
                return Ok(RowIdentifier::PrimaryKey(key_values));
            }
        }

        // Fallback: use all columns. Drivers narrow this to a single row so
        // duplicate rows are not written together.
        tracing::debug!("Using full row for row identification (no key available)");
        let row_values: Vec<(String, Value)> = cell_data
            .all_column_names
            .iter()
            .zip(cell_data.all_row_values.iter())
            .enumerate()
            .map(|(idx, (col, val))| {
                let col_type = cell_data.all_column_types.get(idx).map(|s| s.as_str());
                let value = match val {
                    Value::String(text) => self.parse_value(text, col_type),
                    other => Ok(other.clone()),
                };
                Ok((col.clone(), value?))
            })
            .collect::<ServiceResult<Vec<_>>>()?;

        Ok(RowIdentifier::FullRow(row_values))
    }

    /// Unique keys of a relation that could identify one of its rows, narrowest
    /// first. Returns nothing when the metadata cannot be read, so callers fall
    /// back to matching on all columns.
    async fn unique_keys_for_relation(
        &self,
        schema: &dyn SchemaIntrospection,
        table: &TableRef,
    ) -> Vec<(String, Vec<String>)> {
        let constraints = schema
            .get_constraints(table.schema.as_deref(), &table.name)
            .await
            .unwrap_or_default();
        let indexes = schema
            .get_indexes(table.schema.as_deref(), &table.name)
            .await
            .unwrap_or_default();

        unique_key_candidates(&constraints, &indexes)
    }

    /// The key's value in this row, or `None` when the key cannot address it.
    ///
    /// A key column missing from the row, or NULL in it, identifies nothing:
    /// NULL never compares equal to itself, so such a key would match no row.
    fn key_values_for_row(
        &self,
        key_columns: &[String],
        column_names: &[String],
        row_values: &[Value],
        column_types: &[String],
    ) -> ServiceResult<Option<Vec<(String, Value)>>> {
        if key_columns.is_empty() {
            return Ok(None);
        }

        let mut key_values = Vec::with_capacity(key_columns.len());
        for key_column in key_columns {
            let Some(index) = column_names.iter().position(|col| col == key_column) else {
                return Ok(None);
            };
            let Some(value) = row_values.get(index).cloned() else {
                return Ok(None);
            };

            // Without a declared type there is nothing to parse against, and
            // guessing would risk turning a text key into a number.
            let value = match (value, column_types.get(index)) {
                (Value::String(text), Some(column_type)) => {
                    self.parse_value(&text, Some(column_type.as_str()))?
                }
                (other, _) => other,
            };

            if value.is_null() {
                return Ok(None);
            }

            key_values.push((key_column.clone(), value));
        }

        Ok(Some(key_values))
    }

    /// Parse string value to typed Value
    ///
    /// Uses the database column type when available to avoid misinterpreting
    /// numeric-looking strings (e.g. phone numbers, postal codes) as integers.
    /// Falls back to heuristic inference when no type hint is provided.
    fn parse_value(&self, value_str: &str, column_type: Option<&str>) -> ServiceResult<Value> {
        if value_str.is_empty() {
            return Ok(Value::String(String::new()));
        }

        if value_str.eq_ignore_ascii_case("null") {
            return Ok(Value::Null);
        }

        // When we know the column type, use it to guide parsing
        if let Some(col_type) = column_type {
            return self.parse_value_with_type(value_str, col_type);
        }

        // Fallback: heuristic type inference (no column type available)
        if let Ok(int_val) = value_str.parse::<i64>() {
            if int_val >= i32::MIN as i64 && int_val <= i32::MAX as i64 {
                return Ok(Value::Int32(int_val as i32));
            }
            return Ok(Value::Int64(int_val));
        }

        if let Ok(float_val) = value_str.parse::<f64>() {
            return Ok(Value::Float64(float_val));
        }

        if value_str.eq_ignore_ascii_case("true") {
            return Ok(Value::Bool(true));
        }

        if value_str.eq_ignore_ascii_case("false") {
            return Ok(Value::Bool(false));
        }

        Ok(Value::String(value_str.to_string()))
    }

    /// Parse a value string using the known database column type
    ///
    /// Dispatches on [`SqlTypeInfo`], which strips length/precision modifiers so
    /// that declarations like `decimal(12,4)` and `varchar(255)` classify the
    /// same as their bare forms. Matching on the raw type name would miss them
    /// and silently fall through to the string catch-all.
    fn parse_value_with_type(&self, value_str: &str, column_type: &str) -> ServiceResult<Value> {
        let col_type = Value::normalize_data_type(column_type);
        let type_info = SqlTypeInfo::from_data_type(column_type);

        if type_info.is_array {
            return Ok(Value::parse_from_string(value_str, &col_type));
        }

        if matches!(type_info.base_type.as_str(), "json" | "jsonb") {
            return match Value::parse_from_string(value_str, &col_type) {
                Value::Json(value) => Ok(Value::Json(value)),
                _ => Err(ServiceError::InvalidValue(format!(
                    "Invalid JSON value for {} column",
                    column_type
                ))),
            };
        }

        if type_info.base_type == "set" {
            return Ok(Value::parse_from_string(value_str, &col_type));
        }

        // Checked before the numeric kinds so MySQL's `tinyint(1)` stays boolean
        // rather than classifying as a small integer.
        if type_info.family == SqlTypeFamily::Boolean {
            return match value_str.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "f" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Ok(Value::String(value_str.to_string())),
            };
        }

        if matches!(type_info.family, SqlTypeFamily::Text | SqlTypeFamily::Enum) {
            return Ok(Value::String(value_str.to_string()));
        }

        match type_info.numeric_kind {
            Some(SqlNumericKind::Integer) => {
                if let Ok(val) = value_str.parse::<i64>() {
                    if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
                        return Ok(Value::Int32(val as i32));
                    }
                    return Ok(Value::Int64(val));
                }
                Ok(Value::String(value_str.to_string()))
            }
            Some(SqlNumericKind::Float) => {
                if let Ok(val) = value_str.parse::<f64>() {
                    return Ok(Value::Float64(val));
                }
                Ok(Value::String(value_str.to_string()))
            }
            // Kept as digits rather than going through f64 so that values wider
            // than a double can round-trip without losing precision.
            Some(SqlNumericKind::Decimal) => {
                if Value::is_sql_numeric_literal(value_str) {
                    return Ok(Value::Decimal(value_str.to_string()));
                }
                Ok(Value::String(value_str.to_string()))
            }
            // Dates, timestamps, UUIDs, etc. — keep as string and let the
            // driver handle formatting in value_to_pg_literal / equivalent
            None => Ok(Value::String(value_str.to_string())),
        }
    }

    fn qualified_relation_name(connection: &dyn Connection, table: &TableRef) -> String {
        connection.render_qualified_name(&connection.table_sql_name(table))
    }

    #[cfg(test)]
    fn escape_identifier_for(identifier: &str, driver_name: &str) -> String {
        match driver_name.to_ascii_lowercase().as_str() {
            "mysql" | "mariadb" => {
                format!("`{}`", identifier.replace('`', "``"))
            }
            "mssql" | "sqlserver" => {
                format!("[{}]", identifier.replace(']', "]]"))
            }
            _ => {
                format!("\"{}\"", identifier.replace('"', "\"\""))
            }
        }
    }

    /// Get the default limit for table browsing
    pub fn default_limit(&self) -> usize {
        self.default_limit
    }

    /// Set the default limit for table browsing
    pub fn set_default_limit(&mut self, limit: usize) {
        self.default_limit = limit;
    }

    /// Insert a new row into a table
    ///
    /// # Arguments
    ///
    /// * `connection` - Database connection
    /// * `table_name` - Name of the table
    /// * `row_data` - Row data to insert
    ///
    /// # Returns
    ///
    /// `Ok(())` if the insert succeeds
    #[tracing::instrument(skip(self, connection, row_data), fields(table_name = %table.name))]
    pub async fn insert_row(
        &self,
        connection: Arc<dyn Connection>,
        table: &TableRef,
        row_data: RowInsertData,
    ) -> ServiceResult<()> {
        Self::ensure_relational_connection(connection.as_ref())?;
        tracing::debug!("Inserting row into table {}", table.name);
        if row_data.column_names.is_empty() {
            return Err(ServiceError::TableOperationFailed(
                "No columns specified for insert".to_string(),
            ));
        }

        if row_data.column_names.len() != row_data.values.len() {
            return Err(ServiceError::TableOperationFailed(
                "Column/value count mismatch for insert".to_string(),
            ));
        }

        if !row_data.column_types.is_empty()
            && row_data.column_types.len() != row_data.column_names.len()
        {
            return Err(ServiceError::TableOperationFailed(
                "Column/type count mismatch for insert".to_string(),
            ));
        }

        let schema_columns =
            if let Some(schema_introspection) = connection.as_schema_introspection() {
                schema_introspection
                    .get_columns(table.schema.as_deref(), &table.name)
                    .await
                    .map(|columns| {
                        columns
                            .into_iter()
                            .map(|col| {
                                let name = col.name.clone();
                                (name, col)
                            })
                            .collect::<std::collections::HashMap<_, _>>()
                    })
                    .ok()
            } else {
                None
            };

        let mut column_types = if !row_data.column_types.is_empty() {
            row_data
                .column_names
                .iter()
                .cloned()
                .zip(row_data.column_types.iter().cloned())
                .collect::<std::collections::HashMap<_, _>>()
        } else {
            std::collections::HashMap::new()
        };

        if let Some(columns) = &schema_columns {
            for (name, info) in columns {
                column_types
                    .entry(name.clone())
                    .or_insert_with(|| info.data_type.clone());
            }
        }

        // Build column list, placeholders, and params
        let mut columns = Vec::new();
        let mut placeholders = Vec::new();
        let mut params = Vec::new();
        let mut param_index = 1;

        for (column_name, value) in row_data.column_names.iter().zip(row_data.values.iter()) {
            let column_info = schema_columns
                .as_ref()
                .and_then(|cols| cols.get(column_name));
            let has_default = column_info
                .and_then(|info| info.default_value.as_ref())
                .map(|default| !default.trim().is_empty())
                .unwrap_or(false);
            let is_auto_increment = column_info
                .map(|info| info.is_auto_increment)
                .unwrap_or(false)
                || column_info
                    .and_then(|info| info.default_value.as_ref())
                    .map(|default| default.to_lowercase().contains("nextval("))
                    .unwrap_or(false);

            match value {
                Some(val) => {
                    columns.push(connection.quote_identifier(column_name));
                    placeholders.push(connection.format_bind_placeholder(param_index - 1));
                    param_index += 1;
                    let column_type = column_types.get(column_name).map(|s| s.as_str());
                    let parsed_value = match val {
                        Value::String(text) => self.parse_value(text, column_type)?,
                        other => other.clone(),
                    };
                    params.push(parsed_value);
                }
                None => {
                    if has_default || is_auto_increment {
                        continue;
                    }
                    columns.push(connection.quote_identifier(column_name));
                    placeholders.push("NULL".to_string());
                }
            }
        }

        if columns.is_empty() {
            return Err(ServiceError::TableOperationFailed(
                "No values provided for insert".to_string(),
            ));
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            Self::qualified_relation_name(connection.as_ref(), table),
            columns.join(", "),
            placeholders.join(", ")
        );

        tracing::debug!("Insert SQL: {}", sql);

        connection
            .execute(&sql, &params)
            .await
            .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

        tracing::info!(
            table_name = %table.name,
            "Row inserted successfully"
        );

        Ok(())
    }

    /// Delete rows from a table
    ///
    /// # Arguments
    ///
    /// * `connection` - Database connection
    /// * `table_name` - Name of the table
    /// * `delete_data` - Data identifying rows to delete
    ///
    /// # Returns
    ///
    /// Number of rows deleted
    #[tracing::instrument(skip(self, connection, delete_data), fields(table_name = %table.name))]
    pub async fn delete_rows(
        &self,
        connection: Arc<dyn Connection>,
        table: &TableRef,
        delete_data: RowDeleteData,
    ) -> ServiceResult<u64> {
        Self::ensure_relational_connection(connection.as_ref())?;
        tracing::debug!(
            "Deleting {} rows from table {}",
            delete_data.rows.len(),
            table.name
        );

        if delete_data.rows.is_empty() {
            return Ok(0);
        }

        let schema_introspection = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        // Keys that can address a row on their own, best first.
        let mut key_candidates: Vec<Vec<String>> = schema_introspection
            .get_primary_key(table.schema.as_deref(), &table.name)
            .await
            .ok()
            .flatten()
            .map(|primary_key| primary_key.columns)
            .into_iter()
            .collect();
        if key_candidates.is_empty() {
            key_candidates.extend(
                self.unique_keys_for_relation(schema_introspection, table)
                    .await
                    .into_iter()
                    .map(|(_, columns)| columns),
            );
        }

        let mut total_deleted = 0u64;

        for row_values in &delete_data.rows {
            let key_values = key_candidates
                .iter()
                .find_map(|key_columns| {
                    self.key_values_for_row(
                        key_columns,
                        &delete_data.all_column_names,
                        row_values,
                        &[],
                    )
                    .transpose()
                })
                .transpose()?;

            let row_identifier = match key_values {
                Some(key_values) => RowIdentifier::PrimaryKey(key_values),
                None => {
                    self.build_full_row_identifier(&delete_data.all_column_names, row_values)?
                }
            };

            // Build DELETE statement
            let (where_clause, params) =
                self.build_where_clause(connection.as_ref(), &row_identifier)?;
            let qualified_table = Self::qualified_relation_name(connection.as_ref(), table);
            // Without a key the row is matched on all its columns, which cannot
            // tell duplicate rows apart; the driver narrows it to one row so a
            // single selected row deletes a single row.
            let scope = match row_identifier {
                RowIdentifier::FullRow(_) => {
                    connection.single_row_dml_scope(&qualified_table, &where_clause)
                }
                _ => SingleRowDmlScope::Unsupported,
            };
            let (where_clause, statement_suffix) = scope.apply(&where_clause);
            let sql = format!(
                "DELETE FROM {} WHERE {}{}",
                qualified_table, where_clause, statement_suffix
            );

            tracing::debug!("Delete SQL: {}", sql);

            let result = connection
                .execute(&sql, &params)
                .await
                .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

            total_deleted += result.affected_rows;
        }

        tracing::info!(
            table_name = %table.name,
            deleted_count = total_deleted,
            "Rows deleted successfully"
        );

        Ok(total_deleted)
    }

    /// Build a FullRow identifier from column names and values
    fn build_full_row_identifier(
        &self,
        column_names: &[String],
        row_values: &[Value],
    ) -> ServiceResult<RowIdentifier> {
        let row_values: Vec<(String, Value)> = column_names
            .iter()
            .zip(row_values.iter())
            .map(|(col, val)| (col.clone(), val.clone()))
            .collect();

        Ok(RowIdentifier::FullRow(row_values))
    }

    /// Build WHERE clause from row identifier
    fn build_where_clause(
        &self,
        connection: &dyn Connection,
        row_identifier: &RowIdentifier,
    ) -> ServiceResult<(String, Vec<Value>)> {
        match row_identifier {
            RowIdentifier::RowIndex(_) => Err(ServiceError::TableOperationFailed(
                "Row index-based operations not supported".to_string(),
            )),
            RowIdentifier::PrimaryKey(pk_values) => {
                let conditions: Vec<String> = pk_values
                    .iter()
                    .enumerate()
                    .map(|(idx, (col, _))| {
                        format!(
                            "{} = {}",
                            connection.quote_identifier(col),
                            connection.format_bind_placeholder(idx)
                        )
                    })
                    .collect();
                let params: Vec<Value> = pk_values.iter().map(|(_, v)| v.clone()).collect();
                Ok((conditions.join(" AND "), params))
            }
            RowIdentifier::FullRow(row_values) => {
                let mut next_param_index = 0usize;
                let conditions: Vec<String> = row_values
                    .iter()
                    .map(|(col, val)| {
                        if val == &Value::Null {
                            format!("{} IS NULL", connection.quote_identifier(col))
                        } else {
                            let placeholder = connection.format_bind_placeholder(next_param_index);
                            next_param_index += 1;
                            format!("{} = {}", connection.quote_identifier(col), placeholder)
                        }
                    })
                    .collect();
                let params: Vec<Value> = row_values
                    .iter()
                    .filter(|(_, val)| val != &Value::Null)
                    .map(|(_, v)| v.clone())
                    .collect();
                Ok((conditions.join(" AND "), params))
            }
        }
    }
}

impl Default for TableService {
    fn default() -> Self {
        Self::new(1000)
    }
}

/// The rows a fetch returned, and the plan they were fetched under.
#[derive(Debug)]
pub struct FetchOutcome {
    pub result: QueryResult,
    pub total: Option<KnownTotal>,
    /// The plan actually run; a last-page plan is resolved once the count is known.
    pub plan: FetchPlan,
}

/// Data required to update a cell
#[derive(Debug, Clone)]
pub struct CellUpdateData {
    /// Column name being updated
    pub column_name: String,
    /// New value preserving its decoded database type when possible.
    pub new_value: Option<Value>,
    /// All column names in the row (for building row identifier)
    pub all_column_names: Vec<String>,
    /// All values in the row (for building row identifier)
    pub all_row_values: Vec<Value>,
    /// Database column types (e.g. "varchar", "int4") for type-aware value parsing.
    /// When provided, prevents numeric-looking strings (like phone numbers) from
    /// being incorrectly treated as integers in SQL generation.
    pub all_column_types: Vec<String>,
}

/// Result of a single cell update.
#[derive(Debug, Clone, Default)]
pub struct CellUpdateOutcome {
    /// Rows the driver reported as affected.
    pub affected_rows: u64,
    /// Set when the write could not be confirmed because the driver does not
    /// report trustworthy row counts. The update is still treated as successful;
    /// callers should surface this as a warning rather than reverting the cell.
    pub unconfirmed_reason: Option<String>,
    /// The value the database actually holds after the write, when it could be
    /// read back. Differs from the submitted value whenever the database adjusts
    /// it — rounding a DECIMAL to scale, truncating a string, coercing a type.
    /// `None` means the read-back was skipped or failed; keep the client value.
    pub stored_value: Option<Value>,
}

/// Data required to insert a new row
#[derive(Debug, Clone)]
pub struct RowInsertData {
    /// Column names for the values being inserted
    pub column_names: Vec<String>,
    /// Values to insert, preserving decoded types when available.
    pub values: Vec<Option<Value>>,
    /// Database column types (ordered to match column_names) for type-aware value parsing.
    /// When provided, prevents numeric-looking strings from being treated as integers.
    pub column_types: Vec<String>,
}

/// Data required to delete rows
#[derive(Debug, Clone)]
pub struct RowDeleteData {
    /// All column names in the table
    pub all_column_names: Vec<String>,
    /// Rows to delete (each row contains all column values for identification)
    pub rows: Vec<Vec<Value>>,
}

/// Request for duplicating multiple tables on one connection.
#[derive(Debug, Clone)]
pub struct DuplicateTablesRequest {
    /// Source/target table mappings to duplicate.
    pub operations: Vec<DuplicateTableOperation>,
    /// When true, continue processing remaining tables after one failure.
    pub continue_on_error: bool,
}

/// One duplicate-table operation.
#[derive(Debug, Clone)]
pub struct DuplicateTableOperation {
    /// Table to copy.
    pub source: TableRef,
    /// Name of the copy, created next to the source.
    pub target_table_name: String,
}

/// Outcome for one duplicated table.
#[derive(Debug, Clone)]
pub struct DuplicateTableResult {
    /// Source table name.
    pub source_table_name: String,
    /// Generated target table name.
    pub target_table_name: String,
}

/// Aggregate result for a duplicate-tables operation.
#[derive(Debug, Clone, Default)]
pub struct DuplicateTablesOutcome {
    /// Tables duplicated successfully.
    pub duplicated_tables: Vec<DuplicateTableResult>,
    /// Per-table failures.
    pub errors: Vec<String>,
}

/// Request for deleting multiple tables on one connection.
#[derive(Debug, Clone)]
pub struct DeleteTablesRequest {
    /// Tables to delete.
    pub tables: Vec<TableRef>,
    /// When true, continue processing remaining tables after one failure.
    pub continue_on_error: bool,
}

/// Aggregate result for a delete-tables operation.
#[derive(Debug, Clone, Default)]
pub struct DeleteTablesOutcome {
    /// Table names deleted successfully.
    pub deleted_table_names: Vec<String>,
    /// Per-table failures.
    pub errors: Vec<String>,
}

/// Request for emptying (truncating) multiple tables on one connection.
#[derive(Debug, Clone)]
pub struct EmptyTablesRequest {
    /// Tables to empty.
    pub tables: Vec<TableRef>,
    /// When true, continue processing remaining tables after one failure.
    pub continue_on_error: bool,
}

/// Aggregate result for an empty-tables operation.
#[derive(Debug, Clone, Default)]
pub struct EmptyTablesOutcome {
    /// Table names emptied successfully.
    pub emptied_table_names: Vec<String>,
    /// Sum of affected rows reported by the driver.
    pub total_rows_deleted: u64,
    /// Per-table failures.
    pub errors: Vec<String>,
}

/// Request for renaming a table on one connection.
#[derive(Debug, Clone)]
pub struct RenameTableRequest {
    /// Table to rename.
    pub table: TableRef,
    /// New unqualified table name.
    pub target_table_name: String,
}

/// Request for generating SQL dumps for one or more tables.
#[derive(Debug, Clone)]
pub struct DumpTablesSqlRequest {
    /// Table names to include in the dump.
    pub table_names: Vec<String>,
    /// Include INSERT statements for table data in addition to CREATE TABLE.
    pub include_data: bool,
}

/// Aggregate result for a SQL-dump operation.
#[derive(Debug, Clone, Default)]
pub struct DumpTablesSqlOutcome {
    /// Concatenated SQL content for all requested tables.
    pub sql: String,
    /// Tables that were processed (even when individual sections contain warnings).
    pub processed_table_names: Vec<String>,
    /// Per-table errors captured while building the dump.
    pub errors: Vec<String>,
}

/// Request for loading distinct non-null values from one table column.
#[derive(Debug, Clone)]
pub struct LoadDistinctValuesRequest {
    /// Source table, as the viewer's open resolved it.
    pub table: TableRef,
    /// Column to project distinct values from.
    pub column_name: String,
    /// Maximum number of values to return.
    pub limit: u64,
}

/// Request for loading foreign-key candidate values from a referenced table.
#[derive(Debug, Clone)]
pub struct LoadForeignKeyValuesRequest {
    /// The table the key references.
    pub referenced_table: TableRef,
    /// Referenced key columns used for selected values.
    pub referenced_columns: Vec<String>,
    /// Optional search query used for LIKE filtering.
    pub query: Option<String>,
    /// Maximum number of rows to return.
    pub limit: usize,
}

/// One foreign-key selection option produced by the service.
#[derive(Debug, Clone, Default)]
pub struct ForeignKeyValueOption {
    /// Raw value that should be stored in the cell.
    pub value: String,
    /// User-facing label shown in the selection UI.
    pub label: String,
}

/// Service-layer outcome for loading foreign-key values.
#[derive(Debug, Clone, Default)]
pub struct LoadForeignKeyValuesOutcome {
    /// Candidate values ready for UI mapping.
    pub values: Vec<ForeignKeyValueOption>,
}

/// Service-layer outcome for loading distinct values.
#[derive(Debug, Clone, Default)]
pub struct LoadDistinctValuesOutcome {
    /// Distinct non-null values converted to display strings.
    pub values: Vec<String>,
}

/// One edited cell to include when generating SQL for pending table changes.
#[derive(Debug, Clone)]
pub struct ModifiedCellSqlChange {
    /// Zero-based row index in `all_rows`.
    pub row_index: usize,
    /// Zero-based column index in `column_names`.
    pub column_index: usize,
    /// New value that should be persisted for this cell.
    pub new_value: Value,
}

/// One pending cell edit to apply during commit.
#[derive(Debug, Clone)]
pub struct CommitCellChange {
    /// Zero-based row index in `all_rows`.
    pub row_index: usize,
    /// Zero-based column index in `column_meta`.
    pub column_index: usize,
    /// Original persisted value before editing.
    pub original_value: Value,
    /// New value that should be persisted.
    pub new_value: Value,
}

/// Failed modified-cell update details returned by commit orchestration.
#[derive(Debug, Clone)]
pub struct FailedModifiedCellCommit {
    /// Persisted row values used for the update WHERE clause.
    pub original_row_values: Vec<Value>,
    /// Zero-based edited column index.
    pub column_index: usize,
    /// Edited column name (for UI error summaries).
    pub column_name: String,
    /// Original value for rehydrating pending state in the UI.
    pub original_value: Value,
    /// New value for rehydrating pending state in the UI.
    pub new_value: Value,
    /// Underlying service/driver error text.
    pub error_message: String,
}

/// Failed insert details returned by commit orchestration.
#[derive(Debug, Clone)]
pub struct FailedNewRowCommit {
    /// One-based row number in the pending-new-rows batch.
    pub row_number: usize,
    /// New-row values that failed to insert.
    pub row_values: Vec<Value>,
    /// Underlying service/driver error text.
    pub error_message: String,
}

/// Request payload for committing pending table edits in one orchestration call.
#[derive(Debug, Clone)]
pub struct CommitTableChangesRequest {
    /// Target table, as the viewer's open resolved it.
    pub table: TableRef,
    /// Ordered table columns metadata.
    pub column_meta: Vec<zqlz_core::ColumnMeta>,
    /// Edited cells to apply as UPDATE statements.
    pub modified_cells: Vec<CommitCellChange>,
    /// Zero-based row indexes to delete from `all_rows`.
    pub deleted_row_indices: Vec<usize>,
    /// Newly-added rows to convert into INSERT statements.
    pub new_rows: Vec<Vec<Value>>,
    /// Snapshot of currently-loaded rows.
    pub all_rows: Vec<Vec<Value>>,
    /// Optional text placeholder representing auto-increment values in app state.
    pub auto_increment_placeholder: Option<String>,
}

/// Commit orchestration outcome for pending table edits.
#[derive(Debug, Clone, Default)]
pub struct CommitTableChangesOutcome {
    /// Count of successful update/delete/insert operations.
    pub successful_operations: usize,
    /// Modified cells that failed to persist.
    pub failed_modified_cells: Vec<FailedModifiedCellCommit>,
    /// Rows that failed to delete.
    pub failed_deleted_rows: Vec<Vec<Value>>,
    /// New rows that failed to insert.
    pub failed_new_rows: Vec<FailedNewRowCommit>,
}

/// Request payload for generating SQL statements that represent pending table
/// edits (updates, deletes, inserts).
#[derive(Debug, Clone)]
pub struct GenerateTableChangesSqlRequest {
    /// Target table, as the viewer's open resolved it.
    pub table: TableRef,
    /// Ordered table columns used by edited/deleted/new rows.
    pub column_names: Vec<String>,
    /// Edited cells to apply as UPDATE statements.
    pub modified_cells: Vec<ModifiedCellSqlChange>,
    /// Zero-based row indexes to delete from `all_rows`.
    pub deleted_row_indices: Vec<usize>,
    /// Newly-added rows to convert into INSERT statements.
    pub new_rows: Vec<Vec<Value>>,
    /// Snapshot of currently-loaded persisted rows.
    pub all_rows: Vec<Vec<Value>>,
}

impl TableService {
    /// Generate SQL for pending table edits without executing it.
    ///
    /// This keeps statement-shaping rules in `zqlz-services` while the app layer
    /// stays focused on UI actions such as copying generated SQL to clipboard.
    pub fn generate_table_changes_sql(
        &self,
        connection: &dyn Connection,
        request: GenerateTableChangesSqlRequest,
    ) -> String {
        let GenerateTableChangesSqlRequest {
            table,
            column_names,
            modified_cells,
            deleted_row_indices,
            new_rows,
            all_rows,
        } = request;
        let table_sql = connection.render_qualified_name(&connection.table_sql_name(&table));
        let quote = |column: &str| connection.quote_identifier(column);

        let mut sql_statements: Vec<String> = Vec::new();

        let mut row_updates: std::collections::HashMap<usize, Vec<&ModifiedCellSqlChange>> =
            std::collections::HashMap::new();
        for change in &modified_cells {
            row_updates
                .entry(change.row_index)
                .or_default()
                .push(change);
        }

        for (row_index, changes) in row_updates {
            let Some(row_values) = all_rows.get(row_index) else {
                continue;
            };

            let set_parts: Vec<String> = changes
                .iter()
                .filter_map(|change| {
                    column_names.get(change.column_index).map(|column_name| {
                        let value = Self::value_to_change_sql_literal(&change.new_value);
                        format!("{} = {}", quote(column_name), value)
                    })
                })
                .collect();

            if set_parts.is_empty() {
                continue;
            }

            let where_parts: Vec<String> = column_names
                .iter()
                .zip(row_values.iter())
                .map(|(column_name, value)| {
                    if value.is_null() {
                        format!("{} IS NULL", quote(column_name))
                    } else {
                        let sql_value = Self::value_to_change_sql_literal(value);
                        format!("{} = {}", quote(column_name), sql_value)
                    }
                })
                .collect();

            sql_statements.push(format!(
                "UPDATE {} SET {} WHERE {};",
                table_sql,
                set_parts.join(", "),
                where_parts.join(" AND ")
            ));
        }

        for row_index in deleted_row_indices {
            let Some(row_values) = all_rows.get(row_index) else {
                continue;
            };

            let where_parts: Vec<String> = column_names
                .iter()
                .zip(row_values.iter())
                .map(|(column_name, value)| {
                    if value.is_null() {
                        format!("{} IS NULL", quote(column_name))
                    } else {
                        let sql_value = Self::value_to_change_sql_literal(value);
                        format!("{} = {}", quote(column_name), sql_value)
                    }
                })
                .collect();

            sql_statements.push(format!(
                "DELETE FROM {} WHERE {};",
                table_sql,
                where_parts.join(" AND ")
            ));
        }

        for row_values in &new_rows {
            let column_list = column_names
                .iter()
                .map(|name| quote(name))
                .collect::<Vec<_>>()
                .join(", ");

            let values: Vec<String> = row_values
                .iter()
                .map(Self::value_to_change_sql_literal)
                .collect();

            sql_statements.push(format!(
                "INSERT INTO {} ({}) VALUES ({});",
                table_sql,
                column_list,
                values.join(", ")
            ));
        }

        sql_statements.join("\n")
    }

    /// Commit pending table changes (updates, deletes, inserts) while returning
    /// enough structured failure details for UI rehydration.
    pub async fn commit_table_changes(
        &self,
        connection: Arc<dyn Connection>,
        request: CommitTableChangesRequest,
    ) -> ServiceResult<CommitTableChangesOutcome> {
        let CommitTableChangesRequest {
            table,
            column_meta,
            modified_cells,
            deleted_row_indices,
            new_rows,
            all_rows,
            auto_increment_placeholder,
        } = request;

        let column_names: Vec<String> = column_meta
            .iter()
            .map(|column| column.name.clone())
            .collect();
        let column_types: Vec<String> = column_meta
            .iter()
            .map(|column| column.data_type.clone())
            .collect();

        let Some(original_row_count) = all_rows.len().checked_sub(new_rows.len()) else {
            return Err(ServiceError::TableOperationFailed(
                "Pending new-row count exceeds loaded rows; refresh and retry commit".to_string(),
            ));
        };

        let mut outcome = CommitTableChangesOutcome::default();

        let mut row_updates: std::collections::HashMap<usize, Vec<&CommitCellChange>> =
            std::collections::HashMap::new();
        for change in &modified_cells {
            row_updates
                .entry(change.row_index)
                .or_default()
                .push(change);
        }

        for (row_index, changes) in row_updates {
            if row_index >= original_row_count {
                tracing::warn!(
                    row_index,
                    original_row_count,
                    "Skipping modified-cell commit because row belongs to pending new rows"
                );
                continue;
            }

            let Some(row_values) = all_rows.get(row_index) else {
                continue;
            };

            let mut original_row_values = row_values.clone();
            for row_change in &changes {
                if let Some(cell) = original_row_values.get_mut(row_change.column_index) {
                    *cell = row_change.original_value.clone();
                }
            }

            for change in changes {
                let Some(column_name) = column_names.get(change.column_index).cloned() else {
                    continue;
                };

                let cell_update = CellUpdateData {
                    column_name: column_name.clone(),
                    new_value: Some(change.new_value.clone()).filter(|value| !value.is_null()),
                    all_column_names: column_names.clone(),
                    all_row_values: original_row_values.clone(),
                    all_column_types: column_types.clone(),
                };

                // The caller reloads the table once the batch completes, so a
                // per-cell read-back would only add round trips.
                match self
                    .update_cell_without_read_back(connection.clone(), &table, cell_update)
                    .await
                {
                    Ok(cell_outcome) => {
                        if let Some(reason) = cell_outcome.unconfirmed_reason {
                            tracing::warn!(
                                table_name = %table.name,
                                column = %column_name,
                                "Cell update could not be confirmed: {}",
                                reason
                            );
                        }
                        // The row now holds this value, and the next statement
                        // for the same row has to match it: a keyless row is
                        // matched on all its columns, and even a key can be the
                        // column that was just written.
                        if let Some(cell) = original_row_values.get_mut(change.column_index) {
                            *cell = change.new_value.clone();
                        }
                        outcome.successful_operations =
                            outcome.successful_operations.saturating_add(1);
                    }
                    Err(error) => {
                        outcome
                            .failed_modified_cells
                            .push(FailedModifiedCellCommit {
                                original_row_values: original_row_values.clone(),
                                column_index: change.column_index,
                                column_name,
                                original_value: change.original_value.clone(),
                                new_value: change.new_value.clone(),
                                error_message: error.to_string(),
                            });
                    }
                }
            }
        }

        if !deleted_row_indices.is_empty() {
            let rows_to_delete: Vec<Vec<Value>> = deleted_row_indices
                .iter()
                .filter_map(|row_index| all_rows.get(*row_index).cloned())
                .collect();

            if !rows_to_delete.is_empty() {
                let row_delete_data = RowDeleteData {
                    all_column_names: column_names.clone(),
                    rows: rows_to_delete.clone(),
                };

                match self
                    .delete_rows(connection.clone(), &table, row_delete_data)
                    .await
                {
                    Ok(deleted_count) => {
                        outcome.successful_operations = outcome
                            .successful_operations
                            .saturating_add(deleted_count as usize);
                    }
                    Err(_) => {
                        outcome.failed_deleted_rows = rows_to_delete;
                    }
                }
            }
        }

        for (new_row_index, row_values) in new_rows.iter().enumerate() {
            let insert_values: Vec<Option<Value>> = row_values
                .iter()
                .map(|value| {
                    if let (Some(placeholder), Value::String(text)) =
                        (auto_increment_placeholder.as_ref(), value)
                    {
                        if text == placeholder {
                            None
                        } else {
                            Some(value.clone())
                        }
                    } else {
                        Some(value.clone())
                    }
                })
                .collect();

            let row_insert_data = RowInsertData {
                column_names: column_names.clone(),
                values: insert_values,
                column_types: column_types.clone(),
            };

            match self
                .insert_row(connection.clone(), &table, row_insert_data)
                .await
            {
                Ok(()) => {
                    outcome.successful_operations = outcome.successful_operations.saturating_add(1);
                }
                Err(error) => {
                    outcome.failed_new_rows.push(FailedNewRowCommit {
                        row_number: new_row_index.saturating_add(1),
                        row_values: row_values.clone(),
                        error_message: error.to_string(),
                    });
                }
            }
        }

        Ok(outcome)
    }

    /// Duplicate multiple tables by creating `source + suffix` copies.
    ///
    /// The service owns SQL-construction and execution branching so UI layers can
    /// remain focused on confirmation dialogs and refresh orchestration.
    pub async fn duplicate_tables(
        &self,
        connection: Arc<dyn Connection>,
        request: DuplicateTablesRequest,
    ) -> DuplicateTablesOutcome {
        let DuplicateTablesRequest {
            operations,
            continue_on_error,
        } = request;

        let mut outcome = DuplicateTablesOutcome::default();

        for operation in operations {
            let DuplicateTableOperation {
                source,
                target_table_name,
            } = operation;
            let source_sql_name = source.sql_name(connection.as_ref());
            let target_sql_name = SqlObjectName {
                namespace: source_sql_name.namespace.clone(),
                name: target_table_name.clone(),
            };
            let source_table_name = source.name;
            let sql = match connection.duplicate_table_sql(&source_sql_name, &target_sql_name) {
                Ok(sql) => sql,
                Err(error) => {
                    let error_message = format!(
                        "'{}': failed to build duplicate SQL ({})",
                        source_table_name, error
                    );
                    if continue_on_error {
                        outcome.errors.push(error_message);
                        continue;
                    }
                    outcome.errors.push(error_message);
                    return outcome;
                }
            };

            match connection.execute(&sql, &[]).await {
                Ok(_) => {
                    outcome.duplicated_tables.push(DuplicateTableResult {
                        source_table_name,
                        target_table_name,
                    });
                }
                Err(error) => {
                    let error_message = format!("'{}': {}", source_table_name, error);
                    if continue_on_error {
                        outcome.errors.push(error_message);
                        continue;
                    }
                    outcome.errors.push(error_message);
                    return outcome;
                }
            }
        }

        outcome
    }

    /// Delete multiple tables.
    ///
    /// The service centralizes SQL-construction and best-effort/bail-fast error
    /// behavior so app handlers only coordinate UI state.
    pub async fn delete_tables(
        &self,
        connection: Arc<dyn Connection>,
        request: DeleteTablesRequest,
    ) -> DeleteTablesOutcome {
        let DeleteTablesRequest {
            tables,
            continue_on_error,
        } = request;

        let mut outcome = DeleteTablesOutcome::default();

        for table in tables {
            let table_name = table.name.clone();
            let sql = match connection.drop_table_sql(
                &table.sql_name(connection.as_ref()),
                DropTableOptions::default(),
            ) {
                Ok(sql) => sql,
                Err(error) => {
                    let error_message = format!(
                        "'{}': failed to build DROP TABLE SQL ({})",
                        table_name, error
                    );
                    if continue_on_error {
                        outcome.errors.push(error_message);
                        continue;
                    }
                    outcome.errors.push(error_message);
                    return outcome;
                }
            };

            match connection.execute(&sql, &[]).await {
                Ok(_) => outcome.deleted_table_names.push(table_name),
                Err(error) => {
                    let error_message = format!("'{}': {}", table_name, error);
                    if continue_on_error {
                        outcome.errors.push(error_message);
                        continue;
                    }
                    outcome.errors.push(error_message);
                    return outcome;
                }
            }
        }

        outcome
    }

    /// Empty multiple tables while preserving table structure.
    ///
    /// The service centralizes truncate SQL generation plus best-effort vs
    /// bail-fast behavior so app handlers can remain UI-orchestration only.
    pub async fn empty_tables(
        &self,
        connection: Arc<dyn Connection>,
        request: EmptyTablesRequest,
    ) -> EmptyTablesOutcome {
        let EmptyTablesRequest {
            tables,
            continue_on_error,
        } = request;

        let mut outcome = EmptyTablesOutcome::default();

        for table in tables {
            let table_name = table.name.clone();
            let sql = match connection.truncate_table_sql(&table.sql_name(connection.as_ref())) {
                Ok(sql) => sql,
                Err(error) => {
                    let error_message =
                        format!("'{}': failed to build truncate SQL ({})", table_name, error);
                    if continue_on_error {
                        outcome.errors.push(error_message);
                        continue;
                    }
                    outcome.errors.push(error_message);
                    return outcome;
                }
            };

            match connection.execute(&sql, &[]).await {
                Ok(result) => {
                    outcome.total_rows_deleted = outcome
                        .total_rows_deleted
                        .saturating_add(result.affected_rows);
                    outcome.emptied_table_names.push(table_name);
                }
                Err(error) => {
                    let error_message = format!("'{}': {}", table_name, error);
                    if continue_on_error {
                        outcome.errors.push(error_message);
                        continue;
                    }
                    outcome.errors.push(error_message);
                    return outcome;
                }
            }
        }

        outcome
    }

    /// Rename one table.
    ///
    /// The service owns SQL generation + execution so app handlers and windows
    /// can stay focused on validation, prompts, and refresh orchestration.
    pub async fn rename_table(
        &self,
        connection: Arc<dyn Connection>,
        request: RenameTableRequest,
    ) -> ServiceResult<()> {
        let RenameTableRequest {
            table,
            target_table_name,
        } = request;
        let source_table_name = &table.name;

        let sql = connection
            .rename_table_sql(&table.sql_name(connection.as_ref()), &target_table_name)
            .map_err(|error| {
                ServiceError::TableOperationFailed(format!(
                    "Failed to build rename SQL for table '{}': {}",
                    source_table_name, error
                ))
            })?;

        connection.execute(&sql, &[]).await.map_err(|error| {
            ServiceError::TableOperationFailed(format!(
                "Failed to rename table '{}' to '{}': {}",
                source_table_name, target_table_name, error
            ))
        })?;

        Ok(())
    }

    /// Generate SQL dump text for multiple tables.
    ///
    /// The service owns SQL generation details so app handlers can remain focused
    /// on clipboard and notification orchestration.
    pub async fn dump_tables_sql(
        &self,
        connection: Arc<dyn Connection>,
        request: DumpTablesSqlRequest,
    ) -> DumpTablesSqlOutcome {
        let DumpTablesSqlRequest {
            table_names,
            include_data,
        } = request;

        let mut outcome = DumpTablesSqlOutcome::default();
        if table_names.is_empty() {
            return outcome;
        }

        let mut dump_sections = Vec::with_capacity(table_names.len());
        for table_name in table_names {
            let mut table_sql_parts: Vec<String> = Vec::new();
            table_sql_parts.push(format!("-- Table: {}", table_name));

            if let Some(schema_introspection) = connection.as_schema_introspection() {
                let db_object = DatabaseObject {
                    object_type: ObjectType::Table,
                    schema: None,
                    name: table_name.clone(),
                    signature: None,
                };

                match schema_introspection.generate_ddl(&db_object).await {
                    Ok(create_sql) => {
                        table_sql_parts.push(create_sql);
                    }
                    Err(error) => {
                        outcome.errors.push(format!(
                            "'{}': failed to generate DDL ({})",
                            table_name, error
                        ));
                        table_sql_parts.push(format!("-- Error getting structure: {}", error));
                    }
                }
            } else {
                let error = "schema introspection not supported by this connection";
                outcome.errors.push(format!("'{}': {}", table_name, error));
                table_sql_parts.push(format!("-- Error getting structure: {}", error));
            }

            if include_data {
                let quoted_table_name = connection.quote_identifier(&table_name);
                let query = format!("SELECT * FROM {}", quoted_table_name);

                match connection.query(&query, &[]).await {
                    Ok(result) => {
                        if !result.rows.is_empty() {
                            table_sql_parts.push(String::new());
                            let column_names: Vec<String> = result
                                .columns
                                .iter()
                                .map(|column| column.name.clone())
                                .collect();
                            let quoted_columns = column_names
                                .iter()
                                .map(|column_name| connection.quote_identifier(column_name))
                                .collect::<Vec<_>>()
                                .join(", ");

                            for row in &result.rows {
                                let values = row
                                    .values
                                    .iter()
                                    .map(Self::value_to_sql_literal)
                                    .collect::<Vec<_>>()
                                    .join(", ");

                                table_sql_parts.push(format!(
                                    "INSERT INTO {} ({}) VALUES ({});",
                                    quoted_table_name, quoted_columns, values
                                ));
                            }
                        }
                    }
                    Err(error) => {
                        outcome
                            .errors
                            .push(format!("'{}': failed to dump data ({})", table_name, error));
                        table_sql_parts.push(format!("-- Error getting data: {}", error));
                    }
                }
            }

            outcome.processed_table_names.push(table_name);
            dump_sections.push(table_sql_parts.join("\n"));
        }

        outcome.sql = dump_sections.join("\n\n");
        outcome
    }

    /// Load distinct non-null values for one table column.
    ///
    /// This keeps SQL construction and execution in `zqlz-services` so app
    /// event handlers can remain focused on UI orchestration.
    pub async fn load_distinct_values(
        &self,
        connection: Arc<dyn Connection>,
        request: LoadDistinctValuesRequest,
    ) -> ServiceResult<LoadDistinctValuesOutcome> {
        let LoadDistinctValuesRequest {
            table,
            column_name,
            limit,
        } = request;
        let table_name = table.display_name();
        let table_object_name = connection.table_sql_name(&table);
        let escaped_column = connection.quote_identifier(&column_name);
        let where_clause = format!("{} IS NOT NULL", escaped_column);

        let sql = connection
            .select_distinct_rows_sql(
                &table_object_name,
                std::slice::from_ref(&column_name),
                Some(&where_clause),
                std::slice::from_ref(&column_name),
                limit,
            )
            .map_err(|error| {
                ServiceError::TableOperationFailed(format!(
                    "Failed to build distinct-values SQL for {}.{}: {}",
                    table_name, column_name, error
                ))
            })?;

        let result = connection.query(&sql, &[]).await.map_err(|error| {
            ServiceError::TableOperationFailed(format!(
                "Failed to load distinct values for {}.{}: {}",
                table_name, column_name, error
            ))
        })?;

        let values = result
            .rows
            .iter()
            .filter_map(|row| row.values.first().map(|value| value.to_string()))
            .collect();

        Ok(LoadDistinctValuesOutcome { values })
    }

    /// Load foreign-key candidate values from a referenced table.
    ///
    /// The service owns FK value SQL construction and label-column discovery so
    /// app handlers can remain focused on caching and UI updates.
    pub async fn load_foreign_key_values(
        &self,
        connection: Arc<dyn Connection>,
        request: LoadForeignKeyValuesRequest,
    ) -> ServiceResult<LoadForeignKeyValuesOutcome> {
        let LoadForeignKeyValuesRequest {
            referenced_table,
            referenced_columns,
            query,
            limit,
        } = request;

        let table = &referenced_table;
        let table_object_name = connection.table_sql_name(table);

        let label_column = self
            .best_fk_label_column(
                connection.as_ref(),
                table.schema.as_deref(),
                &table.name,
                &referenced_columns,
            )
            .await;

        let selected_columns = if referenced_columns.is_empty() {
            match &label_column {
                Some(label_column_name) => vec![label_column_name.clone()],
                None => vec!["id".to_string()],
            }
        } else {
            referenced_columns
        };

        let mut projected_columns = selected_columns.clone();
        if let Some(label_column_name) = &label_column {
            if !selected_columns
                .iter()
                .any(|column_name| column_name == label_column_name)
            {
                projected_columns.push(label_column_name.clone());
            }
        }

        let mut where_parts = Vec::new();
        if let Some(search_query) = query
            .as_deref()
            .map(str::trim)
            .filter(|search_query| !search_query.is_empty())
        {
            let escaped_like = escape_sql_like_literal(search_query);

            for column_name in &selected_columns {
                let escaped_column = connection.quote_identifier(column_name);
                let searchable_expr = connection.search_text_cast_expression(&escaped_column);
                where_parts.push(format!(
                    "LOWER({}) LIKE LOWER('%{}%') {}",
                    searchable_expr,
                    escaped_like,
                    sql_like_escape_clause()
                ));
            }

            if let Some(label_column_name) = &label_column {
                if !selected_columns
                    .iter()
                    .any(|column_name| column_name == label_column_name)
                {
                    let escaped_column = connection.quote_identifier(label_column_name);
                    let searchable_expr = connection.search_text_cast_expression(&escaped_column);
                    where_parts.push(format!(
                        "LOWER({}) LIKE LOWER('%{}%') {}",
                        searchable_expr,
                        escaped_like,
                        sql_like_escape_clause()
                    ));
                }
            }
        }

        let where_clause = if where_parts.is_empty() {
            None
        } else {
            Some(where_parts.join(" OR "))
        };

        let mut order_columns = selected_columns.clone();
        if let Some(label_column_name) = &label_column {
            if !order_columns
                .iter()
                .any(|column_name| column_name == label_column_name)
            {
                order_columns.push(label_column_name.clone());
            }
        }

        let sql = connection
            .select_distinct_rows_sql(
                &table_object_name,
                &projected_columns,
                where_clause.as_deref(),
                &order_columns,
                limit.clamp(1, 10) as u64,
            )
            .map_err(|error| {
                ServiceError::TableOperationFailed(format!(
                    "Failed to build foreign-key values SQL for {}: {}",
                    referenced_table.display_name(),
                    error
                ))
            })?;

        let result = connection.query(&sql, &[]).await.map_err(|error| {
            ServiceError::TableOperationFailed(format!(
                "Failed to load foreign-key values from {}: {}",
                referenced_table.display_name(),
                error
            ))
        })?;

        let values = result
            .rows
            .iter()
            .filter_map(|row| {
                let value = row.values.first()?.to_string();
                let label = if row.values.len() > 1 {
                    let extra = row
                        .values
                        .get(1)
                        .map(|value| value.to_string())
                        .unwrap_or_default();
                    format!("{} - {}", value, extra)
                } else {
                    value.clone()
                };
                Some(ForeignKeyValueOption { value, label })
            })
            .collect();

        Ok(LoadForeignKeyValuesOutcome { values })
    }

    async fn best_fk_label_column(
        &self,
        connection: &dyn Connection,
        schema_name: Option<&str>,
        table_name: &str,
        referenced_columns: &[String],
    ) -> Option<String> {
        let schema_introspection = connection.as_schema_introspection()?;
        let columns = schema_introspection
            .get_columns(schema_name, table_name)
            .await
            .ok()?;

        let preferred_names = ["name", "title", "label", "description", "email", "username"];
        for preferred_name in preferred_names {
            if let Some(column) = columns.iter().find(|column| {
                !referenced_columns
                    .iter()
                    .any(|fk_column| fk_column == &column.name)
                    && column.name.eq_ignore_ascii_case(preferred_name)
                    && is_string_like_type(&column.data_type)
            }) {
                return Some(column.name.clone());
            }
        }

        columns
            .iter()
            .find(|column| {
                !referenced_columns
                    .iter()
                    .any(|fk_column| fk_column == &column.name)
                    && is_string_like_type(&column.data_type)
            })
            .map(|column| column.name.clone())
    }

    fn value_to_sql_literal(value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::String(text) => format!("'{}'", text.replace('\'', "''")),
            Value::Bool(boolean_value) => {
                if *boolean_value {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            Value::Bytes(bytes) => {
                let hex_string: String = bytes.iter().map(|byte| format!("{:02x}", byte)).collect();
                format!("X'{}'", hex_string)
            }
            // Reaches the catch-all unquoted via Display, so a non-numeric
            // payload would escape into the statement.
            Value::Decimal(decimal_value) => {
                if Value::is_sql_numeric_literal(decimal_value) {
                    decimal_value.clone()
                } else {
                    format!("'{}'", decimal_value.replace('\'', "''"))
                }
            }
            _ => value.to_string(),
        }
    }

    fn value_to_change_sql_literal(value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(boolean_value) => {
                if *boolean_value {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            Value::Int8(integer_value) => integer_value.to_string(),
            Value::Int16(integer_value) => integer_value.to_string(),
            Value::Int32(integer_value) => integer_value.to_string(),
            Value::Int64(integer_value) => integer_value.to_string(),
            Value::Float32(float_value) => float_value.to_string(),
            Value::Float64(float_value) => float_value.to_string(),
            // Emitted unquoted to preserve precision; quote anything that is not
            // a real number so it cannot escape into the surrounding statement.
            Value::Decimal(decimal_value) => {
                if Value::is_sql_numeric_literal(decimal_value) {
                    decimal_value.clone()
                } else {
                    format!("'{}'", decimal_value.replace('\'', "''"))
                }
            }
            Value::String(text) => format!("'{}'", text.replace('\'', "''")),
            Value::Bytes(bytes) => {
                let hex_string: String = bytes.iter().map(|byte| format!("{:02x}", byte)).collect();
                format!("X'{}'", hex_string)
            }
            Value::Uuid(uuid_value) => format!("'{}'", uuid_value),
            Value::Date(date_value) => format!("'{}'", date_value),
            Value::Time(time_value) => format!("'{}'", time_value.format("%H:%M:%S%.f")),
            Value::DateTime(datetime_value) => {
                format!("'{}'", datetime_value.format("%Y-%m-%d %H:%M:%S%.f"))
            }
            Value::DateTimeUtc(datetime_value) => {
                format!("'{}'", datetime_value.format("%Y-%m-%d %H:%M:%S%.f UTC"))
            }
            Value::Json(json_value) => format!("'{}'", json_value.to_string().replace('\'', "''")),
            Value::Array(values) => {
                let rendered_values: Vec<String> = values
                    .iter()
                    .map(Self::value_to_change_sql_literal)
                    .collect();
                format!("ARRAY[{}]", rendered_values.join(", "))
            }
        }
    }
}

fn is_string_like_type(data_type: &str) -> bool {
    let normalized = data_type.to_ascii_lowercase();
    normalized.contains("char")
        || normalized.contains("text")
        || normalized.contains("name")
        || normalized.contains("json")
        || normalized.contains("uuid")
        || normalized.contains("enum")
}

fn escape_sql_like_literal(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
        .replace('\'', "''")
}

fn sql_like_escape_clause() -> &'static str {
    "ESCAPE '\\'"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::view_models::ColumnInfo;
    use zqlz_core::TableType;

    #[test]
    fn test_escape_identifier_postgresql() {
        assert_eq!(
            TableService::escape_identifier_for("users", "postgresql"),
            "\"users\""
        );
        assert_eq!(
            TableService::escape_identifier_for("user\"table", "postgresql"),
            "\"user\"\"table\""
        );
        assert_eq!(
            TableService::escape_identifier_for("my table", "postgresql"),
            "\"my table\""
        );
    }

    #[test]
    fn test_escape_identifier_mysql() {
        assert_eq!(
            TableService::escape_identifier_for("users", "mysql"),
            "`users`"
        );
        assert_eq!(
            TableService::escape_identifier_for("user`table", "mysql"),
            "`user``table`"
        );
        assert_eq!(
            TableService::escape_identifier_for("my table", "mysql"),
            "`my table`"
        );
    }

    #[test]
    fn test_escape_identifier_mssql() {
        assert_eq!(
            TableService::escape_identifier_for("users", "mssql"),
            "[users]"
        );
        assert_eq!(
            TableService::escape_identifier_for("user]table", "mssql"),
            "[user]]table]"
        );
        assert_eq!(
            TableService::escape_identifier_for("my table", "mssql"),
            "[my table]"
        );
    }

    /// Type dispatch used to match on the raw type name, so any declaration
    /// carrying a length or precision modifier fell through to the string
    /// catch-all instead of being recognised.
    #[test]
    fn parse_value_handles_types_with_modifiers() {
        let service = TableService::new(1000);

        assert_eq!(
            service.parse_value("1.5", Some("decimal(12,4)")).unwrap(),
            Value::Decimal("1.5".to_string())
        );
        assert_eq!(
            service.parse_value("42", Some("int(11)")).unwrap(),
            Value::Int32(42)
        );
        assert_eq!(
            service.parse_value("hello", Some("varchar(255)")).unwrap(),
            Value::String("hello".to_string())
        );
        assert_eq!(
            service.parse_value("1.5", Some("double(8,2)")).unwrap(),
            Value::Float64(1.5)
        );
    }

    /// MySQL spells BOOL as `tinyint(1)`; stripping the modifier would make it
    /// look like an ordinary small integer.
    #[test]
    fn parse_value_keeps_mysql_boolean_columns_boolean() {
        let service = TableService::new(1000);

        assert_eq!(
            service.parse_value("1", Some("tinyint(1)")).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            service.parse_value("0", Some("tinyint(1)")).unwrap(),
            Value::Bool(false)
        );
        // A wider tinyint is still an integer.
        assert_eq!(
            service.parse_value("7", Some("tinyint(4)")).unwrap(),
            Value::Int32(7)
        );
    }

    /// Decimal columns keep their digits instead of being routed through f64,
    /// which cannot represent values this wide.
    #[test]
    fn parse_value_preserves_wide_decimal_precision() {
        let service = TableService::new(1000);

        assert_eq!(
            service
                .parse_value("20000000000000000001.5", Some("numeric"))
                .unwrap(),
            Value::Decimal("20000000000000000001.5".to_string())
        );
    }

    /// A decimal column must never turn unparseable input into `Value::Decimal`,
    /// which drivers emit into SQL unquoted.
    #[test]
    fn parse_value_rejects_non_numeric_decimals() {
        let service = TableService::new(1000);

        assert_eq!(
            service
                .parse_value("0 WHERE 1=1 -- ", Some("decimal(12,4)"))
                .unwrap(),
            Value::String("0 WHERE 1=1 -- ".to_string())
        );
    }

    #[test]
    fn test_parse_value() {
        let service = TableService::new(1000);

        // Empty string becomes empty string value (not NULL)
        assert_eq!(
            service.parse_value("", None).unwrap(),
            Value::String(String::new())
        );
        // "null" and "NULL" become Value::Null
        assert_eq!(service.parse_value("null", None).unwrap(), Value::Null);
        assert_eq!(service.parse_value("NULL", None).unwrap(), Value::Null);
        assert_eq!(service.parse_value("123", None).unwrap(), Value::Int32(123));
        assert_eq!(
            service.parse_value("123.45", None).unwrap(),
            Value::Float64(123.45)
        );
        assert_eq!(
            service.parse_value("true", None).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            service.parse_value("false", None).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            service.parse_value("TRUE", None).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            service.parse_value("hello", None).unwrap(),
            Value::String("hello".to_string())
        );

        // Type-aware parsing: numeric-looking strings stay as strings when column is text
        assert_eq!(
            service.parse_value("1944", Some("varchar")).unwrap(),
            Value::String("1944".to_string())
        );
        assert_eq!(
            service.parse_value("410877354933", Some("text")).unwrap(),
            Value::String("410877354933".to_string())
        );

        // Type-aware parsing: numeric values when column is integer
        assert_eq!(
            service.parse_value("597", Some("int4")).unwrap(),
            Value::Int32(597)
        );
        assert_eq!(
            service.parse_value("248", Some("integer")).unwrap(),
            Value::Int32(248)
        );
        assert_eq!(
            service.parse_value("1111", Some("integer")).unwrap(),
            Value::Int32(1111)
        );
        assert_eq!(
            service.parse_value("[\"1\"]", Some("TextArray")).unwrap(),
            Value::Array(vec![Value::String("1".to_string())])
        );
        assert_eq!(
            service.parse_value("[1, 2]", Some("int4[]")).unwrap(),
            Value::Array(vec![Value::Int32(1), Value::Int32(2)])
        );
        assert_eq!(
            service
                .parse_value("{\"enabled\":true}", Some("Jsonb"))
                .unwrap(),
            Value::Json(serde_json::json!({ "enabled": true }))
        );
        assert!(service.parse_value("{bad json", Some("jsonb")).is_err());
        assert_eq!(
            service
                .parse_value("[\"read\",\"write\"]", Some("set"))
                .unwrap(),
            Value::Array(vec![
                Value::String("read".to_string()),
                Value::String("write".to_string())
            ])
        );
    }

    #[test]
    fn test_default_limit() {
        let service = TableService::new(500);
        assert_eq!(service.default_limit(), 500);

        let default_service = TableService::default();
        assert_eq!(default_service.default_limit(), 1000);
    }

    #[test]
    fn build_open_viewer_schema_viewer_metadata_shapes_foreign_keys_and_column_metadata() {
        let schema_load = OpenViewerSchemaLoad {
            table_details: TableDetails {
                name: "orders".to_string(),
                table_type: TableType::Table,
                columns: vec![ColumnInfo {
                    name: "order_id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    is_primary_key: true,
                    default_value: None,
                    max_length: None,
                    precision: None,
                    scale: None,
                    is_auto_increment: true,
                    comment: None,
                    enum_values: None,
                }],
                indexes: Vec::new(),
                foreign_keys: vec![zqlz_core::ForeignKeyInfo {
                    name: "fk_orders_customers".to_string(),
                    columns: vec!["customer_id".to_string()],
                    referenced_table: "customers".to_string(),
                    referenced_schema: Some("public".to_string()),
                    referenced_columns: vec!["id".to_string()],
                    on_update: zqlz_core::ForeignKeyAction::Cascade,
                    on_delete: zqlz_core::ForeignKeyAction::SetNull,
                    is_deferrable: true,
                    initially_deferred: true,
                }],
                constraints: Vec::new(),
                triggers: Vec::new(),
                primary_key_columns: vec!["order_id".to_string()],
                row_count: Some(42),
            },
            create_statement: Some("CREATE TABLE orders (...)".to_string()),
        };

        let metadata = build_open_viewer_schema_viewer_metadata(&schema_load);

        assert_eq!(metadata.schema_columns.len(), 1);
        assert_eq!(metadata.schema_columns[0].name, "order_id");
        assert_eq!(metadata.primary_key_columns, vec!["order_id".to_string()]);

        assert_eq!(metadata.foreign_keys_for_viewer.len(), 1);
        let foreign_key = &metadata.foreign_keys_for_viewer[0];
        assert_eq!(foreign_key.name, "fk_orders_customers");
        assert_eq!(foreign_key.columns, vec!["customer_id".to_string()]);
        assert_eq!(foreign_key.referenced_table, "customers");
        assert_eq!(foreign_key.referenced_schema.as_deref(), Some("public"));
        assert_eq!(foreign_key.referenced_columns, vec!["id".to_string()]);
        assert_eq!(foreign_key.on_update, zqlz_core::ForeignKeyAction::Cascade);
        assert_eq!(foreign_key.on_delete, zqlz_core::ForeignKeyAction::SetNull);
        assert!(!foreign_key.is_deferrable);
        assert!(!foreign_key.initially_deferred);
    }

    #[test]
    fn generate_table_changes_sql_builds_update_delete_and_insert_statements() {
        let service = TableService::new(1000);
        let connection =
            zqlz_drivers::sqlite::SqliteConnection::open(":memory:").expect("open sqlite");
        let sql = service.generate_table_changes_sql(
            &connection,
            GenerateTableChangesSqlRequest {
                table: TableRef::new(None, None, "users"),
                column_names: vec!["id".to_string(), "name".to_string()],
                modified_cells: vec![ModifiedCellSqlChange {
                    row_index: 0,
                    column_index: 1,
                    new_value: Value::String("Alice O'Connor".to_string()),
                }],
                deleted_row_indices: vec![1],
                new_rows: vec![vec![Value::Int32(3), Value::String("Carol".to_string())]],
                all_rows: vec![
                    vec![Value::Int32(1), Value::String("Alice".to_string())],
                    vec![Value::Int32(2), Value::Null],
                ],
            },
        );

        assert!(sql.contains("UPDATE \"users\" SET \"name\" = 'Alice O''Connor' WHERE \"id\" = 1 AND \"name\" = 'Alice';"));
        assert!(sql.contains("DELETE FROM \"users\" WHERE \"id\" = 2 AND \"name\" IS NULL;"));
        assert!(sql.contains("INSERT INTO \"users\" (\"id\", \"name\") VALUES (3, 'Carol');"));
    }

    #[test]
    fn generate_table_changes_sql_quotes_schema_and_name_separately() {
        let connection =
            zqlz_drivers::sqlite::SqliteConnection::open(":memory:").expect("open sqlite");
        let sql = TableService::new(1000).generate_table_changes_sql(
            &connection,
            GenerateTableChangesSqlRequest {
                table: TableRef::new(None, Some("aux".into()), "odd.name"),
                column_names: vec!["id".to_string()],
                modified_cells: Vec::new(),
                deleted_row_indices: Vec::new(),
                new_rows: vec![vec![Value::Int32(1)]],
                all_rows: Vec::new(),
            },
        );

        assert_eq!(sql, "INSERT INTO \"aux\".\"odd.name\" (\"id\") VALUES (1);");
    }

    #[test]
    fn generate_table_changes_sql_ignores_invalid_row_or_column_indexes() {
        let service = TableService::new(1000);
        let connection =
            zqlz_drivers::sqlite::SqliteConnection::open(":memory:").expect("open sqlite");
        let sql = service.generate_table_changes_sql(
            &connection,
            GenerateTableChangesSqlRequest {
                table: TableRef::new(None, None, "users"),
                column_names: vec!["id".to_string()],
                modified_cells: vec![
                    ModifiedCellSqlChange {
                        row_index: 4,
                        column_index: 0,
                        new_value: Value::Int32(9),
                    },
                    ModifiedCellSqlChange {
                        row_index: 0,
                        column_index: 3,
                        new_value: Value::Int32(7),
                    },
                ],
                deleted_row_indices: vec![9],
                new_rows: Vec::new(),
                all_rows: vec![vec![Value::Int32(1)]],
            },
        );

        assert!(sql.is_empty());
    }

    #[test]
    fn raw_driver_table_name_leaves_main_and_dotted_names_alone() {
        let table =
            |schema: Option<&str>, name: &str| TableRef::new(None, schema.map(Into::into), name);
        assert_eq!(
            raw_driver_table_name(&table(Some("main"), "public.activity_communications")),
            "public.activity_communications"
        );
        assert_eq!(
            raw_driver_table_name(&table(Some("public"), "users")),
            "public.users"
        );
        assert_eq!(raw_driver_table_name(&table(None, "users")), "users");
    }

    #[test]
    fn escape_sql_like_literal_escapes_wildcards_and_quotes() {
        let escaped = escape_sql_like_literal("50%_off\\today's");
        assert_eq!(escaped, "50\\%\\_off\\\\today''s");
    }

    #[test]
    fn sql_like_escape_clause_uses_single_backslash_escape() {
        assert_eq!(sql_like_escape_clause(), "ESCAPE '\\'");
        assert_ne!(sql_like_escape_clause(), "ESCAPE '\\\\'");
    }
}
