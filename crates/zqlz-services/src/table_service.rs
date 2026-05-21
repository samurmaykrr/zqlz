//! Table operations service
//!
//! Provides table browsing, data retrieval, and cell editing operations.

use std::sync::Arc;
use uuid::Uuid;
use zqlz_core::{
    CellUpdateRequest, Connection, DatabaseObject, DriverCategory, DropTableOptions, ObjectType,
    QueryResult, RowIdentifier, SqlObjectName, Value,
};
pub use zqlz_table_workflows::{
    decide_delete_tables, decide_design_tables, decide_duplicate_tables, decide_empty_tables,
    DeleteTablesDecision, DeleteTablesDecisionRequest, DesignTablesDecision,
    DesignTablesDecisionRequest, DuplicateTablesDecision, DuplicateTablesDecisionRequest,
    EmptyTablesDecision, EmptyTablesDecisionRequest, OpenTableViewerCountDecision,
    OpenTableViewerCountDecisionRequest, OpenTablesDecision, OpenTablesDecisionRequest,
    TableWorkflowError,
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

/// Input for open-viewer initial load orchestration.
///
/// This keeps open-viewer data/schema loading policy in the service layer while
/// preserving app-layer control over UI rendering and state updates.
pub struct OpenViewerInitialLoadRequest {
    pub connection: Arc<dyn Connection>,
    pub connection_id: Uuid,
    pub table_name: String,
    pub database_name: Option<String>,
    pub is_view: bool,
    pub limit: Option<usize>,
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
}

/// Service-layer outcome for open-viewer initial load.
pub struct OpenViewerInitialLoadOutcome {
    pub browse_result: ServiceResult<QueryResult>,
    pub schema_result: ServiceResult<OpenViewerSchemaLoad>,
    pub used_schema_only_fallback: bool,
    pub schema_qualifier: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedRelationName {
    table_name: String,
    schema: Option<String>,
}

impl ResolvedRelationName {
    fn new(table_name: impl Into<String>, schema: Option<String>) -> Self {
        Self {
            table_name: table_name.into(),
            schema: schema
                .map(|schema| schema.trim().to_string())
                .filter(|schema| !schema.is_empty()),
        }
    }

    fn schema_ref(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    fn to_sql_object_name(&self) -> SqlObjectName {
        match self.schema_ref() {
            Some(schema_name) => SqlObjectName::with_namespace(schema_name, &self.table_name),
            None => SqlObjectName::new(&self.table_name),
        }
    }

    fn to_raw_driver_table_name(&self) -> String {
        match self.schema_ref() {
            Some("main") => self.table_name.clone(),
            Some(schema_name) => format!("{}.{}", schema_name, self.table_name),
            None => self.table_name.clone(),
        }
    }
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

    OpenViewerSchemaViewerMetadata {
        foreign_keys_for_viewer,
        schema_columns: schema_load.table_details.columns.clone(),
        primary_key_columns: schema_load.table_details.primary_key_columns.clone(),
    }
}

/// Decide whether open-viewer should trigger a background row-count query.
///
/// Keeping this adapter in `zqlz-services` ensures app handlers do not reach
/// into workflow crates directly for table-viewer orchestration decisions.
pub fn decide_open_viewer_count_workflow(
    request: OpenTableViewerCountDecisionRequest,
) -> OpenTableViewerCountDecision {
    zqlz_table_workflows::decide_open_table_viewer_count(request)
}

/// Decide how many table-viewer open requests should be produced from a batch
/// table-open action.
///
/// The UI layer should rely on this service adapter rather than calling
/// workflow crates directly so table-open decision policy remains crate-owned
/// and can evolve without widening app-side dependencies.
pub fn decide_open_tables_workflow(
    request: OpenTablesDecisionRequest,
) -> Result<OpenTablesDecision, TableWorkflowError> {
    zqlz_table_workflows::decide_open_tables(request)
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

    /// Load open-viewer browse and schema payloads concurrently.
    ///
    /// This method intentionally returns both result channels so callers can
    /// preserve partial-delivery behavior (rows first, schema second) without
    /// re-implementing driver/fallback branching in the app shell.
    pub async fn load_open_viewer_initial_data(
        &self,
        schema_service: Arc<SchemaService>,
        request: OpenViewerInitialLoadRequest,
    ) -> OpenViewerInitialLoadOutcome {
        let OpenViewerInitialLoadRequest {
            connection,
            connection_id,
            table_name,
            database_name,
            is_view,
            limit,
        } = request;

        let schema_qualifier = zqlz_core::resolve_schema_qualifier_for_connection(
            connection.as_ref(),
            database_name.as_deref(),
        );
        let resolved_relation = self
            .resolve_relation_reference(
                connection.clone(),
                &table_name,
                schema_qualifier.as_deref(),
                is_view,
            )
            .await;
        let schema_ref = resolved_relation.schema_ref();

        let (mut browse_result, schema_result) = tokio::join!(
            async {
                self.browse_table(
                    connection.clone(),
                    &resolved_relation.table_name,
                    schema_ref,
                    limit,
                    None,
                )
                .await
            },
            async {
                let table_details = schema_service
                    .get_table_details(
                        connection.clone(),
                        connection_id,
                        &resolved_relation.table_name,
                        schema_ref,
                    )
                    .await?;
                let create_statement = schema_service
                    .get_or_generate_ddl(
                        &connection,
                        connection_id,
                        &resolved_relation.table_name,
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
        );

        let mut used_schema_only_fallback = false;
        if let (Err(browse_error), Ok(schema_payload)) = (&browse_result, &schema_result) {
            if should_use_schema_only_table_browse_fallback(
                connection.as_ref(),
                browse_error,
                &schema_payload.table_details,
            ) {
                browse_result = Ok(build_schema_only_query_result(
                    &schema_payload.table_details,
                ));
                used_schema_only_fallback = true;
            }
        }

        OpenViewerInitialLoadOutcome {
            browse_result,
            schema_result,
            used_schema_only_fallback,
            schema_qualifier: resolved_relation.schema.or(schema_qualifier),
        }
    }

    async fn resolve_relation_reference(
        &self,
        connection: Arc<dyn Connection>,
        table_name: &str,
        schema: Option<&str>,
        is_view: bool,
    ) -> ResolvedRelationName {
        let Some(schema_introspection) = connection.as_schema_introspection() else {
            let (embedded_schema, embedded_name) = resolve_table_reference(table_name, schema);
            return ResolvedRelationName::new(embedded_name, embedded_schema);
        };
        let known_schema_names = schema_introspection
            .list_schemas()
            .await
            .map(|schemas| {
                schemas
                    .into_iter()
                    .map(|schema| schema.name)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        if !is_view {
            if let Ok(tables) = schema_introspection.list_tables(schema).await {
                if let Some(table) = tables.iter().find(|table| table.name == table_name) {
                    return ResolvedRelationName::new(table.name.clone(), table.schema.clone());
                }

                let (embedded_schema, embedded_name) = resolve_table_reference(table_name, schema);
                if is_known_schema_name(embedded_schema.as_deref(), &known_schema_names) {
                    return ResolvedRelationName::new(embedded_name, embedded_schema);
                }

                if let Some(table) = tables
                    .into_iter()
                    .find(|table| relation_info_matches_name(&table.name, &embedded_name))
                {
                    return ResolvedRelationName::new(
                        relation_display_name(&table.name, &embedded_name),
                        table.schema,
                    );
                }
            }
        }

        if let Ok(views) = schema_introspection.list_views(schema).await {
            if let Some(view) = views.iter().find(|view| view.name == table_name) {
                return ResolvedRelationName::new(view.name.clone(), view.schema.clone());
            }

            let (embedded_schema, embedded_name) = resolve_table_reference(table_name, schema);
            if is_known_schema_name(embedded_schema.as_deref(), &known_schema_names) {
                return ResolvedRelationName::new(embedded_name, embedded_schema);
            }

            if let Some(view) = views
                .into_iter()
                .find(|view| relation_info_matches_name(&view.name, &embedded_name))
            {
                return ResolvedRelationName::new(
                    relation_display_name(&view.name, &embedded_name),
                    view.schema,
                );
            }
        }

        if connection.supports_materialized_views() {
            if let Ok(views) = schema_introspection.list_materialized_views(schema).await {
                if let Some(view) = views.iter().find(|view| view.name == table_name) {
                    return ResolvedRelationName::new(view.name.clone(), view.schema.clone());
                }

                let (embedded_schema, embedded_name) = resolve_table_reference(table_name, schema);
                if is_known_schema_name(embedded_schema.as_deref(), &known_schema_names) {
                    return ResolvedRelationName::new(embedded_name, embedded_schema);
                }

                if let Some(view) = views
                    .into_iter()
                    .find(|view| relation_info_matches_name(&view.name, &embedded_name))
                {
                    return ResolvedRelationName::new(
                        relation_display_name(&view.name, &embedded_name),
                        view.schema,
                    );
                }
            }
        }

        let (embedded_schema, embedded_name) = resolve_table_reference(table_name, schema);
        ResolvedRelationName::new(embedded_name, embedded_schema)
    }

    /// Create a new table service
    ///
    /// # Arguments
    ///
    /// * `default_limit` - Default number of rows to return when browsing tables
    pub fn new(default_limit: usize) -> Self {
        Self { default_limit }
    }

    /// Browse table data with filters and sorting
    ///
    /// # Arguments
    ///
    /// * `connection` - Database connection
    /// * `table_name` - Name of the table to browse
    /// * `where_clauses` - WHERE clause fragments (will be joined with AND)
    /// * `order_by_clauses` - ORDER BY clause fragments (already formatted as "column ASC/DESC")
    /// * `visible_columns` - Which columns to select (empty means all)
    /// * `limit` - Optional limit (uses default if not specified)
    /// * `offset` - Optional offset for pagination
    /// * `cached_total` - When provided, skip the COUNT(*) query and reuse this value.
    ///   Callers should pass `None` when filters/search/sort change (so the count
    ///   is recalculated) and `Some(count)` for simple page navigations where
    ///   only the offset changed.
    ///
    /// # Returns
    ///
    /// A `QueryResult` containing the filtered and sorted table data
    #[tracing::instrument(skip(self, connection))]
    pub async fn browse_table_with_filters(
        &self,
        connection: Arc<dyn Connection>,
        request: BrowseTableWithFiltersRequest<'_>,
    ) -> ServiceResult<QueryResult> {
        Self::ensure_relational_connection(connection.as_ref())?;
        let BrowseTableWithFiltersRequest {
            table_name,
            schema,
            where_clauses,
            order_by_clauses,
            visible_columns,
            limit,
            offset,
            cached_total,
        } = request;
        let limit = limit.unwrap_or(self.default_limit);
        let offset = offset.unwrap_or(0);
        let relation = self
            .resolve_relation_reference(connection.clone(), table_name, schema, false)
            .await;
        let qualified = Self::qualified_relation_name(connection.as_ref(), &relation);

        // Build WHERE clause (needed for both COUNT and SELECT)
        let where_clause = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        // Build column selection
        let columns = if visible_columns.is_empty() {
            "*".to_string()
        } else {
            visible_columns
                .iter()
                .map(|c| connection.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Build ORDER BY clause
        let order_by_clause = if order_by_clauses.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {}", order_by_clauses.join(", "))
        };

        // Build safe SQL
        let base_sql = format!(
            "SELECT {} FROM {}{}{}",
            columns, qualified, where_clause, order_by_clause
        );
        let data_sql = connection.paginated_select_sql(&base_sql, limit as u64, offset as u64);

        tracing::debug!("Browsing table with filters, SQL: {}", data_sql);

        // Decide the counting strategy:
        // - If a cached total is provided, reuse it (simple page navigation).
        // - If the driver supports fast COUNT(*) (sqlite, duckdb), run exact count.
        // - Otherwise (mysql, postgresql, etc.), run an estimated count from metadata
        //   concurrently with the data query.
        //
        // Note: when filters are active and the driver is slow-count, we skip
        // counting entirely since metadata estimates don't account for WHERE clauses.
        let has_filters = !where_clauses.is_empty();

        let (mut result, total_rows, is_estimated) = if cached_total.is_some() {
            tracing::debug!(
                "Using cached total_rows={}, skipping COUNT(*)",
                cached_total.unwrap_or(0)
            );
            let data_result = connection.query(&data_sql, &[]).await;
            let result =
                data_result.map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;
            (result, cached_total, false)
        } else if connection.supports_fast_exact_count() {
            // Run the data query and COUNT(*) concurrently so the user doesn't wait
            // for a potentially slow full-table count before seeing rows.
            let count_base_sql = format!("SELECT COUNT(*) FROM {}{}", qualified, where_clause);
            let count_sql = connection.paginated_select_sql(&count_base_sql, 1, 0);
            tracing::debug!("Counting rows with SQL: {}", count_sql);

            let count_conn = connection.clone();
            let data_future = connection.query(&data_sql, &[]);
            let count_future = count_conn.query(&count_sql, &[]);

            let (data_result, count_result) = tokio::join!(data_future, count_future);

            let result =
                data_result.map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

            // Extract total count — non-critical, so log and continue if it fails
            let total_rows = match count_result {
                Ok(count_res) => {
                    if !count_res.rows.is_empty() && !count_res.rows[0].values.is_empty() {
                        count_res.rows[0].values[0].as_i64().map(|i| i as u64)
                    } else {
                        None
                    }
                }
                Err(e) => {
                    tracing::warn!("COUNT(*) query failed, pagination total unavailable: {}", e);
                    None
                }
            };

            (result, total_rows, false)
        } else if has_filters {
            // Metadata estimates don't reflect WHERE filters, so skip counting
            tracing::debug!(
                "Active filters present, skipping metadata estimate because estimates do not apply"
            );
            let data_result = connection.query(&data_sql, &[]).await;
            let result =
                data_result.map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;
            (result, None, false)
        } else {
            let estimate_conn = connection.clone();
            let data_future = connection.query(&data_sql, &[]);
            let estimate_future = self.estimate_row_count(estimate_conn, table_name, schema);

            let (data_result, estimate_result) = tokio::join!(data_future, estimate_future);

            let result =
                data_result.map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

            let (total_rows, is_estimated) = match estimate_result {
                Ok(Some((total, estimated))) => (Some(total), estimated),
                Ok(None) => (None, false),
                Err(error) => {
                    tracing::warn!(
                        "Estimated row count query failed, pagination total unavailable: {}",
                        error
                    );
                    (None, false)
                }
            };

            (result, total_rows, is_estimated)
        };

        result.total_rows = total_rows;
        result.is_estimated_total = is_estimated;

        tracing::info!(
            table_name = %table_name,
            rows = result.rows.len(),
            total = ?total_rows,
            is_estimated = is_estimated,
            filters = where_clauses.len(),
            sorts = order_by_clauses.len(),
            "Table data loaded with filters"
        );

        Ok(result)
    }

    /// Run a COUNT(*) query against a table, respecting current filters.
    ///
    /// Used for on-demand counting when the user explicitly requests the last
    /// page on drivers where COUNT(*) is too expensive to run automatically.
    #[tracing::instrument(skip(self, connection))]
    pub async fn count_rows(
        &self,
        connection: Arc<dyn Connection>,
        table_name: &str,
        schema: Option<&str>,
        where_clauses: Vec<String>,
    ) -> ServiceResult<u64> {
        Self::ensure_relational_connection(connection.as_ref())?;
        let relation = self
            .resolve_relation_reference(connection.clone(), table_name, schema, false)
            .await;
        let qualified = Self::qualified_relation_name(connection.as_ref(), &relation);

        let where_clause = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let count_base_sql = format!("SELECT COUNT(*) FROM {}{}", qualified, where_clause);
        let count_sql = connection.paginated_select_sql(&count_base_sql, 1, 0);
        tracing::debug!("On-demand row count, SQL: {}", count_sql);

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
                ServiceError::TableOperationFailed("COUNT(*) query returned no result".to_string())
            })?;

        tracing::info!(table_name = %table_name, total = total, "On-demand row count complete");
        Ok(total)
    }

    /// Navigate to the last page efficiently by running COUNT(\*) and a
    /// reversed-ORDER-BY data fetch **concurrently**.
    ///
    /// Instead of the naive approach (COUNT → compute offset → SELECT with
    /// high OFFSET), this method issues both queries in parallel:
    ///   - `SELECT COUNT(*) FROM table WHERE ...`
    ///   - `SELECT ... FROM table WHERE ... ORDER BY <reversed> LIMIT N`
    ///
    /// The data query uses a reversed sort (PK DESC when no user sort is
    /// active, or flipped ASC↔DESC for user sorts) so it can fetch the
    /// tail rows via an index scan without any OFFSET. The returned rows
    /// are then reversed client-side to restore the expected display order.
    ///
    /// On a 54M-row MySQL table this drops "go to last page" from ~13s
    /// (sequential COUNT + high-OFFSET scan) to ~3.5s (bounded by COUNT).
    #[tracing::instrument(skip(self, connection))]
    pub async fn browse_last_page(
        &self,
        connection: Arc<dyn Connection>,
        request: BrowseLastPageRequest<'_>,
    ) -> ServiceResult<QueryResult> {
        let BrowseLastPageRequest {
            table_name,
            schema,
            where_clauses,
            order_by_clauses,
            visible_columns,
            limit,
            pk_columns,
        } = request;
        Self::ensure_relational_connection(connection.as_ref())?;
        let relation = self
            .resolve_relation_reference(connection.clone(), table_name, schema, false)
            .await;
        let qualified = Self::qualified_relation_name(connection.as_ref(), &relation);

        let where_clause = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let columns = if visible_columns.is_empty() {
            "*".to_string()
        } else {
            visible_columns
                .iter()
                .map(|c| connection.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Build reversed ORDER BY: flip user sorts if present, otherwise
        // use primary key DESC so the engine can satisfy the query via an
        // index scan instead of scanning past millions of rows for a high OFFSET.
        let reversed_order = if order_by_clauses.is_empty() {
            pk_columns
                .iter()
                .map(|col| format!("{} DESC", connection.quote_identifier(col)))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            order_by_clauses
                .iter()
                .map(|clause| Self::reverse_order_by_clause(clause))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let base_sql = format!(
            "SELECT {} FROM {}{} ORDER BY {}",
            columns, qualified, where_clause, reversed_order
        );
        let data_sql = connection.limited_select_sql(&base_sql, limit as u64);

        let count_base_sql = format!("SELECT COUNT(*) FROM {}{}", qualified, where_clause);
        let count_sql = connection.paginated_select_sql(&count_base_sql, 1, 0);

        tracing::debug!("Last-page data SQL: {}", data_sql);
        tracing::debug!("Last-page count SQL: {}", count_sql);

        // Run data query first, then count. Using tokio::join! on the same
        // async-mutex-guarded connection causes waker contention on GPUI's
        // executor, producing spurious "connection closed" errors.
        let mut result = connection
            .query(&data_sql, &[])
            .await
            .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

        let total = match connection.query(&count_sql, &[]).await {
            Ok(count_res) => count_res
                .rows
                .first()
                .and_then(|row| row.values.first())
                .and_then(|v| v.as_i64())
                .map(|i| i as u64)
                .ok_or_else(|| {
                    ServiceError::TableOperationFailed(
                        "COUNT(*) query returned no result".to_string(),
                    )
                })?,
            Err(e) => {
                return Err(ServiceError::TableOperationFailed(format!(
                    "COUNT(*) query failed: {}",
                    e
                )));
            }
        };

        // Restore natural display order (the query fetched rows in reverse)
        result.rows.reverse();
        result.total_rows = Some(total);

        tracing::info!(
            table_name = %table_name,
            rows = result.rows.len(),
            total = total,
            "Last-page data loaded"
        );

        Ok(result)
    }

    /// Fetch a page near the end of a large table using a reversed ORDER BY
    /// with a small OFFSET, avoiding the expensive high-OFFSET full-table scan.
    ///
    /// When navigating to page N of a 54M-row table where the offset would be
    /// e.g. 54_302_000, MySQL has to scan past all those rows. Instead, this
    /// method computes the "distance from the end" and queries with a reversed
    /// sort order plus a small offset from the tail.
    ///
    /// Example: 54_305_000 total rows, limit 1000, page 54303 (offset 54_302_000):
    ///   - Forward:  `ORDER BY id ASC LIMIT 1000 OFFSET 54302000` (scans 54M rows)
    ///   - Reversed: `ORDER BY id DESC LIMIT 1000 OFFSET 2000` (scans 3000 rows)
    ///
    /// The returned rows are reversed client-side to restore display order.
    #[tracing::instrument(skip(self, connection))]
    pub async fn browse_near_end_page(
        &self,
        connection: Arc<dyn Connection>,
        request: BrowseNearEndPageRequest<'_>,
    ) -> ServiceResult<QueryResult> {
        let BrowseNearEndPageRequest {
            table_name,
            schema,
            where_clauses,
            order_by_clauses,
            visible_columns,
            limit,
            offset,
            total_rows,
            pk_columns,
        } = request;
        Self::ensure_relational_connection(connection.as_ref())?;
        let relation = self
            .resolve_relation_reference(connection.clone(), table_name, schema, false)
            .await;
        let qualified = Self::qualified_relation_name(connection.as_ref(), &relation);

        let where_clause = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let columns = if visible_columns.is_empty() {
            "*".to_string()
        } else {
            visible_columns
                .iter()
                .map(|c| connection.quote_identifier(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Compute how far this page is from the end of the table.
        // For the very last page, reverse_offset is 0 (no rows to skip from tail).
        // For the second-to-last page, it's `records_per_page`, etc.
        let total = total_rows as usize;
        let reverse_offset = total.saturating_sub(offset + limit);

        // The reversed query may fetch fewer rows than `limit` on the last page.
        // Compute the actual number of rows to request: for partial last pages
        // this equals `total - offset`, otherwise `limit`.
        let reverse_limit = limit.min(total.saturating_sub(offset));

        let reversed_order = if order_by_clauses.is_empty() {
            pk_columns
                .iter()
                .map(|col| format!("{} DESC", connection.quote_identifier(col)))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            order_by_clauses
                .iter()
                .map(|clause| Self::reverse_order_by_clause(clause))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let base_sql = format!(
            "SELECT {} FROM {}{} ORDER BY {}",
            columns, qualified, where_clause, reversed_order
        );
        let data_sql =
            connection.paginated_select_sql(&base_sql, reverse_limit as u64, reverse_offset as u64);

        tracing::debug!(
            "Near-end page: original offset={}, reversed to OFFSET {} LIMIT {} (total={})",
            offset,
            reverse_offset,
            reverse_limit,
            total
        );
        tracing::debug!("Near-end page SQL: {}", data_sql);

        let mut result = connection
            .query(&data_sql, &[])
            .await
            .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

        // Restore natural display order (the query fetched rows in reverse)
        result.rows.reverse();
        result.total_rows = Some(total_rows);

        tracing::info!(
            table_name = %table_name,
            rows = result.rows.len(),
            total = total_rows,
            reverse_offset = reverse_offset,
            "Near-end page data loaded with reversed query"
        );

        Ok(result)
    }

    /// Flip the direction of an `ORDER BY` fragment.
    ///
    /// Handles optional `NULLS FIRST` / `NULLS LAST` suffixes, preserving
    /// them while reversing only the ASC/DESC direction.
    ///
    /// Input examples:
    ///   `"col" ASC` → `"col" DESC`
    ///   `"col" DESC NULLS FIRST` → `"col" ASC NULLS FIRST`
    ///   `"col"` (implicit ASC) → `"col" DESC`
    fn reverse_order_by_clause(clause: &str) -> String {
        let trimmed = clause.trim();

        // Peel off any trailing NULLS FIRST / NULLS LAST qualifier so we
        // can examine just the direction keyword.
        let (core, nulls_suffix) = if let Some(prefix) = trimmed.strip_suffix(" NULLS FIRST") {
            (prefix, " NULLS FIRST")
        } else if let Some(prefix) = trimmed.strip_suffix(" NULLS LAST") {
            (prefix, " NULLS LAST")
        } else if let Some(prefix) = trimmed.strip_suffix(" nulls first") {
            (prefix, " NULLS FIRST")
        } else if let Some(prefix) = trimmed.strip_suffix(" nulls last") {
            (prefix, " NULLS LAST")
        } else {
            (trimmed, "")
        };

        let reversed_core = if let Some(prefix) = core.strip_suffix(" ASC") {
            format!("{} DESC", prefix)
        } else if let Some(prefix) = core.strip_suffix(" asc") {
            format!("{} DESC", prefix)
        } else if let Some(prefix) = core.strip_suffix(" DESC") {
            format!("{} ASC", prefix)
        } else if let Some(prefix) = core.strip_suffix(" desc") {
            format!("{} ASC", prefix)
        } else {
            // No explicit direction = implicit ASC → reverse to DESC
            format!("{} DESC", core)
        };

        format!("{}{}", reversed_core, nulls_suffix)
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
        table_name: &str,
        schema: Option<&str>,
    ) -> ServiceResult<Option<(u64, bool)>> {
        Self::ensure_relational_connection(connection.as_ref())?;
        let relation = self
            .resolve_relation_reference(connection.clone(), table_name, schema, false)
            .await;
        if connection.supports_fast_exact_count() {
            let qualified = Self::qualified_relation_name(connection.as_ref(), &relation);
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

        let qualified_table_name = relation.to_sql_object_name();

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

    /// Browse table data with automatic LIMIT
    ///
    /// # Arguments
    ///
    /// * `connection` - Database connection
    /// * `table_name` - Name of the table to browse
    /// * `limit` - Optional limit (uses default if not specified)
    /// * `offset` - Optional offset for pagination
    ///
    /// # Returns
    ///
    /// A `QueryResult` containing the table data
    #[tracing::instrument(skip(self, connection))]
    pub async fn browse_table(
        &self,
        connection: Arc<dyn Connection>,
        table_name: &str,
        schema: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> ServiceResult<QueryResult> {
        Self::ensure_relational_connection(connection.as_ref())?;
        let limit = limit.unwrap_or(self.default_limit);
        let offset = offset.unwrap_or(0);
        let relation = self
            .resolve_relation_reference(connection.clone(), table_name, schema, false)
            .await;
        let qualified = Self::qualified_relation_name(connection.as_ref(), &relation);

        // Build safe SQL with proper identifier escaping
        let base_sql = format!("SELECT * FROM {}", qualified);
        let data_sql = connection.paginated_select_sql(&base_sql, limit as u64, offset as u64);

        tracing::debug!("Browsing table with SQL: {}", data_sql);

        let (mut result, total_rows, is_estimated) = if connection.supports_fast_exact_count() {
            // SQLite/DuckDB: COUNT(*) is essentially free, run concurrently.
            let count_base_sql = format!("SELECT COUNT(*) FROM {}", qualified);
            let count_sql = connection.paginated_select_sql(&count_base_sql, 1, 0);
            tracing::debug!("Counting rows with SQL: {}", count_sql);

            let count_conn = connection.clone();
            let data_future = connection.query(&data_sql, &[]);
            let count_future = count_conn.query(&count_sql, &[]);

            let (data_result, count_result) = tokio::join!(data_future, count_future);

            let result =
                data_result.map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

            let total_rows = match count_result {
                Ok(count_res) => {
                    if !count_res.rows.is_empty() && !count_res.rows[0].values.is_empty() {
                        count_res.rows[0].values[0].as_i64().map(|i| i as u64)
                    } else {
                        None
                    }
                }
                Err(e) => {
                    tracing::warn!("COUNT(*) query failed, pagination total unavailable: {}", e);
                    None
                }
            };

            (result, total_rows, false)
        } else {
            let estimate_conn = connection.clone();
            let data_future = connection.query(&data_sql, &[]);
            let estimate_future =
                self.estimate_row_count(estimate_conn, &relation.table_name, relation.schema_ref());

            let (data_result, estimate_result) = tokio::join!(data_future, estimate_future);

            let result =
                data_result.map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

            let (total_rows, is_estimated) = match estimate_result {
                Ok(Some((total, estimated))) => (Some(total), estimated),
                Ok(None) => (None, false),
                Err(error) => {
                    tracing::warn!(
                        "Estimated row count query failed, pagination total unavailable: {}",
                        error
                    );
                    (None, false)
                }
            };

            (result, total_rows, is_estimated)
        };

        result.total_rows = total_rows;
        result.is_estimated_total = is_estimated;

        tracing::info!(
            table_name = %table_name,
            rows = result.rows.len(),
            total = ?total_rows,
            is_estimated = is_estimated,
            "Table data loaded successfully"
        );

        Ok(result)
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
    /// `Ok(())` if the update succeeds
    #[tracing::instrument(skip(self, connection, cell_data), fields(table_name = %table_name))]
    pub async fn update_cell(
        &self,
        connection: Arc<dyn Connection>,
        table_name: &str,
        schema: Option<&str>,
        cell_data: CellUpdateData,
    ) -> ServiceResult<()> {
        Self::ensure_relational_connection(connection.as_ref())?;
        tracing::debug!("Updating cell in table {}", table_name);
        let relation = self
            .resolve_relation_reference(connection.clone(), table_name, schema, false)
            .await;
        // Build row identifier (use primary key if available)
        let row_identifier = self
            .build_row_identifier(connection.clone(), &relation, &cell_data)
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
            table_name: relation.to_raw_driver_table_name(),
            column_name: cell_data.column_name.clone(),
            column_type: target_col_type.map(str::to_string),
            row_column_types: cell_data
                .all_column_names
                .iter()
                .cloned()
                .zip(cell_data.all_column_types.iter().cloned())
                .collect(),
            new_value,
            row_identifier,
        };

        let affected_rows = connection
            .update_cell(update_request)
            .await
            .map_err(|e| ServiceError::UpdateFailed(e.to_string()))?;

        if affected_rows == 0 {
            return Err(ServiceError::UpdateFailed(
                "No rows matched - the row may have been modified or deleted by another user"
                    .to_string(),
            ));
        }

        tracing::info!(
            table_name = %table_name,
            column = %cell_data.column_name,
            affected_rows = affected_rows,
            "Cell updated successfully"
        );

        Ok(())
    }

    /// Build row identifier (prefer primary key, fallback to full row)
    async fn build_row_identifier(
        &self,
        connection: Arc<dyn Connection>,
        relation: &ResolvedRelationName,
        cell_data: &CellUpdateData,
    ) -> ServiceResult<RowIdentifier> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        // Try to use primary key
        if let Ok(Some(pk_info)) = schema
            .get_primary_key(relation.schema_ref(), &relation.table_name)
            .await
        {
            let mut pk_values = Vec::with_capacity(pk_info.columns.len());
            let mut has_complete_primary_key = true;

            for pk_col in &pk_info.columns {
                let Some(idx) = cell_data
                    .all_column_names
                    .iter()
                    .position(|col| col == pk_col)
                else {
                    has_complete_primary_key = false;
                    break;
                };

                let Some(value) = cell_data.all_row_values.get(idx).cloned() else {
                    has_complete_primary_key = false;
                    break;
                };

                let col_type = cell_data.all_column_types.get(idx).map(|s| s.as_str());
                let value = match value {
                    Value::String(text) => self.parse_value(&text, col_type)?,
                    other => other,
                };

                if value.is_null() {
                    has_complete_primary_key = false;
                    break;
                }

                pk_values.push((pk_col.clone(), value));
            }

            if has_complete_primary_key && pk_values.len() == pk_info.columns.len() {
                tracing::debug!("Using primary key for row identification");
                return Ok(RowIdentifier::PrimaryKey(pk_values));
            }
        }

        // Fallback: use all columns
        tracing::debug!("Using full row for row identification (no primary key available)");
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
    fn parse_value_with_type(&self, value_str: &str, column_type: &str) -> ServiceResult<Value> {
        let col_type = Value::normalize_data_type(column_type);

        if Value::array_element_type(&col_type).is_some() {
            return Ok(Value::parse_from_string(value_str, &col_type));
        }

        if matches!(col_type.as_str(), "json" | "jsonb") {
            return match Value::parse_from_string(value_str, &col_type) {
                Value::Json(value) => Ok(Value::Json(value)),
                _ => Err(ServiceError::InvalidValue(format!(
                    "Invalid JSON value for {} column",
                    column_type
                ))),
            };
        }

        if col_type == "set" {
            return Ok(Value::parse_from_string(value_str, &col_type));
        }

        if Self::is_string_type(&col_type) {
            return Ok(Value::String(value_str.to_string()));
        }

        if Self::is_boolean_type(&col_type) {
            return match value_str.to_lowercase().as_str() {
                "true" | "t" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "f" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Ok(Value::String(value_str.to_string())),
            };
        }

        if Self::is_integer_type(&col_type) {
            if let Ok(val) = value_str.parse::<i64>() {
                if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
                    return Ok(Value::Int32(val as i32));
                }
                return Ok(Value::Int64(val));
            }
            return Ok(Value::String(value_str.to_string()));
        }

        if Self::is_float_type(&col_type) {
            if let Ok(val) = value_str.parse::<f64>() {
                return Ok(Value::Float64(val));
            }
            return Ok(Value::String(value_str.to_string()));
        }

        // Dates, timestamps, UUIDs, JSON, etc. — keep as string and let the
        // database driver handle formatting in value_to_pg_literal / equivalent
        Ok(Value::String(value_str.to_string()))
    }

    pub fn is_string_type(col_type: &str) -> bool {
        matches!(
            col_type,
            "text"
                | "varchar"
                | "char"
                | "bpchar"
                | "name"
                | "citext"
                | "character varying"
                | "character"
                | "nvarchar"
                | "nchar"
                | "longtext"
                | "mediumtext"
                | "tinytext"
                | "enum"
                | "set"
        )
    }

    fn is_boolean_type(col_type: &str) -> bool {
        matches!(col_type, "bool" | "boolean" | "tinyint(1)")
    }

    fn is_integer_type(col_type: &str) -> bool {
        matches!(
            col_type,
            "int2"
                | "int4"
                | "int8"
                | "smallint"
                | "integer"
                | "bigint"
                | "int"
                | "mediumint"
                | "tinyint"
                | "serial"
                | "bigserial"
                | "smallserial"
        )
    }

    fn is_float_type(col_type: &str) -> bool {
        matches!(
            col_type,
            "float4"
                | "float8"
                | "real"
                | "double precision"
                | "double"
                | "float"
                | "numeric"
                | "decimal"
                | "money"
        )
    }

    fn qualified_relation_name(
        connection: &dyn Connection,
        relation: &ResolvedRelationName,
    ) -> String {
        match relation.schema_ref() {
            Some(schema_name) => connection.render_qualified_name(
                &zqlz_core::SqlObjectName::with_namespace(schema_name, &relation.table_name),
            ),
            None => connection.quote_identifier(&relation.table_name),
        }
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
    #[tracing::instrument(skip(self, connection, row_data), fields(table_name = %table_name))]
    pub async fn insert_row(
        &self,
        connection: Arc<dyn Connection>,
        table_name: &str,
        schema: Option<&str>,
        row_data: RowInsertData,
    ) -> ServiceResult<()> {
        Self::ensure_relational_connection(connection.as_ref())?;
        tracing::debug!("Inserting row into table {}", table_name);
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

        let relation = self
            .resolve_relation_reference(connection.clone(), table_name, schema, false)
            .await;

        let schema_columns =
            if let Some(schema_introspection) = connection.as_schema_introspection() {
                schema_introspection
                    .get_columns(relation.schema_ref(), &relation.table_name)
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
            Self::qualified_relation_name(connection.as_ref(), &relation),
            columns.join(", "),
            placeholders.join(", ")
        );

        tracing::debug!("Insert SQL: {}", sql);

        connection
            .execute(&sql, &params)
            .await
            .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

        tracing::info!(
            table_name = %table_name,
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
    #[tracing::instrument(skip(self, connection, delete_data), fields(table_name = %table_name))]
    pub async fn delete_rows(
        &self,
        connection: Arc<dyn Connection>,
        table_name: &str,
        schema: Option<&str>,
        delete_data: RowDeleteData,
    ) -> ServiceResult<u64> {
        Self::ensure_relational_connection(connection.as_ref())?;
        tracing::debug!(
            "Deleting {} rows from table {}",
            delete_data.rows.len(),
            table_name
        );

        if delete_data.rows.is_empty() {
            return Ok(0);
        }

        let schema_introspection = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        let relation = self
            .resolve_relation_reference(connection.clone(), table_name, schema, false)
            .await;

        // Try to get primary key
        let pk_info = schema_introspection
            .get_primary_key(relation.schema_ref(), &relation.table_name)
            .await
            .ok()
            .flatten();

        let mut total_deleted = 0u64;

        for row_values in &delete_data.rows {
            // Build row identifier
            let row_identifier = if let Some(ref pk) = pk_info {
                // Use primary key
                let pk_values: Vec<(String, Value)> = pk
                    .columns
                    .iter()
                    .filter_map(|pk_col| {
                        let idx = delete_data
                            .all_column_names
                            .iter()
                            .position(|col| col == pk_col)?;
                        let value = row_values.get(idx)?.clone();
                        Some((pk_col.clone(), value))
                    })
                    .collect();

                if !pk_values.is_empty() {
                    RowIdentifier::PrimaryKey(pk_values)
                } else {
                    // Fallback to full row
                    self.build_full_row_identifier(&delete_data.all_column_names, row_values)?
                }
            } else {
                // No primary key, use full row
                self.build_full_row_identifier(&delete_data.all_column_names, row_values)?
            };

            // Build DELETE statement
            let (where_clause, params) =
                self.build_where_clause(connection.as_ref(), &row_identifier)?;
            let sql = format!(
                "DELETE FROM {} WHERE {}",
                Self::qualified_relation_name(connection.as_ref(), &relation),
                where_clause
            );

            tracing::debug!("Delete SQL: {}", sql);

            let result = connection
                .execute(&sql, &params)
                .await
                .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

            total_deleted += result.affected_rows;
        }

        tracing::info!(
            table_name = %table_name,
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

/// Data required to update a cell
#[derive(Debug, Clone)]
pub struct BrowseTableWithFiltersRequest<'a> {
    /// Name of the table to browse.
    pub table_name: &'a str,
    /// Optional schema or database qualifier.
    pub schema: Option<&'a str>,
    /// WHERE clause fragments joined with `AND`.
    pub where_clauses: Vec<String>,
    /// ORDER BY clause fragments already formatted as SQL.
    pub order_by_clauses: Vec<String>,
    /// Which columns to select. Empty means all columns.
    pub visible_columns: Vec<String>,
    /// Optional page size. Uses the service default when absent.
    pub limit: Option<usize>,
    /// Optional page offset.
    pub offset: Option<usize>,
    /// Cached total row count for simple page navigations.
    pub cached_total: Option<u64>,
}

/// Data required to browse the last page efficiently.
#[derive(Debug, Clone)]
pub struct BrowseLastPageRequest<'a> {
    /// Name of the table to browse.
    pub table_name: &'a str,
    /// Optional schema or database qualifier.
    pub schema: Option<&'a str>,
    /// WHERE clause fragments joined with `AND`.
    pub where_clauses: Vec<String>,
    /// ORDER BY clause fragments already formatted as SQL.
    pub order_by_clauses: Vec<String>,
    /// Which columns to select. Empty means all columns.
    pub visible_columns: Vec<String>,
    /// Number of rows to fetch.
    pub limit: usize,
    /// Primary key columns used when no explicit sort is active.
    pub pk_columns: Vec<String>,
}

/// Data required to browse a page near the end of a table efficiently.
#[derive(Debug, Clone)]
pub struct BrowseNearEndPageRequest<'a> {
    /// Name of the table to browse.
    pub table_name: &'a str,
    /// Optional schema or database qualifier.
    pub schema: Option<&'a str>,
    /// WHERE clause fragments joined with `AND`.
    pub where_clauses: Vec<String>,
    /// ORDER BY clause fragments already formatted as SQL.
    pub order_by_clauses: Vec<String>,
    /// Which columns to select. Empty means all columns.
    pub visible_columns: Vec<String>,
    /// Number of rows to fetch.
    pub limit: usize,
    /// Offset of the requested page in forward order.
    pub offset: usize,
    /// Known total row count used to compute the reverse offset.
    pub total_rows: u64,
    /// Primary key columns used when no explicit sort is active.
    pub pk_columns: Vec<String>,
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
    /// Source table name.
    pub source_table_name: String,
    /// Target table name.
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
    /// Table names to delete.
    pub table_names: Vec<String>,
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
    /// Table names to empty.
    pub table_names: Vec<String>,
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
    /// Existing table name.
    pub source_table_name: String,
    /// New table name.
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
    /// Source table name (optionally schema-qualified as `schema.table`).
    pub table_name: String,
    /// Column to project distinct values from.
    pub column_name: String,
    /// Maximum number of values to return.
    pub limit: u64,
}

/// Request for loading foreign-key candidate values from a referenced table.
#[derive(Debug, Clone)]
pub struct LoadForeignKeyValuesRequest {
    /// Referenced table name (may be schema-qualified as `schema.table`).
    pub referenced_table: String,
    /// Optional schema/database hint when `referenced_table` is unqualified.
    pub referenced_schema: Option<String>,
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
    /// Target table name.
    pub table_name: String,
    /// Optional schema/database qualifier for the table.
    pub schema: Option<String>,
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
    /// Target table name.
    pub table_name: String,
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
    pub fn generate_table_changes_sql(&self, request: GenerateTableChangesSqlRequest) -> String {
        let GenerateTableChangesSqlRequest {
            table_name,
            column_names,
            modified_cells,
            deleted_row_indices,
            new_rows,
            all_rows,
        } = request;

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
                        format!("\"{}\" = {}", column_name, value)
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
                        format!("\"{}\" IS NULL", column_name)
                    } else {
                        let sql_value = Self::value_to_change_sql_literal(value);
                        format!("\"{}\" = {}", column_name, sql_value)
                    }
                })
                .collect();

            sql_statements.push(format!(
                "UPDATE \"{}\" SET {} WHERE {};",
                table_name,
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
                        format!("\"{}\" IS NULL", column_name)
                    } else {
                        let sql_value = Self::value_to_change_sql_literal(value);
                        format!("\"{}\" = {}", column_name, sql_value)
                    }
                })
                .collect();

            sql_statements.push(format!(
                "DELETE FROM \"{}\" WHERE {};",
                table_name,
                where_parts.join(" AND ")
            ));
        }

        for row_values in &new_rows {
            let column_list = column_names
                .iter()
                .map(|name| format!("\"{}\"", name))
                .collect::<Vec<_>>()
                .join(", ");

            let values: Vec<String> = row_values
                .iter()
                .map(Self::value_to_change_sql_literal)
                .collect();

            sql_statements.push(format!(
                "INSERT INTO \"{}\" ({}) VALUES ({});",
                table_name,
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
            table_name,
            schema,
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

                match self
                    .update_cell(
                        connection.clone(),
                        &table_name,
                        schema.as_deref(),
                        cell_update,
                    )
                    .await
                {
                    Ok(()) => {
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
                    .delete_rows(
                        connection.clone(),
                        &table_name,
                        schema.as_deref(),
                        row_delete_data,
                    )
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
                .insert_row(
                    connection.clone(),
                    &table_name,
                    schema.as_deref(),
                    row_insert_data,
                )
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
                source_table_name,
                target_table_name,
            } = operation;
            let sql = match connection.duplicate_table_sql(
                &SqlObjectName::new(&source_table_name),
                &SqlObjectName::new(&target_table_name),
            ) {
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
            table_names,
            continue_on_error,
        } = request;

        let mut outcome = DeleteTablesOutcome::default();

        for table_name in table_names {
            let sql = match connection.drop_table_sql(
                &SqlObjectName::new(&table_name),
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
            table_names,
            continue_on_error,
        } = request;

        let mut outcome = EmptyTablesOutcome::default();

        for table_name in table_names {
            let sql = match connection.truncate_table_sql(&SqlObjectName::new(&table_name)) {
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
            source_table_name,
            target_table_name,
        } = request;

        let sql = connection
            .rename_table_sql(&SqlObjectName::new(&source_table_name), &target_table_name)
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
            table_name,
            column_name,
            limit,
        } = request;

        let relation = self
            .resolve_relation_reference(connection.clone(), &table_name, None, false)
            .await;
        let table_object_name = relation.to_sql_object_name();
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
            referenced_schema,
            referenced_columns,
            query,
            limit,
        } = request;

        let relation = self
            .resolve_relation_reference(
                connection.clone(),
                &referenced_table,
                referenced_schema
                    .as_deref()
                    .map(str::trim)
                    .filter(|schema| !schema.is_empty()),
                false,
            )
            .await;
        let table_object_name = relation.to_sql_object_name();

        let label_column = self
            .best_fk_label_column(
                connection.as_ref(),
                relation.schema_ref(),
                &relation.table_name,
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
                    referenced_table, error
                ))
            })?;

        let result = connection.query(&sql, &[]).await.map_err(|error| {
            ServiceError::TableOperationFailed(format!(
                "Failed to load foreign-key values from {}: {}",
                referenced_table, error
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
            Value::Decimal(decimal_value) => decimal_value.clone(),
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

fn resolve_table_reference(
    table_name: &str,
    schema_hint: Option<&str>,
) -> (Option<String>, String) {
    if let Some((namespace, relation_name)) =
        table_name
            .split_once('.')
            .and_then(|(namespace, relation_name)| {
                if namespace.is_empty() || relation_name.is_empty() {
                    None
                } else {
                    Some((namespace, relation_name))
                }
            })
    {
        if relation_name.starts_with(&format!("{namespace}.")) {
            let relation_name = relation_name
                .strip_prefix(&format!("{namespace}."))
                .unwrap_or(relation_name);
            return (Some(namespace.to_string()), relation_name.to_string());
        }
        return (Some(namespace.to_string()), relation_name.to_string());
    }

    (
        schema_hint
            .map(str::trim)
            .filter(|schema_name| !schema_name.is_empty())
            .map(ToString::to_string),
        table_name.to_string(),
    )
}

fn relation_info_matches_name(catalog_name: &str, requested_name: &str) -> bool {
    catalog_name == requested_name
        || catalog_name
            .split_once('.')
            .map(|(_, relation_name)| relation_name == requested_name)
            .unwrap_or(false)
}

fn is_known_schema_name(schema_name: Option<&str>, known_schema_names: &[String]) -> bool {
    let Some(schema_name) = schema_name else {
        return false;
    };

    known_schema_names
        .iter()
        .any(|known_schema_name| known_schema_name == schema_name)
}

fn relation_display_name(catalog_name: &str, requested_name: &str) -> String {
    catalog_name
        .split_once('.')
        .map(|(_, relation_name)| relation_name.to_string())
        .unwrap_or_else(|| requested_name.to_string())
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
    fn reverse_explicit_asc_to_desc() {
        assert_eq!(
            TableService::reverse_order_by_clause("\"col\" ASC"),
            "\"col\" DESC"
        );
    }

    #[test]
    fn reverse_explicit_desc_to_asc() {
        assert_eq!(
            TableService::reverse_order_by_clause("\"col\" DESC"),
            "\"col\" ASC"
        );
    }

    #[test]
    fn reverse_lowercase_directions() {
        assert_eq!(
            TableService::reverse_order_by_clause("\"name\" asc"),
            "\"name\" DESC"
        );
        assert_eq!(
            TableService::reverse_order_by_clause("\"name\" desc"),
            "\"name\" ASC"
        );
    }

    #[test]
    fn reverse_implicit_asc() {
        // No explicit direction means implicit ASC → should become DESC
        assert_eq!(
            TableService::reverse_order_by_clause("\"created_at\""),
            "\"created_at\" DESC"
        );
    }

    #[test]
    fn reverse_preserves_nulls_first_suffix() {
        assert_eq!(
            TableService::reverse_order_by_clause("\"col\" ASC NULLS FIRST"),
            "\"col\" DESC NULLS FIRST"
        );
        assert_eq!(
            TableService::reverse_order_by_clause("\"col\" DESC NULLS FIRST"),
            "\"col\" ASC NULLS FIRST"
        );
    }

    #[test]
    fn reverse_preserves_nulls_last_suffix() {
        assert_eq!(
            TableService::reverse_order_by_clause("\"col\" ASC NULLS LAST"),
            "\"col\" DESC NULLS LAST"
        );
        assert_eq!(
            TableService::reverse_order_by_clause("\"col\" DESC NULLS LAST"),
            "\"col\" ASC NULLS LAST"
        );
    }

    #[test]
    fn reverse_lowercase_nulls_suffix() {
        assert_eq!(
            TableService::reverse_order_by_clause("\"col\" asc nulls first"),
            "\"col\" DESC NULLS FIRST"
        );
        assert_eq!(
            TableService::reverse_order_by_clause("\"col\" desc nulls last"),
            "\"col\" ASC NULLS LAST"
        );
    }

    #[test]
    fn reverse_implicit_asc_with_nulls_suffix() {
        assert_eq!(
            TableService::reverse_order_by_clause("\"col\" NULLS LAST"),
            "\"col\" DESC NULLS LAST"
        );
    }

    #[test]
    fn reverse_with_leading_trailing_whitespace() {
        assert_eq!(
            TableService::reverse_order_by_clause("  \"col\" ASC  "),
            "\"col\" DESC"
        );
    }

    #[test]
    fn reverse_mysql_backtick_identifier() {
        assert_eq!(
            TableService::reverse_order_by_clause("`booking_id` ASC"),
            "`booking_id` DESC"
        );
        assert_eq!(
            TableService::reverse_order_by_clause("`booking_id` DESC"),
            "`booking_id` ASC"
        );
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
        let sql = service.generate_table_changes_sql(GenerateTableChangesSqlRequest {
            table_name: "users".to_string(),
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
        });

        assert!(sql.contains("UPDATE \"users\" SET \"name\" = 'Alice O''Connor' WHERE \"id\" = 1 AND \"name\" = 'Alice';"));
        assert!(sql.contains("DELETE FROM \"users\" WHERE \"id\" = 2 AND \"name\" IS NULL;"));
        assert!(sql.contains("INSERT INTO \"users\" (\"id\", \"name\") VALUES (3, 'Carol');"));
    }

    #[test]
    fn generate_table_changes_sql_ignores_invalid_row_or_column_indexes() {
        let service = TableService::new(1000);
        let sql = service.generate_table_changes_sql(GenerateTableChangesSqlRequest {
            table_name: "users".to_string(),
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
        });

        assert!(sql.is_empty());
    }

    #[test]
    fn resolve_table_reference_prefers_embedded_namespace() {
        let (schema_name, table_name) = resolve_table_reference("public.users", Some("ignored"));
        assert_eq!(schema_name.as_deref(), Some("public"));
        assert_eq!(table_name, "users");
    }

    #[test]
    fn resolve_table_reference_deduplicates_repeated_embedded_namespace() {
        let (schema_name, table_name) =
            resolve_table_reference("analytics.analytics.dim_dates", None);
        assert_eq!(schema_name.as_deref(), Some("analytics"));
        assert_eq!(table_name, "dim_dates");
    }

    #[test]
    fn resolve_table_reference_uses_schema_hint_for_unqualified_name() {
        let (schema_name, table_name) = resolve_table_reference("users", Some("analytics"));
        assert_eq!(schema_name.as_deref(), Some("analytics"));
        assert_eq!(table_name, "users");
    }

    #[test]
    fn resolved_relation_main_keeps_literal_dotted_name_for_raw_driver_request() {
        let relation =
            ResolvedRelationName::new("public.activity_communications", Some("main".to_string()));

        assert_eq!(relation.table_name, "public.activity_communications");
        assert_eq!(relation.schema_ref(), Some("main"));
        assert_eq!(
            relation.to_raw_driver_table_name(),
            "public.activity_communications"
        );
    }

    #[test]
    fn resolved_relation_postgres_schema_stays_schema_qualified_for_raw_driver_request() {
        let relation = ResolvedRelationName::new("users", Some("public".to_string()));

        assert_eq!(relation.table_name, "users");
        assert_eq!(relation.schema_ref(), Some("public"));
        assert_eq!(relation.to_raw_driver_table_name(), "public.users");
    }

    #[test]
    fn relation_info_matches_qualified_catalog_name() {
        assert!(relation_info_matches_name(
            "public.active_customers",
            "active_customers"
        ));
        assert!(relation_info_matches_name(
            "active_customers",
            "active_customers"
        ));
        assert!(!relation_info_matches_name(
            "public.inactive_customers",
            "active_customers"
        ));
    }

    #[test]
    fn known_schema_check_rejects_unknown_embedded_schema() {
        let schema_names = vec!["main".to_string()];

        assert!(!is_known_schema_name(Some("public"), &schema_names));
        assert!(is_known_schema_name(Some("main"), &schema_names));
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
