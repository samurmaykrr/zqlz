//! Table operations service
//!
//! Provides table browsing, data retrieval, and cell editing operations.

use std::sync::Arc;
use zqlz_core::{CellUpdateRequest, Connection, QueryResult, RowIdentifier, Value};

use crate::error::{ServiceError, ServiceResult};

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
    /// Create a new table service
    ///
    /// # Arguments
    ///
    /// * `default_limit` - Default number of rows to return when browsing tables
    pub fn new(default_limit: usize) -> Self {
        Self { default_limit }
    }

    /// Whether this driver supports cheap COUNT(*) queries.
    ///
    /// SQLite and DuckDB keep row counts in metadata so COUNT(*) is
    /// essentially free. For MySQL/PostgreSQL/MSSQL/ClickHouse the
    /// count requires a full table scan and can take seconds on large
    /// tables, so we skip it and use heuristic "has more" pagination.
    fn supports_fast_count(driver: &str) -> bool {
        matches!(driver, "sqlite" | "duckdb")
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
        table_name: &str,
        schema: Option<&str>,
        where_clauses: Vec<String>,
        order_by_clauses: Vec<String>,
        visible_columns: Vec<String>,
        limit: Option<usize>,
        offset: Option<usize>,
        cached_total: Option<u64>,
    ) -> ServiceResult<QueryResult> {
        let limit = limit.unwrap_or(self.default_limit);
        let offset = offset.unwrap_or(0);
        let driver = connection.driver_name();
        let qualified = Self::qualified_table_name(table_name, schema, driver);

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
                .map(|c| Self::escape_identifier_for(c, driver))
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
        let data_sql = format!(
            "SELECT {} FROM {}{}{} LIMIT {} OFFSET {}",
            columns,
            qualified,
            where_clause,
            order_by_clause,
            limit,
            offset
        );

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
            tracing::debug!("Using cached total_rows={}, skipping COUNT(*)", cached_total.unwrap_or(0));
            let data_result = connection.query(&data_sql, &[]).await;
            let result = data_result
                .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;
            (result, cached_total, false)
        } else if Self::supports_fast_count(driver) {
            // Run the data query and COUNT(*) concurrently so the user doesn't wait
            // for a potentially slow full-table count before seeing rows.
            let count_sql = format!(
                "SELECT COUNT(*) FROM {}{}",
                qualified,
                where_clause
            );
            tracing::debug!("Counting rows with SQL: {}", count_sql);

            let count_conn = connection.clone();
            let data_future = connection.query(&data_sql, &[]);
            let count_future = count_conn.query(&count_sql, &[]);

            let (data_result, count_result) = tokio::join!(data_future, count_future);

            let result = data_result
                .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

            // Extract total count — non-critical, so log and continue if it fails
            let total_rows = match count_result {
                Ok(count_res) => {
                    if !count_res.rows.is_empty() && !count_res.rows[0].values.is_empty() {
                        count_res.rows[0].values[0]
                            .as_i64()
                            .map(|i| i as u64)
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
            tracing::debug!("Driver '{}' with active filters: skipping count (estimates don't apply to filtered queries)", driver);
            let data_result = connection.query(&data_sql, &[]).await;
            let result = data_result
                .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;
            (result, None, false)
        } else {
            // Slow-count driver, no filters: use estimated count from metadata.
            // Sequential execution is required — concurrent futures on the same
            // async-mutex-guarded connection cause waker contention on GPUI's
            // executor, producing spurious "connection closed" errors.
            tracing::debug!("Driver '{}': using estimated row count from metadata", driver);
            let estimate_sql = Self::estimate_row_count_sql(driver, table_name, schema);

            let result = connection
                .query(&data_sql, &[])
                .await
                .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

            let estimated_total = match connection.query(&estimate_sql, &[]).await {
                Ok(est_res) => est_res
                    .rows
                    .first()
                    .and_then(|row| row.values.first())
                    .and_then(|v| v.as_i64())
                    .map(|i| (i as u64).max(0)),
                Err(e) => {
                    tracing::warn!("Estimated row count query failed: {}", e);
                    None
                }
            };

            (result, estimated_total, estimated_total.is_some())
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
        let driver = connection.driver_name();
        let qualified = Self::qualified_table_name(table_name, schema, driver);

        let where_clause = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let count_sql = format!("SELECT COUNT(*) FROM {}{}", qualified, where_clause);
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
                ServiceError::TableOperationFailed(
                    "COUNT(*) query returned no result".to_string(),
                )
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
        table_name: &str,
        schema: Option<&str>,
        where_clauses: Vec<String>,
        order_by_clauses: Vec<String>,
        visible_columns: Vec<String>,
        limit: usize,
        pk_columns: Vec<String>,
    ) -> ServiceResult<QueryResult> {
        let driver = connection.driver_name();
        let qualified = Self::qualified_table_name(table_name, schema, driver);

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
                .map(|c| Self::escape_identifier_for(c, driver))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Build reversed ORDER BY: flip user sorts if present, otherwise
        // use primary key DESC so the engine can satisfy the query via an
        // index scan instead of scanning past millions of rows for a high OFFSET.
        let reversed_order = if order_by_clauses.is_empty() {
            pk_columns
                .iter()
                .map(|col| format!("{} DESC", Self::escape_identifier_for(col, driver)))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            order_by_clauses
                .iter()
                .map(|clause| Self::reverse_order_by_clause(clause))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let data_sql = format!(
            "SELECT {} FROM {}{} ORDER BY {} LIMIT {}",
            columns, qualified, where_clause, reversed_order, limit
        );

        let count_sql = format!("SELECT COUNT(*) FROM {}{}", qualified, where_clause);

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
        table_name: &str,
        schema: Option<&str>,
        where_clauses: Vec<String>,
        order_by_clauses: Vec<String>,
        visible_columns: Vec<String>,
        limit: usize,
        offset: usize,
        total_rows: u64,
        pk_columns: Vec<String>,
    ) -> ServiceResult<QueryResult> {
        let driver = connection.driver_name();
        let qualified = Self::qualified_table_name(table_name, schema, driver);

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
                .map(|c| Self::escape_identifier_for(c, driver))
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
                .map(|col| format!("{} DESC", Self::escape_identifier_for(col, driver)))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            order_by_clauses
                .iter()
                .map(|clause| Self::reverse_order_by_clause(clause))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let data_sql = format!(
            "SELECT {} FROM {}{} ORDER BY {} LIMIT {} OFFSET {}",
            columns, qualified, where_clause, reversed_order, reverse_limit, reverse_offset
        );

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
        let (core, nulls_suffix) =
            if let Some(prefix) = trimmed.strip_suffix(" NULLS FIRST") {
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
    ) -> ServiceResult<(u64, bool)> {
        let driver = connection.driver_name();

        if Self::supports_fast_count(driver) {
            let qualified = Self::qualified_table_name(table_name, schema, driver);
            let count_sql = format!("SELECT COUNT(*) FROM {}", qualified);
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

            return Ok((total, false));
        }

        let estimate_sql = Self::estimate_row_count_sql(driver, table_name, schema);

        tracing::debug!("Estimating row count with SQL: {}", estimate_sql);

        let result = connection
            .query(&estimate_sql, &[])
            .await
            .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

        let estimated = result
            .rows
            .first()
            .and_then(|row| row.values.first())
            .and_then(|v| v.as_i64())
            .map(|i| (i as u64).max(0))
            .ok_or_else(|| {
                ServiceError::TableOperationFailed(
                    "Estimated row count query returned no result".to_string(),
                )
            })?;

        tracing::info!(
            table_name = %table_name,
            estimated = estimated,
            driver = driver,
            "Estimated row count from metadata"
        );

        Ok((estimated, true))
    }

    /// Build the driver-specific SQL to get an estimated row count from metadata.
    fn estimate_row_count_sql(driver: &str, table_name: &str, schema: Option<&str>) -> String {
        match driver {
            "mysql" => {
                let db = schema.unwrap_or("DATABASE()");
                let db_clause = if schema.is_some() {
                    format!("'{}'", db.replace('\'', "''"))
                } else {
                    "DATABASE()".to_string()
                };
                format!(
                    "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} AND TABLE_NAME = '{}'",
                    db_clause,
                    table_name.replace('\'', "''")
                )
            }
            "postgres" | "postgresql" => {
                let schema_name = schema.unwrap_or("public");
                format!(
                    "SELECT reltuples::bigint FROM pg_class WHERE relname = '{}' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{}')",
                    table_name.replace('\'', "''"),
                    schema_name.replace('\'', "''")
                )
            }
            "mssql" | "sqlserver" => {
                let schema_name = schema.unwrap_or("dbo");
                format!(
                    "SELECT SUM(p.rows) FROM sys.partitions p INNER JOIN sys.tables t ON p.object_id = t.object_id INNER JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = '{}' AND s.name = '{}' AND p.index_id IN (0, 1)",
                    table_name.replace('\'', "''"),
                    schema_name.replace('\'', "''")
                )
            }
            "clickhouse" => {
                let db = schema.unwrap_or("default");
                format!(
                    "SELECT sum(rows) FROM system.parts WHERE database = '{}' AND table = '{}' AND active",
                    db.replace('\'', "''"),
                    table_name.replace('\'', "''")
                )
            }
            _ => {
                // MongoDB, Redis, etc. — no metadata shortcut, fall back to 0
                // and let the caller treat it as unknown.
                "SELECT 0".to_string()
            }
        }
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
        let limit = limit.unwrap_or(self.default_limit);
        let offset = offset.unwrap_or(0);
        let driver = connection.driver_name();
        let qualified = Self::qualified_table_name(table_name, schema, driver);

        // Build safe SQL with proper identifier escaping
        let data_sql = format!(
            "SELECT * FROM {} LIMIT {} OFFSET {}",
            qualified,
            limit,
            offset
        );

        tracing::debug!("Browsing table with SQL: {}", data_sql);

        let (mut result, total_rows, is_estimated) = if Self::supports_fast_count(driver) {
            // SQLite/DuckDB: COUNT(*) is essentially free, run concurrently.
            let count_sql = format!(
                "SELECT COUNT(*) FROM {}",
                qualified
            );
            tracing::debug!("Counting rows with SQL: {}", count_sql);

            let count_conn = connection.clone();
            let data_future = connection.query(&data_sql, &[]);
            let count_future = count_conn.query(&count_sql, &[]);

            let (data_result, count_result) = tokio::join!(data_future, count_future);

            let result = data_result
                .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

            let total_rows = match count_result {
                Ok(count_res) => {
                    if !count_res.rows.is_empty() && !count_res.rows[0].values.is_empty() {
                        count_res.rows[0].values[0]
                            .as_i64()
                            .map(|i| i as u64)
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
            // MySQL/PostgreSQL/MSSQL/ClickHouse: fetch data first, then the estimated
            // row count. These drivers share a single underlying connection protected by
            // an async mutex, so running two futures concurrently on the same connection
            // causes pathological waker contention that results in "connection closed"
            // errors. Sequential execution is correct because the mutex serializes them
            // anyway — there is no real parallelism to be gained.
            tracing::debug!("Driver '{}': using estimated row count from metadata", driver);
            let estimate_sql = Self::estimate_row_count_sql(driver, table_name, schema);

            let result = connection
                .query(&data_sql, &[])
                .await
                .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

            let estimated_total = match connection.query(&estimate_sql, &[]).await {
                Ok(est_res) => est_res
                    .rows
                    .first()
                    .and_then(|row| row.values.first())
                    .and_then(|v| v.as_i64())
                    .map(|i| (i as u64).max(0)),
                Err(e) => {
                    tracing::warn!("Estimated row count query failed: {}", e);
                    None
                }
            };

            (result, estimated_total, estimated_total.is_some())
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
        tracing::debug!("Updating cell in table {}", table_name);
        // Build row identifier (use primary key if available)
        let row_identifier = self
            .build_row_identifier(connection.clone(), table_name, &cell_data)
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
            Some(val) => Some(self.parse_value(val, target_col_type)?),
        };

        // Pass raw identifiers to the driver - each driver will handle escaping appropriately
        let table_name_with_schema = match schema {
            Some(s) => format!("{}.{}", s, table_name),
            None => table_name.to_string(),
        };

        let update_request = CellUpdateRequest {
            table_name: table_name_with_schema,
            column_name: cell_data.column_name.clone(),
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
        table_name: &str,
        cell_data: &CellUpdateData,
    ) -> ServiceResult<RowIdentifier> {
        let schema = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        // Try to use primary key
        if let Ok(Some(pk_info)) = schema.get_primary_key(None, table_name).await {
            let pk_values: Vec<(String, Value)> = pk_info
                .columns
                .iter()
                .filter_map(|pk_col| {
                    let idx = cell_data
                        .all_column_names
                        .iter()
                        .position(|col| col == pk_col)?;

                    let value_str = cell_data.all_row_values.get(idx)?;
                    let col_type = cell_data.all_column_types.get(idx).map(|s| s.as_str());
                    let value = self.parse_value(value_str, col_type).ok()?;

                    Some((pk_col.clone(), value))
                })
                .collect();

            if !pk_values.is_empty() {
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
                let value = self.parse_value(val, col_type).unwrap_or(Value::Null);
                (col.clone(), value)
            })
            .collect();

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
        let col_type = column_type.to_lowercase();

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

    /// Escape SQL identifier (table/column name) using the appropriate
    /// quoting style for the target database.
    fn escape_identifier_for(identifier: &str, driver_name: &str) -> String {
        match driver_name {
            "mysql" => format!("`{}`", identifier.replace('`', "``")),
            "mssql" => format!("[{}]", identifier.replace(']', "]]")),
            _ => format!("\"{}\"", identifier.replace('"', "\"\"")),
        }
    }

    /// Build a possibly schema-qualified table reference.
    /// When `schema` is provided (e.g. a MySQL database name), the result is
    /// `schema`.`table`; otherwise just the escaped table name.
    fn qualified_table_name(table_name: &str, schema: Option<&str>, driver_name: &str) -> String {
        match schema {
            Some(s) => format!(
                "{}.{}",
                Self::escape_identifier_for(s, driver_name),
                Self::escape_identifier_for(table_name, driver_name)
            ),
            None => Self::escape_identifier_for(table_name, driver_name),
        }
    }

    fn param_placeholder(driver_name: &str, param_index: usize) -> String {
        if driver_name == "postgresql" {
            format!("${}", param_index)
        } else {
            "?".to_string()
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

    /// Browse a Redis key and return its data as a QueryResult
    ///
    /// This method detects the key type and uses the appropriate Redis command
    /// to fetch the data, then formats it as a QueryResult compatible with
    /// the table viewer.
    ///
    /// # Arguments
    ///
    /// * `connection` - Redis connection
    /// * `key_name` - Name of the Redis key to browse
    /// * `limit` - Optional limit for list/set/zset types
    ///
    /// # Returns
    ///
    /// A `QueryResult` containing the key data
    #[tracing::instrument(skip(self, connection))]
    pub async fn browse_redis_key(
        &self,
        connection: Arc<dyn Connection>,
        key_name: &str,
        limit: Option<usize>,
    ) -> ServiceResult<QueryResult> {
        let limit = limit.unwrap_or(self.default_limit);

        // First, get the key type
        let type_result = connection
            .query(&format!("TYPE {}", key_name), &[])
            .await
            .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?;

        let key_type = type_result
            .rows
            .first()
            .and_then(|r| r.get_by_name("value"))
            .and_then(|v| v.as_str())
            .unwrap_or("none")
            .to_lowercase();

        tracing::debug!(key_name = %key_name, key_type = %key_type, "Browsing Redis key");

        // Execute appropriate command based on key type
        let result = match key_type.as_str() {
            "string" => connection
                .query(&format!("GET {}", key_name), &[])
                .await
                .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?,
            "hash" => connection
                .query(&format!("HGETALL {}", key_name), &[])
                .await
                .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?,
            "list" => {
                // LRANGE key 0 (limit-1) to get first `limit` elements
                let end = if limit > 0 {
                    limit - 1
                } else {
                    -1_isize as usize
                };
                connection
                    .query(&format!("LRANGE {} 0 {}", key_name, end), &[])
                    .await
                    .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?
            }
            "set" => {
                // SSCAN for sets (SMEMBERS can be slow for large sets)
                connection
                    .query(&format!("SMEMBERS {}", key_name), &[])
                    .await
                    .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?
            }
            "zset" => {
                // ZRANGE with WITHSCORES for sorted sets
                let end = if limit > 0 {
                    limit - 1
                } else {
                    -1_isize as usize
                };
                connection
                    .query(&format!("ZRANGE {} 0 {} WITHSCORES", key_name, end), &[])
                    .await
                    .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?
            }
            "stream" => {
                // XRANGE for streams
                connection
                    .query(&format!("XRANGE {} - + COUNT {}", key_name, limit), &[])
                    .await
                    .map_err(|e| ServiceError::TableOperationFailed(e.to_string()))?
            }
            _ => {
                return Err(ServiceError::TableOperationFailed(format!(
                    "Unknown or unsupported Redis key type: {}",
                    key_type
                )));
            }
        };

        tracing::info!(
            key_name = %key_name,
            key_type = %key_type,
            rows = result.rows.len(),
            "Redis key data loaded"
        );

        Ok(result)
    }

    /// Update a Redis key value
    ///
    /// Maps the cell update to the appropriate Redis command based on key type.
    ///
    /// # Arguments
    ///
    /// * `connection` - Redis connection
    /// * `key_name` - Name of the Redis key
    /// * `key_type` - Type of the key (hash, list, set, zset, string)
    /// * `cell_data` - Cell update data
    ///
    /// # Returns
    ///
    /// `Ok(())` if the update succeeds
    #[tracing::instrument(skip(self, connection, cell_data))]
    pub async fn update_redis_key(
        &self,
        connection: Arc<dyn Connection>,
        key_name: &str,
        key_type: &str,
        cell_data: CellUpdateData,
    ) -> ServiceResult<()> {
        let new_value = cell_data.new_value.as_deref().unwrap_or("");

        let cmd = match key_type {
            "string" => {
                // SET key value
                format!("SET {} {}", key_name, new_value)
            }
            "hash" => {
                // HSET key field value
                // The field name should be in the row data
                let field = cell_data
                    .all_row_values
                    .first()
                    .ok_or_else(|| ServiceError::UpdateFailed("No field name found".to_string()))?;
                format!("HSET {} {} {}", key_name, field, new_value)
            }
            "list" => {
                // LSET key index value
                let index = cell_data
                    .all_row_values
                    .first()
                    .ok_or_else(|| ServiceError::UpdateFailed("No index found".to_string()))?;
                format!("LSET {} {} {}", key_name, index, new_value)
            }
            "zset" => {
                // For sorted sets, we need to know if we're updating member or score
                if cell_data.column_name == "score" {
                    // ZADD key score member (updates score for existing member)
                    let member = cell_data
                        .all_row_values
                        .first()
                        .ok_or_else(|| ServiceError::UpdateFailed("No member found".to_string()))?;
                    format!("ZADD {} {} {}", key_name, new_value, member)
                } else {
                    // Can't rename member directly - would need ZREM + ZADD
                    return Err(ServiceError::UpdateFailed(
                        "Cannot rename sorted set member directly. Delete and re-add instead."
                            .to_string(),
                    ));
                }
            }
            "set" => {
                // Sets don't really support "update" - only add/remove
                return Err(ServiceError::UpdateFailed(
                    "Set members cannot be updated. Use add/remove operations instead.".to_string(),
                ));
            }
            _ => {
                return Err(ServiceError::UpdateFailed(format!(
                    "Update not supported for key type: {}",
                    key_type
                )));
            }
        };

        tracing::debug!(command = %cmd, "Executing Redis update");

        connection
            .execute(&cmd, &[])
            .await
            .map_err(|e| ServiceError::UpdateFailed(e.to_string()))?;

        tracing::info!(
            key_name = %key_name,
            key_type = %key_type,
            column = %cell_data.column_name,
            "Redis key updated successfully"
        );

        Ok(())
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
        tracing::debug!("Inserting row into table {}", table_name);
        let driver = connection.driver_name();

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

        let schema_columns = if let Some(schema_introspection) =
            connection.as_schema_introspection()
        {
            schema_introspection
                .get_columns(schema, table_name)
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
            let column_info = schema_columns.as_ref().and_then(|cols| cols.get(column_name));
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
                    columns.push(Self::escape_identifier_for(column_name, driver));
                    placeholders.push(Self::param_placeholder(driver, param_index));
                    param_index += 1;
                    let column_type = column_types.get(column_name).map(|s| s.as_str());
                    params.push(self.parse_value(val, column_type)?);
                }
                None => {
                    if has_default || is_auto_increment {
                        continue;
                    }
                    columns.push(Self::escape_identifier_for(column_name, driver));
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
            Self::qualified_table_name(table_name, schema, driver),
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
        tracing::debug!(
            "Deleting {} rows from table {}",
            delete_data.rows.len(),
            table_name
        );

        if delete_data.rows.is_empty() {
            return Ok(0);
        }

        let driver = connection.driver_name();

        let schema_introspection = connection
            .as_schema_introspection()
            .ok_or(ServiceError::SchemaNotSupported)?;

        // Try to get primary key
        let pk_info = schema_introspection
            .get_primary_key(None, table_name)
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
                        let value_str = row_values.get(idx)?;
                        let value = self.parse_value(value_str, None).ok()?;
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
            let (where_clause, params) = self.build_where_clause(&row_identifier, driver)?;
            let sql = format!(
                "DELETE FROM {} WHERE {}",
                Self::qualified_table_name(table_name, schema, driver),
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
        row_values: &[String],
    ) -> ServiceResult<RowIdentifier> {
        let row_values: Vec<(String, Value)> = column_names
            .iter()
            .zip(row_values.iter())
            .map(|(col, val)| {
                let value = self.parse_value(val, None).unwrap_or(Value::Null);
                (col.clone(), value)
            })
            .collect();

        Ok(RowIdentifier::FullRow(row_values))
    }

    /// Build WHERE clause from row identifier
    fn build_where_clause(
        &self,
        row_identifier: &RowIdentifier,
        driver_name: &str,
    ) -> ServiceResult<(String, Vec<Value>)> {
        match row_identifier {
            RowIdentifier::RowIndex(_) => Err(ServiceError::TableOperationFailed(
                "Row index-based operations not supported".to_string(),
            )),
            RowIdentifier::PrimaryKey(pk_values) => {
                let conditions: Vec<String> = pk_values
                    .iter()
                    .map(|(col, _)| format!("{} = ?", Self::escape_identifier_for(col, driver_name)))
                    .collect();
                let params: Vec<Value> = pk_values.iter().map(|(_, v)| v.clone()).collect();
                Ok((conditions.join(" AND "), params))
            }
            RowIdentifier::FullRow(row_values) => {
                let conditions: Vec<String> = row_values
                    .iter()
                    .map(|(col, val)| {
                        if val == &Value::Null {
                            format!("{} IS NULL", Self::escape_identifier_for(col, driver_name))
                        } else {
                            format!("{} = ?", Self::escape_identifier_for(col, driver_name))
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
pub struct CellUpdateData {
    /// Column name being updated
    pub column_name: String,
    /// New value as a string (None means NULL, Some("") means empty string)
    pub new_value: Option<String>,
    /// All column names in the row (for building row identifier)
    pub all_column_names: Vec<String>,
    /// All values in the row (for building row identifier)
    pub all_row_values: Vec<String>,
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
    /// Values to insert (None means NULL)
    pub values: Vec<Option<String>>,
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
    pub rows: Vec<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(service.parse_value("123", None).unwrap(), Value::Int64(123));
        assert_eq!(
            service.parse_value("123.45", None).unwrap(),
            Value::Float64(123.45)
        );
        assert_eq!(service.parse_value("true", None).unwrap(), Value::Bool(true));
        assert_eq!(service.parse_value("false", None).unwrap(), Value::Bool(false));
        assert_eq!(service.parse_value("TRUE", None).unwrap(), Value::Bool(true));
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
}
