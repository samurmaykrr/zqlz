//! MySQL EXPLAIN Parser
//!
//! Parses EXPLAIN output from MySQL in various formats:
//! - JSON format (EXPLAIN FORMAT=JSON)
//! - Traditional tabular format (EXPLAIN or EXPLAIN EXTENDED)
//!
//! MySQL EXPLAIN output is structured differently from PostgreSQL:
//! - Uses "access_type" instead of "Node Type"
//! - Different cost model and fields
//! - Nested structure in JSON format uses "query_block" and "nested_loop"
//!
//! # Examples
//!
//! ```
//! use zqlz_analyzer::explain::mysql::parse_mysql_explain;
//!
//! let json_output = r#"{
//!   "query_block": {
//!     "select_id": 1,
//!     "cost_info": {
//!       "query_cost": "1.00"
//!     },
//!     "table": {
//!       "table_name": "users",
//!       "access_type": "ALL",
//!       "rows_examined_per_scan": 100
//!     }
//!   }
//! }"#;
//!
//! let plan = parse_mysql_explain(json_output).unwrap();
//! assert!(plan.has_sequential_scans());
//! ```

use crate::explain::plan::{JoinType, NodeCost, NodeType, PlanNode, QueryPlan};
use serde_json::Value;
use thiserror::Error;

/// Errors that can occur when parsing MySQL EXPLAIN output
#[derive(Debug, Error)]
pub enum MysqlExplainError {
    #[error("Invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("Missing query_block in EXPLAIN output")]
    MissingQueryBlock,

    #[error("Invalid plan structure: {0}")]
    InvalidStructure(String),

    #[error("Unsupported format: expected JSON or tabular format")]
    UnsupportedFormat,

    #[error("Empty EXPLAIN output")]
    EmptyOutput,
}

/// Result type for MySQL EXPLAIN parsing
pub type Result<T> = std::result::Result<T, MysqlExplainError>;

/// Parses MySQL EXPLAIN output (JSON or tabular format)
///
/// Automatically detects the format based on the input.
///
/// # Examples
///
/// ```
/// use zqlz_analyzer::explain::mysql::parse_mysql_explain;
///
/// // JSON format
/// let json = r#"{"query_block": {"select_id": 1, "table": {"table_name": "t", "access_type": "ALL"}}}"#;
/// let plan = parse_mysql_explain(json).unwrap();
///
/// // Tabular format (simplified)
/// let tabular = "1\tSIMPLE\tusers\tALL\t100\tUsing where";
/// let plan = parse_mysql_explain(tabular).unwrap();
/// ```
pub fn parse_mysql_explain(output: &str) -> Result<QueryPlan> {
    let trimmed = output.trim();

    if trimmed.is_empty() {
        return Err(MysqlExplainError::EmptyOutput);
    }

    if trimmed.starts_with('{') {
        parse_json_explain(trimmed)
    } else {
        parse_tabular_explain(trimmed)
    }
}

/// Parses MySQL EXPLAIN FORMAT=JSON output
///
/// MySQL JSON EXPLAIN has a different structure than PostgreSQL:
/// - Root contains "query_block" instead of "Plan"
/// - Uses "access_type" for scan types
/// - Nested loops are in "nested_loop" array
/// - Subqueries are in "subqueries" array
pub fn parse_json_explain(json: &str) -> Result<QueryPlan> {
    let value: Value = serde_json::from_str(json)?;

    // MySQL JSON EXPLAIN structure:
    // { "query_block": { "select_id": 1, ... } }
    let query_block = value
        .get("query_block")
        .ok_or(MysqlExplainError::MissingQueryBlock)?;

    let root = parse_query_block(query_block)?;
    let mut plan = QueryPlan::new(root);

    // Extract cost info from query_block
    if let Some(cost_info) = query_block.get("cost_info") {
        if let Some(query_cost) = cost_info
            .get("query_cost")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
        {
            plan.total_cost = Some(query_cost);
        }
    }

    Ok(plan)
}

/// Parses a query_block from MySQL JSON EXPLAIN
fn parse_query_block(block: &Value) -> Result<PlanNode> {
    // A query_block can contain:
    // - "table" - a single table access
    // - "nested_loop" - an array of joined tables
    // - "ordering_operation" - ORDER BY
    // - "grouping_operation" - GROUP BY
    // - "duplicates_removal" - DISTINCT
    // - "subqueries" - subqueries

    // Check for ordering_operation (wraps the rest)
    if let Some(ordering) = block.get("ordering_operation") {
        return parse_ordering_operation(ordering, block);
    }

    // Check for grouping_operation
    if let Some(grouping) = block.get("grouping_operation") {
        return parse_grouping_operation(grouping, block);
    }

    // Check for duplicates_removal (DISTINCT)
    if let Some(distinct) = block.get("duplicates_removal") {
        return parse_duplicates_removal(distinct, block);
    }

    // Check for nested_loop (joins)
    if let Some(nested_loop) = block.get("nested_loop") {
        return parse_nested_loop(nested_loop);
    }

    // Check for single table access
    if let Some(table) = block.get("table") {
        return parse_table_access(table);
    }

    // Check for union_result
    if let Some(union_result) = block.get("union_result") {
        return parse_union_result(union_result);
    }

    // If none of the above, create a generic Result node
    Ok(PlanNode::new(NodeType::Result))
}

/// Parses an ordering_operation (ORDER BY)
fn parse_ordering_operation(ordering: &Value, _parent: &Value) -> Result<PlanNode> {
    let mut node = PlanNode::new(NodeType::Sort);

    // Extract sort keys if available
    if let Some(using_filesort) = ordering.get("using_filesort").and_then(|v| v.as_bool()) {
        if using_filesort {
            node.sort_method = Some("filesort".to_string());
        }
    }

    if let Some(using_tmptable) = ordering
        .get("using_temporary_table")
        .and_then(|v| v.as_bool())
    {
        if using_tmptable {
            node.extra
                .insert("using_temporary_table".to_string(), Value::Bool(true));
        }
    }

    // Parse the nested content
    if let Some(nested_loop) = ordering.get("nested_loop") {
        let child = parse_nested_loop(nested_loop)?;
        node.children.push(child);
    } else if let Some(table) = ordering.get("table") {
        let child = parse_table_access(table)?;
        node.children.push(child);
    } else if let Some(grouping) = ordering.get("grouping_operation") {
        let child = parse_grouping_operation(grouping, ordering)?;
        node.children.push(child);
    } else if let Some(distinct) = ordering.get("duplicates_removal") {
        let child = parse_duplicates_removal(distinct, ordering)?;
        node.children.push(child);
    }

    Ok(node)
}

/// Parses a grouping_operation (GROUP BY)
fn parse_grouping_operation(grouping: &Value, _parent: &Value) -> Result<PlanNode> {
    let mut node = PlanNode::new(NodeType::Aggregate);

    // Check for group keys
    if let Some(group_by_cols) = grouping.get("group_by_columns").and_then(|v| v.as_array()) {
        node.group_keys = group_by_cols
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
    }

    if let Some(using_tmptable) = grouping
        .get("using_temporary_table")
        .and_then(|v| v.as_bool())
    {
        if using_tmptable {
            // This indicates HashAggregate
            node.node_type = NodeType::HashAggregate;
        }
    }

    if let Some(using_filesort) = grouping.get("using_filesort").and_then(|v| v.as_bool()) {
        if using_filesort {
            node.extra
                .insert("using_filesort".to_string(), Value::Bool(true));
        }
    }

    // Parse the nested content
    if let Some(nested_loop) = grouping.get("nested_loop") {
        let child = parse_nested_loop(nested_loop)?;
        node.children.push(child);
    } else if let Some(table) = grouping.get("table") {
        let child = parse_table_access(table)?;
        node.children.push(child);
    }

    Ok(node)
}

/// Parses duplicates_removal (DISTINCT)
fn parse_duplicates_removal(distinct: &Value, _parent: &Value) -> Result<PlanNode> {
    let mut node = PlanNode::new(NodeType::Unique);

    // Parse the nested content
    if let Some(nested_loop) = distinct.get("nested_loop") {
        let child = parse_nested_loop(nested_loop)?;
        node.children.push(child);
    } else if let Some(table) = distinct.get("table") {
        let child = parse_table_access(table)?;
        node.children.push(child);
    }

    Ok(node)
}

/// Parses a nested_loop array (joins)
fn parse_nested_loop(nested_loop: &Value) -> Result<PlanNode> {
    let tables = nested_loop
        .as_array()
        .ok_or_else(|| MysqlExplainError::InvalidStructure("nested_loop is not an array".into()))?;

    if tables.is_empty() {
        return Err(MysqlExplainError::InvalidStructure(
            "Empty nested_loop".into(),
        ));
    }

    // Parse all tables
    let mut children: Vec<PlanNode> = Vec::new();
    for table_obj in tables {
        if let Some(table) = table_obj.get("table") {
            let child = parse_table_access(table)?;
            children.push(child);
        }
    }

    // If only one table, return it directly
    if children.len() == 1 {
        return Ok(children.pop().unwrap());
    }

    // Build nested loop join tree
    // MySQL shows joins as a flat list, we need to build a tree
    let mut current = children.remove(0);
    for child in children {
        let mut join_node = PlanNode::new(NodeType::NestedLoop);
        join_node.join_type = Some(JoinType::Inner); // Default for nested loop
        join_node.children.push(current);
        join_node.children.push(child);
        current = join_node;
    }

    Ok(current)
}

/// Parses a table access from MySQL JSON
fn parse_table_access(table: &Value) -> Result<PlanNode> {
    // Get access_type and map to NodeType
    let access_type = table
        .get("access_type")
        .and_then(|v| v.as_str())
        .unwrap_or("ALL");

    let node_type = mysql_access_type_to_node_type(access_type);
    let mut node = PlanNode::new(node_type);

    // Table name
    if let Some(name) = table.get("table_name").and_then(|v| v.as_str()) {
        node.relation = Some(name.to_string());
    }

    // Cost info
    if let Some(cost_info) = table.get("cost_info") {
        if let Some(read_cost) = cost_info
            .get("read_cost")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
        {
            let eval_cost = cost_info
                .get("eval_cost")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let total = read_cost + eval_cost;
            node.cost = Some(NodeCost::new(0.0, total));
        }
    }

    // Rows examined
    if let Some(rows) = table.get("rows_examined_per_scan").and_then(|v| v.as_u64()) {
        node.rows = Some(rows);
    }

    // Rows produced (filtered)
    if let Some(rows_produced) = table.get("rows_produced_per_join").and_then(|v| v.as_u64()) {
        node.actual_rows = Some(rows_produced);
    }

    // Filtered percentage
    if let Some(filtered) = table.get("filtered").and_then(|v| v.as_str()) {
        node.extra
            .insert("filtered".to_string(), Value::String(filtered.to_string()));
    }

    // Index used
    if let Some(key) = table.get("key").and_then(|v| v.as_str()) {
        if key != "null" {
            node.index_name = Some(key.to_string());
        }
    }

    // Possible keys
    if let Some(possible_keys) = table.get("possible_keys").and_then(|v| v.as_array()) {
        let keys: Vec<String> = possible_keys
            .iter()
            .filter_map(|k| k.as_str().map(String::from))
            .collect();
        if !keys.is_empty() {
            node.extra.insert(
                "possible_keys".to_string(),
                Value::Array(keys.iter().map(|s| Value::String(s.clone())).collect()),
            );
        }
    }

    // Key length
    if let Some(key_length) = table.get("key_length").and_then(|v| v.as_str()) {
        node.extra.insert(
            "key_length".to_string(),
            Value::String(key_length.to_string()),
        );
    }

    // Ref (columns used in index lookup)
    if let Some(ref_cols) = table.get("ref").and_then(|v| v.as_array()) {
        let refs: Vec<String> = ref_cols
            .iter()
            .filter_map(|r| r.as_str().map(String::from))
            .collect();
        if !refs.is_empty() {
            node.extra.insert(
                "ref".to_string(),
                Value::Array(refs.iter().map(|s| Value::String(s.clone())).collect()),
            );
        }
    }

    // Attached condition (WHERE)
    if let Some(condition) = table.get("attached_condition").and_then(|v| v.as_str()) {
        node.filter = Some(condition.to_string());
    }

    // Using index (covering index)
    if let Some(using_index) = table.get("using_index").and_then(|v| v.as_bool()) {
        if using_index {
            // This is effectively an index-only scan
            if node.node_type == NodeType::IndexScan {
                node.node_type = NodeType::IndexOnlyScan;
            }
            node.extra
                .insert("using_index".to_string(), Value::Bool(true));
        }
    }

    // Using index for group-by
    if let Some(_using_index_for_group_by) = table.get("using_index_for_group_by") {
        node.extra
            .insert("using_index_for_group_by".to_string(), Value::Bool(true));
    }

    // Subqueries in this table access
    if let Some(subqueries) = table.get("subqueries").and_then(|v| v.as_array()) {
        for subquery in subqueries {
            if let Some(query_block) = subquery.get("query_block") {
                if let Ok(child) = parse_query_block(query_block) {
                    let mut subquery_scan = PlanNode::new(NodeType::SubqueryScan);
                    subquery_scan.children.push(child);
                    node.children.push(subquery_scan);
                }
            }
        }
    }

    Ok(node)
}

/// Parses union_result
fn parse_union_result(union: &Value) -> Result<PlanNode> {
    let mut node = PlanNode::new(NodeType::Append);

    if let Some(table_name) = union.get("table_name").and_then(|v| v.as_str()) {
        node.description = Some(format!("Union result: {}", table_name));
    }

    if let Some(using_tmptable) = union.get("using_temporary_table").and_then(|v| v.as_bool()) {
        if using_tmptable {
            node.extra
                .insert("using_temporary_table".to_string(), Value::Bool(true));
        }
    }

    // Parse the query_specifications (UNION members)
    if let Some(query_specs) = union.get("query_specifications").and_then(|v| v.as_array()) {
        for spec in query_specs {
            if let Some(query_block) = spec.get("query_block") {
                if let Ok(child) = parse_query_block(query_block) {
                    node.children.push(child);
                }
            }
        }
    }

    Ok(node)
}

/// Maps MySQL access_type to NodeType
fn mysql_access_type_to_node_type(access_type: &str) -> NodeType {
    match access_type.to_lowercase().as_str() {
        "all" => NodeType::SeqScan,                 // Full table scan
        "index" => NodeType::IndexScan,             // Full index scan
        "range" => NodeType::IndexScan,             // Index range scan
        "ref" => NodeType::IndexScan,               // Non-unique index lookup
        "eq_ref" => NodeType::IndexScan,            // Unique index lookup
        "const" => NodeType::IndexScan,             // Constant lookup (very fast)
        "system" => NodeType::IndexScan,            // System table (single row)
        "ref_or_null" => NodeType::IndexScan,       // Like ref, but also searches for NULL
        "fulltext" => NodeType::IndexScan,          // Fulltext index
        "unique_subquery" => NodeType::IndexScan,   // Unique subquery optimization
        "index_subquery" => NodeType::IndexScan,    // Non-unique subquery optimization
        "index_merge" => NodeType::BitmapIndexScan, // Multiple index merge
        _ => NodeType::Unknown,
    }
}

/// Parses MySQL traditional tabular EXPLAIN output
///
/// The tabular format has these columns:
/// id | select_type | table | partitions | type | possible_keys | key | key_len | ref | rows | filtered | Extra
pub fn parse_tabular_explain(text: &str) -> Result<QueryPlan> {
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();

    if lines.is_empty() {
        return Err(MysqlExplainError::EmptyOutput);
    }

    // Check if first line is a header (contains "id" or "select_type")
    let data_lines = if lines[0].contains("select_type") || lines[0].to_lowercase().contains("id\t")
    {
        &lines[1..]
    } else {
        &lines[..]
    };

    if data_lines.is_empty() {
        return Err(MysqlExplainError::EmptyOutput);
    }

    let mut rows: Vec<TabularRow> = Vec::new();
    for line in data_lines {
        if let Some(row) = parse_tabular_row(line) {
            rows.push(row);
        }
    }

    if rows.is_empty() {
        return Err(MysqlExplainError::InvalidStructure(
            "No valid rows found".into(),
        ));
    }

    // Build plan tree from rows
    let root = build_plan_from_rows(&rows)?;
    Ok(QueryPlan::new(root))
}

/// Represents a row from tabular EXPLAIN output
struct TabularRow {
    id: u32,
    select_type: String,
    table: String,
    access_type: String,
    possible_keys: Option<String>,
    key: Option<String>,
    key_len: Option<String>,
    ref_cols: Option<String>,
    rows: Option<u64>,
    filtered: Option<f64>,
    extra: Option<String>,
}

/// Parses a single tabular row
fn parse_tabular_row(line: &str) -> Option<TabularRow> {
    let parts: Vec<&str> = line.split('\t').collect();

    // Need at least id, select_type, table, type
    if parts.len() < 4 {
        // Try pipe-separated format (some MySQL clients use this)
        let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
        if parts.len() < 4 {
            return None;
        }
        return parse_tabular_parts(&parts);
    }

    parse_tabular_parts(&parts)
}

fn parse_tabular_parts(parts: &[&str]) -> Option<TabularRow> {
    // Filter out empty parts (happens with pipe format)
    let parts: Vec<&str> = parts.iter().filter(|s| !s.is_empty()).cloned().collect();

    if parts.len() < 4 {
        return None;
    }

    let id = parts.first()?.trim().parse().ok()?;
    let select_type = parts.get(1)?.trim().to_string();
    let table = parts.get(2)?.trim().to_string();

    // MySQL EXPLAIN output has two possible formats:
    // Without partitions (11 columns): id | select_type | table | type | possible_keys | key | key_len | ref | rows | filtered | Extra
    // With partitions (12 columns): id | select_type | table | partitions | type | possible_keys | key | key_len | ref | rows | filtered | Extra
    //
    // Position 3 is either 'type' (access type) or 'partitions'
    // If position 3 is a valid access type, use 11-column format; otherwise use 12-column format

    let has_partitions = parts.len() >= 5 && !is_access_type(parts[3]) && is_access_type(parts[4]);

    let (
        access_type_idx,
        possible_keys_idx,
        key_idx,
        key_len_idx,
        ref_idx,
        rows_idx,
        filtered_idx,
        extra_idx,
    ) = if has_partitions {
        // 12-column format with partitions
        (4, 5, 6, 7, 8, 9, 10, 11)
    } else {
        // 11-column format without partitions
        (3, 4, 5, 6, 7, 8, 9, 10)
    };

    let access_type = parts.get(access_type_idx)?.trim().to_string();

    let possible_keys = parts.get(possible_keys_idx).map(|s| s.trim().to_string());
    let key = parts
        .get(key_idx)
        .filter(|s| **s != "NULL")
        .map(|s| s.trim().to_string());
    let key_len = parts.get(key_len_idx).map(|s| s.trim().to_string());
    let ref_cols = parts.get(ref_idx).map(|s| s.trim().to_string());
    let rows = parts.get(rows_idx).and_then(|s| s.trim().parse().ok());
    let filtered = parts.get(filtered_idx).and_then(|s| s.trim().parse().ok());
    let extra = parts.get(extra_idx).map(|s| s.trim().to_string());

    Some(TabularRow {
        id,
        select_type,
        table,
        access_type,
        possible_keys,
        key,
        key_len,
        ref_cols,
        rows,
        filtered,
        extra,
    })
}

fn is_access_type(s: &str) -> bool {
    matches!(
        s.to_lowercase().as_str(),
        "all" | "index" | "range" | "ref" | "eq_ref" | "const" | "system" | "null" | "fulltext"
    )
}

/// Builds a plan tree from tabular rows
fn build_plan_from_rows(rows: &[TabularRow]) -> Result<PlanNode> {
    if rows.is_empty() {
        return Err(MysqlExplainError::InvalidStructure("No rows".into()));
    }

    // Group rows by select_id
    // Rows with same id are part of the same query block
    let mut nodes: Vec<PlanNode> = Vec::new();

    for row in rows {
        let node_type = mysql_access_type_to_node_type(&row.access_type);
        let mut node = PlanNode::new(node_type);

        node.relation = Some(row.table.clone());

        if let Some(ref key) = row.key {
            if key != "NULL" {
                node.index_name = Some(key.clone());
            }
        }

        if let Some(rows) = row.rows {
            node.rows = Some(rows);
        }

        if let Some(filtered) = row.filtered {
            node.extra.insert(
                "filtered".to_string(),
                Value::Number(
                    serde_json::Number::from_f64(filtered).unwrap_or(serde_json::Number::from(0)),
                ),
            );
        }

        if let Some(ref extra) = row.extra {
            parse_extra_field(&mut node, extra);
        }

        // Store possible_keys if available
        if let Some(ref possible_keys) = row.possible_keys {
            if possible_keys != "NULL" && !possible_keys.is_empty() {
                node.extra.insert(
                    "possible_keys".to_string(),
                    Value::String(possible_keys.clone()),
                );
            }
        }

        // Store key_len if available
        if let Some(ref key_len) = row.key_len {
            if key_len != "NULL" && !key_len.is_empty() {
                node.extra
                    .insert("key_length".to_string(), Value::String(key_len.clone()));
            }
        }

        // Store ref columns if available
        if let Some(ref ref_cols) = row.ref_cols {
            if ref_cols != "NULL" && !ref_cols.is_empty() {
                node.extra
                    .insert("ref".to_string(), Value::String(ref_cols.clone()));
            }
        }

        // Store select_type for building the tree
        node.extra.insert(
            "select_type".to_string(),
            Value::String(row.select_type.clone()),
        );
        node.extra
            .insert("select_id".to_string(), Value::Number(row.id.into()));

        nodes.push(node);
    }

    // If single node, return it
    if nodes.len() == 1 {
        return Ok(nodes.pop().unwrap());
    }

    // Build join tree (nested loop by default for MySQL)
    let mut current = nodes.remove(0);
    for node in nodes {
        let mut join = PlanNode::new(NodeType::NestedLoop);
        join.join_type = Some(JoinType::Inner);
        join.children.push(current);
        join.children.push(node);
        current = join;
    }

    Ok(current)
}

/// Parses the Extra field from tabular EXPLAIN
fn parse_extra_field(node: &mut PlanNode, extra: &str) {
    let extra_lower = extra.to_lowercase();

    if extra_lower.contains("using index") {
        node.extra
            .insert("using_index".to_string(), Value::Bool(true));
        // This might indicate index-only scan
        if node.node_type == NodeType::IndexScan {
            node.node_type = NodeType::IndexOnlyScan;
        }
    }

    if extra_lower.contains("using where") {
        node.extra
            .insert("using_where".to_string(), Value::Bool(true));
    }

    if extra_lower.contains("using filesort") {
        node.extra
            .insert("using_filesort".to_string(), Value::Bool(true));
    }

    if extra_lower.contains("using temporary") {
        node.extra
            .insert("using_temporary".to_string(), Value::Bool(true));
    }

    if extra_lower.contains("using join buffer") {
        node.extra
            .insert("using_join_buffer".to_string(), Value::Bool(true));
    }

    if extra_lower.contains("range checked for each record") {
        node.extra.insert(
            "range_checked_for_each_record".to_string(),
            Value::Bool(true),
        );
    }

    if extra_lower.contains("using index condition") {
        node.extra
            .insert("using_index_condition".to_string(), Value::Bool(true));
    }

    if extra_lower.contains("using mrr") {
        node.extra
            .insert("using_mrr".to_string(), Value::Bool(true));
    }

    if extra_lower.contains("using index for group-by") {
        node.extra
            .insert("using_index_for_group_by".to_string(), Value::Bool(true));
    }

    // Store the full extra string
    if !extra.is_empty() && extra != "NULL" {
        node.description = Some(extra.to_string());
    }
}

#[cfg(test)]
mod tests;
