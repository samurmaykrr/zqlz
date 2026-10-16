//! MySQL EXPLAIN Parser
//!
//! Parses EXPLAIN output from MySQL / MariaDB in every format the server can emit:
//! - JSON format (`EXPLAIN FORMAT=JSON`), the richest source
//! - Tree text (`EXPLAIN FORMAT=TREE` and `EXPLAIN ANALYZE`), see [`tree`]
//! - Traditional tabular format (plain `EXPLAIN`), the only one MySQL 5.x and
//!   MariaDB can always answer
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
use std::collections::HashSet;
use thiserror::Error;

mod tree;

pub use tree::parse_tree_explain;

/// `extra` key holding the MySQL-specific operation name ("Table scan",
/// "Index lookup", "Nested loop inner join", ...). The visual plan shows it in
/// place of PostgreSQL's node-type names.
pub const OPERATION_KEY: &str = "operation";

/// Errors that can occur when parsing MySQL EXPLAIN output
#[derive(Debug, Error)]
pub enum MysqlExplainError {
    #[error("Invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("Missing query_block in EXPLAIN output")]
    MissingQueryBlock,

    #[error("Invalid plan structure: {0}")]
    InvalidStructure(String),

    #[error("Unsupported format: expected JSON, tree or tabular format")]
    UnsupportedFormat,

    #[error("Empty EXPLAIN output")]
    EmptyOutput,
}

/// Result type for MySQL EXPLAIN parsing
pub type Result<T> = std::result::Result<T, MysqlExplainError>;

/// Parses MySQL EXPLAIN output (JSON, tree or tabular format)
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
/// // Tree format (EXPLAIN FORMAT=TREE / EXPLAIN ANALYZE)
/// let tree = "-> Limit: 5 row(s)  (cost=10 rows=5)\n    -> Table scan on t  (cost=10 rows=100)";
/// let plan = parse_mysql_explain(tree).unwrap();
/// assert_eq!(plan.root.children.len(), 1);
///
/// // Tabular format
/// let tabular = "1\tSIMPLE\tusers\tNULL\tALL\tNULL\tNULL\tNULL\tNULL\t100\t100.00\tUsing where";
/// let plan = parse_mysql_explain(tabular).unwrap();
/// ```
pub fn parse_mysql_explain(output: &str) -> Result<QueryPlan> {
    let trimmed = output.trim();

    if trimmed.is_empty() {
        return Err(MysqlExplainError::EmptyOutput);
    }

    if trimmed.starts_with('{') {
        parse_json_explain(trimmed)
    } else if tree::looks_like_tree(trimmed) {
        parse_tree_explain(trimmed)
    } else {
        parse_tabular_explain(trimmed)
    }
}

// ----------------------------------------------------------------------------
// Shared helpers
// ----------------------------------------------------------------------------

/// Reads a JSON number or a numeric string. MySQL emits costs as strings and
/// row counts as numbers; MariaDB uses numbers for both.
fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn as_count(value: &Value) -> Option<u64> {
    as_f64(value)
        .filter(|n| n.is_finite() && *n >= 0.0)
        .map(|n| n.round() as u64)
}

fn string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn flag(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn json_number(value: f64) -> Value {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// Names the operation a node performs and what it acts on, in MySQL's own
/// vocabulary. `detail` becomes the node's `description`.
pub(crate) fn set_operation(node: &mut PlanNode, operation: &str, detail: Option<String>) {
    node.extra.insert(
        OPERATION_KEY.to_string(),
        Value::String(operation.to_string()),
    );
    node.description = detail.filter(|detail| !detail.trim().is_empty());
}

/// Operation name for a table access, shared by every EXPLAIN format.
fn access_operation(access_type: &str, covering: bool) -> String {
    let name = match access_type.to_ascii_lowercase().as_str() {
        "all" => "Table scan",
        "index" if covering => "Covering index scan",
        "index" => "Index scan",
        "range" if covering => "Covering index range scan",
        "range" => "Index range scan",
        "ref" if covering => "Covering index lookup",
        "ref" => "Index lookup",
        "ref_or_null" => "Index lookup (or NULL)",
        "eq_ref" | "const" if covering => "Single-row covering index lookup",
        "eq_ref" | "const" => "Single-row index lookup",
        "system" => "Constant row",
        "fulltext" => "Full-text index search",
        "index_merge" => "Index merge",
        "unique_subquery" | "index_subquery" => "Index lookup (subquery)",
        _ => return access_type.to_string(),
    };
    name.to_string()
}

/// Derived tables, CTEs and `<subqueryN>` are scanned from a temporary table
/// the planner already materialized; flagging those as full table scans of a
/// real table would be misleading.
fn is_internal_table(name: &str) -> bool {
    name.starts_with('<')
}

fn table_detail(table: &str, key: Option<&str>, refs: Option<String>) -> String {
    let mut detail = table.to_string();
    if let Some(key) = key {
        detail.push_str(" using ");
        detail.push_str(key);
    }
    if let Some(refs) = refs.filter(|refs| !refs.is_empty()) {
        detail.push_str(&format!(" ({refs})"));
    }
    detail.trim().to_string()
}

fn wrap_dml(kind: DmlKind, table: &str, child: PlanNode) -> PlanNode {
    let (node_type, operation) = match kind {
        DmlKind::Insert => (NodeType::Insert, "Insert into"),
        DmlKind::Update => (NodeType::Update, "Update"),
        DmlKind::Delete => (NodeType::Delete, "Delete from"),
    };
    let mut node = PlanNode::new(node_type).with_relation(table);
    set_operation(&mut node, operation, Some(table.to_string()));
    node.cost = child.cost;
    node.rows = child.rows;
    node.children.push(child);
    node
}

#[derive(Clone, Copy)]
enum DmlKind {
    Insert,
    Update,
    Delete,
}

// ----------------------------------------------------------------------------
// JSON format
// ----------------------------------------------------------------------------

/// Parses MySQL EXPLAIN FORMAT=JSON output
///
/// MySQL JSON EXPLAIN has a different structure than PostgreSQL:
/// - Root contains "query_block" instead of "Plan"
/// - Uses "access_type" for scan types
/// - Nested loops are in "nested_loop" array
/// - Subqueries are in "subqueries" array
///
/// MariaDB's variant (`rows` instead of `rows_examined_per_scan`, `filesort`
/// and `temporary_table` wrappers, no costs) is accepted too.
pub fn parse_json_explain(json: &str) -> Result<QueryPlan> {
    let value: Value = serde_json::from_str(json)?;

    // { "query_block": { "select_id": 1, ... } }
    let query_block = value
        .get("query_block")
        .ok_or(MysqlExplainError::MissingQueryBlock)?;

    let mut root = parse_container(query_block)?;

    let query_cost = query_block
        .get("cost_info")
        .and_then(|cost_info| cost_info.get("query_cost"))
        .and_then(as_f64);
    // The query block's cost is the authoritative total for the whole statement.
    if let Some(query_cost) = query_cost {
        root.cost = Some(NodeCost::new(0.0, query_cost));
    }

    let mut plan = QueryPlan::new(root);
    if query_cost.is_some() {
        plan.total_cost = query_cost;
    }
    Ok(plan)
}

/// Keys that wrap whatever they contain in one more operation.
const WRAPPER_KEYS: &[&str] = &[
    "ordering_operation",
    "grouping_operation",
    "duplicates_removal",
    "windowing",
    "buffer_result",
    // MariaDB spellings
    "filesort",
    "temporary_table",
    "duplicate_removal",
    "window_functions_computation",
];

/// Keys whose presence means an object has plan content of its own.
const CONTENT_KEYS: &[&str] = &["nested_loop", "table", "union_result", "message"];

fn has_content(block: &Value) -> bool {
    CONTENT_KEYS
        .iter()
        .chain(WRAPPER_KEYS)
        .any(|key| block.get(*key).is_some())
}

/// Parses anything that can hold plan content: a `query_block` or the body of
/// an ordering / grouping / windowing / ... operation.
fn parse_container(block: &Value) -> Result<PlanNode> {
    let mut node = parse_container_content(block)?;
    attach_subqueries(&mut node, block);
    Ok(node)
}

fn parse_container_content(block: &Value) -> Result<PlanNode> {
    for key in WRAPPER_KEYS {
        if let Some(inner) = block.get(*key).filter(|inner| inner.is_object()) {
            return parse_wrapper(key, inner);
        }
    }

    if let Some(nested_loop) = block.get("nested_loop") {
        return parse_nested_loop(nested_loop);
    }

    if let Some(table) = block.get("table") {
        return parse_table_access(table);
    }

    if let Some(union_result) = block.get("union_result") {
        return parse_union_result(union_result);
    }

    // "No tables used", "Impossible WHERE", "no matching row in const table"...
    let mut node = PlanNode::new(NodeType::Result);
    let message = block.get("message").and_then(Value::as_str);
    set_operation(&mut node, "Result", message.map(String::from));
    Ok(node)
}

fn parse_wrapper(key: &str, inner: &Value) -> Result<PlanNode> {
    let mut node = match key {
        "ordering_operation" | "filesort" => sort_node(inner),
        "grouping_operation" => aggregate_node(inner),
        "duplicates_removal" | "duplicate_removal" => {
            let mut node = PlanNode::new(NodeType::Unique);
            set_operation(&mut node, "Remove duplicates", None);
            node
        }
        "windowing" | "window_functions_computation" => window_node(inner),
        "temporary_table" => {
            let mut node = PlanNode::new(NodeType::Materialize);
            set_operation(&mut node, "Temporary table", None);
            node.extra
                .insert("using_temporary".to_string(), Value::Bool(true));
            node
        }
        _ => {
            let mut node = PlanNode::new(NodeType::Materialize);
            set_operation(&mut node, "Buffer result", None);
            if flag(inner, "using_temporary_table") {
                node.extra
                    .insert("using_temporary".to_string(), Value::Bool(true));
            }
            node
        }
    };

    if has_content(inner) {
        let child = parse_container(inner)?;
        node.cost = child.cost;
        node.rows = child.rows;
        node.children.push(child);
    }
    Ok(node)
}

fn sort_node(ordering: &Value) -> PlanNode {
    let mut node = PlanNode::new(NodeType::Sort);
    let using_filesort = flag(ordering, "using_filesort") || ordering.get("sort_key").is_some();
    let using_temporary = flag(ordering, "using_temporary_table");

    if using_filesort {
        node.sort_method = Some("filesort".to_string());
        node.extra
            .insert("using_filesort".to_string(), Value::Bool(true));
    }
    if using_temporary {
        node.extra
            .insert("using_temporary_table".to_string(), Value::Bool(true));
    }
    if let Some(sort_key) = ordering.get("sort_key").and_then(Value::as_str) {
        node.sort_keys = vec![sort_key.to_string()];
    }

    let detail = match (using_filesort, using_temporary) {
        (true, true) => Some("filesort, temporary table".to_string()),
        (true, false) => Some("filesort".to_string()),
        (false, true) => Some("temporary table".to_string()),
        (false, false) => None,
    };
    set_operation(&mut node, "Sort", detail);
    node
}

fn aggregate_node(grouping: &Value) -> PlanNode {
    let mut node = PlanNode::new(NodeType::Aggregate);
    node.group_keys = string_list(grouping.get("group_by_columns"));

    let keys = node.group_keys.join(", ");
    if flag(grouping, "using_temporary_table") {
        // Groups are collected in a temporary table: hash aggregation.
        node.node_type = NodeType::HashAggregate;
        set_operation(&mut node, "Aggregate using temporary table", Some(keys));
    } else {
        set_operation(&mut node, "Group", Some(keys));
    }

    if flag(grouping, "using_filesort") {
        node.extra
            .insert("using_filesort".to_string(), Value::Bool(true));
    }
    node
}

fn window_node(windowing: &Value) -> PlanNode {
    let mut node = PlanNode::new(NodeType::WindowAgg);
    let windows = windowing
        .get("windows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let functions: Vec<String> = windows
        .iter()
        .flat_map(|window| string_list(window.get("functions")))
        .collect();
    if windows.iter().any(|window| flag(window, "using_filesort")) {
        node.extra
            .insert("using_filesort".to_string(), Value::Bool(true));
    }
    if windows
        .iter()
        .any(|window| flag(window, "using_temporary_table"))
    {
        node.extra
            .insert("using_temporary".to_string(), Value::Bool(true));
    }
    set_operation(&mut node, "Window aggregate", Some(functions.join(", ")));
    node
}

/// Hangs `subqueries` / `attached_subqueries` of `container` under `node`.
fn attach_subqueries(node: &mut PlanNode, container: &Value) {
    for key in ["subqueries", "attached_subqueries"] {
        let Some(subqueries) = container.get(key).and_then(Value::as_array) else {
            continue;
        };
        for subquery in subqueries {
            let Some(child) = subquery
                .get("query_block")
                .and_then(|query_block| parse_container(query_block).ok())
            else {
                continue;
            };

            let mut wrapper = PlanNode::new(NodeType::SubPlan);
            let detail = match (flag(subquery, "dependent"), flag(subquery, "cacheable")) {
                (true, _) => "dependent, re-run per outer row",
                (false, true) => "run once",
                (false, false) => "not cacheable",
            };
            set_operation(&mut wrapper, "Subquery", Some(detail.to_string()));
            wrapper.cost = child.cost;
            wrapper.rows = child.rows;
            wrapper.children.push(child);
            node.children.push(wrapper);
        }
    }
}

/// Parses a nested_loop array (joins)
fn parse_nested_loop(nested_loop: &Value) -> Result<PlanNode> {
    let items = nested_loop
        .as_array()
        .ok_or_else(|| MysqlExplainError::InvalidStructure("nested_loop is not an array".into()))?;

    if items.is_empty() {
        return Err(MysqlExplainError::InvalidStructure(
            "Empty nested_loop".into(),
        ));
    }

    let mut children: Vec<PlanNode> = Vec::new();
    for item in items {
        if let Some(table) = item.get("table") {
            children.push(parse_table_access(table)?);
        } else if let Some(joined) = item.get("block-nl-join") {
            // MariaDB: the joined table with its join buffer description.
            if let Some(table) = joined.get("table") {
                let mut child = parse_table_access(table)?;
                child
                    .extra
                    .insert("using_join_buffer".to_string(), Value::Bool(true));
                children.push(child);
            }
        } else if has_content(item) {
            children.push(parse_container(item)?);
        }
    }

    if children.is_empty() {
        return Err(MysqlExplainError::InvalidStructure(
            "nested_loop contains no tables".into(),
        ));
    }

    // MySQL lists the join order flat; fold it into the left-deep tree it executes.
    let mut current = children.remove(0);
    for child in children {
        current = join_nodes(current, child);
    }
    Ok(current)
}

/// Joins `outer` with `inner` the way MySQL does: a nested loop, or a hash
/// join when the inner table is read through a hash join buffer.
fn join_nodes(outer: PlanNode, inner: PlanNode) -> PlanNode {
    let mentions_hash = |key: &str| {
        inner
            .extra
            .get(key)
            .and_then(Value::as_str)
            .is_some_and(|text| text.to_ascii_lowercase().contains("hash join"))
    };
    let hash = mentions_hash("using_join_buffer") || mentions_hash("extra_info");

    let mut join = PlanNode::new(if hash {
        NodeType::HashJoin
    } else {
        NodeType::NestedLoop
    });
    join.join_type = Some(JoinType::Inner);
    set_operation(
        &mut join,
        if hash {
            "Inner hash join"
        } else {
            "Nested loop inner join"
        },
        None,
    );

    // `prefix_cost` is cumulative over the join so far, which is exactly the
    // total cost of this join node.
    join.cost = inner
        .extra
        .get("prefix_cost")
        .and_then(as_f64)
        .map(|prefix| NodeCost::new(0.0, prefix))
        .or_else(|| {
            let total = outer.cost?.total + inner.cost?.total;
            Some(NodeCost::new(0.0, total))
        });
    join.rows = inner.extra.get("rows_produced").and_then(as_count);
    join.children.push(outer);
    join.children.push(inner);
    join
}

/// Parses a table access from MySQL JSON
fn parse_table_access(table: &Value) -> Result<PlanNode> {
    let access_type = table
        .get("access_type")
        .and_then(Value::as_str)
        .unwrap_or("ALL");
    let covering = flag(table, "using_index");

    let mut node = PlanNode::new(mysql_access_type_to_node_type(access_type));
    node.extra.insert(
        "mysql_access_type".to_string(),
        Value::String(access_type.to_string()),
    );

    let table_name = table.get("table_name").and_then(Value::as_str);
    if let Some(name) = table_name {
        node.relation = Some(name.to_string());
    }

    // Cost info: read + eval is this table's own cost, prefix is cumulative.
    if let Some(cost_info) = table.get("cost_info") {
        if let Some(read_cost) = cost_info.get("read_cost").and_then(as_f64) {
            let eval_cost = cost_info
                .get("eval_cost")
                .and_then(as_f64)
                .unwrap_or(0.0);
            node.cost = Some(NodeCost::new(0.0, read_cost + eval_cost));
        }
        if let Some(prefix_cost) = cost_info.get("prefix_cost").and_then(as_f64) {
            node.extra
                .insert("prefix_cost".to_string(), json_number(prefix_cost));
        }
    }

    // Rows scanned (MySQL) or estimated (MariaDB)
    node.rows = table
        .get("rows_examined_per_scan")
        .or_else(|| table.get("rows"))
        .and_then(as_count);

    // Rows left after the filter, per the optimizer's estimate (not an actual count).
    if let Some(produced) = table.get("rows_produced_per_join").and_then(as_count) {
        node.extra
            .insert("rows_produced".to_string(), Value::Number(produced.into()));
    }

    if let Some(filtered) = table.get("filtered").and_then(as_f64) {
        node.extra
            .insert("filtered".to_string(), json_number(filtered));
    }

    let key = table
        .get("key")
        .and_then(Value::as_str)
        .filter(|key| !key.eq_ignore_ascii_case("null"));
    if let Some(key) = key {
        node.index_name = Some(key.to_string());
    }

    let possible_keys = string_list(table.get("possible_keys"));
    if !possible_keys.is_empty() {
        node.extra.insert(
            "possible_keys".to_string(),
            Value::Array(possible_keys.into_iter().map(Value::String).collect()),
        );
    }

    if let Some(key_length) = table.get("key_length").and_then(Value::as_str) {
        node.extra.insert(
            "key_length".to_string(),
            Value::String(key_length.to_string()),
        );
    }

    let key_parts = string_list(table.get("used_key_parts"));
    let refs = string_list(table.get("ref"));
    if !refs.is_empty() {
        node.extra.insert(
            "ref".to_string(),
            Value::Array(refs.iter().cloned().map(Value::String).collect()),
        );
    }

    if let Some(condition) = table.get("attached_condition").and_then(Value::as_str) {
        node.filter = Some(condition.to_string());
    }
    if let Some(condition) = table
        .get("index_condition")
        .or_else(|| table.get("pushed_index_condition"))
        .and_then(Value::as_str)
    {
        node.index_cond = Some(condition.to_string());
    }

    if covering {
        // A covering index never reads the table row.
        if node.node_type == NodeType::IndexScan {
            node.node_type = NodeType::IndexOnlyScan;
        }
        node.extra
            .insert("using_index".to_string(), Value::Bool(true));
    }

    if table.get("using_index_for_group_by").is_some() {
        node.extra
            .insert("using_index_for_group_by".to_string(), Value::Bool(true));
    }

    if let Some(buffer) = table.get("using_join_buffer").and_then(Value::as_str) {
        node.extra.insert(
            "using_join_buffer".to_string(),
            Value::String(buffer.to_string()),
        );
    }

    let materialized = table
        .get("materialized_from_subquery")
        .filter(|materialized| materialized.is_object());
    if node.node_type == NodeType::SeqScan
        && (materialized.is_some() || table_name.is_some_and(is_internal_table))
    {
        node.node_type = NodeType::CteScan;
    }

    // key columns paired with what they are matched against: (order_id=o.order_id)
    let key_condition = if key_parts.is_empty() {
        (!refs.is_empty()).then(|| format!("ref={}", refs.join(", ")))
    } else {
        let pairs = key_parts
            .iter()
            .enumerate()
            .map(|(index, part)| match refs.get(index) {
                Some(reference) => format!("{part}={reference}"),
                None => part.clone(),
            })
            .collect::<Vec<_>>();
        Some(pairs.join(", "))
    };
    set_operation(
        &mut node,
        &access_operation(access_type, covering),
        Some(table_detail(
            table_name.unwrap_or_default(),
            key,
            key_condition,
        )),
    );

    if let Some(materialized) = materialized
        && let Some(query_block) = materialized.get("query_block")
        && let Ok(child) = parse_container(query_block)
    {
        let mut materialize = PlanNode::new(NodeType::Materialize);
        set_operation(&mut materialize, "Materialize", None);
        materialize
            .extra
            .insert("using_temporary".to_string(), Value::Bool(true));
        materialize.cost = child.cost;
        materialize.rows = child.rows;
        materialize.children.push(child);
        // Reading the temporary table costs what building it did, when MySQL says nothing else.
        node.cost = node.cost.or(materialize.cost);
        node.children.push(materialize);
    }

    attach_subqueries(&mut node, table);

    // UPDATE / DELETE / INSERT statements explain as a table with a marker.
    let dml = [
        ("update", DmlKind::Update),
        ("delete", DmlKind::Delete),
        ("insert", DmlKind::Insert),
    ]
    .into_iter()
    .find(|(key, _)| flag(table, key))
    .map(|(_, kind)| kind);
    if let Some(kind) = dml {
        return Ok(wrap_dml(kind, table_name.unwrap_or_default(), node));
    }

    Ok(node)
}

/// Parses union_result
fn parse_union_result(union: &Value) -> Result<PlanNode> {
    let mut node = PlanNode::new(NodeType::Append);
    set_operation(
        &mut node,
        "Union",
        union
            .get("table_name")
            .and_then(Value::as_str)
            .map(String::from),
    );

    if flag(union, "using_temporary_table") {
        node.extra
            .insert("using_temporary_table".to_string(), Value::Bool(true));
    }

    // The members of the UNION
    if let Some(query_specs) = union.get("query_specifications").and_then(Value::as_array) {
        for spec in query_specs {
            if let Some(query_block) = spec.get("query_block")
                && let Ok(child) = parse_container(query_block)
            {
                node.children.push(child);
            }
        }
    }

    let totals: Option<Vec<f64>> = node
        .children
        .iter()
        .map(|child| child.cost.map(|cost| cost.total))
        .collect();
    if let Some(totals) = totals.filter(|totals| !totals.is_empty()) {
        node.cost = Some(NodeCost::new(0.0, totals.iter().sum()));
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

// ----------------------------------------------------------------------------
// Tabular format
// ----------------------------------------------------------------------------

/// Parses MySQL traditional tabular EXPLAIN output
///
/// The columns are `id | select_type | table | partitions | type |
/// possible_keys | key | key_len | ref | rows | filtered | Extra`. With a header
/// line the columns are looked up by name, so the `partitions` and `filtered`
/// columns may be present or not. Without one the layout is guessed from the
/// column count. Cells are tab-separated (a `|`-bordered table works too).
pub fn parse_tabular_explain(text: &str) -> Result<QueryPlan> {
    let lines: Vec<&str> = text
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with('+'))
        .collect();

    if lines.is_empty() {
        return Err(MysqlExplainError::EmptyOutput);
    }

    let mut header: Option<Columns> = None;
    let mut rows: Vec<TabularRow> = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let cells = split_cells(line);
        if index == 0
            && let Some(columns) = Columns::from_header(&cells)
        {
            header = Some(columns);
            continue;
        }
        let columns = header.unwrap_or_else(|| Columns::positional(cells.len()));
        if let Some(row) = columns.row(&cells) {
            rows.push(row);
        }
    }

    if rows.is_empty() {
        return Err(if header.is_some() && lines.len() == 1 {
            MysqlExplainError::EmptyOutput
        } else {
            MysqlExplainError::InvalidStructure("No valid rows found".into())
        });
    }

    let root = build_plan_from_rows(&rows)?;
    Ok(QueryPlan::new(root))
}

fn split_cells(line: &str) -> Vec<String> {
    if line.contains('\t') {
        return line.split('\t').map(|cell| cell.trim().to_string()).collect();
    }
    // `| 1 | SIMPLE | users | ...` bordered table
    let line = line.trim().trim_start_matches('|').trim_end_matches('|');
    line.split('|').map(|cell| cell.trim().to_string()).collect()
}

/// Column positions of an EXPLAIN table.
#[derive(Clone, Copy)]
struct Columns {
    id: usize,
    select_type: usize,
    table: usize,
    access_type: usize,
    possible_keys: usize,
    key: usize,
    key_len: usize,
    reference: usize,
    rows: usize,
    filtered: Option<usize>,
    extra: Option<usize>,
}

impl Columns {
    fn from_header(cells: &[String]) -> Option<Self> {
        let find = |name: &str| cells.iter().position(|cell| cell.eq_ignore_ascii_case(name));
        let select_type = find("select_type")?;
        Some(Self {
            id: find("id")?,
            select_type,
            table: find("table")?,
            access_type: find("type")?,
            possible_keys: find("possible_keys")?,
            key: find("key")?,
            key_len: find("key_len")?,
            reference: find("ref")?,
            rows: find("rows")?,
            filtered: find("filtered"),
            extra: find("extra"),
        })
    }

    /// Without a header: 12 columns means `partitions` is present (MySQL 5.7+),
    /// 11 is the classic `EXTENDED` layout, 10 has no `filtered`.
    fn positional(column_count: usize) -> Self {
        let shift = usize::from(column_count >= 12);
        let (filtered, extra) = match column_count {
            11.. => (Some(9 + shift), Some(10 + shift)),
            10 => (None, Some(9)),
            _ => (None, None),
        };
        Self {
            id: 0,
            select_type: 1,
            table: 2,
            access_type: 3 + shift,
            possible_keys: 4 + shift,
            key: 5 + shift,
            key_len: 6 + shift,
            reference: 7 + shift,
            rows: 8 + shift,
            filtered,
            extra,
        }
    }

    fn row(&self, cells: &[String]) -> Option<TabularRow> {
        // id, select_type, table and type are the least a real row has
        if cells.len() < 4 {
            return None;
        }
        let cell = |index: usize| {
            cells
                .get(index)
                .map(String::as_str)
                .filter(|cell| !cell.is_empty() && !cell.eq_ignore_ascii_case("null"))
                .map(String::from)
        };
        let select_type = cell(self.select_type)?;
        let id = cell(self.id).and_then(|id| id.parse().ok());
        // Only UNION RESULT rows legitimately have no id.
        if id.is_none() && !select_type.eq_ignore_ascii_case("UNION RESULT") {
            return None;
        }

        Some(TabularRow {
            id,
            select_type,
            table: cell(self.table),
            access_type: cell(self.access_type),
            possible_keys: cell(self.possible_keys),
            key: cell(self.key),
            key_len: cell(self.key_len),
            reference: cell(self.reference),
            rows: cell(self.rows).and_then(|rows| rows.parse().ok()),
            filtered: self
                .filtered
                .and_then(cell)
                .and_then(|filtered| filtered.parse().ok()),
            extra: self.extra.and_then(cell),
        })
    }
}

/// Represents a row from tabular EXPLAIN output
struct TabularRow {
    id: Option<u32>,
    select_type: String,
    table: Option<String>,
    access_type: Option<String>,
    possible_keys: Option<String>,
    key: Option<String>,
    key_len: Option<String>,
    reference: Option<String>,
    rows: Option<u64>,
    filtered: Option<f64>,
    extra: Option<String>,
}

fn is_union_result(row: &TabularRow) -> bool {
    row.select_type.eq_ignore_ascii_case("UNION RESULT")
}

/// Ids a table name like `<derived2>`, `<subquery3>` or `<union1,2>` refers to.
fn referenced_ids(table: &str) -> Vec<u32> {
    let Some(inner) = table.strip_prefix('<').and_then(|t| t.strip_suffix('>')) else {
        return Vec::new();
    };
    let digits = inner.trim_start_matches(|c: char| c.is_ascii_alphabetic());
    digits
        .split(',')
        .filter_map(|id| id.trim().parse().ok())
        .collect()
}

fn row_node(row: &TabularRow) -> PlanNode {
    let extra_items: Vec<String> = row
        .extra
        .as_deref()
        .map(|extra| extra.split(';').map(|item| item.trim().to_string()).collect())
        .unwrap_or_default();
    let has_item = |wanted: &str| extra_items.iter().any(|item| item.eq_ignore_ascii_case(wanted));
    let starts_with_item = |prefix: &str| {
        extra_items
            .iter()
            .any(|item| item.to_ascii_lowercase().starts_with(prefix))
    };

    let (Some(table), Some(access_type)) = (row.table.as_deref(), row.access_type.as_deref())
    else {
        // "No tables used", "Impossible WHERE", "no matching row in const table"...
        let mut node = PlanNode::new(NodeType::Result);
        set_operation(&mut node, "Result", row.extra.clone());
        return node;
    };

    let covering = has_item("Using index") || has_item("Using index for group-by");
    let mut node = PlanNode::new(mysql_access_type_to_node_type(access_type));
    node.relation = Some(table.to_string());
    node.extra.insert(
        "mysql_access_type".to_string(),
        Value::String(access_type.to_string()),
    );

    if covering && node.node_type == NodeType::IndexScan {
        node.node_type = NodeType::IndexOnlyScan;
    }
    if is_internal_table(table) && node.node_type == NodeType::SeqScan {
        node.node_type = NodeType::CteScan;
    }

    node.index_name = row.key.clone();
    node.rows = row.rows;
    if let Some(filtered) = row.filtered {
        node.extra
            .insert("filtered".to_string(), json_number(filtered));
    }
    if let Some(possible_keys) = &row.possible_keys {
        node.extra.insert(
            "possible_keys".to_string(),
            Value::String(possible_keys.clone()),
        );
    }
    if let Some(key_len) = &row.key_len {
        node.extra
            .insert("key_length".to_string(), Value::String(key_len.clone()));
    }
    if let Some(reference) = &row.reference {
        node.extra
            .insert("ref".to_string(), Value::String(reference.clone()));
    }
    node.extra.insert(
        "select_type".to_string(),
        Value::String(row.select_type.clone()),
    );
    if let Some(id) = row.id {
        node.extra
            .insert("select_id".to_string(), Value::Number(id.into()));
    }

    let flags = [
        ("using_index", covering),
        ("using_where", has_item("Using where")),
        ("using_filesort", has_item("Using filesort")),
        ("using_temporary", has_item("Using temporary")),
        ("using_join_buffer", starts_with_item("using join buffer")),
        (
            "range_checked_for_each_record",
            starts_with_item("range checked for each record"),
        ),
        ("using_index_condition", has_item("Using index condition")),
        ("using_mrr", has_item("Using MRR")),
        (
            "using_index_for_group_by",
            has_item("Using index for group-by"),
        ),
    ];
    for (name, set) in flags {
        if set {
            node.extra.insert(name.to_string(), Value::Bool(true));
        }
    }
    if let Some(extra) = &row.extra {
        node.extra
            .insert("extra_info".to_string(), Value::String(extra.clone()));
    }

    let refs = row.reference.as_ref().map(|reference| format!("ref={reference}"));
    set_operation(
        &mut node,
        &access_operation(access_type, covering),
        Some(table_detail(table, row.key.as_deref(), refs)),
    );
    node
}

/// Builds the plan tree for the classic table: rows sharing an id are one
/// join, and `<derivedN>` / `<subqueryN>` / `<unionN,M>` tables point at the
/// groups whose results they read.
struct PlanBuilder<'a> {
    rows: &'a [TabularRow],
    visited: HashSet<u32>,
}

impl PlanBuilder<'_> {
    fn group_ids(&self) -> Vec<u32> {
        let mut ids = Vec::new();
        for id in self
            .rows
            .iter()
            .filter(|row| !is_union_result(row))
            .filter_map(|row| row.id)
        {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        ids
    }

    fn build_group(&mut self, id: u32) -> Option<PlanNode> {
        if !self.visited.insert(id) {
            return None;
        }

        let rows = self.rows;
        let mut nodes = Vec::new();
        for row in rows
            .iter()
            .filter(|row| row.id == Some(id) && !is_union_result(row))
        {
            let mut node = row_node(row);

            if let Some(table) = row.table.as_deref() {
                for referenced in referenced_ids(table) {
                    if let Some(child) = self.build_group(referenced) {
                        let mut materialize = PlanNode::new(NodeType::Materialize);
                        set_operation(&mut materialize, "Materialize", None);
                        materialize
                            .extra
                            .insert("using_temporary".to_string(), Value::Bool(true));
                        materialize.children.push(child);
                        node.children.push(materialize);
                    }
                }
            }

            let node = match (row.select_type.to_ascii_uppercase().as_str(), row.table.as_deref()) {
                ("UPDATE", Some(table)) => wrap_dml(DmlKind::Update, table, node),
                ("DELETE", Some(table)) => wrap_dml(DmlKind::Delete, table, node),
                ("INSERT", Some(table)) => wrap_dml(DmlKind::Insert, table, node),
                _ => node,
            };
            nodes.push(node);
        }

        let mut nodes = nodes.into_iter();
        let mut current = nodes.next()?;
        for node in nodes {
            current = join_nodes(current, node);
        }
        Some(current)
    }

    fn build(mut self) -> Result<PlanNode> {
        let ids = self.group_ids();

        // A UNION RESULT row is the root: its members hang below it.
        let union_row = self.rows.iter().find(|row| is_union_result(row));

        let mut root = if let Some(union_row) = union_row {
            let mut node = PlanNode::new(NodeType::Append);
            set_operation(&mut node, "Union", union_row.table.clone());
            node.extra
                .insert("using_temporary".to_string(), Value::Bool(true));
            if let Some(table) = union_row.table.as_deref() {
                for member in referenced_ids(table) {
                    if let Some(child) = self.build_group(member) {
                        node.children.push(child);
                    }
                }
            }
            node
        } else {
            let first = ids
                .first()
                .copied()
                .ok_or_else(|| MysqlExplainError::InvalidStructure("No rows".into()))?;
            self.build_group(first)
                .ok_or_else(|| MysqlExplainError::InvalidStructure("No rows".into()))?
        };

        // Subqueries nothing refers to by name (WHERE / SELECT list): attach
        // them to the part of the plan that evaluates them.
        for id in ids {
            let select_type = self
                .rows
                .iter()
                .find(|row| row.id == Some(id))
                .map(|row| row.select_type.clone())
                .unwrap_or_default();
            let Some(child) = self.build_group(id) else {
                continue;
            };
            let mut subplan = PlanNode::new(NodeType::SubPlan);
            set_operation(
                &mut subplan,
                "Subquery",
                Some(format!("#{id} ({})", select_type.to_lowercase())),
            );
            subplan.children.push(child);
            attach_target(&mut root).children.push(subplan);
        }

        Ok(root)
    }
}

/// The node subqueries are evaluated at: the last table of the outermost join.
fn attach_target(node: &mut PlanNode) -> &mut PlanNode {
    if node.is_join() && !node.children.is_empty() {
        let last = node.children.len() - 1;
        attach_target(&mut node.children[last])
    } else {
        node
    }
}

/// Builds a plan tree from tabular rows
fn build_plan_from_rows(rows: &[TabularRow]) -> Result<PlanNode> {
    if rows.is_empty() {
        return Err(MysqlExplainError::InvalidStructure("No rows".into()));
    }
    PlanBuilder {
        rows,
        visited: HashSet::new(),
    }
    .build()
}

#[cfg(test)]
mod tests;
