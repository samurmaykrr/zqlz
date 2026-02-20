//! PostgreSQL EXPLAIN Parser
//!
//! Parses EXPLAIN output from PostgreSQL in various formats:
//! - JSON format (EXPLAIN (FORMAT JSON))
//! - Text format (default EXPLAIN)
//!
//! # Examples
//!
//! ```
//! use zqlz_analyzer::explain::postgres::parse_postgres_explain;
//!
//! let json_output = r#"[
//!   {
//!     "Plan": {
//!       "Node Type": "Seq Scan",
//!       "Relation Name": "users",
//!       "Startup Cost": 0.0,
//!       "Total Cost": 10.0,
//!       "Plan Rows": 100,
//!       "Plan Width": 36
//!     }
//!   }
//! ]"#;
//!
//! let plan = parse_postgres_explain(json_output).unwrap();
//! assert!(plan.has_sequential_scans());
//! ```

use crate::explain::plan::{ActualTime, JoinType, NodeCost, NodeType, PlanNode, QueryPlan};
use serde_json::Value;
use thiserror::Error;

/// Errors that can occur when parsing PostgreSQL EXPLAIN output
#[derive(Debug, Error)]
pub enum PostgresExplainError {
    #[error("Invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("Missing Plan object in EXPLAIN output")]
    MissingPlan,

    #[error("Invalid plan structure: {0}")]
    InvalidStructure(String),

    #[error("Unsupported format: expected JSON array or text")]
    UnsupportedFormat,
}

/// Result type for PostgreSQL EXPLAIN parsing
pub type Result<T> = std::result::Result<T, PostgresExplainError>;

/// Parses PostgreSQL EXPLAIN output (JSON or text format)
///
/// Automatically detects the format based on the input.
pub fn parse_postgres_explain(output: &str) -> Result<QueryPlan> {
    let trimmed = output.trim();

    if trimmed.starts_with('[') || trimmed.starts_with('{') {
        parse_json_explain(trimmed)
    } else {
        parse_text_explain(trimmed)
    }
}

/// Parses PostgreSQL EXPLAIN (FORMAT JSON) output
pub fn parse_json_explain(json: &str) -> Result<QueryPlan> {
    let value: Value = serde_json::from_str(json)?;

    // PostgreSQL JSON EXPLAIN wraps the plan in an array
    let plan_obj = if let Some(arr) = value.as_array() {
        arr.first()
            .and_then(|v| v.get("Plan"))
            .ok_or(PostgresExplainError::MissingPlan)?
    } else if let Some(plan) = value.get("Plan") {
        plan
    } else {
        return Err(PostgresExplainError::MissingPlan);
    };

    let root = parse_plan_node(plan_obj)?;
    let mut plan = QueryPlan::new(root);

    // Extract timing information if available (from EXPLAIN ANALYZE)
    if let Some(arr) = value.as_array()
        && let Some(first) = arr.first()
    {
        if let Some(planning) = first.get("Planning Time").and_then(|v| v.as_f64()) {
            plan.planning_time_ms = Some(planning);
        }
        if let Some(execution) = first.get("Execution Time").and_then(|v| v.as_f64()) {
            plan.execution_time_ms = Some(execution);
        }
    }

    Ok(plan)
}

/// Parses a single plan node from JSON
fn parse_plan_node(value: &Value) -> Result<PlanNode> {
    let node_type_str = value
        .get("Node Type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PostgresExplainError::InvalidStructure("Missing Node Type".into()))?;

    let node_type = NodeType::from_postgres_str(node_type_str);
    let mut node = PlanNode::new(node_type);

    // Basic properties
    if let Some(rel) = value.get("Relation Name").and_then(|v| v.as_str()) {
        node.relation = Some(rel.to_string());
    }

    if let Some(schema) = value.get("Schema").and_then(|v| v.as_str()) {
        node.schema = Some(schema.to_string());
    }

    if let Some(alias) = value.get("Alias").and_then(|v| v.as_str()) {
        node.alias = Some(alias.to_string());
    }

    // Cost information
    let startup_cost = value.get("Startup Cost").and_then(|v| v.as_f64());
    let total_cost = value.get("Total Cost").and_then(|v| v.as_f64());
    if let (Some(startup), Some(total)) = (startup_cost, total_cost) {
        node.cost = Some(NodeCost::new(startup, total));
    }

    // Row estimates
    if let Some(rows) = value.get("Plan Rows").and_then(|v| v.as_u64()) {
        node.rows = Some(rows);
    }

    if let Some(width) = value.get("Plan Width").and_then(|v| v.as_u64()) {
        node.width = Some(width as u32);
    }

    // Actual values from EXPLAIN ANALYZE
    if let Some(rows) = value.get("Actual Rows").and_then(|v| v.as_u64()) {
        node.actual_rows = Some(rows);
    }

    let actual_startup = value.get("Actual Startup Time").and_then(|v| v.as_f64());
    let actual_total = value.get("Actual Total Time").and_then(|v| v.as_f64());
    if let (Some(startup), Some(total)) = (actual_startup, actual_total) {
        node.actual_time_ms = Some(ActualTime::new(startup, total));
    }

    if let Some(loops) = value.get("Actual Loops").and_then(|v| v.as_u64()) {
        node.loops = Some(loops);
    }

    // Filter information
    if let Some(filter) = value.get("Filter").and_then(|v| v.as_str()) {
        node.filter = Some(filter.to_string());
    }

    if let Some(removed) = value.get("Rows Removed by Filter").and_then(|v| v.as_u64()) {
        node.rows_removed_by_filter = Some(removed);
    }

    // Index information
    if let Some(idx) = value.get("Index Name").and_then(|v| v.as_str()) {
        node.index_name = Some(idx.to_string());
    }

    if let Some(cond) = value.get("Index Cond").and_then(|v| v.as_str()) {
        node.index_cond = Some(cond.to_string());
    }

    // Join information
    if let Some(join_str) = value.get("Join Type").and_then(|v| v.as_str()) {
        node.join_type = JoinType::parse(join_str);
    }

    if let Some(cond) = value.get("Join Filter").and_then(|v| v.as_str()) {
        node.join_cond = Some(cond.to_string());
    } else if let Some(cond) = value.get("Hash Cond").and_then(|v| v.as_str()) {
        node.join_cond = Some(cond.to_string());
    } else if let Some(cond) = value.get("Merge Cond").and_then(|v| v.as_str()) {
        node.join_cond = Some(cond.to_string());
    }

    // Sort information
    if let Some(keys) = value.get("Sort Key").and_then(|v| v.as_array()) {
        node.sort_keys = keys
            .iter()
            .filter_map(|k| k.as_str().map(String::from))
            .collect();
    }

    if let Some(method) = value.get("Sort Method").and_then(|v| v.as_str()) {
        node.sort_method = Some(method.to_string());
    }

    if let Some(mem) = value.get("Sort Space Used").and_then(|v| v.as_u64()) {
        node.memory_used_kb = Some(mem);
    }

    // Hash information
    if let Some(buckets) = value.get("Hash Buckets").and_then(|v| v.as_u64()) {
        node.hash_buckets = Some(buckets);
    }

    if let Some(batches) = value.get("Hash Batches").and_then(|v| v.as_u64()) {
        node.hash_batches = Some(batches);
    }

    // Group keys
    if let Some(keys) = value.get("Group Key").and_then(|v| v.as_array()) {
        node.group_keys = keys
            .iter()
            .filter_map(|k| k.as_str().map(String::from))
            .collect();
    }

    // Output columns
    if let Some(output) = value.get("Output").and_then(|v| v.as_array()) {
        node.output = output
            .iter()
            .filter_map(|k| k.as_str().map(String::from))
            .collect();
    }

    // Parse child plans
    if let Some(plans) = value.get("Plans").and_then(|v| v.as_array()) {
        for child_value in plans {
            let child = parse_plan_node(child_value)?;
            node.children.push(child);
        }
    }

    // Store any extra properties we haven't explicitly handled
    if let Some(obj) = value.as_object() {
        for (key, val) in obj {
            let known_keys = [
                "Node Type",
                "Relation Name",
                "Schema",
                "Alias",
                "Startup Cost",
                "Total Cost",
                "Plan Rows",
                "Plan Width",
                "Actual Rows",
                "Actual Startup Time",
                "Actual Total Time",
                "Actual Loops",
                "Filter",
                "Rows Removed by Filter",
                "Index Name",
                "Index Cond",
                "Join Type",
                "Join Filter",
                "Hash Cond",
                "Merge Cond",
                "Sort Key",
                "Sort Method",
                "Sort Space Used",
                "Hash Buckets",
                "Hash Batches",
                "Group Key",
                "Output",
                "Plans",
            ];
            if !known_keys.contains(&key.as_str()) {
                node.extra.insert(key.clone(), val.clone());
            }
        }
    }

    Ok(node)
}

/// Parses PostgreSQL text-format EXPLAIN output
///
/// This is a basic parser for the default text output format.
/// For full fidelity, use JSON format instead.
pub fn parse_text_explain(text: &str) -> Result<QueryPlan> {
    let lines: Vec<&str> = text.lines().collect();
    if lines.is_empty() {
        return Err(PostgresExplainError::InvalidStructure(
            "Empty EXPLAIN output".into(),
        ));
    }

    let (root, _) = parse_text_node(&lines, 0, 0)?;
    let mut plan = QueryPlan::new(root);

    // Look for timing info at the end
    for line in lines.iter().rev().take(5) {
        if line.trim().starts_with("Planning Time:") || line.trim().starts_with("Planning time:") {
            if let Some(ms) = extract_time_ms(line) {
                plan.planning_time_ms = Some(ms);
            }
        } else if (line.trim().starts_with("Execution Time:")
            || line.trim().starts_with("Execution time:"))
            && let Some(ms) = extract_time_ms(line)
        {
            plan.execution_time_ms = Some(ms);
        }
    }

    Ok(plan)
}

/// Parses a single node from text format, returns the node and the next line index
#[allow(clippy::only_used_in_recursion)]
fn parse_text_node(
    lines: &[&str],
    start: usize,
    parent_indent: usize,
) -> Result<(PlanNode, usize)> {
    if start >= lines.len() {
        return Err(PostgresExplainError::InvalidStructure(
            "Unexpected end of input".into(),
        ));
    }

    let line = lines[start];
    let indent = count_indent(line);
    let content = line.trim();

    // Skip empty lines or non-plan lines
    if content.is_empty()
        || content.starts_with("Planning Time")
        || content.starts_with("Execution Time")
        || content.starts_with("Planning time")
        || content.starts_with("Execution time")
    {
        if start + 1 < lines.len() {
            return parse_text_node(lines, start + 1, parent_indent);
        } else {
            return Err(PostgresExplainError::InvalidStructure(
                "No plan nodes found".into(),
            ));
        }
    }

    // Parse the node type and properties from the line
    let node = parse_text_line(content)?;
    let mut result = node;

    // Parse child nodes (lines with greater indentation)
    let mut next = start + 1;
    while next < lines.len() {
        let next_line = lines[next];
        let next_indent = count_indent(next_line);
        let next_content = next_line.trim();

        // Skip empty lines and timing info
        if next_content.is_empty()
            || next_content.starts_with("Planning")
            || next_content.starts_with("Execution")
        {
            next += 1;
            continue;
        }

        // If less or equal indentation, we're done with children
        if next_indent <= indent {
            break;
        }

        // Check if this is a child node (starts with ->)
        if next_content.starts_with("->") {
            let (child, after_child) = parse_text_node(lines, next, indent)?;
            result.children.push(child);
            next = after_child;
        } else {
            // It's an additional property line, skip for now
            next += 1;
        }
    }

    Ok((result, next))
}

/// Parses a single text line into a PlanNode
fn parse_text_line(line: &str) -> Result<PlanNode> {
    // Remove leading arrow if present
    let content = line.trim_start_matches("->").trim();

    // Try to extract node type and properties
    // Format: "Node Type on relation  (cost=X..Y rows=N width=W) (actual time=X..Y rows=N loops=L)"

    // Find the cost section
    let (type_part, cost_part) = if let Some(idx) = content.find("  (cost=") {
        (&content[..idx], Some(&content[idx..]))
    } else if let Some(idx) = content.find(" (cost=") {
        (&content[..idx], Some(&content[idx..]))
    } else {
        (content, None)
    };

    // Parse node type and relation
    let (node_type, relation, index_name) = parse_type_and_relation(type_part);
    let mut node = PlanNode::new(node_type);
    node.relation = relation;
    node.index_name = index_name;

    // Parse cost if present
    if let Some(cost_str) = cost_part {
        parse_cost_section(&mut node, cost_str);
    }

    Ok(node)
}

/// Parses node type and relation from the text portion
fn parse_type_and_relation(text: &str) -> (NodeType, Option<String>, Option<String>) {
    // Common patterns:
    // "Seq Scan on users"
    // "Index Scan using users_pkey on users"
    // "Hash Join"
    // "Nested Loop Left Join"

    let parts: Vec<&str> = text.split_whitespace().collect();

    // Try to identify the node type
    let mut node_type = NodeType::Unknown;
    let mut relation = None;
    let mut index_name = None;
    let mut type_end_idx = 0;

    // Check for common node types
    if text.starts_with("Seq Scan") {
        node_type = NodeType::SeqScan;
        type_end_idx = 2;
    } else if text.starts_with("Index Scan") {
        node_type = NodeType::IndexScan;
        type_end_idx = 2;
    } else if text.starts_with("Index Only Scan") {
        node_type = NodeType::IndexOnlyScan;
        type_end_idx = 3;
    } else if text.starts_with("Bitmap Index Scan") {
        node_type = NodeType::BitmapIndexScan;
        type_end_idx = 3;
    } else if text.starts_with("Bitmap Heap Scan") {
        node_type = NodeType::BitmapHeapScan;
        type_end_idx = 3;
    } else if text.starts_with("Nested Loop") {
        node_type = NodeType::NestedLoop;
        type_end_idx = 2;
    } else if text.starts_with("Hash Join") {
        node_type = NodeType::HashJoin;
        type_end_idx = 2;
    } else if text.starts_with("Merge Join") {
        node_type = NodeType::MergeJoin;
        type_end_idx = 2;
    } else if text.starts_with("Hash") && !text.starts_with("Hash Join") {
        node_type = NodeType::Hash;
        type_end_idx = 1;
    } else if text.starts_with("Sort") {
        node_type = NodeType::Sort;
        type_end_idx = 1;
    } else if text.starts_with("Aggregate") {
        node_type = NodeType::Aggregate;
        type_end_idx = 1;
    } else if text.starts_with("HashAggregate") || text.starts_with("Hash Aggregate") {
        node_type = NodeType::HashAggregate;
        type_end_idx = if text.starts_with("Hash Aggregate") {
            2
        } else {
            1
        };
    } else if text.starts_with("GroupAggregate") || text.starts_with("Group Aggregate") {
        node_type = NodeType::GroupAggregate;
        type_end_idx = if text.starts_with("Group Aggregate") {
            2
        } else {
            1
        };
    } else if text.starts_with("Limit") {
        node_type = NodeType::Limit;
        type_end_idx = 1;
    } else if text.starts_with("Append") {
        node_type = NodeType::Append;
        type_end_idx = 1;
    } else if text.starts_with("Materialize") {
        node_type = NodeType::Materialize;
        type_end_idx = 1;
    } else if text.starts_with("Result") {
        node_type = NodeType::Result;
        type_end_idx = 1;
    } else if text.starts_with("Gather") {
        node_type = NodeType::Gather;
        type_end_idx = 1;
    } else if text.starts_with("CTE Scan") {
        node_type = NodeType::CteScan;
        type_end_idx = 2;
    } else if text.starts_with("Unique") {
        node_type = NodeType::Unique;
        type_end_idx = 1;
    } else if text.starts_with("WindowAgg") || text.starts_with("Window Aggregate") {
        node_type = NodeType::WindowAgg;
        type_end_idx = if text.starts_with("Window Aggregate") {
            2
        } else {
            1
        };
    }

    // Look for "on relation" or "using index on relation"
    let remaining: Vec<&str> = parts.iter().skip(type_end_idx).cloned().collect();

    if let Some(on_idx) = remaining.iter().position(|&s| s == "on") {
        if on_idx + 1 < remaining.len() {
            relation = Some(remaining[on_idx + 1].to_string());
        }
        // Check for "using index"
        if let Some(using_idx) = remaining[..on_idx].iter().position(|&s| s == "using")
            && using_idx + 1 < on_idx
        {
            index_name = Some(remaining[using_idx + 1].to_string());
        }
    }

    (node_type, relation, index_name)
}

/// Parses the cost section from text format
fn parse_cost_section(node: &mut PlanNode, cost_str: &str) {
    // Format: "(cost=0.00..10.00 rows=100 width=36) (actual time=0.01..0.05 rows=50 loops=1)"

    // Parse estimated cost
    if let Some(cost_match) = extract_between(cost_str, "cost=", "..")
        && let Ok(startup) = cost_match.parse::<f64>()
        && let Some(total_match) = extract_between(cost_str, "..", " rows=")
        && let Ok(total) = total_match.parse::<f64>()
    {
        node.cost = Some(NodeCost::new(startup, total));
    }

    // Parse estimated rows
    if let Some(rows_match) = extract_between(cost_str, "rows=", " width=")
        && let Ok(rows) = rows_match.parse::<u64>()
    {
        node.rows = Some(rows);
    } else if let Some(rows_match) = extract_between(cost_str, "rows=", ")")
        && let Ok(rows) = rows_match.parse::<u64>()
    {
        node.rows = Some(rows);
    }

    // Parse width
    if let Some(width_match) = extract_between(cost_str, "width=", ")")
        && let Ok(width) = width_match.parse::<u32>()
    {
        node.width = Some(width);
    }

    // Parse actual time (from ANALYZE)
    if let Some(actual_start) = extract_between(cost_str, "actual time=", "..")
        && let Ok(startup) = actual_start.parse::<f64>()
        && extract_between(cost_str, "..", " rows=").is_some()
    {
        // Skip the first ".." which is for estimated cost
        let second_dotdot = cost_str.find("actual time=").and_then(|idx| {
            let after = &cost_str[idx..];
            after.find("..").map(|i| idx + i)
        });
        if let Some(idx) = second_dotdot {
            let after_dotdot = &cost_str[idx + 2..];
            if let Some(end_idx) = after_dotdot.find(" rows=")
                && let Ok(total) = after_dotdot[..end_idx].trim().parse::<f64>()
            {
                node.actual_time_ms = Some(ActualTime::new(startup, total));
            }
        }
    }

    // Parse actual rows
    if cost_str.contains("actual") {
        // Find the second "rows=" (after "actual")
        if let Some(actual_idx) = cost_str.find("actual") {
            let after_actual = &cost_str[actual_idx..];
            if let Some(rows_match) = extract_between(after_actual, "rows=", " loops=")
                && let Ok(rows) = rows_match.parse::<u64>()
            {
                node.actual_rows = Some(rows);
            } else if let Some(rows_match) = extract_between(after_actual, "rows=", ")")
                && let Ok(rows) = rows_match.parse::<u64>()
            {
                node.actual_rows = Some(rows);
            }
        }
    }

    // Parse loops
    if let Some(loops_match) = extract_between(cost_str, "loops=", ")")
        && let Ok(loops) = loops_match.parse::<u64>()
    {
        node.loops = Some(loops);
    }
}

/// Helper to extract text between two markers
fn extract_between<'a>(s: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let start_idx = s.find(start)? + start.len();
    let end_idx = s[start_idx..].find(end)? + start_idx;
    Some(&s[start_idx..end_idx])
}

/// Helper to count leading spaces (indentation)
fn count_indent(s: &str) -> usize {
    s.len() - s.trim_start().len()
}

/// Helper to extract time in ms from a line like "Planning Time: 0.123 ms"
fn extract_time_ms(line: &str) -> Option<f64> {
    let parts: Vec<&str> = line.split(':').collect();
    if parts.len() >= 2 {
        let value_part = parts[1].trim().trim_end_matches("ms").trim();
        value_part.parse().ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
