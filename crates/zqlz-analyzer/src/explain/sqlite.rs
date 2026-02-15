//! SQLite EXPLAIN QUERY PLAN Parser
//!
//! Parses EXPLAIN QUERY PLAN output from SQLite.
//!
//! SQLite's EXPLAIN QUERY PLAN outputs a tree-like structure showing how
//! tables are accessed and joined. The format is:
//!
//! ```text
//! QUERY PLAN
//! |--SCAN users
//! |--SEARCH orders USING INDEX idx_user_id (user_id=?)
//! `--USE TEMP B-TREE FOR ORDER BY
//! ```
//!
//! SQLite 3.24+ added a more structured format with selectid, order, from, detail columns.
//!
//! # Examples
//!
//! ```
//! use zqlz_analyzer::explain::sqlite::parse_sqlite_explain;
//!
//! let output = r#"QUERY PLAN
//! |--SCAN users
//! `--SEARCH orders USING INDEX idx_user_id (user_id=?)"#;
//!
//! let plan = parse_sqlite_explain(output).unwrap();
//! assert!(plan.has_sequential_scans());
//! ```

use crate::explain::plan::{JoinType, NodeType, PlanNode, QueryPlan};
use thiserror::Error;

/// Errors that can occur when parsing SQLite EXPLAIN QUERY PLAN output
#[derive(Debug, Error)]
pub enum SqliteExplainError {
    #[error("Empty EXPLAIN output")]
    EmptyOutput,

    #[error("Invalid plan structure: {0}")]
    InvalidStructure(String),

    #[error("Unrecognized operation: {0}")]
    UnrecognizedOperation(String),
}

/// Result type for SQLite EXPLAIN parsing
pub type Result<T> = std::result::Result<T, SqliteExplainError>;

/// Parses SQLite EXPLAIN QUERY PLAN output
///
/// Supports multiple SQLite output formats:
/// - Tree format (default): Shows indented tree with |-- and `-- prefixes
/// - Tabular format: Shows selectid, order, from, detail columns (SQLite 3.24+)
///
/// # Examples
///
/// ```
/// use zqlz_analyzer::explain::sqlite::parse_sqlite_explain;
///
/// // Tree format
/// let tree_output = r#"QUERY PLAN
/// |--SCAN users
/// `--SEARCH orders USING INDEX idx_orders (user_id=?)"#;
/// let plan = parse_sqlite_explain(tree_output).unwrap();
///
/// // Tabular format  
/// let tabular = "0|0|0|SCAN users\n0|1|1|SEARCH orders USING INDEX idx_orders (user_id=?)";
/// let plan = parse_sqlite_explain(tabular).unwrap();
/// ```
pub fn parse_sqlite_explain(output: &str) -> Result<QueryPlan> {
    let trimmed = output.trim();

    if trimmed.is_empty() {
        return Err(SqliteExplainError::EmptyOutput);
    }

    // Detect format based on content
    if trimmed.starts_with("QUERY PLAN") || trimmed.contains("|--") || trimmed.contains("`--") {
        parse_tree_format(trimmed)
    } else if trimmed.contains('|')
        && trimmed.lines().next().map_or(false, |l| {
            l.split('|').count() >= 4
                && l.split('|')
                    .next()
                    .map_or(false, |s| s.trim().parse::<u32>().is_ok())
        })
    {
        parse_tabular_format(trimmed)
    } else {
        // Try to parse as simple line format
        parse_simple_format(trimmed)
    }
}

/// Parses the tree format output from EXPLAIN QUERY PLAN
///
/// Example:
/// ```text
/// QUERY PLAN
/// |--SCAN users
/// |--SEARCH orders USING INDEX idx_user_id (user_id=?)
/// |  `--CORRELATED SCALAR SUBQUERY 2
/// |     `--SEARCH items USING COVERING INDEX idx_items_order (order_id=?)
/// `--USE TEMP B-TREE FOR ORDER BY
/// ```
pub fn parse_tree_format(output: &str) -> Result<QueryPlan> {
    let lines: Vec<&str> = output.lines().collect();

    if lines.is_empty() {
        return Err(SqliteExplainError::EmptyOutput);
    }

    // Skip "QUERY PLAN" header if present
    let start = if lines[0].trim() == "QUERY PLAN" || lines[0].trim().starts_with("QUERY PLAN") {
        1
    } else {
        0
    };

    if start >= lines.len() {
        // Just "QUERY PLAN" header with no content - return empty Result
        return Ok(QueryPlan::new(PlanNode::new(NodeType::Result)));
    }

    // Parse tree structure
    let mut root_children: Vec<PlanNode> = Vec::new();
    let mut stack: Vec<(usize, PlanNode)> = Vec::new(); // (indent_level, node)

    for line in &lines[start..] {
        let line = *line;
        if line.trim().is_empty() {
            continue;
        }

        let (indent, detail) = parse_tree_line(line);
        let node = parse_detail(&detail)?;

        // Find parent based on indent level
        while !stack.is_empty() && stack.last().unwrap().0 >= indent {
            let (_, child) = stack.pop().unwrap();
            if let Some((_, parent)) = stack.last_mut() {
                parent.children.push(child);
            } else {
                root_children.push(child);
            }
        }

        stack.push((indent, node));
    }

    // Pop remaining items from stack
    while !stack.is_empty() {
        let (_, child) = stack.pop().unwrap();
        if let Some((_, parent)) = stack.last_mut() {
            parent.children.push(child);
        } else {
            root_children.push(child);
        }
    }

    // Build final tree
    let root = if root_children.len() == 1 {
        root_children.pop().unwrap()
    } else if root_children.is_empty() {
        PlanNode::new(NodeType::Result)
    } else {
        // Multiple root nodes - wrap in Append
        let mut append = PlanNode::new(NodeType::Append);
        append.children = root_children;
        append
    };

    Ok(QueryPlan::new(root))
}

/// Parses a single tree line and returns (indent_level, detail_text)
fn parse_tree_line(line: &str) -> (usize, String) {
    let mut indent = 0;
    let mut chars = line.chars().peekable();
    let mut detail_start = 0;

    while let Some(c) = chars.next() {
        match c {
            ' ' | '|' => {
                indent += 1;
                detail_start += 1;
            }
            '-' => {
                // Part of |-- or `--
                detail_start += 1;
                if chars.peek() == Some(&'-') {
                    chars.next();
                    detail_start += 1;
                }
                break;
            }
            '`' => {
                detail_start += 1;
                // Skip -- part
                if chars.peek() == Some(&'-') {
                    chars.next();
                    detail_start += 1;
                }
                if chars.peek() == Some(&'-') {
                    chars.next();
                    detail_start += 1;
                }
                break;
            }
            _ => break,
        }
    }

    let detail = line[detail_start..].trim().to_string();
    (indent, detail)
}

/// Parses tabular format (SQLite 3.24+)
///
/// Format: selectid|order|from|detail
/// Example:
/// ```text
/// 0|0|0|SCAN users
/// 0|1|1|SEARCH orders USING INDEX idx_user_id (user_id=?)
/// ```
pub fn parse_tabular_format(output: &str) -> Result<QueryPlan> {
    let mut nodes: Vec<PlanNode> = Vec::new();

    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 4 {
            continue;
        }

        // parts[0] = selectid, parts[1] = order, parts[2] = from, parts[3] = detail
        let detail = parts[3..].join("|"); // Handle case where detail contains |
        let node = parse_detail(&detail)?;
        nodes.push(node);
    }

    if nodes.is_empty() {
        return Err(SqliteExplainError::EmptyOutput);
    }

    // Build tree from flat list
    let root = if nodes.len() == 1 {
        nodes.pop().unwrap()
    } else {
        // Multiple nodes - wrap in join tree
        let mut current = nodes.remove(0);
        for node in nodes {
            let mut join = PlanNode::new(NodeType::NestedLoop);
            join.join_type = Some(JoinType::Inner);
            join.children.push(current);
            join.children.push(node);
            current = join;
        }
        current
    };

    Ok(QueryPlan::new(root))
}

/// Parses simple line format (one operation per line)
fn parse_simple_format(output: &str) -> Result<QueryPlan> {
    let mut nodes: Vec<PlanNode> = Vec::new();

    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let node = parse_detail(line)?;
        nodes.push(node);
    }

    if nodes.is_empty() {
        return Err(SqliteExplainError::EmptyOutput);
    }

    let root = if nodes.len() == 1 {
        nodes.pop().unwrap()
    } else {
        let mut current = nodes.remove(0);
        for node in nodes {
            let mut join = PlanNode::new(NodeType::NestedLoop);
            join.join_type = Some(JoinType::Inner);
            join.children.push(current);
            join.children.push(node);
            current = join;
        }
        current
    };

    Ok(QueryPlan::new(root))
}

/// Parses a detail string into a PlanNode
fn parse_detail(detail: &str) -> Result<PlanNode> {
    let detail = detail.trim();
    let detail_upper = detail.to_uppercase();

    // Match known patterns
    let node = if detail_upper.starts_with("SCAN") {
        parse_scan_operation(detail)
    } else if detail_upper.starts_with("SEARCH") {
        parse_search_operation(detail)
    } else if detail_upper.starts_with("USE TEMP B-TREE FOR ORDER BY")
        || detail_upper.starts_with("USE TEMP B-TREE FOR DISTINCT")
        || detail_upper.starts_with("USE TEMP B-TREE FOR GROUP BY")
    {
        parse_temp_btree_operation(detail)
    } else if detail_upper.starts_with("USING TEMP B-TREE") {
        parse_temp_btree_operation(detail)
    } else if detail_upper.starts_with("COMPOUND SUBQUERIES") {
        parse_compound_operation(detail)
    } else if detail_upper.starts_with("CORRELATED") || detail_upper.starts_with("SCALAR SUBQUERY")
    {
        parse_subquery_operation(detail)
    } else if detail_upper.starts_with("CO-ROUTINE") {
        parse_coroutine_operation(detail)
    } else if detail_upper.starts_with("EXECUTE") {
        parse_execute_operation(detail)
    } else if detail_upper.starts_with("MATERIALIZE") {
        parse_materialize_operation(detail)
    } else if detail_upper.starts_with("UNION") {
        parse_union_operation(detail)
    } else if detail_upper.starts_with("MERGE") {
        parse_merge_operation(detail)
    } else if detail_upper.starts_with("LEFT") || detail_upper.starts_with("RIGHT") {
        parse_join_operation(detail)
    } else if detail_upper.starts_with("BLOOM FILTER") {
        parse_bloom_filter_operation(detail)
    } else if detail_upper.starts_with("LIST SUBQUERY") {
        parse_list_subquery_operation(detail)
    } else if detail_upper.contains("AUTOMATIC COVERING INDEX")
        || detail_upper.contains("AUTO-INDEX")
    {
        parse_auto_index_operation(detail)
    } else {
        // Unknown operation - create generic node
        let mut node = PlanNode::new(NodeType::Unknown);
        node.description = Some(detail.to_string());
        node
    };

    Ok(node)
}

/// Parses SCAN operation
/// Examples:
/// - SCAN users
/// - SCAN users USING COVERING INDEX idx_all
/// - SCAN CONSTANT ROW
fn parse_scan_operation(detail: &str) -> PlanNode {
    let mut node = PlanNode::new(NodeType::SeqScan);
    let detail_upper = detail.to_uppercase();

    // Check for covering index scan
    if detail_upper.contains("USING COVERING INDEX") {
        node.node_type = NodeType::IndexOnlyScan;
        if let Some(idx_name) = extract_index_name(detail, "COVERING INDEX") {
            node.index_name = Some(idx_name);
        }
    } else if detail_upper.contains("USING INDEX") {
        node.node_type = NodeType::IndexScan;
        if let Some(idx_name) = extract_index_name(detail, "INDEX") {
            node.index_name = Some(idx_name);
        }
    }

    // Extract table name
    // Format: SCAN [TABLE] table_name [AS alias] [USING ...]
    if let Some(table_name) = extract_table_name(detail, "SCAN") {
        node.relation = Some(table_name);
    }

    // Check for CONSTANT ROW
    if detail_upper.contains("CONSTANT ROW") {
        node.node_type = NodeType::ValuesScan;
        node.description = Some("Constant row".to_string());
    }

    // Check for SUBQUERY
    if detail_upper.contains("SUBQUERY") {
        node.node_type = NodeType::SubqueryScan;
    }

    node
}

/// Parses SEARCH operation (index lookup)
/// Examples:
/// - SEARCH users USING INDEX idx_email (email=?)
/// - SEARCH orders USING COVERING INDEX idx_user_order (user_id=? AND order_date>?)
/// - SEARCH items USING INTEGER PRIMARY KEY (rowid=?)
fn parse_search_operation(detail: &str) -> PlanNode {
    let mut node = PlanNode::new(NodeType::IndexScan);
    let detail_upper = detail.to_uppercase();

    // Check for covering index
    if detail_upper.contains("USING COVERING INDEX") {
        node.node_type = NodeType::IndexOnlyScan;
        if let Some(idx_name) = extract_index_name(detail, "COVERING INDEX") {
            node.index_name = Some(idx_name);
        }
    } else if detail_upper.contains("USING INDEX") {
        if let Some(idx_name) = extract_index_name(detail, "INDEX") {
            node.index_name = Some(idx_name);
        }
    } else if detail_upper.contains("INTEGER PRIMARY KEY") || detail_upper.contains("ROWID") {
        node.index_name = Some("PRIMARY KEY".to_string());
    } else if detail_upper.contains("AUTOMATIC COVERING INDEX")
        || detail_upper.contains("AUTO-INDEX")
    {
        node.node_type = NodeType::IndexOnlyScan;
        node.index_name = Some("AUTO-INDEX".to_string());
        node.extra
            .insert("auto_index".to_string(), serde_json::Value::Bool(true));
    }

    // Extract table name
    if let Some(table_name) = extract_table_name(detail, "SEARCH") {
        node.relation = Some(table_name);
    }

    // Extract index condition
    if let Some(cond) = extract_index_condition(detail) {
        node.index_cond = Some(cond);
    }

    node
}

/// Parses USE TEMP B-TREE operations
fn parse_temp_btree_operation(detail: &str) -> PlanNode {
    let detail_upper = detail.to_uppercase();

    if detail_upper.contains("ORDER BY") {
        let mut node = PlanNode::new(NodeType::Sort);
        node.description = Some("Temporary B-tree for ORDER BY".to_string());
        node.extra.insert(
            "using_temp_btree".to_string(),
            serde_json::Value::Bool(true),
        );
        node
    } else if detail_upper.contains("DISTINCT") {
        let mut node = PlanNode::new(NodeType::Unique);
        node.description = Some("Temporary B-tree for DISTINCT".to_string());
        node.extra.insert(
            "using_temp_btree".to_string(),
            serde_json::Value::Bool(true),
        );
        node
    } else if detail_upper.contains("GROUP BY") {
        let mut node = PlanNode::new(NodeType::HashAggregate);
        node.description = Some("Temporary B-tree for GROUP BY".to_string());
        node.extra.insert(
            "using_temp_btree".to_string(),
            serde_json::Value::Bool(true),
        );
        node
    } else if detail_upper.contains("RIGHT PART OF") || detail_upper.contains("LEFT PART OF") {
        // Part of a join operation
        let mut node = PlanNode::new(NodeType::Sort);
        node.description = Some(detail.to_string());
        node
    } else {
        let mut node = PlanNode::new(NodeType::Sort);
        node.description = Some(detail.to_string());
        node
    }
}

/// Parses COMPOUND SUBQUERIES (UNION, INTERSECT, EXCEPT)
fn parse_compound_operation(detail: &str) -> PlanNode {
    let mut node = PlanNode::new(NodeType::SetOp);
    node.description = Some(detail.to_string());
    node
}

/// Parses subquery operations
fn parse_subquery_operation(detail: &str) -> PlanNode {
    let mut node = PlanNode::new(NodeType::SubqueryScan);
    node.description = Some(detail.to_string());

    // Check if correlated
    if detail.to_uppercase().contains("CORRELATED") {
        node.extra
            .insert("correlated".to_string(), serde_json::Value::Bool(true));
    }

    node
}

/// Parses CO-ROUTINE operations (CTEs)
fn parse_coroutine_operation(detail: &str) -> PlanNode {
    let mut node = PlanNode::new(NodeType::CteScan);
    node.description = Some(detail.to_string());

    // Extract CTE name if present
    // Format: CO-ROUTINE cte_name
    let parts: Vec<&str> = detail.split_whitespace().collect();
    if parts.len() > 1 {
        node.relation = Some(parts[1].to_string());
    }

    node
}

/// Parses EXECUTE operations
fn parse_execute_operation(detail: &str) -> PlanNode {
    let mut node = PlanNode::new(NodeType::Result);
    node.description = Some(detail.to_string());
    node
}

/// Parses MATERIALIZE operations
fn parse_materialize_operation(detail: &str) -> PlanNode {
    let mut node = PlanNode::new(NodeType::Materialize);
    node.description = Some(detail.to_string());
    node
}

/// Parses UNION operations
fn parse_union_operation(detail: &str) -> PlanNode {
    let detail_upper = detail.to_uppercase();

    let node_type = if detail_upper.contains("UNION ALL") {
        NodeType::Append
    } else {
        // UNION (with DISTINCT)
        NodeType::SetOp
    };

    let mut node = PlanNode::new(node_type);
    node.description = Some(detail.to_string());
    node
}

/// Parses MERGE operations
fn parse_merge_operation(detail: &str) -> PlanNode {
    let mut node = PlanNode::new(NodeType::MergeAppend);
    node.description = Some(detail.to_string());
    node
}

/// Parses join operations
fn parse_join_operation(detail: &str) -> PlanNode {
    let detail_upper = detail.to_uppercase();
    let mut node = PlanNode::new(NodeType::NestedLoop);

    if detail_upper.contains("LEFT") {
        node.join_type = Some(JoinType::Left);
    } else if detail_upper.contains("RIGHT") {
        node.join_type = Some(JoinType::Right);
    }

    node.description = Some(detail.to_string());
    node
}

/// Parses BLOOM FILTER operations
fn parse_bloom_filter_operation(detail: &str) -> PlanNode {
    let mut node = PlanNode::new(NodeType::Hash);
    node.description = Some(detail.to_string());
    node.extra
        .insert("bloom_filter".to_string(), serde_json::Value::Bool(true));
    node
}

/// Parses LIST SUBQUERY operations
fn parse_list_subquery_operation(detail: &str) -> PlanNode {
    let mut node = PlanNode::new(NodeType::SubqueryScan);
    node.description = Some(detail.to_string());
    node.extra
        .insert("list_subquery".to_string(), serde_json::Value::Bool(true));
    node
}

/// Parses AUTOMATIC COVERING INDEX operations
fn parse_auto_index_operation(detail: &str) -> PlanNode {
    let mut node = PlanNode::new(NodeType::IndexOnlyScan);
    node.description = Some(detail.to_string());
    node.index_name = Some("AUTO-INDEX".to_string());
    node.extra
        .insert("auto_index".to_string(), serde_json::Value::Bool(true));

    // Extract table name if present
    if let Some(table_name) = extract_table_name(detail, "AUTOMATIC COVERING INDEX") {
        node.relation = Some(table_name);
    }

    node
}

/// Extracts table name from detail string
/// Handles formats like:
/// - SCAN table_name
/// - SCAN TABLE table_name
/// - SCAN table_name AS alias
/// - SEARCH table_name USING INDEX ...
fn extract_table_name(detail: &str, operation: &str) -> Option<String> {
    let detail_upper = detail.to_uppercase();
    let op_upper = operation.to_uppercase();

    // Find where the operation keyword ends
    let start = detail_upper.find(&op_upper)? + op_upper.len();
    let remaining = detail[start..].trim();

    // Skip optional "TABLE" keyword
    let remaining = if remaining.to_uppercase().starts_with("TABLE ") {
        remaining[6..].trim()
    } else {
        remaining
    };

    // Get the table name (first word before AS or USING or end)
    let table_name = remaining.split_whitespace().next()?;

    // Skip keywords like CONSTANT, SUBQUERY
    if table_name.to_uppercase() == "CONSTANT"
        || table_name.to_uppercase() == "SUBQUERY"
        || table_name.is_empty()
    {
        return None;
    }

    Some(table_name.to_string())
}

/// Extracts index name from detail string
fn extract_index_name(detail: &str, index_type: &str) -> Option<String> {
    let detail_upper = detail.to_uppercase();
    let search = index_type.to_uppercase();

    let start = detail_upper.find(&search)? + search.len();
    let remaining = detail[start..].trim();

    // Index name is the next word
    let idx_name = remaining.split_whitespace().next()?;

    // Stop at parenthesis if present
    let idx_name = idx_name.split('(').next()?;

    if idx_name.is_empty() {
        return None;
    }

    Some(idx_name.to_string())
}

/// Extracts index condition from parentheses
fn extract_index_condition(detail: &str) -> Option<String> {
    let start = detail.find('(')?;
    let end = detail.rfind(')')?;

    if start < end {
        Some(detail[start + 1..end].to_string())
    } else {
        None
    }
}

/// Maps SQLite operation strings to NodeType
pub fn sqlite_operation_to_node_type(operation: &str) -> NodeType {
    let op_upper = operation.to_uppercase();

    if op_upper.starts_with("SCAN") {
        if op_upper.contains("COVERING INDEX") {
            NodeType::IndexOnlyScan
        } else if op_upper.contains("INDEX") {
            NodeType::IndexScan
        } else if op_upper.contains("CONSTANT ROW") {
            NodeType::ValuesScan
        } else if op_upper.contains("SUBQUERY") {
            NodeType::SubqueryScan
        } else {
            NodeType::SeqScan
        }
    } else if op_upper.starts_with("SEARCH") {
        if op_upper.contains("COVERING INDEX")
            || op_upper.contains("AUTO-INDEX")
            || op_upper.contains("AUTOMATIC COVERING INDEX")
        {
            NodeType::IndexOnlyScan
        } else {
            NodeType::IndexScan
        }
    } else if op_upper.contains("ORDER BY") || op_upper.starts_with("USING TEMP B-TREE") {
        NodeType::Sort
    } else if op_upper.contains("DISTINCT") {
        NodeType::Unique
    } else if op_upper.contains("GROUP BY") {
        NodeType::HashAggregate
    } else if op_upper.starts_with("UNION") {
        if op_upper.contains("ALL") {
            NodeType::Append
        } else {
            NodeType::SetOp
        }
    } else if op_upper.starts_with("COMPOUND") {
        NodeType::SetOp
    } else if op_upper.starts_with("CO-ROUTINE") {
        NodeType::CteScan
    } else if op_upper.contains("SUBQUERY") {
        NodeType::SubqueryScan
    } else if op_upper.starts_with("MATERIALIZE") {
        NodeType::Materialize
    } else if op_upper.starts_with("MERGE") {
        NodeType::MergeAppend
    } else if op_upper.contains("LEFT") || op_upper.contains("RIGHT") || op_upper.contains("JOIN") {
        NodeType::NestedLoop
    } else if op_upper.contains("BLOOM FILTER") {
        NodeType::Hash
    } else {
        NodeType::Unknown
    }
}

#[cfg(test)]
mod tests;
