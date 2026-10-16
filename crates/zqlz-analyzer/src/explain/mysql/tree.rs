//! Parser for MySQL's tree-formatted plans (`EXPLAIN FORMAT=TREE` and
//! `EXPLAIN ANALYZE`).
//!
//! ```text
//! -> Limit: 5 row(s)  (cost=2.48e+9 rows=5) (actual time=2677..2677 rows=0 loops=1)
//!     -> Nested loop inner join  (cost=2.48e+9 rows=24.8e+9) (actual time=2677..2677 rows=0 loops=1)
//!         -> Table scan on orders_orders  (cost=20049 rows=152189) (actual time=0.239..631 rows=150000 loops=1)
//! ```
//!
//! Children are indented by four spaces. Each line carries the optimizer's
//! estimate `(cost=startup..total rows=n)` and, for `EXPLAIN ANALYZE`, what
//! really happened `(actual time=first..last rows=n loops=n)` - times are per
//! loop, in milliseconds.

use super::{MysqlExplainError, Result, set_operation};
use crate::explain::plan::{ActualTime, JoinType, NodeCost, NodeType, PlanNode, QueryPlan};
use serde_json::Value;

/// Deepest tree accepted; real plans are a few dozen levels at most.
const MAX_DEPTH: usize = 256;

/// Whether `text` is tree-formatted (some line starts with `->`).
pub(super) fn looks_like_tree(text: &str) -> bool {
    text.lines().any(|line| line.trim_start().starts_with("->"))
}

/// Parses `EXPLAIN FORMAT=TREE` / `EXPLAIN ANALYZE` output.
///
/// Lines before the first `->` (a column header such as `EXPLAIN`) are skipped.
pub fn parse_tree_explain(text: &str) -> Result<QueryPlan> {
    let mut nodes: Vec<PlanNode> = Vec::new();
    let mut parents: Vec<Option<usize>> = Vec::new();
    // (indent, index) of the chain of ancestors of the line being read
    let mut stack: Vec<(usize, usize)> = Vec::new();

    for line in text.lines() {
        let trimmed = line.trim_start();
        let Some(body) = trimmed.strip_prefix("->") else {
            continue;
        };
        if body.trim().is_empty() {
            continue;
        }
        let indent = line.len() - trimmed.len();

        while stack.last().is_some_and(|(open, _)| *open >= indent) {
            stack.pop();
        }
        // A second top-level line has no parent of its own; keep it under the first.
        let parent = stack.last().map(|(_, index)| *index).or(if nodes.is_empty() {
            None
        } else {
            Some(0)
        });

        nodes.push(parse_line(body.trim()));
        parents.push(parent);
        stack.push((indent, nodes.len() - 1));
    }

    if nodes.is_empty() {
        return Err(if text.trim().is_empty() {
            MysqlExplainError::EmptyOutput
        } else {
            MysqlExplainError::InvalidStructure("No plan nodes found".into())
        });
    }

    if nodes.len() == 1
        && nodes[0]
            .extra
            .get("raw")
            .and_then(Value::as_str)
            .is_some_and(|raw| raw.starts_with("<not executable"))
    {
        return Err(MysqlExplainError::InvalidStructure(
            "MySQL cannot describe this statement as a tree; use EXPLAIN FORMAT=JSON".into(),
        ));
    }

    let mut children: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    for (index, parent) in parents.iter().enumerate() {
        if let Some(parent) = parent {
            children[*parent].push(index);
        }
    }

    let mut slots: Vec<Option<PlanNode>> = nodes.into_iter().map(Some).collect();
    let mut root = assemble(0, &children, &mut slots, 0)?;
    finish(&mut root);

    let execution_time_ms = root
        .actual_time_ms
        .map(|time| time.total * root.loops.unwrap_or(1) as f64);
    let mut plan = QueryPlan::new(root);
    plan.execution_time_ms = execution_time_ms;
    Ok(plan)
}

fn assemble(
    index: usize,
    children: &[Vec<usize>],
    slots: &mut [Option<PlanNode>],
    depth: usize,
) -> Result<PlanNode> {
    if depth > MAX_DEPTH {
        return Err(MysqlExplainError::InvalidStructure(
            "Plan tree is nested too deeply".into(),
        ));
    }
    let mut node = slots[index]
        .take()
        .ok_or_else(|| MysqlExplainError::InvalidStructure("Malformed plan tree".into()))?;
    for child in &children[index] {
        node.children
            .push(assemble(*child, children, slots, depth + 1)?);
    }
    Ok(node)
}

/// What the `(cost=...)` / `(actual time=...)` suffixes of a line said.
#[derive(Default)]
struct Metrics {
    cost: Option<(f64, f64)>,
    rows: Option<f64>,
    actual_time: Option<(f64, f64)>,
    actual_rows: Option<f64>,
    loops: Option<f64>,
    never_executed: bool,
}

/// `a..b` or a lone `a` (then both ends are `a`).
fn parse_range(text: &str) -> Option<(f64, f64)> {
    match text.split_once("..") {
        Some((start, end)) => Some((start.parse().ok()?, end.parse().ok()?)),
        None => text.parse().ok().map(|value| (value, value)),
    }
}

fn keyed<'a>(tokens: &[&'a str], key: &str) -> Option<&'a str> {
    tokens
        .iter()
        .find_map(|token| token.strip_prefix(key)?.strip_prefix('='))
}

/// Splits `Table scan on t  (cost=1 rows=2) (actual time=...)` into the label
/// and the metrics. Parenthesized text that is not a metric belongs to the label.
fn split_metrics(body: &str) -> (&str, Metrics) {
    let mut metrics = Metrics::default();
    let mut label = body.trim_end();

    while label.ends_with(')') {
        let Some(open) = label.rfind('(') else { break };
        let inner = &label[open + 1..label.len() - 1];
        let tokens: Vec<&str> = inner.split_whitespace().collect();

        if inner == "never executed" {
            metrics.never_executed = true;
        } else if let Some(cost) = keyed(&tokens, "cost") {
            metrics.cost = parse_range(cost);
            metrics.rows = keyed(&tokens, "rows").and_then(|rows| rows.parse().ok());
        } else if inner.starts_with("actual time=") {
            // tokens are "actual", "time=a..b", "rows=n", "loops=n"
            metrics.actual_time = keyed(&tokens, "time").and_then(parse_range);
            metrics.actual_rows = keyed(&tokens, "rows").and_then(|rows| rows.parse().ok());
            metrics.loops = keyed(&tokens, "loops").and_then(|loops| loops.parse().ok());
        } else {
            break;
        }
        label = label[..open].trim_end();
    }

    (label, metrics)
}

fn count(value: f64) -> u64 {
    if value.is_finite() && value > 0.0 {
        value.round() as u64
    } else {
        0
    }
}

fn parse_line(body: &str) -> PlanNode {
    let (label, metrics) = split_metrics(body);
    let mut node = classify(label);

    node.extra
        .insert("raw".to_string(), Value::String(label.to_string()));
    node.cost = metrics.cost.map(|(startup, total)| NodeCost::new(startup, total));
    node.rows = metrics.rows.map(count);
    if let Some((first, last)) = metrics.actual_time {
        node.actual_time_ms = Some(ActualTime::new(first, last));
    }
    node.actual_rows = metrics.actual_rows.map(count);
    node.loops = metrics.loops.map(count);
    if metrics.never_executed {
        node.actual_rows = Some(0);
        node.loops = Some(0);
        node.extra
            .insert("never_executed".to_string(), Value::Bool(true));
    }
    node
}

fn has_prefix(lower: &str, prefix: &str) -> bool {
    lower.starts_with(prefix)
}

/// Maps a plan line to a node type plus MySQL's own wording for it.
fn classify(label: &str) -> PlanNode {
    let label = label.trim();
    let lower = label.to_ascii_lowercase();
    let after_colon = || label.split_once(':').map(|(_, rest)| rest.trim().to_string());

    let mut node;

    if let Some(scan) = classify_table_access(label) {
        return scan;
    } else if has_prefix(&lower, "limit/offset:") || has_prefix(&lower, "limit:") {
        node = PlanNode::new(NodeType::Limit);
        let operation = label.split(':').next().unwrap_or("Limit");
        set_operation(&mut node, operation, after_colon());
    } else if has_prefix(&lower, "limit table size:") {
        node = PlanNode::new(NodeType::Limit);
        set_operation(&mut node, "Limit table size", after_colon());
    } else if has_prefix(&lower, "filter:") {
        node = PlanNode::new(NodeType::Filter);
        node.filter = after_colon();
        set_operation(&mut node, "Filter", after_colon());
    } else if has_prefix(&lower, "sort:") || has_prefix(&lower, "sort row ids:") {
        node = PlanNode::new(NodeType::Sort);
        node.sort_method = Some("filesort".to_string());
        node.extra
            .insert("using_filesort".to_string(), Value::Bool(true));
        let keys = after_colon().unwrap_or_default();
        node.sort_keys = keys
            .split(", limit input")
            .next()
            .map(str::trim)
            .filter(|keys| !keys.is_empty())
            .map(String::from)
            .into_iter()
            .collect();
        set_operation(&mut node, "Sort", Some(keys));
    } else if has_prefix(&lower, "nested loop") || has_prefix(&lower, "batched key access") {
        node = PlanNode::new(NodeType::NestedLoop);
        node.join_type = Some(join_type(&lower));
        set_operation(&mut node, label, None);
    } else if lower.contains("hash")
        && (lower.contains("join") || lower.contains("antijoin") || lower.contains("semijoin"))
    {
        node = PlanNode::new(NodeType::HashJoin);
        node.join_type = Some(join_type(&lower));
        // "Inner hash join (a.id = b.id)": the parenthesis holds the condition
        let (operation, condition) = match label.split_once(" (") {
            Some((operation, rest)) => (operation, rest.strip_suffix(')').map(String::from)),
            None => (label, None),
        };
        node.join_cond = condition.clone().filter(|cond| cond != "no condition");
        set_operation(&mut node, operation, condition);
    } else if lower == "hash" {
        node = PlanNode::new(NodeType::Hash);
        set_operation(&mut node, "Hash", None);
    } else if has_prefix(&lower, "aggregate using temporary table") {
        node = PlanNode::new(NodeType::HashAggregate);
        mark_temporary(&mut node);
        set_operation(&mut node, "Aggregate using temporary table", None);
    } else if has_prefix(&lower, "aggregate:") {
        node = PlanNode::new(NodeType::Aggregate);
        set_operation(&mut node, "Aggregate", after_colon());
    } else if has_prefix(&lower, "group aggregate:") {
        node = PlanNode::new(NodeType::GroupAggregate);
        set_operation(&mut node, "Group aggregate", after_colon());
    } else if has_prefix(&lower, "group (no aggregates)") {
        node = PlanNode::new(NodeType::GroupAggregate);
        set_operation(&mut node, "Group (no aggregates)", None);
    } else if has_prefix(&lower, "window aggregate") {
        node = PlanNode::new(NodeType::WindowAgg);
        let operation = label.split(':').next().unwrap_or("Window aggregate");
        set_operation(&mut node, operation, after_colon());
    } else if has_prefix(&lower, "remove duplicates") {
        node = PlanNode::new(NodeType::Unique);
        set_operation(&mut node, "Remove duplicates", label.strip_prefix("Remove duplicates from ").map(String::from));
    } else if has_prefix(&lower, "union") && lower.contains("materialize") {
        node = PlanNode::new(NodeType::Append);
        mark_temporary(&mut node);
        set_operation(&mut node, label, None);
    } else if (has_prefix(&lower, "intersect") || has_prefix(&lower, "except"))
        && lower.contains("materialize")
    {
        node = PlanNode::new(NodeType::SetOp);
        mark_temporary(&mut node);
        set_operation(&mut node, label, None);
    } else if has_prefix(&lower, "materialize") {
        node = PlanNode::new(NodeType::Materialize);
        mark_temporary(&mut node);
        set_operation(&mut node, label, None);
    } else if has_prefix(&lower, "temporary table") {
        node = PlanNode::new(NodeType::Materialize);
        mark_temporary(&mut node);
        set_operation(&mut node, label, None);
    } else if has_prefix(&lower, "stream results") {
        node = PlanNode::new(NodeType::Result);
        set_operation(&mut node, "Stream results", None);
    } else if has_prefix(&lower, "rows fetched before execution")
        || has_prefix(&lower, "zero rows")
        || has_prefix(&lower, "no rows found")
    {
        node = PlanNode::new(NodeType::Result);
        let operation = label.split(':').next().unwrap_or(label);
        set_operation(&mut node, operation, after_colon());
    } else if has_prefix(&lower, "constant row from") {
        node = PlanNode::new(NodeType::IndexScan);
        let table = label["Constant row from".len()..].trim();
        node.relation = Some(table.to_string());
        set_operation(&mut node, "Constant row", Some(table.to_string()));
    } else if has_prefix(&lower, "select #") {
        node = PlanNode::new(NodeType::SubPlan);
        set_operation(&mut node, "Subquery", Some(label.to_string()));
    } else if let Some(rest) = label.strip_prefix("Delete from ") {
        node = PlanNode::new(NodeType::Delete);
        set_operation(&mut node, "Delete from", Some(rest.to_string()));
    } else if let Some(rest) = label
        .strip_prefix("Insert into ")
        .or_else(|| label.strip_prefix("Replace into "))
    {
        node = PlanNode::new(NodeType::Insert);
        set_operation(&mut node, "Insert into", Some(rest.to_string()));
    } else if let Some(rest) = label.strip_prefix("Update ") {
        node = PlanNode::new(NodeType::Update);
        set_operation(&mut node, "Update", Some(rest.to_string()));
    } else if has_prefix(&lower, "intersect rows sorted by row id") {
        node = PlanNode::new(NodeType::BitmapAnd);
        set_operation(&mut node, label, None);
    } else if has_prefix(&lower, "union rows sorted by row id") || has_prefix(&lower, "sort-union") {
        node = PlanNode::new(NodeType::BitmapOr);
        set_operation(&mut node, label, None);
    } else if has_prefix(&lower, "repeat until convergence") {
        node = PlanNode::new(NodeType::RecursiveUnion);
        set_operation(&mut node, label, None);
    } else {
        // Something this parser has never seen: show MySQL's own words.
        node = PlanNode::new(NodeType::Unknown);
        set_operation(&mut node, label.trim_matches(|c| c == '<' || c == '>'), None);
    }

    node
}

fn mark_temporary(node: &mut PlanNode) {
    node.extra
        .insert("using_temporary".to_string(), Value::Bool(true));
}

fn join_type(lower: &str) -> JoinType {
    if lower.contains("anti") {
        JoinType::Anti
    } else if lower.contains("semi") {
        JoinType::Semi
    } else if lower.contains("left") {
        JoinType::Left
    } else if lower.contains("right") {
        JoinType::Right
    } else if lower.contains("full") {
        JoinType::Full
    } else {
        JoinType::Inner
    }
}

/// `Table scan on t`, `Index lookup on o using idx (c=1), with index condition: (...)`, ...
fn classify_table_access(label: &str) -> Option<PlanNode> {
    let (operation, rest) = label.split_once(" on ")?;
    let operation_lower = operation.to_ascii_lowercase();
    let is_table_scan = operation_lower == "table scan";
    let is_index_access = operation_lower.contains("index scan")
        || operation_lower.contains("index lookup")
        || operation_lower.contains("index range scan")
        || operation_lower.contains("index skip scan")
        || operation_lower.contains("index merge")
        || operation_lower.starts_with("full-text index search");
    if !is_table_scan && !is_index_access {
        return None;
    }

    let node_type = if is_table_scan {
        NodeType::SeqScan
    } else if operation_lower.contains("covering") {
        NodeType::IndexOnlyScan
    } else {
        NodeType::IndexScan
    };
    let mut node = PlanNode::new(node_type);

    // `<union temporary>` is one name even though it contains a space
    let table = match rest.strip_prefix('<').and_then(|inner| inner.find('>')) {
        Some(close) => &rest[..close + 2],
        None => rest.split(' ').next().unwrap_or_default(),
    };
    node.relation = Some(table.to_string());

    // "using idx (cols), with index condition: (cond)"
    let (access, index_condition) = match rest.split_once(", with index condition:") {
        Some((access, condition)) => (access, Some(condition.trim().to_string())),
        None => (rest, None),
    };
    node.index_cond = index_condition;
    if let Some((_, after_using)) = access.split_once(" using ") {
        node.index_name = after_using.split_whitespace().next().map(String::from);
    }

    node.extra.insert(
        "mysql_access_type".to_string(),
        Value::String(
            if is_table_scan {
                "ALL"
            } else if operation_lower.contains("range") {
                "range"
            } else if operation_lower.contains("single-row") {
                "eq_ref"
            } else if operation_lower.contains("lookup") {
                "ref"
            } else {
                "index"
            }
            .to_string(),
        ),
    );
    if operation_lower.contains("covering") {
        node.extra
            .insert("using_index".to_string(), Value::Bool(true));
    }
    set_operation(&mut node, operation, Some(access.trim().to_string()));
    Some(node)
}

/// Fills in what the text leaves implicit, once the tree is built.
fn finish(node: &mut PlanNode) {
    for child in &mut node.children {
        finish(child);
    }

    // Reading a materialized derived table / CTE is not a scan of a real table.
    if node.node_type == NodeType::SeqScan
        && (node.relation.as_deref().is_some_and(|name| name.starts_with('<'))
            || node
                .children
                .iter()
                .any(|child| child.node_type == NodeType::Materialize))
    {
        node.node_type = NodeType::CteScan;
    }

    if node.node_type == NodeType::Filter && node.children.len() == 1 {
        let child = &mut node.children[0];
        node.relation = node.relation.take().or_else(|| child.relation.clone());
        // Let scan-level checks (missing index) see the condition applied to the scan.
        if child.is_scan() && child.filter.is_none() {
            child.filter = node.filter.clone();
        }

        // Rows the filter threw away: what went in minus what came out.
        let total = |node: &PlanNode| Some(node.actual_rows? as f64 * node.loops.unwrap_or(1) as f64);
        if let (Some(input), Some(output)) = (total(child), total(node))
            && input > output
        {
            node.rows_removed_by_filter = Some((input - output).round() as u64);
        }
    }

    // Pass-through operators print no cost, or a cost of 0 (Limit, Sort, Window
    // aggregate, ...). They cost what their input costs, which keeps their own
    // share at zero rather than missing and keeps the root's total meaningful.
    if node.cost.is_none_or(|cost| cost.total <= 0.0) {
        let totals: Vec<f64> = node
            .children
            .iter()
            .filter_map(|child| child.cost.map(|cost| cost.total))
            .collect();
        if !totals.is_empty() {
            node.cost = Some(NodeCost::new(0.0, totals.iter().sum()));
        }
    }
    if node.rows.is_none() && node.children.len() == 1 {
        node.rows = node.children[0].rows;
    }
}
