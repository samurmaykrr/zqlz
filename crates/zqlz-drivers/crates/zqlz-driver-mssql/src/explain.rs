use std::collections::HashMap;

use zqlz_analyzer::explain::{JoinType, NodeCost, NodeType, PlanNode, QueryPlan};
use zqlz_core::QueryResult;

/// Builds a plan tree from a `SET SHOWPLAN_ALL ON` result, where every
/// operator row names its parent by `NodeId`. Anything else (including the
/// text showplan) returns `None`.
pub fn parse_mssql_explain_result(result: &QueryResult) -> Option<QueryPlan> {
    let mut nodes: HashMap<i64, PlanNode> = HashMap::new();
    let mut parents: HashMap<i64, i64> = HashMap::new();
    let mut order = Vec::new();
    let mut statement = None;

    for row in &result.rows {
        let field = |name: &str| row.get_by_name(name).map(|value| value.to_json_value());
        if field("Type")?.as_str() != Some("PLAN_ROW") {
            continue;
        }
        let statement_id = field("StmtId")?.as_i64()?;
        if *statement.get_or_insert(statement_id) != statement_id {
            continue;
        }
        let id = field("NodeId")?.as_i64()?;
        let physical_op = field("PhysicalOp")
            .and_then(|value| value.as_str().map(ToString::to_string))
            .unwrap_or_default();
        let logical_op = field("LogicalOp")
            .and_then(|value| value.as_str().map(ToString::to_string))
            .unwrap_or_default();
        let argument = field("Argument")
            .and_then(|value| value.as_str().map(ToString::to_string))
            .unwrap_or_default();

        let mut node = PlanNode::new(mssql_node_type(&physical_op, &logical_op));
        node.description = Some(physical_op);
        node.join_type = mssql_join_type(&logical_op);
        node.rows = field("EstimateRows")
            .and_then(|value| value.as_f64())
            .map(|rows| rows.round() as u64);
        node.width = field("AvgRowSize")
            .and_then(|value| value.as_u64())
            .and_then(|width| u32::try_from(width).ok());
        node.cost = field("TotalSubtreeCost")
            .and_then(|value| value.as_f64())
            .map(|total| NodeCost::new(0.0, total));
        apply_object(&mut node, &argument);
        node.index_cond = clause(&argument, "SEEK:(");
        node.filter = clause(&argument, "PREDICATE:(");

        parents.insert(id, field("Parent")?.as_i64()?);
        order.push(id);
        nodes.insert(id, node);
    }

    // The root is the operator whose parent is the statement row, which is
    // not a plan operator itself.
    let root_id = *order.iter().find(|id| !nodes.contains_key(&parents[id]))?;
    let mut children: HashMap<i64, Vec<i64>> = HashMap::new();
    for id in &order {
        children.entry(parents[id]).or_default().push(*id);
    }
    Some(QueryPlan::new(assemble(root_id, &mut nodes, &children)?))
}

fn assemble(
    id: i64,
    nodes: &mut HashMap<i64, PlanNode>,
    children: &HashMap<i64, Vec<i64>>,
) -> Option<PlanNode> {
    let mut node = nodes.remove(&id)?;
    for child in children.get(&id).into_iter().flatten() {
        node.children.extend(assemble(*child, nodes, children));
    }
    Some(node)
}

fn mssql_node_type(physical_op: &str, logical_op: &str) -> NodeType {
    match physical_op {
        "Table Scan" | "Clustered Index Scan" => NodeType::SeqScan,
        "Index Scan" | "Index Seek" | "Clustered Index Seek" => NodeType::IndexScan,
        "Key Lookup" | "RID Lookup" => NodeType::BitmapHeapScan,
        "Nested Loops" => NodeType::NestedLoop,
        "Merge Join" => NodeType::MergeJoin,
        "Hash Match" if logical_op.contains("Join") => NodeType::HashJoin,
        "Hash Match" | "Stream Aggregate" => NodeType::HashAggregate,
        "Sort" => NodeType::Sort,
        "Top" => NodeType::Limit,
        "Filter" => NodeType::Filter,
        "Compute Scalar" => NodeType::Result,
        "Constant Scan" => NodeType::ValuesScan,
        "Table Spool" | "Index Spool" | "Row Count Spool" => NodeType::Materialize,
        "Parallelism" => NodeType::Gather,
        "Concatenation" => NodeType::Append,
        _ => NodeType::Unknown,
    }
}

fn mssql_join_type(logical_op: &str) -> Option<JoinType> {
    match logical_op {
        "Inner Join" => Some(JoinType::Inner),
        "Left Outer Join" => Some(JoinType::Left),
        "Right Outer Join" => Some(JoinType::Right),
        "Full Outer Join" => Some(JoinType::Full),
        "Left Semi Join" | "Right Semi Join" => Some(JoinType::Semi),
        "Left Anti Semi Join" | "Right Anti Semi Join" => Some(JoinType::Anti),
        "Cross Join" => Some(JoinType::Cross),
        _ => None,
    }
}

/// Reads `OBJECT:([db].[schema].[table].[index] AS [alias])` into the node.
fn apply_object(node: &mut PlanNode, argument: &str) {
    let Some(start) = argument.find("OBJECT:(") else {
        return;
    };
    let rest = &argument[start + "OBJECT:(".len()..];
    let Some(end) = rest.find(')') else {
        return;
    };
    let (path, alias) = match rest[..end].split_once(" AS ") {
        Some((path, alias)) => (path, Some(alias)),
        None => (&rest[..end], None),
    };
    node.alias = alias.map(unbracket);
    let segments: Vec<String> = path.split("].[").map(unbracket).collect();
    match segments.as_slice() {
        [_, schema, table, index] => {
            node.schema = Some(schema.clone());
            node.relation = Some(table.clone());
            node.index_name = Some(index.clone());
        }
        [_, schema, table] => {
            node.schema = Some(schema.clone());
            node.relation = Some(table.clone());
        }
        _ => {}
    }
}

fn unbracket(text: &str) -> String {
    text.trim().trim_matches(['[', ']']).to_string()
}

/// The text of a `KEY:(...)` clause up to its closing parenthesis.
fn clause(argument: &str, key: &str) -> Option<String> {
    let rest = &argument[argument.find(key)? + key.len()..];
    let mut depth = 1;
    for (index, character) in rest.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(rest[..index].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use zqlz_core::{ColumnMeta, Row, Value};

    fn showplan(rows: Vec<(i64, i64, &str, &str, &str, &str)>) -> QueryResult {
        let names = [
            "StmtId",
            "NodeId",
            "Parent",
            "PhysicalOp",
            "LogicalOp",
            "Argument",
            "EstimateRows",
            "AvgRowSize",
            "TotalSubtreeCost",
            "Type",
        ];
        let columns: Vec<String> = names.iter().map(ToString::to_string).collect();
        let meta = columns
            .iter()
            .enumerate()
            .map(|(ordinal, name)| ColumnMeta {
                name: name.clone(),
                data_type: "text".to_string(),
                nullable: true,
                ordinal,
                max_length: None,
                precision: None,
                scale: None,
                auto_increment: false,
                default_value: None,
                comment: None,
                enum_values: None,
            })
            .collect();
        let rows = rows
            .into_iter()
            .map(|(id, parent, physical, logical, argument, kind)| {
                Row::new(
                    columns.clone(),
                    vec![
                        Value::Int64(1),
                        Value::Int64(id),
                        Value::Int64(parent),
                        Value::String(physical.to_string()),
                        Value::String(logical.to_string()),
                        Value::String(argument.to_string()),
                        Value::Float64(30.0),
                        Value::Int64(67),
                        Value::Float64(0.5),
                        Value::String(kind.to_string()),
                    ],
                )
            })
            .collect::<Vec<_>>();
        QueryResult {
            id: Uuid::new_v4(),
            columns: meta,
            total_rows: Some(rows.len() as u64),
            rows,
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms: 0,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn showplan_all_rows_build_a_plan_tree() {
        let plan = parse_mssql_explain_result(&showplan(vec![
            (1, 0, "", "", "1", "SELECT"),
            (2, 1, "Sort", "Sort", "ORDER BY:([Expr1] DESC)", "PLAN_ROW"),
            (3, 2, "Hash Match", "Inner Join", "HASH:([c].[CustomerId])", "PLAN_ROW"),
            (
                4,
                3,
                "Index Seek",
                "Index Seek",
                "OBJECT:([lab].[ops].[Orders].[IX_Orders_Customer] AS [o]), SEEK:([o].[CustomerId]=(1)) ORDERED FORWARD",
                "PLAN_ROW",
            ),
            (
                5,
                3,
                "Clustered Index Scan",
                "Clustered Index Scan",
                "OBJECT:([lab].[ops].[Customers].[PK_Customers] AS [c])",
                "PLAN_ROW",
            ),
        ]))
        .expect("plan");

        assert_eq!(plan.root.node_type, NodeType::Sort);
        let join = &plan.root.children[0];
        assert_eq!(join.node_type, NodeType::HashJoin);
        assert_eq!(join.join_type, Some(JoinType::Inner));
        assert_eq!(join.children.len(), 2);
        let seek = &join.children[0];
        assert_eq!(seek.node_type, NodeType::IndexScan);
        assert_eq!(seek.relation.as_deref(), Some("Orders"));
        assert_eq!(seek.schema.as_deref(), Some("ops"));
        assert_eq!(seek.alias.as_deref(), Some("o"));
        assert_eq!(seek.index_name.as_deref(), Some("IX_Orders_Customer"));
        assert_eq!(seek.index_cond.as_deref(), Some("[o].[CustomerId]=(1)"));
        assert_eq!(join.children[1].node_type, NodeType::SeqScan);
        assert_eq!(seek.rows, Some(30));
    }

    #[test]
    fn text_showplan_is_not_parsed() {
        let mut result = showplan(vec![(1, 0, "", "", "", "PLAN_ROW")]);
        result.rows.clear();
        assert!(parse_mssql_explain_result(&result).is_none());
    }

    #[test]
    fn object_paths_with_and_without_an_index() {
        let mut heap = PlanNode::new(NodeType::SeqScan);
        apply_object(&mut heap, "OBJECT:([shop].[dbo].[orders] AS [o])");
        assert_eq!(heap.schema.as_deref(), Some("dbo"));
        assert_eq!(heap.relation.as_deref(), Some("orders"));
        assert_eq!(heap.index_name, None);
        assert_eq!(heap.alias.as_deref(), Some("o"));

        let mut seek = PlanNode::new(NodeType::SeqScan);
        apply_object(&mut seek, "OBJECT:([shop].[dbo].[orders].[PK_orders]), SEEK:(x)");
        assert_eq!(seek.schema.as_deref(), Some("dbo"));
        assert_eq!(seek.relation.as_deref(), Some("orders"));
        assert_eq!(seek.index_name.as_deref(), Some("PK_orders"));
    }
}
