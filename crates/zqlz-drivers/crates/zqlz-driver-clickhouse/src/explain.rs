use zqlz_analyzer::explain::{NodeType, PlanNode, QueryPlan};
use zqlz_core::QueryResult;

/// Builds a plan tree from `EXPLAIN json = 1, indexes = 1`, which returns the
/// whole document as text. Plain `EXPLAIN` output returns `None`.
pub fn parse_clickhouse_explain_result(result: &QueryResult) -> Option<QueryPlan> {
    let text = result
        .rows
        .iter()
        .filter_map(|row| row.values.first()?.to_json_value().as_str().map(str::to_string))
        .collect::<Vec<_>>()
        .join("\n");
    let document: serde_json::Value = serde_json::from_str(&text).ok()?;
    let plan = document.get(0)?.get("Plan")?;
    Some(QueryPlan::new(parse_node(plan)?))
}

fn parse_node(value: &serde_json::Value) -> Option<PlanNode> {
    let kind = value.get("Node Type")?.as_str()?;
    let children: Vec<PlanNode> = value
        .get("Plans")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(parse_node)
        .collect();
    // Expression steps only rename and project columns, so they would bury the
    // operators that matter.
    if kind == "Expression" && children.len() == 1 {
        return children.into_iter().next();
    }

    let description = value.get("Description").and_then(serde_json::Value::as_str);
    let mut node = PlanNode::new(clickhouse_node_type(kind));
    node.description = Some(description.unwrap_or(kind).to_string());
    node.children = children;

    if kind == "ReadFromMergeTree" {
        if let Some((schema, table)) = description.and_then(|name| name.split_once('.')) {
            node.schema = Some(schema.to_string());
            node.relation = Some(table.to_string());
        } else {
            node.relation = description.map(ToString::to_string);
        }
        apply_index_pruning(&mut node, value);
    }
    Some(node)
}

/// Reads the `Indexes` of a MergeTree read: the scan counts as an index scan
/// when any index dropped granules, and the node notes how many survived.
fn apply_index_pruning(node: &mut PlanNode, value: &serde_json::Value) {
    let Some(indexes) = value.get("Indexes").and_then(serde_json::Value::as_array) else {
        return;
    };
    let granules = |index: &serde_json::Value, key: &str| {
        index.get(key).and_then(serde_json::Value::as_u64)
    };
    let initial = indexes.first().and_then(|index| granules(index, "Initial Granules"));
    let selected = indexes.last().and_then(|index| granules(index, "Selected Granules"));
    let pruning = indexes.iter().find(|index| {
        granules(index, "Selected Granules") < granules(index, "Initial Granules")
    });

    if let Some(index) = pruning {
        node.node_type = NodeType::IndexScan;
        node.index_name = index
            .get("Type")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string);
        node.index_cond = index
            .get("Condition")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string);
    }
    if let (Some(initial), Some(selected)) = (initial, selected) {
        let table = node.description.clone().unwrap_or_default();
        node.description = Some(format!("{table} · {selected}/{initial} granules"));
    }
}

fn clickhouse_node_type(kind: &str) -> NodeType {
    match kind {
        "ReadFromMergeTree" | "ReadFromStorage" | "ReadFromSystem" | "ReadFromMemoryStorage" => {
            NodeType::SeqScan
        }
        "ReadFromRemote" | "ReadFromCluster" => NodeType::ForeignScan,
        "Sorting" | "MergingSorted" | "PartialSorting" => NodeType::Sort,
        "Aggregating" => NodeType::HashAggregate,
        "MergingAggregated" => NodeType::Aggregate,
        "Join" => NodeType::HashJoin,
        "Limit" | "Offset" => NodeType::Limit,
        "Filter" => NodeType::Filter,
        "Distinct" => NodeType::Unique,
        "Union" => NodeType::Append,
        "Window" => NodeType::WindowAgg,
        "CreatingSets" => NodeType::SubPlan,
        "Expression" => NodeType::Result,
        _ => NodeType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use zqlz_core::{ColumnMeta, Row, Value};

    fn explain_result(document: serde_json::Value) -> QueryResult {
        let columns = vec!["explain".to_string()];
        QueryResult {
            id: Uuid::new_v4(),
            columns: vec![ColumnMeta {
                name: "explain".to_string(),
                data_type: "String".to_string(),
                nullable: true,
                ordinal: 0,
                max_length: None,
                precision: None,
                scale: None,
                auto_increment: false,
                default_value: None,
                comment: None,
                enum_values: None,
            }],
            rows: vec![Row::new(
                columns,
                vec![Value::String(document.to_string())],
            )],
            total_rows: Some(1),
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms: 0,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn json_explain_builds_a_plan_tree_without_expression_steps() {
        let plan = parse_clickhouse_explain_result(&explain_result(serde_json::json!([{
            "Plan": {
                "Node Type": "Expression",
                "Description": "Project names",
                "Plans": [{
                    "Node Type": "Sorting",
                    "Description": "Sorting for ORDER BY",
                    "Plans": [{
                        "Node Type": "Aggregating",
                        "Plans": [{
                            "Node Type": "Expression",
                            "Plans": [{
                                "Node Type": "ReadFromMergeTree",
                                "Description": "lab.events",
                                "Indexes": [
                                    {"Type": "Partition", "Condition": "(toYYYYMM(t) in [202606, 202606])",
                                     "Initial Granules": 5, "Selected Granules": 1},
                                    {"Type": "PrimaryKey", "Initial Granules": 1, "Selected Granules": 1}
                                ]
                            }]
                        }]
                    }]
                }]
            }
        }])))
        .expect("plan");

        assert_eq!(plan.root.node_type, NodeType::Sort);
        let aggregate = &plan.root.children[0];
        assert_eq!(aggregate.node_type, NodeType::HashAggregate);
        let scan = &aggregate.children[0];
        assert_eq!(scan.node_type, NodeType::IndexScan);
        assert_eq!(scan.schema.as_deref(), Some("lab"));
        assert_eq!(scan.relation.as_deref(), Some("events"));
        assert_eq!(scan.index_name.as_deref(), Some("Partition"));
        assert_eq!(scan.description.as_deref(), Some("lab.events · 1/5 granules"));
    }

    #[test]
    fn plain_text_explain_is_not_parsed() {
        let result = explain_result(serde_json::Value::String("Expression".to_string()));
        assert!(parse_clickhouse_explain_result(&result).is_none());
    }
}
