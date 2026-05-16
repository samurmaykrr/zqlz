use zqlz_analyzer::explain::{NodeCost, NodeType, PlanNode, QueryPlan};
use zqlz_core::QueryResult;

pub fn parse_mongodb_explain_result(raw_output: &QueryResult) -> Option<QueryPlan> {
    let explain = query_result_first_row_json(raw_output)?;
    if let Some(stages) = explain.get("stages").and_then(serde_json::Value::as_array) {
        return parse_mongodb_aggregate_stages(stages);
    }

    let query_planner = explain.get("queryPlanner").unwrap_or(&explain);
    let winning_plan = explain
        .pointer("/executionStats/executionStages")
        .or_else(|| query_planner.pointer("/winningPlan"))
        .or_else(|| query_planner.pointer("/winningPlan/queryPlan"))?;
    let mut root = parse_mongodb_plan_node(winning_plan)?;
    if let Some(namespace) = query_planner
        .get("namespace")
        .and_then(serde_json::Value::as_str)
    {
        set_mongodb_relation_if_missing(&mut root, namespace);
    }
    let mut plan = QueryPlan::new(root);
    plan.execution_time_ms = explain
        .pointer("/executionStats/executionTimeMillis")
        .and_then(serde_json::Value::as_f64);
    Some(plan)
}

fn parse_mongodb_aggregate_stages(stages: &[serde_json::Value]) -> Option<QueryPlan> {
    let mut nodes = stages
        .iter()
        .filter_map(parse_mongodb_aggregate_stage)
        .collect::<Vec<_>>();
    if nodes.is_empty() {
        return None;
    }
    let mut root = nodes.remove(0);
    for mut stage in nodes {
        stage.children.push(root);
        root = stage;
    }
    Some(QueryPlan::new(root))
}

fn parse_mongodb_aggregate_stage(stage: &serde_json::Value) -> Option<PlanNode> {
    let stage_object = stage.as_object()?;
    if let Some(cursor) = stage_object.get("$cursor") {
        let query_planner = cursor.get("queryPlanner")?;
        let plan_value = cursor
            .pointer("/executionStats/executionStages")
            .or_else(|| query_planner.pointer("/winningPlan"))
            .or_else(|| query_planner.pointer("/winningPlan/queryPlan"))?;
        let mut node = parse_mongodb_plan_node(plan_value)?;
        if let Some(namespace) = query_planner
            .get("namespace")
            .and_then(serde_json::Value::as_str)
        {
            set_mongodb_relation_if_missing(&mut node, namespace);
        }
        return Some(node);
    }

    let (stage_name, stage_body) = stage_object.iter().next()?;
    let mut node = PlanNode::new(mongodb_aggregate_stage_node_type(stage_name));
    node.description = Some(stage_name.trim_start_matches('$').to_uppercase());
    node.rows = stage
        .get("nReturned")
        .or_else(|| stage_body.get("nReturned"))
        .and_then(serde_json::Value::as_u64);
    node.filter = stage_body
        .as_object()
        .map(|_| stage_body.to_string())
        .filter(|text| text != "null");
    Some(node)
}

fn query_result_first_row_json(raw_output: &QueryResult) -> Option<serde_json::Value> {
    let row = raw_output.rows.first()?;
    let mut object = serde_json::Map::new();
    for (column, value) in row.columns().iter().zip(row.values.iter()) {
        object.insert(column.clone(), value.to_json_value());
    }
    Some(serde_json::Value::Object(object))
}

fn parse_mongodb_plan_node(value: &serde_json::Value) -> Option<PlanNode> {
    let stage = value
        .get("stage")
        .or_else(|| {
            value
                .get("queryPlan")
                .and_then(|query_plan| query_plan.get("stage"))
        })
        .and_then(serde_json::Value::as_str)
        .unwrap_or("UNKNOWN");
    let mut node = PlanNode::new(mongodb_stage_node_type(stage));
    node.description = Some(stage.to_string());
    node.rows = value
        .get("nReturned")
        .or_else(|| value.get("estimatedCardinality"))
        .and_then(serde_json::Value::as_u64);
    node.cost = value
        .get("estimatedCost")
        .and_then(serde_json::Value::as_f64)
        .map(|cost| NodeCost {
            startup: 0.0,
            total: cost,
        });
    node.index_name = value
        .get("indexName")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);
    node.filter = value
        .get("filter")
        .or_else(|| value.get("indexBounds"))
        .or_else(|| value.get("parsedTextQuery"))
        .map(|value| value.to_string());

    for key in ["inputStage", "outerStage", "innerStage"] {
        if let Some(child) = value.get(key).and_then(parse_mongodb_plan_node) {
            node.children.push(child);
        }
    }
    if let Some(children) = value
        .get("inputStages")
        .and_then(serde_json::Value::as_array)
    {
        node.children
            .extend(children.iter().filter_map(parse_mongodb_plan_node));
    }
    if let Some(children) = value.get("children").and_then(serde_json::Value::as_array) {
        node.children
            .extend(children.iter().filter_map(parse_mongodb_plan_node));
    }
    if let Some(shards) = value.get("shards").and_then(serde_json::Value::as_array) {
        node.children.extend(shards.iter().filter_map(|shard| {
            shard
                .pointer("/winningPlan")
                .or_else(|| shard.pointer("/executionStages"))
                .and_then(parse_mongodb_plan_node)
        }));
    }

    Some(node)
}

fn mongodb_stage_node_type(stage: &str) -> NodeType {
    match stage {
        "COLLSCAN" => NodeType::SeqScan,
        "IXSCAN" | "TEXT_MATCH" => NodeType::IndexScan,
        "TEXT_OR" => NodeType::BitmapOr,
        "SORT" => NodeType::Sort,
        "LIMIT" => NodeType::Limit,
        "SKIP" => NodeType::Result,
        "FETCH" => NodeType::SeqScan,
        "PROJECTION" | "PROJECTION_SIMPLE" | "PROJECTION_DEFAULT" | "PROJECTION_COVERED" => {
            NodeType::ProjectSet
        }
        "SHARD_MERGE" | "SHARDING_FILTER" => NodeType::Gather,
        _ => NodeType::Unknown,
    }
}

fn mongodb_aggregate_stage_node_type(stage: &str) -> NodeType {
    match stage {
        "$match" => NodeType::SeqScan,
        "$sort" => NodeType::Sort,
        "$limit" => NodeType::Limit,
        "$skip" => NodeType::Result,
        "$group" => NodeType::HashAggregate,
        "$project" | "$addFields" | "$set" | "$unset" => NodeType::ProjectSet,
        "$lookup" | "$graphLookup" => NodeType::HashJoin,
        "$unwind" => NodeType::SubqueryScan,
        "$unionWith" => NodeType::Append,
        "$facet" => NodeType::SubPlan,
        "$count" | "$sortByCount" => NodeType::Aggregate,
        _ => NodeType::Unknown,
    }
}

fn set_mongodb_relation_if_missing(node: &mut PlanNode, namespace: &str) {
    if node.relation.is_none() {
        node.relation = Some(namespace.to_string());
    }
    for child in &mut node.children {
        set_mongodb_relation_if_missing(child, namespace);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use zqlz_core::{ColumnMeta, Row, Value};

    #[test]
    fn mongodb_explain_query_result_builds_visual_plan_tree() {
        let explain = serde_json::json!({
            "queryPlanner": {
                "namespace": "zqlz_feature_lab.products",
                "winningPlan": {
                    "stage": "PROJECTION_SIMPLE",
                    "inputStage": {
                        "stage": "SORT",
                        "inputStage": {
                            "stage": "TEXT_MATCH",
                            "inputStage": {
                                "stage": "TEXT_OR"
                            }
                        }
                    }
                }
            }
        });
        let columns = explain
            .as_object()
            .expect("explain object")
            .keys()
            .enumerate()
            .map(|(ordinal, name)| ColumnMeta {
                name: name.clone(),
                data_type: "json".to_string(),
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
            .collect::<Vec<_>>();
        let values = explain
            .as_object()
            .expect("explain object")
            .values()
            .cloned()
            .map(Value::Json)
            .collect::<Vec<_>>();
        let result = QueryResult {
            id: Uuid::new_v4(),
            columns,
            rows: vec![Row::new(vec!["queryPlanner".to_string()], values)],
            total_rows: Some(1),
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms: 1,
            warnings: Vec::new(),
        };

        let plan = parse_mongodb_explain_result(&result).expect("parsed plan");

        assert_eq!(plan.root.description.as_deref(), Some("PROJECTION_SIMPLE"));
        assert_eq!(plan.root.node_type, NodeType::ProjectSet);
        assert_eq!(plan.root.children[0].node_type, NodeType::Sort);
        assert_eq!(
            plan.root.children[0].children[0].description.as_deref(),
            Some("TEXT_MATCH")
        );
        assert_eq!(
            plan.root.children[0].children[0].children[0]
                .description
                .as_deref(),
            Some("TEXT_OR")
        );
    }

    #[test]
    fn mongodb_aggregate_explain_stages_build_visual_plan_tree() {
        let explain = serde_json::json!({
            "stages": [
                {
                    "$cursor": {
                        "queryPlanner": {
                            "namespace": "zqlz_feature_lab.orders",
                            "winningPlan": {
                                "stage": "PROJECTION_SIMPLE",
                                "inputStage": {
                                    "stage": "COLLSCAN",
                                    "filter": { "status": { "$eq": "paid" } }
                                }
                            }
                        },
                        "executionStats": {
                            "executionStages": {
                                "stage": "PROJECTION_SIMPLE",
                                "nReturned": 5,
                                "inputStage": {
                                    "stage": "COLLSCAN",
                                    "nReturned": 5,
                                    "docsExamined": 15
                                }
                            }
                        }
                    }
                },
                {
                    "$group": { "_id": "$status", "total": { "$sum": "$orderTotal" } },
                    "nReturned": 1
                },
                {
                    "$sort": { "sortKey": { "total": -1 } },
                    "nReturned": 1
                }
            ]
        });
        let result = QueryResult {
            id: Uuid::new_v4(),
            columns: vec![ColumnMeta {
                name: "stages".to_string(),
                data_type: "json".to_string(),
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
                vec!["stages".to_string()],
                vec![Value::Json(explain.get("stages").expect("stages").clone())],
            )],
            total_rows: Some(1),
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms: 1,
            warnings: Vec::new(),
        };

        let plan = parse_mongodb_explain_result(&result).expect("parsed plan");

        assert_eq!(plan.root.node_type, NodeType::Sort);
        assert_eq!(plan.root.children[0].node_type, NodeType::HashAggregate);
        assert_eq!(
            plan.root.children[0].children[0].node_type,
            NodeType::ProjectSet
        );
        assert_eq!(
            plan.root.children[0].children[0].children[0].node_type,
            NodeType::SeqScan
        );
    }
}
