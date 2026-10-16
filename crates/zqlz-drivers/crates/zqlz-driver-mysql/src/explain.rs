use zqlz_analyzer::{explain::QueryPlan, parse_mysql_explain};
use zqlz_core::{QueryResult, Value};

pub fn parse_mysql_explain_result(result: &QueryResult) -> Option<QueryPlan> {
    mysql_query_result_to_plan_text(result)
        .and_then(|plan_text| parse_mysql_explain(&plan_text).ok())
}

/// Flattens an EXPLAIN result into the text `parse_mysql_explain` understands.
///
/// `FORMAT=JSON`, `FORMAT=TREE` and `ANALYZE` answer with one text column (a JSON
/// document or an indented tree); those come back verbatim. The classic
/// `EXPLAIN` answers with a table, which becomes a header line plus
/// tab-separated rows (`NULL` kept as the word, so columns stay aligned).
pub fn mysql_query_result_to_plan_text(result: &QueryResult) -> Option<String> {
    if result.columns.len() == 1 {
        let text = result
            .rows
            .iter()
            .filter_map(|row| row.values.first())
            .map(explain_value_to_text)
            .collect::<Vec<_>>()
            .join("\n");
        let trimmed = text.trim_start();
        if trimmed.starts_with('{') || trimmed.starts_with("->") {
            return Some(text);
        }
    }

    let mut lines = Vec::new();
    if !result.columns.is_empty() {
        lines.push(
            result
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
                .join("\t"),
        );
    }

    lines.extend(result.rows.iter().map(|row| {
        row.values
            .iter()
            .map(|value| explain_value_to_text(value).replace(['\t', '\n', '\r'], " "))
            .collect::<Vec<_>>()
            .join("\t")
    }));

    (!lines.is_empty()).then(|| lines.join("\n"))
}

fn explain_value_to_text(value: &Value) -> String {
    match value {
        Value::Json(json) => json.to_string(),
        Value::String(text) => text.clone(),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_analyzer::parse_mysql_explain;
    use zqlz_core::{ColumnMeta, Row};

    #[test]
    fn mysql_query_result_to_plan_text_tabular() {
        let columns = vec![
            "id",
            "select_type",
            "table",
            "type",
            "possible_keys",
            "key",
            "key_len",
            "ref",
            "rows",
            "filtered",
            "Extra",
        ]
        .into_iter()
        .enumerate()
        .map(|(ordinal, name)| ColumnMeta {
            name: name.to_string(),
            ordinal,
            ..ColumnMeta::default()
        })
        .collect::<Vec<_>>();
        let column_names = columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let mut result = QueryResult::empty();
        result.columns = columns;
        result.rows = vec![Row::new(
            column_names,
            vec![
                Value::Int32(1),
                Value::String("SIMPLE".to_string()),
                Value::String("users".to_string()),
                Value::String("ALL".to_string()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int64(100),
                Value::Decimal("100.00".to_string()),
                Value::String("Using where".to_string()),
            ],
        )];

        let text = mysql_query_result_to_plan_text(&result).expect("plan text");
        assert!(text.starts_with("id\tselect_type\ttable\ttype"));
        assert!(text.contains("1\tSIMPLE\tusers\tALL"));
        assert!(parse_mysql_explain(&text).is_ok());
    }

    fn text_result(column: &str, rows: &[&str]) -> QueryResult {
        let mut result = QueryResult::empty();
        result.columns = vec![ColumnMeta {
            name: column.to_string(),
            ordinal: 0,
            ..ColumnMeta::default()
        }];
        result.rows = rows
            .iter()
            .map(|text| Row::new(vec![column.to_string()], vec![Value::String(text.to_string())]))
            .collect();
        result
    }

    /// MySQL 8 returns twelve columns: `partitions`, `possible_keys`, `key`, `ref`
    /// and `Extra` are NULL for a plain scan. Reading those NULLs as an access
    /// type once produced a single "Unknown operation" node.
    #[test]
    fn classic_mysql8_row_with_null_columns_parses_as_a_table_scan() {
        let names = [
            "id",
            "select_type",
            "table",
            "partitions",
            "type",
            "possible_keys",
            "key",
            "key_len",
            "ref",
            "rows",
            "filtered",
            "Extra",
        ];
        let mut result = QueryResult::empty();
        result.columns = names
            .iter()
            .enumerate()
            .map(|(ordinal, name)| ColumnMeta {
                name: name.to_string(),
                ordinal,
                ..ColumnMeta::default()
            })
            .collect();
        result.rows = vec![Row::new(
            names.iter().map(|name| name.to_string()).collect(),
            vec![
                Value::Int64(1),
                Value::String("SIMPLE".into()),
                Value::String("orders_order_items".into()),
                Value::Null,
                Value::String("ALL".into()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int64(488256),
                Value::Decimal("100.00".into()),
                Value::Null,
            ],
        )];

        let plan = parse_mysql_explain_result(&result).expect("plan");
        assert_eq!(plan.root.node_type, zqlz_analyzer::explain::NodeType::SeqScan);
        assert_eq!(plan.root.relation.as_deref(), Some("orders_order_items"));
        assert_eq!(plan.root.rows, Some(488256));
    }

    #[test]
    fn tree_output_in_one_multiline_cell_parses() {
        let tree = "-> Limit: 5 row(s)  (cost=58297 rows=5)\n    -> Table scan on orders_order_items  (cost=58297 rows=488256)\n";
        let plan = parse_mysql_explain_result(&text_result("EXPLAIN", &[tree])).expect("plan");
        assert_eq!(plan.root.node_type, zqlz_analyzer::explain::NodeType::Limit);
        assert_eq!(plan.root.children.len(), 1);
        assert_eq!(plan.total_cost, Some(58297.0));
    }

    #[test]
    fn tree_output_split_over_rows_parses() {
        let rows = [
            "-> Limit: 5 row(s)  (cost=1 rows=5)",
            "    -> Table scan on t  (cost=1 rows=9)",
        ];
        let plan = parse_mysql_explain_result(&text_result("EXPLAIN", &rows)).expect("plan");
        assert_eq!(plan.root.children.len(), 1);
    }

    #[test]
    fn json_output_parses() {
        let json = r#"{"query_block": {"select_id": 1, "table": {"table_name": "t", "access_type": "ALL", "rows_examined_per_scan": 3}}}"#;
        let plan = parse_mysql_explain_result(&text_result("EXPLAIN", &[json])).expect("plan");
        assert_eq!(plan.root.rows, Some(3));
    }

    #[test]
    fn unparseable_output_is_none() {
        assert!(parse_mysql_explain_result(&text_result("EXPLAIN", &["nonsense"])).is_none());
        assert!(parse_mysql_explain_result(&QueryResult::empty()).is_none());
    }
}
