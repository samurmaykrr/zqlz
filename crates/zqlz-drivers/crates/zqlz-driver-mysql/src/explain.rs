use zqlz_analyzer::{explain::QueryPlan, parse_mysql_explain};
use zqlz_core::{QueryResult, Value};

pub fn parse_mysql_explain_result(result: &QueryResult) -> Option<QueryPlan> {
    mysql_query_result_to_plan_text(result)
        .and_then(|plan_text| parse_mysql_explain(&plan_text).ok())
}

pub fn mysql_query_result_to_plan_text(result: &QueryResult) -> Option<String> {
    if let Some(single_value) = result.rows.first().and_then(|row| {
        (result.columns.len() == 1)
            .then(|| row.values.first())
            .flatten()
    }) {
        let text = explain_value_to_text(single_value);
        let text = text.trim();
        if text.starts_with('{') {
            return Some(text.to_string());
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
            .map(explain_value_to_text)
            .collect::<Vec<_>>()
            .join("\t")
    }));

    (!lines.is_empty()).then(|| lines.join("\n"))
}

fn explain_value_to_text(value: &Value) -> String {
    match value {
        Value::Json(json) => json.to_string(),
        Value::String(text) => text.clone(),
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
}
