use zqlz_analyzer::{explain::QueryPlan, parse_sqlite_explain};
use zqlz_core::{QueryResult, Value};

pub fn parse_sqlite_explain_result(result: &QueryResult) -> Option<QueryPlan> {
    sqlite_query_result_to_plan_text(result)
        .and_then(|plan_text| parse_sqlite_explain(&plan_text).ok())
}

pub fn sqlite_query_result_to_plan_text(result: &QueryResult) -> Option<String> {
    let detail_index = result
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case("detail"));

    let lines = result
        .rows
        .iter()
        .filter_map(|row| {
            let value = detail_index
                .and_then(|index| row.values.get(index))
                .or_else(|| row.values.last())?;
            let text = explain_value_to_text(value);
            let text = text.trim();
            (!text.is_empty()).then(|| text.to_string())
        })
        .collect::<Vec<_>>();

    (!lines.is_empty()).then(|| lines.join("\n"))
}

fn explain_value_to_text(value: &Value) -> String {
    match value {
        Value::Json(json) => json.to_string(),
        Value::String(text) => text.clone(),
        _ => value.to_string(),
    }
}
