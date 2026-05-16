use zqlz_analyzer::{explain::QueryPlan, parse_postgres_explain};
use zqlz_core::{QueryResult, Value};

pub fn parse_postgres_explain_result(result: &QueryResult) -> Option<QueryPlan> {
    let first_value = result.rows.first().and_then(|row| row.values.first())?;

    if let Some(plan) = parse_postgres_json_value(first_value) {
        return Some(plan);
    }

    let plan_text = result
        .rows
        .iter()
        .filter_map(|row| row.values.first())
        .map(explain_value_to_text)
        .collect::<Vec<_>>()
        .join("\n");

    if plan_text.trim().is_empty() {
        None
    } else {
        parse_postgres_explain(&plan_text).ok()
    }
}

fn parse_postgres_json_value(value: &Value) -> Option<QueryPlan> {
    match value {
        Value::Json(json) => parse_postgres_explain(&json.to_string()).ok(),
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.starts_with('[') || trimmed.starts_with('{') {
                parse_postgres_explain(trimmed).ok()
            } else {
                None
            }
        }
        _ => {
            let text = value.to_string();
            let trimmed = text.trim();
            if trimmed.starts_with('[') || trimmed.starts_with('{') {
                parse_postgres_explain(trimmed).ok()
            } else {
                None
            }
        }
    }
}

fn explain_value_to_text(value: &Value) -> String {
    match value {
        Value::Json(json) => json.to_string(),
        Value::String(text) => text.clone(),
        _ => value.to_string(),
    }
}
