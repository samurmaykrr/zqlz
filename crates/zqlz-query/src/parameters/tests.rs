//! Tests for parameter extraction

use super::extractor::{
    Parameter, ParameterStyle, extract_parameters, extract_parameters_with_style,
};

#[test]
fn test_extract_named_parameters_colon() {
    let sql = "SELECT * FROM users WHERE id = :id AND name = :name";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 2);
    assert_eq!(params[0], Parameter::Named("id".into()));
    assert_eq!(params[1], Parameter::Named("name".into()));
}

#[test]
fn test_extract_named_parameters_at_sign() {
    let sql = "SELECT * FROM users WHERE id = @userId AND status = @status";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 2);
    assert_eq!(params[0], Parameter::Named("userId".into()));
    assert_eq!(params[1], Parameter::Named("status".into()));
}

#[test]
fn test_extract_positional_parameters_dollar() {
    let sql = "SELECT * FROM users WHERE id = $1 AND name = $2";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 2);
    assert_eq!(params[0], Parameter::Positional(1));
    assert_eq!(params[1], Parameter::Positional(2));
}

#[test]
fn test_extract_positional_parameters_question_mark() {
    let sql = "SELECT * FROM users WHERE id = ? AND name = ?";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 2);
    assert_eq!(params[0], Parameter::Positional(1));
    assert_eq!(params[1], Parameter::Positional(2));
}

#[test]
fn test_extract_mixed_parameters() {
    let sql = "SELECT * FROM users WHERE id = :id AND status = $1";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 2);
    assert!(params.contains(&Parameter::Named("id".into())));
    assert!(params.contains(&Parameter::Positional(1)));
}

#[test]
fn test_duplicate_named_parameters() {
    let sql = "SELECT * FROM users WHERE id = :id OR parent_id = :id";
    let params = extract_parameters(sql);

    // Duplicates should only appear once
    assert_eq!(params.len(), 1);
    assert_eq!(params[0], Parameter::Named("id".into()));
}

#[test]
fn test_duplicate_positional_parameters() {
    let sql = "SELECT * FROM users WHERE id = $1 OR parent_id = $1";
    let params = extract_parameters(sql);

    // Duplicates should only appear once
    assert_eq!(params.len(), 1);
    assert_eq!(params[0], Parameter::Positional(1));
}

#[test]
fn test_no_parameters() {
    let sql = "SELECT * FROM users WHERE id = 1";
    let params = extract_parameters(sql);

    assert!(params.is_empty());
}

#[test]
fn test_parameters_in_string_literal_ignored() {
    let sql = "SELECT * FROM users WHERE name = ':not_a_param' AND id = :id";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 1);
    assert_eq!(params[0], Parameter::Named("id".into()));
}

#[test]
fn test_parameters_in_single_line_comment_ignored() {
    let sql = "SELECT * FROM users WHERE id = :id -- comment with :fake_param";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 1);
    assert_eq!(params[0], Parameter::Named("id".into()));
}

#[test]
fn test_parameters_in_multiline_comment_ignored() {
    let sql = "SELECT * FROM users /* :fake_param */ WHERE id = :id";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 1);
    assert_eq!(params[0], Parameter::Named("id".into()));
}

#[test]
fn test_parameter_style_detection_colon() {
    let sql = "SELECT * FROM users WHERE id = :id";
    let result = extract_parameters_with_style(sql);

    assert_eq!(result.style, Some(ParameterStyle::ColonNamed));
}

#[test]
fn test_parameter_style_detection_at_sign() {
    let sql = "SELECT * FROM users WHERE id = @id";
    let result = extract_parameters_with_style(sql);

    assert_eq!(result.style, Some(ParameterStyle::AtNamed));
}

#[test]
fn test_parameter_style_detection_dollar_positional() {
    let sql = "SELECT * FROM users WHERE id = $1";
    let result = extract_parameters_with_style(sql);

    assert_eq!(result.style, Some(ParameterStyle::DollarPositional));
}

#[test]
fn test_parameter_style_detection_question_mark() {
    let sql = "SELECT * FROM users WHERE id = ?";
    let result = extract_parameters_with_style(sql);

    assert_eq!(result.style, Some(ParameterStyle::QuestionMark));
}

#[test]
fn test_parameter_style_detection_mixed() {
    let sql = "SELECT * FROM users WHERE id = :id AND status = $1";
    let result = extract_parameters_with_style(sql);

    assert_eq!(result.style, Some(ParameterStyle::Mixed));
}

#[test]
fn test_parameter_style_detection_none() {
    let sql = "SELECT * FROM users WHERE id = 1";
    let result = extract_parameters_with_style(sql);

    assert_eq!(result.style, None);
}

#[test]
fn test_parameter_methods_named() {
    let param = Parameter::Named("test".into());

    assert!(param.is_named());
    assert!(!param.is_positional());
    assert_eq!(param.name(), Some("test"));
    assert_eq!(param.position(), None);
}

#[test]
fn test_parameter_methods_positional() {
    let param = Parameter::Positional(5);

    assert!(!param.is_named());
    assert!(param.is_positional());
    assert_eq!(param.name(), None);
    assert_eq!(param.position(), Some(5));
}

#[test]
fn test_complex_query_with_multiple_parameters() {
    let sql = r#"
        SELECT u.id, u.name, o.total
        FROM users u
        JOIN orders o ON o.user_id = u.id
        WHERE u.created_at > :start_date
          AND u.created_at < :end_date
          AND u.status = :status
          AND o.amount >= :min_amount
        ORDER BY o.created_at DESC
        LIMIT :limit OFFSET :offset
    "#;

    let params = extract_parameters(sql);

    assert_eq!(params.len(), 6);
    assert!(params.contains(&Parameter::Named("start_date".into())));
    assert!(params.contains(&Parameter::Named("end_date".into())));
    assert!(params.contains(&Parameter::Named("status".into())));
    assert!(params.contains(&Parameter::Named("min_amount".into())));
    assert!(params.contains(&Parameter::Named("limit".into())));
    assert!(params.contains(&Parameter::Named("offset".into())));
}

#[test]
fn test_escaped_string_with_quote() {
    let sql = "SELECT * FROM users WHERE name = 'O\\'Brien' AND id = :id";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 1);
    assert_eq!(params[0], Parameter::Named("id".into()));
}

#[test]
fn test_underscore_in_parameter_name() {
    let sql = "SELECT * FROM users WHERE first_name = :first_name AND last_name = :last_name";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 2);
    assert_eq!(params[0], Parameter::Named("first_name".into()));
    assert_eq!(params[1], Parameter::Named("last_name".into()));
}

#[test]
fn test_numbers_in_parameter_name() {
    let sql = "SELECT * FROM users WHERE field1 = :field1 AND field2 = :field2";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 2);
    assert_eq!(params[0], Parameter::Named("field1".into()));
    assert_eq!(params[1], Parameter::Named("field2".into()));
}

#[test]
fn test_large_positional_number() {
    let sql = "SELECT * FROM users WHERE id = $100";
    let params = extract_parameters(sql);

    assert_eq!(params.len(), 1);
    assert_eq!(params[0], Parameter::Positional(100));
}

// =============================================================================
// Binder Tests
// =============================================================================

use super::binder::{BindError, bind_named, bind_positional};
use std::collections::HashMap;
use zqlz_core::Value;

#[test]
fn test_bind_named_parameters_colon() {
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Int64(42));
    params.insert("name".to_string(), Value::String("Alice".to_string()));

    let result = bind_named(
        "SELECT * FROM users WHERE id = :id AND name = :name",
        &params,
    )
    .unwrap();

    assert_eq!(
        result.sql,
        "SELECT * FROM users WHERE id = $1 AND name = $2"
    );
    assert_eq!(result.values.len(), 2);
    assert_eq!(result.values[0], Value::Int64(42));
    assert_eq!(result.values[1], Value::String("Alice".to_string()));
}

#[test]
fn test_bind_named_parameters_at_sign() {
    let mut params = HashMap::new();
    params.insert("userId".to_string(), Value::Int64(99));
    params.insert("status".to_string(), Value::String("active".to_string()));

    let result = bind_named(
        "SELECT * FROM users WHERE id = @userId AND status = @status",
        &params,
    )
    .unwrap();

    assert_eq!(
        result.sql,
        "SELECT * FROM users WHERE id = $1 AND status = $2"
    );
    assert_eq!(result.values.len(), 2);
}

#[test]
fn test_bind_named_parameters_dollar() {
    let mut params = HashMap::new();
    params.insert("user_id".to_string(), Value::Int64(123));

    let result = bind_named("SELECT * FROM users WHERE id = $user_id", &params).unwrap();

    assert_eq!(result.sql, "SELECT * FROM users WHERE id = $1");
    assert_eq!(result.values.len(), 1);
    assert_eq!(result.values[0], Value::Int64(123));
}

#[test]
fn test_bind_parameter_reuse() {
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Int64(5));

    let result = bind_named(
        "SELECT * FROM users WHERE id = :id OR parent_id = :id OR manager_id = :id",
        &params,
    )
    .unwrap();

    assert_eq!(
        result.sql,
        "SELECT * FROM users WHERE id = $1 OR parent_id = $1 OR manager_id = $1"
    );
    assert_eq!(result.values.len(), 1);
    assert_eq!(result.values[0], Value::Int64(5));
}

#[test]
fn test_bind_multiple_parameters_with_reuse() {
    let mut params = HashMap::new();
    params.insert("a".to_string(), Value::Int64(1));
    params.insert("b".to_string(), Value::Int64(2));

    let result = bind_named("SELECT :a, :b, :a, :b, :a", &params).unwrap();

    assert_eq!(result.sql, "SELECT $1, $2, $1, $2, $1");
    assert_eq!(result.values.len(), 2);
    assert_eq!(result.values[0], Value::Int64(1));
    assert_eq!(result.values[1], Value::Int64(2));
}

#[test]
fn test_bind_missing_parameter_error() {
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Int64(42));

    let result = bind_named(
        "SELECT * FROM users WHERE id = :id AND name = :name",
        &params,
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err, BindError::MissingParameter("name".to_string()));
    assert_eq!(err.to_string(), "missing parameter: name");
}

#[test]
fn test_bind_positional_parameters() {
    let values = vec![Value::Int64(42), Value::String("Alice".to_string())];

    let result =
        bind_positional("SELECT * FROM users WHERE id = $1 AND name = $2", &values).unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0], Value::Int64(42));
    assert_eq!(result[1], Value::String("Alice".to_string()));
}

#[test]
fn test_bind_positional_with_reuse() {
    let values = vec![Value::Int64(5)];

    let result = bind_positional(
        "SELECT * FROM users WHERE id = $1 OR parent_id = $1",
        &values,
    )
    .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0], Value::Int64(5));
}

#[test]
fn test_bind_positional_missing_error() {
    let values = vec![Value::Int64(42)];

    let result = bind_positional("SELECT * FROM users WHERE id = $1 AND name = $2", &values);

    assert!(result.is_err());
    match result.unwrap_err() {
        BindError::MissingPositionalParameter(pos) => assert_eq!(pos, 2),
        _ => panic!("expected MissingPositionalParameter error"),
    }
}

#[test]
fn test_bind_positional_question_mark() {
    let values = vec![Value::Int64(42), Value::String("Bob".to_string())];

    let result = bind_positional("SELECT * FROM users WHERE id = ? AND name = ?", &values).unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0], Value::Int64(42));
    assert_eq!(result[1], Value::String("Bob".to_string()));
}

#[test]
fn test_bind_positional_count_mismatch() {
    let values = vec![Value::Int64(42)];

    let result = bind_positional("SELECT * FROM users WHERE id = ? AND name = ?", &values);

    assert!(result.is_err());
    match result.unwrap_err() {
        BindError::ParameterCountMismatch { expected, actual } => {
            assert_eq!(expected, 2);
            assert_eq!(actual, 1);
        }
        _ => panic!("expected ParameterCountMismatch error"),
    }
}

#[test]
fn test_bind_named_ignores_params_in_strings() {
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Int64(42));

    let result = bind_named(
        "SELECT * FROM users WHERE name = ':not_a_param' AND id = :id",
        &params,
    )
    .unwrap();

    assert_eq!(
        result.sql,
        "SELECT * FROM users WHERE name = ':not_a_param' AND id = $1"
    );
    assert_eq!(result.values.len(), 1);
}

#[test]
fn test_bind_named_ignores_params_in_comments() {
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Int64(42));

    let result = bind_named(
        "SELECT * FROM users WHERE id = :id -- comment with :fake_param",
        &params,
    )
    .unwrap();

    assert_eq!(
        result.sql,
        "SELECT * FROM users WHERE id = $1 -- comment with :fake_param"
    );
    assert_eq!(result.values.len(), 1);
}

#[test]
fn test_bind_complex_query() {
    let mut params = HashMap::new();
    params.insert("start".to_string(), Value::String("2024-01-01".to_string()));
    params.insert("end".to_string(), Value::String("2024-12-31".to_string()));
    params.insert("status".to_string(), Value::String("active".to_string()));
    params.insert("limit".to_string(), Value::Int64(100));

    let sql = r#"
        SELECT u.id, u.name
        FROM users u
        WHERE u.created_at BETWEEN :start AND :end
          AND u.status = :status
        LIMIT :limit
    "#;

    let result = bind_named(sql, &params).unwrap();

    assert!(result.sql.contains("$1"));
    assert!(result.sql.contains("$2"));
    assert!(result.sql.contains("$3"));
    assert!(result.sql.contains("$4"));
    assert!(!result.sql.contains(":start"));
    assert!(!result.sql.contains(":end"));
    assert!(!result.sql.contains(":status"));
    assert!(!result.sql.contains(":limit"));
    assert_eq!(result.values.len(), 4);
}

#[test]
fn test_bind_named_empty_query() {
    let params = HashMap::new();

    let result = bind_named("SELECT * FROM users", &params).unwrap();

    assert_eq!(result.sql, "SELECT * FROM users");
    assert!(result.values.is_empty());
}

#[test]
fn test_bind_positional_empty_query() {
    let values: Vec<Value> = vec![];

    let result = bind_positional("SELECT * FROM users", &values).unwrap();

    assert!(result.is_empty());
}

#[test]
fn test_bind_named_with_various_value_types() {
    let mut params = HashMap::new();
    params.insert("null_val".to_string(), Value::Null);
    params.insert("bool_val".to_string(), Value::Bool(true));
    params.insert("int_val".to_string(), Value::Int64(42));
    params.insert("float_val".to_string(), Value::Float64(3.14));
    params.insert("str_val".to_string(), Value::String("hello".to_string()));

    let sql = "SELECT :null_val, :bool_val, :int_val, :float_val, :str_val";
    let result = bind_named(sql, &params).unwrap();

    assert_eq!(result.sql, "SELECT $1, $2, $3, $4, $5");
    assert_eq!(result.values.len(), 5);
}
