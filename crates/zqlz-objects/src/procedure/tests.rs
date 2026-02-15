//! Tests for procedure executor

use super::*;
use uuid::Uuid;
use zqlz_core::{ColumnMeta, QueryResult, Row, Value};

// ============================================================================
// ParameterMode Tests
// ============================================================================

#[test]
fn test_parameter_mode_in_is_input() {
    let mode = ParameterMode::In;
    assert!(mode.is_input());
    assert!(!mode.is_output());
}

#[test]
fn test_parameter_mode_out_is_output() {
    let mode = ParameterMode::Out;
    assert!(!mode.is_input());
    assert!(mode.is_output());
}

#[test]
fn test_parameter_mode_inout_is_both() {
    let mode = ParameterMode::InOut;
    assert!(mode.is_input());
    assert!(mode.is_output());
}

#[test]
fn test_parameter_mode_default() {
    let mode = ParameterMode::default();
    assert_eq!(mode, ParameterMode::In);
}

#[test]
fn test_parameter_mode_serialization() {
    let mode = ParameterMode::InOut;
    let json = serde_json::to_string(&mode).unwrap();
    assert_eq!(json, "\"in_out\"");

    let deserialized: ParameterMode = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, ParameterMode::InOut);
}

// ============================================================================
// ProcedureParameter Tests
// ============================================================================

#[test]
fn test_procedure_parameter_input() {
    let param = ProcedureParameter::input("user_id", Value::Int32(42));

    assert_eq!(param.name, "user_id");
    assert_eq!(param.mode, ParameterMode::In);
    assert_eq!(param.value, Some(Value::Int32(42)));
    assert!(param.data_type.is_none());
}

#[test]
fn test_procedure_parameter_output() {
    let param = ProcedureParameter::output("result");

    assert_eq!(param.name, "result");
    assert_eq!(param.mode, ParameterMode::Out);
    assert!(param.value.is_none());
}

#[test]
fn test_procedure_parameter_inout() {
    let param = ProcedureParameter::inout("counter", Value::Int64(100));

    assert_eq!(param.name, "counter");
    assert_eq!(param.mode, ParameterMode::InOut);
    assert_eq!(param.value, Some(Value::Int64(100)));
}

#[test]
fn test_procedure_parameter_with_type() {
    let param = ProcedureParameter::input("name", Value::String("Alice".to_string()))
        .with_type("VARCHAR(255)");

    assert_eq!(param.data_type, Some("VARCHAR(255)".to_string()));
}

// ============================================================================
// ProcedureDialect Tests
// ============================================================================

#[test]
fn test_dialect_call_keyword_postgresql() {
    assert_eq!(ProcedureDialect::PostgreSQL.call_keyword(), "CALL");
}

#[test]
fn test_dialect_call_keyword_mysql() {
    assert_eq!(ProcedureDialect::MySQL.call_keyword(), "CALL");
}

#[test]
fn test_dialect_call_keyword_mssql() {
    assert_eq!(ProcedureDialect::MsSql.call_keyword(), "EXEC");
}

#[test]
fn test_dialect_call_keyword_sqlite() {
    assert_eq!(ProcedureDialect::SQLite.call_keyword(), "SELECT");
}

#[test]
fn test_dialect_supports_procedures() {
    assert!(ProcedureDialect::PostgreSQL.supports_procedures());
    assert!(ProcedureDialect::MySQL.supports_procedures());
    assert!(ProcedureDialect::MsSql.supports_procedures());
    assert!(!ProcedureDialect::SQLite.supports_procedures());
}

#[test]
fn test_dialect_supports_out_params() {
    assert!(ProcedureDialect::PostgreSQL.supports_out_params());
    assert!(ProcedureDialect::MySQL.supports_out_params());
    assert!(ProcedureDialect::MsSql.supports_out_params());
    assert!(!ProcedureDialect::SQLite.supports_out_params());
}

// ============================================================================
// ProcedureExecutor - Build Call Statement Tests
// ============================================================================

#[test]
fn test_build_call_statement_postgresql_no_params() {
    let executor = ProcedureExecutor::new(ProcedureDialect::PostgreSQL);
    let (sql, values) = executor.build_call_statement("do_something", &[]);

    assert_eq!(sql, "CALL do_something()");
    assert!(values.is_empty());
}

#[test]
fn test_build_call_statement_postgresql_with_params() {
    let executor = ProcedureExecutor::new(ProcedureDialect::PostgreSQL);
    let params = vec![
        ProcedureParameter::input("user_id", Value::Int32(42)),
        ProcedureParameter::input("name", Value::String("Alice".to_string())),
    ];
    let (sql, values) = executor.build_call_statement("create_user", &params);

    assert_eq!(sql, "CALL create_user($1, $2)");
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], Value::Int32(42));
    assert_eq!(values[1], Value::String("Alice".to_string()));
}

#[test]
fn test_build_call_statement_mysql_with_params() {
    let executor = ProcedureExecutor::new(ProcedureDialect::MySQL);
    let params = vec![
        ProcedureParameter::input("id", Value::Int32(1)),
        ProcedureParameter::input("status", Value::String("active".to_string())),
    ];
    let (sql, values) = executor.build_call_statement("update_status", &params);

    assert_eq!(sql, "CALL update_status(?, ?)");
    assert_eq!(values.len(), 2);
}

#[test]
fn test_build_call_statement_mssql_with_params() {
    let executor = ProcedureExecutor::new(ProcedureDialect::MsSql);
    let params = vec![ProcedureParameter::input("id", Value::Int32(1))];
    let (sql, values) = executor.build_call_statement("sp_GetUser", &params);

    assert_eq!(sql, "EXEC sp_GetUser(@p1)");
    assert_eq!(values.len(), 1);
}

#[test]
fn test_build_call_statement_with_out_param_mysql() {
    let executor = ProcedureExecutor::new(ProcedureDialect::MySQL);
    let params = vec![
        ProcedureParameter::input("user_id", Value::Int32(42)),
        ProcedureParameter::output("total"),
    ];
    let (sql, values) = executor.build_call_statement("get_user_total", &params);

    assert_eq!(sql, "CALL get_user_total(?, @total)");
    assert_eq!(values.len(), 1);
}

#[test]
fn test_build_call_statement_with_out_param_mssql() {
    let executor = ProcedureExecutor::new(ProcedureDialect::MsSql);
    let params = vec![
        ProcedureParameter::input("id", Value::Int32(1)),
        ProcedureParameter::output("result"),
    ];
    let (sql, values) = executor.build_call_statement("sp_Calculate", &params);

    assert_eq!(sql, "EXEC sp_Calculate(@p1, @result OUTPUT)");
    assert_eq!(values.len(), 1);
}

#[test]
fn test_build_call_statement_with_null_input() {
    let executor = ProcedureExecutor::new(ProcedureDialect::PostgreSQL);
    let params = vec![ProcedureParameter {
        name: "optional_param".to_string(),
        value: None,
        mode: ParameterMode::In,
        data_type: None,
    }];
    let (sql, _values) = executor.build_call_statement("proc_with_null", &params);

    assert_eq!(sql, "CALL proc_with_null(NULL)");
}

#[test]
fn test_build_call_statement_qualified_name() {
    let executor = ProcedureExecutor::new(ProcedureDialect::PostgreSQL);
    let params = vec![ProcedureParameter::input("val", Value::Int32(1))];
    let (sql, _) = executor.build_call_statement("schema.procedure_name", &params);

    assert_eq!(sql, "CALL schema.procedure_name($1)");
}

// ============================================================================
// ProcedureExecutor - Build Function Call Tests
// ============================================================================

#[test]
fn test_build_function_call_postgresql() {
    let executor = ProcedureExecutor::new(ProcedureDialect::PostgreSQL);
    let params = vec![
        ProcedureParameter::input("a", Value::Int32(10)),
        ProcedureParameter::input("b", Value::Int32(20)),
    ];
    let (sql, values) = executor.build_function_call("add_numbers", &params);

    assert_eq!(sql, "SELECT add_numbers($1, $2)");
    assert_eq!(values.len(), 2);
}

#[test]
fn test_build_function_call_no_params() {
    let executor = ProcedureExecutor::new(ProcedureDialect::PostgreSQL);
    let (sql, values) = executor.build_function_call("now", &[]);

    assert_eq!(sql, "SELECT now()");
    assert!(values.is_empty());
}

// ============================================================================
// ProcedureResult Tests
// ============================================================================

#[test]
fn test_procedure_result_empty() {
    let result = ProcedureResult::empty();

    assert!(result.output_params.is_empty());
    assert!(result.result_sets.is_empty());
    assert!(result.return_value.is_none());
    assert_eq!(result.execution_time_ms, 0);
}

#[test]
fn test_procedure_result_get_output() {
    let mut result = ProcedureResult::empty();
    result
        .output_params
        .insert("total".to_string(), Value::Int64(100));

    assert_eq!(result.get_output("total"), Some(&Value::Int64(100)));
    assert_eq!(result.get_output("nonexistent"), None);
}

#[test]
fn test_procedure_result_has_result_sets() {
    let mut result = ProcedureResult::empty();
    assert!(!result.has_result_sets());

    result.result_sets.push(QueryResult::empty());
    assert!(result.has_result_sets());
}

#[test]
fn test_procedure_result_first_result_set() {
    let result = ProcedureResult::empty();
    assert!(result.first_result_set().is_none());

    let mut result_with_sets = ProcedureResult::empty();
    result_with_sets.result_sets.push(QueryResult::empty());
    assert!(result_with_sets.first_result_set().is_some());
}

// ============================================================================
// ProcedureExecutor - Parse Output Params Tests
// ============================================================================

#[test]
fn test_parse_output_params_empty_result() {
    let executor = ProcedureExecutor::new(ProcedureDialect::PostgreSQL);
    let result = QueryResult::empty();
    let params = vec![ProcedureParameter::output("result")];

    let output = executor.parse_output_params(&result, &params);
    assert!(output.is_empty());
}

#[test]
fn test_parse_output_params_no_output_params() {
    let executor = ProcedureExecutor::new(ProcedureDialect::PostgreSQL);
    let result = create_mock_result(vec![Value::Int32(42)]);
    let params = vec![ProcedureParameter::input("input", Value::Int32(1))];

    let output = executor.parse_output_params(&result, &params);
    assert!(output.is_empty());
}

#[test]
fn test_parse_output_params_with_values() {
    let executor = ProcedureExecutor::new(ProcedureDialect::PostgreSQL);
    let result = create_mock_result(vec![
        Value::Int64(100),
        Value::String("success".to_string()),
    ]);
    let params = vec![
        ProcedureParameter::input("id", Value::Int32(1)),
        ProcedureParameter::output("count"),
        ProcedureParameter::output("status"),
    ];

    let output = executor.parse_output_params(&result, &params);
    assert_eq!(output.get("count"), Some(&Value::Int64(100)));
    assert_eq!(
        output.get("status"),
        Some(&Value::String("success".to_string()))
    );
}

// ============================================================================
// Helper Functions
// ============================================================================

fn create_mock_result(values: Vec<Value>) -> QueryResult {
    let columns: Vec<ColumnMeta> = values
        .iter()
        .enumerate()
        .map(|(i, _)| ColumnMeta {
            name: format!("col{}", i),
            data_type: "TEXT".to_string(),
            nullable: true,
            ordinal: i,
            max_length: None,
            precision: None,
            scale: None,
            auto_increment: false,
            default_value: None,
            comment: None,
            enum_values: None,
        })
        .collect();

    let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let row = Row::new(column_names, values);

    QueryResult {
        id: Uuid::new_v4(),
        columns,
        rows: vec![row],
        total_rows: Some(1),
        is_estimated_total: false,
        affected_rows: 0,
        execution_time_ms: 0,
        warnings: Vec::new(),
    }
}
