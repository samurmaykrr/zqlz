//! Stored procedure executor
//!
//! Provides functionality for executing stored procedures and functions
//! across different database systems with proper parameter handling.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use zqlz_core::{QueryResult, Value};

/// Parameter direction mode for stored procedures
///
/// # Examples
///
/// ```
/// use zqlz_objects::ParameterMode;
///
/// let input = ParameterMode::In;
/// assert!(input.is_input());
/// assert!(!input.is_output());
///
/// let output = ParameterMode::Out;
/// assert!(!output.is_input());
/// assert!(output.is_output());
///
/// let inout = ParameterMode::InOut;
/// assert!(inout.is_input());
/// assert!(inout.is_output());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterMode {
    /// Input parameter - value is passed to the procedure
    In,
    /// Output parameter - value is returned from the procedure
    Out,
    /// Input/Output parameter - value is passed in and can be modified
    InOut,
}

impl ParameterMode {
    /// Check if this parameter accepts input values
    pub fn is_input(&self) -> bool {
        matches!(self, ParameterMode::In | ParameterMode::InOut)
    }

    /// Check if this parameter produces output values
    pub fn is_output(&self) -> bool {
        matches!(self, ParameterMode::Out | ParameterMode::InOut)
    }
}

impl Default for ParameterMode {
    fn default() -> Self {
        ParameterMode::In
    }
}

/// A parameter for a stored procedure call
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcedureParameter {
    /// Parameter name (without @ or : prefix)
    pub name: String,
    /// Parameter value (None for OUT parameters before execution)
    pub value: Option<Value>,
    /// Parameter direction
    pub mode: ParameterMode,
    /// Data type hint (database-specific, e.g., "INTEGER", "VARCHAR(255)")
    pub data_type: Option<String>,
}

impl ProcedureParameter {
    /// Create a new IN parameter with a value
    pub fn input(name: impl Into<String>, value: Value) -> Self {
        Self {
            name: name.into(),
            value: Some(value),
            mode: ParameterMode::In,
            data_type: None,
        }
    }

    /// Create a new OUT parameter (no initial value)
    pub fn output(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: None,
            mode: ParameterMode::Out,
            data_type: None,
        }
    }

    /// Create a new INOUT parameter with an initial value
    pub fn inout(name: impl Into<String>, value: Value) -> Self {
        Self {
            name: name.into(),
            value: Some(value),
            mode: ParameterMode::InOut,
            data_type: None,
        }
    }

    /// Set the data type hint for this parameter
    pub fn with_type(mut self, data_type: impl Into<String>) -> Self {
        self.data_type = Some(data_type.into());
        self
    }
}

/// Result from executing a stored procedure
#[derive(Debug, Clone)]
pub struct ProcedureResult {
    /// Output parameter values keyed by parameter name
    pub output_params: HashMap<String, Value>,
    /// Result sets returned by the procedure (if any)
    pub result_sets: Vec<QueryResult>,
    /// Return value (for functions or procedures with RETURN)
    pub return_value: Option<Value>,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

impl ProcedureResult {
    /// Create an empty procedure result
    pub fn empty() -> Self {
        Self {
            output_params: HashMap::new(),
            result_sets: Vec::new(),
            return_value: None,
            execution_time_ms: 0,
        }
    }

    /// Get an output parameter value by name
    pub fn get_output(&self, name: &str) -> Option<&Value> {
        self.output_params.get(name)
    }

    /// Check if the procedure returned any result sets
    pub fn has_result_sets(&self) -> bool {
        !self.result_sets.is_empty()
    }

    /// Get the first result set (if any)
    pub fn first_result_set(&self) -> Option<&QueryResult> {
        self.result_sets.first()
    }
}

/// Database dialect for procedure execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcedureDialect {
    /// PostgreSQL (CALL procedure, SELECT function)
    PostgreSQL,
    /// MySQL/MariaDB (CALL procedure)
    MySQL,
    /// SQLite (no native stored procedures)
    SQLite,
    /// Microsoft SQL Server (EXEC procedure)
    MsSql,
}

impl ProcedureDialect {
    /// Get the procedure call keyword for this dialect
    pub fn call_keyword(&self) -> &'static str {
        match self {
            ProcedureDialect::PostgreSQL | ProcedureDialect::MySQL => "CALL",
            ProcedureDialect::MsSql => "EXEC",
            ProcedureDialect::SQLite => "SELECT", // SQLite uses user-defined functions
        }
    }

    /// Check if this dialect supports stored procedures
    pub fn supports_procedures(&self) -> bool {
        !matches!(self, ProcedureDialect::SQLite)
    }

    /// Check if this dialect supports OUT parameters
    pub fn supports_out_params(&self) -> bool {
        matches!(
            self,
            ProcedureDialect::PostgreSQL | ProcedureDialect::MySQL | ProcedureDialect::MsSql
        )
    }
}

/// Builder for constructing stored procedure CALL statements
pub struct ProcedureExecutor {
    dialect: ProcedureDialect,
}

impl ProcedureExecutor {
    /// Create a new procedure executor for the specified dialect
    pub fn new(dialect: ProcedureDialect) -> Self {
        Self { dialect }
    }

    /// Build a CALL/EXEC statement for a stored procedure
    ///
    /// # Arguments
    /// * `procedure_name` - Fully qualified procedure name (e.g., "schema.procedure_name")
    /// * `params` - Parameters to pass to the procedure
    ///
    /// # Returns
    /// A tuple of (SQL statement, ordered parameter values for binding)
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_objects::{ProcedureExecutor, ProcedureDialect, ProcedureParameter};
    /// use zqlz_core::Value;
    ///
    /// let executor = ProcedureExecutor::new(ProcedureDialect::PostgreSQL);
    /// let params = vec![
    ///     ProcedureParameter::input("user_id", Value::Int32(42)),
    ///     ProcedureParameter::input("name", Value::String("Alice".to_string())),
    /// ];
    /// let (sql, values) = executor.build_call_statement("create_user", &params);
    /// assert_eq!(sql, "CALL create_user($1, $2)");
    /// assert_eq!(values.len(), 2);
    /// ```
    pub fn build_call_statement(
        &self,
        procedure_name: &str,
        params: &[ProcedureParameter],
    ) -> (String, Vec<Value>) {
        let mut values = Vec::new();
        let mut placeholders = Vec::new();

        for (idx, param) in params.iter().enumerate() {
            if param.mode.is_input() {
                if let Some(ref value) = param.value {
                    values.push(value.clone());
                    placeholders.push(self.format_placeholder(idx + 1, param));
                } else {
                    placeholders.push("NULL".to_string());
                }
            } else {
                placeholders.push(self.format_out_placeholder(param));
            }
        }

        let params_str = placeholders.join(", ");
        let sql = format!(
            "{} {}({})",
            self.dialect.call_keyword(),
            procedure_name,
            params_str
        );

        (sql, values)
    }

    /// Build a SELECT statement to call a function (for databases that use this pattern)
    ///
    /// # Examples
    ///
    /// ```
    /// use zqlz_objects::{ProcedureExecutor, ProcedureDialect, ProcedureParameter};
    /// use zqlz_core::Value;
    ///
    /// let executor = ProcedureExecutor::new(ProcedureDialect::PostgreSQL);
    /// let params = vec![
    ///     ProcedureParameter::input("a", Value::Int32(10)),
    ///     ProcedureParameter::input("b", Value::Int32(20)),
    /// ];
    /// let (sql, values) = executor.build_function_call("add_numbers", &params);
    /// assert_eq!(sql, "SELECT add_numbers($1, $2)");
    /// ```
    pub fn build_function_call(
        &self,
        function_name: &str,
        params: &[ProcedureParameter],
    ) -> (String, Vec<Value>) {
        let mut values = Vec::new();
        let mut placeholders = Vec::new();

        for (idx, param) in params.iter().enumerate() {
            if param.mode.is_input() {
                if let Some(ref value) = param.value {
                    values.push(value.clone());
                    placeholders.push(self.format_placeholder(idx + 1, param));
                } else {
                    placeholders.push("NULL".to_string());
                }
            }
        }

        let params_str = placeholders.join(", ");
        let sql = format!("SELECT {}({})", function_name, params_str);

        (sql, values)
    }

    /// Format a parameter placeholder based on dialect
    fn format_placeholder(&self, position: usize, _param: &ProcedureParameter) -> String {
        match self.dialect {
            ProcedureDialect::PostgreSQL => format!("${}", position),
            ProcedureDialect::MySQL | ProcedureDialect::SQLite => "?".to_string(),
            ProcedureDialect::MsSql => format!("@p{}", position),
        }
    }

    /// Format an OUT parameter placeholder based on dialect
    fn format_out_placeholder(&self, param: &ProcedureParameter) -> String {
        match self.dialect {
            ProcedureDialect::PostgreSQL => "NULL".to_string(), // PostgreSQL uses INOUT with NULL
            ProcedureDialect::MySQL => format!("@{}", param.name), // MySQL uses session variables
            ProcedureDialect::MsSql => format!("@{} OUTPUT", param.name),
            ProcedureDialect::SQLite => "NULL".to_string(), // SQLite doesn't support OUT params
        }
    }

    /// Parse output parameters from a procedure result
    ///
    /// This extracts output parameter values from query results based on the
    /// expected output parameters.
    pub fn parse_output_params(
        &self,
        result: &QueryResult,
        expected_params: &[ProcedureParameter],
    ) -> HashMap<String, Value> {
        let mut output = HashMap::new();

        let output_params: Vec<_> = expected_params
            .iter()
            .filter(|p| p.mode.is_output())
            .collect();

        if output_params.is_empty() || result.rows.is_empty() {
            return output;
        }

        let row = &result.rows[0];
        for (idx, param) in output_params.iter().enumerate() {
            if let Some(value) = row.get(idx) {
                output.insert(param.name.clone(), value.clone());
            }
        }

        output
    }

    /// Get the dialect for this executor
    pub fn dialect(&self) -> ProcedureDialect {
        self.dialect
    }
}
