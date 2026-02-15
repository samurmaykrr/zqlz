//! DBT-style template functions for ZQLZ
//!
//! This module provides DBT (Data Build Tool) compatible Jinja functions
//! for SQL templating, including:
//! - `ref()` - Reference other models/tables
//! - `source()` - Reference source tables
//! - `var()` - Access template variables
//! - `config()` - Model configuration
//! - `env_var()` - Access environment variables
//!
//! Based on dbt-fusion's implementation (Apache 2.0 licensed components)

use minijinja::{Environment, Error, ErrorKind, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Configuration for a DBT model
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelConfig {
    /// Materialization type: table, view, incremental, ephemeral
    pub materialized: Option<String>,
    /// Target schema
    pub schema: Option<String>,
    /// Target database
    pub database: Option<String>,
    /// Alias for the model
    pub alias: Option<String>,
    /// Tags for the model
    pub tags: Vec<String>,
    /// Pre-hook SQL statements
    pub pre_hook: Vec<String>,
    /// Post-hook SQL statements
    pub post_hook: Vec<String>,
    /// Additional custom config
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// A reference to another model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRef {
    /// The model name
    pub name: String,
    /// Optional package name
    pub package: Option<String>,
    /// Optional version
    pub version: Option<String>,
    /// Resolved qualified name (database.schema.table)
    pub qualified_name: String,
}

/// A reference to a source table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRef {
    /// The source name (grouping)
    pub source_name: String,
    /// The table name within the source
    pub table_name: String,
    /// Resolved qualified name (database.schema.table)
    pub qualified_name: String,
}

/// Quoting configuration for identifiers
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotingConfig {
    /// Quote database names
    pub database: bool,
    /// Quote schema names
    pub schema: bool,
    /// Quote identifier/table names
    pub identifier: bool,
}

impl QuotingConfig {
    /// Create a new quoting config with all quoting enabled
    pub fn all_quoted() -> Self {
        Self {
            database: true,
            schema: true,
            identifier: true,
        }
    }

    /// Quote an identifier based on configuration
    pub fn quote_identifier(&self, value: &str) -> String {
        if self.identifier {
            format!("\"{}\"", value.replace('"', "\"\""))
        } else {
            value.to_string()
        }
    }

    /// Quote a schema based on configuration
    pub fn quote_schema(&self, value: &str) -> String {
        if self.schema {
            format!("\"{}\"", value.replace('"', "\"\""))
        } else {
            value.to_string()
        }
    }

    /// Quote a database based on configuration
    pub fn quote_database(&self, value: &str) -> String {
        if self.database {
            format!("\"{}\"", value.replace('"', "\"\""))
        } else {
            value.to_string()
        }
    }
}

/// DBT context for template rendering
///
/// This struct holds all the context needed to resolve DBT functions
/// during template rendering.
#[derive(Debug, Clone, Default)]
pub struct DbtContext {
    /// Model references: model_name -> ModelRef
    pub refs: HashMap<String, ModelRef>,

    /// Source references: (source_name, table_name) -> SourceRef
    pub sources: HashMap<(String, String), SourceRef>,

    /// Variables with values
    pub vars: HashMap<String, serde_json::Value>,

    /// Current model configuration (captured during rendering)
    pub config: Arc<Mutex<ModelConfig>>,

    /// Default schema for models without explicit schema
    pub default_schema: String,

    /// Default database for models without explicit database
    pub default_database: Option<String>,

    /// Quoting configuration
    pub quoting: QuotingConfig,

    /// The current package name (for resolving refs)
    pub current_package: Option<String>,

    /// Whether we're in "execute" mode (false during parsing, true during execution)
    pub execute: bool,
}

impl DbtContext {
    /// Create a new empty DBT context
    pub fn new() -> Self {
        Self {
            refs: HashMap::new(),
            sources: HashMap::new(),
            vars: HashMap::new(),
            config: Arc::new(Mutex::new(ModelConfig::default())),
            default_schema: "public".to_string(),
            default_database: None,
            quoting: QuotingConfig::all_quoted(),
            current_package: None,
            execute: true,
        }
    }

    /// Create a DBT context with a default schema
    pub fn with_schema(mut self, schema: &str) -> Self {
        self.default_schema = schema.to_string();
        self
    }

    /// Set the default database
    pub fn with_database(mut self, database: &str) -> Self {
        self.default_database = Some(database.to_string());
        self
    }

    /// Set quoting configuration
    pub fn with_quoting(mut self, quoting: QuotingConfig) -> Self {
        self.quoting = quoting;
        self
    }

    /// Add a model reference
    pub fn add_ref(&mut self, name: &str, schema: Option<&str>, database: Option<&str>) {
        let schema = schema.unwrap_or(&self.default_schema);
        let database = database.or(self.default_database.as_deref());

        let qualified_name = self.build_qualified_name(database, schema, name);

        self.refs.insert(
            name.to_string(),
            ModelRef {
                name: name.to_string(),
                package: None,
                version: None,
                qualified_name,
            },
        );
    }

    /// Add a model reference with package
    pub fn add_ref_with_package(
        &mut self,
        name: &str,
        package: &str,
        schema: Option<&str>,
        database: Option<&str>,
    ) {
        let schema = schema.unwrap_or(&self.default_schema);
        let database = database.or(self.default_database.as_deref());

        let qualified_name = self.build_qualified_name(database, schema, name);
        let key = format!("{}.{}", package, name);

        self.refs.insert(
            key,
            ModelRef {
                name: name.to_string(),
                package: Some(package.to_string()),
                version: None,
                qualified_name,
            },
        );
    }

    /// Add a source reference
    pub fn add_source(
        &mut self,
        source_name: &str,
        table_name: &str,
        schema: Option<&str>,
        database: Option<&str>,
    ) {
        let schema = schema.unwrap_or(source_name); // Default schema to source name
        let database = database.or(self.default_database.as_deref());

        let qualified_name = self.build_qualified_name(database, schema, table_name);

        self.sources.insert(
            (source_name.to_string(), table_name.to_string()),
            SourceRef {
                source_name: source_name.to_string(),
                table_name: table_name.to_string(),
                qualified_name,
            },
        );
    }

    /// Add a model reference with a pre-computed qualified name
    ///
    /// This is useful when the qualified name is already known (e.g., from ProjectContext)
    pub fn add_ref_qualified(&mut self, name: &str, qualified_name: String) {
        self.refs.insert(
            name.to_string(),
            ModelRef {
                name: name.to_string(),
                package: None,
                version: None,
                qualified_name,
            },
        );
    }

    /// Add a source reference with a pre-computed qualified name
    ///
    /// This is useful when the qualified name is already known (e.g., from ProjectContext)
    pub fn add_source_qualified(
        &mut self,
        source_name: &str,
        table_name: &str,
        qualified_name: String,
    ) {
        self.sources.insert(
            (source_name.to_string(), table_name.to_string()),
            SourceRef {
                source_name: source_name.to_string(),
                table_name: table_name.to_string(),
                qualified_name,
            },
        );
    }

    /// Add a variable
    pub fn add_var(&mut self, name: &str, value: serde_json::Value) {
        self.vars.insert(name.to_string(), value);
    }

    /// Build a qualified table name with proper quoting
    fn build_qualified_name(
        &self,
        database: Option<&str>,
        schema: &str,
        identifier: &str,
    ) -> String {
        let quoted_identifier = self.quoting.quote_identifier(identifier);
        let quoted_schema = self.quoting.quote_schema(schema);

        match database {
            Some(db) => {
                let quoted_db = self.quoting.quote_database(db);
                format!("{}.{}.{}", quoted_db, quoted_schema, quoted_identifier)
            }
            None => format!("{}.{}", quoted_schema, quoted_identifier),
        }
    }

    /// Get the captured model config
    pub fn get_config(&self) -> ModelConfig {
        self.config.lock().unwrap().clone()
    }
}

/// Register DBT functions with a MiniJinja environment
pub fn register_dbt_functions(env: &mut Environment, ctx: DbtContext) {
    let ctx = Arc::new(ctx);

    // Register ref() function
    let ctx_ref = Arc::clone(&ctx);
    env.add_function("ref", move |args: &[Value]| -> Result<Value, Error> {
        dbt_ref(&ctx_ref, args)
    });

    // Register source() function
    let ctx_source = Arc::clone(&ctx);
    env.add_function("source", move |args: &[Value]| -> Result<Value, Error> {
        dbt_source(&ctx_source, args)
    });

    // Register var() function
    let ctx_var = Arc::clone(&ctx);
    env.add_function("var", move |args: &[Value]| -> Result<Value, Error> {
        dbt_var(&ctx_var, args)
    });

    // Register config() function
    let ctx_config = Arc::clone(&ctx);
    env.add_function("config", move |args: &[Value]| -> Result<Value, Error> {
        dbt_config(&ctx_config, args)
    });

    // Register env_var() function
    env.add_function("env_var", dbt_env_var);

    // Register utility functions
    env.add_function("fromjson", fromjson);
    env.add_function("tojson", tojson);
    env.add_function("set", set_fn);
    env.add_function("zip", zip_fn);
    env.add_function("log", log_fn);
    env.add_function("print", print_fn);

    // Add execute global
    let execute = ctx.execute;
    env.add_global("execute", Value::from(execute));
}

/// DBT ref() function - reference another model
///
/// Usage:
/// - `{{ ref('model_name') }}` - Reference a model by name
/// - `{{ ref('package', 'model_name') }}` - Reference a model from a specific package
/// - `{{ ref('model_name', version=1) }}` - Reference a specific version (not fully implemented)
fn dbt_ref(ctx: &DbtContext, args: &[Value]) -> Result<Value, Error> {
    match args.len() {
        1 => {
            // ref('model_name')
            let model_name = args[0].as_str().ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidOperation,
                    "ref() requires a string argument",
                )
            })?;

            if let Some(model_ref) = ctx.refs.get(model_name) {
                Ok(Value::from(model_ref.qualified_name.clone()))
            } else {
                // Model not found - return a placeholder or error based on execute mode
                if ctx.execute {
                    Err(Error::new(
                        ErrorKind::InvalidOperation,
                        format!("Model '{}' not found in refs", model_name),
                    ))
                } else {
                    // During parsing, return empty string
                    Ok(Value::from(""))
                }
            }
        }
        2 => {
            // ref('package', 'model_name') or ref('model_name', version=X)
            let first = args[0].as_str().ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidOperation,
                    "ref() requires string arguments",
                )
            })?;
            let second = args[1].as_str().ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidOperation,
                    "ref() requires string arguments",
                )
            })?;

            // Try package.model_name first
            let key = format!("{}.{}", first, second);
            if let Some(model_ref) = ctx.refs.get(&key) {
                return Ok(Value::from(model_ref.qualified_name.clone()));
            }

            // Fall back to just model name
            if let Some(model_ref) = ctx.refs.get(second) {
                return Ok(Value::from(model_ref.qualified_name.clone()));
            }

            if ctx.execute {
                Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Model '{}.{}' not found in refs", first, second),
                ))
            } else {
                Ok(Value::from(""))
            }
        }
        _ => Err(Error::new(
            ErrorKind::InvalidOperation,
            "ref() requires 1 or 2 arguments",
        )),
    }
}

/// DBT source() function - reference a source table
///
/// Usage:
/// - `{{ source('source_name', 'table_name') }}` - Reference a source table
fn dbt_source(ctx: &DbtContext, args: &[Value]) -> Result<Value, Error> {
    if args.len() != 2 {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "source() requires exactly 2 arguments: source_name and table_name",
        ));
    }

    let source_name = args[0].as_str().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidOperation,
            "source() requires string arguments",
        )
    })?;

    let table_name = args[1].as_str().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidOperation,
            "source() requires string arguments",
        )
    })?;

    let key = (source_name.to_string(), table_name.to_string());

    if let Some(source_ref) = ctx.sources.get(&key) {
        Ok(Value::from(source_ref.qualified_name.clone()))
    } else {
        if ctx.execute {
            Err(Error::new(
                ErrorKind::InvalidOperation,
                format!(
                    "Source '{}.{}' not found in sources",
                    source_name, table_name
                ),
            ))
        } else {
            Ok(Value::from(""))
        }
    }
}

/// DBT var() function - access a variable
///
/// Usage:
/// - `{{ var('var_name') }}` - Get variable value (error if not found)
/// - `{{ var('var_name', 'default') }}` - Get variable with default value
fn dbt_var(ctx: &DbtContext, args: &[Value]) -> Result<Value, Error> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "var() requires 1 or 2 arguments",
        ));
    }

    let var_name = args[0].as_str().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidOperation,
            "var() requires a string variable name",
        )
    })?;

    if let Some(value) = ctx.vars.get(var_name) {
        Ok(Value::from_serialize(value))
    } else if args.len() == 2 {
        // Return default value
        Ok(args[1].clone())
    } else {
        Err(Error::new(
            ErrorKind::InvalidOperation,
            format!("Variable '{}' not found and no default provided", var_name),
        ))
    }
}

/// DBT config() function - set model configuration
///
/// Usage:
/// - `{{ config(materialized='view') }}`
/// - `{{ config(materialized='table', schema='analytics') }}`
fn dbt_config(ctx: &DbtContext, args: &[Value]) -> Result<Value, Error> {
    // config() is typically called with keyword arguments
    // In MiniJinja, these come as a single object argument
    if args.is_empty() {
        return Ok(Value::from(""));
    }

    let config_value = &args[0];

    // Try to parse as an object/map
    if let Ok(iter) = config_value.try_iter() {
        let mut config = ctx.config.lock().unwrap();

        for key in iter {
            if let Some(key_str) = key.as_str() {
                if let Ok(value) = config_value.get_item(&key) {
                    match key_str {
                        "materialized" => {
                            config.materialized = value.as_str().map(|s| s.to_string());
                        }
                        "schema" => {
                            config.schema = value.as_str().map(|s| s.to_string());
                        }
                        "database" => {
                            config.database = value.as_str().map(|s| s.to_string());
                        }
                        "alias" => {
                            config.alias = value.as_str().map(|s| s.to_string());
                        }
                        "tags" => {
                            if let Ok(tags_iter) = value.try_iter() {
                                config.tags = tags_iter
                                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                    .collect();
                            }
                        }
                        "pre_hook" | "pre-hook" => {
                            if let Some(hook) = value.as_str() {
                                config.pre_hook.push(hook.to_string());
                            } else if let Ok(hooks_iter) = value.try_iter() {
                                config.pre_hook.extend(
                                    hooks_iter.filter_map(|v| v.as_str().map(|s| s.to_string())),
                                );
                            }
                        }
                        "post_hook" | "post-hook" => {
                            if let Some(hook) = value.as_str() {
                                config.post_hook.push(hook.to_string());
                            } else if let Ok(hooks_iter) = value.try_iter() {
                                config.post_hook.extend(
                                    hooks_iter.filter_map(|v| v.as_str().map(|s| s.to_string())),
                                );
                            }
                        }
                        _ => {
                            // Store in extra config
                            if let Ok(json_value) = serde_json::to_value(&value) {
                                config.extra.insert(key_str.to_string(), json_value);
                            }
                        }
                    }
                }
            }
        }
    }

    // config() returns empty string (it's a side-effect function)
    Ok(Value::from(""))
}

/// DBT env_var() function - access environment variables
///
/// Usage:
/// - `{{ env_var('VAR_NAME') }}` - Get env var (error if not found)
/// - `{{ env_var('VAR_NAME', 'default') }}` - Get env var with default
fn dbt_env_var(args: &[Value]) -> Result<Value, Error> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "env_var() requires 1 or 2 arguments",
        ));
    }

    let var_name = args[0].as_str().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidOperation,
            "env_var() requires a string variable name",
        )
    })?;

    // Check for secret prefix (DBT convention)
    if var_name.starts_with("DBT_ENV_SECRET_") {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "Secret environment variables cannot be accessed in templates",
        ));
    }

    match std::env::var(var_name) {
        Ok(value) => Ok(Value::from(value)),
        Err(_) => {
            if args.len() == 2 {
                Ok(args[1].clone())
            } else {
                Err(Error::new(
                    ErrorKind::InvalidOperation,
                    format!("Environment variable '{}' not found", var_name),
                ))
            }
        }
    }
}

/// Parse JSON string to value
fn fromjson(args: &[Value]) -> Result<Value, Error> {
    if args.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "fromjson() requires at least 1 argument",
        ));
    }

    let json_str = args[0].as_str().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidOperation,
            "fromjson() requires a string argument",
        )
    })?;

    let default = args.get(1);

    match serde_json::from_str::<serde_json::Value>(json_str) {
        Ok(value) => Ok(Value::from_serialize(value)),
        Err(_) => {
            if let Some(default_value) = default {
                Ok(default_value.clone())
            } else {
                Err(Error::new(
                    ErrorKind::InvalidOperation,
                    "Failed to parse JSON",
                ))
            }
        }
    }
}

/// Convert value to JSON string
fn tojson(args: &[Value]) -> Result<Value, Error> {
    if args.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "tojson() requires at least 1 argument",
        ));
    }

    let value = &args[0];

    match serde_json::to_string(&value) {
        Ok(json_str) => Ok(Value::from(json_str)),
        Err(_) => {
            if let Some(default_value) = args.get(1) {
                Ok(default_value.clone())
            } else {
                Ok(Value::from("{}"))
            }
        }
    }
}

/// Convert iterable to unique set
fn set_fn(args: &[Value]) -> Result<Value, Error> {
    if args.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "set() requires at least 1 argument",
        ));
    }

    let value = &args[0];

    match value.try_iter() {
        Ok(iter) => {
            let set: std::collections::BTreeSet<String> = iter.map(|v| v.to_string()).collect();
            Ok(Value::from_iter(set))
        }
        Err(_) => {
            if let Some(default) = args.get(1) {
                Ok(default.clone())
            } else {
                Ok(Value::from(()))
            }
        }
    }
}

/// Zip multiple iterables
fn zip_fn(args: &[Value]) -> Result<Value, Error> {
    if args.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidOperation,
            "zip() requires at least 1 argument",
        ));
    }

    let mut iterators: Vec<Vec<Value>> = Vec::new();

    for arg in args {
        match arg.try_iter() {
            Ok(iter) => iterators.push(iter.collect()),
            Err(_) => return Ok(Value::from(())),
        }
    }

    let min_len = iterators.iter().map(|v| v.len()).min().unwrap_or(0);

    let mut zipped = Vec::new();
    for i in 0..min_len {
        let tuple: Vec<Value> = iterators.iter().map(|iter| iter[i].clone()).collect();
        zipped.push(Value::from(tuple));
    }

    Ok(Value::from_iter(zipped))
}

/// Log a message (no-op in template context, but captures for debugging)
fn log_fn(args: &[Value]) -> Result<Value, Error> {
    if !args.is_empty() {
        let msg = args[0].to_string();
        // In a real implementation, this would log to a proper logging system
        eprintln!("[dbt log] {}", msg);
    }
    Ok(Value::from(""))
}

/// Print a message
fn print_fn(args: &[Value]) -> Result<Value, Error> {
    if !args.is_empty() {
        let msg = args[0].to_string();
        println!("{}", msg);
    }
    Ok(Value::from(""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dbt_context_ref() {
        let mut ctx = DbtContext::new().with_schema("analytics");
        ctx.add_ref("users", None, None);
        ctx.add_ref("orders", Some("staging"), None);

        assert!(ctx.refs.contains_key("users"));
        assert!(ctx.refs.contains_key("orders"));

        let users_ref = ctx.refs.get("users").unwrap();
        assert_eq!(users_ref.qualified_name, "\"analytics\".\"users\"");

        let orders_ref = ctx.refs.get("orders").unwrap();
        assert_eq!(orders_ref.qualified_name, "\"staging\".\"orders\"");
    }

    #[test]
    fn test_dbt_context_source() {
        let mut ctx = DbtContext::new();
        ctx.add_source("raw", "events", Some("raw_data"), None);

        let key = ("raw".to_string(), "events".to_string());
        assert!(ctx.sources.contains_key(&key));

        let source_ref = ctx.sources.get(&key).unwrap();
        assert_eq!(source_ref.qualified_name, "\"raw_data\".\"events\"");
    }

    #[test]
    fn test_dbt_ref_function() {
        let mut ctx = DbtContext::new().with_schema("public");
        ctx.add_ref("my_model", None, None);

        let result = dbt_ref(&ctx, &[Value::from("my_model")]).unwrap();
        assert_eq!(result.to_string(), "\"public\".\"my_model\"");
    }

    #[test]
    fn test_dbt_source_function() {
        let mut ctx = DbtContext::new();
        ctx.add_source("raw", "users", Some("raw_schema"), None);

        let result = dbt_source(&ctx, &[Value::from("raw"), Value::from("users")]).unwrap();
        assert_eq!(result.to_string(), "\"raw_schema\".\"users\"");
    }

    #[test]
    fn test_dbt_var_function() {
        let mut ctx = DbtContext::new();
        ctx.add_var("start_date", serde_json::json!("2024-01-01"));

        let result = dbt_var(&ctx, &[Value::from("start_date")]).unwrap();
        assert_eq!(result.to_string(), "2024-01-01");
    }

    #[test]
    fn test_dbt_var_with_default() {
        let ctx = DbtContext::new();

        let result = dbt_var(
            &ctx,
            &[Value::from("missing_var"), Value::from("default_value")],
        )
        .unwrap();
        assert_eq!(result.to_string(), "default_value");
    }

    #[test]
    fn test_dbt_env_var() {
        // SAFETY: We're in a test and controlling the env var access
        unsafe {
            std::env::set_var("TEST_DBT_VAR", "test_value");
        }

        let result = dbt_env_var(&[Value::from("TEST_DBT_VAR")]).unwrap();
        assert_eq!(result.to_string(), "test_value");

        // SAFETY: Clean up test env var
        unsafe {
            std::env::remove_var("TEST_DBT_VAR");
        }
    }

    #[test]
    fn test_dbt_env_var_default() {
        let result = dbt_env_var(&[
            Value::from("NONEXISTENT_VAR_12345"),
            Value::from("fallback"),
        ])
        .unwrap();
        assert_eq!(result.to_string(), "fallback");
    }

    #[test]
    fn test_fromjson() {
        let result = fromjson(&[Value::from(r#"{"key": "value"}"#)]).unwrap();
        let key_value = result.get_item(&Value::from("key")).unwrap();
        assert_eq!(key_value.to_string(), "value");
    }

    #[test]
    fn test_tojson() {
        let result = tojson(&[Value::from("test")]).unwrap();
        assert_eq!(result.to_string(), "\"test\"");
    }
}
