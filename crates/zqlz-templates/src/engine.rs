//! Template engine using MiniJinja

use minijinja::{Environment, Value};
use std::collections::HashMap;

use crate::dbt::{DbtContext, ModelConfig, register_dbt_functions};

/// SQL template engine (basic, without DBT functions)
pub struct TemplateEngine {
    env: Environment<'static>,
}

impl TemplateEngine {
    /// Create a new template engine
    pub fn new() -> Self {
        let mut env = Environment::new();

        // Register custom SQL filters
        crate::filters::register_filters(&mut env);

        Self { env }
    }

    /// Render a SQL template with the given context
    pub fn render(
        &self,
        template: &str,
        context: &HashMap<String, Value>,
    ) -> Result<String, minijinja::Error> {
        let tmpl = self.env.template_from_str(template)?;
        tmpl.render(context)
    }

    /// Render a SQL template with a serde-serializable context
    pub fn render_with<T: serde::Serialize>(
        &self,
        template: &str,
        context: &T,
    ) -> Result<String, minijinja::Error> {
        let tmpl = self.env.template_from_str(template)?;
        tmpl.render(context)
    }

    /// Add a named template to the environment
    pub fn add_template(
        &mut self,
        name: &'static str,
        source: &'static str,
    ) -> Result<(), minijinja::Error> {
        self.env.add_template(name, source)
    }
}

impl Default for TemplateEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// DBT-style template engine with ref(), source(), var(), config() functions
///
/// This engine provides full DBT compatibility for SQL templating.
///
/// ## Example
///
/// ```rust,ignore
/// use zqlz_templates::{DbtTemplateEngine, DbtContext};
///
/// let mut ctx = DbtContext::new().with_schema("analytics");
/// ctx.add_ref("users", None, None);
/// ctx.add_source("raw", "events", Some("raw_data"), None);
///
/// let engine = DbtTemplateEngine::new(ctx);
/// let sql = engine.render("SELECT * FROM {{ ref('users') }}").unwrap();
/// ```
pub struct DbtTemplateEngine {
    env: Environment<'static>,
    context: DbtContext,
}

impl DbtTemplateEngine {
    /// Create a new DBT template engine with the given context
    pub fn new(context: DbtContext) -> Self {
        let mut env = Environment::new();

        // Register custom SQL filters
        crate::filters::register_filters(&mut env);

        // Register DBT functions
        register_dbt_functions(&mut env, context.clone());

        Self { env, context }
    }

    /// Create a new DBT template engine with default context
    pub fn with_defaults() -> Self {
        Self::new(DbtContext::new())
    }

    /// Create a new DBT template engine with a specific schema
    pub fn with_schema(schema: &str) -> Self {
        Self::new(DbtContext::new().with_schema(schema))
    }

    /// Get a mutable reference to the context for adding refs/sources/vars
    pub fn context_mut(&mut self) -> &mut DbtContext {
        &mut self.context
    }

    /// Get the context
    pub fn context(&self) -> &DbtContext {
        &self.context
    }

    /// Render a DBT SQL template
    ///
    /// This renders the template with DBT functions like ref(), source(), var()
    /// already registered and available.
    pub fn render(&self, template: &str) -> Result<String, minijinja::Error> {
        let tmpl = self.env.template_from_str(template)?;
        // Render with empty context since DBT functions use the DbtContext
        tmpl.render(HashMap::<String, Value>::new())
    }

    /// Render a DBT SQL template with additional context variables
    ///
    /// This allows passing additional variables beyond what's in DbtContext.
    pub fn render_with_context(
        &self,
        template: &str,
        context: &HashMap<String, Value>,
    ) -> Result<String, minijinja::Error> {
        let tmpl = self.env.template_from_str(template)?;
        tmpl.render(context)
    }

    /// Render a DBT SQL template with a serde-serializable context
    pub fn render_with<T: serde::Serialize>(
        &self,
        template: &str,
        context: &T,
    ) -> Result<String, minijinja::Error> {
        let tmpl = self.env.template_from_str(template)?;
        tmpl.render(context)
    }

    /// Get the model configuration captured during rendering
    ///
    /// Call this after render() to get any config() calls that were made.
    pub fn get_model_config(&self) -> ModelConfig {
        self.context.get_config()
    }

    /// Check if a template contains DBT syntax
    ///
    /// Returns true if the template contains `{{`, `{%`, or `{#` patterns
    /// that indicate Jinja/DBT templating.
    pub fn is_dbt_template(template: &str) -> bool {
        template.contains("{{") || template.contains("{%") || template.contains("{#")
    }

    /// Check if a template uses specific DBT functions
    pub fn uses_dbt_functions(template: &str) -> bool {
        let dbt_patterns = [
            "ref(",
            "source(",
            "var(",
            "config(",
            "env_var(",
            "ref('",
            "source('",
            "var('",
            "config('",
            "env_var('",
        ];
        dbt_patterns
            .iter()
            .any(|pattern| template.contains(pattern))
    }
}

/// Result of compiling a DBT template
#[derive(Debug, Clone)]
pub struct CompiledTemplate {
    /// The rendered SQL
    pub sql: String,
    /// Model configuration extracted from config() calls
    pub config: ModelConfig,
    /// References used in the template
    pub refs_used: Vec<String>,
    /// Sources used in the template
    pub sources_used: Vec<(String, String)>,
    /// Variables used in the template
    pub vars_used: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_template() {
        let engine = TemplateEngine::new();
        let mut context = HashMap::new();
        context.insert("table".to_string(), Value::from("users"));
        context.insert("limit".to_string(), Value::from(10));

        let result = engine
            .render("SELECT * FROM {{ table }} LIMIT {{ limit }}", &context)
            .unwrap();

        assert_eq!(result, "SELECT * FROM users LIMIT 10");
    }

    #[test]
    fn test_dbt_engine_ref() {
        let mut ctx = DbtContext::new().with_schema("analytics");
        ctx.add_ref("users", None, None);

        let engine = DbtTemplateEngine::new(ctx);
        let result = engine.render("SELECT * FROM {{ ref('users') }}").unwrap();

        assert_eq!(result, "SELECT * FROM \"analytics\".\"users\"");
    }

    #[test]
    fn test_dbt_engine_source() {
        let mut ctx = DbtContext::new();
        ctx.add_source("raw", "events", Some("raw_data"), None);

        let engine = DbtTemplateEngine::new(ctx);
        let result = engine
            .render("SELECT * FROM {{ source('raw', 'events') }}")
            .unwrap();

        assert_eq!(result, "SELECT * FROM \"raw_data\".\"events\"");
    }

    #[test]
    fn test_dbt_engine_var() {
        let mut ctx = DbtContext::new().with_schema("public");
        ctx.add_var("start_date", serde_json::json!("2024-01-01"));
        ctx.add_ref("orders", None, None);

        let engine = DbtTemplateEngine::new(ctx);
        let result = engine
            .render(
                r#"SELECT * FROM {{ ref('orders') }} WHERE created_at >= '{{ var("start_date") }}'"#,
            )
            .unwrap();

        assert_eq!(
            result,
            "SELECT * FROM \"public\".\"orders\" WHERE created_at >= '2024-01-01'"
        );
    }

    #[test]
    fn test_dbt_engine_var_with_default() {
        let ctx = DbtContext::new();
        let engine = DbtTemplateEngine::new(ctx);

        let result = engine
            .render(r#"{{ var('missing', 'default_value') }}"#)
            .unwrap();

        assert_eq!(result, "default_value");
    }

    #[test]
    fn test_is_dbt_template() {
        assert!(DbtTemplateEngine::is_dbt_template(
            "SELECT * FROM {{ ref('users') }}"
        ));
        assert!(DbtTemplateEngine::is_dbt_template(
            "{% if condition %}...{% endif %}"
        ));
        assert!(DbtTemplateEngine::is_dbt_template("{# comment #}"));
        assert!(!DbtTemplateEngine::is_dbt_template("SELECT * FROM users"));
    }

    #[test]
    fn test_uses_dbt_functions() {
        assert!(DbtTemplateEngine::uses_dbt_functions("{{ ref('users') }}"));
        assert!(DbtTemplateEngine::uses_dbt_functions(
            "{{ source('raw', 'events') }}"
        ));
        assert!(DbtTemplateEngine::uses_dbt_functions("{{ var('date') }}"));
        assert!(!DbtTemplateEngine::uses_dbt_functions("{{ some_var }}"));
    }

    #[test]
    fn test_complex_dbt_template() {
        let mut ctx = DbtContext::new().with_schema("analytics");
        ctx.add_ref("users", None, None);
        ctx.add_ref("user_summary", Some("reporting"), None);
        ctx.add_source("raw", "events", Some("raw_data"), None);
        ctx.add_var("start_date", serde_json::json!("2024-01-01"));

        let engine = DbtTemplateEngine::new(ctx);

        let template = r#"
WITH source_data AS (
    SELECT * FROM {{ source('raw', 'events') }}
    WHERE event_date >= '{{ var("start_date") }}'
),

user_stats AS (
    SELECT * FROM {{ ref('user_summary') }}
)

SELECT 
    s.user_id,
    s.event_type,
    u.total_purchases
FROM source_data s
LEFT JOIN user_stats u ON s.user_id = u.user_id
JOIN {{ ref('users') }} users ON s.user_id = users.id
"#;

        let result = engine.render(template).unwrap();

        assert!(result.contains("\"raw_data\".\"events\""));
        assert!(result.contains("\"reporting\".\"user_summary\""));
        assert!(result.contains("\"analytics\".\"users\""));
        assert!(result.contains("2024-01-01"));
    }
}
