//! ZQLZ Templates - MiniJinja-based SQL templating
//!
//! This crate provides DBT-like SQL templating functionality
//! using MiniJinja as the template engine.
//!
//! ## Features
//!
//! - **SQL Filters**: `sqlquote`, `inclause`, `identifier` for safe SQL generation
//! - **DBT Functions**: `ref()`, `source()`, `var()`, `config()`, `env_var()`
//! - **Project System**: Organize models with shared context (refs, sources, vars)
//! - **Template Engine**: Flexible rendering with context variables
//!
//! ## Example
//!
//! ```rust,ignore
//! use zqlz_templates::{DbtTemplateEngine, DbtContext};
//!
//! let mut ctx = DbtContext::new().with_schema("analytics");
//! ctx.add_ref("users", None, None);
//! ctx.add_source("raw", "events", Some("raw_data"), None);
//! ctx.add_var("start_date", serde_json::json!("2024-01-01"));
//!
//! let engine = DbtTemplateEngine::new(ctx);
//! let sql = engine.render(r#"
//!     SELECT * FROM {{ ref('users') }}
//!     JOIN {{ source('raw', 'events') }}
//!     WHERE created_at >= '{{ var("start_date") }}'
//! "#).unwrap();
//! ```
//!
//! ## Project-based Workflow
//!
//! ```rust,ignore
//! use zqlz_templates::{Project, Model, SourceDefinition, SourceTable, ProjectContext, DbtTemplateEngine};
//!
//! // Create a project with sources
//! let source = SourceDefinition::new("raw_data", "raw")
//!     .with_table(SourceTable::new("events"))
//!     .with_table(SourceTable::new("users"));
//!
//! let project = Project::new("analytics")
//!     .with_schema("analytics")
//!     .with_source(source)
//!     .with_var("start_date", serde_json::json!("2024-01-01"));
//!
//! // Create models that reference each other
//! let users_model = Model::new(project.id, "dim_users", "SELECT * FROM {{ source('raw_data', 'users') }}");
//! let orders_model = Model::new(project.id, "fct_orders", "SELECT * FROM {{ ref('dim_users') }}");
//!
//! // Build context and render
//! let ctx = ProjectContext::new(project)
//!     .with_model(users_model)
//!     .with_model(orders_model.clone());
//!
//! let dbt_ctx = ctx.build_dbt_context(&orders_model);
//! let engine = DbtTemplateEngine::new(dbt_ctx);
//! let sql = engine.render(&orders_model.sql).unwrap();
//! ```

pub mod dbt;
mod engine;
mod filters;
pub mod project;

pub use engine::{CompiledTemplate, DbtTemplateEngine, TemplateEngine};
pub use filters::SqlFilters;

// Re-export DBT types for convenience
pub use dbt::{
    DbtContext, ModelConfig, ModelRef, QuotingConfig, SourceRef, register_dbt_functions,
};

// Re-export Project types
pub use project::{
    Model, ModelDependency, Project, ProjectContext, SourceColumn, SourceDefinition, SourceTable,
};
