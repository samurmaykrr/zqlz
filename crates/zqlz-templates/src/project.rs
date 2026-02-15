//! DBT-style Project management for ZQLZ
//!
//! This module provides a project-centric approach to template management,
//! similar to how DBT organizes models within projects. A Project contains:
//! - Models (SQL templates that can reference each other via ref())
//! - Sources (external tables that models can reference via source())
//! - Variables (project-wide and model-specific)
//! - Connection context (for auto-populating refs from database schema)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::dbt::{DbtContext, ModelConfig, ModelRef, QuotingConfig};

/// A ZQLZ Template Project - analogous to a DBT project
///
/// Projects group related models together and provide shared context
/// for template rendering (sources, variables, refs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    /// Unique identifier
    pub id: Uuid,

    /// Project name (e.g., "analytics", "marketing_models")
    pub name: String,

    /// Description of the project
    pub description: String,

    /// Connection ID this project is associated with (optional)
    /// When set, refs can be auto-populated from the database schema
    pub connection_id: Option<Uuid>,

    /// Default schema for models in this project
    pub default_schema: String,

    /// Default database (for multi-database systems)
    pub default_database: Option<String>,

    /// Quoting configuration for this project
    pub quoting: QuotingConfig,

    /// Project-level variables (available to all models via var())
    pub vars: HashMap<String, serde_json::Value>,

    /// Source definitions (external tables models can reference)
    pub sources: Vec<SourceDefinition>,

    /// When the project was created
    pub created_at: DateTime<Utc>,

    /// When the project was last modified
    pub updated_at: DateTime<Utc>,
}

impl Project {
    /// Create a new project
    pub fn new(name: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            description: String::new(),
            connection_id: None,
            default_schema: "public".to_string(),
            default_database: None,
            quoting: QuotingConfig::all_quoted(),
            vars: HashMap::new(),
            sources: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Set the connection for this project
    pub fn with_connection(mut self, connection_id: Uuid) -> Self {
        self.connection_id = Some(connection_id);
        self
    }

    /// Set the default schema
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.default_schema = schema.into();
        self
    }

    /// Add a project-level variable
    pub fn with_var(mut self, name: impl Into<String>, value: serde_json::Value) -> Self {
        self.vars.insert(name.into(), value);
        self
    }

    /// Add a source definition
    pub fn with_source(mut self, source: SourceDefinition) -> Self {
        self.sources.push(source);
        self
    }
}

/// A source definition - represents an external data source
///
/// Sources are existing tables in the database that models can reference.
/// This is analogous to DBT's `sources.yml` configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceDefinition {
    /// Unique identifier
    pub id: Uuid,

    /// Source name (grouping name, e.g., "raw_data", "external_api")
    pub name: String,

    /// Description of this source
    pub description: String,

    /// The actual database name (if different from project default)
    pub database: Option<String>,

    /// The actual schema name
    pub schema: String,

    /// Tables within this source
    pub tables: Vec<SourceTable>,
}

impl SourceDefinition {
    /// Create a new source definition
    pub fn new(name: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.into(),
            description: String::new(),
            database: None,
            schema: schema.into(),
            tables: Vec::new(),
        }
    }

    /// Add a table to this source
    pub fn with_table(mut self, table: SourceTable) -> Self {
        self.tables.push(table);
        self
    }
}

/// A table within a source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceTable {
    /// Table name in the source
    pub name: String,

    /// Description of this table
    pub description: String,

    /// Optional alias (if you want to reference it by a different name)
    pub alias: Option<String>,

    /// Column definitions (for documentation/validation)
    pub columns: Vec<SourceColumn>,
}

impl SourceTable {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            alias: None,
            columns: Vec::new(),
        }
    }
}

/// A column within a source table (for documentation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceColumn {
    pub name: String,
    pub description: String,
    pub data_type: Option<String>,
}

/// A Model within a project - a SQL template that can reference other models
///
/// Models are the core unit of work in the template system. They contain
/// SQL with Jinja templating that can use ref(), source(), var(), etc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    /// Unique identifier
    pub id: Uuid,

    /// Project this model belongs to
    pub project_id: Uuid,

    /// Model name (used in ref() calls from other models)
    pub name: String,

    /// Description of what this model does
    pub description: String,

    /// The SQL template content
    pub sql: String,

    /// Model-specific configuration (materialization, schema override, etc.)
    pub config: ModelConfig,

    /// Model-specific variables (override project vars)
    pub vars: HashMap<String, serde_json::Value>,

    /// Tags for organization
    pub tags: Vec<String>,

    /// Models this model depends on (populated after parsing)
    #[serde(default)]
    pub depends_on: Vec<ModelDependency>,

    /// When the model was created
    pub created_at: DateTime<Utc>,

    /// When the model was last modified
    pub updated_at: DateTime<Utc>,
}

impl Model {
    /// Create a new model
    pub fn new(project_id: Uuid, name: impl Into<String>, sql: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            project_id,
            name: name.into(),
            description: String::new(),
            sql: sql.into(),
            config: ModelConfig::default(),
            vars: HashMap::new(),
            tags: Vec::new(),
            depends_on: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Set the model description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Set the materialization type
    pub fn with_materialization(mut self, materialization: impl Into<String>) -> Self {
        self.config.materialized = Some(materialization.into());
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Add a model-specific variable
    pub fn with_var(mut self, name: impl Into<String>, value: serde_json::Value) -> Self {
        self.vars.insert(name.into(), value);
        self
    }
}

/// A dependency of a model on another model or source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ModelDependency {
    /// Depends on another model via ref()
    Model {
        model_name: String,
        /// The model ID if resolved
        model_id: Option<Uuid>,
    },
    /// Depends on a source via source()
    Source {
        source_name: String,
        table_name: String,
    },
}

/// Project context for template rendering
///
/// This is the runtime context used when rendering models. It combines
/// project configuration, model definitions, and optional database schema
/// information to provide a complete DbtContext.
#[derive(Debug, Clone)]
pub struct ProjectContext {
    /// The project
    pub project: Project,

    /// All models in the project (for ref resolution)
    pub models: HashMap<String, Model>,

    /// Additional refs from database schema (tables/views that aren't models)
    pub schema_refs: HashMap<String, ModelRef>,
}

impl ProjectContext {
    /// Create a new project context
    pub fn new(project: Project) -> Self {
        Self {
            project,
            models: HashMap::new(),
            schema_refs: HashMap::new(),
        }
    }

    /// Add a model to the context
    pub fn with_model(mut self, model: Model) -> Self {
        self.models.insert(model.name.clone(), model);
        self
    }

    /// Add models from an iterator
    pub fn with_models(mut self, models: impl IntoIterator<Item = Model>) -> Self {
        for model in models {
            self.models.insert(model.name.clone(), model);
        }
        self
    }

    /// Add a schema ref (table/view from database that can be referenced)
    pub fn with_schema_ref(
        mut self,
        name: impl Into<String>,
        qualified_name: impl Into<String>,
    ) -> Self {
        let name = name.into();
        self.schema_refs.insert(
            name.clone(),
            ModelRef {
                name: name.clone(),
                package: None,
                version: None,
                qualified_name: qualified_name.into(),
            },
        );
        self
    }

    /// Build a DbtContext for rendering a specific model
    pub fn build_dbt_context(&self, model: &Model) -> DbtContext {
        let mut ctx = DbtContext::new()
            .with_schema(&self.project.default_schema)
            .with_quoting(self.project.quoting.clone());

        if let Some(db) = &self.project.default_database {
            ctx = ctx.with_database(db);
        }

        // Add project-level vars
        for (name, value) in &self.project.vars {
            ctx.add_var(name, value.clone());
        }

        // Add model-specific vars (override project vars)
        for (name, value) in &model.vars {
            ctx.add_var(name, value.clone());
        }

        // Add refs for all models in the project
        for (model_name, m) in &self.models {
            let schema = m
                .config
                .schema
                .as_ref()
                .unwrap_or(&self.project.default_schema);
            let qualified = format!(
                "{}.{}",
                self.project.quoting.quote_schema(schema),
                self.project.quoting.quote_identifier(model_name)
            );
            ctx.add_ref_qualified(model_name, qualified);
        }

        // Add schema refs (existing tables/views)
        for (name, model_ref) in &self.schema_refs {
            ctx.add_ref_qualified(name, model_ref.qualified_name.clone());
        }

        // Add sources
        for source_def in &self.project.sources {
            for table in &source_def.tables {
                let table_name = table.alias.as_ref().unwrap_or(&table.name);
                let qualified = format!(
                    "{}.{}",
                    self.project.quoting.quote_schema(&source_def.schema),
                    self.project.quoting.quote_identifier(&table.name)
                );
                ctx.add_source_qualified(&source_def.name, table_name, qualified);
            }
        }

        ctx
    }

    /// Build a DbtContext for ad-hoc template rendering (not a specific model)
    pub fn build_adhoc_context(&self) -> DbtContext {
        let mut ctx = DbtContext::new()
            .with_schema(&self.project.default_schema)
            .with_quoting(self.project.quoting.clone());

        if let Some(db) = &self.project.default_database {
            ctx = ctx.with_database(db);
        }

        // Add project-level vars
        for (name, value) in &self.project.vars {
            ctx.add_var(name, value.clone());
        }

        // Add refs for all models
        for (model_name, m) in &self.models {
            let schema = m
                .config
                .schema
                .as_ref()
                .unwrap_or(&self.project.default_schema);
            let qualified = format!(
                "{}.{}",
                self.project.quoting.quote_schema(schema),
                self.project.quoting.quote_identifier(model_name)
            );
            ctx.add_ref_qualified(model_name, qualified);
        }

        // Add schema refs
        for (name, model_ref) in &self.schema_refs {
            ctx.add_ref_qualified(name, model_ref.qualified_name.clone());
        }

        // Add sources
        for source_def in &self.project.sources {
            for table in &source_def.tables {
                let table_name = table.alias.as_ref().unwrap_or(&table.name);
                let qualified = format!(
                    "{}.{}",
                    self.project.quoting.quote_schema(&source_def.schema),
                    self.project.quoting.quote_identifier(&table.name)
                );
                ctx.add_source_qualified(&source_def.name, table_name, qualified);
            }
        }

        ctx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DbtTemplateEngine;

    #[test]
    fn test_project_context_ref_resolution() {
        // Create a project
        let project = Project::new("analytics")
            .with_schema("analytics")
            .with_var("start_date", serde_json::json!("2024-01-01"));

        // Create models
        let users_model = Model::new(project.id, "users", "SELECT * FROM raw.users");
        let orders_model = Model::new(
            project.id,
            "orders",
            "SELECT * FROM {{ ref('users') }} u JOIN raw.orders o ON u.id = o.user_id",
        );

        // Build context
        let ctx = ProjectContext::new(project)
            .with_model(users_model.clone())
            .with_model(orders_model.clone());

        // Render orders model
        let dbt_ctx = ctx.build_dbt_context(&orders_model);
        let engine = DbtTemplateEngine::new(dbt_ctx);
        let result = engine.render(&orders_model.sql).unwrap();

        assert!(result.contains("\"analytics\".\"users\""));
    }

    #[test]
    fn test_project_with_sources() {
        let source = SourceDefinition::new("raw_data", "raw")
            .with_table(SourceTable::new("events"))
            .with_table(SourceTable::new("users"));

        let project = Project::new("analytics")
            .with_schema("analytics")
            .with_source(source);

        let model = Model::new(
            project.id,
            "user_events",
            "SELECT * FROM {{ source('raw_data', 'events') }} e JOIN {{ source('raw_data', 'users') }} u ON e.user_id = u.id",
        );

        let ctx = ProjectContext::new(project).with_model(model.clone());
        let dbt_ctx = ctx.build_dbt_context(&model);
        let engine = DbtTemplateEngine::new(dbt_ctx);
        let result = engine.render(&model.sql).unwrap();

        assert!(result.contains("\"raw\".\"events\""));
        assert!(result.contains("\"raw\".\"users\""));
    }
}
