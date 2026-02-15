//! Schema Dependencies Analyzer
//!
//! Analyzes dependencies between database objects by parsing SQL definitions.
//! Useful for understanding impact of schema changes and safe refactoring.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use zqlz_core::ObjectType;

/// Reference to a database object
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRef {
    /// Schema name (if any)
    pub schema: Option<String>,
    /// Object name
    pub name: String,
    /// Type of object
    pub object_type: ObjectType,
}

impl ObjectRef {
    /// Create a new object reference
    pub fn new(name: impl Into<String>, object_type: ObjectType) -> Self {
        Self {
            schema: None,
            name: name.into(),
            object_type,
        }
    }

    /// Create an object reference with schema
    pub fn with_schema(
        schema: impl Into<String>,
        name: impl Into<String>,
        object_type: ObjectType,
    ) -> Self {
        Self {
            schema: Some(schema.into()),
            name: name.into(),
            object_type,
        }
    }

    /// Get the fully qualified name (schema.name or just name)
    pub fn qualified_name(&self) -> String {
        match &self.schema {
            Some(s) => format!("{}.{}", s, self.name),
            None => self.name.clone(),
        }
    }
}

/// Dependencies of a database object
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Dependencies {
    /// Objects that this object depends on
    pub depends_on: Vec<ObjectRef>,
    /// Objects that depend on this object
    pub depended_by: Vec<ObjectRef>,
}

impl Dependencies {
    /// Create empty dependencies
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an object that this depends on
    pub fn add_dependency(&mut self, obj: ObjectRef) {
        if !self.depends_on.contains(&obj) {
            self.depends_on.push(obj);
        }
    }

    /// Add an object that depends on this
    pub fn add_dependent(&mut self, obj: ObjectRef) {
        if !self.depended_by.contains(&obj) {
            self.depended_by.push(obj);
        }
    }

    /// Check if this object has any dependencies
    pub fn has_dependencies(&self) -> bool {
        !self.depends_on.is_empty()
    }

    /// Check if any objects depend on this
    pub fn has_dependents(&self) -> bool {
        !self.depended_by.is_empty()
    }
}

/// Dependency graph for a set of database objects
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DependencyGraph {
    /// Map from object to its dependencies
    dependencies: HashMap<String, Dependencies>,
}

impl DependencyGraph {
    /// Create an empty dependency graph
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an object and its dependencies to the graph
    pub fn add_object(&mut self, obj: ObjectRef, depends_on: Vec<ObjectRef>) {
        let key = obj.qualified_name();

        let deps = self.dependencies.entry(key.clone()).or_default();
        for dep in &depends_on {
            deps.add_dependency(dep.clone());
        }

        for dep in depends_on {
            let dep_key = dep.qualified_name();
            let dep_deps = self.dependencies.entry(dep_key).or_default();
            dep_deps.add_dependent(obj.clone());
        }
    }

    /// Get dependencies for an object
    pub fn get(&self, qualified_name: &str) -> Option<&Dependencies> {
        self.dependencies.get(qualified_name)
    }

    /// Get all objects in the graph
    pub fn objects(&self) -> impl Iterator<Item = &String> {
        self.dependencies.keys()
    }

    /// Get count of objects in the graph
    pub fn len(&self) -> usize {
        self.dependencies.len()
    }

    /// Check if the graph is empty
    pub fn is_empty(&self) -> bool {
        self.dependencies.is_empty()
    }

    /// Find all objects that transitively depend on the given object
    pub fn find_all_dependents(&self, qualified_name: &str) -> HashSet<String> {
        let mut visited = HashSet::new();
        let mut queue = vec![qualified_name.to_string()];

        while let Some(current) = queue.pop() {
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current.clone());

            if let Some(deps) = self.dependencies.get(&current) {
                for dep in &deps.depended_by {
                    let name = dep.qualified_name();
                    if !visited.contains(&name) {
                        queue.push(name);
                    }
                }
            }
        }

        visited.remove(qualified_name);
        visited
    }
}

/// Configuration for the dependency analyzer
#[derive(Debug, Clone)]
pub struct AnalyzerConfig {
    /// Default schema to use when schema is not specified
    pub default_schema: Option<String>,
    /// Whether to include system schemas in analysis
    pub include_system_schemas: bool,
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            default_schema: Some("public".to_string()),
            include_system_schemas: false,
        }
    }
}

impl AnalyzerConfig {
    /// Create a new config with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the default schema
    pub fn with_default_schema(mut self, schema: impl Into<String>) -> Self {
        self.default_schema = Some(schema.into());
        self
    }

    /// Include system schemas in analysis
    pub fn with_system_schemas(mut self) -> Self {
        self.include_system_schemas = true;
        self
    }
}

/// Analyzer for extracting dependencies from SQL definitions
#[derive(Debug, Clone)]
pub struct DependencyAnalyzer {
    config: AnalyzerConfig,
}

impl DependencyAnalyzer {
    /// Create a new dependency analyzer with default config
    pub fn new() -> Self {
        Self {
            config: AnalyzerConfig::default(),
        }
    }

    /// Create a dependency analyzer with custom config
    pub fn with_config(config: AnalyzerConfig) -> Self {
        Self { config }
    }

    /// Get the analyzer configuration
    pub fn config(&self) -> &AnalyzerConfig {
        &self.config
    }

    /// Extract dependencies from a view definition SQL
    pub fn extract_from_view_sql(&self, sql: &str) -> Vec<ObjectRef> {
        extract_table_references(sql)
    }

    /// Build a dependency graph from multiple object definitions
    pub fn build_graph(&self, definitions: &[(ObjectRef, &str)]) -> DependencyGraph {
        let mut graph = DependencyGraph::new();

        for (obj, sql) in definitions {
            let deps = self.extract_from_view_sql(sql);
            graph.add_object(obj.clone(), deps);
        }

        graph
    }
}

impl Default for DependencyAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract table references from SQL text
///
/// This function parses SQL and extracts table names referenced in:
/// - FROM clauses
/// - JOIN clauses
/// - Subqueries
/// - CTEs (WITH clause)
pub fn extract_table_references(sql: &str) -> Vec<ObjectRef> {
    let mut refs = Vec::new();
    let mut seen = HashSet::new();

    let sql_upper = sql.to_uppercase();

    let from_positions = find_keyword_positions(&sql_upper, "FROM");
    let join_positions = find_keyword_positions(&sql_upper, "JOIN");
    let update_positions = find_keyword_positions(&sql_upper, "UPDATE");
    let into_positions = find_keyword_positions(&sql_upper, "INTO");

    let all_positions: Vec<(usize, &str)> = from_positions
        .iter()
        .map(|p| (*p, "FROM"))
        .chain(join_positions.iter().map(|p| (*p, "JOIN")))
        .chain(update_positions.iter().map(|p| (*p, "UPDATE")))
        .chain(into_positions.iter().map(|p| (*p, "INTO")))
        .collect();

    for (pos, _keyword) in all_positions {
        if let Some(table_ref) = extract_table_after_keyword(sql, pos) {
            let qualified = table_ref.qualified_name();
            if !seen.contains(&qualified) && !is_sql_keyword(&table_ref.name) {
                seen.insert(qualified);
                refs.push(table_ref);
            }
        }
    }

    refs
}

fn find_keyword_positions(sql: &str, keyword: &str) -> Vec<usize> {
    let mut positions = Vec::new();
    let mut start = 0;

    while let Some(pos) = sql[start..].find(keyword) {
        let absolute_pos = start + pos;
        let before_ok =
            absolute_pos == 0 || !sql.as_bytes()[absolute_pos - 1].is_ascii_alphanumeric();
        let after_ok = absolute_pos + keyword.len() >= sql.len()
            || !sql.as_bytes()[absolute_pos + keyword.len()].is_ascii_alphanumeric();

        if before_ok && after_ok {
            positions.push(absolute_pos);
        }
        start = absolute_pos + keyword.len();
    }

    positions
}

fn extract_table_after_keyword(sql: &str, keyword_pos: usize) -> Option<ObjectRef> {
    let after_keyword = &sql[keyword_pos..];
    let parts: Vec<&str> = after_keyword.split_whitespace().collect();

    if parts.len() < 2 {
        return None;
    }

    let table_part = parts[1].trim_end_matches(|c| c == ',' || c == ';' || c == ')');

    if table_part.is_empty() || table_part.starts_with('(') {
        return None;
    }

    let (schema, name) = if table_part.contains('.') {
        let parts: Vec<&str> = table_part.splitn(2, '.').collect();
        (Some(parts[0].to_string()), parts[1].to_string())
    } else {
        (None, table_part.to_string())
    };

    let clean_name = name
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']');
    let clean_schema = schema.map(|s| {
        s.trim_matches('"')
            .trim_matches('`')
            .trim_matches('[')
            .trim_matches(']')
            .to_string()
    });

    if clean_name.is_empty() {
        return None;
    }

    Some(ObjectRef {
        schema: clean_schema,
        name: clean_name.to_string(),
        object_type: ObjectType::Table,
    })
}

fn is_sql_keyword(word: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "SELECT",
        "FROM",
        "WHERE",
        "JOIN",
        "INNER",
        "LEFT",
        "RIGHT",
        "OUTER",
        "FULL",
        "CROSS",
        "ON",
        "AND",
        "OR",
        "NOT",
        "IN",
        "BETWEEN",
        "LIKE",
        "IS",
        "NULL",
        "TRUE",
        "FALSE",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "AS",
        "ORDER",
        "BY",
        "GROUP",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "UNION",
        "ALL",
        "DISTINCT",
        "SET",
        "VALUES",
        "INTO",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "ALTER",
        "DROP",
        "TABLE",
        "VIEW",
        "INDEX",
        "FUNCTION",
        "PROCEDURE",
        "TRIGGER",
        "SCHEMA",
        "DATABASE",
        "IF",
        "EXISTS",
    ];
    KEYWORDS.contains(&word.to_uppercase().as_str())
}
