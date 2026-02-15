//! SQL Context Analyzer - AST-based context analysis for intelligent completions
//!
//! Uses tree-sitter to precisely determine cursor context in SQL queries.

use super::parser_pool::with_parser;
use std::collections::HashMap;
use tree_sitter::{Parser, Query, QueryCursor, Tree};
use zqlz_ui::widgets::Rope;

/// SQL context information derived from AST
#[derive(Debug, Clone)]
pub enum SqlContext {
    /// General context - start of query or unknown
    General,
    /// Inside SELECT column list
    SelectList {
        /// Tables available in FROM clause with their aliases
        available_tables: Vec<TableRef>,
    },
    /// After FROM or JOIN keyword - expecting table name
    FromClause,
    /// After JOIN keyword - expecting table name
    JoinClause {
        /// Tables already in the FROM clause
        existing_tables: Vec<TableRef>,
    },
    /// In WHERE/ON/HAVING clause - expecting conditions
    ConditionClause {
        /// Tables available for column references
        available_tables: Vec<TableRef>,
    },
    /// After dot - expecting columns from specific table
    AfterDot {
        /// Table or alias being referenced
        table_or_alias: String,
        /// Available tables for alias resolution
        available_tables: Vec<TableRef>,
    },
    /// Inside CTE (WITH clause)
    CommonTableExpression {
        /// CTE name
        cte_name: String,
    },
    /// Inside subquery
    Subquery {
        /// Parent context
        parent_tables: Vec<TableRef>,
    },
}

/// Table reference with optional alias
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    /// Actual table name
    pub table_name: String,
    /// Alias if present
    pub alias: Option<String>,
}

impl TableRef {
    pub fn new(table_name: String, alias: Option<String>) -> Self {
        Self { table_name, alias }
    }

    /// Get the identifier to use when referencing this table
    pub fn identifier(&self) -> &str {
        self.alias.as_ref().unwrap_or(&self.table_name)
    }

    /// Check if the given identifier matches this table reference
    pub fn matches(&self, identifier: &str) -> bool {
        if let Some(alias) = &self.alias {
            alias == identifier
        } else {
            self.table_name == identifier
        }
    }
}

/// Context analyzer using tree-sitter with parser pooling
pub struct ContextAnalyzer;

impl ContextAnalyzer {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self)
    }

    /// Analyze SQL context at a given byte offset
    pub fn analyze(&self, text: &Rope, offset: usize) -> SqlContext {
        with_parser(|parser| {
            let source = text.to_string();

            tracing::trace!(offset = offset, text = %source, "Analyzing SQL context");
            if offset > 0 && offset <= source.len() {
                let text_before = &source[..offset];
                tracing::trace!(text_before = %text_before, "Text before cursor");
            }

            // Parse the SQL
            let tree = match parser.parse(&source, None) {
                Some(tree) => tree,
                None => {
                    tracing::trace!("Failed to parse SQL");
                    return Ok(SqlContext::General);
                }
            };

            // Find the node at the cursor position
            let root_node = tree.root_node();

            #[cfg(test)]
            println!(
                "DEBUG analyze: Root node s-expression:\n{}",
                root_node.to_sexp()
            );

            tracing::trace!(root_sexp = ?root_node.to_sexp(), "Root node");

            let cursor_node = root_node.descendant_for_byte_range(offset, offset);

            #[cfg(test)]
            if let Some(ref node) = cursor_node {
                println!(
                    "DEBUG analyze: cursor_node kind = {}, range = {:?}",
                    node.kind(),
                    node.byte_range()
                );
            } else {
                println!("DEBUG analyze: cursor_node is None");
            }
            if let Some(node) = cursor_node {
                tracing::trace!(
                    kind = node.kind(),
                    range = ?node.byte_range(),
                    text = ?node.utf8_text(source.as_bytes()).ok(),
                    "Node at cursor"
                );

                // Log parent chain
                let mut current = node;
                let mut depth = 0;
                tracing::trace!("Parent chain:");
                loop {
                    tracing::trace!(
                        depth = depth,
                        kind = current.kind(),
                        range = ?current.byte_range(),
                        "Parent node"
                    );
                    if let Some(parent) = current.parent() {
                        current = parent;
                        depth += 1;
                    } else {
                        break;
                    }
                }

                // Log siblings
                if let Some(prev) = node.prev_sibling() {
                    tracing::trace!(
                        kind = prev.kind(),
                        text = ?prev.utf8_text(source.as_bytes()).ok(),
                        "Previous sibling"
                    );
                }
                if let Some(next) = node.next_sibling() {
                    tracing::trace!(
                        kind = next.kind(),
                        text = ?next.utf8_text(source.as_bytes()).ok(),
                        "Next sibling"
                    );
                }
            } else {
                tracing::trace!("No node found at cursor offset");
            }

            // Analyze context based on the cursor position
            let result = self.analyze_node_context(&tree, cursor_node, offset, &source);
            tracing::trace!(context = ?result, "Final context result");
            Ok(result)
        })
        .unwrap_or(SqlContext::General)
    }

    fn analyze_node_context(
        &self,
        tree: &Tree,
        cursor_node: Option<tree_sitter::Node>,
        offset: usize,
        source: &str,
    ) -> SqlContext {
        let Some(node) = cursor_node else {
            tracing::trace!("No node at cursor");
            return SqlContext::General;
        };

        tracing::trace!(kind = node.kind(), "Starting node context analysis");

        // Extract available tables from the enclosing SELECT statement
        let available_tables =
            self.extract_available_tables_at_position(tree.root_node(), node, source);

        // Special case: Check if we're immediately after a dot by looking at the text
        if offset > 0 {
            let text_before = &source[..offset];
            if text_before.ends_with('.') {
                tracing::trace!("Text before cursor ends with dot");
                // Find the identifier before the dot
                if let Some(last_word) = text_before.trim_end_matches('.').split_whitespace().last()
                {
                    let table_name = last_word.trim_end_matches('.');
                    tracing::trace!(table_name = table_name, "Found table name before dot");
                    return SqlContext::AfterDot {
                        table_or_alias: table_name.to_string(),
                        available_tables: available_tables.clone(),
                    };
                }
            }
        }

        // Check if we're after a dot (table.column pattern) using AST or text analysis
        // This is handled by the text check below

        // Walk up the tree to find the enclosing SQL clause
        let mut current = node;
        loop {
            let node_type = current.kind();

            match node_type {
                "select_statement" | "statement" | "select" => {
                    // In SELECT context - show SelectList with available tables
                    let available_tables = self.extract_table_refs(current, source);
                    return SqlContext::SelectList { available_tables };
                }
                "from_clause" | "table_reference" | "from" => {
                    return SqlContext::FromClause;
                }
                "join_clause" | "join" => {
                    // Find parent SELECT to get existing tables
                    let existing_tables = if let Some(parent) = current.parent() {
                        self.extract_table_refs(parent, source)
                    } else {
                        Vec::new()
                    };
                    return SqlContext::JoinClause { existing_tables };
                }
                "where_clause" | "on_clause" | "having_clause" | "where" => {
                    return self.analyze_condition_clause(current, source);
                }
                "with_clause" | "cte" => {
                    // Extract CTE name
                    let mut cursor = current.walk();
                    for child in current.children(&mut cursor) {
                        if child.kind() == "identifier" {
                            if let Ok(cte_name) = child.utf8_text(source.as_bytes()) {
                                return SqlContext::CommonTableExpression {
                                    cte_name: cte_name.to_string(),
                                };
                            }
                        }
                    }
                    return SqlContext::General;
                }
                _ => {}
            }

            // Move to parent
            if let Some(parent) = current.parent() {
                current = parent;
            } else {
                break;
            }
        }

        tracing::trace!("No specific context found, returning General");
        SqlContext::General
    }

    fn analyze_condition_clause(&self, node: tree_sitter::Node, source: &str) -> SqlContext {
        tracing::warn!(
            "🔍 analyze_condition_clause called for node: {}",
            node.kind()
        );

        // Find the parent SELECT statement to get available tables
        let mut current = node;
        while let Some(parent) = current.parent() {
            tracing::warn!("  Checking parent: {}", parent.kind());
            if parent.kind() == "select_statement"
                || parent.kind() == "statement"
                || parent.kind() == "select"
            {
                let available_tables = self.extract_table_refs(parent, source);
                tracing::warn!(
                    "  ✅ Found SELECT statement, extracted {} tables: {:?}",
                    available_tables.len(),
                    available_tables
                        .iter()
                        .map(|t| &t.table_name)
                        .collect::<Vec<_>>()
                );
                return SqlContext::ConditionClause { available_tables };
            }
            current = parent;
        }

        tracing::warn!("  ⚠️ No SELECT statement found, returning empty tables");
        SqlContext::ConditionClause {
            available_tables: Vec::new(),
        }
    }

    fn analyze_cte(&self, node: tree_sitter::Node, source: &str) -> SqlContext {
        // Extract CTE name
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.kind() == "identifier" {
                if let Ok(cte_name) = child.utf8_text(source.as_bytes()) {
                    return SqlContext::CommonTableExpression {
                        cte_name: cte_name.to_string(),
                    };
                }
            }
        }

        SqlContext::General
    }

    /// Extract table references with aliases from a SELECT statement
    fn extract_table_refs(&self, select_node: tree_sitter::Node, source: &str) -> Vec<TableRef> {
        let mut tables = Vec::new();
        let mut cursor = select_node.walk();

        // Find FROM clause and all JOIN clauses
        for child in select_node.children(&mut cursor) {
            #[cfg(test)]
            println!("DEBUG extract_table_refs: child kind = {}", child.kind());

            if child.kind() == "from_clause" || child.kind() == "from" {
                #[cfg(test)]
                println!("DEBUG: Found FROM clause, extracting tables");
                self.extract_table_refs_from_clause(child, source, &mut tables);
            }
        }

        #[cfg(test)]
        println!(
            "DEBUG extract_table_refs: Total tables found = {}",
            tables.len()
        );

        tables
    }

    fn extract_table_refs_from_clause(
        &self,
        from_node: tree_sitter::Node,
        source: &str,
        tables: &mut Vec<TableRef>,
    ) {
        let mut cursor = from_node.walk();

        for child in from_node.children(&mut cursor) {
            match child.kind() {
                "table_reference" | "table_factor" | "relation" => {
                    if let Some(table_ref) = self.parse_table_reference(child, source) {
                        tables.push(table_ref);
                    }
                }
                "join_clause" | "join" => {
                    // Recursively extract from JOIN clauses
                    self.extract_table_refs_from_clause(child, source, tables);
                }
                _ => {
                    // Recursively search in children
                    self.extract_table_refs_from_clause(child, source, tables);
                }
            }
        }
    }

    fn parse_table_reference(&self, node: tree_sitter::Node, source: &str) -> Option<TableRef> {
        let mut table_name = None;
        let mut alias = None;

        // For tree-sitter-sequel, check if this is a "relation" node
        if node.kind() == "relation" {
            // Look for object_reference child node
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                if child.kind() == "object_reference" {
                    // Extract the "name" field from object_reference
                    if let Some(name_node) = child.child_by_field_name("name") {
                        table_name = name_node
                            .utf8_text(source.as_bytes())
                            .ok()
                            .map(|s| s.to_string());
                    }
                }
            }

            // Extract alias field directly from relation node
            if let Some(alias_node) = node.child_by_field_name("alias") {
                alias = alias_node
                    .utf8_text(source.as_bytes())
                    .ok()
                    .map(|s| s.to_string());
            }
        } else {
            // Original logic for other grammars
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                match child.kind() {
                    "identifier" | "table_name" => {
                        if table_name.is_none() {
                            table_name = child
                                .utf8_text(source.as_bytes())
                                .ok()
                                .map(|s| s.to_string());
                        } else if alias.is_none() {
                            // Second identifier is the alias
                            alias = child
                                .utf8_text(source.as_bytes())
                                .ok()
                                .map(|s| s.to_string());
                        }
                    }
                    "as" => {
                        // Next identifier will be the alias
                        continue;
                    }
                    _ => {}
                }
            }
        }

        table_name.map(|name| TableRef::new(name, alias))
    }

    /// Extract CTE (Common Table Expression) names from WITH clauses
    /// Returns a list of CTE names that can be referenced as tables
    pub fn extract_cte_names(&self, text: &Rope, offset: usize) -> Vec<String> {
        with_parser(|parser| {
            let source = text.to_string();
            let tree = match parser.parse(&source, None) {
                Some(tree) => tree,
                None => {
                    tracing::trace!("Failed to parse SQL for CTE extraction");
                    return Ok(Vec::new());
                }
            };

            tracing::trace!("Extracting CTEs from SQL");

            #[cfg(test)]
            println!(
                "DEBUG CTE: Root node s-expression:\n{}",
                tree.root_node().to_sexp()
            );

            let mut cte_names = Vec::new();
            self.extract_ctes_from_node(tree.root_node(), &source, &mut cte_names);

            #[cfg(test)]
            println!("DEBUG CTE: Found {} CTEs: {:?}", cte_names.len(), cte_names);

            tracing::trace!("Found {} CTEs: {:?}", cte_names.len(), cte_names);
            Ok(cte_names)
        })
        .unwrap_or_default()
    }

    /// Recursively extract CTE names from a node
    fn extract_ctes_from_node(
        &self,
        node: tree_sitter::Node,
        source: &str,
        cte_names: &mut Vec<String>,
    ) {
        tracing::trace!("Checking node kind: {}", node.kind());

        // Look for CTE nodes directly (tree-sitter-sequel doesn't have with_clause wrapper)
        if node.kind() == "cte" {
            tracing::trace!("Found cte node");
            // Extract the CTE name (first identifier in the cte node)
            if let Some(cte_name) = self.extract_cte_name(node, source) {
                tracing::trace!("  Extracted CTE name: {}", cte_name);
                cte_names.push(cte_name);
            }
        }

        // Recursively search child nodes
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.extract_ctes_from_node(child, source, cte_names);
        }
    }

    /// Extract the name from a CTE node
    fn extract_cte_name(&self, cte_node: tree_sitter::Node, source: &str) -> Option<String> {
        let mut cursor = cte_node.walk();
        for child in cte_node.children(&mut cursor) {
            tracing::trace!("    CTE child kind: {}", child.kind());
            if child.kind() == "identifier" {
                let name = child
                    .utf8_text(source.as_bytes())
                    .ok()
                    .map(|s| s.to_string());
                tracing::trace!("    Found identifier: {:?}", name);
                return name;
            }
        }
        None
    }

    /// Extract available tables at a specific position in the AST
    /// Walks up from the current node to find the enclosing SELECT statement and extracts table references
    fn extract_available_tables_at_position(
        &self,
        root: tree_sitter::Node,
        current_node: tree_sitter::Node,
        source: &str,
    ) -> Vec<TableRef> {
        let mut node = current_node;

        #[cfg(test)]
        println!(
            "DEBUG extract_available_tables_at_position: starting from node kind = {}",
            node.kind()
        );

        // First, try walking up the tree to find the enclosing SELECT statement
        let mut depth = 0;
        loop {
            #[cfg(test)]
            println!(
                "DEBUG: Walking up depth={}, node kind = {}",
                depth,
                node.kind()
            );

            if node.kind() == "select_statement"
                || node.kind() == "statement"
                || node.kind() == "select"
            {
                #[cfg(test)]
                println!(
                    "DEBUG: Found select_statement while walking up at depth {}",
                    depth
                );
                return self.extract_table_refs(node, source);
            }

            if let Some(parent) = node.parent() {
                node = parent;
                depth += 1;
            } else {
                #[cfg(test)]
                println!("DEBUG: Reached root without finding select_statement");
                break;
            }
        }

        // If we couldn't find SELECT by walking up, search the entire root for any SELECT statement
        // This handles cases where tree-sitter doesn't create proper structure for incomplete queries
        #[cfg(test)]
        println!(
            "DEBUG: No SELECT found while walking up, searching entire tree. Root kind = {}",
            root.kind()
        );

        let result = self.find_first_select_statement(root, source);

        #[cfg(test)]
        println!(
            "DEBUG: find_first_select_statement returned {} tables",
            result.len()
        );

        result
    }

    /// Find the first SELECT statement in the tree
    fn find_first_select_statement(&self, node: tree_sitter::Node, source: &str) -> Vec<TableRef> {
        #[cfg(test)]
        println!(
            "DEBUG find_first_select_statement: node kind = {}",
            node.kind()
        );

        if node.kind() == "select_statement"
            || node.kind() == "statement"
            || node.kind() == "select"
        {
            #[cfg(test)]
            println!("DEBUG: Found select node! Extracting table refs");
            let result = self.extract_table_refs(node, source);
            #[cfg(test)]
            println!("DEBUG: Extracted {} tables", result.len());
            return result;
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            let result = self.find_first_select_statement(child, source);
            if !result.is_empty() {
                return result;
            }
        }

        Vec::new()
    }
}

/// Build a map of aliases to table names from a list of table references
pub fn build_alias_map(table_refs: &[TableRef]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for table_ref in table_refs {
        if let Some(alias) = &table_ref.alias {
            map.insert(alias.clone(), table_ref.table_name.clone());
        }
        // Also map table name to itself for consistency
        map.insert(table_ref.table_name.clone(), table_ref.table_name.clone());
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_ref() {
        let table = TableRef::new("users".to_string(), Some("u".to_string()));
        assert_eq!(table.identifier(), "u");
        assert!(table.matches("u"));
        assert!(!table.matches("users"));

        let table_no_alias = TableRef::new("posts".to_string(), None);
        assert_eq!(table_no_alias.identifier(), "posts");
        assert!(table_no_alias.matches("posts"));
    }

    #[test]
    fn test_build_alias_map() {
        let refs = vec![
            TableRef::new("users".to_string(), Some("u".to_string())),
            TableRef::new("posts".to_string(), None),
        ];

        let map = build_alias_map(&refs);
        assert_eq!(map.get("u"), Some(&"users".to_string()));
        assert_eq!(map.get("users"), Some(&"users".to_string()));
        assert_eq!(map.get("posts"), Some(&"posts".to_string()));
    }
}
