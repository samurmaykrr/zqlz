//! SQL Context Analyzer - AST-based context analysis for intelligent completions
//!
//! Uses tree-sitter to precisely determine cursor context in SQL queries.

use super::parser_pool::with_parser;
use crate::context_tables::{self, TableRef};
use tree_sitter::Tree;
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
    /// Inside a CREATE TABLE column-definition block
    ///
    /// Detected when the cursor is between the opening `(` and the unmatched
    /// closing `)` of a `CREATE [TEMPORARY] TABLE name (...)` statement.
    CreateTable,
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
            let offset = crate::clamp_to_char_boundary(&source, offset);

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

            tracing::trace!(root_sexp = ?root_node.to_sexp(), "Root node");

            let cursor_node = root_node.descendant_for_byte_range(offset, offset);

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

            // Tables in scope, resolved before clause detection so both the
            // text-based rescue and the AST walk can use them.
            let available_tables = cursor_node
                .map(|node| {
                    context_tables::extract_available_tables_at_position(
                        tree.root_node(),
                        node,
                        &source,
                    )
                })
                .unwrap_or_default();
            let available_tables = if available_tables.is_empty() {
                Self::tables_with_placeholder_select_item(parser, &source, offset)
            } else {
                available_tables
            };

            // Analyze context based on the cursor position
            let result =
                self.analyze_node_context(&tree, cursor_node, offset, &source, available_tables);
            tracing::trace!(context = ?result, "Final context result");
            Ok(result)
        })
        .unwrap_or(SqlContext::General)
    }

    /// Re-parses with a placeholder in the select list to recover the FROM clause.
    ///
    /// An empty projection (`select |  from t`) makes the grammar fold `from t` into
    /// the select item as a field plus alias, so no `from` node exists and the
    /// completions fall back to every column in the schema. Filling the hole makes
    /// the statement parse the way the user means it.
    fn tables_with_placeholder_select_item(
        parser: &mut tree_sitter::Parser,
        source: &str,
        offset: usize,
    ) -> Vec<TableRef> {
        // Trailing space so the placeholder cannot glue onto whatever follows the
        // cursor (`select a,|from t` would otherwise become one identifier).
        const PLACEHOLDER: &str = "zqlz_cursor ";

        if offset > source.len() || !source.is_char_boundary(offset) {
            return Vec::new();
        }

        let mut sanitized = String::with_capacity(source.len() + PLACEHOLDER.len());
        sanitized.push_str(&source[..offset]);
        sanitized.push_str(PLACEHOLDER);
        sanitized.push_str(&source[offset..]);

        let Some(tree) = parser.parse(&sanitized, None) else {
            return Vec::new();
        };
        let root = tree.root_node();
        let Some(node) = root.descendant_for_byte_range(offset, offset) else {
            return Vec::new();
        };

        context_tables::extract_available_tables_at_position(root, node, &sanitized)
    }

    fn analyze_node_context(
        &self,
        tree: &Tree,
        cursor_node: Option<tree_sitter::Node>,
        offset: usize,
        source: &str,
        available_tables: Vec<TableRef>,
    ) -> SqlContext {
        let Some(node) = cursor_node else {
            tracing::trace!("No node at cursor");
            return SqlContext::General;
        };

        tracing::trace!(kind = node.kind(), "Starting node context analysis");

        // Text-based special cases checked before AST traversal, because incomplete SQL
        // causes tree-sitter to produce ERROR nodes that prevent accurate clause detection.

        if offset > 0 {
            let text_before = &source[..offset];

            if let Some(context) =
                crate::context_rescue::rescue_incomplete_context(text_before, &available_tables)
            {
                return context;
            }
        }

        // Walk up the tree to find the enclosing SQL clause
        let mut current = node;
        loop {
            let node_type = current.kind();

            match node_type {
                "select_statement" | "statement" | "select" => {
                    // Prefer the enclosing node's own tables, but keep the resolved
                    // set when it has none — that is the placeholder-recovered list
                    // for a statement the grammar could not parse as written.
                    let scoped = context_tables::extract_table_refs(current, source);
                    return SqlContext::SelectList {
                        available_tables: if scoped.is_empty() {
                            available_tables
                        } else {
                            scoped
                        },
                    };
                }
                "create_table_statement" | "create_table" => {
                    return SqlContext::CreateTable;
                }
                "from_clause" | "table_reference" | "from" => {
                    return SqlContext::FromClause;
                }
                "join_clause" | "join" => {
                    // Find parent SELECT to get existing tables
                    let existing_tables = if let Some(parent) = current.parent() {
                        context_tables::extract_table_refs(parent, source)
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
                        if child.kind() == "identifier"
                            && let Ok(cte_name) = child.utf8_text(source.as_bytes())
                        {
                            return SqlContext::CommonTableExpression {
                                cte_name: cte_name.to_string(),
                            };
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
        tracing::trace!("analyze_condition_clause called for node: {}", node.kind());

        // Find the parent SELECT statement to get available tables
        let mut current = node;
        while let Some(parent) = current.parent() {
            tracing::trace!("Checking parent: {}", parent.kind());
            if parent.kind() == "select_statement"
                || parent.kind() == "statement"
                || parent.kind() == "select"
            {
                let available_tables = context_tables::extract_table_refs(parent, source);
                tracing::trace!(
                    "Found SELECT statement, extracted {} tables: {:?}",
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

        tracing::trace!("No SELECT statement found, returning empty tables");
        SqlContext::ConditionClause {
            available_tables: Vec::new(),
        }
    }

    /// Extract CTE (Common Table Expression) names from WITH clauses
    /// Returns a list of CTE names that can be referenced as tables
    pub fn extract_cte_names(&self, text: &Rope, _offset: usize) -> Vec<String> {
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

            let mut cte_names = Vec::new();
            context_tables::extract_ctes_from_node(tree.root_node(), &source, &mut cte_names);

            tracing::trace!("Found {} CTEs: {:?}", cte_names.len(), cte_names);
            Ok(cte_names)
        })
        .unwrap_or_default()
    }

    pub fn extract_available_tables(&self, text: &Rope, offset: usize) -> Vec<TableRef> {
        with_parser(|parser| {
            let source = text.to_string();
            let offset = crate::clamp_to_char_boundary(&source, offset);
            let Some(tree) = parser.parse(&source, None) else {
                return Ok(Vec::new());
            };

            let root_node = tree.root_node();
            let current_node = root_node
                .descendant_for_byte_range(offset, offset)
                .unwrap_or(root_node);
            Ok(context_tables::extract_available_tables_at_position(
                root_node,
                current_node,
                &source,
            ))
        })
        .unwrap_or_default()
    }
}

#[cfg(test)]
#[path = "context_analyzer_tests.rs"]
mod tests;
