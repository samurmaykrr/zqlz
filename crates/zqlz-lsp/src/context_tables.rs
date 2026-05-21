use std::collections::HashMap;

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
        self.alias
            .as_ref()
            .is_some_and(|alias| alias.eq_ignore_ascii_case(identifier))
            || self.table_name.eq_ignore_ascii_case(identifier)
    }
}

/// Extract table references with aliases from a SELECT statement.
pub(crate) fn extract_table_refs(select_node: tree_sitter::Node, source: &str) -> Vec<TableRef> {
    let mut tables = Vec::new();
    let mut cursor = select_node.walk();

    for child in select_node.children(&mut cursor) {
        if child.kind() == "from_clause" || child.kind() == "from" {
            extract_table_refs_from_clause(child, source, &mut tables);
        }
    }

    tables
}

pub(crate) fn extract_available_tables_at_position(
    root: tree_sitter::Node,
    current_node: tree_sitter::Node,
    source: &str,
) -> Vec<TableRef> {
    let mut node = current_node;

    loop {
        if is_select_node(node) {
            return extract_table_refs(node, source);
        }

        if let Some(parent) = node.parent() {
            node = parent;
        } else {
            break;
        }
    }

    find_first_select_statement(root, source)
}

pub(crate) fn extract_ctes_from_node(
    node: tree_sitter::Node,
    source: &str,
    cte_names: &mut Vec<String>,
) {
    if node.kind() == "cte"
        && let Some(cte_name) = extract_cte_name(node, source)
    {
        cte_names.push(cte_name);
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        extract_ctes_from_node(child, source, cte_names);
    }
}

pub(crate) fn extract_cte_name(cte_node: tree_sitter::Node, source: &str) -> Option<String> {
    let mut cursor = cte_node.walk();
    for child in cte_node.children(&mut cursor) {
        if child.kind() == "identifier" {
            return child
                .utf8_text(source.as_bytes())
                .ok()
                .map(|name| name.to_string());
        }
    }
    None
}

/// Build a map of aliases to table names from a list of table references
pub fn build_alias_map(table_refs: &[TableRef]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for table_ref in table_refs {
        if let Some(alias) = &table_ref.alias {
            map.insert(alias.to_lowercase(), table_ref.table_name.clone());
        }
        map.insert(
            table_ref.table_name.to_lowercase(),
            table_ref.table_name.clone(),
        );
    }
    map
}

fn extract_table_refs_from_clause(
    from_node: tree_sitter::Node,
    source: &str,
    tables: &mut Vec<TableRef>,
) {
    let mut cursor = from_node.walk();

    for child in from_node.children(&mut cursor) {
        match child.kind() {
            "table_reference" | "table_factor" | "relation" => {
                if let Some(table_ref) = parse_table_reference(child, source) {
                    tables.push(table_ref);
                }
            }
            "join_clause" | "join" => {
                extract_table_refs_from_clause(child, source, tables);
            }
            _ => {
                extract_table_refs_from_clause(child, source, tables);
            }
        }
    }
}

fn parse_table_reference(node: tree_sitter::Node, source: &str) -> Option<TableRef> {
    let mut table_name = None;
    let mut alias = None;

    if node.kind() == "relation" {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if child.kind() == "object_reference"
                && let Some(name_node) = child.child_by_field_name("name")
            {
                table_name = name_node
                    .utf8_text(source.as_bytes())
                    .ok()
                    .map(|name| name.to_string());
            }
        }

        if let Some(alias_node) = node.child_by_field_name("alias") {
            alias = alias_node
                .utf8_text(source.as_bytes())
                .ok()
                .map(|alias| alias.to_string());
        }
    } else {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if matches!(child.kind(), "identifier" | "table_name") {
                if table_name.is_none() {
                    table_name = child
                        .utf8_text(source.as_bytes())
                        .ok()
                        .map(|name| name.to_string());
                } else if alias.is_none() {
                    alias = child
                        .utf8_text(source.as_bytes())
                        .ok()
                        .map(|alias| alias.to_string());
                }
            }
        }
    }

    table_name.map(|name| TableRef::new(name, alias))
}

fn find_first_select_statement(node: tree_sitter::Node, source: &str) -> Vec<TableRef> {
    if is_select_node(node) {
        return extract_table_refs(node, source);
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        let result = find_first_select_statement(child, source);
        if !result.is_empty() {
            return result;
        }
    }

    Vec::new()
}

fn is_select_node(node: tree_sitter::Node) -> bool {
    matches!(node.kind(), "select_statement" | "statement" | "select")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_ref() {
        let table = TableRef::new("users".to_string(), Some("u".to_string()));
        assert_eq!(table.identifier(), "u");
        assert!(table.matches("u"));
        assert!(table.matches("users"));

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
