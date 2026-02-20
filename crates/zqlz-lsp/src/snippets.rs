use lsp_types::{CompletionItem, CompletionItemKind, InsertTextFormat};

/// SQL snippet templates for common patterns
#[allow(dead_code)]
pub struct SnippetProvider {
    snippets: Vec<SqlSnippet>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct SqlSnippet {
    label: String,
    description: String,
    template: String,
    keywords: Vec<String>,
}

#[allow(dead_code)]
impl SnippetProvider {
    pub fn new() -> Self {
        let mut provider = Self {
            snippets: Vec::new(),
        };
        provider.initialize_snippets();
        provider
    }

    fn initialize_snippets(&mut self) {
        // SELECT snippets
        self.add_snippet(SqlSnippet {
            label: "select".to_string(),
            description: "SELECT query with WHERE clause".to_string(),
            template: "SELECT ${1:*}\nFROM ${2:table_name}\nWHERE ${3:condition}".to_string(),
            keywords: vec!["select".to_string(), "query".to_string()],
        });

        self.add_snippet(SqlSnippet {
            label: "select-join".to_string(),
            description: "SELECT with INNER JOIN".to_string(),
            template: "SELECT ${1:t1.column}, ${2:t2.column}\nFROM ${3:table1} ${4:t1}\nINNER JOIN ${5:table2} ${6:t2} ON ${4:t1}.${7:id} = ${6:t2}.${8:foreign_id}".to_string(),
            keywords: vec!["select".to_string(), "join".to_string()],
        });

        self.add_snippet(SqlSnippet {
            label: "select-left-join".to_string(),
            description: "SELECT with LEFT JOIN".to_string(),
            template: "SELECT ${1:t1.column}, ${2:t2.column}\nFROM ${3:table1} ${4:t1}\nLEFT JOIN ${5:table2} ${6:t2} ON ${4:t1}.${7:id} = ${6:t2}.${8:foreign_id}".to_string(),
            keywords: vec!["select".to_string(), "left".to_string(), "join".to_string()],
        });

        self.add_snippet(SqlSnippet {
            label: "select-group-by".to_string(),
            description: "SELECT with GROUP BY and aggregate".to_string(),
            template: "SELECT ${1:column}, ${2|COUNT,SUM,AVG,MAX,MIN|}(${3:column})\nFROM ${4:table_name}\nGROUP BY ${1:column}".to_string(),
            keywords: vec!["select".to_string(), "group".to_string(), "aggregate".to_string()],
        });

        self.add_snippet(SqlSnippet {
            label: "select-order-by".to_string(),
            description: "SELECT with ORDER BY".to_string(),
            template: "SELECT ${1:*}\nFROM ${2:table_name}\nORDER BY ${3:column} ${4|ASC,DESC|}"
                .to_string(),
            keywords: vec![
                "select".to_string(),
                "order".to_string(),
                "sort".to_string(),
            ],
        });

        self.add_snippet(SqlSnippet {
            label: "select-limit".to_string(),
            description: "SELECT with LIMIT".to_string(),
            template: "SELECT ${1:*}\nFROM ${2:table_name}\nLIMIT ${3:10}".to_string(),
            keywords: vec!["select".to_string(), "limit".to_string()],
        });

        self.add_snippet(SqlSnippet {
            label: "select-distinct".to_string(),
            description: "SELECT DISTINCT values".to_string(),
            template: "SELECT DISTINCT ${1:column}\nFROM ${2:table_name}".to_string(),
            keywords: vec![
                "select".to_string(),
                "distinct".to_string(),
                "unique".to_string(),
            ],
        });

        // INSERT snippets
        self.add_snippet(SqlSnippet {
            label: "insert".to_string(),
            description: "INSERT statement with values".to_string(),
            template: "INSERT INTO ${1:table_name} (${2:column1}, ${3:column2})\nVALUES (${4:value1}, ${5:value2})".to_string(),
            keywords: vec!["insert".to_string(), "add".to_string()],
        });

        self.add_snippet(SqlSnippet {
            label: "insert-multiple".to_string(),
            description: "INSERT multiple rows".to_string(),
            template: "INSERT INTO ${1:table_name} (${2:column1}, ${3:column2})\nVALUES\n  (${4:value1}, ${5:value2}),\n  (${6:value3}, ${7:value4})".to_string(),
            keywords: vec!["insert".to_string(), "multiple".to_string()],
        });

        self.add_snippet(SqlSnippet {
            label: "insert-select".to_string(),
            description: "INSERT from SELECT".to_string(),
            template: "INSERT INTO ${1:target_table} (${2:column1}, ${3:column2})\nSELECT ${2:column1}, ${3:column2}\nFROM ${4:source_table}\nWHERE ${5:condition}".to_string(),
            keywords: vec!["insert".to_string(), "select".to_string()],
        });

        // UPDATE snippets
        self.add_snippet(SqlSnippet {
            label: "update".to_string(),
            description: "UPDATE statement".to_string(),
            template: "UPDATE ${1:table_name}\nSET ${2:column} = ${3:value}\nWHERE ${4:condition}"
                .to_string(),
            keywords: vec!["update".to_string(), "modify".to_string()],
        });

        self.add_snippet(SqlSnippet {
            label: "update-multiple".to_string(),
            description: "UPDATE multiple columns".to_string(),
            template: "UPDATE ${1:table_name}\nSET\n  ${2:column1} = ${3:value1},\n  ${4:column2} = ${5:value2}\nWHERE ${6:condition}".to_string(),
            keywords: vec!["update".to_string(), "multiple".to_string()],
        });

        // DELETE snippets
        self.add_snippet(SqlSnippet {
            label: "delete".to_string(),
            description: "DELETE statement".to_string(),
            template: "DELETE FROM ${1:table_name}\nWHERE ${2:condition}".to_string(),
            keywords: vec!["delete".to_string(), "remove".to_string()],
        });

        // CREATE TABLE snippets
        self.add_snippet(SqlSnippet {
            label: "create-table".to_string(),
            description: "CREATE TABLE statement".to_string(),
            template: "CREATE TABLE ${1:table_name} (\n  ${2:id} ${3|INTEGER,TEXT,REAL,BLOB|} PRIMARY KEY,\n  ${4:column_name} ${5|INTEGER,TEXT,REAL,BLOB,DATETIME|} ${6|NOT NULL,NULL|}\n)".to_string(),
            keywords: vec!["create".to_string(), "table".to_string()],
        });

        self.add_snippet(SqlSnippet {
            label: "create-table-fk".to_string(),
            description: "CREATE TABLE with foreign key".to_string(),
            template: "CREATE TABLE ${1:table_name} (\n  ${2:id} INTEGER PRIMARY KEY,\n  ${3:foreign_id} INTEGER NOT NULL,\n  ${4:column} TEXT,\n  FOREIGN KEY (${3:foreign_id}) REFERENCES ${5:other_table}(${6:id})\n)".to_string(),
            keywords: vec!["create".to_string(), "table".to_string(), "foreign".to_string()],
        });

        // CREATE INDEX snippets
        self.add_snippet(SqlSnippet {
            label: "create-index".to_string(),
            description: "CREATE INDEX statement".to_string(),
            template: "CREATE INDEX ${1:idx_name}\nON ${2:table_name} (${3:column})".to_string(),
            keywords: vec!["create".to_string(), "index".to_string()],
        });

        self.add_snippet(SqlSnippet {
            label: "create-unique-index".to_string(),
            description: "CREATE UNIQUE INDEX statement".to_string(),
            template: "CREATE UNIQUE INDEX ${1:idx_name}\nON ${2:table_name} (${3:column})"
                .to_string(),
            keywords: vec![
                "create".to_string(),
                "unique".to_string(),
                "index".to_string(),
            ],
        });

        // WITH (CTE) snippets
        self.add_snippet(SqlSnippet {
            label: "with-cte".to_string(),
            description: "Common Table Expression (WITH clause)".to_string(),
            template: "WITH ${1:cte_name} AS (\n  SELECT ${2:column}\n  FROM ${3:table_name}\n  WHERE ${4:condition}\n)\nSELECT ${5:*}\nFROM ${1:cte_name}".to_string(),
            keywords: vec!["with".to_string(), "cte".to_string()],
        });

        // CASE expression
        self.add_snippet(SqlSnippet {
            label: "case".to_string(),
            description: "CASE expression".to_string(),
            template: "CASE\n  WHEN ${1:condition} THEN ${2:result}\n  WHEN ${3:condition} THEN ${4:result}\n  ELSE ${5:default_result}\nEND".to_string(),
            keywords: vec!["case".to_string(), "when".to_string()],
        });

        // Subquery snippets
        self.add_snippet(SqlSnippet {
            label: "subquery".to_string(),
            description: "Subquery in WHERE clause".to_string(),
            template: "WHERE ${1:column} IN (\n  SELECT ${2:column}\n  FROM ${3:table_name}\n  WHERE ${4:condition}\n)".to_string(),
            keywords: vec!["subquery".to_string(), "in".to_string()],
        });

        // EXISTS snippet
        self.add_snippet(SqlSnippet {
            label: "exists".to_string(),
            description: "EXISTS subquery".to_string(),
            template:
                "WHERE EXISTS (\n  SELECT 1\n  FROM ${1:table_name}\n  WHERE ${2:condition}\n)"
                    .to_string(),
            keywords: vec!["exists".to_string(), "subquery".to_string()],
        });

        // UNION snippet
        self.add_snippet(SqlSnippet {
            label: "union".to_string(),
            description: "UNION query".to_string(),
            template:
                "SELECT ${1:column}\nFROM ${2:table1}\nUNION\nSELECT ${1:column}\nFROM ${3:table2}"
                    .to_string(),
            keywords: vec!["union".to_string(), "combine".to_string()],
        });

        // Transaction snippets
        self.add_snippet(SqlSnippet {
            label: "transaction".to_string(),
            description: "Transaction block".to_string(),
            template: "BEGIN TRANSACTION;\n\n${1:-- Your SQL statements here}\n\nCOMMIT;"
                .to_string(),
            keywords: vec![
                "transaction".to_string(),
                "begin".to_string(),
                "commit".to_string(),
            ],
        });
    }

    fn add_snippet(&mut self, snippet: SqlSnippet) {
        self.snippets.push(snippet);
    }

    pub fn get_completions(&self, filter: &str) -> Vec<CompletionItem> {
        let filter_lower = filter.to_lowercase();

        self.snippets
            .iter()
            .filter(|snippet| {
                if filter.is_empty() {
                    return true;
                }

                // Match against label or keywords
                snippet.label.to_lowercase().contains(&filter_lower)
                    || snippet.keywords.iter().any(|kw| kw.contains(&filter_lower))
            })
            .map(|snippet| self.snippet_to_completion_item(snippet))
            .collect()
    }

    fn snippet_to_completion_item(&self, snippet: &SqlSnippet) -> CompletionItem {
        // Include keywords in filter_text so fuzzy matching can find snippets by keywords
        let filter_text = format!("{} {}", snippet.label, snippet.keywords.join(" "));

        CompletionItem {
            label: snippet.label.clone(),
            kind: Some(CompletionItemKind::SNIPPET),
            detail: Some(snippet.description.clone()),
            insert_text: Some(snippet.template.clone()),
            insert_text_format: Some(InsertTextFormat::SNIPPET),
            documentation: None,
            deprecated: Some(false),
            preselect: None,
            sort_text: Some(format!("z_{}", snippet.label)), // Lower priority than keywords
            filter_text: Some(filter_text),
            ..Default::default()
        }
    }
}

impl Default for SnippetProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_all_snippets() {
        let provider = SnippetProvider::new();
        let completions = provider.get_completions("");

        assert!(!completions.is_empty());
        assert!(completions
            .iter()
            .all(|c| c.kind == Some(CompletionItemKind::SNIPPET)));
    }

    #[test]
    fn test_filter_snippets_by_label() {
        let provider = SnippetProvider::new();
        let completions = provider.get_completions("select");

        assert!(!completions.is_empty());
        assert!(completions
            .iter()
            .all(|c| c.label.to_lowercase().contains("select")));
    }

    #[test]
    fn test_filter_snippets_by_keyword() {
        let provider = SnippetProvider::new();
        let completions = provider.get_completions("join");

        assert!(!completions.is_empty());
        assert!(completions.iter().any(|c| c.label.contains("join")));
    }

    #[test]
    fn test_snippet_format() {
        let provider = SnippetProvider::new();
        let completions = provider.get_completions("insert");

        assert!(!completions.is_empty());

        let snippet = &completions[0];
        assert!(snippet.insert_text.is_some());
        assert_eq!(snippet.insert_text_format, Some(InsertTextFormat::SNIPPET));
        assert!(snippet.insert_text.as_ref().unwrap().contains("${"));
    }

    #[test]
    fn test_snippet_details() {
        let provider = SnippetProvider::new();
        let completions = provider.get_completions("create-table");

        assert!(!completions.is_empty());

        let snippet = &completions[0];
        assert!(snippet.detail.is_some());
        assert!(!snippet.detail.as_ref().unwrap().is_empty());
    }
}
