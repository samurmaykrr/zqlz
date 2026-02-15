//! SQL Diagnostics and Linting with precise error positioning

use crate::{SchemaCache, SchemaValidator, ValidationSeverity};
use lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::parser::Parser;
use tree_sitter::{Parser as TreeSitterParser, Query, QueryCursor};
use zqlz_core::DialectConfig;
use zqlz_ui::widgets::{Rope, RopeExt};

pub struct SqlDiagnostics {
    ts_parser: Option<TreeSitterParser>,
    schema_validator: SchemaValidator,
}

impl SqlDiagnostics {
    pub fn new() -> Self {
        let mut ts_parser = TreeSitterParser::new();
        let _ = ts_parser.set_language(&tree_sitter_sequel::LANGUAGE.into());

        Self {
            ts_parser: Some(ts_parser),
            schema_validator: SchemaValidator::new(),
        }
    }

    /// Parse SQL and return diagnostics for syntax errors
    ///
    /// This is the legacy method that assumes SQL syntax.
    /// Prefer using `analyze_with_dialect` for dialect-aware analysis.
    pub fn analyze(&mut self, text: &Rope, schema_cache: Option<&SchemaCache>) -> Vec<Diagnostic> {
        self.analyze_with_dialect(text, schema_cache, None)
    }

    /// Parse text and return diagnostics for syntax errors, respecting dialect configuration
    ///
    /// When a `DialectConfig` is provided:
    /// - For non-SQL languages (Command, Document), SQL parser validation is skipped
    /// - Tree-sitter errors are skipped for dialects without tree-sitter grammars
    /// - Schema validation is skipped for non-SQL dialects
    ///
    /// This allows Redis commands like `SET x y` to not show SQL syntax errors.
    pub fn analyze_with_dialect(
        &mut self,
        text: &Rope,
        schema_cache: Option<&SchemaCache>,
        dialect_config: Option<&DialectConfig>,
    ) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let sql = text.to_string();

        // Check if we should skip SQL validation
        let skip_sql = dialect_config
            .map(|c| c.skip_sql_validation())
            .unwrap_or(false);
        let skip_tree_sitter = dialect_config
            .map(|c| c.skip_tree_sitter_errors())
            .unwrap_or(false);

        // Use sqlparser for syntax validation with error location (SQL dialects only)
        if !skip_sql {
            diagnostics.extend(self.check_sqlparser_syntax(&sql, text));
        }

        // Use tree-sitter for more detailed error detection (if grammar exists)
        if !skip_tree_sitter {
            diagnostics.extend(self.check_tree_sitter_errors(&sql, text));
        }

        // Schema-aware validation (if schema is available and dialect is SQL)
        if !skip_sql {
            if let Some(schema) = schema_cache {
                diagnostics.extend(self.check_schema_validation(&sql, schema));
            }

            // Additional heuristic checks (SQL-specific)
            diagnostics.extend(self.check_common_mistakes(&sql, text));
        } else if let Some(config) = dialect_config {
            // For non-SQL dialects with custom validators, run custom validation
            if config.parser.custom_validator {
                diagnostics.extend(self.check_custom_validation(&sql, text, config));
            }
        }

        diagnostics
    }

    /// Run custom validation for command-based dialects (e.g., Redis)
    ///
    /// Uses proper tokenization to handle:
    /// - Quoted strings with escapes: `SET "my key" "hello \"world\""`
    /// - Comments
    /// - Multi-line commands
    ///
    /// Validates against command specifications (arity, deprecation, etc.)
    fn check_custom_validation(
        &self,
        sql: &str,
        _text: &Rope,
        dialect_config: &zqlz_core::DialectConfig,
    ) -> Vec<Diagnostic> {
        // Only Redis has custom validation for now
        // Other command-based dialects would have their own validators
        if dialect_config.id == "redis" {
            use crate::redis_validator::RedisValidator;

            let validator = RedisValidator::new();
            let case_sensitive = dialect_config.syntax.case_sensitive;
            return validator.validate_to_diagnostics(sql, case_sensitive);
        }

        // Fallback: no validation for unknown dialects
        Vec::new()
    }

    /// Check SQL syntax using sqlparser and extract error positions
    fn check_sqlparser_syntax(&self, sql: &str, text: &Rope) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let dialect = GenericDialect {};

        match Parser::parse_sql(&dialect, sql) {
            Ok(_statements) => {
                // SQL is valid, no errors
            }
            Err(e) => {
                // Try to extract position from error message
                let error_msg = e.to_string();
                let (line, col) = self.extract_error_position(&error_msg);

                // Adjust position if we found valid coordinates
                let position = if line > 0 || col > 0 {
                    Position::new(line as u32, col as u32)
                } else {
                    Position::new(0, 0)
                };

                // Try to find the error token in the SQL text for better range
                let range = self.find_error_range(text, position, &error_msg);

                diagnostics.push(Diagnostic {
                    range,
                    severity: Some(DiagnosticSeverity::ERROR),
                    message: format!("SQL Syntax Error: {}", error_msg),
                    source: Some("sqlparser".to_string()),
                    ..Default::default()
                });
            }
        }

        diagnostics
    }

    /// Check for tree-sitter ERROR nodes with precise positioning
    fn check_tree_sitter_errors(&mut self, sql: &str, text: &Rope) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        let Some(ref mut parser) = self.ts_parser else {
            return diagnostics;
        };

        let Some(tree) = parser.parse(sql, None) else {
            return diagnostics;
        };

        // Walk the tree looking for ERROR nodes
        let mut cursor = tree.walk();
        self.find_error_nodes(&mut cursor, sql, text, &mut diagnostics);

        diagnostics
    }

    /// Recursively find ERROR nodes in the syntax tree
    fn find_error_nodes(
        &self,
        cursor: &mut tree_sitter::TreeCursor,
        source: &str,
        text: &Rope,
        diagnostics: &mut Vec<Diagnostic>,
    ) {
        let node = cursor.node();

        if node.kind() == "ERROR" {
            let start_byte = node.start_byte();
            let end_byte = node.end_byte();

            let start_pos = text.offset_to_position(start_byte);
            let end_pos = text.offset_to_position(end_byte);

            let error_text = &source[start_byte..end_byte];
            let preview = if error_text.len() > 30 {
                format!("{}...", &error_text[..30])
            } else {
                error_text.to_string()
            };

            diagnostics.push(Diagnostic {
                range: Range::new(
                    Position::new(start_pos.line as u32, start_pos.character as u32),
                    Position::new(end_pos.line as u32, end_pos.character as u32),
                ),
                severity: Some(DiagnosticSeverity::ERROR),
                message: format!("Syntax error near: '{}'", preview),
                source: Some("tree-sitter".to_string()),
                ..Default::default()
            });
        }

        // Recurse into children
        if cursor.goto_first_child() {
            loop {
                self.find_error_nodes(cursor, source, text, diagnostics);
                if !cursor.goto_next_sibling() {
                    break;
                }
            }
            cursor.goto_parent();
        }
    }

    /// Extract line and column from sqlparser error messages
    fn extract_error_position(&self, error_msg: &str) -> (usize, usize) {
        // sqlparser errors may contain "at line X, column Y" or "near 'token' at position X"
        // This is a basic extraction; enhance as needed

        // Try to extract "line X"
        if let Some(line_pos) = error_msg.find("line ") {
            let rest = &error_msg[line_pos + 5..];
            if let Some(line_end) = rest.find(&[',', ' ', '\n'][..]) {
                if let Ok(line) = rest[..line_end].parse::<usize>() {
                    // Found line number
                    return (line.saturating_sub(1), 0); // Line numbers are 1-based
                }
            }
        }

        (0, 0)
    }

    /// Find a better range for the error by looking for tokens near the position
    fn find_error_range(&self, text: &Rope, position: Position, error_msg: &str) -> Range {
        // Try to extract the problematic token from error message
        let token = self.extract_token_from_error(error_msg);

        if let Some(token_text) = token {
            // Search for the token near the error position
            let line_idx = position.line as usize;
            if let Some(line_content) = self.get_line(text, line_idx) {
                if let Some(token_pos) = line_content.find(&token_text) {
                    let start_pos = Position::new(position.line, token_pos as u32);
                    let end_pos =
                        Position::new(position.line, (token_pos + token_text.len()) as u32);
                    return Range::new(start_pos, end_pos);
                }
            }
        }

        // Fallback: use position or highlight whole line
        Range::new(
            position,
            Position::new(position.line, position.character + 1),
        )
    }

    /// Extract token from error message (e.g., "Expected 'FROM' but found 'WHERE'")
    fn extract_token_from_error(&self, error_msg: &str) -> Option<String> {
        // Look for patterns like "found 'token'" or "near 'token'"
        if let Some(found_pos) = error_msg.find("found '") {
            let rest = &error_msg[found_pos + 7..];
            if let Some(end_pos) = rest.find('\'') {
                return Some(rest[..end_pos].to_string());
            }
        }

        if let Some(near_pos) = error_msg.find("near '") {
            let rest = &error_msg[near_pos + 6..];
            if let Some(end_pos) = rest.find('\'') {
                return Some(rest[..end_pos].to_string());
            }
        }

        None
    }

    /// Get a line from the Rope
    fn get_line(&self, text: &Rope, line_idx: usize) -> Option<String> {
        if line_idx >= text.lines_len() {
            return None;
        }
        Some(text.slice_line(line_idx).to_string())
    }

    /// Check schema-aware validation (unknown tables, columns, etc.)
    fn check_schema_validation(&self, sql: &str, schema: &SchemaCache) -> Vec<Diagnostic> {
        let issues = self.schema_validator.validate(sql, schema);

        issues
            .into_iter()
            .map(|issue| {
                let severity = match issue.severity {
                    ValidationSeverity::Error => DiagnosticSeverity::ERROR,
                    ValidationSeverity::Warning => DiagnosticSeverity::WARNING,
                    ValidationSeverity::Info => DiagnosticSeverity::INFORMATION,
                };

                Diagnostic {
                    range: Range::new(
                        Position::new(issue.line as u32, issue.column as u32),
                        Position::new(issue.line as u32, (issue.column + 1) as u32),
                    ),
                    severity: Some(severity),
                    message: issue.message,
                    source: Some("schema".to_string()),
                    ..Default::default()
                }
            })
            .collect()
    }

    /// Check for common SQL mistakes and best practices
    fn check_common_mistakes(&self, sql: &str, text: &Rope) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let sql_lower = sql.to_lowercase();

        // Check for SELECT * with position
        if let Some(select_star_pos) = sql_lower.find("select *") {
            let pos = text.offset_to_position(select_star_pos);
            diagnostics.push(Diagnostic {
                range: Range::new(
                    Position::new(pos.line as u32, pos.character as u32),
                    Position::new(pos.line as u32, (pos.character + 8) as u32),
                ),
                severity: Some(DiagnosticSeverity::INFORMATION),
                message: "Consider specifying explicit column names instead of SELECT *"
                    .to_string(),
                source: Some("best-practices".to_string()),
                ..Default::default()
            });
        }

        // Check for UPDATE/DELETE without WHERE
        for keyword in ["update", "delete from"] {
            if let Some(keyword_pos) = sql_lower.find(keyword) {
                let rest_of_query = &sql_lower[keyword_pos..];
                if !rest_of_query.contains("where") {
                    let pos = text.offset_to_position(keyword_pos);
                    let end_character = (pos.character as usize + keyword.len()) as u32;
                    diagnostics.push(Diagnostic {
                        range: Range::new(
                            Position::new(pos.line as u32, pos.character as u32),
                            Position::new(pos.line as u32, end_character),
                        ),
                        severity: Some(DiagnosticSeverity::WARNING),
                        message: format!(
                            "{} without WHERE clause will affect all rows",
                            keyword.to_uppercase()
                        ),
                        source: Some("best-practices".to_string()),
                        ..Default::default()
                    });
                }
            }
        }

        // Check for potential SQL injection patterns
        if sql.contains("'; DROP") || sql.contains("\"; DROP") {
            if let Some(drop_pos) = sql.to_uppercase().find("DROP") {
                let pos = text.offset_to_position(drop_pos);
                diagnostics.push(Diagnostic {
                    range: Range::new(
                        Position::new(pos.line as u32, pos.character as u32),
                        Position::new(pos.line as u32, (pos.character + 4) as u32),
                    ),
                    severity: Some(DiagnosticSeverity::ERROR),
                    message: "Potential SQL injection pattern detected".to_string(),
                    source: Some("security".to_string()),
                    ..Default::default()
                });
            }
        }

        diagnostics
    }
}

impl Default for SqlDiagnostics {
    fn default() -> Self {
        Self::new()
    }
}
