use gpui::SharedString;

use crate::widgets::highlighter::{LanguageConfig, registry::LanguageRegistry};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, enum_iterator::Sequence)]
pub enum Language {
    Json,
    Sql,
}

impl From<Language> for SharedString {
    fn from(language: Language) -> Self {
        language.name().into()
    }
}

impl Language {
    pub fn all() -> impl Iterator<Item = Self> {
        enum_iterator::all::<Language>()
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Sql => "sql",
        }
    }

    #[allow(unused, clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "json" | "jsonc" => Self::Json,
            "sql" | "sequel" => Self::Sql,
            _ => Self::Json,
        }
    }

    #[allow(unused)]
    pub(super) fn injection_languages(&self) -> Vec<SharedString> {
        vec![]
    }

    /// Return the language info for the language.
    ///
    /// (language, query, injection, locals)
    pub(super) fn config(&self) -> LanguageConfig {
        let (language, query, injection, locals) = match self {
            Self::Json => (
                tree_sitter_json::LANGUAGE,
                include_str!("languages/json/highlights.scm"),
                "",
                "",
            ),
            Self::Sql => (
                tree_sitter_sequel::LANGUAGE,
                include_str!("languages/sql/highlights.scm"),
                "",
                "",
            ),
        };

        let language = tree_sitter::Language::new(language);

        // Include brackets.scm and folds.scm for SQL language, empty for others
        let brackets = match self {
            Self::Sql => include_str!("languages/sql/brackets.scm"),
            _ => "",
        };

        let folds = match self {
            Self::Sql => include_str!("languages/sql/folds.scm"),
            _ => "",
        };

        LanguageConfig::new(
            self.name(),
            language,
            self.injection_languages(),
            query,
            injection,
            locals,
            brackets,
            folds,
        )
    }
}

/// Registers SQL dialect languages (postgresql, mysql, sqlite) into the registry.
///
/// These share the same tree-sitter grammar as generic SQL but use
/// dialect-specific highlights.scm for better keyword categorization.
pub(super) fn register_sql_dialects(registry: &LanguageRegistry) {
    let grammar = tree_sitter::Language::new(tree_sitter_sequel::LANGUAGE);

    let dialects: &[(&str, &str, &str, &str)] = &[
        (
            "postgresql",
            include_str!("languages/postgresql/highlights.scm"),
            include_str!("languages/postgresql/brackets.scm"),
            include_str!("languages/postgresql/folds.scm"),
        ),
        (
            "mysql",
            include_str!("languages/mysql/highlights.scm"),
            include_str!("languages/mysql/brackets.scm"),
            include_str!("languages/mysql/folds.scm"),
        ),
        (
            "sqlite",
            include_str!("languages/sqlite/highlights.scm"),
            include_str!("languages/sqlite/brackets.scm"),
            include_str!("languages/sqlite/folds.scm"),
        ),
    ];

    for (name, highlights, brackets, folds) in dialects {
        registry.register(
            name,
            &LanguageConfig::new(
                *name,
                grammar.clone(),
                vec![],
                highlights,
                "",
                "",
                brackets,
                folds,
            ),
        );
    }

    // Register ClickHouse as SQL-based dialect
    let clickhouse_dialects: &[(&str, &str, &str, &str)] = &[(
        "clickhouse",
        include_str!("languages/clickhouse/highlights.scm"),
        include_str!("languages/clickhouse/brackets.scm"),
        include_str!("languages/clickhouse/folds.scm"),
    )];

    for (name, highlights, brackets, folds) in clickhouse_dialects {
        registry.register(
            name,
            &LanguageConfig::new(
                *name,
                grammar.clone(),
                vec![],
                highlights,
                "",
                "",
                brackets,
                folds,
            ),
        );
    }
}

/// Registers non-SQL dialects (Redis, MongoDB) into the registry.
///
/// These use JSON grammar as a base for structure with dialect-specific highlights.
pub(super) fn register_nosql_dialects(registry: &LanguageRegistry) {
    let json_grammar = tree_sitter::Language::new(tree_sitter_json::LANGUAGE);

    let dialects: &[(&str, &str, &str, &str)] = &[
        (
            "redis",
            include_str!("languages/redis/highlights.scm"),
            include_str!("languages/redis/brackets.scm"),
            include_str!("languages/redis/folds.scm"),
        ),
        (
            "mongodb",
            include_str!("languages/mongodb/highlights.scm"),
            include_str!("languages/mongodb/brackets.scm"),
            include_str!("languages/mongodb/folds.scm"),
        ),
    ];

    for (name, highlights, brackets, folds) in dialects {
        registry.register(
            name,
            &LanguageConfig::new(
                *name,
                json_grammar.clone(),
                vec![],
                highlights,
                "",
                "",
                brackets,
                folds,
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_language_name() {
        use super::*;

        assert_eq!(Language::Json.name(), "json");
        assert_eq!(Language::Sql.name(), "sql");
    }
}
