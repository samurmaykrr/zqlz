use gpui::SharedString;

use crate::widgets::highlighter::{registry::LanguageRegistry, LanguageConfig};

#[cfg(not(feature = "tree-sitter-languages"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, enum_iterator::Sequence)]
pub enum Language {
    Json,
    Sql,
}

#[cfg(feature = "tree-sitter-languages")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, enum_iterator::Sequence)]
pub enum Language {
    Json,
    Plain,
    Bash,
    C,
    CMake,
    CSharp,
    Cpp,
    Css,
    Diff,
    Ejs,
    Elixir,
    Erb,
    Go,
    GraphQL,
    Html,
    Java,
    JavaScript,
    JsDoc,
    Make,
    Markdown,
    MarkdownInline,
    Proto,
    Python,
    Ruby,
    Rust,
    Scala,
    Sql,
    Swift,
    Toml,
    Tsx,
    TypeScript,
    Yaml,
    Zig,
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
        #[cfg(not(feature = "tree-sitter-languages"))]
        {
            match self {
                Self::Json => "json",
                Self::Sql => "sql",
            }
        }

        #[cfg(feature = "tree-sitter-languages")]
        match self {
            Self::Plain => "text",
            Self::Bash => "bash",
            Self::C => "c",
            Self::CMake => "cmake",
            Self::CSharp => "csharp",
            Self::Cpp => "cpp",
            Self::Css => "css",
            Self::Diff => "diff",
            Self::Ejs => "ejs",
            Self::Elixir => "elixir",
            Self::Erb => "erb",
            Self::Go => "go",
            Self::GraphQL => "graphql",
            Self::Html => "html",
            Self::Java => "java",
            Self::JavaScript => "javascript",
            Self::JsDoc => "jsdoc",
            Self::Json => "json",
            Self::Make => "make",
            Self::Markdown => "markdown",
            Self::MarkdownInline => "markdown_inline",
            Self::Proto => "proto",
            Self::Python => "python",
            Self::Ruby => "ruby",
            Self::Rust => "rust",
            Self::Scala => "scala",
            Self::Sql => "sql",
            Self::Swift => "swift",
            Self::Toml => "toml",
            Self::Tsx => "tsx",
            Self::TypeScript => "typescript",
            Self::Yaml => "yaml",
            Self::Zig => "zig",
        }
    }

    #[allow(unused)]
    pub fn from_str(s: &str) -> Self {
        #[cfg(not(feature = "tree-sitter-languages"))]
        {
            // Support SQL even without the feature flag
            match s {
                "sql" | "sequel" => Self::Sql,
                _ => Self::Json,
            }
        }

        #[cfg(feature = "tree-sitter-languages")]
        match s {
            "bash" | "sh" => Self::Bash,
            "c" => Self::C,
            "cmake" => Self::CMake,
            "cpp" | "c++" => Self::Cpp,
            "csharp" | "cs" => Self::CSharp,
            "css" | "scss" => Self::Css,
            "diff" => Self::Diff,
            "ejs" => Self::Ejs,
            "elixir" | "ex" => Self::Elixir,
            "erb" => Self::Erb,
            "go" => Self::Go,
            "graphql" => Self::GraphQL,
            "html" => Self::Html,
            "java" => Self::Java,
            "javascript" | "js" => Self::JavaScript,
            "jsdoc" => Self::JsDoc,
            "json" | "jsonc" => Self::Json,
            "make" | "makefile" => Self::Make,
            "markdown" | "md" | "mdx" => Self::Markdown,
            "markdown_inline" | "markdown-inline" => Self::MarkdownInline,
            "proto" | "protobuf" => Self::Proto,
            "python" | "py" => Self::Python,
            "ruby" | "rb" => Self::Ruby,
            "rust" | "rs" => Self::Rust,
            "scala" => Self::Scala,
            "sql" => Self::Sql,
            "swift" => Self::Swift,
            "toml" => Self::Toml,
            "tsx" => Self::Tsx,
            "typescript" | "ts" => Self::TypeScript,
            "yaml" | "yml" => Self::Yaml,
            "zig" => Self::Zig,
            _ => Self::Plain,
        }
    }

    #[allow(unused)]
    pub(super) fn injection_languages(&self) -> Vec<SharedString> {
        #[cfg(not(feature = "tree-sitter-languages"))]
        return vec![];

        #[cfg(feature = "tree-sitter-languages")]
        match self {
            Self::Markdown => vec!["markdown-inline", "html", "toml", "yaml"],
            Self::MarkdownInline => vec![],
            Self::Html => vec!["javascript", "css"],
            Self::Rust => vec!["rust"],
            Self::JavaScript | Self::TypeScript => vec![
                "jsdoc",
                "json",
                "css",
                "html",
                "sql",
                "typescript",
                "javascript",
                "tsx",
                "yaml",
                "graphql",
            ],
            _ => vec![],
        }
        .into_iter()
        .map(|s| s.into())
        .collect()
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
            // When the `tree-sitter-languages` feature is enabled the enum gains many
            // more variants (Bash, Python, Rust, …). Those fall through to this arm.
            // Without the feature the enum is exhausted by the arms above, so this
            // arm would be unreachable — hence the cfg guard.
            #[cfg(feature = "tree-sitter-languages")]
            _ => (tree_sitter_json::LANGUAGE, "", "", ""),
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
    #[cfg(feature = "tree-sitter-languages")]
    fn test_language_name() {
        use super::*;

        assert_eq!(Language::MarkdownInline.name(), "markdown_inline");
        assert_eq!(Language::Markdown.name(), "markdown");
        assert_eq!(Language::Json.name(), "json");
        assert_eq!(Language::Yaml.name(), "yaml");
        assert_eq!(Language::Rust.name(), "rust");
        assert_eq!(Language::Go.name(), "go");
        assert_eq!(Language::C.name(), "c");
        assert_eq!(Language::Cpp.name(), "cpp");
        assert_eq!(Language::Sql.name(), "sql");
        assert_eq!(Language::JavaScript.name(), "javascript");
        assert_eq!(Language::Zig.name(), "zig");
        assert_eq!(Language::CSharp.name(), "csharp");
        assert_eq!(Language::TypeScript.name(), "typescript");
        assert_eq!(Language::Tsx.name(), "tsx");
        assert_eq!(Language::Diff.name(), "diff");
        assert_eq!(Language::Elixir.name(), "elixir");
        assert_eq!(Language::Erb.name(), "erb");
        assert_eq!(Language::Ejs.name(), "ejs");
    }
}
