//! Driver registry for managing available database drivers

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use zqlz_core::dialect_config::{CompletionsConfig, SyntaxQualityConfig, SyntaxTermsConfig};
use zqlz_core::{
    DataTypeCategory, DataTypeInfo, DatabaseDriver, DialectBundle, DialectInfo, DriverCapabilities,
    DriverCategory, KeywordInfo, SqlFunctionInfo, SyntaxDriverCapabilities,
    driver_category_from_driver_name, function_term_category, get_highlight_language,
    get_syntax_driver_capabilities, keyword_term_category, syntax_driver_capabilities_from_bundle,
    syntax_term_profile,
};

#[derive(Debug, Clone)]
pub struct DriverSyntaxMetadata {
    pub capabilities: SyntaxDriverCapabilities,
    pub syntax_terms: SyntaxTermsConfig,
    pub syntax_quality: SyntaxQualityConfig,
    pub completions: CompletionsConfig,
    pub completion_triggers: Vec<char>,
    pub completion_word_chars: Vec<char>,
}

pub const DEFAULT_COMPLETION_TRIGGERS: &[char] = &['.', ' ', '(', ','];
pub const DEFAULT_COMPLETION_WORD_CHARS: &[char] = &['$'];

/// Registry of available database drivers
pub struct DriverRegistry {
    drivers: HashMap<String, Arc<dyn DatabaseDriver>>,
}

impl DriverRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            drivers: HashMap::new(),
        }
    }

    /// Create a registry with all built-in drivers registered
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();

        // SQL Databases
        #[cfg(feature = "sqlite")]
        registry.register(Arc::new(crate::sqlite::SqliteDriver::new()));
        #[cfg(feature = "turso")]
        registry.register(Arc::new(crate::turso::TursoDriver::new()));
        #[cfg(feature = "postgres")]
        {
            let postgres_driver = Arc::new(crate::postgres::PostgresDriver::new());
            registry.register(postgres_driver.clone());
            registry.register_alias("postgresql", postgres_driver);
        }
        #[cfg(feature = "mysql")]
        {
            let mysql_driver = Arc::new(crate::mysql::MySqlDriver::new());
            registry.register(mysql_driver.clone());
            registry.register_alias("mariadb", mysql_driver);
        }
        #[cfg(feature = "mssql")]
        {
            let mssql_driver = Arc::new(crate::mssql::MssqlDriver::new());
            registry.register(mssql_driver.clone());
            registry.register_alias("sqlserver", mssql_driver);
        }
        #[cfg(feature = "duckdb")]
        registry.register(Arc::new(crate::duckdb::DuckDbDriver::new()));

        // NoSQL Databases
        #[cfg(feature = "redis")]
        registry.register(Arc::new(crate::redis::RedisDriver::new()));
        #[cfg(feature = "mongodb")]
        {
            let mongodb_driver = Arc::new(crate::mongodb::MongoDbDriver::new());
            registry.register(mongodb_driver.clone());
            registry.register_alias("mongo", mongodb_driver);
        }
        #[cfg(feature = "clickhouse")]
        registry.register(Arc::new(crate::clickhouse::ClickHouseDriver::new()));

        registry
    }

    /// Register a new driver
    pub fn register(&mut self, driver: Arc<dyn DatabaseDriver>) {
        let name = lookup_key(driver.name());
        tracing::info!(driver = %name, "registering database driver");
        self.drivers.insert(name, driver);
    }

    fn register_alias(&mut self, alias: &str, driver: Arc<dyn DatabaseDriver>) {
        let alias = lookup_key(alias);
        tracing::info!(
            alias,
            driver = driver.name(),
            "registering database driver alias"
        );
        self.drivers.insert(alias, driver);
    }

    /// Get a driver by name
    pub fn get(&self, name: &str) -> Option<Arc<dyn DatabaseDriver>> {
        let lookup_key = lookup_key(name);
        let driver = self.drivers.get(&lookup_key).cloned();
        if driver.is_none() {
            tracing::warn!(driver = %name, "driver not found in registry");
        }
        driver
    }

    /// List all registered driver names
    pub fn list(&self) -> Vec<&str> {
        self.drivers.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a driver is registered
    pub fn has(&self, name: &str) -> bool {
        self.drivers.contains_key(&lookup_key(name))
    }

    /// Get dialect info for a driver by name
    pub fn dialect_info(&self, name: &str) -> Option<DialectInfo> {
        self.drivers
            .get(&lookup_key(name))
            .map(|driver| driver.dialect_info())
    }

    /// Get dialect bundle for a driver by name
    pub fn dialect_bundle(&self, name: &str) -> Option<&'static DialectBundle> {
        self.drivers
            .get(&lookup_key(name))
            .and_then(|driver| driver.dialect_bundle())
    }
}

fn lookup_key(name: &str) -> String {
    name.trim().to_lowercase()
}

fn default_registry() -> &'static DriverRegistry {
    static REGISTRY: OnceLock<DriverRegistry> = OnceLock::new();
    REGISTRY.get_or_init(DriverRegistry::with_defaults)
}

/// Get dialect info for a driver by name without needing a registry instance.
/// This is a convenience function that creates a temporary driver instance.
/// For repeated lookups, prefer using a cached `DriverRegistry` instance.
pub fn get_dialect_info(driver_name: &str) -> DialectInfo {
    if let Some(bundle) = get_dialect_bundle(driver_name) {
        return bundle.into();
    }

    match lookup_key(driver_name).as_str() {
        #[cfg(feature = "sqlite")]
        "sqlite" => crate::sqlite::sqlite_dialect(),
        #[cfg(feature = "turso")]
        "turso" => crate::sqlite::sqlite_dialect(),
        #[cfg(feature = "postgres")]
        "postgres" | "postgresql" => crate::postgres::postgres_dialect(),
        #[cfg(feature = "mysql")]
        "mysql" | "mariadb" => crate::mysql::mysql_dialect(),
        #[cfg(feature = "mssql")]
        "mssql" | "sqlserver" => crate::mssql::mssql_dialect(),
        #[cfg(feature = "duckdb")]
        "duckdb" => crate::duckdb::duckdb_dialect(),
        #[cfg(feature = "mongodb")]
        "mongodb" | "mongo" => crate::mongodb::mongodb_dialect(),
        #[cfg(feature = "clickhouse")]
        "clickhouse" => crate::clickhouse::clickhouse_dialect(),
        "generic" | "sql" => generic_sql_dialect_info(),
        _ => DialectInfo::default(),
    }
}

fn generic_sql_dialect_info() -> DialectInfo {
    let terms = syntax_term_profile("sql");
    DialectInfo {
        keywords: terms
            .base_keywords
            .iter()
            .chain(terms.dialect_keywords)
            .copied()
            .map(|keyword| KeywordInfo::new(keyword, keyword_term_category(keyword)))
            .collect(),
        functions: terms
            .base_functions
            .iter()
            .chain(terms.dialect_functions)
            .copied()
            .map(|function| SqlFunctionInfo::new(function, function_term_category(function)))
            .collect(),
        data_types: terms
            .base_types
            .iter()
            .chain(terms.dialect_types)
            .copied()
            .map(|data_type| DataTypeInfo::new(data_type, DataTypeCategory::Other))
            .collect(),
        ..Default::default()
    }
}

pub fn get_driver_capabilities(driver_name: &str) -> Option<DriverCapabilities> {
    default_registry()
        .get(driver_name)
        .map(|driver| driver.capabilities())
}

/// Get the dialect bundle for a driver by name.
///
/// Returns the full DialectBundle containing the config, completions, and highlights.
/// This is available for drivers that have migrated to the declarative dialect system.
///
/// Returns None for drivers that still use the legacy hardcoded dialect functions.
pub fn get_dialect_bundle(driver_name: &str) -> Option<&'static DialectBundle> {
    default_registry().dialect_bundle(driver_name)
}

pub fn get_highlight_language_for_driver(driver_name: &str) -> &'static str {
    get_syntax_metadata_for_driver(driver_name)
        .map(|metadata| metadata.capabilities.profile)
        .unwrap_or_else(|| get_highlight_language(driver_name))
}

pub fn get_syntax_metadata_for_driver(driver_name: &str) -> Option<DriverSyntaxMetadata> {
    get_dialect_bundle(driver_name).map(|bundle| DriverSyntaxMetadata {
        capabilities: syntax_driver_capabilities_from_bundle(bundle),
        syntax_terms: bundle.config.syntax_terms.clone(),
        syntax_quality: bundle.config.syntax_quality.clone(),
        completions: bundle.completions.clone(),
        completion_triggers: chars_from_config(
            &bundle.config.syntax_highlighting.completion_triggers,
        ),
        completion_word_chars: chars_from_config(
            &bundle.config.syntax_highlighting.completion_word_chars,
        ),
    })
}

pub fn get_completion_triggers_for_driver(driver_name: Option<&str>) -> Vec<char> {
    driver_name
        .and_then(get_syntax_metadata_for_driver)
        .map(|metadata| metadata.completion_triggers)
        .filter(|triggers| !triggers.is_empty())
        .unwrap_or_else(|| DEFAULT_COMPLETION_TRIGGERS.to_vec())
}

pub fn get_completion_word_chars_for_driver(driver_name: Option<&str>) -> Vec<char> {
    driver_name
        .and_then(get_syntax_metadata_for_driver)
        .map(|metadata| metadata.completion_word_chars)
        .filter(|word_chars| !word_chars.is_empty())
        .unwrap_or_else(|| DEFAULT_COMPLETION_WORD_CHARS.to_vec())
}

pub fn get_syntax_capabilities_for_driver(driver_name: &str) -> SyntaxDriverCapabilities {
    get_syntax_metadata_for_driver(driver_name)
        .map(|metadata| metadata.capabilities)
        .unwrap_or_else(|| get_syntax_driver_capabilities(driver_name))
}

pub fn supports_sql_lsp_for_driver(driver_name: Option<&str>) -> bool {
    let Some(driver_name) = driver_name else {
        return true;
    };

    if let Some(metadata) = get_syntax_metadata_for_driver(driver_name) {
        let capabilities = metadata.capabilities;
        return !capabilities.command_syntax && !capabilities.document_syntax;
    }

    matches!(
        driver_category_from_driver_name(driver_name),
        DriverCategory::Relational
    )
}

fn chars_from_config(values: &[String]) -> Vec<char> {
    values
        .iter()
        .filter_map(|trigger| {
            let mut chars = trigger.chars();
            let character = chars.next()?;
            chars.next().is_none().then_some(character)
        })
        .collect()
}

impl Default for DriverRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::DriverRegistry;
    use zqlz_core::dialect_config::{GrammarType, SyntaxGrammarName, SyntaxQualityTokenConfig};
    use zqlz_core::{BracketCapability, DialectBundle, syntax_driver_capabilities_from_bundle};

    #[cfg(feature = "mysql")]
    #[test]
    fn mariadb_resolves_to_mysql_driver() {
        let registry = DriverRegistry::with_defaults();
        let driver = registry
            .get("mariadb")
            .expect("mariadb should resolve through mysql driver alias");

        assert_eq!(driver.name(), "mysql");
        assert!(registry.has("mariadb"));
        assert!(registry.dialect_info("mariadb").is_some());
    }

    #[test]
    fn registered_drivers_expose_dialect_bundles() {
        let registry = DriverRegistry::with_defaults();
        for driver_name in registry.list() {
            assert!(
                registry.dialect_bundle(driver_name).is_some(),
                "{driver_name} should expose a dialect bundle"
            );
        }
    }

    #[test]
    fn driver_bundles_expose_syntax_quality_fixtures() {
        let registry = DriverRegistry::with_defaults();
        for driver_name in registry.list() {
            let bundle = registry
                .dialect_bundle(driver_name)
                .expect("registered driver should expose dialect bundle");
            let syntax_quality = &bundle.config.syntax_quality;

            assert!(
                syntax_quality.text.is_some(),
                "{driver_name} should expose driver-owned syntax quality fixture"
            );
            assert!(
                !syntax_quality.expected.is_empty(),
                "{driver_name} should expose expected syntax quality assignments"
            );
            assert!(
                !syntax_quality.rejected.is_empty(),
                "{driver_name} should expose rejected syntax quality assignments"
            );
            assert_syntax_quality_fixture_is_well_formed(driver_name, bundle);
            assert_syntax_quality_fixture_covers_driver_shape(driver_name, bundle);
            assert_driver_grammar_config_is_consistent(driver_name, bundle);
        }
    }

    #[test]
    fn driver_bundles_expose_completion_triggers() {
        let registry = DriverRegistry::with_defaults();
        for driver_name in registry.list() {
            let metadata =
                super::get_syntax_metadata_for_driver(driver_name).expect("syntax metadata");
            assert!(
                !metadata.completion_triggers.is_empty(),
                "{driver_name} should expose driver-owned completion triggers"
            );
            assert!(
                !metadata.completion_word_chars.is_empty(),
                "{driver_name} should expose driver-owned completion word chars"
            );
            let bundle = registry
                .dialect_bundle(driver_name)
                .expect("registered driver should expose dialect bundle");
            assert_single_char_config_values(
                driver_name,
                "completion_triggers",
                &bundle.config.syntax_highlighting.completion_triggers,
            );
            assert_single_char_config_values(
                driver_name,
                "completion_word_chars",
                &bundle.config.syntax_highlighting.completion_word_chars,
            );
        }
    }

    #[test]
    fn sql_lsp_support_comes_from_driver_syntax_capabilities() {
        assert!(super::supports_sql_lsp_for_driver(None));
        assert!(super::supports_sql_lsp_for_driver(Some("postgres")));
        assert!(super::supports_sql_lsp_for_driver(Some("unknown")));
        assert!(!super::supports_sql_lsp_for_driver(Some("redis")));
        assert!(!super::supports_sql_lsp_for_driver(Some("mongodb")));
    }

    #[test]
    fn driver_bundles_own_indent_after_keywords() {
        let registry = DriverRegistry::with_defaults();
        for driver_name in registry.list() {
            let metadata =
                super::get_syntax_metadata_for_driver(driver_name).expect("syntax metadata");
            let bundle = registry
                .dialect_bundle(driver_name)
                .expect("registered driver should expose dialect bundle");

            assert_eq!(
                metadata.capabilities.indent_after_keywords,
                bundle
                    .config
                    .syntax_highlighting
                    .indent_after_keywords
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                "{driver_name} should expose driver-owned newline indent keywords"
            );
        }
    }

    #[test]
    fn driver_bundles_own_auto_close_pairs() {
        let registry = DriverRegistry::with_defaults();
        for driver_name in registry.list() {
            let metadata =
                super::get_syntax_metadata_for_driver(driver_name).expect("syntax metadata");
            let bundle = registry
                .dialect_bundle(driver_name)
                .expect("registered driver should expose dialect bundle");
            let expected_pairs = bundle
                .config
                .syntax_highlighting
                .auto_close_pairs
                .iter()
                .filter_map(|pair| {
                    let mut chars = pair.chars();
                    let opener = chars.next()?;
                    let closer = chars.next()?;
                    chars.next().is_none().then_some((opener, closer))
                })
                .collect::<Vec<_>>();

            assert_eq!(
                metadata.capabilities.auto_close_pairs, expected_pairs,
                "{driver_name} should expose driver-owned auto-close pairs"
            );
        }
    }

    #[test]
    fn completion_helpers_fall_back_to_sql_defaults_for_unknown_drivers() {
        assert_eq!(
            super::get_completion_triggers_for_driver(None),
            super::DEFAULT_COMPLETION_TRIGGERS
        );
        assert_eq!(
            super::get_completion_triggers_for_driver(Some("unknown")),
            super::DEFAULT_COMPLETION_TRIGGERS
        );
        assert_eq!(
            super::get_completion_word_chars_for_driver(None),
            super::DEFAULT_COMPLETION_WORD_CHARS
        );
        assert_eq!(
            super::get_completion_word_chars_for_driver(Some("unknown")),
            super::DEFAULT_COMPLETION_WORD_CHARS
        );
    }

    #[test]
    fn driver_bundles_own_bracket_capabilities() {
        let registry = DriverRegistry::with_defaults();
        for driver_name in registry.list() {
            let bundle = registry
                .dialect_bundle(driver_name)
                .expect("registered driver should expose dialect bundle");
            let metadata =
                super::get_syntax_metadata_for_driver(driver_name).expect("syntax metadata");
            let capabilities = syntax_driver_capabilities_from_bundle(bundle);

            assert_eq!(
                metadata.capabilities.brackets, capabilities.brackets,
                "{driver_name} should expose bracket behavior from its driver bundle"
            );
        }

        #[cfg(feature = "redis")]
        assert_eq!(
            super::get_syntax_metadata_for_driver("redis")
                .expect("redis metadata")
                .capabilities
                .brackets,
            BracketCapability::Standard
        );

        #[cfg(feature = "mongodb")]
        assert_eq!(
            super::get_syntax_metadata_for_driver("mongodb")
                .expect("mongodb metadata")
                .capabilities
                .brackets,
            BracketCapability::TreeSitter
        );
    }

    #[test]
    fn driver_bundles_own_line_comment_prefixes() {
        let registry = DriverRegistry::with_defaults();
        for driver_name in registry.list() {
            let bundle = registry
                .dialect_bundle(driver_name)
                .expect("registered driver should expose dialect bundle");
            let metadata =
                super::get_syntax_metadata_for_driver(driver_name).expect("syntax metadata");

            assert_eq!(
                metadata.capabilities.line_comment_prefix,
                bundle.config.comments.line_comment.as_deref(),
                "{driver_name} should expose line comments from its driver bundle"
            );
        }

        #[cfg(feature = "mongodb")]
        assert_eq!(
            super::get_syntax_metadata_for_driver("mongodb")
                .expect("mongodb metadata")
                .capabilities
                .line_comment_prefix,
            Some("//")
        );

        #[cfg(feature = "postgres")]
        assert_eq!(
            super::get_syntax_metadata_for_driver("postgres")
                .expect("postgres metadata")
                .capabilities
                .line_comment_prefix,
            Some("--")
        );

        #[cfg(feature = "redis")]
        assert_eq!(
            super::get_syntax_metadata_for_driver("redis")
                .expect("redis metadata")
                .capabilities
                .line_comment_prefix,
            Some("#")
        );
    }

    fn assert_syntax_quality_fixture_is_well_formed(driver_name: &str, bundle: &DialectBundle) {
        let syntax_quality = &bundle.config.syntax_quality;
        let text = syntax_quality
            .text
            .as_deref()
            .expect("fixture presence checked by caller");
        let mut expected_assignments = HashSet::new();

        for token in &syntax_quality.expected {
            assert_quality_token(driver_name, text, "expected", token);
            expected_assignments.insert((token.token.as_str(), token.kind.as_str()));
        }

        for token in &syntax_quality.rejected {
            assert_quality_token(driver_name, text, "rejected", token);
            assert!(
                !expected_assignments.contains(&(token.token.as_str(), token.kind.as_str())),
                "{driver_name} syntax fixture rejects same token/kind it expects: {:?}",
                token
            );
        }
    }

    fn assert_syntax_quality_fixture_covers_driver_shape(
        driver_name: &str,
        bundle: &'static DialectBundle,
    ) {
        let expected_kinds = bundle
            .config
            .syntax_quality
            .expected
            .iter()
            .map(|token| token.kind.as_str())
            .collect::<HashSet<_>>();
        let capabilities = syntax_driver_capabilities_from_bundle(bundle);

        if capabilities.command_syntax {
            assert_expected_kinds(
                driver_name,
                &expected_kinds,
                &["Keyword", "String", "Comment"],
            );
            return;
        }

        if capabilities.document_syntax {
            assert_expected_kinds(
                driver_name,
                &expected_kinds,
                &["Identifier", "Function", "Operator", "String"],
            );
            return;
        }

        assert_expected_kinds(
            driver_name,
            &expected_kinds,
            &["Keyword", "Identifier", "Type", "Function"],
        );
    }

    fn assert_expected_kinds(
        driver_name: &str,
        expected_kinds: &HashSet<&str>,
        required_kinds: &[&str],
    ) {
        for required_kind in required_kinds {
            assert!(
                expected_kinds.contains(required_kind),
                "{driver_name} syntax fixture should cover expected {required_kind} assignments"
            );
        }
    }

    fn assert_driver_grammar_config_is_consistent(driver_name: &str, bundle: &DialectBundle) {
        let syntax_grammar = bundle.config.syntax_highlighting.grammar;
        match syntax_grammar {
            SyntaxGrammarName::None => {
                assert_eq!(
                    bundle.config.grammar.grammar_type,
                    GrammarType::None,
                    "{driver_name} should not declare legacy grammar when syntax grammar is none"
                );
                assert!(
                    bundle.config.grammar.name.is_none(),
                    "{driver_name} grammar name should be absent when syntax grammar is none"
                );
            }
            SyntaxGrammarName::Sql => assert_tree_sitter_grammar_name(driver_name, bundle, "sql"),
            SyntaxGrammarName::Javascript => {
                assert_tree_sitter_grammar_name(driver_name, bundle, "javascript")
            }
        }
    }

    fn assert_tree_sitter_grammar_name(
        driver_name: &str,
        bundle: &DialectBundle,
        expected_name: &str,
    ) {
        assert_eq!(
            bundle.config.grammar.grammar_type,
            GrammarType::TreeSitter,
            "{driver_name} should declare tree-sitter grammar ownership"
        );
        assert_eq!(
            bundle.config.grammar.name.as_deref(),
            Some(expected_name),
            "{driver_name} grammar name should match syntax_highlighting.grammar"
        );
    }

    fn assert_quality_token(
        driver_name: &str,
        text: &str,
        group: &str,
        token: &SyntaxQualityTokenConfig,
    ) {
        assert!(
            !token.token.is_empty(),
            "{driver_name} syntax fixture {group} token must not be empty"
        );
        assert!(
            text.contains(&token.token),
            "{driver_name} syntax fixture {group} token {:?} is not present in fixture text",
            token.token
        );
        assert!(
            is_known_highlight_kind(&token.kind),
            "{driver_name} syntax fixture {group} token {:?} uses unknown highlight kind {:?}",
            token.token,
            token.kind
        );
    }

    fn is_known_highlight_kind(kind: &str) -> bool {
        matches!(
            kind,
            "Keyword"
                | "Function"
                | "String"
                | "Comment"
                | "Number"
                | "Operator"
                | "Identifier"
                | "Parameter"
                | "Type"
                | "Punctuation"
                | "Boolean"
                | "Null"
                | "Error"
                | "Default"
        )
    }

    fn assert_single_char_config_values(driver_name: &str, field: &str, values: &[String]) {
        assert!(
            values.iter().all(|value| value.chars().count() == 1),
            "{driver_name} {field} entries should be exactly one character"
        );
        let unique = values.iter().collect::<HashSet<_>>();
        assert_eq!(
            unique.len(),
            values.len(),
            "{driver_name} {field} should not contain duplicate entries"
        );
    }

    #[test]
    fn non_document_driver_bundles_expose_syntax_terms() {
        let registry = DriverRegistry::with_defaults();
        for driver_name in registry.list() {
            let bundle = registry
                .dialect_bundle(driver_name)
                .expect("registered driver should expose dialect bundle");
            let capabilities = syntax_driver_capabilities_from_bundle(bundle);

            if capabilities.document_syntax {
                continue;
            }

            assert!(
                !bundle.config.syntax_terms.keywords.is_empty()
                    || !bundle.config.syntax_terms.functions.is_empty()
                    || !bundle.config.syntax_terms.types.is_empty(),
                "{driver_name} should expose driver-owned syntax terms"
            );
        }
    }

    #[test]
    fn driver_specific_syntax_terms_cover_known_extensions() {
        #[cfg(feature = "redis")]
        {
            let redis = super::get_dialect_bundle("redis").expect("redis bundle");
            assert!(
                redis
                    .config
                    .syntax_terms
                    .keywords
                    .iter()
                    .any(|keyword| keyword == "JSON.SET")
            );
        }

        #[cfg(feature = "mssql")]
        {
            let mssql = super::get_dialect_bundle("sqlserver").expect("mssql bundle");
            assert!(
                mssql
                    .config
                    .syntax_terms
                    .keywords
                    .iter()
                    .any(|keyword| keyword == "NOLOCK")
            );
            assert!(
                mssql
                    .config
                    .syntax_terms
                    .keywords
                    .iter()
                    .any(|keyword| keyword == "ONLY")
            );
        }
    }

    #[test]
    fn get_dialect_bundle_resolves_driver_aliases() {
        let aliases = [
            #[cfg(feature = "postgres")]
            "postgresql",
            #[cfg(feature = "mysql")]
            "mariadb",
            #[cfg(feature = "mssql")]
            "sqlserver",
            #[cfg(feature = "mongodb")]
            "mongo",
            #[cfg(feature = "turso")]
            "turso",
        ];

        for alias in aliases {
            assert!(
                super::get_dialect_bundle(alias).is_some(),
                "{alias} should resolve a dialect bundle"
            );
        }
    }

    #[test]
    fn registry_lookup_is_case_and_whitespace_insensitive() {
        let registry = DriverRegistry::with_defaults();

        #[cfg(feature = "postgres")]
        assert!(registry.dialect_bundle(" PostgreSQL ").is_some());
        #[cfg(feature = "mysql")]
        assert!(registry.get(" MariaDB ").is_some());
        #[cfg(feature = "mssql")]
        assert!(registry.has(" SQLServer "));
        #[cfg(feature = "mongodb")]
        assert!(super::get_dialect_bundle(" Mongo ").is_some());
    }

    #[test]
    fn get_dialect_info_prefers_driver_bundle() {
        let redis_info = super::get_dialect_info(" Redis ");

        assert!(
            redis_info
                .keywords
                .iter()
                .any(|keyword| keyword.keyword.eq_ignore_ascii_case("SCAN")),
            "bundle-derived redis dialect info should include Redis commands"
        );
    }

    #[test]
    #[cfg(feature = "mssql")]
    fn get_dialect_info_exposes_bundle_syntax_terms() {
        let info = super::get_dialect_info("sqlserver");

        assert!(
            info.keywords
                .iter()
                .any(|keyword| keyword.keyword.eq_ignore_ascii_case("TOP")),
            "bundle syntax terms should feed LSP keyword metadata"
        );
        assert!(
            info.keywords
                .iter()
                .any(|keyword| keyword.keyword.eq_ignore_ascii_case("IDENTITY")),
            "bundle syntax terms should expose DDL keyword metadata"
        );
        assert!(
            info.data_types
                .iter()
                .any(|data_type| data_type.name.eq_ignore_ascii_case("UNIQUEIDENTIFIER")),
            "bundle syntax terms should feed LSP type metadata"
        );
    }

    #[test]
    #[cfg(all(feature = "mysql", feature = "mssql"))]
    fn get_driver_capabilities_resolves_driver_and_alias_caps() {
        let mysql = super::get_driver_capabilities(" mysql ")
            .expect("mysql capabilities should resolve from registered driver");
        assert!(!mysql.supports_returning);
        assert!(mysql.supports_upsert);
        assert!(mysql.supports_cte);
        assert!(mysql.supports_window_functions);

        let sqlserver = super::get_driver_capabilities(" SQLServer ")
            .expect("sqlserver alias should resolve driver capabilities");
        assert!(sqlserver.supports_returning);
        assert!(sqlserver.supports_upsert);
        assert!(sqlserver.supports_cte);
        assert!(sqlserver.supports_window_functions);
    }

    #[test]
    fn get_highlight_language_for_driver_resolves_bundles_and_aliases() {
        #[cfg(feature = "postgres")]
        assert_eq!(
            super::get_highlight_language_for_driver(" PostgreSQL "),
            "postgresql"
        );
        #[cfg(feature = "mysql")]
        assert_eq!(super::get_highlight_language_for_driver("mariadb"), "mysql");
        #[cfg(feature = "mssql")]
        assert_eq!(
            super::get_highlight_language_for_driver("sqlserver"),
            "mssql"
        );
        #[cfg(feature = "mongodb")]
        assert_eq!(
            super::get_highlight_language_for_driver(" Mongo "),
            "mongodb"
        );
        assert_eq!(super::get_highlight_language_for_driver("unknown"), "sql");
    }

    #[test]
    fn get_syntax_metadata_for_driver_returns_capabilities_and_terms() {
        #[cfg(feature = "postgres")]
        {
            let metadata =
                super::get_syntax_metadata_for_driver(" PostgreSQL ").expect("postgres metadata");
            assert_eq!(metadata.capabilities.profile, "postgresql");
            assert!(metadata.capabilities.sql_overlays);
            assert!(
                metadata
                    .syntax_terms
                    .types
                    .iter()
                    .any(|term| term == "JSONB")
            );
        }

        #[cfg(feature = "redis")]
        {
            let metadata = super::get_syntax_metadata_for_driver("redis").expect("redis metadata");
            assert_eq!(metadata.capabilities.profile, "redis");
            assert!(metadata.capabilities.command_syntax);
            assert!(
                metadata
                    .syntax_terms
                    .keywords
                    .iter()
                    .any(|term| term == "JSON.SET")
            );
        }

        #[cfg(feature = "mongodb")]
        {
            let metadata = super::get_syntax_metadata_for_driver("mongo").expect("mongo metadata");
            assert_eq!(metadata.capabilities.profile, "mongodb");
            assert!(metadata.capabilities.document_syntax);
            assert!(
                metadata
                    .syntax_terms
                    .functions
                    .iter()
                    .any(|term| term == "aggregate")
            );
        }

        assert!(super::get_syntax_metadata_for_driver("unknown").is_none());
    }
}
