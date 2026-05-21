use crate::{SchemaCache, SqlDiagnostics, SqlDialect};
use lsp_types::Diagnostic;
use zqlz_drivers::get_dialect_bundle;
use zqlz_ui::widgets::Rope;

pub(crate) fn validate_sql(
    diagnostics: &mut SqlDiagnostics,
    text: &Rope,
    schema_cache: &SchemaCache,
    driver_type: &str,
    dialect: SqlDialect,
    dialect_name: &str,
) -> Vec<Diagnostic> {
    let dialect_config_owned = validation_dialect_config(driver_type, dialect, dialect_name);

    diagnostics.analyze_with_dialect(text, Some(schema_cache), dialect_config_owned.as_ref())
}

fn validation_dialect_config(
    driver_type: &str,
    dialect: SqlDialect,
    dialect_name: &str,
) -> Option<zqlz_core::DialectConfig> {
    get_dialect_bundle(driver_type)
        .map(|bundle| bundle.config.clone())
        .or_else(|| dialect.dialect_config().cloned())
        .or_else(|| {
            if dialect == SqlDialect::Generic {
                return None;
            }

            Some(zqlz_core::DialectConfig {
                id: driver_type.to_string(),
                display_name: dialect_name.to_string(),
                ..zqlz_core::DialectConfig::default()
            })
        })
}
