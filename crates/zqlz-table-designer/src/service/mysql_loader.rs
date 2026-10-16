//! MySQL table metadata the generic catalog does not carry.
//!
//! `MODIFY COLUMN` re-specifies a whole column, so anything the designer does
//! not load (ON UPDATE, expression defaults, column collations, prefix and
//! functional index parts, ...) would be silently dropped by the first edit.
//! This reads those facts straight from `information_schema` and overlays them
//! on the design the generic loader built. Every query is best-effort: a
//! failure keeps what the generic loader produced.

use zqlz_core::{Connection, Value};

use super::mysql_ddl::lit;
use crate::models::{CheckConstraintDesign, IndexDesign, TableDesign};

/// Read a catalog value as text; tolerates drivers that return text as bytes.
fn text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Null => None,
        Value::String(s) | Value::Decimal(s) => Some(s.clone()),
        Value::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        Value::Int8(v) => Some(v.to_string()),
        Value::Int16(v) => Some(v.to_string()),
        Value::Int32(v) => Some(v.to_string()),
        Value::Int64(v) => Some(v.to_string()),
        _ => None,
    }
}

fn number(value: Option<&Value>) -> Option<i64> {
    value
        .and_then(Value::as_i64)
        .or_else(|| text(value)?.trim().parse().ok())
}

/// `on update CURRENT_TIMESTAMP(6)` out of `EXTRA`.
pub(crate) fn on_update_from_extra(extra: &str) -> Option<String> {
    let lower = extra.to_ascii_lowercase();
    let at = lower.find("on update ")?;
    let expression = extra[at + "on update ".len()..].trim();
    (!expression.is_empty()).then(|| expression.to_string())
}

/// The `DEFAULT` text a designer user would type for a catalog default.
/// `COLUMN_DEFAULT` is the bare value for literals and the bare expression for
/// `DEFAULT_GENERATED` ones, so only `EXTRA` tells `'now'` from `now()`.
pub(crate) fn catalog_default(raw: &str, column_type: &str, extra: &str) -> String {
    if extra.to_ascii_lowercase().contains("default_generated") {
        let upper = raw.to_ascii_uppercase();
        let timestamp = ["CURRENT_TIMESTAMP", "NOW", "LOCALTIME", "LOCALTIMESTAMP"]
            .iter()
            .any(|name| upper.starts_with(name) && !upper[name.len()..].contains(|c: char| c.is_alphabetic()));
        return if timestamp || raw.starts_with('(') {
            raw.to_string()
        } else {
            format!("({raw})")
        };
    }
    let base = column_type
        .split(|c: char| c == '(' || c.is_whitespace())
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let unquoted = matches!(
        base.as_str(),
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "decimal"
            | "numeric" | "dec" | "fixed" | "float" | "double" | "real" | "year" | "bit"
            | "bool" | "boolean"
    );
    if unquoted {
        raw.to_string()
    } else {
        lit(raw)
    }
}

/// The designer default for one `information_schema.COLUMNS` row. MariaDB
/// already stores defaults as SQL text (`'draft'`, `current_timestamp()`) and
/// reports a missing default as the word `NULL`, so only MySQL's bare values
/// need quoting.
pub(crate) fn column_default(
    raw: Option<String>,
    column_type: &str,
    extra: &str,
    is_mariadb: bool,
) -> Option<String> {
    let extra_lower = extra.to_ascii_lowercase();
    if extra_lower.contains("virtual generated") || extra_lower.contains("stored generated") {
        return None;
    }
    let raw = raw?;
    if is_mariadb {
        return (!raw.eq_ignore_ascii_case("NULL")).then_some(raw);
    }
    Some(catalog_default(&raw, column_type, extra))
}

/// One `information_schema.STATISTICS` row.
pub(crate) struct StatisticsRow {
    pub index: String,
    pub column: Option<String>,
    pub expression: Option<String>,
    pub descending: bool,
    pub prefix: Option<i64>,
    pub index_type: String,
    pub unique: bool,
    pub comment: Option<String>,
}

/// Rows must arrive ordered by index, then position in the index.
pub(crate) fn indexes_from_statistics(rows: Vec<StatisticsRow>) -> Vec<IndexDesign> {
    let mut indexes: Vec<IndexDesign> = Vec::new();
    for row in rows {
        let special = matches!(row.index_type.as_str(), "FULLTEXT" | "SPATIAL");
        let part = match (&row.column, &row.expression) {
            (Some(column), _) => match row.prefix {
                Some(length) if !special => format!("{column}({length})"),
                _ => column.clone(),
            },
            (None, Some(expression)) => expression.clone(),
            (None, None) => continue,
        };
        if indexes.last().is_none_or(|index| index.name != row.index) {
            let mut index = IndexDesign::named(row.index.clone());
            index.is_unique = row.unique;
            index.is_primary = row.index == "PRIMARY";
            index.index_type = row.index_type.clone();
            index.comment = row.comment.clone();
            indexes.push(index);
        }
        if let Some(index) = indexes.last_mut() {
            index.columns.push(part);
            index.column_descending.push(row.descending);
        }
    }
    for index in &mut indexes {
        if !index.column_descending.iter().any(|d| *d) {
            index.column_descending.clear();
        }
    }
    indexes
}

/// Overlay MySQL-only metadata on a freshly loaded design.
pub(crate) async fn apply(
    connection: &dyn Connection,
    schema: Option<&str>,
    table: &str,
    design: &mut TableDesign,
) {
    let schema = design
        .schema
        .clone()
        .filter(|s| !s.is_empty())
        .or_else(|| schema.map(str::to_string));
    let (scope, params) = match &schema {
        Some(schema) => (
            "TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            vec![Value::String(schema.clone()), Value::String(table.to_string())],
        ),
        None => (
            "TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
            vec![Value::String(table.to_string())],
        ),
    };

    let is_mariadb = match connection.query("SELECT VERSION()", &[]).await {
        Ok(result) => result
            .rows
            .first()
            .and_then(|row| text(row.get(0)))
            .is_some_and(|version| version.to_ascii_lowercase().contains("mariadb")),
        Err(_) => false,
    };

    match connection
        .query(
            &format!(
                "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_DEFAULT, EXTRA,
                        CHARACTER_SET_NAME, COLLATION_NAME
                 FROM information_schema.COLUMNS WHERE {scope} ORDER BY ORDINAL_POSITION"
            ),
            &params,
        )
        .await
    {
        Ok(result) => {
            for row in &result.rows {
                let Some(name) = text(row.get(0)) else { continue };
                let Some(column) = design.columns.iter_mut().find(|c| c.name == name) else {
                    continue;
                };
                let column_type = text(row.get(1)).unwrap_or_default();
                let extra = text(row.get(3)).unwrap_or_default();
                column.default_value =
                    column_default(text(row.get(2)), &column_type, &extra, is_mariadb);
                column.on_update = on_update_from_extra(&extra);
                column.invisible = extra.to_ascii_lowercase().contains("invisible");
                column.is_auto_increment = extra.to_ascii_lowercase().contains("auto_increment");
                column.charset = text(row.get(4)).filter(|v| !v.is_empty());
                column.collation = text(row.get(5)).filter(|v| !v.is_empty());
            }
        }
        Err(error) => tracing::warn!(%error, table, "Failed to load MySQL column details"),
    }

    match connection
        .query(
            &format!(
                "SELECT INDEX_NAME, COLUMN_NAME, EXPRESSION, COLLATION, SUB_PART, INDEX_TYPE,
                        NON_UNIQUE, INDEX_COMMENT
                 FROM information_schema.STATISTICS WHERE {scope}
                 ORDER BY INDEX_NAME = 'PRIMARY' DESC, INDEX_NAME, SEQ_IN_INDEX"
            ),
            &params,
        )
        .await
    {
        Ok(result) => {
            let rows = result
                .rows
                .iter()
                .filter_map(|row| {
                    Some(StatisticsRow {
                        index: text(row.get(0))?,
                        column: text(row.get(1)),
                        expression: text(row.get(2)),
                        descending: text(row.get(3)).is_some_and(|c| c.eq_ignore_ascii_case("D")),
                        prefix: number(row.get(4)),
                        index_type: text(row.get(5)).unwrap_or_default().to_ascii_uppercase(),
                        unique: number(row.get(6)) == Some(0),
                        comment: text(row.get(7)).filter(|c| !c.is_empty()),
                    })
                })
                .collect();
            design.indexes = indexes_from_statistics(rows);
            sync_key_flags(design);
        }
        Err(error) => tracing::warn!(%error, table, "Failed to load MySQL indexes"),
    }

    match connection
        .query(
            &format!(
                "SELECT ENGINE, TABLE_COLLATION,
                        (SELECT CHARACTER_SET_NAME
                           FROM information_schema.COLLATION_CHARACTER_SET_APPLICABILITY c
                          WHERE c.COLLATION_NAME = t.TABLE_COLLATION),
                        TABLE_COMMENT
                 FROM information_schema.TABLES t WHERE {scope}"
            ),
            &params,
        )
        .await
    {
        Ok(result) => {
            if let Some(row) = result.rows.first() {
                design.options.engine = text(row.get(0)).filter(|v| !v.is_empty());
                design.options.collation = text(row.get(1)).filter(|v| !v.is_empty());
                design.options.charset = text(row.get(2)).filter(|v| !v.is_empty());
                design.comment = text(row.get(3)).filter(|v| !v.is_empty());
            }
        }
        Err(error) => tracing::warn!(%error, table, "Failed to load MySQL table options"),
    }

    // Unlike the generic catalog, keep every check, whatever its name.
    match connection
        .query(
            "SELECT cc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
             FROM information_schema.CHECK_CONSTRAINTS cc
             JOIN information_schema.TABLE_CONSTRAINTS tc
               ON tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
              AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
             WHERE tc.CONSTRAINT_TYPE = 'CHECK'
               AND tc.TABLE_SCHEMA = COALESCE(?, DATABASE()) AND tc.TABLE_NAME = ?
             ORDER BY cc.CONSTRAINT_NAME",
            &[
                schema.clone().map(Value::String).unwrap_or(Value::Null),
                Value::String(table.to_string()),
            ],
        )
        .await
    {
        Ok(result) => {
            design.check_constraints = result
                .rows
                .iter()
                .filter_map(|row| {
                    Some(
                        CheckConstraintDesign::new()
                            .named(text(row.get(0))?)
                            .expression(text(row.get(1))?),
                    )
                })
                .collect();
        }
        Err(error) => tracing::warn!(%error, table, "Failed to load MySQL check constraints"),
    }
}

/// PK / UQ column flags follow the indexes: UQ only for a plain single-column
/// unique key, never for the primary key or a composite one.
fn sync_key_flags(design: &mut TableDesign) {
    let primary: Vec<String> = design
        .indexes
        .iter()
        .filter(|index| index.is_primary)
        .flat_map(|index| index.columns.iter().cloned())
        .collect();
    for column in &mut design.columns {
        column.is_unique = false;
        column.unique_constraint_name = None;
        if primary.iter().any(|name| name == &column.name) {
            column.is_primary_key = true;
        }
    }
    for index in design
        .indexes
        .iter()
        .filter(|i| i.is_unique && !i.is_primary && i.columns.len() == 1)
    {
        if let Some(column) = design
            .columns
            .iter_mut()
            .find(|c| c.name == index.columns[0] && !c.is_unique)
        {
            column.is_unique = true;
            column.unique_constraint_name = Some(index.name.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ColumnDesign, DatabaseDialect};

    #[test]
    fn mariadb_defaults_are_already_sql() {
        let mariadb = |raw: &str, ty: &str| column_default(Some(raw.to_string()), ty, "", true);
        assert_eq!(
            mariadb("'draft'", "enum('draft','paid')"),
            Some("'draft'".to_string())
        );
        assert_eq!(mariadb("NULL", "datetime(6)"), None);
        assert_eq!(mariadb("0.00", "decimal(12,2)"), Some("0.00".to_string()));
        assert_eq!(
            mariadb("current_timestamp(6)", "timestamp(6)"),
            Some("current_timestamp(6)".to_string())
        );
        assert_eq!(
            column_default(
                Some("'draft'".into()),
                "varchar(8)",
                "VIRTUAL GENERATED",
                true
            ),
            None
        );
        // MySQL keeps quoting bare literals.
        assert_eq!(
            column_default(Some("draft".into()), "varchar(8)", "", false),
            Some("'draft'".to_string())
        );
    }

    #[test]
    fn defaults_are_returned_as_sql() {
        assert_eq!(catalog_default("it's", "varchar(10)", ""), "'it''s'");
        assert_eq!(catalog_default("a\\b", "varchar(5)", ""), "'a\\\\b'");
        assert_eq!(catalog_default("1.50", "decimal(10,2) unsigned", ""), "1.50");
        assert_eq!(catalog_default("0", "tinyint(1)", ""), "0");
        assert_eq!(catalog_default("x", "enum('x','y')", ""), "'x'");
        assert_eq!(catalog_default("2020-01-02", "date", ""), "'2020-01-02'");
        assert_eq!(catalog_default("b'101'", "bit(8)", ""), "b'101'");
        assert_eq!(catalog_default("CURRENT_TIMESTAMP", "timestamp", "DEFAULT_GENERATED"), "CURRENT_TIMESTAMP");
        assert_eq!(catalog_default("CURRENT_TIMESTAMP(6)", "datetime(6)", "DEFAULT_GENERATED"), "CURRENT_TIMESTAMP(6)");
        assert_eq!(catalog_default("uuid()", "char(36)", "DEFAULT_GENERATED"), "(uuid())");
        // A string that merely looks like an expression stays a string.
        assert_eq!(catalog_default("CURRENT_TIMESTAMP", "varchar(30)", ""), "'CURRENT_TIMESTAMP'");
    }

    #[test]
    fn on_update_comes_from_extra() {
        assert_eq!(
            on_update_from_extra("DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)").as_deref(),
            Some("CURRENT_TIMESTAMP(6)")
        );
        assert_eq!(on_update_from_extra("auto_increment"), None);
    }

    fn stat(index: &str, column: Option<&str>, expression: Option<&str>, unique: bool) -> StatisticsRow {
        StatisticsRow {
            index: index.into(),
            column: column.map(Into::into),
            expression: expression.map(Into::into),
            descending: false,
            prefix: None,
            index_type: "BTREE".into(),
            unique,
            comment: None,
        }
    }

    #[test]
    fn indexes_keep_prefix_functional_and_descending_parts() {
        let mut prefix = stat("name_pfx", Some("name"), None, false);
        prefix.prefix = Some(10);
        let mut desc = stat("age_desc", Some("age"), None, false);
        desc.descending = true;
        let mut fulltext = stat("ftx", Some("body"), None, false);
        fulltext.index_type = "FULLTEXT".into();
        let mut spatial = stat("sp", Some("geo"), None, false);
        spatial.index_type = "SPATIAL".into();
        spatial.prefix = Some(32);
        let indexes = indexes_from_statistics(vec![
            stat("PRIMARY", Some("id"), None, true),
            prefix,
            desc,
            stat("fx", None, Some("(`a` + `b`)"), false),
            stat("comp", Some("a"), None, false),
            stat("comp", Some("b"), None, false),
            fulltext,
            spatial,
        ]);
        let shape: Vec<(String, Vec<String>, bool, bool)> = indexes
            .iter()
            .map(|i| (i.name.clone(), i.columns.clone(), i.is_primary, i.column_descending.is_empty()))
            .collect();
        assert_eq!(
            shape,
            [
                ("PRIMARY".to_string(), vec!["id".to_string()], true, true),
                ("name_pfx".to_string(), vec!["name(10)".to_string()], false, true),
                ("age_desc".to_string(), vec!["age".to_string()], false, false),
                ("fx".to_string(), vec!["(`a` + `b`)".to_string()], false, true),
                ("comp".to_string(), vec!["a".to_string(), "b".to_string()], false, true),
                ("ftx".to_string(), vec!["body".to_string()], false, true),
                ("sp".to_string(), vec!["geo".to_string()], false, true),
            ]
        );
        assert_eq!(indexes[5].index_type, "FULLTEXT");
    }

    #[test]
    fn unique_flag_only_for_plain_single_column_non_primary_unique_keys() {
        let mut design = TableDesign::new("t", DatabaseDialect::Mysql);
        for name in ["id", "a", "b", "c", "d"] {
            design = design.with_column(ColumnDesign::named(name));
        }
        let unique = |name: &str, columns: &[&str]| {
            let mut index = IndexDesign::named(name).unique();
            index.columns = columns.iter().map(|c| c.to_string()).collect();
            index
        };
        let mut primary = unique("PRIMARY", &["id"]);
        primary.is_primary = true;
        design.indexes = vec![
            primary,
            unique("uq_ab", &["a", "b"]),
            unique("uq_c", &["c"]),
            unique("uq_d_prefix", &["d(5)"]),
        ];
        sync_key_flags(&mut design);
        let flags: Vec<(bool, bool, Option<&str>)> = design
            .columns
            .iter()
            .map(|c| (c.is_primary_key, c.is_unique, c.unique_constraint_name.as_deref()))
            .collect();
        assert_eq!(
            flags,
            [
                (true, false, None),
                (false, false, None),
                (false, false, None),
                (false, true, Some("uq_c")),
                (false, false, None),
            ]
        );
    }
}
