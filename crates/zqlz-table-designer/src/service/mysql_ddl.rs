//! MySQL DDL generation for the table designer.
//!
//! MySQL differs from the generic path in almost every clause (`MODIFY COLUMN`
//! re-specifies the whole column, indexes are `KEY`s, `DROP INDEX` removes
//! unique constraints, FKs and checks have their own `DROP` forms), so it is
//! generated here as one self-contained unit. `CREATE TABLE` is always a single
//! statement so the SQL preview is exactly what Save runs.

use std::collections::{HashMap, HashSet};

use crate::models::{
    CheckConstraintDesign, ColumnDesign, ForeignKeyDesign, IndexDesign, TableDesign, TableOptions,
};
use zqlz_core::ForeignKeyAction;

/// Quote an identifier, doubling embedded backticks.
pub(crate) fn q(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Quote a string literal (backslashes are escapes under the default sql_mode).
pub(crate) fn lit(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn table_ref(design: &TableDesign) -> String {
    match design.schema.as_deref() {
        Some(schema) if !schema.is_empty() => format!("{}.{}", q(schema), q(&design.table_name)),
        _ => q(&design.table_name),
    }
}

// ---------------------------------------------------------------------------
// Types and defaults
// ---------------------------------------------------------------------------

fn is_attribute_word(word: &str) -> bool {
    ["unsigned", "signed", "zerofill"]
        .iter()
        .any(|attribute| attribute.eq_ignore_ascii_case(word))
}

/// `int unsigned` + length 10 -> `int(10) unsigned`; `enum('a')` stays verbatim.
fn type_spec(column: &ColumnDesign) -> String {
    let declared = column.data_type.trim();
    if declared.contains('(') {
        return declared.to_string();
    }
    let mut words: Vec<&str> = declared.split_whitespace().collect();
    let mut attributes = Vec::new();
    while words.len() > 1 && words.last().is_some_and(|w| is_attribute_word(w)) {
        attributes.insert(0, words.pop().unwrap_or_default());
    }
    let mut spec = words.join(" ");
    match (column.length, column.scale) {
        (Some(length), Some(scale)) => spec.push_str(&format!("({length},{scale})")),
        (Some(length), None) => spec.push_str(&format!("({length})")),
        _ => {}
    }
    for attribute in attributes {
        spec.push(' ');
        spec.push_str(attribute);
    }
    spec
}

fn is_current_timestamp(upper: &str) -> bool {
    ["CURRENT_TIMESTAMP", "NOW", "LOCALTIME", "LOCALTIMESTAMP"]
        .iter()
        .any(|name| {
            upper.strip_prefix(name).is_some_and(|rest| {
                rest.is_empty()
                    || (rest.starts_with('(')
                        && rest.ends_with(')')
                        && rest[1..rest.len() - 1].chars().all(|c| c.is_ascii_digit()))
            })
        })
}

/// The `DEFAULT` operand for what the user typed: literals pass through,
/// function calls get the parentheses MySQL 8 requires for expression defaults,
/// and a bare word is a string literal.
pub(crate) fn default_sql(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let upper = raw.to_ascii_uppercase();
    let first = raw.chars().next().unwrap_or(' ');
    let numeric = raw.chars().all(|c| c.is_ascii_digit() || "+-.eE".contains(c))
        && raw.parse::<f64>().is_ok();
    let bit_or_hex = (upper.starts_with("B'") || upper.starts_with("X'") || upper.starts_with("0X"))
        && !upper.contains(' ');
    if matches!(first, '\'' | '"' | '(')
        || numeric
        || bit_or_hex
        || matches!(upper.as_str(), "NULL" | "TRUE" | "FALSE")
        || is_current_timestamp(&upper)
    {
        return Some(raw.to_string());
    }
    if raw.ends_with(')') && raw.contains('(') && first.is_alphabetic() {
        return Some(format!("({raw})"));
    }
    Some(lit(raw))
}

fn is_literal_default(sql: &str) -> bool {
    !sql.starts_with('(') && !is_current_timestamp(&sql.to_ascii_uppercase())
}

// ---------------------------------------------------------------------------
// Column definitions
// ---------------------------------------------------------------------------

fn column_def(column: &ColumnDesign, options: &TableOptions) -> String {
    let mut def = format!("{} {}", q(&column.name), type_spec(column));

    let same = |a: &Option<String>, b: &Option<String>| match (a, b) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        _ => false,
    };
    if column.charset.is_some() || column.collation.is_some() {
        let differs = !same(&column.charset, &options.charset)
            || (column.collation.is_some() && !same(&column.collation, &options.collation));
        if differs {
            if let Some(charset) = column.charset.as_deref().filter(|c| !c.is_empty()) {
                def.push_str(&format!(" CHARACTER SET {charset}"));
            }
            if let Some(collation) = column.collation.as_deref().filter(|c| !c.is_empty()) {
                def.push_str(&format!(" COLLATE {collation}"));
            }
        }
    }

    let not_null = !column.nullable || column.is_primary_key;
    let generated = column
        .generated_expression
        .as_deref()
        .map(str::trim)
        .filter(|e| !e.is_empty());
    if let Some(expression) = generated {
        let storage = if column.generated_stored {
            "STORED"
        } else {
            "VIRTUAL"
        };
        def.push_str(&format!(" GENERATED ALWAYS AS ({expression}) {storage}"));
        if not_null {
            def.push_str(" NOT NULL");
        }
        if column.invisible {
            def.push_str(" INVISIBLE");
        }
    } else {
        if not_null {
            def.push_str(" NOT NULL");
        } else if column.data_type.trim().to_ascii_lowercase().starts_with("timestamp") {
            // Old servers give a bare TIMESTAMP an implicit NOT NULL default.
            def.push_str(" NULL");
        }
        if !column.is_auto_increment
            && let Some(default) = column.default_value.as_deref().and_then(default_sql)
        {
            def.push_str(&format!(" DEFAULT {default}"));
        }
        if column.invisible {
            def.push_str(" INVISIBLE");
        }
        if column.is_auto_increment {
            def.push_str(" AUTO_INCREMENT");
        }
        if let Some(on_update) = column.on_update.as_deref().filter(|v| !v.is_empty()) {
            def.push_str(&format!(" ON UPDATE {on_update}"));
        }
    }
    if let Some(comment) = column.comment.as_deref().filter(|c| !c.is_empty()) {
        def.push_str(&format!(" COMMENT {}", lit(comment)));
    }
    def
}

/// Everything `MODIFY COLUMN` would change, other than the name and position.
fn column_signature(column: &ColumnDesign, options: &TableOptions) -> String {
    let mut column = column.clone();
    column.name = String::new();
    // Key membership is handled by the PRIMARY KEY statements, and it implies
    // NOT NULL on its own.
    column.is_primary_key = false;
    column_def(&column, options).to_ascii_lowercase()
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

enum KeyPart<'a> {
    Column { name: &'a str, prefix: Option<&'a str> },
    Expression(String),
}

fn split_prefix(token: &str) -> Option<(&str, &str)> {
    let inner = token.strip_suffix(')')?;
    let open = inner.rfind('(')?;
    let length = &inner[open + 1..];
    (!length.is_empty() && length.chars().all(|c| c.is_ascii_digit()))
        .then(|| (inner[..open].trim_end(), length))
}

fn classify<'a>(token: &'a str, columns: &HashSet<String>) -> KeyPart<'a> {
    let token = token.trim();
    if token.starts_with('(') && token.ends_with(')') {
        return KeyPart::Expression(token.to_string());
    }
    if columns.contains(&token.to_lowercase()) {
        return KeyPart::Column { name: token, prefix: None };
    }
    if let Some((name, length)) = split_prefix(token)
        && columns.contains(&name.to_lowercase())
    {
        return KeyPart::Column { name, prefix: Some(length) };
    }
    if !token.is_empty() && token.chars().all(|c| c.is_alphanumeric() || "_$".contains(c)) {
        return KeyPart::Column { name: token, prefix: None };
    }
    KeyPart::Expression(format!("({token})"))
}

/// The column an index key part refers to; `None` for functional parts.
/// Used by design validation.
pub(crate) fn mysql_index_part_column(token: &str, columns: &HashSet<String>) -> Option<String> {
    match classify(token, columns) {
        KeyPart::Column { name, .. } => Some(name.to_string()),
        KeyPart::Expression(_) => None,
    }
}

fn key_parts(index: &IndexDesign, columns: &HashSet<String>) -> String {
    index
        .columns
        .iter()
        .enumerate()
        .map(|(i, token)| {
            let mut part = match classify(token, columns) {
                KeyPart::Column { name, prefix: Some(length) } => format!("{}({length})", q(name)),
                KeyPart::Column { name, prefix: None } => q(name),
                KeyPart::Expression(expression) => expression,
            };
            if index.column_descending.get(i).copied().unwrap_or(false) && !is_special_index(index) {
                part.push_str(" DESC");
            }
            part
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn is_special_index(index: &IndexDesign) -> bool {
    matches!(
        index.index_type.trim().to_ascii_uppercase().as_str(),
        "FULLTEXT" | "SPATIAL"
    )
}

fn column_names(design: &TableDesign) -> HashSet<String> {
    design.columns.iter().map(|c| c.name.to_lowercase()).collect()
}

/// `UNIQUE KEY `n` (`a`, `b`) USING HASH COMMENT '..'` (the CREATE TABLE form).
fn key_definition(index: &IndexDesign, columns: &HashSet<String>) -> String {
    let kind = match index.index_type.trim().to_ascii_uppercase().as_str() {
        "FULLTEXT" => "FULLTEXT KEY",
        "SPATIAL" => "SPATIAL KEY",
        _ if index.is_unique => "UNIQUE KEY",
        _ => "KEY",
    };
    let mut sql = kind.to_string();
    if !index.name.is_empty() {
        sql.push(' ');
        sql.push_str(&q(&index.name));
    }
    sql.push_str(&format!(" ({})", key_parts(index, columns)));
    if index.index_type.trim().eq_ignore_ascii_case("HASH") {
        sql.push_str(" USING HASH");
    }
    if let Some(comment) = index.comment.as_deref().filter(|c| !c.is_empty()) {
        sql.push_str(&format!(" COMMENT {}", lit(comment)));
    }
    sql
}

/// Index identity for diffing; `rename` maps old column names to new ones.
fn index_signature(
    index: &IndexDesign,
    columns: &HashSet<String>,
    rename: &HashMap<String, String>,
) -> String {
    let mut index = index.clone();
    index.name = String::new();
    index.columns = index
        .columns
        .iter()
        .map(|token| match classify(token, columns) {
            KeyPart::Column { name, prefix } => {
                let name = rename.get(&name.to_lowercase()).map_or(name, String::as_str);
                match prefix {
                    Some(length) => format!("{name}({length})"),
                    None => name.to_string(),
                }
            }
            KeyPart::Expression(expression) => expression,
        })
        .collect();
    index.column_descending.resize(index.columns.len(), false);
    let mut known = columns.clone();
    known.extend(rename.values().map(|name| name.to_lowercase()));
    key_definition(&index, &known).to_lowercase()
}

// ---------------------------------------------------------------------------
// Foreign keys and checks
// ---------------------------------------------------------------------------

fn action(action: ForeignKeyAction) -> &'static str {
    super::ddl_generator::fk_action_to_sql(&action)
}

fn foreign_key_definition(foreign_key: &ForeignKeyDesign, design: &TableDesign) -> String {
    let mut sql = String::new();
    if let Some(name) = foreign_key.name.as_deref().filter(|n| !n.is_empty()) {
        sql.push_str(&format!("CONSTRAINT {} ", q(name)));
    }
    let list = |names: &[String]| names.iter().map(|n| q(n)).collect::<Vec<_>>().join(", ");
    let own_schema = design.schema.as_deref().unwrap_or_default();
    let referenced = match foreign_key.referenced_schema.as_deref() {
        Some(schema) if !schema.is_empty() && !schema.eq_ignore_ascii_case(own_schema) => {
            format!("{}.{}", q(schema), q(&foreign_key.referenced_table))
        }
        _ => q(&foreign_key.referenced_table),
    };
    sql.push_str(&format!(
        "FOREIGN KEY ({}) REFERENCES {} ({})",
        list(&foreign_key.columns),
        referenced,
        list(&foreign_key.referenced_columns)
    ));
    if foreign_key.on_delete != ForeignKeyAction::NoAction {
        sql.push_str(&format!(" ON DELETE {}", action(foreign_key.on_delete)));
    }
    if foreign_key.on_update != ForeignKeyAction::NoAction {
        sql.push_str(&format!(" ON UPDATE {}", action(foreign_key.on_update)));
    }
    sql
}

fn foreign_key_signature(
    foreign_key: &ForeignKeyDesign,
    design: &TableDesign,
    rename: &HashMap<String, String>,
) -> String {
    let mut foreign_key = foreign_key.clone();
    foreign_key.name = None;
    for column in &mut foreign_key.columns {
        if let Some(new) = rename.get(&column.to_lowercase()) {
            *column = new.clone();
        }
    }
    foreign_key_definition(&foreign_key, design).to_lowercase()
}

fn check_definition(check: &CheckConstraintDesign) -> String {
    let name = check
        .name
        .as_deref()
        .filter(|n| !n.is_empty())
        .map(|n| format!("CONSTRAINT {} ", q(n)))
        .unwrap_or_default();
    format!("{name}CHECK ({})", check.expression.trim())
}

// ---------------------------------------------------------------------------
// Table options
// ---------------------------------------------------------------------------

fn table_options(options: &TableOptions, comment: Option<&str>) -> String {
    let mut parts = Vec::new();
    if let Some(engine) = options.engine.as_deref().filter(|v| !v.is_empty()) {
        parts.push(format!("ENGINE={engine}"));
    }
    if let Some(charset) = options.charset.as_deref().filter(|v| !v.is_empty()) {
        parts.push(format!("DEFAULT CHARSET={charset}"));
    }
    if let Some(collation) = options.collation.as_deref().filter(|v| !v.is_empty()) {
        parts.push(format!("COLLATE={collation}"));
    }
    if let Some(start) = options.auto_increment_start {
        parts.push(format!("AUTO_INCREMENT={start}"));
    }
    if let Some(row_format) = options.row_format.as_deref().filter(|v| !v.is_empty()) {
        parts.push(format!("ROW_FORMAT={row_format}"));
    }
    if let Some(comment) = comment.filter(|c| !c.is_empty()) {
        parts.push(format!("COMMENT={}", lit(comment)));
    }
    parts.join(" ")
}

// ---------------------------------------------------------------------------
// Primary keys
// ---------------------------------------------------------------------------

/// Primary key column names in key order. A loaded design keeps the order in
/// its PRIMARY index; otherwise it is the column order.
fn primary_key(design: &TableDesign) -> Vec<String> {
    let flagged: Vec<String> = design
        .columns
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.clone())
        .collect();
    let names = column_names(design);
    if let Some(primary) = design.indexes.iter().find(|i| i.is_primary) {
        let ordered: Vec<String> = primary
            .columns
            .iter()
            .filter_map(|token| mysql_index_part_column(token, &names))
            .collect();
        let lower = |list: &[String]| {
            let mut v: Vec<String> = list.iter().map(|s| s.to_lowercase()).collect();
            v.sort();
            v
        };
        if lower(&ordered) == lower(&flagged) {
            return ordered;
        }
    }
    flagged
}

fn key_list(columns: &[String]) -> String {
    columns.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ")
}

// ---------------------------------------------------------------------------
// CREATE TABLE
// ---------------------------------------------------------------------------

/// A name for the key behind a column-level UNIQUE flag: MySQL's own default
/// (the column name), falling back when another index already uses it.
fn unique_key_name(column: &str, taken: &HashSet<String>) -> String {
    let mut name = column.to_string();
    let mut attempt = 1;
    while taken.contains(&name.to_lowercase()) {
        name = if attempt == 1 {
            format!("uq_{column}")
        } else {
            format!("uq_{column}_{attempt}")
        };
        attempt += 1;
    }
    name
}

fn covers_single_column(index: &IndexDesign, column: &str, names: &HashSet<String>) -> bool {
    index.is_unique
        && !index.is_primary
        && index.columns.len() == 1
        && matches!(
            classify(&index.columns[0], names),
            KeyPart::Column { name, prefix: None } if name.eq_ignore_ascii_case(column)
        )
}

pub(crate) fn create_table(design: &TableDesign) -> String {
    let names = column_names(design);
    let mut lines: Vec<String> = design
        .columns
        .iter()
        .map(|column| column_def(column, &design.options))
        .collect();

    let pk = primary_key(design);
    if !pk.is_empty() {
        lines.push(format!("PRIMARY KEY ({})", key_list(&pk)));
    }

    let mut taken: HashSet<String> = design.indexes.iter().map(|i| i.name.to_lowercase()).collect();
    for column in design.columns.iter().filter(|c| c.is_unique && !c.is_primary_key) {
        if design
            .indexes
            .iter()
            .any(|index| covers_single_column(index, &column.name, &names))
        {
            continue;
        }
        let name = unique_key_name(&column.name, &taken);
        taken.insert(name.to_lowercase());
        lines.push(format!("UNIQUE KEY {} ({})", q(&name), q(&column.name)));
    }
    for index in design.indexes.iter().filter(|i| !i.is_primary) {
        lines.push(key_definition(index, &names));
    }
    for foreign_key in &design.foreign_keys {
        lines.push(foreign_key_definition(foreign_key, design));
    }
    for check in &design.check_constraints {
        lines.push(check_definition(check));
    }

    let mut sql = format!(
        "CREATE TABLE {} (\n  {}\n)",
        table_ref(design),
        lines.join(",\n  ")
    );
    let options = table_options(&design.options, design.comment.as_deref());
    if !options.is_empty() {
        sql.push(' ');
        sql.push_str(&options);
    }
    sql.push(';');
    sql
}

// ---------------------------------------------------------------------------
// ALTER TABLE
// ---------------------------------------------------------------------------

fn empty_to_none(value: &Option<String>) -> Option<&str> {
    value.as_deref().filter(|v| !v.is_empty())
}

/// Indexes (into `modified`) of the surviving columns that can stay put: the
/// longest run that is already in the same relative order. Everything else is
/// moved with `AFTER`.
fn stable_columns(original_rank: &[Option<usize>]) -> HashSet<usize> {
    let items: Vec<(usize, usize)> = original_rank
        .iter()
        .enumerate()
        .filter_map(|(i, rank)| rank.map(|rank| (i, rank)))
        .collect();
    let mut best: Vec<(usize, Option<usize>)> = Vec::new();
    for (n, &(_, rank)) in items.iter().enumerate() {
        let mut longest = (1, None);
        for (m, &(_, other)) in items.iter().enumerate().take(n) {
            if other < rank && best[m].0 + 1 > longest.0 {
                longest = (best[m].0 + 1, Some(m));
            }
        }
        best.push(longest);
    }
    let mut stable = HashSet::new();
    let mut cursor = best
        .iter()
        .enumerate()
        .max_by_key(|(_, (length, _))| *length)
        .map(|(n, _)| n);
    while let Some(n) = cursor {
        stable.insert(items[n].0);
        cursor = best[n].1;
    }
    stable
}

pub(crate) fn alter_table(original: &TableDesign, modified: &TableDesign) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();

    if original.table_name != modified.table_name {
        out.push(format!(
            "ALTER TABLE {} RENAME TO {};",
            table_ref(original),
            q(&modified.table_name)
        ));
    }
    let t = table_ref(modified);
    let alter = |clause: String| format!("ALTER TABLE {t} {clause};");

    // Pair every modified column with its original (stable id first, then name).
    let matched: Vec<Option<&ColumnDesign>> = modified
        .columns
        .iter()
        .map(|new| {
            original
                .columns
                .iter()
                .find(|c| c.column_id == new.column_id)
                .or_else(|| original.columns.iter().find(|c| c.name == new.name))
        })
        .collect();
    let kept = |old: &ColumnDesign| {
        matched
            .iter()
            .flatten()
            .any(|m| std::ptr::eq(*m, old))
    };
    let mut rename: HashMap<String, String> = HashMap::new();
    for (new, old) in modified.columns.iter().zip(&matched) {
        if let Some(old) = old
            && old.name != new.name
        {
            rename.insert(old.name.to_lowercase(), new.name.clone());
        }
    }
    let renamed = |name: &str| rename.get(&name.to_lowercase()).cloned().unwrap_or(name.to_string());

    let original_names = column_names(original);
    let modified_names = column_names(modified);
    let no_rename = HashMap::new();

    // --- Drop foreign keys, checks and indexes that are gone or changed ---
    for foreign_key in &original.foreign_keys {
        let Some(name) = foreign_key.name.as_deref() else { continue };
        let unchanged = modified.foreign_keys.iter().any(|f| {
            f.name.as_deref() == Some(name)
                && foreign_key_signature(f, modified, &no_rename)
                    == foreign_key_signature(foreign_key, original, &rename)
        });
        if !unchanged {
            out.push(alter(format!("DROP FOREIGN KEY {}", q(name))));
        }
    }
    for check in &original.check_constraints {
        let Some(name) = check.name.as_deref() else { continue };
        let unchanged = modified
            .check_constraints
            .iter()
            .any(|c| c.name.as_deref() == Some(name) && c.expression.trim() == check.expression.trim());
        if !unchanged {
            out.push(alter(format!("DROP CHECK {}", q(name))));
        }
    }
    let mut dropped_indexes: HashSet<String> = HashSet::new();
    let mut changed_indexes: HashSet<String> = HashSet::new();
    for index in original.indexes.iter().filter(|i| !i.is_primary) {
        let unchanged = modified.indexes.iter().any(|i| {
            i.name.eq_ignore_ascii_case(&index.name)
                && index_signature(i, &modified_names, &no_rename)
                    == index_signature(index, &original_names, &rename)
        });
        if !unchanged {
            changed_indexes.insert(index.name.to_lowercase());
            if dropped_indexes.insert(index.name.to_lowercase()) {
                out.push(alter(format!("DROP INDEX {}", q(&index.name))));
            }
        }
    }
    // Column-level UQ toggled off: drop the key it was backed by.
    for (new, old) in modified.columns.iter().zip(&matched) {
        let Some(old) = old else { continue };
        if old.is_unique && !new.is_unique && !old.is_primary_key {
            let name = old.unique_constraint_name.clone().unwrap_or_else(|| old.name.clone());
            if dropped_indexes.insert(name.to_lowercase()) {
                out.push(alter(format!("DROP INDEX {}", q(&name))));
            }
        }
    }

    // --- Primary key ---
    let original_pk = primary_key(original);
    let modified_pk = primary_key(modified);
    let sorted = |list: &[String], map: bool| {
        let mut v: Vec<String> = list
            .iter()
            .map(|c| if map { renamed(c) } else { c.clone() }.to_lowercase())
            .collect();
        v.sort();
        v
    };
    let pk_changed = sorted(&original_pk, true) != sorted(&modified_pk, false);
    let mut forced: HashSet<String> = HashSet::new();
    if pk_changed && !original_pk.is_empty() {
        // An AUTO_INCREMENT column must stay a key, so it is released in the
        // same statement that drops the key and re-added by the column pass.
        let released: Vec<String> = original
            .columns
            .iter()
            .filter(|c| c.is_auto_increment && original_pk.iter().any(|p| p.eq_ignore_ascii_case(&c.name)))
            .map(|c| {
                forced.insert(c.name.to_lowercase());
                let mut plain = c.clone();
                plain.is_auto_increment = false;
                format!("MODIFY COLUMN {}", column_def(&plain, &original.options))
            })
            .collect();
        let mut clause = released.join(", ");
        if !clause.is_empty() {
            clause.push_str(", ");
        }
        clause.push_str("DROP PRIMARY KEY");
        out.push(alter(clause));
    }

    // --- Rename, then drop columns ---
    for (new, old) in modified.columns.iter().zip(&matched) {
        if let Some(old) = old
            && old.name != new.name
        {
            out.push(alter(format!("RENAME COLUMN {} TO {}", q(&old.name), q(&new.name))));
        }
    }
    for old in original.columns.iter().filter(|c| !kept(c)) {
        out.push(alter(format!("DROP COLUMN {}", q(&old.name))));
    }

    // --- Add, change and move columns in the final order ---
    let survivors: Vec<&ColumnDesign> = original.columns.iter().filter(|c| kept(c)).collect();
    let ranks: Vec<Option<usize>> = matched
        .iter()
        .map(|old| old.and_then(|old| survivors.iter().position(|s| std::ptr::eq(*s, old))))
        .collect();
    let stable = stable_columns(&ranks);
    let mut pending_pk = (pk_changed && !modified_pk.is_empty())
        .then(|| format!("ADD PRIMARY KEY ({})", key_list(&modified_pk)));
    let pk_has = |name: &str| modified_pk.iter().any(|p| p.eq_ignore_ascii_case(name));

    for (i, (new, old)) in modified.columns.iter().zip(&matched).enumerate() {
        let position = match i.checked_sub(1) {
            None => " FIRST".to_string(),
            Some(prev) => format!(" AFTER {}", q(&modified.columns[prev].name)),
        };
        let attach_pk = |pending: &mut Option<String>| {
            if new.is_auto_increment && pk_has(&new.name) {
                pending.take().map(|pk| format!(", {pk}")).unwrap_or_default()
            } else {
                String::new()
            }
        };
        let def = column_def(new, &modified.options);
        match old {
            None => {
                let has_later_existing = ranks[i + 1..].iter().any(Option::is_some);
                let position = if has_later_existing { position } else { String::new() };
                let pk = attach_pk(&mut pending_pk);
                out.push(alter(format!("ADD COLUMN {def}{position}{pk}")));
            }
            Some(old) => {
                let moved = !stable.contains(&i);
                // A released AUTO_INCREMENT column is no longer AUTO_INCREMENT in the table.
                let released = forced.contains(&old.name.to_lowercase());
                let base: &ColumnDesign = old;
                let old = &ColumnDesign {
                    is_auto_increment: base.is_auto_increment && !released,
                    ..base.clone()
                };
                let changed = column_signature(old, &original.options)
                    != column_signature(new, &modified.options)
                    || released && new.is_auto_increment;
                if moved || changed {
                    let only_default = !moved
                        && !released
                        && column_signature(
                            &ColumnDesign { default_value: new.default_value.clone(), ..old.clone() },
                            &original.options,
                        ) == column_signature(new, &modified.options)
                        && new.generated_expression.is_none();
                    if only_default {
                        let clause = match new.default_value.as_deref().and_then(default_sql) {
                            Some(default) if is_literal_default(&default) => {
                                format!("ALTER COLUMN {} SET DEFAULT {default}", q(&new.name))
                            }
                            Some(_) => format!("MODIFY COLUMN {def}"),
                            None => format!("ALTER COLUMN {} DROP DEFAULT", q(&new.name)),
                        };
                        out.push(alter(clause));
                    } else {
                        let position = if moved { position } else { String::new() };
                        let pk = attach_pk(&mut pending_pk);
                        out.push(alter(format!("MODIFY COLUMN {def}{position}{pk}")));
                    }
                }
            }
        }
    }
    if let Some(pk) = pending_pk {
        out.push(alter(pk));
    }

    // --- Add indexes ---
    let present: HashSet<String> = original
        .indexes
        .iter()
        .map(|i| i.name.to_lowercase())
        .filter(|name| !changed_indexes.contains(name))
        .collect();
    let mut taken: HashSet<String> = modified.indexes.iter().map(|i| i.name.to_lowercase()).collect();
    for (new, old) in modified.columns.iter().zip(&matched) {
        let was_unique = old.is_some_and(|o| o.is_unique);
        if new.is_unique
            && !new.is_primary_key
            && !was_unique
            && !modified.indexes.iter().any(|i| {
                covers_single_column(i, &new.name, &modified_names)
                    && !present.contains(&i.name.to_lowercase())
            })
        {
            let name = unique_key_name(&new.name, &taken);
            taken.insert(name.to_lowercase());
            out.push(alter(format!("ADD UNIQUE KEY {} ({})", q(&name), q(&new.name))));
        }
    }
    for index in modified.indexes.iter().filter(|i| !i.is_primary) {
        if !present.contains(&index.name.to_lowercase()) || index.name.is_empty() {
            out.push(alter(format!("ADD {}", key_definition(index, &modified_names))));
        }
    }

    // --- Add foreign keys and checks ---
    for foreign_key in &modified.foreign_keys {
        let existed = foreign_key.name.as_deref().is_some_and(|name| {
            original.foreign_keys.iter().any(|f| {
                f.name.as_deref() == Some(name)
                    && foreign_key_signature(f, original, &rename)
                        == foreign_key_signature(foreign_key, modified, &no_rename)
            })
        });
        if !existed {
            out.push(alter(format!("ADD {}", foreign_key_definition(foreign_key, modified))));
        }
    }
    for check in &modified.check_constraints {
        let existed = check.name.as_deref().is_some_and(|name| {
            original
                .check_constraints
                .iter()
                .any(|c| c.name.as_deref() == Some(name) && c.expression.trim() == check.expression.trim())
        });
        if !existed {
            out.push(alter(format!("ADD {}", check_definition(check))));
        }
    }

    // --- Table options and comment ---
    let (before, after) = (&original.options, &modified.options);
    let mut options = Vec::new();
    let differs = |old: &Option<String>, new: &Option<String>| {
        empty_to_none(new).is_some_and(|new| empty_to_none(old).is_none_or(|old| !old.eq_ignore_ascii_case(new)))
    };
    if differs(&before.engine, &after.engine) {
        options.push(format!("ENGINE={}", after.engine.as_deref().unwrap_or_default()));
    }
    if differs(&before.charset, &after.charset) {
        options.push(format!("DEFAULT CHARSET={}", after.charset.as_deref().unwrap_or_default()));
    }
    if differs(&before.collation, &after.collation) {
        options.push(format!("COLLATE={}", after.collation.as_deref().unwrap_or_default()));
    }
    if differs(&before.row_format, &after.row_format) {
        options.push(format!("ROW_FORMAT={}", after.row_format.as_deref().unwrap_or_default()));
    }
    if let Some(start) = after.auto_increment_start
        && before.auto_increment_start != Some(start)
    {
        options.push(format!("AUTO_INCREMENT={start}"));
    }
    if original.comment.as_deref().unwrap_or_default() != modified.comment.as_deref().unwrap_or_default() {
        options.push(format!(
            "COMMENT={}",
            lit(modified.comment.as_deref().unwrap_or_default())
        ));
    }
    if !options.is_empty() {
        out.push(alter(options.join(" ")));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::DatabaseDialect;

    fn design(columns: Vec<ColumnDesign>) -> TableDesign {
        let mut design = TableDesign::new("t", DatabaseDialect::Mysql);
        for column in columns {
            design = design.with_column(column);
        }
        design
    }

    fn col(name: &str, data_type: &str) -> ColumnDesign {
        ColumnDesign::named(name).data_type(data_type)
    }

    #[test]
    fn names_and_strings_are_escaped() {
        assert_eq!(q("a`b"), "`a``b`");
        assert_eq!(lit("it's \\ ok"), "'it''s \\\\ ok'");
    }

    #[test]
    fn default_literals_expressions_and_barewords() {
        assert_eq!(default_sql("'x'").as_deref(), Some("'x'"));
        assert_eq!(default_sql("42").as_deref(), Some("42"));
        assert_eq!(default_sql("-1.5").as_deref(), Some("-1.5"));
        assert_eq!(default_sql("CURRENT_TIMESTAMP(6)").as_deref(), Some("CURRENT_TIMESTAMP(6)"));
        assert_eq!(default_sql("now()").as_deref(), Some("now()"));
        assert_eq!(default_sql("uuid()").as_deref(), Some("(uuid())"));
        assert_eq!(default_sql("(UUID())").as_deref(), Some("(UUID())"));
        assert_eq!(default_sql("b'101'").as_deref(), Some("b'101'"));
        assert_eq!(default_sql("hello").as_deref(), Some("'hello'"));
        assert_eq!(default_sql("  ").as_deref(), None);
    }

    #[test]
    fn type_spec_keeps_attributes_after_modifiers() {
        let mut c = col("c", "decimal unsigned");
        c.length = Some(10);
        c.scale = Some(2);
        assert_eq!(type_spec(&c), "decimal(10,2) unsigned");
        assert_eq!(type_spec(&col("e", "enum('a','b')")), "enum('a','b')");
        assert_eq!(type_spec(&col("d", "double precision")), "double precision");
    }

    #[test]
    fn create_table_is_a_single_statement() {
        let mut id = col("id", "bigint unsigned").primary_key().auto_increment();
        id.comment = Some("it's".into());
        let mut email = col("email", "varchar").not_null().unique();
        email.length = Some(255);
        let mut updated = col("updated", "timestamp");
        updated.default_value = Some("CURRENT_TIMESTAMP".into());
        updated.on_update = Some("CURRENT_TIMESTAMP".into());
        let mut d = design(vec![id, email, updated, col("back`tick", "int")]);
        d.indexes.push(IndexDesign::named("ix_e").column("email(10)"));
        d.comment = Some("a table".into());
        d.options = TableOptions::mysql();
        let sql = create_table(&d);
        assert_eq!(
            sql,
            "CREATE TABLE `t` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'it''s',\n  \
             `email` varchar(255) NOT NULL,\n  \
             `updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n  \
             `back``tick` int,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `email` (`email`),\n  \
             KEY `ix_e` (`email`(10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 \
             COLLATE=utf8mb4_unicode_ci COMMENT='a table';"
        );
    }

    #[test]
    fn generated_column_has_no_default_or_auto_increment() {
        let mut c = col("s", "int");
        c.generated_expression = Some("(`a` + `b`)".into());
        c.generated_stored = true;
        c.default_value = Some("1".into());
        assert_eq!(
            column_def(&c, &TableOptions::default()),
            "`s` int GENERATED ALWAYS AS ((`a` + `b`)) STORED"
        );
    }

    #[test]
    fn unchanged_design_has_no_statements() {
        let d = design(vec![col("id", "int").primary_key(), col("a", "varchar")]);
        assert!(alter_table(&d, &d.clone()).is_empty());
    }

    #[test]
    fn add_drop_rename_modify_and_move_columns() {
        let original = design(vec![col("id", "int").primary_key(), col("a", "int"), col("b", "int"), col("c", "int")]);
        let mut modified = original.clone();
        modified.columns[1].name = "a2".into();
        modified.columns[2].nullable = false;
        modified.columns.remove(3);
        let mut e = col("e", "text");
        e.comment = Some("hi".into());
        modified.columns.insert(1, e);
        assert_eq!(
            alter_table(&original, &modified),
            [
                "ALTER TABLE `t` RENAME COLUMN `a` TO `a2`;",
                "ALTER TABLE `t` DROP COLUMN `c`;",
                "ALTER TABLE `t` ADD COLUMN `e` text COMMENT 'hi' AFTER `id`;",
                "ALTER TABLE `t` MODIFY COLUMN `b` int NOT NULL;",
            ]
        );
    }

    #[test]
    fn moving_one_column_is_one_statement() {
        let original = design(vec![col("a", "int"), col("b", "int"), col("c", "int"), col("d", "int")]);
        let mut modified = original.clone();
        let a = modified.columns.remove(0);
        modified.columns.push(a);
        assert_eq!(
            alter_table(&original, &modified),
            ["ALTER TABLE `t` MODIFY COLUMN `a` int AFTER `d`;"]
        );
    }

    #[test]
    fn default_only_change_uses_set_and_drop_default() {
        let original = design(vec![col("a", "int")]);
        let mut modified = original.clone();
        modified.columns[0].default_value = Some("5".into());
        assert_eq!(
            alter_table(&original, &modified),
            ["ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT 5;"]
        );
        assert_eq!(
            alter_table(&modified, &original),
            ["ALTER TABLE `t` ALTER COLUMN `a` DROP DEFAULT;"]
        );
        modified.columns[0].default_value = Some("uuid()".into());
        assert_eq!(
            alter_table(&original, &modified),
            ["ALTER TABLE `t` MODIFY COLUMN `a` int DEFAULT (uuid());"]
        );
    }

    #[test]
    fn unique_flag_adds_key_and_drops_it_by_introspected_name() {
        let original = design(vec![col("id", "int").primary_key(), col("code", "varchar")]);
        let mut modified = original.clone();
        modified.columns[1].is_unique = true;
        assert_eq!(
            alter_table(&original, &modified),
            ["ALTER TABLE `t` ADD UNIQUE KEY `code` (`code`);"]
        );
        let mut loaded = modified.clone();
        loaded.columns[1].unique_constraint_name = Some("uq_code".into());
        loaded.indexes.push(IndexDesign::named("uq_code").column("code").unique());
        let mut cleared = loaded.clone();
        cleared.columns[1].is_unique = false;
        assert_eq!(
            alter_table(&loaded, &cleared),
            ["ALTER TABLE `t` DROP INDEX `uq_code`;"]
        );
        // Removing the index from the list as well must not drop it twice.
        cleared.indexes.clear();
        assert_eq!(
            alter_table(&loaded, &cleared),
            ["ALTER TABLE `t` DROP INDEX `uq_code`;"]
        );
    }

    #[test]
    fn dropping_an_auto_increment_primary_key_releases_the_column_first() {
        let original = design(vec![col("id", "int").primary_key().auto_increment(), col("a", "int")]);
        let mut modified = original.clone();
        modified.columns[0].is_primary_key = false;
        modified.columns[0].is_auto_increment = false;
        assert_eq!(
            alter_table(&original, &modified),
            ["ALTER TABLE `t` MODIFY COLUMN `id` int NOT NULL, DROP PRIMARY KEY;"]
        );
    }

    #[test]
    fn composite_primary_key_replaces_an_auto_increment_key_in_one_go() {
        let original = design(vec![col("id", "int").primary_key().auto_increment(), col("a", "int")]);
        let mut modified = original.clone();
        modified.columns[1].is_primary_key = true;
        assert_eq!(
            alter_table(&original, &modified),
            [
                "ALTER TABLE `t` MODIFY COLUMN `id` int NOT NULL, DROP PRIMARY KEY;",
                "ALTER TABLE `t` MODIFY COLUMN `id` int NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (`id`, `a`);",
            ]
        );
    }

    #[test]
    fn indexes_foreign_keys_and_checks_diff_by_definition() {
        let mut original = design(vec![col("id", "int").primary_key(), col("a", "int"), col("b", "int")]);
        original.indexes.push(IndexDesign::named("ix_a").column("a"));
        original.indexes.push(IndexDesign::named("ix_b").column("b"));
        original.foreign_keys.push(
            ForeignKeyDesign::new().named("fk_a").column("a").references("p").referenced_column("id"),
        );
        original.check_constraints.push(CheckConstraintDesign::new().named("c1").expression("a > 0"));
        let mut modified = original.clone();
        modified.indexes[0].columns = vec!["a".into(), "b".into()];
        modified.indexes.remove(1);
        modified.foreign_keys[0].on_delete = ForeignKeyAction::Cascade;
        modified.check_constraints.clear();
        assert_eq!(
            alter_table(&original, &modified),
            [
                "ALTER TABLE `t` DROP FOREIGN KEY `fk_a`;",
                "ALTER TABLE `t` DROP CHECK `c1`;",
                "ALTER TABLE `t` DROP INDEX `ix_a`;",
                "ALTER TABLE `t` DROP INDEX `ix_b`;",
                "ALTER TABLE `t` ADD KEY `ix_a` (`a`, `b`);",
                "ALTER TABLE `t` ADD CONSTRAINT `fk_a` FOREIGN KEY (`a`) REFERENCES `p` (`id`) ON DELETE CASCADE;",
            ]
        );
    }

    #[test]
    fn special_indexes_and_table_options() {
        let mut original = design(vec![col("id", "int").primary_key(), col("body", "text"), col("n", "varchar")]);
        original.options = TableOptions::mysql();
        let mut modified = original.clone();
        modified.indexes.push(IndexDesign { index_type: "FULLTEXT".into(), ..IndexDesign::named("ft").column("body") });
        let mut desc = IndexDesign::named("ix_n").column("n(5)").column("(lower(`n`))");
        desc.column_descending = vec![true, false];
        modified.indexes.push(desc);
        modified.comment = Some("it's".into());
        modified.options.engine = Some("MyISAM".into());
        assert_eq!(
            alter_table(&original, &modified),
            [
                "ALTER TABLE `t` ADD FULLTEXT KEY `ft` (`body`);",
                "ALTER TABLE `t` ADD KEY `ix_n` (`n`(5) DESC, (lower(`n`)));",
                "ALTER TABLE `t` ENGINE=MyISAM COMMENT='it''s';",
            ]
        );
    }
}
