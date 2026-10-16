//! Column design model

use uuid::Uuid;
use zqlz_core::ColumnInfo;

use super::DatabaseDialect;

/// Column design model for table designer
#[derive(Debug, Clone)]
pub struct ColumnDesign {
    /// Stable identifier for tracking renames
    pub column_id: Uuid,
    /// Column name
    pub name: String,
    /// Data type (e.g., "INTEGER", "VARCHAR", "TEXT")
    pub data_type: String,
    /// Length for types that support it (e.g., VARCHAR(255))
    pub length: Option<u32>,
    /// Scale for DECIMAL types
    pub scale: Option<u32>,
    /// Whether NULL values are allowed
    pub nullable: bool,
    /// Default value expression
    pub default_value: Option<String>,
    /// Is this column part of the primary key?
    pub is_primary_key: bool,
    /// Is this part of a composite primary key?
    pub is_part_of_composite_pk: bool,
    /// Is this column auto-incrementing?
    pub is_auto_increment: bool,
    /// Is this column unique?
    pub is_unique: bool,
    /// Name of the existing single-column unique constraint or index backing
    /// `is_unique`, as introspected. `None` for new columns and designs that
    /// were not loaded from a database; dropping it must use this name because
    /// databases name it by their own default (`{table}_{col}_key`) or by the
    /// user's choice.
    pub unique_constraint_name: Option<String>,
    /// Column ordinal position
    pub ordinal: usize,
    /// Comment/description
    pub comment: Option<String>,
    /// Virtual/computed column expression (for SQLite GENERATED columns)
    pub generated_expression: Option<String>,
    /// Whether the generated column is STORED or VIRTUAL
    pub generated_stored: bool,
    /// Column character set (MySQL); `None` inherits the table default.
    pub charset: Option<String>,
    /// Column collation (MySQL); `None` inherits the table default.
    pub collation: Option<String>,
    /// `ON UPDATE` expression (MySQL), e.g. `CURRENT_TIMESTAMP`.
    pub on_update: Option<String>,
    /// INVISIBLE column (MySQL 8).
    pub invisible: bool,
}

impl ColumnDesign {
    /// Create a new empty column design
    pub fn new(ordinal: usize) -> Self {
        Self {
            column_id: Uuid::new_v4(),
            name: String::new(),
            data_type: "TEXT".to_string(),
            length: None,
            scale: None,
            nullable: true,
            default_value: None,
            is_primary_key: false,
            is_part_of_composite_pk: false,
            is_auto_increment: false,
            is_unique: false,
            unique_constraint_name: None,
            ordinal,
            comment: None,
            generated_expression: None,
            generated_stored: false,
            charset: None,
            collation: None,
            on_update: None,
            invisible: false,
        }
    }

    /// Create a column with a specific name
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::new(0)
        }
    }

    /// Create from existing column info
    pub fn from_column_info(info: &ColumnInfo) -> Self {
        Self::from_column_info_for(info, DatabaseDialect::Sqlite)
    }

    /// Create from existing column info, honouring dialect specifics.
    pub fn from_column_info_for(info: &ColumnInfo, dialect: DatabaseDialect) -> Self {
        // Drivers report the declared type with its modifiers (SQLite's PRAGMA,
        // MySQL's COLUMN_TYPE, Postgres' format_type), so the modifiers are taken
        // from that string; reusing `max_length` too would emit `VARCHAR(255)(255)`.
        let (data_type, length, scale) = match split_type_modifiers(&info.data_type) {
            Some((base, length, scale)) => (base, Some(length), scale),
            // Modifiers the Length/Scale fields can't express (`enum('a')`,
            // `timestamp(3) with time zone`) stay inside the type verbatim.
            None if info.data_type.contains('(') => (info.data_type.clone(), None, None),
            // MySQL's COLUMN_TYPE already carries every declared modifier; the
            // reported max length of `text`/`tinytext`/`blob` is a storage
            // limit, and re-emitting it (`tinytext(255)`) would be invalid or
            // silently change the type.
            None if dialect == DatabaseDialect::Mysql => (info.data_type.clone(), None, None),
            None => (
                info.data_type.clone(),
                info.max_length.map(|l| l as u32),
                // Postgres reports scale 0 for integer types; a bare 0 with no
                // length is not a modifier anyone declared.
                info.scale
                    .filter(|&s| s > 0 || info.max_length.is_some())
                    .map(|s| s as u32),
            ),
        };
        Self {
            column_id: Uuid::new_v4(),
            name: info.name.clone(),
            data_type,
            length,
            scale,
            nullable: info.nullable,
            default_value: info.default_value.clone(),
            is_primary_key: info.is_primary_key,
            is_part_of_composite_pk: false,
            is_auto_increment: info.is_auto_increment,
            is_unique: info.is_unique,
            unique_constraint_name: None,
            ordinal: info.ordinal,
            comment: info.comment.clone(),
            generated_expression: info.generation_expression.clone(),
            generated_stored: info.is_generated_stored,
            charset: info.charset.clone(),
            collation: info.collation.clone(),
            on_update: None,
            invisible: false,
        }
    }

    /// Builder: set data type
    pub fn data_type(mut self, data_type: impl Into<String>) -> Self {
        self.data_type = data_type.into();
        self
    }

    /// Builder: set as integer type
    pub fn integer(mut self) -> Self {
        self.data_type = "INTEGER".to_string();
        self
    }

    /// Builder: set as text type
    pub fn text(mut self) -> Self {
        self.data_type = "TEXT".to_string();
        self
    }

    /// Builder: set as primary key
    pub fn primary_key(mut self) -> Self {
        self.is_primary_key = true;
        self.nullable = false;
        self
    }

    /// Builder: set as not null
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Builder: set as unique
    pub fn unique(mut self) -> Self {
        self.is_unique = true;
        self
    }

    /// Builder: set as auto increment
    pub fn auto_increment(mut self) -> Self {
        self.is_auto_increment = true;
        self
    }

    /// Builder: set default value
    pub fn default(mut self, value: impl Into<String>) -> Self {
        self.default_value = Some(value.into());
        self
    }

    /// Builder: set length
    pub fn with_length(mut self, length: u32) -> Self {
        self.length = Some(length);
        self
    }
}

/// Splits a numeric `(length[, scale])` modifier off a declared type:
/// `VARCHAR(255)` -> (`VARCHAR`, 255, None), `DECIMAL(4,2)` -> (`DECIMAL`, 4, Some(2)).
/// MySQL attributes that follow the parentheses stay with the base type:
/// `int(10) unsigned zerofill` -> (`int unsigned zerofill`, 10, None).
fn split_type_modifiers(declared: &str) -> Option<(String, u32, Option<u32>)> {
    let declared = declared.trim_end();
    let (head, suffix) = match declared.strip_suffix(')') {
        Some(_) => (declared, ""),
        None => {
            let close = declared.rfind(')')?;
            let suffix = declared[close + 1..].trim();
            let attributes = ["unsigned", "signed", "zerofill"];
            if suffix.is_empty()
                || !suffix
                    .split_whitespace()
                    .all(|word| attributes.iter().any(|a| a.eq_ignore_ascii_case(word)))
            {
                return None;
            }
            (&declared[..=close], suffix)
        }
    };
    let inner = head.strip_suffix(')')?;
    let open = inner.rfind('(')?;
    let base = inner[..open].trim_end();
    if base.is_empty() {
        return None;
    }
    let mut parts = inner[open + 1..].split(',').map(str::trim);
    let length = parts.next()?.parse().ok()?;
    let scale = match parts.next() {
        Some(scale) => Some(scale.parse().ok()?),
        None => None,
    };
    if parts.next().is_some() {
        return None;
    }
    let base = if suffix.is_empty() {
        base.to_string()
    } else {
        format!("{base} {suffix}")
    };
    Some((base, length, scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(declared: &str) -> ColumnDesign {
        ColumnDesign::from_column_info(&ColumnInfo {
            name: "column".to_string(),
            data_type: declared.to_string(),
            ..Default::default()
        })
    }

    #[test]
    fn loading_preserves_declared_type_and_splits_modifiers() {
        for (declared, data_type, length, scale) in [
            ("INT", "INT", None, None),
            ("TIMESTAMP", "TIMESTAMP", None, None),
            ("BLOB SUB_TYPE TEXT", "BLOB SUB_TYPE TEXT", None, None),
            ("VARCHAR(255)", "VARCHAR", Some(255), None),
            ("DECIMAL(4,2)", "DECIMAL", Some(4), Some(2)),
            (
                "character varying (40)",
                "character varying",
                Some(40),
                None,
            ),
            ("enum('a','b')", "enum('a','b')", None, None),
            ("decimal(10,2) unsigned", "decimal unsigned", Some(10), Some(2)),
            ("int(10) unsigned zerofill", "int unsigned zerofill", Some(10), None),
            ("tinyint(1)", "tinyint", Some(1), None),
            (
                "timestamp(3) with time zone",
                "timestamp(3) with time zone",
                None,
                None,
            ),
        ] {
            let column = load(declared);
            assert_eq!(column.data_type, data_type, "{declared}");
            assert_eq!(column.length, length, "{declared}");
            assert_eq!(column.scale, scale, "{declared}");
        }
    }

    #[test]
    fn declared_modifiers_win_over_reported_max_length() {
        let column = ColumnDesign::from_column_info(&ColumnInfo {
            name: "email".to_string(),
            data_type: "varchar(255)".to_string(),
            max_length: Some(255),
            ..Default::default()
        });
        assert_eq!(column.data_type, "varchar");
        assert_eq!(column.length, Some(255));
    }

    #[test]
    fn integer_scale_zero_is_not_shown() {
        let bigint = ColumnDesign::from_column_info(&ColumnInfo {
            name: "customer_id".to_string(),
            data_type: "bigint".to_string(),
            scale: Some(0),
            ..Default::default()
        });
        assert_eq!(bigint.scale, None);

        let money = ColumnDesign::from_column_info(&ColumnInfo {
            name: "credit_limit".to_string(),
            data_type: "positive_money".to_string(),
            scale: Some(2),
            ..Default::default()
        });
        assert_eq!(money.scale, Some(2));
    }
}
