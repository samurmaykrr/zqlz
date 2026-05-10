//! Sequence design models.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceDesign {
    pub schema: Option<String>,
    pub name: String,
    pub owner: Option<String>,
    pub data_type: String,
    pub start_value: i64,
    pub current_value: Option<i64>,
    pub increment_by: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache_size: i64,
    pub cycle: bool,
    pub owned_by_table: Option<String>,
    pub owned_by_column: Option<String>,
    pub comment: Option<String>,
    pub is_new: bool,
}

impl SequenceDesign {
    pub fn new(schema: Option<String>) -> Self {
        Self {
            schema,
            name: String::new(),
            owner: None,
            data_type: "bigint".to_string(),
            start_value: 1,
            current_value: Some(1),
            increment_by: 1,
            min_value: Some(1),
            max_value: Some(9_223_372_036_854_775_807),
            cache_size: 1,
            cycle: false,
            owned_by_table: None,
            owned_by_column: None,
            comment: None,
            is_new: true,
        }
    }

    pub fn validate(&self) -> Option<String> {
        if self.name.trim().is_empty() {
            return Some("Sequence name is required".to_string());
        }
        if self.data_type.trim().is_empty() {
            return Some("Data type is required".to_string());
        }
        if self.increment_by == 0 {
            return Some("Increment cannot be zero".to_string());
        }
        if self.cache_size < 1 {
            return Some("Cache size must be at least 1".to_string());
        }
        if let (Some(min), Some(max)) = (self.min_value, self.max_value)
            && min >= max
        {
            return Some("Minimum value must be less than maximum value".to_string());
        }
        None
    }

    pub fn to_ddl(&self) -> String {
        if self.is_new {
            self.to_create_ddl()
        } else {
            self.to_alter_ddl()
        }
    }

    fn to_create_ddl(&self) -> String {
        let mut ddl = format!(
            "CREATE SEQUENCE {} AS {}\n    INCREMENT BY {}\n    {}\n    {}\n    START WITH {}\n    CACHE {}\n    {};",
            self.qualified_name(),
            self.data_type,
            self.increment_by,
            self.min_clause(),
            self.max_clause(),
            self.start_value,
            self.cache_size,
            if self.cycle { "CYCLE" } else { "NO CYCLE" },
        );
        self.append_metadata_statements(&mut ddl);
        ddl
    }

    fn to_alter_ddl(&self) -> String {
        let mut ddl = format!(
            "ALTER SEQUENCE {} AS {}\n    INCREMENT BY {}\n    {}\n    {}\n    START WITH {}\n    CACHE {}\n    {};",
            self.qualified_name(),
            self.data_type,
            self.increment_by,
            self.min_clause(),
            self.max_clause(),
            self.start_value,
            self.cache_size,
            if self.cycle { "CYCLE" } else { "NO CYCLE" },
        );
        if let Some(current_value) = self.current_value {
            ddl.push_str(&format!(
                "\nSELECT setval('{}', {}, true);",
                self.qualified_name_unquoted().replace('\'', "''"),
                current_value
            ));
        }
        self.append_metadata_statements(&mut ddl);
        ddl
    }

    fn append_metadata_statements(&self, ddl: &mut String) {
        if let (Some(table), Some(column)) = (&self.owned_by_table, &self.owned_by_column)
            && !table.trim().is_empty()
            && !column.trim().is_empty()
        {
            ddl.push_str(&format!(
                "\nALTER SEQUENCE {} OWNED BY {}.{};",
                self.qualified_name(),
                quote_qualified_identifier(table),
                quote_identifier(column)
            ));
        }
        if let Some(comment) = &self.comment {
            ddl.push_str(&format!(
                "\nCOMMENT ON SEQUENCE {} IS '{}';",
                self.qualified_name(),
                comment.replace('\'', "''")
            ));
        }
    }

    fn min_clause(&self) -> String {
        self.min_value
            .map(|value| format!("MINVALUE {}", value))
            .unwrap_or_else(|| "NO MINVALUE".to_string())
    }

    fn max_clause(&self) -> String {
        self.max_value
            .map(|value| format!("MAXVALUE {}", value))
            .unwrap_or_else(|| "NO MAXVALUE".to_string())
    }

    fn qualified_name(&self) -> String {
        match &self.schema {
            Some(schema) if !schema.is_empty() => {
                format!(
                    "{}.{}",
                    quote_identifier(schema),
                    quote_identifier(&self.name)
                )
            }
            _ => quote_identifier(&self.name),
        }
    }

    fn qualified_name_unquoted(&self) -> String {
        match &self.schema {
            Some(schema) if !schema.is_empty() => format!("{}.{}", schema, self.name),
            _ => self.name.clone(),
        }
    }
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_qualified_identifier(identifier: &str) -> String {
    identifier
        .split('.')
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join(".")
}
