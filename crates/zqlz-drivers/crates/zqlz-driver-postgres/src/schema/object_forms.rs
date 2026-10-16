//! Object-form helpers (enum/domain/procedure form specs, form value
//! decoding) shared by the `SchemaIntrospection` object-form methods.

use zqlz_core::{
    Connection, ObjectFormField, ObjectFormFieldKind, ObjectFormMode, ObjectFormSection,
    ObjectFormSpec, ObjectFormValue, ObjectsPanelObjectRef, Result, TypeKind, ZqlzError,
};

use crate::PostgresConnection;

impl PostgresConnection {
    pub(crate) fn postgres_type_kind_id(type_kind: TypeKind) -> &'static str {
        match type_kind {
            TypeKind::Enum => "enum",
            TypeKind::Domain => "domain",
            TypeKind::Range => "range",
            TypeKind::Composite => "composite_type",
            TypeKind::Base => "type",
        }
    }

    pub(crate) fn postgres_type_kind_label(type_kind: TypeKind) -> &'static str {
        match type_kind {
            TypeKind::Enum => "Enum",
            TypeKind::Domain => "Domain",
            TypeKind::Range => "Range",
            TypeKind::Composite => "Composite Type",
            TypeKind::Base => "Type",
        }
    }

    pub(crate) fn postgres_type_object_ref(
        type_kind: TypeKind,
        name: String,
        schema: String,
    ) -> ObjectsPanelObjectRef {
        ObjectsPanelObjectRef::new(Self::postgres_type_kind_id(type_kind), name)
            .with_schema_option(Some(schema))
    }

    pub(crate) fn quote_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    pub(crate) fn qualified_type_name(&self, schema: Option<&str>, name: &str) -> String {
        match schema.filter(|schema| !schema.trim().is_empty()) {
            Some(schema) => format!(
                "{}.{}",
                self.quote_identifier(schema),
                self.quote_identifier(name)
            ),
            None => self.quote_identifier(name),
        }
    }

    pub(crate) fn object_form_string<'a>(
        values: &'a std::collections::BTreeMap<String, ObjectFormValue>,
        id: &str,
    ) -> &'a str {
        values
            .get(id)
            .and_then(ObjectFormValue::as_string)
            .unwrap_or_default()
            .trim()
    }

    pub(crate) fn object_form_bool(
        values: &std::collections::BTreeMap<String, ObjectFormValue>,
        id: &str,
    ) -> bool {
        values
            .get(id)
            .and_then(ObjectFormValue::as_bool)
            .unwrap_or(false)
    }

    pub(crate) fn object_form_string_list(
        values: &std::collections::BTreeMap<String, ObjectFormValue>,
        id: &str,
    ) -> Vec<String> {
        values
            .get(id)
            .and_then(ObjectFormValue::as_string_list)
            .unwrap_or_default()
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect()
    }

    pub(crate) fn validate_unique_values(kind: &str, values: &[String]) -> Result<()> {
        let mut seen = std::collections::BTreeSet::new();
        for value in values {
            if !seen.insert(value.as_str()) {
                return Err(ZqlzError::Schema(format!(
                    "{} contains duplicate value '{}'",
                    kind, value
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn enum_labels_to_append(labels: &[String], current_labels: &[String]) -> Vec<String> {
        if labels.starts_with(current_labels) {
            return labels.iter().skip(current_labels.len()).cloned().collect();
        }

        let current_labels: std::collections::BTreeSet<&str> =
            current_labels.iter().map(String::as_str).collect();
        labels
            .iter()
            .filter(|label| !current_labels.contains(label.as_str()))
            .cloned()
            .collect()
    }

    pub(crate) fn enum_form_spec(
        mode: ObjectFormMode,
        schema: Option<String>,
        name: Option<String>,
        labels: Vec<String>,
    ) -> ObjectFormSpec {
        let title = match mode {
            ObjectFormMode::Create => "New Enum",
            ObjectFormMode::Edit => "Design Enum",
            ObjectFormMode::Drop => "Drop Enum",
        };

        let name_field = ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
            .required()
            .default_value(ObjectFormValue::String(name.unwrap_or_default()));
        let name_field = if matches!(mode, ObjectFormMode::Create) {
            name_field
        } else {
            name_field.read_only()
        };

        ObjectFormSpec::new("enum", mode, title).sections(vec![ObjectFormSection::new(vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .placeholder("public")
                .default_value(ObjectFormValue::String(
                    schema.unwrap_or_else(|| "public".to_string()),
                )),
            name_field,
            ObjectFormField::new("labels", "Labels", ObjectFormFieldKind::StringList)
                .required()
                .help_text("One enum label per line")
                .default_value(ObjectFormValue::StringList(labels)),
            ObjectFormField::new("comment", "Comment", ObjectFormFieldKind::TextArea),
        ])])
    }

    pub(crate) fn domain_form_spec(
        mode: ObjectFormMode,
        schema: Option<String>,
        name: Option<String>,
        base_type: Option<String>,
        not_null: bool,
        default_value: Option<String>,
        check_expression: Option<String>,
    ) -> ObjectFormSpec {
        let title = match mode {
            ObjectFormMode::Create => "New Domain",
            ObjectFormMode::Edit => "Design Domain",
            ObjectFormMode::Drop => "Drop Domain",
        };

        let name_field = ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
            .required()
            .default_value(ObjectFormValue::String(name.unwrap_or_default()));
        let name_field = if matches!(mode, ObjectFormMode::Create) {
            name_field
        } else {
            name_field.read_only()
        };

        ObjectFormSpec::new("domain", mode, title).sections(vec![ObjectFormSection::new(vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .placeholder("public")
                .default_value(ObjectFormValue::String(
                    schema.unwrap_or_else(|| "public".to_string()),
                )),
            name_field,
            ObjectFormField::new("base_type", "Base Type", ObjectFormFieldKind::Text)
                .required()
                .placeholder("text")
                .default_value(ObjectFormValue::String(base_type.unwrap_or_default())),
            ObjectFormField::new("not_null", "Not Null", ObjectFormFieldKind::Checkbox)
                .default_value(ObjectFormValue::Bool(not_null)),
            ObjectFormField::new("default", "Default", ObjectFormFieldKind::SqlExpression)
                .default_value(ObjectFormValue::String(default_value.unwrap_or_default())),
            ObjectFormField::new("check", "Check", ObjectFormFieldKind::SqlExpression)
                .placeholder("VALUE <> ''")
                .default_value(ObjectFormValue::String(
                    check_expression.unwrap_or_default(),
                )),
            ObjectFormField::new("comment", "Comment", ObjectFormFieldKind::TextArea),
        ])])
    }

    pub(crate) fn procedure_form_spec(
        mode: ObjectFormMode,
        schema: Option<String>,
        name: Option<String>,
    ) -> ObjectFormSpec {
        let title = match mode {
            ObjectFormMode::Create => "New Procedure",
            ObjectFormMode::Edit => "Design Procedure",
            ObjectFormMode::Drop => "Drop Procedure",
        };

        ObjectFormSpec::new("procedure", mode, title).sections(vec![ObjectFormSection::new(vec![
            ObjectFormField::new("schema", "Schema", ObjectFormFieldKind::Text)
                .placeholder("public")
                .default_value(ObjectFormValue::String(
                    schema.unwrap_or_else(|| "public".to_string()),
                )),
            ObjectFormField::new("name", "Name", ObjectFormFieldKind::Text)
                .required()
                .default_value(ObjectFormValue::String(name.unwrap_or_default())),
            ObjectFormField::new("arguments", "Arguments", ObjectFormFieldKind::StringList)
                .help_text("One argument per line, e.g. user_id integer"),
            ObjectFormField::new("language", "Language", ObjectFormFieldKind::Text)
                .required()
                .placeholder("plpgsql")
                .default_value(ObjectFormValue::String("plpgsql".to_string())),
            ObjectFormField::new("body", "Body", ObjectFormFieldKind::SqlExpression)
                .required()
                .default_value(ObjectFormValue::String(
                    "BEGIN\n    -- procedure body\nEND".to_string(),
                )),
            ObjectFormField::new("comment", "Comment", ObjectFormFieldKind::TextArea),
        ])])
    }

}
