use crate::ObjectsPanelObjectRef;

/// Parse a Redis database node label (for example `db0`) into its numeric index.
pub fn parse_redis_database_index(database_name: &str) -> Option<u16> {
    database_name
        .strip_prefix("db")
        .and_then(|value| value.parse::<u16>().ok())
}

/// Build a canonical qualified object identifier for UI and version-history actions.
pub fn format_qualified_object_name(object_ref: &ObjectsPanelObjectRef) -> String {
    let function_signature_suffix = object_ref
        .signature
        .as_deref()
        .filter(|signature| !signature.is_empty())
        .map(|signature| format!("({signature})"))
        .unwrap_or_default();

    let qualified_base = match (object_ref.database.as_deref(), object_ref.schema.as_deref()) {
        (Some(database), Some(schema)) => {
            format!("{}.{}.{}", database, schema, object_ref.name)
        }
        (None, Some(schema)) => format!("{}.{}", schema, object_ref.name),
        (Some(database), None) => format!("{}.{}", database, object_ref.name),
        (None, None) => object_ref.name.clone(),
    };

    match (
        object_ref.kind_id.as_str(),
        function_signature_suffix.is_empty(),
    ) {
        ("function" | "procedure", false) => {
            format!("{}{}", qualified_base, function_signature_suffix)
        }
        _ => qualified_base,
    }
}

#[cfg(test)]
mod tests {
    use super::{format_qualified_object_name, parse_redis_database_index};
    use crate::ObjectsPanelObjectRef;

    fn object_ref(kind_id: &str, name: &str) -> ObjectsPanelObjectRef {
        ObjectsPanelObjectRef::new(kind_id, name)
    }

    #[test]
    fn parse_redis_database_index_handles_expected_formats() {
        assert_eq!(parse_redis_database_index("db0"), Some(0));
        assert_eq!(parse_redis_database_index("db15"), Some(15));
        assert_eq!(parse_redis_database_index("db"), None);
        assert_eq!(parse_redis_database_index("database1"), None);
        assert_eq!(parse_redis_database_index("db99999"), None);
    }

    #[test]
    fn format_qualified_object_name_formats_database_schema_and_signature() {
        let table_name = format_qualified_object_name(
            &object_ref("table", "users")
                .with_database_option(Some("mydb".to_string()))
                .with_schema_option(Some("public".to_string())),
        );
        assert_eq!(table_name, "mydb.public.users");

        let schema_only = format_qualified_object_name(
            &object_ref("table", "users").with_schema_option(Some("public".to_string())),
        );
        assert_eq!(schema_only, "public.users");

        let db_only = format_qualified_object_name(
            &object_ref("table", "users").with_database_option(Some("mydb".to_string())),
        );
        assert_eq!(db_only, "mydb.users");

        let function_name = format_qualified_object_name(
            &object_ref("function", "do_work")
                .with_database_option(Some("mydb".to_string()))
                .with_schema_option(Some("public".to_string()))
                .with_signature_option(Some("integer,text".to_string())),
        );
        assert_eq!(function_name, "mydb.public.do_work(integer,text)");

        let function_without_signature = format_qualified_object_name(
            &object_ref("function", "do_work")
                .with_schema_option(Some("public".to_string()))
                .with_signature_option(Some(String::new())),
        );
        assert_eq!(function_without_signature, "public.do_work");
    }
}
