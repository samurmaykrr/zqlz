use std::sync::Arc;

use thiserror::Error;
use zqlz_core::{Connection, DatabaseObject, ObjectType};

#[derive(Debug, Clone)]
pub struct ObjectDefinitionRequest {
    pub object_type: ObjectType,
    pub object_name: String,
    pub object_schema: Option<String>,
    pub signature: Option<String>,
}

impl ObjectDefinitionRequest {
    pub fn new(object_type: ObjectType, object_name: impl Into<String>) -> Self {
        Self {
            object_type,
            object_name: object_name.into(),
            object_schema: None,
            signature: None,
        }
    }

    pub fn with_schema(mut self, object_schema: Option<String>) -> Self {
        self.object_schema = object_schema;
        self
    }

    pub fn with_signature(mut self, signature: Option<String>) -> Self {
        self.signature = signature;
        self
    }
}

#[derive(Debug, Error)]
pub enum ObjectDefinitionError {
    #[error("This connection does not support schema introspection")]
    SchemaIntrospectionNotSupported,
    #[error("{object_type_label} definitions are not supported by the '{driver}' driver")]
    UnsupportedByDriver {
        object_type_label: &'static str,
        driver: String,
    },
    #[error("{0}")]
    GenerationFailed(String),
}

pub async fn fetch_object_definition(
    connection: &Arc<dyn Connection>,
    request: &ObjectDefinitionRequest,
) -> Result<String, ObjectDefinitionError> {
    let Some(schema_introspection) = connection.as_schema_introspection() else {
        return Err(ObjectDefinitionError::SchemaIntrospectionNotSupported);
    };

    let object = DatabaseObject {
        object_type: request.object_type,
        schema: request.object_schema.clone(),
        name: request.object_name.clone(),
        signature: request.signature.clone(),
    };

    schema_introspection
        .generate_ddl(&object)
        .await
        .map_err(|error| {
            if matches!(
                request.object_type,
                ObjectType::Function | ObjectType::Procedure
            ) && connection.dialect_id() == Some("sqlite")
            {
                return ObjectDefinitionError::UnsupportedByDriver {
                    object_type_label: object_type_label(request.object_type),
                    driver: connection.dialect_id().unwrap_or("unknown").to_string(),
                };
            }

            ObjectDefinitionError::GenerationFailed(error.to_string())
        })
}

fn object_type_label(object_type: ObjectType) -> &'static str {
    match object_type {
        ObjectType::Function => "Function",
        ObjectType::Procedure => "Procedure",
        ObjectType::Trigger => "Trigger",
        ObjectType::Event => "Event",
        ObjectType::View => "View",
        ObjectType::Table => "Table",
        ObjectType::Database => "Database",
        ObjectType::Schema => "Schema",
        ObjectType::MaterializedView => "Materialized view",
        ObjectType::Index => "Index",
        ObjectType::Constraint => "Constraint",
        ObjectType::Sequence => "Sequence",
        ObjectType::Type => "Type",
    }
}

#[cfg(test)]
mod tests {
    use super::object_type_label;
    use zqlz_core::ObjectType;

    #[test]
    fn object_type_labels_match_ui_expectations() {
        assert_eq!(object_type_label(ObjectType::Function), "Function");
        assert_eq!(object_type_label(ObjectType::Procedure), "Procedure");
        assert_eq!(object_type_label(ObjectType::Trigger), "Trigger");
        assert_eq!(object_type_label(ObjectType::Event), "Event");
        assert_eq!(object_type_label(ObjectType::View), "View");
    }
}
