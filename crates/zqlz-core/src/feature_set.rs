use crate::{Connection, DriverCategory, ExplainParserKind, ObjectsPanelManifest};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureAvailability {
    pub available: bool,
    pub reason: Option<String>,
}

impl FeatureAvailability {
    pub fn available() -> Self {
        Self {
            available: true,
            reason: None,
        }
    }

    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self {
            available: false,
            reason: Some(reason.into()),
        }
    }

    pub fn reason_or(self, default_reason: impl Into<String>) -> String {
        self.reason.unwrap_or_else(|| default_reason.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryFeatureSet {
    pub execute: FeatureAvailability,
    pub execute_multiple_statements: FeatureAvailability,
    pub explain: FeatureAvailability,
    pub cancel: FeatureAvailability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataEditingFeatureSet {
    pub browse_rows: FeatureAvailability,
    pub edit_cells: FeatureAvailability,
    pub insert_rows: FeatureAvailability,
    pub delete_rows: FeatureAvailability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectFeatureSet {
    pub browse_objects: FeatureAvailability,
    pub create_objects: FeatureAvailability,
    pub edit_objects: FeatureAvailability,
    pub delete_objects: FeatureAvailability,
    pub available_kinds: Vec<String>,
    pub available_actions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreFeatureSet {
    pub key_value: FeatureAvailability,
    pub document: FeatureAvailability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionFeatureSet {
    pub driver_name: String,
    pub driver_category: DriverCategory,
    pub schema_introspection: FeatureAvailability,
    pub query: QueryFeatureSet,
    pub data_editing: DataEditingFeatureSet,
    pub objects: ObjectFeatureSet,
    pub stores: StoreFeatureSet,
}

impl ConnectionFeatureSet {
    pub fn from_connection(
        connection: &dyn Connection,
        objects_panel_manifest: Option<&ObjectsPanelManifest>,
    ) -> Self {
        Self::from_capabilities(ConnectionFeatureInputs {
            driver_name: connection.driver_name().to_string(),
            driver_category: connection.driver_category(),
            has_schema_introspection: connection.as_schema_introspection().is_some(),
            has_key_value_store: connection.as_key_value_store().is_some(),
            has_document_store: connection.as_document_store().is_some(),
            supports_cancel: connection.cancel_handle().is_some(),
            supports_explain: connection.explain_parser_kind() != ExplainParserKind::None,
            objects_panel_manifest,
        })
    }

    fn from_capabilities(inputs: ConnectionFeatureInputs<'_>) -> Self {
        let driver_category = inputs.driver_category;
        let supports_query = matches!(driver_category, DriverCategory::Relational)
            || inputs.has_key_value_store
            || inputs.has_document_store;
        let supports_data_editing = matches!(driver_category, DriverCategory::Relational)
            || inputs.has_key_value_store
            || inputs.has_document_store;

        let available_actions = inputs
            .objects_panel_manifest
            .map(collect_manifest_actions)
            .unwrap_or_default();
        let available_kinds = inputs
            .objects_panel_manifest
            .map(|manifest| {
                manifest
                    .object_kinds
                    .iter()
                    .map(|kind| kind.id.clone())
                    .collect()
            })
            .unwrap_or_default();

        let can_create_objects = available_actions
            .iter()
            .any(|action| action.starts_with("new_"))
            || available_actions
                .iter()
                .any(|action| action.contains("create"));
        let can_edit_objects = available_actions
            .iter()
            .any(|action| matches!(action.as_str(), "edit" | "rename") || action.contains("edit"));
        let can_delete_objects = available_actions.iter().any(|action| {
            matches!(action.as_str(), "delete" | "drop") || action.contains("delete")
        });

        Self {
            driver_name: inputs.driver_name,
            driver_category,
            schema_introspection: if inputs.has_schema_introspection {
                FeatureAvailability::available()
            } else {
                FeatureAvailability::unavailable("Schema introspection is not supported")
            },
            query: QueryFeatureSet {
                execute: if supports_query {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable("Query execution is not supported")
                },
                execute_multiple_statements: if matches!(
                    driver_category,
                    DriverCategory::Relational
                ) {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable(
                        "Multiple SQL statements require a relational connection",
                    )
                },
                explain: if inputs.supports_explain {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable("EXPLAIN is not supported by this connection")
                },
                cancel: if inputs.supports_cancel {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable("Query cancellation is not supported")
                },
            },
            data_editing: DataEditingFeatureSet {
                browse_rows: if matches!(driver_category, DriverCategory::Relational) {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable(
                        "Row browsing requires a relational connection",
                    )
                },
                edit_cells: if supports_data_editing {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable("Cell editing is not supported")
                },
                insert_rows: if supports_data_editing {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable("Insert is not supported")
                },
                delete_rows: if supports_data_editing {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable("Delete is not supported")
                },
            },
            objects: ObjectFeatureSet {
                browse_objects: if inputs.has_schema_introspection
                    || inputs.has_key_value_store
                    || inputs.has_document_store
                {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable("Object browsing is not supported")
                },
                create_objects: if can_create_objects {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable(
                        "Object creation is not advertised by this connection",
                    )
                },
                edit_objects: if can_edit_objects {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable(
                        "Object editing is not advertised by this connection",
                    )
                },
                delete_objects: if can_delete_objects {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable(
                        "Object deletion is not advertised by this connection",
                    )
                },
                available_kinds,
                available_actions,
            },
            stores: StoreFeatureSet {
                key_value: if inputs.has_key_value_store {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable("Key-value browsing is not supported")
                },
                document: if inputs.has_document_store {
                    FeatureAvailability::available()
                } else {
                    FeatureAvailability::unavailable("Document browsing is not supported")
                },
            },
        }
    }
}

struct ConnectionFeatureInputs<'a> {
    driver_name: String,
    driver_category: DriverCategory,
    has_schema_introspection: bool,
    has_key_value_store: bool,
    has_document_store: bool,
    supports_cancel: bool,
    supports_explain: bool,
    objects_panel_manifest: Option<&'a ObjectsPanelManifest>,
}

fn collect_manifest_actions(manifest: &ObjectsPanelManifest) -> Vec<String> {
    let mut actions = std::collections::BTreeSet::new();
    for action in &manifest.toolbar_actions {
        actions.insert(action.id.clone());
    }
    for kind in &manifest.object_kinds {
        for action in &kind.row_actions {
            actions.insert(action.id.clone());
        }
    }
    actions.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relational_features_include_query_explain_cancel_and_objects() {
        let manifest =
            ObjectsPanelManifest::from_data(&crate::ObjectsPanelData::from_table_infos(Vec::new()));

        let features = ConnectionFeatureSet::from_capabilities(ConnectionFeatureInputs {
            driver_name: "postgres".to_string(),
            driver_category: DriverCategory::Relational,
            has_schema_introspection: true,
            has_key_value_store: false,
            has_document_store: false,
            supports_explain: true,
            supports_cancel: true,
            objects_panel_manifest: Some(&manifest),
        });

        assert!(features.query.execute.available);
        assert!(features.query.explain.available);
        assert!(features.query.cancel.available);
        assert!(features.data_editing.browse_rows.available);
        assert!(features.objects.create_objects.available);
        assert!(!features.stores.key_value.available);
    }

    #[test]
    fn feature_availability_reason_uses_default_when_missing() {
        assert_eq!(
            FeatureAvailability::available().reason_or("fallback"),
            "fallback"
        );
        assert_eq!(
            FeatureAvailability::unavailable("blocked").reason_or("fallback"),
            "blocked"
        );
    }

    #[test]
    fn key_value_features_do_not_claim_relational_browse() {
        let features = ConnectionFeatureSet::from_capabilities(ConnectionFeatureInputs {
            driver_name: "redis".to_string(),
            driver_category: DriverCategory::KeyValue,
            has_schema_introspection: false,
            has_key_value_store: true,
            has_document_store: false,
            supports_explain: false,
            supports_cancel: false,
            objects_panel_manifest: None,
        });

        assert!(features.query.execute.available);
        assert!(!features.query.execute_multiple_statements.available);
        assert!(!features.query.explain.available);
        assert!(!features.data_editing.browse_rows.available);
        assert!(features.data_editing.edit_cells.available);
        assert!(features.stores.key_value.available);
        assert!(!features.stores.document.available);
    }

    #[test]
    fn document_features_advertise_document_store() {
        let features = ConnectionFeatureSet::from_capabilities(ConnectionFeatureInputs {
            driver_name: "mongodb".to_string(),
            driver_category: DriverCategory::Document,
            has_schema_introspection: false,
            has_key_value_store: false,
            has_document_store: true,
            supports_explain: false,
            supports_cancel: false,
            objects_panel_manifest: None,
        });

        assert!(features.query.execute.available);
        assert!(features.stores.document.available);
        assert!(!features.data_editing.browse_rows.available);
        assert!(features.data_editing.insert_rows.available);
    }
}
