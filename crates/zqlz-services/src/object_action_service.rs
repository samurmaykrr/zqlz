use zqlz_core::{
    format_qualified_object_name, parse_redis_database_index, FeatureAvailability,
    ObjectFeatureSet, ObjectFormAction, ObjectFormMode, ObjectType,
    ObjectsPanelAction as ManifestAction, ObjectsPanelManifest, ObjectsPanelObjectRef,
};
use zqlz_versioning::DatabaseObjectType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedObjectRef {
    pub name: String,
    pub schema: Option<String>,
    pub signature: Option<String>,
    pub associated_table: Option<String>,
}

pub fn selected_object_ref(object_ref: &ObjectsPanelObjectRef) -> SelectedObjectRef {
    SelectedObjectRef {
        name: object_ref.name.clone(),
        schema: object_ref.schema.clone(),
        signature: object_ref.signature.clone(),
        associated_table: if object_ref.kind_id == "trigger" {
            object_ref.signature.clone()
        } else {
            None
        },
    }
}

pub fn schema_qualified_action_name(object_ref: &ObjectsPanelObjectRef) -> String {
    object_ref
        .schema
        .as_ref()
        .map(|schema| {
            let schema_prefix = format!("{}.", schema);
            if object_ref.name.starts_with(&schema_prefix) {
                object_ref.name.clone()
            } else {
                format!("{}.{}", schema, object_ref.name)
            }
        })
        .unwrap_or_else(|| object_ref.name.clone())
}

pub fn object_type_for_kind_id(kind_id: &str) -> Option<ObjectType> {
    match kind_id {
        "database" => Some(ObjectType::Database),
        "table" => Some(ObjectType::Table),
        "view" => Some(ObjectType::View),
        "function" => Some(ObjectType::Function),
        "procedure" => Some(ObjectType::Procedure),
        "trigger" => Some(ObjectType::Trigger),
        "event" => Some(ObjectType::Event),
        "sequence" => Some(ObjectType::Sequence),
        "index" => Some(ObjectType::Index),
        "constraint" | "foreign_key" => Some(ObjectType::Constraint),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectFormActionPlan {
    pub kind_id: String,
    pub mode: ObjectFormMode,
    pub object_ref: Option<ObjectsPanelObjectRef>,
}

pub fn plan_object_form_action(
    action: &ObjectFormAction,
    object_refs: &[ObjectsPanelObjectRef],
) -> ObjectFormActionPlan {
    let object_ref = if matches!(action.mode, ObjectFormMode::Create) {
        None
    } else {
        object_refs.first().cloned()
    };

    ObjectFormActionPlan {
        kind_id: action.kind_id.clone(),
        mode: action.mode,
        object_ref,
    }
}

pub fn objects_panel_action_feature_availability(
    features: &ObjectFeatureSet,
    action_id: &str,
) -> FeatureAvailability {
    if !features
        .available_actions
        .iter()
        .any(|available_action| available_action == action_id)
    {
        return FeatureAvailability::unavailable(format!(
            "The '{action_id}' action is not advertised by this connection"
        ));
    }

    match object_feature_for_action(action_id) {
        ObjectActionFeature::Browse => features.browse_objects.clone(),
        ObjectActionFeature::Create => features.create_objects.clone(),
        ObjectActionFeature::Edit => features.edit_objects.clone(),
        ObjectActionFeature::Delete => features.delete_objects.clone(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectActionFeature {
    Browse,
    Create,
    Edit,
    Delete,
}

fn object_feature_for_action(action_id: &str) -> ObjectActionFeature {
    if action_id.starts_with("new_") || action_id.contains("create") {
        return ObjectActionFeature::Create;
    }

    if matches!(action_id, "delete" | "drop" | "empty") || action_id.contains("delete") {
        return ObjectActionFeature::Delete;
    }

    if matches!(action_id, "design" | "edit" | "rename" | "duplicate") || action_id.contains("edit")
    {
        return ObjectActionFeature::Edit;
    }

    ObjectActionFeature::Browse
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionRegistryError {
    pub action_id: String,
    pub kind_id: String,
}

#[derive(Debug, Clone)]
pub struct ObjectsPanelActionRegistry {
    versioned_object_bindings:
        std::collections::BTreeMap<(&'static str, &'static str), DatabaseObjectType>,
    empty_selection_bindings: std::collections::BTreeMap<&'static str, EmptySelectionActionBinding>,
    selection_bindings:
        std::collections::BTreeMap<(&'static str, &'static str), SelectionActionBinding>,
}

impl ObjectsPanelActionRegistry {
    pub fn new() -> Self {
        let mut versioned_object_bindings = std::collections::BTreeMap::new();
        versioned_object_bindings.insert(("view_history", "table"), DatabaseObjectType::Table);
        versioned_object_bindings.insert(
            ("view_history", "partitioned_table"),
            DatabaseObjectType::Table,
        );
        versioned_object_bindings
            .insert(("view_history", "foreign_table"), DatabaseObjectType::Table);
        versioned_object_bindings.insert(("view_history", "view"), DatabaseObjectType::View);
        versioned_object_bindings.insert(
            ("view_history", "materialized_view"),
            DatabaseObjectType::View,
        );
        versioned_object_bindings
            .insert(("view_history", "function"), DatabaseObjectType::Function);
        versioned_object_bindings
            .insert(("view_history", "procedure"), DatabaseObjectType::Procedure);
        versioned_object_bindings.insert(("view_history", "trigger"), DatabaseObjectType::Trigger);
        versioned_object_bindings.insert(("view_history", "event"), DatabaseObjectType::Event);

        let mut empty_selection_bindings = std::collections::BTreeMap::new();
        empty_selection_bindings.insert("refresh", EmptySelectionActionBinding::Refresh);
        empty_selection_bindings.insert("new_table", EmptySelectionActionBinding::NewTable);
        empty_selection_bindings.insert("new_view", EmptySelectionActionBinding::NewView);
        empty_selection_bindings.insert("new_trigger", EmptySelectionActionBinding::NewTrigger);
        empty_selection_bindings.insert(
            "new_function",
            EmptySelectionActionBinding::OpenObjectForm {
                kind_id: "function",
                mode: ObjectFormMode::Create,
            },
        );
        empty_selection_bindings.insert(
            "new_procedure",
            EmptySelectionActionBinding::OpenObjectForm {
                kind_id: "procedure",
                mode: ObjectFormMode::Create,
            },
        );
        empty_selection_bindings.insert(
            "new_database",
            EmptySelectionActionBinding::OpenObjectForm {
                kind_id: "database",
                mode: ObjectFormMode::Create,
            },
        );
        empty_selection_bindings.insert(
            "new_event",
            EmptySelectionActionBinding::OpenObjectForm {
                kind_id: "event",
                mode: ObjectFormMode::Create,
            },
        );
        empty_selection_bindings.insert(
            "new_enum",
            EmptySelectionActionBinding::OpenObjectForm {
                kind_id: "enum",
                mode: ObjectFormMode::Create,
            },
        );
        empty_selection_bindings.insert(
            "new_domain",
            EmptySelectionActionBinding::OpenObjectForm {
                kind_id: "domain",
                mode: ObjectFormMode::Create,
            },
        );
        empty_selection_bindings.insert(
            "import",
            EmptySelectionActionBinding::ImportWithoutSelection,
        );
        empty_selection_bindings.insert("export", EmptySelectionActionBinding::Export);

        let mut selection_bindings = std::collections::BTreeMap::new();
        for kind_id in TABLE_KIND_IDS {
            selection_bindings.insert(("open", *kind_id), SelectionActionBinding::OpenTable);
            selection_bindings.insert(("design", *kind_id), SelectionActionBinding::DesignTable);
            selection_bindings.insert(("rename", *kind_id), SelectionActionBinding::RenameTable);
            selection_bindings.insert(("delete", *kind_id), SelectionActionBinding::DeleteTable);
            selection_bindings.insert(
                ("duplicate", *kind_id),
                SelectionActionBinding::DuplicateTable,
            );
            selection_bindings.insert(("empty", *kind_id), SelectionActionBinding::EmptyTable);
            selection_bindings.insert(("import", *kind_id), SelectionActionBinding::ImportTable);
            selection_bindings.insert(("export", *kind_id), SelectionActionBinding::Export);
            selection_bindings.insert(
                ("dump_sql_structure_data", *kind_id),
                SelectionActionBinding::DumpSqlStructureData,
            );
            selection_bindings.insert(
                ("dump_sql_structure", *kind_id),
                SelectionActionBinding::DumpSqlStructure,
            );
            selection_bindings.insert(("copy_name", *kind_id), SelectionActionBinding::CopyName);
            selection_bindings.insert(
                ("copy_qualified_name", *kind_id),
                SelectionActionBinding::CopyQualifiedName,
            );
            selection_bindings.insert(
                ("view_history", *kind_id),
                SelectionActionBinding::ViewHistory,
            );
        }

        for kind_id in VIEW_KIND_IDS {
            selection_bindings.insert(("open", *kind_id), SelectionActionBinding::OpenView);
            selection_bindings.insert(("design", *kind_id), SelectionActionBinding::DesignView);
            selection_bindings.insert(("rename", *kind_id), SelectionActionBinding::RenameView);
            selection_bindings.insert(("delete", *kind_id), SelectionActionBinding::DeleteView);
            selection_bindings.insert(
                ("duplicate", *kind_id),
                SelectionActionBinding::DuplicateView,
            );
            selection_bindings.insert(("export", *kind_id), SelectionActionBinding::Export);
            selection_bindings.insert(("copy_name", *kind_id), SelectionActionBinding::CopyName);
            selection_bindings.insert(
                ("copy_qualified_name", *kind_id),
                SelectionActionBinding::CopyQualifiedName,
            );
            selection_bindings.insert(
                ("view_history", *kind_id),
                SelectionActionBinding::ViewHistory,
            );
        }

        for kind_id in ROUTINE_KIND_IDS {
            selection_bindings.insert(("open", *kind_id), SelectionActionBinding::OpenRoutine);
            selection_bindings.insert(("design", *kind_id), SelectionActionBinding::DesignRoutine);
            selection_bindings.insert(("copy_name", *kind_id), SelectionActionBinding::CopyName);
            selection_bindings.insert(
                ("copy_qualified_name", *kind_id),
                SelectionActionBinding::CopyQualifiedName,
            );
            selection_bindings.insert(
                ("view_history", *kind_id),
                SelectionActionBinding::ViewHistory,
            );
        }

        selection_bindings.insert(("open", "trigger"), SelectionActionBinding::OpenTriggerDdl);
        selection_bindings.insert(("design", "trigger"), SelectionActionBinding::DesignTrigger);
        selection_bindings.insert(("delete", "trigger"), SelectionActionBinding::DeleteTrigger);
        selection_bindings.insert(("copy_name", "trigger"), SelectionActionBinding::CopyName);
        selection_bindings.insert(
            ("copy_qualified_name", "trigger"),
            SelectionActionBinding::CopyQualifiedName,
        );
        selection_bindings.insert(
            ("view_history", "trigger"),
            SelectionActionBinding::ViewHistory,
        );
        selection_bindings.insert(("open", "sequence"), SelectionActionBinding::OpenSequence);
        selection_bindings.insert(
            ("design", "sequence"),
            SelectionActionBinding::DesignSequence,
        );
        selection_bindings.insert(("copy_name", "sequence"), SelectionActionBinding::CopyName);
        selection_bindings.insert(
            ("copy_qualified_name", "sequence"),
            SelectionActionBinding::CopyQualifiedName,
        );

        for kind_id in METADATA_KIND_IDS {
            selection_bindings.insert(("open", *kind_id), SelectionActionBinding::OpenGenericDdl);
            selection_bindings.insert(("design", *kind_id), SelectionActionBinding::OpenGenericDdl);
            selection_bindings.insert(("copy_name", *kind_id), SelectionActionBinding::CopyName);
            selection_bindings.insert(
                ("copy_qualified_name", *kind_id),
                SelectionActionBinding::CopyQualifiedName,
            );
        }
        selection_bindings.insert(("open", "database"), SelectionActionBinding::OpenGenericDdl);
        selection_bindings.insert(("copy_name", "database"), SelectionActionBinding::CopyName);
        selection_bindings.insert(
            ("copy_qualified_name", "database"),
            SelectionActionBinding::CopyQualifiedName,
        );
        selection_bindings.insert(("open", "event"), SelectionActionBinding::OpenGenericDdl);
        selection_bindings.insert(
            ("design", "event"),
            SelectionActionBinding::OpenObjectForm {
                mode: ObjectFormMode::Edit,
            },
        );
        selection_bindings.insert(
            ("delete", "event"),
            SelectionActionBinding::OpenObjectForm {
                mode: ObjectFormMode::Drop,
            },
        );
        selection_bindings.insert(("copy_name", "event"), SelectionActionBinding::CopyName);
        selection_bindings.insert(
            ("copy_qualified_name", "event"),
            SelectionActionBinding::CopyQualifiedName,
        );
        selection_bindings.insert(
            ("view_history", "event"),
            SelectionActionBinding::ViewHistory,
        );
        for kind_id in ["enum", "domain"] {
            selection_bindings.insert(
                ("design", kind_id),
                SelectionActionBinding::OpenObjectForm {
                    mode: ObjectFormMode::Edit,
                },
            );
            selection_bindings.insert(
                ("delete", kind_id),
                SelectionActionBinding::OpenObjectForm {
                    mode: ObjectFormMode::Drop,
                },
            );
            selection_bindings.insert(("copy_name", kind_id), SelectionActionBinding::CopyName);
            selection_bindings.insert(
                ("copy_qualified_name", kind_id),
                SelectionActionBinding::CopyQualifiedName,
            );
        }
        selection_bindings.insert(("open", "extension"), SelectionActionBinding::OpenExtension);
        selection_bindings.insert(
            ("design", "extension"),
            SelectionActionBinding::OpenExtension,
        );

        selection_bindings.insert(("delete", "key"), SelectionActionBinding::DeleteKey);
        selection_bindings.insert(("copy_name", "key"), SelectionActionBinding::CopyName);
        selection_bindings.insert(
            ("copy_qualified_name", "key"),
            SelectionActionBinding::CopyQualifiedName,
        );
        selection_bindings.insert(
            ("open", "redis_database"),
            SelectionActionBinding::OpenRedisDatabase,
        );
        selection_bindings.insert(
            ("open", "document_collection"),
            SelectionActionBinding::OpenDocumentCollection,
        );

        Self {
            versioned_object_bindings,
            empty_selection_bindings,
            selection_bindings,
        }
    }

    pub fn resolve_versioned_object_type(
        &self,
        action_id: &str,
        kind_id: &str,
    ) -> Result<DatabaseObjectType, ActionRegistryError> {
        self.versioned_object_bindings
            .get(&(action_id, kind_id))
            .cloned()
            .ok_or_else(|| ActionRegistryError {
                action_id: action_id.to_string(),
                kind_id: kind_id.to_string(),
            })
    }

    pub fn has_binding_for_action_id(&self, action_id: &str) -> bool {
        self.empty_selection_bindings.contains_key(action_id)
            || self
                .selection_bindings
                .keys()
                .any(|(bound_action_id, _)| bound_action_id == &action_id)
    }

    /// Resolves an `InvokeAction` through the registry boundary so the event
    /// layer can depend on one typed entry point instead of a separate planner
    /// helper.
    pub fn resolve_objects_panel_action(
        &self,
        action_id: &str,
        object_refs: &[ObjectsPanelObjectRef],
        database_name: Option<String>,
    ) -> ResolvedObjectsPanelAction {
        self.resolve_objects_panel_action_with_manifest(action_id, object_refs, database_name, None)
    }

    pub fn resolve_objects_panel_action_with_manifest(
        &self,
        action_id: &str,
        object_refs: &[ObjectsPanelObjectRef],
        database_name: Option<String>,
        manifest: Option<&ObjectsPanelManifest>,
    ) -> ResolvedObjectsPanelAction {
        if let Some(action) = manifest
            .and_then(|manifest| manifest_action_for_context(manifest, action_id, object_refs))
            .and_then(|action| action.object_form.as_ref())
        {
            return manifest_object_form_action(action, object_refs);
        }

        if object_refs.is_empty() {
            return self
                .empty_selection_bindings
                .get(action_id)
                .map(|binding| binding.clone().into_resolved_action())
                .unwrap_or_else(|| ResolvedObjectsPanelAction::UnknownAction {
                    action_id: action_id.to_string(),
                    object_count: 0,
                });
        }

        let Some(first_object_ref) = object_refs.first() else {
            return ResolvedObjectsPanelAction::Noop;
        };

        let Some(binding) = self
            .selection_bindings
            .get(&(action_id, first_object_ref.kind_id.as_str()))
            .cloned()
        else {
            if action_id == "view_history" {
                return ResolvedObjectsPanelAction::UnsupportedViewHistoryKind {
                    action_id: action_id.to_string(),
                    object_kind: first_object_ref.kind_id.clone(),
                };
            }

            if let Some(binding) = self.empty_selection_bindings.get(action_id) {
                return binding.clone().into_resolved_action();
            }

            return ResolvedObjectsPanelAction::UnknownAction {
                action_id: action_id.to_string(),
                object_count: object_refs.len(),
            };
        };

        binding.into_resolved_action(self, action_id, object_refs, database_name)
    }
}

impl Default for ObjectsPanelActionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

const TABLE_KIND_IDS: &[&str] = &["table", "partitioned_table", "foreign_table"];
const VIEW_KIND_IDS: &[&str] = &["view", "materialized_view"];
const ROUTINE_KIND_IDS: &[&str] = &["function", "procedure"];
const METADATA_KIND_IDS: &[&str] = &[
    "type",
    "range",
    "composite_type",
    "index",
    "schema",
    "extension",
    "foreign_server",
    "foreign_data_wrapper",
    "policy",
    "publication",
    "subscription",
    "event_trigger",
    "language",
    "collation",
    "tablespace",
    "constraint",
    "foreign_key",
    "partition",
    "engine",
    "charset",
    "user",
    "role",
    "grant",
    "plugin",
    "variable",
    "status_variable",
    "process",
    "replica_status",
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum EmptySelectionActionBinding {
    Refresh,
    NewTable,
    NewView,
    NewTrigger,
    OpenObjectForm {
        kind_id: &'static str,
        mode: ObjectFormMode,
    },
    ImportWithoutSelection,
    Export,
}

impl EmptySelectionActionBinding {
    fn into_resolved_action(self) -> ResolvedObjectsPanelAction {
        match self {
            Self::Refresh => ResolvedObjectsPanelAction::Refresh,
            Self::NewTable => ResolvedObjectsPanelAction::NewTable,
            Self::NewView => ResolvedObjectsPanelAction::NewView,
            Self::NewTrigger => ResolvedObjectsPanelAction::NewTrigger,
            Self::OpenObjectForm { kind_id, mode } => ResolvedObjectsPanelAction::OpenObjectForm {
                kind_id: kind_id.to_string(),
                mode,
                object_ref: None,
            },
            Self::ImportWithoutSelection => ResolvedObjectsPanelAction::ImportWithoutSelection,
            Self::Export => ResolvedObjectsPanelAction::Export {
                object_names: Vec::new(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SelectionActionBinding {
    OpenTable,
    OpenView,
    OpenRoutine,
    OpenExtension,
    OpenSequence,
    OpenRedisDatabase,
    OpenDocumentCollection,
    DesignTable,
    DesignView,
    DesignRoutine,
    DesignTrigger,
    DesignSequence,
    OpenGenericDdl,
    OpenObjectForm { mode: ObjectFormMode },
    OpenTriggerDdl,
    RenameTable,
    RenameView,
    DeleteTable,
    DeleteView,
    DeleteKey,
    DeleteTrigger,
    DuplicateTable,
    DuplicateView,
    EmptyTable,
    ImportTable,
    Export,
    DumpSqlStructureData,
    DumpSqlStructure,
    CopyName,
    CopyQualifiedName,
    ViewHistory,
}

impl SelectionActionBinding {
    fn into_resolved_action(
        self,
        registry: &ObjectsPanelActionRegistry,
        action_id: &str,
        object_refs: &[ObjectsPanelObjectRef],
        database_name: Option<String>,
    ) -> ResolvedObjectsPanelAction {
        let names: Vec<String> = object_refs
            .iter()
            .map(|object_ref| object_ref.name.clone())
            .collect();
        let qualified_names: Vec<String> = object_refs
            .iter()
            .map(schema_qualified_action_name)
            .collect();

        match self {
            Self::OpenTable => ResolvedObjectsPanelAction::OpenTables {
                object_names: qualified_names,
                database_name,
            },
            Self::OpenView => ResolvedObjectsPanelAction::OpenViews {
                object_names: qualified_names,
                database_name,
            },
            Self::OpenRoutine => routine_plan(
                object_refs,
                |function_ref| ResolvedObjectsPanelAction::OpenFunction { function_ref },
                |procedure_ref| ResolvedObjectsPanelAction::OpenProcedure { procedure_ref },
            ),
            Self::OpenExtension => selected_ref_plan(object_refs, |extension_ref| {
                ResolvedObjectsPanelAction::OpenExtension { extension_ref }
            }),
            Self::OpenSequence => selected_ref_plan(object_refs, |sequence_ref| {
                ResolvedObjectsPanelAction::OpenSequence { sequence_ref }
            }),
            Self::OpenRedisDatabase => object_refs
                .first()
                .and_then(|object_ref| parse_redis_database_index(&object_ref.name))
                .map(
                    |database_index| ResolvedObjectsPanelAction::OpenRedisDatabase {
                        database_index,
                    },
                )
                .unwrap_or(ResolvedObjectsPanelAction::Noop),
            Self::OpenDocumentCollection => object_refs
                .first()
                .and_then(|object_ref| {
                    object_ref.database.as_ref().map(|database_name| {
                        ResolvedObjectsPanelAction::OpenDocumentCollection {
                            database_name: database_name.clone(),
                            collection_name: object_ref.name.clone(),
                        }
                    })
                })
                .unwrap_or(ResolvedObjectsPanelAction::Noop),
            Self::DesignTable => ResolvedObjectsPanelAction::DesignTables {
                object_names: names,
            },
            Self::DesignView => ResolvedObjectsPanelAction::DesignViews {
                object_names: names,
            },
            Self::DesignRoutine => routine_plan(
                object_refs,
                |function_ref| ResolvedObjectsPanelAction::DesignFunction { function_ref },
                |procedure_ref| ResolvedObjectsPanelAction::DesignProcedure { procedure_ref },
            ),
            Self::DesignTrigger => selected_ref_plan(object_refs, |trigger_ref| {
                ResolvedObjectsPanelAction::DesignTrigger { trigger_ref }
            }),
            Self::DesignSequence => selected_ref_plan(object_refs, |sequence_ref| {
                ResolvedObjectsPanelAction::DesignSequence { sequence_ref }
            }),
            Self::OpenGenericDdl => selected_ref_plan(object_refs, |object_ref| {
                ResolvedObjectsPanelAction::OpenGenericDdl {
                    object_ref,
                    kind_id: object_refs[0].kind_id.clone(),
                }
            }),
            Self::OpenObjectForm { mode } => object_refs
                .first()
                .map(|object_ref| ResolvedObjectsPanelAction::OpenObjectForm {
                    kind_id: object_ref.kind_id.clone(),
                    mode,
                    object_ref: Some(object_ref.clone()),
                })
                .unwrap_or(ResolvedObjectsPanelAction::Noop),
            Self::OpenTriggerDdl => selected_ref_plan(object_refs, |trigger_ref| {
                ResolvedObjectsPanelAction::OpenTriggerDdl { trigger_ref }
            }),
            Self::RenameTable => single_name_plan(object_refs, |table_name| {
                ResolvedObjectsPanelAction::RenameTable { table_name }
            }),
            Self::RenameView => single_name_plan(object_refs, |view_name| {
                ResolvedObjectsPanelAction::RenameView { view_name }
            }),
            Self::DeleteTable => ResolvedObjectsPanelAction::DeleteTables {
                object_names: names,
            },
            Self::DeleteView => ResolvedObjectsPanelAction::DeleteViews {
                object_names: names,
            },
            Self::DeleteKey => ResolvedObjectsPanelAction::DeleteKeys { key_names: names },
            Self::DeleteTrigger => selected_ref_plan(object_refs, |trigger_ref| {
                ResolvedObjectsPanelAction::DeleteTrigger { trigger_ref }
            }),
            Self::DuplicateTable => ResolvedObjectsPanelAction::DuplicateTables {
                object_names: names,
            },
            Self::DuplicateView => ResolvedObjectsPanelAction::DuplicateViews {
                object_names: names,
            },
            Self::EmptyTable => {
                if names.len() == 1 {
                    ResolvedObjectsPanelAction::EmptyTable {
                        table_name: names[0].clone(),
                    }
                } else {
                    ResolvedObjectsPanelAction::EmptyTables { table_names: names }
                }
            }
            Self::ImportTable => object_refs
                .first()
                .map(|object_ref| ResolvedObjectsPanelAction::ImportTable {
                    table_name: object_ref.name.clone(),
                })
                .unwrap_or(ResolvedObjectsPanelAction::Noop),
            Self::Export => ResolvedObjectsPanelAction::Export {
                object_names: names,
            },
            Self::DumpSqlStructureData => single_or_batch_table_plan(
                names,
                |table_name| ResolvedObjectsPanelAction::DumpSqlStructureDataSingle { table_name },
                |table_names| ResolvedObjectsPanelAction::DumpSqlStructureDataBatch { table_names },
            ),
            Self::DumpSqlStructure => single_or_batch_table_plan(
                names,
                |table_name| ResolvedObjectsPanelAction::DumpSqlStructureSingle { table_name },
                |table_names| ResolvedObjectsPanelAction::DumpSqlStructureBatch { table_names },
            ),
            Self::CopyName => ResolvedObjectsPanelAction::CopyObjectNames { names },
            Self::CopyQualifiedName => {
                let names = object_refs
                    .iter()
                    .map(format_qualified_object_name)
                    .collect();
                ResolvedObjectsPanelAction::CopyQualifiedNames { names }
            }
            Self::ViewHistory => object_refs
                .first()
                .map(|object_ref| {
                    match registry.resolve_versioned_object_type(action_id, &object_ref.kind_id) {
                        Ok(db_object_type) => ResolvedObjectsPanelAction::ViewHistory {
                            object_name: object_ref.name.clone(),
                            object_schema: object_ref.schema.clone(),
                            db_object_type,
                        },
                        Err(error) => ResolvedObjectsPanelAction::UnsupportedViewHistoryKind {
                            action_id: error.action_id,
                            object_kind: error.kind_id,
                        },
                    }
                })
                .unwrap_or(ResolvedObjectsPanelAction::Noop),
        }
    }
}

fn selected_ref_plan(
    object_refs: &[ObjectsPanelObjectRef],
    build_plan: impl FnOnce(SelectedObjectRef) -> ResolvedObjectsPanelAction,
) -> ResolvedObjectsPanelAction {
    object_refs
        .first()
        .map(selected_object_ref)
        .map(build_plan)
        .unwrap_or(ResolvedObjectsPanelAction::Noop)
}

fn routine_plan(
    object_refs: &[ObjectsPanelObjectRef],
    function_plan: impl FnOnce(SelectedObjectRef) -> ResolvedObjectsPanelAction,
    procedure_plan: impl FnOnce(SelectedObjectRef) -> ResolvedObjectsPanelAction,
) -> ResolvedObjectsPanelAction {
    let Some(object_ref) = object_refs.first() else {
        return ResolvedObjectsPanelAction::Noop;
    };
    let selected_ref = selected_object_ref(object_ref);

    match object_ref.kind_id.as_str() {
        "function" => function_plan(selected_ref),
        "procedure" => procedure_plan(selected_ref),
        _ => ResolvedObjectsPanelAction::Noop,
    }
}

fn single_name_plan(
    object_refs: &[ObjectsPanelObjectRef],
    build_plan: impl FnOnce(String) -> ResolvedObjectsPanelAction,
) -> ResolvedObjectsPanelAction {
    if object_refs.len() == 1 {
        build_plan(object_refs[0].name.clone())
    } else {
        ResolvedObjectsPanelAction::Noop
    }
}

fn single_or_batch_table_plan(
    table_names: Vec<String>,
    single_plan: impl FnOnce(String) -> ResolvedObjectsPanelAction,
    batch_plan: impl FnOnce(Vec<String>) -> ResolvedObjectsPanelAction,
) -> ResolvedObjectsPanelAction {
    if table_names.len() == 1 {
        single_plan(table_names[0].clone())
    } else {
        batch_plan(table_names)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedObjectsPanelAction {
    Noop,
    Refresh,
    NewTable,
    NewView,
    NewTrigger,
    OpenObjectForm {
        kind_id: String,
        mode: ObjectFormMode,
        object_ref: Option<ObjectsPanelObjectRef>,
    },
    ImportWithoutSelection,
    Export {
        object_names: Vec<String>,
    },
    OpenRedisDatabase {
        database_index: u16,
    },
    OpenDocumentCollection {
        database_name: String,
        collection_name: String,
    },
    OpenTables {
        object_names: Vec<String>,
        database_name: Option<String>,
    },
    OpenViews {
        object_names: Vec<String>,
        database_name: Option<String>,
    },
    OpenFunction {
        function_ref: SelectedObjectRef,
    },
    OpenProcedure {
        procedure_ref: SelectedObjectRef,
    },
    OpenExtension {
        extension_ref: SelectedObjectRef,
    },
    OpenSequence {
        sequence_ref: SelectedObjectRef,
    },
    DesignTables {
        object_names: Vec<String>,
    },
    DesignViews {
        object_names: Vec<String>,
    },
    DesignFunction {
        function_ref: SelectedObjectRef,
    },
    DesignProcedure {
        procedure_ref: SelectedObjectRef,
    },
    DesignTrigger {
        trigger_ref: SelectedObjectRef,
    },
    DesignSequence {
        sequence_ref: SelectedObjectRef,
    },
    OpenGenericDdl {
        object_ref: SelectedObjectRef,
        kind_id: String,
    },
    OpenTriggerDdl {
        trigger_ref: SelectedObjectRef,
    },
    RenameView {
        view_name: String,
    },
    RenameTable {
        table_name: String,
    },
    DeleteTables {
        object_names: Vec<String>,
    },
    DeleteViews {
        object_names: Vec<String>,
    },
    DeleteKeys {
        key_names: Vec<String>,
    },
    DeleteTrigger {
        trigger_ref: SelectedObjectRef,
    },
    DuplicateTables {
        object_names: Vec<String>,
    },
    DuplicateViews {
        object_names: Vec<String>,
    },
    EmptyTable {
        table_name: String,
    },
    EmptyTables {
        table_names: Vec<String>,
    },
    ImportTable {
        table_name: String,
    },
    DumpSqlStructureDataSingle {
        table_name: String,
    },
    DumpSqlStructureDataBatch {
        table_names: Vec<String>,
    },
    DumpSqlStructureSingle {
        table_name: String,
    },
    DumpSqlStructureBatch {
        table_names: Vec<String>,
    },
    CopyObjectNames {
        names: Vec<String>,
    },
    CopyQualifiedNames {
        names: Vec<String>,
    },
    ViewHistory {
        object_name: String,
        object_schema: Option<String>,
        db_object_type: DatabaseObjectType,
    },
    UnsupportedViewHistoryKind {
        action_id: String,
        object_kind: String,
    },
    UnknownAction {
        action_id: String,
        object_count: usize,
    },
}

fn manifest_action_for_context<'a>(
    manifest: &'a ObjectsPanelManifest,
    action_id: &str,
    object_refs: &[ObjectsPanelObjectRef],
) -> Option<&'a ManifestAction> {
    if let Some(object_ref) = object_refs.first() {
        if let Some(action) = manifest
            .object_kinds
            .iter()
            .find(|kind| kind.id == object_ref.kind_id)
            .and_then(|kind| {
                kind.row_actions
                    .iter()
                    .find(|action| action.id == action_id)
            })
        {
            return Some(action);
        }
    }

    manifest
        .toolbar_actions
        .iter()
        .find(|action| action.id == action_id)
}

fn manifest_object_form_action(
    action: &ObjectFormAction,
    object_refs: &[ObjectsPanelObjectRef],
) -> ResolvedObjectsPanelAction {
    let plan = plan_object_form_action(action, object_refs);

    ResolvedObjectsPanelAction::OpenObjectForm {
        kind_id: plan.kind_id,
        mode: plan.mode,
        object_ref: plan.object_ref,
    }
}

/// Builds a user-facing message for action-resolution failures so unsupported
/// actions are visible to the user instead of only being logged.
pub fn objects_panel_action_issue_message(
    action_resolution: &ResolvedObjectsPanelAction,
) -> Option<String> {
    match action_resolution {
        ResolvedObjectsPanelAction::UnsupportedViewHistoryKind {
            action_id,
            object_kind,
        } => Some(format!(
            "The '{action_id}' action is not available for '{object_kind}' objects."
        )),
        ResolvedObjectsPanelAction::UnknownAction { action_id, .. } => Some(format!(
            "The '{action_id}' action is not available in this objects panel context."
        )),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectsPanelActionResolutionTelemetry {
    pub resolution: &'static str,
    pub handler_registered: bool,
    pub unknown_or_unsupported: bool,
}

/// Keeps action-resolution telemetry labels stable so InvokeAction metrics can
/// be interpreted consistently as dispatch behavior evolves.
pub fn classify_objects_panel_action_resolution(
    action_resolution: &ResolvedObjectsPanelAction,
) -> ObjectsPanelActionResolutionTelemetry {
    match action_resolution {
        ResolvedObjectsPanelAction::UnknownAction { .. } => ObjectsPanelActionResolutionTelemetry {
            resolution: "unknown_action",
            handler_registered: false,
            unknown_or_unsupported: true,
        },
        ResolvedObjectsPanelAction::UnsupportedViewHistoryKind { .. } => {
            ObjectsPanelActionResolutionTelemetry {
                resolution: "unsupported_kind",
                handler_registered: false,
                unknown_or_unsupported: true,
            }
        }
        ResolvedObjectsPanelAction::Noop => ObjectsPanelActionResolutionTelemetry {
            resolution: "noop",
            handler_registered: false,
            unknown_or_unsupported: false,
        },
        _ => ObjectsPanelActionResolutionTelemetry {
            resolution: "handled",
            handler_registered: true,
            unknown_or_unsupported: false,
        },
    }
}

/// Returns a representative object reference for a manifest kind.
///
/// Coverage checks use a synthetic selection per kind so actions that look valid
/// in metadata but collapse to a no-op for that object class are caught before
/// they reach the UI.
fn representative_object_ref_for_kind(kind_id: &str) -> ObjectsPanelObjectRef {
    match kind_id {
        "redis_database" => ObjectsPanelObjectRef::new("redis_database", "db0"),
        "document_collection" => ObjectsPanelObjectRef::new("document_collection", "users")
            .with_database_option(Some("app".to_string())),
        "function" | "procedure" => ObjectsPanelObjectRef::new(kind_id, "do_work")
            .with_schema_option(Some("public".to_string()))
            .with_signature_option(Some("integer".to_string())),
        "trigger" => ObjectsPanelObjectRef::new(kind_id, "users_audit")
            .with_schema_option(Some("public".to_string()))
            .with_signature_option(Some("users".to_string())),
        "key" => ObjectsPanelObjectRef::new(kind_id, "session:1"),
        _ => ObjectsPanelObjectRef::new(kind_id, "sample_object")
            .with_schema_option(Some("public".to_string())),
    }
}

fn action_resolution_has_dispatch_path(action_resolution: &ResolvedObjectsPanelAction) -> bool {
    !matches!(
        action_resolution,
        ResolvedObjectsPanelAction::Noop
            | ResolvedObjectsPanelAction::UnknownAction { .. }
            | ResolvedObjectsPanelAction::UnsupportedViewHistoryKind { .. }
    )
}

/// Representative selections keep manifest validation from treating
/// selection-driven toolbar actions as unsupported just because their
/// empty-selection path is intentionally a no-op.
fn action_dispatches_for_any_representative_selection(
    action_id: &str,
    manifest: &ObjectsPanelManifest,
    registry: &ObjectsPanelActionRegistry,
) -> bool {
    manifest.object_kinds.iter().any(|kind| {
        let representative_object_ref = representative_object_ref_for_kind(&kind.id);
        action_resolution_has_dispatch_path(&registry.resolve_objects_panel_action(
            action_id,
            std::slice::from_ref(&representative_object_ref),
            None,
        ))
    })
}

/// Toolbar actions can still depend on selected rows even when they are not
/// single-selection only, so coverage checks first try the empty selection and
/// then fall back to representative selections when needed.
fn toolbar_action_has_dispatch_path(
    action: &ManifestAction,
    manifest: &ObjectsPanelManifest,
    registry: &ObjectsPanelActionRegistry,
) -> bool {
    if action.object_form.is_some() {
        return true;
    }

    if action.requires_single_selection {
        return action_dispatches_for_any_representative_selection(
            action.id.as_str(),
            manifest,
            registry,
        );
    }

    if action_resolution_has_dispatch_path(&registry.resolve_objects_panel_action(
        action.id.as_str(),
        &[],
        None,
    )) {
        return true;
    }

    action_dispatches_for_any_representative_selection(action.id.as_str(), manifest, registry)
}

/// Reports manifest action ids that are not wired to the current dispatcher.
///
/// The check is intentionally soft at runtime because manifests are driver-owned,
/// but the CI tests in this module use a representative selection per kind so the
/// built-in driver manifests stay aligned with the action planner and the
/// version-history registry bindings.
pub fn manifest_action_coverage_gaps(manifest: &ObjectsPanelManifest) -> Vec<String> {
    let mut gaps = Vec::new();
    let mut seen_unknown_action_ids = std::collections::BTreeSet::new();
    let registry = ObjectsPanelActionRegistry::default();

    fn record_unknown_action(
        registry: &ObjectsPanelActionRegistry,
        seen_unknown_action_ids: &mut std::collections::BTreeSet<String>,
        gaps: &mut Vec<String>,
        action_id: &str,
        scope: &str,
        action: &ManifestAction,
    ) -> bool {
        if action.object_form.is_some() {
            return true;
        }

        if !registry.has_binding_for_action_id(action_id) {
            if seen_unknown_action_ids.insert(action_id.to_string()) {
                gaps.push(format!("{scope}: unsupported action_id '{action_id}'"));
            }

            return false;
        }

        true
    }

    for action in &manifest.toolbar_actions {
        if !record_unknown_action(
            &registry,
            &mut seen_unknown_action_ids,
            &mut gaps,
            action.id.as_str(),
            "toolbar",
            action,
        ) {
            continue;
        }

        if !toolbar_action_has_dispatch_path(action, manifest, &registry) {
            if action.requires_single_selection {
                gaps.push(format!(
                    "toolbar action_id '{}' requires a single selection but has no non-noop dispatcher path for any representative kind",
                    action.id
                ));
            } else {
                gaps.push(format!(
                    "toolbar action_id '{}' has no non-noop dispatcher path without a selection or for any representative kind",
                    action.id
                ));
            }
        }
    }

    for kind in &manifest.object_kinds {
        let representative_object_ref = representative_object_ref_for_kind(&kind.id);

        for action in &kind.row_actions {
            if !record_unknown_action(
                &registry,
                &mut seen_unknown_action_ids,
                &mut gaps,
                action.id.as_str(),
                &format!("kind '{}'", kind.id),
                action,
            ) {
                continue;
            }

            if action.object_form.is_some() {
                continue;
            }

            if action.id == "view_history" {
                if let Err(error) = registry.resolve_versioned_object_type("view_history", &kind.id)
                {
                    gaps.push(format!(
                        "kind '{}' exposes 'view_history' but the action registry has no binding for action_id '{}' and kind_id '{}'",
                        kind.id, error.action_id, error.kind_id
                    ));
                    continue;
                }
            }

            let action_resolution = registry.resolve_objects_panel_action(
                action.id.as_str(),
                std::slice::from_ref(&representative_object_ref),
                None,
            );
            if !action_resolution_has_dispatch_path(&action_resolution) {
                gaps.push(format!(
                    "kind '{}' exposes action_id '{}' but the dispatcher has no non-noop path for a representative selection",
                    kind.id, action.id
                ));
            }
        }
    }

    gaps
}

#[cfg(test)]
mod tests {
    use super::*;
    use zqlz_core::{ObjectFormMode, ObjectsPanelManifest, ObjectsPanelObjectKind};

    fn object_ref(kind_id: &str, name: &str) -> ObjectsPanelObjectRef {
        ObjectsPanelObjectRef::new(kind_id, name)
    }

    fn schema_object_ref(kind_id: &str, schema: &str, name: &str) -> ObjectsPanelObjectRef {
        ObjectsPanelObjectRef::new(kind_id, name).with_schema_option(Some(schema.to_string()))
    }

    fn action(action_id: &str) -> zqlz_core::ObjectsPanelAction {
        zqlz_core::ObjectsPanelAction::new(action_id, action_id.to_uppercase())
    }

    fn kind(
        kind_id: &str,
        row_actions: Vec<zqlz_core::ObjectsPanelAction>,
    ) -> ObjectsPanelObjectKind {
        ObjectsPanelObjectKind::new(kind_id, kind_id, format!("{}s", kind_id))
            .columns(Vec::new())
            .row_actions(row_actions)
            .default_row_action("open")
    }

    fn object_features() -> ObjectFeatureSet {
        ObjectFeatureSet {
            browse_objects: FeatureAvailability::available(),
            create_objects: FeatureAvailability::unavailable("create blocked"),
            edit_objects: FeatureAvailability::available(),
            delete_objects: FeatureAvailability::unavailable("delete blocked"),
            available_kinds: vec!["table".to_string()],
            available_actions: vec![
                "open".to_string(),
                "new_table".to_string(),
                "rename".to_string(),
                "delete".to_string(),
            ],
        }
    }

    #[test]
    fn action_feature_availability_rejects_unadvertised_action() {
        let availability = objects_panel_action_feature_availability(&object_features(), "drop");

        assert_eq!(
            availability,
            FeatureAvailability::unavailable(
                "The 'drop' action is not advertised by this connection"
            )
        );
    }

    #[test]
    fn action_feature_availability_maps_action_to_feature_group() {
        let features = object_features();

        assert_eq!(
            objects_panel_action_feature_availability(&features, "open"),
            FeatureAvailability::available()
        );
        assert_eq!(
            objects_panel_action_feature_availability(&features, "new_table"),
            FeatureAvailability::unavailable("create blocked")
        );
        assert_eq!(
            objects_panel_action_feature_availability(&features, "rename"),
            FeatureAvailability::available()
        );
        assert_eq!(
            objects_panel_action_feature_availability(&features, "delete"),
            FeatureAvailability::unavailable("delete blocked")
        );
    }

    #[test]
    fn registry_routes_schema_qualified_table_open_without_double_prefix() {
        let registry = ObjectsPanelActionRegistry::default();

        assert_eq!(
            registry.resolve_objects_panel_action(
                "open",
                &[schema_object_ref("table", "public", "public.users")],
                Some("main".to_string()),
            ),
            ResolvedObjectsPanelAction::OpenTables {
                object_names: vec!["public.users".to_string()],
                database_name: Some("main".to_string()),
            }
        );
    }

    #[test]
    fn registry_routes_document_collection_open() {
        let registry = ObjectsPanelActionRegistry::default();
        let collection_ref = object_ref("document_collection", "users")
            .with_database_option(Some("app".to_string()));

        assert_eq!(
            registry.resolve_objects_panel_action("open", &[collection_ref], None),
            ResolvedObjectsPanelAction::OpenDocumentCollection {
                database_name: "app".to_string(),
                collection_name: "users".to_string(),
            }
        );
    }

    #[test]
    fn registry_routes_manifest_object_form_actions() {
        let registry = ObjectsPanelActionRegistry::default();
        let domain_ref = object_ref("domain", "email_address");
        let manifest = ObjectsPanelManifest {
            object_kinds: vec![ObjectsPanelObjectKind::new("domain", "Domain", "Domains")
                .row_actions(vec![zqlz_core::ObjectsPanelAction::new(
                    "alter_domain_form",
                    "Alter Domain",
                )
                .object_form("domain", ObjectFormMode::Edit)
                .single_selection()])],
            toolbar_actions: vec![zqlz_core::ObjectsPanelAction::new(
                "create_custom_enum",
                "New Enum",
            )
            .object_form("enum", ObjectFormMode::Create)],
        };

        assert_eq!(
            registry.resolve_objects_panel_action_with_manifest(
                "create_custom_enum",
                &[],
                None,
                Some(&manifest),
            ),
            ResolvedObjectsPanelAction::OpenObjectForm {
                kind_id: "enum".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
            }
        );
        assert_eq!(
            registry.resolve_objects_panel_action_with_manifest(
                "alter_domain_form",
                std::slice::from_ref(&domain_ref),
                None,
                Some(&manifest),
            ),
            ResolvedObjectsPanelAction::OpenObjectForm {
                kind_id: "domain".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: Some(domain_ref),
            }
        );
        assert!(
            manifest_action_coverage_gaps(&manifest).is_empty(),
            "object-form manifest actions should not require static registry ids"
        );
    }

    #[test]
    fn manifest_coverage_reports_unknown_and_noop_actions() {
        let manifest = ObjectsPanelManifest {
            object_kinds: vec![
                ObjectsPanelObjectKind::new("table", "Table", "Tables")
                    .columns(Vec::new())
                    .row_actions(vec![
                        action("open"),
                        zqlz_core::ObjectsPanelAction::new("custom_archive", "Custom Archive"),
                    ])
                    .default_row_action("open"),
                kind("sequence", vec![action("open"), action("delete")]),
            ],
            toolbar_actions: vec![action("refresh")],
        };

        let gaps = manifest_action_coverage_gaps(&manifest);

        assert!(
            gaps.iter().any(|gap| gap.contains("custom_archive")),
            "expected unsupported custom action gap, got: {gaps:?}"
        );
        assert!(
            gaps.iter()
                .any(|gap| gap.contains("kind 'sequence'") && gap.contains("'delete'")),
            "expected unsupported sequence delete gap, got: {gaps:?}"
        );
    }

    #[test]
    fn action_issue_messages_and_telemetry_are_service_owned() {
        assert_eq!(
            objects_panel_action_issue_message(
                &ResolvedObjectsPanelAction::UnsupportedViewHistoryKind {
                    action_id: "view_history".to_string(),
                    object_kind: "sequence".to_string(),
                },
            ),
            Some("The 'view_history' action is not available for 'sequence' objects.".to_string())
        );
        assert_eq!(
            classify_objects_panel_action_resolution(&ResolvedObjectsPanelAction::UnknownAction {
                action_id: "merge_rows".to_string(),
                object_count: 3,
            }),
            ObjectsPanelActionResolutionTelemetry {
                resolution: "unknown_action",
                handler_registered: false,
                unknown_or_unsupported: true,
            }
        );
    }
}
