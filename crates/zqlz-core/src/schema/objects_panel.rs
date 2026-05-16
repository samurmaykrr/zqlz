use serde::{Deserialize, Serialize};

use crate::{Result, ZqlzError};

use super::{KeyValueInfo, ObjectFormAction, ObjectFormMode, TableInfo, TableType};

/// Column alignment for objects panel display
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ObjectsPanelColumnAlignment {
    #[default]
    Left,
    Right,
}

/// Column definition for the objects panel, provided by each driver
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPanelColumn {
    /// Unique column identifier (used for sorting, lookup)
    pub id: String,
    /// Display title shown in the column header
    pub title: String,
    /// Default width in pixels
    pub width: f32,
    /// Minimum width in pixels
    pub min_width: f32,
    /// Whether the column can be resized by the user
    pub resizable: bool,
    /// Whether the column is sortable
    pub sortable: bool,
    /// Text alignment
    pub alignment: ObjectsPanelColumnAlignment,
}

impl ObjectsPanelColumn {
    pub fn new(id: impl Into<String>, title: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            width: 100.0,
            min_width: 50.0,
            resizable: true,
            sortable: false,
            alignment: ObjectsPanelColumnAlignment::Left,
        }
    }

    pub fn width(mut self, width: f32) -> Self {
        self.width = width;
        self
    }

    pub fn min_width(mut self, min_width: f32) -> Self {
        self.min_width = min_width;
        self
    }

    pub fn resizable(mut self, resizable: bool) -> Self {
        self.resizable = resizable;
        self
    }

    pub fn sortable(mut self) -> Self {
        self.sortable = true;
        self
    }

    pub fn text_right(mut self) -> Self {
        self.alignment = ObjectsPanelColumnAlignment::Right;
        self
    }
}

/// Declarative action definition for Objects Panel menus and toolbars.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPanelAction {
    /// Stable action identifier for app-layer routing (e.g. "open", "delete").
    pub id: String,
    /// Human-readable label shown in UI.
    pub label: String,
    /// Optional icon key resolved by UI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon_key: Option<String>,
    /// Optional grouping hint for menu organization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// Optional object kind created by this action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_object_kind_id: Option<String>,
    /// Whether this action refreshes the objects panel.
    #[serde(default)]
    pub refreshes_objects_panel: bool,
    /// Whether this action can only run on a single selected object.
    pub requires_single_selection: bool,
    /// Whether this action is destructive.
    pub destructive: bool,
    /// Optional object form opened by this action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_form: Option<ObjectFormAction>,
}

impl ObjectsPanelAction {
    pub fn new(id: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            icon_key: None,
            group: None,
            create_object_kind_id: None,
            refreshes_objects_panel: false,
            requires_single_selection: false,
            destructive: false,
            object_form: None,
        }
    }

    pub fn icon_key(mut self, icon_key: impl Into<String>) -> Self {
        self.icon_key = Some(icon_key.into());
        self
    }

    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    pub fn create_object_kind(mut self, kind_id: impl Into<String>) -> Self {
        self.create_object_kind_id = Some(kind_id.into());
        self
    }

    pub fn refreshes_objects_panel(mut self) -> Self {
        self.refreshes_objects_panel = true;
        self
    }

    pub fn single_selection(mut self) -> Self {
        self.requires_single_selection = true;
        self
    }

    pub fn destructive(mut self) -> Self {
        self.destructive = true;
        self
    }

    pub fn object_form(mut self, kind_id: impl Into<String>, mode: ObjectFormMode) -> Self {
        self.object_form = Some(ObjectFormAction {
            kind_id: kind_id.into(),
            mode,
        });
        self
    }
}

/// Declarative object-kind definition for driver-provided Objects Panel behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPanelObjectKind {
    /// Stable object kind id (e.g. "table", "view", "function").
    pub id: String,
    /// Label for one object.
    pub label_singular: String,
    /// Label for multiple objects.
    pub label_plural: String,
    /// Optional icon key resolved by UI.
    pub icon_key: Option<String>,
    /// Column layout for rows of this kind.
    pub columns: Vec<ObjectsPanelColumn>,
    /// Actions available when rows of this kind are selected.
    pub row_actions: Vec<ObjectsPanelAction>,
    /// Optional default action id used for double-click / Enter.
    pub default_row_action_id: Option<String>,
}

impl ObjectsPanelObjectKind {
    pub fn new(
        id: impl Into<String>,
        label_singular: impl Into<String>,
        label_plural: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            label_singular: label_singular.into(),
            label_plural: label_plural.into(),
            icon_key: None,
            columns: Vec::new(),
            row_actions: Vec::new(),
            default_row_action_id: None,
        }
    }

    pub fn icon_key(mut self, icon_key: impl Into<String>) -> Self {
        self.icon_key = Some(icon_key.into());
        self
    }

    pub fn columns(mut self, columns: Vec<ObjectsPanelColumn>) -> Self {
        self.columns = columns;
        self
    }

    pub fn row_actions(mut self, actions: Vec<ObjectsPanelAction>) -> Self {
        self.row_actions = actions;
        self
    }

    pub fn default_row_action(mut self, action_id: impl Into<String>) -> Self {
        self.default_row_action_id = Some(action_id.into());
        self
    }
}

/// Driver-provided manifest describing object kinds and actions for Objects Panel.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObjectsPanelManifest {
    /// Supported object kinds and their row behavior.
    pub object_kinds: Vec<ObjectsPanelObjectKind>,
    /// Optional global toolbar actions.
    pub toolbar_actions: Vec<ObjectsPanelAction>,
}

impl ObjectsPanelManifest {
    /// Builds a conservative manifest from row data for backward-compatible
    /// drivers that only provide `ObjectsPanelData`.
    pub fn from_data(data: &ObjectsPanelData) -> Self {
        let columns = if data.columns.is_empty() {
            ObjectsPanelData::from_table_infos(Vec::<TableInfo>::new()).columns
        } else {
            data.columns.clone()
        };

        let mut seen_kind_ids = std::collections::BTreeSet::new();
        let mut kinds = Vec::new();

        for row in &data.rows {
            let kind_id = row.object_kind_id().to_string();
            if !seen_kind_ids.insert(kind_id.clone()) {
                continue;
            }

            let label = kind_label_from_id(&kind_id);
            let row_actions = default_actions_for_kind(&kind_id);
            let mut kind =
                ObjectsPanelObjectKind::new(kind_id.clone(), label.clone(), format!("{}s", label))
                    .icon_key(kind_id.clone())
                    .columns(columns.clone())
                    .row_actions(row_actions.clone());

            if row_actions.iter().any(|action| action.id == "open") {
                kind = kind.default_row_action("open");
            }

            kinds.push(kind);
        }

        if kinds.is_empty() {
            kinds.push(
                ObjectsPanelObjectKind::new("table", "Table", "Tables")
                    .icon_key("table")
                    .columns(columns)
                    .row_actions(default_actions_for_kind("table"))
                    .default_row_action("open"),
            );
        }

        let has_relational_create_kinds = kinds
            .iter()
            .any(|kind| matches!(kind.id.as_str(), "table" | "view" | "materialized_view"));

        let mut toolbar_actions = vec![
            ObjectsPanelAction::new("refresh", "Refresh")
                .icon_key("refresh")
                .refreshes_objects_panel(),
        ];
        if has_relational_create_kinds {
            toolbar_actions.extend([
                ObjectsPanelAction::new("new_table", "New Table")
                    .icon_key("create")
                    .create_object_kind("table"),
                ObjectsPanelAction::new("new_view", "New View")
                    .icon_key("create")
                    .create_object_kind("view"),
                ObjectsPanelAction::new("import", "Import Wizard...").icon_key("import"),
                ObjectsPanelAction::new("export", "Export Wizard...").icon_key("export"),
            ]);
        }

        Self {
            object_kinds: kinds,
            toolbar_actions,
        }
    }

    pub fn validate(&self) -> Result<()> {
        let mut seen_kind_ids = std::collections::BTreeSet::new();

        for kind in &self.object_kinds {
            if !seen_kind_ids.insert(kind.id.as_str()) {
                return Err(ZqlzError::Schema(format!(
                    "Objects panel manifest has duplicate kind_id '{}'",
                    kind.id
                )));
            }

            let mut seen_action_ids = std::collections::BTreeSet::new();
            for action in &kind.row_actions {
                if !seen_action_ids.insert(action.id.as_str()) {
                    return Err(ZqlzError::Schema(format!(
                        "Objects panel manifest kind '{}' has duplicate action_id '{}'",
                        kind.id, action.id
                    )));
                }
            }

            if let Some(default_action_id) = kind.default_row_action_id.as_deref()
                && !kind
                    .row_actions
                    .iter()
                    .any(|action| action.id == default_action_id)
            {
                return Err(ZqlzError::Schema(format!(
                    "Objects panel manifest kind '{}' has default_row_action_id '{}' with no matching row action",
                    kind.id, default_action_id
                )));
            }
        }

        Ok(())
    }
}

/// Canonical object identity for generic Objects Panel actions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectsPanelObjectRef {
    /// Object kind id (e.g. "table", "view", "function").
    pub kind_id: String,
    /// Database/catalog scope when relevant.
    pub database: Option<String>,
    /// Schema/namespace scope when relevant.
    pub schema: Option<String>,
    /// Object name in its native catalog.
    pub name: String,
    /// Optional function/procedure signature for overloaded routines.
    pub signature: Option<String>,
    /// Stable key suitable for routing, diffing, and version history lookups.
    pub identity_key: String,
}

impl ObjectsPanelObjectRef {
    pub fn new(kind_id: impl Into<String>, name: impl Into<String>) -> Self {
        let kind_id = kind_id.into();
        let name = name.into();
        let identity_key = Self::build_identity_key(&kind_id, None, None, &name, None);

        Self {
            kind_id,
            database: None,
            schema: None,
            name,
            signature: None,
            identity_key,
        }
    }

    pub fn with_database_option(mut self, database: Option<String>) -> Self {
        self.database = database;
        self.rebuild_identity_key();
        self
    }

    pub fn with_schema_option(mut self, schema: Option<String>) -> Self {
        self.schema = schema;
        self.rebuild_identity_key();
        self
    }

    pub fn with_signature_option(mut self, signature: Option<String>) -> Self {
        self.signature = signature;
        self.rebuild_identity_key();
        self
    }

    pub fn build_identity_key(
        kind_id: &str,
        database: Option<&str>,
        schema: Option<&str>,
        name: &str,
        signature: Option<&str>,
    ) -> String {
        let database_part = database.unwrap_or_default();
        let schema_part = schema.unwrap_or_default();
        let signature_part = signature.unwrap_or_default();
        format!(
            "{}::{}::{}::{}::{}",
            kind_id, database_part, schema_part, name, signature_part
        )
    }

    fn rebuild_identity_key(&mut self) {
        self.identity_key = Self::build_identity_key(
            &self.kind_id,
            self.database.as_deref(),
            self.schema.as_deref(),
            &self.name,
            self.signature.as_deref(),
        );
    }
}

/// A single row in the objects panel, holding both identity info and display values
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPanelRow {
    /// Object name (used for context menus, double-click actions)
    pub name: String,
    /// Optional schema/namespace for the object when the driver exposes it.
    pub schema: Option<String>,
    /// Object type: "table", "view", "key", "redis_database", etc.
    pub object_type: String,
    /// Canonical object identity used by generic action routing.
    #[serde(default)]
    pub object_ref: Option<ObjectsPanelObjectRef>,
    /// Cell values keyed by column id, in display-ready string form
    pub values: std::collections::BTreeMap<String, String>,
    /// Redis database index (only for "redis_database" objects)
    pub redis_database_index: Option<u16>,
    /// Key-value specific metadata (only for key-value stores)
    pub key_value_info: Option<KeyValueInfo>,
}

impl ObjectsPanelRow {
    pub fn object_kind_id(&self) -> &str {
        self.object_ref
            .as_ref()
            .map(|object_ref| object_ref.kind_id.as_str())
            .unwrap_or(self.object_type.as_str())
    }

    pub fn object_name(&self) -> &str {
        self.object_ref
            .as_ref()
            .map(|object_ref| object_ref.name.as_str())
            .unwrap_or(self.name.as_str())
    }

    pub fn object_schema(&self) -> Option<&str> {
        self.object_ref
            .as_ref()
            .and_then(|object_ref| object_ref.schema.as_deref())
            .or(self.schema.as_deref())
    }
}

/// Complete dataset for the objects panel, fully driver-defined
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsPanelData {
    /// Column definitions (order determines display order)
    pub columns: Vec<ObjectsPanelColumn>,
    /// Row data
    pub rows: Vec<ObjectsPanelRow>,
}

impl ObjectsPanelData {
    pub fn new(columns: Vec<ObjectsPanelColumn>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
        }
    }

    /// Build from basic `TableInfo` list with a standard relational column set.
    /// Used as the default fallback for drivers that don't override `list_tables_extended`.
    pub fn from_table_infos(table_infos: Vec<TableInfo>) -> Self {
        let columns = vec![
            ObjectsPanelColumn::new("name", "Name")
                .width(400.0)
                .min_width(150.0)
                .resizable(true)
                .sortable(),
            ObjectsPanelColumn::new("row_count", "Rows")
                .width(80.0)
                .min_width(50.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("index_count", "Indexes")
                .width(80.0)
                .min_width(60.0)
                .resizable(true)
                .sortable()
                .text_right(),
            ObjectsPanelColumn::new("trigger_count", "Triggers")
                .width(80.0)
                .min_width(60.0)
                .resizable(true)
                .sortable()
                .text_right(),
        ];

        let rows = table_infos
            .into_iter()
            .map(|info| {
                let schema = info.schema.clone();
                let object_name = info.name.clone();
                let mut values = std::collections::BTreeMap::new();
                values.insert("name".to_string(), object_name.clone());
                values.insert(
                    "row_count".to_string(),
                    info.row_count
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );
                values.insert(
                    "index_count".to_string(),
                    info.index_count
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );
                values.insert(
                    "trigger_count".to_string(),
                    info.trigger_count
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                );

                let object_type = match info.table_type {
                    TableType::View | TableType::MaterializedView => "view",
                    _ => "table",
                };

                ObjectsPanelRow {
                    name: info.name,
                    schema: schema.clone(),
                    object_type: object_type.to_string(),
                    object_ref: Some(
                        ObjectsPanelObjectRef::new(object_type, object_name)
                            .with_schema_option(schema),
                    ),
                    values,
                    redis_database_index: None,
                    key_value_info: info.key_value_info,
                }
            })
            .collect();

        Self { columns, rows }
    }

    /// Build a kind-scoped view of the data while preserving the driver's columns.
    ///
    /// Callers can reuse the same manifest and column metadata while narrowing the
    /// row set to the currently active kind and optional scope.
    pub fn for_kind_and_scope(&self, kind_id: &str, scope: Option<&str>) -> Self {
        let rows = self
            .rows
            .iter()
            .filter(|row| row.object_kind_id() == kind_id)
            .filter(|row| {
                scope
                    .map(|scope_id| row.object_schema() == Some(scope_id))
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        Self {
            columns: self.columns.clone(),
            rows,
        }
    }
}

fn kind_label_from_id(kind_id: &str) -> String {
    let mut words = Vec::new();
    for part in kind_id.split('_').filter(|part| !part.is_empty()) {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            words.push(format!("{}{}", first.to_uppercase(), chars.as_str()));
        }
    }

    if words.is_empty() {
        "Object".to_string()
    } else {
        words.join(" ")
    }
}

fn default_actions_for_kind(kind_id: &str) -> Vec<ObjectsPanelAction> {
    let mut actions = Vec::new();

    if kind_id != "document_database" {
        actions.push(ObjectsPanelAction::new("open", "Open"));
    }

    if !matches!(
        kind_id,
        "redis_database" | "document_database" | "document_collection"
    ) {
        actions.push(ObjectsPanelAction::new("design", "Design").single_selection());
    }

    actions.push(ObjectsPanelAction::new("copy_name", "Copy Name"));
    actions.push(ObjectsPanelAction::new(
        "copy_qualified_name",
        "Copy Qualified Name",
    ));

    if matches!(
        kind_id,
        "table" | "view" | "function" | "procedure" | "trigger"
    ) {
        actions.push(ObjectsPanelAction::new("view_history", "View History").single_selection());
    }

    // Navigation-only document/Redis rows must not advertise destructive actions
    // that the dispatcher cannot satisfy for those surfaces.
    if !matches!(
        kind_id,
        "redis_database" | "document_database" | "document_collection"
    ) {
        actions.push(ObjectsPanelAction::new("delete", "Delete").destructive());
    }
    actions.push(ObjectsPanelAction::new("refresh", "Refresh"));

    actions
}
