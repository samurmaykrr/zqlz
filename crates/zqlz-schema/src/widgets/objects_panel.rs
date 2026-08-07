//! Objects panel - displays database objects using the Table component

use std::collections::{HashMap, HashSet};

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_connection::SidebarObjectCapabilities;
use zqlz_core::{
    ObjectFeatureSet, ObjectsPanelAction, ObjectsPanelColumn, ObjectsPanelColumnAlignment,
    ObjectsPanelData, ObjectsPanelManifest, ObjectsPanelObjectRef, ObjectsPanelRow,
    SINGLE_SELECTION_REASON, objects_panel_action_feature_availability,
};
use zqlz_ui::widgets::{
    ActiveTheme, Icon, IconName, Sizable, Size, ZqlzIcon, action_icon_from_key,
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputEvent, InputState},
    menu::{DropdownMenu, PopupMenu, PopupMenuItem},
    object_icon_from_key,
    table::{Column, ColumnSort, Table, TableDelegate, TableEvent, TableState},
    typography::body_small,
    v_flex,
};

// Keyboard actions for the objects panel
actions!(objects_panel, [OpenSelected, DeleteSelected, NewObject]);

/// Events emitted by the objects panel
#[derive(Clone, Debug)]
pub enum ObjectsPanelEvent {
    /// User wants to invoke a generic action on one or more objects.
    InvokeAction {
        connection_id: Uuid,
        action_id: String,
        object_refs: Vec<ObjectsPanelObjectRef>,
    },
    ActiveKindChanged {
        connection_id: Uuid,
        kind_id: String,
        scope_id: Option<String>,
    },
}

#[derive(Clone)]
struct ObjectsPanelKindMenuEntry {
    kind_id: String,
    label: String,
}

#[derive(Clone)]
struct ObjectsPanelScopeMenuEntry {
    scope_id: Option<String>,
    label: String,
}

/// Why the connection cannot run `action_id`, or `None` when it can.
fn object_action_unavailable_reason(
    features: &ObjectFeatureSet,
    action_id: &str,
) -> Option<SharedString> {
    let availability = objects_panel_action_feature_availability(features, action_id);
    if availability.available {
        return None;
    }

    Some(SharedString::from(availability.reason_or(format!(
        "The '{action_id}' action is not available for this connection"
    ))))
}

/// A row action paired with the reason it cannot be used, if any.
#[derive(Clone)]
struct OrderedActionEntry {
    action: ObjectsPanelAction,
    disabled_reason: Option<SharedString>,
}

#[derive(Clone)]
struct ObjectsPanelToolbarActionEntry {
    id: String,
    label: String,
    icon_key: Option<String>,
    group: Option<String>,
}

fn object_ref_from_row(row: &ObjectsPanelRow) -> ObjectsPanelObjectRef {
    row.object_ref.clone().unwrap_or_else(|| {
        ObjectsPanelObjectRef::new(row.object_kind_id(), row.object_name())
            .with_schema_option(row.object_schema().map(ToString::to_string))
    })
}

fn resolve_primary_row_action_id(manifest: &ObjectsPanelManifest, row: &ObjectsPanelRow) -> String {
    let Some(kind) = manifest
        .object_kinds
        .iter()
        .find(|kind| kind.id == row.object_kind_id())
    else {
        return "open".to_string();
    };

    let Some(default_action_id) = kind.default_row_action_id.as_deref() else {
        return "open".to_string();
    };

    if default_action_id.is_empty() {
        return "open".to_string();
    }

    if kind
        .row_actions
        .iter()
        .any(|action| action.id == default_action_id)
    {
        return default_action_id.to_string();
    }

    tracing::warn!(
        kind_id = %kind.id,
        default_row_action_id = default_action_id,
        "Objects panel kind has unknown default_row_action_id; falling back to open"
    );
    "open".to_string()
}

fn resolve_destructive_row_action_id(
    manifest: &ObjectsPanelManifest,
    row: &ObjectsPanelRow,
) -> Option<String> {
    manifest
        .object_kinds
        .iter()
        .find(|kind| kind.id == row.object_kind_id())
        .and_then(|kind| {
            kind.row_actions
                .iter()
                .find(|action| action.destructive)
                .map(|action| action.id.clone())
        })
}

fn resolve_create_action_id(
    active_kind_id: Option<&str>,
    toolbar_actions: &[ObjectsPanelAction],
) -> Option<String> {
    let mut seen_create_actions = std::collections::BTreeSet::new();
    let mut create_actions: Vec<&ObjectsPanelAction> = toolbar_actions
        .iter()
        .filter(|action| {
            action.create_object_kind_id.is_some() && seen_create_actions.insert(action.id.as_str())
        })
        .collect();

    if let Some(action) = active_kind_id.and_then(|active_kind_id| {
        create_actions
            .iter()
            .find(|action| action.create_object_kind_id.as_deref() == Some(active_kind_id))
    }) {
        return Some(action.id.clone());
    }

    create_actions
        .drain(..)
        .next()
        .map(|action| action.id.clone())
}

fn resolve_refresh_action_id(toolbar_actions: &[ObjectsPanelAction]) -> Option<String> {
    toolbar_actions
        .iter()
        .find(|action| action.refreshes_objects_panel)
        .map(|action| action.id.clone())
}

/// Keeps the empty-state copy aligned with the manifest-driven kind model so
/// the panel does not imply a tables/views-only surface.
fn empty_state_copy(active_kind_label: Option<&str>) -> (String, String) {
    match active_kind_label {
        Some(kind_label) => (
            format!("No {} found", kind_label.to_lowercase()),
            "Try changing the kind, scope, or search.".to_string(),
        ),
        None => (
            "No objects found".to_string(),
            "Try changing the kind, scope, or search.".to_string(),
        ),
    }
}

/// Delegate for the objects table
pub struct ObjectsTableDelegate {
    /// Column definitions from the driver
    columns: Vec<Column>,
    /// Column IDs in order (parallel to `columns`), used for value lookups
    column_ids: Vec<String>,
    /// All objects (unfiltered)
    objects: Vec<ObjectsPanelRow>,
    /// Default columns from the latest loaded data payload.
    default_columns: Vec<ObjectsPanelColumn>,
    /// Filtered objects (after search)
    filtered_objects: Vec<ObjectsPanelRow>,
    /// UI size (reserved for future use)
    #[allow(dead_code)]
    size: Size,
    /// Connection ID (needed for operations)
    connection_id: Option<Uuid>,
    /// Database name for MySQL multi-database browsing
    database_name: Option<String>,
    /// Weak reference back to the panel (to emit events)
    panel: WeakEntity<ObjectsPanel>,
    /// Whether we're loading
    is_loading: bool,
    /// Effective object capabilities for the active connection.
    object_capabilities: SidebarObjectCapabilities,
    /// Which object actions the active connection can actually perform, so the
    /// context menu can explain why an action is unavailable instead of failing
    /// after the user clicks it.
    object_features: Option<ObjectFeatureSet>,
    /// Declarative object/action behavior for generic panel rendering.
    manifest: ObjectsPanelManifest,
    /// Currently active object kind.
    active_kind_id: Option<String>,
    /// Currently active scope within the active kind.
    active_scope_id: Option<String>,
    /// Kinds whose rows have actually been fetched, so an unfetched kind is not
    /// mistaken for a kind that is genuinely empty.
    loaded_kind_ids: HashSet<String>,
    /// Current search query applied to the active kind.
    search_text: String,
    /// Cached selected row indices for context menu (populated when context menu opens)
    context_menu_selected_rows: Vec<usize>,
}

impl ObjectsTableDelegate {
    pub fn new(panel: WeakEntity<ObjectsPanel>) -> Self {
        // Start with default relational columns; will be replaced on first data load
        let default_data = ObjectsPanelData::from_table_infos(Vec::new());
        let (columns, column_ids) = Self::build_ui_columns(&default_data.columns);

        Self {
            columns,
            column_ids,
            objects: Vec::new(),
            default_columns: default_data.columns,
            filtered_objects: Vec::new(),
            size: Size::Small,
            connection_id: None,
            database_name: None,
            panel,
            is_loading: false,
            object_capabilities: SidebarObjectCapabilities::default(),
            object_features: None,
            manifest: ObjectsPanelManifest::default(),
            active_kind_id: None,
            active_scope_id: None,
            loaded_kind_ids: HashSet::new(),
            search_text: String::new(),
            context_menu_selected_rows: Vec::new(),
        }
    }

    fn resolve_active_kind_id(&self) -> Option<String> {
        if let Some(active_kind_id) = self.active_kind_id.as_deref()
            && self
                .manifest
                .object_kinds
                .iter()
                .any(|kind| kind.id == active_kind_id)
        {
            return Some(active_kind_id.to_string());
        }

        self.manifest
            .object_kinds
            .iter()
            .find(|kind| {
                self.objects
                    .iter()
                    .any(|row| row.object_kind_id() == kind.id.as_str())
            })
            .map(|kind| kind.id.clone())
            .or_else(|| {
                self.manifest
                    .object_kinds
                    .first()
                    .map(|kind| kind.id.clone())
            })
            .or_else(|| {
                self.objects
                    .first()
                    .map(|row| row.object_kind_id().to_string())
            })
    }

    fn columns_for_active_kind(&self) -> &[ObjectsPanelColumn] {
        if let Some(active_kind_id) = self.active_kind_id.as_deref()
            && let Some(kind) = self
                .manifest
                .object_kinds
                .iter()
                .find(|kind| kind.id == active_kind_id)
            && !kind.columns.is_empty()
        {
            return &kind.columns;
        }

        &self.default_columns
    }

    fn available_scope_ids_for_active_kind(&self) -> Vec<String> {
        let mut scope_ids = std::collections::BTreeSet::new();
        let active_kind_id = self.active_kind_id.as_deref();

        for row in &self.objects {
            if !active_kind_id
                .map(|kind_id| row.object_kind_id() == kind_id)
                .unwrap_or(true)
            {
                continue;
            }

            if let Some(scope_id) = row.object_schema().map(str::to_string) {
                scope_ids.insert(scope_id);
            }
        }

        scope_ids.into_iter().collect()
    }

    fn resolve_active_scope_id(&self) -> Option<String> {
        let available_scopes = self.available_scope_ids_for_active_kind();
        if let Some(active_scope_id) = self.active_scope_id.as_deref()
            && available_scopes
                .iter()
                .any(|scope_id| scope_id == active_scope_id)
        {
            return Some(active_scope_id.to_string());
        }

        None
    }

    fn rebuild_filtered_objects(&mut self) {
        let active_kind_id = self.active_kind_id.as_deref();
        let search_lower = self.search_text.to_lowercase();
        let has_search = !search_lower.is_empty();

        self.filtered_objects = self
            .objects
            .iter()
            .filter(|row| {
                active_kind_id
                    .map(|kind_id| row.object_kind_id() == kind_id)
                    .unwrap_or(true)
            })
            .filter(|row| {
                self.active_scope_id
                    .as_deref()
                    .map(|scope_id| row.object_schema() == Some(scope_id))
                    .unwrap_or(true)
            })
            .filter(|row| {
                if !has_search {
                    return true;
                }

                row.object_name().to_lowercase().contains(&search_lower)
            })
            .cloned()
            .collect();
    }

    fn rebuild_active_kind_projection(&mut self) {
        self.active_kind_id = self.resolve_active_kind_id();
        self.active_scope_id = self.resolve_active_scope_id();
        let (columns, column_ids) = Self::build_ui_columns(self.columns_for_active_kind());
        self.columns = columns;
        self.column_ids = column_ids;
        self.rebuild_filtered_objects();
    }

    fn row_identity_key(row: &ObjectsPanelRow) -> String {
        row.object_ref
            .as_ref()
            .map(|object_ref| object_ref.identity_key.clone())
            .unwrap_or_else(|| {
                format!(
                    "{}:{}:{}",
                    row.object_kind_id(),
                    row.object_schema().unwrap_or(""),
                    row.object_name()
                )
            })
    }

    fn merge_rows(existing: ObjectsPanelRow, mut incoming: ObjectsPanelRow) -> ObjectsPanelRow {
        for (key, value) in existing.values {
            incoming.values.entry(key).or_insert(value);
        }

        incoming
    }

    fn merge_kind_rows(
        objects: &mut Vec<ObjectsPanelRow>,
        kind_id: &str,
        incoming_rows: Vec<ObjectsPanelRow>,
        already_loaded: bool,
    ) {
        let existing_count = objects
            .iter()
            .filter(|row| row.object_kind_id() == kind_id)
            .count();

        // Only protect rows against an empty payload once the kind has been fetched
        // before; otherwise a first, legitimately-empty load could never settle.
        if already_loaded && incoming_rows.is_empty() && existing_count > 0 {
            tracing::warn!(
                kind_id,
                existing_count,
                "Skipping empty objects panel kind refresh to preserve visible inventory rows"
            );
            return;
        }

        let mut incoming_rows: Vec<Option<ObjectsPanelRow>> =
            incoming_rows.into_iter().map(Some).collect();
        let mut merged_rows = Vec::with_capacity(objects.len() + incoming_rows.len());

        for existing in objects.drain(..) {
            if existing.object_kind_id() != kind_id {
                merged_rows.push(existing);
                continue;
            }

            let existing_key = Self::row_identity_key(&existing);
            let incoming_index = incoming_rows.iter().position(|row| {
                row.as_ref()
                    .map(|row| Self::row_identity_key(row) == existing_key)
                    .unwrap_or(false)
            });

            if let Some(incoming_index) = incoming_index {
                if let Some(incoming) = incoming_rows[incoming_index].take() {
                    merged_rows.push(Self::merge_rows(existing, incoming));
                }
            } else {
                merged_rows.push(existing);
            }
        }

        merged_rows.extend(incoming_rows.into_iter().flatten());
        *objects = merged_rows;
    }

    /// Convert driver-provided column definitions to UI table columns
    fn build_ui_columns(panel_columns: &[ObjectsPanelColumn]) -> (Vec<Column>, Vec<String>) {
        let ids: Vec<String> = panel_columns.iter().map(|c| c.id.clone()).collect();
        let columns: Vec<Column> = panel_columns
            .iter()
            .map(|panel_col| {
                let mut col = Column::new(panel_col.id.clone(), panel_col.title.clone())
                    .width(panel_col.width)
                    .min_width(panel_col.min_width)
                    .resizable(panel_col.resizable);

                if panel_col.sortable {
                    col = col.sortable();
                }

                if panel_col.alignment == ObjectsPanelColumnAlignment::Right {
                    col = col.text_right();
                }

                col
            })
            .collect();

        (columns, ids)
    }

    /// Load extended data directly from the driver
    pub fn set_extended_data(&mut self, connection_id: Uuid, data: ObjectsPanelData) {
        self.connection_id = Some(connection_id);
        self.default_columns = data.columns;
        self.objects = data.rows;
        // A full payload is self-describing: only the kinds it carries are loaded.
        // Kinds it omits become "not fetched yet" again so they get re-requested.
        self.loaded_kind_ids = self
            .objects
            .iter()
            .map(|row| row.object_kind_id().to_string())
            .collect();
        self.rebuild_active_kind_projection();
    }

    /// Set the effective object capabilities for the active connection.
    pub fn set_object_capabilities(&mut self, object_capabilities: SidebarObjectCapabilities) {
        self.object_capabilities = object_capabilities;
    }

    /// Set which object actions the active connection advertises.
    pub fn set_object_features(&mut self, object_features: Option<ObjectFeatureSet>) {
        self.object_features = object_features;
    }

    /// Why the connection cannot run `action_id`, or `None` when it can. Returns
    /// `None` while the feature set is still unknown so actions are not blocked by
    /// a load that has not finished.
    fn action_unavailable_reason(&self, action_id: &str) -> Option<SharedString> {
        let features = self.object_features.as_ref()?;
        object_action_unavailable_reason(features, action_id)
    }

    pub fn set_manifest(&mut self, manifest: ObjectsPanelManifest) {
        self.manifest = manifest;
        self.rebuild_active_kind_projection();
    }

    fn current_manifest(&self) -> &ObjectsPanelManifest {
        &self.manifest
    }

    pub fn set_active_kind(&mut self, kind_id: Option<&str>) {
        self.active_kind_id = kind_id.map(ToString::to_string);
        self.rebuild_active_kind_projection();
    }

    pub fn set_active_scope(&mut self, scope_id: Option<&str>) {
        self.active_scope_id = scope_id.map(ToString::to_string);
        self.active_scope_id = self.resolve_active_scope_id();
        self.rebuild_filtered_objects();
    }

    pub fn replace_kind_objects(&mut self, kind_id: &str, data: ObjectsPanelData) {
        let already_loaded = self.loaded_kind_ids.contains(kind_id);
        Self::merge_kind_rows(&mut self.objects, kind_id, data.rows, already_loaded);
        self.loaded_kind_ids.insert(kind_id.to_string());
        self.rebuild_active_kind_projection();
    }

    pub fn is_kind_loaded(&self, kind_id: &str) -> bool {
        self.loaded_kind_ids.contains(kind_id)
    }

    fn active_kind_label(&self) -> Option<String> {
        self.active_kind_id.as_deref().and_then(|active_kind_id| {
            self.manifest
                .object_kinds
                .iter()
                .find(|kind| kind.id == active_kind_id)
                .map(|kind| kind.label_plural.clone())
        })
    }

    fn active_kind_id(&self) -> Option<&str> {
        self.active_kind_id.as_deref()
    }

    fn active_scope_label(&self) -> String {
        self.active_scope_id
            .clone()
            .unwrap_or_else(|| "All Scopes".to_string())
    }

    fn active_scope_id(&self) -> Option<&str> {
        self.active_scope_id.as_deref()
    }

    fn kind_menu_entries(&self) -> Vec<ObjectsPanelKindMenuEntry> {
        self.manifest
            .object_kinds
            .iter()
            .map(|kind| ObjectsPanelKindMenuEntry {
                kind_id: kind.id.clone(),
                label: kind.label_plural.clone(),
            })
            .collect()
    }

    fn scope_menu_entries(&self) -> Vec<ObjectsPanelScopeMenuEntry> {
        let mut entries = vec![ObjectsPanelScopeMenuEntry {
            scope_id: None,
            label: "All Scopes".to_string(),
        }];

        entries.extend(
            self.available_scope_ids_for_active_kind()
                .into_iter()
                .map(|scope_id| ObjectsPanelScopeMenuEntry {
                    scope_id: Some(scope_id.clone()),
                    label: scope_id,
                }),
        );

        entries
    }

    fn selected_object_refs(&self, right_clicked_row_ix: usize) -> Vec<ObjectsPanelObjectRef> {
        let mut refs: Vec<ObjectsPanelObjectRef> = self
            .context_menu_selected_rows
            .iter()
            .filter_map(|&row_ix| self.filtered_objects.get(row_ix))
            .map(object_ref_from_row)
            .collect();

        if let Some(row) = self.filtered_objects.get(right_clicked_row_ix) {
            let object_ref = object_ref_from_row(row);
            if !refs
                .iter()
                .any(|existing| existing.identity_key == object_ref.identity_key)
            {
                refs.push(object_ref);
            }
        }

        refs
    }

    /// Set the database name for multi-database browsing (MySQL)
    pub fn set_database_name(&mut self, database_name: Option<String>) {
        self.database_name = database_name;
    }

    #[allow(dead_code)]
    pub fn database_name(&self) -> Option<String> {
        self.database_name.clone()
    }

    /// Filter objects by search text
    pub fn filter(&mut self, search_text: &str) {
        self.search_text = search_text.to_string();
        self.rebuild_filtered_objects();
    }

    /// Clear all objects
    pub fn clear(&mut self) {
        let default_data = ObjectsPanelData::from_table_infos(Vec::new());
        self.connection_id = None;
        self.objects.clear();
        self.default_columns = default_data.columns.clone();
        self.filtered_objects.clear();
        self.object_capabilities = SidebarObjectCapabilities::default();
        self.object_features = None;
        self.manifest = ObjectsPanelManifest::default();
        self.active_kind_id = None;
        self.active_scope_id = None;
        self.search_text.clear();
        let (columns, column_ids) = Self::build_ui_columns(&default_data.columns);
        self.columns = columns;
        self.column_ids = column_ids;
    }

    /// Set loading state
    #[allow(dead_code)]
    pub fn set_loading(&mut self, loading: bool) {
        self.is_loading = loading;
    }

    /// Get the filtered objects (for external access)
    pub fn filtered_objects(&self) -> &[ObjectsPanelRow] {
        &self.filtered_objects
    }
}

impl TableDelegate for ObjectsTableDelegate {
    fn columns_count(&self, _cx: &App) -> usize {
        self.columns.len()
    }

    fn rows_count(&self, _cx: &App) -> usize {
        self.filtered_objects.len()
    }

    fn column(&self, col_ix: usize, _cx: &App) -> Column {
        self.columns
            .get(col_ix)
            .cloned()
            .unwrap_or_else(|| Column::new(format!("col-{}", col_ix), format!("Column {}", col_ix)))
    }

    fn loading(&self, _cx: &App) -> bool {
        self.is_loading && self.filtered_objects.is_empty()
    }

    fn perform_sort(
        &mut self,
        col_ix: usize,
        sort: ColumnSort,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) {
        for (idx, col) in self.columns.iter_mut().enumerate() {
            if idx == col_ix {
                *col = col.clone().sort(sort);
            } else {
                *col = col.clone().sort(ColumnSort::Default);
            }
        }

        let Some(col_id) = self.column_ids.get(col_ix).cloned() else {
            cx.notify();
            return;
        };

        self.filtered_objects.sort_by(|a, b| {
            let val_a = a.values.get(&col_id).map(|s| s.as_str()).unwrap_or("");
            let val_b = b.values.get(&col_id).map(|s| s.as_str()).unwrap_or("");

            // Try numeric comparison first, fall back to string comparison
            let ordering = match (val_a.parse::<i64>(), val_b.parse::<i64>()) {
                (Ok(num_a), Ok(num_b)) => num_a.cmp(&num_b),
                _ => val_a.cmp(val_b),
            };

            match sort {
                ColumnSort::Ascending => ordering,
                ColumnSort::Descending => ordering.reverse(),
                ColumnSort::Default => std::cmp::Ordering::Equal,
            }
        });

        cx.notify();
    }

    fn render_td(
        &mut self,
        row_ix: usize,
        col_ix: usize,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        let Some(obj) = self.filtered_objects.get(row_ix) else {
            return div().into_any_element();
        };

        let Some(col_id) = self.column_ids.get(col_ix) else {
            return div().into_any_element();
        };

        // The "name" column (always first) gets an icon prefix
        if col_id == "name" {
            let icon = self
                .manifest
                .object_kinds
                .iter()
                .find(|kind| kind.id == obj.object_kind_id())
                .and_then(|kind| kind.icon_key.as_deref())
                .map(object_icon_from_key)
                .unwrap_or(ZqlzIcon::Table);

            let name = obj
                .values
                .get("name")
                .cloned()
                .unwrap_or_else(|| obj.name.clone());

            return h_flex()
                .h_full()
                .items_center()
                .gap_2()
                .px_2()
                .child(Icon::new(icon).size_4().text_color(theme.accent))
                .child(
                    div()
                        .text_sm()
                        .font_family(theme.mono_font_family.clone())
                        .child(name),
                )
                .into_any_element();
        }

        // All other columns: render the value string from the row's BTreeMap
        let text = obj.values.get(col_id).cloned().unwrap_or_else(|| {
            if self.is_loading {
                "...".to_string()
            } else {
                "-".to_string()
            }
        });

        div()
            .h_full()
            .flex()
            .items_center()
            .px_2()
            .text_sm()
            .text_color(theme.muted_foreground)
            .child(text)
            .into_any_element()
    }

    fn context_menu(
        &mut self,
        row_ix: usize,
        _col_ix: Option<usize>,
        menu: PopupMenu,
        window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> PopupMenu {
        let Some(obj) = self.filtered_objects.get(row_ix) else {
            return menu;
        };

        let object_kind_id = obj.object_kind_id().to_string();
        let menu_entity = cx.entity();
        let panel = self.panel.clone();

        if let Some(connection_id) = self.connection_id {
            let selected_object_refs = self.selected_object_refs(row_ix);
            if let Some(kind) = self
                .manifest
                .object_kinds
                .iter()
                .find(|kind| kind.id == object_kind_id)
            {
                let mut generic_menu = menu;
                let ordered_actions = ObjectsPanel::ordered_action_entries(
                    &kind.row_actions,
                    selected_object_refs.len() > 1,
                );

                let mut last_group: Option<String> = None;
                for entry in &ordered_actions {
                    let action = &entry.action;
                    if let Some(group) = action.group.as_deref()
                        && last_group.as_deref().is_some()
                        && last_group.as_deref() != Some(group)
                    {
                        generic_menu = generic_menu.separator();
                    }

                    let disabled_reason = entry
                        .disabled_reason
                        .clone()
                        .or_else(|| self.action_unavailable_reason(&action.id));

                    generic_menu = generic_menu.item({
                        let panel = panel.clone();
                        let menu_entity = menu_entity.clone();
                        let action_id = action.id.clone();
                        let object_refs = selected_object_refs.clone();
                        PopupMenuItem::new(action.label.clone())
                            .disabled_with_reason(disabled_reason)
                            .on_click(window.listener_for(
                                &menu_entity,
                                move |_this, _, _, cx| {
                                    _ = panel.update(cx, |_panel, cx| {
                                        cx.emit(ObjectsPanelEvent::InvokeAction {
                                            connection_id,
                                            action_id: action_id.clone(),
                                            object_refs: object_refs.clone(),
                                        });
                                    });
                                },
                            ))
                    });

                    last_group = action.group.clone();
                }

                return generic_menu;
            }
        }

        menu
    }

    fn set_context_menu_selection(&mut self, selected_rows: Vec<usize>) {
        self.context_menu_selected_rows = selected_rows;
    }
}

impl ObjectsTableDelegate {
    #[allow(dead_code)]
    fn render_empty(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<TableState<Self>>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let active_kind_label = self.active_kind_label();
        let (empty_title, empty_message) = empty_state_copy(active_kind_label.as_deref());

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .gap_4()
            .child(
                Icon::new(IconName::File)
                    .size(px(48.))
                    .text_color(theme.muted_foreground),
            )
            .child(body_small(empty_title).color(theme.muted_foreground))
            .child(body_small(empty_message).color(theme.muted_foreground))
            .into_any_element()
    }
}

/// Objects panel showing database objects with search
pub struct ObjectsPanel {
    focus_handle: FocusHandle,
    search_input: Entity<InputState>,
    /// Table state for the objects list
    table_state: Entity<TableState<ObjectsTableDelegate>>,
    /// Whether the panel has a valid connection
    has_connection: bool,
    /// Effective object capabilities for the active connection.
    object_capabilities: SidebarObjectCapabilities,
    /// Currently selected connection ID
    selected_connection_id: Option<Uuid>,
    /// Currently selected connection name
    connection_name: Option<String>,
    /// Database name for MySQL multi-database browsing (distinct from connection_name)
    database_name: Option<String>,
    /// Search text for filtering
    search_text: String,
    /// Monotonic ticket source for per-kind loads, so results superseded by a newer
    /// load cannot overwrite fresher rows.
    kind_request_counter: u64,
    /// Ticket taken by the most recent full reload; per-kind results issued before it
    /// describe data that has since been replaced.
    last_reload_ticket: u64,
    /// Latest outstanding ticket per kind.
    latest_kind_tickets: HashMap<String, u64>,
    /// Context menu for empty area right-click
    empty_area_menu: Option<Entity<PopupMenu>>,
    /// Whether the empty area context menu is visible
    empty_area_menu_open: bool,
    /// Position for the empty area context menu
    empty_area_menu_position: Point<Pixels>,
    /// Subscription for the empty area menu dismiss event
    _empty_area_menu_subscription: Option<Subscription>,
}

impl ObjectsPanel {
    pub fn current_manifest(&self, cx: &App) -> ObjectsPanelManifest {
        self.table_state
            .read(cx)
            .delegate()
            .current_manifest()
            .clone()
    }

    /// Convert manifest toolbar actions into deduplicated UI entries while
    /// preserving driver-provided ordering and grouping.
    fn normalized_toolbar_action_entries(
        toolbar_actions: &[ObjectsPanelAction],
    ) -> Vec<ObjectsPanelToolbarActionEntry> {
        let mut entries = Vec::new();
        let mut seen_ids = std::collections::BTreeSet::new();

        for action in toolbar_actions {
            if action.create_object_kind_id.is_some() {
                continue;
            }

            if !seen_ids.insert(action.id.as_str()) {
                continue;
            }

            entries.push(ObjectsPanelToolbarActionEntry {
                id: action.id.clone(),
                label: action.label.clone(),
                icon_key: action.icon_key.clone(),
                group: action.group.clone(),
            });
        }

        entries
    }

    fn normalized_action_entries_for_empty_area(
        toolbar_actions: &[ObjectsPanelAction],
    ) -> Vec<ObjectsPanelToolbarActionEntry> {
        // Keep the empty-area context menu aligned with visible toolbar actions.
        Self::normalized_toolbar_action_entries(toolbar_actions)
    }

    /// Keep every action visible under multi-selection, marking the ones that need a
    /// single object so the menu can say why they are unavailable.
    fn ordered_action_entries(
        actions: &[ObjectsPanelAction],
        is_multi: bool,
    ) -> Vec<OrderedActionEntry> {
        actions
            .iter()
            .map(|action| OrderedActionEntry {
                disabled_reason: (action.requires_single_selection && is_multi)
                    .then(|| SharedString::from(SINGLE_SELECTION_REASON)),
                action: action.clone(),
            })
            .collect()
    }

    fn build_toolbar_buttons(
        &self,
        actions: &[ObjectsPanelToolbarActionEntry],
        cx: &mut Context<Self>,
    ) -> Vec<AnyElement> {
        let mut buttons = Vec::new();

        for action in actions {
            let action_id = action.id.clone();
            let tooltip = action.label.clone();
            let button_id = format!("objects-toolbar-{}", action.id);
            let button = match action.icon_key.as_deref().and_then(action_icon_from_key) {
                Some(icon) => Button::new(button_id)
                    .ghost()
                    .xsmall()
                    .icon(icon)
                    .tooltip(tooltip)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.emit_toolbar_action(&action_id, cx);
                    }))
                    .into_any_element(),
                None => Button::new(button_id)
                    .ghost()
                    .xsmall()
                    .label(action.label.clone())
                    .tooltip(tooltip)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.emit_toolbar_action(&action_id, cx);
                    }))
                    .into_any_element(),
            };

            buttons.push(button);
        }

        buttons
    }

    fn emit_toolbar_action(&self, action_id: &str, cx: &mut Context<Self>) {
        let Some(connection_id) = self.selected_connection_id else {
            return;
        };

        cx.emit(ObjectsPanelEvent::InvokeAction {
            connection_id,
            action_id: action_id.to_string(),
            object_refs: Vec::new(),
        });
    }

    fn set_active_kind(&mut self, kind_id: Option<&str>, cx: &mut Context<Self>) {
        self.table_state.update(cx, |state, cx| {
            state.delegate_mut().set_active_kind(kind_id);
            state.clear_selection(cx);
            state.refresh(cx);
        });
        if let (Some(connection_id), Some(kind_id)) = (self.selected_connection_id, kind_id) {
            let scope_id = self
                .table_state
                .read(cx)
                .delegate()
                .active_scope_id()
                .map(ToString::to_string);
            cx.emit(ObjectsPanelEvent::ActiveKindChanged {
                connection_id,
                kind_id: kind_id.to_string(),
                scope_id,
            });
        }
        cx.notify();
    }

    fn set_active_scope(&mut self, scope_id: Option<&str>, cx: &mut Context<Self>) {
        self.table_state.update(cx, |state, cx| {
            state.delegate_mut().set_active_scope(scope_id);
            state.clear_selection(cx);
            state.refresh(cx);
        });
        let active_kind_id = self
            .table_state
            .read(cx)
            .delegate()
            .active_kind_id()
            .map(ToString::to_string);
        if let (Some(connection_id), Some(kind_id)) = (self.selected_connection_id, active_kind_id)
        {
            cx.emit(ObjectsPanelEvent::ActiveKindChanged {
                connection_id,
                kind_id,
                scope_id: scope_id.map(ToString::to_string),
            });
        }
        cx.notify();
    }

    /// Start a per-kind load and return the ticket its result must still hold to be applied.
    pub fn begin_kind_request(&mut self, kind_id: &str) -> u64 {
        self.kind_request_counter += 1;
        self.latest_kind_tickets
            .insert(kind_id.to_string(), self.kind_request_counter);
        self.kind_request_counter
    }

    /// A ticket is current while it is the newest request for its own kind and no
    /// full reload has replaced the panel's rows since it was issued.
    pub fn is_kind_request_current(&self, kind_id: &str, ticket: u64) -> bool {
        ticket > self.last_reload_ticket
            && self.latest_kind_tickets.get(kind_id) == Some(&ticket)
    }

    /// Apply per-kind rows only if no newer load has superseded the request.
    pub fn replace_kind_objects_if_current(
        &mut self,
        ticket: u64,
        kind_id: &str,
        data: ObjectsPanelData,
        cx: &mut Context<Self>,
    ) {
        if !self.is_kind_request_current(kind_id, ticket) {
            tracing::debug!(
                kind_id,
                ticket,
                last_reload_ticket = self.last_reload_ticket,
                "Dropping superseded objects panel kind result"
            );
            return;
        }

        self.replace_kind_objects(kind_id, data, cx);
    }

    pub fn replace_kind_objects(
        &mut self,
        kind_id: &str,
        data: ObjectsPanelData,
        cx: &mut Context<Self>,
    ) {
        self.table_state.update(cx, |state, cx| {
            state.delegate_mut().replace_kind_objects(kind_id, data);
            if !self.search_text.is_empty() {
                state.delegate_mut().filter(&self.search_text);
            }
            state.refresh(cx);
        });
        cx.notify();
    }

    pub fn set_loading(&mut self, is_loading: bool, cx: &mut Context<Self>) {
        self.table_state.update(cx, |state, cx| {
            state.delegate_mut().set_loading(is_loading);
            state.refresh(cx);
        });
        cx.notify();
    }

    fn resolve_new_object_action_id(&self, cx: &App) -> Option<String> {
        let table_state = self.table_state.read(cx);
        let table_delegate = table_state.delegate();
        resolve_create_action_id(
            table_delegate.active_kind_id(),
            &table_delegate.manifest.toolbar_actions,
        )
    }

    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let search_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Search objects..."));

        // Create the weak reference first (we'll update it after creating the table state)
        let weak_self = cx.weak_entity();

        // Create table state with delegate
        let table_state = cx.new(|cx| {
            let delegate = ObjectsTableDelegate::new(weak_self);
            TableState::new(delegate, window, cx)
        });

        // Subscribe to search input changes
        cx.subscribe(&search_input, |panel, _input, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change) {
                let search_value = panel.search_input.read(cx).value().to_string();
                panel.search_text = search_value.clone();
                panel.table_state.update(cx, |state, cx| {
                    state.delegate_mut().filter(&search_value);
                    cx.notify();
                });
                cx.notify();
            }
        })
        .detach();

        // Subscribe to table events to handle double-click -> open table/view/database
        cx.subscribe(&table_state, |panel, _table, event: &TableEvent, cx| {
            if let TableEvent::DoubleClickedRow(row_ix) = event {
                // Prefer manifest default action for double-click.
                let table_state = panel.table_state.read(cx);
                if let Some(obj) = table_state.delegate().filtered_objects().get(*row_ix)
                    && let Some(connection_id) = panel.selected_connection_id
                {
                    let action_id =
                        resolve_primary_row_action_id(&table_state.delegate().manifest, obj);
                    cx.emit(ObjectsPanelEvent::InvokeAction {
                        connection_id,
                        action_id,
                        object_refs: vec![object_ref_from_row(obj)],
                    });
                }
            }
        })
        .detach();

        Self {
            focus_handle: cx.focus_handle(),
            search_input,
            table_state,
            has_connection: false,
            object_capabilities: SidebarObjectCapabilities::default(),
            selected_connection_id: None,
            connection_name: None,
            database_name: None,
            search_text: String::new(),
            kind_request_counter: 0,
            last_reload_ticket: 0,
            latest_kind_tickets: HashMap::new(),
            empty_area_menu: None,
            empty_area_menu_open: false,
            empty_area_menu_position: Point::default(),
            _empty_area_menu_subscription: None,
        }
    }

    /// Update the objects list with driver-provided extended data
    #[allow(clippy::too_many_arguments)]
    pub fn load_objects(
        &mut self,
        connection_id: Uuid,
        connection_name: String,
        database_name: Option<String>,
        data: ObjectsPanelData,
        manifest: ObjectsPanelManifest,
        object_capabilities: SidebarObjectCapabilities,
        object_features: Option<ObjectFeatureSet>,
        cx: &mut Context<Self>,
    ) {
        self.selected_connection_id = Some(connection_id);
        self.connection_name = Some(connection_name);
        self.database_name = database_name.clone();
        self.has_connection = true;
        self.object_capabilities = object_capabilities;
        // A full reload replaces every row, so any per-kind request still in flight
        // is describing data that no longer exists.
        self.kind_request_counter += 1;
        self.last_reload_ticket = self.kind_request_counter;
        self.latest_kind_tickets.clear();

        self.table_state.update(cx, |state, cx| {
            state
                .delegate_mut()
                .set_object_capabilities(object_capabilities);
            state.delegate_mut().set_object_features(object_features);
            state.delegate_mut().set_database_name(database_name);
            // Rows must land before the manifest so the first active-kind resolution
            // can prefer a kind that actually has data instead of falling back to the
            // manifest's first kind.
            state.delegate_mut().set_extended_data(connection_id, data);
            state.delegate_mut().set_manifest(manifest);
            if !self.search_text.is_empty() {
                state.delegate_mut().filter(&self.search_text);
            }
            state.refresh(cx);
        });

        cx.notify();

        self.request_active_kind_if_unloaded(cx);
    }

    /// Fetch the active kind lazily when the payload we just applied did not carry
    /// its rows. Without this the panel can settle on a kind nobody ever requested
    /// (e.g. MySQL's leading `database` kind) and render an empty table until the
    /// user opens the kind menu.
    fn request_active_kind_if_unloaded(&mut self, cx: &mut Context<Self>) {
        let Some(connection_id) = self.selected_connection_id else {
            return;
        };

        let delegate = self.table_state.read(cx).delegate();
        let Some(kind_id) = delegate.active_kind_id().map(ToString::to_string) else {
            return;
        };
        if delegate.is_kind_loaded(&kind_id) {
            return;
        }
        let scope_id = delegate.active_scope_id().map(ToString::to_string);

        cx.emit(ObjectsPanelEvent::ActiveKindChanged {
            connection_id,
            kind_id,
            scope_id,
        });
    }

    /// Clear the objects list (called when connection is closed)
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.selected_connection_id = None;
        self.connection_name = None;
        self.database_name = None;
        self.has_connection = false;
        self.object_capabilities = SidebarObjectCapabilities::default();
        self.table_state.update(cx, |state, cx| {
            state.delegate_mut().clear();
            state.refresh(cx);
        });
        cx.notify();
    }

    /// Get the currently selected connection ID
    pub fn selected_connection_id(&self) -> Option<Uuid> {
        self.selected_connection_id
    }

    /// Get the current database name (for MySQL multi-database browsing)
    pub fn database_name(&self) -> Option<String> {
        self.database_name.clone()
    }

    /// Set whether the panel has a valid connection
    pub fn set_has_connection(&mut self, has_connection: bool, cx: &mut Context<Self>) {
        self.has_connection = has_connection;
        if !has_connection {
            self.table_state.update(cx, |state, cx| {
                state.delegate_mut().clear();
                state.refresh(cx);
            });
        }
        cx.notify();
    }

    /// Reuse the manifest action path for refresh so the panel stays on the
    /// single generic event model used by other actions.
    ///
    /// This is called by MainView when the user presses Cmd+R while the panel is focused.
    pub fn refresh(&self, cx: &mut Context<Self>) {
        tracing::info!("ObjectsPanel: Refreshing objects list");
        let table_state = self.table_state.read(cx);
        let Some(action_id) =
            resolve_refresh_action_id(&table_state.delegate().manifest.toolbar_actions)
        else {
            tracing::warn!("Objects panel has no manifest refresh action for keyboard refresh");
            return;
        };

        self.emit_toolbar_action(&action_id, cx);
    }

    /// Open the currently selected row (Enter key handler)
    fn open_selected(&mut self, cx: &mut Context<Self>) {
        let Some(connection_id) = self.selected_connection_id else {
            return;
        };
        let table_state = self.table_state.read(cx);
        let Some(row_ix) = table_state.selected_row() else {
            return;
        };
        let delegate = table_state.delegate();
        let Some(obj) = delegate.filtered_objects().get(row_ix) else {
            return;
        };

        let action_id = resolve_primary_row_action_id(&delegate.manifest, obj);
        cx.emit(ObjectsPanelEvent::InvokeAction {
            connection_id,
            action_id,
            object_refs: vec![object_ref_from_row(obj)],
        });
    }

    /// Delete the currently selected object(s) (Delete/Backspace key handler)
    fn delete_selected(&mut self, cx: &mut Context<Self>) {
        let Some(connection_id) = self.selected_connection_id else {
            return;
        };
        let table_state = self.table_state.read(cx);
        let Some(row_ix) = table_state.selected_row() else {
            return;
        };
        let delegate = table_state.delegate();
        let Some(obj) = delegate.filtered_objects().get(row_ix) else {
            return;
        };

        let Some(action_id) = resolve_destructive_row_action_id(&delegate.manifest, obj) else {
            tracing::warn!(
                object_kind_id = obj.object_kind_id(),
                "Objects panel has no manifest destructive action for keyboard DeleteSelected"
            );
            return;
        };

        cx.emit(ObjectsPanelEvent::InvokeAction {
            connection_id,
            action_id,
            object_refs: vec![
                obj.object_ref
                    .clone()
                    .unwrap_or_else(|| object_ref_from_row(obj)),
            ],
        });
    }

    /// Create a new table (Cmd+N handler)
    fn new_object(&mut self, cx: &mut Context<Self>) {
        if self.selected_connection_id.is_none() {
            return;
        }

        let Some(action_id) = self.resolve_new_object_action_id(cx) else {
            tracing::warn!("Objects panel has no manifest create action for keyboard NewObject");
            return;
        };

        self.emit_toolbar_action(&action_id, cx);
    }

    /// Show the empty-area context menu (right-click on empty space)
    fn show_empty_area_menu(
        &mut self,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.selected_connection_id.is_none() {
            return;
        }

        let (toolbar_actions, object_features) = {
            let delegate = self.table_state.read(cx).delegate();
            (
                delegate.manifest.toolbar_actions.clone(),
                delegate.object_features.clone(),
            )
        };
        let empty_area_actions =
            ObjectsPanel::normalized_action_entries_for_empty_area(&toolbar_actions);

        let panel_weak = cx.entity().downgrade();
        let action_context = self.focus_handle.clone();

        let menu = PopupMenu::build(window, cx, move |menu, _, _| {
            let menu = menu.action_context(action_context.clone());
            let mut menu = menu;
            let mut last_group: Option<String> = None;

            for action in &empty_area_actions {
                if let Some(group) = action.group.as_deref()
                    && last_group.as_deref().is_some()
                    && last_group.as_deref() != Some(group)
                {
                    menu = menu.separator();
                }

                let action_id = action.id.clone();
                let panel = panel_weak.clone();
                let disabled_reason = object_features
                    .as_ref()
                    .and_then(|features| object_action_unavailable_reason(features, &action.id));
                menu = menu.item(
                    PopupMenuItem::new(action.label.clone())
                        .disabled_with_reason(disabled_reason)
                        .on_click(move |_, _, cx| {
                            if let Err(error) = panel.update(cx, |panel, cx| {
                                panel.emit_toolbar_action(&action_id, cx);
                            }) {
                                tracing::warn!(
                                    %error,
                                    action_id,
                                    "Failed to invoke empty-area action"
                                );
                            }
                        }),
                );

                last_group = action.group.clone();
            }

            menu
        });

        self._empty_area_menu_subscription =
            Some(cx.subscribe(&menu, |this, _, _: &DismissEvent, cx| {
                this.empty_area_menu_open = false;
                cx.notify();
            }));

        if !menu.focus_handle(cx).contains_focused(window, cx) {
            menu.focus_handle(cx).focus(window, cx);
        }

        self.empty_area_menu = Some(menu);
        self.empty_area_menu_open = true;
        self.empty_area_menu_position = position;
        cx.notify();
    }
}

impl Render for ObjectsPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let (
            active_kind_label,
            active_kind_id,
            kind_menu_entries,
            active_scope_label,
            active_scope_id,
            scope_menu_entries,
            toolbar_actions,
        ) = {
            let table_state = self.table_state.read(cx);
            let table_delegate = table_state.delegate();

            (
                table_delegate
                    .active_kind_label()
                    .unwrap_or_else(|| "Objects".to_string()),
                table_delegate.active_kind_id().map(ToString::to_string),
                table_delegate.kind_menu_entries(),
                table_delegate.active_scope_label(),
                table_delegate.active_scope_id().map(ToString::to_string),
                table_delegate.scope_menu_entries(),
                table_delegate.manifest.toolbar_actions.clone(),
            )
        };

        let show_kind_switcher = self.has_connection && !kind_menu_entries.is_empty();
        let show_scope_switcher = self.has_connection && scope_menu_entries.len() > 1;

        let normalized_toolbar_actions = Self::normalized_toolbar_action_entries(&toolbar_actions);
        let toolbar_buttons = self.build_toolbar_buttons(&normalized_toolbar_actions, cx);

        let panel_entity = cx.entity().downgrade();
        let kind_switcher = Button::new("objects-kind-switcher")
            .ghost()
            .xsmall()
            .label(format!("Kind: {}", active_kind_label))
            .icon(IconName::ChevronDown)
            .dropdown_menu({
                let kind_menu_entries = kind_menu_entries.clone();
                let active_kind_id = active_kind_id.clone();
                let panel_entity = panel_entity.clone();
                move |menu, _window, _cx| {
                    let mut menu = menu.scrollable(true).max_h(px(420.));
                    for entry in &kind_menu_entries {
                        let kind_id = entry.kind_id.clone();
                        let panel = panel_entity.clone();
                        menu = menu.item(
                            PopupMenuItem::new(entry.label.clone())
                                .checked(active_kind_id.as_deref() == Some(kind_id.as_str()))
                                .on_click(move |_, _window, cx| {
                                    if let Err(error) = panel.update(cx, |panel, cx| {
                                        panel.set_active_kind(Some(kind_id.as_str()), cx);
                                    }) {
                                        tracing::warn!(
                                            %error,
                                            kind_id,
                                            "Failed to switch objects panel kind"
                                        );
                                    }
                                }),
                        );
                    }

                    menu
                }
            });

        let scope_switcher = Button::new("objects-scope-switcher")
            .ghost()
            .xsmall()
            .label(format!("Scope: {}", active_scope_label))
            .icon(IconName::ChevronDown)
            .dropdown_menu({
                let scope_menu_entries = scope_menu_entries.clone();
                let active_scope_id = active_scope_id.clone();
                let panel_entity = panel_entity.clone();
                move |menu, _window, _cx| {
                    let mut menu = menu;
                    for entry in &scope_menu_entries {
                        let scope_id = entry.scope_id.clone();
                        let panel = panel_entity.clone();
                        menu = menu.item(
                            PopupMenuItem::new(entry.label.clone())
                                .checked(active_scope_id == scope_id)
                                .on_click(move |_, _window, cx| {
                                    if let Err(error) = panel.update(cx, |panel, cx| {
                                        panel.set_active_scope(scope_id.as_deref(), cx);
                                    }) {
                                        tracing::warn!(
                                            %error,
                                            scope_id = ?scope_id,
                                            "Failed to switch objects panel scope"
                                        );
                                    }
                                }),
                        );
                    }

                    menu
                }
            });

        let theme = cx.theme();

        v_flex()
            .id("objects-panel")
            .key_context("ObjectsPanel")
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(|this, _: &OpenSelected, _, cx| {
                this.open_selected(cx);
            }))
            .on_action(cx.listener(|this, _: &DeleteSelected, _, cx| {
                this.delete_selected(cx);
            }))
            .on_action(cx.listener(|this, _: &NewObject, _, cx| {
                this.new_object(cx);
            }))
            .size_full()
            .bg(theme.background)
            .font_family(theme.font_family.clone())
            .child(
                // Toolbar
                h_flex()
                    .w_full()
                    .p_2()
                    .gap_2()
                    .border_b_1()
                    .border_color(theme.border)
                    .when(show_kind_switcher, |this| this.child(kind_switcher))
                    .when(show_scope_switcher, |this| this.child(scope_switcher))
                    .children(toolbar_buttons)
                    .child(div().flex_1())
                    .child(
                        div().w(px(200.)).child(
                            Input::new(&self.search_input)
                                .prefix(Icon::new(ZqlzIcon::MagnifyingGlass).size_3()),
                        ),
                    ),
            )
            .child(
                // Content area
                div()
                    .id("objects-content")
                    .flex_1()
                    .w_full()
                    .overflow_hidden()
                    .on_mouse_down(
                        MouseButton::Right,
                        cx.listener(|this, event: &MouseDownEvent, window, cx| {
                            this.show_empty_area_menu(event.position, window, cx);
                        }),
                    )
                    .when(!self.has_connection, |this| {
                        this.child(
                            v_flex()
                                .size_full()
                                .items_center()
                                .justify_center()
                                .gap_4()
                                .child(
                                    Icon::new(IconName::File)
                                        .size(px(48.))
                                        .text_color(theme.muted_foreground),
                                )
                                .child(
                                    body_small("No connection selected")
                                        .color(theme.muted_foreground),
                                )
                                .child(
                                    body_small("Connect to a database to view objects")
                                        .color(theme.muted_foreground),
                                ),
                        )
                    })
                    .when(self.has_connection, |this| {
                        this.child(
                            Table::new(&self.table_state)
                                .stripe(true)
                                .bordered(false)
                                .small(),
                        )
                    }),
            )
            .when_some(
                self.empty_area_menu
                    .clone()
                    .filter(|_| self.empty_area_menu_open),
                |el, menu| {
                    el.child(
                        deferred(
                            anchored()
                                .snap_to_window_with_margin(px(8.))
                                .anchor(Anchor::TopLeft)
                                .position(self.empty_area_menu_position)
                                .child(div().occlude().cursor_default().child(menu)),
                        )
                        .with_priority(1),
                    )
                },
            )
    }
}

impl Focusable for ObjectsPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for ObjectsPanel {}
impl EventEmitter<ObjectsPanelEvent> for ObjectsPanel {}

impl Panel for ObjectsPanel {
    fn panel_name(&self) -> &'static str {
        "ObjectsPanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(name) = &self.connection_name {
            format!("Objects - {}", name)
        } else {
            "Objects".to_string()
        }
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ObjectsPanel, ObjectsTableDelegate, empty_state_copy, resolve_create_action_id,
        resolve_destructive_row_action_id, resolve_primary_row_action_id,
        resolve_refresh_action_id,
    };
    use uuid::Uuid;
    use gpui::SharedString;
    use zqlz_core::{
        ObjectsPanelAction, ObjectsPanelData, ObjectsPanelManifest, ObjectsPanelObjectKind,
        ObjectsPanelObjectRef, ObjectsPanelRow, SINGLE_SELECTION_REASON,
    };

    fn row(kind_id: &str, name: &str) -> ObjectsPanelRow {
        ObjectsPanelRow {
            name: name.to_string(),
            schema: None,
            object_type: kind_id.to_string(),
            object_ref: Some(ObjectsPanelObjectRef::new(kind_id, name)),
            values: std::collections::BTreeMap::new(),
            redis_database_index: None,
            key_value_info: None,
        }
    }

    fn row_with_values(kind_id: &str, name: &str, values: &[(&str, &str)]) -> ObjectsPanelRow {
        let mut row = row(kind_id, name);
        row.values = values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        row
    }

    fn manifest_with_kind_default(
        kind_id: &str,
        default_action_id: Option<&str>,
        row_action_ids: &[&str],
    ) -> ObjectsPanelManifest {
        let row_actions = row_action_ids
            .iter()
            .map(|action_id| ObjectsPanelAction::new(*action_id, action_id.to_uppercase()))
            .collect();

        let mut kind =
            ObjectsPanelObjectKind::new(kind_id, "Kind", "Kinds").row_actions(row_actions);
        if let Some(default_action_id) = default_action_id {
            kind = kind.default_row_action(default_action_id);
        }

        ObjectsPanelManifest {
            object_kinds: vec![kind],
            toolbar_actions: Vec::new(),
        }
    }

    #[test]
    fn ordered_action_entries_keep_single_selection_actions_under_multi_select() {
        let actions = vec![
            ObjectsPanelAction::new("open", "Open"),
            ObjectsPanelAction::new("design", "Design").single_selection(),
        ];

        let entries = ObjectsPanel::ordered_action_entries(&actions, true);

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].disabled_reason, None);
        assert_eq!(
            entries[1].disabled_reason,
            Some(SharedString::from(SINGLE_SELECTION_REASON))
        );
    }

    #[test]
    fn ordered_action_entries_enable_every_action_for_single_selection() {
        let actions = vec![ObjectsPanelAction::new("design", "Design").single_selection()];

        let entries = ObjectsPanel::ordered_action_entries(&actions, false);

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].disabled_reason, None);
    }

    #[test]
    fn resolve_primary_row_action_prefers_valid_manifest_default() {
        let manifest = manifest_with_kind_default("table", Some("design"), &["open", "design"]);
        let row = row("table", "users");

        let action_id = resolve_primary_row_action_id(&manifest, &row);

        assert_eq!(action_id, "design");
    }

    #[test]
    fn resolve_primary_row_action_falls_back_to_open_for_unknown_default() {
        let manifest = manifest_with_kind_default("table", Some("rename"), &["open", "design"]);
        let row = row("table", "users");

        let action_id = resolve_primary_row_action_id(&manifest, &row);

        assert_eq!(action_id, "open");
    }

    #[test]
    fn resolve_primary_row_action_falls_back_to_open_without_kind_default() {
        let manifest = manifest_with_kind_default("table", None, &["open", "design"]);
        let row = row("table", "users");

        let action_id = resolve_primary_row_action_id(&manifest, &row);

        assert_eq!(action_id, "open");
    }

    #[test]
    fn resolve_create_action_prefers_active_kind_action() {
        let toolbar_actions = vec![
            ObjectsPanelAction::new("refresh", "Refresh"),
            ObjectsPanelAction::new("make_table", "New Table").create_object_kind("table"),
            ObjectsPanelAction::new("make_view", "New View").create_object_kind("view"),
        ];

        let action_id = resolve_create_action_id(Some("view"), &toolbar_actions);

        assert_eq!(action_id, Some("make_view".to_string()));
    }

    #[test]
    fn resolve_create_action_uses_first_create_action_when_active_kind_missing() {
        let toolbar_actions = vec![
            ObjectsPanelAction::new("refresh", "Refresh"),
            ObjectsPanelAction::new("make_table", "New Table").create_object_kind("table"),
            ObjectsPanelAction::new("make_view", "New View").create_object_kind("view"),
        ];

        let action_id = resolve_create_action_id(Some("function"), &toolbar_actions);

        assert_eq!(action_id, Some("make_table".to_string()));
    }

    #[test]
    fn resolve_create_action_returns_none_without_create_actions() {
        let toolbar_actions = vec![
            ObjectsPanelAction::new("refresh", "Refresh"),
            ObjectsPanelAction::new("import", "Import"),
        ];

        let action_id = resolve_create_action_id(Some("table"), &toolbar_actions);

        assert_eq!(action_id, None);
    }

    #[test]
    fn resolve_create_action_ignores_action_id_naming_without_create_metadata() {
        let toolbar_actions = vec![
            ObjectsPanelAction::new("new_table", "New Table"),
            ObjectsPanelAction::new("create_view", "New View"),
        ];

        let action_id = resolve_create_action_id(Some("table"), &toolbar_actions);

        assert_eq!(action_id, None);
    }

    #[test]
    fn resolve_refresh_action_uses_manifest_metadata() {
        let toolbar_actions = vec![
            ObjectsPanelAction::new("reload_catalog", "Reload")
                .icon_key("refresh")
                .refreshes_objects_panel(),
            ObjectsPanelAction::new("refresh", "Legacy Refresh"),
        ];

        let action_id = resolve_refresh_action_id(&toolbar_actions);

        assert_eq!(action_id, Some("reload_catalog".to_string()));
    }

    #[test]
    fn resolve_destructive_row_action_uses_manifest_metadata() {
        let manifest = ObjectsPanelManifest {
            object_kinds: vec![
                ObjectsPanelObjectKind::new("table", "Table", "Tables").row_actions(vec![
                    ObjectsPanelAction::new("open", "Open"),
                    ObjectsPanelAction::new("drop_relation", "Drop").destructive(),
                ]),
            ],
            toolbar_actions: Vec::new(),
        };
        let row = row("table", "users");

        let action_id = resolve_destructive_row_action_id(&manifest, &row);

        assert_eq!(action_id, Some("drop_relation".to_string()));
    }

    #[test]
    fn empty_state_copy_uses_active_kind_label() {
        let (title, message) = empty_state_copy(Some("Materialized Views"));

        assert_eq!(title, "No materialized views found");
        assert_eq!(message, "Try changing the kind, scope, or search.");
    }

    #[test]
    fn empty_state_copy_falls_back_to_generic_message() {
        let (title, message) = empty_state_copy(None);

        assert_eq!(title, "No objects found");
        assert_eq!(message, "Try changing the kind, scope, or search.");
    }

    fn manifest_with_kinds(kind_ids: &[&str]) -> ObjectsPanelManifest {
        ObjectsPanelManifest {
            object_kinds: kind_ids
                .iter()
                .map(|kind_id| ObjectsPanelObjectKind::new(*kind_id, "Kind", "Kinds"))
                .collect(),
            toolbar_actions: Vec::new(),
        }
    }

    fn delegate_with_kinds(kind_ids: &[&str]) -> ObjectsTableDelegate {
        let mut delegate = ObjectsTableDelegate::new(gpui::WeakEntity::new_invalid());
        delegate.set_manifest(manifest_with_kinds(kind_ids));
        delegate
    }

    fn data(rows: Vec<ObjectsPanelRow>) -> ObjectsPanelData {
        ObjectsPanelData {
            columns: Vec::new(),
            rows,
        }
    }

    #[test]
    fn active_kind_prefers_a_kind_with_rows_over_the_leading_manifest_kind() {
        // MySQL lists `database` first, but the bootstrap payload only carries tables.
        // `load_objects` applies rows before the manifest so the resolution sees them.
        let mut delegate = ObjectsTableDelegate::new(gpui::WeakEntity::new_invalid());

        delegate.set_extended_data(Uuid::new_v4(), data(vec![row("table", "orders")]));
        delegate.set_manifest(manifest_with_kinds(&["database", "table"]));

        assert_eq!(delegate.active_kind_id(), Some("table"));
        assert!(delegate.is_kind_loaded("table"));
        assert!(!delegate.is_kind_loaded("database"));
    }

    #[test]
    fn a_sticky_active_kind_without_rows_is_reported_as_unloaded() {
        // The user's kind choice survives a reload that did not carry its rows, so the
        // panel must be able to tell "not fetched" from "empty" and re-request it.
        let mut delegate = delegate_with_kinds(&["database", "table"]);
        delegate.set_active_kind(Some("database"));

        delegate.set_extended_data(Uuid::new_v4(), data(vec![row("table", "orders")]));

        assert_eq!(delegate.active_kind_id(), Some("database"));
        assert!(!delegate.is_kind_loaded("database"));
    }

    #[test]
    fn full_payload_marks_kinds_it_omits_as_unloaded() {
        let mut delegate = delegate_with_kinds(&["database", "table"]);
        delegate.replace_kind_objects("database", data(vec![row("database", "erp_lab")]));
        assert!(delegate.is_kind_loaded("database"));

        delegate.set_extended_data(Uuid::new_v4(), data(vec![row("table", "orders")]));

        assert!(!delegate.is_kind_loaded("database"));
    }

    #[test]
    fn first_kind_load_accepts_a_genuinely_empty_result() {
        let mut delegate = delegate_with_kinds(&["database"]);

        delegate.replace_kind_objects("database", data(Vec::new()));

        assert!(delegate.is_kind_loaded("database"));
    }

    #[test]
    fn merge_kind_rows_preserves_visible_inventory_when_enrichment_is_empty() {
        let mut rows = vec![row_with_values("table", "orders", &[("name", "orders")])];

        ObjectsTableDelegate::merge_kind_rows(&mut rows, "table", Vec::new(), true);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].object_name(), "orders");
        assert_eq!(
            rows[0].values.get("name").map(String::as_str),
            Some("orders")
        );
    }

    #[test]
    fn merge_kind_rows_hydrates_existing_inventory_rows_by_identity() {
        let mut rows = vec![row_with_values("table", "orders", &[("name", "orders")])];
        let incoming = vec![row_with_values(
            "table",
            "orders",
            &[("name", "orders"), ("owner", "postgres"), ("oid", "123")],
        )];

        ObjectsTableDelegate::merge_kind_rows(&mut rows, "table", incoming, true);

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values.get("name").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            rows[0].values.get("owner").map(String::as_str),
            Some("postgres")
        );
        assert_eq!(rows[0].values.get("oid").map(String::as_str), Some("123"));
    }

    #[test]
    fn merge_kind_rows_preserves_unmatched_inventory_rows_and_appends_new_rows() {
        let mut rows = vec![row_with_values("table", "orders", &[("name", "orders")])];
        let incoming = vec![row_with_values(
            "table",
            "refunds",
            &[("name", "refunds"), ("owner", "postgres")],
        )];

        ObjectsTableDelegate::merge_kind_rows(&mut rows, "table", incoming, true);

        let names = rows.iter().map(|row| row.object_name()).collect::<Vec<_>>();
        assert_eq!(names, vec!["orders", "refunds"]);
    }

    #[test]
    fn normalized_action_entries_for_empty_area_retains_manifest_actions() {
        let toolbar_actions = vec![
            ObjectsPanelAction::new("refresh", "Refresh"),
            ObjectsPanelAction::new("make_table", "New Table").create_object_kind("table"),
            ObjectsPanelAction::new("import", "Import"),
            ObjectsPanelAction::new("export", "Export"),
        ];

        let entry_ids: Vec<String> =
            ObjectsPanel::normalized_action_entries_for_empty_area(&toolbar_actions)
                .into_iter()
                .map(|entry| entry.id)
                .collect();

        assert_eq!(entry_ids, vec!["refresh", "import", "export"]);
    }
}
