//! Flattened virtual row model for the connection sidebar.
//!
//! The sidebar uses a single virtualized list, so every visible tree node is
//! represented as a normalized row variant.

use std::collections::BTreeMap;

use uuid::Uuid;
use zqlz_core::TableRef;

use crate::widgets::sidebar::types::SidebarSection;

pub(super) const SIDEBAR_ROW_HEIGHT: f32 = 24.0;

#[derive(Clone, Copy, Debug)]
pub(super) enum SidebarRowIcon {
    Folder,
    Database,
    Table,
    View,
    MaterializedView,
    Trigger,
    Function,
    Procedure,
    Event,
    Sequence,
    Domain,
    Type,
    Extension,
    Field,
    Index,
    ForeignKey,
    Constraint,
    Rule,
    Query,
}

#[derive(Clone, Debug)]
pub(super) enum SidebarSectionAction {
    RedisDatabases {
        conn_id: Uuid,
    },
    ConnectionSection {
        conn_id: Uuid,
        section: &'static str,
    },
    DatabaseSection {
        conn_id: Uuid,
        database_name: String,
        section: &'static str,
    },
    SchemaGroup {
        conn_id: Uuid,
        database_name: String,
        schema_name: String,
    },
    SchemaGroupSection {
        conn_id: Uuid,
        database_name: String,
        schema_name: String,
        section: &'static str,
    },
    Queries {
        conn_id: Uuid,
    },
}

#[derive(Clone, Debug)]
pub(super) enum SidebarLeafKind {
    Table {
        conn_id: Uuid,
        table: TableRef,
    },
    View {
        conn_id: Uuid,
        view: TableRef,
    },
    MaterializedView {
        conn_id: Uuid,
        view: TableRef,
    },
    Trigger {
        conn_id: Uuid,
        trigger_name: String,
        object_schema: Option<String>,
    },
    Function {
        conn_id: Uuid,
        function_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
    },
    Procedure {
        conn_id: Uuid,
        procedure_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
    },
    MetadataObject {
        conn_id: Uuid,
        object_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
        object_type: &'static str,
    },
    TableChildSummary,
    Query {
        conn_id: Uuid,
        query_id: Uuid,
        query_name: String,
    },
    QueryFolder {
        conn_id: Uuid,
        folder: String,
    },
    RedisDatabase {
        conn_id: Uuid,
        database_index: u16,
    },
    DocumentCollection {
        conn_id: Uuid,
        database_name: String,
        collection_name: String,
        object_type: &'static str,
    },
}

#[derive(Clone, Debug)]
pub(super) struct ConnectionRow {
    pub(super) conn_id: Uuid,
    pub(super) conn_name: String,
    pub(super) db_type: String,
    pub(super) is_connected: bool,
    pub(super) is_connecting: bool,
}

#[derive(Clone, Debug)]
pub(super) struct DatabaseRow {
    pub(super) conn_id: Uuid,
    pub(super) database_name: String,
    pub(super) is_expanded: bool,
    pub(super) has_schema: bool,
    pub(super) is_active: bool,
    pub(super) size_label: Option<String>,
}

#[derive(Clone, Debug)]
pub(super) struct SchemaNodeRow {
    pub(super) conn_id: Uuid,
    pub(super) database_name: String,
    pub(super) schema_name: String,
    pub(super) is_expanded: bool,
}

#[derive(Clone, Debug)]
pub(super) struct SectionRow {
    pub(super) element_id: String,
    pub(super) icon: SidebarRowIcon,
    pub(super) label: String,
    pub(super) total_count: usize,
    pub(super) filtered_count: usize,
    pub(super) is_expanded: bool,
    pub(super) depth: usize,
    pub(super) action: SidebarSectionAction,
    pub(super) context_menu_section: Option<&'static str>,
}

#[derive(Clone, Debug)]
pub(super) struct LeafRow {
    pub(super) element_id: String,
    pub(super) icon: SidebarRowIcon,
    pub(super) label: String,
    pub(super) depth: usize,
    pub(super) kind: SidebarLeafKind,
}

#[derive(Clone, Debug)]
pub(super) struct LoadingRow {
    pub(super) element_id: String,
    pub(super) text: String,
    pub(super) depth: usize,
}

#[derive(Clone, Debug)]
pub(super) struct NoResultsRow {
    pub(super) query: String,
}

#[derive(Clone, Debug)]
pub(super) enum SidebarVirtualRow {
    Connection(ConnectionRow),
    Database(DatabaseRow),
    SchemaNode(SchemaNodeRow),
    Section(SectionRow),
    Leaf(LeafRow),
    Loading(LoadingRow),
    NoResults(NoResultsRow),
}

/// Objects of one schema, keyed by section. Sections with no objects may be
/// absent; iteration follows [`SidebarSection`]'s render order.
#[derive(Default)]
pub(super) struct SchemaSectionGroup(pub(super) BTreeMap<SidebarSection, Vec<String>>);

impl SchemaSectionGroup {
    pub(super) fn objects(&self, section: SidebarSection) -> &[String] {
        self.0.get(&section).map(Vec::as_slice).unwrap_or(&[])
    }

    pub(super) fn push(&mut self, section: SidebarSection, object_name: String) {
        self.0.entry(section).or_default().push(object_name);
    }

    pub(super) fn total_count(&self) -> usize {
        self.0.values().map(Vec::len).sum()
    }
}
