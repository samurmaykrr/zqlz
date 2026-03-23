//! Flattened virtual row model for the connection sidebar.
//!
//! The sidebar uses a single virtualized list, so every visible tree node is
//! represented as a normalized row variant.

use uuid::Uuid;

pub(super) const SIDEBAR_ROW_HEIGHT: f32 = 24.0;

#[derive(Clone, Debug)]
pub(super) enum SidebarRowIcon {
    Folder,
    Database,
    Table,
    View,
    MaterializedView,
    Trigger,
    Function,
    Procedure,
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
        database_name: Option<String>,
        schema_name: String,
    },
    SchemaGroupSection {
        conn_id: Uuid,
        database_name: Option<String>,
        schema_name: String,
        section: &'static str,
    },
}

#[derive(Clone, Debug)]
pub(super) enum SidebarLeafKind {
    Table {
        conn_id: Uuid,
        open_table_name: String,
        menu_table_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
    },
    View {
        conn_id: Uuid,
        open_view_name: String,
        menu_view_name: String,
        object_schema: Option<String>,
        database_name: Option<String>,
    },
    MaterializedView {
        conn_id: Uuid,
        open_view_name: String,
        menu_view_name: String,
        database_name: Option<String>,
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
    },
    Procedure {
        conn_id: Uuid,
        procedure_name: String,
        object_schema: Option<String>,
    },
    Query {
        conn_id: Uuid,
        query_id: Uuid,
        query_name: String,
    },
    RedisDatabase {
        conn_id: Uuid,
        database_index: u16,
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
    pub(super) has_database_schema: bool,
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

#[derive(Default)]
pub(super) struct SchemaSectionGroup {
    pub(super) tables: Vec<String>,
    pub(super) views: Vec<String>,
    pub(super) materialized_views: Vec<String>,
    pub(super) triggers: Vec<String>,
    pub(super) functions: Vec<String>,
    pub(super) procedures: Vec<String>,
}
