//! Schema UI widgets
//!
//! GPUI panels for browsing and displaying database schema information.

mod objects_panel;
mod schema_details_panel;
mod schema_tree;

pub use objects_panel::{ObjectsPanel, ObjectsPanelEvent};
pub use schema_details_panel::{
    ColumnInfo, ForeignKeyInfo, IndexInfo, SchemaDetails, SchemaDetailsPanel,
    SchemaDetailsPanelEvent,
};
pub use schema_tree::{
    DatabaseSchemaData, SchemaNode, SchemaNodeType, SchemaTreeEvent, SchemaTreePanel,
};
