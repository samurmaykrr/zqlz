//! Schema UI widgets
//!
//! GPUI panels for browsing and displaying database schema information.

mod object_designer_panel;
mod objects_panel;
mod schema_details_panel;
mod schema_tree;

pub use object_designer_panel::{ObjectDesignerPanel, ObjectDesignerPanelEvent};
pub use objects_panel::{DeleteSelected, NewObject, ObjectsPanel, ObjectsPanelEvent, OpenSelected};
pub use schema_details_panel::{
    ColumnInfo, ForeignKeyInfo, IndexInfo, SchemaDetails, SchemaDetailsPanel,
    SchemaDetailsPanelEvent,
};
pub use schema_tree::{
    DatabaseSchemaData, SchemaNode, SchemaNodeType, SchemaTreeEvent, SchemaTreePanel,
};
