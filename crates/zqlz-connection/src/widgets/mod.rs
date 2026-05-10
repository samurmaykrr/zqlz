//! Connection UI widgets
//!
//! This module contains UI widgets related to database connections.

mod connection_form;
mod connection_picker;
mod sidebar;

pub use connection_form::{ConnectionForm, ConnectionFormEvent};
pub use connection_picker::{ConnectionPicker, ConnectionPickerEvent, DatabaseType};
pub use sidebar::{
    ActivateConnection, ConnectionEntry, ConnectionSidebar, ConnectionSidebarEvent,
    DeleteSelectedConnection, SavedQueryInfo, SchemaObjects, ShowContextMenu,
    SidebarObjectCapabilities, SidebarSection, SidebarTableDetailsData, SidebarTableKey,
};
