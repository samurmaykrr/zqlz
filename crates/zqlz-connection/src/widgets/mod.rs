//! Connection UI widgets
//!
//! This module contains UI widgets related to database connections.

mod sidebar;

pub use sidebar::{
    ActivateConnection, ConnectionEntry, ConnectionSidebar, ConnectionSidebarEvent,
    DeleteSelectedConnection, SavedQueryInfo, ShowContextMenu,
};
