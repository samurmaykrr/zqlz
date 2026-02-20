//! Context menu modules for the connection sidebar.
//!
//! This module contains implementations of right-click context menus for different
//! types of sidebar nodes:
//!
//! - **sidebar_menu**: Context menu for the sidebar background
//! - **connection_menu**: Context menu for connection nodes
//! - **table_menu**: Context menu for table nodes
//! - **view_menu**: Context menu for view nodes
//! - **function_menu**: Context menu for function nodes
//! - **procedure_menu**: Context menu for procedure nodes
//! - **trigger_menu**: Context menu for trigger nodes
//! - **query_menu**: Context menu for saved query nodes
//! - **state**: Context menu state management

pub(super) mod state;

mod connection_menu;
mod function_menu;
mod procedure_menu;
mod query_menu;
mod sidebar_menu;
mod table_menu;
mod trigger_menu;
mod view_menu;

// Re-export the ContextMenuState for use in parent module
