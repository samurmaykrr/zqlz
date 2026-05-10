//! Workspace shell primitives for center-tab behavior.

mod close;
mod controller;
mod item;
mod title_bar;

pub use close::{
    WorkspaceItemCloseIntent, WorkspaceSaveCloseDecision, close_intent_for_panel,
    dirty_new_query_close_prompt, dirty_non_query_close_prompt, dirty_saved_query_close_prompt,
    save_close_decision,
};
pub use controller::{
    WorkspaceCloseResult, WorkspaceController, WorkspaceWindowCloseIntent,
    force_close_active_tab_and_dialog, window_close_discard_prompt,
};
pub use item::{WorkspaceItemHandle, WorkspaceItemMetadata};
pub use title_bar::workspace_title_bar;
