//! ZQLZ Application Library
//!
//! This library provides testable components of the ZQLZ application,
//! primarily the SQL LSP implementation.

#[cfg(not(target_os = "macos"))]
use gpui::{Entity, Global};
#[cfg(not(target_os = "macos"))]
use zqlz_ui::widgets::menu::AppMenuBar;

pub mod actions;
pub mod sql_lsp;
pub mod window_manager;
pub mod workspace;
pub mod workspace_state;

#[cfg(not(target_os = "macos"))]
pub struct AppMenuBarGlobal(pub Entity<AppMenuBar>);

#[cfg(not(target_os = "macos"))]
impl Global for AppMenuBarGlobal {}
