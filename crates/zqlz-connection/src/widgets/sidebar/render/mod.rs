//! Rendering functions for the connection sidebar
//!
//! This module contains all UI rendering helpers for the connection sidebar,
//! organized into a clear hierarchy:
//!
//! - **Icon/Logo helpers**: Map database types to themed icons and colored logos
//! - **Tree building blocks**: Section headers, leaf items, and database-specific trees
//! - **Connection rendering**: Top-level connection entries with their schema trees
//!
//! # Architecture
//!
//! The sidebar renders a hierarchical tree with three main levels:
//!
//! 1. **Connection level**: Each database connection (PostgreSQL, MySQL, Redis, etc.)
//! 2. **Database/Schema level**: Either schema objects directly (SQLite) or a list of
//!    databases on the server (PostgreSQL, MySQL), each with their own schema
//! 3. **Object level**: Tables, views, triggers, functions, procedures, and saved queries
//!
//! ## Rendering Flow
//!
//! ```text
//! render_connection
//!   ├─> render_redis_schema_tree (for Redis connections)
//!   │     └─> render_section_header (Databases, Queries sections)
//!   │           └─> render_leaf_item (individual databases, keys)
//!   │
//!   └─> render_schema_tree (for SQL databases)
//!         └─> render_objects_tree (for each database or connection)
//!               ├─> render_section_header (Tables, Views, etc.)
//!               └─> render_leaf_item (individual objects)
//! ```
//!
//! ## Indentation & Depth
//!
//! The tree uses a depth-based indentation system where each level adds 12px:
//! - Depth 1: Connection-level sections (8 + 12 = 20px)
//! - Depth 2: Database nodes or schema sections (8 + 24 = 32px)
//! - Depth 3: Objects within a database (8 + 36 = 44px)
//!
//! ## Search & Filtering
//!
//! When a search query is active, the rendering logic:
//! - Filters items at each level to show only matches
//! - Auto-expands sections containing matches
//! - Shows count as "filtered/total" in section headers
//!
//! ## Multi-Database Support
//!
//! Some database drivers (PostgreSQL, MySQL) support multiple databases per connection.
//! The renderer adapts based on whether `databases` is populated:
//! - Empty: Single database mode - objects render directly under connection
//! - Populated: Multi-database mode - intermediate database nodes appear in tree

use gpui::{Hsla, SharedString};
use uuid::Uuid;

use crate::widgets::sidebar::{
    RedisDatabaseInfo, SavedQueryInfo, SidebarDatabaseInfo, SidebarObjectCapabilities,
};

pub(super) struct LeafItemProps<Icon, OnClick, OnRightClick> {
    pub element_id: SharedString,
    pub icon: Icon,
    pub label: String,
    pub on_click: OnClick,
    pub on_right_click: Option<OnRightClick>,
    pub list_hover: Hsla,
    pub depth: usize,
}

pub(super) struct SectionHeaderProps<'a, Icon, OnClick, OnRightClick> {
    pub element_id: SharedString,
    pub icon: Icon,
    pub label: &'a str,
    pub total_count: usize,
    pub filtered_count: usize,
    pub is_expanded: bool,
    pub on_click: OnClick,
    pub on_right_click: Option<OnRightClick>,
    pub muted_foreground: Hsla,
    pub list_hover: Hsla,
    pub depth: usize,
}

pub(super) struct SqlSchemaTreeProps<'a> {
    pub conn_id: Uuid,
    pub object_capabilities: SidebarObjectCapabilities,
    pub tables: &'a [String],
    pub views: &'a [String],
    pub materialized_views: &'a [String],
    pub triggers: &'a [String],
    pub functions: &'a [String],
    pub procedures: &'a [String],
    pub queries: &'a [SavedQueryInfo],
    pub tables_expanded: bool,
    pub views_expanded: bool,
    pub materialized_views_expanded: bool,
    pub triggers_expanded: bool,
    pub functions_expanded: bool,
    pub procedures_expanded: bool,
    pub queries_expanded: bool,
    pub tables_loading: bool,
    pub views_loading: bool,
    pub materialized_views_loading: bool,
    pub triggers_loading: bool,
    pub functions_loading: bool,
    pub procedures_loading: bool,
    pub databases: &'a [SidebarDatabaseInfo],
    pub schema_name: Option<&'a str>,
    pub schema_expanded: bool,
}

pub(super) struct RedisSchemaTreeProps<'a> {
    pub conn_id: Uuid,
    pub databases: &'a [RedisDatabaseInfo],
    pub databases_expanded: bool,
    pub queries: &'a [SavedQueryInfo],
    pub queries_expanded: bool,
}

mod connection;
mod icons;
mod leaf_item;
mod objects_tree;
mod redis_tree;
mod schema_tree;
mod section_header;
