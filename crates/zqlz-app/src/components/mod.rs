//! UI Components for ZQLZ
//!
//! This module exports all reusable UI components.

mod cell_editor;
mod command_palette;
mod error_modal;
mod inspector_panel;
mod key_value_editor;
mod project_manager;
pub mod table_viewer;
mod template_library;
mod welcome_panel;

pub use cell_editor::{CellData, CellEditorEvent, CellEditorPanel};
pub use command_palette::{Command, CommandPalette, CommandPaletteEvent};
pub use inspector_panel::{InspectorPanel, InspectorPanelEvent, InspectorView};
pub use key_value_editor::{
    KeyValueData, KeyValueEditorEvent, KeyValueEditorPanel, RedisValueType, RowData,
    RowEditorMode, TtlOption,
};
// Re-export ConnectionSidebar from zqlz-connection for backward compatibility
pub use error_modal::{ErrorData, ErrorModal, ErrorModalEvent, SystemInfoDisplay};
pub use project_manager::{ProjectManagerEvent, ProjectManagerPanel};
pub use template_library::{TemplateLibraryEvent, TemplateLibraryPanel};
pub use zqlz_connection::{ConnectionEntry, ConnectionSidebar, ConnectionSidebarEvent};
// Re-export query widgets from zqlz-query for backward compatibility
pub use zqlz_query::widgets::{QueryHistoryPanel, QueryHistoryPanelEvent};
pub use zqlz_query::{
    ExplainResult, ProblemEntry, ProblemsPanel, ProblemsPanelEvent, ProblemSeverity, QueryEditor,
    QueryEditorEvent, QueryExecution, QueryTabsPanel, QueryTabsPanelEvent, ResultsPanel,
    ResultsPanelEvent, StatementResult,
};
// Re-export schema widgets from zqlz-schema for backward compatibility
pub use zqlz_schema::{
    ColumnInfo, DatabaseSchemaData, ForeignKeyInfo, IndexInfo, ObjectsPanel,
    ObjectsPanelEvent, SchemaDetails, SchemaDetailsPanel, SchemaDetailsPanelEvent, SchemaNode,
    SchemaNodeType, SchemaTreeEvent, SchemaTreePanel,
};
// Re-export SettingsPanel from zqlz-settings for backward compatibility
pub use zqlz_settings::{SettingsPanel, SettingsPanelEvent};
// Re-export TableDesigner from zqlz-table-designer for backward compatibility
pub use table_viewer::{
    FilterCondition, PendingCellChange, SortCriterion, SortDirection, TableViewerEvent, TableViewerPanel,
};
pub use welcome_panel::WelcomePanel;
pub use zqlz_table_designer::{DesignerTab, TableDesignerEvent, TableDesignerPanel};
