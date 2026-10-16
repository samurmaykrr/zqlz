//! Events emitted by the Table Designer Panel

use uuid::Uuid;

use crate::models::TableDesign;

/// Events emitted by the TableDesignerPanel
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum TableDesignerEvent {
    /// User wants to save the design.
    /// The parent should execute the DDL via the connection.
    Save {
        /// The connection to execute DDL on
        connection_id: Uuid,
        /// The table design to save
        design: TableDesign,
        /// Whether this is a new table (CREATE) or existing (ALTER)
        is_new: bool,
        /// The original design before modifications (needed for ALTER TABLE diff)
        original_design: Option<TableDesign>,
    },

    /// User cancelled the design.
    /// The parent should close the panel.
    Cancel,

    /// Request DDL preview generation.
    /// Optional - for async DDL generation if needed.
    PreviewDdl {
        /// The design to generate DDL for
        design: TableDesign,
    },
}

/// Tab lifecycle notifications for an existing table's designer.
///
/// Kept separate from [`TableDesignerEvent`] so the host can follow the designed
/// table (e.g. in the schema inspector) without every `TableDesignerEvent` match
/// having to know about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableDesignerActivity {
    BecameActive,
    BecameInactive,
}
