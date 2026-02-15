//! Events emitted by the Trigger Designer Panel

use uuid::Uuid;

use crate::models::TriggerDesign;

/// Events emitted by the TriggerDesignerPanel
#[derive(Debug, Clone)]
pub enum TriggerDesignerEvent {
    /// User wants to save the trigger.
    /// The parent should execute the DDL via the connection.
    Save {
        /// The connection to execute DDL on
        connection_id: Uuid,
        /// The trigger design to save
        design: TriggerDesign,
        /// Whether this is a new trigger (CREATE) or existing (DROP + CREATE)
        is_new: bool,
        /// The original trigger name (for DROP when renaming)
        original_name: Option<String>,
    },

    /// User cancelled the design.
    /// The parent should close the panel.
    Cancel,

    /// Request DDL preview generation.
    PreviewDdl {
        /// The design to generate DDL for
        design: TriggerDesign,
    },
}
