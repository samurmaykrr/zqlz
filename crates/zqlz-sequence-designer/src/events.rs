//! Events emitted by the Sequence Designer Panel.

use uuid::Uuid;

use crate::models::SequenceDesign;

#[derive(Debug, Clone)]
pub enum SequenceDesignerEvent {
    Save {
        connection_id: Uuid,
        design: SequenceDesign,
        is_new: bool,
        original_name: Option<String>,
    },
    Cancel,
    PreviewDdl {
        design: SequenceDesign,
    },
}
