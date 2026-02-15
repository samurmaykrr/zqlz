// Stub for Zed's edit_prediction_types crate

use gpui::Entity;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Next,
    Previous,
    Prev, // Alias for Previous
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EditPredictionGranularity {
    Character,
    Word,
    Line,
    Full,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuggestionDisplayType {
    Inline,
    Overlay,
}

pub trait EditPredictionDelegate: Send + Sync {}

pub trait EditPredictionDelegateHandle {
    fn boxed_clone(&self) -> Box<dyn EditPredictionDelegateHandle>;
}

#[derive(Clone, Debug)]
pub struct EditPrediction {
    pub text: String,
}

#[derive(Clone, Debug)]
pub struct EditPredictionIconSet {
    pub icon: String,
}
