mod diagnostics;
#[allow(clippy::module_inception)]
mod highlighter;
mod languages;
mod registry;

pub use diagnostics::*;
pub use highlighter::*;
pub use languages::*;
pub use registry::*;
