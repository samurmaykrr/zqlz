// Stub for Zed's task crate

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct ResolvedTask {
    pub id: String,
    pub label: String,
}

#[derive(Clone, Debug)]
pub struct TaskTemplate {
    pub label: String,
}

#[derive(Clone, Debug)]
pub struct TaskContext {
    pub cwd: Option<std::path::PathBuf>,
}

#[derive(Clone, Debug)]
pub struct TaskVariables;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VariableName(pub String);

#[derive(Clone, Debug)]
pub enum DebugScenario {
    Run,
    Attach,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunnableTag {
    Test,
    Example,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RevealStrategy {
    Always,
    Never,
}
