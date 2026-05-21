#[derive(Debug, Clone, PartialEq)]
pub enum ValidationSeverity {
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone)]
pub struct ValidationIssue {
    pub severity: ValidationSeverity,
    pub message: String,
    pub symbol: Option<String>,
    pub symbol_role: Option<ValidationSymbolRole>,
    pub qualifier: Option<String>,
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValidationSymbolRole {
    Column,
    Table,
    Wildcard,
}
