use lsp_types::{Diagnostic, DiagnosticSeverity, Position, Range};
use zqlz_core::DiagnosticSeverity as CoreSeverity;

/// Validation error with position information.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub message: String,
    pub severity: CoreSeverity,
    pub start: usize,
    pub end: usize,
    pub line: usize,
    pub column: usize,
    pub help: Option<String>,
}

impl ValidationError {
    pub fn to_diagnostic(&self) -> Diagnostic {
        let severity = match self.severity {
            CoreSeverity::Error => DiagnosticSeverity::ERROR,
            CoreSeverity::Warning => DiagnosticSeverity::WARNING,
            CoreSeverity::Info => DiagnosticSeverity::INFORMATION,
            CoreSeverity::Hint => DiagnosticSeverity::HINT,
        };

        let mut message = self.message.clone();
        if let Some(help) = &self.help {
            message.push_str(&format!("\n\n{help}"));
        }

        Diagnostic {
            range: Range::new(
                Position::new(self.line as u32, self.column as u32),
                Position::new(
                    self.line as u32,
                    (self.column + (self.end - self.start)) as u32,
                ),
            ),
            severity: Some(severity),
            message,
            source: Some("redis-validator".to_string()),
            ..Default::default()
        }
    }
}
