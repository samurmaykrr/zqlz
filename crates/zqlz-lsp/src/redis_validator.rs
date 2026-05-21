//! Redis Command Validator
//!
//! Provides validation for Redis commands using:
//! 1. Proper tokenization (handles quotes, escapes, multi-line)
//! 2. Command specifications (arity, argument types)
//!
//! This follows the industry standard approach used by redis-cli and RedisInsight.

use crate::redis_validation_error::ValidationError;
use lsp_types::Diagnostic;
use std::collections::HashMap;
use zqlz_core::{
    DiagnosticSeverity as CoreSeverity,
    command::{CommandTokenizer, ParsedCommand, parse_commands},
    redis_command_catalog,
    redis_command_spec::RedisCommandSpec,
};

/// Redis validator using proper tokenization and command specs
pub struct RedisValidator {
    /// Command specifications (command name -> spec)
    commands: &'static HashMap<String, RedisCommandSpec>,
}

impl Default for RedisValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisValidator {
    #[allow(dead_code)]
    /// Get command specification by name
    pub fn get_command(&self, name: &str) -> Option<&RedisCommandSpec> {
        self.commands.get(&name.to_uppercase())
    }

    #[allow(dead_code)]
    /// Check if a command exists
    pub fn command_exists(&self, name: &str) -> bool {
        self.commands.contains_key(&name.to_uppercase())
    }

    /// Create a new validator with built-in command specs
    pub fn new() -> Self {
        Self {
            commands: redis_command_catalog::builtin_commands(),
        }
    }

    /// Validate a single command
    pub fn validate_command(&self, cmd: &ParsedCommand) -> Vec<ValidationError> {
        let mut errors = Vec::new();
        let command_upper = cmd.command.to_uppercase();

        // Check if command exists
        let Some(spec) = self.commands.get(&command_upper) else {
            errors.push(ValidationError {
                message: format!("Unknown Redis command: {}", cmd.command),
                severity: CoreSeverity::Error,
                start: cmd.command_token.start,
                end: cmd.command_token.end,
                line: cmd.command_token.line,
                column: cmd.command_token.column,
                help: Some("Check Redis documentation for valid commands".into()),
            });
            return errors;
        };

        // Check for deprecated commands
        if spec.deprecated {
            let help = spec
                .replaced_by
                .as_ref()
                .map(|r| format!("Use {} instead", r))
                .unwrap_or_else(|| "This command is deprecated".into());

            errors.push(ValidationError {
                message: format!("{} is deprecated", command_upper),
                severity: CoreSeverity::Warning,
                start: cmd.command_token.start,
                end: cmd.command_token.end,
                line: cmd.command_token.line,
                column: cmd.command_token.column,
                help: Some(help),
            });
        }

        // Validate argument count
        let arg_count = cmd.args.len();
        if !spec.is_valid_arg_count(arg_count) {
            let expected = if spec.arity < 0 {
                format!("at least {} argument(s)", spec.min_args())
            } else {
                format!("exactly {} argument(s)", spec.min_args())
            };

            errors.push(ValidationError {
                message: format!(
                    "{} requires {}, but got {}",
                    command_upper, expected, arg_count
                ),
                severity: CoreSeverity::Error,
                start: cmd.command_token.start,
                end: if cmd.arg_tokens.is_empty() {
                    cmd.command_token.end
                } else {
                    cmd.arg_tokens
                        .last()
                        .map(|t| t.end)
                        .unwrap_or(cmd.command_token.end)
                },
                line: cmd.command_token.line,
                column: cmd.command_token.column,
                help: Some(format!("{}: {}", command_upper, spec.summary)),
            });
        }

        errors
    }

    /// Validate all commands in text
    pub fn validate(&self, text: &str, case_sensitive: bool) -> Vec<ValidationError> {
        let mut tokenizer = CommandTokenizer::new(text, case_sensitive);
        let tokens = tokenizer.tokenize();
        let commands = parse_commands(&tokens);

        let mut all_errors = Vec::new();
        for cmd in commands {
            all_errors.extend(self.validate_command(&cmd));
        }

        all_errors
    }

    /// Convert validation errors to LSP diagnostics
    pub fn validate_to_diagnostics(&self, text: &str, case_sensitive: bool) -> Vec<Diagnostic> {
        self.validate(text, case_sensitive)
            .into_iter()
            .map(|e| e.to_diagnostic())
            .collect()
    }
}

#[cfg(test)]
#[path = "redis_validator_tests.rs"]
mod tests;
