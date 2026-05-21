use crate::{
    SqlStatementSpan, SyntaxDriverCapabilities, SyntaxExecutionUnitMode, sql_statement_span_at,
    uses_command_syntax, uses_sql_syntax_overlays,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionUnit {
    pub source: String,
    pub sql_span: Option<SqlStatementSpan>,
    pub byte_range: std::ops::Range<usize>,
}

pub fn execution_unit_for_profile(
    source: &str,
    cursor_offset: usize,
    syntax_profile: &'static str,
) -> ExecutionUnit {
    if uses_command_syntax(syntax_profile) {
        return command_execution_unit(source, cursor_offset);
    }

    if !uses_sql_syntax_overlays(syntax_profile) {
        return ExecutionUnit {
            source: source.to_string(),
            sql_span: None,
            byte_range: 0..source.len(),
        };
    }

    sql_execution_unit(source, cursor_offset)
}

pub fn execution_unit_for_capabilities(
    source: &str,
    cursor_offset: usize,
    capabilities: &SyntaxDriverCapabilities,
) -> ExecutionUnit {
    match capabilities.execution_unit {
        SyntaxExecutionUnitMode::CommandLine => command_execution_unit(source, cursor_offset),
        SyntaxExecutionUnitMode::WholeDocument => whole_document_execution_unit(source),
        SyntaxExecutionUnitMode::SqlStatement => sql_execution_unit(source, cursor_offset),
    }
}

fn whole_document_execution_unit(source: &str) -> ExecutionUnit {
    ExecutionUnit {
        source: source.to_string(),
        sql_span: None,
        byte_range: 0..source.len(),
    }
}

fn command_execution_unit(source: &str, cursor_offset: usize) -> ExecutionUnit {
    let cursor_offset = cursor_offset.min(source.len());
    let line_start = source[..cursor_offset]
        .rfind('\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    let line_end = source[cursor_offset..]
        .find('\n')
        .map(|index| cursor_offset + index)
        .unwrap_or(source.len());
    ExecutionUnit {
        source: source[line_start..line_end].to_string(),
        sql_span: None,
        byte_range: line_start..line_end,
    }
}

fn sql_execution_unit(source: &str, cursor_offset: usize) -> ExecutionUnit {
    let Some(span) = sql_statement_span_at(source, cursor_offset) else {
        return ExecutionUnit {
            source: source.to_string(),
            sql_span: None,
            byte_range: 0..source.len(),
        };
    };

    let byte_range = span.start..span.end;
    ExecutionUnit {
        source: source[byte_range.clone()].to_string(),
        sql_span: Some(span),
        byte_range,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_profile_uses_current_line() {
        let source = "GET user:1\nSET user:1 Ada\nGET user:1";
        let unit = execution_unit_for_profile(source, source.find("Ada").expect("value"), "redis");

        assert_eq!(unit.source, "SET user:1 Ada");
        assert_eq!(
            unit.byte_range,
            source.find("SET").expect("set")..source.find("\nGET").expect("next command")
        );
        assert!(unit.sql_span.is_none());
    }

    #[test]
    fn command_capabilities_use_current_line() {
        let source = "GET user:1\nSET user:1 Ada\nGET user:1";
        let capabilities = crate::get_syntax_driver_capabilities("redis");
        let unit = execution_unit_for_capabilities(
            source,
            source.find("Ada").expect("value"),
            &capabilities,
        );

        assert_eq!(unit.source, "SET user:1 Ada");
        assert_eq!(
            unit.byte_range,
            source.find("SET").expect("set")..source.find("\nGET").expect("next command")
        );
        assert!(unit.sql_span.is_none());
    }

    #[test]
    fn document_profile_uses_whole_document() {
        let source = "db.users.find({ active: true })\ndb.users.countDocuments()";
        let unit = execution_unit_for_profile(
            source,
            source.find("count").expect("count command"),
            "mongodb",
        );

        assert_eq!(unit.source, source);
        assert_eq!(unit.byte_range, 0..source.len());
        assert!(unit.sql_span.is_none());
    }

    #[test]
    fn document_capabilities_use_whole_document() {
        let source = "db.users.find({ active: true })\ndb.users.countDocuments()";
        let capabilities = crate::get_syntax_driver_capabilities("mongodb");
        let unit = execution_unit_for_capabilities(
            source,
            source.find("count").expect("count command"),
            &capabilities,
        );

        assert_eq!(unit.source, source);
        assert_eq!(unit.byte_range, 0..source.len());
        assert!(unit.sql_span.is_none());
    }

    #[test]
    fn sql_profile_uses_current_statement_span() {
        let source = "select 1;\nselect 2;";
        let unit = execution_unit_for_profile(
            source,
            source.find('2').expect("second statement"),
            "postgresql",
        );

        assert_eq!(unit.source, "select 2");
        assert_eq!(
            unit.byte_range,
            source.find("select 2").expect("second select")..source.len() - 1
        );
        assert!(unit.sql_span.is_some());
    }
}
