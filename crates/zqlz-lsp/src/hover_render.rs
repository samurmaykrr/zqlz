use lsp_types::{Hover, HoverContents, MarkupContent, MarkupKind};
use zqlz_core::{DialectInfo, ForeignKeyInfo, SequenceInfo};

use super::{
    ColumnInfo, FunctionInfo, IndexInfo, ParameterDirection, ProcedureInfo, TableInfo, TriggerInfo,
    ViewInfo,
};

pub(crate) fn markdown_hover(hover_text: String) -> Hover {
    Hover {
        contents: HoverContents::Markup(MarkupContent {
            kind: MarkupKind::Markdown,
            value: hover_text,
        }),
        range: None,
    }
}

pub(crate) fn create_column_hover(col: &ColumnInfo, table_name: Option<&str>) -> Hover {
    let mut hover_text = format!("**Column: {}**\n\n", col.name);

    if let Some(table) = table_name {
        hover_text.push_str(&format!("Table: `{}`\n", table));
    }

    hover_text.push_str(&format!("Type: `{}`\n", col.data_type));
    let mut attributes = Vec::new();
    if col.is_primary_key {
        attributes.push("PRIMARY KEY");
    }
    if col.is_foreign_key {
        attributes.push("FOREIGN KEY");
    }
    if !col.nullable {
        attributes.push("NOT NULL");
    }

    if !attributes.is_empty() {
        hover_text.push_str(&format!("Attributes: {}\n", attributes.join(", ")));
    }

    if let Some(default) = &col.default_value {
        hover_text.push_str(&format!("Default: `{}`\n", default));
    }
    if let Some(comment) = &col.comment {
        hover_text.push_str(&format!("\n{}\n", comment));
    }

    markdown_hover(hover_text)
}

pub(crate) fn create_table_hover(
    table: &TableInfo,
    columns: Option<&[ColumnInfo]>,
    foreign_keys: Option<&[ForeignKeyInfo]>,
    reverse_foreign_keys: Option<&[(String, ForeignKeyInfo)]>,
) -> Hover {
    let mut hover_text = format!("**Table: {}**\n\n", table.name);

    if let Some(schema) = &table.schema {
        hover_text.push_str(&format!("Schema: `{}`\n", schema));
    }

    if let Some(comment) = &table.comment {
        hover_text.push_str(&format!("\n{}\n\n", comment));
    }

    if let Some(row_count) = table.row_count {
        hover_text.push_str(&format!("Rows: ~{}\n\n", row_count));
    }

    if let Some(columns) = columns
        && !columns.is_empty()
    {
        hover_text.push_str("\n**Columns:**\n");
        for column in columns {
            let mut column_line = format!("- `{}`: {}", column.name, column.data_type);

            if column.is_primary_key {
                column_line.push_str(" **PK**");
            }
            if column.is_foreign_key {
                column_line.push_str(" **FK**");
            }
            if !column.nullable {
                column_line.push_str(" NOT NULL");
            }

            hover_text.push_str(&column_line);
            hover_text.push('\n');
        }
    }

    if let Some(foreign_keys) = foreign_keys
        && !foreign_keys.is_empty()
    {
        hover_text.push_str("\n**Foreign Keys:**\n");
        for foreign_key in foreign_keys {
            hover_text.push_str(&format!(
                "- `{}` → `{}.{}`\n",
                foreign_key.columns.join(", "),
                foreign_key.referenced_table,
                foreign_key.referenced_columns.join(", ")
            ));
        }
    }

    if let Some(reverse_foreign_keys) = reverse_foreign_keys
        && !reverse_foreign_keys.is_empty()
    {
        hover_text.push_str("\n**Referenced By:**\n");
        for (source_table, foreign_key) in reverse_foreign_keys {
            hover_text.push_str(&format!(
                "- `{}.{}` → `{}`\n",
                source_table,
                foreign_key.columns.join(", "),
                foreign_key.referenced_columns.join(", ")
            ));
        }
    }

    markdown_hover(hover_text)
}

pub(crate) fn create_derived_table_hover(name: &str, columns: &[String], kind: &str) -> Hover {
    let mut hover_text = format!("**{}: {}**\n\n", kind, name);

    if columns.is_empty() {
        hover_text.push_str("Projected columns are not available.\n");
    } else {
        hover_text.push_str("**Columns:**\n");
        for column in columns {
            hover_text.push_str(&format!("- `{}`\n", column));
        }
    }

    markdown_hover(hover_text)
}

pub(crate) fn create_dialect_metadata_hover(
    word: &str,
    dialect_info: &DialectInfo,
    dialect_name: &str,
) -> Option<Hover> {
    if let Some(keyword) = dialect_info
        .keywords
        .iter()
        .find(|keyword| keyword.keyword.eq_ignore_ascii_case(word))
    {
        let mut hover_text = format!("**{}**\n\n", keyword.keyword);
        if let Some(description) = &keyword.description {
            hover_text.push_str(description);
        } else {
            hover_text.push_str(&format!("{} keyword", dialect_name));
        }
        if let Some(documentation) = &keyword.documentation {
            hover_text.push_str("\n\n");
            hover_text.push_str(documentation);
        }

        return Some(markdown_hover(hover_text));
    }

    if let Some(function) = dialect_info
        .functions
        .iter()
        .find(|function| function.name.eq_ignore_ascii_case(word))
    {
        let mut hover_text = format!("**{}()**\n\n", function.name);
        if let Some(description) = &function.description {
            hover_text.push_str(description);
        } else {
            hover_text.push_str(&format!("{} function", dialect_name));
        }
        if !function.signatures.is_empty() {
            hover_text.push_str("\n\n**Signatures:**\n");
            for signature in &function.signatures {
                hover_text.push_str("- `");
                hover_text.push_str(signature.signature.as_ref());
                hover_text.push_str("`\n");
            }
        }
        if let Some(return_type) = &function.return_type {
            hover_text.push_str("\nReturns: `");
            hover_text.push_str(return_type.as_ref());
            hover_text.push('`');
        }

        return Some(markdown_hover(hover_text));
    }

    None
}

pub(crate) fn create_sequence_hover(sequence: &SequenceInfo) -> Hover {
    let mut hover_text = format!("**Sequence: {}**\n\n", sequence.name);

    if let Some(schema) = &sequence.schema {
        hover_text.push_str(&format!("Schema: `{}`\n", schema));
    }

    hover_text.push_str(&format!("Type: `{}`\n", sequence.data_type));
    hover_text.push_str(&format!("Start: `{}`\n", sequence.start_value));
    hover_text.push_str(&format!("Min: `{}`\n", sequence.min_value));
    hover_text.push_str(&format!("Max: `{}`\n", sequence.max_value));
    hover_text.push_str(&format!("Increment: `{}`\n", sequence.increment_by));

    if let Some(current_value) = sequence.current_value {
        hover_text.push_str(&format!("Current: `{}`\n", current_value));
    }

    if let Some(owner) = &sequence.owner {
        hover_text.push_str(&format!("Owner: `{}`\n", owner));
    }

    if let Some(comment) = &sequence.comment {
        hover_text.push_str(&format!("\n{}\n", comment));
    }

    markdown_hover(hover_text)
}

pub(crate) fn create_view_hover(view: &ViewInfo) -> Hover {
    let mut hover_text = format!("**View: {}**\n\n", view.name);

    if let Some(schema) = &view.schema {
        hover_text.push_str(&format!("Schema: `{}`\n\n", schema));
    }

    if let Some(definition) = &view.definition {
        hover_text.push_str("**Definition:**\n```sql\n");
        hover_text.push_str(definition);
        hover_text.push_str("\n```\n");
    }

    markdown_hover(hover_text)
}

pub(crate) fn create_procedure_hover(procedure: &ProcedureInfo) -> Hover {
    let mut hover_text = format!("**Stored Procedure: {}**\n\n", procedure.name);

    if let Some(schema) = &procedure.schema {
        hover_text.push_str(&format!("Schema: `{}`\n\n", schema));
    }

    if let Some(comment) = &procedure.comment {
        hover_text.push_str(&format!("{}\n\n", comment));
    }

    if !procedure.parameters.is_empty() {
        hover_text.push_str("**Parameters:**\n");
        for param in &procedure.parameters {
            let direction = match param.direction {
                ParameterDirection::In => "IN",
                ParameterDirection::Out => "OUT",
                ParameterDirection::InOut => "INOUT",
            };
            hover_text.push_str(&format!(
                "- `{}` ({}): {}\n",
                param.name, direction, param.data_type
            ));
        }
        hover_text.push('\n');
    }

    if let Some(return_type) = &procedure.return_type {
        hover_text.push_str(&format!("**Returns:** `{}`\n\n", return_type));
    }

    if let Some(definition) = &procedure.definition {
        hover_text.push_str("**Definition:**\n```sql\n");
        hover_text.push_str(definition);
        hover_text.push_str("\n```\n");
    }

    markdown_hover(hover_text)
}

pub(crate) fn create_function_hover(function: &FunctionInfo) -> Hover {
    let mut hover_text = format!(
        "**{}: {}**\n\n",
        if function.is_aggregate {
            "Aggregate Function"
        } else {
            "Function"
        },
        function.name
    );

    if let Some(schema) = &function.schema {
        hover_text.push_str(&format!("Schema: `{}`\n\n", schema));
    }

    hover_text.push_str(&format!("**Returns:** `{}`\n\n", function.return_type));

    if let Some(comment) = &function.comment {
        hover_text.push_str(&format!("{}\n\n", comment));
    }

    if !function.parameters.is_empty() {
        hover_text.push_str("**Parameters:**\n");
        for param in &function.parameters {
            hover_text.push_str(&format!("- `{}`: {}\n", param.name, param.data_type));
        }
        hover_text.push('\n');
    }

    if let Some(definition) = &function.definition {
        hover_text.push_str("**Definition:**\n```sql\n");
        hover_text.push_str(definition);
        hover_text.push_str("\n```\n");
    }

    markdown_hover(hover_text)
}

pub(crate) fn create_index_hover(index: &IndexInfo) -> Hover {
    let mut hover_text = format!(
        "**{}: {}**\n\n",
        if index.is_unique {
            "Unique Index"
        } else {
            "Index"
        },
        index.name
    );

    hover_text.push_str(&format!("Table: `{}`\n", index.table_name));

    if !index.columns.is_empty() {
        hover_text.push_str(&format!("Columns: `{}`\n", index.columns.join(", ")));
    }

    markdown_hover(hover_text)
}

pub(crate) fn create_trigger_hover(trigger: &TriggerInfo) -> Hover {
    let mut hover_text = format!("**Trigger: {}**\n\n", trigger.name);

    hover_text.push_str(&format!("Table: `{}`\n", trigger.table_name));
    hover_text.push_str(&format!("Timing: {} {}\n\n", trigger.timing, trigger.event));

    if let Some(definition) = &trigger.definition {
        hover_text.push_str("**Definition:**\n```sql\n");
        hover_text.push_str(definition);
        hover_text.push_str("\n```\n");
    }

    markdown_hover(hover_text)
}
