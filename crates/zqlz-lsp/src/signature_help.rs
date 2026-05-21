use lsp_types::{ParameterInformation, ParameterLabel, SignatureHelp, SignatureInformation};
use sqlparser::dialect::Dialect;
use zqlz_core::{DialectInfo, sql_function_call_context};
use zqlz_ui::widgets::Rope;

pub(crate) fn get_signature_help(
    text: &Rope,
    offset: usize,
    dialect: &dyn Dialect,
    dialect_info: &DialectInfo,
) -> Option<SignatureHelp> {
    tracing::debug!("get_signature_help: offset={}", offset);

    let sql = text.to_string();
    let before_cursor = &sql[..crate::clamp_to_char_boundary(&sql, offset)];

    let (function_name, active_parameter) = sql_function_call_context(before_cursor, dialect)?;

    tracing::debug!(
        "Found function call context: func={}, active_param={}",
        function_name,
        active_parameter
    );

    let function_info = dialect_info
        .functions
        .iter()
        .find(|function| function.name.eq_ignore_ascii_case(&function_name))?;

    if function_info.signatures.is_empty() {
        return None;
    }

    let signatures: Vec<SignatureInformation> = function_info
        .signatures
        .iter()
        .map(|signature| {
            let parameters: Vec<ParameterInformation> = signature
                .parameters
                .iter()
                .map(|parameter| ParameterInformation {
                    label: ParameterLabel::Simple(format!(
                        "{}: {}",
                        parameter.name, parameter.param_type
                    )),
                    documentation: parameter.description.as_ref().map(|description| {
                        lsp_types::Documentation::String(description.to_string())
                    }),
                })
                .collect();

            SignatureInformation {
                label: signature.signature.to_string(),
                documentation: function_info
                    .description
                    .as_ref()
                    .map(|description| lsp_types::Documentation::String(description.to_string())),
                parameters: Some(parameters),
                active_parameter: None,
            }
        })
        .collect();

    let active_signature = if signatures.len() == 1 { Some(0) } else { None };

    Some(SignatureHelp {
        signatures,
        active_signature,
        active_parameter: Some(active_parameter as u32),
    })
}
