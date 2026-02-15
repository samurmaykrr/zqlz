//! SQL Parameter Extraction and Binding
//!
//! This module provides utilities for extracting and binding parameters from SQL queries.
//! It supports multiple parameter styles used by different databases:
//!
//! - Named parameters: `:name`, `@name`, `$name`
//! - Positional parameters: `$1`, `$2`, `?`
//!
//! # Example
//!
//! ```
//! use zqlz_query::parameters::{extract_parameters, Parameter};
//!
//! let sql = "SELECT * FROM users WHERE id = :id AND status = $1";
//! let params = extract_parameters(sql);
//! // Returns [Named("id"), Positional(1)]
//! ```

pub mod binder;
mod extractor;

pub use binder::{BindError, BindResult, BoundQuery, bind_named, bind_positional};
pub use extractor::{
    ExtractionResult, Parameter, ParameterStyle, extract_parameters, extract_parameters_with_style,
};

#[cfg(test)]
mod tests;
