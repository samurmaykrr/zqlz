//! ZQLZ Universal Data Interchange Format (UDIF)
//!
//! This crate provides a universal format for exporting and importing data between
//! any database systems. It defines a canonical type system that can represent
//! types from MongoDB, Redis, PostgreSQL, MySQL, SQLite, and more.
//!
//! # Architecture
//!
//! ```text
//! Source DB → Driver Exporter → UDIF Document → Driver Importer → Target DB
//!                    ↓                                ↓
//!              Type Mapping                    Type Mapping
//!              (source→canonical)              (canonical→target)
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! // Export from MongoDB
//! let exporter = MongoExporter::new(mongo_connection);
//! let document = exporter.export_table("users").await?;
//!
//! // Import to PostgreSQL
//! let importer = PostgresImporter::new(pg_connection);
//! importer.import(&document, ImportOptions::default()).await?;
//! ```

mod canonical_types;
mod csv_export;
mod csv_import;
mod document;
mod exporter;
mod importer;
mod type_mapping;
mod value_encoding;
pub mod widgets;

pub use canonical_types::*;
pub use csv_export::{CsvExportError, CsvExportProgress, CsvExportProgressCallback, CsvExporter};
pub use csv_import::{
    CsvImportError, CsvImportProgress, CsvImportProgressCallback, CsvImportResult, CsvImporter,
    CsvPreview, preview_csv_file,
};
pub use document::*;
pub use exporter::{
    ExportError, ExportOptions, ExportPhase, ExportProgress, ExportProgressCallback, Exporter,
    GenericExporter,
};
pub use importer::{
    GenericImporter, IfTableExists, ImportError, ImportOptions, ImportPhase, ImportPreview,
    ImportProgress, ImportProgressCallback, ImportResult, ImportWarning, ImportWarningKind,
    Importer, TypeWarning,
};
pub use type_mapping::*;
pub use value_encoding::*;

pub mod export_helpers {
    pub use super::exporter::helpers::*;
}

pub mod import_helpers {
    pub use super::importer::helpers::*;
}
