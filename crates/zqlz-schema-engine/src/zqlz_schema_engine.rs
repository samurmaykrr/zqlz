//! Shared schema-introspection engine.
//!
//! [`SchemaEngine`] implements [`zqlz_core::SchemaIntrospection`] once, on top
//! of a driver-provided [`zqlz_core::CatalogSource`]. All normalization,
//! composition, heuristics, and objects-panel derivation live here so they
//! exist a single time instead of being copied across every driver.
//!
//! Drivers shrink to a `CatalogSource` adapter (run catalog queries, report
//! raw records) plus a [`zqlz_core::CatalogDialect`]; the engine owns the rest.

mod engine;
mod recorded_catalog;

pub mod contract;

pub use engine::{DefaultDialect, SchemaEngine};
pub use recorded_catalog::{CatalogSnapshot, RecordedCatalog};
