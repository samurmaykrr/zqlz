//! Schema statistics collection and management.
//!
//! This module provides functionality for collecting table and index statistics
//! from various database systems.

mod collector;

#[cfg(test)]
mod tests;

pub use collector::{
    CollectorConfig, IndexStatistics, SchemaStatistics, StatisticsCollector, StatisticsConnection,
    StatisticsQuery, TableStatistics,
};
