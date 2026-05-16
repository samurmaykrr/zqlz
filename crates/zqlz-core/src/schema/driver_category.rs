use serde::{Deserialize, Serialize};

/// Database driver category for determining UI behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DriverCategory {
    /// Traditional SQL databases (PostgreSQL, MySQL, SQLite, etc.)
    #[default]
    Relational,
    /// Key-Value stores (Redis, Memcached, Valkey, etc.)
    KeyValue,
    /// Document databases (MongoDB, CouchDB, etc.)
    Document,
    /// Time-series databases (InfluxDB, TimescaleDB, etc.)
    TimeSeries,
    /// Graph databases (Neo4j, etc.)
    Graph,
    /// Search engines (Elasticsearch, etc.)
    Search,
}
