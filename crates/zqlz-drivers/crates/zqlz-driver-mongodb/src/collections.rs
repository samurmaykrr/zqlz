//! MongoDB collection introspection module
//!
//! This module provides functionality for browsing and inspecting MongoDB collections:
//! - List databases on the server
//! - List collections in a database
//! - Get collection statistics and metadata
//! - Infer schema from document samples
//! - List and manage indexes

use crate::MongoDbConnection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use zqlz_core::{Connection, Result, ZqlzError};

/// Information about a MongoDB database
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatabaseInfo {
    /// The database name
    pub name: String,
    /// Size in bytes
    pub size_bytes: Option<u64>,
    /// Whether this is an empty database
    pub empty: bool,
}

impl DatabaseInfo {
    /// Create a new DatabaseInfo
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            size_bytes: None,
            empty: false,
        }
    }

    /// Set size in bytes
    pub fn with_size(mut self, bytes: u64) -> Self {
        self.size_bytes = Some(bytes);
        self
    }

    /// Set empty flag
    pub fn with_empty(mut self, empty: bool) -> Self {
        self.empty = empty;
        self
    }
}

/// Information about a MongoDB collection
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectionInfo {
    /// The collection name
    pub name: String,
    /// The collection type (collection, view, timeseries)
    pub collection_type: CollectionType,
    /// Whether the collection is capped
    pub capped: bool,
    /// Number of documents (estimated)
    pub document_count: Option<u64>,
    /// Size in bytes (data + indexes)
    pub size_bytes: Option<u64>,
    /// Average document size in bytes
    pub avg_doc_size: Option<u64>,
    /// Storage size (includes pre-allocated space)
    pub storage_size: Option<u64>,
    /// Number of indexes
    pub index_count: Option<u32>,
}

impl CollectionInfo {
    /// Create a new CollectionInfo
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            collection_type: CollectionType::Collection,
            capped: false,
            document_count: None,
            size_bytes: None,
            avg_doc_size: None,
            storage_size: None,
            index_count: None,
        }
    }

    /// Set collection type
    pub fn with_type(mut self, collection_type: CollectionType) -> Self {
        self.collection_type = collection_type;
        self
    }

    /// Set capped flag
    pub fn with_capped(mut self, capped: bool) -> Self {
        self.capped = capped;
        self
    }

    /// Set document count
    pub fn with_document_count(mut self, count: u64) -> Self {
        self.document_count = Some(count);
        self
    }

    /// Set size information
    pub fn with_sizes(mut self, size: u64, storage: u64, avg_doc: u64) -> Self {
        self.size_bytes = Some(size);
        self.storage_size = Some(storage);
        self.avg_doc_size = Some(avg_doc);
        self
    }

    /// Set index count
    pub fn with_index_count(mut self, count: u32) -> Self {
        self.index_count = Some(count);
        self
    }
}

/// MongoDB collection types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CollectionType {
    /// Regular collection
    Collection,
    /// View (read-only, computed from other collections)
    View,
    /// Time series collection
    TimeSeries,
    /// System collection
    System,
}

impl CollectionType {
    /// Parse from MongoDB listCollections response
    pub fn from_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "view" => CollectionType::View,
            "timeseries" => CollectionType::TimeSeries,
            "system" => CollectionType::System,
            _ => CollectionType::Collection,
        }
    }

    /// Get the type string
    pub fn as_str(&self) -> &'static str {
        match self {
            CollectionType::Collection => "collection",
            CollectionType::View => "view",
            CollectionType::TimeSeries => "timeseries",
            CollectionType::System => "system",
        }
    }

    /// Check if this is a regular collection
    pub fn is_collection(&self) -> bool {
        matches!(self, CollectionType::Collection)
    }

    /// Check if this is a view
    pub fn is_view(&self) -> bool {
        matches!(self, CollectionType::View)
    }
}

impl std::fmt::Display for CollectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Information about a MongoDB index
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexInfo {
    /// Index name
    pub name: String,
    /// Key specification (field -> direction)
    pub keys: HashMap<String, IndexDirection>,
    /// Whether this is a unique index
    pub unique: bool,
    /// Whether this is a sparse index
    pub sparse: bool,
    /// TTL index expiration (seconds)
    pub expire_after_seconds: Option<u64>,
    /// Whether this is a text index
    pub is_text: bool,
    /// Whether this is a 2dsphere (geo) index
    pub is_geo: bool,
    /// Whether this is a hashed index
    pub is_hashed: bool,
}

impl IndexInfo {
    /// Create a new IndexInfo
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            keys: HashMap::new(),
            unique: false,
            sparse: false,
            expire_after_seconds: None,
            is_text: false,
            is_geo: false,
            is_hashed: false,
        }
    }

    /// Add a key to the index
    pub fn with_key(mut self, field: impl Into<String>, direction: IndexDirection) -> Self {
        self.keys.insert(field.into(), direction);
        self
    }

    /// Set unique flag
    pub fn with_unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    /// Set sparse flag
    pub fn with_sparse(mut self, sparse: bool) -> Self {
        self.sparse = sparse;
        self
    }

    /// Set TTL expiration
    pub fn with_ttl(mut self, seconds: u64) -> Self {
        self.expire_after_seconds = Some(seconds);
        self
    }

    /// Check if this is the _id index
    pub fn is_primary(&self) -> bool {
        self.name == "_id_"
    }
}

/// Index key direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexDirection {
    /// Ascending (1)
    Ascending,
    /// Descending (-1)
    Descending,
    /// Text index
    Text,
    /// 2dsphere index
    Geo2dsphere,
    /// Hashed index
    Hashed,
}

impl IndexDirection {
    /// Parse from MongoDB index key value
    pub fn from_bson_value(v: &zqlz_core::Value) -> Self {
        match v {
            zqlz_core::Value::Int32(n) => {
                if *n >= 0 {
                    IndexDirection::Ascending
                } else {
                    IndexDirection::Descending
                }
            }
            zqlz_core::Value::Int64(n) => {
                if *n >= 0 {
                    IndexDirection::Ascending
                } else {
                    IndexDirection::Descending
                }
            }
            zqlz_core::Value::String(s) => match s.as_str() {
                "text" => IndexDirection::Text,
                "2dsphere" => IndexDirection::Geo2dsphere,
                "hashed" => IndexDirection::Hashed,
                _ => IndexDirection::Ascending,
            },
            _ => IndexDirection::Ascending,
        }
    }

    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            IndexDirection::Ascending => "1",
            IndexDirection::Descending => "-1",
            IndexDirection::Text => "text",
            IndexDirection::Geo2dsphere => "2dsphere",
            IndexDirection::Hashed => "hashed",
        }
    }
}

impl std::fmt::Display for IndexDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Inferred field from document schema sampling
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InferredField {
    /// Field name (dot notation for nested)
    pub name: String,
    /// Detected BSON types
    pub types: Vec<String>,
    /// Number of documents where this field appears
    pub occurrence_count: u64,
    /// Percentage of documents with this field
    pub occurrence_percentage: f64,
    /// Whether the field appears in all sampled documents
    pub is_required: bool,
}

impl InferredField {
    /// Create a new InferredField
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            types: Vec::new(),
            occurrence_count: 0,
            occurrence_percentage: 0.0,
            is_required: false,
        }
    }

    /// Add a type
    pub fn with_type(mut self, type_name: impl Into<String>) -> Self {
        let t = type_name.into();
        if !self.types.contains(&t) {
            self.types.push(t);
        }
        self
    }

    /// Set occurrence stats
    pub fn with_occurrence(mut self, count: u64, total: u64) -> Self {
        self.occurrence_count = count;
        self.occurrence_percentage = if total > 0 {
            (count as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        // Required only if present in all documents AND there are documents
        self.is_required = total > 0 && count == total;
        self
    }
}

/// Options for listing collections
#[derive(Debug, Clone, Default)]
pub struct ListCollectionsOptions {
    /// Include system collections (those starting with "system.")
    pub include_system: bool,
    /// Include views
    pub include_views: bool,
    /// Filter by name pattern (supports wildcards)
    pub name_filter: Option<String>,
    /// Include collection statistics
    pub include_stats: bool,
}

impl ListCollectionsOptions {
    /// Create default options (excludes system, includes views, no stats)
    pub fn new() -> Self {
        Self {
            include_system: false,
            include_views: true,
            name_filter: None,
            include_stats: false,
        }
    }

    /// Include system collections
    pub fn with_system(mut self) -> Self {
        self.include_system = true;
        self
    }

    /// Exclude views
    pub fn without_views(mut self) -> Self {
        self.include_views = false;
        self
    }

    /// Filter by name pattern
    pub fn with_filter(mut self, pattern: impl Into<String>) -> Self {
        self.name_filter = Some(pattern.into());
        self
    }

    /// Include statistics for each collection
    pub fn with_stats(mut self) -> Self {
        self.include_stats = true;
        self
    }
}

/// List all databases on the MongoDB server
pub async fn list_databases(conn: &MongoDbConnection) -> Result<Vec<DatabaseInfo>> {
    let result = conn.query(r#"{ "listDatabases": 1 }"#, &[]).await?;

    let mut databases = Vec::new();

    // Extract databases from the result
    for row in &result.rows {
        if let Some(zqlz_core::Value::Json(json)) = row.get_by_name("databases")
            && let Some(arr) = json.as_array()
        {
            for db_val in arr {
                if let Some(db_obj) = db_val.as_object() {
                    let name = db_obj
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    if name.is_empty() {
                        continue;
                    }

                    let size = db_obj.get("sizeOnDisk").and_then(|v| v.as_u64());
                    let empty = db_obj
                        .get("empty")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);

                    let mut info = DatabaseInfo::new(name).with_empty(empty);
                    if let Some(s) = size {
                        info = info.with_size(s);
                    }
                    databases.push(info);
                }
            }
        }
    }

    Ok(databases)
}

/// Get information about a specific database
pub async fn get_database_info(conn: &MongoDbConnection, db_name: &str) -> Result<DatabaseInfo> {
    // We need to use the specific database
    let client = conn.client();
    let db = client.database(db_name);

    let result = db
        .run_command(bson::doc! { "dbStats": 1 })
        .await
        .map_err(|e| ZqlzError::Driver(format!("Failed to get database stats: {}", e)))?;

    let data_size = result.get_i64("dataSize").ok().map(|v| v as u64);
    let storage_size = result.get_i64("storageSize").ok().map(|v| v as u64);

    let size = data_size.or(storage_size);

    let collections = result.get_i32("collections").ok().unwrap_or(0);

    let mut info = DatabaseInfo::new(db_name);
    if let Some(s) = size {
        info = info.with_size(s);
    }
    info = info.with_empty(collections == 0);

    Ok(info)
}

/// List collections in a database
pub async fn list_collections(conn: &MongoDbConnection) -> Result<Vec<CollectionInfo>> {
    list_collections_with_options(conn, &ListCollectionsOptions::new()).await
}

/// List collections in a database with options
pub async fn list_collections_with_options(
    conn: &MongoDbConnection,
    options: &ListCollectionsOptions,
) -> Result<Vec<CollectionInfo>> {
    let result = conn.query(r#"{ "listCollections": 1 }"#, &[]).await?;

    let mut collections = Vec::new();

    // Parse the cursor result
    for row in &result.rows {
        // listCollections returns a cursor with firstBatch
        if let Some(zqlz_core::Value::Json(json)) = row.get_by_name("cursor")
            && let Some(cursor_obj) = json.as_object()
            && let Some(batch) = cursor_obj.get("firstBatch").and_then(|v| v.as_array())
        {
            for coll_val in batch {
                if let Some(info) = parse_collection_info(coll_val, options) {
                    collections.push(info);
                }
            }
        }
    }

    // Get stats if requested
    if options.include_stats {
        for coll in &mut collections {
            if let Ok(stats) = get_collection_stats(conn, &coll.name).await {
                coll.document_count = stats.document_count;
                coll.size_bytes = stats.size_bytes;
                coll.avg_doc_size = stats.avg_doc_size;
                coll.storage_size = stats.storage_size;
                coll.index_count = stats.index_count;
            }
        }
    }

    Ok(collections)
}

/// Parse collection info from listCollections result
pub(crate) fn parse_collection_info(
    value: &serde_json::Value,
    options: &ListCollectionsOptions,
) -> Option<CollectionInfo> {
    let obj = value.as_object()?;

    let name = obj.get("name")?.as_str()?.to_string();

    // Filter system collections
    if !options.include_system && name.starts_with("system.") {
        return None;
    }

    // Get collection type
    let coll_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .map(CollectionType::from_string)
        .unwrap_or(CollectionType::Collection);

    // Filter views if not included
    if !options.include_views && coll_type.is_view() {
        return None;
    }

    // Apply name filter
    if let Some(ref pattern) = options.name_filter
        && !name_matches_pattern(&name, pattern)
    {
        return None;
    }

    // Get options for capped
    let capped = obj
        .get("options")
        .and_then(|v| v.as_object())
        .and_then(|opts| opts.get("capped"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    Some(
        CollectionInfo::new(name)
            .with_type(coll_type)
            .with_capped(capped),
    )
}

/// Check if name matches a simple pattern (supports * wildcard)
pub(crate) fn name_matches_pattern(name: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if pattern.contains('*') {
        let parts: Vec<&str> = pattern.split('*').collect();
        if parts.len() == 2 {
            let prefix = parts[0];
            let suffix = parts[1];
            // Name must be long enough to have prefix + at least one char + suffix
            // This ensures there's something matched by the wildcard
            let min_length = prefix.len() + suffix.len() + 1;
            return name.len() >= min_length && name.starts_with(prefix) && name.ends_with(suffix);
        }
    }

    name == pattern
}

/// Get collection statistics
pub async fn get_collection_stats(
    conn: &MongoDbConnection,
    collection_name: &str,
) -> Result<CollectionInfo> {
    let cmd = format!(r#"{{ "collStats": "{}" }}"#, collection_name);
    let result = conn.query(&cmd, &[]).await?;

    let mut info = CollectionInfo::new(collection_name);

    // Extract stats from the first row
    if let Some(row) = result.rows.first() {
        // Document count
        if let Some(zqlz_core::Value::Int64(count)) = row.get_by_name("count") {
            info.document_count = Some(*count as u64);
        }

        // Size
        if let Some(zqlz_core::Value::Int64(size)) = row.get_by_name("size") {
            info.size_bytes = Some(*size as u64);
        }

        // Storage size
        if let Some(zqlz_core::Value::Int64(storage)) = row.get_by_name("storageSize") {
            info.storage_size = Some(*storage as u64);
        }

        // Average object size
        if let Some(zqlz_core::Value::Int64(avg)) = row.get_by_name("avgObjSize") {
            info.avg_doc_size = Some(*avg as u64);
        } else if let Some(zqlz_core::Value::Float64(avg)) = row.get_by_name("avgObjSize") {
            info.avg_doc_size = Some(*avg as u64);
        }

        // Index count
        if let Some(zqlz_core::Value::Int32(n)) = row.get_by_name("nindexes") {
            info.index_count = Some(*n as u32);
        } else if let Some(zqlz_core::Value::Int64(n)) = row.get_by_name("nindexes") {
            info.index_count = Some(*n as u32);
        }

        // Capped
        if let Some(zqlz_core::Value::Bool(capped)) = row.get_by_name("capped") {
            info.capped = *capped;
        }
    }

    Ok(info)
}

/// List indexes on a collection
pub async fn list_indexes(
    conn: &MongoDbConnection,
    collection_name: &str,
) -> Result<Vec<IndexInfo>> {
    let cmd = format!(r#"{{ "listIndexes": "{}" }}"#, collection_name);
    let result = conn.query(&cmd, &[]).await?;

    let mut indexes = Vec::new();

    for row in &result.rows {
        // listIndexes returns a cursor
        if let Some(zqlz_core::Value::Json(json)) = row.get_by_name("cursor")
            && let Some(cursor_obj) = json.as_object()
            && let Some(batch) = cursor_obj.get("firstBatch").and_then(|v| v.as_array())
        {
            for idx_val in batch {
                if let Some(info) = parse_index_info(idx_val) {
                    indexes.push(info);
                }
            }
        }
    }

    Ok(indexes)
}

/// Parse index info from listIndexes result
pub(crate) fn parse_index_info(value: &serde_json::Value) -> Option<IndexInfo> {
    let obj = value.as_object()?;

    let name = obj.get("name")?.as_str()?.to_string();
    let mut info = IndexInfo::new(name);

    // Parse key specification
    if let Some(key_obj) = obj.get("key").and_then(|v| v.as_object()) {
        let mut has_text = false;
        let mut has_geo = false;
        let mut has_hashed = false;

        for (field, dir_val) in key_obj {
            let direction = if let Some(n) = dir_val.as_i64() {
                if n >= 0 {
                    IndexDirection::Ascending
                } else {
                    IndexDirection::Descending
                }
            } else if let Some(s) = dir_val.as_str() {
                match s {
                    "text" => {
                        has_text = true;
                        IndexDirection::Text
                    }
                    "2dsphere" | "2d" => {
                        has_geo = true;
                        IndexDirection::Geo2dsphere
                    }
                    "hashed" => {
                        has_hashed = true;
                        IndexDirection::Hashed
                    }
                    _ => IndexDirection::Ascending,
                }
            } else {
                IndexDirection::Ascending
            };

            info.keys.insert(field.clone(), direction);
        }

        info.is_text = has_text;
        info.is_geo = has_geo;
        info.is_hashed = has_hashed;
    }

    // Unique
    if let Some(unique) = obj.get("unique").and_then(|v| v.as_bool()) {
        info.unique = unique;
    }

    // Sparse
    if let Some(sparse) = obj.get("sparse").and_then(|v| v.as_bool()) {
        info.sparse = sparse;
    }

    // TTL
    if let Some(ttl) = obj.get("expireAfterSeconds").and_then(|v| v.as_u64()) {
        info.expire_after_seconds = Some(ttl);
    }

    Some(info)
}

/// Get the document count for a collection
pub async fn get_document_count(conn: &MongoDbConnection, collection_name: &str) -> Result<u64> {
    let cmd = format!(r#"{{ "count": "{}" }}"#, collection_name);
    let result = conn.query(&cmd, &[]).await?;

    let count = result
        .rows
        .first()
        .and_then(|row| row.get_by_name("n"))
        .and_then(|v| match v {
            zqlz_core::Value::Int64(n) => Some(*n as u64),
            zqlz_core::Value::Int32(n) => Some(*n as u64),
            zqlz_core::Value::Float64(n) => Some(*n as u64),
            _ => None,
        })
        .unwrap_or(0);

    Ok(count)
}

/// Sample documents and infer schema
pub async fn infer_schema(
    conn: &MongoDbConnection,
    collection_name: &str,
    sample_size: u32,
) -> Result<Vec<InferredField>> {
    // Use aggregation with $sample to get random documents
    let cmd = format!(
        r#"{{ "aggregate": "{}", "pipeline": [{{ "$sample": {{ "size": {} }} }}], "cursor": {{}} }}"#,
        collection_name, sample_size
    );

    let result = conn.query(&cmd, &[]).await?;

    let mut field_stats: HashMap<String, (Vec<String>, u64)> = HashMap::new();
    let mut total_docs = 0u64;

    // Process documents from the cursor
    for row in &result.rows {
        if let Some(zqlz_core::Value::Json(json)) = row.get_by_name("cursor")
            && let Some(cursor_obj) = json.as_object()
            && let Some(batch) = cursor_obj.get("firstBatch").and_then(|v| v.as_array())
        {
            for doc in batch {
                total_docs += 1;
                collect_field_types(doc, "", &mut field_stats);
            }
        }
    }

    // Convert to InferredField
    let fields: Vec<InferredField> = field_stats
        .into_iter()
        .map(|(name, (types, count))| {
            InferredField::new(name)
                .with_occurrence(count, total_docs)
                .with_types(types)
        })
        .collect();

    // Sort by name for consistent output
    let mut sorted_fields = fields;
    sorted_fields.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(sorted_fields)
}

/// Helper extension for InferredField
impl InferredField {
    fn with_types(mut self, types: Vec<String>) -> Self {
        self.types = types;
        self
    }
}

/// Recursively collect field types from a JSON document
pub(crate) fn collect_field_types(
    value: &serde_json::Value,
    prefix: &str,
    stats: &mut HashMap<String, (Vec<String>, u64)>,
) {
    if let serde_json::Value::Object(obj) = value {
        for (key, val) in obj {
            let field_name = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{}.{}", prefix, key)
            };

            let type_name = json_type_name(val);

            let entry = stats
                .entry(field_name.clone())
                .or_insert_with(|| (Vec::new(), 0));
            entry.1 += 1;
            if !entry.0.contains(&type_name) {
                entry.0.push(type_name);
            }

            // Recurse into nested objects
            if val.is_object() {
                collect_field_types(val, &field_name, stats);
            }
        }
    }
}

/// Get the type name for a JSON value
pub(crate) fn json_type_name(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "Null".to_string(),
        serde_json::Value::Bool(_) => "Boolean".to_string(),
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                "Int64".to_string()
            } else if n.is_f64() {
                "Double".to_string()
            } else {
                "Number".to_string()
            }
        }
        serde_json::Value::String(s) => {
            // Try to detect ObjectId, Date, etc.
            if s.len() == 24 && s.chars().all(|c| c.is_ascii_hexdigit()) {
                "ObjectId".to_string()
            } else if s.starts_with("ISODate(") || s.contains('T') && s.contains('Z') {
                "Date".to_string()
            } else {
                "String".to_string()
            }
        }
        serde_json::Value::Array(_) => "Array".to_string(),
        serde_json::Value::Object(obj) => {
            // Check for special BSON types encoded as Extended JSON
            if obj.contains_key("$oid") {
                "ObjectId".to_string()
            } else if obj.contains_key("$date") {
                "Date".to_string()
            } else if obj.contains_key("$binary") {
                "BinData".to_string()
            } else if obj.contains_key("$numberDecimal") {
                "Decimal128".to_string()
            } else if obj.contains_key("$numberLong") {
                "Int64".to_string()
            } else if obj.contains_key("$regex") {
                "Regex".to_string()
            } else {
                "Object".to_string()
            }
        }
    }
}

/// Create a collection
pub async fn create_collection(conn: &MongoDbConnection, name: &str) -> Result<()> {
    let cmd = format!(r#"{{ "create": "{}" }}"#, name);
    let result = conn.execute(&cmd, &[]).await?;

    if let Some(err) = result.error {
        return Err(ZqlzError::Driver(err));
    }

    Ok(())
}

/// Drop a collection
pub async fn drop_collection(conn: &MongoDbConnection, name: &str) -> Result<()> {
    let cmd = format!(r#"{{ "drop": "{}" }}"#, name);
    let result = conn.execute(&cmd, &[]).await?;

    if let Some(err) = result.error {
        return Err(ZqlzError::Driver(err));
    }

    Ok(())
}

/// Create an index on a collection
pub async fn create_index(
    conn: &MongoDbConnection,
    collection_name: &str,
    index: &IndexInfo,
) -> Result<()> {
    // Build the key document
    let key_parts: Vec<String> = index
        .keys
        .iter()
        .map(|(field, dir)| format!(r#""{}": {}"#, field, dir.as_str()))
        .collect();
    let key_doc = format!("{{ {} }}", key_parts.join(", "));

    // Build options
    let mut options = Vec::new();
    if index.unique {
        options.push(r#""unique": true"#.to_string());
    }
    if index.sparse {
        options.push(r#""sparse": true"#.to_string());
    }
    if let Some(ttl) = index.expire_after_seconds {
        options.push(format!(r#""expireAfterSeconds": {}"#, ttl));
    }

    let options_str = if options.is_empty() {
        String::new()
    } else {
        format!(", {}", options.join(", "))
    };

    let cmd = format!(
        r#"{{ "createIndexes": "{}", "indexes": [{{ "key": {}, "name": "{}"{} }}] }}"#,
        collection_name, key_doc, index.name, options_str
    );

    let result = conn.execute(&cmd, &[]).await?;

    if let Some(err) = result.error {
        return Err(ZqlzError::Driver(err));
    }

    Ok(())
}

/// Drop an index from a collection
pub async fn drop_index(
    conn: &MongoDbConnection,
    collection_name: &str,
    index_name: &str,
) -> Result<()> {
    let cmd = format!(
        r#"{{ "dropIndexes": "{}", "index": "{}" }}"#,
        collection_name, index_name
    );

    let result = conn.execute(&cmd, &[]).await?;

    if let Some(err) = result.error {
        return Err(ZqlzError::Driver(err));
    }

    Ok(())
}
