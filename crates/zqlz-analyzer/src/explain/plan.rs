//! Query Plan Model - Data structures for representing query execution plans
//!
//! This module defines the unified query plan model that can represent
//! EXPLAIN output from PostgreSQL, MySQL, and SQLite.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a complete query execution plan
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryPlan {
    /// Root node of the plan tree
    pub root: PlanNode,
    /// Total estimated cost of the query
    pub total_cost: Option<f64>,
    /// Total estimated rows to be processed
    pub total_rows: Option<u64>,
    /// Planning time in milliseconds (if available)
    pub planning_time_ms: Option<f64>,
    /// Execution time in milliseconds (if available, from EXPLAIN ANALYZE)
    pub execution_time_ms: Option<f64>,
}

impl QueryPlan {
    /// Creates a new query plan with the given root node
    pub fn new(root: PlanNode) -> Self {
        let total_cost = root.cost.map(|c| c.total);
        let total_rows = root.rows;
        Self {
            root,
            total_cost,
            total_rows,
            planning_time_ms: None,
            execution_time_ms: None,
        }
    }

    /// Sets the planning time
    pub fn with_planning_time(mut self, ms: f64) -> Self {
        self.planning_time_ms = Some(ms);
        self
    }

    /// Sets the execution time
    pub fn with_execution_time(mut self, ms: f64) -> Self {
        self.execution_time_ms = Some(ms);
        self
    }

    /// Returns an iterator over all nodes in the plan (depth-first)
    pub fn iter_nodes(&self) -> PlanNodeIterator<'_> {
        PlanNodeIterator::new(&self.root)
    }

    /// Finds all nodes matching a specific node type
    pub fn find_nodes_by_type(&self, node_type: NodeType) -> Vec<&PlanNode> {
        self.iter_nodes()
            .filter(|n| n.node_type == node_type)
            .collect()
    }

    /// Returns true if the plan contains any sequential scans
    pub fn has_sequential_scans(&self) -> bool {
        self.iter_nodes().any(|n| n.node_type == NodeType::SeqScan)
    }

    /// Returns true if the plan contains any hash operations
    pub fn has_hash_operations(&self) -> bool {
        self.iter_nodes().any(|n| {
            matches!(
                n.node_type,
                NodeType::HashJoin | NodeType::Hash | NodeType::HashAggregate
            )
        })
    }
}

/// Represents a single node in the query plan tree
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanNode {
    /// Type of operation this node performs
    pub node_type: NodeType,
    /// Human-readable description of the operation
    pub description: Option<String>,
    /// Relation/table name (if applicable)
    pub relation: Option<String>,
    /// Schema name (if applicable)
    pub schema: Option<String>,
    /// Alias used in the query (if applicable)
    pub alias: Option<String>,
    /// Cost information
    pub cost: Option<NodeCost>,
    /// Estimated number of rows
    pub rows: Option<u64>,
    /// Estimated width of each row in bytes
    pub width: Option<u32>,
    /// Actual rows returned (from EXPLAIN ANALYZE)
    pub actual_rows: Option<u64>,
    /// Actual time in milliseconds (from EXPLAIN ANALYZE)
    pub actual_time_ms: Option<ActualTime>,
    /// Number of loops/iterations
    pub loops: Option<u64>,
    /// Filter condition applied
    pub filter: Option<String>,
    /// Rows removed by filter (from EXPLAIN ANALYZE)
    pub rows_removed_by_filter: Option<u64>,
    /// Index name used (for index scans)
    pub index_name: Option<String>,
    /// Index condition (for index scans)
    pub index_cond: Option<String>,
    /// Join type (for joins)
    pub join_type: Option<JoinType>,
    /// Join condition
    pub join_cond: Option<String>,
    /// Sort keys (for sort operations)
    pub sort_keys: Vec<String>,
    /// Sort method used (from EXPLAIN ANALYZE)
    pub sort_method: Option<String>,
    /// Memory used in KB (from EXPLAIN ANALYZE)
    pub memory_used_kb: Option<u64>,
    /// Hash buckets (for hash operations)
    pub hash_buckets: Option<u64>,
    /// Hash batches (for hash operations)
    pub hash_batches: Option<u64>,
    /// Group keys (for aggregations)
    pub group_keys: Vec<String>,
    /// Output columns
    pub output: Vec<String>,
    /// Child nodes
    pub children: Vec<PlanNode>,
    /// Additional properties not captured by specific fields
    pub extra: HashMap<String, serde_json::Value>,
}

impl PlanNode {
    /// Creates a new plan node with the given type
    pub fn new(node_type: NodeType) -> Self {
        Self {
            node_type,
            description: None,
            relation: None,
            schema: None,
            alias: None,
            cost: None,
            rows: None,
            width: None,
            actual_rows: None,
            actual_time_ms: None,
            loops: None,
            filter: None,
            rows_removed_by_filter: None,
            index_name: None,
            index_cond: None,
            join_type: None,
            join_cond: None,
            sort_keys: Vec::new(),
            sort_method: None,
            memory_used_kb: None,
            hash_buckets: None,
            hash_batches: None,
            group_keys: Vec::new(),
            output: Vec::new(),
            children: Vec::new(),
            extra: HashMap::new(),
        }
    }

    /// Sets the relation/table name
    pub fn with_relation(mut self, relation: impl Into<String>) -> Self {
        self.relation = Some(relation.into());
        self
    }

    /// Sets the cost information
    pub fn with_cost(mut self, startup: f64, total: f64) -> Self {
        self.cost = Some(NodeCost { startup, total });
        self
    }

    /// Sets the estimated rows
    pub fn with_rows(mut self, rows: u64) -> Self {
        self.rows = Some(rows);
        self
    }

    /// Sets the row width
    pub fn with_width(mut self, width: u32) -> Self {
        self.width = Some(width);
        self
    }

    /// Adds a child node
    pub fn with_child(mut self, child: PlanNode) -> Self {
        self.children.push(child);
        self
    }

    /// Sets the index name
    pub fn with_index(mut self, index_name: impl Into<String>) -> Self {
        self.index_name = Some(index_name.into());
        self
    }

    /// Sets the filter condition
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Returns the total number of nodes in this subtree (including self)
    pub fn node_count(&self) -> usize {
        1 + self.children.iter().map(|c| c.node_count()).sum::<usize>()
    }

    /// Returns the maximum depth of this subtree
    pub fn depth(&self) -> usize {
        if self.children.is_empty() {
            1
        } else {
            1 + self.children.iter().map(|c| c.depth()).max().unwrap_or(0)
        }
    }

    /// Returns true if this is a leaf node (no children)
    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    /// Returns true if this node represents a scan operation
    pub fn is_scan(&self) -> bool {
        matches!(
            self.node_type,
            NodeType::SeqScan
                | NodeType::IndexScan
                | NodeType::IndexOnlyScan
                | NodeType::BitmapIndexScan
                | NodeType::BitmapHeapScan
                | NodeType::TidScan
                | NodeType::ForeignScan
                | NodeType::CteScan
        )
    }

    /// Returns true if this node represents a join operation
    pub fn is_join(&self) -> bool {
        matches!(
            self.node_type,
            NodeType::NestedLoop | NodeType::HashJoin | NodeType::MergeJoin
        )
    }

    /// Returns the effective cost (total - startup)
    pub fn effective_cost(&self) -> Option<f64> {
        self.cost.map(|c| c.total - c.startup)
    }
}

/// Cost information for a plan node
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct NodeCost {
    /// Startup cost (time to return first row)
    pub startup: f64,
    /// Total cost (time to return all rows)
    pub total: f64,
}

impl NodeCost {
    /// Creates a new cost with startup and total values
    pub fn new(startup: f64, total: f64) -> Self {
        Self { startup, total }
    }

    /// Returns the execution cost (total - startup)
    pub fn execution(&self) -> f64 {
        self.total - self.startup
    }
}

/// Actual timing information from EXPLAIN ANALYZE
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct ActualTime {
    /// Time to return first row
    pub startup: f64,
    /// Total execution time
    pub total: f64,
}

impl ActualTime {
    /// Creates new actual timing
    pub fn new(startup: f64, total: f64) -> Self {
        Self { startup, total }
    }
}

/// Type of join operation
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross,
}

impl JoinType {
    /// Parses a join type from a string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "inner" => Some(Self::Inner),
            "left" | "left outer" => Some(Self::Left),
            "right" | "right outer" => Some(Self::Right),
            "full" | "full outer" => Some(Self::Full),
            "semi" => Some(Self::Semi),
            "anti" => Some(Self::Anti),
            "cross" => Some(Self::Cross),
            _ => None,
        }
    }
}

/// Type of operation performed by a plan node
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    // Scan operations
    SeqScan,
    IndexScan,
    IndexOnlyScan,
    BitmapIndexScan,
    BitmapHeapScan,
    TidScan,
    SubqueryScan,
    FunctionScan,
    ValuesScan,
    CteScan,
    WorkTableScan,
    ForeignScan,
    CustomScan,

    // Join operations
    NestedLoop,
    HashJoin,
    MergeJoin,

    // Aggregation operations
    Aggregate,
    GroupAggregate,
    HashAggregate,
    WindowAgg,

    // Sort operations
    Sort,
    IncrementalSort,

    // Set operations
    SetOp,
    Append,
    MergeAppend,
    RecursiveUnion,

    // Limit/Offset
    Limit,

    // Materialize
    Materialize,
    Memoize,

    // Hash
    Hash,

    // Unique
    Unique,

    // Bitmap operations
    BitmapAnd,
    BitmapOr,

    // Subplan
    SubPlan,

    // Modification operations
    ModifyTable,
    Insert,
    Update,
    Delete,

    // Result
    Result,

    // Gather (parallel query)
    Gather,
    GatherMerge,

    // Lock
    LockRows,

    // Project
    ProjectSet,

    // CTE
    CTE,

    // Unknown/Other
    Unknown,
}

impl NodeType {
    /// Parses a node type from PostgreSQL EXPLAIN output
    pub fn from_postgres_str(s: &str) -> Self {
        match s {
            "Seq Scan" => Self::SeqScan,
            "Index Scan" => Self::IndexScan,
            "Index Only Scan" => Self::IndexOnlyScan,
            "Bitmap Index Scan" => Self::BitmapIndexScan,
            "Bitmap Heap Scan" => Self::BitmapHeapScan,
            "Tid Scan" | "TID Scan" => Self::TidScan,
            "Subquery Scan" => Self::SubqueryScan,
            "Function Scan" => Self::FunctionScan,
            "Values Scan" => Self::ValuesScan,
            "CTE Scan" => Self::CteScan,
            "WorkTable Scan" => Self::WorkTableScan,
            "Foreign Scan" => Self::ForeignScan,
            "Custom Scan" => Self::CustomScan,
            "Nested Loop" => Self::NestedLoop,
            "Hash Join" => Self::HashJoin,
            "Merge Join" => Self::MergeJoin,
            "Aggregate" => Self::Aggregate,
            "GroupAggregate" | "Group Aggregate" => Self::GroupAggregate,
            "HashAggregate" | "Hash Aggregate" => Self::HashAggregate,
            "WindowAgg" | "Window Aggregate" => Self::WindowAgg,
            "Sort" => Self::Sort,
            "Incremental Sort" => Self::IncrementalSort,
            "SetOp" | "SetOperation" => Self::SetOp,
            "Append" => Self::Append,
            "Merge Append" | "MergeAppend" => Self::MergeAppend,
            "Recursive Union" => Self::RecursiveUnion,
            "Limit" => Self::Limit,
            "Materialize" => Self::Materialize,
            "Memoize" => Self::Memoize,
            "Hash" => Self::Hash,
            "Unique" => Self::Unique,
            "BitmapAnd" | "Bitmap And" => Self::BitmapAnd,
            "BitmapOr" | "Bitmap Or" => Self::BitmapOr,
            "SubPlan" => Self::SubPlan,
            "ModifyTable" | "Modify Table" => Self::ModifyTable,
            "Insert" => Self::Insert,
            "Update" => Self::Update,
            "Delete" => Self::Delete,
            "Result" => Self::Result,
            "Gather" => Self::Gather,
            "Gather Merge" => Self::GatherMerge,
            "LockRows" | "Lock Rows" => Self::LockRows,
            "ProjectSet" | "Project Set" => Self::ProjectSet,
            "CTE" => Self::CTE,
            _ => Self::Unknown,
        }
    }

    /// Returns a human-readable description of this node type
    pub fn description(&self) -> &'static str {
        match self {
            Self::SeqScan => "Sequential scan (full table scan)",
            Self::IndexScan => "Index scan (uses index to find rows, then reads table)",
            Self::IndexOnlyScan => "Index-only scan (reads data directly from index)",
            Self::BitmapIndexScan => "Bitmap index scan (builds bitmap of matching rows)",
            Self::BitmapHeapScan => "Bitmap heap scan (reads table using bitmap)",
            Self::TidScan => "TID scan (direct row access by tuple ID)",
            Self::SubqueryScan => "Subquery scan (scans subquery results)",
            Self::FunctionScan => "Function scan (scans function return values)",
            Self::ValuesScan => "Values scan (scans VALUES clause)",
            Self::CteScan => "CTE scan (scans common table expression)",
            Self::WorkTableScan => "Work table scan (recursive CTE work table)",
            Self::ForeignScan => "Foreign scan (scans foreign table)",
            Self::CustomScan => "Custom scan (extension-provided scan)",
            Self::NestedLoop => "Nested loop join",
            Self::HashJoin => "Hash join",
            Self::MergeJoin => "Merge join (sorted inputs)",
            Self::Aggregate => "Aggregate",
            Self::GroupAggregate => "Group aggregate (sorted groups)",
            Self::HashAggregate => "Hash aggregate (hash-based grouping)",
            Self::WindowAgg => "Window function aggregate",
            Self::Sort => "Sort",
            Self::IncrementalSort => "Incremental sort (partially presorted)",
            Self::SetOp => "Set operation (UNION/INTERSECT/EXCEPT)",
            Self::Append => "Append (combines multiple inputs)",
            Self::MergeAppend => "Merge append (combines sorted inputs)",
            Self::RecursiveUnion => "Recursive union (recursive CTE)",
            Self::Limit => "Limit (restricts output rows)",
            Self::Materialize => "Materialize (stores results in memory)",
            Self::Memoize => "Memoize (caches repeated lookups)",
            Self::Hash => "Hash (builds hash table for join)",
            Self::Unique => "Unique (removes duplicates)",
            Self::BitmapAnd => "Bitmap AND (combines bitmaps)",
            Self::BitmapOr => "Bitmap OR (combines bitmaps)",
            Self::SubPlan => "SubPlan (subquery execution)",
            Self::ModifyTable => "Modify table (INSERT/UPDATE/DELETE)",
            Self::Insert => "Insert rows",
            Self::Update => "Update rows",
            Self::Delete => "Delete rows",
            Self::Result => "Result (computes expression)",
            Self::Gather => "Gather (collects parallel worker results)",
            Self::GatherMerge => "Gather merge (merges sorted parallel results)",
            Self::LockRows => "Lock rows (FOR UPDATE/SHARE)",
            Self::ProjectSet => "Project set (generates rows from set-returning functions)",
            Self::CTE => "Common Table Expression",
            Self::Unknown => "Unknown operation",
        }
    }

    /// Returns true if this operation typically indicates a performance concern
    pub fn is_potentially_slow(&self) -> bool {
        matches!(self, Self::SeqScan | Self::NestedLoop | Self::Sort)
    }
}

/// Iterator for traversing plan nodes depth-first
pub struct PlanNodeIterator<'a> {
    stack: Vec<&'a PlanNode>,
}

impl<'a> PlanNodeIterator<'a> {
    fn new(root: &'a PlanNode) -> Self {
        Self { stack: vec![root] }
    }
}

impl<'a> Iterator for PlanNodeIterator<'a> {
    type Item = &'a PlanNode;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        // Push children in reverse order so we visit them in order
        for child in node.children.iter().rev() {
            self.stack.push(child);
        }
        Some(node)
    }
}

#[cfg(test)]
mod tests;
