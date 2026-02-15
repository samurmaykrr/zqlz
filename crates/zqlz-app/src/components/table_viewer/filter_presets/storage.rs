//! Filter preset storage using SQLite
//!
//! Provides persistent storage for filter presets (FilterProfile).

use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use super::super::filter_types::{
    ColumnVisibility, FilterCondition, FilterOperator, FilterProfile, LogicalOperator,
    SortCriterion, SortDirection,
};

/// Serializable filter condition for storage
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredFilterCondition {
    id: usize,
    enabled: bool,
    column: Option<String>,
    operator: String,
    value: String,
    value2: Option<String>,
    custom_sql: Option<String>,
    #[serde(default)]
    logical_operator: String,
}

impl From<&FilterCondition> for StoredFilterCondition {
    fn from(fc: &FilterCondition) -> Self {
        Self {
            id: fc.id,
            enabled: fc.enabled,
            column: fc.column.clone(),
            operator: format!("{:?}", fc.operator),
            value: fc.value.clone(),
            value2: fc.value2.clone(),
            custom_sql: fc.custom_sql.clone(),
            logical_operator: format!("{:?}", fc.logical_operator),
        }
    }
}

impl StoredFilterCondition {
    fn to_filter_condition(&self) -> FilterCondition {
        FilterCondition {
            id: self.id,
            enabled: self.enabled,
            column: self.column.clone(),
            operator: parse_filter_operator(&self.operator),
            value: self.value.clone(),
            value2: self.value2.clone(),
            custom_sql: self.custom_sql.clone(),
            logical_operator: parse_logical_operator(&self.logical_operator),
        }
    }
}

/// Serializable sort criterion for storage
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSortCriterion {
    id: usize,
    column: String,
    direction: String,
}

impl From<&SortCriterion> for StoredSortCriterion {
    fn from(sc: &SortCriterion) -> Self {
        Self {
            id: sc.id,
            column: sc.column.clone(),
            direction: format!("{:?}", sc.direction),
        }
    }
}

impl StoredSortCriterion {
    fn to_sort_criterion(&self) -> SortCriterion {
        SortCriterion {
            id: self.id,
            column: self.column.clone(),
            direction: if self.direction == "Descending" {
                SortDirection::Descending
            } else {
                SortDirection::Ascending
            },
        }
    }
}

/// Serializable column visibility for storage
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredColumnVisibility {
    name: String,
    data_type: String,
    visible: bool,
}

impl From<&ColumnVisibility> for StoredColumnVisibility {
    fn from(cv: &ColumnVisibility) -> Self {
        Self {
            name: cv.name.clone(),
            data_type: cv.data_type.clone(),
            visible: cv.visible,
        }
    }
}

impl StoredColumnVisibility {
    fn to_column_visibility(&self) -> ColumnVisibility {
        ColumnVisibility {
            name: self.name.clone(),
            data_type: self.data_type.clone(),
            visible: self.visible,
        }
    }
}

fn parse_filter_operator(s: &str) -> FilterOperator {
    match s {
        "Equal" => FilterOperator::Equal,
        "NotEqual" => FilterOperator::NotEqual,
        "LessThan" => FilterOperator::LessThan,
        "LessThanOrEqual" => FilterOperator::LessThanOrEqual,
        "GreaterThan" => FilterOperator::GreaterThan,
        "GreaterThanOrEqual" => FilterOperator::GreaterThanOrEqual,
        "Contains" => FilterOperator::Contains,
        "DoesNotContain" => FilterOperator::DoesNotContain,
        "BeginsWith" => FilterOperator::BeginsWith,
        "DoesNotBeginWith" => FilterOperator::DoesNotBeginWith,
        "EndsWith" => FilterOperator::EndsWith,
        "DoesNotEndWith" => FilterOperator::DoesNotEndWith,
        "IsNull" => FilterOperator::IsNull,
        "IsNotNull" => FilterOperator::IsNotNull,
        "IsEmpty" => FilterOperator::IsEmpty,
        "IsNotEmpty" => FilterOperator::IsNotEmpty,
        "IsBetween" => FilterOperator::IsBetween,
        "IsNotBetween" => FilterOperator::IsNotBetween,
        "IsInList" => FilterOperator::IsInList,
        "IsNotInList" => FilterOperator::IsNotInList,
        "Custom" => FilterOperator::Custom,
        _ => FilterOperator::Equal,
    }
}

fn parse_logical_operator(s: &str) -> LogicalOperator {
    match s {
        "Or" => LogicalOperator::Or,
        _ => LogicalOperator::And,
    }
}

/// Handle for database connections - either owned or shared
enum ConnectionHandle {
    Owned(Connection),
    Shared(Arc<Mutex<Connection>>),
}

impl ConnectionHandle {
    fn with_conn<T, F: FnOnce(&Connection) -> Result<T>>(&self, f: F) -> Result<T> {
        match self {
            ConnectionHandle::Owned(conn) => f(conn),
            ConnectionHandle::Shared(arc) => {
                let guard = arc
                    .lock()
                    .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
                f(&guard)
            }
        }
    }
}

/// Storage for filter presets using SQLite
pub struct FilterPresetStorage {
    db_path: PathBuf,
    /// Holds the connection for in-memory databases (where each open creates a new db)
    memory_conn: Option<Arc<Mutex<Connection>>>,
}

impl FilterPresetStorage {
    /// Open or create storage at the given path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let db_path = path.into();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let storage = Self {
            db_path,
            memory_conn: None,
        };
        storage.initialize_schema()?;
        Ok(storage)
    }

    /// Create an in-memory storage for testing
    pub fn in_memory() -> Result<Self> {
        let conn =
            Connection::open_in_memory().with_context(|| "Failed to create in-memory database")?;
        let storage = Self {
            db_path: PathBuf::from(":memory:"),
            memory_conn: Some(Arc::new(Mutex::new(conn))),
        };
        storage.initialize_schema()?;
        Ok(storage)
    }

    fn connect(&self) -> Result<ConnectionHandle> {
        if let Some(ref conn) = self.memory_conn {
            Ok(ConnectionHandle::Shared(conn.clone()))
        } else {
            let conn = Connection::open(&self.db_path)
                .with_context(|| format!("Failed to open database at {:?}", self.db_path))?;
            Ok(ConnectionHandle::Owned(conn))
        }
    }

    fn initialize_schema(&self) -> Result<()> {
        let handle = self.connect()?;
        handle.with_conn(|conn| {
            conn.execute(
                "CREATE TABLE IF NOT EXISTS filter_presets (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    table_name TEXT NOT NULL,
                    connection_id TEXT,
                    is_default INTEGER NOT NULL DEFAULT 0,
                    filters_json TEXT NOT NULL DEFAULT '[]',
                    sorts_json TEXT NOT NULL DEFAULT '[]',
                    visibility_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )",
                [],
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_filter_presets_table 
                 ON filter_presets(table_name)",
                [],
            )?;
            Ok(())
        })
    }

    /// Save a filter preset
    pub fn save(&self, id: &Uuid, preset: &FilterProfile) -> Result<()> {
        let handle = self.connect()?;
        let now = Utc::now().to_rfc3339();

        let filters: Vec<StoredFilterCondition> = preset
            .filters
            .iter()
            .map(StoredFilterCondition::from)
            .collect();
        let sorts: Vec<StoredSortCriterion> =
            preset.sorts.iter().map(StoredSortCriterion::from).collect();
        let visibility: Vec<StoredColumnVisibility> = preset
            .column_visibility
            .iter()
            .map(StoredColumnVisibility::from)
            .collect();

        let filters_json = serde_json::to_string(&filters)?;
        let sorts_json = serde_json::to_string(&sorts)?;
        let visibility_json = serde_json::to_string(&visibility)?;

        handle.with_conn(|conn| {
            conn.execute(
                "INSERT OR REPLACE INTO filter_presets 
                 (id, name, description, table_name, connection_id, is_default, filters_json, sorts_json, visibility_json, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    id.to_string(),
                    preset.name,
                    preset.description,
                    preset.table_name,
                    preset.connection_id,
                    if preset.is_default { 1 } else { 0 },
                    filters_json,
                    sorts_json,
                    visibility_json,
                    now,
                    now,
                ],
            )?;
            Ok(())
        })
    }

    /// Load a filter preset by ID
    pub fn get(&self, id: &Uuid) -> Result<Option<FilterProfile>> {
        let handle = self.connect()?;
        let id_str = id.to_string();
        handle.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT name, description, table_name, connection_id, is_default, filters_json, sorts_json, visibility_json
                 FROM filter_presets WHERE id = ?1",
            )?;

            let result = stmt.query_row(params![id_str], |row| {
                let name: String = row.get(0)?;
                let description: Option<String> = row.get(1)?;
                let table_name: String = row.get(2)?;
                let connection_id: Option<String> = row.get(3)?;
                let is_default: i32 = row.get(4)?;
                let filters_json: String = row.get(5)?;
                let sorts_json: String = row.get(6)?;
                let visibility_json: String = row.get(7)?;

                Ok((
                    name,
                    description,
                    table_name,
                    connection_id,
                    is_default,
                    filters_json,
                    sorts_json,
                    visibility_json,
                ))
            });

            match result {
                Ok((
                    name,
                    description,
                    table_name,
                    connection_id,
                    is_default,
                    filters_json,
                    sorts_json,
                    visibility_json,
                )) => {
                    let stored_filters: Vec<StoredFilterCondition> =
                        serde_json::from_str(&filters_json).unwrap_or_default();
                    let stored_sorts: Vec<StoredSortCriterion> =
                        serde_json::from_str(&sorts_json).unwrap_or_default();
                    let stored_visibility: Vec<StoredColumnVisibility> =
                        serde_json::from_str(&visibility_json).unwrap_or_default();

                    Ok(Some(FilterProfile {
                        name,
                        description,
                        filters: stored_filters
                            .iter()
                            .map(|f| f.to_filter_condition())
                            .collect(),
                        sorts: stored_sorts.iter().map(|s| s.to_sort_criterion()).collect(),
                        column_visibility: stored_visibility
                            .iter()
                            .map(|v| v.to_column_visibility())
                            .collect(),
                        is_default: is_default != 0,
                        table_name,
                        connection_id,
                    }))
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
    }

    /// List all presets for a table
    pub fn list_for_table(&self, table_name: &str) -> Result<Vec<(Uuid, FilterProfile)>> {
        let handle = self.connect()?;
        let table_name_owned = table_name.to_string();
        handle.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, name, description, table_name, connection_id, is_default, filters_json, sorts_json, visibility_json
                 FROM filter_presets WHERE table_name = ?1 ORDER BY name ASC",
            )?;

            let rows = stmt.query_map(params![table_name_owned], |row| {
                let id_str: String = row.get(0)?;
                let name: String = row.get(1)?;
                let description: Option<String> = row.get(2)?;
                let table_name: String = row.get(3)?;
                let connection_id: Option<String> = row.get(4)?;
                let is_default: i32 = row.get(5)?;
                let filters_json: String = row.get(6)?;
                let sorts_json: String = row.get(7)?;
                let visibility_json: String = row.get(8)?;

                Ok((
                    id_str,
                    name,
                    description,
                    table_name,
                    connection_id,
                    is_default,
                    filters_json,
                    sorts_json,
                    visibility_json,
                ))
            })?;

            let mut result = Vec::new();
            for row in rows {
                let (
                    id_str,
                    name,
                    description,
                    table_name,
                    connection_id,
                    is_default,
                    filters_json,
                    sorts_json,
                    visibility_json,
                ) = row?;
                let id = Uuid::parse_str(&id_str)?;

                let stored_filters: Vec<StoredFilterCondition> =
                    serde_json::from_str(&filters_json).unwrap_or_default();
                let stored_sorts: Vec<StoredSortCriterion> =
                    serde_json::from_str(&sorts_json).unwrap_or_default();
                let stored_visibility: Vec<StoredColumnVisibility> =
                    serde_json::from_str(&visibility_json).unwrap_or_default();

                result.push((
                    id,
                    FilterProfile {
                        name,
                        description,
                        filters: stored_filters
                            .iter()
                            .map(|f| f.to_filter_condition())
                            .collect(),
                        sorts: stored_sorts.iter().map(|s| s.to_sort_criterion()).collect(),
                        column_visibility: stored_visibility
                            .iter()
                            .map(|v| v.to_column_visibility())
                            .collect(),
                        is_default: is_default != 0,
                        table_name,
                        connection_id,
                    },
                ));
            }
            Ok(result)
        })
    }

    /// Delete a filter preset by ID
    pub fn delete(&self, id: &Uuid) -> Result<bool> {
        let handle = self.connect()?;
        let id_str = id.to_string();
        handle.with_conn(|conn| {
            let rows = conn.execute("DELETE FROM filter_presets WHERE id = ?1", params![id_str])?;
            Ok(rows > 0)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_preset() -> FilterProfile {
        let mut preset = FilterProfile::new("Test Preset".to_string(), "users".to_string());
        preset.description = Some("A test preset".to_string());
        preset.filters.push(FilterCondition {
            id: 1,
            enabled: true,
            column: Some("name".to_string()),
            operator: FilterOperator::Contains,
            value: "John".to_string(),
            value2: None,
            custom_sql: None,
            logical_operator: LogicalOperator::And,
        });
        preset.sorts.push(SortCriterion {
            id: 1,
            column: "created_at".to_string(),
            direction: SortDirection::Descending,
        });
        preset.column_visibility.push(ColumnVisibility {
            name: "id".to_string(),
            data_type: "integer".to_string(),
            visible: false,
        });
        preset
    }

    #[test]
    fn test_save_and_get_preset() {
        let storage = FilterPresetStorage::in_memory().unwrap();
        let preset = create_test_preset();
        let id = Uuid::new_v4();

        storage.save(&id, &preset).unwrap();
        let loaded = storage.get(&id).unwrap().unwrap();

        assert_eq!(loaded.name, "Test Preset");
        assert_eq!(loaded.table_name, "users");
        assert_eq!(loaded.description, Some("A test preset".to_string()));
        assert_eq!(loaded.filters.len(), 1);
        assert_eq!(loaded.filters[0].column, Some("name".to_string()));
        assert_eq!(loaded.sorts.len(), 1);
        assert_eq!(loaded.sorts[0].column, "created_at");
        assert_eq!(loaded.column_visibility.len(), 1);
        assert!(!loaded.column_visibility[0].visible);
    }

    #[test]
    fn test_list_for_table() {
        let storage = FilterPresetStorage::in_memory().unwrap();

        let mut preset1 = FilterProfile::new("Preset A".to_string(), "users".to_string());
        let mut preset2 = FilterProfile::new("Preset B".to_string(), "users".to_string());
        let preset3 = FilterProfile::new("Preset C".to_string(), "orders".to_string());

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        storage.save(&id1, &preset1).unwrap();
        storage.save(&id2, &preset2).unwrap();
        storage.save(&id3, &preset3).unwrap();

        let user_presets = storage.list_for_table("users").unwrap();
        assert_eq!(user_presets.len(), 2);
        assert_eq!(user_presets[0].1.name, "Preset A");
        assert_eq!(user_presets[1].1.name, "Preset B");

        let order_presets = storage.list_for_table("orders").unwrap();
        assert_eq!(order_presets.len(), 1);
        assert_eq!(order_presets[0].1.name, "Preset C");
    }

    #[test]
    fn test_delete_preset() {
        let storage = FilterPresetStorage::in_memory().unwrap();
        let preset = create_test_preset();
        let id = Uuid::new_v4();

        storage.save(&id, &preset).unwrap();
        assert!(storage.get(&id).unwrap().is_some());

        let deleted = storage.delete(&id).unwrap();
        assert!(deleted);
        assert!(storage.get(&id).unwrap().is_none());

        // Deleting non-existent returns false
        let deleted_again = storage.delete(&id).unwrap();
        assert!(!deleted_again);
    }

    #[test]
    fn test_filter_preset_persistence() {
        let storage = FilterPresetStorage::in_memory().unwrap();
        let preset = create_test_preset();
        let id = Uuid::new_v4();

        storage.save(&id, &preset).unwrap();
        let loaded = storage.get(&id).unwrap().unwrap();

        // Verify filter operator was persisted correctly
        assert!(matches!(
            loaded.filters[0].operator,
            FilterOperator::Contains
        ));

        // Verify sort direction was persisted correctly
        assert!(matches!(
            loaded.sorts[0].direction,
            SortDirection::Descending
        ));
    }
}
