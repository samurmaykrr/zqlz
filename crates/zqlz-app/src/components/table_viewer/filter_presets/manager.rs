//! Filter preset manager
//!
//! Provides high-level operations for managing filter presets.

use anyhow::Result;
use uuid::Uuid;

use super::storage::FilterPresetStorage;
use crate::components::table_viewer::filter_types::FilterProfile;

/// Manager for filter presets with high-level CRUD operations
pub struct FilterPresetManager {
    storage: FilterPresetStorage,
}

impl FilterPresetManager {
    /// Create a manager backed by file storage
    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<Self> {
        Ok(Self {
            storage: FilterPresetStorage::open(path)?,
        })
    }

    /// Create an in-memory manager for testing
    pub fn in_memory() -> Result<Self> {
        Ok(Self {
            storage: FilterPresetStorage::in_memory()?,
        })
    }

    /// Save a filter preset, generating a new UUID
    /// Returns the generated UUID
    pub fn save(&self, preset: &FilterProfile) -> Result<Uuid> {
        let id = Uuid::new_v4();
        self.storage.save(&id, preset)?;
        Ok(id)
    }

    /// Save a filter preset with a specific UUID
    pub fn save_with_id(&self, id: &Uuid, preset: &FilterProfile) -> Result<()> {
        self.storage.save(id, preset)
    }

    /// Load a filter preset by UUID
    pub fn get(&self, id: &Uuid) -> Result<Option<FilterProfile>> {
        self.storage.get(id)
    }

    /// Load a filter preset by name for a specific table
    pub fn get_by_name(
        &self,
        table_name: &str,
        name: &str,
    ) -> Result<Option<(Uuid, FilterProfile)>> {
        let presets = self.storage.list_for_table(table_name)?;
        Ok(presets.into_iter().find(|(_, p)| p.name == name))
    }

    /// List all presets for a table
    pub fn list(&self, table_name: &str) -> Result<Vec<(Uuid, FilterProfile)>> {
        self.storage.list_for_table(table_name)
    }

    /// Delete a filter preset by UUID
    pub fn delete(&self, id: &Uuid) -> Result<bool> {
        self.storage.delete(id)
    }

    /// Get the default preset for a table (if any)
    pub fn get_default(&self, table_name: &str) -> Result<Option<(Uuid, FilterProfile)>> {
        let presets = self.storage.list_for_table(table_name)?;
        Ok(presets.into_iter().find(|(_, p)| p.is_default))
    }

    /// Set a preset as the default for its table
    /// Clears the default flag on other presets for the same table
    pub fn set_default(&self, id: &Uuid) -> Result<()> {
        let preset = self
            .storage
            .get(id)?
            .ok_or_else(|| anyhow::anyhow!("Preset not found"))?;

        // Clear default on other presets for this table
        let presets = self.storage.list_for_table(&preset.table_name)?;
        for (other_id, other_preset) in presets {
            if other_preset.is_default && other_id != *id {
                let mut updated = other_preset;
                updated.is_default = false;
                self.storage.save(&other_id, &updated)?;
            }
        }

        // Set this preset as default
        let mut updated_preset = preset;
        updated_preset.is_default = true;
        self.storage.save(id, &updated_preset)?;
        Ok(())
    }

    /// Clear the default preset for a table
    pub fn clear_default(&self, table_name: &str) -> Result<()> {
        let presets = self.storage.list_for_table(table_name)?;
        for (id, preset) in presets {
            if preset.is_default {
                let mut updated = preset;
                updated.is_default = false;
                self.storage.save(&id, &updated)?;
            }
        }
        Ok(())
    }

    /// Rename a preset
    pub fn rename(&self, id: &Uuid, new_name: String) -> Result<()> {
        let preset = self
            .storage
            .get(id)?
            .ok_or_else(|| anyhow::anyhow!("Preset not found"))?;

        let mut updated = preset;
        updated.name = new_name;
        self.storage.save(id, &updated)?;
        Ok(())
    }

    /// Update a preset's description
    pub fn set_description(&self, id: &Uuid, description: Option<String>) -> Result<()> {
        let preset = self
            .storage
            .get(id)?
            .ok_or_else(|| anyhow::anyhow!("Preset not found"))?;

        let mut updated = preset;
        updated.description = description;
        self.storage.save(id, &updated)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::table_viewer::filter_types::{
        FilterCondition, FilterOperator, LogicalOperator, SortCriterion, SortDirection,
    };

    fn create_test_preset(name: &str, table: &str) -> FilterProfile {
        let mut preset = FilterProfile::new(name.to_string(), table.to_string());
        preset.filters.push(FilterCondition {
            id: 1,
            enabled: true,
            column: Some("status".to_string()),
            operator: FilterOperator::Equal,
            value: "active".to_string(),
            value2: None,
            custom_sql: None,
            logical_operator: LogicalOperator::And,
        });
        preset
    }

    #[test]
    fn test_save_filter_preset() {
        let manager = FilterPresetManager::in_memory().unwrap();
        let preset = create_test_preset("My Preset", "users");

        let id = manager.save(&preset).unwrap();
        let loaded = manager.get(&id).unwrap().unwrap();

        assert_eq!(loaded.name, "My Preset");
        assert_eq!(loaded.table_name, "users");
        assert_eq!(loaded.filters.len(), 1);
    }

    #[test]
    fn test_load_filter_preset() {
        let manager = FilterPresetManager::in_memory().unwrap();
        let preset = create_test_preset("Active Users", "users");
        manager.save(&preset).unwrap();

        let found = manager.get_by_name("users", "Active Users").unwrap();
        assert!(found.is_some());
        let (_, loaded) = found.unwrap();
        assert_eq!(loaded.name, "Active Users");

        let not_found = manager.get_by_name("users", "Not Existing").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_delete_filter_preset() {
        let manager = FilterPresetManager::in_memory().unwrap();
        let preset = create_test_preset("To Delete", "users");
        let id = manager.save(&preset).unwrap();

        assert!(manager.get(&id).unwrap().is_some());

        let deleted = manager.delete(&id).unwrap();
        assert!(deleted);
        assert!(manager.get(&id).unwrap().is_none());

        // Deleting again returns false
        let deleted_again = manager.delete(&id).unwrap();
        assert!(!deleted_again);
    }

    #[test]
    fn test_set_default_preset() {
        let manager = FilterPresetManager::in_memory().unwrap();

        let preset1 = create_test_preset("Preset A", "orders");
        let preset2 = create_test_preset("Preset B", "orders");

        let id1 = manager.save(&preset1).unwrap();
        let id2 = manager.save(&preset2).unwrap();

        // Set preset1 as default
        manager.set_default(&id1).unwrap();
        let default = manager.get_default("orders").unwrap();
        assert!(default.is_some());
        assert_eq!(default.unwrap().1.name, "Preset A");

        // Set preset2 as default (should clear preset1)
        manager.set_default(&id2).unwrap();
        let default = manager.get_default("orders").unwrap();
        assert!(default.is_some());
        assert_eq!(default.unwrap().1.name, "Preset B");

        // Verify preset1 is no longer default
        let loaded1 = manager.get(&id1).unwrap().unwrap();
        assert!(!loaded1.is_default);
    }

    #[test]
    fn test_rename_preset() {
        let manager = FilterPresetManager::in_memory().unwrap();
        let preset = create_test_preset("Old Name", "products");
        let id = manager.save(&preset).unwrap();

        manager.rename(&id, "New Name".to_string()).unwrap();
        let loaded = manager.get(&id).unwrap().unwrap();
        assert_eq!(loaded.name, "New Name");
    }
}
