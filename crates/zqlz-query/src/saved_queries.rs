use thiserror::Error;
use uuid::Uuid;
use zqlz_core::validate_query_name;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SavedQueryRecord {
    pub id: Uuid,
    pub name: String,
    pub connection_id: Uuid,
    pub sql: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder: Option<String>,
}

pub trait SavedQueryStore {
    fn query_name_exists(&self, connection_id: Uuid, name: &str) -> anyhow::Result<bool>;
    fn save_query(&self, query: &SavedQueryRecord) -> anyhow::Result<()>;
    fn load_query(&self, query_id: Uuid) -> anyhow::Result<Option<SavedQueryRecord>>;
    fn update_query_sql(&self, query_id: Uuid, sql: &str) -> anyhow::Result<()>;
    fn move_query_to_folder(&self, query_id: Uuid, folder: Option<&str>) -> anyhow::Result<()>;
    fn rename_query(&self, query_id: Uuid, new_name: &str) -> anyhow::Result<()>;
    fn delete_query(&self, query_id: Uuid) -> anyhow::Result<()>;
    fn load_queries_for_connection(
        &self,
        connection_id: Uuid,
    ) -> anyhow::Result<Vec<SavedQueryRecord>>;
}

#[derive(Debug, Error)]
pub enum SavedQueryWorkflowError {
    #[error("{0}")]
    Validation(String),
    #[error("A query with this name already exists")]
    NameAlreadyExists,
    #[error("Query not found")]
    NotFound,
    #[error("{0}")]
    Storage(String),
}

/// User-intent context for formatting saved-query workflow failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SavedQueryOperation {
    Create,
    Update,
    Load,
    Rename,
    Delete,
    LoadForConnection,
}

impl SavedQueryOperation {
    fn action_description(self) -> &'static str {
        match self {
            Self::Create => "save query",
            Self::Update => "save query",
            Self::Load => "load query",
            Self::Rename => "rename query",
            Self::Delete => "delete query",
            Self::LoadForConnection => "load queries",
        }
    }
}

impl SavedQueryWorkflowError {
    /// Convert workflow errors into user-facing messages.
    ///
    /// Keeping this mapping in `zqlz-query` ensures all app surfaces that
    /// consume saved-query workflows communicate failures consistently.
    pub fn user_message(&self, operation: SavedQueryOperation) -> String {
        match self {
            SavedQueryWorkflowError::Validation(error) => error.clone(),
            SavedQueryWorkflowError::NameAlreadyExists => {
                "A query with this name already exists".to_string()
            }
            SavedQueryWorkflowError::NotFound => "Query not found".to_string(),
            SavedQueryWorkflowError::Storage(error) => {
                format!("Failed to {}: {}", operation.action_description(), error)
            }
        }
    }
}

/// Typed request envelope for saved-query workflows.
///
/// This keeps operation dispatch rules in `zqlz-query` so app-layer handlers can
/// delegate persistence concerns through one typed entry point.
pub enum SavedQueryWorkflowRequest {
    Create {
        name: String,
        connection_id: Uuid,
        sql: String,
        folder: Option<String>,
    },
    UpdateSql {
        query_id: Uuid,
        sql: String,
    },
    MoveToFolder {
        query_id: Uuid,
        folder: Option<String>,
    },
    Load {
        query_id: Uuid,
    },
    Rename {
        query_id: Uuid,
        connection_id: Uuid,
        new_name: String,
    },
    Delete {
        query_id: Uuid,
    },
    LoadForConnection {
        connection_id: Uuid,
    },
    Import {
        connection_id: Uuid,
        content: String,
    },
    Export {
        connection_id: Uuid,
    },
}

/// Typed outcome envelope for saved-query workflows.
pub enum SavedQueryWorkflowOutcome {
    Created(SavedQueryRecord),
    Updated,
    Loaded(SavedQueryRecord),
    Renamed,
    Deleted,
    LoadedForConnection(Vec<SavedQueryRecord>),
    Imported(Vec<SavedQueryRecord>),
    Exported(String),
}

impl SavedQueryWorkflowOutcome {
    /// Return a `Created` record or a typed workflow mismatch error.
    pub fn into_created(self) -> Result<SavedQueryRecord, SavedQueryWorkflowError> {
        match self {
            SavedQueryWorkflowOutcome::Created(record) => Ok(record),
            _ => Err(SavedQueryWorkflowError::Validation(
                "Internal workflow mismatch: expected created outcome".to_string(),
            )),
        }
    }

    /// Return success for update operations or a typed workflow mismatch error.
    pub fn into_updated(self) -> Result<(), SavedQueryWorkflowError> {
        match self {
            SavedQueryWorkflowOutcome::Updated => Ok(()),
            _ => Err(SavedQueryWorkflowError::Validation(
                "Internal workflow mismatch: expected updated outcome".to_string(),
            )),
        }
    }

    /// Return a `Loaded` record or a typed workflow mismatch error.
    pub fn into_loaded(self) -> Result<SavedQueryRecord, SavedQueryWorkflowError> {
        match self {
            SavedQueryWorkflowOutcome::Loaded(record) => Ok(record),
            _ => Err(SavedQueryWorkflowError::Validation(
                "Internal workflow mismatch: expected loaded outcome".to_string(),
            )),
        }
    }

    /// Return success for rename operations or a typed workflow mismatch error.
    pub fn into_renamed(self) -> Result<(), SavedQueryWorkflowError> {
        match self {
            SavedQueryWorkflowOutcome::Renamed => Ok(()),
            _ => Err(SavedQueryWorkflowError::Validation(
                "Internal workflow mismatch: expected renamed outcome".to_string(),
            )),
        }
    }

    /// Return success for delete operations or a typed workflow mismatch error.
    pub fn into_deleted(self) -> Result<(), SavedQueryWorkflowError> {
        match self {
            SavedQueryWorkflowOutcome::Deleted => Ok(()),
            _ => Err(SavedQueryWorkflowError::Validation(
                "Internal workflow mismatch: expected deleted outcome".to_string(),
            )),
        }
    }

    /// Return loaded-for-connection records or a typed workflow mismatch error.
    pub fn into_loaded_for_connection(
        self,
    ) -> Result<Vec<SavedQueryRecord>, SavedQueryWorkflowError> {
        match self {
            SavedQueryWorkflowOutcome::LoadedForConnection(records) => Ok(records),
            _ => Err(SavedQueryWorkflowError::Validation(
                "Internal workflow mismatch: expected load-for-connection outcome".to_string(),
            )),
        }
    }

    pub fn into_imported(self) -> Result<Vec<SavedQueryRecord>, SavedQueryWorkflowError> {
        match self {
            SavedQueryWorkflowOutcome::Imported(records) => Ok(records),
            _ => Err(SavedQueryWorkflowError::Validation(
                "Internal workflow mismatch: expected imported outcome".to_string(),
            )),
        }
    }

    pub fn into_exported(self) -> Result<String, SavedQueryWorkflowError> {
        match self {
            SavedQueryWorkflowOutcome::Exported(content) => Ok(content),
            _ => Err(SavedQueryWorkflowError::Validation(
                "Internal workflow mismatch: expected exported outcome".to_string(),
            )),
        }
    }
}

/// Execute a saved-query workflow request through a single typed API.
pub fn run_saved_query_workflow(
    store: &dyn SavedQueryStore,
    request: SavedQueryWorkflowRequest,
) -> Result<SavedQueryWorkflowOutcome, SavedQueryWorkflowError> {
    match request {
        SavedQueryWorkflowRequest::Create {
            name,
            connection_id,
            sql,
            folder,
        } => Ok(SavedQueryWorkflowOutcome::Created(create_saved_query(
            store,
            &name,
            connection_id,
            &sql,
            folder.as_deref(),
        )?)),
        SavedQueryWorkflowRequest::UpdateSql { query_id, sql } => {
            update_saved_query_sql(store, query_id, &sql)?;
            Ok(SavedQueryWorkflowOutcome::Updated)
        }
        SavedQueryWorkflowRequest::MoveToFolder { query_id, folder } => {
            move_saved_query_to_folder(store, query_id, folder.as_deref())?;
            Ok(SavedQueryWorkflowOutcome::Updated)
        }
        SavedQueryWorkflowRequest::Load { query_id } => Ok(SavedQueryWorkflowOutcome::Loaded(
            load_saved_query(store, query_id)?,
        )),
        SavedQueryWorkflowRequest::Rename {
            query_id,
            connection_id,
            new_name,
        } => {
            rename_saved_query(store, query_id, connection_id, &new_name)?;
            Ok(SavedQueryWorkflowOutcome::Renamed)
        }
        SavedQueryWorkflowRequest::Delete { query_id } => {
            delete_saved_query(store, query_id)?;
            Ok(SavedQueryWorkflowOutcome::Deleted)
        }
        SavedQueryWorkflowRequest::LoadForConnection { connection_id } => {
            Ok(SavedQueryWorkflowOutcome::LoadedForConnection(
                load_saved_queries_for_connection(store, connection_id)?,
            ))
        }
        SavedQueryWorkflowRequest::Import {
            connection_id,
            content,
        } => Ok(SavedQueryWorkflowOutcome::Imported(import_saved_queries(
            store,
            connection_id,
            &content,
        )?)),
        SavedQueryWorkflowRequest::Export { connection_id } => Ok(
            SavedQueryWorkflowOutcome::Exported(export_saved_queries(store, connection_id)?),
        ),
    }
}

pub fn create_saved_query(
    store: &dyn SavedQueryStore,
    name: &str,
    connection_id: Uuid,
    sql: &str,
    folder: Option<&str>,
) -> Result<SavedQueryRecord, SavedQueryWorkflowError> {
    let name = name.trim().to_string();
    if let Some(error) = validate_query_name(&name) {
        return Err(SavedQueryWorkflowError::Validation(error.to_string()));
    }

    if store
        .query_name_exists(connection_id, &name)
        .map_err(|error| SavedQueryWorkflowError::Storage(error.to_string()))?
    {
        return Err(SavedQueryWorkflowError::NameAlreadyExists);
    }

    let query = SavedQueryRecord {
        id: Uuid::new_v4(),
        name,
        connection_id,
        sql: sql.to_string(),
        folder: normalize_folder(folder),
    };

    store
        .save_query(&query)
        .map_err(|error| SavedQueryWorkflowError::Storage(error.to_string()))?;

    Ok(query)
}

fn normalize_folder(folder: Option<&str>) -> Option<String> {
    folder
        .map(str::trim)
        .filter(|folder| !folder.is_empty())
        .map(ToOwned::to_owned)
}

pub fn load_saved_query(
    store: &dyn SavedQueryStore,
    query_id: Uuid,
) -> Result<SavedQueryRecord, SavedQueryWorkflowError> {
    store
        .load_query(query_id)
        .map_err(|error| SavedQueryWorkflowError::Storage(error.to_string()))?
        .ok_or(SavedQueryWorkflowError::NotFound)
}

pub fn update_saved_query_sql(
    store: &dyn SavedQueryStore,
    query_id: Uuid,
    sql: &str,
) -> Result<(), SavedQueryWorkflowError> {
    store
        .update_query_sql(query_id, sql)
        .map_err(|error| SavedQueryWorkflowError::Storage(error.to_string()))
}

pub fn move_saved_query_to_folder(
    store: &dyn SavedQueryStore,
    query_id: Uuid,
    folder: Option<&str>,
) -> Result<(), SavedQueryWorkflowError> {
    store
        .move_query_to_folder(query_id, normalize_folder(folder).as_deref())
        .map_err(|error| SavedQueryWorkflowError::Storage(error.to_string()))
}

pub fn rename_saved_query(
    store: &dyn SavedQueryStore,
    query_id: Uuid,
    connection_id: Uuid,
    new_name: &str,
) -> Result<(), SavedQueryWorkflowError> {
    let new_name = new_name.trim().to_string();
    if let Some(error) = validate_query_name(&new_name) {
        return Err(SavedQueryWorkflowError::Validation(error.to_string()));
    }

    if store
        .query_name_exists(connection_id, &new_name)
        .map_err(|error| SavedQueryWorkflowError::Storage(error.to_string()))?
    {
        return Err(SavedQueryWorkflowError::NameAlreadyExists);
    }

    store
        .rename_query(query_id, &new_name)
        .map_err(|error| SavedQueryWorkflowError::Storage(error.to_string()))
}

pub fn delete_saved_query(
    store: &dyn SavedQueryStore,
    query_id: Uuid,
) -> Result<(), SavedQueryWorkflowError> {
    store
        .delete_query(query_id)
        .map_err(|error| SavedQueryWorkflowError::Storage(error.to_string()))
}

pub fn load_saved_queries_for_connection(
    store: &dyn SavedQueryStore,
    connection_id: Uuid,
) -> Result<Vec<SavedQueryRecord>, SavedQueryWorkflowError> {
    store
        .load_queries_for_connection(connection_id)
        .map_err(|error| SavedQueryWorkflowError::Storage(error.to_string()))
}

pub fn export_saved_queries(
    store: &dyn SavedQueryStore,
    connection_id: Uuid,
) -> Result<String, SavedQueryWorkflowError> {
    let queries = load_saved_queries_for_connection(store, connection_id)?;
    serde_json::to_string_pretty(&queries)
        .map_err(|error| SavedQueryWorkflowError::Storage(error.to_string()))
}

pub fn import_saved_queries(
    store: &dyn SavedQueryStore,
    connection_id: Uuid,
    content: &str,
) -> Result<Vec<SavedQueryRecord>, SavedQueryWorkflowError> {
    let queries: Vec<SavedQueryRecord> = serde_json::from_str(content)
        .map_err(|error| SavedQueryWorkflowError::Validation(error.to_string()))?;
    let mut imported = Vec::new();

    for query in queries {
        imported.push(create_saved_query(
            store,
            &query.name,
            connection_id,
            &query.sql,
            query.folder.as_deref(),
        )?);
    }

    Ok(imported)
}

#[cfg(test)]
mod tests {
    use super::{
        SavedQueryRecord, SavedQueryStore, SavedQueryWorkflowError, SavedQueryWorkflowOutcome,
        SavedQueryWorkflowRequest, create_saved_query, rename_saved_query,
        run_saved_query_workflow,
    };
    use std::sync::Mutex;
    use uuid::Uuid;

    struct InMemoryStore {
        records: Mutex<Vec<SavedQueryRecord>>,
    }

    impl InMemoryStore {
        fn new(records: Vec<SavedQueryRecord>) -> Self {
            Self {
                records: Mutex::new(records),
            }
        }
    }

    impl SavedQueryStore for InMemoryStore {
        fn query_name_exists(&self, connection_id: Uuid, name: &str) -> anyhow::Result<bool> {
            Ok(self
                .records
                .lock()
                .expect("test mutex poisoned")
                .iter()
                .any(|record| record.connection_id == connection_id && record.name == name))
        }

        fn save_query(&self, query: &SavedQueryRecord) -> anyhow::Result<()> {
            self.records
                .lock()
                .expect("test mutex poisoned")
                .push(query.clone());
            Ok(())
        }

        fn load_query(&self, query_id: Uuid) -> anyhow::Result<Option<SavedQueryRecord>> {
            Ok(self
                .records
                .lock()
                .expect("test mutex poisoned")
                .iter()
                .find(|record| record.id == query_id)
                .cloned())
        }

        fn update_query_sql(&self, query_id: Uuid, sql: &str) -> anyhow::Result<()> {
            if let Some(record) = self
                .records
                .lock()
                .expect("test mutex poisoned")
                .iter_mut()
                .find(|record| record.id == query_id)
            {
                record.sql = sql.to_string();
            }

            Ok(())
        }

        fn move_query_to_folder(&self, query_id: Uuid, folder: Option<&str>) -> anyhow::Result<()> {
            if let Some(record) = self
                .records
                .lock()
                .expect("test mutex poisoned")
                .iter_mut()
                .find(|record| record.id == query_id)
            {
                record.folder = folder.map(ToOwned::to_owned);
            }

            Ok(())
        }

        fn rename_query(&self, query_id: Uuid, new_name: &str) -> anyhow::Result<()> {
            if let Some(record) = self
                .records
                .lock()
                .expect("test mutex poisoned")
                .iter_mut()
                .find(|record| record.id == query_id)
            {
                record.name = new_name.to_string();
            }

            Ok(())
        }

        fn delete_query(&self, query_id: Uuid) -> anyhow::Result<()> {
            self.records
                .lock()
                .expect("test mutex poisoned")
                .retain(|record| record.id != query_id);
            Ok(())
        }

        fn load_queries_for_connection(
            &self,
            connection_id: Uuid,
        ) -> anyhow::Result<Vec<SavedQueryRecord>> {
            Ok(self
                .records
                .lock()
                .expect("test mutex poisoned")
                .iter()
                .filter(|record| record.connection_id == connection_id)
                .cloned()
                .collect())
        }
    }

    #[test]
    fn create_saved_query_rejects_duplicate_name() {
        let connection_id = Uuid::new_v4();
        let store = InMemoryStore::new(vec![SavedQueryRecord {
            id: Uuid::new_v4(),
            name: "Existing".to_string(),
            connection_id,
            sql: "select 1".to_string(),
            folder: None,
        }]);

        let result = create_saved_query(&store, "Existing", connection_id, "select 2", None);
        assert!(matches!(
            result,
            Err(SavedQueryWorkflowError::NameAlreadyExists)
        ));
    }

    #[test]
    fn rename_saved_query_rejects_invalid_name() {
        let store = InMemoryStore::new(Vec::new());
        let result = rename_saved_query(&store, Uuid::new_v4(), Uuid::new_v4(), "");
        assert!(matches!(
            result,
            Err(SavedQueryWorkflowError::Validation(_))
        ));
    }

    #[test]
    fn run_saved_query_workflow_supports_save_update_load_rename_delete_lifecycle() {
        let connection_id = Uuid::new_v4();
        let store = InMemoryStore::new(Vec::new());

        let created = run_saved_query_workflow(
            &store,
            SavedQueryWorkflowRequest::Create {
                name: "Lifecycle Query".to_string(),
                connection_id,
                sql: "select 1".to_string(),
                folder: Some("Analytics".to_string()),
            },
        )
        .and_then(SavedQueryWorkflowOutcome::into_created)
        .expect("create workflow should succeed");

        run_saved_query_workflow(
            &store,
            SavedQueryWorkflowRequest::UpdateSql {
                query_id: created.id,
                sql: "select 2".to_string(),
            },
        )
        .and_then(SavedQueryWorkflowOutcome::into_updated)
        .expect("update workflow should succeed");

        let loaded_after_update = run_saved_query_workflow(
            &store,
            SavedQueryWorkflowRequest::Load {
                query_id: created.id,
            },
        )
        .and_then(SavedQueryWorkflowOutcome::into_loaded)
        .expect("load workflow should succeed after update");
        assert_eq!(loaded_after_update.sql, "select 2");

        run_saved_query_workflow(
            &store,
            SavedQueryWorkflowRequest::Rename {
                query_id: created.id,
                connection_id,
                new_name: "Renamed Query".to_string(),
            },
        )
        .and_then(SavedQueryWorkflowOutcome::into_renamed)
        .expect("rename workflow should succeed");

        let loaded_for_connection = run_saved_query_workflow(
            &store,
            SavedQueryWorkflowRequest::LoadForConnection { connection_id },
        )
        .and_then(SavedQueryWorkflowOutcome::into_loaded_for_connection)
        .expect("load-for-connection workflow should succeed");
        assert_eq!(loaded_for_connection.len(), 1);
        assert_eq!(loaded_for_connection[0].id, created.id);
        assert_eq!(loaded_for_connection[0].name, "Renamed Query");
        assert_eq!(
            loaded_for_connection[0].folder.as_deref(),
            Some("Analytics")
        );

        run_saved_query_workflow(
            &store,
            SavedQueryWorkflowRequest::MoveToFolder {
                query_id: created.id,
                folder: None,
            },
        )
        .and_then(SavedQueryWorkflowOutcome::into_updated)
        .expect("move to root should succeed");

        let exported =
            run_saved_query_workflow(&store, SavedQueryWorkflowRequest::Export { connection_id })
                .and_then(SavedQueryWorkflowOutcome::into_exported)
                .expect("export should succeed");
        assert!(exported.contains("Renamed Query"));

        run_saved_query_workflow(
            &store,
            SavedQueryWorkflowRequest::Delete {
                query_id: created.id,
            },
        )
        .and_then(SavedQueryWorkflowOutcome::into_deleted)
        .expect("delete workflow should succeed");

        let load_after_delete = run_saved_query_workflow(
            &store,
            SavedQueryWorkflowRequest::Load {
                query_id: created.id,
            },
        )
        .and_then(SavedQueryWorkflowOutcome::into_loaded);
        assert!(matches!(
            load_after_delete,
            Err(SavedQueryWorkflowError::NotFound)
        ));
    }
}
