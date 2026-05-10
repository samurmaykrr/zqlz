//! Generic key-value orchestration.

use std::sync::Arc;

use uuid::Uuid;
use zqlz_core::{
    ColumnMeta, Connection, KeyValueCellUpdateRequest, KeyValueDeleteOutcome,
    KeyValueDeleteRequest, KeyValueKind, KeyValueSaveOutcome, KeyValueSaveRequest,
    KeyValueScanRequest, QueryResult, Row, Value,
};

use crate::error::{ServiceError, ServiceResult};
use crate::table_service::CellUpdateData;

#[derive(Debug, Clone)]
pub struct LoadKeyValueKeysRequest {
    pub database_index: u16,
    pub limit: usize,
    pub scan_batch_size: usize,
}

#[derive(Debug, Clone, Default)]
pub struct LoadKeyValueKeysOutcome {
    pub keys: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LoadKeyValueDatabaseRowsRequest {
    pub database_index: u16,
}

#[derive(Debug, Clone)]
pub struct LoadKeyValueDatabaseRowsOutcome {
    pub query_result: QueryResult,
}

pub struct KeyValueService {
    default_limit: usize,
}

impl KeyValueService {
    pub fn new(default_limit: usize) -> Self {
        Self { default_limit }
    }

    pub async fn load_databases(
        &self,
        connection: Arc<dyn Connection>,
    ) -> ServiceResult<Vec<(u16, Option<i64>)>> {
        let store = Self::store(connection.as_ref())?;
        let databases = store
            .list_key_value_databases()
            .await
            .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))?;

        Ok(databases
            .into_iter()
            .map(|database| (database.index, database.size_bytes))
            .collect())
    }

    pub async fn load_keys(
        &self,
        connection: Arc<dyn Connection>,
        request: LoadKeyValueKeysRequest,
    ) -> ServiceResult<LoadKeyValueKeysOutcome> {
        let store = Self::store(connection.as_ref())?;
        let result = store
            .scan_keys(KeyValueScanRequest {
                database_index: request.database_index,
                limit: request.limit.max(1),
                scan_batch_size: request.scan_batch_size.max(1),
            })
            .await
            .map_err(|error| ServiceError::TableOperationFailed(error.to_string()))?;

        Ok(LoadKeyValueKeysOutcome { keys: result.keys })
    }

    pub async fn load_database_rows(
        &self,
        connection: Arc<dyn Connection>,
        request: LoadKeyValueDatabaseRowsRequest,
    ) -> ServiceResult<LoadKeyValueDatabaseRowsOutcome> {
        let store = Self::store(connection.as_ref())?;
        let summaries = store
            .load_key_summaries(request.database_index)
            .await
            .map_err(|error| ServiceError::TableOperationFailed(error.to_string()))?;

        let columns = Self::database_viewer_columns();
        let column_names: Vec<String> = columns.iter().map(|column| column.name.clone()).collect();
        let rows = summaries
            .into_iter()
            .map(|summary| {
                Row::new(
                    column_names.clone(),
                    vec![
                        Value::String(summary.key),
                        Value::String(summary.kind.as_str().to_string()),
                        summary.preview.map(Value::String).unwrap_or(Value::Null),
                        summary
                            .size_bytes
                            .map(Self::format_bytes_for_display)
                            .map(Value::String)
                            .unwrap_or(Value::Null),
                        summary
                            .ttl_seconds
                            .map(Self::format_ttl_for_display)
                            .map(Value::String)
                            .unwrap_or_else(|| Value::String("No TTL".to_string())),
                    ],
                )
            })
            .collect::<Vec<_>>();

        Ok(LoadKeyValueDatabaseRowsOutcome {
            query_result: QueryResult {
                id: Uuid::new_v4(),
                columns,
                total_rows: Some(rows.len() as u64),
                is_estimated_total: false,
                rows,
                affected_rows: 0,
                execution_time_ms: 0,
                warnings: Vec::new(),
            },
        })
    }

    pub async fn browse_key(
        &self,
        connection: Arc<dyn Connection>,
        key_name: &str,
        limit: Option<usize>,
    ) -> ServiceResult<QueryResult> {
        let store = Self::store(connection.as_ref())?;
        let entry = store
            .read_key(key_name, limit.unwrap_or(self.default_limit))
            .await
            .map_err(|error| ServiceError::TableOperationFailed(error.to_string()))?;
        Ok(entry.data)
    }

    pub async fn save_key(
        &self,
        connection: Arc<dyn Connection>,
        request: KeyValueSaveRequest,
    ) -> ServiceResult<KeyValueSaveOutcome> {
        let store = Self::store(connection.as_ref())?;
        store
            .save_key(request)
            .await
            .map_err(|error| ServiceError::TableOperationFailed(error.to_string()))
    }

    pub async fn update_key_cell(
        &self,
        connection: Arc<dyn Connection>,
        key_name: &str,
        kind: Option<KeyValueKind>,
        cell_data: CellUpdateData,
    ) -> ServiceResult<()> {
        let store = Self::store(connection.as_ref())?;
        let kind = match kind {
            Some(kind) if kind != KeyValueKind::None => kind,
            _ => {
                store
                    .read_key(key_name, 1)
                    .await
                    .map_err(|error| ServiceError::UpdateFailed(error.to_string()))?
                    .kind
            }
        };

        store
            .update_key_cell(KeyValueCellUpdateRequest {
                key: key_name.to_string(),
                kind,
                column_name: cell_data.column_name,
                new_value: cell_data.new_value,
                row_values: cell_data.all_row_values,
            })
            .await
            .map_err(|error| ServiceError::UpdateFailed(error.to_string()))
    }

    pub async fn delete_keys(
        &self,
        connection: Arc<dyn Connection>,
        request: KeyValueDeleteRequest,
    ) -> KeyValueDeleteOutcome {
        let Some(store) = connection.as_key_value_store() else {
            return KeyValueDeleteOutcome {
                deleted_key_names: Vec::new(),
                errors: vec!["Connection does not support key-value operations".to_string()],
            };
        };

        match store.delete_keys(request).await {
            Ok(outcome) => outcome,
            Err(error) => KeyValueDeleteOutcome {
                deleted_key_names: Vec::new(),
                errors: vec![error.to_string()],
            },
        }
    }

    fn store(connection: &dyn Connection) -> ServiceResult<&dyn zqlz_core::KeyValueStore> {
        connection.as_key_value_store().ok_or_else(|| {
            ServiceError::TableOperationFailed(
                "Connection does not support key-value operations".to_string(),
            )
        })
    }

    fn database_viewer_columns() -> Vec<ColumnMeta> {
        vec![
            Self::column("Key", "key name", 0),
            Self::column("Type", "data type", 1),
            Self::column("Value", "value preview", 2),
            Self::column("Size", "memory size", 3),
            Self::column("TTL", "time to live", 4),
        ]
    }

    fn column(name: &str, comment: &str, ordinal: usize) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            data_type: "TEXT".to_string(),
            nullable: ordinal > 1,
            ordinal,
            max_length: None,
            precision: None,
            scale: None,
            auto_increment: false,
            default_value: None,
            comment: Some(comment.to_string()),
            enum_values: None,
        }
    }

    fn format_bytes_for_display(bytes: i64) -> String {
        if bytes < 1024 {
            format!("{} B", bytes)
        } else if bytes < 1024 * 1024 {
            format!("{:.1} KB", bytes as f64 / 1024.0)
        } else if bytes < 1024 * 1024 * 1024 {
            format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
        } else {
            format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        }
    }

    fn format_ttl_for_display(seconds: i64) -> String {
        if seconds == -1 {
            "No TTL".to_string()
        } else if seconds == -2 {
            "Not Found".to_string()
        } else if seconds < 60 {
            format!("{}s", seconds)
        } else if seconds < 3600 {
            format!("{}m {}s", seconds / 60, seconds % 60)
        } else if seconds < 86400 {
            format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
        } else {
            format!("{}d {}h", seconds / 86400, (seconds % 86400) / 3600)
        }
    }
}
