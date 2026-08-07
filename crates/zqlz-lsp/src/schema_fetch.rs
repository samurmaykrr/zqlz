use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;
use zqlz_core::Connection;
use zqlz_services::{SchemaService, TableDetails};

use crate::{
    ColumnInfo, DatabaseObject, FunctionInfo, IndexInfo, ParameterDirection, ParameterInfo,
    ProcedureInfo, SchemaCache, TableInfo, TriggerInfo, ViewInfo,
};

/// Projects a service-level `TableDetails` onto the LSP's flatter `ColumnInfo`.
///
/// Shared by the full schema fetch and by `SqlLsp::merge_table_columns` so an
/// incrementally merged table looks identical to a fully fetched one.
pub(crate) fn columns_from_table_details(
    table_name: &str,
    details: &TableDetails,
) -> Vec<ColumnInfo> {
    columns_from_parts(table_name, &details.columns, &details.foreign_keys)
}

/// The projection itself, shared by every entry point.
///
/// `is_foreign_key` is derived from the foreign-key list rather than read off the
/// column, so the two must always be passed together.
pub(crate) fn columns_from_parts(
    table_name: &str,
    columns: &[zqlz_services::ColumnInfo],
    foreign_keys: &[zqlz_core::ForeignKeyInfo],
) -> Vec<ColumnInfo> {
    let foreign_key_columns: HashSet<&str> = foreign_keys
        .iter()
        .flat_map(|foreign_key| foreign_key.columns.iter().map(String::as_str))
        .collect();

    columns
        .iter()
        .map(|column| ColumnInfo {
            table_name: table_name.to_string(),
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            default_value: column.default_value.clone(),
            is_primary_key: column.is_primary_key,
            is_foreign_key: foreign_key_columns.contains(column.name.as_str()),
            comment: column.comment.clone(),
        })
        .collect()
}

pub(crate) async fn fetch_schema_cache(
    connection: Arc<dyn Connection>,
    connection_id: Uuid,
    active_database: Option<String>,
    active_schema: Option<String>,
    schema_service: &SchemaService,
) -> Result<SchemaCache> {
    let db_schema = schema_service
        .load_database_schema_for_database_and_schema(
            connection.clone(),
            connection_id,
            active_database.as_deref(),
            active_schema.as_deref(),
        )
        .await?;

    tracing::info!(
        "Schema loaded via SchemaService: {} tables, {} views",
        db_schema.tables.len(),
        db_schema.views.len()
    );

    let mut cache = SchemaCache {
        database_name: db_schema.database_name.clone(),
        schema_name: db_schema.schema_name.clone(),
        schema_names: db_schema.schema_names.clone(),
        ..SchemaCache::default()
    };

    for table in &db_schema.table_infos {
        let table_info = TableInfo {
            name: table.name.clone(),
            schema: table.schema.clone(),
            comment: table.comment.clone(),
            row_count: table.row_count,
            table_type: table.table_type,
        };
        cache.tables.insert(table.name.clone(), table_info.clone());
        cache.objects.push(DatabaseObject::Table(table_info));
    }

    for view_name in &db_schema.views {
        let view_info = ViewInfo {
            name: view_name.clone(),
            schema: None,
            definition: None,
            is_materialized: false,
        };
        cache.views.insert(view_name.clone(), view_info.clone());
        cache.objects.push(DatabaseObject::View(view_info));
    }

    for view_name in &db_schema.materialized_views {
        let view_info = ViewInfo {
            name: view_name.clone(),
            schema: None,
            definition: None,
            is_materialized: true,
        };
        cache.views.insert(view_name.clone(), view_info.clone());
        cache.objects.push(DatabaseObject::View(view_info));
    }

    if let Some(cached_details) = schema_service.get_all_cached_table_details(connection_id) {
        for (table_name, details) in cached_details {
            let column_infos = columns_from_table_details(&table_name, &details);

            for column in &column_infos {
                cache.objects.push(DatabaseObject::Column(column.clone()));
            }
            cache
                .columns_by_table
                .insert(table_name.clone(), column_infos);

            for foreign_key in &details.foreign_keys {
                cache
                    .foreign_keys_by_table
                    .entry(table_name.clone())
                    .or_default()
                    .push(foreign_key.clone());

                cache
                    .reverse_foreign_keys
                    .entry(foreign_key.referenced_table.clone())
                    .or_default()
                    .push((table_name.clone(), foreign_key.clone()));
            }
        }
    }

    // The `TableDetails` cache above is only warm for tables something has already
    // opened. Ask the driver for the whole schema's columns in one query — cheaper
    // than the per-table caches it would otherwise have to hope are populated, and
    // immune to the snapshot invalidation a scoped reload performs.
    if let Some(summaries) = schema_service
        .try_bulk_schema_columns(&connection, connection_id, active_schema.as_deref())
        .await
    {
        for (table_name, summary) in summaries {
            if cache
                .columns_by_table
                .keys()
                .any(|name| name.eq_ignore_ascii_case(&table_name))
            {
                continue;
            }

            let column_infos =
                columns_from_parts(&table_name, &summary.columns, &summary.foreign_keys);
            for column in &column_infos {
                cache.objects.push(DatabaseObject::Column(column.clone()));
            }
            cache
                .columns_by_table
                .insert(table_name.clone(), column_infos);

            for foreign_key in &summary.foreign_keys {
                cache
                    .foreign_keys_by_table
                    .entry(table_name.clone())
                    .or_default()
                    .push(foreign_key.clone());
                cache
                    .reverse_foreign_keys
                    .entry(foreign_key.referenced_table.clone())
                    .or_default()
                    .push((table_name.clone(), foreign_key.clone()));
            }
        }
    }

    // Last resort for drivers with no bulk form: whatever the per-table column
    // cache happens to hold, filled by the table viewer, designer or the paced
    // warm-up.
    if let Some(cached_columns) = schema_service.get_all_cached_columns(connection_id) {
        for (cache_key, columns) in cached_columns {
            let table_name = cache_key
                .rsplit_once('.')
                .map(|(_, name)| name)
                .unwrap_or(&cache_key);

            if cache
                .columns_by_table
                .keys()
                .any(|name| name.eq_ignore_ascii_case(table_name))
            {
                continue;
            }

            let column_infos: Vec<ColumnInfo> = columns
                .iter()
                .map(|column| ColumnInfo {
                    table_name: table_name.to_string(),
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                    default_value: column.default_value.clone(),
                    is_primary_key: column.is_primary_key,
                    is_foreign_key: column.foreign_key.is_some(),
                    comment: column.comment.clone(),
                })
                .collect();

            for column in &column_infos {
                cache.objects.push(DatabaseObject::Column(column.clone()));
            }
            cache
                .columns_by_table
                .insert(table_name.to_string(), column_infos);
        }
    }

    for trigger_name in &db_schema.triggers {
        let trigger_info = TriggerInfo {
            name: trigger_name.clone(),
            table_name: String::new(),
            event: String::new(),
            timing: String::new(),
            definition: None,
        };
        cache
            .triggers
            .insert(trigger_name.clone(), trigger_info.clone());
        cache.objects.push(DatabaseObject::Trigger(trigger_info));
    }

    for function_name in &db_schema.functions {
        let function_info = FunctionInfo {
            name: function_name.clone(),
            schema: None,
            return_type: String::new(),
            parameters: Vec::new(),
            definition: None,
            is_aggregate: false,
            comment: None,
        };
        cache
            .functions
            .insert(function_name.clone(), function_info.clone());
        cache.objects.push(DatabaseObject::Function(function_info));
    }

    for procedure_name in &db_schema.procedures {
        let procedure_info = ProcedureInfo {
            name: procedure_name.clone(),
            schema: None,
            parameters: Vec::new(),
            return_type: None,
            definition: None,
            comment: None,
        };
        cache
            .procedures
            .insert(procedure_name.clone(), procedure_info.clone());
        cache
            .objects
            .push(DatabaseObject::StoredProcedure(procedure_info));
    }

    for (table_name, indexes) in &db_schema.table_indexes {
        for index in indexes {
            let index_info = IndexInfo {
                name: index.name.clone(),
                table_name: table_name.clone(),
                columns: index.columns.clone(),
                is_unique: index.is_unique,
            };
            cache.indexes.insert(index.name.clone(), index_info.clone());
            cache.objects.push(DatabaseObject::Index(index_info));
        }
    }

    let metadata_schema = zqlz_core::resolve_metadata_scope_for_connection(
        connection.as_ref(),
        active_database.as_deref(),
        active_schema.as_deref(),
        db_schema.database_name.as_deref(),
        db_schema.schema_name.as_deref(),
    );

    if let Some(schema) = connection.as_schema_introspection() {
        match schema.list_sequences(metadata_schema.as_deref()).await {
            Ok(sequences) => {
                for sequence in sequences {
                    cache
                        .sequences
                        .insert(sequence.name.clone(), sequence.clone());
                    cache.objects.push(DatabaseObject::Sequence(sequence));
                }
            }
            Err(error) => {
                tracing::warn!("Failed to load sequences: {}", error);
            }
        }
    }

    cache.last_refresh = Some(std::time::SystemTime::now());

    let total_columns: usize = cache
        .columns_by_table
        .values()
        .map(|columns| columns.len())
        .sum();
    tracing::info!(
        "Schema cache built: {} tables, {} views, {} columns, {} indexes, {} triggers, {} functions, {} procedures, {} sequences",
        cache.tables.len(),
        cache.views.len(),
        total_columns,
        cache.indexes.len(),
        cache.triggers.len(),
        cache.functions.len(),
        cache.procedures.len(),
        cache.sequences.len()
    );

    Ok(cache)
}

pub(crate) async fn fetch_procedures(conn: &dyn Connection) -> Result<Vec<ProcedureInfo>> {
    let introspection = conn
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("Schema introspection not supported"))?;
    let procedures = introspection.list_procedures(None).await?;
    Ok(procedures
        .into_iter()
        .map(|procedure| ProcedureInfo {
            name: procedure.name,
            schema: procedure.schema,
            parameters: procedure
                .parameters
                .into_iter()
                .map(|parameter| ParameterInfo {
                    name: parameter.name.unwrap_or_default(),
                    data_type: parameter.data_type,
                    direction: match parameter.mode {
                        zqlz_core::ParameterMode::Out => ParameterDirection::Out,
                        zqlz_core::ParameterMode::InOut => ParameterDirection::InOut,
                        _ => ParameterDirection::In,
                    },
                })
                .collect(),
            return_type: None,
            definition: procedure.definition,
            comment: procedure.comment,
        })
        .collect())
}

pub(crate) async fn fetch_functions(conn: &dyn Connection) -> Result<Vec<FunctionInfo>> {
    let introspection = conn
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("Schema introspection not supported"))?;
    let functions = introspection.list_functions(None).await?;
    Ok(functions
        .into_iter()
        .map(|function| FunctionInfo {
            name: function.name,
            schema: function.schema,
            parameters: function
                .parameters
                .into_iter()
                .map(|parameter| ParameterInfo {
                    name: parameter.name.unwrap_or_default(),
                    data_type: parameter.data_type,
                    direction: match parameter.mode {
                        zqlz_core::ParameterMode::Out => ParameterDirection::Out,
                        zqlz_core::ParameterMode::InOut => ParameterDirection::InOut,
                        _ => ParameterDirection::In,
                    },
                })
                .collect(),
            return_type: function.return_type,
            definition: function.definition,
            is_aggregate: false,
            comment: function.comment,
        })
        .collect())
}

pub(crate) async fn fetch_triggers(conn: &dyn Connection) -> Result<Vec<TriggerInfo>> {
    let introspection = conn
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("Schema introspection not supported"))?;
    let triggers = introspection.list_triggers(None, None).await?;
    Ok(triggers
        .into_iter()
        .map(|trigger| TriggerInfo {
            name: trigger.name,
            table_name: trigger.table_name,
            event: trigger
                .events
                .first()
                .map(|event| match event {
                    zqlz_core::TriggerEvent::Insert => "INSERT",
                    zqlz_core::TriggerEvent::Update => "UPDATE",
                    zqlz_core::TriggerEvent::Delete => "DELETE",
                    zqlz_core::TriggerEvent::Truncate => "TRUNCATE",
                })
                .unwrap_or("UNKNOWN")
                .to_string(),
            timing: match trigger.timing {
                zqlz_core::TriggerTiming::Before => "BEFORE",
                zqlz_core::TriggerTiming::After => "AFTER",
                zqlz_core::TriggerTiming::InsteadOf => "INSTEAD OF",
            }
            .to_string(),
            definition: trigger.definition,
        })
        .collect())
}

pub(crate) async fn fetch_indexes(conn: &dyn Connection) -> Result<Vec<IndexInfo>> {
    let introspection = conn
        .as_schema_introspection()
        .ok_or_else(|| anyhow::anyhow!("Schema introspection not supported"))?;

    let tables = introspection.list_tables(None).await?;
    let mut indexes = Vec::new();
    for table in tables {
        let table_indexes = introspection
            .get_indexes(table.schema.as_deref(), &table.name)
            .await?;
        indexes.extend(table_indexes.into_iter().map(|index| IndexInfo {
            name: index.name,
            table_name: table.name.clone(),
            columns: index.columns,
            is_unique: index.is_unique,
        }));
    }

    Ok(indexes)
}
