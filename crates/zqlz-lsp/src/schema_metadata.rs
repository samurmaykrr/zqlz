use std::collections::HashMap;

use zqlz_services::DatabaseSchema;

use crate::SchemaCache;

pub(crate) fn schema_for_metadata(schema_cache: &SchemaCache) -> DatabaseSchema {
    let tables: Vec<String> = schema_cache.tables.keys().cloned().collect();

    let materialized_views: Vec<String> = schema_cache
        .views
        .values()
        .filter(|view| view.is_materialized)
        .map(|view| view.name.clone())
        .collect();
    let functions: Vec<String> = schema_cache.functions.keys().cloned().collect();
    let procedures: Vec<String> = schema_cache.procedures.keys().cloned().collect();
    let triggers: Vec<String> = schema_cache.triggers.keys().cloned().collect();
    let sequences: Vec<String> = schema_cache.sequences.keys().cloned().collect();
    let views: Vec<String> = schema_cache
        .views
        .values()
        .filter(|view| !view.is_materialized)
        .map(|view| view.name.clone())
        .collect();

    let table_infos: Vec<zqlz_core::TableInfo> = schema_cache
        .tables
        .values()
        .map(|table| zqlz_core::TableInfo {
            name: table.name.clone(),
            schema: table.schema.clone(),
            table_type: table.table_type,
            owner: None,
            row_count: table.row_count,
            size_bytes: None,
            comment: table.comment.clone(),
            index_count: None,
            trigger_count: None,
            key_value_info: None,
        })
        .collect();

    let mut table_indexes: HashMap<String, Vec<zqlz_core::IndexInfo>> = HashMap::new();
    for (table_name, index_info) in &schema_cache.indexes {
        let converted = vec![zqlz_core::IndexInfo {
            name: index_info.name.clone(),
            columns: index_info.columns.clone(),
            is_unique: index_info.is_unique,
            is_primary: false,
            index_type: "btree".to_string(),
            comment: None,
            ..Default::default()
        }];
        table_indexes.insert(table_name.clone(), converted);
    }

    DatabaseSchema {
        table_infos,
        objects_panel_data: None,
        objects_panel_manifest: None,
        tables,
        views,
        materialized_views,
        triggers,
        functions,
        procedures,
        events: Vec::new(),
        sequences,
        domains: Vec::new(),
        types: Vec::new(),
        extensions: Vec::new(),
        table_indexes,
        database_name: schema_cache.database_name.clone(),
        schema_name: schema_cache.schema_name.clone(),
        schema_names: schema_cache.schema_names.clone(),
    }
}
