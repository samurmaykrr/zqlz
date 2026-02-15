//! Tests for lazy schema cache

use super::*;
use std::thread;
use std::time::Duration;
use zqlz_core::TableType;

// ============ CacheEntry Tests ============

mod cache_entry_tests {
    use super::*;

    #[test]
    fn test_cache_entry_not_loaded() {
        let entry: CacheEntry<Vec<String>> = CacheEntry::NotLoaded;
        assert!(entry.is_not_loaded());
        assert!(!entry.is_loading());
        assert!(!entry.is_loaded());
        assert!(entry.get().is_none());
    }

    #[test]
    fn test_cache_entry_loading() {
        let entry: CacheEntry<Vec<String>> = CacheEntry::Loading;
        assert!(entry.is_loading());
        assert!(!entry.is_not_loaded());
        assert!(!entry.is_loaded());
        assert!(entry.get().is_none());
    }

    #[test]
    fn test_cache_entry_loaded() {
        let data = vec!["a".to_string(), "b".to_string()];
        let entry = CacheEntry::Loaded(data.clone());
        assert!(entry.is_loaded());
        assert!(!entry.is_not_loaded());
        assert!(!entry.is_loading());
        assert_eq!(entry.get(), Some(&data));
    }
}

// ============ LazyCacheConfig Tests ============

mod config_tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = LazyCacheConfig::default();
        assert_eq!(config.list_ttl, Duration::from_secs(300));
        assert_eq!(config.detail_ttl, Duration::from_secs(180));
        assert_eq!(config.stats_ttl, Duration::from_secs(60));
    }

    #[test]
    fn test_uniform_config() {
        let ttl = Duration::from_secs(120);
        let config = LazyCacheConfig::uniform(ttl);
        assert_eq!(config.list_ttl, ttl);
        assert_eq!(config.detail_ttl, ttl);
        assert_eq!(config.stats_ttl, ttl);
    }

    #[test]
    fn test_custom_config() {
        let config = LazyCacheConfig::new(
            Duration::from_secs(600),
            Duration::from_secs(300),
            Duration::from_secs(120),
        );
        assert_eq!(config.list_ttl, Duration::from_secs(600));
        assert_eq!(config.detail_ttl, Duration::from_secs(300));
        assert_eq!(config.stats_ttl, Duration::from_secs(120));
    }
}

// ============ LazySchemaCache Table Tests ============

mod table_cache_tests {
    use super::*;

    fn make_table(name: &str) -> TableInfo {
        TableInfo {
            schema: None,
            name: name.to_string(),
            table_type: TableType::Table,
            owner: None,
            row_count: Some(100),
            size_bytes: None,
            comment: None,
            index_count: Some(2),
            trigger_count: Some(0),
            key_value_info: None,
        }
    }

    #[test]
    fn test_lazy_load_on_first_access() {
        let cache = LazySchemaCache::with_defaults();
        let conn_id = Uuid::new_v4();

        // First access returns NotLoaded
        let state = cache.get_tables_state(conn_id);
        assert!(state.is_not_loaded());
        assert!(cache.get_tables(conn_id).is_none());
    }

    #[test]
    fn test_cache_returns_loaded_data() {
        let cache = LazySchemaCache::with_defaults();
        let conn_id = Uuid::new_v4();
        let tables = vec![make_table("users"), make_table("orders")];

        // Load tables
        cache.set_tables(conn_id, tables.clone());

        // Should return loaded data
        let result = cache.get_tables(conn_id);
        assert!(result.is_some());
        let cached = result.unwrap();
        assert_eq!(cached.len(), 2);
        assert_eq!(cached[0].name, "users");
        assert_eq!(cached[1].name, "orders");
    }

    #[test]
    fn test_set_loading_returns_true_first_time() {
        let cache = LazySchemaCache::with_defaults();
        let conn_id = Uuid::new_v4();

        // First set_loading should succeed
        assert!(cache.set_tables_loading(conn_id));

        // Should be in loading state
        let state = cache.get_tables_state(conn_id);
        assert!(state.is_loading());
    }

    #[test]
    fn test_set_loading_returns_false_if_already_loading() {
        let cache = LazySchemaCache::with_defaults();
        let conn_id = Uuid::new_v4();

        // First set_loading succeeds
        assert!(cache.set_tables_loading(conn_id));

        // Second set_loading should fail
        assert!(!cache.set_tables_loading(conn_id));
    }

    #[test]
    fn test_concurrent_access_no_duplicate_loads() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cache = Arc::new(LazySchemaCache::with_defaults());
        let conn_id = Uuid::new_v4();
        let load_count = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        // Spawn 10 threads trying to load simultaneously
        for _ in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let load_count_clone = Arc::clone(&load_count);

            let handle = thread::spawn(move || {
                if cache_clone.set_tables_loading(conn_id) {
                    // Only one thread should succeed
                    load_count_clone.fetch_add(1, Ordering::SeqCst);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Only one thread should have been able to set loading
        assert_eq!(load_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_cache_expiration() {
        // Use very short TTL for testing
        let config = LazyCacheConfig::uniform(Duration::from_millis(50));
        let cache = LazySchemaCache::new(config);
        let conn_id = Uuid::new_v4();
        let tables = vec![make_table("test")];

        // Load tables
        cache.set_tables(conn_id, tables);
        assert!(cache.get_tables(conn_id).is_some());

        // Wait for expiration
        thread::sleep(Duration::from_millis(60));

        // Should be expired
        assert!(cache.get_tables(conn_id).is_none());
        let state = cache.get_tables_state(conn_id);
        assert!(state.is_not_loaded());
    }
}

// ============ LazySchemaCache Column Tests ============

mod column_cache_tests {
    use super::*;

    fn make_column(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            ordinal: 0,
            data_type: "text".to_string(),
            nullable: true,
            default_value: None,
            max_length: None,
            precision: None,
            scale: None,
            is_primary_key: false,
            is_auto_increment: false,
            is_unique: false,
            foreign_key: None,
            comment: None,
        }
    }

    #[test]
    fn test_column_cache_per_table() {
        let cache = LazySchemaCache::with_defaults();
        let conn_id = Uuid::new_v4();

        let cols_users = vec![make_column("id"), make_column("name")];
        let cols_orders = vec![make_column("id"), make_column("total")];

        cache.set_columns(conn_id, "users", cols_users.clone());
        cache.set_columns(conn_id, "orders", cols_orders.clone());

        // Get users columns
        let result = cache.get_columns(conn_id, "users");
        assert!(result.is_some());
        let cached = result.unwrap();
        assert_eq!(cached.len(), 2);
        assert_eq!(cached[0].name, "id");
        assert_eq!(cached[1].name, "name");

        // Get orders columns
        let result = cache.get_columns(conn_id, "orders");
        assert!(result.is_some());
        let cached = result.unwrap();
        assert_eq!(cached[0].name, "id");
        assert_eq!(cached[1].name, "total");

        // Non-existent table returns None
        assert!(cache.get_columns(conn_id, "products").is_none());
    }

    #[test]
    fn test_column_loading_state() {
        let cache = LazySchemaCache::with_defaults();
        let conn_id = Uuid::new_v4();

        // Not loaded initially
        let state = cache.get_columns_state(conn_id, "users");
        assert!(state.is_not_loaded());

        // Set loading
        assert!(cache.set_columns_loading(conn_id, "users"));
        let state = cache.get_columns_state(conn_id, "users");
        assert!(state.is_loading());

        // Second set_loading fails
        assert!(!cache.set_columns_loading(conn_id, "users"));
    }
}

// ============ Prefetch Tests ============

mod prefetch_tests {
    use super::*;

    fn make_table(name: &str) -> TableInfo {
        TableInfo {
            schema: None,
            name: name.to_string(),
            table_type: TableType::Table,
            owner: None,
            row_count: Some(100),
            size_bytes: None,
            comment: None,
            index_count: Some(2),
            trigger_count: Some(0),
            key_value_info: None,
        }
    }

    #[test]
    fn test_prefetch_multiple_schemas() {
        let cache = LazySchemaCache::with_defaults();
        let conn1 = Uuid::new_v4();
        let conn2 = Uuid::new_v4();
        let conn3 = Uuid::new_v4();

        // Load tables for conn1 only
        cache.set_tables(conn1, vec![make_table("t1")]);

        // Check which connections need prefetch
        let to_prefetch = cache.tables_to_prefetch(&[conn1, conn2, conn3]);
        assert_eq!(to_prefetch.len(), 2);
        assert!(to_prefetch.contains(&conn2));
        assert!(to_prefetch.contains(&conn3));
        assert!(!to_prefetch.contains(&conn1));
    }

    #[test]
    fn test_tables_needing_columns() {
        let cache = LazySchemaCache::with_defaults();
        let conn_id = Uuid::new_v4();

        // Load columns for "users" only
        cache.set_columns(conn_id, "users", vec![]);

        let tables = vec![
            "users".to_string(),
            "orders".to_string(),
            "products".to_string(),
        ];
        let needed = cache.tables_needing_columns(conn_id, &tables);

        assert_eq!(needed.len(), 2);
        assert!(needed.contains(&"orders".to_string()));
        assert!(needed.contains(&"products".to_string()));
        assert!(!needed.contains(&"users".to_string()));
    }
}

// ============ Cache Management Tests ============

mod management_tests {
    use super::*;

    fn make_table(name: &str) -> TableInfo {
        TableInfo {
            schema: None,
            name: name.to_string(),
            table_type: TableType::Table,
            owner: None,
            row_count: Some(100),
            size_bytes: None,
            comment: None,
            index_count: Some(2),
            trigger_count: Some(0),
            key_value_info: None,
        }
    }

    fn make_column(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            ordinal: 0,
            data_type: "text".to_string(),
            nullable: true,
            default_value: None,
            max_length: None,
            precision: None,
            scale: None,
            is_primary_key: false,
            is_auto_increment: false,
            is_unique: false,
            foreign_key: None,
            comment: None,
        }
    }

    #[test]
    fn test_invalidate_connection() {
        let cache = LazySchemaCache::with_defaults();
        let conn_id = Uuid::new_v4();

        cache.set_tables(conn_id, vec![make_table("users")]);
        cache.set_columns(conn_id, "users", vec![make_column("id")]);

        assert!(cache.get_tables(conn_id).is_some());
        assert!(cache.get_columns(conn_id, "users").is_some());

        cache.invalidate(conn_id);

        assert!(cache.get_tables(conn_id).is_none());
        assert!(cache.get_columns(conn_id, "users").is_none());
    }

    #[test]
    fn test_invalidate_specific_table() {
        let cache = LazySchemaCache::with_defaults();
        let conn_id = Uuid::new_v4();

        cache.set_tables(conn_id, vec![make_table("users"), make_table("orders")]);
        cache.set_columns(conn_id, "users", vec![make_column("id")]);
        cache.set_columns(conn_id, "orders", vec![make_column("id")]);

        // Invalidate only users table
        cache.invalidate_table(conn_id, "users");

        // Tables list still exists
        assert!(cache.get_tables(conn_id).is_some());

        // Users columns are gone
        assert!(cache.get_columns(conn_id, "users").is_none());

        // Orders columns still exist
        assert!(cache.get_columns(conn_id, "orders").is_some());
    }

    #[test]
    fn test_clear_all() {
        let cache = LazySchemaCache::with_defaults();
        let conn1 = Uuid::new_v4();
        let conn2 = Uuid::new_v4();

        cache.set_tables(conn1, vec![make_table("t1")]);
        cache.set_tables(conn2, vec![make_table("t2")]);

        cache.clear();

        assert!(cache.get_tables(conn1).is_none());
        assert!(cache.get_tables(conn2).is_none());
    }
}

// ============ Stats Tests ============

mod stats_tests {
    use super::*;

    fn make_table(name: &str) -> TableInfo {
        TableInfo {
            schema: None,
            name: name.to_string(),
            table_type: TableType::Table,
            owner: None,
            row_count: Some(100),
            size_bytes: None,
            comment: None,
            index_count: Some(2),
            trigger_count: Some(0),
            key_value_info: None,
        }
    }

    fn make_column(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            ordinal: 0,
            data_type: "text".to_string(),
            nullable: true,
            default_value: None,
            max_length: None,
            precision: None,
            scale: None,
            is_primary_key: false,
            is_auto_increment: false,
            is_unique: false,
            foreign_key: None,
            comment: None,
        }
    }

    #[test]
    fn test_stats_empty_cache() {
        let cache = LazySchemaCache::with_defaults();
        let stats = cache.stats();

        assert_eq!(stats.connection_count, 0);
        assert_eq!(stats.tables_loaded, 0);
        assert_eq!(stats.columns_loaded, 0);
        assert_eq!(stats.total_items(), 0);
    }

    #[test]
    fn test_stats_with_data() {
        let cache = LazySchemaCache::with_defaults();
        let conn1 = Uuid::new_v4();
        let conn2 = Uuid::new_v4();

        cache.set_tables(conn1, vec![make_table("t1")]);
        cache.set_columns(conn1, "t1", vec![make_column("c1")]);

        cache.set_tables(conn2, vec![make_table("t2")]);
        cache.set_columns(conn2, "t2", vec![make_column("c2")]);
        cache.set_columns(conn2, "t3", vec![make_column("c3")]);

        let stats = cache.stats();

        assert_eq!(stats.connection_count, 2);
        assert_eq!(stats.tables_loaded, 2);
        assert_eq!(stats.columns_loaded, 3);
        assert_eq!(stats.total_items(), 5);
    }
}

// ============ Thread Safety Tests ============

mod thread_safety_tests {
    use super::*;

    fn make_table(name: &str) -> TableInfo {
        TableInfo {
            schema: None,
            name: name.to_string(),
            table_type: TableType::Table,
            owner: None,
            row_count: Some(100),
            size_bytes: None,
            comment: None,
            index_count: Some(2),
            trigger_count: Some(0),
            key_value_info: None,
        }
    }

    #[test]
    fn test_concurrent_reads_and_writes() {
        let cache = Arc::new(LazySchemaCache::with_defaults());
        let conn_id = Uuid::new_v4();

        let mut handles = vec![];

        // Writers
        for i in 0..5 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                let tables = vec![make_table(&format!("table_{}", i))];
                cache_clone.set_tables(conn_id, tables);
            });
            handles.push(handle);
        }

        // Readers
        for _ in 0..5 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                let _ = cache_clone.get_tables(conn_id);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Cache should still be valid after concurrent access
        // (value depends on last write)
        let stats = cache.stats();
        assert_eq!(stats.connection_count, 1);
    }
}

// ============ SharedLazySchemaCache Tests ============

mod shared_cache_tests {
    use super::*;

    #[test]
    fn test_new_shared_cache() {
        let cache = new_shared_cache();
        let stats = cache.stats();
        assert_eq!(stats.connection_count, 0);
    }

    #[test]
    fn test_new_shared_cache_with_config() {
        let config = LazyCacheConfig::uniform(Duration::from_secs(60));
        let cache = new_shared_cache_with_config(config);
        assert_eq!(cache.config().list_ttl, Duration::from_secs(60));
    }
}
