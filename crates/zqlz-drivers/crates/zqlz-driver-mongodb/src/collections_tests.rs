//! Tests for MongoDB collection introspection module

use super::*;

mod database_info_tests {
    use super::*;

    #[test]
    fn test_database_info_creation() {
        let info = DatabaseInfo::new("mydb");
        assert_eq!(info.name, "mydb");
        assert!(info.size_bytes.is_none());
        assert!(!info.empty);
    }

    #[test]
    fn test_database_info_with_size() {
        let info = DatabaseInfo::new("mydb").with_size(1024);
        assert_eq!(info.size_bytes, Some(1024));
    }

    #[test]
    fn test_database_info_with_empty() {
        let info = DatabaseInfo::new("mydb").with_empty(true);
        assert!(info.empty);
    }

    #[test]
    fn test_database_info_serialization() {
        let info = DatabaseInfo::new("testdb")
            .with_size(2048)
            .with_empty(false);
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"name\":\"testdb\""));
        assert!(json.contains("\"size_bytes\":2048"));
        assert!(json.contains("\"empty\":false"));
    }
}

mod collection_info_tests {
    use super::*;

    #[test]
    fn test_collection_info_creation() {
        let info = CollectionInfo::new("users");
        assert_eq!(info.name, "users");
        assert_eq!(info.collection_type, CollectionType::Collection);
        assert!(!info.capped);
        assert!(info.document_count.is_none());
    }

    #[test]
    fn test_collection_info_with_type() {
        let info = CollectionInfo::new("user_summary").with_type(CollectionType::View);
        assert_eq!(info.collection_type, CollectionType::View);
    }

    #[test]
    fn test_collection_info_with_capped() {
        let info = CollectionInfo::new("logs").with_capped(true);
        assert!(info.capped);
    }

    #[test]
    fn test_collection_info_with_document_count() {
        let info = CollectionInfo::new("users").with_document_count(1000);
        assert_eq!(info.document_count, Some(1000));
    }

    #[test]
    fn test_collection_info_with_sizes() {
        let info = CollectionInfo::new("users").with_sizes(1024, 2048, 512);
        assert_eq!(info.size_bytes, Some(1024));
        assert_eq!(info.storage_size, Some(2048));
        assert_eq!(info.avg_doc_size, Some(512));
    }

    #[test]
    fn test_collection_info_with_index_count() {
        let info = CollectionInfo::new("users").with_index_count(3);
        assert_eq!(info.index_count, Some(3));
    }

    #[test]
    fn test_collection_info_serialization() {
        let info = CollectionInfo::new("orders")
            .with_type(CollectionType::Collection)
            .with_document_count(5000)
            .with_capped(false);
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"name\":\"orders\""));
        assert!(json.contains("\"collection_type\":\"collection\""));
        assert!(json.contains("\"document_count\":5000"));
    }
}

mod collection_type_tests {
    use super::*;

    #[test]
    fn test_collection_type_from_string() {
        assert_eq!(
            CollectionType::from_string("collection"),
            CollectionType::Collection
        );
        assert_eq!(CollectionType::from_string("view"), CollectionType::View);
        assert_eq!(
            CollectionType::from_string("timeseries"),
            CollectionType::TimeSeries
        );
        assert_eq!(
            CollectionType::from_string("system"),
            CollectionType::System
        );
        assert_eq!(
            CollectionType::from_string("unknown"),
            CollectionType::Collection
        );
    }

    #[test]
    fn test_collection_type_as_str() {
        assert_eq!(CollectionType::Collection.as_str(), "collection");
        assert_eq!(CollectionType::View.as_str(), "view");
        assert_eq!(CollectionType::TimeSeries.as_str(), "timeseries");
        assert_eq!(CollectionType::System.as_str(), "system");
    }

    #[test]
    fn test_collection_type_is_collection() {
        assert!(CollectionType::Collection.is_collection());
        assert!(!CollectionType::View.is_collection());
        assert!(!CollectionType::TimeSeries.is_collection());
        assert!(!CollectionType::System.is_collection());
    }

    #[test]
    fn test_collection_type_is_view() {
        assert!(CollectionType::View.is_view());
        assert!(!CollectionType::Collection.is_view());
        assert!(!CollectionType::TimeSeries.is_view());
    }

    #[test]
    fn test_collection_type_display() {
        assert_eq!(format!("{}", CollectionType::Collection), "collection");
        assert_eq!(format!("{}", CollectionType::View), "view");
    }

    #[test]
    fn test_collection_type_serialization() {
        let json = serde_json::to_string(&CollectionType::View).unwrap();
        assert_eq!(json, "\"view\"");

        let deserialized: CollectionType = serde_json::from_str("\"timeseries\"").unwrap();
        assert_eq!(deserialized, CollectionType::TimeSeries);
    }
}

mod index_info_tests {
    use super::*;

    #[test]
    fn test_index_info_creation() {
        let info = IndexInfo::new("idx_email");
        assert_eq!(info.name, "idx_email");
        assert!(info.keys.is_empty());
        assert!(!info.unique);
        assert!(!info.sparse);
        assert!(info.expire_after_seconds.is_none());
    }

    #[test]
    fn test_index_info_with_key() {
        let info = IndexInfo::new("idx_email").with_key("email", IndexDirection::Ascending);
        assert_eq!(info.keys.get("email"), Some(&IndexDirection::Ascending));
    }

    #[test]
    fn test_index_info_with_multiple_keys() {
        let info = IndexInfo::new("idx_compound")
            .with_key("last_name", IndexDirection::Ascending)
            .with_key("first_name", IndexDirection::Ascending);
        assert_eq!(info.keys.len(), 2);
        assert_eq!(info.keys.get("last_name"), Some(&IndexDirection::Ascending));
        assert_eq!(
            info.keys.get("first_name"),
            Some(&IndexDirection::Ascending)
        );
    }

    #[test]
    fn test_index_info_with_unique() {
        let info = IndexInfo::new("idx_email").with_unique(true);
        assert!(info.unique);
    }

    #[test]
    fn test_index_info_with_sparse() {
        let info = IndexInfo::new("idx_optional").with_sparse(true);
        assert!(info.sparse);
    }

    #[test]
    fn test_index_info_with_ttl() {
        let info = IndexInfo::new("idx_session").with_ttl(3600);
        assert_eq!(info.expire_after_seconds, Some(3600));
    }

    #[test]
    fn test_index_info_is_primary() {
        let primary = IndexInfo::new("_id_");
        assert!(primary.is_primary());

        let secondary = IndexInfo::new("idx_email");
        assert!(!secondary.is_primary());
    }

    #[test]
    fn test_index_info_serialization() {
        let info = IndexInfo::new("idx_email")
            .with_key("email", IndexDirection::Ascending)
            .with_unique(true);
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"name\":\"idx_email\""));
        assert!(json.contains("\"unique\":true"));
    }
}

mod index_direction_tests {
    use super::*;

    #[test]
    fn test_index_direction_from_bson_value_positive() {
        let dir = IndexDirection::from_bson_value(&zqlz_core::Value::Int32(1));
        assert_eq!(dir, IndexDirection::Ascending);
    }

    #[test]
    fn test_index_direction_from_bson_value_negative() {
        let dir = IndexDirection::from_bson_value(&zqlz_core::Value::Int32(-1));
        assert_eq!(dir, IndexDirection::Descending);
    }

    #[test]
    fn test_index_direction_from_bson_value_text() {
        let dir = IndexDirection::from_bson_value(&zqlz_core::Value::String("text".to_string()));
        assert_eq!(dir, IndexDirection::Text);
    }

    #[test]
    fn test_index_direction_from_bson_value_geo() {
        let dir =
            IndexDirection::from_bson_value(&zqlz_core::Value::String("2dsphere".to_string()));
        assert_eq!(dir, IndexDirection::Geo2dsphere);
    }

    #[test]
    fn test_index_direction_from_bson_value_hashed() {
        let dir = IndexDirection::from_bson_value(&zqlz_core::Value::String("hashed".to_string()));
        assert_eq!(dir, IndexDirection::Hashed);
    }

    #[test]
    fn test_index_direction_as_str() {
        assert_eq!(IndexDirection::Ascending.as_str(), "1");
        assert_eq!(IndexDirection::Descending.as_str(), "-1");
        assert_eq!(IndexDirection::Text.as_str(), "text");
        assert_eq!(IndexDirection::Geo2dsphere.as_str(), "2dsphere");
        assert_eq!(IndexDirection::Hashed.as_str(), "hashed");
    }

    #[test]
    fn test_index_direction_display() {
        assert_eq!(format!("{}", IndexDirection::Ascending), "1");
        assert_eq!(format!("{}", IndexDirection::Descending), "-1");
    }
}

mod inferred_field_tests {
    use super::*;

    #[test]
    fn test_inferred_field_creation() {
        let field = InferredField::new("email");
        assert_eq!(field.name, "email");
        assert!(field.types.is_empty());
        assert_eq!(field.occurrence_count, 0);
        assert_eq!(field.occurrence_percentage, 0.0);
        assert!(!field.is_required);
    }

    #[test]
    fn test_inferred_field_with_type() {
        let field = InferredField::new("email").with_type("String");
        assert_eq!(field.types, vec!["String"]);
    }

    #[test]
    fn test_inferred_field_with_multiple_types() {
        let field = InferredField::new("value")
            .with_type("String")
            .with_type("Int64")
            .with_type("String"); // duplicate should not be added
        assert_eq!(field.types, vec!["String", "Int64"]);
    }

    #[test]
    fn test_inferred_field_with_occurrence_required() {
        let field = InferredField::new("_id").with_occurrence(100, 100);
        assert_eq!(field.occurrence_count, 100);
        assert_eq!(field.occurrence_percentage, 100.0);
        assert!(field.is_required);
    }

    #[test]
    fn test_inferred_field_with_occurrence_optional() {
        let field = InferredField::new("middle_name").with_occurrence(50, 100);
        assert_eq!(field.occurrence_count, 50);
        assert_eq!(field.occurrence_percentage, 50.0);
        assert!(!field.is_required);
    }

    #[test]
    fn test_inferred_field_with_occurrence_zero_total() {
        let field = InferredField::new("test").with_occurrence(0, 0);
        assert_eq!(field.occurrence_percentage, 0.0);
        assert!(!field.is_required);
    }

    #[test]
    fn test_inferred_field_serialization() {
        let field = InferredField::new("email")
            .with_type("String")
            .with_occurrence(95, 100);
        let json = serde_json::to_string(&field).unwrap();
        assert!(json.contains("\"name\":\"email\""));
        assert!(json.contains("\"types\":[\"String\"]"));
        assert!(json.contains("\"occurrence_count\":95"));
    }
}

mod list_collections_options_tests {
    use super::*;

    #[test]
    fn test_list_collections_options_default() {
        let options = ListCollectionsOptions::new();
        assert!(!options.include_system);
        assert!(options.include_views);
        assert!(options.name_filter.is_none());
        assert!(!options.include_stats);
    }

    #[test]
    fn test_list_collections_options_with_system() {
        let options = ListCollectionsOptions::new().with_system();
        assert!(options.include_system);
    }

    #[test]
    fn test_list_collections_options_without_views() {
        let options = ListCollectionsOptions::new().without_views();
        assert!(!options.include_views);
    }

    #[test]
    fn test_list_collections_options_with_filter() {
        let options = ListCollectionsOptions::new().with_filter("user*");
        assert_eq!(options.name_filter, Some("user*".to_string()));
    }

    #[test]
    fn test_list_collections_options_with_stats() {
        let options = ListCollectionsOptions::new().with_stats();
        assert!(options.include_stats);
    }

    #[test]
    fn test_list_collections_options_chained() {
        let options = ListCollectionsOptions::new()
            .with_system()
            .without_views()
            .with_filter("logs_*")
            .with_stats();
        assert!(options.include_system);
        assert!(!options.include_views);
        assert_eq!(options.name_filter, Some("logs_*".to_string()));
        assert!(options.include_stats);
    }
}

mod name_pattern_tests {
    use super::*;

    #[test]
    fn test_name_matches_pattern_exact() {
        assert!(name_matches_pattern("users", "users"));
        assert!(!name_matches_pattern("users", "orders"));
    }

    #[test]
    fn test_name_matches_pattern_wildcard_all() {
        assert!(name_matches_pattern("users", "*"));
        assert!(name_matches_pattern("anything", "*"));
    }

    #[test]
    fn test_name_matches_pattern_prefix_wildcard() {
        assert!(name_matches_pattern("user_sessions", "user_*"));
        assert!(name_matches_pattern("user_logs", "user_*"));
        assert!(!name_matches_pattern("order_items", "user_*"));
    }

    #[test]
    fn test_name_matches_pattern_suffix_wildcard() {
        assert!(name_matches_pattern("user_logs", "*_logs"));
        assert!(name_matches_pattern("system_logs", "*_logs"));
        assert!(!name_matches_pattern("user_sessions", "*_logs"));
    }

    #[test]
    fn test_name_matches_pattern_prefix_and_suffix() {
        assert!(name_matches_pattern("user_activity_logs", "user_*_logs"));
        assert!(!name_matches_pattern("user_logs", "user_*_logs")); // needs something between
    }
}

mod json_type_name_tests {
    use super::*;

    #[test]
    fn test_json_type_name_null() {
        let result = json_type_name(&serde_json::Value::Null);
        assert_eq!(result, "Null");
    }

    #[test]
    fn test_json_type_name_boolean() {
        let result = json_type_name(&serde_json::json!(true));
        assert_eq!(result, "Boolean");
    }

    #[test]
    fn test_json_type_name_integer() {
        let result = json_type_name(&serde_json::json!(42));
        assert_eq!(result, "Int64");
    }

    #[test]
    fn test_json_type_name_float() {
        let result = json_type_name(&serde_json::json!(3.14));
        assert_eq!(result, "Double");
    }

    #[test]
    fn test_json_type_name_string() {
        let result = json_type_name(&serde_json::json!("hello"));
        assert_eq!(result, "String");
    }

    #[test]
    fn test_json_type_name_object_id() {
        let result = json_type_name(&serde_json::json!("507f1f77bcf86cd799439011"));
        assert_eq!(result, "ObjectId");
    }

    #[test]
    fn test_json_type_name_array() {
        let result = json_type_name(&serde_json::json!([1, 2, 3]));
        assert_eq!(result, "Array");
    }

    #[test]
    fn test_json_type_name_object() {
        let result = json_type_name(&serde_json::json!({"key": "value"}));
        assert_eq!(result, "Object");
    }

    #[test]
    fn test_json_type_name_extended_json_oid() {
        let result = json_type_name(&serde_json::json!({"$oid": "507f1f77bcf86cd799439011"}));
        assert_eq!(result, "ObjectId");
    }

    #[test]
    fn test_json_type_name_extended_json_date() {
        let result = json_type_name(&serde_json::json!({"$date": "2021-01-01T00:00:00Z"}));
        assert_eq!(result, "Date");
    }

    #[test]
    fn test_json_type_name_extended_json_binary() {
        let result =
            json_type_name(&serde_json::json!({"$binary": {"base64": "...", "subType": "00"}}));
        assert_eq!(result, "BinData");
    }

    #[test]
    fn test_json_type_name_extended_json_decimal() {
        let result = json_type_name(&serde_json::json!({"$numberDecimal": "123.45"}));
        assert_eq!(result, "Decimal128");
    }
}

mod collect_field_types_tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_collect_field_types_simple() {
        let doc = serde_json::json!({
            "name": "John",
            "age": 30
        });

        let mut stats = HashMap::new();
        collect_field_types(&doc, "", &mut stats);

        assert_eq!(stats.len(), 2);
        assert!(stats.contains_key("name"));
        assert!(stats.contains_key("age"));
    }

    #[test]
    fn test_collect_field_types_nested() {
        let doc = serde_json::json!({
            "user": {
                "name": "John",
                "email": "john@example.com"
            }
        });

        let mut stats = HashMap::new();
        collect_field_types(&doc, "", &mut stats);

        assert!(stats.contains_key("user"));
        assert!(stats.contains_key("user.name"));
        assert!(stats.contains_key("user.email"));
    }

    #[test]
    fn test_collect_field_types_multiple_docs() {
        let doc1 = serde_json::json!({ "name": "John", "age": 30 });
        let doc2 = serde_json::json!({ "name": "Jane", "email": "jane@example.com" });

        let mut stats = HashMap::new();
        collect_field_types(&doc1, "", &mut stats);
        collect_field_types(&doc2, "", &mut stats);

        // name appears in both
        assert_eq!(stats.get("name").unwrap().1, 2);
        // age appears in one
        assert_eq!(stats.get("age").unwrap().1, 1);
        // email appears in one
        assert_eq!(stats.get("email").unwrap().1, 1);
    }
}

mod parse_collection_info_tests {
    use super::*;

    #[test]
    fn test_parse_collection_info_basic() {
        let value = serde_json::json!({
            "name": "users",
            "type": "collection"
        });
        let options = ListCollectionsOptions::new();
        let info = parse_collection_info(&value, &options).unwrap();

        assert_eq!(info.name, "users");
        assert_eq!(info.collection_type, CollectionType::Collection);
        assert!(!info.capped);
    }

    #[test]
    fn test_parse_collection_info_view() {
        let value = serde_json::json!({
            "name": "user_summary",
            "type": "view"
        });
        let options = ListCollectionsOptions::new();
        let info = parse_collection_info(&value, &options).unwrap();

        assert_eq!(info.collection_type, CollectionType::View);
    }

    #[test]
    fn test_parse_collection_info_capped() {
        let value = serde_json::json!({
            "name": "logs",
            "type": "collection",
            "options": { "capped": true }
        });
        let options = ListCollectionsOptions::new();
        let info = parse_collection_info(&value, &options).unwrap();

        assert!(info.capped);
    }

    #[test]
    fn test_parse_collection_info_filters_system() {
        let value = serde_json::json!({
            "name": "system.indexes",
            "type": "collection"
        });
        let options = ListCollectionsOptions::new(); // excludes system by default
        let result = parse_collection_info(&value, &options);

        assert!(result.is_none());
    }

    #[test]
    fn test_parse_collection_info_includes_system() {
        let value = serde_json::json!({
            "name": "system.indexes",
            "type": "collection"
        });
        let options = ListCollectionsOptions::new().with_system();
        let result = parse_collection_info(&value, &options);

        assert!(result.is_some());
    }

    #[test]
    fn test_parse_collection_info_filters_views() {
        let value = serde_json::json!({
            "name": "user_summary",
            "type": "view"
        });
        let options = ListCollectionsOptions::new().without_views();
        let result = parse_collection_info(&value, &options);

        assert!(result.is_none());
    }

    #[test]
    fn test_parse_collection_info_name_filter() {
        let value = serde_json::json!({
            "name": "user_logs",
            "type": "collection"
        });

        let options_match = ListCollectionsOptions::new().with_filter("user_*");
        assert!(parse_collection_info(&value, &options_match).is_some());

        let options_no_match = ListCollectionsOptions::new().with_filter("order_*");
        assert!(parse_collection_info(&value, &options_no_match).is_none());
    }
}

mod parse_index_info_tests {
    use super::*;

    #[test]
    fn test_parse_index_info_simple() {
        let value = serde_json::json!({
            "name": "idx_email",
            "key": { "email": 1 }
        });
        let info = parse_index_info(&value).unwrap();

        assert_eq!(info.name, "idx_email");
        assert_eq!(info.keys.get("email"), Some(&IndexDirection::Ascending));
    }

    #[test]
    fn test_parse_index_info_descending() {
        let value = serde_json::json!({
            "name": "idx_created_at",
            "key": { "created_at": -1 }
        });
        let info = parse_index_info(&value).unwrap();

        assert_eq!(
            info.keys.get("created_at"),
            Some(&IndexDirection::Descending)
        );
    }

    #[test]
    fn test_parse_index_info_unique() {
        let value = serde_json::json!({
            "name": "idx_email",
            "key": { "email": 1 },
            "unique": true
        });
        let info = parse_index_info(&value).unwrap();

        assert!(info.unique);
    }

    #[test]
    fn test_parse_index_info_sparse() {
        let value = serde_json::json!({
            "name": "idx_optional",
            "key": { "optional_field": 1 },
            "sparse": true
        });
        let info = parse_index_info(&value).unwrap();

        assert!(info.sparse);
    }

    #[test]
    fn test_parse_index_info_ttl() {
        let value = serde_json::json!({
            "name": "idx_session",
            "key": { "created_at": 1 },
            "expireAfterSeconds": 3600
        });
        let info = parse_index_info(&value).unwrap();

        assert_eq!(info.expire_after_seconds, Some(3600));
    }

    #[test]
    fn test_parse_index_info_text() {
        let value = serde_json::json!({
            "name": "idx_content",
            "key": { "content": "text" }
        });
        let info = parse_index_info(&value).unwrap();

        assert!(info.is_text);
        assert_eq!(info.keys.get("content"), Some(&IndexDirection::Text));
    }

    #[test]
    fn test_parse_index_info_geo() {
        let value = serde_json::json!({
            "name": "idx_location",
            "key": { "location": "2dsphere" }
        });
        let info = parse_index_info(&value).unwrap();

        assert!(info.is_geo);
        assert_eq!(
            info.keys.get("location"),
            Some(&IndexDirection::Geo2dsphere)
        );
    }

    #[test]
    fn test_parse_index_info_compound() {
        let value = serde_json::json!({
            "name": "idx_compound",
            "key": { "last_name": 1, "first_name": 1 }
        });
        let info = parse_index_info(&value).unwrap();

        assert_eq!(info.keys.len(), 2);
    }
}
