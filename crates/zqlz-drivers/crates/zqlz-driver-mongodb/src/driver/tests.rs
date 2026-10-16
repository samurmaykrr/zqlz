use super::*;

#[cfg(test)]
mod ping_tests {
    use super::*;

    #[test]
    fn ping_statement_parses_as_a_runnable_command_document() {
        let command: Document = serde_json::from_str(MONGODB_PING_COMMAND)
            .expect("heartbeat ping must be a document the driver can run");
        assert!(command.contains_key("ping"));
    }
}

#[cfg(test)]
mod option_tests {
    use super::*;
    use std::collections::BTreeMap;
    use zqlz_core::{
        DatabaseDriver, HighlightQueryLanguage, ParameterPlaceholderCapability, TreeSitterGrammar,
        syntax_driver_capabilities_from_bundle,
    };

    #[test]
    fn mongodb_dialect_bundle_owns_editor_syntax_capabilities() {
        let driver = MongoDbDriver::new();
        let bundle = driver
            .dialect_bundle()
            .expect("MongoDB driver should expose dialect bundle");
        let capabilities = syntax_driver_capabilities_from_bundle(bundle);

        assert_eq!(capabilities.profile, "mongodb");
        assert_eq!(
            capabilities.tree_sitter_grammar,
            TreeSitterGrammar::Javascript
        );
        assert_eq!(
            capabilities.highlight_query_language,
            HighlightQueryLanguage::MongoDb
        );
        assert_eq!(
            capabilities.parameter_placeholders,
            ParameterPlaceholderCapability::disabled()
        );
        assert!(capabilities.document_syntax);
        assert!(!capabilities.command_syntax);
        assert!(!capabilities.sql_overlays);
    }

    #[test]
    fn mongodb_schema_exposes_common_options() {
        let schema = MongoDbDriver::new().connection_field_schema();
        let field = |id: &str| schema.fields.iter().find(|field| field.id == id).unwrap();

        assert_eq!(field("scheme").default_value.as_deref(), Some("mongodb"));
        assert_eq!(field("replicaSet").tab.as_deref(), Some("advanced"));
        assert_eq!(field("readPreference").default_value.as_deref(), Some(""));
        assert_eq!(
            field("directConnection").default_value.as_deref(),
            Some("false")
        );
        assert_eq!(
            field("serverSelectionTimeoutMS").tab.as_deref(),
            Some("advanced")
        );
        assert_eq!(field("connectTimeoutMS").tab.as_deref(), Some("advanced"));
    }

    #[test]
    fn mongodb_connection_string_supports_srv_and_options() {
        let driver = MongoDbDriver::new();
        let mut config = ConnectionConfig::new("mongodb", "test");
        config.host = "cluster.example.com".to_string();
        config.database = Some("app".to_string());
        config
            .params
            .insert("scheme".to_string(), "mongodb+srv".to_string());
        config
            .params
            .insert("replicaSet".to_string(), "rs0".to_string());
        config.params.insert(
            "readPreference".to_string(),
            "secondaryPreferred".to_string(),
        );
        config
            .params
            .insert("directConnection".to_string(), "true".to_string());
        config
            .params
            .insert("serverSelectionTimeoutMS".to_string(), "5000".to_string());
        config
            .params
            .insert("connectTimeoutMS".to_string(), "3000".to_string());

        let connection_string = driver.build_connection_string(&config);

        assert!(connection_string.starts_with("mongodb+srv://cluster.example.com/app?"));
        assert!(!connection_string.contains(":27017"));
        assert!(connection_string.contains("replicaSet=rs0"));
        assert!(connection_string.contains("readPreference=secondaryPreferred"));
        assert!(connection_string.contains("directConnection=true"));
        assert!(connection_string.contains("serverSelectionTimeoutMS=5000"));
        assert!(connection_string.contains("connectTimeoutMS=3000"));
    }

    #[test]
    fn parse_id_json_recovers_object_id_from_viewer_hex_string() {
        let id = "507f1f77bcf86cd799439011";

        assert!(matches!(
            MongoDbConnection::parse_id_json(id),
            Ok(Bson::ObjectId(object_id)) if object_id.to_hex() == id
        ));
        assert!(matches!(
            MongoDbConnection::parse_id_json("\"507f1f77bcf86cd799439011\""),
            Ok(Bson::ObjectId(object_id)) if object_id.to_hex() == id
        ));
    }

    #[test]
    fn document_cell_update_value_uses_logical_value_json() {
        assert_eq!(
            bson::to_bson(&Value::Json(serde_json::json!({ "nested": true })).to_json_value())
                .expect("json to bson"),
            Bson::Document(bson::doc! { "nested": true })
        );
        assert_eq!(
            bson::to_bson(&Value::Int64(42).to_json_value()).expect("int to bson"),
            Bson::Int64(42)
        );
    }

    #[test]
    fn parse_required_document_json_preserves_extended_object_id() {
        let id = "507f1f77bcf86cd799439011";
        let document = MongoDbConnection::parse_required_document_json(
            &serde_json::json!({ "warehouseId": { "$oid": id } }).to_string(),
            "document",
        )
        .expect("document json should parse");

        assert!(matches!(
            document.get("warehouseId"),
            Some(Bson::ObjectId(object_id)) if object_id.to_hex() == id
        ));
    }

    #[test]
    fn mongodb_validation_summary_extracts_field_reasons() {
        let details = bson::doc! {
            "details": {
                "schemaRulesNotSatisfied": [
                    {
                        "operatorName": "properties",
                        "propertiesNotSatisfied": [
                            {
                                "propertyName": "warehouseId",
                                "details": [
                                    {
                                        "operatorName": "bsonType",
                                        "specifiedAs": { "bsonType": "objectId" },
                                        "reason": "type did not match",
                                        "consideredType": "string"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "operatorName": "required",
                        "missingProperties": ["updatedAt"]
                    }
                ]
            }
        };

        let summary = MongoDbConnection::mongodb_validation_summary(
            "Document failed validation",
            Some(&details),
        );

        assert!(summary.contains("warehouseId expected objectId but got string"));
        assert!(summary.contains("missing required fields: updatedAt"));
    }

    #[test]
    fn index_kind_detects_mongodb_special_index_families() {
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "email": "hashed" }),
            "hashed"
        );
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "body": "text", "location": "2dsphere" }),
            "geo+text"
        );
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "$**": 1 }),
            "wildcard"
        );
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "payload.$**": 1 }),
            "wildcard"
        );
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "$**": "columnstore" }),
            "columnstore"
        );
        assert_eq!(
            MongoDbConnection::index_kind(&bson::doc! { "tenantId": 1, "updatedAt": -1 }),
            "btree"
        );
    }

    #[test]
    fn atlas_search_unavailable_state_populates_search_and_vector_sections() {
        let (search, vector) = MongoDbConnection::search_index_unavailable_rows(
            "zqlz_feature_lab",
            "$listSearchIndexes is not supported".to_string(),
        );

        assert_eq!(search.kind, "search_index");
        assert_eq!(search.name, "$listSearchIndexes");
        assert_eq!(
            search.unavailable_reason.as_deref(),
            Some("$listSearchIndexes is not supported")
        );
        assert_eq!(vector.kind, "vector_search_index");
        assert_eq!(vector.name, "$listSearchIndexes vector");
        assert_eq!(
            vector.unavailable_reason.as_deref(),
            Some("$listSearchIndexes is not supported")
        );
    }

    #[test]
    fn mongodb_command_target_database_uses_optional_db_field() {
        let mut command = bson::doc! { "$db": "config", "find": "collections" };

        assert_eq!(
            MongoDbConnection::command_target_database("app", &mut command),
            "config"
        );
        assert!(!command.contains_key("$db"));

        let mut command = bson::doc! { "buildInfo": 1 };
        assert_eq!(
            MongoDbConnection::command_target_database("app", &mut command),
            "app"
        );
    }

    #[test]
    fn mongodb_shell_find_converts_to_command_document() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.products.find({ $text: { $search: "developer iot" } }, { sku: 1, name: 1 }).sort({ score: { $meta: "textScore" } }).limit(5)"#,
        )
        .unwrap()
        .expect("shell query command");

        assert_eq!(command.get_str("find").unwrap(), "products");
        assert!(
            command
                .get_document("filter")
                .unwrap()
                .contains_key("$text")
        );
        assert!(
            command
                .get_document("projection")
                .unwrap()
                .contains_key("sku")
        );
        assert_eq!(command.get_i64("limit").unwrap(), 5);
        assert!(command.get_document("sort").unwrap().contains_key("score"));
    }

    #[test]
    fn mongodb_shell_find_accepts_object_id_calls() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.orders.find({ customerId: ObjectId("662000000000000000000001"), note: "ObjectId(x)" })"#,
        )
        .unwrap()
        .expect("shell query command");

        let filter = command.get_document("filter").unwrap();
        assert_eq!(
            filter.get_object_id("customerId").unwrap().to_hex(),
            "662000000000000000000001"
        );
        assert_eq!(filter.get_str("note").unwrap(), "ObjectId(x)");
    }

    #[test]
    fn mongodb_shell_find_explain_wraps_cursor_command() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.products.find({ $text: { $search: "developer iot" } }, { sku: 1, name: 1 }).sort({ score: { $meta: "textScore" } }).limit(5).explain("executionStats")"#,
        )
        .unwrap()
        .expect("shell explain command");

        assert_eq!(command.get_str("verbosity").unwrap(), "executionStats");
        let explained = command.get_document("explain").unwrap();
        assert_eq!(explained.get_str("find").unwrap(), "products");
        assert!(
            explained
                .get_document("filter")
                .unwrap()
                .contains_key("$text")
        );
        assert!(
            explained
                .get_document("projection")
                .unwrap()
                .contains_key("sku")
        );
        assert!(
            explained
                .get_document("sort")
                .unwrap()
                .contains_key("score")
        );
        assert_eq!(explained.get_i64("limit").unwrap(), 5);
    }

    #[test]
    fn mongodb_shell_aggregate_converts_to_command_document() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.orders.aggregate([{ $match: { status: { $in: ["paid", "shipped"] } } }, { $sort: { orderTotal: -1 } }])"#,
        )
        .unwrap()
        .expect("shell aggregate command");

        assert_eq!(command.get_str("aggregate").unwrap(), "orders");
        assert_eq!(command.get_array("pipeline").unwrap().len(), 2);
        assert!(command.get_document("cursor").is_ok());
    }

    #[test]
    fn mongodb_shell_aggregate_explain_wraps_command() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.orders.aggregate([{ $match: { status: "paid" } }]).explain()"#,
        )
        .unwrap()
        .expect("shell aggregate explain command");

        assert_eq!(command.get_str("verbosity").unwrap(), "queryPlanner");
        let explained = command.get_document("explain").unwrap();
        assert_eq!(explained.get_str("aggregate").unwrap(), "orders");
        assert_eq!(explained.get_array("pipeline").unwrap().len(), 1);
    }

    #[test]
    fn mongodb_shell_get_collection_supports_quoted_collection_names() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.getCollection("weird names.select").find({ "emoji_😀": { $in: ["雪", "こんにちは"] } })"#,
        )
        .unwrap()
        .expect("shell getCollection command");

        assert_eq!(command.get_str("find").unwrap(), "weird names.select");
        assert!(
            command
                .get_document("filter")
                .unwrap()
                .contains_key("emoji_😀")
        );
    }

    #[test]
    fn mongodb_shell_get_sibling_db_targets_database() {
        let command = MongoDbConnection::parse_shell_query(
            r#"db.getSiblingDB("zqlz_feature_lab").orders.aggregate([{ $match: { status: "paid" } }])"#,
        )
        .unwrap()
        .expect("shell getSiblingDB command");

        assert_eq!(command.get_str("$db").unwrap(), "zqlz_feature_lab");
        assert_eq!(command.get_str("aggregate").unwrap(), "orders");
    }

    #[test]
    fn sharding_metadata_helpers_handle_config_database_shapes() {
        assert_eq!(
            MongoDbConnection::regex_escape("tenant.prod"),
            "tenant\\.prod"
        );
        assert_eq!(
            MongoDbConnection::sharded_collection_name(
                &bson::doc! { "_id": "zqlz_feature_lab.orders", "key": { "tenantId": 1 } },
                0,
            ),
            "zqlz_feature_lab.orders"
        );
        assert_eq!(
            MongoDbConnection::sharding_chunk_name(
                &bson::doc! { "_id": "chunk-a", "ns": "zqlz_feature_lab.orders", "min": { "tenantId": 1 } },
                None,
                3,
            ),
            "zqlz_feature_lab.orders chunk 3"
        );
        assert_eq!(
            MongoDbConnection::sharding_chunk_name(
                &bson::doc! { "_id": bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").expect("valid object id"), "uuid": bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: vec![1, 2, 3] } },
                Some("zqlz_feature_lab.orders"),
                4,
            ),
            "zqlz_feature_lab.orders chunk 4"
        );
        assert_eq!(
            MongoDbConnection::sharding_zone_name(
                &bson::doc! { "ns": "zqlz_feature_lab.orders", "tag": "east" },
                0,
            ),
            "zqlz_feature_lab.orders zone east"
        );
    }

    #[test]
    fn gridfs_bucket_detection_requires_matching_files_and_chunks_collections() {
        fn collection(name: &str, count: Option<u64>, size: Option<u64>) -> DocumentCollectionInfo {
            DocumentCollectionInfo {
                database: "zqlz_feature_lab".to_string(),
                name: name.to_string(),
                collection_type: "collection".to_string(),
                document_count: count,
                size_bytes: size,
                index_count: None,
                options_json: None,
                validator_json: None,
                collation_json: None,
                view_on: None,
                pipeline_json: None,
                timeseries_json: None,
                clustered_index_json: None,
                change_stream_pre_and_post_images: None,
            }
        }

        let buckets = MongoDbConnection::gridfs_buckets_from_collections(
            "zqlz_feature_lab",
            &[
                collection("media.files", Some(2), Some(512)),
                collection("media.chunks", Some(3), Some(2048)),
                collection("orphan.files", Some(1), Some(256)),
            ],
        );

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].name, "media");
        assert_eq!(buckets[0].files_collection, "media.files");
        assert_eq!(buckets[0].chunks_collection, "media.chunks");
        assert_eq!(buckets[0].file_count, Some(2));
        assert_eq!(buckets[0].size_bytes, Some(512));
    }

    #[test]
    fn mongodb_query_result_infers_json_editor_types_for_nested_fields() {
        let result = MongoDbConnection::documents_to_query_result(
            vec![bson::doc! {
                "_id": bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").expect("valid object id"),
                "title": "Guide",
                "payload": { "kind": "guide", "stages": ["group"] },
                "tags": ["aggregation", "orders"],
                "views": 12_i32,
            }],
            0,
        );

        let data_type = |name: &str| {
            result
                .columns
                .iter()
                .find(|column| column.name == name)
                .map(|column| column.data_type.as_str())
        };

        assert_eq!(data_type("_id"), Some("objectId"));
        assert_eq!(data_type("title"), Some("string"));
        assert_eq!(data_type("payload"), Some("json"));
        assert_eq!(data_type("tags"), Some("json"));
        assert_eq!(data_type("views"), Some("int"));
    }

    #[test]
    fn server_metadata_commands_cover_storage_and_read_write_concern() {
        let command_names = MongoDbConnection::server_metadata_commands()
            .into_iter()
            .filter_map(|command| command.keys().next().cloned())
            .collect::<Vec<_>>();

        assert_eq!(
            command_names,
            vec![
                "buildInfo",
                "getParameter",
                "serverStatus",
                "getDefaultRWConcern",
                "replSetGetStatus",
            ]
        );
    }

    #[tokio::test]
    async fn mongodb_object_form_generates_create_index_command() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "collection".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers_email_unique".to_string()),
        );
        values.insert(
            "keys".to_string(),
            ObjectFormValue::String(r#"{"tenantId": 1, "email": 1}"#.to_string()),
        );
        values.insert("unique".to_string(), ObjectFormValue::Bool(true));

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_index".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"createIndexes\": \"customers\""));
        assert!(ddl[0].contains("\"name\": \"customers_email_unique\""));
        assert!(ddl[0].contains("\"unique\": true"));
    }

    #[tokio::test]
    async fn mongodb_object_form_merges_additional_index_options() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "collection".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers_wildcard_idx".to_string()),
        );
        values.insert(
            "keys".to_string(),
            ObjectFormValue::String(r#"{"$**": 1}"#.to_string()),
        );
        values.insert(
            "additional_options".to_string(),
            ObjectFormValue::String(
                r#"{"wildcardProjection":{"profile":1},"hidden":true}"#.to_string(),
            ),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_index".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"wildcardProjection\": {"));
        assert!(ddl[0].contains("\"profile\": 1"));
        assert!(ddl[0].contains("\"hidden\": true"));
    }

    #[tokio::test]
    async fn mongodb_object_form_preserves_view_collation() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("activeCustomers".to_string()),
        );
        values.insert(
            "view_on".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "pipeline".to_string(),
            ObjectFormValue::String(r#"[{"$match":{"active":true}}]"#.to_string()),
        );
        values.insert(
            "collation".to_string(),
            ObjectFormValue::String(r#"{"locale":"en","strength":2}"#.to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_view".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"create\": \"activeCustomers\""));
        assert!(ddl[0].contains("\"viewOn\": \"customers\""));
        assert!(ddl[0].contains("\"collation\": {"));
        assert!(ddl[0].contains("\"strength\": 2"));
    }

    #[tokio::test]
    async fn mongodb_object_form_does_not_emit_unsupported_collection_collation_edit() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "collation".to_string(),
            ObjectFormValue::String(r#"{"locale":"en","strength":2}"#.to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"collMod\": \"customers\""));
        assert!(!ddl[0].contains("\"collation\""));
    }

    #[tokio::test]
    async fn mongodb_collection_edit_form_makes_validation_changes_opt_in() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );

        let spec = connection
            .object_form_spec(&ObjectFormSpecRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
            })
            .await
            .unwrap()
            .expect("collection edit spec");
        let fields = &spec.sections[0].fields;
        let validation_level = fields
            .iter()
            .find(|field| field.id == "validation_level")
            .expect("validation level field");
        let validation_action = fields
            .iter()
            .find(|field| field.id == "validation_action")
            .expect("validation action field");

        assert_eq!(
            validation_level.default_value,
            ObjectFormValue::String(String::new())
        );
        assert_eq!(
            validation_action.default_value,
            ObjectFormValue::String(String::new())
        );

        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(!ddl[0].contains("\"validationLevel\""));
        assert!(!ddl[0].contains("\"validationAction\""));
    }

    #[tokio::test]
    async fn mongodb_object_form_only_changes_pre_post_images_when_requested() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(!ddl[0].contains("changeStreamPreAndPostImages"));

        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "apply_change_stream_pre_post_images".to_string(),
            ObjectFormValue::Bool(true),
        );
        values.insert(
            "change_stream_pre_post_images".to_string(),
            ObjectFormValue::Bool(true),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"changeStreamPreAndPostImages\": {"));
        assert!(ddl[0].contains("\"enabled\": true"));
    }

    #[tokio::test]
    async fn mongodb_object_form_can_enable_pre_post_images_on_create() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("auditLogs".to_string()),
        );
        values.insert(
            "change_stream_pre_post_images".to_string(),
            ObjectFormValue::Bool(true),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"create\": \"auditLogs\""));
        assert!(ddl[0].contains("\"changeStreamPreAndPostImages\": {"));
        assert!(ddl[0].contains("\"enabled\": true"));
    }

    #[tokio::test]
    async fn mongodb_object_form_generates_collection_collmod_special_options() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("inventoryTelemetry".to_string()),
        );
        values.insert(
            "timeseries_granularity".to_string(),
            ObjectFormValue::String("hours".to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"collMod\": \"inventoryTelemetry\""));
        assert!(ddl[0].contains("\"timeseries\": {"));
        assert!(ddl[0].contains("\"granularity\": \"hours\""));
        assert!(!ddl[0].contains("\"expireAfterSeconds\""));

        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("ingestBuffer".to_string()),
        );
        values.insert(
            "expire_after_seconds".to_string(),
            ObjectFormValue::String("86400".to_string()),
        );
        values.insert(
            "capped_size".to_string(),
            ObjectFormValue::String("2097152".to_string()),
        );
        values.insert(
            "capped_max".to_string(),
            ObjectFormValue::String("10000".to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"collMod\": \"ingestBuffer\""));
        assert!(ddl[0].contains("\"expireAfterSeconds\": 86400"));
        assert!(ddl[0].contains("\"cappedSize\": 2097152"));
        assert!(ddl[0].contains("\"cappedMax\": 10000"));
    }

    #[tokio::test]
    async fn mongodb_object_form_does_not_emit_unsupported_view_collation_edit() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("activeCustomers".to_string()),
        );
        values.insert(
            "view_on".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );
        values.insert(
            "pipeline".to_string(),
            ObjectFormValue::String(r#"[{"$match":{"active":true}}]"#.to_string()),
        );
        values.insert(
            "collation".to_string(),
            ObjectFormValue::String(r#"{"locale":"en","strength":2}"#.to_string()),
        );

        let ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_view".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values,
            })
            .await
            .unwrap();

        assert!(ddl[0].contains("\"collMod\": \"activeCustomers\""));
        assert!(!ddl[0].contains("\"collation\""));
    }

    #[tokio::test]
    async fn mongodb_object_form_requires_confirm_for_drop() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("customers".to_string()),
        );

        let error = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_collection".to_string(),
                mode: ObjectFormMode::Drop,
                object_ref: None,
                values,
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Confirm"));
    }

    #[tokio::test]
    async fn mongodb_object_form_requires_password_for_create_user() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut values = BTreeMap::new();
        values.insert(
            "name".to_string(),
            ObjectFormValue::String("reporter".to_string()),
        );
        values.insert("confirm".to_string(), ObjectFormValue::Bool(true));

        let error = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_user".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values,
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("Password is required"));
    }

    #[tokio::test]
    async fn mongodb_object_form_generates_confirmed_user_and_role_creates() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut user_values = BTreeMap::new();
        user_values.insert(
            "name".to_string(),
            ObjectFormValue::String("reporter".to_string()),
        );
        user_values.insert(
            "password".to_string(),
            ObjectFormValue::String("secret".to_string()),
        );
        user_values.insert(
            "roles".to_string(),
            ObjectFormValue::String(r#"[{"role":"read","db":"app"}]"#.to_string()),
        );
        user_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));

        let user_ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_user".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values: user_values,
            })
            .await
            .unwrap();

        assert!(user_ddl[0].contains("\"createUser\": \"reporter\""));
        assert!(user_ddl[0].contains("\"pwd\": \"secret\""));
        assert!(user_ddl[0].contains("\"role\": \"read\""));

        let mut role_values = BTreeMap::new();
        role_values.insert(
            "name".to_string(),
            ObjectFormValue::String("inventoryReader".to_string()),
        );
        role_values.insert(
            "privileges".to_string(),
            ObjectFormValue::String(
                r#"[{"resource":{"db":"app","collection":"inventory"},"actions":["find"]}]"#
                    .to_string(),
            ),
        );
        role_values.insert(
            "roles".to_string(),
            ObjectFormValue::String("[]".to_string()),
        );
        role_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));

        let role_ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_role".to_string(),
                mode: ObjectFormMode::Create,
                object_ref: None,
                values: role_values,
            })
            .await
            .unwrap();

        assert!(role_ddl[0].contains("\"createRole\": \"inventoryReader\""));
        assert!(role_ddl[0].contains("\"actions\": ["));
    }

    #[tokio::test]
    async fn mongodb_object_form_generates_confirmed_user_and_role_updates() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );
        let mut user_values = BTreeMap::new();
        user_values.insert(
            "name".to_string(),
            ObjectFormValue::String("reporter".to_string()),
        );
        user_values.insert(
            "roles".to_string(),
            ObjectFormValue::String(r#"[{"role":"read","db":"app"}]"#.to_string()),
        );
        user_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));

        let user_ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_user".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values: user_values,
            })
            .await
            .unwrap();

        assert!(user_ddl[0].contains("\"updateUser\": \"reporter\""));
        assert!(user_ddl[0].contains("\"role\": \"read\""));

        let mut role_values = BTreeMap::new();
        role_values.insert(
            "name".to_string(),
            ObjectFormValue::String("inventoryReader".to_string()),
        );
        role_values.insert(
            "privileges".to_string(),
            ObjectFormValue::String(
                r#"[{"resource":{"db":"app","collection":"inventory"},"actions":["find"]}]"#
                    .to_string(),
            ),
        );
        role_values.insert(
            "roles".to_string(),
            ObjectFormValue::String("[]".to_string()),
        );
        role_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));

        let role_ddl = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_role".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values: role_values,
            })
            .await
            .unwrap();

        assert!(role_ddl[0].contains("\"updateRole\": \"inventoryReader\""));
        assert!(role_ddl[0].contains("\"actions\": ["));
    }

    #[tokio::test]
    async fn mongodb_admin_edit_forms_do_not_default_to_empty_role_sets() {
        let connection = MongoDbConnection::new(
            Client::with_uri_str("mongodb://localhost:27017")
                .await
                .unwrap(),
            "app".to_string(),
            ConnectionConfig::new("mongodb", "test"),
        );

        let user_spec = connection
            .object_form_spec(&ObjectFormSpecRequest {
                kind_id: "document_user".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
            })
            .await
            .unwrap()
            .expect("user edit spec");
        let user_roles = user_spec.sections[0]
            .fields
            .iter()
            .find(|field| field.id == "roles")
            .expect("user roles field");
        assert_eq!(
            user_roles.default_value,
            ObjectFormValue::String(String::new())
        );

        let role_spec = connection
            .object_form_spec(&ObjectFormSpecRequest {
                kind_id: "document_role".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
            })
            .await
            .unwrap()
            .expect("role edit spec");
        for field_id in ["privileges", "roles"] {
            let field = role_spec.sections[0]
                .fields
                .iter()
                .find(|field| field.id == field_id)
                .expect("role edit field");
            assert_eq!(field.default_value, ObjectFormValue::String(String::new()));
        }

        let mut user_values = BTreeMap::new();
        user_values.insert(
            "name".to_string(),
            ObjectFormValue::String("reporter".to_string()),
        );
        user_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));
        let user_error = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_user".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values: user_values,
            })
            .await
            .expect_err("empty user update should fail");
        assert!(user_error.to_string().contains("user change"));

        let mut role_values = BTreeMap::new();
        role_values.insert(
            "name".to_string(),
            ObjectFormValue::String("inventoryReader".to_string()),
        );
        role_values.insert("confirm".to_string(), ObjectFormValue::Bool(true));
        let role_error = connection
            .generate_object_form_ddl(&ObjectFormDdlRequest {
                kind_id: "document_role".to_string(),
                mode: ObjectFormMode::Edit,
                object_ref: None,
                values: role_values,
            })
            .await
            .expect_err("empty role update should fail");
        assert!(role_error.to_string().contains("role change"));
    }
}
