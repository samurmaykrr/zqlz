use super::*;

/// Reads a `collStats` number, which the server sends as int32 whenever it fits.
fn stat_u64(doc: &bson::Document, key: &str) -> Option<u64> {
    match doc.get(key)? {
        bson::Bson::Int32(value) => u64::try_from(*value).ok(),
        bson::Bson::Int64(value) => u64::try_from(*value).ok(),
        bson::Bson::Double(value) if *value >= 0.0 => Some(*value as u64),
        _ => None,
    }
}

#[async_trait]
impl DocumentStore for MongoDbConnection {
    async fn list_document_databases(&self) -> Result<Vec<DocumentDatabaseInfo>> {
        let client = self.client.clone();
        let databases = run_mongodb_future(async move {
            client.list_database_names().await.map_err(|error| {
                ZqlzError::Driver(format!("Failed to list MongoDB databases: {}", error))
            })
        })
        .await?;

        Ok(databases
            .into_iter()
            .map(|name| DocumentDatabaseInfo {
                name,
                size_bytes: None,
                empty: false,
            })
            .collect())
    }

    async fn list_collections(&self, database: &str) -> Result<Vec<DocumentCollectionInfo>> {
        let database_handle = self.client.database(database);
        let collection_infos = {
            let database_handle = database_handle.clone();
            run_mongodb_future(async move {
                let mut cursor = database_handle.list_collections().await.map_err(|error| {
                    ZqlzError::Driver(format!("Failed to list MongoDB collections: {}", error))
                })?;
                let mut infos = Vec::new();
                while cursor.advance().await.map_err(|error| {
                    ZqlzError::Driver(format!(
                        "Failed to read MongoDB collection metadata: {}",
                        error
                    ))
                })? {
                    infos.push(cursor.deserialize_current().map_err(|error| {
                        ZqlzError::Driver(format!(
                            "Failed to parse MongoDB collection metadata: {}",
                            error
                        ))
                    })?);
                }
                Ok::<Vec<CollectionSpecification>, ZqlzError>(infos)
            })
            .await?
        };

        let mut collections = Vec::with_capacity(collection_infos.len());
        for collection_info in collection_infos {
            let name = collection_info.name.clone();
            let options_json = Self::collection_options_json(&collection_info);
            let validator_json = options_json.get("validator").cloned();
            let collation_json = options_json.get("collation").cloned();
            let timeseries_json = options_json.get("timeseries").cloned();
            let clustered_index_json = options_json.get("clusteredIndex").cloned();
            let change_stream_pre_and_post_images = options_json
                .get("changeStreamPreAndPostImages")
                .and_then(|value| value.get("enabled"))
                .and_then(|value| value.as_bool());
            let view_on = options_json
                .get("viewOn")
                .and_then(|value| value.as_str())
                .map(ToString::to_string);
            let pipeline_json = options_json.get("pipeline").cloned();
            let collection_type = match collection_info.collection_type {
                CollectionType::View => "view",
                CollectionType::Timeseries => "timeseries",
                CollectionType::Collection => "collection",
                _ => "collection",
            }
            .to_string();
            let stats = {
                let database_handle = database_handle.clone();
                let name = name.clone();
                run_mongodb_future(async move {
                    database_handle
                        .run_command(bson::doc! { "collStats": &name })
                        .await
                        .map_err(|error| {
                            ZqlzError::Driver(format!("MongoDB collStats failed: {}", error))
                        })
                })
                .await
                .ok()
            };
            collections.push(DocumentCollectionInfo {
                database: database.to_string(),
                name,
                collection_type,
                document_count: stats.as_ref().and_then(|doc| stat_u64(doc, "count")),
                size_bytes: stats.as_ref().and_then(|doc| stat_u64(doc, "size")),
                index_count: stats
                    .as_ref()
                    .and_then(|doc| stat_u64(doc, "nindexes"))
                    .and_then(|value| u32::try_from(value).ok()),
                options_json: Some(options_json),
                validator_json,
                collation_json,
                view_on,
                pipeline_json,
                timeseries_json,
                clustered_index_json,
                change_stream_pre_and_post_images,
            });
        }

        Ok(collections)
    }

    async fn list_database_objects(&self, database: &str) -> Result<DocumentDatabaseObjects> {
        let collections = self.list_collections(database).await?;
        let database_handle = self.client.database(database);

        let mut indexes = Vec::new();
        for collection in collections
            .iter()
            .filter(|collection| collection.collection_type != "view")
        {
            let collection_handle = database_handle.collection::<Document>(&collection.name);
            let collection_name = collection.name.clone();
            let collection_indexes = run_mongodb_future(async move {
                let cursor = collection_handle.list_indexes().await.map_err(|error| {
                    ZqlzError::Driver(format!(
                        "MongoDB listIndexes failed for {}: {}",
                        collection_name, error
                    ))
                })?;
                cursor.try_collect::<Vec<_>>().await.map_err(|error| {
                    ZqlzError::Driver(format!("MongoDB index cursor failed: {}", error))
                })
            })
            .await
            .unwrap_or_default();

            for index in collection_indexes {
                let keys_json = Self::document_to_json(&index.keys);
                let options_json =
                    serde_json::to_value(&index.options).unwrap_or(serde_json::Value::Null);
                let options = index.options.as_ref();
                let name = options
                    .and_then(|options| options.name.clone())
                    .unwrap_or_else(|| index.keys.keys().cloned().collect::<Vec<_>>().join("_"));
                indexes.push(DocumentIndexInfo {
                    database: database.to_string(),
                    collection: collection.name.clone(),
                    name,
                    keys_json,
                    options_json,
                    unique: options.and_then(|options| options.unique).unwrap_or(false),
                    sparse: options.and_then(|options| options.sparse).unwrap_or(false),
                    ttl_seconds: options
                        .and_then(|options| options.expire_after)
                        .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64),
                    partial_filter_json: options
                        .and_then(|options| options.partial_filter_expression.as_ref())
                        .map(Self::document_to_json),
                    collation_json: options
                        .and_then(|options| options.collation.as_ref())
                        .and_then(|collation| serde_json::to_value(collation).ok()),
                    kind: Self::index_kind(&index.keys),
                });
            }
        }

        let mut functions = Vec::new();
        if collections
            .iter()
            .any(|collection| collection.name == "system.js")
        {
            let system_js = database_handle.collection::<Document>("system.js");
            let function_docs = run_mongodb_future(async move {
                let cursor = system_js.find(bson::doc! {}).await.map_err(|error| {
                    ZqlzError::Driver(format!("MongoDB system.js query failed: {}", error))
                })?;
                cursor
                    .try_collect::<Vec<Document>>()
                    .await
                    .map_err(|error| {
                        ZqlzError::Driver(format!("MongoDB system.js cursor failed: {}", error))
                    })
            })
            .await
            .unwrap_or_default();

            for document in function_docs {
                let name = document
                    .get_str("_id")
                    .ok()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "<anonymous>".to_string());
                let body = document
                    .get("value")
                    .map(|value| match value {
                        Bson::JavaScriptCode(code) => code.clone(),
                        Bson::JavaScriptCodeWithScope(code) => code.code.clone(),
                        Bson::String(code) => code.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default();
                functions.push(DocumentFunctionInfo {
                    database: database.to_string(),
                    name,
                    body,
                });
            }
        }

        let gridfs_buckets = Self::gridfs_buckets_from_collections(database, &collections);

        let users = self
            .run_document_admin_command(database, bson::doc! { "usersInfo": 1 }, "user", "users")
            .await;
        let roles = self
            .run_document_admin_command(
                database,
                bson::doc! { "rolesInfo": 1, "showPrivileges": true },
                "role",
                "roles",
            )
            .await;
        let (search_indexes, vector_indexes) = self
            .list_search_index_metadata(database, &collections)
            .await;
        let mut server = Vec::new();
        for command in Self::server_metadata_commands() {
            server.extend(
                self.run_document_admin_command("admin", command, "server", "")
                    .await,
            );
        }
        let mut sharding = self
            .run_document_admin_command(
                "admin",
                bson::doc! { "listShards": 1 },
                "sharding",
                "shards",
            )
            .await;
        sharding.extend(self.list_sharding_metadata(database).await);

        Ok(DocumentDatabaseObjects {
            collections,
            indexes,
            functions,
            gridfs_buckets,
            users,
            roles,
            search_indexes,
            vector_indexes,
            server,
            sharding,
        })
    }

    async fn query_documents(&self, request: DocumentQueryRequest) -> Result<QueryResult> {
        self.ensure_not_closed()?;
        let start = Instant::now();
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let filter = Self::parse_document_json(request.filter_json.as_deref())?;
        let mut options = FindOptions::default();
        options.skip = Some(request.skip);
        options.limit = Some(request.limit.min(i64::MAX as u64) as i64);
        options.projection = Some(Self::parse_document_json(
            request.projection_json.as_deref(),
        )?)
        .filter(|document| !document.is_empty());
        options.sort = Some(Self::parse_document_json(request.sort_json.as_deref())?)
            .filter(|document| !document.is_empty());

        let documents = run_mongodb_future(async move {
            let cursor = collection
                .find(filter)
                .with_options(options)
                .await
                .map_err(|error| ZqlzError::Driver(format!("MongoDB find failed: {}", error)))?;
            cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|error| ZqlzError::Driver(format!("MongoDB cursor failed: {}", error)))
        })
        .await?;
        Ok(Self::documents_to_query_result(
            documents,
            start.elapsed().as_millis() as u64,
        ))
    }

    async fn aggregate_documents(&self, request: DocumentAggregateRequest) -> Result<QueryResult> {
        self.ensure_not_closed()?;
        let start = Instant::now();
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let mut pipeline = Self::parse_pipeline_json(&request.pipeline_json)?;
        if request.limit > 0 {
            pipeline.push(bson::doc! { "$limit": request.limit.min(i64::MAX as u64) as i64 });
        }
        let documents = run_mongodb_future(async move {
            let cursor = collection.aggregate(pipeline).await.map_err(|error| {
                ZqlzError::Driver(format!("MongoDB aggregate failed: {}", error))
            })?;
            cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|error| ZqlzError::Driver(format!("MongoDB cursor failed: {}", error)))
        })
        .await?;
        Ok(Self::documents_to_query_result(
            documents,
            start.elapsed().as_millis() as u64,
        ))
    }

    async fn insert_document(&self, request: DocumentSaveRequest) -> Result<Value> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let document = Self::parse_required_document_json(&request.document_json, "document")?;
        let result = run_mongodb_future(async move {
            collection
                .insert_one(document)
                .await
                .map_err(|error| Self::format_mongodb_write_error("insert", &error))
        })
        .await?;
        Ok(Self::bson_to_value(&result.inserted_id))
    }

    async fn replace_document(&self, request: DocumentReplaceRequest) -> Result<()> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let id = Self::parse_id_json(&request.id_json)?;
        let document = Self::parse_required_document_json(&request.document_json, "document")?;
        let result = run_mongodb_future(async move {
            collection
                .replace_one(bson::doc! { "_id": id }, document)
                .await
                .map_err(|error| Self::format_mongodb_write_error("replace", &error))
        })
        .await?;
        if result.matched_count == 0 {
            return Err(ZqlzError::NotFound("MongoDB document _id".to_string()));
        }
        Ok(())
    }

    async fn update_document_cell(&self, request: DocumentCellUpdateRequest) -> Result<()> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let id = Self::parse_id_json(&request.id_json)?;
        let value = bson::to_bson(&request.new_value.to_json_value()).unwrap_or(Bson::Null);
        let result = run_mongodb_future(async move {
            collection
                .update_one(
                    bson::doc! { "_id": id },
                    bson::doc! { "$set": { request.field_path: value } },
                )
                .await
                .map_err(|error| Self::format_mongodb_write_error("update", &error))
        })
        .await?;
        if result.matched_count == 0 {
            return Err(ZqlzError::NotFound("MongoDB document _id".to_string()));
        }
        Ok(())
    }

    async fn delete_documents(
        &self,
        request: DocumentDeleteRequest,
    ) -> Result<DocumentDeleteOutcome> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let mut outcome = DocumentDeleteOutcome::default();
        for id_json in request.ids_json {
            let id = match Self::parse_id_json(&id_json) {
                Ok(id) => id,
                Err(error) => {
                    outcome.errors.push(format!("{}: {}", id_json, error));
                    if !request.continue_on_error {
                        return Ok(outcome);
                    }
                    continue;
                }
            };
            let delete_result = {
                let collection = collection.clone();
                run_mongodb_future(async move {
                    collection
                        .delete_one(bson::doc! { "_id": id })
                        .await
                        .map_err(|error| {
                            ZqlzError::Driver(format!("MongoDB delete failed: {}", error))
                        })
                })
                .await
            };
            match delete_result {
                Ok(result) if result.deleted_count > 0 => outcome.deleted_ids.push(id_json),
                Ok(_) => outcome
                    .errors
                    .push(format!("{}: document not found", id_json)),
                Err(error) => outcome.errors.push(format!("{}: {}", id_json, error)),
            }
            if !outcome.errors.is_empty() && !request.continue_on_error {
                return Ok(outcome);
            }
        }
        Ok(outcome)
    }

    async fn sample_schema(
        &self,
        request: DocumentSchemaSampleRequest,
    ) -> Result<Vec<DocumentInferredField>> {
        let collection = self
            .client
            .database(&request.database)
            .collection::<Document>(&request.collection);
        let sample_size = request.sample_size.min(i32::MAX as u64) as i32;
        let documents = run_mongodb_future(async move {
            let cursor = collection
                .aggregate(vec![bson::doc! { "$sample": { "size": sample_size } }])
                .await
                .map_err(|error| {
                    ZqlzError::Driver(format!("MongoDB schema sample failed: {}", error))
                })?;
            cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|error| ZqlzError::Driver(format!("MongoDB cursor failed: {}", error)))
        })
        .await?;
        let total = documents.len() as u64;
        let mut fields = std::collections::BTreeMap::<String, (Vec<String>, u64)>::new();
        for document in documents {
            for (name, value) in document {
                let type_name = match value {
                    Bson::Double(_) => "double",
                    Bson::String(_) => "string",
                    Bson::Array(_) => "array",
                    Bson::Document(_) => "document",
                    Bson::Boolean(_) => "bool",
                    Bson::DateTime(_) => "date",
                    Bson::ObjectId(_) => "objectId",
                    Bson::Int32(_) => "int",
                    Bson::Int64(_) => "long",
                    Bson::Decimal128(_) => "decimal",
                    Bson::Null => "null",
                    _ => "bson",
                }
                .to_string();
                let entry = fields.entry(name).or_insert_with(|| (Vec::new(), 0));
                if !entry.0.contains(&type_name) {
                    entry.0.push(type_name);
                }
                entry.1 += 1;
            }
        }
        Ok(fields
            .into_iter()
            .map(|(name, (types, occurrence_count))| DocumentInferredField {
                name,
                types,
                occurrence_count,
                is_required: total > 0 && occurrence_count == total,
            })
            .collect())
    }
}
