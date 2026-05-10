//! Generic document-store orchestration.

use std::sync::Arc;

use zqlz_core::{
    Connection, DocumentAggregateRequest, DocumentCellUpdateRequest, DocumentCollectionInfo,
    DocumentDatabaseInfo, DocumentDeleteOutcome, DocumentDeleteRequest, DocumentInferredField,
    DocumentQueryRequest, DocumentReplaceRequest, DocumentSaveRequest, DocumentSchemaSampleRequest,
    QueryResult, Value,
};

use crate::error::{ServiceError, ServiceResult};

pub struct DocumentService;

impl DocumentService {
    pub fn new() -> Self {
        Self
    }

    pub async fn list_databases(
        &self,
        connection: Arc<dyn Connection>,
    ) -> ServiceResult<Vec<DocumentDatabaseInfo>> {
        Self::store(connection.as_ref())?
            .list_document_databases()
            .await
            .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))
    }

    pub async fn list_collections(
        &self,
        connection: Arc<dyn Connection>,
        database: &str,
    ) -> ServiceResult<Vec<DocumentCollectionInfo>> {
        Self::store(connection.as_ref())?
            .list_collections(database)
            .await
            .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))
    }

    pub async fn query_documents(
        &self,
        connection: Arc<dyn Connection>,
        request: DocumentQueryRequest,
    ) -> ServiceResult<QueryResult> {
        Self::store(connection.as_ref())?
            .query_documents(request)
            .await
            .map_err(|error| ServiceError::TableOperationFailed(error.to_string()))
    }

    pub async fn aggregate_documents(
        &self,
        connection: Arc<dyn Connection>,
        request: DocumentAggregateRequest,
    ) -> ServiceResult<QueryResult> {
        Self::store(connection.as_ref())?
            .aggregate_documents(request)
            .await
            .map_err(|error| ServiceError::TableOperationFailed(error.to_string()))
    }

    pub async fn insert_document(
        &self,
        connection: Arc<dyn Connection>,
        request: DocumentSaveRequest,
    ) -> ServiceResult<Value> {
        Self::store(connection.as_ref())?
            .insert_document(request)
            .await
            .map_err(|error| ServiceError::TableOperationFailed(error.to_string()))
    }

    pub async fn replace_document(
        &self,
        connection: Arc<dyn Connection>,
        request: DocumentReplaceRequest,
    ) -> ServiceResult<()> {
        Self::store(connection.as_ref())?
            .replace_document(request)
            .await
            .map_err(|error| ServiceError::TableOperationFailed(error.to_string()))
    }

    pub async fn update_document_cell(
        &self,
        connection: Arc<dyn Connection>,
        request: DocumentCellUpdateRequest,
    ) -> ServiceResult<()> {
        Self::store(connection.as_ref())?
            .update_document_cell(request)
            .await
            .map_err(|error| ServiceError::UpdateFailed(error.to_string()))
    }

    pub async fn delete_documents(
        &self,
        connection: Arc<dyn Connection>,
        request: DocumentDeleteRequest,
    ) -> ServiceResult<DocumentDeleteOutcome> {
        Self::store(connection.as_ref())?
            .delete_documents(request)
            .await
            .map_err(|error| ServiceError::TableOperationFailed(error.to_string()))
    }

    pub async fn sample_schema(
        &self,
        connection: Arc<dyn Connection>,
        request: DocumentSchemaSampleRequest,
    ) -> ServiceResult<Vec<DocumentInferredField>> {
        Self::store(connection.as_ref())?
            .sample_schema(request)
            .await
            .map_err(|error| ServiceError::SchemaLoadFailed(error.to_string()))
    }

    fn store(connection: &dyn Connection) -> ServiceResult<&dyn zqlz_core::DocumentStore> {
        connection.as_document_store().ok_or_else(|| {
            ServiceError::TableOperationFailed(
                "Connection does not support document operations".to_string(),
            )
        })
    }
}

impl Default for DocumentService {
    fn default() -> Self {
        Self::new()
    }
}
