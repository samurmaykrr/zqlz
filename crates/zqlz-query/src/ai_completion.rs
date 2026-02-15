//! AI Completion - Provider trait and models for AI-powered inline suggestions
//!
//! This module defines the interface for AI completion providers that can generate
//! inline suggestions for SQL queries. The trait allows for different AI backends
//! (OpenAI, Anthropic, local models, etc.) to be plugged in.

use async_trait::async_trait;
use gpui::SharedString;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use zqlz_settings::AiProvider;

/// Request model for AI completion suggestions.
///
/// Contains all the context needed for an AI provider to generate relevant suggestions.
#[derive(Debug, Clone)]
pub struct CompletionRequest {
    /// The text preceding the cursor (prefix)
    pub prefix: SharedString,
    /// The text following the cursor (suffix)
    pub suffix: SharedString,
    /// The current cursor position (byte offset from start of prefix)
    pub cursor_offset: usize,
    /// Optional: schema information (table names, column names, etc.)
    pub schema_context: Option<SchemaContext>,
    /// Optional: the current database/dialect being used
    pub dialect: Option<SharedString>,
}

impl CompletionRequest {
    /// Creates a new completion request with the given context.
    pub fn new(prefix: SharedString, suffix: SharedString, cursor_offset: usize) -> Self {
        Self {
            prefix,
            suffix,
            cursor_offset,
            schema_context: None,
            dialect: None,
        }
    }

    /// Sets the schema context for the request.
    pub fn with_schema(mut self, context: SchemaContext) -> Self {
        self.schema_context = Some(context);
        self
    }

    /// Sets the SQL dialect for the request.
    pub fn with_dialect(mut self, dialect: SharedString) -> Self {
        self.dialect = Some(dialect);
        self
    }

    /// Returns the total context length (prefix + suffix).
    pub fn context_length(&self) -> usize {
        self.prefix.len() + self.suffix.len()
    }
}

/// Schema context containing database object information.
///
/// This helps the AI provider generate more relevant suggestions
/// by knowing what tables, columns, and other objects exist.
#[derive(Debug, Clone, Default)]
pub struct SchemaContext {
    /// Available table names
    pub tables: Vec<TableInfo>,
    /// Available view names
    pub views: Vec<ViewInfo>,
    /// Available stored procedure names
    pub procedures: Vec<ProcedureInfo>,
    /// Available function names
    pub functions: Vec<FunctionInfo>,
}

impl SchemaContext {
    /// Creates a new empty schema context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a schema context with tables.
    pub fn with_tables(mut self, tables: Vec<TableInfo>) -> Self {
        self.tables = tables;
        self
    }

    /// Creates a schema context with views.
    pub fn with_views(mut self, views: Vec<ViewInfo>) -> Self {
        self.views = views;
        self
    }
}

/// Information about a database table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name
    pub name: SharedString,
    /// Column names and their types
    pub columns: Vec<ColumnInfo>,
    /// Optional table description/comment
    pub description: Option<SharedString>,
}

impl TableInfo {
    /// Creates a new table info.
    pub fn new(name: impl Into<SharedString>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            description: None,
        }
    }

    /// Adds a column to the table.
    pub fn with_column(mut self, column: ColumnInfo) -> Self {
        self.columns.push(column);
        self
    }
}

/// Information about a table column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name
    pub name: SharedString,
    /// Column data type (e.g., "VARCHAR", "INTEGER")
    pub data_type: SharedString,
    /// Whether the column is part of the primary key
    pub is_primary_key: bool,
    /// Whether the column allows NULL values
    pub is_nullable: bool,
    /// Optional column description/comment
    pub description: Option<SharedString>,
}

impl ColumnInfo {
    /// Creates a new column info.
    pub fn new(name: impl Into<SharedString>, data_type: impl Into<SharedString>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            is_primary_key: false,
            is_nullable: true,
            description: None,
        }
    }
}

/// Information about a database view.
#[derive(Debug, Clone)]
pub struct ViewInfo {
    /// View name
    pub name: SharedString,
    /// Column names
    pub columns: Vec<ColumnInfo>,
    /// Optional view definition
    pub definition: Option<SharedString>,
}

impl ViewInfo {
    /// Creates a new view info.
    pub fn new(name: impl Into<SharedString>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            definition: None,
        }
    }
}

/// Information about a stored procedure.
#[derive(Debug, Clone)]
pub struct ProcedureInfo {
    /// Procedure name
    pub name: SharedString,
    /// Parameter names and types
    pub parameters: Vec<ParameterInfo>,
    /// Optional procedure description
    pub description: Option<SharedString>,
}

impl ProcedureInfo {
    /// Creates a new procedure info.
    pub fn new(name: impl Into<SharedString>) -> Self {
        Self {
            name: name.into(),
            parameters: Vec::new(),
            description: None,
        }
    }
}

/// Information about a function.
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    /// Function name
    pub name: SharedString,
    /// Return data type
    pub return_type: Option<SharedString>,
    /// Parameter names and types
    pub parameters: Vec<ParameterInfo>,
    /// Optional function description
    pub description: Option<SharedString>,
}

impl FunctionInfo {
    /// Creates a new function info.
    pub fn new(name: impl Into<SharedString>) -> Self {
        Self {
            name: name.into(),
            return_type: None,
            parameters: Vec::new(),
            description: None,
        }
    }
}

/// Information about a procedure or function parameter.
#[derive(Debug, Clone)]
pub struct ParameterInfo {
    /// Parameter name
    pub name: SharedString,
    /// Parameter data type
    pub data_type: SharedString,
    /// Whether the parameter is IN, OUT, or INOUT
    pub mode: ParameterMode,
    /// Optional default value
    pub default_value: Option<SharedString>,
}

/// Parameter mode for procedures/functions.
#[derive(Debug, Clone, Default)]
pub enum ParameterMode {
    #[default]
    In,
    Out,
    InOut,
}

/// Response model for AI completion suggestions.
#[derive(Debug, Clone)]
pub struct CompletionResponse {
    /// The suggested text to insert
    pub suggestion: SharedString,
    /// The start position of the suggestion (byte offset from cursor)
    pub start_offset: isize,
    /// The end position of the suggestion (byte offset from cursor)
    pub end_offset: isize,
    /// Optional: confidence score (0.0 to 1.0)
    pub confidence: Option<f32>,
    /// Optional: human-readable reason for the suggestion
    pub reason: Option<SharedString>,
    /// Optional: metadata about the suggestion
    pub metadata: Option<CompletionMetadata>,
}

impl CompletionResponse {
    /// Creates a new completion response.
    pub fn new(suggestion: SharedString, start_offset: isize, end_offset: isize) -> Self {
        Self {
            suggestion,
            start_offset,
            end_offset,
            confidence: None,
            reason: None,
            metadata: None,
        }
    }

    /// Sets the confidence score.
    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = Some(confidence.clamp(0.0, 1.0));
        self
    }

    /// Sets the reason for the suggestion.
    pub fn with_reason(mut self, reason: impl Into<SharedString>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Sets the metadata.
    pub fn with_metadata(mut self, metadata: CompletionMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// Metadata about a completion suggestion.
#[derive(Debug, Clone)]
pub struct CompletionMetadata {
    /// The provider that generated this suggestion
    pub provider: SharedString,
    /// Optional: ID for tracking this suggestion
    pub suggestion_id: Option<SharedString>,
    /// Optional: tokens used for the request (for billing/tracking)
    pub tokens_used: Option<u32>,
}

impl CompletionMetadata {
    /// Creates new metadata with a provider name.
    pub fn new(provider: impl Into<SharedString>) -> Self {
        Self {
            provider: provider.into(),
            suggestion_id: None,
            tokens_used: None,
        }
    }
}

/// Error type for AI completion operations.
#[derive(Debug, Clone)]
pub enum CompletionError {
    /// Network-related error
    Network(String),
    /// Authentication error (invalid API key, etc.)
    Authentication(String),
    /// Rate limiting error
    RateLimited(String),
    /// The provider returned an invalid response
    InvalidResponse(String),
    /// The request was cancelled
    Cancelled,
    /// Timeout waiting for response
    Timeout,
    /// Provider not configured or unavailable
    ProviderUnavailable(String),
    /// Other error
    Other(String),
}

impl std::fmt::Display for CompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Network(msg) => write!(f, "Network error: {}", msg),
            Self::Authentication(msg) => write!(f, "Authentication error: {}", msg),
            Self::RateLimited(msg) => write!(f, "Rate limited: {}", msg),
            Self::InvalidResponse(msg) => write!(f, "Invalid response: {}", msg),
            Self::Cancelled => write!(f, "Request cancelled"),
            Self::Timeout => write!(f, "Request timed out"),
            Self::ProviderUnavailable(msg) => write!(f, "Provider unavailable: {}", msg),
            Self::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for CompletionError {}

/// Result type for AI completion operations.
pub type CompletionResult<T> = Result<T, CompletionError>;

/// AI Completion provider trait.
///
/// Implement this trait to create a new AI completion backend.
/// Common implementations include OpenAI, Anthropic, local models, etc.
#[async_trait]
pub trait AiCompletionProvider: Send + Sync {
    /// Request a completion suggestion.
    ///
    /// The provider should analyze the request and return a suggestion
    /// that makes sense in the given context.
    async fn suggest(&self, request: CompletionRequest) -> CompletionResult<CompletionResponse>;

    /// Cancel any in-progress suggestion request.
    ///
    /// This is called when the user dismisses a suggestion or types more characters,
    /// allowing the provider to abort any pending network requests.
    fn cancel(&self);

    /// Get metadata about the provider.
    ///
    /// Returns information like provider name, version, etc.
    fn metadata(&self) -> ProviderMetadata;

    /// Check if the provider is ready to serve requests.
    ///
    /// Returns true if the provider is configured and can handle requests.
    /// Returns false if API keys are missing or provider is otherwise unavailable.
    fn is_available(&self) -> bool;
}

/// Metadata about an AI completion provider.
#[derive(Debug, Clone)]
pub struct ProviderMetadata {
    /// The provider's display name (e.g., "OpenAI", "Anthropic")
    pub name: SharedString,
    /// The model being used (e.g., "gpt-4", "claude-3")
    pub model: SharedString,
    /// Optional: provider-specific information
    pub info: Option<SharedString>,
}

impl ProviderMetadata {
    /// Creates new provider metadata.
    pub fn new(name: impl Into<SharedString>, model: impl Into<SharedString>) -> Self {
        Self {
            name: name.into(),
            model: model.into(),
            info: None,
        }
    }
}

/// Factory for creating AI completion providers based on settings.
pub struct AiProviderFactory;

impl AiProviderFactory {
    /// Creates an AI provider based on the given configuration.
    ///
    /// Returns `None` if the provider is set to `AiProvider::None` or
    /// if the required API key is not configured.
    pub fn create_provider(
        provider: AiProvider,
        api_key: Option<SharedString>,
        model: SharedString,
        temperature: f32,
    ) -> Option<Box<dyn AiCompletionProvider>> {
        match provider {
            AiProvider::OpenAi => {
                let api_key = api_key?;
                Some(Box::new(OpenAiProvider::new(api_key, model, temperature)))
            }
            AiProvider::Anthropic => {
                let api_key = api_key?;
                Some(Box::new(AnthropicProvider::new(api_key, model, temperature)))
            }
            AiProvider::Local => {
                // Local provider would require additional configuration
                // For now, return None until local provider is implemented
                None
            }
            AiProvider::None => None,
        }
    }
}

/// OpenAI API provider for AI completions.
pub struct OpenAiProvider {
    api_key: String,
    model: String,
    temperature: f32,
    client: Client,
    cancel_flag: Arc<RwLock<bool>>,
}

impl OpenAiProvider {
    /// Creates a new OpenAI provider.
    pub fn new(api_key: SharedString, model: SharedString, temperature: f32) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            api_key: api_key.into(),
            model: model.into(),
            temperature,
            client,
            cancel_flag: Arc::new(RwLock::new(false)),
        }
    }
}

#[derive(Serialize)]
struct OpenAiRequest {
    model: String,
    prompt: String,
    suffix: Option<String>,
    max_tokens: u32,
    temperature: f32,
    stream: bool,
}

#[derive(Deserialize)]
struct OpenAiResponse {
    choices: Vec<OpenAiChoice>,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    text: String,
    index: usize,
    finish_reason: Option<String>,
}

#[async_trait]
impl AiCompletionProvider for OpenAiProvider {
    async fn suggest(&self, request: CompletionRequest) -> CompletionResult<CompletionResponse> {
        // Reset cancel flag
        *self.cancel_flag.write().await = false;

        // Check if cancelled
        if *self.cancel_flag.read().await {
            return Err(CompletionError::Cancelled);
        }

        // Build the prompt with schema context if available
        let mut prompt = String::new();
        
        if let Some(ref schema) = request.schema_context {
            prompt.push_str("Database schema:\n");
            for table in &schema.tables {
                prompt.push_str(&format!("- {} (", table.name));
                let cols: Vec<String> = table.columns
                    .iter()
                    .map(|c| format!("{}: {}", c.name, c.data_type))
                    .collect();
                prompt.push_str(&cols.join(", "));
                prompt.push_str(")\n");
            }
            prompt.push_str("\n");
        }
        
        prompt.push_str("SQL query (complete the query):\n");
        prompt.push_str(&request.prefix);
        
        let suffix = if request.suffix.is_empty() {
            None
        } else {
            Some(request.suffix.to_string())
        };

        let req = OpenAiRequest {
            model: self.model.clone(),
            prompt: prompt.to_string(),
            suffix,
            max_tokens: 256,
            temperature: self.temperature,
            stream: false,
        };

        let response = self.client
            .post("https://api.openai.com/v1/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&req)
            .send()
            .await
            .map_err(|e| CompletionError::Network(e.to_string()))?;

        // Check if cancelled after network call
        if *self.cancel_flag.read().await {
            return Err(CompletionError::Cancelled);
        }

        if response.status() == 401 {
            return Err(CompletionError::Authentication(
                "Invalid OpenAI API key".to_string(),
            ));
        }

        if response.status() == 429 {
            return Err(CompletionError::RateLimited(
                "Rate limit exceeded".to_string(),
            ));
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(CompletionError::InvalidResponse(
                format!("Status {}: {}", status, body),
            ));
        }

        let response: OpenAiResponse = response
            .json()
            .await
            .map_err(|e| CompletionError::InvalidResponse(e.to_string()))?;

        if let Some(choice) = response.choices.into_iter().next() {
            let suggestion = choice.text.trim().to_string();
            
            // Calculate start/end offsets based on the suggestion
            // The suggestion is inserted after the prefix
            let start_offset = 0;
            let end_offset = suggestion.len() as isize;

            Ok(CompletionResponse::new(
                suggestion.into(),
                start_offset,
                end_offset,
            )
            .with_confidence(0.8)
            .with_reason(SharedString::from("OpenAI completion")))
        } else {
            Err(CompletionError::InvalidResponse(
                "No completion choices returned".to_string(),
            ))
        }
    }

    fn cancel(&self) {
        // Set the cancel flag
        let flag = self.cancel_flag.clone();
        tokio::spawn(async move {
            *flag.write().await = true;
        });
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata::new("OpenAI", &self.model)
    }

    fn is_available(&self) -> bool {
        !self.api_key.is_empty()
    }
}

/// Anthropic API provider for AI completions.
pub struct AnthropicProvider {
    api_key: String,
    model: String,
    temperature: f32,
    client: Client,
    cancel_flag: Arc<RwLock<bool>>,
}

impl AnthropicProvider {
    /// Creates a new Anthropic provider.
    pub fn new(api_key: SharedString, model: SharedString, temperature: f32) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            api_key: api_key.into(),
            model: model.into(),
            temperature,
            client,
            cancel_flag: Arc::new(RwLock::new(false)),
        }
    }
}

#[derive(Serialize)]
struct AnthropicRequest {
    model: String,
    prompt: String,
    max_tokens_to_sample: u32,
    temperature: f32,
    stream: bool,
}

#[derive(Deserialize)]
struct AnthropicResponse {
    completion: String,
    stop_reason: Option<String>,
    #[serde(default)]
    log_id: String,
}

#[async_trait]
impl AiCompletionProvider for AnthropicProvider {
    async fn suggest(&self, request: CompletionRequest) -> CompletionResult<CompletionResponse> {
        // Reset cancel flag
        *self.cancel_flag.write().await = false;

        // Check if cancelled
        if *self.cancel_flag.read().await {
            return Err(CompletionError::Cancelled);
        }

        // Anthropic uses a special prompt format
        let mut prompt = String::from("\n\nHuman: ");
        
        if let Some(ref schema) = request.schema_context {
            prompt.push_str("Database schema:\n");
            for table in &schema.tables {
                prompt.push_str(&format!("- {} (", table.name));
                let cols: Vec<String> = table.columns
                    .iter()
                    .map(|c| format!("{}: {}", c.name, c.data_type))
                    .collect();
                prompt.push_str(&cols.join(", "));
                prompt.push_str(")\n");
            }
            prompt.push_str("\n");
        }
        
        prompt.push_str("Complete the following SQL query:\n");
        prompt.push_str(&request.prefix);
        
        if !request.suffix.is_empty() {
            prompt.push_str(request.suffix.as_ref());
        }
        
        prompt.push_str("\n\nAssistant:");

        let req = AnthropicRequest {
            model: self.model.clone(),
            prompt: prompt.to_string(),
            max_tokens_to_sample: 256,
            temperature: self.temperature,
            stream: false,
        };

        let response = self.client
            .post("https://api.anthropic.com/v1/complete")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("Content-Type", "application/json")
            .json(&req)
            .send()
            .await
            .map_err(|e| CompletionError::Network(e.to_string()))?;

        // Check if cancelled after network call
        if *self.cancel_flag.read().await {
            return Err(CompletionError::Cancelled);
        }

        if response.status() == 401 {
            return Err(CompletionError::Authentication(
                "Invalid Anthropic API key".to_string(),
            ));
        }

        if response.status() == 429 {
            return Err(CompletionError::RateLimited(
                "Rate limit exceeded".to_string(),
            ));
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(CompletionError::InvalidResponse(
                format!("Status {}: {}", status, body),
            ));
        }

        let response: AnthropicResponse = response
            .json()
            .await
            .map_err(|e| CompletionError::InvalidResponse(e.to_string()))?;

        let suggestion = response.completion.trim().to_string();
        
        if suggestion.is_empty() {
            return Err(CompletionError::InvalidResponse(
                "Empty completion returned".to_string(),
            ));
        }

        let start_offset = 0;
        let end_offset = suggestion.len() as isize;

        Ok(CompletionResponse::new(
            suggestion.into(),
            start_offset,
            end_offset,
        )
        .with_confidence(0.8)
        .with_reason(SharedString::from("Anthropic completion")))
    }

    fn cancel(&self) {
        let flag = self.cancel_flag.clone();
        tokio::spawn(async move {
            *flag.write().await = true;
        });
    }

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata::new("Anthropic", &self.model)
    }

    fn is_available(&self) -> bool {
        !self.api_key.is_empty()
    }
}

/// A no-op provider for when AI is disabled.
pub struct NoOpProvider;

#[async_trait]
impl AiCompletionProvider for NoOpProvider {
    async fn suggest(&self, _request: CompletionRequest) -> CompletionResult<CompletionResponse> {
        Err(CompletionError::ProviderUnavailable(
            "AI provider is disabled".to_string(),
        ))
    }

    fn cancel(&self) {}

    fn metadata(&self) -> ProviderMetadata {
        ProviderMetadata::new("None", "N/A")
    }

    fn is_available(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completion_request() {
        let request = CompletionRequest::new(
            "SELECT * FROM users WHERE ".into(),
            " AND id > 0".into(),
            26,
        );

        assert_eq!(request.prefix.len(), 26);
        assert_eq!(request.suffix.len(), 11);
        assert_eq!(request.context_length(), 37);
    }

    #[test]
    fn test_completion_response() {
        let response = CompletionResponse::new("id".into(), 0, 2)
            .with_confidence(0.95)
            .with_reason("Common column name");

        assert_eq!(response.suggestion.as_ref(), "id");
        assert_eq!(response.start_offset, 0);
        assert_eq!(response.end_offset, 2);
        assert_eq!(response.confidence, Some(0.95));
        assert_eq!(response.reason.unwrap().as_ref(), "Common column name");
    }

    #[test]
    fn test_confidence_clamped() {
        let response = CompletionResponse::new("test".into(), 0, 4).with_confidence(1.5);

        assert_eq!(response.confidence, Some(1.0));
    }

    #[test]
    fn test_schema_context() {
        let ctx = SchemaContext::new()
            .with_tables(vec![TableInfo::new("users")
                .with_column(ColumnInfo::new("id", "INTEGER"))
                .with_column(ColumnInfo::new("name", "VARCHAR"))])
            .with_views(vec![ViewInfo::new("active_users")]);

        assert_eq!(ctx.tables.len(), 1);
        assert_eq!(ctx.tables[0].columns.len(), 2);
        assert_eq!(ctx.views.len(), 1);
    }
}
