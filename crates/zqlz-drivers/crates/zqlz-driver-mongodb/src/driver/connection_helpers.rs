use super::*;

pub struct MongoDbConnection {
    pub(super) client: Client,
    pub(super) database: String,
    #[allow(dead_code)]
    config: ConnectionConfig,
    pub(super) closed: AtomicBool,
}

impl MongoDbConnection {
    /// Create a new MongoDB connection wrapper
    pub fn new(client: Client, database: String, config: ConnectionConfig) -> Self {
        Self {
            client,
            database,
            config,
            closed: AtomicBool::new(false),
        }
    }

    /// Get the current database name
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Get the MongoDB client
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Get the current database object
    pub fn db(&self) -> mongodb::Database {
        self.client.database(&self.database)
    }

    pub(super) fn ensure_not_closed(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ZqlzError::Driver("Connection is closed".to_string()));
        }
        Ok(())
    }

    /// Parse a JSON/BSON query string into a Document
    pub(super) fn parse_query(&self, query: &str) -> Result<Document> {
        let trimmed = query.trim();

        if let Some(command) = Self::parse_shell_query(trimmed)? {
            return Ok(command);
        }

        // Try parsing as JSON first
        if trimmed.starts_with('{') {
            serde_json::from_str::<Document>(trimmed)
                .map_err(|e| ZqlzError::Driver(format!("Invalid JSON document: {}", e)))
        } else {
            // Extended JSON format or other formats
            bson::from_slice(trimmed.as_bytes())
                .map_err(|e| ZqlzError::Driver(format!("Invalid BSON document: {}", e)))
        }
    }

    pub(super) fn parse_shell_query(query: &str) -> Result<Option<Document>> {
        let Some((database, after_db)) = Self::parse_shell_database_target(query)? else {
            return Ok(None);
        };
        let Some((collection, after_collection)) = Self::parse_shell_collection_target(after_db)?
        else {
            return Ok(None);
        };
        if collection.is_empty() {
            return Ok(None);
        }

        if let Some(after_method) = after_collection.strip_prefix("find(") {
            let (arguments, tail) = Self::take_parenthesized_arguments(after_method)?;
            let arguments = Self::split_shell_arguments(&arguments)?;
            let mut command = bson::doc! {
                "find": collection,
                "filter": Self::parse_shell_json_argument(arguments.first(), "find filter")?,
            };
            if let Some(database) = database.as_deref() {
                command.insert("$db", database);
            }

            if let Some(projection) = arguments.get(1) {
                command.insert(
                    "projection",
                    Self::parse_shell_json_document(projection, "find projection")?,
                );
            }

            let mut explain_verbosity = None;
            let mut tail = tail.trim();
            while !tail.is_empty() {
                if let Some(after_sort) = tail.strip_prefix(".sort(") {
                    let (argument, next_tail) = Self::take_parenthesized_arguments(after_sort)?;
                    command.insert(
                        "sort",
                        Self::parse_shell_json_document(&argument, "sort specification")?,
                    );
                    tail = next_tail.trim();
                } else if let Some(after_limit) = tail.strip_prefix(".limit(") {
                    let (argument, next_tail) = Self::take_parenthesized_arguments(after_limit)?;
                    let limit = argument.trim().parse::<i64>().map_err(|error| {
                        ZqlzError::Driver(format!("Invalid MongoDB limit value: {error}"))
                    })?;
                    command.insert("limit", limit);
                    tail = next_tail.trim();
                } else if let Some(after_explain) = tail.strip_prefix(".explain(") {
                    let (argument, next_tail) = Self::take_parenthesized_arguments(after_explain)?;
                    explain_verbosity = Some(Self::parse_shell_explain_verbosity(argument.trim())?);
                    tail = next_tail.trim();
                } else if tail == ";" {
                    break;
                } else {
                    return Err(ZqlzError::Driver(format!(
                        "Unsupported MongoDB cursor method: {tail}"
                    )));
                }
            }

            if let Some(verbosity) = explain_verbosity {
                command = Self::explain_command(command, verbosity);
            }

            return Ok(Some(command));
        }

        if let Some(after_method) = after_collection.strip_prefix("aggregate(") {
            let (argument, tail) = Self::take_parenthesized_arguments(after_method)?;
            let mut explain_verbosity = None;
            let tail = if let Some(after_explain) = tail.trim().strip_prefix(".explain(") {
                let (argument, tail) = Self::take_parenthesized_arguments(after_explain)?;
                explain_verbosity = Some(Self::parse_shell_explain_verbosity(argument.trim())?);
                tail.trim()
            } else {
                tail.trim()
            };
            if !tail.is_empty() && tail != ";" {
                return Err(ZqlzError::Driver(format!(
                    "Unsupported MongoDB aggregate suffix: {tail}"
                )));
            }
            let pipeline = Self::parse_shell_json_array(&argument, "aggregate pipeline")?;
            let mut command = bson::doc! {
                "aggregate": collection,
                "pipeline": pipeline,
                "cursor": {},
            };
            if let Some(database) = database.as_deref() {
                command.insert("$db", database);
            }
            if let Some(verbosity) = explain_verbosity {
                command = Self::explain_command(command, verbosity);
            }
            return Ok(Some(command));
        }

        Ok(None)
    }

    pub(super) fn explain_command(mut command: Document, verbosity: &'static str) -> Document {
        let database = command.remove("$db");
        let mut explain_command = bson::doc! {
            "explain": command,
            "verbosity": verbosity,
        };
        if let Some(database) = database {
            explain_command.insert("$db", database);
        }
        explain_command
    }

    pub(super) fn parse_shell_explain_verbosity(argument: &str) -> Result<&'static str> {
        if argument.is_empty() {
            return Ok("queryPlanner");
        }

        let value = Self::parse_shell_json_value(argument, "explain verbosity")?;
        match value.as_str() {
            Some("queryPlanner") => Ok("queryPlanner"),
            Some("executionStats") => Ok("executionStats"),
            Some("allPlansExecution") => Ok("allPlansExecution"),
            Some(other) => Err(ZqlzError::Driver(format!(
                "Unsupported MongoDB explain verbosity: {other}"
            ))),
            None => Err(ZqlzError::Driver(
                "MongoDB explain verbosity must be a string".to_string(),
            )),
        }
    }

    pub(super) fn parse_shell_database_target(
        input: &str,
    ) -> Result<Option<(Option<String>, &str)>> {
        let Some(after_db) = input.strip_prefix("db") else {
            return Ok(None);
        };

        if let Some(after_method) = after_db.strip_prefix(".getSiblingDB(") {
            let (argument, tail) = Self::take_parenthesized_arguments(after_method)?;
            let tail = tail.strip_prefix('.').ok_or_else(|| {
                ZqlzError::Driver("MongoDB getSiblingDB target must be followed by a method".into())
            })?;
            let database =
                serde_json_lenient::from_str::<String>(argument.trim()).map_err(|error| {
                    ZqlzError::Driver(format!("Invalid MongoDB database name: {error}"))
                })?;
            return Ok(Some((Some(database), tail)));
        }

        if let Some(after_dot) = after_db.strip_prefix('.') {
            return Ok(Some((None, after_dot)));
        }

        Ok(None)
    }

    pub(super) fn parse_shell_collection_target(input: &str) -> Result<Option<(String, &str)>> {
        if let Some(after_method) = input.strip_prefix("getCollection(") {
            let (argument, tail) = Self::take_parenthesized_arguments(after_method)?;
            let tail = tail.strip_prefix('.').ok_or_else(|| {
                ZqlzError::Driver(
                    "MongoDB getCollection target must be followed by a method".into(),
                )
            })?;
            let collection =
                serde_json_lenient::from_str::<String>(argument.trim()).map_err(|error| {
                    ZqlzError::Driver(format!("Invalid MongoDB collection name: {error}"))
                })?;
            return Ok(Some((collection, tail)));
        }

        Ok(input
            .split_once('.')
            .map(|(collection, tail)| (collection.to_string(), tail)))
    }

    pub(super) fn take_parenthesized_arguments(input: &str) -> Result<(String, &str)> {
        let mut depth = 1usize;
        let mut in_string: Option<char> = None;
        let mut escaped = false;

        for (index, character) in input.char_indices() {
            if let Some(quote) = in_string {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == quote {
                    in_string = None;
                }
                continue;
            }

            match character {
                '"' | '\'' => in_string = Some(character),
                '(' => depth = depth.saturating_add(1),
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return Ok((input[..index].to_string(), &input[index + 1..]));
                    }
                }
                _ => {}
            }
        }

        Err(ZqlzError::Driver(
            "Unclosed MongoDB shell method call".to_string(),
        ))
    }

    pub(super) fn split_shell_arguments(arguments: &str) -> Result<Vec<String>> {
        let mut parts = Vec::new();
        let mut start = 0usize;
        let mut depth = 0usize;
        let mut in_string: Option<char> = None;
        let mut escaped = false;

        for (index, character) in arguments.char_indices() {
            if let Some(quote) = in_string {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == quote {
                    in_string = None;
                }
                continue;
            }

            match character {
                '"' | '\'' => in_string = Some(character),
                '{' | '[' | '(' => depth = depth.saturating_add(1),
                '}' | ']' | ')' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => {
                    let part = arguments[start..index].trim();
                    if !part.is_empty() {
                        parts.push(part.to_string());
                    }
                    start = index + 1;
                }
                _ => {}
            }
        }

        let part = arguments[start..].trim();
        if !part.is_empty() {
            parts.push(part.to_string());
        }

        Ok(parts)
    }

    pub(super) fn parse_shell_json_argument(
        argument: Option<&String>,
        context: &str,
    ) -> Result<Document> {
        match argument {
            Some(argument) => Self::parse_shell_json_document(argument, context),
            None => Ok(Document::new()),
        }
    }

    pub(super) fn parse_shell_json_document(argument: &str, context: &str) -> Result<Document> {
        let value = Self::parse_shell_json_value(argument, context)?;
        bson::to_document(&value)
            .map_err(|error| ZqlzError::Driver(format!("Invalid MongoDB {context}: {error}")))
    }

    pub(super) fn parse_shell_json_array(argument: &str, context: &str) -> Result<Vec<Bson>> {
        let value = Self::parse_shell_json_value(argument, context)?;
        let array = value
            .as_array()
            .ok_or_else(|| ZqlzError::Driver(format!("MongoDB {context} must be a JSON array")))?;
        array
            .iter()
            .map(|value| {
                bson::to_bson(value).map_err(|error| {
                    ZqlzError::Driver(format!("Invalid MongoDB {context}: {error}"))
                })
            })
            .collect()
    }

    pub(super) fn parse_shell_json_value(
        argument: &str,
        context: &str,
    ) -> Result<serde_json::Value> {
        let normalized = Self::quote_unquoted_json_keys(&Self::expand_object_id_calls(argument));
        serde_json_lenient::from_str(&normalized)
            .map_err(|error| ZqlzError::Driver(format!("Invalid MongoDB {context}: {error}")))
    }

    /// Rewrites shell `ObjectId("hex")` calls outside of strings into the
    /// extended JSON form `{"$oid": "hex"}` that the rest of the parser reads.
    pub(super) fn expand_object_id_calls(input: &str) -> String {
        const CALL: &str = "ObjectId(";
        let mut output = String::with_capacity(input.len());
        let mut in_string: Option<char> = None;
        let mut escaped = false;
        let mut index = 0;

        while index < input.len() {
            let rest = &input[index..];
            let Some(character) = rest.chars().next() else {
                break;
            };
            if let Some(quote) = in_string {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == quote {
                    in_string = None;
                }
            } else if character == '"' || character == '\'' {
                in_string = Some(character);
            } else if let Some(after) = rest.strip_prefix(CALL)
                && let Some(end) = after.find(')')
            {
                let argument = after[..end].trim();
                if argument.starts_with(['"', '\'']) {
                    output.push_str(&format!("{{\"$oid\":{argument}}}"));
                    index += CALL.len() + end + 1;
                    continue;
                }
            }
            output.push(character);
            index += character.len_utf8();
        }

        output
    }

    pub(super) fn quote_unquoted_json_keys(input: &str) -> String {
        let mut output = String::with_capacity(input.len());
        let mut chars = input.chars().peekable();
        let mut in_string: Option<char> = None;
        let mut escaped = false;
        let mut expecting_key = false;

        while let Some(character) = chars.next() {
            if let Some(quote) = in_string {
                output.push(character);
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == quote {
                    in_string = None;
                }
                continue;
            }

            match character {
                '"' | '\'' => {
                    in_string = Some(character);
                    output.push(character);
                    expecting_key = false;
                }
                '{' | ',' => {
                    output.push(character);
                    expecting_key = true;
                }
                c if expecting_key && c.is_whitespace() => output.push(c),
                c if expecting_key && Self::is_shell_key_start(c) => {
                    let mut key = String::new();
                    key.push(c);
                    while let Some(next) = chars.peek().copied() {
                        if Self::is_shell_key_continue(next) {
                            key.push(next);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    if chars.peek().is_some_and(|next| *next == ':') {
                        output.push('"');
                        output.push_str(&key);
                        output.push('"');
                        expecting_key = false;
                    } else {
                        output.push_str(&key);
                    }
                }
                ':' => {
                    output.push(character);
                    expecting_key = false;
                }
                _ => {
                    output.push(character);
                    expecting_key = false;
                }
            }
        }

        output
    }

    pub(super) fn is_shell_key_start(character: char) -> bool {
        character == '$' || character == '_' || character.is_ascii_alphabetic()
    }

    pub(super) fn is_shell_key_continue(character: char) -> bool {
        character == '$'
            || character == '_'
            || character == '.'
            || character == '-'
            || character.is_ascii_alphanumeric()
    }

    /// Convert a BSON value to our Value type
    pub(super) fn bson_to_value(bson: &Bson) -> Value {
        match bson {
            Bson::Null => Value::Null,
            Bson::Boolean(b) => Value::Bool(*b),
            Bson::Int32(i) => Value::Int32(*i),
            Bson::Int64(i) => Value::Int64(*i),
            Bson::Double(d) => Value::Float64(*d),
            Bson::String(s) => Value::String(s.clone()),
            Bson::Array(arr) => {
                let values: Vec<Value> = arr.iter().map(Self::bson_to_value).collect();
                Value::Array(values)
            }
            Bson::Document(doc) => {
                Value::Json(serde_json::to_value(doc).unwrap_or(serde_json::Value::Null))
            }
            Bson::ObjectId(oid) => Value::String(oid.to_hex()),
            Bson::DateTime(dt) => Value::String(dt.to_string()),
            Bson::Binary(bin) => Value::Bytes(bin.bytes.clone()),
            Bson::Decimal128(d) => Value::Decimal(d.to_string()),
            Bson::Timestamp(ts) => Value::Int64(ts.time as i64),
            Bson::RegularExpression(re) => Value::String(format!("/{}/{}", re.pattern, re.options)),
            Bson::JavaScriptCode(code) => Value::String(code.clone()),
            Bson::JavaScriptCodeWithScope(code) => Value::String(code.code.clone()),
            Bson::Symbol(sym) => Value::String(sym.clone()),
            Bson::Undefined => Value::Null,
            Bson::MaxKey | Bson::MinKey => Value::Null,
            Bson::DbPointer(_) => Value::String("<DbPointer>".to_string()),
        }
    }

    pub(super) fn bson_data_type(bson: &Bson) -> &'static str {
        match bson {
            Bson::Null | Bson::Undefined => "null",
            Bson::Boolean(_) => "bool",
            Bson::Int32(_) => "int",
            Bson::Int64(_) => "long",
            Bson::Double(_) => "double",
            Bson::String(_) | Bson::Symbol(_) => "string",
            Bson::Array(_) | Bson::Document(_) => "json",
            Bson::ObjectId(_) => "objectId",
            Bson::DateTime(_) => "date",
            Bson::Binary(_) => "binary",
            Bson::Decimal128(_) => "decimal",
            Bson::Timestamp(_) => "timestamp",
            Bson::RegularExpression(_) => "regex",
            Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => "javascript",
            Bson::MaxKey => "maxKey",
            Bson::MinKey => "minKey",
            Bson::DbPointer(_) => "dbPointer",
        }
    }

    pub(super) fn inferred_bson_column_type(documents: &[Document], column_name: &str) -> String {
        let mut inferred_type: Option<&'static str> = None;
        for document in documents {
            let Some(value) = document.get(column_name) else {
                continue;
            };
            let data_type = Self::bson_data_type(value);
            if data_type == "null" {
                continue;
            }
            match inferred_type {
                None => inferred_type = Some(data_type),
                Some(existing) if existing == data_type => {}
                Some("int") if data_type == "long" => inferred_type = Some("long"),
                Some("long") if data_type == "int" => {}
                Some("double" | "decimal") if matches!(data_type, "int" | "long") => {}
                Some("int" | "long") if matches!(data_type, "double" | "decimal") => {
                    inferred_type = Some(data_type)
                }
                Some("json") | Some("bson") => inferred_type = Some("bson"),
                Some(_) => {
                    inferred_type = Some("bson");
                    break;
                }
            }
        }

        inferred_type.unwrap_or("bson").to_string()
    }

    pub(super) fn document_to_json(document: &Document) -> serde_json::Value {
        serde_json::to_value(document).unwrap_or(serde_json::Value::Null)
    }

    pub(super) fn bson_to_json(value: &Bson) -> serde_json::Value {
        serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
    }

    pub(super) fn collection_options_json(
        collection_info: &CollectionSpecification,
    ) -> serde_json::Value {
        serde_json::to_value(&collection_info.options).unwrap_or(serde_json::Value::Null)
    }

    pub(super) fn index_kind(keys: &Document) -> String {
        let mut kinds = Vec::new();
        for value in keys.values() {
            match value {
                Bson::String(value) if value == "text" => kinds.push("text"),
                Bson::String(value) if value == "2dsphere" || value == "2d" => kinds.push("geo"),
                Bson::String(value) if value == "hashed" => kinds.push("hashed"),
                Bson::String(value) if value == "columnstore" => kinds.push("columnstore"),
                _ => {}
            }
        }

        if keys.keys().any(|key| key == "$**" || key.ends_with(".$**")) && kinds.is_empty() {
            kinds.push("wildcard");
        }

        if kinds.is_empty() {
            "btree".to_string()
        } else {
            kinds.sort_unstable();
            kinds.dedup();
            kinds.join("+")
        }
    }

    /// Execute a command on the database
    pub(super) async fn run_command(&self, mut command: Document) -> Result<Document> {
        let database_name = Self::command_target_database(&self.database, &mut command);
        let database = self.client.database(&database_name);
        run_mongodb_future(async move {
            database
                .run_command(command)
                .await
                .map_err(|e| ZqlzError::Driver(format!("MongoDB command failed: {}", e)))
        })
        .await
    }

    pub(super) fn command_target_database(
        default_database: &str,
        command: &mut Document,
    ) -> String {
        match command.remove("$db") {
            Some(Bson::String(database)) if !database.is_empty() => database,
            _ => default_database.to_string(),
        }
    }

    pub(super) async fn run_document_admin_command(
        &self,
        database: &str,
        command: Document,
        kind: &str,
        array_field: &str,
    ) -> Vec<DocumentAdminObjectInfo> {
        let database_name = database.to_string();
        let kind_name = kind.to_string();
        let command_name = command.keys().next().cloned().unwrap_or(kind_name.clone());
        let database_handle = self.client.database(database);
        let result = run_mongodb_future(async move {
            database_handle
                .run_command(command)
                .await
                .map_err(|error| ZqlzError::Driver(error.to_string()))
        })
        .await;

        match result {
            Ok(document) => {
                if !array_field.is_empty()
                    && let Some(Bson::Array(items)) = document.get(array_field)
                {
                    return items
                        .iter()
                        .enumerate()
                        .map(|(index, item)| {
                            let details_json = Self::bson_to_json(item);
                            let name = item
                                .as_document()
                                .and_then(|document| {
                                    document
                                        .get_str("user")
                                        .or_else(|_| document.get_str("role"))
                                        .or_else(|_| document.get_str("_id"))
                                        .ok()
                                })
                                .map(ToString::to_string)
                                .unwrap_or_else(|| format!("{kind_name}-{index}"));
                            DocumentAdminObjectInfo {
                                database: database_name.clone(),
                                kind: kind_name.clone(),
                                name,
                                details_json,
                                unavailable_reason: None,
                            }
                        })
                        .collect();
                }

                vec![DocumentAdminObjectInfo {
                    database: database_name,
                    kind: kind_name,
                    name: command_name,
                    details_json: Self::document_to_json(&document),
                    unavailable_reason: None,
                }]
            }
            Err(error) => vec![DocumentAdminObjectInfo {
                database: database_name,
                kind: kind_name,
                name: command_name,
                details_json: serde_json::Value::Null,
                unavailable_reason: Some(error.to_string()),
            }],
        }
    }

    pub(super) async fn list_search_index_metadata(
        &self,
        database: &str,
        collections: &[DocumentCollectionInfo],
    ) -> (Vec<DocumentAdminObjectInfo>, Vec<DocumentAdminObjectInfo>) {
        let database_handle = self.client.database(database);
        let mut search_indexes = Vec::new();
        let mut vector_indexes = Vec::new();
        let mut unavailable_reason = None;

        for collection in collections
            .iter()
            .filter(|collection| collection.collection_type != "view")
        {
            let collection_handle = database_handle.collection::<Document>(&collection.name);
            let collection_name = collection.name.clone();
            let result = run_mongodb_future(async move {
                let cursor = collection_handle
                    .aggregate(vec![bson::doc! { "$listSearchIndexes": {} }])
                    .await
                    .map_err(|error| ZqlzError::Driver(error.to_string()))?;
                cursor
                    .try_collect::<Vec<Document>>()
                    .await
                    .map_err(|error| ZqlzError::Driver(error.to_string()))
            })
            .await;

            match result {
                Ok(index_documents) => {
                    for mut document in index_documents {
                        document.insert("collection", collection.name.clone());
                        let is_vector = document
                            .get_str("type")
                            .map(|value| value.eq_ignore_ascii_case("vectorSearch"))
                            .unwrap_or(false)
                            || document
                                .get("latestDefinition")
                                .or_else(|| document.get("definition"))
                                .is_some_and(Self::contains_vector_search_field);
                        let index_name = document
                            .get_str("name")
                            .map(ToString::to_string)
                            .unwrap_or_else(|_| "<unnamed>".to_string());
                        let object = DocumentAdminObjectInfo {
                            database: database.to_string(),
                            kind: if is_vector {
                                "vector_search_index".to_string()
                            } else {
                                "search_index".to_string()
                            },
                            name: format!("{}.{}", collection.name, index_name),
                            details_json: Self::document_to_json(&document),
                            unavailable_reason: None,
                        };
                        if is_vector {
                            vector_indexes.push(object);
                        } else {
                            search_indexes.push(object);
                        }
                    }
                }
                Err(error) => {
                    if unavailable_reason.is_none() {
                        unavailable_reason = Some(format!("{}: {}", collection_name, error));
                    }
                }
            }
        }

        if let Some(reason) = unavailable_reason
            && search_indexes.is_empty()
            && vector_indexes.is_empty()
        {
            let (search_unavailable, vector_unavailable) =
                Self::search_index_unavailable_rows(database, reason);
            search_indexes.push(search_unavailable);
            vector_indexes.push(vector_unavailable);
        }

        (search_indexes, vector_indexes)
    }

    pub(super) fn search_index_unavailable_rows(
        database: &str,
        reason: String,
    ) -> (DocumentAdminObjectInfo, DocumentAdminObjectInfo) {
        (
            DocumentAdminObjectInfo {
                database: database.to_string(),
                kind: "search_index".to_string(),
                name: "$listSearchIndexes".to_string(),
                details_json: serde_json::Value::Null,
                unavailable_reason: Some(reason.clone()),
            },
            DocumentAdminObjectInfo {
                database: database.to_string(),
                kind: "vector_search_index".to_string(),
                name: "$listSearchIndexes vector".to_string(),
                details_json: serde_json::Value::Null,
                unavailable_reason: Some(reason),
            },
        )
    }

    pub(super) async fn load_config_documents(
        &self,
        collection_name: &str,
        filter: Document,
    ) -> Result<Vec<Document>> {
        let collection = self
            .client
            .database("config")
            .collection::<Document>(collection_name);
        run_mongodb_future(async move {
            let cursor = collection
                .find(filter)
                .await
                .map_err(|error| ZqlzError::Driver(error.to_string()))?;
            cursor
                .try_collect::<Vec<Document>>()
                .await
                .map_err(|error| ZqlzError::Driver(error.to_string()))
        })
        .await
    }

    pub(super) async fn list_sharding_metadata(
        &self,
        database: &str,
    ) -> Vec<DocumentAdminObjectInfo> {
        let namespace_regex = format!("^{}\\.", Self::regex_escape(database));
        let mut objects = Vec::new();

        let collection_documents = match self
            .load_config_documents(
                "collections",
                bson::doc! { "_id": { "$regex": namespace_regex.clone() } },
            )
            .await
        {
            Ok(documents) => documents,
            Err(error) => {
                objects.push(Self::unavailable_admin_object(
                    database,
                    "sharding",
                    "config.collections",
                    error,
                ));
                Vec::new()
            }
        };

        let collection_uuids = collection_documents
            .iter()
            .filter_map(|document| document.get("uuid").cloned())
            .collect::<Vec<_>>();
        let collection_uuid_names = collection_documents
            .iter()
            .filter_map(|document| {
                Some((
                    document.get("uuid")?.clone(),
                    document.get_str("_id").ok()?.to_string(),
                ))
            })
            .collect::<Vec<_>>();
        for (index, document) in collection_documents.into_iter().enumerate() {
            objects.push(Self::config_document_to_admin_object(
                database,
                "sharded_collection",
                Self::sharded_collection_name(&document, index),
                document,
            ));
        }

        let chunk_filter = if collection_uuids.is_empty() {
            bson::doc! { "ns": { "$regex": namespace_regex.clone() } }
        } else {
            bson::doc! { "uuid": { "$in": collection_uuids } }
        };
        match self.load_config_documents("chunks", chunk_filter).await {
            Ok(chunk_documents) => {
                for (index, document) in chunk_documents.into_iter().enumerate() {
                    let namespace = document.get("uuid").and_then(|uuid| {
                        collection_uuid_names
                            .iter()
                            .find_map(|(collection_uuid, name)| {
                                (collection_uuid == uuid).then_some(name.as_str())
                            })
                    });
                    objects.push(Self::config_document_to_admin_object(
                        database,
                        "chunk",
                        Self::sharding_chunk_name(&document, namespace, index),
                        document,
                    ));
                }
            }
            Err(error) => objects.push(Self::unavailable_admin_object(
                database,
                "sharding",
                "config.chunks",
                error,
            )),
        }

        match self
            .load_config_documents(
                "tags",
                bson::doc! { "ns": { "$regex": namespace_regex.clone() } },
            )
            .await
        {
            Ok(tag_documents) => {
                for (index, document) in tag_documents.into_iter().enumerate() {
                    objects.push(Self::config_document_to_admin_object(
                        database,
                        "zone",
                        Self::sharding_zone_name(&document, index),
                        document,
                    ));
                }
            }
            Err(error) => objects.push(Self::unavailable_admin_object(
                database,
                "sharding",
                "config.tags",
                error,
            )),
        }

        objects
    }

    pub(super) fn config_document_to_admin_object(
        database: &str,
        kind: &str,
        name: String,
        document: Document,
    ) -> DocumentAdminObjectInfo {
        DocumentAdminObjectInfo {
            database: database.to_string(),
            kind: kind.to_string(),
            name,
            details_json: Self::document_to_json(&document),
            unavailable_reason: None,
        }
    }

    pub(super) fn unavailable_admin_object(
        database: &str,
        kind: &str,
        name: &str,
        error: ZqlzError,
    ) -> DocumentAdminObjectInfo {
        DocumentAdminObjectInfo {
            database: database.to_string(),
            kind: kind.to_string(),
            name: name.to_string(),
            details_json: serde_json::Value::Null,
            unavailable_reason: Some(error.to_string()),
        }
    }

    pub(super) fn regex_escape(value: &str) -> String {
        value
            .chars()
            .flat_map(|character| match character {
                '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '{' | '}' | '[' | ']' | '^'
                | '$' => vec!['\\', character],
                character => vec![character],
            })
            .collect()
    }

    pub(super) fn sharded_collection_name(document: &Document, index: usize) -> String {
        document
            .get_str("_id")
            .map(ToString::to_string)
            .unwrap_or_else(|_| format!("sharded-collection-{index}"))
    }

    pub(super) fn sharding_chunk_name(
        document: &Document,
        namespace: Option<&str>,
        index: usize,
    ) -> String {
        if let Ok(namespace) = document.get_str("ns") {
            return format!("{namespace} chunk {index}");
        }
        if let Some(namespace) = namespace {
            return format!("{namespace} chunk {index}");
        }
        if let Ok(id) = document.get_str("_id") {
            return id.to_string();
        }
        if let Some(uuid) = document.get("uuid") {
            return format!("chunk {index} {uuid:?}");
        }
        format!("chunk-{index}")
    }

    pub(super) fn sharding_zone_name(document: &Document, index: usize) -> String {
        match (document.get_str("ns"), document.get_str("tag")) {
            (Ok(namespace), Ok(tag)) => format!("{namespace} zone {tag}"),
            (Ok(namespace), Err(_)) => format!("{namespace} zone {index}"),
            (Err(_), Ok(tag)) => tag.to_string(),
            (Err(_), Err(_)) => format!("zone-{index}"),
        }
    }

    pub(super) fn gridfs_buckets_from_collections(
        database: &str,
        collections: &[DocumentCollectionInfo],
    ) -> Vec<DocumentGridFsBucketInfo> {
        let mut gridfs_buckets = Vec::new();
        for files_collection in collections
            .iter()
            .filter(|collection| collection.name.ends_with(".files"))
        {
            let bucket = files_collection
                .name
                .strip_suffix(".files")
                .unwrap_or(&files_collection.name);
            let chunks_collection = format!("{bucket}.chunks");
            if collections
                .iter()
                .any(|collection| collection.name == chunks_collection)
            {
                gridfs_buckets.push(DocumentGridFsBucketInfo {
                    database: database.to_string(),
                    name: bucket.to_string(),
                    files_collection: files_collection.name.clone(),
                    chunks_collection,
                    file_count: files_collection.document_count,
                    size_bytes: files_collection.size_bytes,
                });
            }
        }
        gridfs_buckets
    }

    pub(super) fn server_metadata_commands() -> Vec<Document> {
        vec![
            bson::doc! { "buildInfo": 1 },
            bson::doc! { "getParameter": 1, "featureCompatibilityVersion": 1 },
            bson::doc! { "serverStatus": 1 },
            bson::doc! { "getDefaultRWConcern": 1 },
            bson::doc! { "replSetGetStatus": 1 },
        ]
    }

    pub(super) fn contains_vector_search_field(value: &Bson) -> bool {
        match value {
            Bson::Document(document) => {
                document
                    .get_str("type")
                    .map(|value| value.eq_ignore_ascii_case("vector"))
                    .unwrap_or(false)
                    || document.values().any(Self::contains_vector_search_field)
            }
            Bson::Array(values) => values.iter().any(Self::contains_vector_search_field),
            _ => false,
        }
    }

    pub(super) fn parse_document_json(json: Option<&str>) -> Result<Document> {
        match json.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) => serde_json::from_str::<Document>(value).map_err(|error| {
                ZqlzError::Driver(format!("Invalid MongoDB document JSON: {}", error))
            }),
            None => Ok(Document::new()),
        }
    }

    pub(super) fn parse_required_document_json(json: &str, context: &str) -> Result<Document> {
        let value = serde_json::from_str::<serde_json::Value>(json).map_err(|error| {
            ZqlzError::Driver(format!("Invalid MongoDB {} JSON: {}", context, error))
        })?;
        match Self::json_value_to_bson(value)? {
            Bson::Document(document) => Ok(document),
            _ => Err(ZqlzError::Driver(format!(
                "Invalid MongoDB {} JSON: expected a JSON object",
                context
            ))),
        }
    }

    pub(super) fn json_value_to_bson(value: serde_json::Value) -> Result<Bson> {
        match value {
            serde_json::Value::Null => Ok(Bson::Null),
            serde_json::Value::Bool(value) => Ok(Bson::Boolean(value)),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    Ok(Bson::Int64(value))
                } else if let Some(value) = value.as_u64() {
                    i64::try_from(value)
                        .map(Bson::Int64)
                        .map_err(|_| ZqlzError::Driver("MongoDB number is too large".to_string()))
                } else if let Some(value) = value.as_f64() {
                    Ok(Bson::Double(value))
                } else {
                    Err(ZqlzError::Driver("Invalid MongoDB number".to_string()))
                }
            }
            serde_json::Value::String(value) => Ok(Bson::String(value)),
            serde_json::Value::Array(values) => values
                .into_iter()
                .map(Self::json_value_to_bson)
                .collect::<Result<Vec<_>>>()
                .map(Bson::Array),
            serde_json::Value::Object(mut object) => {
                if object.len() == 1
                    && let Some(serde_json::Value::String(value)) = object.remove("$oid")
                {
                    return ObjectId::parse_str(value.trim())
                        .map(Bson::ObjectId)
                        .map_err(|error| {
                            ZqlzError::Driver(format!("Invalid MongoDB ObjectId: {}", error))
                        });
                }

                let mut document = Document::new();
                for (key, value) in object {
                    document.insert(key, Self::json_value_to_bson(value)?);
                }
                Ok(Bson::Document(document))
            }
        }
    }

    pub(super) fn format_mongodb_write_error(operation: &str, error: &MongoError) -> ZqlzError {
        ZqlzError::Driver(format!(
            "MongoDB {} failed: {}",
            operation,
            Self::mongodb_error_summary(error)
        ))
    }

    pub(super) fn mongodb_error_summary(error: &MongoError) -> String {
        match error.kind.as_ref() {
            ErrorKind::Write(WriteFailure::WriteError(write_error)) if write_error.code == 121 => {
                Self::mongodb_validation_summary(&write_error.message, write_error.details.as_ref())
            }
            ErrorKind::Write(WriteFailure::WriteError(write_error)) => write_error.message.clone(),
            ErrorKind::Write(WriteFailure::WriteConcernError(write_error)) => {
                write_error.message.clone()
            }
            ErrorKind::Command(command_error) => command_error.message.clone(),
            _ => error.to_string(),
        }
    }

    pub(super) fn mongodb_validation_summary(message: &str, details: Option<&Document>) -> String {
        let mut reasons = BTreeSet::new();
        if let Some(details) = details {
            Self::collect_mongodb_validation_reasons(
                &Bson::Document(details.clone()),
                &mut reasons,
            );
        }

        if reasons.is_empty() {
            message.to_string()
        } else {
            format!(
                "{}: {}",
                message,
                reasons.into_iter().collect::<Vec<_>>().join("; ")
            )
        }
    }

    pub(super) fn collect_mongodb_validation_reasons(value: &Bson, reasons: &mut BTreeSet<String>) {
        match value {
            Bson::Document(document) => {
                if let Some(Bson::Array(missing)) = document.get("missingProperties") {
                    let missing = missing.iter().filter_map(Bson::as_str).collect::<Vec<_>>();
                    if !missing.is_empty() {
                        reasons.insert(format!("missing required fields: {}", missing.join(", ")));
                    }
                }

                if let Some(Bson::String(property_name)) = document.get("propertyName") {
                    if let Some(reason) = Self::validation_reason_for_document(document) {
                        reasons.insert(format!("{}: {}", property_name, reason));
                    }
                    if let Some(Bson::Array(details)) = document.get("details") {
                        for detail in details {
                            if let Bson::Document(detail) = detail
                                && let Some(reason) = Self::validation_reason_for_document(detail)
                            {
                                reasons.insert(format!("{} {}", property_name, reason));
                            }
                        }
                    }
                }

                for value in document.values() {
                    Self::collect_mongodb_validation_reasons(value, reasons);
                }
            }
            Bson::Array(values) => {
                for value in values {
                    Self::collect_mongodb_validation_reasons(value, reasons);
                }
            }
            _ => {}
        }
    }

    pub(super) fn validation_reason_for_document(document: &Document) -> Option<String> {
        let reason = document.get_str("reason").ok()?;
        let expected_type = document
            .get_document("specifiedAs")
            .ok()
            .and_then(|specified_as| specified_as.get_str("bsonType").ok());
        let considered_type = document.get_str("consideredType").ok();
        match (expected_type, considered_type) {
            (Some(expected), Some(actual)) => {
                Some(format!("expected {} but got {}", expected, actual))
            }
            _ => Some(reason.to_string()),
        }
    }

    pub(super) fn parse_pipeline_json(json: &str) -> Result<Vec<Document>> {
        serde_json::from_str::<Vec<Document>>(json).map_err(|error| {
            ZqlzError::Driver(format!(
                "MongoDB aggregate pipeline must be a JSON array: {}",
                error
            ))
        })
    }

    pub(super) fn parse_id_json(json: &str) -> Result<Bson> {
        if let Ok(object_id) = bson::oid::ObjectId::parse_str(json) {
            return Ok(Bson::ObjectId(object_id));
        }

        if let Ok(value) = serde_json::from_str::<serde_json::Value>(json) {
            if let serde_json::Value::String(value) = &value
                && let Ok(object_id) = bson::oid::ObjectId::parse_str(value)
            {
                return Ok(Bson::ObjectId(object_id));
            }

            return bson::to_bson(&value).map_err(|error| {
                ZqlzError::Driver(format!("Invalid MongoDB _id BSON: {}", error))
            });
        }

        serde_json::from_str::<Bson>(json).or_else(|_| Ok(Bson::String(json.to_string())))
    }

    pub(super) fn documents_to_query_result(
        documents: Vec<Document>,
        execution_time_ms: u64,
    ) -> QueryResult {
        let mut column_names = Vec::<String>::new();
        for document in &documents {
            for key in document.keys() {
                if !column_names.contains(key) {
                    column_names.push(key.clone());
                }
            }
        }

        let columns = column_names
            .iter()
            .enumerate()
            .map(|(ordinal, name)| ColumnMeta {
                name: name.clone(),
                data_type: Self::inferred_bson_column_type(&documents, name),
                nullable: true,
                ordinal,
                max_length: None,
                precision: None,
                scale: None,
                auto_increment: false,
                default_value: None,
                comment: None,
                enum_values: None,
            })
            .collect();

        let rows = documents
            .into_iter()
            .map(|document| {
                let values = column_names
                    .iter()
                    .map(|name| {
                        document
                            .get(name)
                            .map(Self::bson_to_value)
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                Row::new(column_names.clone(), values)
            })
            .collect::<Vec<_>>();

        QueryResult {
            id: Uuid::new_v4(),
            columns,
            total_rows: Some(rows.len() as u64),
            rows,
            is_estimated_total: false,
            affected_rows: 0,
            execution_time_ms,
            warnings: Vec::new(),
        }
    }
}
