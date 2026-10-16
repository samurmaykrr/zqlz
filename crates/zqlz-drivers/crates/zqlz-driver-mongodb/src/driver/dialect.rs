use super::*;

/// Create MongoDB dialect information
///
/// MongoDB uses a JSON/BSON-based query language rather than SQL.
/// This dialect info describes the MongoDB operations, aggregation stages,
/// and data types available.
pub fn mongodb_dialect() -> DialectInfo {
    DialectInfo {
        id: Cow::Borrowed("mongodb"),
        display_name: Cow::Borrowed("MongoDB Query Language"),

        // MongoDB operations as "keywords"
        keywords: vec![
            // Query operators
            keyword("$eq", KeywordCategory::Operator),
            keyword("$ne", KeywordCategory::Operator),
            keyword("$gt", KeywordCategory::Operator),
            keyword("$gte", KeywordCategory::Operator),
            keyword("$lt", KeywordCategory::Operator),
            keyword("$lte", KeywordCategory::Operator),
            keyword("$in", KeywordCategory::Operator),
            keyword("$nin", KeywordCategory::Operator),
            keyword("$and", KeywordCategory::Operator),
            keyword("$or", KeywordCategory::Operator),
            keyword("$not", KeywordCategory::Operator),
            keyword("$nor", KeywordCategory::Operator),
            keyword("$exists", KeywordCategory::Operator),
            keyword("$type", KeywordCategory::Operator),
            keyword("$regex", KeywordCategory::Operator),
            keyword("$text", KeywordCategory::Operator),
            keyword("$where", KeywordCategory::Operator),
            keyword("$all", KeywordCategory::Operator),
            keyword("$elemMatch", KeywordCategory::Operator),
            keyword("$size", KeywordCategory::Operator),
            // Update operators
            keyword("$set", KeywordCategory::Operator),
            keyword("$unset", KeywordCategory::Operator),
            keyword("$inc", KeywordCategory::Operator),
            keyword("$mul", KeywordCategory::Operator),
            keyword("$rename", KeywordCategory::Operator),
            keyword("$min", KeywordCategory::Operator),
            keyword("$max", KeywordCategory::Operator),
            keyword("$currentDate", KeywordCategory::Operator),
            keyword("$addToSet", KeywordCategory::Operator),
            keyword("$pop", KeywordCategory::Operator),
            keyword("$pull", KeywordCategory::Operator),
            keyword("$push", KeywordCategory::Operator),
            keyword("$each", KeywordCategory::Operator),
            keyword("$slice", KeywordCategory::Operator),
            keyword("$sort", KeywordCategory::Operator),
            keyword("$position", KeywordCategory::Operator),
            // Aggregation stages - use DatabaseSpecific since there's no Statement category
            keyword("$match", KeywordCategory::DatabaseSpecific),
            keyword("$project", KeywordCategory::DatabaseSpecific),
            keyword("$group", KeywordCategory::DatabaseSpecific),
            keyword("$limit", KeywordCategory::DatabaseSpecific),
            keyword("$skip", KeywordCategory::DatabaseSpecific),
            keyword("$unwind", KeywordCategory::DatabaseSpecific),
            keyword("$lookup", KeywordCategory::DatabaseSpecific),
            keyword("$graphLookup", KeywordCategory::DatabaseSpecific),
            keyword("$facet", KeywordCategory::DatabaseSpecific),
            keyword("$bucket", KeywordCategory::DatabaseSpecific),
            keyword("$bucketAuto", KeywordCategory::DatabaseSpecific),
            keyword("$addFields", KeywordCategory::DatabaseSpecific),
            keyword("$replaceRoot", KeywordCategory::DatabaseSpecific),
            keyword("$replaceWith", KeywordCategory::DatabaseSpecific),
            keyword("$merge", KeywordCategory::DatabaseSpecific),
            keyword("$out", KeywordCategory::DatabaseSpecific),
            keyword("$count", KeywordCategory::DatabaseSpecific),
            keyword("$sample", KeywordCategory::DatabaseSpecific),
            keyword("$redact", KeywordCategory::DatabaseSpecific),
            keyword("$geoNear", KeywordCategory::DatabaseSpecific),
            keyword("$setWindowFields", KeywordCategory::DatabaseSpecific),
            keyword("$densify", KeywordCategory::DatabaseSpecific),
            keyword("$fill", KeywordCategory::DatabaseSpecific),
            // Commands - use Dql for query commands, Dml for modification commands
            keyword("find", KeywordCategory::Dql),
            keyword("findOne", KeywordCategory::Dql),
            keyword("insert", KeywordCategory::Dml),
            keyword("insertOne", KeywordCategory::Dml),
            keyword("insertMany", KeywordCategory::Dml),
            keyword("update", KeywordCategory::Dml),
            keyword("updateOne", KeywordCategory::Dml),
            keyword("updateMany", KeywordCategory::Dml),
            keyword("delete", KeywordCategory::Dml),
            keyword("deleteOne", KeywordCategory::Dml),
            keyword("deleteMany", KeywordCategory::Dml),
            keyword("aggregate", KeywordCategory::Dql),
            keyword("count", KeywordCategory::Dql),
            keyword("distinct", KeywordCategory::Dql),
            keyword("createIndex", KeywordCategory::Ddl),
            keyword("dropIndex", KeywordCategory::Ddl),
            keyword("createCollection", KeywordCategory::Ddl),
            keyword("drop", KeywordCategory::Ddl),
            keyword("ping", KeywordCategory::DatabaseSpecific),
            keyword("listCollections", KeywordCategory::DatabaseSpecific),
            keyword("listDatabases", KeywordCategory::DatabaseSpecific),
            keyword("listIndexes", KeywordCategory::DatabaseSpecific),
        ],

        // MongoDB aggregation functions
        functions: vec![
            // Accumulator expressions
            function("$sum", FunctionCategory::Aggregate, "Sum of numeric values"),
            function(
                "$avg",
                FunctionCategory::Aggregate,
                "Average of numeric values",
            ),
            function("$min", FunctionCategory::Aggregate, "Minimum value"),
            function("$max", FunctionCategory::Aggregate, "Maximum value"),
            function(
                "$first",
                FunctionCategory::Aggregate,
                "First value in group",
            ),
            function("$last", FunctionCategory::Aggregate, "Last value in group"),
            function("$push", FunctionCategory::Aggregate, "Array of values"),
            function(
                "$addToSet",
                FunctionCategory::Aggregate,
                "Array of unique values",
            ),
            function(
                "$stdDevPop",
                FunctionCategory::Aggregate,
                "Population standard deviation",
            ),
            function(
                "$stdDevSamp",
                FunctionCategory::Aggregate,
                "Sample standard deviation",
            ),
            // String functions
            function("$concat", FunctionCategory::String, "Concatenate strings"),
            function("$substr", FunctionCategory::String, "Substring extraction"),
            function("$toLower", FunctionCategory::String, "Convert to lowercase"),
            function("$toUpper", FunctionCategory::String, "Convert to uppercase"),
            function("$trim", FunctionCategory::String, "Trim whitespace"),
            function("$split", FunctionCategory::String, "Split string to array"),
            function(
                "$strLenCP",
                FunctionCategory::String,
                "String length in code points",
            ),
            function(
                "$regexMatch",
                FunctionCategory::String,
                "Regex pattern match",
            ),
            function("$regexFind", FunctionCategory::String, "Find regex match"),
            function(
                "$regexFindAll",
                FunctionCategory::String,
                "Find all regex matches",
            ),
            // Date functions
            function(
                "$dateToString",
                FunctionCategory::DateTime,
                "Format date as string",
            ),
            function(
                "$dateFromString",
                FunctionCategory::DateTime,
                "Parse string to date",
            ),
            function(
                "$dayOfMonth",
                FunctionCategory::DateTime,
                "Day of month (1-31)",
            ),
            function(
                "$dayOfWeek",
                FunctionCategory::DateTime,
                "Day of week (1-7)",
            ),
            function(
                "$dayOfYear",
                FunctionCategory::DateTime,
                "Day of year (1-366)",
            ),
            function("$month", FunctionCategory::DateTime, "Month (1-12)"),
            function("$year", FunctionCategory::DateTime, "Year"),
            function("$hour", FunctionCategory::DateTime, "Hour (0-23)"),
            function("$minute", FunctionCategory::DateTime, "Minute (0-59)"),
            function("$second", FunctionCategory::DateTime, "Second (0-59)"),
            function(
                "$dateDiff",
                FunctionCategory::DateTime,
                "Difference between dates",
            ),
            function("$dateAdd", FunctionCategory::DateTime, "Add to date"),
            function(
                "$dateSubtract",
                FunctionCategory::DateTime,
                "Subtract from date",
            ),
            // Math functions - use Numeric category
            function("$abs", FunctionCategory::Numeric, "Absolute value"),
            function("$ceil", FunctionCategory::Numeric, "Ceiling"),
            function("$floor", FunctionCategory::Numeric, "Floor"),
            function("$round", FunctionCategory::Numeric, "Round"),
            function("$sqrt", FunctionCategory::Numeric, "Square root"),
            function("$pow", FunctionCategory::Numeric, "Power"),
            function("$log", FunctionCategory::Numeric, "Logarithm"),
            function("$log10", FunctionCategory::Numeric, "Base 10 logarithm"),
            function("$exp", FunctionCategory::Numeric, "Exponential"),
            function("$mod", FunctionCategory::Numeric, "Modulo"),
            function("$add", FunctionCategory::Numeric, "Addition"),
            function("$subtract", FunctionCategory::Numeric, "Subtraction"),
            function("$multiply", FunctionCategory::Numeric, "Multiplication"),
            function("$divide", FunctionCategory::Numeric, "Division"),
            // Array functions - use Array category
            function("$arrayElemAt", FunctionCategory::Array, "Element at index"),
            function(
                "$concatArrays",
                FunctionCategory::Array,
                "Concatenate arrays",
            ),
            function("$filter", FunctionCategory::Array, "Filter array elements"),
            function("$map", FunctionCategory::Array, "Map over array"),
            function("$reduce", FunctionCategory::Array, "Reduce array to value"),
            function("$reverseArray", FunctionCategory::Array, "Reverse array"),
            function("$size", FunctionCategory::Array, "Array size"),
            function("$slice", FunctionCategory::Array, "Array slice"),
            function("$zip", FunctionCategory::Array, "Zip arrays together"),
            function("$in", FunctionCategory::Array, "Element in array"),
            function("$isArray", FunctionCategory::Array, "Check if array"),
            // Type conversion
            function("$toInt", FunctionCategory::Conversion, "Convert to integer"),
            function("$toLong", FunctionCategory::Conversion, "Convert to long"),
            function(
                "$toDouble",
                FunctionCategory::Conversion,
                "Convert to double",
            ),
            function(
                "$toDecimal",
                FunctionCategory::Conversion,
                "Convert to decimal",
            ),
            function(
                "$toString",
                FunctionCategory::Conversion,
                "Convert to string",
            ),
            function(
                "$toObjectId",
                FunctionCategory::Conversion,
                "Convert to ObjectId",
            ),
            function("$toDate", FunctionCategory::Conversion, "Convert to date"),
            function(
                "$toBool",
                FunctionCategory::Conversion,
                "Convert to boolean",
            ),
            function("$type", FunctionCategory::Conversion, "Get BSON type"),
            // Conditional
            function(
                "$cond",
                FunctionCategory::Conditional,
                "Conditional expression",
            ),
            function("$ifNull", FunctionCategory::Conditional, "Null coalesce"),
            function(
                "$switch",
                FunctionCategory::Conditional,
                "Switch/case expression",
            ),
        ],

        // MongoDB data types
        data_types: vec![
            dtype("Double", DataTypeCategory::Float, "64-bit floating point"),
            dtype("String", DataTypeCategory::String, "UTF-8 string"),
            dtype("Object", DataTypeCategory::Other, "Embedded document"),
            dtype("Array", DataTypeCategory::Array, "Array of values"),
            dtype("BinData", DataTypeCategory::Binary, "Binary data"),
            dtype(
                "ObjectId",
                DataTypeCategory::Other,
                "12-byte unique identifier",
            ),
            dtype("Boolean", DataTypeCategory::Boolean, "True or false"),
            dtype("Date", DataTypeCategory::DateTime, "UTC datetime"),
            dtype("Null", DataTypeCategory::Other, "Null value"),
            dtype("Regex", DataTypeCategory::Other, "Regular expression"),
            dtype("JavaScript", DataTypeCategory::Other, "JavaScript code"),
            dtype("Int32", DataTypeCategory::Integer, "32-bit integer"),
            dtype(
                "Timestamp",
                DataTypeCategory::DateTime,
                "Internal timestamp",
            ),
            dtype("Int64", DataTypeCategory::Integer, "64-bit integer"),
            dtype("Decimal128", DataTypeCategory::Decimal, "128-bit decimal"),
            dtype("MinKey", DataTypeCategory::Other, "Minimum BSON value"),
            dtype("MaxKey", DataTypeCategory::Other, "Maximum BSON value"),
        ],

        table_options: Vec::new(),
        auto_increment: None, // MongoDB uses ObjectId for unique IDs

        identifier_quote: '"',
        string_quote: '"', // JSON uses double quotes for strings
        case_sensitive_identifiers: true,
        statement_terminator: ';',

        comment_styles: CommentStyles {
            line_comment: Some(Cow::Borrowed("//")),
            block_comment_start: Some(Cow::Borrowed("/*")),
            block_comment_end: Some(Cow::Borrowed("*/")),
        },

        // MongoDB uses .explain() method on queries, not SQL EXPLAIN syntax
        explain_config: ExplainConfig {
            explain_format: Cow::Borrowed("{\"explain\": {sql}}"),
            query_plan_format: None,
            analyze_format: None,
            explain_description: Cow::Borrowed(
                "Use .explain() method on cursor or add explain:true to command",
            ),
            query_plan_description: None,
            analyze_is_safe: true,
        },
    }
}

/// Helper to create a keyword
fn keyword(name: &'static str, category: KeywordCategory) -> KeywordInfo {
    KeywordInfo::new(name, category)
}

/// Helper to create a function
fn function(
    name: &'static str,
    category: FunctionCategory,
    description: &'static str,
) -> SqlFunctionInfo {
    SqlFunctionInfo {
        name: Cow::Borrowed(name),
        category,
        description: Some(Cow::Borrowed(description)),
        signatures: Vec::new(),
        return_type: None,
    }
}

/// Helper to create a data type
fn dtype(
    name: &'static str,
    category: DataTypeCategory,
    description: &'static str,
) -> DataTypeInfo {
    DataTypeInfo {
        name: Cow::Borrowed(name),
        aliases: Vec::new(),
        category,
        accepts_length: false,
        accepts_scale: false,
        default_length: None,
        max_length: None,
        description: Some(Cow::Borrowed(description)),
        example: None,
    }
}
