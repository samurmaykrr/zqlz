; ClickHouse SQL dialect highlights
; ClickHouse extends standard SQL with specific engine settings, table functions, and data types
; Uses tree_sitter_sequel grammar with ClickHouse-specific keyword categorization

; Inherit base SQL highlighting patterns
(object_reference
  name: (identifier) @type)

(invocation
  (object_reference
    name: (identifier) @function.call))

(relation
  alias: (identifier) @variable)

(field
  name: (identifier) @property)

(term
  alias: (identifier) @variable)

(literal) @string
(comment) @comment @spell
(marginalia) @comment

((literal) @number
   (#match? @number "^[-+]?%d+$"))

((literal) @float
  (#match? @float "^[-+]?%d*\.%d*$"))

(parameter) @parameter

[
 (keyword_true)
 (keyword_false)
] @boolean

; ClickHouse-specific table engines and functions
[
  "MergeTree"
  "ReplacingMergeTree"
  "SummingMergeTree"
  "AggregatingMergeTree"
  "CollapsingMergeTree"
  "VersionedCollapsingMergeTree"
  "GraphiteMergeTree"
  "Distributed"
  "Replicated"
  "Memory"
  "Buffer"
  "File"
  "URL"
  "HDFS"
  "S3"
  "Kafka"
  "RabbitMQ"
  "JDBC"
  "ODBC"
  "MaterializedView"
] @type.builtin

; ClickHouse-specific settings and attributes
[
  "ENGINE"
  "PARTITION"
  "SAMPLE"
  "SETTINGS"
  "TTL"
  "CODEC"
  "FINAL"
  "PREWHERE"
  "ARRAY JOIN"
  "GLOBAL"
  "LOCAL"
  "TOTALS"
  "EXTREMES"
  "FORMAT"
  "INTO OUTFILE"
  "OPTIMIZE"
  "SYSTEM"
  "QUOTA"
] @attribute

; ClickHouse-specific data types
[
  "UInt8"
  "UInt16"
  "UInt32"
  "UInt64"
  "UInt128"
  "UInt256"
  "Int8"
  "Int16"
  "Int32"
  "Int64"
  "Int128"
  "Int256"
  "Float32"
  "Float64"
  "Decimal"
  "Decimal32"
  "Decimal64"
  "Decimal128"
  "Decimal256"
  "String"
  "FixedString"
  "UUID"
  "Date"
  "Date32"
  "DateTime"
  "DateTime64"
  "Enum"
  "Enum8"
  "Enum16"
  "Array"
  "Tuple"
  "Nested"
  "Nullable"
  "LowCardinality"
  "Map"
  "IPv4"
  "IPv6"
  "Point"
  "Ring"
  "Polygon"
  "MultiPolygon"
  "SimpleAggregateFunction"
  "AggregateFunction"
] @type.builtin

; Core SQL keywords
[
  (keyword_select)
  (keyword_from)
  (keyword_where)
  (keyword_join)
  (keyword_left)
  (keyword_right)
  (keyword_inner)
  (keyword_outer)
  (keyword_full)
  (keyword_cross)
  (keyword_on)
  (keyword_using)
  (keyword_order)
  (keyword_group)
  (keyword_by)
  (keyword_having)
  (keyword_limit)
  (keyword_offset)
  (keyword_union)
  (keyword_intersect)
  (keyword_except)
  (keyword_distinct)
  (keyword_all)
  (keyword_as)
  (keyword_with)
  (keyword_case)
  (keyword_when)
  (keyword_then)
  (keyword_else)
  (keyword_end)
  (keyword_insert)
  (keyword_into)
  (keyword_values)
  (keyword_update)
  (keyword_set)
  (keyword_delete)
  (keyword_create)
  (keyword_alter)
  (keyword_drop)
  (keyword_table)
  (keyword_database)
  (keyword_view)
  (keyword_index)
  (keyword_primary)
  (keyword_key)
  (keyword_foreign)
  (keyword_references)
  (keyword_constraint)
  (keyword_default)
  (keyword_null)
  (keyword_not)
  (keyword_unique)
  (keyword_check)
  (keyword_truncate)
  (keyword_explain)
  (keyword_analyze)
] @keyword

; Operators
[
  (keyword_and)
  (keyword_or)
  (keyword_in)
  (keyword_exists)
  (keyword_between)
  (keyword_like)
  (keyword_is)
] @keyword.operator

; Symbolic operators
[
  "+"
  "-"
  "*"
  "/"
  "%"
  "="
  "<"
  "<="
  "!="
  ">="
  ">"
  "<>"
  ":="
  (op_other)
] @operator

; Punctuation
[
  "("
  ")"
  "["
  "]"
] @punctuation.bracket

[
  ";"
  ","
  "."
] @punctuation.delimiter
