; MongoDB query syntax highlights
; MongoDB uses JSON-like query structure with special operators and aggregation pipeline
; This leverages JSON grammar for structure with MongoDB-specific extensions

; MongoDB operators (starts with $)
((identifier) @operator
  (#match? @operator "^\\$"))

; Common MongoDB operators
[
  "$eq"
  "$ne"
  "$gt"
  "$gte"
  "$lt"
  "$lte"
  "$in"
  "$nin"
  "$and"
  "$or"
  "$not"
  "$nor"
  "$exists"
  "$type"
  "$regex"
  "$options"
  "$match"
  "$group"
  "$sort"
  "$limit"
  "$skip"
  "$project"
  "$unwind"
  "$lookup"
  "$addFields"
  "$count"
  "$sum"
  "$avg"
  "$min"
  "$max"
  "$push"
  "$pull"
  "$set"
  "$unset"
  "$inc"
  "$mul"
  "$rename"
  "$setOnInsert"
  "$currentDate"
] @operator

; MongoDB collection methods
[
  "find"
  "findOne"
  "insert"
  "insertOne"
  "insertMany"
  "update"
  "updateOne"
  "updateMany"
  "delete"
  "deleteOne"
  "deleteMany"
  "aggregate"
  "count"
  "distinct"
  "drop"
  "createIndex"
  "dropIndex"
  "explain"
] @function

; MongoDB database methods
[
  "use"
  "db"
  "show"
  "collections"
  "databases"
] @keyword

; String literals
(string) @string

; Numeric literals
(number) @number

; Boolean and null values
[
  "true"
  "false"
  "null"
  "undefined"
] @boolean

; Object keys in queries
(pair
  key: (string) @property)

; Punctuation - JSON brackets and delimiters
[
  "("
  ")"
  "["
  "]"
  "{"
  "}"
] @punctuation.bracket

[
  ":"
  ","
  "."
] @punctuation.delimiter

; Comments (MongoDB shell supports // and /* */ comments)
(comment) @comment
