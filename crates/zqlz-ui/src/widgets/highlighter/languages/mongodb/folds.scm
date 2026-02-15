; Folding queries for MongoDB
; Defines foldable regions in MongoDB queries

; Arrays - foldable between [ and ]
(array) @fold

; Objects - foldable between { and }
(object) @fold

; Aggregation pipelines (arrays of pipeline stages)
(pair
  key: (string (string_content) @_key)
  value: (array) @fold
  (#match? @_key "^(aggregate|pipeline)$"))
