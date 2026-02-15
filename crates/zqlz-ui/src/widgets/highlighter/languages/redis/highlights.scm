; Redis command syntax highlights
; Redis uses simple command-based syntax: COMMAND key [arguments...]
; This provides basic highlighting for Redis commands using JSON grammar as base
; for structure (since Redis protocol is text-based with structured data)

; Redis commands - treat uppercase identifiers as keywords
((identifier) @keyword
  (#match? @keyword "^[A-Z]+$"))

; Common Redis commands
[
  "GET"
  "SET"
  "DEL"
  "EXISTS"
  "KEYS"
  "SCAN"
  "EXPIRE"
  "TTL"
  "INCR"
  "DECR"
  "HGET"
  "HSET"
  "HDEL"
  "HGETALL"
  "LPUSH"
  "RPUSH"
  "LPOP"
  "RPOP"
  "LRANGE"
  "SADD"
  "SREM"
  "SMEMBERS"
  "ZADD"
  "ZREM"
  "ZRANGE"
  "PUBLISH"
  "SUBSCRIBE"
  "UNSUBSCRIBE"
  "PING"
  "INFO"
  "CONFIG"
  "FLUSHDB"
  "FLUSHALL"
  "SELECT"
  "MULTI"
  "EXEC"
  "DISCARD"
  "WATCH"
  "UNWATCH"
] @keyword

; String literals (keys and values)
(string) @string

; Numeric literals
(number) @number

; Boolean values
[
  "true"
  "false"
  "null"
] @boolean

; Punctuation
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
] @punctuation.delimiter

; Comments (Redis CLI comments start with #)
(comment) @comment
