; Bracket pairs for PostgreSQL
; Used for bracket matching and rainbow colorization

; Parentheses (function calls, subqueries, expressions)
"(" @open
")" @close

; Square brackets (array subscripts, column names with spaces)
"[" @open
"]" @close

; Curly braces (JSON/JSONB literals, arrays)
"{" @open
"}" @close
