; Bracket pairs for SQLite
; Used for bracket matching and rainbow colorization

; Parentheses (function calls, subqueries, expressions)
"(" @open
")" @close

; Square brackets (column names, identifiers)
"[" @open
"]" @close

; Curly braces (JSON in newer SQLite versions)
"{" @open
"}" @close
