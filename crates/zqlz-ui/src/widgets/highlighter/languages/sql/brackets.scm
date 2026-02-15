; Bracket pairs for SQL
; Used for bracket matching and rainbow colorization

; Parentheses
"(" @open
")" @close

; Square brackets (for identifiers in some SQL dialects)
"[" @open
"]" @close

; Curly braces (for JSON/arrays in PostgreSQL, etc.)
"{" @open
"}" @close
