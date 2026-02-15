; Bracket pairs for ClickHouse
; Used for bracket matching in ClickHouse SQL queries

; Parentheses (function calls, subqueries, expressions, tuples)
"(" @open
")" @close

; Square brackets (array subscripts, nested data structures)
"[" @open
"]" @close

; Curly braces (tuples, array literals, map literals)
"{" @open
"}" @close
