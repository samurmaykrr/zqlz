; Bracket pairs for MySQL
; Used for bracket matching and rainbow colorization

; Parentheses (function calls, subqueries, expressions)
"(" @open
")" @close

; Square brackets (used in some MySQL contexts)
"[" @open
"]" @close

; Backticks are MySQL's quote character but not brackets
