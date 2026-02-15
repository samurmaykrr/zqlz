; Bracket pairs for Redis
; Used for bracket matching in Redis commands and data structures

; Parentheses (used in some Redis commands and expressions)
"(" @open
")" @close

; Square brackets (arrays in Redis JSON)
"[" @open
"]" @close

; Curly braces (objects in Redis JSON, hash representations)
"{" @open
"}" @close
