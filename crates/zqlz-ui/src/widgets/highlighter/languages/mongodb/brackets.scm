; Bracket pairs for MongoDB
; Used for bracket matching in MongoDB queries and documents

; Parentheses (function calls, method invocations)
"(" @open
")" @close

; Square brackets (arrays in MongoDB documents)
"[" @open
"]" @close

; Curly braces (MongoDB documents and query objects)
"{" @open
"}" @close
