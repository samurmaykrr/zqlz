; Folding queries for ClickHouse
; Defines foldable regions in ClickHouse SQL queries

; Subqueries in parentheses
(list_expr) @fold

; CASE expressions
(case_expr) @fold

; WITH clauses (CTEs)
(with_clause) @fold

; Function definitions (CREATE FUNCTION)
(create_function) @fold

; Multi-line table definitions
(create_table) @fold

; Multi-line array literals
(array) @fold
