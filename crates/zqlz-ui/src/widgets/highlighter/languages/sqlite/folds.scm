; Folding queries for SQLite
; Defines foldable regions in SQLite queries

; Common Table Expressions (CTEs) - WITH clauses
(with_clause) @fold

; Subqueries in parentheses
(subquery) @fold

; CASE expressions
(case_expr) @fold

; View definitions (CREATE VIEW)
(create_view) @fold

; Table definitions (CREATE TABLE)
(create_table) @fold

; Trigger definitions (CREATE TRIGGER)
(create_trigger) @fold

; Multi-line SELECT statements with multiple clauses
(select_statement) @fold

; Multi-line INSERT statements
(insert_statement) @fold

; Multi-line UPDATE statements
(update_statement) @fold

; BEGIN/END blocks in triggers
(begin_end_block) @fold

; Multi-line JSON objects
(json_object) @fold
