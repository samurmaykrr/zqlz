; Folding queries for PostgreSQL
; Defines foldable regions in PostgreSQL queries

; Common Table Expressions (CTEs) - WITH clauses
(with_clause) @fold

; Subqueries in parentheses
(subquery) @fold

; CASE expressions
(case_expr) @fold

; Function definitions (CREATE FUNCTION)
(create_function) @fold

; Stored procedure definitions (CREATE PROCEDURE)
(create_procedure) @fold

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

; BEGIN/END blocks (PostgreSQL PL/pgSQL)
(begin_end_block) @fold

; DO blocks for anonymous code
(do_block) @fold

; Array literals (PostgreSQL arrays)
(array) @fold

; JSON/JSONB objects
(json_object) @fold
