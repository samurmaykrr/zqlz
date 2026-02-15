; SQLite dialect highlights
; Uses the same tree_sitter_sequel grammar as base SQL, with SQLite-specific
; keyword categorization for improved highlighting relevance.
; SQLite has a minimal type system (type affinity) and fewer keywords than
; PostgreSQL or MySQL.

(object_reference
  name: (identifier) @type)

(invocation
  (object_reference
    name: (identifier) @function.call))

[
  (keyword_btree)
  (keyword_hash)
  (keyword_array)
] @function.call

(relation
  alias: (identifier) @variable)

(field
  name: (identifier) @field)

(term
  alias: (identifier) @variable)

((term
   value: (cast
    name: (keyword_cast) @function.call
    parameter: [(literal)]?)))

(literal) @string
(comment) @comment @spell
(marginalia) @comment

((literal) @number
   (#match? @number "^[-+]?%d+$"))

((literal) @float
  (#match? @float "^[-+]?%d*\.%d*$"))

(parameter) @parameter

[
 (keyword_true)
 (keyword_false)
] @boolean

; SQLite-specific attributes: AUTOINCREMENT, DEFAULT
[
 (keyword_asc)
 (keyword_desc)
 (keyword_nulls)
 (keyword_last)
 (keyword_default)
 (keyword_collate)
 (keyword_auto_increment)
 (keyword_always)
 (keyword_generated)
 (keyword_preceding)
 (keyword_following)
 (keyword_first)
 (keyword_current_timestamp)
] @attribute

; Storage class modifiers — SQLite supports TEMP/TEMPORARY
[
 (keyword_recursive)
 (keyword_temp)
 (keyword_temporary)
 (keyword_virtual)
] @storageclass

; Conditionals
[
 (keyword_case)
 (keyword_when)
 (keyword_then)
 (keyword_else)
] @conditional

; Core SQL + SQLite-specific keywords
[
  (keyword_select)
  (keyword_from)
  (keyword_where)
  (keyword_index)
  (keyword_join)
  (keyword_primary)
  (keyword_delete)
  (keyword_create)
  (keyword_insert)
  (keyword_distinct)
  (keyword_replace)
  (keyword_update)
  (keyword_into)
  (keyword_values)
  (keyword_value)
  (keyword_set)
  (keyword_left)
  (keyword_right)
  (keyword_outer)
  (keyword_inner)
  (keyword_full)
  (keyword_order)
  (keyword_group)
  (keyword_with)
  (keyword_without)
  (keyword_as)
  (keyword_having)
  (keyword_limit)
  (keyword_offset)
  (keyword_table)
  (keyword_tables)
  (keyword_key)
  (keyword_references)
  (keyword_foreign)
  (keyword_constraint)
  (keyword_for)
  (keyword_if)
  (keyword_exists)
  (keyword_column)
  (keyword_columns)
  (keyword_cross)
  (keyword_natural)
  (keyword_alter)
  (keyword_drop)
  (keyword_add)
  (keyword_view)
  (keyword_end)
  (keyword_is)
  (keyword_using)
  (keyword_between)
  (keyword_window)
  (keyword_no)
  (keyword_data)
  (keyword_type)
  (keyword_rename)
  (keyword_to)
  (keyword_all)
  (keyword_any)
  (keyword_some)
  (keyword_returning)
  (keyword_begin)
  (keyword_commit)
  (keyword_rollback)
  (keyword_transaction)
  (keyword_only)
  (keyword_like)
  (keyword_over)
  (keyword_after)
  (keyword_before)
  (keyword_range)
  (keyword_rows)
  (keyword_groups)
  (keyword_exclude)
  (keyword_current)
  (keyword_ties)
  (keyword_others)
  (keyword_row)
  (keyword_stored)
  (keyword_analyze)
  (keyword_explain)
  (keyword_truncate)
  (keyword_vacuum)
  (keyword_conflict)
  (keyword_declare)
  (keyword_filter)
  (keyword_function)
  (keyword_name)
  (keyword_precision)
  (keyword_return)
  (keyword_returns)
  (keyword_trigger)
  (keyword_none)
  (keyword_action)
  (keyword_immediate)
  (keyword_deferred)
  (keyword_each)
  (keyword_instead)
  (keyword_of)
  (keyword_initially)
  (keyword_old)
  (keyword_new)
  (keyword_referencing)
  (keyword_statement)
  (keyword_match)
  (keyword_database)
  (keyword_start)
  (keyword_on)
] @keyword

; Constraint and modifier qualifiers
[
 (keyword_restrict)
 (keyword_unbounded)
 (keyword_unique)
 (keyword_cascade)
 (keyword_ignore)
 (keyword_nothing)
 (keyword_check)
 (keyword_option)
 (keyword_local)
 (keyword_maxvalue)
 (keyword_minvalue)
] @type.qualifier

; SQLite type system — type affinity means fewer specific types
; SQLite recognizes INTEGER, REAL, TEXT, BLOB, NUMERIC as type affinities
[
  (keyword_int)
  (keyword_null)
  (keyword_boolean)
  (keyword_binary)
  (keyword_varbinary)
  (keyword_bit)
  (keyword_character)
  (keyword_smallint)
  (keyword_bigint)
  (keyword_tinyint)
  (keyword_mediumint)
  (keyword_decimal)
  (keyword_float)
  (keyword_double)
  (keyword_numeric)
  (keyword_real)
  (double)
  (keyword_char)
  (keyword_nchar)
  (keyword_varchar)
  (keyword_nvarchar)
  (keyword_varying)
  (keyword_text)
  (keyword_string)
  (keyword_json)
  (keyword_date)
  (keyword_datetime)
  (keyword_time)
  (keyword_timestamp)
  (keyword_interval)
  (keyword_enum)
  (keyword_image)
  (keyword_xml)
  (keyword_inet)
  (keyword_uuid)
  (keyword_jsonb)
  (keyword_bytea)
  (keyword_serial)
  (keyword_smallserial)
  (keyword_bigserial)
  (keyword_money)
  (keyword_smallmoney)
  (keyword_datetime2)
  (keyword_datetimeoffset)
  (keyword_smalldatetime)
  (keyword_timestamptz)
  (keyword_geometry)
  (keyword_geography)
  (keyword_box2d)
  (keyword_box3d)
] @type.builtin

; Keyword operators
[
  (keyword_in)
  (keyword_and)
  (keyword_or)
  (keyword_not)
  (keyword_by)
  (keyword_do)
  (keyword_union)
  (keyword_except)
  (keyword_intersect)
] @keyword.operator

; Symbolic operators
[
  "+"
  "-"
  "*"
  "/"
  "%"
  "^"
  ":="
  "="
  "<"
  "<="
  "!="
  ">="
  ">"
  "<>"
  (op_other)
  (op_unary_other)
] @operator

[
  "("
  ")"
] @punctuation.bracket

[
  ";"
  ","
  "."
] @punctuation.delimiter
