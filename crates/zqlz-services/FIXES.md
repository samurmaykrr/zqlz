# zqlz-services Compilation Fixes

## Summary of Issues

The compilation revealed API mismatches between zqlz-services and its dependencies. Here are the fixes needed:

### 1. QueryHistory API
- `QueryHistory::new()` requires a `max_entries: usize` parameter
- `add_entry()` method doesn't exist - should use `add(QueryHistoryEntry)`
- `get_recent()` doesn't exist - need to build entries and return cloned vec
- `search()` returns an iterator, not a Vec

### 2. SchemaCache API
- `SchemaCache::new()` requires a `Duration` parameter
- `get<T>()` doesn't exist - uses specific methods like `get_tables()`, `get_columns()`
- `set()` doesn't exist - uses specific methods like `set_tables()`

### 3. CellUpdateRequest Fields
- Fields are `table_name`, `column_name`, not `table`, `column`
- `new_value` is `Option<Value>`, need to wrap in `Some()`

### 4. ConnectionManager API
- `test_saved()` takes `Uuid`, not `&SavedConnection`
- `list_active()` doesn't exist - need alternative approach

### 5. ViewModels
- `QueryResult` doesn't implement `Serialize`/`Deserialize`
- Need to make this field non-serializable or convert to simpler type

### 6. Type Mismatches
- `affected_rows` is `u64`, needs `usize` conversion
- `unused import: uuid::Uuid` in view_models.rs

## Fixes Applied

See updated service files in this commit.
