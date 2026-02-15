# ZQLZ Development Guidelines

> **Based on**: `/zed/CLAUDE.md` - Zed's official coding guidelines
>
> **Additional Context**: ZQLZ is a database IDE built with GPUI and gpui-component

---

## Rust Coding Guidelines

* **Prioritize code correctness and clarity.** Speed and efficiency are secondary unless specified.
* **Do not write organizational comments** that summarize the code. Comments should only explain "why" in tricky/non-obvious cases.
* **Prefer implementing functionality in existing files** unless it's a new logical component. Avoid creating many small files.
* **Avoid functions that panic** like `unwrap()`. Use mechanisms like `?` to propagate errors.
* **Be careful with operations like indexing** which may panic if indexes are out of bounds.
* **Never silently discard errors** with `let _ =` on fallible operations. Always handle errors appropriately:
  - Propagate errors with `?` when the calling function should handle them
  - Use `.log_err()` or similar when you need to ignore errors but want visibility
  - Use explicit error handling with `match` or `if let Err(...)` when you need custom logic
  - Example: avoid `let _ = client.request(...).await?;` - use `client.request(...).await?;` instead
* **When implementing async operations that may fail**, ensure errors propagate to the UI layer so users get meaningful feedback via notifications.
* **Never create files with `mod.rs` paths** - prefer `src/some_module.rs` instead of `src/some_module/mod.rs`.
* **When creating new crates**, prefer specifying the library root path in `Cargo.toml` using `[lib] path = "src/crate_name.rs"` instead of the default `lib.rs`, to maintain consistent and descriptive naming (e.g., `zqlz_core.rs` instead of `lib.rs`).
* **Avoid creative additions unless explicitly requested**
* **Use full words for variable names** (no abbreviations like "q" for "queue")
* **Use variable shadowing** to scope clones in async contexts for clarity, minimizing the lifetime of borrowed references.
  Example:
  ```rust
  executor.spawn({
      let connection = connection.clone();
      async move {
          connection.query(...).await
      }
  });
  ```

---

## GPUI Framework

GPUI is a UI framework which also provides primitives for state and concurrency management.

### Context

Context types allow interaction with global state, windows, entities, and system services. They are typically passed to functions as the argument named `cx`. When a function takes callbacks they come after the `cx` parameter.

* `App` is the root context type, providing access to global state and read/update of entities.
* `Context<T>` is provided when updating an `Entity<T>`. This context dereferences into `App`, so functions which take `&App` can also take `&Context<T>`.
* `AsyncApp` and `AsyncWindowContext` are provided by `cx.spawn` and `cx.spawn_in`. These can be held across await points.

### Window

`Window` provides access to the state of an application window. It is passed to functions as an argument named `window` and comes before `cx` when present. It is used for managing focus, dispatching actions, directly drawing, getting user input state, etc.

### Entities

An `Entity<T>` is a handle to state of type `T`. With `thing: Entity<T>`:

* `thing.entity_id()` returns `EntityId`
* `thing.downgrade()` returns `WeakEntity<T>`
* `thing.read(cx: &App)` returns `&T`.
* `thing.read_with(cx, |thing: &T, cx: &App| ...)` returns the closure's return value.
* `thing.update(cx, |thing: &mut T, cx: &mut Context<T>| ...)` allows the closure to mutate the state, and provides a `Context<T>` for interacting with the entity. It returns the closure's return value.
* `thing.update_in(cx, |thing: &mut T, window: &mut Window, cx: &mut Context<T>| ...)` takes a `AsyncWindowContext` or `VisualTestContext`. It's the same as `update` while also providing the `Window`.

Within the closures, the inner `cx` provided to the closure must be used instead of the outer `cx` to avoid issues with multiple borrows.

Trying to update an entity while it's already being updated must be avoided as this will cause a panic.

When `read_with`, `update`, or `update_in` are used with an async context, the closure's return value is wrapped in an `anyhow::Result`.

`WeakEntity<T>` is a weak handle. It has `read_with`, `update`, and `update_in` methods that work the same, but always return an `anyhow::Result` so that they can fail if the entity no longer exists. This can be useful to avoid memory leaks - if entities have mutually recursive handles to each other they will never be dropped.

### Concurrency

All use of entities and UI rendering occurs on a single foreground thread.

`cx.spawn(async move |cx| ...)` runs an async closure on the foreground thread. Within the closure, `cx` is an async context like `AsyncApp` or `AsyncWindowContext`.

When the outer cx is a `Context<T>`, the use of `spawn` instead looks like `cx.spawn(async move |handle, cx| ...)`, where `handle: WeakEntity<T>`.

To do work on other threads, `cx.background_spawn(async move { ... })` is used. Often this background task is awaited on by a foreground task which uses the results to update state.

Both `cx.spawn` and `cx.background_spawn` return a `Task<R>`, which is a future that can be awaited upon. If this task is dropped, then its work is cancelled. To prevent this one of the following must be done:

* Awaiting the task in some other async context.
* Detaching the task via `task.detach()` or `task.detach_and_log_err(cx)`, allowing it to run indefinitely.
* Storing the task in a field, if the work should be halted when the struct is dropped.

A task which doesn't do anything but provide a value can be created with `Task::ready(value)`.

### Elements

The `Render` trait is used to render some state into an element tree that is laid out using flexbox layout. An `Entity<T>` where `T` implements `Render` is sometimes called a "view".

Example:

```rust
struct TextWithBorder(SharedString);

impl Render for TextWithBorder {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().border_1().child(self.0.clone())
    }
}
```

Since `impl IntoElement for SharedString` exists, it can be used as an argument to `child`. `SharedString` is used to avoid copying strings, and is either an `&'static str` or `Arc<str>`.

UI components that are constructed just to be turned into elements can instead implement the `RenderOnce` trait, which is similar to `Render`, but its `render` method takes ownership of `self`. Types that implement this trait can use `#[derive(IntoElement)]` to use them directly as children.

The style methods on elements are similar to those used by Tailwind CSS.

If some attributes or children of an element tree are conditional, `.when(condition, |this| ...)` can be used to run the closure only when `condition` is true. Similarly, `.when_some(option, |this, value| ...)` runs the closure when the `Option` has a value.

### Input Events

Input event handlers can be registered on an element via methods like `.on_click(|event, window, cx: &mut App| ...)`.

Often event handlers will want to update the entity that's in the current `Context<T>`. The `cx.listener` method provides this - its use looks like `.on_click(cx.listener(|this: &mut T, event, window, cx: &mut Context<T>| ...)`.

### Actions

Actions are dispatched via user keyboard interaction or in code via `window.dispatch_action(SomeAction.boxed_clone(), cx)` or `focus_handle.dispatch_action(&SomeAction, window, cx)`.

Actions with no data defined with the `actions!(some_namespace, [SomeAction, AnotherAction])` macro call. Otherwise the `Action` derive macro is used. Doc comments on actions are displayed to the user.

Action handlers can be registered on an element via the event handler `.on_action(|action, window, cx| ...)`. Like other event handlers, this is often used with `cx.listener`.

### Notify

When a view's state has changed in a way that may affect its rendering, it should call `cx.notify()`. This will cause the view to be rerendered. It will also cause any observe callbacks registered for the entity with `cx.observe` to be called.

### Entity Events

While updating an entity (`cx: Context<T>`), it can emit an event using `cx.emit(event)`. Entities register which events they can emit by declaring `impl EventEmittor<EventType> for EntityType {}`.

Other entities can then register a callback to handle these events by doing `cx.subscribe(other_entity, |this, other_entity, event, cx| ...)`. This will return a `Subscription` which deregisters the callback when dropped. Typically `cx.subscribe` happens when creating a new entity and the subscriptions are stored in a `_subscriptions: Vec<Subscription>` field.

### Recent API Changes

GPUI has had some changes to its APIs. Always write code using the new APIs:

* `spawn` methods now take async closures (`AsyncFn`), and so should be called like `cx.spawn(async move |cx| ...)`.
* Use `Entity<T>`. This replaces `Model<T>` and `View<T>` which no longer exist and should NEVER be used.
* Use `App` references. This replaces `AppContext` which no longer exists and should NEVER be used.
* Use `Context<T>` references. This replaces `ModelContext<T>` which no longer exists and should NEVER be used.
* `Window` is now passed around explicitly. The new interface adds a `Window` reference parameter to some methods, and adds some new "*_in" methods for plumbing `Window`. The old types `WindowContext` and `ViewContext<T>` should NEVER be used.

---

## ZQLZ-Specific Guidelines

### 1. Component Usage

**CRITICAL: Use gpui-component Library**

* **NEVER build custom UI components** without checking `GPUI_COMPONENT_REFERENCE.md` first
* **ALL standard components exist** in gpui-component library
* Reference `/gpui-component/component-snippets/` for copy-paste examples

Examples:
```rust
// ✅ CORRECT: Use library components
use gpui_component::table::Table;
use gpui_component::tree::Tree;
use gpui_component::dock::DockArea;

// ❌ WRONG: Building custom when library has it
struct MyCustomTable { ... }  // DON'T DO THIS!
```

### 2. Database Query Execution

* **Always run queries on background threads** using `cx.background_spawn`
* **Update UI on foreground thread** using `update_in`
* **Show notifications** for query completion/errors
* **Never block the UI thread** waiting for database operations

Example:
```rust
impl QueryEditor {
    fn execute_query(&mut self, cx: &mut Context<Self>) {
        self.is_executing = true;
        cx.notify();

        let connection = self.connection.clone();
        let sql = self.get_sql();

        cx.spawn(async move |editor, cx| {
            // Background: Execute query
            let result = connection.query(&sql, &[]).await;

            // Foreground: Update UI
            _ = editor.update_in(cx, |editor, window, cx| {
                editor.is_executing = false;
                match result {
                    Ok(data) => {
                        editor.show_results(data);
                        window.push_notification(
                            Notification::success("Query executed"),
                            cx,
                        );
                    }
                    Err(e) => {
                        editor.show_error(&e);
                        window.push_notification(
                            Notification::error(&e.to_string()),
                            cx,
                        );
                    }
                }
                cx.notify();
            });

            Ok::<_, anyhow::Error>(())
        }).detach();
    }
}
```

### 3. Connection Management

* **Use Arc<ConnectionManager>** for shared access
* **Store active connections** in workspace state
* **Handle connection failures gracefully** with user notifications
* **Never silently retry** - always inform the user

### 4. Schema Introspection

* **Cache schema information** to avoid repeated queries
* **Refresh on user request** (don't auto-refresh constantly)
* **Show progress indicators** for slow schema loads
* **Use Tree component** for schema browser (don't build custom)

### 5. File Organization (Zed Pattern)

* **NO mod.rs files** - Use explicit library paths
* **Large focused files** - Workspace.rs can be 400KB+ (like Zed)
* **Fewer, larger files** preferred over many small files
* **Related logic stays together** - Don't fragment excessively

Example structure:
```
crates/zqlz-workspace/
├── Cargo.toml              # [lib] path = "src/zqlz_workspace.rs"
└── src/
    ├── zqlz_workspace.rs   # Main workspace logic (large file OK!)
    ├── dock.rs             # Dock-specific logic
    ├── pane.rs             # Pane management
    └── notifications.rs    # Notification handling
```

### 6. Actions and Keybindings

* **Define all actions** at the top of the file
* **Use VSCode-like keybindings** for familiarity
* **Document actions** - doc comments shown to users
* **Register in key context** - Scope keybindings appropriately

Example:
```rust
actions!(
    query_editor,
    [
        ExecuteQuery,        // Cmd+Enter
        ExecuteSelection,    // Cmd+Shift+Enter
        StopQuery,           // Cmd+.
        FormatQuery,         // Cmd+Shift+F
        SaveQuery,           // Cmd+S
    ]
);

pub fn init(cx: &mut App) {
    cx.bind_keys([
        KeyBinding::new("cmd-enter", ExecuteQuery, Some("QueryEditor")),
        KeyBinding::new("cmd-shift-enter", ExecuteSelection, Some("QueryEditor")),
        KeyBinding::new("cmd-.", StopQuery, Some("QueryEditor")),
        KeyBinding::new("cmd-shift-f", FormatQuery, Some("QueryEditor")),
        KeyBinding::new("cmd-s", SaveQuery, Some("QueryEditor")),
    ]);
}
```

### 7. Error Handling for Database Operations

* **Propagate errors to UI** - Never silently fail
* **Show specific error messages** - Don't generic "Failed"
* **Log errors** for debugging
* **Use notifications** for user feedback

```rust
// ❌ WRONG: Silent failure
let _ = connection.execute(&sql, &[]).await;

// ✅ CORRECT: Propagate and notify
match connection.execute(&sql, &[]).await {
    Ok(count) => {
        window.push_notification(
            Notification::success(&format!("{} rows affected", count)),
            cx,
        );
    }
    Err(e) => {
        log::error!("Query failed: {}", e);
        window.push_notification(
            Notification::error(&format!("Query failed: {}", e)),
            cx,
        );
    }
}

// ✅ ALSO CORRECT: Propagate with ?
let count = connection.execute(&sql, &[]).await
    .context("Failed to execute query")?;
```

### 8. State Management

* **Use Entity<T>** for all stateful components
* **Store entities** in workspace/parent components
* **Use subscriptions** for event communication
* **Clean up subscriptions** by storing in Vec<Subscription>

Example:
```rust
pub struct Workspace {
    query_editors: Vec<Entity<QueryEditor>>,
    results_panel: Entity<ResultsPanel>,
    connection_sidebar: Entity<ConnectionSidebar>,
    _subscriptions: Vec<Subscription>,
}

impl Workspace {
    fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let results_panel = cx.new(|cx| ResultsPanel::new(cx));
        let query_editor = cx.new(|cx| QueryEditor::new(cx));

        let mut subscriptions = Vec::new();

        // Subscribe to query editor events
        subscriptions.push(cx.subscribe(&query_editor, |this, editor, event, cx| {
            match event {
                QueryEditorEvent::QueryExecuted(results) => {
                    this.results_panel.update(cx, |panel, cx| {
                        panel.show_results(results.clone());
                        cx.notify();
                    });
                }
            }
        }));

        Self {
            query_editors: vec![query_editor],
            results_panel,
            connection_sidebar: cx.new(|cx| ConnectionSidebar::new(cx)),
            _subscriptions: subscriptions,
        }
    }
}
```

---

## Reference Documentation

Before implementing ANY feature, consult:

1. **ZED_PATTERNS.md** - How Zed implements features (essential!)
2. **GPUI_COMPONENT_REFERENCE.md** - Available UI components (check first!)
3. **PLAN.md** - Product specification and architecture
4. **INSTRUCTIONS.md** - Development workflow and patterns

Study these Zed files:
* `/zed/crates/zed/src/zed.rs` - Application structure
* `/zed/crates/workspace/src/workspace.rs` - Main workspace (study carefully!)
* `/zed/crates/workspace/src/dock.rs` - Dock system usage
* `/zed/crates/editor/src/editor.rs` - Complex component example
* `/zed/crates/project_panel/` - Tree-based panel (like our schema browser)

Study gpui-component:
* `/gpui-component/component-snippets/` - Copy-paste examples
* `/gpui-component/examples/` - Full applications
* `/gpui-component/crates/ui/src/` - Component source code

---

## Testing

* Use `#[gpui::test]` for tests that need GPUI context
* Use `#[tokio::test]` for async tests
* Test database drivers with in-memory databases when possible
* Mock connections for UI tests

Example:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use gpui::TestAppContext;

    #[gpui::test]
    fn test_query_editor_execute(cx: &mut TestAppContext) {
        let editor = cx.new(|cx| QueryEditor::new(cx));

        editor.update(cx, |editor, cx| {
            editor.set_text("SELECT 1", cx);
            editor.execute_query(cx);
        });

        // Assertions...
    }
}
```

---

## General Guidelines

* Follow Zed's patterns - they've solved these problems
* Use gpui-component - don't reinvent wheels
* Large focused files - don't fragment excessively
* Never silent errors - always propagate or notify
* Background database work - never block UI
* Entity<T> everywhere - for all stateful components
* Study `/zed/` continuously - it's our reference implementation

---

*This file is based on `/zed/CLAUDE.md` with ZQLZ-specific additions.*
