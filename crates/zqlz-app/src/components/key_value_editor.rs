//! Key-Value / Row Editor Panel
//!
//! A dual-mode form-based editor panel:
//! - **Redis mode**: Edits key-value entries with support for multiple Redis data types
//!   (string, JSON, list, set, hash, zset), type selection, and TTL configuration.
//! - **SQL Row mode**: Edits full rows from relational tables with one input field per column,
//!   type badges, nullable indicators, and NULL checkboxes.

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_core::ColumnMeta;
use zqlz_ui::widgets::{
    button::{Button, ButtonVariant, ButtonVariants},
    checkbox::Checkbox,
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputState},
    select::{Select, SelectEvent, SelectItem, SelectState},
    v_flex, ActiveTheme, Disableable, Icon, IndexPath, Sizable, ZqlzIcon,
};

use super::TableViewerPanel;

/// Represents the type of a Redis value
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum RedisValueType {
    #[default]
    String,
    List,
    Set,
    ZSet,
    Hash,
    Stream,
    Json,
}

impl RedisValueType {
    pub fn as_str(&self) -> &'static str {
        match self {
            RedisValueType::String => "string",
            RedisValueType::List => "list",
            RedisValueType::Set => "set",
            RedisValueType::ZSet => "zset",
            RedisValueType::Hash => "hash",
            RedisValueType::Stream => "stream",
            RedisValueType::Json => "json",
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            RedisValueType::String => "String",
            RedisValueType::List => "List",
            RedisValueType::Set => "Set",
            RedisValueType::ZSet => "Sorted Set",
            RedisValueType::Hash => "Hash",
            RedisValueType::Stream => "Stream",
            RedisValueType::Json => "JSON",
        }
    }

    pub fn all() -> Vec<RedisValueType> {
        vec![
            RedisValueType::String,
            RedisValueType::List,
            RedisValueType::Set,
            RedisValueType::ZSet,
            RedisValueType::Hash,
            RedisValueType::Stream,
            RedisValueType::Json,
        ]
    }

    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "string" => RedisValueType::String,
            "list" => RedisValueType::List,
            "set" => RedisValueType::Set,
            "zset" => RedisValueType::ZSet,
            "hash" => RedisValueType::Hash,
            "stream" => RedisValueType::Stream,
            "json" => RedisValueType::Json,
            _ => RedisValueType::String,
        }
    }

    /// Whether this type uses a collection editor (list of items)
    pub fn is_collection(&self) -> bool {
        matches!(
            self,
            RedisValueType::List
                | RedisValueType::Set
                | RedisValueType::ZSet
                | RedisValueType::Hash
        )
    }
}

impl SelectItem for RedisValueType {
    type Value = Self;

    fn title(&self) -> SharedString {
        self.display_name().into()
    }

    fn value(&self) -> &Self::Value {
        self
    }
}

/// TTL options for a key
#[derive(Clone, Debug, PartialEq)]
pub enum TtlOption {
    NoExpiry,
    Seconds(u64),
    Minutes(u64),
    Hours(u64),
    Days(u64),
    Custom(u64),
}

impl TtlOption {
    pub fn display_name(&self) -> String {
        match self {
            TtlOption::NoExpiry => "No TTL".to_string(),
            TtlOption::Seconds(s) => format!("{} seconds", s),
            TtlOption::Minutes(m) => format!("{} minutes", m),
            TtlOption::Hours(h) => format!("{} hours", h),
            TtlOption::Days(d) => format!("{} days", d),
            TtlOption::Custom(s) => format!("Custom: {} seconds", s),
        }
    }

    pub fn to_seconds(&self) -> Option<u64> {
        match self {
            TtlOption::NoExpiry => None,
            TtlOption::Seconds(s) => Some(*s),
            TtlOption::Minutes(m) => Some(m * 60),
            TtlOption::Hours(h) => Some(h * 3600),
            TtlOption::Days(d) => Some(d * 86400),
            TtlOption::Custom(s) => Some(*s),
        }
    }

    pub fn presets() -> Vec<TtlOption> {
        vec![
            TtlOption::NoExpiry,
            TtlOption::Seconds(30),
            TtlOption::Minutes(1),
            TtlOption::Minutes(5),
            TtlOption::Minutes(15),
            TtlOption::Hours(1),
            TtlOption::Hours(6),
            TtlOption::Hours(12),
            TtlOption::Days(1),
            TtlOption::Days(7),
            TtlOption::Days(30),
        ]
    }
}

impl SelectItem for TtlOption {
    type Value = Self;

    fn title(&self) -> SharedString {
        self.display_name().into()
    }

    fn value(&self) -> &Self::Value {
        self
    }
}

/// A single item in a List or Set
#[derive(Clone, Debug)]
pub struct ListItem {
    pub value: String,
    pub input: Entity<InputState>,
}

/// A field in a Hash
#[derive(Clone, Debug)]
pub struct HashField {
    pub field: String,
    pub value: String,
    pub field_input: Entity<InputState>,
    pub value_input: Entity<InputState>,
}

/// A member in a Sorted Set (ZSet)
#[derive(Clone, Debug)]
pub struct ZSetMember {
    pub member: String,
    pub score: f64,
    pub member_input: Entity<InputState>,
    pub score_input: Entity<InputState>,
}

/// Data for a key-value entry being edited
#[derive(Clone, Debug)]
pub struct KeyValueData {
    pub key: String,
    pub value_type: RedisValueType,
    pub value: Option<String>,
    pub ttl: i64,
    pub size_bytes: Option<i64>,
    pub connection_id: Uuid,
    pub is_new: bool,
}

impl KeyValueData {
    pub fn new(key: String, connection_id: Uuid) -> Self {
        Self {
            key,
            value_type: RedisValueType::String,
            value: None,
            ttl: -1,
            size_bytes: None,
            connection_id,
            is_new: true,
        }
    }

    pub fn with_value(mut self, value: String) -> Self {
        self.value = Some(value);
        self
    }

    pub fn with_type(mut self, value_type: RedisValueType) -> Self {
        self.value_type = value_type;
        self
    }

    pub fn with_ttl(mut self, ttl: i64) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn existing(mut self) -> Self {
        self.is_new = false;
        self
    }
}

/// Which editing mode the panel is in
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowEditorMode {
    /// Editing a Redis key-value entry
    RedisKey,
    /// Editing a SQL table row (new or existing)
    SqlRow,
}

/// Data for a SQL table row being edited
#[derive(Clone, Debug)]
pub struct RowData {
    pub table_name: String,
    pub connection_id: Uuid,
    pub column_meta: Vec<ColumnMeta>,
    /// Current values for each column (empty string for unset)
    pub row_values: Vec<String>,
    /// Row index in the table viewer (None = new row being inserted)
    pub row_index: Option<usize>,
    pub is_new: bool,
    /// The table viewer this row came from, for updating after save
    pub source_viewer: Option<WeakEntity<TableViewerPanel>>,
    /// All column names (parallel to row_values)
    pub all_column_names: Vec<String>,
}

/// A single field in the SQL row editor form
#[derive(Clone, Debug)]
pub struct RowField {
    pub input: Entity<InputState>,
    pub is_null: bool,
}

/// Events emitted by the KeyValueEditor
#[derive(Clone, Debug)]
pub enum KeyValueEditorEvent {
    ValueSaved {
        original_key: String,
        new_key: String,
        connection_id: Uuid,
        value_type: RedisValueType,
        new_value: String,
        new_ttl: Option<u64>,
    },
    Cancelled,
    Deleted {
        key: String,
        connection_id: Uuid,
    },
    /// A SQL row was saved (new or updated)
    RowSaved {
        table_name: String,
        connection_id: Uuid,
        column_names: Vec<String>,
        /// Database column types ordered to match column_names
        column_types: Vec<String>,
        /// Values for each column (None = NULL)
        values: Vec<Option<String>>,
        is_new: bool,
        /// Original row index if editing existing row
        row_index: Option<usize>,
        /// Source viewer to update after save
        source_viewer: Option<WeakEntity<TableViewerPanel>>,
        /// Original row values for building WHERE clause on updates
        original_row_values: Vec<String>,
    },
    /// A field in the SQL row editor was changed by the user
    ///
    /// Used for bidirectional sync: MainView forwards this to
    /// the table grid so inline cell values stay in sync.
    FieldChanged {
        col_index: usize,
        new_value: String,
        is_null: bool,
        row_index: Option<usize>,
        source_viewer: Option<WeakEntity<TableViewerPanel>>,
    },
}

/// Key-Value / Row Editor Panel
pub struct KeyValueEditorPanel {
    focus_handle: FocusHandle,

    /// Which mode the editor is in
    mode: RowEditorMode,

    // --- Redis mode fields ---
    data: Option<KeyValueData>,
    key_input: Entity<InputState>,
    type_selector: Entity<SelectState<Vec<RedisValueType>>>,
    value_input: Entity<InputState>,
    ttl_selector: Entity<SelectState<Vec<TtlOption>>>,

    // Collection editors
    list_items: Vec<ListItem>,
    hash_fields: Vec<HashField>,
    zset_members: Vec<ZSetMember>,

    // --- SQL row mode fields ---
    row_data: Option<RowData>,
    /// One input per column in the SQL row form
    row_fields: Vec<RowField>,
    /// Which field is currently focused in the SQL row editor (for highlighting)
    focused_field_index: Option<usize>,
    /// Subscriptions to InputEvent::Change on each row field input
    _field_subscriptions: Vec<Subscription>,

    // --- Shared fields ---
    validation_error: Option<String>,
    is_modified: bool,
    word_wrap: bool,
    _subscriptions: Vec<Subscription>,
}

impl KeyValueEditorPanel {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let key_input = cx.new(|cx| InputState::new(window, cx).placeholder("Enter key name..."));

        let type_selector = cx.new(|cx| {
            SelectState::new(
                RedisValueType::all(),
                Some(IndexPath::default().row(0)),
                window,
                cx,
            )
        });

        let value_input = cx.new(|cx| {
            InputState::new(window, cx)
                .multi_line(true)
                .soft_wrap(true)
                .placeholder("Enter value...")
        });

        let ttl_selector = cx.new(|cx| {
            SelectState::new(
                TtlOption::presets(),
                Some(IndexPath::default().row(0)),
                window,
                cx,
            )
        });

        let mut subscriptions = Vec::new();

        subscriptions.push(cx.subscribe(&type_selector, |this, _, event, cx| {
            if let SelectEvent::Confirm(Some(value_type)) = event {
                this.on_type_changed(*value_type, cx);
            }
        }));

        Self {
            focus_handle: cx.focus_handle(),
            mode: RowEditorMode::RedisKey,
            data: None,
            key_input,
            type_selector,
            value_input,
            ttl_selector,
            list_items: Vec::new(),
            hash_fields: Vec::new(),
            zset_members: Vec::new(),
            row_data: None,
            row_fields: Vec::new(),
            focused_field_index: None,
            _field_subscriptions: Vec::new(),
            validation_error: None,
            is_modified: false,
            word_wrap: true,
            _subscriptions: subscriptions,
        }
    }

    pub fn edit_key(&mut self, data: KeyValueData, window: &mut Window, cx: &mut Context<Self>) {
        self.mode = RowEditorMode::RedisKey;
        self.row_data = None;
        self.row_fields.clear();

        tracing::info!(
            "Loading key for editing: key={}, type={:?}, ttl={}",
            data.key,
            data.value_type,
            data.ttl
        );

        // Update key input
        self.key_input.update(cx, |input, cx| {
            input.set_value(&data.key, window, cx);
        });

        // Update type selector
        let type_index = RedisValueType::all()
            .iter()
            .position(|t| *t == data.value_type)
            .unwrap_or(0);
        self.type_selector.update(cx, |state, cx| {
            state.set_selected_index(Some(IndexPath::default().row(type_index)), window, cx);
        });

        // Parse and load value based on type
        let value = data.value.clone().unwrap_or_default();
        self.load_value_for_type(data.value_type, &value, window, cx);

        // Update TTL selector - build list with custom TTL if needed
        let ttl_option = self.ttl_to_option(data.ttl);
        let mut ttl_options = TtlOption::presets();

        // Find if the current TTL matches any preset by actual seconds value
        let ttl_index = if data.ttl < 0 {
            // No expiry - always index 0
            0
        } else {
            // Find a preset that matches the TTL in seconds
            let matching_index = ttl_options
                .iter()
                .position(|preset| preset.to_seconds().map(|s| s as i64) == Some(data.ttl));

            if let Some(index) = matching_index {
                index
            } else {
                // No matching preset - add the custom TTL option
                ttl_options.push(ttl_option);
                ttl_options.len() - 1
            }
        };

        // Recreate TTL selector with potentially updated options
        self.ttl_selector = cx.new(|cx| {
            SelectState::new(
                ttl_options,
                Some(IndexPath::default().row(ttl_index)),
                window,
                cx,
            )
        });

        self.data = Some(data);
        self.is_modified = false;
        self.validation_error = None;

        cx.notify();
    }

    /// Load value into appropriate editor based on type
    fn load_value_for_type(
        &mut self,
        value_type: RedisValueType,
        value: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Clear previous collection state
        self.list_items.clear();
        self.hash_fields.clear();
        self.zset_members.clear();

        match value_type {
            RedisValueType::List | RedisValueType::Set => {
                self.parse_list_value(value, window, cx);
            }
            RedisValueType::Hash => {
                self.parse_hash_value(value, window, cx);
            }
            RedisValueType::ZSet => {
                self.parse_zset_value(value, window, cx);
            }
            RedisValueType::Json => {
                let formatted = self.format_json(value).unwrap_or_else(|| value.to_string());
                self.value_input = cx.new(|cx| {
                    InputState::new(window, cx)
                        .multi_line(true)
                        .soft_wrap(self.word_wrap)
                        .placeholder("Enter JSON value...")
                        .code_editor("json")
                        .line_number(true)
                });
                self.value_input.update(cx, |input, cx| {
                    input.set_value(formatted, window, cx);
                });
            }
            _ => {
                // String and other types use the plain text editor
                let value_str = value.to_string();
                self.value_input = cx.new(|cx| {
                    InputState::new(window, cx)
                        .multi_line(true)
                        .soft_wrap(self.word_wrap)
                        .placeholder("Enter value...")
                });
                self.value_input.update(cx, |input, cx| {
                    input.set_value(value_str, window, cx);
                });
            }
        }
    }

    /// Parse list/set value (JSON array or newline-separated)
    fn parse_list_value(&mut self, value: &str, window: &mut Window, cx: &mut Context<Self>) {
        let items: Vec<String> = if value.trim().starts_with('[') {
            serde_json::from_str(value)
                .unwrap_or_else(|_| value.lines().map(|s| s.to_string()).collect())
        } else {
            value
                .lines()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect()
        };

        for item in items {
            let input = cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder("Element...");
                state.set_value(&item, window, cx);
                state
            });
            self.list_items.push(ListItem { value: item, input });
        }
    }

    /// Parse hash value (JSON object)
    fn parse_hash_value(&mut self, value: &str, window: &mut Window, cx: &mut Context<Self>) {
        if let Ok(obj) = serde_json::from_str::<serde_json::Value>(value) {
            if let Some(map) = obj.as_object() {
                for (field, val) in map {
                    let val_str = match val {
                        serde_json::Value::String(s) => s.clone(),
                        _ => val.to_string(),
                    };

                    let field_input = cx.new(|cx| {
                        let mut state = InputState::new(window, cx).placeholder("Field...");
                        state.set_value(field, window, cx);
                        state
                    });
                    let value_input = cx.new(|cx| {
                        let mut state = InputState::new(window, cx).placeholder("Value...");
                        state.set_value(&val_str, window, cx);
                        state
                    });

                    self.hash_fields.push(HashField {
                        field: field.clone(),
                        value: val_str,
                        field_input,
                        value_input,
                    });
                }
            }
        }
    }

    /// Parse zset value (JSON object with member:score or array of [score, member])
    fn parse_zset_value(&mut self, value: &str, window: &mut Window, cx: &mut Context<Self>) {
        if let Ok(obj) = serde_json::from_str::<serde_json::Value>(value) {
            if let Some(map) = obj.as_object() {
                // Format: {"member": score, ...}
                for (member, score_val) in map {
                    let score = score_val.as_f64().unwrap_or(0.0);

                    let member_input = cx.new(|cx| {
                        let mut state = InputState::new(window, cx).placeholder("Element...");
                        state.set_value(member, window, cx);
                        state
                    });
                    let score_input = cx.new(|cx| {
                        let mut state = InputState::new(window, cx).placeholder("Score...");
                        state.set_value(&score.to_string(), window, cx);
                        state
                    });

                    self.zset_members.push(ZSetMember {
                        member: member.clone(),
                        score,
                        member_input,
                        score_input,
                    });
                }
            } else if let Some(arr) = obj.as_array() {
                // Format: [score1, member1, score2, member2, ...]
                for chunk in arr.chunks(2) {
                    if chunk.len() == 2 {
                        let score = chunk[0].as_f64().unwrap_or(0.0);
                        let member = chunk[1].as_str().unwrap_or("").to_string();

                        let member_input = cx.new(|cx| {
                            let mut state = InputState::new(window, cx).placeholder("Element...");
                            state.set_value(&member, window, cx);
                            state
                        });
                        let score_input = cx.new(|cx| {
                            let mut state = InputState::new(window, cx).placeholder("Score...");
                            state.set_value(&score.to_string(), window, cx);
                            state
                        });

                        self.zset_members.push(ZSetMember {
                            member,
                            score,
                            member_input,
                            score_input,
                        });
                    }
                }
            }
        }
    }

    pub fn new_key(&mut self, connection_id: Uuid, window: &mut Window, cx: &mut Context<Self>) {
        self.mode = RowEditorMode::RedisKey;
        let data = KeyValueData::new(String::new(), connection_id);
        self.edit_key(data, window, cx);

        self.key_input.update(cx, |input, cx| {
            input.focus(window, cx);
        });
    }

    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.data = None;
        self.row_data = None;
        self.row_fields.clear();
        self.focused_field_index = None;
        self._field_subscriptions.clear();
        self.list_items.clear();
        self.hash_fields.clear();
        self.zset_members.clear();
        self.is_modified = false;
        self.validation_error = None;
        cx.notify();
    }

    /// Check if the editor is currently editing the specified key for a given connection
    pub fn is_editing_key(&self, key: &str, connection_id: Uuid) -> bool {
        self.data
            .as_ref()
            .map(|d| d.key == key && d.connection_id == connection_id)
            .unwrap_or(false)
    }

    /// Clear the editor if it's currently editing the specified key
    pub fn clear_if_editing_key(&mut self, key: &str, connection_id: Uuid, cx: &mut Context<Self>) {
        if self.is_editing_key(key, connection_id) {
            self.clear(cx);
        }
    }

    /// Open the editor to edit an existing SQL row
    pub fn edit_row(&mut self, data: RowData, window: &mut Window, cx: &mut Context<Self>) {
        tracing::info!(
            "Loading row for editing: table={}, row_index={:?}, columns={}",
            data.table_name,
            data.row_index,
            data.column_meta.len()
        );

        self.mode = RowEditorMode::SqlRow;
        self.data = None;
        self.list_items.clear();
        self.hash_fields.clear();
        self.zset_members.clear();
        self.focused_field_index = None;
        self._field_subscriptions.clear();

        self.row_fields = data
            .column_meta
            .iter()
            .zip(data.row_values.iter())
            .map(|(col, value)| {
                let is_null = value == "NULL" || value.is_empty() && col.nullable;
                let placeholder = format!("{} ({})", col.name, col.data_type);
                let input = cx.new(|cx| {
                    let mut state = InputState::new(window, cx).placeholder(placeholder);
                    if !is_null {
                        state.set_value(value, window, cx);
                    }
                    state
                });
                RowField { input, is_null }
            })
            .collect();

        // Subscribe to InputEvent::Change on each field for bidirectional sync
        let row_index = data.row_index;
        let source_viewer = data.source_viewer.clone();
        for (col_index, field) in self.row_fields.iter().enumerate() {
            let source_viewer = source_viewer.clone();
            let subscription = cx.subscribe(&field.input, move |this, input_entity, event, cx| {
                use zqlz_ui::widgets::input::InputEvent;
                match event {
                    InputEvent::Change => {
                        this.is_modified = true;
                        let new_value = input_entity.read(cx).text().to_string();
                        let is_null = this.row_fields.get(col_index).map_or(false, |f| f.is_null);
                        cx.emit(KeyValueEditorEvent::FieldChanged {
                            col_index,
                            new_value,
                            is_null,
                            row_index,
                            source_viewer: source_viewer.clone(),
                        });
                        cx.notify();
                    }
                    InputEvent::Focus => {
                        this.focused_field_index = Some(col_index);
                        cx.notify();
                    }
                    _ => {}
                }
            });
            self._field_subscriptions.push(subscription);
        }

        self.row_data = Some(data);
        self.is_modified = false;
        self.validation_error = None;
        cx.notify();
    }

    /// Open the editor to create a new SQL row
    pub fn new_row(
        &mut self,
        table_name: String,
        connection_id: Uuid,
        column_meta: Vec<ColumnMeta>,
        source_viewer: Option<WeakEntity<TableViewerPanel>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let all_column_names: Vec<String> = column_meta.iter().map(|c| c.name.clone()).collect();
        let row_values: Vec<String> = column_meta
            .iter()
            .map(|col| col.default_value.clone().unwrap_or_default())
            .collect();

        let data = RowData {
            table_name,
            connection_id,
            column_meta,
            row_values,
            row_index: None,
            is_new: true,
            source_viewer,
            all_column_names,
        };

        self.edit_row(data, window, cx);

        // Focus the first non-auto-increment field
        if let Some(first_field_index) = self
            .row_data
            .as_ref()
            .and_then(|d| d.column_meta.iter().position(|c| !c.auto_increment))
        {
            if let Some(field) = self.row_fields.get(first_field_index) {
                field.input.update(cx, |input, cx| {
                    input.focus(window, cx);
                });
            }
        }
    }

    /// Get the current mode
    pub fn mode(&self) -> &RowEditorMode {
        &self.mode
    }

    /// Check if the editor is currently showing the given table row
    pub fn is_editing_row(&self, table_name: &str, row_index: usize) -> bool {
        self.row_data
            .as_ref()
            .map(|d| d.table_name == table_name && d.row_index == Some(row_index))
            .unwrap_or(false)
    }

    /// Focus a specific field in the SQL row editor
    pub fn focus_field(&mut self, index: usize, window: &mut Window, cx: &mut Context<Self>) {
        self.focused_field_index = Some(index);
        if let Some(field) = self.row_fields.get(index) {
            field.input.update(cx, |input, cx| {
                input.focus(window, cx);
            });
        }
        cx.notify();
    }

    /// Update a specific field's value from an external source (e.g., inline cell edit).
    /// Used for bidirectional sync: table grid → row editor.
    pub fn update_field_value(
        &mut self,
        col_index: usize,
        new_value: &str,
        is_null: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(field) = self.row_fields.get_mut(col_index) {
            field.is_null = is_null;
            if !is_null {
                let owned_value = new_value.to_string();
                field.input.update(cx, |input, cx| {
                    input.set_value(owned_value, window, cx);
                });
            }
            cx.notify();
        }
    }

    fn ttl_to_option(&self, ttl: i64) -> TtlOption {
        if ttl < 0 {
            TtlOption::NoExpiry
        } else {
            TtlOption::Custom(ttl as u64)
        }
    }

    fn on_type_changed(&mut self, value_type: RedisValueType, cx: &mut Context<Self>) {
        tracing::info!("Type changed to: {:?}", value_type);
        self.is_modified = true;

        // Reset collection state when type changes
        if value_type.is_collection() {
            self.list_items.clear();
            self.hash_fields.clear();
            self.zset_members.clear();
        }

        cx.notify();
    }

    fn has_modifications(&self, cx: &App) -> bool {
        let Some(data) = &self.data else {
            return false;
        };

        if data.is_new {
            let key = self.key_input.read(cx).text().to_string();
            return !key.trim().is_empty();
        }

        // Check if key changed
        let current_key = self.key_input.read(cx).text().to_string();
        if current_key != data.key {
            return true;
        }

        // Check if explicitly modified (type change, add/remove items, etc.)
        if self.is_modified {
            return true;
        }

        // Check if any collection item values have been edited in place
        // Compare current input values with stored values
        for item in &self.list_items {
            let current = item.input.read(cx).text().to_string();
            if current != item.value {
                return true;
            }
        }

        for field in &self.hash_fields {
            let current_field = field.field_input.read(cx).text().to_string();
            let current_value = field.value_input.read(cx).text().to_string();
            if current_field != field.field || current_value != field.value {
                return true;
            }
        }

        for member in &self.zset_members {
            let current_member = member.member_input.read(cx).text().to_string();
            let current_score = member.score_input.read(cx).text().to_string();
            let original_score = member.score.to_string();
            if current_member != member.member || current_score != original_score {
                return true;
            }
        }

        // Check if string/json value changed
        let value_type = self
            .type_selector
            .read(cx)
            .selected_value()
            .copied()
            .unwrap_or(RedisValueType::String);

        if !value_type.is_collection() {
            let current_value = self.value_input.read(cx).text().to_string();
            let original_value = data.value.clone().unwrap_or_default();
            if current_value != original_value {
                return true;
            }
        }

        // Check if TTL changed
        let current_ttl = self
            .ttl_selector
            .read(cx)
            .selected_value()
            .cloned()
            .unwrap_or(TtlOption::NoExpiry);
        let current_ttl_seconds = current_ttl.to_seconds().map(|s| s as i64).unwrap_or(-1);
        if current_ttl_seconds != data.ttl {
            return true;
        }

        false
    }

    fn format_json(&self, value: &str) -> Option<String> {
        if value.trim().is_empty() {
            return None;
        }

        serde_json::from_str::<serde_json::Value>(value)
            .ok()
            .and_then(|v| serde_json::to_string_pretty(&v).ok())
    }

    fn format_json_in_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let current_value = self.value_input.read(cx).text().to_string();

        match self.format_json(&current_value) {
            Some(formatted) => {
                self.value_input.update(cx, |input, cx| {
                    input.set_value(formatted, window, cx);
                });
                self.validation_error = None;
                cx.notify();
            }
            None => {
                self.validation_error = Some("Invalid JSON - cannot format".to_string());
                cx.notify();
            }
        }
    }

    fn validate(&mut self, cx: &Context<Self>) -> bool {
        self.validation_error = None;

        match self.mode {
            RowEditorMode::RedisKey => self.validate_redis(cx),
            RowEditorMode::SqlRow => self.validate_sql_row(cx),
        }
    }

    fn validate_redis(&mut self, cx: &Context<Self>) -> bool {
        let Some(data) = &self.data else {
            return true;
        };

        let key = self.key_input.read(cx).text().to_string();
        if key.trim().is_empty() {
            self.validation_error = Some("Key cannot be empty".to_string());
            return false;
        }

        if matches!(data.value_type, RedisValueType::Json) {
            let value = self.value_input.read(cx).text().to_string();
            if !value.trim().is_empty() {
                if let Err(e) = serde_json::from_str::<serde_json::Value>(&value) {
                    self.validation_error = Some(format!("Invalid JSON: {}", e));
                    return false;
                }
            }
        }

        true
    }

    fn validate_sql_row(&mut self, cx: &Context<Self>) -> bool {
        let Some(data) = &self.row_data else {
            return true;
        };

        // Check required (non-nullable, no default) columns have values
        for (index, col) in data.column_meta.iter().enumerate() {
            if col.auto_increment {
                continue;
            }
            if !col.nullable && col.default_value.is_none() {
                if let Some(field) = self.row_fields.get(index) {
                    if field.is_null {
                        self.validation_error =
                            Some(format!("Column '{}' cannot be NULL", col.name));
                        return false;
                    }
                    let value = field.input.read(cx).text().to_string();
                    if value.trim().is_empty() && data.is_new {
                        self.validation_error =
                            Some(format!("Column '{}' requires a value", col.name));
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Get the serialized value based on current type and editor state
    fn get_serialized_value(&self, cx: &App) -> String {
        let value_type = self
            .type_selector
            .read(cx)
            .selected_value()
            .copied()
            .unwrap_or(RedisValueType::String);

        match value_type {
            RedisValueType::List | RedisValueType::Set => {
                let items: Vec<String> = self
                    .list_items
                    .iter()
                    .map(|item| item.input.read(cx).text().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                serde_json::to_string(&items).unwrap_or_default()
            }
            RedisValueType::Hash => {
                let mut map = serde_json::Map::new();
                for field in &self.hash_fields {
                    let key = field.field_input.read(cx).text().to_string();
                    let value = field.value_input.read(cx).text().to_string();
                    if !key.is_empty() {
                        map.insert(key, serde_json::Value::String(value));
                    }
                }
                serde_json::to_string(&map).unwrap_or_default()
            }
            RedisValueType::ZSet => {
                let mut map = serde_json::Map::new();
                for member in &self.zset_members {
                    let element = member.member_input.read(cx).text().to_string();
                    let score_str = member.score_input.read(cx).text().to_string();
                    let score: f64 = score_str.parse().unwrap_or(0.0);
                    if !element.is_empty() {
                        map.insert(
                            element,
                            serde_json::Value::Number(
                                serde_json::Number::from_f64(score)
                                    .unwrap_or(serde_json::Number::from(0)),
                            ),
                        );
                    }
                }
                serde_json::to_string(&map).unwrap_or_default()
            }
            _ => self.value_input.read(cx).text().to_string(),
        }
    }

    fn save(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if !self.validate(cx) {
            cx.notify();
            return;
        }

        match self.mode {
            RowEditorMode::RedisKey => self.save_redis(cx),
            RowEditorMode::SqlRow => self.save_sql_row(cx),
        }
    }

    fn save_redis(&mut self, cx: &mut Context<Self>) {
        let Some(data) = self.data.clone() else {
            return;
        };

        let value_type = self
            .type_selector
            .read(cx)
            .selected_value()
            .copied()
            .unwrap_or(data.value_type);

        let new_value = self.get_serialized_value(cx);
        let ttl_value = self.ttl_selector.read(cx).selected_value();
        let new_ttl = ttl_value.and_then(|t| t.to_seconds());
        let new_key = self.key_input.read(cx).text().to_string();

        cx.emit(KeyValueEditorEvent::ValueSaved {
            original_key: data.key.clone(),
            new_key,
            connection_id: data.connection_id,
            value_type,
            new_value,
            new_ttl,
        });

        self.is_modified = false;
        cx.notify();
    }

    fn save_sql_row(&mut self, cx: &mut Context<Self>) {
        let Some(data) = self.row_data.clone() else {
            return;
        };

        let column_types: Vec<String> = data
            .column_meta
            .iter()
            .map(|col| col.data_type.clone())
            .collect();
        let values: Vec<Option<String>> = self
            .row_fields
            .iter()
            .map(|field| {
                if field.is_null {
                    None
                } else {
                    Some(field.input.read(cx).text().to_string())
                }
            })
            .collect();

        cx.emit(KeyValueEditorEvent::RowSaved {
            table_name: data.table_name.clone(),
            connection_id: data.connection_id,
            column_names: data.all_column_names.clone(),
            column_types,
            values,
            is_new: data.is_new,
            row_index: data.row_index,
            source_viewer: data.source_viewer.clone(),
            original_row_values: data.row_values.clone(),
        });

        self.is_modified = false;
        cx.notify();
    }

    fn cancel(&mut self, cx: &mut Context<Self>) {
        self.data = None;
        self.row_data = None;
        self.row_fields.clear();
        self.focused_field_index = None;
        self._field_subscriptions.clear();
        self.list_items.clear();
        self.hash_fields.clear();
        self.zset_members.clear();
        self.is_modified = false;
        self.validation_error = None;
        cx.emit(KeyValueEditorEvent::Cancelled);
        cx.notify();
    }

    fn delete(&mut self, cx: &mut Context<Self>) {
        let Some(data) = &self.data else {
            return;
        };

        if !data.is_new {
            cx.emit(KeyValueEditorEvent::Deleted {
                key: data.key.clone(),
                connection_id: data.connection_id,
            });
        }

        self.data = None;
        self.list_items.clear();
        self.hash_fields.clear();
        self.zset_members.clear();
        self.is_modified = false;
        cx.notify();
    }

    // Collection item operations
    fn add_list_item(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let input = cx.new(|cx| InputState::new(window, cx).placeholder("Element..."));
        self.list_items.push(ListItem {
            value: String::new(),
            input,
        });
        self.is_modified = true;
        cx.notify();
    }

    fn remove_list_item(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.list_items.len() {
            self.list_items.remove(index);
            self.is_modified = true;
            cx.notify();
        }
    }

    fn move_list_item_up(&mut self, index: usize, cx: &mut Context<Self>) {
        if index > 0 && index < self.list_items.len() {
            self.list_items.swap(index, index - 1);
            self.is_modified = true;
            cx.notify();
        }
    }

    fn move_list_item_down(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.list_items.len() - 1 {
            self.list_items.swap(index, index + 1);
            self.is_modified = true;
            cx.notify();
        }
    }

    fn add_hash_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let field_input = cx.new(|cx| InputState::new(window, cx).placeholder("Field..."));
        let value_input = cx.new(|cx| InputState::new(window, cx).placeholder("Value..."));
        self.hash_fields.push(HashField {
            field: String::new(),
            value: String::new(),
            field_input,
            value_input,
        });
        self.is_modified = true;
        cx.notify();
    }

    fn remove_hash_field(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.hash_fields.len() {
            self.hash_fields.remove(index);
            self.is_modified = true;
            cx.notify();
        }
    }

    fn add_zset_member(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let member_input = cx.new(|cx| InputState::new(window, cx).placeholder("Element..."));
        let score_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("Score...");
            state.set_value("0", window, cx);
            state
        });
        self.zset_members.push(ZSetMember {
            member: String::new(),
            score: 0.0,
            member_input,
            score_input,
        });
        self.is_modified = true;
        cx.notify();
    }

    fn remove_zset_member(&mut self, index: usize, cx: &mut Context<Self>) {
        if index < self.zset_members.len() {
            self.zset_members.remove(index);
            self.is_modified = true;
            cx.notify();
        }
    }

    fn render_field_row(
        &self,
        label: &str,
        content: impl IntoElement,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .gap_2()
            .items_start()
            .child(
                div()
                    .min_w(px(40.))
                    .pt_1()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(format!("{}:", label)),
            )
            .child(div().flex_1().child(content))
    }

    fn render_key_field(&self, cx: &Context<Self>) -> impl IntoElement {
        self.render_field_row("Key", Input::new(&self.key_input).w_full(), cx)
    }

    fn render_type_field(&self, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_field_row("Type", Select::new(&self.type_selector).w(px(120.)), cx)
    }

    /// Render list/set editor with single column
    fn render_list_editor(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let item_count = self.list_items.len();

        v_flex()
            .gap_1()
            .flex_1()
            .child(
                h_flex().items_center().justify_between().child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child("Value:"),
                ),
            )
            // Table header
            .child(
                div()
                    .w_full()
                    .px_2()
                    .py_1()
                    .bg(theme.table_head)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child("Element"),
                    ),
            )
            // Items list
            .child(
                div()
                    .id("list-items-container")
                    .flex_1()
                    .min_h(px(100.))
                    .overflow_y_scroll()
                    .border_1()
                    .border_color(theme.border)
                    .child(v_flex().children(self.list_items.iter().enumerate().map(
                        |(idx, item)| {
                            let index = idx;
                            h_flex()
                                .w_full()
                                .px_1()
                                .py_0p5()
                                .gap_1()
                                .items_center()
                                .border_b_1()
                                .border_color(theme.border)
                                .child(Input::new(&item.input).w_full().xsmall())
                                .child(
                                    Button::new(("remove-item", idx))
                                        .icon(Icon::new(ZqlzIcon::Minus).size_3())
                                        .ghost()
                                        .xsmall()
                                        .on_click(cx.listener(move |this, _, _, cx| {
                                            this.remove_list_item(index, cx);
                                        })),
                                )
                        },
                    ))),
            )
            // Toolbar
            .child(
                h_flex()
                    .gap_1()
                    .items_center()
                    .justify_between()
                    .pt_1()
                    .child(
                        h_flex()
                            .gap_0p5()
                            .child(
                                Button::new("add-item")
                                    .icon(Icon::new(ZqlzIcon::Plus).size_3())
                                    .ghost()
                                    .xsmall()
                                    .tooltip("Add Element")
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.add_list_item(window, cx);
                                    })),
                            )
                            .child(
                                Button::new("remove-selected")
                                    .icon(Icon::new(ZqlzIcon::Minus).size_3())
                                    .ghost()
                                    .xsmall()
                                    .tooltip("Remove Last")
                                    .disabled(self.list_items.is_empty())
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        let len = this.list_items.len();
                                        if len > 0 {
                                            this.remove_list_item(len - 1, cx);
                                        }
                                    })),
                            )
                            .child(
                                Button::new("move-up")
                                    .icon(Icon::new(ZqlzIcon::ArrowUp).size_3())
                                    .ghost()
                                    .xsmall()
                                    .tooltip("Move Up"),
                            )
                            .child(
                                Button::new("move-down")
                                    .icon(Icon::new(ZqlzIcon::ArrowDown).size_3())
                                    .ghost()
                                    .xsmall()
                                    .tooltip("Move Down"),
                            ),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("{} elements", item_count)),
                    ),
            )
    }

    /// Render hash editor with two columns (Field, Value)
    fn render_hash_editor(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let field_count = self.hash_fields.len();

        v_flex()
            .gap_1()
            .flex_1()
            .child(
                h_flex().items_center().justify_between().child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child("Value:"),
                ),
            )
            // Table header
            .child(
                h_flex()
                    .w_full()
                    .px_2()
                    .py_1()
                    .bg(theme.table_head)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .w(px(120.))
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child("Field"),
                    )
                    .child(
                        div()
                            .flex_1()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child("Value"),
                    ),
            )
            // Fields list
            .child(
                div()
                    .id("hash-fields-container")
                    .flex_1()
                    .min_h(px(100.))
                    .overflow_y_scroll()
                    .border_1()
                    .border_color(theme.border)
                    .child(v_flex().children(self.hash_fields.iter().enumerate().map(
                        |(idx, field)| {
                            let index = idx;
                            h_flex()
                                .w_full()
                                .px_1()
                                .py_0p5()
                                .gap_1()
                                .items_center()
                                .border_b_1()
                                .border_color(theme.border)
                                .child(Input::new(&field.field_input).w(px(110.)).xsmall())
                                .child(Input::new(&field.value_input).flex_1().xsmall())
                                .child(
                                    Button::new(("remove-field", idx))
                                        .icon(Icon::new(ZqlzIcon::Minus).size_3())
                                        .ghost()
                                        .xsmall()
                                        .on_click(cx.listener(move |this, _, _, cx| {
                                            this.remove_hash_field(index, cx);
                                        })),
                                )
                        },
                    ))),
            )
            // Toolbar
            .child(
                h_flex()
                    .gap_1()
                    .items_center()
                    .justify_between()
                    .pt_1()
                    .child(
                        h_flex()
                            .gap_0p5()
                            .child(
                                Button::new("add-field")
                                    .icon(Icon::new(ZqlzIcon::Plus).size_3())
                                    .ghost()
                                    .xsmall()
                                    .tooltip("Add Field")
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.add_hash_field(window, cx);
                                    })),
                            )
                            .child(
                                Button::new("remove-last-field")
                                    .icon(Icon::new(ZqlzIcon::Minus).size_3())
                                    .ghost()
                                    .xsmall()
                                    .tooltip("Remove Last")
                                    .disabled(self.hash_fields.is_empty())
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        let len = this.hash_fields.len();
                                        if len > 0 {
                                            this.remove_hash_field(len - 1, cx);
                                        }
                                    })),
                            ),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("{} fields", field_count)),
                    ),
            )
    }

    /// Render zset editor with two columns (Element, Score)
    fn render_zset_editor(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let member_count = self.zset_members.len();

        v_flex()
            .gap_1()
            .flex_1()
            .child(
                h_flex().items_center().justify_between().child(
                    div()
                        .text_xs()
                        .text_color(theme.muted_foreground)
                        .child("Value:"),
                ),
            )
            // Table header
            .child(
                h_flex()
                    .w_full()
                    .px_2()
                    .py_1()
                    .bg(theme.table_head)
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .flex_1()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child("Element"),
                    )
                    .child(
                        div()
                            .w(px(80.))
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child("Score"),
                    ),
            )
            // Members list
            .child(
                div()
                    .id("zset-members-container")
                    .flex_1()
                    .min_h(px(100.))
                    .overflow_y_scroll()
                    .border_1()
                    .border_color(theme.border)
                    .child(v_flex().children(self.zset_members.iter().enumerate().map(
                        |(idx, member)| {
                            let index = idx;
                            h_flex()
                                .w_full()
                                .px_1()
                                .py_0p5()
                                .gap_1()
                                .items_center()
                                .border_b_1()
                                .border_color(theme.border)
                                .child(Input::new(&member.member_input).flex_1().xsmall())
                                .child(Input::new(&member.score_input).w(px(70.)).xsmall())
                                .child(
                                    Button::new(("remove-member", idx))
                                        .icon(Icon::new(ZqlzIcon::Minus).size_3())
                                        .ghost()
                                        .xsmall()
                                        .on_click(cx.listener(move |this, _, _, cx| {
                                            this.remove_zset_member(index, cx);
                                        })),
                                )
                        },
                    ))),
            )
            // Toolbar
            .child(
                h_flex()
                    .gap_1()
                    .items_center()
                    .justify_between()
                    .pt_1()
                    .child(
                        h_flex()
                            .gap_0p5()
                            .child(
                                Button::new("add-member")
                                    .icon(Icon::new(ZqlzIcon::Plus).size_3())
                                    .ghost()
                                    .xsmall()
                                    .tooltip("Add Member")
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.add_zset_member(window, cx);
                                    })),
                            )
                            .child(
                                Button::new("remove-last-member")
                                    .icon(Icon::new(ZqlzIcon::Minus).size_3())
                                    .ghost()
                                    .xsmall()
                                    .tooltip("Remove Last")
                                    .disabled(self.zset_members.is_empty())
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        let len = this.zset_members.len();
                                        if len > 0 {
                                            this.remove_zset_member(len - 1, cx);
                                        }
                                    })),
                            ),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("{} members", member_count)),
                    ),
            )
    }

    /// Render the string/JSON value editor (textarea)
    fn render_string_editor(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let data = self.data.as_ref();
        let is_json = data.map_or(false, |d| matches!(d.value_type, RedisValueType::Json));
        let word_wrap = self.word_wrap;

        v_flex()
            .gap_1()
            .flex_1()
            .child(
                h_flex()
                    .items_center()
                    .justify_between()
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child("Value:"),
                    )
                    .child(
                        h_flex()
                            .gap_0p5()
                            .when(is_json, |this| {
                                this.child(
                                    Button::new("format-json")
                                        .icon(Icon::new(ZqlzIcon::BracketsCurly).size_3())
                                        .with_variant(ButtonVariant::Ghost)
                                        .xsmall()
                                        .tooltip("Format JSON")
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            this.format_json_in_editor(window, cx);
                                        })),
                                )
                            })
                            .child(
                                Button::new("word-wrap")
                                    .icon(Icon::new(ZqlzIcon::TextWrap).size_3())
                                    .with_variant(if word_wrap {
                                        ButtonVariant::Secondary
                                    } else {
                                        ButtonVariant::Ghost
                                    })
                                    .xsmall()
                                    .tooltip(if word_wrap {
                                        "Disable Word Wrap"
                                    } else {
                                        "Enable Word Wrap"
                                    })
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.word_wrap = !this.word_wrap;
                                        this.value_input.update(cx, |input, cx| {
                                            input.set_soft_wrap(this.word_wrap, window, cx);
                                        });
                                        cx.notify();
                                    })),
                            ),
                    ),
            )
            .child(
                div()
                    .flex_1()
                    .min_h(px(100.))
                    .child(Input::new(&self.value_input).w_full().h_full()),
            )
    }

    /// Render the appropriate value editor based on type
    fn render_value_field(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let value_type = self
            .type_selector
            .read(cx)
            .selected_value()
            .copied()
            .unwrap_or(RedisValueType::String);

        match value_type {
            RedisValueType::List | RedisValueType::Set => {
                self.render_list_editor(cx).into_any_element()
            }
            RedisValueType::Hash => self.render_hash_editor(cx).into_any_element(),
            RedisValueType::ZSet => self.render_zset_editor(cx).into_any_element(),
            _ => self.render_string_editor(cx).into_any_element(),
        }
    }

    fn render_ttl_field(&self, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_field_row("TTL", Select::new(&self.ttl_selector).w(px(120.)), cx)
    }

    fn render_action_buttons(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let has_modifications = self.has_modifications(cx);
        let is_new = self.data.as_ref().map_or(true, |d| d.is_new);

        h_flex()
            .gap_1p5()
            .pt_2()
            .justify_between()
            .child(h_flex().gap_1p5().when(!is_new, |this| {
                this.child(
                    Button::new("delete-key")
                        .label("Delete")
                        .small()
                        .with_variant(ButtonVariant::Danger)
                        .on_click(cx.listener(|this, _, _window, cx| {
                            this.delete(cx);
                        })),
                )
            }))
            .child(
                h_flex()
                    .gap_1p5()
                    .child(
                        Button::new("cancel-edit")
                            .label("Discard")
                            .small()
                            .on_click(cx.listener(|this, _, _window, cx| {
                                this.cancel(cx);
                            })),
                    )
                    .child(
                        Button::new("save-edit")
                            .label("Apply")
                            .small()
                            .with_variant(ButtonVariant::Primary)
                            .disabled(!has_modifications)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.save(window, cx);
                            })),
                    ),
            )
    }

    fn render_validation_error(&self) -> Option<impl IntoElement> {
        self.validation_error.as_ref().map(|error| {
            div()
                .text_xs()
                .text_color(rgb(0xff0000))
                .child(error.clone())
        })
    }

    fn render_empty_state(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let message = match self.mode {
            RowEditorMode::RedisKey => "Select a key to edit",
            RowEditorMode::SqlRow => "Select a row to edit",
        };
        v_flex().size_full().items_center().justify_center().child(
            div()
                .text_sm()
                .text_color(theme.muted_foreground)
                .child(message),
        )
    }

    /// Render a single field in the SQL row form
    fn render_sql_row_field(
        &self,
        index: usize,
        col: &ColumnMeta,
        field: &RowField,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let is_auto = col.auto_increment;
        let is_nullable = col.nullable;
        let is_null = field.is_null;
        let field_index = index;

        v_flex()
            .gap_0p5()
            .pb_2()
            .child(
                h_flex()
                    .gap_1()
                    .items_center()
                    .child(
                        div()
                            .text_xs()
                            .font_weight(FontWeight::MEDIUM)
                            .text_color(theme.foreground)
                            .child(col.name.clone()),
                    )
                    .child(
                        div()
                            .text_xs()
                            .px_1()
                            .py_px()
                            .rounded(px(3.))
                            .bg(theme.muted)
                            .text_color(theme.muted_foreground)
                            .child(col.data_type.clone()),
                    )
                    .when(is_auto, |this| {
                        this.child(
                            div()
                                .text_xs()
                                .px_1()
                                .py_px()
                                .rounded(px(3.))
                                .bg(theme.accent)
                                .text_color(theme.accent_foreground)
                                .child("auto"),
                        )
                    })
                    .when(!is_nullable && !is_auto, |this| {
                        this.child(div().text_xs().text_color(rgb(0xef4444)).child("*"))
                    }),
            )
            .child(
                h_flex()
                    .gap_1()
                    .items_center()
                    .child(
                        div().flex_1().child(
                            Input::new(&field.input)
                                .w_full()
                                .small()
                                .disabled(is_auto || is_null),
                        ),
                    )
                    .when(is_nullable, |this| {
                        this.child(
                            h_flex()
                                .gap_0p5()
                                .items_center()
                                .child(Checkbox::new("null-checkbox").checked(is_null).on_click(
                                    cx.listener(move |this, _, _window, cx| {
                                        if let Some(field) = this.row_fields.get_mut(field_index) {
                                            field.is_null = !field.is_null;
                                            this.is_modified = true;
                                            cx.notify();
                                        }
                                    }),
                                ))
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(theme.muted_foreground)
                                        .child("NULL"),
                                ),
                        )
                    }),
            )
    }

    /// Render the SQL row editor form
    fn render_sql_row_editor(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let Some(data) = &self.row_data else {
            return self.render_empty_state(cx).into_any_element();
        };

        let title = if data.is_new {
            format!("New Row — {}", data.table_name)
        } else {
            format!("Edit Row — {}", data.table_name)
        };

        let has_modifications = self.has_row_modifications(cx);

        v_flex()
            .id("sql-row-editor-content")
            .size_full()
            .gap_1()
            .child(
                // Header
                h_flex().px_2().pt_2().pb_1().items_center().child(
                    div()
                        .text_sm()
                        .font_weight(FontWeight::MEDIUM)
                        .text_color(theme.foreground)
                        .child(title),
                ),
            )
            .child(
                // Scrollable form fields
                div()
                    .id("sql-row-fields")
                    .flex_1()
                    .px_2()
                    .overflow_y_scroll()
                    .child(
                        v_flex().gap_0p5().children(
                            data.column_meta
                                .iter()
                                .zip(self.row_fields.iter())
                                .enumerate()
                                .map(|(index, (col, field))| {
                                    self.render_sql_row_field(index, col, field, cx)
                                        .into_any_element()
                                }),
                        ),
                    ),
            )
            .when_some(self.render_validation_error(), |this, error| {
                this.child(div().px_2().child(error))
            })
            .child(
                // Action buttons
                h_flex()
                    .gap_1p5()
                    .px_2()
                    .py_2()
                    .justify_end()
                    .child(Button::new("cancel-row").label("Discard").small().on_click(
                        cx.listener(|this, _, _window, cx| {
                            this.cancel(cx);
                        }),
                    ))
                    .child(
                        Button::new("save-row")
                            .label(if data.is_new { "Insert" } else { "Update" })
                            .small()
                            .with_variant(ButtonVariant::Primary)
                            .disabled(!has_modifications)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.save(window, cx);
                            })),
                    ),
            )
            .into_any_element()
    }

    /// Check if SQL row form has modifications
    fn has_row_modifications(&self, cx: &App) -> bool {
        let Some(data) = &self.row_data else {
            return false;
        };

        if data.is_new {
            // For new rows, check if any field has a value
            return self.row_fields.iter().enumerate().any(|(index, field)| {
                let col = &data.column_meta[index];
                if col.auto_increment {
                    return false;
                }
                if field.is_null {
                    return true;
                }
                let value = field.input.read(cx).text().to_string();
                !value.is_empty()
            });
        }

        // For existing rows, compare with original values
        for (index, field) in self.row_fields.iter().enumerate() {
            if index >= data.row_values.len() {
                break;
            }
            let original = &data.row_values[index];
            if field.is_null {
                if original != "NULL" {
                    return true;
                }
            } else {
                let current = field.input.read(cx).text().to_string();
                if current != *original {
                    return true;
                }
            }
        }

        false
    }

    fn render_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        match self.mode {
            RowEditorMode::RedisKey => {
                let Some(_data) = self.data.clone() else {
                    return self.render_empty_state(cx).into_any_element();
                };

                v_flex()
                    .id("key-value-editor-content")
                    .size_full()
                    .gap_2()
                    .p_2()
                    .overflow_y_scroll()
                    .child(self.render_key_field(cx))
                    .child(self.render_type_field(cx))
                    .child(self.render_value_field(cx))
                    .child(self.render_ttl_field(cx))
                    .when_some(self.render_validation_error(), |this, error| {
                        this.child(error)
                    })
                    .child(self.render_action_buttons(cx))
                    .into_any_element()
            }
            RowEditorMode::SqlRow => self.render_sql_row_editor(cx).into_any_element(),
        }
    }
}

impl EventEmitter<PanelEvent> for KeyValueEditorPanel {}
impl EventEmitter<KeyValueEditorEvent> for KeyValueEditorPanel {}

impl Render for KeyValueEditorPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .id("key-value-editor-panel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .child(self.render_editor(window, cx))
    }
}

impl Focusable for KeyValueEditorPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Panel for KeyValueEditorPanel {
    fn panel_name(&self) -> &'static str {
        "Key Editor"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        match self.mode {
            RowEditorMode::RedisKey => "Key Editor".to_string(),
            RowEditorMode::SqlRow => "Row Editor".to_string(),
        }
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        false
    }
}
