//! Cell editor panel
//!
//! A resizable panel for editing cell values with syntax highlighting,
//! validation, and hex dump viewing for binary/blob columns.

use std::ops::Range;

use gpui::prelude::FluentBuilder;
use gpui::*;
use serde_json;
use uuid::Uuid;
use zqlz_ui::widgets::{
    button::{Button, ButtonVariant, ButtonVariants},
    checkbox::Checkbox,
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    input::{Input, InputState},
    v_flex, ActiveTheme, Disableable, Icon, Sizable, ZqlzIcon,
};

use super::TableViewerPanel;

/// Cell data being edited
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct CellData {
    pub table_name: String,
    pub column_name: String,
    pub column_type: String,
    pub row_id: Option<String>,        // Primary key value for updating
    pub current_value: Option<String>, // None means NULL
    pub row_index: usize,
    pub col_index: usize,
    pub connection_id: Uuid,
    pub all_row_values: Vec<String>, // All column values for this row (for identifying the row)
    pub all_column_names: Vec<String>, // Column names corresponding to all_row_values
    pub all_column_types: Vec<String>, // Database column types for type-aware value parsing
    pub raw_bytes: Option<Vec<u8>>,  // Raw binary data for blob/binary columns
}

/// Events from the cell editor
#[derive(Clone, Debug)]
pub enum CellEditorEvent {
    /// Cell value was saved - application should handle the actual update
    ValueSaved {
        cell_data: CellData,
        new_value: Option<String>,
        /// The source table viewer that should be updated after save
        source_viewer: Option<WeakEntity<TableViewerPanel>>,
    },
    /// Editor was closed without saving
    Cancelled,
}

/// Which view mode the hex viewer is showing
#[derive(Clone, Copy, Debug, PartialEq)]
enum BinaryViewMode {
    /// Classic hex dump: offset | hex pairs | ASCII
    Hex,
    /// Decoded as UTF-8 text (if valid)
    Text,
}

/// Cell editor panel
#[allow(dead_code)]
pub struct CellEditorPanel {
    focus_handle: FocusHandle,

    /// Current cell being edited
    cell_data: Option<CellData>,

    /// Input state for editing (used for text mode, not binary hex view)
    editor_input: Entity<InputState>,

    /// NULL checkbox state
    is_null: bool,

    /// Validation error message
    validation_error: Option<String>,

    /// Whether the value has been modified
    is_modified: bool,

    /// Subscription to editor input changes
    _input_subscription: Option<gpui::Subscription>,

    /// The source table viewer panel (for updating after save)
    source_viewer: Option<WeakEntity<TableViewerPanel>>,

    /// Word wrap enabled
    word_wrap: bool,

    /// Current view mode for binary data
    binary_view_mode: BinaryViewMode,

    /// Pre-formatted hex dump lines (cached to avoid recomputing each render)
    hex_dump_lines: Vec<HexDumpLine>,

    /// Pre-decoded UTF-8 text from binary data (None if not valid UTF-8)
    decoded_text: Option<String>,

    /// Text editor for the decoded UTF-8 view of binary data
    text_view_input: Option<Entity<InputState>>,

    /// Scroll handle for the virtualized hex dump list
    hex_scroll_handle: UniformListScrollHandle,
}

/// A single pre-formatted line of a hex dump.
///
/// Each field is a complete string ready to render as a single text element,
/// eliminating the per-byte div overhead that caused scroll jank.
#[derive(Clone, Debug)]
struct HexDumpLine {
    /// e.g. "00000000"
    offset: SharedString,
    /// e.g. "48 54 4D 4C 20 3C 21 44  4F 43 54 59 50 45 20 68"
    hex: SharedString,
    /// e.g. "HTML <!DOCTYPE h"
    ascii: SharedString,
}

/// Bytes per line in the hex dump display
const HEX_BYTES_PER_LINE: usize = 16;

#[allow(dead_code)]
impl CellEditorPanel {
    #[allow(dead_code)]
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let editor_input = cx.new(|cx| {
            InputState::new(window, cx)
                .multi_line(true)
                .soft_wrap(true)
                .placeholder("Enter value...")
        });

        Self {
            focus_handle: cx.focus_handle(),
            cell_data: None,
            editor_input,
            is_null: false,
            validation_error: None,
            is_modified: false,
            _input_subscription: None,
            source_viewer: None,
            word_wrap: true,
            binary_view_mode: BinaryViewMode::Hex,
            hex_dump_lines: Vec::new(),
            decoded_text: None,
            text_view_input: None,
            hex_scroll_handle: UniformListScrollHandle::new(),
        }
    }

    /// Check if the current cell has binary data to display
    fn has_binary_data(&self) -> bool {
        self.cell_data
            .as_ref()
            .map(|d| d.raw_bytes.is_some())
            .unwrap_or(false)
    }

    /// Pre-format all hex dump lines from raw bytes.
    ///
    /// Each line becomes three `SharedString`s (offset, hex, ascii) so the
    /// renderer only needs 3 text children per line instead of 30+ individual
    /// byte divs. Uses lowercase hex to match conventional hex editor style.
    fn build_hex_dump(bytes: &[u8]) -> Vec<HexDumpLine> {
        bytes
            .chunks(HEX_BYTES_PER_LINE)
            .enumerate()
            .map(|(chunk_idx, chunk)| {
                let offset = format!("{:04x}", chunk_idx * HEX_BYTES_PER_LINE);

                let mut hex = String::with_capacity(3 * HEX_BYTES_PER_LINE + 1);
                for (byte_idx, byte) in chunk.iter().enumerate() {
                    if byte_idx == 8 {
                        hex.push(' ');
                    }
                    if byte_idx > 0 && byte_idx != 8 {
                        hex.push(' ');
                    }
                    hex.push_str(&format!("{:02x}", byte));
                }
                // Pad to fixed width so the ASCII column stays aligned on short last lines
                let full_width = HEX_BYTES_PER_LINE * 3 - 1 + 1;
                while hex.len() < full_width {
                    hex.push(' ');
                }

                let ascii: String = chunk
                    .iter()
                    .map(|byte| {
                        if (0x20..=0x7E).contains(byte) {
                            *byte as char
                        } else {
                            '.'
                        }
                    })
                    .collect();

                HexDumpLine {
                    offset: SharedString::from(offset),
                    hex: SharedString::from(hex),
                    ascii: SharedString::from(ascii),
                }
            })
            .collect()
    }

    /// Load a cell for editing
    pub fn edit_cell(
        &mut self,
        cell_data: CellData,
        source_viewer: Option<WeakEntity<TableViewerPanel>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        tracing::info!(
            "Loading cell for editing: table={}, column={}, type={}, value={:?}, has_bytes={}",
            cell_data.table_name,
            cell_data.column_name,
            cell_data.column_type,
            cell_data.current_value,
            cell_data.raw_bytes.is_some()
        );

        self.is_null = cell_data.current_value.is_none();

        // Handle binary data: build hex dump and attempt UTF-8 decode
        if let Some(ref bytes) = cell_data.raw_bytes {
            self.hex_dump_lines = Self::build_hex_dump(bytes);
            self.decoded_text = String::from_utf8(bytes.clone()).ok();
            self.binary_view_mode = BinaryViewMode::Hex;
            self.hex_scroll_handle = UniformListScrollHandle::new();

            // Create a read-only text view for the decoded text tab
            if let Some(ref text) = self.decoded_text {
                let text_clone = text.clone();
                self.text_view_input = Some(cx.new(|cx| {
                    InputState::new(window, cx)
                        .multi_line(true)
                        .soft_wrap(true)
                        .placeholder("(decoded text)")
                }));
                if let Some(ref input) = self.text_view_input {
                    input.update(cx, |state, cx| {
                        state.set_value(text_clone, window, cx);
                    });
                }
            } else {
                self.text_view_input = None;
            }

            self.cell_data = Some(cell_data);
            self.source_viewer = source_viewer;
            self.is_modified = false;
            self.validation_error = None;
            cx.notify();
            return;
        }

        // Non-binary path: normal text editing
        self.hex_dump_lines.clear();
        self.decoded_text = None;
        self.text_view_input = None;

        let value = cell_data.current_value.clone().unwrap_or_default();
        let language = self.detect_language(&cell_data.column_type);
        let is_json = self.is_json_column(&cell_data.column_type);

        let formatted_value = if is_json {
            self.format_json(&value).unwrap_or(value)
        } else {
            value
        };

        let word_wrap = self.word_wrap;
        self.editor_input = cx.new(|cx| {
            let mut input = InputState::new(window, cx)
                .multi_line(true)
                .soft_wrap(word_wrap)
                .placeholder("Enter value...");

            if let Some(lang) = language {
                tracing::info!("Enabling syntax highlighting for language: {}", lang);
                input = input.code_editor(lang).line_number(true);
            }

            input
        });

        self.editor_input.update(cx, |input, cx| {
            input.set_value(formatted_value, window, cx);
        });

        self._input_subscription = None;

        self.cell_data = Some(cell_data);
        self.source_viewer = source_viewer;
        self.is_modified = false;
        self.validation_error = None;

        cx.notify();
    }

    /// Check if the current value has been modified from the original
    fn has_modifications(&self, cx: &App) -> bool {
        let Some(cell_data) = &self.cell_data else {
            return false;
        };

        // Binary data is read-only in the hex viewer
        if cell_data.raw_bytes.is_some() {
            return false;
        }

        let original_is_null = cell_data.current_value.is_none();
        if self.is_null != original_is_null {
            return true;
        }

        if self.is_null {
            return false;
        }

        let current_value = self.editor_input.read(cx).text().to_string();
        let original_value = cell_data.current_value.clone().unwrap_or_default();

        if self.is_json_column(&cell_data.column_type) {
            if let (Ok(current_json), Ok(original_json)) = (
                serde_json::from_str::<serde_json::Value>(&current_value),
                serde_json::from_str::<serde_json::Value>(&original_value),
            ) {
                return current_json != original_json;
            }
        }

        current_value != original_value
    }

    fn is_json_column(&self, column_type: &str) -> bool {
        let lower = column_type.to_lowercase();
        lower.contains("json") || lower.contains("jsonb")
    }

    /// Detect the appropriate syntax highlighting language based on column type
    fn detect_language(&self, column_type: &str) -> Option<&'static str> {
        let lower = column_type.to_lowercase();

        if lower.contains("json") || lower.contains("jsonb") {
            return Some("json");
        }
        if lower.contains("xml") {
            return Some("html");
        }
        if lower.contains("html") {
            return Some("html");
        }
        if lower.contains("sql") {
            return Some("sql");
        }
        if lower.contains("javascript") || lower.contains("js") {
            return Some("javascript");
        }
        if lower.contains("typescript") || lower.contains("ts") {
            return Some("typescript");
        }
        if lower.contains("markdown") || lower.contains("md") {
            return Some("markdown");
        }
        if lower.contains("rust") || lower.contains("rs") {
            return Some("rust");
        }
        if lower.contains("go") || lower.contains("golang") {
            return Some("go");
        }
        if lower.contains("zig") {
            return Some("zig");
        }

        let col_name_lower = column_type.to_lowercase();
        if lower.contains("text") || lower.contains("varchar") || lower.contains("char") {
            if col_name_lower.contains("code")
                || col_name_lower.contains("script")
                || col_name_lower.contains("source")
            {
                return Some("text");
            }
        }

        None
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
        let current_value = self.editor_input.read(cx).text().to_string();

        match self.format_json(&current_value) {
            Some(formatted) => {
                tracing::info!(
                    "Formatting JSON: {} chars -> {} chars",
                    current_value.len(),
                    formatted.len()
                );
                self.editor_input.update(cx, |input, cx| {
                    input.set_value(formatted, window, cx);
                });
                self.validation_error = None;
                cx.notify();
            }
            None => {
                tracing::warn!("Failed to format JSON - invalid JSON syntax");
                self.validation_error = Some("Invalid JSON - cannot format".to_string());
                cx.notify();
            }
        }
    }

    fn auto_format(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let current_value = self.editor_input.read(cx).text().to_string();
        let trimmed = current_value.trim();

        if (trimmed.starts_with('{') && trimmed.ends_with('}'))
            || (trimmed.starts_with('[') && trimmed.ends_with(']'))
        {
            if let Some(formatted) = self.format_json(&current_value) {
                tracing::info!("Auto-detected JSON and formatted");
                self.editor_input.update(cx, |input, cx| {
                    input.set_value(formatted, window, cx);
                });
                self.validation_error = None;
                cx.notify();
                return;
            }
        }

        let upper = trimmed.to_uppercase();
        if upper.starts_with("SELECT")
            || upper.starts_with("INSERT")
            || upper.starts_with("UPDATE")
            || upper.starts_with("DELETE")
            || upper.starts_with("CREATE")
            || upper.starts_with("ALTER")
            || upper.starts_with("DROP")
            || upper.starts_with("WITH")
        {
            let formatted = sqlformat::format(
                &current_value,
                &sqlformat::QueryParams::None,
                &sqlformat::FormatOptions::default(),
            );
            tracing::info!("Auto-detected SQL and formatted");
            self.editor_input.update(cx, |input, cx| {
                input.set_value(formatted, window, cx);
            });
            self.validation_error = None;
            cx.notify();
            return;
        }

        self.validation_error = Some("Could not detect format (JSON/SQL)".to_string());
        cx.notify();
    }

    fn validate(&mut self, cx: &mut Context<Self>) -> bool {
        self.validation_error = None;

        if self.is_null {
            return true;
        }

        let Some(cell_data) = &self.cell_data else {
            return true;
        };

        let value = self.editor_input.read(cx).text().to_string();

        if self.is_json_column(&cell_data.column_type) && !value.trim().is_empty() {
            if let Err(e) = serde_json::from_str::<serde_json::Value>(&value) {
                self.validation_error = Some(format!("Invalid JSON: {}", e));
                return false;
            }
        }

        true
    }

    fn save(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        tracing::info!("=== CELL EDITOR: Save button clicked ===");
        tracing::debug!("Cell editor save initiated");

        if !self.validate(cx) {
            tracing::warn!("Validation failed, cannot save");
            cx.notify();
            return;
        }

        let Some(cell_data) = self.cell_data.clone() else {
            tracing::error!("No cell data available to save");
            return;
        };

        // Binary data is read-only
        if cell_data.raw_bytes.is_some() {
            return;
        }

        let new_value = if self.is_null {
            None
        } else {
            Some(self.editor_input.read(cx).text().to_string())
        };

        tracing::info!(
            "Cell being saved: table={}, column={}, type={}, row={}, col={}, is_null={}, value_len={}",
            cell_data.table_name,
            cell_data.column_name,
            cell_data.column_type,
            cell_data.row_index,
            cell_data.col_index,
            self.is_null,
            new_value.as_ref().map(|v| v.len()).unwrap_or(0)
        );

        tracing::info!("Emitting ValueSaved event to MainView...");

        cx.emit(CellEditorEvent::ValueSaved {
            cell_data,
            new_value,
            source_viewer: self.source_viewer.clone(),
        });

        self.is_modified = false;
        tracing::info!("CELL EDITOR: Save event emitted successfully");
        cx.notify();
    }

    fn cancel(&mut self, cx: &mut Context<Self>) {
        self.cell_data = None;
        self.is_modified = false;
        self.validation_error = None;
        cx.emit(CellEditorEvent::Cancelled);
        cx.notify();
    }

    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.cell_data = None;
        self.source_viewer = None;
        self.is_modified = false;
        self.validation_error = None;
        self.hex_dump_lines.clear();
        self.decoded_text = None;
        self.text_view_input = None;
        cx.notify();
    }

    #[allow(dead_code)]
    pub fn is_editing_row(&self, table_name: &str, row_index: usize) -> bool {
        self.cell_data
            .as_ref()
            .map(|d| d.table_name == table_name && d.row_index == row_index)
            .unwrap_or(false)
    }

    pub fn is_editing_table(&self, table_name: &str) -> bool {
        self.cell_data
            .as_ref()
            .map(|d| d.table_name == table_name)
            .unwrap_or(false)
    }

    pub fn clear_if_editing_rows(
        &mut self,
        table_name: &str,
        row_indices: &[usize],
        cx: &mut Context<Self>,
    ) {
        if let Some(ref cell_data) = self.cell_data {
            if cell_data.table_name == table_name && row_indices.contains(&cell_data.row_index) {
                self.clear(cx);
            }
        }
    }

    // ---- Rendering helpers ----

    fn get_language_badge_style(lang: &str) -> (Rgba, String) {
        match lang {
            "json" => (rgb(0x3b82f6), "JSON".to_string()),
            "html" => (rgb(0xe34c26), "HTML".to_string()),
            "javascript" => (rgb(0xf7df1e), "JS".to_string()),
            "typescript" => (rgb(0x3178c6), "TS".to_string()),
            "rust" => (rgb(0xce422b), "RUST".to_string()),
            "go" => (rgb(0x00add8), "GO".to_string()),
            "sql" => (rgb(0x336791), "SQL".to_string()),
            "markdown" => (rgb(0x083fa1), "MD".to_string()),
            "zig" => (rgb(0xf7a41d), "ZIG".to_string()),
            _ => (rgb(0x6b7280), lang.to_uppercase()),
        }
    }

    fn render_language_badge(lang: &str) -> impl IntoElement {
        let (bg_color, label) = Self::get_language_badge_style(lang);
        div()
            .px_1p5()
            .py_0p5()
            .rounded_sm()
            .bg(bg_color)
            .text_xs()
            .font_weight(FontWeight::MEDIUM)
            .text_color(rgb(0xffffff))
            .child(label)
    }

    fn render_header(&self, cell_data: &CellData, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let is_binary = cell_data.raw_bytes.is_some();
        let detected_language = if is_binary {
            None
        } else {
            self.detect_language(&cell_data.column_type)
        };

        v_flex()
            .gap_1()
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.foreground)
                            .child(format!(
                                "{}.{}",
                                cell_data.table_name, cell_data.column_name
                            )),
                    )
                    .when(is_binary, |this| this.child(Self::render_binary_badge()))
                    .when_some(detected_language, |this, lang| {
                        this.child(Self::render_language_badge(lang))
                    }),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(format!("Type: {}", cell_data.column_type)),
            )
    }

    /// Badge indicating binary/blob data
    fn render_binary_badge() -> impl IntoElement {
        div()
            .px_1p5()
            .py_0p5()
            .rounded_sm()
            .bg(rgb(0x8b5cf6))
            .text_xs()
            .font_weight(FontWeight::MEDIUM)
            .text_color(rgb(0xffffff))
            .child("BINARY")
    }

    fn render_null_checkbox(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        h_flex()
            .gap_2()
            .items_center()
            .child(
                Checkbox::new("is-null")
                    .checked(self.is_null)
                    .on_click(cx.listener(|this, _checked, _window, cx| {
                        this.is_null = !this.is_null;
                        cx.notify();
                    })),
            )
            .child(div().text_sm().text_color(theme.foreground).child("NULL"))
    }

    fn render_format_json_button(&self, cx: &mut Context<Self>) -> impl IntoElement {
        Button::new("format-json")
            .icon(Icon::new(ZqlzIcon::BracketsCurly).size_4())
            .with_variant(ButtonVariant::Ghost)
            .tooltip("Format JSON")
            .on_click(cx.listener(|this, _, window, cx| {
                this.format_json_in_editor(window, cx);
            }))
    }

    fn render_auto_format_button(&self, cx: &mut Context<Self>) -> impl IntoElement {
        Button::new("auto-format")
            .icon(Icon::new(ZqlzIcon::MagicWand).size_4())
            .with_variant(ButtonVariant::Ghost)
            .tooltip("Auto Format (JSON/SQL)")
            .on_click(cx.listener(|this, _, window, cx| {
                this.auto_format(window, cx);
            }))
    }

    fn render_word_wrap_button(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let is_wrapped = self.word_wrap;
        Button::new("word-wrap")
            .icon(Icon::new(ZqlzIcon::TextWrap).size_4())
            .with_variant(if is_wrapped {
                ButtonVariant::Secondary
            } else {
                ButtonVariant::Ghost
            })
            .tooltip(if is_wrapped {
                "Disable Word Wrap"
            } else {
                "Enable Word Wrap"
            })
            .on_click(cx.listener(|this, _, window, cx| {
                this.word_wrap = !this.word_wrap;
                this.editor_input.update(cx, |input, cx| {
                    input.set_soft_wrap(this.word_wrap, window, cx);
                });
                cx.notify();
            }))
    }

    fn render_format_toolbar(
        &self,
        is_json_column: bool,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        h_flex()
            .gap_1()
            .when(is_json_column, |this| {
                this.child(self.render_format_json_button(cx))
            })
            .when(!is_json_column, |this| {
                this.child(self.render_auto_format_button(cx))
            })
            .child(self.render_word_wrap_button(cx))
    }

    fn render_input_area(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        if self.is_null {
            div().flex_1().w_full().child(
                div()
                    .w_full()
                    .p_2()
                    .rounded_md()
                    .bg(theme.muted)
                    .text_color(theme.muted_foreground)
                    .child("Value is NULL"),
            )
        } else {
            div()
                .flex_1()
                .w_full()
                .flex()
                .flex_col()
                .child(Input::new(&self.editor_input).w_full().h_full())
        }
    }

    fn render_validation_error(&self) -> Option<impl IntoElement> {
        self.validation_error.as_ref().map(|error| {
            div()
                .text_xs()
                .text_color(rgb(0xff0000))
                .child(error.clone())
        })
    }

    fn render_action_buttons(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let has_modifications = self.has_modifications(cx);
        h_flex()
            .gap_2()
            .justify_end()
            .child(
                Button::new("cancel-edit")
                    .label("Cancel")
                    .on_click(cx.listener(|this, _, _window, cx| {
                        this.cancel(cx);
                    })),
            )
            .child(
                Button::new("save-edit")
                    .label("Save")
                    .with_variant(ButtonVariant::Primary)
                    .disabled(!has_modifications)
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.save(window, cx);
                    })),
            )
    }

    fn render_empty_state(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        v_flex().size_full().items_center().justify_center().child(
            div()
                .text_sm()
                .text_color(theme.muted_foreground)
                .child("Select a cell to edit"),
        )
    }

    // ---- Binary / Hex view rendering ----

    /// Render the tab bar for switching between Hex and Text views
    fn render_binary_view_tabs(
        &self,
        has_text: bool,
        byte_count: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        // Extract the color value before building the element tree to avoid
        // holding an immutable borrow of cx across mutable closure boundaries
        let muted_foreground = cx.theme().muted_foreground;
        let current_mode = self.binary_view_mode;

        h_flex()
            .gap_1()
            .items_center()
            .child(self.render_view_tab("Hex", BinaryViewMode::Hex, current_mode, cx))
            .when(has_text, |this| {
                this.child(self.render_view_tab("Text", BinaryViewMode::Text, current_mode, cx))
            })
            .child(
                div()
                    .ml_auto()
                    .text_xs()
                    .text_color(muted_foreground)
                    .child(format_byte_size(byte_count)),
            )
    }

    /// Render a single tab button for the binary view mode switcher
    fn render_view_tab(
        &self,
        label: &'static str,
        mode: BinaryViewMode,
        current: BinaryViewMode,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let is_active = mode == current;
        Button::new(SharedString::from(format!("tab-{}", label.to_lowercase())))
            .label(label)
            .with_variant(if is_active {
                ButtonVariant::Secondary
            } else {
                ButtonVariant::Ghost
            })
            .xsmall()
            .on_click(cx.listener(move |this, _, _window, cx| {
                this.binary_view_mode = mode;
                cx.notify();
            }))
    }

    /// Render the hex dump view using `uniform_list` for virtualization.
    ///
    /// Only visible lines are rendered (typically 20-40), regardless of total
    /// data size. A 20KB blob (1260 lines) renders the same number of elements
    /// as a 100-byte blob — zero scroll jank at any data size.
    fn render_hex_dump(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let line_count = self.hex_dump_lines.len();

        let offset_color = theme.muted_foreground;
        let hex_color = theme.foreground;
        let ascii_color = hsla(142.0 / 360.0, 0.69, 0.58, 1.0);
        let separator_color = theme.border;
        let muted_bg = theme.muted;

        let header = h_flex()
            .gap_0()
            .px_2()
            .py_1()
            .border_b_1()
            .border_color(separator_color)
            .child(
                div()
                    .w(px(72.))
                    .flex_shrink_0()
                    .text_xs()
                    .font_family("Berkeley Mono")
                    .text_color(offset_color)
                    .child("Offset"),
            )
            .child(
                div()
                    .min_w(px(340.))
                    .flex_1()
                    .flex_shrink_0()
                    .text_xs()
                    .font_family("Berkeley Mono")
                    .text_color(offset_color)
                    .child("00 01 02 03 04 05 06 07  08 09 0a 0b 0c 0d 0e 0f"),
            )
            .child(
                div()
                    .w(px(140.))
                    .flex_shrink_0()
                    .pl_2()
                    .text_xs()
                    .font_family("Berkeley Mono")
                    .text_color(offset_color)
                    .child("ASCII"),
            );

        let virtualized_rows = uniform_list(
            "hex-dump-lines",
            line_count,
            cx.processor(
                move |state: &mut CellEditorPanel, visible_range: Range<usize>, _window, _cx| {
                    let mut items = Vec::with_capacity(visible_range.len());

                    for ix in visible_range {
                        let line = &state.hex_dump_lines[ix];
                        let row = h_flex()
                            .id(ix)
                            .gap_0()
                            .px_2()
                            .whitespace_nowrap()
                            .overflow_hidden()
                            .hover(|style| style.bg(muted_bg.opacity(0.5)))
                            .child(
                                div()
                                    .w(px(72.))
                                    .flex_shrink_0()
                                    .text_xs()
                                    .font_family("Berkeley Mono")
                                    .text_color(offset_color)
                                    .child(line.offset.clone()),
                            )
                            .child(
                                div()
                                    .min_w(px(340.))
                                    .flex_1()
                                    .flex_shrink_0()
                                    .text_xs()
                                    .font_family("Berkeley Mono")
                                    .text_color(hex_color)
                                    .child(line.hex.clone()),
                            )
                            .child(
                                div()
                                    .w(px(140.))
                                    .flex_shrink_0()
                                    .border_l_1()
                                    .border_color(separator_color.opacity(0.3))
                                    .pl_2()
                                    .text_xs()
                                    .font_family("Berkeley Mono")
                                    .text_color(ascii_color)
                                    .child(line.ascii.clone()),
                            );

                        items.push(row);
                    }

                    items
                },
            ),
        )
        .flex_grow()
        .size_full()
        .track_scroll(&self.hex_scroll_handle)
        .with_sizing_behavior(ListSizingBehavior::Auto);

        v_flex()
            .flex_1()
            .w_full()
            .rounded_md()
            .border_1()
            .border_color(separator_color)
            .bg(muted_bg.opacity(0.3))
            .child(header)
            .child(virtualized_rows)
    }

    /// Render the text preview for decoded UTF-8 binary data
    fn render_text_view(&self, cx: &Context<Self>) -> impl IntoElement {
        if let Some(ref input) = self.text_view_input {
            div()
                .flex_1()
                .w_full()
                .flex()
                .flex_col()
                .child(Input::new(input).w_full().h_full())
        } else {
            let theme = cx.theme();
            div().flex_1().w_full().child(
                div()
                    .w_full()
                    .p_2()
                    .rounded_md()
                    .bg(theme.muted)
                    .text_color(theme.muted_foreground)
                    .child("Not valid UTF-8 text"),
            )
        }
    }

    /// Render the binary data viewer (hex dump + text preview tabs)
    fn render_binary_editor(
        &mut self,
        cell_data: &CellData,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let byte_count = cell_data.raw_bytes.as_ref().map(|b| b.len()).unwrap_or(0);
        let has_text = self.decoded_text.is_some();
        let muted_foreground = cx.theme().muted_foreground;
        let border_color = cx.theme().border;

        v_flex()
            .size_full()
            .gap_3()
            .p_3()
            .child(self.render_header(cell_data, cx))
            .child(self.render_binary_view_tabs(has_text, byte_count, cx))
            .child(match self.binary_view_mode {
                BinaryViewMode::Hex => self.render_hex_dump(cx).into_any_element(),
                BinaryViewMode::Text => self.render_text_view(cx).into_any_element(),
            })
            .child(
                h_flex()
                    .gap_4()
                    .items_center()
                    .pt_1()
                    .border_t_1()
                    .border_color(border_color)
                    .child(div().text_xs().text_color(muted_foreground).child(format!(
                        "Editing row={}, column={}",
                        cell_data.row_index, cell_data.col_index
                    )))
                    .child(
                        div()
                            .text_xs()
                            .text_color(muted_foreground)
                            .child(format!("Type: Binary; Size: {} byte(s)", byte_count)),
                    )
                    .child(
                        div()
                            .ml_auto()
                            .child(Button::new("close-binary").label("Close").on_click(
                                cx.listener(|this, _, _window, cx| {
                                    this.cancel(cx);
                                }),
                            )),
                    ),
            )
    }

    fn render_editor(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(cell_data) = self.cell_data.clone() else {
            return self.render_empty_state(cx).into_any_element();
        };

        // Binary data gets the hex viewer
        if cell_data.raw_bytes.is_some() {
            return self.render_binary_editor(&cell_data, cx).into_any_element();
        }

        // Normal text editing
        let is_json = self.is_json_column(&cell_data.column_type);
        let show_format_toolbar = !self.is_null;

        v_flex()
            .size_full()
            .gap_3()
            .p_3()
            .child(self.render_header(&cell_data, cx))
            .child(
                h_flex()
                    .gap_4()
                    .items_center()
                    .child(self.render_null_checkbox(cx))
                    .when(show_format_toolbar, |this| {
                        this.child(self.render_format_toolbar(is_json, cx))
                    }),
            )
            .child(self.render_input_area(cx))
            .when_some(self.render_validation_error(), |this, error| {
                this.child(error)
            })
            .child(self.render_action_buttons(cx))
            .into_any_element()
    }
}

/// Format byte count into a human-readable size string
fn format_byte_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{} bytes", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

impl EventEmitter<PanelEvent> for CellEditorPanel {}
impl EventEmitter<CellEditorEvent> for CellEditorPanel {}

impl Render for CellEditorPanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .id("cell-editor-panel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .child(self.render_editor(window, cx))
    }
}

impl Focusable for CellEditorPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Panel for CellEditorPanel {
    fn panel_name(&self) -> &'static str {
        "Cell Editor"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        if let Some(cell_data) = &self.cell_data {
            format!("{}.{}", cell_data.table_name, cell_data.column_name)
        } else {
            "Cell Editor".to_string()
        }
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        false
    }
}
