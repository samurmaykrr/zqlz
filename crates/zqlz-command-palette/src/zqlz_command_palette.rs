use gpui::prelude::FluentBuilder;
use gpui::*;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;
use uuid::Uuid;
use zqlz_connection::SidebarObjectCapabilities;
use zqlz_fuzzy::{FuzzyMatcher, MatchQuality};
use zqlz_ui::widgets::{
    ActiveTheme, Icon, IconName, IndexPath, Sizable, ZqlzIcon,
    kbd::Kbd,
    list::{ListDelegate, ListEvent, ListItem, ListState},
    v_flex,
};

// ── Persistence ─────────────────────────────────────────────────────────

/// A single command usage record for persistence.
#[derive(Clone, Debug)]
pub struct CommandUsageEntry {
    pub command_id: String,
    pub use_count: f32,
    pub last_used: SystemTime,
}

/// Persistence backend for command usage data. Implementors durably store
/// usage stats so frecency rankings survive across app restarts.
///
/// Follows the same decoupled pattern as `HistoryPersistence` from
/// `zqlz-query` — the command palette crate defines the trait, and the
/// storage crate provides the concrete implementation.
pub trait CommandUsagePersistence: Send + Sync {
    /// Write or update a usage entry for a single command.
    fn persist_usage(&self, entry: &CommandUsageEntry);

    /// Load all persisted usage entries.
    fn load_all(&self) -> Vec<CommandUsageEntry>;

    /// Remove all stored usage data.
    fn clear_all(&self);
}

// ── Category definitions ────────────────────────────────────────────────

/// Semantic category for a command. Determines which section header a command
/// appears under and its sort position. Using an enum instead of raw strings
/// avoids fragile prefix-matching when normalizing dynamic categories.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CommandCategory {
    Application,
    Connection,
    Query,
    Editor,
    Layout,
    Tab,
    Focus,
    /// Table commands from a specific connection. The string is the
    /// connection display name (e.g. "production-db").
    Table(String),
    /// View commands from a specific connection.
    View(String),
}

impl CommandCategory {
    /// The display string shown in search results as contextual info.
    pub fn display_label(&self) -> String {
        match self {
            Self::Application => "Application".to_string(),
            Self::Connection => "Connection".to_string(),
            Self::Query => "Query".to_string(),
            Self::Editor => "Editor".to_string(),
            Self::Layout => "Layout".to_string(),
            Self::Tab => "Tab".to_string(),
            Self::Focus => "Focus".to_string(),
            Self::Table(name) => format!("Table: {name}"),
            Self::View(name) => format!("View: {name}"),
        }
    }

    /// The section header name for grouping in the unfiltered palette view.
    fn section_name(&self) -> &'static str {
        match self {
            Self::Application => "Application",
            Self::Connection => "Connection",
            Self::Query => "Query",
            Self::Editor => "Editor",
            Self::Layout => "Layout",
            Self::Tab => "Tab",
            Self::Focus => "Focus",
            Self::Table(_) => "Tables",
            Self::View(_) => "Views",
        }
    }
}

impl fmt::Display for CommandCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display_label())
    }
}

fn icon_for_command(command_id: &str, category: &CommandCategory) -> Option<AnyElement> {
    match command_id {
        "settings" => Some(Icon::new(IconName::Settings).small().into_any_element()),
        "command-palette" => Some(Icon::new(IconName::Palette).small().into_any_element()),
        "quit" => None,
        "refresh" => Some(
            Icon::new(ZqlzIcon::ArrowsClockwise)
                .small()
                .into_any_element(),
        ),
        "new-connection" => Some(Icon::new(ZqlzIcon::Plus).small().into_any_element()),
        "refresh-connection" => Some(
            Icon::new(ZqlzIcon::ArrowsClockwise)
                .small()
                .into_any_element(),
        ),
        "refresh-connections-list" => Some(
            Icon::new(ZqlzIcon::ArrowsClockwise)
                .small()
                .into_any_element(),
        ),
        "new-query" => Some(Icon::new(ZqlzIcon::FileSql).small().into_any_element()),
        "execute-query" | "execute-selection" | "execute-current-statement" => {
            Some(Icon::new(ZqlzIcon::Play).small().into_any_element())
        }
        "explain-query" | "explain-selection" => {
            Some(Icon::new(ZqlzIcon::Lightbulb).small().into_any_element())
        }
        "stop-query" => Some(Icon::new(ZqlzIcon::Stop).small().into_any_element()),
        "format-query" => Some(Icon::new(ZqlzIcon::MagicWand).small().into_any_element()),
        "save-query" | "save-query-as" => {
            Some(Icon::new(ZqlzIcon::FloppyDisk).small().into_any_element())
        }
        "toggle-problems-panel" => Some(Icon::new(ZqlzIcon::Warning).small().into_any_element()),
        "toggle-left-sidebar" => Some(Icon::new(IconName::PanelLeft).small().into_any_element()),
        "toggle-right-sidebar" => Some(Icon::new(IconName::PanelRight).small().into_any_element()),
        "toggle-bottom-panel" => Some(Icon::new(IconName::PanelBottom).small().into_any_element()),
        "close-tab" => Some(Icon::new(IconName::Close).small().into_any_element()),
        "duplicate-line" => Some(Icon::new(ZqlzIcon::Copy).small().into_any_element()),
        "move-line-up" => Some(Icon::new(ZqlzIcon::ArrowUp).small().into_any_element()),
        "move-line-down" => Some(Icon::new(ZqlzIcon::ArrowDown).small().into_any_element()),
        "find-next" | "find-previous" => Some(
            Icon::new(ZqlzIcon::MagnifyingGlass)
                .small()
                .into_any_element(),
        ),
        _ => match category {
            CommandCategory::Focus => None,
            CommandCategory::Table(_) => {
                Some(Icon::new(ZqlzIcon::Table).small().into_any_element())
            }
            CommandCategory::View(_) => Some(Icon::new(ZqlzIcon::Eye).small().into_any_element()),
            CommandCategory::Connection => {
                Some(Icon::new(ZqlzIcon::Plug).small().into_any_element())
            }
            _ => None,
        },
    }
}

// ── Command ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub enum CommandType {
    Static,
    Table {
        connection_id: Uuid,
        table_name: String,
    },
    View {
        connection_id: Uuid,
        view_name: String,
    },
    Connection {
        connection_id: Uuid,
    },
}

/// A command that can be executed from the palette.
pub struct Command {
    pub id: String,
    pub label: String,
    pub category: CommandCategory,
    pub command_type: CommandType,
    pub score: f32,
    pub last_used: Option<SystemTime>,
    /// The action to dispatch when executing this command.
    action: Option<Box<dyn Action>>,
    /// Extra event to emit back to the host (for connections, tables, views).
    palette_event: Option<CommandPaletteEvent>,
    /// Pre-computed lowercased label to avoid per-keystroke allocation.
    label_lower: String,
    /// Pre-computed "category label" string for combined matching.
    combined_search: String,
}

impl Clone for Command {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            label: self.label.clone(),
            category: self.category.clone(),
            command_type: self.command_type.clone(),
            score: self.score,
            last_used: self.last_used,
            action: self.action.as_ref().map(|a| a.boxed_clone()),
            palette_event: self.palette_event.clone(),
            label_lower: self.label_lower.clone(),
            combined_search: self.combined_search.clone(),
        }
    }
}

impl Command {
    /// Build the pre-computed search fields from the label and category.
    fn with_search_fields(mut self) -> Self {
        self.label_lower = self.label.to_lowercase();
        self.combined_search = format!("{} {}", self.category.display_label(), self.label);
        self
    }

    pub fn new_static(
        id: impl Into<String>,
        label: impl Into<String>,
        category: CommandCategory,
        action: impl Action,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            category,
            command_type: CommandType::Static,
            score: 0.0,
            last_used: None,
            action: Some(action.boxed_clone()),
            palette_event: None,
            label_lower: String::new(),
            combined_search: String::new(),
        }
        .with_search_fields()
    }

    pub fn new_table(connection_id: Uuid, connection_name: &str, table_name: String) -> Self {
        let category = CommandCategory::Table(connection_name.to_string());
        Self {
            id: format!("table-{}-{}", connection_id, table_name),
            label: table_name.clone(),
            category,
            command_type: CommandType::Table {
                connection_id,
                table_name: table_name.clone(),
            },
            score: 0.0,
            last_used: None,
            action: None,
            palette_event: Some(CommandPaletteEvent::OpenTable {
                connection_id,
                table_name,
            }),
            label_lower: String::new(),
            combined_search: String::new(),
        }
        .with_search_fields()
    }

    pub fn new_view(connection_id: Uuid, connection_name: &str, view_name: String) -> Self {
        let category = CommandCategory::View(connection_name.to_string());
        Self {
            id: format!("view-{}-{}", connection_id, view_name),
            label: view_name.clone(),
            category,
            command_type: CommandType::View {
                connection_id,
                view_name: view_name.clone(),
            },
            score: 0.0,
            last_used: None,
            action: None,
            palette_event: Some(CommandPaletteEvent::OpenView {
                connection_id,
                view_name,
            }),
            label_lower: String::new(),
            combined_search: String::new(),
        }
        .with_search_fields()
    }

    pub fn new_connection(connection_id: Uuid, connection_name: String) -> Self {
        Self {
            id: format!("connection-{}", connection_id),
            label: format!("Connect to: {}", connection_name),
            category: CommandCategory::Connection,
            command_type: CommandType::Connection { connection_id },
            score: 0.0,
            last_used: None,
            action: None,
            palette_event: Some(CommandPaletteEvent::ConnectToConnection(connection_id)),
            label_lower: String::new(),
            combined_search: String::new(),
        }
        .with_search_fields()
    }
}

// ── Events ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum CommandPaletteEvent {
    Dismissed,
    CommandExecuted(String),
    ConnectToConnection(Uuid),
    OpenTable {
        connection_id: Uuid,
        table_name: String,
    },
    OpenView {
        connection_id: Uuid,
        view_name: String,
    },
}

// ── Usage tracking ──────────────────────────────────────────────────────

/// Prevent unbounded growth from dynamic table/view commands that accumulate
/// across many connections over a long session.
const MAX_USAGE_ENTRIES: usize = 500;

#[derive(Clone, Debug, Default)]
struct CommandUsageStats {
    command_scores: HashMap<String, f32>,
    last_used: HashMap<String, SystemTime>,
}

impl CommandUsageStats {
    fn record_usage(
        &mut self,
        command_id: &str,
        persistence: Option<&dyn CommandUsagePersistence>,
    ) {
        let now = SystemTime::now();
        *self
            .command_scores
            .entry(command_id.to_string())
            .or_insert(0.0) += 1.0;
        self.last_used.insert(command_id.to_string(), now);

        if let Some(persistence) = persistence {
            let count = self.command_scores.get(command_id).copied().unwrap_or(1.0);
            persistence.persist_usage(&CommandUsageEntry {
                command_id: command_id.to_string(),
                use_count: count,
                last_used: now,
            });
        }

        self.evict_if_needed();
    }

    /// Frecency score: combines raw usage count with recency decay so that
    /// recently used commands rank higher even with fewer total uses.
    /// `score = use_count / (1.0 + hours_since_last_use / 24.0)`
    fn frecency_score(&self, command_id: &str) -> f32 {
        let count = self.command_scores.get(command_id).copied().unwrap_or(0.0);
        if count == 0.0 {
            return 0.0;
        }

        let recency_factor = match self.last_used.get(command_id) {
            Some(&last) => {
                let elapsed = SystemTime::now().duration_since(last).unwrap_or_default();
                let hours = elapsed.as_secs_f32() / 3600.0;
                1.0 / (1.0 + hours / 24.0)
            }
            None => 0.1,
        };

        count * recency_factor
    }

    fn get_last_used(&self, command_id: &str) -> Option<SystemTime> {
        self.last_used.get(command_id).copied()
    }

    /// Hydrate from persisted storage on startup.
    fn load_from(&mut self, entries: Vec<CommandUsageEntry>) {
        for entry in entries {
            self.command_scores
                .insert(entry.command_id.clone(), entry.use_count);
            self.last_used.insert(entry.command_id, entry.last_used);
        }
    }

    /// Drop the oldest entries when the map exceeds the cap.
    fn evict_if_needed(&mut self) {
        if self.last_used.len() <= MAX_USAGE_ENTRIES {
            return;
        }

        let mut entries: Vec<(String, SystemTime)> = self
            .last_used
            .iter()
            .map(|(k, &v)| (k.clone(), v))
            .collect();

        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(MAX_USAGE_ENTRIES);

        let keep: std::collections::HashSet<String> = entries.into_iter().map(|(k, _)| k).collect();

        self.last_used.retain(|k, _| keep.contains(k));
        self.command_scores.retain(|k, _| keep.contains(k));
    }
}

// ── Delegate ────────────────────────────────────────────────────────────

/// Flat filtered-command index used when the search query is active.
struct FilteredEntry {
    command_index: usize,
    /// Character positions in the label that matched, for highlight rendering.
    matched_indices: Vec<usize>,
}

pub struct CommandPaletteDelegate {
    /// All registered commands (static + dynamic).
    commands: Vec<Command>,
    /// Category sections for the unfiltered view. Each entry is
    /// `(category_name, Vec<command_index>)`.
    sections: Vec<(String, Vec<usize>)>,
    /// Flat filtered list used when searching.
    filtered: Vec<FilteredEntry>,
    query: String,
    selected_index: Option<IndexPath>,
    usage_stats: CommandUsageStats,
    fuzzy_matcher: FuzzyMatcher,
    /// Optional persistence backend for durable frecency storage.
    persistence: Option<Arc<dyn CommandUsagePersistence>>,
    /// Workspace focus to restore before executing a command so actions resolve
    /// against the main UI instead of the palette search field.
    action_context: Option<WeakFocusHandle>,
    /// Pending event to be picked up by the wrapper after confirm.
    pub(crate) pending_event: Option<CommandPaletteEvent>,
    /// Pending command id for the CommandExecuted event.
    pub(crate) pending_command_id: Option<String>,
}

impl Default for CommandPaletteDelegate {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandPaletteDelegate {
    /// Create an empty delegate. Call `set_commands` to populate it.
    pub fn new() -> Self {
        Self {
            commands: Vec::new(),
            sections: Vec::new(),
            filtered: Vec::new(),
            query: String::new(),
            selected_index: None,
            usage_stats: CommandUsageStats::default(),
            fuzzy_matcher: FuzzyMatcher::new(false),
            persistence: None,
            action_context: None,
            pending_event: None,
            pending_command_id: None,
        }
    }

    /// Keep track of the focus that owned keyboard handling before the palette
    /// opened so confirmation can execute in that context.
    pub fn set_action_context(&mut self, action_context: Option<WeakFocusHandle>) {
        self.action_context = action_context;
    }

    /// Attach a persistence backend. Immediately loads stored usage data.
    pub fn set_persistence(&mut self, persistence: Arc<dyn CommandUsagePersistence>) {
        let entries = persistence.load_all();
        self.usage_stats.load_from(entries);
        self.persistence = Some(persistence);
    }

    /// Replace the full command list. Usage stats are preserved across resets
    /// so frequently-used commands stay ranked higher.
    pub fn set_commands(&mut self, commands: Vec<Command>) {
        self.commands = commands;
        for command in self.commands.iter_mut() {
            command.score = self.usage_stats.frecency_score(&command.id);
            command.last_used = self.usage_stats.get_last_used(&command.id);
        }
        self.rebuild_sections();
    }

    /// Rebuild the category sections from the full command list.
    fn rebuild_sections(&mut self) {
        let mut section_map: HashMap<&'static str, Vec<usize>> = HashMap::new();

        for (index, command) in self.commands.iter().enumerate() {
            section_map
                .entry(command.category.section_name())
                .or_default()
                .push(index);
        }

        let mut sections: Vec<(String, Vec<usize>)> = section_map
            .into_iter()
            .map(|(name, items)| (name.to_string(), items))
            .collect();

        sections.sort_by_key(|(name, _)| match name.as_str() {
            "Application" => 0,
            "Connection" => 1,
            "Query" => 2,
            "Layout" => 3,
            "Focus" => 4,
            "Tables" => 5,
            "Views" => 6,
            _ => 7,
        });

        sections.retain(|(_, items)| !items.is_empty());
        self.sections = sections;
    }

    fn is_searching(&self) -> bool {
        !self.query.is_empty()
    }

    fn total_commands_count(&self) -> usize {
        self.commands.len()
    }

    fn visible_commands_count(&self) -> usize {
        if self.is_searching() {
            self.filtered.len()
        } else {
            self.sections.iter().map(|(_, items)| items.len()).sum()
        }
    }

    /// Resolve an IndexPath to the underlying command index.
    fn command_index_at(&self, ix: IndexPath) -> Option<usize> {
        if self.is_searching() {
            self.filtered.get(ix.row).map(|f| f.command_index)
        } else {
            self.sections
                .get(ix.section)
                .and_then(|(_, items)| items.get(ix.row))
                .copied()
        }
    }

    /// Get the fuzzy matched character indices for a filtered row.
    fn matched_indices_at(&self, ix: IndexPath) -> &[usize] {
        if self.is_searching() {
            self.filtered
                .get(ix.row)
                .map(|f| f.matched_indices.as_slice())
                .unwrap_or(&[])
        } else {
            &[]
        }
    }

    pub fn add_schema_commands(
        &mut self,
        connection_id: Uuid,
        connection_name: &str,
        tables: &[String],
        views: &[String],
        object_capabilities: SidebarObjectCapabilities,
    ) {
        // Remove existing schema commands for this connection.
        self.commands.retain(|cmd| {
            !matches!(
                cmd.command_type,
                CommandType::Table { connection_id: cid, .. } | CommandType::View { connection_id: cid, .. }
                if cid == connection_id
            )
        });

        for table_name in tables {
            self.commands.push(Command::new_table(
                connection_id,
                connection_name,
                table_name.clone(),
            ));
        }

        if object_capabilities.supports_views {
            for view_name in views {
                self.commands.push(Command::new_view(
                    connection_id,
                    connection_name,
                    view_name.clone(),
                ));
            }
        }

        self.rebuild_sections();
    }

    /// Reset state for a fresh open of the palette. The caller should follow
    /// this with `set_commands` if the command list may have changed.
    pub fn reset(&mut self) {
        self.query.clear();
        self.filtered.clear();
        self.pending_event = None;
        self.pending_command_id = None;
        self.selected_index = if self.visible_commands_count() > 0 {
            Some(IndexPath::default())
        } else {
            None
        };
    }
}

// ── Highlight rendering ─────────────────────────────────────────────────

/// Convert matched character indices into byte ranges with highlight styling,
/// grouping consecutive indices into contiguous ranges.
fn build_highlights_from_indices(
    label: &str,
    indices: &[usize],
    color: Hsla,
) -> Vec<(std::ops::Range<usize>, HighlightStyle)> {
    if indices.is_empty() {
        return vec![];
    }

    let label_chars: Vec<(usize, char)> = label.char_indices().collect();
    let style = HighlightStyle {
        color: Some(color),
        font_weight: Some(FontWeight::BOLD),
        ..Default::default()
    };

    let mut ranges = Vec::new();
    let mut i = 0;
    while i < indices.len() {
        let char_idx = indices[i];
        if char_idx >= label_chars.len() {
            i += 1;
            continue;
        }
        let start_byte = label_chars[char_idx].0;
        let mut end_char_idx = char_idx;

        while i + 1 < indices.len() && indices[i + 1] == end_char_idx + 1 {
            end_char_idx = indices[i + 1];
            i += 1;
        }

        let end_byte = if end_char_idx + 1 < label_chars.len() {
            label_chars[end_char_idx + 1].0
        } else {
            label.len()
        };

        ranges.push((start_byte..end_byte, style));
        i += 1;
    }

    ranges
}

impl ListDelegate for CommandPaletteDelegate {
    type Item = ListItem;

    fn perform_search(
        &mut self,
        query: &str,
        _window: &mut Window,
        _cx: &mut Context<ListState<Self>>,
    ) -> Task<()> {
        self.query = query.to_string();

        if query.is_empty() {
            self.filtered.clear();
            self.rebuild_sections();
        } else {
            // Match against both label and "category label" combined, keeping
            // the best result. This lets "nq" match "Query > New Query" via
            // acronym, and "users" match "Tables > users" via prefix.
            struct ScoredEntry {
                command_index: usize,
                quality: MatchQuality,
                fuzzy_score: i32,
                usage_score: f32,
                matched_indices: Vec<usize>,
            }

            let mut scored: Vec<ScoredEntry> = Vec::new();

            for (idx, cmd) in self.commands.iter().enumerate() {
                let label_match = self.fuzzy_matcher.fuzzy_match(query, &cmd.label);
                let combined_match = self.fuzzy_matcher.fuzzy_match(query, &cmd.combined_search);

                // Pick the best match between label-only and category+label.
                let best = match (label_match, combined_match) {
                    (Some(a), Some(b)) => {
                        if !a.is_match() && !b.is_match() {
                            continue;
                        }
                        if (a.quality, a.score) >= (b.quality, b.score) {
                            a
                        } else {
                            b
                        }
                    }
                    (Some(a), None) if a.is_match() => a,
                    (None, Some(b)) if b.is_match() => b,
                    _ => continue,
                };

                scored.push(ScoredEntry {
                    command_index: idx,
                    quality: best.quality,
                    fuzzy_score: best.score,
                    usage_score: cmd.score,
                    matched_indices: best.matched_indices,
                });
            }

            // Primary sort: match quality tier (descending).
            // Secondary sort: fuzzy score within tier (descending).
            // Tertiary sort: usage frequency (descending).
            scored.sort_by(|a, b| {
                b.quality
                    .cmp(&a.quality)
                    .then_with(|| b.fuzzy_score.cmp(&a.fuzzy_score))
                    .then_with(|| {
                        b.usage_score
                            .partial_cmp(&a.usage_score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
            });

            self.filtered = scored
                .into_iter()
                .map(|entry| FilteredEntry {
                    command_index: entry.command_index,
                    matched_indices: entry.matched_indices,
                })
                .collect();
        }

        self.selected_index = if self.visible_commands_count() > 0 {
            Some(IndexPath::default())
        } else {
            None
        };

        Task::ready(())
    }

    fn sections_count(&self, _cx: &App) -> usize {
        if self.is_searching() {
            1
        } else {
            self.sections.len()
        }
    }

    fn items_count(&self, section: usize, _cx: &App) -> usize {
        if self.is_searching() {
            self.filtered.len()
        } else {
            self.sections
                .get(section)
                .map(|(_, items)| items.len())
                .unwrap_or(0)
        }
    }

    fn render_item(
        &mut self,
        ix: IndexPath,
        _window: &mut Window,
        cx: &mut Context<ListState<Self>>,
    ) -> Option<Self::Item> {
        let command_index = self.command_index_at(ix)?;
        let command = self.commands.get(command_index)?;

        let id = SharedString::from(format!("cmd-{}-{}", ix.section, ix.row));
        let label = command.label.clone();
        let category = command.category.clone();
        let command_id = command.id.clone();
        let action = command.action.as_ref().map(|a| a.boxed_clone());
        let action_context = self.action_context.clone();
        let matched_indices = self.matched_indices_at(ix).to_vec();

        let icon_element = icon_for_command(&command_id, &category);
        let category_display = category.display_label();

        let highlight_color = cx.theme().blue;
        let label_shared: SharedString = label.clone().into();
        let label_element = if matched_indices.is_empty() {
            div().child(label_shared).into_any_element()
        } else {
            let highlights =
                build_highlights_from_indices(&label, &matched_indices, highlight_color);
            div()
                .child(StyledText::new(label_shared).with_highlights(highlights))
                .into_any_element()
        };

        let item = ListItem::new(id)
            .py_1()
            .px_2()
            .rounded_sm()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .w_full()
                    .child(
                        div()
                            .w(px(18.0))
                            .flex_shrink_0()
                            .flex()
                            .items_center()
                            .justify_center()
                            .text_color(cx.theme().muted_foreground)
                            .children(icon_element),
                    )
                    .child(
                        div()
                            .flex_1()
                            .overflow_hidden()
                            .text_ellipsis()
                            .text_sm()
                            .child(label_element),
                    )
                    .when(self.is_searching(), {
                        move |this| {
                            this.child(
                                div()
                                    .text_xs()
                                    .text_color(cx.theme().muted_foreground)
                                    .flex_shrink_0()
                                    .max_w(px(140.0))
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .child(category_display),
                            )
                        }
                    }),
            )
            .suffix(move |window, _cx: &mut App| {
                if let Some(action) = &action {
                    let key_binding = action_context
                        .as_ref()
                        .and_then(|handle| handle.upgrade())
                        .and_then(|handle| {
                            Kbd::binding_for_action_in(action.as_ref(), &handle, window)
                        })
                        .or_else(|| Kbd::binding_for_action(action.as_ref(), None, window));

                    if let Some(kbd) = key_binding {
                        return kbd.into_any_element();
                    }
                }

                div().into_any_element()
            });

        Some(item)
    }

    fn render_section_header(
        &mut self,
        section: usize,
        _window: &mut Window,
        cx: &mut Context<ListState<Self>>,
    ) -> Option<impl IntoElement> {
        if self.is_searching() {
            return None;
        }

        let (name, _) = self.sections.get(section)?;
        let is_first = section == 0;

        Some(
            div()
                .px_3()
                .pt_2()
                .pb_1()
                .when(!is_first, |this| {
                    this.mt_1().border_t_1().border_color(cx.theme().border)
                })
                .text_xs()
                .font_weight(FontWeight::SEMIBOLD)
                .text_color(cx.theme().muted_foreground)
                .child(name.clone()),
        )
    }

    fn render_empty(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<ListState<Self>>,
    ) -> impl IntoElement {
        let message = if self.is_searching() {
            format!("No commands matching \"{}\"", self.query)
        } else {
            "No commands available".to_string()
        };

        let hint = if self.is_searching() {
            "Try a different search term"
        } else {
            "Connect to a database to see more commands"
        };

        v_flex()
            .w_full()
            .h(px(80.0))
            .items_center()
            .justify_center()
            .gap_1()
            .child(
                div()
                    .text_sm()
                    .text_color(cx.theme().muted_foreground)
                    .child(message),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(cx.theme().muted_foreground.opacity(0.7))
                    .child(hint),
            )
    }

    fn set_selected_index(
        &mut self,
        ix: Option<IndexPath>,
        _window: &mut Window,
        _cx: &mut Context<ListState<Self>>,
    ) {
        self.selected_index = ix;
    }

    fn confirm(
        &mut self,
        _secondary: bool,
        window: &mut Window,
        cx: &mut Context<ListState<Self>>,
    ) {
        let Some(ix) = self.selected_index else {
            return;
        };

        let Some(command_index) = self.command_index_at(ix) else {
            return;
        };

        let Some(command) = self.commands.get(command_index) else {
            return;
        };

        let command_id = command.id.clone();
        self.usage_stats
            .record_usage(&command_id, self.persistence.as_deref());

        if let Some(action_context) = self
            .action_context
            .as_ref()
            .and_then(|handle| handle.upgrade())
        {
            window.focus(&action_context, cx);
        }

        // Dispatch the action if present.
        if let Some(action) = &command.action {
            window.dispatch_action(action.boxed_clone(), cx);
        }

        // Store events for the wrapper to pick up.
        self.pending_event = command.palette_event.clone();
        self.pending_command_id = Some(command_id);
    }

    fn cancel(&mut self, _window: &mut Window, _cx: &mut Context<ListState<Self>>) {
        if let Some(action_context) = self
            .action_context
            .as_ref()
            .and_then(|handle| handle.upgrade())
        {
            _window.focus(&action_context, _cx);
        }
    }
}

// ── CommandPalette wrapper ──────────────────────────────────────────────

/// Thin wrapper around `ListState<CommandPaletteDelegate>`.
///
/// Persisted across open/close cycles so usage stats survive within a session.
/// Visibility is toggled by the parent rather than recreating.
pub struct CommandPalette {
    list_state: Entity<ListState<CommandPaletteDelegate>>,
    _subscriptions: Vec<Subscription>,
}

impl CommandPalette {
    pub fn new(
        commands: Vec<Command>,
        persistence: Option<Arc<dyn CommandUsagePersistence>>,
        action_context: Option<WeakFocusHandle>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut delegate = CommandPaletteDelegate::new();
        if let Some(persistence) = persistence {
            delegate.set_persistence(persistence);
        }
        delegate.set_action_context(action_context);
        delegate.set_commands(commands);

        let list_state = cx.new(|cx| {
            ListState::new(delegate, window, cx)
                .searchable(true)
                .reset_on_cancel(false)
        });

        // Set initial selection.
        list_state.update(cx, |state, cx| {
            let count = state.delegate().visible_commands_count();
            if count > 0 {
                state.set_selected_index(Some(IndexPath::default()), window, cx);
            }
        });

        let list_subscription = cx.subscribe(
            &list_state,
            |_this, list_state, event: &ListEvent, cx| match event {
                ListEvent::Confirm(_ix) => {
                    let delegate = list_state.read(cx).delegate();
                    let pending_event = delegate.pending_event.clone();
                    let pending_command_id = delegate.pending_command_id.clone();

                    if let Some(event) = pending_event {
                        cx.emit(event);
                    }
                    if let Some(command_id) = pending_command_id {
                        cx.emit(CommandPaletteEvent::CommandExecuted(command_id));
                    }
                    cx.emit(CommandPaletteEvent::Dismissed);
                }
                ListEvent::Cancel => {
                    cx.emit(CommandPaletteEvent::Dismissed);
                }
                ListEvent::Select(_) => {}
            },
        );

        Self {
            list_state,
            _subscriptions: vec![list_subscription],
        }
    }

    /// Reset the palette for a fresh open (clears query, refreshes commands).
    /// Pass a new command list to update what's available.
    pub fn reset(&mut self, commands: Vec<Command>, window: &mut Window, cx: &mut Context<Self>) {
        self.list_state.update(cx, |state, cx| {
            state.set_query("", window, cx);
            state.delegate_mut().reset();
            state.delegate_mut().set_commands(commands);
            let has_items = state.delegate().visible_commands_count() > 0;
            state.set_selected_index(
                if has_items {
                    Some(IndexPath::default())
                } else {
                    None
                },
                window,
                cx,
            );
        });
    }

    pub fn add_schema_commands(
        &mut self,
        connection_id: Uuid,
        connection_name: &str,
        tables: &[String],
        views: &[String],
        object_capabilities: SidebarObjectCapabilities,
        cx: &mut Context<Self>,
    ) {
        self.list_state.update(cx, |state, _cx| {
            state.delegate_mut().add_schema_commands(
                connection_id,
                connection_name,
                tables,
                views,
                object_capabilities,
            );
        });
    }

    /// Focus the palette's search input.
    pub fn focus(&self, window: &mut Window, cx: &mut App) {
        self.list_state.read(cx).focus_handle(cx).focus(window, cx);
    }

    /// Result count text for the footer, e.g. "3 of 42 commands".
    fn result_count_text(&self, cx: &App) -> String {
        let delegate = self.list_state.read(cx).delegate();
        let visible = delegate.visible_commands_count();
        let total = delegate.total_commands_count();
        if delegate.is_searching() {
            format!("{} of {} commands", visible, total)
        } else {
            format!("{} commands", total)
        }
    }
}

impl Render for CommandPalette {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let result_count = self.result_count_text(cx);

        use zqlz_ui::widgets::{h_flex, list::List};

        v_flex()
            .w(px(600.0))
            .max_h(px(500.0))
            .bg(theme.popover)
            .border_1()
            .border_color(theme.border)
            .rounded(theme.radius_lg)
            .shadow_lg()
            .overflow_hidden()
            .child(
                List::new(&self.list_state)
                    .search_placeholder("Type to search commands, tables, views...")
                    .scrollbar_visible(false)
                    .with_size(zqlz_ui::widgets::Size::Small)
                    .max_h(px(400.0)),
            )
            .child(
                div()
                    .w_full()
                    .px_3()
                    .py_1p5()
                    .border_t_1()
                    .border_color(theme.border)
                    .child(
                        h_flex()
                            .items_center()
                            .justify_between()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(result_count)
                            .child(
                                h_flex()
                                    .gap_3()
                                    .child("↑↓ Navigate")
                                    .child("↵ Execute")
                                    .child("Esc Dismiss"),
                            ),
                    ),
            )
    }
}

impl Focusable for CommandPalette {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.list_state.read(cx).focus_handle(cx)
    }
}

impl EventEmitter<CommandPaletteEvent> for CommandPalette {}
