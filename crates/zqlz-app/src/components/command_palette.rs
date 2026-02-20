//! Enhanced Command Palette
//!
//! A powerful searchable command palette (Cmd+Shift+P) with database schema-aware commands,
//! recent queries, frequently used commands tracking, and smart keyboard navigation.

use gpui::prelude::FluentBuilder;
use gpui::*;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::SystemTime;
use uuid::Uuid;
use zqlz_ui::widgets::{
    h_flex,
    input::{Input, InputEvent, InputState},
    v_flex, ActiveTheme, Sizable,
};

use crate::actions::*;
use crate::app::AppState;

/// Types of commands available in the palette
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
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
    RecentQuery {
        query_id: usize,
    },
}

/// A command that can be executed from the command palette
#[derive(Clone)]
pub struct Command {
    pub id: String,
    pub label: String,
    pub category: String,
    pub shortcut: Option<String>,
    pub icon: Option<&'static str>,
    pub command_type: CommandType,
    pub score: f32,
    pub last_used: Option<SystemTime>,
    action_fn: Option<Arc<dyn Fn(&mut Window, &mut App) + Send + Sync>>,
    palette_event: Option<CommandPaletteEvent>,
}

impl Command {
    pub fn new_static(
        id: impl Into<String>,
        label: impl Into<String>,
        category: impl Into<String>,
        shortcut: Option<impl Into<String>>,
        icon: Option<&'static str>,
        action_fn: impl Fn(&mut Window, &mut App) + Send + Sync + 'static,
    ) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            category: category.into(),
            shortcut: shortcut.map(|s| s.into()),
            icon,
            command_type: CommandType::Static,
            score: 0.0,
            last_used: None,
            action_fn: Some(Arc::new(action_fn)),
            palette_event: None,
        }
    }

    pub fn new_table(
        connection_id: Uuid,
        connection_name: &str,
        table_name: String,
        action_fn: impl Fn(&mut Window, &mut App) + Send + Sync + 'static,
    ) -> Self {
        Self {
            id: format!("table-{}-{}", connection_id, table_name),
            label: table_name.clone(),
            category: format!("Table: {}", connection_name),
            shortcut: None,
            icon: Some("▤"),
            command_type: CommandType::Table {
                connection_id,
                table_name: table_name.clone(),
            },
            score: 0.0,
            last_used: None,
            action_fn: Some(Arc::new(action_fn)),
            palette_event: Some(CommandPaletteEvent::OpenTable {
                connection_id,
                table_name,
            }),
        }
    }

    pub fn new_view(
        connection_id: Uuid,
        connection_name: &str,
        view_name: String,
        action_fn: impl Fn(&mut Window, &mut App) + Send + Sync + 'static,
    ) -> Self {
        Self {
            id: format!("view-{}-{}", connection_id, view_name),
            label: view_name.clone(),
            category: format!("View: {}", connection_name),
            shortcut: None,
            icon: Some("◧"),
            command_type: CommandType::View {
                connection_id,
                view_name: view_name.clone(),
            },
            score: 0.0,
            last_used: None,
            action_fn: Some(Arc::new(action_fn)),
            palette_event: Some(CommandPaletteEvent::OpenView {
                connection_id,
                view_name,
            }),
        }
    }

    pub fn new_connection(
        connection_id: Uuid,
        connection_name: String,
        action_fn: impl Fn(&mut Window, &mut App) + Send + Sync + 'static,
    ) -> Self {
        Self {
            id: format!("connection-{}", connection_id),
            label: format!("Connect to: {}", connection_name),
            category: "Recent Connections".to_string(),
            shortcut: None,
            icon: Some("◉"),
            command_type: CommandType::Connection { connection_id },
            score: 0.0,
            last_used: None,
            action_fn: Some(Arc::new(action_fn)),
            palette_event: Some(CommandPaletteEvent::ConnectToConnection(connection_id)),
        }
    }

    pub fn matches(&self, query: &str) -> bool {
        if query.is_empty() {
            return true;
        }

        let query_lower = query.to_lowercase();
        let label_lower = self.label.to_lowercase();
        let category_lower = self.category.to_lowercase();

        if label_lower.contains(&query_lower) || category_lower.contains(&query_lower) {
            return true;
        }

        let combined = format!("{} {}", category_lower, label_lower);
        let mut combined_chars = combined.chars().peekable();
        for qc in query_lower.chars() {
            loop {
                match combined_chars.next() {
                    Some(lc) if lc == qc => break,
                    Some(_) => continue,
                    None => return false,
                }
            }
        }
        true
    }

    pub fn relevance_score(&self, query: &str) -> f32 {
        if query.is_empty() {
            let recency_bonus = if let Some(last_used) = self.last_used {
                if let Ok(elapsed) = SystemTime::now().duration_since(last_used) {
                    let hours = elapsed.as_secs() / 3600;
                    10.0 / (1.0 + hours as f32 / 24.0)
                } else {
                    0.0
                }
            } else {
                0.0
            };
            return self.score + recency_bonus;
        }

        let query_lower = query.to_lowercase();
        let label_lower = self.label.to_lowercase();

        let mut score = self.score;

        if label_lower == query_lower {
            score += 100.0;
        } else if label_lower.starts_with(&query_lower) {
            score += 50.0;
        } else if label_lower.contains(&query_lower) {
            score += 25.0;
        } else {
            score += 10.0;
        }

        score
    }

    pub fn execute(&self, window: &mut Window, cx: &mut App) {
        if let Some(ref action_fn) = self.action_fn {
            action_fn(window, cx);
        }
    }
}

/// Events emitted by the command palette
#[derive(Clone, Debug)]
#[allow(dead_code)]
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

/// Command usage tracking
#[derive(Clone, Debug)]
struct CommandUsageStats {
    command_scores: HashMap<String, f32>,
    last_used: HashMap<String, SystemTime>,
}

impl CommandUsageStats {
    fn new() -> Self {
        Self {
            command_scores: HashMap::new(),
            last_used: HashMap::new(),
        }
    }

    fn record_usage(&mut self, command_id: &str) {
        *self
            .command_scores
            .entry(command_id.to_string())
            .or_insert(0.0) += 1.0;
        self.last_used
            .insert(command_id.to_string(), SystemTime::now());
    }

    fn get_score(&self, command_id: &str) -> f32 {
        self.command_scores.get(command_id).copied().unwrap_or(0.0)
    }

    fn get_last_used(&self, command_id: &str) -> Option<SystemTime> {
        self.last_used.get(command_id).copied()
    }
}

/// Command palette panel
pub struct CommandPalette {
    focus_handle: FocusHandle,
    input_state: Entity<InputState>,
    query: String,
    commands: Vec<Command>,
    filtered_commands: Vec<usize>,
    selected_index: usize,
    usage_stats: CommandUsageStats,
    _subscriptions: Vec<Subscription>,
}

impl CommandPalette {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        tracing::debug!("CommandPalette::new - Creating new command palette");

        let input_state = cx.new(|cx| {
            InputState::new(window, cx).placeholder("Type to search commands, tables, views...")
        });

        let input_weak = input_state.downgrade();
        let subscription =
            cx.subscribe(&input_state, move |this, _input, event: &InputEvent, cx| {
                if matches!(event, InputEvent::Change) {
                    if let Some(input) = input_weak.upgrade() {
                        let new_query = input.read(cx).value().to_string();
                        tracing::debug!(
                            "CommandPalette input changed: query='{}' -> '{}'",
                            this.query,
                            new_query
                        );
                        this.query = new_query;
                        this.filter_and_sort_commands();
                        this.selected_index = 0;
                        tracing::debug!(
                            "CommandPalette filtered to {} commands",
                            this.filtered_commands.len()
                        );
                        cx.notify();
                    }
                }
            });

        let mut palette = Self {
            focus_handle: cx.focus_handle(),
            input_state,
            query: String::new(),
            commands: Vec::new(),
            filtered_commands: Vec::new(),
            selected_index: 0,
            usage_stats: CommandUsageStats::new(),
            _subscriptions: vec![subscription],
        };

        palette.build_commands(cx);
        palette.filter_and_sort_commands();

        tracing::debug!(
            "CommandPalette::new - Created palette with {} total commands, {} visible",
            palette.commands.len(),
            palette.filtered_commands.len()
        );

        palette
    }

    fn build_commands(&mut self, cx: &App) {
        tracing::debug!("CommandPalette::build_commands - Building command list");

        let mut commands = Vec::new();
        let static_commands = Self::build_static_commands();
        tracing::debug!(
            "CommandPalette::build_commands - Added {} static commands",
            static_commands.len()
        );
        commands.extend(static_commands);

        if let Some(app_state) = cx.try_global::<AppState>() {
            let connection_commands = Self::build_connection_commands(app_state);
            tracing::debug!(
                "CommandPalette::build_commands - Added {} connection commands",
                connection_commands.len()
            );
            commands.extend(connection_commands);
        } else {
            tracing::warn!("CommandPalette::build_commands - No AppState available");
        }

        for cmd in commands.iter_mut() {
            cmd.score = self.usage_stats.get_score(&cmd.id);
            cmd.last_used = self.usage_stats.get_last_used(&cmd.id);
        }

        tracing::debug!(
            "CommandPalette::build_commands - Total commands built: {}",
            commands.len()
        );
        self.commands = commands;
    }

    fn build_static_commands() -> Vec<Command> {
        vec![
            Command::new_static(
                "settings",
                "Open Settings",
                "Application",
                Some("Cmd+,"),
                Some("◉"),
                |window, cx| window.dispatch_action(OpenSettings.boxed_clone(), cx),
            ),
            Command::new_static(
                "command-palette",
                "Command Palette",
                "Application",
                Some("Cmd+Shift+P"),
                Some("⌘"),
                |window, cx| window.dispatch_action(OpenCommandPalette.boxed_clone(), cx),
            ),
            Command::new_static(
                "quit",
                "Quit Application",
                "Application",
                Some("Cmd+Q"),
                None,
                |window, cx| window.dispatch_action(Quit.boxed_clone(), cx),
            ),
            Command::new_static(
                "new-connection",
                "New Connection",
                "Connection",
                Some("Cmd+Shift+N"),
                Some("+"),
                |window, cx| window.dispatch_action(NewConnection.boxed_clone(), cx),
            ),
            Command::new_static(
                "refresh-connection",
                "Refresh Current Connection",
                "Connection",
                None::<String>,
                Some("↻"),
                |window, cx| window.dispatch_action(RefreshConnection.boxed_clone(), cx),
            ),
            Command::new_static(
                "refresh-connections-list",
                "Refresh Connections List",
                "Connection",
                None::<String>,
                Some("↻"),
                |window, cx| window.dispatch_action(RefreshConnectionsList.boxed_clone(), cx),
            ),
            Command::new_static(
                "new-query",
                "New Query Tab",
                "Query",
                Some("Cmd+N"),
                Some("□"),
                |window, cx| window.dispatch_action(NewQuery.boxed_clone(), cx),
            ),
            Command::new_static(
                "execute-query",
                "Execute Query",
                "Query",
                Some("Cmd+Enter"),
                Some("▸"),
                |window, cx| window.dispatch_action(ExecuteQuery.boxed_clone(), cx),
            ),
            Command::new_static(
                "execute-selection",
                "Execute Selection",
                "Query",
                Some("Cmd+Shift+Enter"),
                Some("▸"),
                |window, cx| window.dispatch_action(ExecuteSelection.boxed_clone(), cx),
            ),
            Command::new_static(
                "stop-query",
                "Stop Query Execution",
                "Query",
                Some("Cmd+."),
                Some("■"),
                |window, cx| window.dispatch_action(StopQuery.boxed_clone(), cx),
            ),
            Command::new_static(
                "format-query",
                "Format Query",
                "Query",
                Some("Cmd+Shift+F"),
                None,
                |window, cx| window.dispatch_action(FormatQuery.boxed_clone(), cx),
            ),
            Command::new_static(
                "toggle-left-sidebar",
                "Toggle Left Sidebar",
                "Layout",
                Some("Cmd+B"),
                Some("▨"),
                |window, cx| window.dispatch_action(ToggleLeftSidebar.boxed_clone(), cx),
            ),
            Command::new_static(
                "toggle-right-sidebar",
                "Toggle Right Sidebar",
                "Layout",
                Some("Cmd+Shift+B"),
                Some("▧"),
                |window, cx| window.dispatch_action(ToggleRightSidebar.boxed_clone(), cx),
            ),
            Command::new_static(
                "toggle-bottom-panel",
                "Toggle Bottom Panel",
                "Layout",
                Some("Cmd+J"),
                Some("▬"),
                |window, cx| window.dispatch_action(ToggleBottomPanel.boxed_clone(), cx),
            ),
            Command::new_static(
                "focus-editor",
                "Focus Query Editor",
                "Focus",
                Some("Cmd+2"),
                None,
                |window, cx| window.dispatch_action(FocusEditor.boxed_clone(), cx),
            ),
            Command::new_static(
                "focus-results",
                "Focus Results Panel",
                "Focus",
                Some("Cmd+3"),
                None,
                |window, cx| window.dispatch_action(FocusResults.boxed_clone(), cx),
            ),
            Command::new_static(
                "focus-sidebar",
                "Focus Database Sidebar",
                "Focus",
                Some("Cmd+1"),
                None,
                |window, cx| window.dispatch_action(FocusSidebar.boxed_clone(), cx),
            ),
        ]
    }

    fn build_connection_commands(app_state: &AppState) -> Vec<Command> {
        let mut commands = Vec::new();

        for connection in app_state.saved_connections().iter().take(10) {
            let conn_id = connection.id;
            let conn_name = connection.name.clone();

            commands.push(Command::new_connection(
                conn_id,
                conn_name,
                move |_window, _cx| {
                    tracing::info!("Connect to connection: {}", conn_id);
                },
            ));
        }

        commands
    }

    pub fn add_schema_commands(
        &mut self,
        connection_id: Uuid,
        connection_name: &str,
        tables: &[String],
        views: &[String],
    ) {
        self.commands.retain(|cmd| {
            !matches!(
                cmd.command_type,
                CommandType::Table { connection_id: cid, .. } | CommandType::View { connection_id: cid, .. }
                if cid == connection_id
            )
        });

        for table_name in tables.iter() {
            let table_name_clone = table_name.clone();
            let cmd = Command::new_table(
                connection_id,
                connection_name,
                table_name.clone(),
                move |_window, _cx| {
                    tracing::info!("Open table: {}", table_name_clone);
                },
            );
            self.commands.push(cmd);
        }

        for view_name in views.iter() {
            let view_name_clone = view_name.clone();
            let cmd = Command::new_view(
                connection_id,
                connection_name,
                view_name.clone(),
                move |_window, _cx| {
                    tracing::info!("Open view: {}", view_name_clone);
                },
            );
            self.commands.push(cmd);
        }

        self.filter_and_sort_commands();
    }

    fn filter_and_sort_commands(&mut self) {
        let mut filtered: Vec<(usize, f32)> = self
            .commands
            .iter()
            .enumerate()
            .filter(|(_, cmd)| cmd.matches(&self.query))
            .map(|(idx, cmd)| (idx, cmd.relevance_score(&self.query)))
            .collect();

        filtered.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        self.filtered_commands = filtered.into_iter().map(|(idx, _)| idx).collect();
    }

    fn execute_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        tracing::debug!(
            "CommandPalette::execute_selected - selected_index={}, filtered_count={}",
            self.selected_index,
            self.filtered_commands.len()
        );

        if let Some(&cmd_idx) = self.filtered_commands.get(self.selected_index) {
            let command = &self.commands[cmd_idx];
            let command_id = command.id.clone();
            let command_label = command.label.clone();
            let command_category = command.category.clone();
            let palette_event = command.palette_event.clone();

            tracing::info!(
                "CommandPalette::execute_selected - Executing command: id='{}', label='{}', category='{}'",
                command_id, command_label, command_category
            );

            self.usage_stats.record_usage(&command_id);

            tracing::debug!(
                "CommandPalette::execute_selected - Calling action_fn for '{}'",
                command_id
            );
            command.execute(window, cx.deref_mut());

            // Emit specific palette event if one is defined (e.g., for connections, tables, views)
            if let Some(event) = palette_event {
                tracing::debug!(
                    "CommandPalette::execute_selected - Emitting palette event: {:?}",
                    event
                );
                cx.emit(event);
            }

            tracing::debug!("CommandPalette::execute_selected - Emitting CommandExecuted event");
            cx.emit(CommandPaletteEvent::CommandExecuted(command_id.clone()));

            tracing::debug!("CommandPalette::execute_selected - Emitting Dismissed event");
            cx.emit(CommandPaletteEvent::Dismissed);

            tracing::debug!(
                "CommandPalette::execute_selected - Command execution complete for '{}'",
                command_id
            );
        } else {
            tracing::warn!(
                "CommandPalette::execute_selected - No command at selected_index={}",
                self.selected_index
            );
        }
    }

    fn select_up(&mut self, cx: &mut Context<Self>) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
            tracing::trace!(
                "CommandPalette::select_up - New selected_index={}",
                self.selected_index
            );
            cx.notify();
        }
    }

    fn select_down(&mut self, cx: &mut Context<Self>) {
        if self.selected_index + 1 < self.filtered_commands.len() {
            self.selected_index += 1;
            tracing::trace!(
                "CommandPalette::select_down - New selected_index={}",
                self.selected_index
            );
            cx.notify();
        }
    }

    fn select_first(&mut self, cx: &mut Context<Self>) {
        if !self.filtered_commands.is_empty() {
            self.selected_index = 0;
            tracing::trace!("CommandPalette::select_first - Reset to index 0");
            cx.notify();
        }
    }

    fn select_last(&mut self, cx: &mut Context<Self>) {
        if !self.filtered_commands.is_empty() {
            self.selected_index = self.filtered_commands.len() - 1;
            tracing::trace!(
                "CommandPalette::select_last - Jumped to index {}",
                self.selected_index
            );
            cx.notify();
        }
    }

    fn dismiss(&mut self, cx: &mut Context<Self>) {
        tracing::debug!("CommandPalette::dismiss - Dismissing command palette");
        cx.emit(CommandPaletteEvent::Dismissed);
    }

    pub fn input_state(&self) -> &Entity<InputState> {
        &self.input_state
    }
}

impl Render for CommandPalette {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let selected_idx = self.selected_index;

        let command_items: Vec<_> = self
            .filtered_commands
            .iter()
            .enumerate()
            .map(|(idx, &cmd_idx)| {
                let cmd = &self.commands[cmd_idx];
                (
                    idx,
                    cmd.id.clone(),
                    cmd.label.clone(),
                    cmd.category.clone(),
                    cmd.shortcut.clone(),
                    cmd.icon,
                    idx == selected_idx,
                )
            })
            .collect();

        let is_empty = self.filtered_commands.is_empty();

        v_flex()
            .key_context("CommandPalette")
            .track_focus(&self.focus_handle)
            .on_mouse_down(MouseButton::Left, |_, _, _| {})
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                match event.keystroke.key.as_str() {
                    "escape" => this.dismiss(cx),
                    "up" => this.select_up(cx),
                    "down" => this.select_down(cx),
                    "home" => this.select_first(cx),
                    "end" => this.select_last(cx),
                    "pageup" => {
                        for _ in 0..8 {
                            this.select_up(cx);
                        }
                    }
                    "pagedown" => {
                        for _ in 0..8 {
                            this.select_down(cx);
                        }
                    }
                    "enter" => this.execute_selected(window, cx),
                    _ => {}
                }
            }))
            .w(px(600.0))
            .max_h(px(500.0))
            .bg(theme.background)
            .border_1()
            .border_color(theme.border)
            .rounded(px(8.0))
            .shadow_lg()
            .overflow_hidden()
            .child(
                div()
                    .w_full()
                    .p_2()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(Input::new(&self.input_state).small().w_full()),
            )
            .child(
                div()
                    .id("command-list")
                    .flex_1()
                    .w_full()
                    .max_h(px(400.0))
                    .overflow_y_scroll()
                    .p_1()
                    .children(command_items.into_iter().map(
                        |(idx, _id, label, category, shortcut, icon, is_selected)| {
                            h_flex()
                                .id(SharedString::from(format!("cmd-{}", idx)))
                                .w_full()
                                .h(px(36.0))
                                .px_3()
                                .py_2()
                                .gap_3()
                                .items_center()
                                .rounded(px(4.0))
                                .cursor_pointer()
                                .when(is_selected, |this| this.bg(theme.accent))
                                .when(!is_selected, |this| {
                                    this.hover(|style| style.bg(theme.muted))
                                })
                                .on_click(cx.listener(move |this, _, window, cx| {
                                    this.selected_index = idx;
                                    this.execute_selected(window, cx);
                                }))
                                .when_some(icon, |this, icon_char| {
                                    this.child(
                                        div()
                                            .text_sm()
                                            .text_color(if is_selected {
                                                theme.accent_foreground
                                            } else {
                                                theme.muted_foreground
                                            })
                                            .child(icon_char),
                                    )
                                })
                                .child(
                                    div()
                                        .flex_1()
                                        .overflow_hidden()
                                        .text_ellipsis()
                                        .text_sm()
                                        .font_weight(if is_selected {
                                            FontWeight::SEMIBOLD
                                        } else {
                                            FontWeight::NORMAL
                                        })
                                        .text_color(if is_selected {
                                            theme.accent_foreground
                                        } else {
                                            theme.foreground
                                        })
                                        .child(label),
                                )
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(if is_selected {
                                            theme.accent_foreground.opacity(0.7)
                                        } else {
                                            theme.muted_foreground
                                        })
                                        .child(category),
                                )
                                .when_some(shortcut, |this, shortcut| {
                                    this.child(
                                        div()
                                            .px_2()
                                            .py_1()
                                            .rounded(px(4.0))
                                            .bg(if is_selected {
                                                theme.accent_foreground.opacity(0.2)
                                            } else {
                                                theme.muted.opacity(0.5)
                                            })
                                            .text_xs()
                                            .font_family("monospace")
                                            .text_color(if is_selected {
                                                theme.accent_foreground
                                            } else {
                                                theme.muted_foreground
                                            })
                                            .child(shortcut),
                                    )
                                })
                        },
                    ))
                    .when(is_empty, |this| {
                        this.child(
                            div()
                                .w_full()
                                .h(px(80.0))
                                .flex()
                                .flex_col()
                                .items_center()
                                .justify_center()
                                .gap_2()
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .child("No matching commands"),
                                )
                                .child(
                                    div()
                                        .text_xs()
                                        .text_color(theme.muted_foreground.opacity(0.7))
                                        .child("Try a different search term"),
                                ),
                        )
                    }),
            )
            .when(!is_empty, |this| {
                this.child(
                    div()
                        .w_full()
                        .px_3()
                        .py_2()
                        .border_t_1()
                        .border_color(theme.border)
                        .child(
                            h_flex()
                                .gap_4()
                                .text_xs()
                                .text_color(theme.muted_foreground)
                                .child("↑↓ Navigate")
                                .child("↵ Execute")
                                .child("Esc Dismiss"),
                        ),
                )
            })
    }
}

impl Focusable for CommandPalette {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<CommandPaletteEvent> for CommandPalette {}
