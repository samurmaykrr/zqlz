//! Schema tree sidebar
//!
//! Displays a hierarchical tree of database objects (tables, views, triggers, indexes, etc.)

use gpui::prelude::FluentBuilder;
use gpui::*;
use uuid::Uuid;
use zqlz_ui::widgets::{
    button::{Button, ButtonVariants},
    dock::{Panel, PanelEvent, TitleStyle},
    h_flex,
    menu::{PopupMenu, PopupMenuItem},
    v_flex, ActiveTheme, Disableable, Icon, IconName, Sizable,
};

/// Context menu state for schema tree
struct ContextMenuState {
    menu: Entity<PopupMenu>,
    open: bool,
    position: Point<Pixels>,
    menu_subscription: Option<Subscription>,
}

impl ContextMenuState {
    fn new(window: &mut Window, cx: &mut App) -> Entity<Self> {
        cx.new(|cx| {
            let menu = PopupMenu::build(window, cx, |menu, _, _| menu);
            Self {
                menu,
                open: false,
                position: Point::default(),
                menu_subscription: None,
            }
        })
    }
}

impl Render for ContextMenuState {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.open {
            return div().into_any_element();
        }

        deferred(
            anchored()
                .snap_to_window_with_margin(px(8.))
                .anchor(Corner::TopLeft)
                .position(self.position)
                .child(
                    div()
                        .occlude()
                        .font_family(cx.theme().font_family.clone())
                        .cursor_default()
                        .child(self.menu.clone()),
                ),
        )
        .with_priority(1)
        .into_any_element()
    }
}

/// Schema tree node types
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaNodeType {
    Database,
    TablesFolder,
    Table(String),
    ViewsFolder,
    View(String),
    TriggersFolder,
    Trigger(String),
    IndexesFolder,
    Index(String),
    FunctionsFolder,
    Function(String),
    ProceduresFolder,
    Procedure(String),
}

impl SchemaNodeType {
    fn icon_name(&self) -> IconName {
        match self {
            Self::Database => IconName::HardDrive,
            Self::TablesFolder | Self::Table(_) => IconName::LayoutDashboard,
            Self::ViewsFolder | Self::View(_) => IconName::Eye,
            Self::TriggersFolder | Self::Trigger(_) => IconName::Asterisk,
            Self::IndexesFolder | Self::Index(_) => IconName::Menu,
            Self::FunctionsFolder | Self::Function(_) => IconName::Bot,
            Self::ProceduresFolder | Self::Procedure(_) => IconName::SquareTerminal,
        }
    }

    fn display_name(&self) -> String {
        match self {
            Self::Database => "Database".to_string(),
            Self::TablesFolder => "Tables".to_string(),
            Self::Table(name) => name.clone(),
            Self::ViewsFolder => "Views".to_string(),
            Self::View(name) => name.clone(),
            Self::TriggersFolder => "Triggers".to_string(),
            Self::Trigger(name) => name.clone(),
            Self::IndexesFolder => "Indexes".to_string(),
            Self::Index(name) => name.clone(),
            Self::FunctionsFolder => "Functions".to_string(),
            Self::Function(name) => name.clone(),
            Self::ProceduresFolder => "Procedures".to_string(),
            Self::Procedure(name) => name.clone(),
        }
    }
}

/// Schema tree node
#[derive(Clone, Debug)]
pub struct SchemaNode {
    pub node_type: SchemaNodeType,
    pub expanded: bool,
    pub children: Vec<SchemaNode>,
    pub loading: bool,
}

impl SchemaNode {
    pub fn new(node_type: SchemaNodeType) -> Self {
        Self {
            node_type,
            expanded: false,
            children: Vec::new(),
            loading: false,
        }
    }

    pub fn new_with_children(node_type: SchemaNodeType, children: Vec<SchemaNode>) -> Self {
        Self {
            node_type,
            expanded: false,
            children,
            loading: false,
        }
    }
}

/// Database schema data for building the tree
#[derive(Clone, Debug, Default)]
pub struct DatabaseSchemaData {
    pub tables: Vec<String>,
    pub views: Vec<String>,
    pub triggers: Vec<String>,
    pub functions: Vec<String>,
    pub procedures: Vec<String>,
}

impl DatabaseSchemaData {
    /// Build tree nodes from schema data
    pub fn into_tree_nodes(self) -> Vec<SchemaNode> {
        let mut root_nodes = Vec::new();

        // Tables folder
        if !self.tables.is_empty() {
            let table_nodes: Vec<SchemaNode> = self
                .tables
                .into_iter()
                .map(|name| SchemaNode::new(SchemaNodeType::Table(name)))
                .collect();

            root_nodes.push(SchemaNode::new_with_children(
                SchemaNodeType::TablesFolder,
                table_nodes,
            ));
        }

        // Views folder
        if !self.views.is_empty() {
            let view_nodes: Vec<SchemaNode> = self
                .views
                .into_iter()
                .map(|name| SchemaNode::new(SchemaNodeType::View(name)))
                .collect();

            root_nodes.push(SchemaNode::new_with_children(
                SchemaNodeType::ViewsFolder,
                view_nodes,
            ));
        }

        // Triggers folder
        if !self.triggers.is_empty() {
            let trigger_nodes: Vec<SchemaNode> = self
                .triggers
                .into_iter()
                .map(|name| SchemaNode::new(SchemaNodeType::Trigger(name)))
                .collect();

            root_nodes.push(SchemaNode::new_with_children(
                SchemaNodeType::TriggersFolder,
                trigger_nodes,
            ));
        }

        // Functions folder
        if !self.functions.is_empty() {
            let function_nodes: Vec<SchemaNode> = self
                .functions
                .into_iter()
                .map(|name| SchemaNode::new(SchemaNodeType::Function(name)))
                .collect();

            root_nodes.push(SchemaNode::new_with_children(
                SchemaNodeType::FunctionsFolder,
                function_nodes,
            ));
        }

        // Procedures folder
        if !self.procedures.is_empty() {
            let procedure_nodes: Vec<SchemaNode> = self
                .procedures
                .into_iter()
                .map(|name| SchemaNode::new(SchemaNodeType::Procedure(name)))
                .collect();

            root_nodes.push(SchemaNode::new_with_children(
                SchemaNodeType::ProceduresFolder,
                procedure_nodes,
            ));
        }

        root_nodes
    }
}

/// Events from the schema tree
#[derive(Clone, Debug)]
pub enum SchemaTreeEvent {
    /// Node was selected
    NodeSelected(SchemaNodeType),
    /// Request to refresh schema (parent should handle loading)
    RefreshRequested,

    // ============================================
    // View-related events
    // ============================================
    /// Open a view (show its data)
    OpenView {
        connection_id: Uuid,
        view_name: String,
    },
    /// Design/edit a view definition
    DesignView {
        connection_id: Uuid,
        view_name: String,
    },
    /// View version history for a view
    ViewHistory {
        connection_id: Uuid,
        object_name: String,
        object_type: String, // "view", "function", "procedure", "trigger"
    },

    // ============================================
    // Function-related events
    // ============================================
    /// Open/view a function definition
    OpenFunction {
        connection_id: Uuid,
        function_name: String,
    },
    /// Delete a function
    DeleteFunction {
        connection_id: Uuid,
        function_name: String,
    },

    // ============================================
    // Procedure-related events
    // ============================================
    /// Open/view a procedure definition
    OpenProcedure {
        connection_id: Uuid,
        procedure_name: String,
    },
    /// Delete a procedure
    DeleteProcedure {
        connection_id: Uuid,
        procedure_name: String,
    },

    // ============================================
    // Trigger-related events
    // ============================================
    /// Open/view a trigger definition
    OpenTrigger {
        connection_id: Uuid,
        trigger_name: String,
    },
    /// Delete a trigger
    DeleteTrigger {
        connection_id: Uuid,
        trigger_name: String,
    },

    // ============================================
    // Table-related events
    // ============================================
    /// Open a table (show its data)
    OpenTable {
        connection_id: Uuid,
        table_name: String,
    },
    /// Design/edit table structure
    DesignTable {
        connection_id: Uuid,
        table_name: String,
    },
}

/// Schema tree sidebar panel
pub struct SchemaTreePanel {
    focus_handle: FocusHandle,
    connection_id: Option<Uuid>,
    root_nodes: Vec<SchemaNode>,
    selected_node: Option<SchemaNodeType>,
    is_loading: bool,
    context_menu: Option<Entity<ContextMenuState>>,
}

impl SchemaTreePanel {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
            connection_id: None,
            root_nodes: Vec::new(),
            selected_node: None,
            is_loading: false,
            context_menu: None,
        }
    }

    /// Set the active connection ID
    pub fn set_connection_id(&mut self, connection_id: Option<Uuid>, cx: &mut Context<Self>) {
        self.connection_id = connection_id;
        if connection_id.is_none() {
            self.root_nodes.clear();
            self.selected_node = None;
        }
        cx.notify();
    }

    /// Set loading state
    pub fn set_loading(&mut self, loading: bool, cx: &mut Context<Self>) {
        self.is_loading = loading;
        cx.notify();
    }

    /// Set the schema data (parent loads and provides this)
    pub fn set_schema(&mut self, schema: DatabaseSchemaData, cx: &mut Context<Self>) {
        self.root_nodes = schema.into_tree_nodes();
        self.is_loading = false;
        cx.notify();
    }

    /// Clear the tree
    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.connection_id = None;
        self.root_nodes.clear();
        self.selected_node = None;
        self.is_loading = false;
        cx.notify();
    }

    /// Request a refresh (emits event for parent to handle)
    pub fn request_refresh(&mut self, cx: &mut Context<Self>) {
        if self.connection_id.is_some() {
            self.is_loading = true;
            cx.emit(SchemaTreeEvent::RefreshRequested);
            cx.notify();
        }
    }

    /// Toggle node expansion
    fn toggle_node(&mut self, node_type: &SchemaNodeType, cx: &mut Context<Self>) {
        Self::toggle_node_recursive(&mut self.root_nodes, node_type);
        cx.notify();
    }

    fn toggle_node_recursive(nodes: &mut [SchemaNode], target: &SchemaNodeType) {
        for node in nodes.iter_mut() {
            if &node.node_type == target {
                node.expanded = !node.expanded;
                return;
            }
            Self::toggle_node_recursive(&mut node.children, target);
        }
    }

    /// Select a node
    fn select_node(&mut self, node_type: SchemaNodeType, cx: &mut Context<Self>) {
        self.selected_node = Some(node_type.clone());
        cx.emit(SchemaTreeEvent::NodeSelected(node_type));
        cx.notify();
    }

    /// Render the toolbar
    fn render_toolbar(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        h_flex()
            .w_full()
            .h(px(32.0))
            .px_2()
            .gap_1()
            .items_center()
            .border_b_1()
            .border_color(theme.border)
            .child(
                Button::new("refresh-schema")
                    .ghost()
                    .xsmall()
                    .icon(IconName::Redo)
                    .disabled(self.connection_id.is_none())
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.request_refresh(cx);
                    })),
            )
            .child(div().flex_1())
    }

    /// Render tree nodes recursively
    fn render_node(
        &self,
        node: &SchemaNode,
        level: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme();
        let indent = level * 16;
        let has_children = !node.children.is_empty();
        let is_selected = self.selected_node.as_ref() == Some(&node.node_type);
        let node_type = node.node_type.clone();
        let node_type_for_toggle = node.node_type.clone();
        let node_type_for_menu = node.node_type.clone();
        let connection_id = self.connection_id;

        v_flex()
            .w_full()
            .child(
                h_flex()
                    .id(format!("schema-node-{:?}-{}", node.node_type, level))
                    .w_full()
                    .h(px(28.0))
                    .px(px(indent as f32 + 8.0))
                    .gap_1()
                    .items_center()
                    .when(is_selected, |this| this.bg(theme.accent.opacity(0.2)))
                    .hover(|this| this.bg(theme.muted.opacity(0.5)))
                    .cursor_pointer()
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.select_node(node_type.clone(), cx);
                    }))
                    .on_mouse_down(
                        MouseButton::Right,
                        cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                            if let Some(conn_id) = connection_id {
                                this.show_context_menu(
                                    &node_type_for_menu,
                                    conn_id,
                                    event.position,
                                    window,
                                    cx,
                                );
                            }
                        }),
                    )
                    .child(
                        div()
                            .w(px(16.0))
                            .h(px(16.0))
                            .flex()
                            .items_center()
                            .justify_center()
                            .when(has_children, |this| {
                                let expanded = node.expanded;
                                let node_type_clone = node_type_for_toggle.clone();
                                this.child(
                                    Button::new(format!("expand-{:?}", node_type_clone))
                                        .ghost()
                                        .xsmall()
                                        .icon(if expanded {
                                            IconName::ChevronDown
                                        } else {
                                            IconName::ChevronRight
                                        })
                                        .on_click(cx.listener(move |this, _, _, cx| {
                                            this.toggle_node(&node_type_clone, cx);
                                        })),
                                )
                            }),
                    )
                    .child(
                        Icon::new(node.node_type.icon_name())
                            .size_4()
                            .text_color(theme.muted_foreground),
                    )
                    .child(div().text_sm().child(node.node_type.display_name())),
            )
            .when(node.expanded && has_children, |this| {
                let children_elements: Vec<_> = node
                    .children
                    .iter()
                    .map(|child| self.render_node(child, level + 1, window, cx))
                    .collect();
                this.children(children_elements)
            })
            .into_any_element()
    }

    /// Show context menu for a node
    fn show_context_menu(
        &mut self,
        node_type: &SchemaNodeType,
        connection_id: Uuid,
        position: Point<Pixels>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // Initialize context menu state if needed
        if self.context_menu.is_none() {
            self.context_menu = Some(ContextMenuState::new(window, cx));
        }

        let panel_weak = cx.entity().downgrade();

        if let Some(menu_state) = &self.context_menu {
            let node_type = node_type.clone();
            menu_state.update(cx, |state, cx| {
                state.position = position;

                let new_menu = match &node_type {
                    SchemaNodeType::View(name) => {
                        Self::build_view_menu(&panel_weak, connection_id, name.clone(), window, cx)
                    }
                    SchemaNodeType::Function(name) => Self::build_function_menu(
                        &panel_weak,
                        connection_id,
                        name.clone(),
                        window,
                        cx,
                    ),
                    SchemaNodeType::Procedure(name) => Self::build_procedure_menu(
                        &panel_weak,
                        connection_id,
                        name.clone(),
                        window,
                        cx,
                    ),
                    SchemaNodeType::Trigger(name) => Self::build_trigger_menu(
                        &panel_weak,
                        connection_id,
                        name.clone(),
                        window,
                        cx,
                    ),
                    SchemaNodeType::Table(name) => {
                        Self::build_table_menu(&panel_weak, connection_id, name.clone(), window, cx)
                    }
                    _ => return,
                };

                // Subscribe to dismiss event
                let menu_entity = new_menu.clone();
                let menu_state_entity = cx.entity().clone();
                state.menu_subscription = Some(cx.subscribe(
                    &menu_entity,
                    move |_state, _, _event: &DismissEvent, cx| {
                        let menu_state = menu_state_entity.clone();
                        cx.defer(move |cx| {
                            _ = menu_state.update(cx, |state, cx| {
                                state.open = false;
                                cx.notify();
                            });
                        });
                    },
                ));

                state.menu = new_menu.clone();
                state.open = true;

                if !new_menu.focus_handle(cx).contains_focused(window, cx) {
                    new_menu.focus_handle(cx).focus(window, cx);
                }

                cx.notify();
            });
        }
    }

    /// Build context menu for views
    fn build_view_menu(
        panel_weak: &WeakEntity<Self>,
        connection_id: Uuid,
        view_name: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<PopupMenu> {
        let panel = panel_weak.clone();
        let view_name_open = view_name.clone();
        let view_name_design = view_name.clone();
        let view_name_history = view_name.clone();

        PopupMenu::build(window, cx, move |menu, _, _| {
            menu.item(PopupMenuItem::new("Open View").on_click({
                let panel = panel.clone();
                let name = view_name_open.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::OpenView {
                            connection_id,
                            view_name: name.clone(),
                        });
                    });
                }
            }))
            .separator()
            .item(PopupMenuItem::new("Design View").on_click({
                let panel = panel.clone();
                let name = view_name_design.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::DesignView {
                            connection_id,
                            view_name: name.clone(),
                        });
                    });
                }
            }))
            .separator()
            .item(PopupMenuItem::new("View History").on_click({
                let panel = panel.clone();
                let name = view_name_history.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::ViewHistory {
                            connection_id,
                            object_name: name.clone(),
                            object_type: "view".to_string(),
                        });
                    });
                }
            }))
        })
    }

    /// Build context menu for functions
    fn build_function_menu(
        panel_weak: &WeakEntity<Self>,
        connection_id: Uuid,
        function_name: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<PopupMenu> {
        let panel = panel_weak.clone();
        let function_name_open = function_name.clone();
        let function_name_history = function_name.clone();
        let function_name_delete = function_name.clone();

        PopupMenu::build(window, cx, move |menu, _, _| {
            menu.item(PopupMenuItem::new("View Definition").on_click({
                let panel = panel.clone();
                let name = function_name_open.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::OpenFunction {
                            connection_id,
                            function_name: name.clone(),
                        });
                    });
                }
            }))
            .separator()
            .item(PopupMenuItem::new("View History").on_click({
                let panel = panel.clone();
                let name = function_name_history.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::ViewHistory {
                            connection_id,
                            object_name: name.clone(),
                            object_type: "function".to_string(),
                        });
                    });
                }
            }))
            .separator()
            .item(PopupMenuItem::new("Delete Function").on_click({
                let panel = panel.clone();
                let name = function_name_delete.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::DeleteFunction {
                            connection_id,
                            function_name: name.clone(),
                        });
                    });
                }
            }))
        })
    }

    /// Build context menu for procedures
    fn build_procedure_menu(
        panel_weak: &WeakEntity<Self>,
        connection_id: Uuid,
        procedure_name: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<PopupMenu> {
        let panel = panel_weak.clone();
        let procedure_name_open = procedure_name.clone();
        let procedure_name_history = procedure_name.clone();
        let procedure_name_delete = procedure_name.clone();

        PopupMenu::build(window, cx, move |menu, _, _| {
            menu.item(PopupMenuItem::new("View Definition").on_click({
                let panel = panel.clone();
                let name = procedure_name_open.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::OpenProcedure {
                            connection_id,
                            procedure_name: name.clone(),
                        });
                    });
                }
            }))
            .separator()
            .item(PopupMenuItem::new("View History").on_click({
                let panel = panel.clone();
                let name = procedure_name_history.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::ViewHistory {
                            connection_id,
                            object_name: name.clone(),
                            object_type: "procedure".to_string(),
                        });
                    });
                }
            }))
            .separator()
            .item(PopupMenuItem::new("Delete Procedure").on_click({
                let panel = panel.clone();
                let name = procedure_name_delete.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::DeleteProcedure {
                            connection_id,
                            procedure_name: name.clone(),
                        });
                    });
                }
            }))
        })
    }

    /// Build context menu for triggers
    fn build_trigger_menu(
        panel_weak: &WeakEntity<Self>,
        connection_id: Uuid,
        trigger_name: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<PopupMenu> {
        let panel = panel_weak.clone();
        let trigger_name_open = trigger_name.clone();
        let trigger_name_history = trigger_name.clone();
        let trigger_name_delete = trigger_name.clone();

        PopupMenu::build(window, cx, move |menu, _, _| {
            menu.item(PopupMenuItem::new("View Definition").on_click({
                let panel = panel.clone();
                let name = trigger_name_open.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::OpenTrigger {
                            connection_id,
                            trigger_name: name.clone(),
                        });
                    });
                }
            }))
            .separator()
            .item(PopupMenuItem::new("View History").on_click({
                let panel = panel.clone();
                let name = trigger_name_history.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::ViewHistory {
                            connection_id,
                            object_name: name.clone(),
                            object_type: "trigger".to_string(),
                        });
                    });
                }
            }))
            .separator()
            .item(PopupMenuItem::new("Delete Trigger").on_click({
                let panel = panel.clone();
                let name = trigger_name_delete.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::DeleteTrigger {
                            connection_id,
                            trigger_name: name.clone(),
                        });
                    });
                }
            }))
        })
    }

    /// Build context menu for tables
    fn build_table_menu(
        panel_weak: &WeakEntity<Self>,
        connection_id: Uuid,
        table_name: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<PopupMenu> {
        let panel = panel_weak.clone();
        let table_name_open = table_name.clone();
        let table_name_design = table_name.clone();

        PopupMenu::build(window, cx, move |menu, _, _| {
            menu.item(PopupMenuItem::new("Open Table").on_click({
                let panel = panel.clone();
                let name = table_name_open.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::OpenTable {
                            connection_id,
                            table_name: name.clone(),
                        });
                    });
                }
            }))
            .separator()
            .item(PopupMenuItem::new("Design Table").on_click({
                let panel = panel.clone();
                let name = table_name_design.clone();
                move |_event, _window, cx| {
                    _ = panel.update(cx, |_panel, cx| {
                        cx.emit(SchemaTreeEvent::DesignTable {
                            connection_id,
                            table_name: name.clone(),
                        });
                    });
                }
            }))
        })
    }

    /// Render empty state
    fn render_empty_state(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .items_center()
            .justify_center()
            .p_4()
            .child(
                div()
                    .text_sm()
                    .text_color(theme.muted_foreground)
                    .text_center()
                    .child("Connect to a database to browse its schema"),
            )
    }

    /// Render loading state
    fn render_loading(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex().size_full().items_center().justify_center().child(
            div()
                .text_sm()
                .text_color(theme.muted_foreground)
                .child("Loading schema..."),
        )
    }
}

impl Render for SchemaTreePanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        v_flex()
            .id("schema-tree-panel")
            .key_context("SchemaTreePanel")
            .track_focus(&self.focus_handle)
            .size_full()
            .bg(theme.background)
            .child(self.render_toolbar(cx))
            .child(
                div()
                    .id("schema-tree-content")
                    .flex_1()
                    .w_full()
                    .overflow_y_scroll()
                    .map(|this| {
                        if self.is_loading {
                            this.child(self.render_loading(cx))
                        } else if self.root_nodes.is_empty() {
                            this.child(self.render_empty_state(cx))
                        } else {
                            this.child(v_flex().w_full().children({
                                let root_elements: Vec<_> = self
                                    .root_nodes
                                    .iter()
                                    .map(|node| self.render_node(node, 0, window, cx))
                                    .collect();
                                root_elements
                            }))
                        }
                    }),
            )
            .children(self.context_menu.clone())
    }
}

impl Focusable for SchemaTreePanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<PanelEvent> for SchemaTreePanel {}
impl EventEmitter<SchemaTreeEvent> for SchemaTreePanel {}

impl Panel for SchemaTreePanel {
    fn panel_name(&self) -> &'static str {
        "SchemaTreePanel"
    }

    fn title(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        "Schema"
    }

    fn title_style(&self, _cx: &App) -> Option<TitleStyle> {
        None
    }

    fn closable(&self, _cx: &App) -> bool {
        false
    }
}
